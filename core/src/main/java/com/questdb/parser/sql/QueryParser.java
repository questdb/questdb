/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.parser.sql;

import com.questdb.common.ColumnType;
import com.questdb.ex.ParserException;
import com.questdb.parser.sql.model.*;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;
import com.questdb.store.factory.configuration.GenericIndexedBuilder;
import com.questdb.store.factory.configuration.JournalStructure;

public final class QueryParser {

    public static final int MAX_ORDER_BY_COLUMNS = 1560;
    private static final CharSequenceHashSet journalAliasStop = new CharSequenceHashSet();
    private static final CharSequenceHashSet columnAliasStop = new CharSequenceHashSet();
    private static final CharSequenceHashSet groupByStopSet = new CharSequenceHashSet();
    private static final CharSequenceIntHashMap joinStartSet = new CharSequenceIntHashMap();
    private final ObjectPool<ExprNode> exprNodePool = new ObjectPool<>(ExprNode.FACTORY, 128);
    private final ExprAstBuilder astBuilder = new ExprAstBuilder();
    private final ObjectPool<QueryModel> queryModelPool = new ObjectPool<>(QueryModel.FACTORY, 8);
    private final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 64);
    private final ObjectPool<AnalyticColumn> analyticColumnPool = new ObjectPool<>(AnalyticColumn.FACTORY, 8);
    private final ObjectPool<CreateJournalModel> createJournalModelPool = new ObjectPool<>(CreateJournalModel.FACTORY, 4);
    private final ObjectPool<ColumnIndexModel> columnIndexModelPool = new ObjectPool<>(ColumnIndexModel.FACTORY, 8);
    private final ObjectPool<ColumnCastModel> columnCastModelPool = new ObjectPool<>(ColumnCastModel.FACTORY, 8);
    private final ObjectPool<RenameJournalModel> renameJournalModelPool = new ObjectPool<>(RenameJournalModel.FACTORY, 8);
    private final ObjectPool<WithClauseModel> withClauseModelPool = new ObjectPool<>(WithClauseModel.FACTORY, 16);
    private final Lexer secondaryLexer = new Lexer();
    private final ExprParser exprParser = new ExprParser(exprNodePool);
    private Lexer lexer = new Lexer();

    public QueryParser() {
        ExprParser.configureLexer(lexer);
        ExprParser.configureLexer(secondaryLexer);
    }

    public ParsedModel parse(CharSequence query) throws ParserException {
        clear();
        return parseInternal(query);
    }

    private void clear() {
        queryModelPool.clear();
        queryColumnPool.clear();
        exprNodePool.clear();
        analyticColumnPool.clear();
        createJournalModelPool.clear();
        columnIndexModelPool.clear();
        columnCastModelPool.clear();
        renameJournalModelPool.clear();
        withClauseModelPool.clear();
    }

    private ParserException err(String msg) {
        return QueryError.$(lexer.position(), msg);
    }

    private ExprNode expectExpr() throws ParserException {
        ExprNode n = expr();
        if (n == null) {
            throw QueryError.$(lexer.position(), "Expression expected");
        }
        return n;
    }

    private ExprNode expectLiteral() throws ParserException {
        CharSequence tok = tok();
        int pos = lexer.position();
        validateLiteral(pos, tok);
        ExprNode node = exprNodePool.next();
        node.token = tok.toString();
        node.type = ExprNode.LITERAL;
        node.position = pos;
        return node;
    }

    private void expectTok(CharSequence tok, CharSequence expected) throws ParserException {
        if (tok == null || !Chars.equals(tok, expected)) {
            throw QueryError.position(lexer.position()).$('\'').$(expected).$("' expected").$();
        }
    }

    private void expectTok(char expected) throws ParserException {
        expectTok(tok(), lexer.position(), expected);
    }

    private void expectTok(CharSequence tok, int pos, char expected) throws ParserException {
        if (tok == null || !Chars.equals(tok, expected)) {
            throw QueryError.position(pos).$('\'').$(expected).$("' expected").$();
        }
    }

    private ExprNode expr() throws ParserException {
        astBuilder.reset();
        exprParser.parseExpr(lexer, astBuilder);
        return astBuilder.poll();
    }

    private QueryModel getOrParseQueryModelFromWithClause(WithClauseModel wcm) throws ParserException {
        QueryModel m = wcm.popModel();
        if (m != null) {
            return m;
        }

        secondaryLexer.setContent(lexer.getContent(), wcm.getLo(), wcm.getHi());

        Lexer tmp = this.lexer;
        this.lexer = secondaryLexer;
        try {
            return parseQuery(true);
        } finally {
            lexer = tmp;
        }
    }

    private boolean isFieldTerm(CharSequence tok) {
        return Chars.equals(tok, ')') || Chars.equals(tok, ',');
    }

    private ExprNode literal() {
        CharSequence tok = lexer.optionTok();
        if (tok == null) {
            return null;
        }
        return exprNodePool.next().of(ExprNode.LITERAL, Chars.stripQuotes(tok.toString()), 0, lexer.position());
    }

    private ExprNode makeJoinAlias(int index) {
        CharSink b = Misc.getThreadLocalBuilder();
        ExprNode node = exprNodePool.next();
        node.token = b.put("_xQdbA").put(index).toString();
        node.type = ExprNode.LITERAL;
        return node;
    }

    private ExprNode makeModelAlias(String modelAlias, ExprNode node) {
        CharSink b = Misc.getThreadLocalBuilder();
        ExprNode exprNode = exprNodePool.next();
        b.put(modelAlias).put('.').put(node.token);
        exprNode.token = b.toString();
        exprNode.type = ExprNode.LITERAL;
        exprNode.position = node.position;
        return exprNode;
    }

    private ExprNode makeOperation(String token, ExprNode lhs, ExprNode rhs) {
        ExprNode expr = exprNodePool.next();
        expr.token = token;
        expr.type = ExprNode.OPERATION;
        expr.position = 0;
        expr.paramCount = 2;
        expr.lhs = lhs;
        expr.rhs = rhs;
        return expr;
    }

    private CharSequence notTermTok() throws ParserException {
        CharSequence tok = tok();
        if (isFieldTerm(tok)) {
            throw err("Invalid column definition");
        }
        return tok;
    }

    private ParsedModel parseCreateJournal() throws ParserException {
        ExprNode name = exprNodePool.next();
        name.token = Chars.stripQuotes(tok().toString());
        name.position = lexer.position();
        name.type = ExprNode.LITERAL;

        CharSequence tok = tok();

        final JournalStructure struct;
        final QueryModel queryModel;
        if (Chars.equals(tok, '(')) {
            queryModel = null;
            struct = new JournalStructure(name.token);
            lexer.unparse();
            parseJournalFields(struct);
        } else if (Chars.equals(tok, "as")) {
            expectTok('(');
            queryModel = parseQuery(true);
            struct = null;
            expectTok(')');
        } else {
            throw QueryError.position(lexer.position()).$("Unexpected token").$();
        }

        CreateJournalModel model = createJournalModelPool.next();
        model.setStruct(struct);
        model.setQueryModel(queryModel);
        model.setName(name);

        tok = lexer.optionTok();
        while (tok != null && Chars.equals(tok, ',')) {

            int pos = lexer.position();

            tok = tok();
            if (Chars.equals(tok, "index")) {
                expectTok('(');

                ColumnIndexModel columnIndexModel = columnIndexModelPool.next();
                columnIndexModel.setName(expectLiteral());

                pos = lexer.position();
                tok = tok();
                if (Chars.equals(tok, "buckets")) {
                    try {
                        columnIndexModel.setBuckets(Numbers.ceilPow2(Numbers.parseInt(tok())) - 1);
                    } catch (NumericException e) {
                        throw QueryError.$(pos, "Int constant expected");
                    }
                    pos = lexer.position();
                    tok = tok();
                }
                expectTok(tok, pos, ')');

                model.addColumnIndexModel(columnIndexModel);
                tok = lexer.optionTok();
            } else if (Chars.equals(tok, "cast")) {
                expectTok('(');
                ColumnCastModel columnCastModel = columnCastModelPool.next();

                columnCastModel.setName(expectLiteral());
                expectTok(tok(), "as");

                ExprNode node = expectLiteral();
                int type = ColumnType.columnTypeOf(node.token);
                if (type == -1) {
                    throw QueryError.$(node.position, "invalid type");
                }

                columnCastModel.setType(type, node.position);

                if (type == ColumnType.SYMBOL) {
                    tok = lexer.optionTok();
                    pos = lexer.position();

                    if (Chars.equals(tok, "count")) {
                        try {
                            columnCastModel.setCount(Numbers.parseInt(tok()));
                            tok = tok();
                        } catch (NumericException e) {
                            throw QueryError.$(pos, "int value expected");
                        }
                    }
                } else {
                    pos = lexer.position();
                    tok = tok();
                }

                expectTok(tok, pos, ')');

                if (!model.addColumnCastModel(columnCastModel)) {
                    throw QueryError.$(columnCastModel.getName().position, "duplicate cast");
                }

                tok = lexer.optionTok();
            } else {
                throw QueryError.$(pos, "Unexpected token");
            }
        }

        ExprNode timestamp = parseTimestamp(tok);
        if (timestamp != null) {
            model.setTimestamp(timestamp);
            tok = lexer.optionTok();
        }

        ExprNode partitionBy = parsePartitionBy(tok);
        if (partitionBy != null) {
            model.setPartitionBy(partitionBy);
            tok = lexer.optionTok();
        }

        ExprNode hint = parseRecordHint(tok);
        if (hint != null) {
            model.setRecordHint(hint);
            tok = lexer.optionTok();
        }

        if (tok != null) {
            throw QueryError.$(lexer.position(), "Unexpected token");
        }
        return model;
    }

    private ParsedModel parseCreateStatement() throws ParserException {
        CharSequence tok = tok();
        if (Chars.equals(tok, "table")) {
            return parseCreateJournal();
        }

        throw err("table expected");
    }

    private CharSequence parseIndexDefinition(GenericIndexedBuilder builder) throws ParserException {
        CharSequence tok = tok();

        if (isFieldTerm(tok)) {
            return tok;
        }

        expectTok(tok, "index");
        builder.index();

        if (isFieldTerm(tok = tok())) {
            return tok;
        }

        expectTok(tok, "buckets");

        try {
            builder.buckets(Numbers.parseInt(tok()));
        } catch (NumericException e) {
            throw err("bad int");
        }

        return null;
    }

    ParsedModel parseInternal(CharSequence query) throws ParserException {
        lexer.setContent(query);
        CharSequence tok = tok();

        if (Chars.equals(tok, "create")) {
            return parseCreateStatement();
        }

        if (Chars.equals(tok, "rename")) {
            return parseRenameStatement();
        }

        lexer.unparse();
        return parseQuery(false);
    }

    private QueryModel parseJoin(CharSequence tok, int joinType, QueryModel parent) throws ParserException {
        QueryModel joinModel = queryModelPool.next();
        joinModel.setJoinType(joinType);

        if (!Chars.equals(tok, "join")) {
            expectTok(tok(), "join");
        }

        tok = tok();

        if (Chars.equals(tok, '(')) {
            joinModel.setNestedModel(parseQuery(true));
            expectTok(')');
        } else {
            lexer.unparse();
            parseWithClauseOrJournalName(joinModel, parent);
        }

        tok = lexer.optionTok();

        if (tok != null && !journalAliasStop.contains(tok)) {
            lexer.unparse();
            joinModel.setAlias(expr());
        } else {
            lexer.unparse();
        }

        tok = lexer.optionTok();

        if (joinType == QueryModel.JOIN_CROSS && tok != null && Chars.equals(tok, "on")) {
            throw QueryError.$(lexer.position(), "Cross joins cannot have join clauses");
        }

        switch (joinType) {
            case QueryModel.JOIN_ASOF:
                if (tok == null || !Chars.equals("on", tok)) {
                    lexer.unparse();
                    break;
                }
                // intentional fall through
            case QueryModel.JOIN_INNER:
            case QueryModel.JOIN_OUTER:
                expectTok(tok, "on");
                astBuilder.reset();
                exprParser.parseExpr(lexer, astBuilder);
                ExprNode expr;
                switch (astBuilder.size()) {
                    case 0:
                        throw QueryError.$(lexer.position(), "Expression expected");
                    case 1:
                        expr = astBuilder.poll();
                        if (expr.type == ExprNode.LITERAL) {
                            do {
                                joinModel.addJoinColumn(expr);
                            } while ((expr = astBuilder.poll()) != null);
                        } else {
                            joinModel.setJoinCriteria(expr);
                        }
                        break;
                    default:
                        while ((expr = astBuilder.poll()) != null) {
                            if (expr.type != ExprNode.LITERAL) {
                                throw QueryError.$(lexer.position(), "Column name expected");
                            }
                            joinModel.addJoinColumn(expr);
                        }
                        break;
                }
                break;
            default:
                lexer.unparse();
        }

        return joinModel;
    }

    private void parseJournalFields(JournalStructure struct) throws ParserException {
        if (!Chars.equals(tok(), '(')) {
            throw err("( expected");
        }

        while (true) {
            String name = notTermTok().toString();
            CharSequence tok = null;

            switch (ColumnType.columnTypeOf(notTermTok())) {
                case ColumnType.BYTE:
                    struct.$byte(name);
                    break;
                case ColumnType.INT:
                    tok = parseIndexDefinition(struct.$int(name));
                    break;
                case ColumnType.DOUBLE:
                    struct.$double(name);
                    break;
                case ColumnType.BOOLEAN:
                    struct.$bool(name);
                    break;
                case ColumnType.FLOAT:
                    struct.$float(name);
                    break;
                case ColumnType.LONG:
                    tok = parseIndexDefinition(struct.$long(name));
                    break;
                case ColumnType.SHORT:
                    struct.$short(name);
                    break;
                case ColumnType.STRING:
                    tok = parseIndexDefinition(struct.$str(name));
                    break;
                case ColumnType.SYMBOL:
                    tok = parseIndexDefinition(struct.$sym(name));
                    break;
                case ColumnType.BINARY:
                    struct.$bin(name);
                    break;
                case ColumnType.DATE:
                    struct.$date(name);
                    break;
                default:
                    throw err("Unsupported type");
            }

            if (tok == null) {
                tok = tok();
            }

            if (Chars.equals(tok, ')')) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(", or ) expected");
            }
        }
    }

    private void parseLatestBy(QueryModel model) throws ParserException {
        expectTok(tok(), "by");
        model.setLatestBy(expr());
    }

    private ExprNode parsePartitionBy(CharSequence tok) throws ParserException {
        if (Chars.equalsNc("partition", tok)) {
            expectTok(tok(), "by");
            return expectLiteral();
        }
        return null;
    }

    private QueryModel parseQuery(boolean subQuery) throws ParserException {

        CharSequence tok;
        QueryModel model = queryModelPool.next();

        tok = tok();

        if (Chars.equals(tok, "with")) {
            parseWithClauses(model);
            tok = tok();
        }

        // [select]
        if (Chars.equals(tok, "select")) {
            parseSelectColumns(model);
            tok = tok();
        }

        // expect "(" in case of sub-query

        if (Chars.equals(tok, '(')) {
            model.setNestedModel(parseQuery(true));

            // expect closing bracket
            expectTok(')');

            tok = lexer.optionTok();

            // check if tok is not "where" - should be alias

            if (tok != null && !journalAliasStop.contains(tok)) {
                lexer.unparse();
                model.setAlias(literal());
                tok = lexer.optionTok();
            }

            // expect [timestamp(column)]

            ExprNode timestamp = parseTimestamp(tok);
            if (timestamp != null) {
                model.setTimestamp(timestamp);
                tok = lexer.optionTok();
            }
        } else {

            lexer.unparse();

            parseWithClauseOrJournalName(model, model);

            tok = lexer.optionTok();

            if (tok != null && !journalAliasStop.contains(tok)) {
                lexer.unparse();
                model.setAlias(literal());
                tok = lexer.optionTok();
            }

            // expect [timestamp(column)]

            ExprNode timestamp = parseTimestamp(tok);
            if (timestamp != null) {
                model.setTimestamp(timestamp);
                tok = lexer.optionTok();
            }

            // expect [latest by]

            if (Chars.equalsNc("latest", tok)) {
                parseLatestBy(model);
                tok = lexer.optionTok();
            }
        }

        // expect multiple [[inner | outer | cross] join]

        int joinType;
        while (tok != null && (joinType = joinStartSet.get(tok)) != -1) {
            model.addJoinModel(parseJoin(tok, joinType, model));
            tok = lexer.optionTok();
        }

        // expect [where]

        if (tok != null && Chars.equals(tok, "where")) {
            model.setWhereClause(expr());
            tok = lexer.optionTok();
        }

        // expect [group by]

        if (tok != null && Chars.equals(tok, "sample")) {
            expectTok(tok(), "by");
            model.setSampleBy(expectLiteral());
            tok = lexer.optionTok();
        }

        // expect [order by]

        if (tok != null && Chars.equals(tok, "order")) {
            expectTok(tok(), "by");
            do {
                tok = tok();

                if (Chars.equals(tok, ')')) {
                    throw err("Expression expected");
                }

                lexer.unparse();
                ExprNode n = expectLiteral();

                tok = lexer.optionTok();

                if (tok != null && Chars.equalsIgnoreCase(tok, "desc")) {

                    model.addOrderBy(n, QueryModel.ORDER_DIRECTION_DESCENDING);
                    tok = lexer.optionTok();

                } else {

                    model.addOrderBy(n, QueryModel.ORDER_DIRECTION_ASCENDING);

                    if (tok != null && Chars.equalsIgnoreCase(tok, "asc")) {
                        tok = lexer.optionTok();
                    }
                }

                if (model.getOrderBy().size() >= MAX_ORDER_BY_COLUMNS) {
                    throw err("Too many columns");
                }

            } while (tok != null && Chars.equals(tok, ','));
        }

        // expect [limit]
        if (tok != null && Chars.equals(tok, "limit")) {
            ExprNode lo = expr();
            ExprNode hi = null;

            tok = lexer.optionTok();
            if (tok != null && Chars.equals(tok, ',')) {
                hi = expr();
                tok = lexer.optionTok();
            }
            model.setLimit(lo, hi);
        }

        if (subQuery) {
            lexer.unparse();
        } else if (tok != null) {
            throw QueryError.position(lexer.position()).$("Unexpected token: ").$(tok).$();
        }

        resolveJoinColumns(model);

        return model;
    }

    private ExprNode parseRecordHint(CharSequence tok) throws ParserException {
        if (Chars.equalsNc("record", tok)) {
            expectTok(tok(), "hint");
            ExprNode hint = expectExpr();
            if (hint.type != ExprNode.CONSTANT) {
                throw QueryError.$(hint.position, "Constant expected");
            }
            return hint;
        }
        return null;
    }

    private ParsedModel parseRenameStatement() throws ParserException {
        expectTok(tok(), "table");
        RenameJournalModel model = renameJournalModelPool.next();
        ExprNode e = expectExpr();
        if (e.type != ExprNode.LITERAL && e.type != ExprNode.CONSTANT) {
            throw QueryError.$(e.position, "literal or constant expected");
        }
        model.setFrom(e);
        expectTok(tok(), "to");

        e = expectExpr();
        if (e.type != ExprNode.LITERAL && e.type != ExprNode.CONSTANT) {
            throw QueryError.$(e.position, "literal or constant expected");
        }
        model.setTo(e);
        return model;
    }

    private void parseSelectColumns(QueryModel model) throws ParserException {
        CharSequence tok;
        while (true) {
            ExprNode expr = expr();
            if (expr == null) {
                throw QueryError.$(lexer.position(), "missing column");
            }

            String alias;
            int aliasPosition = lexer.position();

            tok = tok();

            if (!columnAliasStop.contains(tok)) {
                alias = tok.toString();
                tok = tok();
            } else {
                alias = null;
                aliasPosition = -1;
            }

            if (Chars.equals(tok, "over")) {
                // analytic
                expectTok('(');

                AnalyticColumn col = analyticColumnPool.next().of(alias, aliasPosition, expr);
                tok = tok();

                if (Chars.equals(tok, "partition")) {
                    expectTok(tok(), "by");

                    ObjList<ExprNode> partitionBy = col.getPartitionBy();

                    do {
                        partitionBy.add(expectLiteral());
                        tok = tok();
                    } while (Chars.equals(tok, ','));
                }

                if (Chars.equals(tok, "order")) {
                    expectTok(tok(), "by");

                    do {
                        ExprNode e = expectLiteral();
                        tok = tok();

                        if (Chars.equalsIgnoreCase(tok, "desc")) {
                            col.addOrderBy(e, QueryModel.ORDER_DIRECTION_DESCENDING);
                            tok = tok();
                        } else {
                            col.addOrderBy(e, QueryModel.ORDER_DIRECTION_ASCENDING);
                            if (Chars.equalsIgnoreCase(tok, "asc")) {
                                tok = tok();
                            }
                        }
                    } while (Chars.equals(tok, ','));
                }

                if (!Chars.equals(tok, ')')) {
                    throw err(") expected");
                }

                model.addColumn(col);

                tok = tok();
            } else {
                model.addColumn(queryColumnPool.next().of(alias, aliasPosition, expr));
            }

            if (Chars.equals(tok, "from")) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(",|from expected");
            }
        }
    }

    private ExprNode parseTimestamp(CharSequence tok) throws ParserException {
        if (Chars.equalsNc("timestamp", tok)) {
            expectTok('(');
            final ExprNode result = expectLiteral();
            expectTok(')');
            return result;
        }
        return null;
    }

    private void parseWithClauseOrJournalName(QueryModel target, QueryModel parent) throws ParserException {
        ExprNode journalName = literal();
        WithClauseModel withClause = journalName != null ? parent.getWithClause(journalName.token) : null;
        if (withClause != null) {
            target.setNestedModel(getOrParseQueryModelFromWithClause(withClause));
        } else {
            target.setJournalName(journalName);
        }
    }

    private void parseWithClauses(QueryModel model) throws ParserException {
        do {
            ExprNode name = expectLiteral();

            if (model.getWithClause(name.token) != null) {
                throw QueryError.$(name.position, "duplicate name");
            }

            expectTok(tok(), "as");
            expectTok('(');
            int lo, hi;
            lo = lexer.position();
            QueryModel m = parseQuery(true);
            hi = lexer.position();
            WithClauseModel wcm = withClauseModelPool.next();
            wcm.of(lo + 1, hi, m);
            expectTok(')');
            model.addWithClause(name.token, wcm);

            CharSequence tok = lexer.optionTok();
            if (tok == null || !Chars.equals(tok, ',')) {
                lexer.unparse();
                break;
            }
        } while (true);
    }

    private void resolveJoinColumns(QueryModel model) {
        ObjList<QueryModel> joinModels = model.getJoinModels();
        if (joinModels.size() == 0) {
            return;
        }

        String modelAlias;

        if (model.getAlias() != null) {
            modelAlias = model.getAlias().token;
        } else if (model.getJournalName() != null) {
            modelAlias = model.getJournalName().token;
        } else {
            ExprNode alias = makeJoinAlias(0);
            model.setAlias(alias);
            modelAlias = alias.token;
        }

        for (int i = 1, n = joinModels.size(); i < n; i++) {
            QueryModel jm = joinModels.getQuick(i);

            ObjList<ExprNode> jc = jm.getJoinColumns();
            if (jc.size() > 0) {

                String jmAlias;

                if (jm.getAlias() != null) {
                    jmAlias = jm.getAlias().token;
                } else if (jm.getJournalName() != null) {
                    jmAlias = jm.getJournalName().token;
                } else {
                    ExprNode alias = makeJoinAlias(i);
                    jm.setAlias(alias);
                    jmAlias = alias.token;
                }

                ExprNode joinCriteria = jm.getJoinCriteria();
                for (int j = 0, m = jc.size(); j < m; j++) {
                    ExprNode node = jc.getQuick(j);
                    ExprNode eq = makeOperation("=", makeModelAlias(modelAlias, node), makeModelAlias(jmAlias, node));
                    if (joinCriteria == null) {
                        joinCriteria = eq;
                    } else {
                        joinCriteria = makeOperation("and", joinCriteria, eq);
                    }
                }
                jm.setJoinCriteria(joinCriteria);
            }
        }
    }

    private CharSequence tok() throws ParserException {
        CharSequence tok = lexer.optionTok();
        if (tok == null) {
            throw err("Unexpected end of input");
        }
        return tok;
    }

    private void validateLiteral(int pos, CharSequence tok) throws ParserException {
        switch (tok.charAt(0)) {
            case '(':
            case ')':
            case ',':
            case '`':
            case '"':
            case '\'':
                throw QueryError.$(pos, "literal expected");
            default:
                break;

        }
    }

    static {
        journalAliasStop.add("where");
        journalAliasStop.add("latest");
        journalAliasStop.add("join");
        journalAliasStop.add("inner");
        journalAliasStop.add("outer");
        journalAliasStop.add("asof");
        journalAliasStop.add("cross");
        journalAliasStop.add("sample");
        journalAliasStop.add("order");
        journalAliasStop.add("on");
        journalAliasStop.add("timestamp");
        journalAliasStop.add("limit");
        journalAliasStop.add(")");
        //
        columnAliasStop.add("from");
        columnAliasStop.add(",");
        columnAliasStop.add("over");
        //
        groupByStopSet.add("order");
        groupByStopSet.add(")");
        groupByStopSet.add(",");

        joinStartSet.put("join", QueryModel.JOIN_INNER);
        joinStartSet.put("inner", QueryModel.JOIN_INNER);
        joinStartSet.put("outer", QueryModel.JOIN_OUTER);
        joinStartSet.put("cross", QueryModel.JOIN_CROSS);
        joinStartSet.put("asof", QueryModel.JOIN_ASOF);
    }
}

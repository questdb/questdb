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

package com.questdb.griffin;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.griffin.model.*;
import com.questdb.std.*;

final class SqlParser {

    static final int MAX_ORDER_BY_COLUMNS = 1560;
    private static final CharSequenceHashSet tableAliasStop = new CharSequenceHashSet();
    private static final CharSequenceHashSet columnAliasStop = new CharSequenceHashSet();
    private static final CharSequenceHashSet groupByStopSet = new CharSequenceHashSet();
    private static final CharSequenceIntHashMap joinStartSet = new CharSequenceIntHashMap();
    private final ObjectPool<SqlNode> sqlNodePool;
    private final ExpressionASTBuilder expressionASTBuilder = new ExpressionASTBuilder();
    private final ObjectPool<QueryModel> queryModelPool;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<AnalyticColumn> analyticColumnPool = new ObjectPool<>(AnalyticColumn.FACTORY, 8);
    private final ObjectPool<CreateTableModel> createTableModelPool = new ObjectPool<>(CreateTableModel.FACTORY, 4);
    private final ObjectPool<ColumnCastModel> columnCastModelPool = new ObjectPool<>(ColumnCastModel.FACTORY, 8);
    private final ObjectPool<RenameTableModel> renameTableModelPool = new ObjectPool<>(RenameTableModel.FACTORY, 8);
    private final ObjectPool<WithClauseModel> withClauseModelPool = new ObjectPool<>(WithClauseModel.FACTORY, 16);
    private final ExpressionParser expressionParser;
    private final CairoConfiguration configuration;
    private final PostOrderTreeTraversalAlgo traversalAlgo;
    private final ObjList<SqlNode> tempExprNodes = new ObjList<>();
    private final CharacterStore characterStore;
    private final SqlOptimiser optimiser;
    private boolean subQueryMode = false;

    SqlParser(
            CairoConfiguration configuration, SqlOptimiser optimiser, CharacterStore characterStore, ObjectPool<SqlNode> sqlNodePool,
            ObjectPool<QueryColumn> queryColumnPool, ObjectPool<QueryModel> queryModelPool,
            PostOrderTreeTraversalAlgo traversalAlgo) {
        this.sqlNodePool = sqlNodePool;
        this.queryModelPool = queryModelPool;
        this.queryColumnPool = queryColumnPool;
        this.configuration = configuration;
        this.traversalAlgo = traversalAlgo;
        this.characterStore = characterStore;
        this.optimiser = optimiser;
        this.expressionParser = new ExpressionParser(sqlNodePool);
    }

    private static SqlException err(GenericLexer lexer, String msg) {
        return SqlException.$(lexer.lastTokenPosition(), msg);
    }

    private static SqlException errUnexpected(GenericLexer lexer, CharSequence token) {
        return SqlException.unexpectedToken(lexer.lastTokenPosition(), token);
    }

    void clear() {
        queryModelPool.clear();
        queryColumnPool.clear();
        sqlNodePool.clear();
        analyticColumnPool.clear();
        createTableModelPool.clear();
        columnCastModelPool.clear();
        renameTableModelPool.clear();
        withClauseModelPool.clear();
        subQueryMode = false;
        characterStore.clear();
    }

    private CharSequence createColumnAlias(SqlNode node, QueryModel model) {
        return SqlUtil.createColumnAlias(characterStore, node.token, Chars.indexOf(node.token, '.'), model.getColumnNameTypeMap());
    }

    private SqlNode expectExpr(GenericLexer lexer) throws SqlException {
        SqlNode n = expr(lexer);
        if (n == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "Expression expected");
        }
        return n;
    }

    private int expectInt(GenericLexer lexer) throws SqlException {
        try {
            return Numbers.parseInt(tok(lexer, "integer"));
        } catch (NumericException e) {
            throw err(lexer, "bad integer");
        }
    }

    private SqlNode expectLiteral(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "literal");
        int pos = lexer.lastTokenPosition();
        validateLiteral(pos, tok);
        return nextLiteral(GenericLexer.immutableOf(tok), pos);
    }

    private CharSequence expectTableNameOrSubQuery(GenericLexer lexer) throws SqlException {
        return tok(lexer, "table name or sub-query");
    }

    private void expectTok(GenericLexer lexer, CharSequence tok, CharSequence expected) throws SqlException {
        if (tok == null || !Chars.equals(tok, expected)) {
            throw SqlException.position(lexer.lastTokenPosition()).put('\'').put(expected).put("' expected");
        }
    }

    private void expectTok(GenericLexer lexer, CharSequence expected) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.lastTokenPosition()).put('\'').put(expected).put("' expected");
        }
        expectTok(lexer, tok, expected);
    }

    private void expectTok(GenericLexer lexer, char expected) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.lastTokenPosition()).put('\'').put(expected).put("' expected");
        }
        expectTok(tok, lexer.lastTokenPosition(), expected);
    }

    private void expectTok(CharSequence tok, int pos, char expected) throws SqlException {
        if (tok == null || !Chars.equals(tok, expected)) {
            throw SqlException.position(pos).put('\'').put(expected).put("' expected");
        }
    }

    SqlNode expr(GenericLexer lexer) throws SqlException {
        expressionASTBuilder.reset();
        expressionParser.parseExpr(lexer, expressionASTBuilder);
        return rewriteCase(expressionASTBuilder.poll());
    }

    // test only
    void expr(GenericLexer lexer, ExpressionParserListener listener) throws SqlException {
        expressionParser.parseExpr(lexer, listener);
    }

    private int getCreateTableColumnIndex(CreateTableModel model, CharSequence columnName, int position) throws SqlException {
        int index = model.getColumnIndex(columnName);
        if (index == -1) {
            throw SqlException.invalidColumn(position, columnName);
        }
        return index;
    }

    private boolean isFieldTerm(CharSequence tok) {
        return Chars.equals(tok, ')') || Chars.equals(tok, ',');
    }

    private SqlNode literal(GenericLexer lexer, CharSequence name) {
        // this can never be null in its current contexts
        // every time this function is called is after lexer.unparse(), which ensures non-null token.
        return sqlNodePool.next().of(SqlNode.LITERAL, GenericLexer.unquote(name), 0, lexer.lastTokenPosition());
    }

    private SqlNode nextLiteral(CharSequence token, int position) {
        return SqlUtil.nextLiteral(sqlNodePool, token, position);
    }

    private CharSequence notTermTok(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "')' or ','");
        if (isFieldTerm(tok)) {
            throw err(lexer, "missing column definition");
        }
        return tok;
    }

    private CharSequence optTok(GenericLexer lexer) {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || (subQueryMode && Chars.equals(tok, ')'))) {
            return null;
        }
        return tok;
    }

    ExecutionModel parse(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "'create', 'rename' or 'select'");

        if (Chars.equals(tok, "select")) {
            return parseSelect(lexer);
        }

        if (Chars.equals(tok, "create")) {
            return parseCreateStatement(lexer);
        }

        if (Chars.equals(tok, "rename")) {
            return parseRenameStatement(lexer);
        }

        return parseSelect(lexer);
    }

    private ExecutionModel parseCreateStatement(GenericLexer lexer) throws SqlException {
        expectTok(lexer, "table");
        return parseCreateTable(lexer);
    }

    private ExecutionModel parseCreateTable(GenericLexer lexer) throws SqlException {
        final CreateTableModel model = createTableModelPool.next();
        model.setName(nextLiteral(GenericLexer.unquote(tok(lexer, "table name")), lexer.lastTokenPosition()));

        CharSequence tok = tok(lexer, "'(' or 'as'");

        if (Chars.equals(tok, '(')) {
            lexer.unparse();
            parseCreateTableColumns(lexer, model);
        } else if (Chars.equals(tok, "as")) {
            parseCreateTableAsSelect(lexer, model);
        } else {
            throw errUnexpected(lexer, tok);
        }

        while ((tok = optTok(lexer)) != null && Chars.equals(tok, ',')) {
            tok = tok(lexer, "'index' or 'cast'");
            if (Chars.equals(tok, "index")) {
                parseCreateTableIndexDef(lexer, model);
            } else if (Chars.equals(tok, "cast")) {
                parseCreateTableCastDef(lexer, model);
            } else {
                throw errUnexpected(lexer, tok);
            }
        }

        SqlNode timestamp = parseTimestamp(lexer, tok);
        if (timestamp != null) {
            // ignore index, validate column
            getCreateTableColumnIndex(model, timestamp.token, timestamp.position);
            model.setTimestamp(timestamp);
            tok = optTok(lexer);
        }

        SqlNode partitionBy = parseCreateTablePartition(lexer, tok);
        if (partitionBy != null) {
            if (PartitionBy.fromString(partitionBy.token) == -1) {
                throw SqlException.$(partitionBy.position, "'NONE', 'DAY', 'MONTH' or 'YEAR' expected");
            }
            model.setPartitionBy(partitionBy);
            tok = optTok(lexer);
        }

        if (tok != null) {
            throw errUnexpected(lexer, tok);
        }
        return model;
    }

    private void parseCreateTableAsSelect(GenericLexer lexer, CreateTableModel model) throws SqlException {
        expectTok(lexer, '(');
        QueryModel queryModel = optimiser.optimise(parseDml(lexer));
        ObjList<QueryColumn> columns = queryModel.getColumns();
        assert columns.size() > 0;

        // we do not know types of columns at this stage
        // compiler must put table together using query metadata.
        for (int i = 0, n = columns.size(); i < n; i++) {
            model.addColumn(columns.getQuick(i).getName(), -1, configuration.getCutlassSymbolCapacity());
        }

        model.setQueryModel(queryModel);
        expectTok(lexer, ')');
    }

    private void parseCreateTableCastDef(GenericLexer lexer, CreateTableModel model) throws SqlException {
        if (model.getQueryModel() == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "cast is only supported in 'create table as ...' context");
        }
        expectTok(lexer, '(');
        ColumnCastModel columnCastModel = columnCastModelPool.next();

        columnCastModel.setName(expectLiteral(lexer));
        expectTok(lexer, "as");

        final SqlNode node = expectLiteral(lexer);
        final int type = toColumnType(lexer, node.token);
        columnCastModel.setType(type, node.position);

        if (type == ColumnType.SYMBOL) {
            if (Chars.equals(optTok(lexer), "capacity")) {
                columnCastModel.setSymbolCapacity(expectInt(lexer));
            } else {
                lexer.unparse();
                columnCastModel.setSymbolCapacity(configuration.getCutlassSymbolCapacity());
            }
        }

        expectTok(lexer, ')');

        if (!model.addColumnCastModel(columnCastModel)) {
            throw SqlException.$(columnCastModel.getName().position, "duplicate cast");
        }
    }

    private void parseCreateTableColumns(GenericLexer lexer, CreateTableModel model) throws SqlException {
        expectTok(lexer, '(');

        while (true) {
            final int position = lexer.lastTokenPosition();
            final CharSequence name = GenericLexer.immutableOf(notTermTok(lexer));
            final int type = toColumnType(lexer, notTermTok(lexer));

            if (!model.addColumn(name, type, configuration.getCutlassSymbolCapacity())) {
                throw SqlException.$(position, "Duplicate column");
            }

            CharSequence tok;
            switch (type) {
                case ColumnType.SYMBOL:
                    tok = tok(lexer, "'capacity', 'nocache', 'cache', 'index' or ')'");

                    if (Chars.equals(tok, "capacity")) {
                        model.symbolCapacity(expectInt(lexer));
                        tok = tok(lexer, "'nocache', 'cache', 'index' or ')'");
                    }

                    if (Chars.equals(tok, "nocache")) {
                        model.cached(false);
                    } else if (Chars.equals(tok, "cache")) {
                        model.cached(true);
                    } else {
                        lexer.unparse();
                    }
                    tok = parseCreateTableInlineIndexDef(lexer, model);
                    break;
                default:
                    tok = null;
                    break;
            }

            if (tok == null) {
                tok = tok(lexer, "',' or ')'");
            }

            if (Chars.equals(tok, ')')) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(lexer, "',' or ')' expected");
            }
        }
    }

    private void parseCreateTableIndexDef(GenericLexer lexer, CreateTableModel model) throws SqlException {
        expectTok(lexer, '(');
        final int columnIndex = getCreateTableColumnIndex(model, expectLiteral(lexer).token, lexer.lastTokenPosition());

        if (Chars.equals(tok(lexer, "'capacity'"), "capacity")) {
            model.setIndexFlags(columnIndex, true, Numbers.ceilPow2(expectInt(lexer)) - 1);
        } else {
            model.setIndexFlags(columnIndex, true, configuration.getIndexValueBlockSize());
            lexer.unparse();
        }
        expectTok(lexer, ')');
    }

    private CharSequence parseCreateTableInlineIndexDef(GenericLexer lexer, CreateTableModel model) throws SqlException {
        CharSequence tok = tok(lexer, "')', or 'index'");

        if (isFieldTerm(tok)) {
            model.setIndexFlags(false, configuration.getIndexValueBlockSize());
            return tok;
        }

        expectTok(lexer, tok, "index");

        if (isFieldTerm(tok = tok(lexer, ") | , expected"))) {
            model.setIndexFlags(true, configuration.getIndexValueBlockSize());
            return tok;
        }

        expectTok(lexer, tok, "capacity");
        model.setIndexFlags(true, expectInt(lexer));
        return null;
    }

    private SqlNode parseCreateTablePartition(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (Chars.equalsNc("partition", tok)) {
            expectTok(lexer, "by");
            return expectLiteral(lexer);
        }
        return null;
    }

    private QueryModel parseDml(GenericLexer lexer) throws SqlException {

        CharSequence tok;
        QueryModel model = queryModelPool.next();

        tok = tok(lexer, "'select', 'with' or table name expected");

        if (Chars.equals(tok, "with")) {
            parseWithClauses(lexer, model);
            tok = tok(lexer, "'select' or table name expected");
        }

        // [select]
        if (Chars.equals(tok, "select")) {
            parseSelectClause(lexer, model);
        } else {
            lexer.unparse();
            // do not default to wildcard column selection when
            // dealing with sub-queries
            if (subQueryMode) {
                parseFromClause(lexer, model, model);
                return model;
            }
            model.addColumn(SqlUtil.nextColumn(queryColumnPool, sqlNodePool, "*", "*"));
        }
        QueryModel nestedModel = queryModelPool.next();
        parseFromClause(lexer, nestedModel, model);
        model.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
        model.setNestedModel(nestedModel);
        return model;
    }

    private void parseFromClause(GenericLexer lexer, QueryModel model, QueryModel masterModel) throws SqlException {
        CharSequence tok = expectTableNameOrSubQuery(lexer);
        // expect "(" in case of sub-query

        if (Chars.equals(tok, '(')) {
            model.setNestedModel(parseSubQuery(lexer));

            tok = optTok(lexer);

            // check if tok is not "where" - should be alias

            if (tok != null && tableAliasStop.excludes(tok)) {
                model.setAlias(literal(lexer, tok));
                tok = optTok(lexer);
            }

            // expect [timestamp(column)]

            SqlNode timestamp = parseTimestamp(lexer, tok);
            if (timestamp != null) {
                model.setTimestamp(timestamp);
                tok = optTok(lexer);
            }
        } else {

            parseSelectFrom(lexer, model, tok, masterModel);

            tok = optTok(lexer);

            if (tok != null && tableAliasStop.excludes(tok)) {
                model.setAlias(literal(lexer, tok));
                tok = optTok(lexer);
            }

            // expect [timestamp(column)]

            SqlNode timestamp = parseTimestamp(lexer, tok);
            if (timestamp != null) {
                model.setTimestamp(timestamp);
                tok = optTok(lexer);
            }

            // expect [latest by]

            if (Chars.equalsNc("latest", tok)) {
                parseLatestBy(lexer, model);
                tok = optTok(lexer);
            }
        }

        // expect multiple [[inner | outer | cross] join]

        int joinType;
        while (tok != null && (joinType = joinStartSet.get(tok)) != -1) {
            model.addJoinModel(parseJoin(lexer, tok, joinType, masterModel));
            tok = optTok(lexer);
        }

        // expect [where]

        if (tok != null && Chars.equals(tok, "where")) {
            model.setWhereClause(expr(lexer));
            tok = optTok(lexer);
        }

        // expect [group by]

        if (tok != null && Chars.equals(tok, "sample")) {
            expectTok(lexer, "by");
            model.setSampleBy(expectLiteral(lexer));
            tok = optTok(lexer);
        }

        // expect [order by]

        if (tok != null && Chars.equals(tok, "order")) {
            expectTok(lexer, "by");
            do {
                SqlNode n = expectLiteral(lexer);

                tok = optTok(lexer);

                if (tok != null && Chars.equalsIgnoreCase(tok, "desc")) {

                    model.addOrderBy(n, QueryModel.ORDER_DIRECTION_DESCENDING);
                    tok = optTok(lexer);

                } else {

                    model.addOrderBy(n, QueryModel.ORDER_DIRECTION_ASCENDING);

                    if (tok != null && Chars.equalsIgnoreCase(tok, "asc")) {
                        tok = optTok(lexer);
                    }
                }

                if (model.getOrderBy().size() >= MAX_ORDER_BY_COLUMNS) {
                    throw err(lexer, "Too many columns");
                }

            } while (tok != null && Chars.equals(tok, ','));
        }

        // expect [limit]
        if (tok != null && Chars.equals(tok, "limit")) {
            SqlNode lo = expr(lexer);
            SqlNode hi = null;

            tok = optTok(lexer);
            if (tok != null && Chars.equals(tok, ',')) {
                hi = expr(lexer);
            } else {
                lexer.unparse();
            }
            model.setLimit(lo, hi);
        } else {
            lexer.unparse();
        }
    }

    private QueryModel parseJoin(GenericLexer lexer, CharSequence tok, int joinType, QueryModel parent) throws SqlException {
        QueryModel joinModel = queryModelPool.next();
        joinModel.setJoinType(joinType);

        if (!Chars.equals(tok, "join")) {
            expectTok(lexer, "join");
        }

        tok = expectTableNameOrSubQuery(lexer);

        if (Chars.equals(tok, '(')) {
            joinModel.setNestedModel(parseSubQuery(lexer));
        } else {
            parseSelectFrom(lexer, joinModel, tok, parent);
        }

        tok = optTok(lexer);

        if (tok != null && tableAliasStop.excludes(tok)) {
            lexer.unparse();
            joinModel.setAlias(expr(lexer));
        } else {
            lexer.unparse();
        }

        tok = optTok(lexer);

        if (joinType == QueryModel.JOIN_CROSS && tok != null && Chars.equals(tok, "on")) {
            throw SqlException.$(lexer.lastTokenPosition(), "Cross joins cannot have join clauses");
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
                expectTok(lexer, tok, "on");
                expressionASTBuilder.reset();
                expressionParser.parseExpr(lexer, expressionASTBuilder);
                SqlNode expr;
                switch (expressionASTBuilder.size()) {
                    case 0:
                        throw SqlException.$(lexer.lastTokenPosition(), "Expression expected");
                    case 1:
                        expr = expressionASTBuilder.poll();
                        if (expr.type == SqlNode.LITERAL) {
                            do {
                                joinModel.addJoinColumn(expr);
                            } while ((expr = expressionASTBuilder.poll()) != null);
                        } else {
                            joinModel.setJoinCriteria(expr);
                        }
                        break;
                    default:
                        // this code handles "join on (a,b,c)", e.g. list of columns
                        while ((expr = expressionASTBuilder.poll()) != null) {
                            if (expr.type != SqlNode.LITERAL) {
                                throw SqlException.$(lexer.lastTokenPosition(), "Column name expected");
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

    private void parseLatestBy(GenericLexer lexer, QueryModel model) throws SqlException {
        expectTok(lexer, "by");
        model.setLatestBy(expr(lexer));
    }

    private ExecutionModel parseRenameStatement(GenericLexer lexer) throws SqlException {
        expectTok(lexer, "table");
        RenameTableModel model = renameTableModelPool.next();
        SqlNode e = expectExpr(lexer);
        if (e.type != SqlNode.LITERAL && e.type != SqlNode.CONSTANT) {
            throw SqlException.$(e.position, "literal or constant expected");
        }
        model.setFrom(e);
        expectTok(lexer, "to");

        e = expectExpr(lexer);
        if (e.type != SqlNode.LITERAL && e.type != SqlNode.CONSTANT) {
            throw SqlException.$(e.position, "literal or constant expected");
        }
        model.setTo(e);
        return model;
    }

    private ExecutionModel parseSelect(GenericLexer lexer) throws SqlException {
        lexer.unparse();
        final QueryModel model = parseDml(lexer);
        final CharSequence tok = optTok(lexer);
        if (tok != null) {
            throw errUnexpected(lexer, tok);
        }
        return model;
    }

    private void parseSelectClause(GenericLexer lexer, QueryModel model) throws SqlException {
        while (true) {
            CharSequence tok = tok(lexer, "column");

            final SqlNode expr;
            // this is quite dramatic workaround for lexer
            // because lexer tokenizes expressions, for something like 'a.*' it would
            // produce two tokens, 'a.' and '*'
            // we should be able to tell if they are together or there is whitespace between them
            // for example "a.  *' would also produce two token and it must be a error
            // to determine if wildcard is correct we would rely on token position
            final char last = tok.charAt(tok.length() - 1);
            if (last == '*') {
                expr = nextLiteral(GenericLexer.immutableOf(tok), lexer.lastTokenPosition());
            } else if (last == '.') {
                // stash 'a.' token
                final int pos = lexer.lastTokenPosition() + tok.length();
                CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
                characterStoreEntry.put(tok);
                tok = tok(lexer, "*");
                if (Chars.equals(tok, '*')) {
                    if (lexer.lastTokenPosition() > pos) {
                        throw SqlException.$(pos, "whitespace is not allowed");
                    }
                    characterStoreEntry.put('*');
                    expr = nextLiteral(characterStoreEntry.toImmutable(), lexer.lastTokenPosition());
                } else {
                    throw SqlException.$(pos, "'*' expected");
                }
            } else {
                lexer.unparse();
                expr = expr(lexer);

                if (expr == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "missing expression");
                }
            }

            CharSequence alias;

            tok = tok(lexer, "',', 'from', 'over' or literal");

            if (columnAliasStop.excludes(tok)) {
                if (Chars.indexOf(tok, '.') != -1) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'.' is not allowed here");
                }
                alias = GenericLexer.immutableOf(tok);
                tok = tok(lexer, "',', 'from' or 'over'");
            } else {
                alias = createColumnAlias(expr, model);
            }

            if (Chars.equals(tok, "over")) {
                // analytic
                expectTok(lexer, '(');

                AnalyticColumn col = analyticColumnPool.next().of(alias, expr);
                tok = tok(lexer, "'");

                if (Chars.equals(tok, "partition")) {
                    expectTok(lexer, "by");

                    ObjList<SqlNode> partitionBy = col.getPartitionBy();

                    do {
                        partitionBy.add(expectLiteral(lexer));
                        tok = tok(lexer, "'order' or ')'");
                    } while (Chars.equals(tok, ','));
                }

                if (Chars.equals(tok, "order")) {
                    expectTok(lexer, "by");

                    do {
                        SqlNode e = expectLiteral(lexer);
                        tok = tok(lexer, "'asc' or 'desc'");

                        if (Chars.equalsIgnoreCase(tok, "desc")) {
                            col.addOrderBy(e, QueryModel.ORDER_DIRECTION_DESCENDING);
                            tok = tok(lexer, "',' or ')'");
                        } else {
                            col.addOrderBy(e, QueryModel.ORDER_DIRECTION_ASCENDING);
                            if (Chars.equalsIgnoreCase(tok, "asc")) {
                                tok = tok(lexer, "',' or ')'");
                            }
                        }
                    } while (Chars.equals(tok, ','));
                }
                expectTok(tok, lexer.lastTokenPosition(), ')');
                model.addColumn(col);
                tok = tok(lexer, "'from' or ','");
            } else {
                model.addColumn(queryColumnPool.next().of(alias, expr));
            }

            if (Chars.equals(tok, "from")) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(lexer, "',' or 'from' expected");
            }
        }
    }

    private void parseSelectFrom(GenericLexer lexer, QueryModel model, CharSequence name, QueryModel masterModel) throws SqlException {
        final SqlNode literal = literal(lexer, name);
        final WithClauseModel withClause = masterModel.getWithClause(name);
        if (withClause != null) {
            model.setNestedModel(parseWith(lexer, withClause));
            model.setAlias(literal);
        } else {
            model.setTableName(literal);
        }
    }

    private QueryModel parseSubQuery(GenericLexer lexer) throws SqlException {
        QueryModel model;
        this.subQueryMode = true;
        try {
            model = parseDml(lexer);
        } finally {
            this.subQueryMode = false;
        }
        expectTok(lexer, ')');
        return model;
    }

    private SqlNode parseTimestamp(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (Chars.equalsNc("timestamp", tok)) {
            expectTok(lexer, '(');
            final SqlNode result = expectLiteral(lexer);
            expectTok(lexer, ')');
            return result;
        }
        return null;
    }

    private QueryModel parseWith(GenericLexer lexer, WithClauseModel wcm) throws SqlException {
        QueryModel m = wcm.popModel();
        if (m != null) {
            return m;
        }

        final int pos = lexer.getPosition();
        lexer.goToPosition(wcm.getPosition());
        // this will not throw exception because this is second pass over the same sub-query
        // we wouldn't be here is syntax was wrong
        m = parseSubQuery(lexer);
        lexer.goToPosition(pos);
        return m;
    }

    private void parseWithClauses(GenericLexer lexer, QueryModel model) throws SqlException {
        do {
            SqlNode name = expectLiteral(lexer);

            if (model.getWithClause(name.token) != null) {
                throw SqlException.$(name.position, "duplicate name");
            }

            expectTok(lexer, "as");
            expectTok(lexer, '(');
            int lo = lexer.lastTokenPosition();
            WithClauseModel wcm = withClauseModelPool.next();
            wcm.of(lo + 1, parseSubQuery(lexer));
            model.addWithClause(name.token, wcm);

            CharSequence tok = optTok(lexer);
            if (tok == null || !Chars.equals(tok, ',')) {
                lexer.unparse();
                break;
            }
        } while (true);
    }


    private SqlNode rewriteCase(SqlNode node) throws SqlException {
        traversalAlgo.traverse(node, node1 -> {
            if (node1.type == SqlNode.FUNCTION && Chars.equals("case", node1.token)) {
                tempExprNodes.clear();
                SqlNode literal = null;
                SqlNode elseExpr;
                boolean convertToSwitch = true;
                final int paramCount = node1.paramCount;
                final int lim;
                if ((paramCount & 1) == 1) {
                    elseExpr = node1.args.getQuick(0);
                    lim = 0;
                } else {
                    elseExpr = null;
                    lim = -1;
                }

                for (int i = paramCount - 1; i > lim; i--) {
                    if ((i & 1) == 1) {
                        // this is "then" clause, copy it as as
                        tempExprNodes.add(node1.args.getQuick(i));
                        continue;
                    }
                    SqlNode where = node1.args.getQuick(i);
                    if (where.type == SqlNode.OPERATION && where.token.charAt(0) == '=') {
                        SqlNode thisConstant;
                        SqlNode thisLiteral;
                        if (where.lhs.type == SqlNode.CONSTANT && where.rhs.type == SqlNode.LITERAL) {
                            thisConstant = where.lhs;
                            thisLiteral = where.rhs;
                        } else if (where.lhs.type == SqlNode.LITERAL && where.rhs.type == SqlNode.CONSTANT) {
                            thisConstant = where.rhs;
                            thisLiteral = where.lhs;
                        } else {
                            convertToSwitch = false;
                            // not supported
                            break;
                        }

                        if (literal == null) {
                            literal = thisLiteral;
                            tempExprNodes.add(thisConstant);
                        } else if (Chars.equals(literal.token, thisLiteral.token)) {
                            tempExprNodes.add(thisConstant);
                        } else {
                            convertToSwitch = false;
                            // not supported
                            break;
                        }
                    } else {
                        convertToSwitch = false;
                        // not supported
                        break;
                    }
                }

                if (convertToSwitch) {
                    node1.token = "switch";
                    node1.args.clear();
                    node1.args.add(literal);
                    node1.args.add(elseExpr);
                    node1.args.addAll(tempExprNodes);
                }
            }
        });
        return node;
    }

    private int toColumnType(GenericLexer lexer, CharSequence tok) throws SqlException {
        final int type = ColumnType.columnTypeOf(tok);
        if (type == -1) {
            throw SqlException.$(lexer.lastTokenPosition(), "unsupported column type: ").put(tok);
        }
        return type;
    }

    private CharSequence tok(GenericLexer lexer, String expectedList) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.lastTokenPosition()).put(expectedList).put(" expected");
        }
        return tok;
    }

    private void validateLiteral(int pos, CharSequence tok) throws SqlException {
        switch (tok.charAt(0)) {
            case '(':
            case ')':
            case ',':
            case '`':
            case '"':
            case '\'':
                throw SqlException.$(pos, "literal expected");
            default:
                break;

        }
    }

    static {
        tableAliasStop.add("where");
        tableAliasStop.add("latest");
        tableAliasStop.add("join");
        tableAliasStop.add("inner");
        tableAliasStop.add("outer");
        tableAliasStop.add("asof");
        tableAliasStop.add("cross");
        tableAliasStop.add("sample");
        tableAliasStop.add("order");
        tableAliasStop.add("on");
        tableAliasStop.add("timestamp");
        tableAliasStop.add("limit");
        tableAliasStop.add(")");
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

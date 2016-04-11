/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.PartitionType;
import com.nfsdb.ex.NumericException;
import com.nfsdb.ex.ParserException;
import com.nfsdb.factory.configuration.GenericIntBuilder;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Numbers;
import com.nfsdb.ql.model.*;
import com.nfsdb.std.CharSequenceHashSet;
import com.nfsdb.std.CharSequenceObjHashMap;
import com.nfsdb.std.ObjectPool;
import com.nfsdb.store.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class QueryParser {

    public static final int MAX_ORDER_BY_COLUMNS = 1560;
    private static final CharSequenceHashSet aliasStopSet = new CharSequenceHashSet();
    private static final CharSequenceHashSet groupByStopSet = new CharSequenceHashSet();
    private static final CharSequenceObjHashMap<QueryModel.JoinType> joinStartSet = new CharSequenceObjHashMap<>();
    private final ObjectPool<ExprNode> exprNodePool = new ObjectPool<>(ExprNode.FACTORY, 128);
    private final Lexer lexer = new Lexer();
    private final ExprParser exprParser = new ExprParser(lexer, exprNodePool);
    private final ExprAstBuilder astBuilder = new ExprAstBuilder();
    private final ObjectPool<QueryModel> queryModelPool = new ObjectPool<>(QueryModel.FACTORY, 8);
    private final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 64);

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

    @SuppressFBWarnings("UCPM_USE_CHARACTER_PARAMETERIZED_METHOD")
    private void expectTok(CharSequence tok, CharSequence expected) throws ParserException {
        if (tok == null || !Chars.equals(tok, expected)) {
            throw QueryError.position(lexer.position()).$('\'').$(expected).$("' expected").$();
        }
    }

    private ExprNode expr() throws ParserException {
        astBuilder.reset();
        exprParser.parseExpr(astBuilder);
        return astBuilder.root();
    }

    private boolean isFieldTerm(CharSequence tok) {
        return Chars.equals(tok, ')') || Chars.equals(tok, ',');
    }

    private ExprNode literal() {
        CharSequence tok = lexer.optionTok();
        if (tok == null) {
            return null;
        }
        return exprNodePool.next().of(ExprNode.NodeType.LITERAL, Chars.stripQuotes(tok.toString()), 0, lexer.position());
    }

    private String notTermTok() throws ParserException {
        CharSequence tok = tok();
        if (isFieldTerm(tok)) {
            throw err("Invalid column definition");
        }
        return tok.toString();
    }

    Statement parse(CharSequence query) throws ParserException {
        queryModelPool.clear();
        queryColumnPool.clear();
        exprNodePool.clear();
        return parseInternal(query);
    }

    private Statement parseCreateJournal() throws ParserException {
        JournalStructure structure = new JournalStructure(tok().toString());
        parseJournalFields(structure);
        CharSequence tok = lexer.optionTok();
        if (tok != null) {
            expectTok(tok, "partition");
            expectTok(tok(), "by");
            structure.partitionBy(PartitionType.valueOf(tok().toString()));
        }
        return new Statement(StatementType.CREATE_JOURNAL, structure);
    }

    private Statement parseCreateStatement() throws ParserException {
        CharSequence tok = tok();
        if (Chars.equals(tok, "journal")) {
            return parseCreateJournal();
        }

        throw err("journal expected");
    }

    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE"})
    private CharSequence parseIntDefinition(GenericIntBuilder genericIntBuilder) throws ParserException {
        CharSequence tok = tok();

        if (isFieldTerm(tok)) {
            return tok;
        }

        expectTok(tok, "index");
        genericIntBuilder.index();

        if (isFieldTerm(tok = tok())) {
            return tok;
        }

        expectTok(tok, "buckets");

        try {
            genericIntBuilder.buckets(Numbers.parseInt(tok()));
        } catch (NumericException e) {
            throw err("expected number of buckets (int)");
        }

        return null;
    }

    Statement parseInternal(CharSequence query) throws ParserException {
        lexer.setContent(query);
        CharSequence tok = tok();
        if (Chars.equals(tok, "create")) {
            return parseCreateStatement();
        }

        lexer.unparse();
        return new Statement(StatementType.QUERY_JOURNAL, parseQuery(false));
    }

    private QueryModel parseJoin(CharSequence tok, QueryModel.JoinType type) throws ParserException {
        QueryModel joinModel = queryModelPool.next();
        joinModel.setJoinType(type);

        if (!Chars.equals(tok, "join")) {
            expectTok(tok(), "join");
        }

        tok = tok();

        if (Chars.equals(tok, '(')) {
            joinModel.setNestedModel(parseQuery(true));
            expectTok(tok(), ")");
        } else {
            lexer.unparse();
            joinModel.setJournalName(expr());
        }

        tok = lexer.optionTok();

        if (tok != null && !aliasStopSet.contains(tok)) {
            lexer.unparse();
            joinModel.setAlias(expr());
        } else {
            lexer.unparse();
        }

        tok = lexer.optionTok();

        if (type == QueryModel.JoinType.CROSS && tok != null && Chars.equals(tok, "on")) {
            throw QueryError.$(lexer.position(), "Cross joins cannot have join clauses");
        }

        switch (type) {
            case ASOF:
                if (tok == null || !Chars.equals("on", tok)) {
                    lexer.unparse();
                    break;
                }
                // intentional fall through
            case INNER:
            case OUTER:
                expectTok(tok, "on");
                ExprNode expr = expr();
                if (expr == null) {
                    throw QueryError.$(lexer.position(), "Expression expected");
                }
                joinModel.setJoinCriteria(expr);
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
            String name = notTermTok();
            CharSequence tok = null;
            switch (ColumnType.valueOf(notTermTok())) {
                case INT:
                    tok = parseIntDefinition(struct.$int(name));
                    break;
                case DOUBLE:
                    struct.$double(name);
                    break;
                case BOOLEAN:
                    struct.$bool(name);
                    break;
                case FLOAT:
                    struct.$float(name);
                    break;
                case LONG:
                    struct.$long(name);
                    break;
                case SHORT:
                    struct.$short(name);
                    break;
                case STRING:
                    struct.$str(name);
                    break;
                case SYMBOL:
                    struct.$sym(name);
                    break;
                case BINARY:
                    struct.$bin(name);
                    break;
                case DATE:
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

    private QueryModel parseQuery(boolean subQuery) throws ParserException {

        CharSequence tok;
        QueryModel model = queryModelPool.next();

        tok = tok();

        // [select]
        if (tok != null && Chars.equals(tok, "select")) {
            parseSelectColumns(model);
            tok = tok();
        }

        // expect "(" in case of sub-query

        if (Chars.equals(tok, '(')) {
            model.setNestedModel(parseQuery(true));

            // expect closing bracket
            expectTok(tok(), ")");

            tok = lexer.optionTok();

            // check if tok is not "where" - should be alias

            if (tok != null && !aliasStopSet.contains(tok)) {
                lexer.unparse();
                model.setAlias(literal());
                tok = lexer.optionTok();
            }

            // expect [timestamp(column)]

            tok = parseTimestamp(tok, model);
        } else {

            lexer.unparse();

            // expect (journal name)

            model.setJournalName(literal());

            tok = lexer.optionTok();

            if (tok != null && !aliasStopSet.contains(tok)) {
                lexer.unparse();
                model.setAlias(literal());
                tok = lexer.optionTok();
            }

            // expect [timestamp(column)]

            tok = parseTimestamp(tok, model);

            // expect [latest by]

            if (tok != null && Chars.equals(tok, "latest")) {
                parseLatestBy(model);
                tok = lexer.optionTok();
            }
        }

        // expect multiple [[inner | outer | cross] join]

        QueryModel.JoinType type;
        while (tok != null && (type = joinStartSet.get(tok)) != null) {
            model.addJoinModel(parseJoin(tok, type));
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
            model.setSampleBy(expectExpr());
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
                ExprNode n = expectExpr();

                if (n.type != ExprNode.NodeType.LITERAL) {
                    throw err("Column name expected");
                }

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
        return model;
    }

    private void parseSelectColumns(QueryModel model) throws ParserException {
        CharSequence tok;
        while (true) {
            ExprNode expr = expr();
            tok = tok();

            // expect (from | , | [column name])

            if (Chars.equals(tok, "from")) {
                model.addColumn(queryColumnPool.next().of(null, expr));
                break;
            }

            if (Chars.equals(tok, ',')) {
                model.addColumn(queryColumnPool.next().of(null, expr));
                continue;
            }

            model.addColumn(queryColumnPool.next().of(tok.toString(), expr));

            tok = tok();

            // expect (from | , )

            if (Chars.equals(tok, "from")) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(",|from expected");
            }
        }
    }

    private CharSequence parseTimestamp(CharSequence tok, QueryModel model) throws ParserException {
        if (tok != null && Chars.equals(tok, "timestamp")) {
            expectTok(tok(), "(");
            model.setTimestamp(expr());
            expectTok(tok(), ")");
            return lexer.optionTok();
        }
        return tok;
    }

    private CharSequence tok() throws ParserException {
        CharSequence tok = lexer.optionTok();
        if (tok == null) {
            throw err("Unexpected end of input");
        }
        return tok;
    }

    static {
        aliasStopSet.add("where");
        aliasStopSet.add("latest");
        aliasStopSet.add("join");
        aliasStopSet.add("inner");
        aliasStopSet.add("outer");
        aliasStopSet.add("asof");
        aliasStopSet.add("cross");
        aliasStopSet.add("sample");
        aliasStopSet.add("order");
        aliasStopSet.add("on");
        aliasStopSet.add("timestamp");
        aliasStopSet.add("limit");
        aliasStopSet.add(")");
        //
        groupByStopSet.add("order");
        groupByStopSet.add(")");
        groupByStopSet.add(",");

        joinStartSet.put("join", QueryModel.JoinType.INNER);
        joinStartSet.put("inner", QueryModel.JoinType.INNER);
        joinStartSet.put("outer", QueryModel.JoinType.OUTER);
        joinStartSet.put("cross", QueryModel.JoinType.CROSS);
        joinStartSet.put("asof", QueryModel.JoinType.ASOF);
    }
}

/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.parser;

import com.nfsdb.PartitionType;
import com.nfsdb.collections.CharSequenceHashSet;
import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.ObjectPool;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.exceptions.ParserException;
import com.nfsdb.factory.configuration.GenericIntBuilder;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.ql.model.*;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Numbers;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

final class QueryParser {

    private static final CharSequenceHashSet aliasStopSet = new CharSequenceHashSet();
    private static final CharSequenceHashSet groupByStopSet = new CharSequenceHashSet();
    private static final CharSequenceObjHashMap<QueryModel.JoinType> joinStartSet = new CharSequenceObjHashMap<>();
    private final Lexer toks = new Lexer();
    private final ExprParser exprParser = new ExprParser(toks);
    private final ExprAstBuilder astBuilder = new ExprAstBuilder();
    private final ObjectPool<QueryModel> queryModelPool = new ObjectPool<>(QueryModel.FACTORY, 8);
    private final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 64);

    private ParserException err(String msg) {
        return new ParserException(toks.position(), msg);
    }

    private ExprNode expectExpr() throws ParserException {
        ExprNode n = expr();
        if (n == null) {
            throw new ParserException(toks.position(), "Expression expected");
        }
        return n;
    }

    @SuppressFBWarnings("UCPM_USE_CHARACTER_PARAMETERIZED_METHOD")
    private void expectTok(CharSequence tok, CharSequence expected) throws ParserException {
        if (tok == null || !Chars.equals(tok, expected)) {
            throw err("\"" + expected + "\" expected");
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

    private String notTermTok() throws ParserException {
        CharSequence tok = tok();
        if (isFieldTerm(tok)) {
            throw err("Invalid column definition");
        }
        return tok.toString();
    }

    private CharSequence optionTok() {
        while (toks.hasNext()) {
            CharSequence cs = toks.next();
            if (!Chars.equals(cs, ' ')) {
                return cs;
            }
        }
        return null;
    }

    Statement parse(CharSequence query) throws ParserException {
        queryModelPool.reset();
        queryColumnPool.reset();
        return parseInternal(query);
    }

    private Statement parseCreateJournal() throws ParserException {
        JournalStructure structure = new JournalStructure(tok().toString());
        parseJournalFields(structure);
        CharSequence tok = optionTok();
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
        toks.setContent(query);
        CharSequence tok = tok();
        if (Chars.equals(tok, "create")) {
            return parseCreateStatement();
        }

        toks.unparse();
        return new Statement(StatementType.QUERY_JOURNAL, parseQuery(false));
    }

    private QueryModel parseJoin(CharSequence tok, QueryModel.JoinType type) throws ParserException {
        QueryModel joinModel = queryModelPool.next();
        joinModel.setJoinType(type);

        if (!Chars.equals(tok, "join")) {
            expectTok(tok(), "join");
        }

        tok = tok();

        if (Chars.equals(tok, "(")) {
            joinModel.setNestedModel(parseQuery(true));
            expectTok(tok(), ")");
        } else {
            toks.unparse();
            joinModel.setJournalName(expr());
        }

        tok = optionTok();

        if (tok != null && !aliasStopSet.contains(tok)) {
            toks.unparse();
            joinModel.setAlias(expr());
        } else {
            toks.unparse();
        }

        tok = optionTok();

        if (type == QueryModel.JoinType.CROSS && tok != null && Chars.equals(tok, "on")) {
            throw new ParserException(toks.position(), "Cross joins cannot have join clauses");
        }

        switch (type) {
            case ASOF:
                if (tok == null || !Chars.equals("on", tok)) {
                    toks.unparse();
                    break;
                }
                // intentional fall through
            case INNER:
            case OUTER:
                expectTok(tok, "on");
                ExprNode expr = expr();
                if (expr == null) {
                    throw new ParserException(toks.position(), "Expression expected");
                }
                joinModel.setJoinCriteria(expr);
                break;
            default:
                toks.unparse();
        }
//
//        if (type != QueryModel.JoinType.INNER) {
//            expectTok(tok, "on");
//            ExprNode expr = expr();
//            if (expr == null) {
//                throw new ParserException(toks.position(), "Expression expected");
//            }
//            joinModel.setJoinCriteria(expr);
//        } else {
//            toks.unparse();
//        }

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

        if (Chars.equals(tok, "(")) {
            model.setNestedModel(parseQuery(true));

            // expect closing bracket
            expectTok(tok(), ")");

            tok = optionTok();

            // check if tok is not "where" - should be alias

            if (tok != null && !aliasStopSet.contains(tok)) {
                toks.unparse();
                model.setAlias(expr());
                tok = optionTok();
            }

            // expect [timestamp(column)]

            tok = parseTimestamp(tok, model);
        } else {

            toks.unparse();

            // expect (journal name)

            model.setJournalName(expr());

            tok = optionTok();

            if (tok != null && !aliasStopSet.contains(tok)) {
                toks.unparse();
                model.setAlias(expr());
                tok = optionTok();
            }

            // expect [timestamp(column)]

            tok = parseTimestamp(tok, model);

            // expect [latest by]

            if (tok != null && Chars.equals(tok, "latest")) {
                parseLatestBy(model);
                tok = optionTok();
            }
        }

        // expect multiple [[inner | outer | cross] join]

        QueryModel.JoinType type;
        while (tok != null && (type = joinStartSet.get(tok)) != null) {
            model.addJoinModel(parseJoin(tok, type));
            tok = optionTok();
        }

        // expect [where]

        if (tok != null && Chars.equals(tok, "where")) {
            model.setWhereClause(expr());
            tok = optionTok();
        }

        // expect [group by]

        if (tok != null && Chars.equals(tok, "sample")) {
            expectTok(tok(), "by");
            model.setSampleBy(expectExpr());
            tok = optionTok();
        }

        // expect [order by]

        if (tok != null && Chars.equals(tok, "order")) {
            expectTok(tok(), "by");
            do {
                tok = tok();

                if (Chars.equals(tok, ")")) {
                    throw err("Expression expected");
                }

                toks.unparse();
                model.addOrderBy(expr());
                tok = optionTok();
            } while (tok != null && Chars.equals(tok, ","));
        }

        // expect [limit]
        if (tok != null && Chars.equals(tok, "limit")) {
            ExprNode lo = expr();
            ExprNode hi = null;

            tok = optionTok();
            if (tok != null && Chars.equals(",", tok)) {
                hi = expr();
                tok = optionTok();
            }
            model.setLimit(lo, hi);
        }

        if (subQuery) {
            toks.unparse();
        } else if (tok != null) {
            throw err("Unexpected token: " + tok);
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

            if (Chars.equals(tok, ",")) {
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
            return optionTok();
        }
        return tok;
    }

    private CharSequence tok() throws ParserException {
        CharSequence tok = toks.optionTok();
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

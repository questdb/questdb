/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.PartitionBy;
import com.questdb.cairo.TableUtils;
import com.questdb.griffin.model.*;
import com.questdb.std.*;
import org.jetbrains.annotations.NotNull;

public final class SqlParser {

    public static final int MAX_ORDER_BY_COLUMNS = 1560;
    private static final LowerCaseAsciiCharSequenceHashSet tableAliasStop = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet columnAliasStop = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet groupByStopSet = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceIntHashMap joinStartSet = new LowerCaseAsciiCharSequenceIntHashMap();

    static {
        tableAliasStop.add("where");
        tableAliasStop.add("latest");
        tableAliasStop.add("join");
        tableAliasStop.add("inner");
        tableAliasStop.add("left");
        tableAliasStop.add("outer");
        tableAliasStop.add("asof");
        tableAliasStop.add("splice");
        tableAliasStop.add("cross");
        tableAliasStop.add("sample");
        tableAliasStop.add("order");
        tableAliasStop.add("on");
        tableAliasStop.add("timestamp");
        tableAliasStop.add("limit");
        tableAliasStop.add(")");
        tableAliasStop.add(";");
        //
        columnAliasStop.add("from");
        columnAliasStop.add(",");
        columnAliasStop.add("over");
        //
        groupByStopSet.add("order");
        groupByStopSet.add(")");
        groupByStopSet.add(",");

        joinStartSet.put("left", QueryModel.JOIN_INNER);
        joinStartSet.put("join", QueryModel.JOIN_INNER);
        joinStartSet.put("inner", QueryModel.JOIN_INNER);
        joinStartSet.put("outer", QueryModel.JOIN_OUTER);
        joinStartSet.put("cross", QueryModel.JOIN_CROSS);
        joinStartSet.put("asof", QueryModel.JOIN_ASOF);
        joinStartSet.put("splice", QueryModel.JOIN_SPLICE);
    }

    private final ObjectPool<ExpressionNode> sqlNodePool;
    private final ExpressionTreeBuilder expressionTreeBuilder = new ExpressionTreeBuilder();
    private final ObjectPool<QueryModel> queryModelPool;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<AnalyticColumn> analyticColumnPool = new ObjectPool<>(AnalyticColumn.FACTORY, 8);
    private final ObjectPool<CreateTableModel> createTableModelPool = new ObjectPool<>(CreateTableModel.FACTORY, 4);
    private final ObjectPool<ColumnCastModel> columnCastModelPool = new ObjectPool<>(ColumnCastModel.FACTORY, 8);
    private final ObjectPool<RenameTableModel> renameTableModelPool = new ObjectPool<>(RenameTableModel.FACTORY, 8);
    private final ObjectPool<WithClauseModel> withClauseModelPool = new ObjectPool<>(WithClauseModel.FACTORY, 16);
    private final ObjectPool<InsertAsSelectModel> insertAsSelectModelPool = new ObjectPool<>(InsertAsSelectModel.FACTORY, 2);
    private final ExpressionParser expressionParser;
    private final CairoConfiguration configuration;
    private final PostOrderTreeTraversalAlgo traversalAlgo;
    private final ObjList<ExpressionNode> tempExprNodes = new ObjList<>();
    private final CharacterStore characterStore;
    private final SqlOptimiser optimiser;
    private boolean subQueryMode = false;

    SqlParser(
            CairoConfiguration configuration,
            SqlOptimiser optimiser,
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> sqlNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            PostOrderTreeTraversalAlgo traversalAlgo) {
        this.sqlNodePool = sqlNodePool;
        this.queryModelPool = queryModelPool;
        this.queryColumnPool = queryColumnPool;
        this.configuration = configuration;
        this.traversalAlgo = traversalAlgo;
        this.characterStore = characterStore;
        this.optimiser = optimiser;
        this.expressionParser = new ExpressionParser(sqlNodePool, this);
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
        insertAsSelectModelPool.clear();
        expressionTreeBuilder.reset();
    }

    private CharSequence createColumnAlias(ExpressionNode node, QueryModel model) {
        return SqlUtil.createColumnAlias(characterStore, node.token, Chars.indexOf(node.token, '.'), model.getColumnNameTypeMap());
    }

    private ExpressionNode expectExpr(GenericLexer lexer) throws SqlException {
        ExpressionNode n = expr(lexer, (QueryModel) null);
        if (n == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "Expression expected");
        }
        return n;
    }

    private int expectInt(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "integer");
        boolean negative;
        if (Chars.equals(tok, '-')) {
            negative = true;
            tok = tok(lexer, "integer");
        } else {
            negative = false;
        }
        try {
            int result = Numbers.parseInt(tok);
            return negative ? -result : result;
        } catch (NumericException e) {
            throw err(lexer, "bad integer");
        }
    }

    private ExpressionNode expectLiteral(GenericLexer lexer) throws SqlException {
        return expectLiteral(lexer, "literal");
    }

    private ExpressionNode expectLiteral(GenericLexer lexer, String expectedList) throws SqlException {
        CharSequence tok = tok(lexer, expectedList);
        int pos = lexer.lastTokenPosition();
        validateLiteral(pos, tok, expectedList);
        return nextLiteral(GenericLexer.immutableOf(tok), pos);
    }

    private CharSequence expectTableNameOrSubQuery(GenericLexer lexer) throws SqlException {
        return tok(lexer, "table name or sub-query");
    }

    private void expectTok(GenericLexer lexer, CharSequence tok, CharSequence expected) throws SqlException {
        if (tok == null || !Chars.equalsLowerCaseAscii(tok, expected)) {
            throw SqlException.position(lexer.lastTokenPosition()).put('\'').put(expected).put("' expected");
        }
    }

    private void expectTok(GenericLexer lexer, CharSequence expected) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put('\'').put(expected).put("' expected");
        }
        expectTok(lexer, tok, expected);
    }

    private void expectTok(GenericLexer lexer, char expected) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put('\'').put(expected).put("' expected");
        }
        expectTok(tok, lexer.lastTokenPosition(), expected);
    }

    private void expectTok(CharSequence tok, int pos, char expected) throws SqlException {
        if (tok == null || !Chars.equals(tok, expected)) {
            throw SqlException.position(pos).put('\'').put(expected).put("' expected");
        }
    }

    ExpressionNode expr(GenericLexer lexer, QueryModel model) throws SqlException {
        try {
            expressionTreeBuilder.pushModel(model);
            expressionParser.parseExpr(lexer, expressionTreeBuilder);
            return rewriteCase(expressionTreeBuilder.poll());
        } catch (SqlException e) {
            expressionTreeBuilder.reset();
            throw e;
        } finally {
            expressionTreeBuilder.popModel();
        }
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

    private ExpressionNode literal(GenericLexer lexer, CharSequence name) {
        return literal(name, lexer.lastTokenPosition());
    }

    private ExpressionNode literal(CharSequence name, int position) {
        // this can never be null in its current contexts
        // every time this function is called is after lexer.unparse(), which ensures non-null token.
        return sqlNodePool.next().of(ExpressionNode.LITERAL, GenericLexer.unquote(name), 0, position);
    }

    private ExpressionNode nextLiteral(CharSequence token, int position) {
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

    ExecutionModel parse(GenericLexer lexer, SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok = tok(lexer, "'create', 'rename' or 'select'");

        if (Chars.equalsLowerCaseAscii(tok, "select")) {
            return parseSelect(lexer);
        }

        if (Chars.equalsLowerCaseAscii(tok, "create")) {
            return parseCreateStatement(lexer, executionContext);
        }

        if (Chars.equalsLowerCaseAscii(tok, "rename")) {
            return parseRenameStatement(lexer);
        }

        if (Chars.equalsLowerCaseAscii(tok, "insert")) {
            return parseInsertAsSelect(lexer);
        }

        return parseSelect(lexer);
    }

    private ExecutionModel parseCreateStatement(GenericLexer lexer, SqlExecutionContext executionContext) throws SqlException {
        expectTok(lexer, "table");
        return parseCreateTable(lexer, executionContext);
    }

    private ExecutionModel parseCreateTable(GenericLexer lexer, SqlExecutionContext executionContext) throws SqlException {
        final CreateTableModel model = createTableModelPool.next();
        final CharSequence tableName = tok(lexer, "table name");
        if (Chars.indexOf(tableName, '.') != -1) {
            throw SqlException.$(lexer.lastTokenPosition(), "'.' is not allowed here");
        }
        model.setName(nextLiteral(GenericLexer.unquote(tableName), lexer.lastTokenPosition()));

        CharSequence tok = tok(lexer, "'(' or 'as'");

        if (Chars.equals(tok, '(')) {
            lexer.unparse();
            parseCreateTableColumns(lexer, model);
        } else if (Chars.equalsLowerCaseAscii(tok, "as")) {
            parseCreateTableAsSelect(lexer, model, executionContext);
        } else {
            throw errUnexpected(lexer, tok);
        }

        while ((tok = optTok(lexer)) != null && Chars.equals(tok, ',')) {
            tok = tok(lexer, "'index' or 'cast'");
            if (Chars.equalsLowerCaseAscii(tok, "index")) {
                parseCreateTableIndexDef(lexer, model);
            } else if (Chars.equalsLowerCaseAscii(tok, "cast")) {
                parseCreateTableCastDef(lexer, model);
            } else {
                throw errUnexpected(lexer, tok);
            }
        }

        ExpressionNode timestamp = parseTimestamp(lexer, tok);
        if (timestamp != null) {
            // ignore index, validate column
            getCreateTableColumnIndex(model, timestamp.token, timestamp.position);
            model.setTimestamp(timestamp);
            tok = optTok(lexer);
        }

        ExpressionNode partitionBy = parseCreateTablePartition(lexer, tok);
        if (partitionBy != null) {
            if (PartitionBy.fromString(partitionBy.token) == -1) {
                throw SqlException.$(partitionBy.position, "'NONE', 'DAY', 'MONTH' or 'YEAR' expected");
            }
            model.setPartitionBy(partitionBy);
            tok = optTok(lexer);
        }

        if (tok == null || Chars.equals(tok, ';')) {
            return model;
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseCreateTableAsSelect(GenericLexer lexer, CreateTableModel model, SqlExecutionContext executionContext) throws SqlException {
        expectTok(lexer, '(');
        QueryModel queryModel = optimiser.optimise(parseDml(lexer), executionContext);
        ObjList<QueryColumn> columns = queryModel.getColumns();
        assert columns.size() > 0;

        // we do not know types of columns at this stage
        // compiler must put table together using query metadata.
        for (int i = 0, n = columns.size(); i < n; i++) {
            model.addColumn(columns.getQuick(i).getName(), -1, configuration.getDefaultSymbolCapacity());
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

        final ExpressionNode columnName = expectLiteral(lexer);
        columnCastModel.setName(columnName);
        expectTok(lexer, "as");

        final ExpressionNode columnType = expectLiteral(lexer);
        final int type = toColumnType(lexer, columnType.token);
        columnCastModel.setType(type, columnName.position, columnType.position);

        if (type == ColumnType.SYMBOL) {
            CharSequence tok = tok(lexer, "'capacity', 'nocache', 'cache', 'index' or ')'");

            int symbolCapacity;
            int capacityPosition;
            if (Chars.equalsLowerCaseAscii(tok, "capacity")) {
                capacityPosition = lexer.getPosition();
                columnCastModel.setSymbolCapacity(symbolCapacity = parseSymbolCapacity(lexer));
                tok = tok(lexer, "'nocache', 'cache', 'index' or ')'");
            } else {
                columnCastModel.setSymbolCapacity(configuration.getDefaultSymbolCapacity());
                symbolCapacity = -1;
                capacityPosition = -1;
            }

            final boolean cached;
            if (Chars.equalsLowerCaseAscii(tok, "nocache")) {
                cached = false;
            } else if (Chars.equalsLowerCaseAscii(tok, "cache")) {
                cached = true;
            } else {
                cached = configuration.getDefaultSymbolCacheFlag();
                lexer.unparse();
            }

            columnCastModel.setSymbolCacheFlag(cached);

            if (cached && symbolCapacity != -1) {
                assert capacityPosition != -1;
                TableUtils.validateSymbolCapacityCached(true, symbolCapacity, capacityPosition);
            }

            // parse index clause

            tok = tok(lexer, "')', or 'index'");

            if (Chars.equalsLowerCaseAscii(tok, "index")) {
                columnCastModel.setIndexed(true);
                tok = tok(lexer, "')', or 'capacity'");

                if (Chars.equalsLowerCaseAscii(tok, "capacity")) {
                    int errorPosition = lexer.getPosition();
                    int indexValueBlockSize = expectInt(lexer);
                    TableUtils.validateIndexValueBlockSize(errorPosition, indexValueBlockSize);
                    columnCastModel.setIndexValueBlockSize(Numbers.ceilPow2(indexValueBlockSize));
                } else {
                    columnCastModel.setIndexValueBlockSize(configuration.getIndexValueBlockSize());
                }
            } else {
                columnCastModel.setIndexed(false);
                lexer.unparse();
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

            if (!model.addColumn(name, type, configuration.getDefaultSymbolCapacity())) {
                throw SqlException.$(position, "Duplicate column");
            }

            CharSequence tok;
            if (type == ColumnType.SYMBOL) {
                tok = tok(lexer, "'capacity', 'nocache', 'cache', 'index' or ')'");

                int symbolCapacity;
                if (Chars.equalsLowerCaseAscii(tok, "capacity")) {
                    // when capacity is not set explicitly it will default via configuration
                    model.symbolCapacity(symbolCapacity = parseSymbolCapacity(lexer));
                    tok = tok(lexer, "'nocache', 'cache', 'index' or ')'");
                } else {
                    symbolCapacity = -1;
                }

                final boolean cached;
                if (Chars.equalsLowerCaseAscii(tok, "nocache")) {
                    cached = false;
                } else if (Chars.equalsLowerCaseAscii(tok, "cache")) {
                    cached = true;
                } else {
                    cached = configuration.getDefaultSymbolCacheFlag();
                    lexer.unparse();
                }
                model.cached(cached);
                if (cached && symbolCapacity != -1) {
                    TableUtils.validateSymbolCapacityCached(true, symbolCapacity, lexer.lastTokenPosition());
                }
                tok = parseCreateTableInlineIndexDef(lexer, model);
            } else {
                tok = null;
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

        if (Chars.equalsLowerCaseAscii(tok(lexer, "'capacity'"), "capacity")) {
            int errorPosition = lexer.getPosition();
            int indexValueBlockSize = expectInt(lexer);
            TableUtils.validateIndexValueBlockSize(errorPosition, indexValueBlockSize);
            model.setIndexFlags(columnIndex, true, Numbers.ceilPow2(indexValueBlockSize));
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

        int errorPosition = lexer.getPosition();
        int indexValueBlockSize = expectInt(lexer);
        TableUtils.validateIndexValueBlockSize(errorPosition, indexValueBlockSize);
        model.setIndexFlags(true, Numbers.ceilPow2(indexValueBlockSize));
        return null;
    }

    private ExpressionNode parseCreateTablePartition(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (Chars.equalsLowerCaseAsciiNc(tok, "partition")) {
            expectTok(lexer, "by");
            return expectLiteral(lexer);
        }
        return null;
    }

    private QueryModel parseDml(GenericLexer lexer) throws SqlException {

        CharSequence tok;
        QueryModel model = queryModelPool.next();

        tok = tok(lexer, "'select', 'with' or table name expected");

        if (Chars.equalsLowerCaseAscii(tok, "with")) {
            parseWithClauses(lexer, model);
            tok = tok(lexer, "'select' or table name expected");
        }

        // [select]
        if (Chars.equalsLowerCaseAscii(tok, "select")) {
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
        if (nestedModel.getLimitHi() != null || nestedModel.getLimitLo() != null) {
            model.setLimit(nestedModel.getLimitLo(), nestedModel.getLimitHi());
            nestedModel.setLimit(null, null);
        }
        if (
                nestedModel.getColumns().size() == 0
                        && nestedModel.getNestedModel() != null
                        && nestedModel.getWhereClause() == null
                        && nestedModel.getSampleBy() == null
                        && nestedModel.getJoinModels().size() == 1
                        && nestedModel.getTimestamp() == null
                        && nestedModel.getOrderBy().size() == 0
                        && nestedModel.getAlias() == null
        ) {
            nestedModel = nestedModel.getNestedModel();
        } else {
            model.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
        }
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

            ExpressionNode timestamp = parseTimestamp(lexer, tok);
            if (timestamp != null) {
                model.setTimestamp(timestamp);
                tok = optTok(lexer);
            }
        } else {

            lexer.unparse();
            parseSelectFrom(lexer, model, masterModel);

            tok = optTok(lexer);

            if (tok != null && tableAliasStop.excludes(tok)) {
                model.setAlias(literal(lexer, tok));
                tok = optTok(lexer);
            }

            // expect [timestamp(column)]

            ExpressionNode timestamp = parseTimestamp(lexer, tok);
            if (timestamp != null) {
                model.setTimestamp(timestamp);
                tok = optTok(lexer);
            }

            // expect [latest by]

            if (Chars.equalsLowerCaseAsciiNc(tok, "latest")) {
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

        if (tok != null && Chars.equalsLowerCaseAscii(tok, "where")) {
            model.setWhereClause(expr(lexer, model));
            tok = optTok(lexer);
        }

        // expect [group by]

        if (tok != null && Chars.equalsLowerCaseAscii(tok, "sample")) {
            expectTok(lexer, "by");
            model.setSampleBy(expectLiteral(lexer));
            tok = optTok(lexer);

            if (tok != null && Chars.equalsLowerCaseAscii(tok, "fill")) {
                expectTok(lexer, '(');
                do {
                    final ExpressionNode fillNode = expectLiteral(lexer, "'none', 'prev', 'mid', 'null' or number");
                    model.addSampleByFill(fillNode);
                    tok = tok(lexer, "',' or ')'");
                    if (Chars.equals(tok, ')')) {
                        break;
                    }
                    expectTok(tok, lexer.lastTokenPosition(), ',');
                } while (true);
                tok = optTok(lexer);
            }
        }

        // expect [order by]

        if (tok != null && Chars.equalsLowerCaseAscii(tok, "order")) {
            expectTok(lexer, "by");
            do {
                ExpressionNode n = expectLiteral(lexer);

                tok = optTok(lexer);

                if (tok != null && Chars.equalsLowerCaseAscii(tok, "desc")) {

                    model.addOrderBy(n, QueryModel.ORDER_DIRECTION_DESCENDING);
                    tok = optTok(lexer);

                } else {

                    model.addOrderBy(n, QueryModel.ORDER_DIRECTION_ASCENDING);

                    if (tok != null && Chars.equalsLowerCaseAscii(tok, "asc")) {
                        tok = optTok(lexer);
                    }
                }

                if (model.getOrderBy().size() >= MAX_ORDER_BY_COLUMNS) {
                    throw err(lexer, "Too many columns");
                }

            } while (tok != null && Chars.equals(tok, ','));
        }

        // expect [limit]
        if (tok != null && Chars.equalsLowerCaseAscii(tok, "limit")) {
            ExpressionNode lo = expr(lexer, model);
            ExpressionNode hi = null;

            tok = optTok(lexer);
            if (tok != null && Chars.equals(tok, ',')) {
                hi = expr(lexer, model);
            } else {
                lexer.unparse();
            }
            model.setLimit(lo, hi);
        } else {
            lexer.unparse();
        }
    }

    private ExecutionModel parseInsertAsSelect(GenericLexer lexer) throws SqlException {
        expectTok(lexer, "into");

        final InsertAsSelectModel model = insertAsSelectModelPool.next();
        model.setTableName(expectLiteral(lexer));

        CharSequence tok = tok(lexer, "'(' or 'select'");

        if (Chars.equals(tok, '(')) {
            do {
                tok = tok(lexer, "column");
                if (Chars.equals(tok, ')')) {
                    throw err(lexer, "missing column name");
                }

                if (!model.addColumn(GenericLexer.immutableOf(tok), lexer.lastTokenPosition())) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("duplicate column name: ").put(tok);
                }

            } while (Chars.equals((tok = tok(lexer, "','")), ','));

            expectTok(tok, lexer.lastTokenPosition(), ')');
            tok = optTok(lexer);
        }

        if (tok == null) {
            throw SqlException.$(lexer.getPosition(), "'select' expected");
        }

        if (Chars.equalsLowerCaseAscii(tok, "select")) {
            model.setSelectKeywordPosition(lexer.lastTokenPosition());
            lexer.unparse();
            final QueryModel queryModel = parseDml(lexer);
            model.setQueryModel(queryModel);
            return model;
        }

        throw err(lexer, "'select' expected");
    }

    private QueryModel parseJoin(GenericLexer lexer, CharSequence tok, int joinType, QueryModel parent) throws SqlException {
        QueryModel joinModel = queryModelPool.next();
        joinModel.setJoinType(joinType);
        joinModel.setJoinKeywordPosition(lexer.lastTokenPosition());

        if (!Chars.equalsLowerCaseAscii(tok, "join")) {
            expectTok(lexer, "join");
        }

        tok = expectTableNameOrSubQuery(lexer);

        if (Chars.equals(tok, '(')) {
            joinModel.setNestedModel(parseSubQuery(lexer));
        } else {
            lexer.unparse();
            parseSelectFrom(lexer, joinModel, parent);
        }

        tok = optTok(lexer);

        if (tok != null && tableAliasStop.excludes(tok)) {
            lexer.unparse();
            joinModel.setAlias(literal(lexer, optTok(lexer)));
        } else {
            lexer.unparse();
        }

        tok = optTok(lexer);

        if (joinType == QueryModel.JOIN_CROSS && tok != null && Chars.equalsLowerCaseAscii(tok, "on")) {
            throw SqlException.$(lexer.lastTokenPosition(), "Cross joins cannot have join clauses");
        }

        switch (joinType) {
            case QueryModel.JOIN_ASOF:
            case QueryModel.JOIN_SPLICE:
                if (tok == null || !Chars.equalsLowerCaseAscii(tok, "on")) {
                    lexer.unparse();
                    break;
                }
                // intentional fall through
            case QueryModel.JOIN_INNER:
            case QueryModel.JOIN_OUTER:
                expectTok(lexer, tok, "on");
                try {
                    expressionParser.parseExpr(lexer, expressionTreeBuilder);
                    ExpressionNode expr;
                    switch (expressionTreeBuilder.size()) {
                        case 0:
                            throw SqlException.$(lexer.lastTokenPosition(), "Expression expected");
                        case 1:
                            expr = expressionTreeBuilder.poll();
                            if (expr.type == ExpressionNode.LITERAL) {
                                do {
                                    joinModel.addJoinColumn(expr);
                                } while ((expr = expressionTreeBuilder.poll()) != null);
                            } else {
                                joinModel.setJoinCriteria(rewriteCase(expr));
                            }
                            break;
                        default:
                            // this code handles "join on (a,b,c)", e.g. list of columns
                            while ((expr = expressionTreeBuilder.poll()) != null) {
                                if (expr.type != ExpressionNode.LITERAL) {
                                    throw SqlException.$(lexer.lastTokenPosition(), "Column name expected");
                                }
                                joinModel.addJoinColumn(expr);
                            }
                            break;
                    }
                } catch (SqlException e) {
                    expressionTreeBuilder.reset();
                    throw e;
                }
                break;
            default:
                lexer.unparse();
                break;
        }

        return joinModel;
    }

    private void parseLatestBy(GenericLexer lexer, QueryModel model) throws SqlException {
        expectTok(lexer, "by");
        model.setLatestBy(expr(lexer, model));
    }

    private ExecutionModel parseRenameStatement(GenericLexer lexer) throws SqlException {
        expectTok(lexer, "table");
        RenameTableModel model = renameTableModelPool.next();
        // todo: review and test
        ExpressionNode e = expectExpr(lexer);
        if (e.type != ExpressionNode.LITERAL && e.type != ExpressionNode.CONSTANT) {
            throw SqlException.$(e.position, "literal or constant expected");
        }
        model.setFrom(e);
        expectTok(lexer, "to");

        e = expectExpr(lexer);
        if (e.type != ExpressionNode.LITERAL && e.type != ExpressionNode.CONSTANT) {
            throw SqlException.$(e.position, "literal or constant expected");
        }
        model.setTo(e);
        return model;
    }

    private ExecutionModel parseSelect(GenericLexer lexer) throws SqlException {
        lexer.unparse();
        final QueryModel model = parseDml(lexer);
        final CharSequence tok = optTok(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            return model;
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseSelectClause(GenericLexer lexer, QueryModel model) throws SqlException {
        while (true) {
            CharSequence tok = tok(lexer, "column");

            final ExpressionNode expr;
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
                // cut off some obvious errors
                if (Chars.equalsLowerCaseAscii(tok, "from")) {
                    throw SqlException.$(lexer.getPosition(), "column name expected");
                }

                if (Chars.equalsLowerCaseAscii(tok, "select")) {
                    throw SqlException.$(lexer.getPosition(), "reserved name");
                }

                lexer.unparse();
                expr = expr(lexer, model);

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
                if (Chars.equalsLowerCaseAscii(tok, "as")) {
                    alias = GenericLexer.unquote(GenericLexer.immutableOf(tok(lexer, "alias")));
                } else {
                    alias = GenericLexer.immutableOf(tok);
                }
                tok = tok(lexer, "',', 'from' or 'over'");
            } else {
                alias = createColumnAlias(expr, model);
            }

            if (Chars.equalsLowerCaseAscii(tok, "over")) {
                // analytic
                expectTok(lexer, '(');

                AnalyticColumn col = analyticColumnPool.next().of(alias, expr);
                tok = tok(lexer, "'");

                if (Chars.equalsLowerCaseAscii(tok, "partition")) {
                    expectTok(lexer, "by");

                    ObjList<ExpressionNode> partitionBy = col.getPartitionBy();

                    do {
                        partitionBy.add(expectLiteral(lexer));
                        tok = tok(lexer, "'order' or ')'");
                    } while (Chars.equals(tok, ','));
                }

                if (Chars.equalsLowerCaseAscii(tok, "order")) {
                    expectTok(lexer, "by");

                    do {
                        ExpressionNode e = expectLiteral(lexer);
                        tok = tok(lexer, "'asc' or 'desc'");

                        if (Chars.equalsLowerCaseAscii(tok, "desc")) {
                            col.addOrderBy(e, QueryModel.ORDER_DIRECTION_DESCENDING);
                            tok = tok(lexer, "',' or ')'");
                        } else {
                            col.addOrderBy(e, QueryModel.ORDER_DIRECTION_ASCENDING);
                            if (Chars.equalsLowerCaseAscii(tok, "asc")) {
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

            if (Chars.equalsLowerCaseAscii(tok, "from")) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(lexer, "',' or 'from' expected");
            }
        }
    }

    private void parseSelectFrom(GenericLexer lexer, QueryModel model, QueryModel masterModel) throws SqlException {
        ExpressionNode expr = expr(lexer, model);
        CharSequence name = expr.token;

        switch (expr.type) {
            case ExpressionNode.LITERAL:
            case ExpressionNode.CONSTANT:
                final ExpressionNode literal = literal(name, expr.position);
                final WithClauseModel withClause = masterModel.getWithClause(name);
                if (withClause != null) {
                    model.setNestedModel(parseWith(lexer, withClause));
                    model.setAlias(literal);
                } else {
                    model.setTableName(literal);
                }
                break;
            case ExpressionNode.FUNCTION:
                model.setTableName(expr);
                break;
            default:
                throw SqlException.$(expr.position, "function, literal or constant is expected");
        }
    }

    QueryModel parseSubQuery(GenericLexer lexer) throws SqlException {
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

    private int parseSymbolCapacity(GenericLexer lexer) throws SqlException {
        final int errorPosition = lexer.getPosition();
        final int symbolCapacity = expectInt(lexer);
        TableUtils.validateSymbolCapacity(errorPosition, symbolCapacity);
        return Numbers.ceilPow2(symbolCapacity);
    }

    private ExpressionNode parseTimestamp(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (Chars.equalsLowerCaseAsciiNc(tok, "timestamp")) {
            expectTok(lexer, '(');
            final ExpressionNode result = expectLiteral(lexer);
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
        final CharSequence unparsed = lexer.getUnparsed();
        lexer.goToPosition(wcm.getPosition(), null);
        // this will not throw exception because this is second pass over the same sub-query
        // we wouldn't be here is syntax was wrong
        m = parseSubQuery(lexer);
        lexer.goToPosition(pos, unparsed);
        return m;
    }

    private void parseWithClauses(GenericLexer lexer, QueryModel model) throws SqlException {
        do {
            ExpressionNode name = expectLiteral(lexer);

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

    private ExpressionNode rewriteCase(ExpressionNode parent) throws SqlException {
        traversalAlgo.traverse(parent, node -> {
            if (node.type == ExpressionNode.FUNCTION && Chars.equalsLowerCaseAscii(node.token, "case")) {
                tempExprNodes.clear();
                ExpressionNode literal = null;
                ExpressionNode elseExpr;
                boolean convertToSwitch = true;
                final int paramCount = node.paramCount;

                if (node.paramCount == 2) {
                    // special case, typically something like
                    // case value else expression end
                    // this can be simplified to "expression" only

                    ExpressionNode that = node.rhs;
                    node.of(that.type, that.token, that.precedence, that.position);
                    node.paramCount = that.paramCount;
                    if (that.paramCount == 2) {
                        node.lhs = that.lhs;
                        node.rhs = that.rhs;
                    } else {
                        node.args.clear();
                        node.args.addAll(that.args);
                    }
                    return;
                }
                final int lim;
                if ((paramCount & 1) == 0) {
                    elseExpr = node.args.getQuick(0);
                    lim = 0;
                } else {
                    elseExpr = null;
                    lim = -1;
                }

                // agrs are in inverted order, hence last list item is the first arg
                ExpressionNode first = node.args.getQuick(paramCount - 1);
                if (first.token != null) {
                    // simple case of 'case' :) e.g.
                    // case x
                    //   when 1 then 'A'
                    //   ...
                    node.token = "switch";
                    return;
                }

                for (int i = paramCount - 2; i > lim; i--) {
                    if ((i & 1) == 1) {
                        // this is "then" clause, copy it as as
                        tempExprNodes.add(node.args.getQuick(i));
                        continue;
                    }
                    ExpressionNode where = node.args.getQuick(i);
                    if (where.type == ExpressionNode.OPERATION && where.token.charAt(0) == '=') {
                        ExpressionNode thisConstant;
                        ExpressionNode thisLiteral;
                        if (where.lhs.type == ExpressionNode.CONSTANT && where.rhs.type == ExpressionNode.LITERAL) {
                            thisConstant = where.lhs;
                            thisLiteral = where.rhs;
                        } else if (where.lhs.type == ExpressionNode.LITERAL && where.rhs.type == ExpressionNode.CONSTANT) {
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
                    int n = tempExprNodes.size();
                    node.token = "switch";
                    node.args.clear();
                    node.args.add(elseExpr);
                    for (int i = n - 1; i > -1; i--) {
                        node.args.add(tempExprNodes.getQuick(i));
                    }
                    node.args.add(literal);
                    node.paramCount = n + 2;
                } else {
                    node.args.remove(paramCount - 1);
                    node.paramCount = paramCount - 1;
                }
            }
        });
        return parent;
    }

    private int toColumnType(GenericLexer lexer, CharSequence tok) throws SqlException {
        final int type = ColumnType.columnTypeOf(tok);
        if (type == -1) {
            throw SqlException.$(lexer.lastTokenPosition(), "unsupported column type: ").put(tok);
        }
        return type;
    }

    private @NotNull CharSequence tok(GenericLexer lexer, String expectedList) throws SqlException {
        final int pos = lexer.getPosition();
        CharSequence tok = optTok(lexer);
        if (tok == null) {
            throw SqlException.position(pos).put(expectedList).put(" expected");
        }
        return tok;
    }

    private void validateLiteral(int pos, CharSequence tok, String expectedList) throws SqlException {
        switch (tok.charAt(0)) {
            case '(':
            case ')':
            case ',':
            case '`':
            case '"':
            case '\'':
                throw SqlException.position(pos).put(expectedList).put(" expected");
            default:
                break;

        }
    }
}

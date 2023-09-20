/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.griffin.model.*;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.SqlWalMode.*;
import static io.questdb.griffin.SqlKeywords.*;

public class SqlParser {
    public static final int MAX_ORDER_BY_COLUMNS = 1560;
    private static final ExpressionNode ONE = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, "1", 0, 0);
    private static final ExpressionNode ZERO_OFFSET = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, "'00:00'", 0, 0);
    private static final LowerCaseAsciiCharSequenceHashSet columnAliasStop = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet groupByStopSet = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceIntHashMap joinStartSet = new LowerCaseAsciiCharSequenceIntHashMap();
    private static final LowerCaseAsciiCharSequenceHashSet setOperations = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet tableAliasStop = new LowerCaseAsciiCharSequenceHashSet();
    private final IntList accumulatedColumnPositions = new IntList();
    private final ObjList<QueryColumn> accumulatedColumns = new ObjList<>();
    private final LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjectPool<AnalyticColumn> analyticColumnPool;
    private final CharacterStore characterStore;
    private final CharSequence column;
    private final ObjectPool<ColumnCastModel> columnCastModelPool;
    private final CairoConfiguration configuration;
    private final ObjectPool<CopyModel> copyModelPool;
    private final ObjectPool<CreateTableModel> createTableModelPool;
    private final ObjectPool<ExplainModel> explainModelPool;
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final ExpressionParser expressionParser;
    private final ExpressionTreeBuilder expressionTreeBuilder;
    private final ObjectPool<InsertModel> insertModelPool;
    private final SqlOptimiser optimiser;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final ObjectPool<RenameTableModel> renameTableModelPool;
    private final PostOrderTreeTraversalAlgo.Visitor rewriteConcat0Ref = this::rewriteConcat0;
    private final PostOrderTreeTraversalAlgo.Visitor rewriteCount0Ref = this::rewriteCount0;
    private final PostOrderTreeTraversalAlgo.Visitor rewritePgCast0Ref = this::rewritePgCast0;
    private final ObjList<ExpressionNode> tempExprNodes = new ObjList<>();
    private final PostOrderTreeTraversalAlgo.Visitor rewriteCase0Ref = this::rewriteCase0;
    private final LowerCaseCharSequenceObjHashMap<WithClauseModel> topLevelWithModel = new LowerCaseCharSequenceObjHashMap<>();
    private final PostOrderTreeTraversalAlgo traversalAlgo;
    private final ObjectPool<WithClauseModel> withClauseModelPool;
    private int digit;
    private boolean subQueryMode = false;

    SqlParser(
            CairoConfiguration configuration,
            SqlOptimiser optimiser,
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            PostOrderTreeTraversalAlgo traversalAlgo
    ) {
        this.expressionNodePool = expressionNodePool;
        this.queryModelPool = queryModelPool;
        this.queryColumnPool = queryColumnPool;
        this.expressionTreeBuilder = new ExpressionTreeBuilder();
        this.analyticColumnPool = new ObjectPool<>(AnalyticColumn.FACTORY, configuration.getAnalyticColumnPoolCapacity());
        this.createTableModelPool = new ObjectPool<>(CreateTableModel.FACTORY, configuration.getCreateTableModelPoolCapacity());
        this.columnCastModelPool = new ObjectPool<>(ColumnCastModel.FACTORY, configuration.getColumnCastModelPoolCapacity());
        this.renameTableModelPool = new ObjectPool<>(RenameTableModel.FACTORY, configuration.getRenameTableModelPoolCapacity());
        this.withClauseModelPool = new ObjectPool<>(WithClauseModel.FACTORY, configuration.getWithClauseModelPoolCapacity());
        this.insertModelPool = new ObjectPool<>(InsertModel.FACTORY, configuration.getInsertPoolCapacity());
        this.copyModelPool = new ObjectPool<>(CopyModel.FACTORY, configuration.getCopyPoolCapacity());
        this.explainModelPool = new ObjectPool<>(ExplainModel.FACTORY, configuration.getExplainPoolCapacity());
        this.configuration = configuration;
        this.traversalAlgo = traversalAlgo;
        this.characterStore = characterStore;
        this.optimiser = optimiser;
        this.expressionParser = new ExpressionParser(expressionNodePool, this, characterStore);
        this.digit = 1;
        this.column = "column";
    }

    public static boolean isFullSampleByPeriod(ExpressionNode n) {
        return n != null && (n.type == ExpressionNode.CONSTANT || (n.type == ExpressionNode.LITERAL && isValidSampleByPeriodLetter(n.token)));
    }

    private static SqlException err(GenericLexer lexer, @Nullable CharSequence tok, @NotNull String msg) {
        return SqlException.parserErr(lexer.lastTokenPosition(), tok, msg);
    }

    private static SqlException errUnexpected(GenericLexer lexer, CharSequence token) {
        return SqlException.unexpectedToken(lexer.lastTokenPosition(), token);
    }

    private static boolean isValidSampleByPeriodLetter(CharSequence token) {
        if (token.length() != 1) return false;
        switch (token.charAt(0)) {
            case 'U':
                // micros
            case 'T':
                // millis
            case 's':
                // seconds
            case 'm':
                // minutes
            case 'h':
                // hours
            case 'd':
                // days
            case 'M':
                // months
            case 'y':
                return true;
            default:
                return false;
        }
    }

    private void addConcatArgs(ObjList<ExpressionNode> args, ExpressionNode leaf) {
        if (leaf.type != ExpressionNode.FUNCTION || !isConcatKeyword(leaf.token)) {
            args.add(leaf);
            return;
        }

        // Nested CONCAT. Expand it from CONCAT(x, CONCAT(y, z)) into CONCAT(x, y, z).
        if (leaf.args.size() > 0) {
            args.addAll(leaf.args);
        } else {
            args.add(leaf.rhs);
            args.add(leaf.lhs);
        }
    }

    private void assertNotDot(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (Chars.indexOf(tok, '.') != -1) {
            throw SqlException.$(lexer.lastTokenPosition(), "'.' is not allowed here");
        }
    }

    //prevent full/right from being used as table aliases
    private void checkSupportedJoinType(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (tok != null && (SqlKeywords.isFullKeyword(tok) || SqlKeywords.isRightKeyword(tok))) {
            throw SqlException.$((lexer.lastTokenPosition()), "unsupported join type");
        }
    }

    private CharSequence createColumnAlias(
            ExpressionNode node,
            LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap
    ) {
        return SqlUtil.createColumnAlias(
                characterStore,
                GenericLexer.unquote(node.token),
                Chars.indexOf(node.token, '.'),
                aliasToColumnMap,
                node.type != ExpressionNode.LITERAL
        );
    }

    private CharSequence createConstColumnAlias(LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap) {
        final CharacterStoreEntry characterStoreEntry = characterStore.newEntry();

        characterStoreEntry.put(column);
        int len = characterStoreEntry.length();
        characterStoreEntry.put(digit);

        while (aliasToColumnMap.contains(characterStoreEntry.toImmutable())) {
            characterStoreEntry.trimTo(len);
            digit++;
            characterStoreEntry.put(digit);
        }
        return characterStoreEntry.toImmutable();
    }

    private void expectBy(GenericLexer lexer) throws SqlException {
        if (isByKeyword(tok(lexer, "'by'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'by' expected");
    }

    private ExpressionNode expectExpr(GenericLexer lexer) throws SqlException {
        final ExpressionNode n = expr(lexer, (QueryModel) null);
        if (n != null) {
            return n;
        }
        throw SqlException.$(lexer.hasUnparsed() ? lexer.lastTokenPosition() : lexer.getPosition(), "Expression expected");
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
            throw err(lexer, tok, "bad integer");
        }
    }

    private ExpressionNode expectLiteral(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "literal");
        int pos = lexer.lastTokenPosition();
        SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, pos);
        validateLiteral(pos, tok);
        return nextLiteral(GenericLexer.immutableOf(GenericLexer.unquote(tok)), pos);
    }

    private long expectLong(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "long integer");
        boolean negative;
        if (Chars.equals(tok, '-')) {
            negative = true;
            tok = tok(lexer, "long integer");
        } else {
            negative = false;
        }
        try {
            long result = Numbers.parseLong(tok);
            return negative ? -result : result;
        } catch (NumericException e) {
            throw err(lexer, tok, "bad long integer");
        }
    }

    private void expectObservation(GenericLexer lexer) throws SqlException {
        if (isObservationKeyword(tok(lexer, "'observation'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'observation' expected");
    }

    private void expectOffset(GenericLexer lexer) throws SqlException {
        if (isOffsetKeyword(tok(lexer, "'offset'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'offset' expected");
    }

    private void expectSample(GenericLexer lexer, QueryModel model) throws SqlException {
        final ExpressionNode n = expr(lexer, (QueryModel) null);
        if (isFullSampleByPeriod(n)) {
            model.setSampleBy(n);
            return;
        }

        // this is complex expression of sample by period. It must follow time unit interval
        // lets preempt the problem where time unit interval is missing, and we hit keyword instead
        final int pos = lexer.lastTokenPosition();
        final CharSequence tok = tok(lexer, "time interval unit");

        if (isValidSampleByPeriodLetter(tok)) {
            model.setSampleBy(n, SqlUtil.nextLiteral(expressionNodePool, tok, pos));
            return;
        }
        throw SqlException.$(pos, "one letter sample by period unit expected");
    }

    private CharSequence expectTableNameOrSubQuery(GenericLexer lexer) throws SqlException {
        return tok(lexer, "table name or sub-query");
    }

    private void expectTo(GenericLexer lexer) throws SqlException {
        if (isToKeyword(tok(lexer, "'to'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'to' expected");
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

    private void expectZone(GenericLexer lexer) throws SqlException {
        if (isZoneKeyword(tok(lexer, "'zone'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'zone' expected");
    }

    private int getCreateTableColumnIndex(CreateTableModel model, CharSequence columnName, int position) throws SqlException {
        int index = model.getColumnIndex(columnName);
        if (index == -1) {
            throw SqlException.invalidColumn(position, columnName);
        }
        return index;
    }

    private boolean isCurrentRow(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (SqlKeywords.isCurrentKeyword(tok)) {
            tok = tok(lexer, "'row'");
            if (SqlKeywords.isRowKeyword(tok)) {
                return true;
            }
            throw SqlException.$(lexer.lastTokenPosition(), "'row' expected");
        }
        return false;
    }

    private boolean isFieldTerm(CharSequence tok) {
        return Chars.equals(tok, ')') || Chars.equals(tok, ',');
    }

    private boolean isUnboundedPreceding(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (SqlKeywords.isUnboundedKeyword(tok)) {
            tok = tok(lexer, "'preceding'");
            if (SqlKeywords.isPrecedingKeyword(tok)) {
                return true;
            }
            throw SqlException.$(lexer.lastTokenPosition(), "'preceding' expected");
        }
        return false;
    }

    private ExpressionNode literal(GenericLexer lexer, CharSequence name) {
        return literal(name, lexer.lastTokenPosition());
    }

    private ExpressionNode literal(CharSequence name, int position) {
        // this can never be null in its current contexts
        // every time this function is called is after lexer.unparse(), which ensures non-null token.
        return expressionNodePool.next().of(ExpressionNode.LITERAL, GenericLexer.unquote(name), 0, position);
    }

    private ExpressionNode nextLiteral(CharSequence token, int position) {
        return SqlUtil.nextLiteral(expressionNodePool, token, position);
    }

    private CharSequence notTermTok(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "')' or ','");
        if (isFieldTerm(tok)) {
            throw err(lexer, tok, "missing column definition");
        }
        return tok;
    }

    private CharSequence optTok(GenericLexer lexer) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || (subQueryMode && Chars.equals(tok, ')'))) {
            return null;
        }
        return tok;
    }

    private QueryModel parseAsSubQueryAndExpectClosingBrace(GenericLexer lexer,
                                                            LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
                                                            boolean useTopLevelWithClauses)
            throws SqlException {
        final QueryModel model = parseAsSubQuery(lexer, withClauses, useTopLevelWithClauses);
        expectTok(lexer, ')');
        return model;
    }

    private ExecutionModel parseCopy(GenericLexer lexer) throws SqlException {
        if (Chars.isBlank(configuration.getSqlCopyInputRoot())) {
            throw SqlException.$(lexer.lastTokenPosition(), "COPY is disabled ['cairo.sql.copy.root' is not set?]");
        }
        ExpressionNode target = expectExpr(lexer);
        CharSequence tok = tok(lexer, "'from' or 'to' or 'cancel'");

        if (isCancelKeyword(tok)) {
            CopyModel model = copyModelPool.next();
            model.setCancel(true);
            model.setTarget(target);
            return model;
        }

        if (isFromKeyword(tok)) {
            final ExpressionNode fileName = expectExpr(lexer);
            if (fileName.token.length() < 3 && Chars.startsWith(fileName.token, '\'')) {
                throw SqlException.$(fileName.position, "file name expected");
            }

            CopyModel model = copyModelPool.next();
            model.setTarget(target);
            model.setFileName(fileName);

            tok = optTok(lexer);
            if (tok != null && isWithKeyword(tok)) {
                tok = tok(lexer, "copy option");
                while (tok != null && !isSemicolon(tok)) {
                    if (isHeaderKeyword(tok)) {
                        model.setHeader(isTrueKeyword(tok(lexer, "'true' or 'false'")));
                        tok = optTok(lexer);
                    } else if (isPartitionKeyword(tok)) {
                        expectTok(lexer, "by");
                        tok = tok(lexer, "year month day hour");
                        int partitionBy = PartitionBy.fromString(tok);
                        if (partitionBy == -1) {
                            throw SqlException.$(lexer.getPosition(), "'NONE', 'HOUR', 'DAY', 'MONTH' or 'YEAR' expected");
                        }
                        model.setPartitionBy(partitionBy);
                        tok = optTok(lexer);
                    } else if (isTimestampKeyword(tok)) {
                        tok = tok(lexer, "timestamp column name expected");
                        CharSequence columnName = GenericLexer.immutableOf(GenericLexer.unquote(tok));
                        if (!TableUtils.isValidColumnName(columnName, configuration.getMaxFileNameLength())) {
                            throw SqlException.$(lexer.getPosition(), "timestamp column name contains invalid characters");
                        }
                        model.setTimestampColumnName(columnName);
                        tok = optTok(lexer);
                    } else if (isFormatKeyword(tok)) {
                        tok = tok(lexer, "timestamp format expected");
                        CharSequence format = GenericLexer.immutableOf(GenericLexer.unquote(tok));
                        model.setTimestampFormat(format);
                        tok = optTok(lexer);
                    } else if (isOnKeyword(tok)) {
                        expectTok(lexer, "error");
                        tok = tok(lexer, "skip_column skip_row abort");
                        if (Chars.equalsIgnoreCase(tok, "skip_column")) {
                            model.setAtomicity(Atomicity.SKIP_COL);
                        } else if (Chars.equalsIgnoreCase(tok, "skip_row")) {
                            model.setAtomicity(Atomicity.SKIP_ROW);
                        } else if (Chars.equalsIgnoreCase(tok, "abort")) {
                            model.setAtomicity(Atomicity.SKIP_ALL);
                        } else {
                            throw SqlException.$(lexer.getPosition(), "invalid 'on error' copy option found");
                        }
                        tok = optTok(lexer);
                    } else if (isDelimiterKeyword(tok)) {
                        tok = tok(lexer, "timestamp character expected");
                        CharSequence delimiter = GenericLexer.immutableOf(GenericLexer.unquote(tok));
                        if (delimiter == null || delimiter.length() != 1) {
                            throw SqlException.$(lexer.getPosition(), "delimiter is empty or contains more than 1 character");
                        }
                        char delimiterChar = delimiter.charAt(0);
                        if (delimiterChar > 127) {
                            throw SqlException.$(lexer.getPosition(), "delimiter is not an ascii character");
                        }
                        model.setDelimiter((byte) delimiterChar);
                        tok = optTok(lexer);
                    } else {
                        throw SqlException.$(lexer.lastTokenPosition(), "unexpected option");
                    }
                }
            } else if (tok != null && !SqlKeywords.isSemicolon(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'with' expected");
            }
            return model;
        }
        throw SqlException.$(lexer.lastTokenPosition(), "'from' expected");
    }

    private ExecutionModel parseCreateStatement(GenericLexer lexer, SqlExecutionContext executionContext) throws SqlException {
        expectTok(lexer, "table");
        return parseCreateTable(lexer, executionContext);
    }

    private ExecutionModel parseCreateTable(GenericLexer lexer, SqlExecutionContext executionContext) throws SqlException {
        final CreateTableModel model = createTableModelPool.next();
        final CharSequence tableName;
        CharSequence tok = tok(lexer, "table name or 'if'");
        if (SqlKeywords.isIfKeyword(tok)) {
            if (SqlKeywords.isNotKeyword(tok(lexer, "'not'")) && SqlKeywords.isExistsKeyword(tok(lexer, "'exists'"))) {
                model.setIgnoreIfExists(true);
                tableName = tok(lexer, "table name");
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'if not exists' expected");
            }
        } else {
            tableName = tok;
        }
        // validate that table name is not a keyword

        assertTableNameIsQuotedOrNotAKeyword(tableName, lexer.lastTokenPosition());

        model.setName(nextLiteral(GenericLexer.assertNoDotsAndSlashes(GenericLexer.unquote(tableName), lexer.lastTokenPosition()), lexer.lastTokenPosition()));

        tok = tok(lexer, "'(' or 'as'");

        if (Chars.equals(tok, '(')) {
            tok = tok(lexer, "like");
            if (isLikeKeyword(tok)) {
                parseLikeTableName(lexer, model);
                return model;
            } else {
                lexer.unparseLast();
                parseCreateTableColumns(lexer, model);
            }
        } else if (isAsKeyword(tok)) {
            parseCreateTableAsSelect(lexer, model, executionContext);
        } else {
            throw errUnexpected(lexer, tok);
        }

        while ((tok = optTok(lexer)) != null && Chars.equals(tok, ',')) {
            tok = tok(lexer, "'index' or 'cast'");
            if (isIndexKeyword(tok)) {
                parseCreateTableIndexDef(lexer, model);
            } else if (isCastKeyword(tok)) {
                parseCreateTableCastDef(lexer, model);
            } else {
                throw errUnexpected(lexer, tok);
            }
        }

        ExpressionNode timestamp = parseTimestamp(lexer, tok);
        if (timestamp != null) {
            // ignore index, validate column
            int timestampIdx = getCreateTableColumnIndex(model, timestamp.token, timestamp.position);
            int timestampType = model.getColumnType(timestampIdx);
            if (timestampType != ColumnType.TIMESTAMP && timestampType != -1) { //type can be -1 for create table as select because types aren't known yet
                throw SqlException.position(timestamp.position).put("TIMESTAMP column expected [actual=").put(ColumnType.nameOf(timestampType)).put(']');
            }
            model.setTimestamp(timestamp);
            tok = optTok(lexer);
        }

        int walSetting = WAL_NOT_SET;

        ExpressionNode partitionBy = parseCreateTablePartition(lexer, tok);
        if (partitionBy != null) {
            if (model.getTimestamp() == null) {
                throw SqlException.$(partitionBy.position, "partitioning is possible only on tables with designated timestamps");
            }
            if (PartitionBy.fromString(partitionBy.token) == -1) {
                throw SqlException.$(partitionBy.position, "'NONE', 'HOUR', 'DAY', 'MONTH' or 'YEAR' expected");
            }
            model.setPartitionBy(partitionBy);
            tok = optTok(lexer);

            if (tok != null) {
                if (isWalKeyword(tok)) {
                    if (!PartitionBy.isPartitioned(model.getPartitionBy())) {
                        throw SqlException.position(lexer.lastTokenPosition()).put("WAL Write Mode can only be used on partitioned tables");
                    }
                    walSetting = WAL_ENABLED;
                    tok = optTok(lexer);
                } else if (isBypassKeyword(tok)) {
                    tok = optTok(lexer);
                    if (tok != null && isWalKeyword(tok)) {
                        walSetting = WAL_DISABLED;
                        tok = optTok(lexer);
                    } else {
                        throw SqlException.position(
                                        tok == null ? lexer.getPosition() : lexer.lastTokenPosition()
                                ).put(" invalid syntax, should be BYPASS WAL but was BYPASS ")
                                .put(tok != null ? tok : "");
                    }
                }
            }
        }
        final boolean isWalEnabled = configuration.isWalSupported() &&
                PartitionBy.isPartitioned(model.getPartitionBy()) &&
                ((walSetting == WAL_NOT_SET && configuration.getWalEnabledDefault()) || walSetting == WAL_ENABLED);
        model.setWalEnabled(isWalEnabled);

        int maxUncommittedRows = configuration.getMaxUncommittedRows();
        long o3MaxLag = configuration.getO3MaxLag();

        if (tok != null && isWithKeyword(tok)) {
            ExpressionNode expr;
            while ((expr = expr(lexer, (QueryModel) null)) != null) {
                if (Chars.equals(expr.token, '=')) {
                    if (isMaxUncommittedRowsKeyword(expr.lhs.token)) {
                        try {
                            maxUncommittedRows = Numbers.parseInt(expr.rhs.token);
                        } catch (NumericException e) {
                            throw SqlException.position(lexer.getPosition()).put(" could not parse maxUncommittedRows value \"").put(expr.rhs.token).put('"');
                        }
                    } else if (isO3MaxLagKeyword(expr.lhs.token)) {
                        o3MaxLag = SqlUtil.expectMicros(expr.rhs.token, lexer.getPosition());
                    } else {
                        throw SqlException.position(lexer.getPosition()).put(" unrecognized ").put(expr.lhs.token).put(" after WITH");
                    }
                    tok = optTok(lexer);
                    if (null != tok && Chars.equals(tok, ',')) {
                        CharSequence peek = optTok(lexer);
                        if (peek != null && isInKeyword(peek)) { // in volume
                            tok = peek;
                            break;
                        }
                        lexer.unparseLast();
                        continue;
                    }
                    break;
                }
                throw SqlException.position(lexer.getPosition()).put(" expected parameter after WITH");
            }
        }
        model.setMaxUncommittedRows(maxUncommittedRows);
        model.setO3MaxLag(o3MaxLag);

        if (tok != null && isInKeyword(tok)) {
            tok = tok(lexer, "volume");
            if (!isVolumeKeyword(tok)) {
                throw SqlException.position(lexer.getPosition()).put("expected 'volume'");
            }
            tok = tok(lexer, "path for volume");
            if (Os.isWindows()) {
                throw SqlException.position(lexer.getPosition()).put("'in volume' is not supported on Windows");
            }
            model.setVolumeAlias(GenericLexer.unquote(tok));
            tok = optTok(lexer);
        }

        if (tok != null && (isDedupKeyword(tok) || isDeduplicateKeyword(tok))) {
            if (!model.isWalEnabled()) {
                throw SqlException.position(lexer.getPosition()).put("deduplication is possible only on WAL tables");
            }

            tok = optTok(lexer);
            if (tok == null || !isUpsertKeyword(tok)) {
                throw SqlException.position(lexer.lastTokenPosition()).put("expected 'upsert'");
            }

            tok = optTok(lexer);
            if (tok == null || !isKeysKeyword(tok)) {
                throw SqlException.position(lexer.lastTokenPosition()).put("expected 'keys'");
            }

            boolean timestampColumnFound = false;

            tok = optTok(lexer);
            if (tok != null && Chars.equals(tok, '(')) {
                tok = optTok(lexer);
                int columnListPos = lexer.lastTokenPosition();

                while (tok != null && !Chars.equals(tok, ')')) {
                    validateLiteral(lexer.lastTokenPosition(), tok);
                    final CharSequence columnName = GenericLexer.unquote(tok);

                    int colIndex = model.getColumnIndex(columnName);
                    if (colIndex < 0) {
                        throw SqlException.position(lexer.lastTokenPosition()).put("deduplicate key column not found [column=").put(columnName).put(']');
                    }
                    if (colIndex == model.getTimestampIndex()) {
                        timestampColumnFound = true;
                    } else {
                        int columnType = model.getColumnType(colIndex);
                        if (ColumnType.isVariableLength(columnType)) {
                            throw SqlException.position(lexer.lastTokenPosition()).put("deduplicate key column can only be fixed size column [column=").put(columnName)
                                    .put(", type=").put(ColumnType.nameOf(columnType)).put(']');
                        }
                    }
                    model.setDedupKeyFlag(colIndex);

                    tok = optTok(lexer);
                    if (tok != null && Chars.equals(tok, ',')) {
                        tok = optTok(lexer);
                    }
                }

                if (!timestampColumnFound) {
                    throw SqlException.position(columnListPos).put("deduplicate key list must include dedicated timestamp column");
                }

                tok = optTok(lexer);
            } else {
                throw SqlException.position(lexer.getPosition()).put("column list expected");
            }
        }

        if (tok == null || Chars.equals(tok, ';')) {
            return model;
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseCreateTableAsSelect(GenericLexer lexer, CreateTableModel model, SqlExecutionContext executionContext) throws SqlException {
        expectTok(lexer, '(');
        QueryModel queryModel = optimiser.optimise(parseDml(lexer, null, lexer.getPosition(), true), executionContext);
        ObjList<QueryColumn> columns = queryModel.getBottomUpColumns();
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

        if (ColumnType.isSymbol(type)) {
            CharSequence tok = tok(lexer, "'capacity', 'nocache', 'cache' or ')'");

            int symbolCapacity;
            int capacityPosition;
            if (isCapacityKeyword(tok)) {
                capacityPosition = lexer.getPosition();
                columnCastModel.setSymbolCapacity(symbolCapacity = parseSymbolCapacity(lexer));
                tok = tok(lexer, "'nocache', 'cache' or ')'");
            } else {
                columnCastModel.setSymbolCapacity(configuration.getDefaultSymbolCapacity());
                symbolCapacity = -1;
                capacityPosition = -1;
            }

            final boolean cached;
            if (isNoCacheKeyword(tok)) {
                cached = false;
            } else if (isCacheKeyword(tok)) {
                cached = true;
            } else {
                cached = configuration.getDefaultSymbolCacheFlag();
                lexer.unparseLast();
            }

            columnCastModel.setSymbolCacheFlag(cached);

            if (cached && symbolCapacity != -1) {
                assert capacityPosition != -1;
                TableUtils.validateSymbolCapacityCached(true, symbolCapacity, capacityPosition);
            }

            columnCastModel.setIndexed(false);
        }

        expectTok(lexer, ')');

        if (!model.addColumnCastModel(columnCastModel)) {
            throw SqlException.$(columnCastModel.getName().position, "duplicate cast");
        }
    }

    private void parseCreateTableColumns(GenericLexer lexer, CreateTableModel model) throws SqlException {
        while (true) {
            CharSequence tok = notTermTok(lexer);
            SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            final CharSequence name = GenericLexer.immutableOf(GenericLexer.unquote(tok));
            final int position = lexer.lastTokenPosition();
            final int type = toColumnType(lexer, notTermTok(lexer));

            if (!TableUtils.isValidColumnName(name, configuration.getMaxFileNameLength())) {
                throw SqlException.$(position, " new column name contains invalid characters");
            }

            model.addColumn(position, name, type, configuration.getDefaultSymbolCapacity());

            if (ColumnType.isSymbol(type)) {
                tok = tok(lexer, "'capacity', 'nocache', 'cache', 'index' or ')'");

                int symbolCapacity;
                if (isCapacityKeyword(tok)) {
                    // when capacity is not set explicitly it will default via configuration
                    model.symbolCapacity(symbolCapacity = parseSymbolCapacity(lexer));
                    tok = tok(lexer, "'nocache', 'cache', 'index' or ')'");
                } else {
                    symbolCapacity = -1;
                }

                final boolean cached;
                if (isNoCacheKeyword(tok)) {
                    cached = false;
                } else if (isCacheKeyword(tok)) {
                    cached = true;
                } else {
                    cached = configuration.getDefaultSymbolCacheFlag();
                    lexer.unparseLast();
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

            //ignoring `PRECISION`
            if (SqlKeywords.isPrecisionKeyword(tok)) {
                tok = tok(lexer, "'NOT' or 'NULL' or ',' or ')'");
            }

            //ignoring `NULL` and `NOT NULL`
            if (SqlKeywords.isNotKeyword(tok)) {
                tok = tok(lexer, "'NULL'");
            }

            if (SqlKeywords.isNullKeyword(tok)) {
                tok = tok(lexer, "','");
            }

            if (Chars.equals(tok, ')')) {
                break;
            }

            if (!Chars.equals(tok, ',')) {
                throw err(lexer, tok, "',' or ')' expected");
            }
        }
    }

    private void parseCreateTableIndexDef(GenericLexer lexer, CreateTableModel model) throws SqlException {
        expectTok(lexer, '(');
        final CharSequence columnName = expectLiteral(lexer).token;
        final int position = lexer.lastTokenPosition();
        final int columnIndex = getCreateTableColumnIndex(model, columnName, position);
        final int columnType = model.getColumnType(columnIndex);
        if (columnType > -1 && !ColumnType.isSymbol(columnType)) {
            throw SqlException.$(position, "indexes are supported only for SYMBOL columns: ").put(columnName);
        }

        if (isCapacityKeyword(tok(lexer, "'capacity'"))) {
            int errorPosition = lexer.getPosition();
            int indexValueBlockSize = expectInt(lexer);
            TableUtils.validateIndexValueBlockSize(errorPosition, indexValueBlockSize);
            model.setIndexFlags(columnIndex, true, Numbers.ceilPow2(indexValueBlockSize));
        } else {
            model.setIndexFlags(columnIndex, true, configuration.getIndexValueBlockSize());
            lexer.unparseLast();
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
        if (tok != null && isPartitionKeyword(tok)) {
            expectTok(lexer, "by");
            return expectLiteral(lexer);
        }
        return null;
    }

    private QueryModel parseDml(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
            int modelPosition,
            boolean useTopLevelWithClauses
    ) throws SqlException {
        QueryModel model = null;
        QueryModel prevModel = null;

        while (true) {
            LowerCaseCharSequenceObjHashMap<WithClauseModel> parentWithClauses = prevModel != null ? prevModel.getWithClauses() : withClauses;
            LowerCaseCharSequenceObjHashMap<WithClauseModel> topWithClauses = useTopLevelWithClauses && model == null ? topLevelWithModel : null;

            QueryModel unionModel = parseDml0(lexer, parentWithClauses, topWithClauses, modelPosition);
            if (prevModel == null) {
                model = unionModel;
                prevModel = model;
            } else {
                prevModel.setUnionModel(unionModel);
                prevModel = unionModel;
            }

            CharSequence tok = optTok(lexer);
            if (tok == null || Chars.equals(tok, ';') || setOperations.excludes(tok)) {
                lexer.unparseLast();
                return model;
            }

            if (prevModel.getNestedModel() != null) {
                if (prevModel.getNestedModel().getOrderByPosition() > 0) {
                    throw SqlException.$(prevModel.getNestedModel().getOrderByPosition(), "unexpected token 'order'");
                }
                if (prevModel.getNestedModel().getLimitPosition() > 0) {
                    throw SqlException.$(prevModel.getNestedModel().getLimitPosition(), "unexpected token 'limit'");
                }
            }

            if (isUnionKeyword(tok)) {
                tok = tok(lexer, "all or select");
                if (isAllKeyword(tok)) {
                    prevModel.setSetOperationType(QueryModel.SET_OPERATION_UNION_ALL);
                    modelPosition = lexer.getPosition();
                } else {
                    prevModel.setSetOperationType(QueryModel.SET_OPERATION_UNION);
                    lexer.unparseLast();
                    modelPosition = lexer.lastTokenPosition();
                }
                continue;
            }

            if (isExceptKeyword(tok)) {
                tok = tok(lexer, "all or select");
                if (isAllKeyword(tok)) {
                    prevModel.setSetOperationType(QueryModel.SET_OPERATION_EXCEPT_ALL);
                    modelPosition = lexer.getPosition();
                } else {
                    prevModel.setSetOperationType(QueryModel.SET_OPERATION_EXCEPT);
                    lexer.unparseLast();
                    modelPosition = lexer.lastTokenPosition();
                }
                continue;
            }

            if (isIntersectKeyword(tok)) {
                tok = tok(lexer, "all or select");
                if (isAllKeyword(tok)) {
                    prevModel.setSetOperationType(QueryModel.SET_OPERATION_INTERSECT_ALL);
                    modelPosition = lexer.getPosition();
                } else {
                    prevModel.setSetOperationType(QueryModel.SET_OPERATION_INTERSECT);
                    lexer.unparseLast();
                    modelPosition = lexer.lastTokenPosition();
                }
            }
        }
    }

    @NotNull
    private QueryModel parseDml0(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> parentWithClauses,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> topWithClauses,
            int modelPosition
    ) throws SqlException {
        CharSequence tok;
        QueryModel model = queryModelPool.next();
        model.setModelPosition(modelPosition);
        if (parentWithClauses != null) {
            model.getWithClauses().putAll(parentWithClauses);
        }

        tok = tok(lexer, "'select', 'with' or table name expected");

        if (isWithKeyword(tok)) {
            parseWithClauses(lexer, model.getWithClauses());
            tok = tok(lexer, "'select' or table name expected");
        } else if (topWithClauses != null) {
            model.getWithClauses().putAll(topWithClauses);
        }

        // [select]
        if (isSelectKeyword(tok)) {
            parseSelectClause(lexer, model);

            tok = optTok(lexer);

            if (tok != null && setOperations.contains(tok)) {
                tok = null;
            }

            if (tok == null || Chars.equals(tok, ';') || Chars.equals(tok, ')')) { //token can also be ';' on query boundary
                QueryModel nestedModel = queryModelPool.next();
                nestedModel.setModelPosition(modelPosition);
                ExpressionNode tableNameExpr = expressionNodePool.next().of(ExpressionNode.FUNCTION, "long_sequence", 0, lexer.lastTokenPosition());
                tableNameExpr.paramCount = 1;
                tableNameExpr.rhs = ONE;
                nestedModel.setTableNameExpr(tableNameExpr);
                model.setSelectModelType(QueryModel.SELECT_MODEL_VIRTUAL);
                model.setNestedModel(nestedModel);
                lexer.unparseLast();
                return model;
            }
        } else {
            lexer.unparseLast();
            SqlUtil.addSelectStar(
                    model,
                    queryColumnPool,
                    expressionNodePool
            );
        }

        QueryModel nestedModel = queryModelPool.next();
        nestedModel.setModelPosition(modelPosition);

        parseFromClause(lexer, nestedModel, model);
        if (nestedModel.getLimitHi() != null || nestedModel.getLimitLo() != null) {
            model.setLimit(nestedModel.getLimitLo(), nestedModel.getLimitHi());
            nestedModel.setLimit(null, null);
        }
        model.setSelectModelType(QueryModel.SELECT_MODEL_CHOOSE);
        model.setNestedModel(nestedModel);
        final ExpressionNode n = nestedModel.getAlias();
        if (n != null) {
            model.setAlias(n);
        }
        return model;
    }

    private QueryModel parseDmlUpdate(GenericLexer lexer) throws SqlException {
        // Update QueryModel structure is
        // QueryModel with SET column expressions (updateQueryModel)
        // |-- nested QueryModel of select-virtual or select-choose of data selected for update (fromModel)
        //     |-- nested QueryModel with selected data (nestedModel)
        //         |-- join QueryModels to represent FROM clause
        CharSequence tok;
        final int modelPosition = lexer.getPosition();

        QueryModel updateQueryModel = queryModelPool.next();
        updateQueryModel.setModelType(ExecutionModel.UPDATE);
        updateQueryModel.setModelPosition(modelPosition);
        QueryModel fromModel = queryModelPool.next();
        fromModel.setModelPosition(modelPosition);
        updateQueryModel.setIsUpdate(true);
        fromModel.setIsUpdate(true);
        tok = tok(lexer, "UPDATE, WITH or table name expected");

        // [update]
        if (isUpdateKeyword(tok)) {
            // parse SET statements into updateQueryModel and rhs of SETs into fromModel to select
            parseUpdateClause(lexer, updateQueryModel, fromModel);

            // create nestedModel QueryModel to source rowids for the update
            QueryModel nestedModel = queryModelPool.next();
            nestedModel.setTableNameExpr(fromModel.getTableNameExpr());
            nestedModel.setAlias(updateQueryModel.getAlias());
            nestedModel.setIsUpdate(true);

            // nest nestedModel inside fromModel
            fromModel.setTableNameExpr(null);
            fromModel.setNestedModel(nestedModel);

            // Add WITH clauses if they exist into fromModel
            fromModel.getWithClauses().putAll(topLevelWithModel);

            tok = optTok(lexer);

            // [from]
            if (tok != null && isFromKeyword(tok)) {
                tok = ","; // FROM in Postgres UPDATE statement means cross join
                int joinType;
                int i = 0;
                while (tok != null && (joinType = joinStartSet.get(tok)) != -1) {
                    if (i++ == 1) {
                        throw SqlException.$(lexer.lastTokenPosition(), "JOIN is not supported on UPDATE statement");
                    }
                    // expect multiple [[inner | outer | cross] join]
                    nestedModel.addJoinModel(parseJoin(lexer, tok, joinType, topLevelWithModel));
                    tok = optTok(lexer);
                }
            } else if (tok != null && isSemicolon(tok)) {
                tok = null;
            } else if (tok != null && !isWhereKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "FROM, WHERE or EOF expected");
            }

            // [where]
            if (tok != null && isWhereKeyword(tok)) {
                ExpressionNode expr = expr(lexer, fromModel);
                if (expr != null) {
                    nestedModel.setWhereClause(expr);
                } else {
                    throw SqlException.$((lexer.lastTokenPosition()), "empty where clause");
                }
            } else if (tok != null && !isSemicolon(tok)) {
                throw errUnexpected(lexer, tok);
            }

            updateQueryModel.setNestedModel(fromModel);
        }
        return updateQueryModel;
    }

    //doesn't allow copy, rename
    private ExecutionModel parseExplain(GenericLexer lexer, SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok = tok(lexer, "'create', 'format', 'insert', 'update', 'select' or 'with'");

        if (isSelectKeyword(tok)) {
            return parseSelect(lexer);
        }

        if (isCreateKeyword(tok)) {
            return parseCreateStatement(lexer, executionContext);
        }

        if (isUpdateKeyword(tok)) {
            return parseUpdate(lexer);
        }

        if (isInsertKeyword(tok)) {
            return parseInsert(lexer);
        }

        if (isWithKeyword(tok)) {
            return parseWith(lexer);
        }

        return parseSelect(lexer);
    }

    private int parseExplainOptions(GenericLexer lexer, CharSequence prevTok) throws SqlException {
        int parenthesisPos = lexer.getPosition();
        CharSequence explainTok = GenericLexer.immutableOf(prevTok);
        CharSequence tok = tok(lexer, "'create', 'insert', 'update', 'select', 'with' or '('");
        if (Chars.equals(tok, '(')) {
            tok = tok(lexer, "'format'");
            if (isFormatKeyword(tok)) {
                tok = tok(lexer, "'text' or 'json'");
                if (SqlKeywords.isTextKeyword(tok) || SqlKeywords.isJsonKeyword(tok)) {
                    int format = SqlKeywords.isJsonKeyword(tok) ? ExplainModel.FORMAT_JSON : ExplainModel.FORMAT_TEXT;
                    tok = tok(lexer, "')'");
                    if (!Chars.equals(tok, ')')) {
                        throw SqlException.$((lexer.lastTokenPosition()), "unexpected explain option found");
                    }
                    return format;
                } else {
                    throw SqlException.$((lexer.lastTokenPosition()), "unexpected explain format found");
                }
            } else {
                lexer.backTo(parenthesisPos, explainTok);
                return ExplainModel.FORMAT_TEXT;
            }
        } else {
            lexer.unparseLast();
            return ExplainModel.FORMAT_TEXT;
        }
    }

    private void parseFromClause(GenericLexer lexer, QueryModel model, QueryModel masterModel) throws SqlException {
        CharSequence tok = expectTableNameOrSubQuery(lexer);
        // expect "(" in case of sub-query

        if (Chars.equals(tok, '(')) {
            QueryModel proposedNested = parseAsSubQueryAndExpectClosingBrace(lexer, masterModel.getWithClauses(), true);
            tok = optTok(lexer);

            // do not collapse aliased sub-queries or those that have timestamp()
            // select * from (table) x
            if (tok == null || (tableAliasStop.contains(tok) && !SqlKeywords.isTimestampKeyword(tok))) {
                final QueryModel target = proposedNested.getNestedModel();
                // when * is artificial, there is no union, there is no "where" clause inside sub-query,
                // e.g. there was no "select * from" we should collapse sub-query to a regular table
                if (
                        proposedNested.isArtificialStar()
                                && proposedNested.getUnionModel() == null
                                && target.getWhereClause() == null
                                && target.getOrderBy().size() == 0
                                && target.getLatestBy().size() == 0
                                && target.getNestedModel() == null
                                && target.getSampleBy() == null
                                && target.getGroupBy().size() == 0
                                && proposedNested.getLimitLo() == null
                                && proposedNested.getLimitHi() == null
                ) {
                    model.setTableNameExpr(target.getTableNameExpr());
                    model.setAlias(target.getAlias());
                    model.setTimestamp(target.getTimestamp());

                    int n = target.getJoinModels().size();
                    for (int i = 1; i < n; i++) {
                        model.addJoinModel(target.getJoinModels().getQuick(i));
                    }
                    proposedNested = null;
                } else {
                    lexer.unparseLast();
                }
            } else {
                lexer.unparseLast();
            }

            if (proposedNested != null) {
                model.setNestedModel(proposedNested);
                model.setNestedModelIsSubQuery(true);
                tok = setModelAliasAndTimestamp(lexer, model);
            }
        } else {
            lexer.unparseLast();
            parseSelectFrom(lexer, model, masterModel.getWithClauses());
            tok = setModelAliasAndTimestamp(lexer, model);

            // expect [latest by] (deprecated syntax)
            if (tok != null && isLatestKeyword(tok)) {
                parseLatestBy(lexer, model);
                tok = optTok(lexer);
            }
        }

        // expect multiple [[inner | outer | cross] join]
        int joinType;
        while (tok != null && (joinType = joinStartSet.get(tok)) != -1) {
            model.addJoinModel(parseJoin(lexer, tok, joinType, masterModel.getWithClauses()));
            tok = optTok(lexer);
        }

        checkSupportedJoinType(lexer, tok);

        // expect [where]

        if (tok != null && isWhereKeyword(tok)) {
            if (model.getLatestByType() == QueryModel.LATEST_BY_NEW) {
                throw SqlException.$((lexer.lastTokenPosition()), "unexpected where clause after 'latest on'");
            }
            ExpressionNode expr = expr(lexer, model);
            if (expr != null) {
                model.setWhereClause(expr);
                tok = optTok(lexer);
            } else {
                throw SqlException.$((lexer.lastTokenPosition()), "empty where clause");
            }
        }

        // expect [latest by] (new syntax)

        if (tok != null && isLatestKeyword(tok)) {
            if (model.getLatestByType() == QueryModel.LATEST_BY_DEPRECATED) {
                throw SqlException.$((lexer.lastTokenPosition()), "mix of new and deprecated 'latest by' syntax");
            }
            expectTok(lexer, "on");
            parseLatestByNew(lexer, model);
            tok = optTok(lexer);
        }

        // expect [sample by]

        if (tok != null && isSampleKeyword(tok)) {
            expectBy(lexer);
            expectSample(lexer, model);
            tok = optTok(lexer);

            if (tok != null && isFillKeyword(tok)) {
                expectTok(lexer, '(');
                do {
                    final ExpressionNode fillNode = expr(lexer, model);
                    if (fillNode == null) {
                        throw SqlException.$(lexer.lastTokenPosition(), "'none', 'prev', 'mid', 'null' or number expected");
                    }
                    model.addSampleByFill(fillNode);
                    tok = tokIncludingLocalBrace(lexer, "',' or ')'");
                    if (Chars.equals(tok, ')')) {
                        break;
                    }
                    expectTok(tok, lexer.lastTokenPosition(), ',');
                } while (true);

                tok = optTok(lexer);
            }

            if (tok != null && isAlignKeyword(tok)) {
                expectTo(lexer);

                tok = tok(lexer, "'calendar' or 'first observation'");

                if (isCalendarKeyword(tok)) {
                    tok = optTok(lexer);
                    if (tok == null) {
                        model.setSampleByTimezoneName(null);
                        model.setSampleByOffset(ZERO_OFFSET);
                    } else if (isTimeKeyword(tok)) {
                        expectZone(lexer);
                        model.setSampleByTimezoneName(expectExpr(lexer));
                        tok = optTok(lexer);
                        if (tok != null && isWithKeyword(tok)) {
                            tok = parseWithOffset(lexer, model);
                        } else {
                            model.setSampleByOffset(ZERO_OFFSET);
                        }
                    } else if (isWithKeyword(tok)) {
                        tok = parseWithOffset(lexer, model);
                    } else {
                        model.setSampleByTimezoneName(null);
                        model.setSampleByOffset(ZERO_OFFSET);
                    }
                } else if (isFirstKeyword(tok)) {
                    expectObservation(lexer);
                    model.setSampleByTimezoneName(null);
                    model.setSampleByOffset(null);
                    tok = optTok(lexer);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'calendar' or 'first observation' expected");
                }
            }
        }

        // expect [group by]

        if (tok != null && isGroupKeyword(tok)) {
            expectBy(lexer);
            do {
                tokIncludingLocalBrace(lexer, "literal");
                lexer.unparseLast();
                ExpressionNode n = expr(lexer, model);
                if (n == null || (n.type != ExpressionNode.LITERAL && n.type != ExpressionNode.CONSTANT && n.type != ExpressionNode.FUNCTION && n.type != ExpressionNode.OPERATION)) {
                    throw SqlException.$(n == null ? lexer.lastTokenPosition() : n.position, "literal expected");
                }

                model.addGroupBy(n);

                tok = optTok(lexer);
            } while (tok != null && Chars.equals(tok, ','));
        }

        // expect [order by]

        if (tok != null && isOrderKeyword(tok)) {
            model.setOrderByPosition(lexer.lastTokenPosition());
            expectBy(lexer);
            do {
                tokIncludingLocalBrace(lexer, "literal");
                lexer.unparseLast();

                ExpressionNode n = expr(lexer, model);
                if (n == null || (n.type == ExpressionNode.QUERY || n.type == ExpressionNode.SET_OPERATION)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "literal or expression expected");
                }

                if ((n.type == ExpressionNode.CONSTANT && Chars.equals("''", n.token)) ||
                        (n.type == ExpressionNode.LITERAL && n.token.length() == 0)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "non-empty literal or expression expected");
                }

                tok = optTok(lexer);

                if (tok != null && isDescKeyword(tok)) {

                    model.addOrderBy(n, QueryModel.ORDER_DIRECTION_DESCENDING);
                    tok = optTok(lexer);

                } else {

                    model.addOrderBy(n, QueryModel.ORDER_DIRECTION_ASCENDING);

                    if (tok != null && isAscKeyword(tok)) {
                        tok = optTok(lexer);
                    }
                }

                if (model.getOrderBy().size() >= MAX_ORDER_BY_COLUMNS) {
                    throw err(lexer, tok, "Too many columns");
                }

            } while (tok != null && Chars.equals(tok, ','));
        }

        // expect [limit]
        if (tok != null && isLimitKeyword(tok)) {
            model.setLimitPosition(lexer.lastTokenPosition());
            ExpressionNode lo = expr(lexer, model);
            ExpressionNode hi = null;

            tok = optTok(lexer);
            if (tok != null && Chars.equals(tok, ',')) {
                hi = expr(lexer, model);
            } else {
                lexer.unparseLast();
            }
            model.setLimit(lo, hi);
        } else {
            lexer.unparseLast();
        }
    }

    private ExecutionModel parseInsert(GenericLexer lexer) throws SqlException {

        final InsertModel model = insertModelPool.next();
        CharSequence tok = tok(lexer, "into or batch");
        if (SqlKeywords.isBatchKeyword(tok)) {
            long val = expectLong(lexer);
            if (val > 0) {
                model.setBatchSize(val);
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "batch size must be positive integer");
            }

            tok = tok(lexer, "into or o3MaxLag");
            if (SqlKeywords.isO3MaxLagKeyword(tok)) {
                int pos = lexer.getPosition();
                model.setO3MaxLag(SqlUtil.expectMicros(tok(lexer, "lag value"), pos));
                expectTok(lexer, "into");
            }
        }

        if (!SqlKeywords.isIntoKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'into' expected");
        }

        tok = tok(lexer, "table name");
        SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        model.setTableName(nextLiteral(GenericLexer.assertNoDotsAndSlashes(GenericLexer.unquote(tok), lexer.lastTokenPosition()), lexer.lastTokenPosition()));

        tok = tok(lexer, "'(' or 'select'");

        if (Chars.equals(tok, '(')) {
            do {
                tok = tok(lexer, "column");
                if (Chars.equals(tok, ')')) {
                    throw err(lexer, tok, "missing column name");
                }

                SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                model.addColumn(GenericLexer.unquote(tok), lexer.lastTokenPosition());

            } while (Chars.equals((tok = tok(lexer, "','")), ','));

            expectTok(tok, lexer.lastTokenPosition(), ')');
            tok = optTok(lexer);
        }

        if (tok == null) {
            throw SqlException.$(lexer.getPosition(), "'select' or 'values' expected");
        }

        if (isSelectKeyword(tok)) {
            model.setSelectKeywordPosition(lexer.lastTokenPosition());
            lexer.unparseLast();
            final QueryModel queryModel = parseDml(lexer, null, lexer.lastTokenPosition(), true);
            model.setQueryModel(queryModel);
            return model;
        }

        if (isValuesKeyword(tok)) {
            do {
                expectTok(lexer, '(');
                ObjList<ExpressionNode> rowValues = new ObjList<>();
                do {
                    rowValues.add(expectExpr(lexer));
                } while (Chars.equals((tok = tok(lexer, "','")), ','));
                expectTok(tok, lexer.lastTokenPosition(), ')');
                model.addRowTupleValues(rowValues);
                model.addEndOfRowTupleValuesPosition(lexer.lastTokenPosition());
                tok = optTok(lexer);
                // no more tokens or ';' should indicate end of statement
                if (tok == null || Chars.equals(tok, ';')) {
                    return model;
                }
                expectTok(tok, lexer.lastTokenPosition(), ',');
            } while (true);
        }

        throw err(lexer, tok, "'select' or 'values' expected");
    }

    private QueryModel parseJoin(GenericLexer lexer, CharSequence tok, int joinType, LowerCaseCharSequenceObjHashMap<WithClauseModel> parent) throws SqlException {
        QueryModel joinModel = queryModelPool.next();

        int errorPos = lexer.lastTokenPosition();

        if (isNotJoinKeyword(tok) && !Chars.equals(tok, ',')) {
            // not already a join?
            // was it "left" ?
            if (isLeftKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = QueryModel.JOIN_OUTER;
                if (isOuterKeyword(tok)) {
                    tok = tok(lexer, "join");
                }
            } else {
                tok = tok(lexer, "join");
            }
            if (isNotJoinKeyword(tok)) {
                throw SqlException.position(errorPos).put("'join' expected");
            }
        }

        joinModel.setJoinType(joinType);
        joinModel.setJoinKeywordPosition(errorPos);

        tok = expectTableNameOrSubQuery(lexer);

        if (Chars.equals(tok, '(')) {
            joinModel.setNestedModel(parseAsSubQueryAndExpectClosingBrace(lexer, parent, true));
        } else {
            lexer.unparseLast();
            parseSelectFrom(lexer, joinModel, parent);
        }

        tok = setModelAliasAndGetOptTok(lexer, joinModel);

        if (joinType == QueryModel.JOIN_CROSS && tok != null && isOnKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "Cross joins cannot have join clauses");
        }

        switch (joinType) {
            case QueryModel.JOIN_ASOF:
            case QueryModel.JOIN_LT:
            case QueryModel.JOIN_SPLICE:
                if (tok == null || !isOnKeyword(tok)) {
                    lexer.unparseLast();
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
                                joinModel.setJoinCriteria(rewriteKnownStatements(expr));
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
                lexer.unparseLast();
                break;
        }

        return joinModel;
    }

    private void parseLatestBy(GenericLexer lexer, QueryModel model) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok != null) {
            if (isByKeyword(tok)) {
                parseLatestByDeprecated(lexer, model);
                return;
            }
            if (isOnKeyword(tok)) {
                parseLatestByNew(lexer, model);
                return;
            }
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'on' or 'by' expected");
    }

    private void parseLatestByDeprecated(GenericLexer lexer, QueryModel model) throws SqlException {
        // 'latest by' is already parsed at this point

        CharSequence tok;
        do {
            model.addLatestBy(expectLiteral(lexer));
            tok = SqlUtil.fetchNext(lexer);
        } while (Chars.equalsNc(tok, ','));

        model.setLatestByType(QueryModel.LATEST_BY_DEPRECATED);

        if (tok != null) {
            lexer.unparseLast();
        }
    }

    private void parseLatestByNew(GenericLexer lexer, QueryModel model) throws SqlException {
        // 'latest on' is already parsed at this point

        // <timestamp>
        final ExpressionNode timestamp = expectLiteral(lexer);
        model.setTimestamp(timestamp);
        // 'partition by'
        expectTok(lexer, "partition");
        expectTok(lexer, "by");
        // <columns>
        CharSequence tok;
        do {
            model.addLatestBy(expectLiteral(lexer));
            tok = SqlUtil.fetchNext(lexer);
        } while (Chars.equalsNc(tok, ','));

        model.setLatestByType(QueryModel.LATEST_BY_NEW);

        if (tok != null) {
            lexer.unparseLast();
        }
    }

    private void parseLikeTableName(GenericLexer lexer, CreateTableModel model) throws SqlException {
        CharSequence tok;
        // todo: validate keyword usage
        tok = tok(lexer, "table name");
        model.setLikeTableName(nextLiteral(GenericLexer.assertNoDotsAndSlashes(GenericLexer.unquote(tok), lexer.lastTokenPosition()), lexer.lastTokenPosition()));
        tok = tok(lexer, ")");
        if (!Chars.equals(tok, ')')) {
            throw errUnexpected(lexer, tok);
        }
        tok = optTok(lexer);
        if (tok != null && !Chars.equals(tok, ';')) {
            throw errUnexpected(lexer, tok);
        }
    }

    private ExecutionModel parseRenameStatement(GenericLexer lexer) throws SqlException {
        expectTok(lexer, "table");
        RenameTableModel model = renameTableModelPool.next();

        CharSequence tok = tok(lexer, "from table name");
        SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());

        model.setFrom(nextLiteral(GenericLexer.unquote(tok), lexer.lastTokenPosition()));


        tok = tok(lexer, "to");
        if (Chars.equals(tok, '(')) {
            throw SqlException.$(lexer.lastTokenPosition(), "function call is not allowed here");
        }
        lexer.unparseLast();

        expectTok(lexer, "to");

        tok = tok(lexer, "to table name");
        SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        model.setTo(nextLiteral(GenericLexer.unquote(tok), lexer.lastTokenPosition()));

        tok = optTok(lexer);

        if (tok != null && Chars.equals(tok, '(')) {
            throw SqlException.$(lexer.lastTokenPosition(), "function call is not allowed here");
        }

        if (tok != null && !Chars.equals(tok, ';')) {
            throw SqlException.$(lexer.lastTokenPosition(), "debris?");
        }

        return model;
    }

    private ExecutionModel parseSelect(GenericLexer lexer) throws SqlException {
        lexer.unparseLast();
        final QueryModel model = parseDml(lexer, null, lexer.lastTokenPosition(), true);
        final CharSequence tok = optTok(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            return model;
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseSelectClause(GenericLexer lexer, QueryModel model) throws SqlException {
        CharSequence tok = tok(lexer, "[distinct] column");

        ExpressionNode expr;
        if (isDistinctKeyword(tok)) {
            model.setDistinct(true);
        } else {
            lexer.unparseLast();
        }

        try {
            boolean hasFrom = false;

            while (true) {

                tok = tok(lexer, "column");
                if (Chars.equals(tok, '*')) {
                    expr = nextLiteral(GenericLexer.immutableOf(tok), lexer.lastTokenPosition());
                } else {
                    // cut off some obvious errors
                    if (isFromKeyword(tok)) {
                        if (accumulatedColumns.size() == 0) {
                            throw SqlException.$(lexer.lastTokenPosition(), "column expression expected");
                        }
                        hasFrom = true;
                        lexer.unparseLast();
                        break;
                    }

                    if (isSelectKeyword(tok)) {
                        throw SqlException.$(lexer.getPosition(), "reserved name");
                    }

                    lexer.unparseLast();
                    expr = expr(lexer, model);

                    if (expr == null) {
                        throw SqlException.$(lexer.lastTokenPosition(), "missing expression");
                    }

                    if (Chars.endsWith(expr.token, '.') && expr.type == ExpressionNode.LITERAL) {
                        throw SqlException.$(expr.position + expr.token.length(), "'*' or column name expected");
                    }
                }

                final CharSequence alias;

                tok = optTok(lexer);

                QueryColumn col;
                final int colPosition = lexer.lastTokenPosition();

                if (tok != null && isOverKeyword(tok)) {
                    // analytic/window function
                    expectTok(lexer, '(');

                    AnalyticColumn winCol = analyticColumnPool.next().of(null, expr);
                    col = winCol;

                    tok = tokIncludingLocalBrace(lexer, "'partition' or 'order' or ')'");

                    if (isPartitionKeyword(tok)) {
                        expectTok(lexer, "by");

                        ObjList<ExpressionNode> partitionBy = winCol.getPartitionBy();

                        do {
                            // allow dangling comma by previewing the token
                            tok = tok(lexer, "column name, 'order' or ')'");
                            if (SqlKeywords.isOrderKeyword(tok)) {
                                if (partitionBy.size() == 0) {
                                    throw SqlException.$(lexer.lastTokenPosition(), "at least one column is expected in `partition by` clause");
                                }
                                break;
                            }
                            lexer.unparseLast();
                            partitionBy.add(expectExpr(lexer));
                            tok = tok(lexer, "'order' or ')'");
                        } while (Chars.equals(tok, ','));
                    }

                    if (isOrderKeyword(tok)) {
                        expectTok(lexer, "by");

                        do {
                            final ExpressionNode orderByExpr = expectExpr(lexer);

                            tok = tokIncludingLocalBrace(lexer, "'asc' or 'desc'");

                            if (isDescKeyword(tok)) {
                                winCol.addOrderBy(orderByExpr, QueryModel.ORDER_DIRECTION_DESCENDING);
                                tok = tokIncludingLocalBrace(lexer, "',' or ')'");
                            } else {
                                winCol.addOrderBy(orderByExpr, QueryModel.ORDER_DIRECTION_ASCENDING);
                                if (isAscKeyword(tok)) {
                                    tok = tokIncludingLocalBrace(lexer, "',' or ')'");
                                }
                            }
                        } while (Chars.equals(tok, ','));
                    }
                    int framingMode = -1;
                    if (isRowsKeyword(tok)) {
                        framingMode = AnalyticColumn.FRAMING_ROWS;
                    } else if (isRangeKeyword(tok)) {
                        framingMode = AnalyticColumn.FRAMING_RANGE;
                    } else if (isGroupKeyword(tok)) {
                        framingMode = AnalyticColumn.FRAMING_GROUP;
                    }

                    if (framingMode != -1) {

                        winCol.setFramingMode(framingMode);

                        // These keywords define for each row a window (a physical or logical
                        // set of rows) used for calculating the function result. The function is
                        // then applied to all the rows in the window. The window moves through the
                        // query result set or partition from top to bottom.

                        /*
                        { ROWS | RANGE }
                        { BETWEEN
                            { UNBOUNDED PRECEDING
                            | CURRENT ROW
                            | value_expr { PRECEDING | FOLLOWING }
                            }
                            AND
                            { UNBOUNDED FOLLOWING
                            | CURRENT ROW
                            | value_expr { PRECEDING | FOLLOWING }
                            }
                        | { UNBOUNDED PRECEDING
                          | CURRENT ROW
                          | value_expr PRECEDING
                          }
                        }
                        */
                        tok = tok(lexer, "'between', 'unbounded', 'current' or expression");
                        if (isBetweenKeyword(tok)) {

                            // Use the BETWEEN ... AND clause to specify a start point and end point for the window.
                            // The first expression (before AND) defines the start point and the second
                            // expression (after AND) defines the end point.

                            // If you omit BETWEEN and specify only one end point, then Oracle considers it the start
                            // point, and the end point defaults to the current row.

                            tok = tok(lexer, "'unbounded', 'current' or expression");
                            // lo
                            if (isUnboundedPreceding(lexer, tok)) {
                                // Specify UNBOUNDED PRECEDING to indicate that the window starts at the first
                                // row of the partition. This is the start point specification and cannot be
                                // used as an end point specification.
                                winCol.setRowsLoKind(AnalyticColumn.PRECEDING, lexer.lastTokenPosition());
                            } else if (isCurrentRow(lexer, tok)) {
                                // As a start point, CURRENT ROW specifies that the window begins at the current row.
                                // In this case the end point cannot be value_expr PRECEDING.
                                winCol.setRowsLoKind(AnalyticColumn.CURRENT, lexer.lastTokenPosition());
                            } else {
                                int pos = lexer.lastTokenPosition();
                                lexer.unparseLast();
                                winCol.setRowsLoExpr(expectExpr(lexer), pos);
                                tok = tok(lexer, "'preceding' or 'following'");
                                if (SqlKeywords.isPrecedingKeyword(tok)) {
                                    winCol.setRowsLoKind(AnalyticColumn.PRECEDING, lexer.lastTokenPosition());
                                } else if (SqlKeywords.isFollowingKeyword(tok)) {
                                    winCol.setRowsLoKind(AnalyticColumn.FOLLOWING, lexer.lastTokenPosition());
                                } else {
                                    throw SqlException.$(lexer.lastTokenPosition(), "'preceding' or 'following' expected");
                                }
                            }

                            tok = tok(lexer, "'and'");

                            if (SqlKeywords.isAndKeyword(tok)) {
                                tok = tok(lexer, "'unbounded', 'current' or expression");
                                // hi
                                if (SqlKeywords.isUnboundedKeyword(tok)) {
                                    tok = tok(lexer, "'following'");
                                    if (SqlKeywords.isFollowingKeyword(tok)) {
                                        // Specify UNBOUNDED FOLLOWING to indicate that the window ends at the
                                        // last row of the partition. This is the end point specification and
                                        // cannot be used as a start point specification.
                                        winCol.setRowsHiKind(AnalyticColumn.FOLLOWING, lexer.lastTokenPosition());
                                    } else {
                                        throw SqlException.$(lexer.lastTokenPosition(), "'following' expected");
                                    }
                                } else if (isCurrentRow(lexer, tok)) {
                                    winCol.setRowsHiKind(AnalyticColumn.CURRENT, lexer.lastTokenPosition());
                                } else {
                                    int pos = lexer.lastTokenPosition();
                                    lexer.unparseLast();
                                    winCol.setRowsHiExpr(expectExpr(lexer), pos);
                                    tok = tok(lexer, "'preceding'  'following'");
                                    if (SqlKeywords.isPrecedingKeyword(tok)) {
                                        if (winCol.getRowsLoKind() == AnalyticColumn.CURRENT) {
                                            // As a start point, CURRENT ROW specifies that the window begins at the current row.
                                            // In this case the end point cannot be value_expr PRECEDING.
                                            throw SqlException.$(lexer.lastTokenPosition(), "start row is CURRENT, end row not must be PRECEDING");
                                        }
                                        winCol.setRowsHiKind(AnalyticColumn.PRECEDING, lexer.lastTokenPosition());
                                    } else if (SqlKeywords.isFollowingKeyword(tok)) {
                                        winCol.setRowsHiKind(AnalyticColumn.FOLLOWING, lexer.lastTokenPosition());
                                    } else {
                                        throw SqlException.$(lexer.lastTokenPosition(), "'preceding' or 'following' expected");
                                    }
                                }
                            } else {
                                throw SqlException.$(lexer.lastTokenPosition(), "'and' expected");
                            }
                        } else {
                            // If you omit BETWEEN and specify only one end point, then QuestDB considers it the
                            // start point, and the end point defaults to the current row.
                            int pos = lexer.lastTokenPosition();
                            if (isUnboundedPreceding(lexer, tok)) {
                                winCol.setRowsLoKind(AnalyticColumn.PRECEDING, lexer.lastTokenPosition());
                            } else if (isCurrentRow(lexer, tok)) {
                                winCol.setRowsLoKind(AnalyticColumn.CURRENT, lexer.lastTokenPosition());
                            } else {
                                lexer.unparseLast();
                                winCol.setRowsLoExpr(expectExpr(lexer), pos);
                                tok = tok(lexer, "'preceding'");
                                if (SqlKeywords.isPrecedingKeyword(tok)) {
                                    winCol.setRowsLoKind(AnalyticColumn.PRECEDING, lexer.lastTokenPosition());
                                } else {
                                    throw SqlException.$(lexer.lastTokenPosition(), "'preceding' expected");
                                }
                            }

                            winCol.setRowsHiKind(AnalyticColumn.CURRENT, pos);
                        }
                        tok = tok(lexer, "'exclude' or ')' expected");

                        if (isExcludeKeyword(tok)) {
                            tok = tok(lexer, "'current', 'group', 'ties' or 'no other' expected");
                            if (SqlKeywords.isCurrentKeyword(tok)) {
                                tok = tok(lexer, "'row' expected");
                                if (SqlKeywords.isRowKeyword(tok)) {
                                    winCol.setExclusionKind(AnalyticColumn.EXCLUDE_CURRENT_ROW);
                                } else {
                                    throw SqlException.$(lexer.lastTokenPosition(), "'row' expected");
                                }
                            } else if (SqlKeywords.isGroupKeyword(tok)) {
                                winCol.setExclusionKind(AnalyticColumn.EXCLUDE_GROUP);
                            } else if (SqlKeywords.isTiesKeyword(tok)) {
                                winCol.setExclusionKind(AnalyticColumn.EXCLUDE_TIES);
                            } else if (SqlKeywords.isNoKeyword(tok)) {
                                tok = tok(lexer, "'others' expected");
                                if (SqlKeywords.isOthersKeyword(tok)) {
                                    winCol.setExclusionKind(AnalyticColumn.EXCLUDE_NO_OTHERS);
                                } else {
                                    throw SqlException.$(lexer.lastTokenPosition(), "'others' expected");
                                }
                            } else {
                                throw SqlException.$(lexer.lastTokenPosition(), "'current', 'group', 'ties' or 'no other' expected");
                            }

                            tok = tok(lexer, "')' expected");
                        }
                    }
                    expectTok(tok, lexer.lastTokenPosition(), ')');
                    tok = optTok(lexer);

                } else {
                    if (expr.type == ExpressionNode.QUERY) {
                        throw SqlException.$(expr.position, "query is not expected, did you mean column?");
                    }
                    col = queryColumnPool.next().of(null, expr);
                }

                if (tok != null && columnAliasStop.excludes(tok)) {
                    assertNotDot(lexer, tok);

                    // verify that * wildcard is not aliased

                    if (isAsKeyword(tok)) {
                        tok = tok(lexer, "alias");
                        SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                        CharSequence aliasTok = GenericLexer.immutableOf(tok);
                        validateIdentifier(lexer, aliasTok);
                        alias = GenericLexer.unquote(aliasTok);
                    } else {
                        validateIdentifier(lexer, tok);
                        SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                        alias = GenericLexer.immutableOf(GenericLexer.unquote(tok));
                    }

                    if (col.getAst().isWildcard()) {
                        throw err(lexer, null, "wildcard cannot have alias");
                    }

                    tok = optTok(lexer);
                    aliasMap.put(alias, col);
                } else {
                    alias = null;
                }

                // correlated sub-queries do not have expr.token values (they are null)
                if (expr.type == ExpressionNode.QUERY) {
                    expr.token = alias;
                }

                if (alias != null) {
                    if (alias.length() == 0) {
                        throw err(lexer, null, "column alias cannot be a blank string");
                    }
                    col.setAlias(alias);
                }

                accumulatedColumns.add(col);
                accumulatedColumnPositions.add(colPosition);

                if (tok == null || Chars.equals(tok, ';') || Chars.equals(tok, ')')) {//accept ending ) in create table as
                    lexer.unparseLast();
                    break;
                }

                if (isFromKeyword(tok)) {
                    hasFrom = true;
                    lexer.unparseLast();
                    break;
                }

                if (setOperations.contains(tok)) {
                    lexer.unparseLast();
                    break;
                }

                if (!Chars.equals(tok, ',')) {
                    throw err(lexer, tok, "',', 'from' or 'over' expected");
                }
            }

            for (int i = 0, n = accumulatedColumns.size(); i < n; i++) {
                QueryColumn qc = accumulatedColumns.getQuick(i);
                if (qc.getAlias() == null) {
                    CharSequence token = qc.getAst().token;
                    if (qc.getAst().isWildcard() && !hasFrom) {
                        throw err(lexer, null, "'from' expected");
                    }
                    CharSequence alias;
                    if (qc.getAst().type == ExpressionNode.CONSTANT && Chars.indexOf(token, '.') != -1) {
                        alias = createConstColumnAlias(aliasMap);
                    } else {
                        alias = createColumnAlias(qc.getAst(), aliasMap);
                    }
                    qc.setAlias(alias);
                    aliasMap.put(alias, qc);
                }
                model.addBottomUpColumn(accumulatedColumnPositions.getQuick(i), qc, false);
            }
        } finally {
            accumulatedColumns.clear();
            accumulatedColumnPositions.clear();
            aliasMap.clear();
        }
    }

    private void parseSelectFrom(GenericLexer lexer, QueryModel model, LowerCaseCharSequenceObjHashMap<WithClauseModel> masterModel) throws SqlException {
        final ExpressionNode expr = expr(lexer, model);
        if (expr == null) {
            throw SqlException.position(lexer.lastTokenPosition()).put("table name expected");
        }
        CharSequence tableName = expr.token;

        // todo: validate table name for overlap with keywords
        switch (expr.type) {
            case ExpressionNode.LITERAL:
            case ExpressionNode.CONSTANT:
                final ExpressionNode literal = literal(tableName, expr.position);
                final WithClauseModel withClause = masterModel.get(tableName);
                if (withClause != null) {
                    model.setNestedModel(parseWith(lexer, withClause));
                    model.setAlias(literal);
                } else {
                    model.setTableNameExpr(literal);
                }
                break;
            case ExpressionNode.FUNCTION:
                model.setTableNameExpr(expr);
                break;
            default:
                throw SqlException.$(expr.position, "function, literal or constant is expected");
        }
    }

    private int parseSymbolCapacity(GenericLexer lexer) throws SqlException {
        final int errorPosition = lexer.getPosition();
        final int symbolCapacity = expectInt(lexer);
        TableUtils.validateSymbolCapacity(errorPosition, symbolCapacity);
        return Numbers.ceilPow2(symbolCapacity);
    }

    private ExpressionNode parseTimestamp(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (tok != null && isTimestampKeyword(tok)) {
            expectTok(lexer, '(');
            final ExpressionNode result = expectLiteral(lexer);
            tokIncludingLocalBrace(lexer, "')'");
            return result;
        }
        return null;
    }

    private ExecutionModel parseUpdate(GenericLexer lexer) throws SqlException {
        lexer.unparseLast();
        final QueryModel model = parseDmlUpdate(lexer);
        final CharSequence tok = optTok(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            return model;
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseUpdateClause(GenericLexer lexer, QueryModel updateQueryModel, QueryModel fromModel) throws SqlException {
        CharSequence tok = tok(lexer, "table name or alias");
        SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        CharSequence tableName = GenericLexer.immutableOf(GenericLexer.unquote(tok));
        ExpressionNode tableNameExpr = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, tableName, 0, 0);
        updateQueryModel.setTableNameExpr(tableNameExpr);
        fromModel.setTableNameExpr(tableNameExpr);

        tok = tok(lexer, "AS, SET or table alias expected");
        if (isAsKeyword(tok)) {
            tok = tok(lexer, "table alias expected");
            if (isSetKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "table alias expected");
            }
        }

        if (!isAsKeyword(tok) && !isSetKeyword(tok)) {
            // This is table alias
            CharSequence tableAlias = GenericLexer.immutableOf(tok);
            SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            ExpressionNode tableAliasExpr = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, tableAlias, 0, 0);
            updateQueryModel.setAlias(tableAliasExpr);
            tok = tok(lexer, "SET expected");
        }

        if (!isSetKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "SET expected");
        }

        while (true) {
            // Column
            tok = tok(lexer, "column name");
            CharSequence col = GenericLexer.immutableOf(GenericLexer.unquote(tok));
            int colPosition = lexer.lastTokenPosition();

            expectTok(lexer, "=");

            // Value expression
            ExpressionNode expr = expr(lexer, (QueryModel) null);
            ExpressionNode setColumnExpression = expressionNodePool.next().of(ExpressionNode.LITERAL, col, 0, colPosition);
            updateQueryModel.getUpdateExpressions().add(setColumnExpression);

            QueryColumn valueColumn = queryColumnPool.next().of(col, expr);
            fromModel.addBottomUpColumn(colPosition, valueColumn, false, "in SET clause");

            tok = optTok(lexer);
            if (tok == null) {
                break;
            }

            if (tok.length() != 1 || tok.charAt(0) != ',') {
                lexer.unparseLast();
                break;
            }
        }
    }

    @NotNull
    private ExecutionModel parseWith(GenericLexer lexer) throws SqlException {
        parseWithClauses(lexer, topLevelWithModel);
        CharSequence tok = tok(lexer, "'select', 'update' or name expected");
        if (isSelectKeyword(tok)) {
            lexer.unparseLast();
            return parseDml(lexer, null, lexer.lastTokenPosition(), true);
        }

        if (isUpdateKeyword(tok)) {
            return parseUpdate(lexer);
        }

        if (isInsertKeyword(tok)) {
            return parseInsert(lexer);
        }

        throw SqlException.$(lexer.lastTokenPosition(), "'select' | 'update' | 'insert' expected");
    }

    private QueryModel parseWith(GenericLexer lexer, WithClauseModel wcm) throws SqlException {
        QueryModel m = wcm.popModel();
        if (m != null) {
            return m;
        }

        lexer.stash();
        lexer.goToPosition(wcm.getPosition());
        // this will not throw exception because this is second pass over the same sub-query
        // we wouldn't be here is syntax was wrong
        m = parseAsSubQueryAndExpectClosingBrace(lexer, wcm.getWithClauses(), false);
        lexer.unstash();
        return m;
    }

    private void parseWithClauses(GenericLexer lexer, LowerCaseCharSequenceObjHashMap<WithClauseModel> model) throws SqlException {
        do {
            ExpressionNode name = expectLiteral(lexer);
            if (name.token.length() == 0) {
                throw SqlException.$(name.position, "empty common table expression name");
            }

            if (model.get(name.token) != null) {
                throw SqlException.$(name.position, "duplicate name");
            }

            expectTok(lexer, "as");
            expectTok(lexer, '(');
            int lo = lexer.lastTokenPosition();
            WithClauseModel wcm = withClauseModelPool.next();
            wcm.of(lo + 1, model, parseAsSubQueryAndExpectClosingBrace(lexer, model, true));
            model.put(name.token, wcm);

            CharSequence tok = optTok(lexer);
            if (tok == null || !Chars.equals(tok, ',')) {
                lexer.unparseLast();
                break;
            }
        } while (true);
    }

    private CharSequence parseWithOffset(GenericLexer lexer, QueryModel model) throws SqlException {
        CharSequence tok;
        expectOffset(lexer);
        model.setSampleByOffset(expectExpr(lexer));
        tok = optTok(lexer);
        return tok;
    }

    private ExpressionNode rewriteCase(ExpressionNode parent) throws SqlException {
        traversalAlgo.traverse(parent, rewriteCase0Ref);
        return parent;
    }

    private void rewriteCase0(ExpressionNode node) {
        if (node.type == ExpressionNode.FUNCTION && isCaseKeyword(node.token)) {
            tempExprNodes.clear();
            ExpressionNode literal = null;
            ExpressionNode elseExpr;
            boolean convertToSwitch = true;
            final int paramCount = node.paramCount;

            final int lim;
            if ((paramCount & 1) == 0) {
                elseExpr = node.args.getQuick(0);
                lim = 0;
            } else {
                elseExpr = null;
                lim = -1;
            }

            // args are in inverted order, hence last list item is the first arg
            ExpressionNode first = node.args.getQuick(paramCount - 1);
            if (first.token != null) {
                // simple case of 'case' :) e.g.
                // case x
                //   when 1 then 'A'
                //   ...
                node.token = "switch";
                return;
            }
            int thenRemainder = elseExpr == null ? 0 : 1;
            for (int i = paramCount - 2; i > lim; i--) {
                if ((i & 1) == thenRemainder) {
                    // this is "then" clause, copy it as is
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
                // remove the 'null' marker arg
                node.args.remove(paramCount - 1);
                node.paramCount = paramCount - 1;

                // 2 args 'case', e.g. case when x>0 then 1
                if (node.paramCount < 3) {
                    node.rhs = node.args.get(0);
                    node.lhs = node.args.get(1);
                    node.args.clear();
                }
            }
        }
    }

    private ExpressionNode rewriteConcat(ExpressionNode parent) throws SqlException {
        traversalAlgo.traverse(parent, rewriteConcat0Ref);
        return parent;
    }

    private void rewriteConcat0(ExpressionNode node) {
        if (node.type == ExpressionNode.OPERATION && isConcatOperator(node.token)) {
            node.type = ExpressionNode.FUNCTION;
            node.token = CONCAT_FUNC_NAME;
            addConcatArgs(node.args, node.rhs);
            addConcatArgs(node.args, node.lhs);
            node.paramCount = node.args.size();
        }
    }

    private ExpressionNode rewriteCount(ExpressionNode parent) throws SqlException {
        traversalAlgo.traverse(parent, rewriteCount0Ref);
        return parent;
    }

    /**
     * Rewrites count(*) expressions to count().
     *
     * @param node expression node, provided by tree walking algo
     */
    private void rewriteCount0(ExpressionNode node) {
        if (node.type == ExpressionNode.FUNCTION && isCountKeyword(node.token)) {
            if (node.paramCount == 1) {
                // special case, typically something like
                // case value else expression end
                // this can be simplified to "expression" only

                ExpressionNode that = node.rhs;
                if (Chars.equalsNc(that.token, '*')) {
                    if (that.rhs == null && node.lhs == null) {
                        that.paramCount = 0;
                        node.rhs = null;
                        node.paramCount = 0;
                    }
                }
            }
        }
    }

    private ExpressionNode rewriteKnownStatements(ExpressionNode parent) throws SqlException {
        return rewritePgCast(
                rewriteConcat(
                        rewriteCase(
                                rewriteCount(
                                        parent
                                )
                        )
                )
        );
    }

    private ExpressionNode rewritePgCast(ExpressionNode parent) throws SqlException {
        traversalAlgo.traverse(parent, rewritePgCast0Ref);
        return parent;
    }

    private void rewritePgCast0(ExpressionNode node) {
        if (node.type == ExpressionNode.OPERATION && SqlKeywords.isColonColon(node.token)) {
            node.token = "cast";
            node.type = ExpressionNode.FUNCTION;
            node.rhs.type = ExpressionNode.CONSTANT;
            // In PG x::float casts x to "double precision" type
            if (SqlKeywords.isFloatKeyword(node.rhs.token) || SqlKeywords.isFloat8Keyword(node.rhs.token)) {
                node.rhs.token = "double";
            } else if (SqlKeywords.isFloat4Keyword(node.rhs.token)) {
                node.rhs.token = "float";
            } else if (SqlKeywords.isDateKeyword(node.rhs.token)) {
                node.token = "to_pg_date";
                node.rhs = node.lhs;
                node.lhs = null;
                node.paramCount = 1;
            }
        }
    }

    private CharSequence setModelAliasAndGetOptTok(GenericLexer lexer, QueryModel joinModel) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok != null && tableAliasStop.excludes(tok)) {
            checkSupportedJoinType(lexer, tok);
            if (SqlKeywords.isAsKeyword(tok)) {
                tok = tok(lexer, "alias");
            }
            if (tok.length() == 0 || SqlKeywords.isEmptyAlias(tok)) {
                throw SqlException.position(lexer.lastTokenPosition()).put("Empty table alias");
            }
            SqlKeywords.assertTableNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            joinModel.setAlias(literal(lexer, tok));
            tok = optTok(lexer);
        }
        return tok;
    }

    private CharSequence setModelAliasAndTimestamp(GenericLexer lexer, QueryModel model) throws SqlException {
        CharSequence tok;
        tok = setModelAliasAndGetOptTok(lexer, model);

        // expect [timestamp(column)]
        ExpressionNode timestamp = parseTimestamp(lexer, tok);
        if (timestamp != null) {
            model.setTimestamp(timestamp);
            model.setExplicitTimestamp(true);
            tok = optTok(lexer);
        }
        return tok;
    }

    private int toColumnType(GenericLexer lexer, CharSequence tok) throws SqlException {
        final short type = ColumnType.tagOf(tok);
        if (type == -1) {
            throw SqlException.$(lexer.lastTokenPosition(), "unsupported column type: ").put(tok);
        }
        if (ColumnType.GEOHASH == type) {
            expectTok(lexer, '(');
            final int bits = GeoHashUtil.parseGeoHashBits(lexer.lastTokenPosition(), 0, expectLiteral(lexer).token);
            expectTok(lexer, ')');
            return ColumnType.getGeoHashTypeWithBits(bits);
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

    private @NotNull CharSequence tokIncludingLocalBrace(GenericLexer lexer, String expectedList) throws SqlException {
        final int pos = lexer.getPosition();
        final CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null) {
            throw SqlException.position(pos).put(expectedList).put(" expected");
        }
        return tok;
    }

    private void validateIdentifier(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (tok == null || tok.length() == 0) {
            throw SqlException.position(lexer.lastTokenPosition()).put("non-empty identifier expected");
        }

        if (Chars.isQuoted(tok)) {
            if (tok.length() == 2) {
                throw SqlException.position(lexer.lastTokenPosition()).put("non-empty identifier expected");
            }
            return;
        }

        char c = tok.charAt(0);

        if (!(Character.isLetter(c) || c == '_')) {
            throw SqlException.position(lexer.lastTokenPosition()).put("identifier should start with a letter or '_'");
        }

        for (int i = 1, n = tok.length(); i < n; i++) {
            c = tok.charAt(i);
            if (!(Character.isLetter(c) ||
                    Character.isDigit(c) ||
                    c == '_' ||
                    c == '$')) {
                throw SqlException.position(lexer.lastTokenPosition()).put("identifier can contain letters, digits, '_' or '$'");
            }
        }
    }

    void clear() {
        queryModelPool.clear();
        queryColumnPool.clear();
        expressionNodePool.clear();
        analyticColumnPool.clear();
        createTableModelPool.clear();
        columnCastModelPool.clear();
        renameTableModelPool.clear();
        withClauseModelPool.clear();
        subQueryMode = false;
        characterStore.clear();
        insertModelPool.clear();
        expressionTreeBuilder.reset();
        copyModelPool.clear();
        topLevelWithModel.clear();
        explainModelPool.clear();
        digit = 1;
    }

    ExpressionNode expr(GenericLexer lexer, QueryModel model) throws SqlException {
        try {
            expressionTreeBuilder.pushModel(model);
            expressionParser.parseExpr(lexer, expressionTreeBuilder);
            return rewriteKnownStatements(expressionTreeBuilder.poll());
        } catch (SqlException e) {
            expressionTreeBuilder.reset();
            throw e;
        } finally {
            expressionTreeBuilder.popModel();
        }
    }

    // test only
    @TestOnly
    void expr(GenericLexer lexer, ExpressionParserListener listener) throws SqlException {
        expressionParser.parseExpr(lexer, listener);
    }

    ExecutionModel parse(GenericLexer lexer, SqlExecutionContext executionContext) throws SqlException {
        CharSequence tok = tok(lexer, "'create', 'rename' or 'select'");

        if (isExplainKeyword(tok)) {
            int format = parseExplainOptions(lexer, tok);
            ExecutionModel model = parseExplain(lexer, executionContext);
            ExplainModel explainModel = explainModelPool.next();
            explainModel.setFormat(format);
            explainModel.setModel(model);
            return explainModel;
        }

        if (isSelectKeyword(tok)) {
            return parseSelect(lexer);
        }

        if (isCreateKeyword(tok)) {
            return parseCreateStatement(lexer, executionContext);
        }

        if (isUpdateKeyword(tok)) {
            return parseUpdate(lexer);
        }

        if (isRenameKeyword(tok)) {
            return parseRenameStatement(lexer);
        }

        if (isInsertKeyword(tok)) {
            return parseInsert(lexer);
        }

        if (isCopyKeyword(tok)) {
            return parseCopy(lexer);
        }

        if (isWithKeyword(tok)) {
            return parseWith(lexer);
        }

        if (isFromKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "Did you mean 'select * from'?");
        }

        return parseSelect(lexer);
    }

    QueryModel parseAsSubQuery(GenericLexer lexer,
                               @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
                               boolean useTopLevelWithClauses)
            throws SqlException {
        QueryModel model;
        this.subQueryMode = true;
        try {
            model = parseDml(lexer, withClauses, lexer.getPosition(), useTopLevelWithClauses);
        } finally {
            this.subQueryMode = false;
        }
        return model;
    }

    static {
        tableAliasStop.add("where");
        tableAliasStop.add("latest");
        tableAliasStop.add("join");
        tableAliasStop.add("inner");
        tableAliasStop.add("left");
        tableAliasStop.add("outer");
        tableAliasStop.add("asof");
        tableAliasStop.add("splice");
        tableAliasStop.add("lt");
        tableAliasStop.add("cross");
        tableAliasStop.add("sample");
        tableAliasStop.add("order");
        tableAliasStop.add("on");
        tableAliasStop.add("timestamp");
        tableAliasStop.add("limit");
        tableAliasStop.add(")");
        tableAliasStop.add(";");
        tableAliasStop.add("union");
        tableAliasStop.add("group");
        tableAliasStop.add("except");
        tableAliasStop.add("intersect");
        tableAliasStop.add("from");
        //
        columnAliasStop.add("from");
        columnAliasStop.add(",");
        columnAliasStop.add("over");
        columnAliasStop.add("union");
        columnAliasStop.add("except");
        columnAliasStop.add("intersect");
        columnAliasStop.add(")");
        columnAliasStop.add(";");
        //
        groupByStopSet.add("order");
        groupByStopSet.add(")");
        groupByStopSet.add(",");

        joinStartSet.put("left", QueryModel.JOIN_INNER);
        joinStartSet.put("join", QueryModel.JOIN_INNER);
        joinStartSet.put("inner", QueryModel.JOIN_INNER);
        joinStartSet.put("left", QueryModel.JOIN_OUTER);//only left join is supported currently 
        joinStartSet.put("cross", QueryModel.JOIN_CROSS);
        joinStartSet.put("asof", QueryModel.JOIN_ASOF);
        joinStartSet.put("splice", QueryModel.JOIN_SPLICE);
        joinStartSet.put("lt", QueryModel.JOIN_LT);
        joinStartSet.put(",", QueryModel.JOIN_CROSS);
        //
        setOperations.add("union");
        setOperations.add("except");
        setOperations.add("intersect");
    }
}

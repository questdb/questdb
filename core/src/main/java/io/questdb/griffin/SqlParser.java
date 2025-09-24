/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.griffin.engine.functions.json.JsonExtractTypedFunctionFactory;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.engine.ops.CreateMatViewOperationBuilder;
import io.questdb.griffin.engine.ops.CreateMatViewOperationBuilderImpl;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilder;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilderImpl;
import io.questdb.griffin.model.CopyModel;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExplainModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.InsertModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.RenameTableModel;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.griffin.model.WithClauseModel;
import io.questdb.std.BufferWindowCharSequence;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseAsciiCharSequenceHashSet;
import io.questdb.std.LowerCaseAsciiCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Os;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.SqlWalMode.*;
import static io.questdb.griffin.SqlKeywords.*;
import static io.questdb.std.GenericLexer.assertNoDotsAndSlashes;
import static io.questdb.std.GenericLexer.unquote;

public class SqlParser {
    public static final int MAX_ORDER_BY_COLUMNS = 1560;
    public static final ExpressionNode ZERO_OFFSET = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, "'00:00'", 0, 0);
    private static final ExpressionNode ONE = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, "1", 0, 0);
    private static final LowerCaseAsciiCharSequenceHashSet columnAliasStop = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet groupByStopSet = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceIntHashMap joinStartSet = new LowerCaseAsciiCharSequenceIntHashMap();
    private static final LowerCaseAsciiCharSequenceHashSet setOperations = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet tableAliasStop = new LowerCaseAsciiCharSequenceHashSet();
    private static final IntList tableNamePositions = new IntList();
    private static final LowerCaseCharSequenceHashSet tableNames = new LowerCaseCharSequenceHashSet();
    private final IntList accumulatedColumnPositions = new IntList();
    private final ObjList<QueryColumn> accumulatedColumns = new ObjList<>();
    private final LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>();
    private final CharacterStore characterStore;
    private final CharSequence column;
    private final CairoConfiguration configuration;
    private final ObjectPool<CopyModel> copyModelPool;
    private final CreateMatViewOperationBuilderImpl createMatViewOperationBuilder = new CreateMatViewOperationBuilderImpl();
    private final ObjectPool<CreateTableColumnModel> createTableColumnModelPool;
    private final CreateTableOperationBuilderImpl createTableOperationBuilder = createMatViewOperationBuilder.getCreateTableOperationBuilder();
    private final ObjectPool<ExplainModel> explainModelPool;
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final ExpressionParser expressionParser;
    private final ExpressionTreeBuilder expressionTreeBuilder;
    private final ObjectPool<InsertModel> insertModelPool;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final ObjectPool<RenameTableModel> renameTableModelPool;
    private final PostOrderTreeTraversalAlgo.Visitor rewriteConcatRef = this::rewriteConcat;
    private final PostOrderTreeTraversalAlgo.Visitor rewriteCountRef = this::rewriteCount;
    private final RewriteDeclaredVariablesInExpressionVisitor rewriteDeclaredVariablesInExpressionVisitor = new RewriteDeclaredVariablesInExpressionVisitor();
    private final PostOrderTreeTraversalAlgo.Visitor rewriteJsonExtractCastRef = this::rewriteJsonExtractCast;
    private final PostOrderTreeTraversalAlgo.Visitor rewritePgCastRef = this::rewritePgCast;
    private final ObjList<ExpressionNode> tempExprNodes = new ObjList<>();
    private final PostOrderTreeTraversalAlgo.Visitor rewriteCaseRef = this::rewriteCase;
    private final LowerCaseCharSequenceObjHashMap<WithClauseModel> topLevelWithModel = new LowerCaseCharSequenceObjHashMap<>();
    private final PostOrderTreeTraversalAlgo traversalAlgo;
    private final ObjectPool<WindowColumn> windowColumnPool;
    private final ObjectPool<WithClauseModel> withClauseModelPool;
    private int digit;
    private boolean overClauseMode = false;
    private boolean subQueryMode = false;

    SqlParser(
            CairoConfiguration configuration,
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
        this.windowColumnPool = new ObjectPool<>(WindowColumn.FACTORY, configuration.getWindowColumnPoolCapacity());
        this.createTableColumnModelPool = new ObjectPool<>(CreateTableColumnModel.FACTORY, configuration.getCreateTableColumnModelPoolCapacity());
        this.renameTableModelPool = new ObjectPool<>(RenameTableModel.FACTORY, configuration.getRenameTableModelPoolCapacity());
        this.withClauseModelPool = new ObjectPool<>(WithClauseModel.FACTORY, configuration.getWithClauseModelPoolCapacity());
        this.insertModelPool = new ObjectPool<>(InsertModel.FACTORY, configuration.getInsertModelPoolCapacity());
        this.copyModelPool = new ObjectPool<>(CopyModel.FACTORY, configuration.getCopyPoolCapacity());
        this.explainModelPool = new ObjectPool<>(ExplainModel.FACTORY, configuration.getExplainPoolCapacity());
        this.configuration = configuration;
        this.traversalAlgo = traversalAlgo;
        this.characterStore = characterStore;
        boolean tempCairoSqlLegacyOperatorPrecedence = configuration.getCairoSqlLegacyOperatorPrecedence();
        if (tempCairoSqlLegacyOperatorPrecedence) {
            this.expressionParser = new ExpressionParser(
                    OperatorExpression.getLegacyRegistry(),
                    OperatorExpression.getRegistry(),
                    expressionNodePool,
                    this,
                    characterStore
            );
        } else {
            this.expressionParser = new ExpressionParser(
                    OperatorExpression.getRegistry(),
                    null,
                    expressionNodePool,
                    this,
                    characterStore
            );
        }
        this.digit = 1;
        this.column = "column";
    }

    public static boolean isFullSampleByPeriod(ExpressionNode n) {
        return n != null && (n.type == ExpressionNode.CONSTANT || (n.type == ExpressionNode.LITERAL && isValidSampleByPeriodLetter(n.token)));
    }

    /**
     * Parses a value and time unit into a TTL value. If the returned value is positive, the time unit
     * is hours. If it's negative, the time unit is months (and the actual value is positive).
     */
    public static int parseTtlHoursOrMonths(GenericLexer lexer) throws SqlException {
        CharSequence tok;
        int valuePos = lexer.getPosition();
        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            throw SqlException.$(lexer.getPosition(), "missing argument, should be <number> <unit> or <number_with_unit>");
        }
        int tokLength = tok.length();
        int unit = -1;
        int unitPos = -1;
        char unitChar = tok.charAt(tokLength - 1);
        if (tokLength > 1 && Character.isLetter(unitChar)) {
            unit = PartitionBy.ttlUnitFromString(tok, tokLength - 1, tokLength);
            if (unit != -1) {
                unitPos = valuePos;
            } else {
                try {
                    Numbers.parseLong(tok, 0, tokLength - 1);
                } catch (NumericException e) {
                    throw SqlException.$(valuePos, "invalid argument, should be <number> <unit> or <number_with_unit>");
                }
                throw SqlException.$(valuePos + tokLength - 1, "invalid time unit, expecting 'H', 'D', 'W', 'M' or 'Y', but was '")
                        .put(unitChar).put('\'');
            }
        }
        // at this point, unit == -1 means the syntax wasn't of the "1H" form, it can still be of the "1 HOUR" form
        int ttlValue;
        try {
            long ttlLong = unit == -1 ? Numbers.parseLong(tok) : Numbers.parseLong(tok, 0, tokLength - 1);
            if (ttlLong > Integer.MAX_VALUE || ttlLong < 0) {
                throw SqlException.$(valuePos, "value out of range: ").put(ttlLong)
                        .put(". Max value: ").put(Integer.MAX_VALUE);
            }
            ttlValue = (int) ttlLong;
        } catch (NumericException e) {
            throw SqlException.$(valuePos, "invalid syntax, should be <number> <unit> but was ").put(tok);
        }
        if (unit == -1) {
            unitPos = lexer.getPosition();
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null) {
                throw SqlException.$(unitPos, "missing unit, 'HOUR(S)', 'DAY(S)', 'WEEK(S)', 'MONTH(S)' or 'YEAR(S)' expected");
            }
            unit = PartitionBy.ttlUnitFromString(tok, 0, tok.length());
        }
        if (unit == -1) {
            throw SqlException.$(unitPos, "invalid unit, expected 'HOUR(S)', 'DAY(S)', 'WEEK(S)', 'MONTH(S)' or 'YEAR(S)', but was '")
                    .put(tok).put('\'');
        }
        return CommonUtils.toHoursOrMonths(ttlValue, unit, valuePos);
    }

    public static ExpressionNode recursiveReplace(ExpressionNode node, ReplacingVisitor visitor) throws SqlException {
        if (node == null) {
            return null;
        }

        switch (node.paramCount) {
            case 0:
                break;
            case 1:
                node.rhs = recursiveReplace(node.rhs, visitor);
                break;
            case 2:
                node.lhs = recursiveReplace(node.lhs, visitor);
                node.rhs = recursiveReplace(node.rhs, visitor);
                break;
            default:
                for (int i = 0; i < node.paramCount; i++) {
                    ExpressionNode arg = node.args.get(i);
                    node.args.set(i, recursiveReplace(arg, visitor));
                }
                break;
        }

        return visitor.visit(node);
    }

    public static void validateMatViewDelay(int length, char lengthUnit, int delay, char delayUnit, int pos) throws SqlException {
        if (delay < 0) {
            throw SqlException.position(pos).put("delay cannot be negative");
        }

        int lengthMinutes;
        switch (lengthUnit) {
            case 'm':
                lengthMinutes = length;
                break;
            case 'h':
                lengthMinutes = length * 60;
                break;
            case 'd':
                lengthMinutes = length * 24 * 60;
                break;
            default:
                throw SqlException.position(pos).put("unsupported length unit: ").put(length).put(lengthUnit)
                        .put(", supported units are 'm', 'h', 'd'");
        }

        int delayMinutes;
        switch (delayUnit) {
            case 'm':
                delayMinutes = delay;
                break;
            case 'h':
                delayMinutes = delay * 60;
                break;
            case 'd':
                delayMinutes = delay * 24 * 60;
                break;
            default:
                throw SqlException.position(pos).put("unsupported delay unit: ").put(delay).put(delayUnit)
                        .put(", supported units are 'm', 'h', 'd'");
        }

        if (delayMinutes >= lengthMinutes) {
            throw SqlException.position(pos).put("delay cannot be equal to or greater than length");
        }
    }

    public static void validateMatViewEveryUnit(char unit, int pos) throws SqlException {
        if (unit != 'M' && unit != 'y' && unit != 'w' && unit != 'd' && unit != 'h' && unit != 'm') {
            throw SqlException.position(pos).put("unsupported interval unit: ").put(unit)
                    .put(", supported units are 'm', 'h', 'd', 'w', 'y', 'M'");
        }
    }

    public static void validateMatViewLength(int interval, char unit, int pos) throws SqlException {
        switch (unit) {
            case 'm':
                if (interval > 24 * 60) {
                    throw SqlException.position(pos).put("maximum supported length interval is 24 hours: ").put(interval).put(unit);
                }
                break;
            case 'h':
                if (interval > 24) {
                    throw SqlException.position(pos).put("maximum supported length interval is 24 hours: ").put(interval).put(unit);
                }
                break;
            case 'd':
                if (interval > 1) {
                    throw SqlException.position(pos).put("maximum supported length interval is 24 hours: ").put(interval).put(unit);
                }
                break;
            default:
                throw SqlException.position(pos).put("unsupported length unit: ").put(interval).put(unit)
                        .put(", supported units are 'm', 'h', 'd'");
        }
    }

    private static void collectAllTableNames(
            @NotNull QueryModel model,
            @NotNull LowerCaseCharSequenceHashSet outTableNames,
            @Nullable IntList outTableNamePositions
    ) {
        QueryModel m = model;
        do {
            final ExpressionNode tableNameExpr = m.getTableNameExpr();
            if (tableNameExpr != null && tableNameExpr.type == ExpressionNode.LITERAL) {
                if (outTableNames.add(unquote(tableNameExpr.token)) && outTableNamePositions != null) {
                    outTableNamePositions.add(tableNameExpr.position);
                }
            }

            final ObjList<QueryModel> joinModels = m.getJoinModels();
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final QueryModel joinModel = joinModels.getQuick(i);
                if (joinModel == m) {
                    continue;
                }
                collectAllTableNames(joinModel, outTableNames, outTableNamePositions);
            }

            final QueryModel unionModel = m.getUnionModel();
            if (unionModel != null) {
                collectAllTableNames(unionModel, outTableNames, outTableNamePositions);
            }

            m = m.getNestedModel();
        } while (m != null);
    }

    private static SqlException err(GenericLexer lexer, @Nullable CharSequence tok, @NotNull String msg) {
        return SqlException.parserErr(lexer.lastTokenPosition(), tok, msg);
    }

    private static SqlException errUnexpected(GenericLexer lexer, CharSequence token) {
        return SqlException.unexpectedToken(lexer.lastTokenPosition(), token);
    }

    private static SqlException errUnexpected(GenericLexer lexer, CharSequence token, @NotNull CharSequence extraMessage) {
        return SqlException.unexpectedToken(lexer.lastTokenPosition(), token, extraMessage);
    }

    private static boolean isValidSampleByPeriodLetter(CharSequence token) {
        if (token.length() != 1) return false;
        switch (token.charAt(0)) {
            case 'n':
                // nanos
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

    private static CreateMatViewOperationBuilder parseCreateMatViewExt(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback,
            CharSequence tok,
            CreateMatViewOperationBuilder builder
    ) throws SqlException {
        CharSequence nextToken = (tok == null || Chars.equals(tok, ';')) ? null : tok;
        return sqlParserCallback.parseCreateMatViewExt(lexer, executionContext.getSecurityContext(), builder, nextToken);
    }

    private static CreateTableOperationBuilder parseCreateTableExt(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback,
            CharSequence tok,
            CreateTableOperationBuilder builder
    ) throws SqlException {
        CharSequence nextToken = (tok == null || Chars.equals(tok, ';')) ? null : tok;
        return sqlParserCallback.parseCreateTableExt(lexer, executionContext.getSecurityContext(), builder, nextToken);
    }

    private static void validateShowTransactions(GenericLexer lexer) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok != null && isIsolationKeyword(tok)) {
            tok = SqlUtil.fetchNext(lexer);
            if (tok != null && isLevelKeyword(tok)) {
                return;
            }
            throw SqlException.position(tok != null ? lexer.lastTokenPosition() : lexer.getPosition()).put("expected 'level'");
        }
        throw SqlException.position(tok != null ? lexer.lastTokenPosition() : lexer.getPosition()).put("expected 'isolation'");
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
        if (Chars.indexOfLastUnquoted(tok, '.') != -1) {
            throw SqlException.$(lexer.lastTokenPosition(), "'.' is not allowed here");
        }
    }

    // prevent full/right from being used as table aliases
    private void checkSupportedJoinType(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (tok != null && (isFullKeyword(tok) || isRightKeyword(tok))) {
            throw SqlException.$((lexer.lastTokenPosition()), "unsupported join type");
        }
    }

    private CharSequence createColumnAlias(
            CharSequence token,
            int type,
            LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap
    ) {
        return SqlUtil.createColumnAlias(
                characterStore,
                unquote(token),
                Chars.indexOfLastUnquoted(token, '.'),
                aliasToColumnMap,
                type != ExpressionNode.LITERAL
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

    private @NotNull CreateTableColumnModel ensureCreateTableColumnModel(CharSequence columnName, int columnNamePos) {
        CreateTableColumnModel touchUpModel = getCreateTableColumnModel(columnName);
        if (touchUpModel != null) {
            return touchUpModel;
        }
        try {
            return newCreateTableColumnModel(columnName, columnNamePos);
        } catch (SqlException e) {
            throw new AssertionError("createColumnModel should never fail here", e);
        }
    }

    private void expectBy(GenericLexer lexer) throws SqlException {
        if (isByKeyword(tok(lexer, "'by'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'by' expected");
    }

    private ExpressionNode expectExpr(GenericLexer lexer, SqlParserCallback sqlParserCallback, LowerCaseCharSequenceObjHashMap<ExpressionNode> decls) throws SqlException {
        final ExpressionNode n = expr(lexer, null, sqlParserCallback, decls);
        if (n != null) {
            return n;
        }
        throw SqlException.$(lexer.hasUnparsed() ? lexer.lastTokenPosition() : lexer.getPosition(), "Expression expected");
    }

    private ExpressionNode expectExpr(GenericLexer lexer, SqlParserCallback sqlParserCallback) throws SqlException {
        return expectExpr(lexer, sqlParserCallback, null);
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
        return expectLiteral(lexer, null);
    }

    private ExpressionNode expectLiteral(GenericLexer lexer, @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls) throws SqlException {
        CharSequence tok = tok(lexer, "literal");
        int pos = lexer.lastTokenPosition();
        assertNameIsQuotedOrNotAKeyword(tok, pos);
        validateLiteral(pos, tok);
        return rewriteDeclaredVariables(nextLiteral(GenericLexer.immutableOf(GenericLexer.unquote(tok)), pos), decls, null);
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

    private void expectSample(GenericLexer lexer, QueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        final ExpressionNode n = expr(lexer, null, sqlParserCallback, model.getDecls());
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

    private void generateColumnAlias(GenericLexer lexer, QueryColumn qc, boolean hasFrom) throws SqlException {
        CharSequence token = qc.getAst().token;
        if (qc.getAst().isWildcard() && !hasFrom) {
            throw err(lexer, null, "'from' expected");
        }

        CharSequence alias;
        if (configuration.isColumnAliasExpressionEnabled()) {
            CharacterStoreEntry entry = characterStore.newEntry();
            qc.getAst().toSink(entry);
            alias = SqlUtil.createExprColumnAlias(
                    characterStore,
                    entry.toImmutable(),
                    aliasMap,
                    configuration.getColumnAliasGeneratedMaxSize(),
                    qc.getAst().type != ExpressionNode.LITERAL
            );
        } else {
            if (qc.getAst().type == ExpressionNode.CONSTANT && Chars.indexOfLastUnquoted(token, '.') != -1) {
                alias = createConstColumnAlias(aliasMap);
            } else {
                CharSequence tokenAlias = qc.getAst().token;
                if (qc.isWindowColumn() && ((WindowColumn) qc).isIgnoreNulls()) {
                    tokenAlias += "_ignore_nulls";
                }
                alias = createColumnAlias(tokenAlias, qc.getAst().type, aliasMap);
            }
        }
        qc.setAlias(alias, QueryColumn.SYNTHESIZED_ALIAS_POSITION);
        aliasMap.put(alias, qc);
    }

    private @Nullable CreateTableColumnModel getCreateTableColumnModel(CharSequence columnName) {
        return createTableOperationBuilder.getColumnModel(columnName);
    }

    private boolean isCurrentRow(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (isCurrentKeyword(tok)) {
            tok = tok(lexer, "'row'");
            if (isRowKeyword(tok)) {
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
        if (isUnboundedKeyword(tok)) {
            tok = tok(lexer, "'preceding'");
            if (isPrecedingKeyword(tok)) {
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
        return expressionNodePool.next().of(ExpressionNode.LITERAL, unquote(name), 0, position);
    }

    private @NotNull CreateTableColumnModel newCreateTableColumnModel(
            CharSequence columnName,
            int columnNamePos
    ) throws SqlException {
        if (createTableOperationBuilder.getColumnModel(columnName) != null) {
            throw SqlException.duplicateColumn(columnNamePos, columnName);
        }
        CreateTableColumnModel model = createTableColumnModelPool.next();
        model.setColumnNamePos(columnNamePos);
        createTableOperationBuilder.addColumnModel(columnName, model);
        return model;
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
        if (tok == null || (subQueryMode && Chars.equals(tok, ')') && !overClauseMode)) {
            return null;
        }
        return tok;
    }

    private QueryModel parseAsSubQueryAndExpectClosingBrace(
            GenericLexer lexer,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
            boolean useTopLevelWithClauses,
            SqlParserCallback sqlParserCallback,
            LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        final QueryModel model = parseAsSubQuery(lexer, withClauses, useTopLevelWithClauses, sqlParserCallback, decls);
        expectTok(lexer, ')');
        return model;
    }

    private ExecutionModel parseCopy(GenericLexer lexer, SqlParserCallback sqlParserCallback) throws SqlException {
        if (Chars.isBlank(configuration.getSqlCopyInputRoot())) {
            throw SqlException.$(lexer.lastTokenPosition(), "COPY is disabled ['cairo.sql.copy.root' is not set?]");
        }
        ExpressionNode target = expectExpr(lexer, sqlParserCallback);
        CharSequence tok = tok(lexer, "'from' or 'to' or 'cancel'");

        if (isCancelKeyword(tok)) {
            CopyModel model = copyModelPool.next();
            model.setCancel(true);
            model.setTarget(target);

            tok = optTok(lexer);
            // no more tokens or ';' should indicate end of statement
            if (tok == null || Chars.equals(tok, ';')) {
                return model;
            }
            throw errUnexpected(lexer, tok);
        }

        if (isFromKeyword(tok)) {
            final ExpressionNode fileName = expectExpr(lexer, sqlParserCallback);
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
                        tok = tok(lexer, "year month day hour none");
                        int partitionBy = PartitionBy.fromString(tok);
                        if (partitionBy == -1) {
                            throw SqlException.$(lexer.getPosition(), "'NONE', 'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
                        }
                        model.setPartitionBy(partitionBy);
                        tok = optTok(lexer);
                    } else if (isTimestampKeyword(tok)) {
                        tok = tok(lexer, "timestamp column name expected");
                        CharSequence columnName = GenericLexer.immutableOf(unquote(tok));
                        if (!TableUtils.isValidColumnName(columnName, configuration.getMaxFileNameLength())) {
                            throw SqlException.$(lexer.getPosition(), "timestamp column name contains invalid characters");
                        }
                        model.setTimestampColumnName(columnName);
                        tok = optTok(lexer);
                    } else if (isFormatKeyword(tok)) {
                        tok = tok(lexer, "timestamp format expected");
                        CharSequence format = GenericLexer.immutableOf(unquote(tok));
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
                        CharSequence delimiter = GenericLexer.immutableOf(unquote(tok));
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
            } else if (tok != null && !isSemicolon(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'with' expected");
            }
            return model;
        }
        throw SqlException.$(lexer.lastTokenPosition(), "'from' expected");
    }

    private ExecutionModel parseCreate(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final CharSequence tok = tok(lexer, "'atomic' or 'table' or 'batch' or 'materialized'");
        if (isMaterializedKeyword(tok)) {
            if (!configuration.isMatViewEnabled()) {
                throw SqlException.$(0, "materialized views are disabled");
            }
            return parseCreateMatView(lexer, executionContext, sqlParserCallback);
        }
        return parseCreateTable(lexer, tok, executionContext, sqlParserCallback);
    }

    private ExecutionModel parseCreateMatView(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final CreateMatViewOperationBuilderImpl mvOpBuilder = createMatViewOperationBuilder;
        final CreateTableOperationBuilderImpl tableOpBuilder = mvOpBuilder.getCreateTableOperationBuilder();
        mvOpBuilder.clear(); // clears tableOpBuilder too
        tableOpBuilder.setDefaultSymbolCapacity(configuration.getDefaultSymbolCapacity());
        tableOpBuilder.setMaxUncommittedRows(configuration.getMaxUncommittedRows());
        tableOpBuilder.setWalEnabled(true); // mat view is always WAL-enabled

        expectTok(lexer, "view");
        CharSequence tok = tok(lexer, "view name or 'if'");
        if (isIfKeyword(tok)) {
            if (isNotKeyword(tok(lexer, "'not'")) && isExistsKeyword(tok(lexer, "'exists'"))) {
                tableOpBuilder.setIgnoreIfExists(true);
                tok = tok(lexer, "view name");
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'if not exists' expected");
            }
        }
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        tableOpBuilder.setTableNameExpr(nextLiteral(
                assertNoDotsAndSlashes(unquote(tok), lexer.lastTokenPosition()), lexer.lastTokenPosition()
        ));

        tok = tok(lexer, "'as' or 'with' or 'refresh'");
        CharSequence baseTableName = null;
        int baseTableNamePos = 0;
        if (isWithKeyword(tok)) {
            expectTok(lexer, "base");
            tok = tok(lexer, "base table");
            baseTableName = sansPublicSchema(tok, lexer);
            assertNameIsQuotedOrNotAKeyword(baseTableName, lexer.lastTokenPosition());
            baseTableName = unquote(baseTableName);
            baseTableNamePos = lexer.lastTokenPosition();
            tok = tok(lexer, "'as' or 'refresh'");
        }

        boolean refreshDefined = false;
        int refreshType = MatViewDefinition.REFRESH_TYPE_IMMEDIATE;
        boolean deferred = false;
        if (isRefreshKeyword(tok)) {
            refreshDefined = true;
            tok = tok(lexer, "'immediate' or 'manual' or 'period' or 'every' or 'as'");
            int every = 0;
            char everyUnit = 0;
            // 'incremental' is obsolete, replaced with 'immediate'
            if (isIncrementalKeyword(tok)) {
                tok = tok(lexer, "'as'");
            } else if (isImmediateKeyword(tok)) {
                tok = tok(lexer, "'deferred' or 'period' or 'as'");
            } else if (isManualKeyword(tok)) {
                refreshType = MatViewDefinition.REFRESH_TYPE_MANUAL;
                tok = tok(lexer, "'deferred' or 'period' or 'as'");
            } else if (isEveryKeyword(tok)) {
                tok = tok(lexer, "interval");
                every = CommonUtils.getStrideMultiple(tok, lexer.lastTokenPosition());
                everyUnit = CommonUtils.getStrideUnit(tok, lexer.lastTokenPosition());
                validateMatViewEveryUnit(everyUnit, lexer.lastTokenPosition());
                refreshType = MatViewDefinition.REFRESH_TYPE_TIMER;
                tok = tok(lexer, "'deferred' or 'start' or 'period' or 'as'");
            }

            if (isDeferredKeyword(tok)) {
                deferred = true;
                if (refreshType == MatViewDefinition.REFRESH_TYPE_TIMER) {
                    tok = tok(lexer, "'start' or 'period' or 'as'");
                } else {
                    tok = tok(lexer, "'period' or 'as'");
                }
            }

            // Timer uses microsecond precision for start time calculation
            if (isPeriodKeyword(tok)) {
                // REFRESH ... PERIOD(LENGTH <interval> [TIME ZONE '<timezone>'] [DELAY <interval>])
                expectTok(lexer, "(");
                expectTok(lexer, "length");
                tok = tok(lexer, "LENGTH interval");
                final int length = CommonUtils.getStrideMultiple(tok, lexer.lastTokenPosition());
                final char lengthUnit = CommonUtils.getStrideUnit(tok, lexer.lastTokenPosition());
                validateMatViewLength(length, lengthUnit, lexer.lastTokenPosition());
                final TimestampSampler periodSamplerMicros = TimestampSamplerFactory.getInstance(
                        MicrosTimestampDriver.INSTANCE,
                        length,
                        lengthUnit,
                        lexer.lastTokenPosition()
                );
                tok = tok(lexer, "'time zone' or 'delay' or ')'");

                TimeZoneRules tzRulesMicros = null;
                String tz = null;
                if (isTimeKeyword(tok)) {
                    expectTok(lexer, "zone");
                    tok = tok(lexer, "TIME ZONE name");
                    if (Chars.equals(tok, ')') || isDelayKeyword(tok)) {
                        throw SqlException.position(lexer.lastTokenPosition()).put("TIME ZONE name expected");
                    }
                    tz = unquote(tok).toString();
                    try {
                        tzRulesMicros = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, tz);
                    } catch (CairoException e) {
                        throw SqlException.position(lexer.lastTokenPosition()).put(e.getFlyweightMessage());
                    }
                    tok = tok(lexer, "'delay' or ')'");
                }

                int delay = 0;
                char delayUnit = 0;
                if (isDelayKeyword(tok)) {
                    tok = tok(lexer, "DELAY interval");
                    delay = CommonUtils.getStrideMultiple(tok, lexer.lastTokenPosition());
                    delayUnit = CommonUtils.getStrideUnit(tok, lexer.lastTokenPosition());
                    validateMatViewDelay(length, lengthUnit, delay, delayUnit, lexer.lastTokenPosition());
                    tok = tok(lexer, "')'");
                }

                if (!Chars.equals(tok, ')')) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("')' expected");
                }

                // Period timer start is at the boundary of the current period.
                final long nowMicros = configuration.getMicrosecondClock().getTicks();
                final long nowLocalMicros = tzRulesMicros != null ? nowMicros + tzRulesMicros.getOffset(nowMicros) : nowMicros;
                final long startUs = periodSamplerMicros.round(nowLocalMicros);

                mvOpBuilder.setTimer(tz, startUs, every, everyUnit);
                mvOpBuilder.setPeriodLength(length, lengthUnit, delay, delayUnit);
                tok = tok(lexer, "'as'");
            } else if (!isAsKeyword(tok)) {
                // REFRESH EVERY <interval> [START '<datetime>' [TIME ZONE '<timezone>']]
                if (refreshType != MatViewDefinition.REFRESH_TYPE_TIMER) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'as' expected");
                }
                // Use the current time as the start timestamp if it wasn't specified.
                long startUs = configuration.getMicrosecondClock().getTicks();
                String tz = null;
                if (isStartKeyword(tok)) {
                    tok = tok(lexer, "START timestamp");
                    try {
                        startUs = MicrosTimestampDriver.INSTANCE.parseFloorLiteral(GenericLexer.unquote(tok));
                    } catch (NumericException e) {
                        throw SqlException.$(lexer.lastTokenPosition(), "invalid START timestamp value");
                    }
                    tok = tok(lexer, "'time zone' or 'as'");

                    if (isTimeKeyword(tok)) {
                        expectTok(lexer, "zone");
                        tok = tok(lexer, "TIME ZONE name");
                        tz = unquote(tok).toString();
                        tok = tok(lexer, "'as'");
                    }
                }
                mvOpBuilder.setTimer(tz, startUs, every, everyUnit);
            } else if (refreshType == MatViewDefinition.REFRESH_TYPE_TIMER) {
                // REFRESH EVERY <interval> AS
                // Don't forget to set timer params.
                final long startUs = configuration.getMicrosecondClock().getTicks();
                mvOpBuilder.setTimer(null, startUs, every, everyUnit);
            }
        }
        mvOpBuilder.setRefreshType(refreshType);
        mvOpBuilder.setDeferred(deferred);

        boolean enclosedInParentheses;
        if (isAsKeyword(tok)) {
            int startOfQuery = lexer.getPosition();
            tok = tok(lexer, "'(' or 'with' or 'select'");
            enclosedInParentheses = Chars.equals(tok, '(');
            if (enclosedInParentheses) {
                startOfQuery = lexer.getPosition();
                tok = tok(lexer, "'with' or 'select'");
            }

            // Parse SELECT for the sake of basic SQL validation.
            // It'll be compiled and optimized later, at the execution phase.
            if (isWithKeyword(tok)) {
                parseWithClauses(lexer, topLevelWithModel, sqlParserCallback, null);
                // CTEs require SELECT to be specified
                expectTok(lexer, "select");
            }
            lexer.unparseLast();
            final QueryModel queryModel = parseDml(lexer, null, lexer.getPosition(), true, sqlParserCallback, null);
            final int endOfQuery = enclosedInParentheses ? lexer.getPosition() - 1 : lexer.getPosition();

            tableNames.clear();
            tableNamePositions.clear();
            collectAllTableNames(queryModel, tableNames, tableNamePositions);

            // Find base table name if not set explicitly.
            if (baseTableName == null) {
                if (tableNames.size() < 1) {
                    throw SqlException.$(startOfQuery, "missing base table, materialized views have to be based on a table");
                }
                if (tableNames.size() > 1) {
                    throw SqlException.$(startOfQuery, "more than one table used in query, base table has to be set using 'WITH BASE'");
                }
                baseTableName = Chars.toString(tableNames.getAny());
                baseTableNamePos = tableNamePositions.getQuick(0);
            }

            mvOpBuilder.setBaseTableNamePosition(baseTableNamePos);
            final String baseTableNameStr = Chars.toString(baseTableName);
            mvOpBuilder.setBaseTableName(baseTableNameStr);

            // Basic validation - check all nested models that read from the base table for window functions, unions, FROM-TO, or FILL.
            if (!tableNames.contains(baseTableNameStr)) {
                throw SqlException.position(queryModel.getModelPosition())
                        .put("base table is not referenced in materialized view query: ").put(baseTableName);
            }
            validateMatViewQuery(queryModel, baseTableNameStr);

            final QueryModel nestedModel = queryModel.getNestedModel();
            if (nestedModel != null) {
                if (nestedModel.getSampleByTimezoneName() != null) {
                    mvOpBuilder.setTimeZone(unquote(nestedModel.getSampleByTimezoneName().token).toString());
                }
                if (nestedModel.getSampleByOffset() != null) {
                    mvOpBuilder.setTimeZoneOffset(unquote(nestedModel.getSampleByOffset().token).toString());
                }
            }

            final String matViewSql = Chars.toString(lexer.getContent(), startOfQuery, endOfQuery);
            tableOpBuilder.setSelectText(matViewSql, startOfQuery);
            tableOpBuilder.setSelectModel(queryModel); // transient model, for toSink() purposes only

            if (enclosedInParentheses) {
                expectTok(lexer, ')');
            } else {
                // We expect nothing more when there are no parentheses.
                tok = optTok(lexer);
                if (tok != null && !Chars.equals(tok, ';')) {
                    throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
                }
                return mvOpBuilder;
            }
        } else {
            if (refreshDefined) {
                throw SqlException.position(lexer.lastTokenPosition()).put("'as' expected");
            }
            throw SqlException.position(lexer.lastTokenPosition()).put("'refresh' or 'as' expected");
        }

        // Optional clauses that go after the parentheses.

        while ((tok = optTok(lexer)) != null && Chars.equals(tok, ',')) {
            tok = tok(lexer, "'index'");
            if (isIndexKeyword(tok)) {
                parseCreateTableIndexDef(lexer, false);
            } else {
                throw errUnexpected(lexer, tok);
            }
        }

        final ExpressionNode timestamp = parseTimestamp(lexer, tok);
        if (timestamp != null) {
            tableOpBuilder.setTimestampExpr(timestamp);
            tok = optTok(lexer);
        }

        final ExpressionNode partitionByExpr = parseCreateTablePartition(lexer, tok);
        int partitionBy = -1;
        if (partitionByExpr != null) {
            partitionBy = PartitionBy.fromString(partitionByExpr.token);
            if (partitionBy == -1) {
                throw SqlException.$(partitionByExpr.position, "'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
            }
            if (!PartitionBy.isPartitioned(partitionBy)) {
                throw SqlException.position(partitionByExpr.position).put("materialized view has to be partitioned");
            }
            tableOpBuilder.setPartitionByExpr(partitionByExpr);
            tok = optTok(lexer);
        }

        if (tok != null && isTtlKeyword(tok)) {
            final int ttlValuePos = lexer.getPosition();
            final int ttlHoursOrMonths = parseTtlHoursOrMonths(lexer);
            if (partitionBy != -1) {
                PartitionBy.validateTtlGranularity(partitionBy, ttlHoursOrMonths, ttlValuePos);
            }
            tableOpBuilder.setTtlHoursOrMonths(ttlHoursOrMonths);
            tableOpBuilder.setTtlPosition(ttlValuePos);
            tok = optTok(lexer);
        }

        if (tok != null && isInKeyword(tok)) {
            parseInVolume(lexer, tableOpBuilder);
            tok = optTok(lexer);
        }

        return parseCreateMatViewExt(lexer, executionContext, sqlParserCallback, tok, mvOpBuilder);
    }

    private ExecutionModel parseCreateTable(
            GenericLexer lexer,
            CharSequence tok,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        CreateTableOperationBuilderImpl builder = createTableOperationBuilder;
        builder.clear();
        builder.setDefaultSymbolCapacity(configuration.getDefaultSymbolCapacity());
        CharSequence tableName;
        // default to non-atomic, batched, creation
        builder.setBatchSize(configuration.getInsertModelBatchSize());
        boolean atomicSpecified = false;
        boolean batchSpecified = false;
        boolean isDirectCreate = true;

        // if it's a CREATE ATOMIC, we don't accept BATCH
        if (isAtomicKeyword(tok)) {
            atomicSpecified = true;
            builder.setBatchSize(-1);
            expectTok(lexer, "table");
            tok = tok(lexer, "table name or 'if'");
        } else if (isBatchKeyword(tok)) {
            batchSpecified = true;

            long val = expectLong(lexer);
            if (val > 0) {
                builder.setBatchSize(val);
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "batch size must be positive integer");
            }

            tok = tok(lexer, "table or o3MaxLag");
            if (isO3MaxLagKeyword(tok)) {
                int pos = lexer.getPosition();
                builder.setBatchO3MaxLag(SqlUtil.expectMicros(tok(lexer, "lag value"), pos));
                expectTok(lexer, "table");
            }
            tok = tok(lexer, "table name or 'if'");
        } else if (isTableKeyword(tok)) {
            tok = tok(lexer, "table name or 'if'");
        } else {
            throw SqlException.$(lexer.lastTokenPosition(), "'atomic' or 'table' or 'batch' expected");
        }

        if (isIfKeyword(tok)) {
            if (isNotKeyword(tok(lexer, "'not'")) && isExistsKeyword(tok(lexer, "'exists'"))) {
                builder.setIgnoreIfExists(true);
                tableName = tok(lexer, "table name");
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "'if not exists' expected");
            }
        } else {
            tableName = tok;
        }
        tableName = sansPublicSchema(tableName, lexer);
        assertNameIsQuotedOrNotAKeyword(tableName, lexer.lastTokenPosition());

        builder.setTableNameExpr(nextLiteral(
                assertNoDotsAndSlashes(unquote(tableName), lexer.lastTokenPosition()), lexer.lastTokenPosition()
        ));

        tok = tok(lexer, "'(' or 'as'");

        if (Chars.equals(tok, '(')) {
            tok = tok(lexer, "like");
            if (isLikeKeyword(tok)) {
                builder.setBatchSize(-1);
                parseCreateTableLikeTable(lexer);
                tok = optTok(lexer);
                return parseCreateTableExt(lexer, executionContext, sqlParserCallback, tok, builder);
            } else {
                lexer.unparseLast();
                parseCreateTableColumns(lexer);
            }
        } else if (isAsKeyword(tok)) {
            isDirectCreate = false;
            parseCreateTableAsSelect(lexer, sqlParserCallback);
        } else {
            throw errUnexpected(lexer, tok);
        }

        // if not CREATE ... AS SELECT, make it atomic
        if (isDirectCreate) {
            builder.setBatchSize(-1);
            builder.setBatchO3MaxLag(-1);

            // if we use atomic or batch keywords, then throw an error
            if (atomicSpecified || batchSpecified) {
                throw SqlException.$(
                        lexer.lastTokenPosition(),
                        "'atomic' or 'batch' keywords can only be used in CREATE ... AS SELECT statements."
                );
            }
        }

        while ((tok = optTok(lexer)) != null && Chars.equals(tok, ',')) {
            tok = tok(lexer, "'index' or 'cast'");
            if (isIndexKeyword(tok)) {
                parseCreateTableIndexDef(lexer, isDirectCreate);
            } else if (isCastKeyword(tok)) {
                parseCreateTableCastDef(lexer);
            } else {
                throw errUnexpected(lexer, tok);
            }
        }

        ExpressionNode timestamp = parseTimestamp(lexer, tok);
        if (timestamp != null) {
            if (isDirectCreate) {
                CreateTableColumnModel model = builder.getColumnModel(timestamp.token);
                if (model == null) {
                    throw SqlException.position(timestamp.position)
                            .put("invalid designated timestamp column [name=").put(timestamp.token).put(']');
                }
                if (!ColumnType.isTimestamp(model.getColumnType())) {
                    throw SqlException
                            .position(timestamp.position)
                            .put("TIMESTAMP column expected [actual=").put(ColumnType.nameOf(model.getColumnType()))
                            .put(", columnName=").put(timestamp.token)
                            .put(']');
                }
            }
            builder.setTimestampExpr(timestamp);
            tok = optTok(lexer);
        }

        int walSetting = WAL_NOT_SET;

        final ExpressionNode partitionByExpr = parseCreateTablePartition(lexer, tok);
        if (partitionByExpr != null) {
            if (builder.getTimestampExpr() == null) {
                throw SqlException.$(partitionByExpr.position, "partitioning is possible only on tables with designated timestamps");
            }
            final int partitionBy = PartitionBy.fromString(partitionByExpr.token);
            if (partitionBy == -1) {
                throw SqlException.$(partitionByExpr.position, "'NONE', 'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
            }
            builder.setPartitionByExpr(partitionByExpr);
            tok = optTok(lexer);

            if (tok != null && isTtlKeyword(tok)) {
                final int ttlValuePos = lexer.getPosition();
                final int ttlHoursOrMonths = parseTtlHoursOrMonths(lexer);
                PartitionBy.validateTtlGranularity(partitionBy, ttlHoursOrMonths, ttlValuePos);
                builder.setTtlHoursOrMonths(ttlHoursOrMonths);
                tok = optTok(lexer);
            }

            if (tok != null) {
                if (isWalKeyword(tok)) {
                    if (!PartitionBy.isPartitioned(builder.getPartitionByFromExpr())) {
                        throw SqlException.position(lexer.lastTokenPosition())
                                .put("WAL Write Mode can only be used on partitioned tables");
                    }
                    walSetting = WAL_ENABLED;
                    tok = optTok(lexer);
                } else if (isBypassKeyword(tok)) {
                    tok = optTok(lexer);
                    if (tok != null && isWalKeyword(tok)) {
                        walSetting = WAL_DISABLED;
                        tok = optTok(lexer);
                    } else {
                        throw SqlException.position(tok == null ? lexer.getPosition() : lexer.lastTokenPosition())
                                .put(" invalid syntax, should be BYPASS WAL but was BYPASS ")
                                .put(tok != null ? tok : "");
                    }
                }
            }
        }
        final boolean isWalEnabled = configuration.isWalSupported()
                && PartitionBy.isPartitioned(builder.getPartitionByFromExpr())
                && ((walSetting == WAL_NOT_SET && configuration.getWalEnabledDefault()) || walSetting == WAL_ENABLED);
        builder.setWalEnabled(isWalEnabled);

        int maxUncommittedRows = configuration.getMaxUncommittedRows();
        long o3MaxLag = configuration.getO3MaxLag();

        if (tok != null && isWithKeyword(tok)) {
            ExpressionNode expr;
            while ((expr = expr(lexer, (QueryModel) null, sqlParserCallback)) != null) {
                if (Chars.equals(expr.token, '=')) {
                    if (isMaxUncommittedRowsKeyword(expr.lhs.token)) {
                        try {
                            maxUncommittedRows = Numbers.parseInt(expr.rhs.token);
                        } catch (NumericException e) {
                            throw SqlException.position(lexer.getPosition())
                                    .put(" could not parse maxUncommittedRows value \"").put(expr.rhs.token).put('"');
                        }
                    } else if (isO3MaxLagKeyword(expr.lhs.token)) {
                        o3MaxLag = SqlUtil.expectMicros(expr.rhs.token, lexer.getPosition());
                    } else {
                        throw SqlException.position(lexer.getPosition()).put(" unrecognized ")
                                .put(expr.lhs.token).put(" after WITH");
                    }
                    tok = optTok(lexer);
                    if (tok != null && Chars.equals(tok, ',')) {
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
        builder.setMaxUncommittedRows(maxUncommittedRows);
        builder.setO3MaxLag(o3MaxLag);

        if (tok != null && isInKeyword(tok)) {
            parseInVolume(lexer, builder);
            tok = optTok(lexer);
        }

        if (tok != null && (isDedupKeyword(tok) || isDeduplicateKeyword(tok))) {
            if (!builder.isWalEnabled()) {
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
                    final CharSequence columnName = unquote(tok);
                    CreateTableColumnModel model = getCreateTableColumnModel(columnName);
                    if (model == null) {
                        if (isDirectCreate) {
                            throw SqlException.position(lexer.lastTokenPosition())
                                    .put("deduplicate key column not found [column=").put(columnName).put(']');
                        }
                        model = newCreateTableColumnModel(columnName, lexer.lastTokenPosition());
                    } else if (model.isDedupKey() && isDirectCreate) {
                        throw SqlException.position(lexer.lastTokenPosition())
                                .put("duplicate dedup column [column=").put(columnName).put(']');
                    } else if (ColumnType.isArray(model.getColumnType())) {
                        throw SqlException.position(lexer.lastTokenPosition())
                                .put("dedup key columns cannot include ARRAY [column=")
                                .put(columnName).put(", type=")
                                .put(ColumnType.nameOf(model.getColumnType())).put(']');
                    }
                    model.setIsDedupKey();
                    int colIndex = builder.getColumnIndex(columnName);
                    if (colIndex == builder.getTimestampIndex()) {
                        timestampColumnFound = true;
                    }

                    tok = optTok(lexer);
                    if (tok != null && Chars.equals(tok, ',')) {
                        tok = optTok(lexer);
                    }
                }

                if (!timestampColumnFound && isDirectCreate) {
                    throw SqlException.position(columnListPos).put("deduplicate key list must include dedicated timestamp column");
                }

                tok = optTok(lexer);
            } else {
                throw SqlException.position(lexer.getPosition()).put("column list expected");
            }
        }
        return parseCreateTableExt(lexer, executionContext, sqlParserCallback, tok, builder);
    }

    private void parseCreateTableAsSelect(GenericLexer lexer, SqlParserCallback sqlParserCallback) throws SqlException {
        expectTok(lexer, '(');
        final int startOfSelect = lexer.getPosition();
        // Parse SELECT for the sake of basic SQL validation.
        // It'll be compiled and optimized later, at the execution phase.
        final QueryModel selectModel = parseDml(lexer, null, startOfSelect, true, sqlParserCallback, null);
        final int endOfSelect = lexer.getPosition() - 1;
        final String selectText = Chars.toString(lexer.getContent(), startOfSelect, endOfSelect);
        createTableOperationBuilder.setSelectText(selectText, startOfSelect);
        createTableOperationBuilder.setSelectModel(selectModel); // transient model, for toSink() purposes only
        expectTok(lexer, ')');
    }

    private void parseCreateTableCastDef(GenericLexer lexer) throws SqlException {
        if (createTableOperationBuilder.getSelectText() == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "cast is only supported in 'create table as ...' context");
        }
        expectTok(lexer, '(');
        final ExpressionNode columnName = expectLiteral(lexer);

        CreateTableColumnModel model = ensureCreateTableColumnModel(columnName.token, columnName.position);
        if (model.getColumnType() != ColumnType.UNDEFINED) {
            throw SqlException.$(lexer.lastTokenPosition(), "duplicate cast");
        }
        expectTok(lexer, "as");

        final ExpressionNode columnType = expectLiteral(lexer);
        final int type = toColumnType(lexer, columnType.token);
        model.setCastType(type, columnType.position);

        if (ColumnType.isSymbol(type)) {
            CharSequence tok = tok(lexer, "'capacity', 'nocache', 'cache' or ')'");

            int symbolCapacity;
            int capacityPosition;
            if (isCapacityKeyword(tok)) {
                capacityPosition = lexer.getPosition();
                symbolCapacity = parseSymbolCapacity(lexer);
                tok = tok(lexer, "'nocache', 'cache' or ')'");
            } else {
                capacityPosition = 0;
                symbolCapacity = configuration.getDefaultSymbolCapacity();
            }
            model.setSymbolCapacity(symbolCapacity);

            final boolean isCached;
            if (isNoCacheKeyword(tok)) {
                isCached = false;
            } else if (isCacheKeyword(tok)) {
                isCached = true;
            } else {
                isCached = configuration.getDefaultSymbolCacheFlag();
                lexer.unparseLast();
            }
            model.setSymbolCacheFlag(isCached);

            if (isCached) {
                TableUtils.validateSymbolCapacityCached(true, symbolCapacity, capacityPosition);
            }
        }
        expectTok(lexer, ')');
    }

    private void parseCreateTableColumns(GenericLexer lexer) throws SqlException {
        while (true) {
            CharSequence tok = notTermTok(lexer);
            assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            final CharSequence columnName = GenericLexer.immutableOf(unquote(tok));
            final int columnPosition = lexer.lastTokenPosition();
            final int columnType = toColumnType(lexer, notTermTok(lexer));

            if (!TableUtils.isValidColumnName(columnName, configuration.getMaxFileNameLength())) {
                throw SqlException.$(columnPosition, " new column name contains invalid characters");
            }

            CreateTableColumnModel model = newCreateTableColumnModel(columnName, columnPosition);
            model.setColumnType(columnType);
            model.setSymbolCapacity(configuration.getDefaultSymbolCapacity());

            if (ColumnType.isSymbol(columnType)) {
                tok = tok(lexer, "'capacity', 'nocache', 'cache', 'index' or ')'");

                int symbolCapacity;
                if (isCapacityKeyword(tok)) {
                    // when capacity is not set explicitly, it will default via configuration
                    model.setSymbolCapacity(symbolCapacity = parseSymbolCapacity(lexer));
                    tok = tok(lexer, "'nocache', 'cache', 'index' or ')'");
                } else {
                    symbolCapacity = -1;
                }

                final boolean cacheFlag;
                if (isNoCacheKeyword(tok)) {
                    cacheFlag = false;
                } else if (isCacheKeyword(tok)) {
                    cacheFlag = true;
                } else {
                    cacheFlag = configuration.getDefaultSymbolCacheFlag();
                    lexer.unparseLast();
                }
                model.setSymbolCacheFlag(cacheFlag);
                if (cacheFlag && symbolCapacity != -1) {
                    TableUtils.validateSymbolCapacityCached(true, symbolCapacity, lexer.lastTokenPosition());
                }
                tok = parseCreateTableInlineIndexDef(lexer, model);
            } else {
                tok = null;
            }

            // check for dodgy array syntax
            CharSequence tempTok = optTok(lexer);
            if (tempTok != null && Chars.equals(tempTok, ']')) {
                throw SqlException.position(columnPosition).put(columnName).put(" has an unmatched `]` - were you trying to define an array?");
            } else {
                lexer.unparseLast();
            }

            if (tok == null) {
                tok = tok(lexer, "',' or ')'");
            }

            // ignore `PRECISION`
            if (isPrecisionKeyword(tok)) {
                tok = tok(lexer, "'NOT' or 'NULL' or ',' or ')'");
            }

            // ignore `NULL` and `NOT NULL`
            if (isNotKeyword(tok)) {
                tok = tok(lexer, "'NULL'");
            }

            if (isNullKeyword(tok)) {
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

    private void parseCreateTableIndexDef(GenericLexer lexer, boolean isDirectCreate) throws SqlException {
        expectTok(lexer, '(');
        final ExpressionNode columnName = expectLiteral(lexer);
        final int columnNamePosition = lexer.lastTokenPosition();

        CreateTableColumnModel model = getCreateTableColumnModel(columnName.token);
        if (model == null) {
            if (isDirectCreate) {
                throw SqlException.invalidColumn(columnNamePosition, columnName.token);
            }
            model = newCreateTableColumnModel(columnName.token, columnName.position);
        } else if (model.isIndexed()) {
            throw SqlException.$(columnNamePosition, "duplicate index clause");
        }
        if (isDirectCreate && model.getColumnType() != ColumnType.SYMBOL) {
            throw SqlException
                    .position(columnNamePosition)
                    .put("indexes are supported only for SYMBOL columns [columnName=").put(columnName.token)
                    .put(", columnType=").put(ColumnType.nameOf(model.getColumnType()))
                    .put(']');
        }

        int indexValueBlockSize;
        if (isCapacityKeyword(tok(lexer, "'capacity'"))) {
            int errorPosition = lexer.getPosition();
            indexValueBlockSize = expectInt(lexer);
            TableUtils.validateIndexValueBlockSize(errorPosition, indexValueBlockSize);
            indexValueBlockSize = Numbers.ceilPow2(indexValueBlockSize);
        } else {
            indexValueBlockSize = configuration.getIndexValueBlockSize();
            lexer.unparseLast();
        }
        model.setIndexed(true, columnNamePosition, indexValueBlockSize);
        expectTok(lexer, ')');
    }

    private CharSequence parseCreateTableInlineIndexDef(GenericLexer lexer, CreateTableColumnModel model) throws SqlException {
        CharSequence tok = tok(lexer, "')', or 'index'");

        if (isFieldTerm(tok)) {
            model.setIndexed(false, -1, configuration.getIndexValueBlockSize());
            return tok;
        }

        expectTok(lexer, tok, "index");
        int indexColumnPosition = lexer.lastTokenPosition();

        if (isFieldTerm(tok = tok(lexer, ") | , expected"))) {
            model.setIndexed(true, indexColumnPosition, configuration.getIndexValueBlockSize());
            return tok;
        }

        expectTok(lexer, tok, "capacity");

        int errorPosition = lexer.getPosition();
        int indexValueBlockSize = expectInt(lexer);
        TableUtils.validateIndexValueBlockSize(errorPosition, indexValueBlockSize);
        model.setIndexed(true, indexColumnPosition, Numbers.ceilPow2(indexValueBlockSize));
        return null;
    }

    private void parseCreateTableLikeTable(GenericLexer lexer) throws SqlException {
        // todo: validate keyword usage
        CharSequence tok = tok(lexer, "table name");
        tok = sansPublicSchema(tok, lexer);
        createTableOperationBuilder.setLikeTableNameExpr(
                nextLiteral(
                        assertNoDotsAndSlashes(
                                unquote(tok),
                                lexer.lastTokenPosition()
                        ),
                        lexer.lastTokenPosition()
                )
        );
        tok = tok(lexer, ")");
        if (!Chars.equals(tok, ')')) {
            throw errUnexpected(lexer, tok);
        }
    }

    private ExpressionNode parseCreateTablePartition(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (tok != null && isPartitionKeyword(tok)) {
            expectTok(lexer, "by");
            return expectLiteral(lexer);
        }
        return null;
    }

    private void parseDeclare(GenericLexer lexer, QueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        int contentLength = lexer.getContent().length();
        while (lexer.getPosition() < contentLength) {
            int pos = lexer.getPosition();

            CharSequence tok = optTok(lexer);

            if (tok == null) {
                break;
            }

            if (tok.charAt(0) == ',') {
                continue;
            }

            if (isSelectKeyword(tok) || !(tok.charAt(0) == '@')) {
                lexer.unparseLast();
                break;
            }

            CharacterStoreEntry cse = characterStore.newEntry();
            cse.put(tok);
            tok = cse.toImmutable();

            CharSequence expectWalrus = optTok(lexer);

            if (expectWalrus == null || !Chars.equals(expectWalrus, ":=")) {
                throw errUnexpected(lexer, expectWalrus, "expected variable assignment operator `:=`");
            }

            lexer.goToPosition(pos);

            ExpressionNode expr = expr(lexer, model, sqlParserCallback, model.getDecls(), tok);

            if (expr == null) {
                throw errUnexpected(lexer, tok, "declaration was empty or could not be parsed");
            }

            if (!Chars.equalsIgnoreCase(expr.lhs.token, tok)) {
                // could be a `DECLARE @x := (1,2,3)` situation
                throw errUnexpected(lexer, tok, "unexpected bind expression - bracket lists are not supported");
            }

            model.getDecls().put(tok, expr);
        }
    }

    private QueryModel parseDml(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
            int modelPosition,
            boolean useTopLevelWithClauses,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        QueryModel model = null;
        QueryModel prevModel = null;

        while (true) {
            LowerCaseCharSequenceObjHashMap<WithClauseModel> parentWithClauses = prevModel != null ? prevModel.getWithClauses() : withClauses;
            LowerCaseCharSequenceObjHashMap<WithClauseModel> topWithClauses = useTopLevelWithClauses && model == null ? topLevelWithModel : null;

            QueryModel unionModel = parseDml0(lexer, parentWithClauses, topWithClauses, modelPosition, sqlParserCallback, decls);
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
                    if (isDistinctKeyword(tok)) {
                        // union distinct is equal to just union, we only consume to 'distinct' token and we are good
                        modelPosition = lexer.getPosition();
                    } else {
                        lexer.unparseLast();
                        modelPosition = lexer.lastTokenPosition();
                    }
                }
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

            // check for decls
            if (prevModel.getDecls() != null && prevModel.getDecls().size() > 0 && decls == null) {
                decls = prevModel.getDecls();
            }
        }
    }

    @NotNull
    private QueryModel parseDml0(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> parentWithClauses,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> topWithClauses,
            int modelPosition,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        CharSequence tok;
        QueryModel model = queryModelPool.next();
        model.setModelPosition(modelPosition);

        // copy decls so nested nodes can use them
        model.copyDeclsFrom(decls);

        if (parentWithClauses != null) {
            model.getWithClauses().putAll(parentWithClauses);
        }

        tok = tok(lexer, "'select', 'with', 'declare' or table name expected");

        // [declare]
        if (isDeclareKeyword(tok)) {
            parseDeclare(lexer, model, sqlParserCallback);
            tok = tok(lexer, "'select', 'with', or table name expected");
        }

        // [with]
        if (isWithKeyword(tok)) {
            parseWithClauses(lexer, model.getWithClauses(), sqlParserCallback, model.getDecls());
            tok = tok(lexer, "'select' or table name expected");
        } else if (topWithClauses != null) {
            model.getWithClauses().putAll(topWithClauses);
        }

        // [select]
        if (isSelectKeyword(tok)) {
            parseSelectClause(lexer, model, sqlParserCallback);

            tok = optTok(lexer);

            if (tok != null && setOperations.contains(tok)) {
                tok = null;
            }

            if (tok == null || Chars.equals(tok, ';') || Chars.equals(tok, ')')) { // token can also be ';' on query boundary
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
        } else if (isShowKeyword(tok)) {
            model.setSelectModelType(QueryModel.SELECT_MODEL_SHOW);
            int showKind = -1;
            tok = SqlUtil.fetchNext(lexer);
            if (tok != null) {
                // show tables
                // show columns from tab
                // show partitions from tab
                // show transaction isolation level
                // show transaction_isolation
                // show max_identifier_length
                // show standard_conforming_strings
                // show search_path
                // show datestyle
                // show time zone
                // show create table tab
                // show create materialized view tab
                if (isTablesKeyword(tok)) {
                    showKind = QueryModel.SHOW_TABLES;
                } else if (isColumnsKeyword(tok)) {
                    parseFromTable(lexer, model);
                    showKind = QueryModel.SHOW_COLUMNS;
                } else if (isPartitionsKeyword(tok)) {
                    parseFromTable(lexer, model);
                    showKind = QueryModel.SHOW_PARTITIONS;
                } else if (isTransactionKeyword(tok)) {
                    showKind = QueryModel.SHOW_TRANSACTION;
                    validateShowTransactions(lexer);
                } else if (isTransactionIsolation(tok)) {
                    showKind = QueryModel.SHOW_TRANSACTION_ISOLATION_LEVEL;
                } else if (isMaxIdentifierLength(tok)) {
                    showKind = QueryModel.SHOW_MAX_IDENTIFIER_LENGTH;
                } else if (isStandardConformingStrings(tok)) {
                    showKind = QueryModel.SHOW_STANDARD_CONFORMING_STRINGS;
                } else if (isSearchPath(tok)) {
                    showKind = QueryModel.SHOW_SEARCH_PATH;
                } else if (isDateStyleKeyword(tok)) {
                    showKind = QueryModel.SHOW_DATE_STYLE;
                } else if (isTimeKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok != null && isZoneKeyword(tok)) {
                        showKind = QueryModel.SHOW_TIME_ZONE;
                    }
                } else if (isParametersKeyword(tok)) {
                    showKind = QueryModel.SHOW_PARAMETERS;
                } else if (isServerVersionKeyword(tok)) {
                    showKind = QueryModel.SHOW_SERVER_VERSION;
                } else if (isServerVersionNumKeyword(tok)) {
                    showKind = QueryModel.SHOW_SERVER_VERSION_NUM;
                } else if (isCreateKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok != null && isTableKeyword(tok)) {
                        parseTableName(lexer, model);
                        showKind = QueryModel.SHOW_CREATE_TABLE;
                    } else if (tok != null && isMaterializedKeyword(tok)) {
                        expectTok(lexer, "view");
                        parseTableName(lexer, model);
                        showKind = QueryModel.SHOW_CREATE_MAT_VIEW;
                    } else {
                        throw SqlException.position(lexer.getPosition()).put("expected 'TABLE' or 'MATERIALIZED VIEW'");
                    }
                } else {
                    showKind = sqlParserCallback.parseShowSql(lexer, model, tok, expressionNodePool);
                }
            }

            if (showKind == -1) {
                throw SqlException.position(lexer.getPosition()).put("expected ")
                        .put("'TABLES', 'COLUMNS FROM <tab>', 'PARTITIONS FROM <tab>', ")
                        .put("'TRANSACTION ISOLATION LEVEL', 'transaction_isolation', ")
                        .put("'max_identifier_length', 'standard_conforming_strings', ")
                        .put("'parameters', 'server_version', 'server_version_num', ")
                        .put("'search_path', 'datestyle', or 'time zone'");
            } else {
                model.setShowKind(showKind);
            }
        } else {
            lexer.unparseLast();
            SqlUtil.addSelectStar(
                    model,
                    queryColumnPool,
                    expressionNodePool
            );
        }

        if (model.getSelectModelType() != QueryModel.SELECT_MODEL_SHOW) {
            QueryModel nestedModel = queryModelPool.next();
            nestedModel.setModelPosition(modelPosition);

            parseFromClause(lexer, nestedModel, model, sqlParserCallback);
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
        }
        return model;
    }

    private QueryModel parseDmlUpdate(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
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
            parseUpdateClause(lexer, updateQueryModel, fromModel, sqlParserCallback);

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
                    nestedModel.addJoinModel(parseJoin(lexer, tok, joinType, topLevelWithModel, sqlParserCallback, decls));
                    tok = optTok(lexer);
                }
            } else if (tok != null && isSemicolon(tok)) {
                tok = null;
            } else if (tok != null && !isWhereKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "FROM, WHERE or EOF expected");
            }

            // [where]
            if (tok != null && isWhereKeyword(tok)) {
                ExpressionNode expr = expr(lexer, fromModel, sqlParserCallback, decls);
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

    // doesn't allow copy, rename
    private ExecutionModel parseExplain(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final CharSequence tok = tok(lexer, "'create', 'format', 'insert', 'update', 'select' or 'with'");

        if (isSelectKeyword(tok)) {
            return parseSelect(lexer, sqlParserCallback, null);
        }

        if (isCreateKeyword(tok)) {
            return parseCreate(lexer, executionContext, sqlParserCallback);
        }

        if (isUpdateKeyword(tok)) {
            return parseUpdate(lexer, sqlParserCallback, null);
        }

        if (isInsertKeyword(tok)) {
            return parseInsert(lexer, sqlParserCallback, null);
        }

        if (isWithKeyword(tok)) {
            return parseWith(lexer, sqlParserCallback, null);
        }

        return parseSelect(lexer, sqlParserCallback, null);
    }

    private int parseExplainOptions(GenericLexer lexer, CharSequence prevTok) throws SqlException {
        int parenthesisPos = lexer.getPosition();
        CharSequence explainTok = GenericLexer.immutableOf(prevTok);
        CharSequence tok = tok(lexer, "'create', 'insert', 'update', 'select', 'with' or '('");
        if (Chars.equals(tok, '(')) {
            tok = tok(lexer, "'format'");
            if (isFormatKeyword(tok)) {
                tok = tok(lexer, "'text' or 'json'");
                if (isTextKeyword(tok) || isJsonKeyword(tok)) {
                    int format = isJsonKeyword(tok) ? ExplainModel.FORMAT_JSON : ExplainModel.FORMAT_TEXT;
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

    private void parseFromClause(GenericLexer lexer, QueryModel model, QueryModel masterModel, SqlParserCallback sqlParserCallback) throws SqlException {
        CharSequence tok = expectTableNameOrSubQuery(lexer);

        // copy decls down
        model.copyDeclsFrom(masterModel);

        QueryModel proposedNested = null;
        ExpressionNode variableExpr;

        // check for variable as subquery
        if (tok.charAt(0) == '@' && (variableExpr = model.getDecls().get(tok)) != null && variableExpr.rhs != null && variableExpr.rhs.queryModel != null) {
            proposedNested = variableExpr.rhs.queryModel;
        }

        // expect "(" in case of sub-query
        if (Chars.equals(tok, '(') || proposedNested != null) {
            if (proposedNested == null) {
                proposedNested = parseAsSubQueryAndExpectClosingBrace(lexer, masterModel.getWithClauses(), true, sqlParserCallback, model.getDecls());
            }

            tok = optTok(lexer);

            // do not collapse aliased sub-queries or those that have timestamp()
            // select * from (table) x
            if (tok == null || (tableAliasStop.contains(tok) && !isTimestampKeyword(tok))) {
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
            parseSelectFrom(lexer, model, masterModel.getWithClauses(), sqlParserCallback);
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
            model.addJoinModel(parseJoin(lexer, tok, joinType, masterModel.getWithClauses(), sqlParserCallback, model.getDecls()));
            tok = optTok(lexer);
        }

        checkSupportedJoinType(lexer, tok);

        // expect [where]

        if (tok != null && isWhereKeyword(tok)) {
            if (model.getLatestByType() == QueryModel.LATEST_BY_NEW) {
                throw SqlException.$((lexer.lastTokenPosition()), "unexpected where clause after 'latest on'");
            }
            ExpressionNode expr = expr(lexer, model, sqlParserCallback, model.getDecls());
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
            expectSample(lexer, model, sqlParserCallback);
            tok = optTok(lexer);

            ExpressionNode fromNode = null, toNode = null;
            // support `SAMPLE BY 5m FROM foo TO bah`
            if (tok != null && isFromKeyword(tok)) {
                fromNode = expr(lexer, model, sqlParserCallback, model.getDecls());
                if (fromNode == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'timestamp' expression expected");
                }
                tok = optTok(lexer);
            }

            if (tok != null && isToKeyword(tok)) {
                toNode = expr(lexer, model, sqlParserCallback, model.getDecls());
                if (toNode == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'timestamp' expression expected");
                }
                tok = optTok(lexer);
            }

            model.setSampleByFromTo(fromNode, toNode);

            if (tok != null && isFillKeyword(tok)) {
                expectTok(lexer, '(');
                do {
                    final ExpressionNode fillNode = expr(lexer, model, sqlParserCallback, model.getDecls());
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
                        model.setSampleByTimezoneName(expectExpr(lexer, sqlParserCallback, model.getDecls()));
                        tok = optTok(lexer);
                        if (tok != null && isWithKeyword(tok)) {
                            tok = parseWithOffset(lexer, model, sqlParserCallback);
                        } else {
                            model.setSampleByOffset(ZERO_OFFSET);
                        }
                    } else if (isWithKeyword(tok)) {
                        tok = parseWithOffset(lexer, model, sqlParserCallback);
                    } else {
                        model.setSampleByTimezoneName(null);
                        model.setSampleByOffset(ZERO_OFFSET);
                    }
                } else if (isFirstKeyword(tok)) {
                    expectObservation(lexer);

                    if (model.getSampleByTo() != null || model.getSampleByFrom() != null) {
                        throw SqlException.$(lexer.getPosition(), "ALIGN TO FIRST OBSERVATION is incompatible with FROM-TO");
                    }

                    model.setSampleByTimezoneName(null);
                    model.setSampleByOffset(null);
                    tok = optTok(lexer);
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'calendar' or 'first observation' expected");
                }
            } else {
                // Set offset according to default config
                if (configuration.getSampleByDefaultAlignmentCalendar()) {
                    model.setSampleByOffset(ZERO_OFFSET);
                } else {
                    model.setSampleByOffset(null);
                }
            }
        }

        // expect [group by]

        if (tok != null && isGroupKeyword(tok)) {
            expectBy(lexer);
            do {
                tokIncludingLocalBrace(lexer, "literal");
                lexer.unparseLast();
                ExpressionNode n = expr(lexer, model, sqlParserCallback, model.getDecls());
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

                ExpressionNode n = expr(lexer, model, sqlParserCallback, model.getDecls());
                if (n == null || (n.type == ExpressionNode.QUERY || n.type == ExpressionNode.SET_OPERATION)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "literal or expression expected");
                }

                if ((n.type == ExpressionNode.CONSTANT && Chars.equals("''", n.token))
                        || (n.type == ExpressionNode.LITERAL && n.token.length() == 0)) {
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
            ExpressionNode lo = expr(lexer, model, sqlParserCallback, model.getDecls());
            ExpressionNode hi = null;

            tok = optTok(lexer);
            if (tok != null && Chars.equals(tok, ',')) {
                hi = expr(lexer, model, sqlParserCallback, model.getDecls());
            } else {
                lexer.unparseLast();
            }
            model.setLimit(lo, hi);
        } else {
            lexer.unparseLast();
        }
    }

    private void parseFromTable(GenericLexer lexer, QueryModel model) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !isFromKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put("expected 'from'");
        }
        parseTableName(lexer, model);
    }

    private void parseHints(GenericLexer lexer, QueryModel model) {
        CharSequence hintToken;
        boolean parsingParams = false;
        CharSequence hintKey = null;
        CharacterStoreEntry hintValuesEntry = null;
        boolean error = false;
        while ((hintToken = SqlUtil.fetchNextHintToken(lexer)) != null) {
            if (error) {
                // if in error state, just consume the rest of hints, but ignore them
                // since in error state we cannot reliably parse them
                continue;
            }

            if (Chars.equals(hintToken, '(')) {
                if (parsingParams) {
                    // hints cannot be nested
                    error = true;
                    continue;
                }
                if (hintKey == null) {
                    // missing key
                    error = true;
                    continue;
                }
                parsingParams = true;
                continue;
            }

            if (Chars.equals(hintToken, ')')) {
                if (!parsingParams) {
                    // unexpected closing parenthesis
                    error = true;
                    continue;
                }
                if (hintValuesEntry == null) {
                    // store last parameter-less hint, e.g. KEY()
                    model.addHint(hintKey, null);
                } else {
                    // ok, there are some parameters
                    model.addHint(hintKey, hintValuesEntry.toImmutable());
                    hintValuesEntry = null;
                }
                hintKey = null;
                parsingParams = false;
                continue;
            }

            if (parsingParams) {
                if (hintValuesEntry == null) {
                    // store first parameter
                    hintValuesEntry = characterStore.newEntry();
                } else {
                    hintValuesEntry.put(SqlHints.HINTS_PARAMS_DELIMITER);
                }
                hintValuesEntry.put(GenericLexer.unquote(hintToken));
                continue;
            }

            if (hintKey != null) {
                // store previous parameter-less hint
                model.addHint(hintKey, null);
            }
            CharacterStoreEntry entry = characterStore.newEntry();
            entry.put(hintToken);
            hintKey = entry.toImmutable();
        }
        if (!error && !parsingParams && hintKey != null) {
            // store the last parameter-less hint
            // why only when not parsingParams? dangling parsingParams indicates a syntax error and in this case
            // we don't want to store the hint
            model.addHint(hintKey, null);
        }
    }

    private void parseInVolume(GenericLexer lexer, CreateTableOperationBuilderImpl tableOpBuilder) throws SqlException {
        int volumeKwPos = lexer.getPosition();
        expectTok(lexer, "volume");
        CharSequence tok = tok(lexer, "path for volume");
        if (Os.isWindows()) {
            throw SqlException.position(volumeKwPos).put("'in volume' is not supported on Windows");
        }
        tableOpBuilder.setVolumeAlias(GenericLexer.unquote(tok), lexer.lastTokenPosition());
    }

    private ExecutionModel parseInsert(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        final InsertModel model = insertModelPool.next();
        CharSequence tok = tok(lexer, "atomic or into or batch");
        model.setBatchSize(configuration.getInsertModelBatchSize());
        boolean atomicSpecified = false;

        if (isAtomicKeyword(tok)) {
            atomicSpecified = true;
            model.setBatchSize(-1);
            tok = tok(lexer, "into");
        }

        if (isBatchKeyword(tok)) {
            long val = expectLong(lexer);
            if (val > 0) {
                model.setBatchSize(val);
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "batch size must be positive integer");
            }

            tok = tok(lexer, "into or o3MaxLag");
            if (isO3MaxLagKeyword(tok)) {
                int pos = lexer.getPosition();
                model.setO3MaxLag(SqlUtil.expectMicros(tok(lexer, "lag value"), pos));
                tok = tok(lexer, "into");
            }
        }

        if (!isIntoKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "'into' expected");
        }

        tok = tok(lexer, "table name");
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        model.setTableName(nextLiteral(assertNoDotsAndSlashes(unquote(tok), lexer.lastTokenPosition()), lexer.lastTokenPosition()));

        tok = tok(lexer, "'(' or 'select'");

        if (Chars.equals(tok, '(')) {
            do {
                tok = tok(lexer, "column");
                if (Chars.equals(tok, ')')) {
                    throw err(lexer, tok, "missing column name");
                }

                assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                model.addColumn(unquote(tok), lexer.lastTokenPosition());
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
            final QueryModel queryModel = parseDml(lexer, null, lexer.lastTokenPosition(), true, sqlParserCallback, decls);
            model.setQueryModel(queryModel);
            tok = optTok(lexer);
            // no more tokens or ';' should indicate end of statement
            if (tok == null || Chars.equals(tok, ';')) {
                return model;
            }
            throw errUnexpected(lexer, tok);
        }

        // if not INSERT INTO SELECT, make it atomic (select returns early)
        model.setBatchSize(-1);

        // if they used atomic or batch keywords, then throw an error
        if (atomicSpecified) {
            throw SqlException.$(lexer.lastTokenPosition(), "'atomic' keyword can only be used in INSERT INTO SELECT statements.");
        }

        if (isValuesKeyword(tok)) {
            do {
                expectTok(lexer, '(');
                ObjList<ExpressionNode> rowValues = new ObjList<>();
                do {
                    rowValues.add(expectExpr(lexer, sqlParserCallback));
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

    private QueryModel parseJoin(
            GenericLexer lexer,
            CharSequence tok,
            int joinType,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> parent,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        QueryModel joinModel = queryModelPool.next();

        joinModel.copyDeclsFrom(decls);

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
            joinModel.setNestedModel(parseAsSubQueryAndExpectClosingBrace(lexer, parent, true, sqlParserCallback, decls));
        } else {
            lexer.unparseLast();
            parseSelectFrom(lexer, joinModel, parent, sqlParserCallback);
        }

        tok = setModelAliasAndGetOptTok(lexer, joinModel);

        if (joinType == QueryModel.JOIN_CROSS && tok != null && isOnKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "Cross joins cannot have join clauses");
        }

        boolean onClauseObserved = false;
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
                onClauseObserved = true;
                try {
                    expressionParser.parseExpr(lexer, expressionTreeBuilder, sqlParserCallback, decls);
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
                                joinModel.setJoinCriteria(rewriteKnownStatements(expr, decls, null));
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

        tok = optTok(lexer);
        if (tok == null || !SqlKeywords.isToleranceKeyword(tok)) {
            lexer.unparseLast();
            return joinModel;
        }
        if (joinType != QueryModel.JOIN_ASOF && joinType != QueryModel.JOIN_LT) {
            throw SqlException.$(lexer.lastTokenPosition(), "TOLERANCE is only supported for ASOF and LT joins");
        }

        final ExpressionNode n = expr(lexer, null, sqlParserCallback, decls);
        if (n == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "ASOF JOIN TOLERANCE period expected");
        }
        if (n.type == ExpressionNode.OPERATION && n.token != null && Chars.equals(n.token, "-")) {
            throw SqlException.$(lexer.lastTokenPosition(), "ASOF JOIN TOLERANCE must be positive");
        }
        if (n.type != ExpressionNode.CONSTANT) {
            throw SqlException.$(lexer.lastTokenPosition(), "ASOF JOIN TOLERANCE must be a constant");
        }
        joinModel.setAsOfJoinTolerance(n);

        if (!onClauseObserved) {
            // no join clauses yet
            tok = optTok(lexer);
            if (tok != null && SqlKeywords.isOnKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'ON' clause must precede 'TOLERANCE' clause. " +
                        "Hint: put the ON condition right after the JOIN, then add TOLERANCE, " +
                        "e.g. … ASOF JOIN t2 ON t1.ts = t2.ts TOLERANCE 1h");
            }
            lexer.unparseLast();
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
            model.addLatestBy(expectLiteral(lexer, model.getDecls()));
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
        final ExpressionNode timestamp = expectLiteral(lexer, model.getDecls());
        model.setTimestamp(timestamp);
        // 'partition by'
        expectTok(lexer, "partition");
        expectTok(lexer, "by");
        // <columns>
        CharSequence tok;
        do {
            model.addLatestBy(expectLiteral(lexer, model.getDecls()));
            tok = SqlUtil.fetchNext(lexer);
        } while (Chars.equalsNc(tok, ','));

        model.setLatestByType(QueryModel.LATEST_BY_NEW);

        if (tok != null) {
            lexer.unparseLast();
        }
    }

    private ExecutionModel parseRenameStatement(GenericLexer lexer) throws SqlException {
        expectTok(lexer, "table");
        RenameTableModel model = renameTableModelPool.next();

        CharSequence tok = tok(lexer, "from table name");
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());

        model.setFrom(nextLiteral(unquote(tok), lexer.lastTokenPosition()));

        tok = tok(lexer, "to");
        if (Chars.equals(tok, '(')) {
            throw SqlException.$(lexer.lastTokenPosition(), "function call is not allowed here");
        }
        lexer.unparseLast();

        expectTok(lexer, "to");

        tok = tok(lexer, "to table name");
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        model.setTo(nextLiteral(unquote(tok), lexer.lastTokenPosition()));

        tok = optTok(lexer);

        if (tok != null && Chars.equals(tok, '(')) {
            throw SqlException.$(lexer.lastTokenPosition(), "function call is not allowed here");
        }

        if (tok != null && !Chars.equals(tok, ';')) {
            throw SqlException.$(lexer.lastTokenPosition(), "debris?");
        }

        return model;
    }

    private ExecutionModel parseSelect(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        lexer.unparseLast();
        final QueryModel model = parseDml(lexer, null, lexer.lastTokenPosition(), true, sqlParserCallback, decls);
        final CharSequence tok = optTok(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            return model;
        }
        if (Chars.equals(tok, ":=")) {
            throw errUnexpected(lexer, tok, "perhaps `DECLARE` was misspelled?");
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseSelectClause(GenericLexer lexer, QueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        int pos = lexer.getPosition();
        CharSequence tok = SqlUtil.fetchNext(lexer, true);
        if (tok == null || (subQueryMode && Chars.equals(tok, ')') && !overClauseMode)) {
            throw SqlException.position(pos).put("[distinct] column expected");
        }

        if (Chars.equals(tok, "/*+")) {
            parseHints(lexer, model);
            tok = tok(lexer, "[distinct] column");
        }

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
                    expr = expr(lexer, model, sqlParserCallback, model.getDecls());

                    if (expr == null) {
                        throw SqlException.$(lexer.lastTokenPosition(), "missing expression");
                    }

                    if (Chars.endsWith(expr.token, '.') && expr.type == ExpressionNode.LITERAL) {
                        throw SqlException.$(expr.position + expr.token.length(), "'*' or column name expected");
                    }
                }

                tok = optTok(lexer);

                QueryColumn col;
                final int colPosition = lexer.lastTokenPosition();

                // windowIgnoreNulls is 0 --> non-window context or default
                // windowIgnoreNulls is 1 --> ignore nulls
                // windowIgnoreNulls is 2 --> respect nulls
                byte windowNullsDesc = 0;
                if (tok != null) {
                    if (isIgnoreWord(tok)) {
                        windowNullsDesc = 1;
                    } else if (isRespectWord(tok)) {
                        windowNullsDesc = 2;
                    }
                }

                if (tok != null && windowNullsDesc > 0) {
                    CharSequence next = optTok(lexer);
                    if (next != null && isNullsWord(next)) {
                        expectTok(lexer, "over");
                    } else {
                        windowNullsDesc = 0;
                        lexer.backTo(colPosition, tok);
                    }
                }

                if ((tok != null && isOverKeyword(tok)) || windowNullsDesc > 0) {
                    // window function
                    expectTok(lexer, '(');
                    overClauseMode = true;//prevent lexer returning ')' ending over clause as null in a sub-query
                    try {
                        WindowColumn winCol = windowColumnPool.next().of(null, expr);
                        col = winCol;

                        tok = tokIncludingLocalBrace(lexer, "'partition' or 'order' or ')'");
                        winCol.setIgnoreNulls(windowNullsDesc == 1);
                        winCol.setNullsDescPos(windowNullsDesc > 0 ? colPosition : 0);

                        if (isPartitionKeyword(tok)) {
                            expectTok(lexer, "by");

                            ObjList<ExpressionNode> partitionBy = winCol.getPartitionBy();

                            do {
                                // allow dangling comma by previewing the token
                                tok = tok(lexer, "column name, 'order' or ')'");
                                if (isOrderKeyword(tok)) {
                                    if (partitionBy.size() == 0) {
                                        throw SqlException.$(lexer.lastTokenPosition(), "at least one column is expected in `partition by` clause");
                                    }
                                    break;
                                }
                                lexer.unparseLast();
                                partitionBy.add(expectExpr(lexer, sqlParserCallback, model.getDecls()));
                                tok = tok(lexer, "'order' or ')'");
                            } while (Chars.equals(tok, ','));
                        }

                        if (isOrderKeyword(tok)) {
                            expectTok(lexer, "by");

                            do {
                                final ExpressionNode orderByExpr = expectExpr(lexer, sqlParserCallback, model.getDecls());

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
                            framingMode = WindowColumn.FRAMING_ROWS;
                        } else if (isRangeKeyword(tok)) {
                            framingMode = WindowColumn.FRAMING_RANGE;
                        } else if (isGroupsKeyword(tok)) {
                            framingMode = WindowColumn.FRAMING_GROUPS;
                        } else if (!Chars.equals(tok, ')')) {
                            throw SqlException.$(lexer.lastTokenPosition(), "'rows', 'groups', 'range' or ')' expected");
                        }

                    /* PG documentation:
                       The default framing option is RANGE UNBOUNDED PRECEDING, which is the same as RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
                       With ORDER BY, this sets the frame to be all rows from the partition start up through the current row's last ORDER BY peer.
                       Without ORDER BY, this means all rows of the partition are included in the window frame, since all rows become peers of the current row.
                     */

                        if (framingMode != -1) {
                            winCol.setFramingMode(framingMode);

                            if (framingMode == WindowColumn.FRAMING_GROUPS && winCol.getOrderBy().size() == 0) {
                                throw SqlException.$(lexer.lastTokenPosition(), "GROUPS mode requires an ORDER BY clause");
                            }

                            // These keywords define for each row a window (a physical or logical
                            // set of rows) used for calculating the function result. The function is
                            // then applied to all the rows in the window. The window moves through the
                            // query result set or partition from top to bottom.

                        /*
                        { ROWS | GROUPS | RANGE }
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
                                    winCol.setRowsLoKind(WindowColumn.PRECEDING, lexer.lastTokenPosition());
                                } else if (isCurrentRow(lexer, tok)) {
                                    // As a start point, CURRENT ROW specifies that the window begins at the current row.
                                    // In this case the end point cannot be value_expr PRECEDING.
                                    winCol.setRowsLoKind(WindowColumn.CURRENT, lexer.lastTokenPosition());
                                } else if (isPrecedingKeyword(tok)) {
                                    throw SqlException.$(lexer.lastTokenPosition(), "integer expression expected");
                                } else {
                                    pos = lexer.lastTokenPosition();
                                    lexer.unparseLast();
                                    winCol.setRowsLoExpr(expectExpr(lexer, sqlParserCallback, model.getDecls()), pos);
                                    if (framingMode == WindowColumn.FRAMING_RANGE) {
                                        char timeUnit = parseTimeUnit(lexer);
                                        if (timeUnit != 0) {
                                            winCol.setRowsLoExprTimeUnit(timeUnit);
                                        }
                                    }

                                    tok = tok(lexer, "'preceding' or 'following'");
                                    if (isPrecedingKeyword(tok)) {
                                        winCol.setRowsLoKind(WindowColumn.PRECEDING, lexer.lastTokenPosition());
                                    } else if (isFollowingKeyword(tok)) {
                                        winCol.setRowsLoKind(WindowColumn.FOLLOWING, lexer.lastTokenPosition());
                                    } else {
                                        throw SqlException.$(lexer.lastTokenPosition(), "'preceding' or 'following' expected");
                                    }
                                }

                                if (winCol.getOrderBy().size() != 1 && winCol.requiresOrderBy()) {//groups mode is validated earlier
                                    throw SqlException.$(lexer.lastTokenPosition(), "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column");
                                }

                                tok = tok(lexer, "'and'");

                                if (isAndKeyword(tok)) {
                                    tok = tok(lexer, "'unbounded', 'current' or expression");
                                    // hi
                                    if (isUnboundedKeyword(tok)) {
                                        tok = tok(lexer, "'following'");
                                        if (isFollowingKeyword(tok)) {
                                            // Specify UNBOUNDED FOLLOWING to indicate that the window ends at the
                                            // last row of the partition. This is the end point specification and
                                            // cannot be used as a start point specification.
                                            winCol.setRowsHiKind(WindowColumn.FOLLOWING, lexer.lastTokenPosition());
                                        } else {
                                            throw SqlException.$(lexer.lastTokenPosition(), "'following' expected");
                                        }
                                    } else if (isCurrentRow(lexer, tok)) {
                                        winCol.setRowsHiKind(WindowColumn.CURRENT, lexer.lastTokenPosition());
                                    } else if (isPrecedingKeyword(tok) || isFollowingKeyword(tok)) {
                                        throw SqlException.$(lexer.lastTokenPosition(), "integer expression expected");
                                    } else {
                                        pos = lexer.lastTokenPosition();
                                        lexer.unparseLast();
                                        winCol.setRowsHiExpr(expectExpr(lexer, sqlParserCallback, model.getDecls()), pos);
                                        if (framingMode == WindowColumn.FRAMING_RANGE) {
                                            char timeUnit = parseTimeUnit(lexer);
                                            if (timeUnit != 0) {
                                                winCol.setRowsHiExprTimeUnit(timeUnit);
                                            }
                                        }

                                        tok = tok(lexer, "'preceding'  'following'");
                                        if (isPrecedingKeyword(tok)) {
                                            if (winCol.getRowsLoKind() == WindowColumn.CURRENT) {
                                                // As a start point, CURRENT ROW specifies that the window begins at the current row.
                                                // In this case the end point cannot be value_expr PRECEDING.
                                                throw SqlException.$(lexer.lastTokenPosition(), "start row is CURRENT, end row not must be PRECEDING");
                                            }
                                            if (winCol.getRowsLoKind() == WindowColumn.FOLLOWING) {
                                                throw SqlException.$(lexer.lastTokenPosition(), "start row is FOLLOWING, end row not must be PRECEDING");
                                            }
                                            winCol.setRowsHiKind(WindowColumn.PRECEDING, lexer.lastTokenPosition());
                                        } else if (isFollowingKeyword(tok)) {
                                            winCol.setRowsHiKind(WindowColumn.FOLLOWING, lexer.lastTokenPosition());
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
                                pos = lexer.lastTokenPosition();
                                if (isUnboundedPreceding(lexer, tok)) {
                                    winCol.setRowsLoKind(WindowColumn.PRECEDING, lexer.lastTokenPosition());
                                } else if (isCurrentRow(lexer, tok)) {
                                    winCol.setRowsLoKind(WindowColumn.CURRENT, lexer.lastTokenPosition());
                                } else if (isPrecedingKeyword(tok) || isFollowingKeyword(tok)) {
                                    throw SqlException.$(pos, "integer expression expected");
                                } else {
                                    lexer.unparseLast();
                                    winCol.setRowsLoExpr(expectExpr(lexer, sqlParserCallback, model.getDecls()), pos);
                                    if (framingMode == WindowColumn.FRAMING_RANGE) {
                                        char timeUnit = parseTimeUnit(lexer);
                                        if (timeUnit != 0) {
                                            winCol.setRowsLoExprTimeUnit(timeUnit);
                                        }
                                    }
                                    tok = tok(lexer, "'preceding'");
                                    if (isPrecedingKeyword(tok)) {
                                        winCol.setRowsLoKind(WindowColumn.PRECEDING, lexer.lastTokenPosition());
                                    } else {
                                        throw SqlException.$(lexer.lastTokenPosition(), "'preceding' expected");
                                    }
                                }

                                winCol.setRowsHiKind(WindowColumn.CURRENT, pos);
                            }

                            if (winCol.getOrderBy().size() != 1 && winCol.requiresOrderBy()) {//groups mode is validated earlier
                                throw SqlException.$(lexer.lastTokenPosition(), "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column");
                            }

                            tok = tok(lexer, "'exclude' or ')' expected");

                            if (isExcludeKeyword(tok)) {
                                tok = tok(lexer, "'current', 'group', 'ties' or 'no other' expected");
                                int excludePos = lexer.lastTokenPosition();
                                if (isCurrentKeyword(tok)) {
                                    tok = tok(lexer, "'row' expected");
                                    if (isRowKeyword(tok)) {
                                        winCol.setExclusionKind(WindowColumn.EXCLUDE_CURRENT_ROW, excludePos);
                                    } else {
                                        throw SqlException.$(lexer.lastTokenPosition(), "'row' expected");
                                    }
                                } else if (isGroupKeyword(tok)) {
                                    winCol.setExclusionKind(WindowColumn.EXCLUDE_GROUP, excludePos);
                                } else if (isTiesKeyword(tok)) {
                                    winCol.setExclusionKind(WindowColumn.EXCLUDE_TIES, excludePos);
                                } else if (isNoKeyword(tok)) {
                                    tok = tok(lexer, "'others' expected");
                                    if (isOthersKeyword(tok)) {
                                        winCol.setExclusionKind(WindowColumn.EXCLUDE_NO_OTHERS, excludePos);
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
                    } finally {
                        overClauseMode = false;
                    }
                    tok = optTok(lexer);

                } else {
                    if (expr.type == ExpressionNode.QUERY) {
                        throw SqlException.$(expr.position, "query is not expected, did you mean column?");
                    }
                    col = queryColumnPool.next().of(null, expr);
                }

                final CharSequence alias;
                final int aliasPosition;
                if (tok != null && columnAliasStop.excludes(tok)) {
                    assertNotDot(lexer, tok);

                    // verify that * wildcard is not aliased

                    if (isAsKeyword(tok)) {
                        tok = tok(lexer, "alias");
                        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                        CharSequence aliasTok = GenericLexer.immutableOf(tok);
                        validateIdentifier(lexer, aliasTok);
                        boolean unquoting = Chars.indexOf(aliasTok, '.') == -1;
                        alias = unquoting ? unquote(aliasTok) : aliasTok;
                        aliasPosition = lexer.lastTokenPosition();
                    } else {
                        validateIdentifier(lexer, tok);
                        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                        boolean unquoting = Chars.indexOf(tok, '.') == -1;
                        alias = GenericLexer.immutableOf(unquoting ? unquote(tok) : tok);
                        aliasPosition = lexer.lastTokenPosition();
                    }

                    if (col.getAst().isWildcard()) {
                        throw err(lexer, null, "wildcard cannot have alias");
                    }

                    tok = optTok(lexer);
                    aliasMap.put(alias, col);
                } else {
                    alias = null;
                    aliasPosition = QueryColumn.SYNTHESIZED_ALIAS_POSITION;
                }

                // correlated sub-queries do not have expr.token values (they are null)
                if (expr.type == ExpressionNode.QUERY) {
                    expr.token = alias;
                }

                if (alias != null) {
                    if (alias.length() == 0) {
                        throw err(lexer, null, "column alias cannot be a blank string");
                    }
                    col.setAlias(alias, aliasPosition);
                }

                accumulatedColumns.add(col);
                accumulatedColumnPositions.add(colPosition);

                if (tok == null || Chars.equals(tok, ';') || Chars.equals(tok, ')')) {
                    //accept ending ')' in create table as
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
                    if (isIgnoreWord(tok) || isRespectWord(tok)) {
                        throw err(lexer, tok, "',', 'nulls' or 'from' expected");
                    }
                    throw err(lexer, tok, "',', 'from' or 'over' expected");
                }
            }

            for (int i = 0, n = accumulatedColumns.size(); i < n; i++) {
                QueryColumn qc = accumulatedColumns.getQuick(i);
                if (qc.getAlias() == null) {
                    generateColumnAlias(lexer, qc, hasFrom);
                }
                model.addBottomUpColumn(accumulatedColumnPositions.getQuick(i), qc, false);
            }
        } finally {
            accumulatedColumns.clear();
            accumulatedColumnPositions.clear();
            aliasMap.clear();
        }
    }

    private void parseSelectFrom(
            GenericLexer lexer,
            QueryModel model,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> masterModel,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        ExpressionNode expr = expr(lexer, model, sqlParserCallback);
        if (expr == null) {
            throw SqlException.position(lexer.lastTokenPosition()).put("table name expected");
        }

        // subquery is expected to be handled outside
        if (expr.type != ExpressionNode.LITERAL && expr.type != ExpressionNode.CONSTANT && expr.type != ExpressionNode.FUNCTION) {
            throw SqlException.$(expr.position, "function, literal or constant is expected");
        }

        // check if it's a decl
        if (model.getDecls().contains(expr.token)) {
            if (expr.type == ExpressionNode.LITERAL) {
                // replace it if so
                expr = model.getDecls().get(expr.token).rhs;
            } else {
                throw SqlException.$(lexer.lastTokenPosition(), "expected literal table name or subquery");
            }
        }

        CharSequence tableName = expr.token;
        switch (expr.type) {
            case ExpressionNode.LITERAL:
            case ExpressionNode.CONSTANT:
                final WithClauseModel withClause = masterModel.get(tableName);
                if (withClause != null) {
                    model.setNestedModel(parseWith(lexer, withClause, sqlParserCallback, model.getDecls()));
                    model.setAlias(literal(tableName, expr.position));
                } else {
                    int dot = Chars.indexOfLastUnquoted(tableName, '.');
                    if (dot == -1) {
                        model.setTableNameExpr(literal(tableName, expr.position));
                    } else {
                        if (isPublicKeyword(tableName, 0, dot)) {
                            if (dot + 1 == tableName.length()) {
                                throw SqlException.$(expr.position, "table name expected");
                            }

                            BufferWindowCharSequence fs = (BufferWindowCharSequence) tableName;
                            fs.shiftLo(dot + 1);
                            model.setTableNameExpr(literal(tableName, expr.position + dot + 1));
                        } else {
                            model.setTableNameExpr(literal(tableName, expr.position));
                        }
                    }
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

    private void parseTableName(GenericLexer lexer, QueryModel model) throws SqlException {
        CharSequence tok = tok(lexer, "expected a table name");
        tok = sansPublicSchema(tok, lexer);
        final CharSequence tableName = assertNoDotsAndSlashes(unquote(tok), lexer.lastTokenPosition());
        ExpressionNode tableNameExpr = expressionNodePool.next().of(ExpressionNode.LITERAL, tableName, 0, lexer.lastTokenPosition());
        tableNameExpr = rewriteDeclaredVariables(tableNameExpr, model.getDecls(), null);
        model.setTableNameExpr(tableNameExpr);
    }

    private char parseTimeUnit(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "'preceding' or time unit");
        char unit = 0;
        if (isNanosecondsKeyword(tok) || isNanosecondKeyword(tok)) {
            unit = WindowColumn.TIME_UNIT_NANOSECOND;
        } else if (isMicrosecondKeyword(tok) || isMicrosecondsKeyword(tok)) {
            unit = WindowColumn.TIME_UNIT_MICROSECOND;
        } else if (isMillisecondKeyword(tok) || isMillisecondsKeyword(tok)) {
            unit = WindowColumn.TIME_UNIT_MILLISECOND;
        } else if (isSecondKeyword(tok) || isSecondsKeyword(tok)) {
            unit = WindowColumn.TIME_UNIT_SECOND;
        } else if (isMinuteKeyword(tok) || isMinutesKeyword(tok)) {
            unit = WindowColumn.TIME_UNIT_MINUTE;
        } else if (isHourKeyword(tok) || isHoursKeyword(tok)) {
            unit = WindowColumn.TIME_UNIT_HOUR;
        } else if (isDayKeyword(tok) || isDaysKeyword(tok)) {
            unit = WindowColumn.TIME_UNIT_DAY;
        }
        if (unit == 0) {
            lexer.unparseLast();
        }
        return unit;
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

    private ExecutionModel parseUpdate(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        lexer.unparseLast();
        final QueryModel model = parseDmlUpdate(lexer, sqlParserCallback, decls);
        final CharSequence tok = optTok(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            return model;
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseUpdateClause(
            GenericLexer lexer,
            QueryModel updateQueryModel,
            QueryModel fromModel,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        CharSequence tok = tok(lexer, "table name or alias");
        tok = sansPublicSchema(tok, lexer);
        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
        CharSequence tableName = GenericLexer.immutableOf(unquote(tok));
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
            assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
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
            CharSequence col = GenericLexer.immutableOf(unquote(tok));
            int colPosition = lexer.lastTokenPosition();

            expectTok(lexer, "=");

            // Value expression
            ExpressionNode expr = expr(lexer, (QueryModel) null, sqlParserCallback);
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

    @SuppressWarnings("SameParameterValue")
    @NotNull
    private ExecutionModel parseWith(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        parseWithClauses(lexer, topLevelWithModel, sqlParserCallback, decls);
        CharSequence tok = tok(lexer, "'select', 'update' or name expected");
        if (isSelectKeyword(tok)) {
            return parseSelect(lexer, sqlParserCallback, decls);
        }

        if (isUpdateKeyword(tok)) {
            return parseUpdate(lexer, sqlParserCallback, decls);
        }

        if (isInsertKeyword(tok)) {
            return parseInsert(lexer, sqlParserCallback, decls);
        }

        throw SqlException.$(lexer.lastTokenPosition(), "'select' | 'update' | 'insert' expected");
    }

    private QueryModel parseWith(
            GenericLexer lexer,
            WithClauseModel wcm,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        QueryModel m = wcm.popModel();
        if (m != null) {
            return m;
        }

        lexer.stash();
        lexer.goToPosition(wcm.getPosition());
        // this will not throw exception because this is second pass over the same sub-query
        // we wouldn't be here is syntax was wrong
        m = parseAsSubQueryAndExpectClosingBrace(lexer, wcm.getWithClauses(), false, sqlParserCallback, decls);
        lexer.unstash();
        return m;
    }

    private void parseWithClauses(
            GenericLexer lexer,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> model,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
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
            // todo: review passing non-null here
            wcm.of(lo + 1, model, parseAsSubQueryAndExpectClosingBrace(lexer, model, true, sqlParserCallback, decls));
            model.put(name.token, wcm);

            CharSequence tok = optTok(lexer);
            if (tok == null || !Chars.equals(tok, ',')) {
                lexer.unparseLast();
                break;
            }
        } while (true);
    }

    private CharSequence parseWithOffset(GenericLexer lexer, QueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        CharSequence tok;
        expectOffset(lexer);
        model.setSampleByOffset(expectExpr(lexer, sqlParserCallback, model.getDecls()));
        tok = optTok(lexer);
        return tok;
    }

    private void rewriteCase(ExpressionNode node) {
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
                // else expression may not have been provided,
                // in which case it needs to be synthesized
                if (elseExpr == null) {
                    elseExpr = SqlUtil.nextConstant(expressionNodePool, "null", node.position);
                }
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

    private void rewriteConcat(ExpressionNode node) {
        if (node.type == ExpressionNode.OPERATION && isConcatOperator(node.token)) {
            node.type = ExpressionNode.FUNCTION;
            node.token = CONCAT_FUNC_NAME;
            addConcatArgs(node.args, node.rhs);
            addConcatArgs(node.args, node.lhs);
            node.paramCount = node.args.size();
            if (node.paramCount > 2) {
                node.rhs = null;
                node.lhs = null;
            }
        }
    }

    /**
     * Rewrites count(*) expressions to count().
     *
     * @param node expression node, provided by tree walking algo
     */
    private void rewriteCount(ExpressionNode node) {
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

    private ExpressionNode rewriteDeclaredVariables(
            ExpressionNode expr,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            @Nullable CharSequence exprTargetVariableName
    ) throws SqlException {
        if (decls == null || decls.size() == 0) { // short circuit null case
            return expr;
        }
        return recursiveReplace(
                expr,
                rewriteDeclaredVariablesInExpressionVisitor.of(decls, exprTargetVariableName)
        );
    }

    /**
     * Rewrites the following:
     * <p>
     * select json_extract(json,path)::varchar -> select json_extract(json,path)
     * select json_extract(json,path)::double -> select json_extract(json,path,double)
     * select json_extract(json,path)::uuid -> select json_extract(json,path)::uuid
     * <p>
     * Notes:
     * - varchar cast it rewritten in a special way, e.g. removed
     * - subset of types is handled more efficiently in the 3-arg function
     * - the remaining type casts are not rewritten, e.g. left as is
     */
    private void rewriteJsonExtractCast(ExpressionNode node) {
        if (node.type == ExpressionNode.FUNCTION && isCastKeyword(node.token)) {
            if (node.lhs != null && isJsonExtract(node.lhs.token) && node.lhs.paramCount == 2) {
                // rewrite cast such as
                // json_extract(json,path)::type -> json_extract(json,path,type)
                // the ::type is already rewritten as
                // cast(json_extract(json,path) as type)
                //

                // we remove the outer cast and let json_extract() do the cast
                ExpressionNode jsonExtractNode = node.lhs;
                // check if the type is a valid symbol
                ExpressionNode typeNode = node.rhs;
                if (typeNode != null) {
                    int castType = ColumnType.typeOf(typeNode.token);
                    if (castType == ColumnType.VARCHAR) {
                        // redundant cast to varchar, just remove it
                        node.token = jsonExtractNode.token;
                        node.paramCount = jsonExtractNode.paramCount;
                        node.type = jsonExtractNode.type;
                        node.position = jsonExtractNode.position;
                        node.lhs = jsonExtractNode.lhs;
                        node.rhs = jsonExtractNode.rhs;
                        node.args.clear();
                    } else if (JsonExtractTypedFunctionFactory.isIntrusivelyOptimized(castType)) {
                        int type = ColumnType.typeOf(typeNode.token);
                        node.token = jsonExtractNode.token;
                        node.paramCount = 3;
                        node.type = jsonExtractNode.type;
                        node.position = jsonExtractNode.position;
                        node.lhs = null;
                        node.rhs = null;
                        node.args.clear();

                        // args are added in reverse order

                        // type integer
                        CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
                        characterStoreEntry.put(type);
                        node.args.add(
                                expressionNodePool.next().of(
                                        ExpressionNode.CONSTANT,
                                        characterStoreEntry.toImmutable(),
                                        typeNode.precedence,
                                        typeNode.position
                                )
                        );
                        node.args.add(jsonExtractNode.rhs);
                        node.args.add(jsonExtractNode.lhs);
                    }
                }
            }
        }
    }

    private ExpressionNode rewriteKnownStatements(
            ExpressionNode parent,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            @Nullable CharSequence exprTargetVariableName
    ) throws SqlException {
        traversalAlgo.traverse(parent, rewriteCountRef);
        traversalAlgo.traverse(parent, rewriteCaseRef);
        traversalAlgo.traverse(parent, rewriteConcatRef);
        traversalAlgo.traverse(parent, rewritePgCastRef);
        traversalAlgo.traverse(parent, rewriteJsonExtractCastRef);
        return rewriteDeclaredVariables(parent, decls, exprTargetVariableName);
    }

    private void rewritePgCast(ExpressionNode node) {
        if (node.type == ExpressionNode.OPERATION && isColonColon(node.token)) {
            node.token = "cast";
            node.type = ExpressionNode.FUNCTION;
            node.rhs.type = ExpressionNode.CONSTANT;
            // In PG x::float casts x to "double precision" type
            // also, we have to rewrite postgres types such as "float8" to our native "double" type
            // All of the above also applies to array types: "float8[]" -> "double[]"
            // or "double precision[][]" -> "double[][]"

            if (rewritePgCast0(node.rhs, "float", ColumnType.DOUBLE)) {
                return;
            }
            if (rewritePgCast0(node.rhs, "float8", ColumnType.DOUBLE)) {
                return;
            }
            if (rewritePgCast0(node.rhs, "float4", ColumnType.FLOAT)) {
                return;
            }
            if (rewritePgCast0(node.rhs, "int4", ColumnType.INT)) {
                return;
            }
            if (rewritePgCast0(node.rhs, "int8", ColumnType.LONG)) {
                return;
            }
            if (rewritePgCast0(node.rhs, "int2", ColumnType.SHORT)) {
                return;
            }
            rewritePgCast0(node.rhs, "double precision", ColumnType.DOUBLE);
        }
    }

    private boolean rewritePgCast0(ExpressionNode typeNode, String srcTypePrefix, short type) {
        CharSequence token = typeNode.token;
        if (!Chars.startsWithLowerCase(token, srcTypePrefix)) {
            return false;
        }

        int len = token.length();
        int prefixLen = srcTypePrefix.length();
        int rem = len - prefixLen;

        if (rem == 0) {
            // full match. e.g. replacing 'float8' with 'double'
            typeNode.token = ColumnType.nameOf(type);
            return true;
        }

        // src has a suffix. it could be an array suffix. consider 'float8[][]' -> 'double[][]'
        if (rem % 2 == 0) {
            // suffix must be even, since square brackets come in pairs
            int dims = rem / 2;
            String suffix = ColumnType.ARRAY_DIM_SUFFIX[dims];
            if (Chars.endsWith(token, suffix)) {
                typeNode.token = ColumnType.nameOf(ColumnType.encodeArrayType(type, dims));
                return true;
            }
        }
        return false;
    }

    @NotNull
    private CharSequence sansPublicSchema(@NotNull CharSequence tok, GenericLexer lexer) throws SqlException {
        int lo = 0;
        int hi = tok.length();
        if (Chars.isQuoted(tok)) {
            lo = 1;
            hi--;
        }
        if (!isPublicKeyword(tok, lo, hi)) {
            return tok;
        }

        CharSequence savedTok = GenericLexer.immutableOf(tok);
        tok = optTok(lexer);
        if (tok == null) {
            return savedTok;
        }
        if (!Chars.equals(tok, '.')) {
            lexer.unparseLast();
            return savedTok;
        }

        tok = tok(lexer, "table name");
        return tok;
    }

    private CharSequence setModelAliasAndGetOptTok(GenericLexer lexer, QueryModel joinModel) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok != null && tableAliasStop.excludes(tok)) {
            checkSupportedJoinType(lexer, tok);
            if (isAsKeyword(tok)) {
                tok = tok(lexer, "alias");
            }
            if (tok.length() == 0 || isEmptyAlias(tok)) {
                throw SqlException.position(lexer.lastTokenPosition()).put("Empty table alias");
            }
            assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
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

    private int toColumnType(GenericLexer lexer, @NotNull CharSequence tok) throws SqlException {
        int typePosition = lexer.lastTokenPosition();
        if (Chars.equalsNc(tok, '[')) {
            // '[' is a wierd type name, it could be that someone is either:
            // 1. array dereferencing [x]
            // 2. inverting array definition, []type
            // 3. left out array definition (type), e.g. just []
            // 4. dangling [, e.g. there is no closing ]

            // we can be brave here, we will error out already, [ is not a type regardless of what we find
            tok = optTok(lexer);
            if (tok == null) {
                throw SqlException.position(typePosition).put("dangling '[' where column type is expected");
            }

            if (Chars.equals(tok, ']')) {
                // we have []
                // lets see if there is a type
                tok = optTok(lexer);
                if (tok == null) {
                    throw SqlException.position(typePosition).put("did you mean 'double[]'?");
                }
                if (!Chars.equals(tok, ')') && !Chars.equals(tok, ',') && !Chars.equals(tok, '(')) {
                    throw SqlException.position(typePosition).put("did you mean '").put(tok).put("[]'?");
                }
            }
            throw SqlException.position(typePosition).put("column type is expected here");
        }
        final int columnType = SqlUtil.toPersistedType(tok, typePosition);
        final int typeTagPosition = lexer.lastTokenPosition();

        // ignore precision keyword for DOUBLE column: 'double precision' is the same type as 'double'
        if (ColumnType.tagOf(columnType) == ColumnType.DOUBLE) {
            CharSequence next = optTok(lexer);
            if (next != null && !isPrecisionKeyword(next)) {
                lexer.unparseLast();
            }
        }

        int nDims = SqlUtil.parseArrayDimensionality(lexer, columnType, typeTagPosition);
        if (nDims > 0) {
            if (!ColumnType.isSupportedArrayElementType(columnType)) {
                throw SqlException.position(typePosition)
                        .put("unsupported array element type [type=")
                        .put(ColumnType.nameOf(columnType))
                        .put(']');
            }
            if (nDims > ColumnType.ARRAY_NDIMS_LIMIT) {
                throw SqlException.position(typePosition)
                        .put("too many array dimensions [nDims=").put(nDims)
                        .put(", maxNDims=").put(ColumnType.ARRAY_NDIMS_LIMIT)
                        .put(']');
            }
            return ColumnType.encodeArrayType(columnType, nDims);
        }

        if (ColumnType.tagOf(columnType) == ColumnType.GEOHASH) {
            expectTok(lexer, '(');
            final int bits = GeoHashUtil.parseGeoHashBits(lexer.lastTokenPosition(), 0, expectLiteral(lexer).token);
            expectTok(lexer, ')');
            return ColumnType.getGeoHashTypeWithBits(bits);
        }
        return columnType;
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

    private void validateMatViewQuery(QueryModel model, String baseTableName) throws SqlException {
        for (QueryModel m = model; m != null; m = m.getNestedModel()) {
            tableNames.clear();
            tableNamePositions.clear();
            collectAllTableNames(m, tableNames, null);
            final boolean baseTableQueried = tableNames.contains(baseTableName);
            final int queriedTableCount = tableNames.size();
            if (baseTableQueried) {
                if (m.getSampleBy() != null && m.getSampleByOffset() == null) {
                    throw SqlException.position(m.getSampleBy().position + m.getSampleBy().token.length() + 1)
                            .put("ALIGN TO FIRST OBSERVATION on base table is not supported for materialized views: ").put(baseTableName);
                }

                if ((m.getSampleByFrom() != null || m.getSampleByTo() != null)) {
                    final int position = m.getSampleByFrom() != null ? m.getSampleByFrom().position : m.getSampleByTo().position;
                    throw SqlException.position(position)
                            .put("FROM-TO on base table is not supported for materialized views: ").put(baseTableName);
                }

                final ObjList<ExpressionNode> sampleByFill = m.getSampleByFill();
                if (sampleByFill != null && sampleByFill.size() > 0) {
                    throw SqlException.position(sampleByFill.get(0).position)
                            .put("FILL on base table is not supported for materialized views: ").put(baseTableName);
                }

                ObjList<QueryColumn> columns = m.getColumns();
                QueryColumn windowFuncColumn = null;
                for (int i = 0, n = columns.size(); i < n; i++) {
                    QueryColumn column = columns.getQuick(i);
                    if (column.isWindowColumn()) {
                        windowFuncColumn = column;
                    }

                    if (!Chars.equals(column.getName(), '*') && !TableUtils.isValidColumnName(column.getName(), configuration.getMaxFileNameLength())) {
                        if (column.getAliasPosition() == QueryColumn.SYNTHESIZED_ALIAS_POSITION) {
                            throw SqlException
                                    .position(column.getAst().position)
                                    .put("column '").put(column.getName()).put("' requires an explicit alias. Use: ")
                                    .put(column.getName()).put(" AS your_column_name");
                        } else {
                            throw SqlException
                                    .position(column.getAliasPosition())
                                    .put("column alias '").put(column.getName()).put("' contains unsupported characters");
                        }
                    }
                }

                if (windowFuncColumn != null) {
                    throw SqlException.position(windowFuncColumn.getAst().position)
                            .put("window function on base table is not supported for materialized views: ").put(baseTableName);
                }
            }

            final ObjList<QueryModel> joinModels = m.getJoinModels();
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final QueryModel joinModel = joinModels.getQuick(i);
                if (joinModel == m) {
                    continue;
                }
                validateMatViewQuery(joinModel, baseTableName);
            }

            final QueryModel unionModel = m.getUnionModel();
            if (unionModel != null) {
                // allow self-UNION on base table, but disallow UNION on base table with any other tables
                if (baseTableQueried && queriedTableCount > 1) {
                    throw SqlException.position(m.getUnionModel().getModelPosition())
                            .put("union on base table is not supported for materialized views: ").put(baseTableName);
                }
                validateMatViewQuery(unionModel, baseTableName);
            }
        }
    }

    void clear() {
        queryModelPool.clear();
        queryColumnPool.clear();
        expressionNodePool.clear();
        windowColumnPool.clear();
        createMatViewOperationBuilder.clear();
        createTableOperationBuilder.clear();
        createTableColumnModelPool.clear();
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
        traversalAlgo.clear();
    }

    ExpressionNode expr(
            GenericLexer lexer,
            QueryModel model,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            @Nullable CharSequence exprTargetVariableName
    ) throws SqlException {
        try {
            expressionTreeBuilder.pushModel(model);
            expressionParser.parseExpr(lexer, expressionTreeBuilder, sqlParserCallback, decls);
            return rewriteKnownStatements(expressionTreeBuilder.poll(), decls, exprTargetVariableName);
        } catch (SqlException e) {
            expressionTreeBuilder.reset();
            throw e;
        } finally {
            expressionTreeBuilder.popModel();
        }
    }

    ExpressionNode expr(GenericLexer lexer, QueryModel model, SqlParserCallback sqlParserCallback, @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls) throws SqlException {
        return expr(lexer, model, sqlParserCallback, decls, null);
    }

    ExpressionNode expr(GenericLexer lexer, QueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        return expr(lexer, model, sqlParserCallback, null, null);
    }

    // test only
    @TestOnly
    void expr(GenericLexer lexer, ExpressionParserListener listener, SqlParserCallback sqlParserCallback) throws SqlException {
        expressionParser.parseExpr(lexer, listener, sqlParserCallback, null);
    }

    ExecutionModel parse(GenericLexer lexer, SqlExecutionContext executionContext, SqlParserCallback sqlParserCallback) throws SqlException {
        final CharSequence tok = tok(lexer, "'create', 'rename' or 'select'");

        if (isExplainKeyword(tok)) {
            int format = parseExplainOptions(lexer, tok);
            ExecutionModel model = parseExplain(lexer, executionContext, sqlParserCallback);
            ExplainModel explainModel = explainModelPool.next();
            explainModel.setFormat(format);
            explainModel.setModel(model);
            return explainModel;
        }

        if (isSelectKeyword(tok)) {
            return parseSelect(lexer, sqlParserCallback, null);
        }

        if (isCreateKeyword(tok)) {
            return parseCreate(lexer, executionContext, sqlParserCallback);
        }

        if (isUpdateKeyword(tok)) {
            return parseUpdate(lexer, sqlParserCallback, null);
        }

        if (isRenameKeyword(tok)) {
            return parseRenameStatement(lexer);
        }

        if (isInsertKeyword(tok)) {
            return parseInsert(lexer, sqlParserCallback, null);
        }

        if (isCopyKeyword(tok)) {
            return parseCopy(lexer, sqlParserCallback);
        }

        if (isWithKeyword(tok)) {
            return parseWith(lexer, sqlParserCallback, null);
        }

        if (isFromKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "Did you mean 'select * from'?");
        }

        return parseSelect(lexer, sqlParserCallback, null);
    }

    QueryModel parseAsSubQuery(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
            boolean useTopLevelWithClauses,
            SqlParserCallback sqlParserCallback,
            LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        QueryModel model;
        this.subQueryMode = true;
        try {
            model = parseDml(lexer, withClauses, lexer.getPosition(), useTopLevelWithClauses, sqlParserCallback, decls);
        } finally {
            this.subQueryMode = false;
        }
        return model;
    }

    public interface ReplacingVisitor {
        ExpressionNode visit(ExpressionNode node) throws SqlException;
    }

    private static class RewriteDeclaredVariablesInExpressionVisitor implements ReplacingVisitor {
        public LowerCaseCharSequenceObjHashMap<ExpressionNode> decls;
        public CharSequence exprTargetVariableName;
        public boolean hasAtChar;

        @Override
        public ExpressionNode visit(ExpressionNode node) throws SqlException {
            if (node.token == null) {
                return node;
            }

            if ((hasAtChar = node.token.charAt(0) == '@') && exprTargetVariableName != null && (Chars.equalsIgnoreCase(node.token, exprTargetVariableName))) {
                return node;
            }

            if (node.token != null && node.type == ExpressionNode.LITERAL && decls.contains(node.token)) {
                return decls.get(node.token).rhs;
            } else if (hasAtChar) {
                throw SqlException.$(node.position, "tried to use undeclared variable `" + node.token + '`');
            }

            return node;
        }

        ReplacingVisitor of(
                @NotNull LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
                @Nullable CharSequence exprTargetVariableName
        ) {
            this.decls = decls;
            this.exprTargetVariableName = exprTargetVariableName;
            return this;
        }
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
        tableAliasStop.add("tolerance");
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

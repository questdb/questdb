/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.griffin.engine.functions.json.JsonExtractTypedFunctionFactory;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.engine.ops.CreateMatViewOperationBuilder;
import io.questdb.griffin.engine.ops.CreateMatViewOperationBuilderImpl;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilder;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilderImpl;
import io.questdb.griffin.engine.ops.CreateViewOperationBuilder;
import io.questdb.griffin.engine.ops.CreateViewOperationBuilderImpl;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.model.CompileViewModel;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExplainModel;
import io.questdb.griffin.model.ExportModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.HorizonJoinContext;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.InsertModel;
import io.questdb.griffin.model.PivotForColumn;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.RenameTableModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.model.WindowJoinContext;
import io.questdb.griffin.model.WithClauseModel;
import io.questdb.std.BufferWindowCharSequence;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.Decimals;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseAsciiCharSequenceHashSet;
import io.questdb.std.LowerCaseAsciiCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
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

import java.util.ArrayDeque;

import static io.questdb.cairo.SqlWalMode.*;
import static io.questdb.griffin.SqlKeywords.*;
import static io.questdb.griffin.SqlOptimiser.hasGroupByFunc;
import static io.questdb.griffin.engine.ops.CreateMatViewOperation.*;
import static io.questdb.std.GenericLexer.assertNoDotsAndSlashes;
import static io.questdb.std.GenericLexer.unquote;

public class SqlParser {
    public static final int MAX_ORDER_BY_COLUMNS = 1560;
    public static final ExpressionNode ZERO_OFFSET = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, "'00:00'", 0, 0);
    private static final ExpressionNode ONE = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, "1", 0, 0);
    private static final LowerCaseAsciiCharSequenceHashSet columnAliasStop = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet groupByStopSet = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceIntHashMap joinStartSet = new LowerCaseAsciiCharSequenceIntHashMap();
    private static final LowerCaseAsciiCharSequenceHashSet pivotForStop = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet setOperations = new LowerCaseAsciiCharSequenceHashSet();
    private static final LowerCaseAsciiCharSequenceHashSet tableAliasStop = new LowerCaseAsciiCharSequenceHashSet();
    private static final IntList tableNamePositions = new IntList();
    private static final LowerCaseCharSequenceHashSet tableNames = new LowerCaseCharSequenceHashSet();
    private final IntList accumulatedColumnPositions = new IntList();
    private final ObjList<QueryColumn> accumulatedColumns = new ObjList<>();
    private final LowerCaseCharSequenceHashSet aliasMap = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceIntHashMap aliasSequenceMap = new LowerCaseCharSequenceIntHashMap();
    private final CairoEngine cairoEngine;
    private final CharacterStore characterStore;
    private final CharSequence column;
    private final ObjectPool<CompileViewModel> compileViewModelPool;
    private final CairoConfiguration configuration;
    private final ObjectPool<ExportModel> copyModelPool;
    private final CreateMatViewOperationBuilderImpl createMatViewOperationBuilder = new CreateMatViewOperationBuilderImpl();
    private final ObjectPool<CreateTableColumnModel> createTableColumnModelPool;
    private final CreateTableOperationBuilderImpl createTableOperationBuilder = createMatViewOperationBuilder.getCreateTableOperationBuilder();
    private final CreateViewOperationBuilderImpl createViewOperationBuilder = new CreateViewOperationBuilderImpl();
    private final ObjectPool<ExplainModel> explainModelPool;
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final ExpressionParser expressionParser;
    private final ExpressionTreeBuilder expressionTreeBuilder;
    private final ObjectPool<InsertModel> insertModelPool;
    private final LowerCaseCharSequenceHashSet pivotAliasMap = new LowerCaseCharSequenceHashSet();
    private final ObjectPool<PivotForColumn> pivotQueryColumnPool;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    // Map of view definitions encountered during query compilation.
    // Using a map ensures consistent view definitions even if views are modified concurrently.
    private final LowerCaseCharSequenceObjHashMap<ViewDefinition> recordedViews = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjectPool<RenameTableModel> renameTableModelPool;
    private final PostOrderTreeTraversalAlgo.Visitor rewriteConcatRef = this::rewriteConcat;
    private final PostOrderTreeTraversalAlgo.Visitor rewriteCountRef = this::rewriteCount;
    private final RewriteDeclaredVariablesInExpressionVisitor rewriteDeclaredVariablesInExpressionVisitor = new RewriteDeclaredVariablesInExpressionVisitor();
    private final PostOrderTreeTraversalAlgo.Visitor rewriteJsonExtractCastRef = this::rewriteJsonExtractCast;
    private final PostOrderTreeTraversalAlgo.Visitor rewritePgCastRef = this::rewritePgCast;
    private final PostOrderTreeTraversalAlgo.Visitor rewritePgNumericRef = this::rewritePgNumeric;
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final CharSequenceHashSet tempCharSequenceSet = new CharSequenceHashSet();
    private final ObjList<ExpressionNode> tempExprNodes = new ObjList<>();
    private final PostOrderTreeTraversalAlgo.Visitor rewriteCaseRef = this::rewriteCase;
    private final LowerCaseCharSequenceObjHashMap<WithClauseModel> topLevelWithModel = new LowerCaseCharSequenceObjHashMap<>();
    private final PostOrderTreeTraversalAlgo traversalAlgo;
    private final ObjectPool<GenericLexer> viewLexers;
    private final SqlParserCallback viewSqlParserCallback = new SqlParserCallback() {
    };
    // Track views currently being compiled to detect cycles during query parsing
    private final LowerCaseCharSequenceHashSet viewsBeingCompiled = new LowerCaseCharSequenceHashSet();
    private final ObjectPool<WindowExpression> windowExpressionPool;
    private final ObjectPool<WithClauseModel> withClauseModelPool;
    private boolean copyMode = false;
    private boolean createTableMode = false;
    private boolean createViewMode = false;
    private int digit;
    private boolean pivotMode = false;
    private boolean subQueryMode = false;

    SqlParser(
            CairoEngine cairoEngine,
            CairoConfiguration configuration,
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<WindowExpression> windowExpressionPool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            PostOrderTreeTraversalAlgo traversalAlgo
    ) {
        this.cairoEngine = cairoEngine;
        this.configuration = cairoEngine.getConfiguration();
        this.expressionNodePool = expressionNodePool;
        this.queryModelPool = queryModelPool;
        this.queryColumnPool = queryColumnPool;
        this.windowExpressionPool = windowExpressionPool;
        this.expressionTreeBuilder = new ExpressionTreeBuilder();
        this.createTableColumnModelPool = new ObjectPool<>(CreateTableColumnModel.FACTORY, configuration.getCreateTableColumnModelPoolCapacity());
        this.renameTableModelPool = new ObjectPool<>(RenameTableModel.FACTORY, configuration.getRenameTableModelPoolCapacity());
        this.withClauseModelPool = new ObjectPool<>(WithClauseModel.FACTORY, configuration.getWithClauseModelPoolCapacity());
        this.insertModelPool = new ObjectPool<>(InsertModel.FACTORY, configuration.getInsertModelPoolCapacity());
        this.compileViewModelPool = new ObjectPool<>(CompileViewModel.FACTORY, configuration.getCompileViewModelPoolCapacity());
        this.copyModelPool = new ObjectPool<>(ExportModel.FACTORY, configuration.getCopyPoolCapacity());
        this.explainModelPool = new ObjectPool<>(ExplainModel.FACTORY, configuration.getExplainPoolCapacity());
        this.pivotQueryColumnPool = new ObjectPool<>(PivotForColumn.FACTORY, configuration.getPivotColumnPoolCapacity());
        this.traversalAlgo = traversalAlgo;
        this.characterStore = characterStore;
        this.viewLexers = new ObjectPool<>(this::createLexer, configuration.getViewLexerPoolCapacity());
        boolean tempCairoSqlLegacyOperatorPrecedence = configuration.getCairoSqlLegacyOperatorPrecedence();
        if (tempCairoSqlLegacyOperatorPrecedence) {
            this.expressionParser = new ExpressionParser(
                    OperatorExpression.getLegacyRegistry(),
                    OperatorExpression.getRegistry(),
                    expressionNodePool,
                    this,
                    characterStore,
                    windowExpressionPool
            );
        } else {
            this.expressionParser = new ExpressionParser(
                    OperatorExpression.getRegistry(),
                    null,
                    expressionNodePool,
                    this,
                    characterStore,
                    windowExpressionPool
            );
        }
        this.digit = 1;
        this.column = "column";
    }

    public static boolean isFullSampleByPeriod(ExpressionNode n) {
        return n != null && (n.type == ExpressionNode.CONSTANT || (n.type == ExpressionNode.LITERAL && isValidSampleByPeriodLetter(n.token)));
    }

    /**
     * Parses a DECIMAL[(precision[, scale])] type from the lexer.
     * The user may specify the precision and scale of the underlying DECIMAL type, if not provided, we use a default
     * precision of 18 and a scale of 3 (or 0 if precision &lt; 8) so that the underlying type will be a DECIMAL64.
     *
     * @return the concrete DECIMAL type with proper precision/scale set.
     */
    public static int parseDecimalColumnType(GenericLexer lexer) throws SqlException {
        int previousTokenPosition = lexer.lastTokenPosition();

        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || tok.charAt(0) != '(') {
            lexer.unparseLast();
            return ColumnType.DECIMAL_DEFAULT_TYPE;
        }

        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || tok.charAt(0) == ')') {
            throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. The precision is missing");
        }
        int precision = DecimalUtil.parsePrecision(lexer.lastTokenPosition(), tok, 0, tok.length());
        int scale = precision < 8 ? 0 : 3;

        tok = SqlUtil.fetchNext(lexer);

        // The user may provide a scale value
        if (tok != null && tok.charAt(0) == ',') {
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || tok.charAt(0) == ')') {
                throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. The scale is missing");
            }
            scale = DecimalUtil.parseScale(lexer.lastTokenPosition(), tok, 0, tok.length());
            tok = SqlUtil.fetchNext(lexer);
        }

        if (tok == null || tok.charAt(0) != ')') {
            throw SqlException.$(lexer.lastTokenPosition(), "Invalid decimal type. Missing ')'");
        }

        if (precision <= 0) {
            throw SqlException.position(previousTokenPosition)
                    .put("Invalid decimal type. The precision (")
                    .put(precision)
                    .put(") must be greater than zero");
        }
        if (precision > Decimals.MAX_PRECISION) {
            throw SqlException.position(previousTokenPosition)
                    .put("Invalid decimal type. The precision (")
                    .put(precision)
                    .put(") must be less than ")
                    .put(Decimals.MAX_PRECISION);
        }
        if (scale < 0) {
            throw SqlException.position(previousTokenPosition)
                    .put("Invalid decimal type. The scale (")
                    .put(scale)
                    .put(") must be greater than or equal to zero");
        }
        if (scale > precision) {
            throw SqlException.position(previousTokenPosition)
                    .put("Invalid decimal type. The precision (")
                    .put(precision)
                    .put(") must be greater than or equal to the scale (")
                    .put(scale)
                    .put(")");
        }

        return ColumnType.getDecimalType(precision, scale);
    }

    /**
     * Parses a GEOHASH(precision) type from the lexer.
     * The precision is specified as a number followed by 'b' (bits) or 'c' (chars),
     * e.g. GEOHASH(5c) or GEOHASH(30b).
     *
     * @return the concrete GEOHASH type with the specified precision.
     */
    public static int parseGeoHashColumnType(GenericLexer lexer) throws SqlException {
        CharSequence tok = SqlUtil.fetchNext(lexer);
        if (tok == null || tok.charAt(0) != '(') {
            throw SqlException.position(lexer.getPosition()).put("missing GEOHASH precision");
        }

        tok = SqlUtil.fetchNext(lexer);
        if (tok != null && tok.charAt(0) != ')') {
            int geoHashBits = GeoHashUtil.parseGeoHashBits(lexer.lastTokenPosition(), 0, tok);
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || tok.charAt(0) != ')') {
                if (tok != null) {
                    throw SqlException.position(lexer.lastTokenPosition())
                            .put("invalid GEOHASH type literal, expected ')'")
                            .put(" found='").put(tok.charAt(0)).put("'");
                }
                throw SqlException.position(lexer.getPosition())
                        .put("invalid GEOHASH type literal, expected ')'");
            }
            return ColumnType.getGeoHashTypeWithBits(geoHashBits);
        } else {
            throw SqlException.position(lexer.lastTokenPosition())
                    .put("missing GEOHASH precision");
        }
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

        // Traverse window context expressions (partition by, order by, frame bounds)
        if (node.windowExpression != null) {
            WindowExpression wc = node.windowExpression;
            ObjList<ExpressionNode> partitionBy = wc.getPartitionBy();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                partitionBy.set(i, recursiveReplace(partitionBy.get(i), visitor));
            }
            ObjList<ExpressionNode> orderBy = wc.getOrderBy();
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                orderBy.set(i, recursiveReplace(orderBy.get(i), visitor));
            }
            ExpressionNode loExpr = wc.getRowsLoExpr();
            if (loExpr != null) {
                wc.setRowsLoExpr(recursiveReplace(loExpr, visitor), wc.getRowsLoExprPos());
            }
            ExpressionNode hiExpr = wc.getRowsHiExpr();
            if (hiExpr != null) {
                wc.setRowsHiExpr(recursiveReplace(hiExpr, visitor), wc.getRowsHiExprPos());
            }
        }

        return visitor.visit(node);
    }

    public static void validateMatViewEveryUnit(char unit, int pos) throws SqlException {
        if (unit != 'M' && unit != 'y' && unit != 'w' && unit != 'd' && unit != 'h' && unit != 'm') {
            throw SqlException.position(pos).put("unsupported interval unit: ").put(unit)
                    .put(", supported units are 'm', 'h', 'd', 'w', 'y', 'M'");
        }
    }

    public static void validateMatViewPeriodDelay(int length, char lengthUnit, int delay, char delayUnit, int pos) throws SqlException {
        if (delay < 0) {
            throw SqlException.position(pos).put("delay cannot be negative");
        }

        final int lengthSeconds = matViewPeriodLengthSeconds(length, lengthUnit, pos);
        final int delaySeconds = matViewPeriodDelaySeconds(delay, delayUnit, pos);
        if (delaySeconds >= lengthSeconds) {
            throw SqlException.position(pos).put("delay cannot be equal to or greater than length");
        }
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

    private static boolean isJsonUnnestSupportedType(int type) {
        int tag = ColumnType.tagOf(type);
        return tag == ColumnType.BOOLEAN
                || tag == ColumnType.SHORT
                || tag == ColumnType.INT
                || tag == ColumnType.LONG
                || tag == ColumnType.DATE
                || tag == ColumnType.DOUBLE
                || tag == ColumnType.STRING
                || tag == ColumnType.VARCHAR
                || tag == ColumnType.TIMESTAMP;
    }

    private static boolean isValidSampleByPeriodLetter(CharSequence token) {
        if (token.length() != 1) return false;
        return switch (token.charAt(0)) {
            // nanos
            // micros
            // millis
            // seconds
            // minutes
            // hours
            // days
            // months
            case 'n', 'U', 'T', 's', 'm', 'h', 'd', 'M', 'y' -> true;
            default -> false;
        };
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

    private static CreateViewOperationBuilder parseCreateViewExt(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback,
            CharSequence tok,
            CreateViewOperationBuilder builder
    ) throws SqlException {
        CharSequence nextToken = (tok == null || Chars.equals(tok, ';')) ? null : tok;
        return sqlParserCallback.parseCreateViewExt(lexer, executionContext.getSecurityContext(), builder, nextToken);
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

    private void clearRecordedViews() {
        recordedViews.clear();
        viewsBeingCompiled.clear();
    }

    private void compileViewQuery(IQueryModel model, TableToken viewToken, int viewPosition) throws SqlException {
        final CharSequence viewName = viewToken.getTableName();

        // Detect cycle: if we're already compiling this view, it's a circular reference
        if (viewsBeingCompiled.contains(viewName)) {
            throw SqlException.$(viewPosition, "circular view reference detected: ").put(viewName);
        }

        // Check if we already have this view definition (ensures consistent snapshot during compilation)
        ViewDefinition viewDefinition = recordedViews.get(viewName);
        if (viewDefinition == null) {
            viewDefinition = cairoEngine.getViewGraph().getViewDefinition(viewToken);
            if (viewDefinition == null) {
                throw SqlException.viewDoesNotExist(viewPosition, viewName);
            }
            recordedViews.put(viewName, viewDefinition);
        }

        // Track that we're compiling this view
        viewsBeingCompiled.add(viewName);
        try {
            final IQueryModel viewModel = compileViewQuery(viewDefinition, viewPosition, model.getDecls());
            viewModel.copyDeclsFrom(model, false);
            model.setNestedModel(viewModel);
            model.setNestedModelIsSubQuery(true);
            if (model.getAlias() == null) {
                model.setAlias(literal(viewName, viewPosition));
            }
        } finally {
            viewsBeingCompiled.remove(viewName);
        }
    }

    private IQueryModel compileViewQuery(
            ViewDefinition viewDefinition,
            int viewPosition,
            LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        final GenericLexer viewLexer = viewLexers.next();
        viewLexer.of(viewDefinition.getViewSql());

        final IQueryModel viewModel = parseAsSubQuery(viewLexer, null, false, viewSqlParserCallback, decls, true);
        final ExpressionNode viewExpr = literal(viewDefinition.getViewToken().getTableName(), viewPosition);
        viewModel.setOriginatingViewNameExpr(viewExpr);
        viewModel.setViewNameExpr(viewExpr);
        return viewModel;
    }

    private CharSequence createColumnAlias(
            CharSequence token,
            int type,
            LowerCaseCharSequenceHashSet aliasToColumnMap
    ) {
        return SqlUtil.createColumnAlias(
                characterStore,
                unquote(token),
                Chars.indexOfLastUnquoted(token, '.'),
                aliasToColumnMap,
                aliasSequenceMap,
                type != ExpressionNode.LITERAL
        );
    }

    private CharSequence createConstColumnAlias(LowerCaseCharSequenceHashSet aliasToColumnMap) {
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

    private GenericLexer createLexer() {
        final GenericLexer lexer = new GenericLexer(configuration.getSqlLexerPoolCapacity());
        SqlCompilerImpl.configureLexer(lexer);
        return lexer;
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

    private boolean expectBoolean(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "'true' or 'false'");
        if (isTrueKeyword(tok)) {
            return true;
        } else if (isFalseKeyword(tok)) {
            return false;
        } else {
            throw errUnexpected(lexer, tok);
        }
    }

    private void expectBy(GenericLexer lexer) throws SqlException {
        if (isByKeyword(tok(lexer, "'by'"))) {
            return;
        }
        throw SqlException.$((lexer.lastTokenPosition()), "'by' expected");
    }

    private double expectDouble(GenericLexer lexer) throws SqlException {
        CharSequence tok = GenericLexer.unquote(expectStringLiteral(lexer).token);
        boolean negative;
        if (Chars.equals(tok, '-')) {
            negative = true;
            tok = tok(lexer, "number");
        } else {
            negative = false;
        }
        try {
            double result = Numbers.parseDouble(tok);
            return negative ? -result : result;
        } catch (NumericException e) {
            throw err(lexer, tok, "bad number");
        }
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

    /**
     * Parses an interval literal like "5s", "-2m", "+10h". Handles optional leading sign.
     */
    private ExpressionNode expectIntervalLiteral(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "interval");
        int pos = lexer.lastTokenPosition();

        // Check for optional sign
        if (Chars.equals(tok, '-') || Chars.equals(tok, '+')) {
            char sign = tok.charAt(0);
            CharSequence valueTok = tok(lexer, "interval value");
            // Combine sign with value: "-" + "2s" -> "-2s"
            CharacterStoreEntry entry = characterStore.newEntry();
            entry.put(sign).put(valueTok);
            return expressionNodePool.next().of(ExpressionNode.CONSTANT, entry.toImmutable(), 0, pos);
        }

        return expressionNodePool.next().of(ExpressionNode.CONSTANT, GenericLexer.immutableOf(tok), 0, pos);
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

    private void expectSample(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
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

    private ExpressionNode expectStringLiteral(GenericLexer lexer) throws SqlException {
        CharSequence tok = tok(lexer, "literal");
        int pos = lexer.lastTokenPosition();
        assertNameIsQuotedOrNotAKeyword(tok, pos);
        return nextLiteral(GenericLexer.immutableOf(tok), pos);
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

    /**
     * Checks that all columns in the combined GROUP BY list do not mix
     * qualified (t.a) and unqualified (a) references to the same column.
     * The grouping set machinery deduplicates by raw token text, so
     * mixed forms would create separate entries and incorrect sets.
     */
    private static void checkMixedQualification(ObjList<ExpressionNode> groupByColumns) throws SqlException {
        // O(N²) but N is the number of GROUP BY columns, bounded at ~31 by the
        // GROUPING_ID bitmask width. Not worth a HashSet for such small N.
        for (int i = 0, n = groupByColumns.size(); i < n; i++) {
            ExpressionNode a = groupByColumns.getQuick(i);
            for (int j = i + 1; j < n; j++) {
                ExpressionNode b = groupByColumns.getQuick(j);
                if (isSameColumnMixedQualification(a.token, b.token)) {
                    throw SqlException.$(b.position, "mixing qualified and unqualified references to the same column");
                }
            }
        }
    }

    /**
     * Validates that all nodes in the list are column references (LITERAL).
     * Rejects expressions in ROLLUP/CUBE/GROUPING SETS to avoid ambiguity
     * in set membership resolution.
     */
    private static void ensureColumnReferences(ObjList<ExpressionNode> columns, int kwPos) throws SqlException {
        for (int i = 0, n = columns.size(); i < n; i++) {
            ExpressionNode node = columns.getQuick(i);
            if (node.type != ExpressionNode.LITERAL) {
                throw SqlException.$(node.position, "column reference expected");
            }
        }
    }

    private static void guardAgainstLatestOnWithGroupingSets(QueryModel model, int kwPos) throws SqlException {
        if (model.getLatestBy().size() > 0) {
            throw SqlException.$(kwPos, "LATEST ON is not supported with GROUPING SETS, ROLLUP, or CUBE");
        }
    }

    /**
     * Returns true if two column tokens refer to the same column but one
     * is qualified (t.col) and the other is not (col).
     */
    private static boolean isSameColumnMixedQualification(CharSequence a, CharSequence b) {
        int aDot = Chars.indexOf(a, '.');
        int bDot = Chars.indexOf(b, '.');
        if ((aDot < 0) == (bDot < 0)) {
            // Both qualified or both unqualified - no mixed issue
            return false;
        }
        // One is qualified, the other is not. Compare the column name
        // part of the qualified token with the full unqualified token.
        if (aDot >= 0) {
            return Chars.equalsIgnoreCase(b, a, aDot + 1, a.length());
        } else {
            return Chars.equalsIgnoreCase(a, b, bDot + 1, b.length());
        }
    }

    private static void expandCube(QueryModel model, int startIdx, int cubeColumnCount, int position, int maxGroupingSets) throws SqlException {
        // 2^N subsets, from the full set (all bits set) down to empty set (0)
        int totalSets = 1 << cubeColumnCount;
        if (totalSets > maxGroupingSets) {
            throw SqlException.$(position, "CUBE produces too many grouping sets (")
                    .put(totalSets).put(", maximum ").put(maxGroupingSets).put(')');
        }
        for (int mask = totalSets - 1; mask >= 0; mask--) {
            IntList set = new IntList();
            for (int bit = 0; bit < cubeColumnCount; bit++) {
                if ((mask & (1 << (cubeColumnCount - 1 - bit))) != 0) {
                    set.add(startIdx + bit);
                }
            }
            model.addGroupingSet(set);
        }
    }

    /**
     * Expands CUBE with composite plain columns prepended to every set.
     * GROUP BY a, CUBE(b, c) -> sets: (a,b,c), (a,b), (a,c), (a).
     */
    private static void expandCubeComposite(QueryModel model, int plainCount, int rollupStart, int cubeColumnCount, int position, int maxGroupingSets) throws SqlException {
        int totalSets = 1 << cubeColumnCount;
        if (totalSets > maxGroupingSets) {
            throw SqlException.$(position, "CUBE produces too many grouping sets (")
                    .put(totalSets).put(", maximum ").put(maxGroupingSets).put(')');
        }
        for (int mask = totalSets - 1; mask >= 0; mask--) {
            IntList set = new IntList();
            // Add all plain columns first
            for (int i = 0; i < plainCount; i++) {
                set.add(i);
            }
            for (int bit = 0; bit < cubeColumnCount; bit++) {
                if ((mask & (1 << (cubeColumnCount - 1 - bit))) != 0) {
                    set.add(rollupStart + bit);
                }
            }
            model.addGroupingSet(set);
        }
    }

    /**
     * Expands ROLLUP into grouping sets. ROLLUP(a, b, c) generates:
     * (a,b,c), (a,b), (a), ().
     *
     * @param model             query model to add grouping sets to
     * @param startIdx          index of the first ROLLUP column in model.groupBy
     * @param rollupColumnCount number of ROLLUP columns
     */
    private static void expandRollup(QueryModel model, int startIdx, int rollupColumnCount, int position, int maxGroupingSets) throws SqlException {
        int totalSets = rollupColumnCount + 1;
        if (totalSets > maxGroupingSets) {
            throw SqlException.$(position, "ROLLUP produces too many grouping sets (")
                    .put(totalSets).put(", maximum ").put(maxGroupingSets).put(')');
        }
        // N+1 sets: from all columns down to empty set
        for (int setSize = rollupColumnCount; setSize >= 0; setSize--) {
            IntList set = new IntList();
            for (int j = 0; j < setSize; j++) {
                set.add(startIdx + j);
            }
            model.addGroupingSet(set);
        }
    }

    /**
     * Expands ROLLUP with composite plain columns prepended to every set.
     * GROUP BY a, ROLLUP(b, c) -> sets: (a,b,c), (a,b), (a).
     */
    private static void expandRollupComposite(QueryModel model, int plainCount, int rollupStart, int rollupColumnCount, int position, int maxGroupingSets) throws SqlException {
        int totalSets = rollupColumnCount + 1;
        if (totalSets > maxGroupingSets) {
            throw SqlException.$(position, "ROLLUP produces too many grouping sets (")
                    .put(totalSets).put(", maximum ").put(maxGroupingSets).put(')');
        }
        for (int setSize = rollupColumnCount; setSize >= 0; setSize--) {
            IntList set = new IntList();
            // Add all plain columns first
            for (int i = 0; i < plainCount; i++) {
                set.add(i);
            }
            for (int j = 0; j < setSize; j++) {
                set.add(rollupStart + j);
            }
            model.addGroupingSet(set);
        }
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
                    aliasSequenceMap,
                    configuration.getColumnAliasGeneratedMaxSize(),
                    qc.getAst().type != ExpressionNode.LITERAL
            );
        } else {
            if (qc.getAst().type == ExpressionNode.CONSTANT && Chars.indexOfLastUnquoted(token, '.') != -1) {
                alias = createConstColumnAlias(aliasMap);
            } else {
                CharSequence tokenAlias = qc.getAst().token;
                if (qc.isWindowExpression() && ((WindowExpression) qc).isIgnoreNulls()) {
                    CharacterStoreEntry cse = characterStore.newEntry();
                    cse.put(tokenAlias);
                    cse.put("_ignore_nulls");
                    tokenAlias = cse.toImmutable();
                }
                alias = createColumnAlias(tokenAlias, qc.getAst().type, aliasMap);
            }
        }
        qc.setAlias(alias, QueryColumn.SYNTHESIZED_ALIAS_POSITION);
        aliasMap.add(alias);
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

    private boolean isExcludePrevailing(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (isExcludeKeyword(tok)) {
            tok = tok(lexer, "'prevailing'");
            if (isPrevailingKeyword(tok)) {
                return true;
            }
            throw SqlException.$(lexer.lastTokenPosition(), "'prevailing' expected");
        }
        return false;
    }

    private boolean isFieldTerm(CharSequence tok) {
        return Chars.equals(tok, ')') || Chars.equals(tok, ',');
    }

    private boolean isIncludePrevailing(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (isIncludeKeyword(tok)) {
            tok = tok(lexer, "'prevailing'");
            if (isPrevailingKeyword(tok)) {
                return true;
            }
            throw SqlException.$(lexer.lastTokenPosition(), "'prevailing' expected");
        }
        return false;
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

    private boolean isUnexpectedRightParenInTopLevelSelect(CharSequence tok) {
        return Chars.equals(tok, ')') && !(subQueryMode || createTableMode || copyMode || createViewMode);
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
        if (tok == null || (subQueryMode && Chars.equals(tok, ')') && !pivotMode)) {
            return null;
        }
        return tok;
    }

    private IQueryModel parseAsSubQueryAndExpectClosingBrace(
            GenericLexer lexer,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
            boolean useTopLevelWithClauses,
            SqlParserCallback sqlParserCallback,
            LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        final IQueryModel model = parseAsSubQuery(lexer, withClauses, useTopLevelWithClauses, sqlParserCallback, decls, false);
        expectTok(lexer, ')');
        return model;
    }

    private ExecutionModel parseCompileView(GenericLexer lexer) throws SqlException {
        expectTok(lexer, "view");

        CharSequence tok = tok(lexer, "view name");
        final TableToken tt = cairoEngine.getTableTokenIfExists(unquote(tok));
        if (tt == null) {
            throw SqlException.viewDoesNotExist(lexer.lastTokenPosition(), tok);
        }
        if (!tt.isView()) {
            throw SqlException.$(lexer.lastTokenPosition(), "view expected, got table");
        }

        final CompileViewModel model = compileViewModelPool.next();
        model.setTableNameExpr(nextLiteral(unquote(tok), lexer.lastTokenPosition()));

        tok = optTok(lexer);
        if (tok != null && !Chars.equals(tok, ';')) {
            throw errUnexpected(lexer, tok);
        }

        final IQueryModel queryModel = queryModelPool.next();
        model.setQueryModel(queryModel);

        compileViewQuery(queryModel, tt, lexer.lastTokenPosition());
        return model;
    }

    private ExecutionModel parseCopy(GenericLexer lexer, SqlParserCallback sqlParserCallback) throws SqlException {
        @Nullable ExpressionNode target = null;
        @Nullable CharSequence selectText = null;
        CharSequence tok = tok(lexer, "copy source");
        int startOfSelect = 0;

        if (tok.length() == 1 && tok.charAt(0) == '(') {
            startOfSelect = lexer.getPosition();
            copyMode = true;
            try {
                parseDml(lexer, startOfSelect, sqlParserCallback);
                final int endOfSelect = lexer.getPosition() - 1;
                selectText = lexer.getContent().subSequence(startOfSelect, endOfSelect);
                expectTok(lexer, ')');
            } finally {
                copyMode = false;
            }
        } else {
            lexer.unparseLast();
            target = expectExpr(lexer, sqlParserCallback);
        }

        tok = tok(lexer, "'from' or 'to' or 'cancel'");

        ExportModel model = copyModelPool.next();
        if (isCancelKeyword(tok)) {
            model.setCancel(true);
            model.setTarget(target);

            tok = optTok(lexer);
            // no more tokens or ';' should indicate end of statement
            if (tok == null || Chars.equals(tok, ';')) {
                return model;
            }

            throw errUnexpected(lexer, tok);
        }

        if (isFromKeyword(tok) || isToKeyword(tok)) {
            tok = GenericLexer.immutableOf(tok);
            final ExpressionNode fileName = expectExpr(lexer, sqlParserCallback);
            if (fileName.token.length() < 3 && Chars.startsWith(fileName.token, '\'')) {
                throw SqlException.$(fileName.position, "file name expected");
            }

            model.setTarget(target);
            model.setSelectText(selectText, startOfSelect);
            model.setFileName(fileName);
        }

        if (isFromKeyword(tok)) {
            if (Chars.isBlank(configuration.getSqlCopyInputRoot())) {
                throw SqlException.$(lexer.lastTokenPosition(), "COPY is disabled ['cairo.sql.copy.root' is not set?]");
            }
            if (selectText != null) {
                throw SqlException.$(startOfSelect, "subqueries are not supported for `COPY-FROM`");
            }

            model.setType(ExportModel.COPY_TYPE_FROM);

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
                        if (partitionBy < 0) {
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

        if (isToKeyword(tok)) {
            // Disable COPY TO when export root is not configured
            if (Chars.isBlank(configuration.getSqlCopyExportRoot())) {
                throw SqlException.$(lexer.lastTokenPosition(), "COPY TO is disabled ['cairo.sql.copy.export.root' is not set?]");
            }

            tok = optTok(lexer);
            model.setType(ExportModel.COPY_TYPE_TO);
            if (tok == null || isSemicolon(tok)) {
                return model;
            }
            if (!isWithKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'with' expected");
            }
            tok = tok(lexer, "copy option");
            while (tok != null && !isSemicolon(tok)) {
                final int optionCode = ExportModel.getExportOption(tok);
                switch (optionCode) {
                    case ExportModel.COPY_OPTION_FORMAT:
                        // only support parquet for now
                        tok = tok(lexer, "'parquet'");
                        if (isParquetKeyword(tok)) {
                            model.setFormat(ExportModel.COPY_FORMAT_PARQUET);
                            model.setParquetDefaults(configuration);
                        } else {
                            throw SqlException.$(lexer.lastTokenPosition(), "unsupported format, only 'parquet' is supported");
                        }
                        break;
                    case ExportModel.COPY_OPTION_PARTITION_BY:
                        final ExpressionNode partitionByExpr = expectLiteral(lexer);
                        final int partitionBy = PartitionBy.fromString(partitionByExpr.token);
                        if (partitionBy < 0) {
                            throw SqlException.$(lexer.lastTokenPosition(), "invalid partition by option: ").put(partitionByExpr.token);
                        }
                        model.setPartitionBy(partitionBy);
                        break;
                    case ExportModel.COPY_OPTION_SIZE_LIMIT:
                        // todo: add this when table writer has appropriate support for it
                        throw SqlException.$(lexer.lastTokenPosition(), "size limit is not yet supported");
                    case ExportModel.COPY_OPTION_COMPRESSION_CODEC:
                        ExpressionNode codecExpr = expectLiteral(lexer);
                        int codec = ParquetCompression.getCompressionCodec(codecExpr.token);
                        if (codec < 0) {
                            SqlException e = SqlException.$(codecExpr.position, "invalid compression codec[").put(codecExpr.token).put("], expected one of: ");
                            ParquetCompression.addCodecNamesToException(e);
                            throw e;
                        }
                        model.setCompressionCodec(codec);
                        break;
                    case ExportModel.COPY_OPTION_COMPRESSION_LEVEL:
                        model.setCompressionLevel(expectInt(lexer), lexer.lastTokenPosition());
                        break;
                    case ExportModel.COPY_OPTION_ROW_GROUP_SIZE:
                        model.setRowGroupSize(expectInt(lexer));
                        break;
                    case ExportModel.COPY_OPTION_DATA_PAGE_SIZE:
                        model.setDataPageSize(expectInt(lexer));
                        break;
                    case ExportModel.COPY_OPTION_RAW_ARRAY_ENCODING:
                        model.setRawArrayEncoding(expectBoolean(lexer));
                        break;
                    case ExportModel.COPY_OPTION_STATISTICS_ENABLED:
                        model.setStatisticsEnabled(expectBoolean(lexer));
                        break;
                    case ExportModel.COPY_OPTION_PARQUET_VERSION:
                        int parquetVersion = expectInt(lexer);
                        if (parquetVersion != ExportModel.PARQUET_VERSION_V1 && parquetVersion != ExportModel.PARQUET_VERSION_V2) {
                            throw SqlException.$(lexer.lastTokenPosition(), "invalid parquet version: ").put(parquetVersion).put(", expected 1 or 2");
                        }
                        model.setParquetVersion(parquetVersion);
                        break;
                    case ExportModel.COPY_OPTION_BLOOM_FILTER_COLUMNS:
                        ExpressionNode bloomFilterColumnsExpr = expectStringLiteral(lexer);
                        model.setBloomFilterColumns(GenericLexer.unquote(bloomFilterColumnsExpr.token), Chars.isQuoted(bloomFilterColumnsExpr.token) ? bloomFilterColumnsExpr.position + 1 : bloomFilterColumnsExpr.position);
                        break;
                    case ExportModel.COPY_OPTION_BLOOM_FILTER_FPP:
                        double fpp = expectDouble(lexer);
                        if (!Double.isFinite(fpp) || fpp <= 0 || fpp >= 1) {
                            throw SqlException.$(lexer.lastTokenPosition(), "bloom_filter_fpp must be between 0 and 1 (exclusive)");
                        }
                        model.setBloomFilterFpp(fpp);
                        break;
                    case ExportModel.COPY_OPTION_UNKNOWN:
                        throw SqlException.$(lexer.lastTokenPosition(), "unrecognised option [option=")
                                .put(tok).put(']');
                }
                tok = optTok(lexer);
            }
            return model;
        }
        throw errUnexpected(lexer, tok);
    }

    private ExecutionModel parseCreate(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        CharSequence tok = tok(lexer, "'atomic' or 'table' or 'batch' or 'materialized' or 'view' or 'or replace'");
        if (isOrKeyword(tok)) {
            // we need to skip OR REPLACE, it is handled in an executor
            expectTok(lexer, "replace");
            tok = tok(lexer, "'view'");
        }
        if (isViewKeyword(tok)) {
            return parseCreateView(lexer, executionContext, sqlParserCallback);
        }
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
                expectTok(lexer, "(");
                tok = tok(lexer, "'length' or 'sample'");
                if (isLengthKeyword(tok)) {
                    // REFRESH ... PERIOD(LENGTH <interval> [TIME ZONE '<timezone>'] [DELAY <interval>])
                    tok = tok(lexer, "LENGTH interval");
                    final int length = CommonUtils.getStrideMultiple(tok, lexer.lastTokenPosition());
                    final char lengthUnit = CommonUtils.getStrideUnit(tok, lexer.lastTokenPosition());
                    validateMatViewPeriodLength(length, lengthUnit, lexer.lastTokenPosition());
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
                        validateMatViewPeriodDelay(length, lengthUnit, delay, delayUnit, lexer.lastTokenPosition());
                        tok = tok(lexer, "')'");
                    }

                    // Period timer start is at the boundary of the current period.
                    final long nowMicros = configuration.getMicrosecondClock().getTicks();
                    final long nowLocalMicros = tzRulesMicros != null ? nowMicros + tzRulesMicros.getOffset(nowMicros) : nowMicros;
                    final long startUs = periodSamplerMicros.round(nowLocalMicros);

                    mvOpBuilder.setTimer(tz, startUs, every, everyUnit);
                    mvOpBuilder.setPeriodLength(length, lengthUnit, delay, delayUnit);
                } else if (isSampleKeyword(tok)) {
                    // REFRESH ... PERIOD(SAMPLE BY INTERVAL)
                    expectTok(lexer, "by");
                    expectTok(lexer, "interval");
                    tok = tok(lexer, "')'");

                    mvOpBuilder.setTimer(null, 0, every, everyUnit);
                    // Set length to -1 to define the period later, once we parse the query.
                    mvOpBuilder.setPeriodLength(-1, (char) 0, 0, (char) 0);
                } else {
                    throw SqlException.position(lexer.lastTokenPosition()).put("'length' or 'sample' expected");
                }

                if (!Chars.equals(tok, ')')) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("')' expected");
                }
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
            final IQueryModel queryModel = parseDml(lexer, lexer.getPosition(), sqlParserCallback);
            final int endOfQuery = enclosedInParentheses ? lexer.getPosition() - 1 : lexer.getPosition();

            tableNames.clear();
            tableNamePositions.clear();
            SqlUtil.collectAllTableNames(queryModel, tableNames, tableNamePositions);

            // Find base table name if not set explicitly.
            if (baseTableName == null) {
                if (tableNames.size() < 1) {
                    throw SqlException.$(startOfQuery, "missing base table, materialized views have to be based on a table");
                }
                if (tableNames.size() > 1) {
                    throw SqlException.$(startOfQuery, "query references multiple tables (views are expanded to their underlying physical tables), use 'WITH BASE' to explicitly select the base table");
                }
                baseTableName = Chars.toString(tableNames.getAny());
                baseTableNamePos = tableNamePositions.getQuick(0);
            }

            mvOpBuilder.setBaseTableNamePosition(baseTableNamePos);
            final String baseTableNameStr = Chars.toString(baseTableName);
            mvOpBuilder.setBaseTableName(baseTableNameStr);

            // Basic validation - check all nested models that read from the base table for window functions, unions, FROM-TO, or FILL.
            if (!tableNames.contains(baseTableNameStr)) {
                final TableToken baseTableToken = cairoEngine.getTableTokenIfExists(baseTableNameStr);
                if (baseTableToken != null && baseTableToken.isView()) {
                    throw SqlException.position(baseTableNamePos)
                            .put("base table should be a physical table, cannot be a view: ").put(baseTableName);
                }
                throw SqlException.position(baseTableNamePos)
                        .put("base table is not referenced in materialized view query: ").put(baseTableName);
            }
            validateMatViewQuery(queryModel, baseTableNameStr);

            final IQueryModel nestedModel = queryModel.getNestedModel();
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

        tok = sqlParserCallback.parseTtlSettings(lexer, tok, partitionBy, tableOpBuilder, true);

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
            // timestamp may be inferred from select query.
            if (builder.getSelectText() == null && builder.getTimestampExpr() == null) {
                throw SqlException.$(partitionByExpr.position, "partitioning is possible only on tables with designated timestamps");
            }
            final int partitionBy = PartitionBy.fromString(partitionByExpr.token);
            if (partitionBy == -1) {
                throw SqlException.$(partitionByExpr.position, "'NONE', 'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
            }
            builder.setPartitionByExpr(partitionByExpr);
            tok = optTok(lexer);

            tok = sqlParserCallback.parseTtlSettings(lexer, tok, partitionBy, builder, false);

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
            while ((expr = expr(lexer, (IQueryModel) null, sqlParserCallback)) != null) {
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
        IQueryModel selectModel;
        createTableMode = true;
        try {
            selectModel = parseDml(lexer, startOfSelect, sqlParserCallback);
        } finally {
            createTableMode = false;
        }
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

            if (tok == null) {
                // check for dodgy array syntax
                CharSequence tempTok = optTok(lexer);
                if (tempTok != null && Chars.equals(tempTok, ']')) {
                    throw SqlException.position(columnPosition).put(columnName).put(" has an unmatched `]` - were you trying to define an array?");
                } else {
                    lexer.unparseLast();
                }
                tok = tok(lexer, "',' or ')'");
            }

            if (isParquetKeyword(tok)) {
                tok = parseCreateTableParquetProperties(lexer, model);
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
        CharSequence tok = tok(lexer, "')', 'index' or 'parquet'");

        if (isFieldTerm(tok) || isParquetKeyword(tok)) {
            model.setIndexed(false, -1, configuration.getIndexValueBlockSize());
            return tok;
        }

        expectTok(lexer, tok, "index");
        int indexColumnPosition = lexer.lastTokenPosition();

        if (isFieldTerm(tok = tok(lexer, ") | , expected")) || isParquetKeyword(tok)) {
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

    private CharSequence parseCreateTableParquetProperties(GenericLexer lexer, CreateTableColumnModel model) throws SqlException {
        model.setParquetEncodingConfig(SqlUtil.parseParquetConfig(lexer, model.getColumnType()));
        return tok(lexer, "',' or ')'");
    }

    private ExpressionNode parseCreateTablePartition(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (tok != null && isPartitionKeyword(tok)) {
            expectTok(lexer, "by");
            return expectLiteral(lexer);
        }
        return null;
    }

    private ExecutionModel parseCreateView(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        final CreateViewOperationBuilderImpl vOpBuilder = createViewOperationBuilder;
        final CreateTableOperationBuilderImpl tableOpBuilder = vOpBuilder.getCreateTableOperationBuilder();
        vOpBuilder.clear(); // clears tableOpBuilder too
        tableOpBuilder.setDefaultSymbolCapacity(configuration.getDefaultSymbolCapacity());
        tableOpBuilder.setMaxUncommittedRows(configuration.getMaxUncommittedRows());
        tableOpBuilder.setWalEnabled(true); // view is always WAL

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

        tok = tok(lexer, "'as'");
        if (!isAsKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put("'as' expected");
        }

        int startOfQuery = lexer.getPosition();
        tok = tok(lexer, "'(' or 'with' or 'select'");
        boolean enclosedInParentheses = Chars.equals(tok, '(');
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
        final IQueryModel queryModel;
        try {
            createViewMode = true;
            queryModel = parseDml(lexer, lexer.getPosition(), sqlParserCallback);
        } finally {
            createViewMode = false;
        }
        final int endOfQuery = enclosedInParentheses ? lexer.getPosition() - 1 : lexer.getPosition();

        final String viewSql = Chars.toString(lexer.getContent(), startOfQuery, endOfQuery);
        tableOpBuilder.setSelectText(viewSql, startOfQuery);
        tableOpBuilder.setSelectModel(queryModel); // transient model, for toSink() purposes only

        SqlUtil.collectTableAndColumnReferences(cairoEngine, queryModel, vOpBuilder.getDependencies());

        if (enclosedInParentheses) {
            expectTok(lexer, ')');
        } else {
            // We expect nothing more when there are no parentheses.
            tok = optTok(lexer);
            if (tok != null && !Chars.equals(tok, ';')) {
                throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
            }
            return vOpBuilder;
        }

        tok = optTok(lexer);
        return parseCreateViewExt(lexer, executionContext, sqlParserCallback, tok, vOpBuilder);
    }

    private void parseDeclare(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
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

            if (isDeclareKeyword(tok)) {
                throw errUnexpected(lexer, tok, "Multiple DECLARE statements are not allowed. Use single DECLARE block: DECLARE @a := 1, @b := 1, @c := 1");
            }

            boolean isOverridable = false;
            if (isOverridableKeyword(tok)) {
                isOverridable = true;
                pos = lexer.getPosition();
                tok = optTok(lexer);
                if (tok == null || tok.charAt(0) != '@') {
                    throw SqlException.$(pos, "variable name expected after OVERRIDABLE");
                }
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
            if (isOverridable) {
                model.getOverridableDecls().add(tok);
            }
        }
    }

    private IQueryModel parseDml(
            GenericLexer lexer,
            int modelPosition,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        return parseDml(lexer, null, modelPosition, true, sqlParserCallback, null, false);
    }

    private IQueryModel parseDml(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
            int modelPosition,
            boolean useTopLevelWithClauses,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            boolean overrideDeclare
    ) throws SqlException {
        IQueryModel model = null;
        IQueryModel prevModel = null;

        while (true) {
            LowerCaseCharSequenceObjHashMap<WithClauseModel> parentWithClauses = prevModel != null ? prevModel.getWithClauses() : withClauses;
            LowerCaseCharSequenceObjHashMap<WithClauseModel> topWithClauses = useTopLevelWithClauses && model == null ? topLevelWithModel : null;
            // Propagate DECLARE variables from previous UNION branch, similar to how WITH clauses are propagated
            LowerCaseCharSequenceObjHashMap<ExpressionNode> parentDecls = prevModel != null ? prevModel.getDecls() : decls;

            IQueryModel unionModel = parseDml0(lexer, parentWithClauses, topWithClauses, modelPosition, sqlParserCallback, parentDecls, overrideDeclare);
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
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_UNION_ALL);
                    modelPosition = lexer.getPosition();
                } else {
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_UNION);
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
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_EXCEPT_ALL);
                    modelPosition = lexer.getPosition();
                } else {
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_EXCEPT);
                    lexer.unparseLast();
                    modelPosition = lexer.lastTokenPosition();
                }
            }

            if (isIntersectKeyword(tok)) {
                tok = tok(lexer, "all or select");
                if (isAllKeyword(tok)) {
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_INTERSECT_ALL);
                    modelPosition = lexer.getPosition();
                } else {
                    prevModel.setSetOperationType(IQueryModel.SET_OPERATION_INTERSECT);
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
    private IQueryModel parseDml0(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> parentWithClauses,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> topWithClauses,
            int modelPosition,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            boolean overrideDeclare
    ) throws SqlException {
        CharSequence tok;
        IQueryModel model = queryModelPool.next();
        model.setModelPosition(modelPosition);

        if (parentWithClauses != null) {
            model.getWithClauses().putAll(parentWithClauses);
        }

        tok = tok(lexer, "'select', 'with', 'declare' or table name expected");

        // [declare]
        if (isDeclareKeyword(tok)) {
            parseDeclare(lexer, model, sqlParserCallback);
            tok = tok(lexer, "'select', 'with', or table name expected");
        }

        // Merge external declares with the query's own declares.
        // When there is a naming conflict, overrideDeclare controls the behavior:
        //   - true: external declares override the query's own declares
        //   - false: query's own declares take precedence over the external ones
        // Currently set to true at view boundaries only, allowing callers to parameterize views.
        model.copyDeclsFrom(decls, overrideDeclare);

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
                IQueryModel nestedModel = queryModelPool.next();
                nestedModel.setModelPosition(modelPosition);
                ExpressionNode tableNameExpr = expressionNodePool.next().of(ExpressionNode.FUNCTION, "long_sequence", 0, lexer.lastTokenPosition());
                tableNameExpr.paramCount = 1;
                tableNameExpr.rhs = ONE;
                nestedModel.setTableNameExpr(tableNameExpr);
                model.setSelectModelType(IQueryModel.SELECT_MODEL_VIRTUAL);
                model.setNestedModel(nestedModel);
                lexer.unparseLast();
                return model;
            }
        } else if (isShowKeyword(tok)) {
            model.setSelectModelType(IQueryModel.SELECT_MODEL_SHOW);
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
                // show create materialized view mv
                // show create view v
                if (isTablesKeyword(tok)) {
                    showKind = IQueryModel.SHOW_TABLES;
                } else if (isColumnsKeyword(tok)) {
                    parseFromTable(lexer, model);
                    showKind = IQueryModel.SHOW_COLUMNS;
                } else if (isPartitionsKeyword(tok)) {
                    parseFromTable(lexer, model);
                    showKind = IQueryModel.SHOW_PARTITIONS;
                } else if (isTransactionKeyword(tok)) {
                    showKind = IQueryModel.SHOW_TRANSACTION;
                    validateShowTransactions(lexer);
                } else if (isTransactionIsolation(tok)) {
                    showKind = IQueryModel.SHOW_TRANSACTION_ISOLATION_LEVEL;
                } else if (isDefaultTransactionReadOnly(tok)) {
                    showKind = IQueryModel.SHOW_DEFAULT_TRANSACTION_READ_ONLY;
                } else if (isMaxIdentifierLength(tok)) {
                    showKind = IQueryModel.SHOW_MAX_IDENTIFIER_LENGTH;
                } else if (isStandardConformingStrings(tok)) {
                    showKind = IQueryModel.SHOW_STANDARD_CONFORMING_STRINGS;
                } else if (isSearchPath(tok)) {
                    showKind = IQueryModel.SHOW_SEARCH_PATH;
                } else if (isDateStyleKeyword(tok)) {
                    showKind = IQueryModel.SHOW_DATE_STYLE;
                } else if (isTimeKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok != null && isZoneKeyword(tok)) {
                        showKind = IQueryModel.SHOW_TIME_ZONE;
                    }
                } else if (isParametersKeyword(tok)) {
                    showKind = IQueryModel.SHOW_PARAMETERS;
                } else if (isServerVersionKeyword(tok)) {
                    showKind = IQueryModel.SHOW_SERVER_VERSION;
                } else if (isServerVersionNumKeyword(tok)) {
                    showKind = IQueryModel.SHOW_SERVER_VERSION_NUM;
                } else if (isCreateKeyword(tok)) {
                    tok = SqlUtil.fetchNext(lexer);
                    if (tok != null && isTableKeyword(tok)) {
                        parseTableName(lexer, model);
                        showKind = IQueryModel.SHOW_CREATE_TABLE;
                    } else if (tok != null && isMaterializedKeyword(tok)) {
                        expectTok(lexer, "view");
                        parseTableName(lexer, model);
                        showKind = IQueryModel.SHOW_CREATE_MAT_VIEW;
                    } else if (tok != null && isViewKeyword(tok)) {
                        parseTableName(lexer, model);
                        showKind = IQueryModel.SHOW_CREATE_VIEW;
                    } else {
                        throw SqlException.position(lexer.lastTokenPosition()).put("expected 'TABLE' or 'VIEW' or 'MATERIALIZED VIEW'");
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

        if (model.getSelectModelType() != IQueryModel.SELECT_MODEL_SHOW) {
            IQueryModel nestedModel = queryModelPool.next();
            nestedModel.setModelPosition(modelPosition);

            nestedModel = parseFromClause(lexer, nestedModel, model, sqlParserCallback);
            if (nestedModel.getLimitHi() != null || nestedModel.getLimitLo() != null) {
                model.setLimit(nestedModel.getLimitLo(), nestedModel.getLimitHi());
                nestedModel.setLimit(null, null);
            }
            model.setSelectModelType(IQueryModel.SELECT_MODEL_CHOOSE);
            model.setNestedModel(nestedModel);
            final ExpressionNode n = nestedModel.getAlias();
            if (n != null) {
                model.setAlias(n);
            }
        }
        return model;
    }

    private IQueryModel parseDmlUpdate(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        // Update IQueryModel structure is
        // IQueryModel with SET column expressions (updateQueryModel)
        // |-- nested IQueryModel of select-virtual or select-choose of data selected for update (fromModel)
        //     |-- nested IQueryModel with selected data (nestedModel)
        //         |-- join QueryModels to represent FROM clause
        CharSequence tok;
        final int modelPosition = lexer.getPosition();

        IQueryModel updateQueryModel = queryModelPool.next();
        updateQueryModel.setModelType(ExecutionModel.UPDATE);
        updateQueryModel.setModelPosition(modelPosition);
        IQueryModel fromModel = queryModelPool.next();
        fromModel.setModelPosition(modelPosition);
        updateQueryModel.setIsUpdate(true);
        fromModel.setIsUpdate(true);
        tok = tok(lexer, "UPDATE, WITH or table name expected");

        // [update]
        if (isUpdateKeyword(tok)) {
            // parse SET statements into updateQueryModel and rhs of SETs into fromModel to select
            parseUpdateClause(lexer, updateQueryModel, fromModel, sqlParserCallback);

            // create nestedModel IQueryModel to source rowids for the update
            IQueryModel nestedModel = queryModelPool.next();
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
                    nestedModel.addJoinModel(parseJoin(lexer, fromModel, tok, joinType, topLevelWithModel, sqlParserCallback, decls));
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

        if (isDropKeyword(tok) || isAlterKeyword(tok) || isRefreshKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put(
                    "'create', 'format', 'insert', 'update', 'select' or 'with'"
            ).put(" expected");
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

    private IQueryModel parseFromClause(GenericLexer lexer, IQueryModel model, IQueryModel masterModel, SqlParserCallback sqlParserCallback) throws SqlException {
        CharSequence tok = expectTableNameOrSubQuery(lexer);

        // copy decls down
        model.copyDeclsFrom(masterModel, false);

        // standalone UNNEST: FROM UNNEST(...)
        if (isUnnestKeyword(tok)) {
            // synthesize long_sequence(1) as the base model
            ExpressionNode longSeq = expressionNodePool.next().of(ExpressionNode.FUNCTION, "long_sequence", 0, 0);
            longSeq.paramCount = 1;
            longSeq.rhs = ONE;
            model.setTableNameExpr(longSeq);

            IQueryModel unnestModel = parseUnnest(lexer, model, model.getDecls(), sqlParserCallback);
            unnestModel.setStandaloneUnnest(true);
            model.addJoinModel(unnestModel);

            tok = optTok(lexer);
        } else {
            IQueryModel proposedNested = null;
            ExpressionNode variableExpr;

            // check for variable as subquery
            if (tok.charAt(0) == '@'
                    && (variableExpr = model.getDecls().get(tok)) != null
                    && variableExpr.rhs != null
                    && variableExpr.rhs.queryModel != null) {
                proposedNested = variableExpr.rhs.queryModel;
            }

            final TableToken tt = cairoEngine.getTableTokenIfExists(unquote(tok));
            if (tt != null && tt.isView()) {
                compileViewQuery(model, tt, lexer.lastTokenPosition());
                tok = setModelAliasAndTimestamp(lexer, model);
                // expect "(" in case of sub-query
            } else if (Chars.equals(tok, '(') || proposedNested != null) {
                if (proposedNested == null) {
                    proposedNested = parseAsSubQueryAndExpectClosingBrace(lexer, masterModel.getWithClauses(), true, sqlParserCallback, model.getDecls());
                }

                tok = optTok(lexer);

                // do not collapse aliased sub-queries or those that have timestamp()
                // select * from (table) x
                if (tok == null || (tableAliasStop.contains(tok) && !isTimestampKeyword(tok))) {
                    final IQueryModel target = proposedNested.getNestedModel();
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
                                    && target.getPivotForColumns().size() == 0
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
        }

        // expect multiple [[inner | outer | cross] join]
        int joinType;
        boolean hasWindowJoin = false;
        boolean hasHorizonJoin = false;
        while (tok != null && (joinType = joinStartSet.get(tok)) != -1) {
            // Check if this is a WINDOW clause (named window definitions) rather than WINDOW JOIN
            // WINDOW clause pattern: WINDOW name AS (...)
            // WINDOW JOIN pattern: WINDOW JOIN table ON ... or WINDOW table ON ...
            if (isWindowKeyword(tok)) {
                // Save lexer state before lookahead
                int windowLastPos = lexer.lastTokenPosition();
                CharSequence windowTok = tok;

                // Lookahead: read two tokens after WINDOW to distinguish
                // WINDOW clause (WINDOW name AS ...) from WINDOW JOIN (WINDOW JOIN table ON ...).
                // We always check both tokens because join keywords like "join", "cross",
                // "left" etc. could theoretically be quoted window names.
                CharSequence nextTok = SqlUtil.fetchNext(lexer);
                boolean isWindowClause = false;
                if (nextTok != null) {
                    if (isAsKeyword(nextTok)) {
                        // WINDOW AS (...) - missing window name
                        lexer.backTo(windowLastPos, windowTok);
                        tok = optTok(lexer);
                        break;
                    }
                    CharSequence afterName = SqlUtil.fetchNext(lexer);
                    if (afterName != null && isAsKeyword(afterName)) {
                        isWindowClause = true;
                    }
                }

                // Restore lexer to start of "window" token so it can be re-read
                lexer.backTo(windowLastPos, windowTok);

                if (isWindowClause) {
                    // Break out of join loop - WINDOW clause will be parsed after the loop
                    // Re-read "window" so tok is valid for WINDOW clause parsing
                    tok = optTok(lexer);
                    break;
                }
                // WINDOW JOIN - re-read "window" so tok is valid for parseJoin
                tok = optTok(lexer);
            }
            if (hasWindowJoin && joinType != IQueryModel.JOIN_WINDOW) {
                throw SqlException.$((lexer.lastTokenPosition()), "no other join types allowed after window join");
            }
            if (hasHorizonJoin && joinType != IQueryModel.JOIN_HORIZON) {
                throw SqlException.$((lexer.lastTokenPosition()), "only horizon joins can follow a horizon join");
            }
            if (joinType == IQueryModel.JOIN_HORIZON && !hasHorizonJoin && model.getJoinModels().size() > 1) {
                throw SqlException.$((lexer.lastTokenPosition()), "horizon join cannot be combined with other joins");
            }
            hasWindowJoin = joinType == IQueryModel.JOIN_WINDOW;
            hasHorizonJoin = joinType == IQueryModel.JOIN_HORIZON;
            model.addJoinModel(parseJoin(lexer, model, tok, joinType, masterModel.getWithClauses(), sqlParserCallback, model.getDecls()));
            tok = optTok(lexer);
        }

        // expect [where]

        if (tok != null && isWhereKeyword(tok)) {
            if (model.getLatestByType() == IQueryModel.LATEST_BY_NEW) {
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
            if (model.getLatestByType() == IQueryModel.LATEST_BY_DEPRECATED) {
                throw SqlException.$((lexer.lastTokenPosition()), "mix of new and deprecated 'latest by' syntax");
            }
            expectTok(lexer, "on");
            parseLatestByNew(lexer, model);
            tok = optTok(lexer);
        }

        // expect [pivot]
        // PIVOT operates on the result of a full subquery.
        // Syntax: SELECT ... FROM <subquery> WHERE <condition> PIVOT (agg FOR col IN (...))
        // The pivot transformation wraps the current model as a nested subquery.
        boolean hasPivot = false;
        if (tok != null && isPivotKeyword(tok)) {
            try {
                pivotMode = true;
                IQueryModel pivotModel = queryModelPool.next();
                pivotModel.setModelPosition(lexer.lastTokenPosition());
                pivotModel.setNestedModel(model);
                tok = parsePivot(lexer, pivotModel, sqlParserCallback);
                hasPivot = true;
                model = pivotModel;
            } finally {
                pivotMode = false;
            }
        }

        // expect [sample by]
        if (tok != null && isSampleKeyword(tok)) {
            if (hasPivot) {
                IQueryModel parentModel = queryModelPool.next();
                parentModel.setNestedModel(model);
                model = parentModel;
            }
            expectBy(lexer);
            expectSample(lexer, model, sqlParserCallback);
            tok = optTok(lexer);

            // support SAMPLE BY 1h ROLLUP(symbol, side)
            if (tok != null && (isRollupKeyword(tok) || isCubeKeyword(tok))) {
                int kwPos = lexer.lastTokenPosition();
                guardAgainstLatestOnWithGroupingSets(model, kwPos);
                boolean isRollup = isRollupKeyword(tok);
                ObjList<ExpressionNode> columnList = parseParenthesizedColumnList(lexer, model, sqlParserCallback);
                if (columnList.size() == 0) {
                    throw SqlException.$(kwPos, (isRollup ? "ROLLUP" : "CUBE") + " requires at least one column");
                }
                ensureColumnReferences(columnList, kwPos);
                checkMixedQualification(columnList);
                if (!isRollup && columnList.size() > 15) {
                    throw SqlException.$(kwPos, "CUBE supports at most 15 columns");
                }
                int startIdx = model.getGroupBy().size();
                for (int i = 0, n = columnList.size(); i < n; i++) {
                    model.addGroupBy(columnList.getQuick(i));
                }
                int maxSets = configuration.getSqlMaxGroupingSets();
                if (isRollup) {
                    expandRollup(model, startIdx, columnList.size(), kwPos, maxSets);
                } else {
                    expandCube(model, startIdx, columnList.size(), kwPos, maxSets);
                }
                tok = optTok(lexer);
            } else if (tok != null && isGroupingKeyword(tok)) {
                guardAgainstLatestOnWithGroupingSets(model, lexer.lastTokenPosition());
                CharSequence next = tok(lexer, "'SETS'");
                if (!isSetsKeyword(next)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "expected SETS after GROUPING");
                }
                parseExplicitGroupingSets(lexer, model, sqlParserCallback);
                tok = optTok(lexer);
            }

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
            if (hasPivot) {
                IQueryModel parentModel = queryModelPool.next();
                parentModel.setNestedModel(model);
                model = parentModel;
            }
            expectBy(lexer);
            tok = parseGroupByColumns(lexer, model, sqlParserCallback);
        }

        // expect [window]
        // WINDOW clause for named window definitions: WINDOW w AS (PARTITION BY ... ORDER BY ...)
        // SQL standard places WINDOW between HAVING/GROUP BY and ORDER BY.
        if (tok != null && isWindowKeyword(tok)) {
            do {
                // Parse window name
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "window name expected after 'window'");
                }
                if (isAsKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "window name expected after 'window'");
                }
                validateIdentifier(lexer, tok);
                SqlKeywords.assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());

                // Intern the window name immediately before any more lexer operations
                // (the lexer reuses its buffer, so tok would be overwritten)
                CharacterStoreEntry cse = characterStore.newEntry();
                cse.put(GenericLexer.unquote(tok));
                CharSequence windowName = cse.toImmutable();
                int windowNamePos = lexer.lastTokenPosition();

                // Check for duplicate window name in the outer (master) model
                if (masterModel.getNamedWindows().keyIndex(windowName) < 0) {
                    throw SqlException.$(windowNamePos, "duplicate window name");
                }

                // Expect AS
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || !isAsKeyword(tok)) {
                    throw SqlException.$(lexer.lastTokenPosition(), "'as' expected after window name");
                }

                // Expect '('
                tok = SqlUtil.fetchNext(lexer);
                if (tok == null || tok.charAt(0) != '(') {
                    throw SqlException.$(lexer.lastTokenPosition(), "'(' expected after 'as'");
                }

                // Create WindowExpression and parse the specification
                WindowExpression windowSpec = windowExpressionPool.next();
                windowSpec.clear();
                expressionParser.parseWindowSpec(lexer, windowSpec, sqlParserCallback, model.getDecls());

                // Validate base window reference (window inheritance):
                // the base must be defined earlier in the same WINDOW clause (no forward references)
                if (windowSpec.hasBaseWindow()) {
                    CharSequence baseName = windowSpec.getBaseWindowName();
                    if (masterModel.getNamedWindows().keyIndex(baseName) > -1) {
                        throw SqlException.$(windowSpec.getBaseWindowNamePosition(), "window '")
                                .put(baseName).put("' is not defined");
                    }
                }

                // Store named window in the outer (master) model where the SELECT columns are defined,
                // not the FROM model. The window functions in SELECT reference these named windows.
                masterModel.getNamedWindows().put(windowName, windowSpec);

                tok = optTok(lexer);
            } while (tok != null && Chars.equals(tok, ','));
        }

        // Validate that all named window references in SELECT columns are defined.
        // Fail fast here rather than waiting for the optimizer.
        validateNamedWindowReferences(masterModel);

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

                // token can sometimes be null, like during parsing of CASE clause
                if ((n.type == ExpressionNode.CONSTANT && Chars.equals("''", n.token))
                        || (n.type == ExpressionNode.LITERAL && (n.token == null || n.token.isEmpty()))) {
                    throw SqlException.$(lexer.lastTokenPosition(), "non-empty literal or expression expected");
                }

                tok = optTok(lexer);

                if (tok != null && isDescKeyword(tok)) {
                    model.addOrderBy(n, IQueryModel.ORDER_DIRECTION_DESCENDING);
                    tok = optTok(lexer);
                } else {
                    model.addOrderBy(n, IQueryModel.ORDER_DIRECTION_ASCENDING);

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
            // questdb accepts open-ended limits like 'LIMIT 5,' and 'LIMIT ,5'.
            // so reject only when neither side of the LIMIT clause parsed.
            if (lo == null && hi == null) {
                throw SqlException.$(lexer.lastTokenPosition(), "limit expression expected");
            }
            model.setLimit(lo, hi);
        } else {
            lexer.unparseLast();
        }
        return model;
    }

    private void parseFromTable(GenericLexer lexer, IQueryModel model) throws SqlException {
        CharSequence tok;
        tok = SqlUtil.fetchNext(lexer);
        if (tok == null || !isFromKeyword(tok)) {
            throw SqlException.position(lexer.lastTokenPosition()).put("expected 'from'");
        }
        parseTableName(lexer, model);
    }

    private void parseHints(GenericLexer lexer, IQueryModel model) {
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

    /**
     * Parses GROUP BY columns, handling ROLLUP, CUBE, and GROUPING SETS syntax.
     * Returns the next unconsumed token (or null).
     *
     * <p>Supported forms:
     * <ul>
     *   <li>{@code GROUP BY a, b} - plain GROUP BY (no grouping sets)</li>
     *   <li>{@code GROUP BY ROLLUP(a, b)} - desugared to GROUPING SETS ((a,b), (a), ())</li>
     *   <li>{@code GROUP BY CUBE(a, b)} - desugared to GROUPING SETS ((a,b), (a), (b), ())</li>
     *   <li>{@code GROUP BY GROUPING SETS ((a,b), (a), ())} - explicit grouping sets</li>
     *   <li>{@code GROUP BY a, ROLLUP(b, c)} - composite: plain cols added to every set</li>
     * </ul>
     */
    private CharSequence parseGroupByColumns(
            GenericLexer lexer,
            QueryModel model,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        CharSequence tok = tokIncludingLocalBrace(lexer, "literal");

        // Check for ROLLUP/CUBE/GROUPING SETS as the first token
        if (isRollupKeyword(tok) || isCubeKeyword(tok)) {
            int kwPos = lexer.lastTokenPosition();
            guardAgainstLatestOnWithGroupingSets(model, kwPos);
            boolean isRollup = isRollupKeyword(tok);
            ObjList<ExpressionNode> columns = parseParenthesizedColumnList(lexer, model, sqlParserCallback);
            if (columns.size() == 0) {
                throw SqlException.$(kwPos, isRollup ? "ROLLUP requires at least one column" : "CUBE requires at least one column");
            }
            ensureColumnReferences(columns, kwPos);
            checkMixedQualification(columns);
            if (!isRollup && columns.size() > 15) {
                throw SqlException.$(kwPos, "CUBE supports at most 15 columns");
            }
            for (int i = 0, n = columns.size(); i < n; i++) {
                model.addGroupBy(columns.getQuick(i));
            }
            int maxSets = configuration.getSqlMaxGroupingSets();
            if (isRollup) {
                expandRollup(model, 0, columns.size(), kwPos, maxSets);
            } else {
                expandCube(model, 0, columns.size(), kwPos, maxSets);
            }
            return optTok(lexer);
        }

        if (isGroupingKeyword(tok)) {
            int kwPos = lexer.lastTokenPosition();
            guardAgainstLatestOnWithGroupingSets(model, kwPos);
            tok = optTok(lexer);
            if (tok == null || !isSetsKeyword(tok)) {
                throw SqlException.$(kwPos, "expected SETS after GROUPING");
            }
            parseExplicitGroupingSets(lexer, model, sqlParserCallback);
            return optTok(lexer);
        }

        // Plain GROUP BY parsing - may contain ROLLUP/CUBE after plain columns (composite)
        lexer.unparseLast();
        ObjList<ExpressionNode> plainColumns = new ObjList<>();
        do {
            tok = tokIncludingLocalBrace(lexer, "literal");
            // Check if the current token is ROLLUP/CUBE (composite syntax)
            if (isRollupKeyword(tok) || isCubeKeyword(tok)) {
                int kwPos = lexer.lastTokenPosition();
                guardAgainstLatestOnWithGroupingSets(model, kwPos);
                boolean isRollup = isRollupKeyword(tok);
                int plainCount = plainColumns.size();
                for (int i = 0; i < plainCount; i++) {
                    model.addGroupBy(plainColumns.getQuick(i));
                }
                ObjList<ExpressionNode> rollupCols = parseParenthesizedColumnList(lexer, model, sqlParserCallback);
                if (rollupCols.size() == 0) {
                    throw SqlException.$(kwPos, isRollup ? "ROLLUP requires at least one column" : "CUBE requires at least one column");
                }
                ensureColumnReferences(rollupCols, kwPos);
                if (!isRollup && rollupCols.size() > 15) {
                    throw SqlException.$(kwPos, "CUBE supports at most 15 columns");
                }
                int rollupStart = plainCount;
                for (int i = 0, n = rollupCols.size(); i < n; i++) {
                    model.addGroupBy(rollupCols.getQuick(i));
                }
                // Check all columns (plain + rollup) for mixed qualification
                checkMixedQualification(model.getGroupBy());
                int maxSets = configuration.getSqlMaxGroupingSets();
                if (isRollup) {
                    expandRollupComposite(model, plainCount, rollupStart, rollupCols.size(), kwPos, maxSets);
                } else {
                    expandCubeComposite(model, plainCount, rollupStart, rollupCols.size(), kwPos, maxSets);
                }
                return optTok(lexer);
            }

            // Regular expression
            lexer.unparseLast();
            ExpressionNode n = expr(lexer, model, sqlParserCallback, model.getDecls());
            if (n == null || (n.type != ExpressionNode.LITERAL && n.type != ExpressionNode.CONSTANT && n.type != ExpressionNode.FUNCTION && n.type != ExpressionNode.OPERATION)) {
                throw SqlException.$(n == null ? lexer.lastTokenPosition() : n.position, "literal expected");
            }
            plainColumns.add(n);

            tok = optTok(lexer);
        } while (tok != null && Chars.equals(tok, ','));

        // Plain GROUP BY - no ROLLUP/CUBE/GROUPING SETS
        for (int i = 0, n = plainColumns.size(); i < n; i++) {
            model.addGroupBy(plainColumns.getQuick(i));
        }
        return tok;
    }

    /**
     * Parses explicit GROUPING SETS ((a, b), (a), ()).
     * The outer parentheses and inner sets with their parentheses.
     * <p>
     * Like ROLLUP/CUBE, explicit GROUPING SETS only accept column
     * references - expressions are rejected to avoid ambiguity in
     * set membership resolution.
     */
    private void parseExplicitGroupingSets(
            GenericLexer lexer,
            QueryModel model,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        expectTok(lexer, '(');
        // Track column name to index mapping for deduplication.
        // Dedup uses raw token text; mixing qualified (t.a) and unqualified
        // (a) references to the same column is rejected below.
        LowerCaseCharSequenceIntHashMap columnIndex = new LowerCaseCharSequenceIntHashMap();

        do {
            // Use tokIncludingLocalBrace to avoid subQueryMode swallowing ')'
            CharSequence tok = tokIncludingLocalBrace(lexer, "'(' or ')'");
            if (Chars.equals(tok, ')')) {
                break;
            }
            expectTok(tok, lexer.lastTokenPosition(), '(');

            IntList setIndices = new IntList();
            // Use tokIncludingLocalBrace to avoid subQueryMode swallowing ')'
            tok = tokIncludingLocalBrace(lexer, "column or ')'");
            if (!Chars.equals(tok, ')')) {
                // Non-empty set
                lexer.unparseLast();
                do {
                    ExpressionNode n = expr(lexer, model, sqlParserCallback, model.getDecls());
                    if (n == null || n.type != ExpressionNode.LITERAL) {
                        throw SqlException.$(n == null ? lexer.lastTokenPosition() : n.position, "column reference expected");
                    }

                    // Detect mixed qualified/unqualified references to the
                    // same column (e.g., both "a" and "t.a"). The dedup map
                    // uses raw token text, so these would create separate
                    // entries and produce incorrect grouping sets.
                    for (int ci = 0, cn = columnIndex.size(); ci < cn; ci++) {
                        CharSequence existing = columnIndex.keys().getQuick(ci);
                        if (existing != null && isSameColumnMixedQualification(n.token, existing)) {
                            throw SqlException.$(n.position, "mixing qualified and unqualified references to the same column");
                        }
                    }
                    int idx = columnIndex.keyIndex(n.token);
                    if (idx >= 0) {
                        int pos = model.getGroupBy().size();
                        model.addGroupBy(n);
                        columnIndex.putAt(idx, n.token, pos);
                        setIndices.add(pos);
                    } else {
                        setIndices.add(columnIndex.valueAt(idx));
                    }

                    // Use tokIncludingLocalBrace to avoid subQueryMode swallowing ')'
                    tok = tokIncludingLocalBrace(lexer, "',' or ')'");
                    if (Chars.equals(tok, ')')) {
                        break;
                    }
                    expectTok(tok, lexer.lastTokenPosition(), ',');
                } while (true);
            }
            model.addGroupingSet(setIndices);
            int maxSets = configuration.getSqlMaxGroupingSets();
            if (model.getGroupingSets().size() > maxSets) {
                throw SqlException.$(lexer.lastTokenPosition(), "too many grouping sets (maximum " + maxSets + ")");
            }

            // Use SqlUtil.fetchNext to avoid subQueryMode swallowing ')'
            tok = SqlUtil.fetchNext(lexer);
            if (tok == null || Chars.equals(tok, ')')) {
                break;
            }
            expectTok(tok, lexer.lastTokenPosition(), ',');
        } while (true);
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
            final IQueryModel queryModel = parseDml(lexer, null, lexer.lastTokenPosition(), true, sqlParserCallback, decls, false);
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

    private IQueryModel parseJoin(
            GenericLexer lexer,
            IQueryModel model,
            CharSequence tok,
            int joinType,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> parent,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        int errorPos = lexer.lastTokenPosition();

        if (isNotJoinKeyword(tok) && !Chars.equals(tok, ',')) {
            // not already a join?
            // was it "left", "right", "full" or window?
            if (isLeftKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = IQueryModel.JOIN_LEFT_OUTER;
                if (isOuterKeyword(tok)) {
                    tok = tok(lexer, "join");
                }
            } else if (isRightKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = IQueryModel.JOIN_RIGHT_OUTER;
                if (isOuterKeyword(tok)) {
                    tok = tok(lexer, "join");
                }
            } else if (isFullKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = IQueryModel.JOIN_FULL_OUTER;
                if (isOuterKeyword(tok)) {
                    tok = tok(lexer, "join");
                }
            } else if (isWindowKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = IQueryModel.JOIN_WINDOW;
            } else if (isHorizonKeyword(tok)) {
                tok = tok(lexer, "join");
                joinType = IQueryModel.JOIN_HORIZON;
            } else if (isLateralKeyword(tok)) {
                joinType = IQueryModel.JOIN_LATERAL_CROSS;
            } else {
                tok = tok(lexer, "join");
            }
            if (joinType != IQueryModel.JOIN_LATERAL_CROSS && isNotJoinKeyword(tok)) {
                throw SqlException.position(errorPos).put("'join' expected");
            }
        }

        tok = expectTableNameOrSubQuery(lexer);

        // UNNEST in comma position: FROM t, UNNEST(...)
        if (isUnnestKeyword(tok) && joinType == QueryModel.JOIN_CROSS) {
            return parseUnnest(lexer, model, decls, sqlParserCallback);
        }

        if (isLateralKeyword(tok) && joinType != IQueryModel.JOIN_LATERAL_CROSS) {
            joinType = switch (joinType) {
                case IQueryModel.JOIN_LEFT_OUTER -> IQueryModel.JOIN_LATERAL_LEFT;
                case IQueryModel.JOIN_INNER -> IQueryModel.JOIN_LATERAL_INNER;
                case IQueryModel.JOIN_CROSS -> IQueryModel.JOIN_LATERAL_CROSS;
                default -> throw SqlException.position(lexer.lastTokenPosition())
                        .put("LATERAL is only supported with INNER, LEFT, or CROSS joins");
            };
            tok = expectTableNameOrSubQuery(lexer);
        }

        if (IQueryModel.isLateralJoin(joinType) && !Chars.equals(tok, '(')) {
            throw SqlException.position(lexer.lastTokenPosition()).put("LATERAL requires a subquery");
        }

        QueryModel joinModel = queryModelPool.next();
        joinModel.copyDeclsFrom(decls, false);
        joinModel.setJoinType(joinType);
        joinModel.setJoinKeywordPosition(errorPos);

        final TableToken tt = cairoEngine.getTableTokenIfExists(unquote(tok));
        if (tt != null && tt.isView()) {
            compileViewQuery(joinModel, tt, lexer.lastTokenPosition());
        } else if (Chars.equals(tok, '(')) {
            joinModel.setNestedModel(parseAsSubQueryAndExpectClosingBrace(lexer, parent, true, sqlParserCallback, decls));
        } else {
            lexer.unparseLast();
            parseSelectFrom(lexer, joinModel, parent, sqlParserCallback);
        }

        tok = setModelAliasAndGetOptTok(lexer, joinModel);

        if ((joinType == IQueryModel.JOIN_CROSS || joinType == IQueryModel.JOIN_LATERAL_CROSS) && tok != null && isOnKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "Cross joins cannot have join clauses");
        }

        boolean onClauseObserved = false;
        switch (joinType) {
            case IQueryModel.JOIN_ASOF:
            case IQueryModel.JOIN_LT:
            case IQueryModel.JOIN_SPLICE:
            case IQueryModel.JOIN_WINDOW:
            case IQueryModel.JOIN_HORIZON:
            case IQueryModel.JOIN_LATERAL_INNER:
            case IQueryModel.JOIN_LATERAL_LEFT:
                if (tok == null || !isOnKeyword(tok)) {
                    lexer.unparseLast();
                    break;
                }
                // intentional fall through
            case IQueryModel.JOIN_INNER:
            case IQueryModel.JOIN_LEFT_OUTER:
            case IQueryModel.JOIN_RIGHT_OUTER:
            case IQueryModel.JOIN_FULL_OUTER:
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
        if (joinType == IQueryModel.JOIN_WINDOW) {
            expectTok(lexer, tok, "range");
            tok = optTok(lexer);
            expectTok(lexer, tok, "between");
            tok = tok(lexer, "'unbounded', 'current' or expression");
            WindowJoinContext context = joinModel.getWindowJoinContext();

            // lo
            if (isUnboundedPreceding(lexer, tok)) {
                context.setLoKind(WindowJoinContext.PRECEDING, lexer.lastTokenPosition());
            } else if (isCurrentRow(lexer, tok)) {
                context.setLoKind(WindowJoinContext.CURRENT, lexer.lastTokenPosition());
            } else if (isPrecedingKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "integer expression expected");
            } else {
                int pos = lexer.lastTokenPosition();
                lexer.unparseLast();
                context.setLoExpr(expectExpr(lexer, sqlParserCallback, model.getDecls()), pos);
                char timeUnit = parseTimeUnit(lexer);
                if (timeUnit != 0) {
                    context.setLoExprTimeUnit(timeUnit);
                }

                tok = tok(lexer, "'preceding' or 'following'");
                if (isPrecedingKeyword(tok)) {
                    context.setLoKind(WindowJoinContext.PRECEDING, lexer.lastTokenPosition());
                } else if (isFollowingKeyword(tok)) {
                    context.setLoKind(WindowJoinContext.FOLLOWING, lexer.lastTokenPosition());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'preceding' or 'following' expected");
                }
            }

            tok = optTok(lexer);
            expectTok(lexer, tok, "and");
            tok = tok(lexer, "'unbounded', 'current' or expression");
            // hi
            if (isUnboundedKeyword(tok)) {
                tok = tok(lexer, "'following'");
                if (isFollowingKeyword(tok)) {
                    context.setHiKind(WindowJoinContext.FOLLOWING, lexer.lastTokenPosition());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'following' expected");
                }
            } else if (isCurrentRow(lexer, tok)) {
                context.setHiKind(WindowJoinContext.CURRENT, lexer.lastTokenPosition());
            } else if (isPrecedingKeyword(tok) || isFollowingKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "integer expression expected");
            } else {
                int pos = lexer.lastTokenPosition();
                lexer.unparseLast();
                context.setHiExpr(expectExpr(lexer, sqlParserCallback, model.getDecls()), pos);
                char timeUnit = parseTimeUnit(lexer);
                if (timeUnit != 0) {
                    context.setHiExprTimeUnit(timeUnit);
                }

                tok = tok(lexer, "'preceding'  'following'");
                if (isPrecedingKeyword(tok)) {
                    if (context.getLoKind() == WindowJoinContext.CURRENT) {
                        throw SqlException.$(lexer.lastTokenPosition(), "start row is CURRENT, end row must not be PRECEDING");
                    }
                    if (context.getLoKind() == WindowJoinContext.FOLLOWING) {
                        throw SqlException.$(lexer.lastTokenPosition(), "start row is FOLLOWING, end row must not be PRECEDING");
                    }
                    context.setHiKind(WindowJoinContext.PRECEDING, lexer.lastTokenPosition());
                } else if (isFollowingKeyword(tok)) {
                    context.setHiKind(WindowJoinContext.FOLLOWING, lexer.lastTokenPosition());
                } else {
                    throw SqlException.$(lexer.lastTokenPosition(), "'preceding' or 'following' expected");
                }
            }

            tok = optTok(lexer);
            if (tok != null) {
                if (isIncludePrevailing(lexer, tok)) {
                    context.setIncludePrevailing(true);
                } else if (isExcludePrevailing(lexer, tok)) {
                    context.setIncludePrevailing(false);
                } else {
                    lexer.unparseLast();
                }
            } else {
                lexer.unparseLast();
            }
            return joinModel;
        }

        if (joinType == IQueryModel.JOIN_HORIZON) {
            HorizonJoinContext context = joinModel.getHorizonJoinContext();

            // RANGE/LIST clause is optional for non-last HORIZON JOINs in a multi-join chain.
            // If the next token is not range/list, this is a non-last HORIZON JOIN — return as-is.
            if (tok == null || (!isRangeKeyword(tok) && !isListKeyword(tok))) {
                lexer.unparseLast();
                return joinModel;
            }

            if (isRangeKeyword(tok)) {
                // RANGE FROM <interval> TO <interval> STEP <interval> AS <alias>
                context.setMode(HorizonJoinContext.MODE_RANGE);

                expectTok(lexer, "from");
                ExpressionNode fromExpr = expectIntervalLiteral(lexer);
                context.setRangeFrom(fromExpr, fromExpr.position);

                tok = tok(lexer, "'to'");
                expectTok(lexer, tok, "to");
                ExpressionNode toExpr = expectIntervalLiteral(lexer);
                context.setRangeTo(toExpr);

                tok = tok(lexer, "'step'");
                expectTok(lexer, tok, "step");
                ExpressionNode stepExpr = expectIntervalLiteral(lexer);
                context.setRangeStep(stepExpr, stepExpr.position);
            } else if (isListKeyword(tok)) {
                // LIST (<expr>, <expr>, ...) AS <alias>
                context.setMode(HorizonJoinContext.MODE_LIST);

                tok = tok(lexer, "'('");
                expectTok(lexer, tok, "(");

                // Parse list of offset expressions
                // Use tokIncludingLocalBrace to avoid subQueryMode swallowing ')'
                tok = tokIncludingLocalBrace(lexer, "expression");
                if (Chars.equals(tok, ')')) {
                    throw SqlException.$(lexer.lastTokenPosition(), "at least one offset expression expected");
                }
                lexer.unparseLast();

                while (true) {
                    ExpressionNode offsetExpr = expectIntervalLiteral(lexer);
                    context.addListOffset(offsetExpr);

                    tok = tokIncludingLocalBrace(lexer, "',' or ')'");
                    if (Chars.equals(tok, ')')) {
                        break;
                    }
                    if (!Chars.equals(tok, ',')) {
                        throw SqlException.$(lexer.lastTokenPosition(), "',' or ')' expected");
                    }
                }
            }

            // Expect AS <alias>
            tok = tok(lexer, "'as'");
            expectTok(lexer, tok, "as");
            tok = tok(lexer, "alias");
            int aliasPos = lexer.lastTokenPosition();
            ExpressionNode aliasNode = literal(tok, aliasPos);
            context.setAlias(aliasNode, aliasPos);

            // Create synthetic offset model for the horizon pseudo-table
            // This model represents the virtual table with offset/timestamp columns
            IQueryModel syntheticOffsetModel = queryModelPool.next();
            syntheticOffsetModel.setJoinType(IQueryModel.JOIN_CROSS);
            syntheticOffsetModel.setAlias(aliasNode);

            // Move HorizonJoinContext to the synthetic model
            // The synthetic model holds the range/list configuration
            HorizonJoinContext syntheticContext = syntheticOffsetModel.getHorizonJoinContext();
            syntheticContext.copyFrom(context);
            context.clear();

            // Add offset and timestamp columns to the synthetic model
            ExpressionNode offsetNode = expressionNodePool.next().of(ExpressionNode.LITERAL, "offset", 0, aliasPos);
            syntheticOffsetModel.addField(queryColumnPool.next().of("offset", offsetNode));

            ExpressionNode timestampNode = expressionNodePool.next().of(ExpressionNode.LITERAL, "timestamp", 0, aliasPos);
            syntheticOffsetModel.addField(queryColumnPool.next().of("timestamp", timestampNode));

            // Add synthetic model to parent's join models before the HORIZON JOIN model
            model.addJoinModel(syntheticOffsetModel);

            return joinModel;
        }

        if (tok == null || !SqlKeywords.isToleranceKeyword(tok)) {
            lexer.unparseLast();
            return joinModel;
        }
        if (joinType != IQueryModel.JOIN_ASOF && joinType != IQueryModel.JOIN_LT) {
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

    private void parseLatestBy(GenericLexer lexer, IQueryModel model) throws SqlException {
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

    private void parseLatestByDeprecated(GenericLexer lexer, IQueryModel model) throws SqlException {
        // 'latest by' is already parsed at this point

        CharSequence tok;
        do {
            model.addLatestBy(expectLiteral(lexer, model.getDecls()));
            tok = SqlUtil.fetchNext(lexer);
        } while (Chars.equalsNc(tok, ','));

        model.setLatestByType(IQueryModel.LATEST_BY_DEPRECATED);

        if (tok != null) {
            lexer.unparseLast();
        }
    }

    private void parseLatestByNew(GenericLexer lexer, IQueryModel model) throws SqlException {
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

        model.setLatestByType(IQueryModel.LATEST_BY_NEW);

        if (tok != null) {
            lexer.unparseLast();
        }
    }

    /**
     * Parses a parenthesized comma-separated list of column expressions: (a, b, c).
     * Used by ROLLUP and CUBE parsing.
     */
    private ObjList<ExpressionNode> parseParenthesizedColumnList(
            GenericLexer lexer,
            QueryModel model,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        expectTok(lexer, '(');
        ObjList<ExpressionNode> columns = new ObjList<>();
        // Use tokIncludingLocalBrace to avoid subQueryMode swallowing ')'
        CharSequence tok = tokIncludingLocalBrace(lexer, "column or ')'");
        if (Chars.equals(tok, ')')) {
            return columns;
        }
        lexer.unparseLast();
        do {
            ExpressionNode n = expr(lexer, model, sqlParserCallback, model.getDecls());
            if (n == null || (n.type != ExpressionNode.LITERAL && n.type != ExpressionNode.CONSTANT && n.type != ExpressionNode.FUNCTION && n.type != ExpressionNode.OPERATION)) {
                throw SqlException.$(n == null ? lexer.lastTokenPosition() : n.position, "literal expected");
            }
            columns.add(n);
            // Use tokIncludingLocalBrace to avoid subQueryMode swallowing ')'
            tok = tokIncludingLocalBrace(lexer, "',' or ')'");
            if (Chars.equals(tok, ')')) {
                break;
            }
            expectTok(tok, lexer.lastTokenPosition(), ',');
        } while (true);
        return columns;
    }

    /**
     * Parses PIVOT clause with the following syntax:
     * <pre>
     * PIVOT (
     *     agg_func(col) [AS alias], ...
     *     FOR pivot_col IN (val1 [AS alias1], val2 [AS alias2], ... | subquery)
     *     [FOR pivot_col2 IN (...)]
     *     [GROUP BY col1, col2, ...] ) [alias]
     * </pre>
     * <p>
     * <b>Note:</b> ELSE clause is not supported in PIVOT FOR columns. Two-phase aggregates (e.g., avg requires
     * sum + count in phase 1) cannot be correctly rewritten for ELSE values. Inserting
     * appropriate expressions during the first aggregation phase is complex and would
     * significantly impact performance. This aligns with mainstream databases which also
     * do not support ELSE in PIVOT. For such requirements, user can use subqueries instead.
     */
    /**
     * Parses a parenthesized comma-separated list of column expressions: (a, b, c).
     * Used by ROLLUP and CUBE parsing.
     */
    private ObjList<ExpressionNode> parseParenthesizedColumnList(
            GenericLexer lexer,
            QueryModel model,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        expectTok(lexer, '(');
        ObjList<ExpressionNode> columns = new ObjList<>();
        CharSequence tok = tok(lexer, "column or ')'");
        if (Chars.equals(tok, ')')) {
            return columns;
        }
        lexer.unparseLast();
        do {
            ExpressionNode n = expr(lexer, model, sqlParserCallback, model.getDecls());
            if (n == null || (n.type != ExpressionNode.LITERAL && n.type != ExpressionNode.CONSTANT && n.type != ExpressionNode.FUNCTION && n.type != ExpressionNode.OPERATION)) {
                throw SqlException.$(n == null ? lexer.lastTokenPosition() : n.position, "literal expected");
            }
            columns.add(n);
            tok = tok(lexer, "',' or ')'");
            if (Chars.equals(tok, ')')) {
                break;
            }
            expectTok(tok, lexer.lastTokenPosition(), ',');
        } while (true);
        return columns;
    }

    private CharSequence parsePivot(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        CharSequence tok;
        expectTok(lexer, '(');

        // Parse aggregate functions.
        FunctionFactoryCache functionFactoryCache = cairoEngine.getFunctionFactoryCache();
        pivotAliasMap.clear();
        do {
            model.addPivotGroupByColumn(parsePivotAggregateColumn(lexer, model, functionFactoryCache, sqlParserCallback));
            tok = optTok(lexer);
        } while (tok != null && isNotForKeyword(tok) && isComma(tok));

        ObjList<QueryColumn> pivotGroupByCols = model.getPivotGroupByColumns();
        boolean hasNoAlias = false;
        for (int i = 0, n = pivotGroupByCols.size(); i < n; i++) {
            QueryColumn qc = pivotGroupByCols.getQuick(i);
            if (qc.getAlias() == null) {
                hasNoAlias = true;
                CharacterStoreEntry entry = characterStore.newEntry();
                qc.getAst().toSink(entry);
                CharSequence alias = SqlUtil.createExprColumnAlias(
                        characterStore,
                        entry.toImmutable(),
                        pivotAliasMap,
                        aliasSequenceMap,
                        configuration.getColumnAliasGeneratedMaxSize(),
                        true
                );
                pivotAliasMap.add(alias);
                qc.setAlias(alias, qc.getAst().position);
            }
        }
        model.setPivotGroupByColumnHasNoAlias(hasNoAlias && pivotGroupByCols.size() == 1);

        if (tok == null || isNotForKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "expected FOR");
        }

        // Parse FOR expressions (e.g., FOR region IN ('East', 'West') ELSE 'Other')
        // We parse "col IN (values)" separately (not as standard IN expression) because
        // PIVOT supports per-value aliases (e.g., 'value' AS alias) which regular IN doesn't.
        while (true) {
            ExpressionNode inColumnExpr;
            try {
                // Stop at top-level IN operator to handle values list with alias support
                expressionParser.setStopOnTopINOperator(true);
                inColumnExpr = expr(lexer, model, sqlParserCallback);
            } finally {
                expressionParser.setStopOnTopINOperator(false);
            }
            if (inColumnExpr == null) {
                throw SqlException.$(lexer.lastTokenPosition(), "expected IN expression");
            }
            if (hasGroupByFunc(sqlNodeStack, functionFactoryCache, inColumnExpr)) {
                throw SqlException.$(inColumnExpr.position, "aggregate functions are not supported in PIVOT FOR expressions");
            }

            expectTok(lexer, "in");
            expectTok(lexer, '(');
            tok = tok(lexer, "'select' or constant");
            lexer.unparseLast();
            boolean isSubquery = isSelectKeyword(tok);
            final PivotForColumn pivotForColumn = pivotQueryColumnPool.next().of(inColumnExpr, !isSubquery);
            model.addPivotForColumn(pivotForColumn);

            if (isSubquery) { // IN list from subquery
                ExpressionNode expr = expr(lexer, model, sqlParserCallback);
                if (expr == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "missing subquery");
                }
                pivotForColumn.setSelectSubqueryExpr(expr);
                expectTok(lexer, ')');
            } else {
                tempCharSequenceSet.clear();
                pivotAliasMap.clear();
                aliasSequenceMap.clear();
                do {
                    ExpressionNode expr = expr(lexer, model, sqlParserCallback);
                    if (expr == null) {
                        throw SqlException.$(lexer.lastTokenPosition(), "missing constant");
                    }
                    CharacterStoreEntry entry = characterStore.newEntry();
                    expr.toSink(entry);
                    CharSequence exprName = entry.toImmutable();
                    final int index = tempCharSequenceSet.keyIndex(exprName);
                    if (index < 0) {
                        throw SqlException.$(expr.position, "duplicate value in PIVOT IN list: ").put(exprName);
                    }
                    tempCharSequenceSet.addAtWithBorrowed(index, exprName);

                    CharSequence nextTok = tok(lexer, "',' or ')'");
                    CharSequence alias;
                    if (isNotForKeyword(nextTok) && columnAliasStop.excludes(nextTok)) {
                        assertNotDot(lexer, nextTok);
                        if (isAsKeyword(nextTok)) {
                            nextTok = tok(lexer, "alias");
                            SqlKeywords.assertNameIsQuotedOrNotAKeyword(nextTok, lexer.lastTokenPosition());
                            CharSequence aliasTok = GenericLexer.immutableOf(nextTok);
                            validateIdentifier(lexer, aliasTok);
                            alias = unquote(aliasTok);
                        } else {
                            validateIdentifier(lexer, nextTok);
                            SqlKeywords.assertNameIsQuotedOrNotAKeyword(nextTok, lexer.lastTokenPosition());
                            alias = GenericLexer.immutableOf(unquote(nextTok));
                        }
                        if (!pivotAliasMap.add(alias)) {
                            throw SqlException.$(lexer.lastTokenPosition(), "duplicate alias in PIVOT IN list: ").put(alias);
                        }
                    } else {
                        lexer.unparseLast();
                        alias = SqlUtil.createExprColumnAlias(
                                characterStore,
                                unquote(exprName),
                                pivotAliasMap,
                                aliasSequenceMap,
                                configuration.getColumnAliasGeneratedMaxSize(),
                                true
                        );
                        pivotAliasMap.add(alias);
                    }

                    pivotForColumn.addValue(expr, alias);
                    tok = tok(lexer, "constant list");
                } while (isComma(tok));

                if (!isRightParen(tok)) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("')' expected");
                }
            }

            tok = optTok(lexer);
            if (tok == null) {
                throw SqlException.$(lexer.lastTokenPosition(), "')' expected");
            }
            if (pivotForStop.contains(tok)) {
                break;
            } else {
                lexer.unparseLast();
            }
        }

        // Parse optional GROUP BY clause
        if (isGroupKeyword(tok)) {
            expectBy(lexer);
            do {
                tokIncludingLocalBrace(lexer, "literal");
                lexer.unparseLast();
                ExpressionNode groupByExpr = expr(lexer, model, sqlParserCallback, model.getDecls());

                if (groupByExpr == null) {
                    throw SqlException.$(lexer.lastTokenPosition(), "group by expression expected");
                }

                switch (groupByExpr.type) {
                    case ExpressionNode.LITERAL:
                    case ExpressionNode.CONSTANT:
                    case ExpressionNode.FUNCTION:
                    case ExpressionNode.OPERATION:
                        break;
                    default:
                        throw SqlException.$(lexer.lastTokenPosition(), "group by expression expected");
                }

                model.addGroupBy(groupByExpr);
                tok = optTok(lexer);
            } while (tok != null && !isRightParen(tok) && isComma(tok));
        }

        if (tok == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "missing ')'");
        }

        if (!isRightParen(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "')' expected");
        }
        tok = setModelAliasAndGetOptTok(lexer, model);

        return tok;
    }

    private QueryColumn parsePivotAggregateColumn(GenericLexer lexer, IQueryModel model, FunctionFactoryCache functionFactoryCache, SqlParserCallback sqlParserCallback) throws SqlException {
        ExpressionNode expr = expr(lexer, model, sqlParserCallback);
        if (expr == null) {
            throw SqlException.$(lexer.lastTokenPosition(), "missing aggregate function expression");
        }
        if (!hasGroupByFunc(sqlNodeStack, functionFactoryCache, expr)) {
            throw SqlException.$(expr.position, "expected aggregate function [col=").put(expr).put(']');
        }
        CharSequence tok = tok(lexer, "'FOR' or ',' or ')'");
        QueryColumn col = queryColumnPool.next().of(null, expr);

        if (isNotForKeyword(tok) && columnAliasStop.excludes(tok)) {
            CharSequence alias;
            assertNotDot(lexer, tok);
            if (isAsKeyword(tok)) {
                tok = tok(lexer, "alias");
                SqlKeywords.assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                CharSequence aliasTok = GenericLexer.immutableOf(tok);
                validateIdentifier(lexer, aliasTok);
                alias = unquote(aliasTok);
            } else {
                validateIdentifier(lexer, tok);
                SqlKeywords.assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                alias = GenericLexer.immutableOf(unquote(tok));
            }
            col.setAlias(alias, lexer.lastTokenPosition());
        } else {
            lexer.unparseLast();
        }
        return col;
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
        final IQueryModel model = parseDml(lexer, null, lexer.lastTokenPosition(), true, sqlParserCallback, decls, false);
        final CharSequence tok = optTok(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            model.recordViews(recordedViews);
            return model;
        }
        if (Chars.equals(tok, ":=")) {
            throw errUnexpected(lexer, tok, "perhaps `DECLARE` was misspelled?");
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseSelectClause(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
        int pos = lexer.getPosition();
        CharSequence tok = SqlUtil.fetchNext(lexer, true);
        if (tok == null || (subQueryMode && Chars.equals(tok, ')'))) {
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

                    if (isUnexpectedRightParenInTopLevelSelect(tok)) {
                        throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [)]");
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

                // ExpressionParser now handles window functions (func(...) OVER (...)) as part of
                // expression parsing. When windowContext is set, the OVER clause has already been
                // consumed from the lexer and we can use the parsed WindowColumn directly.
                if (expr.windowExpression != null) {
                    // ExpressionParser already parsed the window function with its OVER clause
                    col = expr.windowExpression;
                } else {
                    // Regular expression (non-window function)
                    if (expr.type == ExpressionNode.QUERY) {
                        throw SqlException.$(expr.position, "query is not expected, did you mean column?");
                    }
                    col = queryColumnPool.next().of(null, expr);
                }

                final CharSequence alias;
                final int aliasPosition;
                if (tok != null && columnAliasStop.excludes(tok)) {
                    assertNotDot(lexer, tok);
                    if (isAsKeyword(tok)) {
                        tok = tok(lexer, "alias");
                        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                        CharSequence aliasTok = GenericLexer.immutableOf(tok);
                        validateIdentifier(lexer, aliasTok);
                        boolean unquoting = Chars.indexOf(aliasTok, '.') == -1;
                        alias = unquoting ? unquote(aliasTok) : aliasTok;
                    } else {
                        validateIdentifier(lexer, tok);
                        assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
                        boolean unquoting = Chars.indexOf(tok, '.') == -1;
                        alias = GenericLexer.immutableOf(unquoting ? unquote(tok) : tok);
                    }
                    aliasPosition = lexer.lastTokenPosition();

                    if (col.getAst().isWildcard()) {
                        throw err(lexer, null, "wildcard cannot have alias");
                    }

                    tok = optTok(lexer);
                    aliasMap.add(alias);
                } else {
                    alias = null;
                    aliasPosition = QueryColumn.SYNTHESIZED_ALIAS_POSITION;
                }

                // correlated sub-queries do not have expr.token values (they are null)
                if (expr.type == ExpressionNode.QUERY) {
                    expr.token = alias;
                }

                if (alias != null) {
                    if (alias.isEmpty()) {
                        throw err(lexer, null, "column alias cannot be a blank string");
                    }
                    col.setAlias(alias, aliasPosition);
                }

                accumulatedColumns.add(col);
                accumulatedColumnPositions.add(colPosition);

                if (tok == null || Chars.equals(tok, ';')) {
                    lexer.unparseLast();
                    break;
                }

                if (Chars.equals(tok, ')')) {
                    if (isUnexpectedRightParenInTopLevelSelect(tok)) {
                        // it's an unbalanced ')' in top-level SELECT
                        throw SqlException.$(lexer.lastTokenPosition(), "unexpected token [)]");
                    } else {
                        // it's a balanced: ')'
                        lexer.unparseLast();
                        break;
                    }
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
            aliasSequenceMap.clear();
        }
    }

    private void parseSelectFrom(
            GenericLexer lexer,
            IQueryModel model,
            LowerCaseCharSequenceObjHashMap<WithClauseModel> masterModel,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        ExpressionNode expr = expr(lexer, model, sqlParserCallback, model.getDecls());
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
                    IQueryModel cteModel = parseWith(lexer, withClause, sqlParserCallback, model.getDecls());
                    cteModel.setIsCteModel(true);
                    model.setNestedModel(cteModel);
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

    private void parseTableName(GenericLexer lexer, IQueryModel model) throws SqlException {
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
            unit = WindowExpression.TIME_UNIT_NANOSECOND;
        } else if (isMicrosecondKeyword(tok) || isMicrosecondsKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_MICROSECOND;
        } else if (isMillisecondKeyword(tok) || isMillisecondsKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_MILLISECOND;
        } else if (isSecondKeyword(tok) || isSecondsKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_SECOND;
        } else if (isMinuteKeyword(tok) || isMinutesKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_MINUTE;
        } else if (isHourKeyword(tok) || isHoursKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_HOUR;
        } else if (isDayKeyword(tok) || isDaysKeyword(tok)) {
            unit = WindowExpression.TIME_UNIT_DAY;
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

    private IQueryModel parseUnnest(
            GenericLexer lexer,
            IQueryModel parent,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        // Temporarily disable subQueryMode so that optTok() does not swallow
        // the ')' tokens that belong to UNNEST's own parentheses.
        boolean savedSubQueryMode = subQueryMode;
        subQueryMode = false;
        try {
            return parseUnnest0(lexer, parent, decls, sqlParserCallback);
        } finally {
            subQueryMode = savedSubQueryMode;
        }
    }

    private IQueryModel parseUnnest0(
            GenericLexer lexer,
            IQueryModel parent,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            SqlParserCallback sqlParserCallback
    ) throws SqlException {
        QueryModel unnestModel = queryModelPool.next();
        unnestModel.copyDeclsFrom(decls, false);
        unnestModel.setJoinType(QueryModel.JOIN_UNNEST);
        unnestModel.setJoinKeywordPosition(lexer.lastTokenPosition());

        expectTok(lexer, '(');
        // parse comma-separated expressions, each optionally followed by
        // COLUMNS(name TYPE, ...) for JSON UNNEST sources
        do {
            ExpressionNode expression = expr(lexer, parent, sqlParserCallback, decls);
            if (expression == null) {
                throw SqlException.$(lexer.lastTokenPosition(), "expression expected");
            }
            unnestModel.getUnnestExpressions().add(expression);
            CharSequence tok = tok(lexer, "'COLUMNS', ',' or ')'");
            if (isColumnsKeyword(tok)) {
                expectTok(lexer, '(');
                ObjList<CharSequence> colNames = new ObjList<>();
                IntList colTypes = new IntList();
                do {
                    CharSequence colNameTok = tok(lexer, "column name");
                    assertNameIsQuotedOrNotAKeyword(colNameTok, lexer.lastTokenPosition());
                    CharSequence colName = GenericLexer.immutableOf(unquote(colNameTok));
                    CharSequence typeName = tok(lexer, "column type");
                    int type = ColumnType.typeOf(typeName);
                    if (type == -1) {
                        throw SqlException
                                .$(lexer.lastTokenPosition(), "unknown type: ")
                                .put(typeName);
                    }
                    if (!isJsonUnnestSupportedType(type)) {
                        throw SqlException
                                .$(lexer.lastTokenPosition(),
                                        "unsupported type for JSON UNNEST: ")
                                .put(typeName);
                    }
                    colNames.add(colName);
                    colTypes.add(type);
                    tok = tok(lexer, "',' or ')'");
                    if (Chars.equals(tok, ')')) {
                        break;
                    }
                    if (!Chars.equals(tok, ',')) {
                        throw SqlException
                                .$(lexer.lastTokenPosition(),
                                        "',' or ')' expected");
                    }
                } while (true);
                unnestModel.getUnnestJsonColumnNames().add(colNames);
                unnestModel.getUnnestJsonColumnTypes().add(colTypes);
                tok = tok(lexer, "',' or ')'");
            } else {
                // array source - null marker
                unnestModel.getUnnestJsonColumnNames().add(null);
                unnestModel.getUnnestJsonColumnTypes().add(null);
            }
            if (Chars.equals(tok, ')')) {
                break;
            }
            if (!Chars.equals(tok, ',')) {
                throw SqlException.$(lexer.lastTokenPosition(), "',' or ')' expected");
            }
        } while (true);

        // optional WITH ORDINALITY
        CharSequence tok = optTok(lexer);
        if (tok != null && isWithKeyword(tok)) {
            tok = tok(lexer, "'ordinality'");
            if (!isOrdinalityKeyword(tok)) {
                throw SqlException.$(lexer.lastTokenPosition(), "'ordinality' expected");
            }
            unnestModel.setUnnestOrdinality(true);
            tok = optTok(lexer);
        }

        // optional AS alias
        if (tok != null && isAsKeyword(tok)) {
            tok = tok(lexer, "alias");
            unnestModel.setAlias(literal(lexer, tok));
            tok = optTok(lexer);
        } else if (tok != null && tableAliasStop.excludes(tok) && !Chars.equals(tok, '(')) {
            unnestModel.setAlias(literal(lexer, tok));
            tok = optTok(lexer);
        }

        // optional column aliases: (col1, col2, ...)
        int firstExcessAliasPos = -1;
        if (tok != null && Chars.equals(tok, '(')) {
            int maxAliases = unnestModel.getUnnestOutputColumnCount()
                    + (unnestModel.isUnnestOrdinality() ? 1 : 0);
            do {
                tok = tok(lexer, "column alias");
                int aliasPos = lexer.lastTokenPosition();
                assertNameIsQuotedOrNotAKeyword(tok, aliasPos);
                unnestModel.getUnnestColumnAliases().add(GenericLexer.immutableOf(unquote(tok)));
                if (firstExcessAliasPos == -1
                        && unnestModel.getUnnestColumnAliases().size() > maxAliases) {
                    firstExcessAliasPos = aliasPos;
                }
                tok = tok(lexer, "',' or ')'");
                if (Chars.equals(tok, ')')) {
                    break;
                }
                if (!Chars.equals(tok, ',')) {
                    throw SqlException.$(lexer.lastTokenPosition(), "',' or ')' expected");
                }
            } while (true);
        } else if (tok != null) {
            lexer.unparseLast();
        }

        if (firstExcessAliasPos != -1) {
            throw SqlException.$(
                    firstExcessAliasPos,
                    "too many column aliases for UNNEST"
            );
        }

        return unnestModel;
    }

    private ExecutionModel parseUpdate(
            GenericLexer lexer,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        lexer.unparseLast();
        final IQueryModel model = parseDmlUpdate(lexer, sqlParserCallback, decls);
        final CharSequence tok = optTok(lexer);
        if (tok == null || Chars.equals(tok, ';')) {
            return model;
        }
        throw errUnexpected(lexer, tok);
    }

    private void parseUpdateClause(
            GenericLexer lexer,
            IQueryModel updateQueryModel,
            IQueryModel fromModel,
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
            ExpressionNode expr = expr(lexer, (IQueryModel) null, sqlParserCallback);
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

    private IQueryModel parseWith(
            GenericLexer lexer,
            WithClauseModel wcm,
            SqlParserCallback sqlParserCallback,
            @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls
    ) throws SqlException {
        IQueryModel m = wcm.popModel();
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
            if (name.token.isEmpty()) {
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

    private CharSequence parseWithOffset(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
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
     * - varchar cast is rewritten in a special way, e.g. removed
     * - subset of types is handled more efficiently in the 3-arg function
     * - the remaining type casts are not rewritten, e.g. left as is
     */
    private void rewriteJsonExtractCast(ExpressionNode node) {
        if (node.type == ExpressionNode.FUNCTION && isCastKeyword(node.token)) {
            if (node.lhs != null
                    && node.lhs.type == ExpressionNode.FUNCTION
                    && node.lhs.paramCount == 2
                    && node.lhs.token != null
                    && isJsonExtract(node.lhs.token)) {
                // rewrite cast such as
                // json_extract(json,path)::type -> json_extract(json,path,type)
                // the ::type is already rewritten as
                // cast(json_extract(json,path) as type)

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
                        characterStoreEntry.put(castType);
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
        traversalAlgo.traverse(parent, rewritePgNumericRef);
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

    /**
     * Rewrites the following:
     * <p>
     * select '123.456'::numeric::decimal(p, s) -> select '123.456'::decimal(p, s)
     */
    private void rewritePgNumeric(ExpressionNode node) {
        if (node.type != ExpressionNode.FUNCTION || !isCastKeyword(node.token)) {
            return;
        }

        ExpressionNode innerCastNode = node.lhs;
        if (innerCastNode == null || innerCastNode.type != ExpressionNode.FUNCTION || !isCastKeyword(innerCastNode.token)) {
            return;
        }

        ExpressionNode innerTypeNode = innerCastNode.rhs;
        if (innerTypeNode == null || innerTypeNode.type != ExpressionNode.CONSTANT || !isNumericKeyword(innerTypeNode.token)) {
            return;
        }

        ExpressionNode typeNode = node.rhs;
        if (typeNode == null || typeNode.type != ExpressionNode.CONSTANT || typeNode.token.length() < 7 || !startsWithDecimalKeyword(typeNode.token)) {
            return;
        }

        // At this point, we know that the expression is compatible with our rewrite.
        node.lhs = innerCastNode.lhs;
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

    private CharSequence setModelAliasAndGetOptTok(GenericLexer lexer, IQueryModel joinModel) throws SqlException {
        CharSequence tok = optTok(lexer);
        if (tok != null && tableAliasStop.excludes(tok)) {
            if (isAsKeyword(tok)) {
                tok = tok(lexer, "alias");
            }
            if (tok.isEmpty() || isEmptyAlias(tok)) {
                throw SqlException.position(lexer.lastTokenPosition()).put("Empty table alias");
            }
            assertNameIsQuotedOrNotAKeyword(tok, lexer.lastTokenPosition());
            joinModel.setAlias(literal(lexer, tok));
            tok = optTok(lexer);
        }
        return tok;
    }

    private CharSequence setModelAliasAndTimestamp(GenericLexer lexer, IQueryModel model) throws SqlException {
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
            return ColumnType.encodeArrayType(ColumnType.tagOf(columnType), nDims);
        }

        final short typeTag = ColumnType.tagOf(columnType);
        if (typeTag == ColumnType.GEOHASH) {
            expectTok(lexer, '(');
            final int bits = GeoHashUtil.parseGeoHashBits(lexer.lastTokenPosition(), 0, expectLiteral(lexer).token);
            expectTok(lexer, ')');
            return ColumnType.getGeoHashTypeWithBits(bits);
        } else if (typeTag == ColumnType.DECIMAL) {
            return parseDecimalColumnType(lexer);
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

    private void validateMatViewQuery(IQueryModel model, String baseTableName) throws SqlException {
        for (IQueryModel m = model; m != null; m = m.getNestedModel()) {
            tableNames.clear();
            tableNamePositions.clear();
            SqlUtil.collectAllTableNames(m, tableNames, null);
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
                    if (column.isWindowExpression()) {
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

            final ObjList<IQueryModel> joinModels = m.getJoinModels();
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final IQueryModel joinModel = joinModels.getQuick(i);
                if (joinModel == m) {
                    continue;
                }
                validateMatViewQuery(joinModel, baseTableName);
            }

            final IQueryModel unionModel = m.getUnionModel();
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

    private void validateNamedWindowReferences(IQueryModel model) throws SqlException {
        LowerCaseCharSequenceObjHashMap<WindowExpression> namedWindows = model.getNamedWindows();
        ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            QueryColumn qc = columns.getQuick(i);
            if (qc.isWindowExpression()) {
                WindowExpression wc = (WindowExpression) qc;
                if (wc.isNamedWindowReference() && namedWindows.keyIndex(wc.getWindowName()) > -1) {
                    throw SqlException.$(wc.getWindowNamePosition(), "window '").put(wc.getWindowName()).put("' is not defined");
                }
            }
            // Check nested expression trees for all columns, not just window expressions,
            // to catch cases like: row_number() OVER w + 1 (where top-level column is +)
            validateNamedWindowReferencesInExpr(qc.getAst(), namedWindows);
        }
    }

    private void validateNamedWindowReferencesInExpr(ExpressionNode node, LowerCaseCharSequenceObjHashMap<WindowExpression> namedWindows) throws SqlException {
        if (node == null) {
            return;
        }
        if (node.windowExpression != null && node.windowExpression.isNamedWindowReference()) {
            CharSequence name = node.windowExpression.getWindowName();
            if (namedWindows.keyIndex(name) > -1) {
                throw SqlException.$(node.windowExpression.getWindowNamePosition(), "window '").put(name).put("' is not defined");
            }
        }
        if (node.paramCount < 3) {
            validateNamedWindowReferencesInExpr(node.lhs, namedWindows);
            validateNamedWindowReferencesInExpr(node.rhs, namedWindows);
        } else {
            for (int i = 0, n = node.paramCount; i < n; i++) {
                validateNamedWindowReferencesInExpr(node.args.getQuick(i), namedWindows);
            }
        }
    }

    static void validateIdentifier(GenericLexer lexer, CharSequence tok) throws SqlException {
        if (tok == null || tok.isEmpty()) {
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
        windowExpressionPool.clear();
        createViewOperationBuilder.clear();
        createMatViewOperationBuilder.clear();
        createTableOperationBuilder.clear();
        createTableColumnModelPool.clear();
        renameTableModelPool.clear();
        withClauseModelPool.clear();
        compileViewModelPool.clear();
        subQueryMode = false;
        createTableMode = false;
        copyMode = false;
        createViewMode = false;
        characterStore.clear();
        insertModelPool.clear();
        pivotQueryColumnPool.clear();
        expressionTreeBuilder.reset();
        copyModelPool.clear();
        topLevelWithModel.clear();
        explainModelPool.clear();
        viewLexers.clear();
        digit = 1;
        traversalAlgo.clear();
        tempCharSequenceSet.clear();
        aliasMap.clear();
        aliasSequenceMap.clear();
        pivotAliasMap.clear();
        clearRecordedViews();
    }

    ExpressionNode expr(
            GenericLexer lexer,
            IQueryModel model,
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

    ExpressionNode expr(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback, @Nullable LowerCaseCharSequenceObjHashMap<ExpressionNode> decls) throws SqlException {
        return expr(lexer, model, sqlParserCallback, decls, null);
    }

    ExpressionNode expr(GenericLexer lexer, IQueryModel model, SqlParserCallback sqlParserCallback) throws SqlException {
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

        if (isCompileKeyword(tok)) {
            return parseCompileView(lexer);
        }

        if (isFromKeyword(tok)) {
            throw SqlException.$(lexer.lastTokenPosition(), "Did you mean 'select * from'?");
        }

        return parseSelect(lexer, sqlParserCallback, null);
    }

    IQueryModel parseAsSubQuery(
            GenericLexer lexer,
            @Nullable LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses,
            boolean useTopLevelWithClauses,
            SqlParserCallback sqlParserCallback,
            LowerCaseCharSequenceObjHashMap<ExpressionNode> decls,
            boolean overrideDeclare
    ) throws SqlException {
        IQueryModel model;
        this.subQueryMode = true;
        try {
            model = parseDml(lexer, withClauses, lexer.getPosition(), useTopLevelWithClauses, sqlParserCallback, decls, overrideDeclare);
        } finally {
            this.subQueryMode = false;
        }
        return model;
    }

    String parseViewSql(GenericLexer lexer, SqlParserCallback sqlParserCallback) throws SqlException {
        int startOfQuery = lexer.getPosition();
        CharSequence tok = tok(lexer, "'(' or 'with' or 'select'");
        boolean enclosedInParentheses = Chars.equals(tok, '(');
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
        parseAsSubQuery(lexer, null, true, sqlParserCallback, null, false);
        final int endOfQuery = enclosedInParentheses ? lexer.getPosition() - 1 : lexer.getPosition();

        final String viewSql = Chars.toString(lexer.getContent(), startOfQuery, endOfQuery);

        if (enclosedInParentheses) {
            expectTok(lexer, ')');
        }
        tok = optTok(lexer);
        if (tok != null && !Chars.equals(tok, ';')) {
            throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
        }
        return viewSql;
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
        tableAliasStop.add(",");
        tableAliasStop.add(")");
        tableAliasStop.add(";");
        tableAliasStop.add("union");
        tableAliasStop.add("group");
        tableAliasStop.add("except");
        tableAliasStop.add("intersect");
        tableAliasStop.add("from");
        tableAliasStop.add("pivot");
        tableAliasStop.add("tolerance");
        tableAliasStop.add("right");
        tableAliasStop.add("full");
        tableAliasStop.add("range");
        tableAliasStop.add("window");
        tableAliasStop.add("horizon");
        tableAliasStop.add("unnest");
        //
        columnAliasStop.add("from");
        columnAliasStop.add(",");
        columnAliasStop.add("over");
        columnAliasStop.add("union");
        columnAliasStop.add("except");
        columnAliasStop.add("intersect");
        columnAliasStop.add(")");
        columnAliasStop.add(";");
        columnAliasStop.add("FOR");
        //
        groupByStopSet.add("order");
        groupByStopSet.add(")");
        groupByStopSet.add(",");

        joinStartSet.put("left", IQueryModel.JOIN_INNER);
        joinStartSet.put("right", IQueryModel.JOIN_INNER);
        joinStartSet.put("full", IQueryModel.JOIN_INNER);
        joinStartSet.put("join", IQueryModel.JOIN_INNER);
        joinStartSet.put("inner", IQueryModel.JOIN_INNER);
        joinStartSet.put("left", IQueryModel.JOIN_LEFT_OUTER);
        joinStartSet.put("window", IQueryModel.JOIN_WINDOW);
        joinStartSet.put("right", IQueryModel.JOIN_RIGHT_OUTER);
        joinStartSet.put("full", IQueryModel.JOIN_FULL_OUTER);
        joinStartSet.put("cross", IQueryModel.JOIN_CROSS);
        joinStartSet.put("asof", IQueryModel.JOIN_ASOF);
        joinStartSet.put("splice", IQueryModel.JOIN_SPLICE);
        joinStartSet.put("lt", IQueryModel.JOIN_LT);
        joinStartSet.put("horizon", IQueryModel.JOIN_HORIZON);
        joinStartSet.put("lateral", IQueryModel.JOIN_LATERAL_CROSS);
        joinStartSet.put(",", IQueryModel.JOIN_CROSS);
        //
        setOperations.add("union");
        setOperations.add("except");
        setOperations.add("intersect");
        //
        pivotForStop.add("group");
        pivotForStop.add(";");
        pivotForStop.add(")");
        pivotForStop.add("order");
        pivotForStop.add("limit");
    }
}

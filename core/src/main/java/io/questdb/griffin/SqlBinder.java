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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.griffin.BindContext.blockGroupBy;
import static io.questdb.griffin.BindContext.getColumnIndexQuiet;
import static io.questdb.griffin.BindContext.hasColumnReference;
import static io.questdb.griffin.BindContext.hasComputedProjection;
import static io.questdb.griffin.BindContext.isRowCount;
import static io.questdb.griffin.BindContext.isWildcard;
import static io.questdb.griffin.BindContext.joinColumnSource;
import static io.questdb.griffin.BindContext.sourceAlias;
import static io.questdb.griffin.LateralBinder.hasCorrelatedColumns;
import static io.questdb.griffin.OrderBinder.hasComputedOrder;
import static io.questdb.griffin.OrderBinder.hasProjectedOrder;
import static io.questdb.griffin.OrderBinder.isRowCountOrder;
import static io.questdb.griffin.OrderBinder.isSelectedOrder;
import static io.questdb.griffin.TemporalJoinBinder.horizonJoinIndex;
import static io.questdb.griffin.TemporalJoinBinder.validateTimestampOffset;
import static io.questdb.griffin.TemporalJoinBinder.validateTimestampOffsetUnit;
import static io.questdb.griffin.TemporalJoinBinder.windowJoinIndex;

final class SqlBinder implements Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(SqlBinder.class);
    private static final int MAX_RETAINED_SUBQUERY_DEPTH = 8;
    final BindContext ctx;
    final JoinBinder joinBinder;
    final LateralBinder lateralBinder;
    final SampleByBinder sampleByBinder;
    final TemporalJoinBinder temporalJoinBinder;
    private final AggregateBinder aggregateBinder;
    private final SqlCompilerImpl compiler;
    private final StringSink deferredErrorMessage = new StringSink();
    private final ObjList<CharSequence> hintAliases = new ObjList<>();
    private final ObjList<QueryModel> hintAliasModels = new ObjList<>();
    private final ObjectPool<LowerCaseCharSequenceObjHashMap<CharSequence>> hintScopes = new ObjectPool<>(LowerCaseCharSequenceObjHashMap::new, 4);
    private final ObjectPool<LatestByPlan> latestByPlans = new ObjectPool<>(LatestByPlan.FACTORY, 4);
    private final OrderBinder orderBinder;
    private final IntList outputColumnPositions = new IntList();
    private final PivotBinder pivotBinder;
    private final ObjectPool<ScanPlan> scans = new ObjectPool<>(ScanPlan.FACTORY, 4);
    private final ObjectPool<SetOperationPlan> setOperations = new ObjectPool<>(SetOperationPlan.FACTORY, 4);
    private final IntList setTypes = new IntList();
    private final ObjList<SqlBinder> subqueryBinders = new ObjList<>();
    private final ObjList<RecordCursorFactory> subqueryFactories = new ObjList<>();
    private final TranslatingAliases translatingAliases = new TranslatingAliases();
    private final ObjList<CharSequence> updateTableColumnNames = new ObjList<>();
    private final IntList updateTableColumnTypes = new IntList();
    private final ObjList<CharSequence> updateTargetNames = new ObjList<>();
    private final WindowBinder windowBinder;
    private final LongList withinPrefixes = new LongList();
    private int deferredErrorPosition = -1;
    private boolean isUpdate;
    private SqlParserCallback parserCallback;
    private LogicalPlan predicateSource;
    private LogicalPlan root;
    private int scalarBoundDepth;
    private int subqueryCount;
    private long updateMetadataVersion;
    private int updateTableId;
    private CharSequence updateTableName;
    private int updateTablePosition;
    private TableToken updateTableToken;
    private int updateTimestampIndex;

    SqlBinder(CairoConfiguration configuration, FunctionParser functionParser, SqlCompilerImpl compiler) {
        this.compiler = compiler;
        this.ctx = new BindContext(configuration, functionParser, compiler.getSqlNodePool(), compiler.getEmptySchema());
        ctx.functionBinder.setSubqueryBinder(this);
        this.orderBinder = new OrderBinder(ctx, this);
        this.aggregateBinder = new AggregateBinder(ctx, this, orderBinder);
        this.windowBinder = new WindowBinder(ctx, this, aggregateBinder, compiler.getAsm(), compiler.getEntityColumnFilter());
        this.lateralBinder = new LateralBinder(ctx, this, windowBinder, orderBinder, aggregateBinder);
        this.joinBinder = new JoinBinder(ctx, this, lateralBinder, compiler.getScratchIds());
        this.sampleByBinder = new SampleByBinder(ctx, this, windowBinder, orderBinder, aggregateBinder, joinBinder);
        this.temporalJoinBinder = new TemporalJoinBinder(ctx, this, orderBinder, aggregateBinder, joinBinder, lateralBinder);
        this.pivotBinder = new PivotBinder(ctx, this, windowBinder, temporalJoinBinder, aggregateBinder, joinBinder, lateralBinder,
                compiler.getScratchSink());
    }

    @Override
    public void clear() {
        latestByPlans.clear();
        scans.clear();
        setOperations.clear();
        updateTableColumnNames.clear();
        updateTableColumnTypes.clear();
        updateMetadataVersion = 0;
        updateTableId = 0;
        updateTableName = null;
        updateTablePosition = 0;
        updateTableToken = null;
        updateTimestampIndex = -1;
        ctx.clear();
        root = null;
        deferredErrorPosition = -1;
        windowBinder.clear();
        temporalJoinBinder.clear();
        sampleByBinder.clear();
        aggregateBinder.clear();
        pivotBinder.clear();
        orderBinder.clear();
        joinBinder.clear();
        lateralBinder.clear();
        hintAliasModels.clear();
        hintAliases.clear();
        for (int i = 0, n = hintScopes.getPos(); i < n; i++) {
            hintScopes.peekQuick(i).clear();
        }
        hintScopes.clear();
        outputColumnPositions.clear();
        setTypes.clear();
        isUpdate = false;
        updateTargetNames.clear();
        predicateSource = null;
        parserCallback = null;
        final Throwable failure = clearSubqueries(null);
        if (failure != null) {
            LOG.error().$("could not free subquery resources [error=").$(failure).I$();
        }
    }

    @Override
    public void close() {
        clear();
        Misc.freeObjListAndClear(subqueryBinders);
    }

    @TestOnly
    public int getRetainedSubqueryDepthForTesting() {
        int depth = 0;
        for (int i = 0, n = subqueryBinders.size(); i < n; i++) {
            depth = Math.max(depth, 1 + subqueryBinders.getQuick(i).getRetainedSubqueryDepthForTesting());
        }
        return depth;
    }

    private static LogicalPlan columnSource(LogicalPlan plan, int id) {
        while (true) {
            switch (plan) {
                case ProjectPlan project -> {
                    final int index = project.getOutput().getColumnIndexById(id);
                    if (index < 0) {
                        throw new IllegalStateException("predicate column is outside its projection");
                    }
                    if (!(project.getExpressions().getQuick(index) instanceof ColumnExpression column)) {
                        return plan;
                    }
                    id = column.getColumnId();
                    plan = project.getInput();
                }
                case FilterPlan _, SortPlan _ -> plan = plan.inputAt(0);
                case JoinPlan join -> {
                    final JoinInput source = join.getInputs().getQuick(joinColumnSource(join, id));
                    if (source.getUnnest() != null) {
                        return plan;
                    }
                    plan = source.getInput();
                }
                default -> {
                    // LIMIT, grouping, DISTINCT and set operators define their own
                    // predicate scope. Existing timestamp provenance also stops here.
                    return plan;
                }
            }
        }
    }

    /** Makes a computed column's alias resolvable by later columns; a plain column alias names its input column. */
    private static void exposeProjectionReference(OutputSchema scope, ProjectPlan project, CharSequence alias, OutputSchema source) {
        if (scope == null || getColumnIndexQuiet(source, alias) >= 0 || getColumnIndexQuiet(scope, alias) >= 0) {
            return;
        }
        final int index = project.getExpressions().size() - 1;
        final BoundExpression expression = project.getExpressions().getQuick(index);
        final int columnId = expression instanceof ColumnExpression column ? column.getColumnId() : project.getOutput().getColumnId(index);
        scope.add(columnId, GenericLexer.unquote(alias), expression.getDataType(), true);
    }

    private static ExpressionNode firstColumnReference(ExpressionNode expression) {
        if (expression == null || expression.type == ExpressionNode.LITERAL) {
            return expression;
        }
        if (expression.paramCount < 3) {
            final ExpressionNode left = firstColumnReference(expression.lhs);
            return left != null ? left : firstColumnReference(expression.rhs);
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            final ExpressionNode column = firstColumnReference(expression.args.getQuick(i));
            if (column != null) {
                return column;
            }
        }
        return null;
    }

    private static int getOutputColumnPosition(LogicalPlan output, int index) {
        while (output.getType() == LogicalPlan.Type.LIMIT || output.getType() == LogicalPlan.Type.SORT
                || output.getType() == LogicalPlan.Type.SET_OPERATION || output.getType() == LogicalPlan.Type.DISTINCT) {
            output = output.inputAt(0);
        }
        return switch (output) {
            case AggregatePlan aggregate when aggregate.getType() == LogicalPlan.Type.AGGREGATE -> aggregate.getAggregates().getQuick(index).getPosition();
            case ProjectPlan project -> project.getExpressions().get(index).getPosition();
            default -> throw new IllegalStateException("output column is neither aggregated nor projected");
        };
    }

    private static boolean hasComputedColumn(QueryModel model) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (model.getBottomUpColumns().getQuick(i).getAst().type != ExpressionNode.LITERAL) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasSameCorrelation(OutputSchema left, OutputSchema right) {
        if (left.getCorrelatedAliasCount() != right.getCorrelatedAliasCount() || left.getColumnCount() != right.getColumnCount()) {
            return false;
        }
        for (int i = 0, n = left.getCorrelatedAliasCount(); i < n; i++) {
            final CharSequence name = left.getCorrelatedAliasName(i);
            if (right.getCorrelatedColumnIndexQuiet(left.getCorrelatedAliasQualifier(i), name, 0, name.length()) != left.getCorrelatedAliasIndex(i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isProjectionReference(ExpressionNode expression, OutputSchema source, OutputSchema scope) {
        return Chars.indexOfLastUnquoted(expression.token, '.') < 0 && getColumnIndexQuiet(source, expression.token) < 0
                && getColumnIndexQuiet(scope, expression.token) >= 0;
    }

    private static boolean isReferenced(ProjectPlan project, int columnId) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (FunctionBinder.references(project.getExpressions().getQuick(i), columnId)) {
                return true;
            }
        }
        return false;
    }

    private static void validateLatestByColumn(int columnType, int position, CharSequence name) throws SqlException {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.CHAR, ColumnType.SHORT, ColumnType.INT, ColumnType.IPv4,
                 ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.FLOAT, ColumnType.DOUBLE,
                 ColumnType.LONG256, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.VARCHAR_SLICE, ColumnType.SYMBOL,
                 ColumnType.UUID, ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG,
                 ColumnType.LONG128 -> {
            }
            default -> throw SqlException.position(position).put(name).put(" (").put(ColumnType.nameOf(columnType))
                    .put("): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON");
        }
    }

    private static void validateStandaloneConjunct(ExpressionNode expression, BoundExpression predicate) throws SqlException {
        final int type = predicate.getDataType();
        if (type != ColumnType.BOOLEAN && type != ColumnType.NULL) {
            throw SqlException.$(expression.position, "boolean expression expected");
        }
    }

    private int addCursorNode(ExpressionNode node, CharSequence name) {
        int index = -1;
        for (int i = 0, n = ctx.cursorSources.size(); i < n && index < 0; i++) {
            if (ExpressionNode.compareNodesExact(ctx.cursorSources.getQuick(i), node)) {
                index = i;
            }
        }
        if (index < 0) {
            index = ctx.cursorSources.size();
            ctx.cursorSources.add(node);
            final CharSequence base = name != null ? name : node.token;
            final int dot = Chars.indexOfLastUnquoted(base, '.');
            ctx.cursorNames.add(dot < 0 ? base : base.subSequence(dot + 1, base.length()));
        }
        ctx.cursorNodes.add(node);
        ctx.cursorSourceIndexes.add(index);
        return index;
    }

    /**
     * Joins each distinct SELECT-list cursor call once and maps every
     * occurrence of the call to the RECORD column that carries the joined row.
     */
    private LogicalPlan bindCursorColumns(LogicalPlan input, CharSequence inputAlias, int position, SqlExecutionContext executionContext) throws SqlException {
        final JoinPlan join;
        if (input instanceof JoinPlan inputJoin) {
            join = inputJoin;
        } else {
            join = ctx.joins.next().of(position);
            join.getInputs().add(ctx.joinInputs.next().of(input, QueryModel.JOIN_CROSS, inputAlias, position));
            join.getOrderedInputs().add(join.getInputs().getLast());
            joinBinder.addJoinOutput(join, input.getOutput(), inputAlias);
            join.getOutput().setTimestampIndex(input.getOutput().getTimestampIndex());
        }
        for (int i = 0, n = ctx.cursorSources.size(); i < n; i++) {
            final CharSequence name = uniqueCursorName(ctx.cursorNames.getQuick(i), join.getOutput());
            ctx.cursorNames.setQuick(i, name);
            final FunctionSourcePlan record = ctx.functionSources.bindRecord(ctx.cursorSources.getQuick(i), name, executionContext, ctx.nextColumnId++);
            final CharacterStoreEntry entry = ctx.characterStore.newEntry();
            entry.put(QueryModel.SUB_QUERY_ALIAS_PREFIX).put(i + 1);
            final CharSequence alias = entry.toImmutable();
            final JoinInput step = ctx.joinInputs.next().of(record, QueryModel.JOIN_CROSS, alias, position);
            join.getInputs().add(step);
            join.getOrderedInputs().add(step);
            joinBinder.addJoinOutput(join, record.getOutput(), alias);
        }
        for (int i = 0, n = ctx.cursorNodes.size(); i < n; i++) {
            final int columnIndex = getColumnIndexQuiet(join.getOutput(), ctx.cursorNames.getQuick(ctx.cursorSourceIndexes.getQuick(i)));
            ctx.cursorColumns.add(ctx.columns.next().of(join.getOutput().getColumnId(columnIndex), ColumnType.RECORD, ctx.cursorNodes.getQuick(i).position));
        }
        return join;
    }

    private BoundExpression bindFilterConjunct(ExpressionNode expression, LogicalPlan input, CharSequence alias,
                                               boolean isAndArgument, SqlExecutionContext executionContext) throws SqlException {
        if (expression.paramCount == 2 && SqlKeywords.isAndKeyword(expression.token)) {
            final BoundExpression left = bindFilterConjunct(expression.lhs, input, alias, true, executionContext);
            final BoundExpression right = bindFilterConjunct(expression.rhs, input, alias, true, executionContext);
            return ctx.functionBinder.combineConjunction(left, right, expression.position);
        }
        if (distributeSetFilter(expression, input, input.getOutput(), alias, executionContext)) {
            return ctx.constants.next().ofBoolean(true, expression.position);
        }
        ctx.joinNativeTimestampIds.clear();
        if (hasSinglePredicateSource(expression, input, alias)) {
            ctx.copyTimestampScope(input.getOutput());
        }
        final BoundExpression bound = ctx.functionBinder.bindPredicate(expression, input.getOutput(), alias,
                ctx.joinNativeTimestampIds, isAndArgument ? ColumnType.BOOLEAN : ColumnType.UNDEFINED, executionContext);
        if (!isAndArgument) {
            return bound;
        }
        final BoundExpression predicate = ctx.functionBinder.toBooleanSubquery(bound);
        if (!hasColumnReference(expression)) {
            validateStandaloneConjunct(expression, predicate);
        }
        return predicate;
    }

    private LogicalPlan bindFunctionSource(ExpressionNode expression, SqlExecutionContext executionContext) throws SqlException {
        final LogicalPlan sourcePlan = ctx.functionSources.bind(expression, executionContext, ctx.nextColumnId);
        ctx.nextColumnId += sourcePlan.getOutput().getColumnCount();
        return sourcePlan;
    }

    private LogicalPlan bindQueryBlock(QueryModel model, SqlExecutionContext executionContext, boolean isOrderAndLimitEnabled) throws SqlException {
        final LowerCaseCharSequenceObjHashMap<CharSequence> previousHints = ctx.currentHints;
        ctx.currentHints = mergeHints(model, ctx.currentHints);
        if (model.getNestedModel() != null) {
            ctx.currentHints = mergeHints(model.getNestedModel(), ctx.currentHints);
        }
        try {
            return bindQueryBlockWithHints(model, executionContext, isOrderAndLimitEnabled);
        } finally {
            ctx.currentHints = previousHints;
        }
    }

    private LogicalPlan bindQueryBlockWithHints(QueryModel model, SqlExecutionContext executionContext, boolean isOrderAndLimitEnabled) throws SqlException {
        if (model.isPivot() && model.getBottomUpColumns().size() == 0) {
            return pivotBinder.rewritePivot(model, executionContext);
        }
        final QueryModel source = model.getNestedModel();
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (model.getBottomUpColumns().getQuick(i) instanceof WindowExpression window
                    && window.getAst() != null && window.getAst().windowExpression == null) {
                window.getAst().windowExpression = window;
            }
        }
        if (source == null) {
            throw new IllegalStateException("query block has no source");
        }
        final ExpressionNode subsample = source.getSubsample();
        if (subsample != null && Chars.equalsIgnoreCase(subsample.token, "sdt")) {
            if (ctx.isInsideJoin || source.getJoinModels().size() > 1) {
                throw SqlException.$(subsample.position, "SUBSAMPLE sdt is not supported inside a join");
            }
            if (subsample.paramCount != 2) {
                throw SqlException.$(subsample.position, "sdt() requires exactly 2 arguments: column and compdev");
            }
            if (model.isDistinct() || aggregateBinder.hasAggregation(model, source)) {
                throw SqlException.$(subsample.position, "SUBSAMPLE sdt is not supported in an aggregation context");
            }
        }
        windowBinder.validateWindowClauses(model);
        windowBinder.validateWindowClauses(source);
        for (int i = 1, n = source.getJoinModels().size(); i < n; i++) {
            windowBinder.validateWindowClauses(source.getJoinModels().getQuick(i));
        }
        windowBinder.validateNamedWindows(model);
        windowBinder.validateNamedWindows(source);
        ExpressionNode where = source.getWhereClause() == null ? null
                : SqlUtil.optimiseBooleanNot(ExpressionNode.deepClone(ctx.bindingExpressions, source.getWhereClause()), ctx.bindingExpressions);
        if (windowJoinIndex(source) > 0) {
            return temporalJoinBinder.bindWindowJoinQuery(model, source, where, isOrderAndLimitEnabled, executionContext);
        }
        final boolean isHorizonJoin = horizonJoinIndex(source) > 0;
        final int liftedSelectionBase = lateralBinder.liftedSelectionColumns.size();
        LogicalPlan sourcePlan;
        if (isHorizonJoin) {
            sourcePlan = temporalJoinBinder.bindHorizonJoin(model, source, where, executionContext);
        } else if (source.getJoinModels().size() > 1) {
            final QueryModel previousHoistSource = lateralBinder.lateralHoistSource;
            lateralBinder.lateralHoistSource = source;
            try {
                sourcePlan = joinBinder.bindJoins(model, source, where, executionContext);
            } finally {
                lateralBinder.lateralHoistSource = previousHoistSource;
            }
        } else {
            sourcePlan = bindSource(model, source, executionContext);
        }
        final ExpressionNode hoistedWhere = lateralBinder.lateralHoistedWhere;
        lateralBinder.lateralHoistedWhere = null;
        if (isHorizonJoin) {
            where = null;
        }
        if (ctx.lateralScopes.size() > 0 && source.getJoinModels().size() == 1) {
            sourcePlan = lateralBinder.correlateSource(model, source, sourceAlias(source), sourcePlan, where, executionContext);
            where = ctx.correlatedWhere;
        }
        where = lateralBinder.substituteLateralCounts(where, sourcePlan.getOutput());
        ctx.promoteNoArgFunctions(model, sourcePlan.getOutput(), source.getJoinModels().size() > 1 ? null : sourceAlias(source));
        final boolean isSampleByCursor = source.getSampleBy() != null && SampleByBinder.requiresSampleByCursor(source);
        final SampleByPlan sampleBy = isSampleByCursor ? sampleByBinder.bindSampleBy(model, source, sourcePlan, executionContext) : null;
        final BoundExpression sampleByBucket = source.getSampleBy() == null || isSampleByCursor ? null
                : sampleByBinder.bindSampleByBucket(model, source, sourcePlan.getOutput(), executionContext);
        ctx.aliases.clear();
        ctx.aliasSequences.clear();
        ctx.projectionAliasIndexes.clear();

        final LatestByPlan latest = source.getLatestBy().size() > 0 ? bindLatestBy(sourcePlan, source) : null;
        LogicalPlan input = sourcePlan;
        if (where != null && latest != null && sourcePlan.getType() == LogicalPlan.Type.SCAN && ctx.configuration.useWithinLatestByOptimisation()) {
            withinPrefixes.clear();
            validateWithin(where, sourcePlan.getOutput(), sourceAlias(source), executionContext);
        }
        if (where != null && sourcePlan.getType() != LogicalPlan.Type.JOIN) {
            BoundExpression predicate;
            collectKeySubqueryColumns(sourcePlan, latest, executionContext);
            try {
                predicate = bindPredicate(where, sourcePlan, source, executionContext);
            } catch (SqlException e) {
                // The enclosing select list validates before a joined source's filter.
                if (!ctx.isInsideJoin) {
                    throw e;
                }
                if (deferredErrorPosition < 0) {
                    deferredErrorPosition = e.getPosition();
                    deferredErrorMessage.clear();
                    deferredErrorMessage.put(e.getFlyweightMessage());
                }
                predicate = null;
            } finally {
                ctx.functionBinder.getKeySubqueryColumnIds().clear();
            }
            if (predicate != null && (!(predicate instanceof ConstantExpression constant) || constant.getLongValue() == 0)) {
                final FilterPlan filter = ctx.filters.next().of(input, predicate, predicate.getPosition());
                filter.getOutput().copyFrom(input.getOutput());
                input = filter;
            }
        }

        if (source.getSampleBy() != null) {
            input = sampleByBinder.bindSampleByRange(source, input, executionContext);
        }

        if (latest != null) {
            if (ctx.lateralScopes.size() > 0 && sourcePlan.getOutput().getCorrelatedAliasCount() > 0) {
                input = windowBinder.bindLatestWindow(input, latest, model, source.getModelPosition(), executionContext);
            } else {
                latest.replaceInput(0, input);
                input = latest;
            }
        }

        if (!isHorizonJoin) {
            aggregateBinder.validateGroupByKeys(model, source);
        }
        windowBinder.validateWindowOrder(model, source);
        final boolean hasWindows = windowBinder.hasWindows(model, source);
        if (hasWindows && model.isUpdate()) {
            throw windowBinder.updateWindowException(model);
        }
        windowBinder.validateWindowAggregation(model, source, isOrderAndLimitEnabled);
        if (hasWindows) {
            input = windowBinder.bindWindows(model, source, input, executionContext);
            if (sampleByBucket != null && windowBinder.isAggregationFree(model, source)) {
                throw SqlException.$(source.getSampleBy().position, "at least one aggregation function must be present in 'select' clause");
            }
            sourcePlan = input;
            ctx.aliases.clear();
            ctx.aliasSequences.clear();
        }

        if (!model.isDistinct() && !model.isUpdate() && model.getBottomUpColumns().size() == 1
                && !model.getBottomUpColumns().getQuick(0).isWindowExpression()
                && source.getSampleBy() == null
                && blockGroupBy(model, source).size() == 0
                && !hasCorrelatedColumns(input.getOutput())
                && isRowCount(model.getBottomUpColumns().getQuick(0).getAst())
                && isRowCountOrder(model.getBottomUpColumns().getQuick(0), source)) {
            final LogicalPlan counted = orderBinder.bindRowCount(model, source, input,
                    isOrderAndLimitEnabled && source.getSubsample() == null, executionContext);
            return source.getSubsample() == null ? counted : sampleByBinder.bindSubsample(counted, sourcePlan, source, executionContext);
        }

        if (aggregateBinder.hasAggregation(model, source) || isHorizonJoin || model.isDistinct() && !hasWindows && aggregateBinder.isDistinctRewritten(model, source, isOrderAndLimitEnabled)) {
            if (model.isUpdate()) {
                throw aggregateBinder.updateAggregateException(model);
            }
            if (!hasWindows && !isHorizonJoin && collectCursorColumns(model)) {
                input = bindCursorColumns(input, sourceAlias(source), model.getModelPosition(), executionContext);
            }
            final LogicalPlan aggregated = aggregateBinder.bindAggregation(model, source, input,
                    isOrderAndLimitEnabled && source.getSubsample() == null, hasWindows ? ctx.windowSelectExpressions : null,
                    sampleByBucket, sampleBy, executionContext);
            return source.getSubsample() == null ? aggregated
                    : sampleByBinder.bindSubsampleOutput(model, source, sourcePlan, aggregated, isOrderAndLimitEnabled, executionContext);
        }

        if (subsample != null && !hasWindows && source.getSampleBy() == null && !sampleByBinder.hasSubsampleTimestampProjection(model, source, sourcePlan)) {
            sampleByBinder.validateSubsampleCall(subsample);
            if (!Chars.equalsIgnoreCase(subsample.token, "sdt") && !Chars.equalsIgnoreCase(subsample.token, "uniform")
                    && !Chars.equalsIgnoreCase(subsample.token, "cadence")) {
                sampleByBinder.validateSubsampleSelectValue(subsample.args.getQuick(0), model);
            }
            throw sampleByBinder.subsampleTimestampMissing(source, sourcePlan);
        }
        if (!hasWindows && !model.isUpdate() && collectCursorColumns(model)) {
            input = bindCursorColumns(input, sourceAlias(source), model.getModelPosition(), executionContext);
            sourcePlan = input;
        }
        final ProjectPlan project = ctx.projects.next().of(input, model.getModelPosition());
        ctx.sourceProjectionIndexes.setAll(sourcePlan.getOutput().getColumnCount(), -1);
        final OutputSchema referenceScope = !hasWindows && !model.isUpdate() && hasProjectionReferences(model, sourcePlan.getOutput())
                ? projectionReferenceScope(sourcePlan.getOutput()) : null;
        // Qualified sources can hold distinct columns of one name. A virtual projection names a
        // plain column after its translating alias, which an earlier function argument may take.
        final TranslatingAliases translating = !hasWindows && !model.isUpdate() && referenceScope == null
                && sourcePlan.getOutput().hasColumnQualifiers() && hasComputedColumn(model) ? translatingAliases.of() : null;
        if (!hasWindows && !model.isUpdate()) {
            validateTimestampOffset(model, sourcePlan, sourceAlias(source));
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final QueryColumn column = model.getBottomUpColumns().getQuick(i);
            final ExpressionNode expression = lateralBinder.substituteLateralCounts(hasWindows ? ctx.windowSelectExpressions.getQuick(i) : column.getAst(),
                    sourcePlan.getOutput());
            if (!model.isUpdate() && isWildcard(expression)) {
                boolean hasMatch = false;
                for (int k = 0, count = sourcePlan.getOutput().getColumnCount(); k < count; k++) {
                    if (ctx.isWildcardColumn(expression, sourcePlan.getOutput(), k, sourceAlias(source))) {
                        final ExpressionNode compensated = lateralBinder.lateralCompensatedColumn(sourcePlan.getOutput(), k, expression.position);
                        if (compensated == null) {
                            ctx.addProjection(project, sourcePlan.getOutput(), k, sourcePlan.getOutput().getColumnName(k), expression.position, true);
                        } else {
                            ctx.addProjection(project, ctx.functionBinder.bind(compensated, sourcePlan.getOutput(), sourceAlias(source), ColumnType.STRING, executionContext),
                                    null, sourcePlan.getOutput().getColumnName(k), true);
                        }
                        hasMatch = true;
                    }
                }
                if (!hasMatch && Chars.indexOfLastUnquoted(expression.token, '.') >= 0) {
                    throw SqlException.$(expression.position, "invalid table alias");
                }
            } else if (expression.type != ExpressionNode.LITERAL) {
                final int cursorSource = i < ctx.cursorProjectionSources.size() ? ctx.cursorProjectionSources.getQuick(i) : -1;
                CharSequence name = cursorSource < 0 ? column.getName() : ctx.cursorNames.getQuick(cursorSource);
                if (!model.isUpdate() && getColumnIndexQuiet(project.getOutput(), name) >= 0) {
                    throw SqlException.duplicateColumn(0, name);
                }
                final int targetIndex = model.isUpdate() ? getUpdateColumnIndex(name) : -1;
                final int preferredType = targetIndex >= 0 ? updateTableColumnTypes.getQuick(targetIndex) : ColumnType.STRING;
                final OutputSchema scope = referenceScope != null ? referenceScope
                        : hasWindows ? ctx.windowBindingScope(sourcePlan.getOutput()) : sourcePlan.getOutput();
                final BoundExpression bound = targetIndex >= 0
                        ? ctx.functionBinder.bindUpdateAssignment(expression, scope, sourceAlias(source), preferredType, executionContext)
                        : ctx.cursorNodes.size() == 0
                        ? ctx.functionBinder.bind(expression, scope, sourceAlias(source), preferredType, executionContext)
                        : ctx.functionBinder.bind(expression, scope, sourceAlias(source), preferredType, ctx.cursorNodes, ctx.cursorColumns, executionContext);
                if (!model.isUpdate() && ColumnType.isCursor(bound.getDataType())) {
                    throw SqlException.$(expression.position, "cursor function cannot be used as a column [column=").put(name).put(']');
                }
                if (model.isUpdate()) {
                    if (targetIndex >= 0) {
                        name = updateTableColumnNames.getQuick(targetIndex);
                    }
                }
                ctx.addProjection(project, bound, null, name, true);
                ctx.projectionAliasIndexes.add(project.getExpressions().size() - 1);
                exposeProjectionReference(referenceScope, project, column.getName(), sourcePlan.getOutput());
                if (translating != null) {
                    translating.addArguments(bound, sourcePlan.getOutput(), ctx.characterStore);
                }
            } else if (referenceScope != null && isProjectionReference(expression, sourcePlan.getOutput(), referenceScope)) {
                final int index = getColumnIndexQuiet(referenceScope, expression.token);
                ctx.addProjection(project, ctx.columns.next().of(referenceScope.getColumnId(index), referenceScope.getColumnType(index), expression.position),
                        null, column.getAlias() != null ? column.getAlias() : expression.token, true);
                ctx.projectionAliasIndexes.add(project.getExpressions().size() - 1);
            } else {
                final int aliasIndex = hasWindows && ctx.windowAliasIds.getQuick(i) >= 0
                        ? sourcePlan.getOutput().getColumnIndexById(ctx.windowAliasIds.getQuick(i)) : -1;
                final int index = aliasIndex >= 0 ? aliasIndex
                        : ctx.bindColumnIndex(expression, hasWindows ? ctx.windowBindingScope(sourcePlan.getOutput()) : sourcePlan.getOutput(), source);
                CharSequence name = column.getAlias() != null ? column.getAlias() : sourcePlan.getOutput().getColumnName(index);
                boolean requiresUpdateCast = false;
                if (model.isUpdate()) {
                    final int targetIndex = getUpdateColumnIndex(name);
                    // Preserve RHS binding errors before the existing target validator runs.
                    if (targetIndex >= 0) {
                        name = updateTableColumnNames.getQuick(targetIndex);
                        requiresUpdateCast = updateTableColumnTypes.getQuick(targetIndex) != sourcePlan.getOutput().getColumnType(index);
                    }
                }
                final int dot = Chars.indexOfLastUnquoted(expression.token, '.');
                final boolean isSourceAliasReusable = dot < 0
                        || Chars.equalsIgnoreCase(name, expression.token, dot + 1, expression.token.length());
                if (translating != null) {
                    name = translating.add(sourcePlan.getOutput().getColumnId(index), name, ctx.characterStore);
                }
                final boolean isTranslatingCopy = !isSourceAliasReusable && source.getJoinModels().size() == 1
                        && ctx.sourceProjectionIndexes.getQuick(index) < 0 && isReferenced(project, sourcePlan.getOutput().getColumnId(index));
                ctx.addProjection(project, sourcePlan.getOutput(), index, name, expression.position, isSourceAliasReusable);
                if (isTranslatingCopy) {
                    ctx.translatingCopyIds.add(project.getOutput().getColumnId(project.getOutput().getColumnCount() - 1));
                }
                exposeProjectionReference(referenceScope, project, column.getName(), sourcePlan.getOutput());
                if (requiresUpdateCast) {
                    project.getExpressions().setQuick(project.getExpressions().size() - 1,
                            ctx.functionBinder.bind(expression, sourcePlan.getOutput(), sourceAlias(source), executionContext));
                }
            }
        }
        if (source.getJoinModels().size() > 1) {
            throwDeferredError();
        }
        ctx.clearCursorColumns();
        lateralBinder.exposeCorrelated(project, sourcePlan.getOutput(), null);
        lateralBinder.exposeLifted(project, sourcePlan.getOutput());
        for (int i = liftedSelectionBase, n = lateralBinder.liftedSelectionColumns.size(); i < n; i++) {
            final int index = lateralBinder.liftedSelectionColumns.getQuick(i);
            lateralBinder.addLateralTemplate(project.getOutput().getColumnId(index), lateralBinder.liftedSelectionTemplates.getQuick(i),
                    ctx.qualifiedJoinName(lateralBinder.lateralBodyAlias, project.getOutput().getColumnName(index)));
        }
        lateralBinder.liftedSelectionColumns.setPos(liftedSelectionBase);
        lateralBinder.liftedSelectionTemplates.setPos(liftedSelectionBase);
        if (model == lateralBinder.lateralBodyModel) {
            for (int i = 0, n = lateralBinder.lateralWrappedCountColumns.size(); i < n; i++) {
                final int index = lateralBinder.lateralWrappedCountColumns.getQuick(i);
                final CharSequence self = ctx.qualifiedJoinName(lateralBinder.lateralBodyAlias, project.getOutput().getColumnName(index));
                lateralBinder.addLateralTemplate(project.getOutput().getColumnId(index),
                        lateralBinder.zeroCoalesce(ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, self, 0, model.getModelPosition())), self);
            }
            lateralBinder.lateralWrappedCountColumns.clear();
        }

        if (model.isUpdate()) {
            prepareUpdateAssignments(model, project);
        }

        if (source.getSubsample() != null) {
            orderBinder.designateTimestamp(project);
            if (model.isDistinct()) {
                final LogicalPlan distinct = aggregateBinder.bindDistinct(model, source, sourcePlan, project, false, executionContext);
                return sampleByBinder.bindSubsampleOutput(model, source, sourcePlan, distinct, isOrderAndLimitEnabled, executionContext);
            }
            final LogicalPlan sampled = isOrderAndLimitEnabled && source.getOrderBy().size() > 0
                    ? orderBinder.bindOutputOrder(model, sourcePlan, project, source, sourceAlias(source),
                    hasWindows ? ctx.windowOrderExpressions : null, executionContext)
                    : sampleByBinder.bindSubsample(project, sourcePlan, source, executionContext);
            return isOrderAndLimitEnabled ? orderBinder.bindLimit(sampled, model, executionContext) : sampled;
        }
        LogicalPlan result = model.isDistinct()
                ? aggregateBinder.bindDistinct(model, source, sourcePlan, project, isOrderAndLimitEnabled, executionContext)
                : isOrderAndLimitEnabled && (hasComputedOrder(project, source)
                || source.getOrderBy().size() > 0 && (hasComputedProjection(project) || sourcePlan.getType() == LogicalPlan.Type.JOIN
                || sourcePlan.getType() == LogicalPlan.Type.SET_OPERATION && isSelectedOrder(project, source, sourcePlan.getOutput(), sourceAlias(source)))
                || LogicalPlans.hasRepeatedColumn(project) && hasProjectedOrder(project, source)
                || lateralBinder.hasLateralCarrierOrder(source, sourcePlan.getOutput())
                || hoistedWhere != null && source.getOrderBy().size() > 0)
                  ? orderBinder.bindOutputOrder(model, sourcePlan, project, source, sourceAlias(source), hasWindows ? ctx.windowOrderExpressions : null, executionContext)
                  : isOrderAndLimitEnabled ? orderBinder.bindSourceOrder(sourcePlan, project, source, sourceAlias(source)) : orderBinder.designateTimestamp(project);
        if (hoistedWhere != null) {
            result = filterProjection(result, project, hoistedWhere, model, executionContext);
        }
        return isOrderAndLimitEnabled ? orderBinder.bindLimit(result, model, executionContext) : result;
    }

    private LogicalPlan bindQueryWithHints(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        if (model.getUnionModel() == null) {
            final LogicalPlan show = bindShow(model, executionContext);
            if (show != null) {
                return show;
            }
        }
        final boolean wasSetOperationBranch = ctx.isSetOperationBranch;
        ctx.isSetOperationBranch = model.getUnionModel() != null;
        try {
            return bindSetOperation(model, executionContext);
        } finally {
            ctx.isSetOperationBranch = wasSetOperationBranch;
        }
    }

    private ScanPlan bindScan(QueryModel model, QueryModel source, ExpressionNode tableName, TableRecordMetadata metadata) {
        final ScanPlan scan = scans.next().of(metadata.getTableToken(), metadata.getMetadataVersion(), tableName.position, source.isUpdate());
        scan.setHints(scanHints(source));
        final ExpressionNode view = source.getViewNameExpr();
        if (view != null) {
            scan.setView(Chars.toString(view.token), view.position);
        }
        if (source.isUpdate()) {
            updateTableName = tableName.token;
            updateTablePosition = tableName.position;
            updateTableToken = metadata.getTableToken();
            updateTableId = metadata.getTableId();
            updateMetadataVersion = metadata.getMetadataVersion();
            updateTimestampIndex = -1;
            updateTableColumnNames.clear();
            updateTableColumnTypes.clear();
        }
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int type = metadata.getColumnType(i);
            if (type > 0) {
                scan.getOutput().add(ctx.nextColumnId++, metadata.getColumnName(i), type, true);
                scan.getOutput().setSymbolTableStatic(scan.getOutput().getColumnCount() - 1, metadata.isSymbolTableStatic(i));
                scan.getSourceColumnIndexes().add(i);
                if (metadata.isColumnIndexed(i)) {
                    scan.getIndexedColumnIds().add(scan.getOutput().getColumnId(scan.getOutput().getColumnCount() - 1));
                }
                if (i == metadata.getTimestampIndex()) {
                    scan.getOutput().setTimestampIndex(scan.getOutput().getColumnCount() - 1);
                    scan.setNativeTimestamp(scan.getOutput().getTimestampColumnId(), type);
                    ctx.intrinsicTimestampColumnIds.add(scan.getOutput().getTimestampColumnId());
                }
                if (source.isUpdate()) {
                    if (i == metadata.getTimestampIndex()) {
                        updateTimestampIndex = updateTableColumnNames.size();
                    }
                    updateTableColumnTypes.add(type);
                    updateTableColumnNames.add(metadata.getColumnName(i));
                }
            }
        }
        if (source.isUpdate()) {
            model.copyUpdateTableMetadata(source);
        }
        return scan;
    }

    private LogicalPlan bindSetBranchFilter(
            ExpressionNode expression, LogicalPlan branch, int columnIndex, OutputSchema scope, int scopeIndex,
            CharSequence alias, SqlExecutionContext executionContext
    ) throws SqlException {
        final OutputSchema output = branch.getOutput();
        final BoundExpression predicate;
        try {
            ctx.scratchScope.clear();
            ctx.scratchScope.add(output.getColumnId(columnIndex), scope.getColumnName(scopeIndex), output.getColumnType(columnIndex),
                    output.getMetadata(columnIndex), true, scope.getColumnQualifier(scopeIndex));
            ctx.scratchScope.setTimestampIndex(output.getTimestampIndex() == columnIndex ? 0 : -1);
            ctx.joinNativeTimestampIds.clear();
            ctx.copyTimestampScope(output);
            predicate = ctx.functionBinder.toBooleanSubquery(ctx.functionBinder.bindPredicate(expression, ctx.scratchScope, alias,
                    ctx.joinNativeTimestampIds, ColumnType.BOOLEAN, executionContext));
        } finally {
            ctx.scratchScope.clear();
        }
        if (predicate.getDataType() != ColumnType.BOOLEAN) {
            throw SqlException.$(expression.position, "boolean expression expected");
        }
        final FilterPlan filter = ctx.filters.next().of(branch, predicate, predicate.getPosition());
        filter.getOutput().copyFrom(output);
        return filter;
    }

    private LogicalPlan bindSetOperation(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        if (model.getUnionModel() == null) {
            return bindQueryBlock(model, executionContext, true);
        }
        final LogicalPlan show = bindShow(model, executionContext);
        LogicalPlan result = show != null ? show : bindQueryBlock(model, executionContext, false);
        QueryModel branch = model;
        while (branch.getUnionModel() != null) {
            final QueryModel next = branch.getUnionModel();
            final LogicalPlan nextShow = bindShow(next, executionContext);
            LogicalPlan right = nextShow != null ? nextShow : bindQueryBlock(next, executionContext, false);
            if (ctx.lateralScopes.size() > 0 && !hasSameCorrelation(result.getOutput(), right.getOutput())) {
                result = lateralBinder.correlateBranch(result, right.getOutput(), branch.getModelPosition(), executionContext);
                right = lateralBinder.correlateBranch(right, result.getOutput(), next.getModelPosition(), executionContext);
                result = lateralBinder.orderCorrelatedColumns(result, result.getOutput(), branch.getModelPosition());
                right = lateralBinder.orderCorrelatedColumns(right, result.getOutput(), next.getModelPosition());
            }
            final boolean continuesUnion = next.getUnionModel() != null
                    && (next.getSetOperationType() == QueryModel.SET_OPERATION_UNION
                    || next.getSetOperationType() == QueryModel.SET_OPERATION_UNION_ALL);
            final SetOperationPlan operation = setOperations.next().of(
                    result, right, branch.getSetOperationType(), branch.getModelPosition(), next.getModelPosition(), !continuesUnion
            );
            SetOperationBinder.resolveTypes(operation, setTypes);
            for (int i = 0, n = setTypes.size(); i < n; i++) {
                operation.getOutput().add(ctx.nextColumnId++, result.getOutput().getColumnName(i), setTypes.getQuick(i), result.getOutput().isVisible(i));
                operation.getOutput().setSymbolTableStatic(i, SetOperationBinder.isSymbolTableStatic(operation, i));
                final int leftId = result.getOutput().getColumnId(i);
                final int rightId = right.getOutput().getColumnId(i);
                if (ctx.intrinsicTimestampColumnIds.contains(leftId) || ctx.intrinsicTimestampColumnIds.contains(rightId)
                        || ctx.ambiguousTimestampColumnIds.contains(leftId) || ctx.ambiguousTimestampColumnIds.contains(rightId)) {
                    if (LogicalPlans.canPushSetTimestamp(operation, i)) {
                        ctx.intrinsicTimestampColumnIds.add(operation.getOutput().getColumnId(i));
                    } else if (result.getOutput().getColumnType(i) != right.getOutput().getColumnType(i)
                            || ctx.ambiguousTimestampColumnIds.contains(leftId) || ctx.ambiguousTimestampColumnIds.contains(rightId)) {
                        // A bound comparison cannot silently cross a timestamp
                        // precision conversion. Equal branch precisions bind as
                        // an ordinary column above a row-selection boundary.
                        ctx.ambiguousTimestampColumnIds.add(operation.getOutput().getColumnId(i));
                    }
                }
            }
            operation.getOutput().setTimestampIndex(SetOperationBinder.resolveTimestampIndex(operation));
            final OutputSchema leftOutput = result.getOutput();
            for (int i = 0, n = leftOutput.getCorrelatedAliasCount(); i < n; i++) {
                operation.getOutput().addCorrelatedAlias(leftOutput.getCorrelatedAliasQualifier(i), leftOutput.getCorrelatedAliasName(i),
                        leftOutput.getCorrelatedAliasIndex(i));
            }
            result = operation;
            branch = next;
        }
        // The parser places trailing ORDER BY/LIMIT on the last branch. They
        // constrain the whole set; bind them without moving or rebuilding AST nodes.
        final QueryModel ordering = branch.getNestedModel();
        if (ordering != null && ordering.getOrderBy().size() > 0) {
            ctx.aliases.clear();
            ctx.aliasSequences.clear();
            ctx.projectionAliasIndexes.clear();
            ctx.sourceProjectionIndexes.setAll(result.getOutput().getColumnCount(), -1);
            final ProjectPlan projection = ctx.projects.next().of(result, model.getModelPosition());
            for (int i = 0, n = result.getOutput().getColumnCount(); i < n; i++) {
                ctx.addProjection(projection, result.getOutput(), i, result.getOutput().getColumnName(i), getOutputColumnPosition(result, i), true);
            }
            result = hasComputedOrder(projection, ordering)
                    ? orderBinder.bindOutputOrder(null, result, projection, ordering, null, null, executionContext)
                    : orderBinder.bindSourceOrder(result, projection, ordering, null);
        }
        return orderBinder.bindLimit(result, branch, executionContext);
    }

    private LogicalPlan bindShow(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        if (model.getShowKind() == -1) {
            return null;
        }
        final LogicalPlan show = ctx.functionSources.bindShow(model, executionContext, parserCallback, ctx.nextColumnId);
        ctx.nextColumnId += show.getOutput().getColumnCount();
        return show;
    }

    private LogicalPlan bindSourceWithHints(QueryModel model, QueryModel source, SqlExecutionContext executionContext) throws SqlException {
        if (source.isPivot()) {
            return pivotBinder.rewritePivot(source, executionContext);
        }
        if (source.getNestedModel() != null) {
            validateTimestampOffsetUnit(source);
            return bindTimestamp(bindQuery(source.getNestedModel(), executionContext), source);
        } else if (source.getTableNameExpr() != null && source.getTableNameExpr().type == ExpressionNode.FUNCTION) {
            return bindTimestamp(bindFunctionSource(source.getTableNameExpr(), executionContext), source);
        } else {
            final ExpressionNode tableName = source.getTableNameExpr();
            if (tableName == null || tableName.type != ExpressionNode.LITERAL) {
                throw new IllegalStateException("source has no table name");
            }
            final CharSequence quoted = GenericLexer.unquote(tableName.token);
            final boolean isRowIdSuppressed = Chars.startsWith(quoted, QueryModel.NO_ROWID_MARKER);
            final CharSequence name = isRowIdSuppressed ? quoted.subSequence(QueryModel.NO_ROWID_MARKER.length(), quoted.length()) : quoted;
            if (name.isEmpty()) {
                throw SqlException.$(tableName.position, "come on, where is the table name?");
            }
            final TableToken token = executionContext.getTableTokenIfExists(name);
            if (token == null) {
                // As in table enumeration, a missing table name can denote a
                // zero-argument catalogue function. Keep the parser AST unchanged.
                final ExpressionNode call = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, name,
                        tableName.precedence, tableName.position);
                final LogicalPlan functionSource;
                try {
                    functionSource = bindFunctionSource(call, executionContext);
                } catch (SqlException e) {
                    throw SqlException.tableDoesNotExist(tableName.position, name);
                }
                return bindTimestamp(functionSource, source);
            }
            if (token.isView()) {
                assert source.isUpdate();
                throw SqlException.position(tableName.position).put("cannot modify ").put(token.getType().keyword())
                        .put(" [view=").put(token.getTableName()).put(']');
            }

            final ScanPlan scan;
            if (source.isUpdate()) {
                try (TableRecordMetadata metadata = executionContext.getMetadataForWrite(token, source.getMetadataVersion())) {
                    scan = bindScan(model, source, tableName, metadata);
                } catch (CairoException e) {
                    if (e.isOutOfMemory() || e.isTableDoesNotExist()) {
                        throw e;
                    }
                    throw SqlException.position(tableName.position).put(e);
                }
            } else if (token.isLiveView()) {
                // Live views keep _meta without column data files, so read the metadata directly.
                try (TableReaderMetadata metadata = new TableReaderMetadata(executionContext.getCairoEngine().getConfiguration(), token)) {
                    metadata.loadMetadata();
                    scan = bindScan(model, source, tableName, metadata);
                }
            } else {
                try (TableReader reader = executionContext.getReader(token)) {
                    scan = bindScan(model, source, tableName, reader.getMetadata());
                } catch (EntryLockedException e) {
                    throw SqlException.position(tableName.position).put("table is locked: ").put(token.getTableName());
                } catch (CairoException e) {
                    if (e.isOutOfMemory() || e.isTableDoesNotExist()) {
                        throw e;
                    }
                    throw SqlException.position(tableName.position).put(e).setTableBusy(e instanceof EntryUnavailableException);
                }
            }
            scan.setRandomAccess(!isRowIdSuppressed);
            return bindTimestamp(scan, source);
        }
    }

    private LogicalPlan bindTimestamp(LogicalPlan input, QueryModel source) throws SqlException {
        final ExpressionNode timestamp = source.getTimestamp();
        if (timestamp == null) {
            return input;
        }
        input = orderBinder.projectBeforeDeclaredOrder(input);
        final OutputSchema output = input.getOutput();
        final int index = getColumnIndexQuiet(output, timestamp.token);
        if (index < 0) {
            throw SqlException.invalidColumn(timestamp.position, timestamp.token);
        }
        if (!ColumnType.isTimestamp(output.getColumnType(index))) {
            throw SqlException.$(timestamp.position, "not a TIMESTAMP");
        }
        if (input.getType() == LogicalPlan.Type.SCAN) {
            // SQL can designate another timestamp. Partition intervals still use
            // the table's native timestamp, retained separately on the scan.
            output.setTimestampIndex(index);
            return input;
        }
        // A declaration changes metadata and observes the subquery's order. Keep
        // that boundary using the existing projection/factory representation.
        final ProjectPlan project = ctx.projects.next().of(input, timestamp.position);
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            project.getExpressions().add(ctx.columns.next().of(output.getColumnId(i), output.getColumnType(i), timestamp.position));
        }
        project.getOutput().copyFrom(output);
        project.getOutput().setTimestampIndex(index);
        project.markTimestampDeclaration();
        return project;
    }

    private Throwable clearSubqueries(Throwable primary) {
        for (int i = 0; i < subqueryCount; i++) {
            primary = Misc.freeBestEffort(primary, subqueryFactories.getQuick(i));
            try {
                subqueryBinders.getQuick(i).clear();
            } catch (Throwable th) {
                primary = Misc.foldCleanupFailure(primary, th);
            }
        }
        subqueryFactories.clear();
        subqueryCount = 0;
        if (scalarBoundDepth >= MAX_RETAINED_SUBQUERY_DEPTH - 1 && subqueryBinders.size() > 0) {
            // One deeply nested query must not pin a binder per nesting level for the binder lifetime.
            primary = Misc.freeObjListBestEffort(primary, subqueryBinders);
            subqueryBinders.clear();
        }
        return primary;
    }

    private Throwable closePrepared(Throwable primary) {
        return clearSubqueries(ctx.functionSources.closePrepared(ctx.functionBinder.closePrepared(primary)));
    }

    private boolean collectCursorColumns(QueryModel model) {
        ctx.clearCursorColumns();
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        ctx.cursorProjectionSources.setAll(columns.size(), -1);
        for (int i = 0, n = columns.size(); i < n; i++) {
            final ExpressionNode ast = columns.getQuick(i).getAst();
            if (ctx.isCursorCall(ast)) {
                final int count = ctx.cursorSources.size();
                if (addCursorNode(ast, columns.getQuick(i).getName()) == count) {
                    ctx.cursorProjectionSources.setQuick(i, count);
                }
            } else {
                collectCursorNodes(ast);
            }
        }
        return ctx.cursorNodes.size() > 0;
    }

    private void collectCursorNodes(ExpressionNode node) {
        if (node == null || node.type == ExpressionNode.QUERY || ctx.isAggregate(node)) {
            return;
        }
        if (ctx.isCursorCall(node)) {
            addCursorNode(node, null);
            return;
        }
        collectCursorNodes(node.lhs);
        collectCursorNodes(node.rhs);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            collectCursorNodes(node.args.getQuick(i));
        }
    }

    // Hints can name aliases assigned to unnamed subqueries before join rewriting.
    private void collectHintAliases(QueryModel model) {
        if (model == null) {
            return;
        }
        if (model.getName() == null && hintAliasModels.indexOf(model) < 0) {
            hintAliasModels.add(model);
            final CharacterStoreEntry alias = ctx.characterStore.newEntry();
            alias.put(QueryModel.SUB_QUERY_ALIAS_PREFIX).put(hintAliases.size());
            hintAliases.add(alias.toImmutable());
        }
        final ObjList<QueryModel> sources = model.getJoinModels();
        for (int i = 1, n = sources.size(); i < n; i++) {
            collectHintAliases(sources.getQuick(i));
        }
        collectHintAliases(model.getNestedModel());
        collectHintAliases(model.getUnionModel());
    }

    /**
     * Collects the columns whose IN (subquery) conjunct binds as a key subquery: the single
     * SYMBOL LATEST BY key, otherwise the indexed columns of a plain scan.
     */
    private void collectKeySubqueryColumns(LogicalPlan source, LatestByPlan latest, SqlExecutionContext executionContext) {
        if (!(source instanceof ScanPlan scan)) {
            return;
        }
        final IntHashSet ids = ctx.functionBinder.getKeySubqueryColumnIds();
        final OutputSchema output = scan.getOutput();
        if (latest != null) {
            if (latest.getKeyColumnIds().size() == 1) {
                final int id = latest.getKeyColumnIds().getQuick(0);
                if (ColumnType.isSymbol(output.getColumnType(output.getColumnIndexById(id)))) {
                    ids.add(id);
                }
            }
        } else if (!scan.hasHint(ScanPlan.HINT_NO_INDEX) && !executionContext.isLiveViewCompile()) {
            final IntList indexed = scan.getIndexedColumnIds();
            for (int i = 0, n = indexed.size(); i < n; i++) {
                ids.add(indexed.getQuick(i));
            }
        }
    }

    private boolean collectPredicateSource(ExpressionNode expression, LogicalPlan input, CharSequence alias) throws SqlException {
        if (expression == null) {
            return true;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            final OutputSchema output = input.getOutput();
            final int id = output.getColumnId(ctx.bindColumnIndex(expression, output, alias));
            final LogicalPlan origin = columnSource(input, id);
            if (predicateSource == null) {
                predicateSource = origin;
            }
            return predicateSource == origin;
        }
        if (!collectPredicateSource(expression.rhs, input, alias) || !collectPredicateSource(expression.lhs, input, alias)) {
            return false;
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (!collectPredicateSource(expression.args.getQuick(i), input, alias)) {
                return false;
            }
        }
        return true;
    }

    private void collectProjectedSubstitutions(ExpressionNode node, QueryModel model, OutputSchema output) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int index = selectedLiteralIndex(node, model);
            ctx.substitutionNodes.add(node);
            ctx.substitutionColumns.add(ctx.columns.next().of(output.getColumnId(index), output.getColumnType(index), node.position));
            return;
        }
        collectProjectedSubstitutions(node.lhs, model, output);
        collectProjectedSubstitutions(node.rhs, model, output);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            collectProjectedSubstitutions(node.args.getQuick(i), model, output);
        }
    }

    private LogicalPlan filterProjection(LogicalPlan result, ProjectPlan project, ExpressionNode where, QueryModel model,
                                         SqlExecutionContext executionContext) throws SqlException {
        ctx.substitutionNodes.clear();
        ctx.substitutionColumns.clear();
        collectProjectedSubstitutions(where, model, project.getOutput());
        final BoundExpression bound = ctx.functionBinder.bind(where, project.getOutput(), null, ctx.substitutionNodes, ctx.substitutionColumns, executionContext);
        ctx.substitutionNodes.clear();
        ctx.substitutionColumns.clear();
        final FilterPlan filter = ctx.filters.next().of(project, bound, where.position);
        filter.getOutput().copyFrom(project.getOutput());
        if (result == project) {
            return filter;
        }
        for (LogicalPlan node = result; node instanceof UnaryPlan unary; node = unary.getInput()) {
            if (unary.getInput() == project) {
                unary.replaceInput(0, filter);
                break;
            }
        }
        return result;
    }

    private int getUpdateColumnIndex(CharSequence name) {
        final ObjList<CharSequence> columns = updateTableColumnNames;
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(columns.getQuick(i), name)
                    || SqlUtil.isQuoteProtectedAlias(name)
                    && Chars.equalsIgnoreCase(columns.getQuick(i), name, 1, name.length() - 1)) {
                return i;
            }
        }
        return -1;
    }

    private boolean isCursorNameTaken(CharSequence name) {
        for (int i = 0, n = ctx.cursorNames.size(); i < n; i++) {
            if (ctx.cursorNames.getQuick(i) != name && Chars.equalsIgnoreCase(ctx.cursorNames.getQuick(i), name)) {
                return true;
            }
        }
        return false;
    }

    private void prepareUpdateAssignments(QueryModel model, ProjectPlan project) throws SqlException {
        final IntList targetTypes = project.getUpdateTargetTypes();
        final OutputSchema output = project.getOutput();
        final ObjList<QueryColumn> targets = model.getBottomUpColumns();
        updateTargetNames.clear();
        boolean hasConversions = false;
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final CharSequence target = output.getColumnName(i);
            final int targetPosition = i < targets.size() ? targets.getQuick(i).getAliasPosition() : 0;
            final int targetIndex = getUpdateColumnIndex(target);
            if (targetIndex < 0) {
                throw SqlException.invalidColumn(targetPosition, target);
            }
            if (targetIndex == updateTimestampIndex) {
                throw SqlException.$(targetPosition, "Designated timestamp column cannot be updated");
            }
            final CharSequence name = updateTableColumnNames.getQuick(targetIndex);
            for (int k = 0, m = updateTargetNames.size(); k < m; k++) {
                if (Chars.equalsIgnoreCase(updateTargetNames.getQuick(k), name)) {
                    throw SqlException.$(targetPosition, "Duplicate column ").put(target).put(" in SET clause");
                }
            }
            updateTargetNames.add(name);
            final int targetType = updateTableColumnTypes.getQuick(targetIndex);
            targetTypes.add(targetType);
            final BoundExpression expression = project.getExpressions().getQuick(i);
            if (targetType >= 0 && expression.getDataType() != targetType) {
                final Function function = ctx.functionBinder.prepareUpdateAssignment(expression, targetType);
                output.setColumnType(i, FunctionBinder.updateColumnType(function, targetType));
                output.setSymbolTableStatic(i, function instanceof SymbolFunction symbol && symbol.isSymbolTableStatic());
                hasConversions = true;
            }
        }
        if (!hasConversions) {
            targetTypes.clear();
        }
    }

    private OutputSchema projectionReferenceScope(OutputSchema source) {
        ctx.scratchScope.copyFrom(source);
        return ctx.scratchScope;
    }

    /**
     * A window function argument may name an earlier computed column, which it reads through a
     * self-reference column of the inner projection.
     */
    private boolean referencesEarlierAlias(ExpressionNode node, ObjList<QueryColumn> columns, int columnIndex, OutputSchema source,
                                           boolean isWindowArgument) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (Chars.indexOfLastUnquoted(node.token, '.') >= 0 || getColumnIndexQuiet(source, node.token) >= 0) {
                return false;
            }
            for (int i = 0; i < columnIndex; i++) {
                if (Chars.equalsIgnoreCase(columns.getQuick(i).getName(), GenericLexer.unquote(node.token))) {
                    return !isWindowArgument || !windowBinder.isPureComputedColumn(columns.getQuick(i).getAst());
                }
            }
            return false;
        }
        isWindowArgument |= node.windowExpression != null;
        if (referencesEarlierAlias(node.lhs, columns, columnIndex, source, isWindowArgument)
                || referencesEarlierAlias(node.rhs, columns, columnIndex, source, isWindowArgument)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (referencesEarlierAlias(node.args.getQuick(i), columns, columnIndex, source, isWindowArgument)) {
                return true;
            }
        }
        return false;
    }

    private boolean referencesOnlyColumn(ExpressionNode expression, OutputSchema output, CharSequence alias, int index) {
        if (expression == null) {
            return true;
        }
        return switch (expression.type) {
            case ExpressionNode.LITERAL -> FunctionBinder.findColumn(expression, output, alias) == index;
            case ExpressionNode.QUERY -> false;
            default -> {
                if (expression.paramCount < 3) {
                    yield referencesOnlyColumn(expression.lhs, output, alias, index) && referencesOnlyColumn(expression.rhs, output, alias, index);
                }
                for (int i = 0, n = expression.args.size(); i < n; i++) {
                    if (!referencesOnlyColumn(expression.args.getQuick(i), output, alias, index)) {
                        yield false;
                    }
                }
                yield true;
            }
        };
    }

    private int scanHints(QueryModel source) {
        if (ctx.currentHints == null) {
            return 0;
        }
        int hints = 0;
        if (ctx.currentHints.keyIndex(SqlHints.NO_INDEX_HINT) < 0) {
            hints |= ScanPlan.HINT_NO_INDEX | ScanPlan.HINT_NO_COVERING;
        }
        if (ctx.currentHints.keyIndex(SqlHints.NO_COVERING_HINT) < 0) {
            hints |= ScanPlan.HINT_NO_COVERING;
        }
        if (ctx.currentHints.keyIndex(SqlHints.FORCE_USE_COVERING_HINT) < 0) {
            hints |= ScanPlan.HINT_FORCE_USE_COVERING;
        }
        if (ctx.currentHints.keyIndex(SqlHints.NO_SYMBOL_PATTERN_INDEX_HINT) < 0) {
            hints |= ScanPlan.HINT_NO_SYMBOL_PATTERN_INDEX;
        }
        if (Chars.containsWordIgnoreCase(ctx.currentHints.get(SqlHints.ENABLE_PRE_TOUCH_HINT), source.getName(), SqlHints.HINTS_PARAMS_DELIMITER)) {
            hints |= ScanPlan.HINT_PRE_TOUCH;
        }
        return hints;
    }

    private void throwDeferredError() throws SqlException {
        if (deferredErrorPosition >= 0) {
            throw SqlException.$(deferredErrorPosition, deferredErrorMessage);
        }
    }

    private CharSequence uniqueCursorName(CharSequence name, OutputSchema output) {
        CharSequence candidate = name;
        for (int sequence = 1; getColumnIndexQuiet(output, candidate) >= 0 || isCursorNameTaken(candidate); sequence++) {
            final CharacterStoreEntry entry = ctx.characterStore.newEntry();
            entry.put(name).put(sequence);
            candidate = entry.toImmutable();
        }
        return candidate;
    }

    private void validateWithinArgument(ExpressionNode argument, OutputSchema input, CharSequence alias, int columnType,
                                        SqlExecutionContext executionContext) throws SqlException {
        final int position = argument.position;
        if (SqlKeywords.isNullKeyword(argument.token)) {
            throw SqlException.$(position, "GeoHash value expected");
        }
        final int type;
        final long hash;
        if (argument.type == ExpressionNode.FUNCTION || argument.type == ExpressionNode.BIND_VARIABLE
                || argument.type == ExpressionNode.OPERATION) {
            final BoundExpression bound = ctx.functionBinder.bind(argument, input, alias, executionContext);
            if (!(bound instanceof ConstantExpression constant) || !ColumnType.isGeoHash(constant.getDataType())) {
                throw SqlException.$(position, "GeoHash const function expected");
            }
            type = constant.getDataType();
            hash = constant.getLongValue();
        } else {
            final CharSequence token = argument.token;
            final int len = token.length();
            final boolean isBitsPrefix = len > 2 && token.charAt(0) == '#' && token.charAt(1) == '#';
            if (argument.type != ExpressionNode.CONSTANT || !isBitsPrefix && (len < 2 || token.charAt(0) != '#')) {
                throw SqlException.$(position, "GeoHash literal expected");
            }
            try {
                if (isBitsPrefix) {
                    final int bits = len - 2;
                    if (bits > ColumnType.GEOLONG_MAX_BITS) {
                        throw SqlException.$(position, "GeoHash bits literal expected");
                    }
                    type = ColumnType.getGeoHashTypeWithBits(bits);
                    hash = GeoHashes.fromBitStringNl(token, 2);
                } else {
                    final int suffix = ExpressionParser.extractGeoHashSuffix(position, token);
                    final int bits = Numbers.decodeHighShort(suffix);
                    type = ColumnType.getGeoHashTypeWithBits(bits);
                    hash = GeoHashes.fromStringTruncatingNl(token, 1, len - Numbers.decodeLowShort(suffix), bits);
                }
            } catch (NumericException e) {
                throw SqlException.$(position, "GeoHash literal expected");
            }
        }
        try {
            GeoHashes.addNormalizedGeoPrefix(hash, type, columnType, withinPrefixes);
        } catch (NumericException e) {
            throw SqlException.$(position, "GeoHash prefix precision mismatch");
        }
    }

    private void validateWithinCall(ExpressionNode node, OutputSchema input, CharSequence alias,
                                    SqlExecutionContext executionContext) throws SqlException {
        if (withinPrefixes.size() > 0) {
            throw SqlException.$(node.position, "Multiple 'within' expressions not supported");
        }
        if (node.paramCount < 2) {
            throw SqlException.$(node.position, "Too few arguments for 'within'");
        }
        final ExpressionNode column = node.paramCount < 3 ? node.lhs : node.args.getLast();
        if (column.type != ExpressionNode.LITERAL) {
            throw SqlException.unexpectedToken(column.position, column.token);
        }
        final int columnType = input.getColumnType(FunctionBinder.resolveColumn(column, input, alias));
        if (!ColumnType.isGeoHash(columnType)) {
            throw SqlException.$(node.position, "GeoHash column type expected");
        }
        withinPrefixes.add(0, columnType);
        if (node.paramCount == 2) {
            validateWithinArgument(node.rhs, input, alias, columnType, executionContext);
        } else {
            for (int i = node.paramCount - 2; i > -1; i--) {
                validateWithinArgument(node.args.getQuick(i), input, alias, columnType, executionContext);
            }
        }
    }

    static int selectedLiteralIndex(ExpressionNode node, QueryModel model) {
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final ExpressionNode ast = columns.getQuick(i).getAst();
            if (ast.type == ExpressionNode.LITERAL && Chars.equalsIgnoreCase(ast.token, node.token)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Binds the model into an unoptimised plan. {@link SqlCompilerImpl} optimises it and hands the
     * result back through {@link #setRoot(LogicalPlan)}. {@code isFullFatJoins} forces full-fat
     * ASOF/LT joins, which changes their key types.
     */
    LogicalPlan bind(QueryModel model, SqlParserCallback parserCallback, boolean isFullFatJoins, SqlExecutionContext executionContext) throws SqlException {
        clear();
        this.parserCallback = parserCallback;
        ctx.executionContext = executionContext;
        ctx.isFullFatJoins = isFullFatJoins;
        isUpdate = model.isUpdate();
        collectHintAliases(model);
        final SqlBinder previous = ctx.functionParser.swapSubqueryBinder(this);
        final LogicalPlan plan;
        try {
            plan = bindQuery(model, executionContext);
        } finally {
            ctx.functionParser.swapSubqueryBinder(previous);
        }
        throwDeferredError();
        return plan;
    }

    /**
     * Binds command/VALUES expressions without inventing a relational input.
     */
    Function bindExpression(ExpressionNode expression, RecordMetadata metadata, int preferredType, SqlExecutionContext executionContext) throws SqlException {
        assert root == null;
        if (expression == null) {
            return null;
        }
        ctx.scratchScope.clear();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            ctx.scratchScope.add(i, metadata.getColumnName(i), metadata.getColumnType(i), true);
            ctx.scratchScope.setSymbolTableStatic(i, metadata.isSymbolTableStatic(i));
        }
        Function function = null;
        try {
            final BoundExpression bound = ctx.functionBinder.bind(expression, ctx.scratchScope, null, preferredType, executionContext);
            function = ctx.functionBinder.instantiate(bound, ctx.scratchScope, metadata, executionContext);
            // Only the executable closure escapes a standalone expression. Reuse
            // preparation storage per VALUES cell, not once per entire statement.
            ctx.functionBinder.clear();
            return function;
        } catch (Throwable th) {
            Misc.free(function, th);
            final Throwable failure = ctx.functionBinder.closePrepared(th);
            assert failure == th;
            throw th;
        } finally {
            ctx.scratchScope.clear();
        }
    }

    LatestByPlan bindLatestBy(LogicalPlan input, QueryModel source) throws SqlException {
        final OutputSchema output = input.getOutput();
        if (input instanceof ScanPlan scan) {
            if (output.getTimestampColumnId() != scan.getNativeTimestampColumnId()) {
                throw SqlException.$(source.getTimestamp().position, "latest by over a table requires designated TIMESTAMP");
            }
        } else if (output.getTimestampColumnId() < 0) {
            throw SqlException.$(source.getModelPosition(), "latest by query does not provide dedicated TIMESTAMP column");
        }
        final LatestByPlan latest = latestByPlans.next().of(input, output.getTimestampColumnId(), source.getModelPosition());
        latest.setTimestampOrderInherited(source.getNestedModel() != null);
        for (int i = 0, n = source.getLatestBy().size(); i < n; i++) {
            final ExpressionNode key = source.getLatestBy().getQuick(i);
            final int index = ctx.bindColumnIndex(key, output, source);
            validateLatestByColumn(output.getColumnType(index), key.position, key.token);
            latest.getKeyColumnIds().add(output.getColumnId(index));
        }
        for (int i = 0, n = output.getCorrelatedAliasCount(); i < n; i++) {
            final int columnId = output.getColumnId(output.getCorrelatedAliasIndex(i));
            if (!latest.getKeyColumnIds().contains(columnId)) {
                latest.getKeyColumnIds().add(columnId);
            }
        }
        latest.getOutput().copyFrom(output);
        return latest;
    }

    BoundExpression bindPredicate(
            ExpressionNode expression, LogicalPlan input, QueryModel source, SqlExecutionContext executionContext
    ) throws SqlException {
        final OutputSchema metadata = input.getOutput();
        if (expression.type == ExpressionNode.CONSTANT) {
            if (SqlKeywords.isTrueKeyword(expression.token)) {
                return ctx.constants.next().ofBoolean(true, expression.position);
            }
            if (SqlKeywords.isFalseKeyword(expression.token)) {
                return ctx.constants.next().ofBoolean(false, expression.position);
            }
        }
        if (expression.type != ExpressionNode.LITERAL) {
            final BoundExpression bound = ctx.functionBinder.toBooleanSubquery(
                    bindFilterConjunct(expression, input, sourceAlias(source), false, executionContext));
            if (bound.getDataType() != ColumnType.BOOLEAN) {
                throw SqlException.$(expression.position, "boolean expression expected");
            }
            return bound;
        }
        final int index = ctx.bindColumnIndex(expression, metadata, source);
        if (metadata.getColumnType(index) != ColumnType.BOOLEAN) {
            throw SqlException.$(expression.position, "boolean expression expected");
        }
        return ctx.columns.next().of(metadata.getColumnId(index), ColumnType.BOOLEAN, expression.position);
    }

    LogicalPlan bindQuery(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final LowerCaseCharSequenceObjHashMap<CharSequence> previousHints = ctx.currentHints;
        final ExpressionNode originatingView = model.getOriginatingViewNameExpr();
        final int previousRequirementPosition = originatingView == null ? -1
                : ctx.functionParser.enterExecutionRequirementPosition(originatingView.position);
        ctx.currentHints = mergeHints(model, ctx.currentHints);
        try {
            return bindQueryWithHints(model, executionContext);
        } finally {
            ctx.currentHints = previousHints;
            if (originatingView != null) {
                ctx.functionParser.restoreExecutionRequirementPosition(previousRequirementPosition);
            }
        }
    }

    LogicalPlan bindSource(QueryModel model, QueryModel source, SqlExecutionContext executionContext) throws SqlException {
        final LowerCaseCharSequenceObjHashMap<CharSequence> previousHints = ctx.currentHints;
        ctx.currentHints = mergeHints(source, ctx.currentHints);
        try {
            return bindSourceWithHints(model, source, executionContext);
        } finally {
            ctx.currentHints = previousHints;
        }
    }

    RecordCursorFactory claimSubquery(int index) {
        final RecordCursorFactory factory = subqueryFactories.getQuick(index);
        subqueryFactories.setQuick(index, null);
        return factory;
    }

    int compileSubquery(QueryModel model, int position, SqlExecutionContext executionContext) throws SqlException {
        if (subqueryBinders.size() == subqueryCount) {
            subqueryBinders.add(new SqlBinder(ctx.configuration, ctx.functionParser, compiler));
        }
        final int index = subqueryCount++;
        subqueryFactories.extendAndSet(index, null);
        final SqlBinder subquery = subqueryBinders.getQuick(index);
        subquery.scalarBoundDepth = scalarBoundDepth + 1;
        final boolean isWindowContextPushed = !executionContext.getWindowContext().isEmpty();
        if (isWindowContextPushed) {
            executionContext.pushWindowContext();
        }
        try {
            compiler.compileLogicalPlan(subquery, model, parserCallback, executionContext);
            subqueryFactories.setQuick(index, generateSubquery(index, position, executionContext));
        } finally {
            if (isWindowContextPushed) {
                executionContext.popWindowContext();
            }
        }
        return index;
    }

    /**
     * Binds a conjunct that references only a mixed-precision set
     * timestamp in every set branch, so each branch applies its own precision.
     * Returns whether the conjunct left nothing to evaluate above the input.
     */
    boolean distributeSetFilter(
            ExpressionNode expression, LogicalPlan input, OutputSchema scope, CharSequence alias, SqlExecutionContext executionContext
    ) throws SqlException {
        final ExpressionNode column = firstColumnReference(expression);
        if (column == null) {
            return false;
        }
        final int scopeIndex = FunctionBinder.findColumn(column, scope, alias);
        if (scopeIndex < 0 || !referencesOnlyColumn(expression, scope, alias, scopeIndex)) {
            return false;
        }
        LogicalPlan plan = input;
        int columnId = scope.getColumnId(scopeIndex);
        while (plan instanceof ProjectPlan project) {
            final int index = project.getOutput().getColumnIndexById(columnId);
            if (index < 0 || !(project.getExpressions().getQuick(index) instanceof ColumnExpression projected)) {
                return false;
            }
            columnId = projected.getColumnId();
            plan = project.getInput();
        }
        if (!(plan instanceof SetOperationPlan operation) || !ctx.ambiguousTimestampColumnIds.contains(columnId)) {
            return false;
        }
        final int columnIndex = operation.getOutput().getColumnIndexById(columnId);
        return LogicalPlans.setTimestampIndex(operation) == columnIndex
                && distributeSetFilter(expression, operation, columnIndex, scope, scopeIndex, alias, executionContext);
    }

    boolean distributeSetFilter(
            ExpressionNode expression, SetOperationPlan operation, int columnIndex, OutputSchema scope, int scopeIndex,
            CharSequence alias, SqlExecutionContext executionContext
    ) throws SqlException {
        boolean isDistributed = true;
        for (int i = 0; i < 2; i++) {
            final LogicalPlan branch = operation.inputAt(i);
            if (branch instanceof SetOperationPlan branchOperation) {
                isDistributed &= distributeSetFilter(expression, branchOperation, columnIndex, scope, scopeIndex, alias, executionContext);
            } else if (LogicalPlans.skipProjects(branch).getType() == LogicalPlan.Type.LATEST_BY) {
                isDistributed = false;
            } else {
                operation.replaceInput(i, bindSetBranchFilter(expression, branch, columnIndex, scope, scopeIndex, alias, executionContext));
            }
        }
        return isDistributed;
    }

    /**
     * Frees resources a compilation left in flight when no failure is pending, and returns any
     * cleanup failure to the caller.
     */
    Throwable freeResourcesInFlight() {
        return closePrepared(null);
    }

    /**
     * Frees resources a failed compilation left in flight; cleanup failures become suppressed
     * exceptions of the primary.
     */
    void freeResourcesInFlight(@NotNull Throwable primary) {
        final Throwable failure = closePrepared(primary);
        assert failure == primary;
    }

    RecordCursorFactory generateSubquery(int index, int position, SqlExecutionContext executionContext) throws SqlException {
        final SqlBinder subquery = subqueryBinders.getQuick(index);
        executionContext.pushTimestampRequiredFlag(false);
        boolean hasPushedWindowContext = false;
        try {
            if (!executionContext.getWindowContext().isEmpty()) {
                executionContext.pushWindowContext();
                hasPushedWindowContext = true;
            }
            final RecordCursorFactory factory = compiler.generatePlan(subquery, false, executionContext);
            if (!executionContext.allowNonDeterministicFunctions() && factory.usesExternalDataSource()) {
                final SqlException exception = SqlException.nonDeterministicColumn(position, "sub-query",
                        executionContext.isLiveViewCompile() ? "live view" : "materialized view");
                Misc.free(factory, exception);
                throw exception;
            }
            return factory;
        } finally {
            if (hasPushedWindowContext) {
                executionContext.popWindowContext();
            }
            executionContext.popTimestampRequiredFlag();
        }
    }

    FunctionBinder getFunctionBinder() {
        return ctx.functionBinder;
    }

    TableFunctionSources getFunctionSources() {
        return ctx.functionSources;
    }

    /**
     * The first column id the bound plan does not use.
     */
    int getNextColumnId() {
        return ctx.nextColumnId;
    }

    int getOutputColumnPosition(int index) {
        return getOutputColumnPosition(root, index);
    }

    IntList getOutputColumnPositions() {
        // Borrowed binder scratch; callers must consume it before reuse or clear().
        outputColumnPositions.clear();
        for (int i = 0, n = root.getOutput().getColumnCount(); i < n; i++) {
            outputColumnPositions.add(getOutputColumnPosition(i));
        }
        return outputColumnPositions;
    }

    LogicalPlan getRoot() {
        return root;
    }

    int getScalarBoundDepth() {
        return scalarBoundDepth;
    }

    int getSubqueryFirstColumnPosition(int index) {
        return subqueryBinders.getQuick(index).getOutputColumnPosition(0);
    }

    LogicalPlan getSubqueryPlan(int index) {
        return subqueryBinders.getQuick(index).root;
    }

    long getUpdateMetadataVersion() {
        return updateMetadataVersion;
    }

    ObjList<CharSequence> getUpdateTableColumnNames() {
        return updateTableColumnNames;
    }

    IntList getUpdateTableColumnTypes() {
        return updateTableColumnTypes;
    }

    int getUpdateTableId() {
        return updateTableId;
    }

    CharSequence getUpdateTableName() {
        return updateTableName;
    }

    int getUpdateTablePosition() {
        return updateTablePosition;
    }

    TableToken getUpdateTableToken() {
        return updateTableToken;
    }

    ObjList<CharSequence> getUpdateTargetNames() {
        return updateTargetNames;
    }

    /**
     * Whether a SELECT column names an earlier column's alias that no source column shadows.
     * Such a reference reads the earlier column's value rather than evaluating it again.
     */
    boolean hasProjectionReferences(QueryModel model, OutputSchema source) {
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 1, n = columns.size(); i < n; i++) {
            if (referencesEarlierAlias(columns.getQuick(i).getAst(), columns, i, source, false)) {
                return true;
            }
        }
        return false;
    }

    boolean hasSinglePredicateSource(ExpressionNode expression, LogicalPlan input, CharSequence alias) throws SqlException {
        assert predicateSource == null;
        try {
            return collectPredicateSource(expression, input, alias);
        } finally {
            predicateSource = null;
        }
    }

    CharSequence hintAlias(QueryModel model) {
        final CharSequence name = model.getName();
        return name != null ? name : hintAliases.getQuick(hintAliasModels.indexOf(model));
    }

    boolean isUpdate() {
        return isUpdate;
    }

    LowerCaseCharSequenceObjHashMap<CharSequence> mergeHints(
            QueryModel model,
            LowerCaseCharSequenceObjHashMap<CharSequence> inherited
    ) {
        if (model.isCteModel()) {
            inherited = null;
        }
        final LowerCaseCharSequenceObjHashMap<CharSequence> own = model.getHints();
        if (own.size() == 0) {
            return inherited;
        }
        final LowerCaseCharSequenceObjHashMap<CharSequence> merged = hintScopes.next();
        merged.putAll(own);
        if (inherited != null) {
            // A containing SELECT overrides the same hint in a nested SELECT.
            merged.putAll(inherited);
        }
        return merged;
    }

    void retainImplicitTimestamp(LogicalPlan plan) {
        if (plan.getOutput().getTimestampIndex() >= 0) {
            return;
        }
        if (plan instanceof ProjectPlan project) {
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression)) {
                    return;
                }
            }
            retainImplicitTimestamp(project.getInput());
            final OutputSchema input = project.getInput().getOutput();
            final int timestampIndex = input.getTimestampIndex();
            if (timestampIndex >= 0) {
                final int columnId = ctx.nextColumnId++;
                project.getExpressions().add(ctx.columns.next().of(input.getColumnId(timestampIndex),
                        input.getColumnType(timestampIndex), project.getPosition()));
                // An implicit timestamp belongs to the record layout, never the
                // enclosing query's SQL name scope or wildcard expansion.
                project.getOutput().add(columnId, "", input.getColumnType(timestampIndex), input.getMetadata(timestampIndex), false);
                project.getOutput().setTimestampIndex(project.getOutput().getColumnCount() - 1);
                ctx.inheritTimestampBinding(input.getColumnId(timestampIndex), columnId);
            }
        } else if (plan.getType() == LogicalPlan.Type.FILTER || plan.getType() == LogicalPlan.Type.LIMIT) {
            final LogicalPlan input = plan.inputAt(0);
            retainImplicitTimestamp(input);
            plan.getOutput().copyFrom(input.getOutput());
        }
        // Other operators establish their own ordering or equality contract.
        // In particular, never enlarge a DISTINCT or set-operation tuple.
    }

    void setRoot(LogicalPlan root) {
        this.root = root;
    }

    void validateWithin(ExpressionNode node, OutputSchema input, CharSequence alias,
                                SqlExecutionContext executionContext) throws SqlException {
        if (node == null || node.type == ExpressionNode.QUERY) {
            return;
        }
        if (SqlKeywords.isWithinKeyword(node.token)) {
            validateWithinCall(node, input, alias, executionContext);
            return;
        }
        if (SqlKeywords.isAndKeyword(node.token) || SqlKeywords.isOrKeyword(node.token)) {
            final boolean isRightWithin = node.rhs != null && SqlKeywords.isWithinKeyword(node.rhs.token);
            if (isRightWithin) {
                validateWithinCall(node.rhs, input, alias, executionContext);
            }
            validateWithin(node.lhs, input, alias, executionContext);
            if (!isRightWithin) {
                validateWithin(node.rhs, input, alias, executionContext);
            }
        }
    }

    /** Projection aliases: the first reference to a column fixes its alias. */
    private static final class TranslatingAliases {
        private final IntHashSet columnIds = new IntHashSet();
        private final LowerCaseCharSequenceHashSet names = new LowerCaseCharSequenceHashSet();
        private final LowerCaseCharSequenceIntHashMap sequences = new LowerCaseCharSequenceIntHashMap();

        CharSequence add(int columnId, CharSequence name, CharacterStore store) {
            if (!columnIds.add(columnId)) {
                return name;
            }
            final CharSequence alias = SqlUtil.createColumnAlias(store, name, -1, names, sequences, false);
            names.add(alias);
            return alias;
        }

        // Follows ProjectionFactoryGenerator.collectProjectionInputIds: immediate literals are named
        // before descending into nested arguments.
        void addArguments(BoundExpression expression, OutputSchema input, CharacterStore store) {
            if (expression instanceof ColumnExpression column) {
                addColumn(column, input, store);
            } else if (expression instanceof FunctionExpression call) {
                final int count = call.getArgumentCount();
                for (int i = count < 3 ? count - 1 : count - 2; i >= 0; i--) {
                    if (call.argumentAt(i) instanceof ColumnExpression column) {
                        addColumn(column, input, store);
                    }
                }
                if (count >= 3) {
                    addArguments(call.argumentAt(count - 1), input, store);
                }
                for (int i = 0, n = count < 3 ? count : count - 1; i < n; i++) {
                    if (!(call.argumentAt(i) instanceof ColumnExpression)) {
                        addArguments(call.argumentAt(i), input, store);
                    }
                }
            }
        }

        TranslatingAliases of() {
            columnIds.clear();
            names.clear();
            sequences.clear();
            return this;
        }

        private void addColumn(ColumnExpression column, OutputSchema input, CharacterStore store) {
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index >= 0) {
                add(column.getColumnId(), input.getColumnName(index), store);
            }
        }
    }
}

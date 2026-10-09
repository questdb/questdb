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

package io.questdb.griffin.codegen;

import io.questdb.ParanoiaState;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.BoundExpressionRewriter;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.TableFunctionSources;
import io.questdb.griffin.PlanTables;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.ExplainPlanFactory;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.join.SharedRecordCursorFactory;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PhysicalProperties;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.griffin.model.QueryModel.CREATE_MAT_VIEW;

/**
 * Generates the executable factory tree of a bound logical plan. Ownership contract: a generate* method
 * either returns a factory its caller owns, or throws having closed everything it created. A generate*
 * method or factory constructor that takes child factories or functions consumes them on entry, including
 * when it throws, so a caller never closes an input it has handed over.
 */
public class SqlCodeGenerator implements Mutable, Closeable {
    public static final int GKK_MICRO_HOUR_INT = 1;
    public static final int GKK_NANO_HOUR_INT = 2;
    public static final int GKK_VANILLA_INT = 0;
    private static final int MAX_RETAINED_FRAMES = 32;
    private static final PlanVisitor UPDATE_SCANS = plan -> plan instanceof ScanPlan scan && scan.isUpdate() ? TreeWalk.STOP : TreeWalk.CONTINUE;
    public static boolean ALLOW_FUNCTION_MEMOIZATION = true;
    private final AggregateFactoryGenerator aggregateGenerator;
    private final BytecodeAssembler asm;
    private final CairoConfiguration configuration;
    private final OutputSchema emptySchema;
    private final FilterFactoryGenerator filterGenerator;
    private final ObjList<GenerationFrame> generationFrames = new ObjList<>();
    private final ListColumnFilter indexColumnFilter = new ListColumnFilter();
    private final MemoryCARW jitIRMem;
    private final JoinFactoryGenerator joinGenerator;
    private final LatestByFactoryGenerator latestByGenerator;
    private final ProjectionFactoryGenerator projectionGenerator;
    private final RecordComparatorCompiler recordComparatorCompiler;
    private final SampleByFactoryGenerator sampleByGenerator;
    private final ScanFactoryGenerator scanGenerator;
    private final SetOperationFactoryGenerator setOperationGenerator;
    private final SortFactoryGenerator sortGenerator;
    private final LongList tmpLongs = new LongList();
    private final StringSink tmpSink;
    private final WindowFactoryGenerator windowGenerator;
    private int generationDepth;

    public SqlCodeGenerator(
            CairoConfiguration configuration,
            FunctionResolver functionResolver,
            CharacterStore characterStore,
            BytecodeAssembler asm,
            EntityColumnFilter entityColumnFilter,
            OutputSchema emptySchema,
            PlanTables planTables,
            StringSink tmpSink,
            IntHashSet tmpIds,
            IntList tmpIndexes,
            IntList tmpValues,
            IntList tmpMasterKeys,
            IntList tmpSlaveKeys
    ) {
        try {
            this.configuration = configuration;
            this.asm = asm;
            this.emptySchema = emptySchema;
            this.tmpSink = tmpSink;
            this.recordComparatorCompiler = new RecordComparatorCompiler(asm);
            this.jitIRMem = Vm.getCARWInstance(
                    configuration.getSqlJitIRMemoryPageSize(),
                    configuration.getSqlJitIRMemoryMaxPages(),
                    MemoryTag.NATIVE_SQL_COMPILER
            );
            // Pre-touch JIT IR memory to avoid false positive memory leak detections.
            jitIRMem.putByte((byte) 0);
            jitIRMem.truncate();
            final PageFrameReduceTaskFactory reduceTaskFactory = () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_SQL_COMPILER);
            this.filterGenerator = new FilterFactoryGenerator(configuration, characterStore, jitIRMem, reduceTaskFactory, tmpSink,
                    tmpIndexes, tmpValues, tmpMasterKeys, tmpSlaveKeys, tmpLongs);
            this.aggregateGenerator = new AggregateFactoryGenerator(configuration, this, filterGenerator, asm, emptySchema, entityColumnFilter,
                    planTables, tmpIndexes, tmpValues);
            this.joinGenerator = new JoinFactoryGenerator(configuration, this, filterGenerator, functionResolver.getFunctionFactoryCache(), asm,
                    entityColumnFilter, reduceTaskFactory, tmpSink, tmpIds, tmpMasterKeys, tmpSlaveKeys);
            this.latestByGenerator = new LatestByFactoryGenerator(configuration, this, asm, tmpIndexes);
            this.projectionGenerator = new ProjectionFactoryGenerator();
            this.sampleByGenerator = new SampleByFactoryGenerator(configuration, this, functionResolver, asm, entityColumnFilter,
                    recordComparatorCompiler);
            this.scanGenerator = new ScanFactoryGenerator(configuration, filterGenerator, latestByGenerator, reduceTaskFactory,
                    planTables);
            this.sortGenerator = new SortFactoryGenerator(configuration, this, filterGenerator, projectionGenerator, asm, emptySchema, entityColumnFilter,
                    recordComparatorCompiler);
            this.setOperationGenerator = new SetOperationFactoryGenerator(configuration, this, sortGenerator, asm, entityColumnFilter);
            this.windowGenerator = new WindowFactoryGenerator(configuration, this, asm, entityColumnFilter, recordComparatorCompiler);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Wraps a union factory in the projection that re-symbolises the given STRING columns. Takes
     * ownership of {@code union}, including on failure.
     */
    @TestOnly
    public static RecordCursorFactory resymboliseUnion(RecordCursorFactory union, IntList symbolColumns) {
        return SetOperationFactoryGenerator.maybeResymboliseUnion(union, symbolColumns);
    }

    @Override
    public void clear() {
        for (int i = 0, n = generationFrames.size(); i < n; i++) {
            generationFrames.getQuick(i).clear();
        }
        if (generationFrames.size() > MAX_RETAINED_FRAMES) {
            generationFrames.remove(MAX_RETAINED_FRAMES, generationFrames.size() - 1);
        }
        generationDepth = 0;
    }

    @Override
    public void close() {
        final Throwable failure = Misc.freeObjListBestEffort(null, generationFrames);
        generationFrames.clear();
        Misc.free(jitIRMem);
        CairoException.rethrowCleanupFailure(failure);
    }

    public RecordCursorFactory generate(
            LogicalPlan root,
            FunctionInstantiator functionInstantiator,
            BoundExpressionRewriter expressionRewriter,
            TableFunctionSources functionSources,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (root == null) {
            throw new IllegalStateException("query is not bound");
        }
        if (generationFrames.size() == generationDepth) {
            generationFrames.add(new GenerationFrame(configuration, tmpSink, tmpLongs));
        }
        final GenerationFrame frame = generationFrames.getQuick(generationDepth++);
        try {
            frame.clear();
            frame.functionInstantiator = functionInstantiator;
            frame.intervalBounds.of(functionInstantiator);
            frame.expressionRewriter = expressionRewriter;
            frame.functionSources = functionSources;
            try {
                projectionGenerator.setReferenceCounts(frame, root.getOutput(), 1);
                projectionGenerator.collectColumnReferenceCounts(frame, root);
                return generate(frame, root, executionContext);
            } finally {
                frame.functionInstantiator = null;
                frame.intervalBounds.of(null);
                frame.expressionRewriter = null;
                frame.functionSources = null;
            }
        } finally {
            generationDepth--;
        }
    }

    /**
     * Consumes the generated query factory, or null for a statement without a query.
     */
    public RecordCursorFactory generateExplain(ExecutionModel model, @Nullable RecordCursorFactory factory, int format) {
        RecordCursorFactory owned = factory;
        try {
            if (model.getModelType() != ExecutionModel.QUERY) {
                owned = new RecordCursorFactoryStub(model, factory);
            }
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
        return new ExplainPlanFactory(owned, format);
    }

    public BytecodeAssembler getAsm() {
        return asm;
    }

    /**
     * The generation depth the next {@link #generate} call builds its plan at: 0 outside generation, one more for each
     * plan generating around it.
     */
    public int getGenerationDepth() {
        return generationDepth;
    }

    @TestOnly
    public int getGenerationFrameCount() {
        return generationFrames.size();
    }

    public ListColumnFilter getIndexColumnFilter() {
        return indexColumnFilter;
    }

    public RecordComparatorCompiler getRecordComparatorCompiler() {
        return recordComparatorCompiler;
    }

    // used in tests
    public void setEnableJitNullChecks(boolean value) {
        filterGenerator.setEnableJitNullChecks(value);
    }

    private static RecordCursorFactory declareTimestamp(RecordCursorFactory base, int timestampIndex) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final IntList mapping;
        try {
            final RecordMetadata baseMetadata = base.getMetadata();
            mapping = new IntList(baseMetadata.getColumnCount());
            for (int i = 0, n = baseMetadata.getColumnCount(); i < n; i++) {
                metadata.add(baseMetadata.getColumnMetadata(i));
                mapping.add(i);
            }
            metadata.setTimestampIndex(timestampIndex);
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return new SelectedRecordCursorFactory(metadata, mapping, base);
    }

    private static boolean isAgreed(PhysicalProperties.Capability derived, boolean actual) {
        return derived == PhysicalProperties.Capability.UNKNOWN || (derived == PhysicalProperties.Capability.YES) == actual;
    }

    private static String physicalPropertyMismatch(LogicalPlan plan, RecordCursorFactory factory) {
        if (!isAgreed(PhysicalProperties.supportsRandomAccess(plan), factory.recordCursorSupportsRandomAccess())) {
            return "derived random access differs from the factory's";
        }
        if (!isAgreed(PhysicalProperties.supportsPageFrameCursor(plan), factory.supportsPageFrameCursor())) {
            return "derived page-frame support differs from the factory's";
        }
        if (!isAgreed(PhysicalProperties.implementsLimit(plan), factory.implementsLimit())) {
            return "derived LIMIT implementation differs from the factory's";
        }
        if (!isAgreed(PhysicalProperties.isLongSequence(plan), SqlUtil.isLongSequence(factory))) {
            return "derived long_sequence() source differs from the factory's";
        }
        if (!isAgreed(PhysicalProperties.followsOrderAdvice(plan), factory.followedOrderByAdvice())) {
            return "derived order advice differs from the factory's";
        }
        final PhysicalProperties.ScanDirection direction = PhysicalProperties.scanDirection(plan);
        if (direction != PhysicalProperties.ScanDirection.UNKNOWN && direction != PhysicalProperties.ScanDirection.of(factory.getScanDirection())) {
            return "derived scan direction differs from the factory's";
        }
        final int timestampIndex = PhysicalProperties.timestampIndex(plan);
        if (timestampIndex != PhysicalProperties.UNKNOWN_TIMESTAMP && timestampIndex != factory.getMetadata().getTimestampIndex()) {
            return "derived designated timestamp differs from the factory's";
        }
        for (int i = 0, n = factory.getMetadata().getColumnCount(); i < n; i++) {
            if (!isAgreed(PhysicalProperties.supportsLongTopK(plan, i), factory.recordCursorSupportsLongTopK(i))) {
                return "derived long top-K support differs from the factory's";
            }
        }
        if (!isAgreed(PhysicalProperties.supportsTimeFrameCursor(plan), factory.supportsTimeFrameCursor())) {
            return "derived time-frame support differs from the factory's";
        }
        if (!isAgreed(PhysicalProperties.supportsSharedCursors(plan), factory.supportsSharedCursors())) {
            return "derived shared-cursor support differs from the factory's";
        }
        return null;
    }

    /**
     * The scan whose access path the factory of the plan is: a scan, a filter or LATEST BY fused into one.
     */
    private static ScanPlan scanSource(LogicalPlan plan) {
        LogicalPlan source = plan instanceof LatestByPlan latest ? latest.getInput() : plan;
        if (source instanceof FilterPlan) {
            source = source.inputAt(0);
        }
        return source instanceof ScanPlan scan ? scan : null;
    }

    /**
     * Asserts that every physical property the plan derives agrees with the factory codegen built for it. Outside a
     * table scan, codegen substitutes the empty factory wherever it proves a result empty, from facts the plan does
     * not carry; a scan builds it exactly where its access path is empty.
     */
    private static RecordCursorFactory verifyPhysicalProperties(LogicalPlan plan, RecordCursorFactory factory) {
        final ScanPlan scan = scanSource(plan);
        if (scan == null && factory instanceof EmptyTableRecordCursorFactory) {
            return factory;
        }
        final String mismatch;
        try {
            mismatch = scan != null && scan.getAccessPath() == ScanPlan.AccessPath.EMPTY && !(factory instanceof EmptyTableRecordCursorFactory)
                    ? "empty access path differs from the factory" : physicalPropertyMismatch(plan, factory);
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
        if (mismatch != null) {
            final AssertionError failure = new AssertionError(mismatch);
            Misc.free(factory, failure);
            throw failure;
        }
        return factory;
    }

    private RecordCursorFactory generateUnary(GenerationFrame frame, LogicalPlan plan, SqlExecutionContext executionContext) throws SqlException {
        final LogicalPlan input = plan.inputAt(0);
        final BoundExpression residual = plan instanceof FilterPlan filter ? filter.getPredicate() : null;
        final RecordCursorFactory base;
        if (residual != null && input instanceof ScanPlan scan
                && !scan.isWalClientUpdate()) {
            return scanGenerator.generateFiltered(frame, scan, residual, executionContext);
        } else if (plan instanceof LimitPlan limit && input instanceof DistinctPlan distinct) {
            base = aggregateGenerator.generateDistinct(frame, distinct, limit, executionContext);
        } else {
            base = generate(frame, input, executionContext);
        }
        if (plan instanceof FilterPlan filter) {
            final boolean isFused;
            try {
                isFused = WindowFactoryGenerator.fuseKeepFlagFilter(filter, base);
            } catch (Throwable th) {
                Misc.free(base, th);
                throw th;
            }
            if (isFused) {
                return base;
            }
        }
        switch (plan) {
            case FilterPlan filter -> {
                final Function predicate;
                try {
                    if (residual instanceof ConstantExpression constant) {
                        predicate = BooleanConstant.of(constant.getLongValue() != 0);
                    } else if (residual instanceof ColumnExpression column) {
                        final int index = input.getOutput().getColumnIndexById(column.getColumnId());
                        predicate = FunctionResolver.createColumn(column.getPosition(), index, base.getMetadata());
                    } else {
                        predicate = frame.functionInstantiator.instantiate(residual, input.getOutput(), base.getMetadata(), executionContext);
                    }
                } catch (Throwable th) {
                    Misc.free(base, th);
                    throw th;
                }
                if (input instanceof FunctionSourcePlan source) {
                    try {
                        scanGenerator.configurePushdown(frame, source, base, residual, executionContext);
                    } catch (Throwable th) {
                        Misc.free(predicate, th);
                        Misc.free(base, th);
                        throw th;
                    }
                }
                return filterGenerator.generate(frame, residual, input.getOutput(), base, predicate,
                        frame.functionInstantiator, executionContext, hasUpdateScan(input), filter.getAlgorithm() == FilterPlan.Algorithm.PARALLEL,
                        null, input instanceof ScanPlan scan && scan.hasHint(ScanPlan.HINT_PRE_TOUCH));
            }
            case ProjectPlan project -> {
                return projectionGenerator.generateProjection(frame, project, base, executionContext);
            }
            case SortPlan sort -> {
                return sort.getAlgorithm() == SortPlan.Algorithm.TIMESTAMP_DECLARATION
                        ? declareTimestamp(base, PhysicalProperties.timestampIndex(sort))
                        : sortGenerator.generate(frame, sort, base, null, null, 0);
            }
            case LimitPlan limit -> {
                if (limit.getApplication() == LimitPlan.Application.INPUT) {
                    return base;
                }
                Function lo = null;
                final Function hi;
                try {
                    lo = frame.functionInstantiator.instantiate(limit.getLo(), emptySchema, executionContext);
                    hi = limit.getHi() == null ? null : frame.functionInstantiator.instantiate(limit.getHi(), emptySchema, executionContext);
                } catch (Throwable th) {
                    Misc.free(lo, th);
                    Misc.free(base, th);
                    throw th;
                }
                return new LimitRecordCursorFactory(base, lo, hi, limit.getPosition());
            }
            default -> {
                final IllegalStateException failure = new IllegalStateException("unknown logical operation");
                Misc.free(base, failure);
                throw failure;
            }
        }
    }

    static RecordCursorFactory clearAfter(Mutable state, RecordCursorFactory factory) {
        try {
            state.clear();
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
        return factory;
    }

    static RecordCursorFactory closeAfter(Closeable resource, RecordCursorFactory factory) {
        try {
            Misc.free(resource);
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
        return factory;
    }

    static TableColumnMetadata copyColumn(RecordMetadata metadata, int index, String name) {
        final TableColumnMetadata source = metadata.getColumnMetadata(index);
        final TableColumnMetadata copy = new TableColumnMetadata(
                name,
                metadata.getColumnType(index),
                metadata.getColumnIndexType(index),
                metadata.getIndexValueBlockCapacity(index),
                metadata.isSymbolTableStatic(index),
                metadata.getMetadata(index),
                source.getWriterIndex(),
                source.isDedupKeyFlag(),
                source.getReplacingIndex(),
                source.isSymbolCacheFlag(),
                source.getSymbolCapacity(),
                source.getOriginalWriterIndex()
        );
        copy.setParquetEncodingConfig(source.getParquetEncodingConfig());
        if (source.getCoveringColumnIndices() != null) {
            copy.setCoveringColumnIndices(new IntList(source.getCoveringColumnIndices()));
        }
        return copy;
    }

    static boolean hasUpdateScan(LogicalPlan plan) {
        return !plan.walkTopDown(UPDATE_SCANS);
    }

    static int queryModelDirection(SortDirection direction) {
        return direction == SortDirection.DESCENDING ? QueryModel.ORDER_DIRECTION_DESCENDING : QueryModel.ORDER_DIRECTION_ASCENDING;
    }

    static int queryModelJoinType(JoinKind kind) {
        return switch (kind) {
            case CROSS -> QueryModel.JOIN_CROSS;
            case INNER -> QueryModel.JOIN_INNER;
            case LEFT_OUTER -> QueryModel.JOIN_LEFT_OUTER;
            case RIGHT_OUTER -> QueryModel.JOIN_RIGHT_OUTER;
            case FULL_OUTER -> QueryModel.JOIN_FULL_OUTER;
            case ASOF -> QueryModel.JOIN_ASOF;
            case LT -> QueryModel.JOIN_LT;
            case SPLICE -> QueryModel.JOIN_SPLICE;
            case UNNEST -> QueryModel.JOIN_UNNEST;
        };
    }

    static LogicalPlan unwrapColumnProjections(LogicalPlan plan) {
        while (plan instanceof ProjectPlan project && LogicalPlans.isColumnOnlyProjection(project)) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    RecordCursorFactory generate(GenerationFrame frame, LogicalPlan plan, SqlExecutionContext executionContext) throws SqlException {
        if (plan == frame.sharedHeadTarget) {
            frame.sharedHeadTarget = null;
            return new SharedRecordCursorFactory(frame.sharedHeadFactory, frame.sharedHeadId);
        }
        final RecordCursorFactory factory = switch (plan) {
            case WindowPlan window -> windowGenerator.generateWindow(frame, window, null, executionContext);
            case ProjectPlan project when project.inputAt(0) instanceof WindowPlan window && LogicalPlans.isWindowOutputProjection(project, window) ->
                    windowGenerator.generateWindow(frame, window, project, executionContext);
            case ProjectPlan project when project.inputAt(0) instanceof WindowJoinPlan windowJoin && LogicalPlans.isColumnOnlyProjection(project) ->
                    joinGenerator.generateWindowJoin(frame, windowJoin, project, executionContext);
            case LatestByPlan latest -> {
                final LogicalPlan source = latest.getInput() instanceof FilterPlan filter ? filter.getInput() : latest.getInput();
                yield source instanceof ScanPlan scan
                        ? scanGenerator.generateLatestBy(frame, latest, scan, executionContext)
                        : latestByGenerator.generateLatestBy(frame, latest, executionContext);
            }
            case SampleByPlan sample -> sampleByGenerator.generateSampleBy(frame, sample, executionContext);
            case FillPlan fill -> {
                final RecordCursorFactory base = generate(frame, fill.getInput(), executionContext);
                yield sampleByGenerator.generateFill(frame, fill, fill.getInput().getOutput(), base, executionContext);
            }
            case ScanPlan scan -> scanGenerator.generateScan(frame, scan, executionContext);
            case FunctionSourcePlan source -> scanGenerator.generateFunctionSource(frame, source, executionContext);
            case DistinctPlan distinct -> aggregateGenerator.generateDistinct(frame, distinct, null, executionContext);
            case LimitPlan limit when LogicalPlans.hasSortUnderStableProjects(limit.getInput()) ->
                    sortGenerator.generateSortedLimit(frame, limit.getInput(), limit, executionContext);
            case AggregatePlan aggregate -> aggregateGenerator.generateAggregate(frame, aggregate, executionContext);
            case JoinPlan join -> joinGenerator.generateJoin(frame, join, executionContext);
            case WindowJoinPlan windowJoin ->
                    joinGenerator.generateWindowJoin(frame, windowJoin, null, executionContext);
            case SetOperationPlan operation -> setOperationGenerator.generate(frame, operation, executionContext);
            default -> generateUnary(frame, plan, executionContext);
        };
        return ParanoiaState.PLAN_PARANOIA_MODE ? verifyPhysicalProperties(plan, factory) : factory;
    }

    /**
     * Builds the factory under a filter node a parallel consumer steals, without the filter, and prepares the filter
     * over it in {@code target}, which owns the filter function from then on, also on failure. The caller owns the
     * returned factory.
     */
    RecordCursorFactory generateStolenFilter(GenerationFrame frame, FilterPlan filter, PreparedFilter target, SqlExecutionContext executionContext)
            throws SqlException {
        final LogicalPlan input = filter.getInput();
        final BoundExpression predicate = filter.getPredicate();
        final RecordCursorFactory leaf;
        if (LogicalPlans.isFusedFilter(filter)) {
            leaf = scanGenerator.generateStolenFilter(frame, (ScanPlan) input, predicate, target, executionContext);
        } else {
            leaf = generate(frame, input, executionContext);
            try {
                final Function function;
                if (predicate instanceof ColumnExpression column) {
                    function = FunctionResolver.createColumn(column.getPosition(), input.getOutput().getColumnIndexById(column.getColumnId()), leaf.getMetadata());
                } else {
                    function = frame.functionInstantiator.instantiate(predicate, input.getOutput(), leaf.getMetadata(), executionContext);
                }
                target.of(predicate, input.getOutput(), function, filter.getAlgorithm() == FilterPlan.Algorithm.PARALLEL
                        && (!hasUpdateScan(input) || executionContext.isWalApplication()));
                if (input instanceof FunctionSourcePlan source) {
                    scanGenerator.configurePushdown(frame, source, leaf, predicate, executionContext);
                }
            } catch (Throwable th) {
                Misc.free(leaf, th);
                throw th;
            }
        }
        if (target.getFilter() == null) {
            final IllegalStateException failure = new IllegalStateException("stolen filter has no filter over the factory under it");
            Misc.free(leaf, failure);
            throw failure;
        }
        return leaf;
    }

    private static class RecordCursorFactoryStub implements RecordCursorFactory {
        private final int modelType;
        private final String tableName;
        private final String typeName;
        private RecordCursorFactory factory;

        protected RecordCursorFactoryStub(ExecutionModel model, RecordCursorFactory factory) {
            this.modelType = model.getModelType();
            this.tableName = Chars.toString(model.getTableName());
            this.typeName = model.getTypeName();
            this.factory = factory;
        }

        @Override
        public void close() {
            factory = Misc.free(factory);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            if (factory != null) {
                return factory.getCursor(executionContext);
            } else {
                return null;
            }
        }

        @Override
        public RecordMetadata getMetadata() {
            return null;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type(typeName);

            if (tableName != null) {
                sink.meta(modelType == CREATE_MAT_VIEW ? "view" : "table").val(tableName);
            }
            if (factory != null) {
                sink.child(factory);
            }
        }
    }

}

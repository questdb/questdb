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
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
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
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
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
import io.questdb.std.ObjectPool;
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
    private boolean fullFatJoins = false;
    private int generationDepth;

    public SqlCodeGenerator(
            CairoConfiguration configuration,
            FunctionParser functionParser,
            CharacterStore characterStore,
            BytecodeAssembler asm,
            EntityColumnFilter entityColumnFilter,
            OutputSchema emptySchema,
            StringSink tmpSink,
            IntHashSet tmpIds,
            IntList tmpIndexes,
            IntList tmpValues,
            IntList tmpMasterKeys,
            IntList tmpSlaveKeys,
            ObjectPool<SortPlan> sorts
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
            this.aggregateGenerator = new AggregateFactoryGenerator(configuration, this, asm, emptySchema, entityColumnFilter,
                    tmpIndexes, tmpValues, sorts);
            this.joinGenerator = new JoinFactoryGenerator(configuration, this, filterGenerator, functionParser.getFunctionFactoryCache(), asm,
                    entityColumnFilter, reduceTaskFactory, tmpSink, tmpIds, tmpMasterKeys, tmpSlaveKeys);
            this.latestByGenerator = new LatestByFactoryGenerator(configuration, this, asm, tmpIndexes);
            this.projectionGenerator = new ProjectionFactoryGenerator(tmpIndexes, tmpValues);
            this.sampleByGenerator = new SampleByFactoryGenerator(configuration, this, functionParser, asm, entityColumnFilter,
                    recordComparatorCompiler);
            this.scanGenerator = new ScanFactoryGenerator(configuration, filterGenerator, latestByGenerator, emptySchema, reduceTaskFactory);
            this.sortGenerator = new SortFactoryGenerator(configuration, this, projectionGenerator, asm, emptySchema, entityColumnFilter,
                    recordComparatorCompiler, sorts);
            this.setOperationGenerator = new SetOperationFactoryGenerator(configuration, this, sortGenerator, asm, entityColumnFilter);
            this.windowGenerator = new WindowFactoryGenerator(configuration, this, functionParser.getFunctionFactoryCache(), asm, entityColumnFilter, recordComparatorCompiler);
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
        if (generationFrames.size() > BindScopeStack.MAX_RETAINED_DEPTH) {
            generationFrames.remove(BindScopeStack.MAX_RETAINED_DEPTH, generationFrames.size() - 1);
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

    @TestOnly
    public int getGenerationFrameCount() {
        return generationFrames.size();
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

    public ListColumnFilter getIndexColumnFilter() {
        return indexColumnFilter;
    }

    public RecordComparatorCompiler getRecordComparatorCompiler() {
        return recordComparatorCompiler;
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

    private static boolean hasSortUnderStableProjects(LogicalPlan plan) {
        while (plan instanceof ProjectPlan project) {
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                if (!LogicalPlans.isOrderIndependent(project.getExpressions().getQuick(i))) {
                    return false;
                }
            }
            plan = project.getInput();
        }
        return plan instanceof SortPlan sort && !sort.isMarkoutHorizon() && sort.isLimited();
    }

    private static void rejectDerivedLatestWithoutTimestamp(LogicalPlan input) throws SqlException {
        if (!(input instanceof ProjectPlan)) {
            return;
        }
        do {
            input = input.inputAt(0);
        } while (input instanceof ProjectPlan);
        if (input instanceof LatestByPlan latest && latest.isTimestampOrderInherited()) {
            throw SqlException.$(latest.getPosition(), "TIMESTAMP column is required but not provided");
        }
    }

    private RecordCursorFactory generateUnary(
            GenerationFrame frame,
            LogicalPlan plan,
            SqlExecutionContext executionContext,
            int requiredOrderColumnId,
            int requiredScanDirection,
            SortPlan orderAdvice,
            LimitPlan limitAdvice,
            int orderByMnemonic
    ) throws SqlException {
        int inputOrderId;
        int inputScanDirection;
        SortPlan inputOrderAdvice = null;
        LimitPlan inputLimitAdvice = null;
        int inputOrderByMnemonic = orderByMnemonic;
        switch (plan) {
            case FilterPlan _ -> {
                inputOrderId = requiredOrderColumnId;
                inputScanDirection = requiredScanDirection;
                inputOrderAdvice = orderAdvice;
                inputLimitAdvice = plan.inputAt(0) instanceof ScanPlan ? limitAdvice : null;
            }
            case ProjectPlan project -> {
                if (project.hasTimestampDeclaration()) {
                    inputOrderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                }
                final int index = project.getOutput().getColumnIndexById(requiredOrderColumnId);
                if (index >= 0 && project.getExpressions().getQuick(index) instanceof ColumnExpression column) {
                    inputOrderId = column.getColumnId();
                    inputScanDirection = requiredScanDirection;
                } else {
                    inputScanDirection = RecordCursorFactory.SCAN_DIRECTION_OTHER;
                    inputOrderId = -1;
                }
                if (SortFactoryGenerator.hasNativeFilterInput(project)) {
                    inputOrderAdvice = sortGenerator.remapOrderAdvice(project, orderAdvice);
                    inputLimitAdvice = orderAdvice == null || inputOrderAdvice != null ? limitAdvice : null;
                } else if (project.getInput() instanceof AggregatePlan || project.getInput() instanceof WindowPlan
                        || SortFactoryGenerator.hasOrderedJoinMasterInput(project)) {
                    inputOrderAdvice = sortGenerator.remapOrderAdvice(project, orderAdvice);
                }
            }
            case SortPlan sort -> {
                inputOrderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                inputOrderAdvice = sort;
                inputLimitAdvice = limitAdvice;
                // This sort defines its own order; a parent's advice cannot override it.
                if (sort.getColumnIds().size() == 1) {
                    inputOrderId = sort.getColumnIds().getQuick(0);
                    inputScanDirection = sort.getDirections().getQuick(0) == SortDirection.DESCENDING
                            ? RecordCursorFactory.SCAN_DIRECTION_BACKWARD : RecordCursorFactory.SCAN_DIRECTION_FORWARD;
                } else {
                    inputScanDirection = RecordCursorFactory.SCAN_DIRECTION_OTHER;
                    inputOrderId = -1;
                }
            }
            case null, default -> {
                inputScanDirection = RecordCursorFactory.SCAN_DIRECTION_OTHER;
                inputOrderId = -1;
                if (plan instanceof LimitPlan limit) {
                    // The input order stays unset: changing a LIMIT input's direction changes the selected rows.
                    inputLimitAdvice = limit;
                    inputOrderByMnemonic = OrderByMnemonic.ORDER_BY_REQUIRED;
                }
            }
        }

        assert plan != null;
        final LogicalPlan input = plan.inputAt(0);
        final BoundExpression residual = plan instanceof FilterPlan filter ? filter.getPredicate() : null;
        final RecordCursorFactory base;
        if (residual != null && input instanceof ScanPlan scan
                && !ScanFactoryGenerator.isWalClientUpdate(scan, executionContext)) {
            return scanGenerator.generateFiltered(frame, scan, residual, inputOrderId, inputScanDirection,
                    inputOrderAdvice, inputLimitAdvice, inputOrderByMnemonic, executionContext);
        } else if (plan instanceof LimitPlan limit && input instanceof DistinctPlan distinct) {
            base = aggregateGenerator.generateDistinct(frame, distinct, limit, executionContext);
        } else if (plan instanceof SortPlan sort) {
            base = sortGenerator.generateSortInput(frame, sort, executionContext, inputOrderId, inputScanDirection, inputLimitAdvice);
        } else {
            base = generate(frame, input, executionContext, inputOrderId, inputScanDirection, inputOrderAdvice, inputLimitAdvice, inputOrderByMnemonic);
        }
        if (residual instanceof ColumnExpression column
                && WindowFactoryGenerator.tryFuseKeepFlagFilter(base, input.getOutput().getColumnIndexById(column.getColumnId()))) {
            return base;
        }
        switch (plan) {
            case SortPlan _ when base.followedOrderByAdvice() && SortFactoryGenerator.hasAdvisedInput(input) -> {
                return base;
            }
            case SortPlan sort when base.followedOrderByAdvice() && sort.isMarkoutHorizon() -> {
                final int timestampIndex = sort.getOutput().getTimestampIndex();
                return timestampIndex < 0 || timestampIndex == base.getMetadata().getTimestampIndex() || !executionContext.isTimestampRequired()
                        ? base : declareTimestamp(base, timestampIndex);
            }
            case LimitPlan _ when base.implementsLimit() && SortFactoryGenerator.hasNativeFilterInput(input) -> {
                return base;
            }
            case SortPlan _ when inputOrderId >= 0 && base.getMetadata().getTimestampIndex() == input.getOutput().getColumnIndexById(inputOrderId) && base.getScanDirection() == inputScanDirection -> {
                // Remove this one-key sort only when the actual generated input proves it.
                return base;
            }
            default -> {
            }
        }
        switch (plan) {
            case FilterPlan _ -> {
                final Function predicate;
                try {
                    if (residual instanceof ConstantExpression constant) {
                        predicate = BooleanConstant.of(constant.getLongValue() != 0);
                    } else if (residual instanceof ColumnExpression column) {
                        final int index = input.getOutput().getColumnIndexById(column.getColumnId());
                        predicate = FunctionParser.createColumn(column.getPosition(), index, base.getMetadata());
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
                        frame.functionInstantiator, executionContext, hasUpdateScan(input), null,
                        input instanceof ScanPlan scan && scan.hasHint(ScanPlan.HINT_PRE_TOUCH));
            }
            case ProjectPlan project -> {
                final int timestampIndex = orderAdvice != null && base.getMetadata().getTimestampIndex() < 0
                        && SortFactoryGenerator.hasNativeFilterInput(project) ? -1
                        : project.getOutput().getTimestampIndex() < 0 && inputOrderId >= 0
                        && base.getMetadata().getTimestampIndex() == input.getOutput().getColumnIndexById(inputOrderId)
                          ? project.getOutput().getColumnIndexById(requiredOrderColumnId) : project.getOutput().getTimestampIndex();
                return projectionGenerator.generateProjection(frame, project, base, timestampIndex, executionContext);
            }
            case SortPlan sort -> {
                return sortGenerator.generate(frame, sort, base, null, null, 0, executionContext);
            }
            case LimitPlan limit -> {
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

    static RecordCursorFactory closeAfter(Closeable resource, RecordCursorFactory factory) {
        try {
            Misc.free(resource);
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
        return factory;
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

    static boolean hasColumns(OutputSchema schema, SortPlan advice) {
        if (advice == null) {
            return false;
        }
        for (int i = 0, n = advice.getColumnIds().size(); i < n; i++) {
            if (schema.getColumnIndexById(advice.getColumnIds().getQuick(i)) < 0) {
                return false;
            }
        }
        return true;
    }

    static boolean hasUpdateScan(LogicalPlan plan) {
        if (plan instanceof ScanPlan scan) {
            return scan.isUpdate();
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            if (hasUpdateScan(plan.inputAt(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true when the base factory delivers rows in ascending designated-timestamp order, which the
     * TWAP and sparkline aggregates require: their single-batch step-function integration trusts each page
     * frame to already be sorted by the designated timestamp. A forward scan alone is not enough. A sort by
     * a non-timestamp column also reports SCAN_DIRECTION_FORWARD, yet it drops the designated timestamp from
     * its metadata (so getTimestampIndex() no longer matches the timestampIndex that an outer timestamp(col)
     * clause re-attaches by name). Requiring the metadata timestamp to equal timestampIndex rejects such
     * reordered bases, which would otherwise feed the aggregates rows out of timestamp order.
     */
    static boolean isBaseTimestampAscending(RecordCursorFactory factory, int timestampIndex) {
        return factory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD
                && factory.getMetadata().getTimestampIndex() == timestampIndex;
    }

    static boolean isColumnOnlyProjection(ProjectPlan project) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column)
                    || isColumnSelectedBefore(project, i, column.getColumnId())) {
                return false;
            }
        }
        return true;
    }

    static boolean isColumnSelectedBefore(ProjectPlan project, int index, int columnId) {
        for (int k = 0; k < index; k++) {
            if (((ColumnExpression) project.getExpressions().getQuick(k)).getColumnId() == columnId) {
                return true;
            }
        }
        return false;
    }

    static boolean isTimestampDeclarationOnly(LogicalPlan plan) {
        if (!(plan instanceof ProjectPlan project) || !project.hasTimestampDeclaration()) {
            return false;
        }
        final OutputSchema input = project.getInput().getOutput();
        if (project.getExpressions().size() != input.getColumnCount()) {
            return false;
        }
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column) || column.isCast() || !column.isDirectReference()
                    || input.getColumnIndexById(column.getColumnId()) != i || input.getColumnType(i) != project.getOutput().getColumnType(i)
                    || !Chars.equals(input.getColumnName(i), project.getOutput().getColumnName(i))) {
                return false;
            }
        }
        return true;
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
        while (plan instanceof ProjectPlan project && isColumnOnlyProjection(project)) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    RecordCursorFactory generate(
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
            frame.expressionRewriter = expressionRewriter;
            frame.functionSources = functionSources;
            try {
                projectionGenerator.setReferenceCounts(frame, root.getOutput(), 1);
                projectionGenerator.collectColumnReferenceCounts(frame, root);
                aggregateGenerator.countSharedConsumers(frame, root);
                return generate(frame, root, executionContext);
            } finally {
                frame.functionInstantiator = null;
                frame.expressionRewriter = null;
                frame.functionSources = null;
            }
        } finally {
            generationDepth--;
        }
    }

    RecordCursorFactory generate(GenerationFrame frame, LogicalPlan plan, SqlExecutionContext executionContext) throws SqlException {
        return generate(frame, plan, executionContext, -1, RecordCursorFactory.SCAN_DIRECTION_OTHER);
    }

    RecordCursorFactory generate(GenerationFrame frame, LogicalPlan plan, SqlExecutionContext executionContext, int requiredOrderColumnId, int requiredScanDirection) throws SqlException {
        return generate(frame, plan, executionContext, requiredOrderColumnId, requiredScanDirection, null, null, OrderByMnemonic.ORDER_BY_REQUIRED);
    }

    RecordCursorFactory generate(GenerationFrame frame, LogicalPlan plan, SqlExecutionContext executionContext, int requiredOrderColumnId, int requiredScanDirection,
                                 SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic) throws SqlException {
        if (plan == frame.sharedHeadTarget) {
            frame.sharedHeadTarget = null;
            return new SharedRecordCursorFactory(frame.sharedHeadFactory, frame.sharedHeadId);
        }
        return switch (plan) {
            case WindowPlan window ->
                    windowGenerator.generateWindow(frame, window, null, requiredOrderColumnId, requiredScanDirection, orderAdvice,
                            window.isSelectOrdered() && orderAdvice != null && orderAdvice.getInput() == plan, orderByMnemonic, executionContext);
            case ProjectPlan project when project.inputAt(0) instanceof WindowPlan window && WindowFactoryGenerator.isWindowOutputProjection(project, window) -> {
                final int index = project.getOutput().getColumnIndexById(requiredOrderColumnId);
                yield windowGenerator.generateWindow(frame, window, project,
                        index >= 0 && project.getExpressions().getQuick(index) instanceof ColumnExpression column
                                ? column.getColumnId() : -1, requiredScanDirection, sortGenerator.remapOrderAdvice(project, orderAdvice),
                        window.isSelectOrdered() && orderAdvice != null && orderAdvice.getInput() == plan,
                        orderByMnemonic, executionContext);
            }
            case ProjectPlan project when project.inputAt(0) instanceof WindowJoinPlan windowJoin && isColumnOnlyProjection(project) ->
                    joinGenerator.generateWindowJoin(frame, windowJoin, project, executionContext, orderByMnemonic);
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
            case ScanPlan scan -> {
                final int order = requiredOrderColumnId >= 0 && requiredOrderColumnId == scan.getOutput().getTimestampColumnId()
                        && requiredScanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD
                        ? PartitionFrameCursorFactory.ORDER_DESC : PartitionFrameCursorFactory.ORDER_ASC;
                yield scanGenerator.generateScan(frame, scan, executionContext, order);
            }
            case FunctionSourcePlan source -> scanGenerator.generateFunctionSource(frame, source, executionContext);
            case DistinctPlan distinct -> aggregateGenerator.generateDistinct(frame, distinct, null, executionContext);
            case LimitPlan limit when hasSortUnderStableProjects(limit.getInput()) ->
                    sortGenerator.generateSortedLimit(frame, limit.getInput(), limit, executionContext);
            case AggregatePlan aggregate ->
                    aggregateGenerator.generateAggregate(frame, aggregate, orderAdvice, executionContext, requiredOrderColumnId, requiredScanDirection);
            case JoinPlan join ->
                    joinGenerator.generateJoin(frame, join, requiredOrderColumnId, requiredScanDirection, orderAdvice,
                            orderByMnemonic, executionContext);
            case WindowJoinPlan windowJoin ->
                    joinGenerator.generateWindowJoin(frame, windowJoin, null, executionContext, orderByMnemonic);
            case SetOperationPlan operation ->
                    setOperationGenerator.generate(frame, operation, requiredOrderColumnId, requiredScanDirection,
                            orderByMnemonic, executionContext);
            default ->
                    generateUnary(frame, plan, executionContext, requiredOrderColumnId, requiredScanDirection, orderAdvice, limitAdvice,
                            orderByMnemonic);
        };
    }

    RecordCursorFactory generateJoinInput(GenerationFrame frame, LogicalPlan input, SqlExecutionContext executionContext, boolean isTimestampRequired, int orderByMnemonic) throws SqlException {
        return generateJoinInput(frame, input, executionContext, isTimestampRequired, orderByMnemonic, -1, RecordCursorFactory.SCAN_DIRECTION_OTHER, null);
    }

    RecordCursorFactory generateJoinInput(GenerationFrame frame, LogicalPlan input, SqlExecutionContext executionContext, boolean isTimestampRequired, int orderByMnemonic,
                                          int requiredOrderColumnId, int requiredScanDirection, SortPlan orderAdvice) throws SqlException {
        executionContext.pushTimestampRequiredFlag(isTimestampRequired);
        try {
            final RecordCursorFactory factory = generate(frame, input, executionContext, requiredOrderColumnId, requiredScanDirection, orderAdvice, null,
                    isTimestampRequired ? OrderByMnemonic.ORDER_BY_REQUIRED : orderByMnemonic);
            if (isTimestampRequired && factory.getMetadata().getTimestampIndex() < 0) {
                try {
                    rejectDerivedLatestWithoutTimestamp(input);
                } catch (Throwable th) {
                    Misc.free(factory, th);
                    throw th;
                }
            }
            return factory;
        } finally {
            executionContext.popTimestampRequiredFlag();
        }
    }

    boolean isFullFatJoins() {
        return fullFatJoins;
    }

    // used in tests
    void setEnableJitNullChecks(boolean value) {
        filterGenerator.setEnableJitNullChecks(value);
    }

    void setFullFatJoins(boolean fullFatJoins) {
        this.fullFatJoins = fullFatJoins;
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

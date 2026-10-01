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

import io.questdb.cairo.ArrayColumnTypes;
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
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.BitSet;
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
    private final ObjectPool<IntList> intListPool = new ObjectPool<>(IntList::new, 4);
    private final MemoryCARW jitIRMem;
    private final JoinFactoryGenerator joinGenerator;
    private final LatestByFactoryGenerator latestByGenerator;
    // this list is used to generate record sinks
    private final ListColumnFilter listColumnFilterA = new ListColumnFilter();
    private final LongList longScratch = new LongList();
    private final ProjectionFactoryGenerator projectionGenerator;
    private final RecordComparatorCompiler recordComparatorCompiler;
    private final SampleByFactoryGenerator sampleByGenerator;
    private final ScanFactoryGenerator scanGenerator;
    private final StringSink scratchSink;
    private final SetOperationFactoryGenerator setOperationGenerator;
    private final SortFactoryGenerator sortGenerator;
    private final WindowFactoryGenerator windowGenerator;
    private boolean fullFatJoins = false;
    private int generationDepth;
    @Nullable
    private LogicalGenerationTestHook logicalGenerationTestHook;

    @TestOnly
    public SqlCodeGenerator(
            CairoConfiguration configuration,
            FunctionParser functionParser,
            ObjectPool<ExpressionNode> expressionNodePool
    ) {
        this(
                configuration,
                functionParser,
                expressionNodePool,
                new CharacterStore(configuration.getSqlCharacterStoreCapacity(), configuration.getSqlCharacterStoreSequencePoolCapacity()),
                new BytecodeAssembler(),
                new EntityColumnFilter(),
                new OutputSchema(),
                new StringSink(),
                new IntHashSet()
        );
    }

    public SqlCodeGenerator(
            CairoConfiguration configuration,
            FunctionParser functionParser,
            ObjectPool<ExpressionNode> expressionNodePool,
            CharacterStore characterStore,
            BytecodeAssembler asm,
            EntityColumnFilter entityColumnFilter,
            OutputSchema emptySchema,
            StringSink scratchSink,
            IntHashSet idScratch
    ) {
        try {
            this.configuration = configuration;
            this.asm = asm;
            this.emptySchema = emptySchema;
            this.scratchSink = scratchSink;
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
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            final ListColumnFilter listColumnFilterB = new ListColumnFilter();
            final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            final IntList indexScratch = new IntList();
            final IntList valueScratch = new IntList();
            final BitSet symbolScratch = new BitSet();
            this.filterGenerator = new FilterFactoryGenerator(configuration, expressionNodePool, characterStore, jitIRMem, reduceTaskFactory, scratchSink);
            this.aggregateGenerator = new AggregateFactoryGenerator(configuration, this, asm, emptySchema, entityColumnFilter,
                    indexScratch, valueScratch);
            this.joinGenerator = new JoinFactoryGenerator(configuration, this, filterGenerator, functionParser, asm, entityColumnFilter,
                    keyTypes, valueTypes, listColumnFilterA, listColumnFilterB, reduceTaskFactory, scratchSink, indexScratch, valueScratch,
                    idScratch, symbolScratch);
            this.latestByGenerator = new LatestByFactoryGenerator(configuration, this, asm, keyTypes, listColumnFilterA, indexScratch, longScratch);
            this.projectionGenerator = new ProjectionFactoryGenerator(indexScratch, valueScratch);
            this.sampleByGenerator = new SampleByFactoryGenerator(configuration, this, functionParser, asm, entityColumnFilter, intListPool,
                    keyTypes, valueTypes, listColumnFilterA, recordComparatorCompiler);
            this.scanGenerator = new ScanFactoryGenerator(configuration, filterGenerator, latestByGenerator, emptySchema, reduceTaskFactory);
            this.sortGenerator = new SortFactoryGenerator(configuration, this, projectionGenerator, asm, emptySchema, entityColumnFilter,
                    recordComparatorCompiler, listColumnFilterB);
            this.setOperationGenerator = new SetOperationFactoryGenerator(configuration, this, sortGenerator, asm, entityColumnFilter, keyTypes, valueTypes,
                    listColumnFilterB, symbolScratch);
            this.windowGenerator = new WindowFactoryGenerator(configuration, this, asm, entityColumnFilter, recordComparatorCompiler);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        for (int i = 0, n = generationFrames.size(); i < n; i++) {
            generationFrames.getQuick(i).clear();
        }
        generationDepth = 0;
        intListPool.clear();
    }

    @Override
    public void close() {
        final Throwable failure = Misc.freeObjListBestEffort(null, generationFrames);
        generationFrames.clear();
        // Bound the test hook by this generator's lifetime: clear() runs on every compile, so it
        // cannot own the reset, but a hook must never outlive the compiler that installed it.
        logicalGenerationTestHook = null;
        if (setOperationGenerator != null) {
            setOperationGenerator.setUnionSymbolProjectionTestHook(null);
        }
        Misc.free(jitIRMem);
        CairoException.rethrowCleanupFailure(failure);
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
            return new ExplainPlanFactory(owned, format);
        } catch (Throwable th) {
            Misc.free(owned, th);
            throw th;
        }
    }

    public BytecodeAssembler getAsm() {
        return asm;
    }

    public ListColumnFilter getIndexColumnFilter() {
        return listColumnFilterA;
    }

    public RecordComparatorCompiler getRecordComparatorCompiler() {
        return recordComparatorCompiler;
    }

    @TestOnly
    public void setLogicalGenerationTestHook(@Nullable LogicalGenerationTestHook hook) {
        logicalGenerationTestHook = hook;
    }

    @TestOnly
    public void setUnionSymbolProjectionTestHook(@Nullable UnionSymbolProjectionTestHook hook) {
        setOperationGenerator.setUnionSymbolProjectionTestHook(hook);
    }

    public IntList toOrderIndices(RecordMetadata m, ObjList<ExpressionNode> orderBy, IntList orderByDirection) throws SqlException {
        final IntList indices = intListPool.next();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            ExpressionNode tok = orderBy.getQuick(i);
            int index = SqlUtil.getColumnIndexQuiet(m, tok.token);
            if (index == -1) {
                throw SqlException.invalidColumn(tok.position, tok.token);
            }

            // shift index by 1 to use sign as sort direction
            index++;

            // negative column index means descending order of sort
            if (orderByDirection.getQuick(i) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                index = -index;
            }

            indices.add(index);
        }
        return indices;
    }

    private static boolean hasSortUnderStableProjects(LogicalPlan plan) {
        while (plan instanceof ProjectPlan project) {
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                final int flags = project.getExpressions().getQuick(i).getFunctionFlags();
                if ((flags & BoundExpression.STABLE_WITHIN_EXECUTION) == 0
                        || (flags & BoundExpression.NON_DETERMINISTIC) != 0) {
                    return false;
                }
            }
            plan = project.getInput();
        }
        return plan instanceof SortPlan sort && !sort.isMarkoutHorizon() && sort.isLimited();
    }

    private static void rejectDerivedLatestWithoutTimestamp(LogicalPlan input) throws SqlException {
        if (input.getType() != LogicalPlan.Type.PROJECT) {
            return;
        }
        do {
            input = input.inputAt(0);
        } while (input.getType() == LogicalPlan.Type.PROJECT);
        if (input instanceof LatestByPlan latest && latest.isTimestampOrderInherited()) {
            throw SqlException.$(latest.getPosition(), "TIMESTAMP column is required but not provided");
        }
    }

    private int declareTimestamp(GenerationFrame frame, int inputSlot, int timestampIndex) {
        final RecordCursorFactory base = (RecordCursorFactory) frame.resources.resources.getQuick(inputSlot);
        final RecordMetadata baseMetadata = base.getMetadata();
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final IntList mapping = new IntList(baseMetadata.getColumnCount());
        for (int i = 0, n = baseMetadata.getColumnCount(); i < n; i++) {
            metadata.add(baseMetadata.getColumnMetadata(i));
            mapping.add(i);
        }
        metadata.setTimestampIndex(timestampIndex);
        final int slot = frame.resources.reserve();
        final RecordCursorFactory factory = new SelectedRecordCursorFactory(metadata, mapping, base);
        frame.resources.detach(inputSlot);
        frame.resources.own(slot, factory);
        return slot;
    }

    private int generateUnary(
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
                    inputOrderAdvice = sortGenerator.remapOrderAdvice(frame, project, orderAdvice);
                    inputLimitAdvice = orderAdvice == null || inputOrderAdvice != null ? limitAdvice : null;
                } else if (project.getInput().getType() == LogicalPlan.Type.AGGREGATE
                        || project.getInput().getType() == LogicalPlan.Type.WINDOW
                        || SortFactoryGenerator.hasOrderedJoinMasterInput(project)) {
                    inputOrderAdvice = sortGenerator.remapOrderAdvice(frame, project, orderAdvice);
                }
            }
            case SortPlan sort -> {
                inputOrderByMnemonic = OrderByMnemonic.ORDER_BY_INVARIANT;
                inputOrderAdvice = sort;
                inputLimitAdvice = limitAdvice;
                // This sort defines its own order; a parent's advice cannot override it.
                if (sort.getColumnIds().size() == 1) {
                    inputOrderId = sort.getColumnIds().getQuick(0);
                    inputScanDirection = sort.getDirections().getQuick(0) == QueryModel.ORDER_DIRECTION_DESCENDING
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
        final int inputSlot;
        if (residual != null && input instanceof ScanPlan scan
                && !ScanFactoryGenerator.isWalClientUpdate(scan, executionContext)) {
            return scanGenerator.generateFiltered(frame, scan, residual, inputOrderId, inputScanDirection,
                    inputOrderAdvice, inputLimitAdvice, inputOrderByMnemonic, executionContext);
        } else if (plan instanceof LimitPlan limit && input instanceof DistinctPlan distinct) {
            inputSlot = aggregateGenerator.generateDistinct(frame, distinct, limit, executionContext);
        } else if (plan instanceof SortPlan sort) {
            inputSlot = sortGenerator.generateSortInput(frame, sort, executionContext, inputOrderId, inputScanDirection, inputLimitAdvice);
        } else {
            inputSlot = generate(frame, input, executionContext, inputOrderId, inputScanDirection, inputOrderAdvice, inputLimitAdvice, inputOrderByMnemonic);
        }
        final RecordCursorFactory base = (RecordCursorFactory) frame.resources.resources.getQuick(inputSlot);
        if (residual instanceof ColumnExpression column
                && WindowFactoryGenerator.tryFuseKeepFlagFilter(base, input.getOutput().getColumnIndexById(column.getColumnId()))) {
            return inputSlot;
        }
        switch (plan) {
            case SortPlan _ when base.followedOrderByAdvice() && SortFactoryGenerator.hasAdvisedInput(input) -> {
                return inputSlot;
            }
            case SortPlan sort when base.followedOrderByAdvice() && sort.isMarkoutHorizon() -> {
                final int timestampIndex = sort.getOutput().getTimestampIndex();
                return timestampIndex < 0 || timestampIndex == base.getMetadata().getTimestampIndex() || !executionContext.isTimestampRequired()
                        ? inputSlot : declareTimestamp(frame, inputSlot, timestampIndex);
            }
            case LimitPlan _ when base.implementsLimit() && SortFactoryGenerator.hasNativeFilterInput(input) -> {
                return inputSlot;
            }
            case SortPlan _ when inputOrderId >= 0 && base.getMetadata().getTimestampIndex() == input.getOutput().getColumnIndexById(inputOrderId) && base.getScanDirection() == inputScanDirection -> {
                // Remove this one-key sort only when the actual generated input proves it.
                return inputSlot;
            }
            default -> {
            }
        }
        final int slot = frame.resources.reserve();
        switch (plan) {
            case FilterPlan _ -> {
                final int predicateSlot = frame.resources.reserve();
                final Function predicate;
                if (residual instanceof ConstantExpression constant) {
                    predicate = BooleanConstant.of(constant.getLongValue() != 0);
                } else if (residual instanceof ColumnExpression column) {
                    final int index = input.getOutput().getColumnIndexById(column.getColumnId());
                    predicate = FunctionParser.createColumn(column.getPosition(), index, base.getMetadata());
                } else {
                    predicate = frame.functionBinder.instantiate(residual, input.getOutput(), base.getMetadata(), executionContext);
                }
                frame.resources.own(predicateSlot, predicate);
                if (input instanceof FunctionSourcePlan source) {
                    scanGenerator.configurePushdown(frame, source, base, residual, executionContext);
                }
                frame.resources.detach(inputSlot);
                frame.resources.detach(predicateSlot);
                frame.resources.own(slot, filterGenerator.generate(frame, residual, input.getOutput(), base, predicate,
                        frame.functionBinder, executionContext, frame.isUpdate && hasUpdateScan(input), null,
                        input instanceof ScanPlan scan && scan.hasHint(ScanPlan.HINT_PRE_TOUCH)));
                return slot;
            }
            case ProjectPlan project -> {
                final int timestampIndex = orderAdvice != null && base.getMetadata().getTimestampIndex() < 0
                        && SortFactoryGenerator.hasNativeFilterInput(project) ? -1
                        : project.getOutput().getTimestampIndex() < 0 && inputOrderId >= 0
                        && base.getMetadata().getTimestampIndex() == input.getOutput().getColumnIndexById(inputOrderId)
                          ? project.getOutput().getColumnIndexById(requiredOrderColumnId) : project.getOutput().getTimestampIndex();
                return projectionGenerator.generateProjection(frame, project, inputSlot, slot, timestampIndex, executionContext);
            }
            case SortPlan sort -> {
                frame.resources.detach(inputSlot);
                frame.resources.own(slot, sortGenerator.generate(sort, base, null, null, 0, executionContext, frame.functionBinder));
                return slot;
            }
            default -> {
            }
        }
        if (!(plan instanceof LimitPlan limit)) {
            throw new IllegalStateException("unknown logical operation");
        }
        final int loSlot = frame.resources.reserve();
        final Function lo = frame.functionBinder.instantiate(limit.getLo(), emptySchema, executionContext);
        frame.resources.own(loSlot, lo);
        final int hiSlot = frame.resources.reserve();
        final Function hi = limit.getHi() == null ? null : frame.functionBinder.instantiate(limit.getHi(), emptySchema, executionContext);
        if (hi != null) {
            frame.resources.own(hiSlot, hi);
        }
        final RecordCursorFactory factory = new LimitRecordCursorFactory(base, lo, hi, limit.getPosition());
        frame.resources.detach(loSlot);
        if (hi != null) {
            frame.resources.detach(hiSlot);
        }
        frame.resources.detach(inputSlot);
        frame.resources.own(slot, factory);
        return slot;
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

    static LogicalPlan unwrapColumnProjections(LogicalPlan plan) {
        while (plan instanceof ProjectPlan project && isColumnOnlyProjection(project)) {
            plan = plan.inputAt(0);
        }
        return plan;
    }

    RecordCursorFactory generate(
            LogicalPlan root,
            FunctionBinder functionBinder,
            TableFunctionSources functionSources,
            boolean isUpdate,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (root == null) {
            throw new IllegalStateException("query is not bound");
        }
        if (generationFrames.size() == generationDepth) {
            generationFrames.add(new GenerationFrame(configuration, scratchSink, longScratch));
        }
        final GenerationFrame frame = generationFrames.getQuick(generationDepth++);
        try {
            frame.clear();
            frame.functionBinder = functionBinder;
            frame.functionSources = functionSources;
            frame.isUpdate = isUpdate;
            try {
                if (ALLOW_FUNCTION_MEMOIZATION) {
                    projectionGenerator.setReferenceCounts(frame, root.getOutput(), 1);
                    projectionGenerator.collectColumnReferenceCounts(frame, root);
                }
                aggregateGenerator.countSharedConsumers(frame, root);
                final int slot = generate(frame, root, executionContext);
                final Throwable cleanup = frame.closePrepared(frame.resources.closeOwned(slot, null));
                if (cleanup != null) {
                    CairoException.rethrowCleanupFailure(frame.resources.closeOwned(-1, cleanup));
                }
                return (RecordCursorFactory) frame.resources.detach(slot);
            } catch (Throwable e) {
                frame.resources.closeOwned(e);
                final Throwable failure = frame.closePrepared(e);
                assert failure == e;
                throw e;
            } finally {
                frame.functionBinder = null;
                frame.functionSources = null;
                frame.isUpdate = false;
            }
        } finally {
            generationDepth--;
        }
    }

    int generate(GenerationFrame frame, LogicalPlan plan, SqlExecutionContext executionContext) throws SqlException {
        return generate(frame, plan, executionContext, -1, RecordCursorFactory.SCAN_DIRECTION_OTHER);
    }

    int generate(GenerationFrame frame, LogicalPlan plan, SqlExecutionContext executionContext, int requiredOrderColumnId, int requiredScanDirection) throws SqlException {
        return generate(frame, plan, executionContext, requiredOrderColumnId, requiredScanDirection, null, null, OrderByMnemonic.ORDER_BY_REQUIRED);
    }

    int generate(GenerationFrame frame, LogicalPlan plan, SqlExecutionContext executionContext, int requiredOrderColumnId, int requiredScanDirection,
                 SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic) throws SqlException {
        final LogicalGenerationTestHook testHook = logicalGenerationTestHook;
        if (testHook != null) {
            testHook.onGenerate(plan);
        }
        if (plan == frame.sharedHeadTarget) {
            frame.sharedHeadTarget = null;
            final int slot = frame.resources.reserve();
            frame.resources.own(slot, new SharedRecordCursorFactory(frame.sharedHeadFactory, frame.sharedHeadId));
            return slot;
        }
        return switch (plan) {
            case WindowPlan window -> windowGenerator.generateWindow(frame, window, null, requiredOrderColumnId, requiredScanDirection, orderAdvice,
                    window.isSelectOrdered() && orderAdvice != null && orderAdvice.getInput() == plan, orderByMnemonic, executionContext);
            case ProjectPlan project when project.inputAt(0) instanceof WindowPlan window && WindowFactoryGenerator.isWindowOutputProjection(project, window) -> {
                final int index = project.getOutput().getColumnIndexById(requiredOrderColumnId);
                yield windowGenerator.generateWindow(frame, window, project,
                        index >= 0 && project.getExpressions().getQuick(index) instanceof ColumnExpression column
                                ? column.getColumnId() : -1, requiredScanDirection, sortGenerator.remapOrderAdvice(frame, project, orderAdvice),
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
                final int inputSlot = generate(frame, fill.getInput(), executionContext);
                final int slot = frame.resources.reserve();
                final RecordCursorFactory base = (RecordCursorFactory) frame.resources.detach(inputSlot);
                frame.resources.own(slot, sampleByGenerator.generateFill(fill, fill.getInput().getOutput(), base, frame.functionBinder, executionContext));
                yield slot;
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
            case JoinPlan join -> joinGenerator.generateJoin(frame, join, requiredOrderColumnId, requiredScanDirection, orderAdvice,
                    orderByMnemonic, executionContext);
            case WindowJoinPlan windowJoin -> joinGenerator.generateWindowJoin(frame, windowJoin, null, executionContext, orderByMnemonic);
            case SetOperationPlan operation -> setOperationGenerator.generate(frame, operation, requiredOrderColumnId, requiredScanDirection,
                    orderByMnemonic, executionContext);
            default -> generateUnary(frame, plan, executionContext, requiredOrderColumnId, requiredScanDirection, orderAdvice, limitAdvice,
                    orderByMnemonic);
        };
    }

    int generateJoinInput(GenerationFrame frame, LogicalPlan input, SqlExecutionContext executionContext, boolean isTimestampRequired, int orderByMnemonic) throws SqlException {
        return generateJoinInput(frame, input, executionContext, isTimestampRequired, orderByMnemonic, -1, RecordCursorFactory.SCAN_DIRECTION_OTHER, null);
    }

    int generateJoinInput(GenerationFrame frame, LogicalPlan input, SqlExecutionContext executionContext, boolean isTimestampRequired, int orderByMnemonic,
                          int requiredOrderColumnId, int requiredScanDirection, SortPlan orderAdvice) throws SqlException {
        executionContext.pushTimestampRequiredFlag(isTimestampRequired);
        try {
            final int slot = generate(frame, input, executionContext, requiredOrderColumnId, requiredScanDirection, orderAdvice, null,
                    isTimestampRequired ? OrderByMnemonic.ORDER_BY_REQUIRED : orderByMnemonic);
            if (isTimestampRequired && ((RecordCursorFactory) frame.resources.resources.getQuick(slot)).getMetadata().getTimestampIndex() < 0) {
                rejectDerivedLatestWithoutTimestamp(input);
            }
            return slot;
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

    @TestOnly
    @FunctionalInterface
    public interface LogicalGenerationTestHook {
        void onGenerate(LogicalPlan plan) throws SqlException;
    }

    @TestOnly
    public interface UnionSymbolProjectionTestHook {
        int BASE_COLUMN = 0;
        int PROJECTION = 2;
        int SYMBOL_FUNCTION = 1;

        void onFunctionRegistered(int functionKind) throws SqlException;

        void onProjectionConstruction() throws SqlException;

        Function wrapFunction(Function function, int functionKind);
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

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

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.EncodedSortLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.EncodedSortLimitedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.EncodedSortRecordCursorFactory;
import io.questdb.griffin.engine.orderby.LimitedSizeSortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.LongTopKRecordCursorFactory;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.engine.orderby.SortKeyMaterializingRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncTopKRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.ColumnType.isTimestamp;

final class SortFactoryGenerator {
    private final BytecodeAssembler asm;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final OutputSchema emptySchema;
    private final EntityColumnFilter entityColumnFilter;
    private final ListColumnFilter keys;
    private final ProjectionFactoryGenerator projectionGenerator;
    private final RecordComparatorCompiler recordComparatorCompiler;

    SortFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            ProjectionFactoryGenerator projectionGenerator,
            BytecodeAssembler asm,
            OutputSchema emptySchema,
            EntityColumnFilter entityColumnFilter,
            RecordComparatorCompiler recordComparatorCompiler,
            ListColumnFilter keys
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.projectionGenerator = projectionGenerator;
        this.asm = asm;
        this.emptySchema = emptySchema;
        this.entityColumnFilter = entityColumnFilter;
        this.keys = keys;
        this.recordComparatorCompiler = recordComparatorCompiler;
    }

    private static FilterPlan findFilterPlan(SortPlan sort) {
        LogicalPlan input = sort.getInput();
        while (true) {
            if (input instanceof ProjectPlan project) {
                input = project.getInput();
            } else if (input instanceof FilterPlan filter) {
                if (!(filter.getPredicate() instanceof ConstantExpression constant) || constant.getLongValue() == 0) {
                    return filter;
                }
                input = filter.getInput();
            } else {
                return null;
            }
        }
    }

    /**
     * True when order advice reaches a native filter, directly, through a window, or as the master
     * of a join that preserves master order.
     */
    static boolean hasAdvisedInput(LogicalPlan plan) {
        plan = LogicalPlans.skipProjects(plan);
        if (plan instanceof WindowPlan) {
            return hasNativeFilterInput(plan.inputAt(0));
        }
        return hasNativeFilterInput(plan) || hasOrderedJoinMasterInput(plan);
    }

    static boolean hasNativeFilterInput(LogicalPlan plan) {
        plan = LogicalPlans.skipProjects(plan);
        return plan instanceof FilterPlan filter && filter.getInput() instanceof ScanPlan;
    }

    static boolean hasOrderedJoinMasterInput(LogicalPlan plan) {
        plan = LogicalPlans.skipProjects(plan);
        return plan instanceof JoinPlan join && isMasterOrderPreserved(join)
                && hasNativeFilterInput(join.getOrderedInputs().getQuick(0).getInput());
    }

    static boolean isMasterOrderPreserved(JoinPlan join) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        for (int i = 1, n = ordered.size(); i < n; i++) {
            final JoinKind joinType = ordered.getQuick(i).getJoinType();
            if (joinType != JoinKind.INNER && joinType != JoinKind.CROSS && joinType != JoinKind.LEFT_OUTER) {
                return false;
            }
        }
        return true;
    }

    // Consumes the input and optional limit functions on entry, including failure.
    RecordCursorFactory generate(
            SortPlan sort, RecordCursorFactory base, Function lo, Function hi,
            int limitPosition, SqlExecutionContext executionContext, FunctionInstantiator instantiator
    ) throws SqlException {
        try {
            final OutputSchema input = sort.getInput().getOutput();
            keys.clear();
            for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
                final int index = input.getColumnIndexById(sort.getColumnIds().getQuick(i));
                keys.add(sort.getDirections().getQuick(i) == SortDirection.DESCENDING ? -index - 1 : index + 1);
            }
            final int firstKey = keys.getQuick(0);
            final int firstIndex = Math.abs(firstKey) - 1;
            final int direction = firstKey < 0 ? RecordCursorFactory.SCAN_DIRECTION_BACKWARD : RecordCursorFactory.SCAN_DIRECTION_FORWARD;
            final boolean isTimestampOrdered = base.getMetadata().getTimestampIndex() == firstIndex && base.getScanDirection() == direction;
            final boolean isFollowingOrder = base.followedOrderByAdvice() && hasAdvisedInput(sort.getInput());
            if (!isFollowingOrder && (keys.size() != 1 || !isTimestampOrdered)) {
                final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(base.getMetadata());
                metadata.setTimestampIndex(sort.getOutput().getTimestampIndex());
                boolean isLimited = lo != null && base.recordCursorSupportsRandomAccess();
                if (isLimited && lo.isConstant() && hi != null && hi.isConstant()) {
                    lo.init(null, executionContext);
                    hi.init(null, executionContext);
                    isLimited = !(lo.getLong(null) >= 0 && hi.getLong(null) < 0);
                }
                if (isLimited) {
                    if (!isTimestampOrdered && lo.isConstant() && hi == null) {
                        final long count = lo.getLong(null);
                        if (count > 0 && count <= Integer.MAX_VALUE) {
                            final RecordCursorFactory ownedBase = base;
                            base = null;
                            base = tryGenerateTopK(metadata, ownedBase, keys, count, executionContext,
                                    findFilterPlan(sort), instantiator);
                            if (base != ownedBase) {
                                final Function unused = lo;
                                lo = null;
                                unused.close();
                                return base;
                            }
                        }
                    }
                    final RecordCursorFactory ownedBase = base;
                    final Function ownedLo = lo;
                    final Function ownedHi = hi;
                    base = null;
                    lo = null;
                    hi = null;
                    return generateSort(metadata, ownedBase, keys, ownedLo, ownedHi, isTimestampOrdered ? firstIndex : -1);
                }
                final RecordCursorFactory ownedBase = base;
                base = null;
                base = generateSort(metadata, ownedBase, keys, null, null, -1);
            }
            if (lo == null) {
                return base;
            }
            final RecordCursorFactory limited = base;
            final Function ownedLo = lo;
            final Function ownedHi = hi;
            base = null;
            lo = null;
            hi = null;
            return new LimitRecordCursorFactory(limited, ownedLo, ownedHi, limitPosition);
        } catch (Throwable th) {
            Misc.free(base, th);
            Misc.free(lo, th);
            Misc.free(hi, th);
            throw th;
        }
    }

    /**
     * Consumes the input and optional LIMIT functions. A non-null lo selects bounded
     * random-access sorting after the caller's top-K and LIMIT eligibility checks.
     * The column filter is borrowed; retained factories receive a private copy.
     */
    RecordCursorFactory generateSort(
            RecordMetadata orderedMetadata,
            RecordCursorFactory base,
            ListColumnFilter keys,
            @Nullable Function lo,
            @Nullable Function hi,
            int preSortedTimestampIndex
    ) throws SqlException {
        final ListColumnFilter retainedKeys;
        final boolean isEncoded;
        RecordComparator comparator = null;
        RecordSink sink = null;
        IntList indexes = null;
        IntList types = null;
        try {
            final RecordMetadata metadata = base.getMetadata();
            retainedKeys = keys.copy();
            isEncoded = configuration.isSqlOrderBySortEnabled() && SortKeyEncoder.isSupported(metadata, keys);
            if (lo != null) {
                assert base.recordCursorSupportsRandomAccess();
            } else {
                assert hi == null;
                if (!base.recordCursorSupportsRandomAccess()) {
                    entityColumnFilter.of(orderedMetadata.getColumnCount());
                    sink = RecordSinkFactory.getInstance(configuration, asm, orderedMetadata, entityColumnFilter);
                } else if (!isEncoded && base instanceof VirtualRecordCursorFactory virtual) {
                    final int threshold = configuration.getSqlSortKeyMaterializationThreshold();
                    for (int i = 0, n = keys.size(); i < n; i++) {
                        final int index = Math.abs(keys.getQuick(i)) - 1;
                        final int type = metadata.getColumnType(index);
                        final int tag = ColumnType.tagOf(type);
                        if (ColumnType.isFixedSize(tag) && tag != ColumnType.IPv4
                                && (ColumnType.sizeOf(type) <= Long.BYTES || tag == ColumnType.DECIMAL128 || tag == ColumnType.DECIMAL256)
                                && virtual.getColumnComplexity(index) > threshold) {
                            if (indexes == null) {
                                indexes = new IntList();
                                types = new IntList();
                            }
                            indexes.add(index);
                            types.add(type);
                        }
                    }
                }
            }
            if (!isEncoded) {
                comparator = recordComparatorCompiler.newInstance(metadata, keys);
            }
        } catch (Throwable th) {
            Misc.free(base, th);
            Misc.free(lo, th);
            if (hi != lo) {
                Misc.free(hi, th);
            }
            throw th;
        }
        if (lo != null) {
            return isEncoded
                    ? new EncodedSortLimitedLightRecordCursorFactory(configuration, orderedMetadata, base, lo, hi, retainedKeys, preSortedTimestampIndex)
                    : new LimitedSizeSortedLightRecordCursorFactory(configuration, orderedMetadata, base, comparator, lo, hi, retainedKeys, preSortedTimestampIndex);
        }
        if (sink != null) {
            return isEncoded
                    ? new EncodedSortRecordCursorFactory(configuration, orderedMetadata, base, sink, retainedKeys)
                    : new SortedRecordCursorFactory(configuration, orderedMetadata, base, sink, comparator, retainedKeys);
        }
        if (isEncoded) {
            return new EncodedSortLightRecordCursorFactory(configuration, orderedMetadata, base, retainedKeys);
        }
        return new SortedLightRecordCursorFactory(configuration, orderedMetadata,
                indexes == null ? base : new SortKeyMaterializingRecordCursorFactory(configuration, orderedMetadata, base, indexes, types),
                comparator, retainedKeys);
    }

    RecordCursorFactory generateSortInput(GenerationFrame frame, SortPlan sort, SqlExecutionContext executionContext, int requiredOrderColumnId,
                                          int requiredScanDirection, LimitPlan limitAdvice) throws SqlException {
        executionContext.pushTimestampRequiredFlag(false);
        try {
            if (frame.isJoinSlaveInput && requiredScanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD && !sort.isReversal()) {
                return codeGenerator.generate(frame, sort.getInput(), executionContext, -1, RecordCursorFactory.SCAN_DIRECTION_OTHER, null, null, OrderByMnemonic.ORDER_BY_INVARIANT);
            }
            return codeGenerator.generate(frame, sort.getInput(), executionContext, requiredOrderColumnId, requiredScanDirection, sort, limitAdvice, OrderByMnemonic.ORDER_BY_INVARIANT);
        } finally {
            executionContext.popTimestampRequiredFlag();
        }
    }

    RecordCursorFactory generateSortedLimit(GenerationFrame frame, LogicalPlan plan, LimitPlan limit, SqlExecutionContext executionContext) throws SqlException {
        if (plan instanceof ProjectPlan project) {
            final RecordCursorFactory base = generateSortedLimit(frame, project.getInput(), limit, executionContext);
            return projectionGenerator.generateProjection(frame, project, base, project.getOutput().getTimestampIndex(), executionContext);
        }
        if (!(plan instanceof SortPlan sort)) {
            throw new IllegalStateException("sorted limit requires a sort under stable projections");
        }
        final int requiredOrderId = sort.getColumnIds().size() == 1 || limit.getHi() == null
                && !(limit.getLo() instanceof ConstantExpression lo && lo.getLongValue() < 0) ? sort.getColumnIds().getQuick(0) : -1;
        final int direction = sort.getDirections().getQuick(0) == SortDirection.DESCENDING
                ? RecordCursorFactory.SCAN_DIRECTION_BACKWARD : RecordCursorFactory.SCAN_DIRECTION_FORWARD;
        final RecordCursorFactory base = generateSortInput(frame, sort, executionContext, requiredOrderId, direction, limit);
        if (base.implementsLimit() && hasNativeFilterInput(sort.getInput())) {
            return generate(sort, base, null, null, limit.getPosition(), executionContext, frame.functionInstantiator);
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
        return generate(sort, base, lo, hi, limit.getPosition(), executionContext, frame.functionInstantiator);
    }

    SortPlan remapOrderAdvice(GenerationFrame frame, ProjectPlan project, SortPlan advice) {
        if (advice == null || advice.hasAliasedKey()) {
            return null;
        }
        final SortPlan mapped = frame.sorts.next().of(project.getInput(), advice.getPosition());
        for (int i = 0, n = advice.getColumnIds().size(); i < n; i++) {
            final int index = project.getOutput().getColumnIndexById(advice.getColumnIds().getQuick(i));
            if (index < 0 || !(project.getExpressions().getQuick(index) instanceof ColumnExpression column)) {
                return null;
            }
            mapped.getColumnIds().add(column.getColumnId());
            mapped.getDirections().add(advice.getDirections().getQuick(i));
        }
        return mapped;
    }

    /**
     * Returns base unchanged on fallback; consumes it on replacement or failure.
     */
    RecordCursorFactory tryGenerateTopK(
            RecordMetadata orderedMetadata,
            RecordCursorFactory base,
            ListColumnFilter keys,
            long count,
            SqlExecutionContext executionContext,
            @Nullable FilterPlan filterPlan,
            FunctionInstantiator instantiator
    ) throws SqlException {
        assert count > 0 && count <= Integer.MAX_VALUE;
        ObjList<Function> workerFilters = null;
        boolean isTransferred = false;
        try {
            if (keys.size() == 1) {
                final int key = keys.getQuick(0);
                final int index = Math.abs(key) - 1;
                if (base.recordCursorSupportsLongTopK(index)) {
                    isTransferred = true;
                    return new LongTopKRecordCursorFactory(orderedMetadata, base, index, (int) count, key > 0);
                }
            }
            if (!executionContext.isParallelTopKEnabled()) {
                return base;
            }
            RecordCursorFactory projection = base.canPeelForTopK() ? base : null;
            RecordCursorFactory filterFactory = projection != null ? projection.getBaseFactory() : base;
            if (filterFactory == null) {
                return base;
            }
            if (filterFactory.canPeelForTopK()) {
                if (!base.supportsPageFrameCursor()) {
                    return base;
                }
                // Keep nested projections' page-frame layout and ORDER BY indexes together.
                projection = null;
                filterFactory = base;
            }
            final boolean isFilterStealable = !filterFactory.supportsPageFrameCursor() && !filterFactory.implementsLimit()
                    && filterPlan != null && (FilterFactoryGenerator.isParallelFilter(filterFactory)
                    || filterFactory instanceof FilteredRecordCursorFactory && filterPlan.getInput() instanceof ScanPlan);
            final RecordCursorFactory leaf = isFilterStealable ? filterFactory.getBaseFactory() : filterFactory;
            if (leaf == null || !leaf.supportsPageFrameCursor()) {
                return base;
            }
            final ListColumnFilter baseKeys = new ListColumnFilter();
            for (int i = 0, n = keys.size(); i < n; i++) {
                final int key = keys.getQuick(i);
                final int index = leaf == base ? Math.abs(key) - 1 : base.translateOrderByColumnToBase(Math.abs(key) - 1);
                if (index < 0) {
                    return base;
                }
                baseKeys.add(key < 0 ? -index - 1 : index + 1);
            }
            final RecordMetadata leafMetadata = leaf.getMetadata();
            final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(leafMetadata);
            final int firstIndex = Math.abs(baseKeys.getQuick(0)) - 1;
            metadata.setTimestampIndex(isTimestamp(leafMetadata.getColumnType(firstIndex)) ? firstIndex : -1);
            final CairoEngine engine = executionContext.getCairoEngine();
            final MessageBus messageBus = executionContext.getMessageBus();
            final int workerCount = executionContext.getSharedQueryWorkerCount();
            final Function filter = isFilterStealable ? FilterFactoryGenerator.stolenFilter(filterFactory) : null;
            final CompiledFilter compiledFilter = isFilterStealable ? filterFactory.getCompiledFilter() : null;
            final MemoryCARW bindVarMemory = isFilterStealable ? filterFactory.getBindVarMemory() : null;
            final ObjList<Function> bindVarFunctions = isFilterStealable ? filterFactory.getBindVarFunctions() : null;
            IntHashSet filterIndexes = null;
            if (isFilterStealable) {
                filterIndexes = new IntHashSet();
                final OutputSchema filterInput = filterPlan.getInput().getOutput();
                FilterFactoryGenerator.collectColumnIndexes(filterPlan.getPredicate(), filterInput, filterIndexes);
                if (!filter.isThreadSafe()) {
                    workerFilters = new ObjList<>(workerCount);
                    for (int i = 0; i < workerCount; i++) {
                        workerFilters.add(instantiator.instantiate(filterPlan.getPredicate(), filterInput, leafMetadata, executionContext));
                    }
                }
                filterFactory.halfClose();
            }
            // The constructor adopts the leaf and stolen filter state, leaving old wrappers unowned.
            isTransferred = true;
            final RecordCursorFactory topK;
            try {
                topK = new AsyncTopKRecordCursorFactory(engine, configuration,
                        messageBus, metadata, leaf, filter, filterIndexes, workerFilters,
                        compiledFilter, bindVarMemory, bindVarFunctions,
                        recordComparatorCompiler, baseKeys, leafMetadata, count, workerCount);
            } catch (Throwable th) {
                if (projection instanceof VirtualRecordCursorFactory virtual) {
                    Misc.freeObjList(virtual.getFunctions(), th);
                }
                throw th;
            }
            return projection == null ? topK : projection.rewrapOverTopK(topK, orderedMetadata);
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.freeObjList(workerFilters, th);
                Misc.free(base, th);
            }
            throw th;
        }
    }
}

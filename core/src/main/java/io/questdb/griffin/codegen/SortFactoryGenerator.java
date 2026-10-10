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
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
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
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PhysicalProperties;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
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
    private final FilterFactoryGenerator filterGenerator;
    private final ProjectionFactoryGenerator projectionGenerator;
    private final RecordComparatorCompiler recordComparatorCompiler;

    SortFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            FilterFactoryGenerator filterGenerator,
            ProjectionFactoryGenerator projectionGenerator,
            BytecodeAssembler asm,
            OutputSchema emptySchema,
            EntityColumnFilter entityColumnFilter,
            RecordComparatorCompiler recordComparatorCompiler
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.filterGenerator = filterGenerator;
        this.projectionGenerator = projectionGenerator;
        this.asm = asm;
        this.emptySchema = emptySchema;
        this.entityColumnFilter = entityColumnFilter;
        this.recordComparatorCompiler = recordComparatorCompiler;
    }

    // Consumes the input and optional limit functions on entry, including failure.
    RecordCursorFactory generate(GenerationFrame frame, SortPlan sort, RecordCursorFactory base, Function lo, Function hi, int limitPosition) throws SqlException {
        try {
            final SortPlan.Algorithm algorithm = sort.getAlgorithm();
            if (algorithm != SortPlan.Algorithm.INPUT_ORDER) {
                final OutputSchema input = sort.getInput().getOutput();
                final ListColumnFilter keys = frame.listColumnFilterB;
                keys.clear();
                for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
                    final int index = input.getColumnIndexById(sort.getColumnIds().getQuick(i));
                    keys.add(sort.getDirections().getQuick(i) == SortDirection.DESCENDING ? -index - 1 : index + 1);
                }
                final int firstKey = keys.getQuick(0);
                final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(base.getMetadata());
                metadata.setTimestampIndex(PhysicalProperties.timestampIndex(sort));
                final RecordCursorFactory ownedBase = base;
                base = null;
                switch (algorithm) {
                    case LONG_TOP_K -> {
                        final Function count = lo;
                        lo = null;
                        try {
                            return new LongTopKRecordCursorFactory(metadata, ownedBase, Math.abs(firstKey) - 1, (int) count.getLong(null), firstKey > 0);
                        } finally {
                            count.close();
                        }
                    }
                    case PARALLEL_FILTERED_TOP_K, PARALLEL_TOP_K -> {
                        final IllegalStateException failure = new IllegalStateException("parallel top-K generates its own input");
                        Misc.free(ownedBase, failure);
                        throw failure;
                    }
                    case LIMITED, PRESORTED_LIMITED -> {
                        final Function ownedLo = lo;
                        final Function ownedHi = hi;
                        lo = null;
                        hi = null;
                        return generateSort(metadata, ownedBase, keys, ownedLo, ownedHi,
                                algorithm == SortPlan.Algorithm.PRESORTED_LIMITED ? Math.abs(firstKey) - 1 : -1, false);
                    }
                    default ->
                            base = generateSort(metadata, ownedBase, keys, null, null, -1, algorithm == SortPlan.Algorithm.MATERIALIZED);
                }
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
     * Builds the parallel top-K of the sort under the LIMIT over the page frames of the input of the projection it
     * builds over itself, see {@link LogicalPlans#parallelTopKProjection}, applying the filter it steals from that
     * input when the sort's algorithm says so, and the projection over the top-K.
     */
    RecordCursorFactory generateParallelTopK(GenerationFrame frame, SortPlan sort, LimitPlan limit, SqlExecutionContext executionContext)
            throws SqlException {
        final ProjectPlan projection = LogicalPlans.parallelTopKProjection(sort);
        final LogicalPlan source = projection != null ? projection.getInput() : sort.getInput();
        final boolean isFilterStolen = sort.getAlgorithm() == SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K;
        final PreparedFilter prepared = frame.pushPreparedFilter(isFilterStolen);
        final RecordCursorFactory topK;
        RecordCursorFactory leaf = null;
        try {
            leaf = codeGenerator.generateSource(frame, source, prepared, executionContext);
            final long count;
            try (Function lo = frame.functionInstantiator.instantiate(limit.getLo(), emptySchema, executionContext)) {
                count = lo.getLong(null);
            }
            assert count > 0 && count <= Integer.MAX_VALUE;
            if (ParanoiaState.PLAN_PARANOIA_MODE && !leaf.supportsPageFrameCursor()) {
                throw new AssertionError("parallel top-K input does not read page frames");
            }
            final OutputSchema sorted = sort.getInput().getOutput();
            final OutputSchema input = source.getOutput();
            final ListColumnFilter keys = new ListColumnFilter();
            for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
                int columnId = sort.getColumnIds().getQuick(i);
                if (projection != null) {
                    columnId = ((ColumnExpression) projection.getExpressions().getQuick(sorted.getColumnIndexById(columnId))).getColumnId();
                }
                final int index = input.getColumnIndexById(columnId);
                keys.add(sort.getDirections().getQuick(i) == SortDirection.DESCENDING ? -index - 1 : index + 1);
            }
            final RecordMetadata leafMetadata = leaf.getMetadata();
            final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(leafMetadata);
            final int firstIndex = Math.abs(keys.getQuick(0)) - 1;
            metadata.setTimestampIndex(isTimestamp(leafMetadata.getColumnType(firstIndex)) ? firstIndex : -1);
            if (isFilterStolen) {
                filterGenerator.prepareParallel(prepared, leaf, frame.functionInstantiator, executionContext);
            }
            final RecordCursorFactory ownedLeaf = leaf;
            leaf = null;
            final Function filter = isFilterStolen ? prepared.getFilter() : null;
            final IntHashSet filterColumns = isFilterStolen ? prepared.getColumns() : null;
            final ObjList<Function> workerFilters = isFilterStolen ? prepared.getWorkers() : null;
            final CompiledFilter compiledFilter = isFilterStolen ? prepared.getCompiledFilter() : null;
            final MemoryCARW bindVarMemory = isFilterStolen ? prepared.getBindVarMemory() : null;
            final ObjList<Function> bindVarFunctions = isFilterStolen ? prepared.getBindVarFunctions() : null;
            if (isFilterStolen) {
                prepared.adopt();
            }
            topK = new AsyncTopKRecordCursorFactory(executionContext.getCairoEngine(), configuration, executionContext.getMessageBus(),
                    metadata, ownedLeaf, filter, filterColumns, workerFilters, compiledFilter, bindVarMemory, bindVarFunctions,
                    recordComparatorCompiler, keys, leafMetadata, count, executionContext.getSharedQueryWorkerCount());
        } catch (Throwable th) {
            Misc.free(leaf, th);
            frame.popPreparedFilter(prepared, th);
            throw th;
        }
        frame.popPreparedFilter(prepared);
        return projection != null ? projectionGenerator.generateProjection(frame, projection, topK,
                PhysicalProperties.timestampIndex(sort), executionContext) : topK;
    }

    /**
     * Consumes the input and optional LIMIT functions. A non-null lo selects the bounded sort of a random-access
     * input; without one, the sort copies the rows of the input when {@code isMaterialized}.
     * The column filter is borrowed; retained factories receive a private copy.
     */
    RecordCursorFactory generateSort(
            RecordMetadata orderedMetadata,
            RecordCursorFactory base,
            ListColumnFilter keys,
            @Nullable Function lo,
            @Nullable Function hi,
            int preSortedTimestampIndex,
            boolean isMaterialized
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
            if (lo == null) {
                assert hi == null;
                if (isMaterialized) {
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

    RecordCursorFactory generateSortedLimit(GenerationFrame frame, LogicalPlan plan, LimitPlan limit, SqlExecutionContext executionContext) throws SqlException {
        if (plan instanceof ProjectPlan project) {
            final RecordCursorFactory base = generateSortedLimit(frame, project.getInput(), limit, executionContext);
            return projectionGenerator.generateProjection(frame, project, base, executionContext);
        }
        if (!(plan instanceof SortPlan sort)) {
            throw new IllegalStateException("sorted limit requires a sort under stable projections");
        }
        if (sort.getAlgorithm() == SortPlan.Algorithm.PARALLEL_TOP_K || sort.getAlgorithm() == SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K) {
            return generateParallelTopK(frame, sort, limit, executionContext);
        }
        final RecordCursorFactory base = codeGenerator.generate(frame, sort.getInput(), executionContext);
        if (limit.getApplication() == LimitPlan.Application.INPUT) {
            return generate(frame, sort, base, null, null, limit.getPosition());
        }
        codeGenerator.instantiateLimit(frame, limit, base, executionContext);
        return generate(frame, sort, base, frame.limitLo, frame.limitHi, limit.getPosition());
    }

}

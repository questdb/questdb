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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

public class AsyncGroupByNotKeyedRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final UnorderedPageFrameReducer AGGREGATE = AsyncGroupByNotKeyedRecordCursorFactory::aggregate;
    private static final UnorderedPageFrameReducer AGGREGATE_VECT = AsyncGroupByNotKeyedRecordCursorFactory::aggregateVect;
    private static final UnorderedPageFrameReducer FILTER_AND_AGGREGATE = AsyncGroupByNotKeyedRecordCursorFactory::filterAndAggregate;

    private final RecordCursorFactory base;
    private final AsyncGroupByNotKeyedRecordCursor cursor;
    private final UnorderedPageFrameSequence<AsyncGroupByNotKeyedAtom> frameSequence;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final @Nullable ObjList<ObjList<Function>> sharedRecordFunctions;
    private final boolean vectorized;
    private final int workerCount;
    private ObjList<AsyncGroupByNotKeyedSharedCursor> sharedCursors;

    public AsyncGroupByNotKeyedRecordCursorFactory(
            @NotNull CairoEngine engine,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull RecordMetadata groupByMetadata,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            int valueCount,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount,
            @Nullable ObjList<ObjList<Function>> sharedRecordFunctions
    ) {
        super(groupByMetadata);
        try {
            this.base = base;
            this.groupByFunctions = groupByFunctions;
            this.sharedRecordFunctions = sharedRecordFunctions;

            // Compute batch eligibility for each group by function.
            final RecordMetadata baseMetadata = base.getMetadata();
            final int[] batchColumnIndexes = new int[groupByFunctions.size()];
            boolean hasBatchFunctions = false;
            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                batchColumnIndexes[i] = computeBatchColumnIndex(groupByFunctions.getQuick(i), baseMetadata);
                if (batchColumnIndexes[i] != AsyncGroupByNotKeyedAtom.BATCH_NOT_ELIGIBLE) {
                    hasBatchFunctions = true;
                }
            }

            AsyncGroupByNotKeyedAtom atom = new AsyncGroupByNotKeyedAtom(
                    asm,
                    configuration,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
                    batchColumnIndexes,
                    valueCount,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    filter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    workerCount
            );

            final UnorderedPageFrameReducer reducer;
            if (filter != null) {
                reducer = FILTER_AND_AGGREGATE;
            } else if (hasBatchFunctions) {
                reducer = AGGREGATE_VECT;
            } else {
                reducer = AGGREGATE;
            }
            this.vectorized = (reducer == AGGREGATE_VECT);

            this.frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    reducer,
                    workerCount
            );
            this.cursor = new AsyncGroupByNotKeyedRecordCursor(groupByFunctions);
            this.workerCount = workerCount;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final int order = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        frameSequence.of(base, executionContext, order);
        cursor.of(frameSequence, executionContext);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public RecordCursor getSharedCursor(SqlExecutionContext executionContext, int sharedId) throws SqlException {
        if (sharedCursors == null) {
            sharedCursors = new ObjList<>();
        }
        int idx = sharedId - 1;
        AsyncGroupByNotKeyedSharedCursor shared = sharedCursors.getQuiet(idx);
        if (shared == null) {
            assert sharedRecordFunctions != null;
            assert idx < sharedRecordFunctions.size();
            shared = new AsyncGroupByNotKeyedSharedCursor(cursor, sharedRecordFunctions.getQuick(idx), frameSequence.getAtom().getOwnerMapValue());
            sharedCursors.extendAndSet(idx, shared);
        }
        shared.of(executionContext, frameSequence);
        return shared;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsSharedCursors() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (usesCompiledFilter()) {
            sink.type("Async JIT Group By");
        } else {
            sink.type("Async Group By");
        }
        sink.meta("workers").val(workerCount);
        sink.attr("vectorized").val(vectorized);
        sink.optAttr("values", groupByFunctions, true);
        sink.optAttr("filter", frameSequence.getAtom(), true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getFilterContext().getCompiledFilter() != null;
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private static void aggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        assert frameRowCount > 0;
        @SuppressWarnings("unchecked") final AsyncGroupByNotKeyedAtom atom = ((UnorderedPageFrameSequence<AsyncGroupByNotKeyedAtom>) frameSequence).getAtom();

        final boolean owner = stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final AsyncFilterContext filterCtx = atom.getFilterContext();
        final PageFrameMemoryPool frameMemoryPool = filterCtx.getMemoryPool(slotId);
        final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
        record.init(frameMemory);

        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final SimpleMapValue value = atom.getMapValue(slotId);
        try {
            record.setRowIndex(0);
            long rowId = record.getRowId();
            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);
                if (value.isNew()) {
                    functionUpdater.updateNew(value, record, rowId++);
                    value.setNew(false);
                } else {
                    functionUpdater.updateExisting(value, record, rowId++);
                }
            }
        } finally {
            frameMemoryPool.releaseParquetBuffers();
            atom.release(slotId);
        }
    }

    private static void aggregateFiltered(
            @NotNull PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            SimpleMapValue value,
            GroupByFunctionsUpdater functionUpdater
    ) {
        for (long p = 0, n = rows.size(); p < n; p++) {
            long r = rows.get(p);
            record.setRowIndex(r);
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
                value.setNew(false);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
        }
    }

    private static void aggregateVect(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        assert frameRowCount > 0;
        @SuppressWarnings("unchecked") final AsyncGroupByNotKeyedAtom atom = ((UnorderedPageFrameSequence<AsyncGroupByNotKeyedAtom>) frameSequence).getAtom();

        final boolean owner = stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final AsyncFilterContext filterCtx = atom.getFilterContext();
        final PageFrameMemoryPool frameMemoryPool = filterCtx.getMemoryPool(slotId);
        final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);

        final SimpleMapValue value = atom.getMapValue(slotId);
        final int[] batchColumnIndexes = atom.getBatchColumnIndexes();
        final ObjList<GroupByFunction> functions = atom.getGroupByFunctions(slotId);
        final int functionCount = functions.size();
        try {
            if (frameMemory.hasColumnTops()) {
                // Fall back to row-by-row for the entire frame.
                record.init(frameMemory);
                final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
                record.setRowIndex(0);
                long rowId = record.getRowId();
                for (long r = 0; r < frameRowCount; r++) {
                    record.setRowIndex(r);
                    if (value.isNew()) {
                        functionUpdater.updateNew(value, record, rowId++);
                        value.setNew(false);
                    } else {
                        functionUpdater.updateExisting(value, record, rowId++);
                    }
                }
            } else {
                record.init(frameMemory);
                record.setRowIndex(0);
                final long startRowId = record.getRowId();

                // Phase 1: batch-eligible functions.
                final int count = (int) frameRowCount;
                for (int i = 0; i < functionCount; i++) {
                    final int colIdx = batchColumnIndexes[i];
                    if (colIdx == AsyncGroupByNotKeyedAtom.BATCH_NOT_ELIGIBLE) {
                        continue;
                    }
                    final GroupByFunction func = functions.getQuick(i);
                    if (colIdx == AsyncGroupByNotKeyedAtom.BATCH_NO_ARG) {
                        func.computeBatch(value, 0, count, startRowId);
                    } else {
                        func.computeBatch(value, frameMemory.getPageAddress(colIdx), count, startRowId);
                    }
                }

                // Phase 2: non-batch functions (row-by-row).
                if (atom.hasNonBatchFunctions()) {
                    long rowId = startRowId;
                    for (long r = 0; r < frameRowCount; r++) {
                        record.setRowIndex(r);
                        for (int i = 0; i < functionCount; i++) {
                            if (batchColumnIndexes[i] != AsyncGroupByNotKeyedAtom.BATCH_NOT_ELIGIBLE) {
                                continue;
                            }
                            final GroupByFunction func = functions.getQuick(i);
                            if (value.isNew()) {
                                func.computeFirst(value, record, rowId);
                            } else {
                                func.computeNext(value, record, rowId);
                            }
                        }
                        if (value.isNew()) {
                            value.setNew(false);
                        }
                        rowId++;
                    }
                }

                // Ensure isNew is false for the merge phase.
                if (value.isNew()) {
                    value.setNew(false);
                }
            }
        } finally {
            frameMemoryPool.releaseParquetBuffers();
            atom.release(slotId);
        }
    }

    private static int computeBatchColumnIndex(GroupByFunction func, RecordMetadata baseMetadata) {
        if (!func.supportsBatchComputation()) {
            return AsyncGroupByNotKeyedAtom.BATCH_NOT_ELIGIBLE;
        }
        final Function batchArg = func.getComputeBatchArg();
        if (batchArg == null) {
            // No-arg function (e.g. count(*)).
            return AsyncGroupByNotKeyedAtom.BATCH_NO_ARG;
        }
        if (!(batchArg instanceof ColumnFunction columnFunc)) {
            // Argument is an expression, not a direct column reference.
            return AsyncGroupByNotKeyedAtom.BATCH_NOT_ELIGIBLE;
        }
        final int columnIndex = columnFunc.getColumnIndex();
        final int physicalType = baseMetadata.getColumnType(columnIndex);
        final int batchArgType = func.getComputeBatchArgType();
        if (ColumnType.tagOf(batchArgType) != ColumnType.tagOf(physicalType)) {
            // Type mismatch (e.g. sum(short_col) expects INT buffer but column is SHORT).
            return AsyncGroupByNotKeyedAtom.BATCH_NOT_ELIGIBLE;
        }
        return columnIndex;
    }

    private static void filterAndAggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        @SuppressWarnings("unchecked") final AsyncGroupByNotKeyedAtom atom = ((UnorderedPageFrameSequence<AsyncGroupByNotKeyedAtom>) frameSequence).getAtom();
        final long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        assert frameRowCount > 0;

        final boolean owner = stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final AsyncFilterContext filterCtx = atom.getFilterContext();
        final PageFrameAddressCache addressCache = frameSequence.getPageFrameAddressCache();
        final boolean isParquetFrame = addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET;
        final boolean useLateMaterialization = filterCtx.shouldUseLateMaterialization(slotId, isParquetFrame);

        final PageFrameMemoryPool frameMemoryPool = filterCtx.getMemoryPool(slotId);
        final PageFrameMemory frameMemory;
        if (useLateMaterialization) {
            frameMemory = frameMemoryPool.navigateTo(frameIndex, filterCtx.getFilterUsedColumnIndexes());
        } else {
            frameMemory = frameMemoryPool.navigateTo(frameIndex);
        }
        record.init(frameMemory);

        final DirectLongList rows = filterCtx.getFilteredRows(slotId);
        rows.clear();

        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final SimpleMapValue value = atom.getMapValue(slotId);
        final CompiledFilter compiledFilter = filterCtx.getCompiledFilter();
        final Function filter = filterCtx.getFilter(slotId);
        try {
            if (compiledFilter == null || frameMemory.hasColumnTops()) {
                // Use Java-based filter when there is no compiled filter or in case of a page frame with column tops.
                AsyncFilterUtils.applyFilter(filter, rows, record, frameRowCount);
            } else {
                AsyncFilterUtils.applyCompiledFilter(
                        compiledFilter,
                        filterCtx.getBindVarMemory(),
                        filterCtx.getBindVarFunctions(),
                        frameMemory,
                        addressCache,
                        filterCtx.getDataAddresses(slotId),
                        filterCtx.getAuxAddresses(slotId),
                        rows,
                        frameRowCount
                );
            }

            if (isParquetFrame) {
                filterCtx.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (useLateMaterialization && frameMemory.populateRemainingColumns(filterCtx.getFilterUsedColumnIndexes(), rows, false)) {
                PageFrameFilteredMemoryRecord filteredMemoryRecord = filterCtx.getPageFrameFilteredMemoryRecord(slotId);
                filteredMemoryRecord.of(frameMemory, record, filterCtx.getFilterUsedColumnIndexes());
                record = filteredMemoryRecord;
            }
            record.setRowIndex(0);
            long baseRowId = record.getRowId();
            aggregateFiltered(record, rows, baseRowId, value, functionUpdater);
        } finally {
            frameMemoryPool.releaseParquetBuffers();
            atom.release(slotId);
        }
    }

    /**
     * Releases resources held by this factory.
     * <p>
     * Frees the underlying base factory, the prepared cursor, and the frame sequence,
     * and also frees and clears the list of group-by function instances to remove
     * references.
     */
    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(cursor);
        Misc.free(frameSequence);
        Misc.freeObjList(groupByFunctions);
        GroupByRecordCursorFactory.freeSharedRecordFunctions(sharedRecordFunctions);
        // Shared cursors hold no native memory; primary state freed above covers it.
        Misc.clear(sharedCursors);
    }
}

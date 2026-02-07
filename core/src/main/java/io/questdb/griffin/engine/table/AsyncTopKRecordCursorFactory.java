/*******************************************************************************
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
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
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
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

/**
 * ORDER BY + LIMIT (top K) parallel execution.
 */
public class AsyncTopKRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final UnorderedPageFrameReducer FILTER_AND_FIND_TOP_K = AsyncTopKRecordCursorFactory::filterAndFindTopK;
    private static final UnorderedPageFrameReducer FIND_TOP_K = AsyncTopKRecordCursorFactory::findTopK;
    private final RecordCursorFactory base;
    private final AsyncTopKRecordCursor cursor;
    private final UnorderedPageFrameSequence<AsyncTopKAtom> frameSequence;
    private final long lo;
    private final ListColumnFilter orderByFilter;
    private final int workerCount;

    public AsyncTopKRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory base,
            @Nullable Function filter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @NotNull @Transient RecordComparatorCompiler recordComparatorCompiler,
            @NotNull ListColumnFilter orderByFilter,
            @NotNull @Transient RecordMetadata orderByMetadata,
            long lo,
            int workerCount
    ) throws SqlException {
        super(metadata);
        assert !(base instanceof AsyncTopKRecordCursorFactory);
        try {
            this.base = base;
            final AsyncTopKAtom atom = new AsyncTopKAtom(
                    configuration,
                    filter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    recordComparatorCompiler,
                    orderByFilter,
                    orderByMetadata,
                    lo,
                    workerCount
            );
            this.frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    filter != null ? FILTER_AND_FIND_TOP_K : FIND_TOP_K,
                    workerCount
            );
            this.cursor = new AsyncTopKRecordCursor();
            this.orderByFilter = orderByFilter;
            this.lo = lo;
            this.workerCount = workerCount;
        } catch (Throwable th) {
            close();
            throw th;
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
        cursor.of(frameSequence);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return SortedRecordCursorFactory.getScanDirection(orderByFilter);
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    @Override
    public boolean implementsLimit() {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (usesCompiledFilter()) {
            sink.type("Async JIT Top K");
        } else {
            sink.type("Async Top K");
        }
        sink.meta("lo").val(lo);
        sink.meta("workers").val(workerCount);
        sink.optAttr("filter", frameSequence.getAtom(), true);
        SortedLightRecordCursorFactory.addSortKeys(sink, orderByFilter);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getCompiledFilter() != null;
    }

    private static void filterAndFindTopK(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        @SuppressWarnings("unchecked")
        final AsyncTopKAtom atom = ((UnorderedPageFrameSequence<AsyncTopKAtom>) frameSequence).getAtom();
        final long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        assert frameRowCount > 0;

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final PageFrameMemoryPool frameMemoryPool = atom.getMemoryPool(slotId);
        final PageFrameMemoryRecord recordB = atom.getRecordB(slotId);
        final PageFrameAddressCache addressCache = frameSequence.getPageFrameAddressCache();
        final boolean isParquetFrame = addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET;
        final boolean useLateMaterialization = atom.shouldUseLateMaterialization(slotId, isParquetFrame);
        final PageFrameMemory frameMemory;
        if (useLateMaterialization) {
            frameMemory = frameMemoryPool.navigateTo(frameIndex, atom.getFilterUsedColumnIndexes());
        } else {
            frameMemory = frameMemoryPool.navigateTo(frameIndex);
        }

        record.init(frameMemory);
        final DirectLongList rows = atom.getFilteredRows(slotId);
        rows.clear();
        final LimitedSizeLongTreeChain chain = atom.getTreeChain(slotId);
        final RecordComparator comparator = atom.getComparator(slotId);
        final CompiledFilter compiledFilter = atom.getCompiledFilter();
        final Function filter = atom.getFilter(slotId);
        try {
            if (compiledFilter == null || frameMemory.hasColumnTops()) {
                // Use Java-based filter when there is no compiled filter or in case of a page frame with column tops.
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(
                        compiledFilter,
                        atom.getBindVarMemory(),
                        atom.getBindVarFunctions(),
                        frameMemory,
                        addressCache,
                        atom.getDataAddresses(slotId),
                        atom.getAuxAddresses(slotId),
                        rows,
                        frameRowCount
                );
            }
            if (isParquetFrame) {
                atom.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (useLateMaterialization && frameMemory.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                record.init(frameMemory);
            }

            for (long p = 0, n = rows.size(); p < n; p++) {
                long r = rows.get(p);
                record.setRowIndex(r);

                // Tree chain is liable to re-position record to
                // other rows to do record comparison. We must use our
                // own record instance in case base cursor keeps
                // state in the record it returns.
                chain.put(record, frameMemoryPool, recordB, comparator);
            }
        } finally {
            recordB.clear();
            frameMemoryPool.releaseParquetBuffers();
            atom.release(slotId);
        }
    }

    private static void findTopK(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        assert frameRowCount > 0;
        @SuppressWarnings("unchecked")
        final AsyncTopKAtom atom = ((UnorderedPageFrameSequence<AsyncTopKAtom>) frameSequence).getAtom();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final PageFrameMemoryPool frameMemoryPool = atom.getMemoryPool(slotId);
        final PageFrameMemoryRecord recordB = atom.getRecordB(slotId);
        final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
        record.init(frameMemory);
        final LimitedSizeLongTreeChain chain = atom.getTreeChain(slotId);
        final RecordComparator comparator = atom.getComparator(slotId);
        try {
            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);

                // Tree chain is liable to re-position record to
                // other rows to do record comparison. We must use our
                // own record instance in case base cursor keeps
                // state in the record it returns.
                chain.put(record, frameMemoryPool, recordB, comparator);
            }
        } finally {
            recordB.clear();
            frameMemoryPool.releaseParquetBuffers();
            atom.release(slotId);
        }
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(cursor);
        Misc.free(frameSequence);
    }
}

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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
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
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectLongList;
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
    private static final PageFrameReducer FILTER_AND_FIND_TOP_K = AsyncTopKRecordCursorFactory::filterAndFindTopK;
    private static final PageFrameReducer FIND_TOP_K = AsyncTopKRecordCursorFactory::findTopK;
    private final RecordCursorFactory base;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncTopKRecordCursor cursor;
    private final PageFrameSequence<AsyncTopKAtom> frameSequence;
    private final long lo;
    private final ListColumnFilter orderByFilter;
    private final int workerCount;

    public AsyncTopKRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory base,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable Function filter,
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
            this.frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    filter != null ? FILTER_AND_FIND_TOP_K : FIND_TOP_K,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_TOP_K
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
    public PageFrameSequence<AsyncTopKAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, order);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final int order = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        cursor.of(execute(executionContext, collectSubSeq, order));
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
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final DirectLongList rows = task.getFilteredRows();
        final PageFrameSequence<AsyncTopKAtom> frameSequence = task.getFrameSequence(AsyncTopKAtom.class);

        rows.clear();

        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncTopKAtom atom = frameSequence.getAtom();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        try {
            final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
            final PageFrameMemoryPool frameMemoryPool = atom.getMemoryPool(slotId);
            final PageFrameMemoryRecord recordB = atom.getRecordB(slotId);
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(task.getFrameIndex());
            record.init(frameMemory);
            final LimitedSizeLongTreeChain chain = atom.getTreeChain(slotId);
            final RecordComparator comparator = atom.getComparator(slotId);
            final CompiledFilter compiledFilter = atom.getCompiledFilter();
            final Function filter = atom.getFilter(slotId);
            try {
                if (compiledFilter == null || frameMemory.hasColumnTops()) {
                    // Use Java-based filter when there is no compiled filter or in case of a page frame with column tops.
                    applyFilter(filter, rows, record, frameRowCount);
                } else {
                    applyCompiledFilter(frameMemory, compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
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
                atom.release(slotId);
            }
        } finally {
            task.releaseFrameMemory();
        }
    }

    private static void findTopK(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        assert frameRowCount > 0;
        final AsyncTopKAtom atom = task.getFrameSequence(AsyncTopKAtom.class).getAtom();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        try {
            final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
            final PageFrameMemoryPool frameMemoryPool = atom.getMemoryPool(slotId);
            final PageFrameMemoryRecord recordB = atom.getRecordB(slotId);
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(task.getFrameIndex());
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
                atom.release(slotId);
            }
        } finally {
            task.releaseFrameMemory();
        }
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(cursor);
        Misc.free(frameSequence);
    }
}

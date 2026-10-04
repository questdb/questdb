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


package io.questdb.griffin.engine.window;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.table.KeyMajorPageFrameRecordCursor;
import io.questdb.griffin.engine.table.PageFrameRowToucher;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * State the tasks of an {@link AsyncWindowRecordCursorFactory} share: one copy of the window's
 * functions per worker slot plus one for the query's own thread, and the round of tasks the
 * workers are running.
 * <p>
 * A task computes the window over the rows of whole keys of a key-major scan. Every window
 * function is partitioned by that key, so a key's values depend on its own rows only, in the order
 * the scan walks them, and any slot can compute any key. A task starts from clean function state,
 * which costs nothing in results, because the keys of one task share no partition with the keys of
 * another.
 */
public class AsyncWindowAtom implements StatefulAtom, PerWorkerLockOwner {
    private final PerWorkerLocks perWorkerLocks;
    // slot -1, the query's own thread, then the worker slots
    private final ObjList<Slot> slots;
    // the round the workers run, set before the round is dispatched
    private AsyncWindowRecordCursor.Round round;

    /**
     * @param ownerFunctions     the functions of the query's own thread, every output column in
     *                           order; the factory reads their metadata, this atom does not own them
     * @param ownerMapStates     the window Map groups over {@code ownerFunctions}, or null
     * @param perWorkerFunctions one list like {@code ownerFunctions} per worker slot; each entry is
     *                           owned by this atom once it has replaced it with null, also when
     *                           the constructor throws
     * @param perWorkerMapStates the window Map groups of each worker list, entries may be null;
     *                           owned like {@code perWorkerFunctions}
     */
    public AsyncWindowAtom(
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<Function> ownerFunctions,
            @Nullable ObjList<WindowMapState> ownerMapStates,
            @NotNull ObjList<ObjList<Function>> perWorkerFunctions,
            @NotNull ObjList<ObjList<WindowMapState>> perWorkerMapStates
    ) {
        final int workerCount = perWorkerFunctions.size();
        assert perWorkerMapStates.size() == workerCount;
        this.slots = new ObjList<>(workerCount + 1);
        try {
            slots.add(new Slot(configuration, ownerFunctions, ownerMapStates, false));
            for (int i = 0; i < workerCount; i++) {
                slots.add(new Slot(configuration, perWorkerFunctions.getQuick(i), perWorkerMapStates.getQuick(i), true));
                // the slot owns them now
                perWorkerFunctions.setQuick(i, null);
                perWorkerMapStates.setQuick(i, null);
            }
            this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        round = null;
    }

    @Override
    public void close() {
        // The owner's slot borrows the factory's functions; the factory frees those.
        Throwable failure = null;
        for (int i = 0, n = slots.size(); i < n; i++) {
            failure = Misc.freeBestEffort(failure, slots.getQuick(i));
        }
        // idempotent: a failed constructor may close the atom before its owner does
        slots.clear();
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        // the cursor initializes the slots when it opens, before it knows whether it dispatches
    }

    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        return workerId == -1 && owner ? -1 : perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slotId) {
        if (slotId != -1) {
            perWorkerLocks.releaseSlot(slotId);
        }
    }

    // Slot -1 is the query's own thread.
    Slot getSlot(int slotId) {
        return slots.getQuick(slotId + 1);
    }

    int getSlotCount() {
        return slots.size();
    }

    AsyncWindowRecordCursor.Round getRound() {
        return round;
    }

    /**
     * The slots that hold a copy of the functions for the workers, which share them when there
     * are fewer slots than workers.
     */
    @TestOnly
    public int getWorkerSlotCount() {
        return slots.size() - 1;
    }

    /**
     * Tasks the worker slots, not the query's own thread, computed since the last
     * {@link #resetTaskCounts()}.
     */
    @TestOnly
    public long getWorkerSlotTaskCount() {
        long count = 0;
        for (int i = 1, n = slots.size(); i < n; i++) {
            count += slots.getQuick(i).taskCount;
        }
        return count;
    }

    // Lets the worker slots read the scan's rows: each reads them through its own pool over the
    // scan's frame address cache.
    void ofWorkerFrames(PageFrameAddressCache frameAddressCache) {
        for (int i = 1, n = slots.size(); i < n; i++) {
            slots.getQuick(i).ofFrames(frameAddressCache);
        }
    }

    void resetTaskCounts() {
        for (int i = 0, n = slots.size(); i < n; i++) {
            slots.getQuick(i).taskCount = 0;
        }
    }

    void setRound(AsyncWindowRecordCursor.Round round) {
        this.round = round;
    }

    /**
     * One copy of the window's functions and what computing them over collected rows needs: a
     * record and a frame memory pool of its own, and the touch-ahead that overlaps the cache misses
     * of a batch of rows.
     */
    static class Slot implements QuietCloseable {
        // Rows of one frame computed between two touch-ahead loads, see KeyMajorPageFrameRecordCursor.
        private static final int BATCH_ROWS = 32;
        // Batches between two checks of the circuit breaker and of the round's cancellation.
        private static final int CHECK_BATCHES = 64;
        private final long[] batchRows = new long[BATCH_ROWS];
        private final ObjList<Function> functions;
        private final ObjList<WindowMapState> mapStates;
        private final int mapStatesCount;
        private final boolean ownsFunctions;
        private final PageFrameMemoryPool pool;
        private final PageFrameMemoryRecord record;
        private final PageFrameRowToucher toucher = new PageFrameRowToucher();
        private final VirtualRecord virtualRecord;
        private final ObjList<WindowFunction> windowFunctions = new ObjList<>();
        private final int windowFunctionsCount;
        private PageFrameAddressCache frameAddressCache;
        private boolean isOpen;
        // the frame streamRow() last positioned the record on, -1 when it must position it again
        private int streamFrameIndex = -1;
        // the stream's rows before this index have had their columns loaded
        private long streamTouchedHi;
        // written by the thread that holds the slot, read once the round has been awaited
        private long taskCount;

        Slot(
                CairoConfiguration configuration,
                ObjList<Function> functions,
                @Nullable ObjList<WindowMapState> mapStates,
                boolean ownsFunctions
        ) {
            this.functions = functions;
            this.mapStates = mapStates;
            this.mapStatesCount = mapStates != null ? mapStates.size() : 0;
            this.ownsFunctions = ownsFunctions;
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (functions.getQuick(i) instanceof WindowFunction wf) {
                    windowFunctions.add(wf);
                }
            }
            this.windowFunctionsCount = windowFunctions.size();
            this.pool = new PageFrameMemoryPool(configuration);
            this.record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            this.virtualRecord = new VirtualRecord(functions);
        }

        @Override
        public void close() {
            Throwable failure = null;
            try {
                closeCursor();
            } catch (Throwable th) {
                failure = th;
            }
            failure = Misc.freeBestEffort(failure, pool);
            failure = Misc.freeBestEffort(failure, record);
            if (ownsFunctions) {
                failure = Misc.freeObjListBestEffort(failure, mapStates);
                failure = Misc.freeObjListBestEffort(failure, functions);
            }
            CairoException.rethrowCleanupFailure(failure);
        }

        /**
         * Releases what one execution held: the functions' per-partition state and the frame
         * memory. The functions stay compiled for the next execution.
         */
        void closeCursor() {
            frameAddressCache = null;
            Misc.free(pool);
            if (isOpen) {
                isOpen = false;
                for (int i = 0, n = functions.size(); i < n; i++) {
                    final Function function = functions.getQuick(i);
                    if (function != null) {
                        function.cursorClosed();
                    }
                }
                for (int i = 0; i < windowFunctionsCount; i++) {
                    windowFunctions.getQuick(i).reset();
                }
                for (int i = 0; i < mapStatesCount; i++) {
                    mapStates.getQuick(i).reset();
                }
            }
        }

        /**
         * Computes the window over collected rows, in their order, and appends each output row to
         * {@code chain}. The rows are row ids of a {@link KeyMajorPageFrameRecordCursor} walk.
         */
        void compute(
                DirectLongList rows,
                RecordChain chain,
                SqlExecutionCircuitBreaker circuitBreaker,
                UnorderedPageFrameSequence<?> sequence
        ) {
            // the record moves to other frames, so a stream on this slot positions it again
            streamFrameIndex = -1;
            chain.rewind(rows.size());
            final long[] batch = batchRows;
            final long rowCount = rows.size();
            long prevOffset = -1;
            int currentFrameIndex = -1;
            int batches = 0;
            long i = 0;
            while (i < rowCount) {
                if (++batches == CHECK_BATCHES) {
                    batches = 0;
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    if (!sequence.isActive()) {
                        // the round was cancelled, by a LIMIT that closed the cursor or by another
                        // task's error: its output will never be read
                        return;
                    }
                }
                final int frameIndex = KeyMajorPageFrameRecordCursor.toFrameIndex(rows.get(i));
                if (frameIndex != currentFrameIndex) {
                    final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                    record.init(frameMemory);
                    toucher.of(frameAddressCache, frameIndex, frameMemory);
                    currentFrameIndex = frameIndex;
                }
                int n = 0;
                while (n < BATCH_ROWS && i < rowCount) {
                    final long rowId = rows.get(i);
                    if (KeyMajorPageFrameRecordCursor.toFrameIndex(rowId) != frameIndex) {
                        break;
                    }
                    batch[n++] = KeyMajorPageFrameRecordCursor.toFrameRowIndex(rowId);
                    i++;
                }
                if (toucher.isEnabled()) {
                    toucher.touch(batch, n);
                }
                for (int j = 0; j < n; j++) {
                    record.setRowIndex(batch[j]);
                    computeNext(record);
                    prevOffset = chain.put(virtualRecord, prevOffset);
                }
            }
        }

        void countTask() {
            taskCount++;
        }

        void computeNext(Record record) {
            // Groups first, as WindowRecordCursorFactory does: a bound function's computeNext is
            // a no-op, and its getters answer with what its group just materialized.
            for (int i = 0; i < mapStatesCount; i++) {
                mapStates.getQuick(i).computeNext(record);
            }
            for (int i = 0; i < windowFunctionsCount; i++) {
                windowFunctions.getQuick(i).computeNext(record);
            }
        }

        ObjList<Function> getFunctions() {
            return functions;
        }

        VirtualRecord getVirtualRecord() {
            return virtualRecord;
        }

        /**
         * Binds the functions to an execution, as the serial window cursor's {@code of()} does:
         * the per-query tracker and the per-partition maps first, then {@link Function#init}. A
         * worker slot's functions take symbol tables of their own.
         */
        void open(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
                for (int i = 0; i < windowFunctionsCount; i++) {
                    windowFunctions.getQuick(i).setMemoryTracker(memoryTracker);
                }
                for (int i = 0, n = functions.size(); i < n; i++) {
                    if (functions.getQuick(i) instanceof Reopenable reopenable) {
                        reopenable.reopen();
                    }
                }
                for (int i = 0; i < mapStatesCount; i++) {
                    final WindowMapState state = mapStates.getQuick(i);
                    state.setMemoryTracker(memoryTracker);
                    state.reopen();
                }
                pool.setMemoryTracker(memoryTracker);
            }
            record.of(symbolTableSource);
            if (ownsFunctions) {
                final boolean current = executionContext.getCloneSymbolTables();
                executionContext.setCloneSymbolTables(true);
                try {
                    Function.init(functions, symbolTableSource, executionContext, null);
                } finally {
                    executionContext.setCloneSymbolTables(current);
                }
            } else {
                Function.init(functions, symbolTableSource, executionContext, null);
            }
        }

        /**
         * Forgets where {@link #streamRow} left off: the next row positions the record again.
         */
        void resetStream() {
            streamFrameIndex = -1;
            streamTouchedHi = 0;
        }

        /**
         * Computes the window for the row at {@code index} of {@code rows}, which are row ids of a
         * {@link KeyMajorPageFrameRecordCursor} walk, leaving its output in this slot's virtual
         * record. Rows are streamed in order, one call each; the columns of up to a batch of rows
         * of one frame ahead are loaded together first.
         */
        void streamRow(DirectLongList rows, long index) {
            final long rowId = rows.get(index);
            final int frameIndex = KeyMajorPageFrameRecordCursor.toFrameIndex(rowId);
            if (frameIndex != streamFrameIndex) {
                final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                record.init(frameMemory);
                toucher.of(frameAddressCache, frameIndex, frameMemory);
                streamFrameIndex = frameIndex;
                streamTouchedHi = index;
            }
            if (index >= streamTouchedHi && toucher.isEnabled()) {
                final long[] batch = batchRows;
                final long rowCount = rows.size();
                int n = 0;
                long i = index;
                while (n < BATCH_ROWS && i < rowCount) {
                    final long id = rows.get(i);
                    if (KeyMajorPageFrameRecordCursor.toFrameIndex(id) != frameIndex) {
                        break;
                    }
                    batch[n++] = KeyMajorPageFrameRecordCursor.toFrameRowIndex(id);
                    i++;
                }
                toucher.touch(batch, n);
                streamTouchedHi = i;
            }
            record.setRowIndex(KeyMajorPageFrameRecordCursor.toFrameRowIndex(rowId));
            computeNext(record);
        }

        /**
         * Forgets every partition's state, so that the next row starts its key from scratch.
         */
        void toTop() {
            if (!isOpen) {
                // a closed map has no backing to clear
                return;
            }
            GroupByUtils.toTop(functions);
            for (int i = 0; i < mapStatesCount; i++) {
                mapStates.getQuick(i).clear();
            }
        }

        void ofFrames(PageFrameAddressCache frameAddressCache) {
            this.frameAddressCache = frameAddressCache;
            pool.of(frameAddressCache);
            resetStream();
            // the serial mode of an earlier execution may have pointed it at the scan's record
            virtualRecord.of(record);
        }
    }
}

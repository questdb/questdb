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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.table.KeyMajorPageFrameRecordCursor;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Computes a window partitioned by the key of a key-major index scan on the shared query
 * workers, and returns its rows in the scan's order.
 * <p>
 * <b>Prefix.</b> The query's own thread first computes the window itself, streaming row by row the
 * way the serial window does, over the first {@code cairo.sql.parallel.window.min.rows} rows of the
 * walk. It takes the walk in chunks that start small and double, so the first row costs what it
 * costs serially, and a LIMIT or a small result never reaches a worker or opens a worker slot.
 * <p>
 * <b>Rounds.</b> From the first key boundary after the prefix, the query's thread collects the row
 * ids of whole keys into tasks of about {@code cairo.sql.parallel.window.task.rows} rows. A task's
 * worker computes the window over its rows and writes the complete output rows into the task's
 * {@link RecordChain}. Tasks go out in rounds of at most {@code cairo.sql.parallel.window.round.rows}
 * rows, whatever the number of workers: while the query's thread returns the rows of one round,
 * task by task in scan order, the workers compute the next. A task's chain is freed once returned,
 * so at most two rounds of output exist at any time.
 * <p>
 * <b>Large keys.</b> A key above {@code cairo.sql.parallel.window.max.key.rows} rows would make one
 * task's output unbounded, so the query's thread streams it itself, between rounds, as it streams
 * the prefix: no output buffer at all, only the key's row ids.
 * <p>
 * The query's thread computes everything itself, as the serial window does, when the scan is not
 * a key-major one or has a frame other threads cannot read at a stable address (Parquet, or a
 * covering index).
 */
public class AsyncWindowRecordCursor implements RecordCursor {
    static final UnorderedPageFrameReducer REDUCER = AsyncWindowRecordCursor::reduce;
    // the first prefix chunk; each next one is twice as large, up to task.rows
    private static final long FIRST_CHUNK_ROWS = 256;
    private static final int LARGE_KEY_ACTIVE = 2;
    private static final int LARGE_KEY_NONE = 0;
    private static final int LARGE_KEY_PENDING = 1;
    private static final int MODE_PARALLEL = 2;
    private static final int MODE_SERIAL = 1;
    private static final int MODE_UNDECIDED = 0;
    private static final long ROW_IDS_INITIAL_CAPACITY = 1024;
    private final AsyncWindowAtom atom;
    private final long chainMaxPages;
    private final long chainPageSize;
    private final ColumnTypes columnTypes;
    private final long maxKeyRows;
    private final long minRows;
    private final SelectedRecord record;
    private final RecordSink recordSink;
    private final long roundRows;
    private final Round[] rounds = new Round[2];
    private final UnorderedPageFrameSequence<AsyncWindowAtom> sequence;
    private final long taskRows;
    private final int tasksPerRound;
    private RecordCursor baseCursor;
    private SqlExecutionCircuitBreaker circuitBreaker;
    // the round whose tasks are being returned
    private Round emitRound;
    // the task whose rows are being returned
    private Task emitTask;
    private int emitTaskIndex;
    private SqlExecutionContext executionContext;
    // the round the workers are computing, awaited before its rows are returned
    private Round inFlightRound;
    private boolean isOpen;
    private boolean isParallelPhase;
    private boolean isSequenceOpen;
    private boolean isWorkerSlotsOpen;
    private KeyMajorPageFrameRecordCursor keyMajorCursor;
    // the row ids of a key too large for a task, collected while its round was assembled
    private DirectLongList largeKeyRows;
    private long largeKeyRowsStreamed;
    private int largeKeyState = LARGE_KEY_NONE;
    private long maxRoundRows;
    private int mode = MODE_UNDECIDED;
    // the next prefix chunk's row count
    private long nextChunkRows;
    // true while the walk stopped inside a key that this thread streams
    private boolean ownerKeyOpen;
    private long ownerPos;
    // the row ids this thread streams: the prefix's current chunk, or a large key's
    private DirectLongList ownerRows;
    private long parallelRoundCount;
    private long parallelTaskCount;
    private long prefixRowsStreamed;
    private long taskRowsComputed;

    public AsyncWindowRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull AsyncWindowAtom atom,
            @NotNull UnorderedPageFrameSequence<AsyncWindowAtom> sequence,
            @NotNull ColumnTypes columnTypes,
            @NotNull RecordSink recordSink,
            int workerCount
    ) {
        this.atom = atom;
        this.sequence = sequence;
        this.columnTypes = columnTypes;
        this.recordSink = recordSink;
        this.taskRows = configuration.getSqlParallelWindowTaskRows();
        this.maxKeyRows = Math.max(taskRows, configuration.getSqlParallelWindowMaxKeyRows());
        this.minRows = configuration.getSqlParallelWindowMinRows();
        this.roundRows = Math.max(taskRows, configuration.getSqlParallelWindowRoundRows());
        // a few tasks per worker, so that the round's tasks balance across the workers; the
        // round's row budget, not this count, bounds the round's memory
        this.tasksPerRound = Math.max(2, 4 * workerCount);
        // A task's chain holds a task's rows, a few MB, and grows a page at a time: the window
        // store's page keeps the overshoot small where the sort's page would be most of the chain.
        // The sort's value cap bounds it, as it bounds the sort's chain that this replaces.
        this.chainPageSize = configuration.getSqlWindowStorePageSize();
        this.chainMaxPages = Math.max(1L, configuration.getSqlSortValueMaxBytes() / Numbers.ceilPow2(chainPageSize));
        final int columnCount = columnTypes.getColumnCount();
        final IntList identity = new IntList(columnCount);
        for (int i = 0; i < columnCount; i++) {
            identity.add(i);
        }
        this.record = new SelectedRecord(identity);
        rounds[0] = new Round();
        rounds[1] = new Round();
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (mode == MODE_UNDECIDED) {
            // the window returns one row per scan row; see WindowRecordCursorFactory
            baseCursor.calculateSize(circuitBreaker, counter);
        } else {
            while (hasNext()) {
                counter.inc();
            }
        }
    }

    @Override
    public void close() {
        if (!isOpen) {
            return;
        }
        isOpen = false;
        Throwable failure = null;
        awaitInFlightRound(true);
        if (isSequenceOpen) {
            isSequenceOpen = false;
            try {
                sequence.reset();
            } catch (Throwable th) {
                failure = th;
            }
        }
        for (int i = 0, n = atom.getSlotCount(); i < n; i++) {
            try {
                atom.getSlot(i - 1).closeCursor();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        failure = Misc.freeBestEffort(failure, rounds[0]);
        failure = Misc.freeBestEffort(failure, rounds[1]);
        ownerRows = Misc.free(ownerRows);
        largeKeyRows = Misc.free(largeKeyRows);
        keyMajorCursor = null;
        final RecordCursor baseCursor = this.baseCursor;
        this.baseCursor = null;
        failure = Misc.freeBestEffort(failure, baseCursor);
        resetWalkState();
        mode = MODE_UNDECIDED;
        isWorkerSlotsOpen = false;
        executionContext = null;
        circuitBreaker = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * The most rows one round of tasks held since the cursor opened or rewound.
     */
    @TestOnly
    public long getMaxRoundRows() {
        return maxRoundRows;
    }

    /**
     * Rows of keys too large for a task this cursor streamed on the query's thread since it opened or rewound.
     */
    @TestOnly
    public long getLargeKeyRowCount() {
        return largeKeyRowsStreamed;
    }

    /**
     * Rounds dispatched to the workers since the cursor opened or rewound.
     */
    @TestOnly
    public long getParallelRoundCount() {
        return parallelRoundCount;
    }

    /**
     * Tasks dispatched to the workers since the cursor opened or rewound.
     */
    @TestOnly
    public long getParallelTaskCount() {
        return parallelTaskCount;
    }

    /**
     * Rows of the prefix this cursor streamed on the query's thread since it opened or rewound.
     */
    @TestOnly
    public long getPrefixRowCount() {
        return prefixRowsStreamed;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record getRecordB() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) atom.getSlot(-1).getFunctions().getQuick(columnIndex);
    }

    /**
     * Rows the tasks of this cursor computed since it opened or rewound.
     */
    @TestOnly
    public long getTaskRowCount() {
        return taskRowsComputed;
    }

    @Override
    public boolean hasNext() {
        if (mode == MODE_UNDECIDED) {
            chooseMode();
        }
        if (mode == MODE_SERIAL) {
            return hasNextSerial();
        }
        while (true) {
            final Task emitTask = this.emitTask;
            if (emitTask != null) {
                if (emitTask.chain.hasNext()) {
                    return true;
                }
                // Returned: the output goes back now, so that only the rounds in flight hold any.
                // The next fill sizes the chain in one allocation from its row count.
                emitTask.chain.clear();
                this.emitTask = null;
            }
            if (ownerPos < ownerRows.size()) {
                // streamed row by row, as the serial window computes it
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                atom.getSlot(-1).streamRow(ownerRows, ownerPos++);
                return true;
            }
            final Round emitRound = this.emitRound;
            if (emitRound != null) {
                if (emitTaskIndex < emitRound.taskCount) {
                    startEmitting(emitRound.tasks.getQuick(emitTaskIndex++));
                    continue;
                }
                this.emitRound = null;
            }
            final Round inFlightRound = this.inFlightRound;
            if (inFlightRound != null) {
                // Cleared first: whether awaitRound() returns or throws, no task of the round is
                // running afterwards, so there is nothing left for close() to wait for.
                this.inFlightRound = null;
                sequence.awaitRound();
                this.emitRound = inFlightRound;
                emitTaskIndex = 0;
                // compute the next round while this one's rows are returned
                if (largeKeyState == LARGE_KEY_NONE && !isWalkExhausted()) {
                    dispatchRound();
                }
                continue;
            }
            if (refillOwnerRows()) {
                continue;
            }
            if (!isParallelPhase || largeKeyState != LARGE_KEY_NONE) {
                // the prefix has just ended, or a large key: refill once more
                continue;
            }
            if (isWalkExhausted()) {
                return false;
            }
            dispatchRound();
        }
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) atom.getSlot(-1).getFunctions().getQuick(columnIndex)).newSymbolTable();
    }

    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        // own the base cursor first: close() frees it when anything below throws
        this.baseCursor = baseCursor;
        isOpen = true;
        this.executionContext = executionContext;
        this.circuitBreaker = executionContext.getCircuitBreaker();
        mode = MODE_UNDECIDED;
        isWorkerSlotsOpen = false;
        resetWalkState();
        resetCounters();
        atom.resetTaskCounts();
        // the worker slots open only once a round is dispatched, see openWorkerSlots()
        atom.getSlot(-1).open(baseCursor, executionContext);
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
        // a window value depends on other rows of its partition, so every row must be computed
        RecordCursor.skipRows(this, rowCount);
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        // Ends the walk in a state the next pass can start from, also after a hasNext() that
        // threw: no round runs, and a sequence a failure left cancelled starts afresh.
        awaitInFlightRound(true);
        if (isSequenceOpen) {
            isSequenceOpen = false;
            sequence.reset();
        }
        baseCursor.toTop();
        for (int i = 0, n = atom.getSlotCount(); i < n; i++) {
            atom.getSlot(i - 1).toTop();
        }
        resetWalkState();
        resetCounters();
        if (mode == MODE_PARALLEL) {
            // the rewound scan collects its frames again, so the slots read them afresh
            keyMajorCursor.prepareFrames();
            atom.getSlot(-1).ofFrames(keyMajorCursor.getFrameAddressCache());
            if (isWorkerSlotsOpen) {
                atom.ofWorkerFrames(keyMajorCursor.getFrameAddressCache());
            }
        } else if (mode == MODE_SERIAL) {
            record.of(atom.getSlot(-1).getVirtualRecord());
        }
    }

    private static Throwable addFailure(@Nullable Throwable failure, Throwable th) {
        if (failure == null) {
            return th;
        }
        if (failure != th) {
            failure.addSuppressed(th);
        }
        return failure;
    }

    private static void reduce(
            int workerId,
            @NotNull PageFrameMemoryRecord unused,
            int taskIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> sequence,
            @Nullable UnorderedPageFrameSequence<?> stealingSequence
    ) {
        final AsyncWindowAtom atom = (AsyncWindowAtom) sequence.getAtom();
        final Task task = atom.getRound().tasks.getQuick(taskIndex);
        final int slotId = atom.maybeAcquire(workerId, stealingSequence == sequence, circuitBreaker);
        try {
            final AsyncWindowAtom.Slot slot = atom.getSlot(slotId);
            // the task's keys share no partition with any key this slot computed before
            slot.toTop();
            slot.compute(task.rows, task.chain, circuitBreaker, sequence);
            slot.countTask();
        } finally {
            atom.release(slotId);
        }
    }

    // Waits for the round the workers are computing, if any, and drops its output: a cursor that
    // closes or rewinds mid-round must not free or reuse what the round's tasks still write to.
    // The round is cancelled first, which the tasks notice between batches, so the wait is short.
    private void awaitInFlightRound(boolean cancel) {
        final Round round = inFlightRound;
        if (round == null) {
            return;
        }
        inFlightRound = null;
        if (cancel) {
            sequence.cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
            try {
                sequence.awaitRound();
            } catch (Throwable ignore) {
                // the cancellation asked for here, or an error no one reads now; either way no
                // task of the round runs any more
            }
        } else {
            sequence.awaitRound();
        }
    }

    private void chooseMode() {
        mode = MODE_SERIAL;
        if (baseCursor instanceof KeyMajorPageFrameRecordCursor keyMajorCursor) {
            keyMajorCursor.prepareFrames();
            if (keyMajorCursor.hasOnlyPlainNativeFrames()) {
                this.keyMajorCursor = keyMajorCursor;
                mode = MODE_PARALLEL;
                final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
                ownerRows = newRowIds(memoryTracker);
                atom.getSlot(-1).ofFrames(keyMajorCursor.getFrameAddressCache());
                record.of(atom.getSlot(-1).getVirtualRecord());
                return;
            }
        }
        record.of(atom.getSlot(-1).getVirtualRecord());
        atom.getSlot(-1).getVirtualRecord().of(baseCursor.getRecord());
    }

    // Collects the next round's tasks from the scan and dispatches them to the workers. A round
    // stops at a key too large for a task, which this thread then streams itself.
    private void dispatchRound() {
        if (!isSequenceOpen) {
            openWorkerSlots();
            try {
                sequence.ofRounds(baseCursor, executionContext);
            } catch (SqlException e) {
                throw CairoException.nonCritical().put(e.getFlyweightMessage());
            }
            isSequenceOpen = true;
        }
        final Round round = emitRound == rounds[0] ? rounds[1] : rounds[0];
        round.clear();
        long collectedRows = 0;
        while (collectedRows < roundRows && round.taskCount < tasksPerRound) {
            final Task task = round.nextTask(this);
            final DirectLongList rows = task.rows;
            // a task stops at the first key boundary past taskRows, or past what the round has left
            final long taskLimit = Math.min(taskRows, roundRows - collectedRows);
            boolean stop = false;
            while (rows.size() < taskLimit) {
                final long keyLo = rows.size();
                final int status = keyMajorCursor.collectKeyRows(rows, maxKeyRows);
                if (status == KeyMajorPageFrameRecordCursor.COLLECT_KEY_END) {
                    continue;
                }
                if (status == KeyMajorPageFrameRecordCursor.COLLECT_ROW_LIMIT) {
                    // A key too large for a task: its rows so far move to this thread's stream.
                    if (largeKeyRows == null) {
                        largeKeyRows = newRowIds(executionContext.getMemoryTracker());
                    }
                    largeKeyRows.clear();
                    final long keyRows = rows.size() - keyLo;
                    largeKeyRows.ensureCapacity(keyRows);
                    for (long r = keyLo, hi = rows.size(); r < hi; r++) {
                        largeKeyRows.add(rows.get(r));
                    }
                    rows.setPos(keyLo);
                    largeKeyState = LARGE_KEY_PENDING;
                    // the task's list grew for the key's rows, which it no longer holds
                    if (keyLo == 0) {
                        shrinkIfOversized(rows, taskRows);
                    }
                }
                // a key too large for a task, or no key left
                stop = true;
                break;
            }
            if (rows.size() == 0) {
                round.dropLastTask();
            } else {
                round.taskRowCounts.add(rows.size());
                collectedRows += rows.size();
            }
            if (stop) {
                break;
            }
        }
        if (round.taskCount > 0) {
            atom.setRound(round);
            sequence.dispatchRound(REDUCER, round.taskRowCounts);
            inFlightRound = round;
            parallelRoundCount++;
            parallelTaskCount += round.taskCount;
            taskRowsComputed += collectedRows;
            maxRoundRows = Math.max(maxRoundRows, collectedRows);
        }
    }

    private boolean hasNextSerial() {
        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        if (baseCursor.hasNext()) {
            atom.getSlot(-1).computeNext(baseCursor.getRecord());
            return true;
        }
        return false;
    }

    private boolean isWalkExhausted() {
        return keyMajorCursor.isWalkExhausted();
    }

    // Gives a row id list's memory back once it has grown well past what a task or a chunk needs.
    private static void shrinkIfOversized(DirectLongList rows, long taskRows) {
        if (rows.getCapacity() > 2 * Math.max(taskRows, ROW_IDS_INITIAL_CAPACITY)) {
            rows.resetCapacity();
        }
    }

    private DirectLongList newRowIds(MemoryTracker memoryTracker) {
        // charged to the query, under the tag of the parallel operators' row id lists
        final DirectLongList rows = new DirectLongList(ROW_IDS_INITIAL_CAPACITY, MemoryTag.NATIVE_OFFLOAD, true);
        rows.setMemoryTracker(memoryTracker);
        rows.reopen();
        return rows;
    }

    private Task newTask() {
        final RecordChain chain = new RecordChain(
                columnTypes,
                recordSink,
                chainPageSize,
                (int) Math.min(chainMaxPages, Integer.MAX_VALUE),
                PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES.getPropertyPath()
        );
        DirectLongList rows = null;
        try {
            final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
            chain.setMemoryTracker(memoryTracker);
            chain.setSymbolTableResolver(this);
            rows = newRowIds(memoryTracker);
            return new Task(chain, rows);
        } catch (Throwable th) {
            Misc.free(chain);
            Misc.free(rows);
            throw th;
        }
    }

    private void openWorkerSlots() {
        if (!isWorkerSlotsOpen) {
            isWorkerSlotsOpen = true;
            for (int i = 0, n = atom.getSlotCount() - 1; i < n; i++) {
                try {
                    atom.getSlot(i).open(baseCursor, executionContext);
                } catch (SqlException e) {
                    throw CairoException.nonCritical().put(e.getFlyweightMessage());
                }
            }
            atom.ofWorkerFrames(keyMajorCursor.getFrameAddressCache());
        }
    }

    // Fills ownerRows with the next rows this thread streams itself: the prefix's next chunk, or
    // the next chunk of a large key. Returns true when it has rows to stream.
    private boolean refillOwnerRows() {
        final AsyncWindowAtom.Slot owner = atom.getSlot(-1);
        if (largeKeyState == LARGE_KEY_PENDING) {
            // The rounds before the key have been returned and none runs, so no task can take this
            // thread's slot until the key ends: its state carries from chunk to chunk.
            final DirectLongList rows = ownerRows;
            ownerRows = largeKeyRows;
            largeKeyRows = rows;
            ownerPos = 0;
            ownerKeyOpen = true;
            largeKeyState = LARGE_KEY_ACTIVE;
            owner.toTop();
            owner.resetStream();
            record.of(owner.getVirtualRecord());
            largeKeyRowsStreamed += ownerRows.size();
            return ownerRows.size() > 0;
        }
        if (largeKeyState == LARGE_KEY_ACTIVE) {
            if (ownerKeyOpen) {
                // the key's first rows were up to max.key.rows: give that memory back
                shrinkIfOversized(ownerRows, taskRows);
                ownerRows.clear();
                ownerPos = 0;
                ownerKeyOpen = keyMajorCursor.collectKeyRows(ownerRows, taskRows) == KeyMajorPageFrameRecordCursor.COLLECT_ROW_LIMIT;
                owner.resetStream();
                largeKeyRowsStreamed += ownerRows.size();
                if (ownerRows.size() > 0) {
                    return true;
                }
            }
            largeKeyState = LARGE_KEY_NONE;
            return false;
        }
        if (!isParallelPhase) {
            // the prefix: whole keys and, at its end, the rest of the key it stopped in
            ownerRows.clear();
            ownerPos = 0;
            final long chunkRows = nextChunkRows;
            nextChunkRows = Math.min(taskRows, 2 * chunkRows);
            while (ownerRows.size() < chunkRows) {
                if (!ownerKeyOpen && prefixRowsStreamed + ownerRows.size() >= minRows) {
                    // at a key boundary past the prefix: the next keys go to the workers
                    isParallelPhase = true;
                    break;
                }
                final int status = keyMajorCursor.collectKeyRows(ownerRows, chunkRows - ownerRows.size());
                if (status == KeyMajorPageFrameRecordCursor.COLLECT_ROW_LIMIT) {
                    ownerKeyOpen = true;
                    break;
                }
                ownerKeyOpen = false;
                if (status == KeyMajorPageFrameRecordCursor.COLLECT_EXHAUSTED) {
                    break;
                }
            }
            prefixRowsStreamed += ownerRows.size();
            owner.resetStream();
            // the record may point at the last task returned before a rewind
            record.of(owner.getVirtualRecord());
            if (ownerRows.size() > 0) {
                return true;
            }
            if (isWalkExhausted()) {
                // nothing for the workers at all
                isParallelPhase = true;
            }
            return false;
        }
        return false;
    }

    private void resetCounters() {
        maxRoundRows = 0;
        largeKeyRowsStreamed = 0;
        parallelRoundCount = 0;
        parallelTaskCount = 0;
        prefixRowsStreamed = 0;
        taskRowsComputed = 0;
    }

    private void resetWalkState() {
        emitTask = null;
        emitRound = null;
        emitTaskIndex = 0;
        inFlightRound = null;
        largeKeyState = LARGE_KEY_NONE;
        isParallelPhase = minRows <= 0;
        ownerKeyOpen = false;
        ownerPos = 0;
        nextChunkRows = Math.min(FIRST_CHUNK_ROWS, taskRows);
        if (ownerRows != null) {
            ownerRows.clear();
        }
        if (rounds[0] != null) {
            rounds[0].clear();
            rounds[1].clear();
        }
    }

    private void startEmitting(Task task) {
        // Once per task of about task.rows rows, a coarse site: cancellation and the timeout are
        // checked on every call, unlike the per-row check, which samples them.
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        task.chain.toTop();
        record.of(task.chain.getRecord());
        emitTask = task;
    }

    /**
     * The tasks of one dispatch to the workers, reused round after round.
     */
    static class Round implements Mutable, QuietCloseable {
        private final LongList taskRowCounts = new LongList();
        private final ObjList<Task> tasks = new ObjList<>();
        private int taskCount;

        @Override
        public void clear() {
            taskCount = 0;
            taskRowCounts.clear();
        }

        @Override
        public void close() {
            clear();
            Misc.freeObjListAndClear(tasks);
        }

        void dropLastTask() {
            taskCount--;
        }

        Task nextTask(AsyncWindowRecordCursor cursor) {
            if (taskCount == tasks.size()) {
                tasks.add(cursor.newTask());
            }
            final Task task = tasks.getQuick(taskCount++);
            task.reuse(cursor.taskRows);
            return task;
        }
    }

    /**
     * The row ids of whole keys and, once computed, their output rows.
     */
    static class Task implements QuietCloseable {
        private final RecordChain chain;
        private final DirectLongList rows;

        private Task(RecordChain chain, DirectLongList rows) {
            this.chain = chain;
            this.rows = rows;
        }

        @Override
        public void close() {
            Misc.free(chain);
            Misc.free(rows);
        }

        // Empties the task for its next round. A task usually holds about taskRows row ids, but
        // one that grew for a large key would keep that key's memory for good, also after the
        // key's rows moved out to the query's thread: give it back when the list's capacity, not
        // its last size, is well above the usual.
        private void reuse(long taskRows) {
            shrinkIfOversized(rows, taskRows);
            rows.clear();
        }
    }
}

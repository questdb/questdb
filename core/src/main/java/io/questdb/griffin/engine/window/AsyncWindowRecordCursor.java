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
 * The query's own thread walks the scan and collects the row ids of whole keys into tasks of about
 * {@code cairo.sql.parallel.window.task.rows} rows. A task's worker computes the window over its
 * rows and writes the complete output rows into the task's {@link RecordChain}, reading the
 * columns in batches that overlap their cache misses. Tasks go out in rounds: while the query's
 * thread returns the rows of one round, task by task in scan order, the workers compute the next.
 * So at most two rounds of output exist at any time, whatever the size of the result.
 * <p>
 * A key larger than {@code cairo.sql.parallel.window.max.key.rows} would make one task's output
 * unbounded, so the query's thread computes such a key itself, in chunks, between two rounds.
 * <p>
 * The query's thread computes everything itself, as the serial window does, when the scan is not
 * a key-major one or has a frame other threads cannot read at a stable address (Parquet, or a
 * covering index).
 */
public class AsyncWindowRecordCursor implements RecordCursor {
    private static final int GIANT_ACTIVE = 2;
    private static final int GIANT_FIRST_CHUNK = 1;
    private static final int GIANT_NONE = 0;
    private static final int MODE_PARALLEL = 2;
    private static final int MODE_SERIAL = 1;
    private static final int MODE_UNDECIDED = 0;
    static final UnorderedPageFrameReducer REDUCER = AsyncWindowRecordCursor::reduce;
    private final AsyncWindowAtom atom;
    private final long chainMaxPages;
    private final long chainPageSize;
    private final ColumnTypes columnTypes;
    private final SelectedRecord record;
    private final RecordSink recordSink;
    private final Round[] rounds = new Round[2];
    private final UnorderedPageFrameSequence<AsyncWindowAtom> sequence;
    private final long taskRows;
    private final int tasksPerRound;
    private RecordCursor baseCursor;
    private SqlExecutionCircuitBreaker circuitBreaker;
    // the task whose rows are being returned
    private Task emitTask;
    private int emitTaskIndex;
    // the round whose tasks are being returned
    private Round emitRound;
    private SqlExecutionContext executionContext;
    private int giant = GIANT_NONE;
    // the chunk of a key too large for a task, computed on this thread
    private Task giantTask;
    // the round the workers are computing, awaited before its rows are returned
    private Round inFlightRound;
    private boolean isOpen;
    private boolean isSequenceOpen;
    private KeyMajorPageFrameRecordCursor keyMajorCursor;
    private long maxKeyRows;
    private int mode = MODE_UNDECIDED;
    private long largeKeyChunkCount;
    private long parallelRoundCount;
    private long parallelTaskCount;

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
        // a few tasks per worker, so that the round's tasks balance across the workers
        this.tasksPerRound = Math.max(2, 4 * workerCount);
        this.chainPageSize = configuration.getSqlSortValuePageSize();
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
                failure = addFailure(failure, th);
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
        final Task giantTask = this.giantTask;
        this.giantTask = null;
        failure = Misc.freeBestEffort(failure, giantTask);
        keyMajorCursor = null;
        final RecordCursor baseCursor = this.baseCursor;
        this.baseCursor = null;
        failure = Misc.freeBestEffort(failure, baseCursor);
        resetWalkState();
        mode = MODE_UNDECIDED;
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Chunks of keys too large for a task this cursor computed on the query's thread since it
     * opened.
     */
    @TestOnly
    public long getLargeKeyChunkCount() {
        return largeKeyChunkCount;
    }

    /**
     * Rounds dispatched to the workers since the cursor opened.
     */
    @TestOnly
    public long getParallelRoundCount() {
        return parallelRoundCount;
    }

    /**
     * Tasks dispatched to the workers since the cursor opened.
     */
    @TestOnly
    public long getParallelTaskCount() {
        return parallelTaskCount;
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
                this.emitTask = null;
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
                if (giant == GIANT_NONE && !isWalkExhausted()) {
                    dispatchRound();
                }
                continue;
            }
            if (giant != GIANT_NONE) {
                if (computeGiantChunk()) {
                    startEmitting(giantTask);
                }
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
        resetWalkState();
        largeKeyChunkCount = 0;
        parallelRoundCount = 0;
        parallelTaskCount = 0;
        // the worker slots open only if the cursor dispatches, see chooseMode()
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
        awaitInFlightRound(false);
        baseCursor.toTop();
        for (int i = 0, n = atom.getSlotCount(); i < n; i++) {
            atom.getSlot(i - 1).toTop();
        }
        resetWalkState();
        if (mode == MODE_PARALLEL) {
            // the rewound scan collects its frames again, so the slots read them afresh
            keyMajorCursor.prepareFrames();
            atom.ofFrames(keyMajorCursor.getFrameAddressCache());
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
            slot.compute(task.rows, task.chain, circuitBreaker);
        } finally {
            atom.release(slotId);
        }
    }

    // Waits for the round the workers are computing, if any, and drops its output: a cursor that
    // closes or rewinds mid-round must not free or reuse what the round's tasks still write to.
    // Closing cancels the round first, which makes the wait short but leaves the sequence
    // cancelled until it is reset; a rewind lets the round finish, so the next rounds still run.
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
                for (int i = 0, n = atom.getSlotCount() - 1; i < n; i++) {
                    try {
                        atom.getSlot(i).open(baseCursor, executionContext);
                    } catch (SqlException e) {
                        throw CairoException.nonCritical().put(e.getFlyweightMessage());
                    }
                }
                atom.ofFrames(keyMajorCursor.getFrameAddressCache());
                return;
            }
        }
        record.of(atom.getSlot(-1).getVirtualRecord());
        atom.getSlot(-1).getVirtualRecord().of(baseCursor.getRecord());
    }

    // Computes the next chunk of the key too large for a task on this thread, into giantTask.
    // Returns false when the key ended without another row.
    private boolean computeGiantChunk() {
        final AsyncWindowAtom.Slot owner = atom.getSlot(-1);
        if (giant == GIANT_FIRST_CHUNK) {
            // the key's first rows, already collected while the round before it was assembled
            giant = GIANT_ACTIVE;
            owner.toTop();
        } else {
            giantTask.rows.clear();
            final int status = keyMajorCursor.collectKeyRows(giantTask.rows, taskRows);
            if (status != KeyMajorPageFrameRecordCursor.COLLECT_ROW_LIMIT) {
                giant = GIANT_NONE;
            }
            if (giantTask.rows.size() == 0) {
                return false;
            }
        }
        owner.compute(giantTask.rows, giantTask.chain, circuitBreaker);
        largeKeyChunkCount++;
        return true;
    }

    // Collects the next round's tasks from the scan and dispatches them to the workers. A round
    // stops at a key too large for a task, which this thread then computes itself.
    private void dispatchRound() {
        if (!isSequenceOpen) {
            try {
                sequence.ofRounds(baseCursor, executionContext);
            } catch (SqlException e) {
                throw CairoException.nonCritical().put(e.getFlyweightMessage());
            }
            isSequenceOpen = true;
        }
        final Round round = emitRound == rounds[0] ? rounds[1] : rounds[0];
        round.clear();
        final long roundRows = taskRows * tasksPerRound;
        long collectedRows = 0;
        while (collectedRows < roundRows && round.taskCount < tasksPerRound) {
            final Task task = round.nextTask(this);
            final DirectLongList rows = task.rows;
            boolean stop = false;
            while (rows.size() < taskRows) {
                final long keyLo = rows.size();
                final int status = keyMajorCursor.collectKeyRows(rows, maxKeyRows);
                if (status == KeyMajorPageFrameRecordCursor.COLLECT_KEY_END) {
                    continue;
                }
                if (status == KeyMajorPageFrameRecordCursor.COLLECT_ROW_LIMIT) {
                    // A key too large for a task: its rows so far move to this thread's chunk.
                    final Task giantTask = getGiantTask();
                    giantTask.rows.clear();
                    final long keyRows = rows.size() - keyLo;
                    giantTask.rows.ensureCapacity(keyRows);
                    for (long r = keyLo, hi = rows.size(); r < hi; r++) {
                        giantTask.rows.add(rows.get(r));
                    }
                    rows.setPos(keyLo);
                    giant = GIANT_FIRST_CHUNK;
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
        }
    }

    private Task getGiantTask() {
        if (giantTask == null) {
            giantTask = newTask();
        }
        return giantTask;
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

    private Task newTask() {
        final RecordChain chain = new RecordChain(
                columnTypes,
                recordSink,
                chainPageSize,
                (int) Math.min(chainMaxPages, Integer.MAX_VALUE),
                PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES.getPropertyPath()
        );
        try {
            chain.setMemoryTracker(executionContext.getMemoryTracker());
            chain.setSymbolTableResolver(this);
            return new Task(chain);
        } catch (Throwable th) {
            Misc.free(chain);
            throw th;
        }
    }

    private void resetWalkState() {
        emitTask = null;
        emitRound = null;
        emitTaskIndex = 0;
        inFlightRound = null;
        giant = GIANT_NONE;
        if (rounds[0] != null) {
            rounds[0].clear();
            rounds[1].clear();
        }
    }

    private void startEmitting(Task task) {
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
            task.rows.clear();
            return task;
        }
    }

    /**
     * The row ids of whole keys and, once computed, their output rows.
     */
    static class Task implements QuietCloseable {
        private final RecordChain chain;
        private final DirectLongList rows;

        private Task(RecordChain chain) {
            this.chain = chain;
            this.rows = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public void close() {
            Misc.free(chain);
            Misc.free(rows);
        }
    }
}

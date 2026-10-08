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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.window.ReplayableWindowFunction;
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
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Computes a window partitioned by the key of a key-major index scan on the shared query
 * workers, and returns its rows in the scan's order.
 * <p>
 * <b>Prefix.</b> The query's own thread first computes the window itself, streaming row by row the
 * way the serial window does, over the first {@code cairo.sql.parallel.window.min.rows} rows of the
 * walk. It takes the walk in chunks that start small and double, so the first row costs what it
 * costs serially, and a LIMIT or a small result never reaches a worker or opens a worker slot.
 * <p>
 * <b>Pipeline.</b> Past the prefix, the query's thread collects the walk's row ids into tasks of
 * about {@code cairo.sql.parallel.window.task.rows} rows, and the tasks into rounds of at most
 * {@code cairo.sql.parallel.window.round.rows} rows. Up to {@code cairo.sql.parallel.window.max.rounds}
 * rounds are alive at a time, each dispatched through a sequence of its own: the one whose rows are
 * being returned, task by task in scan order, and those the workers compute ahead of it. A task's
 * worker computes the window over its rows and writes the complete output rows into the task's
 * {@link RecordChain}. Once its rows have been returned, the chain of a task the query's own thread
 * computed is freed. The chain of a task a worker computed keeps its memory for the task's next
 * fill, see {@link #finishEmitting}, within the output of {@code max.rounds} full rounds in all,
 * until the rows run out or the cursor closes.
 * <p>
 * <b>Splitting keys.</b> When the window allows it, see {@link AsyncWindowSplitPlan}, tasks are
 * plain slices of the walk: a key larger than a task spans several, each computed by any worker,
 * and the next task rebuilds the key's state from warm-up rows, or the query's thread combines its
 * rows with the key's running values before returning them.
 * <p>
 * <b>Large keys.</b> Otherwise a key is computed whole by one task, and one above
 * {@code cairo.sql.parallel.window.max.key.rows} rows would make a task's output unbounded: the
 * walk skips it, and the query's thread streams it itself, in scan order, walking it apart from the
 * walk. Rounds of the keys after it keep being dispatched meanwhile; their rows follow it.
 * <p>
 * The query's thread computes everything itself, as the serial window does, when the scan is not
 * a key-major one or has a frame other threads cannot read at a stable address (Parquet, or a
 * covering index).
 */
public class AsyncWindowRecordCursor implements RecordCursor {
    static final UnorderedPageFrameReducer REDUCER = AsyncWindowRecordCursor::reduce;
    // the first prefix chunk; each next one is twice as large, up to task.rows
    private static final long FIRST_CHUNK_ROWS = 256;
    private static final int MODE_PARALLEL = 2;
    private static final int MODE_SERIAL = 1;
    private static final int MODE_UNDECIDED = 0;
    private static final long ROW_IDS_INITIAL_CAPACITY = 1024;
    // The first tasks' row budget is task.rows over this, at least 1024 rows or task.rows; it
    // doubles every worker-count tasks up to task.rows, so that a result of a few hundred thousand
    // rows, a frequent key's, makes tasks enough for every worker.
    private static final long TASK_RAMP_DIVISOR = 16;
    private static final int SEGMENT_NONE = 0;
    private static final int SEGMENT_ROUND = 1;
    private static final int SEGMENT_STREAM = 2;
    private final AsyncWindowAtom atom;
    // MODE_PREFIX: each combined column's value at the last row returned, as raw bits
    private final long[] carry;
    // whether a column is combined with a carry, not folded or replayed
    private final boolean hasCarryOp;
    // OP_REPLAY: each replayed column's function, the query thread's own, see ReplayChain
    private final ReplayableWindowFunction[] replayFunctions;
    // whether a column is replayed, see AsyncWindowSplitPlan.OP_REPLAY
    private final boolean hasReplay;
    // OP_FOLD, OP_REPLAY: the pass over every task's rows in walk order, null for neither
    private final ReplayChain replayChain;
    // OP_REPLAY: the rows of ownerRows at which a key starts, ascending, see refillPrefix()
    private final LongList ownerKeyStarts = new LongList();
    private int ownerKeyStartIndex;
    private final long chainMaxPages;
    private final long chainPageSize;
    private final ColumnTypes columnTypes;
    private final long maxKeyRows;
    private final long minRows;
    private final SelectedRecord record;
    private final RecordSink recordSink;
    private final long roundRows;
    private final Round[] rounds;
    // whether the workers may compute the chain's rows column-wise, see AsyncWindowRowKernel
    private final boolean rowKernelsEnabled;
    // the kernels are compiled once the atom has every step, at the first execution
    private boolean rowKernelsCompiled;
    // queue of what to return after the head, in scan order: rounds and streamed keys
    private final int[] segmentKeys;
    private final int[] segmentKinds;
    private final Round[] segmentRounds;
    private final AsyncWindowSplitPlan splitPlan;
    private final boolean splitsKeys;

    private final long taskRows;
    private final int tasksPerRound;
    private final int workerCount;
    // the first tasks' budget, see TASK_RAMP_ROWS: at least a few times a key's warm-up rows
    private final long rampTaskRows;
    // tasks collected since the walk started, for the ramp of task sizes
    private long tasksCollected;
    private RecordCursor baseCursor;
    private SqlExecutionCircuitBreaker circuitBreaker;
    // keys split over tasks with a GROUP BY step: groups that span tasks are completed by this
    // thread, see AsyncWindowAtom.GroupSplit; known once the atom has all its steps
    private boolean groupSplit;
    // a task whose rows follow the groups this thread completed for it, see processGroupBoundary()
    private Task deferredTask;
    // the groups this thread completed, see processGroupBoundary()
    private Task ownerGroupTask;
    // rows of the walk collected so far, past the warm-up rows: the next row's walk position
    private long walkPosition;
    // the task whose rows are being returned
    private Task emitTask;
    private int emitTaskIndex;
    private SqlExecutionContext executionContext;
    private int headKey;
    // what is being returned now
    private int headKind = SEGMENT_NONE;
    private Round headRound;
    // the next frame position of the streamed key, -1 once its last frame has been read
    private int headStreamPos;
    private boolean isOpen;
    // the query's thread streamed rows it has not ended with AsyncWindowAtom.Slot.flush() yet
    private boolean isOwnerFlushPending;
    private boolean isParallelPhase;
    private boolean isWorkerSlotsOpen;
    // chain memory that returned tasks keep for their next fill, see finishEmitting()
    private long keptChainBytes;
    private long keptChainCount;
    private KeyMajorPageFrameRecordCursor keyMajorCursor;
    private long largeKeyRowsStreamed;
    private long maxRoundRows;
    private int mode = MODE_UNDECIDED;
    // the next prefix chunk's row count
    private long nextChunkRows;
    private long ownerPos;
    // the row ids this thread streams: the prefix's current chunk, or a large key's
    private DirectLongList ownerRows;
    private long parallelRoundCount;
    private long parallelTaskCount;
    private long prefixRowsStreamed;
    private long roundsAheadOfStreams;
    private int segmentCount;
    private int segmentHead;
    private long taskRowsComputed;
    // true while the walk stands inside a key, which the next task then continues
    private boolean walkKeyOpen;
    // MODE_WARMUP: the last warm-up rows of the key the walk stands in
    private DirectLongList warmRows;

    public AsyncWindowRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull AsyncWindowAtom atom,
            @NotNull ObjList<UnorderedPageFrameSequence<RoundAtom>> sequences,
            @NotNull ColumnTypes columnTypes,
            @NotNull RecordSink recordSink,
            @NotNull AsyncWindowSplitPlan splitPlan,
            int workerCount
    ) {
        this.atom = atom;
        this.columnTypes = columnTypes;
        this.recordSink = recordSink;
        this.splitPlan = splitPlan;
        this.splitsKeys = splitPlan.getMode() != AsyncWindowSplitPlan.MODE_NONE;
        this.carry = new long[splitPlan.getPrefixCount()];
        this.replayFunctions = new ReplayableWindowFunction[splitPlan.getPrefixCount()];
        boolean hasReplay = false;
        for (int j = 0, n = splitPlan.getPrefixCount(); j < n; j++) {
            if (splitPlan.getPrefixOp(j) == AsyncWindowSplitPlan.OP_REPLAY) {
                // the window's own function, which computed the rows before the tasks too
                replayFunctions[j] = (ReplayableWindowFunction) atom.getSlot(-1).getFunction(splitPlan.getPrefixColumn(j));
                hasReplay = true;
            }
        }
        this.hasReplay = hasReplay;
        boolean hasCarryOp = false;
        for (int j = 0, n = splitPlan.getPrefixCount(); j < n; j++) {
            hasCarryOp |= !AsyncWindowSplitPlan.isFold(splitPlan.getPrefixOp(j));
        }
        this.hasCarryOp = hasCarryOp;
        this.taskRows = configuration.getSqlParallelWindowTaskRows();
        this.maxKeyRows = Math.max(taskRows, configuration.getSqlParallelWindowMaxKeyRows());
        // the prefix: no more than min.rows, which also gates the parallel plan, see prefix.rows
        final long minRows = configuration.getSqlParallelWindowMinRows();
        this.minRows = minRows > 0 ? Math.min(minRows, configuration.getSqlParallelWindowPrefixRows()) : 0;
        this.roundRows = Math.max(taskRows, configuration.getSqlParallelWindowRoundRows());
        // a few tasks per worker, so that the round's tasks balance across the workers; the
        // round's row budget, not this count, bounds the round's memory
        this.tasksPerRound = Math.max(2, 4 * workerCount);
        this.workerCount = Math.max(1, workerCount);
        this.rampTaskRows = Math.min(taskRows, Math.max(Math.max(Math.min(taskRows, 1024), taskRows / TASK_RAMP_DIVISOR), 4 * splitPlan.getWarmupRows()));
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
        final int roundCount = sequences.size();
        this.rounds = new Round[roundCount];
        // every task of every round, the most the walk holds ahead of the rows being returned
        this.replayChain = splitPlan.hasFold() ? new ReplayChain(splitPlan, replayFunctions, roundCount * tasksPerRound) : null;
        for (int i = 0; i < roundCount; i++) {
            final UnorderedPageFrameSequence<RoundAtom> sequence = sequences.getQuick(i);
            rounds[i] = new Round(sequence);
            sequence.getAtom().round = rounds[i];
            sequence.getAtom().replayChain = replayChain;
        }
        this.rowKernelsEnabled = configuration.isSqlParallelWindowKeyRunsEnabled();
        // a round can bring a streamed key with it, and the walk can meet several in a row
        final int segmentCapacity = 4 * roundCount;
        this.segmentKinds = new int[segmentCapacity];
        this.segmentKeys = new int[segmentCapacity];
        this.segmentRounds = new Round[segmentCapacity];
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (mode == MODE_UNDECIDED && !atom.hasRowChangingStage()) {
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
        try {
            stopRounds();
        } catch (Throwable th) {
            failure = th;
        }
        if (replayChain != null) {
            replayChain.reset();
        }
        for (int i = 0, n = atom.getSlotCount(); i < n; i++) {
            try {
                atom.getSlot(i - 1).closeCursor();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        for (Round round : rounds) {
            failure = Misc.freeBestEffort(failure, round);
        }
        failure = Misc.freeBestEffort(failure, ownerGroupTask);
        ownerGroupTask = null;
        deferredTask = null;
        keptChainBytes = 0;
        ownerRows = Misc.free(ownerRows);
        warmRows = Misc.free(warmRows);
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
     * Chain memory that returned tasks keep for their next fill now.
     */
    @TestOnly
    public long getKeptChainBytes() {
        return keptChainBytes;
    }

    /**
     * Returned tasks whose chain kept its memory for the next fill, since the cursor opened or
     * rewound.
     */
    @TestOnly
    public long getKeptChainCount() {
        return keptChainCount;
    }

    /**
     * Rows of keys too large for a task this cursor streamed on the query's thread since it
     * opened or rewound.
     */
    @TestOnly
    public long getLargeKeyRowCount() {
        return largeKeyRowsStreamed;
    }

    /**
     * The most rows one round of tasks held since the cursor opened or rewound, warm-up rows
     * included.
     */
    @TestOnly
    public long getMaxRoundRows() {
        return maxRoundRows;
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
     * Tasks whose folded and replayed columns a worker thread computed, see {@link ReplayChain},
     * since the cursor opened or rewound; -1 when the plan neither folds nor replays.
     */
    @TestOnly
    public long getWorkerPassCount() {
        if (replayChain == null) {
            return -1;
        }
        // read under the lock, which the last pass's writes happened before
        while (!replayChain.lock.compareAndSet(0, 1)) {
            Os.pause();
        }
        try {
            return replayChain.workerPassCount;
        } finally {
            replayChain.lock.set(0);
        }
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

    /**
     * Rounds of later keys that were dispatched, computing or computed, when the query's thread
     * started streaming a large key, summed over the large keys since the cursor opened or rewound.
     */
    @TestOnly
    public long getRoundsAheadOfStreams() {
        return roundsAheadOfStreams;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return atom.getSlot(-1).getOutputSymbols().getSymbolTable(columnIndex);
    }

    /**
     * Rows the tasks of this cursor returned since it opened or rewound, warm-up rows excluded.
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
                finishEmitting(emitTask);
                if (deferredTask != null) {
                    // the groups this thread completed came first; now the task's own rows
                    final Task task = deferredTask;
                    deferredTask = null;
                    startEmitting(task);
                    continue;
                }
            }
            if (ownerPos < ownerRows.size()) {
                // streamed row by row, as the serial window computes it
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                final AsyncWindowAtom.Slot owner = atom.getSlot(-1);
                if (hasReplay) {
                    replayOwnerKeyStart();
                }
                final boolean kept = owner.streamRow(ownerRows, ownerPos++);
                if (hasReplay) {
                    // the replay's state takes the row the function computed, see applyCarry()
                    for (int j = 0, n = replayFunctions.length; j < n; j++) {
                        if (replayFunctions[j] != null) {
                            replayFunctions[j].replayPrefixRow(owner.getFunctionInput());
                        }
                    }
                }
                if (ownerPos == ownerRows.size() && carry.length > 0) {
                    if (groupSplit) {
                        captureGroupCarry();
                    } else {
                        captureCarry(owner.getOutputRecord());
                    }
                }
                if (kept) {
                    return true;
                }
                continue;
            }
            if (isOwnerFlushPending) {
                // the streamed rows ended: a GROUP BY step outputs its last group
                isOwnerFlushPending = false;
                if (atom.getSlot(-1).flush()) {
                    return true;
                }
            }
            if (headKind == SEGMENT_ROUND) {
                final Round round = headRound;
                if (round.state == Round.STATE_IN_FLIGHT) {
                    awaitRound(round);
                    // a round finished: its slots can take the next one
                    produce();
                }
                if (emitTaskIndex < round.taskCount) {
                    startEmitting(round.tasks.getQuick(emitTaskIndex++));
                    continue;
                }
                round.release();
                headKind = SEGMENT_NONE;
                headRound = null;
                // a round is free again
                produce();
                continue;
            }
            if (headKind == SEGMENT_STREAM) {
                if (refillStream()) {
                    continue;
                }
                headKind = SEGMENT_NONE;
                isOwnerFlushPending = true;
                continue;
            }
            if (!isParallelPhase) {
                refillPrefix();
                continue;
            }
            if (!nextSegment()) {
                produce();
                if (!nextSegment()) {
                    if (groupSplit && flushOwnerGroup()) {
                        continue;
                    }
                    releaseChains();
                    return false;
                }
            }
        }
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return atom.getSlot(-1).getOutputSymbols().newSymbolTable(columnIndex);
    }

    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        // own the base cursor first: close() frees it when anything below throws
        this.baseCursor = baseCursor;
        isOpen = true;
        this.executionContext = executionContext;
        this.circuitBreaker = executionContext.getCircuitBreaker();
        this.groupSplit = splitsKeys && atom.hasGroupByStage();
        assert !groupSplit || replayChain == null : "no step goes over a fold";
        if (!rowKernelsCompiled) {
            // The steps are appended to the atom after this cursor is built, see
            // AsyncWindowRecordCursorFactory.withStage(): the workers compute the chain's rows
            // column-wise where every step allows it. Written before any round is dispatched.
            rowKernelsCompiled = true;
            atom.compileRowKernels(columnTypes, rowKernelsEnabled);
        }
        if (replayChain != null) {
            replayChain.reset();
        }
        mode = MODE_UNDECIDED;
        isWorkerSlotsOpen = false;
        resetWalkState();
        resetCounters();
        atom.resetTaskCounts();
        // the worker slots open only once a round is dispatched, see openWorkerSlots()
        atom.getSlot(-1).open(baseCursor, executionContext);
    }

    /**
     * The rest of the task being returned, straight from its chain: workers append a task's rows
     * one after another. Rows the query's thread streams itself (the prefix, streamed keys and
     * the serial mode) come through {@link #hasNext()} only.
     * <p>
     * A block exposes the chain's memory as it is, not through {@code record}. The rule that
     * keeps it correct: a task's rows must be final in memory before the task is emitted, that
     * is, before {@code startEmitting()} sets {@code emitTask}. Any combine a task's rows still
     * owe must be written in place before then, as {@code applyCarry()} does for a running
     * window's carry ({@code keySplit: running carry}). Applying it lazily, in a getter or a
     * wrapping record, would keep {@code hasNext()} correct and make every block wrong.
     */
    @Override
    public RecordBlock peekRecordBlock(int maxRows) {
        final Task emitTask = this.emitTask;
        return emitTask != null ? emitTask.chain.peekSequentialRecordBlock(maxRows) : null;
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
    public void skipRecordBlock(int rowCount) {
        assert emitTask != null : "skipRecordBlock() without a block from peekRecordBlock()";
        emitTask.chain.skipSequentialRecordBlock(rowCount);
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

    // the task chains of the parallel mode; a chain with a variable-size column offers none, nor
    // does the serial mode, both at the cost of a field read per row
    @Override
    public boolean supportsRecordBlocks() {
        return true;
    }

    @Override
    public void toTop() {
        // Ends the walk in a state the next pass can start from, also after a hasNext() that
        // threw: no round runs, and a sequence a failure left cancelled starts afresh.
        stopRounds();
        if (replayChain != null) {
            replayChain.reset();
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
            record.of(atom.getSlot(-1).getOutputRecord());
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
        final RoundAtom roundAtom = (RoundAtom) sequence.getAtom();
        final AsyncWindowAtom atom = roundAtom.atom;
        final Task task = roundAtom.round.tasks.getQuick(taskIndex);
        final int slotId = atom.acquireTaskSlot(workerId, circuitBreaker);
        try {
            final AsyncWindowAtom.Slot slot = atom.getSlot(slotId);
            // a task starts from clean state: warm-up rows rebuild a key it continues
            slot.toTop();
            task.lastOffset = slot.compute(task.rows, task.keyStarts, task.emitFrom, task.chain, circuitBreaker, sequence, task.groupSplit);
            task.computedByWorker = workerId > -1;
            slot.countTask();
            atom.countTask(workerId);
        } finally {
            atom.release(slotId);
        }
        // A task stops early only once its round is cancelled, which stays so: an active round
        // after the task means it computed all its rows. The walk-order pass may go on from it.
        final ReplayChain replayChain = roundAtom.replayChain;
        if (replayChain != null && sequence.isActive()) {
            replayChain.computed(task, workerId > -1);
        }
    }

    // Combines the rows of the key the task continues with the key's running values at the end of
    // the previous rows returned, in place, before they are returned. In place, and before the task
    // is emitted: peekRecordBlock() exposes the chain's memory, so the rows must be final there.
    // Folded and replayed columns are not combined: the ReplayChain computed them.
    private void applyCarry(Task task) {
        assert task.continuesKey;
        final RecordChain chain = task.chain;
        final int n = carry.length;
        final long rowCount = task.firstKeyRows;
        long offset = 0;
        for (long r = 0; r < rowCount; r++) {
            for (int j = 0; j < n; j++) {
                final int type = splitPlan.getPrefixType(j);
                final int op = splitPlan.getPrefixOp(j);
                if (AsyncWindowSplitPlan.isFold(op)) {
                    continue;
                }
                final long address = chain.getAddress(offset, splitPlan.getPrefixColumn(j));
                if (ColumnType.tagOf(type) == ColumnType.INT) {
                    Unsafe.putInt(address, (int) AsyncWindowSplitPlan.combine(op, type, carry[j], Unsafe.getInt(address)));
                } else {
                    Unsafe.putLong(address, AsyncWindowSplitPlan.combine(op, type, carry[j], Unsafe.getLong(address)));
                }
            }
            offset = chain.getNextRecordOffset(offset);
        }
    }

    // OP_REPLAY: before the owner streams the row at ownerPos, starts the replays afresh when a
    // key starts there.
    private void replayOwnerKeyStart() {
        if (ownerKeyStartIndex < ownerKeyStarts.size() && ownerKeyStarts.getQuick(ownerKeyStartIndex) == ownerPos) {
            // equal starts are keys without rows
            do {
                ownerKeyStartIndex++;
            } while (ownerKeyStartIndex < ownerKeyStarts.size() && ownerKeyStarts.getQuick(ownerKeyStartIndex) == ownerPos);
            for (int j = 0, n = replayFunctions.length; j < n; j++) {
                if (replayFunctions[j] != null) {
                    replayFunctions[j].replayKeyStart();
                }
            }
        }
    }

    // Waits for a round the workers compute, taking queued tasks meanwhile.
    private void awaitRound(Round round) {
        // Marked first: whether awaitRound() returns or throws, no task of the round runs after.
        round.state = Round.STATE_READY;
        round.sequence.awaitRound();
    }

    private void captureCarry(Record source) {
        for (int j = 0, n = carry.length; j < n; j++) {
            final int column = splitPlan.getPrefixColumn(j);
            carry[j] = switch (ColumnType.tagOf(splitPlan.getPrefixType(j))) {
                case ColumnType.DOUBLE -> Double.doubleToRawLongBits(source.getDouble(column));
                case ColumnType.INT -> source.getInt(column);
                default -> source.getLong(column);
            };
        }
    }

    private void captureCarry(Task task) {
        final RecordChain chain = task.chain;
        for (int j = 0, n = carry.length; j < n; j++) {
            final long address = chain.getAddress(task.lastOffset, splitPlan.getPrefixColumn(j));
            carry[j] = ColumnType.tagOf(splitPlan.getPrefixType(j)) == ColumnType.INT
                    ? Unsafe.getInt(address)
                    : Unsafe.getLong(address);
        }
    }

    private void chooseMode() {
        mode = MODE_SERIAL;
        if (baseCursor instanceof KeyMajorPageFrameRecordCursor keyMajorCursor) {
            keyMajorCursor.prepareFrames();
            if (keyMajorCursor.hasOnlyPlainNativeFrames() && isWithinExactRowLimit(keyMajorCursor)) {
                this.keyMajorCursor = keyMajorCursor;
                mode = MODE_PARALLEL;
                final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
                ownerRows = newRowIds(memoryTracker);
                if (splitPlan.getMode() == AsyncWindowSplitPlan.MODE_WARMUP) {
                    warmRows = newRowIds(memoryTracker);
                }
                atom.getSlot(-1).ofFrames(keyMajorCursor.getFrameAddressCache());
                record.of(atom.getSlot(-1).getOutputRecord());
                return;
            }
        }
        record.of(atom.getSlot(-1).getOutputRecord());
        atom.getSlot(-1).ofSerial(baseCursor.getRecord());
        isOwnerFlushPending = true;
    }

    // Whether the walk's frames hold no more rows than the plan's carried sums stay exact over,
    // see AsyncWindowSplitPlan.getExactRowLimit(): every key's rows are among them. A walk over
    // more is run serially, whose sums are the serial plan's.
    private boolean isWithinExactRowLimit(KeyMajorPageFrameRecordCursor keyMajorCursor) {
        final long limit = splitPlan.getExactRowLimit();
        if (limit == Long.MAX_VALUE) {
            return true;
        }
        final PageFrameAddressCache frames = keyMajorCursor.getFrameAddressCache();
        long rows = 0;
        for (int i = 0, n = frames.getFrameCount(); i < n; i++) {
            rows += frames.getFrameSize(i);
            if (rows > limit) {
                return false;
            }
        }
        return true;
    }

    // Collects the next round's tasks from the walk and dispatches them, then queues the round.
    // Without key splitting, a round stops at a key too large for a task: it is queued after the
    // round, for this thread to stream.
    private void collectRound(Round round) {
        round.clear();
        long collectedRows = 0;
        // warm-up rows included
        long heldRows = 0;
        int streamKey = -1;
        while (collectedRows < roundRows && round.taskCount < tasksPerRound && !keyMajorCursor.isWalkExhausted()) {
            final Task task = round.nextTask(this);
            final long rampLimit = Math.min(taskRows, rampTaskRows << Math.min(30, tasksCollected++ / workerCount));
            final long taskLimit = Math.min(rampLimit, roundRows - collectedRows);
            final long emitted = splitsKeys ? collectSlice(task, taskLimit) : collectWholeKeys(task, taskLimit);
            if (emitted == 0) {
                round.dropLastTask();
            } else {
                round.taskRowCounts.add(task.rows.size());
                heldRows += task.rows.size();
                collectedRows += emitted;
                task.emittedRows = emitted;
                if (replayChain != null) {
                    replayChain.enqueue(task, carry);
                }
            }
            if (task.largeKeyIndex > -1) {
                streamKey = task.largeKeyIndex;
                break;
            }
        }
        if (round.taskCount > 0) {
            dispatch(round, heldRows);
            enqueue(SEGMENT_ROUND, round, -1);
        }
        if (streamKey > -1) {
            enqueue(SEGMENT_STREAM, null, streamKey);
        }
    }

    // Splitting keys: the next taskLimit rows of the walk, whatever keys they belong to, after the
    // warm-up rows of a key it continues. Returns the rows collected, warm-up excluded.
    private long collectSlice(Task task, long taskLimit) {
        final DirectLongList rows = task.rows;
        task.continuesKey = walkKeyOpen;
        if (task.groupSplit != null) {
            task.groupSplit.continuesKey = walkKeyOpen;
            task.groupSplit.walkBase = walkPosition;
        }
        if (walkKeyOpen && warmRows != null) {
            for (long i = 0, n = warmRows.size(); i < n; i++) {
                rows.add(warmRows.get(i));
            }
        }
        task.emitFrom = rows.size();
        long emitted = 0;
        boolean first = true;
        while (emitted < taskLimit) {
            // a key the task continues has its warm-up rows ahead of its new ones
            final long keyStart = first && walkKeyOpen ? 0 : rows.size();
            task.keyStarts.add(keyStart);
            final long keyLo = rows.size();
            final int status = keyMajorCursor.collectKeyRows(rows, taskLimit - emitted);
            final long collected = rows.size() - keyLo;
            emitted += collected;
            if (first) {
                task.firstKeyRows = collected;
                first = false;
                if (task.groupSplit != null) {
                    // a walk that stopped at a key's last row continues it with no row
                    task.groupSplit.continuesKey = task.continuesKey && collected > 0;
                }
            }
            if (status == KeyMajorPageFrameRecordCursor.COLLECT_ROW_LIMIT) {
                walkKeyOpen = true;
                updateWarmRows(rows, keyStart, false);
                break;
            }
            walkKeyOpen = false;
            if (status == KeyMajorPageFrameRecordCursor.COLLECT_EXHAUSTED) {
                break;
            }
        }
        walkPosition += emitted;
        if (task.groupSplit != null) {
            task.groupSplit.lastKeyContinues = walkKeyOpen;
        }
        return emitted;
    }

    // Whole keys only: keys until the task holds taskLimit rows; a key above max.key.rows is
    // skipped by the walk and marked for this thread to stream. Returns the rows collected.
    private long collectWholeKeys(Task task, long taskLimit) {
        final DirectLongList rows = task.rows;
        task.emitFrom = 0;
        task.continuesKey = false;
        while (rows.size() < taskLimit) {
            final long keyLo = rows.size();
            final int keyIndex = keyMajorCursor.getKeyIndex();
            final int status = keyMajorCursor.collectKeyRows(rows, maxKeyRows);
            if (status == KeyMajorPageFrameRecordCursor.COLLECT_KEY_END) {
                task.keyStarts.add(keyLo);
                continue;
            }
            if (status == KeyMajorPageFrameRecordCursor.COLLECT_ROW_LIMIT) {
                // too large for a task: the walk goes on past it, this thread walks it later
                rows.setPos(keyLo);
                // the list grew for the key's rows, which it no longer holds
                shrinkIfOversized(rows, taskRows);
                keyMajorCursor.skipKey();
                task.largeKeyIndex = keyIndex;
            }
            // a large key, or no key left
            break;
        }
        return rows.size();
    }

    private void dispatch(Round round, long heldRows) {
        if (!isWorkerSlotsOpen) {
            openWorkerSlots();
        }
        if (!round.isSequenceOpen) {
            try {
                round.sequence.ofRounds(baseCursor, executionContext);
            } catch (SqlException e) {
                throw CairoException.nonCritical().put(e.getFlyweightMessage());
            }
            round.isSequenceOpen = true;
        }
        round.state = Round.STATE_IN_FLIGHT;
        round.sequence.dispatchRound(REDUCER, round.taskRowCounts);
        parallelRoundCount++;
        parallelTaskCount += round.taskCount;
        maxRoundRows = Math.max(maxRoundRows, heldRows);
    }

    private void enqueue(int kind, @Nullable Round round, int key) {
        final int tail = (segmentHead + segmentCount) % segmentKinds.length;
        segmentKinds[tail] = kind;
        segmentRounds[tail] = round;
        segmentKeys[tail] = key;
        segmentCount++;
    }

    private void finishEmitting(Task task) {
        if (task == ownerGroupTask) {
            task.chain.clear();
            emitTask = null;
            return;
        }
        // with a GROUP BY step the carry is the open group's, see processGroupBoundary()
        if (carry.length > 0 && task.emittedRows > 0 && !groupSplit) {
            captureCarry(task);
        }
        // Returned. A chain a worker filled keeps its memory for the task's next fill, which
        // would otherwise fault every page of it in afresh. Workers fill tasks ahead of the rows
        // being returned, so their rounds' chains are full at once anyway: keeping their memory
        // does not raise that peak, and it is capped at the output of max.rounds full rounds.
        // A chain the query's own thread filled, as it does when the pool is busy, is freed: only
        // the head round's chains are full at a time then, and keeping every round's memory
        // would hold max.rounds times that. A chain that grew well past a task's usual size, for
        // a large key, is freed too; the next fill sizes it in one allocation from its row count.
        final long stride = task.chain.getFixedRecordStride();
        final long keepLimit = Math.min(2 * taskRows * stride, rounds.length * roundRows * stride - keptChainBytes);
        if (task.computedByWorker && keepLimit > 0) {
            task.keptChainBytes = task.chain.clearKeepingMemory(keepLimit);
            keptChainBytes += task.keptChainBytes;
            if (task.keptChainBytes > 0) {
                keptChainCount++;
            }
        } else {
            task.chain.clear();
        }
        taskRowsComputed += task.emittedRows;
        emitTask = null;
    }

    private Round freeRound() {
        for (Round round : rounds) {
            if (round.state == Round.STATE_FREE) {
                return round;
            }
        }
        return null;
    }

    private boolean hasNextSerial() {
        final AsyncWindowAtom.Slot owner = atom.getSlot(-1);
        while (true) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            if (!baseCursor.hasNext()) {
                if (isOwnerFlushPending) {
                    isOwnerFlushPending = false;
                    return owner.flush();
                }
                return false;
            }
            if (owner.computeNext(owner.getSerialInput())) {
                return true;
            }
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
            final Task task = new Task(chain, rows);
            if (groupSplit) {
                try {
                    final AsyncWindowGroupByStage stage = atom.getSlot(-1).getGroupStage();
                    final RecordChain headChain = new RecordChain(
                            stage.getHeadTypes(),
                            stage.getHeadSink(),
                            chainPageSize,
                            (int) Math.min(chainMaxPages, Integer.MAX_VALUE),
                            PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES.getPropertyPath()
                    );
                    headChain.setMemoryTracker(memoryTracker);
                    task.groupSplit = new AsyncWindowAtom.GroupSplit(headChain, stage.getValueCount(), stage.getKeyCount());
                } catch (Throwable th) {
                    Misc.free(task);
                    throw th;
                }
            }
            return task;
        } catch (Throwable th) {
            Misc.free(chain);
            Misc.free(rows);
            throw th;
        }
    }

    // Takes the next queued segment as the head. Returns false when none is queued.
    private boolean nextSegment() {
        if (segmentCount == 0) {
            return false;
        }
        headKind = segmentKinds[segmentHead];
        headRound = segmentRounds[segmentHead];
        headKey = segmentKeys[segmentHead];
        segmentRounds[segmentHead] = null;
        segmentHead = (segmentHead + 1) % segmentKinds.length;
        segmentCount--;
        emitTaskIndex = 0;
        if (headKind == SEGMENT_STREAM) {
            // A key of its own: no task can take this thread's slot meanwhile, so its state
            // carries from chunk to chunk.
            atom.getSlot(-1).toTop();
            headStreamPos = 0;
            // the workers compute the keys after it while it streams
            produce();
            for (Round round : rounds) {
                if (round.state != Round.STATE_FREE) {
                    roundsAheadOfStreams++;
                }
            }
        }
        return true;
    }

    private void openWorkerSlots() {
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

    // Dispatches rounds while a round is free, the walk has rows left and the queue has room.
    private void produce() {
        if (!isParallelPhase) {
            return;
        }
        while (segmentCount <= segmentKinds.length - 2 && !keyMajorCursor.isWalkExhausted()) {
            final Round round = freeRound();
            if (round == null) {
                return;
            }
            collectRound(round);
        }
    }

    // The prefix's next chunk of rows, for this thread to stream. Ends the prefix, at a key
    // boundary unless keys split, once min.rows rows have been streamed.
    private void refillPrefix() {
        ownerRows.clear();
        ownerKeyStarts.clear();
        ownerKeyStartIndex = 0;
        ownerPos = 0;
        final long chunkRows = nextChunkRows;
        nextChunkRows = Math.min(taskRows, 2 * chunkRows);
        boolean first = true;
        while (ownerRows.size() < chunkRows) {
            if (prefixRowsStreamed + ownerRows.size() >= minRows && (splitsKeys || !walkKeyOpen)) {
                isParallelPhase = true;
                // a group the prefix leaves open mid-key is the next task's to continue
                isOwnerFlushPending = !(groupSplit && walkKeyOpen);
                break;
            }
            final boolean continued = first && walkKeyOpen;
            first = false;
            final long keyLo = ownerRows.size();
            if (!continued) {
                ownerKeyStarts.add(keyLo);
            }
            final int status = keyMajorCursor.collectKeyRows(ownerRows, chunkRows - ownerRows.size());
            if (status == KeyMajorPageFrameRecordCursor.COLLECT_ROW_LIMIT) {
                walkKeyOpen = true;
                updateWarmRows(ownerRows, keyLo, continued);
                break;
            }
            walkKeyOpen = false;
            if (status == KeyMajorPageFrameRecordCursor.COLLECT_EXHAUSTED) {
                // nothing left for the workers at all
                isParallelPhase = true;
                isOwnerFlushPending = true;
                break;
            }
        }
        prefixRowsStreamed += ownerRows.size();
        walkPosition += ownerRows.size();
        atom.getSlot(-1).resetStream();
        // the record may point at the last task returned before a rewind
        record.of(atom.getSlot(-1).getOutputRecord());
    }

    // The streamed key's next chunk, walked apart from the walk, whole frames at a time. Returns
    // false once the key is done.
    private boolean refillStream() {
        if (headStreamPos < 0) {
            return false;
        }
        shrinkIfOversized(ownerRows, taskRows);
        ownerRows.clear();
        ownerKeyStarts.clear();
        ownerKeyStartIndex = 0;
        ownerPos = 0;
        headStreamPos = keyMajorCursor.collectKeyFrames(headKey, headStreamPos, ownerRows, taskRows);
        largeKeyRowsStreamed += ownerRows.size();
        final AsyncWindowAtom.Slot owner = atom.getSlot(-1);
        owner.resetStream();
        record.of(owner.getOutputRecord());
        // keep the workers busy with the keys after this one
        produce();
        return ownerRows.size() > 0 || headStreamPos > -1;
    }

    // Every row has been returned: no chain holds a row to return, or is filled again before a
    // rewind. Frees them all, also the chain of a task the walk's end left without rows.
    private void releaseChains() {
        for (Round round : rounds) {
            for (int i = 0, n = round.tasks.size(); i < n; i++) {
                final Task task = round.tasks.getQuick(i);
                task.keptChainBytes = 0;
                task.chain.clear();
            }
        }
        keptChainBytes = 0;
    }

    private void resetCounters() {
        maxRoundRows = 0;
        keptChainCount = 0;
        largeKeyRowsStreamed = 0;
        parallelRoundCount = 0;
        parallelTaskCount = 0;
        prefixRowsStreamed = 0;
        roundsAheadOfStreams = 0;
        taskRowsComputed = 0;
    }

    private void resetWalkState() {
        emitTask = null;
        deferredTask = null;
        walkPosition = 0;
        if (ownerGroupTask != null) {
            ownerGroupTask.chain.clear();
        }
        emitTaskIndex = 0;
        headKind = SEGMENT_NONE;
        headRound = null;
        headStreamPos = -1;
        for (int i = 0, n = segmentRounds.length; i < n; i++) {
            segmentRounds[i] = null;
        }
        segmentHead = 0;
        segmentCount = 0;
        isParallelPhase = minRows <= 0;
        tasksCollected = 0;
        // the serial mode streams every row, the parallel one its prefix
        isOwnerFlushPending = mode == MODE_SERIAL;
        walkKeyOpen = false;
        ownerPos = 0;
        nextChunkRows = Math.min(FIRST_CHUNK_ROWS, taskRows);
        if (ownerRows != null) {
            ownerRows.clear();
        }
        ownerKeyStarts.clear();
        ownerKeyStartIndex = 0;
        if (warmRows != null) {
            warmRows.clear();
        }
        for (Round round : rounds) {
            round.clear();
        }
    }

    // Gives a row id list's memory back once it has grown well past what a task or a chunk needs,
    // keeping the rows it holds.
    private static void shrinkIfOversized(DirectLongList rows, long taskRows) {
        if (rows.getCapacity() > 2 * Math.max(taskRows, ROW_IDS_INITIAL_CAPACITY)) {
            rows.setCapacity(Math.max(rows.size(), ROW_IDS_INITIAL_CAPACITY));
        }
    }

    private void startEmitting(Task task) {
        // Once per task of about task.rows rows, a coarse site: cancellation and the timeout are
        // checked on every call, unlike the per-row check, which samples them.
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        if (groupSplit) {
            if (!task.isBoundaryProcessed) {
                task.isBoundaryProcessed = true;
                if (processGroupBoundary(task)) {
                    // the groups this thread completed go first, then the task's rows
                    deferredTask = task;
                    beginEmitting(ownerGroupTask);
                    return;
                }
            }
        } else {
            if (replayChain != null) {
                // the folded and replayed columns, computed in walk order by whichever thread got there
                replayChain.awaitPassed(task);
            }
            if (hasCarryOp && task.continuesKey) {
                applyCarry(task);
            }
        }
        beginEmitting(task);
    }

    private void beginEmitting(Task task) {
        task.chain.toTop();
        record.of(task.chain.getRecord());
        emitTask = task;
    }

    // Combines the carried running value with a column of the first rowCount rows of a chain, in
    // place, as applyCarry() does for the output's rows.
    private void applyCarry(RecordChain chain, long rowCount, int column) {
        final int type = splitPlan.getPrefixType(0);
        final int op = splitPlan.getPrefixOp(0);
        long offset = 0;
        for (long r = 0; r < rowCount; r++) {
            final long address = chain.getAddress(offset, column);
            Unsafe.putLong(address, AsyncWindowSplitPlan.combine(op, type, carry[0], Unsafe.getLong(address)));
            offset = chain.getNextRecordOffset(offset);
        }
    }

    // The walk ended with a group open, of a key that had no rows left past the task that left it
    // open: outputs it. Returns whether there was one.
    private boolean flushOwnerGroup() {
        final AsyncWindowGroupByStage stage = atom.getSlot(-1).getGroupStage();
        if (!stage.closeOpenGroup()) {
            return false;
        }
        if (ownerGroupTask == null) {
            ownerGroupTask = newTask();
        }
        ownerGroupTask.chain.clear();
        ownerGroupTask.chain.put(stage.getOutputRecord(), -1);
        beginEmitting(ownerGroupTask);
        return true;
    }

    // The carry of a key the query's thread left open mid-key: its open group's key.
    private void captureGroupCarry() {
        final AsyncWindowGroupByStage stage = atom.getSlot(-1).getGroupStage();
        if (stage.isGroupOpen()) {
            carry[0] = stage.getOpenKey(splitPlan.getGroupCarryKeyIndex());
        }
    }

    /**
     * Completes, in walk order, the groups a task shares with the tasks around it, see
     * {@link AsyncWindowAtom.GroupSplit}, on the GROUP BY step of this thread's slot, which holds
     * the group the walk left open so far. When the task continues a key, the carry goes into its
     * rows of that key, and its captured head rows are replayed into the open group: a head row
     * with another group key closes it. The head group is closed when the task ended it. When the
     * task's last key goes on, its tail group becomes the open one, and its key the carry. The
     * groups closed here go to {@link #ownerGroupTask}; returns whether there are any.
     */
    private boolean processGroupBoundary(Task task) {
        final AsyncWindowGroupByStage stage = atom.getSlot(-1).getGroupStage();
        final AsyncWindowAtom.GroupSplit gs = task.groupSplit;
        if (ownerGroupTask == null) {
            ownerGroupTask = newTask();
        }
        final RecordChain groups = ownerGroupTask.chain;
        groups.clear();
        long prevOffset = -1;
        final boolean carries = carry.length > 0;
        if (!gs.continuesKey && stage.closeOpenGroup()) {
            // the key the walk stopped in had no rows left: its open group is complete
            prevOffset = groups.put(stage.getOutputRecord(), prevOffset);
        }
        if (gs.continuesKey) {
            if (carries) {
                applyCarry(task.chain, gs.firstKeyGroupRows, splitPlan.getPrefixColumn(0));
                applyCarry(gs.headChain, gs.headRows, splitPlan.getGroupCarryInputColumn());
            }
            final RecordChain head = gs.headChain;
            head.toTop();
            long rowId = gs.walkBase;
            while (head.hasNext()) {
                if (stage.replay(head.getRecord(), rowId++)) {
                    prevOffset = groups.put(stage.getOutputRecord(), prevOffset);
                }
            }
            if (gs.headClosed && stage.closeOpenGroup()) {
                prevOffset = groups.put(stage.getOutputRecord(), prevOffset);
            }
        }
        if (gs.lastKeyContinues) {
            if (gs.hasTail) {
                assert !stage.isGroupOpen() : "the task's head group is still open with a tail behind it";
                if (carries && gs.continuesKey && gs.tailIsFirstKey) {
                    final int k = splitPlan.getGroupCarryKeyIndex();
                    gs.tailKeys[k] = AsyncWindowSplitPlan.combine(splitPlan.getPrefixOp(0), splitPlan.getPrefixType(0), carry[0], gs.tailKeys[k]);
                }
                stage.adoptGroup(gs.tailValue, gs.tailKeys, gs.tailLastRowId);
            }
            if (carries) {
                carry[0] = stage.getOpenKey(splitPlan.getGroupCarryKeyIndex());
            }
        }
        return prevOffset != -1;
    }

    // Waits for every round the workers still compute and resets the sequences: a cursor that
    // closes or rewinds must not free or reuse what their tasks still write to. The rounds are
    // cancelled first, which the tasks notice between batches, so the wait is short.
    private void stopRounds() {
        for (Round round : rounds) {
            if (round.state == Round.STATE_IN_FLIGHT) {
                round.state = Round.STATE_READY;
                round.sequence.cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                try {
                    round.sequence.awaitRound();
                } catch (Throwable ignore) {
                    // the cancellation asked for here, or an error no one reads now; either way no
                    // task of the round runs any more
                }
            }
        }
        Throwable failure = null;
        for (Round round : rounds) {
            if (round.isSequenceOpen) {
                round.isSequenceOpen = false;
                try {
                    round.sequence.reset();
                } catch (Throwable th) {
                    failure = addFailure(failure, th);
                }
            }
            round.state = Round.STATE_FREE;
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    // MODE_WARMUP: keeps the last warm-up rows of the key the walk stopped in, from the rows of
    // the list from keyStart on, and, when the key started before the list, from the rows kept
    // before.
    private void updateWarmRows(DirectLongList rows, long keyStart, boolean continued) {
        final DirectLongList warm = warmRows;
        if (warm == null) {
            return;
        }
        final long n = splitPlan.getWarmupRows();
        final long hi = rows.size();
        final long k = hi - keyStart;
        if (k >= n || !continued) {
            warm.clear();
            for (long i = Math.max(keyStart, hi - n); i < hi; i++) {
                warm.add(rows.get(i));
            }
            return;
        }
        // keep the last n - k rows kept so far, then append the k new ones
        final long keep = Math.min(warm.size(), n - k);
        final long from = warm.size() - keep;
        for (long i = 0; i < keep; i++) {
            warm.set(i, warm.get(from + i));
        }
        warm.setPos(keep);
        for (long i = keyStart; i < hi; i++) {
            warm.add(rows.get(i));
        }
    }

    /**
     * The state a round's sequence hands its tasks: the round, and the atom the cursor shares.
     */
    static class RoundAtom implements StatefulAtom {
        final AsyncWindowAtom atom;
        // the cursor's walk-order pass over folded and replayed columns, or null
        ReplayChain replayChain;
        Round round;
        // the round of an AsyncWindowShardCursor, when the factory hashes keys into shards
        AsyncWindowShardCursor.ShardRound shardRound;

        RoundAtom(AsyncWindowAtom atom) {
            this.atom = atom;
        }
    }

    /**
     * The tasks of one dispatch to the workers, and the sequence that dispatches them.
     */
    static class Round implements Mutable, QuietCloseable {
        static final int STATE_FREE = 0;
        static final int STATE_IN_FLIGHT = 1;
        static final int STATE_READY = 2;
        private final UnorderedPageFrameSequence<RoundAtom> sequence;
        private final LongList taskRowCounts = new LongList();
        private final ObjList<Task> tasks = new ObjList<>();
        private boolean isSequenceOpen;
        private int state = STATE_FREE;
        private int taskCount;

        Round(UnorderedPageFrameSequence<RoundAtom> sequence) {
            this.sequence = sequence;
        }

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
            // the memory the chain kept is the fill's now
            cursor.keptChainBytes -= task.keptChainBytes;
            task.keptChainBytes = 0;
            task.reuse(cursor.taskRows);
            return task;
        }

        void release() {
            state = STATE_FREE;
            clear();
        }
    }

    /**
     * The pass over the folded and replayed columns of every task's rows (see
     * {@link AsyncWindowSplitPlan#OP_FOLD}, {@link AsyncWindowSplitPlan#OP_REPLAY}): the serial
     * function's arithmetic, row after row in walk order, over the arguments the workers output.
     * Its state at a task's first row is the one the rows before it in the walk left, the key's
     * own when the task continues it, so it must go over the tasks one after another, in walk
     * order.
     * <p>
     * It does, on whichever thread first finds the next task in walk order computed: the worker
     * that computed it, after its task, or the one that computed the task before it, or the
     * query's thread, which waits for a task's pass before it returns the task's rows. A worker
     * never waits for the pass: it takes the lock or leaves the pass to the thread that holds it.
     * That thread looks again once it has let go, so a task computed meanwhile is not left
     * behind. The pass thus runs alongside the workers' computing and the query thread's
     * returning of rows, where it once took the query's thread before every task.
     * <p>
     * Every thread that goes over a task holds the lock, which orders its writes to the
     * functions' state before the next holder's reads. The query's thread enqueues a task, and
     * writes the replayed functions' state from the rows it streams itself, before it dispatches
     * the task's round. A cursor that rewinds or closes waits for every round first, see
     * stopRounds(), and only then {@link #reset()}s the pass.
     */
    static final class ReplayChain {
        private final int[] columns;
        private final boolean[] foldCounted;
        private final double[] foldSums;
        // the replayed functions by entry, null for a folded one
        private final ReplayableWindowFunction[] functions;
        private final AtomicInteger lock = new AtomicInteger();
        private final int[] ops;
        // each entry's index among the plan's prefix columns, as the carry holds them
        private final int[] prefixIndexes;
        // the tasks from head to tail, by their place in walk order modulo the length
        private final Task[] ring;
        // the next task to go over, written under the lock
        private volatile long head;
        // the next task's place in walk order, written by the query's thread
        private volatile long tail;
        // tasks a worker thread went over, under the lock, since the last reset
        private long workerPassCount;

        ReplayChain(AsyncWindowSplitPlan splitPlan, ReplayableWindowFunction[] replayFunctions, int taskCapacity) {
            int count = 0;
            for (int j = 0, n = splitPlan.getPrefixCount(); j < n; j++) {
                if (AsyncWindowSplitPlan.isFold(splitPlan.getPrefixOp(j))) {
                    count++;
                }
            }
            this.columns = new int[count];
            this.ops = new int[count];
            this.prefixIndexes = new int[count];
            this.functions = new ReplayableWindowFunction[count];
            this.foldSums = new double[count];
            this.foldCounted = new boolean[count];
            for (int j = 0, k = 0, n = splitPlan.getPrefixCount(); j < n; j++) {
                final int op = splitPlan.getPrefixOp(j);
                if (AsyncWindowSplitPlan.isFold(op)) {
                    columns[k] = splitPlan.getPrefixColumn(j);
                    ops[k] = op;
                    prefixIndexes[k] = j;
                    functions[k] = replayFunctions[j];
                    assert (op == AsyncWindowSplitPlan.OP_REPLAY) == (functions[k] != null);
                    k++;
                }
            }
            this.ring = new Task[Math.max(1, taskCapacity)];
        }

        /**
         * Waits until the pass has gone over the task, going over it on this thread when no
         * other does: the query's thread, before it returns the task's rows, once the task's
         * round, and so every task before it in walk order, is computed.
         */
        void awaitPassed(Task task) {
            while (!task.passDone) {
                if (lock.compareAndSet(0, 1)) {
                    try {
                        passComputed();
                    } finally {
                        lock.set(0);
                    }
                    if (!task.passDone) {
                        // Every task up to this one has been computed, unless one stopped early
                        // in a cancelled round, whose rows no one returns.
                        throw CairoException.nonCritical().put("parallel window task was not computed [walkPosition=").put(head).put(']');
                    }
                } else {
                    Os.pause();
                }
            }
        }

        /**
         * A worker computed all of the task's rows: goes over it, and the tasks after it that
         * are computed, unless another thread is at it.
         */
        void computed(Task task, boolean workerThread) {
            task.passComputed = true;
            while (lock.compareAndSet(0, 1)) {
                try {
                    final long passed = passComputed();
                    if (workerThread) {
                        workerPassCount += passed;
                    }
                } finally {
                    lock.set(0);
                }
                // a task computed while the lock was held may have found it taken
                final long h = head;
                if (h >= tail || !ring[(int) (h % ring.length)].passComputed) {
                    return;
                }
            }
        }

        /**
         * Appends a task to the walk, on the query's thread, before its round is dispatched. The
         * first task after the rows the query's thread streamed itself starts the fold from the
         * carry those rows left, when it continues their last key.
         */
        void enqueue(Task task, long[] carry) {
            final long t = tail;
            assert t - head < ring.length : "more tasks alive than the walk holds";
            if (t == 0 && task.continuesKey) {
                for (int k = 0, n = ops.length; k < n; k++) {
                    if (ops[k] == AsyncWindowSplitPlan.OP_FOLD) {
                        final double before = Double.longBitsToDouble(carry[prefixIndexes[k]]);
                        foldSums[k] = Double.isNaN(before) ? 0.0 : before;
                        foldCounted[k] = !Double.isNaN(before);
                    }
                }
            }
            task.passSeq = t;
            task.passComputed = false;
            task.passDone = false;
            ring[(int) (t % ring.length)] = task;
            tail = t + 1;
        }

        /**
         * Forgets every task, once no task of the walk runs any more.
         */
        void reset() {
            head = 0;
            tail = 0;
            workerPassCount = 0;
            Arrays.fill(ring, null);
            Arrays.fill(foldSums, 0.0);
            Arrays.fill(foldCounted, false);
        }

        // Under the lock: goes over the computed tasks from the head on, in walk order. Returns
        // how many.
        private long passComputed() {
            final long h0 = head;
            long h = h0;
            while (h < tail) {
                final Task task = ring[(int) (h % ring.length)];
                if (!task.passComputed) {
                    break;
                }
                assert task.passSeq == h;
                pass(task);
                task.passDone = true;
                head = ++h;
            }
            return h - h0;
        }

        // The serial functions' arithmetic over the task's rows, in walk order, in place: a
        // replayed frame through its function, a folded sum as SumOverUnboundedRowsFrameFunction
        // adds, and its partitioned twin. Each starts afresh at a key the task starts, and goes
        // on from the state the walk's rows before left at a key the task continues.
        private void pass(Task task) {
            final RecordChain chain = task.chain;
            final int n = ops.length;
            final long rowCount = task.emittedRows;
            final LongList keyStarts = task.keyStarts;
            // a continued key's start is among the warm-up rows, or at the task's first row when
            // there are none: either way not a start here
            int keyStartIndex = task.continuesKey ? 1 : 0;
            long nextKeyStart = keyStartIndex < keyStarts.size() ? keyStarts.getQuick(keyStartIndex) - task.emitFrom : Long.MAX_VALUE;
            long offset = 0;
            for (long r = 0; r < rowCount; r++) {
                if (r == nextKeyStart) {
                    // equal starts are keys without rows
                    do {
                        keyStartIndex++;
                        nextKeyStart = keyStartIndex < keyStarts.size() ? keyStarts.getQuick(keyStartIndex) - task.emitFrom : Long.MAX_VALUE;
                    } while (nextKeyStart == r);
                    for (int k = 0; k < n; k++) {
                        if (functions[k] != null) {
                            functions[k].replayKeyStart();
                        } else {
                            // a key the task starts: its sum starts from scratch, as serially
                            foldSums[k] = 0.0;
                            foldCounted[k] = false;
                        }
                    }
                }
                for (int k = 0; k < n; k++) {
                    final long address = chain.getAddress(offset, columns[k]);
                    final ReplayableWindowFunction function = functions[k];
                    if (function != null) {
                        // the worker output the row's argument: the function computes the row from it
                        function.replayNext(Unsafe.getDouble(address));
                        Unsafe.putDouble(address, function.getReplayedValue());
                    } else {
                        final double value = Unsafe.getDouble(address);
                        if (Numbers.isFinite(value)) {
                            foldSums[k] += value;
                            foldCounted[k] = true;
                        }
                        Unsafe.putDouble(address, foldCounted[k] ? foldSums[k] : Double.NaN);
                    }
                }
                offset = chain.getNextRecordOffset(offset);
            }
        }
    }

    /**
     * Row ids of the walk and, once computed, their output rows.
     */
    static class Task implements QuietCloseable {
        private final RecordChain chain;
        // indexes of rows at which a key starts, ascending, 0 first; the rows between two of them
        // are one key's, see AsyncWindowAtom.Slot.computeKeyRuns()
        private final LongList keyStarts = new LongList();
        private final DirectLongList rows;
        // a worker thread computed the task, not the query's thread; written by the computing thread
        private boolean computedByWorker;
        // with keys split over tasks and a GROUP BY step, what the task hands over, else null
        private AsyncWindowAtom.GroupSplit groupSplit;
        // the groups it shares with the tasks before it were completed, see processGroupBoundary()
        private boolean isBoundaryProcessed;
        // the first key of the task continues from the rows returned before it
        private boolean continuesKey;
        // rows before this index only rebuild a continued key's state
        private long emitFrom;
        private long emittedRows;
        // rows of the first key, warm-up excluded
        private long firstKeyRows;
        // memory the chain kept after the task's rows were returned, see finishEmitting()
        private long keptChainBytes;
        // a key the walk skipped after this task's rows, for the query's thread to stream
        private int largeKeyIndex = -1;
        // offset of the last record in the chain, written by the task's worker
        private long lastOffset = -1;
        // ReplayChain: the task's place in walk order, whether its rows are all computed, and
        // whether the pass went over them
        private volatile boolean passComputed;
        private volatile boolean passDone;
        private long passSeq = -1;

        private Task(RecordChain chain, DirectLongList rows) {
            this.chain = chain;
            this.rows = rows;
        }

        @Override
        public void close() {
            Misc.free(chain);
            Misc.free(rows);
            Misc.free(groupSplit);
        }

        // Empties the task for its next round. A task usually holds about taskRows row ids, but
        // one that grew for a large key would keep that key's memory for good, also after the
        // key's rows moved out to the query's thread: give it back when the list's capacity, not
        // its last size, is well above the usual.
        private void reuse(long taskRows) {
            shrinkIfOversized(rows, taskRows);
            rows.clear();
            keyStarts.clear();
            computedByWorker = false;
            isBoundaryProcessed = false;
            if (groupSplit != null) {
                groupSplit.reset();
            }
            continuesKey = false;
            emitFrom = 0;
            emittedRows = 0;
            firstKeyRows = 0;
            largeKeyIndex = -1;
            lastOffset = -1;
        }
    }
}

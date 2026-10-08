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
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The cursor of an {@link AsyncWindowRecordCursorFactory} over a plain scan of a whole table, a
 * window partitioned by a symbol column that no index serves (an indexed one is walked key by key
 * instead, see {@link AsyncWindowRecordCursor}, which needs no partition maps). The keys are hashed
 * into shards, one per worker slot, each slot keeping the functions' state of its own keys from one
 * round to the next. A round takes the
 * next page frames, about {@code cairo.sql.parallel.window.round.rows} rows, and every shard's task
 * reads all of them in table order, computing the window, and the steps after it, for the rows of
 * its own keys only. Every key's rows are thus computed in table order, by one slot, exactly as
 * the serial window computes them; only the order of the output differs: round by round, shard by
 * shard. The planner uses it only when an ORDER BY above makes that order invisible.
 * <p>
 * Rounds pipeline two deep: the workers compute the next round while the query's thread returns
 * this one. A shard's next task never starts before its last one ended, since a round is
 * dispatched only once the round before it was computed.
 * <p>
 * A table with a frame other threads cannot read at a stable address (Parquet, or a covering
 * index) is computed by the query's own thread, frame by frame, as the serial window would.
 */
public class AsyncWindowShardCursor implements RecordCursor {
    static final UnorderedPageFrameReducer REDUCER = AsyncWindowShardCursor::reduce;
    private static final int MODE_PARALLEL = 2;
    private static final int MODE_SERIAL = 1;
    private static final int MODE_UNDECIDED = 0;
    private final AsyncWindowAtom atom;
    private final long chainMaxPages;
    private final long chainPageSize;
    private final ColumnTypes columnTypes;

    private final int keyColumnIndex;
    private final SelectedRecord record;
    private final RecordSink recordSink;
    private final long roundRows;
    private final ShardRound[] rounds;
    private final RecordMetadata scanMetadata;
    private final int shardCount;
    // the slice mode, see the class comment: tasks are disjoint row ranges of one stream
    private final boolean slices;
    private final long sliceRows;
    private final AsyncWindowSplitPlan splitPlan;
    // the slice mode: each combined column's value at the last row returned, and whether one was
    private final long[] carry;
    private final boolean[] foldCounted;
    private final double[] foldSums;
    private boolean hasCarry;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private Task emitTask;
    private int emitTaskIndex;
    private SqlExecutionContext executionContext;
    private int frameCount;
    // the frames of one execution, allocated when the cursor opens and freed when it closes
    private PageFrameAddressCache frameAddressCache;
    private PageFrameCursor frameCursor;
    // the round being returned, and the one the workers compute
    private ShardRound headRound;
    private ShardRound inFlightRound;
    private boolean isOpen;
    private boolean isWorkerSlotsOpen;
    private int mode = MODE_UNDECIDED;
    private int nextFrame;
    private long parallelRoundCount;
    private long parallelTaskCount;
    // MODE_SERIAL: where this thread stands in the frames
    private int serialFrame;
    private long serialRow;
    private long serialRows;

    public AsyncWindowShardCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull AsyncWindowAtom atom,
            @NotNull ObjList<UnorderedPageFrameSequence<AsyncWindowRecordCursor.RoundAtom>> sequences,
            @NotNull ColumnTypes columnTypes,
            @NotNull RecordSink recordSink,
            @NotNull RecordMetadata scanMetadata,
            int keyColumnIndex,
            @NotNull AsyncWindowSplitPlan splitPlan
    ) {
        this.atom = atom;
        this.slices = keyColumnIndex < 0;
        this.splitPlan = splitPlan;
        this.carry = new long[splitPlan.getPrefixCount()];
        this.foldSums = new double[splitPlan.getPrefixCount()];
        this.foldCounted = new boolean[splitPlan.getPrefixCount()];
        this.sliceRows = Math.max(1024, configuration.getSqlParallelWindowTaskRows() / 4);
        this.columnTypes = columnTypes;
        this.recordSink = recordSink;
        this.scanMetadata = scanMetadata;
        this.keyColumnIndex = keyColumnIndex;
        this.shardCount = atom.getWorkerSlotCount();
        this.roundRows = Math.max(configuration.getSqlParallelWindowTaskRows(), configuration.getSqlParallelWindowRoundRows());
        this.chainPageSize = configuration.getSqlWindowStorePageSize();
        this.chainMaxPages = Math.max(1L, configuration.getSqlSortValueMaxBytes() / Numbers.ceilPow2(chainPageSize));
        final int columnCount = columnTypes.getColumnCount();
        final IntList identity = new IntList(columnCount);
        for (int i = 0; i < columnCount; i++) {
            identity.add(i);
        }
        this.record = new SelectedRecord(identity);
        this.rounds = new ShardRound[2];
        for (int i = 0; i < 2; i++) {
            final UnorderedPageFrameSequence<AsyncWindowRecordCursor.RoundAtom> sequence = sequences.getQuick(i);
            rounds[i] = new ShardRound(sequence);
            sequence.getAtom().shardRound = rounds[i];
        }
    }

    /**
     * The shard of a key: its symbol key, mixed, scaled to the shard count.
     */
    static int shardOf(int key, int shardCount) {
        // Fibonacci hashing spreads consecutive keys, and the high half of the product of the hash
        // and the shard count maps it onto the shards without a division
        final long mixed = (key * 0x9E3779B9L) & 0xFFFFFFFFL;
        return (int) ((mixed * shardCount) >>> 32);
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
        for (int i = 0, n = atom.getSlotCount(); i < n; i++) {
            try {
                atom.getSlot(i - 1).closeCursor();
            } catch (Throwable th) {
                failure = failure == null ? th : failure;
            }
        }
        for (ShardRound round : rounds) {
            failure = Misc.freeBestEffort(failure, round);
        }
        frameAddressCache = Misc.free(frameAddressCache);
        frameCursor = Misc.free(frameCursor);
        mode = MODE_UNDECIDED;
        isWorkerSlotsOpen = false;
        headRound = null;
        inFlightRound = null;
        emitTask = null;
        executionContext = null;
        circuitBreaker = null;
        CairoException.rethrowCleanupFailure(failure);
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
        return atom.getSlot(-1).getOutputSymbols().getSymbolTable(columnIndex);
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
            final Task task = emitTask;
            if (task != null) {
                if (task.chain.hasNext()) {
                    return true;
                }
                if (slices && carry.length > 0 && task.rowCount > 0) {
                    captureCarry(task);
                }
                task.chain.clear();
                emitTask = null;
            }
            if (headRound != null) {
                if (emitTaskIndex < headRound.taskCount) {
                    startEmitting(headRound.tasks.getQuick(emitTaskIndex++));
                    continue;
                }
                headRound.state = ShardRound.STATE_FREE;
                headRound = null;
            }
            if (inFlightRound == null) {
                if (!dispatchNext()) {
                    return false;
                }
            }
            // the round computed: its shards' slots may take the next one meanwhile
            final ShardRound round = inFlightRound;
            awaitRound(round);
            inFlightRound = null;
            headRound = round;
            emitTaskIndex = 0;
            dispatchNext();
        }
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return atom.getSlot(-1).getOutputSymbols().newSymbolTable(columnIndex);
    }

    public void of(PageFrameCursor frameCursor, SqlExecutionContext executionContext) throws SqlException {
        // own the frame cursor first: close() frees it when anything below throws
        this.frameCursor = frameCursor;
        isOpen = true;
        if (frameAddressCache == null) {
            frameAddressCache = new PageFrameAddressCache();
        }
        this.executionContext = executionContext;
        this.circuitBreaker = executionContext.getCircuitBreaker();
        mode = MODE_UNDECIDED;
        isWorkerSlotsOpen = false;
        resetWalk();
        atom.resetTaskCounts();
        atom.getSlot(-1).open(frameCursor, executionContext);
    }

    @Override
    public RecordBlock peekRecordBlock(int maxRows) {
        final Task task = emitTask;
        return task != null ? task.chain.peekSequentialRecordBlock(maxRows) : null;
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
        RecordCursor.skipRows(this, rowCount);
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public boolean supportsRecordBlocks() {
        return true;
    }

    @Override
    public void toTop() {
        stopRounds();
        for (int i = 0, n = atom.getSlotCount(); i < n; i++) {
            atom.getSlot(i - 1).toTop();
        }
        resetWalk();
        if (mode == MODE_SERIAL) {
            atom.getSlot(-1).resetStream();
        }
    }

    private static void reduce(
            int workerId,
            @NotNull PageFrameMemoryRecord unused,
            int taskIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> sequence,
            @Nullable UnorderedPageFrameSequence<?> stealingSequence
    ) {
        final AsyncWindowRecordCursor.RoundAtom roundAtom = (AsyncWindowRecordCursor.RoundAtom) sequence.getAtom();
        final ShardRound round = roundAtom.shardRound;
        final Task task = round.tasks.getQuick(taskIndex);
        final AsyncWindowAtom atom = roundAtom.atom;
        if (round.slices) {
            // any slot: a slice starts from clean state, the query's thread combines it afterwards
            final int slotId = atom.acquireTaskSlot(workerId, circuitBreaker);
            try {
                final AsyncWindowAtom.Slot slot = atom.getSlot(slotId);
                slot.toTop();
                task.lastOffset = slot.computeSlice(task.frameIndex, task.rowLo, task.rowHi, task.chain, circuitBreaker, sequence);
                task.rowCount = slot.getSliceRowCount();
                slot.countTask();
                atom.countTask(workerId);
            } finally {
                atom.release(slotId);
            }
            return;
        }
        // a shard's own slot, which no other task of the round uses, and which keeps its keys' state
        final AsyncWindowAtom.Slot slot = atom.getSlot(task.shard);
        slot.computeShard(round.frameLo, round.frameHi, task.shard, round.shardCount, round.keyColumnIndex, task.chain, circuitBreaker, sequence);
        slot.countTask();
        atom.countTask(workerId);
    }

    private void awaitRound(ShardRound round) {
        round.state = ShardRound.STATE_READY;
        round.sequence.awaitRound();
    }

    private void chooseMode() {
        // the frames, every one of which each shard reads
        frameAddressCache.of(scanMetadata, frameCursor.getColumnMapping(), frameCursor.isExternal());
        frameCount = 0;
        PageFrame frame;
        boolean plain = true;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            frameAddressCache.add(frameCount++, frame);
        }
        for (int i = 0; i < frameCount; i++) {
            if (frameAddressCache.getFrameFormat(i) != PartitionFormat.NATIVE || frameAddressCache.isFrameCovered(i)) {
                plain = false;
                break;
            }
        }
        atom.getSlot(-1).ofFrames(frameAddressCache);
        record.of(atom.getSlot(-1).getOutputRecord());
        mode = plain && shardCount > 0 ? MODE_PARALLEL : MODE_SERIAL;
    }

    // Collects the next frames into a free round and dispatches it. Returns false when no frame is
    // left.
    private boolean dispatchNext() {
        if (nextFrame >= frameCount || inFlightRound != null) {
            return false;
        }
        final ShardRound round = rounds[0].state == ShardRound.STATE_FREE ? rounds[0] : rounds[1];
        assert round.state == ShardRound.STATE_FREE;
        final int lo = nextFrame;
        long rows = 0;
        while (nextFrame < frameCount && (rows == 0 || rows < roundRows)) {
            rows += frameAddressCache.getFrameSize(nextFrame++);
        }
        if (!isWorkerSlotsOpen) {
            openWorkerSlots();
        }
        if (!round.isSequenceOpen) {
            try {
                round.sequence.ofRounds(frameCursor, executionContext);
            } catch (SqlException e) {
                throw CairoException.nonCritical().put(e.getFlyweightMessage());
            }
            round.isSequenceOpen = true;
        }
        if (slices) {
            round.ofSlices(this, lo, nextFrame);
        } else {
            round.of(this, lo, nextFrame, rows);
        }
        round.state = ShardRound.STATE_IN_FLIGHT;
        round.sequence.dispatchRound(REDUCER, round.taskRowCounts);
        inFlightRound = round;
        parallelRoundCount++;
        parallelTaskCount += round.taskCount;
        return true;
    }

    // MODE_SERIAL: every row of every frame, through this thread's copy, in table order.
    private boolean hasNextSerial() {
        final AsyncWindowAtom.Slot owner = atom.getSlot(-1);
        while (true) {
            if (serialRow < serialRows) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                if (owner.streamFrameRow(serialFrame - 1, serialRow++)) {
                    return true;
                }
                continue;
            }
            if (serialFrame >= frameCount) {
                return false;
            }
            serialRows = frameAddressCache.getFrameSize(serialFrame++);
            serialRow = 0;
        }
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

    private void openWorkerSlots() {
        isWorkerSlotsOpen = true;
        for (int i = 0, n = atom.getSlotCount() - 1; i < n; i++) {
            try {
                atom.getSlot(i).open(frameCursor, executionContext);
            } catch (SqlException e) {
                throw CairoException.nonCritical().put(e.getFlyweightMessage());
            }
        }
        atom.ofWorkerFrames(frameAddressCache);
    }

    private void resetWalk() {
        nextFrame = 0;
        hasCarry = false;
        serialFrame = 0;
        serialRow = 0;
        serialRows = 0;
        headRound = null;
        inFlightRound = null;
        emitTask = null;
        emitTaskIndex = 0;
        parallelRoundCount = 0;
        parallelTaskCount = 0;
        for (ShardRound round : rounds) {
            round.state = ShardRound.STATE_FREE;
            round.clearChains();
        }
    }

    // The slice mode: combines a slice's rows, computed from clean state, with the stream's values
    // at the last row returned, in place, before they are returned; see
    // AsyncWindowRecordCursor.applyCarry(), whose ops these are.
    private void applyCarry(Task task) {
        final RecordChain chain = task.chain;
        final int n = carry.length;
        for (int j = 0; j < n; j++) {
            if (splitPlan.getPrefixOp(j) == AsyncWindowSplitPlan.OP_FOLD) {
                final double before = hasCarry ? Double.longBitsToDouble(carry[j]) : Double.NaN;
                foldSums[j] = Double.isNaN(before) ? 0.0 : before;
                foldCounted[j] = !Double.isNaN(before);
            }
        }
        long offset = 0;
        for (long r = 0, hi = task.rowCount; r < hi; r++) {
            for (int j = 0; j < n; j++) {
                final int op = splitPlan.getPrefixOp(j);
                final int type = splitPlan.getPrefixType(j);
                final long address = chain.getAddress(offset, splitPlan.getPrefixColumn(j));
                if (op == AsyncWindowSplitPlan.OP_FOLD) {
                    final double value = Unsafe.getDouble(address);
                    if (Numbers.isFinite(value)) {
                        foldSums[j] += value;
                        foldCounted[j] = true;
                    }
                    Unsafe.putDouble(address, foldCounted[j] ? foldSums[j] : Double.NaN);
                } else if (!hasCarry) {
                    continue;
                } else if (ColumnType.tagOf(type) == ColumnType.INT) {
                    Unsafe.putInt(address, (int) AsyncWindowSplitPlan.combine(op, type, carry[j], Unsafe.getInt(address)));
                } else {
                    Unsafe.putLong(address, AsyncWindowSplitPlan.combine(op, type, carry[j], Unsafe.getLong(address)));
                }
            }
            offset = chain.getNextRecordOffset(offset);
        }
    }

    // The slice mode: the stream's values at a slice's last row, after applyCarry().
    private void captureCarry(Task task) {
        final RecordChain chain = task.chain;
        for (int j = 0, n = carry.length; j < n; j++) {
            final long address = chain.getAddress(task.lastOffset, splitPlan.getPrefixColumn(j));
            carry[j] = ColumnType.tagOf(splitPlan.getPrefixType(j)) == ColumnType.INT
                    ? Unsafe.getInt(address)
                    : Unsafe.getLong(address);
        }
        hasCarry = true;
    }

    private void startEmitting(Task task) {
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        if (slices && carry.length > 0 && task.rowCount > 0) {
            applyCarry(task);
        }
        task.chain.toTop();
        record.of(task.chain.getRecord());
        emitTask = task;
    }

    // Waits for the round the workers compute, cancelled first, and resets the sequences: a cursor
    // that closes or rewinds must not free or reuse what their tasks still write to.
    private void stopRounds() {
        for (ShardRound round : rounds) {
            if (round.state == ShardRound.STATE_IN_FLIGHT) {
                round.state = ShardRound.STATE_READY;
                round.sequence.cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                try {
                    round.sequence.awaitRound();
                } catch (Throwable ignore) {
                    // the cancellation asked for here, or an error no one reads now
                }
            }
        }
        Throwable failure = null;
        for (ShardRound round : rounds) {
            if (round.isSequenceOpen) {
                round.isSequenceOpen = false;
                try {
                    round.sequence.reset();
                } catch (Throwable th) {
                    failure = failure == null ? th : failure;
                }
            }
            round.state = ShardRound.STATE_FREE;
        }
        inFlightRound = null;
        headRound = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * The frames of one dispatch, and one task per shard over them.
     */
    static class ShardRound implements Mutable, QuietCloseable {
        static final int STATE_FREE = 0;
        static final int STATE_IN_FLIGHT = 1;
        static final int STATE_READY = 2;
        private final UnorderedPageFrameSequence<AsyncWindowRecordCursor.RoundAtom> sequence;
        private final LongList taskRowCounts = new LongList();
        private final ObjList<Task> tasks = new ObjList<>();
        int frameHi;
        int frameLo;
        int keyColumnIndex;
        int shardCount;
        boolean slices;
        int taskCount;
        private boolean isSequenceOpen;
        private int state = STATE_FREE;

        ShardRound(UnorderedPageFrameSequence<AsyncWindowRecordCursor.RoundAtom> sequence) {
            this.sequence = sequence;
        }

        @Override
        public void clear() {
            taskRowCounts.clear();
        }

        @Override
        public void close() {
            clear();
            Misc.freeObjListAndClear(tasks);
        }

        void clearChains() {
            for (int i = 0, n = tasks.size(); i < n; i++) {
                tasks.getQuick(i).chain.clear();
            }
        }

        void of(AsyncWindowShardCursor cursor, int frameLo, int frameHi, long rows) {
            this.frameLo = frameLo;
            this.frameHi = frameHi;
            this.keyColumnIndex = cursor.keyColumnIndex;
            this.shardCount = cursor.shardCount;
            this.slices = false;
            taskRowCounts.clear();
            taskCount = shardCount;
            for (int s = 0; s < shardCount; s++) {
                final Task task = nextTask(cursor, s);
                task.shard = s;
                // each task reads the round's rows, and outputs about its share of them
                taskRowCounts.add(rows);
            }
        }

        // The slice mode: the round's frames cut into row ranges of slice.rows each.
        void ofSlices(AsyncWindowShardCursor cursor, int frameLo, int frameHi) {
            this.frameLo = frameLo;
            this.frameHi = frameHi;
            this.slices = true;
            taskRowCounts.clear();
            taskCount = 0;
            for (int f = frameLo; f < frameHi; f++) {
                final long frameRows = cursor.frameAddressCache.getFrameSize(f);
                for (long lo = 0; lo < frameRows; lo += cursor.sliceRows) {
                    final Task task = nextTask(cursor, taskCount++);
                    task.frameIndex = f;
                    task.rowLo = lo;
                    task.rowHi = Math.min(frameRows, lo + cursor.sliceRows);
                    taskRowCounts.add(task.rowHi - task.rowLo);
                }
            }
        }

        private Task nextTask(AsyncWindowShardCursor cursor, int index) {
            if (index == tasks.size()) {
                tasks.add(cursor.newTask());
            }
            final Task task = tasks.getQuick(index);
            task.chain.clear();
            task.rowCount = 0;
            task.lastOffset = -1;
            return task;
        }
    }

    /**
     * One shard's output rows of a round.
     */
    static class Task implements QuietCloseable {
        private final RecordChain chain;
        // the slice mode: the frame and its rows
        private int frameIndex;
        private long lastOffset = -1;
        private long rowCount;
        private long rowHi;
        private long rowLo;
        private int shard;

        private Task(RecordChain chain) {
            this.chain = chain;
        }

        @Override
        public void close() {
            Misc.free(chain);
        }
    }
}

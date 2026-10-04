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
import io.questdb.cairo.ColumnType;
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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.QuaternaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.table.KeyMajorPageFrameRecordCursor;
import io.questdb.griffin.engine.table.PageFrameRowToucher;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * State the tasks of an {@link AsyncWindowRecordCursorFactory} share: one copy of the window's
 * functions per worker slot plus one for the query's own thread, which only computes the rows it
 * streams itself.
 * <p>
 * A task computes the window over consecutive rows of a key-major scan. Every window function is
 * partitioned by that key, so a key's values depend on its own rows only, in the order the scan
 * walks them, and any slot can compute any task. A task starts from clean function state; a key it
 * continues from an earlier task is rebuilt by warm-up rows or combined afterwards, see
 * {@link AsyncWindowSplitPlan}.
 */
public class AsyncWindowAtom implements StatefulAtom, PerWorkerLockOwner {
    private final PerWorkerLocks perWorkerLocks;
    // slot -1, the query's own thread, then the worker slots
    private final ObjList<Slot> slots;
    // tasks run on a worker thread, as opposed to one the query's thread stole; written by the
    // workers, read once their rounds have been awaited
    private final AtomicLong workerThreadTaskCount = new AtomicLong();

    /**
     * @param ownerFunctions     the functions of the query's own thread, every output column in
     *                           order; the factory reads their metadata, this atom does not own them
     * @param ownerMapStates     the window Map groups over {@code ownerFunctions}, or null
     * @param perWorkerFunctions one list like {@code ownerFunctions} per worker slot; each entry is
     *                           owned by this atom once it has replaced it with null, also when
     *                           the constructor throws
     * @param perWorkerMapStates the window Map groups of each worker list, entries may be null;
     *                           owned like {@code perWorkerFunctions}
     * @param keyRunColumnIndex  the scan's key column when every window function is partitioned
     *                           by it alone, so that tasks may compute key runs, see
     *                           {@link KeyRunWindowFunction}; -1 otherwise
     */
    public AsyncWindowAtom(
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<Function> ownerFunctions,
            @Nullable ObjList<WindowMapState> ownerMapStates,
            @NotNull ObjList<ObjList<Function>> perWorkerFunctions,
            @NotNull ObjList<ObjList<WindowMapState>> perWorkerMapStates,
            int keyRunColumnIndex
    ) {
        final int workerCount = perWorkerFunctions.size();
        assert perWorkerMapStates.size() == workerCount;
        this.slots = new ObjList<>(workerCount + 1);
        try {
            slots.add(new Slot(configuration, ownerFunctions, ownerMapStates, false, keyRunColumnIndex));
            for (int i = 0; i < workerCount; i++) {
                slots.add(new Slot(configuration, perWorkerFunctions.getQuick(i), perWorkerMapStates.getQuick(i), true, keyRunColumnIndex));
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

    /**
     * A worker slot for a task, also for one the query's own thread steals: slot -1 is the query
     * thread's own, which keeps the state of the rows it streams between two of them.
     */
    public int acquireTaskSlot(int workerId, SqlExecutionCircuitBreaker circuitBreaker) {
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    void countTask(int workerId) {
        if (workerId > -1) {
            workerThreadTaskCount.incrementAndGet();
        }
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

    /**
     * Tasks run on a worker thread, not stolen by the query's own thread, since the last
     * {@link #resetTaskCounts()}.
     */
    @TestOnly
    public long getWorkerThreadTaskCount() {
        return workerThreadTaskCount.get();
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

    /**
     * Tasks computed as key runs, see {@link KeyRunWindowFunction}, since the last
     * {@link #resetTaskCounts()}.
     */
    @TestOnly
    public long getKeyRunTaskCount() {
        long count = 0;
        for (int i = 0, n = slots.size(); i < n; i++) {
            count += slots.getQuick(i).keyRunTaskCount;
        }
        return count;
    }

    /**
     * The columns the touch-ahead of a slot that computed key runs loaded from the last frame it
     * read, the most of any such slot; -1 when no slot computed key runs.
     */
    @TestOnly
    public int getKeyRunLoadedColumnCount() {
        int count = -1;
        for (int i = 0, n = slots.size(); i < n; i++) {
            final Slot slot = slots.getQuick(i);
            if (slot.keyRunTaskCount > 0) {
                count = Math.max(count, slot.toucher.getTouchedColumnCount());
            }
        }
        return count;
    }

    /**
     * The columns of the scan a key run loads, by column index, or null for all of them; null
     * also when tasks do not compute key runs.
     */
    @TestOnly
    public boolean @Nullable [] getKeyRunTouchedColumns() {
        return slots.getQuick(0).keyRunTouchedColumns;
    }

    /**
     * Whether the tasks of this atom's query compute key runs, see {@link KeyRunWindowFunction}.
     * Decided when the query is compiled.
     */
    public boolean isKeyRunEnabled() {
        return slots.getQuick(0).keyRunFunctions != null;
    }

    void resetTaskCounts() {
        workerThreadTaskCount.set(0);
        for (int i = 0, n = slots.size(); i < n; i++) {
            slots.getQuick(i).taskCount = 0;
            slots.getQuick(i).keyRunTaskCount = 0;
        }
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
        // How a key run writes an output column: the run's key, or the column's function read
        // through the getter the chain's record sink uses for its type.
        private static final int OUT_BOOL = 1;
        private static final int OUT_BYTE = 2;
        private static final int OUT_CHAR = 3;
        private static final int OUT_DATE = 4;
        private static final int OUT_DOUBLE = 5;
        private static final int OUT_FLOAT = 6;
        private static final int OUT_GEOBYTE = 7;
        private static final int OUT_GEOINT = 8;
        private static final int OUT_GEOLONG = 9;
        private static final int OUT_GEOSHORT = 10;
        private static final int OUT_INT = 11;
        private static final int OUT_IPV4 = 12;
        private static final int OUT_KEY = 0;
        private static final int OUT_LONG = 13;
        private static final int OUT_SHORT = 14;
        private static final int OUT_TIMESTAMP = 15;
        private final long[] batchRows = new long[BATCH_ROWS];
        private final ObjList<Function> functions;
        // the scan's key column, which key runs read once per run rather than once per row
        private final int keyRunColumnIndex;
        // the window functions as key runs, or null when tasks go through the functions' maps
        private final KeyRunWindowFunction[] keyRunFunctions;
        // the keys of the task's runs so far, kept only while assertions are enabled
        private final IntHashSet keyRunKeys = new IntHashSet();
        // the columns of a frame a key run reads, by column index, or null for all of them
        private final boolean[] keyRunTouchedColumns;
        private final ObjList<WindowMapState> mapStates;
        private final int mapStatesCount;
        // per output column: how a key run writes it, OUT_*, and where in the chain's record
        private final int[] outputKinds;
        private final long[] outputOffsets;
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
        private long keyRunTaskCount;
        private long taskCount;

        Slot(
                CairoConfiguration configuration,
                ObjList<Function> functions,
                @Nullable ObjList<WindowMapState> mapStates,
                boolean ownsFunctions,
                int keyRunColumnIndex
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
            this.keyRunColumnIndex = keyRunColumnIndex;
            this.outputKinds = keyRunColumnIndex > -1 ? toOutputKinds(functions, keyRunColumnIndex) : null;
            if (outputKinds != null) {
                this.keyRunFunctions = new KeyRunWindowFunction[windowFunctionsCount];
                for (int i = 0; i < windowFunctionsCount; i++) {
                    keyRunFunctions[i] = (KeyRunWindowFunction) windowFunctions.getQuick(i);
                }
                this.keyRunTouchedColumns = toTouchedColumns(functions, outputKinds);
                this.outputOffsets = new long[functions.size()];
            } else {
                this.keyRunFunctions = null;
                this.keyRunTouchedColumns = null;
                this.outputOffsets = null;
            }
            this.pool = new PageFrameMemoryPool(configuration);
            this.record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            this.virtualRecord = new VirtualRecord(functions);
        }

        // Adds the columns a function reads to the list, when all it is made of is known to read
        // only the columns of its column references. Returns false for anything else.
        private static boolean collectColumns(Function function, IntList columns) {
            if (function instanceof ColumnFunction cf) {
                columns.add(cf.getColumnIndex());
                return true;
            }
            if (function.isConstant() || function.isRuntimeConstant()) {
                return true;
            }
            if (function instanceof UnaryFunction f) {
                return collectColumns(f.getArg(), columns);
            }
            if (function instanceof BinaryFunction f) {
                return collectColumns(f.getLeft(), columns) && collectColumns(f.getRight(), columns);
            }
            if (function instanceof TernaryFunction f) {
                return collectColumns(f.getLeft(), columns) && collectColumns(f.getCenter(), columns)
                        && collectColumns(f.getRight(), columns);
            }
            if (function instanceof QuaternaryFunction f) {
                return collectColumns(f.getFunc0(), columns) && collectColumns(f.getFunc1(), columns)
                        && collectColumns(f.getFunc2(), columns) && collectColumns(f.getFunc3(), columns);
            }
            if (function instanceof MultiArgFunction f) {
                final ObjList<Function> args = f.args();
                for (int i = 0, n = args.size(); i < n; i++) {
                    if (!collectColumns(args.getQuick(i), columns)) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        /**
         * How a key run writes each output column, or null when the slot cannot compute key runs:
         * every window function must compute runs, and every other column must be a column of the
         * scan of a fixed-size type, so that a run only reads the frame.
         */
        private static int @Nullable [] toOutputKinds(ObjList<Function> functions, int keyColumnIndex) {
            final int n = functions.size();
            final int[] kinds = new int[n];
            for (int i = 0; i < n; i++) {
                final Function function = functions.getQuick(i);
                if (function instanceof WindowFunction) {
                    if (!(function instanceof KeyRunWindowFunction kf) || !kf.isKeyRunSupported()) {
                        return null;
                    }
                } else if (!(function instanceof ColumnFunction)) {
                    return null;
                }
                final int type = function.getType();
                if (function instanceof ColumnFunction cf && cf.getColumnIndex() == keyColumnIndex && ColumnType.isSymbol(type)) {
                    kinds[i] = OUT_KEY;
                    continue;
                }
                switch (ColumnType.tagOf(type)) {
                    case ColumnType.BOOLEAN -> kinds[i] = OUT_BOOL;
                    case ColumnType.BYTE -> kinds[i] = OUT_BYTE;
                    case ColumnType.GEOBYTE -> kinds[i] = OUT_GEOBYTE;
                    case ColumnType.SHORT -> kinds[i] = OUT_SHORT;
                    case ColumnType.GEOSHORT -> kinds[i] = OUT_GEOSHORT;
                    case ColumnType.CHAR -> kinds[i] = OUT_CHAR;
                    // a symbol is written as its key, as the chain's record sink does
                    case ColumnType.INT, ColumnType.SYMBOL -> kinds[i] = OUT_INT;
                    case ColumnType.IPv4 -> kinds[i] = OUT_IPV4;
                    case ColumnType.GEOINT -> kinds[i] = OUT_GEOINT;
                    case ColumnType.FLOAT -> kinds[i] = OUT_FLOAT;
                    case ColumnType.LONG -> kinds[i] = OUT_LONG;
                    case ColumnType.DATE -> kinds[i] = OUT_DATE;
                    case ColumnType.TIMESTAMP -> kinds[i] = OUT_TIMESTAMP;
                    case ColumnType.GEOLONG -> kinds[i] = OUT_GEOLONG;
                    case ColumnType.DOUBLE -> kinds[i] = OUT_DOUBLE;
                    default -> {
                        return null;
                    }
                }
            }
            return kinds;
        }

        /**
         * The columns a key run reads: those of the output columns other than the key, and those
         * of the window functions' arguments. Null, for every column, when a function's columns
         * are not known.
         */
        private static boolean @Nullable [] toTouchedColumns(ObjList<Function> functions, int[] outputKinds) {
            final IntList columns = new IntList();
            for (int i = 0, n = functions.size(); i < n; i++) {
                final Function function = functions.getQuick(i);
                final boolean known;
                if (function instanceof KeyRunWindowFunction kf) {
                    final Function arg = kf.getKeyRunArgument();
                    known = arg != null && collectColumns(arg, columns);
                } else {
                    known = outputKinds[i] == OUT_KEY || collectColumns(function, columns);
                }
                if (!known) {
                    return null;
                }
            }
            int columnCount = 0;
            for (int i = 0, n = columns.size(); i < n; i++) {
                final int columnIndex = columns.getQuick(i);
                if (columnIndex < 0) {
                    return null;
                }
                columnCount = Math.max(columnCount, columnIndex + 1);
            }
            final boolean[] touched = new boolean[columnCount];
            for (int i = 0, n = columns.size(); i < n; i++) {
                touched[columns.getQuick(i)] = true;
            }
            return touched;
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
         * Computes the window over collected rows, in their order, and appends the output rows
         * from {@code emitFrom} on to {@code chain}: the rows before it only rebuild the state of
         * a key that an earlier task started. The rows are row ids of a
         * {@link KeyMajorPageFrameRecordCursor} walk. Returns the offset of the last record
         * appended, -1 when none was.
         */
        long compute(
                DirectLongList rows,
                LongList keyStarts,
                long emitFrom,
                RecordChain chain,
                SqlExecutionCircuitBreaker circuitBreaker,
                UnorderedPageFrameSequence<?> sequence
        ) {
            if (keyRunFunctions != null) {
                return computeKeyRuns(rows, keyStarts, emitFrom, chain, circuitBreaker, sequence);
            }
            // the record moves to other frames, so a stream on this slot positions it again
            streamFrameIndex = -1;
            chain.rewind(rows.size() - emitFrom);
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
                        return prevOffset;
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
                final long batchLo = i - n;
                for (int j = 0; j < n; j++) {
                    record.setRowIndex(batch[j]);
                    computeNext(record);
                    if (batchLo + j >= emitFrom) {
                        prevOffset = chain.put(virtualRecord, prevOffset);
                    }
                }
            }
            return prevOffset;
        }

        /**
         * {@link #compute} for a slot whose window functions compute key runs, see
         * {@link KeyRunWindowFunction}. {@code keyStarts} holds, in ascending order, the indexes
         * of {@code rows} at which a key starts, 0 first: each run of rows between two of them
         * belongs to one key, which no other run of the task continues. A run's functions start
         * from a new partition, its key is read off its first row, and its output rows are written
         * straight into the chain, column by column. Only the columns the run reads are loaded.
         */
        long computeKeyRuns(
                DirectLongList rows,
                LongList keyStarts,
                long emitFrom,
                RecordChain chain,
                SqlExecutionCircuitBreaker circuitBreaker,
                UnorderedPageFrameSequence<?> sequence
        ) {
            streamFrameIndex = -1;
            keyRunTaskCount++;
            final long rowCount = rows.size();
            chain.rewind(rowCount - emitFrom);
            if (rowCount == 0) {
                return -1;
            }
            assert keyStarts.size() > 0 && keyStarts.getQuick(0) == 0 : "a task's rows start a key";
            assert clearKeyRunKeys();
            final long stride = chain.getFixedRecordStride();
            final long[] offsets = outputOffsets;
            for (int c = 0, n = offsets.length; c < n; c++) {
                offsets[c] = chain.getOffsetOfColumn(0, c);
            }
            final KeyRunWindowFunction[] functions = keyRunFunctions;
            final long[] batch = batchRows;
            final int keyStartCount = keyStarts.size();
            int keyStartIndex = 0;
            long nextKeyStart = 0;
            int key = 0;
            long prevOffset = -1;
            int frameIndex = -1;
            int batches = 0;
            long batchLo = 0;
            while (batchLo < rowCount) {
                if (++batches == CHECK_BATCHES) {
                    batches = 0;
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    if (!sequence.isActive()) {
                        // the round was cancelled: its output will never be read
                        break;
                    }
                }
                final int batchFrameIndex = KeyMajorPageFrameRecordCursor.toFrameIndex(rows.get(batchLo));
                if (batchFrameIndex != frameIndex) {
                    frameIndex = batchFrameIndex;
                    final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                    record.init(frameMemory);
                    toucher.of(frameAddressCache, frameIndex, frameMemory, keyRunTouchedColumns);
                }
                final int n = collectBatch(rows, batchLo, frameIndex, batch);
                // The batch's columns are loaded together, then computed: interleaving the loads
                // with the computation of the batch before it was measured to be twice as slow,
                // as the computation leaves the reorder buffer room for far fewer of them.
                if (toucher.isEnabled()) {
                    toucher.touch(batch, n);
                }
                // the batch's rows from emitFrom on are output, into records appended together
                final int emitLo = (int) Math.min(n, Math.max(0, emitFrom - batchLo));
                long address = 0;
                if (emitLo < n) {
                    final long first = chain.appendFixedRecords(prevOffset, n - emitLo);
                    prevOffset = first + (n - emitLo - 1) * stride;
                    address = chain.addressOf(first);
                }
                for (int j = 0; j < n; j++) {
                    record.setRowIndex(batch[j]);
                    if (batchLo + j == nextKeyStart) {
                        // equal starts are keys without rows
                        do {
                            keyStartIndex++;
                            nextKeyStart = keyStartIndex < keyStartCount ? keyStarts.getQuick(keyStartIndex) : Long.MAX_VALUE;
                        } while (nextKeyStart == batchLo + j);
                        for (KeyRunWindowFunction function : functions) {
                            function.keyRunStart();
                        }
                        // every row of the run has the key the walk collected it for
                        key = record.getInt(keyRunColumnIndex);
                        // A run starts its key afresh, which is right only when no other run of
                        // the task has that key: the scan's keys are distinct, see
                        // KeyMajorScanFactory.hasDistinctKeys().
                        assert keyRunKeys.add(key) : "the walk visits key " + key + " twice in one task";
                    }
                    for (KeyRunWindowFunction function : functions) {
                        function.keyRunNext(record);
                    }
                    if (j >= emitLo) {
                        writeRow(address, key);
                        address += stride;
                    }
                }
                batchLo += n;
            }
            return prevOffset;
        }

        private boolean clearKeyRunKeys() {
            keyRunKeys.clear();
            return true;
        }

        // The rows of one frame from index lo of rows, at most a batch of them, as frame row
        // indexes. Returns how many.
        private static int collectBatch(DirectLongList rows, long lo, int frameIndex, long[] batch) {
            final long hi = Math.min(rows.size(), lo + batch.length);
            int n = 0;
            for (long i = lo; i < hi; i++) {
                final long rowId = rows.get(i);
                if (KeyMajorPageFrameRecordCursor.toFrameIndex(rowId) != frameIndex) {
                    break;
                }
                batch[n++] = KeyMajorPageFrameRecordCursor.toFrameRowIndex(rowId);
            }
            return n;
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

        // Writes the current row's output columns into the record at address, as the chain's
        // record sink would copy them from the virtual record.
        private void writeRow(long address, int key) {
            final int[] kinds = outputKinds;
            final long[] offsets = outputOffsets;
            final PageFrameMemoryRecord record = this.record;
            for (int c = 0, n = kinds.length; c < n; c++) {
                final long a = address + offsets[c];
                switch (kinds[c]) {
                    case OUT_KEY -> Unsafe.putInt(a, key);
                    case OUT_DOUBLE -> Unsafe.putDouble(a, functions.getQuick(c).getDouble(record));
                    case OUT_TIMESTAMP -> Unsafe.putLong(a, functions.getQuick(c).getTimestamp(record));
                    case OUT_LONG -> Unsafe.putLong(a, functions.getQuick(c).getLong(record));
                    case OUT_INT -> Unsafe.putInt(a, functions.getQuick(c).getInt(record));
                    case OUT_FLOAT -> Unsafe.putFloat(a, functions.getQuick(c).getFloat(record));
                    case OUT_DATE -> Unsafe.putLong(a, functions.getQuick(c).getDate(record));
                    case OUT_IPV4 -> Unsafe.putInt(a, functions.getQuick(c).getIPv4(record));
                    case OUT_GEOINT -> Unsafe.putInt(a, functions.getQuick(c).getGeoInt(record));
                    case OUT_GEOLONG -> Unsafe.putLong(a, functions.getQuick(c).getGeoLong(record));
                    case OUT_SHORT -> Unsafe.putShort(a, functions.getQuick(c).getShort(record));
                    case OUT_GEOSHORT -> Unsafe.putShort(a, functions.getQuick(c).getGeoShort(record));
                    case OUT_CHAR -> Unsafe.putChar(a, functions.getQuick(c).getChar(record));
                    case OUT_BYTE -> Unsafe.putByte(a, functions.getQuick(c).getByte(record));
                    case OUT_GEOBYTE -> Unsafe.putByte(a, functions.getQuick(c).getGeoByte(record));
                    case OUT_BOOL -> Unsafe.putByte(a, (byte) (functions.getQuick(c).getBool(record) ? 1 : 0));
                    default -> throw new AssertionError("unknown output kind " + kinds[c]);
                }
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

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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.QuaternaryFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.table.KeyMajorPageFrameRecordCursor;
import io.questdb.griffin.engine.table.PageFrameRowToucher;
import io.questdb.griffin.engine.table.SelectedRecord;
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
     * @param crossIndex         the scan's columns the functions read as theirs, when the window's
     *                           base is a projection of the scan's columns, see
     *                           {@link io.questdb.griffin.engine.table.SelectedRecordCursorFactory};
     *                           null when the functions read the scan's columns
     */
    public AsyncWindowAtom(
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<Function> ownerFunctions,
            @Nullable ObjList<WindowMapState> ownerMapStates,
            @NotNull ObjList<ObjList<Function>> perWorkerFunctions,
            @NotNull ObjList<ObjList<WindowMapState>> perWorkerMapStates,
            int keyRunColumnIndex,
            @Nullable IntList crossIndex
    ) {
        final int workerCount = perWorkerFunctions.size();
        assert perWorkerMapStates.size() == workerCount;
        this.slots = new ObjList<>(workerCount + 1);
        try {
            slots.add(new Slot(configuration, ownerFunctions, ownerMapStates, false, keyRunColumnIndex, crossIndex));
            for (int i = 0; i < workerCount; i++) {
                slots.add(new Slot(configuration, perWorkerFunctions.getQuick(i), perWorkerMapStates.getQuick(i), true, keyRunColumnIndex, crossIndex));
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

    /**
     * Appends a step after the window and the steps so far: the query thread's copy, which the
     * factory owns, and one copy per worker slot, which this atom owns once the method returns,
     * also when it throws. The list holds one entry per worker slot.
     */
    public void addStage(@NotNull AsyncWindowStage ownerStage, @NotNull ObjList<AsyncWindowStage> workerStages) {
        try {
            assert workerStages.size() == slots.size() - 1;
            slots.getQuick(0).addStage(ownerStage);
            for (int i = 1, n = slots.size(); i < n; i++) {
                slots.getQuick(i).addStage(workerStages.getQuick(i - 1));
                // the slot owns it now
                workerStages.setQuick(i - 1, null);
            }
        } finally {
            Misc.freeObjListAndClear(workerStages);
        }
    }

    @Override
    public void clear() {
    }

    /**
     * Gives every slot a filter its rows must pass before the window sees them, the WHERE of a
     * plain scan, see {@link AsyncWindowShardCursor}: the query thread's, which the factory owns,
     * and one per worker slot, which this atom owns once the method returns, also when it throws.
     */
    public void setPrefilters(@NotNull Function ownerFilter, @NotNull ObjList<Function> workerFilters) {
        try {
            assert workerFilters.size() == slots.size() - 1;
            slots.getQuick(0).prefilter = ownerFilter;
            for (int i = 1, n = slots.size(); i < n; i++) {
                slots.getQuick(i).prefilter = workerFilters.getQuick(i - 1);
                workerFilters.setQuick(i - 1, null);
            }
        } finally {
            Misc.freeObjListAndClear(workerFilters);
        }
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
     * Whether a stage after the window may drop rows, so that the output has fewer rows than the
     * scan.
     */
    public boolean hasFilterStage() {
        return slots.getQuick(0).hasFilterStage();
    }

    /**
     * Whether a step after the window outputs other rows than the scan's: a filter, or a GROUP BY.
     */
    public boolean hasRowChangingStage() {
        return !slots.getQuick(0).isRowCountPreserved();
    }

    /**
     * Whether the last step is a GROUP BY, after which no other step can come.
     */
    public boolean hasGroupByStage() {
        final Slot owner = slots.getQuick(0);
        return owner.stageCount > 0 && owner.stages.getQuick(owner.stageCount - 1).getKind() == AsyncWindowStage.KIND_GROUP_BY;
    }

    /**
     * Whether the tasks of this atom's query compute key runs, see {@link KeyRunWindowFunction}.
     * Decided when the query is compiled.
     */
    public boolean isKeyRunEnabled() {
        return slots.getQuick(0).keyRunFunctions != null;
    }

    /**
     * The stage, by index among the steps, whose window functions a task that continues a key
     * starts afresh at the key's first own row, after the warm-up rows rebuilt the steps before
     * it; -1 for none.
     */
    public void setCarryStage(int carryStage) {
        for (int i = 0, n = slots.size(); i < n; i++) {
            slots.getQuick(i).setCarryStage(carryStage);
        }
    }

    /**
     * Makes the worker slots start their window functions afresh at every key of a task: their
     * copies were compiled without the PARTITION BY, which a key-major scan's runs of one key at a
     * time make redundant, see {@code SqlCodeGenerator.compileStreamingWindowCopy}. The query
     * thread's own copy keeps its partitions: it streams keys one after another without tasks.
     */
    public void setKeyStartReset(boolean keyStartReset) {
        for (int i = 1, n = slots.size(); i < n; i++) {
            slots.getQuick(i).resetAtKeyStarts = keyStartReset;
        }
    }

    /**
     * Steps the rows go through after the window functions.
     */
    public int getStageCount() {
        return slots.getQuick(0).stageCount;
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
        // the steps after the window functions
        private final ObjList<AsyncWindowStage> stages = new ObjList<>();
        // the stage whose windows restart at a continued key's first own row, -1 for none
        private int carryStage = -1;
        // the GROUP BY step, which comes last, or null
        private AsyncWindowGroupByStage groupStage;
        private int groupStageIndex = -1;
        private final ObjList<Function> functions;
        // the scan's key column, which key runs read once per run rather than once per row
        private final int keyRunColumnIndex;
        // the window functions as key runs, or null when tasks go through the functions' maps
        private KeyRunWindowFunction[] keyRunFunctions;
        // the keys of the task's runs so far, kept only while assertions are enabled
        private final IntHashSet keyRunKeys = new IntHashSet();
        // the columns of a frame a key run reads, by column index, or null for all of them
        private final boolean[] keyRunTouchedColumns;
        private final ObjList<WindowMapState> mapStates;
        private final int mapStatesCount;
        // the record a row is output from: the last stage's, or the window's own
        private Record outputRecord;
        // the symbol tables of the output record's columns
        private SymbolTableSource outputSymbols;
        // per output column: how a key run writes it, OUT_*, and where in the chain's record
        private final int[] outputKinds;
        private final long[] outputOffsets;
        private final boolean ownsFunctions;
        private final PageFrameMemoryPool pool;
        private final PageFrameMemoryRecord record;
        // what the window functions read: the scan's record, or a projection of it
        private final Record functionInput;
        // the projection of the scan's record, null when the functions read it as it is
        private final SelectedRecord selectedRecord;
        private final IntList crossIndex;
        // what the window functions read in the serial mode
        private Record serialInput;
        // the scan's WHERE, which a row must pass before the window sees it, or null
        private Function prefilter;
        // rows the last computeSlice() output
        private long sliceRowCount;
        // the window functions start afresh at each key of a task, see setKeyStartReset()
        private boolean resetAtKeyStarts;
        private int stageCount;
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
                int keyRunColumnIndex,
                @Nullable IntList crossIndex
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
            this.crossIndex = crossIndex;
            if (crossIndex != null) {
                this.selectedRecord = new SelectedRecord(crossIndex);
                selectedRecord.of(record);
                this.functionInput = selectedRecord;
            } else {
                this.selectedRecord = null;
                this.functionInput = record;
            }
            this.virtualRecord = new VirtualRecord(functions);
            this.outputRecord = virtualRecord;
            this.outputSymbols = new FunctionSymbols(functions);
        }

        /**
         * Appends a step after the last one, which reads its output. Tasks no longer compute key
         * runs: those write the window's own output straight into the chain.
         */
        void addStage(AsyncWindowStage stage) {
            stages.add(stage);
            stageCount++;
            outputRecord = stage.bind(outputRecord, outputSymbols);
            outputSymbols = stage;
            keyRunFunctions = null;
            if (stage instanceof AsyncWindowGroupByStage g) {
                groupStage = g;
                groupStageIndex = stageCount - 1;
            }
        }

        AsyncWindowGroupByStage getGroupStage() {
            return groupStage;
        }

        void setCarryStage(int carryStage) {
            this.carryStage = carryStage;
        }

        // The first stage the warm-up rows of a continued key must not reach: the carry stage, whose
        // running values a task computes from its own rows, or else a GROUP BY, whose groups
        // must hold the task's own rows only; -1 for none.
        private int resetStage() {
            return carryStage > -1 ? carryStage : groupStageIndex;
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
                failure = Misc.freeBestEffort(failure, prefilter);
                failure = Misc.freeObjListBestEffort(failure, stages);
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
            for (int i = 0; i < stageCount; i++) {
                stages.getQuick(i).closeCursor();
            }
            if (prefilter != null) {
                prefilter.cursorClosed();
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
                UnorderedPageFrameSequence<?> sequence,
                @Nullable GroupSplit groupSplit
        ) {
            if (keyRunFunctions != null) {
                return computeKeyRuns(rows, keyStarts, emitFrom, chain, circuitBreaker, sequence);
            }
            final long secondKeyStart = keyStarts.size() > 1 ? keyStarts.getQuick(1) : Long.MAX_VALUE;
            final int resetStage = resetStage();
            final int keyStartCount = keyStarts.size();
            int keyStartIndex = 1;
            long nextKeyStart = resetAtKeyStarts ? secondKeyStart : Long.MAX_VALUE;
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
                    final long index = batchLo + j;
                    if (index == nextKeyStart) {
                        // equal starts are keys without rows
                        do {
                            keyStartIndex++;
                            nextKeyStart = keyStartIndex < keyStartCount ? keyStarts.getQuick(keyStartIndex) : Long.MAX_VALUE;
                        } while (nextKeyStart == index);
                        resetWindows();
                    }
                    if (index == emitFrom) {
                        if (emitFrom > 0 && resetStage > -1) {
                            // The warm-up rows rebuilt the stages before the carry stage; its
                            // running values start at the task's own rows, and the cursor adds
                            // the key's carry. A GROUP BY's groups hold the task's own rows only.
                            for (int s = resetStage; s < stageCount; s++) {
                                stages.getQuick(s).toTop();
                            }
                        }
                        if (groupStage != null) {
                            groupStage.setRowId(groupSplit != null ? groupSplit.walkBase : 0);
                            if (groupSplit != null && groupSplit.continuesKey) {
                                groupStage.beginHeadCapture(groupSplit.headChain);
                            }
                        }
                    }
                    if (computeNext(functionInput) && index >= emitFrom) {
                        prevOffset = chain.put(outputRecord, prevOffset);
                        // a group closed by the second key's first row is the first key's last
                        if (groupSplit != null && index <= secondKeyStart) {
                            groupSplit.firstKeyGroupRows++;
                        }
                    }
                }
            }
            if (groupSplit != null) {
                return finishGroupSplit(groupSplit, chain, prevOffset, secondKeyStart >= rowCount);
            }
            // a GROUP BY step outputs the task's last group now; keys never span tasks then
            if (flush()) {
                prevOffset = chain.put(outputRecord, prevOffset);
            }
            return prevOffset;
        }

        // A new key's first row: every window function starts afresh, see setKeyStartReset(). A
        // GROUP BY step keeps its open group, which the key column ends by itself.
        private void resetWindows() {
            GroupByUtils.toTop(functions);
            for (int i = 0; i < mapStatesCount; i++) {
                mapStates.getQuick(i).clear();
            }
            for (int i = 0; i < stageCount; i++) {
                final AsyncWindowStage stage = stages.getQuick(i);
                if (stage.getKind() == AsyncWindowStage.KIND_WINDOW) {
                    stage.toTop();
                }
            }
        }

        /**
         * Ends a task whose keys may continue over tasks, with a GROUP BY step: see
         * {@link GroupSplit}. The rows of a continued key's first group were captured, not
         * aggregated; the group open at the end is handed over, not output, when the task's last
         * key continues in the next task.
         */
        private long finishGroupSplit(GroupSplit groupSplit, RecordChain chain, long prevOffset, boolean singleKeyTask) {
            final AsyncWindowGroupByStage stage = groupStage;
            final boolean capturing = stage.isCapturing();
            // the head group is complete unless the task's rows all belong to it and the key goes on
            groupSplit.headClosed = !capturing || !groupSplit.lastKeyContinues;
            groupSplit.headRows = groupSplit.continuesKey ? stage.getHeadRowCount() : 0;
            groupSplit.hasTail = false;
            groupSplit.tailIsFirstKey = singleKeyTask;
            if (groupSplit.lastKeyContinues) {
                if (!capturing) {
                    groupSplit.hasTail = stage.exportOpenGroup(groupSplit.tailValue, groupSplit.tailKeys);
                }
            } else if (!capturing && stage.closeOpenGroup()) {
                prevOffset = chain.put(outputRecord, prevOffset);
                if (singleKeyTask) {
                    groupSplit.firstKeyGroupRows++;
                }
            }
            return prevOffset;
        }

        /**
         * Computes a shard's rows of the frames from {@code frameLo} to {@code frameHi}, see
         * {@link AsyncWindowShardCursor}: every row whose key falls in the shard, in table order,
         * from the functions' state the shard's earlier rows left; appends the output rows to the
         * chain. Returns the offset of the last record appended, -1 when none was.
         */
        long computeShard(
                int frameLo,
                int frameHi,
                int shard,
                int shardCount,
                int keyColumnIndex,
                RecordChain chain,
                SqlExecutionCircuitBreaker circuitBreaker,
                UnorderedPageFrameSequence<?> sequence
        ) {
            streamFrameIndex = -1;
            long prevOffset = -1;
            int checks = 0;
            final int nullKey = NullMemoryCMR.INSTANCE.getInt(0);
            for (int frameIndex = frameLo; frameIndex < frameHi; frameIndex++) {
                final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                record.init(frameMemory);
                final long rowCount = frameAddressCache.getFrameSize(frameIndex);
                // The key column's symbol keys, read straight from the frame: every shard reads
                // every key of the round, and computes only the rows of its own. A column top
                // (no address) is NULL keys, as the record reads it.
                final long keyAddress = frameMemory.getPageAddresses().get(frameMemory.getColumnOffset() + keyColumnIndex);
                for (long row = 0; row < rowCount; row++) {
                    if (++checks == CHECK_BATCHES * BATCH_ROWS) {
                        checks = 0;
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        if (!sequence.isActive()) {
                            // the round was cancelled: its output will never be read
                            return prevOffset;
                        }
                    }
                    final int key = keyAddress != 0 ? Unsafe.getInt(keyAddress + (row << 2)) : nullKey;
                    if (AsyncWindowShardCursor.shardOf(key, shardCount) != shard) {
                        continue;
                    }
                    record.setRowIndex(row);
                    if ((prefilter == null || prefilter.getBool(functionInput)) && computeNext(functionInput)) {
                        prevOffset = chain.put(outputRecord, prevOffset);
                    }
                }
            }
            return prevOffset;
        }

        /**
         * Computes the rows of a frame from {@code rowLo} to {@code rowHi} that pass the
         * prefilter, from the functions' clean state, see {@link AsyncWindowShardCursor}; appends
         * the output rows to the chain. Returns the offset of the last record appended, -1 when
         * none was; {@link #getSliceRowCount()} tells how many.
         */
        long computeSlice(
                int frameIndex,
                long rowLo,
                long rowHi,
                RecordChain chain,
                SqlExecutionCircuitBreaker circuitBreaker,
                UnorderedPageFrameSequence<?> sequence
        ) {
            streamFrameIndex = -1;
            record.init(pool.navigateTo(frameIndex));
            long prevOffset = -1;
            long rowCount = 0;
            int checks = 0;
            for (long row = rowLo; row < rowHi; row++) {
                if (++checks == CHECK_BATCHES * BATCH_ROWS) {
                    checks = 0;
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    if (!sequence.isActive()) {
                        sliceRowCount = rowCount;
                        return prevOffset;
                    }
                }
                record.setRowIndex(row);
                if ((prefilter == null || prefilter.getBool(functionInput)) && computeNext(functionInput)) {
                    prevOffset = chain.put(outputRecord, prevOffset);
                    rowCount++;
                }
            }
            sliceRowCount = rowCount;
            return prevOffset;
        }

        long getSliceRowCount() {
            return sliceRowCount;
        }

        /**
         * Computes the row of a frame for the serial mode of {@link AsyncWindowShardCursor},
         * leaving its output in the output record. Returns false when a filter step drops it.
         */
        boolean streamFrameRow(int frameIndex, long row) {
            if (frameIndex != streamFrameIndex) {
                record.init(pool.navigateTo(frameIndex));
                streamFrameIndex = frameIndex;
            }
            record.setRowIndex(row);
            return (prefilter == null || prefilter.getBool(functionInput)) && computeNext(functionInput);
        }

        /**
         * Ends the rows computed so far, see {@link AsyncWindowStage#flush()}: returns whether the
         * last step output a row, which {@link #getOutputRecord()} then holds.
         */
        boolean flush() {
            return stageCount > 0 && stages.getQuick(stageCount - 1).flush();
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

        /**
         * Computes the window and the stages after it for the row the record stands on. Returns
         * false when a filter stage drops the row.
         */
        boolean computeNext(Record record) {
            // Groups first, as WindowRecordCursorFactory does: a bound function's computeNext is
            // a no-op, and its getters answer with what its group just materialized.
            for (int i = 0; i < mapStatesCount; i++) {
                mapStates.getQuick(i).computeNext(record);
            }
            for (int i = 0; i < windowFunctionsCount; i++) {
                windowFunctions.getQuick(i).computeNext(record);
            }
            for (int i = 0; i < stageCount; i++) {
                if (!stages.getQuick(i).computeNext()) {
                    return false;
                }
            }
            return true;
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

        boolean hasFilterStage() {
            for (int i = 0; i < stageCount; i++) {
                if (stages.getQuick(i).getKind() == AsyncWindowStage.KIND_FILTER) {
                    return true;
                }
            }
            return false;
        }

        // whether every scan row comes out as one output row
        boolean isRowCountPreserved() {
            for (int i = 0; i < stageCount; i++) {
                final AsyncWindowStage stage = stages.getQuick(i);
                if (stage.getKind() == AsyncWindowStage.KIND_FILTER || !stage.isRowPreserving()) {
                    return false;
                }
            }
            return true;
        }

        /**
         * The record a row is output from, positioned by {@link #computeNext} or
         * {@link #streamRow}.
         */
        Record getOutputRecord() {
            return outputRecord;
        }

        /**
         * The symbol tables of the output record's columns.
         */
        SymbolTableSource getOutputSymbols() {
            return outputSymbols;
        }

        /**
         * What the window functions read in the serial mode, see {@link #ofSerial}.
         */
        Record getSerialInput() {
            return serialInput;
        }

        /**
         * Points the functions at the base cursor's record, for the serial mode: the scan's record,
         * which the slot projects as the functions expect.
         */
        void ofSerial(Record baseRecord) {
            if (selectedRecord != null) {
                selectedRecord.of(baseRecord);
                serialInput = selectedRecord;
            } else {
                serialInput = baseRecord;
            }
            virtualRecord.of(serialInput);
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
            if (crossIndex != null) {
                // the functions read the projection's columns
                symbolTableSource = new CrossIndexSymbols(symbolTableSource, crossIndex);
            }
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
            for (int i = 0; i < stageCount; i++) {
                stages.getQuick(i).open(executionContext, ownsFunctions || executionContext.getCloneSymbolTables());
            }
            if (prefilter != null) {
                final boolean current = executionContext.getCloneSymbolTables();
                executionContext.setCloneSymbolTables(ownsFunctions || current);
                try {
                    prefilter.init(symbolTableSource, executionContext);
                } finally {
                    executionContext.setCloneSymbolTables(current);
                }
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
         * of one frame ahead are loaded together first. Returns false when a filter stage drops
         * the row.
         */
        boolean streamRow(DirectLongList rows, long index) {
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
            return computeNext(functionInput);
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
            for (int i = 0; i < stageCount; i++) {
                stages.getQuick(i).toTop();
            }
            if (prefilter != null) {
                prefilter.toTop();
            }
        }

        void ofFrames(PageFrameAddressCache frameAddressCache) {
            this.frameAddressCache = frameAddressCache;
            pool.of(frameAddressCache);
            resetStream();
            // the serial mode of an earlier execution may have pointed it at the scan's record
            if (selectedRecord != null) {
                // the serial mode of an earlier execution may have pointed it at the base's record
                selectedRecord.of(record);
            }
            virtualRecord.of(functionInput);
        }
    }

    /**
     * The symbol tables of a list of output column functions, as a cursor over them serves them.
     */
    static class FunctionSymbols implements SymbolTableSource {
        private final ObjList<Function> functions;

        FunctionSymbols(ObjList<Function> functions) {
            this.functions = functions;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return (SymbolTable) functions.getQuick(columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return ((SymbolFunction) functions.getQuick(columnIndex)).newSymbolTable();
        }
    }

    /**
     * The symbol tables of a projection of a cursor's columns.
     */
    static class CrossIndexSymbols implements SymbolTableSource {
        private final IntList crossIndex;
        private final SymbolTableSource source;

        CrossIndexSymbols(SymbolTableSource source, IntList crossIndex) {
            this.source = source;
            this.crossIndex = crossIndex;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return source.getSymbolTable(crossIndex.getQuick(columnIndex));
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return source.newSymbolTable(crossIndex.getQuick(columnIndex));
        }
    }

    /**
     * What a task hands the cursor when its keys may continue over tasks and the last step is a
     * GROUP BY: a group that spans two tasks is aggregated by neither alone. A task that continues
     * a key captures the rows of the key's first group in {@link #headChain} (the GROUP BY step's
     * input rows); a task whose last key goes on into the next task leaves that key's open group
     * in {@link #tailValue} and {@link #tailKeys}. The cursor, in walk order, replays the head rows
     * into the group the task before left open, and takes the tail over, see
     * {@link AsyncWindowGroupByStage#replay}.
     */
    public static class GroupSplit implements QuietCloseable {
        final RecordChain headChain;
        final long[] tailKeys;
        final SimpleMapValue tailValue;
        // the task's first key continues a key of the task before it
        boolean continuesKey;
        // rows of the chain that are the first key's closed groups, after its head group
        long firstKeyGroupRows;
        // the head group ended within the task: no later task continues it
        boolean headClosed;
        // a tail group was handed over
        boolean hasTail;
        // rows the head chain holds
        long headRows;
        // the task's last key goes on in the next task
        boolean lastKeyContinues;
        // the tail group is the first key's, which a carry applies to
        boolean tailIsFirstKey;
        // the walk position of the task's first own row, the row id of its first group row
        long walkBase;

        public GroupSplit(RecordChain headChain, int valueCount, int keyCount) {
            this.headChain = headChain;
            this.tailValue = new SimpleMapValue(valueCount);
            this.tailKeys = new long[keyCount];
        }

        @Override
        public void close() {
            Misc.free(headChain);
            Misc.free(tailValue);
        }

        void reset() {
            continuesKey = false;
            firstKeyGroupRows = 0;
            headClosed = false;
            hasTail = false;
            headRows = 0;
            lastKeyContinues = false;
            tailIsFirstKey = false;
            walkBase = 0;
        }
    }
}

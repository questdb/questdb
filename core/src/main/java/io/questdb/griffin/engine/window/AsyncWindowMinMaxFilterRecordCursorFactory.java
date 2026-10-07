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

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ReaderScanProfile;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.window.WholePartitionMinMax;
import io.questdb.griffin.engine.join.JoinRecord;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * A filter over {@code min|max(x) OVER (PARTITION BY k...)} windows, run in two parallel phases
 * instead of a serial two-pass window followed by a serial filter:
 * <ol>
 *     <li>The shared query workers aggregate the base's page frames into one map per worker, from
 *     the partition key to each window's value, and the query's thread merges them. This is a
 *     GROUP BY of the partition keys.</li>
 *     <li>An {@link AsyncFilteredRecordCursorFactory} filters the base's frames on the workers. Its
 *     filter is the query's own, compiled against the window's metadata, and evaluated over a
 *     record that reads the base columns from the frame and each window column from the frozen map,
 *     probed by the row's partition key: a semi-join of the base with the aggregate.</li>
 * </ol>
 * The output is the window's columns, in the window's metadata, row for row in the base's order,
 * which is the order the serial plan emits. Window columns are looked up on demand, so a projection
 * that drops them never probes on the query's thread.
 * <p>
 * <b>One snapshot.</b> Every pass reads the base through one page frame cursor, opened once per
 * execution and rewound between passes, so they all read the same version of the table, as the
 * serial plan's single scan does. A commit that lands while the query runs is not seen by any pass.
 * <p>
 * The aggregation mirrors the window functions' first pass, see {@link WholePartitionMinMax}.
 * Every case but one is independent of row order. A DOUBLE {@code min} compares with a tolerance,
 * so when a partition holds two distinct values within {@code Numbers.DOUBLE_TOLERANCE} of its
 * smallest, the window's value is the first of them in scan order. The workers track each
 * partition's two smallest distinct values; where they are that close, the partition is replayed:
 * the workers collect, frame by frame and over the same snapshot, the argument values of those
 * partitions' rows only, and the query's thread folds them with the window's comparison in scan
 * order. The replay costs one more parallel pass over the key and argument columns, with a map
 * probe per row, plus a serial fold over the replayed partitions' rows. Should those rows exceed
 * the collection budget, the query's thread folds the frames of the same snapshot itself, in scan
 * order, instead: a serial pass over the key and argument columns.
 */
public class AsyncWindowMinMaxFilterRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int ARG_DATE = 4;
    public static final int ARG_DOUBLE = 0;
    public static final int ARG_FLOAT = 1;
    public static final int ARG_LONG = 2;
    public static final int ARG_TIMESTAMP = 3;
    private static final long DEFAULT_MAX_REPLAY_LONGS = 1L << 22;
    private static final long MAX_DENSE_SLOTS = 1 << 22;
    private static final int MODE_AGGREGATE = 0;
    private static final int MODE_COLLECT = 1;
    private static final UnorderedPageFrameReducer REDUCER = AsyncWindowMinMaxFilterRecordCursorFactory::reduce;
    private static final int ROWS_PER_BREAKER_CHECK = 64 * 1024;
    private static final int ROWS_PER_OVERFLOW_CHECK = 4 * 1024;
    private final int[] argColumns;
    private final int[] argKinds;
    private final RecordCursorFactory base;
    private final int baseColumnCount;
    private final IntList crossIndex;
    private final MinMaxCursor cursor;
    private final ObjList<LookupFilter> filters = new ObjList<>();
    // per partition key column: its base column, and whether every one is a SYMBOL
    private final boolean isAllSymbolKeys;
    private final boolean[] isMin;
    private final int[] keyColumnArray;
    private final OrderedMap lookupMap;
    // the value slot of the key's ordinal among the replayed keys, or -1 without a tie window
    private final int ordinalSlot;
    private final PageFrameMemoryRecord replayRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    private final SnapshotBase snapshotBase;
    private final int[] symbolCounts;
    // the DOUBLE and FLOAT min windows, whose value may depend on scan order
    private final int[] tieWindows;
    // value slots per key
    private final int valueCount;
    private final ObjList<CharSequence> windowPlans;
    // per window, its first value slot; a tie window has two, its value and the next distinct one
    private final int[] windowSlots;
    private final int workerCount;
    // The dense lookup of an all-SYMBOL key, or 0: per window, one value per combination of
    // symbol keys, NULL included; see buildDense().
    private long denseAddr;
    private int denseBuildCount;
    private long denseSize;
    private long denseSlots;
    private MemoryTracker denseTracker;
    private AsyncFilteredRecordCursorFactory filterFactory;
    private UnorderedPageFrameSequence<Atom> frameSequence;
    // test seams, see their setters
    private long maxDenseSlots = -1;
    private long maxReplayLongs = DEFAULT_MAX_REPLAY_LONGS;
    private long replayFallbackCount;
    private long replayRunCount;
    private long replayedKeyCount;

    /**
     * Takes ownership of {@code base}, {@code filter} and {@code perWorkerFilters} as soon as it is
     * entered: a throw frees them. The per-worker filters are null when the filter is thread-safe.
     *
     * @param crossIndex      per output column, the base column it reads, or, for a window column,
     *                        {@code baseColumnCount + window ordinal}
     * @param keyColumns      the base columns of the partition key, common to every window
     * @param argColumns      per window, the base column of its argument
     * @param argKinds        per window, one of the {@code ARG_*} kinds
     * @param isMin           per window, min or max
     * @param usedBaseColumns the base columns the filter reads, partition key and arguments included
     */
    public AsyncWindowMinMaxFilterRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory base,
            @NotNull Function filter,
            @Nullable ObjList<Function> perWorkerFilters,
            @NotNull ExpressionNode filterExpr,
            @NotNull IntList crossIndex,
            @Nullable Class<RecordSink> keySinkClass,
            @NotNull ArrayColumnTypes keyTypes,
            @NotNull IntList keyColumns,
            int @NotNull [] argColumns,
            int @NotNull [] argKinds,
            boolean @NotNull [] isMin,
            @NotNull IntHashSet usedBaseColumns,
            @NotNull ObjList<CharSequence> windowPlans,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount
    ) {
        super(metadata);
        this.base = base;
        // owns the base from here on
        this.snapshotBase = new SnapshotBase(base);
        this.baseColumnCount = base.getMetadata().getColumnCount();
        this.crossIndex = crossIndex;
        this.argColumns = argColumns;
        this.argKinds = argKinds;
        this.isMin = isMin;
        this.windowPlans = windowPlans;
        this.workerCount = workerCount;
        this.keyColumnArray = keyColumns.toArray();
        boolean allSymbols = true;
        for (int keyColumn : keyColumnArray) {
            allSymbols &= ColumnType.isSymbol(base.getMetadata().getColumnType(keyColumn));
        }
        this.isAllSymbolKeys = allSymbols;
        this.symbolCounts = new int[keyColumnArray.length];
        final int windowCount = argColumns.length;
        // slots by need: a tie window keeps the next distinct value above its smallest too
        this.windowSlots = new int[windowCount];
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        int tieWindowCount = 0;
        for (int i = 0; i < windowCount; i++) {
            windowSlots[i] = valueTypes.getColumnCount();
            final int type = argKinds[i] <= ARG_FLOAT ? ColumnType.DOUBLE : ColumnType.LONG;
            valueTypes.add(type);
            if (isTieWindow(i)) {
                valueTypes.add(type);
                tieWindowCount++;
            }
        }
        this.tieWindows = new int[tieWindowCount];
        for (int i = 0, t = 0; i < windowCount; i++) {
            if (isTieWindow(i)) {
                tieWindows[t++] = i;
            }
        }
        if (tieWindowCount > 0) {
            this.ordinalSlot = valueTypes.getColumnCount();
            valueTypes.add(ColumnType.LONG);
        } else {
            this.ordinalSlot = -1;
        }
        this.valueCount = valueTypes.getColumnCount();
        OrderedMap lookupMap = null;
        Atom atom = null;
        UnorderedPageFrameSequence<Atom> frameSequence = null;
        AsyncFilteredRecordCursorFactory filterFactory = null;
        boolean ownsWorkerFilters = true;
        try {
            lookupMap = newMap(configuration, keyTypes, valueTypes);
            final IntHashSet aggregateColumns = new IntHashSet();
            for (int i = 0, n = keyColumns.size(); i < n; i++) {
                aggregateColumns.add(keyColumns.getQuick(i));
            }
            for (int i = 0; i < windowCount; i++) {
                aggregateColumns.add(argColumns[i]);
            }
            atom = new Atom(configuration, lookupMap, keyTypes, valueTypes, keySinkClass, base.getMetadata(), keyColumns,
                    aggregateColumns, workerCount);
            final Atom atomToTransfer = atom;
            atom = null;
            frameSequence = new UnorderedPageFrameSequence<>(engine, configuration, messageBus, atomToTransfer, REDUCER, workerCount);

            final int slotCount = perWorkerFilters != null ? perWorkerFilters.size() : workerCount;
            final LookupFilter ownerFilter = new LookupFilter(this, filter, true, keySinkClass, keyColumns);
            filters.add(ownerFilter);
            final ObjList<Function> workerFilters = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                final Function inner = perWorkerFilters != null ? perWorkerFilters.getQuick(i) : filter;
                final LookupFilter workerFilter = new LookupFilter(this, inner, perWorkerFilters != null, keySinkClass, keyColumns);
                if (perWorkerFilters != null) {
                    perWorkerFilters.setQuick(i, null);
                }
                filters.add(workerFilter);
                workerFilters.add(workerFilter);
            }
            ownsWorkerFilters = false;
            // the filter reads the base through the snapshot, as phase one does
            filterFactory = new AsyncFilteredRecordCursorFactory(
                    engine,
                    configuration,
                    messageBus,
                    snapshotBase,
                    ownerFilter,
                    usedBaseColumns,
                    reduceTaskFactory,
                    workerFilters,
                    filterExpr,
                    null,
                    0,
                    workerCount,
                    false
            );
            this.cursor = new MinMaxCursor(keySinkClass, keyColumns);
        } catch (Throwable th) {
            if (ownsWorkerFilters) {
                Misc.freeObjList(perWorkerFilters, th);
            }
            if (filterFactory == null) {
                // the lookup filters own the inner filters, the owner's included; nothing else
                // closes them before the filter factory adopts them
                if (filters.size() > 0) {
                    Misc.freeObjList(filters, th);
                } else {
                    Misc.free(filter, th);
                }
                Misc.free(snapshotBase, th);
            } else {
                Misc.free(filterFactory, th);
            }
            Misc.free(frameSequence, th);
            Misc.free(atom, th);
            Misc.free(lookupMap, th);
            Misc.free(replayRecord, th);
            throw th;
        }
        this.lookupMap = lookupMap;
        this.frameSequence = frameSequence;
        this.filterFactory = filterFactory;
    }

    @Override
    public void changePageFrameSizes(int minRows, int maxRows) {
        base.changePageFrameSizes(minRows, maxRows);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    /**
     * Slots held across both phases; zero whenever no task runs.
     */
    @TestOnly
    public int getAcquiredSlotCount() {
        final PerWorkerLocks filterLocks = filterFactory.getAtom().getPerWorkerLocks();
        return frameSequence.getAtom().locks.getAcquiredSlotCount() + (filterLocks != null ? filterLocks.getAcquiredSlotCount() : 0);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
        final int order = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        try {
            // one page frame cursor, so one version of the table, for every pass
            snapshotBase.open(executionContext, order);
            lookupMap.close();
            lookupMap.setMemoryTracker(memoryTracker);
            lookupMap.reopen();
            aggregate(executionContext, order);
            replay(executionContext, order, memoryTracker);
            buildDense(memoryTracker);
            final RecordCursor filterCursor = filterFactory.getCursor(executionContext);
            cursor.of(filterCursor, memoryTracker);
            return cursor;
        } catch (Throwable th) {
            if (cursor.isOpen) {
                // releases the lookup and the snapshot too
                Misc.free(cursor, th);
            } else {
                try {
                    releaseLookup();
                } catch (Throwable cleanupFailure) {
                    th.addSuppressed(cleanupFailure);
                } finally {
                    snapshotBase.release(th);
                }
            }
            throw th;
        }
    }

    /**
     * Executions that looked window values up in a dense array rather than the map.
     */
    @TestOnly
    public int getDenseBuildCount() {
        return denseBuildCount;
    }

    /**
     * Replays that exceeded the collection budget and folded the snapshot's frames on the query's
     * thread instead.
     */
    @TestOnly
    public long getReplayFallbackCount() {
        return replayFallbackCount;
    }

    @TestOnly
    public long getReplayRunCount() {
        return replayRunCount;
    }

    @TestOnly
    public long getReplayedKeyCount() {
        return replayedKeyCount;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    /**
     * The value slots each partition key holds in the map.
     */
    @TestOnly
    public int getValueSlotCount() {
        return valueCount;
    }

    @Override
    public boolean isNonDeterministic() {
        return filterFactory.isNonDeterministic();
    }

    @Override
    public boolean isStableWithinExecution() {
        return filterFactory.isStableWithinExecution();
    }

    /**
     * Merges one worker's values for a key into another's, as phase one does.
     */
    @TestOnly
    public void mergeForTesting(MapValue dest, MapValue src) {
        merge(dest, src);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    /**
     * Caps the slots of a dense lookup, see {@link #buildDense}; negative restores the default.
     */
    @TestOnly
    public void setMaxDenseSlots(long maxDenseSlots) {
        this.maxDenseSlots = maxDenseSlots;
    }

    /**
     * Caps the longs a replay collects before it falls back to folding on the query's thread.
     */
    @TestOnly
    public void setMaxReplayLongs(long maxReplayLongs) {
        this.maxReplayLongs = maxReplayLongs;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Window Min/Max Filter");
        sink.meta("workers").val(workerCount);
        sink.attr("filter").val(filters.getQuick(0).inner);
        sink.attr("windows").val(windowPlans);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return false;
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    // A symbol key's index in its dense span: NULL first, then the keys; -1 outside the table.
    private static long denseIndex(int key, int count) {
        if (key == SymbolTable.VALUE_IS_NULL) {
            return 0;
        }
        return key >= 0 && key < count ? key + 1L : -1;
    }

    private static boolean isAmbiguous(double min, double next) {
        return !Double.isNaN(next) && Numbers.equals(min, next);
    }

    private static OrderedMap newMap(CairoConfiguration configuration, ArrayColumnTypes keyTypes, ArrayColumnTypes valueTypes) {
        return new OrderedMap(
                configuration.getSqlSmallMapPageSize(),
                keyTypes,
                valueTypes,
                configuration.getSqlSmallMapKeyCapacity(),
                configuration.getSqlFastMapLoadFactor(),
                configuration.getSqlMapMaxResizes(),
                false
        );
    }

    private static void reduce(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        @SuppressWarnings("unchecked") final Atom atom = ((UnorderedPageFrameSequence<Atom>) frameSequence).getAtom();
        final boolean owner = stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final PageFrameMemoryPool pool = atom.getPool(slotId);
        try {
            if (atom.mode == MODE_COLLECT && atom.isCollectOverflow) {
                // the query's thread folds the frames itself
                return;
            }
            final PageFrameMemory frameMemory = pool.navigateTo(frameIndex, atom.aggregateColumns);
            record.init(frameMemory);
            if (atom.mode == MODE_AGGREGATE) {
                atom.factory.aggregateFrame(atom.getMap(slotId), atom.getSink(slotId), record, frameRowCount);
            } else {
                atom.factory.collectFrame(atom, slotId, record, frameIndex, frameRowCount);
            }
        } finally {
            try {
                pool.releaseParquetBuffers();
            } finally {
                atom.release(slotId);
            }
        }
    }

    private static void updateDouble(MapValue value, int slot, double d, boolean isMin) {
        final double current = value.getDouble(slot);
        if (Double.isNaN(current)) {
            value.putDouble(slot, d);
            return;
        }
        final int c = Double.compare(d, current);
        if (isMin) {
            if (c < 0) {
                value.putDouble(slot + 1, current);
                value.putDouble(slot, d);
            } else if (c > 0) {
                final double next = value.getDouble(slot + 1);
                if (Double.isNaN(next) || Double.compare(d, next) < 0) {
                    value.putDouble(slot + 1, d);
                }
            }
        } else if (c > 0) {
            value.putDouble(slot, d);
        }
    }

    private static void updateLong(MapValue value, int slot, long l, boolean isMin) {
        final long current = value.getLong(slot);
        if (current == Numbers.LONG_NULL || (isMin ? l < current : l > current)) {
            value.putLong(slot, l);
        }
    }

    // Phase one: per-worker maps on the workers, merged into the lookup map here.
    private void aggregate(SqlExecutionContext executionContext, int order) throws SqlException {
        final Atom atom = frameSequence.getAtom();
        atom.mode = MODE_AGGREGATE;
        frameSequence.of(snapshotBase, executionContext, order);
        try {
            frameSequence.prepareForDispatch();
            atom.initPools(frameSequence);
            frameSequence.dispatchAndAwait();
            readSymbolCounts();
            for (int i = 0, n = atom.workerMaps.size(); i < n; i++) {
                final OrderedMap workerMap = atom.workerMaps.getQuick(i);
                if (workerMap.isOpen() && workerMap.size() > 0) {
                    lookupMap.merge(workerMap, this::merge);
                }
            }
        } finally {
            frameSequence.await();
            frameSequence.reset();
        }
    }

    private void aggregateFrame(OrderedMap map, RecordSink sink, PageFrameMemoryRecord record, long frameRowCount) {
        final int windowCount = argColumns.length;
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);
            final MapKey key = map.withKey();
            sink.copy(record, key);
            final MapValue value = key.createValue();
            if (value.isNew()) {
                initValue(value);
            }
            for (int i = 0; i < windowCount; i++) {
                final int slot = windowSlots[i];
                switch (argKinds[i]) {
                    case ARG_DOUBLE: {
                        final double d = record.getDouble(argColumns[i]);
                        if (Numbers.isFinite(d)) {
                            updateDouble(value, slot, d, isMin[i]);
                        }
                        break;
                    }
                    case ARG_FLOAT: {
                        final double d = record.getFloat(argColumns[i]);
                        if (Numbers.isFinite(d)) {
                            updateDouble(value, slot, d, isMin[i]);
                        }
                        break;
                    }
                    default: {
                        final long l = readLong(record, i);
                        if (l != Numbers.LONG_NULL) {
                            updateLong(value, slot, l, isMin[i]);
                        }
                        break;
                    }
                }
            }
        }
    }

    /**
     * For a partition key of SYMBOL columns alone, copies the frozen map into an array indexed by
     * the symbol keys, so that a lookup is arithmetic on the row's symbol keys rather than a hash
     * probe. Each key column spans its symbol count plus one slot, for NULL. A window value the map
     * does not hold reads NULL there, as a miss does. Skipped when the product of the spans is
     * large, or sparse next to the map, or a key lies outside its table's symbol count; the lookups
     * then probe the map.
     */
    private void buildDense(MemoryTracker memoryTracker) {
        if (!isAllSymbolKeys || lookupMap.size() == 0) {
            return;
        }
        final long limit = maxDenseSlots >= 0 ? maxDenseSlots : Math.min(MAX_DENSE_SLOTS, 16 * lookupMap.size() + 4096);
        long slots = 1;
        for (int count : symbolCounts) {
            if (count < 0) {
                return;
            }
            slots *= count + 1L;
            if (slots > limit) {
                return;
            }
        }
        final int windowCount = argColumns.length;
        final long size = slots * windowCount * Long.BYTES;
        final long addr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT, memoryTracker);
        denseAddr = addr;
        denseSize = size;
        denseTracker = memoryTracker;
        for (int w = 0; w < windowCount; w++) {
            final long nullBits = argKinds[w] <= ARG_FLOAT ? Double.doubleToRawLongBits(Double.NaN) : Numbers.LONG_NULL;
            Vect.setMemoryLong(addr + w * slots * Long.BYTES, nullBits, slots);
        }
        final MapRecordCursor mapCursor = lookupMap.getCursor();
        final MapRecord mapRecord = mapCursor.getRecord();
        while (mapCursor.hasNext()) {
            long slot = 0;
            for (int k = 0, n = keyColumnArray.length; k < n; k++) {
                final long index = denseIndex(mapRecord.getInt(valueCount + k), symbolCounts[k]);
                if (index < 0) {
                    freeDense();
                    return;
                }
                slot = slot * (symbolCounts[k] + 1L) + index;
            }
            final MapValue value = mapRecord.getValue();
            for (int w = 0; w < windowCount; w++) {
                Unsafe.putLong(addr + (w * slots + slot) * Long.BYTES, value.getLong(windowSlots[w]));
            }
        }
        denseSlots = slots;
        denseBuildCount++;
    }

    // A replay's worker side: the argument values of the replayed keys' rows of one frame, in scan
    // order, appended to the slot's list as [frame index, entry count, entries...], an entry being
    // the key's ordinal followed by the raw bits of each tie window's argument.
    private void collectFrame(Atom atom, int slotId, PageFrameMemoryRecord record, int frameIndex, long frameRowCount) {
        final DirectLongList list = atom.getCollected(slotId);
        final OrderedMap.ProbeView view = atom.getProbeView(slotId);
        final RecordSink sink = atom.getSink(slotId);
        final boolean backward = atom.isBackward;
        long header = -1;
        long entries = 0;
        for (long i = 0; i < frameRowCount; i++) {
            if ((i & (ROWS_PER_OVERFLOW_CHECK - 1)) == 0 && atom.isCollectOverflow) {
                return;
            }
            record.setRowIndex(backward ? frameRowCount - i - 1 : i);
            view.withKey();
            sink.copy(record, view);
            final MapValue value = view.findValue();
            if (value == null) {
                // cannot happen over phase one's snapshot
                continue;
            }
            final long ordinal = value.getLong(ordinalSlot);
            if (ordinal < 0) {
                continue;
            }
            if (header < 0) {
                header = list.size();
                list.add(frameIndex);
                list.add(0);
            }
            list.add(ordinal);
            for (int window : tieWindows) {
                list.add(Double.doubleToRawLongBits(readDouble(record, window)));
            }
            entries++;
            if (list.size() > atom.maxCollectedLongs) {
                atom.isCollectOverflow = true;
                return;
            }
        }
        if (header >= 0) {
            list.set(header + 1, entries);
        }
    }

    // The window's first pass, for one row of a replayed key: the first finite value, then a value
    // the tolerant comparison finds smaller.
    private void fold(long replayAddr, long ordinal, int tie, double d) {
        if (Numbers.isFinite(d)) {
            final long addr = replayAddr + (ordinal * tieWindows.length + tie) * Double.BYTES;
            final double scanMin = Unsafe.getDouble(addr);
            if (Double.isNaN(scanMin) || Numbers.compare(d, scanMin) < 0) {
                Unsafe.putDouble(addr, d);
            }
        }
    }

    // Folds what the workers collected, frame by frame in scan order.
    private void foldCollected(Atom atom, int frameCount, long replayAddr) {
        final IntList frameSlots = atom.frameSlots;
        final LongList frameOffsets = atom.frameOffsets;
        frameSlots.setAll(frameCount, Integer.MIN_VALUE);
        frameOffsets.setAll(frameCount, -1);
        final int entryLongs = 1 + tieWindows.length;
        for (int slotId = -1, n = atom.workerCollected.size(); slotId < n; slotId++) {
            final DirectLongList list = atom.getCollected(slotId);
            for (long pos = 0, size = list.size(); pos < size; ) {
                final int frameIndex = (int) list.get(pos);
                frameSlots.setQuick(frameIndex, slotId);
                frameOffsets.setQuick(frameIndex, pos);
                pos += 2 + list.get(pos + 1) * entryLongs;
            }
        }
        for (int f = 0; f < frameCount; f++) {
            final long offset = frameOffsets.getQuick(f);
            if (offset < 0) {
                continue;
            }
            final DirectLongList list = atom.getCollected(frameSlots.getQuick(f));
            final long entries = list.get(offset + 1);
            for (long e = 0, pos = offset + 2; e < entries; e++, pos += entryLongs) {
                final long ordinal = list.get(pos);
                for (int t = 0; t < tieWindows.length; t++) {
                    fold(replayAddr, ordinal, t, Double.longBitsToDouble(list.get(pos + 1 + t)));
                }
            }
        }
    }

    // The fallback: the query's thread folds the snapshot's frames itself, in scan order.
    private void foldFrames(Atom atom, int frameCount, long replayAddr, SqlExecutionCircuitBreaker circuitBreaker) {
        final PageFrameMemoryPool pool = atom.ownerPool;
        final OrderedMap.ProbeView view = atom.ownerView;
        final RecordSink sink = atom.ownerSink;
        final PageFrameMemoryRecord record = replayRecord;
        long rows = 0;
        for (int f = 0; f < frameCount; f++) {
            try {
                record.init(pool.navigateTo(f, atom.aggregateColumns));
                final long frameRowCount = frameSequence.getFrameRowCount(f);
                for (long i = 0; i < frameRowCount; i++) {
                    if ((++rows & (ROWS_PER_BREAKER_CHECK - 1)) == 0) {
                        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    }
                    record.setRowIndex(atom.isBackward ? frameRowCount - i - 1 : i);
                    view.withKey();
                    sink.copy(record, view);
                    final MapValue value = view.findValue();
                    if (value == null) {
                        continue;
                    }
                    final long ordinal = value.getLong(ordinalSlot);
                    if (ordinal < 0) {
                        continue;
                    }
                    for (int t = 0; t < tieWindows.length; t++) {
                        fold(replayAddr, ordinal, t, readDouble(record, tieWindows[t]));
                    }
                }
            } finally {
                pool.releaseParquetBuffers();
            }
        }
    }

    private void freeDense() {
        if (denseAddr != 0) {
            denseAddr = Unsafe.free(denseAddr, denseSize, MemoryTag.NATIVE_DEFAULT, denseTracker);
            denseSize = 0;
            denseSlots = 0;
            denseTracker = null;
        }
    }

    private void initValue(MapValue value) {
        for (int i = 0, n = argColumns.length; i < n; i++) {
            final int slot = windowSlots[i];
            if (argKinds[i] <= ARG_FLOAT) {
                value.putDouble(slot, Double.NaN);
                if (isTieWindow(i)) {
                    value.putDouble(slot + 1, Double.NaN);
                }
            } else {
                value.putLong(slot, Numbers.LONG_NULL);
            }
        }
        if (ordinalSlot >= 0) {
            value.putLong(ordinalSlot, -1);
        }
    }

    private boolean isTieWindow(int window) {
        return isMin[window] && argKinds[window] <= ARG_FLOAT;
    }

    private void merge(MapValue dest, MapValue src) {
        for (int i = 0, n = argColumns.length; i < n; i++) {
            final int slot = windowSlots[i];
            if (argKinds[i] <= ARG_FLOAT) {
                final double v = src.getDouble(slot);
                if (!Double.isNaN(v)) {
                    updateDouble(dest, slot, v, isMin[i]);
                    if (isTieWindow(i)) {
                        final double next = src.getDouble(slot + 1);
                        if (!Double.isNaN(next)) {
                            updateDouble(dest, slot, next, true);
                        }
                    }
                }
            } else {
                final long v = src.getLong(slot);
                if (v != Numbers.LONG_NULL) {
                    updateLong(dest, slot, v, isMin[i]);
                }
            }
        }
    }

    private double readDouble(Record record, int window) {
        return argKinds[window] == ARG_FLOAT ? record.getFloat(argColumns[window]) : record.getDouble(argColumns[window]);
    }

    private long readLong(Record record, int window) {
        final int column = argColumns[window];
        switch (argKinds[window]) {
            case ARG_TIMESTAMP:
                return record.getTimestamp(column);
            case ARG_DATE:
                return record.getDate(column);
            default:
                return record.getLong(column);
        }
    }

    // The table's symbol counts of the key columns, read while phase one's frame cursor is open,
    // or -1 for a column without a static symbol table.
    private void readSymbolCounts() {
        if (!isAllSymbolKeys) {
            return;
        }
        final SymbolTableSource source = frameSequence.getSymbolTableSource();
        for (int k = 0, n = keyColumnArray.length; k < n; k++) {
            final SymbolTable table = source.getSymbolTable(keyColumnArray[k]);
            symbolCounts[k] = table instanceof StaticSymbolTable st ? st.getSymbolCount() : -1;
        }
    }

    private void releaseLookup() {
        try {
            freeDense();
            lookupMap.close();
        } finally {
            for (int i = 0, n = filters.size(); i < n; i++) {
                filters.getQuick(i).releaseView();
            }
        }
    }

    // Recomputes, in the window's scan order and over the same snapshot, the DOUBLE min of every
    // partition whose two smallest distinct values the tolerance cannot tell apart; see the class
    // docs.
    private void replay(SqlExecutionContext executionContext, int order, MemoryTracker memoryTracker) throws SqlException {
        if (tieWindows.length == 0 || lookupMap.size() == 0) {
            return;
        }
        // number the keys to replay
        long ordinals = 0;
        final MapRecordCursor mapCursor = lookupMap.getCursor();
        final MapRecord mapRecord = mapCursor.getRecord();
        while (mapCursor.hasNext()) {
            final MapValue value = mapRecord.getValue();
            for (int window : tieWindows) {
                final int slot = windowSlots[window];
                if (isAmbiguous(value.getDouble(slot), value.getDouble(slot + 1))) {
                    value.putLong(ordinalSlot, ordinals++);
                    break;
                }
            }
        }
        if (ordinals == 0) {
            return;
        }
        replayRunCount++;
        replayedKeyCount += ordinals;
        final long replaySize = ordinals * tieWindows.length * Double.BYTES;
        final long replayAddr = Unsafe.malloc(replaySize, MemoryTag.NATIVE_DEFAULT, memoryTracker);
        try {
            Vect.setMemoryLong(replayAddr, Double.doubleToRawLongBits(Double.NaN), ordinals * tieWindows.length);
            final Atom atom = frameSequence.getAtom();
            atom.mode = MODE_COLLECT;
            atom.isBackward = order == ORDER_DESC;
            frameSequence.of(snapshotBase, executionContext, order);
            try {
                frameSequence.prepareForDispatch();
                atom.initPools(frameSequence);
                atom.initCollect(lookupMap, Math.max(1, maxReplayLongs / (workerCount + 1)));
                frameSequence.dispatchAndAwait();
                final int frameCount = frameSequence.getFrameCount();
                if (atom.isCollectOverflow) {
                    replayFallbackCount++;
                    foldFrames(atom, frameCount, replayAddr, executionContext.getCircuitBreaker());
                } else {
                    foldCollected(atom, frameCount, replayAddr);
                }
            } finally {
                frameSequence.await();
                frameSequence.reset();
            }
            mapCursor.toTop();
            while (mapCursor.hasNext()) {
                final MapValue value = mapRecord.getValue();
                final long ordinal = value.getLong(ordinalSlot);
                if (ordinal < 0) {
                    continue;
                }
                for (int t = 0; t < tieWindows.length; t++) {
                    final int slot = windowSlots[tieWindows[t]];
                    if (isAmbiguous(value.getDouble(slot), value.getDouble(slot + 1))) {
                        value.putDouble(slot, Unsafe.getDouble(replayAddr + (ordinal * tieWindows.length + t) * Double.BYTES));
                        // settled: the next lookups read the scan-order value
                        value.putDouble(slot + 1, Double.NaN);
                    }
                }
            }
        } finally {
            Unsafe.free(replayAddr, replaySize, MemoryTag.NATIVE_DEFAULT, memoryTracker);
        }
    }

    @Override
    protected void _close() {
        final AsyncFilteredRecordCursorFactory filterFactory = this.filterFactory;
        this.filterFactory = null;
        final UnorderedPageFrameSequence<Atom> frameSequence = this.frameSequence;
        this.frameSequence = null;
        Throwable failure = Misc.freeBestEffort(null, cursor);
        try {
            freeDense();
        } catch (Throwable th) {
            failure = th;
        }
        // the filter factory owns the snapshot, so the base, and the lookup filters
        failure = Misc.freeBestEffort(failure, filterFactory);
        failure = Misc.freeBestEffort(failure, frameSequence);
        failure = Misc.freeBestEffort(failure, lookupMap);
        failure = Misc.freeBestEffort(failure, replayRecord);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * The state of the passes over the base's frames: a map, a key sink, a frame memory pool, a
     * probe view of the lookup map and a list of collected values per slot. The owner's map is the
     * factory's lookup map, which outlives the phase; the workers' maps are freed once merged.
     */
    private class Atom implements StatefulAtom, PerWorkerLockOwner {
        private final IntHashSet aggregateColumns;
        private final AsyncWindowMinMaxFilterRecordCursorFactory factory = AsyncWindowMinMaxFilterRecordCursorFactory.this;
        // a replay's frame index -> the slot and offset of the frame's entries, see foldCollected()
        private final LongList frameOffsets = new LongList();
        private final IntList frameSlots = new IntList();
        private final PerWorkerLocks locks;
        private final DirectLongList ownerCollected;
        private final OrderedMap ownerMap;
        private final PageFrameMemoryPool ownerPool;
        private final RecordSink ownerSink;
        private final OrderedMap.ProbeView ownerView = new OrderedMap.ProbeView();
        private final ObjList<DirectLongList> workerCollected;
        private final ObjList<OrderedMap> workerMaps;
        private final ObjList<PageFrameMemoryPool> workerPools;
        private final ObjList<RecordSink> workerSinks;
        private final ObjList<OrderedMap.ProbeView> workerViews;
        private volatile boolean isCollectOverflow;
        private boolean isBackward;
        private long maxCollectedLongs;
        private MemoryTracker memoryTracker;
        private int mode;

        private Atom(
                CairoConfiguration configuration,
                OrderedMap ownerMap,
                ArrayColumnTypes keyTypes,
                ArrayColumnTypes valueTypes,
                Class<RecordSink> keySinkClass,
                RecordMetadata baseMetadata,
                IntList keyColumns,
                IntHashSet aggregateColumns,
                int workerCount
        ) {
            this.ownerMap = ownerMap;
            this.aggregateColumns = aggregateColumns;
            this.workerMaps = new ObjList<>(workerCount);
            this.workerPools = new ObjList<>(workerCount);
            this.workerSinks = new ObjList<>(workerCount);
            this.workerViews = new ObjList<>(workerCount);
            this.workerCollected = new ObjList<>(workerCount);
            this.ownerCollected = new DirectLongList(256, MemoryTag.NATIVE_DEFAULT, true);
            try {
                this.locks = new PerWorkerLocks(configuration, workerCount);
                this.ownerPool = new PageFrameMemoryPool(configuration);
                this.ownerSink = newKeySink(keySinkClass, baseMetadata, keyColumns);
                for (int i = 0; i < workerCount; i++) {
                    workerMaps.add(newMap(configuration, keyTypes, valueTypes));
                    workerPools.add(new PageFrameMemoryPool(configuration));
                    workerSinks.add(newKeySink(keySinkClass, baseMetadata, keyColumns));
                    workerViews.add(new OrderedMap.ProbeView());
                    workerCollected.add(new DirectLongList(256, MemoryTag.NATIVE_DEFAULT, true));
                }
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void clear() {
            Misc.freeObjListAndKeepObjects(workerMaps);
            Misc.free(ownerPool);
            Misc.freeObjListAndKeepObjects(workerPools);
            Misc.free(ownerView);
            Misc.freeObjListAndKeepObjects(workerViews);
            Misc.free(ownerCollected);
            Misc.freeObjListAndKeepObjects(workerCollected);
            frameSlots.clear();
            frameOffsets.clear();
            memoryTracker = null;
        }

        @Override
        public void close() {
            clear();
        }

        @Override
        public PerWorkerLocks getPerWorkerLocks() {
            return locks;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            memoryTracker = executionContext.getMemoryTracker();
            if (mode == MODE_AGGREGATE) {
                for (int i = 0, n = workerMaps.size(); i < n; i++) {
                    final OrderedMap map = workerMaps.getQuick(i);
                    map.close();
                    map.setMemoryTracker(memoryTracker);
                    map.reopen();
                }
            }
        }

        DirectLongList getCollected(int slotId) {
            return slotId == -1 ? ownerCollected : workerCollected.getQuick(slotId);
        }

        OrderedMap getMap(int slotId) {
            return slotId == -1 ? ownerMap : workerMaps.getQuick(slotId);
        }

        PageFrameMemoryPool getPool(int slotId) {
            return slotId == -1 ? ownerPool : workerPools.getQuick(slotId);
        }

        OrderedMap.ProbeView getProbeView(int slotId) {
            return slotId == -1 ? ownerView : workerViews.getQuick(slotId);
        }

        RecordSink getSink(int slotId) {
            return slotId == -1 ? ownerSink : workerSinks.getQuick(slotId);
        }

        // binds the slots' probe views to the frozen lookup map and empties their lists
        void initCollect(OrderedMap lookupMap, long maxCollectedLongs) {
            this.maxCollectedLongs = maxCollectedLongs;
            isCollectOverflow = false;
            bindCollect(ownerView, ownerCollected, lookupMap);
            for (int i = 0, n = workerViews.size(); i < n; i++) {
                bindCollect(workerViews.getQuick(i), workerCollected.getQuick(i), lookupMap);
            }
        }

        void initPools(UnorderedPageFrameSequence<Atom> frameSequence) {
            ownerPool.setMemoryTracker(memoryTracker);
            ownerPool.of(frameSequence.getPageFrameAddressCache());
            for (int i = 0, n = workerPools.size(); i < n; i++) {
                final PageFrameMemoryPool pool = workerPools.getQuick(i);
                pool.setMemoryTracker(memoryTracker);
                pool.of(frameSequence.getPageFrameAddressCache());
            }
        }

        int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
            if (workerId == -1 && owner) {
                return -1;
            }
            return locks.acquireSlot(workerId, circuitBreaker);
        }

        void release(int slotId) {
            if (slotId != -1) {
                locks.releaseSlot(slotId);
            }
        }

        private void bindCollect(OrderedMap.ProbeView view, DirectLongList list, OrderedMap lookupMap) {
            view.setMemoryTracker(memoryTracker);
            view.of(lookupMap);
            list.setMemoryTracker(memoryTracker);
            list.reopen();
            list.clear();
        }
    }

    /**
     * The base as every pass reads it: through the one page frame cursor {@link #open} takes, which
     * each pass rewinds and none closes. The table's version stays the one the open saw until
     * {@link #release}. Owns the base.
     */
    private static class SnapshotBase extends AbstractRecordCursorFactory {
        private final RecordCursorFactory base;
        private final SharedFrameCursor sharedCursor = new SharedFrameCursor();
        private PageFrameCursor frameCursor;
        private int order;

        private SnapshotBase(RecordCursorFactory base) {
            super(base.getMetadata());
            this.base = base;
        }

        @Override
        public void changePageFrameSizes(int minRows, int maxRows) {
            base.changePageFrameSizes(minRows, maxRows);
        }

        @Override
        public boolean followedOrderByAdvice() {
            return base.followedOrderByAdvice();
        }

        @Override
        public RecordCursorFactory getBaseFactory() {
            return base;
        }

        @Override
        public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) {
            if (frameCursor == null || order != this.order) {
                throw CairoException.critical(0).put("no snapshot is open [order=").put(order).put(']');
            }
            frameCursor.toTop();
            sharedCursor.of(frameCursor);
            return sharedCursor;
        }

        @Override
        public int getPageFrameScanDirection() {
            return base.getPageFrameScanDirection();
        }

        @Override
        public int getScanDirection() {
            return base.getScanDirection();
        }

        @Override
        public TableToken getTableToken() {
            return base.getTableToken();
        }

        @Override
        public boolean isNonDeterministic() {
            return base.isNonDeterministic();
        }

        @Override
        public boolean isStableWithinExecution() {
            return base.isStableWithinExecution();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return base.recordCursorSupportsRandomAccess();
        }

        @Override
        public boolean supportsPageFrameCursor() {
            return true;
        }

        @Override
        public boolean supportsUpdateRowId(TableToken tableToken) {
            return base.supportsUpdateRowId(tableToken);
        }

        @Override
        public void toPlan(PlanSink sink) {
            base.toPlan(sink);
        }

        @Override
        public boolean usesIndex() {
            return base.usesIndex();
        }

        void open(SqlExecutionContext executionContext, int order) throws SqlException {
            assert frameCursor == null;
            frameCursor = base.getPageFrameCursor(executionContext, order);
            this.order = order;
        }

        void release(@Nullable Throwable th) {
            final PageFrameCursor frameCursor = this.frameCursor;
            this.frameCursor = null;
            sharedCursor.of(null);
            if (th != null) {
                Misc.free(frameCursor, th);
            } else {
                Misc.free(frameCursor);
            }
        }

        @Override
        protected void _close() {
            try {
                release(null);
            } finally {
                Misc.free(base);
            }
        }
    }

    /**
     * A pass's view of the snapshot's page frame cursor: closing it leaves the cursor open.
     */
    private static class SharedFrameCursor implements PageFrameCursor {
        private PageFrameCursor delegate;

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
            delegate.calculateSize(counter);
        }

        @Override
        public void close() {
            // the snapshot closes the cursor
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return delegate.getColumnMapping();
        }

        @Override
        public long getRemainingRowsInInterval() {
            return delegate.getRemainingRowsInInterval();
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return delegate.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasActivePushdownFilter() {
            return delegate.hasActivePushdownFilter();
        }

        @Override
        public boolean isExternal() {
            return delegate.isExternal();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return delegate.newSymbolTable(columnIndex);
        }

        @Override
        public @Nullable PageFrame next(long skipTarget) {
            return delegate.next(skipTarget);
        }

        @Override
        public void releaseOpenPartitions() {
            delegate.releaseOpenPartitions();
        }

        @Override
        public void resumeTimer() {
            delegate.resumeTimer();
        }

        @Override
        public void setScanProfile(ReaderScanProfile profile) {
            delegate.setScanProfile(profile);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public boolean supportsSizeCalculation() {
            return delegate.supportsSizeCalculation();
        }

        @Override
        public void suspendTimer() {
            delegate.suspendTimer();
        }

        @Override
        public void toTop() {
            delegate.toTop();
        }

        void of(PageFrameCursor delegate) {
            this.delegate = delegate;
        }
    }

    static RecordSink newKeySink(Class<RecordSink> keySinkClass, RecordMetadata baseMetadata, IntList keyColumns) {
        final io.questdb.cairo.ListColumnFilter filter = new io.questdb.cairo.ListColumnFilter();
        for (int i = 0, n = keyColumns.size(); i < n; i++) {
            filter.add(keyColumns.getQuick(i) + 1);
        }
        return io.questdb.cairo.RecordSinkFactory.getInstance(keySinkClass, baseMetadata, filter, null, null, null, null, null);
    }

    /**
     * The window values of the current base row, looked up by its partition key. Window
     * {@code i} is column {@code i}. A row whose partition has no counted value reads NULL, as the
     * window's second pass does.
     */
    private class LookupRecord implements Record {
        private final RecordSink keySink;
        private final OrderedMap.ProbeView view = new OrderedMap.ProbeView();
        private Record base;

        private LookupRecord(RecordSink keySink) {
            this.keySink = keySink;
        }

        @Override
        public long getDate(int col) {
            return getLong(col);
        }

        @Override
        public double getDouble(int col) {
            if (denseAddr != 0) {
                final long slot = denseSlot();
                if (slot >= 0) {
                    return Unsafe.getDouble(denseAddr + (col * denseSlots + slot) * Long.BYTES);
                }
            }
            final MapValue value = find();
            return value != null ? value.getDouble(windowSlots[col]) : Double.NaN;
        }

        @Override
        public long getLong(int col) {
            if (denseAddr != 0) {
                final long slot = denseSlot();
                if (slot >= 0) {
                    return Unsafe.getLong(denseAddr + (col * denseSlots + slot) * Long.BYTES);
                }
            }
            final MapValue value = find();
            return value != null ? value.getLong(windowSlots[col]) : Numbers.LONG_NULL;
        }

        // The row's slot of the dense lookup, or -1 when a key lies outside it.
        private long denseSlot() {
            long slot = 0;
            for (int k = 0, n = keyColumnArray.length; k < n; k++) {
                final int count = symbolCounts[k];
                final long index = denseIndex(base.getInt(keyColumnArray[k]), count);
                if (index < 0) {
                    return -1;
                }
                slot = slot * (count + 1L) + index;
            }
            return slot;
        }

        @Override
        public long getTimestamp(int col) {
            return getLong(col);
        }

        private MapValue find() {
            view.withKey();
            keySink.copy(base, view);
            return view.findValue();
        }

        void bind(MemoryTracker memoryTracker) {
            view.setMemoryTracker(memoryTracker);
            view.of(lookupMap);
        }

        void of(Record base) {
            this.base = base;
        }
    }

    /**
     * Phase two's filter: the query's filter, compiled against the window's metadata, over the base
     * row joined with its window values. One per slot; each holds its own probe view.
     */
    private static class LookupFilter extends BooleanFunction {
        private final AsyncWindowMinMaxFilterRecordCursorFactory factory;
        private final Function inner;
        private final boolean ownsInner;
        private final LookupRecord lookupRecord;
        private final JoinRecord joinRecord;
        private final SelectedRecord record;
        private final RemappedSymbolTableSource symbolTableSource;

        private LookupFilter(
                AsyncWindowMinMaxFilterRecordCursorFactory factory,
                Function inner,
                boolean ownsInner,
                Class<RecordSink> keySinkClass,
                IntList keyColumns
        ) {
            this.factory = factory;
            this.inner = inner;
            this.ownsInner = ownsInner;
            this.lookupRecord = factory.new LookupRecord(newKeySink(keySinkClass, factory.base.getMetadata(), keyColumns));
            this.joinRecord = new JoinRecord(factory.baseColumnCount);
            this.record = new SelectedRecord(factory.crossIndex);
            this.record.of(joinRecord);
            this.symbolTableSource = new RemappedSymbolTableSource(factory.crossIndex, factory.baseColumnCount);
        }

        @Override
        public void close() {
            releaseView();
            if (ownsInner) {
                Misc.free(inner);
            }
        }

        @Override
        public void cursorClosed() {
            if (ownsInner) {
                inner.cursorClosed();
            }
        }

        @Override
        public boolean getBool(Record rec) {
            lookupRecord.of(rec);
            joinRecord.of(rec, lookupRecord);
            return inner.getBool(record);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            // phase one has frozen the lookup map by now
            lookupRecord.bind(executionContext.getMemoryTracker());
            if (ownsInner) {
                this.symbolTableSource.of(symbolTableSource);
                inner.init(this.symbolTableSource, executionContext);
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof LookupFilter other && other.ownsInner && ownsInner) {
                inner.offerStateTo(other.inner);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(inner);
        }

        @Override
        public void toTop() {
            if (ownsInner) {
                inner.toTop();
            }
        }

        void releaseView() {
            lookupRecord.view.close();
        }
    }

    private static class RemappedSymbolTableSource implements SymbolTableSource {
        private final int baseColumnCount;
        private final IntList crossIndex;
        private SymbolTableSource base;

        private RemappedSymbolTableSource(IntList crossIndex, int baseColumnCount) {
            this.crossIndex = crossIndex;
            this.baseColumnCount = baseColumnCount;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            final int baseIndex = crossIndex.getQuick(columnIndex);
            return baseIndex < baseColumnCount ? base.getSymbolTable(baseIndex) : null;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            final int baseIndex = crossIndex.getQuick(columnIndex);
            return baseIndex < baseColumnCount ? base.newSymbolTable(baseIndex) : null;
        }

        void of(SymbolTableSource base) {
            this.base = base;
        }
    }

    private class MinMaxCursor implements RecordCursor {
        private final LookupRecord lookupA;
        private final LookupRecord lookupB;
        private final JoinRecord joinA;
        private final JoinRecord joinB;
        private final SelectedRecord recordA;
        private final SelectedRecord recordB;
        private RecordCursor baseCursor;
        // the base cursor's own record, which the record reads but while a block's record is read
        private Record baseRecordA;
        private Record baseRecordB;
        private MinMaxBlock block;
        private boolean isOpen;
        private MemoryTracker memoryTracker;

        private MinMaxCursor(Class<RecordSink> keySinkClass, IntList keyColumns) {
            final RecordMetadata baseMetadata = base.getMetadata();
            this.lookupA = new LookupRecord(newKeySink(keySinkClass, baseMetadata, keyColumns));
            this.lookupB = new LookupRecord(newKeySink(keySinkClass, baseMetadata, keyColumns));
            this.joinA = new JoinRecord(baseColumnCount);
            this.joinB = new JoinRecord(baseColumnCount);
            this.recordA = new SelectedRecord(crossIndex);
            this.recordB = new SelectedRecord(crossIndex);
            recordA.of(joinA);
            recordB.of(joinB);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            baseCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                try {
                    // drains the workers before the map they probe goes
                    baseCursor = Misc.free(baseCursor);
                } finally {
                    baseRecordB = null;
                    lookupA.view.close();
                    lookupB.view.close();
                    try {
                        releaseLookup();
                    } finally {
                        // the filter's frames are gone with its cursor
                        snapshotBase.release(null);
                    }
                }
            }
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public Record getRecordB() {
            if (baseRecordB == null) {
                baseRecordB = baseCursor.getRecordB();
                lookupB.bind(memoryTracker);
                lookupB.of(baseRecordB);
                joinB.of(baseRecordB, lookupB);
            }
            return recordB;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            final int baseIndex = crossIndex.getQuick(columnIndex);
            return baseIndex < baseColumnCount ? baseCursor.getSymbolTable(baseIndex) : null;
        }

        @Override
        public boolean hasNext() {
            if (lookupA.base != baseRecordA) {
                pointAt(baseRecordA);
            }
            return baseCursor.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            final int baseIndex = crossIndex.getQuick(columnIndex);
            return baseIndex < baseColumnCount ? baseCursor.newSymbolTable(baseIndex) : null;
        }

        /**
         * The base's block: a base column reads the base block's memory, a window column the
         * lookup, through {@link RecordBlock#getRecordAt}.
         */
        @Override
        public RecordBlock peekRecordBlock(int maxRows) {
            final RecordBlock baseBlock = baseCursor.peekRecordBlock(maxRows);
            if (baseBlock == null) {
                return null;
            }
            if (block == null) {
                block = new MinMaxBlock();
            }
            block.base = baseBlock;
            return block;
        }

        @Override
        public long preComputedStateSize() {
            return baseCursor.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            if (record == recordB) {
                baseCursor.recordAt(baseRecordB, atRowId);
            } else {
                baseCursor.recordAt(baseCursor.getRecord(), atRowId);
            }
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public void skipRecordBlock(int rowCount) {
            if (lookupA.base != baseRecordA) {
                pointAt(baseRecordA);
            }
            baseCursor.skipRecordBlock(rowCount);
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            baseCursor.skipRows(rowCount, maxRowsAfterSkip);
        }

        @Override
        public boolean supportsRecordBlocks() {
            return baseCursor.supportsRecordBlocks();
        }

        @Override
        public void toTop() {
            if (lookupA.base != baseRecordA) {
                pointAt(baseRecordA);
            }
            baseCursor.toTop();
        }

        private void pointAt(Record baseRecord) {
            lookupA.of(baseRecord);
            joinA.of(baseRecord, lookupA);
        }

        void of(RecordCursor baseCursor, MemoryTracker memoryTracker) {
            this.baseCursor = baseCursor;
            this.memoryTracker = memoryTracker;
            this.isOpen = true;
            baseRecordA = baseCursor.getRecord();
            lookupA.bind(memoryTracker);
            pointAt(baseRecordA);
            baseRecordB = null;
        }

        private class MinMaxBlock implements RecordBlock {
            private RecordBlock base;

            @Override
            public long getColumnAddress(int columnIndex) {
                final int baseIndex = crossIndex.getQuick(columnIndex);
                return baseIndex < baseColumnCount ? base.getColumnAddress(baseIndex) : 0;
            }

            @Override
            public long getColumnRowIndexesAddress(int columnIndex) {
                final int baseIndex = crossIndex.getQuick(columnIndex);
                return baseIndex < baseColumnCount ? base.getColumnRowIndexesAddress(baseIndex) : 0;
            }

            @Override
            public long getColumnStride(int columnIndex) {
                final int baseIndex = crossIndex.getQuick(columnIndex);
                return baseIndex < baseColumnCount ? base.getColumnStride(baseIndex) : 0;
            }

            @Override
            public Record getRecordAt(int row) {
                final Record baseRecord = base.getRecordAt(row);
                if (baseRecord != lookupA.base) {
                    // a base block record that is not the base cursor's own: the window columns
                    // look up its key, until the cursor moves
                    pointAt(baseRecord);
                }
                return recordA;
            }

            @Override
            public int getRowCount() {
                return base.getRowCount();
            }

            @Override
            public long getRowIndexesAddress() {
                return base.getRowIndexesAddress();
            }
        }
    }
}

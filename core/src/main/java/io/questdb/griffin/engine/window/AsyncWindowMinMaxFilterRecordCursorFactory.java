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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
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
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
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
 * The aggregation mirrors the window functions' first pass, see {@link WholePartitionMinMax}.
 * Every case but one is independent of row order. A DOUBLE {@code min} compares with a tolerance,
 * so when a partition holds two distinct values within {@code Numbers.DOUBLE_TOLERANCE} of its
 * smallest, the window's value is the first of them in scan order. The workers track each
 * partition's two smallest distinct values; where they are that close, the query's thread replays
 * the window's comparison in scan order over the base, for those partitions only.
 */
public class AsyncWindowMinMaxFilterRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int ARG_DATE = 4;
    public static final int ARG_DOUBLE = 0;
    public static final int ARG_FLOAT = 1;
    public static final int ARG_LONG = 2;
    public static final int ARG_TIMESTAMP = 3;
    private static final UnorderedPageFrameReducer AGGREGATE = AsyncWindowMinMaxFilterRecordCursorFactory::aggregate;
    // Per window: the value, the next distinct value above the smallest (DOUBLE min only), and the
    // scan-order value a replay computes.
    private static final int SLOTS = 3;
    private static final int ROWS_PER_BREAKER_CHECK = 64 * 1024;
    private final int[] argColumns;
    private final int[] argKinds;
    private final RecordCursorFactory base;
    private final int baseColumnCount;
    private final IntList crossIndex;
    private final MinMaxCursor cursor;
    private final ObjList<LookupFilter> filters = new ObjList<>();
    private final boolean[] isMin;
    private final OrderedMap lookupMap;
    private final ObjList<CharSequence> windowPlans;
    private final int workerCount;
    private AsyncFilteredRecordCursorFactory filterFactory;
    private UnorderedPageFrameSequence<Atom> frameSequence;
    private long replayedKeyCount;
    private long replayRunCount;

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
        this.baseColumnCount = base.getMetadata().getColumnCount();
        this.crossIndex = crossIndex;
        this.argColumns = argColumns;
        this.argKinds = argKinds;
        this.isMin = isMin;
        this.windowPlans = windowPlans;
        this.workerCount = workerCount;
        final int windowCount = argColumns.length;
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        for (int i = 0; i < windowCount; i++) {
            final int type = argKinds[i] <= ARG_FLOAT ? ColumnType.DOUBLE : ColumnType.LONG;
            for (int s = 0; s < SLOTS; s++) {
                valueTypes.add(type);
            }
        }
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
            frameSequence = new UnorderedPageFrameSequence<>(engine, configuration, messageBus, atomToTransfer, AGGREGATE, workerCount);

            final int slotCount = perWorkerFilters != null ? perWorkerFilters.size() : workerCount;
            final LookupFilter ownerFilter = new LookupFilter(this, filter, true, keySinkClass, keyTypes, keyColumns);
            filters.add(ownerFilter);
            final ObjList<Function> workerFilters = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                final Function inner = perWorkerFilters != null ? perWorkerFilters.getQuick(i) : filter;
                final LookupFilter workerFilter = new LookupFilter(this, inner, perWorkerFilters != null, keySinkClass, keyTypes, keyColumns);
                if (perWorkerFilters != null) {
                    perWorkerFilters.setQuick(i, null);
                }
                filters.add(workerFilter);
                workerFilters.add(workerFilter);
            }
            ownsWorkerFilters = false;
            filterFactory = new AsyncFilteredRecordCursorFactory(
                    engine,
                    configuration,
                    messageBus,
                    base,
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
            this.cursor = new MinMaxCursor(keySinkClass, keyTypes, keyColumns, windowCount);
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
                Misc.free(base, th);
            } else {
                Misc.free(filterFactory, th);
            }
            Misc.free(frameSequence, th);
            Misc.free(atom, th);
            Misc.free(lookupMap, th);
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

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
        try {
            lookupMap.close();
            lookupMap.setMemoryTracker(memoryTracker);
            lookupMap.reopen();
            aggregate(executionContext);
            replay(executionContext);
            final RecordCursor filterCursor = filterFactory.getCursor(executionContext);
            cursor.of(filterCursor, memoryTracker);
            return cursor;
        } catch (Throwable th) {
            if (cursor.isOpen) {
                // releases the lookup too
                Misc.free(cursor, th);
            } else {
                try {
                    releaseLookup();
                } catch (Throwable cleanupFailure) {
                    th.addSuppressed(cleanupFailure);
                }
            }
            throw th;
        }
    }

    /** Slots held across both phases; zero whenever no task runs. */
    @TestOnly
    public int getAcquiredSlotCount() {
        final PerWorkerLocks filterLocks = filterFactory.getAtom().getPerWorkerLocks();
        return frameSequence.getAtom().locks.getAcquiredSlotCount() + (filterLocks != null ? filterLocks.getAcquiredSlotCount() : 0);
    }

    /** Merges one worker's values for a key into another's, as phase one does. */
    @TestOnly
    public void mergeForTesting(MapValue dest, MapValue src) {
        merge(dest, src);
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

    @Override
    public boolean isNonDeterministic() {
        return filterFactory.isNonDeterministic();
    }

    @Override
    public boolean isStableWithinExecution() {
        return filterFactory.isStableWithinExecution();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
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

    private static void aggregate(
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
            final PageFrameMemory frameMemory = pool.navigateTo(frameIndex, atom.aggregateColumns);
            record.init(frameMemory);
            atom.factory.aggregateFrame(atom.getMap(slotId), atom.getSink(slotId), record, frameRowCount);
        } finally {
            try {
                pool.releaseParquetBuffers();
            } finally {
                atom.release(slotId);
            }
        }
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
    private void aggregate(SqlExecutionContext executionContext) throws SqlException {
        final int order = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        frameSequence.of(base, executionContext, order);
        try {
            frameSequence.prepareForDispatch();
            frameSequence.getAtom().initPools(frameSequence);
            frameSequence.dispatchAndAwait();
            final Atom atom = frameSequence.getAtom();
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
                final int slot = i * SLOTS;
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

    private void initValue(MapValue value) {
        for (int i = 0, n = argColumns.length; i < n; i++) {
            final int slot = i * SLOTS;
            if (argKinds[i] <= ARG_FLOAT) {
                value.putDouble(slot, Double.NaN);
                value.putDouble(slot + 1, Double.NaN);
                value.putDouble(slot + 2, Double.NaN);
            } else {
                value.putLong(slot, Numbers.LONG_NULL);
                value.putLong(slot + 1, Numbers.LONG_NULL);
                value.putLong(slot + 2, Numbers.LONG_NULL);
            }
        }
    }

    private void merge(MapValue dest, MapValue src) {
        for (int i = 0, n = argColumns.length; i < n; i++) {
            final int slot = i * SLOTS;
            if (argKinds[i] <= ARG_FLOAT) {
                final double v = src.getDouble(slot);
                if (!Double.isNaN(v)) {
                    updateDouble(dest, slot, v, isMin[i]);
                    final double next = src.getDouble(slot + 1);
                    if (!Double.isNaN(next)) {
                        updateDouble(dest, slot, next, isMin[i]);
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

    private void releaseLookup() {
        try {
            lookupMap.close();
        } finally {
            for (int i = 0, n = filters.size(); i < n; i++) {
                filters.getQuick(i).releaseView();
            }
        }
    }

    // Recomputes, in the window's scan order, the DOUBLE min of every partition whose two smallest
    // distinct values the tolerance cannot tell apart; see the class docs.
    private void replay(SqlExecutionContext executionContext) throws SqlException {
        boolean hasNearTie = false;
        for (int i = 0, n = argColumns.length; i < n && !hasNearTie; i++) {
            hasNearTie = isMin[i] && argKinds[i] <= ARG_FLOAT;
        }
        if (!hasNearTie || lookupMap.size() == 0) {
            return;
        }
        long ambiguousKeys = 0;
        final MapRecordCursor mapCursor = lookupMap.getCursor();
        final MapRecord mapRecord = mapCursor.getRecord();
        while (mapCursor.hasNext()) {
            final MapValue value = mapRecord.getValue();
            for (int i = 0, n = argColumns.length; i < n; i++) {
                final int slot = i * SLOTS;
                if (isMin[i] && argKinds[i] <= ARG_FLOAT && isAmbiguous(value.getDouble(slot), value.getDouble(slot + 1))) {
                    ambiguousKeys++;
                    break;
                }
            }
        }
        if (ambiguousKeys == 0) {
            return;
        }
        replayRunCount++;
        replayedKeyCount += ambiguousKeys;
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        final RecordSink sink = cursor.ownerSink;
        try (RecordCursor baseCursor = base.getCursor(executionContext)) {
            final Record record = baseCursor.getRecord();
            long rows = 0;
            while (baseCursor.hasNext()) {
                if ((++rows & (ROWS_PER_BREAKER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                final MapKey key = lookupMap.withKey();
                sink.copy(record, key);
                final MapValue value = key.findValue();
                if (value == null) {
                    continue;
                }
                for (int i = 0, n = argColumns.length; i < n; i++) {
                    final int slot = i * SLOTS;
                    if (!isMin[i] || argKinds[i] > ARG_FLOAT || !isAmbiguous(value.getDouble(slot), value.getDouble(slot + 1))) {
                        continue;
                    }
                    final double d = argKinds[i] == ARG_FLOAT ? record.getFloat(argColumns[i]) : record.getDouble(argColumns[i]);
                    if (!Numbers.isFinite(d)) {
                        continue;
                    }
                    // the window's first pass: the first value, then a value the tolerant
                    // comparison finds smaller
                    final double scanMin = value.getDouble(slot + 2);
                    if (Double.isNaN(scanMin) || Numbers.compare(d, scanMin) < 0) {
                        value.putDouble(slot + 2, d);
                    }
                }
            }
        }
        mapCursor.toTop();
        while (mapCursor.hasNext()) {
            final MapValue value = mapRecord.getValue();
            for (int i = 0, n = argColumns.length; i < n; i++) {
                final int slot = i * SLOTS;
                if (isMin[i] && argKinds[i] <= ARG_FLOAT && isAmbiguous(value.getDouble(slot), value.getDouble(slot + 1))) {
                    value.putDouble(slot, value.getDouble(slot + 2));
                    // settled: the next lookups read the scan-order value
                    value.putDouble(slot + 1, Double.NaN);
                }
            }
        }
    }

    @Override
    protected void _close() {
        final AsyncFilteredRecordCursorFactory filterFactory = this.filterFactory;
        this.filterFactory = null;
        final UnorderedPageFrameSequence<Atom> frameSequence = this.frameSequence;
        this.frameSequence = null;
        Throwable failure = Misc.freeBestEffort(null, cursor);
        // the filter factory owns the base and the lookup filters
        failure = Misc.freeBestEffort(failure, filterFactory);
        failure = Misc.freeBestEffort(failure, frameSequence);
        failure = Misc.freeBestEffort(failure, lookupMap);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Phase one's state: a map, a key sink and a frame memory pool per slot. The owner's map is the
     * factory's lookup map, which outlives the phase; the workers' maps are freed once merged.
     */
    private class Atom implements StatefulAtom, PerWorkerLockOwner {
        private final IntHashSet aggregateColumns;
        private final AsyncWindowMinMaxFilterRecordCursorFactory factory = AsyncWindowMinMaxFilterRecordCursorFactory.this;
        private final PerWorkerLocks locks;
        private final OrderedMap ownerMap;
        private final PageFrameMemoryPool ownerPool;
        private final RecordSink ownerSink;
        private final ObjList<OrderedMap> workerMaps;
        private final ObjList<PageFrameMemoryPool> workerPools;
        private final ObjList<RecordSink> workerSinks;
        private MemoryTracker memoryTracker;

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
            try {
                this.locks = new PerWorkerLocks(configuration, workerCount);
                this.ownerPool = new PageFrameMemoryPool(configuration);
                this.ownerSink = newKeySink(keySinkClass, baseMetadata, keyColumns);
                for (int i = 0; i < workerCount; i++) {
                    workerMaps.add(newMap(configuration, keyTypes, valueTypes));
                    workerPools.add(new PageFrameMemoryPool(configuration));
                    workerSinks.add(newKeySink(keySinkClass, baseMetadata, keyColumns));
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
            for (int i = 0, n = workerMaps.size(); i < n; i++) {
                final OrderedMap map = workerMaps.getQuick(i);
                map.close();
                map.setMemoryTracker(memoryTracker);
                map.reopen();
            }
        }

        OrderedMap getMap(int slotId) {
            return slotId == -1 ? ownerMap : workerMaps.getQuick(slotId);
        }

        PageFrameMemoryPool getPool(int slotId) {
            return slotId == -1 ? ownerPool : workerPools.getQuick(slotId);
        }

        RecordSink getSink(int slotId) {
            return slotId == -1 ? ownerSink : workerSinks.getQuick(slotId);
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
            final MapValue value = find();
            return value != null ? value.getDouble(col * SLOTS) : Double.NaN;
        }

        @Override
        public long getLong(int col) {
            final MapValue value = find();
            return value != null ? value.getLong(col * SLOTS) : Numbers.LONG_NULL;
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
                ArrayColumnTypes keyTypes,
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
        private final RecordSink ownerSink;
        private final SelectedRecord recordA;
        private final SelectedRecord recordB;
        private RecordCursor baseCursor;
        private Record baseRecordB;
        private boolean isOpen;
        private MemoryTracker memoryTracker;

        private MinMaxCursor(Class<RecordSink> keySinkClass, ArrayColumnTypes keyTypes, IntList keyColumns, int windowCount) {
            final RecordMetadata baseMetadata = base.getMetadata();
            this.ownerSink = newKeySink(keySinkClass, baseMetadata, keyColumns);
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
                    releaseLookup();
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
            return baseCursor.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            final int baseIndex = crossIndex.getQuick(columnIndex);
            return baseIndex < baseColumnCount ? baseCursor.newSymbolTable(baseIndex) : null;
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
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            baseCursor.skipRows(rowCount, maxRowsAfterSkip);
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
        }

        void of(RecordCursor baseCursor, MemoryTracker memoryTracker) {
            this.baseCursor = baseCursor;
            this.memoryTracker = memoryTracker;
            this.isOpen = true;
            final Record baseRecord = baseCursor.getRecord();
            lookupA.bind(memoryTracker);
            lookupA.of(baseRecord);
            joinA.of(baseRecord, lookupA);
            baseRecordB = null;
        }
    }
}

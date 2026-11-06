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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.RecordValueSink;
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS;
import static io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS_SHR;

public final class StreamGroupByMap implements QuietCloseable {
    // A reduce task re-arms its breaker, so a count-throttled check there probes the client connection once per frame.
    public static final int BREAKER_CHECK_MASK = 4095;
    private static final long SHARD_PAGE_SIZE = 4 * 1024;
    // A map shrinks once its capacity exceeds this many slots per key the segment needs.
    public static final int SHRINK_RATIO = 8;
    private final DirectLongList batchRows;
    private final int batchSize;
    private final IntList columnCrossIndex;
    private final CairoConfiguration configuration;
    private final ObjList<GroupByFunction> functions;
    private final RecordSink inputSink;
    private final Class<RecordSink> inputSinkClass;
    // Span maps shard once they pass the sharding threshold, so that their owner merges them in parallel.
    private final boolean isShardable;
    private final Map map;
    private final ArrayColumnTypes mapKeyTypes;
    private final ArrayColumnTypes mapValueTypes;
    private final DirectLongList mergeMemory;
    private final RecordSink mergeSink;
    private final Class<RecordSink> mergeSinkClass;
    // The most keys a map presizes for, 0 when presizing is off.
    private final long presizeMaxKeys;
    private final SegmentRecord record;
    private final int segmentIndex;
    private final int segmentType;
    private final ObjList<Map> shards = new ObjList<>();
    private final int shardingThreshold;
    private final RecordSink stateSink;
    private final Class<RecordSink> stateSinkClass;
    private final ArrayColumnTypes stateTypes;
    private final GroupByFunctionsUpdater updater;
    private final FlyweightPackedMapValue value;
    private final RecordValueSink valueSink;
    private int initialKeyCapacity;
    private boolean isSharded;
    private MemoryTracker memoryTracker;
    // The most keys a segment held since the map opened.
    private long peakKeys;
    // The keys the first segment after a reopen presizes the map for.
    private long presizeKeys;
    // The most keys a batch of the current segment reserved map capacity for.
    private long reservedKeys;
    private long segmentKey;
    // The most keys a shard of the last merged segment held, so that the next shard() presizes the shards the merge freed.
    private int shardKeyCapacity;

    public StreamGroupByMap(
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            ColumnTypes baseTypes,
            ListColumnFilter columnFilter,
            ColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            int streamKeyIndex,
            ObjList<Function> keyFunctions,
            ObjList<GroupByFunction> functions,
            GroupByFunctionsUpdater updater,
            @Nullable StreamGroupByMap sinkSource
    ) {
        this.configuration = configuration;
        this.functions = functions;
        this.updater = updater;
        isShardable = false;
        shardingThreshold = 0;
        segmentIndex = valueTypes.getColumnCount();
        segmentType = keyTypes.getColumnType(streamKeyIndex);
        batchSize = configuration.getGroupByBatchSize();
        presizeMaxKeys = configuration.isGroupByPresizeEnabled() ? configuration.getGroupByPresizeMaxCapacity() : 0;
        try {
            mapKeyTypes = new ArrayColumnTypes();
            mapValueTypes = new ArrayColumnTypes().addAll(valueTypes);
            final ListColumnFilter inputColumns = new ListColumnFilter();
            final ObjList<Function> inputFunctions = new ObjList<>();
            columnCrossIndex = new IntList();
            for (int i = 0; i < segmentIndex; i++) {
                columnCrossIndex.add(i);
            }
            for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
                if (i == streamKeyIndex) {
                    columnCrossIndex.add(0);
                } else {
                    columnCrossIndex.add(segmentIndex + mapKeyTypes.getColumnCount());
                    mapKeyTypes.add(keyTypes.getColumnType(i));
                    if (i < columnFilter.getColumnCount()) {
                        inputColumns.add(columnFilter.getColumnIndex(i));
                    } else {
                        inputFunctions.add(keyFunctions.getQuick(i - columnFilter.getColumnCount()));
                    }
                }
            }
            final ArrayColumnTypes rowTypes = new ArrayColumnTypes().addAll(mapValueTypes).addAll(mapKeyTypes);
            stateTypes = new ArrayColumnTypes().addAll(rowTypes).add(segmentType);
            final ListColumnFilter rowColumns = new ListColumnFilter();
            final ListColumnFilter mergeColumns = new ListColumnFilter();
            final ListColumnFilter valueColumns = new ListColumnFilter();
            for (int i = 0, n = rowTypes.getColumnCount(); i < n; i++) {
                rowColumns.add(i + 1);
                if (i < segmentIndex) {
                    valueColumns.add(i + 1);
                } else {
                    mergeColumns.add(i + 1);
                }
            }
            // A shared class keeps the call sites monomorphic. A record sink has decimal scratch state, so instances stay per worker.
            if (sinkSource == null) {
                inputSinkClass = RecordSinkFactory.getInstanceClass(configuration, asm, baseTypes, inputColumns, inputFunctions, null, null, null, null);
                mergeSinkClass = RecordSinkFactory.getInstanceClass(configuration, asm, stateTypes, mergeColumns, null, null, null, null, null);
                stateSinkClass = RecordSinkFactory.getInstanceClass(configuration, asm, rowTypes, rowColumns, null, null, null, null, null);
                valueSink = RecordValueSinkFactory.getInstance(asm, stateTypes, valueColumns);
            } else {
                inputSinkClass = sinkSource.inputSinkClass;
                mergeSinkClass = sinkSource.mergeSinkClass;
                stateSinkClass = sinkSource.stateSinkClass;
                valueSink = sinkSource.valueSink;
            }
            inputSink = RecordSinkFactory.getInstance(inputSinkClass, baseTypes, inputColumns, inputFunctions, null, null, null, null);
            mergeSink = RecordSinkFactory.getInstance(mergeSinkClass, stateTypes, mergeColumns, null, null, null, null, null);
            stateSink = RecordSinkFactory.getInstance(stateSinkClass, rowTypes, rowColumns, null, null, null, null, null);
            value = new FlyweightPackedMapValue(mapValueTypes);
            record = new SegmentRecord(columnCrossIndex, segmentIndex + streamKeyIndex);
            map = MapFactory.createUnorderedMap(configuration, mapKeyTypes, mapValueTypes, 1, configuration.getSqlSmallMapPageSize(), true, false);
            mergeMemory = new DirectLongList((value.getSizeInBytes() + 7) / 8, MemoryTag.NATIVE_FAST_MAP, true);
            batchRows = new DirectLongList(batchSize, MemoryTag.NATIVE_FAST_MAP, true);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    private StreamGroupByMap(CairoConfiguration configuration, StreamGroupByMap prototype) {
        this.configuration = configuration;
        isShardable = true;
        shardingThreshold = configuration.getGroupByShardingThreshold();
        functions = prototype.functions;
        updater = prototype.updater;
        segmentIndex = prototype.segmentIndex;
        segmentType = prototype.segmentType;
        batchSize = prototype.batchSize;
        presizeMaxKeys = prototype.presizeMaxKeys;
        columnCrossIndex = prototype.columnCrossIndex;
        mapKeyTypes = prototype.mapKeyTypes;
        mapValueTypes = prototype.mapValueTypes;
        stateTypes = prototype.stateTypes;
        inputSink = prototype.inputSink;
        inputSinkClass = prototype.inputSinkClass;
        mergeSinkClass = prototype.mergeSinkClass;
        stateSinkClass = prototype.stateSinkClass;
        mergeSink = prototype.mergeSink;
        stateSink = prototype.stateSink;
        valueSink = prototype.valueSink;
        try {
            value = new FlyweightPackedMapValue(mapValueTypes);
            record = new SegmentRecord(columnCrossIndex, prototype.record.keyColumn);
            map = MapFactory.createUnorderedMap(configuration, mapKeyTypes, mapValueTypes, 1, configuration.getSqlSmallMapPageSize(), true, false);
            mergeMemory = new DirectLongList((value.getSizeInBytes() + 7) / 8, MemoryTag.NATIVE_FAST_MAP, true);
            batchRows = new DirectLongList(batchSize, MemoryTag.NATIVE_FAST_MAP, true);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    public void aggregate(PageFrameMemoryRecord record, DirectLongList rows, long lo, long hi, long baseRowId, boolean isBatch, SqlExecutionCircuitBreaker circuitBreaker) {
        long row = lo;
        if (!isSharded) {
            row = isBatch
                    ? aggregateBatch(record, rows, lo, hi, baseRowId, circuitBreaker)
                    : aggregateRows(record, rows, lo, hi, baseRowId, circuitBreaker);
        }
        if (row < hi) {
            aggregateSharded(record, rows, row, hi, baseRowId, circuitBreaker);
        }
    }

    public long appendTo(RecordChain chain, long previous, SqlExecutionCircuitBreaker circuitBreaker) {
        final MapRecordCursor cursor = map.getCursor();
        final MapRecord record = cursor.getRecord();
        for (long i = 1; cursor.hasNext(); i++) {
            if ((i & BREAKER_CHECK_MASK) == 0) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            }
            previous = chain.beginRecord(previous);
            stateSink.copy(record, chain);
            if (segmentType == ColumnType.INT) {
                chain.putInt(segmentKey == Numbers.LONG_NULL ? Numbers.INT_NULL : (int) segmentKey);
            } else {
                chain.putLong(segmentKey);
            }
        }
        return previous;
    }

    public void clear(long key) {
        if (isSharded) {
            unshard();
        }
        final long size = map.size();
        peakKeys = Math.max(peakKeys, size);
        if (presizeKeys > 0 && size == 0) {
            // A cached factory runs its query again: give the first segment the capacity the largest segment
            // of the last run needed rather than grow the empty map one rehash at a time.
            map.setKeyCapacity((int) presizeKeys);
        } else {
            // Map.clear() zeroes the whole table, which keeps the capacity of the largest segment so far.
            // Give that capacity back once a segment needs a fraction of it, so a single high-cardinality
            // segment does not make every later segment pay for it.
            if (map.getKeyCapacity() > Math.max(initialKeyCapacity, SHRINK_RATIO * (size + reservedKeys))) {
                map.restoreInitialCapacity();
            }
            // OrderedMap.restoreInitialCapacity() clears only when it reallocates.
            map.clear();
        }
        presizeKeys = 0;
        reservedKeys = 0;
        segmentKey = key;
    }

    @Override
    public void close() {
        if (map != null) {
            peakKeys = Math.max(peakKeys, map.size());
        }
        Throwable th = Misc.freeBestEffort(null, map);
        for (int i = 0, n = shards.size(); i < n; i++) {
            th = Misc.freeBestEffort(th, shards.getQuick(i));
        }
        isSharded = false;
        th = Misc.freeBestEffort(th, mergeMemory);
        th = Misc.freeBestEffort(th, batchRows);
        if (map != null) {
            map.setMemoryTracker(null);
        }
        if (mergeMemory != null) {
            mergeMemory.setMemoryTracker(null);
        }
        if (batchRows != null) {
            batchRows.setMemoryTracker(null);
        }
        CairoException.rethrowCleanupFailure(th);
    }

    public MapRecordCursor getCursor() {
        final MapRecordCursor cursor = map.getCursor();
        record.of(cursor.getRecord(), segmentKey);
        return cursor;
    }

    @TestOnly
    public boolean hasSinkClassesOf(StreamGroupByMap other) {
        return inputSink.getClass() == other.inputSink.getClass() && inputSink != other.inputSink
                && mergeSink.getClass() == other.mergeSink.getClass() && mergeSink != other.mergeSink
                && stateSink.getClass() == other.stateSink.getClass() && stateSink != other.stateSink
                && valueSink == other.valueSink;
    }

    @TestOnly
    public int getKeyCapacity() {
        return map.getKeyCapacity();
    }

    public SelectedRecord getRecord() {
        return record;
    }

    public long getSegmentKey() {
        return segmentKey;
    }

    public long getSegmentKey(Record record) {
        final int column = stateTypes.getColumnCount() - 1;
        return segmentType == ColumnType.INT ? record.getInt(column) : record.getLong(column);
    }

    @TestOnly
    public int getShardKeyCapacity() {
        return shardKeyCapacity;
    }

    public ObjList<Map> getShards() {
        return shards;
    }

    public ArrayColumnTypes getStateTypes() {
        return stateTypes;
    }

    public boolean isOpen() {
        return map.isOpen();
    }

    public boolean isSharded() {
        return isSharded;
    }

    public void merge(Record source, GroupByFunctionsUpdater updater) {
        final MapKey key = map.withKey();
        mergeSink.copy(source, key);
        final MapValue dest = key.createValue();
        if (dest.isNew()) {
            valueSink.copy(source, dest);
        } else {
            mergeMemory.ensureCapacity((value.getSizeInBytes() + 7) / 8);
            value.of(mergeMemory.getAddress());
            valueSink.copy(source, value);
            updater.merge(dest, value);
        }
    }

    public void merge(StreamGroupByMap source, GroupByFunctionsUpdater updater) {
        map.merge(source.map, updater);
    }

    // Routes the entries into the shards by the top bits of the key hash.
    public void mergeInto(ObjList<Map> shards, GroupByFunctionsUpdater updater) {
        final MapRecordCursor cursor = map.getCursor();
        final MapRecord record = cursor.getRecord();
        while (cursor.hasNext()) {
            final long hashCode = record.keyHashCode();
            final MapKey key = shards.getQuick((int) (hashCode >>> NUM_SHARDS_SHR)).withKey();
            record.copyToKey(key);
            final MapValue value = key.createValue(hashCode);
            if (value.isNew()) {
                record.copyValue(value);
            } else {
                updater.merge(value, record.getValue());
            }
        }
    }

    public StreamGroupByMap newInstance(CairoConfiguration configuration) {
        return new StreamGroupByMap(configuration, this);
    }

    public Map newMergeShard() {
        return MapFactory.createUnorderedMap(configuration, mapKeyTypes, mapValueTypes, configuration.getSqlSmallMapKeyCapacity(), shardPageSize(), false, false);
    }

    public void ofRow(Record row, long segmentKey) {
        record.of(row, segmentKey);
    }

    // Called before a merge frees the shards.
    public void rememberShardSizes() {
        long maxKeys = 0;
        for (int i = 0; i < NUM_SHARDS; i++) {
            maxKeys = Math.max(maxKeys, shards.getQuick(i).size());
        }
        shardKeyCapacity = maxKeys <= presizeMaxKeys ? (int) maxKeys : 0;
    }

    public void reopen(MemoryTracker tracker) {
        memoryTracker = tracker;
        map.setMemoryTracker(tracker);
        mergeMemory.setMemoryTracker(tracker);
        batchRows.setMemoryTracker(tracker);
        map.reopen();
        initialKeyCapacity = map.getKeyCapacity();
        map.setBatchEmptyValue(updater);
        presizeKeys = peakKeys > initialKeyCapacity && peakKeys <= presizeMaxKeys ? peakKeys : 0;
        peakKeys = 0;
    }

    public void shard() {
        if (isSharded) {
            return;
        }
        if (shards.size() == 0) {
            for (int i = 0; i < NUM_SHARDS; i++) {
                shards.add(MapFactory.createUnorderedMap(configuration, mapKeyTypes, mapValueTypes, configuration.getSqlSmallMapKeyCapacity(), shardPageSize(), true, false));
            }
        }
        // A merge or unshard() closed the shards.
        for (int i = 0; i < NUM_SHARDS; i++) {
            final Map shard = shards.getQuick(i);
            shard.setMemoryTracker(memoryTracker);
            shard.reopen();
            if (shardKeyCapacity > shard.getKeyCapacity()) {
                shard.setKeyCapacity(shardKeyCapacity);
            }
        }
        isSharded = true;
        mergeInto(shards, updater);
        map.close();
    }

    public long size() {
        return map.size();
    }

    public void update(Record record, long rowId) {
        final MapKey key = map.withKey();
        inputSink.copy(record, key);
        final MapValue value = key.createValue();
        if (value.isNew()) {
            updater.updateNew(value, record, rowId);
        } else {
            updater.updateExisting(value, record, rowId);
        }
    }

    // Returns the first row left once the map shards, so that at most a batch moves into the shards.
    private long aggregateBatch(PageFrameMemoryRecord record, DirectLongList rows, long lo, long hi, long baseRowId, SqlExecutionCircuitBreaker circuitBreaker) {
        batchRows.ensureCapacity(batchSize);
        final long batchAddress = batchRows.getAddress();
        reservedKeys = Math.max(reservedKeys, Math.min(hi - lo, batchSize));
        for (long start = lo; start < hi; start += batchSize) {
            if (start > lo) {
                // The caller checks the breaker between segments, so only the later batches check it here.
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            }
            final long end = Math.min(start + batchSize, hi);
            GroupByUtils.aggregateKeyedBatch(record, map, inputSink, functions, value, batchAddress,
                    rows == null ? 0 : rows.getAddress(), start, end, baseRowId);
            if (isShardable && map.size() > shardingThreshold) {
                shard();
                return end;
            }
        }
        return hi;
    }

    private long aggregateRows(PageFrameMemoryRecord record, DirectLongList rows, long lo, long hi, long baseRowId, SqlExecutionCircuitBreaker circuitBreaker) {
        for (long row = lo; row < hi; row++) {
            if (((row - lo) & BREAKER_CHECK_MASK) == BREAKER_CHECK_MASK) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            }
            final long rowIndex = rows == null ? row : rows.get(row);
            record.setFilteredRowIndex(rowIndex, row);
            update(record, baseRowId + rowIndex);
            if (isShardable && map.size() > shardingThreshold) {
                shard();
                return row + 1;
            }
        }
        return hi;
    }

    private void aggregateSharded(PageFrameMemoryRecord record, DirectLongList rows, long lo, long hi, long baseRowId, SqlExecutionCircuitBreaker circuitBreaker) {
        // The first shard stages the key to hash it.
        final Map lookupShard = shards.getQuick(0);
        for (long row = lo; row < hi; row++) {
            // A per-row check costs high-cardinality buckets up to 16%, since it does not inline here.
            if (((row - lo) & BREAKER_CHECK_MASK) == BREAKER_CHECK_MASK) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            }
            final long rowIndex = rows == null ? row : rows.get(row);
            record.setFilteredRowIndex(rowIndex, row);
            final MapKey lookupKey = lookupShard.withKey();
            inputSink.copy(record, lookupKey);
            lookupKey.commit();
            final long hashCode = lookupKey.hash();
            final Map shard = shards.getQuick((int) (hashCode >>> NUM_SHARDS_SHR));
            final MapKey key;
            if (shard != lookupShard) {
                key = shard.withKey();
                key.copyFrom(lookupKey);
            } else {
                key = lookupKey;
            }
            final MapValue value = key.createValue(hashCode);
            if (value.isNew()) {
                updater.updateNew(value, record, baseRowId + rowIndex);
            } else {
                updater.updateExisting(value, record, baseRowId + rowIndex);
            }
        }
    }

    // A sharded map owns hundreds of these, most of which hold a few keys.
    private long shardPageSize() {
        // A var-size key reports -1 and starts small enough to grow on demand.
        final long entrySize = Math.max(ColumnTypes.sizeInBytes(mapKeyTypes), 0) + ColumnTypes.sizeInBytes(mapValueTypes);
        return Math.min(configuration.getSqlSmallMapPageSize(), Math.max(SHARD_PAGE_SIZE, 2 * entrySize));
    }

    private void unshard() {
        for (int i = 0; i < NUM_SHARDS; i++) {
            shards.getQuick(i).close();
        }
        isSharded = false;
    }

    // Rows hold the values and the other keys; the stream key of every row of a segment comes from the segment.
    private static final class SegmentRecord extends SelectedRecord {
        private final int keyColumn;
        private long segmentKey;

        private SegmentRecord(IntList columnCrossIndex, int keyColumn) {
            super(columnCrossIndex);
            this.keyColumn = keyColumn;
        }

        @Override
        public int getInt(int col) {
            if (col == keyColumn) {
                return segmentKey == Numbers.LONG_NULL ? Numbers.INT_NULL : (int) segmentKey;
            }
            return super.getInt(col);
        }

        @Override
        public long getLong(int col) {
            return col == keyColumn ? segmentKey : super.getLong(col);
        }

        @Override
        public long getTimestamp(int col) {
            return col == keyColumn ? segmentKey : super.getTimestamp(col);
        }

        private void of(Record base, long segmentKey) {
            of(base);
            this.segmentKey = segmentKey;
        }
    }
}

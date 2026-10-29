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
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

public final class StreamGroupByMap implements QuietCloseable {
    // clear() shrinks the map once its capacity exceeds this many slots per key the finished segment needed.
    private static final int SHRINK_RATIO = 8;
    private final DirectLongList batchRows;
    private final int batchSize;
    private final IntList columnCrossIndex;
    private final ObjList<GroupByFunction> functions;
    private final RecordSink inputSink;
    private final Map map;
    private final ArrayColumnTypes mapKeyTypes;
    private final ArrayColumnTypes mapValueTypes;
    private final DirectLongList mergeMemory;
    private final RecordSink mergeSink;
    private final SelectedRecord record;
    private final int segmentIndex;
    private final int segmentType;
    private final RecordSink stateSink;
    private final ArrayColumnTypes stateTypes;
    private final GroupByFunctionsUpdater updater;
    private final FlyweightPackedMapValue value;
    private final RecordValueSink valueSink;
    private int initialKeyCapacity;
    // The most keys a batch of the current segment reserved map capacity for.
    private long reservedKeys;
    private long segmentKey;

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
            GroupByFunctionsUpdater updater
    ) {
        this.functions = functions;
        this.updater = updater;
        segmentIndex = valueTypes.getColumnCount();
        segmentType = keyTypes.getColumnType(streamKeyIndex);
        batchSize = configuration.getGroupByBatchSize();
        try {
            mapKeyTypes = new ArrayColumnTypes();
            mapValueTypes = new ArrayColumnTypes().addAll(valueTypes);
            mapValueTypes.add(segmentType);
            final ListColumnFilter inputColumns = new ListColumnFilter();
            final ObjList<Function> inputFunctions = new ObjList<>();
            columnCrossIndex = new IntList();
            for (int i = 0; i < segmentIndex; i++) {
                columnCrossIndex.add(i);
            }
            for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
                if (i == streamKeyIndex) {
                    columnCrossIndex.add(segmentIndex);
                } else {
                    columnCrossIndex.add(segmentIndex + 1 + mapKeyTypes.getColumnCount());
                    mapKeyTypes.add(keyTypes.getColumnType(i));
                    if (i < columnFilter.getColumnCount()) {
                        inputColumns.add(columnFilter.getColumnIndex(i));
                    } else {
                        inputFunctions.add(keyFunctions.getQuick(i - columnFilter.getColumnCount()));
                    }
                }
            }
            stateTypes = new ArrayColumnTypes().addAll(mapValueTypes).addAll(mapKeyTypes);
            final ListColumnFilter stateColumns = new ListColumnFilter();
            final ListColumnFilter mergeColumns = new ListColumnFilter();
            final ListColumnFilter valueColumns = new ListColumnFilter();
            for (int i = 0, n = stateTypes.getColumnCount(); i < n; i++) {
                stateColumns.add(i + 1);
                if (i <= segmentIndex) {
                    valueColumns.add(i + 1);
                } else {
                    mergeColumns.add(i + 1);
                }
            }
            inputSink = RecordSinkFactory.getInstance(configuration, asm, baseTypes, inputColumns, inputFunctions, null);
            mergeSink = RecordSinkFactory.getInstance(configuration, asm, stateTypes, mergeColumns);
            stateSink = RecordSinkFactory.getInstance(configuration, asm, stateTypes, stateColumns);
            valueSink = RecordValueSinkFactory.getInstance(asm, stateTypes, valueColumns);
            value = new FlyweightPackedMapValue(mapValueTypes);
            record = new SelectedRecord(columnCrossIndex);
            map = MapFactory.createUnorderedMap(configuration, mapKeyTypes, mapValueTypes, true, false);
            mergeMemory = new DirectLongList((value.getSizeInBytes() + 7) / 8, MemoryTag.NATIVE_FAST_MAP, true);
            batchRows = new DirectLongList(batchSize, MemoryTag.NATIVE_FAST_MAP, true);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    private StreamGroupByMap(CairoConfiguration configuration, StreamGroupByMap prototype) {
        functions = prototype.functions;
        updater = prototype.updater;
        segmentIndex = prototype.segmentIndex;
        segmentType = prototype.segmentType;
        batchSize = prototype.batchSize;
        columnCrossIndex = prototype.columnCrossIndex;
        mapKeyTypes = prototype.mapKeyTypes;
        mapValueTypes = prototype.mapValueTypes;
        stateTypes = prototype.stateTypes;
        inputSink = prototype.inputSink;
        mergeSink = prototype.mergeSink;
        stateSink = prototype.stateSink;
        valueSink = prototype.valueSink;
        try {
            value = new FlyweightPackedMapValue(mapValueTypes);
            record = new SelectedRecord(columnCrossIndex);
            map = MapFactory.createUnorderedMap(configuration, mapKeyTypes, mapValueTypes, true, false);
            mergeMemory = new DirectLongList((value.getSizeInBytes() + 7) / 8, MemoryTag.NATIVE_FAST_MAP, true);
            batchRows = new DirectLongList(batchSize, MemoryTag.NATIVE_FAST_MAP, true);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    public void aggregate(PageFrameMemoryRecord record, DirectLongList rows, long lo, long hi, long baseRowId, boolean isBatch, SqlExecutionCircuitBreaker circuitBreaker) {
        if (!isBatch) {
            for (long row = lo; row < hi; row++) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                final long rowIndex = rows == null ? row : rows.get(row);
                record.setFilteredRowIndex(rowIndex, row);
                update(record, baseRowId + rowIndex);
            }
            return;
        }
        batchRows.ensureCapacity(batchSize);
        final long batchAddress = batchRows.getAddress();
        reservedKeys = Math.max(reservedKeys, Math.min(hi - lo, batchSize));
        for (long start = lo; start < hi; start += batchSize) {
            if (start > lo) {
                // The caller checks the breaker once per segment, so only the later batches check it here.
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            }
            final long end = Math.min(start + batchSize, hi);
            GroupByUtils.aggregateKeyedBatch(record, map, inputSink, functions, value, batchAddress,
                    rows == null ? 0 : rows.getAddress(), start, end, baseRowId);
        }
    }

    public long appendTo(RecordChain chain, long previous, SqlExecutionCircuitBreaker circuitBreaker) {
        final MapRecordCursor cursor = map.getCursor();
        final MapRecord record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            GroupByUtils.setStreamGroupByKey(record.getValue(), segmentIndex, segmentType, segmentKey);
            previous = chain.beginRecord(previous);
            stateSink.copy(record, chain);
        }
        return previous;
    }

    public void clear(long key) {
        // Map.clear() zeroes the whole table, which keeps the capacity of the largest segment so far.
        // Give that capacity back once a segment needs a fraction of it, so a single high-cardinality
        // segment does not make every later segment pay for it.
        if (map.getKeyCapacity() > Math.max(initialKeyCapacity, SHRINK_RATIO * (map.size() + reservedKeys))) {
            map.restoreInitialCapacity();
        }
        // OrderedMap.restoreInitialCapacity() clears only when it reallocates.
        map.clear();
        reservedKeys = 0;
        segmentKey = key;
    }

    @Override
    public void close() {
        Throwable th = Misc.freeBestEffort(null, map);
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
        record.of(cursor.getRecord());
        return cursor;
    }

    @TestOnly
    public int getKeyCapacity() {
        return map.getKeyCapacity();
    }

    public SelectedRecord getRecord() {
        return record;
    }

    public long getSegmentKey(Record record) {
        return segmentType == ColumnType.INT ? record.getInt(segmentIndex) : record.getLong(segmentIndex);
    }

    public ArrayColumnTypes getStateTypes() {
        return stateTypes;
    }

    public boolean isOpen() {
        return map.isOpen();
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

    public StreamGroupByMap newInstance(CairoConfiguration configuration) {
        return new StreamGroupByMap(configuration, this);
    }

    public void putSegmentKey(MapRecord record) {
        GroupByUtils.setStreamGroupByKey(record.getValue(), segmentIndex, segmentType, segmentKey);
    }

    public void reopen(MemoryTracker tracker) {
        map.setMemoryTracker(tracker);
        mergeMemory.setMemoryTracker(tracker);
        batchRows.setMemoryTracker(tracker);
        map.reopen();
        initialKeyCapacity = map.getKeyCapacity();
        map.setBatchEmptyValue(updater);
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
            GroupByUtils.setStreamGroupByKey(value, segmentIndex, segmentType, segmentKey);
        } else {
            updater.updateExisting(value, record, rowId);
        }
    }
}

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

public final class StreamGroupByMap implements QuietCloseable {
    private final DirectLongList batchRows;
    private final int batchSize;
    private final ObjList<GroupByFunction> functions;
    private final RecordSink inputSink;
    private final Map map;
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
            final ArrayColumnTypes mapKeyTypes = new ArrayColumnTypes();
            final ArrayColumnTypes mapValueTypes = new ArrayColumnTypes().addAll(valueTypes);
            mapValueTypes.add(segmentType);
            final ListColumnFilter inputColumns = new ListColumnFilter();
            final ObjList<Function> inputFunctions = new ObjList<>();
            final IntList columnCrossIndex = new IntList();
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

    public void aggregate(PageFrameMemoryRecord record, long lo, long hi, long baseRowId, boolean isBatch, SqlExecutionCircuitBreaker circuitBreaker) {
        if (!isBatch) {
            for (long row = lo; row < hi; row++) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                record.setRowIndex(row);
                update(record, baseRowId + row);
            }
            return;
        }
        batchRows.ensureCapacity(batchSize);
        final long batchAddress = batchRows.getAddress();
        for (long start = lo; start < hi; start += batchSize) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            final long end = Math.min(start + batchSize, hi);
            GroupByUtils.aggregateKeyedBatch(record, map, inputSink, functions, value, batchAddress, start, end, baseRowId);
        }
    }

    public long appendTo(RecordChain chain, long previous, SqlExecutionCircuitBreaker circuitBreaker) {
        final MapRecordCursor cursor = map.getCursor();
        final MapRecord record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            GroupByUtils.setStreamGroupByKey(record.getValue(), segmentIndex, segmentType, segmentKey);
            previous = chain.beginRecord(previous);
            stateSink.copy(record, chain);
        }
        return previous;
    }

    public void clear(long key) {
        map.clear();
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

    public Record getRecord() {
        return record;
    }

    public long getSegmentKey(Record record) {
        return segmentType == ColumnType.INT ? record.getInt(segmentIndex) : record.getLong(segmentIndex);
    }

    public ArrayColumnTypes getStateTypes() {
        return stateTypes;
    }

    public void merge(Record source) {
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

    public void reopen(MemoryTracker tracker) {
        map.setMemoryTracker(tracker);
        mergeMemory.setMemoryTracker(tracker);
        batchRows.setMemoryTracker(tracker);
        map.reopen();
        map.setBatchEmptyValue(updater);
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

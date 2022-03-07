/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Used only in the latest by over sub-query case. Assumes that the base factory supports random access.
 */
public class LatestByLightRecordCursorFactory implements RecordCursorFactory {

    private static final int ROW_ID_VALUE_IDX = 0;
    private static final int TIMESTAMP_VALUE_IDX = 1;

    private final RecordCursorFactory base;
    private final RecordMetadata metadata;
    private final int timestampIndex;
    private final boolean orderedByTimestampAsc;
    private final LatestByLightRecordCursor cursor;
    private final RecordSink recordSink;
    // contains <[latest_by columns...], [row id, timestamp column]> pairs
    private final Map latestByMap;

    public LatestByLightRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory base,
            @NotNull RecordSink recordSink,
            @NotNull ColumnTypes columnTypes,
            int timestampIndex,
            boolean orderedByTimestampAsc
    ) {
        assert base.recordCursorSupportsRandomAccess();
        this.base = base;
        this.metadata = base.getMetadata();
        this.recordSink = recordSink;
        ArrayColumnTypes mapValueTypes = new ArrayColumnTypes();
        mapValueTypes.add(ROW_ID_VALUE_IDX, ColumnType.LONG);
        if (!orderedByTimestampAsc) {
            mapValueTypes.add(TIMESTAMP_VALUE_IDX, ColumnType.TIMESTAMP);
        }
        this.latestByMap = MapFactory.createMap(configuration, columnTypes, mapValueTypes);
        this.cursor = new LatestByLightRecordCursor();
        this.timestampIndex = timestampIndex;
        this.orderedByTimestampAsc = orderedByTimestampAsc;
    }

    @Override
    public void close() {
        base.close();
        latestByMap.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        latestByMap.clear();
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        final RecordCursor baseCursor = base.getCursor(executionContext);

        try {
            final Record baseRecord = baseCursor.getRecord();
            if (orderedByTimestampAsc) {
                // We don't need to store and compare timestamps if the sub-query returns them in asc order.
                // In this case we'll be good with the very last row id per each unique key.
                buildMapForOrderedSubQuery(circuitBreaker, baseCursor, baseRecord);
            } else {
                // Otherwise - we have to deal with the timestamps.
                buildMapForUnorderedSubQuery(circuitBreaker, baseCursor, baseRecord);
            }
            cursor.of(baseCursor, latestByMap, circuitBreaker);
            return cursor;
        } catch (Throwable e) {
            baseCursor.close();
            throw e;
        }
    }

    private void buildMapForOrderedSubQuery(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor baseCursor, Record baseRecord) {
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionWhenTripped();

            final MapKey key = latestByMap.withKey();
            recordSink.copy(baseRecord, key);
            final MapValue value = key.createValue();
            value.putLong(ROW_ID_VALUE_IDX, baseRecord.getRowId());
        }
    }

    private void buildMapForUnorderedSubQuery(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor baseCursor, Record baseRecord) {
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionWhenTripped();

            final MapKey key = latestByMap.withKey();
            recordSink.copy(baseRecord, key);
            final MapValue value = key.createValue();

            if (value.isNew()) {
                value.putLong(ROW_ID_VALUE_IDX, baseRecord.getRowId());
                value.putTimestamp(TIMESTAMP_VALUE_IDX, baseRecord.getTimestamp(timestampIndex));
            } else {
                long prevTimestamp = value.getTimestamp(TIMESTAMP_VALUE_IDX);
                long newTimestamp = baseRecord.getTimestamp(timestampIndex);
                if (newTimestamp >= prevTimestamp) {
                    value.putLong(ROW_ID_VALUE_IDX, baseRecord.getRowId());
                    value.putTimestamp(TIMESTAMP_VALUE_IDX, newTimestamp);
                }
            }
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private static class LatestByLightRecordCursor implements RecordCursor {

        private RecordCursor baseCursor;
        private Record baseRecord;
        private Map map;
        private RecordCursor mapCursor;
        private MapRecord mapRecord;
        private SqlExecutionCircuitBreaker circuitBreaker;

        public LatestByLightRecordCursor() {
        }

        public void of(RecordCursor baseCursor, Map map, SqlExecutionCircuitBreaker circuitBreaker) {
            this.baseCursor = baseCursor;
            this.baseRecord = baseCursor.getRecord();
            this.map = map;
            this.mapCursor = map.getCursor();
            this.mapRecord = (MapRecord) mapCursor.getRecord();
            this.circuitBreaker = circuitBreaker;
        }

        @Override
        public void close() {
            Misc.free(baseCursor);
            Misc.free(mapCursor);
            map.restoreInitialCapacity();
        }

        @Override
        public Record getRecord() {
            return baseRecord;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (!mapCursor.hasNext()) {
                return false;
            }
            circuitBreaker.statefulThrowExceptionWhenTripped();
            final MapValue value = mapRecord.getValue();
            final long rowId = value.getLong(ROW_ID_VALUE_IDX);
            baseCursor.recordAt(baseRecord, rowId);
            return true;
        }

        @Override
        public Record getRecordB() {
            return baseCursor.getRecordB();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            baseCursor.recordAt(record, atRowId);
        }

        @Override
        public void toTop() {
            mapCursor.toTop();
        }

        @Override
        public long size() {
            return mapCursor.size();
        }
    }
}

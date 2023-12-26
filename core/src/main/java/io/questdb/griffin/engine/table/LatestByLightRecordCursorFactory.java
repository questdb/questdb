/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Used only in the latest by over sub-query case. Assumes that the base factory supports random access.
 */
public class LatestByLightRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final int ROW_ID_VALUE_IDX = 0;
    private static final int TIMESTAMP_VALUE_IDX = 1;

    private final RecordCursorFactory base;
    private final LatestByLightRecordCursor cursor;
    private final boolean orderedByTimestampAsc;
    private final RecordSink recordSink;
    private final int timestampIndex;

    public LatestByLightRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory base,
            @NotNull RecordSink recordSink,
            @NotNull ColumnTypes columnTypes,
            int timestampIndex,
            boolean orderedByTimestampAsc
    ) {
        super(base.getMetadata());
        assert base.recordCursorSupportsRandomAccess();
        this.base = base;
        this.recordSink = recordSink;
        ArrayColumnTypes mapValueTypes = new ArrayColumnTypes();
        mapValueTypes.add(ROW_ID_VALUE_IDX, ColumnType.LONG);
        if (!orderedByTimestampAsc) {
            mapValueTypes.add(TIMESTAMP_VALUE_IDX, ColumnType.TIMESTAMP);
        }
        Map latestByMap = MapFactory.createMap(configuration, columnTypes, mapValueTypes);
        this.cursor = new LatestByLightRecordCursor(latestByMap);
        this.timestampIndex = timestampIndex;
        this.orderedByTimestampAsc = orderedByTimestampAsc;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        cursor.of(baseCursor, circuitBreaker);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LatestBy light");
        sink.meta("order_by_timestamp").val(orderedByTimestampAsc);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        base.close();
        cursor.close();
    }

    private class LatestByLightRecordCursor implements RecordCursor {

        private final Map latestByMap;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMapBuilt;
        private boolean isOpen;
        private RecordCursor mapCursor;
        private MapRecord mapRecord;

        public LatestByLightRecordCursor(Map latestByMap) {
            this.latestByMap = latestByMap;
            this.isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                Misc.free(baseCursor);
                Misc.free(mapCursor);
                Misc.free(latestByMap);
            }
        }

        @Override
        public Record getRecord() {
            return baseRecord;
        }

        @Override
        public Record getRecordB() {
            return baseCursor.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                buildMap();
                toTop();
                isMapBuilt = true;
            }
            if (!mapCursor.hasNext()) {
                return false;
            }
            circuitBreaker.statefulThrowExceptionIfTripped();
            final MapValue value = mapRecord.getValue();
            final long rowId = value.getLong(ROW_ID_VALUE_IDX);
            baseCursor.recordAt(baseRecord, rowId);
            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        public void of(RecordCursor baseCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            if (!isOpen) {
                isOpen = true;
                latestByMap.reopen();
            }
            this.baseCursor = baseCursor;
            baseRecord = baseCursor.getRecord();
            this.circuitBreaker = circuitBreaker;
            isMapBuilt = false;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            baseCursor.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return isMapBuilt ? mapCursor.size() : -1;
        }

        @Override
        public void toTop() {
            if (mapCursor != null) {
                mapCursor.toTop();
            }
        }

        private void buildMap() {
            if (orderedByTimestampAsc) {
                // We don't need to store and compare timestamps if the sub-query returns them in asc order.
                // In this case we'll be good with the very last row id per each unique key.
                buildMapForOrderedSubQuery();
            } else {
                // Otherwise - we have to deal with the timestamps.
                buildMapForUnorderedSubQuery();
            }
            mapCursor = latestByMap.getCursor();
            mapRecord = (MapRecord) mapCursor.getRecord();
        }

        private void buildMapForOrderedSubQuery() {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                final MapKey key = latestByMap.withKey();
                recordSink.copy(baseRecord, key);
                final MapValue value = key.createValue();
                value.putLong(ROW_ID_VALUE_IDX, baseRecord.getRowId());
            }
        }

        private void buildMapForUnorderedSubQuery() {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

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
    }
}

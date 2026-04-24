/*+****************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * Used only in the earliest by over sub-query case. Assumes that the base factory supports random access.
 */
public class EarliestByLightRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final int ROW_ID_VALUE_IDX = 0;
    private static final int TIMESTAMP_VALUE_IDX = 1;

    private final RecordCursorFactory base;
    private final EarliestByLightRecordCursor cursor;
    private final boolean isOrderedByTimestampAsc;
    private final RecordSink recordSink;
    private final int timestampIndex;

    public EarliestByLightRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory base,
            @NotNull RecordSink recordSink,
            @NotNull ColumnTypes columnTypes,
            int timestampIndex,
            boolean isOrderedByTimestampAsc
    ) {
        super(base.getMetadata());
        assert base.recordCursorSupportsRandomAccess();
        this.base = base;
        this.recordSink = recordSink;
        Map earliestByMap = null;
        try {
            ArrayColumnTypes mapValueTypes = new ArrayColumnTypes();
            mapValueTypes.add(ROW_ID_VALUE_IDX, ColumnType.LONG);
            if (!isOrderedByTimestampAsc) {
                mapValueTypes.add(TIMESTAMP_VALUE_IDX, base.getMetadata().getColumnType(timestampIndex));
            }
            earliestByMap = MapFactory.createOrderedMap(configuration, columnTypes, mapValueTypes);
            this.cursor = new EarliestByLightRecordCursor(earliestByMap);
            earliestByMap = null;
            this.timestampIndex = timestampIndex;
            this.isOrderedByTimestampAsc = isOrderedByTimestampAsc;
        } catch (Throwable th) {
            Misc.free(earliestByMap);
            close();
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
            cursor.of(baseCursor, circuitBreaker);
            return cursor;
        } catch (Throwable th) {
            baseCursor.close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("EarliestBy light");
        sink.meta("order_by_timestamp").val(isOrderedByTimestampAsc);
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
        Misc.free(base);
        Misc.free(cursor);
    }

    private class EarliestByLightRecordCursor implements RecordCursor {

        private final Map earliestByMap;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMapBuilt;
        private boolean isOpen;
        private RecordCursor mapCursor;
        private MapRecord mapRecord;

        public EarliestByLightRecordCursor(Map earliestByMap) {
            this.earliestByMap = earliestByMap;
            this.isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                Misc.free(baseCursor);
                Misc.free(mapCursor);
                Misc.free(earliestByMap);
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
                earliestByMap.reopen();
            }
            this.baseCursor = baseCursor;
            baseRecord = baseCursor.getRecord();
            this.circuitBreaker = circuitBreaker;
            isMapBuilt = false;
        }

        @Override
        public long preComputedStateSize() {
            return isMapBuilt ? 1 : 0;
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
            if (isOrderedByTimestampAsc) {
                // We don't need to store and compare timestamps if the sub-query returns them in asc order.
                // In this case we'll be good with the very first row id per each unique key.
                buildMapForOrderedSubQuery();
            } else {
                // Otherwise - we have to deal with the timestamps.
                buildMapForUnorderedSubQuery();
            }
            mapCursor = earliestByMap.getCursor();
            mapRecord = (MapRecord) mapCursor.getRecord();
        }

        private void buildMapForOrderedSubQuery() {
            // For earliest with ordered ascending, keep the FIRST row per partition.
            // Rows with NULL timestamps are skipped to match buildMapForUnorderedSubQuery
            // semantics; otherwise the winning row would depend on which planner path
            // was chosen.
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                if (baseRecord.getTimestamp(timestampIndex) == Numbers.LONG_NULL) {
                    continue;
                }

                final MapKey key = earliestByMap.withKey();
                recordSink.copy(baseRecord, key);
                final MapValue value = key.createValue();
                if (value.isNew()) {
                    value.putLong(ROW_ID_VALUE_IDX, baseRecord.getRowId());
                }
            }
        }

        private void buildMapForUnorderedSubQuery() {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                // Skip NULL-timestamp rows so the map's key set matches the ordered fast
                // path; otherwise a key with only NULL rows would be emitted here but
                // omitted in buildMapForOrderedSubQuery.
                final long newTimestamp = baseRecord.getTimestamp(timestampIndex);
                if (newTimestamp == Numbers.LONG_NULL) {
                    continue;
                }

                final MapKey key = earliestByMap.withKey();
                recordSink.copy(baseRecord, key);
                final MapValue value = key.createValue();

                if (value.isNew()) {
                    value.putLong(ROW_ID_VALUE_IDX, baseRecord.getRowId());
                    value.putTimestamp(TIMESTAMP_VALUE_IDX, newTimestamp);
                } else {
                    long prevTimestamp = value.getTimestamp(TIMESTAMP_VALUE_IDX);
                    if (newTimestamp < prevTimestamp) {
                        value.putLong(ROW_ID_VALUE_IDX, baseRecord.getRowId());
                        value.putTimestamp(TIMESTAMP_VALUE_IDX, newTimestamp);
                    }
                }
            }
        }
    }
}

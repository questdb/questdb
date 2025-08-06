/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Used only in the latest by over sub-query case.
 */
public class LatestByRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final int RECORD_INDEX_VALUE_IDX = 0;
    private static final int TIMESTAMP_VALUE_IDX = 1;

    private final RecordCursorFactory base;
    private final LatestByRecordCursor cursor;
    private final RecordSink recordSink;
    private final DirectLongList rowIndexes;
    private final long rowIndexesInitialCapacity;

    public LatestByRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory base,
            @NotNull RecordSink recordSink,
            @NotNull ColumnTypes columnTypes,
            int timestampIndex
    ) {
        super(base.getMetadata());
        assert !base.recordCursorSupportsRandomAccess();
        this.base = base;
        this.recordSink = recordSink;
        ArrayColumnTypes mapValueTypes = new ArrayColumnTypes();
        mapValueTypes.add(RECORD_INDEX_VALUE_IDX, ColumnType.LONG);
        mapValueTypes.add(TIMESTAMP_VALUE_IDX, base.getMetadata().getColumnType(timestampIndex));
        Map latestByMap = MapFactory.createOrderedMap(configuration, columnTypes, mapValueTypes);
        this.cursor = new LatestByRecordCursor(latestByMap, timestampIndex);
        this.rowIndexesInitialCapacity = configuration.getSqlLatestByRowCount();
        this.rowIndexes = new DirectLongList(rowIndexesInitialCapacity, MemoryTag.NATIVE_LATEST_BY_LONG_LIST);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // Forcefully disable column pre-touch for nested filter queries.
        executionContext.setColumnPreTouchEnabled(false);
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, recordSink, rowIndexes, rowIndexesInitialCapacity, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LatestBy");
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
        Misc.free(rowIndexes);
        Misc.free(cursor);
        Misc.free(base);
    }

    private static class LatestByRecordCursor implements NoRandomAccessRecordCursor {

        // contains <[latest_by columns...], [row index, timestamp column]> pairs
        private final Map latestByMap;
        private final int timestampIndex;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long index = 0;
        private boolean isMapBuilt;
        private boolean isOpen;
        private RecordSink recordSink;
        private DirectLongList rowIndexes;
        private long rowIndexesCapacityThreshold;
        private long rowIndexesPos = 0;

        public LatestByRecordCursor(Map latestByMap, int timestampIndex) {
            this.latestByMap = latestByMap;
            this.timestampIndex = timestampIndex;
            this.isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                baseCursor = Misc.free(baseCursor);
                if (rowIndexes != null) {
                    rowIndexes.clear();
                    if (rowIndexes.getCapacity() > rowIndexesCapacityThreshold) {
                        // This call will shrink down the underlying array
                        rowIndexes.setCapacity(rowIndexesCapacityThreshold);
                    }
                }
                latestByMap.close();
            }
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
            if (!isMapBuilt) {
                buildMap();
                toTop();
                isMapBuilt = true;
            }

            if (rowIndexesPos == rowIndexes.size()) {
                return false;
            }

            final long nextIndex = rowIndexes.get(rowIndexesPos);
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (index++ == nextIndex) {
                    rowIndexesPos++;
                    return true;
                }
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        public void of(
                RecordCursor baseCursor,
                RecordSink recordSink,
                DirectLongList rowIndexes,
                long rowIndexesCapacityThreshold,
                SqlExecutionCircuitBreaker circuitBreaker
        ) {
            this.baseCursor = baseCursor;
            baseRecord = baseCursor.getRecord();
            if (!isOpen) {
                isOpen = true;
                latestByMap.reopen();
            }
            this.recordSink = recordSink;
            this.rowIndexes = rowIndexes;
            this.circuitBreaker = circuitBreaker;
            this.rowIndexesCapacityThreshold = rowIndexesCapacityThreshold;
            rowIndexesPos = 0;
            index = 0;
            isMapBuilt = false;
        }

        @Override
        public long preComputedStateSize() {
            return RecordCursor.fromBool(isMapBuilt) + baseCursor.preComputedStateSize();
        }

        @Override
        public long size() {
            return isMapBuilt ? rowIndexes.size() : -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            index = 0;
            rowIndexesPos = 0;
        }

        private void buildMap() {
            final Record baseRecord = baseCursor.getRecord();
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                final MapKey key = latestByMap.withKey();
                recordSink.copy(baseRecord, key);
                final MapValue value = key.createValue();

                if (value.isNew()) {
                    value.putLong(RECORD_INDEX_VALUE_IDX, index);
                    value.putTimestamp(TIMESTAMP_VALUE_IDX, baseRecord.getTimestamp(timestampIndex));
                } else {
                    long prevTimestamp = value.getTimestamp(TIMESTAMP_VALUE_IDX);
                    long newTimestamp = baseRecord.getTimestamp(timestampIndex);
                    if (newTimestamp >= prevTimestamp) {
                        value.putLong(RECORD_INDEX_VALUE_IDX, index);
                        value.putTimestamp(TIMESTAMP_VALUE_IDX, newTimestamp);
                    }
                }

                index++;
            }

            // Copy row indexes into the long list.
            try (final RecordCursor mapCursor = latestByMap.getCursor()) {
                final MapRecord mapRecord = (MapRecord) mapCursor.getRecord();
                while (mapCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    final MapValue value = mapRecord.getValue();
                    final long rowId = value.getLong(RECORD_INDEX_VALUE_IDX);
                    rowIndexes.add(rowId);
                }
            }

            // Sort the indexes, so that we can use them when iterating the base cursor.
            rowIndexes.sortAsUnsigned();
            // Map is no longer needed, deallocate native memory.
            latestByMap.close();
        }
    }
}

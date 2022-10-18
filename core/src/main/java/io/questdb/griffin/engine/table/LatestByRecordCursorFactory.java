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
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
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
    private final int timestampIndex;
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
        mapValueTypes.add(TIMESTAMP_VALUE_IDX, ColumnType.TIMESTAMP);
        Map latestByMap = MapFactory.createMap(configuration, columnTypes, mapValueTypes);
        this.cursor = new LatestByRecordCursor(latestByMap);
        this.timestampIndex = timestampIndex;
        this.rowIndexesInitialCapacity = configuration.getSqlLatestByRowCount();
        this.rowIndexes = new DirectLongList(rowIndexesInitialCapacity, MemoryTag.NATIVE_LATEST_BY_LONG_LIST);
    }

    @Override
    protected void _close() {
        base.close();
        rowIndexes.close();
        cursor.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (!cursor.isOpen) {
            cursor.isOpen = true;
            cursor.latestByMap.reopen();
        }
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        final RecordCursor baseCursor = base.getCursor(executionContext);

        try {
            final Record baseRecord = baseCursor.getRecord();
            buildMap(circuitBreaker, baseCursor, baseRecord);

            // Copy the row indexes into the long list.
            try (final RecordCursor mapCursor = cursor.latestByMap.getCursor()) {
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

            // We'll have to iterate the base record once again, so reset it.
            baseCursor.toTop();

            // Map is no longer needed, so deallocate native memory
            cursor.latestByMap.close();

            cursor.of(baseCursor, rowIndexes, rowIndexesInitialCapacity, circuitBreaker);
            return cursor;
        } catch (Throwable e) {
            baseCursor.close();
            throw e;
        }
    }

    private void buildMap(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor baseCursor, Record baseRecord) {
        long index = 0;
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();

            final MapKey key = cursor.latestByMap.withKey();
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
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private static class LatestByRecordCursor implements NoRandomAccessRecordCursor {

        // contains <[latest_by columns...], [row index, timestamp column]> pairs
        private final Map latestByMap;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private long index = 0;
        private DirectLongList rowIndexes;
        private long rowIndexesPos = 0;
        private long rowIndexesCapacityThreshold;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;

        public LatestByRecordCursor(Map latestByMap) {
            this.latestByMap = latestByMap;
            this.isOpen = true;
        }

        public void of(RecordCursor baseCursor, DirectLongList rowIndexes, long rowIndexesCapacityThreshold, SqlExecutionCircuitBreaker circuitBreaker) {
            this.baseCursor = baseCursor;
            this.baseRecord = baseCursor.getRecord();
            this.rowIndexes = rowIndexes;
            this.circuitBreaker = circuitBreaker;
            this.index = 0;
            this.rowIndexesPos = 0;
            this.rowIndexesCapacityThreshold = rowIndexesCapacityThreshold;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                Misc.free(baseCursor);
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
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (rowIndexesPos == rowIndexes.size()) {
                return false;
            }

            final long nextIndex = rowIndexes.get(rowIndexesPos++);
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (index++ == nextIndex) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            this.index = 0;
            this.rowIndexesPos = 0;
        }

        @Override
        public long size() {
            return rowIndexes.size();
        }
    }
}

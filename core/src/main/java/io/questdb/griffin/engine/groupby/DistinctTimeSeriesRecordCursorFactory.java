/*******************************************************************************
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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class DistinctTimeSeriesRecordCursorFactory extends AbstractRecordCursorFactory {
    protected final RecordCursorFactory base;
    private final DistinctTimeSeriesRecordCursor cursor;

    public DistinctTimeSeriesRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @Transient @NotNull EntityColumnFilter columnFilter,
            @Transient @NotNull BytecodeAssembler asm
    ) {
        super(base.getMetadata());
        this.base = base;
        try {
            assert base.recordCursorSupportsRandomAccess();
            final RecordMetadata metadata = base.getMetadata();
            // sink will be storing record columns to map key
            columnFilter.of(metadata.getColumnCount());
            RecordSink recordSink = RecordSinkFactory.getInstance(configuration, asm, metadata, columnFilter);
            Map dataMap = new OrderedMap(
                    configuration.getSqlSmallMapPageSize(),
                    metadata,
                    configuration.getSqlDistinctTimestampKeyCapacity(),
                    configuration.getSqlDistinctTimestampLoadFactor(),
                    Integer.MAX_VALUE
            );
            this.cursor = new DistinctTimeSeriesRecordCursor(
                    getMetadata().getTimestampIndex(),
                    dataMap,
                    recordSink
            );
        } catch (Throwable t) {
            close();
            throw t;
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
            return cursor.of(baseCursor, executionContext);
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("DistinctTimeSeries");
        sink.attr("keys").val(getMetadata());
        sink.child(base);
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(cursor);
    }

    private static class DistinctTimeSeriesRecordCursor implements RecordCursor {
        private final Map dataMap;
        private final RecordSink recordSink;
        private final int timestampIndex;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isFirstRow;
        private boolean isOpen;
        private long prevRowId;
        private long prevTimestamp;
        private Record record;
        private Record recordB;

        public DistinctTimeSeriesRecordCursor(int timestampIndex, Map dataMap, RecordSink recordSink) {
            this.timestampIndex = timestampIndex;
            this.dataMap = dataMap;
            this.recordSink = recordSink;
            this.isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                baseCursor = Misc.free(baseCursor);
                Misc.free(dataMap);
            }
        }

        @Override
        public Record getRecord() {
            return record;
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
            if (isFirstRow) {
                isFirstRow = false;
                if (baseCursor.hasNext()) {
                    prevTimestamp = record.getTimestamp(timestampIndex);
                    prevRowId = record.getRowId();
                    return true;
                }
                return false;
            }

            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                final long timestamp = record.getTimestamp(timestampIndex);
                if (timestamp != prevTimestamp) {
                    prevTimestamp = timestamp;
                    prevRowId = record.getRowId();
                    return true;
                }

                if (checkIfNotDupe()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        public RecordCursor of(RecordCursor baseCursor, SqlExecutionContext sqlExecutionContext) {
            this.baseCursor = baseCursor;
            record = baseCursor.getRecord();
            recordB = baseCursor.getRecordB();
            if (!isOpen) {
                isOpen = true;
                dataMap.reopen();
            }
            circuitBreaker = sqlExecutionContext.getCircuitBreaker();
            isFirstRow = true;
            return this;
        }

        @Override
        public long preComputedStateSize() {
            // no pre-computed state
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            baseCursor.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            dataMap.clear();
            isFirstRow = true;
        }

        private boolean checkIfNotDupe() {
            MapKey key;
            if (prevRowId != -1) {
                // jump recordB to the prev record of the base cursor
                baseCursor.recordAt(recordB, prevRowId);

                dataMap.clear();
                key = dataMap.withKey();
                recordSink.copy(recordB, key);
                // we have already returned this record
                // and map should be empty
                key.create();
                prevRowId = -1;
            }

            key = dataMap.withKey();
            // now copy the current record to the map
            recordSink.copy(record, key);
            // record is not a duplicate
            return key.create();
        }
    }
}

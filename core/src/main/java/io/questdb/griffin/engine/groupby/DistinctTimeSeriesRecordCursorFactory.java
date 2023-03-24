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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.*;
import io.questdb.cairo.map.FastMap;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
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
        assert base.recordCursorSupportsRandomAccess();
        final RecordMetadata metadata = base.getMetadata();
        // sink will be storing record columns to map key
        columnFilter.of(metadata.getColumnCount());
        RecordSink recordSink = RecordSinkFactory.getInstance(asm, metadata, columnFilter, false);
        Map dataMap = new FastMap(
                configuration.getSqlMapPageSize(),
                metadata,
                configuration.getSqlDistinctTimestampKeyCapacity(),
                configuration.getSqlDistinctTimestampLoadFactor(),
                Integer.MAX_VALUE
        );

        this.base = base;
        this.cursor = new DistinctTimeSeriesRecordCursor(
                getMetadata().getTimestampIndex(),
                dataMap,
                recordSink
        );
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return cursor.of(base.getCursor(executionContext), executionContext);
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
        base.close();
        cursor.close();
    }

    private static class DistinctTimeSeriesRecordCursor implements RecordCursor {
        private static final byte COMPUTE_NEXT = 1;
        private static final byte INIT_FIRST_TIMESTAMP = 0;
        private static final byte NO_ROWS = 3;
        private static final byte REUSE_CURRENT = 2;

        private final Map dataMap;
        private final RecordSink recordSink;
        private final int timestampIndex;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;
        private long prevRowId;
        private long prevTimestamp;
        private Record record;
        private Record recordB;
        private byte state = 0;

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
                Misc.free(baseCursor);
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
            if (state == INIT_FIRST_TIMESTAMP) {
                // first iteration to get initial timestamp value
                if (baseCursor.hasNext()) {
                    prevTimestamp = record.getTimestamp(timestampIndex);
                    prevRowId = record.getRowId();
                    state = REUSE_CURRENT;
                } else {
                    // edge case - base cursor is empty, avoid calling hasNext() again
                    state = NO_ROWS;
                }
            }

            if (state == COMPUTE_NEXT) {
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

            boolean next = (state == REUSE_CURRENT);
            state = COMPUTE_NEXT;
            return next;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        public RecordCursor of(RecordCursor baseCursor, SqlExecutionContext sqlExecutionContext) {
            if (!isOpen) {
                isOpen = true;
                dataMap.reopen();
            }
            this.baseCursor = baseCursor;
            circuitBreaker = sqlExecutionContext.getCircuitBreaker();
            record = baseCursor.getRecord();
            recordB = baseCursor.getRecordB();
            state = INIT_FIRST_TIMESTAMP;
            return this;
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

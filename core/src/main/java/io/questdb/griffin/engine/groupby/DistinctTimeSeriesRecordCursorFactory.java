/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class DistinctTimeSeriesRecordCursorFactory implements RecordCursorFactory {

    protected final RecordCursorFactory base;
    private final Map dataMap;
    private final DistinctTimeSeriesRecordCursor cursor;
    // this sink is used to copy recordKeyMap keys to dataMap
    private final RecordMetadata metadata;
    public static final byte COMPUTE_NEXT = 0;
    public static final byte REUSE_CURRENT = 1;
    public static final byte NO_ROWS = 2;

    public DistinctTimeSeriesRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @Transient @NotNull EntityColumnFilter columnFilter,
            @Transient @NotNull BytecodeAssembler asm
    ) {
        final RecordMetadata metadata = base.getMetadata();
        // sink will be storing record columns to map key
        columnFilter.of(metadata.getColumnCount());
        RecordSink recordSink = RecordSinkFactory.getInstance(asm, metadata, columnFilter, false);
        this.dataMap = new FastMap(
                configuration.getSqlMapPageSize(),
                metadata,
                configuration.getSqlDistinctTimestampKeyCapacity(),
                configuration.getSqlDistinctTimestampLoadFactor(),
                Integer.MAX_VALUE
        ) ;

        this.base = base;
        this.metadata = metadata;
        this.cursor = new DistinctTimeSeriesRecordCursor(
                getMetadata().getTimestampIndex(),
                dataMap,
                recordSink
        );
    }

    @Override
    public void close() {
        dataMap.close();
        base.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return cursor.of(base.getCursor(executionContext), executionContext);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    private static class DistinctTimeSeriesRecordCursor implements RecordCursor {
        private final Map dataMap;
        private final RecordSink recordSink;
        private final int timestampIndex;
        private RecordCursor baseCursor;
        private Record record;
        private Record recordB;
        private long prevTimestamp;
        private long prevRowId;
        private byte state = 0;
        private SqlExecutionInterruptor interruptor;

        public DistinctTimeSeriesRecordCursor(int timestampIndex, Map dataMap, RecordSink recordSink) {
            this.timestampIndex = timestampIndex;
            this.dataMap = dataMap;
            this.recordSink = recordSink;
        }

        @Override
        public void close() {
            Misc.free(baseCursor);
            dataMap.clear();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (state == COMPUTE_NEXT) {
                while (baseCursor.hasNext()) {
                    interruptor.checkInterrupted();
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
            boolean next = state == REUSE_CURRENT;
            state = COMPUTE_NEXT;
            return next;
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
            baseCursor.toTop();
            dataMap.clear();
        }

        @Override
        public long size() {
            return -1;
        }

        public RecordCursor of(RecordCursor baseCursor, SqlExecutionContext sqlExecutionContext) {
            this.baseCursor = baseCursor;
            this.interruptor = sqlExecutionContext.getSqlExecutionInterruptor();
            this.record = baseCursor.getRecord();
            this.recordB = baseCursor.getRecordB();
            this.dataMap.clear();

            // first iteration to get initial timestamp value
            if (baseCursor.hasNext()) {
                prevTimestamp = record.getTimestamp(timestampIndex);
                prevRowId = record.getRowId();
                state = REUSE_CURRENT;
            } else {
                // edge case - base cursor is empty, avoid calling hasNext() again
                state = NO_ROWS;
            }
            return this;
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

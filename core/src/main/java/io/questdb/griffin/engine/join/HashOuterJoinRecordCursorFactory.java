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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

public class HashOuterJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final RecordSink masterSink;
    private final RecordSink slaveKeySink;
    private final HashOuterJoinRecordCursor cursor;

    public HashOuterJoinRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just LONG, we store chain references in map
            RecordSink masterSink,
            RecordSink slaveKeySink,
            RecordSink slaveChainSink,
            int columnSplit

    ) {
        super(metadata);
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        RecordChain slaveChain = new RecordChain(slaveFactory.getMetadata(), slaveChainSink, configuration.getSqlHashJoinValuePageSize(), configuration.getSqlHashJoinValueMaxPages());
        this.masterSink = masterSink;
        this.slaveKeySink = slaveKeySink;

        Map joinKeyMap = MapFactory.createMap(configuration, joinColumnTypes, valueTypes);
        this.cursor = new HashOuterJoinRecordCursor(
                columnSplit,
                joinKeyMap,
                slaveChain,
                NullRecordFactory.getInstance(slaveFactory.getMetadata())
        );
    }

    static void buildMap(
            RecordCursor slaveCursor,
            Record record,
            Map joinKeyMap,
            RecordSink slaveKeySink,
            RecordChain slaveChain,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        joinKeyMap.clear();
        slaveChain.clear();
        while (slaveCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            MapKey key = joinKeyMap.withKey();
            key.put(record, slaveKeySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                long offset = slaveChain.put(record, -1);
                value.putLong(0, offset);
                value.putLong(1, offset);
            } else {
                value.putLong(1, slaveChain.put(record, value.getLong(1)));
            }
        }
    }

    @Override
    protected void _close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
        cursor.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        try {
            cursor.of(executionContext, slaveCursor);
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(cursor);
            throw e;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean hasDescendingOrder() {
        return masterFactory.hasDescendingOrder();
    }

    private class HashOuterJoinRecordCursor extends AbstractJoinCursor {
        private final OuterJoinRecord record;
        private final RecordChain slaveChain;
        private final Map joinKeyMap;
        private Record masterRecord;
        private boolean useSlaveCursor;
        private boolean isOpen;

        public HashOuterJoinRecordCursor(int columnSplit, Map joinKeyMap, RecordChain slaveChain, Record nullRecord) {
            super(columnSplit);
            this.record = new OuterJoinRecord(columnSplit, nullRecord);
            this.joinKeyMap = joinKeyMap;
            this.slaveChain = slaveChain;
            this.isOpen = true;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (useSlaveCursor && slaveChain.hasNext()) {
                return true;
            }

            if (masterCursor.hasNext()) {
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChain.of(value.getLong(0));
                    // we know cursor has values
                    // advance to get first value
                    slaveChain.hasNext();
                    useSlaveCursor = true;
                    record.hasSlave(true);
                } else {
                    useSlaveCursor = false;
                    record.hasSlave(false);
                }
                return true;
            }
            return false;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            useSlaveCursor = false;
        }

        private void buildMapOfSlaveRecords(RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            if (!this.isOpen) {
                this.isOpen = true;
                this.joinKeyMap.reallocate();
                this.slaveChain.reallocate();
            }
            HashOuterJoinRecordCursorFactory factory = HashOuterJoinRecordCursorFactory.this;
            buildMap(slaveCursor, slaveCursor.getRecord(), this.joinKeyMap, factory.slaveKeySink, this.slaveChain, circuitBreaker);
        }

        void of(SqlExecutionContext executionContext, RecordCursor slaveCursor) throws SqlException {
            try {
                buildMapOfSlaveRecords(slaveCursor, executionContext.getCircuitBreaker());
                this.masterCursor = masterFactory.getCursor(executionContext);

                this.slaveCursor = slaveCursor;
                this.masterRecord = masterCursor.getRecord();
                Record slaveRecord = slaveChain.getRecord();
                this.slaveChain.setSymbolTableResolver(slaveCursor);
                record.of(masterRecord, slaveRecord);
                useSlaveCursor = false;
            } catch (Throwable e) {
                this.masterCursor = Misc.free(masterCursor);
                throw e;
            }
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                joinKeyMap.close();
                slaveChain.close();
                super.close();
            }
        }
    }
}

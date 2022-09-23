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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

public class HashJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final RecordSink masterSink;
    private final RecordSink slaveKeySink;
    private final HashJoinRecordCursor cursor;

    public HashJoinRecordCursorFactory(
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
        Map joinKeyMap = MapFactory.createMap(configuration, joinColumnTypes, valueTypes);
        RecordChain slaveChain = new RecordChain(slaveFactory.getMetadata(), slaveChainSink, configuration.getSqlHashJoinValuePageSize(), configuration.getSqlHashJoinValueMaxPages());
        this.masterSink = masterSink;
        this.slaveKeySink = slaveKeySink;
        this.cursor = new HashJoinRecordCursor(columnSplit, joinKeyMap, slaveChain);
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
            this.cursor.of(executionContext, slaveCursor);
            return this.cursor;
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

    @Override
    public boolean supportsUpdateRowId(CharSequence tableName) {
        return masterFactory.supportsUpdateRowId(tableName);
    }

    private class HashJoinRecordCursor extends AbstractJoinCursor {
        private final JoinRecord recordA;
        private final RecordChain slaveChain;
        private final Map joinKeyMap;
        private Record masterRecord;
        private boolean useSlaveCursor;
        private boolean isOpen;

        public HashJoinRecordCursor(int columnSplit, Map joinKeyMap, RecordChain slaveChain) {
            super(columnSplit);
            this.recordA = new JoinRecord(columnSplit);
            this.joinKeyMap = joinKeyMap;
            this.slaveChain = slaveChain;
            this.isOpen = true;
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public boolean hasNext() {
            if (useSlaveCursor && slaveChain.hasNext()) {
                return true;
            }

            while (masterCursor.hasNext()) {
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChain.of(value.getLong(0));
                    // we know cursor has values
                    // advance to get first value
                    slaveChain.hasNext();
                    useSlaveCursor = true;
                    return true;
                }
            }
            return false;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            useSlaveCursor = false;
        }

        private void buildMapOfSlaveRecords(RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            if (!isOpen) {
                isOpen = true;
                joinKeyMap.reallocate();
                slaveChain.reallocate();
            }
            HashJoinRecordCursorFactory factory = HashJoinRecordCursorFactory.this;
            HashOuterJoinRecordCursorFactory.buildMap(slaveCursor, slaveCursor.getRecord(), joinKeyMap, factory.slaveKeySink, slaveChain, circuitBreaker);
        }

        void of(SqlExecutionContext executionContext, RecordCursor slaveCursor) throws SqlException {
            try {
                buildMapOfSlaveRecords(slaveCursor, executionContext.getCircuitBreaker());
                this.masterCursor = masterFactory.getCursor(executionContext);
                this.slaveCursor = slaveCursor;
                this.masterRecord = masterCursor.getRecord();
                Record slaveRecord = slaveChain.getRecord();
                this.slaveChain.setSymbolTableResolver(slaveCursor);
                recordA.of(masterRecord, slaveRecord);
                useSlaveCursor = false;
            } catch (Throwable e) {
                masterCursor = Misc.free(masterCursor);
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

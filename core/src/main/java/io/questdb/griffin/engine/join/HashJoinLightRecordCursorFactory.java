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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

public class HashJoinLightRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final HashJoinRecordCursor cursor;
    private final RecordSink masterSink;
    private final RecordSink slaveKeySink;

    public HashJoinLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just LONG, we store chain references in map
            RecordSink masterSink,
            RecordSink slaveKeySink,
            int columnSplit,
            JoinContext joinContext
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        this.masterSink = masterSink;
        this.slaveKeySink = slaveKeySink;
        this.cursor = new HashJoinRecordCursor(columnSplit, configuration, joinColumnTypes, valueTypes);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        RecordCursor masterCursor = null;
        try {
            masterCursor = masterFactory.getCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return masterFactory.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Hash Join Light");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    @Override
    protected void _close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
        cursor.close();
    }

    private class HashJoinRecordCursor extends AbstractJoinCursor {
        private final Map joinKeyMap;
        private final JoinRecord record;
        private final LongChain slaveChain;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMapBuilt;
        private boolean isOpen;
        private Record masterRecord;
        private long size = -1;
        private LongChain.TreeCursor slaveChainCursor;
        private Record slaveRecord;

        public HashJoinRecordCursor(int columnSplit, CairoConfiguration configuration, ColumnTypes joinColumnTypes, ColumnTypes valueTypes) {
            super(columnSplit);
            record = new JoinRecord(columnSplit);
            joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes);
            slaveChain = new LongChain(configuration.getSqlHashJoinLightValuePageSize(), configuration.getSqlHashJoinLightValueMaxPages());
            isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                size = -1;
                joinKeyMap.close();
                slaveChain.close();
                super.close();
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            buildMapOfSlaveRecords();

            if (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                return true;
            }

            while (masterCursor.hasNext()) {
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getLong(0));
                    // we know cursor has values
                    // advance to get the first value
                    slaveChainCursor.hasNext();
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    return true;
                }
            }
            return false;
        }

        @Override
        public long size() {
            if (size > -1) {
                return size;
            }
            buildMapOfSlaveRecords();
            return size = TableUtils.computeCursorSizeFromMap(masterCursor, joinKeyMap, masterSink);
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveChainCursor = null;
            if (!isMapBuilt) {
                slaveCursor.toTop();
                joinKeyMap.clear();
                slaveChain.clear();
            }
        }

        private void buildMapOfSlaveRecords() {
            if (!isMapBuilt) {
                TableUtils.populateRowIDHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain);
                isMapBuilt = true;
            }
        }

        private void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            if (!isOpen) {
                isOpen = true;
                joinKeyMap.reopen();
                slaveChain.reopen();
            }
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.circuitBreaker = circuitBreaker;
            masterRecord = masterCursor.getRecord();
            slaveRecord = slaveCursor.getRecordB();
            record.of(masterRecord, slaveRecord);
            slaveChainCursor = null;
            size = -1;
            isMapBuilt = false;
        }
    }
}

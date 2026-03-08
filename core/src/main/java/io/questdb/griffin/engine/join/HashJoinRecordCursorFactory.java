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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

import static io.questdb.griffin.engine.join.AbstractHashOuterJoinRecordCursor.populateRecordHashMap;

public class HashJoinRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final HashJoinRecordCursor cursor;
    private final RecordSink masterKeySink;
    private final RecordSink slaveKeySink;

    public HashJoinRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just 3 LONGs, we store chain references in map
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            RecordSink slaveChainSink,
            int columnSplit,
            JoinContext joinContext
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        Map joinKeyMap = null;
        RecordChain slaveChain = null;
        try {
            joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes);
            slaveChain = new RecordChain(
                    slaveFactory.getMetadata(),
                    slaveChainSink,
                    configuration.getSqlHashJoinValuePageSize(),
                    configuration.getSqlHashJoinValueMaxPages()
            );
            this.masterKeySink = masterKeySink;
            this.slaveKeySink = slaveKeySink;
            cursor = new HashJoinRecordCursor(columnSplit, joinKeyMap, slaveChain);
        } catch (Throwable th) {
            Misc.free(joinKeyMap);
            Misc.free(slaveChain);
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
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
        sink.type("Hash Join");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    private static long computeCursorSizeFromMap(RecordCursor masterCursor, Map map, RecordSink keySink) {
        final Record masterRecord = masterCursor.getRecord();
        long size = 0;
        try {
            masterCursor.toTop();
            while (masterCursor.hasNext()) {
                MapKey key = map.withKey();
                key.put(masterRecord, keySink);
                MapValue value = key.findValue();
                if (value != null) {
                    size += value.getLong(2);
                }
            }
            return size;
        } finally {
            masterCursor.toTop();
        }
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(cursor);
    }

    private class HashJoinRecordCursor extends AbstractJoinCursor {
        private final Map joinKeyMap;
        private final JoinRecord recordA;
        private final RecordChain slaveChain;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMapBuilt;
        private boolean isOpen;
        private Record masterRecord;
        private long size = -1;
        private boolean useSlaveCursor;

        public HashJoinRecordCursor(int columnSplit, Map joinKeyMap, RecordChain slaveChain) {
            super(columnSplit);
            this.recordA = new JoinRecord(columnSplit);
            this.joinKeyMap = joinKeyMap;
            this.slaveChain = slaveChain;
            this.isOpen = true;
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

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public boolean hasNext() {
            buildMapOfSlaveRecords();

            if (useSlaveCursor && slaveChain.hasNext()) {
                return true;
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterKeySink);
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
        public long preComputedStateSize() {
            return isMapBuilt ? 1 : 0;
        }

        @Override
        public long size() {
            if (size != -1) {
                return size;
            }
            buildMapOfSlaveRecords();
            return size = computeCursorSizeFromMap(masterCursor, joinKeyMap, masterKeySink);
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            useSlaveCursor = false;
            if (!isMapBuilt) {
                slaveCursor.toTop();
                joinKeyMap.clear();
                slaveChain.clear();
            }
        }

        private void buildMapOfSlaveRecords() {
            if (!isMapBuilt) {
                populateRecordHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain);
                isMapBuilt = true;
            }
        }

        private void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            if (!isOpen) {
                isOpen = true;
                joinKeyMap.reopen();
            }
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.circuitBreaker = circuitBreaker;
            masterRecord = masterCursor.getRecord();
            Record slaveRecord = slaveChain.getRecord();
            recordA.of(masterRecord, slaveRecord);
            slaveChain.setSymbolTableResolver(slaveCursor);
            useSlaveCursor = false;
            size = -1;
            isMapBuilt = false;
        }
    }
}

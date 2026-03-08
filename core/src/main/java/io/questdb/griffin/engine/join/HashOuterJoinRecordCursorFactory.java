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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

import static io.questdb.griffin.engine.join.HashOuterJoinFilteredLightRecordCursorFactory.outerJoinTypeToString;

public class HashOuterJoinRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final int columnSplit;
    private final int joinType;
    private final RecordSink masterSink;
    private final RecordSink slaveKeySink;
    private AbstractHashOuterJoinRecordCursor cursor;
    private Map joinKeyMap;
    private RecordChain slaveChain;

    public HashOuterJoinRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just 3 LONGs, we store chain references in map
            RecordSink masterSink,
            RecordSink slaveKeySink,
            RecordSink slaveChainSink,
            int columnSplit,
            JoinContext joinContext,
            int joinType
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        try {
            this.masterSink = masterSink;
            this.slaveKeySink = slaveKeySink;
            this.joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes);
            this.slaveChain = new RecordChain(
                    slaveFactory.getMetadata(),
                    slaveChainSink,
                    configuration.getSqlHashJoinValuePageSize(),
                    configuration.getSqlHashJoinValueMaxPages()
            );
            this.columnSplit = columnSplit;
            this.joinType = joinType;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return joinType == QueryModel.JOIN_LEFT_OUTER && masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor == null) {
            switch (joinType) {
                case QueryModel.JOIN_LEFT_OUTER:
                    cursor = new HashLeftOuterJoinFilteredRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                            joinKeyMap,
                            slaveChain
                    );
                    break;
                case QueryModel.JOIN_RIGHT_OUTER:
                    cursor = new HashRightOuterJoinFilteredRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(masterFactory.getMetadata()),
                            joinKeyMap,
                            slaveChain
                    );
                    break;
                case QueryModel.JOIN_FULL_OUTER:
                    cursor = new HashFullOuterJoinFilteredRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(masterFactory.getMetadata()),
                            NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                            joinKeyMap,
                            slaveChain
                    );
                    break;
                default:
                    assert false : "invalid join type " + joinType;
            }
            this.joinKeyMap = null;
            this.slaveChain = null;
        }

        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        RecordCursor masterCursor = null;
        try {
            masterCursor = masterFactory.getCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext);
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        return joinType == QueryModel.JOIN_LEFT_OUTER ? masterFactory.getScanDirection() : SCAN_DIRECTION_OTHER;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Hash ").val(outerJoinTypeToString(joinType)).val(" Outer Join");
        sink.optAttr("condition", joinContext);
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(cursor);
        Misc.free(joinKeyMap);
        Misc.free(slaveChain);
    }

    private class HashFullOuterJoinFilteredRecordCursor extends AbstractHashOuterJoinRecordCursor {
        private final FullOuterJoinRecord record;
        private MapRecordCursor mapCursor;

        public HashFullOuterJoinFilteredRecordCursor(
                int columnSplit,
                Record masterNullRecord,
                Record slaveNullRecord,
                Map joinKeyMap,
                RecordChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            record = new FullOuterJoinRecord(columnSplit, masterNullRecord, slaveNullRecord);
            isOpen = true;
        }

        @Override
        public void close() {
            super.close();
            mapCursor = Misc.free(mapCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                populateRecordHashMapWithMatchFlag(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain);
                isMapBuilt = true;
                record.hasMaster(true);
                mapCursor = joinKeyMap.getCursor();
            }

            if (useSlaveCursor && slaveChain.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                return true;
            }

            if (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChain.of(value.getLong(0));
                    slaveChain.hasNext();
                    useSlaveCursor = true;
                    record.hasSlave(true);
                    value.putBool(3, true); // mark as matched
                    return true;
                } else {
                    useSlaveCursor = false;
                    record.hasSlave(false);
                }
                return true;
            }

            record.hasMaster(false);
            record.hasSlave(true);
            while (mapCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapValue value = mapCursor.getRecord().getValue();
                if (!value.getBool(3)) { // if not matched
                    slaveChain.of(value.getLong(0));
                    slaveChain.hasNext();
                    useSlaveCursor = true;
                    return true;
                }
            }

            return false;
        }

        @Override
        public void toTop() {
            super.toTop();
            if (mapCursor != null) {
                mapCursor.toTop();
            }
            record.hasMaster(true);
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            super.of(masterCursor, slaveCursor, sqlExecutionContext);
            record.of(masterRecord, slaveRecord);
            this.mapCursor = Misc.free(mapCursor);
        }
    }

    private class HashLeftOuterJoinFilteredRecordCursor extends AbstractHashOuterJoinRecordCursor {
        private final OuterJoinRecord record;

        public HashLeftOuterJoinFilteredRecordCursor(
                int columnSplit,
                Record nullRecord,
                Map joinKeyMap,
                RecordChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            record = new OuterJoinRecord(columnSplit, nullRecord);
            isOpen = true;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                populateRecordHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain);
                isMapBuilt = true;
            }

            circuitBreaker.statefulThrowExceptionIfTripped();

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
            super.toTop();
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            super.of(masterCursor, slaveCursor, sqlExecutionContext);
            record.of(masterRecord, slaveRecord);
        }
    }

    private class HashRightOuterJoinFilteredRecordCursor extends AbstractHashOuterJoinRecordCursor {
        private final RightOuterJoinRecord record;
        private MapRecordCursor mapCursor;

        public HashRightOuterJoinFilteredRecordCursor(
                int columnSplit,
                Record nullRecord,
                Map joinKeyMap,
                RecordChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            record = new RightOuterJoinRecord(columnSplit, nullRecord);
            isOpen = true;
        }

        @Override
        public void close() {
            super.close();
            mapCursor = Misc.free(mapCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                populateRecordHashMapWithMatchFlag(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain);
                isMapBuilt = true;
                record.hasMaster(true);
                mapCursor = joinKeyMap.getCursor();
            }

            if (useSlaveCursor && slaveChain.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                return true;
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChain.of(value.getLong(0));
                    useSlaveCursor = true;
                    value.putBool(3, true); // mark as matched
                    slaveChain.hasNext();
                    return true;
                }
            }

            record.hasMaster(false);
            while (mapCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapValue value = mapCursor.getRecord().getValue();
                if (!value.getBool(3)) { // if not matched
                    slaveChain.of(value.getLong(0));
                    useSlaveCursor = true;
                    slaveChain.hasNext();
                    return true;
                }
            }

            return false;
        }

        @Override
        public void toTop() {
            super.toTop();
            if (mapCursor != null) {
                mapCursor.toTop();
            }
            record.hasMaster(true);
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            super.of(masterCursor, slaveCursor, sqlExecutionContext);
            record.of(masterRecord, slaveRecord);
            this.mapCursor = Misc.free(mapCursor);
        }
    }
}

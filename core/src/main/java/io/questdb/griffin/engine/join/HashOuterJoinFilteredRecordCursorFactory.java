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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordIdSink;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
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
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.engine.join.HashOuterJoinFilteredLightRecordCursorFactory.outerJoinTypeToString;

/**
 * Same as HashOuterJoinRecordCursorFactory but with added filtering (for non-equality
 * or complex join conditions that use functions).
 */
public class HashOuterJoinFilteredRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final int columnSplit;
    private final Function filter;
    private final int joinType;
    private final RecordSink masterSink;
    private final RecordSink slaveKeySink;
    private AbstractHashOuterJoinRecordCursor cursor;
    private Map joinKeyMap;
    private Map matchIdsMap;
    private RecordChain slaveChain;

    public HashOuterJoinFilteredRecordCursorFactory(
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
            @NotNull Function filter,
            JoinContext joinContext,
            int joinType
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        try {
            this.masterSink = masterSink;
            this.slaveKeySink = slaveKeySink;
            this.joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes);
            this.slaveChain = new RecordChain(slaveFactory.getMetadata(),
                    slaveChainSink,
                    configuration.getSqlHashJoinValuePageSize(),
                    configuration.getSqlHashJoinValueMaxPages());
            this.columnSplit = columnSplit;
            this.joinType = joinType;
            if (joinType != QueryModel.JOIN_LEFT_OUTER) {
                matchIdsMap = MapFactory.createUnorderedMap(configuration, RecordIdSink.RECORD_ID_COLUMN_TYPE, ArrayColumnTypes.EMPTY);
            }
            this.filter = filter;
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
                            matchIdsMap,
                            slaveChain
                    );
                    break;
                case QueryModel.JOIN_FULL_OUTER:
                    cursor = new HashFullOuterJoinFilteredRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(masterFactory.getMetadata()),
                            NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                            joinKeyMap,
                            matchIdsMap,
                            slaveChain
                    );
                    break;
                default:
                    assert false : "invalid join type " + joinType;
            }
            this.joinKeyMap = null;
            this.slaveChain = null;
            this.matchIdsMap = null;
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
        sink.attr("filter").val(filter);
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(cursor);
        Misc.free(filter);
        Misc.free(joinKeyMap);
        Misc.free(slaveChain);
        Misc.free(matchIdsMap);
    }

    private class HashFullOuterJoinFilteredRecordCursor extends AbstractHashOuterJoinRecordCursor {
        private final Map matchIdsMap;
        private final FullOuterJoinRecord record;
        private MapRecordCursor mapCursor;

        public HashFullOuterJoinFilteredRecordCursor(
                int columnSplit,
                Record masterNullRecord,
                Record slaveNullRecord,
                Map joinKeyMap,
                Map matchIdsMap,
                RecordChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            record = new FullOuterJoinRecord(columnSplit, masterNullRecord, slaveNullRecord);
            this.matchIdsMap = matchIdsMap;
            isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                matchIdsMap.close();
            }
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
                populateRecordHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain);
                isMapBuilt = true;
                record.hasMaster(true);
                mapCursor = joinKeyMap.getCursor();
            }

            if (useSlaveCursor && slaveChain.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                do {
                    if (record.hasMaster()) {
                        if (filter.getBool(record)) {
                            MapKey keys = matchIdsMap.withKey();
                            keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                            keys.createValue();
                            return true;
                        }
                    } else {
                        MapKey keys = matchIdsMap.withKey();
                        keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                        if (keys.findValue() == null) {
                            return true;
                        }
                    }
                } while (slaveChain.hasNext());
                useSlaveCursor = false;
            }

            if (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChain.of(value.getLong(0));
                    useSlaveCursor = true;
                    record.hasSlave(true);
                    while (slaveChain.hasNext()) {
                        if (filter.getBool(record)) {
                            MapKey keys = matchIdsMap.withKey();
                            keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                            keys.createValue();
                            return true;
                        }
                    }
                }

                useSlaveCursor = false;
                record.hasSlave(false);
                return true;
            }

            record.hasMaster(false);
            record.hasSlave(true);
            while (mapCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapValue value = mapCursor.getRecord().getValue();
                slaveChain.of(value.getLong(0));
                while (slaveChain.hasNext()) {
                    MapKey keys = matchIdsMap.withKey();
                    keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                    if (keys.findValue() == null) {
                        useSlaveCursor = true;
                        return true;
                    }
                }
            }

            return false;
        }

        @Override
        public void toTop() {
            super.toTop();
            filter.toTop();
            if (mapCursor != null) {
                mapCursor.toTop();
            }
            record.hasMaster(true);
            if (!isMapBuilt) {
                matchIdsMap.clear();
            }
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            if (!isOpen) {
                matchIdsMap.reopen();
            }
            super.of(masterCursor, slaveCursor, sqlExecutionContext);
            record.of(masterRecord, slaveRecord);
            filter.init(this, sqlExecutionContext);
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

            if (useSlaveCursor) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                while (slaveChain.hasNext()) {
                    if (filter.getBool(record)) {
                        return true;
                    }
                }
            }

            if (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChain.of(value.getLong(0));
                    useSlaveCursor = true;
                    record.hasSlave(true);
                    while (slaveChain.hasNext()) {
                        if (filter.getBool(record)) {
                            return true;
                        }
                    }
                }
                useSlaveCursor = false;
                record.hasSlave(false);
                return true;
            }
            return false;
        }

        @Override
        public void toTop() {
            super.toTop();
            filter.toTop();
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            super.of(masterCursor, slaveCursor, sqlExecutionContext);
            record.of(masterRecord, slaveRecord);
            filter.init(this, sqlExecutionContext);
        }
    }

    private class HashRightOuterJoinFilteredRecordCursor extends AbstractHashOuterJoinRecordCursor {
        private final Map matchIdsMap;
        private final RightOuterJoinRecord record;
        private MapRecordCursor mapCursor;

        public HashRightOuterJoinFilteredRecordCursor(
                int columnSplit,
                Record nullRecord,
                Map joinKeyMap,
                Map matchIdsMap,
                RecordChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            record = new RightOuterJoinRecord(columnSplit, nullRecord);
            this.matchIdsMap = matchIdsMap;
            isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                matchIdsMap.close();
            }
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
                populateRecordHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain);
                isMapBuilt = true;
                record.hasMaster(true);
                mapCursor = joinKeyMap.getCursor();
            }

            if (useSlaveCursor) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                while (slaveChain.hasNext()) {
                    if (record.hasMaster()) {
                        if (filter.getBool(record)) {
                            MapKey keys = matchIdsMap.withKey();
                            keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                            keys.createValue();
                            return true;
                        }
                    } else {
                        MapKey keys = matchIdsMap.withKey();
                        keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                        if (keys.findValue() == null) {
                            return true;
                        }
                    }
                }
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChain.of(value.getLong(0));
                    useSlaveCursor = true;
                    while (slaveChain.hasNext()) {
                        if (filter.getBool(record)) {
                            MapKey keys = matchIdsMap.withKey();
                            keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                            keys.createValue();
                            return true;
                        }
                    }
                }
            }

            record.hasMaster(false);
            while (mapCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapValue value = mapCursor.getRecord().getValue();
                slaveChain.of(value.getLong(0));
                while (slaveChain.hasNext()) {
                    MapKey keys = matchIdsMap.withKey();
                    keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                    if (keys.findValue() == null) {
                        useSlaveCursor = true;
                        return true;
                    }
                }
            }

            return false;
        }

        @Override
        public void toTop() {
            super.toTop();
            filter.toTop();
            if (mapCursor != null) {
                mapCursor.toTop();
            }
            record.hasMaster(true);
            if (!isMapBuilt) {
                matchIdsMap.clear();
            }
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            if (!isOpen) {
                matchIdsMap.reopen();
            }
            super.of(masterCursor, slaveCursor, sqlExecutionContext);
            record.of(masterRecord, slaveRecord);
            filter.init(this, sqlExecutionContext);
            this.mapCursor = Misc.free(mapCursor);
        }
    }
}

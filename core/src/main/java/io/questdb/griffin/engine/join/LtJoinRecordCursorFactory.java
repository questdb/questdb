/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.RecordValueSink;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

public class LtJoinRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final LtJoinRecordCursor cursor;
    private final int mapEvacuationThreshold;
    private final RecordSink masterKeySink;
    private final IntList slaveColumnIndex; // maps columns after the split to columns in the slave cursor
    private final RecordSink slaveKeySink;
    private final int slaveValueTimestampIndex;
    private final long toleranceInterval;

    public LtJoinRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes mapKeyTypes,
            @Transient ColumnTypes mapValueTypes,
            @Transient ColumnTypes slaveColumnTypes,
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            int columnSplit,
            RecordValueSink slaveValueSink,
            IntList columnIndex, // this column index will be used to retrieve symbol tables from underlying slave
            JoinContext joinContext,
            ColumnFilter masterTableKeyColumns,
            long toleranceInterval,
            int slaveValueTimestampIndex
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        try {
            this.masterKeySink = masterKeySink;
            this.slaveKeySink = slaveKeySink;
            Map joinKeyMapA = MapFactory.createUnorderedMap(configuration, mapKeyTypes, mapValueTypes);
            // if toleranceInterval is not set, we do not need a second map for evacuation. since evacuations are only
            // executed when TOLERANCE_INTERVAL is set
            Map joinKeyMapB = toleranceInterval != Numbers.LONG_NULL ? MapFactory.createUnorderedMap(configuration, mapKeyTypes, mapValueTypes) : null;
            int slaveWrappedOverMaster = slaveColumnTypes.getColumnCount() - masterTableKeyColumns.getColumnCount();
            this.cursor = new LtJoinRecordCursor(
                    columnSplit,
                    joinKeyMapA,
                    joinKeyMapB,
                    NullRecordFactory.getInstance(slaveColumnTypes),
                    masterFactory.getMetadata().getTimestampIndex(),
                    slaveFactory.getMetadata().getTimestampIndex(),
                    masterFactory.getMetadata().getTimestampType(),
                    slaveFactory.getMetadata().getTimestampType(),
                    slaveValueSink,
                    masterTableKeyColumns,
                    slaveWrappedOverMaster,
                    columnIndex
            );
            this.slaveColumnIndex = columnIndex;
            this.toleranceInterval = toleranceInterval;
            this.slaveValueTimestampIndex = slaveValueTimestampIndex;
            this.mapEvacuationThreshold = configuration.getSqlAsOfJoinMapEvacuationThreshold();
        } catch (Throwable th) {
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
        // Forcefully disable column pre-touch for nested filter queries.
        executionContext.setColumnPreTouchEnabled(false);
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        RecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getCursor(executionContext);
            cursor.of(masterCursor, slaveCursor);
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
    public void toPlan(PlanSink sink) {
        sink.type("Lt Join");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(cursor);
    }

    private class LtJoinRecordCursor extends AbstractSymbolWrapOverCursor {
        private final Map joinKeyMapA;
        private final Map joinKeyMapB;
        private final int masterTimestampIndex;
        private final SymbolWrapOverJoinRecord record;
        private final int slaveTimestampIndex;
        private final RecordValueSink valueSink;
        private Map currentJoinKeyMap;
        private boolean danglingSlaveRecord = false;
        private boolean isMasterHasNextPending;
        private boolean isOpen;
        private boolean masterHasNext;
        private Record masterRecord;
        private Record slaveRecord;
        private long slaveTimestamp = Long.MIN_VALUE;

        public LtJoinRecordCursor(
                int columnSplit,
                Map joinKeyMapA,
                Map joinKeyMapB,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                RecordValueSink valueSink,
                ColumnFilter masterTableKeyColumns,
                int slaveWrappedOverMaster,
                IntList slaveColumnIndex
        ) {
            super(columnSplit, slaveWrappedOverMaster, masterTableKeyColumns, slaveColumnIndex, masterTimestampType, slaveTimestampType);
            this.record = new SymbolWrapOverJoinRecord(columnSplit, nullRecord, slaveWrappedOverMaster, masterTableKeyColumns);
            this.joinKeyMapA = joinKeyMapA;
            this.joinKeyMapB = joinKeyMapB;
            this.currentJoinKeyMap = joinKeyMapA;
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
            this.valueSink = valueSink;
            this.isOpen = true;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            masterCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                joinKeyMapA.close();
                if (joinKeyMapB != null) {
                    joinKeyMapB.close();
                }
                super.close();
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
            if (masterHasNext) {
                final long masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
                final long minSlaveTimestamp = toleranceInterval == Numbers.LONG_NULL ? Long.MIN_VALUE : masterTimestamp - toleranceInterval;
                MapKey key;
                MapValue value;
                long slaveTimestamp = this.slaveTimestamp;
                if (slaveTimestamp < masterTimestamp) {
                    if (danglingSlaveRecord) {
                        if (slaveTimestamp >= minSlaveTimestamp) {
                            key = currentJoinKeyMap.withKey();
                            key.put(slaveRecord, slaveKeySink);
                            value = key.createValue();
                            valueSink.copy(slaveRecord, value);
                        }
                        danglingSlaveRecord = false;
                    }

                    evacuateJoinKeyMap(masterTimestamp);

                    while (slaveCursor.hasNext()) {
                        slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                        if (slaveTimestamp < masterTimestamp) {
                            if (slaveTimestamp >= minSlaveTimestamp) {
                                key = currentJoinKeyMap.withKey();
                                key.put(slaveRecord, slaveKeySink);
                                value = key.createValue();
                                valueSink.copy(slaveRecord, value);
                            }
                        } else {
                            danglingSlaveRecord = true;
                            break;
                        }
                    }

                    this.slaveTimestamp = slaveTimestamp;
                }
                key = currentJoinKeyMap.withKey();
                key.put(masterRecord, masterKeySink);
                value = key.findValue();
                if (value != null) {
                    value.setMapRecordHere();
                    if (toleranceInterval == Numbers.LONG_NULL) {
                        record.hasSlave(true);
                    } else {
                        long slaveRecordTimestamp = scaleTimestamp(value.getTimestamp(slaveValueTimestampIndex), slaveTimestampScale);
                        long minTimestamp = masterTimestamp - toleranceInterval;
                        record.hasSlave(slaveRecordTimestamp >= minTimestamp);
                    }
                } else {
                    record.hasSlave(false);
                }

                isMasterHasNextPending = true;
                return true;
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return masterCursor.size();
        }

        @Override
        public void toTop() {
            currentJoinKeyMap.clear();
            currentJoinKeyMap = joinKeyMapA;
            assert currentJoinKeyMap.size() == 0;
            slaveTimestamp = Long.MIN_VALUE;
            danglingSlaveRecord = false;
            masterCursor.toTop();
            slaveCursor.toTop();
            isMasterHasNextPending = true;
        }

        /**
         * Evacuates the join key map by copying records that are not older than `masterTimestamp - toleranceInterval`
         * This is useful when some of the join keys are unique and the map is ever-growing.
         *
         * @param masterTimestamp the timestamp of the current master record
         */
        private void evacuateJoinKeyMap(long masterTimestamp) {
            if (toleranceInterval == Numbers.LONG_NULL || currentJoinKeyMap.size() < mapEvacuationThreshold) {
                return; // no need to evacuate small maps
            }
            assert joinKeyMapB != null : "Join key map B must not be null";
            Map dstMap = currentJoinKeyMap == joinKeyMapA ? joinKeyMapB : joinKeyMapA;
            assert dstMap.size() == 0 : "Evacuating non-empty map: " + dstMap.size();

            MapRecordCursor srcMapCursor = currentJoinKeyMap.getCursor();
            MapRecord srcRecord = currentJoinKeyMap.getRecord();
            long minTimestamp = masterTimestamp - toleranceInterval;
            while (srcMapCursor.hasNext()) {
                MapValue srcValue = srcRecord.getValue();
                long srcTimestamp = scaleTimestamp(srcValue.getTimestamp(slaveValueTimestampIndex), slaveTimestampScale);
                if (srcTimestamp < minTimestamp) {
                    continue; // skip records that are too old
                }
                long srcKeyHash = srcRecord.keyHashCode();
                MapKey dstKey = dstMap.withKey();
                srcRecord.copyToKey(dstKey);
                MapValue dstValue = dstKey.createValue(srcKeyHash);
                srcRecord.copyValue(dstValue);
            }
            currentJoinKeyMap.clear();
            currentJoinKeyMap = dstMap;

            // we need to update the record to point to the new map
            // and we have to preserve the slave record state too since of() implicitly set hasSlave() to true
            boolean hasSlave = record.hasSlave();
            record.of(masterRecord, currentJoinKeyMap.getRecord());
            record.hasSlave(hasSlave);
        }

        private void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            if (!isOpen) {
                isOpen = true;
                joinKeyMapA.reopen();
                if (joinKeyMapB != null) {
                    // reopen joinKeyMapB only if it was created
                    joinKeyMapB.reopen();
                }
            }
            currentJoinKeyMap = joinKeyMapA;
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            slaveTimestamp = Long.MIN_VALUE;
            danglingSlaveRecord = false;
            masterRecord = masterCursor.getRecord();
            slaveRecord = slaveCursor.getRecord();

            MapRecord mapRecordA = joinKeyMapA.getRecord();
            mapRecordA.setSymbolTableResolver(slaveCursor, slaveColumnIndex);
            record.of(masterRecord, mapRecordA);

            if (joinKeyMapB != null) {
                MapRecord mapRecordB = joinKeyMapB.getRecord();
                mapRecordB.setSymbolTableResolver(slaveCursor, slaveColumnIndex);
            }
            isMasterHasNextPending = true;
        }
    }
}

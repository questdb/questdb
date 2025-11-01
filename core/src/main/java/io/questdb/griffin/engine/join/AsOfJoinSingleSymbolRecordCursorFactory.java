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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
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
import io.questdb.std.Numbers;
import io.questdb.std.Transient;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

public class AsOfJoinSingleSymbolRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsofJoinColumnAccessHelper columnAccessHelper;
    private final AsOfSingleSymbolJoinRecordCursor cursor;
    private final int slaveSymbolColumnIndex;
    private final long toleranceInterval;

    public AsOfJoinSingleSymbolRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just LONG, we store row ids in map
            int columnSplit,
            int slaveSymbolColumnIndex,
            AsofJoinColumnAccessHelper columnAccessHelper,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        this.slaveSymbolColumnIndex = slaveSymbolColumnIndex;
        this.columnAccessHelper = columnAccessHelper;
        this.toleranceInterval = toleranceInterval;
        Map joinKeyMap = null;
        try {
            joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes);
            this.cursor = new AsOfSingleSymbolJoinRecordCursor(
                    columnSplit,
                    joinKeyMap,
                    NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                    masterFactory.getMetadata().getTimestampIndex(),
                    slaveFactory.getMetadata().getTimestampIndex(),
                    masterFactory.getMetadata().getTimestampType(),
                    slaveFactory.getMetadata().getTimestampType()
            );
        } catch (Throwable th) {
            Misc.free(joinKeyMap);
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
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        RecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getCursor(executionContext);
            cursor.of(masterCursor, slaveCursor);
        } catch (Throwable ex) {
            Misc.free(masterCursor);
            Misc.free(slaveCursor);
            throw ex;
        }
        return cursor;
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
        sink.type("AsOf Join Single Symbol");
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

    private class AsOfSingleSymbolJoinRecordCursor extends AbstractJoinCursor {
        private final Map joinKeyMap;
        private final int masterTimestampIndex;
        private final long masterTimestampScale;
        private final OuterJoinRecord record;
        private final int slaveTimestampIndex;
        private final long slaveTimestampScale;
        private boolean isMasterHasNextPending;
        private boolean isOpen;
        private long lastSlaveRowID = Long.MIN_VALUE;
        private boolean masterHasNext;
        private Record masterRecord;
        private Record slaveRecord;
        private long slaveTimestamp = Long.MIN_VALUE;

        public AsOfSingleSymbolJoinRecordCursor(
                int columnSplit,
                Map joinKeyMap,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType
        ) {
            super(columnSplit);
            record = new OuterJoinRecord(columnSplit, nullRecord);
            this.joinKeyMap = joinKeyMap;
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
            isOpen = true;
            if (masterTimestampType == slaveTimestampType) {
                masterTimestampScale = slaveTimestampScale = 1L;
            } else {
                masterTimestampScale = ColumnType.getTimestampDriver(masterTimestampType).toNanosScale();
                slaveTimestampScale = ColumnType.getTimestampDriver(slaveTimestampType).toNanosScale();
            }
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            masterCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                slaveTimestamp = Long.MIN_VALUE;
                lastSlaveRowID = Long.MIN_VALUE;
                joinKeyMap.close();
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
                int slaveSymbolKey = columnAccessHelper.getSlaveKey(masterRecord);
                MapKey key;
                MapValue value;
                long slaveTimestamp = this.slaveTimestamp;
                long slaveRowID = this.lastSlaveRowID;
                if (slaveTimestamp <= masterTimestamp) {
                    if (lastSlaveRowID != Numbers.LONG_NULL) {
                        slaveCursor.recordAt(slaveRecord, lastSlaveRowID);
                        slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                        if (slaveTimestamp >= minSlaveTimestamp) {
                            key = joinKeyMap.withKey();
                            key.putInt(slaveRecord.getInt(slaveSymbolColumnIndex));
                            value = key.createValue();
                            value.putLong(0, lastSlaveRowID);
                        }
                    }

                    // NOTE: unlike full fat ASOF JOIN, we don't evacuate joinKeyMap here.
                    // Reasoning: The joinKeyMap here contains only rowNo, so evacuation
                    // would require to dereference rowNo to record to get a timestamp
                    // to decide whether to keep the record or not. This could be expensive.
                    // Given map values are just row IDs, I decided not to evacuate
                    // maps in Light ASOF JOINs

                    final Record rec = slaveCursor.getRecord();
                    while (slaveCursor.hasNext()) {
                        slaveTimestamp = scaleTimestamp(rec.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                        slaveRowID = rec.getRowId();
                        if (slaveTimestamp <= masterTimestamp) {
                            if (slaveTimestamp >= minSlaveTimestamp) {
                                key = joinKeyMap.withKey();
                                key.putInt(rec.getInt(slaveSymbolColumnIndex));
                                value = key.createValue();
                                value.putLong(0, rec.getRowId());
                            }
                        } else {
                            break;
                        }
                    }

                    // now we have dangling slave record, which we need to hold on to
                    this.slaveTimestamp = slaveTimestamp;
                    this.lastSlaveRowID = slaveRowID;
                }
                key = joinKeyMap.withKey();
                key.putInt(slaveSymbolKey);
                value = key.findValue();
                if (value != null) {
                    slaveCursor.recordAt(slaveRecord, value.getLong(0));
                    slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                    record.hasSlave(toleranceInterval == Numbers.LONG_NULL || slaveTimestamp >= masterTimestamp - toleranceInterval);
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
            return masterCursor.preComputedStateSize() + slaveCursor.preComputedStateSize();
        }

        @Override
        public long size() {
            return masterCursor.size();
        }

        @Override
        public void toTop() {
            joinKeyMap.clear();
            slaveTimestamp = Long.MIN_VALUE;
            lastSlaveRowID = Long.MIN_VALUE;
            masterCursor.toTop();
            slaveCursor.toTop();
            isMasterHasNextPending = true;
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            if (!isOpen) {
                isOpen = true;
                joinKeyMap.reopen();
            }
            slaveTimestamp = Long.MIN_VALUE;
            lastSlaveRowID = Long.MIN_VALUE;
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            masterRecord = masterCursor.getRecord();
            slaveRecord = slaveCursor.getRecordB();
            record.of(masterRecord, slaveRecord);
            columnAccessHelper.of(slaveCursor);
            isMasterHasNextPending = true;
        }
    }
}

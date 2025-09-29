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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

public class LtJoinNoKeyRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final LtJoinNoKeyJoinRecordCursor cursor;

    public LtJoinNoKeyRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            long toleranceInterval
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        this.cursor = new LtJoinNoKeyJoinRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampIndex(),
                masterFactory.getMetadata().getTimestampType(),
                slaveFactory.getMetadata().getTimestampType(),
                toleranceInterval
        );
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
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
    }

    private static class LtJoinNoKeyJoinRecordCursor extends AbstractJoinCursor {
        private final int masterTimestampIndex;
        private final long masterTimestampScale;
        private final OuterJoinRecord record;
        private final int slaveTimestampIndex;
        private final long slaveTimestampScale;
        private final long toleranceInterval;
        private boolean isMasterHasNextPending;
        private long latestSlaveRowID = Long.MIN_VALUE;
        private boolean masterHasNext;
        private Record masterRecord;
        private long slaveATimestamp = Long.MIN_VALUE;
        private long slaveBTimestamp = Long.MIN_VALUE;
        private Record slaveRecA;  // used for internal navigation
        private Record slaveRecB; // used by OuterJoinRecord

        public LtJoinNoKeyJoinRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                long toleranceInterval
        ) {
            super(columnSplit);
            this.record = new OuterJoinRecord(columnSplit, nullRecord);
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
            this.toleranceInterval = toleranceInterval;
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
                // great, we have a record no matter what
                final long masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
                if (masterTimestamp <= slaveATimestamp) {
                    isMasterHasNextPending = true;
                    adjustForTolerance(masterTimestamp);
                    return true;
                }
                nextSlave(masterTimestamp);
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
            slaveATimestamp = Long.MIN_VALUE;
            slaveBTimestamp = Long.MIN_VALUE;
            latestSlaveRowID = Long.MIN_VALUE;
            record.hasSlave(false);
            masterCursor.toTop();
            slaveCursor.toTop();
            isMasterHasNextPending = true;
        }

        private void adjustForTolerance(long masterTimestamp) {
            if (toleranceInterval == Numbers.LONG_NULL || !record.hasSlave()) {
                return;
            }
            record.hasSlave(slaveBTimestamp >= masterTimestamp - toleranceInterval);
        }

        private void nextSlave(long masterTimestamp) {
            while (true) {
                boolean slaveHasNext = slaveCursor.hasNext();
                if (latestSlaveRowID != Long.MIN_VALUE) {
                    record.hasSlave(true);
                    slaveCursor.recordAt(slaveRecB, latestSlaveRowID);
                }
                if (slaveHasNext) {
                    slaveATimestamp = scaleTimestamp(slaveRecA.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                    latestSlaveRowID = slaveRecA.getRowId();
                    if (slaveATimestamp >= masterTimestamp) {
                        break;
                    }
                } else {
                    slaveATimestamp = Long.MAX_VALUE;
                    break;
                }
            }
            if (record.hasSlave()) {
                slaveBTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                long minSlaveTimestamp = toleranceInterval == Numbers.LONG_NULL ? Long.MIN_VALUE : masterTimestamp - toleranceInterval;
                record.hasSlave(slaveBTimestamp >= minSlaveTimestamp);
            }
            assert !record.hasSlave() || slaveBTimestamp < masterTimestamp;
        }

        private void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            slaveATimestamp = Long.MIN_VALUE;
            slaveBTimestamp = Long.MIN_VALUE;
            latestSlaveRowID = Long.MIN_VALUE;
            masterRecord = masterCursor.getRecord();
            slaveRecA = slaveCursor.getRecord();
            slaveRecB = slaveCursor.getRecordB();
            record.of(masterRecord, slaveRecB);
            record.hasSlave(false);
            isMasterHasNextPending = true;
        }
    }
}

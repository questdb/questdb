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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

public class LtJoinNoKeyFastRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final LtJoinFastRecordCursor cursor;

    public LtJoinNoKeyFastRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            long toleranceInterval
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.cursor = new LtJoinFastRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampIndex(),
                masterFactory.getMetadata().getTimestampType(),
                slaveFactory.getMetadata().getTimestampType(),
                configuration.getSqlAsOfJoinLookAhead(),
                toleranceInterval
        );
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameRecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
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
        sink.type("Lt Join Fast Scan");
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
    }

    private static class LtJoinFastRecordCursor extends AbstractAsOfJoinFastRecordCursor {

        private final long toleranceInterval;
        private long slaveTimestamp = Numbers.LONG_NULL;

        public LtJoinFastRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                int lookahead,
                long toleranceInterval) {
            super(columnSplit, nullRecord, masterTimestampIndex, slaveTimestampIndex, masterTimestampType, slaveTimestampType, lookahead);
            this.toleranceInterval = toleranceInterval;
        }

        @Override
        public boolean hasNext() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
            if (masterHasNext) {
                final long masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
                if (masterTimestamp <= lookaheadTimestamp) {
                    if (toleranceInterval != Numbers.LONG_NULL && slaveTimestamp < masterTimestamp - toleranceInterval) {
                        record.hasSlave(false);
                    }
                    isMasterHasNextPending = true;
                    return true;
                }
                nextSlave(masterTimestamp - 1);
                if (toleranceInterval != Numbers.LONG_NULL && record.hasSlave()) {
                    slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                    record.hasSlave(slaveTimestamp >= masterTimestamp - toleranceInterval);
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
        public void toTop() {
            super.toTop();
            slaveTimestamp = Numbers.LONG_NULL;
        }
    }
}

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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Rows;

public class AsOfJoinKeyedFastRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsOfJoinKeyedFastRecordCursor cursor;

    public AsOfJoinKeyedFastRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.cursor = new AsOfJoinKeyedFastRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampIndex(),
                configuration.getSqlAsOfJoinLookAhead()
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
        sink.type("AsOf Join Keyed Fast Scan");
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
    }


    private static class AsOfJoinKeyedFastRecordCursor extends AbstractAsOfJoinFastRecordCursor {

        public AsOfJoinKeyedFastRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int lookahead
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, slaveTimestampIndex, lookahead);
        }

        @Override
        public boolean hasNext() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
            if (masterHasNext) {
                final long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
                if (masterTimestamp < lookaheadTimestamp) {
                    isMasterHasNextPending = true;
                    return true;
                }
                nextSlave(masterTimestamp);
                isMasterHasNextPending = true;
                boolean hasSlave = record.hasSlave();
                if (!hasSlave) {
                    return true;
                }

                long masterId = masterRecord.getLong(1);

                TimeFrame timeFrame = slaveCursor.getTimeFrame();
                assert timeFrame.isOpen();

                long rowHi = timeFrame.getRowHi();
                long rowLo = timeFrame.getRowLo();

                assert slaveFrameRow >= rowLo && slaveFrameRow < rowHi;

                long keyedRowId = slaveFrameRow;
                int keyedFrameIndex = slaveFrameIndex;
                for (; ; ) {
                    long slaveId = slaveRecB.getLong(0);
                    if (masterId == slaveId) {
                        // we have a match, that's awesome, no need to traverse the slave cursor!
                        break;
                    }

                    // let's try to move backwards in the slave cursor until we have a match
                    keyedRowId--;
                    if (keyedRowId < rowLo) {
                        // ops, we exhausted this frame, let's try the previous one
                        if (!slaveCursor.prev()) {
                            // there is no previous frame, we are done, no match :(
                            // if we are here, chances are we are also pretty slow because we are scanning the entire slave cursor!
                            record.hasSlave(false);
                            break;
                        }
                        slaveCursor.open();

                        keyedFrameIndex = timeFrame.getIndex();
                        keyedRowId = timeFrame.getRowHi() - 1; // should it be -1? I never know. inclusive, exclusive, it's all a blur. I assume this one is exclusive. to be checked.
                        rowLo = timeFrame.getRowLo();
                        // todo: shouldn't we return the frame back to the original slaveFrameIndex?
                    }
                    slaveCursor.recordAt(slaveRecB, Rows.toRowID(keyedFrameIndex, keyedRowId));
                }
                return true;
            }
            return false;
        }
    }
}

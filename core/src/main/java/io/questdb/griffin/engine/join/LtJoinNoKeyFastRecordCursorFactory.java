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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Rows;

public class LtJoinNoKeyFastRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsOfJoinFastRecordCursor cursor;

    public LtJoinNoKeyFastRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.cursor = new AsOfJoinFastRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampIndex(),
                configuration.getSqlAsOfJoinLookAhead()
        );
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
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
    }

    private static class AsOfJoinFastRecordCursor extends AbstractAsOfJoinFastRecordCursor {

        public AsOfJoinFastRecordCursor(
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
                if (masterTimestamp <= lookaheadTimestamp) {
                    isMasterHasNextPending = true;
                    return true;
                }
                nextSlave(masterTimestamp);
                isMasterHasNextPending = true;
                return true;
            }
            return false;
        }

        private void nextSlave(long masterTimestamp) {
            final TimeFrame frame = slaveCursor.getTimeFrame();
            while (true) {
                if (frame.isOpen() && frame.getIndex() == slaveFrameIndex) {
                    // Scan a few rows to speed up self-join/identical tables cases.
                    if (linearScan(frame, masterTimestamp - 1)) {
                        return;
                    }
                    if (slaveFrameRow < frame.getRowHi()) {
                        // Fallback to binary search.
                        // Find the last value less than the master timestamp.
                        long foundRow = binarySearch(masterTimestamp - 1, slaveFrameRow, frame.getRowHi() - 1);
                        if (foundRow < slaveFrameRow) {
                            // All searched timestamps are equal or greater than the master timestamp.
                            // Linear scan must have found the row.
                            return;
                        }
                        slaveFrameRow = foundRow;
                        record.hasSlave(true);
                        slaveCursor.recordAt(slaveRecB, Rows.toRowID(slaveFrameIndex, slaveFrameRow));
                        long slaveTimestamp = slaveRecB.getTimestamp(slaveTimestampIndex);
                        if (slaveFrameRow < frame.getRowHi() - 1) {
                            slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, slaveFrameRow + 1));
                            lookaheadTimestamp = slaveRecA.getTimestamp(slaveTimestampIndex);
                        } else {
                            lookaheadTimestamp = slaveTimestamp;
                        }
                        if (foundRow < frame.getRowHi() - 1 || lookaheadTimestamp == masterTimestamp - 1) {
                            // We've found the row, so there is no point in checking the next partition.
                            return;
                        }
                    }
                }
                if (!openSlaveFrame(frame, masterTimestamp - 1)) {
                    return;
                }
            }
        }
    }
}

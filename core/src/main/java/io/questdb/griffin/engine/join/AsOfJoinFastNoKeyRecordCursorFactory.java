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

public class AsOfJoinFastNoKeyRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsOfJoinFastRecordCursor cursor;

    public AsOfJoinFastNoKeyRecordCursorFactory(
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
                configuration.getSqlAsOfJoinLookahead()
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
        sink.type("AsOf Join Fast Scan");
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
    }

    private enum ScanDirection {
        FORWARD, BACKWARD
    }

    private static class AsOfJoinFastRecordCursor implements NoRandomAccessRecordCursor {
        private final int columnSplit;
        private final int lookahead;
        private final int masterTimestampIndex;
        private final OuterJoinRecord record;
        private final int slaveTimestampIndex;
        private boolean isMasterHasNextPending;
        // stands for forward/backward scan through slave's time frames
        private boolean isSlaveForwardScan;
        private boolean isSlaveOpenPending;
        private RecordCursor masterCursor;
        private boolean masterHasNext;
        private Record masterRecord;
        private TimeFrameRecordCursor slaveCursor;
        private int slaveFrameIndex = -1;
        private long slaveFrameRow = Long.MIN_VALUE;
        private Record slaveRecA;
        private Record slaveRecB;
        private long slaveTimestamp = Long.MIN_VALUE;

        public AsOfJoinFastRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int lookahead
        ) {
            this.columnSplit = columnSplit;
            this.record = new OuterJoinRecord(columnSplit, nullRecord);
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
            this.lookahead = lookahead;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            masterCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            masterCursor = Misc.free(masterCursor);
            slaveCursor = Misc.free(slaveCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterCursor.getSymbolTable(columnIndex);
            }
            return slaveCursor.getSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public boolean hasNext() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
            if (masterHasNext) {
                final long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
                if (masterTimestamp < slaveTimestamp) {
                    isMasterHasNextPending = true;
                    return true;
                }
                nextSlave(masterTimestamp);
                isMasterHasNextPending = true;
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterCursor.newSymbolTable(columnIndex);
            }
            return slaveCursor.newSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public long size() {
            return masterCursor.size();
        }

        @Override
        public void toTop() {
            slaveTimestamp = Long.MIN_VALUE;
            slaveFrameIndex = -1;
            slaveFrameRow = Long.MIN_VALUE;
            record.hasSlave(false);
            masterCursor.toTop();
            slaveCursor.toTop();
            isMasterHasNextPending = true;
            isSlaveOpenPending = false;
            isSlaveForwardScan = true;
        }

        private long binarySearch(long masterTimestamp, long rowLo, long rowHi) {
            while (rowLo < rowHi) {
                long rowMid = (rowLo + rowHi) >>> 1;
                slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, rowMid));
                long midTimestamp = slaveRecA.getTimestamp(slaveTimestampIndex);

                if (midTimestamp < masterTimestamp) {
                    if (rowLo < rowMid) {
                        rowLo = rowMid;
                    } else {
                        slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, rowHi));
                        if (slaveRecA.getTimestamp(slaveTimestampIndex) > masterTimestamp) {
                            return rowLo;
                        }
                        return rowHi;
                    }
                } else if (midTimestamp > masterTimestamp)
                    rowHi = rowMid;
                else {
                    // In case of multiple equal values, find the last
                    rowMid += 1;
                    while (rowMid > 0 && rowMid <= rowHi) {
                        slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, rowMid));
                        if (midTimestamp != slaveRecA.getTimestamp(slaveTimestampIndex)) {
                            break;
                        }
                        rowMid += 1;
                    }
                    return rowMid - 1;
                }
            }

            slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, rowLo));
            if (slaveRecA.getTimestamp(slaveTimestampIndex) > masterTimestamp) {
                return rowLo - 1;
            }
            return rowLo;
        }

        private boolean linearScan(TimeFrame frame, long masterTimestamp) {
            final long scanHi = Math.min(slaveFrameRow + lookahead, frame.getRowHi());
            while (slaveFrameRow < scanHi) {
                slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, slaveFrameRow));
                slaveTimestamp = slaveRecA.getTimestamp(slaveTimestampIndex);
                if (slaveTimestamp > masterTimestamp) {
                    return true;
                }
                record.hasSlave(true);
                slaveCursor.recordAt(slaveRecB, Rows.toRowID(slaveFrameIndex, slaveFrameRow));
                slaveFrameRow++;
            }
            return false;
        }

        private void nextSlave(long masterTimestamp) {
            final TimeFrame frame = slaveCursor.getTimeFrame();
            while (true) {
                if (slaveFrameIndex >= 0 && slaveFrameIndex == frame.getIndex()) {
                    // Scan a few rows to speed up self-join/identical tables cases.
                    if (linearScan(frame, masterTimestamp)) {
                        return;
                    }
                    if (slaveFrameRow < frame.getRowHi()) {
                        // Fallback to binary search.
                        slaveFrameRow = binarySearch(masterTimestamp, slaveFrameRow, frame.getRowHi() - 1);
                        record.hasSlave(true);
                        slaveCursor.recordAt(slaveRecB, Rows.toRowID(slaveFrameIndex, slaveFrameRow));
                        slaveTimestamp = slaveRecB.getTimestamp(slaveTimestampIndex);
                        return;
                    }
                }
                if (!openSlaveFrame(frame, masterTimestamp)) {
                    return;
                }
            }
        }

        private void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            masterRecord = masterCursor.getRecord();
            slaveRecA = slaveCursor.getRecord();
            slaveRecB = slaveCursor.getRecordB();
            record.of(masterRecord, slaveRecB);
            slaveTimestamp = Long.MIN_VALUE;
            slaveFrameRow = Long.MIN_VALUE;
            slaveFrameIndex = -1;
            toTop();
        }

        private boolean openSlaveFrame(TimeFrame frame, long masterTimestamp) {
            while (true) {
                if (isSlaveOpenPending) {
                    if (slaveCursor.open() < 1) {
                        // Empty frame, scan further.
                        isSlaveOpenPending = false;
                        continue;
                    }
                    // We're lucky! The frame is non-empty.
                    isSlaveOpenPending = false;
                    if (isSlaveForwardScan) {
                        if (masterTimestamp < frame.getTimestampLo()) {
                            // The frame is after the master timestamp, we need the previous frame.
                            isSlaveForwardScan = false;
                            continue;
                        }
                        // The frame is what we need, so we can search through its rows.
                        slaveFrameIndex = frame.getIndex();
                        slaveFrameRow = masterTimestamp < frame.getTimestampHi() ? frame.getRowLo() : frame.getRowHi() - 1;
                    } else {
                        // We were scanning backwards, so position to the last row.
                        slaveFrameIndex = frame.getIndex();
                        slaveFrameRow = frame.getRowHi() - 1;
                        isSlaveForwardScan = true;
                    }
                    return true;
                }

                if (isSlaveForwardScan) {
                    if (!slaveCursor.next() || masterTimestamp < frame.getTimestampEstimateLo()) {
                        // We've reached the last frame or a frame after the searched timestamp.
                        // Try to find something in previous frames.
                        isSlaveForwardScan = false;
                        continue;
                    }
                    if (masterTimestamp >= frame.getTimestampEstimateLo() && masterTimestamp < frame.getTimestampEstimateHi()) {
                        // The frame looks promising, let's open it.
                        isSlaveOpenPending = true;
                    }
                } else {
                    if (!slaveCursor.prev() || slaveFrameIndex == frame.getIndex()) {
                        // We've reached the first frame or an already opened frame. The scan is over.
                        isSlaveForwardScan = true;
                        return false;
                    }
                    // The frame looks promising, let's open it.
                    isSlaveOpenPending = true;
                }
            }
        }
    }
}

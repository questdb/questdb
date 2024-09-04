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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.std.Misc;
import io.questdb.std.Rows;

public abstract class AbstractAsOfJoinFastRecordCursor implements NoRandomAccessRecordCursor {
    protected final int columnSplit;
    protected final int lookahead;
    protected final int masterTimestampIndex;
    protected final OuterJoinRecord record;
    protected final int slaveTimestampIndex;
    protected boolean isMasterHasNextPending;
    // stands for forward/backward scan through slave's time frames
    protected boolean isSlaveForwardScan;
    protected boolean isSlaveOpenPending;
    protected long lookaheadTimestamp = Long.MIN_VALUE;
    protected RecordCursor masterCursor;
    protected boolean masterHasNext;
    protected Record masterRecord;
    protected TimeFrameRecordCursor slaveCursor;
    protected int slaveFrameIndex = -1;
    protected long slaveFrameRow = Long.MIN_VALUE;
    protected Record slaveRecA; // used for internal navigation
    protected Record slaveRecB; // used inside the user-facing OuterJoinRecord

    public AbstractAsOfJoinFastRecordCursor(
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
    public SymbolTable newSymbolTable(int columnIndex) {
        if (columnIndex < columnSplit) {
            return masterCursor.newSymbolTable(columnIndex);
        }
        return slaveCursor.newSymbolTable(columnIndex - columnSplit);
    }

    public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor) {
        this.masterCursor = masterCursor;
        this.slaveCursor = slaveCursor;
        masterRecord = masterCursor.getRecord();
        slaveRecA = slaveCursor.getRecord();
        slaveRecB = slaveCursor.getRecordB();
        record.of(masterRecord, slaveRecB);
        lookaheadTimestamp = Long.MIN_VALUE;
        slaveFrameRow = Long.MIN_VALUE;
        slaveFrameIndex = -1;
        toTop();
    }

    @Override
    public long size() {
        return masterCursor.size();
    }

    @Override
    public void toTop() {
        lookaheadTimestamp = Long.MIN_VALUE;
        slaveFrameIndex = -1;
        slaveFrameRow = Long.MIN_VALUE;
        record.hasSlave(false);
        masterCursor.toTop();
        slaveCursor.toTop();
        isMasterHasNextPending = true;
        isSlaveOpenPending = false;
        isSlaveForwardScan = true;
    }

    // Finds the last value less or equal to the master timestamp.
    // Both rowLo and rowHi are inclusive.
    protected long binarySearch(long masterTimestamp, long rowLo, long rowHi) {
        long lo = rowLo;
        long hi = rowHi;
        while (lo < hi) {
            long mid = (lo + hi) >>> 1;
            slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, mid));
            long midTimestamp = slaveRecA.getTimestamp(slaveTimestampIndex);

            if (midTimestamp <= masterTimestamp) {
                if (lo < mid) {
                    lo = mid;
                } else {
                    slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, hi));
                    if (slaveRecA.getTimestamp(slaveTimestampIndex) > masterTimestamp) {
                        return lo;
                    }
                    return hi;
                }
            } else {
                hi = mid;
            }
        }

        slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, lo));
        if (slaveRecA.getTimestamp(slaveTimestampIndex) > masterTimestamp) {
            return lo - 1;
        }
        return lo;
    }

    /**
     * Returns true if the slave cursor has been advanced to a row with timestamp greater than the master timestamp.
     * This means we do not have to scan the slave cursor further, e.g. by binary search.
     *
     * @param frame           slave cursor time frame
     * @param masterTimestamp master timestamp
     * @return true if the slave cursor has been advanced to a row with timestamp greater than the master timestamp, false otherwise
     */
    protected boolean linearScan(TimeFrame frame, long masterTimestamp) {
        final long scanHi = Math.min(slaveFrameRow + lookahead, frame.getRowHi());
        while (slaveFrameRow < scanHi || (lookaheadTimestamp == masterTimestamp && slaveFrameRow < frame.getRowHi())) {
            slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, slaveFrameRow));
            lookaheadTimestamp = slaveRecA.getTimestamp(slaveTimestampIndex);
            if (lookaheadTimestamp > masterTimestamp) {
                return true;
            }
            record.hasSlave(true);
            slaveCursor.recordAt(slaveRecB, Rows.toRowID(slaveFrameIndex, slaveFrameRow));
            slaveFrameRow++;
        }
        return false;
    }

    protected void nextSlave(long masterTimestamp) {
        final TimeFrame frame = slaveCursor.getTimeFrame();
        while (true) {
            if (frame.isOpen() && frame.getFrameIndex() == slaveFrameIndex) {
                // Scan a few rows to speed up self-join/identical tables cases.
                if (linearScan(frame, masterTimestamp)) {
                    return;
                }
                if (slaveFrameRow < frame.getRowHi()) {
                    // Fallback to binary search.
                    // Find the last value less or equal to the master timestamp.
                    long foundRow = binarySearch(masterTimestamp, slaveFrameRow, frame.getRowHi() - 1);
                    if (foundRow < slaveFrameRow) {
                        // All searched timestamps are greater than the master timestamp.
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
                    if (foundRow < frame.getRowHi() - 1 || slaveTimestamp == masterTimestamp) {
                        // We've found the row, so there is no point in checking the next partition.
                        return;
                    }
                }
            }
            if (!openSlaveFrame(frame, masterTimestamp)) {
                return;
            }
        }
    }

    protected boolean openSlaveFrame(TimeFrame frame, long masterTimestamp) {
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
                    slaveFrameIndex = frame.getFrameIndex();
                    slaveFrameRow = masterTimestamp < frame.getTimestampHi() - 1 ? frame.getRowLo() : frame.getRowHi() - 1;
                } else {
                    // We were scanning backwards, so position to the last row.
                    slaveFrameIndex = frame.getFrameIndex();
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
                if (!slaveCursor.prev() || slaveFrameIndex == frame.getFrameIndex()) {
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

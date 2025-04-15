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

import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
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
    protected TimeFrame slaveTimeFrame;

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
        this.slaveTimeFrame = slaveCursor.getTimeFrame();
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

    public void skipRows(Counter rowCount) throws DataUnavailableException {
        // isMasterHasNextPending is false is only possible when slave cursor navigation inside hasNext() threw DataUnavailableException
        // and in such case we expect hasNext() to be called again, rather than skipRows()
        assert isMasterHasNextPending;
        masterCursor.skipRows(rowCount);
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

    private boolean openSlaveFrame(long masterTimestamp) {
        while (true) {

            // Case 1: Process a frame that was previously marked for opening
            if (isSlaveOpenPending) {
                if (slaveCursor.open() < 1) {
                    // Empty frame, scan further -> case 2
                    isSlaveOpenPending = false;
                    continue;
                }
                // We're lucky! The frame is non-empty.
                isSlaveOpenPending = false;

                if (isSlaveForwardScan) {
                    if (masterTimestamp < slaveTimeFrame.getTimestampLo()) {
                        // The frame is after the master timestamp, we need the previous frame.
                        isSlaveForwardScan = false;
                        continue;
                    }
                    // The frame is what we need, so we can search through its rows.
                    slaveFrameIndex = slaveTimeFrame.getFrameIndex();
                    slaveFrameRow = masterTimestamp < slaveTimeFrame.getTimestampHi() - 1 ? slaveTimeFrame.getRowLo() : slaveTimeFrame.getRowHi() - 1;
                } else {
                    // We were scanning backwards, so position to the last row.
                    slaveFrameIndex = slaveTimeFrame.getFrameIndex();
                    slaveFrameRow = slaveTimeFrame.getRowHi() - 1;
                    isSlaveForwardScan = true;
                }
                return true;
            }

            // Case 2: Scan for a frame to open based on scan direction.
            // This uses only estimated timestamp boundaries since we don't know
            // the precise boundaries until we open the frame.
            if (isSlaveForwardScan) {
                if (!slaveCursor.next() || masterTimestamp < slaveTimeFrame.getTimestampEstimateLo()) {
                    // We've reached the last frame or a frame after the searched timestamp.
                    // Try to find something in previous frames.
                    isSlaveForwardScan = false;
                    continue;
                }
                if (masterTimestamp >= slaveTimeFrame.getTimestampEstimateLo() && masterTimestamp < slaveTimeFrame.getTimestampEstimateHi()) {
                    // The frame looks promising, let's open it.
                    isSlaveOpenPending = true;
                }
            } else {
                if (!slaveCursor.prev() || slaveFrameIndex == slaveTimeFrame.getFrameIndex()) {
                    // We've reached the first frame or an already opened frame. The scan is over.
                    isSlaveForwardScan = true;
                    return false;
                }
                // The frame looks promising, let's open it.
                isSlaveOpenPending = true;
            }
        }
    }

    private long binarySearchScanDown(long v, long low, long high) {
        for (long i = high - 1; i >= low; i--) {
            slaveCursor.recordAtRowIndex(slaveRecA, i);
            long that = slaveRecA.getTimestamp(slaveTimestampIndex);
            // Here the code differs from the original C code:
            // We want to find the last row with value less *or equal* to v
            // while the original code find the first row with value greater than v.
            if (that <= v) {
                return i;
            }
        }
        // all values are greater than v, return low - 1
        return low - 1;
    }

    private long binarySearchScrollDown(long low, long high, long value) {
        long data;
        do {
            if (low < high) {
                low++;
            } else {
                return low;
            }
            slaveCursor.recordAtRowIndex(slaveRecA, low);
            data = slaveRecA.getTimestamp(slaveTimestampIndex);
        } while (data == value);
        return low - 1;
    }

    // Finds the last value less or equal to the master timestamp.
    // Both rowLo and rowHi are inclusive.
    // When multiple rows have the same matching timestamp, the last one is returned.
    // When all rows have timestamps greater than the master timestamp, rowLo - 1 is returned.
    protected long binarySearch(long value, long rowLo, long rowHi) {
        // this is the same algorithm as implemented in C (util.h)
        // template<class T, class V>
        // inline int64_t binary_search(T *data, V value, int64_t low, int64_t high, int32_t scan_dir)
        // please ensure these implementations are in sync
        //
        // there is a notable difference in the algo: the original C code returns an insertion point
        // when there is no match, while we return the last row with value less than or equal to v

        // we expect to use binary search only after linearSearch()
        // this means we expect the slaveRecA to be already set to the right frame.
        // this invariant allows us to avoid calling slaveCursor.recordAt()
        // and we can call cheaper slaveCursor.recordAtRowIndex()
        assert Rows.toPartitionIndex(slaveRecA.getRowId()) == slaveFrameIndex;

        long low = rowLo;
        long high = rowHi;
        while (high - low > 65) {
            final long mid = (low + high) >>> 1;
            slaveCursor.recordAtRowIndex(slaveRecA, mid);
            long midVal = slaveRecA.getTimestamp(slaveTimestampIndex);

            if (midVal < value) {
                low = mid;
            } else if (midVal > value) {
                high = mid - 1;
            } else {
                // In case of multiple equal values, find the last
                return binarySearchScrollDown(mid, high, midVal);
            }
        }

        return binarySearchScanDown(value, low, high + 1);
    }

    /**
     * Returns true if the slave cursor has been advanced to a row with timestamp greater than the master timestamp.
     * This means we do not have to scan the slave cursor further, e.g. by binary search.
     *
     * @param masterTimestamp master timestamp
     * @return true if the slave cursor has been advanced to a row with timestamp greater than the master timestamp, false otherwise
     */
    private boolean linearScan(long masterTimestamp) {
        final long scanHi = Math.min(slaveFrameRow + lookahead, slaveTimeFrame.getRowHi());
        while (slaveFrameRow < scanHi || (lookaheadTimestamp == masterTimestamp && slaveFrameRow < slaveTimeFrame.getRowHi())) {
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
        while (true) {
            if (slaveTimeFrame.isOpen() && slaveTimeFrame.getFrameIndex() == slaveFrameIndex) {
                // Scan a few rows to speed up self-join/identical tables cases.
                if (linearScan(masterTimestamp)) {
                    return;
                }
                if (slaveFrameRow < slaveTimeFrame.getRowHi()) {
                    // Fallback to binary search.
                    // Find the last value less or equal to the master timestamp.
                    long foundRow = binarySearch(masterTimestamp, slaveFrameRow, slaveTimeFrame.getRowHi() - 1);
                    if (foundRow < slaveFrameRow) {
                        // All searched timestamps are greater than the master timestamp.
                        // Linear scan must have found the row.
                        return;
                    }
                    slaveFrameRow = foundRow;
                    record.hasSlave(true);
                    slaveCursor.recordAt(slaveRecB, Rows.toRowID(slaveFrameIndex, slaveFrameRow));
                    long slaveTimestamp = slaveRecB.getTimestamp(slaveTimestampIndex);
                    if (slaveFrameRow < slaveTimeFrame.getRowHi() - 1) {
                        slaveCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, slaveFrameRow + 1));
                        lookaheadTimestamp = slaveRecA.getTimestamp(slaveTimestampIndex);
                    } else {
                        lookaheadTimestamp = slaveTimestamp;
                    }
                    if (foundRow < slaveTimeFrame.getRowHi() - 1 || slaveTimestamp == masterTimestamp) {
                        // We've found the row, so there is no point in checking the next partition.
                        return;
                    }
                }
            }
            if (!openSlaveFrame(masterTimestamp)) {
                return;
            }
        }
    }
}

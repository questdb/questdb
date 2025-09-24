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
    protected final long masterTimestampScale;
    protected final OuterJoinRecord record;
    protected final int slaveTimestampIndex;
    protected final long slaveTimestampScale;
    protected boolean isMasterHasNextPending;
    // stands for forward/backward scan through slave's time frames
    protected boolean isSlaveForwardScan;
    protected boolean isSlaveOpenPending;
    protected long lookaheadTimestamp = Long.MIN_VALUE;
    protected RecordCursor masterCursor;
    protected boolean masterHasNext;
    protected Record masterRecord;
    protected int slaveFrameIndex = -1;
    protected long slaveFrameRow = Long.MIN_VALUE;
    protected Record slaveRecA; // used for internal navigation
    protected Record slaveRecB; // used inside the user-facing OuterJoinRecord
    protected TimeFrame slaveTimeFrame;
    protected TimeFrameRecordCursor slaveTimeFrameCursor;

    public AbstractAsOfJoinFastRecordCursor(
            int columnSplit,
            Record nullRecord,
            int masterTimestampIndex,
            int slaveTimestampIndex,
            int masterTimestampType,
            int slaveTimestampType,
            int lookahead
    ) {
        this.columnSplit = columnSplit;
        this.record = new OuterJoinRecord(columnSplit, nullRecord);
        this.masterTimestampIndex = masterTimestampIndex;
        this.slaveTimestampIndex = slaveTimestampIndex;
        this.lookahead = lookahead;
        if (masterTimestampType == slaveTimestampType) {
            masterTimestampScale = slaveTimestampScale = 1L;
        } else {
            masterTimestampScale = ColumnType.getTimestampDriver(masterTimestampType).toNanosScale();
            slaveTimestampScale = ColumnType.getTimestampDriver(slaveTimestampType).toNanosScale();
        }
    }

    public static long scaleTimestamp(long timestamp, long scale) {
        if (scale == 1 || timestamp == Long.MIN_VALUE) {
            return timestamp;
        }
        try {
            return Math.multiplyExact(timestamp, scale);
        } catch (ArithmeticException e) {
            // May "overflow" when micros scales to nanos, which will return max timestamp.
            return Long.MAX_VALUE;
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        masterCursor.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        masterCursor = Misc.free(masterCursor);
        slaveTimeFrameCursor = Misc.free(slaveTimeFrameCursor);
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
        return slaveTimeFrameCursor.getSymbolTable(columnIndex - columnSplit);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        if (columnIndex < columnSplit) {
            return masterCursor.newSymbolTable(columnIndex);
        }
        return slaveTimeFrameCursor.newSymbolTable(columnIndex - columnSplit);
    }

    public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor) {
        this.masterCursor = masterCursor;
        this.slaveTimeFrameCursor = slaveCursor;
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
        slaveTimeFrameCursor.toTop();
        isMasterHasNextPending = true;
        isSlaveOpenPending = false;
        isSlaveForwardScan = true;
    }

    private long binarySearchScanDown(long v, long low, long high, long totalRowLo) {
        for (long i = high - 1; i >= low; i--) {
            slaveTimeFrameCursor.recordAtRowIndex(slaveRecA, i);
            long that = scaleTimestamp(slaveRecA.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
            // Here the code differs from the original C code:
            // We want to find the last row with value less *or equal* to v
            // while the original code find the first row with value greater than v.
            if (that <= v) {
                return i;
            }
        }
        // all values are greater than v, return totalRowLo - 1
        return totalRowLo - 1;
    }

    private long binarySearchScrollDown(long low, long high, long value) {
        long data;
        do {
            if (low < high) {
                low++;
            } else {
                return low;
            }
            slaveTimeFrameCursor.recordAtRowIndex(slaveRecA, low);
            data = scaleTimestamp(slaveRecA.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
        } while (data == value);
        return low - 1;
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
            slaveTimeFrameCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, slaveFrameRow));
            lookaheadTimestamp = scaleTimestamp(slaveRecA.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
            if (lookaheadTimestamp > masterTimestamp) {
                return true;
            }
            record.hasSlave(true);
            slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(slaveFrameIndex, slaveFrameRow));
            slaveFrameRow++;
        }
        return false;
    }

    private boolean openSlaveFrame(long masterTimestamp) {
        while (true) {

            // Case 1: Process a frame that was previously marked for opening
            if (isSlaveOpenPending) {
                if (slaveTimeFrameCursor.open() < 1) {
                    // Empty frame, scan further -> case 2
                    isSlaveOpenPending = false;
                    continue;
                }
                // We're lucky! The frame is non-empty.
                isSlaveOpenPending = false;

                if (isSlaveForwardScan) {
                    if (masterTimestamp < scaleTimestamp(slaveTimeFrame.getTimestampLo(), slaveTimestampScale)) {
                        // The frame is after the master timestamp, we need the previous frame.
                        isSlaveForwardScan = false;
                        continue;
                    }

                    // The frame is what we need, so we can search through its rows.
                    slaveFrameIndex = slaveTimeFrame.getFrameIndex();
                    slaveFrameRow = masterTimestamp < scaleTimestamp(slaveTimeFrame.getTimestampHi(), slaveTimestampScale) - 1 ? slaveTimeFrame.getRowLo() : slaveTimeFrame.getRowHi() - 1;
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
                if (!slaveTimeFrameCursor.next() || masterTimestamp < scaleTimestamp(slaveTimeFrame.getTimestampEstimateLo(), slaveTimestampScale)) {
                    // We've reached the last frame or a frame after the searched timestamp.
                    // Try to find something in previous frames.
                    isSlaveForwardScan = false;
                    continue;
                }
                if (masterTimestamp < scaleTimestamp(slaveTimeFrame.getTimestampEstimateHi(), slaveTimestampScale)) {
                    // The frame looks promising, let's open it.
                    isSlaveOpenPending = true;
                }
            } else {
                if (!slaveTimeFrameCursor.prev() || slaveFrameIndex == slaveTimeFrame.getFrameIndex()) {
                    // We've reached the first frame or an already opened frame. The scan is over.
                    isSlaveForwardScan = true;
                    return false;
                }
                // The frame looks promising, let's open it.
                isSlaveOpenPending = true;
            }
        }
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
            slaveTimeFrameCursor.recordAtRowIndex(slaveRecA, mid);
            long midVal = scaleTimestamp(slaveRecA.getTimestamp(slaveTimestampIndex), slaveTimestampScale);

            if (midVal < value) {
                low = mid;
            } else if (midVal > value) {
                high = mid - 1;
            } else {
                // In case of multiple equal values, find the last
                return binarySearchScrollDown(mid, high, midVal);
            }
        }

        return binarySearchScanDown(value, low, high + 1, rowLo);
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
                    slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(slaveFrameIndex, slaveFrameRow));
                    long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                    if (slaveFrameRow < slaveTimeFrame.getRowHi() - 1) {
                        slaveTimeFrameCursor.recordAt(slaveRecA, Rows.toRowID(slaveFrameIndex, slaveFrameRow + 1));
                        lookaheadTimestamp = scaleTimestamp(slaveRecA.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
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

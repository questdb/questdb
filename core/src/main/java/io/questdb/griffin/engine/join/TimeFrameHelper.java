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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

public class TimeFrameHelper implements QuietCloseable {
    private final long lookahead;
    private final long scale;
    private int bookmarkedFrameIndex = -1;
    private long bookmarkedRowId = Long.MIN_VALUE;
    private Record record;
    private TimeFrame timeFrame;
    private TimeFrameCursor timeFrameCursor;
    private int timestampIndex;

    public TimeFrameHelper(long lookahead, long scale) {
        this.lookahead = lookahead;
        this.scale = scale;
    }

    @Override
    public void close() {
        Misc.free(timeFrameCursor);
    }

    // finds the first row id within the given interval and load the record to it
    public long findRowLo(long timestampLo, long timestampHi) {
        long rowLo = Long.MIN_VALUE;
        // let's start with the last found frame and row id
        if (bookmarkedFrameIndex != -1) {
            timeFrameCursor.jumpTo(bookmarkedFrameIndex);
            timeFrameCursor.open();
            rowLo = bookmarkedRowId;
        }

        for (; ; ) {
            // find the frame to be scanned
            if (rowLo == Long.MIN_VALUE) {
                while (timeFrameCursor.next()) {
                    // carry on if the frame is to the left of the interval
                    if (scaleTimestamp(timeFrame.getTimestampEstimateHi(), scale) < timestampLo) {
                        // bookmark the frame, so that next time we search we start with it
                        bookmarkCurrentFrame(0);
                        continue;
                    }
                    // check if the frame intersects with the interval, so it's of our interest
                    if (scaleTimestamp(timeFrame.getTimestampEstimateLo(), scale) <= timestampHi) {
                        if (timeFrameCursor.open() == 0) {
                            continue;
                        }
                        // now we know the exact boundaries of the frame, let's check them
                        if (scaleTimestamp(timeFrame.getTimestampHi(), scale) <= timestampLo) {
                            bookmarkCurrentFrame(0);
                            continue;
                        }
                        if (scaleTimestamp(timeFrame.getTimestampLo(), scale) <= timestampHi) {
                            // yay, it's what we need!

                            if (scaleTimestamp(timeFrame.getTimestampLo(), scale) >= timestampLo) {
                                // we can start with the first row
                                bookmarkCurrentFrame(timeFrame.getRowLo());
                                timeFrameCursor.recordAt(record, Rows.toRowID(timeFrame.getFrameIndex(), timeFrame.getRowLo()));
                                return timeFrame.getRowLo();
                            }
                            // we need to find the first row in the intersection
                            rowLo = timeFrame.getRowLo();
                            break;
                        }
                    }

                    // next row may have interval with current frame
                    bookmarkCurrentFrame(0);
                    return Long.MIN_VALUE;
                }
                if (rowLo == Long.MIN_VALUE) {
                    return Long.MIN_VALUE;
                }
            }

            // bookmark the frame
            bookmarkCurrentFrame(rowLo);

            // scan the found frame
            // start with a brief linear scan
            timeFrameCursor.recordAt(record, Rows.toRowID(timeFrame.getFrameIndex(), timeFrame.getRowLo()));
            final long scanResult = linearScan(timestampLo, timestampHi, rowLo);
            if (scanResult >= 0) {
                // we've found the row
                bookmarkCurrentFrame(scanResult);
                return scanResult;
            } else if (scanResult == Long.MIN_VALUE) {
                // there are no timestamps in the wanted interval
                if (scaleTimestamp(timeFrame.getTimestampHi(), scale) > timestampHi) {
                    // the interval is contained in the frame, no need to try the next one
                    return Long.MIN_VALUE;
                }
                // the next frame may have an intersection with the interval, try it
                rowLo = Long.MIN_VALUE;
                continue;
            }
            // ok, the scan gave us nothing, do the binary search
            rowLo = -scanResult - 1;
            final long searchResult = binarySearch(timestampLo, timestampHi, rowLo);
            if (searchResult == Long.MIN_VALUE) {
                // there are no timestamps in the wanted interval
                if (scaleTimestamp(timeFrame.getTimestampHi(), scale) > timestampHi) {
                    // the interval is contained in the frame, no need to try the next one
                    return Long.MIN_VALUE;
                }
                // the next frame may have an intersection with the interval, try it
                rowLo = Long.MIN_VALUE;
                continue;
            }
            // we've found the row
            bookmarkCurrentFrame(searchResult);
            return searchResult;
        }
    }

    public Record getRecord() {
        return record;
    }

    public SymbolTable getSymbolTable(int columnIndex) {
        return timeFrameCursor.getSymbolTable(columnIndex);
    }

    public SymbolTableSource getSymbolTableSource() {
        return timeFrameCursor;
    }

    public int getTimeFrameIndex() {
        return timeFrame.getFrameIndex();
    }

    public long getTimeFrameRowHi() {
        return timeFrame.getRowHi();
    }

    public long getTimeFrameRowLo() {
        return timeFrame.getRowLo();
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public SymbolTable newSymbolTable(int columnIndex) {
        return timeFrameCursor.newSymbolTable(columnIndex);
    }

    public boolean nextFrame(long timestampHi) {
        if (!timeFrameCursor.next()) {
            return false;
        }
        if (timestampHi >= scaleTimestamp(timeFrame.getTimestampEstimateLo(), scale)) {
            return timeFrameCursor.open() > 0;
        }
        return false;
    }

    public void of(TimeFrameCursor timeFrameCursor) {
        this.timeFrameCursor = timeFrameCursor;
        this.record = timeFrameCursor.getRecord();
        this.timeFrame = timeFrameCursor.getTimeFrame();
        this.timestampIndex = timeFrameCursor.getTimestampIndex();
        toTop();
    }

    public void recordAt(long rowId) {
        timeFrameCursor.recordAt(record, rowId);
    }

    public void recordAtRowIndex(long rowIndex) {
        timeFrameCursor.recordAtRowIndex(record, rowIndex);
    }

    public void toTop() {
        timeFrameCursor.toTop();
        bookmarkedFrameIndex = -1;
        bookmarkedRowId = Long.MIN_VALUE;
    }

    // Finds the first (most-left) value in the given interval.
    private long binarySearch(long timestampLo, long timestampHi, long rowLo) {
        long low = rowLo;
        long high = timeFrame.getRowHi() - 1;
        while (high - low > 65) {
            final long mid = (low + high) >>> 1;
            recordAtRowIndex(mid);
            long midTimestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);

            if (midTimestamp < timestampLo) {
                low = mid;
            } else if (midTimestamp > timestampLo) {
                high = mid - 1;
            } else {
                // In case of multiple values equal to timestampLo, find the first one
                return binarySearchScrollUp(low, mid, timestampLo);
            }
        }

        // scan up
        for (long r = low; r < high + 1; r++) {
            recordAtRowIndex(r);
            long timestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);
            if (timestamp >= timestampLo) {
                if (timestamp <= timestampHi) {
                    return r;
                }
                return Long.MIN_VALUE;
            }
        }
        return Long.MIN_VALUE;
    }

    private long binarySearchScrollUp(long low, long high, long timestampLo) {
        long timestamp;
        do {
            if (high > low) {
                high--;
            } else {
                return low;
            }
            recordAtRowIndex(high);
            timestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);
        } while (timestamp == timestampLo);
        return high + 1;
    }

    private void bookmarkCurrentFrame(long rowId) {
        bookmarkedFrameIndex = timeFrame.getFrameIndex();
        bookmarkedRowId = rowId;
    }

    private long linearScan(long timestampLo, long timestampHi, long rowLo) {
        final long scanHi = Math.min(rowLo + lookahead, timeFrame.getRowHi());
        for (long r = rowLo; ; ) {
            final long timestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);
            if (timestamp >= timestampLo) {
                if (timestamp <= timestampHi) {
                    return r;
                }
                return Long.MIN_VALUE;
            }

            if (++r >= scanHi) {
                break;
            }
            recordAtRowIndex(r);
        }
        return -rowLo - lookahead - 1;
    }
}

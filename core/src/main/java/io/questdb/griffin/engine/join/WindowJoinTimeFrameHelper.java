/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Helper for navigating and searching through time frames in window joins.
 * <p>
 * Wraps a {@link TimeFrameCursor} and provides efficient row lookups within timestamp
 * intervals using a combination of linear scan and binary search. Maintains bookmarks
 * to track position for efficient subsequent searches.
 * <p>
 * Supports prevailing semantics: can find and track the most recent row before
 * a given timestamp threshold.
 */
public class WindowJoinTimeFrameHelper {
    private final long lookahead;
    private final long scale;
    private int bookmarkedFrameIndex = -1;
    private long bookmarkedRowIndex = Long.MIN_VALUE;
    private int prevailingFrameIndex = -1;
    private long prevailingRowIndex = Long.MIN_VALUE;
    private Record record;
    private TimeFrame timeFrame;
    private TimeFrameCursor timeFrameCursor;
    private int timestampIndex;

    public WindowJoinTimeFrameHelper(long lookahead, long scale) {
        this.lookahead = lookahead;
        this.scale = scale;
    }

    // Finds the first (most-left) value in the given interval.
    public long binarySearch(long timestampLo, long timestampHi, long rowLo, boolean recordPrevailing) {
        long low = rowLo;
        long high = timeFrame.getRowHi() - 1;
        while (high - low > 65) {
            final long mid = (low + high) >>> 1;
            recordAtRowIndex(mid);
            long midTimestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);

            if (midTimestamp < timestampLo) {
                low = mid;
            } else if (midTimestamp > timestampLo) {
                high = mid;
            } else {
                // In case of multiple values equal to timestampLo, find the first one
                long index = binarySearchScrollUp(low, mid, timestampLo);
                if (recordPrevailing && index > 0) {
                    prevailingFrameIndex = timeFrame.getFrameIndex();
                    prevailingRowIndex = index - 1;
                }
                return index;
            }
        }

        // scan up
        for (long r = low; r < high + 1; r++) {
            recordAtRowIndex(r);
            long timestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);
            if (timestamp >= timestampLo) {
                if (recordPrevailing && r > 0) {
                    prevailingFrameIndex = timeFrame.getFrameIndex();
                    prevailingRowIndex = r - 1;
                }
                if (timestamp <= timestampHi) {
                    return r;
                }

                return Long.MIN_VALUE;
            }
        }
        if (recordPrevailing && timeFrame.getRowHi() > 0) {
            prevailingFrameIndex = timeFrame.getFrameIndex();
            prevailingRowIndex = timeFrame.getRowHi() - 1;
        }
        return Long.MIN_VALUE;
    }

    // Note: don't forget to call recordAtRowIndex() prior to using the record.
    public long findRowLo(long timestampLo, long timestampHi) {
        return findRowLo(timestampLo, timestampHi, false);
    }

    // Finds the first row id within the given interval and load the record to it.
    // Also records the prevailing candidate (last row with timestamp < timestampLo).
    // Note: don't forget to call recordAtRowIndex() prior to using the record.
    public long findRowLo(long timestampLo, long timestampHi, boolean recordPrevailing) {
        long rowLo = Long.MIN_VALUE;
        // Reset prevailing candidate
        prevailingFrameIndex = -1;
        prevailingRowIndex = Long.MIN_VALUE;

        // let's start with the last found frame and row id
        if (bookmarkedFrameIndex != -1) {
            timeFrameCursor.jumpTo(bookmarkedFrameIndex);
            timeFrameCursor.open();
            rowLo = bookmarkedRowIndex;
        }

        for (; ; ) {
            // find the frame to be scanned
            if (rowLo == Long.MIN_VALUE) {
                while (timeFrameCursor.next()) {
                    // carry on if the frame is to the left of the interval
                    if (scaleTimestamp(timeFrame.getTimestampEstimateHi(), scale) < timestampLo) {
                        if (recordPrevailing && timeFrameCursor.open() > 0) {
                            prevailingFrameIndex = timeFrame.getFrameIndex();
                            prevailingRowIndex = timeFrame.getRowHi() - 1;
                        }
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
                            if (recordPrevailing) {
                                prevailingFrameIndex = timeFrame.getFrameIndex();
                                prevailingRowIndex = timeFrame.getRowHi() - 1;
                            }
                            bookmarkCurrentFrame(0);
                            continue;
                        }
                        if (scaleTimestamp(timeFrame.getTimestampLo(), scale) <= timestampHi) {
                            // yay, it's what we need!
                            if (scaleTimestamp(timeFrame.getTimestampLo(), scale) >= timestampLo) {
                                // we can start with the first row
                                bookmarkCurrentFrame(timeFrame.getRowLo());
                                timeFrameCursor.recordAt(record, timeFrame.getFrameIndex(), timeFrame.getRowLo());
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
            timeFrameCursor.recordAt(record, timeFrame.getFrameIndex(), timeFrame.getRowLo());
            final long scanResult = linearScan(timestampLo, timestampHi, rowLo, recordPrevailing);
            if (scanResult >= 0) {
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
            final long searchResult = binarySearch(timestampLo, timestampHi, rowLo, recordPrevailing);
            if (searchResult == Long.MIN_VALUE) {
                if (scaleTimestamp(timeFrame.getTimestampHi(), scale) > timestampHi) {
                    // the interval is contained in the frame, no need to try the next one
                    return Long.MIN_VALUE;
                }
                // the next frame may have an intersection with the interval, try it
                rowLo = Long.MIN_VALUE;
                continue;
            }
            bookmarkCurrentFrame(searchResult);
            return searchResult;
        }
    }

    // Note: don't forget to call recordAtRowIndex() prior to using the record.
    public long findRowLoWithPrevailing(long timestampLo, long timestampHi) {
        long rowLo = Long.MIN_VALUE;
        // record prevailing candidate across frames
        long prevailingRowIndex = Long.MIN_VALUE;
        int prevailingFrameIndex = -1;

        if (bookmarkedFrameIndex != -1) {
            timeFrameCursor.jumpTo(bookmarkedFrameIndex);
            if (timeFrameCursor.open() > 0) {
                long frameTsHi = scaleTimestamp(timeFrame.getTimestampHi(), scale);
                if (frameTsHi < timestampLo) {
                    // Record as prevailing candidate
                    prevailingRowIndex = timeFrame.getRowHi() - 1;
                    prevailingFrameIndex = timeFrame.getFrameIndex();
                    bookmarkCurrentFrame(0);
                } else {
                    rowLo = bookmarkedRowIndex;
                }
            }
        }

        for (; ; ) {
            if (rowLo == Long.MIN_VALUE) {
                while (timeFrameCursor.next()) {
                    long frameEstimateHi = scaleTimestamp(timeFrame.getTimestampEstimateHi(), scale);
                    long frameEstimateLo = scaleTimestamp(timeFrame.getTimestampEstimateLo(), scale);

                    if (frameEstimateHi < timestampLo) {
                        if (timeFrameCursor.open() > 0) {
                            // Record as prevailing candidate
                            prevailingRowIndex = timeFrame.getRowHi() - 1;
                            prevailingFrameIndex = timeFrame.getFrameIndex();
                        }
                        bookmarkCurrentFrame(0);
                        continue;
                    }

                    if (frameEstimateLo <= timestampHi) {
                        if (timeFrameCursor.open() == 0) {
                            continue;
                        }

                        long frameTsHi = scaleTimestamp(timeFrame.getTimestampHi(), scale);
                        long frameTsLo = scaleTimestamp(timeFrame.getTimestampLo(), scale);

                        if (frameTsHi < timestampLo) {
                            prevailingRowIndex = timeFrame.getRowHi() - 1;
                            prevailingFrameIndex = timeFrame.getFrameIndex();
                            bookmarkCurrentFrame(0);
                            continue;
                        }

                        if (frameTsLo <= timestampHi) {
                            if (frameTsLo == timestampLo) {
                                // Exact match at frame start
                                bookmarkCurrentFrame(timeFrame.getRowLo());
                                timeFrameCursor.recordAt(record, timeFrame.getFrameIndex(), timeFrame.getRowLo());
                                return timeFrame.getRowLo();
                            }
                            if (frameTsLo > timestampLo) {
                                // Frame starts after timestampLo, return prevailing if exists
                                if (prevailingRowIndex != Long.MIN_VALUE) {
                                    bookmarkedFrameIndex = prevailingFrameIndex;
                                    bookmarkedRowIndex = prevailingRowIndex;
                                    // Navigate to the prevailing frame so that timeFrame has correct bounds.
                                    // The caller will use getTimeFrameRowHi() for boundary checks during iteration.
                                    timeFrameCursor.jumpTo(prevailingFrameIndex);
                                    timeFrameCursor.open();
                                    timeFrameCursor.recordAt(record, prevailingFrameIndex, prevailingRowIndex);
                                    return prevailingRowIndex;
                                }
                                bookmarkCurrentFrame(timeFrame.getRowLo());
                                timeFrameCursor.recordAt(record, timeFrame.getFrameIndex(), timeFrame.getRowLo());
                                return timeFrame.getRowLo();
                            }
                            rowLo = timeFrame.getRowLo();
                            break;
                        }
                    }

                    if (prevailingRowIndex != Long.MIN_VALUE) {
                        bookmarkedFrameIndex = prevailingFrameIndex;
                        bookmarkedRowIndex = prevailingRowIndex;
                        // Navigate to the prevailing frame so that timeFrame has correct bounds.
                        timeFrameCursor.jumpTo(prevailingFrameIndex);
                        timeFrameCursor.open();
                        timeFrameCursor.recordAt(record, prevailingFrameIndex, prevailingRowIndex);
                        return prevailingRowIndex;
                    }
                    bookmarkCurrentFrame(0);
                    return Long.MIN_VALUE;
                }

                if (rowLo == Long.MIN_VALUE) {
                    if (prevailingRowIndex != Long.MIN_VALUE) {
                        bookmarkedFrameIndex = prevailingFrameIndex;
                        bookmarkedRowIndex = prevailingRowIndex;
                        // Navigate to the prevailing frame so that timeFrame has correct bounds.
                        timeFrameCursor.jumpTo(prevailingFrameIndex);
                        timeFrameCursor.open();
                        timeFrameCursor.recordAt(record, prevailingFrameIndex, prevailingRowIndex);
                        return prevailingRowIndex;
                    }
                    return Long.MIN_VALUE;
                }
            }

            bookmarkCurrentFrame(rowLo);
            timeFrameCursor.recordAt(record, timeFrame.getFrameIndex(), timeFrame.getRowLo());

            // Try linear scan first
            final long scanResult = linearScanWithPrevailing(timestampLo, timestampHi, rowLo);
            if (scanResult >= 0) {
                bookmarkCurrentFrame(scanResult);
                return scanResult;
            } else if (scanResult == Long.MIN_VALUE) {
                // Try next frame
                if (scaleTimestamp(timeFrame.getTimestampHi(), scale) > timestampHi) {
                    return Long.MIN_VALUE;
                }
                rowLo = Long.MIN_VALUE;
                continue;
            }

            long lastScanned = -scanResult - 2;
            final long searchResult = binarySearchWithPrevailing(timestampLo, timestampHi, lastScanned + 1);
            if (searchResult == Long.MIN_VALUE) {
                if (scaleTimestamp(timeFrame.getTimestampHi(), scale) > timestampHi) {
                    return Long.MIN_VALUE;
                }
                rowLo = Long.MIN_VALUE;
                continue;
            }
            bookmarkCurrentFrame(searchResult);
            return searchResult;
        }
    }

    public int getBookmarkedFrameIndex() {
        return bookmarkedFrameIndex;
    }

    public long getBookmarkedRowIndex() {
        return bookmarkedRowIndex;
    }

    public int getPrevailingFrameIndex() {
        return prevailingFrameIndex;
    }

    public long getPrevailingRowIndex() {
        return prevailingRowIndex;
    }

    public Record getRecord() {
        return record;
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

    // note: recordAt() must be called after making this call!!!
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

    // note: recordAt() must be called after making this call!!!
    public boolean previousFrame() {
        if (!timeFrameCursor.prev()) {
            return false;
        }
        return timeFrameCursor.open() > 0;
    }

    public void recordAt(long rowId) {
        timeFrameCursor.recordAt(record, rowId);
    }

    public void recordAtRowIndex(long rowIndex) {
        timeFrameCursor.recordAtRowIndex(record, rowIndex);
    }

    public void restoreBookmark(int frameIndex, long rowIndex) {
        this.bookmarkedFrameIndex = frameIndex;
        this.bookmarkedRowIndex = rowIndex;
        timeFrameCursor.jumpTo(bookmarkedFrameIndex);
        timeFrameCursor.open();
        timeFrameCursor.recordAt(record, frameIndex, rowIndex);
    }

    public void toTop() {
        if (timeFrameCursor != null) {
            timeFrameCursor.toTop();
        }
        bookmarkedFrameIndex = -1;
        bookmarkedRowIndex = Long.MIN_VALUE;
    }

    private long binarySearchScrollUp(long low, long high, long timestampLo) {
        // Binary search until range is small enough for linear scan
        while (high - low > 16) {
            final long mid = (low + high) >>> 1;
            recordAtRowIndex(mid);
            final long midTimestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);

            if (midTimestamp < timestampLo) {
                // midTimestamp is too small, search right half
                low = mid + 1;
            } else {
                // midTimestamp >= timestampLo, answer is at mid or to the left
                high = mid;
            }
        }

        // Linear scan for small range - avoids binary search overhead
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

    // Binary search that returns timestamp <= timestampLo (prevailing/exact match),
    // or first row in [timestampLo, timestampHi] if no prevailing exists.
    // The prevailingFromLinearScan parameter carries forward any prevailing found during linear scan.
    private long binarySearchWithPrevailing(long timestampLo, long timestampHi, long rowLo) {
        long low = rowLo;
        long high = timeFrame.getRowHi() - 1;
        // Start with the row just before rowLo as prevailing candidate (from linear scan)
        long prevailingCandidate = rowLo > timeFrame.getRowLo() ? rowLo - 1 : Long.MIN_VALUE;

        while (high - low > 65) {
            final long mid = (low + high) >>> 1;
            recordAtRowIndex(mid);
            long midTimestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);

            if (midTimestamp < timestampLo) {
                prevailingCandidate = mid;
                low = mid + 1;
            } else if (midTimestamp == timestampLo) {
                // In case of multiple values equal to timestampLo, find the first one
                return binarySearchScrollUp(low, mid, timestampLo);
            } else {
                high = mid - 1;
            }
        }

        for (long r = low; r <= high; r++) {
            recordAtRowIndex(r);
            long timestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);

            if (timestamp < timestampLo) {
                prevailingCandidate = r;
            } else if (timestamp == timestampLo) {
                return r;
            } else {
                if (prevailingCandidate != Long.MIN_VALUE) {
                    return prevailingCandidate;
                }
                if (timestamp <= timestampHi) {
                    return r;
                }
                return Long.MIN_VALUE;
            }
        }

        return prevailingCandidate;
    }

    private void bookmarkCurrentFrame(long rowId) {
        bookmarkedFrameIndex = timeFrame.getFrameIndex();
        bookmarkedRowIndex = rowId;
    }

    private long linearScan(long timestampLo, long timestampHi, long rowLo, boolean recordPrevailing) {
        final long scanHi = Math.min(rowLo + lookahead, timeFrame.getRowHi());
        for (long r = rowLo; ; ) {
            recordAtRowIndex(r);
            final long timestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);
            if (timestamp >= timestampLo) {
                if (recordPrevailing && r > 0) {
                    prevailingFrameIndex = timeFrame.getFrameIndex();
                    prevailingRowIndex = r - 1;
                }
                if (timestamp <= timestampHi) {
                    return r;
                }
                return Long.MIN_VALUE;
            }

            if (++r >= scanHi) {
                break;
            }
        }
        return -rowLo - lookahead - 1;
    }

    // Linear scan that returns timestamp <= timestampLo (prevailing/exact match).
    // Returns:
    //   >= 0: found row index (prevailing or exact match)
    //   Long.MIN_VALUE: no prevailing found (all rows > timestampLo)
    //   negative value: need binary search, encoded as -(last scanned row) - 2
    private long linearScanWithPrevailing(long timestampLo, long timestampHi, long rowLo) {
        final long scanHi = Math.min(rowLo + lookahead, timeFrame.getRowHi());
        long prevailingRow = Long.MIN_VALUE;

        for (long r = rowLo; r < scanHi; r++) {
            recordAtRowIndex(r);
            final long timestamp = scaleTimestamp(record.getTimestamp(timestampIndex), scale);

            if (timestamp < timestampLo) {
                prevailingRow = r;
            } else if (timestamp == timestampLo) {
                return r;
            } else {
                if (prevailingRow != Long.MIN_VALUE) {
                    return prevailingRow;
                }
                if (timestamp <= timestampHi) {
                    return r;
                }
                return Long.MIN_VALUE;
            }
        }

        if (scanHi < timeFrame.getRowHi()) {
            return -(scanHi - 1) - 2;
        }
        return prevailingRow;
    }
}

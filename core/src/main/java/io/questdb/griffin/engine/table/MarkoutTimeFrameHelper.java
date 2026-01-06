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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.Rows;

/**
 * Helper for navigating and searching through time frames for ASOF join lookups
 * in markout queries.
 * <p>
 * Wraps a {@link TimeFrameCursor} and provides efficient ASOF-style lookups
 * (finding the last row with timestamp <= target) using a combination of
 * linear scan and binary search. Maintains bookmarks for efficient subsequent lookups.
 */
public class MarkoutTimeFrameHelper {
    private static final int LINEAR_SCAN_LIMIT = 64;

    private final long lookahead;
    private int bookmarkedFrameIndex = -1;
    private long bookmarkedRowIndex = Long.MIN_VALUE;
    private Record record;
    private TimeFrame timeFrame;
    private TimeFrameCursor timeFrameCursor;
    private int timestampIndex;

    public MarkoutTimeFrameHelper(long lookahead) {
        this.lookahead = lookahead;
    }

    /**
     * Finds the row with the largest timestamp <= targetTimestamp (ASOF semantics).
     * Returns the row index within the current time frame, or Long.MIN_VALUE if not found.
     * <p>
     * After a successful call, use {@link #recordAt(long)} to position the record.
     *
     * @param targetTimestamp the target timestamp to search for
     * @return row index if found, Long.MIN_VALUE otherwise
     */
    public long findAsOfRow(long targetTimestamp) {
        // Start from bookmarked position if available
        long rowLo = Long.MIN_VALUE;
        if (bookmarkedFrameIndex != -1) {
            timeFrameCursor.jumpTo(bookmarkedFrameIndex);
            if (timeFrameCursor.open() > 0) {
                long frameTsHi = timeFrame.getTimestampHi() - 1; // timestampHi is exclusive
                if (frameTsHi <= targetTimestamp) {
                    // Bookmarked frame is entirely <= target, use as candidate
                    rowLo = bookmarkedRowIndex;
                } else if (timeFrame.getTimestampLo() <= targetTimestamp) {
                    // Target is within this frame
                    rowLo = bookmarkedRowIndex;
                }
            }
        }

        // Track the best ASOF match found so far
        int bestFrameIndex = -1;
        long bestRowIndex = Long.MIN_VALUE;

        for (; ; ) {
            if (rowLo == Long.MIN_VALUE) {
                // Navigate through frames to find one containing or before the target
                while (timeFrameCursor.next()) {
                    long frameEstimateHi = timeFrame.getTimestampEstimateHi();

                    if (frameEstimateHi <= targetTimestamp) {
                        // Frame is entirely before target, record as candidate
                        if (timeFrameCursor.open() > 0) {
                            bestFrameIndex = timeFrame.getFrameIndex();
                            bestRowIndex = timeFrame.getRowHi() - 1;
                            bookmarkCurrentFrame(0);
                        }
                        continue;
                    }

                    // Frame may contain or straddle the target
                    if (timeFrame.getTimestampEstimateLo() <= targetTimestamp) {
                        if (timeFrameCursor.open() == 0) {
                            continue;
                        }

                        long frameTsLo = timeFrame.getTimestampLo();
                        long frameTsHi = timeFrame.getTimestampHi() - 1;

                        if (frameTsHi <= targetTimestamp) {
                            // Entire frame is <= target
                            bestFrameIndex = timeFrame.getFrameIndex();
                            bestRowIndex = timeFrame.getRowHi() - 1;
                            bookmarkCurrentFrame(0);
                            continue;
                        }

                        if (frameTsLo <= targetTimestamp) {
                            // Target is within this frame, need to search
                            rowLo = timeFrame.getRowLo();
                            break;
                        }

                        // Frame is entirely after target, return best found so far
                        if (bestRowIndex != Long.MIN_VALUE) {
                            bookmarkedFrameIndex = bestFrameIndex;
                            bookmarkedRowIndex = bestRowIndex;
                            return bestRowIndex;
                        }
                        return Long.MIN_VALUE;
                    }

                    // Frame is entirely after target
                    if (bestRowIndex != Long.MIN_VALUE) {
                        bookmarkedFrameIndex = bestFrameIndex;
                        bookmarkedRowIndex = bestRowIndex;
                        return bestRowIndex;
                    }
                    return Long.MIN_VALUE;
                }

                if (rowLo == Long.MIN_VALUE) {
                    // No more frames, return best found
                    if (bestRowIndex != Long.MIN_VALUE) {
                        bookmarkedFrameIndex = bestFrameIndex;
                        bookmarkedRowIndex = bestRowIndex;
                        return bestRowIndex;
                    }
                    return Long.MIN_VALUE;
                }
            }

            // Search within the current frame for the ASOF row
            bookmarkCurrentFrame(rowLo);
            timeFrameCursor.recordAt(record, Rows.toRowID(timeFrame.getFrameIndex(), timeFrame.getRowLo()));

            // Try linear scan first
            long scanResult = linearScanAsOf(targetTimestamp, rowLo);
            if (scanResult >= 0) {
                bookmarkCurrentFrame(scanResult);
                return scanResult;
            } else if (scanResult == Long.MIN_VALUE) {
                // All rows in scan range are > target, check if we have a previous best
                if (bestRowIndex != Long.MIN_VALUE) {
                    bookmarkedFrameIndex = bestFrameIndex;
                    bookmarkedRowIndex = bestRowIndex;
                    return bestRowIndex;
                }
                return Long.MIN_VALUE;
            }

            // Need binary search
            long searchStart = -scanResult - 1;
            long searchResult = binarySearchAsOf(targetTimestamp, searchStart);
            if (searchResult != Long.MIN_VALUE) {
                bookmarkCurrentFrame(searchResult);
                return searchResult;
            }

            // No match in this frame, try next
            rowLo = Long.MIN_VALUE;
        }
    }

    public Record getRecord() {
        return record;
    }

    public SymbolTableSource getSymbolTableSource() {
        return timeFrameCursor;
    }

    public int getTimestampIndex() {
        return timestampIndex;
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

    public long toRowId(long rowIndex) {
        return Rows.toRowID(timeFrame.getFrameIndex(), rowIndex);
    }

    public void toTop() {
        if (timeFrameCursor != null) {
            timeFrameCursor.toTop();
        }
        bookmarkedFrameIndex = -1;
        bookmarkedRowIndex = Long.MIN_VALUE;
    }

    /**
     * Binary search for the last row with timestamp <= targetTimestamp.
     */
    private long binarySearchAsOf(long targetTimestamp, long rowLo) {
        long low = rowLo;
        long high = timeFrame.getRowHi() - 1;
        long result = Long.MIN_VALUE;

        while (high - low > LINEAR_SCAN_LIMIT) {
            long mid = (low + high) >>> 1;
            recordAtRowIndex(mid);
            long midTimestamp = record.getTimestamp(timestampIndex);

            if (midTimestamp <= targetTimestamp) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        // Linear scan for small range
        for (long r = low; r <= high; r++) {
            recordAtRowIndex(r);
            long timestamp = record.getTimestamp(timestampIndex);
            if (timestamp <= targetTimestamp) {
                result = r;
            } else {
                break;
            }
        }

        return result;
    }

    private void bookmarkCurrentFrame(long rowIndex) {
        bookmarkedFrameIndex = timeFrame.getFrameIndex();
        bookmarkedRowIndex = rowIndex;
    }

    /**
     * Linear scan for the last row with timestamp <= targetTimestamp.
     * Returns:
     * - >= 0: found row index
     * - Long.MIN_VALUE: all rows > targetTimestamp
     * - negative value: need binary search, encoded as -(last scanned row) - 1
     */
    private long linearScanAsOf(long targetTimestamp, long rowLo) {
        long scanHi = Math.min(rowLo + lookahead, timeFrame.getRowHi());
        long result = Long.MIN_VALUE;

        for (long r = rowLo; r < scanHi; r++) {
            recordAtRowIndex(r);
            long timestamp = record.getTimestamp(timestampIndex);

            if (timestamp <= targetTimestamp) {
                result = r;
            } else {
                // Found first row > target, return previous row if any
                return result;
            }
        }

        // Reached scan limit
        if (scanHi < timeFrame.getRowHi()) {
            // More rows to search, need binary search
            if (result != Long.MIN_VALUE) {
                // Last scanned row was <= target, but there might be more
                return -scanHi - 1;
            }
            return -scanHi - 1;
        }

        // Scanned entire frame
        return result;
    }
}

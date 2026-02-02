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

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.Rows;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Helper for navigating and searching through time frames for ASOF join lookups
 * in markout queries.
 * <p>
 * Wraps a {@link TimeFrameCursor} and provides efficient ASOF-style lookups
 * (finding the last row with timestamp <= target) using a combination of
 * linear scan and binary search. Maintains bookmarks for efficient subsequent lookups.
 * <p>
 * Also supports forward scanning to build key-to-rowId maps for keyed ASOF JOIN.
 */
public class MarkoutTimeFrameHelper {
    private static final int LINEAR_SCAN_LIMIT = 64;
    private final long lookahead;
    // Scale factor for slave timestamps to normalize to nanoseconds (1 if no scaling needed)
    private final long slaveTsScale;
    // Bookmark position: where to start the next findAsOfRow search (optimization for sequential access)
    private int bookmarkedFrameIndex = -1;
    private long bookmarkedRowIndex = Long.MIN_VALUE;
    // Current position: result of the last findAsOfRow or backwardScanForKeyMatch call
    private int currentFrameIndex = -1;
    private long currentRowIndex = -1;
    private Record record;
    private TimeFrame timeFrame;
    private TimeFrameCursor timeFrameCursor;
    private int timestampIndex;

    public MarkoutTimeFrameHelper(long lookahead, long slaveTsScale) {
        this.lookahead = lookahead;
        this.slaveTsScale = slaveTsScale;
    }

    /**
     * Forward scan from highWaterMark to targetRowId, updating the map with all keys encountered.
     * <p>
     * This method is used for efficient sorted horizon timestamp processing. Since horizon
     * timestamps are processed in ascending order, the ASOF positions also generally increase.
     * By scanning forward and caching all keys, we ensure each slave row is scanned at most once.
     * <p>
     * The map is updated with (key -> rowId), with later positions overwriting earlier ones.
     * This means after scanning, the map contains the LATEST position for each key up to targetRowId.
     *
     * @param targetRowId     the ASOF position to scan up to (inclusive)
     * @param highWaterMark   the last scanned position (exclusive), or Long.MIN_VALUE to start from beginning
     * @param slaveKeyCopier  copier for slave's join key columns
     * @param keyToRowIdMap   map to update with (key -> rowId) entries
     * @return number of rows scanned, or -1 if targetRowId is before highWaterMark
     */
    public long forwardScanToPosition(
            long targetRowId,
            long highWaterMark,
            RecordSink slaveKeyCopier,
            Map keyToRowIdMap
    ) {
        if (targetRowId == Long.MIN_VALUE) {
            return 0;
        }

        // If target is at or before high water mark, no scanning needed
        if (highWaterMark != Long.MIN_VALUE && targetRowId <= highWaterMark) {
            return 0;
        }

        int targetFrameIndex = Rows.toPartitionIndex(targetRowId);
        long targetRowIndex = Rows.toLocalRowID(targetRowId);

        // Determine starting position
        int startFrameIndex;
        long startRowIndex;
        if (highWaterMark == Long.MIN_VALUE) {
            // Start from the beginning
            timeFrameCursor.toTop();
            if (!timeFrameCursor.next()) {
                return 0;
            }
            if (timeFrameCursor.open() == 0) {
                // Try to find first non-empty frame
                while (timeFrameCursor.next()) {
                    if (timeFrameCursor.open() > 0) {
                        break;
                    }
                }
                if (timeFrame.getRowHi() <= timeFrame.getRowLo()) {
                    return 0;
                }
            }
            startFrameIndex = timeFrame.getFrameIndex();
            startRowIndex = timeFrame.getRowLo();
        } else {
            // Start from just after high water mark
            startFrameIndex = Rows.toPartitionIndex(highWaterMark);
            startRowIndex = Rows.toLocalRowID(highWaterMark) + 1;

            timeFrameCursor.jumpTo(startFrameIndex);
            if (timeFrameCursor.open() == 0) {
                return 0;
            }

            // Check if we need to move to next frame
            if (startRowIndex >= timeFrame.getRowHi()) {
                if (!timeFrameCursor.next()) {
                    return 0;
                }
                if (timeFrameCursor.open() == 0) {
                    return 0;
                }
                startFrameIndex = timeFrame.getFrameIndex();
                startRowIndex = timeFrame.getRowLo();
            }
        }

        long rowsScanned = 0;
        int frameIndex = startFrameIndex;
        long rowIndex = startRowIndex;

        while (true) {
            long currentRowId = Rows.toRowID(frameIndex, rowIndex);

            // Check if we've reached the target
            if (currentRowId > targetRowId) {
                break;
            }

            // Position record and cache the key
            timeFrameCursor.recordAtRowIndex(record, rowIndex);
            MapKey key = keyToRowIdMap.withKey();
            key.put(record, slaveKeyCopier);
            MapValue value = key.createValue();
            // Always update (overwrite) - we want the LATEST position for each key
            value.putLong(0, currentRowId);
            rowsScanned++;

            // Check if we've reached the target
            if (currentRowId == targetRowId) {
                break;
            }

            // Move forward
            rowIndex++;
            if (rowIndex >= timeFrame.getRowHi()) {
                // Move to next frame
                if (!timeFrameCursor.next()) {
                    break;
                }
                if (timeFrameCursor.open() == 0) {
                    // Empty frame, try next
                    continue;
                }
                frameIndex = timeFrame.getFrameIndex();
                rowIndex = timeFrame.getRowLo();
            }
        }

        return rowsScanned;
    }

    /**
     * Finds the row with the largest timestamp <= targetTimestamp (ASOF semantics).
     * Returns the row ID, or Long.MIN_VALUE if not found.
     * <p>
     * After a successful call, the helper is positioned at the found row.
     *
     * @param targetTimestamp the target timestamp to search for
     * @return rowId if found, Long.MIN_VALUE otherwise
     */
    public long findAsOfRow(long targetTimestamp) {
        // Start from bookmarked position if available
        long rowLo = Long.MIN_VALUE;
        if (bookmarkedFrameIndex != -1) {
            timeFrameCursor.jumpTo(bookmarkedFrameIndex);
            if (timeFrameCursor.open() > 0) {
                long frameTsHi = scaleTimestamp(timeFrame.getTimestampHi() - 1, slaveTsScale); // timestampHi is exclusive
                if (frameTsHi <= targetTimestamp) {
                    // Bookmarked frame is entirely <= target, use as candidate
                    rowLo = bookmarkedRowIndex;
                } else if (scaleTimestamp(timeFrame.getTimestampLo(), slaveTsScale) <= targetTimestamp) {
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
                    long frameEstimateHi = scaleTimestamp(timeFrame.getTimestampEstimateHi(), slaveTsScale);

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
                    if (scaleTimestamp(timeFrame.getTimestampEstimateLo(), slaveTsScale) <= targetTimestamp) {
                        if (timeFrameCursor.open() == 0) {
                            continue;
                        }

                        // Scale slave frame timestamps to common unit
                        long frameTsLo = scaleTimestamp(timeFrame.getTimestampLo(), slaveTsScale);
                        long frameTsHi = scaleTimestamp(timeFrame.getTimestampHi() - 1, slaveTsScale);

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
                            setCurrentPosition(bestFrameIndex, bestRowIndex);
                            return Rows.toRowID(bestFrameIndex, bestRowIndex);
                        }
                        // Bookmark current frame so subsequent searches with larger timestamps can find it
                        bookmarkCurrentFrame(0);
                        return Long.MIN_VALUE;
                    }

                    // Frame is entirely after target
                    if (bestRowIndex != Long.MIN_VALUE) {
                        bookmarkedFrameIndex = bestFrameIndex;
                        bookmarkedRowIndex = bestRowIndex;
                        setCurrentPosition(bestFrameIndex, bestRowIndex);
                        return Rows.toRowID(bestFrameIndex, bestRowIndex);
                    }
                    // Bookmark current frame so subsequent searches with larger timestamps can find it
                    bookmarkCurrentFrame(0);
                    return Long.MIN_VALUE;
                }

                if (rowLo == Long.MIN_VALUE) {
                    // No more frames, return best found
                    if (bestRowIndex != Long.MIN_VALUE) {
                        bookmarkedFrameIndex = bestFrameIndex;
                        bookmarkedRowIndex = bestRowIndex;
                        setCurrentPosition(bestFrameIndex, bestRowIndex);
                        return Rows.toRowID(bestFrameIndex, bestRowIndex);
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
                setCurrentPosition(timeFrame.getFrameIndex(), scanResult);
                return Rows.toRowID(timeFrame.getFrameIndex(), scanResult);
            } else if (scanResult == Long.MIN_VALUE) {
                // All rows in scan range are > target, check if we have a previous best
                if (bestRowIndex != Long.MIN_VALUE) {
                    bookmarkedFrameIndex = bestFrameIndex;
                    bookmarkedRowIndex = bestRowIndex;
                    setCurrentPosition(bestFrameIndex, bestRowIndex);
                    return Rows.toRowID(bestFrameIndex, bestRowIndex);
                }
                return Long.MIN_VALUE;
            }

            // Need binary search
            long searchStart = -scanResult - 1;
            long searchResult = binarySearchAsOf(targetTimestamp, searchStart);
            if (searchResult != Long.MIN_VALUE) {
                bookmarkCurrentFrame(searchResult);
                setCurrentPosition(timeFrame.getFrameIndex(), searchResult);
                return Rows.toRowID(timeFrame.getFrameIndex(), searchResult);
            }

            // No match in this frame, try next
            rowLo = Long.MIN_VALUE;
        }
    }

    public Record getRecord() {
        return record;
    }

    public void of(TimeFrameCursor timeFrameCursor) {
        this.timeFrameCursor = timeFrameCursor;
        this.record = timeFrameCursor.getRecord();
        this.timeFrame = timeFrameCursor.getTimeFrame();
        this.timestampIndex = timeFrameCursor.getTimestampIndex();
        toTop();
    }

    public void recordAt(long rowId) {
        int frameIndex = Rows.toPartitionIndex(rowId);
        long rowIndex = Rows.toLocalRowID(rowId);
        timeFrameCursor.jumpTo(frameIndex);
        timeFrameCursor.open();
        timeFrameCursor.recordAtRowIndex(record, rowIndex);
        setCurrentPosition(frameIndex, rowIndex);
    }

    /**
     * Set the bookmark to a specific row ID.
     * This allows external code to control where findAsOfRow starts searching.
     */
    public void setBookmark(long rowId) {
        if (rowId == Long.MIN_VALUE) {
            bookmarkedFrameIndex = -1;
            bookmarkedRowIndex = Long.MIN_VALUE;
        } else {
            bookmarkedFrameIndex = Rows.toPartitionIndex(rowId);
            bookmarkedRowIndex = Rows.toLocalRowID(rowId);
        }
    }

    public void toTop() {
        if (timeFrameCursor != null) {
            timeFrameCursor.toTop();
        }
        bookmarkedFrameIndex = -1;
        bookmarkedRowIndex = Long.MIN_VALUE;
        currentFrameIndex = -1;
        currentRowIndex = -1;
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
            timeFrameCursor.recordAtRowIndex(record, mid);
            long midTimestamp = scaleTimestamp(record.getTimestamp(timestampIndex), slaveTsScale);

            if (midTimestamp <= targetTimestamp) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        // Linear scan for small range
        for (long r = low; r <= high; r++) {
            timeFrameCursor.recordAtRowIndex(record, r);
            long timestamp = scaleTimestamp(record.getTimestamp(timestampIndex), slaveTsScale);
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
            timeFrameCursor.recordAtRowIndex(record, r);
            // Scale slave timestamp to common unit for cross-resolution support
            long timestamp = scaleTimestamp(record.getTimestamp(timestampIndex), slaveTsScale);

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
            return -scanHi - 1;
        }

        // Scanned entire frame
        return result;
    }

    private void setCurrentPosition(int frameIndex, long rowIndex) {
        this.currentFrameIndex = frameIndex;
        this.currentRowIndex = rowIndex;
    }
}

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
 * Also supports forward and backward scanning to build key-to-rowId maps for keyed ASOF JOIN.
 * Watermarks are maintained internally to track scanning progress within a frame.
 */
public class MarkoutTimeFrameHelper {
    private static final int LINEAR_SCAN_LIMIT = 64;
    private final long lookahead;
    // Scale factor for slave timestamps to normalize to nanoseconds (1 if no scaling needed)
    private final long slaveTsScale;
    // Backward watermark: lowest rowId we've backward-scanned (inclusive)
    private long backwardWatermark = Long.MAX_VALUE;
    // Bookmark position: where to start the next findAsOfRow search (optimization for sequential access)
    private int bookmarkedFrameIndex = -1;
    private long bookmarkedRowIndex = Long.MIN_VALUE;
    // Forward watermark: highest rowId we've forward-scanned (inclusive)
    private long forwardWatermark = Long.MIN_VALUE;
    private Record record;
    private TimeFrame timeFrame;
    private TimeFrameCursor timeFrameCursor;
    private int timestampIndex;

    public MarkoutTimeFrameHelper(long lookahead, long slaveTsScale) {
        this.lookahead = lookahead;
        this.slaveTsScale = slaveTsScale;
    }

    /**
     * Backward scan from current backward watermark toward smaller rowIds, looking for a matching key.
     * <p>
     * Adds all keys encountered to the map (only if not already present, since we want
     * the latest/highest rowId for each key). Stops when the target key is found.
     * <p>
     * This is used for the "dense ASOF" algorithm: when a key is not in the cache,
     * we scan backward from the current position to find earlier occurrences.
     * <p>
     * Updates the internal backward watermark after scanning.
     *
     * @param startRowId      starting position for backward scan (inclusive), typically the ASOF position
     * @param masterRecord    master record containing the target key
     * @param masterKeyCopier copier for master's join key columns
     * @param slaveKeyCopier  copier for slave's join key columns
     * @param keyToRowIdMap   map to update with (key -> rowId) entries
     * @return the rowId where target key was found, or Long.MIN_VALUE if not found
     */
    public long backwardScanForKeyMatch(
            long startRowId,
            Record masterRecord,
            RecordSink masterKeyCopier,
            RecordSink slaveKeyCopier,
            Map keyToRowIdMap
    ) {
        if (startRowId == Long.MIN_VALUE) {
            return Long.MIN_VALUE;
        }

        // Don't scan beyond what we've already scanned
        // Start from min(startRowId, backwardWatermark - 1)
        long effectiveStart = startRowId;
        if (backwardWatermark != Long.MAX_VALUE && backwardWatermark > 0) {
            effectiveStart = Math.min(startRowId, backwardWatermark - 1);
        }

        if (backwardWatermark != Long.MAX_VALUE && effectiveStart < backwardWatermark) {
            // Already scanned this region, just look up in map
            MapKey targetKey = keyToRowIdMap.withKey();
            targetKey.put(masterRecord, masterKeyCopier);
            MapValue targetValue = targetKey.findValue();
            return targetValue != null ? targetValue.getLong(0) : Long.MIN_VALUE;
        }

        int frameIndex = Rows.toPartitionIndex(effectiveStart);
        long rowIndex = Rows.toLocalRowID(effectiveStart);

        // Jump to the starting frame
        timeFrameCursor.jumpTo(frameIndex);
        if (timeFrameCursor.open() == 0) {
            return Long.MIN_VALUE;
        }

        // Pre-compute master key hash once (avoids re-hashing on every iteration)
        final MapKey masterKey = keyToRowIdMap.withKey();
        masterKey.put(masterRecord, masterKeyCopier);
        masterKey.commit();
        final long masterHash = masterKey.hash();

        while (true) {
            final long currentRowId = Rows.toRowID(frameIndex, rowIndex);

            // Update backward watermark
            if (backwardWatermark == Long.MAX_VALUE || currentRowId < backwardWatermark) {
                backwardWatermark = currentRowId;
            }

            // Position record at current row
            timeFrameCursor.recordAtRowIndex(record, rowIndex);

            // Add key to map only if not already present (we want latest/highest rowId)
            final MapKey slaveKey = keyToRowIdMap.withKey();
            slaveKey.put(record, slaveKeyCopier);
            slaveKey.commit();
            final long slaveHash = slaveKey.hash();
            final MapValue value = slaveKey.createValue(slaveHash);
            if (value.isNew()) {
                value.putLong(0, currentRowId);
            }

            // Fast path: only check for master key match when hashes match
            // This eliminates N-1 redundant master key lookups
            if (slaveHash == masterHash) {
                // Hashes match - verify with actual map lookup (handles rare hash collisions)
                final MapKey targetKey = keyToRowIdMap.withKey();
                targetKey.put(masterRecord, masterKeyCopier);
                final MapValue targetValue = targetKey.findValue();
                if (targetValue != null) {
                    // Found the target key in the map
                    return targetValue.getLong(0);
                }
            }

            // Move backward
            rowIndex--;
            if (rowIndex < timeFrame.getRowLo()) {
                // Move to previous frame
                if (frameIndex == 0) {
                    // No more frames, update watermark to indicate we've scanned everything
                    backwardWatermark = 0;
                    break;
                }
                frameIndex--;
                timeFrameCursor.jumpTo(frameIndex);
                if (timeFrameCursor.open() == 0) {
                    // Empty frame, try previous
                    continue;
                }
                rowIndex = timeFrame.getRowHi() - 1;
            }
        }

        return Long.MIN_VALUE;
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
                    final long frameEstimateHi = scaleTimestamp(timeFrame.getTimestampEstimateHi(), slaveTsScale);

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
                        final long frameTsLo = scaleTimestamp(timeFrame.getTimestampLo(), slaveTsScale);
                        final long frameTsHi = scaleTimestamp(timeFrame.getTimestampHi() - 1, slaveTsScale);

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
                return Rows.toRowID(timeFrame.getFrameIndex(), scanResult);
            } else if (scanResult == Long.MIN_VALUE) {
                // All rows in scan range are > target, check if we have a previous best
                if (bestRowIndex != Long.MIN_VALUE) {
                    bookmarkedFrameIndex = bestFrameIndex;
                    bookmarkedRowIndex = bestRowIndex;
                    return Rows.toRowID(bestFrameIndex, bestRowIndex);
                }
                return Long.MIN_VALUE;
            }

            // Need binary search
            final long searchStart = -scanResult - 1;
            final long searchResult = binarySearchAsOf(targetTimestamp, searchStart);
            if (searchResult != Long.MIN_VALUE) {
                bookmarkCurrentFrame(searchResult);
                return Rows.toRowID(timeFrame.getFrameIndex(), searchResult);
            }

            // No match in this frame, try next
            rowLo = Long.MIN_VALUE;
        }
    }

    /**
     * Forward scan from current forward watermark to targetRowId, updating the map with all keys encountered.
     * <p>
     * This method is used for efficient sorted horizon timestamp processing. Since horizon
     * timestamps are processed in ascending order, the ASOF positions also generally increase.
     * By scanning forward and caching all keys, we ensure each slave row is scanned at most once.
     * <p>
     * The map is updated with (key -> rowId), with later positions overwriting earlier ones.
     * This means after scanning, the map contains the LATEST position for each key up to targetRowId.
     * <p>
     * Updates the internal forward watermark to targetRowId after successful scan.
     *
     * @param targetRowId    the ASOF position to scan up to (inclusive)
     * @param slaveKeyCopier copier for slave's join key columns
     * @param keyToRowIdMap  map to update with (key -> rowId) entries
     */
    public void forwardScanToPosition(
            long targetRowId,
            RecordSink slaveKeyCopier,
            Map keyToRowIdMap
    ) {
        if (targetRowId == Long.MIN_VALUE) {
            return;
        }

        // If target is at or before forward watermark, no scanning needed
        if (forwardWatermark != Long.MIN_VALUE && targetRowId <= forwardWatermark) {
            return;
        }

        // Determine starting position
        int startFrameIndex;
        long startRowIndex;
        if (forwardWatermark == Long.MIN_VALUE) {
            // Start from the beginning
            timeFrameCursor.toTop();
            if (!timeFrameCursor.next()) {
                return;
            }
            if (timeFrameCursor.open() == 0) {
                // Try to find first non-empty frame
                while (timeFrameCursor.next()) {
                    if (timeFrameCursor.open() > 0) {
                        break;
                    }
                }
                if (timeFrame.getRowHi() <= timeFrame.getRowLo()) {
                    return;
                }
            }
            startFrameIndex = timeFrame.getFrameIndex();
            startRowIndex = timeFrame.getRowLo();
        } else {
            // Start from just after forward watermark
            startFrameIndex = Rows.toPartitionIndex(forwardWatermark);
            startRowIndex = Rows.toLocalRowID(forwardWatermark) + 1;

            timeFrameCursor.jumpTo(startFrameIndex);
            if (timeFrameCursor.open() == 0) {
                return;
            }

            // Check if we need to move to next frame
            if (startRowIndex >= timeFrame.getRowHi()) {
                if (!timeFrameCursor.next()) {
                    return;
                }
                if (timeFrameCursor.open() == 0) {
                    return;
                }
                startFrameIndex = timeFrame.getFrameIndex();
                startRowIndex = timeFrame.getRowLo();
            }
        }

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

            // Update forward watermark
            forwardWatermark = currentRowId;

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
    }

    /**
     * Returns the current forward watermark (highest rowId we've forward-scanned).
     * Returns Long.MIN_VALUE if no forward scanning has been done yet.
     */
    public long getForwardWatermark() {
        return forwardWatermark;
    }

    public Record getRecord() {
        return record;
    }

    /**
     * Initialize the forward watermark. Called after the first backward scan
     * to set the starting point for subsequent forward scans.
     */
    public void initForwardWatermark(long rowId) {
        this.forwardWatermark = rowId;
    }

    public void of(TimeFrameCursor timeFrameCursor) {
        this.timeFrameCursor = timeFrameCursor;
        this.record = timeFrameCursor.getRecord();
        this.timeFrame = timeFrameCursor.getTimeFrame();
        this.timestampIndex = timeFrameCursor.getTimestampIndex();
        // Reset all state for new query
        bookmarkedFrameIndex = -1;
        bookmarkedRowIndex = Long.MIN_VALUE;
        toTop();
    }

    public void recordAt(long rowId) {
        int frameIndex = Rows.toPartitionIndex(rowId);
        long rowIndex = Rows.toLocalRowID(rowId);
        timeFrameCursor.jumpTo(frameIndex);
        timeFrameCursor.open();
        timeFrameCursor.recordAtRowIndex(record, rowIndex);
    }

    /**
     * Reset state for processing a new frame.
     * <p>
     * Resets the forward/backward watermarks (used for key caching within a frame),
     * but preserves the bookmark position (used by findAsOfRow) to speed up
     * ASOF lookups when timestamps increase across frames.
     */
    public void toTop() {
        if (timeFrameCursor != null) {
            timeFrameCursor.toTop();
        }
        // Note: bookmarkedFrameIndex/bookmarkedRowIndex are intentionally NOT reset.
        // They help findAsOfRow() start closer to the target when processing subsequent frames.

        // Reset watermarks for new frame processing (caching is per-frame)
        forwardWatermark = Long.MIN_VALUE;
        backwardWatermark = Long.MAX_VALUE;
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
}

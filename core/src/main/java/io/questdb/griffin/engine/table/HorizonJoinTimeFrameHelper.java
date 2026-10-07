/*+*****************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.LongList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Helper for navigating and searching through time frames for ASOF join lookups
 * in HORIZON JOIN queries.
 * <p>
 * Wraps a {@link TimeFrameCursor} and provides efficient ASOF-style lookups
 * (finding the last row with timestamp less or equal to target) using a combination of
 * linear scan and binary search. Maintains bookmarks for efficient subsequent lookups.
 * <p>
 * Also maintains a key-to-rowId map for keyed ASOF JOIN, together with the intervals of slave
 * rows that the map has not read yet, which the keyed lookups read lazily, see
 * {@link #findKeyedAsOfMatch}.
 */
public class HorizonJoinTimeFrameHelper {
    private static final int LINEAR_SCAN_LIMIT = 64;
    // Most holes that findKeyedAsOfMatch() keeps, see compactHoles(). The holes take two longs each,
    // so their list never grows beyond 2 * MAX_HOLE_COUNT longs, 256 KiB.
    private static final int MAX_HOLE_COUNT = 16_384;
    // Frames below the frame of the position of a compaction from which on the holes in Parquet
    // frames that start there only merge, see compactHoles()
    private static final int UNCACHED_FRAME_DISTANCE = 3;
    // Adaptive scan thresholds (set at construction, used by findKeyedAsOfMatch)
    private final long bwdScanAbsoluteThreshold;
    private final long bwdScanMinGap;
    private final long bwdScanSwitchFactor;
    // Costs of removing a hole, counted by bit length, see compactHoles()
    private final int[] holeCostCounts = new int[Long.SIZE + 1];
    // Intervals of slave rows that the key map of findKeyedAsOfMatch() has not read, two longs each:
    // the row id right below the interval and the row id right above it. Lowest interval first.
    private final LongList holes = new LongList();
    private final long lookahead;
    private final int maxHoleCount;
    // Scale factor for slave timestamps to normalize to nanoseconds (1 if no scaling needed)
    private final long slaveTsScale;
    // Slave rows that the keyed lookups have read, used by the adaptive switching logic. In
    // backward-only mode, every lookup that goes on below the rows read at its position counts the
    // lowest of them once more, as the backward scans that read it again counted it.
    private long backwardScanRows;
    // Bookmark position: where to start the next findAsOfRow search (optimization for sequential access)
    private int bookmarkedFrameIndex = -1;
    private long bookmarkedRowIndex = Long.MIN_VALUE;
    // Adaptive scan state (managed by findKeyedAsOfMatch, reset by toTop)
    private long bwdScanRowsAtPositionStart;
    // Backward scan row counter at the first position of the current window of small gaps
    private long bwdScanRowsAtWindowStart;
    // Distance in rows that the current window of small gaps covers
    private long bwdScanWindowGap;
    // Cached findAsOfRow result: valid while target timestamp < cachedNextRowTs
    private long cachedAsOfRowId = Long.MIN_VALUE;
    private long cachedNextRowTs = Long.MIN_VALUE;
    // Lowest row id that a hole in Parquet frames that compactHoles() fills may start at: the
    // position of the previous compaction of the epoch, or Long.MIN_VALUE before the first one,
    // which compactHoles() raises to the first row of the frames near the position, see
    // isHoleFillable()
    private long fillableHoleRowIdLo = Long.MIN_VALUE;
    // Rows that compactHoles() has filled over the helper's life, for tests
    private long filledRowCount;
    // Whether findKeyedAsOfMatch() keeps the key map across ASOF positions, until toTop()
    private boolean isKeyMapKept;
    // Holes that compactHoles() has merged over the helper's life, for tests
    private long mergedHoleCount;
    private long prevAsOfRowId = Long.MIN_VALUE;
    private Record record;
    private TimeFrame timeFrame;
    private TimeFrameCursor timeFrameCursor;
    private int timestampIndex;

    public HorizonJoinTimeFrameHelper(
            long lookahead,
            long slaveTsScale,
            long bwdScanAbsoluteThreshold,
            long bwdScanMinGap,
            long bwdScanSwitchFactor
    ) {
        this(MAX_HOLE_COUNT, lookahead, slaveTsScale, bwdScanAbsoluteThreshold, bwdScanMinGap, bwdScanSwitchFactor);
    }

    /**
     * Keeps at most the given number of holes instead of {@link #MAX_HOLE_COUNT}, so that tests
     * reach {@link #compactHoles} with few positions. A compaction keeps one hole and removes
     * another, so the limit is at least 2.
     */
    @TestOnly
    public HorizonJoinTimeFrameHelper(
            long lookahead,
            long slaveTsScale,
            long bwdScanAbsoluteThreshold,
            long bwdScanMinGap,
            long bwdScanSwitchFactor,
            int maxHoleCount
    ) {
        this(maxHoleCount, lookahead, slaveTsScale, bwdScanAbsoluteThreshold, bwdScanMinGap, bwdScanSwitchFactor);
    }

    private HorizonJoinTimeFrameHelper(
            int maxHoleCount,
            long lookahead,
            long slaveTsScale,
            long bwdScanAbsoluteThreshold,
            long bwdScanMinGap,
            long bwdScanSwitchFactor
    ) {
        assert maxHoleCount >= 2;
        this.maxHoleCount = maxHoleCount;
        this.lookahead = lookahead;
        this.slaveTsScale = slaveTsScale;
        this.bwdScanAbsoluteThreshold = bwdScanAbsoluteThreshold;
        this.bwdScanMinGap = bwdScanMinGap;
        this.bwdScanSwitchFactor = bwdScanSwitchFactor;
    }

    /**
     * Decides whether backward scan cost justifies keeping the key map across ASOF positions.
     * Uses a relative check (cost vs gap * factor) with overflow protection via
     * {@link Math#multiplyExact}, plus an absolute threshold for cross-frame gaps
     * where the relative check can't trigger (gap encodes frame index bits, >= 2^44).
     *
     * @return true if the key map should be kept across positions
     */
    public static boolean shouldSwitchToForwardScan(
            long bwdScanCost,
            long gap,
            long bwdScanMinGap,
            long bwdScanSwitchFactor,
            long bwdScanAbsoluteThreshold
    ) {
        boolean relativeSwitch = false;
        if (gap > bwdScanMinGap) {
            try {
                relativeSwitch = bwdScanCost > Math.multiplyExact(gap, bwdScanSwitchFactor);
            } catch (ArithmeticException ignore) {
                // overflow: gap is huge, relative switch won't help
            }
        }
        return relativeSwitch || bwdScanCost > bwdScanAbsoluteThreshold;
    }

    /**
     * Finds the row with the largest timestamp less or equal to targetTimestamp (ASOF semantics).
     * Returns the row ID, or Long.MIN_VALUE if not found.
     * <p>
     * After a successful call, the helper is positioned at the found row.
     *
     * @param targetTimestamp the target timestamp to search for
     * @return rowId if found, Long.MIN_VALUE otherwise
     */
    public long findAsOfRow(long targetTimestamp) {
        if (cachedAsOfRowId != Long.MIN_VALUE && targetTimestamp < cachedNextRowTs) {
            return cachedAsOfRowId;
        }
        cachedAsOfRowId = Long.MIN_VALUE;

        // Start from bookmarked position if available
        long rowLo = Long.MIN_VALUE;

        // Track the best ASOF match found so far
        int bestFrameIndex = -1;
        long bestRowIndex = Long.MIN_VALUE;

        if (bookmarkedFrameIndex != -1) {
            timeFrameCursor.jumpTo(bookmarkedFrameIndex);
            if (timeFrameCursor.open() > 0) {
                long frameTsHi = scaleTimestamp(timeFrame.getTimestampHi() - 1, slaveTsScale); // timestampHi is exclusive
                if (frameTsHi <= targetTimestamp) {
                    // Bookmarked frame is entirely <= target, record as candidate
                    // and continue scanning forward for closer matches
                    bestFrameIndex = timeFrame.getFrameIndex();
                    bestRowIndex = timeFrame.getRowHi() - 1;
                    bookmarkCurrentFrame(0);
                } else if (scaleTimestamp(timeFrame.getTimestampLo(), slaveTsScale) <= targetTimestamp) {
                    // Target is within this frame. Validate bookmark: if the row at
                    // bookmarkedRowIndex has timestamp > target (stale bookmark from a
                    // previous page frame), fall back to searching from frame start.
                    if (bookmarkedRowIndex < timeFrame.getRowHi()) {
                        timeFrameCursor.recordAt(record, bookmarkedFrameIndex, bookmarkedRowIndex);
                        if (scaleTimestamp(record.getTimestamp(timestampIndex), slaveTsScale) <= targetTimestamp) {
                            // Fast path: if the next row's timestamp > target, the bookmark
                            // is already the ASOF answer. Avoids full linear scan.
                            final long nextRowIndex = bookmarkedRowIndex + 1;
                            if (nextRowIndex < timeFrame.getRowHi()) {
                                timeFrameCursor.recordAtRowIndex(record, nextRowIndex);
                                final long nextRowTs = scaleTimestamp(record.getTimestamp(timestampIndex), slaveTsScale);
                                if (nextRowTs > targetTimestamp) {
                                    final long result = Rows.toRowID(timeFrame.getFrameIndex(), bookmarkedRowIndex);
                                    cachedAsOfRowId = result;
                                    cachedNextRowTs = nextRowTs;
                                    return result;
                                }
                            }
                            // Slow path: bookmark is the last row in this frame or its ts <= target.
                            // Need to check if the next frame has rows <= target too,
                            // so fall through to the normal search.
                            rowLo = bookmarkedRowIndex;
                        } else {
                            rowLo = timeFrame.getRowLo();
                        }
                    } else {
                        rowLo = timeFrame.getRowLo();
                    }
                } else {
                    // Target is before bookmarked frame. Scan backward from the bookmark
                    // to find the frame containing or just before the target. This handles
                    // non-monotonic horizon timestamps across master page frames.
                    int frameIndex = bookmarkedFrameIndex;
                    while (--frameIndex >= 0) {
                        timeFrameCursor.jumpTo(frameIndex);
                        if (timeFrameCursor.open() == 0) {
                            continue;
                        }
                        final long tsLo = scaleTimestamp(timeFrame.getTimestampLo(), slaveTsScale);
                        final long tsHi = scaleTimestamp(timeFrame.getTimestampHi() - 1, slaveTsScale);
                        if (tsHi <= targetTimestamp) {
                            // Frame is entirely <= target, use as candidate
                            bestFrameIndex = timeFrame.getFrameIndex();
                            bestRowIndex = timeFrame.getRowHi() - 1;
                            bookmarkCurrentFrame(0);
                            break;
                        } else if (tsLo <= targetTimestamp) {
                            // Target is within this frame
                            rowLo = timeFrame.getRowLo();
                            break;
                        }
                        // Frame is entirely after target, keep scanning backward
                    }
                }
            }
        }

        if (rowLo == Long.MIN_VALUE) {
            // Use seekEstimate to binary-search to the target's vicinity, avoiding O(N) linear scan through all
            // preceding frames. Only seek when there was no bookmark — when we had a bookmark, the cursor is already
            // positioned at the bookmarked frame (via jumpTo), so next() will correctly continue from frame F+1.
            if (bookmarkedFrameIndex == -1) {
                final long nativeTargetTimestamp = slaveTsScale == 1 ? targetTimestamp : targetTimestamp / slaveTsScale;
                timeFrameCursor.seekEstimate(nativeTargetTimestamp);
            }
            if (timeFrame.getFrameIndex() >= 0 && timeFrameCursor.open() > 0) {
                final long frameTsHi = scaleTimestamp(timeFrame.getTimestampHi() - 1, slaveTsScale);
                if (frameTsHi <= targetTimestamp) {
                    // Seeked frame is entirely <= target, record as best candidate
                    bestFrameIndex = timeFrame.getFrameIndex();
                    bestRowIndex = timeFrame.getRowHi() - 1;
                    bookmarkCurrentFrame(0);
                } else {
                    final long frameTsLo = scaleTimestamp(timeFrame.getTimestampLo(), slaveTsScale);
                    if (frameTsLo <= targetTimestamp) {
                        // Target is within the seeked frame
                        rowLo = timeFrame.getRowLo();
                    }
                }
            }

            if (rowLo == Long.MIN_VALUE) {
                // Navigate through remaining frames to find one containing or before the target
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
        }

        // Search within the current frame for the ASOF row
        bookmarkCurrentFrame(rowLo);
        timeFrameCursor.recordAt(record, timeFrame.getFrameIndex(), timeFrame.getRowLo());

        // Try linear scan first
        long scanResult = linearScanAsOf(targetTimestamp, rowLo);
        if (scanResult >= 0) {
            return bookmarkAndCache(scanResult);
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
            return bookmarkAndCache(searchResult);
        }

        // Binary search found no rows <= target from searchStart onward.
        // The linear scan confirmed all rows in [rowLo, searchStart) were <= target,
        // so the ASOF match is the last one: searchStart - 1.
        return bookmarkAndCache(searchStart - 1);
    }

    /**
     * Keyed ASOF match: finds the last slave row at or before the ASOF position that holds the
     * master's join key. Call {@link #toTop()} before processing a new frame to reset state.
     * <p>
     * The key map holds, for every key, the last row that holds it among the slave rows that the
     * lookups have read since the current epoch started, and the helper keeps the rows up to the
     * current position that they have not read as holes: intervals of rows between two row ids,
     * both exclusive, which never overlap. Every row up to the position is either read or in a
     * hole. A lookup takes its candidate, the row of its key in the key map, and reads the rows of
     * the holes above the candidate from the top down: it puts the key of every row into the key
     * map, unless the map holds a later row for that key already, and stops at the first row that
     * holds the looked-up key. That row is the match: every row above it up to the position is
     * either a hole row that the lookup has just read, or a row read before, which can't hold the
     * key either, since the key map would hold that later row. The rows of the hole below the
     * match stay unread. When no hole row above the candidate holds the key, the candidate is the
     * match, and there is none when the key map has no row for the key.
     * <p>
     * An epoch starts at the first position after {@link #toTop()}, with an empty key map and a
     * single hole that holds every row up to the position. The lookup starts in backward-only
     * mode, where every new position starts a new epoch, so a lookup scans backward from the
     * position until it finds its key. Once the backward scans cost more than the gaps between the
     * positions, see {@link #shouldSwitchToForwardScan} and
     * {@link #shouldSwitchToForwardScanOverWindow}, the helper keeps the key map until
     * {@link #toTop()}: a new position adds the rows above the previous position as a hole, and
     * its lookups read only the rows of the holes above the candidates they need. A position below
     * the previous one starts a new epoch. The rules count the rows that the lookups read, plus
     * one for every lookup at a position that goes on below the rows already read there: a
     * backward scan that goes on from the lowest of them reads it again, and the thresholds of the
     * cairo.sql.horizon.join.bwd.scan.* properties count that row.
     * <p>
     * A lookup reads only rows between its position and its match, or its position and the
     * first row when there is no match, and each of them once. The lookups at a position
     * therefore read no more rows than backward-only mode reads there. Until the holes compact,
     * the lookups also read every row of an epoch at most once, so from the switch on they read
     * no more rows than forward scans of every gap between the positions together with the
     * backward scans below the position of the switch. A lookup never clears the key map; only a
     * new epoch does, and only when the map holds keys.
     * <p>
     * A hole takes two longs, and a position adds at most one. When the holes reach their limit,
     * {@link #MAX_HOLE_COUNT}, at least a quarter of them go, the cheapest first, see
     * {@link #compactHoles}: two neighbouring holes merge when fewer read rows separate them than
     * the smaller of them holds, and the smaller one fills otherwise, which puts its rows into the
     * key map now. A merge costs the read rows between the holes once more, when a later lookup
     * crosses them; a fill costs the rows of the hole now, which forward scans read as well,
     * unless the hole is the bottom hole of the epoch, which starts at the first row of the slave
     * and holds the rows below the deepest row that the lookups read. The compaction counts the
     * rows of the holes and of the read rows between them exactly within a frame. It doesn't open
     * the frames between the top and the bottom frame of a hole, so a hole that spans frames
     * counts only its rows in those two frames, at most the rows it holds. The bottom hole counts
     * exactly within the first frame, and as more rows than any hole holds once it spans frames,
     * so it never fills then: its frames may hold the whole history of the slave before the epoch.
     * <p>
     * Reading a native frame again costs only the rows, but the time frame cursor of a Parquet
     * slave decodes a row group again when a read returns to one that it no longer keeps, see
     * {@link TimeFrameCursor#isParquetFrame}. The compaction therefore sets apart the holes that
     * span a Parquet frame: such a hole may fill only when the positions have added it since the
     * previous compaction of the epoch and it starts in one of the few frames up to the position,
     * so that the fill stays within the frames that the lookups have just read; it only merges
     * otherwise. A hole in native frames fills whenever the costs select it, however far below the
     * position it lies. When no hole spans a Parquet frame, the compaction decides the pairs of
     * holes from the bottom up, otherwise from the top down, which fills the holes in the frames
     * that the lookups have read last first. The fills of the Parquet holes of an epoch sweep its
     * rows from the bottom up over the compactions and read each row at most once, apart from the
     * rows that a merge of the same compaction takes in, read or just filled, which a fill of the
     * merged hole reads again; and a compaction fills every Parquet hole that the costs select it
     * to fill, since one that it leaves never fills later. Two holes merge across read rows that
     * span frames only when neither of them may fill, which takes a Parquet hole, and the costs
     * select every pair.
     * <p>
     * Fills and lookups read every row of a hole once, so only merges make rows read again: a
     * lookup or a fill reads the read rows that a merge took in once more. A pair of holes that
     * may fill merges only when fewer read rows separate it, counted exactly, than either of its
     * holes counts. When the hole that each such merge takes in has taken in no read rows itself,
     * its counted rows are unread rows that back no other merge, so the merged separations hold
     * fewer rows than forward scans read, and the lookups and the fills of the holes between
     * positions read fewer than twice the rows of forward scans. A chain in which a lookup reads
     * the rows that backed one merge, which then separate the next merge, approaches that: up to
     * 1.996 times the rows of forward scans at a limit of 2 holes, and over Parquet frames at
     * limits of 2 to 4 holes. A hole that took in read rows can back a further merge, and a pair
     * of holes that may not fill merges whenever the costs select it, so its read rows may
     * outnumber the rows of its holes; this argument bounds neither. A fill of the bottom hole
     * comes on top of that: it reads rows below the epoch, which forward scans don't read, but
     * only within the first frame, at most once per epoch, and fewer of them than the hole above
     * it counts, since the smaller hole of a pair fills. A compaction selects the pairs whose
     * costs fall in the cheapest classes of costs that hold at least half of the pairs, and
     * removes at least half of the selected pairs, since a removal ends at most the next selected
     * pair; with the merges that chain, it removes from a quarter to nearly all of the holes.
     * <p>
     * At the production limit, the worst shapes measured over native frames read 1.49 times the
     * rows of forward scans with holes of 32 rows above 31 read rows, all crossed by a later
     * lookup, 1.31 times with nested merges, and 1.18 times with one position per frame, where
     * holes of 14 rows across a frame boundary merge across 6 read rows as they do within a frame.
     * A hole that spans more than two frames counts low, so it fills more readily than its rows
     * warrant: in shapes whose holes hold most of their rows in the frames between, and that no
     * later lookup crosses, the lookups and fills read up to 21 times the rows that they read when
     * such holes merge, though no more than forward scans read. Over Parquet frames, the same
     * shapes read as much, and 1.14 times with holes of 32 rows between 31 and 64 read rows in
     * turn. Where the holes of a compaction span more than a few Parquet frames, most of them may
     * only merge, also those that would cost less to fill, and a later lookup that crosses them
     * reads their read rows once more: 1.94 times the rows of forward scans with one position per
     * frame of 20 rows, holes of 1 row and 19 read rows, 1.80 times with frames of 1,000 rows,
     * holes of 9 rows and 991 read rows, 1.47 times with row groups of 100,000 rows, holes of 400
     * rows and 600 read rows, and 1.38 times with nested merges of holes of 1,024 rows about 2,800
     * rows apart, over frames of a million rows. A Parquet hole that fills spans at most three
     * frames; when it spans three, it counts low, so it may fill more readily than its rows
     * warrant, though it reads no more than forward scans read. Compared with backward-only mode,
     * the fills are the extra cost: a compaction reads them at its position on top of the lookups
     * there, up to the rows that forward scans read when no later lookup needs them, plus the rows
     * of a bottom hole within the first frame. The callers check the circuit breaker once per
     * lookup, and a lookup that compacts reads all the fills between two checks: up to 16.4
     * million rows measured, 16,384 holes of 1,000 rows in a single Parquet frame, and 8.2
     * million rows over native frames. The fills of the Parquet shapes measured decode no
     * row group again, but a lookup of a rare key that crosses old holes decodes again the row
     * groups that hold them, once per such lookup: up to 1.99 times the row group decodes of
     * forward scans measured, with one or two such lookups across the holes of a long epoch, and
     * 1.31 and 1.14 times over shorter epochs. These are measurements, not bounds.
     *
     * @param asOfRowId               the ASOF position (from {@link #findAsOfRow}), or Long.MIN_VALUE if none
     * @param masterKeyRecord         master record containing the target key
     * @param masterAsOfJoinMapSink   copier for master's join key columns
     * @param slaveAsOfJoinMapSink    copier for slave's join key columns
     * @param keyToRowIdMap           map to update with (key -> rowId) entries
     * @param symbolTranslatingRecord nullable; when non-null, used to skip scan for non-existent slave symbols
     * @return the rowId where the target key was found, or Long.MIN_VALUE if not found
     */
    public long findKeyedAsOfMatch(
            long asOfRowId,
            Record masterKeyRecord,
            RecordSink masterAsOfJoinMapSink,
            RecordSink slaveAsOfJoinMapSink,
            Map keyToRowIdMap,
            @Nullable SymbolTranslatingRecord symbolTranslatingRecord
    ) {
        if (asOfRowId == Long.MIN_VALUE) {
            return Long.MIN_VALUE;
        }

        if (asOfRowId != prevAsOfRowId) {
            if (!isKeyMapKept && prevAsOfRowId != Long.MIN_VALUE) {
                final long gap = asOfRowId - prevAsOfRowId;
                isKeyMapKept = shouldSwitchToForwardScan(
                        backwardScanRows - bwdScanRowsAtPositionStart,
                        gap,
                        bwdScanMinGap,
                        bwdScanSwitchFactor,
                        bwdScanAbsoluteThreshold
                ) || shouldSwitchToForwardScanOverWindow(gap);
            }
            if (isKeyMapKept && asOfRowId > prevAsOfRowId) {
                // The rows above the previous position stay unread until a lookup needs them.
                addHole(prevAsOfRowId, asOfRowId + 1, slaveAsOfJoinMapSink, keyToRowIdMap);
            } else {
                // A new epoch, at every position of backward-only mode and at a position below the
                // rows that the key map has read.
                if (keyToRowIdMap.size() > 0) {
                    keyToRowIdMap.clear();
                }
                holes.clear();
                holes.add(-1L, asOfRowId + 1);
                fillableHoleRowIdLo = Long.MIN_VALUE;
                bwdScanRowsAtPositionStart = backwardScanRows;
            }
            prevAsOfRowId = asOfRowId;
        }

        // Fast path: no slave row holds a symbol that the slave's symbol table lacks
        if (symbolTranslatingRecord != null && symbolTranslatingRecord.hasNonExistentKey()) {
            return Long.MIN_VALUE;
        }

        final MapKey targetKey = keyToRowIdMap.withKey();
        targetKey.put(masterKeyRecord, masterAsOfJoinMapSink);
        final MapValue targetValue = targetKey.findValue();
        final long candidateRowId = targetValue != null ? targetValue.getLong(0) : Long.MIN_VALUE;
        if (holes.size() == 0 || holes.getLast() <= candidateRowId + 1) {
            // No hole lies above the candidate.
            return candidateRowId;
        }
        if (!isKeyMapKept && holes.getLast() <= asOfRowId) {
            // The lookup goes on below the rows that the lookups at this position have read. A
            // backward scan read the lowest of them again, and the switch rules count that row.
            backwardScanRows++;
        }
        return scanHolesForKeyMatch(
                candidateRowId,
                masterKeyRecord,
                masterAsOfJoinMapSink,
                slaveAsOfJoinMapSink,
                keyToRowIdMap
        );
    }

    /**
     * Returns the rows that compactions have filled over the helper's life, which come on top of
     * the rows that the lookups read.
     */
    @TestOnly
    public long getFilledRowCount() {
        return filledRowCount;
    }

    /**
     * Returns the holes that compactions have merged into the hole below them over the helper's life.
     */
    @TestOnly
    public long getMergedHoleCount() {
        return mergedHoleCount;
    }

    public Record getRecord() {
        return record;
    }

    /**
     * Returns whether the lookups keep the key map across ASOF positions, which they do from the
     * position where the switch rules pass until {@link #toTop()}.
     */
    @TestOnly
    public boolean isKeyMapKept() {
        return isKeyMapKept;
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
        timeFrameCursor.recordAt(record, rowId);
    }

    /**
     * Reset state for processing a new master page frame.
     * <p>
     * Resets all state including bookmarks. Bookmarks are reset because workers process
     * master page frames in non-deterministic order (dispatched via ring queue). A stale
     * bookmark from a previously processed frame could point to a slave position far from
     * the current target, causing findAsOfRow() to linearly scan through O(N) slave frames
     * instead of using seekEstimate's O(log N) binary search. Resetting bookmarks forces
     * seekEstimate on the first findAsOfRow() call per frame, which is fast and eliminates
     * the jitter caused by out-of-order frame processing.
     */
    public void toTop() {
        if (timeFrameCursor != null) {
            timeFrameCursor.toTop();
        }
        bookmarkedFrameIndex = -1;
        bookmarkedRowIndex = Long.MIN_VALUE;
        bwdScanRowsAtPositionStart = 0;
        bwdScanRowsAtWindowStart = 0;
        bwdScanWindowGap = 0;
        holes.clear();
        fillableHoleRowIdLo = Long.MIN_VALUE;
        cachedAsOfRowId = Long.MIN_VALUE;
        cachedNextRowTs = Long.MIN_VALUE;
        backwardScanRows = 0;
        isKeyMapKept = false;
        prevAsOfRowId = Long.MIN_VALUE;
    }

    /**
     * Counts the read rows between two neighbouring holes, from the upper row id of the lower hole
     * to the lower row id of the upper hole, both inclusive, when both row ids lie in one frame.
     * Read rows between row ids of different frames count as {@link Long#MAX_VALUE}, more than any
     * hole holds, so the two holes never merge: the helper doesn't know how many rows of the frames
     * between them it would read again.
     */
    private static long countSeparationRows(long lowerHoleRowIdHi, long upperHoleRowIdLo) {
        if (Rows.toPartitionIndex(lowerHoleRowIdHi) == Rows.toPartitionIndex(upperHoleRowIdLo)) {
            return upperHoleRowIdLo - lowerHoleRowIdHi + 1;
        }
        return Long.MAX_VALUE;
    }

    /**
     * Adds the rows between two row ids, both exclusive, as a hole on top of the others. It
     * extends the top hole when no read row lies between the two, and compacts the holes first
     * when there are as many of them as the helper keeps.
     */
    private void addHole(long rowIdLo, long rowIdHi, RecordSink slaveAsOfJoinMapSink, Map keyToRowIdMap) {
        final int n = holes.size();
        if (n > 0 && holes.getQuick(n - 1) == rowIdLo + 1) {
            holes.setQuick(n - 1, rowIdHi);
            return;
        }
        if (n >= 2 * maxHoleCount) {
            compactHoles(slaveAsOfJoinMapSink, keyToRowIdMap);
        }
        holes.add(rowIdLo, rowIdHi);
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

    /**
     * Bookmark the result row and populate the ASOF cache for the ultra-fast path.
     * Reads the next row's timestamp to determine how long the cached result stays valid.
     */
    private long bookmarkAndCache(long resultRowIndex) {
        bookmarkCurrentFrame(resultRowIndex);
        final long result = Rows.toRowID(timeFrame.getFrameIndex(), resultRowIndex);
        final long nextRow = resultRowIndex + 1;
        if (nextRow < timeFrame.getRowHi()) {
            timeFrameCursor.recordAtRowIndex(record, nextRow);
            cachedAsOfRowId = result;
            cachedNextRowTs = scaleTimestamp(record.getTimestamp(timestampIndex), slaveTsScale);
        }
        return result;
    }

    private void bookmarkCurrentFrame(long rowIndex) {
        bookmarkedFrameIndex = timeFrame.getFrameIndex();
        bookmarkedRowIndex = rowIndex;
    }

    /**
     * Removes the holes that cost the fewest rows to remove, at least a quarter of them. Two
     * neighbouring holes, with the read rows between them, either merge into one hole, which a
     * lookup that crosses the read rows reads once more, or the smaller of the two fills now: its
     * rows go into the key map as a lookup would put them, and the hole is gone. A pair merges
     * when fewer rows separate the holes than the smaller hole holds, and fills the smaller hole
     * otherwise, the upper one when both hold as many rows. Its cost, the smaller of the two, must
     * not have more bits than the costs that allow half of the holes to go, or the pair stays
     * apart.
     * <p>
     * A hole that spans a frame of a Parquet partition, see {@link #isParquetHole}, may fill only
     * when {@link #isHoleFillable} lets it: when the positions have added it since the previous
     * compaction of the epoch, and its lower row id lies in one of the
     * {@link #UNCACHED_FRAME_DISTANCE} frames up to the frame of the position. The time frame
     * cursor of a Parquet slave keeps at most four decoded row groups, see
     * {@link io.questdb.cairo.sql.ParquetDecodeHint#MONOTONIC}, and decodes a row group again when
     * a read returns to one that it no longer keeps. findAsOfRow() has opened the frames from the
     * frame of the position up to the frame of the new position, so the cursor still keeps the
     * frame of the position and the two below it while the new position lies at most one frame
     * further up; when it lies further, those frames may be gone, and the fills there decode up to
     * three row groups again per compaction. The other Parquet holes only merge, and a pair of
     * them costs the rows between its holes. The fills of the Parquet holes of an epoch therefore
     * read each of its rows at most once, in one sweep from the bottom up over the compactions,
     * apart from the rows that a merge of the same compaction takes in, read or just filled, which
     * a fill of the merged hole reads again; and a compaction fills every Parquet hole that the
     * costs select it to fill, since one that it leaves never fills later. When the Parquet holes
     * of a compaction span more frames than that, most of them only merge, also those that would
     * cost less to fill. A hole in native frames, which the cursors read again without decoding
     * anything, fills whenever the costs select it.
     * <p>
     * When no hole spans a Parquet frame, {@link #compactHolesBottomUp} decides the pairs from the
     * bottom up; otherwise, {@link #compactHolesTopDown} decides them from the top down, which
     * fills the holes in the frames that the lookups have read last first. Each removal of either
     * pass ends at most the next pair of those the costs selected, so the pass removes at least
     * half of them.
     * <p>
     * {@link #countHoleRows} and {@link #countSeparationRows} count rows exactly within a frame,
     * where the cost of a removal is therefore exact. A hole that spans frames counts only its rows
     * in its top and bottom frames, so it may cost less to fill than it does, and read rows that
     * span frames count as more than any hole holds, so the holes around them fill or stay apart;
     * two holes that may not fill merge across such rows only when the costs select every pair.
     * The bottom hole of the epoch counts as more rows than any hole holds once it spans frames,
     * so it may merge with the hole above it, but never fills. Costs O({@link #MAX_HOLE_COUNT})
     * at most once per {@link #MAX_HOLE_COUNT} / 4 holes added, plus a format check of every frame
     * that the holes span, on top of the filled rows and of the frame opens of
     * {@link #countHoleRows}.
     * <p>
     * The pass rewrites the holes in place. A fill that throws, from a key map at its size limit
     * or a frame that fails to load, leaves them half rewritten; the callers then abandon their
     * lookups, and every walk of lookups starts with {@link #toTop()}, which clears the holes.
     */
    private void compactHoles(RecordSink slaveAsOfJoinMapSink, Map keyToRowIdMap) {
        final int n = holes.size();
        // The Parquet holes whose lower row id lies below the frames near the position only merge.
        final int fillableFrameIndexLo = Rows.toPartitionIndex(prevAsOfRowId) - UNCACHED_FRAME_DISTANCE + 1;
        if (fillableFrameIndexLo > 0) {
            fillableHoleRowIdLo = Math.max(fillableHoleRowIdLo, Rows.toRowID(fillableFrameIndexLo, 0));
        }
        for (int i = 0, k = holeCostCounts.length; i < k; i++) {
            holeCostCounts[i] = 0;
        }
        boolean hasParquetHole = isParquetHole(holes.getQuick(0), holes.getQuick(1));
        long lowerHoleSize = countFillableHoleRows(holes.getQuick(0), holes.getQuick(1), hasParquetHole);
        for (int i = 2; i < n; i += 2) {
            final long rowIdLo = holes.getQuick(i);
            final long rowIdHi = holes.getQuick(i + 1);
            final boolean isParquet = isParquetHole(rowIdLo, rowIdHi);
            hasParquetHole |= isParquet;
            final long separation = countSeparationRows(holes.getQuick(i - 1), rowIdLo);
            final long upperHoleSize = countFillableHoleRows(rowIdLo, rowIdHi, isParquet);
            // A pair of holes that may not fill costs its separation, which a merge takes in.
            final long cost = Math.min(separation, Math.min(lowerHoleSize, upperHoleSize));
            holeCostCounts[Long.SIZE - Long.numberOfLeadingZeros(cost)]++;
            lowerHoleSize = upperHoleSize;
        }
        // The fewest bits of the costs that allow half of the holes to go.
        final int removalCount = n / 2 - maxHoleCount / 2;
        int maxCostBits = 0;
        for (int removable = holeCostCounts[0]; removable < removalCount; removable += holeCostCounts[maxCostBits]) {
            maxCostBits++;
        }
        final long maxCost = (1L << maxCostBits) - 1;
        if (hasParquetHole) {
            compactHolesTopDown(maxCost, slaveAsOfJoinMapSink, keyToRowIdMap);
        } else {
            compactHolesBottomUp(maxCost, slaveAsOfJoinMapSink, keyToRowIdMap);
        }
        fillableHoleRowIdLo = prevAsOfRowId;
    }

    /**
     * The pass of {@link #compactHoles} over holes that all lie in native frames, where every hole
     * may fill. It decides every pair from the bottom up on the hole that the pairs below it left:
     * a merge right below raises the size of the lower hole, and a fill of the lower hole of a
     * pair leaves the upper one as it is. A fill of the upper hole ends the next pair, whose hole
     * stays, so that no pair counts filled rows as read rows to take in. No two holes merge across
     * read rows that span frames, which count as more rows than either hole holds.
     */
    private void compactHolesBottomUp(long maxCost, RecordSink slaveAsOfJoinMapSink, Map keyToRowIdMap) {
        final int n = holes.size();
        int out = 0;
        long outRowIdLo = holes.getQuick(0);
        long outRowIdHi = holes.getQuick(1);
        boolean isHoleBelowFilled = false;
        for (int i = 2; i < n; i += 2) {
            final long rowIdLo = holes.getQuick(i);
            final long rowIdHi = holes.getQuick(i + 1);
            if (!isHoleBelowFilled) {
                final long separation = countSeparationRows(outRowIdHi, rowIdLo);
                final long lowerSize = countHoleRows(outRowIdLo, outRowIdHi);
                final long upperSize = countHoleRows(rowIdLo, rowIdHi);
                final long fillCost = Math.min(lowerSize, upperSize);
                if (separation < fillCost) {
                    if (separation <= maxCost) {
                        outRowIdHi = rowIdHi;
                        mergedHoleCount++;
                        continue;
                    }
                } else if (fillCost <= maxCost) {
                    if (upperSize <= lowerSize) {
                        fillHole(rowIdLo, rowIdHi, slaveAsOfJoinMapSink, keyToRowIdMap);
                        isHoleBelowFilled = true;
                    } else {
                        fillHole(outRowIdLo, outRowIdHi, slaveAsOfJoinMapSink, keyToRowIdMap);
                        outRowIdLo = rowIdLo;
                        outRowIdHi = rowIdHi;
                    }
                    continue;
                }
            }
            isHoleBelowFilled = false;
            holes.setQuick(out, outRowIdLo);
            holes.setQuick(out + 1, outRowIdHi);
            out += 2;
            outRowIdLo = rowIdLo;
            outRowIdHi = rowIdHi;
        }
        holes.setQuick(out, outRowIdLo);
        holes.setQuick(out + 1, outRowIdHi);
        holes.setPos(out + 2);
    }

    /**
     * The pass of {@link #compactHoles} over holes of which some span Parquet frames. It decides
     * every pair on the hole that the pairs above it left, and so fills the holes from the top
     * down, those in the frames that the lookups have read last first: a merge right above lowers
     * the upper hole, and a fill of the upper hole of a pair leaves the lower one as it is. After a
     * fill of the lower hole, the upper hole pairs with the next hole down across the filled rows,
     * which the pair counts as read rows within a frame; when the rows between the two span frames,
     * the pair is gone and the upper hole stays, so that no pair takes in filled rows that it can't
     * count. A hole that may not fill counts as more rows than any hole holds, so a pair fills
     * only a hole that may fill, and a pair of holes of which neither may fill merges whenever the
     * costs select it. A merged hole is a Parquet hole when either of its holes is one. The pass
     * writes the holes that it keeps from the end of the list down.
     */
    private void compactHolesTopDown(long maxCost, RecordSink slaveAsOfJoinMapSink, Map keyToRowIdMap) {
        final int n = holes.size();
        int out = n - 2;
        long outRowIdLo = holes.getQuick(out);
        long outRowIdHi = holes.getQuick(out + 1);
        boolean isOutHoleParquet = isParquetHole(outRowIdLo, outRowIdHi);
        boolean isHoleBelowFilled = false;
        for (int i = n - 4; i >= 0; i -= 2) {
            final long rowIdLo = holes.getQuick(i);
            final long rowIdHi = holes.getQuick(i + 1);
            final boolean isParquet = isParquetHole(rowIdLo, rowIdHi);
            final long separation = countSeparationRows(rowIdHi, outRowIdLo);
            // Filled rows across frames between the two holes end their pair.
            final boolean isPaired = !isHoleBelowFilled || separation != Long.MAX_VALUE;
            isHoleBelowFilled = false;
            if (isPaired) {
                final long lowerSize = countFillableHoleRows(rowIdLo, rowIdHi, isParquet);
                final long upperSize = countFillableHoleRows(outRowIdLo, outRowIdHi, isOutHoleParquet);
                final long fillCost = Math.min(lowerSize, upperSize);
                // A pair of holes that may not fill merges whenever the costs select it, also
                // across read rows that span frames.
                if (separation < fillCost || fillCost == Long.MAX_VALUE) {
                    if (separation <= maxCost) {
                        // Read rows that span frames separate a merged pair only when neither hole
                        // may fill, so the upper hole is a Parquet hole already; within a frame, no
                        // frame lies between the two holes.
                        isOutHoleParquet |= isParquet;
                        outRowIdLo = rowIdLo;
                        mergedHoleCount++;
                        continue;
                    }
                } else if (fillCost <= maxCost) {
                    if (upperSize <= lowerSize) {
                        fillHole(outRowIdLo, outRowIdHi, slaveAsOfJoinMapSink, keyToRowIdMap);
                        outRowIdLo = rowIdLo;
                        outRowIdHi = rowIdHi;
                        isOutHoleParquet = isParquet;
                    } else {
                        fillHole(rowIdLo, rowIdHi, slaveAsOfJoinMapSink, keyToRowIdMap);
                        isHoleBelowFilled = true;
                    }
                    continue;
                }
            }
            holes.setQuick(out, outRowIdLo);
            holes.setQuick(out + 1, outRowIdHi);
            out -= 2;
            outRowIdLo = rowIdLo;
            outRowIdHi = rowIdHi;
            isOutHoleParquet = isParquet;
        }
        holes.setQuick(out, outRowIdLo);
        holes.setQuick(out + 1, outRowIdHi);
        holes.arrayCopy(out, 0, n - out);
        holes.setPos(n - out);
    }

    /**
     * Counts the rows of a hole as {@link #countHoleRows} does when {@link #isHoleFillable} lets
     * it fill, and as {@link Long#MAX_VALUE}, more than any hole holds, when it may only merge.
     */
    private long countFillableHoleRows(long rowIdLo, long rowIdHi, boolean isParquet) {
        return isHoleFillable(rowIdLo, isParquet) ? countHoleRows(rowIdLo, rowIdHi) : Long.MAX_VALUE;
    }

    /**
     * Counts the rows of a hole, between two row ids, both exclusive, as {@link #compactHoles}
     * prices it: every row of a hole within one frame. For a hole that spans frames, it counts
     * the rows of the hole in the frame of its upper row id and in the frame of its lower row id,
     * but not the rows of the frames between them, which it doesn't open: a lower bound. The lower
     * row id of a hole is the ASOF row of an earlier position, whose frame findAsOfRow() opened
     * since the last {@link #toTop()}, and the time frame cursors keep the timestamps of a frame
     * from its first open and its partition open, so opening that frame again reads no slave
     * memory. Time frame cursors number the rows of a frame from 0. The bottom hole of the epoch
     * starts below the first row and has no lower row id. Within the first frame, the count is
     * exact. A bottom hole that spans frames counts as {@link Long#MAX_VALUE}, more than any hole
     * holds, so that the compaction never fills it: it holds every row below the deepest row that
     * the epoch read, which forward scans don't read, and its frames may hold the whole history of
     * the slave.
     */
    private long countHoleRows(long rowIdLo, long rowIdHi) {
        final long topFrameRowCount = Rows.toLocalRowID(rowIdHi);
        if (rowIdLo < 0) {
            return Rows.toPartitionIndex(rowIdHi) == 0 ? topFrameRowCount : Long.MAX_VALUE;
        }
        final int frameIndexLo = Rows.toPartitionIndex(rowIdLo);
        final long rowIndexLo = Rows.toLocalRowID(rowIdLo);
        if (frameIndexLo == Rows.toPartitionIndex(rowIdHi)) {
            return topFrameRowCount - rowIndexLo - 1;
        }
        timeFrameCursor.jumpTo(frameIndexLo);
        timeFrameCursor.open();
        return topFrameRowCount + timeFrame.getRowHi() - rowIndexLo - 1;
    }

    /**
     * Reads every row of a hole and puts its key into the key map, unless the map holds a later
     * row for the key, as {@link #scanHolesForKeyMatch} does for the rows it reads.
     */
    private void fillHole(long rowIdLo, long rowIdHi, RecordSink slaveAsOfJoinMapSink, Map keyToRowIdMap) {
        final int frameIndexLo = rowIdLo < 0 ? 0 : Rows.toPartitionIndex(rowIdLo);
        final long rowIndexBelowLo = rowIdLo < 0 ? -1 : Rows.toLocalRowID(rowIdLo);
        long rowIndexHi = Rows.toLocalRowID(rowIdHi) - 1;
        for (int frameIndex = Rows.toPartitionIndex(rowIdHi); frameIndex >= frameIndexLo; frameIndex--, rowIndexHi = Long.MAX_VALUE) {
            timeFrameCursor.jumpTo(frameIndex);
            if (timeFrameCursor.open() == 0) {
                continue;
            }
            final long rowIndexLo = frameIndex == frameIndexLo
                    ? Math.max(timeFrame.getRowLo(), rowIndexBelowLo + 1)
                    : timeFrame.getRowLo();
            final long frameRowIndexHi = Math.min(rowIndexHi, timeFrame.getRowHi() - 1);
            if (frameRowIndexHi < rowIndexLo) {
                continue;
            }
            timeFrameCursor.recordAt(record, frameIndex, frameRowIndexHi);
            final long frameRowId = Rows.toRowID(frameIndex, 0);
            for (long rowIndex = frameRowIndexHi; rowIndex >= rowIndexLo; rowIndex--) {
                timeFrameCursor.recordAtRowIndex(record, rowIndex);
                final long rowId = frameRowId + rowIndex;
                final MapKey slaveKey = keyToRowIdMap.withKey();
                slaveKey.put(record, slaveAsOfJoinMapSink);
                final MapValue value = slaveKey.createValue();
                if (value.isNew() || value.getLong(0) < rowId) {
                    value.putLong(0, rowId);
                }
            }
            filledRowCount += frameRowIndexHi - rowIndexLo + 1;
        }
    }

    /**
     * Tells whether {@link #compactHoles} may fill the hole with the given lower row id, which
     * {@link #isParquetHole} tells to span a Parquet frame or not. A hole in native frames may
     * fill. A Parquet hole may fill only when its lower row id lies at or above the position of
     * the previous compaction of the epoch, if there was one, and in one of the
     * {@link #UNCACHED_FRAME_DISTANCE} frames up to the frame of the position of the compaction.
     * A Parquet bottom hole, whose lower row id lies below the first row, meets both only at the
     * first compaction of the epoch, and only when these frames include the first frame. The
     * other Parquet holes may only merge.
     */
    private boolean isHoleFillable(long holeRowIdLo, boolean isParquet) {
        return !isParquet || holeRowIdLo >= fillableHoleRowIdLo;
    }

    /**
     * Tells whether a fill of the hole between two row ids, both exclusive, may read a frame of a
     * Parquet partition: whether any frame from the frame of its lower row id to the frame of its
     * upper row id is one. The bottom hole of the epoch never fills once it spans frames, see
     * {@link #countHoleRows}, so it counts as a native hole then, whatever its frames.
     */
    private boolean isParquetHole(long rowIdLo, long rowIdHi) {
        final int frameIndexHi = Rows.toPartitionIndex(rowIdHi);
        if (rowIdLo < 0) {
            return frameIndexHi == 0 && timeFrameCursor.isParquetFrame(0);
        }
        for (int frameIndex = Rows.toPartitionIndex(rowIdLo); frameIndex <= frameIndexHi; frameIndex++) {
            if (timeFrameCursor.isParquetFrame(frameIndex)) {
                return true;
            }
        }
        return false;
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

    /**
     * Reads the rows of one frame from rowIndexHi down to rowIndexLo, both inclusive, as
     * {@link #scanHolesForKeyMatch} describes.
     *
     * @return the local row id of the first row that holds the master's key, or -1
     */
    private long scanFrameRowsForKeyMatch(
            int frameIndex,
            long rowIndexHi,
            long rowIndexLo,
            long masterHash,
            Record masterKeyRecord,
            RecordSink masterAsOfJoinMapSink,
            RecordSink slaveAsOfJoinMapSink,
            Map keyToRowIdMap
    ) {
        timeFrameCursor.recordAt(record, frameIndex, rowIndexHi);
        final long frameRowId = Rows.toRowID(frameIndex, 0);
        for (long rowIndex = rowIndexHi; rowIndex >= rowIndexLo; rowIndex--) {
            timeFrameCursor.recordAtRowIndex(record, rowIndex);
            final long rowId = frameRowId + rowIndex;
            final MapKey slaveKey = keyToRowIdMap.withKey();
            slaveKey.put(record, slaveAsOfJoinMapSink);
            slaveKey.commit();
            final long slaveHash = slaveKey.hash();
            final MapValue value = slaveKey.createValue(slaveHash);
            if (value.isNew() || value.getLong(0) < rowId) {
                value.putLong(0, rowId);
                // Fast path: only check for master key match when hashes match. The map held
                // an earlier row for the master's key, the candidate, so the key matches when
                // the map now holds this row for it.
                if (slaveHash == masterHash) {
                    final MapKey targetKey = keyToRowIdMap.withKey();
                    targetKey.put(masterKeyRecord, masterAsOfJoinMapSink);
                    final MapValue targetValue = targetKey.findValue();
                    if (targetValue != null && targetValue.getLong(0) == rowId) {
                        backwardScanRows += rowIndexHi - rowIndex + 1;
                        return rowIndex;
                    }
                }
            }
        }
        backwardScanRows += rowIndexHi - rowIndexLo + 1;
        return -1;
    }

    /**
     * Reads the holes above the candidate from the top down, puts the key of every row it reads
     * into the key map unless the map holds a later row for the key, and stops at the first row
     * that holds the master's key. Shrinks the top hole to the rows below the last row it read,
     * and drops the holes it has read up.
     *
     * @return the match: the row it stopped at, or the candidate when no hole row above it holds
     * the key
     */
    private long scanHolesForKeyMatch(
            long candidateRowId,
            Record masterKeyRecord,
            RecordSink masterAsOfJoinMapSink,
            RecordSink slaveAsOfJoinMapSink,
            Map keyToRowIdMap
    ) {
        // Pre-compute master key hash once (avoids re-hashing on every iteration)
        final MapKey masterKey = keyToRowIdMap.withKey();
        masterKey.put(masterKeyRecord, masterAsOfJoinMapSink);
        masterKey.commit();
        final long masterHash = masterKey.hash();

        int top = holes.size() - 2;
        do {
            final long holeRowIdLo = holes.getQuick(top);
            final long holeRowIdHi = holes.getQuick(top + 1);
            // A merged hole may hold the candidate; the rows up to it are read.
            final long scanRowIdLo = Math.max(holeRowIdLo, candidateRowId);
            // The lowest frame to read and the local row id below its lowest row to read.
            final int frameIndexLo = scanRowIdLo < 0 ? 0 : Rows.toPartitionIndex(scanRowIdLo);
            final long rowIndexBelowLo = scanRowIdLo < 0 ? -1 : Rows.toLocalRowID(scanRowIdLo);
            int frameIndex = Rows.toPartitionIndex(holeRowIdHi);
            long rowIndexHi = Rows.toLocalRowID(holeRowIdHi) - 1;
            long matchRowId = Long.MIN_VALUE;
            for (; frameIndex >= frameIndexLo; frameIndex--, rowIndexHi = Long.MAX_VALUE) {
                timeFrameCursor.jumpTo(frameIndex);
                if (timeFrameCursor.open() == 0) {
                    continue;
                }
                final long rowIndexLo = frameIndex == frameIndexLo
                        ? Math.max(timeFrame.getRowLo(), rowIndexBelowLo + 1)
                        : timeFrame.getRowLo();
                final long frameRowIndexHi = Math.min(rowIndexHi, timeFrame.getRowHi() - 1);
                if (frameRowIndexHi < rowIndexLo) {
                    continue;
                }
                final long matchRowIndex = scanFrameRowsForKeyMatch(
                        frameIndex,
                        frameRowIndexHi,
                        rowIndexLo,
                        masterHash,
                        masterKeyRecord,
                        masterAsOfJoinMapSink,
                        slaveAsOfJoinMapSink,
                        keyToRowIdMap
                );
                if (matchRowIndex >= 0) {
                    matchRowId = Rows.toRowID(frameIndex, matchRowIndex);
                    break;
                }
            }

            if (matchRowId != Long.MIN_VALUE) {
                // The rows below the match stay unread.
                shrinkTopHole(top, holeRowIdLo, matchRowId);
                return matchRowId;
            }
            if (scanRowIdLo > holeRowIdLo) {
                // The scan reached the candidate inside a merged hole.
                shrinkTopHole(top, holeRowIdLo, candidateRowId);
                return candidateRowId;
            }
            holes.setPos(top);
            top -= 2;
        } while (top >= 0 && holes.getQuick(top + 1) > candidateRowId + 1);
        return candidateRowId;
    }

    /**
     * Applies the relative switch check to a run of ASOF positions that sit at most the min gap
     * apart. No single gap of such a run passes the min gap test of
     * {@link #shouldSwitchToForwardScan}, so backward-only mode would re-scan for the key at every
     * position however long the run is. The gaps and the backward scan cost at the positions they
     * connect accumulate into a window instead. Once the window covers more than the min gap, the
     * check compares the window's cost with the rows a forward scan would have read over the same
     * distance, and the next window starts.
     *
     * @param gap distance in rows from the previous ASOF position to the current one
     * @return true if the key map should be kept across positions
     */
    private boolean shouldSwitchToForwardScanOverWindow(long gap) {
        boolean isSwitch = false;
        boolean isWindowOpen = gap > 0 && gap <= bwdScanMinGap;
        if (isWindowOpen) {
            bwdScanWindowGap += gap;
            if (bwdScanWindowGap > bwdScanMinGap) {
                isSwitch = shouldSwitchToForwardScan(
                        backwardScanRows - bwdScanRowsAtWindowStart,
                        bwdScanWindowGap,
                        bwdScanMinGap,
                        bwdScanSwitchFactor,
                        Long.MAX_VALUE
                );
                isWindowOpen = false;
            }
        }
        if (!isWindowOpen) {
            // The window is spent, or the gap is above the min gap and had its own check; a
            // cross-frame gap is far above it. Either way, a new window starts at the current position.
            bwdScanWindowGap = 0;
            bwdScanRowsAtWindowStart = backwardScanRows;
        }
        return isSwitch;
    }

    private void shrinkTopHole(int top, long rowIdLo, long rowIdHi) {
        if (rowIdHi > rowIdLo + 1) {
            holes.setQuick(top + 1, rowIdHi);
        } else {
            holes.setPos(top);
        }
    }
}

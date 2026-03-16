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

package io.questdb.cairo;

import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Vect;

/**
 * Computes the merge strategy between Parquet row groups and incoming O3 (out-of-order) data.
 * <p>
 * This class determines which row groups (or slices thereof) need to be merged with O3 data,
 * which can be copied as-is, and which O3 data ranges don't overlap with any row group.
 * <p>
 * The output supports row group splitting by allowing partial row group ranges.
 */
public class O3ParquetMergeStrategy {

    /**
     * Default threshold for small row groups that should be merged with adjacent O3 data
     * even when there's no timestamp overlap.
     */
    public static final int DEFAULT_SMALL_ROW_GROUP_THRESHOLD = 4096;
    /**
     * Input descriptor for a single row group's timestamp bounds.
     * Stored in a LongList as consecutive triples: (min, max, rowCount).
     */
    public static final int ROW_GROUP_ENTRY_SIZE = 3;
    public static final int ROW_GROUP_MAX_OFFSET = 1;
    public static final int ROW_GROUP_MIN_OFFSET = 0;
    public static final int ROW_GROUP_ROW_COUNT_OFFSET = 2;

    /**
     * Helper to add a row group entry to the bounds list.
     */
    public static void addRowGroupBounds(LongList rowGroupBounds, long min, long max, long rowCount) {
        rowGroupBounds.add(min);
        rowGroupBounds.add(max);
        rowGroupBounds.add(rowCount);
    }

    /**
     * Computes the merge strategy between existing row groups and incoming O3 data.
     * <p>
     * The algorithm uses both min and max timestamps to detect true overlap:
     * <ul>
     * <li>O3 data that overlaps with a row group (o3Ts &gt;= rgMin AND o3Ts &lt;= rgMax) is merged</li>
     * <li>O3 data in gaps between row groups creates new row groups (COPY_O3)</li>
     * <li>Exception: if an adjacent row group is "small" (&lt; smallRowGroupThreshold rows),
     * the O3 data is merged into that row group instead of creating a new one</li>
     * </ul>
     * <p>
     * Row group bounds are provided as triples (min, max, rowCount) in the rowGroupBounds list.
     * Use {@link #addRowGroupBounds} to populate the list.
     *
     * @param rowGroupBounds         List of (min, max, rowCount) triples for each row group.
     * @param sortedTimestampsAddr   Native address of sorted O3 timestamps (16 bytes per entry).
     * @param srcOooLo               Start index of O3 data range (inclusive).
     * @param srcOooHi               End index of O3 data range (inclusive).
     * @param smallRowGroupThreshold Row groups with fewer rows than this are considered "small"
     *                               and will have adjacent non-overlapping O3 data merged into them.
     * @param maxRowGroupSize        Maximum number of rows per COPY_O3 action. Larger ranges are split
     *                               into multiple actions of at most this size. Use Integer.MAX_VALUE
     *                               to disable splitting.
     * @param actionsBuf             Pre-allocated buffer to receive computed merge actions (reused across calls).
     *                               Objects in the buffer beyond the returned count are stale and must not be read.
     * @param rgO3Ranges             Pre-allocated scratch list for per-row-group O3 ranges (reused across calls).
     * @param gapO3Ranges            Pre-allocated scratch list for per-gap O3 ranges (reused across calls).
     * @return the number of actions written into actionsBuf
     */
    public static int computeMergeActions(
            LongList rowGroupBounds,
            long sortedTimestampsAddr,
            long srcOooLo,
            long srcOooHi,
            int smallRowGroupThreshold,
            int maxRowGroupSize,
            ObjList<MergeAction> actionsBuf,
            LongList rgO3Ranges,
            LongList gapO3Ranges
    ) {
        int actionCount = 0;

        final int rowGroupCount = getRowGroupCount(rowGroupBounds);
        if (rowGroupCount == 0) {
            // No existing row groups - all O3 data becomes new row groups
            if (srcOooLo <= srcOooHi) {
                actionCount = addCopyO3Actions(actionsBuf, actionCount, srcOooLo, srcOooHi, maxRowGroupSize, smallRowGroupThreshold);
            }
            return actionCount;
        }

        // Track O3 data assigned to each row group as interleaved (lo, hi) pairs,
        // initialized to -1 meaning "no O3 data".
        final int rgPairCount = rowGroupCount * 2;
        rgO3Ranges.clear();
        rgO3Ranges.setPos(rgPairCount);
        rgO3Ranges.fill(0, rgPairCount, -1);

        // Collect gap regions: O3 data that doesn't overlap with any row group.
        // Gap i = data before row group i; gap rowGroupCount = data after the last row group.
        final int gapPairCount = (rowGroupCount + 1) * 2;
        gapO3Ranges.clear();
        gapO3Ranges.setPos(gapPairCount);
        gapO3Ranges.fill(0, gapPairCount, -1);

        // Assign each O3 data point to either a row group or a gap
        long o3Cursor = srcOooLo;
        int currentRg = 0;

        while (o3Cursor <= srcOooHi && currentRg < rowGroupCount) {
            final long rgMin = getRowGroupMin(rowGroupBounds, currentRg);
            final long rgMax = getRowGroupMax(rowGroupBounds, currentRg);
            final long o3Ts = TableWriter.getTimestampIndexValue(sortedTimestampsAddr, o3Cursor);

            if (o3Ts < rgMin) {
                // O3 data is before this row group - it's in the gap before currentRg
                // Find all O3 data before rgMin
                long gapHi = Vect.boundedBinarySearchIndexT(
                        sortedTimestampsAddr,
                        rgMin - 1,
                        o3Cursor,
                        srcOooHi,
                        Vect.BIN_SEARCH_SCAN_DOWN
                );
                if (gapHi >= o3Cursor) {
                    setRangeLo(gapO3Ranges, currentRg, o3Cursor);
                    setRangeHi(gapO3Ranges, currentRg, gapHi);
                    o3Cursor = gapHi + 1;
                }
            } else if (o3Ts <= rgMax) {
                // O3 data overlaps with this row group
                // Find all O3 data that overlaps (timestamps <= rgMax)
                long overlapHi = Vect.boundedBinarySearchIndexT(
                        sortedTimestampsAddr,
                        rgMax,
                        o3Cursor,
                        srcOooHi,
                        Vect.BIN_SEARCH_SCAN_DOWN
                );
                if (overlapHi >= o3Cursor) {
                    setRangeLo(rgO3Ranges, currentRg, o3Cursor);
                    setRangeHi(rgO3Ranges, currentRg, overlapHi);
                    o3Cursor = overlapHi + 1;
                }
                currentRg++;
            } else {
                // O3 timestamp is after this row group's max, move to next row group
                currentRg++;
            }
        }

        // Any remaining O3 data goes to the gap after the last row group
        if (o3Cursor <= srcOooHi) {
            setRangeLo(gapO3Ranges, rowGroupCount, o3Cursor);
            setRangeHi(gapO3Ranges, rowGroupCount, srcOooHi);
        }

        // Now merge gap O3 data into adjacent small row groups
        for (int gap = 0; gap <= rowGroupCount; gap++) {
            if (getRangeLo(gapO3Ranges, gap) < 0) {
                continue; // No O3 data in this gap
            }

            // Check adjacent row groups for small size
            int prevRg = gap - 1;
            boolean prevIsSmall = prevRg >= 0 && getRowGroupRowCount(rowGroupBounds, prevRg) < smallRowGroupThreshold;
            boolean nextIsSmall = gap < rowGroupCount && getRowGroupRowCount(rowGroupBounds, gap) < smallRowGroupThreshold;

            if (prevIsSmall) {
                // Merge gap data into previous row group
                if (getRangeLo(rgO3Ranges, prevRg) < 0) {
                    setRangeLo(rgO3Ranges, prevRg, getRangeLo(gapO3Ranges, gap));
                    setRangeHi(rgO3Ranges, prevRg, getRangeHi(gapO3Ranges, gap));
                } else {
                    // Extend existing O3 range (gap data comes after)
                    setRangeHi(rgO3Ranges, prevRg, getRangeHi(gapO3Ranges, gap));
                }
                setRangeLo(gapO3Ranges, gap, -1);
                setRangeHi(gapO3Ranges, gap, -1);
            } else if (nextIsSmall) {
                // Merge gap data into next row group
                if (getRangeLo(rgO3Ranges, gap) < 0) {
                    setRangeLo(rgO3Ranges, gap, getRangeLo(gapO3Ranges, gap));
                    setRangeHi(rgO3Ranges, gap, getRangeHi(gapO3Ranges, gap));
                } else {
                    // Gap data comes before existing O3 range
                    setRangeLo(rgO3Ranges, gap, getRangeLo(gapO3Ranges, gap));
                }
                setRangeLo(gapO3Ranges, gap, -1);
                setRangeHi(gapO3Ranges, gap, -1);
            }
            // Otherwise, gap data remains as COPY_O3
        }

        // Generate actions in timestamp order
        for (int rg = 0; rg < rowGroupCount; rg++) {
            // First, emit any COPY_O3 for gap before this row group
            if (getRangeLo(gapO3Ranges, rg) >= 0) {
                actionCount = addCopyO3Actions(actionsBuf, actionCount, getRangeLo(gapO3Ranges, rg), getRangeHi(gapO3Ranges, rg), maxRowGroupSize, smallRowGroupThreshold);
            }

            // Then, emit action for this row group
            long rgRowCount = getRowGroupRowCount(rowGroupBounds, rg);
            if (getRangeLo(rgO3Ranges, rg) >= 0) {
                nextAction(actionsBuf, actionCount++).setMerge(rg, 0, rgRowCount - 1, getRangeLo(rgO3Ranges, rg), getRangeHi(rgO3Ranges, rg));
            } else {
                nextAction(actionsBuf, actionCount++).setCopyRowGroupSlice(rg, 0, rgRowCount - 1);
            }
        }

        // Finally, emit any COPY_O3 for gap after last row group
        if (getRangeLo(gapO3Ranges, rowGroupCount) >= 0) {
            actionCount = addCopyO3Actions(actionsBuf, actionCount, getRangeLo(gapO3Ranges, rowGroupCount), getRangeHi(gapO3Ranges, rowGroupCount), maxRowGroupSize, smallRowGroupThreshold);
        }

        return actionCount;
    }

    /**
     * Emits one or more COPY_O3 actions, splitting the range into chunks of
     * maxRowGroupSize rows. If the last chunk would be smaller than
     * smallRowGroupThreshold, it absorbs the remainder into the previous chunk
     * (up to 1.5x maxRowGroupSize).
     *
     * @return the updated actionCount
     */
    private static int addCopyO3Actions(ObjList<MergeAction> actionsBuf, int actionCount, long o3Lo, long o3Hi, int maxRowGroupSize, int smallRowGroupThreshold) {
        assert maxRowGroupSize > 0 : "maxRowGroupSize must be > 0";
        long cursor = o3Lo;
        while (cursor <= o3Hi) {
            long remaining = o3Hi - cursor + 1;
            if (remaining <= maxRowGroupSize) {
                // Last (or only) chunk: emit whatever remains
                nextAction(actionsBuf, actionCount++).setCopyO3(cursor, o3Hi);
                break;
            }
            long afterChunk = remaining - maxRowGroupSize;
            if (afterChunk < smallRowGroupThreshold && afterChunk + maxRowGroupSize <= maxRowGroupSize * 3L / 2) {
                // Remainder after this chunk would be below the small row group
                // threshold. Absorb it to avoid producing an undersized row group.
                nextAction(actionsBuf, actionCount++).setCopyO3(cursor, o3Hi);
                break;
            }
            long chunkEnd = cursor + maxRowGroupSize - 1;
            nextAction(actionsBuf, actionCount++).setCopyO3(cursor, chunkEnd);
            cursor = chunkEnd + 1;
        }
        return actionCount;
    }

    private static long getRangeHi(LongList ranges, int index) {
        return ranges.getQuick(index * 2 + 1);
    }

    private static long getRangeLo(LongList ranges, int index) {
        return ranges.getQuick(index * 2);
    }

    /**
     * Returns an existing or newly created MergeAction at the given index.
     * The buffer only grows, never shrinks — callers use the returned action
     * count to know which entries are valid.
     */
    private static MergeAction nextAction(ObjList<MergeAction> actionsBuf, int index) {
        if (index < actionsBuf.size()) {
            MergeAction existing = actionsBuf.getQuick(index);
            existing.clear();
            return existing;
        }
        MergeAction action = new MergeAction();
        actionsBuf.add(action);
        return action;
    }

    private static void setRangeHi(LongList ranges, int index, long value) {
        ranges.setQuick(index * 2 + 1, value);
    }

    private static void setRangeLo(LongList ranges, int index, long value) {
        ranges.setQuick(index * 2, value);
    }

    /**
     * Returns the number of row groups in the bounds list.
     */
    public static int getRowGroupCount(LongList rowGroupBounds) {
        return rowGroupBounds.size() / ROW_GROUP_ENTRY_SIZE;
    }

    /**
     * Helper to get row group max timestamp from bounds list.
     */
    public static long getRowGroupMax(LongList rowGroupBounds, int rowGroupIndex) {
        return rowGroupBounds.get(rowGroupIndex * ROW_GROUP_ENTRY_SIZE + ROW_GROUP_MAX_OFFSET);
    }

    /**
     * Helper to get row group min timestamp from bounds list.
     */
    public static long getRowGroupMin(LongList rowGroupBounds, int rowGroupIndex) {
        return rowGroupBounds.get(rowGroupIndex * ROW_GROUP_ENTRY_SIZE + ROW_GROUP_MIN_OFFSET);
    }

    /**
     * Helper to get row group row count from bounds list.
     */
    public static long getRowGroupRowCount(LongList rowGroupBounds, int rowGroupIndex) {
        return rowGroupBounds.get(rowGroupIndex * ROW_GROUP_ENTRY_SIZE + ROW_GROUP_ROW_COUNT_OFFSET);
    }

    /**
     * Action type for merge strategy output.
     */
    public enum ActionType {
        /**
         * Merge a row group slice with overlapping O3 data.
         * rowGroupIndex, rgLo, rgHi, o3Lo, o3Hi are all valid.
         */
        MERGE,

        /**
         * Copy a row group slice as-is (no overlapping O3 data).
         * rowGroupIndex, rgLo, rgHi are valid; o3Lo/o3Hi are -1.
         */
        COPY_ROW_GROUP_SLICE,

        /**
         * Copy O3 data as a new row group (no overlapping row group).
         * o3Lo/o3Hi are valid; rowGroupIndex is -1, rgLo/rgHi are -1.
         */
        COPY_O3
    }

    /**
     * Represents a single merge action in the computed strategy.
     * <p>
     * Supports partial row group operations via rgLo/rgHi fields which specify
     * the row range within the row group (0-based, inclusive).
     */
    public static class MergeAction {
        public long o3Hi;          // O3 data index (inclusive), -1 if COPY_ROW_GROUP_SLICE
        public long o3Lo;          // O3 data index (inclusive), -1 if COPY_ROW_GROUP_SLICE
        public long rgHi;          // row index within row group (inclusive), -1 if COPY_O3
        public long rgLo;          // row index within row group (inclusive), -1 if COPY_O3
        public int rowGroupIndex;  // -1 if COPY_O3
        public ActionType type;

        public MergeAction() {
            clear();
        }

        public void clear() {
            this.type = null;
            this.rowGroupIndex = -1;
            this.rgLo = -1;
            this.rgHi = -1;
            this.o3Lo = -1;
            this.o3Hi = -1;
        }

        /**
         * Returns the number of rows from O3 data (0 if COPY_ROW_GROUP_SLICE).
         */
        public long getO3RowCount() {
            return o3Hi >= 0 ? o3Hi - o3Lo + 1 : 0;
        }

        /**
         * Returns the number of rows from the row group slice (0 if COPY_O3).
         */
        public long getRowGroupRowCount() {
            return rgHi >= 0 ? rgHi - rgLo + 1 : 0;
        }

        /**
         * Returns the total number of rows this action will produce.
         */
        public long getTotalRowCount() {
            return getRowGroupRowCount() + getO3RowCount();
        }

        /**
         * Set this action to copy O3 data as a new row group.
         *
         * @param o3Lo start index in O3 data (inclusive)
         * @param o3Hi end index in O3 data (inclusive)
         */
        public void setCopyO3(long o3Lo, long o3Hi) {
            this.type = ActionType.COPY_O3;
            this.rowGroupIndex = -1;
            this.rgLo = -1;
            this.rgHi = -1;
            this.o3Lo = o3Lo;
            this.o3Hi = o3Hi;
        }

        /**
         * Set this action to copy a row group slice without merge.
         *
         * @param rowGroupIndex the row group index
         * @param rgLo          start row within the row group (inclusive)
         * @param rgHi          end row within the row group (inclusive)
         */
        public void setCopyRowGroupSlice(int rowGroupIndex, long rgLo, long rgHi) {
            this.type = ActionType.COPY_ROW_GROUP_SLICE;
            this.rowGroupIndex = rowGroupIndex;
            this.rgLo = rgLo;
            this.rgHi = rgHi;
            this.o3Lo = -1;
            this.o3Hi = -1;
        }

        /**
         * Set this action to merge a row group slice with O3 data.
         *
         * @param rowGroupIndex the row group index
         * @param rgLo          start row within the row group (inclusive)
         * @param rgHi          end row within the row group (inclusive)
         * @param o3Lo          start index in O3 data (inclusive)
         * @param o3Hi          end index in O3 data (inclusive)
         */
        public void setMerge(int rowGroupIndex, long rgLo, long rgHi, long o3Lo, long o3Hi) {
            this.type = ActionType.MERGE;
            this.rowGroupIndex = rowGroupIndex;
            this.rgLo = rgLo;
            this.rgHi = rgHi;
            this.o3Lo = o3Lo;
            this.o3Hi = o3Hi;
        }

        @Override
        public String toString() {
            return switch (type) {
                case MERGE ->
                        "MERGE(rg=" + rowGroupIndex + "[" + rgLo + "," + rgHi + "], o3=[" + o3Lo + "," + o3Hi + "])";
                case COPY_ROW_GROUP_SLICE ->
                        "COPY_ROW_GROUP_SLICE(rg=" + rowGroupIndex + "[" + rgLo + "," + rgHi + "])";
                case COPY_O3 -> "COPY_O3(o3=[" + o3Lo + "," + o3Hi + "])";
            };
        }
    }
}

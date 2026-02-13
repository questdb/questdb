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
     * Computes the merge strategy with default small row group threshold.
     */
    public static void computeMergeActions(
            LongList rowGroupBounds,
            long sortedTimestampsAddr,
            long srcOooLo,
            long srcOooHi,
            ObjList<MergeAction> actions
    ) {
        computeMergeActions(
                rowGroupBounds,
                sortedTimestampsAddr,
                srcOooLo,
                srcOooHi,
                DEFAULT_SMALL_ROW_GROUP_THRESHOLD,
                actions
        );
    }

    /**
     * Computes the merge strategy between existing row groups and incoming O3 data.
     * <p>
     * The algorithm uses both min and max timestamps to detect true overlap:
     * - O3 data that overlaps with a row group (o3Ts >= rgMin AND o3Ts <= rgMax) is merged
     * - O3 data in gaps between row groups creates new row groups (COPY_O3)
     * - Exception: if an adjacent row group is "small" (< smallRowGroupThreshold rows),
     * the O3 data is merged into that row group instead of creating a new one
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
     * @param actions                Output list to receive computed merge actions. Will be cleared first.
     */
    public static void computeMergeActions(
            LongList rowGroupBounds,
            long sortedTimestampsAddr,
            long srcOooLo,
            long srcOooHi,
            int smallRowGroupThreshold,
            ObjList<MergeAction> actions
    ) {
        actions.clear();

        final int rowGroupCount = getRowGroupCount(rowGroupBounds);
        if (rowGroupCount == 0) {
            // No existing row groups - all O3 data becomes a new row group
            if (srcOooLo <= srcOooHi) {
                MergeAction action = new MergeAction();
                action.setCopyO3(srcOooLo, srcOooHi);
                actions.add(action);
            }
            return;
        }

        // Track O3 data assigned to each row group: [o3Lo, o3Hi] or [-1, -1] if none
        // Also track "gap" O3 data that falls between row groups
        long[] rgO3Lo = new long[rowGroupCount];
        long[] rgO3Hi = new long[rowGroupCount];
        for (int i = 0; i < rowGroupCount; i++) {
            rgO3Lo[i] = -1;
            rgO3Hi[i] = -1;
        }

        // Collect gap regions: O3 data that doesn't overlap with any row group
        // gapO3[i] = O3 data in gap before row group i (gap 0 = before rg0, gap N = after rgN-1)
        long[] gapO3Lo = new long[rowGroupCount + 1];
        long[] gapO3Hi = new long[rowGroupCount + 1];
        for (int i = 0; i <= rowGroupCount; i++) {
            gapO3Lo[i] = -1;
            gapO3Hi[i] = -1;
        }

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
                        rgMin,
                        o3Cursor,
                        srcOooHi,
                        Vect.BIN_SEARCH_SCAN_DOWN
                );
                if (gapHi >= o3Cursor) {
                    gapO3Lo[currentRg] = o3Cursor;
                    gapO3Hi[currentRg] = gapHi;
                    o3Cursor = gapHi + 1;
                }
            } else if (o3Ts <= rgMax) {
                // O3 data overlaps with this row group
                // Find all O3 data that overlaps (timestamps <= rgMax)
                long overlapHi = Vect.boundedBinarySearchIndexT(
                        sortedTimestampsAddr,
                        rgMax + 1,
                        o3Cursor,
                        srcOooHi,
                        Vect.BIN_SEARCH_SCAN_DOWN
                );
                if (overlapHi >= o3Cursor) {
                    rgO3Lo[currentRg] = o3Cursor;
                    rgO3Hi[currentRg] = overlapHi;
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
            gapO3Lo[rowGroupCount] = o3Cursor;
            gapO3Hi[rowGroupCount] = srcOooHi;
        }

        // Now merge gap O3 data into adjacent small row groups
        for (int gap = 0; gap <= rowGroupCount; gap++) {
            if (gapO3Lo[gap] < 0) {
                continue; // No O3 data in this gap
            }

            // Check adjacent row groups for small size
            int prevRg = gap - 1;
            boolean prevIsSmall = prevRg >= 0 && getRowGroupRowCount(rowGroupBounds, prevRg) < smallRowGroupThreshold;
            boolean nextIsSmall = gap < rowGroupCount && getRowGroupRowCount(rowGroupBounds, gap) < smallRowGroupThreshold;

            if (prevIsSmall) {
                // Merge gap data into previous row group
                if (rgO3Lo[prevRg] < 0) {
                    rgO3Lo[prevRg] = gapO3Lo[gap];
                    rgO3Hi[prevRg] = gapO3Hi[gap];
                } else {
                    // Extend existing O3 range (gap data comes after)
                    rgO3Hi[prevRg] = gapO3Hi[gap];
                }
                gapO3Lo[gap] = -1;
                gapO3Hi[gap] = -1;
            } else if (nextIsSmall) {
                // Merge gap data into next row group
                if (rgO3Lo[gap] < 0) {
                    rgO3Lo[gap] = gapO3Lo[gap];
                    rgO3Hi[gap] = gapO3Hi[gap];
                } else {
                    // Gap data comes before existing O3 range
                    rgO3Lo[gap] = gapO3Lo[gap];
                }
                gapO3Lo[gap] = -1;
                gapO3Hi[gap] = -1;
            }
            // Otherwise, gap data remains as COPY_O3
        }

        // Generate actions in timestamp order
        for (int rg = 0; rg < rowGroupCount; rg++) {
            // First, emit any COPY_O3 for gap before this row group
            if (gapO3Lo[rg] >= 0) {
                MergeAction action = new MergeAction();
                action.setCopyO3(gapO3Lo[rg], gapO3Hi[rg]);
                actions.add(action);
            }

            // Then, emit action for this row group
            long rgRowCount = getRowGroupRowCount(rowGroupBounds, rg);
            if (rgO3Lo[rg] >= 0) {
                MergeAction action = new MergeAction();
                action.setMerge(rg, 0, rgRowCount - 1, rgO3Lo[rg], rgO3Hi[rg]);
                actions.add(action);
            } else {
                MergeAction action = new MergeAction();
                action.setCopyRowGroupSlice(rg, 0, rgRowCount - 1);
                actions.add(action);
            }
        }

        // Finally, emit any COPY_O3 for gap after last row group
        if (gapO3Lo[rowGroupCount] >= 0) {
            MergeAction action = new MergeAction();
            action.setCopyO3(gapO3Lo[rowGroupCount], gapO3Hi[rowGroupCount]);
            actions.add(action);
        }
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

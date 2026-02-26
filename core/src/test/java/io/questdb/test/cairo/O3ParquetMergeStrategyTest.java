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

package io.questdb.test.cairo;

import io.questdb.cairo.O3ParquetMergeStrategy;
import io.questdb.cairo.O3ParquetMergeStrategy.ActionType;
import io.questdb.cairo.O3ParquetMergeStrategy.MergeAction;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class O3ParquetMergeStrategyTest extends AbstractCairoTest {

    @Test
    public void testCustomSmallRowGroupThreshold() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            // Row group 0: min=100, max=200, rowCount=5000
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 5000);
            // Row group 1: min=400, max=500, rowCount=10000
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 400, 500, 10_000);

            ObjList<MergeAction> actions = new ObjList<>();

            // O3 data in gap
            long sortedTimestampsAddr = allocateSortedTimestamps(300);
            try {
                // With threshold=4096, rg0 (5000 rows) is NOT small -> COPY_O3
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        0, 0,
                        4096,
                        actions
                );

                Assert.assertEquals(3, actions.size());
                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(0).type);
                Assert.assertEquals(ActionType.COPY_O3, actions.get(1).type);
                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(2).type);

                // With threshold=6000, rg0 (5000 rows) IS small -> MERGE
                actions.clear();
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        0, 0,
                        6000,
                        actions
                );

                Assert.assertEquals(2, actions.size());
                Assert.assertEquals(ActionType.MERGE, actions.get(0).type);
                Assert.assertEquals(0, actions.get(0).rowGroupIndex);
                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(1).type);
            } finally {
                freeSortedTimestamps(sortedTimestampsAddr, 1);
            }
        });
    }

    @Test
    public void testMaxRowGroupSizeSplitting() throws Exception {
        assertMemoryLeak(() -> {
            ObjList<MergeAction> actions = new ObjList<>();
            LongList rowGroupBounds = new LongList();
            LongList rgO3Ranges = new LongList();
            LongList gapO3Ranges = new LongList();

            // --- Case 1: totalRows <= maxRowGroupSize -> single COPY_O3, no split ---
            // 10 O3 rows with maxRowGroupSize=10 -> fits in 1 chunk
            long addr = allocateSortedTimestamps(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            try {
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, 9,
                        0, 10, actions, rgO3Ranges, gapO3Ranges
                );
                Assert.assertEquals(1, actions.size());
                Assert.assertEquals(ActionType.COPY_O3, actions.get(0).type);
                Assert.assertEquals(0, actions.get(0).o3Lo);
                Assert.assertEquals(9, actions.get(0).o3Hi);
            } finally {
                freeSortedTimestamps(addr, 10);
            }

            // --- Case 2: totalRows = maxRowGroupSize + 1 -> 2 chunks: full + remainder of 1 ---
            long[] ts11 = new long[11];
            for (int i = 0; i < 11; i++) {
                ts11[i] = i + 1;
            }
            addr = allocateSortedTimestamps(ts11);
            try {
                actions.clear();
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, 10,
                        0, 10, actions, rgO3Ranges, gapO3Ranges
                );
                Assert.assertEquals(2, actions.size());
                Assert.assertEquals(ActionType.COPY_O3, actions.get(0).type);
                Assert.assertEquals(ActionType.COPY_O3, actions.get(1).type);
                Assert.assertEquals(10, actions.get(0).o3Hi - actions.get(0).o3Lo + 1);
                Assert.assertEquals(1, actions.get(1).o3Hi - actions.get(1).o3Lo + 1);
                // Contiguous
                Assert.assertEquals(0, actions.get(0).o3Lo);
                Assert.assertEquals(actions.get(0).o3Hi + 1, actions.get(1).o3Lo);
                Assert.assertEquals(10, actions.get(1).o3Hi);
            } finally {
                freeSortedTimestamps(addr, 11);
            }

            // --- Case 3: totalRows = 3x -> 3 chunks of exactly maxRowGroupSize ---
            long[] ts30 = new long[30];
            for (int i = 0; i < 30; i++) {
                ts30[i] = i + 1;
            }
            addr = allocateSortedTimestamps(ts30);
            try {
                actions.clear();
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, 29,
                        0, 10, actions, rgO3Ranges, gapO3Ranges
                );
                Assert.assertEquals(3, actions.size());
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(ActionType.COPY_O3, actions.get(i).type);
                    Assert.assertEquals(10, actions.get(i).o3Hi - actions.get(i).o3Lo + 1);
                }
            } finally {
                freeSortedTimestamps(addr, 30);
            }

            // --- Case 4: totalRows = 3x + 7 -> 3 full chunks + remainder of 7 ---
            // e.g. 1.07M rows with 100K max -> 10 full + 70K remainder
            long[] ts37 = new long[37];
            for (int i = 0; i < 37; i++) {
                ts37[i] = i + 1;
            }
            addr = allocateSortedTimestamps(ts37);
            try {
                actions.clear();
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, 36,
                        0, 10, actions, rgO3Ranges, gapO3Ranges
                );
                Assert.assertEquals(4, actions.size());
                // First 3 chunks are exactly maxRowGroupSize
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(ActionType.COPY_O3, actions.get(i).type);
                    Assert.assertEquals(10, actions.get(i).o3Hi - actions.get(i).o3Lo + 1);
                }
                // Last chunk is the remainder
                Assert.assertEquals(ActionType.COPY_O3, actions.get(3).type);
                Assert.assertEquals(7, actions.get(3).o3Hi - actions.get(3).o3Lo + 1);
            } finally {
                freeSortedTimestamps(addr, 37);
            }

            // --- Case 5: single row -> 1 chunk of 1 row ---
            addr = allocateSortedTimestamps(42);
            try {
                actions.clear();
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, 0,
                        0, 10, actions, rgO3Ranges, gapO3Ranges
                );
                Assert.assertEquals(1, actions.size());
                Assert.assertEquals(ActionType.COPY_O3, actions.get(0).type);
                Assert.assertEquals(0, actions.get(0).o3Lo);
                Assert.assertEquals(0, actions.get(0).o3Hi);
            } finally {
                freeSortedTimestamps(addr, 1);
            }

            // --- Case 6: splitting in gap between existing row groups ---
            rowGroupBounds.clear();
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 1, 10, 10_000);
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 110, 10_000);
            // 25 O3 rows in the gap [50..74], maxRowGroupSize=10 -> 2 full + remainder of 5
            long[] tsGap = new long[25];
            for (int i = 0; i < 25; i++) {
                tsGap[i] = 50 + i;
            }
            addr = allocateSortedTimestamps(tsGap);
            try {
                actions.clear();
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, 24,
                        0, 10, actions, rgO3Ranges, gapO3Ranges
                );
                // Expected: COPY_ROW_GROUP_SLICE(rg0), COPY_O3(10), COPY_O3(10), COPY_O3(5), COPY_ROW_GROUP_SLICE(rg1)
                Assert.assertEquals(5, actions.size());
                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(0).type);
                Assert.assertEquals(ActionType.COPY_O3, actions.get(1).type);
                Assert.assertEquals(ActionType.COPY_O3, actions.get(2).type);
                Assert.assertEquals(ActionType.COPY_O3, actions.get(3).type);
                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(4).type);
                Assert.assertEquals(10, actions.get(1).o3Hi - actions.get(1).o3Lo + 1);
                Assert.assertEquals(10, actions.get(2).o3Hi - actions.get(2).o3Lo + 1);
                Assert.assertEquals(5, actions.get(3).o3Hi - actions.get(3).o3Lo + 1);
            } finally {
                freeSortedTimestamps(addr, 25);
            }

            // --- Case 7: 1_010_000 rows, maxRgSize=100K, threshold=25K ---
            // Remainder of 10K < 25K threshold -> absorbed into last chunk: 9x100K + 110K
            int totalRows = 1_010_000;
            int maxRgSize = 100_000;
            int smallRgThreshold = maxRgSize / 4; // 25_000
            rowGroupBounds.clear();
            long[] tsLarge = new long[totalRows];
            for (int i = 0; i < totalRows; i++) {
                tsLarge[i] = i + 1;
            }
            addr = allocateSortedTimestamps(tsLarge);
            try {
                actions.clear();
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, totalRows - 1,
                        smallRgThreshold, maxRgSize, actions, rgO3Ranges, gapO3Ranges
                );
                Assert.assertEquals(10, actions.size());
                for (int i = 0; i < 9; i++) {
                    Assert.assertEquals(ActionType.COPY_O3, actions.get(i).type);
                    Assert.assertEquals(maxRgSize, actions.get(i).o3Hi - actions.get(i).o3Lo + 1);
                }
                // Last chunk absorbed the 10K remainder: 100K + 10K = 110K
                Assert.assertEquals(ActionType.COPY_O3, actions.get(9).type);
                Assert.assertEquals(110_000, actions.get(9).o3Hi - actions.get(9).o3Lo + 1);
            } finally {
                freeSortedTimestamps(addr, totalRows);
            }

            // --- Case 8: 1_070_000 rows, remainder >= threshold -> no absorption ---
            // Remainder of 70K >= 25K threshold -> kept separate: 10x100K + 70K
            totalRows = 1_070_000;
            long[] tsLarge2 = new long[totalRows];
            for (int i = 0; i < totalRows; i++) {
                tsLarge2[i] = i + 1;
            }
            addr = allocateSortedTimestamps(tsLarge2);
            try {
                actions.clear();
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, totalRows - 1,
                        smallRgThreshold, maxRgSize, actions, rgO3Ranges, gapO3Ranges
                );
                Assert.assertEquals(11, actions.size());
                for (int i = 0; i < 10; i++) {
                    Assert.assertEquals(ActionType.COPY_O3, actions.get(i).type);
                    Assert.assertEquals(maxRgSize, actions.get(i).o3Hi - actions.get(i).o3Lo + 1);
                }
                // Last chunk is 70K, above threshold, not absorbed
                Assert.assertEquals(ActionType.COPY_O3, actions.get(10).type);
                Assert.assertEquals(70_000, actions.get(10).o3Hi - actions.get(10).o3Lo + 1);
            } finally {
                freeSortedTimestamps(addr, totalRows);
            }
        });
    }

    @Test
    public void testMaxRowGroupSizeSplittingFuzz() throws Exception {
        assertMemoryLeak(() -> {
            ObjList<MergeAction> actions = new ObjList<>();
            LongList rowGroupBounds = new LongList();
            LongList rgO3Ranges = new LongList();
            LongList gapO3Ranges = new LongList();

            int maxRgSize = 100_000;
            int smallRgThreshold = maxRgSize / 4;
            java.util.Random rnd = new java.util.Random(42);

            int totalRows = 1 + rnd.nextInt(2_000_000);
            rowGroupBounds.clear();
            long[] ts = new long[totalRows];
            for (int i = 0; i < totalRows; i++) {
                ts[i] = i + 1;
            }
            long addr = allocateSortedTimestamps(ts);
            try {
                actions.clear();
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, totalRows - 1,
                        smallRgThreshold, maxRgSize, actions, rgO3Ranges, gapO3Ranges
                );

                // Verify total row count matches
                long totalEmitted = 0;
                int nonExactCount = 0;
                for (int i = 0; i < actions.size(); i++) {
                    MergeAction a = actions.get(i);
                    Assert.assertEquals(ActionType.COPY_O3, a.type);
                    long chunkSize = a.o3Hi - a.o3Lo + 1;
                    totalEmitted += chunkSize;

                    if (chunkSize != maxRgSize) {
                        nonExactCount++;
                    }

                    if (actions.size() > 1) {
                        // Multiple chunks: each must be > threshold/4 and <= 1.5x
                        Assert.assertTrue(
                                "chunk " + i + " size " + chunkSize + " < " + smallRgThreshold
                                        + " (totalRows=" + totalRows + ")",
                                chunkSize >= smallRgThreshold
                        );
                        Assert.assertTrue(
                                "chunk " + i + " size " + chunkSize + " > 1.5x " + maxRgSize
                                        + " (totalRows=" + totalRows + ")",
                                chunkSize <= maxRgSize * 3L / 2
                        );
                    }
                }

                Assert.assertEquals("total rows mismatch", totalRows, totalEmitted);

                // At most 1 row group can differ from maxRowGroupSize
                Assert.assertTrue(
                        "more than 1 non-exact chunk: " + nonExactCount
                                + " (totalRows=" + totalRows + ")",
                        nonExactCount <= 1
                );
            } finally {
                freeSortedTimestamps(addr, totalRows);
            }
        });
    }

    @Test
    public void testMergeActionHelpers() {
        MergeAction action = new MergeAction();

        action.setMerge(0, 10, 99, 5, 14);
        Assert.assertEquals(90, action.getRowGroupRowCount());
        Assert.assertEquals(10, action.getO3RowCount());
        Assert.assertEquals(100, action.getTotalRowCount());

        action.setCopyRowGroupSlice(1, 0, 49);
        Assert.assertEquals(50, action.getRowGroupRowCount());
        Assert.assertEquals(0, action.getO3RowCount());
        Assert.assertEquals(50, action.getTotalRowCount());

        action.setCopyO3(100, 199);
        Assert.assertEquals(0, action.getRowGroupRowCount());
        Assert.assertEquals(100, action.getO3RowCount());
        Assert.assertEquals(100, action.getTotalRowCount());
    }

    @Test
    public void testMixedOverlapAndGap() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            // Row group 0: min=100, max=200, rowCount=10000 (large)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 10_000);
            // Row group 1: min=400, max=500, rowCount=10000 (large)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 400, 500, 10_000);
            // Row group 2: min=700, max=800, rowCount=10000 (large)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 700, 800, 10_000);

            ObjList<MergeAction> actions = new ObjList<>();

            // O3 data: [150] overlaps rg0, [300] in gap, [450] overlaps rg1, [600] in gap, [750] overlaps rg2
            long sortedTimestampsAddr = allocateSortedTimestamps(150, 300, 450, 600, 750);
            try {
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        0, 4,
                        actions
                );

                // Expected:
                // MERGE(rg0, o3[0])
                // COPY_O3(o3[1]) - gap between rg0 and rg1
                // MERGE(rg1, o3[2])
                // COPY_O3(o3[3]) - gap between rg1 and rg2
                // MERGE(rg2, o3[4])
                Assert.assertEquals(5, actions.size());

                Assert.assertEquals(ActionType.MERGE, actions.get(0).type);
                Assert.assertEquals(0, actions.get(0).rowGroupIndex);
                Assert.assertEquals(0, actions.get(0).o3Lo);
                Assert.assertEquals(0, actions.get(0).o3Hi);

                Assert.assertEquals(ActionType.COPY_O3, actions.get(1).type);
                Assert.assertEquals(1, actions.get(1).o3Lo);
                Assert.assertEquals(1, actions.get(1).o3Hi);

                Assert.assertEquals(ActionType.MERGE, actions.get(2).type);
                Assert.assertEquals(1, actions.get(2).rowGroupIndex);
                Assert.assertEquals(2, actions.get(2).o3Lo);
                Assert.assertEquals(2, actions.get(2).o3Hi);

                Assert.assertEquals(ActionType.COPY_O3, actions.get(3).type);
                Assert.assertEquals(3, actions.get(3).o3Lo);
                Assert.assertEquals(3, actions.get(3).o3Hi);

                Assert.assertEquals(ActionType.MERGE, actions.get(4).type);
                Assert.assertEquals(2, actions.get(4).rowGroupIndex);
                Assert.assertEquals(4, actions.get(4).o3Lo);
                Assert.assertEquals(4, actions.get(4).o3Hi);
            } finally {
                freeSortedTimestamps(sortedTimestampsAddr, 5);
            }
        });
    }

    @Test
    public void testNoRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            ObjList<MergeAction> actions = new ObjList<>();

            // O3 data with timestamps [100, 200, 300]
            long sortedTimestampsAddr = allocateSortedTimestamps(100, 200, 300);
            try {
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        0, 2,
                        actions
                );

                Assert.assertEquals(1, actions.size());
                MergeAction action = actions.get(0);
                Assert.assertEquals(ActionType.COPY_O3, action.type);
                Assert.assertEquals(0, action.o3Lo);
                Assert.assertEquals(2, action.o3Hi);
            } finally {
                freeSortedTimestamps(sortedTimestampsAddr, 3);
            }
        });
    }

    @Test
    public void testO3AfterAllRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            // Row group 0: min=100, max=200, rowCount=10000 (large)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 10_000);

            ObjList<MergeAction> actions = new ObjList<>();

            // O3 data with timestamps [500, 600, 700] - all after rg0
            long sortedTimestampsAddr = allocateSortedTimestamps(500, 600, 700);
            try {
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        0, 2,
                        actions
                );

                // Should produce: COPY_ROW_GROUP_SLICE(rg0), COPY_O3
                Assert.assertEquals(2, actions.size());

                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(0).type);
                Assert.assertEquals(0, actions.get(0).rowGroupIndex);

                Assert.assertEquals(ActionType.COPY_O3, actions.get(1).type);
                Assert.assertEquals(0, actions.get(1).o3Lo);
                Assert.assertEquals(2, actions.get(1).o3Hi);
            } finally {
                freeSortedTimestamps(sortedTimestampsAddr, 3);
            }
        });
    }

    @Test
    public void testO3BeforeAllRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            // Row group 0: min=500, max=600, rowCount=10000 (large)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 500, 600, 10_000);

            ObjList<MergeAction> actions = new ObjList<>();

            // O3 data with timestamps [100, 200, 300] - all before rg0
            long sortedTimestampsAddr = allocateSortedTimestamps(100, 200, 300);
            try {
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        0, 2,
                        actions
                );

                // Should produce: COPY_O3, COPY_ROW_GROUP_SLICE(rg0)
                Assert.assertEquals(2, actions.size());

                Assert.assertEquals(ActionType.COPY_O3, actions.get(0).type);
                Assert.assertEquals(0, actions.get(0).o3Lo);
                Assert.assertEquals(2, actions.get(0).o3Hi);

                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(1).type);
                Assert.assertEquals(0, actions.get(1).rowGroupIndex);
            } finally {
                freeSortedTimestamps(sortedTimestampsAddr, 3);
            }
        });
    }

    @Test
    public void testO3InGapBetweenLargeRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            // Row group 0: min=100, max=200, rowCount=10000 (large)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 10_000);
            // Row group 1: min=400, max=500, rowCount=10000 (large)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 400, 500, 10_000);

            ObjList<MergeAction> actions = new ObjList<>();

            // O3 data with timestamps [250, 300, 350] - in gap between row groups
            long sortedTimestampsAddr = allocateSortedTimestamps(250, 300, 350);
            try {
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        0, 2,
                        actions
                );

                // Should produce: COPY_ROW_GROUP_SLICE(rg0), COPY_O3, COPY_ROW_GROUP_SLICE(rg1)
                Assert.assertEquals(3, actions.size());

                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(0).type);
                Assert.assertEquals(0, actions.get(0).rowGroupIndex);

                Assert.assertEquals(ActionType.COPY_O3, actions.get(1).type);
                Assert.assertEquals(0, actions.get(1).o3Lo);
                Assert.assertEquals(2, actions.get(1).o3Hi);

                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(2).type);
                Assert.assertEquals(1, actions.get(2).rowGroupIndex);
            } finally {
                freeSortedTimestamps(sortedTimestampsAddr, 3);
            }
        });
    }

    @Test
    public void testO3InGapMergedIntoSmallNextRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            // Row group 0: min=100, max=200, rowCount=10000 (large)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 10_000);
            // Row group 1: min=400, max=500, rowCount=100 (small, < 4096)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 400, 500, 100);

            ObjList<MergeAction> actions = new ObjList<>();

            // O3 data with timestamps [250, 300, 350] - in gap, rg1 is small
            long sortedTimestampsAddr = allocateSortedTimestamps(250, 300, 350);
            try {
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        0, 2,
                        actions
                );

                // Gap O3 should be merged into small rg1
                Assert.assertEquals(2, actions.size());

                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(0).type);
                Assert.assertEquals(0, actions.get(0).rowGroupIndex);

                Assert.assertEquals(ActionType.MERGE, actions.get(1).type);
                Assert.assertEquals(1, actions.get(1).rowGroupIndex);
                Assert.assertEquals(0, actions.get(1).o3Lo);
                Assert.assertEquals(2, actions.get(1).o3Hi);
            } finally {
                freeSortedTimestamps(sortedTimestampsAddr, 3);
            }
        });
    }

    @Test
    public void testO3InGapMergedIntoSmallPreviousRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            // Row group 0: min=100, max=200, rowCount=100 (small, < 4096)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 100);
            // Row group 1: min=400, max=500, rowCount=10000 (large)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 400, 500, 10_000);

            ObjList<MergeAction> actions = new ObjList<>();

            // O3 data with timestamps [250, 300, 350] - in gap, but rg0 is small
            long sortedTimestampsAddr = allocateSortedTimestamps(250, 300, 350);
            try {
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        0, 2,
                        actions
                );

                // Gap O3 should be merged into small rg0
                Assert.assertEquals(2, actions.size());

                Assert.assertEquals(ActionType.MERGE, actions.get(0).type);
                Assert.assertEquals(0, actions.get(0).rowGroupIndex);
                Assert.assertEquals(0, actions.get(0).o3Lo);
                Assert.assertEquals(2, actions.get(0).o3Hi);

                Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actions.get(1).type);
                Assert.assertEquals(1, actions.get(1).rowGroupIndex);
            } finally {
                freeSortedTimestamps(sortedTimestampsAddr, 3);
            }
        });
    }

    @Test
    public void testRowGroupBoundsHelpers() {
        LongList bounds = new LongList();
        O3ParquetMergeStrategy.addRowGroupBounds(bounds, 100, 200, 50);
        O3ParquetMergeStrategy.addRowGroupBounds(bounds, 300, 400, 75);

        Assert.assertEquals(2, O3ParquetMergeStrategy.getRowGroupCount(bounds));

        Assert.assertEquals(100, O3ParquetMergeStrategy.getRowGroupMin(bounds, 0));
        Assert.assertEquals(200, O3ParquetMergeStrategy.getRowGroupMax(bounds, 0));
        Assert.assertEquals(50, O3ParquetMergeStrategy.getRowGroupRowCount(bounds, 0));

        Assert.assertEquals(300, O3ParquetMergeStrategy.getRowGroupMin(bounds, 1));
        Assert.assertEquals(400, O3ParquetMergeStrategy.getRowGroupMax(bounds, 1));
        Assert.assertEquals(75, O3ParquetMergeStrategy.getRowGroupRowCount(bounds, 1));
    }

    @Test
    public void testSingleRowGroupWithOverlap() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            // Row group 0: min=100, max=500, rowCount=10000 (large)
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 500, 10_000);

            ObjList<MergeAction> actions = new ObjList<>();

            // O3 data with timestamps [150, 250, 350] - all within [100, 500]
            long sortedTimestampsAddr = allocateSortedTimestamps(150, 250, 350);
            try {
                O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        0, 2,
                        actions
                );

                Assert.assertEquals(1, actions.size());
                MergeAction action = actions.get(0);
                Assert.assertEquals(ActionType.MERGE, action.type);
                Assert.assertEquals(0, action.rowGroupIndex);
                Assert.assertEquals(0, action.rgLo);
                Assert.assertEquals(9999, action.rgHi);
                Assert.assertEquals(0, action.o3Lo);
                Assert.assertEquals(2, action.o3Hi);
            } finally {
                freeSortedTimestamps(sortedTimestampsAddr, 3);
            }
        });
    }

    /**
     * Allocates a native memory buffer with sorted timestamps in O3 format.
     * Each entry is 16 bytes: 8 bytes timestamp + 8 bytes original index.
     */
    private long allocateSortedTimestamps(long... timestamps) {
        long addr = Unsafe.malloc(timestamps.length * 16L, MemoryTag.NATIVE_O3);
        for (int i = 0; i < timestamps.length; i++) {
            Unsafe.getUnsafe().putLong(addr + i * 16L, timestamps[i]);
            Unsafe.getUnsafe().putLong(addr + i * 16L + 8, i);
        }
        return addr;
    }

    private void freeSortedTimestamps(long addr, int count) {
        Unsafe.free(addr, count * 16L, MemoryTag.NATIVE_O3);
    }
}

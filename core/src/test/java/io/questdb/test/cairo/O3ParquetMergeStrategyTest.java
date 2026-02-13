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
    public void testCustomSmallRowGroupThreshold() {
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
    public void testMixedOverlapAndGap() {
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
    }

    @Test
    public void testNoRowGroups() {
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
    }

    @Test
    public void testO3AfterAllRowGroups() {
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
    }

    @Test
    public void testO3BeforeAllRowGroups() {
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
    }

    @Test
    public void testO3InGapBetweenLargeRowGroups() {
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
    }

    @Test
    public void testO3InGapMergedIntoSmallNextRowGroup() {
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
    }

    @Test
    public void testO3InGapMergedIntoSmallPreviousRowGroup() {
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
    public void testSingleRowGroupWithOverlap() {
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

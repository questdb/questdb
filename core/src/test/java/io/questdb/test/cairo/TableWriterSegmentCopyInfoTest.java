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

package io.questdb.test.cairo;

import io.questdb.cairo.TableWriterSegmentCopyInfo;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableWriterSegmentCopyInfoTest {
    // The longs of a list entry and the initial capacity of the lists, see TableWriterSegmentCopyInfo
    private static final long INITIAL_LIST_LONGS = 4 + 4 + 3 + 7 + 5 + 4 + 2;
    private static final long ITEM_LONGS = 5;
    private static final long ROWS_PER_TXN = 64;
    private static final long RUN_LONGS = 7;
    private static final long RUN_ORDER_LONGS = 2;
    private static final long SEGMENT_LONGS = 4;
    private static final long SORT_PLAN_TXN_LONGS = 1;
    private static final int TXN_COUNT = 2_000;
    private static final long TXN_LONGS = 4;
    private static final long TXN_META_LONGS = 3;
    private static final int TXNS_PER_SEGMENT = 2;

    @Test
    public void testResetCapacityOnFreshInstance() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long memBase = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
            try (TableWriterSegmentCopyInfo copyInfo = new TableWriterSegmentCopyInfo()) {
                final long memFresh = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
                Assert.assertTrue(memFresh > memBase);

                copyInfo.resetCapacity();
                Assert.assertEquals(memFresh, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER));
                assertEmpty(copyInfo);

                // the lists stay usable
                addBlock(copyInfo, 4);
                Assert.assertTrue(copyInfo.buildSortPlan());
                Assert.assertEquals(4, copyInfo.getSortPlanItemCount());
            }
            Assert.assertEquals(memBase, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER));
        });
    }

    @Test
    public void testResetCapacityReleasesBlockLists() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long memBase = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
            try (TableWriterSegmentCopyInfo copyInfo = new TableWriterSegmentCopyInfo()) {
                final long memFresh = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);

                addBlock(copyInfo, TXN_COUNT);
                Assert.assertTrue(copyInfo.buildSortPlan());
                // every transaction is a run and a plan item of its own, the block fills all the lists
                Assert.assertEquals(TXN_COUNT, copyInfo.getTxnCount());
                Assert.assertEquals(TXN_COUNT / TXNS_PER_SEGMENT, copyInfo.getSegmentCount());
                Assert.assertEquals(TXN_COUNT, copyInfo.getSortPlanItemCount());
                Assert.assertEquals(TXN_COUNT, copyInfo.getSortPlanTxnCount());
                Assert.assertEquals(TXN_COUNT / 2 * ROWS_PER_TXN, copyInfo.getSortPlanCopyRows());
                final long filledLongs = TXN_COUNT * (TXN_LONGS + TXN_META_LONGS + RUN_LONGS + ITEM_LONGS + SORT_PLAN_TXN_LONGS + RUN_ORDER_LONGS)
                        + TXN_COUNT / TXNS_PER_SEGMENT * SEGMENT_LONGS;
                final long memGrown = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
                Assert.assertTrue(
                        "lists did not grow [fresh=" + memFresh + ", grown=" + memGrown + ']',
                        memGrown - memFresh >= (filledLongs - INITIAL_LIST_LONGS) * Long.BYTES
                );
                final LongList items = readLongs(copyInfo.getSortPlanItemsAddress(), copyInfo.getSortPlanItemCount() * ITEM_LONGS);
                final LongList planTxns = readLongs(copyInfo.getSortPlanTxnsAddress(), copyInfo.getSortPlanTxnCount());

                copyInfo.resetCapacity();
                Assert.assertEquals(memFresh, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER));
                assertEmpty(copyInfo);

                // the lists grow again and the plan of the same block is the same
                addBlock(copyInfo, TXN_COUNT);
                Assert.assertTrue(copyInfo.buildSortPlan());
                Assert.assertEquals(TXN_COUNT, copyInfo.getTxnCount());
                Assert.assertEquals(TXN_COUNT / TXNS_PER_SEGMENT, copyInfo.getSegmentCount());
                Assert.assertEquals(TXN_COUNT / 2 * ROWS_PER_TXN, copyInfo.getSortPlanCopyRows());
                TestUtils.assertEquals(items, readLongs(copyInfo.getSortPlanItemsAddress(), copyInfo.getSortPlanItemCount() * ITEM_LONGS));
                TestUtils.assertEquals(planTxns, readLongs(copyInfo.getSortPlanTxnsAddress(), copyInfo.getSortPlanTxnCount()));
                Assert.assertEquals(memGrown, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER));

                copyInfo.resetCapacity();
                Assert.assertEquals(memFresh, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER));
            }
            // close frees the lists after the reset
            Assert.assertEquals(memBase, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER));
        });
    }

    // Adds sorted and unsorted transactions of ROWS_PER_TXN rows in turn, in descending time order and without
    // overlaps. The plan copies the sorted ones and sorts the unsorted ones, every transaction is a plan item.
    private static void addBlock(TableWriterSegmentCopyInfo copyInfo, int txnCount) {
        final long step = 10;
        final long txnSpan = ROWS_PER_TXN * step;
        for (int s = 0, segmentCount = txnCount / TXNS_PER_SEGMENT; s < segmentCount; s++) {
            for (int i = 0; i < TXNS_PER_SEGMENT; i++) {
                final int t = s * TXNS_PER_SEGMENT + i;
                final long minTs = (txnCount - t) * txnSpan;
                copyInfo.addTxn(i * ROWS_PER_TXN, t, ROWS_PER_TXN, s, minTs, minTs + txnSpan - step, t % 2 == 0);
            }
            copyInfo.addSegment(1, s, 0, TXNS_PER_SEGMENT * ROWS_PER_TXN, false);
        }
    }

    private static void assertEmpty(TableWriterSegmentCopyInfo copyInfo) {
        Assert.assertEquals(0, copyInfo.getTxnCount());
        Assert.assertEquals(0, copyInfo.getSegmentCount());
        Assert.assertEquals(0, copyInfo.getSortPlanItemCount());
        Assert.assertEquals(0, copyInfo.getSortPlanTxnCount());
        Assert.assertEquals(0, copyInfo.getSortPlanCopyRows());
        Assert.assertEquals(0, copyInfo.getTotalRows());
    }

    private static LongList readLongs(long address, long count) {
        final LongList values = new LongList();
        for (long i = 0; i < count; i++) {
            values.add(Unsafe.getLong(address + i * Long.BYTES));
        }
        return values;
    }
}

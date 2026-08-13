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

package io.questdb.test.cairo.o3;

import io.questdb.cairo.O3CompositeMergeStrategy;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * The planner is pure - piece bounds and sorted O3 timestamps in, an action list out - so it needs no
 * table, no writer and no files. Every case here is a shape the composite dispatch has to get right.
 */
public class O3CompositeMergeStrategyTest {

    @Test
    public void testApplyCutDeclinesWhereItWouldSaveNothing() {
        // A cut at or below the floor, above the last row, or on a piece whose data is unbounded, leaves
        // the list untouched - the caller applies cuts blind and reads the answer off the list.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 100);
        Assert.assertFalse(O3CompositeMergeStrategy.applyCut(bounds, 0, 100));
        Assert.assertFalse(O3CompositeMergeStrategy.applyCut(bounds, 0, 99));
        Assert.assertFalse(O3CompositeMergeStrategy.applyCut(bounds, 0, 200));
        Assert.assertEquals("P0(tsLo=100,tsHi=199,rows=100)", formatBounds(bounds));

        final LongList unbounded = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(unbounded, 100, Numbers.LONG_NULL, 0, 100);
        Assert.assertFalse(O3CompositeMergeStrategy.applyCut(unbounded, 0, 150));
    }

    @Test
    public void testApplyCutSplitsAPieceInTwo() {
        // Both halves address the same files at the same offsets, so this is the whole of what a pre-split
        // does to the geometry. Rows are apportioned by timestamp position, the same estimate computeCuts
        // decided on.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 0, 999, 0, 1000);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 1000, 1999, 0, 500);
        Assert.assertTrue(O3CompositeMergeStrategy.applyCut(bounds, 0, 400));
        Assert.assertEquals(
                "P0(tsLo=0,tsHi=399,rows=400) P1(tsLo=400,tsHi=999,rows=600) P2(tsLo=1000,tsHi=1999,rows=500)",
                formatBounds(bounds)
        );
    }

    @Test
    public void testACutTimestampOutsideEveryPieceIsDeclined() {
        // Clustering proposes cuts from the shape of the incoming work, so a cut can name a timestamp no
        // piece holds. It is dropped, not guessed at.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 100);
        Assert.assertFalse(O3CompositeMergeStrategy.applyCutAt(bounds, 50));
        Assert.assertFalse(O3CompositeMergeStrategy.applyCutAt(bounds, 500));
        Assert.assertEquals("P0(tsLo=100,tsHi=199,rows=100)", formatBounds(bounds));
    }

    @Test
    public void testClusteringCutsThenBatchCutsThenDecisions() {
        // The whole decision pipeline on one partition that arrives as a SINGLE piece covering a day.
        //
        // Transaction clustering has looked at the incoming block and found the work is dense in two
        // strides with a cold gap between them, so it asks for cuts at that gap's edges. The batch then
        // lands inside the first stride, and its own edges refine that piece further. What comes out is a
        // decision per piece: the cold gap and the untouched tail are KEPT - not copied, not read - and
        // only the sliver the batch actually overlaps is MERGED.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 0, 999, 0, 1000);

        final LongList clusterCuts = new LongList();
        clusterCuts.add(300);  // start of the cold gap
        clusterCuts.add(700);  // end of the cold gap
        for (int i = 0, n = clusterCuts.size(); i < n; i++) {
            Assert.assertTrue(O3CompositeMergeStrategy.applyCutAt(bounds, clusterCuts.getQuick(i)));
        }
        Assert.assertEquals(
                "P0(tsLo=0,tsHi=299,rows=300) P1(tsLo=300,tsHi=699,rows=400) P2(tsLo=700,tsHi=999,rows=300)",
                formatBounds(bounds)
        );

        withTimestamps(new long[]{100, 110}, addr -> {
            final LongList cuts = new LongList();
            final int cutCount = O3CompositeMergeStrategy.computeCuts(bounds, addr, 0, 1, 50, 8, cuts);
            for (int c = cutCount - 1; c >= 0; c--) {
                O3CompositeMergeStrategy.applyCut(bounds, (int) cuts.getQuick(c * 2), cuts.getQuick(c * 2 + 1));
            }
            Assert.assertEquals(
                    "P0(tsLo=0,tsHi=99,rows=100) P1(tsLo=100,tsHi=110,rows=11) P2(tsLo=111,tsHi=299,rows=189)"
                            + " P3(tsLo=300,tsHi=699,rows=400) P4(tsLo=700,tsHi=999,rows=300)",
                    formatBounds(bounds)
            );

            final ObjList<O3CompositeMergeStrategy.Action> actions = new ObjList<>();
            final int n = O3CompositeMergeStrategy.computeActions(bounds, addr, 0, 1, 0, actions);
            Assert.assertEquals(5, n);
            Assert.assertEquals("KEEP(p=0)", actions.getQuick(0).toString());
            Assert.assertEquals("MERGE(p=1, o3=[0,1])", actions.getQuick(1).toString());
            Assert.assertEquals("KEEP(p=2)", actions.getQuick(2).toString());
            Assert.assertEquals("KEEP(p=3)", actions.getQuick(3).toString());
            Assert.assertEquals("KEEP(p=4)", actions.getQuick(4).toString());
        });
    }

    @Test
    public void testBatchBelowFirstPieceBecomesAHeadPiece() {
        // The shape batchBelowPieceRows carries today behind an isCommitReplaceMode gate, and the shape
        // the phantom-floor rescue founds a second _txn record for. Here it is just a gap action.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        withTimestamps(new long[]{10, 20, 30}, addr -> {
            final ObjList<O3CompositeMergeStrategy.Action> actions = new ObjList<>();
            final int n = O3CompositeMergeStrategy.computeActions(bounds, addr, 0, 2, 0, actions);
            Assert.assertEquals(2, n);
            Assert.assertEquals("NEW_PIECE(o3=[0,2])", actions.getQuick(0).toString());
            Assert.assertEquals("KEEP(p=0)", actions.getQuick(1).toString());
        });
    }

    @Test
    public void testBatchBetweenTwoPiecesBecomesItsOwnPiece() {
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 400, 499, 0, 50);
        withTimestamps(new long[]{250, 260}, addr -> {
            final ObjList<O3CompositeMergeStrategy.Action> actions = new ObjList<>();
            final int n = O3CompositeMergeStrategy.computeActions(bounds, addr, 0, 1, 0, actions);
            Assert.assertEquals(3, n);
            Assert.assertEquals("KEEP(p=0)", actions.getQuick(0).toString());
            Assert.assertEquals("NEW_PIECE(o3=[0,1])", actions.getQuick(1).toString());
            Assert.assertEquals("KEEP(p=1)", actions.getQuick(2).toString());
        });
    }

    @Test
    public void testBatchOverlappingOnePieceMergesOnlyThatPiece() {
        // The whole point of the design: three pieces, and only the one the batch lands in is rewritten.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 200, 299, 0, 60);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 300, 399, 0, 70);
        withTimestamps(new long[]{250, 251}, addr -> {
            final ObjList<O3CompositeMergeStrategy.Action> actions = new ObjList<>();
            final int n = O3CompositeMergeStrategy.computeActions(bounds, addr, 0, 1, 0, actions);
            Assert.assertEquals(3, n);
            Assert.assertEquals("KEEP(p=0)", actions.getQuick(0).toString());
            Assert.assertEquals("MERGE(p=1, o3=[0,1])", actions.getQuick(1).toString());
            Assert.assertEquals("KEEP(p=2)", actions.getQuick(2).toString());
        });
    }

    @Test
    public void testCutsAreNotProposedForASliver() {
        // A cut that spares fewer than minPieceRows costs a record and saves nothing.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 0, 999, 0, 1000);
        withTimestamps(new long[]{5, 995}, addr -> {
            final LongList cuts = new LongList();
            final int n = O3CompositeMergeStrategy.computeCuts(bounds, addr, 0, 1, 100, 8, cuts);
            Assert.assertEquals(0, n);
        });
    }

    @Test
    public void testCutsSpareTheDataOnBothSidesOfTheBatch() {
        // One piece covering a whole day, a batch landing in the middle: without a cut the whole day is
        // rewritten. Two cuts leave only the middle to merge.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 0, 999, 0, 1000);
        withTimestamps(new long[]{500, 510}, addr -> {
            final LongList cuts = new LongList();
            final int n = O3CompositeMergeStrategy.computeCuts(bounds, addr, 0, 1, 100, 8, cuts);
            Assert.assertEquals("cut(piece=0,ts=500) cut(piece=0,ts=511)", formatCuts(cuts, n));
        });
    }

    @Test
    public void testCutsStopAtTheBudget() {
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 0, 999, 0, 1000);
        withTimestamps(new long[]{500, 510}, addr -> {
            final LongList cuts = new LongList();
            Assert.assertEquals(1, O3CompositeMergeStrategy.computeCuts(bounds, addr, 0, 1, 100, 1, cuts));
        });
    }

    @Test
    public void testCutsThenActionsRewriteOnlyTheMiddle() {
        // The whole pre-split round trip, which is what the composite dispatch runs per directory: one
        // piece covering a day, a batch landing in the middle. Without the cuts the day is one MERGE -
        // the whole 1000 rows rewritten for 2 new ones. With them the merge is 11 rows wide and the data
        // on either side is KEPT, which is the write amplification the design exists to remove.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 0, 999, 0, 1000);
        withTimestamps(new long[]{500, 510}, addr -> {
            final LongList cuts = new LongList();
            final int cutCount = O3CompositeMergeStrategy.computeCuts(bounds, addr, 0, 1, 100, 8, cuts);
            // Right to left: a cut inserts a piece and shifts every index above it.
            for (int c = cutCount - 1; c >= 0; c--) {
                Assert.assertTrue(O3CompositeMergeStrategy.applyCut(bounds, (int) cuts.getQuick(c * 2), cuts.getQuick(c * 2 + 1)));
            }
            Assert.assertEquals(
                    "P0(tsLo=0,tsHi=499,rows=500) P1(tsLo=500,tsHi=510,rows=11) P2(tsLo=511,tsHi=999,rows=489)",
                    formatBounds(bounds)
            );
            final ObjList<O3CompositeMergeStrategy.Action> actions = new ObjList<>();
            final int n = O3CompositeMergeStrategy.computeActions(bounds, addr, 0, 1, 0, actions);
            Assert.assertEquals(3, n);
            Assert.assertEquals("KEEP(p=0)", actions.getQuick(0).toString());
            Assert.assertEquals("MERGE(p=1, o3=[0,1])", actions.getQuick(1).toString());
            Assert.assertEquals("KEEP(p=2)", actions.getQuick(2).toString());
        });
    }

    @Test
    public void testNoCutWhenTsHiIsUnknown() {
        // Nothing bounds the piece's data, so there is no basis for apportioning rows to a cut point.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 0, Numbers.LONG_NULL, 0, 1000);
        withTimestamps(new long[]{500}, addr -> {
            final LongList cuts = new LongList();
            Assert.assertEquals(0, O3CompositeMergeStrategy.computeCuts(bounds, addr, 0, 0, 10, 8, cuts));
        });
    }

    @Test
    public void testChronologicalAppendTouchesNoExistingPiece() {
        // The common workload. Every existing piece is KEPT and the batch becomes one new piece, so the
        // commit writes only the rows it brought - no amplification at all.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 200, 299, 0, 60);
        withTimestamps(new long[]{500, 501, 502}, addr -> {
            final ObjList<O3CompositeMergeStrategy.Action> actions = new ObjList<>();
            final int n = O3CompositeMergeStrategy.computeActions(bounds, addr, 0, 2, 0, actions);
            Assert.assertEquals(3, n);
            Assert.assertEquals("KEEP(p=0)", actions.getQuick(0).toString());
            Assert.assertEquals("KEEP(p=1)", actions.getQuick(1).toString());
            Assert.assertEquals("NEW_PIECE(o3=[0,2])", actions.getQuick(2).toString());
        });
    }

    @Test
    public void testEveryO3RowIsClaimedExactlyOnce() {
        // The invariant that matters most: the action list must partition the batch. A row claimed twice
        // is duplicated, a row claimed by nobody is lost, and neither throws.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 300, 399, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 500, 599, 0, 50);
        final long[] ts = {10, 150, 151, 250, 350, 450, 550, 900};
        withTimestamps(ts, addr -> {
            final ObjList<O3CompositeMergeStrategy.Action> actions = new ObjList<>();
            final int n = O3CompositeMergeStrategy.computeActions(bounds, addr, 0, ts.length - 1, 0, actions);
            long claimed = 0;
            long expectedNext = 0;
            for (int i = 0; i < n; i++) {
                final O3CompositeMergeStrategy.Action a = actions.getQuick(i);
                if (a.getO3RowCount() > 0) {
                    Assert.assertEquals("actions must claim the batch in order", expectedNext, a.o3Lo);
                    expectedNext = a.o3Hi + 1;
                    claimed += a.getO3RowCount();
                }
            }
            Assert.assertEquals("every o3 row claimed exactly once", ts.length, claimed);
        });
    }

    @Test
    public void testSmallPieceAbsorbsAdjacentGapInsteadOfFoundingAPiece() {
        // The same trade parquet's smallRowGroupThreshold makes: do not found a piece next to a tiny one.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 5);
        withTimestamps(new long[]{10, 20}, addr -> {
            final ObjList<O3CompositeMergeStrategy.Action> actions = new ObjList<>();
            final int n = O3CompositeMergeStrategy.computeActions(bounds, addr, 0, 1, 1000, actions);
            Assert.assertEquals(1, n);
            Assert.assertEquals("MERGE(p=0, o3=[0,1])", actions.getQuick(0).toString());
        });
    }

    @Test
    public void testUnknownTsHiClaimsTheWholeRoutingRange() {
        // tsHi is LONG_NULL when it was never recorded, so nothing bounds the piece's data and it has to
        // claim its whole routing range - otherwise rows that may already be in it become a second piece
        // at an overlapping timestamp.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, Numbers.LONG_NULL, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 400, 499, 0, 50);
        withTimestamps(new long[]{250, 260}, addr -> {
            final ObjList<O3CompositeMergeStrategy.Action> actions = new ObjList<>();
            final int n = O3CompositeMergeStrategy.computeActions(bounds, addr, 0, 1, 0, actions);
            Assert.assertEquals(2, n);
            Assert.assertEquals("MERGE(p=0, o3=[0,1])", actions.getQuick(0).toString());
            Assert.assertEquals("KEEP(p=1)", actions.getQuick(1).toString());
        });
    }

    private static String formatBounds(LongList bounds) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0, n = bounds.size() / O3CompositeMergeStrategy.LONGS_PER_BOUND; i < n; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append("P").append(i)
                    .append("(tsLo=").append(O3CompositeMergeStrategy.getTsLo(bounds, i))
                    .append(",tsHi=").append(O3CompositeMergeStrategy.getTsHi(bounds, i))
                    .append(",rows=").append(O3CompositeMergeStrategy.getRowCount(bounds, i)).append(')');
        }
        return sb.toString();
    }

    private static String formatCuts(LongList cuts, int n) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append("cut(piece=").append(cuts.getQuick(i * 2))
                    .append(",ts=").append(cuts.getQuick(i * 2 + 1)).append(')');
        }
        return sb.toString();
    }

    /**
     * The sorted O3 timestamp index is 16 bytes per entry: the timestamp, then the source row.
     */
    private static void withTimestamps(long[] timestamps, TimestampConsumer body) {
        final long size = timestamps.length * 16L;
        final long addr = Unsafe.malloc(size, MemoryTag.NATIVE_O3);
        try {
            for (int i = 0; i < timestamps.length; i++) {
                Unsafe.getUnsafe().putLong(addr + i * 16L, timestamps[i]);
                Unsafe.getUnsafe().putLong(addr + i * 16L + 8, i);
            }
            body.accept(addr);
        } finally {
            Unsafe.free(addr, size, MemoryTag.NATIVE_O3);
        }
    }

    @FunctionalInterface
    private interface TimestampConsumer {
        void accept(long addr);
    }
}

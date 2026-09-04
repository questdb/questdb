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
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * The planner is pure - piece bounds and sorted O3 timestamps in, an action list out - so it needs no
 * table, no writer and no files. Every case here is a shape the composite dispatch has to get right.
 */
public class O3CompositeMergeStrategyTest {

    /**
     * A physical extent no piece built in these fixtures can ever reach, which disables
     * {@link O3CompositeMergeStrategy.ActionType#APPEND} for every test that predates it and is not about
     * it: none of their pieces' {@code rowOffset + rowCount} can equal it by accident.
     */
    private static final long NO_APPEND = Long.MAX_VALUE;

    @Test
    public void testApplyCutDeclinesWhereItWouldSaveNothing() {
        // A cut at or below the floor, above the last row, or on a piece whose data is unbounded, leaves
        // the list untouched - the caller applies cuts blind and reads the answer off the list.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 100);
        Assert.assertFalse(applyCut(bounds, 0, 100));
        Assert.assertFalse(applyCut(bounds, 0, 99));
        Assert.assertFalse(applyCut(bounds, 0, 200));
        Assert.assertEquals("P0(tsLo=100,tsHi=199,rows=100)", formatBounds(bounds));

        final LongList unbounded = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(unbounded, 100, Numbers.LONG_NULL, 0, 100);
        Assert.assertFalse(applyCut(unbounded, 0, 150));
    }

    @Test
    public void testApplyCutSplitsAPieceInTwo() {
        // Both halves address the same files at the same offsets, so this is the whole of what a pre-split
        // does to the geometry. The row the cut lands on is resolved against the data by the caller; these
        // fixtures hold one row per timestamp tick, so it is the offset of the cut into the piece.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 0, 999, 0, 1000);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 1000, 1999, 0, 500);
        Assert.assertTrue(applyCut(bounds, 0, 400));
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
        Assert.assertFalse(applyCutAt(bounds, 50));
        Assert.assertFalse(applyCutAt(bounds, 500));
        Assert.assertEquals("P0(tsLo=100,tsHi=199,rows=100)", formatBounds(bounds));
    }

    @Test
    public void testAppendDoesNotFireWhenTailPieceAlsoMerges() {
        // The batch straddles the last piece's own tsHi: some of it falls inside the piece's data range, so
        // the piece MERGEs rather than KEEPs. APPEND only ever replaces a would-be KEEP, so the rows above
        // tsHi still found a plain NEW_PIECE.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        withTimestamps(new long[]{150, 250}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 1, 0, 50);
            Assert.assertEquals(-1, plan.appendActionIndex);
            Assert.assertEquals(2, plan.actions.size());
            Assert.assertEquals("MERGE(p=0, o3=[0,0])", plan.actions.getQuick(0).toString());
            Assert.assertEquals("NEW_PIECE(o3=[1,1])", plan.actions.getQuick(1).toString());
        });
    }

    @Test
    public void testAppendDoesNotFireWhenTailPieceIsNotAtThePhysicalTail() {
        // A hole above the last piece - say, a prior commit relocated some OTHER piece to the files' tail -
        // means this piece does not own it, so extending it in place would overwrite bytes that belong to
        // whatever actually sits there. physicalRows one above the piece's own reach models that hole.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        withTimestamps(new long[]{500}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 0, 0, 51);
            Assert.assertEquals(-1, plan.appendActionIndex);
            Assert.assertEquals(2, plan.actions.size());
            Assert.assertEquals("KEEP(p=0)", plan.actions.getQuick(0).toString());
            Assert.assertEquals("NEW_PIECE(o3=[0,0])", plan.actions.getQuick(1).toString());
        });
    }

    @Test
    public void testAppendExtendsTheTailPieceOnATieWhenCommitCannotDedup() {
        // Every incoming row rounds to the tail piece's own tsHi - the shape a writer producing many rows
        // per second, one commit per second, leaves behind. Outside dedup that tie needs no comparison, so
        // it is left for APPEND instead of forcing a MERGE that would rewrite the whole piece to reorder
        // nothing.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        withTimestamps(new long[]{199, 199, 199}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 2, 0, 50, false);
            Assert.assertEquals(0, plan.appendActionIndex);
            Assert.assertEquals(1, plan.actions.size());
            Assert.assertEquals("APPEND(p=0, o3=[0,2])", plan.actions.getQuick(0).toString());
        });
    }

    @Test
    public void testTailTieStillMergesUnderDedup() {
        // Same shape as testAppendExtendsTheTailPieceOnATieWhenCommitCannotDedup, but this commit can
        // collide with an existing row and needs the key comparison MERGE runs - so the tie is still
        // claimed and the piece still rewrites, exactly as before this optimisation existed.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        withTimestamps(new long[]{199, 199, 199}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 2, 0, 50, true);
            Assert.assertEquals(-1, plan.appendActionIndex);
            Assert.assertEquals(1, plan.actions.size());
            Assert.assertEquals("MERGE(p=0, o3=[0,2])", plan.actions.getQuick(0).toString());
        });
    }

    @Test
    public void testAppendStillFiresOnATieToASinglePointTailPiece() {
        // The tail piece may already be a single point in time - built by an earlier tie of its own.
        // Growing the ONE piece a table ever has at its tail can never found a competing piece at that
        // same instant, so the tsLo == tsHi exception below does not apply to the tail: its ties are
        // always spared and left for APPEND.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 199, 199, 0, 5);
        withTimestamps(new long[]{199, 199}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 1, 0, 5, false);
            Assert.assertEquals(0, plan.appendActionIndex);
            Assert.assertEquals(1, plan.actions.size());
            Assert.assertEquals("APPEND(p=0, o3=[0,1])", plan.actions.getQuick(0).toString());
        });
    }

    @Test
    public void testSinglePointTailPieceThatLostTheTailMergesItsOwnTie() {
        // The counterpart of the test above, at the one physicalRows that changes the answer. A
        // single-point tail piece is exempted from the tsLo == tsHi rule because APPEND is supposed to
        // absorb its ties - but APPEND requires the piece to still OWN the files' tail, and once an
        // earlier piece has been merged out past it, it does not. The tie then has nowhere to go: the
        // piece is KEPT and the batch founds a NEW_PIECE at the very tsLo the kept piece already has,
        // which PartitionGeometry.addPiece rejects and the O3 worker turns into a suspended table.
        // A piece that cannot append must claim its own ties and merge them in place instead.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 199, 199, 0, 5);
        withTimestamps(new long[]{199, 199}, addr -> {
            // physicalRows 6, not 5: rowOffset + rowCount no longer reaches the files' tail.
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 1, 0, 6, false);
            Assert.assertEquals(-1, plan.appendActionIndex);
            Assert.assertEquals(
                    "a spared tie founded a second piece at the kept piece's own tsLo",
                    1,
                    plan.actions.size()
            );
            Assert.assertEquals("MERGE(p=0, o3=[0,1])", plan.actions.getQuick(0).toString());
        });
    }

    @Test
    public void testTieOnAnEarlierPieceFoundsItsOwnPieceInsteadOfMerging() {
        // The batch ties an EARLIER, non-degenerate piece's tsHi rather than the tail's. That piece owns
        // none of the files' tail, so there is nowhere to append to - but outside dedup the tie still
        // needs no comparison, so it is spared from the piece's own claim and left to found its own tiny
        // piece in the gap instead of a full rewrite of the earlier piece.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 400, 499, 0, 60);
        withTimestamps(new long[]{199}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 0, 0, NO_APPEND, false);
            Assert.assertEquals(-1, plan.appendActionIndex);
            Assert.assertEquals(3, plan.actions.size());
            Assert.assertEquals("KEEP(p=0)", plan.actions.getQuick(0).toString());
            Assert.assertEquals("NEW_PIECE(o3=[0,0])", plan.actions.getQuick(1).toString());
            Assert.assertEquals("KEEP(p=1)", plan.actions.getQuick(2).toString());
        });
    }

    @Test
    public void testRepeatedTieOnAnEarlierPieceMergesIntoTheSinglePointPieceInsteadOfFoundingAnother() {
        // A second commit ties the SAME earlier instant a first one already carved into its own
        // single-point piece. Sparing it again would found a second piece at that exact instant, which
        // nothing distinguishes from the first - so a single-point piece keeps claiming its own ties and
        // grows in place via an ordinary, cheap MERGE instead of ever letting two pieces share a timestamp.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 199, 199, 1000, 1);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 400, 499, 0, 60);
        withTimestamps(new long[]{199}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 0, 0, NO_APPEND, false);
            Assert.assertEquals(-1, plan.appendActionIndex);
            Assert.assertEquals(3, plan.actions.size());
            Assert.assertEquals("KEEP(p=0)", plan.actions.getQuick(0).toString());
            Assert.assertEquals("MERGE(p=1, o3=[0,0])", plan.actions.getQuick(1).toString());
            Assert.assertEquals("KEEP(p=2)", plan.actions.getQuick(2).toString());
        });
    }

    @Test
    public void testTieOnAnEarlierPieceStillMergesUnderDedup() {
        // Same shape as testTieOnAnEarlierPieceFoundsItsOwnPieceInsteadOfMerging, but this commit can
        // collide with an existing row and needs the key comparison MERGE runs - so the tie is still
        // claimed by the piece it touches, exactly as before this optimisation existed.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 400, 499, 0, 60);
        withTimestamps(new long[]{199}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 0, 0, NO_APPEND, true);
            Assert.assertEquals(-1, plan.appendActionIndex);
            Assert.assertEquals(2, plan.actions.size());
            Assert.assertEquals("MERGE(p=0, o3=[0,0])", plan.actions.getQuick(0).toString());
            Assert.assertEquals("KEEP(p=1)", plan.actions.getQuick(1).toString());
        });
    }

    @Test
    public void testAppendExtendsTheTailPieceAroundAHeadGapToo() {
        // The worked example: a batch with rows both below the floor and above the tail in the SAME commit.
        // The head rows still found their own piece - APPEND only ever concerns the tail - while the tail
        // rows extend the existing piece instead of founding a second new one.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 1100, 1400, 0, 50);
        withTimestamps(new long[]{900, 1000, 1500, 1600}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 3, 0, 50);
            Assert.assertEquals(1, plan.appendActionIndex);
            Assert.assertEquals(2, plan.actions.size());
            Assert.assertEquals("NEW_PIECE(o3=[0,1])", plan.actions.getQuick(0).toString());
            Assert.assertEquals("APPEND(p=0, o3=[2,3])", plan.actions.getQuick(1).toString());
        });
    }

    @Test
    public void testAppendExtendsTheTailPieceInsteadOfFoundingANewOne() {
        // Same shape as testChronologicalAppendTouchesNoExistingPiece, but with a physicalRows that matches
        // reality: the last piece genuinely owns the files' tail, so the batch above it extends that piece
        // in place instead of founding a third one.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 200, 299, 0, 60);
        withTimestamps(new long[]{500, 501, 502}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 2, 0, 60);
            Assert.assertEquals(1, plan.appendActionIndex);
            Assert.assertEquals(2, plan.actions.size());
            Assert.assertEquals("KEEP(p=0)", plan.actions.getQuick(0).toString());
            Assert.assertEquals("APPEND(p=1, o3=[0,2])", plan.actions.getQuick(1).toString());
        });
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
            Assert.assertTrue(applyCutAt(bounds, clusterCuts.getQuick(i)));
        }
        Assert.assertEquals(
                "P0(tsLo=0,tsHi=299,rows=300) P1(tsLo=300,tsHi=699,rows=400) P2(tsLo=700,tsHi=999,rows=300)",
                formatBounds(bounds)
        );

        withTimestamps(new long[]{100, 110}, addr -> {
            final LongList cuts = new LongList();
            final int cutCount = O3CompositeMergeStrategy.computeCuts(bounds, addr, 0, 1, 50, 8, cuts);
            for (int c = cutCount - 1; c >= 0; c--) {
                applyCut(bounds, (int) cuts.getQuick(c * 2), cuts.getQuick(c * 2 + 1));
            }
            Assert.assertEquals(
                    "P0(tsLo=0,tsHi=99,rows=100) P1(tsLo=100,tsHi=110,rows=11) P2(tsLo=111,tsHi=299,rows=189)"
                            + " P3(tsLo=300,tsHi=699,rows=400) P4(tsLo=700,tsHi=999,rows=300)",
                    formatBounds(bounds)
            );

            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 1, 0);
            Assert.assertEquals(5, plan.actions.size());
            Assert.assertEquals("KEEP(p=0)", plan.actions.getQuick(0).toString());
            Assert.assertEquals("MERGE(p=1, o3=[0,1])", plan.actions.getQuick(1).toString());
            Assert.assertEquals("KEEP(p=2)", plan.actions.getQuick(2).toString());
            Assert.assertEquals("KEEP(p=3)", plan.actions.getQuick(3).toString());
            Assert.assertEquals("KEEP(p=4)", plan.actions.getQuick(4).toString());
        });
    }

    @Test
    public void testBatchBelowFirstPieceBecomesAHeadPiece() {
        // The shape batchBelowPieceRows carries today behind an isCommitReplaceMode gate, and the shape
        // the phantom-floor rescue founds a second _txn record for. Here it is just a gap action.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        withTimestamps(new long[]{10, 20, 30}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 2, 0);
            Assert.assertEquals(2, plan.actions.size());
            Assert.assertEquals("NEW_PIECE(o3=[0,2])", plan.actions.getQuick(0).toString());
            Assert.assertEquals("KEEP(p=0)", plan.actions.getQuick(1).toString());
        });
    }

    @Test
    public void testBatchBetweenTwoPiecesBecomesItsOwnPiece() {
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 400, 499, 0, 50);
        withTimestamps(new long[]{250, 260}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 1, 0);
            Assert.assertEquals(3, plan.actions.size());
            Assert.assertEquals("KEEP(p=0)", plan.actions.getQuick(0).toString());
            Assert.assertEquals("NEW_PIECE(o3=[0,1])", plan.actions.getQuick(1).toString());
            Assert.assertEquals("KEEP(p=1)", plan.actions.getQuick(2).toString());
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
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 1, 0);
            Assert.assertEquals(3, plan.actions.size());
            Assert.assertEquals("KEEP(p=0)", plan.actions.getQuick(0).toString());
            Assert.assertEquals("MERGE(p=1, o3=[0,1])", plan.actions.getQuick(1).toString());
            Assert.assertEquals("KEEP(p=2)", plan.actions.getQuick(2).toString());
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
                Assert.assertTrue(applyCut(bounds, (int) cuts.getQuick(c * 2), cuts.getQuick(c * 2 + 1)));
            }
            Assert.assertEquals(
                    "P0(tsLo=0,tsHi=499,rows=500) P1(tsLo=500,tsHi=510,rows=11) P2(tsLo=511,tsHi=999,rows=489)",
                    formatBounds(bounds)
            );
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 1, 0);
            Assert.assertEquals(3, plan.actions.size());
            Assert.assertEquals("KEEP(p=0)", plan.actions.getQuick(0).toString());
            Assert.assertEquals("MERGE(p=1, o3=[0,1])", plan.actions.getQuick(1).toString());
            Assert.assertEquals("KEEP(p=2)", plan.actions.getQuick(2).toString());
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
        // commit writes only the rows it brought - no amplification at all. NO_APPEND keeps this test about
        // that invariant rather than the tail-extension optimisation - see
        // testAppendExtendsTheTailPieceInsteadOfFoundingANewOne for the shape where it fires instead.
        final LongList bounds = new LongList();
        O3CompositeMergeStrategy.addPieceBounds(bounds, 100, 199, 0, 50);
        O3CompositeMergeStrategy.addPieceBounds(bounds, 200, 299, 0, 60);
        withTimestamps(new long[]{500, 501, 502}, addr -> {
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 2, 0);
            Assert.assertEquals(-1, plan.appendActionIndex);
            Assert.assertEquals(3, plan.actions.size());
            Assert.assertEquals("KEEP(p=0)", plan.actions.getQuick(0).toString());
            Assert.assertEquals("KEEP(p=1)", plan.actions.getQuick(1).toString());
            Assert.assertEquals("NEW_PIECE(o3=[0,2])", plan.actions.getQuick(2).toString());
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
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, ts.length - 1, 0);
            long claimed = 0;
            long expectedNext = 0;
            for (int i = 0, n = plan.actions.size(); i < n; i++) {
                final O3CompositeMergeStrategy.Action a = plan.actions.getQuick(i);
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
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 1, 1000);
            Assert.assertEquals(1, plan.actions.size());
            Assert.assertEquals("MERGE(p=0, o3=[0,1])", plan.actions.getQuick(0).toString());
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
            final O3CompositeMergeStrategy.Plan plan = computeActions(bounds, addr, 0, 1, 0);
            Assert.assertEquals(2, plan.actions.size());
            Assert.assertEquals("MERGE(p=0, o3=[0,1])", plan.actions.getQuick(0).toString());
            Assert.assertEquals("KEEP(p=1)", plan.actions.getQuick(1).toString());
        });
    }

    /**
     * {@link O3CompositeMergeStrategy#applyCut} takes the row a cut resolves to and each half's own
     * bound, because a timestamp range says nothing about where inside it the rows sit - the caller
     * searches the piece's timestamp column for all three. Every fixture here holds one row per timestamp
     * tick, so they follow from the cut, and spelling them out keeps this class free of files.
     */
    private static boolean applyCut(LongList bounds, int piece, long cutTs) {
        final long tsLo = O3CompositeMergeStrategy.getTsLo(bounds, piece);
        if (cutTs <= tsLo || cutTs > O3CompositeMergeStrategy.getTsHi(bounds, piece)) {
            return false;
        }
        // One row per tick, so the cut's offset into the piece is the row, the lower half's last row is
        // the tick below the cut, and the upper half's first row is the cut itself.
        return O3CompositeMergeStrategy.applyCut(bounds, piece, cutTs - tsLo, cutTs - 1, cutTs);
    }

    /**
     * Cuts whichever piece contains {@code cutTs}, as the composite dispatch does for a cut that came from
     * transaction clustering and so carries a timestamp and no piece index.
     */
    private static boolean applyCutAt(LongList bounds, long cutTs) {
        final int piece = O3CompositeMergeStrategy.findPieceContaining(bounds, cutTs);
        return piece > -1 && applyCut(bounds, piece, cutTs);
    }

    /**
     * {@link O3CompositeMergeStrategy#computeActions}, with {@link #NO_APPEND} disabling the tail-extension
     * optimisation for tests not about it, and {@code commitMayDedup=true} - the conservative, pre-existing
     * behaviour - for tests not about the dedup-free tail-tie optimisation either.
     */
    private static O3CompositeMergeStrategy.Plan computeActions(
            LongList bounds, long sortedTimestampsAddr, long srcOooLo, long srcOooHi, long smallPieceThreshold
    ) {
        return computeActions(bounds, sortedTimestampsAddr, srcOooLo, srcOooHi, smallPieceThreshold, NO_APPEND);
    }

    private static O3CompositeMergeStrategy.Plan computeActions(
            LongList bounds,
            long sortedTimestampsAddr,
            long srcOooLo,
            long srcOooHi,
            long smallPieceThreshold,
            long physicalRows
    ) {
        return computeActions(bounds, sortedTimestampsAddr, srcOooLo, srcOooHi, smallPieceThreshold, physicalRows, true);
    }

    private static O3CompositeMergeStrategy.Plan computeActions(
            LongList bounds,
            long sortedTimestampsAddr,
            long srcOooLo,
            long srcOooHi,
            long smallPieceThreshold,
            long physicalRows,
            boolean commitMayDedup
    ) {
        return O3CompositeMergeStrategy.computeActions(
                bounds, sortedTimestampsAddr, srcOooLo, srcOooHi, smallPieceThreshold, physicalRows, commitMayDedup,
                new O3CompositeMergeStrategy.Plan()
        );
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

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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorPlan;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.griffin.SqlException;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the change-set classification an out-of-order repair plans from: which
 * executor runs, which sealed anchor it may trust, where it commits, and how
 * far down it unseals retained checkpoints. The refresh job derives all of this
 * once per repair against one pinned base snapshot; these tests exercise that
 * decision directly, without an engine, a base table or a replay.
 * <p>
 * The properties under test are safety properties, not optimisations: an anchor
 * chosen at or above a change would silently drop rows the resume never re-reads,
 * and a retire floor left too high would strand a poisoned anchor for a later
 * repair to resume from.
 */
public class LiveViewCheckpointRepairPlanTest {
    private static final long BEGINNING = Long.MIN_VALUE; // START FROM BEGINNING, = Numbers.LONG_NULL
    // The view is unanchored, or its anchor carries no fixed segment the repair can
    // bound itself with.
    private static final LiveViewCheckpointAnchorPlan NO_ANCHOR = null;
    // The caller could not bound how far up the incorporated change reaches, so no
    // convergence boundary can be derived and the repair reads to the end of the base
    // table.
    private static final long NO_CHANGE_MAX_TS = Numbers.LONG_NULL;
    // The live-view table holds no durable row, so nothing bounds D from below and the
    // output floor R collapses to S.
    private static final long NO_DURABLE_OUTPUT = Numbers.LONG_NULL;
    // No finite RANGE dependency covers every window function, so the rebuild has no
    // dependency floor to localize to.
    private static final long NO_RANGE = Numbers.LONG_NULL;
    // No finite ROWS dependency either, so nothing calls back into a per-key discovery.
    private static final LiveViewCheckpointRepairPlan.RowsBoundSource NO_ROWS = null;
    // The caller could not prove the incorporated change set added rows and removed
    // none, which denies the ROWS discovery its affected key domain.
    private static final boolean NOT_INSERT_ONLY = false;
    // The repair has no way to put the runtime window state back, so it must not stop
    // short of the frontier.
    private static final long NO_RUNTIME_FRONTIER = Numbers.LONG_NULL;
    // An epoch-aligned anchor whose segments are 4_000 microseconds wide, so the cases
    // below can state a segment start and end directly against their small timestamps.
    private static final LiveViewCheckpointAnchorPlan SEGMENTS_OF_4000 =
            LiveViewCheckpointAnchorPlan.of('U', 4_000, 0, ColumnType.TIMESTAMP_MICRO);
    // No cost oracle, so the plan cannot weigh a qualifying anchor's resume against a
    // localized rebuild and keeps the anchor - the disposition every case below the
    // cost ones asserts. They are about which anchor may be trusted and which floors a
    // rebuild derives, neither of which the price changes.
    private static final LiveViewCheckpointRepairPlan.ScanCostSource UNPRICED = null;

    @Test
    public void testAnchorBoundsRunOnlyForARepairThatCouldUseThem() throws SqlException {
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();

        // No change ceiling: nothing says which segment the change stops in.
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, SEGMENTS_OF_4000, true, 9_000, NO_CHANGE_MAX_TS, 9_000, UNPRICED);
        assertAnchorRebuildIsUnlocalized(plan);

        // No runtime frontier: the repair cannot put the window state - including the
        // anchor map - back, so it must not stop short of the tail.
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, SEGMENTS_OF_4000, true, 9_000, 6_000, NO_RUNTIME_FRONTIER, UNPRICED);
        assertAnchorRebuildIsUnlocalized(plan);

        // Output the runtime holds but has not made durable sits above the live-view
        // table's frontier, and a replacement stopping at H would neither re-emit it nor
        // leave it on disk.
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, SEGMENTS_OF_4000, true, 8_000, 6_000, 9_000, UNPRICED);
        assertAnchorRebuildIsUnlocalized(plan);

        // The live-view table holds no durable row, so R collapses to S and the rebuild
        // re-emits the whole history whatever the segment says.
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, SEGMENTS_OF_4000, true, NO_DURABLE_OUTPUT, 6_000, 9_000, UNPRICED);
        assertAnchorRebuildIsUnlocalized(plan);

        // The frontier sits inside the segment the change lands in, so the change is
        // NOT outside the state the runtime currently holds - the state the replay ends
        // on is the correct one and must be promoted, which a finite H would prevent.
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, SEGMENTS_OF_4000, true, 7_000, 6_000, 7_000, UNPRICED);
        assertAnchorRebuildIsUnlocalized(plan);

        // A resume is bounded by its anchor, so the segment is never consulted.
        plan.of(new TestAnchors().add(2_000, 11), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, SEGMENTS_OF_4000, true, 9_000, 6_000, 9_000, UNPRICED);
        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertFalse(plan.isLocalized());
    }

    @Test
    public void testAnchorSegmentDeclinesWithoutARepresentableSegmentEnd() throws SqlException {
        // A nanosecond anchor period over a microsecond column advances nothing, so the
        // segment has no end the plan can name. That is H = EOF, and an anchored view
        // must not localize on it: promoting the replay's state would drop every
        // partition whose rows all sit below L, exactly as it would for a ROWS frame.
        final LiveViewCheckpointAnchorPlan subResolution =
                LiveViewCheckpointAnchorPlan.of('n', 1, 0, ColumnType.TIMESTAMP_MICRO);
        Assert.assertNotNull(subResolution);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, subResolution, true, 9_000, 6_000, 9_000, UNPRICED);

        assertAnchorRebuildIsUnlocalized(plan);
    }

    @Test
    public void testAnchorSegmentUnionsWithAFrameDependency() throws SqlException {
        // A view mixing an anchored window with a bounded RANGE one - each plan bounds the
        // functions of its own kind, so neither may be dropped in favour of the other. The
        // RANGE arm proves L = R - W = 4_900 and H = changeMaxTs + W + 1 = 6_101, the
        // segment proves 4_000 and 8_000, and the union is the outer pair of the two: the
        // warm-up satisfies both frames and the replacement stops where the later of them
        // converges.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, SEGMENTS_OF_4000, true, 9_000, 6_000, 9_000, UNPRICED);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(4_000, plan.getReplayLowTs());
        Assert.assertEquals(5_000, plan.getOutputLowTs());
        Assert.assertEquals(HighBoundTag.FINITE, plan.getHighBoundTag());
        Assert.assertEquals(8_000, plan.getHighTsExclusive());

        // The frontier now sits between the two arms' bounds. It clears the RANGE arm's
        // 6_101 but not the union's 8_000, and it is the union the runtime is restored
        // against - so the plan declines rather than stopping where only one arm converged.
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, SEGMENTS_OF_4000, true, 9_000, 6_000, 7_000, UNPRICED);
        assertAnchorRebuildIsUnlocalized(plan);
    }

    @Test
    public void testAnchorSegmentLocalizesFromTheSegmentBoundaries() throws SqlException {
        // S=1_000, C=5_000, D=9_000 -> R = 5_000, and the anchor buckets rows in
        // 4_000-wide segments. The state a row at R holds is exactly the rows in
        // [4_000, R] - the anchor reset at 4_000 put every function on it back to
        // identity - and the change at 6_000 reaches no output at or above 8_000, where
        // the next reset throws its contribution away. So the rebuild reads from 4_000,
        // re-emits from 5_000 and stops at 8_000 instead of running out the tail.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, SEGMENTS_OF_4000, true, 9_000, 6_000, 9_000, UNPRICED);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(4_000, plan.getReplayLowTs());
        Assert.assertEquals(5_000, plan.getOutputLowTs());
        Assert.assertEquals(HighBoundTag.FINITE, plan.getHighBoundTag());
        Assert.assertEquals(8_000, plan.getHighTsExclusive());
        Assert.assertEquals(7_999, plan.getScanHighTsInclusive());
        // A finite bound is what puts the pre-repair runtime - functions and anchor map
        // alike - back over the state the replay ends on.
        Assert.assertTrue(plan.isRuntimeStatePreserved());
        // Unlike the ROWS path, no key domain enters either bound, so the anchor path
        // needs no insert-only proof: the same change set with a deletion in it localizes
        // identically.
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, SEGMENTS_OF_4000, NOT_INSERT_ONLY, 9_000, 6_000, 9_000, UNPRICED);
        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(4_000, plan.getReplayLowTs());
        Assert.assertEquals(8_000, plan.getHighTsExclusive());
    }

    @Test
    public void testAnchorSegmentStartIsClampedToTheViewBoundary() throws SqlException {
        // The view's START FROM boundary sits inside the segment holding R, so the rows
        // below it are not the view's to replay. L clamps up, which is exact rather than
        // conservative: the whole-history run never saw them either.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 5_000, 4_500, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, SEGMENTS_OF_4000, true, 9_000, 6_000, 9_000, UNPRICED);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(4_500, plan.getReplayLowTs());
        Assert.assertEquals(5_000, plan.getOutputLowTs());
        Assert.assertEquals(8_000, plan.getHighTsExclusive());
    }

    @Test
    public void testApplyAheadKeepsAnchorBelowAheadFloor() throws SqlException {
        // Apply raced 3 seqTxns past the trigger and the lowest in-view timestamp
        // among them (150) sits below the trigger (400). The resume may only
        // anchor below that floor, so the anchor at 300 - fine against the trigger
        // alone - is rejected in favour of the one at 100.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 400, BEGINNING, 7, 10, 150, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertTrue(plan.isApplyAhead());
        Assert.assertEquals(11, plan.getAnchorCheckpointId());
        Assert.assertEquals(100, plan.getAnchorMaxTs());
        Assert.assertEquals(101, plan.getReplayLowTs());
        // Commit at the pinned snapshot, not the trigger: the replay materialises
        // the ahead range too.
        Assert.assertEquals(10, plan.getCommitSeqTxn());
        // Everything at or above the ahead floor is unsealed, including entries a
        // back-dated ahead row invalidated below the trigger.
        Assert.assertEquals(150, plan.getRetireLowTs());
        Assert.assertEquals(150, plan.getApplyAheadMinTs());
    }

    @Test
    public void testApplyAheadRebuildsWhenNoAnchorBelowAheadFloor() throws SqlException {
        // Same ring, but the ahead range reaches down to 50 - below every anchor.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 400, BEGINNING, 7, 10, 50, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(LiveViewCheckpointRepairPlan.DISPOSITION_BOUNDARY_REBUILD, plan.getDisposition());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getAnchorCheckpointId());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getAnchorMaxTs());
        Assert.assertEquals(10, plan.getCommitSeqTxn());
        Assert.assertEquals(50, plan.getRetireLowTs());
    }

    @Test
    public void testApplyAheadUnclassifiableRetiresEverything() throws SqlException {
        // A structural or non-DATA commit in the ahead range reports LONG_NULL: the
        // repair cannot bound what it changed, so no anchor survives and the whole
        // ring is retired.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 400, BEGINNING, 7, 10, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getRetireLowTs());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getApplyAheadMinTs());
        Assert.assertEquals(10, plan.getCommitSeqTxn());
    }

    @Test
    public void testApplyAheadWithHigherFloorKeepsCorrectionFloor() throws SqlException {
        // The ahead range's lowest in-view timestamp (900) sits ABOVE the trigger
        // (400), so it cannot lower anything: the retire floor stays at C and the
        // anchor selection is the one the trigger alone would make.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 400, BEGINNING, 7, 9, 900, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(300, plan.getAnchorMaxTs());
        Assert.assertEquals(400, plan.getRetireLowTs());
        Assert.assertEquals(400, plan.getCorrectionTs());
        Assert.assertEquals(9, plan.getCommitSeqTxn());
    }

    @Test
    public void testBoundedMissResumesFromOlderAnchor() throws SqlException {
        // The head (300) sits at or above the late row (300), so it cannot anchor
        // the resume - its state already incorporates rows the change invalidates.
        // The next entry down (200) can.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 300, BEGINNING, 5, 5, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(12, plan.getAnchorCheckpointId());
        Assert.assertEquals(200, plan.getAnchorMaxTs());
        Assert.assertEquals(201, plan.getReplayLowTs());
        Assert.assertEquals(300, plan.getRetireLowTs());
        // One search only: no apply-ahead gap to re-anchor against.
        Assert.assertEquals(1, anchors.searches);
    }

    @Test
    public void testChangeBelowWholeRingRebuilds() throws SqlException {
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 50, BEGINNING, 5, 5, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(5, plan.getCommitSeqTxn());
        Assert.assertEquals(50, plan.getCorrectionTs());
        Assert.assertEquals(50, plan.getRetireLowTs());
        // A rebuild scans from the view boundary.
        Assert.assertEquals(BEGINNING, plan.getReplayLowTs());
    }

    @Test
    public void testCorrectionFloorClampsToViewLowerBound() throws SqlException {
        // A commit routinely reaches below the view's START FROM boundary; those
        // rows are simply not the view's, so C clamps up to the boundary. Both the
        // delete authority and the retire floor follow it.
        final TestAnchors anchors = new TestAnchors();
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 10, 1_000, 5, 5, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertEquals(1_000, plan.getCorrectionTs());
        Assert.assertEquals(1_000, plan.getRetireLowTs());
        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(1_000, plan.getReplayLowTs());
    }

    @Test
    public void testCostAsksTheRowsDiscoveryAnAnchoredRepairUsedToSkip() throws SqlException {
        // Which disposition is cheaper turns on the rows the rebuild adds below the
        // anchor's floor against the rows the resume adds above H, and for a ROWS view
        // both of those bounds ARE the discovery's answer. So an anchored repair now
        // runs it. The change deep in history is the case that repays it.
        final TestScanCost cost = new TestScanCost(1_000, 100_000);
        final TestRowsBounds rows = new TestRowsBounds(4_500, HighBoundTag.FINITE, 5_200);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors().add(4_000, 11), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, true, 100_000, 5_000, 100_000, cost);

        Assert.assertEquals(1, rows.discoveries);
        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(4_500, plan.getReplayLowTs());
        Assert.assertEquals(5_000, plan.getOutputLowTs());
        Assert.assertEquals(5_200, plan.getHighTsExclusive());
        Assert.assertEquals(96_000, plan.getResumeScanRows());
        Assert.assertEquals(700, plan.getRebuildScanRows());

        // The same view with the change near the head, where the discovery's answer is
        // that the rebuild reads more. It is spent either way - that is the price of
        // making the choice on evidence - and the scan budget is what bounds it.
        final TestRowsBounds headRows = new TestRowsBounds(45_000, HighBoundTag.FINITE, 100_000);
        plan.of(new TestAnchors().add(99_000, 11), 99_500, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, headRows, NO_ANCHOR, true, 100_000, 99_999, 100_000, cost);

        Assert.assertEquals(1, headRows.discoveries);
        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(99_001, plan.getReplayLowTs());
        // A key whose Nmax predecessors sit far below the correction is what makes the
        // rebuild expensive here: 55_000 warm-up-and-replay rows against a 1_000-row tail.
        Assert.assertEquals(1_000, plan.getResumeScanRows());
        Assert.assertEquals(55_000, plan.getRebuildScanRows());
    }

    @Test
    public void testCostKeepsTheAnchorForAChangeNearTheHead() throws SqlException {
        // A change close to the base table's head leaves the resume a short tail, and no
        // warm-up beats it: the rebuild has to reconstruct one whole frame width below
        // the correction before it may emit a row. The anchor is the cheaper disposition
        // and the plan says so, having derived - and discarded - the rebuild bounds.
        final TestScanCost cost = new TestScanCost(1_000, 100_000);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors().add(98_000, 11), 99_000, 1_000, 9, 9, Numbers.LONG_NULL, 10_000, NO_ROWS, NO_ANCHOR, true, 100_000, 99_500, 100_000, cost);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(98_000, plan.getAnchorMaxTs());
        Assert.assertEquals(98_001, plan.getReplayLowTs());
        // The resume emits every row it reads, so the rebuild's floors are discarded
        // whole. Its high bound was already end-of-frame: a frame this wide converges a
        // width past the change, which is past the runtime frontier, and that is exactly
        // why the rebuild reads to the end of the base table from a floor below the
        // anchor's.
        Assert.assertEquals(98_001, plan.getOutputLowTs());
        Assert.assertFalse(plan.isLocalized());
        assertEofHighBound(plan);
        // 2_000 rows above the anchor against the 11_001 of [R - W, H): the warm-up
        // alone costs five times the tail.
        Assert.assertEquals(2_000, plan.getResumeScanRows());
        Assert.assertEquals(11_001, plan.getRebuildScanRows());
    }

    @Test
    public void testCostKeepsTheAnchorOnATieAndOnAnUnlocalizedRebuild() throws SqlException {
        // A tie keeps the anchor. The resume needs no warm-up, stages no root version
        // and carries no repair descriptor, so an equal row count is not an equal
        // repair - and an estimate is not exact enough to spend that on.
        final TestScanCost cost = new TestScanCost(0, 1_000);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        // Anchor at 499 puts the resume at [500, 1_000], 501 rows. W = 0 and
        // changeMaxTs = C put the rebuild at [500, 500]... which is 1 row, so widen the
        // frame until the two meet: W = 250 gives L = 250, H = 751, and [250, 750] is
        // 501 rows exactly.
        plan.of(new TestAnchors().add(499, 11), 500, 0, 9, 9, Numbers.LONG_NULL, 250, NO_ROWS, NO_ANCHOR, true, 1_000, 500, 1_000, cost);

        Assert.assertEquals(501, plan.getResumeScanRows());
        Assert.assertEquals(501, plan.getRebuildScanRows());
        Assert.assertTrue(plan.isResumeFromAnchor());

        // A rebuild that could not localize is not priced at all: it reads the whole view
        // history, which is every row the resume reads and then the ones below the anchor
        // as well.
        plan.of(new TestAnchors().add(499, 11), 500, 0, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, true, 1_000, 500, 1_000, cost);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(501, plan.getResumeScanRows());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getRebuildScanRows());
    }

    @Test
    public void testCostPrefersTheLocalizedRebuildOverADistantAnchor() throws SqlException {
        // The pathology a dense checkpoint cadence creates: an anchor sits just below an
        // old correction, so a resume qualifies - and then replays every row above it,
        // which on a long-lived view is the whole thing. The dependency interval is two
        // frame widths wide however old the correction is, so the plan takes it.
        final TestScanCost cost = new TestScanCost(1_000, 100_000);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors().add(4_000, 11), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, true, 100_000, 5_000, 100_000, cost);

        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertTrue(plan.isLocalized());
        // The anchor is dropped rather than left on a rebuild that will not restore it.
        Assert.assertEquals(Numbers.LONG_NULL, plan.getAnchorCheckpointId());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getAnchorMaxTs());
        Assert.assertEquals(4_900, plan.getReplayLowTs());
        Assert.assertEquals(5_000, plan.getOutputLowTs());
        Assert.assertEquals(HighBoundTag.FINITE, plan.getHighBoundTag());
        Assert.assertEquals(5_101, plan.getHighTsExclusive());
        // 96_000 rows above the anchor against the 201 of [R - W, H).
        Assert.assertEquals(96_000, plan.getResumeScanRows());
        Assert.assertEquals(201, plan.getRebuildScanRows());

        // Same repair, unpriced: the plan cannot compare and keeps the anchor, which is
        // what every case in this class that states UNPRICED relies on.
        plan.of(new TestAnchors().add(4_000, 11), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, true, 100_000, 5_000, 100_000, UNPRICED);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getResumeScanRows());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getRebuildScanRows());
    }

    @Test
    public void testFiniteHighBoundClampsAboveTheOutputFloor() throws SqlException {
        // Defensive. The refresh job cannot produce this pair - the change ceiling is
        // the maximum of a set that contains the trigger timestamp, and R never rises
        // above that timestamp - but the plan is handed both numbers separately and a
        // replacement's high bound has to sit strictly above its low bound or the
        // interval is empty or inverted. A change ceiling under R clamps to R + 1,
        // which re-emits the single timestamp group at R: sound, because the replay
        // reproduces it identically.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 1_000, BEGINNING, 9, 9, Numbers.LONG_NULL, 10, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 5_000, 500, 5_000, UNPRICED);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(1_000, plan.getOutputLowTs());
        Assert.assertEquals(HighBoundTag.FINITE, plan.getHighBoundTag());
        Assert.assertEquals(1_001, plan.getHighTsExclusive());
        Assert.assertEquals(1_000, plan.getScanHighTsInclusive());
    }

    @Test
    public void testFiniteHighBoundFromRangeDependency() throws SqlException {
        // The step-4b win, on the shape 4a bounded from below. No anchor sits below the
        // change, so the repair takes the boundary rebuild - but a 100-unit RANGE
        // look-behind closes it at both ends: a row at m sits in the frame of every row
        // in [m, m + W] and no other, so a change topping out at 520 cannot reach output
        // at or above 621.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, 520, 900, UNPRICED);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(520, plan.getChangeMaxTs());
        Assert.assertEquals(500, plan.getOutputLowTs());
        Assert.assertEquals(400, plan.getReplayLowTs());
        Assert.assertEquals(HighBoundTag.FINITE, plan.getHighBoundTag());
        Assert.assertFalse(plan.isHighBoundEof());
        // Exclusive H, and the inclusive form the bounded forward cursor takes. The
        // whole timestamp tie at 620 is admitted; nothing at 621 is.
        Assert.assertEquals(621, plan.getHighTsExclusive());
        Assert.assertEquals(620, plan.getScanHighTsInclusive());
        // The runtime frontier sits above H, so no changed row is inside the frame the
        // runtime holds: the state the repair found is correct and the state the replay
        // ends on is not.
        Assert.assertTrue(plan.isRuntimeStatePreserved());
    }

    @Test
    public void testFiniteHighBoundRequiresBoundedChangeAndPreservableRuntime() throws SqlException {
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();

        // No change ceiling: a non-DATA or structural entry in the incorporated range
        // can change rows anywhere, so no arithmetic over the inserted timestamps
        // bounds it.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, NO_CHANGE_MAX_TS, 900, UNPRICED);
        Assert.assertTrue(plan.isLocalized());
        assertEofHighBound(plan);
        Assert.assertFalse(plan.isRuntimeStatePreserved());

        // No way to put the runtime state back afterwards, so the repair must run all
        // the way to the frontier and keep what the replay produced.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, 520, NO_RUNTIME_FRONTIER, UNPRICED);
        assertEofHighBound(plan);

        // The frontier sits BELOW H, so the change does reach the frame the runtime
        // holds. Stopping at H would leave the runtime describing a boundary its own
        // state has already passed.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, 520, 600, UNPRICED);
        assertEofHighBound(plan);

        // Output above the durable frontier exists only in an un-flushed lead or a
        // rolled-back draft. A replacement stopping at H would neither re-emit it nor
        // leave it on disk, so it would be lost while the watermark advanced past the
        // base rows behind it.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 800, 520, 900, UNPRICED);
        Assert.assertTrue(plan.isLocalized());
        assertEofHighBound(plan);

        // No finite RANGE dependency: the rebuild is unlocalized, and a width it does
        // not have is the width H would be measured in.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, 520, 900, UNPRICED);
        assertEofHighBound(plan);
    }

    @Test
    public void testFiniteHighBoundSaturatesAtTopOfRange() throws SqlException {
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();

        // changeMaxTs + W wraps. A change that late converges nowhere the arithmetic
        // can name, so the repair reads the tail out.
        plan.of(new TestAnchors(), Long.MAX_VALUE - 20, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, Long.MAX_VALUE - 5, Long.MAX_VALUE - 10, Long.MAX_VALUE - 5, UNPRICED);
        Assert.assertTrue(plan.isLocalized());
        assertEofHighBound(plan);

        // The sum lands exactly on Long.MAX_VALUE. It does not wrap, but its exclusive
        // successor is not representable - which is precisely why the bound is tagged
        // rather than spelled as a timestamp, since Long.MAX_VALUE is real data.
        plan.of(new TestAnchors(), Long.MAX_VALUE - 200, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, Long.MAX_VALUE, Long.MAX_VALUE - 100, Long.MAX_VALUE, UNPRICED);
        Assert.assertTrue(plan.isLocalized());
        assertEofHighBound(plan);
    }

    @Test
    public void testNewestBoundaryBelowTheChangeAnchorsTheResume() throws SqlException {
        // The common shape: the change sits above every boundary, so the newest one
        // anchors the resume and the search runs exactly once.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 500, BEGINNING, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(12, plan.getAnchorCheckpointId());
        Assert.assertEquals(200, plan.getAnchorMaxTs());
        Assert.assertEquals(201, plan.getReplayLowTs());
        Assert.assertEquals(9, plan.getCommitSeqTxn());
        Assert.assertEquals(500, plan.getCorrectionTs());
        Assert.assertEquals(500, plan.getRetireLowTs());
        Assert.assertFalse(plan.isApplyAhead());
        Assert.assertEquals(1, anchors.searches);
    }

    @Test
    public void testHighBoundIsTaggedEofForResumeAndUnlocalizedRebuild() throws SqlException {
        // H is the tagged exclusive bound above which the repair may not read. Only a
        // localized rebuild derives a finite one: a resume is already bounded below by
        // its anchor and keeps replaying the whole tail so the runtime ends at the
        // frontier, and an unlocalized rebuild has no dependency width to compute a
        // forward influence from. Both tag EOF, however many repairs the instance is
        // reused for.
        final TestAnchors anchors = new TestAnchors().add(100, 11);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 500, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, 520, 900, UNPRICED);
        Assert.assertTrue(plan.isResumeFromAnchor());
        assertEofHighBound(plan);

        plan.of(anchors, 50, BEGINNING, 10, 10, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);
        Assert.assertFalse(plan.isResumeFromAnchor());
        assertEofHighBound(plan);
    }

    @Test
    public void testLateRowAtExactlyHeadMaxTsIsNotAHeadHit() throws SqlException {
        // The head covers rows up to AND INCLUDING its maxTs while the resume starts
        // strictly above it, so a late row at exactly headMaxTs would be neither
        // covered nor re-read. The strict comparison routes it to the older anchor.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 200, BEGINNING, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(100, plan.getAnchorMaxTs());
        Assert.assertEquals(101, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedFloorsClampToViewLowerBound() throws SqlException {
        // Neither floor may reach below the view's START FROM boundary: rows under it
        // are not the view's, so reading them would warm the state up with data the
        // view never incorporated and replacing from below it would delete a prefix
        // the view does not own. W pulls L under the boundary here; S wins.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 500, 450, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(500, plan.getOutputLowTs());
        Assert.assertEquals(450, plan.getReplayLowTs());

        // A boundary above the late row clamps C itself, so both floors land on the
        // boundary and there is nothing left to localize: reading and re-emitting from
        // S IS the whole-history rebuild, so the executor keeps its established
        // replayMinTs-clamped replacement boundary rather than a redundant R.
        plan.of(new TestAnchors(), 500, 520, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);
        Assert.assertFalse(plan.isLocalized());
        Assert.assertEquals(520, plan.getOutputLowTs());
        Assert.assertEquals(520, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedFloorsFollowTheApplyAheadFloor() throws SqlException {
        // Safety, not optimisation. The rebuild materialises the whole pinned snapshot
        // and then advances the watermark past it, so a back-dated row in the range
        // apply raced past the trigger gets exactly one chance to be read. Floors
        // derived from C alone would sit above it and lose it for good; they follow the
        // retire floor, which already carries min(C, applyAheadMinTs).
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 800, BEGINNING, 7, 10, 600, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isApplyAhead());
        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(800, plan.getCorrectionTs());
        Assert.assertEquals(600, plan.getRetireLowTs());
        Assert.assertEquals(600, plan.getOutputLowTs());
        Assert.assertEquals(500, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedFloorsFromRangeDependency() throws SqlException {
        // The motivating case: no anchor sits below the change, so the repair falls to
        // the boundary rebuild - but a 100-unit RANGE look-behind bounds it anyway. The
        // rebuild reads from C - W and re-emits from C, whatever the view's age.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(500, plan.getCorrectionTs());
        // R = C: the durable frontier sits above the change, so no non-durable output
        // can lower the floor.
        Assert.assertEquals(500, plan.getOutputLowTs());
        // L = R - W: the state a row at R sees is exactly the rows in [R - W, R].
        Assert.assertEquals(400, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedFloorsResetOnPlanReuse() throws SqlException {
        // One plan instance per refresh worker. A repair over a view with no RANGE
        // dependency must not inherit the previous repair's floors, or it would skip
        // history it is required to read.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);
        Assert.assertTrue(plan.isLocalized());

        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);
        Assert.assertFalse(plan.isLocalized());
        Assert.assertEquals(BEGINNING, plan.getOutputLowTs());
        Assert.assertEquals(BEGINNING, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedOutputFloorClampsToDurableFrontier() throws SqlException {
        // The change sits ABOVE the live-view table's frontier, so output between the
        // two exists only in runtime state - a discarded in-RAM lead or a rolled-back
        // draft. C alone would strand it: nothing else would ever re-emit it. R drops
        // to the frontier so the replacement re-materialises it.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 900, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 500, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(900, plan.getCorrectionTs());
        Assert.assertEquals(500, plan.getOutputLowTs());
        Assert.assertEquals(400, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedRebuildRequiresDataTriggerRangeAndDurableOutput() throws SqlException {
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();

        // A non-DATA / recovery trigger carries no timestamp, so there is no C to
        // derive floors from and the whole view has to be rebuilt.
        plan.of(new TestAnchors(), Numbers.LONG_NULL, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);
        assertUnlocalized(plan);

        // No finite RANGE dependency covers every window function: nothing proves how
        // far back the state at R reaches.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);
        assertUnlocalized(plan);

        // The live-view table holds no durable row, so every row the runtime produced
        // is non-durable and the replacement must start at the view boundary.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);
        assertUnlocalized(plan);

        // Apply raced ahead over a range that could not be classified: nothing bounds
        // what changed in it, so no floor may be raised above the view boundary.
        plan.of(new TestAnchors(), 500, BEGINNING, 7, 10, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);
        Assert.assertEquals(Numbers.LONG_NULL, plan.getRetireLowTs());
        assertUnlocalized(plan);
    }

    @Test
    public void testLocalizedScanFloorSaturatesAtBottomOfRange() throws SqlException {
        // A width wider than the distance from R to the bottom of the timestamp range
        // must clamp, not wrap: a wrapped floor would sit ABOVE R and the scan would
        // skip the warm-up entirely. The output floor stays localized either way.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), Long.MIN_VALUE + 10, BEGINNING, 9, 9, Numbers.LONG_NULL, Long.MAX_VALUE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, Long.MIN_VALUE + 50, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(Long.MIN_VALUE + 10, plan.getOutputLowTs());
        Assert.assertEquals(Long.MIN_VALUE, plan.getReplayLowTs());
    }

    @Test
    public void testMaxValueTimestampsStayRepresentable() throws SqlException {
        // Long.MAX_VALUE is real timestamp data. An anchor is always strictly below
        // the change, so anchorMaxTs + 1 cannot overflow even at the top of the
        // range.
        final TestAnchors anchors = new TestAnchors().add(Long.MAX_VALUE - 1, 11);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, Long.MAX_VALUE, BEGINNING, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(Long.MAX_VALUE, plan.getReplayLowTs());
        Assert.assertEquals(Long.MAX_VALUE, plan.getCorrectionTs());
    }

    @Test
    public void testMixedFrameDependenciesDeclineOnAnUnprovenHighBound() throws SqlException {
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();

        // EOF sits above every timestamp, so it is what the union takes when one arm
        // proves no finite bound - and a ROWS function cannot be localized behind it:
        // its frame never expires by time, so a key with no row at or above R keeps
        // state the replay from L never sees. The RANGE arm's own 6_101 is beside the
        // point, because the replacement it would bound re-emits the ROWS function too.
        TestRowsBounds rows = new TestRowsBounds(3_000, HighBoundTag.EOF, Numbers.LONG_NULL);
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, 100, rows, NO_ANCHOR, true, 9_000, 6_000, 9_000, UNPRICED);
        Assert.assertEquals(1, rows.discoveries);
        assertRowsRebuildIsUnlocalized(plan);

        // The same refusal from the other side: the change tops out where
        // changeMaxTs + W leaves the timestamp domain, so the RANGE arm names no bound.
        // Alone it would still localize its floor and read the tail out; beside a ROWS
        // arm it cannot, because the tail is what the ROWS runtime would be promoted from.
        rows = new TestRowsBounds(Long.MAX_VALUE - 300, HighBoundTag.FINITE, Long.MAX_VALUE - 50);
        plan.of(new TestAnchors(), Long.MAX_VALUE - 200, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, rows, NO_ANCHOR, true, Long.MAX_VALUE, Long.MAX_VALUE - 100, Long.MAX_VALUE, UNPRICED);
        Assert.assertEquals(1, rows.discoveries);
        Assert.assertFalse(plan.isLocalized());
        assertEofHighBound(plan);
        Assert.assertEquals(BEGINNING, plan.getOutputLowTs());
        Assert.assertEquals(BEGINNING, plan.getReplayLowTs());

        // And the insert-only proof the ROWS arm needs gates the whole union, not just
        // its own half: a deleting change set leaves the RANGE arm's bounds correct and
        // the ROWS arm's affected key domain unknowable, so nothing runs.
        rows = new TestRowsBounds(3_000, HighBoundTag.FINITE, 7_000);
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, 100, rows, NO_ANCHOR, NOT_INSERT_ONLY, 9_000, 6_000, 9_000, UNPRICED);
        Assert.assertEquals(0, rows.discoveries);
        assertRowsRebuildIsUnlocalized(plan);
    }

    @Test
    public void testMixedFrameDependenciesUnionTheirBounds() throws SqlException {
        // A factory holding both a bounded RANGE and a bounded ROWS window. Each plan
        // bounds the functions of its own kind, and the union takes the earliest L and
        // the latest H: the warm-up then satisfies both frames, and the replacement stops
        // where the later of the two converges.
        //
        // S=1_000, C=5_000, D=9_000 -> R = 5_000. The RANGE arm proves L = R - W = 4_000
        // and H = changeMaxTs + W + 1 = 7_001; the discovery answers L = 3_000 and
        // H = 6_500 for the same floor.
        final TestRowsBounds rows = new TestRowsBounds(3_000, HighBoundTag.FINITE, 6_500);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, 1_000, rows, NO_ANCHOR, true, 9_000, 6_000, 9_000, UNPRICED);

        Assert.assertEquals(1, rows.discoveries);
        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(3_000, plan.getReplayLowTs());
        Assert.assertEquals(5_000, plan.getOutputLowTs());
        Assert.assertEquals(HighBoundTag.FINITE, plan.getHighBoundTag());
        Assert.assertEquals(7_001, plan.getHighTsExclusive());
        Assert.assertTrue(plan.isRuntimeStatePreserved());

        // Widen the ROWS answer past the RANGE arm's and the union follows it instead:
        // neither arm is preferred, only the outer bound of the two is kept.
        final TestRowsBounds deeperRows = new TestRowsBounds(4_500, HighBoundTag.FINITE, 8_000);
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, 1_000, deeperRows, NO_ANCHOR, true, 9_000, 6_000, 9_000, UNPRICED);
        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(4_000, plan.getReplayLowTs());
        Assert.assertEquals(8_000, plan.getHighTsExclusive());
    }

    @Test
    public void testNonDataTriggerRebuildsAndRetiresEverything() throws SqlException {
        // A non-DATA / recovery trigger carries no timestamp: it authorises no
        // deletion, cannot bound which anchors it unsealed, and never searches the
        // ring. Restart restore, metadata drift and WAL-loss re-derive all land here.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, Numbers.LONG_NULL, BEGINNING, 4, 8, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getCorrectionTs());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getRetireLowTs());
        Assert.assertEquals(8, plan.getCommitSeqTxn());
        Assert.assertEquals(0, anchors.searches);
    }

    @Test
    public void testPlanIsReusableAcrossRepairs() throws SqlException {
        // One plan instance per refresh worker: a second repair must not inherit
        // the first one's anchor.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 500, BEGINNING, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);
        Assert.assertTrue(plan.isResumeFromAnchor());

        plan.of(anchors, 50, BEGINNING, 10, 10, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);
        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getAnchorCheckpointId());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getAnchorMaxTs());
        Assert.assertEquals(50, plan.getCorrectionTs());
    }

    @Test
    public void testReplayLowTsNeverFallsBelowViewLowerBound() throws SqlException {
        // An anchor is only ever written from seeded or drained output, so it sits
        // at or above the boundary. The clamp keeps the property local: no anchor,
        // however it was written, can pull the scan below the boundary.
        final TestAnchors anchors = new TestAnchors().add(100, 11);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(1_000, plan.getReplayLowTs());
    }

    @Test
    public void testResumeOutputFloorEqualsScanFloorAndNeverLocalizes() throws SqlException {
        // A resume restores the anchor's state instead of warming one up, so every row
        // it reads is a row it emits: L and R coincide. Localization is a property of
        // the boundary rebuild only - the plan must not raise a resume's floors even
        // when the view carries a RANGE dependency, since the anchor already bounds it.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(300, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 500, BEGINNING, 9, 9, Numbers.LONG_NULL, 100, NO_ROWS, NO_ANCHOR, NOT_INSERT_ONLY, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER, UNPRICED);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertFalse(plan.isLocalized());
        Assert.assertEquals(301, plan.getReplayLowTs());
        Assert.assertEquals(301, plan.getOutputLowTs());
    }

    @Test
    public void testRowsDependencyLocalizesFromTheDiscoveredBounds() throws SqlException {
        // S=1_000, C=5_000, D=9_000 -> R = 5_000. The discovery answers L=3_000 and
        // H=7_000 for that floor, and the plan adopts both: the rebuild reads from
        // 3_000, re-emits from 5_000, and stops at 7_000 instead of the tail.
        final TestRowsBounds rows = new TestRowsBounds(3_000, HighBoundTag.FINITE, 7_000);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, true, 9_000, 6_000, 9_000, UNPRICED);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(3_000, plan.getReplayLowTs());
        Assert.assertEquals(5_000, plan.getOutputLowTs());
        Assert.assertEquals(HighBoundTag.FINITE, plan.getHighBoundTag());
        Assert.assertEquals(7_000, plan.getHighTsExclusive());
        // The scan's inclusive high bound is one below the exclusive convergence
        // boundary, and a finite bound is what puts the pre-repair runtime back.
        Assert.assertEquals(6_999, plan.getScanHighTsInclusive());
        Assert.assertTrue(plan.isRuntimeStatePreserved());
        // The discovery searched from R, over the change interval the plan derived.
        Assert.assertEquals(1, rows.discoveries);
        Assert.assertEquals(1_000, rows.viewLowerBoundTs);
        Assert.assertEquals(5_000, rows.outputLowTs);
        Assert.assertEquals(5_000, rows.changeLowTs);
        Assert.assertEquals(6_000, rows.changeMaxTs);
    }

    @Test
    public void testRowsDiscoveryDeclinesWithoutAFiniteHighBound() throws SqlException {
        // A ROWS frame holds a key's last Nmax rows however old they are, so a key with
        // no row at or above R keeps state a replay from L never reconstructs. Only a
        // finite H puts the pre-repair runtime state back over the replay's, so an EOF
        // answer declines the whole localization rather than raising the floors alone
        // and promoting a runtime that has lost those keys.
        final TestRowsBounds rows = new TestRowsBounds(3_000, HighBoundTag.EOF, Numbers.LONG_NULL);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, true, 9_000, 6_000, 9_000, UNPRICED);

        Assert.assertEquals(1, rows.discoveries);
        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertFalse(plan.isLocalized());
        Assert.assertEquals(1_000, plan.getOutputLowTs());
        Assert.assertEquals(1_000, plan.getReplayLowTs());
        assertEofHighBound(plan);
    }

    @Test
    public void testRowsDiscoveryRunsOnlyForARepairThatCouldUseItsAnswer() throws SqlException {
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();

        // Not provably insert-only. The discovery reads the affected key domain off the
        // post-change snapshot, so a deletion could have emptied a key out of the change
        // interval - leaving it invisible while its later rows still need repairing.
        TestRowsBounds rows = new TestRowsBounds(3_000, HighBoundTag.FINITE, 7_000);
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, NOT_INSERT_ONLY, 9_000, 6_000, 9_000, UNPRICED);
        Assert.assertEquals(0, rows.discoveries);
        assertRowsRebuildIsUnlocalized(plan);

        // No change ceiling: nothing says which interval the affected keys are in.
        rows = new TestRowsBounds(3_000, HighBoundTag.FINITE, 7_000);
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, true, 9_000, NO_CHANGE_MAX_TS, 9_000, UNPRICED);
        Assert.assertEquals(0, rows.discoveries);
        assertRowsRebuildIsUnlocalized(plan);

        // No runtime frontier: the repair cannot put the window state back, so it must
        // not stop short of the tail whatever the data proves.
        rows = new TestRowsBounds(3_000, HighBoundTag.FINITE, 7_000);
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, true, 9_000, 6_000, NO_RUNTIME_FRONTIER, UNPRICED);
        Assert.assertEquals(0, rows.discoveries);
        assertRowsRebuildIsUnlocalized(plan);

        // The live-view table holds no durable row, so R collapses to S and the rebuild
        // re-emits the whole history whatever L says.
        rows = new TestRowsBounds(3_000, HighBoundTag.FINITE, 7_000);
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, true, NO_DURABLE_OUTPUT, 6_000, 9_000, UNPRICED);
        Assert.assertEquals(0, rows.discoveries);
        assertRowsRebuildIsUnlocalized(plan);

        // A resume is bounded by its anchor, so the discovery is never consulted.
        rows = new TestRowsBounds(3_000, HighBoundTag.FINITE, 7_000);
        plan.of(new TestAnchors().add(2_000, 11), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, true, 9_000, 6_000, 9_000, UNPRICED);
        Assert.assertEquals(0, rows.discoveries);
        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertFalse(plan.isLocalized());
    }

    @Test
    public void testRowsHighBoundMustClearTheRuntimeFrontier() throws SqlException {
        // The frontier test is the RANGE path's, unchanged: a bound above where the
        // runtime state stands does not prove the change sits outside the frame that
        // state holds, so the repair reads the tail out and promotes what the replay
        // ends on instead of restoring what it entered with.
        final TestRowsBounds rows = new TestRowsBounds(3_000, HighBoundTag.FINITE, 7_000);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, true, 9_000, 6_000, 6_500, UNPRICED);

        Assert.assertEquals(1, rows.discoveries);
        assertRowsRebuildIsUnlocalized(plan);
    }

    @Test
    public void testRowsScanFloorIsClampedIntoTheReplacementRange() throws SqlException {
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();

        // A key that ran out of history pins L to S, which is the answer rather than a
        // give-up: the whole view history has been seen. The rebuild reads it all and
        // still re-emits only [R, H).
        TestRowsBounds rows = new TestRowsBounds(1_000, HighBoundTag.FINITE, 7_000);
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, true, 9_000, 6_000, 9_000, UNPRICED);
        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(1_000, plan.getReplayLowTs());
        Assert.assertEquals(5_000, plan.getOutputLowTs());

        // No key in Q needs warm-up, so the discovery reports R itself: the two floors
        // coincide and the replay emits every row it reads.
        rows = new TestRowsBounds(5_000, HighBoundTag.FINITE, 7_000);
        plan.of(new TestAnchors(), 5_000, 1_000, 9, 9, Numbers.LONG_NULL, NO_RANGE, rows, NO_ANCHOR, true, 9_000, 6_000, 9_000, UNPRICED);
        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(5_000, plan.getReplayLowTs());
        Assert.assertEquals(5_000, plan.getOutputLowTs());
    }

    @Test
    public void testRowsSearchIntervalIsClampedToTheViewBoundary() throws SqlException {
        // Apply raced ahead and its lowest in-view timestamp (400) drops the retire
        // floor below the view's boundary (1_000). R clamps up to the boundary, which
        // leaves nothing to localize - and the change interval the discovery would have
        // searched is clamped the same way, since a row below the boundary is not the
        // view's row and marks no key affected.
        final TestRowsBounds rows = new TestRowsBounds(1_000, HighBoundTag.FINITE, 7_000);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 5_000, 1_000, 7, 9, 400, NO_RANGE, rows, NO_ANCHOR, true, 9_000, 6_000, 9_000, UNPRICED);

        Assert.assertEquals(400, plan.getRetireLowTs());
        Assert.assertEquals(0, rows.discoveries);
        assertRowsRebuildIsUnlocalized(plan);
    }

    @Test
    public void testApplyAheadClassificationIsRequiredOnlyWhenItCanChangeThePlan() {
        // No gap: nothing to classify.
        Assert.assertFalse(LiveViewCheckpointRepairPlan.isApplyAheadClassificationRequired(100, 7, 7));
        // Gap with a DATA trigger: the ahead range can lower the retire floor and
        // reject the anchor.
        Assert.assertTrue(LiveViewCheckpointRepairPlan.isApplyAheadClassificationRequired(100, 7, 8));
        // Gap with a non-DATA trigger: the plan already retires everything and
        // rebuilds, so reading the base WAL-E would change nothing.
        Assert.assertFalse(LiveViewCheckpointRepairPlan.isApplyAheadClassificationRequired(Numbers.LONG_NULL, 7, 8));
    }

    /**
     * An anchored rebuild that declined localization: both floors back at the view
     * boundary and the tail read out, which is what an anchored view's repair did before
     * the segment bounded it.
     */
    private static void assertAnchorRebuildIsUnlocalized(LiveViewCheckpointRepairPlan plan) {
        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertFalse(plan.isLocalized());
        Assert.assertEquals(1_000, plan.getOutputLowTs());
        Assert.assertEquals(1_000, plan.getReplayLowTs());
        assertEofHighBound(plan);
    }

    private static void assertEofHighBound(LiveViewCheckpointRepairPlan plan) {
        Assert.assertTrue(plan.isHighBoundEof());
        Assert.assertEquals(HighBoundTag.EOF, plan.getHighBoundTag());
        // No timestamp can express the bound, so none is offered as one.
        Assert.assertEquals(Numbers.LONG_NULL, plan.getHighTsExclusive());
        // The bounded forward cursor takes an INCLUSIVE high, and Long.MAX_VALUE as
        // an inclusive bound admits every row - up to one sitting at the very top of
        // the range, which the same value as an exclusive bound would drop. That
        // asymmetry is the whole reason the bound is tagged rather than a long.
        Assert.assertEquals(Long.MAX_VALUE, plan.getScanHighTsInclusive());
    }

    /**
     * A ROWS rebuild that declined localization: both floors back at the view boundary
     * and the tail read out, which is what the repair did before the discovery existed.
     */
    private static void assertRowsRebuildIsUnlocalized(LiveViewCheckpointRepairPlan plan) {
        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertFalse(plan.isLocalized());
        Assert.assertEquals(1_000, plan.getOutputLowTs());
        Assert.assertEquals(1_000, plan.getReplayLowTs());
        assertEofHighBound(plan);
    }

    private static void assertUnlocalized(LiveViewCheckpointRepairPlan plan) {
        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertFalse(plan.isLocalized());
        // Both floors collapse to the view boundary, which is the whole-history
        // rebuild the executor ran before any dependency was derived.
        Assert.assertEquals(BEGINNING, plan.getOutputLowTs());
        Assert.assertEquals(BEGINNING, plan.getReplayLowTs());
    }

    /**
     * Sealed anchors in ascending {@code maxTs} order, newest last - the ordering
     * {@link io.questdb.cairo.lv.LiveViewInstance} maintains for the retained
     * checkpoint ring. Counts searches so a test can prove the plan skipped one.
     */
    private static class TestAnchors implements LiveViewCheckpointRepairPlan.AnchorSource {
        private final LongList checkpointIds = new LongList();
        private final LongList maxTimestamps = new LongList();
        private int searches;

        public TestAnchors add(long maxTs, long checkpointId) {
            maxTimestamps.add(maxTs);
            checkpointIds.add(checkpointId);
            return this;
        }

        @Override
        public boolean findAnchorBelow(long ceilTs, @NotNull LiveViewCheckpointTimelineEntry out) {
            searches++;
            for (int i = maxTimestamps.size() - 1; i >= 0; i--) {
                if (maxTimestamps.getQuick(i) < ceilTs) {
                    out.maxTimestamp = maxTimestamps.getQuick(i);
                    out.checkpointId = checkpointIds.getQuick(i);
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * A stand-in for the per-key ROWS discovery: returns bounds the case chose and
     * records what the plan asked it to search for. The discovery's own correctness is
     * pinned over real data elsewhere; what these cases exercise is the plan's half of
     * the contract - when the search runs at all, and what it does with the answer.
     */
    private static class TestRowsBounds implements LiveViewCheckpointRepairPlan.RowsBoundSource {
        private final long dependencyLowTs;
        private final HighBoundTag highBoundTag;
        private final long highTsExclusive;
        private long changeLowTs = Numbers.LONG_NULL;
        private long changeMaxTs = Numbers.LONG_NULL;
        private int discoveries;
        private long outputLowTs = Numbers.LONG_NULL;
        private long viewLowerBoundTs = Numbers.LONG_NULL;

        TestRowsBounds(long dependencyLowTs, HighBoundTag highBoundTag, long highTsExclusive) {
            this.dependencyLowTs = dependencyLowTs;
            this.highBoundTag = highBoundTag;
            this.highTsExclusive = highTsExclusive;
        }

        @Override
        public void discoverRowsBounds(long viewLowerBoundTs, long outputLowTs, long changeLowTs, long changeMaxTs) {
            discoveries++;
            this.viewLowerBoundTs = viewLowerBoundTs;
            this.outputLowTs = outputLowTs;
            this.changeLowTs = changeLowTs;
            this.changeMaxTs = changeMaxTs;
        }

        @Override
        public long getRowsDependencyLowTs() {
            return dependencyLowTs;
        }

        @Override
        public HighBoundTag getRowsHighBoundTag() {
            return highBoundTag;
        }

        @Override
        public long getRowsHighTsExclusive() {
            return highTsExclusive;
        }
    }

    /**
     * A base table holding exactly one row per timestamp unit between the two bounds
     * the case chooses, so a candidate interval costs its own width and every number
     * below is readable off the bounds. The real estimate interpolates over partition
     * metadata and is pinned against real data elsewhere; what these cases exercise is
     * what the plan does with the two numbers it gets back.
     */
    private static class TestScanCost implements LiveViewCheckpointRepairPlan.ScanCostSource {
        private final long tableMaxTs;
        private final long tableMinTs;

        TestScanCost(long tableMinTs, long tableMaxTs) {
            this.tableMinTs = tableMinTs;
            this.tableMaxTs = tableMaxTs;
        }

        @Override
        public long estimateScanRows(long lowTs, long highTsInclusive) {
            final long lo = Math.max(lowTs, tableMinTs);
            final long hi = Math.min(highTsInclusive, tableMaxTs);
            return hi < lo ? 0 : hi - lo + 1;
        }
    }
}

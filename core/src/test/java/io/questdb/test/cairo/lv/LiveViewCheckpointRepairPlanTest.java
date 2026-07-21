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

import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the change-set classification an out-of-order repair plans from: which
 * executor runs, which sealed anchor it may trust, where it commits, and how far
 * down it unseals retained checkpoints. The refresh job derives all of this once
 * per repair against one pinned base snapshot (design section 12.1); these tests
 * exercise that decision directly, without an engine, a base table or a replay.
 * <p>
 * The properties under test are safety properties, not optimisations: an anchor
 * chosen at or above a change would silently drop rows the resume never re-reads,
 * and a retire floor left too high would strand a poisoned anchor for a later
 * repair to resume from.
 */
public class LiveViewCheckpointRepairPlanTest {
    private static final long BEGINNING = Long.MIN_VALUE; // START FROM BEGINNING, = Numbers.LONG_NULL
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
    // The repair has no way to put the runtime window state back, so it must not stop
    // short of the frontier.
    private static final long NO_RUNTIME_FRONTIER = Numbers.LONG_NULL;

    @Test
    public void testApplyAheadKeepsAnchorBelowAheadFloor() {
        // Apply raced 3 seqTxns past the trigger and the lowest in-view timestamp
        // among them (150) sits below the trigger (400). The resume may only
        // anchor below that floor, so the anchor at 300 - fine against the trigger
        // alone - is rejected in favour of the one at 100.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 400, BEGINNING, 7, 10, 13, 300, 150, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertTrue(plan.isApplyAhead());
        Assert.assertEquals(11, plan.getAnchorLvSeqTxn());
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
    public void testApplyAheadRebuildsWhenNoAnchorBelowAheadFloor() {
        // Same ring, but the ahead range reaches down to 50 - below every anchor.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 400, BEGINNING, 7, 10, 13, 300, 50, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(LiveViewCheckpointRepairPlan.DISPOSITION_BOUNDARY_REBUILD, plan.getDisposition());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getAnchorLvSeqTxn());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getAnchorMaxTs());
        Assert.assertEquals(10, plan.getCommitSeqTxn());
        Assert.assertEquals(50, plan.getRetireLowTs());
    }

    @Test
    public void testApplyAheadUnclassifiableRetiresEverything() {
        // A structural or non-DATA commit in the ahead range reports LONG_NULL: the
        // repair cannot bound what it changed, so no anchor survives and the whole
        // ring is retired.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 400, BEGINNING, 7, 10, 13, 300, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getRetireLowTs());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getApplyAheadMinTs());
        Assert.assertEquals(10, plan.getCommitSeqTxn());
    }

    @Test
    public void testApplyAheadWithHigherFloorKeepsCorrectionFloor() {
        // The ahead range's lowest in-view timestamp (900) sits ABOVE the trigger
        // (400), so it cannot lower anything: the retire floor stays at C and the
        // anchor selection is the one the trigger alone would make.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 400, BEGINNING, 7, 9, 13, 300, 900, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(300, plan.getAnchorMaxTs());
        Assert.assertEquals(400, plan.getRetireLowTs());
        Assert.assertEquals(400, plan.getCorrectionTs());
        Assert.assertEquals(9, plan.getCommitSeqTxn());
    }

    @Test
    public void testBoundedMissResumesFromOlderAnchor() {
        // The head (300) sits at or above the late row (300), so it cannot anchor
        // the resume - its state already incorporates rows the change invalidates.
        // The next entry down (200) can.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 300, BEGINNING, 5, 5, 13, 300, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(12, plan.getAnchorLvSeqTxn());
        Assert.assertEquals(200, plan.getAnchorMaxTs());
        Assert.assertEquals(201, plan.getReplayLowTs());
        Assert.assertEquals(300, plan.getRetireLowTs());
        // One search only: no apply-ahead gap to re-anchor against.
        Assert.assertEquals(1, anchors.searches);
    }

    @Test
    public void testChangeBelowWholeRingRebuilds() {
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 50, BEGINNING, 5, 5, 13, 300, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(5, plan.getCommitSeqTxn());
        Assert.assertEquals(50, plan.getCorrectionTs());
        Assert.assertEquals(50, plan.getRetireLowTs());
        // A rebuild scans from the view boundary.
        Assert.assertEquals(BEGINNING, plan.getReplayLowTs());
    }

    @Test
    public void testCorrectionFloorClampsToViewLowerBound() {
        // A commit routinely reaches below the view's START FROM boundary; those
        // rows are simply not the view's, so C clamps up to the boundary. Both the
        // delete authority and the retire floor follow it.
        final TestAnchors anchors = new TestAnchors();
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 10, 1_000, 5, 5, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertEquals(1_000, plan.getCorrectionTs());
        Assert.assertEquals(1_000, plan.getRetireLowTs());
        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(1_000, plan.getReplayLowTs());
    }

    @Test
    public void testFiniteHighBoundClampsAboveTheOutputFloor() {
        // Defensive. The refresh job cannot produce this pair - the change ceiling is
        // the maximum of a set that contains the trigger timestamp, and R never rises
        // above that timestamp - but the plan is handed both numbers separately and a
        // replacement's high bound has to sit strictly above its low bound or the
        // interval is empty or inverted. A change ceiling under R clamps to R + 1,
        // which re-emits the single timestamp group at R: sound, because the replay
        // reproduces it identically.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 1_000, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 10, 5_000, 500, 5_000);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(1_000, plan.getOutputLowTs());
        Assert.assertEquals(HighBoundTag.FINITE, plan.getHighBoundTag());
        Assert.assertEquals(1_001, plan.getHighTsExclusive());
        Assert.assertEquals(1_000, plan.getScanHighTsInclusive());
    }

    @Test
    public void testFiniteHighBoundFromRangeDependency() {
        // The step-4b win, on the shape 4a bounded from below. No anchor sits below the
        // change, so the repair takes the boundary rebuild - but a 100-unit RANGE
        // look-behind closes it at both ends: a row at m sits in the frame of every row
        // in [m, m + W] and no other, so a change topping out at 520 cannot reach output
        // at or above 621.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 900, 520, 900);

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
    public void testFiniteHighBoundRequiresBoundedChangeAndPreservableRuntime() {
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();

        // No change ceiling: a non-DATA or structural entry in the incorporated range
        // can change rows anywhere, so no arithmetic over the inserted timestamps
        // bounds it.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 900, NO_CHANGE_MAX_TS, 900);
        Assert.assertTrue(plan.isLocalized());
        assertEofHighBound(plan);
        Assert.assertFalse(plan.isRuntimeStatePreserved());

        // No way to put the runtime state back afterwards, so the repair must run all
        // the way to the frontier and keep what the replay produced.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 900, 520, NO_RUNTIME_FRONTIER);
        assertEofHighBound(plan);

        // The frontier sits BELOW H, so the change does reach the frame the runtime
        // holds. Stopping at H would leave the runtime describing a boundary its own
        // state has already passed.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 900, 520, 600);
        assertEofHighBound(plan);

        // Output above the durable frontier exists only in an un-flushed lead or a
        // rolled-back draft. A replacement stopping at H would neither re-emit it nor
        // leave it on disk, so it would be lost while the watermark advanced past the
        // base rows behind it.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 800, 520, 900);
        Assert.assertTrue(plan.isLocalized());
        assertEofHighBound(plan);

        // No finite RANGE dependency: the rebuild is unlocalized, and a width it does
        // not have is the width H would be measured in.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, NO_RANGE, 900, 520, 900);
        assertEofHighBound(plan);
    }

    @Test
    public void testFiniteHighBoundSaturatesAtTopOfRange() {
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();

        // changeMaxTs + W wraps. A change that late converges nowhere the arithmetic
        // can name, so the repair reads the tail out.
        plan.of(
                new TestAnchors(),
                Long.MAX_VALUE - 20,
                BEGINNING,
                9,
                9,
                Numbers.LONG_NULL,
                Numbers.LONG_NULL,
                Numbers.LONG_NULL,
                100,
                Long.MAX_VALUE - 5,
                Long.MAX_VALUE - 10,
                Long.MAX_VALUE - 5
        );
        Assert.assertTrue(plan.isLocalized());
        assertEofHighBound(plan);

        // The sum lands exactly on Long.MAX_VALUE. It does not wrap, but its exclusive
        // successor is not representable - which is precisely why the bound is tagged
        // rather than spelled as a timestamp, since Long.MAX_VALUE is real data.
        plan.of(
                new TestAnchors(),
                Long.MAX_VALUE - 200,
                BEGINNING,
                9,
                9,
                Numbers.LONG_NULL,
                Numbers.LONG_NULL,
                Numbers.LONG_NULL,
                100,
                Long.MAX_VALUE,
                Long.MAX_VALUE - 100,
                Long.MAX_VALUE
        );
        Assert.assertTrue(plan.isLocalized());
        assertEofHighBound(plan);
    }

    @Test
    public void testHeadHitResumesFromHead() {
        // The head is the newest sealed anchor, so a head strictly below the late
        // row anchors the resume directly - no ring search at all.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 500, BEGINNING, 9, 9, 12, 200, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(12, plan.getAnchorLvSeqTxn());
        Assert.assertEquals(200, plan.getAnchorMaxTs());
        Assert.assertEquals(201, plan.getReplayLowTs());
        Assert.assertEquals(9, plan.getCommitSeqTxn());
        Assert.assertEquals(500, plan.getCorrectionTs());
        Assert.assertEquals(500, plan.getRetireLowTs());
        Assert.assertFalse(plan.isApplyAhead());
        Assert.assertEquals(0, anchors.searches);
    }

    @Test
    public void testHeadWithNoMaxTsCannotAnchor() {
        // A head with no maxTs would floor the replay at LONG_NULL + 1 and admit
        // every base row, including rows below the view's boundary. It is treated
        // as no head at all, and the ring search takes over.
        final TestAnchors anchors = new TestAnchors().add(100, 11);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 500, BEGINNING, 9, 9, 12, Numbers.LONG_NULL, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(11, plan.getAnchorLvSeqTxn());
        Assert.assertEquals(100, plan.getAnchorMaxTs());
        Assert.assertEquals(1, anchors.searches);
    }

    @Test
    public void testHighBoundIsTaggedEofForResumeAndUnlocalizedRebuild() {
        // H is the tagged exclusive bound above which the repair may not read. Only a
        // localized rebuild derives a finite one: a resume is already bounded below by
        // its anchor and keeps replaying the whole tail so the runtime ends at the
        // frontier, and an unlocalized rebuild has no dependency width to compute a
        // forward influence from. Both tag EOF, however many repairs the instance is
        // reused for.
        final TestAnchors anchors = new TestAnchors().add(100, 11);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 500, BEGINNING, 9, 9, 11, 100, Numbers.LONG_NULL, 100, 900, 520, 900);
        Assert.assertTrue(plan.isResumeFromAnchor());
        assertEofHighBound(plan);

        plan.of(anchors, 50, BEGINNING, 10, 10, 11, 100, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);
        Assert.assertFalse(plan.isResumeFromAnchor());
        assertEofHighBound(plan);
    }

    @Test
    public void testLateRowAtExactlyHeadMaxTsIsNotAHeadHit() {
        // The head covers rows up to AND INCLUDING its maxTs while the resume starts
        // strictly above it, so a late row at exactly headMaxTs would be neither
        // covered nor re-read. The strict comparison routes it to the older anchor.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 200, BEGINNING, 9, 9, 12, 200, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(100, plan.getAnchorMaxTs());
        Assert.assertEquals(101, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedFloorsClampToViewLowerBound() {
        // Neither floor may reach below the view's START FROM boundary: rows under it
        // are not the view's, so reading them would warm the state up with data the
        // view never incorporated and replacing from below it would delete a prefix
        // the view does not own. W pulls L under the boundary here; S wins.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 500, 450, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(500, plan.getOutputLowTs());
        Assert.assertEquals(450, plan.getReplayLowTs());

        // A boundary above the late row clamps C itself, so both floors land on the
        // boundary and there is nothing left to localize: reading and re-emitting from
        // S IS the whole-history rebuild, so the executor keeps its established
        // replayMinTs-clamped replacement boundary rather than a redundant R.
        plan.of(new TestAnchors(), 500, 520, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);
        Assert.assertFalse(plan.isLocalized());
        Assert.assertEquals(520, plan.getOutputLowTs());
        Assert.assertEquals(520, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedFloorsFollowTheApplyAheadFloor() {
        // Safety, not optimisation. The rebuild materialises the whole pinned snapshot
        // and then advances the watermark past it, so a back-dated row in the range
        // apply raced past the trigger gets exactly one chance to be read. Floors
        // derived from C alone would sit above it and lose it for good; they follow the
        // retire floor, which already carries min(C, applyAheadMinTs).
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 800, BEGINNING, 7, 10, Numbers.LONG_NULL, Numbers.LONG_NULL, 600, 100, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isApplyAhead());
        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(800, plan.getCorrectionTs());
        Assert.assertEquals(600, plan.getRetireLowTs());
        Assert.assertEquals(600, plan.getOutputLowTs());
        Assert.assertEquals(500, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedFloorsFromRangeDependency() {
        // The motivating case: no anchor sits below the change, so the repair falls to
        // the boundary rebuild - but a 100-unit RANGE look-behind bounds it anyway. The
        // rebuild reads from C - W and re-emits from C, whatever the view's age.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

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
    public void testLocalizedFloorsResetOnPlanReuse() {
        // One plan instance per refresh worker. A repair over a view with no RANGE
        // dependency must not inherit the previous repair's floors, or it would skip
        // history it is required to read.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);
        Assert.assertTrue(plan.isLocalized());

        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);
        Assert.assertFalse(plan.isLocalized());
        Assert.assertEquals(BEGINNING, plan.getOutputLowTs());
        Assert.assertEquals(BEGINNING, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedOutputFloorClampsToDurableFrontier() {
        // The change sits ABOVE the live-view table's frontier, so output between the
        // two exists only in runtime state - a discarded in-RAM lead or a rolled-back
        // draft. C alone would strand it: nothing else would ever re-emit it. R drops
        // to the frontier so the replacement re-materialises it.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(new TestAnchors(), 900, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 500, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(900, plan.getCorrectionTs());
        Assert.assertEquals(500, plan.getOutputLowTs());
        Assert.assertEquals(400, plan.getReplayLowTs());
    }

    @Test
    public void testLocalizedRebuildRequiresDataTriggerRangeAndDurableOutput() {
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();

        // A non-DATA / recovery trigger carries no timestamp, so there is no C to
        // derive floors from and the whole view has to be rebuilt.
        plan.of(new TestAnchors(), Numbers.LONG_NULL, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);
        assertUnlocalized(plan);

        // No finite RANGE dependency covers every window function: nothing proves how
        // far back the state at R reaches.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, NO_RANGE, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);
        assertUnlocalized(plan);

        // The live-view table holds no durable row, so every row the runtime produced
        // is non-durable and the replacement must start at the view boundary.
        plan.of(new TestAnchors(), 500, BEGINNING, 9, 9, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);
        assertUnlocalized(plan);

        // Apply raced ahead over a range that could not be classified: nothing bounds
        // what changed in it, so no floor may be raised above the view boundary.
        plan.of(new TestAnchors(), 500, BEGINNING, 7, 10, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, 100, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);
        Assert.assertEquals(Numbers.LONG_NULL, plan.getRetireLowTs());
        assertUnlocalized(plan);
    }

    @Test
    public void testLocalizedScanFloorSaturatesAtBottomOfRange() {
        // A width wider than the distance from R to the bottom of the timestamp range
        // must clamp, not wrap: a wrapped floor would sit ABOVE R and the scan would
        // skip the warm-up entirely. The output floor stays localized either way.
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(
                new TestAnchors(),
                Long.MIN_VALUE + 10,
                BEGINNING,
                9,
                9,
                Numbers.LONG_NULL,
                Numbers.LONG_NULL,
                Numbers.LONG_NULL,
                Long.MAX_VALUE,
                Long.MIN_VALUE + 50,
                NO_CHANGE_MAX_TS,
                NO_RUNTIME_FRONTIER
        );

        Assert.assertTrue(plan.isLocalized());
        Assert.assertEquals(Long.MIN_VALUE + 10, plan.getOutputLowTs());
        Assert.assertEquals(Long.MIN_VALUE, plan.getReplayLowTs());
    }

    @Test
    public void testMaxValueTimestampsStayRepresentable() {
        // Long.MAX_VALUE is real timestamp data. An anchor is always strictly below
        // the change, so anchorMaxTs + 1 cannot overflow even at the top of the
        // range.
        final TestAnchors anchors = new TestAnchors().add(Long.MAX_VALUE - 1, 11);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, Long.MAX_VALUE, BEGINNING, 9, 9, 11, Long.MAX_VALUE - 1, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(Long.MAX_VALUE, plan.getReplayLowTs());
        Assert.assertEquals(Long.MAX_VALUE, plan.getCorrectionTs());
    }

    @Test
    public void testNonDataTriggerRebuildsAndRetiresEverything() {
        // A non-DATA / recovery trigger carries no timestamp: it authorises no
        // deletion, cannot bound which anchors it unsealed, and never searches the
        // ring. Restart restore, metadata drift and WAL-loss re-derive all land here.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, Numbers.LONG_NULL, BEGINNING, 4, 8, 12, 200, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getCorrectionTs());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getRetireLowTs());
        Assert.assertEquals(8, plan.getCommitSeqTxn());
        Assert.assertEquals(0, anchors.searches);
    }

    @Test
    public void testPlanIsReusableAcrossRepairs() {
        // One plan instance per refresh worker: a second repair must not inherit
        // the first one's anchor.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 500, BEGINNING, 9, 9, 12, 200, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);
        Assert.assertTrue(plan.isResumeFromAnchor());

        plan.of(anchors, 50, BEGINNING, 10, 10, 12, 200, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);
        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getAnchorLvSeqTxn());
        Assert.assertEquals(Numbers.LONG_NULL, plan.getAnchorMaxTs());
        Assert.assertEquals(50, plan.getCorrectionTs());
    }

    @Test
    public void testReplayLowTsNeverFallsBelowViewLowerBound() {
        // An anchor is only ever written from seeded or drained output, so it sits
        // at or above the boundary. The clamp keeps the property local: no anchor,
        // however it was written, can pull the scan below the boundary.
        final TestAnchors anchors = new TestAnchors().add(100, 11);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 5_000, 1_000, 9, 9, 11, 100, Numbers.LONG_NULL, NO_RANGE, NO_DURABLE_OUTPUT, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(1_000, plan.getReplayLowTs());
    }

    @Test
    public void testResumeOutputFloorEqualsScanFloorAndNeverLocalizes() {
        // A resume restores the anchor's state instead of warming one up, so every row
        // it reads is a row it emits: L and R coincide. Localization is a property of
        // the boundary rebuild only - the plan must not raise a resume's floors even
        // when the view carries a RANGE dependency, since the anchor already bounds it.
        final TestAnchors anchors = new TestAnchors().add(100, 11);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 500, BEGINNING, 9, 9, 12, 300, Numbers.LONG_NULL, 100, 900, NO_CHANGE_MAX_TS, NO_RUNTIME_FRONTIER);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertFalse(plan.isLocalized());
        Assert.assertEquals(301, plan.getReplayLowTs());
        Assert.assertEquals(301, plan.getOutputLowTs());
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
        private final LongList lvSeqTxns = new LongList();
        private final LongList maxTimestamps = new LongList();
        private int searches;

        public TestAnchors add(long maxTs, long lvSeqTxn) {
            maxTimestamps.add(maxTs);
            lvSeqTxns.add(lvSeqTxn);
            return this;
        }

        @Override
        public int findAnchorBelow(long ceilTs) {
            searches++;
            for (int i = maxTimestamps.size() - 1; i >= 0; i--) {
                if (maxTimestamps.getQuick(i) < ceilTs) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public long getAnchorLvSeqTxn(int index) {
            return lvSeqTxns.getQuick(index);
        }

        @Override
        public long getAnchorMaxTs(int index) {
            return maxTimestamps.getQuick(index);
        }
    }
}

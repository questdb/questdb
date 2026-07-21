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

    @Test
    public void testApplyAheadKeepsAnchorBelowAheadFloor() {
        // Apply raced 3 seqTxns past the trigger and the lowest in-view timestamp
        // among them (150) sits below the trigger (400). The resume may only
        // anchor below that floor, so the anchor at 300 - fine against the trigger
        // alone - is rejected in favour of the one at 100.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(300, 13);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 400, BEGINNING, 7, 10, 13, 300, 150);

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
        plan.of(anchors, 400, BEGINNING, 7, 10, 13, 300, 50);

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
        plan.of(anchors, 400, BEGINNING, 7, 10, 13, 300, Numbers.LONG_NULL);

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
        plan.of(anchors, 400, BEGINNING, 7, 9, 13, 300, 900);

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
        plan.of(anchors, 300, BEGINNING, 5, 5, 13, 300, Numbers.LONG_NULL);

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
        plan.of(anchors, 50, BEGINNING, 5, 5, 13, 300, Numbers.LONG_NULL);

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
        plan.of(anchors, 10, 1_000, 5, 5, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);

        Assert.assertEquals(1_000, plan.getCorrectionTs());
        Assert.assertEquals(1_000, plan.getRetireLowTs());
        Assert.assertFalse(plan.isResumeFromAnchor());
        Assert.assertEquals(1_000, plan.getReplayLowTs());
    }

    @Test
    public void testHeadHitResumesFromHead() {
        // The head is the newest sealed anchor, so a head strictly below the late
        // row anchors the resume directly - no ring search at all.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 500, BEGINNING, 9, 9, 12, 200, Numbers.LONG_NULL);

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
        plan.of(anchors, 500, BEGINNING, 9, 9, 12, Numbers.LONG_NULL, Numbers.LONG_NULL);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(11, plan.getAnchorLvSeqTxn());
        Assert.assertEquals(100, plan.getAnchorMaxTs());
        Assert.assertEquals(1, anchors.searches);
    }

    @Test
    public void testLateRowAtExactlyHeadMaxTsIsNotAHeadHit() {
        // The head covers rows up to AND INCLUDING its maxTs while the resume starts
        // strictly above it, so a late row at exactly headMaxTs would be neither
        // covered nor re-read. The strict comparison routes it to the older anchor.
        final TestAnchors anchors = new TestAnchors().add(100, 11).add(200, 12);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, 200, BEGINNING, 9, 9, 12, 200, Numbers.LONG_NULL);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(100, plan.getAnchorMaxTs());
        Assert.assertEquals(101, plan.getReplayLowTs());
    }

    @Test
    public void testMaxValueTimestampsStayRepresentable() {
        // Long.MAX_VALUE is real timestamp data. An anchor is always strictly below
        // the change, so anchorMaxTs + 1 cannot overflow even at the top of the
        // range.
        final TestAnchors anchors = new TestAnchors().add(Long.MAX_VALUE - 1, 11);
        final LiveViewCheckpointRepairPlan plan = new LiveViewCheckpointRepairPlan();
        plan.of(anchors, Long.MAX_VALUE, BEGINNING, 9, 9, 11, Long.MAX_VALUE - 1, Numbers.LONG_NULL);

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
        plan.of(anchors, Numbers.LONG_NULL, BEGINNING, 4, 8, 12, 200, Numbers.LONG_NULL);

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
        plan.of(anchors, 500, BEGINNING, 9, 9, 12, 200, Numbers.LONG_NULL);
        Assert.assertTrue(plan.isResumeFromAnchor());

        plan.of(anchors, 50, BEGINNING, 10, 10, 12, 200, Numbers.LONG_NULL);
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
        plan.of(anchors, 5_000, 1_000, 9, 9, 11, 100, Numbers.LONG_NULL);

        Assert.assertTrue(plan.isResumeFromAnchor());
        Assert.assertEquals(1_000, plan.getReplayLowTs());
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

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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.Disposition;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.HighBoundTag;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.RepairPublicationStage;
import org.junit.Assert;
import org.junit.Test;

import java.util.EnumMap;
import java.util.Map;

/**
 * Freezes the Phase 0 contracts pinned by {@link LiveViewCheckpointContracts} so
 * that a later phase cannot silently drift the supported-window matrix, the repair
 * publication ordering, or the floating-point tolerance while wiring up the
 * versioned checkpoint timeline. Each assertion mirrors a decision in
 * {@code LIVE_VIEW_VERSIONED_CHECKPOINT_TIMELINE_DESIGN.md}; changing the contract
 * must be a deliberate edit here, not an accident elsewhere.
 */
public class LiveViewCheckpointContractsTest {

    @Test
    public void testDependencyKindSetIsFrozen() {
        // The matrix has exactly six rows (design section 6). Adding or removing a
        // shape must force a conscious update to this test and the expected-
        // disposition map below.
        Assert.assertEquals(6, DependencyKind.values().length);
        Assert.assertEquals(4, Disposition.values().length);
    }

    @Test
    public void testEligibilityHelpersMatchDisposition() {
        for (DependencyKind kind : DependencyKind.values()) {
            Assert.assertEquals(
                    "isEligibleNow must be true only for ELIGIBLE [kind=" + kind + ']',
                    kind.getDisposition() == Disposition.ELIGIBLE,
                    kind.isEligibleNow()
            );
            Assert.assertEquals(
                    "isRejectedPermanently must be true only for REJECT [kind=" + kind + ']',
                    kind.getDisposition() == Disposition.REJECT,
                    kind.isRejectedPermanently()
            );
            // Every kind documents both derivation strategies.
            Assert.assertNotNull(kind.getLowBoundStrategy());
            Assert.assertNotNull(kind.getHighBoundStrategy());
            Assert.assertFalse(kind.getLowBoundStrategy().isEmpty());
            Assert.assertFalse(kind.getHighBoundStrategy().isEmpty());
        }
    }

    @Test
    public void testFloatingToleranceConstantsAreSmallAndPositive() {
        Assert.assertTrue(LiveViewCheckpointContracts.FLOATING_RELATIVE_TOLERANCE > 0);
        Assert.assertTrue(LiveViewCheckpointContracts.FLOATING_RELATIVE_TOLERANCE < 1e-3);
        Assert.assertTrue(LiveViewCheckpointContracts.FLOATING_ABSOLUTE_TOLERANCE > 0);
        Assert.assertTrue(LiveViewCheckpointContracts.FLOATING_ABSOLUTE_TOLERANCE < 1e-3);
    }

    @Test
    public void testFloatingToleranceExactAndDriftWithinBound() {
        // Exact equality is trivially within tolerance.
        Assert.assertTrue(LiveViewCheckpointContracts.isWithinFloatingTolerance(1.0, 1.0));
        Assert.assertTrue(LiveViewCheckpointContracts.isWithinFloatingTolerance(0.0, 0.0));
        // A one-ULP drift on a large accumulator - the motivating add-then-subtract
        // rounding difference - stays within tolerance.
        final double big = 1.234567890123e12;
        Assert.assertTrue(LiveViewCheckpointContracts.isWithinFloatingTolerance(big, Math.nextUp(big)));
        // A tiny absolute drift near zero is admitted by the absolute floor.
        Assert.assertTrue(LiveViewCheckpointContracts.isWithinFloatingTolerance(0.0, 1e-12));
    }

    @Test
    public void testFloatingToleranceRejectsRealDifference() {
        // A relative difference well beyond the ceiling is a real divergence, not
        // rounding drift.
        Assert.assertFalse(LiveViewCheckpointContracts.isWithinFloatingTolerance(1.0, 1.01));
        Assert.assertFalse(LiveViewCheckpointContracts.isWithinFloatingTolerance(100.0, 101.0));
        Assert.assertFalse(LiveViewCheckpointContracts.isWithinFloatingTolerance(0.0, 1.0));
    }

    @Test
    public void testFloatingToleranceTreatsNonFiniteAsExact() {
        // NaN, infinities, and signed zero round-trip by raw bits in the codec, so
        // the tolerance helper must require exact bit-equality for non-finite values.
        Assert.assertTrue(LiveViewCheckpointContracts.isWithinFloatingTolerance(Double.NaN, Double.NaN));
        Assert.assertTrue(LiveViewCheckpointContracts.isWithinFloatingTolerance(
                Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
        Assert.assertTrue(LiveViewCheckpointContracts.isWithinFloatingTolerance(
                Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));

        Assert.assertFalse(LiveViewCheckpointContracts.isWithinFloatingTolerance(Double.NaN, 0.0));
        Assert.assertFalse(LiveViewCheckpointContracts.isWithinFloatingTolerance(0.0, Double.NaN));
        Assert.assertFalse(LiveViewCheckpointContracts.isWithinFloatingTolerance(
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
        Assert.assertFalse(LiveViewCheckpointContracts.isWithinFloatingTolerance(
                Double.POSITIVE_INFINITY, Double.MAX_VALUE));
    }

    @Test
    public void testHighBoundTagIsTagged() {
        // H must be a tagged bound so Long.MAX_VALUE stays a valid data timestamp
        // rather than a sentinel for infinity (design section 6).
        Assert.assertEquals(2, HighBoundTag.values().length);
        Assert.assertNotNull(HighBoundTag.valueOf("FINITE"));
        Assert.assertNotNull(HighBoundTag.valueOf("EOF"));
    }

    @Test
    public void testRepairPublicationStageOrderingIsFrozen() {
        // Ordinal order is the required happens-before order (design section 12.6).
        final RepairPublicationStage[] expected = {
                RepairPublicationStage.PLAN,
                RepairPublicationStage.CANDIDATE_ROOTS_AND_RUNTIME_READY,
                RepairPublicationStage.LV_WAL_REPLACEMENT_COMMITTED,
                RepairPublicationStage.LV_REPLACEMENT_APPLIED,
                RepairPublicationStage.TIMELINE_GENERATION_PUBLISHED,
                RepairPublicationStage.RUNTIME_TIER_PROMOTED_IF_NEEDED,
                RepairPublicationStage.CONSUMED_WATERMARK_AND_PURGE_FLOOR_ADVANCED,
        };
        Assert.assertArrayEquals(expected, RepairPublicationStage.values());

        // The load-bearing orderings, spelled out so a reorder of the enum body
        // breaks a named assertion and not just the array check above.
        Assert.assertTrue(
                "the WAL replacement must commit before it is applied",
                RepairPublicationStage.LV_WAL_REPLACEMENT_COMMITTED.ordinal()
                        < RepairPublicationStage.LV_REPLACEMENT_APPLIED.ordinal()
        );
        Assert.assertTrue(
                "the replacement must apply before the timeline generation publishes",
                RepairPublicationStage.LV_REPLACEMENT_APPLIED.ordinal()
                        < RepairPublicationStage.TIMELINE_GENERATION_PUBLISHED.ordinal()
        );
        Assert.assertTrue(
                "the timeline must publish before the runtime is promoted",
                RepairPublicationStage.TIMELINE_GENERATION_PUBLISHED.ordinal()
                        < RepairPublicationStage.RUNTIME_TIER_PROMOTED_IF_NEEDED.ordinal()
        );
        Assert.assertTrue(
                "the purge floor must advance last, past all materialized output",
                RepairPublicationStage.RUNTIME_TIER_PROMOTED_IF_NEEDED.ordinal()
                        < RepairPublicationStage.CONSUMED_WATERMARK_AND_PURGE_FLOOR_ADVANCED.ordinal()
        );
    }

    @Test
    public void testUnanchoredRankIsCutButAnchoredSegmentStaysEligible() {
        // The Phase 0 scope cut: unanchored row_number/rank/dense_rank are rejected
        // with no finite H, while their anchored, segment-reset forms remain proven
        // and return in Phase 7.
        Assert.assertEquals(Disposition.REJECT, DependencyKind.UNANCHORED_RANK.getDisposition());
        Assert.assertTrue(DependencyKind.UNANCHORED_RANK.isRejectedPermanently());
        Assert.assertFalse(DependencyKind.UNANCHORED_RANK.isEligibleNow());

        Assert.assertEquals(Disposition.ELIGIBLE_PHASE_7, DependencyKind.FIXED_ANCHOR_SEGMENT.getDisposition());
        Assert.assertFalse(
                "the anchored segment is proven but not wired until Phase 7",
                DependencyKind.FIXED_ANCHOR_SEGMENT.isEligibleNow()
        );
    }

    @Test
    public void testWindowMatrixDispositionsAreFrozen() {
        // Every row of the design section 6 matrix, pinned exactly. A change to any
        // disposition must be a deliberate edit to this map.
        final Map<DependencyKind, Disposition> expected = new EnumMap<>(DependencyKind.class);
        expected.put(DependencyKind.ROWS_N_PRECEDING_CURRENT_ROW, Disposition.ELIGIBLE);
        expected.put(DependencyKind.RANGE_W_PRECEDING_CURRENT_ROW, Disposition.ELIGIBLE);
        expected.put(DependencyKind.FIXED_ANCHOR_SEGMENT, Disposition.ELIGIBLE_PHASE_7);
        expected.put(DependencyKind.UNBOUNDED_CUMULATIVE_NO_RESET, Disposition.REJECT);
        expected.put(DependencyKind.UNANCHORED_RANK, Disposition.REJECT);
        expected.put(DependencyKind.FOLLOWING_OR_DATA_DEPENDENT, Disposition.REJECT_INITIALLY);

        Assert.assertEquals(DependencyKind.values().length, expected.size());
        for (DependencyKind kind : DependencyKind.values()) {
            Assert.assertEquals(
                    "unexpected disposition [kind=" + kind + ']',
                    expected.get(kind),
                    kind.getDisposition()
            );
        }
    }
}

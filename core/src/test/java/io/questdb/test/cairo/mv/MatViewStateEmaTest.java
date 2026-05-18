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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Edge-case tests for the EMA recording and gap-threshold derivation on
 * {@link MatViewState}. The EMA methods don't dereference the mat view
 * definition or the telemetry facade, but we still construct a real (empty)
 * definition rather than passing {@code null} -- otherwise a future maintainer
 * adding a {@code viewDefinition.getMatViewToken()} call to any EMA method
 * would silently NPE every test in this class.
 */
public class MatViewStateEmaTest {

    private MatViewState state;

    @Before
    public void setUp() {
        state = new MatViewState(new MatViewDefinition(), null);
        Assert.assertTrue(state.tryLock());
    }

    @After
    public void tearDown() {
        if (state != null) {
            state.unlock();
            state.close();
        }
    }

    @Test
    public void testColdStartThresholdReturnsDefault() {
        Assert.assertEquals(0L, state.getAvgCommitNanos());
        Assert.assertEquals(0L, state.getAvgScanNanosPerTsUnit());
        Assert.assertEquals(MatViewState.COLD_START_GAP_THRESHOLD_TS_UNITS,
                state.getCommitGapThresholdTsUnits());
    }

    @Test
    public void testEmaConvergesOverManySamples() {
        // Seed at 1_000_000ns and feed 50 samples at 100_000ns; EMA should
        // converge toward 100_000 but not reach it exactly.
        state.setRefreshMetricsForTesting(1_000_000L, 0L);
        for (int i = 0; i < 50; i++) {
            state.recordCommitNanos(100_000L);
        }
        final long avg = state.getAvgCommitNanos();
        Assert.assertTrue("EMA should approach 100_000; got: " + avg, avg < 200_000);
        Assert.assertTrue("EMA should still be > 100_000 after only 50 samples; got: " + avg, avg > 100_000);
    }

    @Test
    public void testEmaIgnoresNegativeAndZeroSamples() {
        state.recordCommitNanos(500_000L);
        final long after = state.getAvgCommitNanos();
        state.recordCommitNanos(0L);
        state.recordCommitNanos(-100L);
        Assert.assertEquals("Zero / negative samples must not alter the EMA",
                after, state.getAvgCommitNanos());
    }

    @Test
    public void testFastScanOverWideRangeDoesNotDegradeThresholdBelowColdStart() {
        // Sub-resolution sample: 100ns scan over a 1s (= 1_000_000us) range.
        // True rate ~1e-4 ns/us. Integer floor in recordScanMetrics records it
        // as perTsUnit=1, which is 10_000x the truth and locks the EMA into a
        // regime where the derived threshold collapses below the cold-start
        // default -- i.e. the auto-tune actively degrades clustering on the
        // same workload the optimisation was designed to help.
        state.recordScanMetrics(100L, 1_000_000L);
        state.recordCommitNanos(1_000_000L);

        final long threshold = state.getCommitGapThresholdTsUnits();
        Assert.assertTrue(
                "Sub-resolution scan sample must not pull threshold below cold-start default; got: " + threshold,
                threshold >= MatViewState.COLD_START_GAP_THRESHOLD_TS_UNITS
        );
    }

    @Test
    public void testOutlierCappedAtMultiplierOfPrev() {
        // Seed at 100. Feed a sample of 10_000 (= 100x prev). The cap is
        // 5x, so the folded value is computed against 500, not 10_000.
        state.setRefreshMetricsForTesting(100L, 1L);
        state.recordCommitNanos(10_000L);
        // EMA: (100 * 7 + min(10000, 100*5)) / 8 = (700 + 500) / 8 = 150.
        Assert.assertEquals(150L, state.getAvgCommitNanos());
    }

    @Test
    public void testScanMetricsIgnoresZeroRange() {
        state.recordScanMetrics(1_000_000L, 0L);
        Assert.assertEquals(0L, state.getAvgScanNanosPerTsUnit());

        state.recordScanMetrics(1_000_000L, -1L);
        Assert.assertEquals(0L, state.getAvgScanNanosPerTsUnit());
    }

    @Test
    public void testScanMetricsRecordsRatePerTsUnit() {
        // 1_000_000ns scan over 1_000us range -> 1000 ns/us.
        state.recordScanMetrics(1_000_000L, 1_000L);
        Assert.assertEquals(1_000L, state.getAvgScanNanosPerTsUnit());

        // Cold-start derived threshold = commit / scanPerTsUnit.
        state.recordCommitNanos(5_000_000L);
        Assert.assertEquals(5_000L, state.getCommitGapThresholdTsUnits());
    }

    @Test
    public void testSetRefreshMetricsForTestingClampsNegatives() {
        // Negative seeds should be coerced to zero, not retained.
        state.setRefreshMetricsForTesting(-1L, -100L);
        Assert.assertEquals(0L, state.getAvgCommitNanos());
        Assert.assertEquals(0L, state.getAvgScanNanosPerTsUnit());
    }

    @Test
    public void testThresholdReportsDisabledWhenScanCostExceedsCommit() {
        // commit (500us) < scanPerTsUnit (100ms / tsUnit): the cost model
        // says scanning even one ts unit is more expensive than a full
        // REPLACE_RANGE commit, so gap-based merging should be off. The
        // previous implementation returned Math.max(1, 0) = 1, which looks
        // like "merge any gap below 1 ts unit" in materialized_views() but
        // is functionally indistinguishable from disabled -- unionInPlace
        // guarantees adjacent intervals have gap >= 1, so the condition
        // gap < 1 is unreachable. Surface 0 instead so operators reading
        // the catalogue can tell clustering is dormant.
        state.setRefreshMetricsForTesting(500_000L, 100_000_000L);
        Assert.assertEquals(
                "Threshold must report 0 when commit < scanPerTsUnit; the >=1 clamp hides a disabled cost model",
                0L,
                state.getCommitGapThresholdTsUnits()
        );
    }

    @Test
    public void testThresholdSurvivesExtremeScanRate() {
        // Pathological cold-start: a single first sample with tiny range
        // can lock in a huge per-tsUnit rate. The threshold collapses to
        // the "disabled" sentinel (0) without flipping negative or
        // overflowing.
        state.setRefreshMetricsForTesting(0L, 0L);
        // 100ms scan over 1us range -> 100_000_000 ns/us.
        state.recordScanMetrics(100_000_000L, 1L);
        state.recordCommitNanos(500_000L);
        final long threshold = state.getCommitGapThresholdTsUnits();
        Assert.assertTrue("Threshold must not flip negative under pathological scan rate; got: "
                + threshold, threshold >= 0);
        // commit (500_000) / perTsUnit (100_000_000) = 0, the gap-merge-disabled sentinel.
        Assert.assertEquals(0L, threshold);
    }

    @Test
    public void testThresholdWithLargeButNotOverflowingCommit() {
        // Large commit and tiny scan-rate produce a large but in-range threshold.
        state.setRefreshMetricsForTesting(1_000_000_000_000L, 1L);
        Assert.assertEquals(1_000_000_000_000L, state.getCommitGapThresholdTsUnits());
    }

    @Test
    public void testEmaArithmeticDoesNotOverflowOnMaxValues() {
        // Seed near Long.MAX_VALUE / 7 so prev * (EMA_ALPHA_INV - 1) is right
        // at the overflow boundary; verify the 50/50-blend fallback fires and
        // produces the exact expected value.
        final long seed = Long.MAX_VALUE / 7 - 1;
        state.setRefreshMetricsForTesting(seed, 1L);
        state.recordCommitNanos(Long.MAX_VALUE);

        // Expected math: cap = min(MAX, 5*seed). 5*seed = 5*(MAX/7-1) is
        // in-range, so cap = 5*(MAX/7-1). Then weighted = 7*seed wraps via
        // multiplyExact; the fallback returns (seed/2) + (cap/2).
        final long expectedCap = 5L * seed;
        final long expectedAvg = (seed / 2) + (expectedCap / 2);
        Assert.assertEquals(
                "EMA fallback must use exact 50/50 blend on overflow",
                expectedAvg,
                state.getAvgCommitNanos()
        );
        // Threshold must remain non-negative even when the EMA folds via the
        // overflow fallback. Whether it lands at 0 (disabled sentinel) or a
        // positive value depends on the post-fallback commit/perTsUnit ratio.
        Assert.assertTrue(
                "Threshold must stay >= 0 even after overflow; got: " + state.getCommitGapThresholdTsUnits(),
                state.getCommitGapThresholdTsUnits() >= 0L
        );
    }

    @Test
    public void testEmaOuterMultiplierOverflowFallback() {
        // Seed prev where 5 * prev overflows -- the outlier cap multiplyExact
        // catch should saturate to Long.MAX_VALUE and the EMA still update.
        final long seed = Long.MAX_VALUE / 4; // 5 * seed overflows
        state.setRefreshMetricsForTesting(seed, 1L);
        // Sample below seed -- not capped, used directly.
        state.recordCommitNanos(seed - 100L);
        // weighted = 7 * seed overflows; fallback = (seed/2) + (sample/2).
        final long expectedAvg = (seed / 2) + ((seed - 100L) / 2);
        Assert.assertEquals(expectedAvg, state.getAvgCommitNanos());
    }

    @Test
    public void testRefreshStatsZeroesEverything() {
        state.setRefreshMetricsForTesting(123_456L, 78L);
        state.refreshStats();
        Assert.assertEquals(0L, state.getAvgCommitNanos());
        Assert.assertEquals(0L, state.getAvgScanNanosPerTsUnit());
        Assert.assertEquals(MatViewState.COLD_START_GAP_THRESHOLD_TS_UNITS,
                state.getCommitGapThresholdTsUnits());
    }
}

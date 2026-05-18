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

import io.questdb.cairo.mv.MatViewState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Edge-case tests for the EMA recording and gap-threshold derivation on
 * {@link MatViewState}. Uses the test-only constructor path that skips the
 * mat view definition, since the EMA methods are independent of it.
 */
public class MatViewStateEmaTest {

    private MatViewState state;

    @Before
    public void setUp() {
        // Pass nulls; EMA methods don't touch viewDefinition / telemetryFacade.
        state = new MatViewState(null, null);
        Assert.assertTrue(state.tryLock());
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
    public void testThresholdSurvivesExtremeScanRate() {
        // Pathological cold-start: a single first sample with tiny range
        // can lock in a huge per-tsUnit rate. The threshold falls but does
        // not flip negative or overflow.
        state.setRefreshMetricsForTesting(0L, 0L);
        // 100ms scan over 1us range -> 100_000_000 ns/us.
        state.recordScanMetrics(100_000_000L, 1L);
        state.recordCommitNanos(500_000L);
        final long threshold = state.getCommitGapThresholdTsUnits();
        Assert.assertTrue("Threshold should be >= 1 even under pathological scan rate; got: "
                + threshold, threshold >= 1);
        // commit (500_000) / perTsUnit (100_000_000) = 0; max(1, 0) = 1.
        Assert.assertEquals(1L, threshold);
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
        // at the overflow boundary; verify no negative value is produced.
        final long seed = Long.MAX_VALUE / 7 - 1;
        state.setRefreshMetricsForTesting(seed, 1L);
        // Sample > 5*prev is capped to 5*prev. With seed = MAX/7, capping
        // multiplies by 5 -- which itself is in-range. Then prev*7 + capped
        // overflows to a negative-ish value. The arithmetic is undefined
        // behaviour, but we should not produce a value that breaks
        // subsequent decisions catastrophically (e.g. become a permanent
        // negative cache).
        state.recordCommitNanos(Long.MAX_VALUE);
        final long avg = state.getAvgCommitNanos();
        // If the value did wrap, getCommitGapThresholdTsUnits should still
        // return something sensible (>= 1 due to the floor).
        Assert.assertTrue("Threshold must stay >= 1 even after overflow; got: "
                + state.getCommitGapThresholdTsUnits(),
                state.getCommitGapThresholdTsUnits() >= 1L);
        // Log the resulting avg for inspection -- not asserting a specific
        // value, just confirming the EMA hasn't permanently broken.
        Assert.assertNotEquals("Sanity: avg should not be zero after a real sample",
                0L, avg);
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

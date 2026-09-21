/*******************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointOpenSegmentCost;
import org.junit.Assert;
import org.junit.Test;

public class LiveViewCheckpointOpenSegmentCostTest {

    @Test
    public void testColdRestoreCanOverrideTheRowVerdict() {
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();

        Assert.assertTrue(cost.shouldOverrideWholeRange(
                false,
                70L * 1024 * 1024,
                16_666,
                190_000,
                10_000
        ));
        Assert.assertTrue(cost.getLastWholeEstimateNanos() > cost.getLastKeyedEstimateNanos());
    }

    @Test
    public void testReusableRuntimeNeverAddsRestoreCost() {
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();

        Assert.assertFalse(cost.shouldOverrideWholeRange(
                true,
                2L * 1024 * 1024 * 1024,
                16_666,
                190_000,
                10_000
        ));
        Assert.assertTrue(cost.getLastWholeEstimateNanos() < cost.getLastKeyedEstimateNanos());
    }

    @Test
    public void testPerViewSamplesMoveTheCrossover() {
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();

        // The cold-start priors price this route in favour of the keyed executor:
        // whole = 20_000 rows * 100 + 64_000_000 bytes * 6 = 386_000_000 ns,
        // keyed = 200_000 cost rows * 250 + 10_000 keys * 5_000 = 100_000_000 ns.
        // The 150% keyed upper bound (150_000_000) stays well under the 85% whole-range
        // hysteresis floor (328_100_000), so the model overrides the row verdict.
        Assert.assertTrue(cost.shouldOverrideWholeRange(
                false,
                64_000_000,
                20_000,
                200_000,
                10_000
        ));
        Assert.assertEquals(386_000_000L, cost.getLastWholeEstimateNanos());
        Assert.assertEquals(100_000_000L, cost.getLastKeyedEstimateNanos());

        // This view then measures a restore that runs 12x faster than the prior (0.5 ns/byte),
        // a whole-range scan twice as fast (50 ns/row), a keyed scan 4x slower (1_000 ns per
        // cost row) and a key-state transplant twice as slow (10_000 ns/key).
        cost.setRatesForTest(
                32_000_000,
                64_000_000,
                1_000_000,
                20_000,
                1_000_000,
                20_000,
                200_000_000,
                200_000,
                100_000_000,
                10_000
        );

        // The samples move the crossover to the other side of the same inputs:
        // whole = 1_000_000 + 32_000_000 = 33_000_000 ns,
        // keyed = 200_000_000 + 100_000_000 = 300_000_000 ns, so the 150% keyed upper
        // bound (450_000_000) now dwarfs the 85% hysteresis floor (28_050_000) and the
        // model leaves the row verdict alone.
        Assert.assertFalse(cost.shouldOverrideWholeRange(
                false,
                64_000_000,
                20_000,
                200_000,
                10_000
        ));
        Assert.assertEquals(33_000_000L, cost.getLastWholeEstimateNanos());
        Assert.assertEquals(300_000_000L, cost.getLastKeyedEstimateNanos());
    }

    @Test
    public void testEstimatesSaturateInsteadOfWrapping() {
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();

        Assert.assertFalse(cost.shouldOverrideWholeRange(
                false,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE
        ));
        Assert.assertEquals(Long.MAX_VALUE, cost.getLastWholeEstimateNanos());
        Assert.assertEquals(Long.MAX_VALUE, cost.getLastKeyedEstimateNanos());
    }

    @Test
    public void testTheBreakEvenIsTheLastCostTheColdPriorsOverrideAt() {
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();

        // whole = 1_000_000 bytes * 6 = 6_000_000ns, under an 85% hysteresis floor of
        // 5_100_000ns; keyed = cost * 250 + 1 key * 5_000, whose 150% upper bound is
        // 375 * cost + 7_500. That stays below the floor up to a cost of 13_579 rows and
        // reaches it at 13_580.
        final long maxCost = cost.maxOverridingKeyedCostRows(false, 1_000_000, 0, 1);
        Assert.assertEquals(13_579, maxCost);
        assertBreakEven(cost, maxCost, 1_000_000, 0, 1);
    }

    @Test
    public void testTheBreakEvenInvertsTheSampledRatesRounding() {
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();
        // Rates that divide unevenly, so every scale() on the way rounds up.
        cost.setRatesForTest(10, 3, 7, 3, 5, 3, 10, 3, 20, 7);

        // whole = ceil(10 * 100 / 3) = 334ns, under a floor of ceil(283.9) = 284ns; the key
        // state is ceil(20 / 7) = 3ns. A cost of 55 rows scans in ceil(550 / 3) = 184ns and
        // bounds the keyed side at ceil(1.5 * 187) = 281ns, under the floor; 56 rows scan in
        // 187ns and bound it at 285ns, over it.
        final long maxCost = cost.maxOverridingKeyedCostRows(false, 100, 0, 1);
        Assert.assertEquals(55, maxCost);
        assertBreakEven(cost, maxCost, 100, 0, 1);
    }

    @Test
    public void testTheBreakEvenHoldsAcrossTheInputs() {
        final long[] rootBytes = {1, 40, 1_470, 1_471, 12_345, 1_000_000, 70L * 1024 * 1024, Long.MAX_VALUE / 7, Long.MAX_VALUE};
        final long[] wholeRows = {0, 1, 7, 16_666, 1_000_000_000L};
        final long[] keyCounts = {1, 2, 3, 17, 10_000, Long.MAX_VALUE};
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();
        for (int rates = 0; rates < 4; rates++) {
            switch (rates) {
                // The cold priors.
                case 0 -> cost.setRatesForTest(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
                // Uneven samples, so the scales round.
                case 1 -> cost.setRatesForTest(10, 3, 7, 3, 5, 3, 10, 3, 20, 7);
                // A restore-dominant view.
                case 2 -> cost.setRatesForTest(1_000_000_000L, 1, 1, 1, 1, 1, 1, 1, 1, 1);
                // A cheap restore beside an expensive keyed scan.
                default -> cost.setRatesForTest(1, 1_000, 3, 1, 3, 1, 1_000_003, 7, 999, 1);
            }
            for (long bytes : rootBytes) {
                for (long rows : wholeRows) {
                    for (long keys : keyCounts) {
                        assertBreakEven(cost, cost.maxOverridingKeyedCostRows(false, bytes, rows, keys), bytes, rows, keys);
                    }
                }
            }
        }
    }

    @Test
    public void testNoCostOverridesWithoutARestoreOrAKey() {
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();
        cost.setRatesForTest(1_000_000_000L, 1, 1, 1, 1, 1, 1, 1, 1, 1);

        // A reusable runtime restores nothing, whatever the root weighs.
        Assert.assertEquals(
                LiveViewCheckpointOpenSegmentCost.NO_OVERRIDING_KEYED_COST,
                cost.maxOverridingKeyedCostRows(true, 1_000_000, 0, 1)
        );
        Assert.assertFalse(cost.shouldOverrideWholeRange(true, 1_000_000, 0, 0, 1));
        // Neither does a root of no bytes.
        Assert.assertEquals(
                LiveViewCheckpointOpenSegmentCost.NO_OVERRIDING_KEYED_COST,
                cost.maxOverridingKeyedCostRows(false, 0, 0, 1)
        );
        Assert.assertEquals(
                LiveViewCheckpointOpenSegmentCost.NO_OVERRIDING_KEYED_COST,
                cost.maxOverridingKeyedCostRows(false, -1, 0, 1)
        );
        // And with no key there is nothing to follow.
        Assert.assertEquals(
                LiveViewCheckpointOpenSegmentCost.NO_OVERRIDING_KEYED_COST,
                cost.maxOverridingKeyedCostRows(false, 1_000_000, 0, 0)
        );
        Assert.assertFalse(cost.shouldOverrideWholeRange(false, 1_000_000, 0, 0, 0));
    }

    @Test
    public void testNoCostOverridesARestoreTheKeyStateAloneOutweighs() {
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();

        // The cold priors restore 1_470 bytes in 8_820ns, under a floor of 7_497ns, and one
        // key's state bounds the keyed side at 7_500ns before it scans a row.
        Assert.assertEquals(
                LiveViewCheckpointOpenSegmentCost.NO_OVERRIDING_KEYED_COST,
                cost.maxOverridingKeyedCostRows(false, 1_470, 0, 1)
        );
        Assert.assertFalse(cost.shouldOverrideWholeRange(false, 1_470, 0, 0, 1));
        // One byte more lifts the floor to ceil(7_502.1) = 7_503ns, which a scan of no rows
        // undercuts and a scan of one row, at 7_875ns, does not.
        Assert.assertEquals(0, cost.maxOverridingKeyedCostRows(false, 1_471, 0, 1));
        assertBreakEven(cost, 0, 1_471, 0, 1);
    }

    @Test
    public void testTheBreakEvenSaturatesInsteadOfWrapping() {
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();

        // The largest root the cold priors can price: its restore saturates at
        // Long.MAX_VALUE, and the break-even stays finite and exact below it.
        final long maxCost = cost.maxOverridingKeyedCostRows(false, Long.MAX_VALUE, 0, 1);
        Assert.assertTrue(maxCost > 0 && maxCost < Long.MAX_VALUE);
        assertBreakEven(cost, maxCost, Long.MAX_VALUE, 0, 1);

        // A keyed sample so wide that every cost scans in one nanosecond: no cost is too
        // high, and the break-even is Long.MAX_VALUE itself rather than a wrapped value.
        cost.setRatesForTest(1_000_000_000L, 1, 1, 1, 1, 1, 1, Long.MAX_VALUE, 1, 1);
        Assert.assertEquals(Long.MAX_VALUE, cost.maxOverridingKeyedCostRows(false, 1_000, 0, 1));
        Assert.assertTrue(cost.shouldOverrideWholeRange(false, 1_000, 0, Long.MAX_VALUE, 1));
    }

    @Test
    public void testTheBreakEvenLeavesTheLastEstimatesAlone() {
        final LiveViewCheckpointOpenSegmentCost cost = new LiveViewCheckpointOpenSegmentCost();
        Assert.assertTrue(cost.shouldOverrideWholeRange(false, 64_000_000, 20_000, 200_000, 10_000));

        Assert.assertEquals(13_579, cost.maxOverridingKeyedCostRows(false, 1_000_000, 0, 1));

        // Still the figures the override itself priced, which is what a pricing log reads.
        Assert.assertEquals(386_000_000L, cost.getLastWholeEstimateNanos());
        Assert.assertEquals(100_000_000L, cost.getLastKeyedEstimateNanos());
    }

    /**
     * The override holds at {@code maxCost} and fails one row above it, or - for
     * {@link LiveViewCheckpointOpenSegmentCost#NO_OVERRIDING_KEYED_COST} - fails even at a
     * cost of zero.
     */
    private static void assertBreakEven(
            LiveViewCheckpointOpenSegmentCost cost,
            long maxCost,
            long bytes,
            long rows,
            long keys
    ) {
        final String inputs = "bytes=" + bytes + ", rows=" + rows + ", keys=" + keys + ", maxCost=" + maxCost;
        if (maxCost == LiveViewCheckpointOpenSegmentCost.NO_OVERRIDING_KEYED_COST) {
            Assert.assertFalse(inputs, cost.shouldOverrideWholeRange(false, bytes, rows, 0, keys));
            return;
        }
        Assert.assertTrue(inputs, maxCost >= 0);
        Assert.assertTrue(inputs, cost.shouldOverrideWholeRange(false, bytes, rows, maxCost, keys));
        if (maxCost < Long.MAX_VALUE) {
            Assert.assertFalse(inputs, cost.shouldOverrideWholeRange(false, bytes, rows, maxCost + 1, keys));
        }
    }
}

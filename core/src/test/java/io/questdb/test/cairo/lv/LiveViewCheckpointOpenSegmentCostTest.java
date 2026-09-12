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
}

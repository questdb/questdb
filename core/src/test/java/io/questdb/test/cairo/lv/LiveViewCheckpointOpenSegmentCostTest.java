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
        cost.setRatesForTest(
                1_000,
                1_000_000,
                1_000_000,
                10_000,
                1_000_000,
                10_000,
                100_000_000,
                100_000,
                100_000_000,
                1_000
        );

        Assert.assertFalse(cost.shouldOverrideWholeRange(
                false,
                1_000_000,
                10_000,
                100_000,
                1_000
        ));
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

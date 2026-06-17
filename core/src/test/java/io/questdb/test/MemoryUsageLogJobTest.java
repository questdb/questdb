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

package io.questdb.test;

import io.questdb.MemoryUsageLogJob;
import org.junit.Assert;
import org.junit.Test;

public class MemoryUsageLogJobTest {
    @Test
    public void testRunSeriallyHonoursDynamicEnableDisable() {
        final long[] ticks = {0};
        final boolean[] enabled = {true};
        final MemoryUsageLogJob job = new MemoryUsageLogJob(() -> ticks[0], () -> enabled[0], () -> 1000);

        Assert.assertTrue(job.run());

        enabled[0] = false;
        ticks[0] = 5_000_000;
        Assert.assertFalse(job.run());

        enabled[0] = true;
        Assert.assertTrue(job.run());
    }

    @Test
    public void testRunSeriallyHonoursDynamicIntervalChange() {
        final long[] ticks = {0};
        final long[] intervalMillis = {1000};
        final MemoryUsageLogJob job = new MemoryUsageLogJob(() -> ticks[0], () -> true, () -> intervalMillis[0]);

        Assert.assertTrue(job.run());

        intervalMillis[0] = 10;
        ticks[0] = 10_000;
        Assert.assertTrue(job.run());

        ticks[0] = 19_999;
        Assert.assertFalse(job.run());

        ticks[0] = 20_000;
        Assert.assertTrue(job.run());
    }

    @Test
    public void testRunSeriallyHonoursInterval() {
        final long[] ticks = {0};
        final MemoryUsageLogJob job = new MemoryUsageLogJob(() -> ticks[0], () -> true, () -> 1000);

        Assert.assertTrue(job.run());
        Assert.assertFalse(job.run());

        ticks[0] = 999_999;
        Assert.assertFalse(job.run());

        ticks[0] = 1_000_000;
        Assert.assertTrue(job.run());
    }
}

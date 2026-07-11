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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.OperationExecutor;
import org.junit.Assert;
import org.junit.Test;

public class OperationExecutorWindowStepTest {

    @Test
    public void testUniformDensityGivesRowsPerStepWidth() {
        // 1000 rows uniformly over ts [0, 999] (span 1000). rowsPerStep=100 -> ~100 ts units per window.
        Assert.assertEquals(100, OperationExecutor.deleteWindowStep(0, 999, 1000, 100));
    }

    @Test
    public void testStepAtLeastOne() {
        // Denser than one row per ts unit: step floors at 1 (never 0, which would not advance the loop).
        Assert.assertEquals(1, OperationExecutor.deleteWindowStep(0, 9, 1_000_000, 100));
    }

    @Test
    public void testRowsPerStepExceedsTableGivesSingleWindow() {
        // rowsPerStep >= tableRows -> step spans the whole populated range (one window).
        long step = OperationExecutor.deleteWindowStep(0, 999, 1000, 10_000);
        Assert.assertTrue("step must cover the whole span", step >= 1000);
    }

    @Test
    public void testEmptyTableSingleWindow() {
        Assert.assertEquals(Long.MAX_VALUE, OperationExecutor.deleteWindowStep(0, 0, 0, 100));
    }

    @Test
    public void testHugeSpanNoOverflow() {
        // Near-max span must not overflow to a negative/zero step (double math in estimateBucketsForRows).
        long step = OperationExecutor.deleteWindowStep(0, (Long.MAX_VALUE >> 1), 1_000_000_000L, 1_000_000L);
        Assert.assertTrue("step positive", step > 0);
    }
}

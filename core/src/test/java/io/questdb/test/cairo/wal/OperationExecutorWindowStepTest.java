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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.wal.WalUtils;
import org.junit.Assert;
import org.junit.Test;

public class OperationExecutorWindowStepTest {

    @Test
    public void testAtomicWindowAlignsExactBoundary() {
        final long dayMicros = 86_400_000_000L;
        Assert.assertEquals(
                dayMicros,
                WalUtils.deleteAtomicWindowHiExcl(0, dayMicros, 3 * dayMicros, ColumnType.TIMESTAMP, PartitionBy.DAY)
        );
    }

    @Test
    public void testAtomicWindowAlignsToPartitionCeiling() {
        final long dayMicros = 86_400_000_000L;
        final long minuteMicros = 60_000_000L;
        Assert.assertEquals(
                dayMicros,
                WalUtils.deleteAtomicWindowHiExcl(
                        minuteMicros,
                        1000 * minuteMicros,
                        3 * dayMicros,
                        ColumnType.TIMESTAMP,
                        PartitionBy.DAY
                )
        );
    }

    @Test
    public void testAtomicWindowCeilingOverflowConsumesRemainder() {
        Assert.assertEquals(
                Long.MAX_VALUE,
                WalUtils.deleteAtomicWindowHiExcl(
                        Long.MAX_VALUE - 100,
                        50,
                        Long.MAX_VALUE - 1,
                        ColumnType.TIMESTAMP_NANO,
                        PartitionBy.DAY
                )
        );
    }

    @Test
    public void testCrossZeroFullDomainSpanAdvancesInBoundedWindows() {
        final long maxTs = Long.MAX_VALUE - 1;
        final long minTs = Long.MIN_VALUE + 1;
        final long step = WalUtils.deleteWindowStep(minTs, maxTs, 1_000_000_000L, 1_000_000L);
        Assert.assertTrue("step must remain positive and density-scaled, was " + step, step > 1_000_000L);

        long lo = minTs;
        int windowCount = 0;
        while (lo <= maxTs && windowCount < 2_000) {
            final long hi = WalUtils.deleteWindowHiExcl(lo, step, maxTs);
            Assert.assertTrue("window high must advance [lo=" + lo + ", hi=" + hi + ']', hi > lo);
            lo = hi;
            windowCount++;
        }
        Assert.assertEquals(maxTs + 1, lo);
        Assert.assertTrue("expected multiple density windows, was " + windowCount, windowCount > 1);
        Assert.assertTrue("window count must remain density-bounded, was " + windowCount, windowCount < 2_000);
    }

    @Test
    public void testCrossZeroRemainingSpanDoesNotOverflow() {
        Assert.assertEquals(
                Long.MIN_VALUE + 2,
                WalUtils.deleteWindowHiExcl(Long.MIN_VALUE + 1, 1, 0)
        );
    }

    @Test
    public void testUniformDensityGivesRowsPerStepWidth() {
        // 1000 rows uniformly over ts [0, 999] (span 1000). rowsPerStep=100 -> ~100 ts units per window.
        Assert.assertEquals(100, WalUtils.deleteWindowStep(0, 999, 1000, 100));
    }

    @Test
    public void testStepAtLeastOne() {
        // Denser than one row per ts unit: step floors at 1 (never 0, which would not advance the loop).
        Assert.assertEquals(1, WalUtils.deleteWindowStep(0, 9, 1_000_000, 100));
    }

    @Test
    public void testRowsPerStepExceedsTableGivesSingleWindow() {
        // rowsPerStep >= tableRows -> step spans the whole populated range (one window).
        long step = WalUtils.deleteWindowStep(0, 999, 1000, 10_000);
        Assert.assertTrue("step must cover the whole span", step >= 1000);
    }

    @Test
    public void testEmptyTableSingleWindow() {
        Assert.assertEquals(Long.MAX_VALUE, WalUtils.deleteWindowStep(0, 0, 0, 100));
    }

    @Test
    public void testHugeSpanNoOverflow() {
        // Near-max span must not overflow to a negative/zero step (double math in estimateBucketsForRows).
        long step = WalUtils.deleteWindowStep(0, (Long.MAX_VALUE >> 1), 1_000_000_000L, 1_000_000L);
        Assert.assertTrue("step positive", step > 0);
    }

    @Test
    public void testFullPositiveDomainSpanDoesNotOverflow() {
        // minTs=0, maxTs=Long.MAX_VALUE fills the whole positive domain: `maxTs - minTs + 1` overflows to a
        // negative span, which (before the clamp) floored the step to 1 and exploded the window count to ~2^63.
        // The clamp must keep the step large (one/few windows), not 1.
        long step = WalUtils.deleteWindowStep(0, Long.MAX_VALUE, 1_000_000_000L, 1_000_000L);
        Assert.assertTrue("step must not be floored to 1 by span overflow, was " + step, step > 1_000_000L);
    }
}

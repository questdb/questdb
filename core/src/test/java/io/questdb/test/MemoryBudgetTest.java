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

import io.questdb.MemoryBudget;
import org.junit.Assert;
import org.junit.Test;

public class MemoryBudgetTest {

    private static final long GB = 1024L * 1024 * 1024;
    private static final long MB = 1024L * 1024;

    @Test
    public void testDisabledWhenBudgetIsZero() {
        MemoryBudget b = new MemoryBudget(0, 0, 12, 1, 32, 8);
        Assert.assertFalse(b.isEnabled());
        Assert.assertEquals(-1, b.getWorkerCount());
        Assert.assertEquals(-1L, b.getSqlCopyBufferSize());
    }

    @Test
    public void testDisabledPassesStockPoolCapacityThrough() {
        // With no budget the caller must get its own default back untouched.
        MemoryBudget b = new MemoryBudget(0, 0, 12, 1, 32, 8);
        Assert.assertEquals(4096, b.getSqlPoolCapacity(4096));
    }

    @Test
    public void testCopyBufferCollapsesUnderASmallBudget() {
        // 30 workers x 2MB = 60MB of import buffer is the single largest
        // fixed native item at boot. A 256MB budget cannot afford it.
        MemoryBudget b = new MemoryBudget(256 * MB, 128 * MB, 12, 1, 32, 8);
        Assert.assertTrue("copy buffer must shrink far below the 2MB stock default",
                b.getSqlCopyBufferSize() <= 256 * 1024);
        long totalImport = b.getSqlCopyBufferSize() * b.getWorkerCount();
        Assert.assertTrue("total import buffer must fit in a few MB, was " + totalImport,
                totalImport <= 8 * MB);
    }

    @Test
    public void testWorkerCountCollapsesUnderASmallBudget() {
        MemoryBudget b = new MemoryBudget(256 * MB, 128 * MB, 12, 1, 32, 8);
        int w = b.getWorkerCount();
        Assert.assertTrue("workers must be >= 1", w >= 1);
        Assert.assertTrue("32 CPUs must not all get workers on a 256MB budget, was " + w,
                w <= 4);
    }

    @Test
    public void testWorkerCountNeverExceedsCpuCount() {
        // A generous budget must not conjure more workers than the machine has.
        MemoryBudget b = new MemoryBudget(64 * GB, 128 * MB, 12, 1, 2, 8);
        Assert.assertTrue("must never exceed available CPUs", b.getWorkerCount() <= 2);
    }

    @Test
    public void testSqlPoolCapacityShrinksButStaysUsable() {
        MemoryBudget b = new MemoryBudget(256 * MB, 128 * MB, 12, 1, 32, 8);
        int scaled = b.getSqlPoolCapacity(4096);
        Assert.assertTrue("pool must shrink from stock", scaled < 4096);
        Assert.assertTrue("pool must stay usable, was " + scaled, scaled >= 16);
    }

    @Test
    public void testEveryDerivedValueIsMonotonicInBudget() {
        MemoryBudget small = new MemoryBudget(256 * MB, 64 * MB, 12, 1, 32, 8);
        MemoryBudget large = new MemoryBudget(4 * GB, 64 * MB, 12, 1, 32, 8);
        Assert.assertTrue(large.getSqlCopyBufferSize() >= small.getSqlCopyBufferSize());
        Assert.assertTrue(large.getWorkerCount() >= small.getWorkerCount());
        Assert.assertTrue(large.getSqlPoolCapacity(4096) >= small.getSqlPoolCapacity(4096));
        Assert.assertTrue(large.getWriterDataAppendPageSize() >= small.getWriterDataAppendPageSize());
        Assert.assertTrue(large.getConnectionBufferSize() >= small.getConnectionBufferSize());

        // The five assertions above only use >=, so an implementation with every
        // getter hardcoded to a constant would pass them vacuously. bootArenaBytes
        // is a direct fixed-fraction multiply of `usable` (usable * SHARE_BOOT) with
        // no saturation (unlike workerCount, which caps at cpuCount) and no
        // division by a budget-dependent worker count (unlike sqlCopyBufferSize,
        // which is near-flat pre-saturation because both numerator and denominator
        // grow with the budget). With fixedOverheadBytes fixed at 64MB across both
        // budgets here, `usable` strictly increases from 256MB to 4GB, so
        // bootArenaBytes strictly increases too - this pair genuinely proves the
        // arithmetic depends on the budget, not just that it never decreases.
        Assert.assertTrue("bootArenaBytes must strictly grow with the budget, was " +
                        small.getBootArenaBytes() + " vs " + large.getBootArenaBytes(),
                large.getBootArenaBytes() > small.getBootArenaBytes());
    }

    @Test
    public void testNeverReturnsZeroOnAnAbsurdlySmallBudget() {
        // Budget smaller than its own declared overhead must still yield
        // usable minimums, never zero or negative — downstream code divides
        // by several of these.
        MemoryBudget b = new MemoryBudget(16 * MB, 128 * MB, 200, 4, 64, 64);
        Assert.assertTrue(b.getSqlCopyBufferSize() >= 4096);
        Assert.assertTrue(b.getConnectionBufferSize() >= 4096);
        Assert.assertTrue(b.getWriterDataAppendPageSize() >= 4096);
        Assert.assertTrue(b.getO3ColumnMemorySize() >= 4096);
        Assert.assertTrue(b.getWorkerCount() >= 1);
        Assert.assertTrue(b.getSqlPoolCapacity(4096) >= 16);
        Assert.assertTrue(b.getQueryArenaBytes() > 0);
        Assert.assertTrue(b.getWriteArenaBytes() > 0);
    }

    @Test
    public void testPageSizesAreFourKAligned() {
        MemoryBudget b = new MemoryBudget(256 * MB, 128 * MB, 12, 1, 32, 8);
        Assert.assertEquals(0, b.getWriterDataAppendPageSize() % 4096);
        Assert.assertEquals(0, b.getO3ColumnMemorySize() % 4096);
        Assert.assertEquals(0, b.getSqlCopyBufferSize() % 4096);
        Assert.assertEquals(0, b.getConnectionBufferSize() % 4096);
    }
}

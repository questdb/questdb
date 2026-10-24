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

package io.questdb.test.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.Chars;
import io.questdb.std.DirectIntCharSequenceHashMap;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.NativeMemoryBudget;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class DirectSymbolMapTest {

    @Test
    public void testCacheChunkAllocationBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            for (int length : new int[]{122, 123, 4094}) {
                NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE);
                try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                    String value = "x".repeat(length);
                    long chunkBytes = length == 122 ? 256 : length == 123 ? 512 : 16_384;
                    Assert.assertTrue(put(map, 0, value));
                    Assert.assertEquals(112 + chunkBytes, map.allocatedBytes());
                    Assert.assertEquals(map.allocatedBytes(), budget.getUsedBytes());
                    DirectString retained = new DirectString();
                    TestUtils.assertEquals(value, map.get(0, retained));
                    long address = retained.lo();
                    Assert.assertTrue(put(map, 1, ""));
                    Assert.assertEquals(112 + chunkBytes + (length == 122 ? 512 : 0), map.allocatedBytes());
                    Assert.assertEquals(map.allocatedBytes(), budget.getUsedBytes());
                    Assert.assertEquals(address, retained.lo());
                    TestUtils.assertEquals(value, retained);
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            }
        });
    }

    @Test
    public void testCacheChunkDirectoryBoundariesAndStableViews() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE);
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_SYMBOL_CACHE, budget)) {
                ObjList<DirectString> retained = new ObjList<>();
                long[] addresses = new long[17];
                DirectString view = new DirectString();
                for (int count = 1; count <= 17; count++) {
                    map.setNextChunkSize(256);
                    String value = "x".repeat(116) + (100_000 + count);
                    Assert.assertTrue(put(map, count, value));
                    retained.add(map.get(count, new DirectString()));
                    addresses[count - 1] = retained.getQuick(count - 1).lo();
                    long indexBytes = 80 + (count >= 6 ? 144 : 0) + (count >= 12 ? 272 : 0);
                    long directoryBytes = 32 + (count >= 4 ? 64 : 0) + (count >= 8 ? 128 : 0) + (count >= 16 ? 256 : 0);
                    assertCacheBytes(indexBytes + directoryBytes + count * 256L, map, budget, before);
                    Assert.assertEquals(count, map.size());
                    for (int key = 1; key <= count; key++) {
                        String expected = "x".repeat(116) + (100_000 + key);
                        TestUtils.assertEquals(expected, retained.getQuick(key - 1));
                        TestUtils.assertEquals(expected, map.get(key, view));
                        Assert.assertEquals(addresses[key - 1], retained.getQuick(key - 1).lo());
                        Assert.assertEquals(addresses[key - 1], view.lo());
                    }
                }
                map.close();
                assertCacheBytes(0, map, budget, before);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE));
        });
    }

    @Test
    public void testCacheChunkDirectoryGrowthFailures() throws Exception {
        for (int count : new int[]{3, 7, 15}) {
            for (int failureMode = 0; failureMode < 3; failureMode++) {
                assertCacheDirectoryGrowthFailure(count, failureMode);
            }
        }
    }

    @Test
    public void testCacheContendedInsertDoesNotWait() throws Exception {
        assertMemoryLeak(() -> {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch contenderDone = new CountDownLatch(1);
            AtomicInteger reservations = new AtomicInteger();
            NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024) {
                @Override
                public boolean tryReserve(long bytes) {
                    if (reservations.incrementAndGet() == 1) {
                        entered.countDown();
                        try {
                            Assert.assertTrue(contenderDone.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                    return super.tryReserve(bytes);
                }
            };
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                TestUtils.runConcurrently(2, worker -> {
                    if (worker == 0) {
                        Assert.assertEquals(DirectIntCharSequenceHashMap.INSERTED, map.tryPutIfAbsent(1, 0, 0));
                    } else {
                        try {
                            Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
                            Assert.assertEquals(DirectIntCharSequenceHashMap.BUSY, map.tryPutIfAbsent(2, 0, 0));
                            Assert.assertEquals(0, map.size());
                            Assert.assertEquals(1, reservations.get());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        } finally {
                            contenderDone.countDown();
                        }
                    }
                });
                DirectString view = new DirectString();
                TestUtils.assertEquals("", map.get(1, view));
                Assert.assertNull(map.get(2, view));
                Assert.assertEquals(1, map.size());
                Assert.assertEquals(map.allocatedBytes(), budget.getUsedBytes());
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testConcurrentCachesShareOneBudget() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(32 * 1024);
            CountDownLatch filled = new CountDownLatch(8);
            TestUtils.runConcurrently(8, worker -> {
                try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                    try {
                        DirectString view = new DirectString();
                        for (int key = 0; key < 1000; key++) {
                            if (put(map, key, "value")) {
                                TestUtils.assertEquals("value", map.get(key, view));
                            }
                            Assert.assertTrue(budget.getUsedBytes() <= budget.getLimit());
                        }
                    } finally {
                        // Hold every map until all writers finish so closing one map
                        // cannot mask an overshoot by concurrent reservations.
                        filled.countDown();
                        try {
                            Assert.assertTrue(filled.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                }
            });
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testConcurrentCacheDenseGrowth() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(96, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                for (int key = 0; key < 96; key++) {
                    Assert.assertTrue(put(map, key, ""));
                }
                DirectString retained = new DirectString();
                map.get(0, retained);
                long address = retained.lo();
                long before = map.allocatedBytes();
                Assert.assertTrue(put(map, 256, ""));
                Assert.assertEquals(before + 16 + 512L * Long.BYTES, map.allocatedBytes());
                Assert.assertEquals(map.allocatedBytes(), budget.getUsedBytes());
                Assert.assertEquals(97, map.size());
                Assert.assertTrue(map.isFilling());
                DirectString view = new DirectString();
                for (int key = 0; key < 96; key++) {
                    TestUtils.assertEquals("", map.get(key, view));
                }
                TestUtils.assertEquals("", map.get(256, view));
                Assert.assertNull(map.get(512, view));
                Assert.assertEquals(address, retained.lo());
                TestUtils.assertEquals("", retained);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testConcurrentCacheDenseGrowthAppendFailureAccounting() throws Exception {
        assertMemoryLeak(() -> {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE);
            NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(256, 0.7, MemoryTag.NATIVE_SYMBOL_CACHE, budget)) {
                for (int key = 0; key < 179; key++) {
                    Assert.assertEquals(DirectIntCharSequenceHashMap.INSERTED, map.tryPutIfAbsent(key, 0, 0));
                }
                DirectString retained = map.get(0, new DirectString());
                long address = retained.lo();
                assertCacheBytes(4112 + 256 + 512 + 32, map, budget, before);
                // Admit the doubled index, but refuse the next chunk after it publishes.
                budget.setLimit(map.allocatedBytes() + 8208);
                Assert.assertEquals(DirectIntCharSequenceHashMap.STOPPED, putResult(map, 512, "x".repeat(122)));
                assertCacheBytes(13_120, map, budget, before);
                Assert.assertEquals(budget.getLimit(), budget.getUsedBytes());
                Assert.assertEquals(179, map.size());
                Assert.assertFalse(map.isFilling());
                Assert.assertEquals(DirectIntCharSequenceHashMap.STOP_BUDGET, map.getStopReason());
                DirectString view = new DirectString();
                for (int key = 0; key < 179; key++) {
                    TestUtils.assertEquals("", map.get(key, view));
                }
                Assert.assertNull(map.get(512, view));
                TestUtils.assertEquals("", map.get(0, view));
                Assert.assertEquals(address, view.lo());
                Assert.assertEquals(address, retained.lo());
                TestUtils.assertEquals("", retained);
                budget.setLimit(Long.MAX_VALUE);
                Assert.assertEquals(DirectIntCharSequenceHashMap.NOT_INSERTED, map.tryPutIfAbsent(512, 0, 0));
                assertCacheBytes(13_120, map, budget, before);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE));
        });
    }

    @Test
    public void testConcurrentCacheDenseGrowthBudgetFallback() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(96, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                for (int key = 0; key < 96; key++) {
                    Assert.assertTrue(put(map, key, ""));
                }
                long before = map.allocatedBytes();
                budget.setLimit(before + 16 + 256L * Long.BYTES);
                Assert.assertTrue(put(map, 256, ""));
                Assert.assertTrue(put(map, 257, ""));
                Assert.assertEquals(budget.getLimit(), map.allocatedBytes());
                Assert.assertEquals(map.allocatedBytes(), budget.getUsedBytes());
                Assert.assertEquals(98, map.size());
                Assert.assertTrue(map.isFilling());
                Assert.assertEquals(0, map.getStopReason());
                DirectString view = new DirectString();
                for (int key = 0; key < 96; key++) {
                    TestUtils.assertEquals("", map.get(key, view));
                }
                TestUtils.assertEquals("", map.get(256, view));
                TestUtils.assertEquals("", map.get(257, view));
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testConcurrentCacheDenseGrowthOomReleasesReservation() throws Exception {
        assertMemoryLeak(() -> {
            long rssLimit = Unsafe.getRssMemLimit();
            NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(96, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                for (int key = 0; key < 96; key++) {
                    Assert.assertTrue(put(map, key, ""));
                }
                long before = map.allocatedBytes();
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 16 + 256L * Long.BYTES);
                Assert.assertEquals(DirectIntCharSequenceHashMap.STOPPED, map.tryPutIfAbsent(256, 0, 0));
                Unsafe.setRssMemLimit(rssLimit);
                Assert.assertEquals(DirectIntCharSequenceHashMap.STOP_OOM, map.getStopReason());
                Assert.assertFalse(map.isFilling());
                Assert.assertEquals(96, map.size());
                Assert.assertEquals(before, map.allocatedBytes());
                Assert.assertEquals(before, budget.getUsedBytes());
                DirectString view = new DirectString();
                for (int key = 0; key < 96; key++) {
                    TestUtils.assertEquals("", map.get(key, view));
                }
                Assert.assertNull(map.get(256, view));
                Assert.assertEquals(DirectIntCharSequenceHashMap.NOT_INSERTED, map.tryPutIfAbsent(256, 0, 0));
                Assert.assertEquals(before, budget.getUsedBytes());
            } finally {
                Unsafe.setRssMemLimit(rssLimit);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testConcurrentCacheDenseGrowthRetiredIndexAccounting() throws Exception {
        assertMemoryLeak(() -> {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE);
            NativeMemoryBudget budget = new NativeMemoryBudget(6480);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_SYMBOL_CACHE, budget)) {
                for (int key = 4096; key <= 4140; key++) {
                    Assert.assertEquals(DirectIntCharSequenceHashMap.INSERTED, map.tryPutIfAbsent(key, 0, 0));
                }
                DirectString retained = map.get(4096, new DirectString());
                long address = retained.lo();
                // Retain indexes with 8, 16, 32, 64 and 128 slots, each with a 16-byte header.
                assertCacheBytes(2064 + 256 + 32, map, budget, before);
                Assert.assertEquals(DirectIntCharSequenceHashMap.INSERTED, map.tryPutIfAbsent(4224, 0, 0));
                assertCacheBytes(4128 + 256 + 32, map, budget, before);
                Assert.assertEquals(46, map.size());
                Assert.assertEquals(DirectIntCharSequenceHashMap.INSERTED, map.tryPutIfAbsent(4352, 0, 0));
                // Keep both the direct 256-slot index and its same-size sparse replacement.
                assertCacheBytes(6192 + 256 + 32, map, budget, before);
                Assert.assertEquals(budget.getLimit(), budget.getUsedBytes());
                Assert.assertEquals(47, map.size());
                Assert.assertTrue(map.isFilling());
                DirectString view = new DirectString();
                for (int key = 4096; key <= 4140; key++) {
                    TestUtils.assertEquals("", map.get(key, view));
                }
                TestUtils.assertEquals("", map.get(4224, view));
                TestUtils.assertEquals("", map.get(4352, view));
                Assert.assertNull(map.get(4141, view));
                TestUtils.assertEquals("", map.get(4096, view));
                Assert.assertEquals(address, view.lo());
                Assert.assertEquals(address, retained.lo());
                TestUtils.assertEquals("", retained);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE));
        });
    }

    @Test
    public void testConcurrentCacheDenseGrowthRoundedThreshold() throws Exception {
        assertMemoryLeak(() -> {
            for (int count : new int[]{178, 179}) {
                long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE);
                NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE);
                try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(256, 0.7, MemoryTag.NATIVE_SYMBOL_CACHE, budget)) {
                    for (int key = 0; key < count; key++) {
                        Assert.assertEquals(DirectIntCharSequenceHashMap.INSERTED, map.tryPutIfAbsent(key, 0, 0));
                    }
                    DirectString retained = map.get(0, new DirectString());
                    long address = retained.lo();
                    assertCacheBytes(4112 + 256 + 512 + 32, map, budget, before);
                    Assert.assertEquals(DirectIntCharSequenceHashMap.INSERTED, map.tryPutIfAbsent(512, 0, 0));
                    // floor(512 * 0.7) = 358; only size 179 permits optional doubling.
                    assertCacheBytes(4912 + (count == 178 ? 4112 : 8208), map, budget, before);
                    Assert.assertEquals(count + 1, map.size());
                    Assert.assertTrue(map.isFilling());
                    DirectString view = new DirectString();
                    for (int key = 0; key < count; key++) {
                        TestUtils.assertEquals("", map.get(key, view));
                    }
                    TestUtils.assertEquals("", map.get(512, view));
                    Assert.assertNull(map.get(1024, view));
                    TestUtils.assertEquals("", map.get(0, view));
                    Assert.assertEquals(address, view.lo());
                    Assert.assertEquals(address, retained.lo());
                    TestUtils.assertEquals("", retained);
                }
                Assert.assertEquals(0, budget.getUsedBytes());
                Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE));
            }
        });
    }

    @Test
    public void testConcurrentCacheDenseGrowthSlotLimit() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(96, 0.7, MemoryTag.NATIVE_DEFAULT, null)) {
                map.setMaxSlots(256);
                for (int key = 0; key < 96; key++) {
                    Assert.assertTrue(put(map, key, ""));
                }
                long before = map.allocatedBytes();
                Assert.assertTrue(put(map, 256, ""));
                Assert.assertEquals(before + 16 + 256L * Long.BYTES, map.allocatedBytes());
                Assert.assertEquals(97, map.size());
                Assert.assertTrue(map.isFilling());
                DirectString view = new DirectString();
                for (int key = 0; key < 96; key++) {
                    TestUtils.assertEquals("", map.get(key, view));
                }
                TestUtils.assertEquals("", map.get(256, view));
            }
        });
    }

    @Test
    public void testConcurrentCacheDenseGrowthSpanLimit() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(96, 0.7, MemoryTag.NATIVE_DEFAULT, null)) {
                for (int key = 0; key < 96; key++) {
                    Assert.assertTrue(put(map, key, ""));
                }
                long before = map.allocatedBytes();
                Assert.assertTrue(put(map, 512, ""));
                Assert.assertEquals(before + 16 + 256L * Long.BYTES, map.allocatedBytes());
                Assert.assertEquals(97, map.size());
                Assert.assertTrue(map.isFilling());
                DirectString view = new DirectString();
                for (int key = 0; key < 96; key++) {
                    TestUtils.assertEquals("", map.get(key, view));
                }
                TestUtils.assertEquals("", map.get(512, view));
            }
        });
    }

    @Test
    public void testConcurrentCacheDenseGrowthSparseJumps() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(96, 0.7, MemoryTag.NATIVE_DEFAULT, null)) {
                for (int key = 0; key < 96; key++) {
                    Assert.assertTrue(put(map, key, ""));
                }
                long before = map.allocatedBytes();
                for (int key = 256; key <= 32_768; key *= 2) {
                    Assert.assertTrue(put(map, key, ""));
                    Assert.assertEquals(before + (key == 256 ? 1 : 2) * (16 + 512L * Long.BYTES), map.allocatedBytes());
                }
                Assert.assertEquals(104, map.size());
                Assert.assertTrue(map.isFilling());
                DirectString view = new DirectString();
                for (int key = 0; key < 96; key++) {
                    TestUtils.assertEquals("", map.get(key, view));
                }
                for (int key = 256; key <= 32_768; key *= 2) {
                    TestUtils.assertEquals("", map.get(key, view));
                }
            }
        });
    }

    @Test
    public void testConcurrentCacheDirectIndexAliases() throws Exception {
        assertMemoryLeak(() -> {
            for (int base : new int[]{Integer.MIN_VALUE, -65, 65, Integer.MAX_VALUE - 95}) {
                try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(96, 0.7, MemoryTag.NATIVE_DEFAULT, null)) {
                    for (int i = 95; i > 0; i -= 2) {
                        Assert.assertTrue(put(map, base + i, "value" + i));
                    }
                    DirectString view = new DirectString();
                    for (int i = 0; i < 96; i++) {
                        if ((i & 1) == 0) {
                            Assert.assertNull(map.get(base + i, view));
                        } else {
                            TestUtils.assertEquals("value" + i, map.get(base + i, view));
                        }
                        Assert.assertNull(map.get((base + i) ^ 256, view));
                    }
                    int alias = (base + 1) ^ 256;
                    Assert.assertTrue(put(map, alias, "alias"));
                    TestUtils.assertEquals("alias", map.get(alias, view));
                    for (int i = 1; i < 96; i += 2) {
                        TestUtils.assertEquals("value" + i, map.get(base + i, view));
                    }
                    Assert.assertEquals(49, map.size());
                }
            }
        });
    }

    @Test
    public void testConcurrentCacheReindexBudgetLimit() throws Exception {
        for (boolean isRssFailure : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                long rssLimit = Unsafe.getRssMemLimit();
                NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE);
                try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                    for (int key = 0; key < 1024; key++) {
                        Assert.assertTrue(put(map, key, "value" + key));
                    }
                    if (isRssFailure) {
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                    } else {
                        budget.setLimit(budget.getUsedBytes());
                    }
                    Assert.assertEquals(DirectIntCharSequenceHashMap.STOPPED, map.tryPutIfAbsent(Integer.MAX_VALUE, 0, 0));
                    Unsafe.setRssMemLimit(rssLimit);
                    Assert.assertEquals(isRssFailure ? DirectIntCharSequenceHashMap.STOP_OOM : DirectIntCharSequenceHashMap.STOP_BUDGET, map.getStopReason());
                    Assert.assertFalse(map.isFilling());
                    Assert.assertEquals(1024, map.size());
                    Assert.assertEquals(map.allocatedBytes(), budget.getUsedBytes());
                    DirectString view = new DirectString();
                    for (int key = 0; key < 1024; key++) {
                        TestUtils.assertEquals("value" + key, map.get(key, view));
                    }
                    Assert.assertNull(map.get(Integer.MAX_VALUE, view));
                } finally {
                    Unsafe.setRssMemLimit(rssLimit);
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            });
        }
    }

    @Test
    public void testConcurrentCacheSparseKeyRanges() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, null)) {
                DirectString view = new DirectString();
                DirectString retained = new DirectString();
                for (int key = 0; key < 1024; key++) {
                    Assert.assertTrue(put(map, key, "value" + key));
                }
                map.get(0, retained);
                long address = retained.lo();
                for (int i = 1; i < 32; i++) {
                    for (int key = 0; key < 1024; key++) {
                        int sparseKey = (i << 20) + key;
                        Assert.assertTrue(put(map, sparseKey, "value" + sparseKey));
                    }
                }
                Assert.assertTrue(put(map, Integer.MIN_VALUE, "min"));
                Assert.assertTrue(put(map, Integer.MAX_VALUE, "max"));
                for (int i = 0; i < 32; i++) {
                    for (int key = 0; key < 1024; key++) {
                        int sparseKey = (i << 20) + key;
                        TestUtils.assertEquals("value" + sparseKey, map.get(sparseKey, view));
                    }
                    Assert.assertNull(map.get((i << 20) + 1024, view));
                }
                TestUtils.assertEquals("min", map.get(Integer.MIN_VALUE, view));
                TestUtils.assertEquals("max", map.get(Integer.MAX_VALUE, view));
                Assert.assertEquals(32_770, map.size());
                Assert.assertEquals(address, retained.lo());
                TestUtils.assertEquals("value0", retained);
            }
        });
    }

    @Test
    public void testConcurrentCacheStoppedKeyRange() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(496);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                for (int key = -10; key <= 10; key += 5) {
                    Assert.assertTrue(put(map, key, "v"));
                }
                Assert.assertEquals(DirectIntCharSequenceHashMap.STOPPED, putResult(map, 1000, "v"));
                DirectString view = new DirectString();
                for (int key = -10; key <= 10; key += 5) {
                    TestUtils.assertEquals("v", map.get(key, view));
                }
                for (int key : new int[]{Integer.MIN_VALUE, -11, -1, 1, 11, 1000, Integer.MAX_VALUE}) {
                    Assert.assertNull(map.get(key, view));
                }
                budget.setLimit(1024 * 1024);
                Assert.assertEquals(DirectIntCharSequenceHashMap.NOT_INSERTED, putResult(map, 1000, "v"));
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testConcurrentCacheUnsignedChunkReferences() throws Exception {
        assertMemoryLeak(() -> {
            String prefix = "x".repeat(116);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, null)) {
                for (int key = 0; key < 16_383; key++) {
                    map.setNextChunkSize(256);
                    Assert.assertTrue(put(map, key, prefix + (100_000 + key)));
                }
                Assert.assertEquals(DirectIntCharSequenceHashMap.STOPPED, putResult(map, 16_383, "next"));
                Assert.assertEquals(DirectIntCharSequenceHashMap.STOP_LIMIT, map.getStopReason());
                Assert.assertTrue(map.allocatedBytes() < 6 * 1024 * 1024);
                DirectString view = new DirectString();
                for (int key = 0; key < 16_383; key++) {
                    TestUtils.assertEquals(prefix + (100_000 + key), map.get(key, view));
                }
            }
        });
    }

    @Test
    public void testConcurrentCacheStructuralLimits() throws Exception {
        for (boolean isChunkLimit : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, null)) {
                    if (isChunkLimit) {
                        map.setMaxChunks(3);
                    } else {
                        map.setMaxSlots(8);
                    }
                    String value = "x".repeat(isChunkLimit ? 4094 : 1);
                    int count = isChunkLimit ? 11 : 5;
                    for (int i = 0; i < count; i++) {
                        Assert.assertTrue(put(map, i, value));
                    }
                    Assert.assertEquals(DirectIntCharSequenceHashMap.STOPPED, putResult(map, count, value));
                    Assert.assertFalse(map.isFilling());
                    Assert.assertEquals(DirectIntCharSequenceHashMap.STOP_LIMIT, map.getStopReason());
                    Assert.assertEquals(count, map.size());
                    DirectString view = new DirectString();
                    TestUtils.assertEquals(value, map.get(0, view));
                    TestUtils.assertEquals(value, map.get(count - 1, view));
                    Assert.assertEquals(DirectIntCharSequenceHashMap.NOT_INSERTED, putResult(map, count, value));
                    try {
                        map.setMaxChunks(16_384);
                        Assert.fail();
                    } catch (IllegalArgumentException expected) {
                        Assert.assertEquals("invalid chunk limit", expected.getMessage());
                    }
                }
            });
        }
    }

    @Test
    public void testConcurrentCacheOversizedValuesDoNotAllocateOrStop() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, null)) {
                for (int count : new int[]{0, 5}) {
                    while (map.size() < count) {
                        Assert.assertTrue(put(map, map.size(), "small"));
                    }
                    long bytes = map.allocatedBytes();
                    Assert.assertEquals(DirectIntCharSequenceHashMap.NOT_INSERTED, putResult(map, 42, "x".repeat(4095)));
                    Assert.assertEquals(DirectIntCharSequenceHashMap.NOT_INSERTED, map.tryPutIfAbsent(42, 1, Integer.MAX_VALUE));
                    Assert.assertEquals(bytes, map.allocatedBytes());
                    Assert.assertEquals(count, map.size());
                    Assert.assertTrue(map.isFilling());
                }
                Assert.assertTrue(put(map, 42, "x".repeat(4094)));
                Assert.assertEquals(4094, map.get(42, new DirectString()).length());
            }
        });
    }

    @Test
    public void testConcurrentCacheAllocationFailures() throws Exception {
        assertCacheAllocationFailure(new ObjList<>("a"), 1);
        assertCacheAllocationFailure(new ObjList<>("a"), 2);
        assertCacheAllocationFailure(new ObjList<>("a"), 3);
        assertCacheAllocationFailure(new ObjList<>("a", "b", "c", "d", "e", "f"), 4);
        assertCacheAllocationFailure(new ObjList<>("x".repeat(122), "y"), 4);
        ObjList<String> values = new ObjList<>();
        for (int i = 0; i < 12; i++) {
            values.add("x".repeat(4094));
        }
        assertCacheAllocationFailure(values, 8);
        assertCacheAllocationFailure(values, 9);
    }

    @Test
    public void testConcurrentCacheChunkBoundariesAndStableViews() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(16 * 1024 * 1024);
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                DirectString retained = new DirectString();
                DirectString view = new DirectString();
                Assert.assertTrue(put(map, -1, "retained"));
                map.get(-1, retained);
                long address = retained.lo();
                int[] lengths = {0, 1, 125, 126, 127, 4093, 4094};
                for (int i = 0; i < lengths.length; i++) {
                    String value = "界".repeat(lengths[i]);
                    Assert.assertTrue(put(map, i, value));
                    TestUtils.assertEquals(value, map.get(i, view));
                }
                for (int i = 10; i < 20_000; i++) {
                    Assert.assertTrue(put(map, i, "value" + i));
                }
                for (int i = 10; i < 20_000; i++) {
                    TestUtils.assertEquals("value" + i, map.get(i, view));
                }
                TestUtils.assertEquals("retained", retained);
                Assert.assertEquals(address, retained.lo());
                Assert.assertEquals(budget.getUsedBytes(), map.allocatedBytes());
                Assert.assertEquals(map.allocatedBytes(), Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) - before);
                // Includes every retired index; even sparse keys need no dense key array.
                Assert.assertTrue(map.allocatedBytes() < 2 * 1024 * 1024);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testConcurrentCacheKeysAndLazyAllocation() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                DirectString view = new DirectString();
                Assert.assertNull(map.get(0, view));
                Assert.assertEquals(0, map.allocatedBytes());
                int[] keys = {0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE};
                for (int key : keys) {
                    String value = "symbol" + key;
                    Assert.assertTrue(put(map, key, value));
                    long bytes = map.allocatedBytes();
                    Assert.assertFalse(put(map, key, "replacement"));
                    Assert.assertEquals(bytes, map.allocatedBytes());
                    TestUtils.assertEquals(value, map.get(key, view));
                }
                Assert.assertEquals(keys.length, map.size());
                map.get(Integer.MAX_VALUE, view);
                Assert.assertEquals(DirectIntCharSequenceHashMap.INSERTED, map.tryPutIfAbsent(42, view.lo(), view.length()));
                TestUtils.assertEquals("symbol" + Integer.MAX_VALUE, map.get(42, view));
                map.close();
                Assert.assertEquals(0, map.allocatedBytes());
                Assert.assertNull(map.get(0, view));
                Assert.assertFalse(put(map, 0, "closed"));
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testConcurrentCacheReadersAndWriters() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(8 * 1024 * 1024);
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                AtomicInteger inserts = new AtomicInteger();
                TestUtils.runConcurrently(8, id -> {
                    DirectString view = new DirectString();
                    for (int i = 0; i < 20_000; i++) {
                        int key = (i * 31 + id) % 4000;
                        String value = "symbol" + key;
                        if (id < 4 && put(map, key, value)) {
                            inserts.incrementAndGet();
                        }
                        CharSequence result = map.get(key, view);
                        if (result != null) {
                            TestUtils.assertEquals(value, result);
                        }
                    }
                });
                Assert.assertEquals(inserts.get(), map.size());
                Assert.assertTrue(map.size() > 1000);
                Assert.assertTrue(map.size() <= 4000);
                Assert.assertEquals(budget.getUsedBytes(), map.allocatedBytes());
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testConcurrentCacheStopRacingWriters() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger reservations = new AtomicInteger();
            AtomicInteger stops = new AtomicInteger();
            NativeMemoryBudget budget = new NativeMemoryBudget(4096) {
                @Override
                public boolean tryReserve(long bytes) {
                    reservations.incrementAndGet();
                    return super.tryReserve(bytes);
                }
            };
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                TestUtils.runConcurrently(16, id -> {
                    DirectString view = new DirectString();
                    int lastInserted = -1;
                    for (int i = 0; i < 10_000; i++) {
                        int key = i * 16 + id;
                        int result = putResult(map, key, "value");
                        if (result == DirectIntCharSequenceHashMap.INSERTED) {
                            lastInserted = key;
                        } else if (result == DirectIntCharSequenceHashMap.STOPPED) {
                            stops.incrementAndGet();
                        }
                        if (lastInserted != -1) {
                            TestUtils.assertEquals("value", map.get(lastInserted, view));
                        }
                    }
                    Assert.assertFalse(map.isFilling());
                    int calls = reservations.get();
                    for (int i = 0; i < 10_000; i++) {
                        Assert.assertFalse(put(map, -i, "value"));
                    }
                    Assert.assertEquals(calls, reservations.get());
                });
                Assert.assertTrue(map.size() > 0);
                Assert.assertEquals(1, stops.get());
                Assert.assertEquals(DirectIntCharSequenceHashMap.STOP_BUDGET, map.getStopReason());
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testConcurrentCacheUnexpectedFailureReleasesUnpublishedBlocks() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger calls = new AtomicInteger();
            NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024) {
                @Override
                public boolean tryReserve(long bytes) {
                    if (calls.incrementAndGet() == 3) {
                        throw new IllegalStateException("allocation failed");
                    }
                    return super.tryReserve(bytes);
                }
            };
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                try {
                    put(map, 0, "value");
                    Assert.fail();
                } catch (IllegalStateException expected) {
                    Assert.assertEquals("allocation failed", expected.getMessage());
                }
                Assert.assertEquals(0, map.size());
                Assert.assertEquals(80, map.allocatedBytes());
                Assert.assertEquals(map.allocatedBytes(), budget.getUsedBytes());
                Assert.assertTrue(map.isFilling());
                Assert.assertTrue(put(map, 0, "success"));
                TestUtils.assertEquals("success", map.get(0, new DirectString()));
            }
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testNativeMemoryBudgetOverflow() throws Exception {
        assertMemoryLeak(() -> {
            NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE);
            Assert.assertTrue(budget.tryReserve(Long.MAX_VALUE - 1));
            Assert.assertFalse(budget.tryReserve(2));
            Assert.assertTrue(budget.tryReserve(1));
            Assert.assertFalse(budget.tryReserve(1));
            budget.release(Long.MAX_VALUE);
            Assert.assertEquals(0, budget.getUsedBytes());
        });
    }

    @Test
    public void testClearResetsStateButKeepsBuffer() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < 8; i++) {
                    Assert.assertEquals(i, map.intern("sym" + i));
                }
                Assert.assertEquals(8, map.size());

                map.clear();
                Assert.assertEquals(0, map.size());
                Assert.assertNull(map.valueOf(0));
                Assert.assertEquals(-1, map.keyOf("sym3"));

                // Reuse after clear: keys restart at 0.
                Assert.assertEquals(0, map.intern("fresh0"));
                Assert.assertEquals(1, map.intern("fresh1"));
                Assert.assertEquals(0, map.intern("fresh0"));
                Assert.assertEquals("fresh1", Chars.toString(map.valueOf(1)));
            }
        });
    }

    @Test
    public void testCopyFromPreservesInternKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    DirectSymbolMap src = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT);
                    DirectSymbolMap dst = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)
            ) {
                Assert.assertEquals(0, src.intern("AAPL"));
                Assert.assertEquals(1, src.intern("MSFT"));
                Assert.assertEquals(2, src.intern("GOOG"));

                // dst already has unrelated contents; copyFrom should discard them.
                dst.intern("DELETE_ME");
                dst.copyFrom(src);

                Assert.assertEquals(3, dst.size());
                Assert.assertEquals("AAPL", Chars.toString(dst.valueOf(0)));
                Assert.assertEquals("MSFT", Chars.toString(dst.valueOf(1)));
                Assert.assertEquals("GOOG", Chars.toString(dst.valueOf(2)));
                Assert.assertEquals(0, dst.keyOf("AAPL"));
                Assert.assertEquals(2, dst.keyOf("GOOG"));
                Assert.assertEquals(-1, dst.keyOf("DELETE_ME"));
            }
        });
    }

    @Test
    public void testGrowthPreservesExistingValues() throws Exception {
        // Start with a tiny buffer and populate enough entries to force several reallocations.
        assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            final int n = 5_000;
            String[] values = new String[n];
            try (DirectSymbolMap map = new DirectSymbolMap(16, 4, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < n; i++) {
                    values[i] = "sym_" + i + "_" + rnd.nextString(rnd.nextInt(32) + 1);
                    int key = map.intern(values[i]);
                    Assert.assertEquals(i, key);
                }
                Assert.assertEquals(n, map.size());

                for (int i = 0; i < n; i++) {
                    Assert.assertEquals(values[i], Chars.toString(map.valueOf(i)));
                    Assert.assertEquals(i, map.keyOf(values[i]));
                }
            }
        });
    }

    @Test
    public void testInternDedupsRepeatValues() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                Assert.assertEquals(0, map.intern("AAPL"));
                Assert.assertEquals(1, map.intern("MSFT"));
                Assert.assertEquals(0, map.intern("AAPL"));
                Assert.assertEquals(1, map.intern("MSFT"));
                Assert.assertEquals(2, map.intern("GOOG"));
                Assert.assertEquals(3, map.size());

                Assert.assertEquals("AAPL", Chars.toString(map.valueOf(0)));
                Assert.assertEquals("MSFT", Chars.toString(map.valueOf(1)));
                Assert.assertEquals("GOOG", Chars.toString(map.valueOf(2)));
                Assert.assertNull(map.valueOf(3));

                Assert.assertEquals(0, map.keyOf("AAPL"));
                Assert.assertEquals(2, map.keyOf("GOOG"));
                Assert.assertEquals(-1, map.keyOf("UNKNOWN"));
            }
        });
    }

    @Test
    public void testPutExternallyKeyedOverwritesExisting() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                map.put(10, "first");
                map.put(20, "second");
                Assert.assertEquals(2, map.size());
                Assert.assertEquals("first", Chars.toString(map.valueOf(10)));
                Assert.assertEquals("second", Chars.toString(map.valueOf(20)));

                // Overwrite existing key: size unchanged, value updated.
                map.put(10, "updated");
                Assert.assertEquals(2, map.size());
                Assert.assertEquals("updated", Chars.toString(map.valueOf(10)));

                // keyOf on put-only map returns -1 (no reverse index).
                Assert.assertEquals(-1, map.keyOf("updated"));
            }
        });
    }

    @Test
    public void testExplicitKeyLookupDoesNotScanValues() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                final int symbolCount = 4096;
                for (int i = 0; i < symbolCount; i++) {
                    map.put(i, "value-" + i);
                }

                final class CountingCharSequence extends AbstractCharSequence {
                    private int accesses;
                    private final String value = "absent-value";

                    @Override
                    public char charAt(int index) {
                        accesses++;
                        return value.charAt(index);
                    }

                    @Override
                    public int length() {
                        accesses++;
                        return value.length();
                    }

                    @Override
                    protected CharSequence _subSequence(int start, int end) {
                        return value.subSequence(start, end);
                    }
                }

                final CountingCharSequence probe = new CountingCharSequence();
                Assert.assertEquals(-1, map.keyOf(probe, 0, symbolCount));
                Assert.assertTrue(
                        "a miss must hash the probe once, not compare every explicit symbol [accesses="
                                + probe.accesses + ']',
                        probe.accesses <= probe.value.length() * 4
                );
            }
        });
    }

    @Test
    public void testExplicitKeyLookupHonorsBoundsDuplicatesAndOverwrites() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                map.put(2, "X");
                map.put(3, "Y");
                map.put(2, "Y");

                Assert.assertEquals(-1, map.keyOf("X", 0, 4));
                Assert.assertEquals(2, map.keyOf("Y", 2, 3));
                Assert.assertEquals(3, map.keyOf("Y", 3, 4));
                Assert.assertEquals(-1, map.keyOf("Y", 0, 2));
            }
        });
    }

    @Test
    public void testExplicitReverseIndexIncrementalAfterBuild() throws Exception {
        // Once the explicit reverse index has been built by a bounded keyOf, later put()s must keep
        // it correct without a full rebuild: new keys extend it incrementally, and an overwrite falls
        // back to a rebuild so the stale mapping is dropped.
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                map.put(0, "a");
                map.put(1, "b");
                // Build the explicit reverse index.
                Assert.assertEquals(0, map.keyOf("a", 0, 2));
                Assert.assertEquals(1, map.keyOf("b", 0, 2));

                // New keys added after the index is built must be found (incremental maintenance).
                map.put(2, "c");
                map.put(3, "a"); // duplicate value "a" under a fresh key -> both keys must be retained
                Assert.assertEquals(2, map.keyOf("c", 0, 4));
                Assert.assertEquals(0, map.keyOf("a", 0, 1));
                Assert.assertEquals(3, map.keyOf("a", 3, 4));
                Assert.assertEquals(-1, map.keyOf("a", 1, 3));

                // Overwriting an existing key must be reflected: the stale (oldOffset, key) entry is
                // dropped by the rebuild fallback.
                map.put(1, "z");
                Assert.assertEquals(-1, map.keyOf("b", 0, 4));
                Assert.assertEquals(1, map.keyOf("z", 0, 4));
                // "a" at key 3 survives the overwrite rebuild (single in-band match, deterministic).
                Assert.assertEquals(3, map.keyOf("a", 3, 4));
            }
        });
    }

    @Test
    public void testExplicitReverseIndexIncrementalMaintenanceStaysLinear() throws Exception {
        // Regression for the quadratic reverse-index rebuild: WAL replay put()s one transaction's
        // symbols then a bounded keyOf runs per transaction (filter init). If every keyOf triggered
        // a full clear+rescan of the cumulative map, total scan work would be 1+2+...+N = O(N^2).
        // Incremental maintenance keeps it linear: the index is rebuilt at most once (the first
        // lookup), and every later put() extends it in O(1).
        assertMemoryLeak(() -> {
            final int n = 2000;
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                for (int t = 0; t < n; t++) {
                    map.put(t, "sym" + t);
                    // filter init resolves the just-added symbol within the transaction's key band
                    Assert.assertEquals(t, map.keyOf("sym" + t, 0, t + 1));
                }
                final long scanned = map.getExplicitRebuildScannedEntries();
                Assert.assertTrue(
                        "reverse-index rebuild scan work must stay near-linear across " + n
                                + " transactions, but scanned " + scanned + " entries",
                        scanned <= 8L * n
                );
            }
        });
    }

    @Test
    public void testPutNullStoresNullSentinel() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                map.put(5, "real");
                map.put(7, null);
                Assert.assertEquals("real", Chars.toString(map.valueOf(5)));
                Assert.assertNull(map.valueOf(7));
                Assert.assertNull(map.valueOf(999));
            }
        });
    }

    @Test
    public void testValueOfIntoCallerView() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                map.intern("one");
                map.intern("two");
                map.intern("three");

                DirectString viewA = new DirectString();
                DirectString viewB = new DirectString();

                // Bind two views independently; each must survive the other's later overwrite.
                map.valueOf(0, viewA);
                map.valueOf(2, viewB);
                Assert.assertEquals("one", Chars.toString(viewA));
                Assert.assertEquals("three", Chars.toString(viewB));

                // Rebind B elsewhere; A remains untouched.
                map.valueOf(1, viewB);
                Assert.assertEquals("one", Chars.toString(viewA));
                Assert.assertEquals("two", Chars.toString(viewB));
            }
        });
    }

    @Test
    public void testInternRejectsNull() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                try {
                    map.intern(null);
                    Assert.fail("intern(null) must be rejected");
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "does not accept null"));
                }
            }
        });
    }

    @Test
    public void testInternResolvesHashCollision() throws Exception {
        // "Aa" and "BB" share the classic Java String hash (31 * 'A' + 'a' == 31 * 'B' + 'B' == 2112),
        // so both probes land on the same initial slot in ValueToKeyMap. Insertion and lookup must
        // walk past the colliding non-matching slot and resolve to the correct entry.
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                Assert.assertEquals(0, map.intern("Aa"));
                Assert.assertEquals(1, map.intern("BB"));
                Assert.assertEquals(0, map.intern("Aa"));
                Assert.assertEquals(1, map.intern("BB"));
                Assert.assertEquals(2, map.size());

                Assert.assertEquals("Aa", Chars.toString(map.valueOf(0)));
                Assert.assertEquals("BB", Chars.toString(map.valueOf(1)));
                Assert.assertEquals(0, map.keyOf("Aa"));
                Assert.assertEquals(1, map.keyOf("BB"));
            }
        });
    }

    @Test
    public void testEmptyStringRoundTrips() throws Exception {
        // Length-0 entries exercise the edge of append/matches/hashBytes where the
        // char loop body never runs. Verify both intern and put modes accept "" and
        // distinguish it from null and from a missing key.
        assertMemoryLeak(() -> {
            try (DirectSymbolMap intern = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                Assert.assertEquals(0, intern.intern(""));
                Assert.assertEquals(1, intern.intern("nonempty"));
                Assert.assertEquals(0, intern.intern(""));
                Assert.assertEquals("", Chars.toString(intern.valueOf(0)));
                Assert.assertEquals(0, intern.keyOf(""));
                Assert.assertEquals(-1, intern.keyOf("unknown"));
            }
            try (DirectSymbolMap extern = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                extern.put(42, "");
                extern.put(43, null);
                Assert.assertEquals("", Chars.toString(extern.valueOf(42)));
                Assert.assertNull(extern.valueOf(43));
            }
        });
    }

    @Test
    public void testCopyFromPutOnlySource() throws Exception {
        // Put-only source has no ValueToKeyMap; copyFrom should replicate the
        // (key, value) entries with the same int keys and leave the destination
        // in put-only mode too (keyOf returns -1).
        assertMemoryLeak(() -> {
            try (
                    DirectSymbolMap src = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT);
                    DirectSymbolMap dst = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)
            ) {
                src.put(0, "a");
                src.put(1, "b");
                src.put(2, "c");
                dst.copyFrom(src);

                Assert.assertEquals(3, dst.size());
                Assert.assertEquals("a", Chars.toString(dst.valueOf(0)));
                Assert.assertEquals("b", Chars.toString(dst.valueOf(1)));
                Assert.assertEquals("c", Chars.toString(dst.valueOf(2)));
                Assert.assertEquals(-1, dst.keyOf("a"));
                Assert.assertEquals(-1, dst.keyOf("b"));
            }
        });
    }

    @Test
    public void testNonAsciiContentRoundTrips() throws Exception {
        // End-to-end UTF-16: write via putChar, read via getChar, hash/equals by char value.
        // Non-BMP chars encoded as surrogate pairs are treated as two chars, which is
        // consistent with CharSequence.charAt — no special handling needed.
        assertMemoryLeak(() -> {
            String cyrillic = "Пример";
            String japanese = "日本語"; // nihongo
            String surrogatePair = "🚀"; // U+1F680 rocket
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                Assert.assertEquals(0, map.intern(cyrillic));
                Assert.assertEquals(1, map.intern(japanese));
                Assert.assertEquals(2, map.intern(surrogatePair));
                Assert.assertEquals(0, map.intern(cyrillic));

                Assert.assertEquals(cyrillic, Chars.toString(map.valueOf(0)));
                Assert.assertEquals(japanese, Chars.toString(map.valueOf(1)));
                Assert.assertEquals(surrogatePair, Chars.toString(map.valueOf(2)));

                Assert.assertEquals(0, map.keyOf(cyrillic));
                Assert.assertEquals(1, map.keyOf(japanese));
                Assert.assertEquals(2, map.keyOf(surrogatePair));
            }
        });
    }

    @Test
    public void testReopenAfterClose() throws Exception {
        // Reopenable contract: a closed map can be re-initialized via reopen()
        // and used again. Prior state must not leak into the reopened instance.
        assertMemoryLeak(() -> {
            DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT);
            try (map) {
                map.intern("before");
                map.intern("close");
            }
            try (map) {
                map.reopen();
                Assert.assertEquals(0, map.size());
                Assert.assertNull(map.valueOf(0));
                Assert.assertEquals(-1, map.keyOf("before"));

                // Fresh interns start from key 0 on the reopened instance.
                Assert.assertEquals(0, map.intern("after"));
                Assert.assertEquals(1, map.intern("reopen"));
                Assert.assertEquals("after", Chars.toString(map.valueOf(0)));
                Assert.assertEquals(0, map.keyOf("after"));
            }
        });
    }

    private static void assertCacheBytes(long expected, DirectIntCharSequenceHashMap map, NativeMemoryBudget budget, long before) {
        Assert.assertEquals(expected, map.allocatedBytes());
        Assert.assertEquals(expected, budget.getUsedBytes());
        Assert.assertEquals(expected, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE) - before);
    }

    private static void assertCacheDirectoryGrowthFailure(int count, int failureMode) throws Exception {
        assertMemoryLeak(() -> {
            long rssLimit = Unsafe.getRssMemLimit();
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE);
            long indexBytes = 80 + (count >= 6 ? 144 : 0) + (count >= 12 ? 272 : 0);
            long directoryBytes = 32 + (count >= 4 ? 64 : 0) + (count >= 8 ? 128 : 0);
            long expectedBytes = indexBytes + directoryBytes + count * 256L;
            long newDirectoryBytes = (count + 1) * 2L * Long.BYTES;
            AtomicInteger calls = new AtomicInteger();
            AtomicInteger failAt = new AtomicInteger(Integer.MAX_VALUE);
            NativeMemoryBudget budget = new NativeMemoryBudget(Long.MAX_VALUE) {
                @Override
                public boolean tryReserve(long bytes) {
                    if (calls.incrementAndGet() == failAt.get()) {
                        Assert.assertEquals(newDirectoryBytes, bytes);
                        // The new chunk succeeded, but the directory has not published yet.
                        Assert.assertEquals(expectedBytes + 256, getUsedBytes());
                        Assert.assertEquals(expectedBytes + 256, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE) - before);
                        switch (failureMode) {
                            case 0 -> setLimit(getUsedBytes());
                            case 1 -> Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                            default -> throw new IllegalStateException("directory allocation failed");
                        }
                    }
                    return super.tryReserve(bytes);
                }
            };
            try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_SYMBOL_CACHE, budget)) {
                ObjList<DirectString> retained = new ObjList<>();
                long[] addresses = new long[count];
                for (int key = 0; key < count; key++) {
                    map.setNextChunkSize(256);
                    Assert.assertTrue(put(map, key, "x".repeat(116) + (100_000 + key)));
                    retained.add(map.get(key, new DirectString()));
                    addresses[key] = retained.getQuick(key).lo();
                }
                assertCacheBytes(expectedBytes, map, budget, before);
                failAt.set(calls.get() + 2);
                map.setNextChunkSize(256);
                String value = "x".repeat(116) + (100_000 + count);
                if (failureMode == 2) {
                    try {
                        putResult(map, count, value);
                        Assert.fail();
                    } catch (IllegalStateException expected) {
                        Assert.assertEquals("directory allocation failed", expected.getMessage());
                    }
                } else {
                    Assert.assertEquals(DirectIntCharSequenceHashMap.STOPPED, putResult(map, count, value));
                }
                Assert.assertEquals(failAt.get(), calls.get());
                assertCacheBytes(expectedBytes, map, budget, before);
                Assert.assertEquals(count, map.size());
                DirectString view = new DirectString();
                Assert.assertNull(map.get(count, view));
                for (int key = 0; key < count; key++) {
                    String expected = "x".repeat(116) + (100_000 + key);
                    TestUtils.assertEquals(expected, retained.getQuick(key));
                    TestUtils.assertEquals(expected, map.get(key, view));
                    Assert.assertEquals(addresses[key], retained.getQuick(key).lo());
                    Assert.assertEquals(addresses[key], view.lo());
                }
                budget.setLimit(Long.MAX_VALUE);
                if (failureMode == 2) {
                    Assert.assertTrue(map.isFilling());
                    Assert.assertEquals(0, map.getStopReason());
                    Assert.assertTrue(put(map, count, value));
                    Assert.assertEquals(failAt.get() + 2, calls.get());
                    assertCacheBytes(expectedBytes + 256 + newDirectoryBytes, map, budget, before);
                    TestUtils.assertEquals(value, map.get(count, view));
                    Assert.assertEquals(count + 1, map.size());
                    for (int key = 0; key < count; key++) {
                        TestUtils.assertEquals("x".repeat(116) + (100_000 + key), map.get(key, view));
                        Assert.assertEquals(addresses[key], view.lo());
                    }
                } else {
                    Assert.assertFalse(map.isFilling());
                    Assert.assertEquals(failureMode == 0 ? DirectIntCharSequenceHashMap.STOP_BUDGET : DirectIntCharSequenceHashMap.STOP_OOM, map.getStopReason());
                    Assert.assertEquals(DirectIntCharSequenceHashMap.NOT_INSERTED, putResult(map, count, value));
                    Assert.assertEquals(failAt.get(), calls.get());
                    assertCacheBytes(expectedBytes, map, budget, before);
                }
            } finally {
                Unsafe.setRssMemLimit(rssLimit);
            }
            Assert.assertEquals(0, budget.getUsedBytes());
            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SYMBOL_CACHE));
        });
    }

    private static boolean put(DirectIntCharSequenceHashMap map, int key, CharSequence value) {
        return putResult(map, key, value) == DirectIntCharSequenceHashMap.INSERTED;
    }

    private static int putResult(DirectIntCharSequenceHashMap map, int key, CharSequence value) {
        long rssLimit = Unsafe.getRssMemLimit();
        try (DirectUtf16Sink sink = new DirectUtf16Sink(2L * (value.length() + 1))) {
            sink.put(value);
            try {
                return map.tryPutIfAbsent(key, sink.ptr(), sink.length());
            } finally {
                // Fault-injecting budgets can lower the RSS limit during map allocation.
                Unsafe.setRssMemLimit(rssLimit);
            }
        }
    }

    private static void assertCacheAllocationFailure(ObjList<String> values, int failAt) throws Exception {
        for (boolean isRssFailure : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                long rssLimit = Unsafe.getRssMemLimit();
                AtomicInteger calls = new AtomicInteger();
                NativeMemoryBudget budget = new NativeMemoryBudget(1024 * 1024) {
                    @Override
                    public boolean tryReserve(long bytes) {
                        if (calls.incrementAndGet() == failAt) {
                            if (!isRssFailure) {
                                return false;
                            }
                            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                        }
                        return super.tryReserve(bytes);
                    }
                };
                try (DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, budget)) {
                    for (int i = 0; i < values.size() - 1; i++) {
                        Assert.assertTrue(put(map, i, values.getQuick(i)));
                    }
                    Assert.assertEquals(DirectIntCharSequenceHashMap.STOPPED, putResult(map, values.size() - 1, values.getQuick(values.size() - 1)));
                    Unsafe.setRssMemLimit(rssLimit);
                    Assert.assertFalse(map.isFilling());
                    Assert.assertEquals(isRssFailure ? DirectIntCharSequenceHashMap.STOP_OOM : DirectIntCharSequenceHashMap.STOP_BUDGET, map.getStopReason());
                    Assert.assertEquals(values.size() - 1, map.size());
                    Assert.assertEquals(map.allocatedBytes(), budget.getUsedBytes());
                    DirectString view = new DirectString();
                    for (int i = 0; i < values.size() - 1; i++) {
                        TestUtils.assertEquals(values.getQuick(i), map.get(i, view));
                    }
                    int before = calls.get();
                    Assert.assertFalse(put(map, 42, "small"));
                    Assert.assertEquals(before, calls.get());
                } finally {
                    Unsafe.setRssMemLimit(rssLimit);
                }
                Assert.assertEquals(0, budget.getUsedBytes());
            });
        }
    }

}

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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectBitSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DirectBitSetTest {
    private static final Log LOG = LogFactory.getLog(DirectBitSetTest.class);

    @Test
    public void testCloseIsIdempotent() throws Exception {
        LOG.info().$("testCloseIsIdempotent").$();
        TestUtils.assertMemoryLeak(() -> {
            DirectBitSet set = new DirectBitSet();
            set.set(100);
            set.close();
            set.close();
            Assert.assertEquals(0, set.capacity());
            Assert.assertFalse(set.get(100));
        });
    }

    @Test
    public void testGetAndSet() throws Exception {
        LOG.info().$("testGetAndSet").$();
        TestUtils.assertMemoryLeak(() -> {
            final int N = 1000;
            try (DirectBitSet set = new DirectBitSet()) {
                Assert.assertTrue(set.capacity() > 0);

                for (int i = 0; i < N; i++) {
                    Assert.assertFalse(set.get(i));
                }
                Assert.assertTrue(set.capacity() >= N);

                for (int i = 0; i < N; i++) {
                    Assert.assertFalse(set.getAndSet(i));
                    Assert.assertTrue(set.get(i));
                }

                for (int i = 0; i < N; i++) {
                    Assert.assertTrue(set.getAndSet(i));
                    Assert.assertTrue(set.get(i));
                }
            }
        });
    }

    @Test
    public void testKeepClosedSkipsInitialAllocation() throws Exception {
        LOG.info().$("testKeepClosedSkipsInitialAllocation").$();
        TestUtils.assertMemoryLeak(() -> {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_BIT_SET);
            try (DirectBitSet set = new DirectBitSet(1024, MemoryTag.NATIVE_BIT_SET, true)) {
                Assert.assertEquals(0, set.capacity());
                Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_BIT_SET));
                Assert.assertFalse(set.get(0));

                set.reserve(2048);
                Assert.assertTrue(set.capacity() >= 2048);
                set.set(1000);
                Assert.assertTrue(set.get(1000));
            }
            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_BIT_SET));
        });
    }

    @Test
    public void testMemoryTagAccounting() throws Exception {
        LOG.info().$("testMemoryTagAccounting").$();
        TestUtils.assertMemoryLeak(() -> {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_BIT_SET);
            try (DirectBitSet set = new DirectBitSet(1_000_000)) {
                long held = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_BIT_SET) - before;
                Assert.assertTrue("expected native bit set allocation accounted, got " + held, held >= 1_000_000 / 8);
                set.set(999_999);
            }
            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_BIT_SET));
        });
    }

    @Test
    public void testNextSetBit() throws Exception {
        LOG.info().$("testNextSetBit").$();
        TestUtils.assertMemoryLeak(() -> {
            try (DirectBitSet set = new DirectBitSet()) {
                Assert.assertEquals(-1, set.nextSetBit(0));
                Assert.assertEquals(-1, set.nextSetBit(100));

                set.set(5);
                Assert.assertEquals(5, set.nextSetBit(-10));
                Assert.assertEquals(5, set.nextSetBit(0));
                Assert.assertEquals(5, set.nextSetBit(5));
                Assert.assertEquals(-1, set.nextSetBit(6));

                set.set(7);
                set.set(63);
                Assert.assertEquals(5, set.nextSetBit(0));
                Assert.assertEquals(7, set.nextSetBit(6));
                Assert.assertEquals(7, set.nextSetBit(7));
                Assert.assertEquals(63, set.nextSetBit(8));
                Assert.assertEquals(63, set.nextSetBit(63));
                Assert.assertEquals(-1, set.nextSetBit(64));

                set.set(200);
                Assert.assertEquals(200, set.nextSetBit(64));
                Assert.assertEquals(200, set.nextSetBit(199));
                Assert.assertEquals(200, set.nextSetBit(200));
                Assert.assertEquals(-1, set.nextSetBit(201));
            }

            try (DirectBitSet set = new DirectBitSet(10_000)) {
                set.set(9999);
                Assert.assertEquals(9999, set.nextSetBit(0));
                Assert.assertEquals(9999, set.nextSetBit(9999));
                Assert.assertEquals(-1, set.nextSetBit(10_000));
            }

            try (DirectBitSet set = new DirectBitSet()) {
                set.set(10);
                set.set(20);
                set.unset(10);
                Assert.assertEquals(20, set.nextSetBit(0));
                set.unset(20);
                Assert.assertEquals(-1, set.nextSetBit(0));
            }

            try (DirectBitSet set = new DirectBitSet(64)) {
                set.set(10);
                Assert.assertEquals(-1, set.nextSetBit(1_000_000));
            }
        });
    }

    @Test
    public void testPreSizedAvoidsResize() throws Exception {
        LOG.info().$("testPreSizedAvoidsResize").$();
        TestUtils.assertMemoryLeak(() -> {
            final int N = 1_000_000;
            try (DirectBitSet set = new DirectBitSet(N + 1)) {
                long cap = set.capacity();
                Assert.assertTrue(cap >= N + 1);
                for (int i = 0; i < N; i++) {
                    set.set(i);
                }
                Assert.assertEquals(cap, set.capacity());
            }
        });
    }

    @Test
    public void testReopenAfterClose() throws Exception {
        LOG.info().$("testReopenAfterClose").$();
        TestUtils.assertMemoryLeak(() -> {
            DirectBitSet set = new DirectBitSet();
            set.set(42);
            Assert.assertTrue(set.get(42));
            set.close();
            Assert.assertEquals(0, set.capacity());

            set.reopen();
            Assert.assertTrue(set.capacity() > 0);
            Assert.assertFalse(set.get(42));
            set.set(42);
            Assert.assertTrue(set.get(42));
            set.close();
        });
    }

    @Test
    public void testReservePreservesExistingBits() throws Exception {
        LOG.info().$("testReservePreservesExistingBits").$();
        TestUtils.assertMemoryLeak(() -> {
            try (DirectBitSet set = new DirectBitSet(128)) {
                set.set(10);
                set.set(100);
                Assert.assertTrue(set.capacity() < 10_000);

                set.reserve(10_000);
                Assert.assertTrue(set.capacity() >= 10_000);
                Assert.assertTrue(set.get(10));
                Assert.assertTrue(set.get(100));
                Assert.assertFalse(set.get(9_999));

                // Calling reserve with a smaller value is a no-op.
                long cap = set.capacity();
                set.reserve(64);
                Assert.assertEquals(cap, set.capacity());
            }
        });
    }

    @Test
    public void testResetCapacity() throws Exception {
        LOG.info().$("testResetCapacity").$();
        TestUtils.assertMemoryLeak(() -> {
            final int N = 1000;
            final int max = 1_000_000;
            final Rnd rnd = new Rnd();
            try (DirectBitSet set = new DirectBitSet()) {
                final long initialCapacity = set.capacity();
                Assert.assertTrue(initialCapacity > 0);

                for (int i = 0; i < N; i++) {
                    int idx = rnd.nextInt(max);
                    Assert.assertFalse(set.get(idx));
                    set.set(idx);
                }
                Assert.assertTrue(set.capacity() >= N);

                set.resetCapacity();
                Assert.assertEquals(initialCapacity, set.capacity());

                rnd.reset();
                for (int i = 0; i < N; i++) {
                    Assert.assertFalse(set.get(rnd.nextInt(max)));
                }
            }
        });
    }

    @Test
    public void testResetCapacityOnClosed() throws Exception {
        LOG.info().$("testResetCapacityOnClosed").$();
        TestUtils.assertMemoryLeak(() -> {
            DirectBitSet set = new DirectBitSet(128);
            set.set(100);
            set.close();
            set.resetCapacity();
            Assert.assertTrue(set.capacity() >= 128);
            Assert.assertFalse(set.get(100));
            set.set(50);
            Assert.assertTrue(set.get(50));
            set.close();
        });
    }

    @Test
    public void testResizeMemLeak() throws Exception {
        LOG.info().$("testResizeMemLeak").$();
        TestUtils.assertMemoryLeak(() -> {
            try (DirectBitSet set = new DirectBitSet()) {
                for (int i = 0; i < 1_000_000; i++) {
                    set.set(i);
                }
            }
        });
    }

    @Test
    public void testSmoke() throws Exception {
        LOG.info().$("testSmoke").$();
        TestUtils.assertMemoryLeak(() -> {
            final int N = 1000;
            final int max = 1_000_000;
            final Rnd rnd = new Rnd();
            try (DirectBitSet set = new DirectBitSet()) {
                Assert.assertTrue(set.capacity() > 0);

                for (int i = 0; i < N; i++) {
                    Assert.assertFalse(set.get(rnd.nextInt(max)));
                }
                Assert.assertTrue(set.capacity() >= N);

                rnd.reset();
                for (int i = 0; i < N; i++) {
                    set.set(rnd.nextInt(max));
                }

                rnd.reset();
                for (int i = 0; i < N; i++) {
                    Assert.assertTrue(set.get(rnd.nextInt(max)));
                }

                set.clear();

                rnd.reset();
                for (int i = 0; i < N; i++) {
                    Assert.assertFalse(set.get(rnd.nextInt(max)));
                }
            }
        });
    }

    @Test
    public void testUnset() throws Exception {
        LOG.info().$("testUnset").$();
        TestUtils.assertMemoryLeak(() -> {
            try (DirectBitSet set = new DirectBitSet()) {
                for (int i = 0; i < 256; i++) {
                    set.set(i);
                }
                for (int i = 0; i < 256; i += 2) {
                    set.unset(i);
                }
                for (int i = 0; i < 256; i++) {
                    Assert.assertEquals((i & 1) != 0, set.get(i));
                }
                // Unsetting out-of-range bit is a no-op and must not grow the set.
                long cap = set.capacity();
                set.unset(10_000_000);
                Assert.assertEquals(cap, set.capacity());
            }
        });
    }
}

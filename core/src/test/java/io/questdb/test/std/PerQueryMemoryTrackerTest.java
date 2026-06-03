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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.PerQueryMemoryTracker;
import io.questdb.std.PerQueryMemoryTrackerProvider;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class PerQueryMemoryTrackerTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static final SecurityContext SEC = AllowAllSecurityContext.INSTANCE;

    @Test
    public void testAcquireReadsLimitFromConfigEachAcquire() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The provider must read the configured limit on every acquire so a dynamic
            // config reload that changes the limit applies to subsequently acquired trackers.
            LimitsConfiguration config = new LimitsConfiguration(1024, 0, 0);
            try (PerQueryMemoryTrackerProvider provider = new PerQueryMemoryTrackerProvider(config)) {
                MemoryTracker t1 = provider.acquire(SEC, 1, MemoryTrackerWorkload.QUERY);
                Assert.assertEquals(1024, t1.getLimit());
                t1.close();

                // Simulate a reload bumping the limit.
                config.queryLimit = 4096;

                // The pool hands back the same skeleton, but the freshly read limit must win.
                MemoryTracker t2 = provider.acquire(SEC, 2, MemoryTrackerWorkload.QUERY);
                Assert.assertSame(t1, t2);
                Assert.assertEquals(4096, t2.getLimit());
                t2.close();

                // A reload back to unlimited must also take effect.
                config.queryLimit = 0;
                MemoryTracker t3 = provider.acquire(SEC, 3, MemoryTrackerWorkload.QUERY);
                Assert.assertEquals(0, t3.getLimit());
                t3.close();
            }
        });
    }

    @Test
    public void testAcquireResetsUsedAndAppliesLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(1024, 2048, 4096)) {
                MemoryTracker t = provider.acquire(SEC, 7, MemoryTrackerWorkload.QUERY);
                Assert.assertEquals(0, t.getUsed());
                Assert.assertEquals(1024, t.getLimit());
                Assert.assertEquals(7, t.getQueryId());
                Assert.assertEquals(MemoryTrackerWorkload.QUERY, t.getWorkload());

                // Charge something against the tracker, then close.
                long p = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT, t);
                Assert.assertEquals(256, t.getUsed());
                Unsafe.free(p, 256, MemoryTag.NATIVE_DEFAULT, t);
                Assert.assertEquals(0, t.getUsed());
                t.close();

                // Re-acquire for a different workload; counter and limit must be reset to the
                // MAT_VIEW_REFRESH values.
                MemoryTracker t2 = provider.acquire(SEC, 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH);
                Assert.assertEquals(0, t2.getUsed());
                Assert.assertEquals(2048, t2.getLimit());
                Assert.assertEquals(42, t2.getQueryId());
                Assert.assertEquals(MemoryTrackerWorkload.MAT_VIEW_REFRESH, t2.getWorkload());
                t2.close();
            }
        });
    }

    @Test
    public void testFreeNullTrackerDegradesToGlobal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT, null);
            Assert.assertNotEquals(0, ptr);
            long ret = Unsafe.free(ptr, 64, MemoryTag.NATIVE_DEFAULT, null);
            Assert.assertEquals(0, ret);
        });
    }

    @Test
    public void testGetNativeAllocatorIsStableAcrossCalls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(0, 0, 0)) {
                MemoryTracker t = provider.acquire(SEC, 1, MemoryTrackerWorkload.QUERY);
                try {
                    long a1 = Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT, t);
                    long a2 = Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertNotEquals(0, a1);
                    Assert.assertEquals(a1, a2);

                    // Different tag should yield a different allocator.
                    long b = Unsafe.getNativeAllocator(MemoryTag.NATIVE_O3, t);
                    Assert.assertNotEquals(0, b);
                    Assert.assertNotEquals(a1, b);

                    // Null tracker degrades to the global static allocator for that tag.
                    long g = Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT, null);
                    Assert.assertEquals(Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT), g);
                    Assert.assertNotEquals(a1, g);
                } finally {
                    t.close();
                }
            }
        });
    }

    @Test
    public void testMallocBeyondLimitThrowsPerQueryMessage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(1024, 0, 0)) {
                MemoryTracker t = provider.acquire(SEC, 11, MemoryTrackerWorkload.QUERY);
                try {
                    long ptr = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(512, t.getUsed());
                    try {
                        Unsafe.malloc(513, MemoryTag.NATIVE_DEFAULT, t);
                        Assert.fail("expected CairoException on per-query breach");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        String msg = e.getFlyweightMessage().toString();
                        TestUtils.assertContains(msg, "query memory limit exceeded");
                        TestUtils.assertContains(msg, "workload=QUERY");
                        TestUtils.assertContains(msg, "queryId=11");
                        TestUtils.assertContains(msg, "limit=1024");
                        TestUtils.assertContains(msg, "used=512");
                        TestUtils.assertContains(msg, "size=513");
                        TestUtils.assertContains(msg, "memoryTag=" + MemoryTag.NATIVE_DEFAULT);
                    }
                    // Counter must be unchanged on a rejected allocation.
                    Assert.assertEquals(512, t.getUsed());
                    Unsafe.free(ptr, 512, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(0, t.getUsed());
                } finally {
                    t.close();
                }
            }
        });
    }

    @Test
    public void testMallocExactlyAtLimitSucceeds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(1024, 0, 0)) {
                MemoryTracker t = provider.acquire(SEC, 1, MemoryTrackerWorkload.QUERY);
                try {
                    long ptr = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(1024, t.getUsed());
                    Unsafe.free(ptr, 1024, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(0, t.getUsed());
                } finally {
                    t.close();
                }
            }
        });
    }

    @Test
    public void testMallocNullTrackerDegradesToGlobal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // No tracker -- behaves exactly like the existing two-arg malloc.
            long ptr = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT, null);
            Assert.assertNotEquals(0, ptr);
            Unsafe.free(ptr, 64, MemoryTag.NATIVE_DEFAULT, null);
        });
    }

    @Test
    public void testMultiThreadedPoolContention() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(8192, 0, 0)) {
                final int threadCount = 16;
                final int iterationsPerThread = 200;
                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch done = new CountDownLatch(threadCount);
                final AtomicInteger failures = new AtomicInteger();
                Thread[] threads = new Thread[threadCount];
                for (int i = 0; i < threadCount; i++) {
                    threads[i] = new Thread(() -> {
                        try {
                            start.await();
                            for (int j = 0; j < iterationsPerThread; j++) {
                                MemoryTracker t = provider.acquire(SEC, j, MemoryTrackerWorkload.QUERY);
                                long p = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT, t);
                                Assert.assertEquals(128, t.getUsed());
                                Unsafe.free(p, 128, MemoryTag.NATIVE_DEFAULT, t);
                                t.close();
                            }
                        } catch (Throwable e) {
                            failures.incrementAndGet();
                            e.printStackTrace();
                        } finally {
                            done.countDown();
                        }
                    });
                    threads[i].start();
                }
                start.countDown();
                done.await();
                Assert.assertEquals(0, failures.get());
                // The pool should now hold at most as many trackers as the peak concurrency.
                Assert.assertTrue(provider.getPooledCount() <= threadCount);
            }
        });
    }

    @Test
    public void testPoolReuseKeepsSameNativeBlock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(1024, 0, 0)) {
                MemoryTracker t1 = provider.acquire(SEC, 1, MemoryTrackerWorkload.QUERY);
                long addr1 = t1.nativeAddress();
                t1.close();

                MemoryTracker t2 = provider.acquire(SEC, 2, MemoryTrackerWorkload.QUERY);
                // Pool returned the same skeleton, so the underlying native block is reused.
                Assert.assertSame(t1, t2);
                Assert.assertEquals(addr1, t2.nativeAddress());
                t2.close();
            }
        });
    }

    @Test
    public void testReallocBreachLeavesOldBlockIntact() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(1024, 0, 0)) {
                MemoryTracker t = provider.acquire(SEC, 3, MemoryTrackerWorkload.QUERY);
                try {
                    long ptr = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(256, t.getUsed());

                    // Write a sentinel into the existing block.
                    Unsafe.putLong(ptr, 0xDEADBEEFL);

                    // Try to grow well past the per-query limit.
                    try {
                        Unsafe.realloc(ptr, 256, 4096, MemoryTag.NATIVE_DEFAULT, t);
                        Assert.fail("expected breach on grow");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    }

                    // Strong exception safety: the old block must be intact and the counter unchanged.
                    Assert.assertEquals(256, t.getUsed());
                    Assert.assertEquals(0xDEADBEEFL, Unsafe.getLong(ptr));

                    Unsafe.free(ptr, 256, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(0, t.getUsed());
                } finally {
                    t.close();
                }
            }
        });
    }

    @Test
    public void testReallocGrowAndShrinkUpdatesBothCounters() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(4096, 0, 0)) {
                MemoryTracker t = provider.acquire(SEC, 1, MemoryTrackerWorkload.QUERY);
                try {
                    long globalBefore = Unsafe.getRssMemUsed();

                    long ptr = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(128, t.getUsed());
                    Assert.assertEquals(globalBefore + 128, Unsafe.getRssMemUsed());

                    ptr = Unsafe.realloc(ptr, 128, 512, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(512, t.getUsed());
                    Assert.assertEquals(globalBefore + 512, Unsafe.getRssMemUsed());

                    ptr = Unsafe.realloc(ptr, 512, 64, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(64, t.getUsed());
                    Assert.assertEquals(globalBefore + 64, Unsafe.getRssMemUsed());

                    Unsafe.free(ptr, 64, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(0, t.getUsed());
                    Assert.assertEquals(globalBefore, Unsafe.getRssMemUsed());
                } finally {
                    t.close();
                }
            }
        });
    }

    @Test
    public void testUnlimitedTrackerNeverBreaches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(0, 0, 0)) {
                MemoryTracker t = provider.acquire(SEC, 1, MemoryTrackerWorkload.QUERY);
                try {
                    Assert.assertEquals(0, t.getLimit());
                    long p = Unsafe.malloc(1024 * 1024, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(1024 * 1024, t.getUsed());
                    Unsafe.free(p, 1024 * 1024, MemoryTag.NATIVE_DEFAULT, t);
                    Assert.assertEquals(0, t.getUsed());
                } finally {
                    t.close();
                }
            }
        });
    }

    @Test
    public void testWorkloadSelectsRightLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(100, 200, 300)) {
                MemoryTracker q = provider.acquire(SEC, 1, MemoryTrackerWorkload.QUERY);
                Assert.assertEquals(100, q.getLimit());
                q.close();

                MemoryTracker mv = provider.acquire(SEC, 2, MemoryTrackerWorkload.MAT_VIEW_REFRESH);
                Assert.assertEquals(200, mv.getLimit());
                mv.close();

                MemoryTracker w = provider.acquire(SEC, 3, MemoryTrackerWorkload.WAL_APPLY);
                Assert.assertEquals(300, w.getLimit());
                w.close();
            }
        });
    }

    @Test
    public void testWrappedExceptionTypeIsCairoException() throws Exception {
        // Verifies that the per-query breach throws the exact same exception type
        // (CairoException with isOutOfMemory()) that the global RSS breach does, so
        // existing handlers continue to apply unchanged in both scopes.
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(16, 0, 0)) {
                MemoryTracker t = provider.acquire(SEC, 1, MemoryTrackerWorkload.QUERY);
                try {
                    try {
                        Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT, t);
                        Assert.fail();
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        Assert.assertEquals(CairoException.NON_CRITICAL, e.getErrno());
                    }
                } finally {
                    t.close();
                }
            }
        });
    }

    @Test
    public void testProviderCloseDrainsPool() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            PerQueryMemoryTrackerProvider provider = newProvider(1024, 0, 0);
            // Pre-populate the pool with a few trackers.
            MemoryTracker a = provider.acquire(SEC, 1, MemoryTrackerWorkload.QUERY);
            MemoryTracker b = provider.acquire(SEC, 2, MemoryTrackerWorkload.QUERY);
            MemoryTracker c = provider.acquire(SEC, 3, MemoryTrackerWorkload.QUERY);
            a.close();
            b.close();
            c.close();
            Assert.assertEquals(3, provider.getPooledCount());

            // close() must release every retained native block (verified by the
            // surrounding assertMemoryLeak).
            provider.close();
            Assert.assertEquals(0, provider.getPooledCount());
        });
    }

    private static PerQueryMemoryTrackerProvider newProvider(long queryLimit, long matViewRefreshLimit, long walApplyLimit) {
        return new PerQueryMemoryTrackerProvider(new LimitsConfiguration(queryLimit, matViewRefreshLimit, walApplyLimit));
    }

    /**
     * A {@link DefaultCairoConfiguration} whose three memory limits are mutable, so a test can
     * simulate a dynamic config reload by changing a limit between two acquisitions.
     */
    private static final class LimitsConfiguration extends DefaultCairoConfiguration {
        long matViewRefreshLimit;
        long queryLimit;
        long walApplyLimit;

        LimitsConfiguration(long queryLimit, long matViewRefreshLimit, long walApplyLimit) {
            super(temp.getRoot().getAbsolutePath());
            this.queryLimit = queryLimit;
            this.matViewRefreshLimit = matViewRefreshLimit;
            this.walApplyLimit = walApplyLimit;
        }

        @Override
        public long getMatViewRefreshMemoryLimitBytes() {
            return matViewRefreshLimit;
        }

        @Override
        public long getQueryMemoryLimitBytes() {
            return queryLimit;
        }

        @Override
        public long getWalApplyMemoryLimitBytes() {
            return walApplyLimit;
        }
    }
}

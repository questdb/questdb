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

import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.PerQueryMemoryTracker;
import io.questdb.std.PerQueryMemoryTrackerProvider;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PerQueryMemoryTrackerProviderTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    // The default OSS context uses the configured workload limits.
    private static final SecurityContext SECURITY_CONTEXT = AllowAllSecurityContext.INSTANCE;

    @Test
    public void testAcquireAssertsCleanRecycleOnDirtyRelease() throws Exception {
        // The init() recycle guard is a Java assertion, so it only fires under -ea. QuestDB CI runs with
        // assertions enabled; skip cleanly if they are off so the test never false-passes.
        Assume.assumeTrue("requires -ea", areAssertionsEnabled());

        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(0, 0, 0, 0)) {
                // Charge a tracker and release it without freeing, so it returns to the pool dirty. close()
                // must not assert: it runs from a finally at every production callsite, so throwing there
                // would mask the in-flight exception and strand the tracker (mirrors OSS
                // PerQueryMemoryTracker.close).
                PerQueryMemoryTracker t = (PerQueryMemoryTracker) provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);
                // pAllocated tracks the 256-byte block. A failing assert below must not leak it -- a leaked
                // native block trips the surrounding assertMemoryLeak and makes the failure harder to read.
                // (It does not replace the assertion message: TestUtils.assertMemoryLeak calls skipChecks()
                // when the body throws, so the real error still surfaces.)
                long p = 0;
                boolean pAllocated = false;
                boolean trackerLive = true;
                try {
                    // Inside the try, so a throwing malloc cannot strand the tracker acquired just above.
                    p = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT, t);
                    pAllocated = true;
                    Assert.assertEquals(256, t.getUsed());
                    t.close();
                    Assert.assertEquals(1, provider.getPooledCount());

                    // Re-acquiring the recycled block must trip the guard at init(), the quiescent boundary.
                    try {
                        provider.acquire(SECURITY_CONTEXT, 2, MemoryTrackerWorkload.QUERY);
                        Assert.fail("expected AssertionError on dirty recycle");
                    } catch (AssertionError e) {
                        TestUtils.assertContains(e.getMessage(), "tracker recycled with used=256");
                    }

                    // The tracker is neither pooled nor returned to the caller.
                    Assert.assertEquals(0, provider.getPooledCount());

                    // acquire() must NOT have destroyed it. A dirty recycle means an allocation is still
                    // charged to this tracker, and production frees that allocation through the tracker-aware
                    // overload -- which decrements the tracker's native {used} block and dispatches through
                    // the per-tag Rust allocator, both of which destroy() would have freed. Exercise exactly
                    // that path: it must read live memory, not freed memory.
                    Unsafe.free(p, 256, MemoryTag.NATIVE_DEFAULT, t);
                    pAllocated = false;

                    // The decrement landed in the tracker's own block, proving it is still mapped. Reading
                    // getUsed() here is the observation that a destroy() in acquire() would have turned into
                    // a use-after-free.
                    Assert.assertEquals(0, t.getUsed());

                    // The block is deliberately leaked by acquire(); the test owns it from here.
                    destroy(t);
                    trackerLive = false;
                } finally {
                    if (pAllocated) {
                        Unsafe.free(p, 256, MemoryTag.NATIVE_DEFAULT, t);
                    }
                    if (trackerLive) {
                        destroy(t);
                    }
                }
            }
        });
    }

    @Test
    public void testAcquireReadsWorkloadLimitFromConfigEachAcquire() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The provider must read the configured limit on every acquire so a dynamic config reload that
            // changes the limit applies to subsequently acquired trackers (mirrors OSS
            // PerQueryMemoryTrackerTest#testAcquireReadsLimitFromConfigEachAcquire).
            LimitsConfiguration config = new LimitsConfiguration(1024, 0, 0, 0);
            try (PerQueryMemoryTrackerProvider provider = new PerQueryMemoryTrackerProvider(config)) {
                MemoryTracker t1 = provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);
                try {
                    Assert.assertEquals(1024, t1.getLimit());
                } finally {
                    t1.close();
                }

                // Simulate a reload bumping the limit.
                config.queryLimit = 4096;

                // The pool hands back the same skeleton, but the freshly read limit must win.
                MemoryTracker t2 = provider.acquire(SECURITY_CONTEXT, 2, MemoryTrackerWorkload.QUERY);
                try {
                    Assert.assertSame(t1, t2);
                    Assert.assertEquals(4096, t2.getLimit());
                } finally {
                    t2.close();
                }

                // A reload back to unlimited must also take effect.
                config.queryLimit = 0;
                MemoryTracker t3 = provider.acquire(SECURITY_CONTEXT, 3, MemoryTrackerWorkload.QUERY);
                try {
                    Assert.assertEquals(0, t3.getLimit());
                } finally {
                    t3.close();
                }
            }
        });
    }

    @Test
    public void testAcquireReconcilesCoveredIndexChargeOnRecycle() throws Exception {
        // The sole failure signal on a reverted reconcileCovered() is init()'s `assert getUsed()==0`, which
        // only fires under -ea. Under -da, init() unconditionally resets used to 0 and the covered buffer is
        // freed globally, so the test would false-pass with the fix reverted. Skip cleanly when assertions
        // are off; QuestDB CI runs with -ea, where the fix is genuinely guarded (mirrors the -ea guard on
        // testAcquireAssertsCleanRecycleOnDirtyRelease).
        Assume.assumeTrue("requires -ea", areAssertionsEnabled());

        TestUtils.assertMemoryLeak(() -> {
            // The covered-index decode path (PageFrameMemoryPool) charges the per-query tracker's native
            // `used` at allocation but frees the buffers on global-only accounting on a LATER query, so no
            // free path ever decrements `used` for covered bytes. reconcileCovered() at tracker teardown is
            // the sole mechanism that removes that charge, leaving the pooled block clean. Without the
            // close()/init() reconcileCovered() calls, the tracker returns to the pool with used > 0 and the
            // next acquire trips init()'s recycle guard. Reproduce the covered scenario directly, without a
            // real covering index (mirrors OSS PerQueryMemoryTracker's covered-recycle contract).
            try (PerQueryMemoryTrackerProvider provider = newProvider(0, 0, 0, 0)) {
                MemoryTracker t = provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);
                // trackerLive hands t back to the pool if anything below throws before its own close(); the
                // provider frees only what it holds, so an escaping tracker would strand its native block.
                boolean trackerLive = true;
                try {
                    // Charge the tracker like a covered-index decode allocation does.
                    final long p = Unsafe.malloc(256, MemoryTag.NATIVE_INDEX_READER, t);
                    try {
                        t.addCoveredBytes(256);
                        Assert.assertEquals(256, t.getUsed());
                    } finally {
                        // Global-only free (no tracker), exactly as the covered buffers are freed by a later
                        // query: `used` is intentionally left at 256.
                        Unsafe.free(p, 256, MemoryTag.NATIVE_INDEX_READER);
                    }
                    Assert.assertEquals(256, t.getUsed());

                    // close() must reconcile the outstanding covered charge so the block recycles clean.
                    t.close();
                    trackerLive = false;

                    // Re-acquiring the pooled instance must not trip init()'s used==0 guard, and it must start
                    // clean. Without reconcileCovered() this acquire throws AssertionError (under -ea) / hands
                    // back a tracker whose native accounting is desynced by 256.
                    MemoryTracker t2 = provider.acquire(SECURITY_CONTEXT, 2, MemoryTrackerWorkload.QUERY);
                    try {
                        Assert.assertSame(t, t2);
                        Assert.assertEquals(0, t2.getUsed());
                    } finally {
                        t2.close();
                    }
                } finally {
                    if (trackerLive) {
                        t.close();
                    }
                }
            }
        });
    }

    @Test
    public void testAcquireResetsUsedAndAppliesLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(1024, 2048, 4096, 8192)) {
                MemoryTracker t = provider.acquire(SECURITY_CONTEXT, 7, MemoryTrackerWorkload.QUERY);
                try {
                    Assert.assertEquals(0, t.getUsed());
                    Assert.assertEquals(1024, t.getLimit());
                    Assert.assertEquals(7, t.getQueryId());
                    Assert.assertEquals(MemoryTrackerWorkload.QUERY, t.getWorkload());

                    // Charge something against the tracker, then free it so it returns to the pool clean.
                    long p = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT, t);
                    try {
                        Assert.assertEquals(256, t.getUsed());
                    } finally {
                        Unsafe.free(p, 256, MemoryTag.NATIVE_DEFAULT, t);
                    }
                    Assert.assertEquals(0, t.getUsed());
                } finally {
                    t.close();
                }

                // Re-acquire the pooled skeleton for a different workload; init() must reset the counter to
                // 0 on the reused native block and re-apply the MAT_VIEW_REFRESH limit/queryId/workload.
                MemoryTracker t2 = provider.acquire(SECURITY_CONTEXT, 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH);
                try {
                    Assert.assertSame(t, t2);
                    Assert.assertEquals(0, t2.getUsed());
                    Assert.assertEquals(2048, t2.getLimit());
                    Assert.assertEquals(42, t2.getQueryId());
                    Assert.assertEquals(MemoryTrackerWorkload.MAT_VIEW_REFRESH, t2.getWorkload());
                } finally {
                    t2.close();
                }
            }
        });
    }

    @Test
    public void testClearDestroysPooledTrackersAndKeepsProviderUsable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // clear() destroys every pooled tracker but, unlike close(), must leave the provider serving:
            // it frees the native blocks (a miss here surfaces as an assertMemoryLeak failure) and empties
            // the pool, and a subsequent acquire() must build a fresh tracker rather than hand back a
            // destroyed one whose native block is already freed.
            try (PerQueryMemoryTrackerProvider provider = newProvider(1024, 0, 0, 0)) {
                final MemoryTracker pooled = provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);
                try {
                    // Bind a per-tag Rust allocator as well, so clear() runs the full destroy() path.
                    Assert.assertNotEquals(0, Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT, pooled));
                } finally {
                    pooled.close();
                }
                Assert.assertEquals(1, provider.getPooledCount());

                provider.clear();
                Assert.assertEquals(0, provider.getPooledCount());

                final MemoryTracker fresh = provider.acquire(SECURITY_CONTEXT, 2, MemoryTrackerWorkload.QUERY);
                try {
                    // The pooled tracker was destroyed, so this must be a new instance on a live native block.
                    Assert.assertNotSame(pooled, fresh);
                    Assert.assertEquals(0, fresh.getUsed());
                    Assert.assertEquals(1024, fresh.getLimit());
                    final long p = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT, fresh);
                    try {
                        Assert.assertEquals(256, fresh.getUsed());
                    } finally {
                        Unsafe.free(p, 256, MemoryTag.NATIVE_DEFAULT, fresh);
                    }
                } finally {
                    fresh.close();
                }
                Assert.assertEquals(1, provider.getPooledCount());

                // clear() stays usable on a live provider, and close() must still drain what came after it.
                provider.clear();
                Assert.assertEquals(0, provider.getPooledCount());
            }
        });
    }

    @Test
    public void testCloseDrainsPool() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(1024, 0, 0, 0)) {
                MemoryTracker t = provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);
                t.close();
                Assert.assertEquals(1, provider.getPooledCount());
                // exiting try-with-resources closes the provider and drains the pool; assertMemoryLeak
                // verifies the drained native blocks were freed.
            }
        });
    }

    @Test
    public void testCloseThenReleaseDoesNotLeak() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final PerQueryMemoryTrackerProvider provider = newProvider(1024, 0, 0, 0);
            final MemoryTracker tracker = provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);
            provider.close();
            tracker.close();
            Assert.assertEquals(0, provider.getPooledCount());
        });
    }

    @Test
    public void testConcurrentAcquireReleaseKeepsPoolConsistent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The provider is built on a ConcurrentPool with volatile native reads and is designed for
            // concurrent use (one tracker per query thread). Hammer one provider from many threads, each
            // looping acquire/use/close, to stress the pool the way per-test single-threaded coverage never
            // does. A pool double-pop or a torn read of the native {used,limit} block would otherwise pass
            // every other test in this class.
            // Each thread carries its OWN principal limit, so the block it reads back identifies the thread
            // that initialized it. With one shared limit every thread expects the same number, and a tracker
            // handed to two threads at once reports the value both of them wanted anyway -- the read cannot
            // fail, whatever the pool does.
            final int threadCount = 8;
            final int iterations = 10_000;
            final PerQueryMemoryTrackerProvider provider = new PerQueryMemoryTrackerProvider(new LimitsConfiguration(1024, 0, 0, 0)) {
                @Override
                protected long limitFor(SecurityContext securityContext, MemoryTrackerWorkload workload) {
                    return ((LimitSecurityContext) securityContext).limit;
                }
            };
            boolean workersTerminated = false;
            try {
                final AtomicInteger tornLimits = new AtomicInteger();
                final AtomicReference<Throwable> error = new AtomicReference<>();
                final AtomicBoolean running = new AtomicBoolean(true);
                final Set<MemoryTracker> trackers = Collections.synchronizedSet(
                        Collections.newSetFromMap(new IdentityHashMap<>()));
                final Thread[] threads = new Thread[threadCount];
                final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                final SOCountDownLatch halt = new SOCountDownLatch(threadCount);
                for (int t = 0; t < threadCount; t++) {
                    // Distinct per thread, and none equal to the 1024 configured workload limit, so a tracker
                    // that never got this thread's init() is caught whichever way it went wrong.
                    final long threadLimit = 2048L + t;
                    final SecurityContext ctx = new LimitSecurityContext(threadLimit);
                    threads[t] = new Thread(() -> {
                        // Everything the worker does sits inside the try, so halt.countDown() runs on every
                        // path and the test thread can never be left waiting on a worker that died early.
                        try {
                            TestUtils.await(barrier);
                            for (int i = 0; i < iterations && running.get(); i++) {
                                final MemoryTracker tracker = provider.acquire(ctx, i, MemoryTrackerWorkload.QUERY);
                                trackers.add(tracker);
                                try {
                                    // This thread owns the tracker between acquire() and close(), so the block
                                    // must read back the limit its own acquire() wrote. Anything else means a
                                    // second thread was handed the same tracker, or the read tore.
                                    if (tracker.getLimit() != threadLimit) {
                                        tornLimits.incrementAndGet();
                                    }
                                } finally {
                                    tracker.close();
                                }
                            }
                        } catch (Throwable e) {
                            // Retain the first failure for the test thread to rethrow; printing it would leave the
                            // suite reporting nothing but an opaque counter mismatch.
                            error.compareAndSet(null, e);
                        } finally {
                            Path.clearThreadLocals();
                            halt.countDown();
                        }
                    });
                    threads[t].start();
                }
                // Bounded wait: a worker wedged in the pool (a lost push, a double pop) must fail this test
                // rather than hang the suite on an unbounded await.
                final boolean completedInTime = halt.await(TimeUnit.MINUTES.toNanos(2));
                if (!completedInTime) {
                    running.set(false);
                    for (Thread thread : threads) {
                        thread.interrupt();
                    }
                    for (Thread thread : threads) {
                        thread.join(TimeUnit.SECONDS.toMillis(10));
                    }
                } else {
                    for (Thread thread : threads) {
                        thread.join();
                    }
                }
                for (Thread thread : threads) {
                    Assert.assertFalse("worker did not terminate", thread.isAlive());
                }
                workersTerminated = true;
                Assert.assertTrue("workers did not finish within 2 minutes", completedInTime);

                final Throwable failure = error.get();
                if (failure != null) {
                    throw new AssertionError("worker failed under concurrent acquire/release", failure);
                }
                Assert.assertEquals("trackers observed carrying another thread's limit", 0, tornLimits.get());
                // After every thread has released all its trackers, the pool holds exactly the distinct
                // tracker instances ever created. Comparing the identity-set size with the pool count catches
                // lost pushes; a duplicate push would make the pool count exceed the distinct identity count.
                // The surrounding assertMemoryLeak independently catches a leaked or double-freed native block.
                final int pooled = provider.getPooledCount();
                Assert.assertEquals("every created tracker must be returned exactly once", trackers.size(), pooled);
            } finally {
                // Never destroy native tracker state while a worker can still be using it.
                if (workersTerminated) {
                    provider.close();
                }
            }
        });
    }

    @Test
    public void testDestroyFreesPerTagNativeAllocators() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // destroy() owns every per-tag Rust QdbAllocator the tracker handed out. Those blocks come from a
            // raw UNSAFE.allocateMemory and are recorded in no Unsafe counter, so assertMemoryLeak is blind to
            // them: the tracker's nativeAllocators cache is the only observable that destroy() released them.
            // Read it reflectively - without this assertion, dropping freeNativeAllocators() from destroy()
            // leaks one QdbAllocator per bound tag per tracker with every other test still green.
            final PerQueryMemoryTrackerProvider provider = newProvider(0, 0, 0, 0);
            final PerQueryMemoryTracker tracker = (PerQueryMemoryTracker) provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);
            boolean destroyed = false;
            try {
                init(tracker, 1);

                final long defaultAllocator = Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT, tracker);
                final long o3Allocator = Unsafe.getNativeAllocator(MemoryTag.NATIVE_O3, tracker);
                Assert.assertNotEquals(0, defaultAllocator);
                Assert.assertNotEquals(0, o3Allocator);
                Assert.assertNotEquals(defaultAllocator, o3Allocator);
                // The cache is keyed by tag: a repeat call for a bound tag reuses the block.
                Assert.assertEquals(defaultAllocator, Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT, tracker));
                Assert.assertEquals(2, boundNativeAllocators(tracker));

                destroy(tracker);
                destroyed = true;

                Assert.assertEquals(
                        "destroy() must free and unbind every per-tag native allocator",
                        0,
                        boundNativeAllocators(tracker)
                );
            } finally {
                // The tracker never entered the pool, so provider.close() drains nothing and cannot free it: a
                // failing assert before destroy() would otherwise leak its native block and mask the real message
                // under assertMemoryLeak. destroy() is a plain free with no used==0 guard, so a manual call here
                // is safe; the flag keeps it to exactly one destroy on the happy path.
                if (!destroyed) {
                    destroy(tracker);
                }
                provider.close();
            }
        });
    }

    @Test
    public void testInitReconcilesCoveredChargeWithoutClose() throws Exception {
        Assume.assumeTrue("requires -ea", areAssertionsEnabled());

        TestUtils.assertMemoryLeak(() -> {
            final PerQueryMemoryTrackerProvider provider = newProvider(0, 0, 0, 0);
            final PerQueryMemoryTracker tracker = (PerQueryMemoryTracker) provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);
            try {
                init(tracker, 1);
                final long p = Unsafe.malloc(256, MemoryTag.NATIVE_INDEX_READER, tracker);
                tracker.addCoveredBytes(256);
                Unsafe.free(p, 256, MemoryTag.NATIVE_INDEX_READER);
                Assert.assertEquals(256, tracker.getUsed());

                init(tracker, 2);
                Assert.assertEquals(0, tracker.getUsed());
            } finally {
                destroy(tracker);
                provider.close();
            }
        });
    }

    @Test
    public void testPoolingReusesTracker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(1024, 0, 0, 0)) {
                Assert.assertEquals(0, provider.getPooledCount());
                MemoryTracker t = provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);
                t.close();
                Assert.assertEquals(1, provider.getPooledCount());
                MemoryTracker t2 = provider.acquire(SECURITY_CONTEXT, 2, MemoryTrackerWorkload.QUERY);
                try {
                    Assert.assertSame(t, t2);
                    Assert.assertEquals(0, provider.getPooledCount());
                } finally {
                    t2.close();
                }
            }
        });
    }

    @Test
    public void testPoolReturnKeepsNativeAllocatorsBound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The counterpart to destroy(): a pool return must NOT free the tracker's per-tag allocators.
            // Rust holds the QdbAllocator pointer for the tracker's whole life, and re-binding on every
            // acquire would reintroduce exactly the per-workload native alloc/free the pool exists to avoid.
            try (PerQueryMemoryTrackerProvider provider = newProvider(0, 0, 0, 0)) {
                final MemoryTracker t1 = provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);
                final long allocator;
                try {
                    allocator = Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT, t1);
                    Assert.assertNotEquals(0, allocator);
                } finally {
                    t1.close();
                }

                final MemoryTracker t2 = provider.acquire(SECURITY_CONTEXT, 2, MemoryTrackerWorkload.QUERY);
                try {
                    Assert.assertSame(t1, t2);
                    Assert.assertEquals(allocator, Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT, t2));
                } finally {
                    t2.close();
                }
                // Closing the provider drains the recycled tracker and frees that allocator exactly once.
            }
        });
    }

    @Test
    public void testQueryIdWorkloadAndUsedAreInitialized() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PerQueryMemoryTrackerProvider provider = newProvider(1024, 2048, 4096, 8192)) {
                MemoryTracker t = provider.acquire(SECURITY_CONTEXT, 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH);
                try {
                    Assert.assertEquals(42, t.getQueryId());
                    Assert.assertEquals(MemoryTrackerWorkload.MAT_VIEW_REFRESH, t.getWorkload());
                    Assert.assertEquals(2048, t.getLimit());
                    Assert.assertEquals(0, t.getUsed());
                } finally {
                    t.close();
                }
            }
        });
    }

    @Test
    public void testReleaseThatRacesCloseDoesNotLeak() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final PerQueryMemoryTrackerProvider provider = newProvider(1024, 0, 0, 0);
            final PerQueryMemoryTracker tracker = (PerQueryMemoryTracker) provider.acquire(SECURITY_CONTEXT, 1, MemoryTrackerWorkload.QUERY);

            // Deterministically resume release() after its initial closed==false read and after close() has
            // completed its drain. The releasing thread must observe closure after pushing and drain the tracker.
            provider.close();
            final Method release = PerQueryMemoryTrackerProvider.class.getDeclaredMethod(
                    "releaseAfterClosedCheck", PerQueryMemoryTracker.class, boolean.class
            );
            release.setAccessible(true);
            release.invoke(provider, tracker, false);

            Assert.assertEquals(0, provider.getPooledCount());
        });
    }

    // Acquires a tracker for the given context/workload and asserts its effective limit, always releasing the
    // tracker's native block in finally so a failed assertion cannot leak it and mask the real message under
    // assertMemoryLeak.
    private static void assertAcquiredLimit(
            PerQueryMemoryTrackerProvider provider,
            SecurityContext securityContext,
            long queryId,
            MemoryTrackerWorkload workload,
            long expectedLimit
    ) {
        final MemoryTracker tracker = provider.acquire(securityContext, queryId, workload);
        try {
            Assert.assertEquals(expectedLimit, tracker.getLimit());
        } finally {
            tracker.close();
        }
    }

    // True when the JVM runs with -ea. Several tests turn on an assertion being the failure signal, so they
    // must skip rather than false-pass under -da.
    private static boolean areAssertionsEnabled() {
        boolean enabled = false;
        //noinspection AssertWithSideEffects,ConstantConditions
        assert enabled = true;
        return enabled;
    }

    // Counts the per-tag Rust allocator blocks currently bound to the tracker. MemoryTracker.nativeAllocators
    // is private to the OSS base class with no accessor, and the blocks bypass Unsafe's memory counters, so
    // reflection is the only way to assert destroy() released them.
    private static int boundNativeAllocators(MemoryTracker tracker) throws Exception {
        final Field field = MemoryTracker.class.getDeclaredField("nativeAllocators");
        field.setAccessible(true);
        int count = 0;
        for (long address : (long[]) field.get(tracker)) {
            if (address != 0) {
                count++;
            }
        }
        return count;
    }

    private static void destroy(MemoryTracker tracker) throws Exception {
        final Method method = PerQueryMemoryTracker.class.getDeclaredMethod("destroy");
        method.setAccessible(true);
        method.invoke(tracker);
    }

    private static void init(MemoryTracker tracker, long queryId) throws Exception {
        final Method method = PerQueryMemoryTracker.class.getDeclaredMethod("init", long.class, MemoryTrackerWorkload.class, long.class);
        method.setAccessible(true);
        method.invoke(tracker, queryId, MemoryTrackerWorkload.QUERY, 0);
    }

    private static PerQueryMemoryTrackerProvider newProvider(
            long queryLimit,
            long matViewRefreshLimit,
            long walApplyLimit,
            long liveViewRefreshLimit
    ) {
        return new PerQueryMemoryTrackerProvider(
                new LimitsConfiguration(queryLimit, matViewRefreshLimit, walApplyLimit, liveViewRefreshLimit)
        );
    }

    private static final class LimitSecurityContext extends AllowAllSecurityContext {
        private final long limit;

        private LimitSecurityContext(long limit) {
            this.limit = limit;
        }
    }

    /**
     * A {@link DefaultCairoConfiguration} whose workload memory limits are mutable, so a test can simulate a
     * dynamic config reload by changing a limit between two acquisitions.
     */
    private static final class LimitsConfiguration extends DefaultCairoConfiguration {
        long liveViewRefreshLimit;
        long matViewRefreshLimit;
        long queryLimit;
        long walApplyLimit;

        LimitsConfiguration(long queryLimit, long matViewRefreshLimit, long walApplyLimit, long liveViewRefreshLimit) {
            super(temp.getRoot().getAbsolutePath());
            this.queryLimit = queryLimit;
            this.matViewRefreshLimit = matViewRefreshLimit;
            this.walApplyLimit = walApplyLimit;
            this.liveViewRefreshLimit = liveViewRefreshLimit;
        }

        @Override
        public long getLiveViewRefreshMemoryLimitBytes() {
            return liveViewRefreshLimit;
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

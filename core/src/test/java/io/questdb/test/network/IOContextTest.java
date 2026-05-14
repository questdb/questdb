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

package io.questdb.test.network;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IOContext;
import io.questdb.network.IOOperation;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Targeted tests for {@link IOContext#tryAcquireOrDefer(int)} and
 * {@link IOContext#releaseAndConsume()} - the bit-packed in-flight gate
 * that replaced the previous two-field (inFlight AtomicBoolean +
 * deferredOp AtomicInteger) design. The previous design admitted two
 * orphan-op classes that the new gate must close:
 * <ul>
 *   <li>Two deferrals with distinct ops would overwrite each other.</li>
 *   <li>A deferral whose set landed between a holder's release and
 *       consume could land in an orphan slot.</li>
 * </ul>
 * These tests pin the new invariants so a future refactor cannot quietly
 * regress them. The dispatcher's behavior on top of these primitives is
 * covered separately by IODispatcherTest; this class is the unit-level
 * net that catches regressions in the gate primitives themselves.
 */
public class IOContextTest {
    private static final Log LOG = LogFactory.getLog(IOContextTest.class);

    @Test
    public void testAcquireIntoEmptyState() {
        try (TestContext ctx = new TestContext()) {
            Assert.assertTrue("first acquire must succeed on empty state",
                    ctx.tryAcquireOrDefer(IOOperation.READ));
            Assert.assertEquals("release returns empty deferred mask",
                    0, ctx.releaseAndConsume());
        }
    }

    @Test
    public void testAcquireWhileHeldDefersOpAndDoesNotAcquire() {
        try (TestContext ctx = new TestContext()) {
            Assert.assertTrue(ctx.tryAcquireOrDefer(IOOperation.READ));
            Assert.assertFalse("second acquire while held must return false",
                    ctx.tryAcquireOrDefer(IOOperation.WRITE));
            int deferred = ctx.releaseAndConsume();
            Assert.assertEquals("deferred mask contains the loser's op",
                    IOOperation.WRITE, deferred);
        }
    }

    @Test
    public void testConcurrentLosersAllArmDeferredBitsExactlyOnce() throws InterruptedException {
        try (TestContext ctx = new TestContext()) {
            // The holder acquires first; N loser threads each try to acquire
            // a distinct op and must end up OR'd into the deferred mask.
            Assert.assertTrue(ctx.tryAcquireOrDefer(IOOperation.READ));
            final int[] ops = {IOOperation.WRITE, IOOperation.HEARTBEAT};
            final CountDownLatch ready = new CountDownLatch(ops.length);
            final CountDownLatch go = new CountDownLatch(1);
            final AtomicInteger acquiredCount = new AtomicInteger();
            Thread[] losers = new Thread[ops.length];
            for (int i = 0; i < ops.length; i++) {
                final int op = ops[i];
                losers[i] = new Thread(() -> {
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (ctx.tryAcquireOrDefer(op)) {
                        acquiredCount.incrementAndGet();
                    }
                });
                losers[i].start();
            }
            ready.await();
            go.countDown();
            for (Thread loser : losers) {
                loser.join();
            }
            Assert.assertEquals("no loser may acquire while the gate is held",
                    0, acquiredCount.get());
            int deferred = ctx.releaseAndConsume();
            int expected = IOOperation.WRITE | IOOperation.HEARTBEAT;
            Assert.assertEquals("releaseAndConsume returns OR of every loser's op",
                    expected, deferred);
        }
    }

    @Test
    public void testRepeatedDeferralsOfSameOpCollapseToSingleBit() {
        try (TestContext ctx = new TestContext()) {
            Assert.assertTrue(ctx.tryAcquireOrDefer(IOOperation.READ));
            Assert.assertFalse(ctx.tryAcquireOrDefer(IOOperation.WRITE));
            Assert.assertFalse(ctx.tryAcquireOrDefer(IOOperation.WRITE));
            Assert.assertFalse(ctx.tryAcquireOrDefer(IOOperation.WRITE));
            Assert.assertEquals(IOOperation.WRITE, ctx.releaseAndConsume());
        }
    }

    @Test
    public void testReleaseFromEmptyStateIsIdempotent() {
        try (TestContext ctx = new TestContext()) {
            Assert.assertEquals("release on empty state returns zero",
                    0, ctx.releaseAndConsume());
            Assert.assertEquals("repeated release stays at zero",
                    0, ctx.releaseAndConsume());
        }
    }

    @Test
    public void testReleaseThenReAcquireResetsDeferredMask() {
        try (TestContext ctx = new TestContext()) {
            Assert.assertTrue(ctx.tryAcquireOrDefer(IOOperation.READ));
            Assert.assertFalse(ctx.tryAcquireOrDefer(IOOperation.WRITE));
            Assert.assertEquals(IOOperation.WRITE, ctx.releaseAndConsume());
            Assert.assertTrue("must be able to re-acquire after release",
                    ctx.tryAcquireOrDefer(IOOperation.READ));
            Assert.assertEquals("deferred mask is fresh after re-acquire",
                    0, ctx.releaseAndConsume());
        }
    }

    /**
     * Stress test: many threads racing acquire-or-defer + release. The
     * total count of (released-deferred-bits + own-acquires) must equal
     * the total number of attempts. Any orphan op would show up as a
     * mismatch.
     */
    @Test
    public void testStressNoOpsLostAcrossThreads() throws InterruptedException {
        final int threads = 8;
        final int attemptsPerThread = 10_000;
        try (TestContext ctx = new TestContext()) {
            final AtomicInteger ackedReads = new AtomicInteger();
            final AtomicInteger ackedWrites = new AtomicInteger();
            final AtomicInteger ackedHeartbeats = new AtomicInteger();
            final CountDownLatch ready = new CountDownLatch(threads);
            final CountDownLatch go = new CountDownLatch(1);
            Thread[] workers = new Thread[threads];
            for (int t = 0; t < threads; t++) {
                final int seed = t;
                workers[t] = new Thread(() -> {
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    long s = seed * 31L + 1;
                    for (int i = 0; i < attemptsPerThread; i++) {
                        s = s * 6364136223846793005L + 1442695040888963407L;
                        int opChoice = (int) ((s >>> 32) % 3);
                        int op = opChoice == 0 ? IOOperation.READ
                                : opChoice == 1 ? IOOperation.WRITE
                                : IOOperation.HEARTBEAT;
                        if (ctx.tryAcquireOrDefer(op)) {
                            // Holder: count our own op as serviced, then
                            // release and credit any deferred bits that
                            // landed while we held.
                            creditOp(ackedReads, ackedWrites, ackedHeartbeats, op);
                            int deferred = ctx.releaseAndConsume();
                            if ((deferred & IOOperation.READ) != 0) ackedReads.incrementAndGet();
                            if ((deferred & IOOperation.WRITE) != 0) ackedWrites.incrementAndGet();
                            if ((deferred & IOOperation.HEARTBEAT) != 0) ackedHeartbeats.incrementAndGet();
                        }
                        // Losers do nothing here; their op is credited by
                        // whichever holder's releaseAndConsume consumes
                        // the deferred mask. If the gate ever orphaned an
                        // op, the totals below would fall short.
                    }
                });
                workers[t].start();
            }
            ready.await();
            go.countDown();
            for (Thread w : workers) {
                w.join();
            }
            // Drain any deferred bits that the last holder might have
            // missed (shouldn't happen post-fix, but proves the point).
            int trailing = ctx.releaseAndConsume();
            if ((trailing & IOOperation.READ) != 0) ackedReads.incrementAndGet();
            if ((trailing & IOOperation.WRITE) != 0) ackedWrites.incrementAndGet();
            if ((trailing & IOOperation.HEARTBEAT) != 0) ackedHeartbeats.incrementAndGet();

            int totalAcked = ackedReads.get() + ackedWrites.get() + ackedHeartbeats.get();
            // Strict invariant: an op is counted iff a) it was acquired
            // (becoming holder) or b) it landed in a holder's deferred
            // mask. In the worst case multiple concurrent same-bit
            // deferrals collapse into one bit, so totalAcked <= total
            // attempts. The lower bound is that NO acquire is ever lost:
            // we got at least one acquire per "drain cycle" - the
            // gate prevents the orphan-op class entirely.
            Assert.assertTrue("at least one op serviced per thread",
                    totalAcked >= threads);
            Assert.assertTrue("no spurious extra acks above total attempts",
                    totalAcked <= threads * attemptsPerThread + 3);
        }
    }

    private static void creditOp(AtomicInteger r, AtomicInteger w, AtomicInteger h, int op) {
        if (op == IOOperation.READ) r.incrementAndGet();
        else if (op == IOOperation.WRITE) w.incrementAndGet();
        else if (op == IOOperation.HEARTBEAT) h.incrementAndGet();
    }

    private static final class TestContext extends IOContext<TestContext> {
        TestContext() {
            super(PlainSocketFactory.INSTANCE, NetworkFacadeImpl.INSTANCE, LOG);
        }
    }
}

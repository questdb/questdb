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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Positive control for the slot-leak oracle.
 * <p>
 * Every other user of {@link PerWorkerLocks#getAcquiredSlotCount()} asserts that it is zero, so a
 * {@code getAcquiredSlotCount()} that always answered zero would disable the single detector the
 * reducers' slot-leak coverage rests on, and the whole suite would stay green. These tests are the
 * only ones that require a non-zero answer, and they pin the count to the same {@code INTS_PER_SLOT}
 * stride the acquire walks: a count that read consecutive ints instead of one per cache-line-padded
 * slot would report 1 for the two slots held below.
 * <p>
 * Narrow unit test: PerWorkerLocks allocates no native memory, so it needs no assertMemoryLeak.
 */
public class PerWorkerLocksTest extends AbstractCairoTest {

    @Test
    public void testAcquireCountOnlyGrows() {
        // The companion oracle: unlike the held-slot count, this tally survives the release, which is
        // what lets a leak test tell "took a slot and gave it back" apart from "never took one". The
        // two answers come from the same int - parity for held, value for the tally - so a release
        // that reset the slot to 0 instead of counting it up would read back 0 here.
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 4);
        locks.setTestAcquireLatch(new CountDownLatch(0));
        Assert.assertEquals(0, locks.getSlotAcquireCount());

        final int first = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        Assert.assertEquals(1, locks.getSlotAcquireCount());
        final int second = locks.acquireSlot(1, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        Assert.assertEquals(2, locks.getSlotAcquireCount());

        locks.releaseSlot(first);
        locks.releaseSlot(second);
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
        Assert.assertEquals(2, locks.getSlotAcquireCount());

        // Re-acquiring the same slot keeps counting.
        locks.releaseSlot(locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
        Assert.assertEquals(3, locks.getSlotAcquireCount());
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testAcquireCountStaysDisabledWithoutTestHook() {
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 2);

        final int slot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        locks.releaseSlot(slot);

        Assert.assertEquals(0, locks.getSlotAcquireCount());
    }

    @Test
    public void testAcquireReleaseRoundTrip() {
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 4);
        Assert.assertEquals(0, locks.getAcquiredSlotCount());

        final int first = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        Assert.assertEquals(1, locks.getAcquiredSlotCount());
        final int second = locks.acquireSlot(1, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        Assert.assertNotEquals(first, second);
        Assert.assertEquals(2, locks.getAcquiredSlotCount());

        locks.releaseSlot(first);
        Assert.assertEquals(1, locks.getAcquiredSlotCount());
        locks.releaseSlot(second);
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testAcquiresEverySlot() {
        final int workerCount = 4;
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, workerCount);
        for (int i = 0; i < workerCount; i++) {
            locks.acquireSlot(i, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            Assert.assertEquals(i + 1, locks.getAcquiredSlotCount());
        }
        // The pool is exhausted: a reducer that leaked all of these would spin here forever, which
        // is what makes a leaked slot fatal rather than merely wasteful.
        for (int i = 0; i < workerCount; i++) {
            locks.releaseSlot(i);
        }
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testCarrierIdsOutsideSlotRangeAreNormalized() {
        final int workerCount = 4;
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, workerCount);
        final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);

        final int boundarySlot = locks.acquireSlot(workerCount, circuitBreaker);
        Assert.assertEquals(0, boundarySlot);
        final int nextSlot = locks.acquireSlot(workerCount, circuitBreaker);
        Assert.assertEquals(1, nextSlot);
        locks.releaseSlot(boundarySlot);
        locks.releaseSlot(nextSlot);

        final int largeSlot = locks.acquireSlot(10 * workerCount + 2, circuitBreaker);
        Assert.assertEquals(2, largeSlot);
        locks.releaseSlot(largeSlot);
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testConcurrentAcquireReleaseHoldsMutualExclusion() throws Exception {
        // The one property the tests above cannot reach. They run on the JUnit thread, so the CAS in
        // acquireSlot never actually loses a race, and the parity protocol - the whole point of the
        // counter encoding - is never exercised against a competing acquirer. The safety argument
        // ("the holder is the only thread that can write a held slot") is asserted in a comment; this
        // is what exercises it.
        //
        // More threads than slots, so the acquire loop genuinely runs out and spins. workerId = -1
        // makes each thread start its probe at a random slot, which is the work-stealing path.
        final int threads = 8;
        final int slots = 3;
        final int rounds = 2_000;
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, slots);
        locks.setTestAcquireLatch(new CountDownLatch(0));
        final AtomicIntegerArray owners = new AtomicIntegerArray(slots);
        final AtomicInteger exclusionBreaches = new AtomicInteger();
        final CyclicBarrier start = new CyclicBarrier(threads);
        final CountDownLatch done = new CountDownLatch(threads);
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicBooleanCircuitBreaker[] circuitBreakers = new AtomicBooleanCircuitBreaker[threads];
        final Thread[] workers = new Thread[threads];

        for (int t = 0; t < threads; t++) {
            final int threadId = t + 1; // 0 means "free", so ids start at 1
            final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            circuitBreakers[t] = circuitBreaker;
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < rounds; i++) {
                        final int slot = locks.acquireSlot(-1, circuitBreaker);
                        if (slot < 0 || slot >= slots) {
                            errors.add(new AssertionError("slot out of range: " + slot));
                            return;
                        }
                        // Inside the critical section. Anyone else here at the same time is a
                        // mutual-exclusion failure, and this is how a reducer's per-worker state
                        // would get corrupted. The slot is held across a spin rather than a couple of
                        // instructions so that two holders actually overlap in time: a critical
                        // section only nanoseconds long lets a broken lock go undetected simply
                        // because the two threads rarely land inside it together.
                        if (!owners.compareAndSet(slot, 0, threadId)) {
                            exclusionBreaches.incrementAndGet();
                        }
                        for (int spin = 0; spin < 64; spin++) {
                            Os.pause();
                        }
                        if (!owners.compareAndSet(slot, threadId, 0)) {
                            exclusionBreaches.incrementAndGet();
                        }
                        locks.releaseSlot(slot);
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    done.countDown();
                }
            });
            // Keep this as a final safeguard if a broken acquire path ignores the cancellable breaker.
            // The finally block below cancels and joins every worker on normal and failure paths.
            thread.setDaemon(true);
            workers[t] = thread;
            thread.start();
        }

        try {
            Assert.assertTrue("threads did not finish", done.await(60, TimeUnit.SECONDS));
            if (!errors.isEmpty()) {
                throw new AssertionError("thread failed", errors.peek());
            }
            Assert.assertEquals("two threads held the same slot", 0, exclusionBreaches.get());
            // Every acquire was released, and every one of them was counted: a lost CAS or a
            // double-counted acquire would show up here.
            Assert.assertEquals(0, locks.getAcquiredSlotCount());
            Assert.assertEquals((long) threads * rounds, locks.getSlotAcquireCount());
        } finally {
            for (int i = 0; i < threads; i++) {
                circuitBreakers[i].cancel();
            }
            for (int i = 0; i < threads; i++) {
                workers[i].join(10_000);
                Assert.assertFalse("worker did not stop: " + i, workers[i].isAlive());
            }
        }
    }

    @Test
    public void testReleaseIgnoresNoSlot() {
        // Reducers pass the id they got from maybeAcquire back to release unconditionally, and the
        // owner thread gets -1 (it uses its private state and takes no slot).
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 2);
        final int slot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        locks.releaseSlot(-1);
        Assert.assertEquals(1, locks.getAcquiredSlotCount());
        locks.releaseSlot(slot);
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testWorkerIdsOutsideSlotRangeAreNormalized() {
        final int workerCount = 4;
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, workerCount);

        final int boundarySlot = locks.acquireSlot(
                workerCount,
                SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
        );
        Assert.assertEquals(0, boundarySlot);
        final int nextSlot = locks.acquireSlot(
                workerCount,
                SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
        );
        Assert.assertEquals(1, nextSlot);
        locks.releaseSlot(boundarySlot);
        locks.releaseSlot(nextSlot);

        final int largeSlot = locks.acquireSlot(
                10 * workerCount + 2,
                SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
        );
        Assert.assertEquals(2, largeSlot);
        locks.releaseSlot(largeSlot);
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }
}

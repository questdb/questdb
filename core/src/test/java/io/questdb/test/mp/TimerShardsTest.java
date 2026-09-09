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

package io.questdb.test.mp;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.DelayedFireable;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ThreadMXBean;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

public class TimerShardsTest {
    private static final Log LOG = LogFactory.getLog(TimerShardsTest.class);

    @Test
    public void testExpiresEntriesAtDeadline() throws InterruptedException {
        TimerShards shards = new TimerShards(2, "test-timer", LOG);
        shards.start();
        try {
            int n = 100;
            CountDownLatch latch = new CountDownLatch(n);
            AtomicInteger fires = new AtomicInteger();
            long base = System.currentTimeMillis();
            for (int i = 0; i < n; i++) {
                long deadline = base + (i % 100);
                shards.register(new TestEntry(deadline, () -> {
                    fires.incrementAndGet();
                    latch.countDown();
                }, null));
            }
            Assert.assertTrue("not all entries fired in time", latch.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(n, fires.get());
        } finally {
            shards.shutdown();
        }
    }

    @Test
    public void testConcurrentHaltAndShutdown() throws Exception {
        final TimerShards shards = new TimerShards(1, "test-concurrent-stop", LOG);
        final CyclicBarrier startBarrier = new CyclicBarrier(3);
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        final Thread haltThread = new Thread(() -> {
            try {
                startBarrier.await(5, TimeUnit.SECONDS);
                shards.halt();
            } catch (Throwable th) {
                failure.compareAndSet(null, th);
            }
        }, "test-concurrent-halt");
        final Thread shutdownThread = new Thread(() -> {
            try {
                startBarrier.await(5, TimeUnit.SECONDS);
                shards.shutdown();
            } catch (Throwable th) {
                failure.compareAndSet(null, th);
            }
        }, "test-concurrent-shutdown");

        try {
            haltThread.start();
            shutdownThread.start();
            startBarrier.await(5, TimeUnit.SECONDS);
            haltThread.join(TimeUnit.SECONDS.toMillis(5));
            shutdownThread.join(TimeUnit.SECONDS.toMillis(5));
        } finally {
            haltThread.interrupt();
            shutdownThread.interrupt();
            haltThread.join(TimeUnit.SECONDS.toMillis(5));
            shutdownThread.join(TimeUnit.SECONDS.toMillis(5));
            shards.halt();
        }

        Assert.assertFalse("halt thread did not stop", haltThread.isAlive());
        Assert.assertFalse("shutdown thread did not stop", shutdownThread.isAlive());
        Assert.assertNull("concurrent lifecycle call failed", failure.get());
    }

    @Test
    public void testHaltDoesNotRepeatExpiredJoinBudget() throws Exception {
        final CountDownLatch expireStarted = new CountDownLatch(1);
        final CountDownLatch haltReturned = new CountDownLatch(1);
        final CountDownLatch releaseExpire = new CountDownLatch(1);
        final AtomicReference<Thread> timerThread = new AtomicReference<>();
        final TimerShards shards = new TimerShards(1, "test-halt-after-timeout", LOG);
        final Thread haltThread = new Thread(() -> {
            shards.halt();
            haltReturned.countDown();
        }, "test-halt-after-timeout-caller");

        try {
            shards.start();
            shards.register(new TestEntry(System.currentTimeMillis(), () -> {
                timerThread.set(Thread.currentThread());
                expireStarted.countDown();
                TestUtils.await(releaseExpire);
            }, null));
            Assert.assertTrue("timer entry did not start", expireStarted.await(5, TimeUnit.SECONDS));

            // Exhaust this shard's join budget once in shutdown(). halt() must
            // not apply a fresh budget to the same still-blocked thread.
            Assert.assertFalse(
                    "shutdown did not exhaust the expired shard join budget",
                    shards.shutdown(System.nanoTime() - 1)
            );
            haltThread.start();
            Assert.assertTrue(
                    "halt repeated the expired shard join budget",
                    haltReturned.await(500, TimeUnit.MILLISECONDS)
            );
        } finally {
            releaseExpire.countDown();
            haltThread.join(TimeUnit.SECONDS.toMillis(5));
            final Thread timer = timerThread.get();
            if (timer != null) {
                timer.join(TimeUnit.SECONDS.toMillis(5));
            }
            shards.halt();
        }
        Assert.assertFalse("halt thread did not stop", haltThread.isAlive());
        Assert.assertFalse("shard thread did not stop", timerThread.get().isAlive());
    }

    @Test
    public void testInterruptDoesNotSpinAndShardKeepsRunning() throws Exception {
        final String threadName = "test-interrupted-timer-0";
        TimerShards shards = new TimerShards(1, "test-interrupted-timer", LOG);
        shards.start();
        try {
            shards.register(new TestEntry(System.currentTimeMillis() + 60_000, null, null));

            Thread timerThread = null;
            for (Thread thread : Thread.getAllStackTraces().keySet()) {
                if (threadName.equals(thread.getName())) {
                    timerThread = thread;
                    break;
                }
            }
            Assert.assertNotNull("timer thread not found", timerThread);
            final Thread timer = timerThread;
            TestUtils.assertEventually(
                    () -> Assert.assertEquals(Thread.State.TIMED_WAITING, timer.getState()),
                    5
            );

            try (TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadCpuTimeScope()) {
                ThreadMXBean bean = scope.getBean();
                Assume.assumeTrue("thread CPU time measurement not supported", bean.isThreadCpuTimeSupported());
                long cpuBefore = bean.getThreadCpuTime(timer.threadId());
                Assert.assertTrue("CPU time measurement is disabled", cpuBefore >= 0);
                timer.interrupt();
                Thread.sleep(500);
                long cpuAfter = bean.getThreadCpuTime(timer.threadId());
                Assert.assertTrue("CPU time moved backwards", cpuAfter >= cpuBefore);
                Assert.assertTrue(
                        "interrupted timer burned " + (cpuAfter - cpuBefore) + "ns of CPU time",
                        cpuAfter - cpuBefore < TimeUnit.MILLISECONDS.toNanos(100)
                );
            }

            CountDownLatch fired = new CountDownLatch(1);
            shards.register(new TestEntry(System.currentTimeMillis(), fired::countDown, null));
            Assert.assertTrue("interrupted shard stopped firing entries", fired.await(5, TimeUnit.SECONDS));
        } finally {
            shards.shutdown();
        }
    }

    @Test
    public void testInterruptedShutdownJoinsShardThreads() throws Exception {
        final CountDownLatch expireStarted = new CountDownLatch(1);
        final CountDownLatch releaseExpire = new CountDownLatch(1);
        final CountDownLatch shutdownReturned = new CountDownLatch(1);
        final AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        final AtomicReference<Thread> timerThread = new AtomicReference<>();
        final TimerShards shards = new TimerShards(1, "test-interrupted-shutdown", LOG);
        final AtomicReference<Boolean> isInterruptRestored = new AtomicReference<>();
        final Thread shutdownThread = new Thread(() -> {
            try {
                Thread.currentThread().interrupt();
                shards.shutdown();
                isInterruptRestored.set(Thread.currentThread().isInterrupted());
            } catch (Throwable th) {
                shutdownFailure.set(th);
            } finally {
                shutdownReturned.countDown();
            }
        }, "test-interrupted-shutdown-caller");

        try {
            shards.start();
            shards.register(new TestEntry(System.currentTimeMillis(), () -> {
                timerThread.set(Thread.currentThread());
                expireStarted.countDown();
                TestUtils.await(releaseExpire);
            }, null));
            Assert.assertTrue("timer entry did not start", expireStarted.await(5, TimeUnit.SECONDS));

            shutdownThread.start();
            TestUtils.assertEventually(() -> {
                if (shutdownReturned.getCount() == 0) {
                    return;
                }
                Assert.assertFalse("shutdown thread has not consumed its interrupt", shutdownThread.isInterrupted());
                boolean isJoining = false;
                for (StackTraceElement frame : shutdownThread.getStackTrace()) {
                    if (frame.getMethodName().equals("join")) {
                        isJoining = true;
                        break;
                    }
                }
                Assert.assertTrue("shutdown thread did not wait for the shard", isJoining);
            }, 1);
            Assert.assertEquals("shutdown returned while the shard was active", 1, shutdownReturned.getCount());
            Assert.assertTrue("shard stopped before the callback was released", timerThread.get().isAlive());
        } finally {
            releaseExpire.countDown();
            shutdownThread.join(TimeUnit.SECONDS.toMillis(5));
            final Thread timer = timerThread.get();
            if (timer != null) {
                timer.join(TimeUnit.SECONDS.toMillis(5));
            }
            shards.halt();
        }
        Assert.assertFalse("shutdown thread did not stop", shutdownThread.isAlive());
        Assert.assertNull("shutdown failed", shutdownFailure.get());
        Assert.assertEquals("shutdown did not restore the interrupt flag", Boolean.TRUE, isInterruptRestored.get());
        Assert.assertFalse("shard thread did not stop", timerThread.get().isAlive());
    }

    @Test
    public void testLateRegistrationAfterShutdown() {
        TimerShards shards = new TimerShards(1, "test-timer", LOG);
        shards.start();
        shards.shutdown();
        AtomicInteger shutdownCount = new AtomicInteger();
        SourceRegistrationResult result = shards.register(
                new TestEntry(System.currentTimeMillis() + 100_000, null, shutdownCount::incrementAndGet)
        );
        Assert.assertSame(SourceRegistrationResult.NOT_ACCEPTED, result);
        Assert.assertEquals(0, shutdownCount.get());
    }

    @Test
    public void testNonPowerOfTwoShardCountIsExact() {
        TimerShards shards = new TimerShards(3, "test-timer", LOG);
        Assert.assertEquals(3, shards.getShardCount());
    }

    @Test
    public void testRacesFireAndShutdown() throws InterruptedException {
        TimerShards shards = new TimerShards(1, "test-timer", LOG);
        shards.start();
        AtomicInteger fired = new AtomicInteger();
        AtomicInteger shutdown = new AtomicInteger();
        AtomicInteger terminalCount = new AtomicInteger();
        // Deadline now: race between expire firing and shutdown drain.
        shards.register(new TestEntry(System.currentTimeMillis(), () -> {
            if (terminalCount.incrementAndGet() == 1) fired.incrementAndGet();
        }, () -> {
            if (terminalCount.incrementAndGet() == 1) shutdown.incrementAndGet();
        }));
        Thread.sleep(2);
        shards.shutdown();
        // Either expire or shutdown won - exactly one terminal transition.
        Assert.assertEquals("one CAS should win, one no-op", 1, terminalCount.get());
        Assert.assertEquals(1, fired.get() + shutdown.get());
    }

    @Test
    public void testRegistrationRejectedWhenShutdownStartsAfterOffer() throws InterruptedException {
        CountDownLatch blockerEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        AtomicBoolean isShutdownHookEnabled = new AtomicBoolean();
        AtomicInteger terminalCount = new AtomicInteger();
        AtomicReference<Thread> shutdownThreadRef = new AtomicReference<>();
        AtomicReference<TimerShards> shardsRef = new AtomicReference<>();
        TimerShards shards = new TimerShards(1, "test-timer", LOG, () -> {
            if (isShutdownHookEnabled.compareAndSet(true, false)) {
                Thread shutdownThread = new Thread(shardsRef.get()::shutdown);
                shutdownThread.setDaemon(true);
                shutdownThreadRef.set(shutdownThread);
                shutdownThread.start();
                awaitThreadWaiting(shutdownThread);
            }
        });
        shardsRef.set(shards);
        shards.start();
        try {
            Assert.assertSame(
                    SourceRegistrationResult.ACCEPTED,
                    shards.register(new TestEntry(System.currentTimeMillis(), () -> {
                        blockerEntered.countDown();
                        try {
                            releaseBlocker.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }, null))
            );
            Assert.assertTrue(blockerEntered.await(5, TimeUnit.SECONDS));

            isShutdownHookEnabled.set(true);
            TestEntry entry = new TestEntry(
                    System.currentTimeMillis() + 60_000,
                    terminalCount::incrementAndGet,
                    terminalCount::incrementAndGet
            );
            Assert.assertSame(SourceRegistrationResult.NOT_ACCEPTED, shards.register(entry));
            Assert.assertEquals(0, terminalCount.get());

            releaseBlocker.countDown();
            Thread shutdownThread = shutdownThreadRef.get();
            shutdownThread.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse(shutdownThread.isAlive());
            Assert.assertEquals(0, terminalCount.get());
        } finally {
            releaseBlocker.countDown();
            shards.shutdown();
        }
    }

    @Test
    public void testRegistrationResultOwnsShutdownRace() throws InterruptedException {
        final int count = 1_000;
        TimerShards shards = new TimerShards(1, "test-timer", LOG);
        shards.start();
        AtomicIntegerArray terminalCounts = new AtomicIntegerArray(count);
        AtomicReferenceArray<SourceRegistrationResult> results = new AtomicReferenceArray<>(count);
        Thread registerThread = new Thread(() -> {
            for (int i = 0; i < count; i++) {
                final int index = i;
                results.set(i, shards.register(new TestEntry(
                        System.currentTimeMillis() + 60_000,
                        () -> terminalCounts.incrementAndGet(index),
                        () -> terminalCounts.incrementAndGet(index)
                )));
            }
        });
        registerThread.start();
        shards.shutdown();
        registerThread.join();

        for (int i = 0; i < count; i++) {
            SourceRegistrationResult result = results.get(i);
            Assert.assertNotNull(result);
            Assert.assertEquals(
                    result == SourceRegistrationResult.ACCEPTED ? 1 : 0,
                    terminalCounts.get(i)
            );
        }
    }

    @Test
    public void testSentinelWakesBlockedTake() {
        TimerShards shards = new TimerShards(2, "test-timer", LOG);
        shards.start();
        // Register a far-future entry so the take() is parked.
        shards.register(new TestEntry(System.currentTimeMillis() + 60_000, () -> {
        }, null));
        long start = System.nanoTime();
        shards.halt();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("halt() should return promptly, took " + elapsedMs + "ms", elapsedMs < 1_000);
    }

    @Test
    public void testShardDistribution() {
        int shardCount = 4;
        TimerShards shards = new TimerShards(shardCount, "test-timer", LOG);
        // Register far-future entries and check size grows. We can't probe per-shard
        // size directly without exposing it, so we verify the total reflects every
        // register call (no silent drop on a healthy register).
        int n = 1_000;
        long deadline = System.currentTimeMillis() + 60_000;
        shards.start();
        try {
            for (int i = 0; i < n; i++) {
                shards.register(new TestEntry(deadline, () -> {
                }, null));
            }
            Assert.assertEquals(n, shards.size());
        } finally {
            shards.shutdown();
        }
    }

    @Test
    public void testShutdownCallbackCanReenterShutdown() {
        TimerShards shards = new TimerShards(1, "test-timer", LOG);
        AtomicInteger shutdownCount = new AtomicInteger();
        long deadline = System.currentTimeMillis() + 60_000;
        shards.start();
        shards.register(new TestEntry(deadline, null, () -> {
            shutdownCount.incrementAndGet();
            shards.shutdown();
        }));
        shards.register(new TestEntry(deadline + 60_000, null, shutdownCount::incrementAndGet));
        shards.shutdown();
        Assert.assertEquals(2, shutdownCount.get());
    }

    @Test
    public void testShutdownDoesNotDropPoppedEntry() {
        // A shard daemon that pops a due entry in the window after shutdown() clears the
        // running flag but before its drain snapshot runs must still fire that entry's
        // terminal hook: take() already removed it from the heap, so the drain cannot see
        // it, and dropping it would strand the continuation bound to it. Race register-due-now
        // against shutdown() many times; every entry must receive exactly one terminal call
        // (expire or shutdown), never zero.
        final int iterations = 200;
        final int perIteration = 64;
        for (int iter = 0; iter < iterations; iter++) {
            TimerShards shards = new TimerShards(1, "test-timer", LOG);
            shards.start();
            AtomicInteger terminalCalls = new AtomicInteger();
            Runnable terminal = terminalCalls::incrementAndGet;
            long now = System.currentTimeMillis();
            for (int i = 0; i < perIteration; i++) {
                shards.register(new TestEntry(now, terminal, terminal));
            }
            shards.shutdown();
            Assert.assertEquals(
                    "iteration " + iter + ": every entry must get exactly one terminal call (no drop, no double)",
                    perIteration,
                    terminalCalls.get()
            );
        }
    }

    @Test
    public void testShutdownDrainsAllRegardlessOfDeadline() throws InterruptedException {
        TimerShards shards = new TimerShards(2, "test-timer", LOG);
        shards.start();
        int n = 50;
        CountDownLatch latch = new CountDownLatch(n);
        long deadline = System.currentTimeMillis() + 60_000;
        for (int i = 0; i < n; i++) {
            shards.register(new TestEntry(deadline, null, latch::countDown));
        }
        shards.shutdown();
        Assert.assertTrue("shutdown should call shutdown() on every entry",
                latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testShutdownFromShardDoesNotSelfJoin() throws InterruptedException {
        final AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        final CountDownLatch shutdownReturned = new CountDownLatch(1);
        final TimerShards shards = new TimerShards(1, "test-timer", LOG);
        shards.start();
        try {
            Assert.assertSame(
                    SourceRegistrationResult.ACCEPTED,
                    shards.register(new TestEntry(System.currentTimeMillis(), () -> {
                        try {
                            shards.shutdown();
                        } catch (Throwable th) {
                            shutdownFailure.set(th);
                        } finally {
                            shutdownReturned.countDown();
                        }
                    }, null))
            );
            Assert.assertTrue(shutdownReturned.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(shutdownFailure.get() instanceof IllegalStateException);
            shards.shutdown();
        } finally {
            shards.shutdown();
        }
    }

    @Test
    public void testShutdownIsIdempotent() {
        TimerShards shards = new TimerShards(2, "test-timer", LOG);
        shards.start();
        shards.shutdown();
        Assert.assertEquals(0, shards.size());
        shards.shutdown();
        Assert.assertEquals(0, shards.size());
        shards.halt();
    }

    @Test
    public void testShutdownRestoresInterruptAfterJoining() {
        TimerShards shards = new TimerShards(1, "test-timer", LOG);
        shards.start();
        Thread.currentThread().interrupt();
        try {
            shards.shutdown();
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            shards.shutdown();
        }
    }

    @Test
    public void testUnregisterReleasesAcceptedEntry() {
        TimerShards shards = new TimerShards(1, "test-timer", LOG);
        shards.start();
        AtomicInteger terminalCount = new AtomicInteger();
        TestEntry entry = new TestEntry(
                System.currentTimeMillis() + 60_000,
                terminalCount::incrementAndGet,
                terminalCount::incrementAndGet
        );
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, shards.register(entry));
        Assert.assertTrue(shards.unregister(entry));
        shards.shutdown();
        Assert.assertEquals(0, terminalCount.get());
    }

    private static void awaitThreadWaiting(Thread thread) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            final Thread.State state = thread.getState();
            if (state == Thread.State.BLOCKED
                    || state == Thread.State.TIMED_WAITING
                    || state == Thread.State.WAITING) {
                return;
            }
            if (state == Thread.State.TERMINATED) {
                Assert.fail("thread terminated before waiting [name=" + thread.getName() + ']');
            }
            LockSupport.parkNanos(100_000);
        }
        Assert.fail("thread did not wait [name=" + thread.getName() + ", state=" + thread.getState() + ']');
    }

    private static final class TestEntry implements DelayedFireable {
        private final long deadlineMillis;
        private int heapIndex = -1;
        private final Runnable onExpire;
        private final Runnable onShutdown;

        TestEntry(long deadlineMillis, Runnable onExpire, Runnable onShutdown) {
            this.deadlineMillis = deadlineMillis;
            this.onExpire = onExpire;
            this.onShutdown = onShutdown;
        }

        @Override
        public int compareTo(@NotNull java.util.concurrent.Delayed o) {
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public void expire() {
            if (onExpire != null) onExpire.run();
        }

        @Override
        public long getDelay(@NotNull TimeUnit unit) {
            return unit.convert(deadlineMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int getHeapIndex() {
            return heapIndex;
        }

        @Override
        public void setHeapIndex(int heapIndex) {
            this.heapIndex = heapIndex;
        }

        @Override
        public void shutdown() {
            if (onShutdown != null) onShutdown.run();
        }
    }
}

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
import io.questdb.mp.continuation.TimerShards;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void testLateRegistrationAfterShutdown() {
        TimerShards shards = new TimerShards(1, "test-timer", LOG);
        shards.start();
        shards.shutdown();
        AtomicInteger shutdownCount = new AtomicInteger();
        shards.register(new TestEntry(System.currentTimeMillis() + 100_000, null, shutdownCount::incrementAndGet));
        Assert.assertEquals(1, shutdownCount.get());
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
        Assert.assertEquals("one CAS should win, one no-op", 1, terminalCount.get() == 1 ? 1 : 0);
        Assert.assertEquals(1, fired.get() + shutdown.get());
    }

    @Test
    public void testSentinelWakesBlockedTake() throws InterruptedException {
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
        // Don't start threads - just register far-future entries and check size grows.
        // We can't probe per-shard size directly without exposing it, so we verify the
        // total reflects every register call (no silent drop on a healthy register).
        int n = 1_000;
        long deadline = System.currentTimeMillis() + 60_000;
        for (int i = 0; i < n; i++) {
            // Force running to true via start() since register guards on running.
        }
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
    public void testShutdownIsIdempotent() {
        TimerShards shards = new TimerShards(2, "test-timer", LOG);
        shards.start();
        shards.shutdown();
        shards.shutdown();
        shards.halt();
    }

    private static final class TestEntry implements DelayedFireable {
        private final long deadlineMillis;
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
        public void shutdown() {
            if (onShutdown != null) onShutdown.run();
        }
    }
}

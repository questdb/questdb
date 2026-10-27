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

import io.questdb.mp.IdleParkJob;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A worker parks in an {@link IdleParkJob} only in place of its own nap or sleep, for no longer,
 * and, in a fiber-hosting pool, only while a sibling can take the fiber wake-ups that a worker
 * waiting in a job cannot.
 */
public class WorkerIdleParkTest {
    private static final long SLEEP_MILLIS = 50;

    @Test
    public void testErrorInParkDoesNotKillWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (WorkerPoolMode mode : WorkerPoolMode.values()) {
                final ParkJob job = new ParkJob();
                job.failNext.set(true);
                try (WorkerPool pool = newPool(mode, 2, SLEEP_MILLIS)) {
                    pool.assign(job);
                    pool.start();
                    TestUtils.assertEventually(() -> Assert.assertTrue(mode.name(), job.parks.get() > 3), 10);
                    Assert.assertFalse(job.failNext.get());
                }
            }
        });
    }

    @Test
    public void testFiberWorkerParksInJobOnlyWithReadySibling() throws Exception {
        // alone, a fiber-hosting worker is the only fiber wake target: it never parks in a job
        TestUtils.assertMemoryLeak(() -> {
            final ParkJob job = new ParkJob();
            try (WorkerPool pool = newPool(WorkerPoolMode.FIBER_HOST, 1, SLEEP_MILLIS)) {
                pool.assign(job);
                pool.start();
                Os.sleep(500);
                Assert.assertEquals(0, job.parks.get());
            }
            // a legacy worker has no fiber wake-ups to miss
            final ParkJob legacyJob = new ParkJob();
            try (WorkerPool pool = newPool(WorkerPoolMode.LEGACY, 1, SLEEP_MILLIS)) {
                pool.assign(legacyJob);
                pool.start();
                TestUtils.assertEventually(() -> Assert.assertTrue(legacyJob.parks.get() > 3), 10);
            }
            // nor does one with a sibling
            final ParkJob pairJob = new ParkJob();
            try (WorkerPool pool = newPool(WorkerPoolMode.FIBER_HOST, 2, SLEEP_MILLIS)) {
                pool.assign(pairJob);
                pool.start();
                TestUtils.assertEventually(() -> Assert.assertTrue(pairJob.parks.get() > 3), 10);
            }
        });
    }

    @Test
    public void testFiberWokenWhileSiblingParksInJob() throws Exception {
        // the worker parked in the job cannot be woken by an unpark; a fiber launched meanwhile
        // must wake its sibling, long before the park would end
        TestUtils.assertMemoryLeak(() -> {
            final long parkMillis = 5_000;
            final ParkJob job = new ParkJob();
            job.holdFullPark = true;
            try (WorkerPool pool = newPool(WorkerPoolMode.FIBER_HOST, 2, parkMillis)) {
                pool.assign(job);
                pool.start();
                try {
                    for (int i = 0; i < 5; i++) {
                        // a sleep-length park, not a nap
                        TestUtils.assertEventually(() -> Assert.assertTrue(job.inPark), 30);
                        // let the sibling back off past its naps, into its own sleep
                        Os.sleep(500);
                        final CountDownLatch ran = new CountDownLatch(1);
                        final long start = System.nanoTime();
                        Assert.assertEquals(LaunchResult.LAUNCHED, pool.getFiberRuntime().launch(new FiberTask() {
                            @Override
                            protected boolean runStep() {
                                ran.countDown();
                                return true;
                            }
                        }));
                        Assert.assertTrue(ran.await(30, TimeUnit.SECONDS));
                        final long millis = (System.nanoTime() - start) / 1_000_000;
                        Assert.assertTrue("the fiber waited " + millis + "ms for a worker", millis < parkMillis / 5);
                        Assert.assertTrue("the park ended early", job.inPark);
                    }
                } finally {
                    job.release.countDown();
                }
            }
        });
    }

    @Test
    public void testParksOnlyInPlaceOfNapOrSleep() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (WorkerPoolMode mode : WorkerPoolMode.values()) {
                final ParkJob job = new ParkJob();
                try (WorkerPool pool = newPool(mode, 2, SLEEP_MILLIS)) {
                    pool.assign(job);
                    pool.start();
                    // idle: the workers nap, then sleep, in the job
                    TestUtils.assertEventually(() -> Assert.assertTrue(mode.name(), job.parks.get() > 3), 10);
                    for (Long nanos : job.parkNanos) {
                        Assert.assertTrue(
                                mode + ": parked for " + nanos + "ns",
                                nanos == 1_000_000L || nanos == TimeUnit.MILLISECONDS.toNanos(SLEEP_MILLIS)
                        );
                    }
                    // busy: a worker that finds work never parks
                    job.busy = true;
                    Os.sleep(SLEEP_MILLIS * 3);
                    final int parks = job.parks.get();
                    Os.sleep(500);
                    Assert.assertEquals(mode.name(), parks, job.parks.get());
                    job.busy = false;
                }
            }
        });
    }

    private static WorkerPool newPool(WorkerPoolMode mode, int workerCount, long sleepMillis) {
        return new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public long getNapThreshold() {
                return 100;
            }

            @Override
            public String getPoolName() {
                return "idle-park-test";
            }

            @Override
            public long getSleepThreshold() {
                return 200;
            }

            @Override
            public long getSleepTimeout() {
                return sleepMillis;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return mode;
            }

            @Override
            public long getYieldThreshold() {
                return 10;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        });
    }

    private static class ParkJob implements IdleParkJob {
        final AtomicBoolean failNext = new AtomicBoolean();
        final AtomicBoolean locked = new AtomicBoolean();
        final ConcurrentLinkedQueue<Long> parkNanos = new ConcurrentLinkedQueue<>();
        final AtomicInteger parks = new AtomicInteger();
        final CountDownLatch release = new CountDownLatch(1);
        volatile boolean busy;
        volatile boolean holdFullPark;
        volatile boolean inPark;

        @Override
        public void cancelIdlePark() {
            locked.set(false);
        }

        @Override
        public boolean idlePark(long maxParkNanos) {
            try {
                parks.incrementAndGet();
                parkNanos.add(maxParkNanos);
                if (failNext.compareAndSet(true, false)) {
                    throw new RuntimeException("injected park failure");
                }
                if (holdFullPark && maxParkNanos > 1_000_000L) {
                    // like a socket poll: an unpark does not end it
                    inPark = true;
                    try {
                        release.await(maxParkNanos, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        inPark = false;
                    }
                }
                return false;
            } finally {
                locked.set(false);
            }
        }

        @Override
        public boolean run(@NotNull WorkerContext workerContext) {
            return busy;
        }

        @Override
        public boolean tryBeginIdlePark() {
            return locked.compareAndSet(false, true);
        }
    }
}

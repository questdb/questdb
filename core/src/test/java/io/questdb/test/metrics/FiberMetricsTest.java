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

package io.questdb.test.metrics;

import io.questdb.metrics.FiberMetrics;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class FiberMetricsTest {
    private static final long AWAIT_SECONDS = 10;

    @Test
    public void testScrapeLocalFallbackPublicationAndClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool(4, WorkerPoolMode.FIBER_HOST);
            final FiberRuntime runtime = pool.getFiberRuntime();
            final FiberMetrics metrics = new FiberMetrics();
            metrics.register("test", runtime);

            final int taskCount = runtime.getLocalQueueCapacityForTesting(0) + 1;
            final CountDownLatch allTasksRan = new CountDownLatch(taskCount);
            final CountDownLatch peersBlocked = new CountDownLatch(pool.getWorkerCount() - 1);
            final CountDownLatch publicationCommitted = new CountDownLatch(1);
            final CountDownLatch releasePublisher = new CountDownLatch(1);
            final AtomicBoolean isLaunched = new AtomicBoolean();
            final AtomicReference<Throwable> jobError = new AtomicReference<>();
            for (int workerId = 1; workerId < pool.getWorkerCount(); workerId++) {
                final AtomicBoolean isPeerBlocked = new AtomicBoolean();
                pool.assign(workerId, workerContext -> {
                    if (isPeerBlocked.compareAndSet(false, true)) {
                        peersBlocked.countDown();
                        try {
                            if (!releasePublisher.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting to release peer Worker");
                            }
                        } catch (Throwable th) {
                            jobError.compareAndSet(null, th);
                        }
                    }
                    return false;
                });
            }
            pool.assign(0, workerContext -> {
                if (isLaunched.compareAndSet(false, true)) {
                    try {
                        if (!peersBlocked.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting for peer Workers to block");
                        }
                        for (int i = 0; i < taskCount; i++) {
                            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                                @Override
                                protected boolean runStep() {
                                    allTasksRan.countDown();
                                    return true;
                                }
                            }));
                        }
                        publicationCommitted.countDown();
                        if (!releasePublisher.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting to release publishing Worker");
                        }
                    } catch (Throwable th) {
                        jobError.compareAndSet(null, th);
                        publicationCommitted.countDown();
                    }
                }
                return false;
            });
            pool.start();
            try {
                Assert.assertTrue(publicationCommitted.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                TestUtils.rethrowFirst(jobError);
                Assert.assertEquals(1, runtime.getLocalFallbackPublicationCount());

                try (DirectUtf8Sink sink = new DirectUtf8Sink(2048)) {
                    metrics.scrapeIntoPrometheus(sink);
                    assertOccursExactlyOnce(
                            sink.toString(),
                            "questdb_worker_pool_fiber_scheduler_publication_total{worker_pool=\"test\",route=\"local_fallback\"} 1\n"
                    );

                    metrics.clear();
                    sink.clear();
                    metrics.scrapeIntoPrometheus(sink);
                    assertOccursExactlyOnce(
                            sink.toString(),
                            "questdb_worker_pool_fiber_scheduler_publication_total{worker_pool=\"test\",route=\"local_fallback\"} 0\n"
                    );
                }

                releasePublisher.countDown();
                Assert.assertTrue(allTasksRan.await(AWAIT_SECONDS, TimeUnit.SECONDS));
            } finally {
                releasePublisher.countDown();
                pool.halt();
            }
            TestUtils.rethrowFirst(jobError);
        });
    }

    @Test
    public void testScrapeClearAndUnregister() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberMetrics metrics = new FiberMetrics();
            metrics.register("test\"pool", runtime);

            final Fiber fiber = runtime.tryReserveFiber();
            Assert.assertNotNull(fiber);
            Assert.assertNull(runtime.tryReserveFiber());
            runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());
            final OneShotTask task = new OneShotTask();
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(LaunchResult.SATURATED, runtime.launch(new OneShotTask()));
            Assert.assertEquals(1, runtime.drain(1));

            try (DirectUtf8Sink sink = new DirectUtf8Sink(2048)) {
                metrics.scrapeIntoPrometheus(sink);
                final String value = sink.toString();
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_live{worker_pool=\"test\\\"pool\"} 1\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_max_live{worker_pool=\"test\\\"pool\"} 1\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_outstanding{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_queued{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_mounted{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_finalizing{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_parked{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_retained{worker_pool=\"test\\\"pool\"} 1\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_created_total{worker_pool=\"test\\\"pool\"} 1\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_retired_total{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_mount_total{worker_pool=\"test\\\"pool\"} 1\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_saturation_total{worker_pool=\"test\\\"pool\"} 2\n");
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_mount_budget_exhaustion_total{worker_pool=\"test\\\"pool\"} 0\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_inline_suspend_violation_total{worker_pool=\"test\\\"pool\"} 0\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_scheduler_publication_total{worker_pool=\"test\\\"pool\",route=\"global\"} 1\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_scheduler_selection_total{worker_pool=\"test\\\"pool\",source=\"global\"} 1\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_wake_total{worker_pool=\"test\\\"pool\"} 0\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_orphaned_shard_total{worker_pool=\"test\\\"pool\"} 0\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_orphan_recovery_total{worker_pool=\"test\\\"pool\"} 0\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_launch_total{worker_pool=\"test\\\"pool\",result=\"launched\"} 1\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_launch_total{worker_pool=\"test\\\"pool\",result=\"saturated\"} 1\n"
                );

                metrics.clear();
                sink.clear();
                metrics.scrapeIntoPrometheus(sink);
                TestUtils.assertContains(
                        sink.toString(),
                        "questdb_worker_pool_fiber_saturation_total{worker_pool=\"test\\\"pool\"} 0\n"
                );
                TestUtils.assertContains(
                        sink.toString(),
                        "questdb_worker_pool_fiber_launch_total{worker_pool=\"test\\\"pool\",result=\"launched\"} 0\n"
                );
                TestUtils.assertContains(
                        sink.toString(),
                        "questdb_worker_pool_fiber_scheduler_publication_total{worker_pool=\"test\\\"pool\",route=\"global\"} 0\n"
                );
                TestUtils.assertContains(
                        sink.toString(),
                        "questdb_worker_pool_fiber_scheduler_selection_total{worker_pool=\"test\\\"pool\",source=\"global\"} 0\n"
                );

                metrics.unregister(runtime);
                sink.clear();
                metrics.scrapeIntoPrometheus(sink);
                Assert.assertEquals(0, sink.size());
            } finally {
                close(runtime);
            }
        });
    }

    private static void assertOccursExactlyOnce(String value, String expected) {
        int occurrenceCount = 0;
        int fromIndex = 0;
        while ((fromIndex = value.indexOf(expected, fromIndex)) != -1) {
            occurrenceCount++;
            fromIndex += expected.length();
        }
        Assert.assertEquals("unexpected sample occurrence count", 1, occurrenceCount);
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static class OneShotTask extends FiberTask {
        @Override
        protected boolean runStep() {
            return true;
        }
    }
}

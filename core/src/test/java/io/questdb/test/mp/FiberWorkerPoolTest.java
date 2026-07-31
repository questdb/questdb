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

import io.questdb.Metrics;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FiberWorkerPoolTest {

    @Test
    public void testFiberHostHaltRetriesAfterRuntimeDrainTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool(fiberHostConfiguration(
                    "fiber-halt-retry-test",
                    1,
                    true
            ));
            final AtomicBoolean isCleanerClosed = new AtomicBoolean();
            final CountDownLatch releaseTask = new CountDownLatch(1);
            final CountDownLatch taskEntered = new CountDownLatch(1);
            final FiberTask task = new FiberTask() {
                @Override
                protected boolean runStep() {
                    taskEntered.countDown();
                    try {
                        releaseTask.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                    return true;
                }
            };
            pool.assign(workerContext -> true);
            pool.assignThreadLocalCleaner(0, () -> isCleanerClosed.set(true));
            final FiberRuntime runtime = pool.getFiberRuntime();
            pool.start();
            try {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertTrue(taskEntered.await(10, TimeUnit.SECONDS));
                Assert.assertFalse(pool.halt(TimeUnit.MILLISECONDS.toNanos(1)));
                Assert.assertFalse(isCleanerClosed.get());
                Assert.assertEquals(FiberRuntimeState.QUIESCING, runtime.state());

                releaseTask.countDown();
                Assert.assertTrue(pool.halt(TimeUnit.SECONDS.toNanos(10)));
                Assert.assertTrue(isCleanerClosed.get());
                Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
            } finally {
                releaseTask.countDown();
                pool.halt(TimeUnit.SECONDS.toNanos(10));
            }
        });
    }

    @Test
    public void testFiberHostHaltStrictTimeoutSignalsWorkersBeforeThrowing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool(fiberHostConfiguration(
                    "fiber-strict-halt-timeout-test",
                    1,
                    true
            ));
            final CountDownLatch cleanerClosed = new CountDownLatch(1);
            final CountDownLatch releaseTask = new CountDownLatch(1);
            final CountDownLatch taskEntered = new CountDownLatch(1);
            final FiberTask task = new FiberTask() {
                @Override
                protected boolean runStep() {
                    taskEntered.countDown();
                    try {
                        releaseTask.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                    return true;
                }
            };
            pool.assign(workerContext -> true);
            pool.assignThreadLocalCleaner(0, cleanerClosed::countDown);
            final FiberRuntime runtime = pool.getFiberRuntime();
            pool.start();
            try {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertTrue(taskEntered.await(10, TimeUnit.SECONDS));
                try {
                    pool.haltAndAssertCleanForTest(TimeUnit.MILLISECONDS.toNanos(1));
                    Assert.fail();
                } catch (AssertionError ignored) {
                }

                releaseTask.countDown();
                Assert.assertTrue(cleanerClosed.await(10, TimeUnit.SECONDS));
            } finally {
                releaseTask.countDown();
                pool.halt(TimeUnit.SECONDS.toNanos(10));
            }
        });
    }

    @Test
    public void testFiberHostHaltTerminalRetriesAfterRuntimeDrainTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool(fiberHostConfiguration(
                    "fiber-terminal-halt-retry-test",
                    1,
                    true,
                    129
            ));
            final CountDownLatch releaseTask = new CountDownLatch(1);
            final CountDownLatch taskEntered = new CountDownLatch(1);
            final FiberTask blockingTask = new FiberTask() {
                @Override
                protected boolean runStep() {
                    taskEntered.countDown();
                    try {
                        releaseTask.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                    return true;
                }
            };
            final ObjList<OneShotTask> queuedTasks = new ObjList<>(128);
            final FiberRuntime runtime = pool.getFiberRuntime();
            pool.start();
            try {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(blockingTask));
                Assert.assertTrue(taskEntered.await(10, TimeUnit.SECONDS));
                for (int i = 0; i < 128; i++) {
                    final OneShotTask queuedTask = new OneShotTask();
                    queuedTasks.add(queuedTask);
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(queuedTask));
                }

                Assert.assertFalse(pool.isHaltTerminalSuccessfulForTesting(TimeUnit.MILLISECONDS.toNanos(1)));
                releaseTask.countDown();
                Assert.assertTrue(pool.halt(TimeUnit.SECONDS.toNanos(10)));
                for (int i = 0; i < queuedTasks.size(); i++) {
                    Assert.assertTrue(queuedTasks.getQuick(i).isDone());
                }
                Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
            } finally {
                releaseTask.countDown();
                pool.halt(TimeUnit.SECONDS.toNanos(10));
            }
        });
    }

    @Test
    public void testLegacyPoolRotatesJobOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int recordLimit = 32;
            final StringBuilder order = new StringBuilder(recordLimit);
            final CountDownLatch filled = new CountDownLatch(1);
            final class RecordingJob implements Job {
                private final char id;

                private RecordingJob(char id) {
                    this.id = id;
                }

                @Override
                public boolean run(@NotNull WorkerContext workerContext) {
                    if (order.length() < recordLimit) {
                        order.append(id);
                        if (order.length() == recordLimit) {
                            filled.countDown();
                        }
                    }
                    return false;
                }
            }
            final WorkerPool pool = new TestWorkerPool(
                    "legacy-job-rotation-test",
                    1,
                    Metrics.DISABLED,
                    WorkerPoolMode.LEGACY
            );
            pool.assign(new RecordingJob('a'));
            pool.assign(new RecordingJob('b'));
            pool.start();
            try {
                Assert.assertTrue(filled.await(10, TimeUnit.SECONDS));
            } finally {
                Assert.assertTrue(pool.halt(TimeUnit.SECONDS.toNanos(10)));
            }
            // without rotation the sequence is a strict "abab..." alternation; an adjacent repeat
            // is the rotation's observable signature
            final String sequence = order.toString();
            Assert.assertTrue(sequence, sequence.indexOf("aa") >= 0 || sequence.indexOf("bb") >= 0);
            Assert.assertTrue(sequence, sequence.indexOf('a') >= 0 && sequence.indexOf('b') >= 0);
        });
    }

    @Test
    public void testFiberHostRotatesJobAdmission() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool(fiberHostConfiguration(
                    "fiber-job-rotation-test",
                    1,
                    false
            ));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final CompetingFiberJob firstJob = new CompetingFiberJob(runtime);
            final CompetingFiberJob secondJob = new CompetingFiberJob(runtime);
            pool.assign(firstJob);
            pool.assign(secondJob);
            pool.start();
            try {
                TestUtils.assertEventually(() -> Assert.assertTrue(firstJob.completionCount.get() >= 20));
                TestUtils.assertEventually(() -> Assert.assertTrue(secondJob.completionCount.get() >= 20));
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testFiberHostRunsPlainJobsAndFibers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool(fiberHostConfiguration(
                    "fiber-host-test",
                    2,
                    false
            ));
            final AtomicBoolean hasRunPlainJob = new AtomicBoolean();
            final AtomicBoolean isPlainJobFrame = new AtomicBoolean();
            pool.assign(workerContext -> {
                isPlainJobFrame.set(Fiber.current() == null);
                hasRunPlainJob.set(true);
                return false;
            });
            final FiberRuntime runtime = pool.getFiberRuntime();
            pool.start();
            try {
                final WaitingTask waiting = new WaitingTask();
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(waiting));
                TestUtils.assertEventually(() -> Assert.assertEquals(1, waiting.waitQueue.size()));
                Assert.assertEquals(FiberTask.STATE_OWNED, waiting.getScheduleState());
                Assert.assertTrue(hasRunPlainJob.get());
                Assert.assertTrue(isPlainJobFrame.get());

                waiting.fire();
                TestUtils.assertEventually(() -> Assert.assertTrue(waiting.isDone()));
                Assert.assertNull(waiting.error);
                Assert.assertTrue(waiting.hasResumed);
                TestUtils.assertEventually(() -> Assert.assertEquals(1, runtime.getRetainedFiberCount()));

                for (int i = 0; i < 100; i++) {
                    final OneShotTask task = new OneShotTask();
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                    TestUtils.assertEventually(() -> Assert.assertTrue(task.isDone()));
                    TestUtils.assertEventually(() -> Assert.assertEquals(1, runtime.getRetainedFiberCount()));
                }
                Assert.assertEquals(1, runtime.getCreatedFiberCount());
                Assert.assertEquals(0, runtime.getRetiredFiberCount());
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testLegacyPoolHasNoFiberRuntime() {
        final TestWorkerPool pool = new TestWorkerPool(legacyConfiguration("legacy-pool-test", 1));
        Assert.assertEquals(WorkerPoolMode.LEGACY, pool.getWorkerPoolMode());
        Assert.assertFalse(pool.isFiberHost());
        try {
            pool.getFiberRuntime();
            Assert.fail();
        } catch (IllegalStateException ignored) {
        } finally {
            pool.halt();
        }
    }

    @Test
    public void testProductionConfigurationDefaultsToLegacy() {
        final WorkerPoolConfiguration configuration = () -> 1;
        Assert.assertEquals(WorkerPoolMode.LEGACY, configuration.getWorkerPoolMode());
    }

    @Test
    public void testTestPoolModeCanBeSelectedAtConstruction() {
        for (WorkerPoolMode mode : WorkerPoolMode.values()) {
            try (TestWorkerPool pool = new TestWorkerPool(1, mode)) {
                Assert.assertEquals(mode, pool.getWorkerPoolMode());
            }
        }
    }

    private static WorkerPoolConfiguration fiberHostConfiguration(String poolName, int workerCount, boolean isDaemon) {
        return fiberHostConfiguration(poolName, workerCount, isDaemon, 1);
    }

    private static WorkerPoolConfiguration fiberHostConfiguration(
            String poolName,
            int workerCount,
            boolean isDaemon,
            int maxLiveCount
    ) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                return maxLiveCount;
            }

            @Override
            public int getFiberRetainedCount() {
                return 1;
            }

            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.FIBER_HOST;
            }

            @Override
            public boolean isDaemonPool() {
                return isDaemon;
            }
        };
    }

    private static WorkerPoolConfiguration legacyConfiguration(String poolName, int workerCount) {
        return new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.LEGACY;
            }
        };
    }

    private static class CompetingFiberJob implements Job {
        private final AtomicInteger completionCount = new AtomicInteger();
        private final FiberRuntime runtime;
        private final FiberTask task = new FiberTask() {
            @Override
            protected void onDone() {
                completionCount.incrementAndGet();
            }

            @Override
            protected boolean runStep() {
                return true;
            }
        };

        private CompetingFiberJob(FiberRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        public boolean run(Job.WorkerContext workerContext) {
            if (task.isDone()) {
                task.reopen();
            }
            return runtime.launch(task) == LaunchResult.LAUNCHED;
        }
    }

    private static class OneShotTask extends FiberTask {
        @Override
        protected boolean runStep() {
            return true;
        }
    }

    private static class WaitingTask extends FiberTask {
        private volatile Throwable error;
        private volatile boolean hasResumed;
        private final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();

        private void fire() {
            waitQueue.fire(1, false);
        }

        @Override
        protected void onError(Throwable th) {
            error = th;
        }

        @Override
        protected boolean runStep() {
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            final long token = fiber.beginWaitBuild(1);
            final FiberWalWaitRegistration registration = coordinator.acquireWal(token, 1);
            try {
                if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED) {
                    throw new IllegalStateException("wait registration failed");
                }
                final int reason = fiber.suspendWait(token);
                if (reason != FiberWaitCoordinator.REASON_WAL) {
                    throw new IllegalStateException("unexpected wait reason [reason=" + reason + ']');
                }
                hasResumed = true;
                return true;
            } finally {
                registration.cancel();
                coordinator.abort(token);
                coordinator.consume(token);
            }
        }
    }
}

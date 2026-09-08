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
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.mp.DynamicFiberWorkerPoolConfiguration;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolConfigurationWrapper;
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
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class FiberWorkerPoolTest {

    @Test
    public void testBoundedHaltDoesNotMountParkedFiberAfterWorkersExit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int continuationWon = 2;
            final int haltWon = 1;
            final String poolName = "fiber-bounded-dead-worker-halt-test";
            final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return poolName;
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }

                @Override
                public boolean haltOnError() {
                    return true;
                }

                @Override
                public boolean isDaemonPool() {
                    return true;
                }
            });
            final FiberRuntime runtime = pool.getFiberRuntime();
            final CountDownLatch outcomeSelected = new CountDownLatch(1);
            final CountDownLatch releaseContinuation = new CountDownLatch(1);
            final CountDownLatch workerCleanerEntered = new CountDownLatch(1);
            final AtomicBoolean isTaskLaunched = new AtomicBoolean();
            final AtomicInteger outcome = new AtomicInteger();
            final AtomicInteger resumeReason = new AtomicInteger(-1);
            final AtomicReference<Boolean> haltResult = new AtomicReference<>();
            final AtomicReference<Thread> workerThread = new AtomicReference<>();
            final AtomicReference<Throwable> haltFailure = new AtomicReference<>();
            final FiberTask task = new FiberTask() {
                private final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();

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
                        resumeReason.set(fiber.suspendWait(token));
                        if (outcome.compareAndSet(0, continuationWon)) {
                            outcomeSelected.countDown();
                        }
                        try {
                            if (!releaseContinuation.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting to release Fiber continuation");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                        return true;
                    } finally {
                        registration.cancel();
                        coordinator.abort(token);
                        coordinator.consume(token);
                    }
                }
            };
            pool.assign(workerContext -> {
                if (isTaskLaunched.compareAndSet(false, true)) {
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                    return true;
                }
                if (runtime.getParkedFiberCount() == 1) {
                    throw new RuntimeException("deterministic worker failure");
                }
                return true;
            });
            pool.assignThreadLocalCleaner(0, () -> {
                workerThread.set(Thread.currentThread());
                workerCleanerEntered.countDown();
            });
            final Thread halter = new Thread(() -> {
                try {
                    haltResult.set(pool.haltWithin(TimeUnit.MILLISECONDS.toNanos(100)));
                    if (outcome.compareAndSet(0, haltWon)) {
                        outcomeSelected.countDown();
                    }
                } catch (Throwable th) {
                    haltFailure.set(th);
                    outcomeSelected.countDown();
                }
            }, poolName + "-halter");
            halter.setDaemon(true);
            pool.start();
            try {
                Assert.assertTrue(workerCleanerEntered.await(10, TimeUnit.SECONDS));
                final Thread exitedWorker = workerThread.get();
                Assert.assertNotNull(exitedWorker);
                exitedWorker.join(10_000L);
                Assert.assertFalse(exitedWorker.isAlive());
                Assert.assertEquals(1, runtime.getParkedFiberCount());

                halter.start();
                Assert.assertTrue(outcomeSelected.await(10, TimeUnit.SECONDS));
            } finally {
                releaseContinuation.countDown();
                pool.halt();
                halter.join(10_000L);
            }
            Assert.assertFalse(halter.isAlive());
            if (haltFailure.get() != null) {
                throw new AssertionError(haltFailure.get());
            }
            Assert.assertEquals(haltWon, outcome.get());
            Assert.assertEquals(Boolean.FALSE, haltResult.get());
            Assert.assertEquals(FiberWaitCoordinator.REASON_SHUTDOWN, resumeReason.get());
            Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
        });
    }

    @Test
    public void testCloseUsesTerminalHalt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final AtomicBoolean isBoundedHaltCalled = new AtomicBoolean();
            final AtomicBoolean isJobClosed = new AtomicBoolean();
            final AtomicBoolean isResourceClosed = new AtomicBoolean();
            try (
                    WorkerPool pool = new WorkerPool(legacyConfiguration("terminal-close-test", 1)) {
                        @Override
                        public boolean haltWithin(long timeoutNanos) {
                            isBoundedHaltCalled.set(true);
                            return false;
                        }
                    }
            ) {
                pool.assign(new Job() {
                    @Override
                    public void closeInstance() {
                        isJobClosed.set(true);
                    }

                    @Override
                    public boolean run(WorkerContext workerContext) {
                        return false;
                    }
                });
                pool.freeResourceOnExit(() -> isResourceClosed.set(true));
                pool.close();
                Assert.assertFalse(isBoundedHaltCalled.get());
                Assert.assertTrue(isJobClosed.get());
                Assert.assertTrue(isResourceClosed.get());
            }
        });
    }

    @Test
    public void testDynamicFiberConfigurationConstructionRollsBackPublication() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Metrics metrics = new Metrics(true, new MetricsRegistryImpl());
            final RuntimeException expected = new RuntimeException("listener registration failed");
            final RuntimeException cleanupFailure = new RuntimeException("listener rollback failed");
            final AtomicReference<DynamicFiberWorkerPoolConfiguration.FiberConfigurationListener> listenerReference =
                    new AtomicReference<>();
            final WorkerPoolConfigurationWrapper configuration = new WorkerPoolConfigurationWrapper() {
                @Override
                public Metrics getMetrics() {
                    return metrics;
                }

                @Override
                public void setFiberConfigurationListener(FiberConfigurationListener listener) {
                    super.setFiberConfigurationListener(listener);
                    listenerReference.set(listener);
                    if (listener != null) {
                        throw expected;
                    }
                    throw cleanupFailure;
                }
            };
            configuration.setDelegate(fiberHostConfiguration("fiber-construction-rollback-test", 1, false));

            final RuntimeException failure = Assert.assertThrows(
                    RuntimeException.class,
                    () -> new TestWorkerPool(configuration)
            );
            Assert.assertSame(expected, failure);
            Assert.assertArrayEquals(new Throwable[]{cleanupFailure}, failure.getSuppressed());
            Assert.assertNull(listenerReference.get());
            try (DirectUtf8Sink sink = new DirectUtf8Sink(512)) {
                metrics.fiberMetrics().scrapeIntoPrometheus(sink);
                Assert.assertEquals(0, sink.size());
            }
        });
    }

    @Test
    public void testDynamicFiberConfigurationConstructionUsesCoherentTuple() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch initialMaxLiveRead = new CountDownLatch(1);
            final CountDownLatch releaseInitialMaxLiveRead = new CountDownLatch(1);
            final WorkerPoolConfigurationWrapper configuration = new WorkerPoolConfigurationWrapper();
            configuration.setDelegate(fiberHostConfiguration(
                    "fiber-dynamic-construction-test",
                    1,
                    false,
                    2,
                    2,
                    3,
                    () -> {
                        initialMaxLiveRead.countDown();
                        try {
                            if (!releaseInitialMaxLiveRead.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting to release initial Fiber configuration");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
            ));

            final AtomicReference<Throwable> asyncError = new AtomicReference<>();
            final AtomicReference<TestWorkerPool> constructedPool = new AtomicReference<>();
            final Thread constructor = new Thread(() -> {
                try {
                    constructedPool.set(new TestWorkerPool(configuration));
                } catch (Throwable th) {
                    asyncError.set(th);
                }
            });
            constructor.start();
            try {
                Assert.assertTrue(initialMaxLiveRead.await(10, TimeUnit.SECONDS));
                configuration.setDelegate(fiberHostConfiguration(
                        "fiber-dynamic-construction-test",
                        1,
                        false,
                        4,
                        4,
                        5,
                        null
                ));
            } finally {
                releaseInitialMaxLiveRead.countDown();
                constructor.join(10_000L);
            }
            Assert.assertFalse(constructor.isAlive());

            final TestWorkerPool pool = constructedPool.get();
            try {
                if (asyncError.get() != null) {
                    throw new AssertionError(asyncError.get());
                }
                Assert.assertNotNull(pool);
                final FiberRuntime runtime = pool.getFiberRuntime();
                Assert.assertEquals(4, runtime.getMaxLiveFiberCount());
                Assert.assertEquals(4, runtime.getMaxRetainedFiberCount());
                Assert.assertEquals(5, runtime.getMountBudget());
            } finally {
                if (pool != null) {
                    pool.halt();
                }
            }
        });
    }

    @Test
    public void testDynamicFiberConfigurationHaltContinuesAfterListenerFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Metrics metrics = new Metrics(true, new MetricsRegistryImpl());
            final RuntimeException expected = new RuntimeException("listener removal failed");
            final AtomicReference<DynamicFiberWorkerPoolConfiguration.FiberConfigurationListener> listenerReference =
                    new AtomicReference<>();
            final WorkerPoolConfigurationWrapper configuration = new WorkerPoolConfigurationWrapper() {
                @Override
                public Metrics getMetrics() {
                    return metrics;
                }

                @Override
                public void setFiberConfigurationListener(FiberConfigurationListener listener) {
                    super.setFiberConfigurationListener(listener);
                    listenerReference.set(listener);
                    if (listener == null) {
                        throw expected;
                    }
                }
            };
            configuration.setDelegate(fiberHostConfiguration("fiber-halt-cleanup-test", 1, false));
            final TestWorkerPool pool = new TestWorkerPool(configuration);
            final AtomicBoolean isJobClosed = new AtomicBoolean();
            final AtomicBoolean isResourceClosed = new AtomicBoolean();
            pool.assign(new Job() {
                @Override
                public void closeInstance() {
                    isJobClosed.set(true);
                }

                @Override
                public boolean run(WorkerContext workerContext) {
                    return false;
                }
            });
            pool.freeResourceOnExit(() -> isResourceClosed.set(true));

            final RuntimeException failure = Assert.assertThrows(RuntimeException.class, pool::halt);
            Assert.assertSame(expected, failure);
            Assert.assertNull(listenerReference.get());
            Assert.assertTrue(isJobClosed.get());
            Assert.assertTrue(isResourceClosed.get());
            Assert.assertEquals(FiberRuntimeState.CLOSED, pool.getFiberRuntime().state());
            try (DirectUtf8Sink sink = new DirectUtf8Sink(512)) {
                metrics.fiberMetrics().scrapeIntoPrometheus(sink);
                Assert.assertEquals(0, sink.size());
            }
        });
    }

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
                Assert.assertFalse(pool.haltWithin(TimeUnit.MILLISECONDS.toNanos(1)));
                Assert.assertFalse(isCleanerClosed.get());
                Assert.assertEquals(FiberRuntimeState.QUIESCING, runtime.state());

                releaseTask.countDown();
                TestUtils.assertEventually(() -> Assert.assertTrue(isCleanerClosed.get()), 10);
                Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(10)));
                Assert.assertTrue(isCleanerClosed.get());
                Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
            } finally {
                releaseTask.countDown();
                pool.haltWithin(TimeUnit.SECONDS.toNanos(10));
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
                final AssertionError failure = Assert.assertThrows(
                        AssertionError.class,
                        () -> pool.haltAndAssertCleanForTest(TimeUnit.MILLISECONDS.toNanos(1))
                );
                TestUtils.assertContains(failure.getMessage(), "fiber runtime to drain");

                releaseTask.countDown();
                Assert.assertTrue(cleanerClosed.await(10, TimeUnit.SECONDS));
            } finally {
                releaseTask.countDown();
                pool.haltWithin(TimeUnit.SECONDS.toNanos(10));
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
                Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(10)));
                for (int i = 0; i < queuedTasks.size(); i++) {
                    Assert.assertTrue(queuedTasks.getQuick(i).isDone());
                }
                Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
            } finally {
                releaseTask.countDown();
                pool.haltWithin(TimeUnit.SECONDS.toNanos(10));
            }
        });
    }

    @Test
    public void testHaltDrainsFiberRuntimeAfterWorkersExit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String poolName = "fiber-dead-worker-halt-test";
            final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return poolName;
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }

                @Override
                public boolean haltOnError() {
                    return true;
                }

                @Override
                public boolean isDaemonPool() {
                    return true;
                }
            });
            final FiberRuntime runtime = pool.getFiberRuntime();
            final AtomicInteger tick = new AtomicInteger();
            pool.assign(workerContext -> {
                if (tick.incrementAndGet() == 1) {
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new OneShotTask()));
                    return false;
                }
                if (runtime.getCreatedFiberCount() > 0) {
                    throw new RuntimeException("deterministic worker failure");
                }
                return false;
            });
            pool.start();
            try {
                final long workerExitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (isWorkerThreadAlive(poolName) && System.nanoTime() < workerExitDeadline) {
                    Os.pause();
                }
                Assert.assertFalse(isWorkerThreadAlive(poolName));
                Assert.assertTrue(runtime.getCreatedFiberCount() > runtime.getRetiredFiberCount());

                final CountDownLatch haltReturned = new CountDownLatch(1);
                final AtomicReference<Throwable> haltFailure = new AtomicReference<>();
                final Thread halter = new Thread(() -> {
                    try {
                        pool.halt();
                    } catch (Throwable th) {
                        haltFailure.set(th);
                    } finally {
                        haltReturned.countDown();
                    }
                }, poolName + "-halter");
                halter.setDaemon(true);
                halter.start();
                Assert.assertTrue(haltReturned.await(10, TimeUnit.SECONDS));
                Assert.assertNull(haltFailure.get());
                Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
                Assert.assertEquals(runtime.getCreatedFiberCount(), runtime.getRetiredFiberCount());
            } finally {
                pool.haltWithin(TimeUnit.SECONDS.toNanos(10));
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
                Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(10)));
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
            final AtomicBoolean isPlainJobSuspensionBlocking = new AtomicBoolean();
            pool.assign(workerContext -> {
                isPlainJobFrame.set(Fiber.current() == null);
                isPlainJobSuspensionBlocking.set(
                        SuspensionScope.getMode() == SuspensionScope.Mode.BLOCKING
                );
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
                Assert.assertTrue(isPlainJobSuspensionBlocking.get());

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
    public void testLiveWorkerObservesMountBudgetUpdate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPoolConfigurationWrapper configuration = new WorkerPoolConfigurationWrapper();
            configuration.setDelegate(fiberHostConfiguration("fiber-mount-budget-test", 1, false, 4, 1));
            final TestWorkerPool pool = new TestWorkerPool(configuration);
            final CountDownLatch[] entered = {
                    new CountDownLatch(1),
                    new CountDownLatch(1),
                    new CountDownLatch(1)
            };
            final CountDownLatch[] release = {
                    new CountDownLatch(1),
                    new CountDownLatch(1),
                    new CountDownLatch(1)
            };
            final AtomicInteger loop = new AtomicInteger();
            final AtomicInteger completed = new AtomicInteger();
            pool.assign(workerContext -> {
                final int index = loop.getAndIncrement();
                if (index < entered.length) {
                    entered[index].countDown();
                    try {
                        release[index].await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
                return false;
            });
            final FiberRuntime runtime = pool.getFiberRuntime();
            pool.start();
            try {
                Assert.assertTrue(entered[0].await(10, TimeUnit.SECONDS));
                for (int i = 0; i < 4; i++) {
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                        @Override
                        protected void onDone() {
                            completed.incrementAndGet();
                        }

                        @Override
                        protected boolean runStep() {
                            return true;
                        }
                    }));
                }

                release[0].countDown();
                Assert.assertTrue(entered[1].await(10, TimeUnit.SECONDS));
                Assert.assertEquals(1, completed.get());

                configuration.setDelegate(fiberHostConfiguration("fiber-mount-budget-test", 1, false, 4, 3));
                Assert.assertEquals(3, runtime.getMountBudget());
                release[1].countDown();
                Assert.assertTrue(entered[2].await(10, TimeUnit.SECONDS));
                Assert.assertEquals(4, completed.get());
            } finally {
                for (int i = 0; i < release.length; i++) {
                    release[i].countDown();
                }
                pool.halt();
            }
        });
    }

    @Test
    public void testWorkerPoolConfigurationInterfaceDefaultsToLegacy() {
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

    @Test
    public void testTestWorkerPoolCloseUsesStrictDefaultTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final AtomicLong timeoutNanos = new AtomicLong(-1);
            try (
                    TestWorkerPool ignored = new TestWorkerPool(1, WorkerPoolMode.LEGACY) {
                        @Override
                        public void haltAndAssertCleanForTest(long timeout) {
                            timeoutNanos.set(timeout);
                            super.haltAndAssertCleanForTest(timeout);
                        }
                    }
            ) {
            }
            Assert.assertEquals(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS, timeoutNanos.get());
        });
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
        return fiberHostConfiguration(poolName, workerCount, isDaemon, maxLiveCount, 64);
    }

    private static WorkerPoolConfiguration fiberHostConfiguration(
            String poolName,
            int workerCount,
            boolean isDaemon,
            int maxLiveCount,
            int mountBudget
    ) {
        return fiberHostConfiguration(poolName, workerCount, isDaemon, maxLiveCount, 1, mountBudget, null);
    }

    private static WorkerPoolConfiguration fiberHostConfiguration(
            String poolName,
            int workerCount,
            boolean isDaemon,
            int maxLiveCount,
            int retainedCount,
            int mountBudget,
            Runnable beforeMaxLiveRead
    ) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                if (beforeMaxLiveRead != null) {
                    beforeMaxLiveRead.run();
                }
                return maxLiveCount;
            }

            @Override
            public int getFiberMountBudget() {
                return mountBudget;
            }

            @Override
            public int getFiberRetainedCount() {
                return retainedCount;
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

    private static boolean isWorkerThreadAlive(String poolName) {
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && thread.getName().startsWith(poolName + '_')) {
                return true;
            }
        }
        return false;
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

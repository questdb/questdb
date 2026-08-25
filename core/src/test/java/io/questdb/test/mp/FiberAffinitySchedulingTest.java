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
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWakeSink;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class FiberAffinitySchedulingTest {
    private static final long AWAIT_SECONDS = 10;

    @Test
    public void testExternalPublicationWakesLongParkedWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-external-wake", 1));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final CountDownLatch ran = new CountDownLatch(1);
            final AtomicInteger mountedWorkerId = new AtomicInteger(FiberRuntime.NO_WORKER);
            pool.start();
            try {
                awaitReadyCount(pool, 1);
                final long start = System.nanoTime();
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                    @Override
                    protected boolean runStep() {
                        mountedWorkerId.set(Worker.current().getWorkerId());
                        ran.countDown();
                        return true;
                    }
                }));
                Assert.assertTrue(ran.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                Assert.assertTrue(
                        "external publication waited for the configured 60-second park timeout",
                        System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5)
                );
                Assert.assertEquals(0, mountedWorkerId.get());
                awaitOutstanding(runtime, 0);
                Assert.assertEquals(1, runtime.getWakeClaimCount());
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testExternalPublicationWakesOverflowSizedPark() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration(
                    "fiber-overflow-park-wake",
                    1,
                    64,
                    false,
                    Long.MAX_VALUE
            ));
            final CountDownLatch ran = new CountDownLatch(1);
            pool.start();
            try {
                // If millisecond-to-nanosecond conversion wraps, the Worker never registers as
                // park-ready and this assertion fails instead of accepting a hot idle loop.
                awaitReadyCount(pool, 1);
                Assert.assertEquals(LaunchResult.LAUNCHED, pool.getFiberRuntime().launch(new FiberTask() {
                    @Override
                    protected boolean runStep() {
                        ran.countDown();
                        return true;
                    }
                }));
                Assert.assertTrue(ran.await(AWAIT_SECONDS, TimeUnit.SECONDS));
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testParkInterruptIsPreservedWithoutBusySpin() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-park-interrupt", 1));
            final CountDownLatch interruptObserved = new CountDownLatch(1);
            final AtomicInteger jobRunCount = new AtomicInteger();
            final AtomicReference<Thread> workerThread = new AtomicReference<>();
            pool.assign(workerContext -> {
                workerThread.compareAndSet(null, Thread.currentThread());
                jobRunCount.incrementAndGet();
                if (Thread.currentThread().isInterrupted()) {
                    interruptObserved.countDown();
                }
                return false;
            });
            pool.start();
            try {
                awaitReadyCount(pool, 1);
                Objects.requireNonNull(workerThread.get()).interrupt();
                Assert.assertTrue(interruptObserved.await(AWAIT_SECONDS, TimeUnit.SECONDS));

                // The helper restores the status for user code, then consumes/remembers it on the
                // next timed park. Leaving it set must not turn the idle Worker into a hot loop.
                // An interrupt and a stale LockSupport permit may each cause one immediate return,
                // so let those bounded returns settle before sampling the steady idle state.
                Thread.sleep(20L);
                awaitReadyCount(pool, 1);
                final int countAfterRepark = jobRunCount.get();
                Thread.sleep(50L);
                final int extraRuns = jobRunCount.get() - countAfterRepark;
                Assert.assertTrue("interrupted idle Worker remained hot [extraRuns=" + extraRuns + ']', extraRuns <= 2);
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testExternalResumePrefersLastMountWorkerAndReservationResetsHint() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-last-mounter-wake", 2));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final CountDownLatch directLaunchReturned = new CountDownLatch(1);
            final CountDownLatch resumed = new CountDownLatch(1);
            final CountDownLatch workerZeroObservedWake = new CountDownLatch(1);
            final CountDownLatch workerOneObservedWake = new CountDownLatch(1);
            final AtomicBoolean isDirectLaunchPending = new AtomicBoolean(true);
            final AtomicBoolean isWakeObservationArmed = new AtomicBoolean();
            final AtomicInteger firstMountWorkerId = new AtomicInteger(FiberRuntime.NO_WORKER);
            final AtomicInteger resumedMountWorkerId = new AtomicInteger(FiberRuntime.NO_WORKER);
            final AtomicReference<Fiber> mountedFiber = new AtomicReference<>();
            final AtomicReference<Throwable> jobError = new AtomicReference<>();
            final FiberTask task = new FiberTask() {
                @Override
                protected boolean runStep() {
                    final Fiber fiber = Objects.requireNonNull(Fiber.current());
                    mountedFiber.set(fiber);
                    firstMountWorkerId.set(Worker.current().getWorkerId());
                    Assert.assertEquals(firstMountWorkerId.get(), fiber.getLastMountWorkerIdForTesting());
                    final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
                    final long token = fiber.beginWaitBuild(1);
                    final FiberWalWaitRegistration registration = coordinator.acquireWal(token, 1);
                    try {
                        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register(waitQueue));
                        Assert.assertEquals(FiberWaitCoordinator.REASON_WAL, fiber.suspendWait(token));
                        resumedMountWorkerId.set(Worker.current().getWorkerId());
                        resumed.countDown();
                        return true;
                    } finally {
                        registration.cancel();
                        coordinator.abort(token);
                        coordinator.consume(token);
                    }
                }
            };
            pool.assign(0, workerContext -> {
                if (isDirectLaunchPending.compareAndSet(true, false)) {
                    final Fiber fiber = runtime.tryReserveFiber();
                    if (fiber == null) {
                        jobError.compareAndSet(null, new AssertionError("could not reserve direct Fiber"));
                    } else {
                        try {
                            Assert.assertEquals(
                                    LaunchResult.LAUNCHED,
                                    runtime.launchReservedDirect(
                                            fiber,
                                            fiber.getReservationEpoch(),
                                            task,
                                            task.getIncarnation()
                                    )
                            );
                        } catch (Throwable th) {
                            jobError.compareAndSet(null, th);
                        }
                    }
                    directLaunchReturned.countDown();
                }
                if (isWakeObservationArmed.get()) {
                    workerZeroObservedWake.countDown();
                }
                return false;
            });
            pool.assign(1, workerContext -> {
                if (isWakeObservationArmed.get()) {
                    workerOneObservedWake.countDown();
                }
                return false;
            });
            pool.start();
            Fiber recycled = null;
            try {
                Assert.assertTrue(directLaunchReturned.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                if (jobError.get() != null) {
                    throw new AssertionError(jobError.get());
                }
                awaitWaitQueueSize(waitQueue, 1);
                awaitReadyCount(pool, 2);
                Assert.assertEquals(0, firstMountWorkerId.get());
                Assert.assertTrue(pool.isWorkerReadyForTesting(0));
                Assert.assertTrue(pool.isWorkerReadyForTesting(1));

                isWakeObservationArmed.set(true);
                waitQueue.fire(1, false);
                Assert.assertTrue(workerZeroObservedWake.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                Assert.assertTrue(
                        "generic peer was woken instead of the last mounter",
                        pool.isWorkerReadyForTesting(1)
                );
                Assert.assertEquals(1, workerOneObservedWake.getCount());
                Assert.assertTrue(resumed.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                Assert.assertEquals(0, resumedMountWorkerId.get());
                awaitOutstanding(runtime, 0);
                Assert.assertEquals(1, runtime.getWakeClaimCount());

                recycled = runtime.tryReserveFiber();
                Assert.assertSame(mountedFiber.get(), recycled);
                Assert.assertEquals(FiberRuntime.NO_WORKER, recycled.getLastMountWorkerIdForTesting());
            } finally {
                if (recycled != null) {
                    runtime.releaseReservedFiber(recycled, recycled.getReservationEpoch());
                }
                pool.halt();
            }
        });
    }

    @Test
    public void testPostCommitWakeFailureDoesNotRollBackPublication() {
        final AtomicInteger wakeAttempts = new AtomicInteger();
        final FiberRuntime runtime = new FiberRuntime(1, 1, 1, 0, new FiberWakeSink() {
            @Override
            public void wakeAll() {
                throw new AssertionError("injected wake-all failure");
            }

            @Override
            public boolean wakeOne(int preferredWorkerId) {
                wakeAttempts.incrementAndGet();
                throw new AssertionError("injected wake failure");
            }
        });
        final AtomicBoolean hasRun = new AtomicBoolean();
        Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
            @Override
            protected boolean runStep() {
                hasRun.set(true);
                return true;
            }
        }));
        Assert.assertEquals(1, wakeAttempts.get());
        Assert.assertEquals(1, runtime.getQueuedCount());
        Assert.assertEquals(1, runtime.drain(1));
        Assert.assertTrue(hasRun.get());
        Assert.assertEquals(0, runtime.getOutstandingTaskCount());
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    @Test
    public void testUnboundedHaltDrainsOwnerLocalQueueWithDetachedContext() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-detached-halt", 1));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final CountDownLatch localCommitted = new CountDownLatch(1);
            final CountDownLatch releaseOwnerJob = new CountDownLatch(1);
            final CountDownLatch detachedTaskAbandoned = new CountDownLatch(1);
            final AtomicBoolean isLaunchPending = new AtomicBoolean(true);
            final AtomicReference<Fiber> queuedFiber = new AtomicReference<>();
            final AtomicReference<Throwable> haltFailure = new AtomicReference<>();
            pool.assign(workerContext -> {
                if (isLaunchPending.compareAndSet(true, false)) {
                    final Fiber fiber = runtime.tryReserveFiber();
                    Assert.assertNotNull(fiber);
                    queuedFiber.set(fiber);
                    final FiberTask task = new FiberTask() {
                        @Override
                        protected void onAbandoned() {
                            Assert.assertNull(Worker.current());
                            detachedTaskAbandoned.countDown();
                        }

                        @Override
                        protected boolean runStep() {
                            throw new AssertionError("quiescing Fiber should have been abandoned");
                        }
                    };
                    Assert.assertEquals(
                            LaunchResult.LAUNCHED,
                            runtime.launchReserved(
                                    fiber,
                                    fiber.getReservationEpoch(),
                                    task,
                                    task.getIncarnation()
                            )
                    );
                    localCommitted.countDown();
                    try {
                        Assert.assertTrue(releaseOwnerJob.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
                return false;
            });
            pool.start();
            final Thread halter = new Thread(() -> {
                try {
                    pool.halt();
                } catch (Throwable th) {
                    haltFailure.compareAndSet(null, th);
                }
            }, "fiber-detached-halter");
            try {
                Assert.assertTrue(localCommitted.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                Assert.assertEquals(1, runtime.getLocalQueueDepthForTesting(0));
                halter.start();
                Assert.assertTrue(detachedTaskAbandoned.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                Assert.assertEquals(
                        FiberRuntime.NO_WORKER,
                        queuedFiber.get().getLastMountWorkerIdForTesting()
                );
            } finally {
                releaseOwnerJob.countDown();
                if (halter.getState() != Thread.State.NEW) {
                    halter.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
                    Assert.assertFalse("halter did not stop", halter.isAlive());
                } else {
                    pool.halt();
                }
            }
            if (haltFailure.get() != null) {
                throw new AssertionError(haltFailure.get());
            }
            Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
        });
    }

    @Test
    public void testSingleSameRuntimePublicationDoesNotWakeParkedPeer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-same-runtime-no-wake", 2));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final CountDownLatch publicationCommitted = new CountDownLatch(1);
            final CountDownLatch releasePublisher = new CountDownLatch(1);
            final CountDownLatch taskRan = new CountDownLatch(1);
            final AtomicBoolean isLaunched = new AtomicBoolean();
            final AtomicInteger mountedWorkerId = new AtomicInteger(FiberRuntime.NO_WORKER);
            final AtomicReference<Throwable> jobError = new AtomicReference<>();
            pool.assign(0, workerContext -> {
                if (isLaunched.compareAndSet(false, true)) {
                    try {
                        awaitWorkerReady(pool, 1);
                        Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                            @Override
                            protected boolean runStep() {
                                mountedWorkerId.set(Worker.current().getWorkerId());
                                taskRan.countDown();
                                return true;
                            }
                        }));
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
                if (jobError.get() != null) {
                    throw new AssertionError(jobError.get());
                }
                Assert.assertEquals(1, runtime.getQueuedCount());
                Assert.assertEquals(0, runtime.getWakeClaimCount());
                Assert.assertTrue("same-runtime publication woke the parked peer",
                        pool.isWorkerReadyForTesting(1));
                Assert.assertEquals(1, taskRan.getCount());

                releasePublisher.countDown();
                Assert.assertTrue(taskRan.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                Assert.assertEquals(0, mountedWorkerId.get());
                awaitOutstanding(runtime, 0);
            } finally {
                releasePublisher.countDown();
                pool.halt();
            }
        });
    }

    @Test
    public void testSameRuntimeOverflowWakesParkedPeer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-same-runtime-overflow-wake", 8));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final int taskCount = runtime.getLocalQueueCapacityForTesting(0) + 1;
            final CountDownLatch allTasksRan = new CountDownLatch(taskCount);
            final CountDownLatch publicationCommitted = new CountDownLatch(1);
            final CountDownLatch releasePublisher = new CountDownLatch(1);
            final AtomicBoolean isLaunched = new AtomicBoolean();
            final AtomicInteger firstMountedWorkerId = new AtomicInteger(FiberRuntime.NO_WORKER);
            final AtomicReference<Throwable> jobError = new AtomicReference<>();
            pool.assign(0, workerContext -> {
                if (isLaunched.compareAndSet(false, true)) {
                    try {
                        for (int workerId = 1; workerId < pool.getWorkerCount(); workerId++) {
                            awaitWorkerReady(pool, workerId);
                        }
                        for (int i = 0; i < taskCount; i++) {
                            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                                @Override
                                protected boolean runStep() {
                                    firstMountedWorkerId.compareAndSet(
                                            FiberRuntime.NO_WORKER,
                                            Worker.current().getWorkerId()
                                    );
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
                if (jobError.get() != null) {
                    throw new AssertionError(jobError.get());
                }
                Assert.assertEquals(taskCount - 1L, runtime.getLocalPublicationCount());
                Assert.assertEquals(1, runtime.getLocalFallbackPublicationCount());
                Assert.assertEquals(1, runtime.getWakeClaimCount());
                Assert.assertTrue(allTasksRan.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                Assert.assertNotEquals(0, firstMountedWorkerId.get());
                awaitOutstanding(runtime, 0);
            } finally {
                releasePublisher.countDown();
                pool.halt();
            }
        });
    }

    @Test
    public void testSingleWorkerProcessingResignalStaysOwnerLocalWithoutWake() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-single-worker-resignal", 1));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final CountDownLatch done = new CountDownLatch(1);
            final AtomicBoolean isLaunched = new AtomicBoolean();
            final AtomicInteger firstMountWorkerId = new AtomicInteger(FiberRuntime.NO_WORKER);
            final AtomicInteger resumedMountWorkerId = new AtomicInteger(FiberRuntime.NO_WORKER);
            final AtomicInteger runCount = new AtomicInteger();
            final AtomicReference<LaunchResult> resignalResult = new AtomicReference<>();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final FiberTask task = new FiberTask() {
                @Override
                protected void onDone() {
                    done.countDown();
                }

                @Override
                protected void onError(Throwable th) {
                    error.compareAndSet(null, th);
                    done.countDown();
                }

                @Override
                protected void onParked() {
                    resignalResult.set(runtime.launch(this));
                }

                @Override
                protected boolean runStep() {
                    final int workerId = Worker.current().getWorkerId();
                    final Fiber fiber = Objects.requireNonNull(Fiber.current());
                    if (fiber.getLastMountWorkerIdForTesting() != workerId) {
                        error.compareAndSet(
                                null,
                                new AssertionError("last mounter was not recorded before continuation code")
                        );
                    }
                    if (runCount.incrementAndGet() == 1) {
                        firstMountWorkerId.set(workerId);
                        return false;
                    }
                    resumedMountWorkerId.set(workerId);
                    return true;
                }
            };
            pool.assign(0, workerContext -> {
                if (isLaunched.compareAndSet(false, true)) {
                    try {
                        Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                        done.countDown();
                    }
                }
                return false;
            });
            pool.start();
            try {
                Assert.assertTrue(done.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                if (error.get() != null) {
                    throw new AssertionError(error.get());
                }
                awaitOutstanding(runtime, 0);
                Assert.assertEquals(2, runCount.get());
                Assert.assertSame(LaunchResult.ALREADY_OWNED, resignalResult.get());
                Assert.assertEquals(0, firstMountWorkerId.get());
                Assert.assertEquals(0, resumedMountWorkerId.get());
                Assert.assertEquals(2, runtime.getLocalPublicationCount());
                Assert.assertEquals(2, runtime.getLocalSelectionCount());
                Assert.assertEquals(0, runtime.getGlobalPublicationCount());
                Assert.assertEquals(0, runtime.getGlobalSelectionCount());
                Assert.assertEquals(0, runtime.getWakeClaimCount());
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testPeriodicGlobalProbeServicesExternalPublicationUnderContinuousLocalLoad() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-global-probe", 1));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final CountDownLatch externalRan = new CountDownLatch(1);
            final CountDownLatch localDone = new CountDownLatch(1);
            final AtomicBoolean isLocalLaunchPending = new AtomicBoolean(true);
            final AtomicBoolean stopLocal = new AtomicBoolean();
            final AtomicInteger localMountsAfterExternalPublication = new AtomicInteger();
            final AtomicInteger localMountCount = new AtomicInteger();
            final AtomicLong externalObservedAt = new AtomicLong(-1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final FiberTask localTask = new FiberTask() {
                @Override
                protected void onDone() {
                    localDone.countDown();
                }

                @Override
                protected void onError(Throwable th) {
                    error.compareAndSet(null, th);
                    localDone.countDown();
                }

                @Override
                protected void onParked() {
                    runtime.launch(this);
                }

                @Override
                protected boolean runStep() {
                    localMountCount.incrementAndGet();
                    if (runtime.getGlobalPublicationCount() != 0) {
                        localMountsAfterExternalPublication.incrementAndGet();
                    }
                    return stopLocal.get();
                }
            };
            pool.assign(0, workerContext -> {
                if (isLocalLaunchPending.compareAndSet(true, false)) {
                    try {
                        Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(localTask));
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                        localDone.countDown();
                    }
                }
                return false;
            });
            pool.start();
            try {
                awaitAtLeast(localMountCount, FiberRuntime.getGlobalProbeIntervalForTesting() * 2);
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                    @Override
                    protected boolean runStep() {
                        externalObservedAt.set(localMountsAfterExternalPublication.get());
                        externalRan.countDown();
                        return true;
                    }
                }));
                Assert.assertTrue(externalRan.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                Assert.assertTrue(
                        "periodic global probe exceeded its selection bound [localMounts="
                                + externalObservedAt.get() + ']',
                        externalObservedAt.get() <= FiberRuntime.getGlobalProbeIntervalForTesting() + 1L
                );
                Assert.assertEquals(1, runtime.getGlobalPublicationCount());
                Assert.assertEquals(1, runtime.getGlobalSelectionCount());
            } finally {
                stopLocal.set(true);
                Assert.assertTrue(localDone.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                pool.halt();
            }
            if (error.get() != null) {
                throw new AssertionError(error.get());
            }
        });
    }

    @Test
    public void testWorkerExitAdvertisesAndRecoversOwnerLocalWork() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration(
                    "fiber-orphan-recovery",
                    2,
                    64,
                    true
            ));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final CountDownLatch recovered = new CountDownLatch(1);
            final AtomicBoolean isFailurePending = new AtomicBoolean(true);
            final AtomicInteger mountedWorkerId = new AtomicInteger(FiberRuntime.NO_WORKER);
            pool.assign(0, workerContext -> {
                if (isFailurePending.compareAndSet(true, false)) {
                    awaitWorkerReady(pool, 1);
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                        @Override
                        protected boolean runStep() {
                            mountedWorkerId.set(Worker.current().getWorkerId());
                            recovered.countDown();
                            return true;
                        }
                    }));
                    throw new RuntimeException("deterministic owner failure");
                }
                return false;
            });
            pool.start();
            try {
                Assert.assertTrue(recovered.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                Assert.assertEquals(1, mountedWorkerId.get());
                Assert.assertEquals(1, runtime.getOrphanedShardTransitionCount());
                Assert.assertEquals(1, runtime.getOrphanedEntryRecoveryCount());
                Assert.assertEquals(1, runtime.getStolenSelectionCount());
                Assert.assertEquals(1, runtime.getWakeClaimCount());
                awaitOutstanding(runtime, 0);
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testWorkerExitTransfersWakeResponsibilityForVisibleGlobalWork() {
        final AtomicInteger wakeAttempts = new AtomicInteger();
        final FiberRuntime runtime = new FiberRuntime(1, 1, 64, 1, new FiberWakeSink() {
            @Override
            public void wakeAll() {
            }

            @Override
            public boolean wakeOne(int preferredWorkerId) {
                wakeAttempts.incrementAndGet();
                return true;
            }
        });
        final FiberRuntime.OwnerContext ownerContext = runtime.getOwnerContext(0);
        runtime.activateOwner(ownerContext);
        Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
            @Override
            protected boolean runStep() {
                return true;
            }
        }));
        Assert.assertEquals(1, wakeAttempts.get());
        Assert.assertEquals(1, runtime.getQueuedCount());

        // The first wake models a global commit whose claim selected the exiting owner.
        // onOwnerExit() must re-signal still-visible global work so another ready Worker can take
        // responsibility after producer revocation.
        runtime.onOwnerExit(ownerContext);
        Assert.assertEquals(2, wakeAttempts.get());
        Assert.assertEquals(2, runtime.getWakeClaimCount());

        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    @Test
    public void testPreferredWakeClaimsExactReadyWorkerBeforeFallback() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-preferred-wake", 2));
            final CountDownLatch workerZeroBlocked = new CountDownLatch(1);
            final CountDownLatch releaseWorkerZero = new CountDownLatch(1);
            final AtomicBoolean isBlockArmed = new AtomicBoolean();
            final AtomicReference<Throwable> jobError = new AtomicReference<>();
            pool.assign(0, workerContext -> {
                if (isBlockArmed.compareAndSet(true, false)) {
                    workerZeroBlocked.countDown();
                    try {
                        if (!releaseWorkerZero.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting to release preferred Worker");
                        }
                    } catch (Throwable th) {
                        jobError.compareAndSet(null, th);
                    }
                }
                return false;
            });
            pool.start();
            try {
                awaitReadyCount(pool, 2);
                Assert.assertTrue(pool.isWorkerReadyForTesting(0));
                Assert.assertTrue(pool.isWorkerReadyForTesting(1));
                isBlockArmed.set(true);
                Assert.assertTrue(pool.wakeOneForTesting(0));
                Assert.assertTrue(workerZeroBlocked.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                Assert.assertFalse(pool.isWorkerReadyForTesting(0));
                Assert.assertTrue(pool.isWorkerReadyForTesting(1));
            } finally {
                releaseWorkerZero.countDown();
                pool.halt();
            }
            if (jobError.get() != null) {
                throw new AssertionError(jobError.get());
            }
        });
    }

    @Test
    public void testOpenOwnerfulRuntimeRejectsDetachedDrain() throws Exception {
        final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-open-detached-drain", 1));
        pool.start();
        try {
            awaitReadyCount(pool, 1);
            Assert.assertThrows(IllegalStateException.class, () -> pool.getFiberRuntime().drain(1));
            Assert.assertEquals(FiberRuntimeState.OPEN, pool.getFiberRuntime().state());
        } finally {
            pool.halt();
        }
    }

    @Test
    public void testOpenUnstartedOwnerfulRuntimeAllowsDetachedDrain() {
        final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-open-unstarted-drain", 1));
        final FiberRuntime runtime = pool.getFiberRuntime();
        final AtomicBoolean isTaskRun = new AtomicBoolean();
        try {
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                @Override
                protected boolean runStep() {
                    isTaskRun.set(true);
                    return true;
                }
            }));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(isTaskRun.get());
            Assert.assertEquals(FiberRuntimeState.OPEN, runtime.state());
        } finally {
            pool.halt();
        }
    }

    @Test
    public void testQuiescingOwnerfulRuntimeRejectsDetachedDrainFromForeignWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool targetPool = new WorkerPool(fiberConfiguration("fiber-foreign-drain-target", 1));
            final WorkerPool foreignPool = new WorkerPool(legacyConfiguration("fiber-foreign-drain-caller"));
            final FiberRuntime runtime = targetPool.getFiberRuntime();
            final CountDownLatch rejected = new CountDownLatch(1);
            final AtomicBoolean isAttempted = new AtomicBoolean();
            final AtomicReference<Throwable> unexpected = new AtomicReference<>();
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                @Override
                protected boolean runStep() {
                    throw new AssertionError("foreign Worker must not mount the target Fiber");
                }
            }));
            runtime.beginQuiesce();
            foreignPool.assign(workerContext -> {
                if (isAttempted.compareAndSet(false, true)) {
                    try {
                        runtime.drain(1);
                        unexpected.set(new AssertionError("foreign Worker detached drain was accepted"));
                    } catch (IllegalStateException expected) {
                        rejected.countDown();
                    } catch (Throwable th) {
                        unexpected.set(th);
                    }
                }
                return false;
            });
            foreignPool.start();
            try {
                Assert.assertTrue(rejected.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                if (unexpected.get() != null) {
                    throw new AssertionError(unexpected.get());
                }
                Assert.assertEquals(FiberRuntimeState.QUIESCING, runtime.state());
                Assert.assertEquals(1, runtime.getQueuedCount());
            } finally {
                foreignPool.halt();
                targetPool.halt();
            }
        });
    }

    @Test
    public void testCompletedFiberHostHaltIsIdempotentFromForeignWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool targetPool = new WorkerPool(fiberConfiguration("fiber-completed-halt-target", 1));
            final WorkerPool foreignPool = new WorkerPool(legacyConfiguration("fiber-completed-halt-caller"));
            final CountDownLatch completed = new CountDownLatch(1);
            final AtomicBoolean isAttempted = new AtomicBoolean();
            final AtomicReference<Throwable> unexpected = new AtomicReference<>();
            targetPool.halt();
            Assert.assertEquals(FiberRuntimeState.CLOSED, targetPool.getFiberRuntime().state());
            foreignPool.assign(workerContext -> {
                if (isAttempted.compareAndSet(false, true)) {
                    try {
                        targetPool.halt();
                        if (!targetPool.haltWithin(TimeUnit.MILLISECONDS.toNanos(1))) {
                            unexpected.set(new AssertionError("completed Fiber-host halt was not idempotent"));
                        }
                    } catch (Throwable th) {
                        unexpected.set(th);
                    } finally {
                        completed.countDown();
                    }
                }
                return false;
            });
            foreignPool.start();
            try {
                Assert.assertTrue(completed.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                if (unexpected.get() != null) {
                    throw new AssertionError(unexpected.get());
                }
            } finally {
                foreignPool.halt();
                targetPool.halt();
            }
        });
    }

    @Test
    public void testTerminalHaltFromWorkerIsRejectedWithoutMutation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-worker-halt-preflight", 1));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final CountDownLatch rejected = new CountDownLatch(1);
            final CountDownLatch taskRan = new CountDownLatch(1);
            final AtomicBoolean isAttempted = new AtomicBoolean();
            final AtomicReference<Throwable> unexpected = new AtomicReference<>();
            pool.assign(workerContext -> {
                if (isAttempted.compareAndSet(false, true)) {
                    try {
                        pool.halt();
                        unexpected.set(new AssertionError("Worker terminal halt was accepted"));
                    } catch (IllegalStateException expected) {
                        try {
                            pool.haltWithin(TimeUnit.MILLISECONDS.toNanos(1));
                            unexpected.set(new AssertionError("bounded Worker terminal halt was accepted"));
                        } catch (IllegalStateException boundedExpected) {
                            rejected.countDown();
                        }
                    } catch (Throwable th) {
                        unexpected.set(th);
                    }
                }
                return false;
            });
            pool.start();
            try {
                Assert.assertTrue(rejected.await(AWAIT_SECONDS, TimeUnit.SECONDS));
                if (unexpected.get() != null) {
                    throw new AssertionError(unexpected.get());
                }
                Assert.assertEquals(FiberRuntimeState.OPEN, runtime.state());
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                    @Override
                    protected boolean runStep() {
                        taskRan.countDown();
                        return true;
                    }
                }));
                Assert.assertTrue(taskRan.await(AWAIT_SECONDS, TimeUnit.SECONDS));
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testZeroOwnerMountedFiberCannotTerminallyHaltPool() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-zero-owner-halt-preflight", 0));
            final FiberRuntime runtime = pool.getFiberRuntime();
            final AtomicBoolean isResourceClosed = new AtomicBoolean();
            final AtomicReference<Throwable> haltFailure = new AtomicReference<>();
            pool.freeResourceOnExit(() -> isResourceClosed.set(true));
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                @Override
                protected boolean runStep() {
                    try {
                        pool.halt();
                        haltFailure.set(new AssertionError("mounted Fiber terminal halt was accepted"));
                    } catch (IllegalStateException expected) {
                        haltFailure.set(expected);
                    }
                    return true;
                }
            }));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(haltFailure.get() instanceof IllegalStateException);
            Assert.assertEquals(FiberRuntimeState.OPEN, runtime.state());
            Assert.assertFalse(isResourceClosed.get());
            pool.halt();
            Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
            Assert.assertTrue(isResourceClosed.get());
        });
    }

    @Test
    public void testTerminalHaltRejectsRoleSwitchLockBeforeLifecycleMutation() {
        final WorkerPool pool = new WorkerPool(fiberConfiguration("fiber-role-halt-preflight", 1));
        final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
        SuspensionScope.enterRoleSwitchWriteLock(scope);
        try {
            Assert.assertThrows(IllegalStateException.class, pool::halt);
            Assert.assertEquals(FiberRuntimeState.OPEN, pool.getFiberRuntime().state());
        } finally {
            SuspensionScope.leaveRoleSwitchWriteLock(scope);
            pool.halt();
        }
    }

    private static void awaitOutstanding(FiberRuntime runtime, int expected) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        while (runtime.getOutstandingTaskCount() != expected && System.nanoTime() < deadline) {
            Os.pause();
        }
        Assert.assertEquals(expected, runtime.getOutstandingTaskCount());
    }

    private static void awaitAtLeast(AtomicInteger value, int expected) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        while (value.get() < expected && System.nanoTime() < deadline) {
            Os.pause();
        }
        Assert.assertTrue("value did not reach target [value=" + value.get() + ", target=" + expected + ']',
                value.get() >= expected);
    }

    private static void awaitReadyCount(WorkerPool pool, int expected) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        while (!areWorkersReady(pool, expected) && System.nanoTime() < deadline) {
            Os.pause();
        }
        Assert.assertEquals(expected, pool.getReadyWorkerCountForTesting());
        for (int workerId = 0; workerId < expected; workerId++) {
            Assert.assertTrue(pool.isWorkerReadyForTesting(workerId));
        }
    }

    private static void awaitWaitQueueSize(FiberWalWaitQueue queue, int expected) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        while (queue.size() != expected && System.nanoTime() < deadline) {
            Os.pause();
        }
        Assert.assertEquals(expected, queue.size());
    }

    private static void awaitWorkerReady(WorkerPool pool, int workerId) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        while (!pool.isWorkerReadyForTesting(workerId) && System.nanoTime() < deadline) {
            Os.pause();
        }
        Assert.assertTrue(pool.isWorkerReadyForTesting(workerId));
    }

    private static WorkerPoolConfiguration fiberConfiguration(String poolName, int workerCount) {
        return fiberConfiguration(poolName, workerCount, 64, false);
    }

    private static WorkerPoolConfiguration fiberConfiguration(
            String poolName,
            int workerCount,
            int mountBudget,
            boolean haltOnError
    ) {
        return fiberConfiguration(
                poolName,
                workerCount,
                mountBudget,
                haltOnError,
                TimeUnit.SECONDS.toMillis(60)
        );
    }

    private static WorkerPoolConfiguration fiberConfiguration(
            String poolName,
            int workerCount,
            int mountBudget,
            boolean haltOnError,
            long sleepTimeoutMillis
    ) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                return 64;
            }

            @Override
            public int getFiberMountBudget() {
                return mountBudget;
            }

            @Override
            public int getFiberRetainedCount() {
                return 16;
            }

            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public long getNapThreshold() {
                return 2;
            }

            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public long getSleepThreshold() {
                // Wake tests must observe the wakeable long-park path, never the preceding
                // one-millisecond nap which may expire without a publication claim.
                return 0;
            }

            @Override
            public long getSleepTimeout() {
                return sleepTimeoutMillis;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean haltOnError() {
                return haltOnError;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.FIBER_HOST;
            }

            @Override
            public long getYieldThreshold() {
                return 1;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        };
    }

    private static boolean areWorkersReady(WorkerPool pool, int expected) {
        if (pool.getReadyWorkerCountForTesting() != expected) {
            return false;
        }
        for (int workerId = 0; workerId < expected; workerId++) {
            if (!pool.isWorkerReadyForTesting(workerId)) {
                return false;
            }
        }
        return true;
    }

    private static WorkerPoolConfiguration legacyConfiguration(String poolName) {
        return new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        };
    }
}

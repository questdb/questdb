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

import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeConfigurationListener;
import io.questdb.mp.continuation.FiberRuntimeQuiesceListener;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import jdk.internal.vm.Continuation;
import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FiberRuntimeTest {
    private static final long EXCEEDED_DRAIN_TIME_BUDGET_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    @Test
    public void testAwaitCapacityReportsAlreadyCancelledSignal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
            final long generation = cancellationSignal.getGeneration();
            cancellationSignal.cancel();
            final FiberRuntime runtime = new FiberRuntime(2);
            final CancelledCapacityTask task = new CancelledCapacityTask(runtime, cancellationSignal, generation);
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(task.isDone());
                // capacity is available, but the pre-cancelled signal must win the early return
                Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, task.reason);
            }
        });
    }

    @Test
    public void testCancellationGenerationRemainsBoundAcrossResume() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
            final long taskGeneration = cancellationSignal.getGeneration();
            final CancellationGenerationTask task = new CancellationGenerationTask(
                    cancellationSignal,
                    taskGeneration
            );
            final FiberRuntime runtime = new FiberRuntime(1);
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                try {
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertFalse(task.isDone());

                    final long nextGeneration = cancellationSignal.reopen();
                    Assert.assertNotEquals(taskGeneration, nextGeneration);
                    Assert.assertEquals(1, runtime.drain(1));

                    Assert.assertTrue(task.isDone());
                    Assert.assertEquals(taskGeneration, task.resumedGeneration);
                    Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, task.secondWaitReason);
                } finally {
                    cancellationSignal.cancel();
                }
            }
        });
    }

    @Test
    public void testCancellationGenerationRemainsBoundAcrossTaskPark() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
            final long taskGeneration = cancellationSignal.getGeneration();
            final CancellationGenerationParkTask task = new CancellationGenerationParkTask(cancellationSignal);
            final FiberRuntime runtime = new FiberRuntime(1);
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertFalse(task.isDone());

                Assert.assertNotEquals(taskGeneration, cancellationSignal.reopen());
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertTrue(task.isDone());
                Assert.assertEquals(taskGeneration, task.resumedGeneration);
            }
        });
    }

    @Test
    public void testCancellationScratchRemainsFiberConfinedAcrossSuspend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final AtomicBoolean expectedFlag = new AtomicBoolean();
            final AtomicBoolean replacementFlag = new AtomicBoolean();
            final ScratchWaitTask waitTask = new ScratchWaitTask(waitQueue, expectedFlag);
            final ScratchMutationTask mutationTask = new ScratchMutationTask(replacementFlag);
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(waitTask));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertFalse(waitTask.isDone());

                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(mutationTask));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(mutationTask.isDone());

                waitQueue.fire(1, false);
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(waitTask.isDone());
                Assert.assertSame(expectedFlag, waitTask.resumedFlag);
            }
        });
    }

    @Test
    public void testCancellationWinsDuringParkPublication() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            final CancelOnParkTask task = new CancelOnParkTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(8));
            Assert.assertTrue(task.isCancelled());
            Assert.assertEquals(12, task.callbackOrder);
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getParkedFiberCount());

            close(runtime);
        });
    }

    @Test
    public void testCapacityWaitsForRetiringFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1, 2);
            final OneShotTask firstTask = new OneShotTask();
            final OneShotTask secondTask = new OneShotTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(firstTask));
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(secondTask));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getQueuedCount());

            final Fiber fiber = runtime.tryReserveFiber();
            Assert.assertNotNull(fiber);
            final CapacityWaitTask capacityWaitTask = new CapacityWaitTask(runtime);
            Assert.assertEquals(
                    LaunchResult.LAUNCHED,
                    runtime.launchReservedDirect(
                            fiber,
                            fiber.getReservationEpoch(),
                            capacityWaitTask,
                            capacityWaitTask.getIncarnation()
                    )
            );
            Assert.assertFalse(capacityWaitTask.isDone());
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(capacityWaitTask.isDone());
            Assert.assertTrue(capacityWaitTask.hasReservedAfterWake);
            Assert.assertEquals(0, runtime.getParkedFiberCount());

            close(runtime);
        });
    }

    @Test
    public void testConcurrentLaunchAndDrainReusesOutcomeSafely() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int cycleCount = 50_000;
            final int threadCount = 4;
            final FiberRuntime runtime = new FiberRuntime(threadCount);
            final CountDownLatch done = new CountDownLatch(threadCount);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch start = new CountDownLatch(1);
            final ObjList<Thread> threads = new ObjList<>(threadCount);
            for (int i = 0; i < threadCount; i++) {
                final Thread thread = new Thread(() -> {
                    try {
                        runtime.initializeCarrier();
                        start.await();
                        final OneShotTask task = new OneShotTask();
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                        for (int cycle = 0; cycle < cycleCount; cycle++) {
                            runConcurrentTask(runtime, task, deadline);
                        }
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                    } finally {
                        done.countDown();
                    }
                });
                threads.add(thread);
                thread.start();
            }

            start.countDown();
            try {
                Assert.assertTrue(done.await(30, TimeUnit.SECONDS));
            } finally {
                for (int i = 0; i < threadCount; i++) {
                    threads.getQuick(i).join(30_000);
                }
            }
            Assert.assertNull(error.get());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
            close(runtime);
        });
    }

    @Test
    public void testConcurrentReservationsRespectMaxLiveLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int maxLive = 4;
            final int threadCount = 16;
            final FiberRuntime runtime = new FiberRuntime(maxLive);
            final AtomicInteger reservationCount = new AtomicInteger();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch attempted = new CountDownLatch(threadCount);
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch start = new CountDownLatch(1);
            final ObjList<Thread> threads = new ObjList<>(threadCount);
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                try {
                    for (int i = 0; i < threadCount; i++) {
                        final Thread thread = new Thread(() -> {
                            Fiber fiber = null;
                            long reservationEpoch = 0;
                            try {
                                start.await();
                                fiber = runtime.tryReserveFiber();
                                if (fiber != null) {
                                    reservationEpoch = fiber.getReservationEpoch();
                                    reservationCount.incrementAndGet();
                                }
                                attempted.countDown();
                                release.await();
                            } catch (Throwable th) {
                                error.compareAndSet(null, th);
                            } finally {
                                if (fiber != null) {
                                    runtime.releaseReservedFiber(fiber, reservationEpoch);
                                }
                            }
                        });
                        threads.add(thread);
                        thread.start();
                    }
                    start.countDown();
                    Assert.assertTrue(attempted.await(5, TimeUnit.SECONDS));
                    Assert.assertNull(error.get());
                    Assert.assertEquals(maxLive, reservationCount.get());
                    Assert.assertEquals(maxLive, runtime.getCreatedFiberCount());
                    Assert.assertEquals(maxLive, runtime.getLiveFiberCount());
                    Assert.assertEquals(maxLive, runtime.getOutstandingTaskCount());
                    Assert.assertEquals(threadCount - maxLive, runtime.getSaturationCount());
                } finally {
                    release.countDown();
                    for (int i = 0, n = threads.size(); i < n; i++) {
                        threads.getQuick(i).join();
                    }
                }
            }
        });
    }

    @Test
    public void testCooperativeYieldReschedulesFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final CooperativeYieldTask task = new CooperativeYieldTask(runtime);
            final OneShotTask competingTask = new OneShotTask();
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertFalse(task.isResumed);
                Assert.assertFalse(task.isDone());
                Assert.assertEquals(1, runtime.getMountCount());
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertEquals(1, runtime.getQueuedCount());
                Assert.assertEquals(LaunchResult.SATURATED, runtime.launch(competingTask));

                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(task.isResumed);
                Assert.assertTrue(task.isDone());
                Assert.assertEquals(1, task.doneCount);
                Assert.assertEquals(2, runtime.getMountCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertEquals(0, runtime.getQueuedCount());
            }
        });
    }

    @Test
    public void testCooperativeYieldReturnsAfterDrainTimeBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final CooperativeYieldTask task = new CooperativeYieldTask(runtime);
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                exhaustDrainTimeBudgetAfterProcess(runtime);
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(64));
                Assert.assertNull(task.error);
                Assert.assertFalse(task.isResumed);
                Assert.assertFalse(task.isDone());
                Assert.assertEquals(1, runtime.getMountCount());
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertEquals(1, runtime.getQueuedCount());
                Assert.assertEquals(LaunchResult.SATURATED, runtime.launch(new OneShotTask()));

                Assert.assertEquals(1, runtime.drain(64));
                Assert.assertNull(task.error);
                Assert.assertTrue(task.isResumed);
                Assert.assertTrue(task.isDone());
                Assert.assertEquals(1, task.doneCount);
                Assert.assertEquals(2, runtime.getMountCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertEquals(0, runtime.getQueuedCount());
            }
        });
    }

    @Test
    public void testConfigurationListenerReceivesCurrentValuesOnRegistration() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1, 1, 1);
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                runtime.updateConfiguration(4, 2, 7);

                final AtomicInteger maxLiveFiberCount = new AtomicInteger(-1);
                final AtomicInteger maxRetainedFiberCount = new AtomicInteger(-1);
                runtime.registerConfigurationListener((maxLive, maxRetained) -> {
                    maxLiveFiberCount.set(maxLive);
                    maxRetainedFiberCount.set(maxRetained);
                });

                Assert.assertEquals(4, maxLiveFiberCount.get());
                Assert.assertEquals(2, maxRetainedFiberCount.get());
            }
        });
    }

    @Test
    public void testConfigurationListenerCanBeUnregistered() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1, 1, 1);
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                final AtomicInteger callCount = new AtomicInteger();
                final FiberRuntimeConfigurationListener listener =
                        (maxLive, maxRetained) -> callCount.incrementAndGet();
                runtime.registerConfigurationListener(listener);

                Assert.assertEquals(1, callCount.get());
                Assert.assertEquals(1, runtime.getConfigurationListenerCountForTesting());
                Assert.assertTrue(runtime.unregisterConfigurationListener(listener));
                Assert.assertFalse(runtime.unregisterConfigurationListener(listener));
                Assert.assertEquals(0, runtime.getConfigurationListenerCountForTesting());

                runtime.updateConfiguration(2, 1, 1);

                Assert.assertEquals(1, callCount.get());
            }
        });
    }

    @Test
    public void testConfigurationListenersAreReleasedOnQuiesce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1, 1, 1);
            runtime.registerConfigurationListener((maxLive, maxRetained) -> {
            });
            Assert.assertEquals(1, runtime.getConfigurationListenerCountForTesting());

            runtime.beginQuiesce();

            Assert.assertEquals(0, runtime.getConfigurationListenerCountForTesting());
            Assert.assertTrue(runtime.awaitClosed(System.nanoTime() + TimeUnit.SECONDS.toNanos(5)));
            runtime.closeAfterDrained();
        });
    }

    @Test
    public void testConfigurationUpdateChangesLiveRetentionAndMountBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2, 2, 1);
            final OneShotTask firstTask = new OneShotTask();
            final OneShotTask secondTask = new OneShotTask();
            final OneShotTask thirdTask = new OneShotTask();
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(firstTask));
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(secondTask));

                runtime.updateConfiguration(1, 1, 7);
                Assert.assertEquals(1, runtime.getMaxLiveFiberCount());
                Assert.assertEquals(1, runtime.getMaxRetainedFiberCount());
                Assert.assertEquals(7, runtime.getMountBudget());
                Assert.assertEquals(LaunchResult.SATURATED, runtime.launch(thirdTask));

                Assert.assertEquals(2, runtime.drain(2));
                Assert.assertEquals(1, runtime.getRetainedFiberCount());
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(1, runtime.getRetiredFiberCount());

                runtime.updateConfiguration(3, 3, 9);
                firstTask.reopen();
                secondTask.reopen();
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(firstTask));
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(secondTask));
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(thirdTask));
                Assert.assertEquals(3, runtime.drain(3));
                Assert.assertEquals(3, runtime.getRetainedFiberCount());

                runtime.updateConfiguration(3, 1, 11);
                Assert.assertEquals(3, runtime.getMaxLiveFiberCount());
                Assert.assertEquals(1, runtime.getMaxRetainedFiberCount());
                Assert.assertEquals(11, runtime.getMountBudget());
                Assert.assertEquals(1, runtime.getRetainedFiberCount());
                Assert.assertEquals(2, runtime.drain(2));
                Assert.assertEquals(3, runtime.getRetiredFiberCount());
            }
        });
    }

    @Test
    public void testDirectLaunchCompletesWithoutQueueRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final DirectTask task = new DirectTask();
            final Fiber fiber = runtime.tryReserveFiber();

            Assert.assertNotNull(fiber);
            Assert.assertEquals(
                    LaunchResult.LAUNCHED,
                    runtime.launchReservedDirect(fiber, fiber.getReservationEpoch(), task, task.getIncarnation())
            );
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(Thread.currentThread(), task.thread);
            Assert.assertEquals(SuspensionScope.Mode.FIBER, task.mode);
            Assert.assertNull(SuspensionScope.getMode());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(1, runtime.getMountCount());

            close(runtime);
        });
    }

    @Test
    public void testDirectLaunchDefersWhileCarrierHoldsRoleSwitchReadLock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final DirectTask task = new DirectTask();
            final Fiber fiber = runtime.tryReserveFiber();
            final Lock lock = new ReentrantLock();
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();

            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                Assert.assertNotNull(fiber);
                lock.lock();
                SuspensionScope.enterRoleSwitchReadLock(scope, lock);
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
                    Assert.assertFalse(task.isDone());
                    Assert.assertEquals(1, runtime.getQueuedCount());
                    Assert.assertEquals(0, runtime.drain(1));
                } finally {
                    SuspensionScope.leaveRoleSwitchReadLock(scope, lock);
                    lock.unlock();
                }

                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(task.isDone());
            }
        });
    }

    @Test
    public void testDirectLaunchDuringCompletionQueuesReusedFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final OneShotTask replacement = new OneShotTask();
            final LaunchDirectReplacementOnDoneTask task = new LaunchDirectReplacementOnDoneTask(runtime, replacement);

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertTrue(task.isDone());
            Assert.assertEquals(LaunchResult.LAUNCHED, task.replacementLaunchResult);
            Assert.assertFalse(replacement.isDone());
            Assert.assertEquals(1, runtime.getQueuedCount());

            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(replacement.isDone());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());

            close(runtime);
        });
    }

    @Test
    public void testDirectLaunchParksAndResumesFromQueue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final PooledWaitTask task = new PooledWaitTask(waitQueue);
            final Fiber fiber = runtime.tryReserveFiber();

            Assert.assertNotNull(fiber);
            Assert.assertEquals(
                    LaunchResult.LAUNCHED,
                    runtime.launchReservedDirect(fiber, fiber.getReservationEpoch(), task, task.getIncarnation())
            );
            Assert.assertFalse(task.isDone());
            Assert.assertEquals(1, waitQueue.size());
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            waitQueue.fire(1, false);
            Assert.assertEquals(1, runtime.getQueuedCount());
            Assert.assertEquals(0, runtime.getParkedFiberCount());
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());

            close(runtime);
        });
    }

    @Test
    public void testDirectLaunchPostProcessFailureKeepsReservationBalanced() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final OneShotTask task = new OneShotTask();
            final Fiber fiber = runtime.tryReserveFiber();
            Assert.assertNotNull(fiber);
            final long reservationEpoch = fiber.getReservationEpoch();
            runtime.setAfterProcessForTesting(() -> {
                runtime.setAfterProcessForTesting(null);
                throw new IllegalStateException("post-process failure");
            });

            Assert.assertEquals(
                    LaunchResult.LAUNCHED,
                    runtime.launchReservedDirect(fiber, reservationEpoch, task, task.getIncarnation())
            );
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());

            final Fiber reusedFiber = runtime.tryReserveFiber();
            Assert.assertSame(fiber, reusedFiber);
            final long reusedReservationEpoch = reusedFiber.getReservationEpoch();
            runtime.releaseReservedFiber(fiber, reservationEpoch);
            Assert.assertEquals(1, runtime.getOutstandingTaskCount());
            runtime.releaseReservedFiber(reusedFiber, reusedReservationEpoch);
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());

            close(runtime);
        });
    }

    @Test
    public void testDriverFailureCancelsActiveWaitRegistrations() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue firstQueue = new FiberWalWaitQueue();
            final FiberWalWaitQueue secondQueue = new FiberWalWaitQueue();
            final DriverFailureWithActiveWaitTask task = new DriverFailureWithActiveWaitTask(firstQueue, secondQueue);

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, firstQueue.size());
            Assert.assertEquals(1, secondQueue.size());

            firstQueue.fire(1, false);
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertTrue(task.isDone());
            Assert.assertEquals(12, task.callbackOrder);
            Assert.assertEquals(0, firstQueue.size());
            Assert.assertEquals(0, secondQueue.size());
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());

            close(runtime);
        });
    }

    @Test
    public void testDriverFailureReleasesWaitBuildAdmission() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final DriverFailureDuringWaitBuildTask task = new DriverFailureDuringWaitBuildTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(12, task.callbackOrder);
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());

            close(runtime);
        });
    }

    @Test
    public void testDriverFailureTerminalizesTaskAndQuarantinesFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final DriverFailureTask task = new DriverFailureTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(12, task.callbackOrder);
            Assert.assertEquals("fiber did not unmount to free", task.error.getMessage());
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(1, runtime.getCreatedFiberCount());
            Assert.assertEquals(1, runtime.getRetiredFiberCount());

            final OneShotTask replacement = new OneShotTask();
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(replacement));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(replacement.isDone());
            Assert.assertEquals(2, runtime.getCreatedFiberCount());

            close(runtime);
        });
    }

    @Test
    public void testDrainTimeBudgetDoesNotLimitWaitingOrCompletedTasks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(3);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final PooledWaitTask waitingTask = new PooledWaitTask(waitQueue);
            final OneShotTask firstTask = new OneShotTask();
            final OneShotTask secondTask = new OneShotTask();
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                exhaustDrainTimeBudgetAfterProcess(runtime);
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(waitingTask));
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(firstTask));
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(secondTask));
                Assert.assertEquals(3, runtime.drain(64));
                Assert.assertNull(waitingTask.error);
                Assert.assertFalse(waitingTask.isDone());
                Assert.assertTrue(firstTask.isDone());
                Assert.assertTrue(secondTask.isDone());
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                Assert.assertEquals(1, runtime.getParkedFiberCount());
                Assert.assertEquals(0, runtime.getQueuedCount());

                waitQueue.fire(1, false);
                Assert.assertEquals(1, runtime.drain(64));
                Assert.assertNull(waitingTask.error);
                Assert.assertTrue(waitingTask.isDone());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertEquals(0, runtime.getQueuedCount());
            }
        });
    }

    @Test
    public void testDriverFailureUnwindsSuspendedTask() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final DriverFailureUnwindTask task = new DriverFailureUnwindTask(runtime, waitQueue);
            runtime.setAfterProcessForTesting(() -> {
                runtime.setAfterProcessForTesting(null);
                fillRunQueueForTesting(runtime);
                waitQueue.fire(1, false);
            });

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertTrue(task.isDone());
            Assert.assertEquals(12, task.callbackOrder);
            Assert.assertEquals("fiber ring is full", task.error.getMessage());
            Assert.assertEquals(1, task.cleanupCount);
            Assert.assertEquals(0, waitQueue.size());
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getParkedFiberCount());
            Assert.assertEquals(1, runtime.getRetiredFiberCount());

            close(runtime);
        });
    }

    @Test
    public void testEarlyReadyQueuesOnlyAfterProcessorReturns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            final CountDownLatch processorFinishing = new CountDownLatch(1);
            final CountDownLatch releaseProcessor = new CountDownLatch(1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final EarlyReadyTask task = new EarlyReadyTask(runtime);
            runtime.setAfterProcessForTesting(() -> {
                processorFinishing.countDown();
                try {
                    releaseProcessor.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            final Thread processor = new Thread(() -> {
                try {
                    runtime.initializeCarrier();
                    runtime.drain(1);
                } catch (Throwable th) {
                    error.set(th);
                }
            });
            processor.start();
            Assert.assertTrue(processorFinishing.await(10, TimeUnit.SECONDS));
            final int queuedWhileProcessing = runtime.getQueuedCount();
            runtime.setAfterProcessForTesting(null);
            releaseProcessor.countDown();
            processor.join(10_000);

            Assert.assertFalse(processor.isAlive());
            Assert.assertNull(error.get());
            Assert.assertEquals(1, runtime.drain(8));
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(0, queuedWhileProcessing);
            close(runtime);
        });
    }

    @Test
    public void testEarlyReadyRelaunchesAfterUnmount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            final EarlyReadyTask task = new EarlyReadyTask(runtime);

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(2, runtime.drain(8));
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(FiberTask.STATE_ARMING, task.observedParkState);
            Assert.assertEquals(LaunchResult.ALREADY_OWNED, task.wakeResult);
            Assert.assertEquals(2, task.runCount);
            Assert.assertEquals(1, runtime.getCreatedFiberCount());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());

            close(runtime);
        });
    }

    @Test
    public void testErrorCallsErrorThenDone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            final ErrorTask task = new ErrorTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(8));
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(12, task.callbackOrder);

            close(runtime);
        });
    }

    @Test
    public void testFreeFiberDoesNotRetainCompletedTask() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final ReferenceQueue<OneShotTask> taskQueue = new ReferenceQueue<>();
            final WeakReference<OneShotTask> taskRef = launchAndForget(runtime, taskQueue);

            // Wait on the reference queue rather than polling ref.get(): the queue hands off from
            // the reference processor, so a collector that clears the referent concurrently is
            // observed without a sleep-calibrated race. System.gc() is still only a hint, hence
            // the retry loop and the explicit message.
            Reference<? extends OneShotTask> collected = null;
            for (int i = 0; i < 20 && collected == null; i++) {
                System.gc();
                collected = taskQueue.remove(500);
            }

            Assert.assertNotNull("completed task was still reachable from the free fiber", collected);
            Assert.assertSame(taskRef, collected);
            close(runtime);
        });
    }

    @Test
    public void testImmediateRelaunchKeepsCurrentFiberUnderPoolContention() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1, 2);
            final OneShotTask seed = new OneShotTask();
            final StealOnParkTask task = new StealOnParkTask(runtime);
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                try {
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(seed));
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertTrue(seed.isDone());

                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertNotNull(task.reservedFiber);
                    Assert.assertFalse(task.isDone());
                    Assert.assertEquals(1, task.runCount);

                    runtime.releaseReservedFiber(task.reservedFiber, task.reservedFiberEpoch);
                    task.reservedFiber = null;
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertTrue(task.isDone());
                    Assert.assertEquals(2, task.runCount);
                    Assert.assertEquals(2, runtime.getCreatedFiberCount());
                } finally {
                    if (task.reservedFiber != null) {
                        runtime.releaseReservedFiber(task.reservedFiber, task.reservedFiberEpoch);
                        task.reservedFiber = null;
                    }
                }
            }
        });
    }

    @Test
    public void testInitialEnqueueFailureRollsBackReservation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final EnqueueFailureTask task = new EnqueueFailureTask();
            task.runtime = runtime;
            fillRunQueueForTesting(runtime);

            Assert.assertEquals(LaunchResult.TERMINAL, runtime.launch(task));
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(12, task.callbackOrder);
            Assert.assertEquals("fiber ring is full", task.error.getMessage());
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getQueuedCount());

            final OneShotTask replacement = new OneShotTask();
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(replacement));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(replacement.isDone());
            Assert.assertEquals(1, runtime.getCreatedFiberCount());

            close(runtime);
        });
    }

    @Test
    public void testPinnedCooperativeYieldRestoresMountedState() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final CooperativePinnedYieldTask task = new CooperativePinnedYieldTask(runtime);
            final OneShotTask competingTask = new OneShotTask();
            try {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(task.isRefusalObserved);
                Assert.assertFalse(task.isResumedAfterSuccessfulYield);
                Assert.assertFalse(task.isDone());
                Assert.assertEquals(0, task.doneCount);
                Assert.assertEquals(1, runtime.getMountCount());
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertEquals(1, runtime.getQueuedCount());
                Assert.assertEquals(LaunchResult.SATURATED, runtime.launch(competingTask));

                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertNull(task.error);
                Assert.assertTrue(task.isResumedAfterSuccessfulYield);
                Assert.assertTrue(task.isDone());
                Assert.assertEquals(1, task.doneCount);
                Assert.assertEquals(2, runtime.getMountCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertEquals(0, runtime.getQueuedCount());
            } finally {
                close(runtime, 1);
            }
        });
    }

    @Test
    public void testQuiesceAbandonsQueuedTask() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            final NeverMountedTask task = new NeverMountedTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            runtime.beginQuiesce();
            Assert.assertEquals(1, runtime.drain(8));
            Assert.assertTrue(task.isCancelled());
            Assert.assertFalse(task.hasRun);
            Assert.assertEquals(12, task.callbackOrder);
            Assert.assertTrue(runtime.awaitClosed(System.nanoTime() + 5_000_000_000L));
            runtime.closeAfterDrained();
        });
    }

    @Test
    public void testQuiesceDoesNotRetireOutstandingReservation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final OneShotTask task = new OneShotTask();
            final Fiber fiber = runtime.tryReserveFiber();

            Assert.assertNotNull(fiber);
            runtime.beginQuiesce();

            Assert.assertEquals(FiberRuntimeState.QUIESCING, runtime.state());
            Assert.assertEquals(0, runtime.getRetiredFiberCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(1, runtime.getOutstandingTaskCount());

            Assert.assertEquals(
                    LaunchResult.QUIESCING,
                    runtime.launchReserved(fiber, fiber.getReservationEpoch(), task, task.getIncarnation())
            );
            Assert.assertEquals(FiberTask.STATE_IDLE, task.getScheduleState());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(1, runtime.getQueuedCount());
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getRetiredFiberCount());
            Assert.assertTrue(runtime.awaitClosed(System.nanoTime() + 5_000_000_000L));
            runtime.closeAfterDrained();
        });
    }

    @Test
    public void testQuiesceListenerCanBeUnregisteredWhileOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final TestQuiesceListener listener = new TestQuiesceListener();
            listener.isComplete = true;
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                runtime.registerQuiesceListener(listener);

                Assert.assertEquals(1, runtime.getQuiesceListenerCountForTesting());
                Assert.assertTrue(runtime.unregisterQuiesceListener(listener));
                Assert.assertFalse(runtime.unregisterQuiesceListener(listener));
                Assert.assertEquals(0, runtime.getQuiesceListenerCountForTesting());

                runtime.beginQuiesce();

                Assert.assertEquals(0, listener.beginCount);
            }
        });
    }

    @Test
    public void testQuiesceListenerCannotBeUnregisteredAfterQuiesceStarts() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch beginEntered = new CountDownLatch(1);
            final AtomicInteger beginCount = new AtomicInteger();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch releaseBegin = new CountDownLatch(1);
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberRuntimeQuiesceListener listener = new FiberRuntimeQuiesceListener() {
                @Override
                public void beginQuiesce() {
                    beginCount.incrementAndGet();
                    beginEntered.countDown();
                    try {
                        if (!releaseBegin.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting to release quiesce listener");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }

                @Override
                public boolean isQuiesced() {
                    return true;
                }

                @Override
                public void progressQuiesce() {
                }
            };
            runtime.registerQuiesceListener(listener);
            final Thread quiesceThread = new Thread(() -> {
                try {
                    runtime.beginQuiesce();
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                }
            });
            quiesceThread.start();
            try {
                Assert.assertTrue(beginEntered.await(5, TimeUnit.SECONDS));
                Assert.assertEquals(FiberRuntimeState.QUIESCING, runtime.state());
                Assert.assertFalse(runtime.unregisterQuiesceListener(listener));
                Assert.assertEquals(1, runtime.getQuiesceListenerCountForTesting());
            } finally {
                releaseBegin.countDown();
                quiesceThread.join(5_000);
            }

            Assert.assertFalse(quiesceThread.isAlive());
            Assert.assertNull(error.get());
            Assert.assertEquals(1, beginCount.get());
            Assert.assertTrue(runtime.awaitClosed(System.nanoTime() + TimeUnit.SECONDS.toNanos(5)));
            Assert.assertEquals(0, runtime.getQuiesceListenerCountForTesting());
            runtime.closeAfterDrained();
        });
    }

    @Test
    public void testQuiesceListenerRespectsAwaitDeadline() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final TestQuiesceListener listener = new TestQuiesceListener();
            runtime.registerQuiesceListener(listener);

            runtime.beginQuiesce();

            Assert.assertEquals(1, listener.beginCount);
            Assert.assertFalse(runtime.awaitClosed(System.nanoTime() + 20_000_000L));
            Assert.assertEquals(FiberRuntimeState.QUIESCING, runtime.state());
            Assert.assertTrue(listener.progressCount > 0);

            listener.isComplete = true;
            Assert.assertTrue(runtime.awaitClosed(System.nanoTime() + 5_000_000_000L));
            runtime.closeAfterDrained();
        });
    }

    @Test
    public void testQuiesceSerializesReservationRelease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch continueRelease = new CountDownLatch(1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch quiesceComplete = new CountDownLatch(1);
            final CountDownLatch reservationReleased = new CountDownLatch(1);
            final FiberRuntime runtime = new FiberRuntime(1);
            final Fiber fiber = runtime.tryReserveFiber();
            Assert.assertNotNull(fiber);
            final long reservationEpoch = fiber.getReservationEpoch();
            runtime.setAfterReservationReleaseForTesting(() -> {
                reservationReleased.countDown();
                try {
                    if (!continueRelease.await(5, TimeUnit.SECONDS)) {
                        error.compareAndSet(null, new AssertionError("timed out releasing fiber reservation"));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    error.compareAndSet(null, e);
                }
            });

            final Thread quiesceThread = new Thread(() -> {
                try {
                    runtime.beginQuiesce();
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                } finally {
                    quiesceComplete.countDown();
                }
            });
            final Thread releaseThread = new Thread(() -> {
                try {
                    runtime.releaseReservedFiber(fiber, reservationEpoch);
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                }
            });
            try (RuntimeGuard runtimeGuard = new RuntimeGuard(runtime)) {
                try {
                    releaseThread.start();
                    Assert.assertTrue(reservationReleased.await(5, TimeUnit.SECONDS));
                    quiesceThread.start();
                    TestUtils.assertEventually(() -> Assert.assertEquals(
                            Thread.State.BLOCKED,
                            quiesceThread.getState()
                    ));
                    Assert.assertEquals(1, quiesceComplete.getCount());
                    continueRelease.countDown();
                    releaseThread.join(5_000);
                    quiesceThread.join(5_000);
                    runtime.setAfterReservationReleaseForTesting(null);

                    Assert.assertFalse(releaseThread.isAlive());
                    Assert.assertFalse(quiesceThread.isAlive());
                    Assert.assertNull(error.get());
                    Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertEquals(1, runtime.getRetiredFiberCount());
                    Assert.assertTrue(runtime.awaitClosed(System.nanoTime() + 5_000_000_000L));
                    runtime.closeAfterDrained();
                    runtimeGuard.disarm();
                } finally {
                    continueRelease.countDown();
                    releaseThread.join(5_000);
                    if (quiesceThread.getState() != Thread.State.NEW) {
                        quiesceThread.join(5_000);
                    }
                    runtime.setAfterReservationReleaseForTesting(null);
                    runtime.releaseReservedFiber(fiber, reservationEpoch);
                }
            }
        });
    }

    @Test
    public void testResignalEnqueueFailureTerminalizesReusedFiberTask() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final EnqueueFailureTask replacement = new EnqueueFailureTask();
            final LaunchReplacementOnDoneTask task = new LaunchReplacementOnDoneTask(runtime, replacement);

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertTrue(task.isDone());
            Assert.assertEquals(LaunchResult.LAUNCHED, task.replacementLaunchResult);
            Assert.assertTrue(replacement.isDone());
            Assert.assertEquals(12, replacement.callbackOrder);
            Assert.assertEquals("fiber ring is full", replacement.error.getMessage());
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(1, runtime.getRetiredFiberCount());

            close(runtime);
        });
    }

    @Test
    public void testSaturationRollbackSignalsCapacityWaiter() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2, 2);
            final Fiber reservedFiber = runtime.tryReserveFiber();
            Assert.assertNotNull(reservedFiber);
            final CapacitySignalTask task = new CapacitySignalTask(runtime);
            try (RuntimeGuard ignored = new RuntimeGuard(runtime)) {
                try {
                    Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertFalse(task.isDone());
                    Assert.assertEquals(1, runtime.getParkedFiberCount());

                    Assert.assertNull(runtime.tryReserveFiber());
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertTrue(task.isDone());
                    Assert.assertEquals(FiberWaitCoordinator.REASON_CAPACITY, task.reason);
                    Assert.assertEquals(0, runtime.getParkedFiberCount());
                } finally {
                    runtime.releaseReservedFiber(reservedFiber, reservedFiber.getReservationEpoch());
                }
            }
        });
    }

    @Test
    public void testStableOwnedAndTerminalLaunchDoNotAcquireFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            final OneShotTask task = new OneShotTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(LaunchResult.ALREADY_OWNED, runtime.launch(task));
            Assert.assertEquals(1, runtime.getCreatedFiberCount());
            Assert.assertEquals(1, runtime.drain(8));
            Assert.assertEquals(LaunchResult.TERMINAL, runtime.launch(task));
            Assert.assertEquals(1, runtime.getCreatedFiberCount());

            close(runtime);
        });
    }

    @Test
    public void testStaleIncarnationCannotClaimReopenedTask() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            final OneShotTask task = new OneShotTask();
            final long staleIncarnation = task.getIncarnation();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task, staleIncarnation));
            Assert.assertEquals(1, runtime.drain(8));
            task.reopen();

            Assert.assertEquals(LaunchResult.STALE_INCARNATION, runtime.launch(task, staleIncarnation));
            Assert.assertEquals(FiberTask.STATE_IDLE, task.getScheduleState());
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(8));

            close(runtime);
        });
    }

    @Test
    public void testStaleReservationCannotReleaseReusedFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final Fiber firstReservation = runtime.tryReserveFiber();
            Assert.assertNotNull(firstReservation);
            final long firstEpoch = firstReservation.getReservationEpoch();
            runtime.releaseReservedFiber(firstReservation, firstEpoch);

            final Fiber secondReservation = runtime.tryReserveFiber();
            Assert.assertSame(firstReservation, secondReservation);
            final long secondEpoch = secondReservation.getReservationEpoch();
            Assert.assertNotEquals(firstEpoch, secondEpoch);
            try {
                runtime.releaseReservedFiber(firstReservation, firstEpoch);
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                Assert.assertNull(runtime.tryReserveFiber());
            } finally {
                runtime.releaseReservedFiber(secondReservation, secondEpoch);
            }

            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            close(runtime);
        });
    }

    @Test
    public void testSteadyStateDeepWaitAndResumeAllocateNoJavaHeap() throws Exception {
        assertMemoryLeakWithAllocationCounter(threadMXBean -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final PooledWaitTask shallowTask = new PooledWaitTask(waitQueue);
            final DeepWaitTask deepTask = new DeepWaitTask(waitQueue, 1024);

            for (int i = 0; i < 10_000; i++) {
                runWait(runtime, shallowTask, waitQueue);
            }
            final long shallowBaseline = measureWaitAllocation(threadMXBean, runtime, shallowTask, waitQueue, 20_000);

            for (int i = 0; i < 2_000; i++) {
                runWait(runtime, deepTask, waitQueue);
            }
            final long deepSteadyState = measureWaitAllocation(threadMXBean, runtime, deepTask, waitQueue, 2_000);
            final long shallowAfterDeep = measureWaitAllocation(threadMXBean, runtime, shallowTask, waitQueue, 20_000);

            Assert.assertEquals("shallow park allocates at steady state", 0, shallowBaseline);
            Assert.assertEquals(
                    "deep park allocates at steady state, so the JVM drops the enlarged stack chunk"
                            + " instead of reusing it; the zero-allocation claim does not cover deep plans"
                            + " [bytesPerRound=" + deepSteadyState + ']',
                    0,
                    deepSteadyState
            );
            Assert.assertEquals(
                    "shallow park allocates after a deep park, so the fiber did not settle back"
                            + " [bytesPerRound=" + shallowAfterDeep + ']',
                    0,
                    shallowAfterDeep
            );

            close(runtime);
        });
    }

    @Test
    public void testSteadyStateLaunchAndDrainAllocateNoJavaHeap() throws Exception {
        assertMemoryLeakWithAllocationCounter(threadMXBean -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final OneShotTask task = new OneShotTask();
            for (int i = 0; i < 10_000; i++) {
                runOne(runtime, task);
            }

            long minAllocatedBytes = Long.MAX_VALUE;
            for (int round = 0; round < 5; round++) {
                final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                for (int i = 0; i < 100_000; i++) {
                    runOne(runtime, task);
                }
                minAllocatedBytes = Math.min(
                        minAllocatedBytes,
                        threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore
                );
            }

            Assert.assertEquals(0, minAllocatedBytes);
            close(runtime);
        });
    }

    @Test
    public void testSteadyStateReservedLaunchAndDrainAllocateNoJavaHeap() throws Exception {
        assertMemoryLeakWithAllocationCounter(threadMXBean -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final OneShotTask task = new OneShotTask();
            for (int i = 0; i < 10_000; i++) {
                runReserved(runtime, task);
            }

            long minAllocatedBytes = Long.MAX_VALUE;
            for (int round = 0; round < 5; round++) {
                final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                for (int i = 0; i < 100_000; i++) {
                    runReserved(runtime, task);
                }
                minAllocatedBytes = Math.min(
                        minAllocatedBytes,
                        threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore
                );
            }

            Assert.assertEquals(0, minAllocatedBytes);
            close(runtime);
        });
    }

    @Test
    public void testSteadyStateWaitAndResumeAllocateNoJavaHeap() throws Exception {
        assertMemoryLeakWithAllocationCounter(threadMXBean -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final PooledWaitTask task = new PooledWaitTask(waitQueue);
            for (int i = 0; i < 10_000; i++) {
                runWait(runtime, task, waitQueue);
            }

            long minAllocatedBytes = Long.MAX_VALUE;
            for (int round = 0; round < 5; round++) {
                final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                for (int i = 0; i < 20_000; i++) {
                    runWait(runtime, task, waitQueue);
                }
                minAllocatedBytes = Math.min(
                        minAllocatedBytes,
                        threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore
                );
            }

            Assert.assertEquals(0, minAllocatedBytes);
            close(runtime);
        });
    }

    private static void assertMemoryLeakWithAllocationCounter(AllocationCounterTest test) throws Exception {
        try (TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()) {
            final com.sun.management.ThreadMXBean threadMXBean = scope.getBean();
            TestUtils.assertMemoryLeak(() -> test.run(threadMXBean));
        }
    }

    private static void close(FiberRuntime runtime) {
        close(runtime, 0);
    }

    private static void close(FiberRuntime runtime, long expectedInlineSuspendViolationCount) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        Assert.assertEquals(expectedInlineSuspendViolationCount, runtime.getInlineSuspendViolationCount());
        runtime.closeAfterDrained();
    }

    private static void exhaustDrainTimeBudgetAfterProcess(FiberRuntime runtime) {
        runtime.setAfterProcessForTesting(() -> {
            runtime.setAfterProcessForTesting(null);
            // Establish elapsed time before notification publication without assuming an upper bound.
            final long startNanos = System.nanoTime();
            while (System.nanoTime() - startNanos < EXCEEDED_DRAIN_TIME_BUDGET_NANOS) {
                Thread.onSpinWait();
            }
        });
    }

    private static void fillRunQueueForTesting(FiberRuntime runtime) {
        runtime.setRunQueueDepthForTesting(runtime.getRunQueueCapacity());
    }

    private static WeakReference<OneShotTask> launchAndForget(
            FiberRuntime runtime,
            ReferenceQueue<OneShotTask> queue
    ) {
        final OneShotTask task = new OneShotTask();
        Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
        Assert.assertEquals(1, runtime.drain(1));
        Assert.assertTrue(task.isDone());
        return new WeakReference<>(task, queue);
    }

    private static long measureWaitAllocation(
            com.sun.management.ThreadMXBean threadMXBean,
            FiberRuntime runtime,
            PooledWaitTask task,
            FiberWalWaitQueue waitQueue,
            int cyclesPerRound
    ) {
        long minAllocatedBytes = Long.MAX_VALUE;
        for (int round = 0; round < 5; round++) {
            final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
            for (int i = 0; i < cyclesPerRound; i++) {
                runWait(runtime, task, waitQueue);
            }
            minAllocatedBytes = Math.min(
                    minAllocatedBytes,
                    threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore
            );
        }
        return minAllocatedBytes;
    }

    private static void resetRunQueueForTesting(FiberRuntime runtime) {
        runtime.setRunQueueDepthForTesting(0);
    }

    private static void runConcurrentTask(FiberRuntime runtime, OneShotTask task, long deadline) {
        if (task.isDone()) {
            task.reopen();
        }
        while (true) {
            final LaunchResult result = runtime.launch(task);
            if (result == LaunchResult.LAUNCHED) {
                break;
            }
            if (result != LaunchResult.SATURATED) {
                throw new AssertionError("unexpected fiber launch result [result=" + result + ']');
            }
            runtime.drain(8);
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("timed out launching fiber task");
            }
        }
        while (!task.isDone()) {
            runtime.drain(8);
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("timed out completing fiber task");
            }
        }
    }

    private static void runOne(FiberRuntime runtime, OneShotTask task) {
        if (task.isDone()) {
            task.reopen();
        }
        if (runtime.launch(task) != LaunchResult.LAUNCHED || runtime.drain(1) != 1 || !task.isDone()) {
            throw new AssertionError("fiber did not complete");
        }
    }

    private static void runReserved(FiberRuntime runtime, OneShotTask task) {
        if (task.isDone()) {
            task.reopen();
        }
        final Fiber fiber = runtime.tryReserveFiber();
        if (fiber == null
                || runtime.launchReserved(
                fiber,
                fiber.getReservationEpoch(),
                task,
                task.getIncarnation()
        ) != LaunchResult.LAUNCHED
                || runtime.drain(1) != 1
                || !task.isDone()) {
            throw new AssertionError("reserved fiber did not complete");
        }
    }

    private static void runWait(FiberRuntime runtime, PooledWaitTask task, FiberWalWaitQueue waitQueue) {
        if (task.isDone()) {
            task.reopen();
        }
        task.error = null;
        final LaunchResult launchResult = runtime.launch(task);
        final int parkCount = launchResult == LaunchResult.LAUNCHED ? runtime.drain(1) : 0;
        throwTaskError(task, "fiber failed before parking");
        if (launchResult != LaunchResult.LAUNCHED
                || parkCount != 1
                || task.isDone()
                || waitQueue.size() != 1) {
            throw new AssertionError("fiber did not park");
        }
        waitQueue.fire(1, false);
        final int resumeCount = runtime.drain(1);
        throwTaskError(task, "fiber failed while resuming");
        if (resumeCount != 1 || !task.isDone() || waitQueue.size() != 0) {
            throw new AssertionError("fiber did not resume");
        }
    }

    private static void throwTaskError(PooledWaitTask task, String message) {
        if (task.error != null) {
            throw new AssertionError(message, task.error);
        }
    }

    @FunctionalInterface
    private interface AllocationCounterTest {
        void run(com.sun.management.ThreadMXBean threadMXBean) throws Exception;
    }

    private abstract static class CallbackTask extends FiberTask {
        int callbackOrder;

        @Override
        protected void onAbandoned() {
            callbackOrder = callbackOrder * 10 + 1;
        }

        @Override
        protected void onDone() {
            callbackOrder = callbackOrder * 10 + 2;
        }
    }

    private static class CancelOnParkTask extends CallbackTask {
        @Override
        protected void onParked() {
            signalAxisA(SIGNAL_CANCEL);
        }

        @Override
        protected boolean runStep() {
            return false;
        }
    }

    private static class CancellationGenerationParkTask extends FiberTask {
        private final FiberCancellationSignal cancellationSignal;
        private long resumedGeneration;
        private int runCount;

        private CancellationGenerationParkTask(FiberCancellationSignal cancellationSignal) {
            this.cancellationSignal = cancellationSignal;
        }

        @Override
        public FiberCancellationSignal getCancellationSignal() {
            return cancellationSignal;
        }

        @Override
        protected boolean runStep() {
            if (++runCount == 1) {
                return false;
            }
            resumedGeneration = SuspensionScope.getCancellationSignalGeneration();
            return true;
        }
    }

    private static class CancellationGenerationTask extends FiberTask {
        private final FiberCancellationSignal cancellationSignal;
        private final long generation;
        private long resumedGeneration;
        private int secondWaitReason;

        private CancellationGenerationTask(FiberCancellationSignal cancellationSignal, long generation) {
            this.cancellationSignal = cancellationSignal;
            this.generation = generation;
        }

        @Override
        public FiberCancellationSignal getCancellationSignal() {
            return cancellationSignal;
        }

        private int awaitCancellation() {
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            final long token = fiber.beginWaitBuild(1);
            try {
                if (!coordinator.armCancellation(
                        token,
                        cancellationSignal,
                        SuspensionScope.getCancellationSignalGeneration()
                )) {
                    throw new IllegalStateException("cancellation wait registration failed");
                }
                return fiber.suspendWait(token);
            } catch (RuntimeException | Error th) {
                coordinator.abort(token);
                coordinator.consume(token);
                throw th;
            }
        }

        @Override
        protected long getCancellationSignalGeneration(FiberCancellationSignal cancellationSignal) {
            return generation;
        }

        @Override
        protected boolean runStep() {
            Assert.assertEquals(generation, SuspensionScope.getCancellationSignalGeneration());
            Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, awaitCancellation());
            resumedGeneration = SuspensionScope.getCancellationSignalGeneration();
            secondWaitReason = awaitCancellation();
            return true;
        }
    }

    private static class CancelledCapacityTask extends FiberTask {
        private final FiberCancellationSignal cancellationSignal;
        private final long generation;
        private final FiberRuntime runtime;
        private int reason;

        private CancelledCapacityTask(FiberRuntime runtime, FiberCancellationSignal cancellationSignal, long generation) {
            this.runtime = runtime;
            this.cancellationSignal = cancellationSignal;
            this.generation = generation;
        }

        @Override
        protected boolean runStep() {
            reason = runtime.awaitCapacity(cancellationSignal, generation);
            return true;
        }
    }

    private static class CapacitySignalTask extends FiberTask {
        private final FiberRuntime runtime;
        private int reason;

        private CapacitySignalTask(FiberRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        protected boolean runStep() {
            reason = runtime.awaitCapacity();
            return true;
        }
    }

    private static class CapacityWaitTask extends FiberTask {
        private final FiberRuntime runtime;
        private boolean hasReservedAfterWake;

        private CapacityWaitTask(FiberRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        protected boolean runStep() {
            final Fiber prematureFiber = runtime.tryReserveFiber();
            if (prematureFiber != null) {
                runtime.releaseReservedFiber(prematureFiber, prematureFiber.getReservationEpoch());
                throw new AssertionError("retiring fiber was available before retirement completed");
            }
            final int reason = runtime.awaitCapacity();
            if (reason != FiberWaitCoordinator.REASON_CAPACITY) {
                throw new AssertionError("unexpected capacity wait reason [reason=" + reason + ']');
            }
            final Fiber fiber = runtime.tryReserveFiber();
            if (fiber == null) {
                throw new AssertionError("fiber capacity was not available after wake");
            }
            hasReservedAfterWake = true;
            runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());
            return true;
        }
    }

    private static class CooperativePinnedYieldTask extends FiberTask {
        private int doneCount;
        private Throwable error;
        private boolean isRefusalObserved;
        private boolean isResumedAfterSuccessfulYield;
        private final FiberRuntime runtime;

        private CooperativePinnedYieldTask(FiberRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        protected void onDone() {
            doneCount++;
        }

        @Override
        protected void onError(Throwable th) {
            error = th;
        }

        @Override
        protected boolean runStep() {
            final Fiber fiber = Fiber.current();
            Assert.assertNotNull(fiber);
            Assert.assertEquals(1, runtime.getOutstandingTaskCount());
            Continuation.pin();
            try {
                Assert.assertFalse(Fiber.yieldCooperatively());
                isRefusalObserved = true;
                Assert.assertSame(fiber, Fiber.current());
                Assert.assertTrue(Fiber.isMounted());
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
            } finally {
                Continuation.unpin();
            }
            Assert.assertTrue(Fiber.yieldCooperatively());
            isResumedAfterSuccessfulYield = true;
            Assert.assertSame(fiber, Fiber.current());
            Assert.assertTrue(Fiber.isMounted());
            Assert.assertEquals(1, runtime.getOutstandingTaskCount());
            return true;
        }
    }

    private static class CooperativeYieldTask extends FiberTask {
        private int doneCount;
        private Throwable error;
        private boolean isResumed;
        private final FiberRuntime runtime;

        private CooperativeYieldTask(FiberRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        protected void onDone() {
            doneCount++;
        }

        @Override
        protected void onError(Throwable th) {
            error = th;
        }

        @Override
        protected boolean runStep() {
            Assert.assertEquals(1, runtime.getOutstandingTaskCount());
            Assert.assertTrue(Fiber.yieldCooperatively());
            Assert.assertEquals(1, runtime.getOutstandingTaskCount());
            isResumed = true;
            return true;
        }
    }

    private static class DeepWaitTask extends PooledWaitTask {
        private final int depth;

        private DeepWaitTask(FiberWalWaitQueue waitQueue, int depth) {
            super(waitQueue);
            this.depth = depth;
        }

        // HotSpot inlines at most MaxRecursiveInlineLevel (1) self-recursive levels,
        // so the frames this builds survive into the frozen stack
        private boolean descend(int remaining) {
            if (remaining == 0) {
                return super.runStep();
            }
            return descend(remaining - 1);
        }

        @Override
        protected boolean runStep() {
            return descend(depth);
        }
    }

    private static class DirectTask extends FiberTask {
        private SuspensionScope.Mode mode;
        private Thread thread;

        @Override
        protected boolean runStep() {
            mode = SuspensionScope.getMode();
            thread = Thread.currentThread();
            return true;
        }
    }

    private static class DriverFailureDuringWaitBuildTask extends DriverFailureTask {
        @Override
        protected boolean runStep() {
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            final long token = fiber.beginWaitBuild(1);
            fiber.getWaitCoordinator().acquireWal(token, 1);
            fiber.setExecutionStateForTesting(Long.MAX_VALUE);
            return true;
        }
    }

    private static class DriverFailureTask extends FiberTask {
        int callbackOrder;
        Throwable error;

        @Override
        protected void onDone() {
            callbackOrder = callbackOrder * 10 + 2;
        }

        @Override
        protected void onError(Throwable th) {
            callbackOrder = callbackOrder * 10 + 1;
            error = th;
        }

        @Override
        protected boolean runStep() {
            Objects.requireNonNull(Fiber.current()).setExecutionStateForTesting(Long.MAX_VALUE);
            return true;
        }
    }

    private static class DriverFailureUnwindTask extends DriverFailureTask {
        private final FiberRuntime runtime;
        private final FiberWalWaitQueue waitQueue;
        private int cleanupCount;

        private DriverFailureUnwindTask(FiberRuntime runtime, FiberWalWaitQueue waitQueue) {
            this.runtime = runtime;
            this.waitQueue = waitQueue;
        }

        @Override
        protected void onError(Throwable th) {
            resetRunQueueForTesting(runtime);
            super.onError(th);
        }

        @Override
        protected boolean runStep() {
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            final long token = fiber.beginWaitBuild(1);
            FiberWalWaitRegistration registration = null;
            try {
                registration = coordinator.acquireWal(token, 1);
                if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED) {
                    throw new IllegalStateException("wait registration failed");
                }
                final int reason = fiber.suspendWait(token);
                if (reason != FiberWaitCoordinator.REASON_WAL) {
                    throw new IllegalStateException("unexpected wait reason");
                }
                return true;
            } finally {
                cleanupCount++;
                if (registration != null) {
                    registration.cancel();
                }
                coordinator.teardownWait(token);
            }
        }
    }

    private static class DriverFailureWithActiveWaitTask extends DriverFailureTask {
        private final FiberWalWaitQueue firstQueue;
        private final FiberWalWaitQueue secondQueue;

        private DriverFailureWithActiveWaitTask(FiberWalWaitQueue firstQueue, FiberWalWaitQueue secondQueue) {
            this.firstQueue = firstQueue;
            this.secondQueue = secondQueue;
        }

        @Override
        protected boolean runStep() {
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            final long token = fiber.beginWaitBuild(2);
            FiberWalWaitRegistration firstRegistration = null;
            FiberWalWaitRegistration secondRegistration = null;
            try {
                firstRegistration = coordinator.acquireWal(token, 1);
                secondRegistration = coordinator.acquireWal(token, 1);
                if (firstRegistration.register(firstQueue) != SourceRegistrationResult.ACCEPTED
                        || secondRegistration.register(secondQueue) != SourceRegistrationResult.ACCEPTED) {
                    throw new IllegalStateException("wait registration failed");
                }
                final int reason = fiber.suspendWait(token);
                if (reason != FiberWaitCoordinator.REASON_WAL) {
                    throw new IllegalStateException("unexpected wait reason");
                }
                fiber.setExecutionStateForTesting(Long.MAX_VALUE);
                return true;
            } catch (RuntimeException | Error th) {
                if (firstRegistration != null) {
                    firstRegistration.cancel();
                }
                if (secondRegistration != null) {
                    secondRegistration.cancel();
                }
                coordinator.abort(token);
                coordinator.consume(token);
                throw th;
            }
        }
    }

    private static class EarlyReadyTask extends FiberTask {
        private final FiberRuntime runtime;
        int observedParkState;
        int runCount;
        LaunchResult wakeResult;

        private EarlyReadyTask(FiberRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        protected void onParked() {
            observedParkState = getScheduleState();
            wakeResult = runtime.launch(this);
        }

        @Override
        protected boolean runStep() {
            return ++runCount == 2;
        }
    }

    private static class EnqueueFailureTask extends FiberTask {
        int callbackOrder;
        Throwable error;
        FiberRuntime runtime;

        @Override
        protected void onDone() {
            callbackOrder = callbackOrder * 10 + 2;
        }

        @Override
        protected void onError(Throwable th) {
            resetRunQueueForTesting(runtime);
            callbackOrder = callbackOrder * 10 + 1;
            error = th;
        }

        @Override
        protected boolean runStep() {
            return true;
        }
    }

    private static class ErrorTask extends FiberTask {
        int callbackOrder;

        @Override
        protected void onDone() {
            callbackOrder = callbackOrder * 10 + 2;
        }

        @Override
        protected void onError(Throwable th) {
            callbackOrder = callbackOrder * 10 + 1;
        }

        @Override
        protected boolean runStep() {
            throw new IllegalStateException("test");
        }
    }

    private static class LaunchDirectReplacementOnDoneTask extends FiberTask {
        private final OneShotTask replacement;
        private final FiberRuntime runtime;
        private LaunchResult replacementLaunchResult;

        private LaunchDirectReplacementOnDoneTask(FiberRuntime runtime, OneShotTask replacement) {
            this.runtime = runtime;
            this.replacement = replacement;
        }

        @Override
        protected void onDone() {
            final Fiber fiber = runtime.tryReserveFiber();
            if (fiber == null) {
                throw new AssertionError("replacement fiber reservation failed");
            }
            replacementLaunchResult = runtime.launchReservedDirect(
                    fiber,
                    fiber.getReservationEpoch(),
                    replacement,
                    replacement.getIncarnation()
            );
        }

        @Override
        protected boolean runStep() {
            return true;
        }
    }

    private static class LaunchReplacementOnDoneTask extends FiberTask {
        private final EnqueueFailureTask replacement;
        private final FiberRuntime runtime;
        private LaunchResult replacementLaunchResult;

        private LaunchReplacementOnDoneTask(FiberRuntime runtime, EnqueueFailureTask replacement) {
            this.runtime = runtime;
            this.replacement = replacement;
        }

        @Override
        protected void onDone() {
            replacementLaunchResult = runtime.launch(replacement);
            replacement.runtime = runtime;
            fillRunQueueForTesting(runtime);
        }

        @Override
        protected boolean runStep() {
            return true;
        }
    }

    private static class NeverMountedTask extends CallbackTask {
        boolean hasRun;

        @Override
        protected boolean runStep() {
            hasRun = true;
            return true;
        }
    }

    private static class OneShotTask extends FiberTask {
        @Override
        protected boolean runStep() {
            return true;
        }
    }

    private static class PooledWaitTask extends FiberTask {
        private final FiberWalWaitQueue waitQueue;
        private Throwable error;

        private PooledWaitTask(FiberWalWaitQueue waitQueue) {
            this.waitQueue = waitQueue;
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
            FiberWalWaitRegistration registration = null;
            try {
                registration = coordinator.acquireWal(token, 1);
                if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED) {
                    throw new IllegalStateException("wait registration failed");
                }
                final int reason = fiber.suspendWait(token);
                registration.cancel();
                if (reason != FiberWaitCoordinator.REASON_WAL) {
                    throw new IllegalStateException("unexpected wait reason");
                }
                return true;
            } catch (RuntimeException | Error th) {
                if (registration != null) {
                    registration.cancel();
                }
                coordinator.abort(token);
                coordinator.consume(token);
                throw th;
            }
        }
    }

    private static final class RuntimeGuard implements AutoCloseable {
        private final FiberRuntime runtime;
        private boolean isArmed = true;

        private RuntimeGuard(FiberRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        public void close() {
            if (isArmed) {
                isArmed = false;
                FiberRuntimeTest.close(runtime);
            }
        }

        private void disarm() {
            isArmed = false;
        }
    }

    private static class ScratchMutationTask extends FiberTask {
        private final AtomicBoolean flag;

        private ScratchMutationTask(AtomicBoolean flag) {
            this.flag = flag;
        }

        @Override
        protected boolean runStep() {
            SuspensionScope.getCancellationBindingScratch().set(flag);
            return true;
        }
    }

    private static class ScratchWaitTask extends FiberTask {
        private final AtomicBoolean expectedFlag;
        private final FiberWalWaitQueue waitQueue;
        private AtomicBoolean resumedFlag;

        private ScratchWaitTask(FiberWalWaitQueue waitQueue, AtomicBoolean expectedFlag) {
            this.expectedFlag = expectedFlag;
            this.waitQueue = waitQueue;
        }

        @Override
        protected boolean runStep() {
            final CancellationBinding binding = SuspensionScope.getCancellationBindingScratch();
            binding.set(expectedFlag);
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            final long token = fiber.beginWaitBuild(1);
            FiberWalWaitRegistration registration = null;
            try {
                registration = coordinator.acquireWal(token, 1);
                if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED) {
                    throw new IllegalStateException("wait registration failed");
                }
                final int reason = fiber.suspendWait(token);
                registration.cancel();
                if (reason != FiberWaitCoordinator.REASON_WAL) {
                    throw new IllegalStateException("unexpected wait reason");
                }
                resumedFlag = binding.getFlag();
                return true;
            } catch (RuntimeException | Error th) {
                if (registration != null) {
                    registration.cancel();
                }
                coordinator.abort(token);
                coordinator.consume(token);
                throw th;
            }
        }
    }

    private static class StealOnParkTask extends FiberTask {
        private final FiberRuntime runtime;
        private Fiber reservedFiber;
        private long reservedFiberEpoch;
        private int runCount;

        private StealOnParkTask(FiberRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        protected void onParked() {
            reservedFiber = runtime.tryReserveFiber();
            if (reservedFiber == null) {
                throw new AssertionError("test reservation failed");
            }
            reservedFiberEpoch = reservedFiber.getReservationEpoch();
            if (!signalAxisA(SIGNAL_READY)) {
                throw new AssertionError("test task was not arming");
            }
        }

        @Override
        protected boolean runStep() {
            return ++runCount == 2;
        }
    }

    private static class TestQuiesceListener implements FiberRuntimeQuiesceListener {
        private int beginCount;
        private volatile boolean isComplete;
        private int progressCount;

        @Override
        public void beginQuiesce() {
            beginCount++;
        }

        @Override
        public boolean isQuiesced() {
            return isComplete;
        }

        @Override
        public void progressQuiesce() {
            progressCount++;
        }
    }
}

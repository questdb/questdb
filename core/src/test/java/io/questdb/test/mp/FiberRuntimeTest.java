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

import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
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
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FiberRuntimeTest {

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
                    runtime.launchReservedDirect(fiber, capacityWaitTask, capacityWaitTask.getIncarnation())
            );
            Assert.assertFalse(capacityWaitTask.isDone());
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(capacityWaitTask.isDone());
            Assert.assertTrue(capacityWaitTask.hasReservedAfterWake);

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
            try {
                for (int i = 0; i < threadCount; i++) {
                    final Thread thread = new Thread(() -> {
                        Fiber fiber = null;
                        try {
                            start.await();
                            fiber = runtime.tryReserveFiber();
                            if (fiber != null) {
                                reservationCount.incrementAndGet();
                            }
                            attempted.countDown();
                            release.await();
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        } finally {
                            if (fiber != null) {
                                runtime.releaseReservedFiber(fiber);
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
                close(runtime);
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
                    runtime.launchReservedDirect(fiber, task, task.getIncarnation())
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
                    runtime.launchReservedDirect(fiber, task, task.getIncarnation())
            );
            Assert.assertFalse(task.isDone());
            Assert.assertEquals(1, waitQueue.size());
            Assert.assertEquals(0, runtime.getQueuedCount());

            waitQueue.fire(1, false);
            Assert.assertEquals(1, runtime.getQueuedCount());
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());

            close(runtime);
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
            close(runtime);
            Assert.assertEquals(0, queuedWhileProcessing);
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
            final WeakReference<OneShotTask> taskRef = launchAndForget(runtime);

            for (int i = 0; i < 100 && taskRef.get() != null; i++) {
                System.gc();
                Thread.sleep(10);
            }

            Assert.assertNull(taskRef.get());
            close(runtime);
        });
    }

    @Test
    public void testImmediateRelaunchKeepsCurrentFiberUnderPoolContention() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1, 2);
            final OneShotTask seed = new OneShotTask();
            final StealOnParkTask task = new StealOnParkTask(runtime);
            try {
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(seed));
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(seed.isDone());

                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertNotNull(task.reservedFiber);
                Assert.assertFalse(task.isDone());
                Assert.assertEquals(1, task.runCount);

                runtime.releaseReservedFiber(task.reservedFiber);
                task.reservedFiber = null;
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(task.isDone());
                Assert.assertEquals(2, task.runCount);
                Assert.assertEquals(2, runtime.getCreatedFiberCount());
            } finally {
                if (task.reservedFiber != null) {
                    runtime.releaseReservedFiber(task.reservedFiber);
                    task.reservedFiber = null;
                }
                close(runtime);
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
                    runtime.launchReserved(fiber, task, task.getIncarnation())
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
    public void testSteadyStateDeepWaitAndResumeAllocateNoJavaHeap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final java.lang.management.ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
            Assume.assumeTrue(mxBean instanceof com.sun.management.ThreadMXBean);
            final com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) mxBean;
            Assume.assumeTrue(threadMXBean.isThreadAllocatedMemorySupported());
            if (!threadMXBean.isThreadAllocatedMemoryEnabled()) {
                threadMXBean.setThreadAllocatedMemoryEnabled(true);
            }

            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final PooledWaitTask shallowTask = new PooledWaitTask(waitQueue);
            final DeepWaitTask deepTask = new DeepWaitTask(waitQueue, 4096);

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
        TestUtils.assertMemoryLeak(() -> {
            final java.lang.management.ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
            Assume.assumeTrue(mxBean instanceof com.sun.management.ThreadMXBean);
            final com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) mxBean;
            Assume.assumeTrue(threadMXBean.isThreadAllocatedMemorySupported());
            if (!threadMXBean.isThreadAllocatedMemoryEnabled()) {
                threadMXBean.setThreadAllocatedMemoryEnabled(true);
            }

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
        TestUtils.assertMemoryLeak(() -> {
            final java.lang.management.ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
            Assume.assumeTrue(mxBean instanceof com.sun.management.ThreadMXBean);
            final com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) mxBean;
            Assume.assumeTrue(threadMXBean.isThreadAllocatedMemorySupported());
            if (!threadMXBean.isThreadAllocatedMemoryEnabled()) {
                threadMXBean.setThreadAllocatedMemoryEnabled(true);
            }

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
        TestUtils.assertMemoryLeak(() -> {
            final java.lang.management.ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
            Assume.assumeTrue(mxBean instanceof com.sun.management.ThreadMXBean);
            final com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) mxBean;
            Assume.assumeTrue(threadMXBean.isThreadAllocatedMemorySupported());
            if (!threadMXBean.isThreadAllocatedMemoryEnabled()) {
                threadMXBean.setThreadAllocatedMemoryEnabled(true);
            }

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

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        Assert.assertEquals(0, runtime.getInlineSuspendViolationCount());
        runtime.closeAfterDrained();
    }

    private static void fillRunQueueForTesting(FiberRuntime runtime) {
        runtime.setRunQueueDepthForTesting(runtime.getRunQueueCapacity());
    }

    private static WeakReference<OneShotTask> launchAndForget(FiberRuntime runtime) {
        final OneShotTask task = new OneShotTask();
        Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
        Assert.assertEquals(1, runtime.drain(1));
        Assert.assertTrue(task.isDone());
        return new WeakReference<>(task);
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
                || runtime.launchReserved(fiber, task, task.getIncarnation()) != LaunchResult.LAUNCHED
                || runtime.drain(1) != 1
                || !task.isDone()) {
            throw new AssertionError("reserved fiber did not complete");
        }
    }

    private static void runWait(FiberRuntime runtime, PooledWaitTask task, FiberWalWaitQueue waitQueue) {
        if (task.isDone()) {
            task.reopen();
        }
        if (runtime.launch(task) != LaunchResult.LAUNCHED
                || runtime.drain(1) != 1
                || task.isDone()
                || waitQueue.size() != 1) {
            throw new AssertionError("fiber did not park");
        }
        waitQueue.fire(1, false);
        if (runtime.drain(1) != 1 || !task.isDone() || task.hasError || waitQueue.size() != 0) {
            throw new AssertionError("fiber did not resume");
        }
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

    private static class CapacityWaitTask extends FiberTask {
        private boolean hasReservedAfterWake;
        private final FiberRuntime runtime;

        private CapacityWaitTask(FiberRuntime runtime) {
            this.runtime = runtime;
        }

        @Override
        protected boolean runStep() {
            final Fiber prematureFiber = runtime.tryReserveFiber();
            if (prematureFiber != null) {
                runtime.releaseReservedFiber(prematureFiber);
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
            runtime.releaseReservedFiber(fiber);
            return true;
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

    private static class DeepWaitTask extends PooledWaitTask {
        private final int depth;

        private DeepWaitTask(FiberWalWaitQueue waitQueue, int depth) {
            super(waitQueue);
            this.depth = depth;
        }

        @Override
        protected boolean runStep() {
            return descend(depth);
        }

        // HotSpot inlines at most MaxRecursiveInlineLevel (1) self-recursive levels,
        // so the frames this builds survive into the frozen stack
        private boolean descend(int remaining) {
            if (remaining == 0) {
                return super.runStep();
            }
            return descend(remaining - 1);
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
                        || !coordinator.tryAcceptSource(token)
                        || secondRegistration.register(secondQueue) != SourceRegistrationResult.ACCEPTED
                        || !coordinator.tryAcceptSource(token)) {
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
        private LaunchResult replacementLaunchResult;
        private final FiberRuntime runtime;

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
        private LaunchResult replacementLaunchResult;
        private final FiberRuntime runtime;

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
        private boolean hasError;

        private PooledWaitTask(FiberWalWaitQueue waitQueue) {
            this.waitQueue = waitQueue;
        }

        @Override
        protected void onError(Throwable th) {
            hasError = true;
        }

        @Override
        protected boolean runStep() {
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            final long token = fiber.beginWaitBuild(1);
            FiberWalWaitRegistration registration = null;
            try {
                registration = coordinator.acquireWal(token, 1);
                if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED
                        || !coordinator.tryAcceptSource(token)) {
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

    private static class StealOnParkTask extends FiberTask {
        private final FiberRuntime runtime;
        private Fiber reservedFiber;
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

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
import io.questdb.mp.continuation.FiberEventWaitQueue;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class FiberTest {

    @Test
    public void testCancelledTaskCanBeReopened() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final OneShotTask task = new OneShotTask();

            Assert.assertTrue(task.tryCancel());
            Assert.assertEquals(LaunchResult.TERMINAL, runtime.launch(task));
            Assert.assertEquals(0, runtime.getCreatedFiberCount());

            task.reopen();
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.isDone());
            Assert.assertTrue(task.hasRun);
            close(runtime);
        });
    }

    @Test
    public void testCurrentResolvesMountedFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final CurrentRecordingTask task = new CurrentRecordingTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.isDone());
            Assert.assertNotNull(task.observedFiber);
            Assert.assertTrue(task.wasMounted);
            Assert.assertNull(Fiber.current());
            Assert.assertFalse(Fiber.isMounted());
            close(runtime);
        });
    }

    @Test
    public void testCurrentUnsetOutsideMount() {
        Assert.assertFalse(Fiber.isMounted());
        Assert.assertNull(Fiber.current());
    }

    @Test
    public void testCancellationSourceFollowsFiberAcrossCarriers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final CancellationSourceRecordingTask task = new CancellationSourceRecordingTask(waitQueue);
            final AtomicReference<Throwable> resumeFailure = new AtomicReference<>();
            final AtomicReference<CancellationBinding.Source> carrierSourceAfterDrain = new AtomicReference<>();
            final CancellationBinding.Source carrierSource = new RecordingCancellationSource();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertFalse(task.isDone());

            waitQueue.fire(1, false);
            final Thread resumeThread = new Thread(() -> {
                try {
                    SuspensionScope.enterCancellationSource(carrierSource);
                    Assert.assertEquals(1, runtime.drain(1));
                    carrierSourceAfterDrain.set(SuspensionScope.getCancellationSource());
                } catch (Throwable th) {
                    resumeFailure.set(th);
                }
            });
            resumeThread.start();
            join(resumeThread);
            Assert.assertNull(resumeFailure.get());
            Assert.assertTrue(task.isDone());
            Assert.assertSame(task.installedSource, task.resumedSource);
            Assert.assertSame(carrierSource, carrierSourceAfterDrain.get());
            close(runtime);
        });
    }

    @Test
    public void testParkedFiberResumesOnDifferentThread() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final CarrierRecordingTask task = new CarrierRecordingTask(waitQueue);
            final AtomicReference<Throwable> resumeFailure = new AtomicReference<>();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertSame(Thread.currentThread(), task.firstCarrier);
            Assert.assertFalse(task.isDone());

            waitQueue.fire(1, false);
            final Thread resumeThread = new Thread(() -> {
                try {
                    Assert.assertEquals(1, runtime.drain(1));
                } catch (Throwable th) {
                    resumeFailure.set(th);
                }
            });
            resumeThread.start();
            join(resumeThread);
            Assert.assertNull(resumeFailure.get());
            Assert.assertTrue(task.isDone());
            Assert.assertSame(resumeThread, task.secondCarrier);
            close(runtime);
        });
    }

    @Test
    public void testRepeatedParksAdvanceExecution() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final TwoParkTask task = new TwoParkTask(waitQueue);

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, task.step);
            Assert.assertFalse(task.isDone());

            waitQueue.fire(1, false);
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(2, task.step);
            Assert.assertFalse(task.isDone());

            waitQueue.fire(1, false);
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(3, task.step);
            Assert.assertTrue(task.isDone());
            close(runtime);
        });
    }

    @Test
    public void testSaturationDoesNotCreateOverflowFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final WaitingTask waiting = new WaitingTask(waitQueue);

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(waiting));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, waitQueue.size());
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            final OneShotTask oneShot = new OneShotTask();
            Assert.assertEquals(LaunchResult.SATURATED, runtime.launch(oneShot));
            Assert.assertEquals(1, runtime.getCreatedFiberCount());
            Assert.assertEquals(1, runtime.getLiveFiberCount());
            Assert.assertEquals(1, runtime.getOutstandingTaskCount());
            Assert.assertEquals(1, runtime.getLaunchCount(LaunchResult.SATURATED));
            Assert.assertEquals(1, runtime.getSaturationCount());

            waitQueue.fire(1, false);
            Assert.assertEquals(1, runtime.drain(8));
            Assert.assertTrue(waiting.isDone());
            Assert.assertEquals(0, runtime.getParkedFiberCount());

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(oneShot));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(oneShot.isDone());
            Assert.assertEquals(1, runtime.getCreatedFiberCount());
            Assert.assertEquals(1, runtime.getRetainedFiberCount());
            Assert.assertEquals(0, runtime.getRetiredFiberCount());
            close(runtime);
        });
    }

    @Test
    public void testSequentialTasksReuseOneFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            for (int i = 0; i < 1_000; i++) {
                final OneShotTask task = new OneShotTask();
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(task.isDone());
            }
            Assert.assertEquals(1, runtime.getCreatedFiberCount());
            Assert.assertEquals(1, runtime.getRetainedFiberCount());
            close(runtime);
        });
    }

    @Test
    public void testStaleFireCannotWakeReusedWait() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch oldHelperPaused = new CountDownLatch(1);
            final CountDownLatch releaseOldHelper = new CountDownLatch(1);
            final AtomicBoolean isFirstHelper = new AtomicBoolean(true);
            final AtomicReference<Throwable> oldHelperFailure = new AtomicReference<>();
            final AtomicReference<Throwable> resumeFailure = new AtomicReference<>();
            final FiberRuntime runtime = new FiberRuntime(1, 1, null, () -> {
                if (isFirstHelper.compareAndSet(true, false)) {
                    oldHelperPaused.countDown();
                    await(releaseOldHelper);
                }
            });
            final ReusedWaitTask task = new ReusedWaitTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            final FiberWaitCoordinator coordinator = task.coordinator;

            final Thread oldHelper = new Thread(() -> {
                try {
                    Assert.assertTrue(coordinator.fire(task.firstToken, FiberWaitCoordinator.REASON_TIMER));
                } catch (Throwable th) {
                    oldHelperFailure.set(th);
                }
            });
            final Thread resumeThread = new Thread(() -> {
                try {
                    Assert.assertEquals(1, runtime.drain(1));
                } catch (Throwable th) {
                    resumeFailure.set(th);
                }
            });
            try {
                oldHelper.start();
                await(oldHelperPaused);
                Assert.assertFalse(coordinator.fire(task.firstToken, FiberWaitCoordinator.REASON_WAL));

                resumeThread.start();
                await(task.secondWaitBuilding);

                releaseOldHelper.countDown();
                join(oldHelper);
                Assert.assertNull(oldHelperFailure.get());

                task.allowSecondSuspend.countDown();
                join(resumeThread);
                Assert.assertNull(resumeFailure.get());
                Assert.assertFalse(task.isDone());

                Assert.assertTrue(coordinator.fire(task.secondToken, FiberWaitCoordinator.REASON_WAL));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(task.isDone());
                Assert.assertEquals(FiberWaitCoordinator.REASON_TIMER, task.firstReason);
                Assert.assertEquals(FiberWaitCoordinator.REASON_WAL, task.secondReason);
            } finally {
                releaseOldHelper.countDown();
                task.allowSecondSuspend.countDown();
                join(oldHelper);
                join(resumeThread);
                close(runtime);
            }
        });
    }

    @Test
    public void testWaitResumeRestoresDeepFrames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final DeepWaitingTask task = new DeepWaitingTask(waitQueue);

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, waitQueue.size());
            Assert.assertFalse(task.isDone());

            waitQueue.fire(1, false);
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(42, task.result);
            close(runtime);
        });
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
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

    private static void join(Thread thread) {
        if (thread.getState() == Thread.State.NEW) {
            return;
        }
        try {
            thread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        if (thread.isAlive()) {
            throw new AssertionError("timed out joining test thread");
        }
    }

    private static int waitDeep(WaitingTask task, int depth) {
        if (depth == 0) {
            task.awaitWal();
            return 35;
        }
        return waitDeep(task, depth - 1) + 1;
    }

    private static class CarrierRecordingTask extends WaitingTask {
        private volatile Thread firstCarrier;
        private volatile Thread secondCarrier;

        private CarrierRecordingTask(FiberWalWaitQueue waitQueue) {
            super(waitQueue);
        }

        @Override
        protected boolean runStep() {
            firstCarrier = Thread.currentThread();
            awaitWal();
            secondCarrier = Thread.currentThread();
            return true;
        }
    }

    private static class CancellationSourceRecordingTask extends WaitingTask {
        private final CancellationBinding.Source installedSource = new RecordingCancellationSource();
        private volatile CancellationBinding.Source resumedSource;

        private CancellationSourceRecordingTask(FiberWalWaitQueue waitQueue) {
            super(waitQueue);
        }

        @Override
        protected boolean runStep() {
            SuspensionScope.enterCancellationSource(installedSource);
            awaitWal();
            resumedSource = SuspensionScope.getCancellationSource();
            return true;
        }
    }

    private static class CurrentRecordingTask extends FiberTask {
        private Fiber observedFiber;
        private boolean wasMounted;

        @Override
        protected boolean runStep() {
            observedFiber = Fiber.current();
            wasMounted = Fiber.isMounted();
            return true;
        }
    }

    private static class DeepWaitingTask extends WaitingTask {
        private int result;

        private DeepWaitingTask(FiberWalWaitQueue waitQueue) {
            super(waitQueue);
        }

        @Override
        protected boolean runStep() {
            result = waitDeep(this, 7);
            return true;
        }
    }

    private static class OneShotTask extends FiberTask {
        private boolean hasRun;

        @Override
        protected boolean runStep() {
            hasRun = true;
            return true;
        }
    }

    private static class RecordingCancellationSource implements CancellationBinding.Source {
        @Override
        public void copyCancelledFlagTo(CancellationBinding target) {
            target.clear();
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
        }
    }

    private static class ReusedWaitTask extends FiberTask {
        private final CountDownLatch allowSecondSuspend = new CountDownLatch(1);
        private final CountDownLatch secondWaitBuilding = new CountDownLatch(1);
        private final FiberEventWaitQueue waitQueue =
                new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);
        private volatile FiberWaitCoordinator coordinator;
        private volatile int firstReason;
        private volatile long firstToken;
        private volatile int secondReason;
        private volatile long secondToken;

        @Override
        protected boolean runStep() {
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            coordinator = fiber.getWaitCoordinator();

            firstToken = fiber.beginWaitBuild(1);
            try {
                if (!coordinator.armEvent(firstToken, waitQueue)) {
                    throw new IllegalStateException("first wait registration failed");
                }
                firstReason = fiber.suspendWait(firstToken);
            } finally {
                coordinator.abort(firstToken);
                coordinator.consume(firstToken);
            }

            secondToken = fiber.beginWaitBuild(1);
            try {
                if (!coordinator.armEvent(secondToken, waitQueue)) {
                    throw new IllegalStateException("second wait registration failed");
                }
                secondWaitBuilding.countDown();
                await(allowSecondSuspend);
                secondReason = fiber.suspendWait(secondToken);
            } finally {
                coordinator.abort(secondToken);
                coordinator.consume(secondToken);
            }
            return true;
        }
    }

    private static class TwoParkTask extends WaitingTask {
        private int step;

        private TwoParkTask(FiberWalWaitQueue waitQueue) {
            super(waitQueue);
        }

        @Override
        protected boolean runStep() {
            step = 1;
            awaitWal();
            step = 2;
            awaitWal();
            step = 3;
            return true;
        }
    }

    private static class WaitingTask extends FiberTask {
        private final FiberWalWaitQueue waitQueue;

        private WaitingTask(FiberWalWaitQueue waitQueue) {
            this.waitQueue = waitQueue;
        }

        void awaitWal() {
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
            } finally {
                registration.cancel();
                coordinator.abort(token);
                coordinator.consume(token);
            }
        }

        @Override
        protected boolean runStep() {
            awaitWal();
            return true;
        }
    }
}

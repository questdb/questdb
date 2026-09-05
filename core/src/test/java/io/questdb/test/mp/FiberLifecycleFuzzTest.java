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
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberEventWaitQueue;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Randomly composes FiberTask scheduling, fiber waits, cancellation, saturation and quiesce.
 */
public class FiberLifecycleFuzzTest {
    private static final long AWAIT_CLOSED_TIMEOUT_NANOS = 30_000_000_000L;
    private static final long DRIVER_JOIN_TIMEOUT_MILLIS = 60_000;
    private static final Log LOG = LogFactory.getLog(FiberLifecycleFuzzTest.class);
    private static final int MAX_CARRIER_COUNT = 4;
    private static final int MAX_DRIVER_COUNT = 8;
    private static final int MAX_LIVE_FIBER_LIMIT = 4;
    private static final int MAX_TASK_COUNT = 16;
    private static final int MIN_DRIVER_COUNT = 2;
    private static final int OPS_PER_DRIVER = 10_000;
    private static final int ROUND_COUNT = 30;
    private static final long THREAD_JOIN_TIMEOUT_MILLIS = 10_000;

    @Test
    public void testQuiesceUnderLoad() throws Exception {
        runFuzz(TestUtils.generateRandom(LOG), true);
    }

    @Test
    public void testRandomLifecycleComposition() throws Exception {
        runFuzz(TestUtils.generateRandom(LOG), false);
    }

    private static Throwable addFailure(Throwable failure, Throwable next) {
        if (failure == null) {
            return next;
        }
        if (failure != next) {
            failure.addSuppressed(next);
        }
        return failure;
    }

    private static void awaitClosed(FiberRuntime runtime, AtomicReference<Throwable> firstError) {
        final long deadline = System.nanoTime() + AWAIT_CLOSED_TIMEOUT_NANOS;
        while (runtime.state() != FiberRuntimeState.CLOSED) {
            throwIfThreadFailed(firstError);
            final long now = System.nanoTime();
            if (now >= deadline) {
                Assert.fail("fiber runtime failed to drain after fuzz");
            }
            runtime.awaitClosed(Math.min(deadline, now + 10_000_000L));
        }
    }

    private static void awaitLatch(
            CountDownLatch latch,
            AtomicReference<Throwable> firstError,
            String message
    ) throws Exception {
        final long deadline = System.nanoTime() + THREAD_JOIN_TIMEOUT_MILLIS * 1_000_000L;
        while (!latch.await(10, TimeUnit.MILLISECONDS)) {
            throwIfThreadFailed(firstError);
            if (System.nanoTime() >= deadline) {
                Assert.fail(message);
            }
        }
    }

    private static Throwable cleanupRound(
            FiberRuntime runtime,
            ObjList<Thread> threads,
            AtomicBoolean isCarriersDone,
            AtomicBoolean isDriversDone,
            AtomicBoolean isStopRequested,
            CountDownLatch quiesceComplete,
            Throwable failure
    ) {
        isStopRequested.set(true);
        quiesceComplete.countDown();
        isDriversDone.set(true);
        try {
            runtime.beginQuiesce();
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }

        final long closeDeadline = System.nanoTime() + AWAIT_CLOSED_TIMEOUT_NANOS;
        try {
            while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < closeDeadline) {
                runtime.drain(64);
                Os.pause();
            }
            if (runtime.state() != FiberRuntimeState.CLOSED) {
                failure = addFailure(failure, new AssertionError("fiber runtime cleanup timed out"));
            }
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        } finally {
            isCarriersDone.set(true);
        }

        boolean isInterrupted = false;
        final long joinDeadline = System.nanoTime() + THREAD_JOIN_TIMEOUT_MILLIS * 1_000_000L;
        for (int i = 0, n = threads.size(); i < n; i++) {
            final Thread thread = threads.getQuick(i);
            while (thread.isAlive()) {
                final long remainingNanos = joinDeadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    failure = addFailure(
                            failure,
                            new AssertionError("fuzz thread cleanup timed out [name=" + thread.getName() + ']')
                    );
                    break;
                }
                try {
                    thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
                } catch (InterruptedException e) {
                    isInterrupted = true;
                }
            }
        }
        if (isInterrupted) {
            Thread.currentThread().interrupt();
        }

        if (runtime.state() == FiberRuntimeState.CLOSED) {
            try {
                runtime.closeAfterDrained();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        return failure;
    }

    private static void joinThreads(
            ObjList<Thread> threads,
            int lo,
            int hi,
            long timeoutMillis,
            AtomicReference<Throwable> firstError
    ) throws Exception {
        final long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        for (int i = lo; i < hi; i++) {
            final Thread thread = threads.getQuick(i);
            while (thread.isAlive()) {
                throwIfThreadFailed(firstError);
                final long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    Assert.fail("fuzz thread failed to stop [name=" + thread.getName() + ']');
                }
                thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
            }
        }
    }

    private static void runFuzz(Rnd masterRnd, boolean isQuiesceUnderLoad) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            runRound(
                    masterRnd,
                    0,
                    isQuiesceUnderLoad,
                    MAX_DRIVER_COUNT,
                    1,
                    1,
                    1,
                    1
            );
            for (int round = 1; round < ROUND_COUNT; round++) {
                final int driverCount = MIN_DRIVER_COUNT
                        + masterRnd.nextInt(MAX_DRIVER_COUNT - MIN_DRIVER_COUNT + 1);
                final int carrierCount = 1 + masterRnd.nextInt(MAX_CARRIER_COUNT);
                final int taskCount = 1 + masterRnd.nextInt(MAX_TASK_COUNT);
                final int maxLiveFiberCount = 1 + masterRnd.nextInt(MAX_LIVE_FIBER_LIMIT);
                final int retainedFiberCount = 1 + masterRnd.nextInt(maxLiveFiberCount);
                runRound(
                        masterRnd,
                        round,
                        isQuiesceUnderLoad,
                        driverCount,
                        carrierCount,
                        taskCount,
                        maxLiveFiberCount,
                        retainedFiberCount
                );
            }
        });
    }

    private static void runRound(
            Rnd masterRnd,
            int round,
            boolean isQuiesceUnderLoad,
            int driverCount,
            int carrierCount,
            int taskCount,
            int maxLiveFiberCount,
            int retainedFiberCount
    ) throws Exception {
        final long totalOps = (long) driverCount * OPS_PER_DRIVER;
        LOG.info().$("fuzz round [round=").$(round)
                .$(", drivers=").$(driverCount)
                .$(", carriers=").$(carrierCount)
                .$(", tasks=").$(taskCount)
                .$(", maxLive=").$(maxLiveFiberCount)
                .$(", retained=").$(retainedFiberCount)
                .$(", quiesceUnderLoad=").$(isQuiesceUnderLoad)
                .I$();

        final FiberRuntime runtime = new FiberRuntime(retainedFiberCount, maxLiveFiberCount);
        final FiberEventWaitQueue waitQueue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);
        final AtomicReference<Throwable> firstError = new AtomicReference<>();
        final AtomicBoolean isDriversDone = new AtomicBoolean();
        final AtomicBoolean isCarriersDone = new AtomicBoolean();
        final AtomicBoolean isStopRequested = new AtomicBoolean();
        final AtomicLong opCount = new AtomicLong();
        final CountDownLatch firstTaskStep = new CountDownLatch(1);
        final ProgressWakeProbe progressWakeProbe = new ProgressWakeProbe();

        final ObjList<FuzzTask> tasks = new ObjList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            tasks.add(new FuzzTask(
                    new Rnd(masterRnd.nextLong(), masterRnd.nextLong()),
                    waitQueue,
                    new FiberCancellationSignal(),
                    firstError,
                    firstTaskStep,
                    progressWakeProbe,
                    !isQuiesceUnderLoad && i == 0
            ));
        }

        final CountDownLatch started = new CountDownLatch(driverCount + carrierCount + 1);
        final CountDownLatch quiesceReady = new CountDownLatch(isQuiesceUnderLoad ? driverCount : 0);
        final CountDownLatch quiesceComplete = new CountDownLatch(isQuiesceUnderLoad ? 1 : 0);
        final ObjList<Thread> threads = new ObjList<>(driverCount + carrierCount + 1);

        for (int i = 0; i < driverCount; i++) {
            final Rnd rnd = new Rnd(masterRnd.nextLong(), masterRnd.nextLong());
            threads.add(thread("fiber-fuzz-r" + round + "-driver-" + i, firstError, started, () -> {
                final long[] staleGenerations = new long[taskCount];
                final long[] staleIncarnations = new long[taskCount];
                for (int op = 0; op < OPS_PER_DRIVER && !isStopRequested.get(); op++) {
                    if (isQuiesceUnderLoad && op == OPS_PER_DRIVER / 4) {
                        quiesceReady.countDown();
                        if (!quiesceComplete.await(THREAD_JOIN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                            throw new AssertionError("quiesce release timed out");
                        }
                        Assert.assertEquals(LaunchResult.QUIESCING, runtime.launch(new QuiesceProbeTask()));
                    }
                    final int taskIndex = rnd.nextInt(taskCount);
                    final FuzzTask task = tasks.getQuick(taskIndex);
                    switch (rnd.nextInt(13)) {
                        case 0, 1, 2, 3 -> runtime.launch(task);
                        case 4 -> runtime.launch(task, staleIncarnations[taskIndex]);
                        case 5 -> task.tryCancel();
                        case 6 -> task.signalAxisA(FiberTask.SIGNAL_READY);
                        case 7 -> task.signalAxisA(
                                rnd.nextBoolean() ? FiberTask.SIGNAL_CANCEL : FiberTask.SIGNAL_DISCONNECT
                        );
                        case 8 -> task.tryReopen();
                        case 9 -> task.cancellationSignal.cancel(task.cancellationSignal.getGeneration());
                        case 10 -> task.cancellationSignal.cancel(staleGenerations[taskIndex]);
                        case 11 -> {
                            staleGenerations[taskIndex] = task.cancellationSignal.getGeneration();
                            task.cancellationSignal.reopen();
                        }
                        default -> staleIncarnations[taskIndex] = task.getIncarnation();
                    }
                    opCount.incrementAndGet();
                }
            }));
        }

        for (int i = 0; i < carrierCount; i++) {
            threads.add(thread("fiber-fuzz-r" + round + "-carrier-" + i, firstError, started, () -> {
                while (!isCarriersDone.get()) {
                    if (runtime.drain(16) == 0) {
                        Os.pause();
                    }
                }
            }));
        }

        threads.add(thread("fiber-fuzz-r" + round + "-event", firstError, started, () -> {
            while (!isDriversDone.get()) {
                waitQueue.fire();
                Os.pause();
            }
        }));

        Throwable failure = null;
        try {
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new QuiesceProbeTask()));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(tasks.getQuick(0)));
            if (!isQuiesceUnderLoad) {
                Assert.assertEquals(1, runtime.drain(1));
                awaitLatch(progressWakeProbe.firstWaitArmed, firstError, "fuzz event wait failed to arm");
                waitQueue.fire();
                Assert.assertEquals(1, runtime.drain(1));
                awaitLatch(progressWakeProbe.firstProgressResume, firstError, "fuzz progress wake failed to resume");
                Assert.assertTrue(progressWakeProbe.progressWakeCount.get() > 0);
            }
            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).start();
            }
            awaitLatch(started, firstError, "fuzz threads failed to start");
            awaitLatch(firstTaskStep, firstError, "fuzz task failed to execute");

            if (isQuiesceUnderLoad) {
                awaitLatch(quiesceReady, firstError, "drivers failed to reach quiesce checkpoint");
                runtime.beginQuiesce();
                quiesceComplete.countDown();
            }

            joinThreads(threads, 0, driverCount, DRIVER_JOIN_TIMEOUT_MILLIS, firstError);
            isDriversDone.set(true);
            if (!isQuiesceUnderLoad) {
                runtime.beginQuiesce();
            }
            awaitClosed(runtime, firstError);
            isCarriersDone.set(true);
            joinThreads(threads, driverCount, threads.size(), THREAD_JOIN_TIMEOUT_MILLIS, firstError);
            throwIfThreadFailed(firstError);

            Assert.assertEquals(totalOps, opCount.get());
            Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(runtime.getCreatedFiberCount(), runtime.getRetiredFiberCount());
            Assert.assertEquals(0, runtime.getInlineSuspendViolationCount());
            Assert.assertEquals(0, runtime.getLaunchCount(LaunchResult.RESOURCE_FAILURE));
            Assert.assertTrue(runtime.getLaunchCount(LaunchResult.LAUNCHED) > 0);
            if (isQuiesceUnderLoad) {
                Assert.assertTrue(runtime.getLaunchCount(LaunchResult.QUIESCING) >= driverCount);
            }
            long stepCount = 0;
            for (int i = 0; i < taskCount; i++) {
                final FuzzTask task = tasks.getQuick(i);
                final int state = task.getScheduleState();
                Assert.assertTrue(
                        "task left in a non-settled state [state=" + state + ']',
                        state == FiberTask.STATE_IDLE
                                || state == FiberTask.STATE_DONE
                                || state == FiberTask.STATE_CANCELLED
                );
                Assert.assertEquals(0, task.exclusionViolationCount.get());
                Assert.assertEquals(0, task.reasonViolationCount.get());
                Assert.assertEquals(0, task.unmountedStepViolationCount.get());
                stepCount += task.stepCount.get();
            }
            Assert.assertTrue("fuzz did not exercise the runtime", stepCount > 0);
        } catch (Throwable th) {
            failure = th;
        } finally {
            failure = cleanupRound(
                    runtime,
                    threads,
                    isCarriersDone,
                    isDriversDone,
                    isStopRequested,
                    quiesceComplete,
                    failure
            );
        }
        final Throwable threadFailure = firstError.get();
        if (threadFailure != null) {
            final AssertionError wrapper = new AssertionError("fuzz thread failed", threadFailure);
            failure = addFailure(wrapper, failure);
        }
        throwFailure(failure);
    }

    private static Thread thread(
            String name,
            AtomicReference<Throwable> firstError,
            CountDownLatch started,
            FuzzThreadBody body
    ) {
        final Thread thread = new Thread(() -> {
            started.countDown();
            try {
                body.run();
            } catch (Throwable th) {
                firstError.compareAndSet(null, th);
            }
        }, name);
        thread.setDaemon(true);
        return thread;
    }

    private static void throwFailure(Throwable failure) throws Exception {
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new AssertionError(failure);
        }
    }

    private static void throwIfThreadFailed(AtomicReference<Throwable> firstError) {
        final Throwable failure = firstError.get();
        if (failure != null) {
            throw new AssertionError("fuzz thread failed", failure);
        }
    }

    @FunctionalInterface
    private interface FuzzThreadBody {
        void run() throws Exception;
    }

    private static class FuzzTask extends FiberTask {
        final FiberCancellationSignal cancellationSignal;
        final AtomicInteger exclusionViolationCount = new AtomicInteger();
        final AtomicInteger reasonViolationCount = new AtomicInteger();
        final AtomicLong stepCount = new AtomicLong();
        final AtomicInteger unmountedStepViolationCount = new AtomicInteger();
        private final AtomicReference<Throwable> firstError;
        private final CountDownLatch firstTaskStep;
        private final boolean isDeterministicFirstWait;
        private final AtomicBoolean isRunning = new AtomicBoolean();
        private final ProgressWakeProbe progressWakeProbe;
        private final Rnd rnd;
        private final FiberEventWaitQueue waitQueue;

        FuzzTask(
                Rnd rnd,
                FiberEventWaitQueue waitQueue,
                FiberCancellationSignal cancellationSignal,
                AtomicReference<Throwable> firstError,
                CountDownLatch firstTaskStep,
                ProgressWakeProbe progressWakeProbe,
                boolean isDeterministicFirstWait
        ) {
            this.cancellationSignal = cancellationSignal;
            this.firstError = firstError;
            this.firstTaskStep = firstTaskStep;
            this.isDeterministicFirstWait = isDeterministicFirstWait;
            this.progressWakeProbe = progressWakeProbe;
            this.rnd = rnd;
            this.waitQueue = waitQueue;
        }

        private boolean suspendOnEvent(boolean isDeterministicWait) {
            final Fiber fiber = Fiber.current();
            if (fiber == null || !Fiber.isMounted()) {
                unmountedStepViolationCount.incrementAndGet();
                if (isDeterministicWait) {
                    throw new AssertionError("deterministic event wait ran without a mounted fiber");
                }
                return true;
            }
            final boolean isCancellationRequested = !isDeterministicWait && rnd.nextBoolean();
            final long generation = cancellationSignal.getGeneration();
            final long token = fiber.tryBeginWaitBuild(isCancellationRequested ? 2 : 1);
            if (token == Fiber.TOKEN_REFUSED) {
                if (isDeterministicWait) {
                    throw new AssertionError("deterministic event wait was refused");
                }
                return true;
            }
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            try {
                if (!coordinator.armEvent(token, waitQueue)) {
                    if (isDeterministicWait) {
                        throw new AssertionError("deterministic event wait failed to arm");
                    }
                    return true;
                }
                if (isDeterministicWait) {
                    progressWakeProbe.firstWaitArmed.countDown();
                }
                boolean isCancellationArmed = false;
                if (isCancellationRequested) {
                    // a concurrent reopen() may have made the captured generation stale; that is a
                    // legal registration refusal, not a violation
                    isCancellationArmed = coordinator.armCancellation(token, cancellationSignal, generation);
                    if (!isCancellationArmed) {
                        return true;
                    }
                }
                final int reason = fiber.suspendWait(token);
                if (reason == FiberWaitCoordinator.REASON_PROGRESS) {
                    progressWakeProbe.progressWakeCount.incrementAndGet();
                    if (isDeterministicWait) {
                        progressWakeProbe.firstProgressResume.countDown();
                        return false;
                    }
                    return rnd.nextBoolean();
                }
                if (isDeterministicWait) {
                    throw new AssertionError("unexpected deterministic wake reason [reason=" + reason + ']');
                }
                if (reason == FiberWaitCoordinator.REASON_SHUTDOWN
                        || (reason == FiberWaitCoordinator.REASON_CANCEL && isCancellationArmed)) {
                    return true;
                }
                reasonViolationCount.incrementAndGet();
                return true;
            } finally {
                coordinator.teardownWait(token);
            }
        }

        @Override
        protected void onError(Throwable th) {
            firstError.compareAndSet(null, th);
        }

        @Override
        protected boolean runStep() {
            if (!isRunning.compareAndSet(false, true)) {
                exclusionViolationCount.incrementAndGet();
            }
            try {
                final long currentStepCount = stepCount.incrementAndGet();
                final boolean isDeterministicWait = isDeterministicFirstWait && currentStepCount == 1;
                firstTaskStep.countDown();
                if (isDeterministicWait) {
                    return suspendOnEvent(true);
                }
                final int dice = rnd.nextInt(10);
                if (dice < 3) {
                    return true;
                }
                if (dice < 6) {
                    return false;
                }
                return suspendOnEvent(false);
            } finally {
                isRunning.set(false);
            }
        }
    }

    private static class ProgressWakeProbe {
        private final CountDownLatch firstProgressResume = new CountDownLatch(1);
        private final CountDownLatch firstWaitArmed = new CountDownLatch(1);
        private final AtomicLong progressWakeCount = new AtomicLong();
    }

    private static class QuiesceProbeTask extends FiberTask {
        @Override
        protected boolean runStep() {
            return true;
        }
    }
}

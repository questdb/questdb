/*******************************************************************************
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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
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
 * Randomised composition over a FIBER_HOST WorkerPool with real Workers: external and owner-local
 * publication, stealing, park and wake, orphan recovery after an injected Worker death, and halt
 * under load. {@link FiberLifecycleFuzzTest} covers the standalone runtime with detached carriers.
 */
public class FiberPoolSchedulingFuzzTest {
    private static final long HALT_TIMEOUT_NANOS = 30_000_000_000L;
    private static final Log LOG = LogFactory.getLog(FiberPoolSchedulingFuzzTest.class);
    private static final int MAX_DRIVER_COUNT = 6;
    private static final int MAX_LIVE_FIBER_LIMIT = 64;
    private static final int MAX_MOUNT_BUDGET = 16;
    private static final int MAX_TASK_COUNT = 64;
    private static final int MAX_WORKER_COUNT = 4;
    private static final int MIN_DRIVER_COUNT = 2;
    private static final int OPS_PER_DRIVER = 20_000;
    private static final int ROUND_COUNT = 20;
    private static final long WAIT_TIMEOUT_MILLIS = 10_000;

    @Test
    public void testHaltUnderLoad() throws Exception {
        runFuzz(TestUtils.generateRandom(LOG), true);
    }

    @Test
    public void testRandomSchedulingComposition() throws Exception {
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

    private static void awaitKilledWorkerExit(FiberRuntime runtime, AtomicReference<Throwable> firstError) throws Exception {
        final long deadline = System.nanoTime() + WAIT_TIMEOUT_MILLIS * 1_000_000L;
        while (runtime.getOrphanedShardTransitionCount() == 0) {
            throwIfThreadFailed(firstError);
            if (System.nanoTime() >= deadline) {
                Assert.fail("killed Worker did not exit before the halt");
            }
            Os.pause();
        }
    }

    private static void awaitLatch(
            CountDownLatch latch,
            AtomicReference<Throwable> firstError,
            String message
    ) throws Exception {
        final long deadline = System.nanoTime() + WAIT_TIMEOUT_MILLIS * 1_000_000L;
        while (!latch.await(10, TimeUnit.MILLISECONDS)) {
            throwIfThreadFailed(firstError);
            if (System.nanoTime() >= deadline) {
                Assert.fail(message);
            }
        }
    }

    private static WorkerPoolConfiguration configuration(
            String poolName,
            int workerCount,
            int maxLiveFiberCount,
            int retainedFiberCount,
            int mountBudget
    ) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                return maxLiveFiberCount;
            }

            @Override
            public int getFiberMountBudget() {
                return mountBudget;
            }

            @Override
            public int getFiberRetainedCount() {
                return retainedFiberCount;
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
                return 0;
            }

            @Override
            public long getSleepTimeout() {
                return 1;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean haltOnError() {
                return true;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.FIBER_HOST;
            }
        };
    }

    private static void haltPool(WorkerPool pool, boolean isBoundedHalt) {
        if (isBoundedHalt) {
            Assert.assertTrue("bounded halt retained live pool resources", pool.haltWithin(HALT_TIMEOUT_NANOS));
        } else {
            pool.halt();
        }
    }

    private static Throwable joinThreads(ObjList<Thread> threads, Throwable failure) {
        boolean isInterrupted = false;
        final long deadline = System.nanoTime() + WAIT_TIMEOUT_MILLIS * 1_000_000L;
        for (int i = 0, n = threads.size(); i < n; i++) {
            final Thread thread = threads.getQuick(i);
            while (thread.isAlive()) {
                final long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    failure = addFailure(failure, new AssertionError("fuzz thread failed to stop [name=" + thread.getName() + ']'));
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
        return failure;
    }

    private static void joinThreads(
            ObjList<Thread> threads,
            int lo,
            int hi,
            AtomicReference<Throwable> firstError
    ) throws Exception {
        final long deadline = System.nanoTime() + WAIT_TIMEOUT_MILLIS * 1_000_000L;
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

    private static void runFuzz(Rnd masterRnd, boolean isHaltUnderLoad) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (int round = 0; round < ROUND_COUNT; round++) {
                runRound(masterRnd, round, isHaltUnderLoad);
            }
        });
    }

    private static void runRound(Rnd masterRnd, int round, boolean isHaltUnderLoad) throws Exception {
        final int workerCount = 1 + masterRnd.nextInt(MAX_WORKER_COUNT);
        final int driverCount = MIN_DRIVER_COUNT + masterRnd.nextInt(MAX_DRIVER_COUNT - MIN_DRIVER_COUNT + 1);
        final int taskCount = 1 + masterRnd.nextInt(MAX_TASK_COUNT);
        final int maxLiveFiberCount = 1 + masterRnd.nextInt(MAX_LIVE_FIBER_LIMIT);
        final int retainedFiberCount = 1 + masterRnd.nextInt(maxLiveFiberCount);
        final int mountBudget = 1 + masterRnd.nextInt(MAX_MOUNT_BUDGET);
        // a single worker cannot recover its own orphaned work; bounded halt would then retain resources
        final int killedWorkerId = workerCount > 1 && masterRnd.nextBoolean()
                ? masterRnd.nextInt(workerCount)
                : FiberRuntime.NO_WORKER;
        final boolean isBoundedHalt = masterRnd.nextBoolean();
        LOG.info().$("pool fuzz round [round=").$(round)
                .$(", workers=").$(workerCount)
                .$(", drivers=").$(driverCount)
                .$(", tasks=").$(taskCount)
                .$(", maxLive=").$(maxLiveFiberCount)
                .$(", retained=").$(retainedFiberCount)
                .$(", mountBudget=").$(mountBudget)
                .$(", killedWorker=").$(killedWorkerId)
                .$(", boundedHalt=").$(isBoundedHalt)
                .$(", haltUnderLoad=").$(isHaltUnderLoad)
                .I$();

        final WorkerPool pool = new WorkerPool(configuration(
                "fiber-pool-fuzz-r" + round,
                workerCount,
                maxLiveFiberCount,
                retainedFiberCount,
                mountBudget
        ));
        final FiberRuntime runtime = pool.getFiberRuntime();
        final FiberEventWaitQueue waitQueue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);
        final AtomicReference<Throwable> firstError = new AtomicReference<>();
        final AtomicBoolean isStopRequested = new AtomicBoolean();
        final AtomicBoolean isEventDone = new AtomicBoolean();
        final AtomicBoolean isKillRequested = new AtomicBoolean();
        final CountDownLatch killFired = new CountDownLatch(1);
        final AtomicBoolean isKilledWithLocalWork = new AtomicBoolean();
        final CountDownLatch haltReady = new CountDownLatch(1);
        final AtomicLong opCount = new AtomicLong();
        final AtomicLong ownerNestedLaunchCount = new AtomicLong();
        final AtomicLong capacityWaitCount = new AtomicLong();

        final ObjList<FuzzTask> tasks = new ObjList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            tasks.add(new FuzzTask(
                    new Rnd(masterRnd.nextLong(), masterRnd.nextLong()),
                    runtime,
                    tasks,
                    waitQueue,
                    new FiberCancellationSignal(),
                    firstError,
                    ownerNestedLaunchCount,
                    capacityWaitCount
            ));
        }

        if (killedWorkerId != FiberRuntime.NO_WORKER) {
            pool.assign(killedWorkerId, workerContext -> {
                if (isKillRequested.get()) {
                    for (int i = 0, n = tasks.size(); i < n; i++) {
                        tasks.getQuick(i).recordLaunch(runtime.launch(tasks.getQuick(i)));
                    }
                    // Local work is best-effort; failure injection must not depend on task availability.
                    final boolean hasLocalWork = runtime.getLocalQueueDepthForTesting(killedWorkerId) > 0;
                    isKilledWithLocalWork.set(hasLocalWork);
                    killFired.countDown();
                    throw new InjectedWorkerFailure();
                }
                return false;
            });
        }

        final CountDownLatch started = new CountDownLatch(driverCount + 1);
        final ObjList<Thread> threads = new ObjList<>(driverCount + 1);
        for (int i = 0; i < driverCount; i++) {
            final int driverId = i;
            final Rnd rnd = new Rnd(masterRnd.nextLong(), masterRnd.nextLong());
            threads.add(thread("fiber-pool-fuzz-r" + round + "-driver-" + i, firstError, started, () -> {
                final long[] staleIncarnations = new long[taskCount];
                try {
                    for (int op = 0; op < OPS_PER_DRIVER && !isStopRequested.get(); op++) {
                        if (driverId == 0) {
                            if (killedWorkerId != FiberRuntime.NO_WORKER && op == OPS_PER_DRIVER / 4) {
                                isKillRequested.set(true);
                                if (!killFired.await(WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                                    throw new AssertionError("injected worker failure did not fire");
                                }
                            }
                            if (op == OPS_PER_DRIVER / 2) {
                                haltReady.countDown();
                            }
                        }
                        final int taskIndex = rnd.nextInt(taskCount);
                        final FuzzTask task = tasks.getQuick(taskIndex);
                        switch (rnd.nextInt(12)) {
                            case 0, 1, 2, 3, 4 -> {
                                if (task.recordLaunch(runtime.launch(task)) == LaunchResult.SATURATED) {
                                    for (int pause = rnd.nextInt(64); pause > 0; pause--) {
                                        Os.pause();
                                    }
                                }
                            }
                            case 5 -> task.recordLaunch(runtime.launch(task, staleIncarnations[taskIndex]));
                            case 6 -> task.tryCancel();
                            case 7 -> task.signalAxisA(FiberTask.SIGNAL_READY);
                            case 8 -> task.tryReopen();
                            case 9 -> task.cancellationSignal.cancel(task.cancellationSignal.getGeneration());
                            case 10 -> task.cancellationSignal.reopen();
                            default -> staleIncarnations[taskIndex] = task.getIncarnation();
                        }
                        opCount.incrementAndGet();
                    }
                } finally {
                    haltReady.countDown();
                }
            }));
        }
        threads.add(thread("fiber-pool-fuzz-r" + round + "-event", firstError, started, () -> {
            while (!isEventDone.get()) {
                waitQueue.fire();
                Os.pause();
            }
        }));

        Throwable failure = null;
        try {
            pool.start();
            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).start();
            }
            awaitLatch(started, firstError, "fuzz threads failed to start");
            if (isHaltUnderLoad) {
                awaitLatch(haltReady, firstError, "driver failed to reach the halt checkpoint");
            } else {
                joinThreads(threads, 0, driverCount, firstError);
            }
            if (killedWorkerId != FiberRuntime.NO_WORKER) {
                // an exit observed after the runtime closes stops the shard instead of orphaning it
                awaitKilledWorkerExit(runtime, firstError);
            }
            haltPool(pool, isBoundedHalt);
            isStopRequested.set(true);
            isEventDone.set(true);
            joinThreads(threads, 0, threads.size(), firstError);
            throwIfThreadFailed(firstError);

            if (!isHaltUnderLoad) {
                Assert.assertEquals((long) driverCount * OPS_PER_DRIVER, opCount.get());
            }
            LOG.info().$("pool fuzz round done [round=").$(round)
                    .$(", launched=").$(runtime.getLaunchCount(LaunchResult.LAUNCHED))
                    .$(", saturated=").$(runtime.getLaunchCount(LaunchResult.SATURATED))
                    .$(", quiescing=").$(runtime.getLaunchCount(LaunchResult.QUIESCING))
                    .$(", mounts=").$(runtime.getMountCount())
                    .$(", local=").$(runtime.getLocalPublicationCount())
                    .$(", fallback=").$(runtime.getLocalFallbackPublicationCount())
                    .$(", global=").$(runtime.getGlobalPublicationCount())
                    .$(", stolen=").$(runtime.getStolenSelectionCount())
                    .$(", wakeClaims=").$(runtime.getWakeClaimCount())
                    .$(", orphaned=").$(runtime.getOrphanedShardTransitionCount())
                    .$(", recovered=").$(runtime.getOrphanedEntryRecoveryCount())
                    .$(", killedWithLocalWork=").$(isKilledWithLocalWork.get())
                    .$(", capacityWaits=").$(capacityWaitCount.get())
                    .I$();
            Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(runtime.getCreatedFiberCount(), runtime.getRetiredFiberCount());
            Assert.assertEquals(0, pool.getReadyWorkerCountForTesting());
            for (int i = 0; i < workerCount; i++) {
                Assert.assertEquals("local queue not drained [workerId=" + i + ']', 0, runtime.getLocalQueueDepthForTesting(i));
            }
            Assert.assertEquals(0, runtime.getInlineSuspendViolationCount());
            Assert.assertEquals(0, runtime.getLaunchCount(LaunchResult.RESOURCE_FAILURE));
            Assert.assertTrue(runtime.getLaunchCount(LaunchResult.LAUNCHED) > 0);

            final long localPublications = runtime.getLocalPublicationCount();
            final long fallbackPublications = runtime.getLocalFallbackPublicationCount();
            final long globalPublications = runtime.getGlobalPublicationCount();
            final long orphanTransitions = runtime.getOrphanedShardTransitionCount();
            // every LAUNCHED result publishes exactly once; resignals add more
            Assert.assertTrue(
                    localPublications + fallbackPublications + globalPublications >= runtime.getLaunchCount(LaunchResult.LAUNCHED)
            );
            if (ownerNestedLaunchCount.get() > 0) {
                Assert.assertTrue("owner publication never reached a local queue", localPublications + fallbackPublications > 0);
            }
            // every Worker exit may orphan its shard once; a killed Worker exits while the runtime is open
            Assert.assertTrue(orphanTransitions <= workerCount);
            if (killFired.getCount() == 0) {
                Assert.assertTrue("killed Worker did not orphan its shard", orphanTransitions >= 1);
                if (isKilledWithLocalWork.get()) {
                    // a dead owner's entries leave only through a steal: ordinary, orphan recovery, or the detached drain
                    Assert.assertTrue("orphaned local work was never taken", runtime.getStolenSelectionCount() >= 1);
                }
            }
            // wake attempts: one per external or fallback publication, one per orphan transition
            Assert.assertTrue(runtime.getWakeClaimCount() <= globalPublications + fallbackPublications + orphanTransitions);

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
                // a claim ends in onDone() or parks back to IDLE without a callback; abandonment precedes onDone()
                Assert.assertTrue("more terminal callbacks than claims", task.doneCount.get() <= task.launchedCount.get());
                Assert.assertTrue(task.abandonedCount.get() <= task.doneCount.get());
                stepCount += task.stepCount.get();
            }
            Assert.assertTrue("fuzz did not exercise the pool", stepCount > 0);
        } catch (Throwable th) {
            failure = th;
        } finally {
            isStopRequested.set(true);
            isEventDone.set(true);
            try {
                pool.halt();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
            failure = joinThreads(threads, failure);
        }
        final Throwable threadFailure = firstError.get();
        if (threadFailure != null) {
            failure = addFailure(new AssertionError("fuzz thread failed", threadFailure), failure);
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

    // stackless: the Worker exit window must stay short so peers see the orphaned local work
    private static final class InjectedWorkerFailure extends RuntimeException {
        InjectedWorkerFailure() {
            super("injected worker failure", null, false, false);
        }
    }

    private static class FuzzTask extends FiberTask {
        final AtomicLong abandonedCount = new AtomicLong();
        final FiberCancellationSignal cancellationSignal;
        final AtomicLong doneCount = new AtomicLong();
        final AtomicInteger exclusionViolationCount = new AtomicInteger();
        final AtomicLong launchedCount = new AtomicLong();
        final AtomicInteger reasonViolationCount = new AtomicInteger();
        final AtomicLong stepCount = new AtomicLong();
        final AtomicInteger unmountedStepViolationCount = new AtomicInteger();
        private final AtomicLong capacityWaitCount;
        private final AtomicReference<Throwable> firstError;
        private final AtomicBoolean isRunning = new AtomicBoolean();
        private final AtomicLong ownerNestedLaunchCount;
        private final Rnd rnd;
        private final FiberRuntime runtime;
        private final ObjList<FuzzTask> tasks;
        private final FiberEventWaitQueue waitQueue;

        FuzzTask(
                Rnd rnd,
                FiberRuntime runtime,
                ObjList<FuzzTask> tasks,
                FiberEventWaitQueue waitQueue,
                FiberCancellationSignal cancellationSignal,
                AtomicReference<Throwable> firstError,
                AtomicLong ownerNestedLaunchCount,
                AtomicLong capacityWaitCount
        ) {
            this.cancellationSignal = cancellationSignal;
            this.capacityWaitCount = capacityWaitCount;
            this.firstError = firstError;
            this.ownerNestedLaunchCount = ownerNestedLaunchCount;
            this.rnd = rnd;
            this.runtime = runtime;
            this.tasks = tasks;
            this.waitQueue = waitQueue;
        }

        LaunchResult recordLaunch(LaunchResult result) {
            if (result == LaunchResult.LAUNCHED) {
                launchedCount.incrementAndGet();
            }
            return result;
        }

        private void launchPeer() {
            final FuzzTask peer = tasks.getQuick(rnd.nextInt(tasks.size()));
            final boolean isOwnerCarrier = Worker.current() != null;
            LaunchResult result = peer.recordLaunch(runtime.launch(peer));
            if (result == LaunchResult.SATURATED && rnd.nextBoolean()) {
                capacityWaitCount.incrementAndGet();
                final int reason = runtime.awaitCapacity();
                if (reason != FiberWaitCoordinator.REASON_CAPACITY && reason != FiberWaitCoordinator.REASON_SHUTDOWN) {
                    reasonViolationCount.incrementAndGet();
                    return;
                }
                result = peer.recordLaunch(runtime.launch(peer));
            }
            if (result == LaunchResult.LAUNCHED && isOwnerCarrier) {
                ownerNestedLaunchCount.incrementAndGet();
            }
        }

        private boolean suspendOnEvent() {
            final Fiber fiber = Fiber.current();
            if (fiber == null || !Fiber.isMounted()) {
                unmountedStepViolationCount.incrementAndGet();
                return true;
            }
            final boolean isCancellationRequested = rnd.nextBoolean();
            final long generation = cancellationSignal.getGeneration();
            final long token = fiber.tryBeginWaitBuild(isCancellationRequested ? 2 : 1);
            if (token == Fiber.TOKEN_REFUSED) {
                return true;
            }
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            try {
                if (!coordinator.armEvent(token, waitQueue)) {
                    return true;
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
                    return rnd.nextBoolean();
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
        protected void onAbandoned() {
            abandonedCount.incrementAndGet();
        }

        @Override
        protected void onDone() {
            doneCount.incrementAndGet();
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
                stepCount.incrementAndGet();
                final int dice = rnd.nextInt(10);
                if (dice < 2) {
                    launchPeer();
                    return rnd.nextBoolean();
                }
                if (dice < 5) {
                    return true;
                }
                if (dice < 7) {
                    return false;
                }
                return suspendOnEvent();
            } finally {
                isRunning.set(false);
            }
        }
    }
}

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

package io.questdb.cairo.sql.async;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberEventWaitQueue;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeConfigurationListener;
import io.questdb.mp.continuation.FiberRuntimeQuiesceListener;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class PageFrameReduceDispatcher implements FiberRuntimeConfigurationListener, FiberRuntimeQuiesceListener, QuietCloseable {
    static final int DEFAULT_BATCH_LIMIT = 64;
    private static final Log LOG = LogFactory.getLog(PageFrameReduceDispatcher.class);
    private static final long PUBLICATION_OPEN = Long.MIN_VALUE;
    private static final long PUBLICATION_PERMIT_MASK = Long.MAX_VALUE;
    private static final int QUIESCE_DRAINED = 3;
    private static final int QUIESCE_DRAINING = 2;
    private static final int QUIESCE_OPEN = 0;
    private static final int QUIESCE_REQUESTED = 1;
    private final long configuredBatchRowBudget;
    private final MessageBus messageBus;
    private final AtomicLong progressVersion = new AtomicLong();
    private final FiberEventWaitQueue progressWaitQueue =
            new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);
    private final AtomicLong publicationAdmission = new AtomicLong(PUBLICATION_OPEN);
    private final AtomicInteger quiesceState = new AtomicInteger(QUIESCE_OPEN);
    private final FiberRuntime runtime;
    private final FiberTaskPool<PageFrameFiberTask> taskPool;
    private final MillisecondClock timerClock;
    private final long timerIntervalMillis;
    private final TimerShards timerShards;
    private volatile int batchLimit = DEFAULT_BATCH_LIMIT;
    private volatile long batchRowBudget;
    private volatile boolean isClosed;

    public PageFrameReduceDispatcher(CairoEngine engine, MessageBus messageBus, FiberRuntime runtime) {
        // Stop each batch after it reaches one configured-max-frame's row count.
        this.configuredBatchRowBudget = engine.getConfiguration().getSqlPageFrameMaxRows();
        this.batchRowBudget = configuredBatchRowBudget;
        this.messageBus = messageBus;
        this.runtime = runtime;
        this.taskPool = new FiberTaskPool<>(
                runtime.getMaxLiveFiberCount(),
                runtime.getMaxRetainedFiberCount(),
                pool -> new PageFrameFiberTask(engine, pool, this)
        );
        this.timerClock = engine.getConfiguration().getMillisecondClock();
        this.timerIntervalMillis = Math.max(
                1,
                engine.getConfiguration().getQueryContinuationWakeIntervalMillis()
        );
        this.timerShards = engine.getTimerShards();
        boolean isConfigurationListenerRegistered = false;
        boolean isQuiesceListenerRegistered = false;
        try {
            runtime.registerConfigurationListener(this);
            isConfigurationListenerRegistered = true;
            runtime.registerQuiesceListener(this);
            isQuiesceListenerRegistered = true;
        } catch (Throwable th) {
            if (isQuiesceListenerRegistered) {
                try {
                    runtime.unregisterQuiesceListener(this);
                } catch (Throwable cleanupFailure) {
                    if (cleanupFailure != th) {
                        th.addSuppressed(cleanupFailure);
                    }
                }
            }
            if (isConfigurationListenerRegistered) {
                try {
                    runtime.unregisterConfigurationListener(this);
                } catch (Throwable cleanupFailure) {
                    if (cleanupFailure != th) {
                        th.addSuppressed(cleanupFailure);
                    }
                }
            }
            try {
                taskPool.close();
            } catch (Throwable cleanupFailure) {
                if (cleanupFailure != th) {
                    th.addSuppressed(cleanupFailure);
                }
            }
            throw th;
        }
    }

    @TestOnly
    public TaskLeaseForTesting acquireTaskLeaseForTesting() {
        if (!taskPool.tryLease()) {
            throw new IllegalStateException("page frame fiber task lease unavailable");
        }
        try {
            return new TaskLeaseForTesting(taskPool);
        } catch (Throwable th) {
            try {
                taskPool.releaseLease();
            } catch (Throwable cleanupFailure) {
                suppressCleanupFailure(th, cleanupFailure);
            }
            throw th;
        }
    }

    public int awaitProgress(
            long observedVersion,
            @Nullable FiberCancellationSignal cancellationSignal
    ) {
        return awaitProgress(
                observedVersion,
                cancellationSignal,
                cancellationSignal != null ? cancellationSignal.getGeneration() : CancellationBinding.NO_GENERATION
        );
    }

    @TestOnly
    public int awaitProgress(
            PageFrameSequence<?> frameSequence,
            long observedSequenceVersion,
            long observedGlobalVersion,
            @Nullable FiberCancellationSignal cancellationSignal
    ) {
        return awaitProgress(
                frameSequence,
                observedSequenceVersion,
                observedGlobalVersion,
                cancellationSignal,
                cancellationSignal != null ? cancellationSignal.getGeneration() : CancellationBinding.NO_GENERATION,
                null,
                CancellationBinding.NO_GENERATION,
                false
        );
    }

    @TestOnly
    public int awaitProgress(
            PageFrameSequence<?> frameSequence,
            long observedSequenceVersion,
            long observedGlobalVersion,
            @Nullable FiberCancellationSignal cancellationSignal,
            @Nullable FiberCancellationSignal supplementalCancellationSignal
    ) {
        return awaitProgress(
                frameSequence,
                observedSequenceVersion,
                observedGlobalVersion,
                cancellationSignal,
                cancellationSignal != null ? cancellationSignal.getGeneration() : CancellationBinding.NO_GENERATION,
                supplementalCancellationSignal,
                supplementalCancellationSignal != null
                        ? supplementalCancellationSignal.getGeneration()
                        : CancellationBinding.NO_GENERATION,
                false
        );
    }

    public int awaitProgress(
            long observedVersion,
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration
    ) {
        return awaitProgress(
                observedVersion,
                cancellationSignal,
                cancellationSignalGeneration,
                false
        );
    }

    @Override
    public void beginQuiesce() {
        if (!quiesceState.compareAndSet(QUIESCE_OPEN, QUIESCE_REQUESTED)) {
            return;
        }
        progressWaitQueue.fireAll();
        while (true) {
            final long current = publicationAdmission.get();
            if ((current & PUBLICATION_OPEN) == 0
                    || publicationAdmission.compareAndSet(current, current & PUBLICATION_PERMIT_MASK)) {
                return;
            }
        }
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        beginQuiesce();
        progressQuiesce();
        final boolean isDrained = isQuiesced();
        // close() runs once from the pool's freeOnExit sweep; a throw-first ordering would leak
        // the task pool's native state for good
        isClosed = true;
        if (messageBus.getPageFrameReduceDispatcher() == this) {
            messageBus.setPageFrameReduceDispatcher(null);
        }
        runtime.unregisterConfigurationListener(this);
        runtime.unregisterQuiesceListener(this);
        try {
            progressWaitQueue.shutdown();
        } finally {
            taskPool.close();
        }
        if (!isDrained) {
            throw new IllegalStateException("page frame reduce dispatcher has active publications");
        }
    }

    @TestOnly
    public void closeTaskPoolForTesting() {
        taskPool.close();
    }

    public boolean consumeOrdered(
            int workerId,
            RingQueue<PageFrameReduceTask> queue,
            MCSequence subSeq,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        if (hasNoPendingTasks(subSeq)) {
            return true;
        }
        final Fiber fiber = reserveFiber(stealingFrameSequence);
        if (fiber == null) {
            return true;
        }
        final long reservationEpoch = fiber.getReservationEpoch();
        if (!tryLeaseTask(fiber, reservationEpoch)) {
            return true;
        }
        PageFrameFiberTask fiberTask = null;
        boolean hasLaunchOwnership = true;
        try {
            do {
                final long cursor = subSeq.next();
                if (cursor > -1) {
                    final PageFrameReduceTask reduceTask = queue.get(cursor);
                    final PageFrameSequence<?> frameSequence = reduceTask.getFrameSequence();
                    try {
                        fiberTask = taskPool.acquireLeased();
                    } catch (Throwable th) {
                        completeFailedOrderedAcquisition(subSeq, cursor, reduceTask, frameSequence, th);
                        throw th;
                    }
                    fiberTask.ofOrdered(
                            workerId,
                            queue,
                            subSeq,
                            cursor,
                            reduceTask,
                            frameSequence
                    );
                    hasLaunchOwnership = false;
                    launch(fiber, reservationEpoch, fiberTask, workerId > -1);
                    return false;
                }
                if (cursor == -1) {
                    return true;
                }
                Os.pause();
            } while (true);
        } finally {
            try {
                if (hasLaunchOwnership) {
                    if (fiberTask != null) {
                        taskPool.release(fiberTask);
                    } else {
                        taskPool.releaseLease();
                    }
                }
            } finally {
                runtime.releaseReservedFiber(fiber, reservationEpoch);
            }
        }
    }

    public boolean consumeUnordered(
            int workerId,
            RingQueue<UnorderedPageFrameReduceTask> queue,
            MCSequence subSeq,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        if (hasNoPendingTasks(subSeq)) {
            return true;
        }
        final Fiber fiber = reserveFiber(stealingFrameSequence);
        if (fiber == null) {
            return true;
        }
        final long reservationEpoch = fiber.getReservationEpoch();
        if (!tryLeaseTask(fiber, reservationEpoch)) {
            return true;
        }
        PageFrameFiberTask fiberTask = null;
        boolean hasLaunchOwnership = true;
        try {
            do {
                final long cursor = subSeq.next();
                if (cursor > -1) {
                    final UnorderedPageFrameReduceTask reduceTask = queue.get(cursor);
                    final UnorderedPageFrameSequence<?> frameSequence = reduceTask.getFrameSequence();
                    final int frameIndex = reduceTask.getFrameIndex();
                    final long frameSequenceId = reduceTask.getFrameSequenceId();
                    reduceTask.clear();
                    subSeq.done(cursor);
                    signalProgress(frameSequence);
                    if (frameSequenceId != frameSequence.getId()) {
                        LOG.error()
                                .$("skipping stale task [expected=").$(frameSequence.getId())
                                .$(", got=").$(frameSequenceId)
                                .I$();
                        return false;
                    }
                    try {
                        fiberTask = taskPool.acquireLeased();
                    } catch (Throwable th) {
                        completeFailedUnorderedAcquisition(frameSequence, th);
                        throw th;
                    }
                    fiberTask.ofUnordered(workerId, queue, subSeq, frameIndex, frameSequence);
                    hasLaunchOwnership = false;
                    launch(fiber, reservationEpoch, fiberTask, workerId > -1);
                    return false;
                }
                if (cursor == -1) {
                    return true;
                }
                Os.pause();
            } while (true);
        } finally {
            try {
                if (hasLaunchOwnership) {
                    if (fiberTask != null) {
                        taskPool.release(fiberTask);
                    } else {
                        taskPool.releaseLease();
                    }
                }
            } finally {
                runtime.releaseReservedFiber(fiber, reservationEpoch);
            }
        }
    }

    public int getBatchLimit() {
        return batchLimit;
    }

    public long getBatchRowBudget() {
        return batchRowBudget;
    }

    @TestOnly
    public int getCreatedTaskCount() {
        return taskPool.getCreatedCount();
    }

    public long getProgressVersion() {
        return progressVersion.get();
    }

    @TestOnly
    public int getTaskCapacity() {
        return taskPool.getCapacity();
    }

    @TestOnly
    public int getTaskMaxRetainedCount() {
        return taskPool.getMaxRetainedCount();
    }

    public boolean isCurrentFiberOwned() {
        return runtime.isCurrentFiberOwned();
    }

    @Override
    public boolean isQuiesced() {
        return quiesceState.get() == QUIESCE_DRAINED;
    }

    @Override
    public void onConfigurationChanged(int maxLiveFiberCount, int maxRetainedFiberCount) {
        taskPool.updateLimits(maxLiveFiberCount, maxRetainedFiberCount);
    }

    @Override
    public void progressQuiesce() {
        if (publicationAdmission.get() != 0
                || !quiesceState.compareAndSet(QUIESCE_REQUESTED, QUIESCE_DRAINING)) {
            return;
        }
        boolean isDrained = false;
        try {
            isDrained = drainPublishedTasks() && taskPool.hasNoLeasedTasks();
        } finally {
            quiesceState.set(isDrained ? QUIESCE_DRAINED : QUIESCE_REQUESTED);
        }
    }

    public void releasePublication() {
        final long current = publicationAdmission.decrementAndGet();
        if ((current & PUBLICATION_PERMIT_MASK) == PUBLICATION_PERMIT_MASK) {
            publicationAdmission.incrementAndGet();
            throw new IllegalStateException("page frame publication admission underflow");
        }
    }

    @TestOnly
    public void releaseTaskLeaseForTesting() {
        taskPool.releaseLease();
    }

    @TestOnly
    public void runWithTaskPoolLockedForTesting(Runnable action) {
        synchronized (taskPool) {
            action.run();
        }
    }

    @TestOnly
    public void setBatchRowBudgetForTesting(long batchRowBudget) {
        this.batchRowBudget = batchRowBudget > 0 ? batchRowBudget : configuredBatchRowBudget;
    }

    @TestOnly
    public void setFreeTaskScheduleStateForTesting(int expectedState, int targetState) {
        taskPool.setFreeTaskScheduleStateForTesting(expectedState, targetState);
    }

    @TestOnly
    public void signalProgressForTesting(PageFrameSequence<?> frameSequence) {
        signalProgress(frameSequence);
    }

    public boolean tryAcquirePublication() {
        if (runtime.isCurrentFiberOwned()) {
            return false;
        }
        while (true) {
            final long current = publicationAdmission.get();
            if ((current & PUBLICATION_OPEN) == 0) {
                return false;
            }
            if ((current & PUBLICATION_PERMIT_MASK) == PUBLICATION_PERMIT_MASK) {
                throw new IllegalStateException("page frame publication admission overflow");
            }
            if (publicationAdmission.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    @TestOnly
    public boolean tryLeaseTaskForTesting() {
        return taskPool.tryLease();
    }

    @TestOnly
    public static final class TaskLeaseForTesting {
        private final AtomicBoolean isReleased = new AtomicBoolean();
        private final PageFrameFiberTask task;
        private final FiberTaskPool<PageFrameFiberTask> taskPool;

        private TaskLeaseForTesting(FiberTaskPool<PageFrameFiberTask> taskPool) {
            this.taskPool = taskPool;
            this.task = taskPool.acquireLeased();
        }

        public void release() {
            if (!isReleased.compareAndSet(false, true)) {
                throw new IllegalStateException("page frame fiber task lease already released");
            }
            taskPool.release(task);
        }
    }

    private static boolean hasNoPendingTasks(MCSequence subSeq) {
        return subSeq.current() >= subSeq.getBarrier().current();
    }

    private static IllegalStateException launchFailed(LaunchResult result) {
        return new IllegalStateException("page frame fiber launch failed [result=" + result + ']');
    }

    private static void suppressCleanupFailure(Throwable failure, Throwable cleanupFailure) {
        if (failure != cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private int awaitProgress(
            long observedVersion,
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration,
            boolean isQuiescingAllowed
    ) {
        if (isClosed || (!isQuiescingAllowed && quiesceState.get() != QUIESCE_OPEN)) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        if (!Fiber.isMounted() || !SuspensionScope.isFiberMode()) {
            return FiberWaitCoordinator.REASON_NONE;
        }
        final Fiber fiber = Fiber.current();
        if (fiber == null) {
            return FiberWaitCoordinator.REASON_NONE;
        }
        final long token = fiber.tryBeginWaitBuild(cancellationSignal == null ? 2 : 3);
        if (token == Fiber.TOKEN_REFUSED) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        try {
            if (!coordinator.armEvent(token, progressWaitQueue)) {
                throw new IllegalStateException("page frame progress wait registration failed");
            }
            if (cancellationSignal != null
                    && !coordinator.armCancellation(token, cancellationSignal, cancellationSignalGeneration)) {
                throw new IllegalStateException("page frame progress cancellation registration failed");
            }
            if (!coordinator.armTimer(token, timerShards, timerClock, timerIntervalMillis)) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            if (isClosed || (!isQuiescingAllowed && quiesceState.get() != QUIESCE_OPEN)) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            if (progressVersion.get() != observedVersion) {
                return coordinator.preferPendingCancel(token, FiberWaitCoordinator.REASON_PROGRESS);
            }
            return fiber.suspendWait(token, FiberWaitCoordinator.REASON_PROGRESS);
        } finally {
            coordinator.teardownWait(token);
        }
    }

    private int awaitProgress(
            AbstractPageFrameSequence frameSequence,
            long observedSequenceVersion,
            long observedGlobalVersion,
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration,
            @Nullable FiberCancellationSignal supplementalCancellationSignal,
            long supplementalCancellationSignalGeneration,
            boolean isQuiescingAllowed
    ) {
        if (isClosed || (!isQuiescingAllowed && quiesceState.get() != QUIESCE_OPEN)) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        if (!Fiber.isMounted() || !SuspensionScope.isFiberMode()) {
            return FiberWaitCoordinator.REASON_NONE;
        }
        final Fiber fiber = Fiber.current();
        if (fiber == null) {
            return FiberWaitCoordinator.REASON_NONE;
        }
        final long token = fiber.tryBeginWaitBuild(
                3
                        + (cancellationSignal != null ? 1 : 0)
                        + (supplementalCancellationSignal != null ? 1 : 0)
        );
        if (token == Fiber.TOKEN_REFUSED) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        try {
            if (!coordinator.armEvent(token, frameSequence.getProgressWaitQueue())
                    || !coordinator.armEvent(token, progressWaitQueue)) {
                throw new IllegalStateException("page frame progress wait registration failed");
            }
            if (cancellationSignal != null
                    && !coordinator.armCancellation(token, cancellationSignal, cancellationSignalGeneration)) {
                throw new IllegalStateException("page frame progress cancellation registration failed");
            }
            if (supplementalCancellationSignal != null
                    && !coordinator.armCancellation(
                    token,
                    supplementalCancellationSignal,
                    supplementalCancellationSignalGeneration
            )) {
                throw new IllegalStateException("page frame supplemental cancellation registration failed");
            }
            if (!coordinator.armTimer(token, timerShards, timerClock, timerIntervalMillis)) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            if (isClosed || (!isQuiescingAllowed && quiesceState.get() != QUIESCE_OPEN)) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            if (frameSequence.getProgressVersion() != observedSequenceVersion
                    || progressVersion.get() != observedGlobalVersion) {
                return coordinator.preferPendingCancel(token, FiberWaitCoordinator.REASON_PROGRESS);
            }
            return fiber.suspendWait(token, FiberWaitCoordinator.REASON_PROGRESS);
        } finally {
            coordinator.teardownWait(token);
        }
    }

    private boolean drainOrdered(RingQueue<PageFrameReduceTask> queue, MCSequence subSeq) {
        while (true) {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final PageFrameReduceTask task = queue.get(cursor);
                final PageFrameSequence<?> frameSequence = task.getFrameSequence();
                if (frameSequence.isActive()
                        && frameSequence.cancelIfChanged(SqlExecutionCircuitBreaker.STATE_CANCELLED)) {
                    LOG.info().$("cancelling in-flight query, dispatcher is quiescing [frameSequenceId=")
                            .$(frameSequence.getId()).I$();
                }
                try {
                    subSeq.done(cursor);
                } finally {
                    frameSequence.getReduceFinishedCounter().incrementAndGet();
                    signalProgress(frameSequence);
                }
            } else {
                return cursor == -1;
            }
        }
    }

    private boolean drainPublishedTasks() {
        for (int shard = 0, n = messageBus.getPageFrameReduceShardCount(); shard < n; shard++) {
            if (!drainOrdered(
                    messageBus.getPageFrameReduceQueue(shard),
                    messageBus.getPageFrameReduceSubSeq(shard)
            )) {
                return false;
            }
        }
        return drainUnordered(
                messageBus.getUnorderedPageFrameReduceQueue(),
                messageBus.getUnorderedPageFrameReduceSubSeq()
        );
    }

    private boolean drainUnordered(RingQueue<UnorderedPageFrameReduceTask> queue, MCSequence subSeq) {
        while (true) {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final UnorderedPageFrameReduceTask task = queue.get(cursor);
                final UnorderedPageFrameSequence<?> frameSequence = task.getFrameSequence();
                final long frameSequenceId = task.getFrameSequenceId();
                task.clear();
                subSeq.done(cursor);
                signalProgress(frameSequence);
                if (frameSequenceId == frameSequence.getId()) {
                    if (frameSequence.isActive()
                            && frameSequence.cancelIfChanged(SqlExecutionCircuitBreaker.STATE_CANCELLED)) {
                        LOG.info().$("cancelling in-flight query, dispatcher is quiescing [frameSequenceId=")
                                .$(frameSequenceId).I$();
                    }
                    frameSequence.getDoneLatch().countDown();
                }
            } else {
                return cursor == -1;
            }
        }
    }

    private void completeFailedOrderedAcquisition(
            MCSequence subSeq,
            long cursor,
            PageFrameReduceTask reduceTask,
            PageFrameSequence<?> frameSequence,
            Throwable failure
    ) {
        try {
            if (frameSequence.isReducerFailureReportable(failure)) {
                reduceTask.setErrorMsg(failure);
                frameSequence.cancelOnReducerError(failure);
            }
        } catch (Throwable cleanupFailure) {
            suppressCleanupFailure(failure, cleanupFailure);
        }
        try {
            subSeq.done(cursor);
        } catch (Throwable cleanupFailure) {
            suppressCleanupFailure(failure, cleanupFailure);
        }
        try {
            frameSequence.getReduceFinishedCounter().incrementAndGet();
        } catch (Throwable cleanupFailure) {
            suppressCleanupFailure(failure, cleanupFailure);
        }
        try {
            signalProgress(frameSequence);
        } catch (Throwable cleanupFailure) {
            suppressCleanupFailure(failure, cleanupFailure);
        }
    }

    private void completeFailedUnorderedAcquisition(
            UnorderedPageFrameSequence<?> frameSequence,
            Throwable failure
    ) {
        try {
            if (frameSequence.isReducerFailureReportable(failure)) {
                frameSequence.setError(failure);
            }
        } catch (Throwable cleanupFailure) {
            suppressCleanupFailure(failure, cleanupFailure);
        }
        try {
            frameSequence.getDoneLatch().countDown();
        } catch (Throwable cleanupFailure) {
            suppressCleanupFailure(failure, cleanupFailure);
        }
        try {
            frameSequence.signalProgress();
        } catch (Throwable cleanupFailure) {
            suppressCleanupFailure(failure, cleanupFailure);
        }
    }

    private void launch(
            Fiber fiber,
            long reservationEpoch,
            PageFrameFiberTask task,
            boolean isDirectMountAllowed
    ) {
        final long taskIncarnation = task.getIncarnation();
        final LaunchResult result = isDirectMountAllowed && !Fiber.isMounted()
                ? runtime.launchReservedDirect(fiber, reservationEpoch, task, taskIncarnation)
                : runtime.launchReserved(fiber, reservationEpoch, task, taskIncarnation);
        // launchReserved() may already have run the terminal callbacks, which recycle the task and bump
        // its incarnation; another carrier can then own it. Only abort a task still at our incarnation.
        if (result != LaunchResult.LAUNCHED
                && task.getIncarnation() == taskIncarnation
                && task.isBound()) {
            task.abortBeforeLaunch();
        }
        if (result != LaunchResult.LAUNCHED && result != LaunchResult.QUIESCING) {
            throw launchFailed(result);
        }
    }

    private @Nullable Fiber reserveFiber(@Nullable AbstractPageFrameSequence stealingFrameSequence) {
        final boolean isDraining = stealingFrameSequence != null && !stealingFrameSequence.isActive();
        FiberCancellationSignal cancellationSignal = isDraining
                ? null
                : SuspensionScope.getCancellationSignal();
        long cancellationSignalGeneration = isDraining
                ? CancellationBinding.NO_GENERATION
                : SuspensionScope.getCancellationSignalGeneration();
        if (cancellationSignal == null && !isDraining && stealingFrameSequence != null) {
            final CancellationBinding cancellationBinding = SuspensionScope.getCancellationBindingScratch();
            stealingFrameSequence.getCircuitBreaker().copyCancelledFlagTo(cancellationBinding);
            final AtomicBoolean cancelledFlag = cancellationBinding.getFlag();
            if (cancelledFlag instanceof FiberCancellationSignal signal) {
                cancellationSignal = signal;
                cancellationSignalGeneration = cancellationBinding.getGeneration(cancelledFlag);
            }
        }
        while (true) {
            final Fiber fiber = runtime.tryReserveFiber();
            if (fiber != null || !SuspensionScope.isFiberMode() || !Fiber.isMounted()) {
                return fiber;
            }
            final int reason = runtime.awaitCapacity(
                    cancellationSignal,
                    cancellationSignalGeneration
            );
            if (reason == FiberWaitCoordinator.REASON_CAPACITY) {
                continue;
            }
            return null;
        }
    }

    private boolean tryLeaseTask(Fiber fiber, long reservationEpoch) {
        final boolean isLeased = taskPool.tryLease();
        if (!isLeased) {
            runtime.releaseReservedFiber(fiber, reservationEpoch);
        }
        return isLeased;
    }

    boolean isProgressWaitTerminated(
            AbstractPageFrameSequence frameSequence,
            long observedSequenceVersion,
            long observedGlobalVersion,
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration,
            @Nullable FiberCancellationSignal supplementalCancellationSignal,
            long supplementalCancellationSignalGeneration,
            @Nullable SqlExecutionCircuitBreaker circuitBreaker,
            boolean isDraining
    ) {
        final int reason = awaitProgress(
                frameSequence,
                observedSequenceVersion,
                observedGlobalVersion,
                cancellationSignal,
                cancellationSignalGeneration,
                supplementalCancellationSignal,
                supplementalCancellationSignalGeneration,
                isDraining
        );
        return switch (reason) {
            case FiberWaitCoordinator.REASON_CANCEL -> {
                if (circuitBreaker != null) {
                    circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                }
                yield true;
            }
            case FiberWaitCoordinator.REASON_NONE, FiberWaitCoordinator.REASON_SHUTDOWN -> true;
            case FiberWaitCoordinator.REASON_PROGRESS -> false;
            case FiberWaitCoordinator.REASON_TIMER -> {
                if (circuitBreaker != null) {
                    circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                }
                yield false;
            }
            default -> throw new IllegalStateException(
                    "unexpected page frame progress wait reason [reason=" + reason + ']'
            );
        };
    }

    boolean isQuiescing() {
        return quiesceState.get() != QUIESCE_OPEN;
    }

    void signalProgress() {
        progressVersion.incrementAndGet();
        progressWaitQueue.fire();
    }

    void signalProgress(AbstractPageFrameSequence frameSequence) {
        signalProgress();
        frameSequence.signalProgress();
    }

}

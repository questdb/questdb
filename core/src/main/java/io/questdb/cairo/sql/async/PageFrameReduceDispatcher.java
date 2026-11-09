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
import io.questdb.mp.continuation.FiberDispatchContext;
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
import java.util.concurrent.atomic.AtomicLongArray;

public final class PageFrameReduceDispatcher implements FiberRuntimeConfigurationListener, FiberRuntimeQuiesceListener, QuietCloseable {
    static final long DEFAULT_BATCH_CHECK_ROWS = 262_144L;
    static final int DEFAULT_BATCH_LIMIT = 64;
    private static final Log LOG = LogFactory.getLog(PageFrameReduceDispatcher.class);
    private static final long PUBLICATION_OPEN = Long.MIN_VALUE;
    private static final long PUBLICATION_PERMIT_MASK = Long.MAX_VALUE;
    private static final int QUIESCE_DRAINED = 3;
    private static final int QUIESCE_DRAINING = 2;
    private static final int QUIESCE_OPEN = 0;
    private static final int QUIESCE_REQUESTED = 1;
    private static final int REASON_TAIL_PROGRESS_POLL = -1;
    private static final int RING_VERSION_STRIDE_SHIFT = 3;
    private final BatchPolicy batchPolicy;
    private final MessageBus messageBus;
    private final AtomicLong progressVersion = new AtomicLong();
    private final FiberEventWaitQueue progressWaitQueue =
            new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);
    private final AtomicLong publicationAdmission = new AtomicLong(PUBLICATION_OPEN);
    private final AtomicInteger quiesceState = new AtomicInteger(QUIESCE_OPEN);
    private final AtomicLongArray ringProgressVersions;
    private final FiberEventWaitQueue[] ringProgressWaitQueues;
    private final FiberRuntime runtime;
    private final FiberTaskPool<PageFrameFiberTask> taskPool;
    private final MillisecondClock timerClock;
    private final long timerIntervalMillis;
    private final TimerShards timerShards;
    private volatile boolean isClosed;

    public PageFrameReduceDispatcher(CairoEngine engine, MessageBus messageBus, FiberRuntime runtime) {
        this.batchPolicy = new BatchPolicy(engine.getConfiguration().getSqlPageFrameMaxRows());
        this.messageBus = messageBus;
        final int ringCount = messageBus.getPageFrameReduceShardCount() + 1;
        this.ringProgressVersions = new AtomicLongArray(ringCount << RING_VERSION_STRIDE_SHIFT);
        this.ringProgressWaitQueues = new FiberEventWaitQueue[ringCount];
        for (int i = 0; i < ringCount; i++) {
            ringProgressWaitQueues[i] = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);
        }
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
        try {
            runtime.registerConfigurationListener(this);
            isConfigurationListenerRegistered = true;
            runtime.registerQuiesceListener(this);
        } catch (Throwable th) {
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
                0,
                cancellationSignal,
                cancellationSignal != null ? cancellationSignal.getGeneration() : CancellationBinding.NO_GENERATION,
                null,
                CancellationBinding.NO_GENERATION,
                false,
                -1
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
                0,
                cancellationSignal,
                cancellationSignal != null ? cancellationSignal.getGeneration() : CancellationBinding.NO_GENERATION,
                supplementalCancellationSignal,
                supplementalCancellationSignal != null
                        ? supplementalCancellationSignal.getGeneration()
                        : CancellationBinding.NO_GENERATION,
                false,
                -1
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

    @TestOnly
    public int awaitShardProgress(
            PageFrameSequence<?> frameSequence,
            long observedSequenceVersion,
            long observedShardVersion,
            @Nullable FiberCancellationSignal cancellationSignal
    ) {
        return awaitProgress(
                frameSequence,
                observedSequenceVersion,
                progressVersion.get(),
                observedShardVersion,
                cancellationSignal,
                cancellationSignal != null ? cancellationSignal.getGeneration() : CancellationBinding.NO_GENERATION,
                null,
                CancellationBinding.NO_GENERATION,
                false,
                frameSequence.getShard()
        );
    }

    @Override
    public void beginQuiesce() {
        if (!quiesceState.compareAndSet(QUIESCE_OPEN, QUIESCE_REQUESTED)) {
            return;
        }
        progressVersion.incrementAndGet();
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
            progressVersion.incrementAndGet();
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
                    signalRingProgress(getUnorderedRing());
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

    public long getBatchCheckRows() {
        return batchPolicy.getCheckRows();
    }

    public int getBatchLimit() {
        return DEFAULT_BATCH_LIMIT;
    }

    public long getBatchRowBudget() {
        return batchPolicy.getRowBudget();
    }

    @TestOnly
    public int getCreatedTaskCount() {
        return taskPool.getCreatedCount();
    }

    public long getProgressVersion() {
        return progressVersion.get();
    }

    public long getRingProgressVersion(int ring) {
        return ringProgressVersions.get(ring << RING_VERSION_STRIDE_SHIFT);
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
    public void setBatchCheckRowsForTesting(long batchCheckRows) {
        batchPolicy.setCheckRows(batchCheckRows);
    }

    @TestOnly
    public void setBatchRowBudgetForTesting(long batchRowBudget) {
        batchPolicy.setRowBudget(batchRowBudget);
    }

    @TestOnly
    public void setFreeTaskScheduleStateForTesting(int expectedState, int targetState) {
        taskPool.setFreeTaskScheduleStateForTesting(expectedState, targetState);
    }

    @TestOnly
    public void signalProgressForTesting(PageFrameSequence<?> frameSequence) {
        signalProgress(frameSequence, frameSequence.getShard());
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
            long observedRingVersion,
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration,
            @Nullable FiberCancellationSignal supplementalCancellationSignal,
            long supplementalCancellationSignalGeneration,
            boolean isQuiescingAllowed,
            int ring
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
                        + (ring > -1 ? 1 : 0)
                        + (cancellationSignal != null ? 1 : 0)
                        + (supplementalCancellationSignal != null ? 1 : 0)
        );
        if (token == Fiber.TOKEN_REFUSED) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        try {
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
            // conditions present at registration win over completion, so re-check after the spin
            if (!isQuiescingAllowed
                    && frameSequence instanceof UnorderedPageFrameSequence<?> unorderedFrameSequence
                    && isOpenForTailSpin()
                    && unorderedFrameSequence.isDoneAfterTailSpin()
                    && isOpenForTailSpin()) {
                final int reason = coordinator.preferPendingCancel(token, FiberWaitCoordinator.REASON_PROGRESS);
                if (reason == FiberWaitCoordinator.REASON_PROGRESS
                        && unorderedFrameSequence.getDispatchContext() != null) {
                    return REASON_TAIL_PROGRESS_POLL;
                }
                return reason;
            }
            if (!coordinator.armEvent(token, frameSequence.getProgressWaitQueue())
                    || !coordinator.armEvent(token, progressWaitQueue)
                    || ring > -1 && !coordinator.armEvent(token, ringProgressWaitQueues[ring])) {
                throw new IllegalStateException("page frame progress wait registration failed");
            }
            if (!coordinator.armTimer(token, timerShards, timerClock, timerIntervalMillis)) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            if (isClosed || (!isQuiescingAllowed && quiesceState.get() != QUIESCE_OPEN)) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            if (frameSequence.getProgressVersion() != observedSequenceVersion
                    || progressVersion.get() != observedGlobalVersion
                    || ring > -1 && getRingProgressVersion(ring) != observedRingVersion) {
                return coordinator.preferPendingCancel(token, FiberWaitCoordinator.REASON_PROGRESS);
            }
            return fiber.suspendWait(token, FiberWaitCoordinator.REASON_PROGRESS);
        } finally {
            coordinator.teardownWait(token);
        }
    }

    private void completeFailedOrderedAcquisition(
            MCSequence subSeq,
            long cursor,
            PageFrameReduceTask reduceTask,
            PageFrameSequence<?> frameSequence,
            Throwable failure
    ) {
        final int shard = frameSequence.getShard();
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
            signalProgress(frameSequence, shard);
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

    private boolean drainOrdered(RingQueue<PageFrameReduceTask> queue, MCSequence subSeq) {
        while (true) {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final PageFrameReduceTask task = queue.get(cursor);
                final PageFrameSequence<?> frameSequence = task.getFrameSequence();
                final int shard = frameSequence.getShard();
                if (frameSequence.isActive()
                        && frameSequence.cancelIfChanged(SqlExecutionCircuitBreaker.STATE_CANCELLED)) {
                    LOG.info().$("cancelling in-flight query, dispatcher is quiescing [frameSequenceId=")
                            .$(frameSequence.getId()).I$();
                }
                try {
                    subSeq.done(cursor);
                } finally {
                    frameSequence.getReduceFinishedCounter().incrementAndGet();
                    signalProgress(frameSequence, shard);
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
                signalRingProgress(getUnorderedRing());
                if (frameSequenceId == frameSequence.getId()) {
                    if (frameSequence.isActive()
                            && frameSequence.cancelIfChanged(SqlExecutionCircuitBreaker.STATE_CANCELLED)) {
                        LOG.info().$("cancelling in-flight query, dispatcher is quiescing [frameSequenceId=")
                                .$(frameSequenceId).I$();
                    }
                    frameSequence.getDoneLatch().countDown();
                    signalProgress(frameSequence);
                }
            } else {
                return cursor == -1;
            }
        }
    }

    private boolean isOpenForTailSpin() {
        return timerShards.isRunning() && !isClosed && quiesceState.get() == QUIESCE_OPEN;
    }

    private void launch(
            Fiber fiber,
            long reservationEpoch,
            PageFrameFiberTask task,
            boolean isDirectMountAllowed
    ) {
        final long taskIncarnation = task.getIncarnation();
        final FiberDispatchContext dispatchContext = task.getDispatchContext();
        final LaunchResult result = isDirectMountAllowed && !Fiber.isMounted()
                ? runtime.launchReservedDirect(fiber, reservationEpoch, task, taskIncarnation, dispatchContext)
                : runtime.launchReserved(fiber, reservationEpoch, task, taskIncarnation, dispatchContext);
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

    BatchPolicy getBatchPolicy() {
        return batchPolicy;
    }

    boolean isProgressWaitTerminated(
            AbstractPageFrameSequence frameSequence,
            long observedSequenceVersion,
            long observedRingVersion,
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration,
            @Nullable FiberCancellationSignal supplementalCancellationSignal,
            long supplementalCancellationSignalGeneration,
            @Nullable SqlExecutionCircuitBreaker circuitBreaker,
            boolean isDraining,
            int ring
    ) {
        final long observedGlobalVersion = progressVersion.get();
        while (true) {
            final int reason = awaitProgress(
                    frameSequence,
                    observedSequenceVersion,
                    observedGlobalVersion,
                    observedRingVersion,
                    cancellationSignal,
                    cancellationSignalGeneration,
                    supplementalCancellationSignal,
                    supplementalCancellationSignalGeneration,
                    isDraining,
                    ring
            );
            switch (reason) {
                case FiberWaitCoordinator.REASON_TIMER -> {
                    if (circuitBreaker != null) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
                    }
                }
                case FiberWaitCoordinator.REASON_CANCEL -> {
                    if (circuitBreaker != null) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
                    }
                    return true;
                }
                case FiberWaitCoordinator.REASON_NONE, FiberWaitCoordinator.REASON_SHUTDOWN -> {
                    return true;
                }
                case FiberWaitCoordinator.REASON_PROGRESS -> {
                    return false;
                }
                case REASON_TAIL_PROGRESS_POLL -> {
                    // awaitProgress() tears down the BUILDING wait before returning this private
                    // reason. Poll the current owner ticket only after that teardown, so an expired
                    // grant can yield without nesting two suspension protocols.
                    Fiber.pollMountedDispatchTicket();
                    return false;
                }
                default -> throw new IllegalStateException(
                        "unexpected page frame progress wait reason [reason=" + reason + ']'
                );
            }
        }
    }

    int getUnorderedRing() {
        return ringProgressWaitQueues.length - 1;
    }

    boolean isQuiescing() {
        return quiesceState.get() != QUIESCE_OPEN;
    }

    void signalProgress(AbstractPageFrameSequence frameSequence) {
        frameSequence.signalProgress();
    }

    void signalProgress(AbstractPageFrameSequence frameSequence, int ring) {
        frameSequence.signalProgress();
        signalRingProgress(ring);
    }

    void signalRingProgress(int ring) {
        ringProgressVersions.incrementAndGet(ring << RING_VERSION_STRIDE_SHIFT);
        ringProgressWaitQueues[ring].fireAll();
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

    /**
     * The natural frame and row budget belongs to the batch, including all of its mount segments.
     * Dispatch preemption suspends this state; it does not start a new batch.
     */
    static final class Batch {
        private final BatchPolicy policy;
        private @Nullable FiberDispatchContext dispatchContext;
        private long dispatchOwnerId;
        private int entryCount;
        private boolean isPollDue;
        private long rows;
        private long rowsSinceCheck;

        Batch(BatchPolicy policy) {
            this.policy = policy;
        }

        void addRows(long rowCount) {
            entryCount++;
            rows += rowCount;
            rowsSinceCheck += rowCount > 0 ? rowCount : policy.getCheckRows();
        }

        void begin() {
            dispatchContext = Fiber.captureDispatchContext();
            dispatchOwnerId = queryRegistryOwnerId(dispatchContext);
            entryCount = 0;
            isPollDue = false;
            rows = 0;
            rowsSinceCheck = 0;
        }

        void clear() {
            dispatchContext = null;
        }

        boolean shouldContinue() {
            if (entryCount >= DEFAULT_BATCH_LIMIT || rows >= policy.getRowBudget()) {
                return false;
            }
            if (rowsSinceCheck >= policy.getCheckRows()) {
                rowsSinceCheck = 0;
                isPollDue = true;
            }
            return true;
        }

        /**
         * Query leases can reuse a context object, so compare its owner ID as well as its identity
         * before retaining the current grant. Switching or preemption leaves the batch budget intact.
         */
        void switchTo(@Nullable FiberDispatchContext nextContext) {
            final long nextOwnerId = queryRegistryOwnerId(nextContext);
            if (dispatchContext != nextContext || dispatchOwnerId != nextOwnerId) {
                if (!Fiber.switchDispatchContext(nextContext)) {
                    throw new IllegalStateException("reducer could not switch dispatch context");
                }
            } else if (isPollDue) {
                Fiber.pollMountedDispatchTicket();
            }
            isPollDue = false;
            dispatchContext = nextContext;
            dispatchOwnerId = nextOwnerId;
        }

        private static long queryRegistryOwnerId(@Nullable FiberDispatchContext context) {
            return context != null ? context.getQueryRegistryOwnerId() : -1;
        }
    }

    static final class BatchPolicy {
        private final long configuredRowBudget;
        private volatile long checkRows = DEFAULT_BATCH_CHECK_ROWS;
        private volatile long rowBudget;

        BatchPolicy(long rowBudget) {
            this.configuredRowBudget = rowBudget;
            this.rowBudget = rowBudget;
        }

        long getCheckRows() {
            return checkRows;
        }

        long getRowBudget() {
            return rowBudget;
        }

        void setCheckRows(long checkRows) {
            this.checkRows = checkRows >= 0 ? checkRows : DEFAULT_BATCH_CHECK_ROWS;
        }

        void setRowBudget(long rowBudget) {
            this.rowBudget = rowBudget > 0 ? rowBudget : configuredRowBudget;
        }
    }
}

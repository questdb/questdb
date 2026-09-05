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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.groupby.PostAggregationCircuitBreaker;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateEntry;
import io.questdb.mp.CountDownLatchSPI;
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
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.tasks.GroupByLongTopKTask;
import io.questdb.tasks.GroupByMergeShardTask;
import io.questdb.tasks.LatestByTask;
import io.questdb.tasks.VectorAggregateTask;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class QueryParallelFiberDispatcher implements FiberRuntimeConfigurationListener, FiberRuntimeQuiesceListener, QuietCloseable {
    public static final long OWNER_YIELD_UNSET = Long.MIN_VALUE;
    private static final long OWNER_HELP_YIELD_INTERVAL_NANOS = 1_000_000L;
    private static final long PUBLICATION_OPEN = Long.MIN_VALUE;
    private static final long PUBLICATION_PERMIT_MASK = Long.MAX_VALUE;
    private static final int QUIESCE_DRAINED = 3;
    private static final int QUIESCE_DRAINING = 2;
    private static final int QUIESCE_OPEN = 0;
    private static final int QUIESCE_REQUESTED = 1;
    private final long batchRowBudget;
    private final FiberTaskPool<LatestByFiberTask> latestByTaskPool;
    private final FiberTaskPool<GroupByLongTopKFiberTask> longTopKTaskPool;
    private final FiberTaskPool<GroupByMergeShardFiberTask> mergeShardTaskPool;
    private final MessageBus messageBus;
    private final NanosecondClock nanosecondClock;
    private final AtomicLong progressVersion = new AtomicLong();
    private final FiberEventWaitQueue progressWaitQueue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);
    private final AtomicLong publicationAdmission = new AtomicLong(PUBLICATION_OPEN);
    private final AtomicInteger quiesceState = new AtomicInteger(QUIESCE_OPEN);
    private final FiberRuntime runtime;
    private final MillisecondClock timerClock;
    private final long timerIntervalMillis;
    private final TimerShards timerShards;
    private final FiberTaskPool<VectorAggregateFiberTask> vectorAggregateTaskPool;
    private volatile boolean isClosed;

    public QueryParallelFiberDispatcher(CairoEngine engine, MessageBus messageBus, FiberRuntime runtime) {
        // Stop each batch after it reaches one configured-max-frame's row count.
        this.batchRowBudget = engine.getConfiguration().getSqlPageFrameMaxRows();
        this.messageBus = messageBus;
        this.nanosecondClock = engine.getConfiguration().getNanosecondClock();
        this.runtime = runtime;
        this.timerClock = engine.getConfiguration().getMillisecondClock();
        this.timerIntervalMillis = Math.max(1, engine.getConfiguration().getQueryContinuationWakeIntervalMillis());
        this.timerShards = engine.getTimerShards();
        final int capacity = runtime.getMaxLiveFiberCount();
        final int maxRetainedCount = runtime.getMaxRetainedFiberCount();
        this.latestByTaskPool = new FiberTaskPool<>(
                capacity,
                maxRetainedCount,
                pool -> new LatestByFiberTask(this, pool, timerShards)
        );
        this.longTopKTaskPool = new FiberTaskPool<>(
                capacity,
                maxRetainedCount,
                pool -> new GroupByLongTopKFiberTask(this, pool, timerShards)
        );
        this.mergeShardTaskPool = new FiberTaskPool<>(
                capacity,
                maxRetainedCount,
                pool -> new GroupByMergeShardFiberTask(this, pool, timerShards)
        );
        this.vectorAggregateTaskPool = new FiberTaskPool<>(
                capacity,
                maxRetainedCount,
                pool -> new VectorAggregateFiberTask(this, pool, timerShards)
        );
        boolean configurationListenerRegistered = false;
        try {
            runtime.registerConfigurationListener(this);
            configurationListenerRegistered = true;
            runtime.registerQuiesceListener(this);
        } catch (Throwable th) {
            if (configurationListenerRegistered) {
                try {
                    runtime.unregisterConfigurationListener(this);
                } catch (Throwable cleanupFailure) {
                    addSuppressed(th, cleanupFailure);
                }
            }
            Throwable failure = Misc.freeBestEffort(th, latestByTaskPool);
            failure = Misc.freeBestEffort(failure, longTopKTaskPool);
            failure = Misc.freeBestEffort(failure, mergeShardTaskPool);
            failure = Misc.freeBestEffort(failure, vectorAggregateTaskPool);
            CairoException.rethrowCleanupFailure(failure);
        }
    }

    /**
     * Whether the calling owner runs on a mounted Fiber in FIBER mode, i.e. whether it can park
     * or yield cooperatively. Unlike {@link #isOwnerParkable()}, this ignores the dispatcher state:
     * a cooperative yield goes through the Fiber runtime and stays valid while the dispatcher
     * quiesces.
     */
    public static boolean isFiberOwner() {
        return Fiber.isMounted()
                && SuspensionScope.isFiberMode()
                && Fiber.current() != null;
    }

    public boolean awaitProgress(
            AsyncQueryProgressState progressState,
            long observedVersion,
            long observedGlobalVersion,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        FiberCancellationSignal cancellationSignal = SuspensionScope.getCancellationSignal();
        long cancellationSignalGeneration = SuspensionScope.getCancellationSignalGeneration();
        if (cancellationSignal == null) {
            final CancellationBinding cancellationBinding = SuspensionScope.getCancellationBindingScratch();
            circuitBreaker.copyCancelledFlagTo(cancellationBinding);
            final AtomicBoolean cancelledFlag = cancellationBinding.getFlag();
            if (cancelledFlag instanceof FiberCancellationSignal signal) {
                cancellationSignal = signal;
                cancellationSignalGeneration = cancellationBinding.getGeneration(cancelledFlag);
            }
        }
        final int reason = awaitProgress(
                progressState,
                observedVersion,
                observedGlobalVersion,
                cancellationSignal,
                cancellationSignalGeneration
        );
        return switch (reason) {
            case FiberWaitCoordinator.REASON_CANCEL -> {
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                yield true;
            }
            case FiberWaitCoordinator.REASON_PROGRESS -> true;
            case FiberWaitCoordinator.REASON_TIMER -> {
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                yield true;
            }
            case FiberWaitCoordinator.REASON_NONE, FiberWaitCoordinator.REASON_SHUTDOWN -> false;
            default -> throw new IllegalStateException(
                    "unexpected query parallel progress wait reason [reason=" + reason + ']'
            );
        };
    }

    // Drain loops must run to latch completion before their callers release native state, so this
    // variant never throws; the loop-top breaker check surfaces the error after the drain.
    public boolean awaitProgressWhileDraining(
            AsyncQueryProgressState progressState,
            long observedVersion,
            long observedGlobalVersion
    ) {
        final int reason = awaitProgress(
                progressState,
                observedVersion,
                observedGlobalVersion,
                null,
                CancellationBinding.NO_GENERATION
        );
        return switch (reason) {
            case FiberWaitCoordinator.REASON_PROGRESS, FiberWaitCoordinator.REASON_TIMER -> true;
            case FiberWaitCoordinator.REASON_NONE, FiberWaitCoordinator.REASON_SHUTDOWN -> false;
            default -> throw new IllegalStateException(
                    "unexpected query parallel drain wait reason [reason=" + reason + ']'
            );
        };
    }

    /**
     * Waits for drain progress while the owner breaker is still healthy. Cancellation only wakes
     * the Fiber; it never escapes this must-complete drain. The caller must propagate the observed
     * cancellation to the shared task breaker, then use the non-cancellable overload so an already
     * cancelled signal cannot make the drain spin.
     */
    public boolean awaitProgressWhileDraining(
            AsyncQueryProgressState progressState,
            long observedVersion,
            long observedGlobalVersion,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        FiberCancellationSignal cancellationSignal = SuspensionScope.getCancellationSignal();
        long cancellationSignalGeneration = SuspensionScope.getCancellationSignalGeneration();
        if (cancellationSignal == null) {
            final CancellationBinding cancellationBinding = SuspensionScope.getCancellationBindingScratch();
            circuitBreaker.copyCancelledFlagTo(cancellationBinding);
            final AtomicBoolean cancelledFlag = cancellationBinding.getFlag();
            if (cancelledFlag instanceof FiberCancellationSignal signal) {
                cancellationSignal = signal;
                cancellationSignalGeneration = cancellationBinding.getGeneration(cancelledFlag);
            }
        }
        final int reason = awaitProgress(
                progressState,
                observedVersion,
                observedGlobalVersion,
                cancellationSignal,
                cancellationSignalGeneration
        );
        return switch (reason) {
            case FiberWaitCoordinator.REASON_CANCEL,
                 FiberWaitCoordinator.REASON_PROGRESS,
                 FiberWaitCoordinator.REASON_TIMER -> true;
            case FiberWaitCoordinator.REASON_NONE, FiberWaitCoordinator.REASON_SHUTDOWN -> false;
            default -> throw new IllegalStateException(
                    "unexpected query parallel drain wait reason [reason=" + reason + ']'
            );
        };
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
        final boolean drained = isQuiesced();
        isClosed = true;
        Throwable failure = drained
                ? null
                : new IllegalStateException("query parallel fiber dispatcher has active publications");
        try {
            if (messageBus.getQueryParallelFiberDispatcher() == this) {
                messageBus.setQueryParallelFiberDispatcher(null);
            }
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        try {
            runtime.unregisterConfigurationListener(this);
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        try {
            runtime.unregisterQuiesceListener(this);
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        try {
            progressWaitQueue.shutdown();
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        failure = Misc.freeBestEffort(failure, latestByTaskPool);
        failure = Misc.freeBestEffort(failure, longTopKTaskPool);
        failure = Misc.freeBestEffort(failure, mergeShardTaskPool);
        failure = Misc.freeBestEffort(failure, vectorAggregateTaskPool);
        CairoException.rethrowCleanupFailure(failure);
    }

    public boolean consumeLatestBy(int workerId) {
        final MCSequence subSeq = messageBus.getLatestBySubSeq();
        if (quiesceState.get() != QUIESCE_OPEN || hasNoPendingTasks(subSeq)) {
            return true;
        }
        final Fiber fiber = reserveFiber();
        if (fiber == null) {
            return true;
        }
        final long reservationEpoch = fiber.getReservationEpoch();
        if (!tryLeaseTask(fiber, reservationEpoch, latestByTaskPool)) {
            return true;
        }
        LatestByFiberTask fiberTask = null;
        boolean launchOwnership = true;
        try {
            final long cursor = nextCursor(subSeq);
            if (cursor < 0) {
                return true;
            }
            final RingQueue<LatestByTask> queue = messageBus.getLatestByQueue();
            final LatestByTask task = queue.get(cursor);
            try {
                fiberTask = latestByTaskPool.acquireLeased();
            } catch (Throwable th) {
                completeFailedLatestByAcquisition(subSeq, cursor, task, th);
                throw th;
            }
            fiberTask.of(task, queue, subSeq, cursor);
            launchOwnership = false;
            launch(fiber, reservationEpoch, fiberTask, workerId > -1);
            return false;
        } finally {
            try {
                if (launchOwnership) {
                    if (fiberTask != null) {
                        abortOrRelease(fiberTask, latestByTaskPool);
                    } else {
                        latestByTaskPool.releaseLease();
                    }
                }
            } finally {
                runtime.releaseReservedFiber(fiber, reservationEpoch);
            }
        }
    }

    public boolean consumeLongTopK(int workerId) {
        final MCSequence subSeq = messageBus.getGroupByLongTopKSubSeq();
        if (quiesceState.get() != QUIESCE_OPEN || hasNoPendingTasks(subSeq)) {
            return true;
        }
        final Fiber fiber = reserveFiber();
        if (fiber == null) {
            return true;
        }
        final long reservationEpoch = fiber.getReservationEpoch();
        if (!tryLeaseTask(fiber, reservationEpoch, longTopKTaskPool)) {
            return true;
        }
        GroupByLongTopKFiberTask fiberTask = null;
        boolean launchOwnership = true;
        try {
            final long cursor = nextCursor(subSeq);
            if (cursor < 0) {
                return true;
            }
            final RingQueue<GroupByLongTopKTask> queue = messageBus.getGroupByLongTopKQueue();
            final GroupByLongTopKTask task = queue.get(cursor);
            try {
                fiberTask = longTopKTaskPool.acquireLeased();
            } catch (Throwable th) {
                completeFailedLongTopKAcquisition(subSeq, cursor, task, th);
                throw th;
            }
            fiberTask.of(workerId, task, queue, subSeq, cursor);
            launchOwnership = false;
            launch(fiber, reservationEpoch, fiberTask, workerId > -1);
            return false;
        } finally {
            try {
                if (launchOwnership) {
                    if (fiberTask != null) {
                        abortOrRelease(fiberTask, longTopKTaskPool);
                    } else {
                        longTopKTaskPool.releaseLease();
                    }
                }
            } finally {
                runtime.releaseReservedFiber(fiber, reservationEpoch);
            }
        }
    }

    public boolean consumeMergeShard(int workerId) {
        final MCSequence subSeq = messageBus.getGroupByMergeShardSubSeq();
        if (quiesceState.get() != QUIESCE_OPEN || hasNoPendingTasks(subSeq)) {
            return true;
        }
        final Fiber fiber = reserveFiber();
        if (fiber == null) {
            return true;
        }
        final long reservationEpoch = fiber.getReservationEpoch();
        if (!tryLeaseTask(fiber, reservationEpoch, mergeShardTaskPool)) {
            return true;
        }
        GroupByMergeShardFiberTask fiberTask = null;
        boolean launchOwnership = true;
        try {
            final long cursor = nextCursor(subSeq);
            if (cursor < 0) {
                return true;
            }
            final RingQueue<GroupByMergeShardTask> queue = messageBus.getGroupByMergeShardQueue();
            final GroupByMergeShardTask task = queue.get(cursor);
            try {
                fiberTask = mergeShardTaskPool.acquireLeased();
            } catch (Throwable th) {
                completeFailedMergeShardAcquisition(subSeq, cursor, task, th);
                throw th;
            }
            fiberTask.of(workerId, task, queue, subSeq, cursor);
            launchOwnership = false;
            launch(fiber, reservationEpoch, fiberTask, workerId > -1);
            return false;
        } finally {
            try {
                if (launchOwnership) {
                    if (fiberTask != null) {
                        abortOrRelease(fiberTask, mergeShardTaskPool);
                    } else {
                        mergeShardTaskPool.releaseLease();
                    }
                }
            } finally {
                runtime.releaseReservedFiber(fiber, reservationEpoch);
            }
        }
    }

    public boolean consumeVectorAggregate(int workerId) {
        final MCSequence subSeq = messageBus.getVectorAggregateSubSeq();
        if (quiesceState.get() != QUIESCE_OPEN || hasNoPendingTasks(subSeq)) {
            return true;
        }
        final Fiber fiber = reserveFiber();
        if (fiber == null) {
            return true;
        }
        final long reservationEpoch = fiber.getReservationEpoch();
        if (!tryLeaseTask(fiber, reservationEpoch, vectorAggregateTaskPool)) {
            return true;
        }
        VectorAggregateFiberTask fiberTask = null;
        boolean launchOwnership = true;
        try {
            final long cursor = nextCursor(subSeq);
            if (cursor < 0) {
                return true;
            }
            final RingQueue<VectorAggregateTask> queue = messageBus.getVectorAggregateQueue();
            final VectorAggregateTask task = queue.get(cursor);
            try {
                fiberTask = vectorAggregateTaskPool.acquireLeased();
            } catch (Throwable th) {
                completeFailedVectorAggregateAcquisition(subSeq, cursor, task, th);
                throw th;
            }
            fiberTask.of(workerId, task, queue, subSeq, cursor);
            launchOwnership = false;
            launch(fiber, reservationEpoch, fiberTask, workerId > -1);
            return false;
        } finally {
            try {
                if (launchOwnership) {
                    if (fiberTask != null) {
                        abortOrRelease(fiberTask, vectorAggregateTaskPool);
                    } else {
                        vectorAggregateTaskPool.releaseLease();
                    }
                }
            } finally {
                runtime.releaseReservedFiber(fiber, reservationEpoch);
            }
        }
    }

    /**
     * Throttles the cooperative yield of a Fiber owner that helps with its own unpublished work,
     * so the owner gives the carrier up at most once per interval. Start with
     * {@link #OWNER_YIELD_UNSET} and pass the returned value back on every call.
     */
    public long cooperateFiberOwner(long lastOwnerYieldNanos) {
        final long now = nanosecondClock.getTicks();
        if (lastOwnerYieldNanos == OWNER_YIELD_UNSET) {
            return now;
        }
        final long elapsed = now - lastOwnerYieldNanos;
        if (elapsed < 0) {
            return now;
        }
        if (elapsed >= OWNER_HELP_YIELD_INTERVAL_NANOS) {
            Fiber.yieldCooperatively();
            return nanosecondClock.getTicks();
        }
        return lastOwnerYieldNanos;
    }

    @TestOnly
    public int getLatestByCreatedTaskCount() {
        return latestByTaskPool.getCreatedCount();
    }

    @TestOnly
    public int getLongTopKCreatedTaskCount() {
        return longTopKTaskPool.getCreatedCount();
    }

    @TestOnly
    public int getMergeShardCreatedTaskCount() {
        return mergeShardTaskPool.getCreatedCount();
    }

    public long getProgressVersion() {
        return progressVersion.get();
    }

    @TestOnly
    public int getVectorAggregateCreatedTaskCount() {
        return vectorAggregateTaskPool.getCreatedCount();
    }

    public boolean isOwnerParkable() {
        return !isClosed
                && quiesceState.get() == QUIESCE_OPEN
                && isFiberOwner();
    }

    @Override
    public boolean isQuiesced() {
        return quiesceState.get() == QUIESCE_DRAINED;
    }

    @Override
    public void onConfigurationChanged(int maxLiveFiberCount, int maxRetainedFiberCount) {
        latestByTaskPool.updateLimits(maxLiveFiberCount, maxRetainedFiberCount);
        longTopKTaskPool.updateLimits(maxLiveFiberCount, maxRetainedFiberCount);
        mergeShardTaskPool.updateLimits(maxLiveFiberCount, maxRetainedFiberCount);
        vectorAggregateTaskPool.updateLimits(maxLiveFiberCount, maxRetainedFiberCount);
    }

    @Override
    public void progressQuiesce() {
        if (!quiesceState.compareAndSet(QUIESCE_REQUESTED, QUIESCE_DRAINING)) {
            return;
        }
        boolean drained = false;
        try {
            drained = drainPublishedTasks()
                    && publicationAdmission.get() == 0
                    && latestByTaskPool.hasNoLeasedTasks()
                    && longTopKTaskPool.hasNoLeasedTasks()
                    && mergeShardTaskPool.hasNoLeasedTasks()
                    && vectorAggregateTaskPool.hasNoLeasedTasks();
        } finally {
            quiesceState.set(drained ? QUIESCE_DRAINED : QUIESCE_REQUESTED);
        }
    }

    public void releasePublication() {
        final long current = publicationAdmission.decrementAndGet();
        if ((current & PUBLICATION_PERMIT_MASK) == PUBLICATION_PERMIT_MASK) {
            publicationAdmission.incrementAndGet();
            throw new IllegalStateException("query parallel publication admission underflow");
        }
    }

    @TestOnly
    public void runWithOwnerWaitQueueLockedForTesting(
            AsyncQueryProgressState progressState,
            Runnable action
    ) {
        synchronized (progressState.getWaitQueue()) {
            action.run();
        }
    }

    @TestOnly
    public void runWithQueueWaitQueueLockedForTesting(Runnable action) {
        synchronized (progressWaitQueue) {
            action.run();
        }
    }

    @TestOnly
    public void setBeforeMergeShardTaskCreationForTesting(Runnable hook) {
        mergeShardTaskPool.setBeforeNewTaskForTesting(hook);
    }

    public void signalOwnerProgress(@Nullable AsyncQueryProgressState progressState) {
        if (progressState != null) {
            progressState.signalProgress();
        }
    }

    public void signalQueueProgress() {
        progressVersion.incrementAndGet();
        progressWaitQueue.fire();
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
                throw new IllegalStateException("query parallel publication admission overflow");
            }
            if (publicationAdmission.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    int getBatchLimit() {
        return PageFrameReduceDispatcher.DEFAULT_BATCH_LIMIT;
    }

    long getBatchRowBudget() {
        return batchRowBudget;
    }

    private static <T extends AbstractQueryParallelFiberTask> void abortOrRelease(
            T task,
            FiberTaskPool<T> taskPool
    ) {
        if (task.isBound()) {
            task.abortBeforeLaunch();
        } else {
            taskPool.release(task);
        }
    }

    private static Throwable addFailure(@Nullable Throwable primary, Throwable failure) {
        if (primary == null) {
            return failure;
        }
        addSuppressed(primary, failure);
        return primary;
    }

    private static void addSuppressed(Throwable primary, Throwable failure) {
        if (primary != failure) {
            primary.addSuppressed(failure);
        }
    }

    private static boolean hasNoPendingTasks(MCSequence subSeq) {
        return subSeq.current() >= subSeq.getBarrier().current();
    }

    private static IllegalStateException launchFailed(LaunchResult result) {
        return new IllegalStateException("query parallel fiber launch failed [result=" + result + ']');
    }

    private long nextCursor(MCSequence subSeq) {
        while (true) {
            final long cursor = subSeq.next();
            if (cursor != -2 || quiesceState.get() != QUIESCE_OPEN) {
                return cursor;
            }
            Os.pause();
        }
    }

    private void completeFailedLatestByAcquisition(
            MCSequence subSeq,
            long cursor,
            LatestByTask task,
            Throwable failure
    ) {
        final AsyncQueryProgressState progressState = task.getProgressState();
        try {
            task.abort();
        } catch (Throwable cleanupFailure) {
            addSuppressed(failure, cleanupFailure);
        }
        completeFailedCursorOwnership(subSeq, cursor, progressState, failure);
    }

    private void completeFailedLongTopKAcquisition(
            MCSequence subSeq,
            long cursor,
            GroupByLongTopKTask task,
            Throwable failure
    ) {
        final PostAggregationCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final AsyncQueryProgressState progressState = task.getAtom() != null
                ? task.getAtom().getShardingContext().getProgressState()
                : null;
        try {
            task.clear();
        } catch (Throwable cleanupFailure) {
            addSuppressed(failure, cleanupFailure);
        }
        completeFailedCountedAcquisition(
                subSeq,
                cursor,
                circuitBreaker,
                startedCounter,
                doneLatch,
                progressState,
                failure
        );
    }

    private void completeFailedMergeShardAcquisition(
            MCSequence subSeq,
            long cursor,
            GroupByMergeShardTask task,
            Throwable failure
    ) {
        final PostAggregationCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final AsyncQueryProgressState progressState = task.getShardingContext() != null
                ? task.getShardingContext().getProgressState()
                : null;
        try {
            task.clear();
        } catch (Throwable cleanupFailure) {
            addSuppressed(failure, cleanupFailure);
        }
        completeFailedCountedAcquisition(
                subSeq,
                cursor,
                circuitBreaker,
                startedCounter,
                doneLatch,
                progressState,
                failure
        );
    }

    private void completeFailedVectorAggregateAcquisition(
            MCSequence subSeq,
            long cursor,
            VectorAggregateTask task,
            Throwable failure
    ) {
        final VectorAggregateEntry entry = task.entry;
        final AsyncQueryProgressState progressState = entry != null ? entry.getProgressState() : null;
        task.entry = null;
        try {
            if (entry != null) {
                entry.abort(false);
            }
        } catch (Throwable cleanupFailure) {
            addSuppressed(failure, cleanupFailure);
        }
        completeFailedCursorOwnership(subSeq, cursor, progressState, failure);
    }

    private void completeFailedCountedAcquisition(
            MCSequence subSeq,
            long cursor,
            @Nullable PostAggregationCircuitBreaker circuitBreaker,
            @Nullable AtomicInteger startedCounter,
            @Nullable CountDownLatchSPI doneLatch,
            @Nullable AsyncQueryProgressState progressState,
            Throwable failure
    ) {
        try {
            if (circuitBreaker != null) {
                circuitBreaker.cancel(failure);
            }
        } catch (Throwable cleanupFailure) {
            addSuppressed(failure, cleanupFailure);
        }
        try {
            if (startedCounter != null) {
                startedCounter.incrementAndGet();
            }
        } catch (Throwable cleanupFailure) {
            addSuppressed(failure, cleanupFailure);
        }
        try {
            if (doneLatch != null) {
                doneLatch.countDown();
            }
        } catch (Throwable cleanupFailure) {
            addSuppressed(failure, cleanupFailure);
        }
        completeFailedCursorOwnership(subSeq, cursor, progressState, failure);
    }

    private void completeFailedCursorOwnership(
            MCSequence subSeq,
            long cursor,
            @Nullable AsyncQueryProgressState progressState,
            Throwable failure
    ) {
        try {
            subSeq.done(cursor);
        } catch (Throwable cleanupFailure) {
            addSuppressed(failure, cleanupFailure);
        }
        try {
            signalQueueProgress();
        } catch (Throwable cleanupFailure) {
            addSuppressed(failure, cleanupFailure);
        }
        try {
            signalOwnerProgress(progressState);
        } catch (Throwable cleanupFailure) {
            addSuppressed(failure, cleanupFailure);
        }
    }

    private boolean drainLatestBy() {
        final RingQueue<LatestByTask> queue = messageBus.getLatestByQueue();
        final MCSequence subSeq = messageBus.getLatestBySubSeq();
        while (true) {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final LatestByTask task = queue.get(cursor);
                final AsyncQueryProgressState progressState = task.getProgressState();
                try {
                    task.abort();
                } finally {
                    try {
                        subSeq.done(cursor);
                    } finally {
                        try {
                            signalQueueProgress();
                        } finally {
                            signalOwnerProgress(progressState);
                        }
                    }
                }
            } else {
                return cursor == -1;
            }
        }
    }

    private boolean drainLongTopK() {
        final RingQueue<GroupByLongTopKTask> queue = messageBus.getGroupByLongTopKQueue();
        final MCSequence subSeq = messageBus.getGroupByLongTopKSubSeq();
        while (true) {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final GroupByLongTopKTask task = queue.get(cursor);
                final PostAggregationCircuitBreaker circuitBreaker = task.getCircuitBreaker();
                final AtomicInteger startedCounter = task.getStartedCounter();
                final CountDownLatchSPI doneLatch = task.getDoneLatch();
                final AsyncQueryProgressState progressState = task.getAtom() != null
                        ? task.getAtom().getShardingContext().getProgressState()
                        : null;
                task.clear();
                try {
                    circuitBreaker.cancel();
                } finally {
                    try {
                        startedCounter.incrementAndGet();
                        doneLatch.countDown();
                    } finally {
                        try {
                            subSeq.done(cursor);
                        } finally {
                            try {
                                signalQueueProgress();
                            } finally {
                                signalOwnerProgress(progressState);
                            }
                        }
                    }
                }
            } else {
                return cursor == -1;
            }
        }
    }

    private boolean drainMergeShard() {
        final RingQueue<GroupByMergeShardTask> queue = messageBus.getGroupByMergeShardQueue();
        final MCSequence subSeq = messageBus.getGroupByMergeShardSubSeq();
        while (true) {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final GroupByMergeShardTask task = queue.get(cursor);
                final PostAggregationCircuitBreaker circuitBreaker = task.getCircuitBreaker();
                final AtomicInteger startedCounter = task.getStartedCounter();
                final CountDownLatchSPI doneLatch = task.getDoneLatch();
                final AsyncQueryProgressState progressState = task.getShardingContext() != null
                        ? task.getShardingContext().getProgressState()
                        : null;
                task.clear();
                try {
                    circuitBreaker.cancel();
                } finally {
                    try {
                        startedCounter.incrementAndGet();
                        doneLatch.countDown();
                    } finally {
                        try {
                            subSeq.done(cursor);
                        } finally {
                            try {
                                signalQueueProgress();
                            } finally {
                                signalOwnerProgress(progressState);
                            }
                        }
                    }
                }
            } else {
                return cursor == -1;
            }
        }
    }

    private boolean drainPublishedTasks() {
        return drainLatestBy()
                && drainLongTopK()
                && drainMergeShard()
                && drainVectorAggregate();
    }

    private boolean drainVectorAggregate() {
        final RingQueue<VectorAggregateTask> queue = messageBus.getVectorAggregateQueue();
        final MCSequence subSeq = messageBus.getVectorAggregateSubSeq();
        while (true) {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final VectorAggregateTask task = queue.get(cursor);
                final VectorAggregateEntry entry = task.entry;
                final AsyncQueryProgressState progressState = entry != null ? entry.getProgressState() : null;
                task.entry = null;
                try {
                    if (entry != null) {
                        entry.abort(false);
                    }
                } finally {
                    try {
                        subSeq.done(cursor);
                    } finally {
                        try {
                            signalQueueProgress();
                        } finally {
                            signalOwnerProgress(progressState);
                        }
                    }
                }
            } else {
                return cursor == -1;
            }
        }
    }

    private void launch(
            Fiber fiber,
            long reservationEpoch,
            AbstractQueryParallelFiberTask task,
            boolean directMountAllowed
    ) {
        final long taskIncarnation = task.getIncarnation();
        final LaunchResult result = directMountAllowed && !Fiber.isMounted()
                ? runtime.launchReservedDirect(fiber, reservationEpoch, task, taskIncarnation)
                : runtime.launchReserved(fiber, reservationEpoch, task, taskIncarnation);
        if (result != LaunchResult.LAUNCHED
                && task.getIncarnation() == taskIncarnation
                && task.isBound()) {
            task.abortBeforeLaunch();
        }
        if (result != LaunchResult.LAUNCHED && result != LaunchResult.QUIESCING) {
            throw launchFailed(result);
        }
    }

    private @Nullable Fiber reserveFiber() {
        final FiberCancellationSignal cancellationSignal = SuspensionScope.getCancellationSignal();
        final long cancellationSignalGeneration = SuspensionScope.getCancellationSignalGeneration();
        while (true) {
            final Fiber fiber = runtime.tryReserveFiber();
            if (fiber != null || !SuspensionScope.isFiberMode() || !Fiber.isMounted()) {
                return fiber;
            }
            final int reason = runtime.awaitCapacity(cancellationSignal, cancellationSignalGeneration);
            if (reason != FiberWaitCoordinator.REASON_CAPACITY) {
                return null;
            }
        }
    }

    private <T extends AbstractQueryParallelFiberTask> boolean tryLeaseTask(
            Fiber fiber,
            long reservationEpoch,
            FiberTaskPool<T> taskPool
    ) {
        final boolean isLeased = taskPool.tryLease();
        if (!isLeased) {
            runtime.releaseReservedFiber(fiber, reservationEpoch);
        }
        return isLeased;
    }

    private int awaitProgress(
            AsyncQueryProgressState progressState,
            long observedVersion,
            long observedGlobalVersion,
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration
    ) {
        if (isClosed || quiesceState.get() != QUIESCE_OPEN) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        if (!Fiber.isMounted() || !SuspensionScope.isFiberMode()) {
            return FiberWaitCoordinator.REASON_NONE;
        }
        final Fiber fiber = Fiber.current();
        if (fiber == null) {
            return FiberWaitCoordinator.REASON_NONE;
        }
        final long token = fiber.tryBeginWaitBuild(cancellationSignal == null ? 3 : 4);
        if (token == Fiber.TOKEN_REFUSED) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        try {
            if (!coordinator.armEvent(token, progressState.getWaitQueue())
                    || !coordinator.armEvent(token, progressWaitQueue)) {
                throw new IllegalStateException("query parallel progress wait registration failed");
            }
            if (cancellationSignal != null
                    && !coordinator.armCancellation(token, cancellationSignal, cancellationSignalGeneration)) {
                throw new IllegalStateException("query parallel progress cancellation registration failed");
            }
            if (!coordinator.armTimer(token, timerShards, timerClock, timerIntervalMillis)) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            if (isClosed || quiesceState.get() != QUIESCE_OPEN) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            if (progressState.getVersion() != observedVersion || progressVersion.get() != observedGlobalVersion) {
                return coordinator.preferPendingCancel(token, FiberWaitCoordinator.REASON_PROGRESS);
            }
            return fiber.suspendWait(token, FiberWaitCoordinator.REASON_PROGRESS);
        } finally {
            coordinator.teardownWait(token);
        }
    }
}

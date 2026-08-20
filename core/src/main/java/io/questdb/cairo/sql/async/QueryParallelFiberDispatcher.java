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
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
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
    private static final long PUBLICATION_OPEN = Long.MIN_VALUE;
    private static final long PUBLICATION_PERMIT_MASK = Long.MAX_VALUE;
    private static final int QUIESCE_DRAINED = 3;
    private static final int QUIESCE_DRAINING = 2;
    private static final int QUIESCE_OPEN = 0;
    private static final int QUIESCE_REQUESTED = 1;
    private final QueryParallelFiberTaskPool<LatestByFiberTask> latestByTaskPool;
    private final QueryParallelFiberTaskPool<GroupByLongTopKFiberTask> longTopKTaskPool;
    private final QueryParallelFiberTaskPool<GroupByMergeShardFiberTask> mergeShardTaskPool;
    private final MessageBus messageBus;
    private final AtomicLong progressVersion = new AtomicLong();
    private final FiberEventWaitQueue progressWaitQueue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);
    private final AtomicLong publicationAdmission = new AtomicLong(PUBLICATION_OPEN);
    private final AtomicInteger quiesceState = new AtomicInteger(QUIESCE_OPEN);
    private final FiberRuntime runtime;
    private final MillisecondClock timerClock;
    private final long timerIntervalMillis;
    private final TimerShards timerShards;
    private final QueryParallelFiberTaskPool<VectorAggregateFiberTask> vectorAggregateTaskPool;
    private volatile boolean isClosed;

    public QueryParallelFiberDispatcher(CairoEngine engine, MessageBus messageBus, FiberRuntime runtime) {
        this.messageBus = messageBus;
        this.runtime = runtime;
        this.timerClock = engine.getConfiguration().getMillisecondClock();
        this.timerIntervalMillis = Math.max(1, engine.getConfiguration().getQueryContinuationWakeIntervalMillis());
        this.timerShards = engine.getTimerShards();
        final int capacity = runtime.getMaxLiveFiberCount();
        final int maxRetainedCount = runtime.getMaxRetainedFiberCount();
        this.latestByTaskPool = new QueryParallelFiberTaskPool<>(
                capacity,
                maxRetainedCount,
                pool -> new LatestByFiberTask(this, pool, timerShards)
        );
        this.longTopKTaskPool = new QueryParallelFiberTaskPool<>(
                capacity,
                maxRetainedCount,
                pool -> new GroupByLongTopKFiberTask(this, pool, timerShards)
        );
        this.mergeShardTaskPool = new QueryParallelFiberTaskPool<>(
                capacity,
                maxRetainedCount,
                pool -> new GroupByMergeShardFiberTask(this, pool, timerShards)
        );
        this.vectorAggregateTaskPool = new QueryParallelFiberTaskPool<>(
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

    public int awaitProgress(
            long observedVersion,
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
        final long token = fiber.tryBeginWaitBuild(cancellationSignal == null ? 2 : 3);
        if (token == Fiber.TOKEN_REFUSED) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        try {
            if (!coordinator.armEvent(token, progressWaitQueue)) {
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
            if (progressVersion.get() != observedVersion) {
                return FiberWaitCoordinator.REASON_PROGRESS;
            }
            return fiber.suspendWait(token, FiberWaitCoordinator.REASON_PROGRESS);
        } finally {
            coordinator.teardownWait(token);
        }
    }

    public boolean awaitProgress(long observedVersion, SqlExecutionCircuitBreaker circuitBreaker) {
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
                observedVersion,
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
        final LatestByFiberTask fiberTask = tryAcquireTask(fiber, reservationEpoch, latestByTaskPool);
        if (fiberTask == null) {
            return true;
        }
        boolean launchOwnership = true;
        try {
            final long cursor = nextCursor(subSeq);
            if (cursor < 0) {
                return true;
            }
            fiberTask.of(messageBus.getLatestByQueue().get(cursor), subSeq, cursor);
            launchOwnership = false;
            launch(fiber, reservationEpoch, fiberTask, workerId > -1);
            return false;
        } finally {
            try {
                if (launchOwnership) {
                    abortOrRelease(fiberTask, latestByTaskPool);
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
        final GroupByLongTopKFiberTask fiberTask = tryAcquireTask(fiber, reservationEpoch, longTopKTaskPool);
        if (fiberTask == null) {
            return true;
        }
        boolean launchOwnership = true;
        try {
            final long cursor = nextCursor(subSeq);
            if (cursor < 0) {
                return true;
            }
            fiberTask.of(workerId, messageBus.getGroupByLongTopKQueue().get(cursor), subSeq, cursor);
            launchOwnership = false;
            launch(fiber, reservationEpoch, fiberTask, workerId > -1);
            return false;
        } finally {
            try {
                if (launchOwnership) {
                    abortOrRelease(fiberTask, longTopKTaskPool);
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
        final GroupByMergeShardFiberTask fiberTask = tryAcquireTask(fiber, reservationEpoch, mergeShardTaskPool);
        if (fiberTask == null) {
            return true;
        }
        boolean launchOwnership = true;
        try {
            final long cursor = nextCursor(subSeq);
            if (cursor < 0) {
                return true;
            }
            fiberTask.of(workerId, messageBus.getGroupByMergeShardQueue().get(cursor), subSeq, cursor);
            launchOwnership = false;
            launch(fiber, reservationEpoch, fiberTask, workerId > -1);
            return false;
        } finally {
            try {
                if (launchOwnership) {
                    abortOrRelease(fiberTask, mergeShardTaskPool);
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
        final VectorAggregateFiberTask fiberTask = tryAcquireTask(fiber, reservationEpoch, vectorAggregateTaskPool);
        if (fiberTask == null) {
            return true;
        }
        boolean launchOwnership = true;
        try {
            final long cursor = nextCursor(subSeq);
            if (cursor < 0) {
                return true;
            }
            fiberTask.of(workerId, messageBus.getVectorAggregateQueue().get(cursor), subSeq, cursor);
            launchOwnership = false;
            launch(fiber, reservationEpoch, fiberTask, workerId > -1);
            return false;
        } finally {
            try {
                if (launchOwnership) {
                    abortOrRelease(fiberTask, vectorAggregateTaskPool);
                }
            } finally {
                runtime.releaseReservedFiber(fiber, reservationEpoch);
            }
        }
    }

    public long getProgressVersion() {
        return progressVersion.get();
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

    @TestOnly
    public void setBeforeMergeShardTaskCreationForTesting(Runnable hook) {
        mergeShardTaskPool.setBeforeNewTaskForTesting(hook);
    }

    @TestOnly
    public int getVectorAggregateCreatedTaskCount() {
        return vectorAggregateTaskPool.getCreatedCount();
    }

    public boolean isOwnerParkable() {
        return !isClosed
                && quiesceState.get() == QUIESCE_OPEN
                && Fiber.isMounted()
                && SuspensionScope.isFiberMode()
                && Fiber.current() != null;
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
    public void signalProgressForTesting() {
        signalProgress();
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

    private static void abortOrRelease(
            AbstractQueryParallelFiberTask task,
            QueryParallelFiberTaskPool<?> taskPool
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

    private boolean drainLatestBy() {
        final RingQueue<LatestByTask> queue = messageBus.getLatestByQueue();
        final MCSequence subSeq = messageBus.getLatestBySubSeq();
        while (true) {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                try {
                    queue.get(cursor).abort();
                } finally {
                    subSeq.done(cursor);
                    signalProgress();
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
                final AtomicBooleanCircuitBreaker circuitBreaker = task.getCircuitBreaker();
                final AtomicInteger startedCounter = task.getStartedCounter();
                final CountDownLatchSPI doneLatch = task.getDoneLatch();
                task.clear();
                try {
                    circuitBreaker.cancel();
                } finally {
                    try {
                        startedCounter.incrementAndGet();
                        doneLatch.countDown();
                    } finally {
                        subSeq.done(cursor);
                        signalProgress();
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
                final AtomicBooleanCircuitBreaker circuitBreaker = task.getCircuitBreaker();
                final AtomicInteger startedCounter = task.getStartedCounter();
                final CountDownLatchSPI doneLatch = task.getDoneLatch();
                task.clear();
                try {
                    circuitBreaker.cancel();
                } finally {
                    try {
                        startedCounter.incrementAndGet();
                        doneLatch.countDown();
                    } finally {
                        subSeq.done(cursor);
                        signalProgress();
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
                task.entry = null;
                try {
                    if (entry != null) {
                        entry.abort(false);
                    }
                } finally {
                    subSeq.done(cursor);
                    signalProgress();
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

    private <T extends AbstractQueryParallelFiberTask> @Nullable T tryAcquireTask(
            Fiber fiber,
            long reservationEpoch,
            QueryParallelFiberTaskPool<T> taskPool
    ) {
        boolean hasFiberReservation = true;
        try {
            final T task = taskPool.tryAcquire();
            if (task != null) {
                hasFiberReservation = false;
            }
            return task;
        } finally {
            if (hasFiberReservation) {
                runtime.releaseReservedFiber(fiber, reservationEpoch);
            }
        }
    }

    void signalProgress() {
        progressVersion.incrementAndGet();
        progressWaitQueue.fireAll();
    }
}

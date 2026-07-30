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
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberEventWaitQueue;
import io.questdb.mp.continuation.FiberRuntime;
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class PageFrameReduceDispatcher implements FiberRuntimeQuiesceListener, QuietCloseable {
    private static final Log LOG = LogFactory.getLog(PageFrameReduceDispatcher.class);
    private static final long PUBLICATION_OPEN = Long.MIN_VALUE;
    private static final long PUBLICATION_PERMIT_MASK = Long.MAX_VALUE;
    private static final int QUIESCE_DRAINED = 3;
    private static final int QUIESCE_DRAINING = 2;
    private static final int QUIESCE_OPEN = 0;
    private static final int QUIESCE_REQUESTED = 1;
    private final MessageBus messageBus;
    private final AtomicLong progressVersion = new AtomicLong();
    private final FiberEventWaitQueue progressWaitQueue =
            new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);
    private final AtomicLong publicationAdmission = new AtomicLong(PUBLICATION_OPEN);
    private final AtomicInteger quiesceState = new AtomicInteger(QUIESCE_OPEN);
    private final FiberRuntime runtime;
    private final PageFrameFiberTaskPool taskPool;
    private final MillisecondClock timerClock;
    private final long timerIntervalMillis;
    private final TimerShards timerShards;
    private volatile boolean isClosed;

    public PageFrameReduceDispatcher(CairoEngine engine, MessageBus messageBus, FiberRuntime runtime) {
        this.messageBus = messageBus;
        this.runtime = runtime;
        this.taskPool = new PageFrameFiberTaskPool(engine, runtime.getMaxLiveFiberCount(), this);
        this.timerClock = engine.getConfiguration().getMillisecondClock();
        this.timerIntervalMillis = Math.max(
                1,
                engine.getConfiguration().getQueryContinuationWakeIntervalMillis()
        );
        this.timerShards = engine.getTimerShards();
        try {
            runtime.registerQuiesceListener(this);
        } catch (Throwable th) {
            taskPool.close();
            throw th;
        }
    }

    public int awaitProgress(
            long observedVersion,
            @Nullable FiberCancellationSignal cancellationSignal
    ) {
        if (quiesceState.get() != QUIESCE_OPEN) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        if (!Fiber.isMounted()) {
            return FiberWaitCoordinator.REASON_NONE;
        }
        final Fiber fiber = Fiber.current();
        if (fiber == null) {
            return FiberWaitCoordinator.REASON_NONE;
        }
        final long token;
        try {
            token = fiber.beginWaitBuild(cancellationSignal == null ? 2 : 3);
        } catch (IllegalStateException e) {
            if (quiesceState.get() != QUIESCE_OPEN) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            throw e;
        }
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        try {
            if (!coordinator.armEvent(token, progressWaitQueue)) {
                throw new IllegalStateException("page frame progress wait registration failed");
            }
            if (cancellationSignal != null && !coordinator.armCancellation(token, cancellationSignal)) {
                throw new IllegalStateException("page frame progress cancellation registration failed");
            }
            if (!coordinator.armTimer(token, timerShards, timerClock, timerIntervalMillis)) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            if (quiesceState.get() != QUIESCE_OPEN) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            if (progressVersion.get() != observedVersion) {
                return FiberWaitCoordinator.REASON_PROGRESS;
            }
            return fiber.suspendWait(token);
        } finally {
            coordinator.teardownWait(token);
        }
    }

    @Override
    public void beginQuiesce() {
        if (!quiesceState.compareAndSet(QUIESCE_OPEN, QUIESCE_REQUESTED)) {
            return;
        }
        progressWaitQueue.shutdown();
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
        if (!isQuiesced()) {
            throw new IllegalStateException("page frame reduce dispatcher has active publications");
        }
        isClosed = true;
        if (messageBus.getPageFrameReduceDispatcher() == this) {
            messageBus.setPageFrameReduceDispatcher(null);
        }
        taskPool.close();
    }

    public boolean consumeOrdered(
            int workerId,
            RingQueue<PageFrameReduceTask> queue,
            MCSequence subSeq,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        if (!hasPendingTasks(subSeq)) {
            return true;
        }
        final Fiber fiber = reserveFiber(stealingFrameSequence != null && !stealingFrameSequence.isActive());
        if (fiber == null) {
            return true;
        }
        final PageFrameFiberTask fiberTask = tryAcquireTask(fiber);
        if (fiberTask == null) {
            return true;
        }
        do {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final PageFrameReduceTask reduceTask = queue.get(cursor);
                final PageFrameSequence<?> frameSequence = reduceTask.getFrameSequence();
                fiberTask.ofOrdered(
                        workerId,
                        subSeq,
                        cursor,
                        reduceTask,
                        frameSequence
                );
                launch(fiber, fiberTask);
                return false;
            }
            if (cursor == -1) {
                taskPool.release(fiberTask);
                runtime.releaseReservedFiber(fiber);
                return true;
            }
            Os.pause();
        } while (true);
    }

    public boolean consumeUnordered(
            int workerId,
            RingQueue<UnorderedPageFrameReduceTask> queue,
            MCSequence subSeq,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        if (!hasPendingTasks(subSeq)) {
            return true;
        }
        final Fiber fiber = reserveFiber(stealingFrameSequence != null && !stealingFrameSequence.isActive());
        if (fiber == null) {
            return true;
        }
        final PageFrameFiberTask fiberTask = tryAcquireTask(fiber);
        if (fiberTask == null) {
            return true;
        }
        do {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final UnorderedPageFrameReduceTask reduceTask = queue.get(cursor);
                final UnorderedPageFrameSequence<?> frameSequence = reduceTask.getFrameSequence();
                final int frameIndex = reduceTask.getFrameIndex();
                final long frameSequenceId = reduceTask.getFrameSequenceId();
                reduceTask.clear();
                subSeq.done(cursor);
                signalProgress();
                if (frameSequenceId != frameSequence.getId()) {
                    LOG.error()
                            .$("skipping stale task [expected=").$(frameSequence.getId())
                            .$(", got=").$(frameSequenceId)
                            .I$();
                    taskPool.release(fiberTask);
                    runtime.releaseReservedFiber(fiber);
                    return false;
                }
                fiberTask.ofUnordered(workerId, frameIndex, frameSequence);
                launch(fiber, fiberTask);
                return false;
            }
            if (cursor == -1) {
                taskPool.release(fiberTask);
                runtime.releaseReservedFiber(fiber);
                return true;
            }
            Os.pause();
        } while (true);
    }

    @TestOnly
    public int getCreatedTaskCount() {
        return taskPool.getCreatedCount();
    }

    public long getProgressVersion() {
        return progressVersion.get();
    }

    @Override
    public boolean isQuiesced() {
        return quiesceState.get() == QUIESCE_DRAINED;
    }

    @Override
    public void progressQuiesce() {
        if (publicationAdmission.get() != 0
                || !quiesceState.compareAndSet(QUIESCE_REQUESTED, QUIESCE_DRAINING)) {
            return;
        }
        boolean isDrained = false;
        try {
            isDrained = drainPublishedTasks();
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

    public boolean tryAcquirePublication() {
        final boolean isCurrentFiberOwned = runtime.isCurrentFiberOwned();
        while (true) {
            final long current = publicationAdmission.get();
            if ((current & PUBLICATION_OPEN) == 0) {
                return false;
            }
            if (isCurrentFiberOwned) {
                throw new IllegalStateException("page frame owner cannot publish work to its current fiber runtime");
            }
            if ((current & PUBLICATION_PERMIT_MASK) == PUBLICATION_PERMIT_MASK) {
                throw new IllegalStateException("page frame publication admission overflow");
            }
            if (publicationAdmission.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    boolean awaitProgress(
            long observedVersion,
            @Nullable FiberCancellationSignal cancellationSignal,
            @Nullable SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final int reason = awaitProgress(observedVersion, cancellationSignal);
        return switch (reason) {
            case FiberWaitCoordinator.REASON_ABORTED ->
                    throw new IllegalStateException("fiber refused page frame progress suspension");
            case FiberWaitCoordinator.REASON_CANCEL -> {
                if (circuitBreaker != null) {
                    circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                }
                yield false;
            }
            case FiberWaitCoordinator.REASON_NONE, FiberWaitCoordinator.REASON_SHUTDOWN -> false;
            case FiberWaitCoordinator.REASON_PROGRESS -> true;
            case FiberWaitCoordinator.REASON_TIMER -> {
                if (circuitBreaker != null) {
                    circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                }
                yield true;
            }
            default -> throw new IllegalStateException(
                    "unexpected page frame progress wait reason [reason=" + reason + ']'
            );
        };
    }

    void signalProgress() {
        progressVersion.incrementAndGet();
        progressWaitQueue.fire();
    }

    private static boolean hasPendingTasks(MCSequence subSeq) {
        final long next = subSeq.current() + 1;
        return subSeq.getBarrier().availableIndex(next) >= next;
    }

    private static IllegalStateException launchFailed(LaunchResult result) {
        return new IllegalStateException("page frame fiber launch failed [result=" + result + ']');
    }

    private boolean drainOrdered(RingQueue<PageFrameReduceTask> queue, MCSequence subSeq) {
        while (true) {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final PageFrameReduceTask task = queue.get(cursor);
                final PageFrameSequence<?> frameSequence = task.getFrameSequence();
                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                try {
                    subSeq.done(cursor);
                } finally {
                    frameSequence.getReduceFinishedCounter().incrementAndGet();
                    signalProgress();
                }
            } else if (cursor == -1) {
                return true;
            } else {
                return false;
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
                signalProgress();
                if (frameSequenceId == frameSequence.getId()) {
                    frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                    frameSequence.getDoneLatch().countDown();
                }
            } else if (cursor == -1) {
                return true;
            } else {
                return false;
            }
        }
    }

    private void launch(Fiber fiber, PageFrameFiberTask task) {
        final LaunchResult result = Fiber.isMounted()
                ? runtime.launchReserved(fiber, task, task.getIncarnation())
                : runtime.launchReservedDirect(fiber, task, task.getIncarnation());
        if (result != LaunchResult.LAUNCHED && task.isBound()) {
            task.abortBeforeLaunch();
        }
        if (result != LaunchResult.LAUNCHED && result != LaunchResult.QUIESCING) {
            throw launchFailed(result);
        }
    }

    private @Nullable Fiber reserveFiber(boolean isDraining) {
        while (true) {
            final Fiber fiber = runtime.tryReserveFiber();
            if (fiber != null || !Fiber.isMounted()) {
                return fiber;
            }
            final int reason = runtime.awaitCapacity(
                    isDraining ? null : SuspensionScope.getCancellationSignal()
            );
            if (reason == FiberWaitCoordinator.REASON_CAPACITY) {
                continue;
            }
            if (reason == FiberWaitCoordinator.REASON_ABORTED) {
                throw new IllegalStateException("fiber refused page frame capacity suspension");
            }
            return null;
        }
    }

    private @Nullable PageFrameFiberTask tryAcquireTask(Fiber fiber) {
        boolean hasFiberReservation = true;
        try {
            final PageFrameFiberTask task = taskPool.tryAcquire();
            if (task != null) {
                hasFiberReservation = false;
            }
            return task;
        } finally {
            if (hasFiberReservation) {
                runtime.releaseReservedFiber(fiber);
            }
        }
    }

}

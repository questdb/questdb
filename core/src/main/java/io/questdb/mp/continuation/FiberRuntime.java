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

package io.questdb.mp.continuation;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public final class FiberRuntime {
    private static final long ADMISSION_OPEN = Long.MIN_VALUE;
    private static final long ADMISSION_PERMIT_MASK = Long.MAX_VALUE;
    private static final Log LOG = LogFactory.getLog(FiberRuntime.class);
    private final AtomicLong admission = new AtomicLong(ADMISSION_OPEN);
    private final @Nullable Runnable beforeFiberAcquireForTesting;
    private final AtomicLong budgetExhaustionCount = new AtomicLong();
    private final FiberEventWaitQueue capacityWaitQueue;
    private final SOCountDownLatch closedLatch = new SOCountDownLatch(1);
    private final FiberPool fiberPool;
    private final AtomicInteger finalizerCount = new AtomicInteger();
    private final AtomicLong inlineSuspendViolationCount = new AtomicLong();
    private volatile boolean isPoolQuiesced;
    private final AtomicLongArray launchCounts = new AtomicLongArray(LaunchResult.COUNT);
    private final int maxLiveFiberCount;
    private final AtomicLong mountCount = new AtomicLong();
    private final AtomicInteger mountedCount = new AtomicInteger();
    private final AtomicInteger outstandingTaskCount = new AtomicInteger();
    private final ObjList<FiberRuntimeQuiesceListener> quiesceListeners = new ObjList<>();
    private final FiberRunQueue runQueue;
    private final AtomicLong saturationCount = new AtomicLong();
    private volatile FiberRuntimeState state = FiberRuntimeState.OPEN;

    public FiberRuntime(int retainedFiberCount) {
        this(retainedFiberCount, retainedFiberCount, null, null);
    }

    public FiberRuntime(int retainedFiberCount, int maxLiveFiberCount) {
        this(retainedFiberCount, maxLiveFiberCount, null, null);
    }

    @TestOnly
    public FiberRuntime(
            int retainedFiberCount,
            int maxLiveFiberCount,
            @Nullable Runnable beforeFiberAcquireForTesting
    ) {
        this(retainedFiberCount, maxLiveFiberCount, beforeFiberAcquireForTesting, null);
    }

    @TestOnly
    public FiberRuntime(
            int retainedFiberCount,
            int maxLiveFiberCount,
            @Nullable Runnable beforeFiberAcquireForTesting,
            @Nullable Runnable beforeWaitFireForTesting
    ) {
        if (maxLiveFiberCount < 1) {
            throw new IllegalArgumentException("maxLiveFiberCount must be positive");
        }
        if (retainedFiberCount < 1 || retainedFiberCount > maxLiveFiberCount) {
            throw new IllegalArgumentException(
                    "retainedFiberCount must be positive and not exceed maxLiveFiberCount"
            );
        }
        this.beforeFiberAcquireForTesting = beforeFiberAcquireForTesting;
        this.maxLiveFiberCount = maxLiveFiberCount;
        this.capacityWaitQueue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_CAPACITY);
        this.runQueue = new FiberRunQueue(maxLiveFiberCount);
        this.fiberPool = new FiberPool(
                retainedFiberCount,
                maxLiveFiberCount,
                this,
                beforeWaitFireForTesting
        );
    }

    public int awaitCapacity() {
        return awaitCapacity(SuspensionScope.getCancellationSignal());
    }

    public int awaitCapacity(@Nullable FiberCancellationSignal cancellationSignal) {
        if (state != FiberRuntimeState.OPEN) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        final Fiber fiber = Fiber.current();
        if (fiber == null || !Fiber.isMounted()) {
            return FiberWaitCoordinator.REASON_NONE;
        }
        final long token;
        try {
            token = fiber.beginWaitBuild(cancellationSignal == null ? 1 : 2);
        } catch (IllegalStateException e) {
            if (state != FiberRuntimeState.OPEN) {
                return FiberWaitCoordinator.REASON_SHUTDOWN;
            }
            throw e;
        }
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        FiberCancellationWaitRegistration cancellationRegistration = null;
        FiberEventWaitRegistration capacityRegistration = null;
        try {
            capacityRegistration = coordinator.acquireEvent(token);
            if (capacityRegistration.register(capacityWaitQueue) != SourceRegistrationResult.ACCEPTED
                    || !coordinator.tryAcceptSource(token)) {
                throw new IllegalStateException("fiber capacity wait registration failed");
            }
            if (cancellationSignal != null) {
                cancellationRegistration = coordinator.acquireCancellation(token, cancellationSignal);
                if (cancellationRegistration.register() != SourceRegistrationResult.ACCEPTED
                        || !coordinator.tryAcceptSource(token)) {
                    throw new IllegalStateException("fiber capacity cancellation registration failed");
                }
            }
            if (state == FiberRuntimeState.OPEN
                    && outstandingTaskCount.get() < maxLiveFiberCount
                    && fiberPool.hasAvailableFiber()) {
                return FiberWaitCoordinator.REASON_CAPACITY;
            }
            return fiber.suspendWait(token);
        } finally {
            if (cancellationRegistration != null) {
                cancellationRegistration.cancel();
            }
            if (capacityRegistration != null) {
                capacityRegistration.cancel();
            }
            coordinator.abort(token);
            coordinator.consume(token);
        }
    }

    public boolean awaitClosed(long deadlineNanos) {
        while (state != FiberRuntimeState.CLOSED) {
            tryClose();
            if (state == FiberRuntimeState.CLOSED) {
                return true;
            }
            final long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                return false;
            }
            closedLatch.await(Math.min(remainingNanos, 1_000_000L));
        }
        return true;
    }

    public void beginQuiesce() {
        synchronized (this) {
            if (state != FiberRuntimeState.OPEN) {
                return;
            }
            while (true) {
                final long current = admission.get();
                if ((current & ADMISSION_OPEN) == 0
                        || admission.compareAndSet(current, current & ADMISSION_PERMIT_MASK)) {
                    break;
                }
            }
            state = FiberRuntimeState.QUIESCING;
        }
        beginQuiesceListeners();
        capacityWaitQueue.shutdown();
        tryClose();
    }

    public void closeAfterDrained() {
        if (state != FiberRuntimeState.CLOSED) {
            throw new IllegalStateException("fiber runtime is not closed [state=" + state + ']');
        }
        fiberPool.clearRegistry();
    }

    public int drain(int attemptBudget) {
        if (attemptBudget < 1) {
            throw new IllegalArgumentException("attemptBudget must be positive");
        }
        int attempts = 0;
        while (attempts < attemptBudget) {
            final Fiber fiber = runQueue.tryDequeue();
            if (fiber == null) {
                break;
            }
            attempts++;
            process(fiber, fiber.getOutcomeScratch(), false);
        }
        if (attempts == attemptBudget && runQueue.depth() > 0) {
            budgetExhaustionCount.incrementAndGet();
        }
        tryClose();
        return attempts;
    }

    public long getBudgetExhaustionCount() {
        return budgetExhaustionCount.get();
    }

    public int getCreatedFiberCount() {
        return fiberPool.getCreatedCount();
    }

    public int getFinalizerCount() {
        return finalizerCount.get();
    }

    public long getInlineSuspendViolationCount() {
        return inlineSuspendViolationCount.get();
    }

    public long getLaunchCount(LaunchResult result) {
        return launchCounts.get(result.ordinal());
    }

    public int getLiveFiberCount() {
        return fiberPool.getLiveCount();
    }

    public int getMaxLiveFiberCount() {
        return maxLiveFiberCount;
    }

    public long getMountCount() {
        return mountCount.get();
    }

    public int getMountedCount() {
        return mountedCount.get();
    }

    public int getOutstandingTaskCount() {
        return outstandingTaskCount.get();
    }

    public int getParkedFiberCount() {
        return fiberPool.getParkedCount();
    }

    public int getQueuedCount() {
        return runQueue.depth();
    }

    public int getRetainedFiberCount() {
        return fiberPool.getRetainedCount();
    }

    public int getRetiredFiberCount() {
        return fiberPool.getRetiredCount();
    }

    @TestOnly
    public int getRunQueueCapacity() {
        return runQueue.capacity();
    }

    public long getSaturationCount() {
        return saturationCount.get();
    }

    public void initializeCarrier() {
        SuspensionScope.initializeCarrier();
        Fiber.initializeCarrier();
    }

    public LaunchResult launch(FiberTask task) {
        return launch(task, task.getIncarnation());
    }

    public LaunchResult launch(FiberTask task, long taskIncarnation) {
        final LaunchResult result = preflight(task, taskIncarnation);
        if (result != null) {
            return record(result);
        }
        final Fiber fiber;
        try {
            fiber = tryReserveFiber();
        } catch (Throwable th) {
            return record(LaunchResult.RESOURCE_FAILURE);
        }
        if (fiber == null) {
            return record(state == FiberRuntimeState.OPEN ? LaunchResult.SATURATED : LaunchResult.QUIESCING);
        }
        return launchReserved(fiber, task, taskIncarnation);
    }

    public LaunchResult launchReserved(Fiber fiber, FiberTask task, long taskIncarnation) {
        if (fiber.isForeignTo(this)) {
            throw new IllegalArgumentException("fiber reservation does not belong to this runtime");
        }
        if (!fiber.isReserved()) {
            throw new IllegalArgumentException("fiber is not reserved");
        }
        if (!acquireAdmission()) {
            releaseReservedFiber(fiber);
            return record(LaunchResult.QUIESCING);
        }
        Fiber reservedFiber = fiber;
        boolean isReserved = true;
        try {
            if (task.getIncarnation() != taskIncarnation) {
                return record(LaunchResult.STALE_INCARNATION);
            }
            final int state = task.getScheduleState();
            if (state == FiberTask.STATE_OWNED) {
                return record(LaunchResult.ALREADY_OWNED);
            }
            if (task.isDone()) {
                return record(LaunchResult.TERMINAL);
            }
            final int claim = task.claim(taskIncarnation);
            switch (claim) {
                case FiberTask.CLAIM_LAUNCHED:
                    reservedFiber.stageAndRequestRun(task);
                    reservedFiber = null;
                    isReserved = false;
                    return record(LaunchResult.LAUNCHED);
                case FiberTask.CLAIM_ALREADY_OWNED:
                case FiberTask.CLAIM_SIGNALLED:
                    return record(LaunchResult.ALREADY_OWNED);
                case FiberTask.CLAIM_STALE:
                    return record(LaunchResult.STALE_INCARNATION);
                default:
                    return record(LaunchResult.TERMINAL);
            }
        } catch (Throwable e) {
            if (isReserved) {
                terminalError(task, e);
                isReserved = false;
            }
            return record(LaunchResult.TERMINAL);
        } finally {
            if (reservedFiber != null) {
                reservedFiber.releaseReservation();
                fiberPool.release(reservedFiber);
            }
            if (isReserved) {
                releaseTaskSlot();
            }
            releaseAdmission();
        }
    }

    public LaunchResult launchReservedDirect(Fiber fiber, FiberTask task, long taskIncarnation) {
        if (fiber.isForeignTo(this)) {
            throw new IllegalArgumentException("fiber reservation does not belong to this runtime");
        }
        if (!fiber.isReserved()) {
            throw new IllegalArgumentException("fiber is not reserved");
        }
        if (!acquireAdmission()) {
            releaseReservedFiber(fiber);
            return record(LaunchResult.QUIESCING);
        }
        Fiber directFiber = null;
        Fiber reservedFiber = fiber;
        boolean isReserved = true;
        LaunchResult result;
        try {
            if (task.getIncarnation() != taskIncarnation) {
                result = LaunchResult.STALE_INCARNATION;
            } else {
                final int state = task.getScheduleState();
                if (state == FiberTask.STATE_OWNED) {
                    result = LaunchResult.ALREADY_OWNED;
                } else if (task.isDone()) {
                    result = LaunchResult.TERMINAL;
                } else {
                    final int claim = task.claim(taskIncarnation);
                    switch (claim) {
                        case FiberTask.CLAIM_LAUNCHED:
                            if (reservedFiber.stageForDirectMountOrRequestRun(task)) {
                                directFiber = reservedFiber;
                            }
                            reservedFiber = null;
                            isReserved = false;
                            result = LaunchResult.LAUNCHED;
                            break;
                        case FiberTask.CLAIM_ALREADY_OWNED:
                        case FiberTask.CLAIM_SIGNALLED:
                            result = LaunchResult.ALREADY_OWNED;
                            break;
                        case FiberTask.CLAIM_STALE:
                            result = LaunchResult.STALE_INCARNATION;
                            break;
                        default:
                            result = LaunchResult.TERMINAL;
                            break;
                    }
                }
            }
        } catch (Throwable e) {
            if (isReserved) {
                terminalError(task, e);
                isReserved = false;
            }
            result = LaunchResult.TERMINAL;
        } finally {
            if (reservedFiber != null) {
                reservedFiber.releaseReservation();
                fiberPool.release(reservedFiber);
            }
            if (isReserved) {
                releaseTaskSlot();
            }
            releaseAdmission();
        }
        if (directFiber != null) {
            process(directFiber, directFiber.getOutcomeScratch(), true);
        }
        return record(result);
    }

    public synchronized void registerQuiesceListener(FiberRuntimeQuiesceListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("fiber runtime quiesce listener must not be null");
        }
        if (state != FiberRuntimeState.OPEN) {
            throw new IllegalStateException("fiber runtime is not open");
        }
        quiesceListeners.add(listener);
    }

    public void releaseReservedFiber(Fiber fiber) {
        if (fiber.isForeignTo(this)) {
            throw new IllegalArgumentException("fiber reservation does not belong to this runtime");
        }
        if (!fiber.isReserved()) {
            throw new IllegalArgumentException("fiber is not reserved");
        }
        try {
            fiber.releaseReservation();
            fiberPool.release(fiber);
        } finally {
            releaseTaskSlot();
        }
    }

    @TestOnly
    public void setRunQueueDepthForTesting(int depth) {
        runQueue.setDepthForTesting(depth);
    }

    public FiberRuntimeState state() {
        return state;
    }

    @Nullable
    public Fiber tryReserveFiber() {
        if (!acquireAdmission()) {
            return null;
        }
        boolean isReserved = false;
        try {
            while (true) {
                final int count = outstandingTaskCount.get();
                if (count >= maxLiveFiberCount) {
                    saturationCount.incrementAndGet();
                    return null;
                }
                if (outstandingTaskCount.compareAndSet(count, count + 1)) {
                    isReserved = true;
                    break;
                }
            }
            final Runnable hook = beforeFiberAcquireForTesting;
            if (hook != null) {
                hook.run();
            }
            final Fiber fiber = fiberPool.tryAcquire();
            if (fiber != null) {
                isReserved = false;
            } else {
                saturationCount.incrementAndGet();
            }
            return fiber;
        } finally {
            if (isReserved) {
                releaseTaskSlot();
            }
            releaseAdmission();
        }
    }

    boolean acquireAdmission() {
        while (true) {
            final long current = admission.get();
            if ((current & ADMISSION_OPEN) == 0) {
                return false;
            }
            if ((current & ADMISSION_PERMIT_MASK) == ADMISSION_PERMIT_MASK) {
                throw new IllegalStateException("fiber runtime admission overflow");
            }
            if (admission.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    void enqueue(Fiber fiber) {
        runQueue.put(fiber);
    }

    void onInlineSuspendViolation() {
        inlineSuspendViolationCount.incrementAndGet();
    }

    void releaseAdmission() {
        final long value = admission.decrementAndGet();
        if ((value & ADMISSION_PERMIT_MASK) == ADMISSION_PERMIT_MASK) {
            throw new IllegalStateException("fiber runtime admission underflow");
        }
    }

    void signalCapacity() {
        if (state == FiberRuntimeState.OPEN) {
            capacityWaitQueue.fire();
        }
    }

    private void advanceQuiesce() {
        if (state != FiberRuntimeState.QUIESCING
                || isPoolQuiesced
                || (admission.get() & ADMISSION_PERMIT_MASK) != 0
                || !isListenerQuiesceComplete()) {
            return;
        }
        synchronized (this) {
            if (state == FiberRuntimeState.QUIESCING
                    && !isPoolQuiesced
                    && (admission.get() & ADMISSION_PERMIT_MASK) == 0
                    && isListenerQuiesced()) {
                fiberPool.beginQuiesce();
                isPoolQuiesced = true;
            }
        }
    }

    private void beginQuiesceListeners() {
        for (int i = 0, n = quiesceListeners.size(); i < n; i++) {
            try {
                quiesceListeners.getQuick(i).beginQuiesce();
            } catch (Throwable th) {
                LOG.critical().$("fiber runtime quiesce listener failed [error=").$(th).I$();
            }
        }
    }

    private void completeAbandoned(FiberTask task, boolean isOwned) {
        releaseTaskSlot();
        try {
            if (isOwned) {
                task.markCancelledFromOwned();
            }
        } catch (Throwable th) {
            LOG.error().$("fiber task terminal transition failed [error=").$(th).I$();
        }
        try {
            task.notifyAbandoned();
        } catch (Throwable th) {
            LOG.error().$("fiber task abandonment callback failed [error=").$(th).I$();
        }
        notifyDone(task);
    }

    private void completeDone(FiberTask task) {
        releaseTaskSlot();
        try {
            task.markDoneFromOwned();
        } catch (Throwable th) {
            LOG.error().$("fiber task terminal transition failed [error=").$(th).I$();
        }
        notifyDone(task);
    }

    private void completeError(FiberTask task, Throwable th) {
        releaseTaskSlot();
        try {
            if (!task.isDone()) {
                task.markDoneFromOwned();
            }
        } catch (Throwable transitionError) {
            LOG.error().$("fiber task terminal transition failed [error=").$(transitionError).I$();
        }
        try {
            task.notifyError(th);
        } catch (Throwable callbackError) {
            LOG.error().$("fiber task error callback failed [error=").$(callbackError).I$();
        }
        notifyDone(task);
    }

    private void finalizeOutcome(Fiber.Outcome outcome) {
        final FiberTask task = outcome.task;
        if (task == null) {
            return;
        }
        finalizerCount.incrementAndGet();
        try {
            switch (outcome.type) {
                case Fiber.OUTCOME_ABANDONED:
                    completeAbandoned(task, true);
                    break;
                case Fiber.OUTCOME_DONE:
                    completeDone(task);
                    break;
                case Fiber.OUTCOME_ERROR:
                    completeError(task, outcome.error);
                    break;
                default:
                    throw new IllegalStateException("missing fiber task outcome");
            }
        } catch (Throwable th) {
            LOG.error().$("fiber task finalization failed [error=").$(th).I$();
        } finally {
            finalizerCount.decrementAndGet();
        }
    }

    private boolean finalizePark(Fiber fiber, FiberTask task) {
        finalizerCount.incrementAndGet();
        boolean hasFiberOwnership = true;
        try {
            if (!acquireAdmission()) {
                fiberPool.release(fiber);
                hasFiberOwnership = false;
                completeAbandoned(task, true);
                return false;
            }
            try {
                task.preparePark();
                if (!task.beginArming()) {
                    throw new IllegalStateException("fiber task is not owned");
                }
                task.publishPark();
                final int result = task.resolveArming();
                if (result == FiberTask.PARK_IDLE) {
                    fiberPool.release(fiber);
                    hasFiberOwnership = false;
                    releaseTaskSlot();
                } else if (result == FiberTask.PARK_RELAUNCH) {
                    fiber.restageAndRequestRun(task);
                } else {
                    fiberPool.release(fiber);
                    hasFiberOwnership = false;
                    completeAbandoned(task, false);
                }
            } catch (Throwable th) {
                task.abortArming();
                if (hasFiberOwnership) {
                    fiberPool.release(fiber);
                    hasFiberOwnership = false;
                }
                terminalError(task, th);
            } finally {
                releaseAdmission();
            }
            return hasFiberOwnership;
        } finally {
            finalizerCount.decrementAndGet();
        }
    }

    private boolean handleDriverFailure(
            Fiber fiber,
            Fiber.Outcome outcome,
            boolean hasFiberOwnership,
            Throwable th
    ) {
        if (!hasFiberOwnership) {
            return false;
        }
        final FiberTask task = fiber.detachTaskAfterDriverFailure(outcome);
        try {
            fiberPool.retireAfterDriverFailure(fiber);
        } catch (Throwable retirementError) {
            LOG.critical().$("fiber quarantine failed [error=").$(retirementError).I$();
        }
        if (task != null && task.abortArming()) {
            terminalError(task, th);
        }
        return true;
    }

    private boolean isListenerQuiesceComplete() {
        boolean isComplete = true;
        for (int i = 0, n = quiesceListeners.size(); i < n; i++) {
            final FiberRuntimeQuiesceListener listener = quiesceListeners.getQuick(i);
            try {
                listener.progressQuiesce();
                isComplete &= listener.isQuiesced();
            } catch (Throwable th) {
                LOG.critical().$("fiber runtime quiesce listener failed [error=").$(th).I$();
                isComplete = false;
            }
        }
        return isComplete;
    }

    private boolean isListenerQuiesced() {
        for (int i = 0, n = quiesceListeners.size(); i < n; i++) {
            if (!quiesceListeners.getQuick(i).isQuiesced()) {
                return false;
            }
        }
        return true;
    }

    private void process(Fiber fiber, Fiber.Outcome outcome, boolean isDirectMount) {
        if (!isDirectMount && !fiber.beginProcessing()) {
            LOG.critical().$("fiber queue invariant failed [state=").$(fiber.getExecutionState()).I$();
            return;
        }
        boolean hasFiberOwnership = true;
        boolean isTerminated = false;
        outcome.clear();
        try {
            if (!fiber.beginMount()) {
                throw new IllegalStateException(
                        "fiber mount state invariant failed [state=" + fiber.getExecutionState() + ']'
                );
            }
            mountedCount.incrementAndGet();
            mountCount.incrementAndGet();
            try {
                fiber.runMounted();
            } catch (IllegalStateException e) {
                LOG.critical().$("fiber mount failed [error=").$(e).I$();
                if (fiber.isDone() || !fiber.transitionMountedToRunnable()) {
                    throw e;
                }
                fiber.requestRun();
                return;
            } finally {
                mountedCount.decrementAndGet();
            }
            if (fiber.isDone()) {
                fiber.takeOutcome(outcome);
                fiber.markRetired();
                fiberPool.onRetired(fiber);
                hasFiberOwnership = false;
                finalizeOutcome(outcome);
            } else if (fiber.getYieldReason() == Fiber.YIELD_WAIT) {
                fiber.publishWaiting();
            } else {
                fiber.takeOutcome(outcome);
                if (!fiber.transitionMountedToFree()) {
                    throw new IllegalStateException("fiber did not unmount to free");
                }
                if (outcome.type == Fiber.OUTCOME_PARKED) {
                    hasFiberOwnership = finalizePark(fiber, outcome.task);
                } else {
                    fiberPool.release(fiber);
                    hasFiberOwnership = false;
                    finalizeOutcome(outcome);
                }
            }
        } catch (Throwable th) {
            LOG.critical().$("fiber driver failed [error=").$(th).I$();
            isTerminated = handleDriverFailure(fiber, outcome, hasFiberOwnership, th);
        } finally {
            outcome.clear();
            try {
                if (isTerminated) {
                    fiber.finishTerminatedProcessing();
                } else {
                    fiber.finishProcessing();
                }
            } catch (Throwable th) {
                LOG.critical().$("fiber notification finalization failed [error=").$(th).I$();
                if (!isTerminated
                        && (hasFiberOwnership || fiber.getExecutionState() == Fiber.EXECUTION_RUNNABLE)) {
                    isTerminated = handleDriverFailure(fiber, outcome, true, th);
                    if (isTerminated) {
                        try {
                            fiber.finishTerminatedProcessing();
                        } catch (Throwable notificationError) {
                            LOG.critical().$("fiber terminal notification finalization failed [error=")
                                    .$(notificationError).I$();
                        }
                    }
                }
            }
        }
    }

    private LaunchResult preflight(FiberTask task, long taskIncarnation) {
        while (true) {
            if (task.getIncarnation() != taskIncarnation) {
                return LaunchResult.STALE_INCARNATION;
            }
            final int scheduleState = task.getScheduleState();
            if (scheduleState == FiberTask.STATE_OWNED) {
                return LaunchResult.ALREADY_OWNED;
            }
            if (task.isDone()) {
                return LaunchResult.TERMINAL;
            }
            if (scheduleState == FiberTask.STATE_IDLE) {
                return null;
            }
            if (task.signalAxisA(taskIncarnation, FiberTask.SIGNAL_READY)) {
                return LaunchResult.ALREADY_OWNED;
            }
        }
    }

    private LaunchResult record(LaunchResult result) {
        launchCounts.incrementAndGet(result.ordinal());
        return result;
    }

    private void releaseTaskSlot() {
        final int count = outstandingTaskCount.decrementAndGet();
        if (count < 0) {
            outstandingTaskCount.incrementAndGet();
            throw new IllegalStateException("fiber runtime task slot underflow");
        }
        signalCapacity();
    }

    private void terminalError(FiberTask task, Throwable th) {
        completeError(task, th);
    }

    private void tryClose() {
        if (state != FiberRuntimeState.QUIESCING) {
            return;
        }
        if (!isPoolQuiesced) {
            advanceQuiesce();
        }
        if (state == FiberRuntimeState.QUIESCING
                && isPoolQuiesced
                && outstandingTaskCount.get() == 0
                && finalizerCount.get() == 0
                && runQueue.depth() == 0
                && fiberPool.getCreatedCount() == fiberPool.getRetiredCount()
                && !fiberPool.hasInFlightWaitRegistrations()) {
            state = FiberRuntimeState.CLOSED;
            closedLatch.countDown();
        }
    }

    private static void notifyDone(FiberTask task) {
        try {
            task.notifyDone();
        } catch (Throwable th) {
            LOG.error().$("fiber task completion callback failed [error=").$(th).I$();
        }
    }
}

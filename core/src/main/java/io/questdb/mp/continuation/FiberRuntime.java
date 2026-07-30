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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class FiberRuntime {
    private static final long ADMISSION_OPEN = Long.MIN_VALUE;
    private static final long ADMISSION_PERMIT_MASK = Long.MAX_VALUE;
    private static final Log LOG = LogFactory.getLog(FiberRuntime.class);
    private final AtomicLong admission = new AtomicLong(ADMISSION_OPEN);
    private volatile @Nullable Runnable afterProcessForTesting;
    private final @Nullable Runnable beforeFiberAcquireForTesting;
    private final LongAdder budgetExhaustionCount = new LongAdder();
    private final FiberEventWaitQueue capacityWaitQueue;
    private final SOCountDownLatch closedLatch = new SOCountDownLatch(1);
    private final FiberPool fiberPool;
    private final AtomicInteger finalizerCount = new AtomicInteger();
    private final LongAdder inlineSuspendViolationCount = new LongAdder();
    private final AtomicBoolean isInlineSuspendViolationLogged = new AtomicBoolean();
    private volatile boolean isPoolQuiesced;
    private final ObjList<LongAdder> launchCounts = new ObjList<>(LaunchResult.COUNT);
    private final int maxLiveFiberCount;
    private final LongAdder mountCount = new LongAdder();
    private final LongAdder mountedCount = new LongAdder();
    private final AtomicInteger outstandingTaskCount = new AtomicInteger();
    private final ObjList<FiberRuntimeQuiesceListener> quiesceListeners = new ObjList<>();
    private final FiberRunQueue runQueue;
    private final LongAdder saturationCount = new LongAdder();
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
        for (int i = 0; i < LaunchResult.COUNT; i++) {
            launchCounts.add(new LongAdder());
        }
    }

    public int awaitCapacity() {
        return awaitCapacity(
                SuspensionScope.getCancellationSignal(),
                SuspensionScope.getCancellationSignalGeneration()
        );
    }

    public int awaitCapacity(@Nullable FiberCancellationSignal cancellationSignal) {
        return awaitCapacity(
                cancellationSignal,
                cancellationSignal != null ? cancellationSignal.getGeneration() : CancellationBinding.NO_GENERATION
        );
    }

    public int awaitCapacity(
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration
    ) {
        if (state != FiberRuntimeState.OPEN) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        final Fiber fiber = Fiber.current();
        if (fiber == null || !Fiber.isMounted()) {
            return FiberWaitCoordinator.REASON_NONE;
        }
        final long token = fiber.tryBeginWaitBuild(cancellationSignal == null ? 1 : 2);
        if (token == Fiber.TOKEN_REFUSED) {
            return FiberWaitCoordinator.REASON_SHUTDOWN;
        }
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        try {
            if (!coordinator.armEvent(token, capacityWaitQueue)) {
                throw new IllegalStateException("fiber capacity wait registration failed");
            }
            if (cancellationSignal != null
                    && !coordinator.armCancellation(token, cancellationSignal, cancellationSignalGeneration)) {
                throw new IllegalStateException("fiber capacity cancellation registration failed");
            }
            if (state == FiberRuntimeState.OPEN
                    && outstandingTaskCount.get() < maxLiveFiberCount
                    && fiberPool.hasAvailableFiber()) {
                return FiberWaitCoordinator.REASON_CAPACITY;
            }
            return fiber.suspendWait(token);
        } finally {
            coordinator.teardownWait(token);
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
        try {
            beginQuiesceListeners();
            capacityWaitQueue.shutdown();
        } finally {
            tryClose();
        }
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
            if (process(fiber, fiber.getOutcomeScratch(), false)) {
                finishProcessingAfterUnmount(fiber);
            }
        }
        if (attempts == attemptBudget && runQueue.depth() > 0) {
            budgetExhaustionCount.increment();
        }
        tryClose();
        return attempts;
    }

    public long getBudgetExhaustionCount() {
        return budgetExhaustionCount.sum();
    }

    public int getCreatedFiberCount() {
        return fiberPool.getCreatedCount();
    }

    public int getFinalizerCount() {
        return finalizerCount.get();
    }

    public long getInlineSuspendViolationCount() {
        return inlineSuspendViolationCount.sum();
    }

    public long getLaunchCount(LaunchResult result) {
        return launchCounts.getQuick(result.ordinal()).sum();
    }

    public int getLiveFiberCount() {
        return fiberPool.getLiveCount();
    }

    public int getMaxLiveFiberCount() {
        return maxLiveFiberCount;
    }

    public long getMountCount() {
        return mountCount.sum();
    }

    public int getMountedCount() {
        return mountedCount.intValue();
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
        return saturationCount.sum();
    }

    public void initializeCarrier() {
        SuspensionScope.initializeCarrier();
    }

    public boolean isCurrentFiberOwned() {
        if (!Fiber.isMounted()) {
            return false;
        }
        final Fiber fiber = Fiber.current();
        return fiber != null && !fiber.isForeignTo(this);
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
            LOG.critical().$("fiber reservation failed [error=").$(th).I$();
            return record(LaunchResult.RESOURCE_FAILURE);
        }
        if (fiber == null) {
            return record(state == FiberRuntimeState.OPEN ? LaunchResult.SATURATED : LaunchResult.QUIESCING);
        }
        return launchReserved(fiber, task, taskIncarnation);
    }

    public LaunchResult launchReserved(Fiber fiber, FiberTask task, long taskIncarnation) {
        return launchReserved(fiber, task, taskIncarnation, false);
    }

    public LaunchResult launchReservedDirect(Fiber fiber, FiberTask task, long taskIncarnation) {
        return launchReserved(fiber, task, taskIncarnation, true);
    }

    private LaunchResult launchReserved(
            Fiber fiber,
            FiberTask task,
            long taskIncarnation,
            boolean isDirectMountAllowed
    ) {
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
            // claim() folds the incarnation, ownership and terminal checks into its CAS loop
            final int claim = task.claim(taskIncarnation);
            switch (claim) {
                case FiberTask.CLAIM_LAUNCHED -> {
                    if (isDirectMountAllowed) {
                        if (reservedFiber.stageForDirectMountOrRequestRun(task)) {
                            directFiber = reservedFiber;
                        }
                    } else {
                        reservedFiber.stageAndRequestRun(task);
                    }
                    reservedFiber = null;
                    isReserved = false;
                    result = LaunchResult.LAUNCHED;
                }
                case FiberTask.CLAIM_ALREADY_OWNED, FiberTask.CLAIM_SIGNALLED ->
                        result = LaunchResult.ALREADY_OWNED;
                case FiberTask.CLAIM_STALE -> result = LaunchResult.STALE_INCARNATION;
                default -> result = LaunchResult.TERMINAL;
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
            if (process(directFiber, directFiber.getOutcomeScratch(), true)) {
                finishProcessingAfterUnmount(directFiber);
            }
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
    public void setAfterProcessForTesting(@Nullable Runnable afterProcessForTesting) {
        this.afterProcessForTesting = afterProcessForTesting;
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
            // xadd instead of a CAS loop; the transient overshoot is only ever read by
            // conservative checks, and the rollback happens under the admission permit
            if (outstandingTaskCount.getAndIncrement() >= maxLiveFiberCount) {
                outstandingTaskCount.decrementAndGet();
                saturationCount.increment();
                return null;
            }
            isReserved = true;
            final Runnable hook = beforeFiberAcquireForTesting;
            if (hook != null) {
                hook.run();
            }
            final Fiber fiber = fiberPool.tryAcquire();
            if (fiber != null) {
                isReserved = false;
            } else {
                saturationCount.increment();
            }
            return fiber;
        } finally {
            if (isReserved) {
                releaseTaskSlot();
            }
            releaseAdmission();
        }
    }

    private static IllegalStateException mountInvariantFailed(int state) {
        return new IllegalStateException("fiber mount state invariant failed [state=" + state + ']');
    }

    private static void notifyDone(FiberTask task) {
        try {
            task.notifyDone();
        } catch (Throwable th) {
            LOG.error().$("fiber task completion callback failed [error=").$(th).I$();
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
                case Fiber.OUTCOME_ABANDONED -> completeAbandoned(task, true);
                case Fiber.OUTCOME_DONE -> completeDone(task);
                case Fiber.OUTCOME_ERROR -> completeError(task, outcome.error);
                default -> throw new IllegalStateException("missing fiber task outcome");
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

    private void finishProcessingAfterUnmount(Fiber fiber) {
        try {
            fiber.finishProcessing();
        } catch (Throwable th) {
            LOG.critical().$("fiber notification finalization failed [error=").$(th).I$();
            final Fiber.Outcome outcome = fiber.getOutcomeScratch();
            outcome.clear();
            if (handleDriverFailure(fiber, outcome, true, th)) {
                try {
                    fiber.finishTerminatedProcessing();
                } catch (Throwable notificationError) {
                    LOG.critical().$("fiber terminal notification finalization failed [error=")
                            .$(notificationError).I$();
                }
            }
            outcome.clear();
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
        // terminal callbacks release the task slot, so they must run inside the finalizer guard
        // tryClose() checks
        finalizerCount.incrementAndGet();
        try {
            final FiberTask task = fiber.detachTaskAfterDriverFailure(outcome);
            try {
                fiberPool.retireAfterDriverFailure(fiber);
            } catch (Throwable retirementError) {
                LOG.critical().$("fiber quarantine failed [error=").$(retirementError).I$();
            }
            if (task != null && task.abortArming()) {
                terminalError(task, th);
            }
        } finally {
            finalizerCount.decrementAndGet();
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

    private boolean process(Fiber fiber, Fiber.Outcome outcome, boolean isDirectMount) {
        if (!isDirectMount && !fiber.beginProcessing()) {
            LOG.critical().$("fiber queue invariant failed [state=").$(fiber.getExecutionState()).I$();
            return false;
        }
        boolean hasFiberOwnership = true;
        boolean isTerminated = false;
        outcome.clear();
        try {
            if (!fiber.beginMount()) {
                throw mountInvariantFailed(fiber.getExecutionState());
            }
            mountedCount.increment();
            mountCount.increment();
            try {
                fiber.runMounted();
            } finally {
                mountedCount.decrement();
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
            if (isTerminated) {
                try {
                    fiber.finishTerminatedProcessing();
                } catch (Throwable th) {
                    LOG.critical().$("fiber terminal notification finalization failed [error=").$(th).I$();
                }
            }
        }
        final Runnable hook = afterProcessForTesting;
        if (hook != null) {
            hook.run();
        }
        return !isTerminated;
    }

    private LaunchResult record(LaunchResult result) {
        launchCounts.getQuick(result.ordinal()).increment();
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

    void onInlineSuspendViolation(CharSequence pinnedReason) {
        inlineSuspendViolationCount.increment();
        if (isInlineSuspendViolationLogged.compareAndSet(false, true)) {
            LOG.critical().$("fiber suspension refused, carrier is pinned [reason=").$(pinnedReason).I$();
        }
    }

    void releaseAdmission() {
        final long value = admission.decrementAndGet();
        if ((value & ADMISSION_PERMIT_MASK) == ADMISSION_PERMIT_MASK) {
            // the open flag is the sign bit: an unrestored underflow would close admission for good
            admission.incrementAndGet();
            throw new IllegalStateException("fiber runtime admission underflow");
        }
    }

    void signalCapacity() {
        if (state == FiberRuntimeState.OPEN) {
            capacityWaitQueue.fire();
        }
    }
}

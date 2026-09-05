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
    private static final long DRAIN_TIME_BUDGET_NANOS = 2_000_000L;
    private static final Log LOG = LogFactory.getLog(FiberRuntime.class);
    private static final int PROCESS_OWNED = 2;
    private static final int PROCESS_RELEASED = 1;
    private static final int PROCESS_TERMINATED = 0;
    private final AtomicLong admission = new AtomicLong(ADMISSION_OPEN);
    private final @Nullable Runnable beforeFiberAcquireForTesting;
    private final LongAdder budgetExhaustionCount = new LongAdder();
    private final FiberEventWaitQueue capacityWaitQueue;
    private final SOCountDownLatch closedLatch = new SOCountDownLatch(1);
    private final ObjList<FiberRuntimeConfigurationListener> configurationListeners = new ObjList<>();
    private final FiberPool fiberPool;
    private final AtomicInteger finalizerCount = new AtomicInteger();
    private final LongAdder inlineSuspendViolationCount = new LongAdder();
    private final AtomicBoolean isInlineSuspendViolationLogged = new AtomicBoolean();
    private final AtomicBoolean isQuiesceListenerPassActive = new AtomicBoolean();
    private final ObjList<LongAdder> launchCounts = new ObjList<>(LaunchResult.COUNT);
    private final LongAdder mountCount = new LongAdder();
    private final LongAdder mountedCount = new LongAdder();
    private final AtomicInteger outstandingTaskCount = new AtomicInteger();
    private final ObjList<FiberRuntimeQuiesceListener> quiesceListeners = new ObjList<>();
    private final FiberRunQueue runQueue;
    private final LongAdder saturationCount = new LongAdder();
    private volatile @Nullable Runnable afterProcessForTesting;
    private volatile @Nullable Runnable afterReservationReleaseForTesting;
    private volatile Configuration configuration;
    private volatile boolean isPoolQuiesced;
    private volatile FiberRuntimeState state = FiberRuntimeState.OPEN;

    public FiberRuntime(int retainedFiberCount) {
        this(retainedFiberCount, retainedFiberCount, 64, null, null);
    }

    public FiberRuntime(int retainedFiberCount, int maxLiveFiberCount) {
        this(retainedFiberCount, maxLiveFiberCount, 64, null, null);
    }

    public FiberRuntime(int retainedFiberCount, int maxLiveFiberCount, int mountBudget) {
        this(retainedFiberCount, maxLiveFiberCount, mountBudget, null, null);
    }

    @TestOnly
    public FiberRuntime(
            int retainedFiberCount,
            int maxLiveFiberCount,
            @Nullable Runnable beforeFiberAcquireForTesting
    ) {
        this(retainedFiberCount, maxLiveFiberCount, 64, beforeFiberAcquireForTesting, null);
    }

    @TestOnly
    public FiberRuntime(
            int retainedFiberCount,
            int maxLiveFiberCount,
            @Nullable Runnable beforeFiberAcquireForTesting,
            @Nullable Runnable beforeWaitFireForTesting
    ) {
        this(retainedFiberCount, maxLiveFiberCount, 64, beforeFiberAcquireForTesting, beforeWaitFireForTesting);
    }

    private FiberRuntime(
            int retainedFiberCount,
            int maxLiveFiberCount,
            int mountBudget,
            @Nullable Runnable beforeFiberAcquireForTesting,
            @Nullable Runnable beforeWaitFireForTesting
    ) {
        try {
            Fiber.verifyRuntimeAccess();
        } catch (LinkageError e) {
            throw new IllegalStateException(
                    "fiber-host mode requires --add-exports=java.base/jdk.internal.vm=io.questdb"
                            + " on a module-path launch, or --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
                            + " on a class-path launch; set *.worker.fiber.enabled=false to run legacy pools",
                    e
            );
        }
        if (maxLiveFiberCount < 1) {
            throw new IllegalArgumentException("maxLiveFiberCount must be positive");
        }
        if (retainedFiberCount < 1 || retainedFiberCount > maxLiveFiberCount) {
            throw new IllegalArgumentException(
                    "retainedFiberCount must be positive and not exceed maxLiveFiberCount"
            );
        }
        if (mountBudget < 1) {
            throw new IllegalArgumentException("mountBudget must be positive");
        }
        this.beforeFiberAcquireForTesting = beforeFiberAcquireForTesting;
        this.configuration = new Configuration(maxLiveFiberCount, retainedFiberCount, mountBudget);
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
        boolean isWaitTornDown = false;
        try {
            if (!coordinator.armEvent(token, capacityWaitQueue)) {
                throw new IllegalStateException("fiber capacity wait registration failed");
            }
            if (cancellationSignal != null
                    && !coordinator.armCancellation(token, cancellationSignal, cancellationSignalGeneration)) {
                throw new IllegalStateException("fiber capacity cancellation registration failed");
            }
            if (state == FiberRuntimeState.OPEN
                    && outstandingTaskCount.get() < configuration.maxLiveFiberCount
                    && fiberPool.hasAvailableFiber()) {
                return coordinator.preferPendingCancel(token, FiberWaitCoordinator.REASON_CAPACITY);
            }
            return fiber.suspendWait(token, FiberWaitCoordinator.REASON_NONE);
        } catch (RuntimeException | Error th) {
            isWaitTornDown = true;
            try {
                coordinator.teardownWait(token);
            } catch (RuntimeException | Error cleanupFailure) {
                if (cleanupFailure != th) {
                    th.addSuppressed(cleanupFailure);
                }
            }
            try {
                capacityWaitQueue.fire();
            } catch (RuntimeException | Error cleanupFailure) {
                if (cleanupFailure != th) {
                    th.addSuppressed(cleanupFailure);
                }
            }
            throw th;
        } finally {
            if (!isWaitTornDown) {
                coordinator.teardownWait(token);
            }
        }
    }

    public void awaitClosed() {
        while (state != FiberRuntimeState.CLOSED) {
            tryClose();
            if (state != FiberRuntimeState.CLOSED) {
                closedLatch.await(1_000_000L);
            }
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
            isQuiesceListenerPassActive.set(true);
            state = FiberRuntimeState.QUIESCING;
            configurationListeners.clear();
        }
        try {
            beginQuiesceListeners();
            capacityWaitQueue.shutdown();
        } finally {
            isQuiesceListenerPassActive.set(false);
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
        if (SuspensionScope.hasAnyRoleSwitchLock(SuspensionScope.scope())) {
            tryClose();
            return 0;
        }
        int attempts = 0;
        long drainStartNanos = 0;
        while (attempts < attemptBudget) {
            final Fiber fiber = runQueue.tryDequeue();
            if (fiber == null) {
                break;
            }
            if (attempts == 0) {
                drainStartNanos = System.nanoTime();
            }
            attempts++;
            final int processResult = process(fiber, false);
            // Capture the yield reason before finalization can republish the fiber to another carrier.
            final boolean isCooperativeYield = processResult == PROCESS_OWNED
                    && fiber.getYieldReason() == Fiber.YIELD_COOPERATIVE;
            if (processResult != PROCESS_TERMINATED) {
                finishProcessingAfterUnmount(fiber, processResult == PROCESS_OWNED);
            }
            if (isCooperativeYield && System.nanoTime() - drainStartNanos >= DRAIN_TIME_BUDGET_NANOS) {
                break;
            }
        }
        if (attempts == attemptBudget && runQueue.hasAvailable()) {
            budgetExhaustionCount.increment();
        }
        tryClose();
        return attempts;
    }

    public long getBudgetExhaustionCount() {
        return budgetExhaustionCount.sum();
    }

    public long getCreatedFiberCount() {
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
        return configuration.maxLiveFiberCount;
    }

    public int getMaxRetainedFiberCount() {
        return configuration.maxRetainedFiberCount;
    }

    public int getMountBudget() {
        return configuration.mountBudget;
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

    public long getRetiredFiberCount() {
        return fiberPool.getRetiredCount();
    }

    @TestOnly
    public synchronized int getConfigurationListenerCountForTesting() {
        return configurationListeners.size();
    }

    @TestOnly
    public synchronized int getQuiesceListenerCountForTesting() {
        return quiesceListeners.size();
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
        return launchReserved(fiber, fiber.getReservationEpoch(), task, taskIncarnation);
    }

    /**
     * Consumes the matching fiber reservation before it starts the launch. The caller may always
     * attempt to release the same epoch afterward; a consumed or stale epoch is a no-op.
     */
    public LaunchResult launchReserved(
            Fiber fiber,
            long reservationEpoch,
            FiberTask task,
            long taskIncarnation
    ) {
        return launchReserved(fiber, reservationEpoch, task, taskIncarnation, false);
    }

    /**
     * Launches the reserved fiber inline when the caller is a scheduler-controlled carrier at a
     * clean mount boundary. The caller must not hold an intrinsic monitor across this call. With
     * lightweight locking, a continuation yield transfers the carrier's lock stack into the stack
     * chunk, which would detach an outer monitor from its matching {@code monitorexit}. When the
     * current execution owns a role-switch lock, this method queues the fiber instead.
     */
    public LaunchResult launchReservedDirect(
            Fiber fiber,
            long reservationEpoch,
            FiberTask task,
            long taskIncarnation
    ) {
        // Direct mount nests no continuation and, per CARRIER_MONITOR.md, requires a clean
        // worker-loop boundary. The held-monitor half of that contract has no cheap runtime
        // check; this pins the half that does.
        assert !Fiber.isMounted() : "direct launch requires an unmounted carrier";
        return launchReserved(
                fiber,
                reservationEpoch,
                task,
                taskIncarnation,
                !SuspensionScope.hasAnyRoleSwitchLock(SuspensionScope.scope())
        );
    }

    public synchronized void registerConfigurationListener(FiberRuntimeConfigurationListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("fiber runtime configuration listener must not be null");
        }
        if (state != FiberRuntimeState.OPEN) {
            throw new IllegalStateException("fiber runtime is not open");
        }
        configurationListeners.add(listener);
        final Configuration currentConfiguration = configuration;
        try {
            listener.onConfigurationChanged(
                    currentConfiguration.maxLiveFiberCount,
                    currentConfiguration.maxRetainedFiberCount
            );
        } catch (Throwable th) {
            LOG.critical().$("fiber runtime configuration listener failed [error=").$(th).I$();
        }
    }

    public synchronized boolean unregisterConfigurationListener(FiberRuntimeConfigurationListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("fiber runtime configuration listener must not be null");
        }
        for (int i = 0, n = configurationListeners.size(); i < n; i++) {
            if (configurationListeners.getQuick(i) == listener) {
                configurationListeners.remove(i);
                return true;
            }
        }
        return false;
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

    public synchronized boolean unregisterQuiesceListener(FiberRuntimeQuiesceListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("fiber runtime quiesce listener must not be null");
        }
        if (state != FiberRuntimeState.OPEN) {
            return false;
        }
        for (int i = 0, n = quiesceListeners.size(); i < n; i++) {
            if (quiesceListeners.getQuick(i) == listener) {
                quiesceListeners.remove(i);
                return true;
            }
        }
        return false;
    }

    public void releaseReservedFiber(Fiber fiber, long reservationEpoch) {
        if (fiber.isForeignTo(this)) {
            throw new IllegalArgumentException("fiber reservation does not belong to this runtime");
        }
        releaseReservation(fiber, reservationEpoch, true);
    }

    @TestOnly
    public void setAfterProcessForTesting(@Nullable Runnable afterProcessForTesting) {
        this.afterProcessForTesting = afterProcessForTesting;
    }

    @TestOnly
    public void setAfterReservationReleaseForTesting(@Nullable Runnable afterReservationReleaseForTesting) {
        this.afterReservationReleaseForTesting = afterReservationReleaseForTesting;
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
            final int maxLiveFiberCount = configuration.maxLiveFiberCount;
            if (outstandingTaskCount.getAndIncrement() >= maxLiveFiberCount) {
                releaseTaskSlot();
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

    public void updateConfiguration(int maxLiveFiberCount, int retainedFiberCount, int mountBudget) {
        if (maxLiveFiberCount < 1) {
            throw new IllegalArgumentException("maxLiveFiberCount must be positive");
        }
        if (retainedFiberCount < 1) {
            throw new IllegalArgumentException("retainedFiberCount must be positive");
        }
        if (mountBudget < 1) {
            throw new IllegalArgumentException("mountBudget must be positive");
        }
        final int maxRetainedFiberCount = Math.min(maxLiveFiberCount, retainedFiberCount);
        final int previousMaxLiveFiberCount;
        synchronized (this) {
            if (state != FiberRuntimeState.OPEN) {
                return;
            }
            previousMaxLiveFiberCount = configuration.maxLiveFiberCount;
            configuration = new Configuration(maxLiveFiberCount, maxRetainedFiberCount, mountBudget);
            fiberPool.reconcileRetention();
            for (int i = 0, n = configurationListeners.size(); i < n; i++) {
                try {
                    configurationListeners.getQuick(i).onConfigurationChanged(
                            maxLiveFiberCount,
                            maxRetainedFiberCount
                    );
                } catch (Throwable th) {
                    LOG.critical().$("fiber runtime configuration listener failed [error=").$(th).I$();
                }
            }
        }
        if (maxLiveFiberCount > previousMaxLiveFiberCount) {
            capacityWaitQueue.fireAll();
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
                || !isQuiesceListenerPassActive.compareAndSet(false, true)) {
            return;
        }
        try {
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
                    quiesceListeners.clear();
                    isPoolQuiesced = true;
                }
            }
        } finally {
            isQuiesceListenerPassActive.set(false);
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
                case Fiber.OUTCOME_ABANDONED, Fiber.OUTCOME_PARKED -> completeAbandoned(task, true);
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
                releaseFiber(fiber);
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
                    hasFiberOwnership = false;
                    releaseFiber(fiber);
                    releaseTaskSlot();
                } else if (result == FiberTask.PARK_RELAUNCH) {
                    fiber.restageAndRequestRun(task);
                } else {
                    hasFiberOwnership = false;
                    releaseFiber(fiber);
                    completeAbandoned(task, false);
                }
            } catch (Throwable th) {
                final boolean isTaskOwned = task.abortArming();
                if (hasFiberOwnership) {
                    hasFiberOwnership = false;
                    releaseFiber(fiber);
                }
                if (isTaskOwned) {
                    terminalError(task, th);
                }
            } finally {
                releaseAdmission();
            }
            return hasFiberOwnership;
        } finally {
            finalizerCount.decrementAndGet();
        }
    }

    private void finishFiberRetirement(Fiber fiber) {
        try {
            fiberPool.onRetired(fiber);
        } catch (Throwable th) {
            LOG.critical().$("fiber retirement finalization failed [error=").$(th).I$();
        }
    }

    private void finishProcessingAfterUnmount(Fiber fiber, boolean hasFiberOwnership) {
        try {
            fiber.finishProcessing();
        } catch (Throwable th) {
            LOG.critical().$("fiber notification finalization failed [error=").$(th).I$();
            final Fiber.Outcome outcome = fiber.getOutcomeScratch();
            outcome.clear();
            final boolean hasCurrentOwnership = hasFiberOwnership
                    || fiber.getTaskAfterDriverFailure(outcome) != null;
            if (handleDriverFailure(fiber, outcome, hasCurrentOwnership, th)) {
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
            final FiberTask task = fiber.getTaskAfterDriverFailure(outcome);
            try {
                fiberPool.retireAfterDriverFailure(fiber, th);
            } catch (Throwable retirementError) {
                LOG.critical().$("fiber quarantine failed [error=").$(retirementError).I$();
                if (retirementError != th) {
                    th.addSuppressed(retirementError);
                }
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

    private LaunchResult launchReserved(
            Fiber fiber,
            long reservationEpoch,
            FiberTask task,
            long taskIncarnation,
            boolean isDirectMountAllowed
    ) {
        if (fiber.isForeignTo(this)) {
            throw new IllegalArgumentException("fiber reservation does not belong to this runtime");
        }
        if (fiber.isReservationStale(reservationEpoch)) {
            throw new IllegalArgumentException("fiber reservation is stale or already consumed");
        }
        Fiber directFiber = null;
        boolean hasAdmission = false;
        boolean hasFiberReservation = true;
        boolean hasTaskSlot = true;
        boolean isTaskClaimed = false;
        LaunchResult result;
        try {
            if (!acquireAdmission()) {
                result = LaunchResult.QUIESCING;
            } else {
                hasAdmission = true;
                // claim() folds the incarnation, ownership and terminal checks into its CAS loop
                final int claim = task.claim(taskIncarnation);
                switch (claim) {
                    case FiberTask.CLAIM_LAUNCHED -> {
                        isTaskClaimed = true;
                        if (isDirectMountAllowed) {
                            if (fiber.stageForDirectMountOrRequestRun(task, reservationEpoch)) {
                                directFiber = fiber;
                            }
                        } else {
                            fiber.stageAndRequestRun(task, reservationEpoch);
                        }
                        hasFiberReservation = false;
                        hasTaskSlot = false;
                        result = LaunchResult.LAUNCHED;
                    }
                    case FiberTask.CLAIM_ALREADY_OWNED, FiberTask.CLAIM_SIGNALLED ->
                            result = LaunchResult.ALREADY_OWNED;
                    case FiberTask.CLAIM_STALE -> result = LaunchResult.STALE_INCARNATION;
                    default -> result = LaunchResult.TERMINAL;
                }
            }
        } catch (Throwable e) {
            if (isTaskClaimed) {
                hasTaskSlot = false;
                try {
                    terminalError(task, e);
                } catch (Throwable terminalFailure) {
                    LOG.critical().$("fiber launch terminalization failed [error=").$(terminalFailure).I$();
                }
                result = LaunchResult.TERMINAL;
            } else {
                LOG.critical().$("fiber launch failed before task claim [error=").$(e).I$();
                result = LaunchResult.RESOURCE_FAILURE;
            }
        } finally {
            if (hasFiberReservation) {
                releaseReservation(fiber, reservationEpoch, hasTaskSlot);
            }
            if (hasAdmission) {
                try {
                    releaseAdmission();
                } catch (Throwable th) {
                    LOG.critical().$("fiber launch admission release failed [error=").$(th).I$();
                }
            }
        }
        if (directFiber != null) {
            final int processResult = process(directFiber, true);
            if (processResult != PROCESS_TERMINATED) {
                finishProcessingAfterUnmount(directFiber, processResult == PROCESS_OWNED);
            }
        }
        return record(result);
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

    private int process(Fiber fiber, boolean isDirectMount) {
        if (!isDirectMount && !fiber.beginProcessing()) {
            LOG.critical().$("fiber queue invariant failed [state=").$(fiber.getNotificationState()).I$();
            return PROCESS_TERMINATED;
        }
        boolean hasFiberOwnership = true;
        boolean isTerminated = false;
        Fiber.Outcome outcome = fiber.getOutcomeScratch();
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
                fiber.beginRetirement();
                fiber.markRetired();
                finishFiberRetirement(fiber);
                hasFiberOwnership = false;
                finalizeOutcome(outcome);
            } else if (fiber.getYieldReason() == Fiber.YIELD_COOPERATIVE) {
                fiber.publishCooperativeYield();
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
                    releaseFiber(fiber);
                    hasFiberOwnership = false;
                    finalizeOutcome(outcome);
                }
            }
            final Runnable hook = afterProcessForTesting;
            if (hook != null) {
                hook.run();
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
        return isTerminated
                ? PROCESS_TERMINATED
                : hasFiberOwnership ? PROCESS_OWNED : PROCESS_RELEASED;
    }

    private LaunchResult record(LaunchResult result) {
        try {
            launchCounts.getQuick(result.ordinal()).increment();
        } catch (Throwable th) {
            LOG.error().$("fiber launch metric update failed [error=").$(th).I$();
        }
        return result;
    }

    private void releaseFiber(Fiber fiber) {
        try {
            fiberPool.release(fiber);
        } catch (Throwable th) {
            onFiberPoolReleaseFailure(th);
        }
    }

    private void releaseReservation(Fiber fiber, long reservationEpoch, boolean hasTaskSlot) {
        if (!fiberPool.releaseReservation(fiber, reservationEpoch)) {
            return;
        }
        if (hasTaskSlot) {
            try {
                releaseTaskSlot();
            } catch (Throwable th) {
                LOG.critical().$("fiber reservation task slot release failed [error=").$(th).I$();
            }
        }
    }

    private void releaseTaskSlot() {
        final int count = outstandingTaskCount.decrementAndGet();
        if (count < 0) {
            outstandingTaskCount.incrementAndGet();
            throw new IllegalStateException("fiber runtime task slot underflow");
        }
        try {
            signalCapacity();
        } catch (Throwable th) {
            LOG.critical().$("fiber capacity signal failed [error=").$(th).I$();
        }
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
                && !runQueue.hasAvailable()
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

    void onFiberPoolReleaseFailure(Throwable th) {
        LOG.critical().$("fiber pool release failed [error=").$(th).I$();
    }

    void onInlineSuspendViolation(CharSequence pinnedReason) {
        inlineSuspendViolationCount.increment();
        if (isInlineSuspendViolationLogged.compareAndSet(false, true)) {
            LOG.critical().$("fiber suspension refused, carrier is pinned [reason=").$(pinnedReason).I$();
        }
    }

    @TestOnly
    void onReservationReleasedForTesting() {
        final Runnable hook = afterReservationReleaseForTesting;
        if (hook != null) {
            hook.run();
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

    private static final class Configuration {
        private final int maxLiveFiberCount;
        private final int maxRetainedFiberCount;
        private final int mountBudget;

        private Configuration(int maxLiveFiberCount, int maxRetainedFiberCount, int mountBudget) {
            this.maxLiveFiberCount = maxLiveFiberCount;
            this.maxRetainedFiberCount = maxRetainedFiberCount;
            this.mountBudget = mountBudget;
        }
    }
}

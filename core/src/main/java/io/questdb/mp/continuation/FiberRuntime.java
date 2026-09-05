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
import io.questdb.mp.Worker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class FiberRuntime {
    public static final int NO_WORKER = -1;
    private static final long ADMISSION_OPEN = Long.MIN_VALUE;
    private static final long ADMISSION_PERMIT_MASK = Long.MAX_VALUE;
    private static final long DRAIN_TIME_BUDGET_NANOS = 2_000_000L;
    // Bound global-injection starvation under continuous local work without probing the shared
    // MPMC queue on every selection. The countdown measures successful selections, not time; 61
    // also leaves room for a global probe within the default mount budget of 64.
    private static final int GLOBAL_PROBE_INTERVAL = 61;
    private static final Log LOG = LogFactory.getLog(FiberRuntime.class);
    private static final int PROCESS_OWNED = 2;
    private static final int PROCESS_RELEASED = 1;
    private static final int PROCESS_TERMINATED = 0;
    private final AtomicLong admission = new AtomicLong(ADMISSION_OPEN);
    private final @Nullable Runnable beforeFiberAcquireForTesting;
    private final BindingRole bindingRole;
    private final LongAdder budgetExhaustionCount = new LongAdder();
    private final FiberEventWaitQueue capacityWaitQueue;
    private final SOCountDownLatch closedLatch = new SOCountDownLatch(1);
    private final ObjList<FiberRuntimeConfigurationListener> configurationListeners = new ObjList<>();
    private final AtomicInteger detachedStealCursor = new AtomicInteger();
    private final FiberPool fiberPool;
    private final AtomicInteger finalizerCount = new AtomicInteger();
    private final LongAdder globalPublicationCount = new LongAdder();
    private final LongAdder globalSelectionCount = new LongAdder();
    private final LongAdder inlineSuspendViolationCount = new LongAdder();
    private final AtomicBoolean isInlineSuspendViolationLogged = new AtomicBoolean();
    private final AtomicBoolean isQuiesceListenerPassActive = new AtomicBoolean();
    private final ObjList<LongAdder> launchCounts = new ObjList<>(LaunchResult.COUNT);
    private final LongAdder localFallbackPublicationCount = new LongAdder();
    private final LongAdder localPublicationCount = new LongAdder();
    private final LongAdder localSelectionCount = new LongAdder();
    private final LongAdder mountCount = new LongAdder();
    private final LongAdder mountedCount = new LongAdder();
    private final AtomicInteger orphanedCount = new AtomicInteger();
    private final LongAdder orphanedEntryRecoveryCount = new LongAdder();
    private final LongAdder orphanedShardTransitionCount = new LongAdder();
    private final long[] orphanedWords;
    private final AtomicInteger outstandingTaskCount = new AtomicInteger();
    private final ObjList<OwnerContext> ownerContexts;
    private final int ownerWorkerCount;
    private final ObjList<FiberRuntimeQuiesceListener> quiesceListeners = new ObjList<>();
    private final FiberRunQueue runQueue;
    private final LongAdder saturationCount = new LongAdder();
    private final ObjList<Shard> shards;
    private final LongAdder stolenSelectionCount = new LongAdder();
    private final LongAdder wakeClaimCount = new LongAdder();
    private final FiberWakeSink wakeSink;
    private volatile @Nullable Runnable afterProcessForTesting;
    private volatile @Nullable Runnable afterReservationReleaseForTesting;
    private volatile Configuration configuration;
    private volatile boolean isPoolQuiesced;
    private volatile FiberRuntimeState state = FiberRuntimeState.OPEN;

    public FiberRuntime(int retainedFiberCount) {
        this(
                retainedFiberCount,
                retainedFiberCount,
                64,
                null,
                null,
                BindingRole.STANDALONE_TEST,
                0,
                FiberWakeSink.NO_OP
        );
    }

    public FiberRuntime(int retainedFiberCount, int maxLiveFiberCount) {
        this(
                retainedFiberCount,
                maxLiveFiberCount,
                64,
                null,
                null,
                BindingRole.STANDALONE_TEST,
                0,
                FiberWakeSink.NO_OP
        );
    }

    public FiberRuntime(int retainedFiberCount, int maxLiveFiberCount, int mountBudget) {
        this(
                retainedFiberCount,
                maxLiveFiberCount,
                mountBudget,
                null,
                null,
                BindingRole.STANDALONE_TEST,
                0,
                FiberWakeSink.NO_OP
        );
    }

    public FiberRuntime(
            int retainedFiberCount,
            int maxLiveFiberCount,
            int mountBudget,
            int ownerWorkerCount,
            FiberWakeSink wakeSink
    ) {
        this(
                retainedFiberCount,
                maxLiveFiberCount,
                mountBudget,
                null,
                null,
                BindingRole.POOL_BOUND,
                ownerWorkerCount,
                wakeSink
        );
    }

    @TestOnly
    public FiberRuntime(
            int retainedFiberCount,
            int maxLiveFiberCount,
            @Nullable Runnable beforeFiberAcquireForTesting
    ) {
        this(
                retainedFiberCount,
                maxLiveFiberCount,
                64,
                beforeFiberAcquireForTesting,
                null,
                BindingRole.STANDALONE_TEST,
                0,
                FiberWakeSink.NO_OP
        );
    }

    @TestOnly
    public FiberRuntime(
            int retainedFiberCount,
            int maxLiveFiberCount,
            @Nullable Runnable beforeFiberAcquireForTesting,
            @Nullable Runnable beforeWaitFireForTesting
    ) {
        this(
                retainedFiberCount,
                maxLiveFiberCount,
                64,
                beforeFiberAcquireForTesting,
                beforeWaitFireForTesting,
                BindingRole.STANDALONE_TEST,
                0,
                FiberWakeSink.NO_OP
        );
    }

    private FiberRuntime(
            int retainedFiberCount,
            int maxLiveFiberCount,
            int mountBudget,
            @Nullable Runnable beforeFiberAcquireForTesting,
            @Nullable Runnable beforeWaitFireForTesting,
            BindingRole bindingRole,
            int ownerWorkerCount,
            FiberWakeSink wakeSink
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
        if (ownerWorkerCount < 0) {
            throw new IllegalArgumentException("ownerWorkerCount must not be negative");
        }
        if (bindingRole == BindingRole.STANDALONE_TEST && ownerWorkerCount != 0) {
            throw new IllegalArgumentException("standalone Fiber runtime cannot have owner Workers");
        }
        if (wakeSink == null) {
            throw new IllegalArgumentException("Fiber wake sink must not be null");
        }
        this.beforeFiberAcquireForTesting = beforeFiberAcquireForTesting;
        this.bindingRole = bindingRole;
        this.ownerWorkerCount = ownerWorkerCount;
        this.wakeSink = wakeSink;
        this.configuration = new Configuration(maxLiveFiberCount, retainedFiberCount, mountBudget);
        this.capacityWaitQueue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_CAPACITY);
        this.runQueue = new FiberRunQueue(maxLiveFiberCount);
        this.ownerContexts = new ObjList<>(ownerWorkerCount);
        this.shards = new ObjList<>(ownerWorkerCount);
        this.orphanedWords = new long[(int) (((long) ownerWorkerCount + Long.SIZE - 1) / Long.SIZE)];
        if (ownerWorkerCount > 0) {
            final int localCapacity = FiberLocalRunQueue.calculateCapacity(maxLiveFiberCount, ownerWorkerCount);
            for (int workerId = 0; workerId < ownerWorkerCount; workerId++) {
                final Shard shard = new Shard(
                        workerId,
                        localCapacity,
                        (int) (((long) GLOBAL_PROBE_INTERVAL * workerId) / ownerWorkerCount),
                        workerId + 1 == ownerWorkerCount ? 0 : workerId + 1
                );
                shards.add(shard);
                ownerContexts.add(new OwnerContext(this, workerId, shard));
            }
        }
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

    public void activateOwner(OwnerContext ownerContext) {
        final Shard shard = validateOwner(ownerContext);
        if (!shard.ownerState.compareAndSet(Shard.UNSTARTED, Shard.ACTIVE)) {
            throw new IllegalStateException("Fiber owner shard is not unstarted [workerId="
                    + shard.workerId + ", state=" + shard.ownerState.get() + ']');
        }
        shard.carrierScope = SuspensionScope.scope();
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
            wakeAllWorkers();
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
        validateAttemptBudget(attemptBudget);
        if (bindingRole == BindingRole.POOL_BOUND && ownerWorkerCount > 0) {
            if (state == FiberRuntimeState.OPEN && hasStartedOwner()) {
                throw new IllegalStateException(
                        "detached drain requires a quiescing or unstarted pool-bound Fiber runtime"
                );
            }
            if (Worker.current() != null || Fiber.isMounted()
                    || SuspensionScope.hasAnyRoleSwitchLock(SuspensionScope.scope())) {
                throw new IllegalStateException("detached drain requires a clean non-Worker carrier");
            }
        } else if (SuspensionScope.hasAnyRoleSwitchLock(SuspensionScope.scope())) {
            tryClose();
            return 0;
        }
        int attempts = 0;
        long drainStartNanos = 0;
        while (attempts < attemptBudget) {
            final Fiber fiber = selectDetached();
            if (fiber == null) {
                break;
            }
            if (attempts == 0) {
                drainStartNanos = System.nanoTime();
            }
            attempts++;
            final int processResult = process(fiber, false, null);
            // Capture the yield reason before finalization can republish the fiber to another carrier.
            final boolean isCooperativeYield = processResult == PROCESS_OWNED
                    && fiber.getYieldReason() == Fiber.YIELD_COOPERATIVE;
            if (processResult != PROCESS_TERMINATED) {
                finishProcessingAfterUnmount(fiber, processResult == PROCESS_OWNED, null);
            }
            if (isCooperativeYield && System.nanoTime() - drainStartNanos >= DRAIN_TIME_BUDGET_NANOS) {
                break;
            }
        }
        if (attempts == attemptBudget && hasQueuedWork()) {
            budgetExhaustionCount.increment();
        }
        tryClose();
        return attempts;
    }

    /**
     * Final pre-park search after the caller has removed its ready bit. It may process at most one
     * queued Fiber, preserving the Worker-loop mount budget from before the idle transition.
     */
    public boolean drainOneBeforePark(OwnerContext ownerContext) {
        final Shard shard = ownedShard(ownerContext);
        if (SuspensionScope.hasAnyRoleSwitchLock(shard.carrierScope)) {
            return false;
        }
        final Fiber fiber = selectBeforePark(shard);
        if (fiber == null) {
            tryClose();
            return false;
        }
        processSelected(fiber, ownerContext, false);
        tryClose();
        return true;
    }

    public int drainOwned(OwnerContext ownerContext, int attemptBudget) {
        validateAttemptBudget(attemptBudget);
        final Shard shard = ownedShard(ownerContext);
        if (SuspensionScope.hasAnyRoleSwitchLock(shard.carrierScope)) {
            tryClose();
            return 0;
        }
        int attempts = 0;
        while (attempts < attemptBudget) {
            final Fiber fiber = selectOwned(shard);
            if (fiber == null) {
                break;
            }
            attempts++;
            processSelected(fiber, ownerContext, false);
        }
        if (attempts == attemptBudget && hasQueuedWork()) {
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

    public long getGlobalPublicationCount() {
        return globalPublicationCount.sum();
    }

    public long getGlobalSelectionCount() {
        return globalSelectionCount.sum();
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

    public long getLocalFallbackPublicationCount() {
        return localFallbackPublicationCount.sum();
    }

    public long getLocalPublicationCount() {
        return localPublicationCount.sum();
    }

    public long getLocalSelectionCount() {
        return localSelectionCount.sum();
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

    public long getOrphanedEntryRecoveryCount() {
        return orphanedEntryRecoveryCount.sum();
    }

    public long getOrphanedShardTransitionCount() {
        return orphanedShardTransitionCount.sum();
    }

    public OwnerContext getOwnerContext(int workerId) {
        if (bindingRole != BindingRole.POOL_BOUND || workerId < 0 || workerId >= ownerWorkerCount) {
            throw new IllegalArgumentException("Fiber owner Worker id is out of range [workerId="
                    + workerId + ", workerCount=" + ownerWorkerCount + ']');
        }
        return ownerContexts.getQuick(workerId);
    }

    public int getParkedFiberCount() {
        return fiberPool.getParkedCount();
    }

    public int getQueuedCount() {
        long count = runQueue.depth();
        for (int i = 0, n = shards.size(); i < n; i++) {
            count += shards.getQuick(i).localQueue.depth();
            if (count >= Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
        }
        return (int) count;
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
    public static int calculateLocalQueueCapacityForTesting(int initialMaxLiveCount, int workerCount) {
        return FiberLocalRunQueue.calculateCapacity(initialMaxLiveCount, workerCount);
    }

    @TestOnly
    public static int getGlobalProbeIntervalForTesting() {
        return GLOBAL_PROBE_INTERVAL;
    }

    @TestOnly
    public boolean claimLocalHeadForTesting(int workerId, long expectedHead) {
        return getShardForTesting(workerId).localQueue.claimHeadForTesting(expectedHead);
    }

    @TestOnly
    public int getLocalQueueCapacityForTesting(int workerId) {
        return getShardForTesting(workerId).localQueue.capacity();
    }

    @TestOnly
    public int getLocalQueueDepthForTesting(int workerId) {
        return getShardForTesting(workerId).localQueue.depth();
    }

    @TestOnly
    public int getRunQueueCapacity() {
        return runQueue.capacity();
    }

    @TestOnly
    public void initializeLocalPositionForTesting(int workerId, long position) {
        getShardForTesting(workerId).localQueue.initializeEmptyPositionForTesting(position);
    }

    @TestOnly
    public boolean offerLocalForTesting(int workerId, Fiber fiber) {
        return getShardForTesting(workerId).localQueue.offer(fiber);
    }

    @TestOnly
    public Fiber releaseLocalClaimForTesting(int workerId, long claimedHead) {
        return getShardForTesting(workerId).localQueue.releaseClaimForTesting(claimedHead);
    }

    @TestOnly
    public @Nullable Fiber tryDequeueLocalForTesting(int workerId) {
        return getShardForTesting(workerId).localQueue.tryDequeue();
    }

    public long getSaturationCount() {
        return saturationCount.sum();
    }

    public long getStolenSelectionCount() {
        return stolenSelectionCount.sum();
    }

    public long getWakeClaimCount() {
        return wakeClaimCount.sum();
    }

    /**
     * Non-mutating half of the ready-Worker search. The caller keeps its ready bit published while
     * this checks every source, then clears that bit before calling {@link #drainOneBeforePark}.
     */
    public boolean hasWorkAfterReady(OwnerContext ownerContext) {
        final Shard shard = ownedShard(ownerContext);
        if (SuspensionScope.hasAnyRoleSwitchLock(shard.carrierScope)) {
            return false;
        }
        if (runQueue.hasAvailable() || shard.localQueue.hasAvailable()) {
            return true;
        }
        int workerId = shard.stealCursor;
        for (int i = 0; i < ownerWorkerCount; i++) {
            if (workerId != shard.workerId && shards.getQuick(workerId).localQueue.hasAvailable()) {
                return true;
            }
            if (++workerId == ownerWorkerCount) {
                workerId = 0;
            }
        }
        return false;
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
        return launchReserved(fiber, reservationEpoch, task, taskIncarnation, false, null);
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
        final OwnerContext ownerContext = currentOwnerContext();
        final boolean isOwnerRequired = bindingRole == BindingRole.POOL_BOUND && ownerWorkerCount > 0;
        return launchReserved(
                fiber,
                reservationEpoch,
                task,
                taskIncarnation,
                !SuspensionScope.hasAnyRoleSwitchLock(SuspensionScope.scope())
                        && (!isOwnerRequired || ownerContext != null),
                ownerContext
        );
    }

    public void onOwnerExit(OwnerContext ownerContext) {
        final Shard shard = validateOwner(ownerContext);
        final int targetState = state == FiberRuntimeState.CLOSED && !shard.localQueue.hasAvailable()
                ? Shard.STOPPED
                : Shard.ORPHANED;
        if (!shard.ownerState.compareAndSet(Shard.ACTIVE, targetState)) {
            throw new IllegalStateException("Fiber owner shard is not active [workerId="
                    + shard.workerId + ", state=" + shard.ownerState.get() + ']');
        }
        shard.carrierScope = null;
        if (targetState == Shard.ORPHANED) {
            orphanedShardTransitionCount.increment();
        }
        if (targetState == Shard.ORPHANED && shard.localQueue.hasAvailable()) {
            advertiseOrphan(shard);
        } else if (targetState == Shard.ORPHANED && runQueue.hasAvailable()) {
            // A prior global wake may have claimed this now-exiting owner, or no peer may have
            // been ready at commit time. Re-signal still-visible work after producer revocation.
            wakeAfterCommit(NO_WORKER);
        }
        tryClose();
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

    private void advertiseOrphan(Shard shard) {
        final int wordIndex = shard.workerId >>> 6;
        final long bit = 1L << (shard.workerId & 63);
        orphanedCount.incrementAndGet();
        while (true) {
            final long current = Unsafe.arrayGetVolatile(orphanedWords, wordIndex);
            if ((current & bit) != 0) {
                orphanedCount.decrementAndGet();
                LOG.critical().$("Fiber shard is already advertised as orphaned [value=").$(shard.workerId).I$();
                assert false : "Fiber shard is already advertised as orphaned";
                return;
            }
            if (Unsafe.cas(orphanedWords, wordIndex, current, current | bit)) {
                if (shard.localQueue.hasAvailable()) {
                    wakeAfterCommit(NO_WORKER);
                } else {
                    clearOrphanIfEmpty(shard);
                }
                return;
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

    private void clearOrphanIfEmpty(Shard shard) {
        if (shard.localQueue.hasAvailable()) {
            return;
        }
        final int wordIndex = shard.workerId >>> 6;
        final long bit = 1L << (shard.workerId & 63);
        while (true) {
            final long current = Unsafe.arrayGetVolatile(orphanedWords, wordIndex);
            if ((current & bit) == 0) {
                return;
            }
            if (Unsafe.cas(orphanedWords, wordIndex, current, current & ~bit)) {
                final int count = orphanedCount.decrementAndGet();
                if (count < 0) {
                    LOG.critical().$("Fiber orphaned shard count underflow [value=").$(count).I$();
                    assert false : "Fiber orphaned shard count underflow";
                }
                return;
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

    private @Nullable OwnerContext currentOwnerContext() {
        final Worker worker = Worker.current();
        if (worker == null) {
            return null;
        }
        final OwnerContext ownerContext = worker.getFiberOwnerContext();
        return ownerContext != null
                && ownerContext.runtime == this
                && ownerContext.shard.ownerState.get() == Shard.ACTIVE
                ? ownerContext
                : null;
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

    private void finishProcessingAfterUnmount(
            Fiber fiber,
            boolean hasFiberOwnership,
            @Nullable OwnerContext ownerContext
    ) {
        try {
            fiber.finishProcessing(ownerContext);
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

    private Shard getShardForTesting(int workerId) {
        if (workerId < 0 || workerId >= ownerWorkerCount) {
            throw new IllegalArgumentException("Fiber shard Worker id is out of range [workerId="
                    + workerId + ", workerCount=" + ownerWorkerCount + ']');
        }
        return shards.getQuick(workerId);
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

    private boolean hasQueuedWork() {
        if (runQueue.hasAvailable()) {
            return true;
        }
        for (int i = 0, n = shards.size(); i < n; i++) {
            if (shards.getQuick(i).localQueue.hasAvailable()) {
                return true;
            }
        }
        return false;
    }

    private boolean hasStartedOwner() {
        for (int i = 0; i < ownerWorkerCount; i++) {
            if (shards.getQuick(i).ownerState.get() != Shard.UNSTARTED) {
                return true;
            }
        }
        return false;
    }

    private boolean isActiveOwnerOnCurrentCarrier(OwnerContext ownerContext) {
        final Shard shard = validateActiveOwner(ownerContext);
        if (shard.carrierScope != SuspensionScope.scope()) {
            throw new IllegalStateException("owned drain requires the activating carrier [workerId="
                    + shard.workerId + ']');
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
            boolean isDirectMountAllowed,
            @Nullable OwnerContext directOwnerContext
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
            final int processResult = process(directFiber, true, directOwnerContext);
            if (processResult != PROCESS_TERMINATED) {
                finishProcessingAfterUnmount(
                        directFiber,
                        processResult == PROCESS_OWNED,
                        directOwnerContext
                );
            }
        }
        return record(result);
    }

    private Shard nextVictim(Shard ownerShard) {
        int workerId = ownerShard.stealCursor;
        if (workerId == ownerShard.workerId && ++workerId == ownerWorkerCount) {
            workerId = 0;
        }
        final Shard victim = shards.getQuick(workerId);
        if (++workerId == ownerWorkerCount) {
            workerId = 0;
        }
        if (workerId == ownerShard.workerId && ++workerId == ownerWorkerCount) {
            workerId = 0;
        }
        ownerShard.stealCursor = workerId;
        return victim;
    }

    private void onOwnedSelection(Shard shard) {
        shard.globalProbeCountdown--;
    }

    private Shard ownedShard(OwnerContext ownerContext) {
        assert isActiveOwnerOnCurrentCarrier(ownerContext);
        return ownerContext.shard;
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

    private int process(Fiber fiber, boolean isDirectMount, @Nullable OwnerContext ownerContext) {
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
            fiber.setLastMountWorkerId(ownerContext != null ? ownerContext.workerId : NO_WORKER);
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

    private void processSelected(
            Fiber fiber,
            @Nullable OwnerContext ownerContext,
            boolean isDirectMount
    ) {
        final int processResult = process(fiber, isDirectMount, ownerContext);
        if (processResult != PROCESS_TERMINATED) {
            finishProcessingAfterUnmount(fiber, processResult == PROCESS_OWNED, ownerContext);
        }
    }

    private void publish(
            Fiber fiber,
            @Nullable OwnerContext ownerContext,
            PublicationMode publicationMode
    ) {
        final boolean isOwnerPublication = ownerContext != null && ownerContext.runtime == this;
        if (publicationMode.isLocalPublicationAllowed
                && isOwnerPublication
                && ownerContext.shard.ownerState.get() == Shard.ACTIVE
                && ownerContext.shard.localQueue.offer(fiber)) {
            localPublicationCount.increment();
            return;
        }
        runQueue.put(fiber);
        // An external publisher cannot service this runtime. A normal owner publication reaches
        // this branch only after its bounded local queue is unavailable, which is the structural
        // backlog signal that justifies adding one parked peer. Owner-generated cleanup is forced
        // global to preserve FIFO behind injected work, but the active owner can service it and
        // must not pay an eager-wake penalty.
        if (!isOwnerPublication || publicationMode.isLocalPublicationAllowed) {
            wakeAfterCommit(!isOwnerPublication && publicationMode.isLastMountPreferenceAllowed
                    ? fiber.getLastMountWorkerId()
                    : NO_WORKER);
        }
        if (isOwnerPublication && publicationMode.isLocalPublicationAllowed) {
            localFallbackPublicationCount.increment();
        } else {
            globalPublicationCount.increment();
        }
    }

    private LaunchResult record(LaunchResult result) {
        launchCounts.getQuick(result.ordinal()).increment();
        return result;
    }

    private void recordStolenSelection(Shard shard) {
        stolenSelectionCount.increment();
        if (shard.ownerState.get() == Shard.ORPHANED) {
            orphanedEntryRecoveryCount.increment();
        }
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

    private @Nullable Fiber selectBeforePark(Shard ownerShard) {
        Fiber fiber = runQueue.tryDequeue();
        if (fiber != null) {
            onOwnedSelection(ownerShard);
            globalSelectionCount.increment();
            return fiber;
        }
        fiber = ownerShard.localQueue.tryDequeue();
        if (fiber != null) {
            onOwnedSelection(ownerShard);
            localSelectionCount.increment();
            return fiber;
        }
        for (int i = 1; i < ownerWorkerCount; i++) {
            final Shard victim = nextVictim(ownerShard);
            fiber = victim.localQueue.tryDequeue();
            if (victim.ownerState.get() == Shard.ORPHANED) {
                clearOrphanIfEmpty(victim);
            }
            if (fiber != null) {
                onOwnedSelection(ownerShard);
                recordStolenSelection(victim);
                return fiber;
            }
        }
        return null;
    }

    private @Nullable Fiber selectDetached() {
        final Fiber globalFiber = runQueue.tryDequeue();
        if (globalFiber != null || ownerWorkerCount == 0) {
            if (globalFiber != null) {
                globalSelectionCount.increment();
            }
            return globalFiber;
        }
        final int start = Math.floorMod(detachedStealCursor.getAndIncrement(), ownerWorkerCount);
        for (int i = 0; i < ownerWorkerCount; i++) {
            final int workerId = (int) (((long) start + i) % ownerWorkerCount);
            final Shard shard = shards.getQuick(workerId);
            final Fiber fiber = shard.localQueue.tryDequeue();
            if (shard.ownerState.get() == Shard.ORPHANED) {
                clearOrphanIfEmpty(shard);
            }
            if (fiber != null) {
                recordStolenSelection(shard);
                return fiber;
            }
        }
        return null;
    }

    private @Nullable Fiber selectOwned(Shard ownerShard) {
        Fiber fiber;
        if (ownerShard.globalProbeCountdown <= 0) {
            ownerShard.globalProbeCountdown = GLOBAL_PROBE_INTERVAL;
            fiber = runQueue.tryDequeue();
            if (fiber != null) {
                onOwnedSelection(ownerShard);
                globalSelectionCount.increment();
                return fiber;
            }
        }
        if (ownerWorkerCount > 1 && orphanedCount.get() != 0) {
            fiber = tryDequeueAdvertisedOrphan(ownerShard);
            if (fiber != null) {
                onOwnedSelection(ownerShard);
                return fiber;
            }
        }
        fiber = ownerShard.localQueue.tryDequeue();
        if (fiber != null) {
            onOwnedSelection(ownerShard);
            localSelectionCount.increment();
            return fiber;
        }
        // Invariant: an empty local queue always checks the global queue. The scheduled probe
        // counts down only on successful selections, so a Worker whose Jobs stay busy and whose
        // local queue is empty would otherwise never select injected work.
        fiber = runQueue.tryDequeue();
        if (fiber != null) {
            onOwnedSelection(ownerShard);
            globalSelectionCount.increment();
            return fiber;
        }
        if (ownerWorkerCount > 1) {
            final Shard victim = nextVictim(ownerShard);
            fiber = victim.localQueue.tryDequeue();
            if (victim.ownerState.get() == Shard.ORPHANED) {
                clearOrphanIfEmpty(victim);
            }
            if (fiber != null) {
                onOwnedSelection(ownerShard);
                recordStolenSelection(victim);
                return fiber;
            }
        }
        return null;
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
                && !hasQueuedWork()
                && fiberPool.getCreatedCount() == fiberPool.getRetiredCount()
                && !fiberPool.hasInFlightWaitRegistrations()) {
            state = FiberRuntimeState.CLOSED;
            closedLatch.countDown();
        }
    }

    private @Nullable Fiber tryDequeueAdvertisedOrphan(Shard ownerShard) {
        int workerId = ownerShard.stealCursor;
        for (int i = 0; i < ownerWorkerCount; i++) {
            final int wordIndex = workerId >>> 6;
            final long bit = 1L << (workerId & 63);
            if ((Unsafe.arrayGetVolatile(orphanedWords, wordIndex) & bit) != 0) {
                ownerShard.stealCursor = workerId + 1 == ownerWorkerCount ? 0 : workerId + 1;
                final Shard orphanedShard = shards.getQuick(workerId);
                final Fiber fiber = orphanedShard.localQueue.tryDequeue();
                clearOrphanIfEmpty(orphanedShard);
                if (fiber != null) {
                    recordStolenSelection(orphanedShard);
                }
                return fiber;
            }
            if (++workerId == ownerWorkerCount) {
                workerId = 0;
            }
        }
        return null;
    }

    private Shard validateActiveOwner(OwnerContext ownerContext) {
        final Shard shard = validateOwner(ownerContext);
        if (shard.ownerState.get() != Shard.ACTIVE) {
            throw new IllegalStateException("Fiber owner shard is not active [workerId="
                    + shard.workerId + ", state=" + shard.ownerState.get() + ']');
        }
        return shard;
    }

    private void validateAttemptBudget(int attemptBudget) {
        if (attemptBudget < 1) {
            throw new IllegalArgumentException("attemptBudget must be positive");
        }
    }

    private Shard validateOwner(OwnerContext ownerContext) {
        if (ownerContext == null
                || ownerContext.runtime != this
                || ownerContext.workerId < 0
                || ownerContext.workerId >= ownerWorkerCount
                || ownerContexts.getQuick(ownerContext.workerId) != ownerContext
                || shards.getQuick(ownerContext.workerId) != ownerContext.shard) {
            throw new IllegalArgumentException("Fiber owner context does not belong to this runtime");
        }
        return ownerContext.shard;
    }

    private void wakeAfterCommit(int preferredWorkerId) {
        try {
            if (wakeSink.wakeOne(preferredWorkerId)) {
                wakeClaimCount.increment();
            }
        } catch (RuntimeException | Error th) {
            LOG.error().$("Fiber Worker wake failed after queue commit [error=").$(th).I$();
        }
    }

    private void wakeAllWorkers() {
        try {
            wakeSink.wakeAll();
        } catch (RuntimeException | Error th) {
            LOG.error().$("Fiber Worker wake-all failed [error=").$(th).I$();
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
        final OwnerContext ownerContext = currentOwnerContext();
        if (fiber.isShutdownRequested()) {
            // Retirement and runtime-shutdown continuations are cleanup, not affinity work. Keep
            // them on the global queue so they do not jump ahead of older injected tasks through
            // owner-local priority, and do not reuse a stale last-mounter wake hint.
            publish(fiber, ownerContext, PublicationMode.SHUTDOWN_CLEANUP);
            return;
        }
        publish(fiber, ownerContext, PublicationMode.REQUEST_RUN);
    }

    void enqueueAfterProcessing(Fiber fiber, @Nullable OwnerContext ownerContext) {
        if (ownerContext != null) {
            validateOwner(ownerContext);
        }
        if (fiber.isShutdownRequested()) {
            publish(fiber, ownerContext, PublicationMode.SHUTDOWN_CLEANUP);
            return;
        }
        // A null explicit context is DETACHED, not an arbitrary external publisher, so it must not
        // reuse the Fiber's previous owner as a wake preference.
        publish(fiber, ownerContext, PublicationMode.POST_PROCESS_RESIGNAL);
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

    private enum BindingRole {
        POOL_BOUND,
        STANDALONE_TEST
    }

    private enum PublicationMode {
        POST_PROCESS_RESIGNAL(true, false),
        REQUEST_RUN(true, true),
        SHUTDOWN_CLEANUP(false, false);

        private final boolean isLastMountPreferenceAllowed;
        private final boolean isLocalPublicationAllowed;

        PublicationMode(boolean isLocalPublicationAllowed, boolean isLastMountPreferenceAllowed) {
            this.isLocalPublicationAllowed = isLocalPublicationAllowed;
            this.isLastMountPreferenceAllowed = isLastMountPreferenceAllowed;
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

    public static final class OwnerContext {
        private final FiberRuntime runtime;
        private final Shard shard;
        private final int workerId;

        private OwnerContext(FiberRuntime runtime, int workerId, Shard shard) {
            this.runtime = runtime;
            this.workerId = workerId;
            this.shard = shard;
        }

        public int getWorkerId() {
            return workerId;
        }

        public boolean isOwnedBy(FiberRuntime runtime) {
            return this.runtime == runtime;
        }
    }

    private static final class Shard {
        private static final int ACTIVE = 1;
        private static final int ORPHANED = 2;
        private static final int STOPPED = 3;
        private static final int UNSTARTED = 0;
        private final FiberLocalRunQueue localQueue;
        private final AtomicInteger ownerState = new AtomicInteger(UNSTARTED);
        private final int workerId;
        private SuspensionScope.CarrierScope carrierScope;
        private int globalProbeCountdown;
        private int stealCursor;

        private Shard(int workerId, int capacity, int globalProbeCountdown, int stealCursor) {
            this.workerId = workerId;
            this.localQueue = new FiberLocalRunQueue(capacity);
            this.globalProbeCountdown = globalProbeCountdown;
            this.stealCursor = stealCursor;
        }
    }
}

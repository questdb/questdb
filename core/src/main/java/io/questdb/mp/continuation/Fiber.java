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

import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.NanosecondClock;
import jdk.internal.vm.Continuation;
import jdk.internal.vm.ContinuationScope;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.Lock;

public final class Fiber implements FiberWaitCoordinator.Target {
    public static final long TOKEN_REFUSED = 0;
    public static final int YIELD_COOPERATIVE = 2;
    public static final int YIELD_FREE = 0;
    public static final int YIELD_WAIT = 1;
    static final int EXECUTION_DONE = 6;
    static final int EXECUTION_FREE = 0;
    static final int EXECUTION_MOUNTED = 2;
    static final int EXECUTION_PARKING = 3;
    static final int EXECUTION_RESERVED = 7;
    static final int EXECUTION_RESUME_PENDING = 4;
    static final int EXECUTION_RUNNABLE = 1;
    static final int EXECUTION_STATE_BITS = 3;
    static final int EXECUTION_WAITING = 5;
    static final int OUTCOME_ABANDONED = 4;
    static final int OUTCOME_DONE = 2;
    static final int OUTCOME_ERROR = 3;
    static final int OUTCOME_NONE = 0;
    static final int OUTCOME_PARKED = 1;
    private static final long EXECUTION_STATE_MASK = (1L << EXECUTION_STATE_BITS) - 1;
    private static final long EXECUTION_STATE_OFFSET = Unsafe.getFieldOffset(Fiber.class, "executionState");
    private static final long EXECUTION_TOKEN_MASK = -1L >>> EXECUTION_STATE_BITS;
    private static final int NOTIFICATION_IDLE = 0;
    private static final int NOTIFICATION_PROCESSING = 2;
    private static final int NOTIFICATION_QUEUED = 1;
    private static final int NOTIFICATION_RESIGNAL = 3;
    private static final long NOTIFICATION_STATE_OFFSET = Unsafe.getFieldOffset(Fiber.class, "notificationState");
    private static final long RETIREMENT_STATE_OFFSET = Unsafe.getFieldOffset(Fiber.class, "retirementState");
    private static final ContinuationScope SCOPE = new ContinuationScope("questdb-fiber");
    private static final long WAIT_ADMISSION_OFFSET = Unsafe.getFieldOffset(Fiber.class, "waitAdmission");
    private final CancellationBinding cancellationBindingScratch = new CancellationBinding();
    private final PinnableContinuation continuation;
    private final Rnd fiberAsyncRandom;
    private final Rnd fiberRandom;
    private final Outcome outcomeScratch = new Outcome();
    private final FiberPool pool;
    private final SuspensionScope.RoleSwitchReadLockState roleSwitchReadLocks = new SuspensionScope.RoleSwitchReadLockState();
    private final FiberWaitCoordinator waitCoordinator;
    private FiberCancellationSignal assignedCancellationSignal;
    private long assignedCancellationSignalGeneration = CancellationBinding.NO_GENERATION;
    private CancellationBinding.Source assignedCancellationSource;
    private FiberCancellationSignal assignedSupplementalCancellationSignal;
    private long assignedSupplementalCancellationSignalGeneration = CancellationBinding.NO_GENERATION;
    private FiberTask assignedTask;
    private TimerShards assignedTimerShards;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long executionState = packExecutionState(0, EXECUTION_FREE);
    private boolean isAsyncRandomInitialized;
    private boolean isRandomInitialized;
    private volatile boolean isShutdown;
    private int lastMountWorkerId = FiberRuntime.NO_WORKER;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int notificationState = NOTIFICATION_IDLE;
    private Throwable outcomeError;
    private FiberTask outcomeTask;
    private int outcomeType;
    private int registryIndex = -1;
    private volatile long reservationEpoch;
    @SuppressWarnings("unused")
    private volatile int retirementState;
    @SuppressWarnings("unused")
    private volatile int waitAdmission;
    private int yieldReason = YIELD_WAIT;

    Fiber(FiberPool pool, @Nullable Runnable beforeWaitFireForTesting) {
        this.continuation = new PinnableContinuation(this::taskRunnerLoop);
        this.fiberAsyncRandom = new Rnd();
        this.fiberRandom = new Rnd();
        this.pool = pool;
        this.waitCoordinator = new FiberWaitCoordinator(this, beforeWaitFireForTesting);
    }

    @Nullable
    public static Fiber current() {
        return SuspensionScope.scope().fiber;
    }

    public static boolean isMounted() {
        return Continuation.getCurrentContinuation(SCOPE) != null;
    }

    /**
     * Cooperatively unmounts the current Fiber and reschedules it through its runtime. A false
     * return means the continuation was pinned and the method restored the mounted state before
     * returning.
     */
    public static boolean yieldCooperatively() {
        final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
        final Fiber fiber = scope.fiber;
        if (fiber == null || scope.mode != SuspensionScope.Mode.FIBER) {
            throw new IllegalStateException("cooperative yield requires a mounted Fiber scope");
        }
        if (!Unsafe.cas(
                fiber,
                EXECUTION_STATE_OFFSET,
                packExecutionState(0, EXECUTION_MOUNTED),
                packExecutionState(0, EXECUTION_PARKING)
        )) {
            throw new IllegalStateException("cooperative yield requires mounted execution");
        }
        final int previousYieldReason = fiber.yieldReason;
        fiber.yieldReason = YIELD_COOPERATIVE;
        boolean isSuspended = false;
        boolean isRolledBack = false;
        try {
            isSuspended = suspend();
            if (!isSuspended) {
                fiber.rollbackCooperativeYield();
                isRolledBack = true;
            } else if (fiber.executionState != packExecutionState(0, EXECUTION_MOUNTED)) {
                throw new IllegalStateException("cooperative yield resumed without a mount");
            }
            return isSuspended;
        } catch (Throwable th) {
            if (!isSuspended && !isRolledBack) {
                fiber.rollbackCooperativeYield();
            }
            throw th;
        } finally {
            fiber.yieldReason = previousYieldReason;
        }
    }

    static void verifyRuntimeAccess() {
        Continuation.getCurrentContinuation(SCOPE);
    }

    @Override
    public void abortWait(long token) {
        while (true) {
            final long current = executionState;
            if (executionToken(current) != token) {
                return;
            }
            final int state = executionState(current);
            if (state != EXECUTION_MOUNTED
                    && state != EXECUTION_PARKING
                    && state != EXECUTION_RESUME_PENDING
                    && state != EXECUTION_RUNNABLE) {
                // fireWait() is the only wake path for a WAITING fiber. abort() callers run on
                // the owning fiber's carrier while it is still mounted; a matching-token WAITING
                // state here means that contract broke and the fiber would strand parked.
                assert state != EXECUTION_WAITING : "abortWait on a WAITING fiber";
                return;
            }
            final int targetState = state == EXECUTION_RUNNABLE ? EXECUTION_RUNNABLE : EXECUTION_MOUNTED;
            if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, packExecutionState(0, targetState))) {
                releaseWaitAdmission();
                return;
            }
        }
    }

    public long beginWaitBuild(int sourceCount) {
        final long token = tryBeginWaitBuild(sourceCount);
        if (token == TOKEN_REFUSED) {
            throw new IllegalStateException("fiber runtime is quiescing");
        }
        return token;
    }

    @Override
    public boolean fireWait(long token, int reason) {
        while (true) {
            final long current = executionState;
            if (executionToken(current) != token) {
                return false;
            }
            final int state = executionState(current);
            if (state == EXECUTION_PARKING) {
                if (Unsafe.cas(
                        this,
                        EXECUTION_STATE_OFFSET,
                        current,
                        withExecutionState(current, EXECUTION_RESUME_PENDING)
                )) {
                    return true;
                }
            } else if (state == EXECUTION_WAITING) {
                if (Unsafe.cas(
                        this,
                        EXECUTION_STATE_OFFSET,
                        current,
                        withExecutionState(current, EXECUTION_RUNNABLE)
                )) {
                    pool.onUnparked();
                    requestRun();
                    return true;
                }
            } else {
                return state == EXECUTION_RESUME_PENDING || state == EXECUTION_RUNNABLE || state == EXECUTION_MOUNTED;
            }
        }
    }

    public Rnd getAsyncRandom(NanosecondClock nanosecondClock, MicrosecondClock microsecondClock) {
        if (!isAsyncRandomInitialized) {
            fiberAsyncRandom.reset(
                    nanosecondClock.getTicks(),
                    microsecondClock.getTicks()
            );
            isAsyncRandomInitialized = true;
        }
        return fiberAsyncRandom;
    }

    public Rnd getRandom(NanosecondClock nanosecondClock, MicrosecondClock microsecondClock) {
        if (!isRandomInitialized) {
            fiberRandom.reset(
                    nanosecondClock.getTicks(),
                    microsecondClock.getTicks()
            );
            isRandomInitialized = true;
        }
        return fiberRandom;
    }

    public long getReservationEpoch() {
        final long state = executionState;
        if (executionState(state) != EXECUTION_RESERVED) {
            throw new IllegalStateException("fiber reservation is not active");
        }
        return executionToken(state);
    }

    public FiberWaitCoordinator getWaitCoordinator() {
        return waitCoordinator;
    }

    @TestOnly
    public int getLastMountWorkerIdForTesting() {
        return lastMountWorkerId;
    }

    @Override
    public void onWaitRegistrationAcquired() {
        pool.onWaitRegistrationAcquired();
    }

    @Override
    public void onWaitRegistrationReleased() {
        pool.onWaitRegistrationReleased();
    }

    @TestOnly
    public void setExecutionStateForTesting(long executionState) {
        this.executionState = executionState;
    }

    public int suspendWait(long token) {
        return suspendWait(token, FiberWaitCoordinator.REASON_NONE);
    }

    public int suspendWait(long token, int abortedReason) {
        if (!waitCoordinator.seal(token)) {
            releaseWaitAdmission();
            return waitCoordinator.consumeWait(token, abortedReason);
        }
        releaseWaitAdmission();
        final long current = executionState;
        if (executionToken(current) != token) {
            throw new IllegalStateException("fiber wait token changed");
        }
        if (executionState(current) == EXECUTION_RESUME_PENDING) {
            return waitCoordinator.consumeWait(token, abortedReason);
        }
        if (executionState(current) != EXECUTION_PARKING) {
            throw new IllegalStateException("fiber wait is not parking");
        }
        if (suspend()) {
            return waitCoordinator.consumeWait(token, abortedReason);
        }
        waitCoordinator.abort(token);
        return waitCoordinator.consumeWait(token, abortedReason);
    }

    /**
     * Begins a wait build, returning {@link #TOKEN_REFUSED} instead of throwing when the runtime
     * has closed admission. Callers that treat a quiescing runtime as a normal shutdown outcome
     * must use this over {@link #beginWaitBuild(int)}.
     */
    public long tryBeginWaitBuild(int sourceCount) {
        if (isShutdown) {
            return TOKEN_REFUSED;
        }
        if (!pool.beginWaitArm()) {
            return TOKEN_REFUSED;
        }
        if (!Unsafe.cas(this, WAIT_ADMISSION_OFFSET, 0, 1)) {
            pool.endWaitArm();
            throw new IllegalStateException("fiber wait admission is already held");
        }
        try {
            final long token = waitCoordinator.beginBuild(sourceCount);
            if (token > EXECUTION_TOKEN_MASK) {
                throw new IllegalStateException("fiber wait token exhausted");
            }
            if (!Unsafe.cas(
                    this,
                    EXECUTION_STATE_OFFSET,
                    packExecutionState(0, EXECUTION_MOUNTED),
                    packExecutionState(token, EXECUTION_PARKING)
            )) {
                waitCoordinator.abort(token);
                waitCoordinator.consume(token);
                throw new IllegalStateException("fiber is not mounted");
            }
            return token;
        } catch (Throwable th) {
            releaseWaitAdmission();
            throw th;
        }
    }

    private static int executionState(long executionState) {
        return (int) (executionState & EXECUTION_STATE_MASK);
    }

    private static long executionToken(long executionState) {
        return executionState >>> EXECUTION_STATE_BITS;
    }

    private static boolean hasNoRoleSwitchReadLock() {
        final Fiber fiber = current();
        return fiber == null || !fiber.roleSwitchReadLocks.hasAny();
    }

    private static IllegalStateException invalidNotificationState(int state) {
        return new IllegalStateException("invalid fiber notification state [state=" + state + ']');
    }

    private static IllegalStateException invalidTerminatedNotificationState(int state) {
        return new IllegalStateException("invalid terminated fiber notification state [state=" + state + ']');
    }

    private static IllegalStateException invalidWaitState(int state) {
        return new IllegalStateException("invalid fiber wait state [state=" + state + ']');
    }

    private static long packExecutionState(long token, int state) {
        return (token << EXECUTION_STATE_BITS) | state;
    }

    private static boolean suspend() {
        // A role-switch read holder runs in BLOCKING mode and releases the fence before any
        // wait; a park with the hold live would migrate the fiber off the tracked reentrancy
        // state and stall a queued role-switch writer silently.
        assert hasNoRoleSwitchReadLock() : "fiber suspending while holding a role-switch read lock";
        final boolean isSuspended = Continuation.yield(SCOPE);
        if (!isSuspended) {
            final Fiber fiber = current();
            if (fiber != null) {
                fiber.onSuspendRefused();
            }
        }
        return isSuspended;
    }

    private static long withExecutionState(long executionState, int state) {
        return (executionState & ~EXECUTION_STATE_MASK) | state;
    }

    private void abandonAssignedTask() {
        if (assignedTask != null) {
            outcomeTask = assignedTask;
            clearAssignedTask();
            outcomeType = OUTCOME_ABANDONED;
        }
    }

    private void clearAssignedTask() {
        assignedCancellationSignal = null;
        assignedCancellationSignalGeneration = CancellationBinding.NO_GENERATION;
        assignedCancellationSource = null;
        assignedSupplementalCancellationSignal = null;
        assignedSupplementalCancellationSignalGeneration = CancellationBinding.NO_GENERATION;
        assignedTask = null;
        assignedTimerShards = null;
    }

    private Throwable releaseRoleSwitchReadLock(boolean isTaskLeak) {
        if (!roleSwitchReadLocks.hasAny()) {
            return null;
        }
        final int leakedDepth = roleSwitchReadLocks.getHoldCount();
        Throwable failure = isTaskLeak
                ? new IllegalStateException("fiber task leaked role-switch read lock [depth=" + leakedDepth + ']')
                : null;
        final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
        final Fiber previousFiber = scope.fiber;
        final SuspensionScope.Mode previousMode = scope.mode;
        final SuspensionScope.Mode savedMode = roleSwitchReadLocks.getPreviousMode();
        scope.fiber = this;
        try {
            Lock lock;
            while ((lock = roleSwitchReadLocks.getAnyLock()) != null) {
                lock.unlock();
            }
        } catch (Throwable th) {
            if (failure == null) {
                failure = th;
            } else if (failure != th) {
                failure.addSuppressed(th);
            }
        } finally {
            roleSwitchReadLocks.clear();
            scope.fiber = previousFiber;
            scope.mode = previousFiber == this ? savedMode : previousMode;
        }
        return failure;
    }

    private void releaseWaitAdmission() {
        if (Unsafe.cas(this, WAIT_ADMISSION_OFFSET, 1, 0)) {
            pool.endWaitArm();
        }
    }

    private void rollbackCooperativeYield() {
        while (true) {
            final long current = executionState;
            if (executionToken(current) != 0) {
                throw new IllegalStateException("cooperative yield changed the execution token");
            }
            final int state = executionState(current);
            if (state == EXECUTION_MOUNTED) {
                return;
            }
            if (state != EXECUTION_PARKING && state != EXECUTION_RESUME_PENDING) {
                throw new IllegalStateException("cooperative yield cannot restore execution [state=" + state + ']');
            }
            if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, packExecutionState(0, EXECUTION_MOUNTED))) {
                return;
            }
        }
    }

    private void rollbackUnpublished(FiberTask task, long reservationEpoch) {
        if (assignedTask != task
                || executionState != packExecutionState(0, EXECUTION_RUNNABLE)
                || notificationState != NOTIFICATION_IDLE) {
            throw new IllegalStateException("fiber cannot roll back unpublished task");
        }
        clearAssignedTask();
        executionState = packExecutionState(reservationEpoch, EXECUTION_RESERVED);
    }

    private void runAssignedTask() {
        final FiberTask task = assignedTask;
        if (task == null) {
            throw new IllegalStateException("fiber has no assigned task");
        }
        boolean isDone = false;
        Throwable error = null;
        try {
            isDone = task.runStep();
        } catch (Throwable th) {
            error = th;
        }
        final Throwable lockLeak = releaseRoleSwitchReadLock(true);
        if (lockLeak != null) {
            if (error == null) {
                error = lockLeak;
            } else {
                error.addSuppressed(lockLeak);
            }
        }
        if (error == null && !isDone) {
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            task.updateCancellationBinding(scope.cancellationSignal, scope.cancellationSignalGeneration);
        }
        clearAssignedTask();
        outcomeError = error;
        outcomeTask = task;
        outcomeType = error != null ? OUTCOME_ERROR : isDone ? OUTCOME_DONE : OUTCOME_PARKED;
    }

    private void taskRunnerLoop() {
        while (!isShutdown) {
            if (assignedTask != null) {
                yieldReason = YIELD_WAIT;
                runAssignedTask();
                if (isShutdown) {
                    break;
                }
            }
            yieldReason = YIELD_FREE;
            if (suspend()) {
                continue;
            }
            break;
        }
        abandonAssignedTask();
    }

    boolean beginMount() {
        while (true) {
            final long current = executionState;
            if (executionState(current) != EXECUTION_RUNNABLE) {
                return false;
            }
            if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, withExecutionState(current, EXECUTION_MOUNTED))) {
                return true;
            }
        }
    }

    boolean beginProcessing() {
        return Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_QUEUED, NOTIFICATION_PROCESSING);
    }

    void beginRetirement() {
        Unsafe.cas(this, RETIREMENT_STATE_OFFSET, 0, 1);
    }

    boolean completeRetirement() {
        return Unsafe.cas(this, RETIREMENT_STATE_OFFSET, 1, 2);
    }

    void finishProcessing(@Nullable FiberRuntime.OwnerContext ownerContext) {
        while (true) {
            final int state = notificationState;
            if (state == NOTIFICATION_PROCESSING) {
                if (Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_PROCESSING, NOTIFICATION_IDLE)) {
                    return;
                }
            } else if (state == NOTIFICATION_RESIGNAL) {
                if (Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_RESIGNAL, NOTIFICATION_QUEUED)) {
                    try {
                        pool.enqueueAfterProcessing(this, ownerContext);
                        return;
                    } catch (Throwable th) {
                        if (!Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_QUEUED, NOTIFICATION_RESIGNAL)) {
                            throw new IllegalStateException("fiber enqueue failure rollback failed", th);
                        }
                        throw th;
                    }
                }
            } else {
                throw invalidNotificationState(state);
            }
        }
    }

    void finishTerminatedProcessing() {
        while (true) {
            final int state = notificationState;
            if (state != NOTIFICATION_PROCESSING && state != NOTIFICATION_RESIGNAL) {
                throw invalidTerminatedNotificationState(state);
            }
            if (Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, state, NOTIFICATION_IDLE)) {
                return;
            }
        }
    }

    CancellationBinding getCancellationBindingScratch() {
        return cancellationBindingScratch;
    }

    int getExecutionState() {
        return executionState(executionState);
    }

    int getLastMountWorkerId() {
        return lastMountWorkerId;
    }

    int getNotificationState() {
        return notificationState;
    }

    Outcome getOutcomeScratch() {
        return outcomeScratch;
    }

    int getRegistryIndex() {
        return registryIndex;
    }

    SuspensionScope.RoleSwitchReadLockState getRoleSwitchReadLockState() {
        return roleSwitchReadLocks;
    }

    @Nullable
    FiberTask getTaskAfterDriverFailure(Outcome mountedOutcome) {
        return assignedTask != null
                ? assignedTask
                : outcomeTask != null
                  ? outcomeTask
                  : mountedOutcome.task;
    }

    int getYieldReason() {
        return yieldReason;
    }

    boolean isDone() {
        return continuation.isDone();
    }

    boolean isForeignTo(FiberRuntime runtime) {
        return pool.getRuntime() != runtime;
    }

    boolean isReservationStale(long reservationEpoch) {
        final long state = executionState;
        if (state != packExecutionState(reservationEpoch, EXECUTION_RESERVED)) {
            return true;
        }
        final int notification = notificationState;
        return notification != NOTIFICATION_IDLE && notification != NOTIFICATION_PROCESSING;
    }

    boolean isReserved() {
        if (executionState(executionState) != EXECUTION_RESERVED) {
            return false;
        }
        final int state = notificationState;
        return state == NOTIFICATION_IDLE || state == NOTIFICATION_PROCESSING;
    }

    boolean isShutdownRequested() {
        return isShutdown;
    }

    void markRetired() {
        clearAssignedTask();
        while (true) {
            final long current = executionState;
            if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, packExecutionState(0, EXECUTION_DONE))) {
                if (executionState(current) == EXECUTION_WAITING) {
                    pool.onUnparked();
                }
                break;
            }
        }
        outcomeError = null;
        outcomeTask = null;
        outcomeType = OUTCOME_NONE;
    }

    void onSuspendRefused() {
        pool.getRuntime().onInlineSuspendViolation(continuation.takePinnedReason());
        while (true) {
            final long current = executionState;
            final int state = executionState(current);
            if (state != EXECUTION_PARKING && state != EXECUTION_RESUME_PENDING) {
                return;
            }
            if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, withExecutionState(current, EXECUTION_MOUNTED))) {
                return;
            }
        }
    }

    void prepareDriverFailure(Throwable driverFailure) throws Throwable {
        isShutdown = true;
        Throwable failure = null;
        try {
            waitCoordinator.quarantine();
        } catch (Throwable th) {
            failure = th;
        }
        try {
            releaseWaitAdmission();
        } catch (Throwable th) {
            if (failure == null) {
                failure = th;
            } else if (failure != th) {
                failure.addSuppressed(th);
            }
        }
        if (!continuation.isDone()) {
            try {
                runMounted();
            } catch (Throwable th) {
                if (failure == null) {
                    failure = th;
                } else if (failure != th) {
                    failure.addSuppressed(th);
                }
            }
        }
        if (outcomeError != null) {
            if (outcomeError != driverFailure) {
                driverFailure.addSuppressed(outcomeError);
            }
        }
        final Throwable lockFailure = releaseRoleSwitchReadLock(false);
        if (failure == null) {
            failure = lockFailure;
        } else if (lockFailure != null && failure != lockFailure) {
            failure.addSuppressed(lockFailure);
        }
        if (failure != null) {
            throw failure;
        }
    }

    void prepareShutdown() {
        isShutdown = true;
        waitCoordinator.shutdown();
        if (transitionFreeToRunnable()) {
            requestRun();
        }
    }

    void publishCooperativeYield() {
        if (!Unsafe.cas(
                this,
                EXECUTION_STATE_OFFSET,
                packExecutionState(0, EXECUTION_PARKING),
                packExecutionState(0, EXECUTION_RUNNABLE)
        )) {
            throw new IllegalStateException("Fiber cooperative yield is not parking");
        }
        requestRun();
    }

    void publishWaiting() {
        while (true) {
            final long current = executionState;
            final int state = executionState(current);
            if (state == EXECUTION_PARKING) {
                pool.onParked();
                if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, withExecutionState(current, EXECUTION_WAITING))) {
                    return;
                }
                pool.onUnparked();
            } else if (state == EXECUTION_RESUME_PENDING) {
                if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, withExecutionState(current, EXECUTION_RUNNABLE))) {
                    requestRun();
                    return;
                }
            } else {
                throw invalidWaitState(state);
            }
        }
    }

    void requestRun() {
        while (true) {
            final int state = notificationState;
            if (state == NOTIFICATION_IDLE) {
                if (Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_IDLE, NOTIFICATION_QUEUED)) {
                    try {
                        pool.enqueue(this);
                    } catch (Throwable th) {
                        if (!Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_QUEUED, NOTIFICATION_IDLE)) {
                            throw new IllegalStateException("fiber enqueue failure rollback failed", th);
                        }
                        throw th;
                    }
                    return;
                }
            } else if (state == NOTIFICATION_PROCESSING) {
                if (Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_PROCESSING, NOTIFICATION_RESIGNAL)) {
                    return;
                }
            } else if (state == NOTIFICATION_QUEUED || state == NOTIFICATION_RESIGNAL) {
                return;
            } else {
                throw invalidNotificationState(state);
            }
        }
    }

    long reserve() {
        if (reservationEpoch == EXECUTION_TOKEN_MASK) {
            throw new IllegalStateException("fiber reservation epoch exhausted");
        }
        final long nextEpoch = reservationEpoch + 1;
        if (!Unsafe.cas(
                this,
                EXECUTION_STATE_OFFSET,
                packExecutionState(0, EXECUTION_FREE),
                packExecutionState(nextEpoch, EXECUTION_RESERVED)
        )) {
            throw new IllegalStateException("fiber is not free");
        }
        reservationEpoch = nextEpoch;
        lastMountWorkerId = FiberRuntime.NO_WORKER;
        return nextEpoch;
    }

    void restageAndRequestRun(FiberTask task) {
        final long reservationEpoch = reserve();
        try {
            stageAndRequestRun(task, reservationEpoch);
        } catch (RuntimeException | Error th) {
            tryReleaseReservation(reservationEpoch);
            throw th;
        }
    }

    void runMounted() {
        final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
        if (scope.roleSwitchReadLocks.hasAny() || scope.roleSwitchWriteLockDepth > 0) {
            throw new IllegalStateException("fiber mount would hide a carrier role-switch lock");
        }
        final Fiber previousFiber = scope.fiber;
        final FiberCancellationSignal previousCancellationSignal = scope.cancellationSignal;
        final long previousCancellationSignalGeneration = scope.cancellationSignalGeneration;
        final CancellationBinding.Source previousCancellationSource = scope.cancellationSource;
        final SuspensionScope.Mode previousMode = scope.mode;
        final FiberCancellationSignal previousSupplementalCancellationSignal = scope.supplementalCancellationSignal;
        final long previousSupplementalCancellationSignalGeneration = scope.supplementalCancellationSignalGeneration;
        final TimerShards previousTimerShards = scope.timerShards;
        scope.cancellationSignal = assignedCancellationSignal;
        scope.cancellationSignalGeneration = assignedCancellationSignalGeneration;
        scope.cancellationSource = assignedCancellationSource;
        scope.fiber = this;
        scope.mode = SuspensionScope.Mode.FIBER;
        scope.supplementalCancellationSignal = assignedSupplementalCancellationSignal;
        scope.supplementalCancellationSignalGeneration = assignedSupplementalCancellationSignalGeneration;
        scope.timerShards = assignedTimerShards;
        try {
            continuation.run();
        } finally {
            try {
                if (assignedTask != null) {
                    assignedCancellationSignal = scope.cancellationSignal;
                    assignedCancellationSignalGeneration = scope.cancellationSignalGeneration;
                    assignedCancellationSource = scope.cancellationSource;
                    assignedSupplementalCancellationSignal = scope.supplementalCancellationSignal;
                    assignedSupplementalCancellationSignalGeneration = scope.supplementalCancellationSignalGeneration;
                    assignedTimerShards = scope.timerShards;
                    assignedTask.updateCancellationBinding(
                            assignedCancellationSignal,
                            assignedCancellationSignalGeneration
                    );
                }
            } finally {
                scope.cancellationSignal = previousCancellationSignal;
                scope.cancellationSignalGeneration = previousCancellationSignalGeneration;
                scope.cancellationSource = previousCancellationSource;
                scope.fiber = previousFiber;
                scope.mode = previousMode;
                scope.supplementalCancellationSignal = previousSupplementalCancellationSignal;
                scope.supplementalCancellationSignalGeneration = previousSupplementalCancellationSignalGeneration;
                scope.timerShards = previousTimerShards;
            }
        }
    }

    void setLastMountWorkerId(int workerId) {
        lastMountWorkerId = workerId;
    }

    void setRegistryIndex(int registryIndex) {
        this.registryIndex = registryIndex;
    }

    void stage(FiberTask task, long reservationEpoch) {
        if (assignedTask != null
                || executionState != packExecutionState(reservationEpoch, EXECUTION_RESERVED)) {
            throw new IllegalStateException("fiber is not reserved");
        }
        task.captureCancellationBinding();
        assignedCancellationSignal = task.getBoundCancellationSignal();
        assignedCancellationSignalGeneration = task.getBoundCancellationSignalGeneration();
        assignedTask = task;
        if (!Unsafe.cas(
                this,
                EXECUTION_STATE_OFFSET,
                packExecutionState(reservationEpoch, EXECUTION_RESERVED),
                packExecutionState(0, EXECUTION_RUNNABLE)
        )) {
            clearAssignedTask();
            throw new IllegalStateException("fiber is not reserved");
        }
    }

    void stageAndRequestRun(FiberTask task, long reservationEpoch) {
        stage(task, reservationEpoch);
        try {
            requestRun();
        } catch (RuntimeException | Error th) {
            rollbackUnpublished(task, reservationEpoch);
            throw th;
        }
    }

    boolean stageForDirectMountOrRequestRun(FiberTask task, long reservationEpoch) {
        while (true) {
            final int state = notificationState;
            if (state == NOTIFICATION_IDLE) {
                if (!Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_IDLE, NOTIFICATION_PROCESSING)) {
                    continue;
                }
                try {
                    stage(task, reservationEpoch);
                    return true;
                } catch (Throwable th) {
                    finishTerminatedProcessing();
                    throw th;
                }
            }
            if (state == NOTIFICATION_PROCESSING) {
                stageAndRequestRun(task, reservationEpoch);
                return false;
            }
            throw new IllegalStateException("fiber notification is not idle or processing");
        }
    }

    void takeOutcome(Outcome target) {
        target.error = outcomeError;
        target.task = outcomeTask;
        target.type = outcomeType;
        outcomeError = null;
        outcomeTask = null;
        outcomeType = OUTCOME_NONE;
    }

    boolean transitionFreeToRunnable() {
        return Unsafe.cas(
                this,
                EXECUTION_STATE_OFFSET,
                packExecutionState(0, EXECUTION_FREE),
                packExecutionState(0, EXECUTION_RUNNABLE)
        );
    }

    boolean transitionMountedToFree() {
        while (true) {
            final long current = executionState;
            if (executionToken(current) != 0) {
                return false;
            }
            final int state = executionState(current);
            if (state != EXECUTION_MOUNTED && state != EXECUTION_RESUME_PENDING) {
                return false;
            }
            if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, packExecutionState(0, EXECUTION_FREE))) {
                return true;
            }
        }
    }

    boolean tryReleaseReservation(long reservationEpoch) {
        if (reservationEpoch < 1 || reservationEpoch > EXECUTION_TOKEN_MASK) {
            return false;
        }
        return Unsafe.cas(
                this,
                EXECUTION_STATE_OFFSET,
                packExecutionState(reservationEpoch, EXECUTION_RESERVED),
                packExecutionState(0, EXECUTION_FREE)
        );
    }

    static final class Outcome {
        Throwable error;
        FiberTask task;
        int type;

        void clear() {
            error = null;
            task = null;
            type = OUTCOME_NONE;
        }
    }

    // onPinned() throws by default; recording the reason and returning normally is what makes
    // Continuation.yield report a refused suspend through its return value
    private static final class PinnableContinuation extends Continuation {
        private Pinned pinnedReason;

        private PinnableContinuation(Runnable body) {
            super(SCOPE, body);
        }

        @Override
        protected void onPinned(Pinned reason) {
            pinnedReason = reason;
        }

        private CharSequence takePinnedReason() {
            final Pinned reason = pinnedReason;
            pinnedReason = null;
            return reason != null ? reason.name() : "UNKNOWN";
        }
    }
}

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

import io.questdb.std.CarrierLocal;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import jdk.internal.vm.Continuation;
import jdk.internal.vm.ContinuationScope;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

public final class Fiber implements FiberWaitCoordinator.Target {
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
    private static final CarrierLocal<Fiber> CURRENT = new CarrierLocal<>();
    private static final long EXECUTION_STATE_MASK = (1L << EXECUTION_STATE_BITS) - 1;
    private static final long EXECUTION_STATE_OFFSET = Unsafe.getFieldOffset(Fiber.class, "executionState");
    private static final long EXECUTION_TOKEN_MASK = -1L >>> EXECUTION_STATE_BITS;
    private static final int NOTIFICATION_IDLE = 0;
    private static final int NOTIFICATION_PROCESSING = 2;
    private static final int NOTIFICATION_QUEUED = 1;
    private static final int NOTIFICATION_RESIGNAL = 3;
    private static final long NOTIFICATION_STATE_OFFSET = Unsafe.getFieldOffset(Fiber.class, "notificationState");
    private static final AtomicLong RANDOM_SEED = new AtomicLong(System.nanoTime());
    private static final long RETIREMENT_STATE_OFFSET = Unsafe.getFieldOffset(Fiber.class, "retirementState");
    private static final ContinuationScope SCOPE = new ContinuationScope("questdb-fiber");
    private static final long WAIT_ADMISSION_OFFSET = Unsafe.getFieldOffset(Fiber.class, "waitAdmission");
    private FiberTask assignedTask;
    private final Continuation continuation;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long executionState = packExecutionState(0, EXECUTION_FREE);
    private final Rnd fiberAsyncRandom;
    private final Rnd fiberRandom;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int notificationState = NOTIFICATION_IDLE;
    private Throwable outcomeError;
    private final Outcome outcomeScratch = new Outcome();
    private FiberTask outcomeTask;
    private int outcomeType;
    private final FiberPool pool;
    private int registryIndex = -1;
    @SuppressWarnings("unused")
    private volatile int retirementState;
    private volatile boolean shutdown;
    @SuppressWarnings("unused")
    private volatile int waitAdmission;
    private final FiberWaitCoordinator waitCoordinator;
    private int yieldReason = YIELD_WAIT;

    Fiber(FiberPool pool, @Nullable Runnable beforeWaitFireForTesting) {
        final long asyncSeed = RANDOM_SEED.getAndAdd(0x9e3779b97f4a7c15L);
        final long randomSeed = RANDOM_SEED.getAndAdd(0x9e3779b97f4a7c15L);
        this.continuation = new Continuation(SCOPE, this::taskRunnerLoop);
        this.fiberAsyncRandom = new Rnd(asyncSeed, asyncSeed ^ 0xd1b54a32d192ed03L);
        this.fiberRandom = new Rnd(randomSeed, randomSeed ^ 0x94d049bb133111ebL);
        this.pool = pool;
        this.waitCoordinator = new FiberWaitCoordinator(this, beforeWaitFireForTesting);
    }

    @Nullable
    public static Fiber current() {
        return CURRENT.get();
    }

    public static void initializeCarrier() {
        CURRENT.get();
    }

    public static boolean isMounted() {
        return Continuation.getCurrentContinuation(SCOPE) != null;
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
        if (!pool.beginWaitArm()) {
            throw new IllegalStateException("fiber runtime is quiescing");
        }
        waitAdmission = 1;
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
                    requestRun();
                    return true;
                }
            } else {
                return state == EXECUTION_RESUME_PENDING || state == EXECUTION_RUNNABLE || state == EXECUTION_MOUNTED;
            }
        }
    }

    public Rnd getAsyncRandom() {
        return fiberAsyncRandom;
    }

    public Rnd getRandom() {
        return fiberRandom;
    }

    public FiberWaitCoordinator getWaitCoordinator() {
        return waitCoordinator;
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
        if (!waitCoordinator.seal(token)) {
            releaseWaitAdmission();
            final int reason = waitCoordinator.consume(token);
            if (reason != FiberWaitCoordinator.REASON_NONE) {
                return reason;
            }
            throw new IllegalStateException("fiber wait cannot be sealed");
        }
        releaseWaitAdmission();
        final long current = executionState;
        if (executionToken(current) != token) {
            throw new IllegalStateException("fiber wait token changed");
        }
        if (executionState(current) == EXECUTION_RESUME_PENDING) {
            return waitCoordinator.consume(token);
        }
        if (executionState(current) != EXECUTION_PARKING) {
            throw new IllegalStateException("fiber wait is not parking");
        }
        if (suspend()) {
            return waitCoordinator.consume(token);
        }
        waitCoordinator.abort(token);
        return waitCoordinator.consume(token);
    }

    private static int executionState(long executionState) {
        return (int) (executionState & EXECUTION_STATE_MASK);
    }

    private static long executionToken(long executionState) {
        return executionState >>> EXECUTION_STATE_BITS;
    }

    private static long packExecutionState(long token, int state) {
        return (token << EXECUTION_STATE_BITS) | state;
    }

    private static boolean suspend() {
        final boolean isSuspended = Continuation.yield(SCOPE);
        if (!isSuspended) {
            final Fiber fiber = CURRENT.get();
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
            assignedTask = null;
            outcomeType = OUTCOME_ABANDONED;
        }
    }

    private void releaseWaitAdmission() {
        if (Unsafe.cas(this, WAIT_ADMISSION_OFFSET, 1, 0)) {
            pool.endWaitArm();
        }
    }

    private void rollbackUnpublished(FiberTask task) {
        if (assignedTask != task
                || executionState != packExecutionState(0, EXECUTION_RUNNABLE)
                || notificationState != NOTIFICATION_QUEUED) {
            throw new IllegalStateException("fiber cannot roll back unpublished task");
        }
        notificationState = NOTIFICATION_IDLE;
        assignedTask = null;
        executionState = packExecutionState(0, EXECUTION_RESERVED);
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
        } catch (BackpressureSignal ignore) {
        } catch (Throwable th) {
            error = th;
        }
        assignedTask = null;
        outcomeError = error;
        outcomeTask = task;
        outcomeType = error != null ? OUTCOME_ERROR : isDone ? OUTCOME_DONE : OUTCOME_PARKED;
    }

    private void taskRunnerLoop() {
        while (!shutdown) {
            if (assignedTask == null) {
                yieldReason = YIELD_FREE;
                if (!suspend()) {
                    Os.pause();
                }
                continue;
            }
            yieldReason = YIELD_WAIT;
            runAssignedTask();
            yieldReason = YIELD_FREE;
            if (!suspend()) {
                Os.pause();
            }
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

    @Nullable
    FiberTask detachTaskAfterDriverFailure(Outcome mountedOutcome) {
        final FiberTask task = assignedTask != null
                ? assignedTask
                : outcomeTask != null
                ? outcomeTask
                : mountedOutcome.task;
        assignedTask = null;
        outcomeError = null;
        outcomeTask = null;
        outcomeType = OUTCOME_NONE;
        return task;
    }

    void finishProcessing() {
        while (true) {
            final int state = notificationState;
            if (state == NOTIFICATION_PROCESSING) {
                if (Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_PROCESSING, NOTIFICATION_IDLE)) {
                    return;
                }
            } else if (state == NOTIFICATION_RESIGNAL) {
                if (Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_RESIGNAL, NOTIFICATION_QUEUED)) {
                    try {
                        pool.enqueue(this);
                        return;
                    } catch (Throwable th) {
                        if (!Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_QUEUED, NOTIFICATION_PROCESSING)) {
                            throw new IllegalStateException("fiber enqueue failure rollback failed", th);
                        }
                        throw th;
                    }
                }
            } else {
                throw new IllegalStateException("invalid fiber notification state [state=" + state + ']');
            }
        }
    }

    void finishTerminatedProcessing() {
        while (true) {
            final int state = notificationState;
            if (state != NOTIFICATION_PROCESSING && state != NOTIFICATION_RESIGNAL) {
                throw new IllegalStateException("invalid terminated fiber notification state [state=" + state + ']');
            }
            if (Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, state, NOTIFICATION_IDLE)) {
                return;
            }
        }
    }

    int getExecutionState() {
        return executionState(executionState);
    }

    Outcome getOutcomeScratch() {
        return outcomeScratch;
    }

    int getRegistryIndex() {
        return registryIndex;
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

    boolean isReserved() {
        if (executionState != packExecutionState(0, EXECUTION_RESERVED)) {
            return false;
        }
        final int state = notificationState;
        return state == NOTIFICATION_IDLE || state == NOTIFICATION_PROCESSING;
    }

    void markRetired() {
        assignedTask = null;
        executionState = packExecutionState(0, EXECUTION_DONE);
        outcomeError = null;
        outcomeTask = null;
        outcomeType = OUTCOME_NONE;
    }

    void onSuspendRefused() {
        pool.getRuntime().onInlineSuspendViolation();
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

    void prepareDriverFailure() {
        shutdown = true;
        try {
            waitCoordinator.quarantine();
        } finally {
            releaseWaitAdmission();
        }
    }

    void prepareShutdown() {
        shutdown = true;
        waitCoordinator.shutdown();
        if (transitionFreeToRunnable()) {
            requestRun();
        }
    }

    void publishWaiting() {
        while (true) {
            final long current = executionState;
            final int state = executionState(current);
            if (state == EXECUTION_PARKING || state == EXECUTION_MOUNTED) {
                if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, withExecutionState(current, EXECUTION_WAITING))) {
                    return;
                }
            } else if (state == EXECUTION_RESUME_PENDING) {
                if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, withExecutionState(current, EXECUTION_RUNNABLE))) {
                    requestRun();
                    return;
                }
            } else {
                throw new IllegalStateException("invalid fiber wait state [state=" + state + ']');
            }
        }
    }

    void releaseReservation() {
        if (!Unsafe.cas(
                this,
                EXECUTION_STATE_OFFSET,
                packExecutionState(0, EXECUTION_RESERVED),
                packExecutionState(0, EXECUTION_FREE)
        )) {
            throw new IllegalStateException("fiber is not reserved");
        }
    }

    void requestRun() {
        while (true) {
            final int state = notificationState;
            if (state == NOTIFICATION_IDLE) {
                if (Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_IDLE, NOTIFICATION_QUEUED)) {
                    pool.enqueue(this);
                    return;
                }
            } else if (state == NOTIFICATION_PROCESSING) {
                if (Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_PROCESSING, NOTIFICATION_RESIGNAL)) {
                    return;
                }
            } else if (state == NOTIFICATION_QUEUED || state == NOTIFICATION_RESIGNAL) {
                return;
            } else {
                throw new IllegalStateException("invalid fiber notification state [state=" + state + ']');
            }
        }
    }

    void reserve() {
        if (!Unsafe.cas(
                this,
                EXECUTION_STATE_OFFSET,
                packExecutionState(0, EXECUTION_FREE),
                packExecutionState(0, EXECUTION_RESERVED)
        )) {
            throw new IllegalStateException("fiber is not free");
        }
    }

    void restageAndRequestRun(FiberTask task) {
        reserve();
        try {
            stageAndRequestRun(task);
        } catch (RuntimeException | Error th) {
            releaseReservation();
            throw th;
        }
    }

    void runMounted() {
        final Fiber previous = CURRENT.get();
        final FiberCancellationSignal previousCancellationSignal = SuspensionScope.enterCancellationSignal(
                assignedTask != null ? assignedTask.getCancellationSignal() : null
        );
        final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.FIBER);
        CURRENT.set(this);
        try {
            continuation.run();
        } finally {
            CURRENT.set(previous);
            SuspensionScope.restore(previousMode);
            SuspensionScope.restoreCancellationSignal(previousCancellationSignal);
        }
    }

    void setRegistryIndex(int registryIndex) {
        this.registryIndex = registryIndex;
    }

    void stage(FiberTask task) {
        if (assignedTask != null || executionState != packExecutionState(0, EXECUTION_RESERVED)) {
            throw new IllegalStateException("fiber is not reserved");
        }
        assignedTask = task;
        if (!Unsafe.cas(
                this,
                EXECUTION_STATE_OFFSET,
                packExecutionState(0, EXECUTION_RESERVED),
                packExecutionState(0, EXECUTION_RUNNABLE)
        )) {
            assignedTask = null;
            throw new IllegalStateException("fiber is not reserved");
        }
    }

    void stageAndRequestRun(FiberTask task) {
        stage(task);
        try {
            requestRun();
        } catch (RuntimeException | Error th) {
            rollbackUnpublished(task);
            throw th;
        }
    }

    boolean stageForDirectMountOrRequestRun(FiberTask task) {
        while (true) {
            final int state = notificationState;
            if (state == NOTIFICATION_IDLE) {
                if (!Unsafe.cas(this, NOTIFICATION_STATE_OFFSET, NOTIFICATION_IDLE, NOTIFICATION_PROCESSING)) {
                    continue;
                }
                try {
                    stage(task);
                    return true;
                } catch (Throwable th) {
                    notificationState = NOTIFICATION_IDLE;
                    throw th;
                }
            }
            if (state == NOTIFICATION_PROCESSING) {
                stageAndRequestRun(task);
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

    boolean transitionMountedToRunnable() {
        while (true) {
            final long current = executionState;
            if (executionState(current) != EXECUTION_MOUNTED) {
                return false;
            }
            if (Unsafe.cas(this, EXECUTION_STATE_OFFSET, current, withExecutionState(current, EXECUTION_RUNNABLE))) {
                return true;
            }
        }
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
}

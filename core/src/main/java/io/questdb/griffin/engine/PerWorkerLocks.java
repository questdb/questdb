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

package io.questdb.griffin.engine;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberSlotWaitQueue;
import io.questdb.mp.continuation.FiberSlotWaitRegistration;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Used to synchronize access to list-like collections used by worker threads.
 * <p>
 * Each slot uses the 0/1 protocol: acquire changes 0 to 1 with one CAS and release stores 0.
 */
public class PerWorkerLocks implements FiberSlotWaitQueue.SlotReleaser {
    // Reserve extra int array elements to avoid false sharing. A cache line is assumed to take 64 bytes.
    private static final int INTS_PER_SLOT = 64 / Integer.BYTES;
    private static final Log LOG = LogFactory.getLog(PerWorkerLocks.class);
    private static final int SLOT_WAIT_ABORTED = -2;
    private static final int SLOT_WAIT_TIMER_REFUSED = -3;
    private final AtomicIntegerArray locks;
    // Used to randomize acquire attempts for work stealing threads. Accessed in a racy way, intentionally.
    private final Rnd rnd;
    private final FiberSlotWaitQueue slotWaitQueue;
    private final MillisecondClock timerClock;
    private final long timerIntervalMillis;
    private final int workerCount;
    // Test-only: null in production, in which case acquireSlot() reads it once per frame and skips
    // the count down. Volatile so that a reducer on any thread sees the latch a test installs on the
    // owner thread, which is what lets an atom keep a final reference to its locks.
    private volatile CountDownLatch testAcquireLatch;
    private volatile @Nullable Runnable testBeforeSlotRelease;

    public PerWorkerLocks(@NotNull CairoConfiguration configuration, int workerCount) {
        // Every parallel operator that builds locks is gated on sharedQueryWorkerCount > 0
        // (SqlExecutionContextImpl), so a zero-slot lock is unreachable. It would also be unusable:
        // acquireSlot() folds with workerId % workerCount and probes one slot per worker.
        assert workerCount > 0;
        this.rnd = new Rnd(
                configuration.getNanosecondClock().getTicks(),
                configuration.getMicrosecondClock().getTicks()
        );
        this.workerCount = workerCount;
        locks = new AtomicIntegerArray(INTS_PER_SLOT * workerCount);
        slotWaitQueue = new FiberSlotWaitQueue(this);
        timerClock = configuration.getMillisecondClock();
        timerIntervalMillis = Math.max(1, configuration.getQueryContinuationWakeIntervalMillis());
    }

    /**
     * Acquires a slot for the given worker: a mounted fiber parks until one frees up, other callers
     * spin. A successful acquire must be
     * paired with a {@link #releaseSlot(int)} in a finally: there is no reset here, and an atom
     * outlives the query that borrowed it, so a slot leaked on an error path stays lost for as long
     * as the owning factory sits in the SQL cache. Once every slot has leaked, each later execution
     * spins here forever for a slot nobody will release. That is why a reducer must keep every
     * statement that can throw - decoding a frame, charging the per-query memory tracker - inside the
     * try that releases the slot.
     *
     * @throws io.questdb.cairo.CairoException when the circuit breaker has tripped
     */
    public int acquireSlot(int workerId, SqlExecutionCircuitBreaker sqlCircuitBreaker) {
        return acquireSlot(workerId, sqlCircuitBreaker, sqlCircuitBreaker);
    }

    public int acquireSlot(int carrierId, ExecutionCircuitBreaker circuitBreaker) {
        return acquireSlot(carrierId, circuitBreaker, null);
    }

    /**
     * Returns the number of slots currently held. Every acquired slot must be released, so this is
     * zero whenever no worker is inside a locked section. A non-zero count once all workers are done
     * means a slot leaked: there is no reset, so a leaked slot is lost for the lifetime of the owning
     * atom, and the pool eventually starves.
     */
    @TestOnly
    public int getAcquiredSlotCount() {
        int count = 0;
        for (int i = 0; i < workerCount; i++) {
            if (locks.get(INTS_PER_SLOT * i) != 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the latch a test installed, or null - which is every production query. A test-supplied
     * work stealing strategy reads it to decide whether to hold the owner thread off.
     */
    @TestOnly
    public @Nullable CountDownLatch getTestAcquireLatch() {
        return testAcquireLatch;
    }

    @Override
    public void releaseSlot(int slot) {
        if (slot > -1) {
            final int lockIndex = INTS_PER_SLOT * slot;
            while (true) {
                try {
                    if (slotWaitQueue.transfer(slot)) {
                        return;
                    }
                } catch (Throwable th) {
                    // transfer() can only throw from fire(), which runs after markGranted() handed the
                    // slot over; that path releases the slot itself, so re-releasing here would double
                    // release. Callers invoke this from a finally, so nothing may propagate.
                    LOG.critical().$("reducer slot transfer failed [slot=").$(slot).$(", error=").$(th).I$();
                    return;
                }
                final Runnable beforeSlotRelease = testBeforeSlotRelease;
                if (beforeSlotRelease != null) {
                    beforeSlotRelease.run();
                }
                locks.set(lockIndex, 0);
                if (!slotWaitQueue.hasWaiters() || !locks.compareAndSet(lockIndex, 0, 1)) {
                    return;
                }
            }
        }
    }

    /**
     * Installs the latch a worker counts down once it has taken a slot, or removes it when given
     * null. A leak test needs it because a slot that was taken and returned and a slot that was
     * never taken both report zero held slots; only the latch tells them apart.
     */
    @TestOnly
    public void setTestAcquireLatch(@Nullable CountDownLatch latch) {
        testAcquireLatch = latch;
    }

    @TestOnly
    public void setTestBeforeSlotRelease(@Nullable Runnable beforeSlotRelease) {
        testBeforeSlotRelease = beforeSlotRelease;
    }

    private int acquireSlot(
            int slotStart,
            ExecutionCircuitBreaker circuitBreaker,
            @Nullable SqlExecutionCircuitBreaker statefulCircuitBreaker
    ) {
        slotStart = normalizeSlotStart(slotStart);
        int slot = tryAcquireSlot(slotStart);
        if (slot > -1) {
            countDownTestAcquireLatch();
            return slot;
        }
        final SuspensionScope.Mode mode = SuspensionScope.getMode();
        if (mode == SuspensionScope.Mode.FIBER) {
            final SqlExecutionCircuitBreaker sqlCircuitBreaker =
                    circuitBreaker instanceof SqlExecutionCircuitBreaker sqlBreaker ? sqlBreaker : null;
            final int fiberSlot = awaitSlot(slotStart, sqlCircuitBreaker);
            if (fiberSlot > -1) {
                try {
                    // Cancellation can race with a grant; only the connection probe may be throttled here.
                    if (sqlCircuitBreaker != null) {
                        sqlCircuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    } else {
                        checkCircuitBreaker(circuitBreaker, statefulCircuitBreaker);
                    }
                } catch (Throwable th) {
                    releaseSlot(fiberSlot);
                    throw th;
                }
                countDownTestAcquireLatch();
                return fiberSlot;
            }
            if (fiberSlot == SLOT_WAIT_TIMER_REFUSED) {
                throw CairoException.nonCritical().put("query aborted, server is closing").setInterruption(true);
            }
            if (fiberSlot == SLOT_WAIT_ABORTED) {
                throw CairoException.nonCritical()
                        .put("reducer slot wait could not suspend the mounted fiber")
                        .setInterruption(true);
            }
            if (sqlCircuitBreaker != null) {
                sqlCircuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            }
            throw CairoException.nonCritical().put("query aborted").setInterruption(true);
        }
        while (true) {
            checkCircuitBreaker(circuitBreaker, statefulCircuitBreaker);
            Os.pause();
            slot = tryAcquireSlot(slotStart);
            if (slot > -1) {
                countDownTestAcquireLatch();
                return slot;
            }
        }
    }

    private int awaitSlot(int slotStart, @Nullable ExecutionCircuitBreaker circuitBreaker) {
        final Fiber fiber = Fiber.current();
        if (fiber == null || !Fiber.isMounted()) {
            throw CairoException.nonCritical().put("reducer slot wait requires a mounted fiber");
        }
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
        final TimerShards timerShards = SuspensionScope.getTimerShards(scope);
        FiberCancellationSignal cancellationSignal = SuspensionScope.getCancellationSignal(scope);
        long cancellationSignalGeneration = SuspensionScope.getCancellationSignalGeneration(scope);
        FiberCancellationSignal supplementalCancellationSignal = SuspensionScope.getSupplementalCancellationSignal(scope);
        final long supplementalCancellationSignalGeneration =
                SuspensionScope.getSupplementalCancellationSignalGeneration(scope);
        if (cancellationSignal == null && circuitBreaker instanceof SqlExecutionCircuitBreaker sqlCircuitBreaker) {
            final CancellationBinding cancellationBinding = SuspensionScope.getCancellationBindingScratch(scope);
            sqlCircuitBreaker.copyCancelledFlagTo(cancellationBinding);
            final AtomicBoolean cancelledFlag = cancellationBinding.getFlag();
            if (cancelledFlag instanceof FiberCancellationSignal signal) {
                cancellationSignal = signal;
                cancellationSignalGeneration = cancellationBinding.getGeneration(cancelledFlag);
            }
        }
        if (supplementalCancellationSignal == cancellationSignal) {
            supplementalCancellationSignal = null;
        }
        final int sourceCount = 1
                + (cancellationSignal != null ? 1 : 0)
                + (supplementalCancellationSignal != null ? 1 : 0)
                + (timerShards != null ? 1 : 0);
        while (true) {
            if (circuitBreaker != null && circuitBreaker.checkIfTripped()) {
                return -1;
            }
            final long token = fiber.tryBeginWaitBuild(sourceCount);
            if (token == Fiber.TOKEN_REFUSED) {
                return -1;
            }
            try {
                final FiberSlotWaitRegistration slotRegistration = coordinator.acquireSlot(token);
                if (slotRegistration.register(slotWaitQueue) != SourceRegistrationResult.ACCEPTED) {
                    throw new IllegalStateException("reducer slot wait registration failed");
                }
                if (cancellationSignal != null
                        && !coordinator.armCancellation(token, cancellationSignal, cancellationSignalGeneration)) {
                    throw new IllegalStateException("reducer cancellation registration failed");
                }
                if (supplementalCancellationSignal != null
                        && !coordinator.armCancellation(
                        token,
                        supplementalCancellationSignal,
                        supplementalCancellationSignalGeneration
                )) {
                    throw new IllegalStateException("reducer supplemental cancellation registration failed");
                }
                if (timerShards != null
                        && !coordinator.armTimer(token, timerShards, timerClock, timerIntervalMillis)) {
                    return SLOT_WAIT_TIMER_REFUSED;
                }

                final int slot = tryAcquireSlot(slotStart);
                if (slot > -1) {
                    return slot;
                }

                final int reason = fiber.suspendWait(token, SLOT_WAIT_ABORTED);
                if (reason == FiberWaitCoordinator.REASON_SLOT) {
                    return slotRegistration.takeSlot();
                }
                if (reason == SLOT_WAIT_ABORTED) {
                    return SLOT_WAIT_ABORTED;
                }
                if (reason != FiberWaitCoordinator.REASON_TIMER) {
                    return -1;
                }
            } finally {
                coordinator.teardownWait(token);
            }
        }
    }

    private void checkCircuitBreaker(
            ExecutionCircuitBreaker circuitBreaker,
            @Nullable SqlExecutionCircuitBreaker statefulCircuitBreaker
    ) {
        if (statefulCircuitBreaker != null) {
            statefulCircuitBreaker.statefulThrowExceptionIfTripped();
        } else if (circuitBreaker.checkIfTripped()) {
            throw CairoException.nonCritical().put("query aborted").setInterruption(true);
        }
    }

    private void countDownTestAcquireLatch() {
        final CountDownLatch latch = testAcquireLatch;
        if (latch != null) {
            latch.countDown();
        }
    }

    private int normalizeSlotStart(int workerId) {
        return workerId == -1
                ? rnd.nextInt(workerCount)
                : workerId >= workerCount ? workerId % workerCount : workerId;
    }

    private int tryAcquireSlot(int slotStart) {
        for (int i = 0; i < workerCount; i++) {
            int slot = i + slotStart;
            if (slot >= workerCount) {
                slot -= workerCount;
            }
            if (locks.compareAndSet(INTS_PER_SLOT * slot, 0, 1)) {
                return slot;
            }
        }
        return -1;
    }
}

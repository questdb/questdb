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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberEventWaitQueue;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.Os;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

abstract class AbstractPageFrameSequence {
    private static final int CANCEL_REASON_REDUCER_ERROR = -2;
    private static final int CANCEL_REASON_UNSET = -1;
    private final AtomicInteger cancelReason = new AtomicInteger(CANCEL_REASON_UNSET);
    private final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
    private final AtomicLong progressVersion = new AtomicLong();
    private final FiberEventWaitQueue progressWaitQueue =
            new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);

    public CairoException buildInterruptionException() {
        final int reason = getCancelReason();
        if (reason == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
            return CairoException.queryCancelled();
        }
        if (reason == SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION) {
            return CairoException.queryDisconnected(getCircuitBreaker().getFd());
        }
        return CairoException.queryTimedOut();
    }

    public void cancel(int reason) {
        cancelIfChanged(reason);
    }

    boolean cancelIfChanged(int reason) {
        while (true) {
            final int current = cancelReason.get();
            if (!isCancelReasonTransitionAllowed(current, reason)) {
                return false;
            }
            if (cancelReason.compareAndSet(current, reason)) {
                if (reason != SqlExecutionCircuitBreaker.STATE_OK) {
                    cancellationSignal.cancel();
                }
                return true;
            }
        }
    }

    void enterReducerCancellationScope() {
        final CancellationBinding cancellationBinding = SuspensionScope.getCancellationBindingScratch();
        getCircuitBreaker().copyCancelledFlagTo(cancellationBinding);
        final AtomicBoolean cancelledFlag = cancellationBinding.getFlag();
        final FiberCancellationSignal supplementalCancellationSignal;
        final long supplementalCancellationSignalGeneration;
        if (cancelledFlag instanceof FiberCancellationSignal signal && signal != cancellationSignal) {
            supplementalCancellationSignal = signal;
            supplementalCancellationSignalGeneration = cancellationBinding.getGeneration(cancelledFlag);
        } else {
            supplementalCancellationSignal = null;
            supplementalCancellationSignalGeneration = CancellationBinding.NO_GENERATION;
        }
        SuspensionScope.enterCancellationSignal(cancellationSignal);
        SuspensionScope.enterSupplementalCancellationSignal(
                supplementalCancellationSignal,
                supplementalCancellationSignalGeneration
        );
    }

    public int getCancelReason() {
        final int reason = cancelReason.get();
        return reason == CANCEL_REASON_UNSET || reason == CANCEL_REASON_REDUCER_ERROR
                ? SqlExecutionCircuitBreaker.STATE_OK
                : reason;
    }

    public FiberCancellationSignal getCancellationSignal() {
        return cancellationSignal;
    }

    public abstract SqlExecutionCircuitBreaker getCircuitBreaker();

    public long getProgressVersion() {
        return progressVersion.get();
    }

    final boolean isReducerFailureReportable(Throwable th) {
        final int reason = cancelReason.get();
        return reason == CANCEL_REASON_UNSET
                || reason == SqlExecutionCircuitBreaker.STATE_OK
                || !(th instanceof CairoException e && e.isInterruption());
    }

    public boolean isActive() {
        return cancelReason.get() == CANCEL_REASON_UNSET;
    }

    public abstract boolean isUninterruptible();

    // Hoist out of collect/dispatch loops: invariant while the loop runs, and each evaluation
    // costs a carrier-identity lookup.
    protected static boolean isFiberSuspendable() {
        return SuspensionScope.isFiberMode() && Fiber.isMounted();
    }

    protected final void awaitProgress(
            PageFrameReduceDispatcher dispatcher,
            long observedSequenceProgress,
            long observedGlobalProgress,
            boolean isDraining
    ) {
        final boolean isInterruptible = !isDraining && !isUninterruptible();
        final SqlExecutionCircuitBreaker circuitBreaker = isInterruptible ? getCircuitBreaker() : null;
        FiberCancellationSignal cancellationSignal = isInterruptible
                ? SuspensionScope.getCancellationSignal()
                : null;
        long cancellationSignalGeneration = isInterruptible
                ? SuspensionScope.getCancellationSignalGeneration()
                : CancellationBinding.NO_GENERATION;
        FiberCancellationSignal supplementalCancellationSignal = isInterruptible
                ? SuspensionScope.getSupplementalCancellationSignal()
                : null;
        final long supplementalCancellationSignalGeneration = isInterruptible
                ? SuspensionScope.getSupplementalCancellationSignalGeneration()
                : CancellationBinding.NO_GENERATION;
        if (cancellationSignal == null && circuitBreaker != null) {
            final CancellationBinding cancellationBinding = SuspensionScope.getCancellationBindingScratch();
            circuitBreaker.copyCancelledFlagTo(cancellationBinding);
            final AtomicBoolean cancelledFlag = cancellationBinding.getFlag();
            if (cancelledFlag instanceof FiberCancellationSignal signal) {
                cancellationSignal = signal;
                cancellationSignalGeneration = cancellationBinding.getGeneration(cancelledFlag);
            }
        }
        if (supplementalCancellationSignal == cancellationSignal) {
            supplementalCancellationSignal = null;
        }
        final boolean isProgressWaitTerminated;
        try {
            isProgressWaitTerminated = dispatcher.isProgressWaitTerminated(
                    this,
                    observedSequenceProgress,
                    observedGlobalProgress,
                    cancellationSignal,
                    cancellationSignalGeneration,
                    supplementalCancellationSignal,
                    supplementalCancellationSignalGeneration,
                    circuitBreaker,
                    isDraining
            );
        } catch (CairoException e) {
            if (isInterruptionSuperseded(e)) {
                Os.pause();
                return;
            }
            throw e;
        }
        if (isProgressWaitTerminated) {
            if (!isDraining && dispatcher.isQuiescing()) {
                if (!hasNonInterruptionWon(SqlExecutionCircuitBreaker.STATE_CANCELLED)) {
                    throw buildInterruptionException();
                }
                Os.pause();
                return;
            }
            if (!isDraining && !hasNonInterruptionWon(SqlExecutionCircuitBreaker.STATE_CANCELLED)) {
                throw buildInterruptionException();
            }
            Os.pause();
        }
    }

    protected final boolean isInterruptionSuperseded(CairoException exception) {
        final int interruptionReason = exception.getInterruptionReason();
        if (interruptionReason == SqlExecutionCircuitBreaker.STATE_OK) {
            return false;
        }
        if (hasNonInterruptionWon(interruptionReason)) {
            return true;
        }
        if (getCancelReason() != interruptionReason) {
            throw buildInterruptionException();
        }
        return false;
    }

    protected final void resetCancellation() {
        cancellationSignal.reopen();
        cancelReason.set(CANCEL_REASON_UNSET);
    }

    final void cancelOnReducerError(Throwable th) {
        final int interruptionReason = th instanceof CairoException e
                ? e.getInterruptionReason()
                : SqlExecutionCircuitBreaker.STATE_OK;
        cancel(interruptionReason == SqlExecutionCircuitBreaker.STATE_OK
                ? CANCEL_REASON_REDUCER_ERROR
                : interruptionReason);
    }

    FiberEventWaitQueue getProgressWaitQueue() {
        return progressWaitQueue;
    }

    void signalProgress() {
        progressVersion.incrementAndGet();
        progressWaitQueue.fire();
    }

    private boolean hasNonInterruptionWon(int interruptionReason) {
        cancel(interruptionReason);
        final int reason = cancelReason.get();
        return reason == SqlExecutionCircuitBreaker.STATE_OK || reason == CANCEL_REASON_REDUCER_ERROR;
    }

    private static boolean isCancelReasonTransitionAllowed(int current, int next) {
        return current == CANCEL_REASON_UNSET
                || current == SqlExecutionCircuitBreaker.STATE_OK
                && next != SqlExecutionCircuitBreaker.STATE_OK
                && next != CANCEL_REASON_REDUCER_ERROR;
    }
}

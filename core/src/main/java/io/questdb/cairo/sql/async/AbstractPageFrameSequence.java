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
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.Os;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

abstract class AbstractPageFrameSequence {
    private static final int CANCEL_REASON_UNSET = -1;
    private final AtomicInteger cancelReason = new AtomicInteger(CANCEL_REASON_UNSET);
    private final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();

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
        if (cancelReason.compareAndSet(CANCEL_REASON_UNSET, reason)) {
            cancellationSignal.cancel();
        }
    }

    public int getCancelReason() {
        final int reason = cancelReason.get();
        return reason == CANCEL_REASON_UNSET ? SqlExecutionCircuitBreaker.STATE_OK : reason;
    }

    public FiberCancellationSignal getCancellationSignal() {
        return cancellationSignal;
    }

    public abstract SqlExecutionCircuitBreaker getCircuitBreaker();

    public boolean isActive() {
        return cancelReason.get() == CANCEL_REASON_UNSET;
    }

    public abstract boolean isUninterruptible();

    private boolean hasNonInterruptionWon(int interruptionReason) {
        cancel(interruptionReason);
        return cancelReason.get() == SqlExecutionCircuitBreaker.STATE_OK;
    }

    // Hoist out of collect/dispatch loops: invariant while the loop runs, and each evaluation
    // costs a carrier-identity lookup.
    protected static boolean isFiberSuspendable() {
        return SuspensionScope.isFiberMode() && Fiber.isMounted();
    }

    protected final void awaitProgress(
            PageFrameReduceDispatcher dispatcher,
            long observedProgress,
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
        if (cancellationSignal == null && circuitBreaker != null) {
            final CancellationBinding cancellationBinding = SuspensionScope.getCancellationBindingScratch();
            circuitBreaker.copyCancelledFlagTo(cancellationBinding);
            final AtomicBoolean cancelledFlag = cancellationBinding.getFlag();
            if (cancelledFlag instanceof FiberCancellationSignal signal) {
                cancellationSignal = signal;
                cancellationSignalGeneration = cancellationBinding.getGeneration(cancelledFlag);
            }
        }
        final boolean hasProgress;
        try {
            hasProgress = dispatcher.awaitProgress(
                    observedProgress,
                    cancellationSignal,
                    cancellationSignalGeneration,
                    circuitBreaker
            );
        } catch (CairoException e) {
            if (isInterruptionSuperseded(e)) {
                Os.pause();
                return;
            }
            throw e;
        }
        if (!hasProgress) {
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
}

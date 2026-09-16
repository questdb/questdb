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
import io.questdb.mp.MCSequence;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

abstract class AbstractQueryParallelFiberTask extends FiberTask implements QuietCloseable {
    private final CancellationBinding cancellationBinding = new CancellationBinding();
    private final QueryParallelFiberDispatcher dispatcher;
    private final FiberTaskPool<?> pool;
    private final TimerShards timerShards;
    private MCSequence batchSubSeq;
    private int batchWorkerId = -1;
    private AsyncQueryProgressState progressState;

    protected AbstractQueryParallelFiberTask(
            QueryParallelFiberDispatcher dispatcher,
            FiberTaskPool<?> pool,
            TimerShards timerShards
    ) {
        this.dispatcher = dispatcher;
        this.pool = pool;
        this.timerShards = timerShards;
    }

    final void abortBeforeLaunch() {
        Throwable failure = null;
        try {
            cancelOwner();
        } catch (Throwable th) {
            failure = th;
        }
        try {
            completeOwnership();
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        try {
            dispatcher.signalOwnerProgress(progressState);
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        try {
            recycle();
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    final void bindBatch(int workerId, MCSequence subSeq) {
        this.batchWorkerId = workerId;
        this.batchSubSeq = subSeq;
    }

    final void bindCancellation(SqlExecutionCircuitBreaker circuitBreaker) {
        circuitBreaker.copyCancelledFlagTo(cancellationBinding);
    }

    final void bindProgress(AsyncQueryProgressState progressState) {
        this.progressState = progressState;
    }

    @Override
    public void close() {
        clearBinding();
        clearBatchBinding();
        batchSubSeq = null;
        batchWorkerId = -1;
        cancellationBinding.clear();
        progressState = null;
    }

    @Override
    public final @Nullable FiberCancellationSignal getCancellationSignal() {
        final AtomicBoolean flag = cancellationBinding.getFlag();
        return flag instanceof FiberCancellationSignal signal ? signal : null;
    }

    @Override
    protected final long getCancellationSignalGeneration(FiberCancellationSignal cancellationSignal) {
        return cancellationBinding.getGeneration(cancellationSignal);
    }

    abstract boolean isBound();

    @Override
    protected final void onAbandoned() {
        cancelOwner();
    }

    @Override
    protected final void onDone() {
        try {
            completeOwnership();
        } finally {
            try {
                dispatcher.signalOwnerProgress(progressState);
            } finally {
                recycle();
            }
        }
    }

    @Override
    protected final void onError(Throwable th) {
        onTaskError(th);
    }

    @Override
    protected final boolean runStep() {
        SuspensionScope.enterTimerShards(timerShards);
        long batchWeight = boundEntryWeight();
        if (!runTask()) {
            return false;
        }
        final MCSequence subSeq = batchSubSeq;
        if (subSeq != null) {
            final int batchLimit = dispatcher.getBatchLimit();
            final long batchWeightBudget = dispatcher.getBatchRowBudget();
            for (int i = 1; i < batchLimit && batchWeight < batchWeightBudget; i++) {
                final long cursor = claimNext(subSeq);
                if (cursor < 0) {
                    break;
                }
                // onDone() signals only the last entry's owner
                signalOwnerProgress();
                rebind(batchWorkerId, subSeq, cursor);
                // entries of one batch can belong to different queries; the carrier scope's
                // signal must track the entry, not the mount
                enterBoundCancellationScope();
                batchWeight += boundEntryWeight();
                if (!runTask()) {
                    return false;
                }
            }
        }
        return true;
    }

    protected long boundEntryWeight() {
        return 0;
    }

    protected abstract void cancelOwner();

    protected abstract void clearBatchBinding();

    protected abstract void clearBinding();

    protected abstract void completeOwnership();

    protected abstract void onTaskError(Throwable th);

    protected abstract void rebind(int workerId, MCSequence subSeq, long cursor);

    protected abstract boolean runTask();

    protected final void signalOwnerProgress() {
        dispatcher.signalOwnerProgress(progressState);
    }

    protected final void signalQueueProgress() {
        dispatcher.signalQueueProgress();
    }

    private static Throwable addFailure(@Nullable Throwable primary, Throwable failure) {
        if (primary == null) {
            return failure;
        }
        if (primary != failure) {
            primary.addSuppressed(failure);
        }
        return primary;
    }

    private static long claimNext(MCSequence subSeq) {
        while (true) {
            final long next = subSeq.next();
            if (next != -2) {
                return next;
            }
            Os.pause();
        }
    }

    private void enterBoundCancellationScope() {
        final AtomicBoolean cancelledFlag = cancellationBinding.getFlag();
        if (cancelledFlag instanceof FiberCancellationSignal signal) {
            SuspensionScope.enterCancellationSignal(signal, cancellationBinding.getGeneration(cancelledFlag));
        } else {
            SuspensionScope.enterCancellationSignal(null, CancellationBinding.NO_GENERATION);
        }
        SuspensionScope.enterSupplementalCancellationSignal(null, CancellationBinding.NO_GENERATION);
    }

    private void recycle() {
        clearBinding();
        clearBatchBinding();
        batchSubSeq = null;
        batchWorkerId = -1;
        cancellationBinding.clear();
        progressState = null;
        try {
            tryReopen();
        } finally {
            pool.releaseSelf(this);
        }
    }
}

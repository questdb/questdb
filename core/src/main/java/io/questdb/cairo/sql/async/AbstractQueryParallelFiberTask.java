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
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberDispatchContext;
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
    private @Nullable FiberDispatchContext batchDispatchContext;
    private long batchDispatchOwnerId;
    private Fiber batchFiber;
    private long batchMountVersion;
    private long batchStartNanos;
    private MCSequence batchSubSeq;
    private int batchWorkerId = -1;
    private FiberDispatchContext dispatchContext;
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

    final void bindCancellation(
            SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable FiberDispatchContext dispatchContext
    ) {
        circuitBreaker.copyCancelledFlagTo(cancellationBinding);
        this.dispatchContext = dispatchContext;
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
        dispatchContext = null;
        progressState = null;
    }

    @Nullable
    final FiberDispatchContext getDispatchContext() {
        return dispatchContext;
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
        batchDispatchContext = Fiber.captureDispatchContext();
        batchDispatchOwnerId = getQueryRegistryOwnerId(batchDispatchContext);
        batchFiber = Fiber.current();
        batchMountVersion = batchFiber.getMountVersion();
        batchStartNanos = System.nanoTime();
        if (!runTask()) {
            return false;
        }
        final MCSequence subSeq = batchSubSeq;
        if (subSeq != null) {
            while (continueBatch()) {
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
                switchDispatchContext(dispatchContext);
                if (!runTask()) {
                    return false;
                }
            }
        }
        return true;
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

    private static long getQueryRegistryOwnerId(@Nullable FiberDispatchContext context) {
        return context != null ? context.getQueryRegistryOwnerId() : -1;
    }

    private boolean continueBatch() {
        refreshBatchClock();
        return switch (dispatcher.checkBatch(batchStartNanos)) {
            case PageFrameReduceDispatcher.BATCH_CONTINUE -> true;
            case PageFrameReduceDispatcher.BATCH_YIELD -> {
                if (!Fiber.yieldCooperatively()) {
                    yield false;
                }
                batchMountVersion = batchFiber.getMountVersion();
                batchStartNanos = System.nanoTime();
                yield true;
            }
            default -> false;
        };
    }

    private void refreshBatchClock() {
        final long mountVersion = batchFiber.getMountVersion();
        if (mountVersion != batchMountVersion) {
            // time spent unmounted must not count against the batch
            batchMountVersion = mountVersion;
            batchStartNanos = System.nanoTime();
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
        dispatchContext = null;
        progressState = null;
        try {
            tryReopen();
        } finally {
            pool.releaseSelf(this);
        }
    }

    private void switchDispatchContext(@Nullable FiberDispatchContext nextContext) {
        // Query leases may pool and mutate a context object after its owner finishes. The owner ID
        // snapshot prevents reference-identity ABA from running a later query on the previous grant.
        final long nextOwnerId = getQueryRegistryOwnerId(nextContext);
        if (batchDispatchContext != nextContext || batchDispatchOwnerId != nextOwnerId) {
            if (!Fiber.yieldForDispatch(nextContext)) {
                throw new IllegalStateException("query parallel reducer could not switch dispatch context");
            }
            refreshBatchClock();
        }
        batchDispatchContext = nextContext;
        batchDispatchOwnerId = nextOwnerId;
    }
}

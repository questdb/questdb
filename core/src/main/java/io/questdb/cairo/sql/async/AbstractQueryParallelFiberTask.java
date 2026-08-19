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
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

abstract class AbstractQueryParallelFiberTask extends FiberTask implements QuietCloseable {
    AbstractQueryParallelFiberTask nextFree;
    boolean pooled;
    private final CancellationBinding cancellationBinding = new CancellationBinding();
    private final QueryParallelFiberDispatcher dispatcher;
    private final QueryParallelFiberTaskPool<?> pool;
    private final TimerShards timerShards;

    protected AbstractQueryParallelFiberTask(
            QueryParallelFiberDispatcher dispatcher,
            QueryParallelFiberTaskPool<?> pool,
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
            dispatcher.signalProgress();
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

    final void bindCancellation(SqlExecutionCircuitBreaker circuitBreaker) {
        circuitBreaker.copyCancelledFlagTo(cancellationBinding);
    }

    @Override
    public void close() {
        clearBinding();
        cancellationBinding.clear();
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
                dispatcher.signalProgress();
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
        return runTask();
    }

    protected abstract void cancelOwner();

    protected abstract void clearBinding();

    protected abstract void completeOwnership();

    protected abstract void onTaskError(Throwable th);

    protected abstract boolean runTask();

    protected final void signalProgress() {
        dispatcher.signalProgress();
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

    private void recycle() {
        clearBinding();
        cancellationBinding.clear();
        try {
            tryReopen();
        } finally {
            pool.release(this);
        }
    }
}

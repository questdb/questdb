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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionSuspension;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

class WaitWalFunction extends BooleanFunction implements Function {
    private final Function seqTxnArg;
    private final CharSequence tableName;
    private SqlExecutionContext executionContext;
    private long seqTxn;
    private SeqTxnTracker seqTxnTracker;
    private TableToken tableToken;

    public WaitWalFunction(CharSequence tableName, @Nullable Function seqTxnArg) {
        this.tableName = tableName;
        this.seqTxnArg = seqTxnArg;
    }

    @Override
    public void close() {
        if (seqTxnArg != null) {
            seqTxnArg.close();
        }
    }

    @Override
    public void cursorClosed() {
        if (seqTxnArg != null) {
            seqTxnArg.cursorClosed();
        }
        super.cursorClosed();
    }

    @Override
    public boolean getBool(Record rec) {
        if (seqTxnTracker == null) {
            return true;
        }

        // Fast path: already caught up.
        if (seqTxnTracker.getWriterTxn() >= seqTxn) {
            return true;
        }

        final Fiber fiber = SqlExecutionSuspension.currentFiber();
        if (fiber != null && waitInFiber(fiber)) {
            return true;
        }

        // Legacy polling fallback.
        for (int i = 0; seqTxnTracker.getWriterTxn() < seqTxn; i++) {
            Os.sleep(1);
            executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottled();
            if (i % 1000 == 0) {
                throwIfTerminated();
            }
        }
        throwIfTerminated();
        return true;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (seqTxnArg != null) {
            seqTxnArg.init(symbolTableSource, executionContext);
        }
        TableToken tt = executionContext.getCairoEngine().verifyTableName(tableName);
        if (tt.isWal()) {
            seqTxnTracker = executionContext.getCairoEngine().getTableSequencerAPI().getTxnTracker(tt);
            // NULL argument behaves like the no-arg form: wait for the seqTxn observed at call time.
            long providedSeqTxn = seqTxnArg != null ? seqTxnArg.getLong(null) : Numbers.LONG_NULL;
            seqTxn = providedSeqTxn != Numbers.LONG_NULL ? providedSeqTxn : seqTxnTracker.getSeqTxn();
            tableToken = tt;
            this.executionContext = executionContext;
        } else {
            seqTxnTracker = null;
            tableToken = null;
            this.executionContext = null;
        }
        super.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isNonDeterministic() {
        // The result depends on live WAL apply progress, not just the arguments, so the
        // function must not be folded or admitted where non-deterministic functions are
        // forbidden (e.g. materialized-view definitions, where a parked wait would stall refresh).
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("wait_wal_table(").val(tableName);
        if (seqTxnArg != null) {
            sink.val(", ").val(seqTxnArg);
        }
        sink.val(')');
    }

    private CairoException abortedException() {
        return CairoException.nonCritical()
                .put("wait_wal_table aborted, connection closing [tableName=")
                .put(tableName)
                .put(']');
    }

    private void throwIfCancelled(
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration,
            @Nullable FiberCancellationSignal supplementalCancellationSignal,
            long supplementalCancellationSignalGeneration
    ) {
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
        if ((cancellationSignal != null && cancellationSignal.isCancelled(cancellationSignalGeneration))
                || (supplementalCancellationSignal != null
                && supplementalCancellationSignal.isCancelled(supplementalCancellationSignalGeneration))) {
            throw CairoException.queryCancelled();
        }
    }

    private void throwIfTerminated() {
        if (seqTxnTracker.isSuspended()) {
            throw CairoException.nonCritical().put("table is suspended [tableName=").put(tableName).put("]");
        }
        if (seqTxnTracker.isDropped()) {
            throw CairoException.tableDoesNotExist(tableToken.getTableName());
        }
    }

    private boolean waitInFiber(Fiber fiber) {
        final MillisecondClock clock = executionContext.getCairoEngine().getConfiguration().getMillisecondClock();
        final long wakeIntervalMillis = Math.max(
                1,
                executionContext.getCairoEngine().getConfiguration().getQueryContinuationWakeIntervalMillis()
        );
        final TimerShards timerShards = executionContext.getCairoEngine().getTimerShards();
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        FiberCancellationSignal cancellationSignal = SuspensionScope.getCancellationSignal();
        long cancellationSignalGeneration = SuspensionScope.getCancellationSignalGeneration();
        FiberCancellationSignal supplementalCancellationSignal = SuspensionScope.getSupplementalCancellationSignal();
        final long supplementalCancellationSignalGeneration =
                SuspensionScope.getSupplementalCancellationSignalGeneration();
        if (cancellationSignal == null) {
            final CancellationBinding cancellationBinding = SuspensionScope.getCancellationBindingScratch();
            executionContext.getCircuitBreaker().copyCancelledFlagTo(cancellationBinding);
            final AtomicBoolean cancelledFlag = cancellationBinding.getFlag();
            if (cancelledFlag instanceof FiberCancellationSignal signal) {
                cancellationSignal = signal;
                cancellationSignalGeneration = cancellationBinding.getGeneration(cancelledFlag);
            }
        }
        if (supplementalCancellationSignal == cancellationSignal) {
            supplementalCancellationSignal = null;
        }
        while (seqTxnTracker.getWriterTxn() < seqTxn) {
            throwIfCancelled(
                    cancellationSignal,
                    cancellationSignalGeneration,
                    supplementalCancellationSignal,
                    supplementalCancellationSignalGeneration
            );
            throwIfTerminated();

            final int sourceCount = 2
                    + (cancellationSignal != null ? 1 : 0)
                    + (supplementalCancellationSignal != null ? 1 : 0);
            long token = fiber.tryBeginWaitBuild(sourceCount);
            if (token == Fiber.TOKEN_REFUSED) {
                throw abortedException();
            }
            try {
                if (cancellationSignal != null
                        && !coordinator.armCancellation(
                        token,
                        cancellationSignal,
                        cancellationSignalGeneration
                )) {
                    throw abortedException();
                }
                if (supplementalCancellationSignal != null
                        && !coordinator.armCancellation(
                        token,
                        supplementalCancellationSignal,
                        supplementalCancellationSignalGeneration
                )) {
                    throw abortedException();
                }
                final FiberWalWaitRegistration walRegistration = coordinator.acquireWal(token, seqTxn);
                if (seqTxnTracker.registerWaiter(walRegistration) != SourceRegistrationResult.ACCEPTED
                        || !coordinator.armTimer(token, timerShards, clock, wakeIntervalMillis)) {
                    throw abortedException();
                }
                int reason = fiber.suspendWait(token, FiberWaitCoordinator.REASON_NONE);
                if (reason == FiberWaitCoordinator.REASON_NONE) {
                    return false;
                }
                if (reason == FiberWaitCoordinator.REASON_SHUTDOWN) {
                    throw abortedException();
                }
                if (reason == FiberWaitCoordinator.REASON_CANCEL) {
                    executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                    throw CairoException.queryCancelled();
                }
            } finally {
                coordinator.teardownWait(token);
            }
        }
        throwIfCancelled(
                cancellationSignal,
                cancellationSignalGeneration,
                supplementalCancellationSignal,
                supplementalCancellationSignalGeneration
        );
        throwIfTerminated();
        return true;
    }

}

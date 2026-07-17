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
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.mp.continuation.TxnWaiter;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import org.jetbrains.annotations.Nullable;

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

        // Continuation path: the worker thread is carrying a WorkerContinuation
        // (mounted in its outer driver), so we can suspend the stack and free
        // the carrier. Wakes on each cycle so the circuit breaker can probe
        // the fd (broken connection), the cancel flag, and the SQL timeout.
        // If the body is still healthy after the probe, we re-park; otherwise
        // the breaker throws and the wait ends. This guarantees the wait can
        // NEVER be unbounded: a dead client, an explicit cancel, or a timeout
        // always wins.
        //
        // Pooled across iterations: fireWaiters drops the waiter from the
        // tracker queue when it CASes the state to FIRED, so by the time we
        // resume from suspend the previous queue entry is gone and reset()
        // can safely flip state back to PENDING for the next park. Stale
        // shard entries from a prior cycle are harmless: expire() is purely
        // state-driven, never reads deadlineMillis.
        TxnWaiter waiter = new TxnWaiter(
                executionContext.getCairoEngine().getTimerShards(),
                executionContext.getCairoEngine().getConfiguration().getQueryContinuationWakeIntervalMillis(),
                seqTxn
        );
        boolean firstRegister = true;
        if (waiter.tryBindCurrent()) {
            try {
                while (seqTxnTracker.getWriterTxn() < seqTxn) {
                    // Owning context is closing: do not re-park. Throwing unwinds the
                    // body all the way to the continuation loop's tail suspend, which is
                    // what the close path needs in order to drive cont.run() to isDone().
                    if (waiter.isShuttingDown()) {
                        throw CairoException.nonCritical().put("wait_wal_table aborted, connection closing [tableName=").put(tableName).put("]");
                    }
                    // Probe before re-parking: detects timeout, cancellation, broken
                    // connection, shutdown.
                    executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                    throwIfTerminated();

                    waiter.reset();
                    if (firstRegister) {
                        seqTxnTracker.registerWaiter(waiter);
                        firstRegister = false;
                    }
                    if (!waiter.suspend()) {
                        // The JDK refused to yield because the carrier is pinned (a
                        // synchronized or native frame sits above this call). The body
                        // never unmounted, so this is the same carrier that registered
                        // the waiter.
                        break;
                    }
                    // Resumed: either the waiter fired (target met or table state
                    // changed) or the timer shard cancelled it at the deadline.
                    // Loop top re-checks writerTxn and probes the breaker.
                }
                if (seqTxnTracker.getWriterTxn() >= seqTxn) {
                    throwIfTerminated();
                    return true;
                }
                // else: yield was refused at least once; fall through to polling.
            } finally {
                // Hygiene: unconditionally mark the waiter CANCELLED so the next
                // fireWaiters() walk drops the holder immediately instead of waiting
                // up to waitIntervalMillis for the timer pop. This is the last touch
                // of the waiter -- the body is unwinding and never re-suspends -- so
                // the write is safe from any prior state: if a throw landed during the
                // iteration-1 PENDING window (before any racer fired) it is the
                // cancellation; on iteration 2+ a racer already FIRED us and the
                // overwrite is unobservable (the fired waiter was already dequeued).
                // The timer-shard heap entry is not touched either way -- it pops at
                // its deadline and observes CANCELLED / FIRED, so expire() short-circuits.
                waiter.cancel();
            }
        }

        // Legacy polling fallback: no continuation gateway, or yield refused.
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

    private void throwIfTerminated() {
        if (seqTxnTracker.isSuspended()) {
            throw CairoException.nonCritical().put("table is suspended [tableName=").put(tableName).put("]");
        }
        if (seqTxnTracker.isDropped()) {
            throw CairoException.tableDoesNotExist(tableToken.getTableName());
        }
    }
}

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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TxnWaiter;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.mp.WorkerContinuation;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;

public class WaitWalTableFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "wait_wal_table(s)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final CharSequence tableName = args.getQuick(0).getStrA(null);
        return new WaitWalFunction(tableName);
    }

    private static class WaitWalFunction extends BooleanFunction implements Function {
        // How long the parked continuation sleeps between connection-liveness probes.
        // Each cycle wakes the body, runs statefulThrowExceptionIfTrippedNoThrottle()
        // (tests the fd, the cancel flag, and the SQL timeout), and re-parks if the
        // wait should continue. 200 ms keeps disconnect/timeout detection latency
        // tight without burning CPU on the timeout sweep.
        private static final long WAKE_INTERVAL_MILLIS = 200L;
        private final CharSequence tableName;
        private SqlExecutionContext executionContext;
        private long seqTxn;
        private SeqTxnTracker seqTxnTracker;

        public WaitWalFunction(CharSequence tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean getBool(Record rec) {
            if (seqTxnTracker == null) {
                return true;
            }

            // Fast path: already caught up.
            if (seqTxnTracker.getWriterTxn() >= seqTxn) {
                throwIfSuspended();
                return true;
            }

            // Continuation path: the worker thread is carrying a WorkerContinuation
            // (mounted in its outer driver), so we can suspend the stack and free
            // the carrier. Wakes every WAKE_INTERVAL_MILLIS so the circuit breaker
            // can probe the fd (broken connection), the cancel flag, and the SQL
            // timeout. If the body is still healthy after the probe, we re-park;
            // otherwise the breaker throws and the wait ends. This guarantees the
            // wait can NEVER be unbounded: a dead client, an explicit cancel, or
            // a timeout always wins.
            WorkerContinuation cont = WorkerContinuation.current();
            if (cont != null && WorkerContinuation.isMounted()) {
                MillisecondClock clock = executionContext.getCairoEngine().getConfiguration().getMillisecondClock();
                while (seqTxnTracker.getWriterTxn() < seqTxn) {
                    // Owning context is closing: do not re-park. Throwing unwinds the
                    // body all the way to the continuation loop's tail suspend, which is
                    // what the close path needs in order to drive cont.run() to isDone().
                    if (cont.isShutdown()) {
                        throw CairoException.nonCritical().put("wait_wal_table aborted, connection closing [tableName=").put(tableName).put("]");
                    }
                    // Probe before re-parking: detects timeout, cancellation, broken
                    // connection. If tripped, this throws and the wait ends.
                    // Use NoThrottle: the wake interval (1 s) already paces these
                    // checks; the throttled variant skips most calls and would let
                    // a tripped breaker go undetected for many wake cycles.
                    executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                    throwIfSuspended();
                    long deadline = clock.getTicks() + WAKE_INTERVAL_MILLIS;
                    TxnWaiter waiter = new TxnWaiter(seqTxn, cont, deadline);
                    seqTxnTracker.registerWaiter(waiter);
                    if (!WorkerContinuation.suspend()) {
                        // The JDK refused to yield because the carrier is pinned (a
                        // synchronized or native frame sits above this call). The body
                        // never unmounted, so this is the same carrier that registered
                        // the waiter. Cancel the waiter so a concurrent fireWaiters does
                        // not schedule a phantom resume of this still-mounted
                        // continuation, then fall through to legacy polling.
                        waiter.tryCancel();
                        break;
                    }
                    // Resumed: either the waiter fired (target met or table state
                    // changed) or the WaiterTimeoutJob cancelled it after WAKE_INTERVAL.
                    // Loop top re-checks writerTxn and probes the breaker.
                }
                if (seqTxnTracker.getWriterTxn() >= seqTxn) {
                    throwIfSuspended();
                    return true;
                }
                // else: yield was refused at least once; fall through to polling.
            }

            // Legacy polling fallback: no continuation gateway, or yield refused.
            for (int i = 0; seqTxnTracker.getWriterTxn() < seqTxn; i++) {
                Os.sleep(1);
                executionContext.getCircuitBreaker().statefulThrowExceptionIfTripped();
                if (i % 1000 == 0 && seqTxnTracker.isSuspended()) {
                    throwIfSuspended();
                }
            }
            throwIfSuspended();
            return true;
        }

        private void throwIfSuspended() {
            if (seqTxnTracker.isSuspended()) {
                throw CairoException.nonCritical().put("table is suspended [tableName=").put(tableName).put("]");
            }
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TableToken tt = executionContext.getCairoEngine().verifyTableName(tableName);
            if (tt.isWal()) {
                seqTxnTracker = executionContext.getCairoEngine().getTableSequencerAPI().getTxnTracker(tt);
                seqTxn = seqTxnTracker.getSeqTxn();
                this.executionContext = executionContext;
            } else {
                seqTxnTracker = null;
                this.executionContext = null;
            }
            super.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("wait_wal_table(").val(tableName).val(')');
        }
    }
}

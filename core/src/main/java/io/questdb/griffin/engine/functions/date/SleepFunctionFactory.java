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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.mp.TimerCont;
import io.questdb.mp.TimerShards;
import io.questdb.mp.WorkerContinuation;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;

/**
 * {@code sleep(seconds)} parks the current SQL evaluation for {@code seconds} of
 * wall-clock time and returns the server's current timestamp on wake. Built on
 * {@link TimerCont}: the worker carrier is freed for the duration of the sleep
 * instead of being burned in {@link Os#sleep(long)}. Falls back to {@code Os.sleep}
 * if no continuation is mounted.
 *
 * <p>The sleep is paced by {@code griffin.query.continuation.wake.interval}: the body
 * wakes on each tick to probe the SQL circuit breaker (timeout, cancel, broken
 * connection) and then re-arms a fresh timer entry until total elapsed time reaches
 * the requested duration. Engine close drains the timer shards, the body observes
 * {@code isShuttingDown()}, and unwinds.
 */
public class SleepFunctionFactory implements FunctionFactory {
    private static final String SIGNATURE = "sleep(D)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.getQuick(0), argPositions.getQuick(0));
    }

    private static class Func extends TimestampFunction implements UnaryFunction {
        private final Function arg;
        private final int argPosition;
        private MillisecondClock clock;
        private SqlExecutionContext executionContext;
        private long wakeIntervalMillis;

        public Func(Function arg, int argPosition) {
            super(io.questdb.cairo.ColumnType.TIMESTAMP_MICRO);
            this.arg = arg;
            this.argPosition = argPosition;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getTimestamp(Record rec) {
            final double seconds = arg.getDouble(rec);
            if (Double.isNaN(seconds) || Double.isInfinite(seconds) || seconds < 0) {
                throw CairoException.nonCritical().position(argPosition)
                        .put("sleep duration must be a finite non-negative number of seconds [value=").put(seconds).put(']');
            }
            final long sleepMillis = (long) (seconds * 1_000d);
            if (sleepMillis <= 0) {
                return executionContext.getMicrosecondTimestamp();
            }

            final long deadline = clock.getTicks() + sleepMillis;
            final TimerShards shards = executionContext.getCairoEngine().getTimerShards();

            // Continuation path: park the carrier through the timer shard. Wakes every
            // wakeIntervalMillis to probe the circuit breaker; re-arms a fresh entry
            // until the total deadline is reached or the breaker trips.
            if (WorkerContinuation.isMounted()) {
                while (true) {
                    long now = clock.getTicks();
                    long remaining = deadline - now;
                    if (remaining <= 0) {
                        return executionContext.getMicrosecondTimestamp();
                    }
                    executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                    long chunk = Math.min(remaining, wakeIntervalMillis);
                    TimerCont t = TimerCont.scheduleAfter(shards, chunk);
                    if (!WorkerContinuation.suspend()) {
                        // Carrier pinned by a synchronized or native frame: cancel the
                        // pending timer entry and fall back to the polling loop below.
                        t.abortContinuation();
                        break;
                    }
                    if (t.isShuttingDown()) {
                        throw CairoException.nonCritical().put("sleep aborted, connection closing");
                    }
                }
            }

            // Legacy polling fallback: no continuation gateway, or yield refused.
            while (clock.getTicks() < deadline) {
                executionContext.getCircuitBreaker().statefulThrowExceptionIfTripped();
                Os.sleep(1);
            }
            return executionContext.getMicrosecondTimestamp();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            this.executionContext = executionContext;
            CairoConfiguration configuration = executionContext.getCairoEngine().getConfiguration();
            this.clock = configuration.getMillisecondClock();
            this.wakeIntervalMillis = Math.max(1, configuration.getQueryContinuationWakeIntervalMillis());
        }

        @Override
        public boolean isConstant() {
            // sleep has a side effect (parking the carrier) and returns a different
            // timestamp on every call. Must NOT be constant-folded by the compiler
            // even when the argument is a literal.
            return false;
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public boolean isRuntimeConstant() {
            return false;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("sleep(").val(arg).val(')');
        }
    }
}

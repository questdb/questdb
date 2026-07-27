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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.mp.continuation.TimerCont;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.mp.continuation.WorkerContinuation;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;

/**
 * {@code sleep(seconds)} is a table function that parks the current SQL
 * evaluation for {@code seconds} of wall-clock time and emits a single row
 * containing the server's current timestamp on wake. The cursor form lets
 * callers run {@code sleep(1)} as a top-level query, instead of wrapping it
 * in {@code SELECT sleep(1)}.
 *
 * <p>Implementation is built on {@link TimerCont}: the worker carrier is
 * freed for the duration of the sleep instead of being burned in
 * {@link Os#sleep(long)}. Falls back to {@code Os.sleep} if no continuation
 * is mounted.
 *
 * <p>The sleep is paced by {@code griffin.query.continuation.wake.interval}:
 * the body wakes on each tick to probe the SQL circuit breaker (timeout,
 * cancel, broken connection) and then re-arms a fresh timer entry until total
 * elapsed time reaches the requested duration. Engine close drains the timer
 * shards, the body observes {@code isShuttingDown()}, and unwinds.
 */
public class SleepFunctionFactory implements FunctionFactory {
    private static final long MAX_SLEEP_MILLIS = 24L * 60 * 60 * 1_000;
    private static final RecordMetadata METADATA;
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
        return new CursorFunction(new SleepRecordCursorFactory(args.getQuick(0), argPositions.getQuick(0)));
    }

    private static class SleepRecord implements Record {
        private long timestamp;

        @Override
        public long getTimestamp(int col) {
            assert col == 0;
            return timestamp;
        }
    }

    private static class SleepRecordCursor implements NoRandomAccessRecordCursor {
        private final SleepRecord record = new SleepRecord();
        private boolean hasRow;

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (hasRow) {
                hasRow = false;
                return true;
            }
            return false;
        }

        public void of(long wakeTimestamp) {
            record.timestamp = wakeTimestamp;
            hasRow = true;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            hasRow = true;
        }
    }

    private static class SleepRecordCursorFactory extends AbstractRecordCursorFactory {
        private final Function arg;
        private final int argPosition;
        private final SleepRecordCursor cursor = new SleepRecordCursor();

        public SleepRecordCursorFactory(Function arg, int argPosition) {
            super(METADATA);
            this.arg = arg;
            this.argPosition = argPosition;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            arg.init(null, executionContext);
            final double seconds = arg.getDouble(null);
            if (Double.isNaN(seconds) || Double.isInfinite(seconds) || seconds < 0) {
                throw CairoException.nonCritical().position(argPosition)
                        .put("sleep duration must be a finite non-negative number of seconds [value=").put(seconds).put(']');
            }
            final double millisD = seconds * 1_000d;
            if (millisD > MAX_SLEEP_MILLIS) {
                throw CairoException.nonCritical().position(argPosition)
                        .put("sleep duration exceeds 24 hour maximum [value=").put(seconds).put(']');
            }
            final long sleepMillis = (long) millisD;
            if (sleepMillis > 0) {
                sleep(executionContext, sleepMillis);
            }
            cursor.of(executionContext.getMicrosecondTimestamp());
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("sleep(").val(arg).val(')');
        }

        @Override
        protected void _close() {
            Misc.free(arg);
        }

        private static void sleep(SqlExecutionContext executionContext, long sleepMillis) {
            final CairoConfiguration configuration = executionContext.getCairoEngine().getConfiguration();
            final MillisecondClock clock = configuration.getMillisecondClock();
            final long wakeIntervalMillis = Math.max(1, configuration.getQueryContinuationWakeIntervalMillis());
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
                        return;
                    }
                    executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                    long chunk = Math.min(remaining, wakeIntervalMillis);
                    TimerCont t = TimerCont.scheduleAfter(shards, clock, chunk);
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
                executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottled();
                Os.sleep(1);
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("sleep", ColumnType.TIMESTAMP_MICRO));
        METADATA = metadata;
    }
}

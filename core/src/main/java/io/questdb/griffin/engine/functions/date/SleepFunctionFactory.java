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
import io.questdb.griffin.SqlExecutionSuspension;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@code sleep(seconds)} is a table function that parks the current SQL
 * evaluation for {@code seconds} of wall-clock time and emits a single row
 * containing the server's current timestamp on wake. The cursor form lets
 * callers run {@code sleep(1)} as a top-level query, instead of wrapping it
 * in {@code SELECT sleep(1)}.
 *
 * <p>A Fiber uses its token-qualified wait coordinator. Synchronous callers
 * use {@link Os#sleep(long)}.
 *
 * <p>The sleep is paced by {@code griffin.query.continuation.wake.interval}:
 * the body wakes on each tick to probe timeout and connection state, while
 * query cancellation signals the wait directly. The body re-arms a fresh
 * timer entry until total elapsed time reaches the requested duration.
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

            final Fiber fiber = SqlExecutionSuspension.currentFiber();
            if (fiber != null) {
                final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
                FiberCancellationSignal cancellationSignal = SuspensionScope.getCancellationSignal();
                long cancellationSignalGeneration = SuspensionScope.getCancellationSignalGeneration();
                FiberCancellationSignal supplementalCancellationSignal =
                        SuspensionScope.getSupplementalCancellationSignal();
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
                while (true) {
                    throwIfCancelled(
                            executionContext,
                            cancellationSignal,
                            cancellationSignalGeneration,
                            supplementalCancellationSignal,
                            supplementalCancellationSignalGeneration
                    );
                    long now = clock.getTicks();
                    long remaining = deadline - now;
                    if (remaining <= 0) {
                        return;
                    }
                    long chunk = Math.min(remaining, wakeIntervalMillis);
                    final int sourceCount = 1
                            + (cancellationSignal != null ? 1 : 0)
                            + (supplementalCancellationSignal != null ? 1 : 0);
                    long token = fiber.tryBeginWaitBuild(sourceCount);
                    if (token == Fiber.TOKEN_REFUSED) {
                        throw CairoException.nonCritical().put("sleep aborted, connection closing");
                    }
                    try {
                        if (cancellationSignal != null
                                && !coordinator.armCancellation(
                                token,
                                cancellationSignal,
                                cancellationSignalGeneration
                        )) {
                            throw CairoException.nonCritical().put("sleep aborted, connection closing");
                        }
                        if (supplementalCancellationSignal != null
                                && !coordinator.armCancellation(
                                token,
                                supplementalCancellationSignal,
                                supplementalCancellationSignalGeneration
                        )) {
                            throw CairoException.nonCritical().put("sleep aborted, connection closing");
                        }
                        if (!coordinator.armTimer(token, shards, clock, chunk)) {
                            throw CairoException.nonCritical().put("sleep aborted, connection closing");
                        }
                        int reason = fiber.suspendWait(token, FiberWaitCoordinator.REASON_NONE);
                        if (reason == FiberWaitCoordinator.REASON_NONE) {
                            break;
                        }
                        if (reason == FiberWaitCoordinator.REASON_SHUTDOWN) {
                            throw CairoException.nonCritical().put("sleep aborted, connection closing");
                        }
                        if (reason == FiberWaitCoordinator.REASON_CANCEL) {
                            executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                            throw CairoException.queryCancelled();
                        }
                    } finally {
                        coordinator.teardownWait(token);
                    }
                }
            }

            // Legacy polling fallback.
            while (clock.getTicks() < deadline) {
                executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottled();
                Os.sleep(1);
            }
        }

        private static void throwIfCancelled(
                SqlExecutionContext executionContext,
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
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("sleep", ColumnType.TIMESTAMP_MICRO));
        METADATA = metadata;
    }
}

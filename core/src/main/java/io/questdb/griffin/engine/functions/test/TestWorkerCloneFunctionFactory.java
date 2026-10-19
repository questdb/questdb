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

package io.questdb.griffin.engine.functions.test;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dev-mode probe that makes worker-clone execution observable to concurrency tests. Outside dev
 * mode the factory folds to its boolean argument.
 */
public class TestWorkerCloneFunctionFactory implements FunctionFactory {
    private static final ThreadLocal<RunState> RUN_STATE = new ThreadLocal<>();

    public static void arm(double threshold) {
        if (RUN_STATE.get() != null) {
            throw new IllegalStateException("test_worker_clone run is already armed on this thread");
        }
        RUN_STATE.set(new RunState(threshold, Thread.currentThread().threadId()));
    }

    public static int created() {
        return state().created();
    }

    public static void disarm() {
        final RunState state = RUN_STATE.get();
        if (state != null) {
            state.releaseWorker();
            RUN_STATE.remove();
        }
    }

    public static int evaluations(int instanceId) {
        return state().evaluations(instanceId);
    }

    public static int mismatches() {
        return state().mismatches.get();
    }

    public static int workerEvaluations() {
        return state().workerEvaluations.get();
    }

    private static RunState state() {
        final RunState state = RUN_STATE.get();
        if (state == null) {
            throw new IllegalStateException("test_worker_clone run is not armed on this thread");
        }
        return state;
    }

    @Override
    public String getSignature() {
        return "test_worker_clone(TD)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (!configuration.isDevModeEnabled()) {
            final Function discarded = args.getQuick(1);
            args.setQuick(1, null);
            Misc.free(discarded);
            return args.getQuick(0);
        }
        final RunState state = RUN_STATE.get();
        if (state == null) {
            throw SqlException.$(position, "test_worker_clone must be armed before compilation");
        }
        return new Func(args.getQuick(0), args.getQuick(1), state.registerInstance(), state);
    }

    private static class Func extends BooleanFunction implements BinaryFunction {
        private final Function comparison;
        private final int instanceId;
        private final RunState state;
        private final Function value;

        private Func(Function comparison, Function value, int instanceId, RunState state) {
            this.comparison = comparison;
            this.value = value;
            this.instanceId = instanceId;
            this.state = state;
        }

        @Override
        public boolean getBool(Record rec) {
            final boolean isWorkerClone = Thread.currentThread().threadId() != state.ownerThreadId && instanceId > 0;
            state.incrementEvaluation(instanceId);
            if (isWorkerClone) {
                state.workerEvaluations.incrementAndGet();
                try {
                    final boolean result = comparison.getBool(rec);
                    if (result != (value.getDouble(rec) > state.expectedThreshold)) {
                        state.mismatches.incrementAndGet();
                    }
                    return result;
                } finally {
                    state.releaseWorker();
                }
            }

            final boolean result = comparison.getBool(rec);
            if (Thread.currentThread().threadId() == state.ownerThreadId) {
                try {
                    if (!state.workerReached.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("worker clone did not evaluate a frame");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while waiting for worker clone", e);
                }
            }
            return result;
        }

        @Override
        public Function getLeft() {
            return comparison;
        }

        @Override
        public String getName() {
            return "test_worker_clone";
        }

        @Override
        public Function getRight() {
            return value;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("test_worker_clone(").val(comparison).val(',').val(value).val(')');
        }
    }

    private static class RunState {
        private final ObjList<AtomicInteger> evaluations = new ObjList<>();
        private final double expectedThreshold;
        private final AtomicInteger mismatches = new AtomicInteger();
        private final long ownerThreadId;
        private final AtomicInteger workerEvaluations = new AtomicInteger();
        private final CountDownLatch workerReached = new CountDownLatch(1);

        private RunState(double expectedThreshold, long ownerThreadId) {
            this.expectedThreshold = expectedThreshold;
            this.ownerThreadId = ownerThreadId;
        }

        private synchronized int created() {
            return evaluations.size();
        }

        private synchronized int evaluations(int instanceId) {
            return evaluations.getQuick(instanceId).get();
        }

        private synchronized void incrementEvaluation(int instanceId) {
            evaluations.getQuick(instanceId).incrementAndGet();
        }

        private synchronized int registerInstance() {
            final int instanceId = evaluations.size();
            evaluations.add(new AtomicInteger());
            return instanceId;
        }

        private void releaseWorker() {
            workerReached.countDown();
        }
    }
}

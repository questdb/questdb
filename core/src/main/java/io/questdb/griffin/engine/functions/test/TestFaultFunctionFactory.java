/*******************************************************************************
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dev-mode test function {@code test_fault()} that returns true until armed,
 * then throws once on its Nth per-row evaluation. The query fuzzer emits it
 * into a generated query and arms it to verify that a factory frees its
 * resources when an expression throws mid-cursor, and that the query recovers
 * once the fault is removed. A separate compile-time arm
 * ({@link #armToFailAfterCompiles}) makes the Nth {@link #newInstance} call
 * throw instead, so tests can fail a specific per-worker clone compilation
 * mid-loop. An init-time arm ({@link #armToFailAfterInits}) makes the Nth
 * {@code Function.init} call throw, so tests can fail a specific clone list's
 * cursor-open initialization after the owner initialized. Outside dev mode it
 * folds to the BOOLEAN constant true, so it is inert in production.
 */
public class TestFaultFunctionFactory implements FunctionFactory {
    public static final String CALL = "test_fault()";
    private static final AtomicInteger CLOSE_CALLS = new AtomicInteger();
    private static final ObjList<Throwable> CLOSE_FAILURES = new ObjList<>();
    // -1 means disarmed. When armed to N, newInstance() succeeds on the first
    // N calls and throws on call N+1, then disarms itself.
    private static final AtomicInteger COMPILE_COUNTDOWN = new AtomicInteger(-1);
    // -1 means disarmed. When armed to N, the function returns true on the first
    // N getBool() calls and throws on call N+1, then disarms itself.
    private static final AtomicInteger COUNTDOWN = new AtomicInteger(-1);
    private static final AtomicInteger CREATED = new AtomicInteger();
    // -1 means disarmed. When armed to N, init() succeeds on the first N calls
    // and throws on call N+1, then disarms itself.
    private static final AtomicInteger INIT_COUNTDOWN = new AtomicInteger(-1);
    private static final AtomicInteger INIT_PROBE_EPOCH = new AtomicInteger();
    private static volatile boolean isCloseFailureArmed;
    private static final AtomicReference<Throwable> LAST_COMPILE_FAILURE = new AtomicReference<>();
    private static final AtomicInteger OFFER_COUNT = new AtomicInteger();
    private static final AtomicInteger TRIGGERED = new AtomicInteger();

    public static void armCloseFailures() {
        CLOSE_CALLS.set(0);
        synchronized (CLOSE_FAILURES) {
            CLOSE_FAILURES.clear();
        }
        CREATED.set(0);
        LAST_COMPILE_FAILURE.set(null);
        isCloseFailureArmed = true;
    }

    public static void armToFailAfter(int successfulCalls) {
        TRIGGERED.set(0);
        COUNTDOWN.set(successfulCalls);
    }

    public static void armToFailAfterCompiles(int successfulCompiles) {
        TRIGGERED.set(0);
        COMPILE_COUNTDOWN.set(successfulCompiles);
    }

    public static void armToFailAfterInits(int successfulInits) {
        INIT_PROBE_EPOCH.incrementAndGet();
        OFFER_COUNT.set(0);
        TRIGGERED.set(0);
        INIT_COUNTDOWN.set(successfulInits);
    }

    public static int closeCalls() {
        return CLOSE_CALLS.get();
    }

    public static int closeFailureCount() {
        synchronized (CLOSE_FAILURES) {
            return CLOSE_FAILURES.size();
        }
    }

    public static Throwable closeFailure(int index) {
        synchronized (CLOSE_FAILURES) {
            return CLOSE_FAILURES.getQuick(index);
        }
    }

    public static int created() {
        return CREATED.get();
    }

    public static void disarm() {
        COMPILE_COUNTDOWN.set(-1);
        COUNTDOWN.set(-1);
        INIT_COUNTDOWN.set(-1);
        isCloseFailureArmed = false;
    }

    public static int faultsTriggered() {
        return TRIGGERED.get();
    }

    public static Throwable lastCompileFailure() {
        return LAST_COMPILE_FAILURE.get();
    }

    // Counts donations from functions whose init succeeded under the current init-failure arm.
    public static int offersFromInitProbe() {
        return OFFER_COUNT.get();
    }

    @Override
    public String getSignature() {
        return CALL;
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
            return BooleanConstant.TRUE;
        }
        if (COMPILE_COUNTDOWN.get() >= 0 && COMPILE_COUNTDOWN.getAndDecrement() == 0) {
            TRIGGERED.incrementAndGet();
            final SqlException failure = SqlException.$(position, "test_fault: injected compile failure");
            LAST_COMPILE_FAILURE.set(failure);
            throw failure;
        }
        final int creationIndex = CREATED.getAndIncrement();
        final RuntimeException closeFailure;
        if (isCloseFailureArmed) {
            closeFailure = new RuntimeException("test_fault: injected close failure " + creationIndex);
            synchronized (CLOSE_FAILURES) {
                CLOSE_FAILURES.extendAndSet(creationIndex, closeFailure);
            }
        } else {
            closeFailure = null;
        }
        return new Func(closeFailure);
    }

    private static class Func extends BooleanFunction {
        private final RuntimeException closeFailure;
        private int initProbeEpoch = -1;

        private Func(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            CLOSE_CALLS.incrementAndGet();
            if (isCloseFailureArmed && closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public boolean getBool(Record rec) {
            if (COUNTDOWN.get() < 0) {
                return true; // disarmed
            }
            if (COUNTDOWN.getAndDecrement() == 0) {
                TRIGGERED.incrementAndGet();
                throw CairoException.nonCritical().put("test_fault: injected failure");
            }
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            if (INIT_COUNTDOWN.get() >= 0) {
                if (INIT_COUNTDOWN.getAndDecrement() == 0) {
                    TRIGGERED.incrementAndGet();
                    throw CairoException.nonCritical().put("test_fault: injected init failure");
                }
                initProbeEpoch = INIT_PROBE_EPOCH.get();
            }
        }

        @Override
        public void offerStateTo(Function that) {
            if (initProbeEpoch == INIT_PROBE_EPOCH.get()) {
                OFFER_COUNT.incrementAndGet();
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(CALL);
        }
    }
}

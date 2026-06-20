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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dev-mode test function {@code test_fault()} that returns true until armed,
 * then throws once on its Nth per-row evaluation. The query fuzzer emits it
 * into a generated query and arms it to verify that a factory frees its
 * resources when an expression throws mid-cursor, and that the query recovers
 * once the fault is removed. Outside dev mode it folds to the BOOLEAN constant
 * true, so it is inert in production.
 */
public class TestFaultFunctionFactory implements FunctionFactory {
    public static final String CALL = "test_fault()";
    // -1 means disarmed. When armed to N, the function returns true on the first
    // N getBool() calls and throws on call N+1, then disarms itself.
    private static final AtomicInteger COUNTDOWN = new AtomicInteger(-1);
    private static final AtomicInteger TRIGGERED = new AtomicInteger();

    public static void armToFailAfter(int successfulCalls) {
        TRIGGERED.set(0);
        COUNTDOWN.set(successfulCalls);
    }

    public static void disarm() {
        COUNTDOWN.set(-1);
    }

    public static int faultsTriggered() {
        return TRIGGERED.get();
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
    ) {
        if (!configuration.isDevModeEnabled()) {
            return BooleanConstant.TRUE;
        }
        return new Func();
    }

    private static class Func extends BooleanFunction {
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
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(CALL);
        }
    }
}

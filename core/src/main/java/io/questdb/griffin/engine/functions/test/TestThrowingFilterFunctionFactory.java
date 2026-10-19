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
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only boolean filter function whose {@code newInstance()} throws on a configurable
 * construction call number, after allocating native memory on every prior call. Used to
 * exercise resource-cleanup paths in per-worker filter compilation. Each successful
 * instance allocates a small native buffer freed in {@link Func#close()}, so a failure
 * to free constructed instances on the throwing-Nth-call path surfaces via
 * {@code assertMemoryLeak}.
 */
public class TestThrowingFilterFunctionFactory implements FunctionFactory {
    public static final AtomicInteger CLOSE_COUNT = new AtomicInteger();
    public static final AtomicInteger CONSTRUCT_COUNT = new AtomicInteger();
    public static final String SIGNATURE = "test_throwing_filter()";
    private static volatile int throwOnCall = -1;

    public static void reset(int throwOnCallNumber) {
        CONSTRUCT_COUNT.set(0);
        CLOSE_COUNT.set(0);
        throwOnCall = throwOnCallNumber;
    }

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
    ) throws SqlException {
        if (!configuration.isDevModeEnabled()) {
            return BooleanConstant.TRUE;
        }
        int n = CONSTRUCT_COUNT.incrementAndGet();
        if (n == throwOnCall) {
            throw SqlException.$(position, "test_throwing_filter: configured to throw on call ").put(n);
        }
        return new Func();
    }

    private static class Func extends BooleanFunction {
        private static final long ALLOC_SIZE = 64;
        private long addr;

        public Func() {
            this.addr = Unsafe.malloc(ALLOC_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public void close() {
            if (addr != 0) {
                addr = Unsafe.free(addr, ALLOC_SIZE, MemoryTag.NATIVE_DEFAULT);
                CLOSE_COUNT.incrementAndGet();
            }
        }

        @Override
        public boolean getBool(Record rec) {
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }
    }
}

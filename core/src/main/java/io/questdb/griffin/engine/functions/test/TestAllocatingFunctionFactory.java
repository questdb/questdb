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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * Always returns 42 long value. Allocates the given number of bytes in native memory
 * to simplify testing record factory / function leaks.
 */
public class TestAllocatingFunctionFactory implements FunctionFactory {
    private static final long MAX_BYTES = Numbers.SIZE_1MB;

    @Override
    public String getSignature() {
        return "alloc(l)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (configuration.isDevModeEnabled()) {
            long bytes = args.getQuick(0).getLong(null);
            if (bytes > MAX_BYTES) {
                throw SqlException.$(argPositions.getQuick(0), "too much to allocate: ").put(bytes);
            }
            return new Func(bytes);
        }
        return LongConstant.ZERO;
    }

    private static class Func extends LongFunction {
        private final long allocSize;
        private long addr;

        public Func(long allocSize) {
            this.addr = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
            this.allocSize = allocSize;
        }

        @Override
        public void close() {
            addr = Unsafe.free(addr, allocSize, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public long getLong(Record rec) {
            return 42;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("alloc(").val(allocSize).val(')');
        }
    }
}

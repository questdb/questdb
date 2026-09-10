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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * Returns its (constant) timestamp argument as a <b>runtime-constant</b> value while allocating a
 * small native buffer at construction time. This exists to test that code paths which compile a
 * runtime-constant timestamp bound (e.g. the {@code and_offset} interval pushdown residual) free the
 * compiled function on every exit - a plain bind-variable bound holds no native memory, so its leak is
 * invisible to {@code assertMemoryLeak}, whereas this function's buffer is tracked and surfaces the
 * leak. It is a test-only helper (only referenced from tests) that frees its buffer in {@link
 * Func#close()}, so a correctly-managed caller never leaks it.
 */
public class TestRuntimeConstAllocatingTimestampFunctionFactory implements FunctionFactory {
    private static final long ALLOC_BYTES = 1024;

    @Override
    public String getSignature() {
        return "alloc_ts(n)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function arg = args.getQuick(0);
        try {
            return new Func(arg.getTimestamp(null));
        } finally {
            // Func retains only the timestamp value, not the argument function. FunctionParser
            // transfers argument ownership to the returned function, so free the consumed argument
            // here to honor that contract. The signature requires a TIMESTAMP constant, so this frees
            // no native memory today, but it keeps this leak-testing helper correct if it is ever
            // handed a native-memory-owning argument. Null the slot so the parser's error-path
            // freeObjList cannot double-free it.
            args.setQuick(0, Misc.free(arg));
        }
    }

    private static class Func extends TimestampFunction {
        private final long value;
        private long addr;

        Func(long value) {
            super(ColumnType.TIMESTAMP);
            this.value = value;
            this.addr = Unsafe.malloc(ALLOC_BYTES, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public void close() {
            if (addr != 0) {
                addr = Unsafe.free(addr, ALLOC_BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        }

        @Override
        public long getTimestamp(Record rec) {
            return value;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("alloc_ts(").val(value).val(')');
        }
    }
}

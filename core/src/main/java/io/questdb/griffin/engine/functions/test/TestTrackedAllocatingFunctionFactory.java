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
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * Always returns 42. Allocates the given number of bytes in native memory through
 * the per-query {@link MemoryTracker} bound on the execution context, so the
 * allocation charges the active workload's memory limit. The tracker-aware
 * counterpart of {@code alloc(l)}, it lets a test drive a per-workload limit
 * breach from SQL even where no production allocator is reachable, e.g. inside
 * an UPDATE applied by a WAL apply batch, where the bound tracker is the
 * WAL_APPLY one. Dev-mode only.
 */
public class TestTrackedAllocatingFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "alloc_tracked(l)";
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
            return new Func(args.getQuick(0).getLong(null), sqlExecutionContext.getMemoryTracker());
        }
        return LongConstant.ZERO;
    }

    private static class Func extends LongFunction {
        private final long allocSize;
        private final MemoryTracker memoryTracker;
        private long addr;

        public Func(long allocSize, MemoryTracker memoryTracker) {
            this.allocSize = allocSize;
            this.memoryTracker = memoryTracker;
            // Charges the bound per-query tracker; throws CairoException(OOM) on breach,
            // leaving addr at 0 so close() is a no-op.
            this.addr = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT, memoryTracker);
        }

        @Override
        public void close() {
            if (addr != 0) {
                addr = Unsafe.free(addr, allocSize, MemoryTag.NATIVE_DEFAULT, memoryTracker);
            }
        }

        @Override
        public long getLong(Record rec) {
            return 42;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("alloc_tracked(").val(allocSize).val(')');
        }
    }
}

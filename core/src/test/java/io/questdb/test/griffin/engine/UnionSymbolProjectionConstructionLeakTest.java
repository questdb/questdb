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

package io.questdb.test.griffin.engine;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestThrowingFilterFunctionFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UnionSymbolProjectionConstructionLeakTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testConstructionFailureFreesFunctionsAndDistinctUnionMap() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (s SYMBOL)");
            execute("CREATE TABLE tb (s SYMBOL)");

            // Fail before allocating the projection, after its base function is owned by the
            // function list, and after the symbol wrapper has replaced the base function. The
            // branch functions allocate native memory, while the distinct UNION owns an OrderedMap,
            // so assertMemoryLeak also verifies the union factory is released on every path.
            assertConstructionFailure(SqlCodeGenerator.UnionSymbolProjectionTestHook.PROJECTION);
            assertConstructionFailure(SqlCodeGenerator.UnionSymbolProjectionTestHook.BASE_COLUMN);
            assertConstructionFailure(SqlCodeGenerator.UnionSymbolProjectionTestHook.SYMBOL_FUNCTION);
        });
    }

    private static void assertConstructionFailure(int failureKind) throws Exception {
        final ProjectionFailureHook hook = new ProjectionFailureHook(failureKind);
        TestThrowingFilterFunctionFactory.reset(-1);
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setUnionSymbolProjectionTestHook(hook);
            try (RecordCursorFactory ignored = select(
                    compiler,
                    "SELECT s, test_throwing_filter() f FROM ta UNION SELECT s, test_throwing_filter() f FROM tb",
                    sqlExecutionContext
            )) {
                Assert.fail("expected injected union symbol projection failure");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "injected union symbol projection failure");
            }
        }

        hook.assertAllConstructedFunctionsClosed();
        Assert.assertEquals(2, TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get());
        Assert.assertEquals(2, TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
    }

    private static class NativeAllocation {
        private static final long ALLOC_SIZE = 64;
        private final int[] closeCounts;
        private final int functionKind;
        private long address = Unsafe.malloc(ALLOC_SIZE, MemoryTag.NATIVE_DEFAULT);

        private NativeAllocation(int[] closeCounts, int functionKind) {
            this.closeCounts = closeCounts;
            this.functionKind = functionKind;
        }

        private void close() {
            if (address != 0) {
                address = Unsafe.free(address, ALLOC_SIZE, MemoryTag.NATIVE_DEFAULT);
                closeCounts[functionKind]++;
            }
        }
    }

    private static class ProjectionFailureHook implements SqlCodeGenerator.UnionSymbolProjectionTestHook {
        private final int[] closeCounts = new int[2];
        private final int[] constructCounts = new int[2];
        private final int failureKind;

        private ProjectionFailureHook(int failureKind) {
            this.failureKind = failureKind;
        }

        @Override
        public void onFunctionRegistered(int functionKind) throws SqlException {
            if (functionKind == failureKind) {
                throw SqlException.$(0, "injected union symbol projection failure");
            }
        }

        @Override
        public void onProjectionConstruction() throws SqlException {
            if (failureKind == PROJECTION) {
                throw SqlException.$(0, "injected union symbol projection failure");
            }
        }

        @Override
        public Function wrapFunction(Function function, int functionKind) {
            constructCounts[functionKind]++;
            return functionKind == BASE_COLUMN
                    ? new TrackingStrFunction(function, closeCounts, functionKind)
                    : new TrackingSymbolFunction(function, closeCounts, functionKind);
        }

        private void assertAllConstructedFunctionsClosed() {
            Assert.assertEquals(failureKind == PROJECTION ? 0 : 1, constructCounts[BASE_COLUMN]);
            Assert.assertEquals(failureKind == SYMBOL_FUNCTION ? 1 : 0, constructCounts[SYMBOL_FUNCTION]);
            Assert.assertArrayEquals(constructCounts, closeCounts);
        }
    }

    private static class TrackingStrFunction extends StrFunction {
        private final Function arg;
        private final NativeAllocation allocation;

        private TrackingStrFunction(Function arg, int[] closeCounts, int functionKind) {
            this.arg = arg;
            this.allocation = new NativeAllocation(closeCounts, functionKind);
        }

        @Override
        public void close() {
            allocation.close();
            arg.close();
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return arg.getStrA(rec);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return arg.getStrB(rec);
        }

        @Override
        public int getStrLen(Record rec) {
            return arg.getStrLen(rec);
        }
    }

    private static class TrackingSymbolFunction extends CastStrToSymbolFunctionFactory.Func {
        private final NativeAllocation allocation;

        private TrackingSymbolFunction(Function arg, int[] closeCounts, int functionKind) {
            super(arg);
            this.allocation = new NativeAllocation(closeCounts, functionKind);
        }

        @Override
        public void close() {
            try {
                allocation.close();
            } finally {
                super.close();
            }
        }
    }
}

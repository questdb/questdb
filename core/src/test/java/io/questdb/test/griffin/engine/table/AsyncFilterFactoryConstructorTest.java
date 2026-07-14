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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the ownership contract of the async filter factory constructors. A throw part-way through one
 * of them never returns the factory, so {@code _close()} never runs and everything the constructor
 * allocated up to that point - the cursors' native records, the JIT bind variable memory, and the
 * per-worker filters it was handed - is unreachable unless the constructor itself releases it.
 * SqlCodeGenerator frees only what it passed in (the compiled filters, the filter, the bind variable
 * functions and the base factory), which is what these tests do after the expected throw.
 * <p>
 * Each test injects the failure through a configuration getter that is read at exactly one point of
 * the construction, walking the fault down the three ownership hand-offs: before the atom exists
 * (nobody owns the per-worker filters yet), inside the atom's constructor (it has taken them, but the
 * frame sequence has not taken the atom), and inside the frame sequence's constructor (which closes
 * the atom on its own failure path, so the factory must not close it a second time). The per-worker
 * filters hold native memory and count their closes, so a leak fails the {@code assertMemoryLeak}
 * and a double free fails the close-count assertion.
 */
public class AsyncFilterFactoryConstructorTest extends AbstractCairoTest {

    @Test
    public void testFactoryFreesPartialStateOnAtomFailure() throws Exception {
        assertConstructorFailureIsClean(false, FaultPoint.ATOM);
    }

    @Test
    public void testFactoryFreesPartialStateOnFrameSequenceFailure() throws Exception {
        assertConstructorFailureIsClean(false, FaultPoint.FRAME_SEQUENCE);
    }

    @Test
    public void testFactoryReleasesEverythingOnClose() throws Exception {
        assertConstructorSuccessCloses(false);
    }

    @Test
    public void testJitFactoryFreesPartialStateOnAtomFailure() throws Exception {
        assertConstructorFailureIsClean(true, FaultPoint.ATOM);
    }

    @Test
    public void testJitFactoryFreesPartialStateOnBindVarMemoryFailure() throws Exception {
        assertConstructorFailureIsClean(true, FaultPoint.BIND_VAR_MEMORY);
    }

    @Test
    public void testJitFactoryFreesPartialStateOnFrameSequenceFailure() throws Exception {
        assertConstructorFailureIsClean(true, FaultPoint.FRAME_SEQUENCE);
    }

    @Test
    public void testJitFactoryReleasesEverythingOnClose() throws Exception {
        assertConstructorSuccessCloses(true);
    }

    private static void assertConstructorFailureIsClean(boolean isJit, FaultPoint faultPoint) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final CairoConfiguration configuration = new FaultInjectingConfiguration(engine.getConfiguration(), faultPoint);
            final RecordCursorFactory base = select("SELECT * FROM x");
            // Owned by the caller, exactly as in SqlCodeGenerator: the constructor must not close any
            // of these, on either the success or the failure path.
            final NativeFilter filter = new NativeFilter();
            final CompiledFilter compiledFilter = new CompiledFilter();
            final CompiledCountOnlyFilter compiledCountOnlyFilter = new CompiledCountOnlyFilter();
            final ObjList<Function> bindVarFunctions = new ObjList<>();
            // Handed over to the factory, which passes them to the atom.
            final NativeFilter perWorkerFilter = new NativeFilter();
            final ObjList<Function> perWorkerFilters = new ObjList<>();
            perWorkerFilters.add(perWorkerFilter);

            try {
                newFactory(isJit, configuration, base, filter, compiledFilter, compiledCountOnlyFilter, bindVarFunctions, perWorkerFilters);
                Assert.fail("expected the injected " + faultPoint + " failure to propagate");
            } catch (FaultInjectedException expected) {
                Assert.assertEquals(faultPoint, expected.faultPoint);
            }

            Assert.assertEquals("the per-worker filter must be closed exactly once", 1, perWorkerFilter.closeCount);
            Assert.assertEquals("the filter belongs to the caller and must be left open", 0, filter.closeCount);

            Misc.free(filter);
            Misc.free(compiledFilter);
            Misc.free(compiledCountOnlyFilter);
            Misc.freeObjList(bindVarFunctions);
            Misc.free(base);
        });
    }

    private static void assertConstructorSuccessCloses(boolean isJit) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final NativeFilter filter = new NativeFilter();
            final NativeFilter perWorkerFilter = new NativeFilter();
            final ObjList<Function> perWorkerFilters = new ObjList<>();
            perWorkerFilters.add(perWorkerFilter);

            final RecordCursorFactory factory = newFactory(
                    isJit,
                    engine.getConfiguration(),
                    select("SELECT * FROM x"),
                    filter,
                    new CompiledFilter(),
                    new CompiledCountOnlyFilter(),
                    new ObjList<>(),
                    perWorkerFilters
            );
            factory.close();

            // The factory owns everything once it is built, so it closes each of them exactly once.
            Assert.assertEquals(1, filter.closeCount);
            Assert.assertEquals(1, perWorkerFilter.closeCount);
        });
    }

    private static RecordCursorFactory newFactory(
            boolean isJit,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            Function filter,
            CompiledFilter compiledFilter,
            CompiledCountOnlyFilter compiledCountOnlyFilter,
            ObjList<Function> bindVarFunctions,
            ObjList<Function> perWorkerFilters
    ) {
        final ExpressionNode filterExpr = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.CONSTANT, "true", 0, 0);
        final PageFrameReduceTaskFactory reduceTaskFactory =
                () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_SQL_COMPILER);
        if (isJit) {
            return new AsyncJitFilteredRecordCursorFactory(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    base,
                    bindVarFunctions,
                    compiledFilter,
                    compiledCountOnlyFilter,
                    filter,
                    new IntHashSet(),
                    reduceTaskFactory,
                    perWorkerFilters,
                    filterExpr,
                    null,
                    0,
                    1,
                    false
            );
        }
        // The Java filter path takes neither the compiled filters nor the bind variable functions, so
        // they stay the caller's; free the ones this test built for the JIT signature.
        Misc.free(compiledFilter);
        Misc.free(compiledCountOnlyFilter);
        Misc.freeObjList(bindVarFunctions);
        return new AsyncFilteredRecordCursorFactory(
                engine,
                configuration,
                engine.getMessageBus(),
                base,
                filter,
                new IntHashSet(),
                reduceTaskFactory,
                perWorkerFilters,
                filterExpr,
                null,
                0,
                1,
                false
        );
    }

    private enum FaultPoint {
        // Read by the factory constructor to size the JIT bind variable memory, i.e. after the cursors
        // are built and before anything takes the per-worker filters over.
        BIND_VAR_MEMORY,
        // Read by the AsyncFilterAtom constructor, which has already taken the per-worker filters.
        ATOM,
        // Read by the PageFrameSequence constructor, which owns the atom by then and closes it on its
        // own failure path.
        FRAME_SEQUENCE
    }

    private static class FaultInjectedException extends RuntimeException {
        private final FaultPoint faultPoint;

        private FaultInjectedException(FaultPoint faultPoint) {
            super("injected failure at " + faultPoint);
            this.faultPoint = faultPoint;
        }
    }

    private static class FaultInjectingConfiguration extends CairoConfigurationWrapper {
        private final FaultPoint faultPoint;

        private FaultInjectingConfiguration(CairoConfiguration delegate, FaultPoint faultPoint) {
            super(delegate);
            this.faultPoint = faultPoint;
        }

        @Override
        public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
            if (faultPoint == FaultPoint.FRAME_SEQUENCE) {
                throw new FaultInjectedException(faultPoint);
            }
            return super.getCircuitBreakerConfiguration();
        }

        @Override
        public int getSqlJitBindVarsMemoryPageSize() {
            if (faultPoint == FaultPoint.BIND_VAR_MEMORY) {
                throw new FaultInjectedException(faultPoint);
            }
            return super.getSqlJitBindVarsMemoryPageSize();
        }

        @Override
        public double getSqlParallelFilterPreTouchThreshold() {
            if (faultPoint == FaultPoint.ATOM) {
                throw new FaultInjectedException(faultPoint);
            }
            return super.getSqlParallelFilterPreTouchThreshold();
        }
    }

    /**
     * A filter that holds native memory, so dropping it fails the leak check, and counts its closes,
     * so closing it twice fails an assertion instead of silently double-freeing.
     */
    private static class NativeFilter extends BooleanFunction {
        private static final long SIZE = 64;
        private int closeCount;
        private long ptr;

        private NativeFilter() {
            ptr = Unsafe.malloc(SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public void close() {
            closeCount++;
            if (ptr != 0) {
                ptr = Unsafe.free(ptr, SIZE, MemoryTag.NATIVE_DEFAULT);
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
            sink.val("NativeFilter");
        }
    }
}

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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.join.AsyncWindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.std.BytecodeAssembler;
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
 * Pins the ownership contract of the async WINDOW JOIN factory constructors. A throw part-way through
 * one of them never returns the factory, so {@code _close()} never runs and everything the constructor
 * handed to the atom - the owner/per-worker join filters and master filters - is unreachable unless the
 * constructor itself releases it. SqlCodeGenerator frees only what it owns before the ctor call (the
 * base factories and the join metadata), which is what these tests do after the expected throw.
 * <p>
 * Each test injects the failure at exactly one point of the construction, walking the fault down the
 * ownership hand-offs the ctor comment describes:
 * <ul>
 *   <li>ATOM: {@code getSqlAsOfJoinLookAhead()} throws inside the AsyncWindowJoin(Fast)Atom constructor,
 *       which has already adopted the filters (it closes itself on its own ctor failure);</li>
 *   <li>FRAME_SEQUENCE: {@code getCircuitBreakerConfiguration()} throws inside the PageFrameSequence
 *       constructor, which owns the atom by then and closes it on its own failure path (so the factory
 *       must not close it a second time);</li>
 *   <li>CURSOR: {@code getMetadata()} throws inside the AsyncWindowJoinRecordCursor constructor, which
 *       runs last, with a fully built frame sequence that only the factory ctor's catch can release.
 *       Cursor construction reads no configuration, so this one is injected through the slave factory
 *       instead - see {@link CountingSlaveFactory}.</li>
 * </ul>
 * The filters hold native memory and count their closes, so a leak fails {@code assertMemoryLeak} and a
 * double free fails the close-count assertion.
 */
public class AsyncWindowJoinFactoryConstructorTest extends AbstractCairoTest {

    @Test
    public void testFastFactoryFreesPartialStateOnAtomFailure() throws Exception {
        assertConstructorFailureIsClean(true, FaultPoint.ATOM);
    }

    @Test
    public void testFastFactoryFreesPartialStateOnCursorFailure() throws Exception {
        assertCursorFailureIsClean(true);
    }

    @Test
    public void testFastFactoryFreesPartialStateOnFrameSequenceFailure() throws Exception {
        assertConstructorFailureIsClean(true, FaultPoint.FRAME_SEQUENCE);
    }

    @Test
    public void testFastFactoryReleasesEverythingOnClose() throws Exception {
        assertConstructorSuccessCloses(true);
    }

    @Test
    public void testGeneralFactoryFreesPartialStateOnAtomFailure() throws Exception {
        assertConstructorFailureIsClean(false, FaultPoint.ATOM);
    }

    @Test
    public void testGeneralFactoryFreesPartialStateOnCursorFailure() throws Exception {
        assertCursorFailureIsClean(false);
    }

    @Test
    public void testGeneralFactoryFreesPartialStateOnFrameSequenceFailure() throws Exception {
        assertConstructorFailureIsClean(false, FaultPoint.FRAME_SEQUENCE);
    }

    @Test
    public void testGeneralFactoryReleasesEverythingOnClose() throws Exception {
        assertConstructorSuccessCloses(false);
    }

    private static void assertConstructorFailureIsClean(boolean fast, FaultPoint faultPoint) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final CairoConfiguration configuration = new FaultInjectingConfiguration(engine.getConfiguration(), faultPoint);
            // Base factories and the join metadata are built with the real config and stay the caller's
            // responsibility on a ctor throw, exactly as in SqlCodeGenerator.
            final RecordCursorFactory masterFactory = baseFactory("master");
            final RecordCursorFactory slaveFactory = baseFactory("slave");
            final JoinRecordMetadata joinMetadata = new JoinRecordMetadata(engine.getConfiguration(), 0);

            // Handed to the factory, which passes them to the atom; the atom must free each exactly once
            // on its ctor failure, and the frame sequence must free them (through the atom) on its.
            final NativeFilter joinFilter = new NativeFilter();
            final NativeFilter perWorkerJoinFilter = new NativeFilter();
            final ObjList<Function> perWorkerJoinFilters = new ObjList<>();
            perWorkerJoinFilters.add(perWorkerJoinFilter);
            final NativeFilter masterFilter = new NativeFilter();
            final NativeFilter perWorkerMasterFilter = new NativeFilter();
            final ObjList<Function> perWorkerMasterFilters = new ObjList<>();
            perWorkerMasterFilters.add(perWorkerMasterFilter);

            try {
                if (fast) {
                    newFastFactory(configuration, masterFactory, slaveFactory, joinMetadata,
                            joinFilter, perWorkerJoinFilters, masterFilter, perWorkerMasterFilters);
                } else {
                    newGeneralFactory(configuration, masterFactory, slaveFactory, joinMetadata,
                            joinFilter, perWorkerJoinFilters, masterFilter, perWorkerMasterFilters);
                }
                Assert.fail("expected the injected " + faultPoint + " failure to propagate");
            } catch (FaultInjectedException expected) {
                Assert.assertEquals(faultPoint, expected.faultPoint);
            }

            Assert.assertEquals("owner join filter must be closed exactly once", 1, joinFilter.closeCount);
            Assert.assertEquals("per-worker join filter must be closed exactly once", 1, perWorkerJoinFilter.closeCount);
            Assert.assertEquals("owner master filter must be closed exactly once", 1, masterFilter.closeCount);
            Assert.assertEquals("per-worker master filter must be closed exactly once", 1, perWorkerMasterFilter.closeCount);

            // The base factories and the join metadata belong to the caller on a ctor throw.
            Misc.free(masterFactory);
            Misc.free(slaveFactory);
            Misc.free(joinMetadata);
        });
    }

    private static void assertConstructorSuccessCloses(boolean fast) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final RecordCursorFactory masterFactory = baseFactory("master");
            final RecordCursorFactory slaveFactory = baseFactory("slave");
            final JoinRecordMetadata joinMetadata = new JoinRecordMetadata(engine.getConfiguration(), 0);

            final NativeFilter joinFilter = new NativeFilter();
            final NativeFilter perWorkerJoinFilter = new NativeFilter();
            final ObjList<Function> perWorkerJoinFilters = new ObjList<>();
            perWorkerJoinFilters.add(perWorkerJoinFilter);
            final NativeFilter masterFilter = new NativeFilter();
            final NativeFilter perWorkerMasterFilter = new NativeFilter();
            final ObjList<Function> perWorkerMasterFilters = new ObjList<>();
            perWorkerMasterFilters.add(perWorkerMasterFilter);

            final RecordCursorFactory factory = fast
                    ? newFastFactory(engine.getConfiguration(), masterFactory, slaveFactory, joinMetadata,
                    joinFilter, perWorkerJoinFilters, masterFilter, perWorkerMasterFilters)
                    : newGeneralFactory(engine.getConfiguration(), masterFactory, slaveFactory, joinMetadata,
                    joinFilter, perWorkerJoinFilters, masterFilter, perWorkerMasterFilters);
            // The factory owns everything once built, so a single close() frees each exactly once and,
            // through _close(), the base factories and the join metadata too.
            factory.close();

            Assert.assertEquals(1, joinFilter.closeCount);
            Assert.assertEquals(1, perWorkerJoinFilter.closeCount);
            Assert.assertEquals(1, masterFilter.closeCount);
            Assert.assertEquals(1, perWorkerMasterFilter.closeCount);
        });
    }

    /**
     * Fails the cursor constructor, the only fault point that reaches the factory ctor's catch with a
     * live frame sequence to release. The cursor reads no configuration, so the fault rides in on the
     * slave factory's {@code getMetadata()} instead: the cursor is the last thing the ctor builds, so
     * the last such call of the whole construction is the cursor's. Pass 0 counts the calls, pass 1
     * fails the last one. Building the cursor first - as the code did before this ownership fix -
     * moves that last call somewhere else, and the filter close-count assertions below go red.
     */
    private static void assertCursorFailureIsClean(boolean fast) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            int metadataCalls = 0;
            for (int pass = 0; pass < 2; pass++) {
                final RecordCursorFactory masterFactory = baseFactory("master");
                final RecordCursorFactory slaveFactory = baseFactory("slave");
                final JoinRecordMetadata joinMetadata = new JoinRecordMetadata(engine.getConfiguration(), 0);
                final CountingSlaveFactory countingSlaveFactory =
                        new CountingSlaveFactory(slaveFactory, pass == 0 ? 0 : metadataCalls);

                final NativeFilter joinFilter = new NativeFilter();
                final NativeFilter perWorkerJoinFilter = new NativeFilter();
                final ObjList<Function> perWorkerJoinFilters = new ObjList<>();
                perWorkerJoinFilters.add(perWorkerJoinFilter);
                final NativeFilter masterFilter = new NativeFilter();
                final NativeFilter perWorkerMasterFilter = new NativeFilter();
                final ObjList<Function> perWorkerMasterFilters = new ObjList<>();
                perWorkerMasterFilters.add(perWorkerMasterFilter);

                if (pass == 0) {
                    final RecordCursorFactory factory = fast
                            ? newFastFactory(engine.getConfiguration(), masterFactory, countingSlaveFactory, joinMetadata,
                            joinFilter, perWorkerJoinFilters, masterFilter, perWorkerMasterFilters)
                            : newGeneralFactory(engine.getConfiguration(), masterFactory, countingSlaveFactory, joinMetadata,
                            joinFilter, perWorkerJoinFilters, masterFilter, perWorkerMasterFilters);
                    metadataCalls = countingSlaveFactory.calls;
                    Assert.assertTrue("the ctor must read the slave factory metadata", metadataCalls > 0);
                    // Owns everything on success, so this also releases the base factories and the metadata.
                    factory.close();
                    continue;
                }

                try {
                    if (fast) {
                        newFastFactory(engine.getConfiguration(), masterFactory, countingSlaveFactory, joinMetadata,
                                joinFilter, perWorkerJoinFilters, masterFilter, perWorkerMasterFilters);
                    } else {
                        newGeneralFactory(engine.getConfiguration(), masterFactory, countingSlaveFactory, joinMetadata,
                                joinFilter, perWorkerJoinFilters, masterFilter, perWorkerMasterFilters);
                    }
                    Assert.fail("expected the injected CURSOR failure to propagate");
                } catch (FaultInjectedException expected) {
                    Assert.assertEquals(FaultPoint.CURSOR, expected.faultPoint);
                }

                Assert.assertEquals("owner join filter must be closed exactly once", 1, joinFilter.closeCount);
                Assert.assertEquals("per-worker join filter must be closed exactly once", 1, perWorkerJoinFilter.closeCount);
                Assert.assertEquals("owner master filter must be closed exactly once", 1, masterFilter.closeCount);
                Assert.assertEquals("per-worker master filter must be closed exactly once", 1, perWorkerMasterFilter.closeCount);

                // The base factories and the join metadata belong to the caller on a ctor throw.
                Misc.free(masterFactory);
                Misc.free(slaveFactory);
                Misc.free(joinMetadata);
            }
        });
    }

    private static RecordCursorFactory baseFactory(String tableName) throws Exception {
        final RecordCursorFactory factory = select("SELECT * FROM " + tableName);
        return factory instanceof io.questdb.griffin.engine.QueryProgress
                ? ((io.questdb.griffin.engine.QueryProgress) factory).getBaseFactory()
                : factory;
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE master (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE slave (sym SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
    }

    private static RecordCursorFactory newFastFactory(
            CairoConfiguration configuration,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            JoinRecordMetadata joinMetadata,
            Function joinFilter,
            ObjList<Function> perWorkerJoinFilters,
            Function masterFilter,
            ObjList<Function> perWorkerMasterFilters
    ) {
        final PageFrameReduceTaskFactory reduceTaskFactory =
                () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_SQL_COMPILER);
        final ObjList<GroupByFunction> groupByFunctions = new ObjList<>();
        return new AsyncWindowJoinFastRecordCursorFactory(
                engine,
                configuration,
                new BytecodeAssembler(),
                engine.getMessageBus(),
                joinMetadata,
                new GenericRecordMetadata(),
                null,                    // columnIndex
                masterFactory,
                slaveFactory,
                joinFilter,
                perWorkerJoinFilters,
                false,                   // includePrevailing
                0,                       // masterSymbolIndex
                0,                       // slaveSymbolIndex
                0L,                      // windowLo
                0L,                      // windowHi
                new ArrayColumnTypes(),  // valueTypes
                groupByFunctions,        // groupByFunctions (empty is a valid 0-function updater)
                null,                    // perWorkerGroupByFunctions
                null,                    // compiledMasterFilter
                null,                    // bindVarMemory
                null,                    // bindVarFunctions
                masterFilter,
                perWorkerMasterFilters,
                new IntHashSet(),        // filterUsedColumnIndexes
                false,                   // vectorized
                reduceTaskFactory,
                1                        // workerCount
        );
    }

    private static RecordCursorFactory newGeneralFactory(
            CairoConfiguration configuration,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            JoinRecordMetadata joinMetadata,
            Function joinFilter,
            ObjList<Function> perWorkerJoinFilters,
            Function masterFilter,
            ObjList<Function> perWorkerMasterFilters
    ) {
        final PageFrameReduceTaskFactory reduceTaskFactory =
                () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_SQL_COMPILER);
        final ObjList<GroupByFunction> groupByFunctions = new ObjList<>();
        return new AsyncWindowJoinRecordCursorFactory(
                engine,
                configuration,
                new BytecodeAssembler(),
                engine.getMessageBus(),
                joinMetadata,
                new GenericRecordMetadata(),
                null,                    // columnIndex
                masterFactory,
                slaveFactory,
                false,                   // includePrevailing
                joinFilter,
                perWorkerJoinFilters,
                0L,                      // windowLo
                0L,                      // windowHi
                null,                    // windowLoFunc (static window -> not dynamic)
                null,                    // windowHiFunc
                null,                    // perWorkerWindowLoFuncs
                null,                    // perWorkerWindowHiFuncs
                0,                       // loSign
                0,                       // hiSign
                (char) 0,                // loTimeUnit
                (char) 0,                // hiTimeUnit
                null,                    // timestampDriver
                new ArrayColumnTypes(),  // valueTypes
                groupByFunctions,        // groupByFunctions
                null,                    // perWorkerGroupByFunctions
                null,                    // compiledMasterFilter
                null,                    // bindVarMemory
                null,                    // bindVarFunctions
                masterFilter,
                perWorkerMasterFilters,
                new IntHashSet(),        // filterUsedColumnIndexes
                false,                   // vectorized
                reduceTaskFactory,
                1                        // workerCount
        );
    }

    private enum FaultPoint {
        // Read by the AsyncWindowJoin(Fast)Atom constructor (WindowJoinTimeFrameHelper build), after it
        // has adopted the owner/per-worker filters.
        ATOM,
        // Raised by the slave factory the AsyncWindowJoinRecordCursor constructor reads its metadata
        // from. It runs last, so the frame sequence is live and only the factory ctor can release it.
        CURSOR,
        // Read by the PageFrameSequence constructor, which owns the atom by then and closes it on its
        // own failure path.
        FRAME_SEQUENCE
    }

    /**
     * Delegates to the real slave factory, counting {@code getMetadata()} calls and optionally throwing
     * on the n-th one. {@code AsyncWindowJoinRecordCursor}'s constructor makes exactly one such call, so
     * targeting the last one fails that constructor and nothing built before it.
     */
    private static class CountingSlaveFactory implements RecordCursorFactory {
        private final RecordCursorFactory delegate;
        private final int throwOnCall;
        private int calls;

        private CountingSlaveFactory(RecordCursorFactory delegate, int throwOnCall) {
            this.delegate = delegate;
            this.throwOnCall = throwOnCall;
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public RecordMetadata getMetadata() {
            if (++calls == throwOnCall) {
                throw new FaultInjectedException(FaultPoint.CURSOR);
            }
            return delegate.getMetadata();
        }

        @Override
        public ConcurrentTimeFrameCursor newTimeFrameCursor() {
            return delegate.newTimeFrameCursor();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return delegate.recordCursorSupportsRandomAccess();
        }

        @Override
        public boolean supportsTimeFrameCursor() {
            return delegate.supportsTimeFrameCursor();
        }

        @Override
        public void toPlan(PlanSink sink) {
            delegate.toPlan(sink);
        }
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
        public int getSqlAsOfJoinLookAhead() {
            if (faultPoint == FaultPoint.ATOM) {
                throw new FaultInjectedException(faultPoint);
            }
            return super.getSqlAsOfJoinLookAhead();
        }
    }

    /**
     * A filter that holds native memory, so dropping it fails the leak check, and counts its closes, so
     * closing it twice fails an assertion instead of silently double-freeing.
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

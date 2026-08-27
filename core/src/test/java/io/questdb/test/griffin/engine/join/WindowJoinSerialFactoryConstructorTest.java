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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.WindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.WindowJoinRecordCursorFactory;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.FaultInjectedException;
import io.questdb.test.tools.FaultInjectingConfiguration;
import io.questdb.test.tools.FaultInjectingConfiguration.FaultMethod;
import io.questdb.test.tools.NativeFilter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the ownership contract of the SERIAL WINDOW JOIN factory constructors, the half
 * {@link AsyncWindowJoinFactoryConstructorTest} already pins for the async ones.
 * <p>
 * The contract both must honour: on a constructor throw the base factories and the join metadata
 * stay the CALLER's, because {@code SqlCodeGenerator}'s catch frees master, slave and the join
 * metadata itself. The serial constructors used to call {@code close()} on failure, which releases
 * those three through {@code _close()} as well - a second release of everything the generator was
 * about to release. It went unnoticed because a double {@code close()} on an
 * {@code AbstractRecordCursorFactory} is a no-op (flag-guarded), but a factory implementing
 * {@code RecordCursorFactory} directly has no such guard: {@code CoveringIndexRecordCursorFactory}
 * frees its partition-frame factory and its functions unguarded, and {@link JoinRecordMetadata} is
 * reference counted, so its count went negative.
 * <p>
 * {@link CountingFactory} reproduces that shape - it implements the interface directly and counts
 * closes - so the assertion below reads 2 against the old behaviour and 1 against the fixed one.
 */
public class WindowJoinSerialFactoryConstructorTest extends AbstractCairoTest {

    @Test
    public void testFastFactoryLeavesBaseFactoriesToTheCallerOnFailure() throws Exception {
        assertConstructorFailureLeavesBasesToCaller(true, true, false);
    }

    @Test
    public void testFastFactoryReleasesEverythingOnClose() throws Exception {
        assertConstructorSuccessCloses(true, true, false);
    }

    @Test
    public void testFastNonVectorizedFactoryLeavesBaseFactoriesToTheCallerOnFailure() throws Exception {
        // allVectorized == false is the normal production shape whenever a join filter survives
        // (SqlCodeGenerator sets allVectorized = joinFilter == null). It builds the keyed
        // WindowJoinFastRecordCursor, whose constructor used to leak its two native maps on a throw.
        assertConstructorFailureLeavesBasesToCaller(true, false, false);
    }

    @Test
    public void testFastNonVectorizedFactoryReleasesEverythingOnClose() throws Exception {
        assertConstructorSuccessCloses(true, false, false);
    }

    @Test
    public void testFastNonVectorizedPrevailingFactoryLeavesBaseFactoriesToTheCallerOnFailure() throws Exception {
        // include prevailing + a surviving join filter builds
        // WindowJoinWithPrevailingAndJoinFilterFastRecordCursor, which extends
        // WindowJoinFastRecordCursor and inherits its (previously leaking) constructor.
        assertConstructorFailureLeavesBasesToCaller(true, false, true);
    }

    @Test
    public void testFastNonVectorizedPrevailingFactoryReleasesEverythingOnClose() throws Exception {
        assertConstructorSuccessCloses(true, false, true);
    }

    @Test
    public void testFastNonVectorizedPrevailingNoFilterFactoryLeavesBaseFactoriesToTheCallerOnFailure() throws Exception {
        // include prevailing + NO join filter builds WindowJoinWithPrevailingFastRecordCursor
        // (the joinFilter == null && allVectorized == false shape: an INCLUDE PREVAILING window
        // join over a non-vectorizable aggregate). It allocates a native prevailingCache after
        // super(); the base ctor throw here still leaks the two maps without the guard, and this
        // exercises the subclass's overridden close() with a null prevailingCache.
        assertConstructorFailureLeavesBasesToCaller(true, false, true, false);
    }

    @Test
    public void testGeneralFactoryLeavesBaseFactoriesToTheCallerOnFailure() throws Exception {
        assertConstructorFailureLeavesBasesToCaller(false, false, false);
    }

    @Test
    public void testGeneralFactoryReleasesEverythingOnClose() throws Exception {
        assertConstructorSuccessCloses(false, false, false);
    }

    private static void assertConstructorFailureLeavesBasesToCaller(boolean fast, boolean allVectorized, boolean includePrevailing) throws Exception {
        assertConstructorFailureLeavesBasesToCaller(fast, allVectorized, includePrevailing, true);
    }

    private static void assertConstructorFailureLeavesBasesToCaller(boolean fast, boolean allVectorized, boolean includePrevailing, boolean withJoinFilter) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // getSqlAsOfJoinLookAhead() faults inside the cursor constructor, the last thing both
            // factory constructors build, so the throw reaches the catch with every adopted handle live.
            final CairoConfiguration configuration =
                    new FaultInjectingConfiguration(engine.getConfiguration(), FaultMethod.SQL_AS_OF_JOIN_LOOK_AHEAD, null);
            final CountingFactory masterFactory = new CountingFactory(baseFactory("master"));
            final CountingFactory slaveFactory = new CountingFactory(baseFactory("slave"));
            final JoinRecordMetadata joinMetadata = new JoinRecordMetadata(engine.getConfiguration(), 0);
            // The complementary half of the contract: what the ctor DID adopt must still be freed.
            // It holds native memory, so over-nulling would both leak and fail assertMemoryLeak. A
            // null filter is the joinFilter == null && allVectorized == false shape (an INCLUDE
            // PREVAILING window join over a non-vectorizable aggregate) that builds the keyed
            // WindowJoinWithPrevailingFastRecordCursor.
            final NativeFilter joinFilter = withJoinFilter ? new NativeFilter() : null;

            try {
                newFactory(fast, allVectorized, includePrevailing, configuration, masterFactory, slaveFactory, joinMetadata, joinFilter);
                Assert.fail("expected the injected failure to propagate");
            } catch (FaultInjectedException ignore) {
            }

            Assert.assertEquals("the ctor must not close the master it does not own yet", 0, masterFactory.closeCount);
            Assert.assertEquals("the ctor must not close the slave it does not own yet", 0, slaveFactory.closeCount);
            if (joinFilter != null) {
                Assert.assertEquals("the adopted join filter must be closed exactly once", 1, joinFilter.closeCount);
            }

            // Exactly what SqlCodeGenerator's catch does. It must be the FIRST close of each.
            Misc.free(masterFactory);
            Misc.free(slaveFactory);
            Misc.free(joinMetadata);

            Assert.assertEquals("master must be closed exactly once", 1, masterFactory.closeCount);
            Assert.assertEquals("slave must be closed exactly once", 1, slaveFactory.closeCount);
        });
    }

    private static void assertConstructorSuccessCloses(boolean fast, boolean allVectorized, boolean includePrevailing) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final CountingFactory masterFactory = new CountingFactory(baseFactory("master"));
            final CountingFactory slaveFactory = new CountingFactory(baseFactory("slave"));
            final JoinRecordMetadata joinMetadata = new JoinRecordMetadata(engine.getConfiguration(), 0);
            final NativeFilter joinFilter = new NativeFilter();

            final RecordCursorFactory factory =
                    newFactory(fast, allVectorized, includePrevailing, engine.getConfiguration(), masterFactory, slaveFactory, joinMetadata, joinFilter);
            // On success the factory owns all of them, so one close() releases each exactly once.
            factory.close();

            Assert.assertEquals("master must be closed exactly once", 1, masterFactory.closeCount);
            Assert.assertEquals("slave must be closed exactly once", 1, slaveFactory.closeCount);
            Assert.assertEquals("the adopted join filter must be closed exactly once", 1, joinFilter.closeCount);
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

    private static RecordCursorFactory newFactory(
            boolean fast,
            boolean allVectorized,
            boolean includePrevailing,
            CairoConfiguration configuration,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            JoinRecordMetadata joinMetadata,
            Function joinFilter
    ) {
        final ArrayColumnTypes columnTypes = new ArrayColumnTypes();
        columnTypes.add(ColumnType.LONG);
        final ObjList<GroupByFunction> groupByFunctions = new ObjList<>();
        if (fast) {
            return new WindowJoinFastRecordCursorFactory(
                    new BytecodeAssembler(),
                    configuration,
                    new GenericRecordMetadata(),
                    joinMetadata,
                    masterFactory,
                    slaveFactory,
                    null,                    // columnIndex
                    includePrevailing,       // includePrevailing
                    0,                       // windowLo
                    0,                       // windowHi
                    groupByFunctions,
                    columnTypes,
                    0,                       // rightSymbolIndex
                    0,                       // leftSymbolIndex
                    joinFilter,
                    allVectorized            // allVectorized
            );
        }
        return new WindowJoinRecordCursorFactory(
                new BytecodeAssembler(),
                configuration,
                new GenericRecordMetadata(),
                joinMetadata,
                masterFactory,
                slaveFactory,
                includePrevailing,           // includePrevailing
                null,                        // columnIndex
                0,                           // windowLo
                0,                           // windowHi
                null,                        // windowLoFunc
                null,                        // windowHiFunc
                1,                           // loSign
                1,                           // hiSign
                'u',                         // loTimeUnit
                'u',                         // hiTimeUnit
                null,                        // timestampDriver
                groupByFunctions,
                columnTypes,
                joinFilter
        );
    }

    /**
     * Implements {@link RecordCursorFactory} directly, exactly as
     * {@code CoveringIndexRecordCursorFactory} does, so {@link #close()} carries no idempotency
     * guard and a second close is observable.
     */
    private static class CountingFactory implements RecordCursorFactory {
        private final RecordCursorFactory base;
        int closeCount;

        private CountingFactory(RecordCursorFactory base) {
            this.base = base;
        }

        @Override
        public void close() {
            closeCount++;
            Misc.free(base);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            return base.getCursor(executionContext);
        }

        @Override
        public RecordMetadata getMetadata() {
            return base.getMetadata();
        }

        @Override
        public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
            return base.getPageFrameCursor(executionContext, order);
        }

        @Override
        public int getScanDirection() {
            return base.getScanDirection();
        }

        @Override
        public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
            return base.getTimeFrameCursor(executionContext);
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return base.recordCursorSupportsRandomAccess();
        }

        @Override
        public boolean supportsPageFrameCursor() {
            return base.supportsPageFrameCursor();
        }

        @Override
        public boolean supportsTimeFrameCursor() {
            return base.supportsTimeFrameCursor();
        }

        @Override
        public void toPlan(PlanSink sink) {
            base.toPlan(sink);
        }
    }
}

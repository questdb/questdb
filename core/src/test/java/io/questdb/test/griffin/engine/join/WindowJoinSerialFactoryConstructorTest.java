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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.WindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.WindowJoinRecordCursorFactory;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
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
        assertConstructorFailureLeavesBasesToCaller(true);
    }

    @Test
    public void testFastFactoryReleasesEverythingOnClose() throws Exception {
        assertConstructorSuccessCloses(true);
    }

    @Test
    public void testGeneralFactoryLeavesBaseFactoriesToTheCallerOnFailure() throws Exception {
        assertConstructorFailureLeavesBasesToCaller(false);
    }

    @Test
    public void testGeneralFactoryReleasesEverythingOnClose() throws Exception {
        assertConstructorSuccessCloses(false);
    }

    private static void assertConstructorFailureLeavesBasesToCaller(boolean fast) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final CairoConfiguration configuration = new FaultInjectingConfiguration(engine.getConfiguration());
            final CountingFactory masterFactory = new CountingFactory(baseFactory("master"));
            final CountingFactory slaveFactory = new CountingFactory(baseFactory("slave"));
            final JoinRecordMetadata joinMetadata = new JoinRecordMetadata(engine.getConfiguration(), 0);
            // The complementary half of the contract: what the ctor DID adopt must still be freed.
            // It holds native memory, so over-nulling would both leak and fail assertMemoryLeak.
            final NativeFilter joinFilter = new NativeFilter();

            try {
                newFactory(fast, configuration, masterFactory, slaveFactory, joinMetadata, joinFilter);
                Assert.fail("expected the injected failure to propagate");
            } catch (FaultInjectedException ignore) {
            }

            Assert.assertEquals("the ctor must not close the master it does not own yet", 0, masterFactory.closeCount);
            Assert.assertEquals("the ctor must not close the slave it does not own yet", 0, slaveFactory.closeCount);
            Assert.assertEquals("the adopted join filter must be closed exactly once", 1, joinFilter.closeCount);

            // Exactly what SqlCodeGenerator's catch does. It must be the FIRST close of each.
            Misc.free(masterFactory);
            Misc.free(slaveFactory);
            Misc.free(joinMetadata);

            Assert.assertEquals("master must be closed exactly once", 1, masterFactory.closeCount);
            Assert.assertEquals("slave must be closed exactly once", 1, slaveFactory.closeCount);
        });
    }

    private static void assertConstructorSuccessCloses(boolean fast) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final CountingFactory masterFactory = new CountingFactory(baseFactory("master"));
            final CountingFactory slaveFactory = new CountingFactory(baseFactory("slave"));
            final JoinRecordMetadata joinMetadata = new JoinRecordMetadata(engine.getConfiguration(), 0);
            final NativeFilter joinFilter = new NativeFilter();

            final RecordCursorFactory factory =
                    newFactory(fast, engine.getConfiguration(), masterFactory, slaveFactory, joinMetadata, joinFilter);
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
                    false,                   // includePrevailing
                    0,                       // windowLo
                    0,                       // windowHi
                    groupByFunctions,
                    columnTypes,
                    0,                       // rightSymbolIndex
                    0,                       // leftSymbolIndex
                    joinFilter,
                    true                     // allVectorized
            );
        }
        return new WindowJoinRecordCursorFactory(
                new BytecodeAssembler(),
                configuration,
                new GenericRecordMetadata(),
                joinMetadata,
                masterFactory,
                slaveFactory,
                false,                       // includePrevailing
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

    /**
     * Holds native memory and counts its closes, so both over-nulling (leak, and a zero count) and
     * double release are observable. Mirrors AsyncWindowJoinFactoryConstructorTest's filter.
     */
    private static class NativeFilter extends BooleanFunction {
        private static final long SIZE = 64;
        int closeCount;
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

    private static class FaultInjectedException extends RuntimeException {
        private FaultInjectedException() {
            super("injected", null, false, false);
        }
    }

    /**
     * Fails inside the cursor constructor, the last thing both factory constructors build, so the
     * throw reaches the catch with every adopted handle live.
     */
    private static class FaultInjectingConfiguration extends CairoConfigurationWrapper {
        private FaultInjectingConfiguration(CairoConfiguration delegate) {
            super(delegate);
        }

        @Override
        public int getSqlAsOfJoinLookAhead() {
            throw new FaultInjectedException();
        }
    }

}

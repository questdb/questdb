/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.o3;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.Sequence;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.TestTimestampType;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class O3FailureFuzzTest extends AbstractO3Test {
    private final static AtomicInteger counter = new AtomicInteger(0);
    private final static AtomicBoolean failNextAllocOrOpen = new AtomicBoolean(false);
    private static final TestFilesFacadeImpl ffAllocateFailure = new TestFilesFacadeImpl() {
        @Override
        public boolean allocate(long fd, long size) {
            if (counter.decrementAndGet() == 0) {
                failNextAllocOrOpen.set(false);
                return false;
            }
            return super.allocate(fd, size);
        }

        @Override
        public long length(long fd) {
            if (!failNextAllocOrOpen.get() && counter.decrementAndGet() == 0) {
                failNextAllocOrOpen.set(true);
                return 0;
            }
            return super.length(fd);
        }
    };
    private static final FilesFacade ffOpenFailure = new TestFilesFacadeImpl() {
        @Override
        public long openRW(LPSZ name, int opts) {
            if ((Utf8s.endsWithAscii(name, Files.SEPARATOR + "ts.d") && Utf8s.containsAscii(name, "1970-01-06") && counter.decrementAndGet() == 0) && failNextAllocOrOpen.get()) {
                failNextAllocOrOpen.set(false);
                return -1;
            }
            return super.openRW(name, opts);
        }
    };

    private static int failCounter;
    private static Rnd rnd;
    private final int workerCount;

    public O3FailureFuzzTest(ParallelMode mode, TestTimestampType timestampType) {
        super(timestampType);
        this.workerCount = mode == ParallelMode.CONTENDED ? 0 : 2;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {ParallelMode.PARALLEL, TestTimestampType.MICRO},
                {ParallelMode.PARALLEL, TestTimestampType.NANO},
                {ParallelMode.CONTENDED, TestTimestampType.MICRO},
                {ParallelMode.CONTENDED, TestTimestampType.NANO},
        });
    }


    public static void reset() {
        failNextAllocOrOpen.set(false);
        counter.set(Integer.MAX_VALUE);
    }

    @Before
    public void setUp() {
        super.setUp();
        rnd = TestUtils.generateRandom(LOG);
    }

    @After
    public void tearDown() throws Exception {
        reset();
        super.tearDown();
    }

    @Test
    public void testFuzzAllocationFailure() throws Exception {
        runFuzzRoutine(O3FailureFuzzTest::testPartitionedDataAppendOODataNotNullStrTailFailRetry0);
    }

    @Test
    public void testPartitionedDataAppend() throws Exception {
        runFuzzRoutine(O3FailureFuzzTest::testPartitionedDataAppendOOPrependOODatThenRegularAppend0);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOOData() throws Exception {
        runFuzzRoutine(O3FailureFuzzTest::testPartitionedDataAppendOOPrependOODataFailRetry0);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODataMapVarContended() throws Exception {
        reset();
        failCounter = rnd.nextInt(5);
        executeWithPool(0, O3FailureFuzzTest::testPartitionedDataAppendOOPrependOODataFailRetry0, new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (failNextAllocOrOpen.get() && this.fd == fd) {
                    failNextAllocOrOpen.set(false);
                    this.fd = -1;
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (Utf8s.endsWithAscii(name, "1970-01-06" + Files.SEPARATOR + "m.d") && counter.decrementAndGet() == 0) {
                    this.fd = fd;
                    failNextAllocOrOpen.set(true);
                }
                return fd;
            }
        });
    }

    @Test
    public void testPartitionedOpenTimestampFailContended() throws Exception {
        runFuzzRoutineOpen(O3FailureFuzzTest::testPartitionedDataAppendOOPrependOODataFailRetry0);
    }

    private static void assertXCountAndMax(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence expectedMaxTimestamp
    ) throws SqlException {
        assertXCount(compiler, sqlExecutionContext);
        assertMaxTimestamp(engine, expectedMaxTimestamp);
    }

    private static void checkDistressedOrNoSpaceLeft(CairoError e) {
        if (!Chars.equals(e.getMessage(), "Table 'x' is distressed") &&
                !Chars.contains(e.getMessage(), "No space left")) {
            Assert.fail(e.getMessage());
        }
    }

    @NotNull
    private static String prepareCountAndMaxTimestampSinks(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "select count() from x",
                sink2
        );

        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "select max(ts) from x",
                sink
        );

        return Chars.toString(sink);
    }

    private static void reset(int failCounter, CairoEngine engine) {
        reset();
        counter.set(failCounter);
        MessageBus messageBus = engine.getMessageBus();
        final Sequence partitionSubSeq = messageBus.getO3PartitionSubSeq();
        final Sequence openColumnSubSeq = messageBus.getO3OpenColumnSubSeq();
        final Sequence copySubSeq = messageBus.getO3CopySubSeq();

        while (true) {
            long cursor = partitionSubSeq.next();
            if (cursor > -1) {
                partitionSubSeq.done(cursor);
                Assert.fail("partitionSubSeq not empty");
            }

            cursor = openColumnSubSeq.next();
            if (cursor > -1) {
                openColumnSubSeq.done(cursor);
                Assert.fail("openColumnSubSeq not empty");
                continue;
            }

            cursor = copySubSeq.next();
            if (cursor > -1) {
                copySubSeq.done(cursor);
                Assert.fail("copySubSeq not empty");
                continue;
            }
            break;
        }

    }

    private static void testPartitionedDataAppendOODataNotNullStrTailFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " from long_sequence(510)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518300000010L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        for (int i = 0; i < 20; i++) {
            try {
                reset(failCounter, engine);
                engine.execute("insert into x select * from append", sqlExecutionContext);
                return;
            } catch (CairoException ignored) {
            } catch (CairoError e) {
                checkDistressedOrNoSpaceLeft(e);
            }
        }

        reset();

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataAppendOOPrependOODatThenRegularAppend0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " from long_sequence(510)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // all records but one is appended to middle partition
        // last record is prepended to the last partition
        engine.execute(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518390000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " from long_sequence(101)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        for (int i = 0; i < 20; i++) {
            try {
                reset(failCounter, engine);
                engine.execute("insert into x select * from append", sqlExecutionContext);
                return;
            } catch (CairoException ignored) {
            } catch (CairoError e) {
                checkDistressedOrNoSpaceLeft(e);
            }
        }

        reset();
        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        // all records but one is appended to middle partition
        // last record is prepended to the last partition
        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(551000000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " from long_sequence(101)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );


        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append2)",
                "insert into x select * from append2"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext,
                engine);

        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataAppendOOPrependOODataFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " from long_sequence(510)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // all records but one is appended to middle partition
        // last record is prepended to the last partition
        engine.execute(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518390000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " from long_sequence(101)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        for (int i = 0; i < 20; i++) {
            try {
                reset(failCounter, engine);
                engine.execute("insert into x select * from append", sqlExecutionContext);
                return;
            } catch (CairoException ignored) {
            } catch (CairoError e) {
                checkDistressedOrNoSpaceLeft(e);
            }
        }

        reset();

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        if (rnd.nextBoolean()) {
            engine.releaseAllWriters();
        }

        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext,
                engine);

        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private void runFuzzRoutine(CustomisableRunnableWithTimestampType routine) throws Exception {
        reset();
        failCounter = 14 + rnd.nextInt(200);
        LOG.info().$("failCounter=").$(failCounter).$();
        executeWithPool(workerCount, routine, ffAllocateFailure);
    }

    private void runFuzzRoutineOpen(CustomisableRunnableWithTimestampType routine) throws Exception {
        reset();
        failCounter = rnd.nextInt(26);
        LOG.info().$("failCounter=").$(failCounter).$();
        executeWithPool(workerCount, routine, ffOpenFailure);
    }
}

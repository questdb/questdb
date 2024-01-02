/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

public class SecurityTest extends AbstractCairoTest {
    private static final AtomicInteger nCheckInterruptedCalls = new AtomicInteger();
    private static long circuitBreakerCallLimit = Long.MAX_VALUE;
    private static long circuitBreakerTimeoutDeadline = Long.MAX_VALUE;
    private static SqlCompiler memoryRestrictedCompiler;
    private static CairoEngine memoryRestrictedEngine;
    private static SqlExecutionContext readOnlyExecutionContext;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        inputRoot = TestUtils.getCsvRoot();
        AbstractCairoTest.setUpStatic();
        CairoConfiguration readOnlyConfiguration = new DefaultTestCairoConfiguration(root) {

            @Override
            public int getSqlJoinMetadataPageSize() {
                return 64;
            }

            @Override
            public int getSqlMapMaxResizes() {
                return 2;
            }

            @Override
            public int getSqlSmallMapPageSize() {
                return 64;
            }

            @Override
            public int getSqlSortKeyMaxPages() {
                return 2;
            }

            @Override
            public long getSqlSortKeyPageSize() {
                return 64;
            }

            @Override
            public int getSqlSortLightValueMaxPages() {
                return 11;
            }

            @Override
            public long getSqlSortLightValuePageSize() {
                return 1024;
            }

            @Override
            public boolean isSqlParallelFilterEnabled() {
                // Async factories use a special circuit breaker (see PageFrameSequence),
                // so we make sure to use a single-threaded factory in this test.
                return false;
            }

            @Override
            public boolean isSqlParallelGroupByEnabled() {
                // Async factories use a special circuit breaker (see PageFrameSequence),
                // so we make sure to use a single-threaded factory in this test.
                return false;
            }
        };
        memoryRestrictedEngine = new CairoEngine(readOnlyConfiguration);
        SqlExecutionCircuitBreaker dummyCircuitBreaker = new SqlExecutionCircuitBreaker() {
            private long deadline;

            @Override
            public void cancel() {
            }

            @Override
            public boolean checkIfTripped() {
                return false;
            }

            @Override
            public boolean checkIfTripped(long millis, int fd) {
                return false;
            }

            @Override
            public SqlExecutionCircuitBreakerConfiguration getConfiguration() {
                return null;
            }

            @Override
            public int getFd() {
                return -1;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isTimerSet() {
                return false;
            }

            @Override
            public void resetTimer() {
                deadline = circuitBreakerTimeoutDeadline;
            }

            @Override
            public void setFd(int fd) {
            }

            @Override
            public void statefulThrowExceptionIfTripped() {
                int nCalls = nCheckInterruptedCalls.incrementAndGet();
                long max = circuitBreakerCallLimit;
                if (nCalls > max || MicrosecondClockImpl.INSTANCE.getTicks() > deadline) {
                    throw CairoException.critical(0).put("Interrupting SQL processing, max calls is ").put(max);
                }
            }

            @Override
            public void statefulThrowExceptionIfTrippedNoThrottle() {
                statefulThrowExceptionIfTripped();
            }

            @Override
            public void unsetTimer() {
            }
        };

        readOnlyExecutionContext = new SqlExecutionContextImpl(memoryRestrictedEngine, 1)
                .with(
                        ReadOnlySecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        dummyCircuitBreaker
                );
        memoryRestrictedCompiler = memoryRestrictedEngine.getSqlCompiler();
    }

    @AfterClass
    public static void tearDownStatic() {
        memoryRestrictedCompiler = Misc.free(memoryRestrictedCompiler);
        memoryRestrictedEngine = Misc.free(memoryRestrictedEngine);
        AbstractCairoTest.tearDownStatic();
    }

    @After
    public void tearDown() throws Exception {
        // we've to close id file, otherwise parent tearDown() fails on TestUtils.removeTestPath(root) in Windows
        memoryRestrictedEngine.getTableIdGenerator().close();
        memoryRestrictedEngine.clear();
        memoryRestrictedEngine.getTableSequencerAPI().releaseInactive();
        memoryRestrictedEngine.closeNameRegistry();
        super.tearDown();
    }

    @Test
    public void testAlterTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(cust_id int, ccy symbol, balance double)");
            memoryRestrictedEngine.reloadTableNames();

            insert("insert into balances values (1, 'EUR', 140.6)");
            assertQuery(
                    "cust_id\tccy\tbalance\n1\tEUR\t140.6\n",
                    "select * from balances",
                    null,
                    true,
                    true
            );

            try {
                assertException("alter table balances add column newcol int", readOnlyExecutionContext);
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }
            assertQueryPlain("cust_id\tccy\tbalance\n1\tEUR\t140.6\n", "select * from balances");
        });
    }

    @Test
    public void testBackupTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            // create infrastructure where backup is enabled (dir configured)
            ddl("create table balances(cust_id int, ccy symbol, balance double)");

            final File backupDir = temp.newFolder();
            final DateFormat backupSubDirFormat = new TimestampFormatCompiler().compile("ddMMMyyyy");
            try (
                    CairoEngine engine = new CairoEngine(new DefaultTestCairoConfiguration(root) {
                        @Override
                        public DateFormat getBackupDirTimestampFormat() {
                            return backupSubDirFormat;
                        }

                        @Override
                        public CharSequence getBackupRoot() {
                            return backupDir.getAbsolutePath();
                        }
                    });
                    SqlCompiler compiler2 = engine.getSqlCompiler();
                    SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
            ) {
                sqlExecutionContext.with(ReadOnlySecurityContext.INSTANCE, null);
                try {
                    compiler2.compile("backup table balances", sqlExecutionContext);
                    Assert.fail();
                } catch (Exception ex) {
                    Assert.assertTrue(ex.toString().contains("permission denied"));
                }
            }
        });
    }

    @Test
    public void testCircuitBreakerTimeout() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tab as (select" +
                    " rnd_double(2) d" +
                    " from long_sequence(10000000))");
            memoryRestrictedEngine.reloadTableNames();

            try {
                setMaxCircuitBreakerChecks(Long.MAX_VALUE);
                circuitBreakerTimeoutDeadline = MicrosecondClockImpl.INSTANCE.getTicks() + Timestamps.SECOND_MICROS;
                TestUtils.printSql(
                        engine,
                        readOnlyExecutionContext,
                        "tab order by d",
                        sink
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("Interrupting SQL processing"));
            }
        });
    }

    @Test
    public void testCircuitBreakerWithNonKeyedAgg() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(3,3,3,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10000)) timestamp(ts1)");
            memoryRestrictedEngine.reloadTableNames();

            assertQuery(
                    memoryRestrictedCompiler,
                    "sum\n" +
                            "165.6121723103405\n",
                    "select sum(d1) from tb1 where d1 < 0.2",
                    null,
                    false,
                    readOnlyExecutionContext,
                    true
            );
            Assert.assertTrue(nCheckInterruptedCalls.get() > 0);
            try {
                setMaxCircuitBreakerChecks(2);
                assertQuery(
                        memoryRestrictedCompiler,
                        "sym1\nWCP\nICC\nUOJ\nFJG\nOZZ\nGHV\nWEK\nVDZ\nETJ\nUED\n",
                        "select sum(d1) from tb1 where d1 < 0.2",
                        null,
                        false,
                        readOnlyExecutionContext,
                        true
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("Interrupting SQL processing, max calls is 2"));
            }
        });
    }

    @Test
    public void testCircuitBreakerWithUnion() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(3,3,3,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)");
            ddl("create table tb2 as (select" +
                    " rnd_symbol(20,3,3,20000) sym1," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(10000000000, 1000000000) ts2" +
                    " from long_sequence(100)) timestamp(ts2)");
            assertQuery(
                    memoryRestrictedCompiler,
                    "sym1\nWCP\nICC\nUOJ\nFJG\nOZZ\nGHV\nWEK\nVDZ\nETJ\nUED\n",
                    "select sym1 from tb1 where d1 < 0.2 union select sym1 from tb2 where d2 < 0.1",
                    null,
                    false,
                    readOnlyExecutionContext
            );
            Assert.assertTrue(nCheckInterruptedCalls.get() > 0);
            try {
                setMaxCircuitBreakerChecks(2);
                assertQuery(
                        memoryRestrictedCompiler,
                        "sym1\nWCP\nICC\nUOJ\nFJG\nOZZ\nGHV\nWEK\nVDZ\nETJ\nUED\n",
                        "select sym1 from tb1 where d1 < 0.2 union select sym1 from tb2 where d2 < 0.1",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("Interrupting SQL processing, max calls is 2"));
            }
        });
    }

    @Test
    public void testCopyDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            try {
                ddl("create table testDisallowCopySerial (l long)");
                assertException("copy testDisallowCopySerial from '/test-alltypes.csv' with header true", readOnlyExecutionContext);
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.toString(), "permission denied");
            }
        });
    }

    @Test
    public void testCreateTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertException("create table balances(cust_id int, ccy symbol, balance double)", readOnlyExecutionContext);
            } catch (Exception ex) {
                TestUtils.assertContains(ex.getMessage(), "permission denied");
            }
            try {
                assertQuery("count\n1\n", "select count() from balances", null);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.toString().contains("table does not exist"));
            }
        });
    }

    @Test
    public void testDropTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(cust_id int, ccy symbol, balance double)");
            memoryRestrictedEngine.reloadTableNames();
            try {
                assertException("drop table balances", readOnlyExecutionContext);
            } catch (Exception ex) {
                TestUtils.assertContains(ex.getMessage(), "permission denied");
            }
            assertQuery("count\n0\n", "select count() from balances", null, false, true);
        });
    }

    @Test
    public void testInsertDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(cust_id int, ccy symbol, balance double)");
            memoryRestrictedEngine.reloadTableNames();

            assertQuery("count\n0\n", "select count() from balances", null, false, true);

            insert("insert into balances values (1, 'EUR', 140.6)");
            assertQuery(
                    "count\n1\n",
                    "select count() from balances",

                    null,
                    false,
                    true
            );

            try {
                insert("insert into balances values (2, 'ZAR', 140.6)", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }

            assertQuery("count\n1\n", "select count() from balances", null, false, true);
        });
    }

    @Test
    public void testMemoryResizesWithImplicitGroupBy() throws Exception {
        SqlExecutionContext readOnlyExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(ReadOnlySecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(2,2,2,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(1000)) timestamp(ts)");
            assertQuery(
                    memoryRestrictedCompiler,
                    "sym2\td\n" +
                            "GZ\t0.0011075361080621349\n" +
                            "GZ\t0.007985454958725269\n" +
                            "GZ\t0.007868356216637062\n" +
                            "GZ\t0.0014986299883373855\n" +
                            "GZ\t0.006817672510656014\n" +
                            "RX\t0.0016532800623808575\n" +
                            "RX\t0.0072398675350549\n" +
                            "RX\t6.503932953429992E-4\n" +
                            "RX\t0.006651203432318287\n" +
                            "RX\t4.016718301054212E-4\n",
                    "select sym2, d from tb1 where d < 0.01 order by sym2",
                    null,
                    true,
                    readOnlyExecutionContext
            );
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym2, d from tb1 order by sym2",
                        null,
                        true,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("Maximum number of pages (11) breached"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithDistinct() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(40,4,4,20000) sym1," +
                    " rnd_symbol(40,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(40)) timestamp(ts)");
            assertQuery(
                    memoryRestrictedCompiler,
                    "sym1\tsym2\n" +
                            "OOZZ\tHNZH\n" +
                            "GPGW\tQSRL\n" +
                            "FJGE\tQCEH\n" +
                            "PEHN\tIPHZ\n",
                    "select distinct sym1, sym2 from tb1 where d < 0.07",
                    null,
                    true,
                    readOnlyExecutionContext
            );
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select distinct sym1, sym2 from tb1",
                        null,
                        true,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithFullFatInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl(
                    "create table tb1 as (select" +
                            " rnd_symbol(4,4,4,20000) sym1," +
                            " rnd_double(2) d1," +
                            " timestamp_sequence(0, 1000000000) ts1" +
                            " from long_sequence(10)) timestamp(ts1)"
            );

            ddl(
                    "create table tb2 as (select" +
                            " rnd_symbol(3,3,3,20000) sym2," +
                            " rnd_double(2) d2," +
                            " timestamp_sequence(0, 1000000000) ts2" +
                            " from long_sequence(10)) timestamp(ts2)"
            );

            assertQueryFullFat(
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                    null,
                    false,
                    true,
                    true
            );
            memoryRestrictedCompiler.setFullFatJoins(true);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            } finally {
                memoryRestrictedCompiler.setFullFatJoins(false);
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithFullFatOuterJoin() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl(
                    "create table tb1 as (select" +
                            " rnd_symbol(4,4,4,20000) sym1," +
                            " rnd_double(2) d1," +
                            " timestamp_sequence(0, 1000000000) ts1" +
                            " from long_sequence(10)) timestamp(ts1)"
            );
            ddl(
                    "create table tb2 as (select" +
                            " rnd_symbol(3,3,3,20000) sym2," +
                            " rnd_double(2) d2," +
                            " timestamp_sequence(0, 1000000000) ts2" +
                            " from long_sequence(10)) timestamp(ts2)"
            );

            assertQueryFullFat(
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 left join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                    null,
                    false,
                    false,
                    true
            );

            memoryRestrictedCompiler.setFullFatJoins(true);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sym2 from tb1 left join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            } finally {
                memoryRestrictedCompiler.setFullFatJoins(false);
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)");
            ddl("create table tb2 as (select" +
                    " rnd_symbol(3,3,3,20000) sym2," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(0, 1000000000) ts2" +
                    " from long_sequence(10)) timestamp(ts2)");
            assertQuery(
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                    null,
                    false,
                    true
            );

            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithLatestBy() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_long() d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(100)) timestamp(ts)");
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select ts, d from tb1 LATEST ON ts PARTITION BY d",
                        "ts",
                        true,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithLeftHashJoinWithFilter() throws Exception {
        assertLeftHashJoin(false);
    }

    @Test
    public void testMemoryRestrictionsWithLeftHashJoinWithFilterFullFat() throws Exception {
        assertLeftHashJoin(true);
    }

    @Test
    public void testMemoryRestrictionsWithOuterJoin() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)");
            ddl("create table tb2 as (select" +
                    " rnd_symbol(3,3,3,20000) sym2," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(0, 1000000000) ts2" +
                    " from long_sequence(10)) timestamp(ts2)");

            assertQuery(
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 left join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                    null,
                    false,
                    false
            );

            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sym2 from tb1 left join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithRandomAccessOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10)) timestamp(ts)");

            assertQuery(
                    memoryRestrictedCompiler,
                    "sym\td\nVTJW\t0.1985581797355932\nVTJW\t0.21583224269349388\n",
                    "select sym, d from tb1 where d < 0.3 ORDER BY d",
                    null,
                    true,
                    readOnlyExecutionContext
            );

            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym, d from tb1 where d < 0.5 ORDER BY d",
                        null,
                        true,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("Maximum number of pages (2) breached"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithSampleByFillLinear() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)");

            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select ts, sum(d) from tb1 SAMPLE BY 5d FILL(linear)",
                        "ts",
                        true,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithSampleByFillNone() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_symbol(20,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)");

            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(none)",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                TestUtils.assertContains(ex.getMessage(), "limit of 2 resizes exceeded");
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithSampleByFillNull() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_symbol(20,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)");

            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(null)",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithSampleByFillPrev() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)");

            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(prev)",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithSampleByFillValue() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_symbol(20,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 100000000000) ts" +
                    " from long_sequence(1000)) timestamp(ts)");

            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(2.0)",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithUnion() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(3,3,3,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)");
            ddl("create table tb2 as (select" +
                    " rnd_symbol(20,3,3,20000) sym1," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(10000000000, 1000000000) ts2" +
                    " from long_sequence(100)) timestamp(ts2)");

            assertQuery(
                    memoryRestrictedCompiler,
                    "sym1\nWCP\nICC\nUOJ\nFJG\nOZZ\nGHV\nWEK\nVDZ\nETJ\nUED\n",
                    "select sym1 from tb1 where d1 < 0.2 union select sym1 from tb2 where d2 < 0.1",
                    null,
                    false,
                    readOnlyExecutionContext
            );
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1 from tb1 where d1 < 0.2 union select sym1 from tb2",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithoutRandomAccessOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000001000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)");
            ddl("create table tb2 as (select" +
                    " rnd_symbol(3,3,3,20000) sym2," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(0, 1000000000) ts2" +
                    " from long_sequence(10)) timestamp(ts2)");

            assertQuery(
                    memoryRestrictedCompiler,
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 asof join tb2 where d1 < 0.3 ORDER BY d1",
                    null,
                    true,
                    readOnlyExecutionContext
            );
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sym2 from tb1 asof join tb2 where d1 < 0.9 ORDER BY d1",
                        null,
                        true,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("Maximum number of pages (2) breached"));
            }
        });
    }

    @Test
    public void testRenameTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(cust_id int, ccy symbol, balance double)");
            try {
                assertException("rename table balances to newname", readOnlyExecutionContext);
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }
            assertQuery("count\n0\n", "select count() from balances", null, false, true);
        });
    }

    @Test
    public void testTreeResizesWithImplicitGroupBy() throws Exception {
        SqlExecutionContext readOnlyExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(ReadOnlySecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(2,2,2,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(2000)) timestamp(ts)");

            memoryRestrictedEngine.reloadTableNames();
            assertQuery(
                    memoryRestrictedCompiler,
                    "sym2\tcount\nGZ\t1040\nRX\t960\n",
                    "select sym2, count() from tb1 order by sym2",
                    null,
                    true,
                    readOnlyExecutionContext,
                    true
            );
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, count() from tb1 order by sym1",
                        null,
                        true,
                        readOnlyExecutionContext,
                        true
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("Maximum number of pages (2) breached"));
            }
        });
    }

    private static void setMaxCircuitBreakerChecks(long max) {
        nCheckInterruptedCalls.set(0);
        circuitBreakerCallLimit = max;
    }

    private void assertLeftHashJoin(boolean fullFat) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            ddl("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)");
            ddl("create table tb2 as (select" +
                    " rnd_symbol(3,3,3,20000) sym2," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(0, 1000000000) ts2" +
                    " from long_sequence(10)) timestamp(ts2)");

            assertQuery(
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 left join tb2 on tb2.ts2=tb1.ts1 and tb2.ts2::long > 0  where d1 < 0.3",
                    null,
                    false,
                    false
            );
            memoryRestrictedCompiler.setFullFatJoins(fullFat);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sym2 from tb1 left join tb2 on tb2.ts2=tb1.ts1 and tb2.ts2::long > 0  where d1 < 0.3",
                        null,
                        false,
                        readOnlyExecutionContext
                );
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            } finally {
                memoryRestrictedCompiler.setFullFatJoins(false);
            }
        });
    }

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        memoryRestrictedEngine.releaseInactive();
        memoryRestrictedEngine.releaseInactiveTableSequencers();
        memoryRestrictedEngine.closeNameRegistry();
        TestUtils.assertMemoryLeak(() -> {
            try {
                circuitBreakerCallLimit = Integer.MAX_VALUE;
                nCheckInterruptedCalls.set(0);
                code.run();
                engine.releaseInactive();
                Assert.assertEquals("engine's busy writer count", 0, engine.getBusyWriterCount());
                Assert.assertEquals("engine's busy reader count", 0, engine.getBusyReaderCount());
                memoryRestrictedEngine.releaseInactive();
                memoryRestrictedEngine.releaseInactiveTableSequencers();
                memoryRestrictedEngine.closeNameRegistry();
                Assert.assertEquals("restricted engine's busy writer count", 0, memoryRestrictedEngine.getBusyWriterCount());
                Assert.assertEquals("restricted engine's busy reader count", 0, memoryRestrictedEngine.getBusyReaderCount());
            } finally {
                engine.clear();
                memoryRestrictedEngine.clear();
            }
        });
    }

    @Override
    protected void assertQuery(
            SqlCompiler compiler,
            String expected,
            String query,
            String expectedTimestamp,
            boolean supportsRandomAccess,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        memoryRestrictedEngine.reloadTableNames();
        assertQuery(
                compiler,
                expected,
                query,
                expectedTimestamp,
                supportsRandomAccess,
                sqlExecutionContext,
                false
        );
    }
}

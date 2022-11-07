/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.CairoSecurityContextImpl;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.*;

import java.util.concurrent.atomic.AtomicInteger;

public class SecurityTest extends AbstractGriffinTest {
    private static final AtomicInteger nCheckInterruptedCalls = new AtomicInteger();
    private static SqlExecutionContext readOnlyExecutionContext;
    private static SqlCompiler memoryRestrictedCompiler;
    private static CairoEngine memoryRestrictedEngine;
    private static long circuitBreakerCallLimit = Long.MAX_VALUE;
    private static long circuitBreakerTimeoutDeadline = Long.MAX_VALUE;

    @BeforeClass
    public static void setUpStatic() {
        inputRoot = TestUtils.getCsvRoot();
        AbstractGriffinTest.setUpStatic();
        CairoConfiguration readOnlyConfiguration = new DefaultCairoConfiguration(root) {
            @Override
            public int getSqlJoinMetadataMaxResizes() {
                return 10;
            }

            @Override
            public int getSqlJoinMetadataPageSize() {
                return 64;
            }

            @Override
            public int getSqlMapMaxResizes() {
                return 2;
            }

            @Override
            public int getSqlMapPageSize() {
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

        };
        memoryRestrictedEngine = new CairoEngine(readOnlyConfiguration);
        SqlExecutionCircuitBreaker dummyCircuitBreaker = new SqlExecutionCircuitBreaker() {
            private long deadline;

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
            public boolean checkIfTripped() {
                return false;
            }

            @Override
            public boolean checkIfTripped(long millis, long fd) {
                return false;
            }

            @Override
            public void resetTimer() {
                deadline = circuitBreakerTimeoutDeadline;
            }

            @Override
            public SqlExecutionCircuitBreakerConfiguration getConfiguration() {
                return null;
            }

            @Override
            public void setFd(long fd) {
            }

            @Override
            public long getFd() {
                return -1;
            }

            @Override
            public void unsetTimer() {

            }

            @Override
            public boolean isTimerSet() {
                return false;
            }
        };

        readOnlyExecutionContext = new SqlExecutionContextImpl(memoryRestrictedEngine, 1)
                .with(
                        new CairoSecurityContextImpl(
                                false),
                        bindVariableService,
                        null,
                        -1,
                        dummyCircuitBreaker
                );
        memoryRestrictedCompiler = new SqlCompiler(memoryRestrictedEngine);
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractGriffinTest.tearDownStatic();
        Misc.free(memoryRestrictedCompiler);
        Misc.free(memoryRestrictedEngine);
    }

    @After
    public void tearDown() {
        //we've to close id file, otherwise parent tearDown() fails on TestUtils.removeTestPath(root) in Windows 
        memoryRestrictedEngine.getTableIdGenerator().close();
        memoryRestrictedEngine.clear();
        super.tearDown();
    }

    @Test
    public void testAlterTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            CompiledQuery cq = compiler.compile("insert into balances values (1, 'EUR', 140.6)", sqlExecutionContext);
            InsertOperation insertStatement = cq.getInsertOperation();
            try (InsertMethod method = insertStatement.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }
            assertQuery("cust_id\tccy\tbalance\n1\tEUR\t140.6\n", "select * from balances", null, true, true);

            try {
                compile("alter table balances add column newcol int", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }

            assertQueryPlain("cust_id\tccy\tbalance\n1\tEUR\t140.6\n", "select * from balances");
        });
    }

    @Test
    public void testBackupTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                compiler.compile("backup table balances", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }
        });
    }

    @Test
    public void testCircuitBreakerTimeout() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tab as (select" +
                    " rnd_double(2) d" +
                    " from long_sequence(10000000))", sqlExecutionContext);
            try {
                setMaxCircuitBreakerChecks(Long.MAX_VALUE);
                circuitBreakerTimeoutDeadline = MicrosecondClockImpl.INSTANCE.getTicks() + Timestamps.SECOND_MICROS;
                TestUtils.printSql(
                        compiler,
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(3,3,3,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10000)) timestamp(ts1)", sqlExecutionContext);
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
                        true,
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(3,3,3,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)", sqlExecutionContext);
            compiler.compile("create table tb2 as (select" +
                    " rnd_symbol(20,3,3,20000) sym1," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(10000000000, 1000000000) ts2" +
                    " from long_sequence(100)) timestamp(ts2)", sqlExecutionContext);
            assertQuery(
                    memoryRestrictedCompiler,
                    "sym1\nWCP\nICC\nUOJ\nFJG\nOZZ\nGHV\nWEK\nVDZ\nETJ\nUED\n",
                    "select sym1 from tb1 where d1 < 0.2 union select sym1 from tb2 where d2 < 0.1",
                    null,
                    false, readOnlyExecutionContext);
            Assert.assertTrue(nCheckInterruptedCalls.get() > 0);
            try {
                setMaxCircuitBreakerChecks(2);
                assertQuery(
                        memoryRestrictedCompiler,
                        "sym1\nWCP\nICC\nUOJ\nFJG\nOZZ\nGHV\nWEK\nVDZ\nETJ\nUED\n",
                        "select sym1 from tb1 where d1 < 0.2 union select sym1 from tb2 where d2 < 0.1",
                        null,
                        false, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("Interrupting SQL processing, max calls is 2"));
            }
        });
    }

    @Test
    public void testCreateTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
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
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                compiler.compile("drop table balances", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }
            assertQuery("count\n0\n", "select count() from balances", null, false, true);
        });
    }

    @Test
    public void testCopyDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("copy testDisallowCopySerial from '/test-alltypes.csv' with header true", readOnlyExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.toString(), "permission denied");
            }
        });
    }

    @Test
    public void testInsertDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery("count\n0\n", "select count() from balances", null, false, true);

            CompiledQuery cq = compiler.compile("insert into balances values (1, 'EUR', 140.6)", sqlExecutionContext);
            InsertOperation insertOperation = cq.getInsertOperation();
            try (InsertMethod method = insertOperation.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }
            assertQuery("count\n1\n", "select count() from balances", null, false, true);

            try {
                cq = compiler.compile("insert into balances values (2, 'ZAR', 140.6)", readOnlyExecutionContext);
                insertOperation = cq.getInsertOperation();
                try (InsertMethod method = insertOperation.createMethod(readOnlyExecutionContext)) {
                    method.execute();
                    method.commit();
                }
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
                .with(new CairoSecurityContextImpl(false),
                        bindVariableService,
                        null,
                        -1,
                        null);
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(2,2,2,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(1000)) timestamp(ts)", sqlExecutionContext);
            assertQuery(
                    memoryRestrictedCompiler,
                    "sym2\td\nGZ\t0.006817672510656014\nGZ\t0.0014986299883373855\nGZ\t0.007868356216637062\nGZ\t0.007985454958725269\nGZ\t0.0011075361080621349\nRX\t4.016718301054212E-4\nRX\t0.006651203432318287\nRX\t6.503932953429992E-4\nRX\t0.0072398675350549\nRX\t0.0016532800623808575\n",
                    "select sym2, d from tb1 where d < 0.01 order by sym2",
                    null,
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "",
                        "select sym2, d from tb1 order by sym2",
                        null,
                        true, readOnlyExecutionContext);
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_symbol(20,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(20)) timestamp(ts)", sqlExecutionContext);
            assertQuery(
                    memoryRestrictedCompiler,
                    "sym1\tsym2\nDEYY\tCXZO\nDEYY\tDSWU\nSXUX\tZSRY\n",
                    "select distinct sym1, sym2 from tb1 where d < 0.07",
                    null,
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "sym1\tsym2\nHYRX\tGPGW\nVTJW\tIBBT\nVTJW\tGPGW\n",
                        "select distinct sym1, sym2 from tb1",
                        null,
                        true, readOnlyExecutionContext);
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)", sqlExecutionContext);
            compiler.compile("create table tb2 as (select" +
                    " rnd_symbol(3,3,3,20000) sym2," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(0, 1000000000) ts2" +
                    " from long_sequence(10)) timestamp(ts2)", sqlExecutionContext);
            try {
                compiler.setFullFatJoins(true);
                assertQuery(
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                        "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false, sqlExecutionContext);
                try {
                    assertQuery(
                            memoryRestrictedCompiler,
                            "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                            "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                            null,
                            false, readOnlyExecutionContext);
                    Assert.fail();
                } catch (Exception ex) {
                    Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
                }
            } finally {
                compiler.setFullFatJoins(false);
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)", sqlExecutionContext);
            compiler.compile("create table tb2 as (select" +
                    " rnd_symbol(3,3,3,20000) sym2," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(0, 1000000000) ts2" +
                    " from long_sequence(10)) timestamp(ts2)", sqlExecutionContext);
            assertQuery(
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                    null,
                    false, sqlExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                        "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false, readOnlyExecutionContext);
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_long() d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(100)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select ts, d from tb1 LATEST ON ts PARTITION BY d",
                        "ts",
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithOuterJoin() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)", sqlExecutionContext);
            compiler.compile("create table tb2 as (select" +
                    " rnd_symbol(3,3,3,20000) sym2," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(0, 1000000000) ts2" +
                    " from long_sequence(10)) timestamp(ts2)", sqlExecutionContext);
            assertQuery(
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 outer join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                    null,
                    false, sqlExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                        "select sym1, sym2 from tb1 outer join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false, readOnlyExecutionContext);
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10)) timestamp(ts)", sqlExecutionContext);
            assertQuery(
                    memoryRestrictedCompiler,
                    "sym\td\nVTJW\t0.1985581797355932\nVTJW\t0.21583224269349388\n",
                    "select sym, d from tb1 where d < 0.3 ORDER BY d",
                    null,
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "sym\td\nVTJW\t0.1985581797355932\nVTJW\t0.21583224269349388\nPEHN\t0.3288176907679504\n",
                        "select sym, d from tb1 where d < 0.5 ORDER BY d",
                        null,
                        true, readOnlyExecutionContext);
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select ts, sum(d) from tb1 SAMPLE BY 5d FILL(linear)",
                        "ts",
                        true, readOnlyExecutionContext);
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_symbol(20,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
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
                MatcherAssert.assertThat(ex.toString(), Matchers.containsString("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithSampleByFillNull() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_symbol(20,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(null)",
                        null,
                        true, readOnlyExecutionContext);
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(prev)",
                        null,
                        true, readOnlyExecutionContext);
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_symbol(20,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 100000000000) ts" +
                    " from long_sequence(1000)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(2.0)",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
            }
        });
    }

    @Test
    @Ignore
    public void testMemoryRestrictionsWithUnion() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(3,3,3,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)", sqlExecutionContext);
            compiler.compile("create table tb2 as (select" +
                    " rnd_symbol(20,3,3,20000) sym1," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(10000000000, 1000000000) ts2" +
                    " from long_sequence(100)) timestamp(ts2)", sqlExecutionContext);
            assertQuery(
                    memoryRestrictedCompiler,
                    "sym1\nWCP\nICC\nUOJ\nFJG\nOZZ\nGHV\nWEK\nVDZ\nETJ\nUED\n",
                    "select sym1 from tb1 where d1 < 0.2 union select sym1 from tb2 where d2 < 0.1",
                    null,
                    false, readOnlyExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "sym1\td1\tts1\nVTJW\t0.1985581797355932\t1970-01-01T01:06:40.000000Z\nVTJW\t0.21583224269349388\t1970-01-01T01:40:00.000000Z\nRQQ\t0.5522494170511608\t1970-01-01T02:46:40.000000Z\n",
                        "select sym1 from tb1 where d1 < 0.2 union select sym1 from tb2",
                        null,
                        false, readOnlyExecutionContext);
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
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000001000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)", sqlExecutionContext);
            compiler.compile("create table tb2 as (select" +
                    " rnd_symbol(3,3,3,20000) sym2," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(0, 1000000000) ts2" +
                    " from long_sequence(10)) timestamp(ts2)", sqlExecutionContext);
            assertQuery(
                    memoryRestrictedCompiler,
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 asof join tb2 where d1 < 0.3 ORDER BY d1",
                    null,
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\nPEHN\tRQQ\n",
                        "select sym1, sym2 from tb1 asof join tb2 where d1 < 0.9 ORDER BY d1",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("Maximum number of pages (2) breached"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithFullFatOuterJoin() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_double(2) d1," +
                    " timestamp_sequence(0, 1000000000) ts1" +
                    " from long_sequence(10)) timestamp(ts1)", sqlExecutionContext);
            compiler.compile("create table tb2 as (select" +
                    " rnd_symbol(3,3,3,20000) sym2," +
                    " rnd_double(2) d2," +
                    " timestamp_sequence(0, 1000000000) ts2" +
                    " from long_sequence(10)) timestamp(ts2)", sqlExecutionContext);
            try {
                compiler.setFullFatJoins(true);
                assertQuery(
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                        "select sym1, sym2 from tb1 outer join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false, sqlExecutionContext);
                try {
                    assertQuery(
                            memoryRestrictedCompiler,
                            "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                            "select sym1, sym2 from tb1 outer join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                            null,
                            false, readOnlyExecutionContext);
                    Assert.fail();
                } catch (Exception ex) {
                    Assert.assertTrue(ex.toString().contains("limit of 2 resizes exceeded"));
                }
            } finally {
                compiler.setFullFatJoins(false);
            }
        });
    }

    @Test
    public void testRenameTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                compiler.compile("rename table balances to newname", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }
            assertQuery("count\n0\n", "select count() from balances", null, false, true);
        });
    }

    @Test
    public void testTreeResizesWithImplicitGroupBy() throws Exception {
        SqlExecutionContext readOnlyExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(new CairoSecurityContextImpl(false),
                        bindVariableService,
                        null,
                        -1,
                        null);
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(2,2,2,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(2000)) timestamp(ts)", sqlExecutionContext);
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
                        "sym1\tcount\nPEHN\t265\nCPSW\t231\nHYRX\t262\nVTJW\t242\n",
                        "select sym1, count() from tb1 order by sym1",
                        null,
                        readOnlyExecutionContext, true,
                        true,
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

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                circuitBreakerCallLimit = Integer.MAX_VALUE;
                nCheckInterruptedCalls.set(0);
                code.run();
                engine.releaseInactive();
                Assert.assertEquals("engine's busy writer count", 0, engine.getBusyWriterCount());
                Assert.assertEquals("engine's busy reader count", 0, engine.getBusyReaderCount());
                memoryRestrictedEngine.releaseInactive();
                Assert.assertEquals("restricted engine's busy writer count", 0, memoryRestrictedEngine.getBusyWriterCount());
                Assert.assertEquals("restricted engine's busy reader count", 0, memoryRestrictedEngine.getBusyReaderCount());
            } finally {
                engine.clear();
                memoryRestrictedEngine.clear();
            }
        });
    }

}

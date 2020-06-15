/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.CairoSecurityContextImpl;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertStatement;
import io.questdb.test.tools.TestUtils;

public class SecurityTest extends AbstractGriffinTest {
    private static SqlExecutionContext readOnlyExecutionContext;
    private static SqlCompiler memoryRestrictedCompiler;
    private static CairoEngine memoryRestrictedEngine;
    private static final AtomicInteger nCheckInterruptedCalls = new AtomicInteger();
    private static int maxNCheckInterruptedCalls = Integer.MAX_VALUE;

    @BeforeClass
    public static void setUpReadOnlyExecutionContext() {
        CairoConfiguration readOnlyConfiguration = new DefaultCairoConfiguration(root) {
            @Override
            public int getSqlMapPageSize() {
                return 64;
            }

            @Override
            public int getSqlMapMaxResizes() {
                return 2;
            }

            @Override
            public long getSqlSortKeyPageSize() {
                return 64;
            }

            @Override
            public int getSqlSortKeyMaxPages() {
                return 2;
            }
        };
        memoryRestrictedEngine = new CairoEngine(readOnlyConfiguration, messageBus);
        SqlExecutionInterruptor dummyInterruptor = new SqlExecutionInterruptor() {
            @Override
            public void checkInterrupted() {
                int nCalls = nCheckInterruptedCalls.incrementAndGet();
                int max = maxNCheckInterruptedCalls;
                if (nCalls > max) {
                    throw CairoException.instance(0).put("Interrupting SQL processing, max calls is ").put(max);
                }
            }
        };
        readOnlyExecutionContext = new SqlExecutionContextImpl(
                messageBus,
                1,
                memoryRestrictedEngine)
                        .with(
                                new CairoSecurityContextImpl(false,
                                        2),
                                bindVariableService,
                                null,
                                -1,
                                dummyInterruptor);
        memoryRestrictedCompiler = new SqlCompiler(memoryRestrictedEngine, messageBus);
    }

    private static void setMaxInterruptorChecks(int max) {
        nCheckInterruptedCalls.set(0);
        maxNCheckInterruptedCalls = max;
    }

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                maxNCheckInterruptedCalls = Integer.MAX_VALUE;
                nCheckInterruptedCalls.set(0);
                code.run();
                engine.releaseInactive();
                Assert.assertEquals(0, engine.getBusyWriterCount());
                Assert.assertEquals(0, engine.getBusyReaderCount());
                memoryRestrictedEngine.releaseInactive();
                Assert.assertEquals(0, memoryRestrictedEngine.getBusyWriterCount());
                Assert.assertEquals(0, memoryRestrictedEngine.getBusyReaderCount());
            } finally {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                memoryRestrictedEngine.releaseAllReaders();
                memoryRestrictedEngine.releaseAllWriters();
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
    public void testInsertDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            assertQuery("count\n0\n", "select count() from balances", null);

            CompiledQuery cq = compiler.compile("insert into balances values (1, 'EUR', 140.6)", sqlExecutionContext);
            InsertStatement insertStatement = cq.getInsertStatement();
            try (InsertMethod method = insertStatement.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }
            assertQuery("count\n1\n", "select count() from balances", null);

            try {
                cq = compiler.compile("insert into balances values (2, 'ZAR', 140.6)", readOnlyExecutionContext);
                insertStatement = cq.getInsertStatement();
                try (InsertMethod method = insertStatement.createMethod(readOnlyExecutionContext)) {
                    method.execute();
                    method.commit();
                }
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }

            assertQuery("count\n1\n", "select count() from balances", null);
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
            assertQuery("count\n0\n", "select count() from balances", null);
        });
    }

    @Test
    public void testAlterTableDeniedOnNoWriteAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            CompiledQuery cq = compiler.compile("insert into balances values (1, 'EUR', 140.6)", sqlExecutionContext);
            InsertStatement insertStatement = cq.getInsertStatement();
            try (InsertMethod method = insertStatement.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }
            assertQuery("cust_id\tccy\tbalance\n1\tEUR\t140.6\n", "select * from balances", null, true);

            try {
                compiler.compile("alter table balances add column newcol int", readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("permission denied"));
            }

            assertQuery("cust_id\tccy\tbalance\n1\tEUR\t140.6\n", "select * from balances", null, true);
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
            assertQuery("count\n0\n", "select count() from balances", null);
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
    public void testMaxInMemoryRowsWithoutRandomAccessOrderBy() throws Exception {
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
                    "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                    "select sym1, sym2 from tb1 asof join tb2 where d1 < 0.3 ORDER BY d1",
                    null,
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\nPEHN\tRQQ\n",
                        "select sym1, sym2 from tb1 asof join tb2 where d1 < 0.34 ORDER BY d1",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithDistinct() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10)) timestamp(ts)", sqlExecutionContext);
            assertQuery(
                    "sym1\tsym2\nVTJW\tIBBT\nVTJW\tGPGW\n",
                    "select distinct sym1, sym2 from tb1 where d < 0.3",
                    null,
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        "sym1\tsym2\nHYRX\tGPGW\nVTJW\tIBBT\nVTJW\tGPGW\n",
                        "select distinct sym1, sym2 from tb1 where d < 0.34",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithSampleByFillLinear() throws Exception {
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
                        "TOO MUCH",
                        "select ts, sum(d) from tb1 SAMPLE BY 5d FILL(linear)",
                        "ts",
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithSampleByFillNone() throws Exception {
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
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(none)",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithSampleByFillPrev() throws Exception {
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
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(prev)",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithSampleByFillValue() throws Exception {
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
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(2.0)",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithSampleByFillNull() throws Exception {
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
                        "TOO MUCH",
                        "select sym1, sum(d) from tb1 SAMPLE BY 5d FILL(null)",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithLatestBy() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.getRandom().reset();
            compiler.compile("create table tb1 as (select" +
                    " rnd_symbol(4,4,4,20000) sym1," +
                    " rnd_symbol(4,4,4,20000) sym2," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10)) timestamp(ts)", sqlExecutionContext);
            try {
                assertQuery(
                        "TOO MUCH",
                        "select ts, d from tb1 LATEST BY d",
                        "ts",
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMemoryRestrictionsWithImplicitGroupBy() throws Exception {
        SqlExecutionContext readOnlyExecutionContext = new SqlExecutionContextImpl(
                messageBus,
                1,
                engine)
                        .with(
                                new CairoSecurityContextImpl(false,
                                        3),
                                bindVariableService,
                                null, -1, null);
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
                    true, readOnlyExecutionContext);
            try {
                assertQuery(
                        memoryRestrictedCompiler,
                        "sym1\tcount\nPEHN\t265\nCPSW\t231\nHYRX\t262\nVTJW\t242\n",
                        "select sym1, count() from tb1 order by sym1",
                        null,
                        true, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("Maximum number of pages (2) breached"));
            }
        });
    }

    @Test
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
    public void testInterruptorWithUnion() throws Exception {
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
                setMaxInterruptorChecks(2);
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
    public void testMaxInMemoryRowsWithInnerJoin() throws Exception {
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
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                        "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithOuterJoin() throws Exception {
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
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                        "select sym1, sym2 from tb1 outer join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false, readOnlyExecutionContext);
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithFullFatInnerJoin() throws Exception {
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
                compiler.setFullSatJoins(true);
                assertQuery(
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                        "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false, sqlExecutionContext);
                try {
                    assertQuery(
                            "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                            "select sym1, sym2 from tb1 inner join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                            null,
                            false, readOnlyExecutionContext);
                    Assert.fail();
                } catch (Exception ex) {
                    Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
                }
            } finally {
                compiler.setFullSatJoins(false);
            }
        });
    }

    @Test
    public void testMaxInMemoryRowsWithFullFatOuterJoin() throws Exception {
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
                compiler.setFullSatJoins(true);
                assertQuery(
                        "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                        "select sym1, sym2 from tb1 outer join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                        null,
                        false, sqlExecutionContext);
                try {
                    assertQuery(
                            "sym1\tsym2\nVTJW\tFJG\nVTJW\tULO\n",
                            "select sym1, sym2 from tb1 outer join tb2 on tb2.ts2=tb1.ts1 where d1 < 0.3",
                            null,
                            false, readOnlyExecutionContext);
                    Assert.fail();
                } catch (Exception ex) {
                    Assert.assertTrue(ex.toString().contains("limit of 2 exceeded"));
                }
            } finally {
                compiler.setFullSatJoins(false);
            }
        });
    }

}

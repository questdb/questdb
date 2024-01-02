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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.groupby.vect.GroupByVectorAggregateJob;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.MemoryTag;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * This test verifies that various factories use circuit breaker and thus can time out or detect broken connection.
 */
@SuppressWarnings("SameParameterValue")
public class QueryExecutionTimeoutTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        SqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 0;
            }
        };

        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(config, MemoryTag.NATIVE_CB5) {
            @Override
            protected boolean testConnection(int fd) {
                return false;
            }

            {
                setTimeout(-100); // trigger timeout on first check
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testLatestByAllIndexedWithManyWorkersAndMinimalQueue() throws Exception {
        executeWithPool(
                3,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInLatestByAllIndexed(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testLatestByAllIndexedWithManyWorkersAndRegularQueue() throws Exception {
        executeWithPool(
                3,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInLatestByAllIndexed(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testLatestByAllIndexedWithOneWorkerAndMinimalQueue() throws Exception {
        executeWithPool(
                1,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInLatestByAllIndexed(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testLatestByAllIndexedWithOneWorkerAndRegularQueue() throws Exception {
        executeWithPool(
                1,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInLatestByAllIndexed(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInCreateTableAsSelectFromRealTable() throws Exception {
        unsetTimeout();
        try {
            assertTimeout("create table instest as (select rnd_int(), rnd_long(), rnd_double() from long_sequence(10000))", "create table instest2 as (select * from instest);");
        } finally {
            resetTimeout();
        }

        try {
            compile("select * from instest2");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "table does not exist");
        }
    }

    @Test
    public void testTimeoutInCreateTableAsSelectFromVirtualTable() throws Exception {
        assertTimeout("create table instest as (select rnd_int(), rnd_long(), rnd_double() from long_sequence(10000000))");

        try {
            compile("select * from instest");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "table does not exist");
        }
    }

    @Test
    public void testTimeoutInInsertAsSelect() throws Exception {
        assertTimeout("create table instest ( i int, l long, d double ) ",
                "insert into instest select rnd_int(), rnd_long(), rnd_double() from long_sequence(10000000)");
    }

    @Test
    public void testTimeoutInInsertAsSelectBatchedAndOrderedByTs() throws Exception {
        assertTimeout("create table instest ( i int, l long, d double, ts timestamp ) timestamp(ts) ",
                "insert batch 100 into instest select rnd_int(), rnd_long(), rnd_double(), cast(x as timestamp) from long_sequence(10000000)");
    }

    @Test
    public void testTimeoutInInsertAsSelectBatchedAndOrderedByTsAsString() throws Exception {
        assertTimeout("create table instest ( i int, l long, d double, ts timestamp ) timestamp(ts) ",
                "insert batch 100 into instest select rnd_int(), rnd_long(), rnd_double(), cast(cast(x as timestamp) as string) from long_sequence(10000000)");
    }

    @Test
    public void testTimeoutInInsertAsSelectOrderedByTs() throws Exception {
        assertTimeout("create table instest ( i int, l long, d double, ts timestamp ) timestamp(ts) ",
                "insert into instest select rnd_int(), rnd_long(), rnd_double(), cast(x as timestamp) from long_sequence(10000000)");
    }

    @Test
    public void testTimeoutInLatestByAll() throws Exception {
        assertTimeout("create table xx(value long256, ts timestamp) timestamp(ts)",
                "insert into xx values(null, 0)",
                "select * from xx latest on ts partition by value");
    }

    @Test
    public void testTimeoutInLatestByAllFiltered() throws Exception {
        assertTimeout("create table x as " +
                        "(select  rnd_double(0)*100 a, " +
                        "rnd_str(2,4,4) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x latest by b where b = 'HNR'");
    }

    @Test
    public void testTimeoutInLatestByAllIndexed() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            testTimeoutInLatestByAllIndexed(compiler, sqlExecutionContext);
        }
    }

    @Test
    public void testTimeoutInLatestByValue() throws Exception {
        assertTimeout("create table x as " +
                        "(select  rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x where b = 'RXGZ' latest on k partition by b");
    }

    @Test
    public void testTimeoutInLatestByValueFiltered() throws Exception {
        assertTimeout("CREATE table trades(symbol symbol, side symbol, ts timestamp) timestamp(ts)",
                "insert into trades " +
                        "select 'BTC' || x, 'buy' || x, dateadd( 's', x::int, now() ) " +
                        "from long_sequence(10000)",
                "SELECT * FROM trades " +
                        "WHERE symbol in ('BTC1') " +
                        "AND side in 'buy1' " +
                        "LATEST ON ts " +
                        "PARTITION BY symbol;");
    }

    @Test
    public void testTimeoutInLatestByValueIndexed() throws Exception {
        assertTimeout("create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(200)), " +
                        "index(b) timestamp(k) partition by DAY",
                "select * from x where b = 'PEHN' and a < 22 and test_match() latest on k partition by b");
    }

    @Test
    public void testTimeoutInLatestByValueList() throws Exception {
        assertTimeout("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', 'c', 'd', 'e', 'f') s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select ts, x, s from t latest on ts partition by s");
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindAllDistinctSymbolsAndFilter() throws Exception {
        assertTimeout("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "selecT * from t where x%2 = 1 latest on ts partition by s");
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindAllDistinctSymbolsAndNoFilter() throws Exception {
        assertTimeout("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', 'c', 'd', 'e', 'f') s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select ts, x, s from t latest on ts partition by s");
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindSelectedSymbolsAndFilter() throws Exception {
        assertTimeout("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select * from t " +
                        "where s in ('a', 'b') and x%2 = 0 " +
                        "latest on ts partition by s");
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindSelectedSymbolsAndNoFilter() throws Exception {
        assertTimeout("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select * from t where s in ('a', null) latest on ts partition by s");
    }

    @Test
    public void testTimeoutInLatestByValues() throws Exception {
        assertTimeout("create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x where b in (select list('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10)) latest on k partition by b");
    }

    @Test
    public void testTimeoutInLatestByValuesFiltered() throws Exception {
        assertTimeout("create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10)) and a > 12 and a < 50 and test_match() latest on k partition by b");
    }

    @Test
    public void testTimeoutInLatestByValuesIndexed() throws Exception {
        assertTimeout("create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 10000000000) k " +
                        "from long_sequence(300)), " +
                        "index(b) timestamp(k) partition by DAY",
                "select * from x where b in ('XYZ', 'HYRX') and a > 30 and test_match() latest on k partition by b");
    }

    @Test
    public void testTimeoutInMultiHashJoin() throws Exception {
        circuitBreaker.setTimeout(1);
        try {
            assertTimeout("create table grouptest as " +
                            "(select cast(x%1000000 as int) as i, x as l from long_sequence(100000) );\n",
                    "select * from \n" +
                            "(\n" +
                            "  select * \n" +
                            "  from grouptest gt1\n" +
                            "  join grouptest gt2 on i\n" +
                            ")\n" +
                            "join grouptest gt3 on i");
        } finally {
            resetTimeout();
        }
    }

    @Test
    public void testTimeoutInNonVectorizedKeyedGroupBy() throws Exception {
        assertTimeout("create table grouptest as (select x as i, x as l from long_sequence(10000) );",
                "select i, avg(l), max(l) \n" +
                        "from grouptest \n" +
                        "group by i");
    }

    @Test
    public void testTimeoutInNonVectorizedNonKeyedGroupBy() throws Exception {
        assertTimeout("create table grouptest as (select x as i, x as l from long_sequence(10000) );",
                "select avg(cast(l as int)), max(l) \n" +
                        "from grouptest \n");
    }

    @Test
    public void testTimeoutInOrderedRowNumber() throws Exception {
        assertTimeout("create table rntest as (select x as key from long_sequence(1000));\n",
                "select row_number() over (partition by key%1000 order by key ), key  \n" +
                        "from rntest");
    }

    @Test
    public void testTimeoutInParallelKeyedGroupBy() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            testTimeoutInParallelKeyedGroupBy(compiler, sqlExecutionContext);
        }
    }

    @Test
    public void testTimeoutInParallelKeyedGroupByWithManyWorkersAndMinimalQueue() throws Exception {
        pageFrameMaxRows = 1000;
        executeWithPool(
                3,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelKeyedGroupByWithManyWorkersAndRegularQueue() throws Exception {
        pageFrameMaxRows = 1000;
        executeWithPool(
                3,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelKeyedGroupByWithOneWorkerAndMinimalQueue() throws Exception {
        pageFrameMaxRows = 1000;
        executeWithPool(
                1,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelKeyedGroupByWithOneWorkerAndRegularQueue() throws Exception {
        pageFrameMaxRows = 1000;
        executeWithPool(
                1,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelNonKeyedGroupBy() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            testTimeoutInParallelNonKeyedGroupBy(compiler, sqlExecutionContext);
        }
    }

    @Test
    public void testTimeoutInParallelNonKeyedGroupByWithManyWorkersAndMinimalQueue() throws Exception {
        executeWithPool(
                3,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelNonKeyedGroupByWithManyWorkersAndRegularQueue() throws Exception {
        executeWithPool(
                3,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelNonKeyedGroupByWithOneWorkersAndRegularQueue() throws Exception {
        executeWithPool(
                1,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInRowNumber() throws Exception {
        assertTimeout(
                "create table rntest as (select x as key from long_sequence(1000));\n",
                "select row_number() over (partition by key%1000 ), key  \n" +
                        "from rntest"
        );
    }

    @Test
    public void testTimeoutInUpdateTable() throws Exception {
        assertTimeout(
                "create table updtest as (select rnd_int() i, rnd_long() l, rnd_double() d from long_sequence(10000))",
                "update updtest  set i = rnd_int(), l = i * l, d = d/7 *31",
                null
        );
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupBy() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            testTimeoutInVectorizedKeyedGroupBy(compiler, sqlExecutionContext);
        }
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupByWithManyWorkersAndMinimalQueue() throws Exception {
        pageFrameMaxRows = 1000;
        executeWithPool(
                3,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupByWithManyWorkersAndRegularQueue() throws Exception {
        pageFrameMaxRows = 1000;
        executeWithPool(
                3,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupByWithOneWorkerAndMinimalQueue() throws Exception {
        pageFrameMaxRows = 1000;
        executeWithPool(
                1,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupByWithOneWorkerAndRegularQueue() throws Exception {
        pageFrameMaxRows = 1000;
        executeWithPool(
                1,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInVectorizedNonKeyedGroupBy() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            testTimeoutInVectorizedNonKeyedGroupBy(compiler, sqlExecutionContext);
        }
    }

    @Test // triggers timeout when processing task in main thread because queue is too small
    public void testTimeoutInVectorizedNonKeyedGroupByWithManyWorkersAndMinimalQueue() throws Exception {
        executeWithPool(
                3,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test // triggers timeout at end of task creation in main thread
    public void testTimeoutInVectorizedNonKeyedGroupByWithManyWorkersAndRegularQueue() throws Exception {
        executeWithPool(
                3,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test // triggers timeout at end of task creation in main thread
    public void testTimeoutInVectorizedNonKeyedGroupByWithOneWorkersAndRegularQueue() throws Exception {
        executeWithPool(
                1,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @SuppressWarnings("SameParameterValue")
    private void assertTimeout(String ddl) throws Exception {
        assertTimeout(ddl, null, null);
    }

    private void assertTimeout(String ddl, String query) throws Exception {
        assertTimeout(ddl, null, query);
    }

    private void assertTimeout(String ddl, String query, SqlCompiler compiler, SqlExecutionContext context) throws Exception {
        assertTimeout(ddl, null, query, compiler, context);
    }

    private void assertTimeout(String ddl, String dml, String query) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertTimeout(ddl, dml, query, compiler, sqlExecutionContext);
        }
    }

    private void assertTimeout(String ddl, String dml, String query, SqlCompiler compiler, SqlExecutionContext context) throws Exception {
        try {
            assertMemoryLeak(() -> {
                if (dml != null || query != null) {
                    unsetTimeout();
                }
                ddl(compiler, ddl, context);
                if (dml != null) {
                    if (query == null) {
                        resetTimeout();
                    }

                    compile(compiler, dml, context);
                }

                if (query != null) {
                    resetTimeout();
                    snapshotMemoryUsage();
                    CompiledQuery cc = compiler.compile(query, context);
                    try (RecordCursorFactory factory = cc.getRecordCursorFactory();
                         RecordCursor cursor = factory.getCursor(context)) {
                        cursor.hasNext();
                    }
                    assertFactoryMemoryUsage();
                }
            });

            Assert.fail("Cairo timeout exception expected!");
        } catch (SqlException se) {
            resetTimeout();
            TestUtils.assertContains(se.getFlyweightMessage(), "timeout, query aborted");
        } catch (CairoException ce) {
            resetTimeout();
            TestUtils.assertContains(ce.getFlyweightMessage(), "timeout, query aborted");
            Assert.assertTrue("Exception should be interrupted! " + ce, ce.isInterruption());
        }
    }

    private void executeWithPool(
            int workerCount,
            int queueSize,
            CustomisableRunnable runnable
    ) throws Exception {
        assertMemoryLeak(() -> {
            if (workerCount > 0) {
                WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
                    @Override
                    public long getSleepTimeout() {
                        return 1;
                    }

                    @Override
                    public int getWorkerCount() {
                        return workerCount - 1;
                    }
                });

                final CairoConfiguration configuration1 = new DefaultTestCairoConfiguration(root) {
                    @Override
                    public int getGroupByMergeShardQueueCapacity() {
                        return queueSize;
                    }

                    @Override
                    public int getLatestByQueueCapacity() {
                        return queueSize;
                    }

                    @Override
                    public int getPageFrameReduceQueueCapacity() {
                        return queueSize;
                    }

                    @Override
                    public int getSqlPageFrameMaxRows() {
                        return configuration.getSqlPageFrameMaxRows();
                    }

                    @Override
                    public int getVectorAggregateQueueCapacity() {
                        return queueSize;
                    }
                };

                execute(pool, runnable, configuration1);
            } else {
                final CairoConfiguration configuration1 = new DefaultTestCairoConfiguration(root);
                execute(null, runnable, configuration1);
            }
        });
    }

    private void resetTimeout() {
        circuitBreaker.setTimeout(-100);
    }

    private void testTimeoutInLatestByAllIndexed(SqlCompiler compiler, @SuppressWarnings("unused") SqlExecutionContext context) throws Exception {
        assertTimeout(
                "create table x as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) k," +
                        " rnd_double(0)*100 a1," +
                        " rnd_double(0)*100 a2," +
                        " rnd_double(0)*100 a3," +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "select * from (select a,k,b from x latest on k partition by b) where a > 40",
                compiler,
                context
        );
    }

    private void testTimeoutInParallelKeyedGroupBy(SqlCompiler compiler, @SuppressWarnings("unused") SqlExecutionContext context) throws Exception {
        assertTimeout(
                "create table grouptest as (select cast(x%1000000 as int) as i, (x%100) as price, (x%1000) as quantity from long_sequence(10000) );",
                "select i, vwap(price, quantity) from grouptest group by i order by i",
                compiler,
                context
        );
    }

    private void testTimeoutInParallelNonKeyedGroupBy(SqlCompiler compiler, @SuppressWarnings("unused") SqlExecutionContext context) throws Exception {
        assertTimeout(
                "create table grouptest as (select (x%100) as price, (x%1000) as quantity from long_sequence(10000) );",
                "select vwap(price, quantity) from grouptest",
                compiler,
                context
        );
    }

    private void testTimeoutInVectorizedKeyedGroupBy(SqlCompiler compiler, @SuppressWarnings("unused") SqlExecutionContext context) throws Exception {
        assertTimeout(
                "create table grouptest as (select cast(x%1000000 as int) as i, x as l from long_sequence(10000) );",
                "select i, avg(l), max(l) from grouptest group by i",
                compiler,
                context
        );
    }

    private void testTimeoutInVectorizedNonKeyedGroupBy(SqlCompiler compiler, @SuppressWarnings("unused") SqlExecutionContext context) throws Exception {
        assertTimeout(
                "create table grouptest as (select cast(x%1000000 as int) as i, x as l from long_sequence(10000) );",
                "select avg(l), max(l) from grouptest",
                compiler,
                context
        );
    }

    private void unsetTimeout() {
        circuitBreaker.setTimeout(Long.MAX_VALUE);
    }

    protected static void execute(
            @Nullable WorkerPool pool,
            CustomisableRunnable runnable,
            CairoConfiguration configuration
    ) throws Exception {
        final int workerCount = pool == null ? 1 : pool.getWorkerCount() + 1;

        try (
                final CairoEngine engine = new CairoEngine(configuration);
                final SqlExecutionContextImpl sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, workerCount)
        ) {
            sqlExecutionContext.with(circuitBreaker);
            if (pool != null) {
                pool.assign(new GroupByVectorAggregateJob(engine.getMessageBus()));
                pool.assign(new GroupByMergeShardJob(engine.getMessageBus()));
                pool.assign(new LatestByAllIndexedJob(engine.getMessageBus()));
                pool.start(LOG);
            }

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                runnable.run(engine, compiler, sqlExecutionContext);
            }
            Assert.assertEquals("busy writer", 0, engine.getBusyWriterCount());
            Assert.assertEquals("busy reader", 0, engine.getBusyReaderCount());
        } finally {
            if (pool != null) {
                pool.halt();
            }
        }
    }
}

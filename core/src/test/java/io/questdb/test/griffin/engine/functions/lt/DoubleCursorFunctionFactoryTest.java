/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@code double < (sub-query)} / {@code double > (sub-query)} operators, where the
 * right-hand side is a scalar sub-query (cursor) executed once per query execution.
 *
 * @see io.questdb.griffin.engine.functions.lt.LtDoubleCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.lt.GtDoubleCursorFunctionFactory
 */
public class DoubleCursorFunctionFactoryTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // exercise the parallel group by / async filter paths
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 4);
        super.setUp();
    }

    @Test
    public void testCombinedWithIntervalFilter() throws Exception {
        // mirrors the motivating example: an interval filter plus a scalar sub-query predicate,
        // where the sub-query itself carries the same interval filter
        assertMemoryLeak(() -> {
            execute(
                    "create table trades as (" +
                            "  select x::double price, timestamp_sequence('2024-01-01', 60000000) ts" +
                            "  from long_sequence(100)" +
                            ") timestamp(ts) partition by day"
            );
            // prices 1..100 within 2024-01-01, avg = 50.5 -> price > 50.5 -> 50 rows
            assertQuery("select count() c from trades " +
                    "where ts in '2024-01-01' and price > (select avg(price) from trades where ts in '2024-01-01')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n50\n");
        });
    }

    @Test
    public void testEmptyCursorSelectsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            String empty = "price\n";
            assertQuery("select price from t where price > (select avg(price) from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select price from t where price < (select avg(price) from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns(empty);
        });
    }

    @Test
    public void testErrorMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where price > (select avg(price), 1 x from t)")
                    .fails(35, "select must provide exactly one column");
        });
    }

    @Test
    public void testErrorNonNumericCursorColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where price > (select 'abc' from t)")
                    .fails(35, "cannot compare DOUBLE and STRING");
        });
    }

    @Test
    public void testGreaterThan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // avg(price) = 5.5
            assertQuery("select price from t where price > (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("""
                            price
                            6.0
                            7.0
                            8.0
                            9.0
                            10.0
                            """);
        });
    }

    @Test
    public void testGreaterThanOrEqualNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // avg(price) = 5.5 -> price >= 5.5 == price > 5.5 for integer prices
            assertQuery("select price from t where price >= (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("""
                            price
                            6.0
                            7.0
                            8.0
                            9.0
                            10.0
                            """);
        });
    }

    @Test
    public void testLessThan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // avg(price) = 5.5
            assertQuery("select price from t where price < (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("""
                            price
                            1.0
                            2.0
                            3.0
                            4.0
                            5.0
                            """);
        });
    }

    @Test
    public void testLessThanOrEqualNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // avg(price) = 5.5 -> price <= 5.5 == price < 5.5 for integer prices
            assertQuery("select price from t where price <= (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("""
                            price
                            1.0
                            2.0
                            3.0
                            4.0
                            5.0
                            """);
        });
    }

    @Test
    public void testLongCursorColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // sum(x) over 1..10 = 55, so no price > 55
            assertQuery("select price from t where price > (select sum(x) from long_sequence(10))")
                    .noLeakCheck()
                    .returns("price\n");
            // count() = 10, price > 10 -> none; price > 9.x
            assertQuery("select price from t where price > (select count() from t)")
                    .noLeakCheck()
                    .returns("price\n");
        });
    }

    @Test
    public void testNullCursorSelectsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            String empty = "price\n";
            assertQuery("select price from t where price > (select null::double)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select price from t where price < (select null::double)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select price from t where price > (select null)")
                    .noLeakCheck()
                    .returns(empty);
        });
    }

    @Test
    public void testParallelGroupByWithCursorPredicate() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table trades as (" +
                            "  select (x % 10)::int grp, x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            // plan: the cursor predicate is pushed into the parallel (async) group by filter
            assertQuery("explain select grp, count() c from trades where price > (select avg(price) from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("""
                            QUERY PLAN
                            Encode sort light
                              keys: [grp]
                                Async Group By workers: 4
                                  keys: [grp]
                                  values: [count(*)]
                                  filter: price [thread-safe] > cursor\s
                                    Async Group By workers: 4
                                      vectorized: true
                                      values: [avg(price)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);

            // avg(price) = 50000.5 -> x in 50001..100000 -> 5000 rows per grp
            assertQuery("select grp, count() c from trades where price > (select avg(price) from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("""
                            grp\tc
                            0\t5000
                            1\t5000
                            2\t5000
                            3\t5000
                            4\t5000
                            5\t5000
                            6\t5000
                            7\t5000
                            8\t5000
                            9\t5000
                            """);
        });
    }

    @Test
    public void testPlanAsyncFilterStateShared() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // non-thread-safe left operand forces per-worker clones and state sharing
            assertQuery("select price from t where price::string::double > (select avg(price) from t)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: price::string::double > cursor\s
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testPlanAsyncFilterThreadSafe() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where price > (select avg(price) from t)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: price [thread-safe] > cursor\s
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testParallelAsyncFilterAndKeyedSumLessThan() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table trades as (" +
                            "  select (x % 10)::int grp, x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            // (1) a plain filter with the cursor predicate must run on the async filter concurrently
            assertQuery("explain select ts, price from trades where price < (select avg(price) from trades)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("""
                            QUERY PLAN
                            Async Filter workers: 4
                              filter: price [thread-safe] < cursor\s
                                Async Group By workers: 4
                                  vectorized: true
                                  values: [avg(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                            """);

            // filter correctness: avg(price) = 50000.5 -> price in 1..50000 -> 50000 rows
            assertQuery("select count() c from trades where price < (select avg(price) from trades)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("c\n50000\n");

            // (2) a keyed aggregate (sum) over the same predicate must stay parallel (Async Group By workers: 4)
            assertQuery("explain select grp, sum(price) s from trades where price < (select avg(price) from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("""
                            QUERY PLAN
                            Encode sort light
                              keys: [grp]
                                Async Group By workers: 4
                                  keys: [grp]
                                  values: [sum(price)]
                                  filter: price [thread-safe] < cursor\s
                                    Async Group By workers: 4
                                      vectorized: true
                                      values: [avg(price)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);

            assertQuery("select grp, sum(price) s from trades where price < (select avg(price) from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("""
                            grp	s
                            0	1.25025E8
                            1	1.2498E8
                            2	1.24985E8
                            3	1.2499E8
                            4	1.24995E8
                            5	1.25E8
                            6	1.25005E8
                            7	1.2501E8
                            8	1.25015E8
                            9	1.2502E8
                            """);
        });
    }

    private void runWithPool(PoolRunnable body) throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (_, compiler, sqlExecutionContext) ->
                        body.run(compiler, sqlExecutionContext), configuration, LOG);
            }
        });
    }

    @FunctionalInterface
    private interface PoolRunnable {
        void run(SqlCompiler compiler, SqlExecutionContext ctx) throws Exception;
    }
}

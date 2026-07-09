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
 * Tests for the {@code int < (sub-query)} / {@code int > (sub-query)} operators, where the right-hand
 * side is a scalar sub-query (cursor). The left operand is coerced up to the width of the cursor scalar,
 * so an {@code int} compared with a {@code long} sub-query is compared losslessly as longs.
 *
 * @see io.questdb.griffin.engine.functions.lt.LtIntCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.lt.GtIntCursorFunctionFactory
 */
public class IntCursorFunctionFactoryTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 4);
        super.setUp();
    }

    @Test
    public void testByteAndShortLeftOperands() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table s as (select x::short sh, x::byte b from long_sequence(5))");
            // short left, int cursor scalar (3) -> long comparison
            assertQuery("select sh from s where sh < (select 3)")
                    .noLeakCheck()
                    .returns("sh\n1\n2\n");
            // byte left, int cursor scalar (3) -> long comparison
            assertQuery("select b from s where b > (select 3)")
                    .noLeakCheck()
                    .returns("b\n4\n5\n");
        });
    }

    @Test
    public void testDoubleCursorScalar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // avg(i) = 5.5 -> double comparison mode
            assertQuery("select i from t where i < (select avg(i) from t)")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n5\n");
            assertQuery("select i from t where i > (select avg(i) from t)")
                    .noLeakCheck()
                    .returns("i\n6\n7\n8\n9\n10\n");
        });
    }

    @Test
    public void testErrorMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            assertQuery("select i from t where i > (select max(i), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
        });
    }

    @Test
    public void testErrorNonNumericCursorColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            assertQuery("select i from t where i > (select 'abc' from t)")
                    .fails(27, "cannot compare INT and STRING");
        });
    }

    @Test
    public void testGreaterThanIntCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // min(i) = 1 -> i > 1 -> 2..10
            assertQuery("select i from t where i > (select min(i) from t)")
                    .noLeakCheck()
                    .returns("i\n2\n3\n4\n5\n6\n7\n8\n9\n10\n");
            // negated: i <= 1 -> 1
            assertQuery("select i from t where i <= (select min(i) from t)")
                    .noLeakCheck()
                    .returns("i\n1\n");
        });
    }

    @Test
    public void testGreaterThanLongOverflowsInt() throws Exception {
        assertMemoryLeak(() -> {
            // int values near INT_MAX; cursor scalar 5_000_000_000 overflows int range
            execute("create table t as (select (2000000000 + x)::int i from long_sequence(3))");
            // 2000000001..2000000003 > 5_000_000_000 -> none.
            // if the long scalar were narrowed to int (705032704) each row would wrongly match.
            assertQuery("select i from t where i > (select 5000000000)")
                    .noLeakCheck()
                    .returns("i\n");
        });
    }

    @Test
    public void testLessThanIntCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // max(i) = 10 -> i < 10 -> 1..9
            assertQuery("select i from t where i < (select max(i) from t)")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
            // negated: i >= 10 -> 10
            assertQuery("select i from t where i >= (select max(i) from t)")
                    .noLeakCheck()
                    .returns("i\n10\n");
        });
    }

    @Test
    public void testLessThanLongOverflowsInt() throws Exception {
        assertMemoryLeak(() -> {
            // int values near INT_MAX; cursor scalar 5_000_000_000 overflows int range
            execute("create table t as (select (2000000000 + x)::int i from long_sequence(3))");
            // every int is < 5_000_000_000. if the long scalar were narrowed to int (705032704),
            // these rows would be wrongly dropped.
            assertQuery("select i from t where i < (select 5000000000)")
                    .noLeakCheck()
                    .returns("i\n2000000001\n2000000002\n2000000003\n");
        });
    }

    @Test
    public void testNullAndEmptyCursorSelectNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            assertQuery("select i from t where i < (select null)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i > (select null::long)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i < (select max(i) from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns("i\n");
        });
    }

    @Test
    public void testParallelGroupByWithIntCursorPredicate() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table trades as (" +
                            "  select (x % 10)::int grp, x::int qty, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            // the int/cursor predicate is pushed into the parallel (async) group by filter;
            // max(qty)/2 = 50000 is an int cursor scalar -> long comparison mode
            assertQuery("explain select grp, count() c from trades where qty > (select max(qty) / 2 from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("""
                            QUERY PLAN
                            Encode sort light
                              keys: [grp]
                                Async Group By workers: 4
                                  keys: [grp]
                                  values: [count(*)]
                                  filter: qty [thread-safe] > cursor\s
                                    VirtualRecord
                                      functions: [max/2]
                                        Async Group By workers: 4
                                          vectorized: true
                                          values: [max(qty)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);

            // qty in 50001..100000 -> 5000 rows per grp
            assertQuery("select grp, count() c from trades where qty > (select max(qty) / 2 from trades) order by grp")
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
    public void testPlanAsyncFilterLongMode() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            assertQuery("select i from t where i > (select max(i) from t)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: i [thread-safe] > cursor\s
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [max(i)]
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
                            "  select (x % 10)::int grp, x::int qty, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            // (1) a plain filter with the int/cursor predicate must run on the async filter concurrently
            assertQuery("explain select ts, qty from trades where qty < (select max(qty) / 2 from trades)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("""
                            QUERY PLAN
                            Async Filter workers: 4
                              filter: qty [thread-safe] < cursor\s
                                VirtualRecord
                                  functions: [max/2]
                                    Async Group By workers: 4
                                      vectorized: true
                                      values: [max(qty)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                            """);

            // filter correctness: max(qty)/2 = 50000 -> qty in 1..49999 -> 49999 rows
            assertQuery("select count() c from trades where qty < (select max(qty) / 2 from trades)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("c\n49999\n");

            // (2) a keyed aggregate (sum) over the same predicate must stay parallel (Async Group By workers: 4)
            assertQuery("explain select grp, sum(qty) s from trades where qty < (select max(qty) / 2 from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("""
                            QUERY PLAN
                            Encode sort light
                              keys: [grp]
                                Async Group By workers: 4
                                  keys: [grp]
                                  values: [sum(qty)]
                                  filter: qty [thread-safe] < cursor\s
                                    VirtualRecord
                                      functions: [max/2]
                                        Async Group By workers: 4
                                          vectorized: true
                                          values: [max(qty)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);

            assertQuery("select grp, sum(qty) s from trades where qty < (select max(qty) / 2 from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .returnsOnce("""
                            grp	s
                            0	124975000
                            1	124980000
                            2	124985000
                            3	124990000
                            4	124995000
                            5	125000000
                            6	125005000
                            7	125010000
                            8	125015000
                            9	125020000
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

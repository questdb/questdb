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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@code double < (sub-query)} / {@code double > (sub-query)} operators, where the
 * right-hand side is a scalar sub-query (cursor) executed once per query execution.
 *
 * @see io.questdb.griffin.engine.functions.lt.LtDoubleCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.lt.GtDoubleCursorFunctionFactory
 */
public class DoubleCursorFunctionFactoryTest extends AbstractCursorFunctionFactoryTest {

    @Override
    @Before
    public void setUp() {
        // exercise the parallel horizon join paths; horizon joins scan the master with small frames
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        super.setUp();
    }

    @Test
    public void testAsyncGroupByExpressionKeyExecutesCursorOnce() throws Exception {
        // The cursor comparison as a GROUP BY expression key runs on the parallel Async Group By path,
        // where per-worker clones of the key function are initialized with the owner's state donated
        // up front. The scalar sub-query must execute exactly once per query - not once per worker -
        // and every worker must observe the same threshold. test_timestamp_counter() increments once
        // per row the sub-query cursor reads, so the counter equals the number of RHS executions.
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (50000)", ctx);
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            final String query = "select price::string::double > (select test_timestamp_counter(ts)::long from src) k, count() c " +
                    "from t group by k order by k";

            // the non-thread-safe left operand must still run on the parallel group by
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By workers: 4");

            // threshold = 50000 -> 1..50000 false, 50001..100000 true
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tc
                            false\t50000
                            true\t50000
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count,
            // decoupled from how many times the assertion battery above opens cursors
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor(
                            "k\tc\nfalse\t50000\ntrue\t50000\n",
                            cursor,
                            factory.getMetadata(),
                            true,
                            sink
                    );
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testNotKeyedAggregateArgumentExecutesCursorOnce() throws Exception {
        // An aggregate whose argument contains a cursor comparison runs on the not-keyed parallel
        // group by path (AsyncGroupByNotKeyedAtom) with per-worker clones of the group-by functions.
        // The scalar sub-query inside the aggregate argument must execute exactly once per query -
        // not once per worker - and every worker clone must observe the same threshold.
        // test_timestamp_counter() increments once per row the sub-query cursor reads, so the counter
        // equals the number of RHS executions. (The keyed variant is not testable: the optimizer
        // rejects scalar sub-queries inside aggregate arguments of keyed GROUP BY queries.)
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (5000)", ctx);
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            final String query = "select sum(case when price::string::double > (select test_timestamp_counter(ts)::long from src) then 1 else 0 end) s from t";

            // the non-thread-safe aggregate argument must still run on the parallel group by
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By workers: 4");

            // threshold = 5000 -> x in 5001..10000 -> 5000 rows
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            s
                            5000
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("s\n5000\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testHorizonJoinAggregateArgumentExecutesCursorOnce() throws Exception {
        // A not-keyed single-slave HORIZON JOIN whose aggregate argument contains a cursor
        // comparison runs on the parallel path (BaseAsyncHorizonJoinAtom) with per-worker clones of
        // the group-by functions. The scalar sub-query must execute exactly once per query - not
        // once per worker - and every worker clone must observe the same threshold.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);

            final String query = "SELECT avg((t.qty::string::double > (SELECT test_timestamp_counter(ts)::long FROM src))::int) a " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0) AS h";

            // the non-thread-safe aggregate argument must still run on the parallel horizon join
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Horizon Join workers: 4");

            // threshold = 5000 -> qty in 5001..10000 -> 5000 of 10000 rows -> avg 0.5
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a
                            0.5
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("a\n0.5\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testHorizonJoinExpressionKeyExecutesCursorOnce() throws Exception {
        // The cursor comparison as a GROUP BY expression key of a single-slave HORIZON JOIN runs on
        // the parallel path (AsyncHorizonJoinAtom), where per-worker clones of the key function are
        // initialized with the owner's state donated up front. The scalar sub-query must execute
        // exactly once per query - not once per worker - and every worker must observe the same
        // threshold. test_timestamp_counter() increments once per row the sub-query cursor reads, so
        // the counter equals the number of RHS executions.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);

            final String query = "SELECT t.qty::string::double > (SELECT test_timestamp_counter(ts)::long FROM src) k, avg(p.price) a " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0) AS h " +
                    "GROUP BY k ORDER BY k";

            // the non-thread-safe key must still run on the parallel horizon join
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Horizon Join workers: 4");

            // threshold = 5000 -> both key groups see the single price 100.0
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ta
                            false\t100.0
                            true\t100.0
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("k\ta\nfalse\t100.0\ntrue\t100.0\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testMultiHorizonJoinAggregateArgumentExecutesCursorOnce() throws Exception {
        // Same contract as the single-slave variant, on the multi-slave parallel path
        // (BaseAsyncMultiHorizonJoinAtom): the scalar sub-query inside an aggregate argument must
        // execute exactly once per query even with 4 workers holding group-by function clones.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);

            final String query = "SELECT avg((t.qty::string::double > (SELECT test_timestamp_counter(ts)::long FROM src))::int) a, avg(a2.ask) b " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) HORIZON JOIN asks a2 ON (t.sym = a2.sym) LIST (0) AS h";

            // the non-thread-safe aggregate argument must still run on the parallel horizon join
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Multi Horizon Join workers: 4");

            // threshold = 5000 -> avg 0.5; the single ask 200.0 matches every trade
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a\tb
                            0.5\t200.0
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("a\tb\n0.5\t200.0\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testMultiHorizonJoinExpressionKeyExecutesCursorOnce() throws Exception {
        // Same contract as the single-slave variant, on the multi-slave parallel path
        // (AsyncMultiHorizonJoinAtom): the scalar sub-query inside a GROUP BY expression key must
        // execute exactly once per query even with 4 workers holding key function clones.
        runWithPool((compiler, ctx) -> {
            createHorizonJoinTables(compiler, ctx);

            final String query = "SELECT t.qty::string::double > (SELECT test_timestamp_counter(ts)::long FROM src) k, avg(p.price) a, avg(a2.ask) b " +
                    "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) HORIZON JOIN asks a2 ON (t.sym = a2.sym) LIST (0) AS h " +
                    "GROUP BY k ORDER BY k";

            // the non-thread-safe key must still run on the parallel horizon join
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Multi Horizon Join workers: 4");

            // threshold = 5000 -> both key groups see the single price 100.0 and ask 200.0
            assertQuery(query)
                    .withContext(ctx)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ta\tb
                            false\t100.0\t200.0
                            true\t100.0\t200.0
                            """);

            // one explicitly compiled execution pins the exact sub-query execution count
            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("k\ta\tb\nfalse\t100.0\t200.0\ntrue\t100.0\t200.0\n", cursor, factory.getMetadata(), true, sink);
                }
            }
            Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());
        });
    }

    @Test
    public void testSampleByFillLinearCursorComparisonKey() throws Exception {
        // regression: compiling the scalar sub-query of a cursor-comparison key must not corrupt
        // generateSampleBy's projection scratch state, and the execution plan must render across
        // the nested sub-query plan of the key
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select x::double price, x::double qty, timestamp_sequence(0, 60000000) ts" +
                    " from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            final String query = "select price > (select avg(price) from t) k, sum(qty) s, ts from t sample by 1h fill(linear)";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("Sample By");
            // avg(price) = 5.5 -> k=false sums 1..5, k=true sums 6..10; single 1h bucket
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            k\ts\tts
                            false\t15.0\t1970-01-01T00:00:00.000000Z
                            true\t40.0\t1970-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testWindowJoinCursorComparisonProjection() throws Exception {
        // regression: compiling the scalar sub-query of a cursor-comparison projection must not
        // corrupt the WINDOW JOIN aggregation scratch state in generateJoins, and the execution
        // plan must render across the nested sub-query plan
        assertMemoryLeak(() -> {
            execute("create table trades as (" +
                    "select 'A'::symbol sym, x::double price, timestamp_sequence(1000000, 1000000) ts" +
                    " from long_sequence(4)" +
                    ") timestamp(ts) partition by day");
            execute("create table prices (ts timestamp, sym symbol, price double) timestamp(ts)");
            execute("insert into prices values (0, 'A', 2.0)");
            final String query = "select t.price > (select avg(p2.price) from prices p2) k, sum(p.price) w " +
                    "from trades t window join prices p range between 100 seconds preceding and 1 seconds following";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("Window Join");
            // avg = 2.0 -> k = price > 2.0; every window sees the single price 2.0
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\tw
                            false\t2.0
                            false\t2.0
                            true\t2.0
                            true\t2.0
                            """);
        });
    }

    @Test
    public void testHorizonJoinAggregateSubQueryKey() throws Exception {
        // regression: a cursor-comparison GROUP BY key whose scalar sub-query itself aggregates
        // recursively re-enters group-by generation while generateHorizonJoinFactory holds its
        // projection scratch state; the factory must stay keyed and produce correct groups
        assertMemoryLeak(() -> {
            execute("create table trades as (" +
                    "select 'A'::symbol sym, x::double qty, timestamp_sequence(1000000, 1000000) ts" +
                    " from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            execute("create table prices (ts timestamp, sym symbol, price double) timestamp(ts)");
            execute("insert into prices values (0, 'A', 5.0)");
            final String query = "select t.qty > (select avg(price) from prices) k, avg(p.price) a " +
                    "from trades t horizon join prices p on (t.sym = p.sym) list (0) as h " +
                    "group by k order by k";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("Horizon Join");
            // avg(prices.price) = 5.0 -> qty 1..5 false, 6..10 true; both groups see price 5.0
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ta
                            false\t5.0
                            true\t5.0
                            """);
        });
    }

    @Test
    public void testMultiHorizonJoinAggregateSubQueryKey() throws Exception {
        // multi-slave counterpart of testHorizonJoinAggregateSubQueryKey, covering
        // generateMultiHorizonJoinFactory's projection scratch state
        assertMemoryLeak(() -> {
            execute("create table trades as (" +
                    "select 'A'::symbol sym, x::double qty, timestamp_sequence(1000000, 1000000) ts" +
                    " from long_sequence(10)" +
                    ") timestamp(ts) partition by day");
            execute("create table prices (ts timestamp, sym symbol, price double) timestamp(ts)");
            execute("insert into prices values (0, 'A', 5.0)");
            execute("create table asks (ts timestamp, sym symbol, ask double) timestamp(ts)");
            execute("insert into asks values (0, 'A', 7.0)");
            final String query = "select t.qty > (select avg(price) from prices) k, avg(p.price) a, avg(a2.ask) b " +
                    "from trades t horizon join prices p on (t.sym = p.sym) horizon join asks a2 on (t.sym = a2.sym) list (0) as h " +
                    "group by k order by k";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("Multi Horizon Join");
            // avg(prices.price) = 5.0 -> qty 1..5 false, 6..10 true; both groups see 5.0 and 7.0
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ta\tb
                            false\t5.0\t7.0
                            true\t5.0\t7.0
                            """);
        });
    }

    @Test
    public void testGroupByCursorComparisonKey() throws Exception {
        // regression: compiling the sub-query of a cursor comparison used as a GROUP BY key must not
        // corrupt the outer projection scratch state of the code generator (used to throw
        // ArrayIndexOutOfBoundsException from extractVirtualFunctionsFromProjection)
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double d from long_sequence(10000))");
            // avg(d) = 5000.5 -> two groups of 5000 rows each
            assertQuery("select d > (select avg(d) from t) b, count() c from t group by b order by b")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            b\tc
                            false\t5000
                            true\t5000
                            """);
            assertQuery("select d < (select avg(d) from t) b, count() c from t group by b order by b")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            b\tc
                            false\t5000
                            true\t5000
                            """);
        });
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
    public void testBareLiteralNullComparison() throws Exception {
        // End-to-end guard for the ColumnType NULL->CURSOR overload fix: a bare `null` literal is a scalar,
        // never a cursor. `price <= null` (i.e. not(price > null)) must compile to a scalar null-comparison
        // instead of binding to the `>(?C)` cursor-comparison factory and blowing up on
        // getRecordCursorFactory() of the NULL constant. The existing null tests all use (select null...),
        // a different code path, so this pins the bare-literal end-to-end path.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // null comparison matches no rows for every operator, and must not throw at compile time
            assertQuery("select price from t where price <= null")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where price >= null")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where price > null")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where price < null")
                    .noLeakCheck()
                    .returns("price\n");
        });
    }

    @Test
    public void testCursorOnLeftIsSupportedViaSwap() throws Exception {
        // (select ...) > col is supported: the optimizer swaps the operands so the cursor becomes the
        // right-hand scalar sub-query. Pin the correctness of the swapped comparison so it can never
        // silently degrade into an internal failure.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // (avg = 5.5) > price -> price < 5.5 -> 1..5
            assertQuery("select price from t where (select avg(price) from t) > price")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n5.0\n");
            // (avg = 5.5) < price -> price > 5.5 -> 6..10
            assertQuery("select price from t where (select avg(price) from t) < price")
                    .noLeakCheck()
                    .returns("price\n6.0\n7.0\n8.0\n9.0\n10.0\n");
        });
    }

    @Test
    public void testCursorVsCursorFailsCleanly() throws Exception {
        // Comparing two scalar sub-queries has no supporting factory; it must surface a clean
        // "no matching operator" error rather than an internal failure.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where (select max(price) from t) > (select min(price) from t)")
                    .fails(53, "there is no matching operator `>` with the argument types: CURSOR > CURSOR");
            assertQuery("select price from t where (select max(price) from t) < (select min(price) from t)")
                    .fails(53, "there is no matching operator `<` with the argument types: CURSOR < CURSOR");
        });
    }

    @Test
    public void testWalTableCursorPredicate() throws Exception {
        // Correctness of the cursor-scalar predicate on a WAL table (non-async, non-parallel execution
        // path) - the correctness/plan tests elsewhere run only under the parallel async-filter path.
        assertMemoryLeak(() -> {
            execute("create table w (price double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into w select x::double, timestamp_sequence(0, 1000000) from long_sequence(10)");
            drainWalQueue();
            // avg(price) = 5.5 -> price > 5.5 -> 6..10
            assertQuery("select price from w where price > (select avg(price) from w)")
                    .noLeakCheck()
                    .returns("price\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            // negated: price <= 5.5 -> 1..5
            assertQuery("select price from w where price <= (select avg(price) from w)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n5.0\n");
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
            // negated operator over an empty cursor (value == NaN) must also match no rows
            assertQuery("select price from t where price >= (select avg(price) from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns(empty);
        });
    }

    @Test
    public void testErrorMultipleColumns() throws Exception {
        // the < and > factories duplicate the validation code, so both must be asserted
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where price > (select avg(price), 1 x from t)")
                    .fails(35, "select must provide exactly one column");
            assertQuery("select price from t where price < (select avg(price), 1 x from t)")
                    .fails(35, "select must provide exactly one column");
        });
    }

    @Test
    public void testErrorNonNumericCursorColumn() throws Exception {
        // the < and > factories duplicate the validation code, so both must be asserted
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where price > (select 'abc' from t)")
                    .fails(35, "cannot compare DOUBLE and STRING");
            assertQuery("select price from t where price < (select 'abc' from t)")
                    .fails(35, "cannot compare DOUBLE and STRING");
        });
    }

    @Test
    public void testTypedNumericCursorScalars() throws Exception {
        // pins the BYTE/SHORT/INT readers of the cursor scalar plus the typed-NULL branches
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            // BYTE cursor scalar
            assertQuery("select price from t where price > (select 5::byte)")
                    .noLeakCheck()
                    .returns("price\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            assertQuery("select price from t where price < (select 5::byte)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n");
            // SHORT cursor scalar, boundary via the negated operator (price == 5.0 matches <=)
            assertQuery("select price from t where price > (select 5::short)")
                    .noLeakCheck()
                    .returns("price\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            assertQuery("select price from t where price <= (select 5::short)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n5.0\n");
            // INT cursor scalar, boundary via the negated operator (price == 5.0 matches >=)
            assertQuery("select price from t where price < (select 5::int)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n");
            assertQuery("select price from t where price >= (select 5::int)")
                    .noLeakCheck()
                    .returns("price\n5.0\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            // typed INT/LONG NULL scalars must map to NaN and match no rows for every operator
            assertQuery("select price from t where price > (select null::int)")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where price < (select null::int)")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where price >= (select null::long)")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where price <= (select null::long)")
                    .noLeakCheck()
                    .returns("price\n");
        });
    }

    @Test
    public void testWorkerStateSharedExecutesCursorOnceAndRefreshes() throws Exception {
        // Proves the worker-state contract of the async filter path with a non-thread-safe left
        // operand: (1) the scalar sub-query executes exactly once per query execution even with 4
        // workers; (2) every worker clone observes the owner's scalar (rows across the threshold are
        // classified correctly); (3) re-executing the same compiled factory refreshes the cached state.
        // test_timestamp_counter() increments once per row the sub-query cursor reads, so the counter
        // equals the number of RHS executions.
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (5000)", ctx);
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::double price, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(
                    "select count() c from t where price::string::double > (select test_timestamp_counter(ts)::long from src)",
                    ctx
            ).getRecordCursorFactory()) {
                // threshold = 5000 -> 5001..10000 -> 5000 rows
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("c\n5000\n", cursor, factory.getMetadata(), true, sink);
                }
                Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());

                // change the RHS and re-execute the same compiled factory: the cached scalar must refresh
                execute(compiler, "update src set ts = 9000", ctx);
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertCursor("c\n1000\n", cursor, factory.getMetadata(), true, sink);
                }
                Assert.assertEquals(2, TestTimestampCounterFactory.COUNTER.get());
            }
        });
    }

    @Test
    public void testMultiRowCursorFails() throws Exception {
        // a scalar sub-query yielding more than one row is an error, reported at the sub-query position
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            assertQuery("select price from t where price > (select x::double from long_sequence(2))")
                    .fails(35, "scalar sub-query returned more than one row");
            assertQuery("select price from t where price < (select x::double from long_sequence(2))")
                    .fails(35, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testFloatBindVariableLeftOperand() throws Exception {
        // A FLOAT-typed function (here a bind variable) reaches the cursor-comparison factory as a
        // raw FLOAT - unlike a FLOAT column, which the optimizer widens to DOUBLE before the factory
        // ever sees it. This pins the FLOAT left-operand support in the factory guard: the value is
        // widened to double losslessly via Function#getDouble.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price from long_sequence(10))");
            bindVariableService.clear();
            // avg(price) = 5.5; :thr == avg -> strict comparisons select no rows on either side
            bindVariableService.setFloat("thr", 5.5f);
            assertQuery("select price from t where :thr > (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("price\n");
            assertQuery("select price from t where :thr < (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("price\n");
            // :thr > avg -> the constant predicate is true, so every row matches
            bindVariableService.setFloat("thr", 9.5f);
            assertQuery("select price from t where :thr > (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("""
                            price
                            1.0
                            2.0
                            3.0
                            4.0
                            5.0
                            6.0
                            7.0
                            8.0
                            9.0
                            10.0
                            """);
        });
    }

    @Test
    public void testFloatColumnLeftOperand() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::float price from long_sequence(10))");
            // avg(price) = 5.5; a FLOAT column is widened to DOUBLE up front, comparison stays exact
            assertQuery("select price from t where price > (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("price\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            assertQuery("select price from t where price < (select avg(price) from t)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n5.0\n");
        });
    }

    @Test
    public void testFloatCursorColumnAndBothSides() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double price, x::float fprice from long_sequence(10))");
            // FLOAT cursor scalar on the right (read via Record#getFloat): min(fprice) = 1.0
            assertQuery("select price from t where price > (select min(fprice) from t)")
                    .noLeakCheck()
                    .returns("price\n2.0\n3.0\n4.0\n5.0\n6.0\n7.0\n8.0\n9.0\n10.0\n");
            // FLOAT on both sides: FLOAT column left (widened) vs FLOAT cursor scalar right
            assertQuery("select fprice from t where fprice < (select max(fprice) from t)")
                    .noLeakCheck()
                    .returns("fprice\n1.0\n2.0\n3.0\n4.0\n5.0\n6.0\n7.0\n8.0\n9.0\n");
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
            // negated operators exercise the hand-rolled null-under-negation branch (value == NaN)
            assertQuery("select price from t where price >= (select null::double)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select price from t where price <= (select null::double)")
                    .noLeakCheck()
                    .returns(empty);
        });
    }

    @Test
    public void testNullLeftColumn() throws Exception {
        // long_sequence never yields null cells, so the null LEFT-column path needs an explicit null.
        // A null left value must never match a non-null cursor scalar (any operator), and must follow
        // QuestDB's null == null convention against a null cursor: >= and <= match, strict > / < do not.
        assertMemoryLeak(() -> {
            execute("create table t (id int, price double)");
            execute("insert into t values (1, null), (2, 5.0), (3, 8.0)");
            // null-left (id 1) is excluded for every operator against a non-null cursor
            assertQuery("select id from t where price > (select min(price) from t)") // > 5
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where price < (select max(price) from t)") // < 8
                    .noLeakCheck()
                    .returns("id\n2\n");
            assertQuery("select id from t where price >= (select max(price) from t)") // >= 8
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where price <= (select min(price) from t)") // <= 5
                    .noLeakCheck()
                    .returns("id\n2\n");
            // null == null: a null left value matches a null cursor for >= and <= only
            assertQuery("select id from t where price >= (select null::double)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where price <= (select null::double)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where price > (select null::double)")
                    .noLeakCheck()
                    .returns("id\n");
            assertQuery("select id from t where price < (select null::double)")
                    .noLeakCheck()
                    .returns("id\n");
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
            assertQuery("select grp, count() c from trades where price > (select avg(price) from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlan("""
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
                    .expectSize()
                    .returns("""
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
            assertQuery("select ts, price from trades where price < (select avg(price) from trades)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlan("""
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
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n50000\n");

            // (2) a keyed aggregate (sum) over the same predicate must stay parallel (Async Group By workers: 4)
            assertQuery("select grp, sum(price) s from trades where price < (select avg(price) from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlan("""
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
                    .expectSize()
                    .returns("""
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

    private void createHorizonJoinTables(SqlCompiler compiler, SqlExecutionContext ctx) throws Exception {
        execute(compiler, "create table src (ts timestamp)", ctx);
        execute(compiler, "insert into src values (5000)", ctx);
        execute(
                compiler,
                "create table trades as (" +
                        "  select 'A'::symbol sym, x::double qty, timestamp_sequence(1000000, 1000000) ts" +
                        "  from long_sequence(10000)" +
                        ") timestamp(ts) partition by day",
                ctx
        );
        execute(compiler, "create table prices (ts timestamp, sym symbol, price double) timestamp(ts)", ctx);
        execute(compiler, "insert into prices values (0, 'A', 100.0)", ctx);
        execute(compiler, "create table asks (ts timestamp, sym symbol, ask double) timestamp(ts)", ctx);
        execute(compiler, "insert into asks values (0, 'A', 200.0)", ctx);
    }

}

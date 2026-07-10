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
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
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
        // enables the test_timestamp_counter() function used to count sub-query executions
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
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
                            "  select x::int i, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(
                    "select count() c from t where i::string::int > (select test_timestamp_counter(ts)::int from src)",
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
        // the < and > factories duplicate the validation code, so both must be asserted
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            assertQuery("select i from t where i > (select max(i), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
            assertQuery("select i from t where i < (select max(i), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
        });
    }

    @Test
    public void testFloatCursorColumn() throws Exception {
        // A FLOAT-typed cursor scalar routes to the DoubleCursorFunc FLOAT arm (read via Record#getFloat).
        // A float value derived from a table column widens to DOUBLE in projection, so a FLOAT constant
        // sub-query is used to keep the cursor column FLOAT and actually exercise the getFloat(0) path.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // i > 5.5f -> 6..10
            assertQuery("select i from t where i > (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("i\n6\n7\n8\n9\n10\n");
            // i < 5.5f -> 1..5
            assertQuery("select i from t where i < (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n5\n");
            // negated operators over the FLOAT arm
            assertQuery("select i from t where i >= (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("i\n6\n7\n8\n9\n10\n");
        });
    }

    @Test
    public void testErrorNonNumericCursorColumn() throws Exception {
        // the < and > factories duplicate the validation code, so both must be asserted
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            assertQuery("select i from t where i > (select 'abc' from t)")
                    .fails(27, "cannot compare INT and STRING");
            assertQuery("select i from t where i < (select 'abc' from t)")
                    .fails(27, "cannot compare INT and STRING");
        });
    }

    @Test
    public void testMultiRowCursorFails() throws Exception {
        // A scalar sub-query yielding more than one row is an error, reported at the sub-query position.
        // The INT/LONG/DOUBLE(FLOAT) cursor modes are separately implemented readers inside each factory,
        // so each mode is asserted for both operators.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // INT cursor mode
            assertQuery("select i from t where i > (select x::int from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select i from t where i < (select x::int from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            // LONG cursor mode
            assertQuery("select i from t where i > (select x from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select i from t where i < (select x from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            // DOUBLE cursor mode
            assertQuery("select i from t where i > (select x::double from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select i from t where i < (select x::double from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            // FLOAT cursor mode (shares DoubleCursorFunc, distinct reader arm)
            assertQuery("select i from t where i > (select x::float from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select i from t where i < (select x::float from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testTypedNumericCursorScalars() throws Exception {
        // pins the BYTE/SHORT readers of the cursor scalar and the typed FLOAT/DOUBLE NULL
        // branches of the double comparison mode
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // BYTE cursor scalar
            assertQuery("select i from t where i > (select 5::byte)")
                    .noLeakCheck()
                    .returns("i\n6\n7\n8\n9\n10\n");
            assertQuery("select i from t where i < (select 5::byte)")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n");
            // SHORT cursor scalar, boundaries via the negated operators
            assertQuery("select i from t where i >= (select 5::short)")
                    .noLeakCheck()
                    .returns("i\n5\n6\n7\n8\n9\n10\n");
            assertQuery("select i from t where i <= (select 5::short)")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n5\n");
            // typed FLOAT/DOUBLE NULL scalars route through the double comparison mode and
            // must match no rows for every operator
            assertQuery("select i from t where i > (select null::double)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i < (select null::float)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i >= (select null::double)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i <= (select null::float)")
                    .noLeakCheck()
                    .returns("i\n");
        });
    }

    @Test
    public void testBareLiteralNullComparison() throws Exception {
        // End-to-end guard for the ColumnType NULL->CURSOR overload fix: a bare `null` literal is a scalar,
        // never a cursor. `i <= null` (i.e. not(i > null)) must compile to a scalar null-comparison instead
        // of binding to the `>(?C)` cursor-comparison factory and blowing up on getRecordCursorFactory().
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // null comparison matches no rows for every operator, and must not throw at compile time
            assertQuery("select i from t where i <= null")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i >= null")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i > null")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i < null")
                    .noLeakCheck()
                    .returns("i\n");
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
            // negated operators over a null / empty cursor must also match no rows
            assertQuery("select i from t where i >= (select null)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i <= (select null::long)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i >= (select max(i) from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns("i\n");
        });
    }

    @Test
    public void testNullLeftColumn() throws Exception {
        // long_sequence never yields null cells, so the null LEFT-column path needs an explicit null.
        // A null left value must never match a non-null cursor scalar (any operator), and must follow
        // QuestDB's null == null convention against a null cursor: >= and <= match, strict > / < do not.
        assertMemoryLeak(() -> {
            execute("create table t (id int, i int)");
            execute("insert into t values (1, null), (2, 5), (3, 8)");
            // null-left (id 1) is excluded for every operator against a non-null cursor
            assertQuery("select id from t where i > (select min(i) from t)") // > 5
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where i < (select max(i) from t)") // < 8
                    .noLeakCheck()
                    .returns("id\n2\n");
            assertQuery("select id from t where i >= (select max(i) from t)") // >= 8
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where i <= (select min(i) from t)") // <= 5
                    .noLeakCheck()
                    .returns("id\n2\n");
            // null == null: a null left value matches a null cursor for >= and <= only
            assertQuery("select id from t where i >= (select null)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where i <= (select null)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where i > (select null)")
                    .noLeakCheck()
                    .returns("id\n");
            assertQuery("select id from t where i < (select null)")
                    .noLeakCheck()
                    .returns("id\n");
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
            assertQuery("select grp, count() c from trades where qty > (select max(qty) / 2 from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlan("""
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
            assertQuery("select ts, qty from trades where qty < (select max(qty) / 2 from trades)")
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlan("""
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
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n49999\n");

            // (2) a keyed aggregate (sum) over the same predicate must stay parallel (Async Group By workers: 4)
            assertQuery("select grp, sum(qty) s from trades where qty < (select max(qty) / 2 from trades) order by grp")
                    .withContext(ctx)
                    .noLeakCheck()
                    .assertsPlan("""
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
                    .expectSize()
                    .returns("""
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

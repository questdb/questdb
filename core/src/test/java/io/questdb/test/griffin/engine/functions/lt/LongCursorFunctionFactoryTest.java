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
 * Tests for the {@code long < (sub-query)} / {@code long > (sub-query)} operators, where the right-hand
 * side is a scalar sub-query (cursor). When the cursor scalar is an integer type the comparison is done
 * as a {@code long} comparison, so {@code long} values beyond 2^53 keep full precision (a comparison via
 * {@code double} would conflate them).
 *
 * @see io.questdb.griffin.engine.functions.lt.LtLongCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.lt.GtLongCursorFunctionFactory
 */
public class LongCursorFunctionFactoryTest extends AbstractCairoTest {

    // 2^53, 2^53+1, 2^53+2 : the middle value is NOT representable as a double (it rounds to 2^53),
    // so any comparison performed via double would conflate 2^53 and 2^53+1.
    private static final long POW2_53 = 9007199254740992L;

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
    public void testDoubleCursorScalar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            // avg(l) = 5.5 -> double comparison mode
            assertQuery("select l from t where l < (select avg(l) from t)")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n");
            assertQuery("select l from t where l > (select avg(l) from t)")
                    .noLeakCheck()
                    .returns("l\n6\n7\n8\n9\n10\n");
        });
    }

    @Test
    public void testErrorMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l > (select max(l), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
        });
    }

    @Test
    public void testFloatCursorColumn() throws Exception {
        // A FLOAT-typed cursor scalar routes to the DoubleCursorFunc FLOAT arm (read via Record#getFloat).
        // A float value derived from a table column widens to DOUBLE in projection, so a FLOAT constant
        // sub-query is used to keep the cursor column FLOAT and actually exercise the getFloat(0) path.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            // l > 5.5f -> 6..10
            assertQuery("select l from t where l > (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("l\n6\n7\n8\n9\n10\n");
            // l < 5.5f -> 1..5
            assertQuery("select l from t where l < (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n");
            // negated operators over the FLOAT arm
            assertQuery("select l from t where l <= (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n");
        });
    }

    @Test
    public void testIntCursorColumn() throws Exception {
        // An INT-typed cursor scalar goes through readScalarLong's INT branch (Numbers.intToLong).
        // That mapping is the sole guard that an INT_NULL cursor scalar becomes LONG_NULL (matching no
        // rows) rather than -2147483648L; a plain widening would make l > (select null::int) match rows.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            // non-null INT cursor scalar: l > 5 -> 6..10, l < 5 -> 1..4
            assertQuery("select l from t where l > (select 5::int)")
                    .noLeakCheck()
                    .returns("l\n6\n7\n8\n9\n10\n");
            assertQuery("select l from t where l < (select 5::int)")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n");
            // negated: l <= 5 -> 1..5
            assertQuery("select l from t where l <= (select 5::int)")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n");
            // INT_NULL cursor scalar must map to LONG_NULL -> matches no rows for every operator
            assertQuery("select l from t where l > (select null::int)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l < (select null::int)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l >= (select null::int)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l <= (select null::int)")
                    .noLeakCheck()
                    .returns("l\n");
        });
    }

    @Test
    public void testErrorNonNumericCursorColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l > (select 'abc' from t)")
                    .fails(27, "cannot compare LONG and STRING");
        });
    }

    @Test
    public void testMultiRowCursorFails() throws Exception {
        // a scalar sub-query yielding more than one row is an error, reported at the sub-query position
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l > (select x::long from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select l from t where l < (select x::long from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testBareLiteralNullComparison() throws Exception {
        // End-to-end guard for the ColumnType NULL->CURSOR overload fix: a bare `null` literal is a scalar,
        // never a cursor. `l <= null` (i.e. not(l > null)) must compile to a scalar null-comparison instead
        // of binding to the `>(?C)` cursor-comparison factory and blowing up on getRecordCursorFactory().
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            // null comparison matches no rows for every operator, and must not throw at compile time
            assertQuery("select l from t where l <= null")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l >= null")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l > null")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l < null")
                    .noLeakCheck()
                    .returns("l\n");
        });
    }

    @Test
    public void testGreaterThanLongCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l > (select min(l) from t)")
                    .noLeakCheck()
                    .returns("l\n2\n3\n4\n5\n6\n7\n8\n9\n10\n");
            // negated: l <= min -> 1
            assertQuery("select l from t where l <= (select min(l) from t)")
                    .noLeakCheck()
                    .returns("l\n1\n");
        });
    }

    @Test
    public void testGreaterThanPreservesLongPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (l long)");
            execute("insert into t values (" + POW2_53 + "), (" + (POW2_53 + 1) + "), (" + (POW2_53 + 2) + ")");
            // l > 2^53 : as long -> {2^53+1, 2^53+2}. via double, 2^53+1 == 2^53 -> it would be dropped.
            assertQuery("select l from t where l > (select " + POW2_53 + ")")
                    .noLeakCheck()
                    .returns("l\n" + (POW2_53 + 1) + "\n" + (POW2_53 + 2) + "\n");
        });
    }

    @Test
    public void testLessThanLongCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l < (select max(l) from t)")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
            // negated: l >= max -> 10
            assertQuery("select l from t where l >= (select max(l) from t)")
                    .noLeakCheck()
                    .returns("l\n10\n");
        });
    }

    @Test
    public void testLessThanPreservesLongPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (l long)");
            execute("insert into t values (" + POW2_53 + "), (" + (POW2_53 + 1) + "), (" + (POW2_53 + 2) + ")");
            // l < 2^53+1 : as long -> {2^53}. via double, 2^53+1 rounds to 2^53, so nothing would match.
            assertQuery("select l from t where l < (select " + (POW2_53 + 1) + ")")
                    .noLeakCheck()
                    .returns("l\n" + POW2_53 + "\n");
        });
    }

    @Test
    public void testNullAndEmptyCursorSelectNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l < (select null)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l > (select null::long)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l < (select max(l) from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns("l\n");
            // negated operators over a null / empty cursor must also match no rows
            assertQuery("select l from t where l >= (select null)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l <= (select null::long)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l >= (select max(l) from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns("l\n");
        });
    }

    @Test
    public void testNullLeftColumn() throws Exception {
        // long_sequence never yields null cells, so the null LEFT-column path needs an explicit null.
        // A null left value must never match a non-null cursor scalar (any operator), and must follow
        // QuestDB's null == null convention against a null cursor: >= and <= match, strict > / < do not.
        assertMemoryLeak(() -> {
            execute("create table t (id int, l long)");
            execute("insert into t values (1, null), (2, 5), (3, 8)");
            // null-left (id 1) is excluded for every operator against a non-null cursor
            assertQuery("select id from t where l > (select min(l) from t)") // > 5
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where l < (select max(l) from t)") // < 8
                    .noLeakCheck()
                    .returns("id\n2\n");
            assertQuery("select id from t where l >= (select max(l) from t)") // >= 8
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where l <= (select min(l) from t)") // <= 5
                    .noLeakCheck()
                    .returns("id\n2\n");
            // null == null: a null left value matches a null cursor for >= and <= only
            assertQuery("select id from t where l >= (select null)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where l <= (select null)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where l > (select null)")
                    .noLeakCheck()
                    .returns("id\n");
            assertQuery("select id from t where l < (select null)")
                    .noLeakCheck()
                    .returns("id\n");
        });
    }

    @Test
    public void testParallelGroupByWithLongCursorPredicate() throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table trades as (" +
                            "  select (x % 10)::int grp, x::long qty, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

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
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l > (select max(l) from t)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: l [thread-safe] > cursor\s
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [max(l)]
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
                            "  select (x % 10)::int grp, x::long qty, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            // (1) a plain filter with the long/cursor predicate must run on the async filter concurrently
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

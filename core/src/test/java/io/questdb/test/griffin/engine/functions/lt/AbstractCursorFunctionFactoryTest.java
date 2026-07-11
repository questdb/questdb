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

/**
 * Shared harness of the numeric cursor-comparison factory tests: enables the parallel group by /
 * async filter paths and the {@code test_timestamp_counter()} instrumentation, and runs test
 * bodies against a four-worker pool.
 */
abstract class AbstractCursorFunctionFactoryTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // exercise the parallel group by / async filter paths
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 4);
        // enables the test_timestamp_counter() function used to count sub-query executions
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    /**
     * Asserts that a bare {@code null} literal on the right of every comparison operator compiles
     * to a scalar null-comparison (never a cursor comparison) and matches no rows. The generic
     * behavior is shared by every numeric left-operand type; type-specific rationale stays with
     * the concrete test.
     */
    protected final void assertBareNullBehavior(String columnType, String columnName) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::" + columnType + " " + columnName + " from long_sequence(10))");
            final String empty = columnName + "\n";
            // null comparison matches no rows for every operator, and must not throw at compile time
            assertQuery("select " + columnName + " from t where " + columnName + " <= null")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " >= null")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " > null")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " < null")
                    .noLeakCheck()
                    .returns(empty);
        });
    }

    /**
     * Asserts that a null cursor scalar (bare or typed) and an empty scalar sub-query match no
     * rows for the strict operators and their negated forms alike. The generic behavior is shared
     * by every numeric left-operand type.
     */
    protected final void assertNullAndEmptyCursorBehavior(String columnType, String columnName) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::" + columnType + " " + columnName + " from long_sequence(10))");
            final String empty = columnName + "\n";
            assertQuery("select " + columnName + " from t where " + columnName + " < (select null)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " > (select null::long)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " < (select max(" + columnName + ") from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns(empty);
            // negated operators over a null / empty cursor must also match no rows
            assertQuery("select " + columnName + " from t where " + columnName + " >= (select null)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " <= (select null::long)")
                    .noLeakCheck()
                    .returns(empty);
            assertQuery("select " + columnName + " from t where " + columnName + " >= (select max(" + columnName + ") from t where 1 <> 1)")
                    .noLeakCheck()
                    .returns(empty);
        });
    }

    /**
     * Asserts the null LEFT-column contract: long_sequence never yields null cells, so the table
     * carries an explicit null. A null left value must never match a non-null cursor scalar (any
     * operator), and must follow QuestDB's null == null convention against a null cursor: >= and
     * <= match, strict > / < do not. The generic behavior is shared by every numeric left-operand
     * type.
     */
    protected final void assertNullLeftColumnBehavior(String columnType, String columnName) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (id int, " + columnName + " " + columnType + ")");
            execute("insert into t values (1, null), (2, 5), (3, 8)");
            // null-left (id 1) is excluded for every operator against a non-null cursor
            assertQuery("select id from t where " + columnName + " > (select min(" + columnName + ") from t)") // > 5
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where " + columnName + " < (select max(" + columnName + ") from t)") // < 8
                    .noLeakCheck()
                    .returns("id\n2\n");
            assertQuery("select id from t where " + columnName + " >= (select max(" + columnName + ") from t)") // >= 8
                    .noLeakCheck()
                    .returns("id\n3\n");
            assertQuery("select id from t where " + columnName + " <= (select min(" + columnName + ") from t)") // <= 5
                    .noLeakCheck()
                    .returns("id\n2\n");
            // null == null: a null left value matches a null cursor for >= and <= only
            assertQuery("select id from t where " + columnName + " >= (select null)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where " + columnName + " <= (select null)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where " + columnName + " > (select null)")
                    .noLeakCheck()
                    .returns("id\n");
            assertQuery("select id from t where " + columnName + " < (select null)")
                    .noLeakCheck()
                    .returns("id\n");
        });
    }

    /**
     * Asserts the async-filter and keyed parallel group by contract of the cursor predicate:
     * (1) a plain filter with the cursor predicate runs on the async filter concurrently;
     * (2) the filter classifies rows correctly (max(qty)/2 = 50000 -> qty in 1..49999);
     * (3) a keyed aggregate (sum) over the same predicate stays parallel. The integer cursor
     * scalar compares in long mode, so the plan and the results are identical for every integer
     * left-operand type.
     */
    protected final void assertParallelAsyncFilterAndKeyedSumLessThanBehavior(String qtyColumnType) throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table trades as (" +
                            "  select (x % 10)::int grp, x::" + qtyColumnType + " qty, timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(100000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            // (1) a plain filter with the cursor predicate must run on the async filter concurrently
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
                            grp\ts
                            0\t124975000
                            1\t124980000
                            2\t124985000
                            3\t124990000
                            4\t124995000
                            5\t125000000
                            6\t125005000
                            7\t125010000
                            8\t125015000
                            9\t125020000
                            """);
        });
    }

    /**
     * Asserts that the cursor predicate is pushed into the parallel (async) group by filter and
     * classifies rows correctly. max(qty)/2 = 50000 is an integer cursor scalar, so the
     * comparison runs in long mode and the plan and the results are identical for every integer
     * left-operand type.
     */
    protected final void assertParallelGroupByWithCursorPredicateBehavior(String qtyColumnType) throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(
                    compiler,
                    "create table trades as (" +
                            "  select (x % 10)::int grp, x::" + qtyColumnType + " qty, timestamp_sequence(0, 1000000) ts" +
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

    /**
     * Asserts that the cursor comparison against a max() sub-query compiles to the async filter
     * with the long comparison mode, rendering the cursor plan inline. The plan shape is
     * identical for every integer left-operand type up to the column name.
     */
    protected final void assertPlanAsyncFilterLongModeBehavior(String columnType, String columnName) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::" + columnType + " " + columnName + " from long_sequence(10))");
            assertQuery("select " + columnName + " from t where " + columnName + " > (select max(" + columnName + ") from t)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: %s [thread-safe] > cursor\s
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [max(%s)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """.formatted(columnName, columnName));
        });
    }

    /**
     * Proves the worker-state contract of the async filter path with a non-thread-safe left
     * operand: (1) the scalar sub-query executes exactly once per query execution even with 4
     * workers; (2) every worker clone observes the owner's scalar (rows across the threshold are
     * classified correctly); (3) re-executing the same compiled factory refreshes the cached
     * state. test_timestamp_counter() increments once per row the sub-query cursor reads, so the
     * counter equals the number of RHS executions. The contract is shared by every numeric
     * left-operand type; {@code cursorScalarType} picks the cursor comparison mode under test.
     */
    protected final void assertWorkerStateSharedBehavior(String columnType, String columnName, String cursorScalarType) throws Exception {
        runWithPool((compiler, ctx) -> {
            execute(compiler, "create table src (ts timestamp)", ctx);
            execute(compiler, "insert into src values (5000)", ctx);
            execute(
                    compiler,
                    "create table t as (" +
                            "  select x::" + columnType + " " + columnName + ", timestamp_sequence(0, 1000000) ts" +
                            "  from long_sequence(10000)" +
                            ") timestamp(ts) partition by day",
                    ctx
            );

            TestTimestampCounterFactory.COUNTER.set(0);
            try (RecordCursorFactory factory = compiler.compile(
                    "select count() c from t where " + columnName + "::string::" + columnType +
                            " > (select test_timestamp_counter(ts)::" + cursorScalarType + " from src)",
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

    protected final void runWithPool(PoolRunnable body) throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (_, compiler, sqlExecutionContext) ->
                        body.run(compiler, sqlExecutionContext), configuration, LOG);
            }
        });
    }

    @FunctionalInterface
    protected interface PoolRunnable {
        void run(SqlCompiler compiler, SqlExecutionContext ctx) throws Exception;
    }
}

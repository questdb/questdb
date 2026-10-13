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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A function with per-call state ({@code timestamp_sequence()}, which counts its calls) gives a
 * row its value by the order of the calls, so the plan must not copy it per worker: each copy
 * would count from its own start. Every query here runs on a 4-worker pool, with function
 * memoization on as in production, once with every parallel plan switched off and once with them
 * on, and the two must agree. Each query reaches one place that compiles a copy of a function per
 * worker: the Async Window chain's filter and GROUP BY steps, a window over a stolen filter, the
 * Async Filter's per-worker filters, and the parallel GROUP BY's per-worker functions and filter.
 * The positive control checks that functions without per-call state (a clock, now()) keep their
 * parallel plans.
 */
public class StatefulFunctionWorkerCopiesTest extends AbstractCairoTest {
    private static final String LAGGED = "(SELECT time, ex, price, size, lag(price) OVER (ORDER BY time) lp FROM trade WHERE sym = 'BIG')";

    @Override
    public void setUp() {
        super.setUp();
        allowFunctionMemoization();
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_PAGE_FRAME_MAX_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 200);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 5_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 1_000);
    }

    @Test
    public void testAsyncFilter() throws Exception {
        assertAllMatchSerial(
                "SELECT time, sym, price FROM trade WHERE timestamp_sequence(0, 1) < 5_000::timestamp",
                "SELECT time, sym, price FROM trade WHERE price > 101 AND timestamp_sequence(0, 1) < 5_000::timestamp",
                "SELECT time, sym, price FROM trade WHERE timestamp_sequence(time, 1) < '1970-01-02'::timestamp",
                // the copy is a clone too when the stateful function is deep in an expression
                "SELECT time, sym FROM trade WHERE coalesce(price, 0) + timestamp_sequence(0, 1)::long % 7 > 103"
        );
    }

    @Test
    public void testChainFilterStep() throws Exception {
        assertAllMatchSerial(
                "SELECT time, lp FROM " + LAGGED + " WHERE lp + 1_000 < timestamp_sequence(0, 1)::long",
                "SELECT time, lp FROM " + LAGGED + " WHERE lp < 101 OR timestamp_sequence(0, 1)::long % 3 = 0"
        );
    }

    @Test
    public void testChainGroupByStep() throws Exception {
        assertAllMatchSerial(
                "SELECT g, count() n, sum(timestamp_sequence(0, 1)::long) c FROM (SELECT time, sum(f) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) g FROM (SELECT time, CASE WHEN price > lp THEN 1 ELSE 0 END f FROM " + LAGGED + ")) ORDER BY g"
        );
    }

    @Test
    public void testAsyncWindowFunctionArgument() throws Exception {
        assertAllMatchSerial(
                "SELECT time, sum(timestamp_sequence(0, 1)::long) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM trade WHERE sym = 'BIG'",
                "SELECT time, avg(timestamp_sequence(0, 1)::long) OVER (ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) s FROM trade WHERE sym = 'BIG'"
        );
    }

    @Test
    public void testRowPureStaysParallel() throws Exception {
        // the positive control: the same shapes without the stateful function keep their
        // parallel plans
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            final String[][] queries = {
                    {"SELECT time, sym, price FROM trade WHERE price > 101 AND time < now()", "Async"},
                    // a clock and a memoized cast have no per-call state: still on the workers
                    {"SELECT time, sym, price FROM trade WHERE price > 101 AND time < systimestamp()", "Async"},
                    {"SELECT sym, count() n FROM trade WHERE time < systimestamp() ORDER BY sym", "Async"},
                    {"SELECT time, lp FROM " + LAGGED + " WHERE lp < 101 + size", "then: filter"},
                    {"SELECT g, count() n, sum(seq::long) c FROM (SELECT time, seq, sum(f) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) g FROM (SELECT time, seq, CASE WHEN price > lp THEN 1 ELSE 0 END f FROM (SELECT time, seq, price, lag(price) OVER (ORDER BY time) lp FROM trade WHERE sym = 'BIG'))) ORDER BY g", "then: group by"},
                    {"SELECT sym, sum(seq::long) s FROM trade WHERE price > 101 ORDER BY sym", "Async"},
                    {"SELECT time, sum(seq::long) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM trade WHERE sym = 'BIG'", "Async Window"},
            };
            for (String[] query : queries) {
                Assert.assertEquals(query[0], run(engine, ctx, query[0], false), run(engine, ctx, query[0], true));
                Assert.assertTrue(query[0] + "\n" + plan(engine, ctx, query[0]), plan(engine, ctx, query[0]).contains(query[1]));
            }
        }));
    }

    @Test
    public void testParallelGroupBy() throws Exception {
        assertAllMatchSerial(
                "SELECT sym, count() n FROM trade WHERE timestamp_sequence(0, 1) < 5_000::timestamp ORDER BY sym",
                "SELECT sym, sum(timestamp_sequence(0, 1)::long) s FROM trade ORDER BY sym",
                "SELECT timestamp_sequence(0, 1)::long % 3 k, count() n FROM trade ORDER BY k",
                // the POSTING index gather: the GROUP BY compiles its own copies of the filter
                "SELECT ex, count() n FROM trade WHERE sym IN ('BIG', 'K1') AND timestamp_sequence(0, 1) < 3_000::timestamp ORDER BY ex"
        );
    }

    @Test
    public void testWindowOverStolenFilter() throws Exception {
        assertAllMatchSerial(
                // the shards' stolen filter
                "SELECT time, sum(size) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM trade WHERE ex = 'T' AND timestamp_sequence(0, 1) < 3_000::timestamp",
                // the index scan's residual filter
                "SELECT time, sum(size) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM trade WHERE sym = 'BIG' AND timestamp_sequence(0, 1) < 3_000::timestamp"
        );
    }

    private static String run(CairoEngine engine, SqlExecutionContextImpl ctx, String query, boolean parallel) throws Exception {
        ctx.setParallelWindowEnabled(parallel);
        ctx.setParallelFilterEnabled(parallel);
        ctx.setParallelGroupByEnabled(parallel);
        try {
            final StringSink sink = new StringSink();
            engine.print(query, sink, ctx);
            return sink.toString();
        } finally {
            ctx.setParallelWindowEnabled(false);
            ctx.setParallelFilterEnabled(true);
            ctx.setParallelGroupByEnabled(true);
        }
    }

    private static String plan(CairoEngine engine, SqlExecutionContextImpl ctx, String query) throws Exception {
        ctx.setParallelWindowEnabled(true);
        try {
            final StringSink plan = new StringSink();
            engine.print("explain " + query, plan, ctx);
            return plan.toString();
        } finally {
            ctx.setParallelWindowEnabled(false);
        }
    }

    private void assertAllMatchSerial(String... queries) throws Exception {
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            final StringSink failures = new StringSink();
            for (String query : queries) {
                final String expected = run(engine, ctx, query, false);
                Assert.assertTrue(query, expected.indexOf('\n') > 0);
                for (int i = 0; i < 3; i++) {
                    final String actual = run(engine, ctx, query, true);
                    if (!expected.equals(actual)) {
                        failures.put("WRONG ROWS: ").put(query).put('\n').put(plan(engine, ctx, query))
                                .put("expected ").put(Long.toString(expected.lines().count())).put(" lines; got ")
                                .put(Long.toString(actual.lines().count())).put(" lines\n");
                        break;
                    }
                }
            }
            if (failures.length() > 0) {
                Assert.fail(failures.toString());
            }
        }));
    }

    private void inPool(PoolTest test) throws Exception {
        final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
        TestUtils.execute(pool, (engine, compiler, context) -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
            ctx.changePageFrameSizes(1, 64);
            engine.execute(
                    "create table trade (time timestamp, sym symbol index type posting, ex symbol, price float, size float, seq int)" +
                            " timestamp(time) partition by DAY",
                    ctx
            );
            final int rows = 20_000;
            engine.execute(
                    "insert into trade select" +
                            " ((x / 2) * " + (3 * 86_400_000_000L / rows) + "L)::timestamp," +
                            " case when x % 5 < 2 then 'BIG' when x % 13 = 0 then null else 'K' || (x % 20) end," +
                            " case when x % 3 = 0 then 'T' else 'Q' end," +
                            " case when x % 11 = 0 then null else (100 + (x / 7) % 5)::float end," +
                            " case when x % 17 = 0 then null else (x % 9 + 0.25)::float end," +
                            " (x % 97 - (x % 7) * 3)::int" +
                            " from long_sequence(" + rows + ")",
                    ctx
            );
            test.run(engine, ctx);
        }, configuration, LOG);
    }

    @FunctionalInterface
    private interface PoolTest {
        void run(CairoEngine engine, SqlExecutionContextImpl ctx) throws Exception;
    }
}

/*+*****************************************************************************
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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Row slices of a plain scan with a WHERE (see {@code SqlCodeGenerator.generateSliceWindow}) run
 * the Async JIT Filter's compiled filter over each slice's rows, the frame's column addresses
 * moved to the slice's first row, rather than the Java filter row by row: a tiny result of a
 * large table then costs what the Async JIT Filter costs. Frames here are larger than a slice, so
 * that most slices start inside a frame. A walk that an interval leaves too small for the parallel
 * window runs on the query's own thread. Every query runs serially and on the parallel window, and
 * the two must agree bit for bit.
 */
public class WindowSliceCompiledFilterTest extends AbstractCairoTest {
    private static final String RUN = " OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";

    @Test
    public void testBindVariables() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        config();
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext);
            final String q = "SELECT time, sum(size)" + RUN + " s, count()" + RUN + " c FROM trade WHERE ex = :ex AND size > :lim";
            for (String ex : new String[]{"L", "T", "NOPE"}) {
                for (long lim : new long[]{-1, 50, 99}) {
                    sqlExecutionContext.getBindVariableService().clear();
                    sqlExecutionContext.getBindVariableService().setStr("ex", ex);
                    sqlExecutionContext.getBindVariableService().setLong("lim", lim);
                    assertSlices(q, true);
                }
            }
            sqlExecutionContext.getBindVariableService().clear();
        });
    }

    @Test
    public void testColumnTopsRunTheJavaFilter() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        config();
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext);
            // the partitions written before the column was added have a column top
            execute("alter table trade add column i2 int");
            execute("insert into trade select ('2026-04-04'::timestamp + x * 10_000_000L), case when x % 50 = 0 then 'L' else 'T' end," +
                    " (x % 100)::double, (x % 7)::int, 'v' || (x % 3), (x % 5)::int from long_sequence(5000)");
            assertSlices("SELECT time, sum(size)" + RUN + " s FROM trade WHERE i2 > 2", true);
            assertSlices("SELECT time, sum(size)" + RUN + " s FROM trade WHERE i2 IS NULL", true);
            assertSlices("SELECT time, sum(size)" + RUN + " s FROM trade WHERE ex = 'L' OR i2 = 1", true);
        });
    }

    @Test
    public void testFilters() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        config();
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext);
            final String[] filters = {
                    "ex = 'L'",
                    "ex = 'T'",
                    "ex = 'NOPE'",
                    "ex <> 'T'",
                    "ex IN ('L', 'Q')",
                    "size > 50 AND i < 4",
                    "size > 50 OR ex = 'L'",
                    "size = 13",
                    "i = 3",
                    "size IS NULL",
                    "time > '2026-04-02T12:00:00' AND ex = 'L'",
                    // a var-size column, which the compiled filter reads through its aux vector
                    "vc = 'v1'",
                    "vc IS NOT NULL AND ex = 'L'",
            };
            for (String filter : filters) {
                assertSlices("SELECT time, sum(size)" + RUN + " s FROM trade WHERE " + filter, false);
                assertSlices("SELECT time, size, count()" + RUN + " c, row_number() OVER () rn, max(i)" + RUN + " m FROM trade WHERE " + filter, false);
                assertSlices("SELECT time, sum(size)" + RUN + " s FROM trade WHERE " + filter + " LIMIT 17", false);
            }
            // the plan shows the compiled filter
            TestUtils.assertContains(plan("SELECT time, sum(size)" + RUN + " s FROM trade WHERE ex = 'L'"), "jit: true");
        });
    }

    @Test
    public void testOnWorkerPool() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        config();
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
                createTrade(engine, ctx);
                for (String filter : new String[]{"ex = 'L'", "ex = 'T'", "size > 50 AND i < 4", "vc = 'v2'"}) {
                    WindowChainProofTest.assertMatchesSerial(engine, ctx, "SELECT time, sum(size)" + RUN + " s, count()" + RUN + " c FROM trade WHERE " + filter);
                }
            }, configuration, LOG);
        });
    }

    // The planner gates the parallel window on the table's rows; the frames an interval leaves
    // of it are known only to the cursor, which computes a walk of fewer than min.rows rows on
    // the query's own thread, as the serial window would.
    @Test
    public void testSmallIntervalRunsSerially() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        config();
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 5_000);
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext);
            final String day = "SELECT time, sum(size)" + RUN + " s FROM trade WHERE time IN '2026-04-02' AND ex <> 'L'";
            final String hours = "SELECT time, sum(size)" + RUN + " s FROM trade WHERE time BETWEEN '2026-04-02T00:00:00' AND '2026-04-02T05:59:59' AND ex <> 'L'";
            final String shards = "SELECT ex, time, s FROM (SELECT ex, time, sum(size) OVER (PARTITION BY ex ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM trade WHERE time BETWEEN '2026-04-02T00:00:00' AND '2026-04-02T05:59:59') ORDER BY ex, time, s";
            final String allShards = "SELECT ex, time, s FROM (SELECT ex, time, sum(size) OVER (PARTITION BY ex ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM trade) ORDER BY ex, time, s";
            for (String q : new String[]{day, hours, shards, allShards}) {
                WindowChainProofTest.assertMatchesSerial(engine, sqlExecutionContext, q);
            }
            // a day is 8640 rows, six hours 2160
            Assert.assertTrue(parallelRounds(day) > 0);
            Assert.assertEquals(0, parallelRounds(hours));
            Assert.assertEquals(0, parallelRounds(shards));
            Assert.assertTrue(parallelRounds(allShards) > 0);
        });
    }

    // The rounds the workers computed for the query's shard or slice walk, 0 when it ran serially.
    private static long parallelRounds(String query) throws Exception {
        sqlExecutionContext.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
            final AsyncWindowRecordCursorFactory async = WindowChainProofTest.findAsyncFactory(factory);
            Assert.assertTrue(query, async.isSliceMode() || async.isShardMode());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                long rows = 0;
                while (cursor.hasNext()) {
                    rows++;
                }
                Assert.assertTrue(rows > 0);
                return async.getShardCursor().getParallelRoundCount();
            }
        } finally {
            sqlExecutionContext.setParallelWindowEnabled(false);
        }
    }

    // the query runs serially and on row slices, with the same rows
    private static void assertSlices(String query, boolean jitOptional) throws Exception {
        sqlExecutionContext.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
            final AsyncWindowRecordCursorFactory async = WindowChainProofTest.findAsyncFactory(factory);
            Assert.assertTrue(query, async.isSliceMode());
        } finally {
            sqlExecutionContext.setParallelWindowEnabled(false);
        }
        final String plan = plan(query);
        if (!jitOptional && !Chars.contains(query, "vc ")) {
            // filters the JIT compiles: the slices run the compiled filter
            TestUtils.assertContains(plan, "jit: true");
        }
        WindowChainProofTest.assertMatchesSerial(engine, sqlExecutionContext, query);
    }

    private static void config() {
        // slices of 1024 rows in frames of up to 1M rows; a table of three partitions of 8640 rows
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 4096);
    }

    /**
     * Table {@code trade}: an unindexed symbol {@code ex}, rare 'L' (1 row in 50) and common 'T';
     * sizes with NULLs, a small INT and a VARCHAR; three DAY partitions.
     */
    private static void createTrade(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        engine.execute("create table trade (time timestamp, ex symbol, size double, i int, vc varchar) timestamp(time) partition by DAY", ctx);
        engine.execute(
                "insert into trade select ('2026-04-01'::timestamp + x * 10_000_000L), case when x % 50 = 0 then 'L' when x % 3 = 0 then 'Q' else 'T' end," +
                        " case when x % 41 = 0 then null else (x % 100)::double + rnd_double() end, (x % 7)::int," +
                        " case when x % 11 = 0 then null else 'v' || (x % 3) end" +
                        " from long_sequence(25920)",
                ctx
        );
    }

    private static String plan(String query) throws Exception {
        sqlExecutionContext.setParallelWindowEnabled(true);
        try {
            final StringSink plan = new StringSink();
            engine.print("explain " + query, plan, sqlExecutionContext);
            return plan.toString();
        } finally {
            sqlExecutionContext.setParallelWindowEnabled(false);
        }
    }
}

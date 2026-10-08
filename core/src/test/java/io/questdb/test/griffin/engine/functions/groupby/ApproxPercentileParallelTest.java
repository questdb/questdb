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


package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * approx_percentile over LONG arguments runs in parallel GROUP BY at every precision (the paged off-heap
 * histogram), and returns exactly what the serial on-heap histograms returned. Every query is run with
 * cairo.sql.parallel.approx.percentile.enabled=false first; its output is the expected output of the
 * parallel run. The values are integers recorded into integer counts, so the comparison is exact.
 */
public class ApproxPercentileParallelTest extends AbstractCairoTest {
    private static final String[] PARALLEL_QUERIES = {
            // not keyed, the TAQ idx 76/77 shape
            "SELECT approx_percentile(i + j, 0.5, 5) AS med FROM tab WHERE g = 'g3'",
            "SELECT approx_percentile(x, 0.5, 5) FROM tab",
            "SELECT approx_percentile(x, 0.0, 3), approx_percentile(x, 1.0, 4), approx_median(x, 5) FROM tab",
            "SELECT approx_percentile(x, 0.999, 5) FROM tab WHERE x > 5000",
            // keyed: few keys and many keys (sharded merge)
            "SELECT g, approx_percentile(x, 0.9, 4), approx_percentile(i + j, 0.25, 5) FROM tab ORDER BY g",
            "SELECT k, approx_percentile(x, 0.5, 5) FROM tab ORDER BY k",
            "SELECT k % 7 AS kk, approx_percentile(wide, 0.75, 5) FROM tab ORDER BY kk",
            // the array form, at every precision class
            "SELECT approx_percentile(x, ARRAY[0.0, 0.1, 0.5, 0.999, 1.0], 5) FROM tab",
            "SELECT g, approx_percentile(x, ARRAY[0.25, 0.5, 0.75]) FROM tab ORDER BY g",
            "SELECT g, approx_percentile(x, ARRAY[0.3, 0.6], 2), approx_percentile(wide, ARRAY[0.3, 0.6], 4) FROM tab ORDER BY g",
            // NULLs: a group of only NULLs, no rows at all
            "SELECT g, approx_percentile(x, 0.5, 5), approx_percentile(x, ARRAY[0.5], 5) FROM tab WHERE g IN ('nulls', 'g1') ORDER BY g",
            "SELECT approx_percentile(x, 0.5, 5), approx_percentile(x, ARRAY[0.5], 3) FROM tab WHERE g = 'none'",
            "SELECT approx_percentile(x, 0.5, 5) FROM tab WHERE g = 'nulls'",
            // SAMPLE BY without FILL is rewritten to a GROUP BY, which now runs in parallel
            "SELECT ts, g, approx_percentile(x, ARRAY[0.5, 0.9], 5), approx_percentile(i, 0.1, 4) FROM tab WHERE g IN ('g1', 'g2') SAMPLE BY 1h ORDER BY ts, g",
    };
    private static final String[] SERIAL_QUERIES = {
            // SAMPLE BY with fills runs serially either way; the values must not change
            "SELECT ts, approx_percentile(x, 0.5, 5) FROM tab WHERE g = 'g2' SAMPLE BY 30m FILL(NULL)",
            "SELECT ts, approx_percentile(x, 0.5, 5) FROM tab WHERE g = 'g2' SAMPLE BY 30m FILL(PREV)",
    };

    @Override
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 4);
        super.setUp();
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, g SYMBOL, i INT, j INT) TIMESTAMP(ts) PARTITION BY DAY");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_APPROX_PERCENTILE_ENABLED, "false");
            assertPlan(
                    "SELECT approx_percentile(i + j, 0.5, 5) FROM tab WHERE g = 'a'",
                    """
                            GroupBy vectorized: false
                              values: [approx_percentile(i+j,0.5)]
                                Async JIT Filter workers: 1
                                  filter: g='a'
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """
            );
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_APPROX_PERCENTILE_ENABLED, "true");
            assertPlan(
                    "SELECT approx_percentile(i + j, 0.5, 5) FROM tab WHERE g = 'a'",
                    """
                            Async JIT Group By workers: 1
                              vectorized: false
                              values: [approx_percentile(i+j,0.5)]
                              filter: g='a'
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """
            );
            assertPlan(
                    "SELECT g, approx_percentile(i, ARRAY[0.5, 0.9], 3) FROM tab",
                    """
                            Async Group By workers: 1
                              keys: [g]
                              values: [approx_percentile(i)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """
            );
        });
    }

    @Test
    public void testNegativeValueError() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        createTable(engine, ctx);
                        for (String query : new String[]{
                                "SELECT approx_percentile(x - 5000, 0.5, 5) FROM tab",
                                "SELECT g, approx_percentile(x - 5000, ARRAY[0.5], 4) FROM tab",
                        }) {
                            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_APPROX_PERCENTILE_ENABLED, "false");
                            final String serialError = error(engine, ctx, query);
                            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_APPROX_PERCENTILE_ENABLED, "true");
                            Assert.assertTrue(plan(engine, ctx, query), plan(engine, ctx, query).contains("Async"));
                            final String parallelError = error(engine, ctx, query);
                            Assert.assertTrue(serialError, serialError.contains("Histogram recorded value cannot be negative."));
                            Assert.assertTrue(parallelError, parallelError.contains("Histogram recorded value cannot be negative."));
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelMatchesSerial() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        createTable(engine, ctx);
                        for (String query : PARALLEL_QUERIES) {
                            assertMatchesSerial(engine, ctx, query, true);
                        }
                        for (String query : SERIAL_QUERIES) {
                            assertMatchesSerial(engine, ctx, query, false);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void assertMatchesSerial(CairoEngine engine, SqlExecutionContext ctx, String query, boolean parallel) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_APPROX_PERCENTILE_ENABLED, "false");
        final String serialPlan = plan(engine, ctx, query);
        final StringSink expected = new StringSink();
        TestUtils.printSql(engine, ctx, query, expected);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_APPROX_PERCENTILE_ENABLED, "true");
        final String parallelPlan = plan(engine, ctx, query);
        if (parallel) {
            Assert.assertFalse(query + '\n' + serialPlan, serialPlan.contains("Async Group By") || serialPlan.contains("Async JIT Group By"));
            Assert.assertTrue(query + '\n' + parallelPlan, parallelPlan.contains("Async Group By") || parallelPlan.contains("Async JIT Group By"));
        }
        // a non-trivial expected output: the comparison must not be vacuous
        Assert.assertTrue(query + '\n' + expected, expected.toString().split("\n").length > 1);
        new QueryAssertion(engine, ctx, () -> {
        }, query)
                .noLeakCheck()
                .sizeMayVary()
                .inferRandomAccess()
                .inferTimestamp()
                .returns(expected.toString());
    }

    private static void createTable(CairoEngine engine, SqlExecutionContext ctx) throws SqlException {
        engine.execute(
                "CREATE TABLE tab (ts TIMESTAMP, g SYMBOL, k INT, i INT, j INT, x LONG, wide LONG) TIMESTAMP(ts) PARTITION BY HOUR",
                ctx
        );
        // 30k rows over 9 hours (many page frames per partition), a 2h hole for the fills,
        // integer sizes with few distinct values (TAQ-like), values over every magnitude, NULLs
        engine.execute(
                """
                        INSERT INTO tab SELECT
                            CASE WHEN x < 10000 THEN x * 1_000_000 ELSE (x + 7000) * 1_000_000 END::timestamp,
                            CASE WHEN x % 13 = 0 THEN 'nulls' ELSE rnd_symbol('g1', 'g2', 'g3', 'g4') END,
                            rnd_int(0, 999, 0),
                            rnd_int(0, 600, 0) * 100,
                            CASE WHEN x % 11 = 0 THEN NULL ELSE rnd_int(0, 600, 0) * 100 END,
                            CASE WHEN x % 13 = 0 OR x % 17 = 0 THEN NULL ELSE rnd_long(0, 20_000_000, 0) END,
                            abs(rnd_long() / 2) / power(2, rnd_int(0, 62, 0))::long
                        FROM long_sequence(30_000)
                        """,
                ctx
        );
    }

    private static void assertPlan(String query, String expected) throws SqlException {
        TestUtils.assertEquals("QUERY PLAN\n" + expected, plan(engine, sqlExecutionContext, query));
    }

    private static String error(CairoEngine engine, SqlExecutionContext ctx, String query) {
        try {
            TestUtils.printSql(engine, ctx, query, new StringSink());
            Assert.fail("expected an error: " + query);
            return null;
        } catch (CairoException | SqlException e) {
            return e.getMessage();
        } catch (Exception e) {
            return e.toString();
        }
    }

    private static String plan(CairoEngine engine, SqlExecutionContext ctx, String query) throws SqlException {
        final StringSink sink = new StringSink();
        engine.print("EXPLAIN " + query, sink, ctx);
        return sink.toString();
    }
}

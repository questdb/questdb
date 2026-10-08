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


package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The parallel WINDOW JOIN INCLUDE PREVAILING finds prevailing rows through per-block summaries
 * shared by its page frames, and skips or memoises the backward scan under a join filter of symbol
 * equalities. The serial factory does neither, so it is the oracle here: non-zero windows, where
 * the in-memory index and the prevailing lookup interact, and join filters of one or two symbol
 * equalities, reversed operands, an extra predicate, an OR, a column with a column top and NULLs on
 * both sides, over native and Parquet slaves, with blocks of one or of several time frames.
 */
public class WindowJoinPrevailingParallelVsSerialTest extends AbstractCairoTest {
    private static final String[] FILTERS = {
            "",
            " AND t.ex = q.ex",
            " AND q.ex = t.ex",
            " AND t.ex = q.ex AND q.bid > 0.3",
            " AND t.ex = q.ex AND t.ex2 = q.ex2",
            " AND (t.ex = q.ex OR q.bid > 0.9)",
            " AND t.ex2 = q.ex2",
            " AND t.ex = q.ex AND t.ex = q.ex2",
    };
    private static final int PAGE_FRAME_ROWS = 100;
    private static final String[] WINDOWS = {
            "CURRENT ROW AND CURRENT ROW",
            "10 MINUTE PRECEDING AND CURRENT ROW",
            "2 HOUR PRECEDING AND 30 MINUTE PRECEDING",
            "5 MINUTE PRECEDING AND 5 MINUTE FOLLOWING",
    };

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, PAGE_FRAME_ROWS);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, PAGE_FRAME_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_ROWS);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, PAGE_FRAME_ROWS);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        super.setUp();
    }

    @Test
    public void testManyJoinableKeysNative() throws Exception {
        // 6000 more keys, each quoted once at the start, make blocks of several time frames
        assertParallelMatchesSerial(false, 6000);
    }

    @Test
    public void testManyJoinableKeysParquet() throws Exception {
        assertParallelMatchesSerial(true, 6000);
    }

    @Test
    public void testNative() throws Exception {
        assertParallelMatchesSerial(false, 0);
    }

    @Test
    public void testParquet() throws Exception {
        assertParallelMatchesSerial(true, 0);
    }

    private void assertSameRows(CairoEngine engine, SqlExecutionContext ctx, String sql, boolean isFullContract) throws Exception {
        final SqlExecutionContextImpl context = (SqlExecutionContextImpl) ctx;
        final StringSink plan = new StringSink();
        final StringSink serial = new StringSink();
        final StringSink parallel = new StringSink();
        context.setParallelWindowJoinEnabled(false);
        try {
            TestUtils.printSql(engine, ctx, "EXPLAIN " + sql, plan);
            Assert.assertFalse("serial plan is parallel: " + plan, plan.toString().contains("Async Window"));
            TestUtils.printSql(engine, ctx, sql, serial);
        } finally {
            context.setParallelWindowJoinEnabled(true);
        }
        TestUtils.printSql(engine, ctx, "EXPLAIN " + sql, plan);
        Assert.assertTrue("parallel plan is serial: " + sql + "\n" + plan, plan.toString().contains("Async Window"));
        TestUtils.printSql(engine, ctx, sql, parallel);
        if (!Chars.equals(serial, parallel)) {
            Assert.fail("parallel differs from serial (seeds are in the log)\n" + sql + "\n" + plan + "\n" + firstDiff(serial, parallel));
        }
        if (isFullContract) {
            // and the rest of the cursor contract, on the parallel factory
            assertQuery(sql).withEngine(engine).withContext(ctx).noLeakCheck().inferRandomAccess().inferTimestamp().returns(serial.toString());
        }
    }

    private static void createTables(CairoEngine engine, SqlExecutionContext ctx, Rnd rnd, boolean parquetSlave, int extraKeys) throws SqlException {
        engine.execute("DROP TABLE IF EXISTS quotes", ctx);
        engine.execute("DROP TABLE IF EXISTS trades", ctx);
        final int k = 5 + rnd.nextInt(60);
        final int dup = 1 + rnd.nextInt(3);
        engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, ex SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        if (extraKeys > 0) {
            // keys that can join but trade once, at the very start: they only widen the summaries
            engine.execute("INSERT INTO quotes SELECT '2023-12-31T23:00:00'::timestamp + x, 'm' || x, 'X0', 0.5 FROM long_sequence(" + extraKeys + ")", ctx);
        }
        // skewed keys (high keys are rare), NULL keys, NULL ex, several rows per timestamp
        engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + (x / " + dup + ") * 4_000_000L, "
                + "CASE WHEN rnd_int(0, 40, 0) = 0 THEN NULL ELSE 'k' || ((rnd_int(0, " + (k - 1) + ", 0) * rnd_int(0, " + (k - 1) + ", 0)) / " + k + ") END, "
                + "CASE WHEN rnd_int(0, 25, 0) = 0 THEN NULL ELSE 'X' || rnd_int(0, 2, 0) END, rnd_double() FROM long_sequence(" + (15000 + rnd.nextInt(15000)) + ")", ctx);
        // rare keys, quoted only early on day 1
        engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 30_000_000L + 1, 'r' || (x % 40), 'X' || (x % 3), rnd_double() FROM long_sequence(120)", ctx);
        // column top: ex2 is added later
        engine.execute("ALTER TABLE quotes ADD COLUMN ex2 SYMBOL", ctx);
        engine.execute("INSERT INTO quotes SELECT '2024-01-03'::timestamp + x * 3_000_000L, 'k' || rnd_int(0, " + (k - 1) + ", 0), "
                + "CASE WHEN rnd_int(0, 25, 0) = 0 THEN NULL ELSE 'X' || rnd_int(0, 2, 0) END, rnd_double(), "
                + "CASE WHEN rnd_int(0, 9, 0) = 0 THEN NULL ELSE 'X' || rnd_int(0, 3, 0) END FROM long_sequence(10000)", ctx);
        // same-key ties
        engine.execute("INSERT INTO quotes SELECT ts, sym, ex, -bid, ex2 FROM quotes WHERE bid < 0.003", ctx);
        if (parquetSlave) {
            engine.execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'", ctx);
        }
        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, ex SYMBOL, px DOUBLE, ex2 SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        if (extraKeys > 0) {
            engine.execute("INSERT INTO trades SELECT '2023-12-31'::timestamp + x, 'm' || x, 'X0', 0.0, NULL FROM long_sequence(" + extraKeys + ")", ctx);
        }
        // trades: k* (some never quoted), r*, z* (never quoted), NULLs
        engine.execute("INSERT INTO trades SELECT '2024-01-01'::timestamp + x * 37_000_000L + rnd_int(0, 3, 0), "
                + "CASE WHEN rnd_int(0, 30, 0) = 0 THEN NULL WHEN rnd_int(0, 5, 0) = 0 THEN 'r' || rnd_int(0, 45, 0) "
                + "WHEN rnd_int(0, 11, 0) = 0 THEN 'z' || rnd_int(0, 3, 0) ELSE 'k' || rnd_int(0, " + (k + 3) + ", 0) END, "
                + "CASE WHEN rnd_int(0, 20, 0) = 0 THEN NULL ELSE 'X' || rnd_int(0, 3, 0) END, rnd_double() * 10, "
                + "CASE WHEN rnd_int(0, 6, 0) = 0 THEN NULL ELSE 'X' || rnd_int(0, 4, 0) END FROM long_sequence(" + (2000 + rnd.nextInt(2000)) + ")", ctx);
        // trades exactly at quote timestamps
        engine.execute("INSERT INTO trades SELECT ts, sym, ex, 9.0, ex2 FROM quotes WHERE bid > 0.9995", ctx);
    }

    private static String firstDiff(CharSequence expected, CharSequence actual) {
        final String[] a = expected.toString().split("\n");
        final String[] b = actual.toString().split("\n");
        for (int i = 0, n = Math.max(a.length, b.length); i < n; i++) {
            final String x = i < a.length ? a[i] : "<none>";
            final String y = i < b.length ? b[i] : "<none>";
            if (!x.equals(y)) {
                return "rows " + a.length + " vs " + b.length + ", line " + i + "\n serial  : " + x + "\n parallel: " + y;
            }
        }
        return "";
    }

    private void assertParallelMatchesSerial(boolean parquetSlave, int extraKeys) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                ((SqlExecutionContextImpl) ctx).setRandom(new Rnd(rnd.nextLong(), rnd.nextLong()));
                createTables(engine, ctx, rnd, parquetSlave, extraKeys);
                for (String window : WINDOWS) {
                    for (String filter : FILTERS) {
                        final String masterFilter = rnd.nextBoolean() ? "" : " WHERE t.px > 5";
                        assertSameRows(engine, ctx, "SELECT t.ts, t.sym, t.ex, t.px, last(q.bid) lb, first(q.bid) fb, count() c, sum(q.bid) sb, last(q.ts) lt, last(q.ex) lex "
                                + "FROM trades t WINDOW JOIN quotes q ON (t.sym = q.sym" + filter + ") RANGE BETWEEN " + window
                                + " INCLUDE PREVAILING" + masterFilter, false);
                    }
                    // vectorized aggregates
                    assertSameRows(engine, ctx, "SELECT t.ts, t.sym, sum(q.bid) sb, count() c, last(q.bid) lb, first(q.bid) fb "
                            + "FROM trades t WINDOW JOIN quotes q ON (t.sym = q.sym) RANGE BETWEEN " + window + " INCLUDE PREVAILING", true);
                }
            }, configuration, LOG);
        });
    }
}

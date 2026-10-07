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
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A WINDOW JOIN over {@code [t.ts, t.ts]} with INCLUDE PREVAILING and {@code last()} of the slave
 * columns is an ASOF JOIN: the last slave row at or before the master row, ties in storage order,
 * NULL when there is none. The prevailing row comes from a backward walk of the slave that the
 * page frames of the master share, per block of slave time frames; these tests check it against
 * ASOF on data that sends the walk through many blocks - rare keys whose last row is partitions
 * back, keys with no row at all, NULL keys, ties - with one key and with a join filter as a second
 * key, on native and Parquet slaves.
 */
public class WindowJoinPrevailingAsOfTest extends AbstractCairoTest {
    private static final int PAGE_FRAME_MAX_ROWS = 100;

    @Override
    @Before
    public void setUp() {
        // small master page frames and small slave time frames: many of both
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        super.setUp();
    }

    @Test
    public void testManyMasterKeysMakeMultiFrameBlocks() throws Exception {
        // 4000 master symbols x ~600 slave frames is more than the summaries' entry budget, so a
        // block spans several frames and the walk starts mid-block
        assertEquivalent(false, 4000);
    }

    @Test
    public void testNativeSlave() throws Exception {
        assertEquivalent(false, 0);
    }

    @Test
    public void testParquetSlave() throws Exception {
        assertEquivalent(true, 0);
    }

    private static void assertNotVacuous(CairoEngine engine, SqlExecutionContext ctx, String sql) throws SqlException {
        final StringSink sink = new StringSink();
        TestUtils.printSql(engine, ctx, "SELECT count(), count(bid) FROM (" + sql + ")", sink);
        final String[] counts = sink.toString().split("\n")[1].split("\t");
        final long rows = Long.parseLong(counts[0]);
        final long matched = Long.parseLong(counts[1]);
        Assert.assertTrue("no rows: " + sql, rows > 0);
        Assert.assertTrue("no matches: " + sql, matched > 0);
        Assert.assertTrue("every row matched, the no-prior-row case is not exercised: " + sql, matched < rows);
    }

    private void assertPlanContains(CairoEngine engine, SqlExecutionContext ctx, String sql, String expected) throws Exception {
        assertQuery(sql).withEngine(engine).withContext(ctx).noLeakCheck().assertsPlanContaining(expected);
    }

    private void assertEquivalent(boolean parquetSlave, int extraMasterSymbols) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        createTables(engine, ctx, parquetSlave, extraMasterSymbols);
                        for (String masterFilter : new String[]{"", " WHERE t.px > 5", " WHERE t.sym IN ('f1', 'r3', 'r7', 'z1', 'late')"}) {
                            // one key
                            final String asof = "SELECT t.ts, t.sym, t.px, q.bid, q.cond, q.ts qts FROM trades t ASOF JOIN quotes q ON (sym)"
                                    + masterFilter;
                            final String wj = "SELECT ts, sym, px, bid, cond, qts FROM (SELECT t.ts ts, t.sym sym, t.px px, "
                                    + "last(q.bid) bid, last(q.cond) cond, last(q.ts) qts FROM trades t WINDOW JOIN quotes q ON (t.sym = q.sym) "
                                    + "RANGE BETWEEN CURRENT ROW AND CURRENT ROW INCLUDE PREVAILING" + masterFilter + ")";
                            assertPlanContains(engine, ctx, wj, "Async Window Fast Join");
                            TestUtils.assertSqlCursors(engine, ctx, asof, wj, LOG);
                            assertNotVacuous(engine, ctx, asof);
                            // vectorized aggregates: no symbol column
                            final String asofVect = "SELECT t.ts, t.sym, q.bid, q.ts qts FROM trades t ASOF JOIN quotes q ON (sym)" + masterFilter;
                            final String wjVect = "SELECT ts, sym, bid, qts FROM (SELECT t.ts ts, t.sym sym, "
                                    + "last(q.bid) bid, last(q.ts) qts FROM trades t WINDOW JOIN quotes q ON (t.sym = q.sym) "
                                    + "RANGE BETWEEN CURRENT ROW AND CURRENT ROW INCLUDE PREVAILING" + masterFilter + ")";
                            TestUtils.assertSqlCursors(engine, ctx, asofVect, wjVect, LOG);
                            // two keys: ex is a join filter on top of the symbol key
                            final String asof2 = "SELECT t.ts, t.sym, t.ex, q.bid, q.ts qts FROM trades t ASOF JOIN quotes q ON (sym, ex)" + masterFilter;
                            final String wj2 = "SELECT ts, sym, ex, bid, qts FROM (SELECT t.ts ts, t.sym sym, t.ex ex, "
                                    + "last(q.bid) bid, last(q.ts) qts FROM trades t WINDOW JOIN quotes q ON (t.sym = q.sym AND t.ex = q.ex) "
                                    + "RANGE BETWEEN CURRENT ROW AND CURRENT ROW INCLUDE PREVAILING" + masterFilter + ")";
                            assertPlanContains(engine, ctx, wj2, "Async Window Fast Join");
                            TestUtils.assertSqlCursors(engine, ctx, asof2, wj2, LOG);
                            assertNotVacuous(engine, ctx, asof2);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void createTables(CairoEngine engine, SqlExecutionContext ctx, boolean parquetSlave, int extraMasterSymbols) throws SqlException {
        // quotes: three days, ~600 time frames. f0..f9 quote all the time; r0..r49 only in the
        // first hours of day 1; 'late' only on day 3; some rows have a NULL symbol; two rows per
        // timestamp, and copies of some rows at the same timestamp (same-key ties)
        engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, ex SYMBOL, bid DOUBLE, cond SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + (x / 2) * 8_640_000L, "
                + "CASE WHEN x % 97 = 0 THEN NULL ELSE 'f' || (x % 10) END, 'X' || (x % 3), x::double, "
                + "CASE WHEN x % 7 = 0 THEN NULL ELSE 'c' || (x % 5) END FROM long_sequence(60000)", ctx);
        engine.execute("INSERT INTO quotes SELECT ts, sym, ex, -bid, cond FROM quotes WHERE bid % 499 = 0", ctx);
        engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 60_000_000L + 3, 'r' || (x % 50), 'X' || (x % 3), x::double + 0.5, 'rc' "
                + "FROM long_sequence(180)", ctx);
        engine.execute("INSERT INTO quotes VALUES ('2024-01-03T20:00:00.000000Z', 'late', 'X0', 7.5, 'lc')", ctx);
        if (parquetSlave) {
            engine.execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'", ctx);
        }
        // trades: the same three days; f*, r*, z* (never quoted), 'late', NULL
        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, ex SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        if (extraMasterSymbols > 0) {
            // symbols that only grow the master's symbol table, at the very start
            engine.execute("INSERT INTO trades SELECT '2023-12-31'::timestamp + x, 'm' || x, 'X0', 0.0 FROM long_sequence(" + extraMasterSymbols + ")", ctx);
        }
        engine.execute("INSERT INTO trades SELECT '2024-01-01'::timestamp + x * 43_200_000L + (x % 3), "
                + "CASE WHEN x % 31 = 0 THEN NULL WHEN x % 5 = 0 THEN 'r' || (x % 50) WHEN x % 11 = 0 THEN 'z' || (x % 3) "
                + "WHEN x % 13 = 0 THEN 'late' ELSE 'f' || (x % 10) END, 'X' || (x % 4), (x % 10)::double FROM long_sequence(6000)", ctx);
        // a trade exactly at a quote tie
        engine.execute("INSERT INTO trades VALUES ('2024-01-02T00:00:00.000000Z', 'f0', 'X0', 9.0)", ctx);
    }
}

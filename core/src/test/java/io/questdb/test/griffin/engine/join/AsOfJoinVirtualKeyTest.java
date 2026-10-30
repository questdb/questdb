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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.AsyncAsOfJoinAtom;
import io.questdb.griffin.engine.join.AsyncAsOfJoinRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.engine.join.AsyncAsOfJoinTest.assertParallel;
import static io.questdb.test.griffin.engine.join.AsyncAsOfJoinTest.assertPlanContains;

/**
 * A non-keyed ASOF JOIN whose two sides are filtered by the same SYMBOL equality - the "virtual
 * key" ({@code cairo.sql.asof.join.virtual.key.enabled}) - runs as the keyed Async AsOf Join and
 * returns exactly what {@code asof_linear} returns: ties in storage order, NULL as the value, a
 * value the slave never holds, a rare value, TOLERANCE, projections that drop the key columns, an
 * extra master conjunct, bind variables, and Parquet and column-top layouts; self-joins, duplicated
 * and reordered projections, chained joins, an outer ORDER BY / LIMIT, and a scalar sub-query in the
 * master's filter on a cached factory re-executed after the sub-query's table changed.
 */
public class AsOfJoinVirtualKeyTest extends AbstractCairoTest {
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final String[] VALUES = {"'f2'", "'r4'", "'z1'", "'late'", "null", "'f7'"};

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        super.setUp();
        AsyncAsOfJoinRecordCursorFactory.WALK_MODE = AsyncAsOfJoinRecordCursorFactory.WALK_AUTO;
        AsyncAsOfJoinAtom.KEYS_MODE = AsyncAsOfJoinAtom.KEYS_AUTO;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        AsyncAsOfJoinRecordCursorFactory.WALK_MODE = AsyncAsOfJoinRecordCursorFactory.WALK_AUTO;
        AsyncAsOfJoinAtom.KEYS_MODE = AsyncAsOfJoinAtom.KEYS_AUTO;
        super.tearDown();
    }

    @Test
    public void testCachedFactoryWithScalarSubQueryInMasterFilter() throws Exception {
        // the master's filter binds a scalar sub-query in its JIT filter; the factory is re-executed
        // after the sub-query's table changes, and every execution equals a fresh linear plan
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        AsyncAsOfJoinTest.createTables(engine, ctx, new Rnd(42, 4242), false, false, false);
                        final String body = "t.ts, t.px, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2' AND px > " +
                                "(SELECT max(px) - 100 FROM trades WHERE ex = 'Z9')) t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') q";
                        assertPlanContains(engine, ctx, "SELECT " + body, "virtual key");
                        try (RecordCursorFactory factory = engine.select("SELECT " + body, ctx)) {
                            final int[] rowCounts = new int[3];
                            for (int round = 0; round < 3; round++) {
                                final String actual = print(factory, ctx);
                                final String expected;
                                try (RecordCursorFactory linear = engine.select("SELECT /*+ asof_linear(t q) */ " + body, ctx)) {
                                    expected = print(linear, ctx);
                                }
                                TestUtils.assertEquals("round " + round, expected, actual);
                                rowCounts[round] = actual.split("\n").length;
                                engine.execute("INSERT INTO trades VALUES ('2025-01-01T00:00:00.000000Z', 'f0', 'Z9', " + (round == 0 ? 50 : 103) + ".0)", ctx);
                            }
                            // the sub-query's value moved the rows: no sub-query row, then a low and a high max
                            Assert.assertNotEquals(rowCounts[0], rowCounts[1]);
                            Assert.assertNotEquals(rowCounts[1], rowCounts[2]);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testColumnTops() throws Exception {
        assertSeeds(2, false, false, true);
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE quote (time TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(time) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO quote VALUES ('2024-01-01T00:00:01', 'A', 1.0), ('2024-01-01T00:00:02', 'B', 2.0), ('2024-01-01T00:00:03', 'A', 3.0), " +
                    "('2024-01-01T00:00:03', 'B', 4.0), ('2024-01-01T00:00:03', 'A', 5.0), ('2024-01-01T00:00:05', 'A', 6.0)");
            execute("CREATE TABLE trade (time TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(time) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO trade VALUES ('2024-01-01T00:00:00', 'A', 10.0), ('2024-01-01T00:00:02', 'A', 11.0), ('2024-01-01T00:00:03', 'A', 12.0), " +
                    "('2024-01-01T00:00:04', 'B', 13.0), ('2024-01-01T00:00:06', 'A', 14.0)");
            // neither side projects the key; the tie at 00:00:03 takes the last A row
            final String query = "SELECT /*+ asof_parallel(t q) */ t.time, t.price, q.bid FROM (SELECT time, price FROM trade WHERE sym = 'A') t " +
                    "ASOF JOIN (SELECT time, bid FROM quote WHERE sym = 'A') q";
            assertQuery(query)
                    .withPlan("""
                            SelectedRecord
                                SelectedRecord
                                    Async AsOf Join workers: 1
                                      symbol: sym=sym
                                      select: hint, virtual key
                                      master filter: sym='A'
                                      slave key filter: sym='A'
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trade
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: quote
                            """)
                    .noRandomAccess()
                    .timestamp("time")
                    .returns("""
                            time\tprice\tbid
                            2024-01-01T00:00:00.000000Z\t10.0\tnull
                            2024-01-01T00:00:02.000000Z\t11.0\t1.0
                            2024-01-01T00:00:03.000000Z\t12.0\t5.0
                            2024-01-01T00:00:06.000000Z\t14.0\t6.0
                            """);
            // the feature off: the serial non-keyed join, the same rows
            setProperty(PropertyKey.CAIRO_SQL_ASOF_JOIN_VIRTUAL_KEY_ENABLED, "false");
            assertQuery(query)
                    .withPlanContaining("Filtered AsOf Join")
                    .withPlanNotContaining("Async AsOf Join")
                    .noRandomAccess()
                    .timestamp("time")
                    .returns("""
                            time\tprice\tbid
                            2024-01-01T00:00:00.000000Z\t10.0\tnull
                            2024-01-01T00:00:02.000000Z\t11.0\t1.0
                            2024-01-01T00:00:03.000000Z\t12.0\t5.0
                            2024-01-01T00:00:06.000000Z\t14.0\t6.0
                            """);
        });
    }

    @Test
    public void testNative() throws Exception {
        assertSeeds(4, false, false, false);
    }

    @Test
    public void testNotEligible() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        AsyncAsOfJoinTest.createTables(engine, ctx, new Rnd(), false, false, false);
                        final String[] serial = {
                                // the slave filter is more than the key equality
                                "SELECT t.ts, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2' AND bid > 5) q",
                                // different values
                                "SELECT t.ts, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f3') q",
                                // the master equality is not a conjunct
                                "SELECT t.ts, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2' OR px > 5) t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') q",
                                // an unfiltered master
                                "SELECT t.ts, q.bid FROM trades t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') q",
                                // a slave filter on a column that is not a SYMBOL
                                "SELECT t.ts, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE st = 'f2') q",
                                // not an equality: the sides do not share one value
                                "SELECT t.ts, q.bid FROM (SELECT ts, px FROM trades WHERE sym != 'f2') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym != 'f2') q",
                                "SELECT t.ts, q.bid FROM (SELECT ts, px FROM trades WHERE sym IN ('f2', 'f3')) t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym IN ('f2', 'f3')) q",
                                // the serial hint is kept
                                "SELECT /*+ asof_linear(t q) */ t.ts, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') q",
                        };
                        for (String sql : serial) {
                            assertParallel(engine, ctx, sql, false);
                            final String linear = sql.contains("asof_linear") ? sql : sql.replace("SELECT t.ts", "SELECT /*+ asof_linear(t q) */ t.ts");
                            TestUtils.assertSqlCursors(engine, ctx, linear, sql, LOG);
                        }
                        // eligible, then switched off
                        final String eligible = "SELECT t.ts, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') q";
                        assertParallel(engine, ctx, eligible, true);
                        assertPlanContains(engine, ctx, eligible, "select: auto, virtual key");
                        node1.setProperty(PropertyKey.CAIRO_SQL_ASOF_JOIN_VIRTUAL_KEY_ENABLED, "false");
                        assertParallel(engine, ctx, eligible, false);
                        node1.setProperty(PropertyKey.CAIRO_SQL_ASOF_JOIN_VIRTUAL_KEY_ENABLED, "true");
                        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_ASOF_JOIN_ENABLED, "false");
                        assertParallel(engine, ctx, eligible, false);
                        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_ASOF_JOIN_ENABLED, "true");
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParquet() throws Exception {
        assertSeeds(2, true, true, false);
    }

    @Test
    public void testShapes() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        AsyncAsOfJoinTest.createTables(engine, ctx, new Rnd(42, 4242), false, false, false);
                        final String[] bodies = {
                                // self-joins, the quotes case with ties of the same key
                                "t.ts, t.px, q.px qpx, q.ex qex FROM (SELECT ts, px, ex FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ts, px, ex FROM trades WHERE sym = 'f2') q",
                                "t.ts, t.bid, q.bid qb FROM (SELECT ts, bid FROM quotes WHERE sym = 'f3') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f3') q",
                                "t.ts, t.bid, q.bid qb FROM (SELECT ts, bid FROM quotes WHERE sym = null) t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = null) q",
                                // a self-join with TOLERANCE
                                "t.ts, t.bid, q.bid qb FROM (SELECT ts, bid FROM quotes WHERE sym = 'f3') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f3') q TOLERANCE 3s",
                                // duplicated and reordered columns: cross indexes that are not the identity
                                "t.ts, t.px, t.p2, q.bid, q.b2 FROM (SELECT ts, px, px p2 FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ts, bid, bid b2 FROM quotes WHERE sym = 'f2') q",
                                "t.ex, t.px, t.ts, q.ex qex, q.bid, q.ts qts FROM (SELECT ex, px, ts FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ex, bid, ts FROM quotes WHERE sym = 'f2') q",
                                "t.px, t.sym, t.ts, q.bid, q.sym qs FROM (SELECT px, sym, ts, sym s2 FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT sym, bid, ts FROM quotes WHERE sym = 'f2') q",
                                // the master's equality on another SYMBOL column holding the value
                                "t.ts, t.px, q.bid FROM (SELECT ts, px FROM trades WHERE ex = 'X1' AND sym = 'f2') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE ex = 'X1') q",
                                // an interval on either side
                                "t.ts, t.px, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f4' AND ts IN '2024-01-02') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f4') q",
                                "t.ts, t.px, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f4') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f4' AND ts > '2024-01-02') q",
                                // a scalar sub-query in the master's filter (bound in its JIT filter)
                                "t.ts, t.px, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2' AND px > (SELECT avg(px) FROM trades)) t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') q",
                                // IN (sub-query) on the key in the master's filter
                                "t.ts, t.px, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2' AND sym IN (SELECT sym FROM quotes WHERE bid > 5)) t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') q",
                                // an outer filter, an outer ORDER BY ... DESC LIMIT
                                "t.ts, t.px, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') q WHERE q.bid > 1000",
                                "t.ts, t.px, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') q ORDER BY t.ts DESC LIMIT 7",
                                // two ASOF joins in a chain
                                "t.ts, q.bid, r.bid rb FROM (SELECT ts, px FROM trades WHERE sym = 'f2') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') q ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f2') r",
                                // an aggregate over the join
                                "count(), sum(q.bid), min(q.ts) FROM (SELECT ts, px FROM trades WHERE sym = 'f5') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'f5') q",
                                // an escaped quote, a value on neither side
                                "t.ts, q.bid FROM (SELECT ts, px FROM trades WHERE sym = 'o''k') t ASOF JOIN (SELECT ts, bid FROM quotes WHERE sym = 'o''k') q",
                        };
                        for (String body : bodies) {
                            final String sql = "SELECT " + body;
                            assertPlanContains(engine, ctx, sql, "virtual key");
                            TestUtils.assertSqlCursors(engine, ctx, "SELECT /*+ asof_linear(t q) asof_linear(t r) */ " + body, sql, LOG);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void assertForms(CairoEngine engine, SqlExecutionContext ctx, String value, boolean expectParallel) throws Exception {
        final String[] forms = {
                // neither side projects the key
                "t.ts, t.px, t.ex, q.bid, q.ex qex, q.bsz, q.v, q.l FROM (SELECT ts, px, ex FROM trades WHERE sym = $V) t " +
                        "ASOF JOIN (SELECT ts, bid, ex, bsz, v, l FROM quotes WHERE sym = $V) q",
                // both project it, in another position
                "t.ts, t.sym, q.sym qsym, q.bid FROM (SELECT px, sym, ts FROM trades WHERE sym = $V) t " +
                        "ASOF JOIN (SELECT bid, ts, sym FROM quotes WHERE sym = $V) q",
                // no projection at all, an extra master conjunct, the value on the left
                "t.ts, t.px, q.bid, q.u FROM (SELECT * FROM trades WHERE px > 3 AND sym = $V) t " +
                        "ASOF JOIN (SELECT * FROM quotes WHERE $V = sym) q",
                // TOLERANCE
                "t.ts, q.bid, q.c FROM (SELECT ts, px FROM trades WHERE sym = $V) t " +
                        "ASOF JOIN (SELECT ts, bid, c FROM quotes WHERE sym = $V) q TOLERANCE 10m",
                // a different SYMBOL column on each side, holding the same value
                "t.ts, q.bid FROM (SELECT ts, px FROM trades WHERE sym = $V) t " +
                        "ASOF JOIN (SELECT ts, bid FROM quotes WHERE cond = $V) q",
        };
        for (String form : forms) {
            final String body = form.replace("$V", value);
            final String sql = "SELECT " + body;
            assertParallel(engine, ctx, sql, expectParallel);
            TestUtils.assertSqlCursors(engine, ctx, "SELECT /*+ asof_linear(t q) */ " + body, sql, LOG);
            TestUtils.assertSqlCursors(engine, ctx, "SELECT /*+ asof_linear(t q) */ " + body, "SELECT /*+ asof_parallel(t q) */ " + body, LOG);
            TestUtils.assertSqlCursors(engine, ctx, "SELECT count() FROM (SELECT /*+ asof_linear(t q) */ " + body + ")", "SELECT count() FROM (" + sql + ")", LOG);
            TestUtils.assertSqlCursors(engine, ctx, "SELECT /*+ asof_linear(t q) */ " + body + " LIMIT 5, 40", sql + " LIMIT 5, 40", LOG);
        }
    }

    private static String print(RecordCursorFactory factory, SqlExecutionContext ctx) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
        return sink.toString();
    }

    private void assertSeeds(int seeds, boolean parquetSlave, boolean parquetMaster, boolean columnTops) throws Exception {
        final Rnd seedRnd = TestUtils.generateRandom(LOG);
        for (int s = 0; s < seeds; s++) {
            final long s0 = seedRnd.nextLong();
            final long s1 = seedRnd.nextLong();
            LOG.info().$("seed [s0=").$(s0).$(", s1=").$(s1).I$();
            assertMemoryLeak(() -> {
                final WorkerPool pool = new TestWorkerPool(4);
                TestUtils.execute(
                        pool,
                        (engine, compiler, ctx) -> {
                            engine.execute("DROP TABLE IF EXISTS quotes", ctx);
                            engine.execute("DROP TABLE IF EXISTS trades", ctx);
                            AsyncAsOfJoinTest.createTables(engine, ctx, new Rnd(s0, s1), parquetSlave, parquetMaster, columnTops);
                            for (String value : VALUES) {
                                assertForms(engine, ctx, value, true);
                            }
                            // a bind variable on both sides is the same value, but the parallel join
                            // takes a slave key filter only when it is deterministic, which a bind
                            // variable is not: the serial join, with the same rows
                            ctx.getBindVariableService().clear();
                            ctx.getBindVariableService().setStr("v", "f3");
                            assertForms(engine, ctx, ":v", false);
                            ctx.getBindVariableService().setStr("v", "r2");
                            assertForms(engine, ctx, ":v", false);
                            ctx.getBindVariableService().clear();
                        },
                        configuration,
                        LOG
                );
            });
        }
    }
}

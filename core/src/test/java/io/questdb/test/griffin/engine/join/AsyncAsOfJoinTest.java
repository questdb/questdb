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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.AsyncAsOfJoinAtom;
import io.questdb.griffin.engine.join.AsyncAsOfJoinRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Async AsOf Join against the serial ASOF JOIN algorithms, on seeded random data that covers ties in
 * storage order, keys with no prior row, NULL keys, keys the slave never holds, lookback across
 * partitions and frames, TOLERANCE, master filters (compiled and Java), slave filters on the key,
 * projections, LIMIT, column tops and Parquet partitions on either side.
 */
public class AsyncAsOfJoinTest extends AbstractCairoTest {
    private static final String[] MASTER_FILTERS = {
            "",
            " WHERE t.px > 5",
            " WHERE t.sym IN ('f1', 'r3', 'r7', 'z1', 'late')",
            " WHERE t.px > 3 OR t.sym = 'r1'",
            " WHERE length(t.sym) > 2",
            " WHERE t.sym = 'f2'",
            " WHERE t.sym = 'r4'",
    };
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final String[] SERIAL_HINTS = {"asof_linear", "asof_dense"};
    private static final int[] WALK_MODES = {
            AsyncAsOfJoinRecordCursorFactory.WALK_ALWAYS,
            AsyncAsOfJoinRecordCursorFactory.WALK_NEVER,
            AsyncAsOfJoinRecordCursorFactory.WALK_AUTO
    };

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
        AsyncAsOfJoinRecordCursorFactory.WALK_MODE = AsyncAsOfJoinRecordCursorFactory.WALK_AUTO;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        AsyncAsOfJoinRecordCursorFactory.WALK_MODE = AsyncAsOfJoinRecordCursorFactory.WALK_AUTO;
        super.tearDown();
    }

    @Test
    public void testColumnTops() throws Exception {
        assertFuzz(3, false, false, true);
    }

    @Test
    public void testChoice() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL INDEX, ex SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 1_000_000L, 'f' || (x % 10), 'X', x FROM long_sequence(100000)", ctx);
                        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, ex SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        engine.execute("INSERT INTO trades SELECT '2024-01-01'::timestamp + x * 10_000_000L, 'f' || (x % 12), 'X', x FROM long_sequence(9000)", ctx);
                        final String join = " t.ts, t.sym, q.bid FROM trades t ASOF JOIN quotes q ON (sym)";
                        // no hint: the serial default (Dense) is replaced
                        assertPlanContains(engine, ctx, "SELECT" + join, "Async AsOf Join workers: 4\n      symbol: sym=sym\n      select: auto");
                        // a confidently small master keeps the #7423 choice: the index scan
                        assertPlanContains(engine, ctx, "SELECT" + join + " WHERE t.ts IN '2024-01-01T00:00:10'", "AsOf Join Indexed Scan");
                        // ... unless the hint asks
                        assertPlanContains(engine, ctx, "SELECT /*+ asof_parallel(t q) */" + join + " WHERE t.ts IN '2024-01-01T00:00:10'", "select: hint");
                        // every other algorithm hint is kept
                        assertPlanContains(engine, ctx, "SELECT /*+ asof_linear(t q) */" + join, "AsOf Join Light");
                        assertPlanContains(engine, ctx, "SELECT /*+ asof_dense(t q) */" + join, "AsOf Join Dense Single Symbol");
                        assertPlanContains(engine, ctx, "SELECT /*+ asof_fast(t q) */" + join, "AsOf Join Fast");
                        assertPlanContains(engine, ctx, "SELECT /*+ asof_memoized(t q) */" + join, "AsOf Join Memoized Scan");
                        assertPlanContains(engine, ctx, "SELECT /*+ asof_index(t q) */" + join, "AsOf Join Indexed Scan");
                        // two keys, a non-symbol key, no key: serial
                        assertPlanContains(engine, ctx, "SELECT /*+ asof_parallel(t q) */ t.ts, q.bid FROM trades t ASOF JOIN quotes q ON (sym, ex)", "AsOf Join Dense Dual Symbol");
                        assertParallel(engine, ctx, "SELECT /*+ asof_parallel(t q) */ t.ts, q.bid FROM trades t ASOF JOIN (SELECT ts, sym, bid, bid::long bl FROM quotes) q ON (sym) WHERE t.px::long = q.bl", false);
                        assertParallel(engine, ctx, "SELECT /*+ asof_parallel(t q) */ t.ts, q.bid FROM trades t ASOF JOIN quotes q", false);
                        // a slave filter on a column other than the key: serial
                        assertParallel(engine, ctx, "SELECT /*+ asof_parallel(t q) */" + join.replace("quotes q", "(SELECT * FROM quotes WHERE bid > 5) q"), false);
                        // a master that cannot give page frames: serial
                        assertParallel(engine, ctx, "SELECT /*+ asof_parallel(t q) */" + join.replace("trades t", "(SELECT * FROM trades ORDER BY ts LIMIT 100) t"), false);
                        // switched off
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_ASOF_JOIN_ENABLED, "false");
                        assertParallel(engine, ctx, "SELECT /*+ asof_parallel(t q) */" + join, false);
                        assertParallel(engine, ctx, "SELECT" + join, false);
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_ASOF_JOIN_ENABLED, "true");
                        assertParallel(engine, ctx, "SELECT" + join, true);
                        // the window join machinery switched off for the context switches it off too
                        ctx.setParallelWindowJoinEnabled(false);
                        try {
                            assertParallel(engine, ctx, "SELECT /*+ asof_parallel(t q) */" + join, false);
                        } finally {
                            ctx.setParallelWindowJoinEnabled(true);
                        }
                    },
                    configuration,
                    LOG
            );
        });
        // one worker: no automatic choice, the hint still applies
        assertMemoryLeak(() -> {
            execute("CREATE TABLE quotes1 (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO quotes1 SELECT '2024-01-01'::timestamp + x * 1_000_000L, 'f' || (x % 10), x FROM long_sequence(1000)");
            execute("CREATE TABLE trades1 (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO trades1 SELECT '2024-01-01'::timestamp + x * 10_000_000L, 'f' || (x % 12), x FROM long_sequence(90)");
            assertParallel(engine, sqlExecutionContext, "SELECT t.ts, q.bid FROM trades1 t ASOF JOIN quotes1 q ON (sym)", false);
            assertParallel(engine, sqlExecutionContext, "SELECT /*+ asof_parallel(t q) */ t.ts, q.bid FROM trades1 t ASOF JOIN quotes1 q ON (sym)", true);
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT /*+ asof_linear(t q) */ t.ts, t.sym, q.bid, q.ts FROM trades1 t ASOF JOIN quotes1 q ON (sym)",
                    "SELECT /*+ asof_parallel(t q) */ t.ts, t.sym, q.bid, q.ts FROM trades1 t ASOF JOIN quotes1 q ON (sym)",
                    LOG
            );
        });
    }

    @Test
    public void testWalkOrSpanScan() throws Exception {
        // the walk serves frames with few master rows against long spans, and is given up for the
        // span scan where every slave row's key joins and the walk would read about as many rows
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 10_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 100_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 10_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100_000);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 100_000L, CASE WHEN x % 2 = 0 THEN 'k0' ELSE 'k' || (x % 400) END, x "
                                + "FROM long_sequence(400000)", ctx);
                        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        engine.execute("INSERT INTO trades SELECT '2024-01-01'::timestamp + x * 20_000L + 7, 'k' || ((x * 7) % 400), x FROM long_sequence(2000000)", ctx);
                        final String selective = "SELECT /*+ asof_parallel(t q) */ t.ts, t.sym, q.bid FROM trades t ASOF JOIN quotes q ON (sym) WHERE t.sym = 'k0'";
                        final String dense = "SELECT /*+ asof_parallel(t q) */ t.ts, t.sym, q.bid FROM trades t ASOF JOIN quotes q ON (sym)";
                        try (RecordCursorFactory factory = engine.select(selective, ctx)) {
                            drain(factory, ctx);
                            final AsyncAsOfJoinAtom atom = findAtom(factory);
                            Assert.assertTrue("the walk must serve a selective master", atom.getStatFramesWalk() > 0);
                            Assert.assertEquals(0, atom.getStatWalkAborts());
                        }
                        try (RecordCursorFactory factory = engine.select(dense, ctx)) {
                            drain(factory, ctx);
                            final AsyncAsOfJoinAtom atom = findAtom(factory);
                            Assert.assertTrue("the walk must give up on a dense master", atom.getStatWalkAborts() > 0);
                            Assert.assertTrue(atom.getStatFramesSpan() > 0);
                        }
                        TestUtils.assertSqlCursors(engine, ctx, selective.replace("asof_parallel", "asof_linear"), selective, LOG);
                        TestUtils.assertSqlCursors(engine, ctx, dense.replace("asof_parallel", "asof_dense"), dense, LOG);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void drain(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
        }
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        assertPlan(
                                engine,
                                ctx,
                                "SELECT /*+ asof_parallel(t q) */ t.ts, t.sym, q.bid FROM trades t ASOF JOIN quotes q ON (sym) TOLERANCE 1m WHERE t.px > 1",
                                """
                                        SelectedRecord
                                            Async AsOf Join workers: 4
                                              symbol: sym=sym
                                              select: hint
                                              tolerance: 60000000
                                              master filter: 1<px
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: quotes
                                        """
                        );
                        assertPlan(
                                engine,
                                ctx,
                                "SELECT t.ts, t.sym, q.bid FROM trades t ASOF JOIN (SELECT ts, sym, bid FROM quotes WHERE sym IN ('a', 'b')) q ON (sym)",
                                """
                                        SelectedRecord
                                            Async AsOf Join workers: 4
                                              symbol: sym=sym
                                              select: auto
                                              slave key filter: sym in [a,b]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades
                                                SelectedRecord
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: quotes
                                        """
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testMemoryLimitFallsBackToLeanJoin() throws Exception {
        // 300k master symbols that only grow the master's symbol table: the shared key arrays need
        // 1.2 MB. Under limits that refuse them the join runs every frame without per-key state, and
        // must return what it returns without a limit, wherever the serial join completes too.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 1_000_000L, 'f' || (x % 10), x FROM long_sequence(250000)", ctx);
                        engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 1_000_000L + 1, 'r' || x, x FROM long_sequence(200)", ctx);
                        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        engine.execute("INSERT INTO trades SELECT '2023-12-31'::timestamp + x, 'm' || x, 0.0 FROM long_sequence(300000)", ctx);
                        engine.execute("INSERT INTO trades SELECT '2024-01-03'::timestamp + x * 10_000_000L, CASE WHEN x % 3 = 0 THEN 'f' || (x % 10) "
                                + "ELSE 'r' || (x % 230 + 1) END, x FROM long_sequence(3000)", ctx);
                        final String body = " t.ts, t.sym, q.bid, q.ts qts FROM trades t ASOF JOIN quotes q ON (sym) WHERE t.ts >= '2024-01-03'";
                        final String parallel = "SELECT /*+ asof_parallel(t q) */" + body;
                        final String serial = "SELECT /*+ asof_linear(t q) */" + body;
                        assertParallel(engine, ctx, parallel, true);
                        final StringSink expected = new StringSink();
                        TestUtils.printSql(engine, ctx, serial, expected);
                        assertNotVacuous(engine, ctx, parallel, "bid");
                        final StringSink actual = new StringSink();
                        boolean leanSeen = false;
                        for (long limit : new long[]{512L << 10, 1L << 20, 2L << 20, 64L << 20}) {
                            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                            try {
                                try {
                                    TestUtils.printSql(engine, ctx, serial, actual);
                                } catch (Throwable th) {
                                    // the serial join does not complete under this limit either
                                    LOG.info().$("serial join fails under limit [limit=").$(limit).$(", error=").$(th.getMessage()).I$();
                                    continue;
                                }
                                try (RecordCursorFactory factory = engine.select(parallel, ctx)) {
                                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                                        CursorPrinter.println(cursor, factory.getMetadata(), actual);
                                    }
                                    leanSeen |= findAtom(factory).getStatFramesLean() > 0;
                                } catch (Throwable th) {
                                    throw new AssertionError("parallel join failed under limit=" + limit, th);
                                }
                                TestUtils.assertEquals("limit=" + limit, expected, actual);
                            } finally {
                                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                            }
                        }
                        Assert.assertTrue("no limit made the join fall back to the lean mode", leanSeen);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testMixedTimestampTypes() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, ctx) -> {
                        engine.execute("CREATE TABLE quotes (ts TIMESTAMP_NS, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        // ns quotes, some 1 ns past a trade's microsecond, some exactly at it
                        engine.execute("INSERT INTO quotes SELECT ('2024-01-01'::timestamp + x * 700_000L)::timestamp_ns + (x % 3), "
                                + "CASE WHEN x % 17 = 0 THEN NULL ELSE 'f' || (x % 7) END, x FROM long_sequence(20000)", ctx);
                        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                        engine.execute("INSERT INTO trades SELECT '2024-01-01'::timestamp + x * 2_100_000L, CASE WHEN x % 13 = 0 THEN NULL ELSE 'f' || (x % 9) END, x "
                                + "FROM long_sequence(6000)", ctx);
                        for (String tolerance : new String[]{"", " TOLERANCE 1s", " TOLERANCE 500000n"}) {
                            final String body = " t.ts, t.sym, q.bid, q.ts qts FROM trades t ASOF JOIN quotes q ON (sym)" + tolerance;
                            assertParallel(engine, ctx, "SELECT /*+ asof_parallel(t q) */" + body, true);
                            TestUtils.assertSqlCursors(engine, ctx, "SELECT /*+ asof_linear(t q) */" + body, "SELECT /*+ asof_parallel(t q) */" + body, LOG);
                            // and the other way round: ns master, us slave
                            final String reversed = " q.ts, q.sym, t.px, t.ts tts FROM quotes q ASOF JOIN trades t ON (sym)" + tolerance;
                            assertParallel(engine, ctx, "SELECT /*+ asof_parallel(q t) */" + reversed, true);
                            TestUtils.assertSqlCursors(engine, ctx, "SELECT /*+ asof_linear(q t) */" + reversed, "SELECT /*+ asof_parallel(q t) */" + reversed, LOG);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNative() throws Exception {
        assertFuzz(6, false, false, false);
    }

    @Test
    public void testParquetMaster() throws Exception {
        assertFuzz(3, false, true, false);
    }

    @Test
    public void testParquetSlave() throws Exception {
        assertFuzz(3, true, false, false);
    }

    static AsyncAsOfJoinAtom findAtom(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncAsOfJoinRecordCursorFactory asof) {
                return asof.getAtom();
            }
        }
        throw new AssertionError("no Async AsOf Join");
    }

    static void assertPlan(CairoEngine engine, SqlExecutionContext ctx, String sql, String expected) throws SqlException {
        final StringSink plan = new StringSink();
        TestUtils.printSql(engine, ctx, "EXPLAIN " + sql, plan);
        TestUtils.assertEquals("QUERY PLAN\n" + expected, plan);
    }

    static void assertPlanContains(CairoEngine engine, SqlExecutionContext ctx, String sql, String expected) throws SqlException {
        final StringSink plan = new StringSink();
        TestUtils.printSql(engine, ctx, "EXPLAIN " + sql, plan);
        if (!Chars.contains(plan, expected.replace("\\n", "\n"))) {
            Assert.fail("plan of " + sql + " does not contain " + expected + ":\n" + plan);
        }
    }

    static void assertNotVacuous(CairoEngine engine, SqlExecutionContext ctx, String sql, String slaveColumn) throws SqlException {
        final StringSink sink = new StringSink();
        TestUtils.printSql(engine, ctx, "SELECT count(), count(" + slaveColumn + ") FROM (" + sql + ")", sink);
        final String[] counts = sink.toString().split("\n")[1].split("\t");
        final long rows = Long.parseLong(counts[0]);
        final long matched = Long.parseLong(counts[1]);
        Assert.assertTrue("no rows: " + sql, rows > 0);
        Assert.assertTrue("no matches: " + sql, matched > 0);
        Assert.assertTrue("every row matched, the no-prior-row case is not exercised: " + sql, matched < rows);
    }

    public static void assertParallel(CairoEngine engine, SqlExecutionContext ctx, String sql, boolean expected) throws SqlException {
        try (RecordCursorFactory factory = engine.select(sql, ctx)) {
            boolean found = false;
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                found |= f instanceof AsyncAsOfJoinRecordCursorFactory;
            }
            if (found != expected) {
                final StringSink plan = new StringSink();
                TestUtils.printSql(engine, ctx, "EXPLAIN " + sql, plan);
                Assert.fail((expected ? "Async AsOf Join expected: " : "Async AsOf Join not expected: ") + sql + "\n" + plan);
            }
        }
    }

    static void createTables(
            CairoEngine engine,
            SqlExecutionContext ctx,
            Rnd rnd,
            boolean parquetSlave,
            boolean parquetMaster,
            boolean columnTops
    ) throws SqlException {
        // quotes: four days. f0..f9 quote all the time, r0..r29 only on day 1, 'late' only on day 4;
        // NULL keys; ties: several rows per timestamp, and copies of rows at the same timestamp
        final long step = 5_000_000L + rnd.nextInt(30) * 1_000_000L;
        final int perTimestamp = 1 + rnd.nextInt(3);
        final int quoteCount = (int) (4 * 86_400_000_000L / step) * perTimestamp;
        engine.execute(
                "CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, ex SYMBOL, bid DOUBLE, bsz INT, ok BOOLEAN, b BYTE, sh SHORT, " +
                        "c CHAR, l LONG, f FLOAT, d DATE, ip IPv4, g GEOHASH(4c), g1 GEOHASH(1c), dc8 DECIMAL(2,1), dc16 DECIMAL(4,1), " +
                        "dc32 DECIMAL(9,2), dc64 DECIMAL(12,2), tn TIMESTAMP_NS, u UUID, st STRING, v VARCHAR, cond SYMBOL" +
                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                ctx
        );
        final String quoteValues = "CASE WHEN x % " + (13 + rnd.nextInt(80)) + " = 0 THEN NULL ELSE 'f' || (x % 10) END, " +
                "'X' || (x % 3), x::double, (x % 1000)::int, x % 3 = 0, (x % 120)::byte, (x % 30000)::short, rnd_char(), " +
                "CASE WHEN x % 5 = 0 THEN NULL ELSE x * 1000003 END, CASE WHEN x % 4 = 0 THEN NULL ELSE (x / 3.0)::float END, " +
                "(x * 86400000)::date, CASE WHEN x % 13 = 0 THEN NULL ELSE rnd_ipv4() END, rnd_geohash(20), rnd_geohash(5), " +
                "rnd_decimal(2,1,5), rnd_decimal(4,1,5), rnd_decimal(9,2,5), rnd_decimal(12,2,5), (x * 1000)::timestamp_ns, rnd_uuid4(), " +
                "CASE WHEN x % 10 = 0 THEN NULL ELSE 's' || x END, CASE WHEN x % 8 = 0 THEN NULL ELSE 'v' || x END, " +
                "CASE WHEN x % 7 = 0 THEN NULL ELSE 'c' || (x % 5) END";
        engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + (x / " + perTimestamp + ") * " + step + "L, "
                + quoteValues + " FROM long_sequence(" + quoteCount + ")", ctx);
        // same-key ties: copies at the same timestamp, appended later (storage order decides)
        engine.execute("INSERT INTO quotes SELECT ts, sym, ex, -bid, bsz, ok, b, sh, c, l, f, d, ip, g, g1, dc8, dc16, dc32, dc64, tn, u, st, v, cond " +
                "FROM quotes WHERE bid % " + (199 + rnd.nextInt(300)) + " = 0", ctx);
        // rare keys, only on day 1
        engine.execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 60_000_000L + 3, 'r' || (x % 30), 'X1', x::double + 0.5, "
                + "1, true, 1::byte, 1::short, 'r', x, 1.5::float, (x * 86400000)::date, '1.1.1.1', rnd_geohash(20), rnd_geohash(5), "
                + "rnd_decimal(2,1,0), rnd_decimal(4,1,0), rnd_decimal(9,2,0), rnd_decimal(12,2,0), (x * 1000)::timestamp_ns, rnd_uuid4(), 'rs', 'rv', 'rc' "
                + "FROM long_sequence(" + (60 + rnd.nextInt(120)) + ")", ctx);
        engine.execute("INSERT INTO quotes SELECT '2024-01-04T20:00:00.000000Z'::timestamp + x, 'late', 'X0', 7.5, 2, false, 2::byte, 2::short, 'l', 2, "
                + "2.5::float, (x * 86400000)::date, '2.2.2.2', rnd_geohash(20), rnd_geohash(5), rnd_decimal(2,1,0), rnd_decimal(4,1,0), "
                + "rnd_decimal(9,2,0), rnd_decimal(12,2,0), (x * 1000)::timestamp_ns, rnd_uuid4(), 'ls', 'lv', 'lc' FROM long_sequence(3)", ctx);
        if (columnTops) {
            // a column added after the first days: NULL above its top
            engine.execute("ALTER TABLE quotes ADD COLUMN late_px DOUBLE", ctx);
            engine.execute("ALTER TABLE quotes ADD COLUMN late_sym SYMBOL", ctx);
            engine.execute("INSERT INTO quotes (ts, sym, ex, bid, late_px, late_sym) SELECT '2024-01-04T10:00:00.000000Z'::timestamp + x * 1_000_000L, "
                    + "'f' || (x % 10), 'X2', x::double, x * 2.0, 'k' || (x % 4) FROM long_sequence(500)", ctx);
        }
        if (parquetSlave) {
            engine.execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-04'", ctx);
        }

        // trades: the same four days; f*, r*, z* (never quoted), 'late', NULL; one trade before any quote
        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, ex SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO trades VALUES ('2023-12-31T23:00:00.000000Z', 'f1', 'X0', 1.0)", ctx);
        final long tradeStep = 15_000_000L + rnd.nextInt(60) * 1_000_000L;
        final int tradeCount = (int) (4 * 86_400_000_000L / tradeStep);
        engine.execute("INSERT INTO trades SELECT '2024-01-01'::timestamp + x * " + tradeStep + "L + (x % 3), "
                + "CASE WHEN x % 31 = 0 THEN NULL WHEN x % 5 = 0 THEN 'r' || (x % 40) WHEN x % 11 = 0 THEN 'z' || (x % 3) "
                + "WHEN x % 13 = 0 THEN 'late' ELSE 'f' || (x % 10) END, 'X' || (x % 4), (x % 10)::double FROM long_sequence(" + tradeCount + ")", ctx);
        // trades exactly at quote timestamps (ties with the slave)
        engine.execute("INSERT INTO trades SELECT ts, sym, 'X0', 9.0 FROM quotes WHERE bid % 997 = 0 AND sym IS NOT NULL", ctx);
        if (columnTops) {
            engine.execute("ALTER TABLE trades ADD COLUMN late_sym SYMBOL", ctx);
            engine.execute("INSERT INTO trades (ts, sym, ex, px, late_sym) SELECT '2024-01-04T10:00:00.000000Z'::timestamp + x * 3_000_000L + 1, "
                    + "'f' || (x % 10), 'X0', 4.0, CASE WHEN x % 9 = 0 THEN NULL ELSE 'k' || (x % 5) END FROM long_sequence(200)", ctx);
        }
        if (parquetMaster) {
            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-04'", ctx);
        }
    }

    private void assertFuzz(int seeds, boolean parquetSlave, boolean parquetMaster, boolean columnTops) throws Exception {
        final Rnd seedRnd = TestUtils.generateRandom(LOG);
        for (int s = 0; s < seeds; s++) {
            final long s0 = seedRnd.nextLong();
            final long s1 = seedRnd.nextLong();
            // the walk and the span scan forced, then the automatic choice between them
            final int walkMode = WALK_MODES[s % WALK_MODES.length];
            LOG.info().$("seed [s0=").$(s0).$(", s1=").$(s1).$(", walkMode=").$(walkMode).I$();
            AsyncAsOfJoinRecordCursorFactory.WALK_MODE = walkMode;
            assertMemoryLeak(() -> {
                final WorkerPool pool = new TestWorkerPool(4);
                TestUtils.execute(
                        pool,
                        (engine, compiler, ctx) -> {
                            final Rnd rnd = new Rnd(s0, s1);
                            ctx.setRandom(new Rnd(s0, s1));
                            engine.execute("DROP TABLE IF EXISTS quotes", ctx);
                            engine.execute("DROP TABLE IF EXISTS trades", ctx);
                            createTables(engine, ctx, rnd, parquetSlave, parquetMaster, columnTops);
                            assertQueries(engine, ctx, rnd, columnTops);
                        },
                        configuration,
                        LOG
                );
            });
        }
    }

    private void assertQueries(CairoEngine engine, SqlExecutionContext ctx, Rnd rnd, boolean columnTops) throws SqlException {
        final String[] slaves = {
                "quotes q",
                // a projection
                "(SELECT ts, sym, bid, cond, st FROM quotes) q",
                // a filter on the key column alone
                "(SELECT * FROM quotes WHERE sym IN ('f1', 'f3', 'r3', 'r4', 'late')) q",
                // the same under a projection
                "(SELECT ts, sym, bid, cond, st FROM quotes WHERE sym IN ('f1', 'f3', 'r3', 'r4', 'late') OR sym IS NULL) q",
        };
        final String[] tolerances = {"", " TOLERANCE 30m", " TOLERANCE 3h"};
        for (String slave : slaves) {
            final boolean isFullSlave = slave.startsWith("quotes") || slave.contains("SELECT *");
            final String slaveColumns = isFullSlave
                    ? "q.ts qts, q.sym qsym, q.ex, q.bid, q.bsz, q.ok, q.b, q.sh, q.c, q.l, q.f, q.d, q.ip, q.g, q.g1, q.dc8, q.dc16, q.dc32, "
                      + "q.dc64, q.tn, q.u, q.st, q.v, q.cond" + (columnTops ? ", q.late_px, q.late_sym" : "")
                    : "q.ts qts, q.sym qsym, q.bid, q.cond, q.st";
            for (String masterFilter : MASTER_FILTERS) {
                final String tolerance = tolerances[rnd.nextInt(tolerances.length)];
                final String body = " t.ts, t.sym, t.px, " + slaveColumns + " FROM trades t ASOF JOIN " + slave + " ON (sym)" + tolerance + masterFilter;
                final String parallel = "SELECT /*+ asof_parallel(t q) */" + body;
                assertParallel(engine, ctx, parallel, true);
                for (String hint : SERIAL_HINTS) {
                    final String serial = "SELECT /*+ " + hint + "(t q) */" + body;
                    TestUtils.assertSqlCursors(engine, ctx, serial, parallel, LOG);
                }
                // LIMIT over the join
                TestUtils.assertSqlCursors(
                        engine,
                        ctx,
                        "SELECT /*+ asof_linear(t q) */" + body + " LIMIT 7",
                        parallel + " LIMIT 7",
                        LOG
                );
                TestUtils.assertSqlCursors(
                        engine,
                        ctx,
                        "SELECT /*+ asof_linear(t q) */" + body + " LIMIT 50, 75",
                        parallel + " LIMIT 50, 75",
                        LOG
                );
            }
        }
        // a master written as a filtered projection, the TAQ shape
        final String taq = " t.sym, t.ts, t.px, q.bid, q.cond FROM (SELECT sym, ts, px FROM trades WHERE sym IN ('f1', 'r2', 'z1', 'late')) t "
                + "ASOF JOIN (SELECT sym, ts, bid, cond FROM quotes) q ON (sym)";
        assertParallel(engine, ctx, "SELECT /*+ asof_parallel(t q) */" + taq, true);
        TestUtils.assertSqlCursors(engine, ctx, "SELECT /*+ asof_linear(t q) */" + taq, "SELECT /*+ asof_parallel(t q) */" + taq, LOG);
        assertNotVacuous(engine, ctx, "SELECT /*+ asof_parallel(t q) */" + taq, "bid");
        assertNotVacuous(engine, ctx, "SELECT /*+ asof_parallel(t q) */ t.ts, q.bid FROM trades t ASOF JOIN quotes q ON (sym)", "bid");
        if (columnTops) {
            // a key column with a column top on both sides
            final String tops = " t.ts, t.late_sym, q.bid, q.late_px FROM trades t ASOF JOIN quotes q ON (late_sym)";
            assertParallel(engine, ctx, "SELECT /*+ asof_parallel(t q) */" + tops, true);
            TestUtils.assertSqlCursors(engine, ctx, "SELECT /*+ asof_linear(t q) */" + tops, "SELECT /*+ asof_parallel(t q) */" + tops, LOG);
            TestUtils.assertSqlCursors(engine, ctx, "SELECT /*+ asof_dense(t q) */" + tops, "SELECT /*+ asof_parallel(t q) */" + tops, LOG);
        }
    }
}

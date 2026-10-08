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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.window.AsyncWindowAtom;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursor;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.AsyncWindowSplitPlan;
import io.questdb.griffin.engine.window.AsyncWindowStage;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Window chains of the NYSE TAQ benchmark on the shared workers: a window over one key, a running
 * sum, a whole-table lag with a filter over it, and price-change runs (lag, a CASE, a running sum
 * of it, then GROUP BY the run). The planner walks the scan key by key (a single key; an IN list
 * or the whole table when an ORDER BY above makes the order of the keys invisible), and chains the
 * projections, windows, filters and GROUP BY over the Async Window as steps its workers compute.
 * <p>
 * Every query runs with the parallel window switched off, which is the serial plan, and on; the
 * two must agree bit for bit, except the sums and averages of a plan that splits a key over tasks,
 * which may differ in their last bits, as documented for the parallel window. The tests also check
 * the plan they mean to test: the steps, the key-major scans, and that the sort over the GROUP BY
 * is elided where the groups come out in its order.
 */
@RunWith(Parameterized.class)
public class WindowChainTest extends AbstractCairoTest {
    private static final String CH_EXPR = "CASE WHEN (price IS NULL) != (lag(price) OVER (%s) IS NULL) OR price != lag(price) OVER (%s) THEN 1 ELSE 0 END";
    private static final long MAX_KEY_ROWS = 400;
    private static final long MIN_ROWS = 100;
    private static final long ROUND_ROWS = 200;
    private static final long TASK_ROWS = 50;
    private final String indexType;

    public WindowChainTest(String indexType) {
        this.indexType = indexType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"bitmap"},
                {"posting"},
        });
    }

    // idx 48: a 20-row rolling average over one key
    public static String q48(String sym) {
        return "SELECT time, avg(price) OVER (ORDER BY time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS price FROM trade WHERE sym = '" + sym + "'";
    }

    // idx 52: a running sum over one key
    public static String q52(String sym) {
        return "SELECT time, sum(size) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS size FROM trade WHERE sym = '" + sym + "'";
    }

    // idx 55: a running sum over a filtered table
    public static String q55(String ex) {
        return "SELECT time, sum(size) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS size FROM trade WHERE ex = '" + ex + "'";
    }

    // idx 61, Index text: the lag inside an expression, filtered on it
    public static String q61Index() {
        return "SELECT sym, seq AS seqDecr FROM (SELECT sym, seq, seq - lag(seq) OVER (PARTITION BY sym ORDER BY time) AS seq_delta FROM trade) WHERE seq_delta < 0 ORDER BY sym, seqDecr";
    }

    // idx 61, Manual Opt text
    public static String q61ManualOpt() {
        return "SELECT sym, seq AS seqDecr FROM (SELECT sym, seq, lag(seq) OVER (PARTITION BY sym ORDER BY time) AS prev_seq FROM trade) WHERE seq < prev_seq ORDER BY sym, seqDecr";
    }

    // idx 72: price-change runs of one key
    public static String q72(String sym) {
        final String w = "ORDER BY time";
        return "WITH src AS (SELECT time, price, size FROM trade WHERE sym = '" + sym + "'), " +
                "ch AS (SELECT time, price, size, " + String.format(CH_EXPR, w, w) + " AS price_changed FROM src), " +
                "runs AS (SELECT time, price, size, sum(price_changed) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pricegroup FROM ch) " +
                "SELECT pricegroup, first(price) AS price, min(time) AS FirstTime, max(time) AS LastTime, count() AS cnt, sum(size::double) AS size FROM runs ORDER BY pricegroup";
    }

    // idx 73 and the Index text of 74: price-change runs of every key of a list
    public static String q73(String in) {
        final String w = "PARTITION BY sym ORDER BY time";
        return "WITH src AS (SELECT sym, time, price, size FROM trade WHERE sym IN (" + in + ")), " +
                "ch AS (SELECT sym, time, price, size, " + String.format(CH_EXPR, w, w) + " AS price_changed FROM src), " +
                "runs AS (SELECT sym, time, price, size, sum(price_changed) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pricegroup FROM ch) " +
                "SELECT sym, pricegroup, first(price) AS price, min(time) AS FirstTime, max(time) AS LastTime, count() AS cnt, sum(size::double) AS size FROM runs ORDER BY sym, pricegroup";
    }

    // idx 74, Manual Opt text: the scan ordered by the key, the windows without ORDER BY
    public static String q74ManualOpt(String in) {
        final String w = "PARTITION BY sym";
        return "WITH src AS (SELECT sym, time, price, size FROM trade WHERE sym IN (" + in + ") ORDER BY sym), " +
                "ch AS (SELECT sym, time, price, size, " + String.format(CH_EXPR, w, w) + " AS price_changed FROM src), " +
                "runs AS (SELECT sym, time, price, size, sum(price_changed) OVER (PARTITION BY sym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pricegroup FROM ch) " +
                "SELECT sym, pricegroup, first(price) AS price, min(time) AS FirstTime, max(time) AS LastTime, count() AS cnt, sum(size::double) AS size FROM runs ORDER BY sym, pricegroup";
    }

    @Override
    public void setUp() {
        super.setUp();
        // 64 rows per page frame: every key spans many frames
        sqlExecutionContext.changePageFrameSizes(1, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, TASK_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, MAX_KEY_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, MIN_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, ROUND_ROWS);
    }

    @Test
    public void testChainSwitchedOff() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_CHAIN_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            for (String query : new String[]{q61Index(), q61ManualOpt(), q72("BIG"), q73(allKeys()), q74ManualOpt(allKeys())}) {
                assertMatchesSerial(engine, sqlExecutionContext, query, null);
                sqlExecutionContext.setParallelWindowEnabled(true);
                final StringSink plan = new StringSink();
                engine.print("explain " + query, plan, sqlExecutionContext);
                sqlExecutionContext.setParallelWindowEnabled(false);
                Assert.assertFalse(query + "\n" + plan, Chars.contains(plan, " then: "));
            }
        });
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            sqlExecutionContext.setParallelWindowEnabled(true);
            final String[] queries = {q48("BIG"), q52("BIG"), q61Index(), q61ManualOpt(), q72("BIG"), q73("'K1', 'BIG'"), q74ManualOpt("'K1', 'BIG'")};
            for (String query : queries) {
                final StringSink plan = new StringSink();
                engine.print("explain " + query, plan, sqlExecutionContext);
                System.out.println("PLAN " + query + "\n" + plan);
            }
            sqlExecutionContext.setParallelWindowEnabled(false);
        });
    }

    @Test
    public void testIdx48And52SingleKey() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            for (String sym : new String[]{"BIG", "K3", "NOPE"}) {
                assertMatchesSerial(engine, sqlExecutionContext, q48(sym), null);
                assertMatchesSerial(engine, sqlExecutionContext, q52(sym), null);
            }
            // keys whole: no split, so every value is the serial one, bit for bit
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 100_000);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 100_000);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 100_000);
            assertMatchesSerial(engine, sqlExecutionContext, q48("BIG"), null);
            assertMatchesSerial(engine, sqlExecutionContext, q52("BIG"), null);
        });
    }

    @Test
    public void testIdx55RowSlices() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            final String[] queries = {
                    q55("T"),
                    q55("NOPE"),
                    "SELECT time, sum(size) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s," +
                            " count() OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c," +
                            " row_number() OVER () AS rn FROM trade",
                    q55("T") + " LIMIT 13",
                    q55("Q") + " LIMIT -7",
            };
            for (String query : queries) {
                assertMatchesSerial(engine, sqlExecutionContext, query, null);
            }
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select(q55("T"), sqlExecutionContext)) {
                Assert.assertTrue(findAsyncFactory(factory).isSliceMode());
            } finally {
                sqlExecutionContext.setParallelWindowEnabled(false);
            }
        });
    }

    @Test
    public void testIdx61WholeTable() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            for (String query : new String[]{q61Index(), q61ManualOpt()}) {
                assertMatchesSerial(engine, sqlExecutionContext, query, AsyncWindowStage.KIND_FILTER);
                assertMatchesSerial(engine, sqlExecutionContext, query + " LIMIT 7", AsyncWindowStage.KIND_FILTER);
                assertMatchesSerial(engine, sqlExecutionContext, query + " LIMIT -3", AsyncWindowStage.KIND_FILTER);
            }
            // without an ORDER BY over it, the order of the rows is the table's: no key-major walk
            final String unordered = "SELECT sym, seq FROM (SELECT sym, seq, lag(seq) OVER (PARTITION BY sym ORDER BY time) AS prev_seq FROM trade) WHERE seq < prev_seq";
            assertMatchesSerial(engine, sqlExecutionContext, unordered, -1);
            // ordered by a column that leaves ties, which a key-major walk would reorder
            final String ties = "SELECT sym, seq, time FROM (SELECT sym, seq, time, lag(seq) OVER (PARTITION BY sym ORDER BY time) AS prev_seq FROM trade) WHERE seq < prev_seq ORDER BY sym";
            assertMatchesSerial(engine, sqlExecutionContext, ties, -1);
        });
    }

    @Test
    public void testIdx72SingleKeyRuns() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            for (String sym : new String[]{"BIG", "K3", "NOPE"}) {
                assertMatchesSerial(engine, sqlExecutionContext, q72(sym), AsyncWindowStage.KIND_GROUP_BY);
            }
            // BIG within a task: computed by a task rather than streamed by the query's thread
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 100_000);
            assertMatchesSerial(engine, sqlExecutionContext, q72("BIG"), AsyncWindowStage.KIND_GROUP_BY);
            assertMatchesSerial(engine, sqlExecutionContext, q72("BIG") + " LIMIT 5", AsyncWindowStage.KIND_GROUP_BY);
        });
    }

    @Test
    public void testIdx73And74Runs() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            final String[] lists = {allKeys(), "'K1', 'K2', 'BIG', 'K7'", "'K5', 'NOPE'", "'K9'", "null, 'K1', 'BIG'", "'K3', null"};
            for (String in : lists) {
                assertMatchesSerial(engine, sqlExecutionContext, q73(in), AsyncWindowStage.KIND_GROUP_BY);
                assertMatchesSerial(engine, sqlExecutionContext, q74ManualOpt(in), AsyncWindowStage.KIND_GROUP_BY);
                assertMatchesSerial(engine, sqlExecutionContext, q73(in) + " LIMIT 11", AsyncWindowStage.KIND_GROUP_BY);
            }
            // whole keys within tasks: BIG computed by a task too
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 100_000);
            assertMatchesSerial(engine, sqlExecutionContext, q73(allKeys()), AsyncWindowStage.KIND_GROUP_BY);
            assertMatchesSerial(engine, sqlExecutionContext, q74ManualOpt(allKeys()), AsyncWindowStage.KIND_GROUP_BY);
        });
    }

    @Test
    public void testOnWorkerPool() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 200);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 5_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 1_000);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createTrade(engine, ctx, 20_000);
            final String[] queries = {q48("BIG"), q52("BIG"), q55("T"), q61Index(), q61ManualOpt(), q72("BIG"), q73(allKeys()), q74ManualOpt(allKeys())};
            long workerTasks = 0;
            for (String query : queries) {
                for (int run = 0; run < 3; run++) {
                    workerTasks += assertMatchesSerial(engine, ctx, query, null);
                }
            }
            Assert.assertTrue(workerTasks > 0);
        }));
    }

    @Test
    public void testCancelMidQuery() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 5_000);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createTrade(engine, ctx, 20_000);
            final String query = q73(allKeys());
            final String expected = serial(engine, ctx, query);
            final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    engine,
                    new DefaultSqlExecutionCircuitBreakerConfiguration()
            );
            try {
                ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), circuitBreaker);
                ctx.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = engine.select(query, ctx)) {
                    final AsyncWindowRecordCursorFactory async = findAsyncFactory(factory);
                    try (RecordCursor cursor = async.getCursor(ctx)) {
                        for (int i = 0; i < 50; i++) {
                            Assert.assertTrue(cursor.hasNext());
                        }
                        circuitBreaker.cancel();
                        try {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("cancelled query ran to completion");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getMessage(), e.isCancellation());
                        }
                        circuitBreaker.clearCancelSentinel();
                        circuitBreaker.resetTimer();
                    }
                    assertSlotsReleased(factory);
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertEquals(expected, rawRows(cursor, factory.getMetadata()));
                    }
                } finally {
                    ctx.setParallelWindowEnabled(false);
                }
            } finally {
                Misc.free(circuitBreaker);
            }
        }));
    }

    @Test
    public void testParquetPartition() throws Exception {
        // a Parquet partition keeps the serial plan's scan, or runs serially if planned before
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 64);
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            final String[] queries = {q48("BIG"), q61ManualOpt(), q72("BIG"), q73(allKeys())};
            final ObjList<RecordCursorFactory> cached = new ObjList<>();
            final ObjList<String> expected = new ObjList<>();
            try {
                for (String query : queries) {
                    expected.add(serial(engine, sqlExecutionContext, query));
                    sqlExecutionContext.setParallelWindowEnabled(true);
                    cached.add(engine.select(query, sqlExecutionContext));
                    sqlExecutionContext.setParallelWindowEnabled(false);
                }
                execute("alter table trade convert partition to parquet list '1970-01-01'");
                for (int i = 0; i < queries.length; i++) {
                    // the plans made over native partitions
                    sqlExecutionContext.setParallelWindowEnabled(true);
                    try (RecordCursor cursor = cached.getQuick(i).getCursor(sqlExecutionContext)) {
                        assertRowsMatch(queries[i], cached.getQuick(i), expected.getQuick(i), rawRows(cursor, cached.getQuick(i).getMetadata()));
                    } finally {
                        sqlExecutionContext.setParallelWindowEnabled(false);
                    }
                    // and new plans over a Parquet partition
                    assertMatchesSerial(engine, sqlExecutionContext, queries[i], null);
                }
            } finally {
                Misc.freeObjList(cached);
            }
        });
    }

    @Test
    public void testRerunsAndRewinds() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            for (String query : new String[]{q61ManualOpt(), q72("BIG"), q73(allKeys())}) {
                final String expected = serial(engine, sqlExecutionContext, query);
                sqlExecutionContext.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = select(query)) {
                    for (int run = 0; run < 3; run++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            for (int i = 0; i < 37 && cursor.hasNext(); i++) {
                                // a partial pass
                            }
                            cursor.toTop();
                            assertRowsMatch(query, factory, expected, rawRows(cursor, factory.getMetadata()));
                            cursor.toTop();
                            assertRowsMatch(query, factory, expected, rawRows(cursor, factory.getMetadata()));
                        }
                    }
                    assertSlotsReleased(factory);
                } finally {
                    sqlExecutionContext.setParallelWindowEnabled(false);
                }
            }
        });
    }

    @Test
    public void testUnprovenGroupKeysStaySerial() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            final String w = "PARTITION BY sym ORDER BY time";
            // a CASE that can be negative: its running sum may come back to an earlier value
            final String negative = "WITH ch AS (SELECT sym, time, price, size, CASE WHEN price > lag(price) OVER (" + w + ") THEN 1 ELSE -1 END AS d FROM trade WHERE sym IN (" + allKeys() + ")), " +
                    "runs AS (SELECT sym, time, price, size, sum(d) OVER (" + w + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g FROM ch) " +
                    "SELECT sym, g, count() cnt, sum(size::double) s FROM runs ORDER BY sym, g";
            assertMatchesSerial(engine, sqlExecutionContext, negative, AsyncWindowStage.KIND_WINDOW);
            // a group key that is not the scan's key nor the running sum
            final String byPrice = "WITH runs AS (SELECT sym, time, price, size, sum(size) OVER (" + w + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g FROM trade WHERE sym IN (" + allKeys() + ")) " +
                    "SELECT sym, price, count() cnt FROM runs ORDER BY sym, price";
            assertMatchesSerial(engine, sqlExecutionContext, byPrice, null);
        });
    }

    private static String allKeys() {
        final StringSink sink = new StringSink();
        sink.put("'BIG'");
        for (int i = 0; i < 20; i++) {
            sink.put(", 'K").put(i).put('\'');
        }
        return sink.toString();
    }

    private static void assertRowsMatch(String query, RecordCursorFactory factory, String expected, String actual) {
        final AsyncWindowRecordCursorFactory async = findAsyncFactoryOrNull(factory);
        if (async == null || !mayDifferInLastBits(async.getSplitPlan())) {
            TestUtils.assertEquals(query, expected, actual);
            return;
        }
        // a split key: sums and averages of DOUBLE may differ in their last bits
        final String[] e = expected.split("\n");
        final String[] a = actual.split("\n");
        Assert.assertEquals(query, e.length, a.length);
        final RecordMetadata metadata = factory.getMetadata();
        for (int i = 0; i < e.length; i++) {
            if (e[i].equals(a[i])) {
                continue;
            }
            final String[] ev = e[i].split("\t", -1);
            final String[] av = a[i].split("\t", -1);
            Assert.assertEquals(query, ev.length, av.length);
            for (int c = 0; c < ev.length; c++) {
                if (ev[c].equals(av[c])) {
                    continue;
                }
                final String message = query + " row " + i + " column " + c + ": expected " + ev[c] + " but was " + av[c];
                Assert.assertEquals(message, ColumnType.DOUBLE, ColumnType.tagOf(metadata.getColumnType(c)));
                final double x = Double.longBitsToDouble(Long.parseLong(ev[c]));
                final double y = Double.longBitsToDouble(Long.parseLong(av[c]));
                Assert.assertTrue(message, Math.abs(x - y) <= 1e-12 * Math.max(1.0, Math.abs(x)));
            }
        }
    }

    // The documented exception: warm-up rows rebuild a frame's sum in another order, and a carry
    // is added to a DOUBLE sum computed from scratch. A fold (OP_FOLD) and every integer
    // combination are exact.
    private static boolean mayDifferInLastBits(AsyncWindowSplitPlan plan) {
        if (plan.getMode() == AsyncWindowSplitPlan.MODE_WARMUP) {
            return true;
        }
        for (int i = 0, n = plan.getPrefixCount(); i < n; i++) {
            if (plan.getPrefixOp(i) == AsyncWindowSplitPlan.OP_ADD && ColumnType.tagOf(plan.getPrefixType(i)) == ColumnType.DOUBLE) {
                return true;
            }
        }
        return false;
    }

    private static void assertSlotsReleased(RecordCursorFactory factory) {
        final PerWorkerLocks locks = TestUtils.findPerWorkerLocks(factory, "async window");
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    private static AsyncWindowRecordCursorFactory findAsyncFactory(RecordCursorFactory factory) {
        final AsyncWindowRecordCursorFactory async = findAsyncFactoryOrNull(factory);
        Assert.assertNotNull("no Async Window in the factory tree", async);
        return async;
    }

    private static AsyncWindowRecordCursorFactory findAsyncFactoryOrNull(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncWindowRecordCursorFactory async) {
                return async;
            }
        }
        return null;
    }

    // Every value of every row as raw bits: doubles and floats by their bits, symbols by value.
    private static String rawRows(RecordCursor cursor, RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        final Record record = cursor.getRecord();
        final int n = metadata.getColumnCount();
        while (cursor.hasNext()) {
            for (int c = 0; c < n; c++) {
                if (c > 0) {
                    sink.put('\t');
                }
                switch (ColumnType.tagOf(metadata.getColumnType(c))) {
                    case ColumnType.DOUBLE -> sink.put(Double.doubleToRawLongBits(record.getDouble(c)));
                    case ColumnType.FLOAT -> sink.put(Float.floatToRawIntBits(record.getFloat(c)));
                    case ColumnType.INT -> sink.put(record.getInt(c));
                    case ColumnType.LONG -> sink.put(record.getLong(c));
                    case ColumnType.TIMESTAMP -> sink.put(record.getTimestamp(c));
                    case ColumnType.SYMBOL -> sink.put(record.getSymA(c));
                    default -> throw new AssertionError("type " + ColumnType.nameOf(metadata.getColumnType(c)));
                }
            }
            sink.put('\n');
        }
        return sink.toString();
    }

    private static String serial(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        ctx.setParallelWindowEnabled(false);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            Assert.assertNull(query, findAsyncFactoryOrNull(factory));
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                return rawRows(cursor, factory.getMetadata());
            }
        }
    }

    /**
     * Runs the query serially and on the parallel window, compares the rows, and returns the
     * tasks the worker threads computed.
     *
     * @param lastStage the kind of the Async Window's last step the plan must have, -1 for no
     *                  Async Window at all, null for any plan
     */
    private long assertMatchesSerial(CairoEngine engine, SqlExecutionContext ctx, String query, Integer lastStage) throws Exception {
        final String expected = serial(engine, ctx, query);
        ctx.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            final AsyncWindowRecordCursorFactory async = findAsyncFactoryOrNull(factory);
            if (lastStage != null) {
                if (lastStage == -1) {
                    Assert.assertNull(query, async);
                } else {
                    Assert.assertNotNull(query, async);
                    final ObjList<AsyncWindowStage> stages = async.getStages();
                    Assert.assertTrue(query, stages.size() > 0);
                    Assert.assertEquals(query, (int) lastStage, stages.getLast().getKind());
                }
            }
            for (int pass = 0; pass < 2; pass++) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    assertRowsMatch(query, factory, expected, rawRows(cursor, factory.getMetadata()));
                    cursor.toTop();
                    assertRowsMatch(query, factory, expected, rawRows(cursor, factory.getMetadata()));
                }
            }
            if (async == null) {
                return 0;
            }
            assertSlotsReleased(factory);
            return ((AsyncWindowAtom) async.getAtom()).getWorkerThreadTaskCount();
        } finally {
            ctx.setParallelWindowEnabled(false);
        }
    }

    /**
     * A {@code trade}-like table: key BIG holds 40% of the rows, K0..K19 the rest, less one in
     * thirteen that is NULL; two rows share each timestamp; price repeats in runs of a few rows and
     * is NULL one row in eleven; size is NULL one row in seventeen; seq goes down now and then.
     * Three DAY partitions.
     */
    private void createTrade(CairoEngine engine, SqlExecutionContext ctx, int rows) throws Exception {
        engine.execute(
                "create table trade (time timestamp, sym symbol index type " + indexType + ", ex symbol, price float, size float, seq int)" +
                        " timestamp(time) partition by DAY",
                ctx
        );
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
    }

    private void inPool(PoolTest test) throws Exception {
        final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
        TestUtils.execute(pool, (engine, compiler, context) -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
            ctx.changePageFrameSizes(1, 64);
            test.run(engine, ctx);
        }, configuration, LOG);
    }

    @FunctionalInterface
    private interface PoolTest {
        void run(CairoEngine engine, SqlExecutionContextImpl ctx) throws Exception;
    }
}

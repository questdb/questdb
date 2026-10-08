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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursor;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.AsyncWindowSplitPlan;
import io.questdb.mp.WorkerPool;
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
 * The walk-order pass over folded and replayed columns (OP_FOLD, OP_REPLAY) of the parallel window,
 * which the thread that finds the next task in walk order computed runs, off the query's thread:
 * bounded DOUBLE frames over keys that span tasks and rounds (idx 49 and 50), running DOUBLE sums,
 * both together, on a worker pool and without one, with and without the rows the query's thread
 * streams first, under LIMIT, rewinds and second cursors. Every value must be the serial plan's,
 * bit for bit, on data whose sums round at every step.
 */
@RunWith(Parameterized.class)
public class WindowReplayChainTest extends AbstractCairoTest {
    private static final String IN_LIST = "'BIG', 'K1', 'K2', 'K3', 'K7', 'MISSING', 'K11', 'K12', 'K13', 'K14', 'K15', 'K16', 'K17', 'K18', 'K19'";
    private static final int ROWS = 24_000;
    private final String indexType;

    public WindowReplayChainTest(String indexType) {
        this.indexType = indexType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"bitmap"},
                {"posting"},
        });
    }

    // idx 49 and 50: two bounded avg over the key, one of a DOUBLE expression (replayed), one of
    // whole numbers (rebuilt from warm-up rows)
    public static String q49(String table, String in) {
        return "SELECT sym, time, m, s FROM (SELECT sym, time," +
                " avg((bsize * bid + asize * ask) / (bsize + asize)) OVER (PARTITION BY sym ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS m," +
                " avg(asize + bsize) OVER (PARTITION BY sym ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s" +
                " FROM " + table + " WHERE sym IN (" + in + ") ORDER BY sym)";
    }

    @Override
    public void setUp() {
        super.setUp();
        sqlExecutionContext.changePageFrameSizes(1, 128);
        // tasks of 37 rows, rounds of 4 of them: a key spans many tasks and rounds
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 37);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 100_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 150);
    }

    @Test
    public void testFoldAndReplay() throws Exception {
        assertMemoryLeak(() -> {
            createQuotes(engine, sqlExecutionContext);
            for (String query : queries()) {
                assertMatchesSerial(engine, sqlExecutionContext, query);
            }
            assertOps(engine, sqlExecutionContext);
        });
    }

    @Test
    public void testFoldAndReplayNoPrefix() throws Exception {
        // the first task starts the walk: no rows of the query's thread seed the pass
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_PREFIX_ROWS, 0);
        assertMemoryLeak(() -> {
            createQuotes(engine, sqlExecutionContext);
            for (String query : queries()) {
                assertMatchesSerial(engine, sqlExecutionContext, query);
            }
        });
    }

    @Test
    public void testFoldAndReplayOnWorkerPool() throws Exception {
        inPool((engine, ctx) -> {
            createQuotes(engine, ctx);
            long workerPasses = 0;
            for (String query : queries()) {
                workerPasses += assertMatchesSerial(engine, ctx, query);
            }
            // the workers went over tasks themselves, not only the query's thread
            Assert.assertTrue("worker passes: " + workerPasses, workerPasses > 0);
        });
    }

    @Test
    public void testFoldAndReplayOnWorkerPoolTinyRounds() throws Exception {
        // two rounds alive at a time, of tasks of 9 rows: the pass crosses rounds all the time,
        // and the walk's ring of tasks wraps around
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 9);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 27);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_ROUNDS, 2);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_PREFIX_ROWS, 0);
        inPool((engine, ctx) -> {
            createQuotes(engine, ctx);
            for (String query : queries()) {
                assertMatchesSerial(engine, ctx, query);
            }
        });
    }

    @Test
    public void testSignedZeroAtKeyStart() throws Exception {
        // Review round 4, m1: worker copies compiled without the PARTITION BY started a bounded
        // frame's sum from 0.0, and 0.0 + -0.0 is 0.0, where the partitioned serial function
        // starts from the first value, -0.0. A running DOUBLE max beside it keeps the keys whole
        // (no key runs, no split), so the workers compute every key afresh.
        assertMemoryLeak(() -> {
            engine.execute(
                    "create table z (time timestamp, sym symbol index type " + indexType + ", v double, w double)" +
                            " timestamp(time) partition by DAY",
                    sqlExecutionContext
            );
            // every key's first three rows are -0.0, then values that keep the sum at a signed zero
            // for a while (-0.0, 0.0), then ordinary ones
            engine.execute(
                    "insert into z select" +
                            " (x * 1_000_000L)::timestamp," +
                            " 'K' || (x % 10)," +
                            " case when x <= 30 then -0.0 when x <= 60 then (case when x % 20 < 10 then -0.0 else 0.0 end)" +
                            " when x % 7 = 0 then null else rnd_double() * 10.0 / 3.0 - 1.5 end," +
                            " rnd_double()" +
                            " from long_sequence(4000)",
                    sqlExecutionContext
            );
            final String[] frames = {
                    "avg(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)",
                    "sum(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)",
                    "sum(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)",
                    "avg(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING)",
                    "sum(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                    "avg(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                    "sum(v) OVER (PARTITION BY sym ORDER BY time)",
                    "avg(v) OVER (PARTITION BY sym)",
                    "first_value(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)",
                    "last_value(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)",
                    "min(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)",
                    "max(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)",
                    "lag(v) OVER (PARTITION BY sym ORDER BY time)",
            };
            for (String frame : frames) {
                for (String where : new String[]{"", " WHERE sym IN ('K3', 'K1', 'K7', 'K0')"}) {
                    final String query = "SELECT sym, time, y, m FROM (SELECT sym, time, " + frame + " y," +
                            " max(w) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m" +
                            " FROM z" + where + ") ORDER BY sym, time, y, m";
                    final String expected = serial(engine, sqlExecutionContext, query);
                    sqlExecutionContext.setParallelWindowEnabled(true);
                    try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                        if (findAsyncFactoryOrNull(factory) == null) {
                            // a window that does not stream, cached as serially
                            continue;
                        }
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            TestUtils.assertEquals(query, expected, rawRows(cursor, factory.getMetadata()));
                        }
                    } finally {
                        sqlExecutionContext.setParallelWindowEnabled(false);
                    }
                }
            }
            // the bounded frames, the ones that differed, ran in parallel
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select("SELECT sym, time, y, m FROM (SELECT sym, time, " + frames[0] + " y," +
                    " max(w) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) m FROM z) ORDER BY sym, time, y, m", sqlExecutionContext)) {
                Assert.assertNotNull(findAsyncFactoryOrNull(factory));
            } finally {
                sqlExecutionContext.setParallelWindowEnabled(false);
            }
        });
    }

    @Test
    public void testLimitAndRewindOnWorkerPool() throws Exception {
        // a LIMIT closes the cursor with rounds in flight and passes pending; the next execution
        // of the same factory starts the pass afresh
        inPool((engine, ctx) -> {
            createQuotes(engine, ctx);
            final String[] queries = {
                    q49("q", IN_LIST) + " LIMIT 333",
                    q49("q", IN_LIST) + " LIMIT 5000, 5100",
                    q49("q", IN_LIST) + " LIMIT -40",
                    q49("q", "'BIG'") + " LIMIT 1000",
                    "SELECT sym, time, s FROM (SELECT sym, time, sum(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM q WHERE sym IN (" + IN_LIST + ")) ORDER BY sym, time, s LIMIT 2000",
            };
            for (String query : queries) {
                final String expected = serial(engine, ctx, query);
                ctx.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = engine.select(query, ctx)) {
                    Assert.assertNotNull(query, findAsyncFactoryOrNull(factory));
                    for (int pass = 0; pass < 3; pass++) {
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertEquals(query, expected, rawRows(cursor, factory.getMetadata()));
                        }
                    }
                    assertSlotsReleased(factory);
                } finally {
                    ctx.setParallelWindowEnabled(false);
                }
            }
        });
    }

    private static void assertSlotsReleased(RecordCursorFactory factory) {
        final PerWorkerLocks locks = TestUtils.findPerWorkerLocks(factory, "async window");
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    private static AsyncWindowRecordCursorFactory findAsyncFactoryOrNull(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncWindowRecordCursorFactory async) {
                return async;
            }
        }
        return null;
    }

    private static String[] queries() {
        return new String[]{
                q49("q", IN_LIST),
                q49("q", "'BIG'"),
                q49("q", "'K3', 'BIG', 'K1'"),
                // the whole table, NULL keys included
                "SELECT sym, time, m, s FROM (SELECT sym, time," +
                        " avg((bsize * bid + asize * ask) / (bsize + asize)) OVER (PARTITION BY sym ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS m," +
                        " avg(asize + bsize) OVER (PARTITION BY sym ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s" +
                        " FROM q) ORDER BY sym, time, m, s",
                // two replayed frames of other shapes, and a sum
                "SELECT sym, time, a, b FROM (SELECT sym, time," +
                        " avg(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) a," +
                        " sum(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) b" +
                        " FROM q WHERE sym IN (" + IN_LIST + ")) ORDER BY sym, time, a, b",
                // a running sum folded beside a replayed frame
                "SELECT sym, time, a, s FROM (SELECT sym, time," +
                        " sum(bid) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) a," +
                        " sum(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s" +
                        " FROM q WHERE sym IN (" + IN_LIST + ")) ORDER BY sym, time, a, s",
                // a running sum folded beside warm-up rows
                "SELECT sym, time, s, l FROM (SELECT sym, time," +
                        " sum(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s," +
                        " lag(v) OVER (PARTITION BY sym ORDER BY time) l" +
                        " FROM q) ORDER BY sym, time, s, l",
                // a single key: replayed (idx 48) and folded (idx 52)
                "SELECT time, avg(v) OVER (ORDER BY time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) a FROM q WHERE sym = 'BIG'",
                "SELECT time, sum(v) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM q WHERE sym = 'BIG'",
        };
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
     * Runs the query serially and on the parallel window, which must split keys and fold or
     * replay, twice, rewinding each cursor once; compares the rows bit for bit. Returns the tasks
     * a worker thread went over in the walk-order pass.
     */
    private long assertMatchesSerial(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        final String expected = serial(engine, ctx, query);
        ctx.setParallelWindowEnabled(true);
        long workerPasses = 0;
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            final AsyncWindowRecordCursorFactory async = findAsyncFactoryOrNull(factory);
            Assert.assertNotNull(query, async);
            Assert.assertTrue(query, async.getSplitPlan().hasFold());
            for (int pass = 0; pass < 2; pass++) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertEquals(query, expected, rawRows(cursor, factory.getMetadata()));
                    final AsyncWindowRecordCursor asyncCursor = async.getKeyMajorCursor();
                    Assert.assertTrue(query, asyncCursor.getParallelTaskCount() > 0);
                    workerPasses += asyncCursor.getWorkerPassCount();
                    cursor.toTop();
                    TestUtils.assertEquals(query, expected, rawRows(cursor, factory.getMetadata()));
                    workerPasses += asyncCursor.getWorkerPassCount();
                }
            }
            assertSlotsReleased(factory);
        } finally {
            ctx.setParallelWindowEnabled(false);
        }
        return workerPasses;
    }

    private void assertOps(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        final String[] queries = queries();
        final int[] ops = {
                AsyncWindowSplitPlan.OP_REPLAY,
                AsyncWindowSplitPlan.OP_REPLAY,
                AsyncWindowSplitPlan.OP_REPLAY,
                AsyncWindowSplitPlan.OP_REPLAY,
                AsyncWindowSplitPlan.OP_REPLAY,
                AsyncWindowSplitPlan.OP_FOLD,
                AsyncWindowSplitPlan.OP_FOLD,
                AsyncWindowSplitPlan.OP_REPLAY,
                AsyncWindowSplitPlan.OP_FOLD,
        };
        Assert.assertEquals(queries.length, ops.length);
        for (int q = 0; q < queries.length; q++) {
            ctx.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select(queries[q], ctx)) {
                final AsyncWindowSplitPlan plan = findAsyncFactoryOrNull(factory).getSplitPlan();
                boolean found = false;
                for (int i = 0, n = plan.getPrefixCount(); i < n; i++) {
                    found |= plan.getPrefixOp(i) == ops[q];
                }
                Assert.assertTrue(queries[q], found);
            } finally {
                ctx.setParallelWindowEnabled(false);
            }
        }
    }

    /**
     * Quotes whose sums round at every step: key BIG holds 40% of the rows, K0..K19 the rest, less
     * one in thirteen that is NULL. bid and v are non-representable fractions with now and then a
     * huge, a tiny, a negative zero, a NaN or an infinite value; bsize + asize is 0 one row in 19.
     */
    private void createQuotes(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        engine.execute(
                "create table q (time timestamp, sym symbol index type " + indexType + ", bid double, ask double, bsize int, asize int, v double)" +
                        " timestamp(time) partition by DAY",
                ctx
        );
        engine.execute(
                "insert into q select" +
                        " ((x / 2) * " + (3 * 86_400_000_000L / ROWS) + "L)::timestamp," +
                        " case when x % 5 < 2 then 'BIG' when x % 13 = 0 then null else 'K' || (x % 20) end," +
                        " case when x % 41 = 0 then null when x % 31 = 0 then -0.0 when x % 29 = 0 then 1e16 when x % 29 = 1 then -1e16" +
                        " when x % 23 = 0 then 1e-300 else rnd_double() * 1000.0 / 7.0 + 10.0 end," +
                        " rnd_double() * 100.0 / 3.0 + 10.0," +
                        " case when x % 19 = 0 then 0 else (x % 300)::int end," +
                        " case when x % 19 = 0 then 0 when x % 43 = 0 then null else (x % 200 + 1)::int end," +
                        " case when x % 37 = 0 then null when x % 31 = 0 then -0.0 when x % 29 = 0 then 1e16 when x % 29 = 1 then -1e16" +
                        " when x % 97 = 0 then cast('Infinity' as double) when x % 89 = 0 then cast('-Infinity' as double)" +
                        " when x % 23 = 0 then 1e-300 when x % 13 = 0 then 1e9 + rnd_double() else rnd_double() * 1000.0 / 7.0 - 50.0 end" +
                        " from long_sequence(" + ROWS + ")",
                ctx
        );
    }

    private void inPool(PoolTest test) throws Exception {
        final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
        TestUtils.execute(pool, (engine, compiler, context) -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
            ctx.changePageFrameSizes(1, 128);
            test.run(engine, ctx);
        }, configuration, LOG);
    }

    @FunctionalInterface
    private interface PoolTest {
        void run(CairoEngine engine, SqlExecutionContextImpl ctx) throws Exception;
    }
}

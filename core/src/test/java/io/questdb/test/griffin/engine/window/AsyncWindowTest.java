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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursor;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A window whose every function is partitioned by the key of a key-major index scan runs on the
 * shared query workers ({@code Async Window}). Its output must be the serial window's, row for
 * row: every test compares the two, on the same data, with the parallel window switched off and
 * on.
 * <p>
 * The tests shrink the page frames, so that each key's rows span many frames and partitions, and
 * the tasks, so that a few hundred rows make many tasks and rounds. A key above
 * {@code cairo.sql.parallel.window.max.key.rows} runs on the query's own thread in chunks.
 */
@RunWith(Parameterized.class)
public class AsyncWindowTest extends AbstractCairoTest {
    private static final String IDX50_COLUMNS = "sym, ts, " +
            "avg((bsize * bid + asize * ask) / (bsize + asize)) over (partition by sym rows between 4 preceding and current row) mid, " +
            "avg(asize + bsize) over (partition by sym rows between 4 preceding and current row) size";
    private final String indexType;

    public AsyncWindowTest(String indexType) {
        this.indexType = indexType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"bitmap"},
                {"posting"},
        });
    }

    @Override
    public void setUp() {
        super.setUp();
        // 64 rows per page frame: every key of the fixtures spans many frames
        sqlExecutionContext.changePageFrameSizes(1, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 400);
    }

    @Test
    public void testCachedFactoryRerunsAndRewinds() throws Exception {
        assertMemoryLeak(() -> {
            createQuote("DAY", 3_000);
            final String query = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'S3', 'BIG', null) order by sym";
            final String expected = serial(query);
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = select(query)) {
                assertAsync(factory, true);
                for (int run = 0; run < 3; run++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // a partial pass, then a rewind mid-round, then a full pass
                        for (int i = 0; i < 77 && cursor.hasNext(); i++) {
                            // skip
                        }
                        cursor.toTop();
                        TestUtils.assertEquals(expected, print(cursor, factory));
                        cursor.toTop();
                        TestUtils.assertEquals(expected, print(cursor, factory));
                    }
                }
                assertSlotsReleased(factory);
            }
        });
    }

    @Test
    public void testBindVariablesChangeBetweenRuns() throws Exception {
        assertMemoryLeak(() -> {
            createQuote("DAY", 2_000);
            final String query = "select " + IDX50_COLUMNS + " from q where sym in ($1, $2, $3) order by sym";
            final String[][] binds = {{"S1", "S5", "BIG"}, {"S7", null, "S2"}, {"NOPE", "S3", "S3"}};
            final String[] expected = new String[binds.length];
            sqlExecutionContext.setParallelWindowEnabled(false);
            for (int b = 0; b < binds.length; b++) {
                bind(binds[b]);
                expected[b] = printToString(query);
            }
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = select(query)) {
                assertAsync(factory, true);
                for (int b = 0; b < binds.length; b++) {
                    bind(binds[b]);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        TestUtils.assertEquals(expected[b], print(cursor, factory));
                    }
                }
            }
        });
    }

    @Test
    public void testCancelMidQueryReleasesEverything() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
                ctx.changePageFrameSizes(1, 64);
                createQuote(engine, ctx, "DAY", 20_000);
                final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                        engine,
                        new DefaultSqlExecutionCircuitBreakerConfiguration()
                );
                try {
                    ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), circuitBreaker);
                    final String query = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'BIG', 'S4', 'S9') order by sym";
                    try (RecordCursorFactory factory = engine.select(query, ctx)) {
                        assertAsync(factory, true);
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            for (int i = 0; i < 100; i++) {
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
                        }
                        assertSlotsReleased(factory);
                        // the factory still runs once the breaker is reset
                        circuitBreaker.resetTimer();
                        circuitBreaker.unsetTimer();
                        ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), null);
                        final StringSink expected = new StringSink();
                        ctx.setParallelWindowEnabled(false);
                        engine.print(query, expected, ctx);
                        ctx.setParallelWindowEnabled(true);
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertEquals(expected, print(cursor, factory));
                        }
                    }
                } finally {
                    Misc.free(circuitBreaker);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testEveryFunctionFamilyMatchesSerial() throws Exception {
        assertMemoryLeak(() -> {
            createQuote("DAY", 2_000);
            final String w = " over (partition by sym rows between 3 preceding and current row)";
            final String cumulative = " over (partition by sym rows between unbounded preceding and current row)";
            final String whole = " over (partition by sym)";
            final String[] functions = {
                    "avg(bid)" + w, "sum(bsize)" + w, "min(ask)" + w, "max(asize)" + w, "count()" + w, "count(bid)" + w,
                    "avg(bid)" + cumulative, "sum(x)" + cumulative, "min(bid)" + cumulative, "max(x)" + cumulative,
                    "row_number()" + whole, "first_value(bid)" + w, "last_value(x)" + w,
                    "lag(x)" + whole, "lag(bid, 2)" + whole,
                    "avg(bid) over (partition by sym rows between 5 preceding and 1 preceding)",
                    "sum(bid * 2 + 1) over (partition by sym, bsize % 3 rows between 2 preceding and current row)",
            };
            for (String function : functions) {
                assertParallelMatchesSerial("select sym, x, " + function + " f from q where sym in ('S1', 'BIG', 'S2', null, 'S5') order by sym");
            }
            // several functions, fused and not, in one window
            final StringSink columns = new StringSink();
            for (int i = 0; i < functions.length; i++) {
                columns.put(", ").put(functions[i]).put(" f").put(i);
            }
            assertParallelMatchesSerial("select sym, ts" + columns + " from q where sym in ('S1', 'BIG', 'S2', null, 'S5') order by sym");
        });
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createQuote("DAY", 100);
            sqlExecutionContext.setParallelWindowEnabled(true);
            final StringSink actualPlan = new StringSink();
            printSql("explain select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2') order by sym", actualPlan);
            TestUtils.assertContains(
                    actualPlan,
                    "QUERY PLAN\n" +
                            "Async Window workers: 1\n" +
                            "  functions: [avg(bsize*bid+asize*ask/bsize+asize) over (partition by [sym] rows between 4 preceding and current row)," +
                            "avg(asize+bsize) over (partition by [sym] rows between 4 preceding and current row)]\n" +
                            "  keyShards: sym\n" +
                            "    FilterOnValues symbolOrder: asc\n" +
                            "      keyMajor: true\n" +
                            "        Cursor-order scan\n"
            );
            sqlExecutionContext.setParallelWindowEnabled(false);
            final StringSink plan = new StringSink();
            printSql("explain select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2') order by sym", plan);
            TestUtils.assertContains(plan, "Window");
            TestUtils.assertNotContains(plan, "Async Window");
        });
    }

    @Test
    public void testIdx50ShapeAcrossPartitionsAndFrames() throws Exception {
        assertMemoryLeak(() -> {
            for (String partitionBy : new String[]{"NONE", "DAY", "HOUR"}) {
                execute("drop table if exists q");
                createQuote(partitionBy, 3_000);
                assertParallelMatchesSerial("select sym, ts, mid, size from (select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'BIG', null) order by sym)");
                assertParallelMatchesSerial("select " + IDX50_COLUMNS + " from q where sym in ('S3', 'S1', 'BIG') order by sym desc");
                assertParallelMatchesSerial("select " + IDX50_COLUMNS + " from q where sym in ('S3', 'S1', 'BIG') and bid > 0.5 order by sym");
                assertParallelMatchesSerial("select " + IDX50_COLUMNS + " from q where sym != 'S2' order by sym");
                assertParallelMatchesSerial("select " + IDX50_COLUMNS + " from q where sym not in ('S2', 'BIG', null) order by sym desc");
            }
        });
    }

    @Test
    public void testIneligibleWindowsStaySerial() throws Exception {
        assertMemoryLeak(() -> {
            createQuote("DAY", 300);
            sqlExecutionContext.setParallelWindowEnabled(true);
            final String in = " from q where sym in ('S1', 'S2', 'BIG') order by sym";
            // not partitioned by the scan's key
            assertSerialPlan("select sym, avg(bid) over (partition by bsize rows between 2 preceding and current row) a" + in);
            // one of the functions is not partitioned at all
            assertSerialPlan("select sym, avg(bid) over (partition by sym rows between 2 preceding and current row) a, " +
                    "avg(bid) over (rows between 2 preceding and current row) b" + in);
            // the scan is not key-major: no ORDER BY sym
            assertSerialPlan("select sym, avg(bid) over (partition by sym rows between 2 preceding and current row) a from q where sym in ('S1', 'S2')");
            // switched off
            sqlExecutionContext.setParallelWindowEnabled(false);
            assertSerialPlan("select sym, avg(bid) over (partition by sym rows between 2 preceding and current row) a" + in);
        });
    }

    @Test
    public void testKeysLargerThanATaskRunOnTheQueryThread() throws Exception {
        assertMemoryLeak(() -> {
            createQuote("DAY", 5_000);
            // BIG holds about half of the rows: far above max.key.rows (400), so it runs on the
            // query's thread in chunks between the rounds of the small keys around it
            final String query = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'BIG', 'S2', 'S3') order by sym";
            assertParallelMatchesSerial(query);
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                }
                final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                // BIG's ~2500 rows: its first 400 collected with the round before it, then chunks of 50
                Assert.assertTrue(asyncCursor.getLargeKeyChunkCount() > 10);
                Assert.assertTrue(asyncCursor.getParallelRoundCount() > 1);
            }
            // and on its own: one key, nothing for the workers
            assertParallelMatchesSerial("select " + IDX50_COLUMNS + " from q where sym in ('BIG', 'NOPE') order by sym");
            // a key exactly at the limit, and one row above it
            for (long maxKeyRows : new long[]{countOf("S1"), countOf("S1") - 1}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, maxKeyRows);
                assertParallelMatchesSerial("select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2') order by sym");
            }
        });
    }

    @Test
    public void testLimit() throws Exception {
        assertMemoryLeak(() -> {
            createQuote("DAY", 3_000);
            final String window = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'BIG', 'S2', 'S3', null) order by sym";
            assertParallelMatchesSerial(window + " limit 7");
            assertParallelMatchesSerial(window + " limit 120, 140");
            assertParallelMatchesSerial(window + " limit 1300, 1310");
            assertParallelMatchesSerial(window + " limit -5");
            assertParallelMatchesSerial("select count(), sum(mid), sum(size) from (" + window + ")");
        });
    }

    @Test
    public void testManyTinyKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type " + indexType + ", x long, ts timestamp) timestamp(ts) partition by DAY");
            // 2000 keys of 1 to 3 rows each, interleaved in time
            execute("insert into t select 'K' || (x % 2000), x, (x * 60_000_000L)::timestamp from long_sequence(4500)");
            final StringSink in = new StringSink();
            in.put("'K0'");
            for (int i = 1; i < 2000; i += 3) {
                in.put(", 'K").put(i).put('\'');
            }
            final String query = "select sym, x, sum(x) over (partition by sym rows between 1 preceding and current row) s, " +
                    "row_number() over (partition by sym) rn from t where sym in (" + in + ") order by sym";
            assertParallelMatchesSerial(query);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 1);
            assertParallelMatchesSerial(query);
        });
    }

    @Test
    public void testNullKeyOnly() throws Exception {
        assertMemoryLeak(() -> {
            createQuote("DAY", 2_000);
            // a single key is not an IN-list scan, so it stays serial
            assertParallelMatchesSerial("select " + IDX50_COLUMNS + " from q where sym in (null) order by sym", false);
            assertParallelMatchesSerial("select " + IDX50_COLUMNS + " from q where sym in (null, 'NOPE') order by sym");
            assertParallelMatchesSerial("select " + IDX50_COLUMNS + " from q where sym in ('NOPE', 'NOPE2') order by sym");
        });
    }

    @Test
    public void testOnWorkerPool() throws Exception {
        // the small keys, ~1400 rows each, go to the workers a few per round; BIG runs on the
        // query's thread
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 200);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 5_000);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                ((SqlExecutionContextImpl) ctx).changePageFrameSizes(1, 64);
                createQuote(engine, ctx, "DAY", 30_000);
                final String[] queries = {
                        "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'BIG', null) order by sym",
                        "select " + IDX50_COLUMNS + " from q where sym != 'S4' order by sym desc",
                        "select sym, x, sum(x) over (partition by sym rows between unbounded preceding and current row) s, " +
                                "count() over (partition by sym rows between 9 preceding and current row) c " +
                                "from q where sym in ('S1', 'S2', 'S3', 'S9') order by sym limit 333",
                };
                for (String query : queries) {
                    ctx.setParallelWindowEnabled(false);
                    final StringSink expected = new StringSink();
                    engine.print(query, expected, ctx);
                    ctx.setParallelWindowEnabled(true);
                    try (RecordCursorFactory factory = engine.select(query, ctx)) {
                        final StringSink plan = new StringSink();
                        engine.print("explain " + query, plan, ctx);
                        TestUtils.assertContains(plan, "Async Window workers: 4");
                        assertAsync(factory, true);
                        for (int run = 0; run < 3; run++) {
                            try (RecordCursor cursor = factory.getCursor(ctx)) {
                                TestUtils.assertEquals(expected, print(cursor, factory));
                            }
                            // the rows went through the workers, in many tasks and rounds
                            final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                            Assert.assertTrue(asyncCursor.getParallelRoundCount() > 1);
                            Assert.assertTrue(asyncCursor.getParallelTaskCount() > asyncCursor.getParallelRoundCount());
                        }
                        assertSlotsReleased(factory);
                    }
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testTimeoutOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            // The first run counts the breaker's clock reads; the second trips the breaker half way.
            final AtomicLong ticks = new AtomicLong();
            final AtomicLong tripAt = new AtomicLong(Long.MAX_VALUE);
            final SqlExecutionCircuitBreakerConfiguration breakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public @NotNull MillisecondClock getClock() {
                    return () -> ticks.incrementAndGet() < tripAt.get() ? 0 : Long.MAX_VALUE;
                }

                @Override
                public long getQueryTimeout() {
                    return 1;
                }
            };
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
                ctx.changePageFrameSizes(1, 64);
                createQuote(engine, ctx, "DAY", 20_000);
                final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, breakerConfiguration);
                try {
                    ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), circuitBreaker);
                    final String query = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6') order by sym";
                    try (RecordCursorFactory factory = engine.select(query, ctx)) {
                        assertAsync(factory, true);
                        ticks.set(0);
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                        }
                        final long fullRunTicks = ticks.get();
                        Assert.assertTrue(fullRunTicks > 4);
                        ticks.set(0);
                        tripAt.set(fullRunTicks / 2);
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("query did not time out");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
                        }
                        assertSlotsReleased(factory);
                    }
                } finally {
                    Misc.free(circuitBreaker);
                }
            }, configuration, LOG);
        });
    }

    private static void assertAsync(RecordCursorFactory factory, boolean expected) {
        boolean found = false;
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncWindowRecordCursorFactory) {
                found = true;
                break;
            }
        }
        Assert.assertEquals("Async Window in the factory tree", expected, found);
    }

    private static void assertSlotsReleased(RecordCursorFactory factory) {
        final PerWorkerLocks locks = TestUtils.findPerWorkerLocks(factory, "async window");
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    private static AsyncWindowRecordCursor findAsyncCursor(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncWindowRecordCursorFactory asyncFactory) {
                return asyncFactory.getAsyncCursor();
            }
        }
        throw new AssertionError("no Async Window in the factory tree");
    }

    private static String print(RecordCursor cursor, RecordCursorFactory factory) {
        final StringSink sink = new StringSink();
        CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        return sink.toString();
    }

    private void assertParallelMatchesSerial(String query) throws Exception {
        assertParallelMatchesSerial(query, true);
    }

    private void assertParallelMatchesSerial(String query, boolean expectAsync) throws Exception {
        final String expected = serial(query);
        sqlExecutionContext.setParallelWindowEnabled(true);
        try {
            try (RecordCursorFactory factory = select(query)) {
                assertAsync(factory, expectAsync);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertEquals(query, expected, print(cursor, factory));
                }
                if (expectAsync) {
                    assertSlotsReleased(factory);
                    // not the serial fallback: some rows went through tasks or chunks
                    final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                    final boolean hasRows = expected.indexOf('\n') < expected.length() - 1;
                    Assert.assertEquals(
                            query,
                            hasRows,
                            asyncCursor.getParallelTaskCount() + asyncCursor.getLargeKeyChunkCount() > 0
                    );
                }
            }
        } finally {
            sqlExecutionContext.setParallelWindowEnabled(false);
        }
    }

    private void assertSerialPlan(String query) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            assertAsync(factory, false);
            boolean found = false;
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                found |= f instanceof WindowRecordCursorFactory;
            }
            Assert.assertTrue("serial Window in the factory tree: " + query, found);
        }
    }

    private void bind(String[] values) throws Exception {
        bindVariableService.clear();
        for (int i = 0; i < values.length; i++) {
            bindVariableService.setStr(i, values[i]);
        }
    }

    private long countOf(String sym) throws Exception {
        final StringSink sink = new StringSink();
        printSql("select count() from q where sym = '" + sym + "'", sink);
        return Long.parseLong(sink.toString().split("\n")[1]);
    }

    /**
     * {@code quote}-like table {@code q}: a symbol index on {@code sym}, half of the rows in key
     * BIG, the rest spread evenly over S0..S9, less one in eleven that is NULL, interleaved in time
     * over three days.
     */
    private void createQuote(String partitionBy, int rows) throws Exception {
        createQuote(engine, sqlExecutionContext, partitionBy, rows);
    }

    private void createQuote(CairoEngine engine, SqlExecutionContext ctx, String partitionBy, int rows) throws Exception {
        engine.execute(
                "create table q (sym symbol index type " + indexType + ", bid float, bsize int, ask float, asize int, x long, ts timestamp)" +
                        " timestamp(ts) partition by " + partitionBy,
                ctx
        );
        engine.execute(
                "insert into q select" +
                        " case when x % 2 = 0 then 'BIG' when x % 11 = 0 then null else 'S' || (x / 2 % 10) end," +
                        " rnd_float(), rnd_int(1, 1000, 0), rnd_float(), rnd_int(1, 1000, 0), x," +
                        " (x * " + (3 * 86_400_000_000L / rows) + "L)::timestamp" +
                        " from long_sequence(" + rows + ")",
                ctx
        );
    }

    private String printToString(String query) throws Exception {
        final StringSink sink = new StringSink();
        printSql(query, sink);
        return sink.toString();
    }

    private String serial(String query) throws Exception {
        sqlExecutionContext.setParallelWindowEnabled(false);
        try (RecordCursorFactory factory = select(query)) {
            assertAsync(factory, false);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                return print(cursor, factory);
            }
        }
    }
}

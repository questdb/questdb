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
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.window.AsyncWindowAtom;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursor;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
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
 * on, and checks that each row of the result was computed exactly once, by the query's thread
 * (prefix, large keys) or by a task.
 * <p>
 * The tests shrink the page frames, so that each key's rows span many frames and partitions, and
 * the tasks and rounds, so that a few hundred rows make many of them. The query's own thread
 * computes the first {@code min.rows} rows itself and any key above {@code max.key.rows}. The
 * tests that run without a worker pool have their tasks stolen by the query's thread, through the
 * owner's copy of the functions; the {@code OnWorkerPool} variants run on four real workers and
 * check that the workers' copies computed tasks.
 */
@RunWith(Parameterized.class)
public class AsyncWindowTest extends AbstractCairoTest {
    private static final String IDX50_COLUMNS = "sym, ts, " +
            "avg((bsize * bid + asize * ask) / (bsize + asize)) over (partition by sym rows between 4 preceding and current row) mid, " +
            "avg(asize + bsize) over (partition by sym rows between 4 preceding and current row) size";
    private static final long MAX_KEY_ROWS = 400;
    private static final long MIN_ROWS = 100;
    private static final long ROUND_ROWS = 200;
    private static final long TASK_ROWS = 50;
    private static final String W = " over (partition by sym rows between 3 preceding and current row)";
    private static final String[] FUNCTIONS = {
            "avg(bid)" + W, "sum(bsize)" + W, "min(ask)" + W, "max(asize)" + W, "count()" + W, "count(bid)" + W,
            "avg(bid) over (partition by sym rows between unbounded preceding and current row)",
            "sum(x) over (partition by sym rows between unbounded preceding and current row)",
            "min(bid) over (partition by sym rows between unbounded preceding and current row)",
            "max(x) over (partition by sym rows between unbounded preceding and current row)",
            "row_number() over (partition by sym)", "first_value(bid)" + W, "last_value(x)" + W,
            "last_value(bid) ignore nulls" + W,
            "lag(x) over (partition by sym)", "lag(bid, 2) over (partition by sym)",
            "avg(bid) over (partition by sym rows between 5 preceding and 1 preceding)",
            "sum(bid * 2 + 1) over (partition by sym, bsize % 3 rows between 2 preceding and current row)",
    };
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
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, TASK_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, MAX_KEY_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, MIN_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, ROUND_ROWS);
    }

    @Test
    public void testBindVariablesChangeBetweenRuns() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 2_000);
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
    public void testCachedFactoryRerunsAndRewinds() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 3_000);
            final String query = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'S3', 'BIG', null) order by sym";
            final String expected = serial(engine, sqlExecutionContext, query);
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = select(query)) {
                assertAsync(factory, true);
                for (int run = 0; run < 3; run++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // a partial pass that ends inside a round, a rewind, then two full passes
                        for (int i = 0; i < 377 && cursor.hasNext(); i++) {
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
    public void testCachedPlanOverPartitionTurnedParquetRunsSerially() throws Exception {
        // A plan made over native partitions still runs once one turns Parquet, whose decoded
        // frames have no stable address for other threads: the query's thread computes it all.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 64);
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 3_000);
            final String query = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'BIG', null) order by sym";
            final String expected = serial(engine, sqlExecutionContext, query);
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = select(query)) {
                assertAsync(factory, true);
                final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertEquals(expected, print(cursor, factory));
                }
                Assert.assertTrue(asyncCursor.getParallelTaskCount() > 0);
                execute("alter table q convert partition to parquet list '1970-01-01'");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertEquals(expected, print(cursor, factory));
                    cursor.toTop();
                    TestUtils.assertEquals(expected, print(cursor, factory));
                }
                Assert.assertEquals(0, asyncCursor.getParallelTaskCount());
                Assert.assertEquals(0, asyncCursor.getPrefixRowCount() + asyncCursor.getLargeKeyRowCount());
                assertSlotsReleased(factory);
            }
        });
    }

    @Test
    public void testCancelMidQueryThenRewindRunsAgain() throws Exception {
        // the S keys, ~900 rows each, go to the workers
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 2_000);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createQuote(engine, ctx, "DAY", 20_000);
            final String query = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'BIG', 'S4', 'S9') order by sym";
            final String expected = serial(engine, ctx, query);
            final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    engine,
                    new DefaultSqlExecutionCircuitBreakerConfiguration()
            );
            try {
                ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), circuitBreaker);
                ctx.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = engine.select(query, ctx)) {
                    assertAsync(factory, true);
                    // the window's own cursor: the query progress wrapper closes its cursor on error
                    try (RecordCursor cursor = findAsyncFactory(factory).getCursor(ctx)) {
                        // past the prefix, which is all of BIG, the first key, into the rounds
                        for (int i = 0; i < 12_000; i++) {
                            Assert.assertTrue(cursor.hasNext());
                        }
                        final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                        Assert.assertTrue(
                                "prefix=" + asyncCursor.getPrefixRowCount() + ", tasks=" + asyncCursor.getTaskRowCount()
                                        + ", large=" + asyncCursor.getLargeKeyRowCount(),
                                asyncCursor.getParallelRoundCount() > 0
                        );
                        circuitBreaker.cancel();
                        try {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("cancelled query ran to completion");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getMessage(), e.isCancellation());
                        }
                        // a rewind after the failure starts a clean pass
                        circuitBreaker.clearCancelSentinel();
                        circuitBreaker.resetTimer();
                        cursor.toTop();
                        TestUtils.assertEquals(expected, print(cursor, factory));
                    }
                    assertSlotsReleased(factory);
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertEquals(expected, print(cursor, factory));
                    }
                }
            } finally {
                Misc.free(circuitBreaker);
            }
        }));
    }

    @Test
    public void testEveryFunctionFamilyMatchesSerial() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 2_000);
            assertFunctionFamilies(engine, sqlExecutionContext, false);
        });
    }

    @Test
    public void testEveryFunctionFamilyMatchesSerialOnWorkerPool() throws Exception {
        // rounds of several tasks of ~1,400 rows: a round outlasts the wake-up of a parked worker
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 200);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 5_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 3_000);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createQuote(engine, ctx, "DAY", 30_000);
            // for every function, the workers' own copies computed some of the tasks
            assertFunctionFamilies(engine, ctx, true);
        }));
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 2_000);
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
                createQuote(engine, sqlExecutionContext, partitionBy, 3_000);
                assertMatchesSerial(engine, sqlExecutionContext, "select sym, ts, mid, size from (select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'BIG', null) order by sym)");
                assertMatchesSerial(engine, sqlExecutionContext, "select " + IDX50_COLUMNS + " from q where sym in ('S3', 'S1', 'BIG') order by sym desc");
                assertMatchesSerial(engine, sqlExecutionContext, "select " + IDX50_COLUMNS + " from q where sym in ('S3', 'S1', 'BIG') and bid > 0.5 order by sym");
                assertMatchesSerial(engine, sqlExecutionContext, "select " + IDX50_COLUMNS + " from q where sym != 'S2' order by sym");
                assertMatchesSerial(engine, sqlExecutionContext, "select " + IDX50_COLUMNS + " from q where sym not in ('S2', 'BIG', null) order by sym desc");
            }
        });
    }

    @Test
    public void testIneligibleWindowsStaySerial() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 300);
            sqlExecutionContext.setParallelWindowEnabled(true);
            final String in = " from q where sym in ('S1', 'S2', 'BIG') order by sym";
            // not partitioned by the scan's key
            assertSerialPlan("select sym, avg(bid) over (partition by bsize rows between 2 preceding and current row) a" + in);
            // one of the functions is not partitioned at all
            assertSerialPlan("select sym, avg(bid) over (partition by sym rows between 2 preceding and current row) a, " +
                    "avg(bid) over (rows between 2 preceding and current row) b" + in);
            // the scan is not key-major: no ORDER BY sym
            assertSerialPlan("select sym, avg(bid) over (partition by sym rows between 2 preceding and current row) a from q where sym in ('S1', 'S2')");
            // a random argument: the query's one Rnd is not thread safe
            assertSerialPlan("select sym, avg(rnd_double(0)) over (partition by sym rows between 2 preceding and current row) a" + in);
            assertSerialPlan("select sym, sum(bid) over (partition by sym, rnd_int(1, 3, 0) rows between 2 preceding and current row) a" + in);
            // switched off
            sqlExecutionContext.setParallelWindowEnabled(false);
            assertSerialPlan("select sym, avg(bid) over (partition by sym rows between 2 preceding and current row) a" + in);
        });
    }

    @Test
    public void testKeysLargerThanATaskRunOnTheQueryThread() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 5_000);
            assertLargeKeys(engine, sqlExecutionContext);
        });
    }

    @Test
    public void testKeysLargerThanATaskRunOnTheQueryThreadOnWorkerPool() throws Exception {
        // the S keys, ~900 rows each, go to the workers; BIG, 10,000 rows, does not
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 2_000);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createQuote(engine, ctx, "DAY", 20_000);
            assertWorkersTookTasks(() -> assertLargeKeys(engine, ctx));
        }));
    }

    @Test
    public void testLimit() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 3_000);
            final String window = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'BIG', 'S2', 'S3', null) order by sym";
            assertMatchesSerial(engine, sqlExecutionContext, window + " limit 7");
            assertMatchesSerial(engine, sqlExecutionContext, window + " limit 120, 140");
            assertMatchesSerial(engine, sqlExecutionContext, window + " limit 1300, 1310");
            assertMatchesSerial(engine, sqlExecutionContext, window + " limit -5");
            assertMatchesSerial(engine, sqlExecutionContext, "select count(), sum(mid), sum(size) from (" + window + ")");
        });
    }

    @Test
    public void testLimitAndSmallResultsStayOnTheQueryThread() throws Exception {
        // A LIMIT reads its rows from the first prefix chunk, and a result smaller than min.rows
        // is all prefix: neither waits for a round of tasks nor opens a worker slot.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 2_000);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createQuote(engine, ctx, "DAY", 20_000);
            final String window = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'S3', 'BIG') order by sym";
            final String expected = serial(engine, ctx, window + " limit 10");
            ctx.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select(window + " limit 10", ctx)) {
                assertAsync(factory, true);
                final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                for (int run = 0; run < 3; run++) {
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertEquals(expected, print(cursor, factory));
                    }
                    Assert.assertEquals(0, asyncCursor.getParallelRoundCount());
                    // only the first chunk of the walk was computed
                    Assert.assertTrue(asyncCursor.getPrefixRowCount() <= 256);
                    Assert.assertEquals(0, findAtom(factory).getWorkerSlotTaskCount());
                }
            }
            // S1 and S2 hold ~1,800 rows together: all of it is prefix
            assertMatchesSerial(engine, ctx, "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2') order by sym");
            ctx.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select("select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2') order by sym", ctx)) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    print(cursor, factory);
                }
                Assert.assertEquals(0, findAsyncCursor(factory).getParallelRoundCount());
            }
        }));
    }

    @Test
    public void testLargeKeysGiveTheirRowIdMemoryBack() throws Exception {
        // Keys of 70,000 rows, above max.key.rows, every tenth among keys of 1,500 rows: each ends
        // the round it is met in, after its rows grew that round's last task's row id list. The
        // lists must shrink back, so the query's row id memory stays near a few tasks' worth.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 2_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 60_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 8_000);
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type " + indexType + ", x long, ts timestamp) timestamp(ts) partition by DAY");
            // K00..K99; K00, K10, ... K90 take 70,000 rows each, the others 1,500
            execute("insert into t select 'K' || lpad(((x - 1) % 100)::string, 2, '0'), x, (x * 1_000_000L)::timestamp from long_sequence(150_000)");
            execute("insert into t select 'K' || lpad(((x - 1) % 10 * 10)::string, 2, '0'), x, ((150_000 + x) * 1_000_000L)::timestamp from long_sequence(685_000)");
            final StringSink in = new StringSink();
            for (int i = 0; i < 100; i++) {
                in.put(i > 0 ? ", " : "").put("'K").put(i < 10 ? "0" : "").put(i).put('\'');
            }
            final String query = "select sym, x, sum(x) over (partition by sym rows between 2 preceding and current row) s from t where sym in (" + in + ") order by sym";
            final String expected = serial(engine, sqlExecutionContext, query);
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = select(query)) {
                assertAsync(factory, true);
                final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                long peakAfterLargeKey = 0;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final StringSink actual = new StringSink();
                    CursorPrinter.println(cursor, factory.getMetadata(), actual, true, false);
                    TestUtils.assertEquals(expected, actual);
                    // the drain has passed all ten large keys; the cursor still holds its lists
                    peakAfterLargeKey = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD) - before;
                    final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                    // K00 is the prefix, the other nine are large keys
                    Assert.assertEquals(9 * 70_000, asyncCursor.getLargeKeyRowCount());
                    Assert.assertTrue(asyncCursor.getParallelRoundCount() > 10);
                }
                // 2 rounds of at most 4 tasks, the prefix's and the large key's lists: at most 11
                // lists of 2 * task.rows row ids. A list that kept a large key's capacity is
                // 60,000+ row ids, 480 KB, alone.
                Assert.assertTrue("row id memory still held: " + peakAfterLargeKey, peakAfterLargeKey <= 11 * 2 * 2_000 * 8L);
            }
        });
    }

    @Test
    public void testManyTinyKeys() throws Exception {
        assertMemoryLeak(() -> assertManyTinyKeys(engine, sqlExecutionContext));
    }

    @Test
    public void testManyTinyKeysOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> inPool((engine, ctx) -> assertWorkersTookTasks(() -> assertManyTinyKeys(engine, ctx))));
    }

    @Test
    public void testWorkerCopiesCappedByRoundSize() throws Exception {
        // a round of 100 rows holds at most 2 tasks of 50: 4 workers get 2 copies, and share them
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 100);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createQuote(engine, ctx, "DAY", 6_000);
            final String query = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'BIG', null) order by sym";
            ctx.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select(query, ctx)) {
                Assert.assertEquals(2, findAtom(factory).getWorkerSlotCount());
            }
            assertWorkersTookTasks(() -> assertMatchesSerial(engine, ctx, query));
        }));
    }

    @Test
    public void testNullKeyOnly() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 2_000);
            // a single key is not an IN-list scan, so it stays serial
            assertMatchesSerial(engine, sqlExecutionContext, "select " + IDX50_COLUMNS + " from q where sym in (null) order by sym", false);
            assertMatchesSerial(engine, sqlExecutionContext, "select " + IDX50_COLUMNS + " from q where sym in (null, 'NOPE') order by sym");
            assertMatchesSerial(engine, sqlExecutionContext, "select " + IDX50_COLUMNS + " from q where sym in ('NOPE', 'NOPE2') order by sym");
        });
    }

    @Test
    public void testOnWorkerPool() throws Exception {
        // the small keys, ~1400 rows each, go to the workers a few per round; BIG runs on the
        // query's thread
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 200);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 5_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 3_000);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createQuote(engine, ctx, "DAY", 30_000);
            final String[] queries = {
                    "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'BIG', null) order by sym",
                    "select " + IDX50_COLUMNS + " from q where sym != 'S4' order by sym desc",
                    "select sym, x, sum(x) over (partition by sym rows between unbounded preceding and current row) s, " +
                            "count() over (partition by sym rows between 9 preceding and current row) c " +
                            "from q where sym in ('S1', 'S2', 'S3', 'S9') order by sym limit 3333",
            };
            long workerTasks = 0;
            for (String query : queries) {
                final String expected = serial(engine, ctx, query);
                ctx.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = engine.select(query, ctx)) {
                    final StringSink plan = new StringSink();
                    engine.print("explain " + query, plan, ctx);
                    TestUtils.assertContains(plan, "Async Window workers: 4");
                    for (int run = 0; run < 3; run++) {
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertEquals(expected, print(cursor, factory));
                        }
                        final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                        Assert.assertTrue(asyncCursor.getParallelRoundCount() > 0);
                        Assert.assertTrue(asyncCursor.getParallelTaskCount() > asyncCursor.getParallelRoundCount());
                        // a round's rows stay within round.rows plus one key, whatever the worker count
                        Assert.assertTrue(asyncCursor.getMaxRoundRows() <= 3_000 + 5_000);
                        workerTasks += findAtom(factory).getWorkerSlotTaskCount();
                    }
                    assertSlotsReleased(factory);
                }
            }
            Assert.assertTrue(workerTasks > 0);
        }));
    }

    @Test
    public void testSelectiveInListOnLargeTableStaysSerial() throws Exception {
        // the plan estimates an IN list's rows as its keys' share of the table: 2 keys of 1,000
        // stay serial, and compile no worker copies; 100 keys of 1,000 do not
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 1_000);
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type " + indexType + ", x long, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into t select 'K' || (x % 1000), x, (x * 60_000_000L)::timestamp from long_sequence(20_000)");
            sqlExecutionContext.setParallelWindowEnabled(true);
            final String window = "select sym, x, sum(x) over (partition by sym rows between 2 preceding and current row) s from t where sym in (";
            assertSerialPlan(window + "'K1', 'K2') order by sym");
            final StringSink in = new StringSink();
            for (int i = 0; i < 100; i++) {
                in.put(i > 0 ? ", " : "").put("'K").put(i).put('\'');
            }
            assertMatchesSerial(engine, sqlExecutionContext, window + in + ") order by sym");
        });
    }

    @Test
    public void testSmallTableStaysSerial() throws Exception {
        // a table under min.rows plans the serial window: no worker copies are compiled
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 1_000);
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 999);
            sqlExecutionContext.setParallelWindowEnabled(true);
            final String query = "select " + IDX50_COLUMNS + " from q where sym != 'NOPE' order by sym";
            assertSerialPlan(query);
            execute("insert into q select 'S1', 1, 1, 1, 1, 1, '1970-01-04'::timestamp from long_sequence(1)");
            assertMatchesSerial(engine, sqlExecutionContext, query);
        });
    }

    @Test
    public void testTimeoutOnWorkerPoolThenRewind() throws Exception {
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
            inPool((engine, ctx) -> {
                createQuote(engine, ctx, "DAY", 20_000);
                final String query = "select " + IDX50_COLUMNS + " from q where sym in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6') order by sym";
                final String expected = serial(engine, ctx, query);
                final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, breakerConfiguration);
                try {
                    ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), circuitBreaker);
                    ctx.setParallelWindowEnabled(true);
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
                        // the window's own cursor: the query progress wrapper closes its cursor on error
                        try (RecordCursor cursor = findAsyncFactory(factory).getCursor(ctx)) {
                            try {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                }
                                Assert.fail("query did not time out");
                            } catch (CairoException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
                            }
                            // a rewind after the failure starts a clean pass
                            tripAt.set(Long.MAX_VALUE);
                            circuitBreaker.resetTimer();
                            cursor.toTop();
                            TestUtils.assertEquals(expected, print(cursor, factory));
                        }
                        assertSlotsReleased(factory);
                    }
                } finally {
                    Misc.free(circuitBreaker);
                }
            });
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

    // A round of small tasks may be all stolen by the query's thread before a worker wakes up:
    // repeat the pass, every one compared with the serial window, until a worker has taken some.
    private static void assertWorkersTookTasks(WorkerTaskCount pass) throws Exception {
        long workerTasks = 0;
        for (int i = 0; i < 10 && workerTasks == 0; i++) {
            workerTasks = pass.run();
        }
        Assert.assertTrue("no worker slot computed a task", workerTasks > 0);
    }

    private static AsyncWindowRecordCursor findAsyncCursor(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncWindowRecordCursorFactory asyncFactory) {
                return asyncFactory.getAsyncCursor();
            }
        }
        throw new AssertionError("no Async Window in the factory tree");
    }

    private static AsyncWindowRecordCursorFactory findAsyncFactory(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncWindowRecordCursorFactory asyncFactory) {
                return asyncFactory;
            }
        }
        throw new AssertionError("no Async Window in the factory tree");
    }

    private static AsyncWindowAtom findAtom(RecordCursorFactory factory) {
        return (AsyncWindowAtom) TestUtils.findAtom(factory, "async window");
    }

    private static String print(RecordCursor cursor, RecordCursorFactory factory) {
        final StringSink sink = new StringSink();
        CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        return sink.toString();
    }

    private static String serial(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        ctx.setParallelWindowEnabled(false);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            assertAsync(factory, false);
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                return print(cursor, factory);
            }
        }
    }

    // With requireWorkers, every query must have had tasks computed by a worker's copy.
    private void assertFunctionFamilies(CairoEngine engine, SqlExecutionContext ctx, boolean requireWorkers) throws Exception {
        // with workers, keys enough for rounds of several tasks; NULL and BIG come first
        final String in = requireWorkers
                ? "'S0', 'S1', 'BIG', 'S2', null, 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'S9'"
                : "'S1', 'BIG', 'S2', null, 'S5'";
        final ObjList<String> queries = new ObjList<>();
        for (String function : FUNCTIONS) {
            queries.add("select sym, x, " + function + " f from q where sym in (" + in + ") order by sym");
        }
        // several functions, fused and not, in one window
        final StringSink columns = new StringSink();
        for (int i = 0; i < FUNCTIONS.length; i++) {
            columns.put(", ").put(FUNCTIONS[i]).put(" f").put(i);
        }
        queries.add("select sym, ts" + columns + " from q where sym in (" + in + ") order by sym");
        queries.add("select sym, ts" + columns + " from q where sym != 'S3' order by sym desc");
        for (int i = 0, n = queries.size(); i < n; i++) {
            final String query = queries.getQuick(i);
            if (requireWorkers) {
                assertWorkersTookTasks(() -> assertMatchesSerial(engine, ctx, query));
            } else {
                assertMatchesSerial(engine, ctx, query);
            }
        }
    }

    // Returns the tasks the worker slots computed.
    private long assertLargeKeys(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        // BIG holds half of the rows, far above max.key.rows (400): the query's thread streams it,
        // between the rounds of the small keys around it, with no buffer for its output
        // descending: the prefix ends after S8, and BIG comes after rounds of S7 to S0
        final String query = "select " + IDX50_COLUMNS + " from q where sym in ('S0', 'S1', 'BIG', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8') order by sym desc";
        long workerTasks = assertMatchesSerial(engine, ctx, query);
        ctx.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                print(cursor, factory);
            }
            final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
            Assert.assertEquals(countOf(engine, ctx, "BIG"), asyncCursor.getLargeKeyRowCount());
            Assert.assertTrue(asyncCursor.getParallelRoundCount() > 0);
        }
        // one large key alone, after the prefix: nothing for the workers
        workerTasks += assertMatchesSerial(engine, ctx, "select " + IDX50_COLUMNS + " from q where sym in ('BIG', 'NOPE') order by sym");
        // a key exactly at the limit, and one row above it
        final long s1 = countOf(engine, ctx, "S1");
        for (long maxKeyRows : new long[]{s1, s1 - 1}) {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, maxKeyRows);
            workerTasks += assertMatchesSerial(engine, ctx, "select " + IDX50_COLUMNS + " from q where sym in ('S0', 'S1', 'S2') order by sym");
        }
        return workerTasks;
    }

    // Returns the tasks the worker slots computed.
    private long assertManyTinyKeys(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        engine.execute("drop table if exists t", ctx);
        engine.execute("create table t (sym symbol index type " + indexType + ", x long, ts timestamp) timestamp(ts) partition by DAY", ctx);
        // 2000 keys of 1 to 3 rows each, interleaved in time
        engine.execute("insert into t select 'K' || (x % 2000), x, (x * 60_000_000L)::timestamp from long_sequence(4500)", ctx);
        final StringSink in = new StringSink();
        in.put("'K0'");
        for (int i = 1; i < 2000; i += 3) {
            in.put(", 'K").put(i).put('\'');
        }
        final String query = "select sym, x, sum(x) over (partition by sym rows between 1 preceding and current row) s, " +
                "row_number() over (partition by sym) rn from t where sym in (" + in + ") order by sym";
        long workerTasks = assertMatchesSerial(engine, ctx, query);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 1);
        workerTasks += assertMatchesSerial(engine, ctx, query);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, TASK_ROWS);
        return workerTasks;
    }

    private long assertMatchesSerial(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        return assertMatchesSerial(engine, ctx, query, true);
    }

    // Returns the tasks the worker slots computed.
    private long assertMatchesSerial(CairoEngine engine, SqlExecutionContext ctx, String query, boolean expectAsync) throws Exception {
        final String expected = serial(engine, ctx, query);
        ctx.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            assertAsync(factory, expectAsync);
            long rowCount = 0;
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                final String actual = print(cursor, factory);
                TestUtils.assertEquals(query, expected, actual);
                cursor.toTop();
                while (cursor.hasNext()) {
                    rowCount++;
                }
            }
            if (!expectAsync) {
                return 0;
            }
            assertSlotsReleased(factory);
            // every row of the last pass was computed once: by the query's thread or by a task
            final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
            // the counters count the window's rows, which a LIMIT or an aggregate above it hides
            if (!query.contains(" limit ") && !query.startsWith("select count(")) {
                Assert.assertEquals(
                        query,
                        rowCount,
                        asyncCursor.getPrefixRowCount() + asyncCursor.getLargeKeyRowCount() + asyncCursor.getTaskRowCount()
                );
            }
            return findAtom(factory).getWorkerSlotTaskCount();
        } finally {
            ctx.setParallelWindowEnabled(false);
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

    private long countOf(CairoEngine engine, SqlExecutionContext ctx, String sym) throws Exception {
        final StringSink sink = new StringSink();
        engine.print("select count() from q where sym = '" + sym + "'", sink, ctx);
        return Long.parseLong(sink.toString().split("\n")[1]);
    }

    /**
     * {@code quote}-like table {@code q}: a symbol index on {@code sym}, half of the rows in key
     * BIG, the rest spread evenly over S0..S9, less one in eleven that is NULL, interleaved in time
     * over three days. One {@code bid} in seven is NULL, for the functions that skip NULLs.
     */
    private void createQuote(CairoEngine engine, SqlExecutionContext ctx, String partitionBy, int rows) throws Exception {
        engine.execute(
                "create table q (sym symbol index type " + indexType + ", bid float, bsize int, ask float, asize int, x long, ts timestamp)" +
                        " timestamp(ts) partition by " + partitionBy,
                ctx
        );
        engine.execute(
                "insert into q select" +
                        " case when x % 2 = 0 then 'BIG' when x % 11 = 0 then null else 'S' || (x / 2 % 10) end," +
                        " case when x % 7 = 3 then null else rnd_float() end, rnd_int(1, 1000, 0), rnd_float(), rnd_int(1, 1000, 0), x," +
                        " (x * " + (3 * 86_400_000_000L / rows) + "L)::timestamp" +
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

    private String printToString(String query) throws Exception {
        final StringSink sink = new StringSink();
        printSql(query, sink);
        return sink.toString();
    }

    @FunctionalInterface
    private interface WorkerTaskCount {
        long run() throws Exception;
    }

    @FunctionalInterface
    private interface PoolTest {
        void run(CairoEngine engine, SqlExecutionContextImpl ctx) throws Exception;
    }
}

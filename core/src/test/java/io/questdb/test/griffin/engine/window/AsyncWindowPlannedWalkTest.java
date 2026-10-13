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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.table.KeyMajorPageFrameRecordCursor;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursor;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The parallel window over a key-major index walk, with the walk planned
 * ({@code cairo.sql.parallel.frame.dispatch.enabled}): the query's thread counts each task's rows
 * from the posting index, and the thread that computes the task reads its row ids. Every query
 * runs three ways on the same data: the serial window, the parallel window with the walk
 * collected on the query's thread, and with the walk planned. All three must print the same
 * result, row for row, and the planned run must really have planned: the walk reports it, and
 * tasks were read from plans.
 */
public class AsyncWindowPlannedWalkTest extends AbstractCairoTest {
    private static final String IDX50_COLUMNS = "sym, ts, " +
            "avg((bsize * bid + asize * ask) / (bsize + asize)) over (partition by sym rows between 4 preceding and current row) mid, " +
            "avg(asize + bsize) over (partition by sym rows between 4 preceding and current row) size";
    private static final String[] SHAPES = {
            // warm-up rows and a replayed frame: idx 49/50
            IDX50_COLUMNS,
            // lag, the row kernels' shape: idx 61
            "sym, x, x - lag(x) over (partition by sym) d",
            // a running carry
            "sym, x, sum(x) over (partition by sym rows between unbounded preceding and current row) s, row_number() over (partition by sym) rn, "
                    + "count(bid) over (partition by sym rows between unbounded preceding and current row) c",
            // keys whole: large keys stream on the query's thread
            "sym, x, avg(bid) over (partition by sym rows between unbounded preceding and current row) a, lag(x, 2) over (partition by sym) g",
            // several bounded frames and lags
            "sym, x, avg(bid) over (partition by sym rows between 3 preceding and current row) a, "
                    + "min(bid) over (partition by sym rows between 3 preceding and 1 preceding) mn, lag(x, 3) over (partition by sym) g",
    };

    // queries the planner gave a parallel window, and the others, see assertThreeWays()
    private int asyncPlans;
    private int serialPlans;

    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_PAGE_FRAME_MAX_ROWS, 1);
        sqlExecutionContext.changePageFrameSizes(1, 64);
        setSizes(50, 400, 100, 200);
        asyncPlans = 0;
        serialPlans = 0;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        // most queries of every test run in parallel, or the test proves little
        Assert.assertTrue("async=" + asyncPlans + ", serial=" + serialPlans, asyncPlans > 0 && asyncPlans >= serialPlans);
    }

    @Test
    public void testBindVariablesAndMissingSymbols() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 3_000, "posting");
            final String query = "select " + SHAPES[0] + " from q where sym in ($1, $2, $3, $4) order by sym";
            final String[][] binds = {{"S1", "S5", "BIG", null}, {"S7", "NOPE", "S2", "S2"}, {"NOPE", "S3", "S3", "BIG"}};
            for (String[] b : binds) {
                bind(b);
                assertThreeWays(engine, sqlExecutionContext, query, true);
            }
        });
    }

    @Test
    public void testBitmapIndexIsNotPlanned() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 3_000, "bitmap");
            for (String shape : SHAPES) {
                assertThreeWays(engine, sqlExecutionContext, "select " + shape + " from q where sym in ('S1', 'BIG', null, 'S4') order by sym", false);
            }
        });
    }

    @Test
    public void testColumnTopsAndPartitionsWithoutTheColumn() throws Exception {
        assertMemoryLeak(() -> {
            // the first day has no sym2 at all; the second gets it mid-partition, a column top
            execute("create table q (sym symbol index type posting, bid float, bsize int, ask float, asize int, x long, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into q select 'S' || (x % 7), rnd_float(), rnd_int(1, 1000, 0), rnd_float(), rnd_int(1, 1000, 0), x, (x * 60_000_000L)::timestamp from long_sequence(2000)");
            execute("alter table q add column sym2 symbol index type posting");
            execute("insert into q select 'S' || (x % 7), rnd_float(), rnd_int(1, 1000, 0), rnd_float(), rnd_int(1, 1000, 0), 2000 + x, ((2000 + x) * 60_000_000L)::timestamp, "
                    + "case when x % 5 = 0 then null else 'T' || (x % 4) end from long_sequence(2000)");
            // planned: the column exists in every partition the scan reads
            assertThreeWays(engine, sqlExecutionContext,
                    "select sym, x, sum(x) over (partition by sym rows between unbounded preceding and current row) s from q where sym in ('S1', 'S3', 'S6') order by sym", true);
            // sym2's index has no reader in the first day's partition: collected, not planned
            for (String in : new String[]{"'T1', null, 'T3'", "null", "'T0', 'T2'"}) {
                assertThreeWays(engine, sqlExecutionContext,
                        "select sym2, x, lag(x) over (partition by sym2) g, sum(x) over (partition by sym2 rows between unbounded preceding and current row) s from q where sym2 in (" + in + ") order by sym2", false);
            }
            // within the second day only: a column top, and NULL rows from it in the posting reader's null prefix
            for (String in : new String[]{"'T1', null, 'T3'", "null", "'T0', 'T2', 'T1'"}) {
                assertThreeWays(engine, sqlExecutionContext,
                        "select sym2, x, lag(x) over (partition by sym2) g, avg(bid) over (partition by sym2 rows between 2 preceding and current row) a "
                                + "from q where sym2 in (" + in + ") and ts >= '1970-01-02' order by sym2", true);
            }
        });
    }

    @Test
    public void testCountAndIndexDisagreeFailsTheQuery() throws Exception {
        // the plan counts one (key, frame) a row short, then a row long: the query must fail, not
        // drop the surplus row or come up a row short
        assertMemoryLeak(() -> inPool(4, (engine, ctx) -> {
            createQuote(engine, ctx, "DAY", 3_000, "posting");
            final String query = "select " + SHAPES[0] + " from q where sym in ('S1', 'BIG', 'S2') order by sym";
            assertThreeWays(engine, ctx, query, true);
            setFrameDispatch(true);
            ctx.setParallelWindowEnabled(true);
            for (long skew : new long[]{-1, 1}) {
                try (RecordCursorFactory factory = engine.select(query, ctx)) {
                    KeyMajorPageFrameRecordCursor.setCountSkew(skew);
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        final String result = print(cursor, factory);
                        Assert.fail("skew " + skew + ": no error, " + result.length() + " chars printed");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "other rows than it counted");
                    } finally {
                        KeyMajorPageFrameRecordCursor.setCountSkew(0);
                    }
                    assertSlotsReleased(factory);
                }
            }
            // and the walk is sound again
            assertThreeWays(engine, ctx, query, true);
        }));
    }

    @Test
    public void testFilteredKeysAreNotPlanned() throws Exception {
        // a residual filter the index cannot count: the walk is collected on the query's thread
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 4_000, "posting");
            for (String shape : SHAPES) {
                assertThreeWays(engine, sqlExecutionContext, "select " + shape + " from q where sym in ('S1', 'BIG', null, 'S4') and bsize > 300 order by sym", false);
                assertThreeWays(engine, sqlExecutionContext, "select " + shape + " from q where sym not in ('S1') and asize % 3 = 0 order by sym", false);
            }
        });
    }

    @Test
    public void testFunctionShapesAcrossTaskSizes() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 6_000, "posting");
            final String[] filters = {
                    "where sym in ('S1', 'BIG', 'S2', null, 'S5', 'S9')",
                    "where sym != 'S4'",
                    "where sym not in ('S4', 'BIG')",
            };
            final long[][] sizes = {{50, 400, 100, 200}, {1, 3, 0, 1}, {7, 50, 13, 21}, {1_000, 100_000, 0, 2_000}};
            for (long[] size : sizes) {
                setSizes(size[0], size[1], size[2], size[3]);
                for (String shape : SHAPES) {
                    for (String filter : filters) {
                        for (String order : new String[]{"sym", "sym desc"}) {
                            assertThreeWays(engine, sqlExecutionContext, "select " + shape + " from q " + filter + " order by " + order, true);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testIdx61ShapeWalksEveryKey() throws Exception {
        // the window over the whole table, ordered by the key outside: a walk over every symbol of the table
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 5_000, "posting");
            for (long[] size : new long[][]{{50, 400, 100, 200}, {1, 2, 0, 1}, {300, 100_000, 0, 600}}) {
                setSizes(size[0], size[1], size[2], size[3]);
                assertThreeWays(engine, sqlExecutionContext,
                        "select sym, x as d from (select sym, x, x - lag(x) over (partition by sym order by ts) dx from q) where dx > 3 order by sym, d", true);
                assertThreeWays(engine, sqlExecutionContext,
                        "select sym, x, dx from (select sym, x, ts, x - lag(x, 2) over (partition by sym order by ts) dx from q) where dx < 30 order by sym, ts", true);
            }
        });
    }

    @Test
    public void testLimitRewindsAndPartialPasses() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 4_000, "posting");
            for (String shape : SHAPES) {
                final String query = "select " + shape + " from q where sym in ('S1', 'BIG', 'S2', null, 'S5') order by sym";
                assertThreeWays(engine, sqlExecutionContext, query + " limit 7", true);
                assertThreeWays(engine, sqlExecutionContext, query + " limit 333", true);
                final String expected = serial(engine, sqlExecutionContext, query);
                setFrameDispatch(true);
                sqlExecutionContext.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = select(query)) {
                    for (int run = 0; run < 3; run++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            // ends inside a round, rewinds, then whole passes
                            for (int i = 0; i < 377 + run * 400 && cursor.hasNext(); i++) {
                                // skip
                            }
                            cursor.toTop();
                            TestUtils.assertEquals(expected, print(cursor, factory));
                            Assert.assertTrue(findAsyncCursor(factory).isWalkPlanned());
                            cursor.toTop();
                            TestUtils.assertEquals(expected, print(cursor, factory));
                        }
                        // closed mid-pass: its tasks must be done with the frozen readers first
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            for (int i = 0; i < 150 + run * 100 && cursor.hasNext(); i++) {
                                // skip
                            }
                        }
                    }
                    assertSlotsReleased(factory);
                }
            }
            // the readers are unfrozen: new rows show in the next query
            execute("insert into q select 'S1', 1.0, 1, 1.0, 1, 99_999, '1970-01-03T23:59:59.000000Z' from long_sequence(1)");
            assertThreeWays(engine, sqlExecutionContext, "select " + SHAPES[1] + " from q where sym in ('S1', 'S2') order by sym", true);
        });
    }

    @Test
    public void testManyGenerationsAndOutOfOrderCommits() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table q (sym symbol index type posting, bid float, bsize int, ask float, asize int, x long, ts timestamp) timestamp(ts) partition by DAY");
            final Rnd rnd = TestUtils.generateRandom(LOG);
            // many small commits, some out of order, so that each partition's index has many generations
            for (int c = 0; c < 40; c++) {
                final long base = rnd.nextBoolean() ? c * 200L : rnd.nextLong(40 * 200);
                execute("insert into q select rnd_symbol('A','B','C','D','E','F','G', null), rnd_float(), rnd_int(1, 1000, 0), rnd_float(), rnd_int(1, 1000, 0), "
                        + base + " + x, ((" + base + " + x) * 600_000_000L)::timestamp from long_sequence(" + (50 + rnd.nextInt(150)) + ")");
            }
            for (String shape : SHAPES) {
                for (String filter : new String[]{"where sym in ('A', 'C', null, 'G', 'Z')", "where sym not in ('B')", "where sym != 'D'"}) {
                    assertThreeWays(engine, sqlExecutionContext, "select " + shape + " from q " + filter + " order by sym", true);
                }
            }
        });
    }

    @Test
    public void testManyTinyKeysOnManyWorkers() throws Exception {
        setSizes(1, 50, 0, 3);
        assertMemoryLeak(() -> inPool(8, (engine, ctx) -> {
            engine.execute("create table q (sym symbol index type posting, bid float, bsize int, ask float, asize int, x long, ts timestamp) timestamp(ts) partition by HOUR", ctx);
            // 1500 keys of 1 to 3 rows over many hourly partitions
            engine.execute("insert into q select 'K' || (x % 1500), rnd_float(), rnd_int(1, 1000, 0), rnd_float(), rnd_int(1, 1000, 0), x, (x * 30_000_000L)::timestamp "
                    + "from long_sequence(3000)", ctx);
            final StringSink in = new StringSink();
            for (int k = 0; k < 1500; k += 3) {
                in.put(k == 0 ? "" : ", ").put("'K").put(k).put('\'');
            }
            long workerPlannedTasks = 0;
            for (String shape : SHAPES) {
                workerPlannedTasks += assertThreeWays(engine, ctx, "select " + shape + " from q where sym in (" + in + ", null) order by sym", true);
            }
            workerPlannedTasks += assertThreeWays(engine, ctx,
                    "select sym, x as d from (select sym, x, x - lag(x) over (partition by sym order by ts) dx from q) where dx > 3 order by sym, d", true);
            Assert.assertTrue(workerPlannedTasks > 0);
        }));
    }

    @Test
    public void testOnWorkerPoolRepeatedly() throws Exception {
        setSizes(200, 5_000, 1_000, 3_000);
        assertMemoryLeak(() -> inPool(4, (engine, ctx) -> {
            createQuote(engine, ctx, "DAY", 30_000, "posting");
            for (int run = 0; run < 3; run++) {
                for (String shape : SHAPES) {
                    assertThreeWays(engine, ctx, "select " + shape + " from q where sym in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'BIG', null) order by sym", true);
                    assertThreeWays(engine, ctx, "select " + shape + " from q where sym != 'S3' order by sym desc", true);
                }
            }
        }));
    }

    @Test
    public void testRoundsReturnComputedTasksEarly() throws Exception {
        // A round is not a barrier: its computed tasks return, in order, while its later tasks
        // still compute. Every shape, with keys split and whole, must still match the serial
        // window, and some task must have returned before its round finished.
        setSizes(100, 2_000, 0, 2_000);
        assertMemoryLeak(() -> inPool(4, (engine, ctx) -> {
            createQuote(engine, ctx, "DAY", 40_000, "posting");
            long early = 0;
            for (int run = 0; run < 4; run++) {
                for (String shape : SHAPES) {
                    final String query = "select " + shape + " from q where sym in ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'BIG', null) order by sym";
                    final String expected = serial(engine, ctx, query);
                    setFrameDispatch(true);
                    ctx.setParallelWindowEnabled(true);
                    try (RecordCursorFactory factory = engine.select(query, ctx)) {
                        final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                        asyncPlans++;
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertEquals(query, expected, print(cursor, factory));
                            early += asyncCursor.getEarlyEmittedTaskCount();
                        }
                        assertSlotsReleased(factory);
                    }
                    // switched off, no task returns early
                    setFrameDispatch(false);
                    try (RecordCursorFactory factory = engine.select(query, ctx)) {
                        final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertEquals(query, expected, print(cursor, factory));
                            Assert.assertEquals(0, asyncCursor.getEarlyEmittedTaskCount());
                        }
                    }
                }
            }
            Assert.assertTrue("no task returned before its round finished", early > 0);
        }));
    }

    @Test
    public void testPrefixIsCappedWithTheSwitchOn() throws Exception {
        // min.rows and prefix.rows above the cap: the query's thread streams at most
        // PREFIX_ROWS_DISPATCH rows alone with the switch on, prefix.rows with it off
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 1_000);
        // min.rows also gates the parallel plan by the table's rows
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 20_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_PREFIX_ROWS, 16_384);
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 80_000, "posting");
            final String query = "select " + SHAPES[1] + " from q where sym in ('S1', 'BIG', 'S2', 'S3', 'S4', 'S5') order by sym";
            final String expected = serial(engine, sqlExecutionContext, query);
            for (boolean on : new boolean[]{true, false}) {
                setFrameDispatch(on);
                sqlExecutionContext.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = select(query)) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        TestUtils.assertEquals(query, expected, print(cursor, factory));
                    }
                    final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                    asyncPlans++;
                    final long prefix = asyncCursor.getPrefixRowCount();
                    if (on) {
                        // the prefix ends within the chunk that reaches the cap: a chunk is at most task.rows
                        Assert.assertTrue("prefix " + prefix, prefix > 0 && prefix <= 2048 + 1_000);
                    } else {
                        Assert.assertTrue("prefix " + prefix, prefix > 2048 + 1_000 && prefix <= 16_384 + 1_000);
                    }
                    Assert.assertTrue(asyncCursor.getParallelTaskCount() > 0);
                }
            }
        });
    }

    @Test
    public void testSwitchedOffCollectsOnTheQueryThread() throws Exception {
        assertMemoryLeak(() -> {
            createQuote(engine, sqlExecutionContext, "DAY", 3_000, "posting");
            final String query = "select " + SHAPES[0] + " from q where sym in ('S1', 'BIG', 'S2') order by sym";
            setFrameDispatch(false);
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertEquals(serial(engine, sqlExecutionContext, query), print(cursor, factory));
                }
                final AsyncWindowRecordCursor asyncCursor = findAsyncCursor(factory);
                asyncPlans++;
                Assert.assertFalse(asyncCursor.isWalkPlanned());
                Assert.assertEquals(0, asyncCursor.getPlannedTaskCount());
                Assert.assertTrue(asyncCursor.getParallelTaskCount() > 0);
            }
        });
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

    private static String serial(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        ctx.setParallelWindowEnabled(false);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                Assert.assertFalse(f instanceof AsyncWindowRecordCursorFactory);
            }
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                return print(cursor, factory);
            }
        }
    }

    private static void setFrameDispatch(boolean enabled) {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, String.valueOf(enabled));
    }

    private static void setSizes(long taskRows, long maxKeyRows, long minRows, long roundRows) {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, taskRows);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, maxKeyRows);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, minRows);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, roundRows);
    }

    // Serial, parallel collected and parallel planned: the same rows. With expectPlanned, the
    // planned run planned its walk whenever it ran in parallel and had tasks. Returns the tasks
    // read from plans.
    private long assertThreeWays(CairoEngine engine, SqlExecutionContext ctx, String query, boolean expectPlanned) throws Exception {
        final String expected = serial(engine, ctx, query);
        long plannedTasks = 0;
        for (boolean planned : new boolean[]{false, true}) {
            setFrameDispatch(planned);
            ctx.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select(query, ctx)) {
                AsyncWindowRecordCursor asyncCursor = null;
                for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                    if (f instanceof AsyncWindowRecordCursorFactory asyncFactory) {
                        asyncCursor = asyncFactory.getAsyncCursor();
                    }
                }
                if (asyncCursor == null) {
                    // not a parallel window plan: nothing to plan, the result must still match
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertEquals(query, expected, print(cursor, factory));
                    }
                    serialPlans++;
                    continue;
                }
                asyncPlans++;
                for (int run = 0; run < 2; run++) {
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertEquals(query + " [planned=" + planned + ", run=" + run + ']', expected, print(cursor, factory));
                        if (planned && expectPlanned && asyncCursor.getParallelTaskCount() > 0) {
                            Assert.assertTrue(query, asyncCursor.isWalkPlanned());
                            // a LIMIT may close the cursor before any task of a round is computed
                            Assert.assertTrue(query, asyncCursor.getPlannedTaskCount() > 0 || query.contains(" limit "));
                        }
                        if (!planned || !expectPlanned) {
                            Assert.assertFalse(query, asyncCursor.isWalkPlanned());
                            Assert.assertEquals(query, 0, asyncCursor.getPlannedTaskCount());
                        }
                        if (planned) {
                            plannedTasks += asyncCursor.getPlannedTaskCount();
                        }
                    }
                }
                assertSlotsReleased(factory);
            }
        }
        return plannedTasks;
    }

    private void bind(String[] values) throws Exception {
        bindVariableService.clear();
        for (int i = 0; i < values.length; i++) {
            bindVariableService.setStr(i, values[i]);
        }
    }

    private void createQuote(CairoEngine engine, SqlExecutionContext ctx, String partitionBy, int rows, String indexType) throws Exception {
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

    private void inPool(int workers, PoolTest test) throws Exception {
        final WorkerPool pool = new TestWorkerPool(workers, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
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

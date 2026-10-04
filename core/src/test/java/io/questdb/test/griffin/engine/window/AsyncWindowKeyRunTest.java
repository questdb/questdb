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
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.window.AsyncWindowAtom;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.AsyncWindowSplitPlan;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
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
 * Key runs: an {@code Async Window} task whose window functions are all partitioned by the scan's
 * key alone computes each key's rows with the partition's state in the functions themselves, and
 * writes its output rows straight into the task's chain (see {@code AsyncWindowAtom.Slot#computeKeyRuns}).
 * Every test reads every value of every row as raw bits, and compares three runs of each query:
 * <ul>
 *     <li>with key runs, which must have computed the tasks and show in the plan;</li>
 *     <li>through the functions' maps, as before key runs ({@code cairo.sql.parallel.window.key.runs.enabled}
 *     off), which must equal them bit for bit whatever the split, since both compute a split key the
 *     same way;</li>
 *     <li>the serial window, which they must equal bit for bit when no key is split.</li>
 * </ul>
 * The fixture holds NULLs in every column, zero denominators, -0.0 and keys of every size.
 */
@RunWith(Parameterized.class)
public class AsyncWindowKeyRunTest extends AbstractCairoTest {
    private static final String IDX50 = "sym, ts, " +
            "avg((bsize * bid + asize * ask) / (bsize + asize)) over (partition by sym rows between 4 preceding and current row) mid, " +
            "avg(asize + bsize) over (partition by sym rows between 4 preceding and current row) size";
    private static final String IN = "('S1', 'BIG', 'S2', null, 'S5', 'S7', 'S0', 'NOPE', 'S9')";
    private static final long MAX_KEY_ROWS = 400;
    private static final long MIN_ROWS = 100;
    private static final long ROUND_ROWS = 200;
    private static final long TASK_ROWS = 50;
    // every kind of bounded ROWS frame a key run computes, sum and avg, over FLOAT, INT, LONG,
    // DOUBLE and expressions
    private static final String[] FRAMES = {
            "avg(d) over (partition by sym rows between 3 preceding and current row)",
            "sum(d) over (partition by sym rows between 1 preceding and current row)",
            "avg(bid) over (partition by sym rows between 5 preceding and 2 preceding)",
            "sum(bsize) over (partition by sym rows between 1 preceding and 1 preceding)",
            "avg(x) over (partition by sym rows between 100 preceding and current row)",
            "sum(d * 2 - bid) over (partition by sym rows between 7 preceding and 3 preceding)",
            "sum(d) over (partition by sym rows between unbounded preceding and 2 preceding)",
            "avg(asize / bsize) over (partition by sym rows between unbounded preceding and 1 preceding)",
    };
    private final String indexType;

    public AsyncWindowKeyRunTest(String indexType) {
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
        // 64 rows per page frame: every key spans many frames, and so do batches and tasks
        sqlExecutionContext.changePageFrameSizes(1, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, TASK_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, MAX_KEY_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, MIN_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, ROUND_ROWS);
    }

    @Test
    public void testColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "DAY", 3_000);
            // the older partitions have no value for the new columns: column tops, read as NULL
            execute("alter table k add column late double");
            execute("alter table k add column late_i int");
            execute("insert into k (sym, x, d, late, late_i, ts) select 'S' || (x % 10), x, x / 3.0, " +
                    "case when x % 4 = 0 then null else x / 7.0 end, case when x % 5 = 0 then null else x::int end, " +
                    "('2000-01-04'::timestamp + x * 60_000_000L) from long_sequence(500)");
            final String columns = "sym, late, late_i, ts, sum(late) over (partition by sym rows between 2 preceding and current row) s, " +
                    "avg(late_i + d) over (partition by sym rows between 3 preceding and 1 preceding) a";
            assertKeyRuns(engine, sqlExecutionContext, "select " + columns + " from k where sym in " + IN + " order by sym");
            assertKeyRuns(engine, sqlExecutionContext, "select " + columns + " from k where sym != 'S3' order by sym desc");
        });
    }

    @Test
    public void testDuplicateBindKeys() throws Exception {
        // A task restarts each key of the walk. A key the walk visited twice, for an IN list's
        // repeated bind value, would restart where the serial window continues: the scan must
        // walk each distinct value once, see KeyMajorScanFactory.hasDistinctKeys().
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "DAY", 3_000);
            final String[][] tuples = {
                    {"S1", "S1", "S2", "S2"},
                    {"S2", "S1", "S2", "S1"},
                    {"BIG", "BIG", "BIG", "S3"},
                    {null, "S4", null, "S4"},
                    {"S5", "NOPE", "S5", "NOPE"},
            };
            for (String[] tuple : tuples) {
                bindVariableService.clear();
                for (int i = 0; i < tuple.length; i++) {
                    bindVariableService.setStr(i, tuple[i]);
                }
                assertKeyRuns(engine, sqlExecutionContext, "select sym, x, sum(d) over (partition by sym rows between 3 preceding and current row) s " +
                        "from k where sym in ($1, $2, $3, $4) order by sym");
                assertKeyRuns(engine, sqlExecutionContext, "select " + IDX50 + " from k where sym in ($1, 'S5', $2, $3, 'S1', $4) order by sym desc");
            }
        });
    }

    @Test
    public void testFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "DAY", 4_000);
            final StringSink columns = new StringSink();
            columns.put("sym, x");
            for (int i = 0; i < FRAMES.length; i++) {
                assertKeyRuns(engine, sqlExecutionContext, "select sym, x, " + FRAMES[i] + " f from k where sym in " + IN + " order by sym");
                columns.put(", ").put(FRAMES[i]).put(" f").put(i);
            }
            // all of them in one window; the unbounded frames keep its keys whole
            assertKeyRuns(engine, sqlExecutionContext, "select " + columns + " from k where sym in " + IN + " order by sym");
            assertKeyRuns(engine, sqlExecutionContext, "select " + columns + " from k where sym != 'S4' order by sym desc");
        });
    }

    @Test
    public void testIdx50Shape() throws Exception {
        assertMemoryLeak(() -> {
            for (String partitionBy : new String[]{"NONE", "DAY", "HOUR"}) {
                execute("drop table if exists k");
                createTable(engine, sqlExecutionContext, partitionBy, 3_000);
                assertKeyRuns(engine, sqlExecutionContext, "select sym, ts, mid, size from (select " + IDX50 + " from k where sym in " + IN + " order by sym)");
                assertKeyRuns(engine, sqlExecutionContext, "select " + IDX50 + " from k where sym in " + IN + " order by sym desc");
                assertKeyRuns(engine, sqlExecutionContext, "select " + IDX50 + " from k where sym in ('S3', 'S3', 'S1', 'BIG', 'S1') order by sym");
                assertKeyRuns(engine, sqlExecutionContext, "select " + IDX50 + " from k where sym in " + IN + " and bid > 0.5 order by sym");
                assertKeyRuns(engine, sqlExecutionContext, "select " + IDX50 + " from k where sym != 'S2' order by sym");
                assertKeyRuns(engine, sqlExecutionContext, "select " + IDX50 + " from k where sym not in ('S2', 'BIG', null) order by sym desc");
            }
        });
    }

    @Test
    public void testIneligibleWindowsKeepTheMaps() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "DAY", 2_000);
            final String w = " over (partition by sym rows between 3 preceding and current row)";
            final String[] queries = {
                    // a partition finer than the key
                    "select sym, x, avg(d) over (partition by sym, bsize % 3 rows between 3 preceding and current row) a from k where sym in " + IN + " order by sym",
                    // a window function without key runs
                    "select sym, x, avg(d)" + w + " a, count()" + w + " c from k where sym in " + IN + " order by sym",
                    "select sym, x, avg(d) over (partition by sym rows between current row and current row) a from k where sym in " + IN + " order by sym",
                    // a column of a type a key run does not write
                    "select sym, v, avg(d)" + w + " a from k where sym in " + IN + " order by sym",
                    "select sym, u, avg(d)" + w + " a from k where sym in " + IN + " order by sym",
            };
            for (String query : queries) {
                sqlExecutionContext.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                    final AsyncWindowAtom atom;
                    try {
                        atom = findAtom(factory);
                    } catch (AssertionError e) {
                        throw new AssertionError(query, e);
                    }
                    Assert.assertFalse(query, atom.isKeyRunEnabled());
                } finally {
                    sqlExecutionContext.setParallelWindowEnabled(false);
                }
                final String serial = bits(engine, sqlExecutionContext, query, false, false);
                final String parallel = bits(engine, sqlExecutionContext, query, true, false);
                if (findSplitMode(engine, sqlExecutionContext, query) == AsyncWindowSplitPlan.MODE_NONE) {
                    TestUtils.assertEquals(query, serial, parallel);
                }
            }
        });
    }

    @Test
    public void testLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "DAY", 3_000);
            for (String limit : new String[]{"1", "10", "333", "100, 900", "1450, 1460", "-50", "5000"}) {
                assertKeyRuns(engine, sqlExecutionContext, "select " + IDX50 + " from k where sym in " + IN + " order by sym limit " + limit);
                assertKeyRuns(engine, sqlExecutionContext, "select sym, x, " + FRAMES[2] + " f from k where sym != 'S1' order by sym desc limit " + limit);
            }
        });
    }

    @Test
    public void testOnWorkerPool() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 200);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 5_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 3_000);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
                ctx.changePageFrameSizes(1, 64);
                createTable(engine, ctx, "DAY", 30_000);
                final String[] queries = {
                        "select " + IDX50 + " from k where sym in " + IN + " order by sym",
                        "select sym, x, " + FRAMES[0] + " a, " + FRAMES[5] + " b from k where sym != 'S4' order by sym desc",
                        "select sym, x, " + FRAMES[6] + " a from k where sym not in ('BIG', null) order by sym",
                };
                for (String query : queries) {
                    // a round of small tasks may all be stolen by the query's thread before a
                    // worker wakes up: repeat until a worker computed key runs
                    long workerTasks = 0;
                    for (int i = 0; i < 10 && workerTasks == 0; i++) {
                        workerTasks = assertKeyRuns(engine, ctx, query);
                    }
                    Assert.assertTrue(query, workerTasks > 0);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testOnlyTheColumnsReadAreLoaded() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "DAY", 1_000);
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select("select " + IDX50 + " from k where sym in " + IN + " order by sym", sqlExecutionContext)) {
                final boolean[] touched = findAtom(factory).getKeyRunTouchedColumns();
                Assert.assertNotNull(touched);
                // the scan's columns: the window's arguments and the output's, but not the key,
                // which a run reads once
                final RecordMetadata scan = findAsyncFactory(factory).getBaseFactory().getMetadata();
                final StringSink names = new StringSink();
                for (int c = 0; c < touched.length; c++) {
                    if (touched[c]) {
                        names.put(scan.getColumnName(c)).put(' ');
                    }
                }
                Assert.assertEquals(5, names.toString().split(" ").length);
                for (String column : new String[]{"bid", "bsize", "ask", "asize", "ts"}) {
                    TestUtils.assertContains(names, column + " ");
                }
                final int symIndex = scan.getColumnIndex("sym");
                Assert.assertTrue(symIndex >= touched.length || !touched[symIndex]);
                // and the touch-ahead of the tasks loaded those, and no other
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // drain
                    }
                }
                Assert.assertEquals(5, findAtom(factory).getKeyRunLoadedColumnCount());
            } finally {
                sqlExecutionContext.setParallelWindowEnabled(false);
            }
        });
    }

    @Test
    public void testPassThroughColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "DAY", 3_000);
            final String columns = "sym, ex, b, by, sh, ch, i, ip, f, l, dt, d, g1, g2, g3, g4, ts, x, " +
                    "sum(d) over (partition by sym rows between 2 preceding and current row) s, " +
                    "avg(i) over (partition by sym rows between 4 preceding and 1 preceding) a";
            assertKeyRuns(engine, sqlExecutionContext, "select " + columns + " from k where sym in " + IN + " order by sym");
            assertKeyRuns(engine, sqlExecutionContext, "select " + columns + " from k where sym != 'S6' order by sym desc limit 2500");
            // the key column not first
            assertKeyRuns(engine, sqlExecutionContext, "select x, ts, sym, " + FRAMES[1] + " f from k where sym in " + IN + " order by sym");
        });
    }

    @Test
    public void testTasksOfManyKeysAndTinyTasks() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "DAY", 3_000);
            execute("create table t (sym symbol index type " + indexType + ", x long, d double, ts timestamp) timestamp(ts) partition by DAY");
            // 2000 keys of 1 to 3 rows each, interleaved in time
            execute("insert into t select 'K' || (x % 2000), x, case when x % 9 = 0 then null else x / 3.0 end, (x * 60_000_000L)::timestamp from long_sequence(4500)");
            final StringSink in = new StringSink();
            in.put("'K0'");
            for (int i = 1; i < 2000; i += 3) {
                in.put(", 'K").put(i).put('\'');
            }
            final String query = "select sym, x, sum(d) over (partition by sym rows between 1 preceding and current row) s, " +
                    "avg(x) over (partition by sym rows between 2 preceding and current row) a from t where sym in (" + in + ") order by sym";
            for (long taskRows : new long[]{1, 2, 7, TASK_ROWS}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, taskRows);
                assertKeyRuns(engine, sqlExecutionContext, query);
                assertKeyRuns(engine, sqlExecutionContext, "select " + IDX50 + " from k where sym in " + IN + " order by sym");
            }
        });
    }

    private static String bits(CairoEngine engine, SqlExecutionContext ctx, String query, boolean parallel, boolean keyRuns) throws Exception {
        ctx.setParallelWindowEnabled(parallel);
        // Set only when it changes the value: setting a property to the value it has drops the
        // other overrides set since the configuration was last read.
        if (!keyRuns) {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_KEY_RUNS_ENABLED, "false");
        }
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            final StringSink sink = new StringSink();
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                printBits(cursor, factory.getMetadata(), sink);
            }
            if (parallel) {
                final AsyncWindowAtom atom = findAtom(factory);
                if (!keyRuns || !atom.isKeyRunEnabled()) {
                    Assert.assertEquals(query, 0, atom.getKeyRunTaskCount());
                } else if (!query.contains(" limit ")) {
                    Assert.assertTrue(query, atom.getKeyRunTaskCount() > 0);
                }
            }
            return sink.toString();
        } finally {
            ctx.setParallelWindowEnabled(false);
            if (!keyRuns) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_KEY_RUNS_ENABLED, "true");
            }
        }
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
        return (AsyncWindowAtom) findAsyncFactory(factory).getAtom();
    }

    private static String plan(RecordCursorFactory factory, SqlExecutionContext ctx) {
        final TextPlanSink planSink = new TextPlanSink();
        planSink.of(factory, ctx);
        final StringSink lines = new StringSink();
        for (int i = 1; i <= planSink.getLineCount(); i++) {
            lines.put(planSink.getLine(i)).put('\n');
        }
        return lines.toString();
    }

    private static int findSplitMode(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        ctx.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            return findAsyncFactory(factory).getSplitPlan().getMode();
        } finally {
            ctx.setParallelWindowEnabled(false);
        }
    }

    // Every value of every row as the bits a reader gets from the record.
    private static void printBits(RecordCursor cursor, RecordMetadata metadata, StringSink sink) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            printRecord(record, metadata, sink);
        }
    }

    private static void printRecord(Record record, RecordMetadata metadata, StringSink sink) {
        final int columnCount = metadata.getColumnCount();
        for (int c = 0; c < columnCount; c++) {
            if (c > 0) {
                sink.put('\t');
            }
            final int type = metadata.getColumnType(c);
            switch (ColumnType.tagOf(type)) {
                case ColumnType.BOOLEAN -> sink.put(record.getBool(c));
                case ColumnType.BYTE -> sink.put(record.getByte(c));
                case ColumnType.SHORT -> sink.put(record.getShort(c));
                case ColumnType.CHAR -> sink.put((int) record.getChar(c));
                case ColumnType.INT -> sink.put(record.getInt(c));
                case ColumnType.IPv4 -> sink.put(record.getIPv4(c));
                case ColumnType.SYMBOL -> sink.put(record.getInt(c)).put(':').put(record.getSymA(c));
                case ColumnType.FLOAT -> sink.put(Float.floatToRawIntBits(record.getFloat(c)));
                case ColumnType.LONG -> sink.put(record.getLong(c));
                case ColumnType.DATE -> sink.put(record.getDate(c));
                case ColumnType.TIMESTAMP -> sink.put(record.getTimestamp(c));
                case ColumnType.DOUBLE -> sink.put(Double.doubleToRawLongBits(record.getDouble(c)));
                case ColumnType.GEOBYTE -> sink.put(record.getGeoByte(c));
                case ColumnType.GEOSHORT -> sink.put(record.getGeoShort(c));
                case ColumnType.GEOINT -> sink.put(record.getGeoInt(c));
                case ColumnType.GEOLONG -> sink.put(record.getGeoLong(c));
                case ColumnType.VARCHAR -> sink.put(record.getVarcharA(c));
                case ColumnType.UUID -> sink.put(record.getLong128Hi(c)).put('/').put(record.getLong128Lo(c));
                default -> throw new AssertionError("unexpected column type " + ColumnType.nameOf(type));
            }
        }
        sink.put('\n');
    }

    /**
     * Runs the query with key runs, through the maps, and serially, and checks them as the class
     * comment says; then again with tasks so large and no prefix, so that no key is split, where
     * key runs must equal the serial window bit for bit. Returns the tasks worker threads computed
     * with key runs.
     */
    private long assertKeyRuns(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        ctx.setParallelWindowEnabled(true);
        final long workerTasks;
        final String keyRuns;
        try {
            try (RecordCursorFactory factory = engine.select(query, ctx)) {
                final AsyncWindowAtom atom = findAtom(factory);
                Assert.assertTrue(query, atom.isKeyRunEnabled());
                TestUtils.assertContains(plan(factory, ctx), "keyRuns: true");
                final StringSink sink = new StringSink();
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    printBits(cursor, factory.getMetadata(), sink);
                }
                keyRuns = sink.toString();
                // a LIMIT may stop before the first task: the query's thread computes the first rows
                Assert.assertTrue(query, atom.getKeyRunTaskCount() > 0 || query.contains(" limit "));
                workerTasks = atom.getWorkerThreadTaskCount();
            }
            // compiled again with key runs off: through the maps
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_KEY_RUNS_ENABLED, "false");
            try (RecordCursorFactory factory = engine.select(query, ctx)) {
                final AsyncWindowAtom atom = findAtom(factory);
                Assert.assertFalse(query, atom.isKeyRunEnabled());
                Assert.assertFalse(query, Chars.contains(plan(factory, ctx), "keyRuns"));
                final StringSink sink = new StringSink();
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    printBits(cursor, factory.getMetadata(), sink);
                }
                Assert.assertEquals(query, 0, atom.getKeyRunTaskCount());
                TestUtils.assertEquals(query, sink, keyRuns);
            }
        } finally {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_KEY_RUNS_ENABLED, "true");
            ctx.setParallelWindowEnabled(false);
        }
        final String serial = bits(engine, ctx, query, false, false);
        if (findSplitMode(engine, ctx, query) == AsyncWindowSplitPlan.MODE_NONE) {
            TestUtils.assertEquals(query, serial, keyRuns);
        }
        // no key split: one task takes every key whole
        final long minRows = engine.getConfiguration().getSqlParallelWindowMinRows();
        final long taskRows = engine.getConfiguration().getSqlParallelWindowTaskRows();
        final long roundRows = engine.getConfiguration().getSqlParallelWindowRoundRows();
        final long maxKeyRows = engine.getConfiguration().getSqlParallelWindowMaxKeyRows();
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 10_000_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 10_000_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 10_000_000);
        try {
            TestUtils.assertEquals(query, serial, bits(engine, ctx, query, true, true));
        } finally {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, minRows);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, taskRows);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, roundRows);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, maxKeyRows);
        }
        return workerTasks;
    }

    /**
     * Table {@code k}: a symbol index on {@code sym}, half of the rows in key BIG, the rest over
     * S0..S9, one in eleven NULL, interleaved in time over three days. Every other column has
     * NULLs; one row in thirteen has {@code bsize + asize = 0}; {@code d} has -0.0 and NaN.
     */
    private void createTable(CairoEngine engine, SqlExecutionContext ctx, String partitionBy, int rows) throws Exception {
        engine.execute(
                "create table k (sym symbol index type " + indexType + ", ex symbol, bid float, bsize int, ask float, asize int, " +
                        "d double, x long, b boolean, by byte, sh short, ch char, i int, ip ipv4, f float, l long, dt date, " +
                        "g1 geohash(5b), g2 geohash(10b), g3 geohash(20b), g4 geohash(40b), v varchar, u uuid, ts timestamp)" +
                        " timestamp(ts) partition by " + partitionBy,
                ctx
        );
        engine.execute(
                "insert into k select" +
                        " case when x % 2 = 0 then 'BIG' when x % 11 = 0 then null else 'S' || (x / 2 % 10) end," +
                        " case when x % 6 = 0 then null else rnd_symbol('A', 'B', 'C') end," +
                        " case when x % 7 = 3 then null else rnd_float() end," +
                        " case when x % 13 = 0 then 0 when x % 17 = 0 then null else rnd_int(1, 1000, 0) end," +
                        " case when x % 19 = 0 then null else rnd_float() end," +
                        " case when x % 13 = 0 then 0 when x % 23 = 0 then null else rnd_int(1, 1000, 0) end," +
                        " case when x % 5 = 0 then -0.0 when x % 9 = 0 then null else rnd_double() * 100 - 50 end," +
                        " x," +
                        " rnd_boolean()," +
                        " rnd_byte(-100, 100)," +
                        " rnd_short(-1000, 1000)," +
                        " rnd_char()," +
                        " case when x % 4 = 0 then null else rnd_int() end," +
                        " case when x % 5 = 0 then null else rnd_ipv4() end," +
                        " case when x % 6 = 0 then null else rnd_float() * 10 end," +
                        " case when x % 7 = 0 then null else rnd_long() end," +
                        " case when x % 8 = 0 then null else rnd_date() end," +
                        " case when x % 3 = 0 then null else rnd_geohash(5) end," +
                        " case when x % 3 = 1 then null else rnd_geohash(10) end," +
                        " case when x % 3 = 2 then null else rnd_geohash(20) end," +
                        " case when x % 4 = 1 then null else rnd_geohash(40) end," +
                        " rnd_varchar(1, 5, 1)," +
                        " rnd_uuid4()," +
                        " (x * " + (3 * 86_400_000_000L / rows) + "L)::timestamp" +
                        " from long_sequence(" + rows + ")",
                ctx
        );
    }
}

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
import io.questdb.griffin.engine.window.AsyncWindowAtom;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
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
import java.util.TreeSet;

/**
 * The column-wise kernels of the parallel window's row-by-row tasks (see
 * {@code AsyncWindowRowKernel}): {@code lag} over a key-major walk, projections of {@code +} and
 * {@code -}, and filters that compare (NYSE TAQ idx 61). Each query runs with the parallel window
 * off, which is the serial plan, with it on and the kernels off (the row path), and with both on;
 * all three must agree bit for bit, over data with NULLs, extreme values, NaN, infinities, -0.0
 * and runs of equal values, with keys that span tasks (warm-up rows), on a worker pool and
 * without one.
 */
@RunWith(Parameterized.class)
public class WindowRowKernelTest extends AbstractCairoTest {
    private static final String IN_LIST = "'BIG', 'K1', 'K2', 'K3', 'K7', 'MISSING', 'K11', 'K12', 'K13', 'K14', 'K15', 'K16', 'K17', 'K18', 'K19'";
    private static final int ROWS = 12_000;
    private final String indexType;

    public WindowRowKernelTest(String indexType) {
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
        sqlExecutionContext.changePageFrameSizes(1, 128);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 61);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 100_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 250);
    }

    @Test
    public void testColumnTopsAndConversions() throws Exception {
        // a column added after the first rows has no data in their frames: it reads NULL there;
        // a column whose type changed is read through a conversion, which takes the row path
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            engine.execute("alter table t add column late int", sqlExecutionContext);
            engine.execute("alter table t add column late_d double", sqlExecutionContext);
            engine.execute(
                    "insert into t (time, sym, i, l, d, f, late, late_d) select" +
                            " (" + (3 * 86_400_000_000L) + "L + x * 1_000_000L)::timestamp, 'K' || (x % 7), x::int, x, x / 7.0, (x / 3.0)::float," +
                            " case when x % 5 = 0 then null else (x % 13)::int end, x / 11.0" +
                            " from long_sequence(3000)",
                    sqlExecutionContext
            );
            final String[] queries = {
                    // every row, the rows before the column top included: NULL there, not 0
                    "SELECT sym, time, late, pl, late_d, pd FROM (SELECT sym, time, l, late, late_d, lag(late) OVER (PARTITION BY sym ORDER BY time) pl, lag(late_d) OVER (PARTITION BY sym ORDER BY time) pd FROM t)" +
                            " WHERE l != 0 ORDER BY sym, time, late, pl, late_d, pd",
                    "SELECT sym, late FROM (SELECT sym, late, lag(late) OVER (PARTITION BY sym ORDER BY time) pl FROM t) WHERE late < pl ORDER BY sym, late",
                    "SELECT sym, late_d FROM (SELECT sym, late_d, lag(late_d) OVER (PARTITION BY sym ORDER BY time) pl FROM t) WHERE late_d >= pl ORDER BY sym, late_d",
            };
            for (String query : queries) {
                Assert.assertTrue(query, assertKernelMatches(engine, sqlExecutionContext, query) > 0);
            }
            engine.execute("alter table t alter column i type long", sqlExecutionContext);
            assertKernelMatches(engine, sqlExecutionContext, q61ManualOpt("i"));
            assertKernelMatches(engine, sqlExecutionContext, q61Index("i"));
        });
    }

    @Test
    public void testComparisonsAndTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            int kernelQueries = 0;
            for (String query : comparisonQueries()) {
                if (assertKernelMatches(engine, sqlExecutionContext, query) > 0) {
                    kernelQueries++;
                }
            }
            // every comparison of an INT, LONG or DOUBLE value, and of FLOAT read as DOUBLE
            Assert.assertTrue("kernel queries: " + kernelQueries, kernelQueries >= 90);
        });
    }

    @Test
    public void testLargeFrames() throws Exception {
        // one partition, one frame: one key's rows end and the next key's start in the same
        // frame, so a batch must stop at the key's end, not only at the frame's
        sqlExecutionContext.changePageFrameSizes(1, 1_000_000);
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "NONE");
            for (String column : new String[]{"i", "l"}) {
                Assert.assertTrue(assertKernelMatches(engine, sqlExecutionContext, q61ManualOpt(column)) > 0);
                Assert.assertTrue(assertKernelMatches(engine, sqlExecutionContext, q61Index(column)) > 0);
            }
            Assert.assertTrue(assertKernelMatches(engine, sqlExecutionContext,
                    "SELECT sym, time, i, pi, d, pd FROM (SELECT sym, time, i, d, lag(i) OVER (PARTITION BY sym ORDER BY time) pi, lag(d) OVER (PARTITION BY sym ORDER BY time) pd FROM t)" +
                            " WHERE i != 0 ORDER BY sym, time, i, pi, d, pd") > 0);
            Assert.assertTrue(assertKernelMatches(engine, sqlExecutionContext,
                    "SELECT sym, time, i, pi FROM (SELECT sym, time, i, lag(i, 3) OVER (PARTITION BY sym ORDER BY time) pi FROM t WHERE sym IN (" + IN_LIST + "))" +
                            " WHERE i >= pi ORDER BY sym, time, i, pi") > 0);
        });
    }

    @Test
    public void testIdx61() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            for (String column : new String[]{"i", "l"}) {
                Assert.assertTrue(assertKernelMatches(engine, sqlExecutionContext, q61ManualOpt(column)) > 0);
                Assert.assertTrue(assertKernelMatches(engine, sqlExecutionContext, q61Index(column)) > 0);
            }
            // every row, not only the few the filter keeps, and the lag itself
            Assert.assertTrue(assertKernelMatches(engine, sqlExecutionContext,
                    "SELECT sym, time, i, pi, d, pd FROM (SELECT sym, time, i, d, lag(i) OVER (PARTITION BY sym ORDER BY time) pi, lag(d) OVER (PARTITION BY sym ORDER BY time) pd FROM t)" +
                            " WHERE i != 0 ORDER BY sym, time, i, pi, d, pd") > 0);
        });
    }

    @Test
    public void testKeysAndOffsets() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            final String[] queries = {
                    // an IN list, missing keys and duplicates included
                    "SELECT sym, i FROM (SELECT sym, i, lag(i) OVER (PARTITION BY sym ORDER BY time) p FROM t WHERE sym IN (" + IN_LIST + ", 'K1')) WHERE i < p ORDER BY sym, i",
                    // a single key: no PARTITION BY
                    "SELECT time, i, p FROM (SELECT time, i, lag(i) OVER (ORDER BY time) p FROM t WHERE sym = 'BIG') WHERE i < p",
                    "SELECT time, d, p FROM (SELECT time, d, lag(d) OVER (ORDER BY time) p FROM t WHERE sym = 'BIG') WHERE d - p > 3.5",
                    // offsets of 2 and 3 rows: warm-up rows rebuild them
                    "SELECT sym, time, i, p FROM (SELECT sym, time, i, lag(i, 2) OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE i < p ORDER BY sym, time, i, p",
                    "SELECT sym, time, l, p, q FROM (SELECT sym, time, l, lag(l, 3) OVER (PARTITION BY sym ORDER BY time) p, lag(l) OVER (PARTITION BY sym ORDER BY time) q FROM t) WHERE p < q ORDER BY sym, time, l, p, q",
                    // two lags of one column, two filters, a projection over a projected column
                    "SELECT sym, time, x FROM (SELECT sym, time, i - p x, p FROM (SELECT sym, time, i, lag(i) OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE p > 0) WHERE x < 0 ORDER BY sym, time, x",
                    // the NULL key
                    "SELECT sym, time, l FROM (SELECT sym, time, l, lag(l) OVER (PARTITION BY sym ORDER BY time) p FROM t WHERE sym IN (NULL, 'K3')) WHERE l != p ORDER BY sym, time, l",
            };
            for (String query : queries) {
                Assert.assertTrue(query, assertKernelMatches(engine, sqlExecutionContext, query) > 0);
            }
            // beyond the kernels: the row path computes them
            final String[] rowPath = {
                    "SELECT sym, i FROM (SELECT sym, i, lag(i, 1, 0) OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE i < p ORDER BY sym, i",
                    "SELECT sym, i FROM (SELECT sym, i, lag(i) IGNORE NULLS OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE i < p ORDER BY sym, i",
                    "SELECT sym, i FROM (SELECT sym, i, lag(i) OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE i * 2 < p ORDER BY sym, i",
                    "SELECT sym, i FROM (SELECT sym, i, lag(i) OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE i < p AND i > 0 ORDER BY sym, i",
                    "SELECT sym, s FROM (SELECT sym, s, lag(s) OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE s != p ORDER BY sym, s",
            };
            for (String query : rowPath) {
                Assert.assertEquals(query, 0, assertKernelMatches(engine, sqlExecutionContext, query));
            }
        });
    }

    @Test
    public void testLimitAndRewindOnWorkerPool() throws Exception {
        inPool((engine, ctx) -> {
            createTable(engine, ctx);
            for (String query : new String[]{
                    q61ManualOpt("i") + " LIMIT 7",
                    "SELECT sym, time, i, p FROM (SELECT sym, time, i, lag(i) OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE i != p ORDER BY sym, time, i, p LIMIT 1000",
                    "SELECT sym, time, i, p FROM (SELECT sym, time, i, lag(i) OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE i != p ORDER BY sym, time, i, p LIMIT 3000, 3100",
            }) {
                final String expected = serial(engine, ctx, query);
                ctx.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = engine.select(query, ctx)) {
                    for (int pass = 0; pass < 3; pass++) {
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertEquals(query, expected, rawRows(cursor, factory.getMetadata()));
                        }
                    }
                    // compiled at the first execution, once the plan has every step
                    Assert.assertTrue(query, ((AsyncWindowAtom) findAsyncFactory(factory).getAtom()).isRowKernelEnabled());
                    final PerWorkerLocks locks = TestUtils.findPerWorkerLocks(factory, "async window");
                    Assert.assertEquals(0, locks.getAcquiredSlotCount());
                } finally {
                    ctx.setParallelWindowEnabled(false);
                }
            }
        });
    }

    @Test
    public void testOnWorkerPool() throws Exception {
        inPool((engine, ctx) -> {
            createTable(engine, ctx);
            long kernelTasks = 0;
            for (String column : new String[]{"i", "l"}) {
                kernelTasks += assertKernelMatches(engine, ctx, q61ManualOpt(column));
                kernelTasks += assertKernelMatches(engine, ctx, q61Index(column));
            }
            for (String query : comparisonQueries()) {
                kernelTasks += assertKernelMatches(engine, ctx, query);
            }
            Assert.assertTrue(kernelTasks > 0);
        });
    }

    @Test
    public void testTouchedColumns() throws Exception {
        // a row by row task loads the columns its rows read, named by the scan whose frames it reads
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            assertTouched(q61ManualOpt("i"), "i", "sym");
            assertTouched(q61Index("l"), "l", "sym");
            // a ROWS frame reads no ORDER BY, a RANGE frame does
            assertTouched("SELECT sym, d, s FROM (SELECT sym, d, sum(d) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) s, lag(i) OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE s > p ORDER BY sym, d, s", "d", "i", "sym");
            assertTouched("SELECT sym, d, s FROM (SELECT sym, d, sum(d) OVER (PARTITION BY sym ORDER BY time RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW) s FROM t) WHERE s > 0 ORDER BY sym, d, s", "d", "sym", "time");
        });
    }

    @Test
    public void testSwitchedOffWithKeyRuns() throws Exception {
        // cairo.sql.parallel.window.key.runs.enabled=false switches the kernels off too
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_KEY_RUNS_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            Assert.assertEquals(0, assertKernelMatches(engine, sqlExecutionContext, q61ManualOpt("i")));
        });
    }

    private void assertTouched(String query, String... columns) throws Exception {
        assertKernelMatches(engine, sqlExecutionContext, query);
        sqlExecutionContext.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
            final AsyncWindowRecordCursorFactory async = findAsyncFactory(factory);
            final boolean[] touched = ((AsyncWindowAtom) async.getAtom()).getRowTouchedColumns();
            Assert.assertNotNull(query, touched);
            final RecordMetadata scanMetadata = async.getScanMetadata();
            final TreeSet<String> actual = new TreeSet<>();
            for (int c = 0; c < touched.length; c++) {
                if (touched[c]) {
                    actual.add(scanMetadata.getColumnName(c));
                }
            }
            Assert.assertEquals(query, new TreeSet<>(Arrays.asList(columns)), actual);
        } finally {
            sqlExecutionContext.setParallelWindowEnabled(false);
        }
    }

    private static String q61Index(String column) {
        return "SELECT sym, " + column + " AS seqDecr FROM (SELECT sym, " + column + ", " + column + " - lag(" + column + ") OVER (PARTITION BY sym ORDER BY time) AS seq_delta FROM t) WHERE seq_delta < 0 ORDER BY sym, seqDecr";
    }

    private static String q61ManualOpt(String column) {
        return "SELECT sym, " + column + " AS seqDecr FROM (SELECT sym, " + column + ", lag(" + column + ") OVER (PARTITION BY sym ORDER BY time) AS prev_seq FROM t) WHERE " + column + " < prev_seq ORDER BY sym, seqDecr";
    }

    private static String[] comparisonQueries() {
        final String[] columns = {"i", "l", "d", "f"};
        final String[] ops = {"<", "<=", ">", ">=", "=", "!="};
        final String[] queries = new String[columns.length * ops.length * 3];
        int q = 0;
        for (String c : columns) {
            for (String op : ops) {
                // the value against its lag, both ways round
                queries[q++] = "SELECT sym, time, " + c + ", p FROM (SELECT sym, time, " + c + ", lag(" + c + ") OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE " + c + " " + op + " p ORDER BY sym, time, " + c + ", p";
                queries[q++] = "SELECT sym, time, " + c + ", p FROM (SELECT sym, time, " + c + ", lag(" + c + ") OVER (PARTITION BY sym ORDER BY time) p FROM t) WHERE p " + op + " " + c + " ORDER BY sym, time, " + c + ", p";
                // the difference against a constant, an IN list
                queries[q++] = "SELECT sym, time, " + c + ", x FROM (SELECT sym, time, " + c + ", " + c + " - lag(" + c + ") OVER (PARTITION BY sym ORDER BY time) x FROM t WHERE sym IN (" + IN_LIST + ")) WHERE x " + op + " 0 ORDER BY sym, time, " + c + ", x";
            }
        }
        // a value against a constant of its own type (INT against INT), and against a value of
        // another type, read through the base class's conversion (LONG and INT as DOUBLE)
        final String[] mixed = {"i OP 5", "5 OP i", "l OP d", "i OP d", "f OP l", "d OP p"};
        final String[] all = Arrays.copyOf(queries, q + mixed.length * ops.length);
        for (String m : mixed) {
            for (String op : ops) {
                all[q++] = "SELECT sym, time, i, l, d, f, p FROM (SELECT sym, time, i, l, d, f, lag(l) OVER (PARTITION BY sym ORDER BY time) p FROM t)" +
                        " WHERE " + m.replace("OP", op) + " ORDER BY sym, time, i, l, d, f, p";
            }
        }
        return all;
    }

    private static AsyncWindowRecordCursorFactory findAsyncFactory(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncWindowRecordCursorFactory async) {
                return async;
            }
        }
        Assert.fail("no Async Window in the factory tree");
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
                    case ColumnType.SHORT -> sink.put(record.getShort(c));
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
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                return rawRows(cursor, factory.getMetadata());
            }
        }
    }

    /**
     * Runs the query serially, on the parallel window's row path and with its kernels, twice with
     * a rewind each; compares every result with the serial one bit for bit. Returns the tasks the
     * kernels computed.
     */
    private long assertKernelMatches(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        final String expected = serial(engine, ctx, query);
        ctx.setParallelWindowEnabled(true);
        long kernelTasks = 0;
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            final AsyncWindowAtom atom = (AsyncWindowAtom) findAsyncFactory(factory).getAtom();
            for (int pass = 0; pass < 2; pass++) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertEquals(query, expected, rawRows(cursor, factory.getMetadata()));
                    kernelTasks += atom.getRowKernelTaskCount();
                    cursor.toTop();
                    TestUtils.assertEquals(query, expected, rawRows(cursor, factory.getMetadata()));
                    kernelTasks += atom.getRowKernelTaskCount();
                }
            }
            Assert.assertEquals(query, kernelTasks > 0, atom.isRowKernelEnabled());
        } finally {
            ctx.setParallelWindowEnabled(false);
        }
        // the row path of the same plan
        AsyncWindowAtom.DEBUG_DISABLE_ROW_KERNELS = true;
        ctx.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                TestUtils.assertEquals(query, expected, rawRows(cursor, factory.getMetadata()));
                Assert.assertEquals(0, ((AsyncWindowAtom) findAsyncFactory(factory).getAtom()).getRowKernelTaskCount());
            }
        } finally {
            AsyncWindowAtom.DEBUG_DISABLE_ROW_KERNELS = false;
            ctx.setParallelWindowEnabled(false);
        }
        return kernelTasks;
    }

    /**
     * Keys BIG (40% of the rows), K0..K19 and NULL (one row in thirteen). Each column has NULLs,
     * runs of equal values, and its type's extremes: INT and LONG their min and max, DOUBLE and
     * FLOAT NaN, infinities, -0.0, 0.0 and values that differ in the last bit. s is a SHORT.
     */
    private void createTable(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        createTable(engine, ctx, "DAY");
    }

    private void createTable(CairoEngine engine, SqlExecutionContext ctx, String partitionBy) throws Exception {
        engine.execute(
                "create table t (time timestamp, sym symbol index type " + indexType + ", i int, l long, d double, f float, s short)" +
                        " timestamp(time) partition by " + partitionBy,
                ctx
        );
        engine.execute(
                "insert into t select" +
                        " ((x / 2) * " + (3 * 86_400_000_000L / ROWS) + "L)::timestamp," +
                        " case when x % 5 < 2 then 'BIG' when x % 13 = 0 then null else 'K' || (x % 20) end," +
                        " case when x % 17 = 0 then null when x % 101 = 0 then 2147483647 when x % 103 = 0 then -2147483647" +
                        " when x % 7 < 3 then 5 else (x % 97 - (x % 7) * 3)::int end," +
                        " case when x % 19 = 0 then null when x % 101 = 0 then 9223372036854775807L when x % 103 = 0 then -9223372036854775807L" +
                        " when x % 7 < 3 then 5 else x % 89 - (x % 5) * 11 end," +
                        " case when x % 23 = 0 then null when x % 29 = 0 then cast('Infinity' as double) when x % 31 = 0 then cast('-Infinity' as double)" +
                        " when x % 37 = 0 then -0.0 when x % 41 = 0 then 0.0 when x % 7 < 3 then 0.1 + 0.2 when x % 7 = 3 then 0.3" +
                        " else (x % 83) / 7.0 end," +
                        " case when x % 23 = 0 then null when x % 29 = 0 then cast('Infinity' as float) when x % 37 = 0 then -0.0::float" +
                        " when x % 7 < 3 then 0.5::float else ((x % 83) / 3.0)::float end," +
                        " (x % 11)::short" +
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

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
package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.DeferredSingleSymbolFilterPageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.IndexKeyPageFrameCursor;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * The parallel plan of a single-symbol POSTING index scan (an Async Filter whose workers read
 * each frame's rows of the symbol from the frame's posting index), against the serial scan with
 * the frame dispatch switch off: the same rows in the same order, for every shape the serial
 * scan serves, and the choice between the two made per open.
 */
public class IndexKeyScanParallelTest extends AbstractCairoTest {
    private static final String[] QUERIES = {
            "select * from t where sym = 'A'",
            "select ts, x, y from t where sym = 'R7'",
            "select ts, (x + y) / 2 m, sym from t where sym = 'A'",
            "select * from t where sym = 'A' and ts in '1970-01-02'",
            "select * from t where sym = 'A' and ts between '1970-01-01T07' and '1970-01-03T19'",
            "select * from t where sym = null",
            "select * from t where sym = 'NOPE'",
            "select * from t where sym = $1",
            "select * from t where sym = 'A' limit 777",
            "select * from t where sym = 'A' limit 1000, 1333",
            "select ts, x from t where sym = 'B' order by x desc limit 20",
            "select count() from t where sym = 'A'",
            "select * from (select * from t where sym = 'A') where x % 7 = 0",
            // a column added later: the first partitions have no posting reader for it
            "select ts, x, s2 from t where s2 = 'q'",
            "select ts, x, s2 from t where s2 = null",
    };

    @Override
    @After
    public void tearDown() throws Exception {
        DeferredSingleSymbolFilterPageFrameRecordCursorFactory.resetParallelThresholdsForTesting();
        super.tearDown();
    }

    @Test
    public void testChoiceIsPerOpen() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            // a frequent symbol opens the parallel plan, a rare one the serial scan
            DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(4_000, Long.MAX_VALUE, Long.MAX_VALUE);
            final String query = "select * from t where sym = $1";
            try (RecordCursorFactory factory = select(query)) {
                final DeferredSingleSymbolFilterPageFrameRecordCursorFactory scan = findScan(factory);
                Assert.assertNotNull(scan);
                Assert.assertNotNull(scan.getParallelScan());
                for (String symbol : new String[]{"A", "R3", "A", "NOPE", "B"}) {
                    bindVariableService.clear();
                    bindVariableService.setStr(0, symbol);
                    final long[] before = scan.getOpenCounts();
                    final String actual = print(factory, sqlExecutionContext);
                    final long[] after = scan.getOpenCounts();
                    final boolean frequent = !symbol.startsWith("R") && !symbol.equals("NOPE");
                    Assert.assertEquals(symbol + " estimate " + scan.getLastEstimateRows() + " opens " + java.util.Arrays.toString(after), frequent, after[1] > before[1]);
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
                    TestUtils.assertEquals(symbol, print(engine, sqlExecutionContext, query), actual);
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
                }
            }
        });
    }

    @Test
    public void testCursorHeldOpenAcrossCommitsAndReopen() throws Exception {
        // A parallel open's cursor, read while the table takes appends to its last partition, O3
        // rows into old partitions, new partitions and an UPDATE, returns the snapshot it opened on,
        // row for row and bit for bit, as a serial cursor opened at the same time does; and the
        // cached factory, reopened after those commits, returns the serial scan's rows again.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx);
                for (String query : new String[]{
                        "select * from t where sym = 'A'",
                        "select * from t where s2 = 'q'",
                        "select * from t where sym = 'A' and ts > '1970-01-02'",
                }) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
                    try (RecordCursorFactory serialFactory = engine.select(query, ctx)) {
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
                        DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(0, 0, 0);
                        try (RecordCursorFactory factory = engine.select(query, ctx)) {
                            final DeferredSingleSymbolFilterPageFrameRecordCursorFactory scan = findScan(factory);
                            Assert.assertNotNull(scan.getParallelScan());
                            Assert.assertNull(findScan(serialFactory).getParallelScan());
                            for (int round = 0; round < 4; round++) {
                                final StringSink expected = new StringSink();
                                final StringSink actual = new StringSink();
                                final long parallelOpens = scan.getOpenCounts()[1];
                                try (
                                        RecordCursor serialCursor = serialFactory.getCursor(ctx);
                                        RecordCursor cursor = factory.getCursor(ctx)
                                ) {
                                    int n = 0;
                                    while (cursor.hasNext()) {
                                        rawRow(cursor.getRecord(), factory.getMetadata(), actual);
                                        if (++n == 50) {
                                            engine.execute("insert into t select 'A', 1_000_000 + x, 0.5, (" + (round * 1_000_000L)
                                                    + " + x * 1_000_000L)::timestamp, 'q' from long_sequence(500)", ctx);
                                            engine.execute("insert into t select case when x % 2 = 0 then 'A' else 'B' end, 2_000_000 + x, 0.25, (400_000_000_000L + "
                                                    + round + " * 10_000_000_000L + x * 1_000_000L)::timestamp, 'q' from long_sequence(500)", ctx);
                                            engine.execute("update t set y = 7.0 where x % 1000 = 3", ctx);
                                        }
                                    }
                                    Assert.assertTrue(query, n > 50);
                                    while (serialCursor.hasNext()) {
                                        rawRow(serialCursor.getRecord(), serialFactory.getMetadata(), expected);
                                    }
                                }
                                Assert.assertEquals(query + " [round " + round + ']', parallelOpens + 1, scan.getOpenCounts()[1]);
                                TestUtils.assertEquals(query + " [held open, round " + round + ']', expected, actual);
                                TestUtils.assertEquals(query + " [reopened, round " + round + ']', print(serialFactory, ctx), print(factory, ctx));
                            }
                        }
                    }
                    DeferredSingleSymbolFilterPageFrameRecordCursorFactory.resetParallelThresholdsForTesting();
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testDescendingScansStaySerial() throws Exception {
        // The parallel plan's frames come in ascending order, so a descending scan never opens it:
        // with every threshold at 0, these return the switch-off rows and open no parallel plan.
        // Negative LIMITs and an ascending ORDER BY ts with a LIMIT are controls that may open it.
        final String[] queries = {
                "select * from t where sym = 'A' order by ts desc",
                "select * from t where sym = 'A' order by ts desc limit 7",
                "select * from t where sym = 'A' and ts in '1970-01-02T03;5h' order by ts desc",
                "select * from t where s2 = 'q' order by ts desc limit 11",
                "select * from t where sym = 'A' limit -7",
                "select * from t where sym = 'A' limit -1000, -990",
                "select * from t where s2 = null limit -5",
                "select * from t where sym = 'A' order by ts limit 5",
                "select * from t where sym = 'A' and ts > '1970-01-03T19:30' limit 3",
        };
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx);
                for (String query : queries) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
                    final String expected = print(engine, ctx, query);
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
                    DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(0, 0, 0);
                    try (RecordCursorFactory factory = engine.select(query, ctx)) {
                        final DeferredSingleSymbolFilterPageFrameRecordCursorFactory scan = findScan(factory);
                        Assert.assertNotNull(query, scan);
                        for (int run = 0; run < 2; run++) {
                            TestUtils.assertEquals(query + " [run " + run + ']', expected, print(factory, ctx));
                        }
                        if (query.contains(" desc")) {
                            Assert.assertTrue(query, scan.getParallelScan() == null || scan.getOpenCounts()[1] == 0);
                        }
                    }
                    DeferredSingleSymbolFilterPageFrameRecordCursorFactory.resetParallelThresholdsForTesting();
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testLimitBoundsTheParallelOpen() throws Exception {
        // A constant LIMIT caps what an open reads: below the thresholds it keeps the serial scan
        // without counting the symbol's rows; above them the parallel plan stops dispatching frames
        // once it has the rows, so it reads fewer frames than without the LIMIT.
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(4_000, Long.MAX_VALUE, Long.MAX_VALUE);
            try (RecordCursorFactory factory = select("select * from t where sym = 'A' limit 10")) {
                final DeferredSingleSymbolFilterPageFrameRecordCursorFactory scan = findScan(factory);
                Assert.assertNotNull(scan.getParallelScan());
                final String actual = print(factory, sqlExecutionContext);
                Assert.assertArrayEquals(new long[]{1, 0}, scan.getOpenCounts());
                Assert.assertEquals(10, scan.getLastEstimateRows());
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
                TestUtils.assertEquals(print(engine, sqlExecutionContext, "select * from t where sym = 'A' limit 10"), actual);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            }
            DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(0, 0, 0);
            final long[] frames = new long[2];
            final String[] queries = {"select * from t where sym = 'A'", "select * from t where sym = 'A' limit 10"};
            for (int i = 0; i < 2; i++) {
                IndexKeyPageFrameCursor.TEST_INDEX_FRAMES.set(0);
                try (RecordCursorFactory factory = select(queries[i])) {
                    final String actual = print(factory, sqlExecutionContext);
                    Assert.assertEquals(1, findScan(factory).getOpenCounts()[1]);
                    frames[i] = IndexKeyPageFrameCursor.TEST_INDEX_FRAMES.get();
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
                    TestUtils.assertEquals(queries[i], print(engine, sqlExecutionContext, queries[i]), actual);
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
                }
            }
            Assert.assertTrue(java.util.Arrays.toString(frames), frames[1] < frames[0]);
        });
    }

    @Test
    public void testParallelMatchesSerialOnTestContext() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            assertArms(engine, sqlExecutionContext, true);
        });
    }

    @Test
    public void testParallelMatchesSerialOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx);
                assertArms(engine, ctx, true);
            }, configuration, LOG);
        });
    }

    @Test
    public void testParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            // the partitions from before s2 existed
            execute("alter table t convert partition to parquet where ts < '1970-01-02'");
            assertArms(engine, sqlExecutionContext, false);
        });
    }

    private static void assertArms(CairoEngine engine, SqlExecutionContext ctx, boolean nativeColumnTops) throws Exception {
        long parallelOpens = 0;
        IndexKeyPageFrameCursor.TEST_INDEX_FRAMES.set(0);
        IndexKeyPageFrameCursor.TEST_FILTER_FRAMES.set(0);
        for (String query : QUERIES) {
            bindVariableService.clear();
            bindVariableService.setStr(0, "B");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
            final String expected = print(engine, ctx, query);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            // always parallel when the scan has a parallel plan
            DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(0, 0, 0);
            try (RecordCursorFactory factory = engine.select(query, ctx)) {
                for (int run = 0; run < 2; run++) {
                    TestUtils.assertEquals(query + " [parallel, run " + run + "]", expected, print(factory, ctx));
                }
                final DeferredSingleSymbolFilterPageFrameRecordCursorFactory scan = findScan(factory);
                if (scan != null) {
                    parallelOpens += scan.getOpenCounts()[1];
                }
            }
            // the default thresholds: these selections are small, so the serial scan
            DeferredSingleSymbolFilterPageFrameRecordCursorFactory.resetParallelThresholdsForTesting();
            TestUtils.assertEquals(query + " [default]", expected, print(engine, ctx, query));
        }
        Assert.assertTrue("no parallel open", parallelOpens > 0);
        // most frames read from the index; the native partitions from before s2 existed through the
        // filter (Parquet frames are always filtered)
        Assert.assertTrue(IndexKeyPageFrameCursor.TEST_INDEX_FRAMES.get() > 0);
        Assert.assertEquals(nativeColumnTops, IndexKeyPageFrameCursor.TEST_FILTER_FRAMES.get() > 0);
    }

    // 4 days by the hour; A in half the rows, B in a quarter, R0..R49 rare, every 17th NULL; s2
    // added after the first day's partitions
    private static void createTable(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        engine.execute("create table t (sym symbol index type posting, x long, y double, ts timestamp) timestamp(ts) partition by hour bypass wal", ctx);
        engine.execute(
                "insert into t select case when x % 17 = 0 then null when x % 2 = 0 then 'A' when x % 4 = 1 then 'B' else 'R' || (x % 50) end, " +
                        "x, rnd_double(), (x * 4_320_000L)::timestamp from long_sequence(20_000)",
                ctx
        );
        engine.execute("alter table t add column s2 symbol index type posting", ctx);
        engine.execute(
                "insert into t select case when x % 3 = 0 then 'q' when x % 5 = 0 then null else 'w' end, " +
                        "20_000 + x, rnd_double(), ((20_000 + x) * 4_320_000L)::timestamp, case when x % 3 = 0 then 'q' when x % 5 = 0 then null else 'w' end from long_sequence(60_000)",
                ctx
        );
    }

    private static DeferredSingleSymbolFilterPageFrameRecordCursorFactory findScan(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof DeferredSingleSymbolFilterPageFrameRecordCursorFactory scan) {
                return scan;
            }
        }
        return null;
    }

    private static String print(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            return print(factory, ctx);
        }
    }

    private static String print(RecordCursorFactory factory, SqlExecutionContext ctx) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
        return sink.toString();
    }

    // a row as its raw values: symbols as text, doubles as their bits
    private static void rawRow(Record record, RecordMetadata metadata, StringSink sink) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int tag = ColumnType.tagOf(metadata.getColumnType(i));
            if (tag == ColumnType.SYMBOL) {
                sink.put(record.getSymA(i));
            } else if (tag == ColumnType.DOUBLE) {
                sink.put(Double.doubleToRawLongBits(record.getDouble(i)));
            } else {
                sink.put(record.getLong(i));
            }
            sink.put('\t');
        }
        sink.put('\n');
    }
}

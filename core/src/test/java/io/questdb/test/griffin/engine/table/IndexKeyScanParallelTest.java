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
import io.questdb.std.Rnd;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.PartitionGeometry;
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
    public void testCompositePiecesRelocatedAndShifted() throws Exception {
        // Merge-append moves the pieces it merges to the files' tail, above their partition rows, and
        // leaves the pieces between them below theirs. The parallel scan reads each frame's rows of
        // the symbol from the posting index in file rows, at its own piece's shift.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 128);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 100_000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting, x long, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into t select rnd_symbol('A', 'B', 'C'), x, timestamp_sequence('1970-01-01', 21_600_000L) from long_sequence(12_000)");
            drainWalQueue();
            final Rnd rnd = new Rnd(5, 13);
            for (int c = 0; c < 10; c++) {
                final long minute = rnd.nextLong(3 * 24 * 60 - 10);
                execute("insert into t select rnd_symbol('A', 'B', 'R'), 100_000 + x, dateadd('m', " + minute + ", '1970-01-01')::timestamp + x "
                        + "from long_sequence(" + (1 + rnd.nextInt(4)) + ")");
                drainWalQueue();
            }
            try (TableReader reader = getReader("t")) {
                final PartitionGeometry geometry = reader.getGeometry();
                boolean isAbove = false;
                boolean isBelow = false;
                for (int p = 0, n = reader.getPartitionCount(); p < n; p++) {
                    if (reader.getTxFile().isPartitionComposite(p)) {
                        geometry.resolve(p);
                        for (int i = 0, m = geometry.getPieceCount(p); i < m; i++) {
                            isAbove |= geometry.getPieceShift(p, i) > 0;
                            isBelow |= geometry.getPieceShift(p, i) < 0;
                        }
                    }
                }
                Assert.assertTrue("no relocated piece", isAbove && isBelow);
            }
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(0, 0, 0);
            for (String query : new String[]{
                    "select * from t where sym = 'A'",
                    "select * from t where sym = 'R'",
                    "select * from t where sym = 'B' and ts > '1970-01-02'",
                    "select count() from t where sym = 'A'",
            }) {
                try (RecordCursorFactory factory = select(query)) {
                    final DeferredSingleSymbolFilterPageFrameRecordCursorFactory scan = findScan(factory);
                    Assert.assertNotNull(query, scan);
                    Assert.assertNotNull(query, scan.getParallelScan());
                    final long parallelOpens = scan.getOpenCounts()[1];
                    final String actual = print(factory, sqlExecutionContext);
                    Assert.assertTrue(query, scan.getOpenCounts()[1] > parallelOpens);
                    TestUtils.assertEquals(query, print(engine, sqlExecutionContext, query.replaceFirst("select ", "select /*+ no_index */ ")), actual);
                }
            }
        });
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
                                            // appends in place to the last partition, which keeps its name txn
                                            engine.execute("insert into t select 'A', 3_000_000 + x, 0.125, (max_ts + x)::timestamp, 'q'"
                                                    + " from long_sequence(300) cross join (select max(ts) max_ts from t)", ctx);
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
    public void testEstimateProbesAtMost64Partitions() throws Exception {
        // An open's estimate reads the posting counts of at most 64 partitions, by stride
        // ceil(P / 64) past that; below 64 partitions (and 1M rows) the table gate skips it.
        // 4 rows an hour: k1, k2, k3 ('r' in the first hour) and NULL, one of each.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(Long.MAX_VALUE, 1, 64);
            final int[] partitions = {63, 64, 65, 100, 1000};
            // stride 1, 1, 2, 2, 16
            final int[] strides = {1, 1, 2, 2, 16};
            final int[] probes = {0, 64, 33, 50, 63};
            for (int i = 0; i < partitions.length; i++) {
                final int p = partitions[i];
                final String table = "t" + p;
                createHourly(engine, sqlExecutionContext, table, p, 4, "case when x % 4 = 0 then null when x = 3 then 'r' else 'k' || (x % 4) end");
                final boolean gated = probes[i] == 0;
                // a symbol in every partition, and NULL, likewise
                for (String key : new String[]{"'k1'", "null"}) {
                    final String query = "select ts, a from " + table + " where s = " + key;
                    try (RecordCursorFactory factory = select(query)) {
                        assertOpen(engine, sqlExecutionContext, factory, query, query, probes[i], gated ? 4L * p : (long) probes[i] * strides[i], p, null);
                    }
                }
                // a symbol in the first partition only: the first frame is always probed
                String query = "select ts, a from " + table + " where s = 'r'";
                try (RecordCursorFactory factory = select(query)) {
                    assertOpen(engine, sqlExecutionContext, factory, query, query, probes[i], gated ? 4L * p : strides[i], 1, false);
                }
                // an absent symbol: no probe
                query = "select ts, a from " + table + " where s = 'nope'";
                try (RecordCursorFactory factory = select(query)) {
                    assertOpen(engine, sqlExecutionContext, factory, query, query, 0, 0, 0, false);
                }
            }
        });
    }

    @Test
    public void testEstimateBoundedInEachContextOnTestContext() throws Exception {
        assertMemoryLeak(() -> assertEstimateBoundedInEachContext(engine, sqlExecutionContext));
    }

    @Test
    public void testEstimateBoundedInEachContextOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> assertEstimateBoundedInEachContext(engine, ctx), configuration, LOG);
        });
    }

    @Test
    public void testEstimateOverIntervalFrames() throws Exception {
        // An interval cursor bounds its frames by the (interval, partition) pairs: the stride
        // comes from that bound. 100 DAY partitions, 400 half-hour windows from 00:00 every 6h;
        // rows sit at minute 5 of each 10, off the windows' edges: 3 rows a window.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(Long.MAX_VALUE, 1, 0);
            final String windows = "ts in '1970-01-01T00:00;30m;6h;400'";
            // a row every 10 minutes all day: 4 frames a day, 400 frames, bound 400, stride 7,
            // probes ceil(400 / 7) = 58 of 3 rows each
            execute("create table full_day (s symbol index type posting, a long, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into full_day select 'k1', x, ((x - 1) * 600_000_000L + 300_000_000L)::timestamp from long_sequence(14_400)");
            String query = "select ts, a from full_day where s = 'k1' and " + windows;
            try (RecordCursorFactory factory = select(query)) {
                assertOpen(engine, sqlExecutionContext, factory, query, query, 58, 58 * 3 * 7, 1200, null);
            }
            // rows only in each day's first hour: the windows at 06, 12 and 18h hold no
            // rows and emit no frame, 100 frames; the bound still counts 397 pairs (the last day's
            // later windows lie past the data), stride 7, probes ceil(100 / 7) = 15
            execute("create table early (s symbol index type posting, a long, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into early select 'k1', x, ((x - 1) / 6 * 86_400_000_000L + (x - 1) % 6 * 600_000_000L + 300_000_000L)::timestamp from long_sequence(600)");
            query = "select ts, a from early where s = 'k1' and " + windows;
            try (RecordCursorFactory factory = select(query)) {
                assertOpen(engine, sqlExecutionContext, factory, query, query, 15, 15 * 3 * 7, 300, null);
            }
        });
    }

    @Test
    public void testEstimateOverParquetPartitions() throws Exception {
        // Parquet frames count in the stride but are not probed: the estimate follows the native
        // partitions only. 1000 hours of 4 rows, the first 900 Parquet: stride 16, frames 912..992.
        assertMemoryLeak(() -> {
            createHourly(engine, sqlExecutionContext, "tp", 1000, 4, "case when x % 4 = 0 then null else 'k' || (x % 4) end");
            execute("alter table tp convert partition to parquet where ts < '1970-02-07T12'");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(Long.MAX_VALUE, 90, 0);
            final String query = "select ts, a from tp where s = 'k1'";
            try (RecordCursorFactory factory = select(query)) {
                assertOpen(engine, sqlExecutionContext, factory, query, query, 6, 6 * 16, 1000, true);
            }
        });
    }

    @Test
    public void testEstimateWithoutProbesResetsTheLastEstimate() throws Exception {
        // One factory, bound to native partitions, then to Parquet ones and past the table: an
        // open that probes nothing reports no rows, not the previous open's estimate.
        assertMemoryLeak(() -> {
            createHourly(engine, sqlExecutionContext, "tq", 100, 4, "case when x % 4 = 0 then null else 'k' || (x % 4) end");
            execute("alter table tq convert partition to parquet where ts < '1970-01-03T02'");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            DeferredSingleSymbolFilterPageFrameRecordCursorFactory.setParallelThresholdsForTesting(Long.MAX_VALUE, 1, 0);
            final String query = "select ts, a from tq where s = 'k1' and ts >= $1 and ts < $2";
            final long hour = 3_600_000_000L;
            // hours [lo, hi), probes, estimate, rows
            final long[][] binds = {
                    {60, 70, 10, 10, 10},
                    {10, 20, 0, 0, 10},
                    {60, 70, 10, 10, 10},
                    {500, 600, 0, 0, 0},
            };
            try (RecordCursorFactory factory = select(query)) {
                for (long[] bind : binds) {
                    bindVariableService.clear();
                    bindVariableService.setTimestamp(0, bind[0] * hour);
                    bindVariableService.setTimestamp(1, bind[1] * hour);
                    assertOpen(engine, sqlExecutionContext, factory, query, query + " " + bind[0] + ".." + bind[1], bind[2], bind[3], (int) bind[4], null);
                }
            }
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

    // 1000 hours of 20 rows: F in 18 of each hour's rows, m in one, k in one of every 100 rows
    // (hours 4, 9, 14, ...); a = x
    private static void assertEstimateBoundedInEachContext(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        createHourly(engine, ctx, "t", 1000, 20, "case when x % 100 = 0 then 'k' when x % 10 = 0 then 'm' else 'F' end");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
        // a bound key, per open, at the default thresholds: stride 16, 63 probes; F reads
        // 63 x 18 x 16 = 18 144 rows over 1 008 partitions (parallel), k the 12 sampled hours
        // that are 4 mod 5, 12 x 16 = 192 rows (serial)
        String query = "select ts, a from t where s = $1";
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            final String[] keys = {"F", "k", "nope", "F"};
            final long[] probes = {63, 63, 0, 63};
            final long[] estimates = {18_144, 192, 0, 18_144};
            final int[] rows = {18_000, 200, 0, 18_000};
            final boolean[] parallel = {true, false, false, true};
            for (int i = 0; i < keys.length; i++) {
                ctx.getBindVariableService().clear();
                ctx.getBindVariableService().setStr(0, keys[i]);
                assertOpen(engine, ctx, factory, query, query + " [" + keys[i] + ']', probes[i], estimates[i], rows[i], parallel[i]);
            }
        }
        // an interval: 672 hours, stride 11, 62 probes, 62 x 18 x 11 = 12 276 rows (serial)
        query = "select ts, a from t where s = 'F' and ts >= '1970-01-02' and ts < '1970-01-30'";
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            assertOpen(engine, ctx, factory, query, query, 62, 12_276, 672 * 18, false);
        }
        // GROUP BY with the index choice and the parallel index GROUP BY off: the scan below it
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_INDEX_GROUPBY_ENABLED, "false");
        try {
            query = "select count(), sum(a) from t where s = 'F'";
            try (RecordCursorFactory factory = engine.select(query, ctx)) {
                final DeferredSingleSymbolFilterPageFrameRecordCursorFactory scan = findScan(factory);
                Assert.assertNotNull(query, scan);
                // x in 1..20 000 not a multiple of 10: 18 000 rows, sum 200 010 000 - 20 010 000
                TestUtils.assertEquals("count\tsum\n18000\t180000000\n", print(factory, ctx));
                Assert.assertEquals(63, scan.getLastEstimateProbes());
                Assert.assertEquals(18_144, scan.getLastEstimateRows());
                Assert.assertArrayEquals(new long[]{0, 1}, scan.getOpenCounts());
            }
        } finally {
            setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, "true");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_INDEX_GROUPBY_ENABLED, "true");
        }
    }

    // opens the factory and checks the estimate's probes and rows, the plan the open chose (when
    // parallel is not null) and the rows against the serial scan with the frame dispatch off
    private static void assertOpen(
            CairoEngine engine,
            SqlExecutionContext ctx,
            RecordCursorFactory factory,
            String query,
            String label,
            long probes,
            long estimate,
            int rows,
            Boolean parallel
    ) throws Exception {
        final DeferredSingleSymbolFilterPageFrameRecordCursorFactory scan = findScan(factory);
        Assert.assertNotNull(label, scan);
        Assert.assertNotNull(label, scan.getParallelScan());
        final long[] before = scan.getOpenCounts();
        final String actual = print(factory, ctx);
        final long[] after = scan.getOpenCounts();
        Assert.assertEquals(label + " probes", probes, scan.getLastEstimateProbes());
        Assert.assertEquals(label + " estimate", estimate, scan.getLastEstimateRows());
        if (parallel != null) {
            Assert.assertEquals(label + " parallel", parallel ? 1 : 0, after[1] - before[1]);
        }
        Assert.assertEquals(label + " opens", 1, after[0] - before[0] + after[1] - before[1]);
        int lines = 0;
        for (int i = 0, n = actual.length(); i < n; i++) {
            if (actual.charAt(i) == '\n') {
                lines++;
            }
        }
        Assert.assertEquals(label + " rows", rows + 1, lines);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
        try {
            TestUtils.assertEquals(label, print(engine, ctx, query), actual);
        } finally {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
        }
    }

    // partitions hours of rowsPerPartition rows each, s from the given expression of x, a = x
    private static void createHourly(CairoEngine engine, SqlExecutionContext ctx, String table, int partitions, int rowsPerPartition, String symbol) throws Exception {
        engine.execute("create table " + table + " (s symbol index type posting, a long, ts timestamp) timestamp(ts) partition by hour bypass wal", ctx);
        engine.execute(
                "insert into " + table + " select " + symbol + ", x, ((x - 1) * " + (3_600_000_000L / rowsPerPartition) + "L)::timestamp from long_sequence(" + (long) partitions * rowsPerPartition + ")",
                ctx
        );
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

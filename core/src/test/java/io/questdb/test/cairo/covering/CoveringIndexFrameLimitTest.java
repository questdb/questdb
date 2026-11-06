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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.LongList;
import io.questdb.std.Rows;
import io.questdb.std.str.StringSink;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * A covering-index scan emits fewer page frames than the row ids of its frames address.
 * {@code PageFrameMemoryRecord.getRowId()} is {@code Rows.toRowID(frameIndex, rowIndex)}, which
 * equals {@code LONG_NULL} at frame index 2^19 and carries the sign bit past it, so a scan of more
 * frames returns the wrong first(), last() and their siblings, silently, while count(), min(),
 * max() and sum() stay right. Every frame mode reaches the limit through its row cap,
 * {@code cairo.sql.parallel.covering.frame.rows}: a single key, the merge of an IN list, and per
 * key. A scan that has emitted an eighth of the limit widens its frames so that the rest of it
 * fits, and one whose partition frames alone fill the limit fails rather than answer wrong rows.
 * <p>
 * The fixture holds 5,243,000 rows, 0.1 s apart, in seven DAY partitions: every 5th row is A,
 * 1,048,600 rows (20%), and every 10th from the first is C, 524,300 rows (10%). A scan of A in
 * frames of 2 rows would emit 524,300 frames, past the limit.
 */
public class CoveringIndexFrameLimitTest extends AbstractCoveringIndexQueryTest {
    // Rows.MAX_SAFE_PARTITION_INDEX: a scan emits fewer frames than this
    private static final int FRAME_LIMIT = Rows.MAX_SAFE_PARTITION_INDEX;
    // CoveringIndexRecordCursorFactory.FRAME_BUDGET_CHECK_FRAMES: a scan widens its frames once it has emitted this many
    private static final int CHECK_FRAMES = FRAME_LIMIT >> 3;
    // CoveringPageFrameCursor.FRAME_BUDGET_SLACK: frames the widened bound reserves for the backward scan's first chunks
    private static final int FRAME_SLACK = 64;
    private static final int PARTITIONS = 7;
    private static final long ROWS = 5_243_000;
    // rows of each DAY partition: x * 100_000 micros, 864,000 rows a day
    private static final long ROWS_PER_DAY = 864_000;

    @After
    public void clearFrameCapOverride() {
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
    }

    @Test
    public void testBackwardScanPastFrameCheck() throws Exception {
        // A negative LIMIT reads the scan's frames last first: 5,000 of A's rows with v % 1000 = 0,
        // one in 200, are the last 1,000,000 rows of A, which frames of 1 row take past the check, and
        // the frames after it widen
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 1);
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", false);
            final String tail = "select /*+ use_index */ ts, v, s from t where sym = 'A' and v % 1000 = 0 limit -5000";
            assertPlanContains(tail, "CoveringIndex");
            assertSameRows(tail);
            final String aggregate = "select count() c, sum(v) s, min(ts) lo, max(ts) hi from (" + tail + ")";
            assertSameRows(aggregate);
            // the frames, from the covering scan itself: backward, at most 1 row before the check
            try (RecordCursorFactory factory = select("select /*+ use_index */ ts, v from t where sym = 'A'")) {
                final LongList frames = frameRows(factory, PartitionFrameCursorFactory.ORDER_DESC);
                assertFramesUnderLimit(frames, 1);
                Assert.assertEquals(countA(), sum(frames));
            }
        });
    }

    @Test
    public void testDefaultFrameRowsKeepTheirFrames() throws Exception {
        // at the default 65,536 rows a scan of A emits 17 frames, far below the check: the frames are
        // the configured ones, and so are the answers
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", false);
            try (RecordCursorFactory factory = select("select /*+ use_index */ ts, v from t where sym = 'A'")) {
                TestUtils.assertEquals(modelFrames(perPartitionA(), 65_536, 65_536).toString(), frameRows(factory, PartitionFrameCursorFactory.ORDER_ASC).toString());
            }
            assertSameRows("select first(v) f, last(v) l, count() c, min(v) mn, max(v) mx, sum(v) s from t where sym = 'A'");
        });
    }

    @Test
    public void testFramesBelowCheckKeepConfiguredRows() throws Exception {
        // 32 rows per frame: A takes 32,771 frames, fewer than the check's 65,535, so every frame
        // holds the configured rows, as before the limit was enforced
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 32);
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", false);
            try (RecordCursorFactory factory = select("select /*+ use_index */ ts, v from t where sym = 'A'")) {
                final LongList expected = modelFrames(perPartitionA(), 32, 32);
                Assert.assertTrue(expected.size() < CHECK_FRAMES);
                TestUtils.assertEquals(expected.toString(), frameRows(factory, PartitionFrameCursorFactory.ORDER_ASC).toString());
            }
        });
    }

    @Test
    public void testFramesPastCheckWidenToFit() throws Exception {
        // 2 rows per frame: the first 65,535 frames keep 2 rows, the rest take the fewest rows that
        // keep the scan below the limit, for the table's 5,243,000 rows over its 7 partitions
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 2);
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", false);
            final int widened = widenedRows(ROWS, PARTITIONS, 1, 2);
            Assert.assertEquals(12, widened);
            final String expectedA = modelFrames(perPartitionA(), 2, widened).toString();
            try (RecordCursorFactory factory = select("select /*+ use_index */ ts, v from t where sym = 'A'")) {
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    final LongList frames = frameRows(cursor);
                    TestUtils.assertEquals(expectedA, frames.toString());
                    assertFramesUnderLimit(frames, 2);
                    // a rewound scan emits its frames again, from the configured rows
                    cursor.toTop();
                    TestUtils.assertEquals(expectedA, frameRows(cursor).toString());
                }
                // and so does the next execution
                TestUtils.assertEquals(expectedA, frameRows(factory, PartitionFrameCursorFactory.ORDER_ASC).toString());
            }
            // an interval scan does not count its rows: the bound takes the table's, the same here
            try (RecordCursorFactory factory = select("select /*+ use_index */ ts, v from t where sym = 'A' and ts in '1970-01'")) {
                TestUtils.assertEquals(expectedA, frameRows(factory, PartitionFrameCursorFactory.ORDER_ASC).toString());
            }
            // the merge of A and C: one frame per partition's rows of both keys
            try (RecordCursorFactory factory = select("select /*+ use_index */ ts, v from t where sym in ('A', 'C')")) {
                final long[] perPartition = perPartitionA();
                final long[] perPartitionC = perPartitionC();
                for (int p = 0; p < PARTITIONS; p++) {
                    perPartition[p] += perPartitionC[p];
                }
                final LongList frames = frameRows(factory, PartitionFrameCursorFactory.ORDER_ASC);
                TestUtils.assertEquals(modelFrames(perPartition, 2, widened).toString(), frames.toString());
                assertFramesUnderLimit(frames, 2);
            }
        });
    }

    @Test
    public void testFramesPastCheckWithEnoughRowsKeepTheirRows() throws Exception {
        // 900,000 rows in two partitions, every 5th A: 180,000 rows of A in frames of 2 rows pass the
        // check, but 2 rows per frame already keep the rest of the scan below the limit, so the frames
        // after the check keep their 2 rows
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 2);
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", false, 900_000);
            Assert.assertEquals(2, widenedRows(900_000, 2, 1, 2));
            try (RecordCursorFactory factory = select("select /*+ use_index */ ts, v from t where sym = 'A'")) {
                final LongList frames = frameRows(factory, PartitionFrameCursorFactory.ORDER_ASC);
                final LongList expected = modelFrames(perPartition(900_000, 2, 5, 0), 2, 2);
                Assert.assertTrue(expected.size() > CHECK_FRAMES);
                TestUtils.assertEquals(expected.toString(), frames.toString());
            }
            assertSameRows("select first(v) f, last(v) l, count() c from t where sym = 'A'");
        });
    }

    @Test
    public void testGroupByAroundTheOldFrameLimit() throws Exception {
        // frames of 2 rows of a single partition's A: 2^19 frames, the last at index 2^19 - 1, and
        // 2^19 + 1 frames, which ended at the LONG_NULL row id before the scan widened its frames
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 2);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                for (long rows : new long[]{5_242_880, 5_242_885}) {
                    final String table = "t" + rows;
                    engine.execute("create table " + table + " (ts timestamp, sym symbol index type posting include (v), v long)"
                            + " timestamp(ts) partition by day bypass wal", ctx);
                    engine.execute("insert into " + table + " select (x * 1000L)::timestamp, case when x % 5 = 0 then 'A' else 'B' || (x % 7) end, x"
                            + " from long_sequence(" + rows + ")", ctx);
                    assertGroupByOfA(engine, compiler, ctx, table, rows);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testGroupByPastFrameLimit() throws Exception {
        // the review's case: frames of 2 rows, a 20% key over 5.2M rows, four workers
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 2);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx, "t", false);
                assertGroupByShapes(engine, compiler, ctx, "t");
            }, configuration, LOG);
        });
    }

    @Test
    public void testGroupByPastFrameLimitWal() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 2);
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t_wal", true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertGroupByOfA(engine, compiler, sqlExecutionContext, "t_wal", ROWS);
            }
        });
    }

    @Test
    public void testGroupByWithCoveringFramesCappedByPageFrameRows() throws Exception {
        // master's way to the limit, which this change keeps closed: cairo.sql.page.frame.max.rows also
        // caps covering frames, and the test hook sets the cap directly. The expected rows are plain
        // Java's, as the table scan the no_index hint gives reads 2-row frames too.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", false);
            final String sql = "select first(v) f, last(v) l, count() c, min(v) mn, max(v) mx, sum(v) s from t where sym = 'A'";
            assertPlanContains(sql, "CoveringIndex");
            assertQuery(sql).noLeakCheck().expectSize().noRandomAccess().returns(expectedGroupByOfA(ROWS));
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(2);
            assertQuery(sql).noLeakCheck().expectSize().noRandomAccess().returns(expectedGroupByOfA(ROWS));
        });
    }

    @Test
    public void testGroupBySettingAtOne() throws Exception {
        // frames of 1 row: 524,289 rows of A
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 1);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx, "t", false, 2_621_445);
                assertGroupByOfA(engine, compiler, ctx, "t", 2_621_445);
            }, configuration, LOG);
        });
    }

    @Test
    public void testPartitionFramesFillingTheLimitFailTheScan() throws Exception {
        // 524,300 one-second windows, each holding one row of A: the scan's partition frames alone
        // exceed the limit, which no frame size can fix, so the scan fails rather than answer from
        // row ids past it
        assertMemoryLeak(() -> {
            execute("create table w (ts timestamp, sym symbol index type posting include (v), v long) timestamp(ts) partition by day bypass wal");
            execute("insert into w select (x * 2_000_000L)::timestamp, 'A', x from long_sequence(524_300)");
            final String windows = "'1970-01-01T00:00:02;1s;2s;524300'";
            final String sql = "select /*+ use_index */ count() c, first(v) f, last(v) l from w where sym = 'A' and ts in " + windows;
            assertPlanContains(sql, "CoveringIndex");
            assertQuery(sql).noLeakCheck().failsWith(
                    "covering index scan exceeds the page frames one query can address [table=w, limit=524287], query fewer partitions or time intervals at a time"
            );
            // the table keeps working, through the covering scan too
            assertQuery("select /*+ use_index */ count() c, first(v) f, last(v) l from w where sym = 'A' and ts in '1970-01-01T00:00:02;1s;2s;1000'")
                    .noLeakCheck().expectSize().noRandomAccess().returns("c\tf\tl\n1000\t1\t1000\n");
            // 460,000 windows leave no room to widen the frames past the check, which they do not
            // need: one frame each, below the limit
            assertQuery("select /*+ use_index */ count() c, first(v) f, last(v) l from w where sym = 'A' and ts in '1970-01-01T00:00:02;1s;2s;460000'")
                    .noLeakCheck().expectSize().noRandomAccess().returns("c\tf\tl\n460000\t1\t460000\n");
        });
    }

    @Test
    public void testPerKeyPastFrameCheck() throws Exception {
        // 11 rows per frame admit per-key mode for A and C (2 keys x 7 partitions + 5,243,000 / 11
        // frames is below the limit); in frames of 11 rows the scan would emit 142,999 frames, past
        // the check, so the frames after it widen to 12 rows
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 11);
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", false);
            Assert.assertEquals(12, widenedRows(ROWS, PARTITIONS, 2, 11));
            final String sql = "select sym, first(v) f, last(v) l, count() c, first(s) fs, last(s) ls from t where sym in ('A', 'C') order by sym";
            assertPlanContains(sql, "per-key");
            assertRunsPerKeyMode(sql);
            assertSameRows(sql);
            assertQuery(sql).noLeakCheck().expectSize().returns(
                    "sym\tf\tl\tc\tfs\tls\n" +
                            "A\t5\t5243000\t1048600\tv5\tv5243000\n" +
                            "C\t1\t5242991\t524300\tv1\tv5242991\n"
            );
        });
    }

    private static void assertFramesUnderLimit(LongList frames, int configuredRows) {
        Assert.assertTrue(frames.size() + " frames", frames.size() < FRAME_LIMIT);
        for (int i = 0, n = Math.min(frames.size(), CHECK_FRAMES); i < n; i++) {
            Assert.assertTrue("frame " + i + ": " + frames.getQuick(i) + " rows", frames.getQuick(i) > 0 && frames.getQuick(i) <= configuredRows);
        }
    }

    private static void assertGroupByOfA(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext ctx, String table, long rows) throws Exception {
        final String sql = "select first(v) f, last(v) l, count() c, min(v) mn, max(v) mx, sum(v) s from " + table + " where sym = 'A'";
        final StringSink plan = new StringSink();
        TestUtils.printSql(compiler, ctx, "explain " + sql, plan);
        TestUtils.assertContains(plan, "CoveringIndex");
        TestUtils.assertContains(plan, "Async Group By");
        // three executions of one factory, and the second pass of each
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            for (int i = 0; i < 3; i++) {
                new io.questdb.test.QueryAssertion(engine, factory).withContext(ctx).expectSize().noRandomAccess().returns(expectedGroupByOfA(rows));
            }
        }
    }

    private static void assertGroupByShapes(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext ctx, String table) throws Exception {
        assertGroupByOfA(engine, compiler, ctx, table, ROWS);
        final String[] queries = {
                // the review's case, varchar too
                "select first(v) f, last(v) l, first(s) fs, last(s) ls, count() c from " + table + " where sym = 'A'",
                // keyed
                "select v % 2 k, first(v) f, last(v) l, count() c from " + table + " where sym = 'A' order by k",
                // OHLC bars
                "select ts, first(v) open, max(v) high, min(v) low, last(v) close, count() n from " + table + " where sym = 'A' sample by 1d",
                "select first_not_null(v) f, last_not_null(v) l from " + table + " where sym = 'A'",
                // a symbol pattern
                "select first(v) f, last(v) l, count() c from " + table + " where sym like 'A%'",
                "with a as (select ts, v from " + table + " where sym = 'A') select first(v) f, last(v) l from a",
                // an interval scan, which bounds its rows by the table's
                "select first(v) f, last(v) l, count() c from " + table + " where sym = 'A' and ts in '1970-01-02;3d'",
                // the merge of an IN list
                "select /*+ use_index */ first(v) f, last(v) l, count() c from " + table + " where sym in ('A', 'C')",
                // grouped by the key: per-key mode is declined at its ceiling and the merge takes over
                "select sym, first(v) f, last(v) l, count() c from " + table + " where sym in ('A', 'C') order by sym",
        };
        for (String q : queries) {
            final StringSink plan = new StringSink();
            TestUtils.printSql(compiler, ctx, "explain " + q, plan);
            TestUtils.assertContains(q, plan, "CoveringIndex");
            TestUtils.assertContains(q, plan, "Async Group By");
            final StringSink expected = new StringSink();
            TestUtils.printSql(compiler, ctx, referenceOf(q), expected);
            final StringSink noIndexPlan = new StringSink();
            TestUtils.printSql(compiler, ctx, "explain " + referenceOf(q), noIndexPlan);
            Assert.assertFalse(q + '\n' + noIndexPlan, noIndexPlan.toString().contains("CoveringIndex"));
            try (RecordCursorFactory factory = compiler.compile(q, ctx).getRecordCursorFactory()) {
                new io.questdb.test.QueryAssertion(engine, factory).withContext(ctx)
                        .sizeMayVary().inferRandomAccess().inferTimestamp().returns(expected);
            }
        }
    }

    private static long countA() {
        long count = 0;
        for (long n : perPartitionA()) {
            count += n;
        }
        return count;
    }

    private static void createTable(CairoEngine engine, SqlExecutionContext ctx, String name, boolean isWal) throws SqlException {
        createTable(engine, ctx, name, isWal, ROWS);
    }

    private static void createTable(CairoEngine engine, SqlExecutionContext ctx, String name, boolean isWal, long rows) throws SqlException {
        engine.execute("create table " + name + " (ts timestamp, sym symbol index type posting include (ts, v, s), v long, s varchar)"
                + " timestamp(ts) partition by day " + (isWal ? "wal" : "bypass wal"), ctx);
        engine.execute("insert into " + name + " select (x * 100_000L)::timestamp,"
                + " case when x % 5 = 0 then 'A' when x % 10 = 1 then 'C' else 'B' || (x % 7) end,"
                + " x, ('v' || x)::varchar from long_sequence(" + rows + ")", ctx);
        if (isWal) {
            TestUtils.drainWalQueue(engine);
        }
    }

    // v = x for x in 1..rows, A where x % 5 = 0
    private static String expectedGroupByOfA(long rows) {
        final long count = rows / 5;
        final long last = 5 * count;
        final long sum = 5 * (count * (count + 1) / 2);
        return "f\tl\tc\tmn\tmx\ts\n5\t" + last + '\t' + count + "\t5\t" + last + '\t' + sum + '\n';
    }

    private static LongList frameRows(RecordCursorFactory factory, int order) throws SqlException {
        try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, order)) {
            return frameRows(cursor);
        }
    }

    private static LongList frameRows(PageFrameCursor cursor) {
        final LongList frames = new LongList();
        PageFrame frame;
        while ((frame = cursor.next()) != null) {
            frames.add(frame.getPartitionHi() - frame.getPartitionLo());
        }
        return frames;
    }

    /**
     * The frames a forward scan emits, from plain Java: each partition's rows in frames of
     * {@code rows}, and once the scan has emitted the check's frames, of {@code widenedRows}.
     */
    private static LongList modelFrames(long[] perPartition, int rows, int widenedRows) {
        final LongList frames = new LongList();
        long cap = rows;
        for (long partitionRows : perPartition) {
            for (long left = partitionRows; left > 0; ) {
                final long frameRows = Math.min(cap, left);
                frames.add(frameRows);
                left -= frameRows;
                if (frames.size() == CHECK_FRAMES) {
                    cap = widenedRows;
                }
            }
        }
        return frames;
    }

    // rows of A in each of the fixture's partitions: x % 5 = 0 for x in the partition's rows
    private static long[] perPartitionA() {
        return perPartition(ROWS, PARTITIONS, 5, 0);
    }

    // rows of C in each of the fixture's partitions: x % 10 = 1
    private static long[] perPartitionC() {
        return perPartition(ROWS, PARTITIONS, 10, 1);
    }

    // rows x of 1..rows with x % modulo == remainder in each DAY partition, x * 100_000 micros apart
    private static long[] perPartition(long rows, int partitions, int modulo, int remainder) {
        final long[] counts = new long[partitions];
        for (int p = 0; p < partitions; p++) {
            final long lo = Math.max(1, p * ROWS_PER_DAY);
            final long hi = Math.min(rows, (p + 1) * ROWS_PER_DAY - 1);
            counts[p] = countCongruent(hi, modulo, remainder) - countCongruent(lo - 1, modulo, remainder);
        }
        return counts;
    }

    // x in 1..n with x % modulo == remainder
    private static long countCongruent(long n, int modulo, int remainder) {
        if (n < 1) {
            return 0;
        }
        final long first = remainder == 0 ? modulo : remainder;
        return n < first ? 0 : (n - first) / modulo + 1;
    }

    // the query through a scan of the table: its use_index hint, or none, becomes no_index
    private static String referenceOf(String query) {
        return query.contains("use_index")
                ? query.replace("use_index", "no_index")
                : query.replaceFirst("select ", "select /*+ no_index */ ");
    }

    private static long sum(LongList frames) {
        long sum = 0;
        for (int i = 0, n = frames.size(); i < n; i++) {
            sum += frames.getQuick(i);
        }
        return sum;
    }

    /**
     * The rows per frame a scan widens to at the check, from the documented bound: the fewest
     * for which the table's rows split into fewer frames than the limit leaves past the frames
     * emitted, the (key, partition) pairs and the slack, and never fewer than configured.
     */
    private static int widenedRows(long tableRows, int partitions, int keys, int configuredRows) {
        final long budget = FRAME_LIMIT - CHECK_FRAMES - FRAME_SLACK - (long) keys * partitions;
        return (int) Math.max(configuredRows, tableRows / budget + 1);
    }

    private void assertPlanContains(String query, String fragment) throws Exception {
        final StringSink plan = new StringSink();
        printSql("explain " + query, plan);
        TestUtils.assertContains(query + '\n' + plan, plan, fragment);
    }

    private void assertSameRows(String query) throws Exception {
        final String reference = referenceOf(query);
        final StringSink plan = new StringSink();
        printSql("explain " + reference, plan);
        Assert.assertFalse(reference + '\n' + plan, plan.toString().contains("CoveringIndex"));
        assertSameResult(query, reference);
    }
}

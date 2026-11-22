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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.PartitionFrameState;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.BwdTableReaderPageFrameCursor;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.ExtraNullColumnCursorFactory;
import io.questdb.griffin.engine.table.FwdTableReaderPageFrameCursor;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 * PageFrameCursor#rampUpFrameSizes() makes the native page frames of a table scan start at the
 * given row count, and every frame after the first hold at most the rows of the frames before it
 * divided by the shared query worker count, until frames reach their regular size. Each expected
 * frame follows by hand from that rule: a frame prints as partitionIndex:lo-hi, with a P when it
 * is a row group of a Parquet partition, and a D when it materializes a window of a Delta
 * partition that holds late rows.
 * <p>
 * Most tables hold 20 rows a day, one row every 72 minutes, and with cairo.sql.page.frame.min.rows
 * of 1 and cairo.sql.page.frame.max.rows of 20, a regular frame holds a whole day. The test
 * execution context runs one shared query worker.
 * <p>
 * Only Enterprise writes Delta partitions, so a Delta partition here is a real native partition
 * that a synthetic partition frame state splits into windows, see allocDeltaState().
 */
public class PageFrameRampUpTest extends AbstractCairoTest {
    private static final String DAILY_ROWS = "SELECT x, timestamp_sequence('2024-01-01'::TIMESTAMP, 4_320_000_000) ts FROM long_sequence(";
    // the rows of the sub-frames that materialize a Delta window with late rows
    private static final long DELTA_SUBFRAME_ROWS = 8;

    @Test
    public void testBackwardFramesRampUpToRegularSize() throws Exception {
        setPageFrameRows(20);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" + DAILY_ROWS + "40)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try (RecordCursorFactory factory = select("SELECT * FROM t")) {
                // the last day comes first, and its frames come from its last row down
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_DESC, 2, "1:18-20 1:16-18 1:12-16 1:4-12 1:0-4 0:0-20");
                // the factory opens the same cursor again, which drops the ramp-up
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_DESC, 0, "1:0-20 0:0-20");
            }
        });
    }

    @Test
    public void testCursorsThatCannotSplitFramesIgnoreRampUp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (payload), payload LONG)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t SELECT '2024-01-01'::TIMESTAMP + x * 1_000_000L, 'A', x FROM long_sequence(40)");
            // the covering index serves the 40 rows of the key from frames of its own
            try (RecordCursorFactory factory = select("SELECT sym, payload FROM t WHERE sym = 'A'")) {
                Assert.assertTrue(factory.getBaseFactory() instanceof CoveringIndexRecordCursorFactory);
                final String frames;
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    frames = frames(cursor);
                    cursor.toTop();
                    Assert.assertEquals(40, rowCount(cursor));
                }
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 2, frames);
            }
        });
    }

    @Test
    public void testDeltaFramesRampUp() throws Exception {
        setPageFrameRows(20);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" + DAILY_ROWS + "60)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // the first day in scan order is a Delta partition of one window that late writes did not
            // touch: its frames ramp up as a native day's do, and the days after it take their regular frame
            final long[] windows = {20, 0};
            assertDeltaFrames(PartitionFrameCursorFactory.ORDER_ASC, 0, windows, 2, "0:0-2 0:2-4 0:4-8 0:8-16 0:16-20 1:0-20 2:0-20");
            assertDeltaFrames(PartitionFrameCursorFactory.ORDER_DESC, 2, windows, 2, "2:18-20 2:16-18 2:12-16 2:4-12 2:0-4 1:0-20 0:0-20");
            // without the ramp-up, the Delta partition takes its regular frame
            assertDeltaFrames(PartitionFrameCursorFactory.ORDER_ASC, 0, windows, 0, "0:0-20 1:0-20 2:0-20");
            assertDeltaFrames(PartitionFrameCursorFactory.ORDER_DESC, 2, windows, 0, "2:0-20 1:0-20 0:0-20");
        });
    }

    @Test
    public void testDeltaFramesRampUpAroundWindowsWithLateRows() throws Exception {
        setPageFrameRows(20);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" + DAILY_ROWS + "60)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // the Delta partition splits its day into a window of 12 rows and a window of 8 rows that late
            // writes grow to 10: the first window ramps up, the second keeps its sub-frames of 8 rows, and
            // the rows of both count among the rows before
            final long[] windows = {12, 0, 8, 2};
            assertDeltaFrames(
                    PartitionFrameCursorFactory.ORDER_ASC,
                    0,
                    windows,
                    2,
                    "0:0-2 0:2-4 0:4-8 0:8-12 0:12-20D 0:20-22D 1:0-20 2:0-20"
            );
            // backward, the window with late rows comes first, and its 10 rows size the next frame
            assertDeltaFrames(
                    PartitionFrameCursorFactory.ORDER_DESC,
                    2,
                    windows,
                    2,
                    "2:20-22D 2:12-20D 2:2-12 2:0-2 1:0-20 0:0-20"
            );
        });
    }

    @Test
    public void testDeltaFramesRampUpSplitsAtColumnTops() throws Exception {
        setPageFrameRows(60);
        assertMemoryLeak(() -> {
            // y has no value in the first 20 rows of the day
            execute("CREATE TABLE t AS (SELECT x, timestamp_sequence('2024-01-01'::TIMESTAMP, 1_000_000) ts FROM long_sequence(20))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE t ADD COLUMN y INT");
            execute("INSERT INTO t SELECT x + 20, timestamp_sequence('2024-01-01T00:00:20'::TIMESTAMP, 1_000_000), x::INT FROM long_sequence(40)");
            // the day is a Delta partition of one window that late writes did not touch; the column top
            // at row 20 cuts the fifth frame, which would hold rows 16 to 31, and the frames after it
            // grow from the 20 rows that the frames before it hold
            final long[] windows = {60, 0};
            assertDeltaFrames(
                    PartitionFrameCursorFactory.ORDER_ASC,
                    0,
                    windows,
                    2,
                    "0:0-2 0:2-4 0:4-8 0:8-16 0:16-20 0:20-40 0:40-60"
            );
            assertDeltaFrames(
                    PartitionFrameCursorFactory.ORDER_DESC,
                    0,
                    windows,
                    2,
                    "0:58-60 0:56-58 0:52-56 0:44-52 0:28-44 0:20-28 0:0-20"
            );
        });
    }

    @Test
    public void testForwardFramesRampUpToRegularSize() throws Exception {
        setPageFrameRows(20);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" + DAILY_ROWS + "40)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try (RecordCursorFactory factory = select("SELECT * FROM t")) {
                // 2 rows, 2 rows, then as many rows as the frames before hold, until the day ends; the
                // second day starts after 20 rows and takes its regular frame
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 2, "0:0-2 0:2-4 0:4-8 0:8-16 0:16-20 1:0-20");
                // the factory opens the same cursor again, which drops the ramp-up
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 0, "0:0-20 1:0-20");
                // a first frame of a single row
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 1, "0:0-1 0:1-2 0:2-4 0:4-8 0:8-16 0:16-20 1:0-20");
            }
        });
    }

    @Test
    public void testFramesShareRampUpAmongWorkers() throws Exception {
        setPageFrameRows(1_000);
        assertMemoryLeak(() -> {
            // one day of 40 rows: 4 workers split it into regular frames of 10 rows
            execute("CREATE TABLE t AS (SELECT x, timestamp_sequence('2024-01-01'::TIMESTAMP, 1_000_000) ts FROM long_sequence(40))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try (
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 4);
                    RecordCursorFactory factory = engine.select("SELECT * FROM t", context)
            ) {
                // a frame holds at most a quarter of the rows before it, and no fewer than 2 rows, so
                // the frames grow by a quarter and never reach the regular 10 rows
                assertFrames(
                        factory,
                        context,
                        PartitionFrameCursorFactory.ORDER_ASC,
                        2,
                        "0:0-2 0:2-4 0:4-6 0:6-8 0:8-10 0:10-12 0:12-15 0:15-18 0:18-22 0:22-27 0:27-33 0:33-40"
                );
                assertFrames(
                        factory,
                        context,
                        PartitionFrameCursorFactory.ORDER_DESC,
                        2,
                        "0:38-40 0:36-38 0:34-36 0:32-34 0:30-32 0:28-30 0:25-28 0:22-25 0:18-22 0:13-18 0:7-13 0:0-7"
                );
                assertFrames(factory, context, PartitionFrameCursorFactory.ORDER_ASC, 0, "0:0-10 0:10-20 0:20-30 0:30-40");
            }
        });
    }

    @Test
    public void testRampUpBeyondRegularFrameSize() throws Exception {
        setPageFrameRows(20);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" + DAILY_ROWS + "40)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try (RecordCursorFactory factory = select("SELECT * FROM t")) {
                // a first frame larger than the regular one changes no frame
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 50, "0:0-20 1:0-20");
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_DESC, 20, "1:0-20 0:0-20");
            }
        });
    }

    @Test
    public void testRampUpCountsRowsOfDeltaWindowsWithLateRows() throws Exception {
        setPageFrameRows(20);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" + DAILY_ROWS + "60)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // late writes grow the one window of the Delta partition from 20 rows to 30: the window keeps
            // its sub-frames of 8 rows, and its 30 rows let the next day take its regular frame at once
            final long[] windows = {20, 10};
            assertDeltaFrames(
                    PartitionFrameCursorFactory.ORDER_ASC,
                    0,
                    windows,
                    2,
                    "0:0-8D 0:8-16D 0:16-24D 0:24-30D 1:0-20 2:0-20"
            );
            assertDeltaFrames(
                    PartitionFrameCursorFactory.ORDER_DESC,
                    2,
                    windows,
                    2,
                    "2:24-30D 2:16-24D 2:8-16D 2:0-8D 1:0-20 0:0-20"
            );
        });
    }

    @Test
    public void testRampUpCountsRowsOfParquetPartitions() throws Exception {
        setPageFrameRows(20);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" + DAILY_ROWS + "80)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-03'");
            try (RecordCursorFactory factory = select("SELECT * FROM t")) {
                // the Parquet day keeps its row group, and its 20 rows let the next day take its regular
                // frame at once
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 2, "0:0-20P 1:0-20 2:0-20P 3:0-20");
            }
            try (RecordCursorFactory factory = select("SELECT * FROM t WHERE ts < '2024-01-04'")) {
                // backward, the Parquet third day comes before the native second day
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_DESC, 2, "2:0-20P 1:0-20 0:0-20P");
            }
        });
    }

    @Test
    public void testRampUpSplitsAtColumnTops() throws Exception {
        setPageFrameRows(40);
        assertMemoryLeak(() -> {
            // y has no value in the first 20 rows of the day
            execute("CREATE TABLE t AS (SELECT x, timestamp_sequence('2024-01-01'::TIMESTAMP, 1_000_000) ts FROM long_sequence(20))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE t ADD COLUMN y INT");
            execute("INSERT INTO t SELECT x + 20, timestamp_sequence('2024-01-01T00:00:20'::TIMESTAMP, 1_000_000), x::INT FROM long_sequence(20)");
            try (RecordCursorFactory factory = select("SELECT * FROM t")) {
                // the column top at row 20 cuts the fifth frame, which would hold rows 16 to 31
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 2, "0:0-2 0:2-4 0:4-8 0:8-16 0:16-20 0:20-40");
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_DESC, 2, "0:38-40 0:36-38 0:32-36 0:24-32 0:20-24 0:0-20");
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 0, "0:0-20 0:20-40");
            }
        });
    }

    @Test
    public void testProjectionCursorsPassRampUpOn() throws Exception {
        setPageFrameRows(20);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" + DAILY_ROWS + "40)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // the factories of select() run under a QueryProgress, whose cursor passes the call on, as
            // the cursor of a projection that repeats a column does
            try (RecordCursorFactory factory = select("SELECT x, x x2 FROM t")) {
                Assert.assertTrue(factory instanceof QueryProgress);
                Assert.assertTrue(factory.getBaseFactory() instanceof SelectedRecordCursorFactory);
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 2, "0:0-2 0:2-4 0:4-8 0:8-16 0:16-20 1:0-20");
            }
            // the NULL padding of a WINDOW JOIN that no row can match passes the call on too
            try (RecordCursorFactory outer = select("SELECT * FROM t")) {
                final RecordCursorFactory base = outer.getBaseFactory();
                final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(base.getMetadata());
                metadata.add(new TableColumnMetadata("pad", ColumnType.LONG));
                // QueryProgress owns the base, so the padding factory, which would close it again, stays open
                final RecordCursorFactory padded = new ExtraNullColumnCursorFactory(metadata, base.getMetadata().getColumnCount(), base);
                assertFrames(padded, PartitionFrameCursorFactory.ORDER_DESC, 2, "1:18-20 1:16-18 1:12-16 1:4-12 1:0-4 0:0-20");
            }
        });
    }

    @Test
    public void testRampUpWithinInterval() throws Exception {
        setPageFrameRows(20);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" + DAILY_ROWS + "40)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // rows 5 to 14 of the first day, whose regular frame holds all 10 of them
            try (RecordCursorFactory factory = select("SELECT * FROM t WHERE ts >= '2024-01-01T06:00' AND ts < '2024-01-01T18:00'")) {
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 2, "0:5-7 0:7-9 0:9-13 0:13-15");
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_DESC, 2, "0:13-15 0:11-13 0:7-11 0:5-7");
                assertFrames(factory, PartitionFrameCursorFactory.ORDER_ASC, 0, "0:5-15");
            }
        });
    }

    @Test
    public void testToTopRestartsRampUp() throws Exception {
        setPageFrameRows(20);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" + DAILY_ROWS + "40)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try (RecordCursorFactory factory = select("SELECT * FROM t")) {
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    cursor.rampUpFrameSizes(2);
                    Assert.assertEquals("0:0-2 0:2-4 0:4-8 0:8-16 0:16-20 1:0-20", frames(cursor));
                    cursor.toTop();
                    Assert.assertEquals("0:0-2 0:2-4 0:4-8 0:8-16 0:16-20 1:0-20", frames(cursor));
                }
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_DESC)) {
                    cursor.rampUpFrameSizes(2);
                    Assert.assertEquals("1:18-20 1:16-18 1:12-16 1:4-12 1:0-4 0:0-20", frames(cursor));
                    cursor.toTop();
                    Assert.assertEquals("1:18-20 1:16-18 1:12-16 1:4-12 1:0-4 0:0-20", frames(cursor));
                }
            }
        });
    }

    /**
     * Allocates a Delta partition state that splits a native partition into windows, given in pairs:
     * the rows of the partition that a window holds, and the rows that late writes add to it. A window
     * with late rows materializes in sub-frames of DELTA_SUBFRAME_ROWS rows. The state lays out the
     * header and the window summaries that PartitionFrameState reads, and the caller frees it.
     */
    private static long allocDeltaState(long[] windows) {
        final int windowCount = windows.length / 2;
        final long state = Unsafe.calloc(deltaStateSize(windows), MemoryTag.NATIVE_DEFAULT);
        final long summaries = state + PartitionFrameState.HEADER_BYTES;
        long baseRowCount = 0;
        long logicalRowCount = 0;
        for (int i = 0; i < windowCount; i++) {
            final long baseRows = windows[2 * i];
            final long lateRows = windows[2 * i + 1];
            final long summary = summaries + (long) i * PartitionFrameState.SUMMARY_LONGS * Long.BYTES;
            Unsafe.putLong(summary + PartitionFrameState.SUMMARY_BASE_ROWS * Long.BYTES, baseRows);
            Unsafe.putLong(summary + PartitionFrameState.SUMMARY_LOGICAL_ROWS * Long.BYTES, baseRows + lateRows);
            Unsafe.putLong(summary + PartitionFrameState.SUMMARY_FLAGS * Long.BYTES, lateRows > 0 ? PartitionFrameState.WINDOW_HAS_DELTA : 0);
            baseRowCount += baseRows;
            logicalRowCount += baseRows + lateRows;
        }
        // the header: summaries address, window count, logical rows, custom frames, sub-frame rows, base rows
        Unsafe.putLong(state, summaries);
        Unsafe.putLong(state + Long.BYTES, windowCount);
        Unsafe.putLong(state + 2 * Long.BYTES, logicalRowCount);
        Unsafe.putLong(state + 3 * Long.BYTES, 1);
        Unsafe.putLong(state + 4 * Long.BYTES, DELTA_SUBFRAME_ROWS);
        Unsafe.putLong(state + 5 * Long.BYTES, baseRowCount);
        return state;
    }

    private static long deltaStateSize(long[] windows) {
        return PartitionFrameState.HEADER_BYTES + (long) (windows.length / 2) * PartitionFrameState.SUMMARY_LONGS * Long.BYTES;
    }

    private static String frames(PageFrameCursor cursor) {
        final StringSink sink = new StringSink();
        PageFrame frame;
        while ((frame = cursor.next()) != null) {
            if (!sink.isEmpty()) {
                sink.put(' ');
            }
            sink.put(frame.getPartitionIndex()).put(':').put(frame.getPartitionLo()).put('-').put(frame.getPartitionHi());
            if (frame.getFormat() == PartitionFormat.PARQUET) {
                sink.put('P');
            }
            if (frame.getPartitionFrameState() != 0) {
                sink.put('D');
            }
        }
        return sink.toString();
    }

    // a table scan of all columns, which shares its frames among one worker, as the test execution context does
    private static TablePageFrameCursor newPageFrameCursor(TableReader reader, int order) {
        final IntList columnIndexes = new IntList();
        final IntList columnSizeShifts = new IntList();
        for (int i = 0, n = reader.getMetadata().getColumnCount(); i < n; i++) {
            columnIndexes.add(i);
            columnSizeShifts.add(Numbers.msb(ColumnType.sizeOf(reader.getMetadata().getColumnType(i))));
        }
        return order == PartitionFrameCursorFactory.ORDER_ASC
                ? new FwdTableReaderPageFrameCursor(columnIndexes, columnSizeShifts, null, 1)
                : new BwdTableReaderPageFrameCursor(columnIndexes, columnSizeShifts, null, 1);
    }

    private static long rowCount(PageFrameCursor cursor) {
        long rowCount = 0;
        PageFrame frame;
        while ((frame = cursor.next()) != null) {
            rowCount += frame.getPartitionHi() - frame.getPartitionLo();
        }
        return rowCount;
    }

    private static void setPageFrameRows(int maxRows) {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, maxRows);
    }

    /**
     * Walks the frames of table t from the top twice, with partition deltaPartitionIndex a Delta
     * partition of the given windows, see allocDeltaState(). The frames ramp up from
     * firstFrameRowCount rows, or do not ramp up when it is 0.
     */
    private void assertDeltaFrames(
            int order,
            int deltaPartitionIndex,
            long[] windows,
            int firstFrameRowCount,
            String expected
    ) throws Exception {
        final long state = allocDeltaState(windows);
        try (
                TableReader reader = engine.getReader("t");
                TablePageFrameCursor cursor = newPageFrameCursor(reader, order)
        ) {
            final boolean isForward = order == PartitionFrameCursorFactory.ORDER_ASC;
            cursor.of(sqlExecutionContext, new DeltaPartitionFrameCursor(reader, isForward, deltaPartitionIndex, state));
            if (firstFrameRowCount > 0) {
                cursor.rampUpFrameSizes(firstFrameRowCount);
            }
            Assert.assertEquals(expected, frames(cursor));
            // the frames ramp up again from the top
            cursor.toTop();
            Assert.assertEquals(expected, frames(cursor));
        } finally {
            Unsafe.free(state, deltaStateSize(windows), MemoryTag.NATIVE_DEFAULT);
        }
    }

    // walks the frames of a cursor that ramps up from firstFrameRowCount rows, or that does not ramp up when it is 0
    private void assertFrames(RecordCursorFactory factory, int order, int firstFrameRowCount, String expected) throws Exception {
        assertFrames(factory, sqlExecutionContext, order, firstFrameRowCount, expected);
    }

    private void assertFrames(
            RecordCursorFactory factory,
            SqlExecutionContext context,
            int order,
            int firstFrameRowCount,
            String expected
    ) throws Exception {
        try (PageFrameCursor cursor = factory.getPageFrameCursor(context, order)) {
            if (firstFrameRowCount > 0) {
                cursor.rampUpFrameSizes(firstFrameRowCount);
            }
            Assert.assertEquals(expected, frames(cursor));
        }
    }

    // Hands out every partition of a table in scan order as one frame over all its rows, and partition
    // deltaPartitionIndex with the given Delta partition state.
    private static class DeltaPartitionFrameCursor implements PartitionFrameCursor, PartitionFrame {
        private final int deltaPartitionIndex;
        private final long deltaState;
        private final boolean isForward;
        private final TableReader reader;
        private int partitionIndex;
        private long rowHi;

        private DeltaPartitionFrameCursor(TableReader reader, boolean isForward, int deltaPartitionIndex, long deltaState) {
            this.reader = reader;
            this.isForward = isForward;
            this.deltaPartitionIndex = deltaPartitionIndex;
            this.deltaState = deltaState;
            toTop();
        }

        @Override
        public void close() {
            // the test owns the reader
        }

        @Override
        public byte getPartitionFormat() {
            return reader.getPartitionFormat(partitionIndex);
        }

        @Override
        public long getPartitionFrameState() {
            return partitionIndex == deltaPartitionIndex ? deltaState : 0;
        }

        @Override
        public int getPartitionIndex() {
            return partitionIndex;
        }

        @Override
        public long getRowHi() {
            return rowHi;
        }

        @Override
        public long getRowLo() {
            return 0;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return reader.getSymbolTable(columnIndex);
        }

        @Override
        public TableReader getTableReader() {
            return reader;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return reader.newSymbolTable(columnIndex);
        }

        @Override
        public @Nullable PartitionFrame next(long skipTarget) {
            partitionIndex += isForward ? 1 : -1;
            if (partitionIndex < 0 || partitionIndex >= reader.getPartitionCount()) {
                return null;
            }
            final long partitionRowCount = reader.openPartition(partitionIndex);
            if (partitionIndex == deltaPartitionIndex) {
                // the windows of the state must cover the rows of the partition they split
                Assert.assertEquals(partitionRowCount, PartitionFrameState.getBasePartitionRowCount(deltaState));
                rowHi = PartitionFrameState.getLogicalPartitionRowCount(deltaState);
            } else {
                rowHi = partitionRowCount;
            }
            return this;
        }

        @Override
        public boolean reload() {
            return false;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            partitionIndex = isForward ? -1 : reader.getPartitionCount();
        }
    }
}

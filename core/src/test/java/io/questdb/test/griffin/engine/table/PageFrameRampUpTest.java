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
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.ExtraNullColumnCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * PageFrameCursor#rampUpFrameSizes() makes the native page frames of a table scan start at the
 * given row count, and every frame after the first hold at most the rows of the frames before it
 * divided by the shared query worker count, until frames reach their regular size. Each expected
 * frame follows by hand from that rule: a frame prints as partitionIndex:lo-hi, with a P when it
 * is a row group of a Parquet partition.
 * <p>
 * Most tables hold 20 rows a day, one row every 72 minutes, and with cairo.sql.page.frame.min.rows
 * of 1 and cairo.sql.page.frame.max.rows of 20, a regular frame holds a whole day. The test
 * execution context runs one shared query worker.
 */
public class PageFrameRampUpTest extends AbstractCairoTest {
    private static final String DAILY_ROWS = "SELECT x, timestamp_sequence('2024-01-01'::TIMESTAMP, 4_320_000_000) ts FROM long_sequence(";

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
        }
        return sink.toString();
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
}

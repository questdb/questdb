/*******************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Rows;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * A parquet row group larger than cairo.sql.page.frame.max.rows must be split into several
 * bounded sub-frames, each anchored to its row group, matching the native partition path. Also
 * covers the frame-count ceiling guard that fails loudly instead of overflowing the rowId.
 * <p>
 * The 25000 data rows live in a non-active partition (2024-01-01) so it can be converted to
 * parquet; a sentinel row in 2024-01-02 keeps it non-active.
 */
public class ParquetSubFrameTest extends AbstractCairoTest {

    @Test
    public void testFrameCountCeilingGuardThrows() throws Exception {
        // One row per frame on a native table, just past the rowId frame ceiling.
        final long rows = Rows.MAX_SAFE_PARTITION_INDEX + 5L;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01', 1) FROM long_sequence(" + rows + ")");
            try (RecordCursorFactory factory = select("x")) {
                sqlExecutionContext.changePageFrameSizes(1, 1);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                        // drain; the guard must trip before the cursor exhausts
                    }
                    Assert.fail("expected a too-many-frames CairoException");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "too many page frames");
                }
            }
        });
    }

    @Test
    public void testPageFrameRowLimitNeverExceedsMax() throws Exception {
        // The tiny-trailing-frame adjustment in calculatePageFrameRowLimit must never push a frame past
        // page.frame.max.rows: a frame larger than Map.BATCH_ROW_INDEX_MASK + 1 rows overflows the 24-bit
        // frame-relative row index packed into every batched GROUP BY entry (silent corruption). 1050 rows
        // with max=1000/min=100 hits the adjustment (1050 % 1000 = 50 < 100), which without the clamp
        // returns 1050 and emits a single 1050-row frame. Native path; calculatePageFrameRowLimit is shared.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01', 1) FROM long_sequence(1050)");
            try (RecordCursorFactory factory = select("x")) {
                sqlExecutionContext.changePageFrameSizes(100, 1000);
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                    long total = 0;
                    int frameCount = 0;
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        final long size = frame.getPartitionHi() - frame.getPartitionLo();
                        Assert.assertTrue("frame exceeds max rows: " + size, size <= 1000);
                        total += size;
                        frameCount++;
                    }
                    Assert.assertEquals(1050, total);
                    // 1000 + 50, so two frames, not one oversized frame
                    Assert.assertEquals(2, frameCount);
                }
            }
        });
    }

    @Test
    public void testRowGroupSplitsIntoBoundedSubFramesBackward() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10_000);
        assertMemoryLeak(() -> {
            buildParquetTable();
            try (RecordCursorFactory factory = select("x")) {
                sqlExecutionContext.changePageFrameSizes(100, 1000);
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_DESC)) {
                    int frameCount = 0;
                    long expectedHi = 25_000;
                    boolean inParquet = false;
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        if (frame.getParquetRowGroup() < 0) {
                            if (inParquet) {
                                break;
                            }
                            continue; // leading native sentinel partition
                        }
                        inParquet = true;
                        final long lo = frame.getPartitionLo();
                        final long hi = frame.getPartitionHi();
                        Assert.assertTrue("frame exceeds max rows: " + (hi - lo), hi - lo <= 1000);
                        Assert.assertEquals(expectedHi, hi);
                        expectedHi = lo;
                        frameCount++;
                    }
                    Assert.assertEquals(0, expectedHi);
                    Assert.assertEquals(25, frameCount);
                }
            }
        });
    }

    @Test
    public void testRowGroupSplitsIntoBoundedSubFramesForward() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10_000);
        assertMemoryLeak(() -> {
            buildParquetTable();
            try (RecordCursorFactory factory = select("x")) {
                sqlExecutionContext.changePageFrameSizes(100, 1000);
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                    int frameCount = 0;
                    long expectedLo = 0;
                    int prevRowGroup = -1;
                    int distinctRowGroups = 0;
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        final int rg = frame.getParquetRowGroup();
                        if (rg < 0) {
                            break; // trailing native sentinel partition
                        }
                        final long lo = frame.getPartitionLo();
                        final long hi = frame.getPartitionHi();
                        Assert.assertTrue("frame exceeds max rows: " + (hi - lo), hi - lo <= 1000);
                        Assert.assertEquals(expectedLo, lo);
                        if (rg != prevRowGroup) {
                            distinctRowGroups++;
                            prevRowGroup = rg;
                        }
                        expectedLo = hi;
                        frameCount++;
                    }
                    Assert.assertEquals(25_000, expectedLo);
                    // 3 row groups (10000, 10000, 5000) each split into 1000-row sub-frames
                    Assert.assertEquals(25, frameCount);
                    Assert.assertEquals(3, distinctRowGroups);
                }
            }
        });
    }

    @Test
    public void testSubFrameFullScanRoundTrip() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x SELECT x, timestamp_sequence('2024-01-01', 1000) FROM long_sequence(25_000)");
            execute("INSERT INTO x VALUES (-1, '2024-01-02T00:00:00.000000Z')");

            // capture native (oracle) output before converting to parquet
            final StringSink forwardOracle = new StringSink();
            final StringSink backwardOracle = new StringSink();
            dumpSplitScan(forwardOracle, "SELECT v, ts FROM x WHERE ts < '2024-01-02'");
            dumpSplitScan(backwardOracle, "SELECT v, ts FROM x WHERE ts < '2024-01-02' ORDER BY ts DESC");

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");

            // parquet sub-frame scans must reproduce the oracle row-for-row
            final StringSink forwardActual = new StringSink();
            final StringSink backwardActual = new StringSink();
            dumpSplitScan(forwardActual, "SELECT v, ts FROM x WHERE ts < '2024-01-02'");
            dumpSplitScan(backwardActual, "SELECT v, ts FROM x WHERE ts < '2024-01-02' ORDER BY ts DESC");
            TestUtils.assertEquals(forwardOracle, forwardActual);
            TestUtils.assertEquals(backwardOracle, backwardActual);

            // random access (recordAt) over the split frames must be stable
            try (RecordCursorFactory factory = select("SELECT v FROM x WHERE ts < '2024-01-02'")) {
                sqlExecutionContext.changePageFrameSizes(100, 1000);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    Assert.assertTrue(cursor.hasNext());
                    final long firstV = record.getLong(0);
                    final long firstRowId = record.getRowId();
                    long lastV = firstV;
                    long lastRowId = firstRowId;
                    while (cursor.hasNext()) {
                        lastV = record.getLong(0);
                        lastRowId = record.getRowId();
                    }
                    Assert.assertEquals(1L, firstV);
                    Assert.assertEquals(25_000L, lastV);
                    final Record recordB = cursor.getRecordB();
                    cursor.recordAt(recordB, firstRowId);
                    Assert.assertEquals(1L, recordB.getLong(0));
                    cursor.recordAt(recordB, lastRowId);
                    Assert.assertEquals(25_000L, recordB.getLong(0));
                }
            }
        });
    }

    private void buildParquetTable() throws Exception {
        execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO x SELECT x, timestamp_sequence('2024-01-01', 1000) FROM long_sequence(25_000)");
        execute("INSERT INTO x VALUES (-1, '2024-01-02T00:00:00.000000Z')");
        execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    private void dumpSplitScan(StringSink sink, String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            sqlExecutionContext.changePageFrameSizes(100, 1000);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    sink.put(record.getLong(0)).put(',').put(record.getLong(1)).put('\n');
                }
            }
        }
    }
}

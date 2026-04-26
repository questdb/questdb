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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.wal.ColumnarRowAppender;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.qwp.protocol.QwpArrayColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpBooleanColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpDecimalColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpGeoHashColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpNullBitmap;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpSymbolColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cutlass.qwp.QwpNullBitmapTestUtil;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * Test suite for {@link ColumnarRowAppender} interface and
 * {@link io.questdb.cairo.wal.WalColumnarRowAppender} implementation.
 * <p>
 * Tests bulk column-oriented writes to WAL, verifying the fast path (direct memcpy)
 * and slow path (null expansion) for various column types.
 */
public class WalColumnarRowAppenderTest extends AbstractCairoTest {
    @Test
    public void testBeginColumnarWrite_AlreadyInProgress() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_lifecycle_double", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(10);

                try {
                    appender.beginColumnarWrite(10);
                    Assert.fail("Expected CairoException");
                } catch (CairoException e) {
                    assertTrue(e.getMessage().contains("already in columnar write mode"));
                }

                // Cleanup
                appender.cancelColumnarWrite();
            }
        });
    }

    /**
     * Tests that beginColumnarWrite() properly handles segment rolling when columns
     * are added while rollSegmentOnNextRow is true.
     * <p>
     * This covers a bug where addColumn() would defer opening column files when
     * rollSegmentOnNextRow was true, but the columnar write path didn't trigger
     * segment rolling, causing writes to fail with AssertionError in TableUtils.mapRW().
     * <p>
     * The test uses a very low segment rollover row count (1) to trigger
     * rollSegmentOnNextRow=true after the first commit.
     */
    @Test
    public void testBeginColumnarWrite_RollsSegmentWhenPending() throws Exception {
        // Set very low segment rollover threshold to trigger rollSegmentOnNextRow
        node1.setProperty(io.questdb.PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 1);

        assertMemoryLeak(() -> {
            // Create table with initial column
            TableToken tableToken = createTable(new TableModel(configuration, "test_segment_roll", PartitionBy.HOUR)
                    .col("col_a", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 10;
            long baseTimestamp = 1_000_000_000L;

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                // Write some data and commit - this will set rollSegmentOnNextRow=true
                // because segmentRowCount (1) >= walSegmentRolloverRowCount (1)
                TableWriter.Row row = walWriter.newRow(baseTimestamp);
                row.putLong(0, 100L);
                row.append();
                walWriter.commit();

                // Add a new column while rollSegmentOnNextRow is true
                // This will configure the column memory but NOT open the file
                // (because openColumnFiles is skipped when rollSegmentOnNextRow is true)
                walWriter.addColumn("col_b", ColumnType.LONG, AllowAllSecurityContext.INSTANCE);

                // Now try columnar write - this should trigger segment roll in beginColumnarWrite()
                // Before the fix, this would fail with AssertionError in TableUtils.mapRW()
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                long valuesAddrA = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
                long valuesAddrB = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putLong(valuesAddrA + (long) i * 8, 1000L + i);
                        Unsafe.putLong(valuesAddrB + (long) i * 8, 2000L + i);
                    }

                    long[] timestamps = new long[rowCount];
                    for (int i = 0; i < rowCount; i++) {
                        timestamps[i] = baseTimestamp + (i + 1) * 1_000_000L;
                    }

                    // This should not throw - beginColumnarWrite should roll the segment
                    appender.beginColumnarWrite(rowCount);

                    // Write col_a (index 0)
                    appender.putFixedColumn(0, valuesAddrA, rowCount, 8, 0, rowCount);

                    // Write col_b (index 2, after timestamp at index 1)
                    appender.putFixedColumn(2, valuesAddrB, rowCount, 8, 0, rowCount);

                    // Write timestamp
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);

                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                } finally {
                    Unsafe.free(valuesAddrA, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(valuesAddrB, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
                }
            }

            // Verify data via SQL
            drainWalQueue();
            assertSql(
                    "col_a\tcol_b\n" +
                            "100\tnull\n" +  // First row (before col_b was added)
                            "1000\t2000\n" +
                            "1001\t2001\n" +
                            "1002\t2002\n" +
                            "1003\t2003\n" +
                            "1004\t2004\n" +
                            "1005\t2005\n" +
                            "1006\t2006\n" +
                            "1007\t2007\n" +
                            "1008\t2008\n" +
                            "1009\t2009\n",
                    "SELECT col_a, col_b FROM test_segment_roll ORDER BY ts"
            );
        });
    }

    @Test
    public void testBeginEndColumnarWrite_Success() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_lifecycle_success", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 10;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(valuesAddr + (long) i * 4, i * 100);
                }

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    // First columnar write
                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();

                    // Verify segment row count was updated
                    assertEquals(rowCount, walWriter.getSegmentRowCount());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCancelColumnarWrite_NotInProgress() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_lifecycle_cancel_noop", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                // Should be a no-op when not in progress (no exception)
                appender.cancelColumnarWrite();
            }
        });
    }

    @Test
    public void testCancelColumnarWrite_Rollback() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_lifecycle_cancel", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 10;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(valuesAddr + (long) i * 4, i * 100);
                }

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    // Start write but cancel it
                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    appender.cancelColumnarWrite();

                    // Verify no rows were written
                    assertEquals(0, walWriter.getSegmentRowCount());

                    // Should be able to start a new write after cancel
                    appender.beginColumnarWrite(5);
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testColumnarMatchesRowPath_FixedWidth() throws Exception {
        assertMemoryLeak(() -> {
            // Write via row path
            TableToken tableTokenRow = createTable(new TableModel(configuration, "test_equiv_row", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            // Write via columnar path
            TableToken tableTokenColumnar = createTable(new TableModel(configuration, "test_equiv_col", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 50;
            int[] values = new int[rowCount];
            long[] timestamps = new long[rowCount];
            long baseTimestamp = 1_000_000_000L;
            for (int i = 0; i < rowCount; i++) {
                values[i] = i * 10;
                timestamps[i] = baseTimestamp + i * 1_000_000L;
            }

            // Row path write
            String walNameRow;
            try (WalWriter walWriter = engine.getWalWriter(tableTokenRow)) {
                walNameRow = walWriter.getWalName();
                for (int i = 0; i < rowCount; i++) {
                    TableWriter.Row row = walWriter.newRow(timestamps[i]);
                    row.putInt(0, values[i]);
                    row.append();
                }
                walWriter.commit();
            }

            // Columnar path write
            String walNameColumnar;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(valuesAddr + (long) i * 4, values[i]);
                }

                try (WalWriter walWriter = engine.getWalWriter(tableTokenColumnar)) {
                    walNameColumnar = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }

            // Compare results
            try (WalReader readerRow = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableTokenRow, walNameRow, 0, rowCount);
                 WalReader readerColumnar = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableTokenColumnar, walNameColumnar, 0, rowCount)) {

                RecordCursor cursorRow = readerRow.getDataCursor();
                RecordCursor cursorColumnar = readerColumnar.getDataCursor();
                Record recordRow = cursorRow.getRecord();
                Record recordColumnar = cursorColumnar.getRecord();

                int row = 0;
                while (cursorRow.hasNext() && cursorColumnar.hasNext()) {
                    assertEquals("Row " + row + " INT mismatch",
                            recordRow.getInt(0), recordColumnar.getInt(0));
                    assertEquals("Row " + row + " TIMESTAMP mismatch",
                            recordRow.getTimestamp(1), recordColumnar.getTimestamp(1));
                    row++;
                }
                assertEquals(rowCount, row);
                assertFalse(cursorRow.hasNext());
                assertFalse(cursorColumnar.hasNext());
            }
        });
    }

    @Test
    public void testColumnarMatchesRowPath_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableTokenRow = createTable(new TableModel(configuration, "test_equiv_null_row", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            TableToken tableTokenColumnar = createTable(new TableModel(configuration, "test_equiv_null_col", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 6;
            // Pattern: [NULL, 100, NULL, 200, NULL, 300]
            int[] values = {Numbers.INT_NULL, 100, Numbers.INT_NULL, 200, Numbers.INT_NULL, 300};
            long[] timestamps = makeTimestamps(rowCount);

            // Row path write
            String walNameRow;
            try (WalWriter walWriter = engine.getWalWriter(tableTokenRow)) {
                walNameRow = walWriter.getWalName();
                for (int i = 0; i < rowCount; i++) {
                    TableWriter.Row row = walWriter.newRow(timestamps[i]);
                    row.putInt(0, values[i]);
                    row.append();
                }
                walWriter.commit();
            }

            // Columnar path write
            String walNameColumnar;
            int valueCount = 3;
            long valuesAddr = Unsafe.malloc((long) valueCount * 4, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 2);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 4);

                Unsafe.putInt(valuesAddr, 100);
                Unsafe.putInt(valuesAddr + 4, 200);
                Unsafe.putInt(valuesAddr + 8, 300);

                try (WalWriter walWriter = engine.getWalWriter(tableTokenColumnar)) {
                    walNameColumnar = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 4, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 4, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }

            // Compare results
            try (WalReader readerRow = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableTokenRow, walNameRow, 0, rowCount);
                 WalReader readerColumnar = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableTokenColumnar, walNameColumnar, 0, rowCount)) {

                RecordCursor cursorRow = readerRow.getDataCursor();
                RecordCursor cursorColumnar = readerColumnar.getDataCursor();
                Record recordRow = cursorRow.getRecord();
                Record recordColumnar = cursorColumnar.getRecord();

                int row = 0;
                while (cursorRow.hasNext() && cursorColumnar.hasNext()) {
                    assertEquals("Row " + row + " INT mismatch",
                            recordRow.getInt(0), recordColumnar.getInt(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testColumnarWrite_LargeBatch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_large_batch", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 100_000;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(valuesAddr + (long) i * 4, i);
                }

                long baseTimestamp = 1_000_000_000L;
                long[] timestamps = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    timestamps[i] = baseTimestamp + i * 1000L;
                }

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(row, record.getInt(0));
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testColumnarWrite_MultipleBatches() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_multi_batch", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            long baseTimestamp = 1_000_000_000L;

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                // First batch: 5 rows
                int batch1Count = 5;
                long batch1Addr = Unsafe.malloc((long) batch1Count * 4, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < batch1Count; i++) {
                        Unsafe.putInt(batch1Addr + (long) i * 4, i * 10);
                    }
                    long[] ts1 = new long[batch1Count];
                    for (int i = 0; i < batch1Count; i++) {
                        ts1[i] = baseTimestamp + i * 1_000_000L;
                    }

                    appender.beginColumnarWrite(batch1Count);
                    appender.putFixedColumn(0, batch1Addr, batch1Count, 4, 0, batch1Count);
                    putTimestampColumn(appender, walWriter, ts1, batch1Count);
                    appender.endColumnarWrite(ts1[0], ts1[batch1Count - 1], false);
                    walWriter.commit();
                } finally {
                    Unsafe.free(batch1Addr, (long) batch1Count * 4, MemoryTag.NATIVE_DEFAULT);
                }

                // Second batch: 5 more rows
                int batch2Count = 5;
                long batch2Addr = Unsafe.malloc((long) batch2Count * 4, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < batch2Count; i++) {
                        Unsafe.putInt(batch2Addr + (long) i * 4, (batch1Count + i) * 10);
                    }
                    long[] ts2 = new long[batch2Count];
                    for (int i = 0; i < batch2Count; i++) {
                        ts2[i] = baseTimestamp + (batch1Count + i) * 1_000_000L;
                    }

                    appender.beginColumnarWrite(batch2Count);
                    appender.putFixedColumn(0, batch2Addr, batch2Count, 4, 0, batch2Count);
                    putTimestampColumn(appender, walWriter, ts2, batch2Count);
                    appender.endColumnarWrite(ts2[0], ts2[batch2Count - 1], false);
                    walWriter.commit();
                } finally {
                    Unsafe.free(batch2Addr, (long) batch2Count * 4, MemoryTag.NATIVE_DEFAULT);
                }
            }

            drainWalQueue();
            assertSql(
                    """
                            value
                            0
                            10
                            20
                            30
                            40
                            50
                            60
                            70
                            80
                            90
                            """,
                    "SELECT value FROM test_multi_batch ORDER BY ts"
            );
        });
    }

    @Test
    public void testColumnarWrite_MultipleSortedBatchesInSingleDedupTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_multi_batch_dedup (id int, ts timestamp) timestamp(ts) partition by HOUR WAL DEDUP UPSERT KEYS(ts, id)");

            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("test_multi_batch_dedup"))) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                int batchCount = 5;
                long baseTimestamp = 1_000_000_000L;
                long valuesAddr = Unsafe.malloc((long) batchCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < batchCount; i++) {
                        Unsafe.putInt(valuesAddr + (long) i * Integer.BYTES, i);
                    }

                    long[] timestamps = new long[batchCount];
                    for (int i = 0; i < batchCount; i++) {
                        timestamps[i] = baseTimestamp + i * 1_000_000L;
                    }

                    appender.beginColumnarWrite(batchCount);
                    appender.putFixedColumn(0, valuesAddr, batchCount, Integer.BYTES, 0, batchCount);
                    putTimestampColumn(appender, walWriter, timestamps, batchCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[batchCount - 1], false);

                    appender.beginColumnarWrite(batchCount);
                    appender.putFixedColumn(0, valuesAddr, batchCount, Integer.BYTES, 0, batchCount);
                    putTimestampColumn(appender, walWriter, timestamps, batchCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[batchCount - 1], false);

                    walWriter.commit();
                } finally {
                    Unsafe.free(valuesAddr, (long) batchCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }

            drainWalQueue();
            assertSql("count\n5\n", "SELECT count() FROM test_multi_batch_dedup");
            assertSql("id\n0\n1\n2\n3\n4\n", "SELECT id FROM test_multi_batch_dedup ORDER BY ts, id");
        });
    }

    /**
     * Tests that columns not written during columnar write are properly filled with nulls.
     * This tests the bug where finishColumnarWrite only wrote ONE null instead of rowCount nulls.
     */
    @Test
    public void testColumnarWrite_PartialColumns_NullFilling() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with 3 columns but only write 1 column + timestamp
            TableToken tableToken = createTable(new TableModel(configuration, "test_partial_cols", PartitionBy.HOUR)
                    .col("written_col", ColumnType.INT)
                    .col("unwritten_col1", ColumnType.LONG)  // Will NOT be written
                    .col("unwritten_col2", ColumnType.DOUBLE)  // Will NOT be written
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 10;  // Write 10 rows but only write one column
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(valuesAddr + (long) i * 4, i * 100);
                }

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    // Only write column 0 and timestamp - columns 1 and 2 are NOT written
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                // Verify: should have 10 rows, column 0 has values, columns 1 and 2 should be NULL
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        // Column 0 should have values
                        assertEquals("Row " + row + " col 0", row * 100, record.getInt(0));
                        // Column 1 (LONG) should be NULL
                        assertEquals("Row " + row + " col 1 should be NULL", Numbers.LONG_NULL, record.getLong(1));
                        // Column 2 (DOUBLE) should be NaN (null sentinel for doubles)
                        assertTrue("Row " + row + " col 2 should be NaN", Double.isNaN(record.getDouble(2)));
                        // Timestamp should be correct
                        assertEquals("Row " + row + " timestamp", timestamps[row], record.getTimestamp(3));
                        row++;
                    }
                    assertEquals("Should have 10 rows", rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Verifies that putServerAssignedTimestampColumnar writes the caller-provided
     * timestamp to all rows, so the WAL event metadata (min/max) is consistent
     * with the actual data.
     */
    @Test
    public void testColumnarWrite_ServerAssignedTimestamp_ConsistentMinMax() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_server_ts_consistent", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            long valuesAddr = Unsafe.malloc((long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            long now = 100_000_000L;
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(valuesAddr + (long) i * Integer.BYTES, i);
                }

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, Integer.BYTES, 0, rowCount);

                    // The caller captures the timestamp and passes it to both
                    // putServerAssignedTimestampColumnar and endColumnarWrite,
                    // ensuring metadata and data are consistent.
                    walWriter.putServerAssignedTimestampColumnar(rowCount, now);
                    appender.endColumnarWrite(now, now, false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(
                        sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {

                    // Verify all rows have the same timestamp matching the caller-provided value
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    int row = 0;
                    while (cursor.hasNext()) {
                        long ts = record.getTimestamp(1);
                        assertEquals("All rows should have the caller-provided timestamp", now, ts);
                        row++;
                    }
                    assertEquals(rowCount, row);

                    // Verify WAL event maxTimestamp matches the same value
                    io.questdb.cairo.wal.WalEventCursor eventCursor = reader.getWalEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(io.questdb.cairo.wal.WalTxnType.DATA, eventCursor.getType());

                    io.questdb.cairo.wal.WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    long eventMaxTimestamp = dataInfo.getMaxTimestamp();
                    assertEquals(now, eventMaxTimestamp);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testColumnarWrite_SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_single_row", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            long valuesAddr = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putInt(valuesAddr, 42);
                long timestamp = 1_000_000_000L;

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    putTimestampColumn(appender, walWriter, new long[]{timestamp}, rowCount);
                    appender.endColumnarWrite(timestamp, timestamp, false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(42, record.getInt(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCommitDuringColumnarWrite_Throws() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_lifecycle_commit_guard", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(1);
                putTimestampColumn(appender, walWriter, new long[]{1_000_000L}, 1);

                try {
                    walWriter.commit();
                    Assert.fail("Expected CairoException");
                } catch (CairoException e) {
                    assertTrue(e.getMessage().contains("cannot commit during columnar write"));
                }
            }
        });
    }

    @Test
    public void testEndColumnarWrite_MissingDesignatedTimestamp_Throws() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_lifecycle_missing_ts", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            long valuesAddr = Unsafe.malloc((long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putInt(valuesAddr, 42);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, Integer.BYTES, 0, rowCount);

                    try {
                        appender.endColumnarWrite(1_000_000L, 1_000_000L, false);
                        Assert.fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("designated timestamp column"));
                    } finally {
                        appender.cancelColumnarWrite();
                    }

                    assertEquals(0, walWriter.getSegmentRowCount());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEndColumnarWrite_NotInProgress() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_lifecycle_end", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                try {
                    appender.endColumnarWrite(0, 0, false);
                    Assert.fail("Expected CairoException");
                } catch (CairoException e) {
                    assertTrue(e.getMessage().contains("not in columnar write mode"));
                }
            }
        });
    }

    /**
     * Tests that Gorilla-encoded timestamp cursors correctly report they don't support direct access.
     */
    @Test
    public void testGorillaEncodedTimestamp_DoesNotSupportDirectAccess() throws Exception {
        assertMemoryLeak(() -> {
            QwpTimestampColumnCursor tsCursor = new QwpTimestampColumnCursor();

            long[] timestamps = {1_000_000L, 1_001_000L};

            // Build wire format with Gorilla encoding
            // Wire format: [null bitmap flag (0)] [encoding byte (0x01)] [first timestamp] [second timestamp]
            int dataLength = 1 + 1 + 8 + 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            try {
                // No null bitmap
                Unsafe.putByte(dataAddress, (byte) 0);

                // Write Gorilla encoding flag
                Unsafe.putByte(dataAddress + 1, (byte) 0x01); // ENCODING_GORILLA

                // Write first two timestamps (required for Gorilla)
                Unsafe.putLong(dataAddress + 2, timestamps[0]);
                Unsafe.putLong(dataAddress + 10, timestamps[1]);

                // Initialize cursor with gorillaEnabled=true
                tsCursor.of(dataAddress, dataLength, 2, QwpConstants.TYPE_TIMESTAMP,
                        true);

                // Verify Gorilla encoding is detected
                assertFalse("Gorilla-encoded timestamp should NOT support direct access",
                        tsCursor.supportsDirectAccess());

                // Verify the cursor can still be used for row-by-row access
                tsCursor.resetRowPosition();
                tsCursor.advanceRow();
                assertEquals(timestamps[0], tsCursor.getTimestamp());
                tsCursor.advanceRow();
                assertEquals(timestamps[1], tsCursor.getTimestamp());

            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Tests that LONG256 values are correctly read from little-endian wire format.
     * Wire format: [long0: 8 bytes] [long1: 8 bytes] [long2: 8 bytes] [long3: 8 bytes]
     * where long0 is least significant and long3 is most significant.
     */
    @Test
    public void testLong256LittleEndianWireFormat() throws Exception {
        assertMemoryLeak(() -> {
            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();

            // Test LONG256 value
            long expected0 = 0x1111111111111111L; // least significant
            long expected1 = 0x2222222222222222L;
            long expected2 = 0x3333333333333333L;
            long expected3 = 0x4444444444444444L; // most significant

            // Wire format is little-endian: least significant first
            int dataLength = 1 + 32; // null bitmap flag + 4 longs
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            try {
                // No null bitmap
                Unsafe.putByte(dataAddress, (byte) 0);

                // Write in little-endian order
                Unsafe.putLong(dataAddress + 1, expected0);
                Unsafe.putLong(dataAddress + 9, expected1);
                Unsafe.putLong(dataAddress + 17, expected2);
                Unsafe.putLong(dataAddress + 25, expected3);

                // Initialize cursor
                cursor.of(dataAddress, dataLength, 1, QwpConstants.TYPE_LONG256);

                // Read and verify
                cursor.advanceRow();
                assertEquals("LONG256_0 mismatch", expected0, cursor.getLong256_0());
                assertEquals("LONG256_1 mismatch", expected1, cursor.getLong256_1());
                assertEquals("LONG256_2 mismatch", expected2, cursor.getLong256_2());
                assertEquals("LONG256_3 mismatch", expected3, cursor.getLong256_3());

                // Verify direct memory matches what columnar path would memcpy
                long valuesAddr = cursor.getValuesAddress();
                assertEquals("Wire long0 should match", expected0, Unsafe.getLong(valuesAddr));
                assertEquals("Wire long1 should match", expected1, Unsafe.getLong(valuesAddr + 8));
                assertEquals("Wire long2 should match", expected2, Unsafe.getLong(valuesAddr + 16));
                assertEquals("Wire long3 should match", expected3, Unsafe.getLong(valuesAddr + 24));

            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testLong256_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long256_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.LONG256)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            int valueCount = 2;
            long valuesAddr = Unsafe.malloc((long) valueCount * 32, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 1);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 3);

                // Long256 value 1: (10, 20, 30, 40)
                Unsafe.putLong(valuesAddr, 10L);
                Unsafe.putLong(valuesAddr + 8, 20L);
                Unsafe.putLong(valuesAddr + 16, 30L);
                Unsafe.putLong(valuesAddr + 24, 40L);
                // Long256 value 2: (50, 60, 70, 80)
                Unsafe.putLong(valuesAddr + 32, 50L);
                Unsafe.putLong(valuesAddr + 40, 60L);
                Unsafe.putLong(valuesAddr + 48, 70L);
                Unsafe.putLong(valuesAddr + 56, 80L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 32, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    // Row 0: non-null (10, 20, 30, 40)
                    assertTrue(cursor.hasNext());
                    assertEquals(10L, record.getLong256A(0).getLong0());
                    assertEquals(20L, record.getLong256A(0).getLong1());
                    assertEquals(30L, record.getLong256A(0).getLong2());
                    assertEquals(40L, record.getLong256A(0).getLong3());

                    // Row 1: null
                    assertTrue(cursor.hasNext());
                    assertEquals(Numbers.LONG_NULL, record.getLong256A(0).getLong0());
                    assertEquals(Numbers.LONG_NULL, record.getLong256A(0).getLong1());
                    assertEquals(Numbers.LONG_NULL, record.getLong256A(0).getLong2());
                    assertEquals(Numbers.LONG_NULL, record.getLong256A(0).getLong3());

                    // Row 2: non-null (50, 60, 70, 80)
                    assertTrue(cursor.hasNext());
                    assertEquals(50L, record.getLong256A(0).getLong0());
                    assertEquals(60L, record.getLong256A(0).getLong1());
                    assertEquals(70L, record.getLong256A(0).getLong2());
                    assertEquals(80L, record.getLong256A(0).getLong3());

                    // Row 3: null
                    assertTrue(cursor.hasNext());
                    assertEquals(Numbers.LONG_NULL, record.getLong256A(0).getLong0());
                    assertEquals(Numbers.LONG_NULL, record.getLong256A(0).getLong1());
                    assertEquals(Numbers.LONG_NULL, record.getLong256A(0).getLong2());
                    assertEquals(Numbers.LONG_NULL, record.getLong256A(0).getLong3());

                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 32, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMultipleColumns_AllTypes() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_multi_all", PartitionBy.HOUR)
                    .col("int_col", ColumnType.INT)
                    .col("long_col", ColumnType.LONG)
                    .col("double_col", ColumnType.DOUBLE)
                    .col("varchar_col", ColumnType.VARCHAR)
                    .col("sym_col", ColumnType.SYMBOL)
                    .col("bool_col", ColumnType.BOOLEAN)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 10;
            long intAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            long longAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            long doubleAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(intAddr + (long) i * 4, i * 10);
                    Unsafe.putLong(longAddr + (long) i * 8, (long) i * 1000);
                    Unsafe.putDouble(doubleAddr + (long) i * 8, i * 1.5);
                }

                String[] varcharValues = new String[rowCount];
                String[] symbolValues = new String[rowCount];
                Boolean[] boolValues = new Boolean[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    varcharValues[i] = "str_" + i;
                    symbolValues[i] = "sym_" + (i % 3);
                    boolValues[i] = i % 2 == 0;
                }

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken);
                     StringColumnWireFormat varcharWire = new StringColumnWireFormat(varcharValues);
                     SymbolColumnWireFormat symbolWire = new SymbolColumnWireFormat(symbolValues);
                     BooleanColumnWireFormat boolWire = new BooleanColumnWireFormat(boolValues)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, intAddr, rowCount, 4, 0, rowCount);
                    appender.putFixedColumn(1, longAddr, rowCount, 8, 0, rowCount);
                    appender.putFixedColumn(2, doubleAddr, rowCount, 8, 0, rowCount);
                    appender.putVarcharColumn(3, varcharWire.cursor, rowCount);
                    appender.putSymbolColumn(4, symbolWire.cursor, rowCount);
                    appender.putBooleanColumn(5, boolWire.cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(row * 10, record.getInt(0));
                        assertEquals((long) row * 1000, record.getLong(1));
                        assertEquals(row * 1.5, record.getDouble(2), 0.0001);
                        TestUtils.assertEquals(varcharValues[row], record.getVarcharA(3));
                        TestUtils.assertEquals(symbolValues[row], record.getSymA(4));
                        assertEquals(boolValues[row], record.getBool(5));
                        assertEquals(timestamps[row], record.getTimestamp(6));
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(intAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(longAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(doubleAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutArrayColumn_1D_Double_AllNull() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 1;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_1d_allnull", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, true)) {
                for (int i = 0; i < rowCount; i++) {
                    wf.addNullRow(i);
                }
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    int row = 0;
                    while (cursor.hasNext()) {
                        ArrayView arr = record.getArray(0, columnType);
                        assertTrue("row " + row + " should be null", arr.isNull());
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_1D_Double_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 1;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_1d_nn", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 10;
            int elementsPerRow = 4;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, false)) {
                for (int i = 0; i < rowCount; i++) {
                    double[] values = new double[elementsPerRow];
                    for (int j = 0; j < elementsPerRow; j++) {
                        values[j] = i * 100.0 + j + 0.5;
                    }
                    wf.addDoubleRow(new int[]{elementsPerRow}, values);
                }
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    int row = 0;
                    while (cursor.hasNext()) {
                        ArrayView arr = record.getArray(0, columnType);
                        assertFalse("row " + row + " should not be null", arr.isNull());
                        assertEquals(1, arr.getDimCount());
                        assertEquals(elementsPerRow, arr.getDimLen(0));
                        for (int j = 0; j < elementsPerRow; j++) {
                            assertEquals(row * 100.0 + j + 0.5, arr.getDouble(j), 1e-15);
                        }
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_1D_Double_SingleElement() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 1;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_1d_single", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, false)) {
                wf.addDoubleRow(new int[]{1}, new double[]{42.0});
                wf.addDoubleRow(new int[]{1}, new double[]{-1.5});
                wf.addDoubleRow(new int[]{1}, new double[]{0.0});
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                double[] expected = {42.0, -1.5, 0.0};
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    int row = 0;
                    while (cursor.hasNext()) {
                        ArrayView arr = record.getArray(0, columnType);
                        assertFalse(arr.isNull());
                        assertEquals(1, arr.getDimLen(0));
                        assertEquals(expected[row], arr.getDouble(0), 1e-15);
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_1D_Double_SpecialValues() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 1;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_1d_special", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            double[] specialValues = {
                    Double.MAX_VALUE, Double.MIN_VALUE, -Double.MAX_VALUE,
                    Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN, -0.0
            };

            int rowCount = 1;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, false)) {
                wf.addDoubleRow(new int[]{specialValues.length}, specialValues);
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    ArrayView arr = record.getArray(0, columnType);
                    assertFalse(arr.isNull());
                    assertEquals(specialValues.length, arr.getDimLen(0));
                    assertEquals(Double.MAX_VALUE, arr.getDouble(0), 0);
                    assertEquals(Double.MIN_VALUE, arr.getDouble(1), 0);
                    assertEquals(-Double.MAX_VALUE, arr.getDouble(2), 0);
                    assertEquals(Double.POSITIVE_INFINITY, arr.getDouble(3), 0);
                    assertEquals(Double.NEGATIVE_INFINITY, arr.getDouble(4), 0);
                    assertTrue(Double.isNaN(arr.getDouble(5)));
                    assertEquals(-0.0, arr.getDouble(6), 0);
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_1D_Double_VaryingLengths() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 1;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_1d_varlen", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            int[][] lengths = {{1}, {5}, {2}, {10}};
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, false)) {
                for (int i = 0; i < rowCount; i++) {
                    int len = lengths[i][0];
                    double[] values = new double[len];
                    for (int j = 0; j < len; j++) {
                        values[j] = i * 100.0 + j;
                    }
                    wf.addDoubleRow(new int[]{len}, values);
                }
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    int row = 0;
                    while (cursor.hasNext()) {
                        ArrayView arr = record.getArray(0, columnType);
                        assertFalse(arr.isNull());
                        int len = lengths[row][0];
                        assertEquals(len, arr.getDimLen(0));
                        for (int j = 0; j < len; j++) {
                            assertEquals(row * 100.0 + j, arr.getDouble(j), 1e-15);
                        }
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_1D_Double_WithNulls_Alternating() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 1;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_1d_alt", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 6;
            int elementsPerRow = 3;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, true)) {
                for (int i = 0; i < rowCount; i++) {
                    if (i % 2 == 0) {
                        wf.addNullRow(i);
                    } else {
                        double[] values = new double[elementsPerRow];
                        for (int j = 0; j < elementsPerRow; j++) {
                            values[j] = i * 10.0 + j;
                        }
                        wf.addDoubleRow(new int[]{elementsPerRow}, values);
                    }
                }
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    int row = 0;
                    while (cursor.hasNext()) {
                        ArrayView arr = record.getArray(0, columnType);
                        if (row % 2 == 0) {
                            assertTrue("row " + row + " should be null", arr.isNull());
                        } else {
                            assertFalse("row " + row + " should not be null", arr.isNull());
                            assertEquals(elementsPerRow, arr.getDimLen(0));
                            for (int j = 0; j < elementsPerRow; j++) {
                                assertEquals(row * 10.0 + j, arr.getDouble(j), 1e-15);
                            }
                        }
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_1D_Double_WithNulls_FirstNull() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 1;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_1d_fn", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, true)) {
                wf.addNullRow(0);
                wf.addDoubleRow(new int[]{2}, new double[]{1.0, 2.0});
                wf.addDoubleRow(new int[]{2}, new double[]{3.0, 4.0});
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertTrue(record.getArray(0, columnType).isNull());
                    assertTrue(cursor.hasNext());
                    ArrayView arr1 = record.getArray(0, columnType);
                    assertEquals(1.0, arr1.getDouble(0), 1e-15);
                    assertEquals(2.0, arr1.getDouble(1), 1e-15);
                    assertTrue(cursor.hasNext());
                    ArrayView arr2 = record.getArray(0, columnType);
                    assertEquals(3.0, arr2.getDouble(0), 1e-15);
                    assertEquals(4.0, arr2.getDouble(1), 1e-15);
                    assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_1D_Double_WithNulls_LastNull() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 1;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_1d_ln", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, true)) {
                wf.addDoubleRow(new int[]{2}, new double[]{1.0, 2.0});
                wf.addDoubleRow(new int[]{2}, new double[]{3.0, 4.0});
                wf.addNullRow(2);
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertFalse(record.getArray(0, columnType).isNull());
                    assertTrue(cursor.hasNext());
                    assertFalse(record.getArray(0, columnType).isNull());
                    assertTrue(cursor.hasNext());
                    assertTrue(record.getArray(0, columnType).isNull());
                    assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_2D_Double_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 2;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_2d", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int dim0 = 2, dim1 = 3;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, false)) {
                for (int i = 0; i < rowCount; i++) {
                    double[] values = new double[dim0 * dim1];
                    for (int j = 0; j < values.length; j++) {
                        values[j] = i * 100.0 + j;
                    }
                    wf.addDoubleRow(new int[]{dim0, dim1}, values);
                }
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    int row = 0;
                    while (cursor.hasNext()) {
                        ArrayView arr = record.getArray(0, columnType);
                        assertFalse(arr.isNull());
                        assertEquals(2, arr.getDimCount());
                        assertEquals(dim0, arr.getDimLen(0));
                        assertEquals(dim1, arr.getDimLen(1));
                        for (int j = 0; j < dim0 * dim1; j++) {
                            assertEquals(row * 100.0 + j, arr.getDouble(j), 1e-15);
                        }
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_2D_Double_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 2;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_2d_nulls", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            int dim0 = 2, dim1 = 2;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, true)) {
                wf.addDoubleRow(new int[]{dim0, dim1}, new double[]{1.0, 2.0, 3.0, 4.0});
                wf.addNullRow(1);
                wf.addDoubleRow(new int[]{dim0, dim1}, new double[]{5.0, 6.0, 7.0, 8.0});
                wf.addNullRow(3);
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    ArrayView a0 = record.getArray(0, columnType);
                    assertFalse(a0.isNull());
                    assertEquals(1.0, a0.getDouble(0), 1e-15);
                    assertTrue(cursor.hasNext());
                    assertTrue(record.getArray(0, columnType).isNull());
                    assertTrue(cursor.hasNext());
                    ArrayView a2 = record.getArray(0, columnType);
                    assertFalse(a2.isNull());
                    assertEquals(5.0, a2.getDouble(0), 1e-15);
                    assertTrue(cursor.hasNext());
                    assertTrue(record.getArray(0, columnType).isNull());
                    assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_3D_Double_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 3;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_3d", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            int d0 = 2, d1 = 3, d2 = 2;
            int totalElements = d0 * d1 * d2;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, false)) {
                for (int i = 0; i < rowCount; i++) {
                    double[] values = new double[totalElements];
                    for (int j = 0; j < totalElements; j++) {
                        values[j] = i * 1000.0 + j;
                    }
                    wf.addDoubleRow(new int[]{d0, d1, d2}, values);
                }
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    int row = 0;
                    while (cursor.hasNext()) {
                        ArrayView arr = record.getArray(0, columnType);
                        assertFalse(arr.isNull());
                        assertEquals(3, arr.getDimCount());
                        assertEquals(d0, arr.getDimLen(0));
                        assertEquals(d1, arr.getDimLen(1));
                        assertEquals(d2, arr.getDimLen(2));
                        for (int j = 0; j < totalElements; j++) {
                            assertEquals(row * 1000.0 + j, arr.getDouble(j), 1e-15);
                        }
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_LargeArray() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 1;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_large", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            int elemCount = 10_000;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, false)) {
                double[] values = new double[elemCount];
                for (int j = 0; j < elemCount; j++) {
                    values[j] = j * 0.001;
                }
                wf.addDoubleRow(new int[]{elemCount}, values);
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    ArrayView arr = record.getArray(0, columnType);
                    assertFalse(arr.isNull());
                    assertEquals(elemCount, arr.getDimLen(0));
                    assertEquals(0.0, arr.getDouble(0), 1e-15);
                    assertEquals(5000 * 0.001, arr.getDouble(5000), 1e-12);
                    assertEquals((elemCount - 1) * 0.001, arr.getDouble(elemCount - 1), 1e-12);
                }
            }
        });
    }

    @Test
    public void testPutArrayColumn_SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            int nDims = 1;
            int columnType = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            TableToken tableToken = createTable(new TableModel(configuration, "test_arr_single_row", PartitionBy.HOUR)
                    .col("arr", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            try (ArrayColumnWireFormat wf = new ArrayColumnWireFormat(rowCount, nDims, false)) {
                wf.addDoubleRow(new int[]{3}, new double[]{1.1, 2.2, 3.3});
                wf.build();

                long[] timestamps = makeTimestamps(rowCount);
                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                    appender.beginColumnarWrite(rowCount);
                    appender.putArrayColumn(0, wf.cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    ArrayView arr = record.getArray(0, columnType);
                    assertFalse(arr.isNull());
                    assertEquals(3, arr.getDimLen(0));
                    assertEquals(1.1, arr.getDouble(0), 1e-15);
                    assertEquals(2.2, arr.getDouble(1), 1e-15);
                    assertEquals(3.3, arr.getDouble(2), 1e-15);
                    assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testPutBooleanColumn_AllFalse() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_bool_false", PartitionBy.HOUR)
                    .col("value", ColumnType.BOOLEAN)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            Boolean[] values = {false, false, false, false, false};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 BooleanColumnWireFormat wireFormat = new BooleanColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putBooleanColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertFalse(record.getBool(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutBooleanColumn_AllTrue() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_bool_true", PartitionBy.HOUR)
                    .col("value", ColumnType.BOOLEAN)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            Boolean[] values = {true, true, true, true, true};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 BooleanColumnWireFormat wireFormat = new BooleanColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putBooleanColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertTrue(record.getBool(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutBooleanColumn_Mixed() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_bool_mixed", PartitionBy.HOUR)
                    .col("value", ColumnType.BOOLEAN)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 6;
            Boolean[] values = {true, false, true, false, true, false};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 BooleanColumnWireFormat wireFormat = new BooleanColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putBooleanColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertEquals(values[row], record.getBool(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutBooleanColumn_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_bool_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.BOOLEAN)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // Null booleans are stored as false (0) in WAL
            Boolean[] values = {true, null, false, null};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 BooleanColumnWireFormat wireFormat = new BooleanColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putBooleanColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                assertTrue(cursor.hasNext());
                assertTrue(record.getBool(0));

                assertTrue(cursor.hasNext());
                assertFalse(record.getBool(0)); // NULL → false

                assertTrue(cursor.hasNext());
                assertFalse(record.getBool(0));

                assertTrue(cursor.hasNext());
                assertFalse(record.getBool(0)); // NULL → false

                assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testPutBooleanToNumericColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_bool_num", PartitionBy.HOUR)
                    .col("b", ColumnType.BYTE)
                    .col("s", ColumnType.SHORT)
                    .col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
                    .col("f", ColumnType.FLOAT)
                    .col("d", ColumnType.DOUBLE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            Boolean[] values = {true, false, null, true};
            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 BooleanColumnWireFormat bFmt = new BooleanColumnWireFormat(values);
                 BooleanColumnWireFormat sFmt = new BooleanColumnWireFormat(values);
                 BooleanColumnWireFormat iFmt = new BooleanColumnWireFormat(values);
                 BooleanColumnWireFormat lFmt = new BooleanColumnWireFormat(values);
                 BooleanColumnWireFormat fFmt = new BooleanColumnWireFormat(values);
                 BooleanColumnWireFormat dFmt = new BooleanColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putBooleanToNumericColumn(0, bFmt.cursor, rowCount, ColumnType.BYTE);
                appender.putBooleanToNumericColumn(1, sFmt.cursor, rowCount, ColumnType.SHORT);
                appender.putBooleanToNumericColumn(2, iFmt.cursor, rowCount, ColumnType.INT);
                appender.putBooleanToNumericColumn(3, lFmt.cursor, rowCount, ColumnType.LONG);
                appender.putBooleanToNumericColumn(4, fFmt.cursor, rowCount, ColumnType.FLOAT);
                appender.putBooleanToNumericColumn(5, dFmt.cursor, rowCount, ColumnType.DOUBLE);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            // BYTE and SHORT: null sentinel is 0
            // INT, LONG, FLOAT, DOUBLE: null sentinel is distinct (null)
            assertSql(
                    """
                            b\ts\ti\tl\tf\td
                            1\t1\t1\t1\t1.0\t1.0
                            0\t0\t0\t0\t0.0\t0.0
                            0\t0\tnull\tnull\tnull\tnull
                            1\t1\t1\t1\t1.0\t1.0
                            """,
                    "SELECT b, s, i, l, f, d FROM test_bool_num ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutBooleanToStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_bool_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            Boolean[] values = {true, false, null, true};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 BooleanColumnWireFormat wireFormat = new BooleanColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putBooleanToStringColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\ntrue\nfalse\n\ntrue\n",
                    "SELECT value FROM test_bool_str ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutBooleanToVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_bool_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            Boolean[] values = {false, true, null, false};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 BooleanColumnWireFormat wireFormat = new BooleanColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putBooleanToVarcharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\nfalse\ntrue\n\nfalse\n",
                    "SELECT value FROM test_bool_vc ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutCharColumn_EmptyString() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_char_empty", PartitionBy.HOUR)
                    .col("value", ColumnType.CHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"", "", ""};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putCharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertEquals(0, record.getChar(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutCharColumn_FourByteUtf8() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_char_4byte", PartitionBy.HOUR)
                    .col("value", ColumnType.CHAR)
                    .timestamp("ts")
                    .wal()
            );

            // 4-byte UTF-8 characters (emoji) are not representable as a single char,
            // so utf8CharDecode returns 0 and the column stores (char) 0
            int rowCount = 2;
            String[] values = {"\uD83C\uDF89", "\uD83D\uDE00"};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putCharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertEquals(0, record.getChar(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutCharColumn_MultiByte() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_char_multibyte", PartitionBy.HOUR)
                    .col("value", ColumnType.CHAR)
                    .timestamp("ts")
                    .wal()
            );

            // 2-byte UTF-8: é (U+00E9), ñ (U+00F1)
            // 3-byte UTF-8: € (U+20AC)
            int rowCount = 3;
            String[] values = {"é", "ñ", "€"};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putCharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertEquals(values[row].charAt(0), record.getChar(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutDecimalColumn_Decimal256ToDecimal128_Overflow() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(38, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec_to_dec128_ovf", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            // Wire format: [null bitmap flag (0)][scale: 1 byte][values: rowCount * 32 bytes, little-endian]
            int dataLength = 1 + 1 + rowCount * 32;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putByte(dataAddress + 1, (byte) 2);
                // 256-bit value with hl=1 overflows 128-bit decimal
                // Wire order is true little-endian: ll, lh, hl, hh
                long offset = 2;
                Unsafe.putLong(dataAddress + offset, 12_345L);      // ll
                Unsafe.putLong(dataAddress + offset + 8, 0L);       // lh
                Unsafe.putLong(dataAddress + offset + 16, 1L);      // hl
                Unsafe.putLong(dataAddress + offset + 24, 0L);      // hh

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL256);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    try {
                        appender.putDecimal256Column(0, cursor, rowCount, columnType);
                        fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("decimal value overflows"));
                    }
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalColumn_Decimal256ToDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(18, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec256_to_dec64", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            // Wire format: [null bitmap flag (0)][scale: 1 byte][values: rowCount * 32 bytes, little-endian]
            int dataLength = 1 + 1 + rowCount * 32;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putByte(dataAddress + 1, (byte) 2);
                // Value 1: 12345 (= 123.45), sign-extended to 256-bit
                // Wire order is true little-endian: ll, lh, hl, hh
                long offset = 2;
                Unsafe.putLong(dataAddress + offset, 12_345L);      // ll
                Unsafe.putLong(dataAddress + offset + 8, 0L);       // lh
                Unsafe.putLong(dataAddress + offset + 16, 0L);      // hl
                Unsafe.putLong(dataAddress + offset + 24, 0L);      // hh
                // Value 2: -9999 (= -99.99), sign-extended
                offset = 34;
                Unsafe.putLong(dataAddress + offset, -9999L);       // ll
                Unsafe.putLong(dataAddress + offset + 8, -1L);      // lh
                Unsafe.putLong(dataAddress + offset + 16, -1L);     // hl
                Unsafe.putLong(dataAddress + offset + 24, -1L);     // hh

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL256);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putDecimal64Column(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                123.45
                                -99.99
                                """,
                        "SELECT value FROM test_dec256_to_dec64 ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalColumn_Decimal64ToDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(38, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec64_to_dec128", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            // Wire format: [null bitmap flag (0)][scale: 1 byte][values: rowCount * 8 bytes, little-endian]
            int dataLength = 1 + 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putByte(dataAddress + 1, (byte) 2);
                // Values: 12345 (= 123.45), -9999 (= -99.99) in little-endian
                Unsafe.putLong(dataAddress + 2, 12_345L);
                Unsafe.putLong(dataAddress + 10, -9999L);

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putDecimal128Column(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                123.45
                                -99.99
                                """,
                        "SELECT value FROM test_dec64_to_dec128 ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalColumn_Decimal64ToDecimal16_Overflow() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(4, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec_to_dec16_ovf", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            // Wire format: [null bitmap flag (0)][scale: 1 byte][values: rowCount * 8 bytes, little-endian]
            int dataLength = 1 + 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putByte(dataAddress + 1, (byte) 2);
                // Value: 40_000 overflows short range for DECIMAL16
                Unsafe.putLong(dataAddress + 2, 40_000L);

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    try {
                        appender.putDecimal64Column(0, cursor, rowCount, columnType);
                        fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("decimal value overflows"));
                    }
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalColumn_Decimal64ToDecimal16_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL16: precision 4 (2 bytes storage), scale 2
            int columnType = ColumnType.getDecimalType(4, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec64_to_dec16_nulls", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // Pattern: [NULL, 12.34, NULL, -99.99]
            byte scale = 2;
            long[] unscaledValues = {1234L, -9999L};
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 1 + unscaledValues.length * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 2);

                Unsafe.putByte(bitmapAddr + bitmapSize, scale);

                long valuesAddr = bitmapAddr + bitmapSize + 1;
                for (int i = 0; i < unscaledValues.length; i++) {
                    Unsafe.putLong(valuesAddr + (long) i * 8, unscaledValues[i]);
                }

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putDecimalToSmallDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n\n12.34\n\n-99.99\n",
                        "SELECT value FROM test_dec64_to_dec16_nulls ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalColumn_Decimal64ToDecimal32_Overflow() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(9, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec_to_dec32_ovf", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            int dataLength = 1 + 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putByte(dataAddress + 1, (byte) 2);
                // Value: 3_000_000_000 overflows int range for DECIMAL32
                Unsafe.putLong(dataAddress + 2, 3_000_000_000L);

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    try {
                        appender.putDecimal64Column(0, cursor, rowCount, columnType);
                        fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("decimal value overflows"));
                    }
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalColumn_Decimal64ToDecimal64_ScaleDownPrecisionLoss() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(10, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec64_to_dec64_precision_loss", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            int dataLength = 1 + 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putByte(dataAddress + 1, (byte) 4); // wire scale
                Unsafe.putLong(dataAddress + 2, 12_345L); // 1.2345 -> cannot be represented at scale 2

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    try {
                        appender.putDecimal64Column(0, cursor, rowCount, columnType);
                        fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("precision loss"));
                    }
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalColumn_Decimal64ToDecimal8_PrecisionOverflow() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(2, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec64_to_dec8_precision_ovf", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            int dataLength = 1 + 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putByte(dataAddress + 1, (byte) 0); // wire scale
                Unsafe.putLong(dataAddress + 2, 13L); // 13 -> 13.0, exceeds DECIMAL(2,1)

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    try {
                        appender.putDecimalToSmallDecimalColumn(0, cursor, rowCount, columnType);
                        fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("decimal value overflows"));
                    }
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalColumn_Decimal64ToDecimal8_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL8: precision 2 (1 byte storage), scale 1
            int columnType = ColumnType.getDecimalType(2, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec64_to_dec8_nulls", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // Pattern: [NULL, 1.5, NULL, -2.5]
            byte scale = 1;
            long[] unscaledValues = {15L, -25L};
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 1 + unscaledValues.length * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 2);

                Unsafe.putByte(bitmapAddr + bitmapSize, scale);

                long valuesAddr = bitmapAddr + bitmapSize + 1;
                for (int i = 0; i < unscaledValues.length; i++) {
                    Unsafe.putLong(valuesAddr + (long) i * 8, unscaledValues[i]);
                }

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putDecimalToSmallDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n\n1.5\n\n-2.5\n",
                        "SELECT value FROM test_dec64_to_dec8_nulls ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalColumn_Rescale() throws Exception {
        assertMemoryLeak(() -> {
            // Wire scale=2, column scale=4
            int columnType = ColumnType.getDecimalType(18, 4);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec_rescale", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            int dataLength = 1 + 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                // Wire scale = 2
                Unsafe.putByte(dataAddress + 1, (byte) 2);
                // Values: 12345 (= 123.45), -9999 (= -99.99) in little-endian
                Unsafe.putLong(dataAddress + 2, 12_345L);
                Unsafe.putLong(dataAddress + 10, -9999L);

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putDecimal64Column(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                123.4500
                                -99.9900
                                """,
                        "SELECT value FROM test_dec_rescale ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalToStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            // Decimal64 wire format: null bitmap flag + null bitmap + scale byte + little-endian values
            // Values: 1.50 (unscaled=150, scale=2), -3.75 (unscaled=-375, scale=2), null
            byte scale = 2;
            long[] unscaledValues = {150, -375};
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 1 + unscaledValues.length * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 2);

                Unsafe.putByte(bitmapAddr + bitmapSize, scale);

                long valuesAddr = bitmapAddr + bitmapSize + 1;
                for (int i = 0; i < unscaledValues.length; i++) {
                    Unsafe.putLong(valuesAddr + (long) i * 8, unscaledValues[i]);
                }

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putDecimalToStringColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n1.50\n-3.75\n\n",
                        "SELECT value FROM test_dec_str ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutDecimalToVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            byte scale = 2;
            long[] unscaledValues = {250, -100};
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 1 + unscaledValues.length * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putByte(bitmapAddr + bitmapSize, scale);

                long valuesAddr = bitmapAddr + bitmapSize + 1;
                for (int i = 0; i < unscaledValues.length; i++) {
                    Unsafe.putLong(valuesAddr + (long) i * 8, unscaledValues[i]);
                }

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putDecimalToVarcharColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n2.50\n\n-1.00\n",
                        "SELECT value FROM test_dec_vc ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Byte_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_byte", PartitionBy.HOUR)
                    .col("value", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 100;
            long valuesAddr = Unsafe.malloc(rowCount, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putByte(valuesAddr + i, (byte) (i % 128));
                }

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 1, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                // Verify via WalReader
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals((byte) (row % 128), record.getByte(0));
                        assertEquals(timestamps[row], record.getTimestamp(1));
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, rowCount, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Byte_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_byte_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 6;
            // Pattern: [NULL, 10, NULL, 20, NULL, 30]
            int valueCount = 3;
            long valuesAddr = Unsafe.malloc(valueCount, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 2);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 4);

                Unsafe.putByte(valuesAddr, (byte) 10);
                Unsafe.putByte(valuesAddr + 1, (byte) 20);
                Unsafe.putByte(valuesAddr + 2, (byte) 30);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 1, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    byte[] expected = {0, 10, 0, 20, 0, 30};
                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals("Row " + row, expected[row], record.getByte(0));
                        row++;
                    }
                    assertEquals(6, row);
                }
            } finally {
                Unsafe.free(valuesAddr, valueCount, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Char_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_char_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.CHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 6;
            // Pattern: [NULL, 'A', NULL, 'B', NULL, 'C']
            int valueCount = 3;
            long valuesAddr = Unsafe.malloc((long) valueCount * 2, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 2);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 4);

                Unsafe.putShort(valuesAddr, (short) 'A');
                Unsafe.putShort(valuesAddr + 2, (short) 'B');
                Unsafe.putShort(valuesAddr + 4, (short) 'C');

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 2, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    char[] expected = {0, 'A', 0, 'B', 0, 'C'};
                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals("Row " + row, expected[row], record.getChar(0));
                        row++;
                    }
                    assertEquals(6, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 2, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Date_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_date", PartitionBy.HOUR)
                    .col("value", ColumnType.DATE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 100;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                long baseDate = 1_640_000_000_000L; // Milliseconds
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(valuesAddr + (long) i * 8, baseDate + i * 86_400_000L);
                }

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 8, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(baseDate + row * 86_400_000L, record.getDate(0));
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Date_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_date_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.DATE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            int valueCount = 2;
            long valuesAddr = Unsafe.malloc((long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 1);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 3);

                Unsafe.putLong(valuesAddr, 1_700_000_000_000L);
                Unsafe.putLong(valuesAddr + 8, 1_600_000_000_000L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 8, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(1_700_000_000_000L, record.getDate(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(Numbers.LONG_NULL, record.getDate(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(1_600_000_000_000L, record.getDate(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(Numbers.LONG_NULL, record.getDate(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Double_NaN_IsNull() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_double_nan", PartitionBy.HOUR)
                    .col("value", ColumnType.DOUBLE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                // NaN is the DOUBLE null sentinel, so writing it as data is indistinguishable from NULL
                Unsafe.putDouble(valuesAddr, 1.5);
                Unsafe.putDouble(valuesAddr + 8, Double.NaN);
                Unsafe.putDouble(valuesAddr + 16, 3.5);

                long baseTimestamp = 1_000_000_000L;
                long[] timestamps = {baseTimestamp, baseTimestamp + 1_000_000L, baseTimestamp + 2_000_000L};

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 8, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(1.5, record.getDouble(0), 0.0001);

                    assertTrue(cursor.hasNext());
                    assertTrue(Double.isNaN(record.getDouble(0)));

                    assertTrue(cursor.hasNext());
                    assertEquals(3.5, record.getDouble(0), 0.0001);

                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Double_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_double", PartitionBy.HOUR)
                    .col("value", ColumnType.DOUBLE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 100;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putDouble(valuesAddr + (long) i * 8, i * 2.5);
                }

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 8, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(row * 2.5, record.getDouble(0), 0.0001);
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Double_SpecialValues() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_double_special", PartitionBy.HOUR)
                    .col("value", ColumnType.DOUBLE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putDouble(valuesAddr, Double.POSITIVE_INFINITY);
                Unsafe.putDouble(valuesAddr + 8, Double.NEGATIVE_INFINITY);
                Unsafe.putDouble(valuesAddr + 16, -0.0);
                Unsafe.putDouble(valuesAddr + 24, Double.MIN_VALUE);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 8, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(Double.POSITIVE_INFINITY, record.getDouble(0), 0);

                    assertTrue(cursor.hasNext());
                    assertEquals(Double.NEGATIVE_INFINITY, record.getDouble(0), 0);

                    assertTrue(cursor.hasNext());
                    assertEquals(-0.0, record.getDouble(0), 0);

                    assertTrue(cursor.hasNext());
                    assertEquals(Double.MIN_VALUE, record.getDouble(0), 0);

                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Double_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_double_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.DOUBLE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // Pattern: [1.5, NULL, 3.5, NULL]
            int valueCount = 2;
            long valuesAddr = Unsafe.malloc((long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 1);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 3);

                Unsafe.putDouble(valuesAddr, 1.5);
                Unsafe.putDouble(valuesAddr + 8, 3.5);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 8, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(1.5, record.getDouble(0), 0.0001);

                    assertTrue(cursor.hasNext());
                    assertTrue(Double.isNaN(record.getDouble(0))); // NaN sentinel

                    assertTrue(cursor.hasNext());
                    assertEquals(3.5, record.getDouble(0), 0.0001);

                    assertTrue(cursor.hasNext());
                    assertTrue(Double.isNaN(record.getDouble(0))); // NaN sentinel

                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Float_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_float", PartitionBy.HOUR)
                    .col("value", ColumnType.FLOAT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 100;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putFloat(valuesAddr + (long) i * 4, i * 1.5f);
                }

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(row * 1.5f, record.getFloat(0), 0.0001f);
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Float_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_float_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.FLOAT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 6;
            int valueCount = 3;
            long valuesAddr = Unsafe.malloc((long) valueCount * 4, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 2);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 4);

                Unsafe.putFloat(valuesAddr, 1.5f);
                Unsafe.putFloat(valuesAddr + 4, 2.5f);
                Unsafe.putFloat(valuesAddr + 8, 3.5f);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 4, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        if (row % 2 == 0) {
                            assertTrue("Row " + row + " should be NaN (null)", Float.isNaN(record.getFloat(0)));
                        } else {
                            //noinspection IntegerDivisionInFloatingPointContext
                            assertEquals(row / 2 * 1.0f + 1.5f, record.getFloat(0), 1e-6f);
                        }
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 4, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Int_MinMax() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_int_minmax", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                // INT_NULL is Integer.MIN_VALUE, so MIN valid is MIN_VALUE + 1
                Unsafe.putInt(valuesAddr, Integer.MIN_VALUE + 1);
                Unsafe.putInt(valuesAddr + 4, 0);
                Unsafe.putInt(valuesAddr + 8, Integer.MAX_VALUE);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(Integer.MIN_VALUE + 1, record.getInt(0));

                    assertTrue(cursor.hasNext());
                    assertEquals(0, record.getInt(0));

                    assertTrue(cursor.hasNext());
                    assertEquals(Integer.MAX_VALUE, record.getInt(0));

                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Int_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_int", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 100;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(valuesAddr + (long) i * 4, i * 10);
                }

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(row * 10, record.getInt(0));
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Int_WithNulls_AllNull() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_int_allnull", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            int valueCount = 0;
            long valuesAddr = 0; // No values
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillAllNull(nullBitmapAddr, rowCount);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 4, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(Numbers.INT_NULL, record.getInt(0));
                        row++;
                    }
                    assertEquals(5, row);
                }
            } finally {
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Int_WithNulls_Alternating() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_int_alternating", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 6;
            // Pattern: [NULL, 100, NULL, 200, NULL, 300]
            int valueCount = 3;
            long valuesAddr = Unsafe.malloc((long) valueCount * 4, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 2);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 4);

                Unsafe.putInt(valuesAddr, 100);
                Unsafe.putInt(valuesAddr + 4, 200);
                Unsafe.putInt(valuesAddr + 8, 300);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 4, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int[] expected = {Numbers.INT_NULL, 100, Numbers.INT_NULL, 200, Numbers.INT_NULL, 300};
                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals("Row " + row, expected[row], record.getInt(0));
                        row++;
                    }
                    assertEquals(6, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 4, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Int_WithNulls_FirstNull() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_int_nulls1", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            // Values: [NULL, 100, 200, 300, 400]
            int valueCount = 4;
            long valuesAddr = Unsafe.malloc((long) valueCount * 4, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                // Set up null bitmap: row 0 is null
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 0);

                // Values are packed (no gap for null)
                Unsafe.putInt(valuesAddr, 100);
                Unsafe.putInt(valuesAddr + 4, 200);
                Unsafe.putInt(valuesAddr + 8, 300);
                Unsafe.putInt(valuesAddr + 12, 400);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 4, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(Numbers.INT_NULL, record.getInt(0)); // Row 0: NULL

                    assertTrue(cursor.hasNext());
                    assertEquals(100, record.getInt(0)); // Row 1

                    assertTrue(cursor.hasNext());
                    assertEquals(200, record.getInt(0)); // Row 2

                    assertTrue(cursor.hasNext());
                    assertEquals(300, record.getInt(0)); // Row 3

                    assertTrue(cursor.hasNext());
                    assertEquals(400, record.getInt(0)); // Row 4

                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 4, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Int_WithNulls_LastNull() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_int_nulls2", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            // Values: [100, 200, 300, 400, NULL]
            int valueCount = 4;
            long valuesAddr = Unsafe.malloc((long) valueCount * 4, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 4);

                Unsafe.putInt(valuesAddr, 100);
                Unsafe.putInt(valuesAddr + 4, 200);
                Unsafe.putInt(valuesAddr + 8, 300);
                Unsafe.putInt(valuesAddr + 12, 400);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 4, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int[] expected = {100, 200, 300, 400, Numbers.INT_NULL};
                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(expected[row], record.getInt(0));
                        row++;
                    }
                    assertEquals(5, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 4, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Long_MinMax() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long_minmax", PartitionBy.HOUR)
                    .col("value", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                // LONG_NULL is Long.MIN_VALUE, so MIN valid is MIN_VALUE + 1
                Unsafe.putLong(valuesAddr, Long.MIN_VALUE + 1);
                Unsafe.putLong(valuesAddr + 8, 0L);
                Unsafe.putLong(valuesAddr + 16, Long.MAX_VALUE);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 8, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(Long.MIN_VALUE + 1, record.getLong(0));

                    assertTrue(cursor.hasNext());
                    assertEquals(0L, record.getLong(0));

                    assertTrue(cursor.hasNext());
                    assertEquals(Long.MAX_VALUE, record.getLong(0));

                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Long_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long", PartitionBy.HOUR)
                    .col("value", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 100;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(valuesAddr + (long) i * 8, (long) i * 1_000_000_000L);
                }

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 8, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals((long) row * 1_000_000_000L, record.getLong(0));
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Long_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // Pattern: [1000, NULL, 3000, 4000]
            int valueCount = 3;
            long valuesAddr = Unsafe.malloc((long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 1);

                Unsafe.putLong(valuesAddr, 1000L);
                Unsafe.putLong(valuesAddr + 8, 3000L);
                Unsafe.putLong(valuesAddr + 16, 4000L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 8, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    long[] expected = {1000L, Numbers.LONG_NULL, 3000L, 4000L};
                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(expected[row], record.getLong(0));
                        row++;
                    }
                    assertEquals(4, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Short_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_short", PartitionBy.HOUR)
                    .col("value", ColumnType.SHORT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 100;
            long valuesAddr = Unsafe.malloc((long) rowCount * 2, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putShort(valuesAddr + (long) i * 2, (short) (i * 100));
                }

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 2, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals((short) (row * 100), record.getShort(0));
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 2, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_Short_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_short_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.SHORT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 6;
            // Pattern: [NULL, 100, NULL, 200, NULL, 300]
            int valueCount = 3;
            long valuesAddr = Unsafe.malloc((long) valueCount * 2, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 2);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 4);

                Unsafe.putShort(valuesAddr, (short) 100);
                Unsafe.putShort(valuesAddr + 2, (short) 200);
                Unsafe.putShort(valuesAddr + 4, (short) 300);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 2, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    short[] expected = {0, 100, 0, 200, 0, 300};
                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals("Row " + row, expected[row], record.getShort(0));
                        row++;
                    }
                    assertEquals(6, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 2, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_UUID_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_uuid", PartitionBy.HOUR)
                    .col("value", ColumnType.UUID)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 10;
            long valuesAddr = Unsafe.malloc((long) rowCount * 16, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    long offset = (long) i * 16;
                    Unsafe.putLong(valuesAddr + offset, i * 1000L);      // lo
                    Unsafe.putLong(valuesAddr + offset + 8, i * 2000L);  // hi
                }

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 16, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(row * 1000L, record.getLong128Lo(0));
                        assertEquals(row * 2000L, record.getLong128Hi(0));
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumn_UUID_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_uuid_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.UUID)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            int valueCount = 2;
            long valuesAddr = Unsafe.malloc((long) valueCount * 16, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 2);

                // UUID 1: lo=1, hi=2
                Unsafe.putLong(valuesAddr, 1L);
                Unsafe.putLong(valuesAddr + 8, 2L);
                // UUID 2: lo=3, hi=4
                Unsafe.putLong(valuesAddr + 16, 3L);
                Unsafe.putLong(valuesAddr + 24, 4L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, valueCount, 16, nullBitmapAddr, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(Numbers.LONG_NULL, record.getLong128Lo(0));
                    assertEquals(Numbers.LONG_NULL, record.getLong128Hi(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(1L, record.getLong128Lo(0));
                    assertEquals(2L, record.getLong128Hi(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(Numbers.LONG_NULL, record.getLong128Lo(0));
                    assertEquals(Numbers.LONG_NULL, record.getLong128Hi(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(3L, record.getLong128Lo(0));
                    assertEquals(4L, record.getLong128Hi(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 16, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedOtherToStringColumn_Date() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_date_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            // Date values in milliseconds since epoch
            long[] dateValues = {1_630_933_921_000L, 0L, 1_672_531_200_000L};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(dataAddress + 1 + (long) i * 8, dateValues[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DATE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedOtherToStringColumn(0, cursor, rowCount, QwpConstants.TYPE_DATE);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                2021-09-06T13:12:01.000Z
                                1970-01-01T00:00:00.000Z
                                2023-01-01T00:00:00.000Z
                                """,
                        "SELECT value FROM test_date_str ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedOtherToStringColumn_Timestamp() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_ts_other_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            // Timestamp values in microseconds since epoch
            long[] tsValues = {1_630_933_921_000_000L, 0L, 1_672_531_200_000_000L};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(dataAddress + 1 + (long) i * 8, tsValues[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_TIMESTAMP);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedOtherToStringColumn(0, cursor, rowCount, QwpConstants.TYPE_TIMESTAMP);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                2021-09-06T13:12:01.000Z
                                1970-01-01T00:00:00.000Z
                                2023-01-01T00:00:00.000Z
                                """,
                        "SELECT value FROM test_ts_other_str ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedOtherToStringColumn_TimestampNanos() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_tsn_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            // Timestamp values in nanoseconds since epoch
            long[] tsNanosValues = {1_630_933_921_000_000_000L, 0L, 1_672_531_200_000_000_000L};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(dataAddress + 1 + (long) i * 8, tsNanosValues[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_TIMESTAMP_NANOS);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedOtherToStringColumn(0, cursor, rowCount, QwpConstants.TYPE_TIMESTAMP_NANOS);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                2021-09-06T13:12:01.000Z
                                1970-01-01T00:00:00.000Z
                                2023-01-01T00:00:00.000Z
                                """,
                        "SELECT value FROM test_tsn_str ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedOtherToStringColumn_Uuid() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_uuid_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            // UUID: 550e8400-e29b-41d4-a716-446655440000
            long uuidLo = 0xa716446655440000L;
            long uuidHi = 0x550e8400e29b41d4L;

            int dataLength = 1 + rowCount * 16;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                // Row 0: the UUID
                Unsafe.putLong(dataAddress + 1, uuidLo);
                Unsafe.putLong(dataAddress + 9, uuidHi);
                // Row 1: another UUID
                Unsafe.putLong(dataAddress + 17, 0x123456789abcdef0L);
                Unsafe.putLong(dataAddress + 25, 0xfedcba9876543210L);
                // Row 2: yet another
                Unsafe.putLong(dataAddress + 33, 1L);
                Unsafe.putLong(dataAddress + 41, 2L);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_UUID);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedOtherToStringColumn(0, cursor, rowCount, QwpConstants.TYPE_UUID);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                550e8400-e29b-41d4-a716-446655440000
                                fedcba98-7654-3210-1234-56789abcdef0
                                00000000-0000-0002-0000-000000000001
                                """,
                        "SELECT value FROM test_uuid_str ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedOtherToVarcharColumn_Date() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_date_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            long[] dateValues = {1_630_933_921_000L}; // 1 non-null date in milliseconds
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 8; // 1 non-null date
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putLong(bitmapAddr + bitmapSize, dateValues[0]);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DATE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedOtherToVarcharColumn(0, cursor, rowCount, QwpConstants.TYPE_DATE);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n2021-09-06T13:12:01.000Z\n\n",
                        "SELECT value FROM test_date_vc ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedOtherToVarcharColumn_Timestamp() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_ts_other_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            long[] tsValues = {1_630_933_921_000_000L}; // 1 non-null timestamp in microseconds
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 8; // 1 non-null timestamp
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putLong(bitmapAddr + bitmapSize, tsValues[0]);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_TIMESTAMP);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedOtherToVarcharColumn(0, cursor, rowCount, QwpConstants.TYPE_TIMESTAMP);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n2021-09-06T13:12:01.000Z\n\n",
                        "SELECT value FROM test_ts_other_vc ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedOtherToVarcharColumn_TimestampNanos() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_tsn_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            long[] tsNanosValues = {1_630_933_921_000_000_000L}; // 1 non-null timestamp in nanoseconds
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 8; // 1 non-null timestamp
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putLong(bitmapAddr + bitmapSize, tsNanosValues[0]);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_TIMESTAMP_NANOS);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedOtherToVarcharColumn(0, cursor, rowCount, QwpConstants.TYPE_TIMESTAMP_NANOS);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n2021-09-06T13:12:01.000Z\n\n",
                        "SELECT value FROM test_tsn_vc ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedOtherToVarcharColumn_Uuid() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_uuid_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            long uuidLo = 0xa716446655440000L;
            long uuidHi = 0x550e8400e29b41d4L;

            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 16; // 1 non-null UUID
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putLong(bitmapAddr + bitmapSize, uuidLo);
                Unsafe.putLong(bitmapAddr + bitmapSize + 8, uuidHi);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_UUID);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedOtherToVarcharColumn(0, cursor, rowCount, QwpConstants.TYPE_UUID);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n550e8400-e29b-41d4-a716-446655440000\n\n",
                        "SELECT value FROM test_uuid_vc ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedToDecimal64Column_PrecisionOverflow() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(10, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_fixed_dec64_precision_ovf", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putLong(dataAddress + 1, 100_000_000L); // 100000000.00 exceeds DECIMAL(10,2)

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    try {
                        appender.putFixedToDecimal64Column(0, cursor, rowCount, columnType);
                        fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("decimal value overflows"));
                    }
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedToSmallDecimalColumn_Decimal16_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL16: precision 3-4 (2 bytes storage), scale 1
            int columnType = ColumnType.getDecimalType(4, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_fixed_dec16", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            long[] values = {42, -5, 0};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedToSmallDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42.0
                                -5.0
                                0.0
                                """,
                        "SELECT value FROM test_fixed_dec16 ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedToSmallDecimalColumn_Decimal16_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(4, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_fixed_dec16_nulls", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 2);

                Unsafe.putLong(bitmapAddr + bitmapSize, 7);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedToSmallDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n\n7.0\n\n",
                        "SELECT value FROM test_fixed_dec16_nulls ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedToSmallDecimalColumn_Decimal8_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL8: precision 1-2 (1 byte storage), scale 1
            int columnType = ColumnType.getDecimalType(2, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_fixed_dec8", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            long[] values = {5, -3, 0};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedToSmallDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                5.0
                                -3.0
                                0.0
                                """,
                        "SELECT value FROM test_fixed_dec8 ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedToSmallDecimalColumn_Decimal8_PrecisionOverflow() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(2, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_fixed_dec8_precision_ovf", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putLong(dataAddress + 1, 13L); // 13 -> 13.0, exceeds DECIMAL(2,1)

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    try {
                        appender.putFixedToSmallDecimalColumn(0, cursor, rowCount, columnType);
                        fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("decimal value overflows"));
                    }
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedToSmallDecimalColumn_Decimal8_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(2, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_fixed_dec8_nulls", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            // Pattern: [3, NULL, -1]
            long[] nonNullValues = {3, -1};
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + nonNullValues.length * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                long valuesStart = bitmapAddr + bitmapSize;
                for (int i = 0; i < nonNullValues.length; i++) {
                    Unsafe.putLong(valuesStart + (long) i * 8, nonNullValues[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedToSmallDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n3.0\n\n-1.0\n",
                        "SELECT value FROM test_fixed_dec8_nulls ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedToStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            long[] values = {42, -100, 0};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedToStringColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42
                                -100
                                0
                                """,
                        "SELECT value FROM test_long_str ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedToSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long_sym", PartitionBy.HOUR)
                    .col("value", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            long[] values = {1, 2, 1, 3};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedToSymbolColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                1
                                2
                                1
                                3
                                """,
                        "SELECT value FROM test_long_sym ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedToVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 2 * 8; // 2 non-null values
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putLong(bitmapAddr + bitmapSize, 999);
                Unsafe.putLong(bitmapAddr + bitmapSize + 8, -42);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedToVarcharColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n999\n\n-42\n",
                        "SELECT value FROM test_long_vc ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToDecimal128Column_NonFinite() throws Exception {
        assertPutFloatToDecimalNonFinite(
                ColumnType.getDecimalType(38, 2), "test_float_dec128_nonf",
                456.75, 2.25, "value\n\n456.75\n\n2.25\n"
        );
    }

    @Test
    public void testPutFloatToDecimal16Column_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            // precision 4 -> DECIMAL16
            int columnType = ColumnType.getDecimalType(4, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_float_dec16", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            // Values that fit in DECIMAL16 with precision=4, scale=2: range -99.99 .. 99.99
            double[] values = {1.5, 2.25, 0.0, -3.75, 99.0};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putDouble(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                1.50
                                2.25
                                0.00
                                -3.75
                                99.00
                                """,
                        "SELECT value FROM test_float_dec16 ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToDecimal16Column_NonFinite() throws Exception {
        assertPutFloatToDecimalNonFinite(
                ColumnType.getDecimalType(4, 2), "test_float_dec16_nonf",
                7.75, 2.25, "value\n\n7.75\n\n2.25\n"
        );
    }

    @Test
    public void testPutFloatToDecimal16Column_PrecisionError() throws Exception {
        assertMemoryLeak(() -> {
            // precision=4, scale=2 -> max value 99.99; 123.5 exceeds precision
            int columnType = ColumnType.getDecimalType(4, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_float_dec16_err", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putDouble(dataAddress + 1, 123.5);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    try {
                        appender.putFloatToDecimalColumn(0, cursor, rowCount, columnType);
                        fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("cannot be converted to"));
                    }
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToDecimal16Column_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            // precision 4 -> DECIMAL16
            int columnType = ColumnType.getDecimalType(4, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_float_dec16_nulls", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // Pattern: [NULL, 1.5, NULL, 2.25]
            double[] nonNullValues = {1.5, 2.25};
            int valueCount = nonNullValues.length;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + valueCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 2);

                long valuesStart = bitmapAddr + bitmapSize;
                for (int i = 0; i < valueCount; i++) {
                    Unsafe.putDouble(valuesStart + (long) i * 8, nonNullValues[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n\n1.50\n\n2.25\n",
                        "SELECT value FROM test_float_dec16_nulls ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToDecimal256Column_NonFinite() throws Exception {
        assertPutFloatToDecimalNonFinite(
                ColumnType.getDecimalType(76, 2), "test_float_dec256_nonf",
                1234.50, 2.25, "value\n\n1234.50\n\n2.25\n"
        );
    }

    @Test
    public void testPutFloatToDecimal32Column_NonFinite() throws Exception {
        assertPutFloatToDecimalNonFinite(
                ColumnType.getDecimalType(9, 2), "test_float_dec32_nonf",
                12.50, 2.25, "value\n\n12.50\n\n2.25\n"
        );
    }

    @Test
    public void testPutFloatToDecimal8Column_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            // precision 2 -> DECIMAL8
            int columnType = ColumnType.getDecimalType(2, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_float_dec8", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            // Values that fit in DECIMAL8 with precision=2, scale=1: range -9.9 .. 9.9
            double[] values = {1.5, 2.5, 0.0, -3.5, 9.0};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putDouble(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                1.5
                                2.5
                                0.0
                                -3.5
                                9.0
                                """,
                        "SELECT value FROM test_float_dec8 ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToDecimal8Column_NonFinite() throws Exception {
        assertPutFloatToDecimalNonFinite(
                ColumnType.getDecimalType(2, 1), "test_float_dec8_nonf",
                3.5, 2.5, "value\n\n3.5\n\n2.5\n"
        );
    }

    @Test
    public void testPutFloatToDecimal8Column_PrecisionError() throws Exception {
        assertMemoryLeak(() -> {
            // precision=2, scale=1 -> max value 9.9; 12.5 exceeds precision
            int columnType = ColumnType.getDecimalType(2, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_float_dec8_err", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putDouble(dataAddress + 1, 12.5);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    try {
                        appender.putFloatToDecimalColumn(0, cursor, rowCount, columnType);
                        fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("cannot be converted to"));
                    }
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToDecimal8Column_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            // precision 2 -> DECIMAL8
            int columnType = ColumnType.getDecimalType(2, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_float_dec8_nulls", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // Pattern: [NULL, 1.5, NULL, 2.5]
            double[] nonNullValues = {1.5, 2.5};
            int valueCount = nonNullValues.length;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + valueCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 2);

                long valuesStart = bitmapAddr + bitmapSize;
                for (int i = 0; i < valueCount; i++) {
                    Unsafe.putDouble(valuesStart + (long) i * 8, nonNullValues[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n\n1.5\n\n2.5\n",
                        "SELECT value FROM test_float_dec8_nulls ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToDecimalColumn_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(10, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_float_dec", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            // All values are exactly representable as doubles
            double[] values = {1.5, 2.25, 0.0, -3.75, 100.0};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putDouble(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                1.50
                                2.25
                                0.00
                                -3.75
                                100.00
                                """,
                        "SELECT value FROM test_float_dec ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToDecimalColumn_NonFinite() throws Exception {
        assertPutFloatToDecimalNonFinite(
                ColumnType.getDecimalType(18, 2), "test_float_dec_nonf",
                99.25, 2.25, "value\n\n99.25\n\n2.25\n"
        );
    }

    @Test
    public void testPutFloatToDecimalColumn_PrecisionError() throws Exception {
        assertMemoryLeak(() -> {
            // Scale=1 means only 1 decimal place; 1.25 needs 2
            int columnType = ColumnType.getDecimalType(10, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_float_dec_err", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                Unsafe.putDouble(dataAddress + 1, 1.25);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    try {
                        appender.putFloatToDecimalColumn(0, cursor, rowCount, columnType);
                        fail("Expected CairoException");
                    } catch (CairoException e) {
                        assertTrue(e.getMessage().contains("cannot be converted to"));
                    }
                    appender.cancelColumnarWrite();
                }
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToDecimalColumn_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(10, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_float_dec_nulls", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // Pattern: [NULL, 1.5, NULL, 2.25]
            double[] nonNullValues = {1.5, 2.25};
            int valueCount = nonNullValues.length;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + valueCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 0);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 2);

                long valuesStart = bitmapAddr + bitmapSize;
                for (int i = 0; i < valueCount; i++) {
                    Unsafe.putDouble(valuesStart + (long) i * 8, nonNullValues[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n\n1.50\n\n2.25\n",
                        "SELECT value FROM test_float_dec_nulls ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToNumericColumn_DoubleToInt() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_dbl_int", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            double[] values = {42.0, -100.0, 0.0};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putDouble(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToNumericColumn(0, cursor, rowCount, ColumnType.INT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42
                                -100
                                0
                                """,
                        "SELECT value FROM test_dbl_int ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToNumericColumn_NullToByte() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_dbl_byte_null", PartitionBy.HOUR)
                    .col("value", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 2 * 8; // 2 non-null doubles
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putDouble(bitmapAddr + bitmapSize, 42.0);
                Unsafe.putDouble(bitmapAddr + bitmapSize + 8, 7.0);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToNumericColumn(0, cursor, rowCount, ColumnType.BYTE);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42
                                0
                                7
                                """,
                        "SELECT value FROM test_dbl_byte_null ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToNumericColumn_NullToDouble() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_flt_dbl_null", PartitionBy.HOUR)
                    .col("value", ColumnType.DOUBLE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 2 * 4; // 2 non-null floats
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putFloat(bitmapAddr + bitmapSize, 42.0f);
                Unsafe.putFloat(bitmapAddr + bitmapSize + 4, 7.0f);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_FLOAT);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToNumericColumn(0, cursor, rowCount, ColumnType.DOUBLE);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42.0
                                null
                                7.0
                                """,
                        "SELECT value FROM test_flt_dbl_null ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToNumericColumn_NullToLong() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_dbl_long_null", PartitionBy.HOUR)
                    .col("value", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 2 * 8; // 2 non-null doubles
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putDouble(bitmapAddr + bitmapSize, 42.0);
                Unsafe.putDouble(bitmapAddr + bitmapSize + 8, 7.0);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToNumericColumn(0, cursor, rowCount, ColumnType.LONG);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42
                                null
                                7
                                """,
                        "SELECT value FROM test_dbl_long_null ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToNumericColumn_NullToShort() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_dbl_short_null", PartitionBy.HOUR)
                    .col("value", ColumnType.SHORT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 2 * 8; // 2 non-null doubles
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putDouble(bitmapAddr + bitmapSize, 42.0);
                Unsafe.putDouble(bitmapAddr + bitmapSize + 8, 7.0);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToNumericColumn(0, cursor, rowCount, ColumnType.SHORT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42
                                0
                                7
                                """,
                        "SELECT value FROM test_dbl_short_null ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_dbl_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            double[] values = {1.5, -3.14, 0.0};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putDouble(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToStringColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                1.5
                                -3.14
                                0.0
                                """,
                        "SELECT value FROM test_dbl_str ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_dbl_sym", PartitionBy.HOUR)
                    .col("value", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            double[] values = {1.5, 2.5, 1.5};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putDouble(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToSymbolColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                1.5
                                2.5
                                1.5
                                """,
                        "SELECT value FROM test_dbl_sym ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFloatToVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_dbl_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 2 * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putDouble(bitmapAddr + bitmapSize, 3.14);
                Unsafe.putDouble(bitmapAddr + bitmapSize + 8, -0.5);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToVarcharColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n3.14\n\n-0.5\n",
                        "SELECT value FROM test_dbl_vc ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutGeoHashColumn_GeoShort() throws Exception {
        assertMemoryLeak(() -> {
            int precision = 10; // 10 bits → GEOSHORT (8-15 bit range)
            int columnType = ColumnType.getGeoHashTypeWithBits(precision);
            TableToken tableToken = createTable(new TableModel(configuration, "test_geo_short", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int valueSize = (precision + 7) / 8; // ceil(10/8) = 2 bytes
            int nullCount = 1; // row 2 is null
            int nonNullCount = rowCount - nullCount;

            // Wire format: [null bitmap flag] [null bitmap] [precision varint] [packed non-null values]
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int precVarintLen = QwpVarint.encodedLength(precision);
            int dataLength = flagSize + bitmapSize + precVarintLen + nonNullCount * valueSize;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                // Null count varint
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;

                // Null bitmap: row 2 is null
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 2);

                // Precision varint
                long addr = QwpVarint.encode(bitmapAddr + bitmapSize, precision);

                // Packed values (non-null only): 2 bytes each
                // Row 0: hash 789 → base32 "sp" (s=24, p=21 → (24<<5)|21 = 789)
                Unsafe.putShort(addr, (short) 789);
                // Row 1: hash 835 → base32 "u3" (u=26, 3=3 → (26<<5)|3 = 835)
                Unsafe.putShort(addr + valueSize, (short) 835);

                QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putGeoHashColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                sp
                                u3
                                \n""",
                        "SELECT value FROM test_geo_short ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutGeoHashToStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_geo_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            // GeoHash with 5-bit precision → binary string output
            int precision = 5;
            int valueSizeBytes = (precision + 7) / 8; // ceil(5/8) = 1

            // Build wire data: varint(0) + varint(precision) + packed values (1 byte each)
            int precVarintLen = QwpVarint.encodedLength(precision);
            int dataLength = 1 + precVarintLen + rowCount * 8; // null bitmap flag + values stored as longs
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                long addr = QwpVarint.encode(dataAddress + 1, precision);
                // 5-bit values stored in 1-byte slots, but cursor uses valueSizeBytes
                Unsafe.putByte(addr, (byte) 0b10110); // row 0 → "10110"
                Unsafe.putByte(addr + valueSizeBytes, (byte) 0b11111); // row 1 → "11111"

                QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putGeoHashToStringColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                10110
                                11111
                                """,
                        "SELECT value FROM test_geo_str ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutGeoHashToStringColumn_CharPrecision() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_geo_str_char", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            // 2-character geohash = 10 bits precision
            int wirePrecision = 10;
            int valueSizeBytes = (wirePrecision + 7) / 8; // 2

            int precVarintLen = QwpVarint.encodedLength(wirePrecision);
            int dataLength = 1 + precVarintLen + rowCount * valueSizeBytes;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                long addr = QwpVarint.encode(dataAddress + 1, wirePrecision);
                // 2-char geohash "01" = (0 << 5) | 1 = 1
                Unsafe.putShort(addr, (short) 1);
                // 2-char geohash "11" = (1 << 5) | 1 = 33
                Unsafe.putShort(addr + valueSizeBytes, (short) 33);

                QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount);

                // Override precision to negative to indicate character-based encoding
                long precisionOffset = Unsafe.getFieldOffset(QwpGeoHashColumnCursor.class, "precision");
                Unsafe.putInt(cursor, precisionOffset, -2);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putGeoHashToStringColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                01
                                11
                                """,
                        "SELECT value FROM test_geo_str_char ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutGeoHashToVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_geo_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            int precision = 5; // 5-bit precision
            int valueSizeBytes = (precision + 7) / 8; // 1

            int precVarintLen = QwpVarint.encodedLength(precision);
            int dataLength = 1 + precVarintLen + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                long addr = QwpVarint.encode(dataAddress + 1, precision);
                Unsafe.putByte(addr, (byte) 0b00001); // "00001"
                Unsafe.putByte(addr + valueSizeBytes, (byte) 0b00000); // "00000"

                QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putGeoHashToVarcharColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                00001
                                00000
                                """,
                        "SELECT value FROM test_geo_vc ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutGeoHashToVarcharColumn_CharPrecision() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_geo_vc_char", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            // 2-character geohash = 10 bits precision
            int wirePrecision = 10;
            int valueSizeBytes = (wirePrecision + 7) / 8; // 2

            int precVarintLen = QwpVarint.encodedLength(wirePrecision);
            int dataLength = 1 + precVarintLen + rowCount * valueSizeBytes;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                long addr = QwpVarint.encode(dataAddress + 1, wirePrecision);
                // 2-char geohash "01" = (0 << 5) | 1 = 1
                Unsafe.putShort(addr, (short) 1);
                // 2-char geohash "11" = (1 << 5) | 1 = 33
                Unsafe.putShort(addr + valueSizeBytes, (short) 33);

                QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount);

                // Override precision to negative to indicate character-based encoding
                long precisionOffset = Unsafe.getFieldOffset(QwpGeoHashColumnCursor.class, "precision");
                Unsafe.putInt(cursor, precisionOffset, -2);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putGeoHashToVarcharColumn(0, cursor, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                01
                                11
                                """,
                        "SELECT value FROM test_geo_vc_char ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutIntegerToNumericColumn_LongToByte_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long_byte", PartitionBy.HOUR)
                    .col("value", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 2 * 8; // 2 non-null values
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putLong(bitmapAddr + bitmapSize, 42);
                Unsafe.putLong(bitmapAddr + bitmapSize + 8, -10);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putIntegerToNumericColumn(0, cursor, rowCount, ColumnType.BYTE);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42
                                0
                                -10
                                """,
                        "SELECT value FROM test_long_byte ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutIntegerToNumericColumn_LongToDouble() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long_dbl", PartitionBy.HOUR)
                    .col("value", ColumnType.DOUBLE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            long[] values = {42, -100, 0};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putIntegerToNumericColumn(0, cursor, rowCount, ColumnType.DOUBLE);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42.0
                                -100.0
                                0.0
                                """,
                        "SELECT value FROM test_long_dbl ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutIntegerToNumericColumn_LongToFloat_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long_float", PartitionBy.HOUR)
                    .col("value", ColumnType.FLOAT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 2 * 8; // 2 non-null values
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putLong(bitmapAddr + bitmapSize, 42);
                Unsafe.putLong(bitmapAddr + bitmapSize + 8, -100);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putIntegerToNumericColumn(0, cursor, rowCount, ColumnType.FLOAT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42.0
                                null
                                -100.0
                                """,
                        "SELECT value FROM test_long_float ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutIntegerToNumericColumn_LongToInt_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long_int", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 2 * 8; // 2 non-null values
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putLong(bitmapAddr + bitmapSize, 42);
                Unsafe.putLong(bitmapAddr + bitmapSize + 8, -100);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putIntegerToNumericColumn(0, cursor, rowCount, ColumnType.INT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                42
                                null
                                -100
                                """,
                        "SELECT value FROM test_long_int ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutIntegerToNumericColumn_LongToShort() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_long_short", PartitionBy.HOUR)
                    .col("value", ColumnType.SHORT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + 2 * 8; // 2 non-null values
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 1);

                Unsafe.putLong(bitmapAddr + bitmapSize, 100);
                Unsafe.putLong(bitmapAddr + bitmapSize + 8, -200);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_LONG);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putIntegerToNumericColumn(0, cursor, rowCount, ColumnType.SHORT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                100
                                0
                                -200
                                """,
                        "SELECT value FROM test_long_short ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutStringToBooleanColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_bool", PartitionBy.HOUR)
                    .col("value", ColumnType.BOOLEAN)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            String[] values = {"true", "false", "1", "0", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToBooleanColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    """
                            value
                            true
                            false
                            true
                            false
                            false
                            """,
                    "SELECT value FROM test_str_bool ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToBooleanColumn_InvalidSingleChar() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_bool_bad", PartitionBy.HOUR)
                    .col("value", ColumnType.BOOLEAN)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            String[] values = {"x"};

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                try {
                    appender.putStringToBooleanColumn(0, wireFormat.cursor, rowCount);
                    fail("Expected CairoException");
                } catch (CairoException e) {
                    assertTrue(e.getMessage().contains("cannot parse boolean from string [value=x, column=value]"));
                }
                appender.cancelColumnarWrite();
            }
        });
    }

    @Test
    public void testPutStringToDecimal128Column_ParseError() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(38, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_dec128_err", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            String[] values = {"not_a_number"};

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                try {
                    appender.putStringToDecimalColumn(0, wireFormat.cursor, rowCount, columnType);
                    fail("Expected CairoException");
                } catch (CairoException e) {
                    assertTrue(e.getMessage().contains("cannot parse decimal from string"));
                }
                appender.cancelColumnarWrite();
            }
        });
    }

    @Test
    public void testPutStringToDecimal16Column_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(4, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_dec16_nulls", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            String[] values = {"1.50", null, "2.25", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToDecimalColumn(0, wireFormat.cursor, rowCount, columnType);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\n1.50\n\n2.25\n\n",
                    "SELECT value FROM test_str_dec16_nulls ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToDecimal8Column_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(2, 1);
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_dec8_nulls", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            String[] values = {"1.5", null, "2.5", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToDecimalColumn(0, wireFormat.cursor, rowCount, columnType);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\n1.5\n\n2.5\n\n",
                    "SELECT value FROM test_str_dec8_nulls ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToDecimalColumn_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(10, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_dec", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            String[] values = {"1.50", "2.25", "0.00", "-3.75", "100.00"};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToDecimalColumn(0, wireFormat.cursor, rowCount, columnType);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    """
                            value
                            1.50
                            2.25
                            0.00
                            -3.75
                            100.00
                            """,
                    "SELECT value FROM test_str_dec ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToDecimalColumn_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(10, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_dec_nulls", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            String[] values = {"1.50", null, "2.25", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToDecimalColumn(0, wireFormat.cursor, rowCount, columnType);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\n1.50\n\n2.25\n\n",
                    "SELECT value FROM test_str_dec_nulls ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToGeoHashColumn() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getGeoHashTypeWithBits(25); // 5 chars
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_geo", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"sp052", "u33d8", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToGeoHashColumn(0, wireFormat.cursor, rowCount, columnType);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\nsp052\nu33d8\n\n",
                    "SELECT value FROM test_str_geo ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToGeoHashColumn_GeoByte() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getGeoHashTypeWithBits(5); // 1 char, GEOBYTE
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_geobyte", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"s", "u", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToGeoHashColumn(0, wireFormat.cursor, rowCount, columnType);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\ns\nu\n\n",
                    "SELECT value FROM test_str_geobyte ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToGeoHashColumn_GeoLong() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getGeoHashTypeWithBits(60); // 12 chars, GEOLONG
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_geolong", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"sp052w92p1p8", "u33d8b12dvpp", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToGeoHashColumn(0, wireFormat.cursor, rowCount, columnType);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\nsp052w92p1p8\nu33d8b12dvpp\n\n",
                    "SELECT value FROM test_str_geolong ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToGeoHashColumn_GeoShort() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getGeoHashTypeWithBits(10); // 2 chars, GEOSHORT
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_geoshort", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"sp", "u3", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToGeoHashColumn(0, wireFormat.cursor, rowCount, columnType);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\nsp\nu3\n\n",
                    "SELECT value FROM test_str_geoshort ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToLong256Column() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_l256", PartitionBy.HOUR)
                    .col("value", ColumnType.LONG256)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"0x01", "0xff", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToLong256Column(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\n0x01\n0xff\n\n",
                    "SELECT value FROM test_str_l256 ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToNumericColumn_Int() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_int", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"42", "-100", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToNumericColumn(0, wireFormat.cursor, rowCount, ColumnType.INT);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    """
                            value
                            42
                            -100
                            null
                            """,
                    "SELECT value FROM test_str_int ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToNumericColumn_Long() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_long", PartitionBy.HOUR)
                    .col("value", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"1000000000", "-1", "0"};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToNumericColumn(0, wireFormat.cursor, rowCount, ColumnType.LONG);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    """
                            value
                            1000000000
                            -1
                            0
                            """,
                    "SELECT value FROM test_str_long ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_sym", PartitionBy.HOUR)
                    .col("value", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            String[] values = {"alpha", "beta", null, "alpha"};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToSymbolColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\nalpha\nbeta\n\nalpha\n",
                    "SELECT value FROM test_str_sym ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_ts", PartitionBy.HOUR)
                    .col("value", ColumnType.TIMESTAMP)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"2021-09-06T13:12:01.000000Z", "2023-01-01T00:00:00.000000Z", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToTimestampColumn(0, wireFormat.cursor, rowCount, ColumnType.TIMESTAMP);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\n2021-09-06T13:12:01.000000Z\n2023-01-01T00:00:00.000000Z\n\n",
                    "SELECT value FROM test_str_ts ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutStringToUuidColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_str_uuid", PartitionBy.HOUR)
                    .col("value", ColumnType.UUID)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"550e8400-e29b-41d4-a716-446655440000", null, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringToUuidColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\n550e8400-e29b-41d4-a716-446655440000\n\na0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                    "SELECT value FROM test_str_uuid ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutSymbolColumn_ExistingSymbols() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_symbol_existing", PartitionBy.HOUR)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            String[] values = {"A", "B", "A", "C", "B"};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putSymbolColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    TestUtils.assertEquals(values[row], record.getSymA(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutSymbolColumn_LargeDictionary() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_symbol_large", PartitionBy.HOUR)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1000;
            String[] values = new String[rowCount];
            for (int i = 0; i < rowCount; i++) {
                values[i] = "symbol_" + i;
            }

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putSymbolColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    TestUtils.assertEquals(values[row], record.getSymA(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    /**
     * Tests that putSymbolColumn returns true on success.
     * This is the happy path - symbol resolution succeeds.
     */
    @Test
    public void testPutSymbolColumn_ReturnsTrue_OnSuccess() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_symbol_success", PartitionBy.HOUR)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"A", "B", "C"};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putSymbolColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }
        });
    }

    /**
     * Tests that putSymbolColumn throws CairoException when called on
     * a non-SYMBOL column (where symbolMapReader is null).
     */
    @Test
    public void testPutSymbolColumn_ThrowsOnNonSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            // Create table where column 0 is INT, not SYMBOL
            TableToken tableToken = createTable(new TableModel(configuration, "test_symbol_wrong_col", PartitionBy.HOUR)
                    .col("int_col", ColumnType.INT)  // NOT a symbol column!
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] symbolValues = {"A", "B", "C"};

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(symbolValues)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);

                // Try to write symbol data to an INT column - symbolMapReader will be null
                // Row-oriented path throws UnsupportedOperationException for this case
                // Columnar path should do the same
                try {
                    appender.putSymbolColumn(0, wireFormat.cursor, rowCount);
                    appender.cancelColumnarWrite();
                    fail("Expected CairoException");
                } catch (CairoException e) {
                    assertTrue(e.getMessage().contains("symbol map reader is not available"));
                    appender.cancelColumnarWrite();
                }
            }
        });
    }

    @Test
    public void testPutSymbolColumn_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_symbol_nulls", PartitionBy.HOUR)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            String[] values = {"A", null, "B", null};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putSymbolColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                assertTrue(cursor.hasNext());
                TestUtils.assertEquals("A", record.getSymA(0));

                assertTrue(cursor.hasNext());
                assertNull(record.getSymA(0));

                assertTrue(cursor.hasNext());
                TestUtils.assertEquals("B", record.getSymA(0));

                assertTrue(cursor.hasNext());
                assertNull(record.getSymA(0));

                assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testPutSymbolColumn_WithNulls_NullFlagPropagated() throws Exception {
        // Verify that NULL symbols written via the columnar path propagate
        // the null flag to the main table's symbol map after WAL apply.
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_sym_null_flag", PartitionBy.HOUR)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            String[] values = {"A", null, "B", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putSymbolColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();

            // After WAL apply, the main table's symbol map must report
            // containsNullValue()=true so that subsequent WAL writers
            // and query optimisations are aware that NULLs exist.
            try (TableReader reader = engine.getReader(tableToken)) {
                assertTrue(
                        "symbol map must report containsNullValue after columnar NULL write",
                        reader.getSymbolMapReader(0).containsNullValue()
                );
            }
        });
    }

    @Test
    public void testPutSymbolToStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_sym_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"hello", null, "world"};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putSymbolToStringColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\nhello\n\nworld\n",
                    "SELECT value FROM test_sym_str ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutSymbolToStringColumn_DeltaModeNullDictEntry() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_sym_str_delta", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            // Connection dictionary with a null entry at index 1
            ObjList<String> connectionDict = new ObjList<>();
            connectionDict.add("hello");
            connectionDict.add(null);
            connectionDict.add("world");

            // Rows: index 0 ("hello"), index 1 (null dict entry), index 2 ("world")
            int rowCount = 3;
            int[] indices = {0, 1, 2};
            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 DeltaSymbolColumnWireFormat wireFormat = new DeltaSymbolColumnWireFormat(indices, rowCount, connectionDict)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putSymbolToStringColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\nhello\n\nworld\n",
                    "SELECT value FROM test_sym_str_delta ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutSymbolToVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_sym_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"abc", "def", null};

            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putSymbolToVarcharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\nabc\ndef\n\n",
                    "SELECT value FROM test_sym_vc ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutSymbolToVarcharColumn_DeltaModeNullDictEntry() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_sym_vc_delta", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            // Connection dictionary with a null entry at index 1
            ObjList<String> connectionDict = new ObjList<>();
            connectionDict.add("abc");
            connectionDict.add(null);
            connectionDict.add("def");

            // Rows: index 0 ("abc"), index 1 (null dict entry), index 2 ("def")
            int rowCount = 3;
            int[] indices = {0, 1, 2};
            long[] timestamps = makeTimestamps(rowCount);

            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 DeltaSymbolColumnWireFormat wireFormat = new DeltaSymbolColumnWireFormat(indices, rowCount, connectionDict)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putSymbolToVarcharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            drainWalQueue();
            assertSql(
                    "value\nabc\n\ndef\n",
                    "SELECT value FROM test_sym_vc_delta ORDER BY ts"
            );
        });
    }

    @Test
    public void testPutTimestampColumn_MultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_ts_multi", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 100;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(valuesAddr + (long) i * 4, i);
                }

                long baseTimestamp = 1_640_000_000_000_000L;
                long[] timestamps = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    timestamps[i] = baseTimestamp + i * 1_000_000L;
                }

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    int row = 0;
                    while (cursor.hasNext()) {
                        assertEquals(row, record.getInt(0));
                        assertEquals(timestamps[row], record.getTimestamp(1));
                        row++;
                    }
                    assertEquals(rowCount, row);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutTimestampColumn_NonDesignated() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_ts_nondes", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .col("other_ts", ColumnType.TIMESTAMP)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(valuesAddr + (long) i * 4, i);
                }

                long baseTimestamp = 1_640_000_000_000_000L;
                long[] timestamps = new long[rowCount];
                long[] otherTimestamps = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    timestamps[i] = baseTimestamp + i * 1_000_000L;
                    otherTimestamps[i] = baseTimestamp + 100_000_000L + i * 500_000L;
                }

                long otherTsAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putLong(otherTsAddr + (long) i * 8, otherTimestamps[i]);
                    }

                    String walName;
                    try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                        walName = walWriter.getWalName();
                        ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                        appender.beginColumnarWrite(rowCount);
                        appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                        // column 1 is other_ts — a non-designated timestamp
                        appender.putTimestampColumn(1, otherTsAddr, rowCount, 0, rowCount, Numbers.LONG_NULL);
                        putTimestampColumn(appender, walWriter, timestamps, rowCount);
                        appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                        walWriter.commit();
                    }

                    try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                        RecordCursor cursor = reader.getDataCursor();
                        Record record = cursor.getRecord();

                        for (int row = 0; row < rowCount; row++) {
                            assertTrue(cursor.hasNext());
                            assertEquals(row, record.getInt(0));
                            assertEquals(otherTimestamps[row], record.getTimestamp(1));
                            assertEquals(timestamps[row], record.getTimestamp(2));
                        }
                        assertFalse(cursor.hasNext());
                    }
                } finally {
                    Unsafe.free(otherTsAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutTimestampColumn_NonDesignatedWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_ts_nondes_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .col("other_ts", ColumnType.TIMESTAMP)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // Pattern: [ts0, NULL, ts2, ts3]
            int valueCount = 3;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putInt(valuesAddr + (long) i * 4, i);
                }

                long baseTimestamp = 1_640_000_000_000_000L;
                long[] timestamps = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    timestamps[i] = baseTimestamp + i * 1_000_000L;
                }
                long[] otherTsValues = {
                        baseTimestamp + 100_000_000L,
                        baseTimestamp + 100_000_000L + 1_000_000L,
                        baseTimestamp + 100_000_000L + 2_000_000L
                };

                long otherTsAddr = Unsafe.malloc((long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
                long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < valueCount; i++) {
                        Unsafe.putLong(otherTsAddr + (long) i * 8, otherTsValues[i]);
                    }
                    QwpNullBitmapTestUtil.fillNoneNull(nullBitmapAddr, rowCount);
                    QwpNullBitmapTestUtil.setNull(nullBitmapAddr, 1);

                    String walName;
                    try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                        walName = walWriter.getWalName();
                        ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                        appender.beginColumnarWrite(rowCount);
                        appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                        // column 1 is other_ts — a non-designated timestamp with nulls
                        appender.putTimestampColumn(1, otherTsAddr, valueCount, nullBitmapAddr, rowCount, Numbers.LONG_NULL);
                        putTimestampColumn(appender, walWriter, timestamps, rowCount);
                        appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                        walWriter.commit();
                    }

                    try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                        RecordCursor cursor = reader.getDataCursor();
                        Record record = cursor.getRecord();

                        assertTrue(cursor.hasNext());
                        assertEquals(0, record.getInt(0));
                        assertEquals(otherTsValues[0], record.getTimestamp(1));

                        assertTrue(cursor.hasNext());
                        assertEquals(1, record.getInt(0));
                        assertEquals(Numbers.LONG_NULL, record.getTimestamp(1)); // NULL

                        assertTrue(cursor.hasNext());
                        assertEquals(2, record.getInt(0));
                        assertEquals(otherTsValues[1], record.getTimestamp(1));

                        assertTrue(cursor.hasNext());
                        assertEquals(3, record.getInt(0));
                        assertEquals(otherTsValues[2], record.getTimestamp(1));

                        assertFalse(cursor.hasNext());
                    }
                } finally {
                    Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(otherTsAddr, (long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutTimestampColumn_SingleRow() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_ts_single", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 1;
            long valuesAddr = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putInt(valuesAddr, 42);
                long timestamp = 1_640_000_000_000_000L;

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                    putTimestampColumn(appender, walWriter, new long[]{timestamp}, rowCount);
                    appender.endColumnarWrite(timestamp, timestamp, false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(42, record.getInt(0));
                    assertEquals(timestamp, record.getTimestamp(1));

                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutTimestampToStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_ts_str", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            // Timestamps in microseconds
            long[] tsValues = {1_630_933_921_000_000L, 0L, 1_672_531_200_000_000L};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putLong(dataAddress + 1 + (long) i * 8, tsValues[i]);
                }

                QwpTimestampColumnCursor cursor = new QwpTimestampColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_TIMESTAMP, false);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putTimestampToStringColumn(0, cursor, rowCount, QwpConstants.TYPE_TIMESTAMP);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        """
                                value
                                2021-09-06T13:12:01.000Z
                                1970-01-01T00:00:00.000Z
                                2023-01-01T00:00:00.000Z
                                """,
                        "SELECT value FROM test_ts_str ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutTimestampToVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_ts_vc", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            long[] tsValues = {1_630_933_921_000_000L, 0L};
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            int flagSize = 1;
            int dataLength = flagSize + bitmapSize + tsValues.length * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 1); // null bitmap present
                long bitmapAddr = dataAddress + 1;
                QwpNullBitmapTestUtil.fillNoneNull(bitmapAddr, rowCount);
                QwpNullBitmapTestUtil.setNull(bitmapAddr, 2);

                for (int i = 0; i < tsValues.length; i++) {
                    Unsafe.putLong(bitmapAddr + bitmapSize + (long) i * 8, tsValues[i]);
                }

                QwpTimestampColumnCursor cursor = new QwpTimestampColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_TIMESTAMP, false);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putTimestampToVarcharColumn(0, cursor, rowCount, QwpConstants.TYPE_TIMESTAMP);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(
                        "value\n2021-09-06T13:12:01.000Z\n1970-01-01T00:00:00.000Z\n\n",
                        "SELECT value FROM test_ts_vc ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutVarcharColumn_AllNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_varchar_allnulls", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {null, null, null};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putVarcharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertNull(record.getVarcharA(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutVarcharColumn_Empty() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_varchar_empty", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"", "", ""};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putVarcharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    TestUtils.assertEquals("", record.getVarcharA(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutVarcharColumn_SingleChar() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_varchar_char", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            String[] values = {"a", "b", "c", "d", "e"};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putVarcharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    TestUtils.assertEquals(values[row], record.getVarcharA(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    /**
     * Tests that STRING column type works the same as VARCHAR in the columnar path.
     * This verifies the fix for the missing STRING case in appendToWalColumnar switch.
     */
    @Test
    public void testPutVarcharColumn_StringColumnType() throws Exception {
        assertMemoryLeak(() -> {
            // Use STRING column type instead of VARCHAR
            TableToken tableToken = createTable(new TableModel(configuration, "test_string_col", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            String[] values = {"hello", "world", "test"};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                // STRING columns use putStringColumn (different storage format than VARCHAR)
                appender.putStringColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    TestUtils.assertEquals(values[row], record.getStrA(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    /**
     * Tests STRING column with nulls.
     */
    @Test
    public void testPutVarcharColumn_StringColumnType_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_string_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            String[] values = {"alpha", null, "beta", null};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putStringColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                assertTrue(cursor.hasNext());
                TestUtils.assertEquals("alpha", record.getStrA(0));

                assertTrue(cursor.hasNext());
                assertNull(record.getStrA(0));

                assertTrue(cursor.hasNext());
                TestUtils.assertEquals("beta", record.getStrA(0));

                assertTrue(cursor.hasNext());
                assertNull(record.getStrA(0));

                assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testPutVarcharColumn_Utf8Mixed() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_varchar_utf8", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            String[] values = {"hello", "世界", "🎉", "café"};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putVarcharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    TestUtils.assertEquals(values[row], record.getVarcharA(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutVarcharColumn_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_varchar_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.VARCHAR)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            String[] values = {"hello", null, "world", null};

            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken);
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putVarcharColumn(0, wireFormat.cursor, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                assertTrue(cursor.hasNext());
                TestUtils.assertEquals("hello", record.getVarcharA(0));

                assertTrue(cursor.hasNext());
                assertNull(record.getVarcharA(0));

                assertTrue(cursor.hasNext());
                TestUtils.assertEquals("world", record.getVarcharA(0));

                assertTrue(cursor.hasNext());
                assertNull(record.getVarcharA(0));

                assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testRollbackDuringColumnarWrite_Throws() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_lifecycle_rollback_guard", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(1);
                putTimestampColumn(appender, walWriter, new long[]{1_000_000L}, 1);

                try {
                    walWriter.rollback();
                    Assert.fail("Expected CairoException");
                } catch (CairoException e) {
                    assertTrue(e.getMessage().contains("cannot rollback during columnar write"));
                }
            }
        });
    }

    /**
     * Tests that UUID values are correctly read from little-endian wire format.
     * Wire format: [lo: 8 bytes little-endian] [hi: 8 bytes little-endian]
     */
    @Test
    public void testUuidLittleEndianWireFormat() throws Exception {
        assertMemoryLeak(() -> {
            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();

            // Test UUID: 550e8400-e29b-41d4-a716-446655440000
            // hi = 0x550e8400e29b41d4, lo = 0xa716446655440000
            long expectedHi = 0x550e8400e29b41d4L;
            long expectedLo = 0xa716446655440000L;

            // Wire format is little-endian: lo first, then hi
            int dataLength = 1 + 16; // null bitmap flag + 2 longs
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            try {
                // No null bitmap
                Unsafe.putByte(dataAddress, (byte) 0);

                // Write in little-endian order: lo first, then hi
                Unsafe.putLong(dataAddress + 1, expectedLo);
                Unsafe.putLong(dataAddress + 9, expectedHi);

                // Initialize cursor
                cursor.of(dataAddress, dataLength, 1, QwpConstants.TYPE_UUID);

                // Read and verify
                cursor.advanceRow();
                assertEquals("UUID hi mismatch", expectedHi, cursor.getUuidHi());
                assertEquals("UUID lo mismatch", expectedLo, cursor.getUuidLo());

                // Verify direct memory matches what columnar path would memcpy
                assertEquals("Wire lo should match", expectedLo, Unsafe.getLong(cursor.getValuesAddress()));
                assertEquals("Wire hi should match", expectedHi, Unsafe.getLong(cursor.getValuesAddress() + 8));

            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static long[] makeTimestamps(int rowCount) {
        long baseTimestamp = 1_000_000_000L;
        long[] timestamps = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            timestamps[i] = baseTimestamp + i * 1_000_000L;
        }
        return timestamps;
    }

    private void assertPutFloatToDecimalNonFinite(
            int columnType,
            String tableName,
            double validValue1,
            double validValue2,
            String expectedResult
    ) throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // [NaN, valid1, +Infinity, valid2] — non-finite values should become NULL
            double[] values = {Double.NaN, validValue1, Double.POSITIVE_INFINITY, validValue2};

            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(dataAddress, (byte) 0); // no null bitmap
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.putDouble(dataAddress + 1 + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DOUBLE);

                long[] timestamps = makeTimestamps(rowCount);

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFloatToDecimalColumn(0, cursor, rowCount, columnType);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                drainWalQueue();
                assertSql(expectedResult, "SELECT value FROM " + tableName + " ORDER BY ts");
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Writes the designated timestamp column for the given timestamps.
     */
    private void putTimestampColumn(ColumnarRowAppender appender, WalWriter walWriter, long[] timestamps, int rowCount) {
        int tsIndex = walWriter.getMetadata().getTimestampIndex();

        long tsAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putLong(tsAddr + (long) i * 8, timestamps[i]);
            }
            appender.putTimestampColumn(tsIndex, tsAddr, rowCount, 0, rowCount, Numbers.LONG_NULL);
        } finally {
            Unsafe.free(tsAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Helper to build QwpArrayColumnCursor wire format data.
     * <p>
     * Wire format:
     * <pre>
     * [null bitmap flag: 1 byte]
     * [null bitmap: ceil(rowCount/8) bytes, if flag != 0]
     * For each non-null row:
     *   [nDims: 1 byte]
     *   [dim sizes: nDims * 4 bytes, int32 LE each]
     *   [values: totalElements * 8 bytes, LE]
     * </pre>
     */
    private static class ArrayColumnWireFormat implements AutoCloseable {
        final QwpArrayColumnCursor cursor = new QwpArrayColumnCursor();
        final boolean hasNulls;
        final int nDims;
        final int rowCount;
        private final java.io.ByteArrayOutputStream dataStream = new java.io.ByteArrayOutputStream();
        private final boolean[] nullFlags;
        long dataAddress;
        int dataLength;

        ArrayColumnWireFormat(int rowCount, int nDims, boolean hasNulls) {
            this.rowCount = rowCount;
            this.nDims = nDims;
            this.hasNulls = hasNulls;
            this.nullFlags = new boolean[rowCount];
        }

        @Override
        public void close() {
            if (dataAddress != 0) {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
                dataAddress = 0;
            }
        }

        private void writeInt(int value) {
            dataStream.write(value & 0xFF);
            dataStream.write((value >> 8) & 0xFF);
            dataStream.write((value >> 16) & 0xFF);
            dataStream.write((value >> 24) & 0xFF);
        }

        private void writeLong(long value) {
            for (int i = 0; i < 8; i++) {
                dataStream.write((int) ((value >> (i * 8)) & 0xFF));
            }
        }

        void addDoubleRow(int[] shape, double[] values) {
            assert shape.length == nDims;
            dataStream.write((byte) nDims);
            for (int dimSize : shape) {
                writeInt(dimSize);
            }
            for (double v : values) {
                writeLong(Double.doubleToRawLongBits(v));
            }
        }

        void addNullRow(int rowIndex) {
            nullFlags[rowIndex] = true;
        }

        void build() throws QwpParseException {
            byte[] rowData = dataStream.toByteArray();

            int bitmapSize = hasNulls ? QwpNullBitmap.sizeInBytes(rowCount) : 0;
            dataLength = 1 + bitmapSize + rowData.length;
            dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            long addr = dataAddress;
            Unsafe.putByte(addr, (byte) (hasNulls ? 1 : 0));
            addr++;

            if (hasNulls) {
                QwpNullBitmapTestUtil.fillNoneNull(addr, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (nullFlags[i]) {
                        QwpNullBitmapTestUtil.setNull(addr, i);
                    }
                }
                addr += bitmapSize;
            }

            for (int i = 0; i < rowData.length; i++) {
                Unsafe.putByte(addr + i, rowData[i]);
            }

            byte typeCode = QwpConstants.TYPE_DOUBLE_ARRAY;
            cursor.of(dataAddress, dataLength, rowCount, typeCode);
        }
    }

    /**
     * Helper to build wire format for QwpBooleanColumnCursor.
     * Wire format:
     * - [null bitmap flag byte]
     * - [null bitmap if flag != 0]: ceil(rowCount/8) bytes
     * - [value bitmap]: ceil(valueCount/8) bytes, bit[i]=1 means true
     */
    private static class BooleanColumnWireFormat implements AutoCloseable {
        final QwpBooleanColumnCursor cursor = new QwpBooleanColumnCursor();
        long dataAddress;
        int dataLength;

        BooleanColumnWireFormat(Boolean[] values) throws QwpParseException {
            int rowCount = values.length;
            int nullCount = 0;

            for (Boolean value : values) {
                if (value == null) {
                    nullCount++;
                }
            }

            int valueCount = rowCount - nullCount;

            // Calculate sizes
            int flagSize = 1;
            int bitmapSize = nullCount > 0 ? QwpNullBitmap.sizeInBytes(rowCount) : 0;
            int valueBitmapSize = (valueCount + 7) / 8;

            dataLength = flagSize + bitmapSize + valueBitmapSize;
            dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            // Write null bitmap flag
            Unsafe.putByte(dataAddress, (byte) (nullCount > 0 ? 1 : 0));
            long addr = dataAddress + 1;

            // Write null bitmap
            if (nullCount > 0) {
                QwpNullBitmapTestUtil.fillNoneNull(addr, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (values[i] == null) {
                        QwpNullBitmapTestUtil.setNull(addr, i);
                    }
                }
            }

            // Zero value bitmap (bits are set via OR)
            long valueBitmapAddr = addr + bitmapSize;
            for (int i = 0; i < valueBitmapSize; i++) {
                Unsafe.putByte(valueBitmapAddr + i, (byte) 0);
            }
            int valueIdx = 0;
            for (int i = 0; i < rowCount; i++) {
                if (values[i] != null) {
                    if (values[i]) {
                        int byteIndex = valueIdx >>> 3;
                        int bitIndex = valueIdx & 7;
                        byte b = Unsafe.getByte(valueBitmapAddr + byteIndex);
                        b = (byte) (b | (1 << bitIndex));
                        Unsafe.putByte(valueBitmapAddr + byteIndex, b);
                    }
                    valueIdx++;
                }
            }

            // Initialize cursor
            cursor.of(dataAddress, dataLength, rowCount);
        }

        @Override
        public void close() {
            Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Helper to build delta-mode wire format for QwpSymbolColumnCursor.
     * Delta mode uses a connection-level dictionary instead of a per-column dictionary.
     * Wire format:
     * - [null bitmap flag byte]
     * - [null bitmap if flag != 0]: ceil(rowCount/8) bytes
     * - [indices]: varint per non-null value, references connection dictionary
     */
    private static class DeltaSymbolColumnWireFormat implements AutoCloseable {
        final QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
        long dataAddress;
        int dataLength;

        DeltaSymbolColumnWireFormat(int[] indices, int rowCount, ObjList<String> connectionDict) throws QwpParseException {
            int nullCount = 0;
            for (int i = 0; i < rowCount; i++) {
                if (indices[i] < 0) {
                    nullCount++;
                }
            }

            int flagSize = 1;
            int bitmapSize = nullCount > 0 ? QwpNullBitmap.sizeInBytes(rowCount) : 0;

            // Calculate indices size
            int indicesBytes = 0;
            for (int i = 0; i < rowCount; i++) {
                if (indices[i] >= 0) {
                    indicesBytes += QwpVarint.encodedLength(indices[i]);
                }
            }

            dataLength = flagSize + bitmapSize + indicesBytes;
            dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            // Write null bitmap flag
            Unsafe.putByte(dataAddress, (byte) (nullCount > 0 ? 1 : 0));
            long addr = dataAddress + 1;

            // Write null bitmap
            if (nullCount > 0) {
                QwpNullBitmapTestUtil.fillNoneNull(addr, rowCount);
            }

            // Write indices
            addr = addr + bitmapSize;
            for (int i = 0; i < rowCount; i++) {
                if (indices[i] >= 0) {
                    addr = QwpVarint.encode(addr, indices[i]);
                }
            }

            // Initialize cursor in delta mode
            cursor.of(dataAddress, dataLength, rowCount, connectionDict);
        }

        @Override
        public void close() {
            Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Helper to build wire format for QwpStringColumnCursor.
     * Wire format:
     * - [null bitmap flag byte]
     * - [null bitmap if flag != 0]: ceil(rowCount/8) bytes
     * - [offset array]: (valueCount+1) * 4 bytes, uint32 little-endian
     * - [string data]: concatenated UTF-8 bytes
     */
    private static class StringColumnWireFormat implements AutoCloseable {
        final QwpStringColumnCursor cursor = new QwpStringColumnCursor();
        long dataAddress;
        int dataLength;

        StringColumnWireFormat(String[] values) throws QwpParseException {
            // Calculate null count and prepare byte arrays
            int rowCount = values.length;
            int nullCount = 0;
            byte[][] utf8Values = new byte[rowCount][];

            for (int i = 0; i < rowCount; i++) {
                if (values[i] == null) {
                    nullCount++;
                    utf8Values[i] = null;
                } else {
                    utf8Values[i] = values[i].getBytes(StandardCharsets.UTF_8);
                }
            }

            int valueCount = rowCount - nullCount;

            // Calculate sizes
            int flagSize = 1;
            int bitmapSize = nullCount > 0 ? QwpNullBitmap.sizeInBytes(rowCount) : 0;
            int offsetArraySize = (valueCount + 1) * 4;

            int totalStringLen = 0;
            for (byte[] utf8Value : utf8Values) {
                if (utf8Value != null) {
                    totalStringLen += utf8Value.length;
                }
            }

            dataLength = flagSize + bitmapSize + offsetArraySize + totalStringLen;
            dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            // Write null bitmap flag
            Unsafe.putByte(dataAddress, (byte) (nullCount > 0 ? 1 : 0));
            long addr = dataAddress + 1;

            // Write null bitmap
            int offset = 1;
            if (nullCount > 0) {
                QwpNullBitmapTestUtil.fillNoneNull(addr, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (values[i] == null) {
                        QwpNullBitmapTestUtil.setNull(addr, i);
                    }
                }
                offset += bitmapSize;
            }

            // Write offset array and string data
            long offsetArrayAddr = dataAddress + offset;
            long stringDataAddr = offsetArrayAddr + offsetArraySize;
            int stringOffset = 0;
            int valueIdx = 0;

            for (int i = 0; i < rowCount; i++) {
                byte[] utf8 = utf8Values[i];
                if (utf8 != null) {
                    Unsafe.putInt(offsetArrayAddr + (long) valueIdx * 4, stringOffset);
                    for (byte b : utf8) {
                        Unsafe.putByte(stringDataAddr + stringOffset++, b);
                    }
                    valueIdx++;
                }
            }
            Unsafe.putInt(offsetArrayAddr + (long) valueCount * 4, stringOffset);

            // Initialize cursor
            cursor.of(dataAddress, dataLength, rowCount, (byte) 0x07);
        }

        @Override
        public void close() {
            Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Helper to build wire format for QwpSymbolColumnCursor.
     * Wire format:
     * - [null bitmap flag byte]
     * - [null bitmap if flag != 0]: ceil(rowCount/8) bytes
     * - [dictionary size]: varint
     * - For each dictionary entry: [string length]: varint, [string data]: UTF-8 bytes
     * - [indices]: varint per non-null value, references dictionary entry
     */
    private static class SymbolColumnWireFormat implements AutoCloseable {
        final QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
        long dataAddress;
        int dataLength;

        SymbolColumnWireFormat(String[] values) throws QwpParseException {
            int rowCount = values.length;

            // Build dictionary
            java.util.Map<String, Integer> dictMap = new java.util.LinkedHashMap<>();
            java.util.List<String> dictList = new java.util.ArrayList<>();
            int[] indices = new int[rowCount];
            int nullCount = 0;

            for (int i = 0; i < rowCount; i++) {
                if (values[i] == null) {
                    indices[i] = -1;
                    nullCount++;
                } else {
                    Integer idx = dictMap.get(values[i]);
                    if (idx == null) {
                        idx = dictList.size();
                        dictMap.put(values[i], idx);
                        dictList.add(values[i]);
                    }
                    indices[i] = idx;
                }
            }

            int dictionarySize = dictList.size();

            // Calculate sizes
            int flagSize = 1;
            int bitmapSize = nullCount > 0 ? QwpNullBitmap.sizeInBytes(rowCount) : 0;

            // Calculate dictionary size in bytes
            int dictBytes = QwpVarint.encodedLength(dictionarySize);
            for (String s : dictList) {
                byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
                dictBytes += QwpVarint.encodedLength(utf8.length) + utf8.length;
            }

            // Calculate indices size
            int indicesBytes = 0;
            for (int i = 0; i < rowCount; i++) {
                if (indices[i] >= 0) {
                    indicesBytes += QwpVarint.encodedLength(indices[i]);
                }
            }

            dataLength = flagSize + bitmapSize + dictBytes + indicesBytes;
            dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            // Write null bitmap flag
            Unsafe.putByte(dataAddress, (byte) (nullCount > 0 ? 1 : 0));
            long writeAddr = dataAddress + 1;

            // Write null bitmap
            int offset = 1;
            if (nullCount > 0) {
                QwpNullBitmapTestUtil.fillNoneNull(writeAddr, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (values[i] == null) {
                        QwpNullBitmapTestUtil.setNull(writeAddr, i);
                    }
                }
                offset += bitmapSize;
            }

            // Write dictionary
            long addr = dataAddress + offset;
            addr = QwpVarint.encode(addr, dictionarySize);
            for (String s : dictList) {
                byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
                addr = QwpVarint.encode(addr, utf8.length);
                for (byte b : utf8) {
                    Unsafe.putByte(addr++, b);
                }
            }

            // Write indices
            for (int i = 0; i < rowCount; i++) {
                if (indices[i] >= 0) {
                    addr = QwpVarint.encode(addr, indices[i]);
                }
            }

            // Initialize cursor
            cursor.of(dataAddress, dataLength, rowCount);
        }

        @Override
        public void close() {
            Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
        }
    }
}

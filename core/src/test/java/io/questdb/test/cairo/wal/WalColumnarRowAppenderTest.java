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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.wal.ColumnarRowAppender;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.qwp.protocol.QwpBooleanColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpDecimalColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpNullBitmap;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpSymbolColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
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
            long baseTimestamp = 1000000000L;

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
                walWriter.addColumn("col_b", ColumnType.LONG, null);

                // Now try columnar write - this should trigger segment roll in beginColumnarWrite()
                // Before the fix, this would fail with AssertionError in TableUtils.mapRW()
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                long valuesAddrA = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
                long valuesAddrB = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.getUnsafe().putLong(valuesAddrA + (long) i * 8, 1000L + i);
                        Unsafe.getUnsafe().putLong(valuesAddrB + (long) i * 8, 2000L + i);
                    }

                    long[] timestamps = new long[rowCount];
                    for (int i = 0; i < rowCount; i++) {
                        timestamps[i] = baseTimestamp + (i + 1) * 1000000L;
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
                    "select col_a, col_b from test_segment_roll order by ts"
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
                    Unsafe.getUnsafe().putInt(valuesAddr + (long) i * 4, i * 100);
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
                    Unsafe.getUnsafe().putInt(valuesAddr + (long) i * 4, i * 100);
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
            long baseTimestamp = 1000000000L;
            for (int i = 0; i < rowCount; i++) {
                values[i] = i * 10;
                timestamps[i] = baseTimestamp + i * 1000000L;
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
                    Unsafe.getUnsafe().putInt(valuesAddr + (long) i * 4, values[i]);
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
                QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmap.setNull(nullBitmapAddr, 0);
                QwpNullBitmap.setNull(nullBitmapAddr, 2);
                QwpNullBitmap.setNull(nullBitmapAddr, 4);

                Unsafe.getUnsafe().putInt(valuesAddr, 100);
                Unsafe.getUnsafe().putInt(valuesAddr + 4, 200);
                Unsafe.getUnsafe().putInt(valuesAddr + 8, 300);

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

            int rowCount = 100000;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.getUnsafe().putInt(valuesAddr + (long) i * 4, i);
                }

                long baseTimestamp = 1000000000L;
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
                        Unsafe.getUnsafe().putInt(batch1Addr + (long) i * 4, i * 10);
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
                        Unsafe.getUnsafe().putInt(batch2Addr + (long) i * 4, (batch1Count + i) * 10);
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
                    Unsafe.getUnsafe().putInt(valuesAddr + (long) i * 4, i * 100);
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
                Unsafe.getUnsafe().putInt(valuesAddr, 42);
                long timestamp = 1000000000L;

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

            long[] timestamps = {1000000L, 1001000L};

            // Build wire format with Gorilla encoding
            // Wire format: [encoding byte (0x01)] [first timestamp] [second timestamp]
            int dataLength = 1 + 8 + 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            try {
                // Write Gorilla encoding flag
                Unsafe.getUnsafe().putByte(dataAddress, (byte) 0x01); // ENCODING_GORILLA

                // Write first two timestamps (required for Gorilla)
                Unsafe.getUnsafe().putLong(dataAddress + 1, timestamps[0]);
                Unsafe.getUnsafe().putLong(dataAddress + 9, timestamps[1]);

                // Initialize cursor with gorillaEnabled=true
                tsCursor.of(dataAddress, dataLength, 2, QwpConstants.TYPE_TIMESTAMP,
                        false, true);

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
            int dataLength = 32; // 4 longs
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            try {
                // Write in little-endian order
                Unsafe.getUnsafe().putLong(dataAddress, expected0);
                Unsafe.getUnsafe().putLong(dataAddress + 8, expected1);
                Unsafe.getUnsafe().putLong(dataAddress + 16, expected2);
                Unsafe.getUnsafe().putLong(dataAddress + 24, expected3);

                // Initialize cursor
                cursor.of(dataAddress, 1, QwpConstants.TYPE_LONG256,
                        false);

                // Read and verify
                cursor.advanceRow();
                assertEquals("LONG256_0 mismatch", expected0, cursor.getLong256_0());
                assertEquals("LONG256_1 mismatch", expected1, cursor.getLong256_1());
                assertEquals("LONG256_2 mismatch", expected2, cursor.getLong256_2());
                assertEquals("LONG256_3 mismatch", expected3, cursor.getLong256_3());

                // Verify direct memory matches what columnar path would memcpy
                long valuesAddr = cursor.getValuesAddress();
                assertEquals("Wire long0 should match", expected0, Unsafe.getUnsafe().getLong(valuesAddr));
                assertEquals("Wire long1 should match", expected1, Unsafe.getUnsafe().getLong(valuesAddr + 8));
                assertEquals("Wire long2 should match", expected2, Unsafe.getUnsafe().getLong(valuesAddr + 16));
                assertEquals("Wire long3 should match", expected3, Unsafe.getUnsafe().getLong(valuesAddr + 24));

            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
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
                    Unsafe.getUnsafe().putInt(intAddr + (long) i * 4, i * 10);
                    Unsafe.getUnsafe().putLong(longAddr + (long) i * 8, (long) i * 1000);
                    Unsafe.getUnsafe().putDouble(doubleAddr + (long) i * 8, i * 1.5);
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
                     StringColumnWireFormat varcharWire = new StringColumnWireFormat(varcharValues, false);
                     SymbolColumnWireFormat symbolWire = new SymbolColumnWireFormat(symbolValues, false);
                     BooleanColumnWireFormat boolWire = new BooleanColumnWireFormat(boolValues, false)) {
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
                        assertEquals(symbolValues[row], record.getSymA(4));
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
                 BooleanColumnWireFormat wireFormat = new BooleanColumnWireFormat(values, false)) {
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
                 BooleanColumnWireFormat wireFormat = new BooleanColumnWireFormat(values, false)) {
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
                 BooleanColumnWireFormat wireFormat = new BooleanColumnWireFormat(values, false)) {
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
                 BooleanColumnWireFormat wireFormat = new BooleanColumnWireFormat(values, true)) {
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
    public void testPutDecimalColumn_Decimal256ToDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            int columnType = ColumnType.getDecimalType(18, 2);
            TableToken tableToken = createTable(new TableModel(configuration, "test_dec256_to_dec64", PartitionBy.HOUR)
                    .col("value", columnType)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            // Wire format: [scale: 1 byte][values: rowCount * 32 bytes, big-endian]
            int dataLength = 1 + rowCount * 32;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(dataAddress, (byte) 2);
                // Value 1: 12345 (= 123.45), sign-extended to 256-bit big-endian (hh, hl, lh, ll)
                long offset = 1;
                Unsafe.getUnsafe().putLong(dataAddress + offset, Long.reverseBytes(0L));
                Unsafe.getUnsafe().putLong(dataAddress + offset + 8, Long.reverseBytes(0L));
                Unsafe.getUnsafe().putLong(dataAddress + offset + 16, Long.reverseBytes(0L));
                Unsafe.getUnsafe().putLong(dataAddress + offset + 24, Long.reverseBytes(12345L));
                // Value 2: -9999 (= -99.99), sign-extended
                offset = 33;
                Unsafe.getUnsafe().putLong(dataAddress + offset, Long.reverseBytes(-1L));
                Unsafe.getUnsafe().putLong(dataAddress + offset + 8, Long.reverseBytes(-1L));
                Unsafe.getUnsafe().putLong(dataAddress + offset + 16, Long.reverseBytes(-1L));
                Unsafe.getUnsafe().putLong(dataAddress + offset + 24, Long.reverseBytes(-9999L));

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL256,
                        false);

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
            // Wire format: [scale: 1 byte][values: rowCount * 8 bytes, big-endian]
            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(dataAddress, (byte) 2);
                // Values: 12345 (= 123.45), -9999 (= -99.99) in big-endian
                Unsafe.getUnsafe().putLong(dataAddress + 1, Long.reverseBytes(12345L));
                Unsafe.getUnsafe().putLong(dataAddress + 9, Long.reverseBytes(-9999L));

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64,
                        false);

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
            int dataLength = 1 + rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                // Wire scale = 2
                Unsafe.getUnsafe().putByte(dataAddress, (byte) 2);
                // Values: 12345 (= 123.45), -9999 (= -99.99) in big-endian
                Unsafe.getUnsafe().putLong(dataAddress + 1, Long.reverseBytes(12345L));
                Unsafe.getUnsafe().putLong(dataAddress + 9, Long.reverseBytes(-9999L));

                QwpDecimalColumnCursor cursor = new QwpDecimalColumnCursor();
                cursor.of(dataAddress, dataLength, rowCount, QwpConstants.TYPE_DECIMAL64,
                        false);

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
    public void testPutFixedColumnNarrowing_Byte_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_byte", PartitionBy.HOUR)
                    .col("value", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            // Source data: LONG (8 bytes each)
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putLong(valuesAddr, 0L);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, 1L);
                Unsafe.getUnsafe().putLong(valuesAddr + 16, 127L);
                Unsafe.getUnsafe().putLong(valuesAddr + 24, -128L);
                Unsafe.getUnsafe().putLong(valuesAddr + 32, -1L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, rowCount, 8, 0, rowCount, ColumnType.BYTE);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals((byte) 0, record.getByte(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((byte) 1, record.getByte(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((byte) 127, record.getByte(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((byte) -128, record.getByte(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((byte) -1, record.getByte(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumnNarrowing_Byte_Overflow() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_byte_overflow", PartitionBy.HOUR)
                    .col("value", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                // Values outside byte range [-128, 127]
                Unsafe.getUnsafe().putLong(valuesAddr, 200L);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, -200L);
                Unsafe.getUnsafe().putLong(valuesAddr + 16, 300L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, rowCount, 8, 0, rowCount, ColumnType.BYTE);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                // Narrowing uses Java cast semantics — values wrap silently
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals((byte) 200, record.getByte(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((byte) -200, record.getByte(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((byte) 300, record.getByte(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumnNarrowing_Byte_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_byte_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            int valueCount = 3; // Only 3 non-null values
            // Values: [NULL, 42, NULL, 100, -50]
            long valuesAddr = Unsafe.malloc((long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmap.setNull(nullBitmapAddr, 0);
                QwpNullBitmap.setNull(nullBitmapAddr, 2);

                // Packed values (no gaps for nulls)
                Unsafe.getUnsafe().putLong(valuesAddr, 42L);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, 100L);
                Unsafe.getUnsafe().putLong(valuesAddr + 16, -50L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, valueCount, 8, nullBitmapAddr, rowCount, ColumnType.BYTE);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals((byte) 0, record.getByte(0)); // NULL sentinel for BYTE
                    assertTrue(cursor.hasNext());
                    assertEquals((byte) 42, record.getByte(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((byte) 0, record.getByte(0)); // NULL sentinel for BYTE
                    assertTrue(cursor.hasNext());
                    assertEquals((byte) 100, record.getByte(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((byte) -50, record.getByte(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumnNarrowing_Float_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_float", PartitionBy.HOUR)
                    .col("value", ColumnType.FLOAT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            // Source data: DOUBLE (8 bytes each)
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putDouble(valuesAddr, 0.0);
                Unsafe.getUnsafe().putDouble(valuesAddr + 8, 3.14159);
                Unsafe.getUnsafe().putDouble(valuesAddr + 16, -2.71828);
                Unsafe.getUnsafe().putDouble(valuesAddr + 24, Float.MAX_VALUE);
                Unsafe.getUnsafe().putDouble(valuesAddr + 32, Float.MIN_VALUE);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, rowCount, 8, 0, rowCount, ColumnType.FLOAT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(0.0f, record.getFloat(0), 0.0001f);
                    assertTrue(cursor.hasNext());
                    assertEquals(3.14159f, record.getFloat(0), 0.0001f);
                    assertTrue(cursor.hasNext());
                    assertEquals(-2.71828f, record.getFloat(0), 0.0001f);
                    assertTrue(cursor.hasNext());
                    assertEquals(Float.MAX_VALUE, record.getFloat(0), 0.0001f);
                    assertTrue(cursor.hasNext());
                    assertEquals(Float.MIN_VALUE, record.getFloat(0), 0.0001f);
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumnNarrowing_Float_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_float_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.FLOAT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            int valueCount = 2;
            // Values: [NULL, 1.5, NULL, -2.5]
            long valuesAddr = Unsafe.malloc((long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmap.setNull(nullBitmapAddr, 0);
                QwpNullBitmap.setNull(nullBitmapAddr, 2);

                Unsafe.getUnsafe().putDouble(valuesAddr, 1.5);
                Unsafe.getUnsafe().putDouble(valuesAddr + 8, -2.5);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, valueCount, 8, nullBitmapAddr, rowCount, ColumnType.FLOAT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertTrue(Float.isNaN(record.getFloat(0))); // NULL sentinel for FLOAT
                    assertTrue(cursor.hasNext());
                    assertEquals(1.5f, record.getFloat(0), 0.0001f);
                    assertTrue(cursor.hasNext());
                    assertTrue(Float.isNaN(record.getFloat(0))); // NULL sentinel for FLOAT
                    assertTrue(cursor.hasNext());
                    assertEquals(-2.5f, record.getFloat(0), 0.0001f);
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumnNarrowing_Int_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_int", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putLong(valuesAddr, 0L);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, 123456789L);
                Unsafe.getUnsafe().putLong(valuesAddr + 16, 2147483647L); // INT_MAX
                Unsafe.getUnsafe().putLong(valuesAddr + 24, -2147483647L); // INT_MIN + 1 (INT_MIN is the null sentinel)
                Unsafe.getUnsafe().putLong(valuesAddr + 32, -1L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, rowCount, 8, 0, rowCount, ColumnType.INT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(0, record.getInt(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(123456789, record.getInt(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(Integer.MAX_VALUE, record.getInt(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(Integer.MIN_VALUE + 1, record.getInt(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(-1, record.getInt(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumnNarrowing_Int_Overflow() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_int_overflow", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 2;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                // Values outside int range [-2147483648, 2147483647]
                Unsafe.getUnsafe().putLong(valuesAddr, 3_000_000_000L);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, -3_000_000_000L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, rowCount, 8, 0, rowCount, ColumnType.INT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                // Narrowing uses Java cast semantics — values wrap silently
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals((int) 3_000_000_000L, record.getInt(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((int) -3_000_000_000L, record.getInt(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumnNarrowing_Int_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_int_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            int valueCount = 2;
            // Values: [NULL, 999, NULL, -888]
            long valuesAddr = Unsafe.malloc((long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmap.setNull(nullBitmapAddr, 0);
                QwpNullBitmap.setNull(nullBitmapAddr, 2);

                Unsafe.getUnsafe().putLong(valuesAddr, 999L);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, -888L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, valueCount, 8, nullBitmapAddr, rowCount, ColumnType.INT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals(Numbers.INT_NULL, record.getInt(0)); // NULL sentinel for INT
                    assertTrue(cursor.hasNext());
                    assertEquals(999, record.getInt(0));
                    assertTrue(cursor.hasNext());
                    assertEquals(Numbers.INT_NULL, record.getInt(0)); // NULL sentinel for INT
                    assertTrue(cursor.hasNext());
                    assertEquals(-888, record.getInt(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumnNarrowing_Short_NoNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_short", PartitionBy.HOUR)
                    .col("value", ColumnType.SHORT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putLong(valuesAddr, 0L);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, 1000L);
                Unsafe.getUnsafe().putLong(valuesAddr + 16, 32767L);
                Unsafe.getUnsafe().putLong(valuesAddr + 24, -32768L);
                Unsafe.getUnsafe().putLong(valuesAddr + 32, -1L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, rowCount, 8, 0, rowCount, ColumnType.SHORT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals((short) 0, record.getShort(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((short) 1000, record.getShort(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((short) 32767, record.getShort(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((short) -32768, record.getShort(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((short) -1, record.getShort(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumnNarrowing_Short_Overflow() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_short_overflow", PartitionBy.HOUR)
                    .col("value", ColumnType.SHORT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 3;
            long valuesAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            try {
                // Values outside short range [-32768, 32767]
                Unsafe.getUnsafe().putLong(valuesAddr, 40_000L);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, -40_000L);
                Unsafe.getUnsafe().putLong(valuesAddr + 16, 100_000L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, rowCount, 8, 0, rowCount, ColumnType.SHORT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                // Narrowing uses Java cast semantics — values wrap silently
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals((short) 40_000, record.getShort(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((short) -40_000, record.getShort(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((short) 100_000, record.getShort(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutFixedColumnNarrowing_Short_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_narrow_short_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.SHORT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            int valueCount = 2;
            // Values: [NULL, 1234, NULL, -5678]
            long valuesAddr = Unsafe.malloc((long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmap.setNull(nullBitmapAddr, 0);
                QwpNullBitmap.setNull(nullBitmapAddr, 2);

                Unsafe.getUnsafe().putLong(valuesAddr, 1234L);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, -5678L);

                long[] timestamps = makeTimestamps(rowCount);

                String walName;
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    walName = walWriter.getWalName();
                    ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                    appender.beginColumnarWrite(rowCount);
                    appender.putFixedColumnNarrowing(0, valuesAddr, valueCount, 8, nullBitmapAddr, rowCount, ColumnType.SHORT);
                    putTimestampColumn(appender, walWriter, timestamps, rowCount);
                    appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                    RecordCursor cursor = reader.getDataCursor();
                    Record record = cursor.getRecord();

                    assertTrue(cursor.hasNext());
                    assertEquals((short) 0, record.getShort(0)); // NULL sentinel for SHORT
                    assertTrue(cursor.hasNext());
                    assertEquals((short) 1234, record.getShort(0));
                    assertTrue(cursor.hasNext());
                    assertEquals((short) 0, record.getShort(0)); // NULL sentinel for SHORT
                    assertTrue(cursor.hasNext());
                    assertEquals((short) -5678, record.getShort(0));
                    assertFalse(cursor.hasNext());
                }
            } finally {
                Unsafe.free(valuesAddr, (long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
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
                    Unsafe.getUnsafe().putByte(valuesAddr + i, (byte) (i % 128));
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
                long baseDate = 1640000000000L; // Milliseconds
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.getUnsafe().putLong(valuesAddr + (long) i * 8, baseDate + i * 86400000L);
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
                        assertEquals(baseDate + row * 86400000L, record.getDate(0));
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
                Unsafe.getUnsafe().putDouble(valuesAddr, 1.5);
                Unsafe.getUnsafe().putDouble(valuesAddr + 8, Double.NaN);
                Unsafe.getUnsafe().putDouble(valuesAddr + 16, 3.5);

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
                    Unsafe.getUnsafe().putDouble(valuesAddr + (long) i * 8, i * 2.5);
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
                Unsafe.getUnsafe().putDouble(valuesAddr, Double.POSITIVE_INFINITY);
                Unsafe.getUnsafe().putDouble(valuesAddr + 8, Double.NEGATIVE_INFINITY);
                Unsafe.getUnsafe().putDouble(valuesAddr + 16, -0.0);
                Unsafe.getUnsafe().putDouble(valuesAddr + 24, Double.MIN_VALUE);

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
                QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmap.setNull(nullBitmapAddr, 1);
                QwpNullBitmap.setNull(nullBitmapAddr, 3);

                Unsafe.getUnsafe().putDouble(valuesAddr, 1.5);
                Unsafe.getUnsafe().putDouble(valuesAddr + 8, 3.5);

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
                    Unsafe.getUnsafe().putFloat(valuesAddr + (long) i * 4, i * 1.5f);
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
                Unsafe.getUnsafe().putInt(valuesAddr, Integer.MIN_VALUE + 1);
                Unsafe.getUnsafe().putInt(valuesAddr + 4, 0);
                Unsafe.getUnsafe().putInt(valuesAddr + 8, Integer.MAX_VALUE);

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
                    Unsafe.getUnsafe().putInt(valuesAddr + (long) i * 4, i * 10);
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
                QwpNullBitmap.fillAllNull(nullBitmapAddr, rowCount);

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
                QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmap.setNull(nullBitmapAddr, 0);
                QwpNullBitmap.setNull(nullBitmapAddr, 2);
                QwpNullBitmap.setNull(nullBitmapAddr, 4);

                Unsafe.getUnsafe().putInt(valuesAddr, 100);
                Unsafe.getUnsafe().putInt(valuesAddr + 4, 200);
                Unsafe.getUnsafe().putInt(valuesAddr + 8, 300);

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
                QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmap.setNull(nullBitmapAddr, 0);

                // Values are packed (no gap for null)
                Unsafe.getUnsafe().putInt(valuesAddr, 100);
                Unsafe.getUnsafe().putInt(valuesAddr + 4, 200);
                Unsafe.getUnsafe().putInt(valuesAddr + 8, 300);
                Unsafe.getUnsafe().putInt(valuesAddr + 12, 400);

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
                QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmap.setNull(nullBitmapAddr, 4);

                Unsafe.getUnsafe().putInt(valuesAddr, 100);
                Unsafe.getUnsafe().putInt(valuesAddr + 4, 200);
                Unsafe.getUnsafe().putInt(valuesAddr + 8, 300);
                Unsafe.getUnsafe().putInt(valuesAddr + 12, 400);

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
                Unsafe.getUnsafe().putLong(valuesAddr, Long.MIN_VALUE + 1);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, 0L);
                Unsafe.getUnsafe().putLong(valuesAddr + 16, Long.MAX_VALUE);

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
                    Unsafe.getUnsafe().putLong(valuesAddr + (long) i * 8, (long) i * 1000000000L);
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
                        assertEquals((long) row * 1000000000L, record.getLong(0));
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
                QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                QwpNullBitmap.setNull(nullBitmapAddr, 1);

                Unsafe.getUnsafe().putLong(valuesAddr, 1000L);
                Unsafe.getUnsafe().putLong(valuesAddr + 8, 3000L);
                Unsafe.getUnsafe().putLong(valuesAddr + 16, 4000L);

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
                    Unsafe.getUnsafe().putShort(valuesAddr + (long) i * 2, (short) (i * 100));
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
                    Unsafe.getUnsafe().putLong(valuesAddr + offset, i * 1000L);      // lo
                    Unsafe.getUnsafe().putLong(valuesAddr + offset + 8, i * 2000L);  // hi
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

            int dataLength = rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.getUnsafe().putDouble(dataAddress + (long) i * 8, values[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, rowCount, QwpConstants.TYPE_DOUBLE,
                        false);

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
            int dataLength = rowCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putDouble(dataAddress, 1.25);

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, rowCount, QwpConstants.TYPE_DOUBLE,
                        false);

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
            int dataLength = bitmapSize + valueCount * 8;
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpNullBitmap.fillNoneNull(dataAddress, rowCount);
                QwpNullBitmap.setNull(dataAddress, 0);
                QwpNullBitmap.setNull(dataAddress, 2);

                long valuesStart = dataAddress + bitmapSize;
                for (int i = 0; i < valueCount; i++) {
                    Unsafe.getUnsafe().putDouble(valuesStart + (long) i * 8, nonNullValues[i]);
                }

                QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
                cursor.of(dataAddress, rowCount, QwpConstants.TYPE_DOUBLE,
                        true);

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
                                """,
                        "SELECT value FROM test_float_dec_nulls ORDER BY ts"
                );
            } finally {
                Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPutNullColumn_Double() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_null_double", PartitionBy.HOUR)
                    .col("value", ColumnType.DOUBLE)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putNullColumn(0, ColumnType.DOUBLE, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertTrue(Double.isNaN(record.getDouble(0)));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutNullColumn_Int() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_null_int", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putNullColumn(0, ColumnType.INT, rowCount);
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
                assertEquals(rowCount, row);
            }
        });
    }

    @Test
    public void testPutNullColumn_Long() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_null_long", PartitionBy.HOUR)
                    .col("value", ColumnType.LONG)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 5;
            long[] timestamps = makeTimestamps(rowCount);

            String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                appender.putNullColumn(0, ColumnType.LONG, rowCount);
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertEquals(Numbers.LONG_NULL, record.getLong(0));
                    row++;
                }
                assertEquals(rowCount, row);
            }
        });
    }

    /**
     * Tests that putNullColumn throws CairoException for unsupported
     * column types (like BINARY).
     */
    @Test
    public void testPutNullColumn_ThrowsForUnsupportedType() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with a BINARY column - not supported by columnar path
            TableToken tableToken = createTable(new TableModel(configuration, "test_binary_null", PartitionBy.HOUR)
                    .col("data", ColumnType.BINARY)
                    .timestamp("ts")
                    .wal()
            );

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(1);

                // Trying to write nulls for BINARY should throw - we don't support this type
                try {
                    appender.putNullColumn(0, ColumnType.BINARY, 1);
                    fail("Expected CairoException for BINARY column");
                } catch (CairoException e) {
                    assertTrue(e.getMessage().contains("BINARY"));
                }

                appender.cancelColumnarWrite();
            }
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
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values, false)) {
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
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values, true)) {
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
                            
                            """,
                    "SELECT value FROM test_str_dec_nulls ORDER BY ts"
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
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values, false)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                assertTrue(appender.putSymbolColumn(0, wireFormat.cursor, rowCount));
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertEquals(values[row], record.getSymA(0));
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
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values, false)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                assertTrue(appender.putSymbolColumn(0, wireFormat.cursor, rowCount));
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                int row = 0;
                while (cursor.hasNext()) {
                    assertEquals(values[row], record.getSymA(0));
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
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values, false)) {
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                // Verify putSymbolColumn returns true on success
                boolean result = appender.putSymbolColumn(0, wireFormat.cursor, rowCount);
                assertTrue("putSymbolColumn should return true on success", result);
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
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(symbolValues, false)) {
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
                 SymbolColumnWireFormat wireFormat = new SymbolColumnWireFormat(values, true)) {
                walName = walWriter.getWalName();
                ColumnarRowAppender appender = walWriter.getColumnarRowAppender();

                appender.beginColumnarWrite(rowCount);
                assertTrue(appender.putSymbolColumn(0, wireFormat.cursor, rowCount));
                putTimestampColumn(appender, walWriter, timestamps, rowCount);
                appender.endColumnarWrite(timestamps[0], timestamps[rowCount - 1], false);

                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();

                assertTrue(cursor.hasNext());
                assertEquals("A", record.getSymA(0));

                assertTrue(cursor.hasNext());
                assertNull(record.getSymA(0));

                assertTrue(cursor.hasNext());
                assertEquals("B", record.getSymA(0));

                assertTrue(cursor.hasNext());
                assertNull(record.getSymA(0));

                assertFalse(cursor.hasNext());
            }
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
                    Unsafe.getUnsafe().putInt(valuesAddr + (long) i * 4, i);
                }

                long baseTimestamp = 1640000000000000L;
                long[] timestamps = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    timestamps[i] = baseTimestamp + i * 1000000L;
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
                Unsafe.getUnsafe().putInt(valuesAddr, 42);
                long timestamp = 1640000000000000L;

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
    public void testPutTimestampColumn_WithNulls() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(new TableModel(configuration, "test_ts_nulls", PartitionBy.HOUR)
                    .col("value", ColumnType.INT)
                    .timestamp("ts")
                    .wal()
            );

            int rowCount = 4;
            // Pattern: [ts0, NULL, ts2, ts3]
            int valueCount = 3;
            long valuesAddr = Unsafe.malloc((long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < rowCount; i++) {
                    Unsafe.getUnsafe().putInt(valuesAddr + (long) i * 4, i);
                }

                long baseTimestamp = 1640000000000000L;
                long[] timestampValues = {baseTimestamp, baseTimestamp + 2000000L, baseTimestamp + 3000000L};

                int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
                long nullBitmapAddr = Unsafe.malloc(bitmapSize, MemoryTag.NATIVE_DEFAULT);
                long tsAddr = Unsafe.malloc((long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                try {
                    QwpNullBitmap.fillNoneNull(nullBitmapAddr, rowCount);
                    QwpNullBitmap.setNull(nullBitmapAddr, 1);

                    for (int i = 0; i < valueCount; i++) {
                        Unsafe.getUnsafe().putLong(tsAddr + (long) i * 8, timestampValues[i]);
                    }

                    String walName;
                    try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                        walName = walWriter.getWalName();
                        ColumnarRowAppender appender = walWriter.getColumnarRowAppender();
                        int tsIndex = walWriter.getMetadata().getTimestampIndex();

                        appender.beginColumnarWrite(rowCount);
                        appender.putFixedColumn(0, valuesAddr, rowCount, 4, 0, rowCount);
                        appender.putTimestampColumn(tsIndex, tsAddr, valueCount, nullBitmapAddr, rowCount, 0, Numbers.LONG_NULL);
                        appender.endColumnarWrite(timestampValues[0], timestampValues[2], false);

                        walWriter.commit();
                    }

                    try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowCount)) {
                        RecordCursor cursor = reader.getDataCursor();
                        Record record = cursor.getRecord();

                        assertTrue(cursor.hasNext());
                        assertEquals(timestampValues[0], record.getTimestamp(1));

                        assertTrue(cursor.hasNext());
                        assertEquals(Numbers.LONG_NULL, record.getTimestamp(1)); // NULL

                        assertTrue(cursor.hasNext());
                        assertEquals(timestampValues[1], record.getTimestamp(1));

                        assertTrue(cursor.hasNext());
                        assertEquals(timestampValues[2], record.getTimestamp(1));

                        assertFalse(cursor.hasNext());
                    }
                } finally {
                    Unsafe.free(nullBitmapAddr, bitmapSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(tsAddr, (long) valueCount * 8, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) rowCount * 4, MemoryTag.NATIVE_DEFAULT);
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
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values, true)) {
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
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values, false)) {
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
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values, false)) {
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
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values, false)) {
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
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values, true)) {
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
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values, false)) {
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
                 StringColumnWireFormat wireFormat = new StringColumnWireFormat(values, true)) {
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
            int dataLength = 16; // 2 longs
            long dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            try {
                // Write in little-endian order: lo first, then hi
                Unsafe.getUnsafe().putLong(dataAddress, expectedLo);
                Unsafe.getUnsafe().putLong(dataAddress + 8, expectedHi);

                // Initialize cursor
                cursor.of(dataAddress, 1, QwpConstants.TYPE_UUID,
                        false);

                // Read and verify
                cursor.advanceRow();
                assertEquals("UUID hi mismatch", expectedHi, cursor.getUuidHi());
                assertEquals("UUID lo mismatch", expectedLo, cursor.getUuidLo());

                // Verify direct memory matches what columnar path would memcpy
                assertEquals("Wire lo should match", expectedLo, Unsafe.getUnsafe().getLong(cursor.getValuesAddress()));
                assertEquals("Wire hi should match", expectedHi, Unsafe.getUnsafe().getLong(cursor.getValuesAddress() + 8));

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

    /**
     * Writes the designated timestamp column for the given timestamps.
     */
    private void putTimestampColumn(ColumnarRowAppender appender, WalWriter walWriter, long[] timestamps, int rowCount) {
        int tsIndex = walWriter.getMetadata().getTimestampIndex();
        long startRowId = walWriter.getSegmentRowCount();

        long tsAddr = Unsafe.malloc((long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putLong(tsAddr + (long) i * 8, timestamps[i]);
            }
            appender.putTimestampColumn(tsIndex, tsAddr, rowCount, 0, rowCount, startRowId, Numbers.LONG_NULL);
        } finally {
            Unsafe.free(tsAddr, (long) rowCount * 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Helper to build wire format for QwpBooleanColumnCursor.
     * Wire format:
     * - [null bitmap if nullable]: ceil(rowCount/8) bytes
     * - [value bitmap]: ceil(valueCount/8) bytes, bit[i]=1 means true
     */
    private static class BooleanColumnWireFormat implements AutoCloseable {
        final QwpBooleanColumnCursor cursor = new QwpBooleanColumnCursor();
        long dataAddress;
        int dataLength;

        BooleanColumnWireFormat(Boolean[] values, boolean nullable) {
            int rowCount = values.length;
            int nullCount = 0;

            for (Boolean value : values) {
                if (value == null) {
                    nullCount++;
                }
            }

            int valueCount = rowCount - nullCount;

            // Calculate sizes
            int bitmapSize = nullable ? QwpNullBitmap.sizeInBytes(rowCount) : 0;
            int valueBitmapSize = (valueCount + 7) / 8;

            dataLength = bitmapSize + valueBitmapSize;
            dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            // Write null bitmap
            if (nullable) {
                QwpNullBitmap.fillNoneNull(dataAddress, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (values[i] == null) {
                        QwpNullBitmap.setNull(dataAddress, i);
                    }
                }
            }

            // Zero value bitmap (bits are set via OR)
            long valueBitmapAddr = dataAddress + bitmapSize;
            for (int i = 0; i < valueBitmapSize; i++) {
                Unsafe.getUnsafe().putByte(valueBitmapAddr + i, (byte) 0);
            }
            int valueIdx = 0;
            for (int i = 0; i < rowCount; i++) {
                if (values[i] != null) {
                    if (values[i]) {
                        int byteIndex = valueIdx >>> 3;
                        int bitIndex = valueIdx & 7;
                        byte b = Unsafe.getUnsafe().getByte(valueBitmapAddr + byteIndex);
                        b = (byte) (b | (1 << bitIndex));
                        Unsafe.getUnsafe().putByte(valueBitmapAddr + byteIndex, b);
                    }
                    valueIdx++;
                }
            }

            // Initialize cursor
            cursor.of(dataAddress, rowCount, nullable);
        }

        @Override
        public void close() {
            Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Helper to build wire format for QwpStringColumnCursor.
     * Wire format:
     * - [null bitmap if nullable]: ceil(rowCount/8) bytes
     * - [offset array]: (valueCount+1) * 4 bytes, uint32 little-endian
     * - [string data]: concatenated UTF-8 bytes
     */
    private static class StringColumnWireFormat implements AutoCloseable {
        final QwpStringColumnCursor cursor = new QwpStringColumnCursor();
        long dataAddress;
        int dataLength;

        StringColumnWireFormat(String[] values, boolean nullable) {
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
            int bitmapSize = nullable ? QwpNullBitmap.sizeInBytes(rowCount) : 0;
            int offsetArraySize = (valueCount + 1) * 4;

            int totalStringLen = 0;
            for (byte[] utf8Value : utf8Values) {
                if (utf8Value != null) {
                    totalStringLen += utf8Value.length;
                }
            }

            dataLength = bitmapSize + offsetArraySize + totalStringLen;
            dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            // Write null bitmap
            int offset = 0;
            if (nullable) {
                QwpNullBitmap.fillNoneNull(dataAddress, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (values[i] == null) {
                        QwpNullBitmap.setNull(dataAddress, i);
                    }
                }
                offset = bitmapSize;
            }

            // Write offset array and string data
            long offsetArrayAddr = dataAddress + offset;
            long stringDataAddr = offsetArrayAddr + offsetArraySize;
            int stringOffset = 0;
            int valueIdx = 0;

            for (int i = 0; i < rowCount; i++) {
                byte[] utf8 = utf8Values[i];
                if (utf8 != null) {
                    Unsafe.getUnsafe().putInt(offsetArrayAddr + (long) valueIdx * 4, stringOffset);
                    for (byte b : utf8) {
                        Unsafe.getUnsafe().putByte(stringDataAddr + stringOffset++, b);
                    }
                    valueIdx++;
                }
            }
            Unsafe.getUnsafe().putInt(offsetArrayAddr + (long) valueCount * 4, stringOffset);

            // Initialize cursor
            cursor.of(dataAddress, dataLength, rowCount, (byte) 0x07, nullable);
        }

        @Override
        public void close() {
            Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Helper to build wire format for QwpSymbolColumnCursor.
     * Wire format:
     * - [null bitmap if nullable]: ceil(rowCount/8) bytes
     * - [dictionary size]: varint
     * - For each dictionary entry: [string length]: varint, [string data]: UTF-8 bytes
     * - [indices]: varint per non-null value, references dictionary entry
     */
    private static class SymbolColumnWireFormat implements AutoCloseable {
        final QwpSymbolColumnCursor cursor = new QwpSymbolColumnCursor();
        long dataAddress;
        int dataLength;

        SymbolColumnWireFormat(String[] values, boolean nullable) throws QwpParseException {
            int rowCount = values.length;

            // Build dictionary
            java.util.Map<String, Integer> dictMap = new java.util.LinkedHashMap<>();
            java.util.List<String> dictList = new java.util.ArrayList<>();
            int[] indices = new int[rowCount];

            for (int i = 0; i < rowCount; i++) {
                if (values[i] == null) {
                    indices[i] = -1;
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
            int bitmapSize = nullable ? QwpNullBitmap.sizeInBytes(rowCount) : 0;

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

            dataLength = bitmapSize + dictBytes + indicesBytes;
            dataAddress = Unsafe.malloc(dataLength, MemoryTag.NATIVE_DEFAULT);

            // Write null bitmap
            int offset = 0;
            if (nullable) {
                QwpNullBitmap.fillNoneNull(dataAddress, rowCount);
                for (int i = 0; i < rowCount; i++) {
                    if (values[i] == null) {
                        QwpNullBitmap.setNull(dataAddress, i);
                    }
                }
                offset = bitmapSize;
            }

            // Write dictionary
            long addr = dataAddress + offset;
            addr = QwpVarint.encode(addr, dictionarySize);
            for (String s : dictList) {
                byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
                addr = QwpVarint.encode(addr, utf8.length);
                for (byte b : utf8) {
                    Unsafe.getUnsafe().putByte(addr++, b);
                }
            }

            // Write indices
            for (int i = 0; i < rowCount; i++) {
                if (indices[i] >= 0) {
                    addr = QwpVarint.encode(addr, indices[i]);
                }
            }

            // Initialize cursor
            cursor.of(dataAddress, dataLength, rowCount, nullable);
        }

        @Override
        public void close() {
            Unsafe.free(dataAddress, dataLength, MemoryTag.NATIVE_DEFAULT);
        }
    }
}

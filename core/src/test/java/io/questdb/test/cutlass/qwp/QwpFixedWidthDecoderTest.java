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

package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpFixedWidthDecoderTest {

    @Test
    public void testDecodeByteColumn() throws QwpParseException {
        byte[] values = {1, 2, 3, -128, 127, 0};
        int rowCount = values.length;

        long address = Unsafe.malloc(rowCount, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putByte(address + i, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, rowCount, rowCount, TYPE_BYTE, false);

            Assert.assertEquals(rowCount, consumed);
            for (int i = 0; i < rowCount; i++) {
                cursor.advanceRow();
                Assert.assertFalse(cursor.isNull());
                Assert.assertEquals(values[i], (byte) cursor.getLong());
            }
        } finally {
            Unsafe.free(address, rowCount, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeByteColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            byte[] values = {1, 0, 3, 0, 5};
            boolean[] nulls = {false, true, false, true, false};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_BYTE, true);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    if (nulls[i]) {
                        col.addNull();
                    } else {
                        col.addByte(values[i]);
                    }
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals(rowCount, table.getRowCount());

                    int colIdx = findColumnIndex(table, TYPE_BYTE);
                    Assert.assertNotEquals(-1, colIdx);

                    for (int i = 0; i < rowCount; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();

                        if (nulls[i]) {
                            Assert.assertTrue("Row " + i + " should be null", table.isColumnNull(colIdx));
                        } else {
                            Assert.assertFalse("Row " + i + " should not be null", table.isColumnNull(colIdx));
                            QwpFixedWidthColumnCursor cursor = table.getFixedWidthColumn(colIdx);
                            Assert.assertEquals(values[i], (byte) cursor.getLong());
                        }
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }

    @Test
    public void testDecodeDateColumn() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0L, 86_400_000L, 1_609_459_200_000L, -86_400_000L};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_DATE, false);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    col.addLong(values[i]);
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals(rowCount, table.getRowCount());

                    int colIdx = findColumnIndex(table, TYPE_DATE);
                    Assert.assertNotEquals(-1, colIdx);

                    for (int i = 0; i < rowCount; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();

                        QwpFixedWidthColumnCursor cursor = table.getFixedWidthColumn(colIdx);
                        Assert.assertFalse(cursor.isNull());
                        Assert.assertEquals(values[i], cursor.getLong());
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }

    @Test
    public void testDecodeDoubleColumn() throws Exception {
        assertMemoryLeak(() -> {
            double[] values = {1.5, -2.5, 0.0, Double.MAX_VALUE, Double.MIN_VALUE};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_DOUBLE, false);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    col.addDouble(values[i]);
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals(rowCount, table.getRowCount());

                    int colIdx = findColumnIndex(table, TYPE_DOUBLE);
                    Assert.assertNotEquals(-1, colIdx);

                    for (int i = 0; i < rowCount; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();

                        QwpFixedWidthColumnCursor cursor = table.getFixedWidthColumn(colIdx);
                        Assert.assertFalse(cursor.isNull());
                        Assert.assertEquals(values[i], cursor.getDouble(), 0.0001);
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }

    @Test
    public void testDecodeDoubleColumnSpecialValues() throws Exception {
        assertMemoryLeak(() -> {
            double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_DOUBLE, false);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    col.addDouble(values[i]);
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals(rowCount, table.getRowCount());

                    int colIdx = findColumnIndex(table, TYPE_DOUBLE);
                    Assert.assertNotEquals(-1, colIdx);

                    table.nextRow();
                    Assert.assertTrue(Double.isNaN(table.getFixedWidthColumn(colIdx).getDouble()));

                    table.nextRow();
                    Assert.assertEquals(Double.POSITIVE_INFINITY, table.getFixedWidthColumn(colIdx).getDouble(), 0.0);

                    table.nextRow();
                    Assert.assertEquals(Double.NEGATIVE_INFINITY, table.getFixedWidthColumn(colIdx).getDouble(), 0.0);
                }
            }
        });
    }

    @Test
    public void testDecodeDoubleColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            double[] values = {1.5, 0.0, 3.5};
            boolean[] nulls = {false, true, false};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_DOUBLE, true);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    if (nulls[i]) {
                        col.addNull();
                    } else {
                        col.addDouble(values[i]);
                    }
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals(rowCount, table.getRowCount());

                    int colIdx = findColumnIndex(table, TYPE_DOUBLE);
                    Assert.assertNotEquals(-1, colIdx);

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(1.5, table.getFixedWidthColumn(colIdx).getDouble(), 0.0001);

                    table.nextRow();
                    Assert.assertTrue(table.isColumnNull(colIdx));

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(3.5, table.getFixedWidthColumn(colIdx).getDouble(), 0.0001);
                }
            }
        });
    }

    @Test
    public void testDecodeEmptyColumn() throws QwpParseException {
        QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
        int consumed = cursor.of(0, 0, 0, TYPE_LONG, false);
        Assert.assertEquals(0, consumed);
    }

    @Test
    public void testDecodeFloatColumn() throws Exception {
        assertMemoryLeak(() -> {
            float[] values = {1.5f, -2.5f, 0.0f, Float.MAX_VALUE, Float.MIN_VALUE};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_FLOAT, false);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    col.addFloat(values[i]);
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals(rowCount, table.getRowCount());

                    int colIdx = findColumnIndex(table, TYPE_FLOAT);
                    Assert.assertNotEquals(-1, colIdx);

                    for (int i = 0; i < rowCount; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();

                        QwpFixedWidthColumnCursor cursor = table.getFixedWidthColumn(colIdx);
                        Assert.assertFalse(cursor.isNull());
                        Assert.assertEquals(values[i], (float) cursor.getDouble(), 0.0001f);
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }

    @Test
    public void testDecodeFloatColumnSpecialValues() throws Exception {
        assertMemoryLeak(() -> {
            float[] values = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_FLOAT, false);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    col.addFloat(values[i]);
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    int colIdx = findColumnIndex(table, TYPE_FLOAT);
                    Assert.assertNotEquals(-1, colIdx);

                    table.nextRow();
                    Assert.assertTrue(Float.isNaN((float) table.getFixedWidthColumn(colIdx).getDouble()));

                    table.nextRow();
                    Assert.assertEquals(Float.POSITIVE_INFINITY, (float) table.getFixedWidthColumn(colIdx).getDouble(), 0.0f);

                    table.nextRow();
                    Assert.assertEquals(Float.NEGATIVE_INFINITY, (float) table.getFixedWidthColumn(colIdx).getDouble(), 0.0f);
                }
            }
        });
    }

    @Test
    public void testDecodeFloatColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            float[] values = {1.5f, 0.0f, 3.5f};
            boolean[] nulls = {false, true, false};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_FLOAT, true);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    if (nulls[i]) {
                        col.addNull();
                    } else {
                        col.addFloat(values[i]);
                    }
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    int colIdx = findColumnIndex(table, TYPE_FLOAT);
                    Assert.assertNotEquals(-1, colIdx);

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(1.5f, (float) table.getFixedWidthColumn(colIdx).getDouble(), 0.0001f);

                    table.nextRow();
                    Assert.assertTrue(table.isColumnNull(colIdx));

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(3.5f, (float) table.getFixedWidthColumn(colIdx).getDouble(), 0.0001f);
                }
            }
        });
    }

    @Test
    public void testDecodeInsufficientDataForNullBitmap() {
        QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();

        // 10 rows need 2 bytes for null bitmap, but we only provide 1
        long address = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            cursor.of(address, 1, 10, TYPE_LONG, true);
            Assert.fail("expected QwpParseException for truncated null bitmap");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForValues() {
        QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();

        // 10 long values need 80 bytes, we provide 40
        int size = 40;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            cursor.of(address, size, 10, TYPE_LONG, false);
            Assert.fail("expected QwpParseException for truncated values");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeIntColumn() throws QwpParseException {
        int[] values = {1, 65536, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 0};
        int rowCount = values.length;

        int size = rowCount * 4;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putInt(address + (long) i * 4, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_INT, false);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                cursor.advanceRow();
                Assert.assertFalse(cursor.isNull());
                Assert.assertEquals(values[i], (int) cursor.getLong());
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeIntColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            int[] values = {100, 0, 300, 0};
            boolean[] nulls = {false, true, false, true};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_INT, true);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    if (nulls[i]) {
                        col.addNull();
                    } else {
                        col.addInt(values[i]);
                    }
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    int colIdx = findColumnIndex(table, TYPE_INT);
                    Assert.assertNotEquals(-1, colIdx);

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(100, (int) table.getFixedWidthColumn(colIdx).getLong());

                    table.nextRow();
                    Assert.assertTrue(table.isColumnNull(colIdx));

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(300, (int) table.getFixedWidthColumn(colIdx).getLong());

                    table.nextRow();
                    Assert.assertTrue(table.isColumnNull(colIdx));
                }
            }
        });
    }

    @Test
    public void testDecodeLargeColumn() throws QwpParseException {
        int rowCount = 100_000;
        int size = rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putLong(address + (long) i * 8, i);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_LONG, false);

            Assert.assertEquals(size, consumed);

            // Advance to row 0 and verify
            cursor.advanceRow();
            Assert.assertEquals(0L, cursor.getLong());

            // Skip to row 50000
            for (int i = 1; i <= 50_000; i++) {
                cursor.advanceRow();
            }
            Assert.assertEquals(50_000L, cursor.getLong());

            // Skip to row 99999
            for (int i = 50_001; i <= 99_999; i++) {
                cursor.advanceRow();
            }
            Assert.assertEquals(99_999L, cursor.getLong());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeLong256Column() throws QwpParseException {
        int rowCount = 2;
        int size = rowCount * 32;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Row 0: little-endian LONG256 (4 longs, least significant first)
            Unsafe.getUnsafe().putLong(address, 0x3333333344444444L);       // l0 (least significant)
            Unsafe.getUnsafe().putLong(address + 8, 0x1111111122222222L);   // l1
            Unsafe.getUnsafe().putLong(address + 16, 0xFEDCBA9876543210L);  // l2
            Unsafe.getUnsafe().putLong(address + 24, 0x0123456789ABCDEFL);  // l3 (most significant)

            // Row 1: all zeros
            for (int i = 0; i < 32; i++) {
                Unsafe.getUnsafe().putByte(address + 32 + i, (byte) 0);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_LONG256, false);

            Assert.assertEquals(size, consumed);

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(0x3333333344444444L, cursor.getLong256_0());
            Assert.assertEquals(0x1111111122222222L, cursor.getLong256_1());
            Assert.assertEquals(0xFEDCBA9876543210L, cursor.getLong256_2());
            Assert.assertEquals(0x0123456789ABCDEFL, cursor.getLong256_3());

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(0L, cursor.getLong256_0());
            Assert.assertEquals(0L, cursor.getLong256_1());
            Assert.assertEquals(0L, cursor.getLong256_2());
            Assert.assertEquals(0L, cursor.getLong256_3());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeLongColumn() throws QwpParseException {
        long[] values = {1L, 4_294_967_296L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 0L};
        int rowCount = values.length;

        int size = rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putLong(address + (long) i * 8, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_LONG, false);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                cursor.advanceRow();
                Assert.assertFalse(cursor.isNull());
                Assert.assertEquals(values[i], cursor.getLong());
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeLongColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {100L, 0L, 300L};
            boolean[] nulls = {false, true, false};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_LONG, true);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    if (nulls[i]) {
                        col.addNull();
                    } else {
                        col.addLong(values[i]);
                    }
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    int colIdx = findColumnIndex(table, TYPE_LONG);
                    Assert.assertNotEquals(-1, colIdx);

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(100L, table.getFixedWidthColumn(colIdx).getLong());

                    table.nextRow();
                    Assert.assertTrue(table.isColumnNull(colIdx));

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(300L, table.getFixedWidthColumn(colIdx).getLong());
                }
            }
        });
    }

    @Test
    public void testDecodeShortColumn() throws QwpParseException {
        short[] values = {1, 256, -1, Short.MAX_VALUE, Short.MIN_VALUE, 0};
        int rowCount = values.length;

        int size = rowCount * 2;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowCount; i++) {
                Unsafe.getUnsafe().putShort(address + (long) i * 2, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_SHORT, false);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                cursor.advanceRow();
                Assert.assertFalse(cursor.isNull());
                Assert.assertEquals(values[i], cursor.getShort());
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeShortColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            short[] values = {100, 0, 300};
            boolean[] nulls = {false, true, false};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_SHORT, true);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    if (nulls[i]) {
                        col.addNull();
                    } else {
                        col.addShort(values[i]);
                    }
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    int colIdx = findColumnIndex(table, TYPE_SHORT);
                    Assert.assertNotEquals(-1, colIdx);

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(values[0], table.getFixedWidthColumn(colIdx).getShort());

                    table.nextRow();
                    Assert.assertTrue(table.isColumnNull(colIdx));

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(values[2], table.getFixedWidthColumn(colIdx).getShort());
                }
            }
        });
    }

    @Test
    public void testDecodeTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0L, 1_000_000_000L, 1_609_459_200_000_000L};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_TIMESTAMP, false);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    col.addLong(values[i]);
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals(rowCount, table.getRowCount());

                    // Column 0 is the non-designated "val" timestamp, decoded via
                    // QwpTimestampColumnCursor (not fixed-width).
                    for (int i = 0; i < rowCount; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();

                        QwpTimestampColumnCursor cursor = table.getTimestampColumn(0);
                        Assert.assertFalse(cursor.isNull());
                        Assert.assertEquals(values[i], cursor.getTimestamp());
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }

    @Test
    public void testDecodeUuidColumn() throws Exception {
        assertMemoryLeak(() -> {
            long[] hiValues = {0x0123456789ABCDEFL, 0xFEDCBA9876543210L, 0L};
            long[] loValues = {0xFEDCBA9876543210L, 0x0123456789ABCDEFL, 0L};
            int rowCount = hiValues.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_UUID, false);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    col.addUuid(hiValues[i], loValues[i]);
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals(rowCount, table.getRowCount());

                    int colIdx = findColumnIndex(table, TYPE_UUID);
                    Assert.assertNotEquals(-1, colIdx);

                    for (int i = 0; i < rowCount; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();

                        QwpFixedWidthColumnCursor cursor = table.getFixedWidthColumn(colIdx);
                        Assert.assertFalse(cursor.isNull());
                        Assert.assertEquals("Row " + i + " uuid hi", hiValues[i], cursor.getUuidHi());
                        Assert.assertEquals("Row " + i + " uuid lo", loValues[i], cursor.getUuidLo());
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }

    @Test
    public void testDecodeUuidColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            long[] hiValues = {0x0123456789ABCDEFL, 0L, 0xABCDEF0123456789L};
            long[] loValues = {0xFEDCBA9876543210L, 0L, 0x9876543210ABCDEFL};
            boolean[] nulls = {false, true, false};
            int rowCount = hiValues.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_UUID, true);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                for (int i = 0; i < rowCount; i++) {
                    if (nulls[i]) {
                        col.addNull();
                    } else {
                        col.addUuid(hiValues[i], loValues[i]);
                    }
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    int colIdx = findColumnIndex(table, TYPE_UUID);
                    Assert.assertNotEquals(-1, colIdx);

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(hiValues[0], table.getFixedWidthColumn(colIdx).getUuidHi());
                    Assert.assertEquals(loValues[0], table.getFixedWidthColumn(colIdx).getUuidLo());

                    table.nextRow();
                    Assert.assertTrue(table.isColumnNull(colIdx));

                    table.nextRow();
                    Assert.assertFalse(table.isColumnNull(colIdx));
                    Assert.assertEquals(hiValues[2], table.getFixedWidthColumn(colIdx).getUuidHi());
                    Assert.assertEquals(loValues[2], table.getFixedWidthColumn(colIdx).getUuidLo());
                }
            }
        });
    }

    private static int findColumnIndex(QwpTableBlockCursor table, byte typeCode) {
        for (int c = 0; c < table.getColumnCount(); c++) {
            if ((table.getColumnDef(c).getTypeCode() & TYPE_MASK) == typeCode) {
                return c;
            }
        }
        return -1;
    }
}

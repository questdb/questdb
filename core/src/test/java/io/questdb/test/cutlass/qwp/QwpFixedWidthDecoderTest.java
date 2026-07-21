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

package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpFixedWidthDecoderTest {

    @Test
    public void testDecodeByteColumn() throws QwpParseException {
        byte[] values = {1, 2, 3, -128, 127, 0};
        int rowCount = values.length;

        int size = 1 + rowCount; // flag byte + values
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putByte(address + 1 + i, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_BYTE);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                cursor.advanceRow();
                Assert.assertFalse(cursor.isNull());
                Assert.assertEquals(values[i], (byte) cursor.getLong());
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeByteColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            byte[] values = {1, 0, 3, 0, 5};
            boolean[] nulls = {false, true, false, true, false};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, nulls, values);

                int size = encoder.encode(buffer);
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
                QwpTableBuffer buffer = getQwpTableBuffer(QwpConstants.TYPE_DATE, rowCount, values);

                int size = encoder.encode(buffer);
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
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, values);

                int size = encoder.encode(buffer);
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
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, values);

                int size = encoder.encode(buffer);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals(rowCount, table.getRowCount());

                    int colIdx = findColumnIndex(table, TYPE_DOUBLE);
                    Assert.assertNotEquals(-1, colIdx);

                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();
                    Assert.assertTrue(Double.isNaN(table.getFixedWidthColumn(colIdx).getDouble()));

                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();
                    Assert.assertEquals(Double.POSITIVE_INFINITY, table.getFixedWidthColumn(colIdx).getDouble(), 0.0);

                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();
                    Assert.assertEquals(Double.NEGATIVE_INFINITY, table.getFixedWidthColumn(colIdx).getDouble(), 0.0);

                    Assert.assertFalse(table.hasNextRow());
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
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, nulls, values);

                int size = encoder.encode(buffer);
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
        int size = 1;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, 0, TYPE_LONG);
            Assert.assertEquals(1, consumed);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeFloatColumn() throws Exception {
        assertMemoryLeak(() -> {
            float[] values = {1.5f, -2.5f, 0.0f, Float.MAX_VALUE, Float.MIN_VALUE};
            int rowCount = values.length;

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, values);

                int size = encoder.encode(buffer);
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
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, values);

                int size = encoder.encode(buffer);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    int colIdx = findColumnIndex(table, TYPE_FLOAT);
                    Assert.assertNotEquals(-1, colIdx);

                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();
                    Assert.assertTrue(Float.isNaN((float) table.getFixedWidthColumn(colIdx).getDouble()));

                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();
                    Assert.assertEquals(Float.POSITIVE_INFINITY, (float) table.getFixedWidthColumn(colIdx).getDouble(), 0.0f);

                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();
                    Assert.assertEquals(Float.NEGATIVE_INFINITY, (float) table.getFixedWidthColumn(colIdx).getDouble(), 0.0f);

                    Assert.assertFalse(table.hasNextRow());
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
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, nulls, values);

                int size = encoder.encode(buffer);
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

        // null bitmap flag=1 but only 1 byte left, not enough for bitmap (10 rows need 2 bytes)
        int size = 2;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 1); // null bitmap present
            cursor.of(address, size, 10, TYPE_LONG);
            Assert.fail("expected QwpParseException for truncated null bitmap");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForValues() {
        QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();

        // no null bitmap + 10 long values need 80 bytes, we provide 41 (1 for flag + 40 for data)
        int size = 41;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            cursor.of(address, size, 10, TYPE_LONG);
            Assert.fail("expected QwpParseException for truncated values");
        } catch (QwpParseException e) {
            Assert.assertTrue(e.getMessage().contains("truncated"));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeIntColumn() throws QwpParseException {
        int[] values = {1, 65_536, -1, Integer.MAX_VALUE, Integer.MIN_VALUE + 1, 0};
        int rowCount = values.length;

        int size = 1 + rowCount * 4; // flag byte + values
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putInt(address + 1 + (long) i * 4, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_INT);

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
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, nulls, values);

                int size = encoder.encode(buffer);
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
        int size = 1 + rowCount * 8; // flag byte + values
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putLong(address + 1 + (long) i * 8, i);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_LONG);

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
        int size = 1 + rowCount * 32; // flag byte + values
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            // Row 0: little-endian LONG256 (4 longs, least significant first)
            Unsafe.putLong(address + 1, 0x3333333344444444L);       // l0 (least significant)
            Unsafe.putLong(address + 1 + 8, 0x1111111122222222L);   // l1
            Unsafe.putLong(address + 1 + 16, 0xFEDCBA9876543210L);  // l2
            Unsafe.putLong(address + 1 + 24, 0x0123456789ABCDEFL);  // l3 (most significant)

            // Row 1: all zeros
            for (int i = 0; i < 32; i++) {
                Unsafe.putByte(address + 1 + 32 + i, (byte) 0);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_LONG256);

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
        long[] values = {1L, 4_294_967_296L, -1L, Long.MAX_VALUE, Long.MIN_VALUE + 1, 0L};
        int rowCount = values.length;

        int size = 1 + rowCount * 8; // flag byte + values
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putLong(address + 1 + (long) i * 8, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_LONG);

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
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, nulls, values);

                int size = encoder.encode(buffer);
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

        int size = 1 + rowCount * 2; // flag byte + values
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putShort(address + 1 + (long) i * 2, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            int consumed = cursor.of(address, size, rowCount, TYPE_SHORT);

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
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, nulls, values);

                int size = encoder.encode(buffer);
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
                QwpTableBuffer buffer = getQwpTableBuffer(QwpConstants.TYPE_TIMESTAMP, rowCount, values);

                int size = encoder.encode(buffer);
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
                QwpTableBuffer buffer = getQwpTableBuffer(rowCount, hiValues, loValues);

                int size = encoder.encode(buffer);
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
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

                for (int i = 0; i < rowCount; i++) {
                    if (nulls[i]) {
                        col.addNull();
                    } else {
                        col.addUuid(hiValues[i], loValues[i]);
                    }
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
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

    @Test
    public void testDecodeDateColumnSentinelNullNoBitmap() throws QwpParseException {
        long[] values = {1_000L, Numbers.LONG_NULL, -2_000L};
        assertLongSentinelNull(values, TYPE_DATE);
    }

    @Test
    public void testDecodeDoubleColumnSentinelNullNoBitmap() throws QwpParseException {
        double[] values = {1.5, Double.NaN, -2.5};
        int rowCount = values.length;
        int size = 1 + rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putDouble(address + 1 + (long) i * 8, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            cursor.of(address, size, rowCount, TYPE_DOUBLE);

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(1.5, cursor.getDouble(), 0.0);

            cursor.advanceRow();
            Assert.assertTrue("NaN must be reported as NULL when no bitmap is present", cursor.isNull());

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(-2.5, cursor.getDouble(), 0.0);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeFloatColumnSentinelNullNoBitmap() throws QwpParseException {
        float[] values = {1.5f, Float.NaN, -2.5f};
        int rowCount = values.length;
        int size = 1 + rowCount * 4;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putFloat(address + 1 + (long) i * 4, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            cursor.of(address, size, rowCount, TYPE_FLOAT);

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(1.5f, (float) cursor.getDouble(), 0.0f);

            cursor.advanceRow();
            Assert.assertTrue("NaN must be reported as NULL when no bitmap is present", cursor.isNull());

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(-2.5f, (float) cursor.getDouble(), 0.0f);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeIntColumnExplicitSentinelWithBitmap() throws QwpParseException {
        int rowCount = 1;
        int size = 1 + 1 + 4;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 1);          // bitmap present
            Unsafe.putByte(address + 1, (byte) 0);      // bitmap: row 0 NOT null
            Unsafe.putInt(address + 2, Numbers.INT_NULL);

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            cursor.of(address, size, rowCount, TYPE_INT);

            cursor.advanceRow();
            Assert.assertFalse("bitmap-marked non-null must not be sentinel-detected", cursor.isNull());
            Assert.assertEquals(Numbers.INT_NULL, (int) cursor.getLong());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeIPv4ColumnSentinelNullNoBitmap() throws QwpParseException {
        // QwpTableBuffer.addIPv4 javadoc: "The bit pattern 0 (i.e. 0.0.0.0) is
        // reserved by QuestDB as the IPv4 NULL sentinel and surfaces as NULL
        // on read regardless of the null bitmap." The encoder writes 0 inline
        // as a value when there are no bitmap-tagged nulls; the cursor's
        // sentinel switch is the protocol-intended null detector for this case.
        int[] values = {0x0A000001, Numbers.IPv4_NULL, 0xFFFFFFFF};
        int rowCount = values.length;
        int size = 1 + rowCount * 4;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putInt(address + 1 + (long) i * 4, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            cursor.of(address, size, rowCount, TYPE_IPV4);

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(0x0A000001, (int) cursor.getLong());

            cursor.advanceRow();
            Assert.assertTrue("IPv4_NULL must be reported as NULL when no bitmap is present", cursor.isNull());

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(0xFFFFFFFF, (int) cursor.getLong());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeIntColumnSentinelNullNoBitmap() throws QwpParseException {
        int[] values = {42, Numbers.INT_NULL, -1};
        int rowCount = values.length;
        int size = 1 + rowCount * 4;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putInt(address + 1 + (long) i * 4, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            cursor.of(address, size, rowCount, TYPE_INT);

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(42, (int) cursor.getLong());

            cursor.advanceRow();
            Assert.assertTrue("INT_NULL must be reported as NULL when no bitmap is present", cursor.isNull());

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
            Assert.assertEquals(-1, (int) cursor.getLong());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeLong256ColumnSentinelNullNoBitmap() throws QwpParseException {
        // LONG256 NULL = (LONG_NULL, LONG_NULL, LONG_NULL, LONG_NULL) per
        // Long256Impl.NULL_LONG256. With no bitmap the cursor must classify
        // that bit pattern as NULL or downstream consumers (e.g. the
        // LONG256->String type-conversion path) will format it as a
        // non-null Long256 value.
        long[][] values = {
                {0x1L, 0x2L, 0x3L, 0x4L},
                {Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL},
                {-1L, -1L, -1L, -1L}
        };
        int rowCount = values.length;
        int size = 1 + rowCount * 32;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                long base = address + 1 + (long) i * 32;
                Unsafe.putLong(base, values[i][0]);
                Unsafe.putLong(base + 8, values[i][1]);
                Unsafe.putLong(base + 16, values[i][2]);
                Unsafe.putLong(base + 24, values[i][3]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            cursor.of(address, size, rowCount, TYPE_LONG256);

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());

            cursor.advanceRow();
            Assert.assertTrue("LONG256 NULL_LONG256 must be reported as NULL when no bitmap is present",
                    cursor.isNull());

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeLongColumnSentinelNullNoBitmap() throws QwpParseException {
        long[] values = {42L, Numbers.LONG_NULL, -1L};
        assertLongSentinelNull(values, TYPE_LONG);
    }

    @Test
    public void testDecodeTimestampColumnSentinelNullNoBitmap() throws QwpParseException {
        long[] values = {1_000_000L, Numbers.LONG_NULL, -1L};
        assertLongSentinelNull(values, TYPE_TIMESTAMP);
    }

    @Test
    public void testDecodeTimestampNanosColumnSentinelNullNoBitmap() throws QwpParseException {
        long[] values = {1_000_000_000L, Numbers.LONG_NULL, -1L};
        assertLongSentinelNull(values, TYPE_TIMESTAMP_NANOS);
    }

    @Test
    public void testDecodeUuidColumnSentinelNullNoBitmap() throws QwpParseException {
        // UUID NULL = (LONG_NULL, LONG_NULL) per Uuid.isNull(lo, hi). With no
        // bitmap the cursor must classify that bit pattern as NULL or the
        // UUID->String type-conversion path (WalColumnarRowAppender
        // putFixedOtherToStringColumn / putFixedOtherToVarcharColumn ->
        // formatFixedOtherValue) formats it as a non-null Uuid value.
        // Layout per QwpFixedWidthColumnCursor.readCurrentValue: lo first,
        // hi second.
        long[][] values = {
                {0x1234L, 0x5678L},
                {Numbers.LONG_NULL, Numbers.LONG_NULL},
                {-1L, -1L}
        };
        int rowCount = values.length;
        int size = 1 + rowCount * 16;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                long base = address + 1 + (long) i * 16;
                Unsafe.putLong(base, values[i][0]);       // lo
                Unsafe.putLong(base + 8, values[i][1]);   // hi
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            cursor.of(address, size, rowCount, TYPE_UUID);

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());

            cursor.advanceRow();
            Assert.assertTrue("UUID NULL must be reported as NULL when no bitmap is present",
                    cursor.isNull());

            cursor.advanceRow();
            Assert.assertFalse(cursor.isNull());
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertLongSentinelNull(long[] values, byte typeCode) throws QwpParseException {
        int rowCount = values.length;
        int size = 1 + rowCount * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(address, (byte) 0); // no null bitmap
            for (int i = 0; i < rowCount; i++) {
                Unsafe.putLong(address + 1 + (long) i * 8, values[i]);
            }

            QwpFixedWidthColumnCursor cursor = new QwpFixedWidthColumnCursor();
            cursor.of(address, size, rowCount, typeCode);

            for (int i = 0; i < rowCount; i++) {
                cursor.advanceRow();
                if (values[i] == Numbers.LONG_NULL) {
                    Assert.assertTrue(
                            "row " + i + " (" + QwpConstants.getTypeName(typeCode)
                                    + ") LONG_NULL must be reported as NULL when no bitmap is present",
                            cursor.isNull()
                    );
                } else {
                    Assert.assertFalse("row " + i + " must not be NULL", cursor.isNull());
                    Assert.assertEquals(values[i], cursor.getLong());
                }
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static int findColumnIndex(QwpTableBlockCursor table, byte typeCode) {
        for (int c = 0; c < table.getColumnCount(); c++) {
            if (table.getColumnDef(c).getTypeCode() == typeCode) {
                return c;
            }
        }
        return -1;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(int rowCount, boolean[] nulls, byte[] values) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_table");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_BYTE, true);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < rowCount; i++) {
            if (nulls[i]) {
                col.addNull();
            } else {
                col.addByte(values[i]);
            }
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(int rowCount, double[] values) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_table");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_DOUBLE, false);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < rowCount; i++) {
            col.addDouble(values[i]);
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(int rowCount, boolean[] nulls, double[] values) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_table");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_DOUBLE, true);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < rowCount; i++) {
            if (nulls[i]) {
                col.addNull();
            } else {
                col.addDouble(values[i]);
            }
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(int rowCount, float[] values) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_table");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_FLOAT, false);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < rowCount; i++) {
            col.addFloat(values[i]);
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(int rowCount, boolean[] nulls, float[] values) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_table");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_FLOAT, true);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < rowCount; i++) {
            if (nulls[i]) {
                col.addNull();
            } else {
                col.addFloat(values[i]);
            }
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(int rowCount, boolean[] nulls, int[] values) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_table");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_INT, true);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < rowCount; i++) {
            if (nulls[i]) {
                col.addNull();
            } else {
                col.addInt(values[i]);
            }
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(int rowCount, boolean[] nulls, long[] values) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_table");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_LONG, true);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < rowCount; i++) {
            if (nulls[i]) {
                col.addNull();
            } else {
                col.addLong(values[i]);
            }
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(int rowCount, boolean[] nulls, short[] values) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_table");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_SHORT, true);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < rowCount; i++) {
            if (nulls[i]) {
                col.addNull();
            } else {
                col.addShort(values[i]);
            }
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(byte typeTimestamp, int rowCount, long[] values) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_table");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", typeTimestamp, false);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < rowCount; i++) {
            col.addLong(values[i]);
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(int rowCount, long[] hiValues, long[] loValues) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_table");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("val", TYPE_UUID, false);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < rowCount; i++) {
            col.addUuid(hiValues[i], loValues[i]);
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }
}

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

package io.questdb.test.cutlass.line.websocket;

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Unit tests for QwpWebSocketEncoder.
 */
public class QwpWebSocketEncoderTest {

    @Test
    public void testEncodeSingleRowWithLong() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            // Add a long column
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("value", TYPE_LONG, false);
            col.addLong(12345L);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12); // At least header size

            QwpBufferWriter buf = encoder.getBuffer();
            long ptr = buf.getBufferPtr();

            // Verify header magic
            Assert.assertEquals((byte) 'I', Unsafe.getUnsafe().getByte(ptr));
            Assert.assertEquals((byte) 'L', Unsafe.getUnsafe().getByte(ptr + 1));
            Assert.assertEquals((byte) 'P', Unsafe.getUnsafe().getByte(ptr + 2));
            Assert.assertEquals((byte) '4', Unsafe.getUnsafe().getByte(ptr + 3));

            // Version
            Assert.assertEquals(VERSION_1, Unsafe.getUnsafe().getByte(ptr + 4));

            // Table count (little-endian short)
            Assert.assertEquals((short) 1, Unsafe.getUnsafe().getShort(ptr + 6));
        }
    }

    @Test
    public void testEncodeSingleRowWithDouble() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("temperature", TYPE_DOUBLE, false);
            col.addDouble(23.5);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeSingleRowWithString() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("name", TYPE_STRING, true);
            col.addString("hello");
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeSingleRowWithBoolean() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("active", TYPE_BOOLEAN, false);
            col.addBoolean(true);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeSingleRowWithTimestamp() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            // Add a timestamp column (designated timestamp uses empty name)
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
            col.addLong(1000000L); // Micros
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeMultipleColumns() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("weather");

            // Add multiple columns
            QwpTableBuffer.ColumnBuffer tempCol = buffer.getOrCreateColumn("temperature", TYPE_DOUBLE, false);
            tempCol.addDouble(23.5);

            QwpTableBuffer.ColumnBuffer humCol = buffer.getOrCreateColumn("humidity", TYPE_LONG, false);
            humCol.addLong(65);

            QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
            tsCol.addLong(1000000L);

            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);

            // Verify header
            QwpBufferWriter buf = encoder.getBuffer();
            long ptr = buf.getBufferPtr();
            Assert.assertEquals((byte) 'I', Unsafe.getUnsafe().getByte(ptr));
            Assert.assertEquals((byte) 'L', Unsafe.getUnsafe().getByte(ptr + 1));
            Assert.assertEquals((byte) 'P', Unsafe.getUnsafe().getByte(ptr + 2));
            Assert.assertEquals((byte) '4', Unsafe.getUnsafe().getByte(ptr + 3));
        }
    }

    @Test
    public void testReset() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
            col.addLong(1L);
            buffer.nextRow();

            int size1 = encoder.encode(buffer, false);

            // Reset and encode again
            buffer.reset();
            col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
            col.addLong(2L);
            buffer.nextRow();

            int size2 = encoder.encode(buffer, false);

            // Sizes should be similar (same schema)
            Assert.assertEquals(size1, size2);
        }
    }

    @Test
    public void testGorillaFlagEnabled() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(true);
            Assert.assertTrue(encoder.isGorillaEnabled());

            QwpTableBuffer buffer = new QwpTableBuffer("test");
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
            col.addLong(1000000L);
            buffer.nextRow();

            encoder.encode(buffer, false);

            // Check flags byte has Gorilla bit set
            QwpBufferWriter buf = encoder.getBuffer();
            byte flags = Unsafe.getUnsafe().getByte(buf.getBufferPtr() + 5);
            Assert.assertEquals(FLAG_GORILLA, (byte) (flags & FLAG_GORILLA));
        }
    }

    @Test
    public void testGorillaFlagDisabled() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(false);
            Assert.assertFalse(encoder.isGorillaEnabled());

            QwpTableBuffer buffer = new QwpTableBuffer("test");
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
            col.addLong(1000000L);
            buffer.nextRow();

            encoder.encode(buffer, false);

            // Check flags byte doesn't have Gorilla bit set
            QwpBufferWriter buf = encoder.getBuffer();
            byte flags = Unsafe.getUnsafe().getByte(buf.getBufferPtr() + 5);
            Assert.assertEquals(0, flags & FLAG_GORILLA);
        }
    }

    @Test
    public void testEncodeEmptyTableName() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            // Edge case: empty table name (probably invalid but let's verify encoding works)
            QwpTableBuffer buffer = new QwpTableBuffer("");
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
            col.addLong(1L);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 0);
        }
    }

    @Test
    public void testEncodeNullableColumnWithValue() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test");

            // Nullable column with a value
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("name", TYPE_STRING, true);
            col.addString("hello");
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeNullableColumnWithNull() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test");

            // Nullable column with null
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("name", TYPE_STRING, true);
            col.addString(null);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeDoubleArray() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("values", TYPE_DOUBLE_ARRAY, true);
            col.addDoubleArray(new double[]{1.0, 2.0, 3.0});
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeLongArray() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("values", TYPE_LONG_ARRAY, true);
            col.addLongArray(new long[]{1L, 2L, 3L});
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testPayloadLengthPatched() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
            col.addLong(42L);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);

            // Payload length is at offset 8 (4 magic + 1 version + 1 flags + 2 tablecount)
            QwpBufferWriter buf = encoder.getBuffer();
            int payloadLength = Unsafe.getUnsafe().getInt(buf.getBufferPtr() + 8);

            // Payload length should be total size minus header (12 bytes)
            Assert.assertEquals(size - 12, payloadLength);
        }
    }

    // ==================== SYMBOL COLUMN TESTS ====================

    @Test
    public void testEncodeSingleSymbol() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("host", TYPE_SYMBOL, false);
            col.addSymbol("server1");
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeMultipleSymbolsSameDictionary() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("host", TYPE_SYMBOL, false);
            col.addSymbol("server1");
            buffer.nextRow();

            col.addSymbol("server1"); // Same symbol
            buffer.nextRow();

            col.addSymbol("server2"); // Different symbol
            buffer.nextRow();

            col.addSymbol("server1"); // Back to first
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(4, buffer.getRowCount());
        }
    }

    @Test
    public void testEncodeSymbolWithManyDistinctValues() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("host", TYPE_SYMBOL, false);
            for (int i = 0; i < 100; i++) {
                col.addSymbol("server" + i);
                buffer.nextRow();
            }

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(100, buffer.getRowCount());
        }
    }

    @Test
    public void testEncodeNullableSymbolWithNull() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("host", TYPE_SYMBOL, true);
            col.addSymbol("server1");
            buffer.nextRow();

            col.addSymbol(null); // Null symbol
            buffer.nextRow();

            col.addSymbol("server2");
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(3, buffer.getRowCount());
        }
    }

    // ==================== UUID COLUMN TESTS ====================

    @Test
    public void testEncodeUuid() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("id", TYPE_UUID, false);
            col.addUuid(0x123456789ABCDEF0L, 0xFEDCBA9876543210L);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeMultipleUuids() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("id", TYPE_UUID, false);
            for (int i = 0; i < 10; i++) {
                col.addUuid(i * 1000L, i * 2000L);
                buffer.nextRow();
            }

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(10, buffer.getRowCount());
        }
    }

    // ==================== DECIMAL COLUMN TESTS ====================

    @Test
    public void testEncodeDecimal64() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("price", TYPE_DECIMAL64, false);
            col.addDecimal64(io.questdb.client.std.Decimal64.fromLong(12345L, 2)); // 123.45
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeMultipleDecimal64() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("price", TYPE_DECIMAL64, false);
            col.addDecimal64(io.questdb.client.std.Decimal64.fromLong(12345L, 2)); // 123.45
            buffer.nextRow();

            col.addDecimal64(io.questdb.client.std.Decimal64.fromLong(67890L, 2)); // 678.90
            buffer.nextRow();

            col.addDecimal64(io.questdb.client.std.Decimal64.fromLong(11111L, 2)); // 111.11
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(3, buffer.getRowCount());
        }
    }

    @Test
    public void testEncodeDecimal128() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("amount", TYPE_DECIMAL128, false);
            col.addDecimal128(io.questdb.client.std.Decimal128.fromLong(123456789012345L, 4));
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeDecimal256() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("bignum", TYPE_DECIMAL256, false);
            col.addDecimal256(io.questdb.client.std.Decimal256.fromLong(Long.MAX_VALUE, 6));
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    // ==================== ARRAY COLUMN TESTS ====================

    @Test
    public void testEncode2DDoubleArray() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("matrix", TYPE_DOUBLE_ARRAY, true);
            col.addDoubleArray(new double[][]{{1.0, 2.0}, {3.0, 4.0}});
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncode3DDoubleArray() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("tensor", TYPE_DOUBLE_ARRAY, true);
            col.addDoubleArray(new double[][][]{
                    {{1.0, 2.0}, {3.0, 4.0}},
                    {{5.0, 6.0}, {7.0, 8.0}}
            });
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncode2DLongArray() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("matrix", TYPE_LONG_ARRAY, true);
            col.addLongArray(new long[][]{{1L, 2L}, {3L, 4L}});
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeLargeArray() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            // Large 1D array
            double[] largeArray = new double[1000];
            for (int i = 0; i < 1000; i++) {
                largeArray[i] = i * 1.5;
            }

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("values", TYPE_DOUBLE_ARRAY, true);
            col.addDoubleArray(largeArray);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 8000); // At least 8 bytes per double
        }
    }

    // ==================== MULTIPLE ROWS TESTS ====================

    @Test
    public void testEncodeMultipleRows() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("metrics");

            for (int i = 0; i < 100; i++) {
                QwpTableBuffer.ColumnBuffer valCol = buffer.getOrCreateColumn("value", TYPE_LONG, false);
                valCol.addLong(i);

                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
                tsCol.addLong(1000000L + i);

                buffer.nextRow();
            }

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(100, buffer.getRowCount());
        }
    }

    @Test
    public void testEncodeLargeRowCount() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("metrics");

            for (int i = 0; i < 10000; i++) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(i);
                buffer.nextRow();
            }

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(10000, buffer.getRowCount());
        }
    }

    // ==================== MIXED COLUMN TYPES ====================

    @Test
    public void testEncodeMixedColumnTypes() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("events");

            // Add columns of different types
            QwpTableBuffer.ColumnBuffer symbolCol = buffer.getOrCreateColumn("host", TYPE_SYMBOL, false);
            symbolCol.addSymbol("server1");

            QwpTableBuffer.ColumnBuffer longCol = buffer.getOrCreateColumn("count", TYPE_LONG, false);
            longCol.addLong(42);

            QwpTableBuffer.ColumnBuffer doubleCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, false);
            doubleCol.addDouble(3.14);

            QwpTableBuffer.ColumnBuffer boolCol = buffer.getOrCreateColumn("active", TYPE_BOOLEAN, false);
            boolCol.addBoolean(true);

            QwpTableBuffer.ColumnBuffer stringCol = buffer.getOrCreateColumn("message", TYPE_STRING, true);
            stringCol.addString("hello world");

            QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
            tsCol.addLong(1000000L);

            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeMixedColumnsMultipleRows() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("events");

            for (int i = 0; i < 50; i++) {
                QwpTableBuffer.ColumnBuffer symbolCol = buffer.getOrCreateColumn("host", TYPE_SYMBOL, false);
                symbolCol.addSymbol("server" + (i % 5));

                QwpTableBuffer.ColumnBuffer longCol = buffer.getOrCreateColumn("count", TYPE_LONG, false);
                longCol.addLong(i * 10);

                QwpTableBuffer.ColumnBuffer doubleCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, false);
                doubleCol.addDouble(i * 1.5);

                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
                tsCol.addLong(1000000L + i);

                buffer.nextRow();
            }

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(50, buffer.getRowCount());
        }
    }

    // ==================== EDGE CASES ====================

    @Test
    public void testEncodeEmptyString() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("name", TYPE_STRING, true);
            col.addString("");
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeLongString() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 10000; i++) {
                sb.append('a');
            }

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("data", TYPE_STRING, true);
            col.addString(sb.toString());
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 10000);
        }
    }

    @Test
    public void testEncodeUnicodeString() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("name", TYPE_STRING, true);
            col.addString("Hello 世界 🌍");
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeZeroLong() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
            col.addLong(0L);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeNegativeLong() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
            col.addLong(-123456789L);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeMaxMinLong() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
            col.addLong(Long.MAX_VALUE);
            buffer.nextRow();

            col.addLong(Long.MIN_VALUE);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(2, buffer.getRowCount());
        }
    }

    @Test
    public void testEncodeSpecialDoubles() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_DOUBLE, false);
            col.addDouble(Double.MAX_VALUE);
            buffer.nextRow();

            col.addDouble(Double.MIN_VALUE);
            buffer.nextRow();

            col.addDouble(Double.POSITIVE_INFINITY);
            buffer.nextRow();

            col.addDouble(Double.NEGATIVE_INFINITY);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(4, buffer.getRowCount());
        }
    }

    @Test
    public void testEncodeNaNDouble() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_DOUBLE, false);
            col.addDouble(Double.NaN);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testEncodeAllBooleanValues() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("flag", TYPE_BOOLEAN, false);
            for (int i = 0; i < 100; i++) {
                col.addBoolean(i % 2 == 0);
                buffer.nextRow();
            }

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(100, buffer.getRowCount());
        }
    }

    // ==================== SCHEMA REFERENCE TESTS ====================

    @Test
    public void testEncodeWithSchemaRef() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
            col.addLong(42L);
            buffer.nextRow();

            int size = encoder.encode(buffer, true); // Use schema reference
            Assert.assertTrue(size > 12);
        }
    }

    // ==================== BUFFER REUSE TESTS ====================

    @Test
    public void testEncoderReusability() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            // Encode first message
            QwpTableBuffer buffer1 = new QwpTableBuffer("table1");
            QwpTableBuffer.ColumnBuffer col1 = buffer1.getOrCreateColumn("x", TYPE_LONG, false);
            col1.addLong(1L);
            buffer1.nextRow();
            int size1 = encoder.encode(buffer1, false);

            // Encode second message (encoder should reset internally)
            QwpTableBuffer buffer2 = new QwpTableBuffer("table2");
            QwpTableBuffer.ColumnBuffer col2 = buffer2.getOrCreateColumn("y", TYPE_DOUBLE, false);
            col2.addDouble(2.0);
            buffer2.nextRow();
            int size2 = encoder.encode(buffer2, false);

            // Both should succeed
            Assert.assertTrue(size1 > 12);
            Assert.assertTrue(size2 > 12);
        }
    }

    @Test
    public void testBufferResetAndReuse() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test");

            // First batch
            for (int i = 0; i < 100; i++) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(i);
                buffer.nextRow();
            }
            int size1 = encoder.encode(buffer, false);

            // Reset and second batch
            buffer.reset();
            for (int i = 0; i < 50; i++) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(i * 2);
                buffer.nextRow();
            }
            int size2 = encoder.encode(buffer, false);

            Assert.assertTrue(size1 > size2); // More rows = larger
            Assert.assertEquals(50, buffer.getRowCount());
        }
    }

    // ==================== ALL BASIC TYPES IN ONE ROW ====================

    @Test
    public void testEncodeAllBasicTypesInOneRow() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("all_types");

            buffer.getOrCreateColumn("b", TYPE_BOOLEAN, false).addBoolean(true);
            buffer.getOrCreateColumn("by", TYPE_BYTE, false).addByte((byte) 42);
            buffer.getOrCreateColumn("sh", TYPE_SHORT, false).addShort((short) 1000);
            buffer.getOrCreateColumn("i", TYPE_INT, false).addInt(100000);
            buffer.getOrCreateColumn("l", TYPE_LONG, false).addLong(1000000000L);
            buffer.getOrCreateColumn("f", TYPE_FLOAT, false).addFloat(3.14f);
            buffer.getOrCreateColumn("d", TYPE_DOUBLE, false).addDouble(3.14159265);
            buffer.getOrCreateColumn("s", TYPE_STRING, true).addString("test");
            buffer.getOrCreateColumn("sym", TYPE_SYMBOL, false).addSymbol("AAPL");
            buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1000000L);

            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
            Assert.assertEquals(1, buffer.getRowCount());
        }
    }

    // ==================== Delta Symbol Dictionary Tests ====================

    @Test
    public void testEncodeWithDeltaDict_freshConnection_sendsAllSymbols() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            // Add symbol column with global IDs
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ticker", TYPE_SYMBOL, false);

            // Simulate adding symbols via global dictionary
            int id1 = globalDict.getOrAddSymbol("AAPL");  // ID 0
            int id2 = globalDict.getOrAddSymbol("GOOG");  // ID 1
            col.addSymbolWithGlobalId("AAPL", id1);
            buffer.nextRow();
            col.addSymbolWithGlobalId("GOOG", id2);
            buffer.nextRow();

            // Fresh connection: confirmedMaxId = -1, so delta should include all symbols (0, 1)
            int confirmedMaxId = -1;
            int batchMaxId = 1;

            int size = encoder.encodeWithDeltaDict(buffer, globalDict, confirmedMaxId, batchMaxId, false);
            Assert.assertTrue(size > 12);

            QwpBufferWriter buf = encoder.getBuffer();
            long ptr = buf.getBufferPtr();

            // Verify header flag has FLAG_DELTA_SYMBOL_DICT set
            byte flags = Unsafe.getUnsafe().getByte(ptr + HEADER_OFFSET_FLAGS);
            Assert.assertTrue("Delta flag should be set", (flags & FLAG_DELTA_SYMBOL_DICT) != 0);
        }
    }

    @Test
    public void testEncodeWithDeltaDict_withConfirmed_sendsOnlyNew() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            // Pre-populate dictionary (simulating symbols already sent)
            globalDict.getOrAddSymbol("AAPL");  // ID 0
            globalDict.getOrAddSymbol("GOOG");  // ID 1

            // Now add new symbols
            int id2 = globalDict.getOrAddSymbol("MSFT");  // ID 2
            int id3 = globalDict.getOrAddSymbol("TSLA");  // ID 3

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ticker", TYPE_SYMBOL, false);
            col.addSymbolWithGlobalId("MSFT", id2);
            buffer.nextRow();
            col.addSymbolWithGlobalId("TSLA", id3);
            buffer.nextRow();

            // Server has confirmed IDs 0-1, so delta should only include 2-3
            int confirmedMaxId = 1;
            int batchMaxId = 3;

            int size = encoder.encodeWithDeltaDict(buffer, globalDict, confirmedMaxId, batchMaxId, false);
            Assert.assertTrue(size > 12);

            QwpBufferWriter buf = encoder.getBuffer();
            long ptr = buf.getBufferPtr();

            // Verify delta flag is set
            byte flags = Unsafe.getUnsafe().getByte(ptr + HEADER_OFFSET_FLAGS);
            Assert.assertTrue("Delta flag should be set", (flags & FLAG_DELTA_SYMBOL_DICT) != 0);

            // Read delta section after header
            long pos = ptr + HEADER_SIZE;

            // Read deltaStart varint (should be 2 = confirmedMaxId + 1)
            int deltaStart = Unsafe.getUnsafe().getByte(pos) & 0x7F;
            Assert.assertEquals(2, deltaStart);
        }
    }

    @Test
    public void testEncodeWithDeltaDict_noNewSymbols_sendsEmptyDelta() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();
            QwpTableBuffer buffer = new QwpTableBuffer("test_table");

            // Pre-populate dictionary with all symbols
            int id0 = globalDict.getOrAddSymbol("AAPL");  // ID 0
            int id1 = globalDict.getOrAddSymbol("GOOG");  // ID 1

            // Use only existing symbols
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ticker", TYPE_SYMBOL, false);
            col.addSymbolWithGlobalId("AAPL", id0);
            buffer.nextRow();
            col.addSymbolWithGlobalId("GOOG", id1);
            buffer.nextRow();

            // Server has confirmed all symbols (0-1), batchMaxId is 1
            int confirmedMaxId = 1;
            int batchMaxId = 1;

            int size = encoder.encodeWithDeltaDict(buffer, globalDict, confirmedMaxId, batchMaxId, false);
            Assert.assertTrue(size > 12);

            QwpBufferWriter buf = encoder.getBuffer();
            long ptr = buf.getBufferPtr();

            // Verify delta flag is set
            byte flags = Unsafe.getUnsafe().getByte(ptr + HEADER_OFFSET_FLAGS);
            Assert.assertTrue("Delta flag should be set", (flags & FLAG_DELTA_SYMBOL_DICT) != 0);

            // Read delta section after header
            long pos = ptr + HEADER_SIZE;

            // Read deltaStart varint (should be 2 = confirmedMaxId + 1)
            int deltaStart = Unsafe.getUnsafe().getByte(pos) & 0x7F;
            Assert.assertEquals(2, deltaStart);
            pos++;

            // Read deltaCount varint (should be 0)
            int deltaCount = Unsafe.getUnsafe().getByte(pos) & 0x7F;
            Assert.assertEquals(0, deltaCount);
        }
    }

    @Test
    public void testGlobalSymbolDictionaryBasics() {
        GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

        // Test sequential IDs
        Assert.assertEquals(0, dict.getOrAddSymbol("AAPL"));
        Assert.assertEquals(1, dict.getOrAddSymbol("GOOG"));
        Assert.assertEquals(2, dict.getOrAddSymbol("MSFT"));

        // Test deduplication
        Assert.assertEquals(0, dict.getOrAddSymbol("AAPL"));
        Assert.assertEquals(1, dict.getOrAddSymbol("GOOG"));

        // Test retrieval
        Assert.assertEquals("AAPL", dict.getSymbol(0));
        Assert.assertEquals("GOOG", dict.getSymbol(1));
        Assert.assertEquals("MSFT", dict.getSymbol(2));

        // Test size
        Assert.assertEquals(3, dict.size());
    }

    // ==================== GORILLA TIMESTAMP ENCODING TESTS ====================

    @Test
    public void testGorillaEncoding_multipleTimestamps_usesGorillaEncoding() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(true);

            QwpTableBuffer buffer = new QwpTableBuffer("test");

            // Add multiple timestamps with constant delta (best compression)
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
            for (int i = 0; i < 100; i++) {
                col.addLong(1000000000L + i * 1000L);
                buffer.nextRow();
            }

            int sizeWithGorilla = encoder.encode(buffer, false);

            // Now encode without Gorilla
            encoder.setGorillaEnabled(false);
            buffer.reset();
            col = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
            for (int i = 0; i < 100; i++) {
                col.addLong(1000000000L + i * 1000L);
                buffer.nextRow();
            }

            int sizeWithoutGorilla = encoder.encode(buffer, false);

            // Gorilla should produce smaller output for constant-delta timestamps
            Assert.assertTrue("Gorilla encoding should be smaller",
                    sizeWithGorilla < sizeWithoutGorilla);
        }
    }

    @Test
    public void testGorillaEncoding_twoTimestamps_usesUncompressed() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(true);

            QwpTableBuffer buffer = new QwpTableBuffer("test");

            // Only 2 timestamps - should use uncompressed (Gorilla needs 3+)
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
            col.addLong(1000000L);
            buffer.nextRow();
            col.addLong(2000000L);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);

            // Verify header has Gorilla flag set
            QwpBufferWriter buf = encoder.getBuffer();
            byte flags = Unsafe.getUnsafe().getByte(buf.getBufferPtr() + HEADER_OFFSET_FLAGS);
            Assert.assertEquals(FLAG_GORILLA, (byte) (flags & FLAG_GORILLA));
        }
    }

    @Test
    public void testGorillaEncoding_singleTimestamp_usesUncompressed() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(true);

            QwpTableBuffer buffer = new QwpTableBuffer("test");

            // Single timestamp - should use uncompressed
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
            col.addLong(1000000L);
            buffer.nextRow();

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);
        }
    }

    @Test
    public void testGorillaEncoding_compressionRatio() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(true);

            QwpTableBuffer buffer = new QwpTableBuffer("metrics");

            // Add many timestamps with constant delta - best case for Gorilla
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
            for (int i = 0; i < 1000; i++) {
                col.addLong(1000000000L + i * 1000L);
                buffer.nextRow();
            }

            int sizeWithGorilla = encoder.encode(buffer, false);

            // Calculate theoretical minimum size for Gorilla:
            // - Header: 12 bytes
            // - Table header, column schema, etc.
            // - First timestamp: 8 bytes
            // - Second timestamp: 8 bytes
            // - Remaining 998 timestamps: 998 bits (1 bit each for DoD=0) = ~125 bytes

            // Calculate size without Gorilla (1000 * 8 = 8000 bytes just for timestamps)
            encoder.setGorillaEnabled(false);
            buffer.reset();
            col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
            for (int i = 0; i < 1000; i++) {
                col.addLong(1000000000L + i * 1000L);
                buffer.nextRow();
            }

            int sizeWithoutGorilla = encoder.encode(buffer, false);

            // For constant delta, Gorilla should achieve significant compression
            double compressionRatio = (double) sizeWithGorilla / sizeWithoutGorilla;
            Assert.assertTrue("Compression ratio should be < 0.2 for constant delta",
                    compressionRatio < 0.2);
        }
    }

    @Test
    public void testGorillaEncoding_varyingDelta() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(true);

            QwpTableBuffer buffer = new QwpTableBuffer("test");

            // Varying deltas that exercise different buckets
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
            long[] timestamps = {
                    1000000000L,
                    1000001000L,  // delta=1000
                    1000002000L,  // DoD=0
                    1000003050L,  // DoD=50
                    1000004200L,  // DoD=100
                    1000006200L,  // DoD=850
            };

            for (long ts : timestamps) {
                col.addLong(ts);
                buffer.nextRow();
            }

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);

            // Verify header has Gorilla flag
            QwpBufferWriter buf = encoder.getBuffer();
            byte flags = Unsafe.getUnsafe().getByte(buf.getBufferPtr() + HEADER_OFFSET_FLAGS);
            Assert.assertEquals(FLAG_GORILLA, (byte) (flags & FLAG_GORILLA));
        }
    }

    @Test
    public void testGorillaEncoding_nanosTimestamps() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(true);

            QwpTableBuffer buffer = new QwpTableBuffer("test");

            // Use TYPE_TIMESTAMP_NANOS
            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP_NANOS, true);
            for (int i = 0; i < 100; i++) {
                col.addLong(1000000000000000000L + i * 1000000L); // Nanos with millisecond intervals
                buffer.nextRow();
            }

            int size = encoder.encode(buffer, false);
            Assert.assertTrue(size > 12);

            // Verify header has Gorilla flag
            QwpBufferWriter buf = encoder.getBuffer();
            byte flags = Unsafe.getUnsafe().getByte(buf.getBufferPtr() + HEADER_OFFSET_FLAGS);
            Assert.assertEquals(FLAG_GORILLA, (byte) (flags & FLAG_GORILLA));
        }
    }

    @Test
    public void testGorillaEncoding_multipleTimestampColumns() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(true);

            QwpTableBuffer buffer = new QwpTableBuffer("test");

            // Add multiple timestamp columns
            for (int i = 0; i < 50; i++) {
                QwpTableBuffer.ColumnBuffer ts1Col = buffer.getOrCreateColumn("ts1", TYPE_TIMESTAMP, true);
                ts1Col.addLong(1000000000L + i * 1000L);

                QwpTableBuffer.ColumnBuffer ts2Col = buffer.getOrCreateColumn("ts2", TYPE_TIMESTAMP, true);
                ts2Col.addLong(2000000000L + i * 2000L);

                buffer.nextRow();
            }

            int sizeWithGorilla = encoder.encode(buffer, false);

            // Compare with uncompressed
            encoder.setGorillaEnabled(false);
            buffer.reset();
            for (int i = 0; i < 50; i++) {
                QwpTableBuffer.ColumnBuffer ts1Col = buffer.getOrCreateColumn("ts1", TYPE_TIMESTAMP, true);
                ts1Col.addLong(1000000000L + i * 1000L);

                QwpTableBuffer.ColumnBuffer ts2Col = buffer.getOrCreateColumn("ts2", TYPE_TIMESTAMP, true);
                ts2Col.addLong(2000000000L + i * 2000L);

                buffer.nextRow();
            }

            int sizeWithoutGorilla = encoder.encode(buffer, false);

            Assert.assertTrue("Gorilla should compress multiple timestamp columns",
                    sizeWithGorilla < sizeWithoutGorilla);
        }
    }
}

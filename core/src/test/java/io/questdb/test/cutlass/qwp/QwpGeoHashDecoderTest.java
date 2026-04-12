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

import io.questdb.cairo.ColumnType;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpGeoHashColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpNullBitmap;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpGeoHashDecoderTest {

    /**
     * Verifies that the GeoHash cursor rejects a precision varint whose value
     * exceeds int range, even when the truncated (int) value would fall within
     * the valid [1, 60] range. Without the fix, the varint value 0x100000001L
     * (4294967297) would silently truncate to 1, bypassing the precision check.
     */
    @Test
    public void testAddGeoHashInvalidPrecisionVarintOverflow() {
        // Craft a buffer with: no-null-bitmap flag (1 byte) + precision varint
        // that encodes (1L << 32) + 1 = 0x100000001L. When truncated to int,
        // this becomes 1, which is within the valid precision range [1, 60].
        long overflowPrecision = (1L << 32) + 1;
        byte[] varintBuf = new byte[QwpVarint.MAX_VARINT_BYTES];
        int varintLen = QwpVarint.encode(varintBuf, 0, overflowPrecision);
        int bufSize = 1 + varintLen;

        long address = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            // No null bitmap
            Unsafe.getUnsafe().putByte(address, (byte) 0);
            // Write overflowing precision varint
            for (int i = 0; i < varintLen; i++) {
                Unsafe.getUnsafe().putByte(address + 1 + i, varintBuf[i]);
            }

            QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
            cursor.of(address, bufSize, 1);
            Assert.fail("Expected QwpParseException for precision varint exceeding int range");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_COLUMN_TYPE, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("invalid GeoHash precision"));
        } finally {
            Unsafe.free(address, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testAddGeoHashInvalidPrecisionTooLarge() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                try {
                    col.addGeoHash(0, 61);
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("precision"));
                }
            }
        });
    }

    @Test
    public void testAddGeoHashInvalidPrecisionZero() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                try {
                    col.addGeoHash(0, 0);
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("precision"));
                }
            }
        });
    }

    @Test
    public void testAddGeoHashPrecisionMismatch() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                col.addGeoHash(0b10110, 5);
                buffer.nextRow();
                try {
                    col.addGeoHash(0xFF, 10);
                    Assert.fail("Expected LineSenderException for precision mismatch");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("mismatch"));
                }
            }
        });
    }

    @Test
    public void testCursorNullablePackedValues() throws QwpParseException {
        // 5 rows, precision=5 (1 byte per value), rows 1 and 3 are null.
        // Values are packed: only non-null values are stored after the null bitmap.
        // This matches the packed format used by QwpFixedWidthColumnCursor and
        // QwpDecimalColumnCursor for nullable columns.
        int rowCount = 5;
        int precision = 5;
        int valueSize = 1;
        int nullCount = 2;
        int nonNullCount = rowCount - nullCount;

        int flagSize = 1;
        int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
        int precisionVarintSize = 1;
        int packedSize = flagSize + bitmapSize + precisionVarintSize + nonNullCount * valueSize;

        int allocSize = packedSize + 10; // extra space for safety

        long address = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
        try {
            // Zero out buffer
            for (int i = 0; i < allocSize; i++) {
                Unsafe.getUnsafe().putByte(address + i, (byte) 0);
            }

            int offset = 0;

            // Write null bitmap flag
            Unsafe.getUnsafe().putByte(address + offset, (byte) 1); // null bitmap present
            offset += flagSize;

            // Write null bitmap (rows 1 and 3 are null)
            QwpNullBitmapTestUtil.fillNoneNull(address + offset, rowCount);
            QwpNullBitmapTestUtil.setNull(address + offset, 1);
            QwpNullBitmapTestUtil.setNull(address + offset, 3);
            offset += bitmapSize;

            // Write precision varint
            QwpVarint.encode(address + offset, precision);
            offset += precisionVarintSize;

            // Write packed values (only non-null rows)
            Unsafe.getUnsafe().putByte(address + offset, (byte) 22);      // row 0
            Unsafe.getUnsafe().putByte(address + offset + 1, (byte) 31);  // row 2
            Unsafe.getUnsafe().putByte(address + offset + 2, (byte) 21);  // row 4

            // Initialize cursor
            QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
            int consumed = cursor.of(address, allocSize, rowCount);

            // Verify bytes consumed matches packed format
            Assert.assertEquals("Bytes consumed should match packed format size", packedSize, consumed);

            // Row 0: not null, value = 22
            cursor.advanceRow();
            Assert.assertFalse("Row 0 should not be null", cursor.isNull());
            Assert.assertEquals("Row 0 value", 22L, cursor.getGeoHash());

            // Row 1: null
            cursor.advanceRow();
            Assert.assertTrue("Row 1 should be null", cursor.isNull());

            // Row 2: not null, value = 31
            cursor.advanceRow();
            Assert.assertFalse("Row 2 should not be null", cursor.isNull());
            Assert.assertEquals("Row 2 value", 31L, cursor.getGeoHash());

            // Row 3: null
            cursor.advanceRow();
            Assert.assertTrue("Row 3 should be null", cursor.isNull());

            // Row 4: not null, value = 21
            cursor.advanceRow();
            Assert.assertFalse("Row 4 should not be null", cursor.isNull());
            Assert.assertEquals("Row 4 value", 21L, cursor.getGeoHash());
        } finally {
            Unsafe.free(address, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeEmptyColumn() throws QwpParseException {
        // no null bitmap + precision varint, 0 rows
        int allocSize = 1 + QwpVarint.encodedLength(5); // flag byte + precision
        long address = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            QwpVarint.encode(address + 1, 5);
            QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
            int consumed = cursor.of(address, allocSize, 0);
            Assert.assertEquals(allocSize, consumed);
        } finally {
            Unsafe.free(address, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeGeoHashAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0L, 0L, 0L};
            boolean[] nulls = {true, true, true};
            assertNullableEncoderRoundTrip(values, nulls, 10);
        });
    }

    @Test
    public void testDecodeGeoHashByte() throws Exception {
        assertMemoryLeak(() -> {
            // 5-bit precision (typical for single geohash character)
            long[] values = {0b10110, 0b01001, 0b11111, 0b00000, 0b10101};
            assertEncoderRoundTrip(values, 5);
        });
    }

    @Test
    public void testDecodeGeoHashByte7Bits() throws Exception {
        assertMemoryLeak(() -> {
            // Maximum bits for GEOBYTE
            long[] values = {0b1110110, 0b0100101, 0b1111111, 0b0000000, 0b1010101};
            assertEncoderRoundTrip(values, 7);
        });
    }

    @Test
    public void testDecodeGeoHashByteMinBits() throws Exception {
        assertMemoryLeak(() -> {
            // Minimum 1-bit geohash
            long[] values = {0, 1, 1, 0, 1};
            assertEncoderRoundTrip(values, 1);
        });
    }

    @Test
    public void testDecodeGeoHashInt() throws Exception {
        assertMemoryLeak(() -> {
            // 20-bit precision (typical for 4 geohash characters)
            long[] values = {0xABCDE, 0x12345, 0xFFFFF, 0x00000};
            assertEncoderRoundTrip(values, 20);
        });
    }

    @Test
    public void testDecodeGeoHashInt31Bits() throws Exception {
        assertMemoryLeak(() -> {
            // Maximum bits for GEOINT
            long[] values = {0x7FFFFFFFL, 0x55555555L, 0x2AAAAAABL, 0x00000000L};
            assertEncoderRoundTrip(values, 31);
        });
    }

    @Test
    public void testDecodeGeoHashLong() throws Exception {
        assertMemoryLeak(() -> {
            // 40-bit precision (typical for 8 geohash characters)
            long[] values = {0xABCDEF1234L, 0x123456789AL, 0xFFFFFFFFFFL, 0x0000000000L};
            assertEncoderRoundTrip(values, 40);
        });
    }

    @Test
    public void testDecodeGeoHashLong60Bits() throws Exception {
        assertMemoryLeak(() -> {
            // Maximum bits for GEOLONG (60 bits)
            long[] values = {
                    0x0FFFFFFFFFFFFFFFL,
                    0x0555555555555555L,
                    0x0AAAAAAAAAAAAAAAL,
                    0x0000000000000000L
            };
            assertEncoderRoundTrip(values, 60);
        });
    }

    @Test
    public void testDecodeGeoHashShort() throws Exception {
        assertMemoryLeak(() -> {
            // 10-bit precision (typical for 2 geohash characters)
            long[] values = {0b1011010110, 0b0100101001, 0b1111111111, 0b0000000000};
            assertEncoderRoundTrip(values, 10);
        });
    }

    @Test
    public void testDecodeGeoHashShort15Bits() throws Exception {
        assertMemoryLeak(() -> {
            // Maximum bits for GEOSHORT
            long[] values = {0b111011010110110, 0b010010100101001, 0b111111111111111};
            assertEncoderRoundTrip(values, 15);
        });
    }

    @Test
    public void testDecodeGeoHashWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0b10110, 0L, 0b11111, 0L, 0b10101};
            boolean[] nulls = {false, true, false, true, false};
            assertNullableEncoderRoundTrip(values, nulls, 5);
        });
    }

    @Test
    public void testDecodeInvalidPrecisionTooLarge() {
        int allocSize = 10;
        long address = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            // Encode precision = 61 (invalid, max is 60)
            QwpVarint.encode(address + 1, 61);

            QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
            Assert.assertThrows(QwpParseException.class, () ->
                    cursor.of(address, allocSize, 1));
        } finally {
            Unsafe.free(address, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInvalidPrecisionZero() {
        int allocSize = 10;
        long address = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            // Encode precision = 0 (invalid)
            QwpVarint.encode(address + 1, 0);

            QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
            Assert.assertThrows(QwpParseException.class, () ->
                    cursor.of(address, allocSize, 1));
        } finally {
            Unsafe.free(address, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeLargeColumn() throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = 10_000;
            int precision = 25; // 4-byte storage

            long[] values = new long[rowCount];
            for (int i = 0; i < rowCount; i++) {
                values[i] = (i * 12_345L) & ((1L << precision) - 1);
            }

            assertEncoderRoundTrip(values, precision);
        });
    }

    @Test
    public void testDecodeLargeColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = 10_000;
            int precision = 30;

            long[] values = new long[rowCount];
            boolean[] nulls = new boolean[rowCount];

            for (int i = 0; i < rowCount; i++) {
                values[i] = (i * 12_345L) & ((1L << precision) - 1);
                nulls[i] = (i % 7 == 0); // Every 7th value is null
            }

            assertNullableEncoderRoundTrip(values, nulls, precision);
        });
    }

    @Test
    public void testDecodePackedValue() throws Exception {
        assertMemoryLeak(() -> {
            // 3-byte packed (17-24 bits)
            assertEncoderRoundTrip(new long[]{0x123456L}, 24);

            // 5-byte packed (33-40 bits)
            assertEncoderRoundTrip(new long[]{0x123456789AL}, 40);

            // 6-byte packed (41-48 bits)
            assertEncoderRoundTrip(new long[]{0x123456789ABCL}, 48);

            // 7-byte packed (49-56 bits)
            assertEncoderRoundTrip(new long[]{0x123456789ABCDEL}, 56);
        });
    }

    @Test
    public void testDecodePrecisionExtraction() throws Exception {
        assertMemoryLeak(() -> {
            // Test that precision is correctly extracted for various values
            int[] precisions = {1, 5, 7, 8, 10, 15, 16, 20, 31, 32, 40, 60};

            for (int precision : precisions) {
                long[] values = {1L << (precision - 1)}; // Set highest bit
                assertEncoderRoundTrip(values, precision);
            }
        });
    }

    @Test
    public void testDecodeThrowsOnInsufficientDataForValues() {
        int precisionVarintSize = QwpVarint.encodedLength(10);
        int allocSize = 1 + precisionVarintSize + 4; // flag byte + precision + only 4 bytes for values
        long address = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            QwpVarint.encode(address + 1, 10);

            QwpGeoHashColumnCursor cursor = new QwpGeoHashColumnCursor();
            Assert.assertThrows(QwpParseException.class, () ->
                    cursor.of(address, allocSize, 5));
        } finally {
            Unsafe.free(address, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeVariablePrecision() throws Exception {
        assertMemoryLeak(() -> {
            // GEOBYTE boundary
            assertEncoderRoundTrip(new long[]{0x7F}, 7);  // Max for GEOBYTE
            assertEncoderRoundTrip(new long[]{0xFF}, 8);  // Min for GEOSHORT

            // GEOSHORT boundary
            assertEncoderRoundTrip(new long[]{0x7FFF}, 15);  // Max for GEOSHORT
            assertEncoderRoundTrip(new long[]{0xFFFF}, 16);  // Min for GEOINT

            // GEOINT boundary
            assertEncoderRoundTrip(new long[]{0x7FFFFFFFL}, 31);  // Max for GEOINT
            assertEncoderRoundTrip(new long[]{0xFFFFFFFFL}, 32);  // Min for GEOLONG
        });
    }

    @Test
    public void testEncodeDecodeGeoHashWithOtherColumns() throws Exception {
        assertMemoryLeak(() -> {
            int rowCount = 5;
            int precision = 20;
            long[] geoValues = {0xABCDE, 0x12345, 0xFFFFF, 0x00000, 0x55555};
            long[] longValues = {100, 200, 300, 400, 500};
            double[] doubleValues = {1.1, 2.2, 3.3, 4.4, 5.5};

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("mixed_table");

                QwpTableBuffer.ColumnBuffer geoCol = buffer.getOrCreateColumn("location", TYPE_GEOHASH, false);
                QwpTableBuffer.ColumnBuffer longCol = buffer.getOrCreateColumn("id", TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer dblCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, false);
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

                for (int i = 0; i < rowCount; i++) {
                    geoCol.addGeoHash(geoValues[i], precision);
                    longCol.addLong(longValues[i]);
                    dblCol.addDouble(doubleValues[i]);
                    tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                Assert.assertTrue(size > 12);

                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();

                    Assert.assertEquals("mixed_table", table.getTableName());
                    Assert.assertEquals(rowCount, table.getRowCount());
                    Assert.assertEquals(4, table.getColumnCount());

                    int geoColIdx = findGeoHashColumnIndex(table);
                    Assert.assertNotEquals("GEOHASH column not found", -1, geoColIdx);

                    for (int i = 0; i < rowCount; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();

                        QwpGeoHashColumnCursor geoCursor = table.getGeoHashColumn(geoColIdx);
                        Assert.assertFalse(geoCursor.isNull());
                        Assert.assertEquals(precision, geoCursor.getPrecision());
                        Assert.assertEquals("Row " + i + " geohash mismatch",
                                geoValues[i], geoCursor.getGeoHash());
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }

    @Test
    public void testEncodeDecodeNullableGeoHashNoNulls() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0xABCDE, 0x12345, 0xFFFFF};
            boolean[] nulls = {false, false, false};
            assertNullableEncoderRoundTrip(values, nulls, 20);
        });
    }

    @Test
    public void testEncodeDecodeNullableGeoLongWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            long[] values = {0x0FFFFFFFFFFFFFFFL, 0, 0x0555555555555555L};
            boolean[] nulls = {false, true, false};
            assertNullableEncoderRoundTrip(values, nulls, 60);
        });
    }

    @Test
    public void testEncodeDecodeSingleNullRow() throws Exception {
        assertMemoryLeak(() -> assertNullableEncoderRoundTrip(new long[]{0}, new boolean[]{true}, 5));
    }

    @Test
    public void testEncodeDecodeSingleNullableRow() throws Exception {
        assertMemoryLeak(() -> assertNullableEncoderRoundTrip(new long[]{0b10110}, new boolean[]{false}, 5));
    }

    @Test
    public void testEncodeDecodeSingleRow() throws Exception {
        assertMemoryLeak(() -> assertEncoderRoundTrip(new long[]{0b10110}, 5));
    }

    @Test
    public void testEncodeGeoHashProducesValidMessage() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                col.addGeoHash(0b10110, 5);
                buffer.nextRow();

                int size = encoder.encode(buffer, false);
                Assert.assertTrue(size > 12);

                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                // Verify QWP1 header magic
                Assert.assertEquals((byte) 'Q', io.questdb.client.std.Unsafe.getUnsafe().getByte(ptr));
                Assert.assertEquals((byte) 'W', io.questdb.client.std.Unsafe.getUnsafe().getByte(ptr + 1));
                Assert.assertEquals((byte) 'P', io.questdb.client.std.Unsafe.getUnsafe().getByte(ptr + 2));
                Assert.assertEquals((byte) '1', io.questdb.client.std.Unsafe.getUnsafe().getByte(ptr + 3));

                // Table count = 1
                Assert.assertEquals((short) 1, io.questdb.client.std.Unsafe.getUnsafe().getShort(ptr + 6));
            }
        });
    }

    @Test
    public void testEncodeMultipleGeoHashRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                for (int i = 0; i < 100; i++) {
                    col.addGeoHash(i & 0x1F, 5);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer, false);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(100, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeNullableGeoHash() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, true);
                col.addGeoHash(0b10110, 5);
                buffer.nextRow();

                col.addNull();
                buffer.nextRow();

                col.addGeoHash(0b11111, 5);
                buffer.nextRow();

                int size = encoder.encode(buffer, false);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(3, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testGetColumnType() {
        // GEOBYTE: 1-7 bits
        for (int bits = 1; bits <= 7; bits++) {
            int type = ColumnType.getGeoHashTypeWithBits(bits);
            Assert.assertEquals(ColumnType.GEOBYTE, ColumnType.tagOf(type));
        }

        // GEOSHORT: 8-15 bits
        for (int bits = 8; bits <= 15; bits++) {
            int type = ColumnType.getGeoHashTypeWithBits(bits);
            Assert.assertEquals(ColumnType.GEOSHORT, ColumnType.tagOf(type));
        }

        // GEOINT: 16-31 bits
        for (int bits = 16; bits <= 31; bits++) {
            int type = ColumnType.getGeoHashTypeWithBits(bits);
            Assert.assertEquals(ColumnType.GEOINT, ColumnType.tagOf(type));
        }

        // GEOLONG: 32-60 bits
        for (int bits = 32; bits <= 60; bits++) {
            int type = ColumnType.getGeoHashTypeWithBits(bits);
            Assert.assertEquals(ColumnType.GEOLONG, ColumnType.tagOf(type));
        }
    }

    @Test
    public void testGetStorageSize() {
        // 1-7 bits -> 1 byte
        for (int bits = 1; bits <= 7; bits++) {
            Assert.assertEquals(1, getStorageSize(bits));
        }

        // 8-15 bits -> 2 bytes
        for (int bits = 8; bits <= 15; bits++) {
            Assert.assertEquals(2, getStorageSize(bits));
        }

        // 16-31 bits -> 4 bytes
        for (int bits = 16; bits <= 31; bits++) {
            Assert.assertEquals(4, getStorageSize(bits));
        }

        // 32-60 bits -> 8 bytes
        for (int bits = 32; bits <= 60; bits++) {
            Assert.assertEquals(8, getStorageSize(bits));
        }
    }

    @Test
    public void testResetAllowsNewPrecision() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpTableBuffer buffer = new QwpTableBuffer("test_table");

                // First batch: 5-bit precision
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                col.addGeoHash(0b10110, 5);
                buffer.nextRow();

                encoder.encode(buffer, false);
                buffer.reset();

                // After reset: 20-bit precision works fine
                col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
                col.addGeoHash(0xABCDE, 20);
                buffer.nextRow();

                int size = encoder.encode(buffer, false);
                Assert.assertTrue(size > 12);
            }
        });
    }

    private static int findGeoHashColumnIndex(QwpTableBlockCursor table) {
        for (int c = 0; c < table.getColumnCount(); c++) {
            if (table.getColumnDef(c).getTypeCode() == TYPE_GEOHASH) {
                return c;
            }
        }
        return -1;
    }

    private static int getStorageSize(int precision) {
        if (precision <= ColumnType.GEOBYTE_MAX_BITS) {
            return 1;
        } else if (precision <= ColumnType.GEOSHORT_MAX_BITS) {
            return 2;
        } else if (precision <= ColumnType.GEOINT_MAX_BITS) {
            return 4;
        } else {
            return 8;
        }
    }

    private void assertEncoderRoundTrip(long[] values, int precision) throws QwpParseException {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = new QwpTableBuffer("test_geohash");

            QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, false);
            QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

            for (int i = 0; i < values.length; i++) {
                col.addGeoHash(values[i], precision);
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

                Assert.assertEquals(values.length, table.getRowCount());

                int geoColIdx = findGeoHashColumnIndex(table);
                Assert.assertNotEquals("GEOHASH column not found", -1, geoColIdx);

                for (int i = 0; i < values.length; i++) {
                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();

                    QwpGeoHashColumnCursor geoCursor = table.getGeoHashColumn(geoColIdx);
                    Assert.assertFalse("Row " + i + " should not be null", geoCursor.isNull());
                    Assert.assertEquals("Row " + i + " precision mismatch",
                            precision, geoCursor.getPrecision());
                    Assert.assertEquals("Row " + i + " value mismatch",
                            values[i], geoCursor.getGeoHash());
                }
                Assert.assertFalse(table.hasNextRow());
            }
        }
    }

    private void assertNullableEncoderRoundTrip(long[] values, boolean[] nulls, int precision) throws QwpParseException {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpTableBuffer buffer = getQwpTableBuffer(values, nulls, precision);

            int size = encoder.encode(buffer, false);
            QwpBufferWriter buf = encoder.getBuffer();
            long ptr = buf.getBufferPtr();

            try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                QwpMessageCursor msg = decoder.decode(ptr, size);
                Assert.assertTrue(msg.hasNextTable());
                QwpTableBlockCursor table = msg.nextTable();

                Assert.assertEquals(values.length, table.getRowCount());

                int geoColIdx = findGeoHashColumnIndex(table);
                Assert.assertNotEquals("GEOHASH column not found", -1, geoColIdx);

                for (int i = 0; i < values.length; i++) {
                    Assert.assertTrue(table.hasNextRow());
                    table.nextRow();

                    if (nulls[i]) {
                        Assert.assertTrue("Row " + i + " should be null",
                                table.isColumnNull(geoColIdx));
                    } else {
                        Assert.assertFalse("Row " + i + " should not be null",
                                table.isColumnNull(geoColIdx));
                        QwpGeoHashColumnCursor geoCursor = table.getGeoHashColumn(geoColIdx);
                        Assert.assertEquals(precision, geoCursor.getPrecision());
                        Assert.assertEquals("Row " + i + " value mismatch",
                                values[i], geoCursor.getGeoHash());
                    }
                }
                Assert.assertFalse(table.hasNextRow());
            }
        }
    }

    private static @NotNull QwpTableBuffer getQwpTableBuffer(long[] values, boolean[] nulls, int precision) {
        QwpTableBuffer buffer = new QwpTableBuffer("test_geohash");

        QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("geo", TYPE_GEOHASH, true);
        QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);

        for (int i = 0; i < values.length; i++) {
            if (nulls[i]) {
                col.addNull();
            } else {
                col.addGeoHash(values[i], precision);
            }
            tsCol.addLong(1_000_000_000_000L + i * 1_000_000L);
            buffer.nextRow();
        }
        return buffer;
    }
}

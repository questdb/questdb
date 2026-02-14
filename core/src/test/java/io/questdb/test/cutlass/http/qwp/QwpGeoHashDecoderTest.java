/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.http.qwp;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.qwp.protocol.QwpGeoHashDecoder;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class QwpGeoHashDecoderTest {

    // ==================== GEOBYTE (1-7 bits) Tests ====================

    @Test
    public void testDecodeGeoHashByte() throws QwpParseException {
        // 5-bit precision (typical for single geohash character)
        long[] values = {0b10110, 0b01001, 0b11111, 0b00000, 0b10101};
        int precision = 5;
        testRoundTrip(values, precision, null);
    }

    @Test
    public void testDecodeGeoHashByte7Bits() throws QwpParseException {
        // Maximum bits for GEOBYTE
        long[] values = {0b1110110, 0b0100101, 0b1111111, 0b0000000, 0b1010101};
        int precision = 7;
        testRoundTrip(values, precision, null);
    }

    @Test
    public void testDecodeGeoHashByteMinBits() throws QwpParseException {
        // Minimum 1-bit geohash
        long[] values = {0, 1, 1, 0, 1};
        int precision = 1;
        testRoundTrip(values, precision, null);
    }

    // ==================== GEOSHORT (8-15 bits) Tests ====================

    @Test
    public void testDecodeGeoHashShort() throws QwpParseException {
        // 10-bit precision (typical for 2 geohash characters)
        long[] values = {0b1011010110, 0b0100101001, 0b1111111111, 0b0000000000};
        int precision = 10;
        testRoundTrip(values, precision, null);
    }

    @Test
    public void testDecodeGeoHashShort15Bits() throws QwpParseException {
        // Maximum bits for GEOSHORT
        long[] values = {0b111011010110110, 0b010010100101001, 0b111111111111111};
        int precision = 15;
        testRoundTrip(values, precision, null);
    }

    // ==================== GEOINT (16-31 bits) Tests ====================

    @Test
    public void testDecodeGeoHashInt() throws QwpParseException {
        // 20-bit precision (typical for 4 geohash characters)
        long[] values = {0xABCDE, 0x12345, 0xFFFFF, 0x00000};
        int precision = 20;
        testRoundTrip(values, precision, null);
    }

    @Test
    public void testDecodeGeoHashInt31Bits() throws QwpParseException {
        // Maximum bits for GEOINT
        long[] values = {0x7FFFFFFFL, 0x55555555L, 0x2AAAAAABL, 0x00000000L};
        int precision = 31;
        testRoundTrip(values, precision, null);
    }

    // ==================== GEOLONG (32-60 bits) Tests ====================

    @Test
    public void testDecodeGeoHashLong() throws QwpParseException {
        // 40-bit precision (typical for 8 geohash characters)
        long[] values = {0xABCDEF1234L, 0x123456789AL, 0xFFFFFFFFFFL, 0x0000000000L};
        int precision = 40;
        testRoundTrip(values, precision, null);
    }

    @Test
    public void testDecodeGeoHashLong60Bits() throws QwpParseException {
        // Maximum bits for GEOLONG (60 bits)
        long[] values = {
                0x0FFFFFFFFFFFFFFFL,
                0x0555555555555555L,
                0x0AAAAAAAAAAAAAAAL,
                0x0000000000000000L
        };
        int precision = 60;
        testRoundTrip(values, precision, null);
    }

    // ==================== Null Handling Tests ====================

    @Test
    public void testDecodeGeoHashWithNulls() throws QwpParseException {
        long[] values = {0b10110, 0L, 0b11111, 0L, 0b10101};
        boolean[] nulls = {false, true, false, true, false};
        int precision = 5;
        testRoundTrip(values, precision, nulls);
    }

    @Test
    public void testDecodeGeoHashAllNulls() throws QwpParseException {
        long[] values = {0L, 0L, 0L};
        boolean[] nulls = {true, true, true};
        int precision = 10;
        testRoundTrip(values, precision, nulls);
    }

    // ==================== Precision Tests ====================

    @Test
    public void testDecodePrecisionExtraction() throws QwpParseException {
        // Test that precision is correctly extracted for various values
        int[] precisions = {1, 5, 7, 8, 10, 15, 16, 20, 31, 32, 40, 60};

        for (int precision : precisions) {
            long[] values = {1L << (precision - 1)}; // Set highest bit
            testRoundTrip(values, precision, null);
        }
    }

    @Test
    public void testDecodePackedValue() throws QwpParseException {
        // Test various packed value sizes
        // 3-byte packed (17-24 bits)
        long[] values3 = {0x123456L};
        testRoundTrip(values3, 24, null);

        // 5-byte packed (33-40 bits)
        long[] values5 = {0x123456789AL};
        testRoundTrip(values5, 40, null);

        // 6-byte packed (41-48 bits)
        long[] values6 = {0x123456789ABCL};
        testRoundTrip(values6, 48, null);

        // 7-byte packed (49-56 bits)
        long[] values7 = {0x123456789ABCDEL};
        testRoundTrip(values7, 56, null);
    }

    @Test
    public void testDecodeVariablePrecision() throws QwpParseException {
        // Test boundary precision values

        // GEOBYTE boundary
        testRoundTrip(new long[]{0x7F}, 7, null);  // Max for GEOBYTE
        testRoundTrip(new long[]{0xFF}, 8, null);  // Min for GEOSHORT

        // GEOSHORT boundary
        testRoundTrip(new long[]{0x7FFF}, 15, null);  // Max for GEOSHORT
        testRoundTrip(new long[]{0xFFFF}, 16, null);  // Min for GEOINT

        // GEOINT boundary
        testRoundTrip(new long[]{0x7FFFFFFFL}, 31, null);  // Max for GEOINT
        testRoundTrip(new long[]{0xFFFFFFFFL}, 32, null);  // Min for GEOLONG
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testDecodeInvalidPrecisionZero() {
        long address = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
        try {
            // Encode precision = 0 (invalid)
            QwpVarint.encode(address, 0);

            QwpGeoHashDecoder.ArrayGeoHashSink sink = new QwpGeoHashDecoder.ArrayGeoHashSink(1);
            Assert.assertThrows(QwpParseException.class, () ->
                    QwpGeoHashDecoder.INSTANCE.decode(address, 10, 1, false, sink));
        } finally {
            Unsafe.free(address, 10, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInvalidPrecisionTooLarge() {
        long address = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
        try {
            // Encode precision = 61 (invalid, max is 60)
            QwpVarint.encode(address, 61);

            QwpGeoHashDecoder.ArrayGeoHashSink sink = new QwpGeoHashDecoder.ArrayGeoHashSink(1);
            Assert.assertThrows(QwpParseException.class, () ->
                    QwpGeoHashDecoder.INSTANCE.decode(address, 10, 1, false, sink));
        } finally {
            Unsafe.free(address, 10, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForValues() {
        long address = Unsafe.malloc(5, MemoryTag.NATIVE_DEFAULT);
        try {
            // Encode precision = 10 (needs 2 bytes per value), but only provide 4 bytes total
            QwpVarint.encode(address, 10);

            QwpGeoHashDecoder.ArrayGeoHashSink sink = new QwpGeoHashDecoder.ArrayGeoHashSink(5);
            Assert.assertThrows(QwpParseException.class, () ->
                    QwpGeoHashDecoder.INSTANCE.decode(address, 5, 5, false, sink));
        } finally {
            Unsafe.free(address, 5, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Empty Column Tests ====================

    @Test
    public void testDecodeEmptyColumn() throws QwpParseException {
        QwpGeoHashDecoder.ArrayGeoHashSink sink = new QwpGeoHashDecoder.ArrayGeoHashSink(0);
        int consumed = QwpGeoHashDecoder.INSTANCE.decode(0, 0, 0, false, sink);
        Assert.assertEquals(0, consumed);
    }

    // ==================== Large Column Tests ====================

    @Test
    public void testDecodeLargeColumn() throws QwpParseException {
        int rowCount = 10000;
        int precision = 25; // 4-byte storage

        long[] values = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = (i * 12345L) & ((1L << precision) - 1);
        }

        testRoundTrip(values, precision, null);
    }

    @Test
    public void testDecodeLargeColumnWithNulls() throws QwpParseException {
        int rowCount = 10000;
        int precision = 30;

        long[] values = new long[rowCount];
        boolean[] nulls = new boolean[rowCount];

        for (int i = 0; i < rowCount; i++) {
            values[i] = (i * 12345L) & ((1L << precision) - 1);
            nulls[i] = (i % 7 == 0); // Every 7th value is null
        }

        testRoundTrip(values, precision, nulls);
    }

    // ==================== Column Type Mapping Tests ====================

    @Test
    public void testGetColumnType() {
        // GEOBYTE: 1-7 bits
        for (int bits = 1; bits <= 7; bits++) {
            int type = QwpGeoHashDecoder.getColumnType(bits);
            Assert.assertEquals(ColumnType.GEOBYTE, ColumnType.tagOf(type));
        }

        // GEOSHORT: 8-15 bits
        for (int bits = 8; bits <= 15; bits++) {
            int type = QwpGeoHashDecoder.getColumnType(bits);
            Assert.assertEquals(ColumnType.GEOSHORT, ColumnType.tagOf(type));
        }

        // GEOINT: 16-31 bits
        for (int bits = 16; bits <= 31; bits++) {
            int type = QwpGeoHashDecoder.getColumnType(bits);
            Assert.assertEquals(ColumnType.GEOINT, ColumnType.tagOf(type));
        }

        // GEOLONG: 32-60 bits
        for (int bits = 32; bits <= 60; bits++) {
            int type = QwpGeoHashDecoder.getColumnType(bits);
            Assert.assertEquals(ColumnType.GEOLONG, ColumnType.tagOf(type));
        }
    }

    @Test
    public void testGetStorageSize() {
        // 1-7 bits -> 1 byte
        for (int bits = 1; bits <= 7; bits++) {
            Assert.assertEquals(1, QwpGeoHashDecoder.getStorageSize(bits));
        }

        // 8-15 bits -> 2 bytes
        for (int bits = 8; bits <= 15; bits++) {
            Assert.assertEquals(2, QwpGeoHashDecoder.getStorageSize(bits));
        }

        // 16-31 bits -> 4 bytes
        for (int bits = 16; bits <= 31; bits++) {
            Assert.assertEquals(4, QwpGeoHashDecoder.getStorageSize(bits));
        }

        // 32-60 bits -> 8 bytes
        for (int bits = 32; bits <= 60; bits++) {
            Assert.assertEquals(8, QwpGeoHashDecoder.getStorageSize(bits));
        }
    }

    // ==================== Size Calculation Tests ====================

    @Test
    public void testCalculateEncodedSize() {
        // Basic size calculation
        int size = QwpGeoHashDecoder.calculateEncodedSize(10, 5, false);
        // 1 byte (precision varint) + 10 * 1 byte (values) = 11
        Assert.assertEquals(11, size);

        // With nullable
        size = QwpGeoHashDecoder.calculateEncodedSize(10, 5, true);
        // 2 bytes (null bitmap) + 1 byte (precision) + 10 * 1 byte (values) = 13
        Assert.assertEquals(13, size);

        // Larger precision
        size = QwpGeoHashDecoder.calculateEncodedSize(10, 40, false);
        // 1 byte (precision varint) + 10 * 5 bytes (values) = 51
        Assert.assertEquals(51, size);
    }

    // ==================== Helper Methods ====================

    private void testRoundTrip(long[] values, int precision, boolean[] nulls) throws QwpParseException {
        int rowCount = values.length;
        boolean nullable = nulls != null;

        int size = QwpGeoHashDecoder.calculateEncodedSize(rowCount, precision, nullable);
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            // Encode
            long end = QwpGeoHashDecoder.encode(address, values, precision, nulls);
            int actualSize = (int) (end - address);
            Assert.assertEquals("Encoded size should match calculated size", size, actualSize);

            // Decode
            QwpGeoHashDecoder.ArrayGeoHashSink sink = new QwpGeoHashDecoder.ArrayGeoHashSink(rowCount);
            int consumed = QwpGeoHashDecoder.INSTANCE.decode(address, actualSize, rowCount, nullable, sink);

            Assert.assertEquals("Consumed bytes should match encoded size", actualSize, consumed);

            // Verify values
            for (int i = 0; i < rowCount; i++) {
                if (nullable && nulls[i]) {
                    Assert.assertTrue("Row " + i + " should be null", sink.isNull(i));
                } else {
                    Assert.assertFalse("Row " + i + " should not be null", sink.isNull(i));
                    Assert.assertEquals("Row " + i + " value mismatch", values[i], sink.getValue(i));
                    Assert.assertEquals("Row " + i + " precision mismatch", precision, sink.getPrecision(i));
                }
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }
}

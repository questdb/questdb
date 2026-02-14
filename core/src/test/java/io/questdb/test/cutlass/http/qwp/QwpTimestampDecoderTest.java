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

import io.questdb.cutlass.qwp.protocol.QwpColumnDecoder;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTimestampDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class QwpTimestampDecoderTest {

    // ==================== Uncompressed Timestamp Tests ====================

    @Test
    public void testDecodeUncompressedTimestamps() throws QwpParseException {
        long[] timestamps = {1000000000L, 1000001000L, 1000002000L, 1000003000L, 1000004000L};
        int rowCount = timestamps.length;

        int size = 1 + rowCount * 8; // encoding flag + values
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = QwpTimestampDecoder.encodeUncompressed(address, timestamps, null);
            Assert.assertEquals(size, end - address);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpTimestampDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                Assert.assertEquals(timestamps[i], sink.getValue(i));
                Assert.assertFalse(sink.isNull(i));
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeUncompressedWithNulls() throws QwpParseException {
        long[] timestamps = {1000000000L, 0L, 1000002000L, 0L, 1000004000L};
        boolean[] nulls = {false, true, false, true, false};
        int rowCount = timestamps.length;

        // Count non-null values
        int nullCount = 0;
        for (boolean isNull : nulls) {
            if (isNull) nullCount++;
        }
        int valueCount = rowCount - nullCount;

        int bitmapSize = (rowCount + 7) / 8;
        int size = bitmapSize + 1 + valueCount * 8; // bitmap + encoding flag + non-null values only
        int bufferSize = bitmapSize + 1 + rowCount * 8; // Allocate max possible
        long address = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = QwpTimestampDecoder.encodeUncompressed(address, timestamps, nulls);
            Assert.assertEquals(size, end - address);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpTimestampDecoder.INSTANCE.decode(address, bufferSize, rowCount, true, sink);

            Assert.assertEquals(size, consumed);
            for (int i = 0; i < rowCount; i++) {
                if (nulls[i]) {
                    Assert.assertTrue("Row " + i + " should be null", sink.isNull(i));
                } else {
                    Assert.assertEquals(timestamps[i], sink.getValue(i));
                    Assert.assertFalse(sink.isNull(i));
                }
            }
        } finally {
            Unsafe.free(address, bufferSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeZeroRows() throws QwpParseException {
        QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(0);
        int consumed = QwpTimestampDecoder.INSTANCE.decode(0, 0, 0, false, sink);
        Assert.assertEquals(0, consumed);
    }

    // ==================== Gorilla Encoding Flag Tests ====================

    @Test
    public void testDecodeGorillaEncodingFlag() throws QwpParseException {
        // Single timestamp with Gorilla encoding
        long[] timestamps = {1000000000L};
        int rowCount = 1;

        // Gorilla with one row: encoding flag (1) + first timestamp (8) = 9 bytes
        int size = 1 + 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = QwpTimestampDecoder.encodeGorilla(address, timestamps, null);
            Assert.assertEquals(size, end - address);

            // Verify encoding flag
            Assert.assertEquals(QwpTimestampDecoder.ENCODING_GORILLA, Unsafe.getUnsafe().getByte(address));

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpTimestampDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            Assert.assertEquals(timestamps[0], sink.getValue(0));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeGorillaTwoTimestamps() throws QwpParseException {
        // Two timestamps - no delta-of-delta needed
        long[] timestamps = {1000000000L, 1000001000L};
        int rowCount = 2;

        // Gorilla with two rows: encoding flag (1) + first (8) + second (8) = 17 bytes
        int size = 1 + 8 + 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = QwpTimestampDecoder.encodeGorilla(address, timestamps, null);
            Assert.assertEquals(size, end - address);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpTimestampDecoder.INSTANCE.decode(address, size, rowCount, false, sink);

            Assert.assertEquals(size, consumed);
            Assert.assertEquals(timestamps[0], sink.getValue(0));
            Assert.assertEquals(timestamps[1], sink.getValue(1));
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Gorilla Delta-of-Delta Tests ====================

    @Test
    public void testDecodeGorillaDeltaZero() throws QwpParseException {
        // Constant interval: DoD = 0 (1-bit encoding)
        // t0=1000, t1=2000, t2=3000 -> delta=1000, DoD=0
        long[] timestamps = {1000L, 2000L, 3000L, 4000L, 5000L};
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaSmall() throws QwpParseException {
        // Small DoD in range [-63, 64] -> 9-bit encoding (2 prefix + 7 value)
        // t0=1000, t1=2000, t2=3050 -> delta0=1000, delta1=1050, DoD=50
        long[] timestamps = {1000L, 2000L, 3050L};
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaSmallNegative() throws QwpParseException {
        // Negative small DoD
        // t0=1000, t1=2000, t2=2950 -> delta0=1000, delta1=950, DoD=-50
        long[] timestamps = {1000L, 2000L, 2950L};
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaMedium() throws QwpParseException {
        // Medium DoD in range [-255, 256] but outside [-63, 64] -> 12-bit encoding
        // DoD = 200
        long[] timestamps = {1000L, 2000L, 3200L};
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaMediumNegative() throws QwpParseException {
        // Negative medium DoD = -200
        long[] timestamps = {1000L, 2000L, 2800L};
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaLarge() throws QwpParseException {
        // Large DoD in range [-2047, 2048] but outside [-255, 256] -> 16-bit encoding
        // DoD = 1000
        long[] timestamps = {1000L, 2000L, 4000L};
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaLargeNegative() throws QwpParseException {
        // Negative large DoD = -1000
        long[] timestamps = {10000L, 11000L, 11000L};
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaHuge() throws QwpParseException {
        // Huge DoD outside [-2047, 2048] -> 36-bit encoding
        // DoD = 10000
        long[] timestamps = {1000L, 2000L, 13000L};
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaMixedBuckets() throws QwpParseException {
        // Mix of different bucket sizes
        long[] timestamps = {
                1000000000L,  // t0
                1000001000L,  // t1, delta=1000
                1000002000L,  // t2, DoD=0 (1-bit)
                1000003050L,  // t3, DoD=50 (9-bit)
                1000004200L,  // t4, DoD=100 (9-bit)
                1000006200L,  // t5, DoD=850 (16-bit)
                1000106200L   // t6, DoD=98000 (36-bit)
        };
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaRegularInterval() throws QwpParseException {
        // Regular 1-second interval - all DoD should be 0
        long[] timestamps = new long[100];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = 1000000000L + i * 1000000L; // 1ms = 1000000ns
        }
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaIrregularInterval() throws QwpParseException {
        // Irregular intervals with varying deltas
        long[] timestamps = {
                1000000000L,
                1000001000L,  // delta=1000
                1000003000L,  // delta=2000, DoD=1000
                1000003500L,  // delta=500, DoD=-1500
                1000010000L,  // delta=6500, DoD=6000
                1000010001L   // delta=1, DoD=-6499
        };
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaWithNulls() throws QwpParseException {
        long[] timestamps = {1000L, 2000L, 0L, 4000L, 0L, 6000L};
        boolean[] nulls = {false, false, true, false, true, false};
        testGorillaRoundTrip(timestamps, nulls);
    }

    @Test
    public void testDecodeGorillaSignedValues() throws QwpParseException {
        // Test with timestamps that produce negative DoD values in all ranges
        // These require proper sign extension during decoding
        long[] timestamps = {
                10000L,   // t0
                20000L,   // t1, delta=10000
                29940L,   // t2, DoD=-60 (in 7-bit range)
                39780L,   // t3, DoD=-100 (in 9-bit range)
                48420L,   // t4, DoD=-1500 (in 12-bit range)
                55000L,   // t5, DoD=-3000 (in 32-bit range)
        };
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaBitAlignment() throws QwpParseException {
        // Test that bit reading works correctly across byte boundaries
        // by using various bit-width encodings that don't align on byte boundaries
        long[] timestamps = {
                1000L, 2000L,     // first two uncompressed
                3000L,            // DoD=0 (1 bit)
                4000L,            // DoD=0 (1 bit)
                4050L,            // DoD=50 (9 bits) - crosses byte boundary
                5000L,            // DoD=-50 (9 bits)
                5200L,            // DoD=150 (12 bits)
                6100L,            // DoD=-100 (9 bits)
        };
        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaLargeColumn() throws QwpParseException {
        // 100K timestamps with varying patterns
        int count = 100000;
        long[] timestamps = new long[count];
        timestamps[0] = 1000000000L;
        timestamps[1] = 1000001000L;

        // Generate with some variation to exercise different buckets
        for (int i = 2; i < count; i++) {
            long prevDelta = timestamps[i - 1] - timestamps[i - 2];
            // Add some variation: mostly constant with occasional jumps
            int variation = (i % 100 == 0) ? (i % 1000) - 500 : 0;
            timestamps[i] = timestamps[i - 1] + prevDelta + variation;
        }

        testGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaRoundTrip() throws QwpParseException {
        // Comprehensive round-trip test with various patterns
        // Note: DoD values must fit in 32-bit signed range for proper encoding
        long[] timestamps = {
                0L,
                1L,
                2L,
                1000000000L,  // Large jump, but DoD fits in 32 bits
                1000000001L,
        };
        testGorillaRoundTrip(timestamps, null);
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testDecodeIncompleteData() {
        // Not enough data for encoding flag
        long address = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(5);
            // Only 1 byte but need at least encoding flag + 8 bytes for first timestamp
            Assert.assertThrows(QwpParseException.class, () ->
                    QwpTimestampDecoder.INSTANCE.decode(address, 1, 5, false, sink));
        } finally {
            Unsafe.free(address, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInvalidEncodingFlag() {
        long address = Unsafe.malloc(100, MemoryTag.NATIVE_DEFAULT);
        try {
            // Write invalid encoding flag
            Unsafe.getUnsafe().putByte(address, (byte) 0xFF);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(1);
            Assert.assertThrows(QwpParseException.class, () ->
                    QwpTimestampDecoder.INSTANCE.decode(address, 100, 1, false, sink));
        } finally {
            Unsafe.free(address, 100, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForFirstTimestamp() {
        // Gorilla encoding flag but not enough bytes for first timestamp
        long address = Unsafe.malloc(5, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, QwpTimestampDecoder.ENCODING_GORILLA);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(1);
            Assert.assertThrows(QwpParseException.class, () ->
                    QwpTimestampDecoder.INSTANCE.decode(address, 5, 1, false, sink));
        } finally {
            Unsafe.free(address, 5, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForSecondTimestamp() {
        // Gorilla encoding flag + first timestamp but not enough for second
        long address = Unsafe.malloc(12, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, QwpTimestampDecoder.ENCODING_GORILLA);
            Unsafe.getUnsafe().putLong(address + 1, 1000000L);

            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(2);
            Assert.assertThrows(QwpParseException.class, () ->
                    QwpTimestampDecoder.INSTANCE.decode(address, 12, 2, false, sink));
        } finally {
            Unsafe.free(address, 12, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Expected Size Tests ====================

    @Test
    public void testExpectedSize() {
        // expectedSize returns worst case (uncompressed)
        int rowCount = 10;
        int expected = 1 + rowCount * 8; // encoding flag + values
        Assert.assertEquals(expected, QwpTimestampDecoder.INSTANCE.expectedSize(rowCount, false));

        // With nullable
        int bitmapSize = (rowCount + 7) / 8;
        expected = bitmapSize + 1 + rowCount * 8;
        Assert.assertEquals(expected, QwpTimestampDecoder.INSTANCE.expectedSize(rowCount, true));
    }

    // ==================== Size Calculation Tests ====================

    @Test
    public void testCalculateGorillaSize() {
        // All zeros DoD - most compact
        long[] timestamps = {1000L, 2000L, 3000L, 4000L, 5000L};
        int size = QwpTimestampDecoder.calculateGorillaSize(timestamps, false);

        // Expected: 1 (flag) + 8 (first) + 8 (second) + ceil(3 bits / 8) = 17 + 1 = 18
        // Actually 3 timestamps with DoD=0 need 3 bits = 1 byte
        Assert.assertTrue(size > 0);
        Assert.assertTrue(size <= 1 + timestamps.length * 8); // Should be <= uncompressed
    }

    // ==================== Helper Methods ====================

    private void testGorillaRoundTrip(long[] timestamps, boolean[] nulls) throws QwpParseException {
        int rowCount = timestamps.length;
        boolean nullable = nulls != null;

        // Calculate size and allocate buffer
        int maxSize = QwpTimestampDecoder.calculateGorillaSize(timestamps, nullable) + 100; // extra padding
        long address = Unsafe.malloc(maxSize, MemoryTag.NATIVE_DEFAULT);
        try {
            // Encode
            long end = QwpTimestampDecoder.encodeGorilla(address, timestamps, nulls);
            int actualSize = (int) (end - address);

            // Decode
            QwpColumnDecoder.ArrayColumnSink sink = new QwpColumnDecoder.ArrayColumnSink(rowCount);
            int consumed = QwpTimestampDecoder.INSTANCE.decode(address, actualSize, rowCount, nullable, sink);

            Assert.assertEquals("Consumed bytes should match encoded size", actualSize, consumed);

            // Verify values
            for (int i = 0; i < rowCount; i++) {
                if (nullable && nulls[i]) {
                    Assert.assertTrue("Row " + i + " should be null", sink.isNull(i));
                } else {
                    Assert.assertFalse("Row " + i + " should not be null", sink.isNull(i));
                    Assert.assertEquals("Row " + i + " value mismatch", timestamps[i], sink.getValue(i));
                }
            }
        } finally {
            Unsafe.free(address, maxSize, MemoryTag.NATIVE_DEFAULT);
        }
    }
}

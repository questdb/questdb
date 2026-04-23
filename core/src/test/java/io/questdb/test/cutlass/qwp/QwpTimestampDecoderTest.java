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
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;
import io.questdb.cutlass.qwp.server.QwpStreamingDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpTimestampDecoderTest {

    @Test
    public void testDecodeGorillaBitAlignment() throws Exception {
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
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaBucketBoundaryValues() throws Exception {
        // Boundary values at the edge of signed two's complement ranges.
        // 7-bit signed range is [-64, 63], so DoD=64 must NOT use the 7-bit bucket
        // (64 overflows 7-bit signed to -64). Same for 9-bit (256) and 12-bit (2048).
        // With the bug, these boundary values get silently corrupted on round-trip.

        // DoD = 64 (just outside 7-bit signed max of 63)
        // t0=1000, t1=2000 (delta=1000), t2=3064 (delta=1064, DoD=64)
        assertGorillaRoundTrip(new long[]{1000L, 2000L, 3064L}, null);

        // DoD = -64 (at 7-bit signed min, should fit in 7-bit bucket)
        assertGorillaRoundTrip(new long[]{1000L, 2000L, 2936L}, null);

        // DoD = 256 (just outside 9-bit signed max of 255)
        assertGorillaRoundTrip(new long[]{1000L, 2000L, 3256L}, null);

        // DoD = -256 (at 9-bit signed min, should fit in 9-bit bucket)
        assertGorillaRoundTrip(new long[]{1000L, 2000L, 2744L}, null);

        // DoD = 2048 (just outside 12-bit signed max of 2047)
        assertGorillaRoundTrip(new long[]{1000L, 2000L, 5048L}, null);

        // DoD = -2048 (at 12-bit signed min, should fit in 12-bit bucket)
        assertGorillaRoundTrip(new long[]{1000L, 2000L, -48L}, null);
    }

    @Test
    public void testDecodeGorillaDecodesOnlyOnce() throws Exception {
        // Verify that Gorilla-compressed timestamps are decoded exactly once,
        // not twice (once in of() pre-scan + once in advanceRow() iteration).
        // With the double-decode bug, a batch of N timestamps causes 2*(N-2)
        // decode operations instead of N-2.
        int count = 1000;
        long[] timestamps = new long[count];
        timestamps[0] = 1_000_000_000L;
        timestamps[1] = 1_000_001_000L;
        for (int i = 2; i < count; i++) {
            long prevDelta = timestamps[i - 1] - timestamps[i - 2];
            int variation = (i % 50 == 0) ? (i % 500) - 250 : 0;
            timestamps[i] = timestamps[i - 1] + prevDelta + variation;
        }
        int expectedDecodeCount = count - 2;

        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                encoder.setGorillaEnabled(true);
                QwpTableBuffer buffer = new QwpTableBuffer("test_ts");
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                for (long ts : timestamps) {
                    tsCol.addLong(ts);
                    buffer.nextRow();
                }
                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();
                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();
                    int tsColIdx = table.getColumnCount() - 1;

                    // Iterate all rows (this triggers advanceRow on the timestamp cursor)
                    for (int i = 0; i < count; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();
                    }

                    QwpTimestampColumnCursor cursor = table.getTimestampColumn(tsColIdx);
                    Assert.assertEquals(
                            "Gorilla decoder should decode each value exactly once",
                            expectedDecodeCount,
                            cursor.getGorillaDecodeCount()
                    );
                }
            }
        });
    }

    @Test
    public void testDecodeGorillaDeltaHuge() throws Exception {
        // Huge DoD outside [-2047, 2048] -> 36-bit encoding
        // DoD = 10000
        long[] timestamps = {1000L, 2000L, 13_000L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaLarge() throws Exception {
        // Large DoD in range [-2047, 2048] but outside [-255, 256] -> 16-bit encoding
        // DoD = 1000
        long[] timestamps = {1000L, 2000L, 4000L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaLargeNegative() throws Exception {
        // Negative large DoD = -1000
        long[] timestamps = {10_000L, 11_000L, 11_000L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaMedium() throws Exception {
        // Medium DoD in range [-255, 256] but outside [-63, 64] -> 12-bit encoding
        // DoD = 200
        long[] timestamps = {1000L, 2000L, 3200L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaMediumNegative() throws Exception {
        // Negative medium DoD = -200
        long[] timestamps = {1000L, 2000L, 2800L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaSmall() throws Exception {
        // Small DoD in range [-63, 64] -> 9-bit encoding (2 prefix + 7 value)
        // t0=1000, t1=2000, t2=3050 -> delta0=1000, delta1=1050, DoD=50
        long[] timestamps = {1000L, 2000L, 3050L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaSmallNegative() throws Exception {
        // Negative small DoD
        // t0=1000, t1=2000, t2=2950 -> delta0=1000, delta1=950, DoD=-50
        long[] timestamps = {1000L, 2000L, 2950L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaDeltaZero() throws Exception {
        // Constant interval: DoD = 0 (1-bit encoding)
        // t0=1000, t1=2000, t2=3000 -> delta=1000, DoD=0
        long[] timestamps = {1000L, 2000L, 3000L, 4000L, 5000L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaEncodingFlag() throws Exception {
        // Single timestamp with Gorilla encoding
        long[] timestamps = {1_000_000_000L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaIrregularInterval() throws Exception {
        // Irregular intervals with varying deltas
        long[] timestamps = {
                1_000_000_000L,
                1_000_001_000L,  // delta=1000
                1_000_003_000L,  // delta=2000, DoD=1000
                1_000_003_500L,  // delta=500, DoD=-1500
                1_000_010_000L,  // delta=6500, DoD=6000
                1_000_010_001L   // delta=1, DoD=-6499
        };
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaLargeColumn() throws Exception {
        // 100K timestamps with varying patterns
        int count = 100_000;
        long[] timestamps = new long[count];
        timestamps[0] = 1_000_000_000L;
        timestamps[1] = 1_000_001_000L;

        // Generate with some variation to exercise different buckets
        for (int i = 2; i < count; i++) {
            long prevDelta = timestamps[i - 1] - timestamps[i - 2];
            // Add some variation: mostly constant with occasional jumps
            int variation = (i % 100 == 0) ? (i % 1000) - 500 : 0;
            timestamps[i] = timestamps[i - 1] + prevDelta + variation;
        }

        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaMixedBuckets() throws Exception {
        // Mix of different bucket sizes
        long[] timestamps = {
                1_000_000_000L,  // t0
                1_000_001_000L,  // t1, delta=1000
                1_000_002_000L,  // t2, DoD=0 (1-bit)
                1_000_003_050L,  // t3, DoD=50 (9-bit)
                1_000_004_200L,  // t4, DoD=100 (9-bit)
                1_000_006_200L,  // t5, DoD=850 (16-bit)
                1_000_106_200L   // t6, DoD=98000 (36-bit)
        };
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaRegularInterval() throws Exception {
        // Regular 1-second interval - all DoD should be 0
        long[] timestamps = new long[100];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = 1_000_000_000L + i * 1_000_000L; // 1ms = 1000000ns
        }
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaRoundTrip() throws Exception {
        // Comprehensive round-trip test with various patterns
        // Note: DoD values must fit in 32-bit signed range for proper encoding
        long[] timestamps = {
                0L,
                1L,
                2L,
                1_000_000_000L,  // Large jump, but DoD fits in 32 bits
                1_000_000_001L,
        };
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaSignedValues() throws Exception {
        // Test with timestamps that produce negative DoD values in all ranges
        // These require proper sign extension during decoding
        long[] timestamps = {
                10_000L,   // t0
                20_000L,   // t1, delta=10000
                29_940L,   // t2, DoD=-60 (in 7-bit range)
                39_780L,   // t3, DoD=-100 (in 9-bit range)
                48_420L,   // t4, DoD=-1500 (in 12-bit range)
                55_000L,   // t5, DoD=-3000 (in 32-bit range)
        };
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaTwoTimestamps() throws Exception {
        // Two timestamps - no delta-of-delta needed
        long[] timestamps = {1_000_000_000L, 1_000_001_000L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeGorillaWithNulls() throws Exception {
        long[] timestamps = {1000L, 2000L, 0L, 4000L, 0L, 6000L};
        boolean[] nulls = {false, false, true, false, true, false};
        assertGorillaRoundTrip(timestamps, nulls);
    }

    @Test
    public void testDecodeIncompleteData() {
        // no null bitmap, Gorilla flag but insufficient data for timestamps
        int bufSize = 32;
        long address = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            Unsafe.getUnsafe().putByte(address + 1, QwpTimestampColumnCursor.ENCODING_GORILLA);
            Unsafe.getUnsafe().putLong(address + 2, 1000L);
            Unsafe.getUnsafe().putLong(address + 10, 2000L);
            // No gorilla data follows for the remaining 3 rows

            QwpTimestampColumnCursor cursor = new QwpTimestampColumnCursor();
            // dataLength=18 means 0 bytes for gorilla data, decoder throws on first DoD read
            Assert.assertThrows(QwpParseException.class, () ->
                    cursor.of(address, 18, 5, TYPE_TIMESTAMP, true));
        } finally {
            Unsafe.free(address, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForFirstTimestamp() {
        // no null bitmap, Gorilla encoding flag but insufficient bytes for first timestamp
        int bufSize = 32;
        long address = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            Unsafe.getUnsafe().putByte(address + 1, QwpTimestampColumnCursor.ENCODING_GORILLA);
            // No valid first timestamp, and 3 rows require gorilla decoding

            QwpTimestampColumnCursor cursor = new QwpTimestampColumnCursor();
            // dataLength=6 means gorillaDataLength is negative, decoder throws immediately
            Assert.assertThrows(QwpParseException.class, () ->
                    cursor.of(address, 6, 3, TYPE_TIMESTAMP, true));
        } finally {
            Unsafe.free(address, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInsufficientDataForSecondTimestamp() {
        // no null bitmap, Gorilla encoding flag + first timestamp but not enough for second
        int bufSize = 32;
        long address = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            Unsafe.getUnsafe().putByte(address + 1, QwpTimestampColumnCursor.ENCODING_GORILLA);
            Unsafe.getUnsafe().putLong(address + 2, 1_000_000L);
            // No valid second timestamp, and 3 rows require gorilla decoding

            QwpTimestampColumnCursor cursor = new QwpTimestampColumnCursor();
            // dataLength=13 means gorillaDataLength is negative, decoder throws immediately
            Assert.assertThrows(QwpParseException.class, () ->
                    cursor.of(address, 13, 3, TYPE_TIMESTAMP, true));
        } finally {
            Unsafe.free(address, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeInvalidEncodingFlag() {
        int bufSize = 100;
        long address = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            // Write invalid encoding flag
            Unsafe.getUnsafe().putByte(address + 1, (byte) 0xFF);

            QwpTimestampColumnCursor cursor = new QwpTimestampColumnCursor();
            Assert.assertThrows(QwpParseException.class, () ->
                    cursor.of(address, bufSize, 1, TYPE_TIMESTAMP, true));
        } finally {
            Unsafe.free(address, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeUncompressedTimestamps() throws Exception {
        long[] timestamps = {1_000_000_000L, 1_000_001_000L, 1_000_002_000L, 1_000_003_000L, 1_000_004_000L};
        assertGorillaRoundTrip(timestamps, null);
    }

    @Test
    public void testDecodeUncompressedWithNulls() throws Exception {
        long[] timestamps = {1_000_000_000L, 0L, 1_000_002_000L, 0L, 1_000_004_000L};
        boolean[] nulls = {false, true, false, true, false};
        assertGorillaRoundTrip(timestamps, nulls);
    }

    @Test
    public void testDecodeZeroRows() throws QwpParseException {
        // Zero rows with gorillaEnabled=false: no null bitmap
        int size = 1;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(address, (byte) 0); // no null bitmap
            QwpTimestampColumnCursor cursor = new QwpTimestampColumnCursor();
            int consumed = cursor.of(address, size, 0, TYPE_TIMESTAMP, false);
            Assert.assertEquals(1, consumed);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertGorillaRoundTrip(long[] timestamps, boolean[] nulls) throws Exception {
        assertMemoryLeak(() -> {
            boolean nullable = nulls != null;
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                encoder.setGorillaEnabled(true);
                QwpTableBuffer buffer = new QwpTableBuffer("test_ts");
                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                for (int i = 0; i < timestamps.length; i++) {
                    if (nullable && nulls[i]) {
                        tsCol.addNull();
                    } else {
                        tsCol.addLong(timestamps[i]);
                    }
                    buffer.nextRow();
                }
                int size = encoder.encode(buffer, false);
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();
                try (QwpStreamingDecoder decoder = new QwpStreamingDecoder()) {
                    QwpMessageCursor msg = decoder.decode(ptr, size);
                    Assert.assertTrue(msg.hasNextTable());
                    QwpTableBlockCursor table = msg.nextTable();
                    Assert.assertEquals(timestamps.length, table.getRowCount());
                    int tsColIdx = table.getColumnCount() - 1;
                    for (int i = 0; i < timestamps.length; i++) {
                        Assert.assertTrue(table.hasNextRow());
                        table.nextRow();
                        if (nullable && nulls[i]) {
                            Assert.assertTrue("Row " + i + " should be null", table.isColumnNull(tsColIdx));
                        } else {
                            Assert.assertFalse("Row " + i + " should not be null", table.isColumnNull(tsColIdx));
                            QwpTimestampColumnCursor cursor = table.getTimestampColumn(tsColIdx);
                            Assert.assertEquals("Row " + i, timestamps[i], cursor.getTimestamp());
                        }
                    }
                    Assert.assertFalse(table.hasNextRow());
                }
            }
        });
    }
}

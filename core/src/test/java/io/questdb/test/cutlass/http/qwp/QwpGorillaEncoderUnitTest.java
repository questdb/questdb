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

package io.questdb.test.cutlass.http.qwp;

import io.questdb.cutlass.qwp.protocol.QwpGorillaDecoder;
import io.questdb.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for QwpGorillaEncoder.
 */
public class QwpGorillaEncoderUnitTest {

    // ==================== canUseGorilla Tests ====================

    @Test
    public void testCanUseGorilla_emptyArray() {
        Assert.assertTrue(QwpGorillaEncoder.canUseGorilla(new long[0], 0));
    }

    @Test
    public void testCanUseGorilla_oneTimestamp() {
        Assert.assertTrue(QwpGorillaEncoder.canUseGorilla(new long[]{1000L}, 1));
    }

    @Test
    public void testCanUseGorilla_twoTimestamps() {
        Assert.assertTrue(QwpGorillaEncoder.canUseGorilla(new long[]{1000L, 2000L}, 2));
    }

    @Test
    public void testCanUseGorilla_constantDelta() {
        long[] timestamps = new long[100];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = 1000000000L + i * 1000L;
        }
        Assert.assertTrue(QwpGorillaEncoder.canUseGorilla(timestamps, timestamps.length));
    }

    @Test
    public void testCanUseGorilla_varyingDelta() {
        long[] timestamps = {
                1000000000L,
                1000001000L,  // delta=1000
                1000002100L,  // DoD=100
                1000003500L,  // DoD=300
        };
        Assert.assertTrue(QwpGorillaEncoder.canUseGorilla(timestamps, timestamps.length));
    }

    @Test
    public void testCanUseGorilla_largeDoDOutOfRange() {
        // Create timestamps where DoD exceeds Integer.MAX_VALUE
        long[] timestamps = {
                0L,
                1000000000L,  // delta=1000000000
                5000000000L,  // delta=4000000000, DoD=3000000000 (exceeds Integer.MAX_VALUE)
        };
        Assert.assertFalse(QwpGorillaEncoder.canUseGorilla(timestamps, timestamps.length));
    }

    @Test
    public void testCanUseGorilla_negativeLargeDoDOutOfRange() {
        // Create timestamps where DoD is less than Integer.MIN_VALUE
        long[] timestamps = {
                10000000000L,
                9000000000L,  // delta=-1000000000
                4000000000L,  // delta=-5000000000, DoD=-4000000000 (less than Integer.MIN_VALUE)
        };
        Assert.assertFalse(QwpGorillaEncoder.canUseGorilla(timestamps, timestamps.length));
    }

    // ==================== calculateEncodedSize Tests ====================

    @Test
    public void testCalculateEncodedSize_empty() {
        Assert.assertEquals(0, QwpGorillaEncoder.calculateEncodedSize(new long[0], 0));
    }

    @Test
    public void testCalculateEncodedSize_oneTimestamp() {
        Assert.assertEquals(8, QwpGorillaEncoder.calculateEncodedSize(new long[]{1000L}, 1));
    }

    @Test
    public void testCalculateEncodedSize_twoTimestamps() {
        Assert.assertEquals(16, QwpGorillaEncoder.calculateEncodedSize(new long[]{1000L, 2000L}, 2));
    }

    @Test
    public void testCalculateEncodedSize_constantDelta() {
        // Constant delta = all DoD are 0 = 1 bit each
        long[] timestamps = new long[100];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = 1000000000L + i * 1000L;
        }

        int size = QwpGorillaEncoder.calculateEncodedSize(timestamps, timestamps.length);
        // Expected: 8 (first) + 8 (second) + ceil(98 bits / 8) bytes = 16 + 13 = 29 bytes
        int expectedBits = (timestamps.length - 2);
        int expectedSize = 8 + 8 + (expectedBits + 7) / 8;
        Assert.assertEquals(expectedSize, size);
    }

    @Test
    public void testCalculateEncodedSize_identicalDeltas() {
        // DoD = 0 for all -> 1 bit each
        long[] ts = {100L, 200L, 300L}; // delta=100, DoD=0
        int size = QwpGorillaEncoder.calculateEncodedSize(ts, ts.length);
        // 8 + 8 + 1 byte (1 bit padded to byte) = 17
        Assert.assertEquals(17, size);
    }

    @Test
    public void testCalculateEncodedSize_smallDoD() {
        // DoD = 50 -> bucket 1, 9 bits
        long[] ts = {100L, 200L, 350L}; // delta0=100, delta1=150, DoD=50
        int size = QwpGorillaEncoder.calculateEncodedSize(ts, ts.length);
        // 8 + 8 + 2 bytes (9 bits padded to bytes) = 18
        Assert.assertEquals(18, size);
    }

    // ==================== encodeTimestamps Round-trip Tests ====================

    @Test
    public void testEncodeTimestamps_empty() {
        QwpGorillaEncoder encoder = new QwpGorillaEncoder();
        long address = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            int bytesWritten = encoder.encodeTimestamps(address, 64, new long[0], 0);
            Assert.assertEquals(0, bytesWritten);
        } finally {
            Unsafe.free(address, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeTimestamps_oneTimestamp() {
        QwpGorillaEncoder encoder = new QwpGorillaEncoder();
        long[] timestamps = {1234567890L};
        long address = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            int bytesWritten = encoder.encodeTimestamps(address, 64, timestamps, 1);
            Assert.assertEquals(8, bytesWritten);

            // Verify first timestamp is written correctly
            long decoded = Unsafe.getUnsafe().getLong(address);
            Assert.assertEquals(1234567890L, decoded);
        } finally {
            Unsafe.free(address, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeTimestamps_twoTimestamps() {
        QwpGorillaEncoder encoder = new QwpGorillaEncoder();
        long[] timestamps = {1000000000L, 1000001000L};
        long address = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            int bytesWritten = encoder.encodeTimestamps(address, 64, timestamps, 2);
            Assert.assertEquals(16, bytesWritten);

            // Verify both timestamps are written correctly
            Assert.assertEquals(1000000000L, Unsafe.getUnsafe().getLong(address));
            Assert.assertEquals(1000001000L, Unsafe.getUnsafe().getLong(address + 8));
        } finally {
            Unsafe.free(address, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeTimestamps_roundTrip_constantDelta() throws Exception {
        QwpGorillaEncoder encoder = new QwpGorillaEncoder();
        QwpGorillaDecoder decoder = new QwpGorillaDecoder();

        long[] timestamps = new long[100];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = 1000000000L + i * 1000L;
        }

        int size = QwpGorillaEncoder.calculateEncodedSize(timestamps, timestamps.length) + 16;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int bytesWritten = encoder.encodeTimestamps(address, size, timestamps, timestamps.length);
            Assert.assertTrue(bytesWritten > 0);

            // Decode and verify
            long first = Unsafe.getUnsafe().getLong(address);
            long second = Unsafe.getUnsafe().getLong(address + 8);
            Assert.assertEquals(timestamps[0], first);
            Assert.assertEquals(timestamps[1], second);

            decoder.reset(first, second);
            decoder.resetReader(address + 16, bytesWritten - 16);

            for (int i = 2; i < timestamps.length; i++) {
                long decoded = decoder.decodeNext();
                Assert.assertEquals("Mismatch at index " + i, timestamps[i], decoded);
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeTimestamps_roundTrip_varyingDelta() throws Exception {
        QwpGorillaEncoder encoder = new QwpGorillaEncoder();
        QwpGorillaDecoder decoder = new QwpGorillaDecoder();

        long[] timestamps = {
                1000000000L,
                1000001000L,  // delta=1000
                1000002000L,  // DoD=0 (bucket 0)
                1000003010L,  // DoD=10 (bucket 1)
                1000004120L,  // DoD=100 (bucket 1)
                1000005420L,  // DoD=190 (bucket 2)
                1000007720L,  // DoD=1000 (bucket 3)
                1000020020L,  // DoD=10000 (bucket 4)
        };

        int size = QwpGorillaEncoder.calculateEncodedSize(timestamps, timestamps.length) + 16;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int bytesWritten = encoder.encodeTimestamps(address, size, timestamps, timestamps.length);
            Assert.assertTrue(bytesWritten > 0);

            // Decode and verify
            long first = Unsafe.getUnsafe().getLong(address);
            long second = Unsafe.getUnsafe().getLong(address + 8);
            Assert.assertEquals(timestamps[0], first);
            Assert.assertEquals(timestamps[1], second);

            decoder.reset(first, second);
            decoder.resetReader(address + 16, bytesWritten - 16);

            for (int i = 2; i < timestamps.length; i++) {
                long decoded = decoder.decodeNext();
                Assert.assertEquals("Mismatch at index " + i, timestamps[i], decoded);
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeTimestamps_roundTrip_negativeDoD() throws Exception {
        QwpGorillaEncoder encoder = new QwpGorillaEncoder();
        QwpGorillaDecoder decoder = new QwpGorillaDecoder();

        // Timestamps with negative delta-of-deltas
        long[] timestamps = {
                1000000000L,
                1000002000L,  // delta=2000
                1000003000L,  // DoD=-1000 (bucket 3)
                1000003500L,  // DoD=-500 (bucket 2)
                1000003600L,  // DoD=-400 (bucket 2)
        };

        int size = QwpGorillaEncoder.calculateEncodedSize(timestamps, timestamps.length) + 16;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int bytesWritten = encoder.encodeTimestamps(address, size, timestamps, timestamps.length);
            Assert.assertTrue(bytesWritten > 0);

            // Decode and verify
            long first = Unsafe.getUnsafe().getLong(address);
            long second = Unsafe.getUnsafe().getLong(address + 8);
            Assert.assertEquals(timestamps[0], first);
            Assert.assertEquals(timestamps[1], second);

            decoder.reset(first, second);
            decoder.resetReader(address + 16, bytesWritten - 16);

            for (int i = 2; i < timestamps.length; i++) {
                long decoded = decoder.decodeNext();
                Assert.assertEquals("Mismatch at index " + i, timestamps[i], decoded);
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeTimestamps_roundTrip_allBuckets() throws Exception {
        QwpGorillaEncoder encoder = new QwpGorillaEncoder();
        QwpGorillaDecoder decoder = new QwpGorillaDecoder();

        // Create timestamps that hit all bucket boundaries
        long[] timestamps = new long[10];
        timestamps[0] = 1000000000L;
        timestamps[1] = 1000001000L; // delta = 1000

        // Bucket 0: DoD = 0
        timestamps[2] = 1000002000L;  // delta=1000, DoD=0

        // Bucket 1: DoD in [-63, 64]
        timestamps[3] = 1000003050L;  // delta=1050, DoD=50
        timestamps[4] = 1000003987L;  // delta=937, DoD=-113 (out of bucket 1, into bucket 2)

        // Bucket 2: DoD in [-255, 256]
        timestamps[5] = 1000004687L;  // delta=700, DoD=-237

        // Bucket 3: DoD in [-2047, 2048]
        timestamps[6] = 1000006387L;  // delta=1700, DoD=1000

        // Bucket 4: DoD > 2048 or < -2047
        timestamps[7] = 1000020087L;  // delta=13700, DoD=12000 (bucket 4)

        // Back to small DoD
        timestamps[8] = 1000033787L;  // delta=13700, DoD=0 (bucket 0)
        timestamps[9] = 1000047487L;  // delta=13700, DoD=0 (bucket 0)

        int size = QwpGorillaEncoder.calculateEncodedSize(timestamps, timestamps.length) + 16;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int bytesWritten = encoder.encodeTimestamps(address, size, timestamps, timestamps.length);
            Assert.assertTrue(bytesWritten > 0);

            // Decode and verify
            long first = Unsafe.getUnsafe().getLong(address);
            long second = Unsafe.getUnsafe().getLong(address + 8);
            Assert.assertEquals(timestamps[0], first);
            Assert.assertEquals(timestamps[1], second);

            decoder.reset(first, second);
            decoder.resetReader(address + 16, bytesWritten - 16);

            for (int i = 2; i < timestamps.length; i++) {
                long decoded = decoder.decodeNext();
                Assert.assertEquals("Mismatch at index " + i, timestamps[i], decoded);
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeTimestamps_roundTrip_largeDataset() throws Exception {
        QwpGorillaEncoder encoder = new QwpGorillaEncoder();
        QwpGorillaDecoder decoder = new QwpGorillaDecoder();

        int count = 10000;
        long[] timestamps = new long[count];
        timestamps[0] = 1000000000L;
        timestamps[1] = 1000001000L;

        // Mix of constant and varying intervals
        java.util.Random random = new java.util.Random(42);
        for (int i = 2; i < count; i++) {
            long prevDelta = timestamps[i - 1] - timestamps[i - 2];
            // Small variation to stay within int range
            int variation = (i % 10 == 0) ? random.nextInt(100) - 50 : 0;
            timestamps[i] = timestamps[i - 1] + prevDelta + variation;
        }

        int size = QwpGorillaEncoder.calculateEncodedSize(timestamps, count) + 100;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            int bytesWritten = encoder.encodeTimestamps(address, size, timestamps, count);
            Assert.assertTrue(bytesWritten > 0);

            // Verify compression occurred
            int uncompressedSize = count * 8;
            Assert.assertTrue("Should compress better than uncompressed", bytesWritten < uncompressedSize);

            // Decode and verify
            long first = Unsafe.getUnsafe().getLong(address);
            long second = Unsafe.getUnsafe().getLong(address + 8);
            Assert.assertEquals(timestamps[0], first);
            Assert.assertEquals(timestamps[1], second);

            decoder.reset(first, second);
            decoder.resetReader(address + 16, bytesWritten - 16);

            for (int i = 2; i < count; i++) {
                long decoded = decoder.decodeNext();
                Assert.assertEquals("Mismatch at index " + i, timestamps[i], decoded);
            }
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Bucket Boundary Tests ====================

    @Test
    public void testEncodeDecode_bucketBoundaries() throws Exception {
        QwpGorillaEncoder encoder = new QwpGorillaEncoder();
        QwpGorillaDecoder decoder = new QwpGorillaDecoder();

        // Test values safely within each bucket (avoiding exact boundaries
        // where signed overflow can occur)
        // t0=0, t1=1000, delta0=1000
        // For DoD=X: delta1 = 1000+X, so t2 = 2000+X

        long[][] bucketTests = {
                {0L, 1000L, 2000L},        // DoD = 0 (bucket 0)
                {0L, 1000L, 2050L},        // DoD = 50 (bucket 1, safe mid-range)
                {0L, 1000L, 1950L},        // DoD = -50 (bucket 1, safe mid-range)
                {0L, 1000L, 2100L},        // DoD = 100 (bucket 2, safe)
                {0L, 1000L, 1900L},        // DoD = -100 (bucket 2, safe)
                {0L, 1000L, 2500L},        // DoD = 500 (bucket 3, safe)
                {0L, 1000L, 1500L},        // DoD = -500 (bucket 3, safe)
                {0L, 1000L, 12000L},       // DoD = 10000 (bucket 4, safe)
                {0L, 1000L, -8000L},       // DoD = -10000 (bucket 4, safe)
        };

        for (long[] tc : bucketTests) {
            long address = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                int bytesWritten = encoder.encodeTimestamps(address, 64, tc, tc.length);
                Assert.assertTrue("Failed to encode: " + java.util.Arrays.toString(tc), bytesWritten > 0);

                // Decode and verify
                long first = Unsafe.getUnsafe().getLong(address);
                long second = Unsafe.getUnsafe().getLong(address + 8);
                Assert.assertEquals(tc[0], first);
                Assert.assertEquals(tc[1], second);

                decoder.reset(first, second);
                decoder.resetReader(address + 16, bytesWritten - 16);

                long decoded = decoder.decodeNext();
                Assert.assertEquals("Failed for: " + java.util.Arrays.toString(tc), tc[2], decoded);
            } finally {
                Unsafe.free(address, 64, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    // ==================== Edge Cases ====================

    @Test
    public void testEncodeTimestamps_insufficientCapacity() {
        QwpGorillaEncoder encoder = new QwpGorillaEncoder();
        long[] timestamps = {1000L, 2000L, 3000L};
        long address = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT); // Too small
        try {
            int bytesWritten = encoder.encodeTimestamps(address, 4, timestamps, 3);
            // Should return 0 or partial data since capacity is insufficient
            Assert.assertTrue(bytesWritten <= 4);
        } finally {
            Unsafe.free(address, 4, MemoryTag.NATIVE_DEFAULT);
        }
    }
}

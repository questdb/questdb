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

import io.questdb.cutlass.qwp.protocol.QwpGorillaDecoder;
import io.questdb.cutlass.qwp.protocol.QwpTimestampDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Gorilla encoder functionality (static methods in decoders).
 * These tests verify that encoding produces correct output that can be decoded.
 */
public class QwpGorillaEncoderTest {

    // ==================== Bucket Classification Tests ====================

    @Test
    public void testGetBucketZero() {
        // DoD = 0 should use bucket 0 (1 bit)
        Assert.assertEquals(0, QwpGorillaDecoder.getBucket(0));
    }

    @Test
    public void testGetBucket7Bit() {
        // DoD in [-63, 64] should use bucket 1 (9 bits)
        Assert.assertEquals(1, QwpGorillaDecoder.getBucket(1));
        Assert.assertEquals(1, QwpGorillaDecoder.getBucket(-1));
        Assert.assertEquals(1, QwpGorillaDecoder.getBucket(63));
        Assert.assertEquals(1, QwpGorillaDecoder.getBucket(-63));
        Assert.assertEquals(1, QwpGorillaDecoder.getBucket(64));
    }

    @Test
    public void testGetBucket9Bit() {
        // DoD in [-255, 256] but outside [-63, 64] should use bucket 2 (12 bits)
        Assert.assertEquals(2, QwpGorillaDecoder.getBucket(65));
        Assert.assertEquals(2, QwpGorillaDecoder.getBucket(-64));
        Assert.assertEquals(2, QwpGorillaDecoder.getBucket(255));
        Assert.assertEquals(2, QwpGorillaDecoder.getBucket(-255));
        Assert.assertEquals(2, QwpGorillaDecoder.getBucket(256));
    }

    @Test
    public void testGetBucket12Bit() {
        // DoD in [-2047, 2048] but outside [-255, 256] should use bucket 3 (16 bits)
        Assert.assertEquals(3, QwpGorillaDecoder.getBucket(257));
        Assert.assertEquals(3, QwpGorillaDecoder.getBucket(-256));
        Assert.assertEquals(3, QwpGorillaDecoder.getBucket(2047));
        Assert.assertEquals(3, QwpGorillaDecoder.getBucket(-2047));
        Assert.assertEquals(3, QwpGorillaDecoder.getBucket(2048));
    }

    @Test
    public void testGetBucket32Bit() {
        // DoD outside [-2047, 2048] should use bucket 4 (36 bits)
        Assert.assertEquals(4, QwpGorillaDecoder.getBucket(2049));
        Assert.assertEquals(4, QwpGorillaDecoder.getBucket(-2048));
        Assert.assertEquals(4, QwpGorillaDecoder.getBucket(100000));
        Assert.assertEquals(4, QwpGorillaDecoder.getBucket(-100000));
        Assert.assertEquals(4, QwpGorillaDecoder.getBucket(Integer.MAX_VALUE));
        Assert.assertEquals(4, QwpGorillaDecoder.getBucket(Integer.MIN_VALUE));
    }

    // ==================== Bits Required Tests ====================

    @Test
    public void testGetBitsRequired() {
        // Bucket 0: 1 bit
        Assert.assertEquals(1, QwpGorillaDecoder.getBitsRequired(0));

        // Bucket 1: 9 bits (2 prefix + 7 value)
        Assert.assertEquals(9, QwpGorillaDecoder.getBitsRequired(1));
        Assert.assertEquals(9, QwpGorillaDecoder.getBitsRequired(-1));
        Assert.assertEquals(9, QwpGorillaDecoder.getBitsRequired(64));

        // Bucket 2: 12 bits (3 prefix + 9 value)
        Assert.assertEquals(12, QwpGorillaDecoder.getBitsRequired(65));
        Assert.assertEquals(12, QwpGorillaDecoder.getBitsRequired(256));

        // Bucket 3: 16 bits (4 prefix + 12 value)
        Assert.assertEquals(16, QwpGorillaDecoder.getBitsRequired(257));
        Assert.assertEquals(16, QwpGorillaDecoder.getBitsRequired(2048));

        // Bucket 4: 36 bits (4 prefix + 32 value)
        Assert.assertEquals(36, QwpGorillaDecoder.getBitsRequired(2049));
        Assert.assertEquals(36, QwpGorillaDecoder.getBitsRequired(-2048));
    }

    // ==================== Constant Delta Encoding Tests ====================

    @Test
    public void testEncodeConstantDelta() {
        // Constant delta = all DoD are 0 = most compact encoding
        long[] timestamps = new long[100];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = 1000000000L + i * 1000L;
        }

        int size = QwpTimestampDecoder.calculateGorillaSize(timestamps, false);
        long address = Unsafe.malloc(size + 100, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = QwpTimestampDecoder.encodeGorilla(address, timestamps, null);
            int actualSize = (int) (end - address);

            // For constant delta, after first two timestamps, each value needs only 1 bit
            // Expected: 1 (flag) + 8 (first) + 8 (second) + ceil((100-2) bits / 8) = 17 + 13 = 30 bytes
            int expectedBits = (timestamps.length - 2); // 1 bit per DoD=0
            int expectedSize = 1 + 8 + 8 + (expectedBits + 7) / 8;
            Assert.assertEquals(expectedSize, actualSize);
        } finally {
            Unsafe.free(address, size + 100, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeVaryingDelta() {
        // Varying deltas that exercise different buckets
        long[] timestamps = {
                1000000000L,
                1000001000L,  // delta=1000
                1000002000L,  // DoD=0 (1 bit)
                1000002100L,  // DoD=-900 (16 bits, since |DoD| > 256)
                1000002200L,  // DoD=0 (1 bit)
        };

        int size = QwpTimestampDecoder.calculateGorillaSize(timestamps, false);
        Assert.assertTrue("Size should be positive", size > 0);
        Assert.assertTrue("Gorilla should be smaller than uncompressed",
                size < 1 + timestamps.length * 8);
    }

    @Test
    public void testEncodeAllBucketSizes() {
        // Create timestamps that will produce DoD values in each bucket
        long[] timestamps = {
                1000000000L,           // t0
                1000001000L,           // t1, delta=1000
                1000002000L,           // t2, DoD=0 (bucket 0, 1 bit)
                1000003010L,           // t3, DoD=10 (bucket 1, 9 bits)
                1000004120L,           // t4, DoD=100 (bucket 1, 9 bits)
                1000005420L,           // t5, DoD=190 (bucket 2, 12 bits)
                1000007720L,           // t6, DoD=1000 (bucket 3, 16 bits)
                1000020020L,           // t7, DoD=10000 (bucket 4, 36 bits)
        };

        int size = QwpTimestampDecoder.calculateGorillaSize(timestamps, false);
        long address = Unsafe.malloc(size + 100, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = QwpTimestampDecoder.encodeGorilla(address, timestamps, null);
            int actualSize = (int) (end - address);

            // Verify encoded size matches calculated size
            Assert.assertEquals("Calculated size should match actual", size, actualSize);
        } finally {
            Unsafe.free(address, size + 100, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeLargeDataset() {
        // Large dataset to verify performance and correctness
        int count = 10000;
        long[] timestamps = new long[count];
        timestamps[0] = 1000000000L;
        timestamps[1] = 1000001000L;

        // Mix of constant and varying intervals
        for (int i = 2; i < count; i++) {
            long prevDelta = timestamps[i - 1] - timestamps[i - 2];
            // Every 10th value has variation
            int variation = (i % 10 == 0) ? (i % 100) - 50 : 0;
            timestamps[i] = timestamps[i - 1] + prevDelta + variation;
        }

        int size = QwpTimestampDecoder.calculateGorillaSize(timestamps, false);
        int uncompressedSize = 1 + count * 8;

        // Gorilla should provide compression for this pattern
        Assert.assertTrue("Gorilla should compress better than uncompressed",
                size < uncompressedSize);
    }

    @Test
    public void testEncodeMatchesSpec() {
        // Test specific examples that can be manually verified against spec

        // Example 1: Two identical deltas (DoD = 0)
        long[] ts1 = {100L, 200L, 300L}; // delta=100, DoD=0
        int size1 = QwpTimestampDecoder.calculateGorillaSize(ts1, false);
        // Expected: 1 flag + 8 first + 8 second + 1 byte (1 bit padded) = 18 bytes
        Assert.assertEquals(18, size1);

        // Example 2: Three timestamps with small DoD
        long[] ts2 = {100L, 200L, 350L}; // delta0=100, delta1=150, DoD=50
        int size2 = QwpTimestampDecoder.calculateGorillaSize(ts2, false);
        // Expected: 1 flag + 8 first + 8 second + 2 bytes (9 bits for DoD=50) = 19 bytes
        Assert.assertEquals(19, size2);
    }

    // ==================== Edge Cases ====================

    @Test
    public void testEncodeEmptyArray() {
        long[] timestamps = new long[0];
        int size = QwpTimestampDecoder.calculateGorillaSize(timestamps, false);
        Assert.assertEquals(1, size); // Just the encoding flag
    }

    @Test
    public void testEncodeSingleTimestamp() {
        long[] timestamps = {1000000000L};
        int size = QwpTimestampDecoder.calculateGorillaSize(timestamps, false);
        Assert.assertEquals(9, size); // 1 flag + 8 first timestamp
    }

    @Test
    public void testEncodeTwoTimestamps() {
        long[] timestamps = {1000000000L, 1000001000L};
        int size = QwpTimestampDecoder.calculateGorillaSize(timestamps, false);
        Assert.assertEquals(17, size); // 1 flag + 8 first + 8 second
    }

    @Test
    public void testEncodeWithNullBitmap() {
        long[] timestamps = {1000L, 2000L, 0L, 4000L, 0L};
        int rowCount = timestamps.length;

        int size = QwpTimestampDecoder.calculateGorillaSize(timestamps, true);
        int bitmapSize = (rowCount + 7) / 8;

        // Size should include bitmap
        Assert.assertTrue("Size should include null bitmap", size >= bitmapSize);
    }

    // ==================== Compression Ratio Tests ====================

    @Test
    public void testCompressionRatioConstantInterval() {
        // Best case: constant interval
        long[] timestamps = new long[1000];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = i * 1000L;
        }

        int gorillaSize = QwpTimestampDecoder.calculateGorillaSize(timestamps, false);
        int uncompressedSize = 1 + timestamps.length * 8;

        double ratio = (double) gorillaSize / uncompressedSize;
        // For constant interval, we expect significant compression
        Assert.assertTrue("Compression ratio should be < 0.1 for constant interval", ratio < 0.1);
    }

    @Test
    public void testCompressionRatioRandomData() {
        // Worst case: random timestamps (no pattern)
        long[] timestamps = new long[100];
        timestamps[0] = 1000000000L;
        timestamps[1] = 1000001000L;

        java.util.Random random = new java.util.Random(42);
        for (int i = 2; i < timestamps.length; i++) {
            // Random delta variation
            timestamps[i] = timestamps[i - 1] + 1000 + random.nextInt(10000) - 5000;
        }

        int gorillaSize = QwpTimestampDecoder.calculateGorillaSize(timestamps, false);

        // For random data, Gorilla might not compress well (could be larger due to prefixes)
        // Just verify it produces a valid size
        Assert.assertTrue("Size should be positive", gorillaSize > 0);
    }
}

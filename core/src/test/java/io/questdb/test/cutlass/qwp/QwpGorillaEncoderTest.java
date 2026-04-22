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

import io.questdb.client.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.cutlass.qwp.protocol.QwpBitReader;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class QwpGorillaEncoderTest {

    @Test
    public void testCalculateEncodedSizeConstantDelta() throws Exception {
        assertMemoryLeak(() -> {
            long[] timestamps = new long[100];
            for (int i = 0; i < timestamps.length; i++) {
                timestamps[i] = 1_000_000_000L + i * 1000L;
            }
            long src = putTimestamps(timestamps);
            try {
                int size = QwpGorillaEncoder.calculateEncodedSize(src, timestamps.length);
                // 8 (first) + 8 (second) + ceil(98 bits / 8) = 29
                int expectedBits = timestamps.length - 2;
                int expectedSize = 8 + 8 + (expectedBits + 7) / 8;
                Assert.assertEquals(expectedSize, size);
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCalculateEncodedSizeEmpty() throws Exception {
        assertMemoryLeak(() -> Assert.assertEquals(0, QwpGorillaEncoder.calculateEncodedSize(0, 0)));
    }

    @Test
    public void testCalculateEncodedSizeIdenticalDeltas() throws Exception {
        assertMemoryLeak(() -> {
            long[] ts = {100L, 200L, 300L}; // delta=100, DoD=0
            long src = putTimestamps(ts);
            try {
                int size = QwpGorillaEncoder.calculateEncodedSize(src, ts.length);
                // 8 + 8 + 1 byte (1 bit padded to byte) = 17
                Assert.assertEquals(17, size);
            } finally {
                Unsafe.free(src, (long) ts.length * 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCalculateEncodedSizeOneTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            long[] ts = {1000L};
            long src = putTimestamps(ts);
            try {
                Assert.assertEquals(8, QwpGorillaEncoder.calculateEncodedSize(src, 1));
            } finally {
                Unsafe.free(src, 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCalculateEncodedSizeSmallDoD() throws Exception {
        assertMemoryLeak(() -> {
            long[] ts = {100L, 200L, 350L}; // delta0=100, delta1=150, DoD=50
            long src = putTimestamps(ts);
            try {
                int size = QwpGorillaEncoder.calculateEncodedSize(src, ts.length);
                // 8 + 8 + 2 bytes (9 bits padded to bytes) = 18
                Assert.assertEquals(18, size);
            } finally {
                Unsafe.free(src, (long) ts.length * 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCalculateEncodedSizeTwoTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            long[] ts = {1000L, 2000L};
            long src = putTimestamps(ts);
            try {
                Assert.assertEquals(16, QwpGorillaEncoder.calculateEncodedSize(src, 2));
            } finally {
                Unsafe.free(src, 16, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCanUseGorillaConstantDelta() throws Exception {
        assertMemoryLeak(() -> {
            long[] timestamps = new long[100];
            for (int i = 0; i < timestamps.length; i++) {
                timestamps[i] = 1_000_000_000L + i * 1000L;
            }
            long src = putTimestamps(timestamps);
            try {
                Assert.assertTrue(QwpGorillaEncoder.canUseGorilla(src, timestamps.length));
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCanUseGorillaEmpty() throws Exception {
        assertMemoryLeak(() -> Assert.assertTrue(QwpGorillaEncoder.canUseGorilla(0, 0)));
    }

    @Test
    public void testCanUseGorillaLargeDoDOutOfRange() throws Exception {
        assertMemoryLeak(() -> {
            // DoD = 3_000_000_000 exceeds Integer.MAX_VALUE
            long[] timestamps = {
                    0L,
                    1_000_000_000L,  // delta=1_000_000_000
                    5_000_000_000L,  // delta=4_000_000_000, DoD=3_000_000_000
            };
            long src = putTimestamps(timestamps);
            try {
                Assert.assertFalse(QwpGorillaEncoder.canUseGorilla(src, timestamps.length));
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCanUseGorillaNegativeLargeDoDOutOfRange() throws Exception {
        assertMemoryLeak(() -> {
            // DoD = -4_000_000_000 is less than Integer.MIN_VALUE
            long[] timestamps = {
                    10_000_000_000L,
                    9_000_000_000L,   // delta=-1_000_000_000
                    4_000_000_000L,   // delta=-5_000_000_000, DoD=-4_000_000_000
            };
            long src = putTimestamps(timestamps);
            try {
                Assert.assertFalse(QwpGorillaEncoder.canUseGorilla(src, timestamps.length));
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCanUseGorillaOneTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            long[] ts = {1000L};
            long src = putTimestamps(ts);
            try {
                Assert.assertTrue(QwpGorillaEncoder.canUseGorilla(src, 1));
            } finally {
                Unsafe.free(src, 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCanUseGorillaTwoTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            long[] ts = {1000L, 2000L};
            long src = putTimestamps(ts);
            try {
                Assert.assertTrue(QwpGorillaEncoder.canUseGorilla(src, 2));
            } finally {
                Unsafe.free(src, 16, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCanUseGorillaVaryingDelta() throws Exception {
        assertMemoryLeak(() -> {
            long[] timestamps = {
                    1_000_000_000L,
                    1_000_001_000L,  // delta=1000
                    1_000_002_100L,  // DoD=100
                    1_000_003_500L,  // DoD=300
            };
            long src = putTimestamps(timestamps);
            try {
                Assert.assertTrue(QwpGorillaEncoder.canUseGorilla(src, timestamps.length));
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCompressionRatioConstantInterval() throws Exception {
        assertMemoryLeak(() -> {
            long[] timestamps = new long[1000];
            for (int i = 0; i < timestamps.length; i++) {
                timestamps[i] = i * 1000L;
            }
            long src = putTimestamps(timestamps);
            try {
                int gorillaSize = QwpGorillaEncoder.calculateEncodedSize(src, timestamps.length);
                int uncompressedSize = timestamps.length * 8;
                double ratio = (double) gorillaSize / uncompressedSize;
                Assert.assertTrue("Compression ratio should be < 0.1 for constant interval", ratio < 0.1);
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCompressionRatioRandomData() throws Exception {
        assertMemoryLeak(() -> {
            long[] timestamps = new long[100];
            timestamps[0] = 1_000_000_000L;
            timestamps[1] = 1_000_001_000L;
            java.util.Random random = new java.util.Random(42);
            for (int i = 2; i < timestamps.length; i++) {
                timestamps[i] = timestamps[i - 1] + 1000 + random.nextInt(10_000) - 5000;
            }
            long src = putTimestamps(timestamps);
            try {
                int gorillaSize = QwpGorillaEncoder.calculateEncodedSize(src, timestamps.length);
                Assert.assertTrue("Size should be positive", gorillaSize > 0);
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeDecodeBucketBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            QwpBitReader reader = new QwpBitReader();

            // t0=0, t1=10_000, delta0=10_000
            // For DoD=X: delta1 = 10_000+X, so t2 = 20_000+X
            long[][] bucketTests = {
                    {0L, 10_000L, 20_000L},         // DoD = 0 (bucket 0)
                    {0L, 10_000L, 20_063L},         // DoD = 63 (bucket 1 max)
                    {0L, 10_000L, 19_936L},         // DoD = -64 (bucket 1 min)
                    {0L, 10_000L, 20_064L},         // DoD = 64 (bucket 2 start)
                    {0L, 10_000L, 19_935L},         // DoD = -65 (bucket 2 start)
                    {0L, 10_000L, 20_255L},         // DoD = 255 (bucket 2 max)
                    {0L, 10_000L, 19_744L},         // DoD = -256 (bucket 2 min)
                    {0L, 10_000L, 20_256L},         // DoD = 256 (bucket 3 start)
                    {0L, 10_000L, 19_743L},         // DoD = -257 (bucket 3 start)
                    {0L, 10_000L, 22_047L},         // DoD = 2047 (bucket 3 max)
                    {0L, 10_000L, 17_952L},         // DoD = -2048 (bucket 3 min)
                    {0L, 10_000L, 22_048L},         // DoD = 2048 (bucket 4 start)
                    {0L, 10_000L, 17_951L},         // DoD = -2049 (bucket 4 start)
                    {0L, 10_000L, 110_000L},        // DoD = 100_000 (bucket 4, large)
                    {0L, 10_000L, -80_000L},        // DoD = -100_000 (bucket 4, large)
            };

            for (long[] tc : bucketTests) {
                long src = putTimestamps(tc);
                long dst = Unsafe.malloc(64, MemoryTag.NATIVE_ILP_RSS);
                try {
                    int bytesWritten = encoder.encodeTimestamps(dst, 64, src, tc.length);
                    Assert.assertTrue("Failed to encode: " + java.util.Arrays.toString(tc), bytesWritten > 0);

                    long first = Unsafe.getLong(dst);
                    long second = Unsafe.getLong(dst + 8);
                    Assert.assertEquals(tc[0], first);
                    Assert.assertEquals(tc[1], second);

                    reader.reset(dst + 16, bytesWritten - 16);
                    long dod = decodeDoD(reader);
                    long delta = (second - first) + dod;
                    long decoded = second + delta;
                    Assert.assertEquals("Failed for: " + java.util.Arrays.toString(tc), tc[2], decoded);
                } finally {
                    Unsafe.free(src, (long) tc.length * 8, MemoryTag.NATIVE_ILP_RSS);
                    Unsafe.free(dst, 64, MemoryTag.NATIVE_ILP_RSS);
                }
            }
        });
    }

    @Test
    public void testEncodeTimestampsEmpty() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            long dst = Unsafe.malloc(64, MemoryTag.NATIVE_ILP_RSS);
            try {
                int bytesWritten = encoder.encodeTimestamps(dst, 64, 0, 0);
                Assert.assertEquals(0, bytesWritten);
            } finally {
                Unsafe.free(dst, 64, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeTimestampsOneTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            long[] timestamps = {1_234_567_890L};
            long src = putTimestamps(timestamps);
            long dst = Unsafe.malloc(64, MemoryTag.NATIVE_ILP_RSS);
            try {
                int bytesWritten = encoder.encodeTimestamps(dst, 64, src, 1);
                Assert.assertEquals(8, bytesWritten);
                Assert.assertEquals(1_234_567_890L, Unsafe.getLong(dst));
            } finally {
                Unsafe.free(src, 8, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, 64, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeTimestampsRoundTripAllBuckets() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            QwpBitReader reader = new QwpBitReader();

            long[] timestamps = new long[10];
            timestamps[0] = 1_000_000_000L;
            timestamps[1] = 1_000_001_000L; // delta = 1000
            timestamps[2] = 1_000_002_000L; // DoD=0 (bucket 0)
            timestamps[3] = 1_000_003_050L; // DoD=50 (bucket 1)
            timestamps[4] = 1_000_003_987L; // DoD=-113 (bucket 2)
            timestamps[5] = 1_000_004_687L; // DoD=-237 (bucket 2)
            timestamps[6] = 1_000_006_387L; // DoD=1000 (bucket 3)
            timestamps[7] = 1_000_020_087L; // DoD=12000 (bucket 4)
            timestamps[8] = 1_000_033_787L; // DoD=0 (bucket 0)
            timestamps[9] = 1_000_047_487L; // DoD=0 (bucket 0)

            long src = putTimestamps(timestamps);
            int capacity = QwpGorillaEncoder.calculateEncodedSize(src, timestamps.length) + 16;
            long dst = Unsafe.malloc(capacity, MemoryTag.NATIVE_ILP_RSS);
            try {
                int bytesWritten = encoder.encodeTimestamps(dst, capacity, src, timestamps.length);
                Assert.assertTrue(bytesWritten > 0);

                long first = Unsafe.getLong(dst);
                long second = Unsafe.getLong(dst + 8);
                Assert.assertEquals(timestamps[0], first);
                Assert.assertEquals(timestamps[1], second);

                reader.reset(dst + 16, bytesWritten - 16);
                long prevTs = second;
                long prevDelta = second - first;
                for (int i = 2; i < timestamps.length; i++) {
                    long dod = decodeDoD(reader);
                    long delta = prevDelta + dod;
                    long ts = prevTs + delta;
                    Assert.assertEquals("Mismatch at index " + i, timestamps[i], ts);
                    prevDelta = delta;
                    prevTs = ts;
                }
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, capacity, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeTimestampsRoundTripConstantDelta() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            QwpBitReader reader = new QwpBitReader();

            long[] timestamps = new long[100];
            for (int i = 0; i < timestamps.length; i++) {
                timestamps[i] = 1_000_000_000L + i * 1000L;
            }

            long src = putTimestamps(timestamps);
            int capacity = QwpGorillaEncoder.calculateEncodedSize(src, timestamps.length) + 16;
            long dst = Unsafe.malloc(capacity, MemoryTag.NATIVE_ILP_RSS);
            try {
                int bytesWritten = encoder.encodeTimestamps(dst, capacity, src, timestamps.length);
                Assert.assertTrue(bytesWritten > 0);

                long first = Unsafe.getLong(dst);
                long second = Unsafe.getLong(dst + 8);
                Assert.assertEquals(timestamps[0], first);
                Assert.assertEquals(timestamps[1], second);

                reader.reset(dst + 16, bytesWritten - 16);
                long prevTs = second;
                long prevDelta = second - first;
                for (int i = 2; i < timestamps.length; i++) {
                    long dod = decodeDoD(reader);
                    long delta = prevDelta + dod;
                    long ts = prevTs + delta;
                    Assert.assertEquals("Mismatch at index " + i, timestamps[i], ts);
                    prevDelta = delta;
                    prevTs = ts;
                }
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, capacity, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeTimestampsRoundTripLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            QwpBitReader reader = new QwpBitReader();

            int count = 10_000;
            long[] timestamps = new long[count];
            timestamps[0] = 1_000_000_000L;
            timestamps[1] = 1_000_001_000L;

            java.util.Random random = new java.util.Random(42);
            for (int i = 2; i < count; i++) {
                long prevDelta = timestamps[i - 1] - timestamps[i - 2];
                int variation = (i % 10 == 0) ? random.nextInt(100) - 50 : 0;
                timestamps[i] = timestamps[i - 1] + prevDelta + variation;
            }

            long src = putTimestamps(timestamps);
            int capacity = QwpGorillaEncoder.calculateEncodedSize(src, count) + 100;
            long dst = Unsafe.malloc(capacity, MemoryTag.NATIVE_ILP_RSS);
            try {
                int bytesWritten = encoder.encodeTimestamps(dst, capacity, src, count);
                Assert.assertTrue(bytesWritten > 0);
                Assert.assertTrue("Should compress better than uncompressed", bytesWritten < count * 8);

                long first = Unsafe.getLong(dst);
                long second = Unsafe.getLong(dst + 8);
                Assert.assertEquals(timestamps[0], first);
                Assert.assertEquals(timestamps[1], second);

                reader.reset(dst + 16, bytesWritten - 16);
                long prevTs = second;
                long prevDelta = second - first;
                for (int i = 2; i < count; i++) {
                    long dod = decodeDoD(reader);
                    long delta = prevDelta + dod;
                    long ts = prevTs + delta;
                    Assert.assertEquals("Mismatch at index " + i, timestamps[i], ts);
                    prevDelta = delta;
                    prevTs = ts;
                }
            } finally {
                Unsafe.free(src, (long) count * 8, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, capacity, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeTimestampsRoundTripNegativeDoD() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            QwpBitReader reader = new QwpBitReader();

            long[] timestamps = {
                    1_000_000_000L,
                    1_000_002_000L,  // delta=2000
                    1_000_003_000L,  // DoD=-1000 (bucket 3)
                    1_000_003_500L,  // DoD=-500 (bucket 2)
                    1_000_003_600L,  // DoD=-400 (bucket 2)
            };

            long src = putTimestamps(timestamps);
            int capacity = QwpGorillaEncoder.calculateEncodedSize(src, timestamps.length) + 16;
            long dst = Unsafe.malloc(capacity, MemoryTag.NATIVE_ILP_RSS);
            try {
                int bytesWritten = encoder.encodeTimestamps(dst, capacity, src, timestamps.length);
                Assert.assertTrue(bytesWritten > 0);

                long first = Unsafe.getLong(dst);
                long second = Unsafe.getLong(dst + 8);
                Assert.assertEquals(timestamps[0], first);
                Assert.assertEquals(timestamps[1], second);

                reader.reset(dst + 16, bytesWritten - 16);
                long prevTs = second;
                long prevDelta = second - first;
                for (int i = 2; i < timestamps.length; i++) {
                    long dod = decodeDoD(reader);
                    long delta = prevDelta + dod;
                    long ts = prevTs + delta;
                    Assert.assertEquals("Mismatch at index " + i, timestamps[i], ts);
                    prevDelta = delta;
                    prevTs = ts;
                }
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, capacity, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeTimestampsRoundTripVaryingDelta() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            QwpBitReader reader = new QwpBitReader();

            long[] timestamps = {
                    1_000_000_000L,
                    1_000_001_000L,  // delta=1000
                    1_000_002_000L,  // DoD=0 (bucket 0)
                    1_000_003_010L,  // DoD=10 (bucket 1)
                    1_000_004_120L,  // DoD=100 (bucket 1)
                    1_000_005_420L,  // DoD=190 (bucket 2)
                    1_000_007_720L,  // DoD=1000 (bucket 3)
                    1_000_020_020L,  // DoD=10000 (bucket 4)
            };

            long src = putTimestamps(timestamps);
            int capacity = QwpGorillaEncoder.calculateEncodedSize(src, timestamps.length) + 16;
            long dst = Unsafe.malloc(capacity, MemoryTag.NATIVE_ILP_RSS);
            try {
                int bytesWritten = encoder.encodeTimestamps(dst, capacity, src, timestamps.length);
                Assert.assertTrue(bytesWritten > 0);

                long first = Unsafe.getLong(dst);
                long second = Unsafe.getLong(dst + 8);
                Assert.assertEquals(timestamps[0], first);
                Assert.assertEquals(timestamps[1], second);

                reader.reset(dst + 16, bytesWritten - 16);
                long prevTs = second;
                long prevDelta = second - first;
                for (int i = 2; i < timestamps.length; i++) {
                    long dod = decodeDoD(reader);
                    long delta = prevDelta + dod;
                    long ts = prevTs + delta;
                    Assert.assertEquals("Mismatch at index " + i, timestamps[i], ts);
                    prevDelta = delta;
                    prevTs = ts;
                }
            } finally {
                Unsafe.free(src, (long) timestamps.length * 8, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, capacity, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeTimestampsTwoTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            long[] timestamps = {1_000_000_000L, 1_000_001_000L};
            long src = putTimestamps(timestamps);
            long dst = Unsafe.malloc(64, MemoryTag.NATIVE_ILP_RSS);
            try {
                int bytesWritten = encoder.encodeTimestamps(dst, 64, src, 2);
                Assert.assertEquals(16, bytesWritten);
                Assert.assertEquals(1_000_000_000L, Unsafe.getLong(dst));
                Assert.assertEquals(1_000_001_000L, Unsafe.getLong(dst + 8));
            } finally {
                Unsafe.free(src, 16, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, 64, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testGetBitsRequired() throws Exception {
        assertMemoryLeak(() -> {
            // Bucket 0: 1 bit
            Assert.assertEquals(1, QwpGorillaEncoder.getBitsRequired(0));

            // Bucket 1: 9 bits (2 prefix + 7 value)
            Assert.assertEquals(9, QwpGorillaEncoder.getBitsRequired(1));
            Assert.assertEquals(9, QwpGorillaEncoder.getBitsRequired(-1));
            Assert.assertEquals(9, QwpGorillaEncoder.getBitsRequired(63));
            Assert.assertEquals(9, QwpGorillaEncoder.getBitsRequired(-64));

            // Bucket 2: 12 bits (3 prefix + 9 value)
            Assert.assertEquals(12, QwpGorillaEncoder.getBitsRequired(64));
            Assert.assertEquals(12, QwpGorillaEncoder.getBitsRequired(255));
            Assert.assertEquals(12, QwpGorillaEncoder.getBitsRequired(-256));

            // Bucket 3: 16 bits (4 prefix + 12 value)
            Assert.assertEquals(16, QwpGorillaEncoder.getBitsRequired(256));
            Assert.assertEquals(16, QwpGorillaEncoder.getBitsRequired(2047));
            Assert.assertEquals(16, QwpGorillaEncoder.getBitsRequired(-2048));

            // Bucket 4: 36 bits (4 prefix + 32 value)
            Assert.assertEquals(36, QwpGorillaEncoder.getBitsRequired(2048));
            Assert.assertEquals(36, QwpGorillaEncoder.getBitsRequired(-2049));
        });
    }

    @Test
    public void testGetBucket12Bit() throws Exception {
        assertMemoryLeak(() -> {
            // DoD in [-2048, 2047] but outside [-256, 255] -> bucket 3 (16 bits)
            Assert.assertEquals(3, QwpGorillaEncoder.getBucket(256));
            Assert.assertEquals(3, QwpGorillaEncoder.getBucket(-257));
            Assert.assertEquals(3, QwpGorillaEncoder.getBucket(2047));
            Assert.assertEquals(3, QwpGorillaEncoder.getBucket(-2047));
            Assert.assertEquals(3, QwpGorillaEncoder.getBucket(-2048));
        });
    }

    @Test
    public void testGetBucket32Bit() throws Exception {
        assertMemoryLeak(() -> {
            // DoD outside [-2048, 2047] -> bucket 4 (36 bits)
            Assert.assertEquals(4, QwpGorillaEncoder.getBucket(2048));
            Assert.assertEquals(4, QwpGorillaEncoder.getBucket(-2049));
            Assert.assertEquals(4, QwpGorillaEncoder.getBucket(100_000));
            Assert.assertEquals(4, QwpGorillaEncoder.getBucket(-100_000));
            Assert.assertEquals(4, QwpGorillaEncoder.getBucket(Integer.MAX_VALUE));
            Assert.assertEquals(4, QwpGorillaEncoder.getBucket(Integer.MIN_VALUE));
        });
    }

    @Test
    public void testGetBucket7Bit() throws Exception {
        assertMemoryLeak(() -> {
            // DoD in [-64, 63] -> bucket 1 (9 bits)
            Assert.assertEquals(1, QwpGorillaEncoder.getBucket(1));
            Assert.assertEquals(1, QwpGorillaEncoder.getBucket(-1));
            Assert.assertEquals(1, QwpGorillaEncoder.getBucket(63));
            Assert.assertEquals(1, QwpGorillaEncoder.getBucket(-63));
            Assert.assertEquals(1, QwpGorillaEncoder.getBucket(-64));
        });
    }

    @Test
    public void testGetBucket9Bit() throws Exception {
        assertMemoryLeak(() -> {
            // DoD in [-256, 255] but outside [-64, 63] -> bucket 2 (12 bits)
            Assert.assertEquals(2, QwpGorillaEncoder.getBucket(64));
            Assert.assertEquals(2, QwpGorillaEncoder.getBucket(-65));
            Assert.assertEquals(2, QwpGorillaEncoder.getBucket(255));
            Assert.assertEquals(2, QwpGorillaEncoder.getBucket(-255));
            Assert.assertEquals(2, QwpGorillaEncoder.getBucket(-256));
        });
    }

    @Test
    public void testGetBucketZero() throws Exception {
        assertMemoryLeak(() -> Assert.assertEquals(0, QwpGorillaEncoder.getBucket(0)));
    }

    /**
     * Decodes a delta-of-delta value from the bit stream, mirroring the
     * core QwpGorillaDecoder.decodeDoD() logic.
     */
    private static long decodeDoD(QwpBitReader reader) throws QwpParseException {
        int bit = reader.readBit();
        if (bit == 0) {
            return 0;
        }
        bit = reader.readBit();
        if (bit == 0) {
            return reader.readSigned(7);
        }
        bit = reader.readBit();
        if (bit == 0) {
            return reader.readSigned(9);
        }
        bit = reader.readBit();
        if (bit == 0) {
            return reader.readSigned(12);
        }
        return reader.readSigned(32);
    }

    /**
     * Writes a Java array of timestamps to off-heap memory.
     *
     * @param timestamps the timestamps to write
     * @return the address of the allocated memory (caller must free)
     */
    private static long putTimestamps(long[] timestamps) {
        long size = (long) timestamps.length * 8;
        long address = Unsafe.malloc(size, MemoryTag.NATIVE_ILP_RSS);
        for (int i = 0; i < timestamps.length; i++) {
            Unsafe.putLong(address + (long) i * 8, timestamps[i]);
        }
        return address;
    }
}

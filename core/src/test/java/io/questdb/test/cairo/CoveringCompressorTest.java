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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.CoveringCompressor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class CoveringCompressorTest extends AbstractCairoTest {

    @Test
    public void testAllExceptionsBlock() throws Exception {
        // When ALL values are ALP exceptions (irrational numbers), fillValue=0,
        // bw=0, and all values are stored in the exception list
        assertMemoryLeak(() -> {
            double[] input = {Math.PI, Math.E, Math.sqrt(2), Math.sqrt(3), Math.log(2),
                    Math.log(10), Math.sin(1), Math.cos(1)};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                CoveringCompressor.compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                CoveringCompressor.decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("value " + i + " (" + input[i] + ")",
                            Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAllIdenticalValues() throws Exception {
        assertMemoryLeak(() -> {
            int count = 64;
            double[] input = new double[count];
            Arrays.fill(input, 42.5);
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                int compressedSize = CoveringCompressor.compressDoubles(srcAddr, count, 3, destAddr);
                Assert.assertEquals(CoveringCompressor.DOUBLE_HEADER_SIZE, compressedSize);
                double[] output = new double[count];
                CoveringCompressor.decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals(42.5, output[i], 0.0);
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAlpEncodeDecode() throws Exception {
        assertMemoryLeak(() -> {
            double[] values = {10.5, 20.5, 11.5, 30.5, 21.5, 12.5};
            int params = findParams(values);
            int e = params >>> 16;
            int f = params & 0xFFFF;

            for (double val : values) {
                long enc = CoveringCompressor.alpEncode(val, e, f);
                double dec = CoveringCompressor.alpDecode(enc, e, f);
                Assert.assertEquals(Double.doubleToRawLongBits(val), Double.doubleToRawLongBits(dec));
            }
        });
    }

    @Test
    public void testAlpTightPriceRange() throws Exception {
        assertMemoryLeak(() -> {
            double[] values = new double[256];
            for (int i = 0; i < 256; i++) {
                values[i] = 99.00 + i * 0.01;
            }
            int params = findParams(values);
            int e = params >>> 16;
            int f = params & 0xFFFF;

            Assert.assertTrue("exponent should be > 0", e > 0);
            for (double val : values) {
                long enc = CoveringCompressor.alpEncode(val, e, f);
                double dec = CoveringCompressor.alpDecode(enc, e, f);
                Assert.assertEquals(Double.doubleToRawLongBits(val), Double.doubleToRawLongBits(dec));
            }
        });
    }

    @Test
    public void testCompressDecompressDoubles() throws Exception {
        assertMemoryLeak(() -> {
            double[] input = {10.5, 20.5, 11.5, 30.5, 21.5, 12.5, 15.0, 25.0};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                int compressedSize = CoveringCompressor.compressDoubles(srcAddr, count, 3, destAddr);
                Assert.assertTrue("compressed should be smaller", compressedSize < count * Double.BYTES);
                double[] output = new double[count];
                CoveringCompressor.decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals(Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressDecompressFloatAsInt() throws Exception {
        // FLOAT is compressed via compressInts (raw int bits) and reconstructed with Float.intBitsToFloat
        assertMemoryLeak(() -> {
            float[] input = {1.5f, Float.NaN, -0.0f, Float.MAX_VALUE, Float.MIN_VALUE, Float.MIN_NORMAL,
                    Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.0f, -42.75f};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.INT), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putFloat(srcAddr + (long) i * Float.BYTES, input[i]);
                }
                CoveringCompressor.compressInts(srcAddr, count, destAddr);
                int[] output = new int[count];
                CoveringCompressor.decompressInts(destAddr, output);
                for (int i = 0; i < count; i++) {
                    float recovered = Float.intBitsToFloat(output[i]);
                    Assert.assertEquals("float " + i + " (" + input[i] + ")",
                            Float.floatToRawIntBits(input[i]), Float.floatToRawIntBits(recovered));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.INT), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressDecompressInts() throws Exception {
        assertMemoryLeak(() -> {
            int[] input = {100, 200, 150, 300, 250, 120, 180, 280};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.INT), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putInt(srcAddr + (long) i * Integer.BYTES, input[i]);
                }
                int compressedSize = CoveringCompressor.compressInts(srcAddr, count, destAddr);
                Assert.assertTrue("compressed should be smaller", compressedSize < count * Integer.BYTES);
                int[] output = new int[count];
                CoveringCompressor.decompressInts(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(srcAddr, (long) count * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.INT), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressDecompressLongs() throws Exception {
        assertMemoryLeak(() -> {
            long[] input = {1000L, 1005L, 1002L, 1008L, 1001L, 1003L, 1007L, 1004L};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                int compressedSize = CoveringCompressor.compressLongs(srcAddr, count, destAddr);
                Assert.assertTrue("compressed should be smaller", compressedSize < count * Long.BYTES);
                long[] output = new long[count];
                CoveringCompressor.decompressLongs(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressionRatio() throws Exception {
        assertMemoryLeak(() -> {
            int count = 256;
            double[] input = new double[count];
            Random rng = new Random(123);
            for (int i = 0; i < count; i++) {
                input[i] = 10.0 + rng.nextInt(2000) * 0.01;
            }
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                int compressedSize = CoveringCompressor.compressDoubles(srcAddr, count, 3, destAddr);
                double ratio = (double) (count * Double.BYTES) / compressedSize;
                Assert.assertTrue("prices should compress at least 2x, got " + ratio + "x", ratio >= 2.0);
                double[] output = new double[count];
                CoveringCompressor.decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals(Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaCompressDecompressNanoTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            // Sorted nanosecond timestamps — ~100ms intervals with jitter
            int count = 256;
            long[] input = new long[count];
            long baseTs = 1_711_900_800_000_000_000L; // 2024-04-01 in nanos
            Random rng = new Random(99);
            for (int i = 0; i < count; i++) {
                input[i] = baseTs + (long) i * 100_000_000L + rng.nextInt(10_000_000);
            }
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                int deltaSize = CoveringCompressor.compressLongsDelta(srcAddr, count, destAddr, workAddr);
                long[] output = new long[count];
                CoveringCompressor.decompressLongs(destAddr, output);
                Assert.assertArrayEquals(input, output);

                // Compare with plain FoR
                int plainSize = CoveringCompressor.compressLongs(srcAddr, count, destAddr);
                Assert.assertTrue("delta (" + deltaSize + ") should be much smaller than plain (" + plainSize + ")",
                        deltaSize < plainSize);
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(workAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaCompressDecompressSortedTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            // Sorted microsecond timestamps — 1 second intervals over 4 minutes
            int count = 256;
            long[] input = new long[count];
            long baseTs = 1_711_900_800_000_000L; // 2024-04-01 in micros
            for (int i = 0; i < count; i++) {
                input[i] = baseTs + (long) i * 1_000_000L;
            }
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                int compressedSize = CoveringCompressor.compressLongsDelta(srcAddr, count, destAddr, workAddr);
                // Constant deltas → bitWidth=0, only header bytes (21 = count(4)+bw(1)+deltaBase(8)+firstValue(8))
                Assert.assertEquals(21, compressedSize);
                long[] output = new long[count];
                CoveringCompressor.decompressLongs(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(workAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaCompressDecompressToAddr() throws Exception {
        assertMemoryLeak(() -> {
            int count = 64;
            long[] input = new long[count];
            long baseTs = 1_711_900_800_000_000L;
            Random rng = new Random(7);
            for (int i = 0; i < count; i++) {
                input[i] = baseTs + (long) i * 1_000_000L + rng.nextInt(500_000);
            }
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long outAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                CoveringCompressor.compressLongsDelta(srcAddr, count, destAddr, workAddr);
                int decoded = CoveringCompressor.decompressLongsToAddr(destAddr, outAddr, workAddr);
                Assert.assertEquals(count, decoded);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("value " + i, input[i],
                            Unsafe.getUnsafe().getLong(outAddr + (long) i * Long.BYTES));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(workAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(outAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaCompressEmptyInput() throws Exception {
        // count=0 falls back to compressLongs which handles it
        assertMemoryLeak(() -> {
            long srcAddr = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(1, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                // count=0 via compressLongs: header only, bitWidth=0
                long[] output = new long[0];
                CoveringCompressor.decompressLongs(destAddr, output);
                Assert.assertEquals(0, output.length);
            } finally {
                Unsafe.free(srcAddr, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(1, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(workAddr, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaCompressExtremeValues() throws Exception {
        // Test near Long.MAX_VALUE/MIN_VALUE — verify delta overflow is handled
        assertMemoryLeak(() -> {
            long[] input = {Long.MIN_VALUE + 1, 0, Long.MAX_VALUE - 1, Long.MAX_VALUE};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                CoveringCompressor.compressLongsDelta(srcAddr, count, destAddr, workAddr);
                long[] output = new long[count];
                CoveringCompressor.decompressLongs(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(workAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaCompressNegativeDeltas() throws Exception {
        // Non-monotonic data (e.g., O3 reorder) produces negative deltas
        assertMemoryLeak(() -> {
            long[] input = {100, 95, 110, 90, 120, 85, 130, 80};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                CoveringCompressor.compressLongsDelta(srcAddr, count, destAddr, workAddr);
                long[] output = new long[count];
                CoveringCompressor.decompressLongs(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(workAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaCompressSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            // count <= 1 falls back to plain FoR
            long[] input = {1_711_900_800_000_000L};
            long srcAddr = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(1, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putLong(srcAddr, input[0]);
                CoveringCompressor.compressLongsDelta(srcAddr, 1, destAddr, workAddr);
                long[] output = new long[1];
                CoveringCompressor.decompressLongs(destAddr, output);
                Assert.assertEquals(input[0], output[0]);
            } finally {
                Unsafe.free(srcAddr, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(1, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(workAddr, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaCompressTwoValues() throws Exception {
        // count=2: smallest case that produces a delta (deltaCount=1)
        assertMemoryLeak(() -> {
            long[] input = {1_000_000L, 2_000_000L};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                CoveringCompressor.compressLongsDelta(srcAddr, count, destAddr, workAddr);
                long[] output = new long[count];
                CoveringCompressor.decompressLongs(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP), MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(workAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDeltaCompressionRatioBenchmark() throws Exception {
        // Benchmark compression ratios across different timestamp scenarios
        assertMemoryLeak(() -> {
            int count = 256;
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            int maxBuf = CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP);
            long destAddr = Unsafe.malloc(maxBuf, MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                int rawSize = count * Long.BYTES;
                StringBuilder report = new StringBuilder("\n=== Delta FoR Compression Ratios (256 values) ===\n");
                report.append(String.format("%-45s %8s %8s %8s %8s\n",
                        "Scenario", "Raw", "Plain", "Delta", "Ratio"));
                report.append("-".repeat(85)).append('\n');

                // Scenario 1: Microsecond timestamps, 1-second intervals (constant delta)
                long base = 1_711_900_800_000_000L;
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, base + (long) i * 1_000_000L);
                }
                appendRatioLine(report, "Micros, 1s intervals (constant delta)", rawSize, srcAddr, count, destAddr, workAddr);

                // Scenario 2: Microsecond timestamps, 1-second intervals with jitter
                Random rng = new Random(42);
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES,
                            base + (long) i * 1_000_000L + rng.nextInt(500_000));
                }
                appendRatioLine(report, "Micros, 1s intervals + 500ms jitter", rawSize, srcAddr, count, destAddr, workAddr);

                // Scenario 3: Nanosecond timestamps, 1-second intervals (constant delta)
                long nanoBase = 1_711_900_800_000_000_000L;
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES,
                            nanoBase + (long) i * 1_000_000_000L);
                }
                appendRatioLine(report, "Nanos, 1s intervals (constant delta)", rawSize, srcAddr, count, destAddr, workAddr);

                // Scenario 4: Nanosecond timestamps, 100ms intervals with 10ms jitter
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES,
                            nanoBase + (long) i * 100_000_000L + rng.nextInt(10_000_000));
                }
                appendRatioLine(report, "Nanos, 100ms intervals + 10ms jitter", rawSize, srcAddr, count, destAddr, workAddr);

                // Scenario 5: Nanosecond timestamps, 1-hour span, irregular intervals
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES,
                            nanoBase + (long) (rng.nextDouble() * 3_600_000_000_000L));
                }
                // Sort for realistic scenario
                long[] tmp = new long[count];
                for (int i = 0; i < count; i++) {
                    tmp[i] = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
                }
                java.util.Arrays.sort(tmp);
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, tmp[i]);
                }
                appendRatioLine(report, "Nanos, 1hr span, sorted random", rawSize, srcAddr, count, destAddr, workAddr);

                // Scenario 6: Microsecond timestamps, 1-hour span, sorted random
                for (int i = 0; i < count; i++) {
                    tmp[i] = base + (long) (rng.nextDouble() * 3_600_000_000L);
                }
                java.util.Arrays.sort(tmp);
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, tmp[i]);
                }
                appendRatioLine(report, "Micros, 1hr span, sorted random", rawSize, srcAddr, count, destAddr, workAddr);

                // Scenario 7: Nanosecond timestamps, 1ms intervals (high-frequency)
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES,
                            nanoBase + (long) i * 1_000_000L + rng.nextInt(100_000));
                }
                appendRatioLine(report, "Nanos, 1ms intervals + 100us jitter", rawSize, srcAddr, count, destAddr, workAddr);

                LOG.info().$(report).$();

                // Verify delta always compresses better than plain for sorted timestamps
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES,
                            nanoBase + (long) i * 100_000_000L);
                }
                int plainSize = CoveringCompressor.compressLongs(srcAddr, count, destAddr);
                int deltaSize = CoveringCompressor.compressLongsDelta(srcAddr, count, destAddr, workAddr);
                Assert.assertTrue("delta (" + deltaSize + ") must beat plain (" + plainSize + ") for sorted nanos",
                        deltaSize < plainSize);
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, maxBuf, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(workAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNaNAndInfAreExceptions() throws Exception {
        assertMemoryLeak(() -> {
            double[] input = {10.5, Double.NaN, 11.5, Double.POSITIVE_INFINITY, 12.5,
                    Double.NEGATIVE_INFINITY, -0.0, 13.5};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                CoveringCompressor.compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                CoveringCompressor.decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals(Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRandomDoublesLossless() throws Exception {
        assertMemoryLeak(() -> {
            Random rng = new Random(42);
            int count = 256;
            double[] input = new double[count];
            for (int i = 0; i < count; i++) {
                input[i] = rng.nextDouble() * 1000.0;
            }
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                CoveringCompressor.compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                CoveringCompressor.decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals(Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSubnormalsAndExtremes() throws Exception {
        // Edge case doubles: subnormals, max/min values, zero
        assertMemoryLeak(() -> {
            double[] input = {Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE,
                    -Double.MAX_VALUE, 0.0, -0.0, 1e308, 1e-308, -1e-308};
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                CoveringCompressor.compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                CoveringCompressor.decompressDoubles(destAddr, output);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("value " + i + " (" + input[i] + ")",
                            Double.doubleToRawLongBits(input[i]), Double.doubleToRawLongBits(output[i]));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static int findParams(double[] values) {
        long addr = Unsafe.malloc((long) values.length * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < values.length; i++) {
                Unsafe.getUnsafe().putDouble(addr + (long) i * Double.BYTES, values[i]);
            }
            return CoveringCompressor.findBestAlpParams(addr, values.length, 3);
        } finally {
            Unsafe.free(addr, (long) values.length * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void appendRatioLine(StringBuilder report, String scenario, int rawSize,
                                 long srcAddr, int count, long destAddr, long workAddr) {
        int plainSize = CoveringCompressor.compressLongs(srcAddr, count, destAddr);
        int deltaSize = CoveringCompressor.compressLongsDelta(srcAddr, count, destAddr, workAddr);
        double deltaRatio = (double) rawSize / deltaSize;
        report.append(String.format("%-45s %6dB %6dB %6dB %6.1fx\n",
                scenario, rawSize, plainSize, deltaSize, deltaRatio));
    }
}

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

package io.questdb.test.cairo.covering;

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
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
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
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                int compressedSize = compressDoubles(srcAddr, count, 3, destAddr);
                Assert.assertEquals(CoveringCompressor.DOUBLE_HEADER_SIZE, compressedSize);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
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
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                int compressedSize = compressDoubles(srcAddr, count, 3, destAddr);
                Assert.assertTrue("compressed should be smaller", compressedSize < count * Double.BYTES);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
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
                    Unsafe.putFloat(srcAddr + (long) i * Float.BYTES, input[i]);
                }
                compressInts(srcAddr, count, destAddr);
                int[] output = new int[count];
                decompressInts(destAddr, output);
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
                    Unsafe.putInt(srcAddr + (long) i * Integer.BYTES, input[i]);
                }
                int compressedSize = compressInts(srcAddr, count, destAddr);
                Assert.assertTrue("compressed should be smaller", compressedSize < count * Integer.BYTES);
                int[] output = new int[count];
                decompressInts(destAddr, output);
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
                    Unsafe.putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                int compressedSize = CoveringCompressor.compressLongs(srcAddr, count, destAddr);
                Assert.assertTrue("compressed should be smaller", compressedSize < count * Long.BYTES);
                long[] output = new long[count];
                decompressLongs(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompressFloatsRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            float[] input = {
                    1.5f, 2.25f, 3.125f, -4.0625f, 0.0f, -0.0f,
                    1e-3f, 1e3f, 42.0f, 42.0f, 42.0f, Float.MIN_NORMAL
            };
            int count = input.length;
            int destCap = CoveringCompressor.maxCompressedSize(count, ColumnType.FLOAT);
            long srcAddr = Unsafe.malloc((long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            long encAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long excAddr = Unsafe.malloc(count, MemoryTag.NATIVE_DEFAULT);
            long decAddr = Unsafe.malloc((long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
            long decodeWsAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putFloat(srcAddr + (long) i * Float.BYTES, input[i]);
                }
                int sz = CoveringCompressor.compressFloats(srcAddr, count, destAddr, encAddr, excAddr);
                Assert.assertTrue("compressed size must be positive", sz > 0);

                CoveringCompressor.decompressFloatsToAddr(destAddr, decAddr, decodeWsAddr);
                for (int i = 0; i < count; i++) {
                    float actual = Unsafe.getFloat(decAddr + (long) i * Float.BYTES);
                    Assert.assertEquals("value " + i,
                            Float.floatToRawIntBits(input[i]), Float.floatToRawIntBits(actual));
                }
            } finally {
                Unsafe.free(decodeWsAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(decAddr, (long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(excAddr, count, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(encAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, destCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(srcAddr, (long) count * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
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
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                int compressedSize = compressDoubles(srcAddr, count, 3, destAddr);
                double ratio = (double) (count * Double.BYTES) / compressedSize;
                Assert.assertTrue("prices should compress at least 2x, got " + ratio + "x", ratio >= 2.0);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
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
    public void testEmptyBlockBwIsZero() throws Exception {
        assertMemoryLeak(() -> {
            int destCap = CoveringCompressor.maxCompressedSize(1, ColumnType.LONG);
            long destAddr = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            try {
                int sz = CoveringCompressor.compressLongs(0L, 0, destAddr);
                Assert.assertTrue("header must fit", sz > 0);
                Assert.assertEquals(0, Unsafe.getInt(destAddr));
                Assert.assertEquals(0, Unsafe.getByte(destAddr + 4));
            } finally {
                Unsafe.free(destAddr, destCap, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testLinearPredFallbackOnBwOverflow() throws Exception {
        assertMemoryLeak(() -> {
            long[] input = {0L, Long.MIN_VALUE, Long.MAX_VALUE, 0L};
            int count = input.length;
            int destCap = CoveringCompressor.maxCompressedSize(count, ColumnType.TIMESTAMP);
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            long workAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                int sz = CoveringCompressor.compressLongsLinearPred(srcAddr, count, destAddr, workAddr);
                Assert.assertTrue("compressed size must be positive", sz > 0);

                int flagByte = Unsafe.getByte(destAddr + 4) & 0xFF;
                Assert.assertNotEquals("expected plain FoR after fallback, got linear-pred",
                        0xC0, flagByte & 0xC0);

                long[] output = new long[count];
                decompressLongs(destAddr, output);
                Assert.assertArrayEquals(input, output);
            } finally {
                Unsafe.free(workAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, destCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaxCompressedSizeRejectsUnsupportedTypes() {
        int[] unsupported = {ColumnType.VARCHAR, ColumnType.STRING, ColumnType.BINARY};
        for (int type : unsupported) {
            Assert.assertThrows("column type " + type,
                    AssertionError.class,
                    () -> CoveringCompressor.maxCompressedSize(100, type));
        }
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
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
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
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
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
    public void testReadLongAtBitWidth63HighBitsRoundTrip() throws Exception {
        // BitpackUtils.unpackValue uses a per-byte OR loop. When bitShift + bitWidth > 64,
        // the loop iterates 9 times, and the 9th iteration shifts a byte by 64 (Java treats
        // as shift-by-0), OR'ing the value's high bits into the low byte where they get
        // discarded by the subsequent right-shift. For bw=63 this drops bits 57..62 of the
        // offset for any index where (index * 63) % 8 >= 2, i.e. indices {1, 2, 3, 4, 5, 6}
        // out of every 8.
        assertMemoryLeak(() -> {
            // forBase=0, forMax=(1<<62)|3 -> span requires 63 bits.
            long[] input = {
                    0L,
                    1L << 62,
                    1L,
                    (1L << 62) | 1L,
                    2L,
                    (1L << 62) | 2L,
                    3L,
                    (1L << 62) | 3L,
            };
            int count = input.length;
            long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc(CoveringCompressor.maxCompressedSize(count, ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.putLong(srcAddr + (long) i * Long.BYTES, input[i]);
                }
                CoveringCompressor.compressLongs(srcAddr, count, destAddr);
                // Assert the compressor really picked bw=63, otherwise the test premise is invalid.
                int bw = Unsafe.getByte(destAddr + 4) & 0xFF;
                Assert.assertEquals("expected bw=63", 63, bw);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("readLongAt at index " + i, input[i], CoveringCompressor.readLongAt(destAddr, i));
                }
            } finally {
                Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, CoveringCompressor.maxCompressedSize(count, ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
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
                    Unsafe.putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
                }
                compressDoubles(srcAddr, count, 3, destAddr);
                double[] output = new double[count];
                decompressDoubles(destAddr, output);
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

    private static int compressDoubles(long srcAddr, int count, int valueShift, long destAddr) {
        long encAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long excAddr = Unsafe.malloc(count, MemoryTag.NATIVE_DEFAULT);
        try {
            return CoveringCompressor.compressDoubles(srcAddr, count, valueShift, destAddr, encAddr, excAddr);
        } finally {
            Unsafe.free(excAddr, count, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(encAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static int compressInts(long srcAddr, int count, long destAddr) {
        long wsAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            return CoveringCompressor.compressInts(srcAddr, count, destAddr, wsAddr);
        } finally {
            Unsafe.free(wsAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void decompressDoubles(long srcAddr, double[] output) {
        int count = output.length;
        long outAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long wsAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            CoveringCompressor.decompressDoublesToAddr(srcAddr, outAddr, wsAddr);
            for (int i = 0; i < count; i++) {
                output[i] = Unsafe.getDouble(outAddr + (long) i * Double.BYTES);
            }
        } finally {
            Unsafe.free(wsAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void decompressInts(long srcAddr, int[] output) {
        int count = output.length;
        long outAddr = Unsafe.malloc((long) count * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        long wsAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            CoveringCompressor.decompressIntsToAddr(srcAddr, outAddr, wsAddr);
            for (int i = 0; i < count; i++) {
                output[i] = Unsafe.getInt(outAddr + (long) i * Integer.BYTES);
            }
        } finally {
            Unsafe.free(wsAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outAddr, (long) count * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void decompressLongs(long srcAddr, long[] output) {
        int count = output.length;
        long outAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long wsAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            CoveringCompressor.decompressLongsToAddr(srcAddr, outAddr, wsAddr);
            for (int i = 0; i < count; i++) {
                output[i] = Unsafe.getLong(outAddr + (long) i * Long.BYTES);
            }
        } finally {
            Unsafe.free(wsAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static int findParams(double[] values) {
        long addr = Unsafe.malloc((long) values.length * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < values.length; i++) {
                Unsafe.putDouble(addr + (long) i * Double.BYTES, values[i]);
            }
            return CoveringCompressor.findBestAlpParams(addr, values.length, 3);
        } finally {
            Unsafe.free(addr, (long) values.length * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }
}

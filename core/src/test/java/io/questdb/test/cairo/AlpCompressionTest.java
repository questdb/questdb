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

import io.questdb.cairo.idx.AlpCompression;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class AlpCompressionTest {

    @Test
    public void testAlpEncodeDecode() {
        // Prices with 1 decimal place: e=1, f=0 should work
        double[] values = {10.5, 20.5, 11.5, 30.5, 21.5, 12.5};
        int params = findParams(values);
        int e = params >>> 16;
        int f = params & 0xFFFF;

        for (double val : values) {
            long enc = AlpCompression.alpEncode(val, e, f);
            double dec = AlpCompression.alpDecode(enc, e, f);
            Assert.assertEquals(
                    "lossless round-trip for " + val,
                    Double.doubleToRawLongBits(val),
                    Double.doubleToRawLongBits(dec)
            );
        }
    }

    @Test
    public void testAlpTightPriceRange() {
        // Prices with 2 decimal places: should compress very well
        double[] values = new double[256];
        for (int i = 0; i < 256; i++) {
            values[i] = 99.00 + i * 0.01;
        }
        int params = findParams(values);
        int e = params >>> 16;

        // Should find exponent that makes these integers
        Assert.assertTrue("exponent should be > 0", e > 0);

        // Verify lossless round-trip
        for (double val : values) {
            long enc = AlpCompression.alpEncode(val, e, params & 0xFFFF);
            double dec = AlpCompression.alpDecode(enc, e, params & 0xFFFF);
            Assert.assertEquals(
                    Double.doubleToRawLongBits(val),
                    Double.doubleToRawLongBits(dec)
            );
        }
    }

    @Test
    public void testCompressDecompressDoubles() {
        double[] input = {10.5, 20.5, 11.5, 30.5, 21.5, 12.5, 15.0, 25.0};
        int count = input.length;

        long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long destAddr = Unsafe.malloc(AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
            }

            int compressedSize = AlpCompression.compressDoubles(srcAddr, count, 3, destAddr);
            Assert.assertTrue("compressed should be smaller", compressedSize < count * Double.BYTES);

            double[] output = new double[count];
            AlpCompression.decompressDoubles(destAddr, output);

            for (int i = 0; i < count; i++) {
                Assert.assertEquals(
                        "value " + i + " lossless",
                        Double.doubleToRawLongBits(input[i]),
                        Double.doubleToRawLongBits(output[i])
                );
            }
        } finally {
            Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr, AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCompressDecompressLongs() {
        long[] input = {1000L, 1005L, 1002L, 1008L, 1001L, 1003L, 1007L, 1004L};
        int count = input.length;

        long srcAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long destAddr = Unsafe.malloc(AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, input[i]);
            }

            int compressedSize = AlpCompression.compressLongs(srcAddr, count, destAddr);
            Assert.assertTrue("compressed should be smaller", compressedSize < count * Long.BYTES);

            long[] output = new long[count];
            AlpCompression.decompressLongs(destAddr, output);

            Assert.assertArrayEquals(input, output);
        } finally {
            Unsafe.free(srcAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr, AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.LONG), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCompressDecompressInts() {
        int[] input = {100, 200, 150, 300, 250, 120, 180, 280};
        int count = input.length;

        long srcAddr = Unsafe.malloc((long) count * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        long destAddr = Unsafe.malloc(AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.INT), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putInt(srcAddr + (long) i * Integer.BYTES, input[i]);
            }

            int compressedSize = AlpCompression.compressInts(srcAddr, count, destAddr);
            Assert.assertTrue("compressed should be smaller", compressedSize < count * Integer.BYTES);

            int[] output = new int[count];
            AlpCompression.decompressInts(destAddr, output);

            Assert.assertArrayEquals(input, output);
        } finally {
            Unsafe.free(srcAddr, (long) count * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr, AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.INT), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testNaNAndInfAreExceptions() {
        double[] input = {10.5, Double.NaN, 11.5, Double.POSITIVE_INFINITY, 12.5, Double.NEGATIVE_INFINITY, -0.0, 13.5};
        int count = input.length;

        long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long destAddr = Unsafe.malloc(AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
            }

            AlpCompression.compressDoubles(srcAddr, count, 3, destAddr);

            double[] output = new double[count];
            AlpCompression.decompressDoubles(destAddr, output);

            for (int i = 0; i < count; i++) {
                Assert.assertEquals(
                        "value " + i + " (" + input[i] + ") lossless",
                        Double.doubleToRawLongBits(input[i]),
                        Double.doubleToRawLongBits(output[i])
                );
            }
        } finally {
            Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr, AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRandomDoublesLossless() {
        Random rng = new Random(42);
        int count = 256;
        double[] input = new double[count];
        for (int i = 0; i < count; i++) {
            input[i] = rng.nextDouble() * 1000.0;
        }

        long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long destAddr = Unsafe.malloc(AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
            }

            AlpCompression.compressDoubles(srcAddr, count, 3, destAddr);

            double[] output = new double[count];
            AlpCompression.decompressDoubles(destAddr, output);

            for (int i = 0; i < count; i++) {
                Assert.assertEquals(
                        "value " + i + " lossless",
                        Double.doubleToRawLongBits(input[i]),
                        Double.doubleToRawLongBits(output[i])
                );
            }
        } finally {
            Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr, AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testAllIdenticalValues() {
        int count = 64;
        double[] input = new double[count];
        for (int i = 0; i < count; i++) {
            input[i] = 42.5;
        }

        long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long destAddr = Unsafe.malloc(AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
            }

            int compressedSize = AlpCompression.compressDoubles(srcAddr, count, 3, destAddr);
            // bitWidth=0: only header, no packed data
            Assert.assertEquals(AlpCompression.DOUBLE_HEADER_SIZE, compressedSize);

            double[] output = new double[count];
            AlpCompression.decompressDoubles(destAddr, output);

            for (int i = 0; i < count; i++) {
                Assert.assertEquals(42.5, output[i], 0.0);
            }
        } finally {
            Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr, AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCompressionRatio() {
        // Prices 10.00-30.00 with 2 decimal places: should get ~8x compression
        int count = 256;
        double[] input = new double[count];
        Random rng = new Random(123);
        for (int i = 0; i < count; i++) {
            input[i] = 10.0 + rng.nextInt(2000) * 0.01; // 10.00 to 29.99
        }

        long srcAddr = Unsafe.malloc((long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long destAddr = Unsafe.malloc(AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putDouble(srcAddr + (long) i * Double.BYTES, input[i]);
            }

            int compressedSize = AlpCompression.compressDoubles(srcAddr, count, 3, destAddr);
            double ratio = (double) (count * Double.BYTES) / compressedSize;
            System.out.printf("ALP compression: %d -> %d bytes (%.1fx)%n",
                    count * Double.BYTES, compressedSize, ratio);
            Assert.assertTrue("prices should compress at least 2x, got " + ratio + "x", ratio >= 2.0);

            // Verify lossless
            double[] output = new double[count];
            AlpCompression.decompressDoubles(destAddr, output);
            for (int i = 0; i < count; i++) {
                Assert.assertEquals(
                        Double.doubleToRawLongBits(input[i]),
                        Double.doubleToRawLongBits(output[i])
                );
            }
        } finally {
            Unsafe.free(srcAddr, (long) count * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(destAddr, AlpCompression.maxCompressedSize(count, io.questdb.cairo.ColumnType.DOUBLE), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static int findParams(double[] values) {
        long addr = Unsafe.malloc((long) values.length * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < values.length; i++) {
                Unsafe.getUnsafe().putDouble(addr + (long) i * Double.BYTES, values[i]);
            }
            return AlpCompression.findBestAlpParams(addr, values.length, 3);
        } finally {
            Unsafe.free(addr, (long) values.length * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }
}

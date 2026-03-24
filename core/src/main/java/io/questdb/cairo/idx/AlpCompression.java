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

package io.questdb.cairo.idx;

import io.questdb.cairo.ColumnType;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

/**
 * ALP (Adaptive Lossless floating-Point) compression for covering index
 * sidecar data. Encodes doubles/floats by finding a decimal exponent that
 * converts values to integers, then bitpacks those integers with FoR.
 * <p>
 * For integer column types (BYTE, SHORT, INT, LONG), uses plain FoR bitpacking
 * without ALP transformation.
 * <p>
 * Based on "ALP: Adaptive Lossless floating-Point Compression" (Afroozeh &
 * Boncz, SIGMOD 2024).
 */
public class AlpCompression {

    static final int MAX_EXPONENT = 18;
    // 2^52 + 2^51: forces rounding in IEEE 754 double arithmetic
    private static final double SWEET = (double) (1L << 52) + (double) (1L << 51);
    // SWEET trick works in [-2^51, 2^51]. Use a slightly conservative limit.
    private static final double ENCODING_UPPER_LIMIT = (double) (1L << 51);
    private static final double ENCODING_LOWER_LIMIT = -(double) (1L << 51);

    // Pre-computed powers of 10 as doubles (ascending)
    static final double[] F10 = {
            1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
            1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
    };

    // Pre-computed inverse powers of 10 as doubles
    static final double[] IF10 = {
            1e-0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9,
            1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18
    };

    // Compressed block header sizes (excluding packed data)
    // DOUBLE: valueCount(4) + e(1) + f(1) + bitWidth(1) + excCount(4) + forBase(8) = 19
    public static final int DOUBLE_HEADER_SIZE = 19;
    // FLOAT is compressed via compressInts (INT_HEADER_SIZE)
    // LONG: valueCount(4) + bitWidth(1) + forBase(8) = 13
    static final int LONG_HEADER_SIZE = 13;
    // INT: valueCount(4) + bitWidth(1) + forBase(4) = 9
    static final int INT_HEADER_SIZE = 9;
    // SHORT: valueCount(4) + bitWidth(1) + forBase(2) = 7
    static final int SHORT_HEADER_SIZE = 7;

    /**
     * Encode a single double value to a long integer using ALP.
     *
     * @return encoded long, or Long.MAX_VALUE if the value is not encodable
     */
    public static long alpEncode(double value, int e, int f) {
        double tmp = value * F10[e] * IF10[f];
        if (!Double.isFinite(tmp) || tmp > ENCODING_UPPER_LIMIT || tmp < ENCODING_LOWER_LIMIT) {
            return Long.MAX_VALUE;
        }
        return (long) ((tmp + SWEET) - SWEET);
    }

    /**
     * Decode a long integer back to a double using ALP.
     */
    public static double alpDecode(long encoded, int e, int f) {
        return (double) encoded * F10[f] * IF10[e];
    }

    /**
     * Find the best (e, f) exponent/factor pair for a set of double values.
     * Tries all 190 combinations and picks the one that minimizes compressed size.
     *
     * @return packed int: high 16 bits = e, low 16 bits = f
     */
    private static final int SAMPLE_SIZE = 64;

    public static int findBestAlpParams(long srcAddr, int count, int valueShift) {
        int bestE = 0, bestF = 0;
        long bestCost = Long.MAX_VALUE;

        // Sample up to SAMPLE_SIZE equidistant values for parameter search.
        // ALP parameters depend on the data's decimal scale, not individual values,
        // so a small sample is sufficient.
        int sampleStep = Math.max(1, count / SAMPLE_SIZE);
        int sampleCount = (count + sampleStep - 1) / sampleStep;

        for (int e = MAX_EXPONENT; e >= 0; e--) {
            for (int f = e; f >= 0; f--) {
                int exceptions = 0;
                long maxEnc = Long.MIN_VALUE;
                long minEnc = Long.MAX_VALUE;

                for (int i = 0; i < count; i += sampleStep) {
                    double val = readDouble(srcAddr, i, valueShift);
                    long enc = alpEncode(val, e, f);
                    if (enc == Long.MAX_VALUE) {
                        exceptions++;
                        continue;
                    }
                    double dec = alpDecode(enc, e, f);
                    if (Double.doubleToRawLongBits(dec) != Double.doubleToRawLongBits(val)) {
                        exceptions++;
                    } else {
                        maxEnc = Math.max(maxEnc, enc);
                        minEnc = Math.min(minEnc, enc);
                    }
                }

                if (sampleCount - exceptions < 1) {
                    continue;
                }
                int bw = (maxEnc == minEnc) ? 0 : 64 - Long.numberOfLeadingZeros(maxEnc - minEnc);
                long cost = (long) sampleCount * bw + (long) exceptions * (64 + 16);

                if (cost < bestCost) {
                    bestCost = cost;
                    bestE = e;
                    bestF = f;
                }
            }
        }
        return (bestE << 16) | bestF;
    }

    /**
     * Compress a stride of double values using ALP + FoR bitpacking.
     * Writes the compressed block to destAddr.
     *
     * @return number of bytes written
     */
    public static int compressDoubles(long srcAddr, int count, int valueShift, long destAddr) {
        long encodedAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        long exceptionAddr = Unsafe.malloc(count, MemoryTag.NATIVE_INDEX_READER);
        try {
            return compressDoubles(srcAddr, count, valueShift, destAddr, encodedAddr, exceptionAddr);
        } finally {
            Unsafe.free(encodedAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.free(exceptionAddr, count, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    public static int compressDoubles(long srcAddr, int count, int valueShift, long destAddr,
                                      long encodedAddr, long exceptionAddr) {
        int params = findBestAlpParams(srcAddr, count, valueShift);
        int e = params >>> 16;
        int f = params & 0xFFFF;

        // Clear exception flags
        Unsafe.getUnsafe().setMemory(exceptionAddr, count, (byte) 0);
        int excCount = 0;
        long fillValue = 0;
        boolean fillFound = false;

        for (int i = 0; i < count; i++) {
            double val = readDouble(srcAddr, i, valueShift);
            long enc = alpEncode(val, e, f);
            if (enc == Long.MAX_VALUE) {
                Unsafe.getUnsafe().putByte(exceptionAddr + i, (byte) 1);
                excCount++;
                continue;
            }
            double dec = alpDecode(enc, e, f);
            if (Double.doubleToRawLongBits(dec) != Double.doubleToRawLongBits(val)) {
                Unsafe.getUnsafe().putByte(exceptionAddr + i, (byte) 1);
                excCount++;
            } else {
                Unsafe.getUnsafe().putLong(encodedAddr + (long) i * Long.BYTES, enc);
                if (!fillFound) {
                    fillValue = enc;
                    fillFound = true;
                }
            }
        }

        // Replace exceptions with fill value
        for (int i = 0; i < count; i++) {
            if (Unsafe.getUnsafe().getByte(exceptionAddr + i) != 0) {
                Unsafe.getUnsafe().putLong(encodedAddr + (long) i * Long.BYTES, fillValue);
            }
        }

        // Compute FoR base and bit width
        long forBase = Long.MAX_VALUE;
        long forMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long v = Unsafe.getUnsafe().getLong(encodedAddr + (long) i * Long.BYTES);
            forBase = Math.min(forBase, v);
            forMax = Math.max(forMax, v);
        }
        if (forBase == Long.MAX_VALUE) {
            forBase = 0;
        }
        int bw = (forMax == forBase) ? 0 : 64 - Long.numberOfLeadingZeros(forMax - forBase);

        // Write header
        long pos = destAddr;
        Unsafe.getUnsafe().putInt(pos, count);
        pos += 4;
        Unsafe.getUnsafe().putByte(pos++, (byte) e);
        Unsafe.getUnsafe().putByte(pos++, (byte) f);
        Unsafe.getUnsafe().putByte(pos++, (byte) bw);
        Unsafe.getUnsafe().putInt(pos, excCount);
        pos += 4;
        Unsafe.getUnsafe().putLong(pos, forBase);
        pos += 8;

        // Write packed data
        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        if (bw > 0) {
            BitpackUtils.packValues(encodedAddr, count, forBase, bw, pos);
        }
        pos += packedBytes;

        // Write exception positions and values
        for (int i = 0; i < count; i++) {
            if (Unsafe.getUnsafe().getByte(exceptionAddr + i) != 0) {
                Unsafe.getUnsafe().putInt(pos, i);
                pos += 4;
            }
        }
        for (int i = 0; i < count; i++) {
            if (Unsafe.getUnsafe().getByte(exceptionAddr + i) != 0) {
                double val = readDouble(srcAddr, i, valueShift);
                Unsafe.getUnsafe().putDouble(pos, val);
                pos += 8;
            }
        }

        return (int) (pos - destAddr);
    }

    /**
     * Decompress ALP-encoded doubles into a pre-allocated output buffer.
     *
     * @return number of bytes consumed from srcAddr
     */
    public static int decompressDoubles(long srcAddr, double[] output) {
        int count = Unsafe.getUnsafe().getInt(srcAddr);
        long workspaceAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        try {
            return decompressDoubles(srcAddr, output, workspaceAddr);
        } finally {
            Unsafe.free(workspaceAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    /**
     * Decompress ALP-encoded doubles using a pre-allocated native workspace to avoid allocation.
     * The workspace must have room for count longs (read from the header at srcAddr).
     */
    public static int decompressDoubles(long srcAddr, double[] output, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int e = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int f = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int excCount = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        long forBase = Unsafe.getUnsafe().getLong(pos);
        pos += 8;

        // Unpack FoR integers
        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, count, bw, forBase, workspaceAddr);
        } else {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(workspaceAddr + (long) i * Long.BYTES, forBase);
            }
        }
        pos += packedBytes;

        // ALP decode: integer -> double
        for (int i = 0; i < count; i++) {
            output[i] = alpDecode(Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES), e, f);
        }

        // Patch exceptions
        long excPosAddr = pos;
        long excValAddr = pos + (long) excCount * 4;
        for (int i = 0; i < excCount; i++) {
            int excIdx = Unsafe.getUnsafe().getInt(excPosAddr + (long) i * 4);
            double excVal = Unsafe.getUnsafe().getDouble(excValAddr + (long) i * 8);
            output[excIdx] = excVal;
        }
        pos = excValAddr + (long) excCount * 8;

        return (int) (pos - srcAddr);
    }

    /**
     * Compress a stride of long values using FoR bitpacking.
     *
     * @return number of bytes written
     */
    public static int compressLongs(long srcAddr, int count, long destAddr) {
        long forBase = Long.MAX_VALUE;
        long forMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
            forBase = Math.min(forBase, val);
            forMax = Math.max(forMax, val);
        }
        if (forBase == Long.MAX_VALUE) {
            forBase = 0;
        }
        int bw = (forMax == forBase) ? 0 : 64 - Long.numberOfLeadingZeros(forMax - forBase);

        long pos = destAddr;
        Unsafe.getUnsafe().putInt(pos, count);
        pos += 4;
        Unsafe.getUnsafe().putByte(pos++, (byte) bw);
        Unsafe.getUnsafe().putLong(pos, forBase);
        pos += 8;

        if (bw > 0) {
            BitpackUtils.packValues(srcAddr, count, forBase, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(count, bw);

        return (int) (pos - destAddr);
    }

    /**
     * Decompress FoR-encoded longs.
     */
    public static int decompressLongs(long srcAddr, long[] output) {
        int count = Unsafe.getUnsafe().getInt(srcAddr);
        long workspaceAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        try {
            return decompressLongs(srcAddr, output, workspaceAddr);
        } finally {
            Unsafe.free(workspaceAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    /**
     * Decompress FoR-encoded longs using a pre-allocated native workspace to avoid allocation.
     * The workspace must have room for count longs (read from the header at srcAddr).
     */
    public static int decompressLongs(long srcAddr, long[] output, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        long forBase = Unsafe.getUnsafe().getLong(pos);
        pos += 8;

        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, count, bw, forBase, workspaceAddr);
        } else {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(workspaceAddr + (long) i * Long.BYTES, forBase);
            }
        }
        for (int i = 0; i < count; i++) {
            output[i] = Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES);
        }
        pos += packedBytes;

        return (int) (pos - srcAddr);
    }

    /**
     * Compress a stride of int values using FoR bitpacking.
     * Widens to long internally, then narrows on decompress.
     *
     * @return number of bytes written
     */
    public static int compressInts(long srcAddr, int count, long destAddr) {
        long workspaceAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        try {
            return compressInts(srcAddr, count, destAddr, workspaceAddr);
        } finally {
            Unsafe.free(workspaceAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    public static int compressInts(long srcAddr, int count, long destAddr, long workspaceAddr) {
        int forBase = Integer.MAX_VALUE;
        int forMax = Integer.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            int val = Unsafe.getUnsafe().getInt(srcAddr + (long) i * Integer.BYTES);
            forBase = Math.min(forBase, val);
            forMax = Math.max(forMax, val);
        }
        if (forBase == Integer.MAX_VALUE) {
            forBase = 0;
        }
        long range = Integer.toUnsignedLong(forMax - forBase);
        int bw = (range == 0) ? 0 : 64 - Long.numberOfLeadingZeros(range);

        long pos = destAddr;
        Unsafe.getUnsafe().putInt(pos, count);
        pos += 4;
        Unsafe.getUnsafe().putByte(pos++, (byte) bw);
        Unsafe.getUnsafe().putInt(pos, forBase);
        pos += 4;

        if (bw > 0) {
            // Widen int→long into workspace for bitpacking
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(workspaceAddr + (long) i * Long.BYTES,
                        Unsafe.getUnsafe().getInt(srcAddr + (long) i * Integer.BYTES));
            }
            BitpackUtils.packValues(workspaceAddr, count, (long) forBase, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(count, bw);

        return (int) (pos - destAddr);
    }

    /**
     * Decompress FoR-encoded ints.
     */
    public static int decompressInts(long srcAddr, int[] output) {
        int count = Unsafe.getUnsafe().getInt(srcAddr);
        long workspaceAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        try {
            return decompressInts(srcAddr, output, workspaceAddr);
        } finally {
            Unsafe.free(workspaceAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    /**
     * Decompress FoR-encoded ints using a pre-allocated native workspace to avoid allocation.
     */
    public static int decompressInts(long srcAddr, int[] output, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int forBase = Unsafe.getUnsafe().getInt(pos);
        pos += 4;

        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, count, bw, 0, workspaceAddr);
            for (int i = 0; i < count; i++) {
                output[i] = forBase + (int) Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES);
            }
        } else {
            for (int i = 0; i < count; i++) {
                output[i] = forBase;
            }
        }
        pos += packedBytes;

        return (int) (pos - srcAddr);
    }

    /**
     * Compute the maximum compressed size for a stride of values.
     * Used to pre-allocate the output buffer.
     */
    public static int maxCompressedSize(int count, int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DOUBLE:
                // ALP header + packed data (worst case 64 bits) + all exceptions
                return DOUBLE_HEADER_SIZE + BitpackUtils.packedDataSize(count, 64)
                        + count * (4 + 8); // worst case: all exceptions (4B pos + 8B value)
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
            case ColumnType.GEOLONG:
                return LONG_HEADER_SIZE + BitpackUtils.packedDataSize(count, 64);
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.GEOINT:
                return INT_HEADER_SIZE + BitpackUtils.packedDataSize(count, 32);
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
                return SHORT_HEADER_SIZE + BitpackUtils.packedDataSize(count, 16);
            default:
                // BYTE, BOOLEAN, GEOBYTE: count header (4B) + 1 byte each
                return 4 + count;
        }
    }

    /**
     * Read a double from a source address, handling the case where the source
     * is a non-double column (e.g., float stored in 4 bytes).
     */
    private static double readDouble(long srcAddr, int index, int shift) {
        if (shift == 3) { // Double.BYTES = 8 = 2^3
            return Unsafe.getUnsafe().getDouble(srcAddr + (long) index * Double.BYTES);
        } else { // Float.BYTES = 4 = 2^2
            return Unsafe.getUnsafe().getFloat(srcAddr + (long) index * Float.BYTES);
        }
    }
}

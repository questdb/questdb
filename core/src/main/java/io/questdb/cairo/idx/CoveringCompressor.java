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
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

/**
 * ALP (Adaptive Lossless floating-Point) compression for covering index
 * sidecar data. Encodes doubles/floats by finding a decimal exponent (e) and
 * correction factor (f) that convert values to integers, then bitpacks those
 * integers with FoR.
 * <p>
 * For integer column types (BYTE, SHORT, INT, LONG), uses plain FoR bitpacking
 * without ALP transformation.
 * <p>
 * Based on "ALP: Adaptive Lossless floating-Point Compression" (Afroozeh &amp;
 * Boncz, SIGMOD 2024).
 */
public class CoveringCompressor {

    static final int MAX_EXPONENT = 18;
    static final int MAX_EXPONENT_FLOAT = 9;
    // 2^52 + 2^51: forces rounding in IEEE 754 double arithmetic
    private static final double SWEET = (double) (1L << 52) + (double) (1L << 51);
    // SWEET trick works in [-2^51, 2^51]. Use a slightly conservative limit.
    private static final double ENCODING_UPPER_LIMIT = (double) (1L << 51);
    private static final double ENCODING_LOWER_LIMIT = -(double) (1L << 51);
    // Float ALP: 2^23 + 2^22 forces rounding in float arithmetic
    private static final float SWEET_F = (float) (1 << 23) + (float) (1 << 22);
    private static final float FLOAT_ENCODING_UPPER_LIMIT = (float) (1 << 22);
    private static final float FLOAT_ENCODING_LOWER_LIMIT = -(float) (1 << 22);

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

    // Float-precision powers of 10
    static final float[] F10F = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f};
    static final float[] IF10F = {1e-0f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f};

    // Compressed block header sizes (excluding packed data)
    // DOUBLE: valueCount(4) + e(1) + f(1) + bitWidth(1) + excCount(4) + forBase(8) = 19
    public static final int DOUBLE_HEADER_SIZE = 19;
    // FLOAT ALP: valueCount(4) + e(1) + f(1) + bitWidth(1) + excCount(4) + forBase(4) = 15
    public static final int FLOAT_ALP_HEADER_SIZE = 15;
    // LONG: valueCount(4) + bitWidth(1) + forBase(8) = 13
    static final int LONG_HEADER_SIZE = 13;
    // LONG delta: valueCount(4) + bitWidth|0x80(1) + deltaBase(8) + firstValue(8) = 21
    static final int LONG_DELTA_HEADER_SIZE = 21;
    // Bit 7 of the bitWidth byte indicates delta encoding (bw in bits 0-6, max 63)
    static final int DELTA_FLAG = 0x80;
    // Bits 7+6 both set indicate linear-prediction encoding (bw in bits 0-5, max 63).
    // Distinguishes from delta-only (bit 7 only) and plain FoR (no flags).
    static final int LINEAR_PRED_FLAG = 0xC0;
    private static final int BW_MASK_6BIT = 0x3F;
    // LONG linear-pred: valueCount(4) + bw|0x40(1) + residualBase(8) + firstValue(8) + stride(8) = 29
    static final int LONG_LINEAR_PRED_HEADER_SIZE = 29;
    // INT: valueCount(4) + bitWidth(1) + forBase(4) = 9
    static final int INT_HEADER_SIZE = 9;
    // SHORT: valueCount(4) + bitWidth(1) + forBase(2) = 7
    static final int SHORT_HEADER_SIZE = 7;
    // BYTE: valueCount(4) + bitWidth(1) + forBase(1) = 6
    static final int BYTE_HEADER_SIZE = 6;

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

    public static float alpDecodeFloat(int encoded, int e, int f) {
        return (float) encoded * F10F[f] * IF10F[e];
    }

    public static int alpEncodeFloat(float value, int e, int f) {
        float tmp = value * F10F[e] * IF10F[f];
        if (!Float.isFinite(tmp) || tmp > FLOAT_ENCODING_UPPER_LIMIT || tmp < FLOAT_ENCODING_LOWER_LIMIT) {
            return Integer.MAX_VALUE;
        }
        return (int) ((tmp + SWEET_F) - SWEET_F);
    }

    public static int compressFloats(long srcAddr, int count, long destAddr, long encodedAddr, long exceptionAddr) {
        int params = findBestAlpParamsFloat(srcAddr, count);
        int e = params >>> 16;
        int f = params & 0xFFFF;

        Unsafe.getUnsafe().setMemory(exceptionAddr, count, (byte) 0);
        int excCount = 0;
        int fillValue = 0;
        boolean fillFound = false;

        for (int i = 0; i < count; i++) {
            float val = Unsafe.getUnsafe().getFloat(srcAddr + (long) i * Float.BYTES);
            int enc = alpEncodeFloat(val, e, f);
            if (enc == Integer.MAX_VALUE) {
                Unsafe.getUnsafe().putByte(exceptionAddr + i, (byte) 1);
                excCount++;
                continue;
            }
            float dec = alpDecodeFloat(enc, e, f);
            if (Float.floatToRawIntBits(dec) != Float.floatToRawIntBits(val)) {
                Unsafe.getUnsafe().putByte(exceptionAddr + i, (byte) 1);
                excCount++;
            } else {
                Unsafe.getUnsafe().putInt(encodedAddr + (long) i * Integer.BYTES, enc);
                if (!fillFound) {
                    fillValue = enc;
                    fillFound = true;
                }
            }
        }

        for (int i = 0; i < count; i++) {
            if (Unsafe.getUnsafe().getByte(exceptionAddr + i) != 0) {
                Unsafe.getUnsafe().putInt(encodedAddr + (long) i * Integer.BYTES, fillValue);
            }
        }

        int forBase = Integer.MAX_VALUE;
        int forMax = Integer.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            int v = Unsafe.getUnsafe().getInt(encodedAddr + (long) i * Integer.BYTES);
            forBase = Math.min(forBase, v);
            forMax = Math.max(forMax, v);
        }
        if (forBase == Integer.MAX_VALUE) forBase = 0;
        long range = (long) forMax - (long) forBase;
        int bw = (range == 0) ? 0 : 64 - Long.numberOfLeadingZeros(range);

        // Write header
        long pos = destAddr;
        Unsafe.getUnsafe().putInt(pos, count);
        pos += 4;
        Unsafe.getUnsafe().putByte(pos++, (byte) e);
        Unsafe.getUnsafe().putByte(pos++, (byte) f);
        Unsafe.getUnsafe().putByte(pos++, (byte) bw);
        Unsafe.getUnsafe().putInt(pos, excCount);
        pos += 4;
        Unsafe.getUnsafe().putInt(pos, forBase);
        pos += 4;

        // Pack encoded values (widen ints to longs in-place for BitpackUtils)
        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        if (bw > 0) {
            // Widen in reverse to avoid overwriting unread ints (long stride > int stride)
            for (int i = count - 1; i >= 0; i--) {
                Unsafe.getUnsafe().putLong(encodedAddr + (long) i * Long.BYTES,
                        Unsafe.getUnsafe().getInt(encodedAddr + (long) i * Integer.BYTES));
            }
            BitpackUtils.packValues(encodedAddr, count, forBase, bw, pos);
        }
        pos += packedBytes;

        // Exception positions and values
        for (int i = 0; i < count; i++) {
            if (Unsafe.getUnsafe().getByte(exceptionAddr + i) != 0) {
                Unsafe.getUnsafe().putInt(pos, i);
                pos += 4;
            }
        }
        for (int i = 0; i < count; i++) {
            if (Unsafe.getUnsafe().getByte(exceptionAddr + i) != 0) {
                Unsafe.getUnsafe().putFloat(pos, Unsafe.getUnsafe().getFloat(srcAddr + (long) i * Float.BYTES));
                pos += 4;
            }
        }
        return (int) (pos - destAddr);
    }

    public static int decompressFloatsToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int e = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int f = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int excCount = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int forBase = Unsafe.getUnsafe().getInt(pos);
        pos += 4;

        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, count, bw, forBase, workspaceAddr);
        } else {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(workspaceAddr + (long) i * Long.BYTES, forBase);
            }
        }
        pos += packedBytes;

        for (int i = 0; i < count; i++) {
            int encoded = (int) Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES);
            Unsafe.getUnsafe().putFloat(outputAddr + (long) i * Float.BYTES, alpDecodeFloat(encoded, e, f));
        }

        long excPosAddr = pos;
        long excValAddr = pos + (long) excCount * 4;
        for (int i = 0; i < excCount; i++) {
            int excIdx = Unsafe.getUnsafe().getInt(excPosAddr + (long) i * 4);
            float excVal = Unsafe.getUnsafe().getFloat(excValAddr + (long) i * 4);
            Unsafe.getUnsafe().putFloat(outputAddr + (long) excIdx * Float.BYTES, excVal);
        }
        return count;
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
     * Decompress ALP-encoded doubles directly to native memory.
     * The workspace must have room for count longs (read from the header at srcAddr).
     *
     * @return number of values decoded
     */
    public static int decompressDoublesToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
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

        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, count, bw, forBase, workspaceAddr);
        } else {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(workspaceAddr + (long) i * Long.BYTES, forBase);
            }
        }
        pos += packedBytes;

        for (int i = 0; i < count; i++) {
            Unsafe.getUnsafe().putDouble(outputAddr + (long) i * Double.BYTES,
                    alpDecode(Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES), e, f));
        }

        long excPosAddr = pos;
        long excValAddr = pos + (long) excCount * 4;
        for (int i = 0; i < excCount; i++) {
            int excIdx = Unsafe.getUnsafe().getInt(excPosAddr + (long) i * 4);
            double excVal = Unsafe.getUnsafe().getDouble(excValAddr + (long) i * 8);
            Unsafe.getUnsafe().putDouble(outputAddr + (long) excIdx * Double.BYTES, excVal);
        }

        return count;
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
     * Compress longs using delta + FoR. Stores first value in the header, then
     * FoR-compresses the deltas between consecutive values. Effective for
     * sorted/monotonic data like timestamps.
     * <p>
     * Header: count(4B) + bitWidth|DELTA_FLAG(1B) + deltaBase(8B) + firstValue(8B) = 21 bytes.
     * Bit 7 of the bitWidth byte distinguishes delta from plain FoR.
     * Reuses {@link BitpackUtils#packValues} for the FoR step.
     *
     * @param longWorkspaceAddr workspace for count longs (used to store deltas)
     * @return number of bytes written
     */
    public static int compressLongsDelta(long srcAddr, int count, long destAddr, long longWorkspaceAddr) {
        if (count <= 1) {
            return compressLongs(srcAddr, count, destAddr);
        }
        long firstValue = Unsafe.getUnsafe().getLong(srcAddr);
        int deltaCount = count - 1;

        // Compute deltas into workspace and find min/max
        long deltaBase = Long.MAX_VALUE;
        long deltaMax = Long.MIN_VALUE;
        long prev = firstValue;
        for (int i = 0; i < deltaCount; i++) {
            long val = Unsafe.getUnsafe().getLong(srcAddr + (long) (i + 1) * Long.BYTES);
            long delta = val - prev;
            prev = val;
            Unsafe.getUnsafe().putLong(longWorkspaceAddr + (long) i * Long.BYTES, delta);
            deltaBase = Math.min(deltaBase, delta);
            deltaMax = Math.max(deltaMax, delta);
        }
        if (deltaBase == Long.MAX_VALUE) {
            deltaBase = 0;
        }
        int bw = (deltaMax == deltaBase) ? 0 : 64 - Long.numberOfLeadingZeros(deltaMax - deltaBase);

        // Write header: count + bitWidth|flag + deltaBase + firstValue
        long pos = destAddr;
        Unsafe.getUnsafe().putInt(pos, count);
        pos += 4;
        Unsafe.getUnsafe().putByte(pos++, (byte) (bw | DELTA_FLAG));
        Unsafe.getUnsafe().putLong(pos, deltaBase);
        pos += 8;
        Unsafe.getUnsafe().putLong(pos, firstValue);
        pos += 8;

        // FoR-pack deltas using existing infrastructure
        if (bw > 0) {
            BitpackUtils.packValues(longWorkspaceAddr, deltaCount, deltaBase, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(deltaCount, bw);

        return (int) (pos - destAddr);
    }

    /**
     * Compress longs using linear prediction + FoR. Computes a linear stride
     * from first to last value, then FoR-compresses the residuals (deviations
     * from the predicted line). Provides O(1) random-access decode.
     * <p>
     * Header: count(4B) + bw|LINEAR_PRED_FLAG(1B) + residualBase(8B) + firstValue(8B) + stride(8B) = 29 bytes.
     *
     * @param longWorkspaceAddr workspace for count longs (used to store residuals)
     * @return number of bytes written
     */
    public static int compressLongsLinearPred(long srcAddr, int count, long destAddr, long longWorkspaceAddr) {
        if (count <= 1) {
            return compressLongs(srcAddr, count, destAddr);
        }
        // Caller guarantees sorted ascending, non-null values (designated timestamp).
        long firstValue = Unsafe.getUnsafe().getLong(srcAddr);
        long lastValue = Unsafe.getUnsafe().getLong(srcAddr + (long) (count - 1) * Long.BYTES);
        long stride = (lastValue - firstValue) / (count - 1);

        // Compute residuals and find min/max
        long resMin = Long.MAX_VALUE;
        long resMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
            long predicted = firstValue + (long) i * stride;
            long residual = val - predicted;
            Unsafe.getUnsafe().putLong(longWorkspaceAddr + (long) i * Long.BYTES, residual);
            resMin = Math.min(resMin, residual);
            resMax = Math.max(resMax, residual);
        }
        if (resMin == Long.MAX_VALUE) {
            resMin = 0;
        }
        int bw = (resMax == resMin) ? 0 : 64 - Long.numberOfLeadingZeros(resMax - resMin);

        long pos = destAddr;
        Unsafe.getUnsafe().putInt(pos, count);
        pos += 4;
        Unsafe.getUnsafe().putByte(pos++, (byte) (bw | LINEAR_PRED_FLAG));
        Unsafe.getUnsafe().putLong(pos, resMin);
        pos += 8;
        Unsafe.getUnsafe().putLong(pos, firstValue);
        pos += 8;
        Unsafe.getUnsafe().putLong(pos, stride);
        pos += 8;

        if (bw > 0) {
            BitpackUtils.packValues(longWorkspaceAddr, count, resMin, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(count, bw);

        return (int) (pos - destAddr);
    }

    /**
     * Compress timestamps using the best available encoding. Tries linear-prediction
     * FoR first (O(1) random access), falls back to delta FoR if linear-pred produces
     * a larger result (e.g., data contains NULL sentinels or highly irregular intervals).
     *
     * @param longWorkspaceAddr workspace for 2 × count longs (used by both encoders)
     * @return number of bytes written
     */
    public static int compressTimestamps(long srcAddr, int count, long destAddr, long longWorkspaceAddr) {
        if (count <= 2) {
            return compressLongsDelta(srcAddr, count, destAddr, longWorkspaceAddr);
        }

        // Compute linear-pred size without writing
        long firstValue = Unsafe.getUnsafe().getLong(srcAddr);
        long lastValue = Unsafe.getUnsafe().getLong(srcAddr + (long) (count - 1) * Long.BYTES);
        long stride = (lastValue - firstValue) / (count - 1);

        long resMin = Long.MAX_VALUE;
        long resMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
            long predicted = firstValue + (long) i * stride;
            long residual = val - predicted;
            resMin = Math.min(resMin, residual);
            resMax = Math.max(resMax, residual);
        }
        long lpRange = resMax - resMin;
        int lpBw = (lpRange == 0) ? 0 : 64 - Long.numberOfLeadingZeros(lpRange);
        int lpSize = LONG_LINEAR_PRED_HEADER_SIZE + BitpackUtils.packedDataSize(count, lpBw);

        // Compute delta size
        long deltaMin = Long.MAX_VALUE;
        long deltaMax = Long.MIN_VALUE;
        long prev = firstValue;
        for (int i = 1; i < count; i++) {
            long val = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
            long delta = val - prev;
            prev = val;
            deltaMin = Math.min(deltaMin, delta);
            deltaMax = Math.max(deltaMax, delta);
        }
        long dRange = deltaMax - deltaMin;
        int dBw = (dRange == 0) ? 0 : 64 - Long.numberOfLeadingZeros(dRange);
        int deltaSize = LONG_DELTA_HEADER_SIZE + BitpackUtils.packedDataSize(count - 1, dBw);

        if (lpSize <= deltaSize) {
            return compressLongsLinearPred(srcAddr, count, destAddr, longWorkspaceAddr);
        }
        return compressLongsDelta(srcAddr, count, destAddr, longWorkspaceAddr);
    }

    /**
     * Decompress FoR-encoded longs (allocates workspace internally).
     * Transparently handles both plain FoR and delta FoR.
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
     * Transparently handles both plain FoR and delta FoR (detected via bit 7 of bitWidth at offset 4).
     */
    public static int decompressLongs(long srcAddr, long[] output, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int rawBw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        boolean isLinearPred = (rawBw & LINEAR_PRED_FLAG) == LINEAR_PRED_FLAG;
        boolean isDelta = !isLinearPred && (rawBw & DELTA_FLAG) != 0;
        int bw = isLinearPred ? (rawBw & BW_MASK_6BIT) : isDelta ? (rawBw & 0x7F) : rawBw;
        long forBase = Unsafe.getUnsafe().getLong(pos);
        pos += 8;

        if (isLinearPred) {
            long firstValue = Unsafe.getUnsafe().getLong(pos);
            pos += 8;
            long stride = Unsafe.getUnsafe().getLong(pos);
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
                output[i] = firstValue + (long) i * stride
                        + Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES);
            }
            pos += packedBytes;
        } else {
            long firstValue = 0;
            int packedCount;
            if (isDelta) {
                firstValue = Unsafe.getUnsafe().getLong(pos);
                pos += 8;
                packedCount = count - 1;
            } else {
                packedCount = count;
            }

            int packedBytes = BitpackUtils.packedDataSize(packedCount, bw);
            if (bw > 0) {
                BitpackUtils.unpackAllValues(pos, packedCount, bw, forBase, workspaceAddr);
            } else {
                for (int i = 0; i < packedCount; i++) {
                    Unsafe.getUnsafe().putLong(workspaceAddr + (long) i * Long.BYTES, forBase);
                }
            }

            if (isDelta) {
                output[0] = firstValue;
                long current = firstValue;
                for (int i = 0; i < packedCount; i++) {
                    current += Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES);
                    output[i + 1] = current;
                }
            } else {
                for (int i = 0; i < count; i++) {
                    output[i] = Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES);
                }
            }
            pos += packedBytes;
        }
        return (int) (pos - srcAddr);
    }

    /**
     * Decompress FoR-encoded longs directly to native memory.
     * The workspace must have room for count longs (read from the header at srcAddr).
     * Transparently handles both plain FoR and delta FoR.
     *
     * @return number of values decoded
     */
    public static int decompressLongsToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int rawBw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        boolean isLinearPred = (rawBw & LINEAR_PRED_FLAG) == LINEAR_PRED_FLAG;
        boolean isDelta = !isLinearPred && (rawBw & DELTA_FLAG) != 0;
        int bw = isLinearPred ? (rawBw & BW_MASK_6BIT) : isDelta ? (rawBw & 0x7F) : rawBw;
        long forBase = Unsafe.getUnsafe().getLong(pos);
        pos += 8;

        if (isLinearPred) {
            long firstValue = Unsafe.getUnsafe().getLong(pos);
            pos += 8;
            long stride = Unsafe.getUnsafe().getLong(pos);
            pos += 8;
            if (bw > 0) {
                BitpackUtils.unpackAllValues(pos, count, bw, forBase, workspaceAddr);
            } else {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(workspaceAddr + (long) i * Long.BYTES, forBase);
                }
            }
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(outputAddr + (long) i * Long.BYTES,
                        firstValue + (long) i * stride
                                + Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES));
            }
        } else {
            long firstValue = 0;
            int packedCount;
            if (isDelta) {
                firstValue = Unsafe.getUnsafe().getLong(pos);
                pos += 8;
                packedCount = count - 1;
            } else {
                packedCount = count;
            }

            if (bw > 0) {
                BitpackUtils.unpackAllValues(pos, packedCount, bw, forBase, workspaceAddr);
            } else {
                for (int i = 0; i < packedCount; i++) {
                    Unsafe.getUnsafe().putLong(workspaceAddr + (long) i * Long.BYTES, forBase);
                }
            }

            if (isDelta) {
                Unsafe.getUnsafe().putLong(outputAddr, firstValue);
                long current = firstValue;
                for (int i = 0; i < packedCount; i++) {
                    current += Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES);
                    Unsafe.getUnsafe().putLong(outputAddr + (long) (i + 1) * Long.BYTES, current);
                }
            } else {
                Unsafe.getUnsafe().copyMemory(workspaceAddr, outputAddr, (long) count * Long.BYTES);
            }
        }

        return count;
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
     * Decompress FoR-encoded ints directly to native memory.
     * The workspace must have room for count longs (read from the header at srcAddr).
     *
     * @return number of values decoded
     */
    public static int decompressIntsToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
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
                Unsafe.getUnsafe().putInt(outputAddr + (long) i * Integer.BYTES,
                        forBase + (int) Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES));
            }
        } else {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putInt(outputAddr + (long) i * Integer.BYTES, forBase);
            }
        }

        return count;
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
            case ColumnType.FLOAT:
                // Float ALP header + packed data (worst case 32 bits) + all exceptions
                return FLOAT_ALP_HEADER_SIZE + BitpackUtils.packedDataSize(count, 32)
                        + count * (4 + 4); // worst case: all exceptions (4B pos + 4B value)
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.GEOLONG:
            case ColumnType.DECIMAL64:
                return LONG_HEADER_SIZE + BitpackUtils.packedDataSize(count, 64);
            case ColumnType.TIMESTAMP:
                // Linear-prediction header is larger than delta (29 vs 21 bytes)
                return LONG_LINEAR_PRED_HEADER_SIZE + BitpackUtils.packedDataSize(count, 64);
            case ColumnType.INT:
            case ColumnType.IPv4:
            case ColumnType.GEOINT:
            case ColumnType.SYMBOL:
            case ColumnType.DECIMAL32:
                return INT_HEADER_SIZE + BitpackUtils.packedDataSize(count, 32);
            case ColumnType.CHAR:
            case ColumnType.SHORT:
            case ColumnType.GEOSHORT:
            case ColumnType.DECIMAL16:
                return SHORT_HEADER_SIZE + BitpackUtils.packedDataSize(count, 16);
            case ColumnType.BYTE:
            case ColumnType.BOOLEAN:
            case ColumnType.GEOBYTE:
            case ColumnType.DECIMAL8:
                return BYTE_HEADER_SIZE + BitpackUtils.packedDataSize(count, 8);
            default:
                // UUID, DECIMAL128, LONG256, DECIMAL256:
                // raw copy with count header (4B) + valueSize each
                return 4 + count * ColumnType.sizeOf(columnType);
        }
    }

    // ==================================================================================
    // Point-access decoders: decode a single value at a given index from compressed data.
    // O(1) random access, zero allocation.
    // ==================================================================================

    /**
     * Read a single ALP-compressed double at the given index.
     * Parses the header, unpacks one FoR value, ALP-decodes it, and checks exceptions.
     */
    public static double readDoubleAt(long srcAddr, int index) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        if (index < 0 || index >= count) return Double.NaN;
        pos += 4;
        int e = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int f = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int excCount = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        long forBase = Unsafe.getUnsafe().getLong(pos);
        pos += 8;

        // Check exceptions first (sparse — typically 0-5 entries)
        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        long excPosAddr = pos + packedBytes;
        long excValAddr = excPosAddr + (long) excCount * Integer.BYTES;
        for (int i = 0; i < excCount; i++) {
            if (Unsafe.getUnsafe().getInt(excPosAddr + (long) i * Integer.BYTES) == index) {
                return Unsafe.getUnsafe().getDouble(excValAddr + (long) i * Double.BYTES);
            }
        }

        long encoded = (bw > 0) ? BitpackUtils.unpackValue(pos, index, bw, forBase) : forBase;
        return alpDecode(encoded, e, f);
    }

    /**
     * Read a single FoR-compressed long at the given index.
     * Handles plain FoR, delta FoR (sequential fallback), and linear-prediction FoR.
     */
    public static long readLongAt(long srcAddr, int index) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        if (index < 0 || index >= count) return Numbers.LONG_NULL;
        pos += 4;
        int rawBw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        boolean isLinearPred = (rawBw & LINEAR_PRED_FLAG) == LINEAR_PRED_FLAG;
        boolean isDelta = !isLinearPred && (rawBw & DELTA_FLAG) != 0;
        int bw = isLinearPred ? (rawBw & BW_MASK_6BIT) : isDelta ? (rawBw & 0x7F) : rawBw;
        long forBase = Unsafe.getUnsafe().getLong(pos);
        pos += 8;

        if (isLinearPred) {
            long firstValue = Unsafe.getUnsafe().getLong(pos);
            pos += 8;
            long stride = Unsafe.getUnsafe().getLong(pos);
            pos += 8;
            long residual = (bw > 0) ? BitpackUtils.unpackValue(pos, index, bw, forBase) : forBase;
            return firstValue + (long) index * stride + residual;
        } else if (isDelta) {
            // Delta FoR: must sum deltas sequentially — O(index) fallback for old data
            long firstValue = Unsafe.getUnsafe().getLong(pos);
            pos += 8;
            if (index == 0) return firstValue;
            long current = firstValue;
            for (int i = 0; i < index; i++) {
                long delta = (bw > 0) ? BitpackUtils.unpackValue(pos, i, bw, forBase) : forBase;
                current += delta;
            }
            return current;
        } else {
            return (bw > 0) ? BitpackUtils.unpackValue(pos, index, bw, forBase) : forBase;
        }
    }

    /**
     * Read a single FoR-compressed int at the given index.
     */
    public static int readIntAt(long srcAddr, int index) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        if (index < 0 || index >= count) return Numbers.INT_NULL;
        pos += 4;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int forBase = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        if (bw == 0) return forBase;
        return forBase + (int) BitpackUtils.unpackValue(pos, index, bw, 0);
    }

    public static int findBestAlpParamsFloat(long srcAddr, int count) {
        int bestE = 0, bestF = 0;
        long bestCost = Long.MAX_VALUE;
        int sampleStep = Math.max(1, count / SAMPLE_SIZE);
        int sampleCount = (count + sampleStep - 1) / sampleStep;

        for (int e = MAX_EXPONENT_FLOAT; e >= 0; e--) {
            for (int f = e; f >= 0; f--) {
                int exceptions = 0;
                int maxEnc = Integer.MIN_VALUE;
                int minEnc = Integer.MAX_VALUE;

                for (int i = 0; i < count; i += sampleStep) {
                    float val = Unsafe.getUnsafe().getFloat(srcAddr + (long) i * Float.BYTES);
                    int enc = alpEncodeFloat(val, e, f);
                    if (enc == Integer.MAX_VALUE) {
                        exceptions++;
                        continue;
                    }
                    float dec = alpDecodeFloat(enc, e, f);
                    if (Float.floatToRawIntBits(dec) != Float.floatToRawIntBits(val)) {
                        exceptions++;
                    } else {
                        maxEnc = Math.max(maxEnc, enc);
                        minEnc = Math.min(minEnc, enc);
                    }
                }

                if (sampleCount - exceptions < 1) continue;
                long range = (long) maxEnc - (long) minEnc;
                int bw = (range == 0) ? 0 : 64 - Long.numberOfLeadingZeros(range);
                long cost = (long) sampleCount * bw + (long) exceptions * (32 + 16);

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
     * Read a single ALP-compressed float at the given index.
     */
    public static float readFloatAt(long srcAddr, int index) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        if (index < 0 || index >= count) return Float.NaN;
        pos += 4;
        int e = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int f = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        int excCount = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int forBase = Unsafe.getUnsafe().getInt(pos);
        pos += 4;

        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        long excPosAddr = pos + packedBytes;
        long excValAddr = excPosAddr + (long) excCount * Integer.BYTES;
        for (int i = 0; i < excCount; i++) {
            if (Unsafe.getUnsafe().getInt(excPosAddr + (long) i * Integer.BYTES) == index) {
                return Unsafe.getUnsafe().getFloat(excValAddr + (long) i * Float.BYTES);
            }
        }

        int encoded = (bw > 0) ? forBase + (int) BitpackUtils.unpackValue(pos, index, bw, 0) : forBase;
        return alpDecodeFloat(encoded, e, f);
    }

    public static int compressShorts(long srcAddr, int count, long destAddr, long workspaceAddr) {
        // Use long arithmetic to handle signed shorts and GeoHash null sentinels (-1)
        // without overflow. Sign-extend each short to long for correct signed comparison.
        long forBase = Long.MAX_VALUE;
        long forMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getUnsafe().getShort(srcAddr + (long) i * Short.BYTES);
            if (val < forBase) forBase = val;
            if (val > forMax) forMax = val;
        }
        if (forBase == Long.MAX_VALUE) forBase = 0;
        long range = forMax - forBase;
        int bw = (range == 0) ? 0 : 64 - Long.numberOfLeadingZeros(range);

        long pos = destAddr;
        Unsafe.getUnsafe().putInt(pos, count);
        pos += 4;
        Unsafe.getUnsafe().putByte(pos++, (byte) bw);
        Unsafe.getUnsafe().putShort(pos, (short) forBase);
        pos += 2;

        if (bw > 0) {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(workspaceAddr + (long) i * Long.BYTES,
                        (long) Unsafe.getUnsafe().getShort(srcAddr + (long) i * Short.BYTES));
            }
            BitpackUtils.packValues(workspaceAddr, count, forBase, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(count, bw);
        return (int) (pos - destAddr);
    }

    public static int compressBytes(long srcAddr, int count, long destAddr, long workspaceAddr) {
        long forBase = Long.MAX_VALUE;
        long forMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getUnsafe().getByte(srcAddr + i);
            if (val < forBase) forBase = val;
            if (val > forMax) forMax = val;
        }
        if (forBase == Long.MAX_VALUE) forBase = 0;
        long range = forMax - forBase;
        int bw = (range == 0) ? 0 : 64 - Long.numberOfLeadingZeros(range);

        long pos = destAddr;
        Unsafe.getUnsafe().putInt(pos, count);
        pos += 4;
        Unsafe.getUnsafe().putByte(pos++, (byte) bw);
        Unsafe.getUnsafe().putByte(pos++, (byte) forBase);

        if (bw > 0) {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(workspaceAddr + (long) i * Long.BYTES,
                        (long) Unsafe.getUnsafe().getByte(srcAddr + i));
            }
            BitpackUtils.packValues(workspaceAddr, count, forBase, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(count, bw);
        return (int) (pos - destAddr);
    }

    public static int decompressShortsToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        long forBase = Unsafe.getUnsafe().getShort(pos);
        pos += 2; // sign-extend

        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, count, bw, forBase, workspaceAddr);
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putShort(outputAddr + (long) i * Short.BYTES,
                        (short) Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES));
            }
        } else {
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putShort(outputAddr + (long) i * Short.BYTES, (short) forBase);
            }
        }
        return count;
    }

    public static int decompressBytesToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        long forBase = Unsafe.getUnsafe().getByte(pos++); // sign-extend

        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, count, bw, forBase, workspaceAddr);
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putByte(outputAddr + i,
                        (byte) Unsafe.getUnsafe().getLong(workspaceAddr + (long) i * Long.BYTES));
            }
        } else {
            Unsafe.getUnsafe().setMemory(outputAddr, count, (byte) forBase);
        }
        return count;
    }

    public static short readShortAt(long srcAddr, int index) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        if (index < 0 || index >= count) return 0;
        pos += 4;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        long forBase = Unsafe.getUnsafe().getShort(pos); // sign-extend
        pos += 2;
        if (bw == 0) return (short) forBase;
        return (short) (forBase + BitpackUtils.unpackValue(pos, index, bw, 0));
    }

    public static byte readByteAt(long srcAddr, int index) {
        long pos = srcAddr;
        int count = Unsafe.getUnsafe().getInt(pos);
        if (index < 0 || index >= count) return 0;
        pos += 4;
        int bw = Unsafe.getUnsafe().getByte(pos++) & 0xFF;
        long forBase = Unsafe.getUnsafe().getByte(pos++); // sign-extend
        if (bw == 0) return (byte) forBase;
        return (byte) (forBase + BitpackUtils.unpackValue(pos, index, bw, 0));
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

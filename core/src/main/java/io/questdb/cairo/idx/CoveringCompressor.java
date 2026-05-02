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

package io.questdb.cairo.idx;

import io.questdb.cairo.ColumnType;
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

    // Compressed block header sizes (excluding packed data)
    // DOUBLE: valueCount(4) + e(1) + f(1) + bitWidth(1) + excCount(4) + forBase(8) = 19
    public static final int DOUBLE_HEADER_SIZE = 19;
    // FLOAT ALP: valueCount(4) + e(1) + f(1) + bitWidth(1) + excCount(4) + forBase(4) = 15
    public static final int FLOAT_ALP_HEADER_SIZE = 15;
    // BYTE: valueCount(4) + bitWidth(1) + forBase(1) = 6
    static final int BYTE_HEADER_SIZE = 6;
    // Pre-computed powers of 10 as doubles (ascending)
    static final double[] F10 = {
            1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
            1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
    };
    // Float-precision powers of 10
    static final float[] F10F = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f};
    // Pre-computed inverse powers of 10 as doubles
    static final double[] IF10 = {
            1e-0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9,
            1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18
    };
    static final float[] IF10F = {1e-0f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f};
    static final int INT_HEADER_SIZE = 9;
    static final int LINEAR_PRED_FLAG = 0xC0;
    static final int LONG_HEADER_SIZE = 13;
    static final int LONG_LINEAR_PRED_HEADER_SIZE = 29;
    static final int MAX_EXPONENT = 18;
    static final int MAX_EXPONENT_FLOAT = 9;
    static final int RAW_BLOCK_FLAG = 0x80000000;
    static final int SHORT_HEADER_SIZE = 7;
    private static final int BW_MASK_6BIT = 0x3F;
    private static final double ENCODING_LOWER_LIMIT = -(double) (1L << 51);
    private static final double ENCODING_UPPER_LIMIT = (double) (1L << 51);
    private static final float FLOAT_ENCODING_LOWER_LIMIT = -(float) (1 << 22);
    private static final float FLOAT_ENCODING_UPPER_LIMIT = (float) (1 << 22);
    private static final int SAMPLE_SIZE = 64;
    private static final double SWEET = (double) (1L << 52) + (double) (1L << 51);
    private static final float SWEET_F = (float) (1 << 23) + (float) (1 << 22);

    /**
     * Decode a long integer back to a double using ALP.
     */
    public static double alpDecode(long encoded, int e, int f) {
        return (double) encoded * F10[f] * IF10[e];
    }

    public static float alpDecodeFloat(int encoded, int e, int f) {
        return (float) encoded * F10F[f] * IF10F[e];
    }

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

    public static int alpEncodeFloat(float value, int e, int f) {
        float tmp = value * F10F[e] * IF10F[f];
        if (!Float.isFinite(tmp) || tmp > FLOAT_ENCODING_UPPER_LIMIT || tmp < FLOAT_ENCODING_LOWER_LIMIT) {
            return Integer.MAX_VALUE;
        }
        return (int) ((tmp + SWEET_F) - SWEET_F);
    }

    public static int compressBytes(long srcAddr, int count, long destAddr, long workspaceAddr) {
        long forBase = Long.MAX_VALUE;
        long forMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getByte(srcAddr + i);
            if (val < forBase) forBase = val;
            if (val > forMax) forMax = val;
        }
        int bw = bitsRequired(forBase, forMax);
        if (forBase == Long.MAX_VALUE) {
            forBase = 0;
        }

        long pos = destAddr;
        Unsafe.putInt(pos, count);
        pos += 4;
        Unsafe.putByte(pos++, (byte) bw);
        Unsafe.putByte(pos++, (byte) forBase);

        if (bw > 0) {
            for (int i = 0; i < count; i++) {
                Unsafe.putLong(workspaceAddr + (long) i * Long.BYTES,
                        Unsafe.getByte(srcAddr + i));
            }
            PostingIndexNative.packValuesNativeFallback(workspaceAddr, count, forBase, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(count, bw);
        return (int) (pos - destAddr);
    }

    /**
     * Compress a stride of double values using ALP + FoR bitpacking.
     * Writes the compressed block to destAddr.
     *
     * @return number of bytes written
     */
    public static int compressDoubles(long srcAddr, int count, int valueShift, long destAddr,
                                      long encodedAddr, long exceptionAddr) {
        int params = findBestAlpParams(srcAddr, count, valueShift);
        int e = params >>> 16;
        int f = params & 0xFFFF;

        // Clear exception flags
        Unsafe.setMemory(exceptionAddr, count, (byte) 0);
        int excCount = 0;
        long fillValue = 0;
        boolean fillFound = false;

        for (int i = 0; i < count; i++) {
            double val = readDouble(srcAddr, i, valueShift);
            long enc = alpEncode(val, e, f);
            if (enc == Long.MAX_VALUE) {
                Unsafe.putByte(exceptionAddr + i, (byte) 1);
                excCount++;
                continue;
            }
            double dec = alpDecode(enc, e, f);
            if (Double.doubleToRawLongBits(dec) != Double.doubleToRawLongBits(val)) {
                Unsafe.putByte(exceptionAddr + i, (byte) 1);
                excCount++;
            } else {
                Unsafe.putLong(encodedAddr + (long) i * Long.BYTES, enc);
                if (!fillFound) {
                    fillValue = enc;
                    fillFound = true;
                }
            }
        }

        // Replace exceptions with fill value
        for (int i = 0; i < count; i++) {
            if (Unsafe.getByte(exceptionAddr + i) != 0) {
                Unsafe.putLong(encodedAddr + (long) i * Long.BYTES, fillValue);
            }
        }

        // Compute FoR base and bit width
        long forBase = Long.MAX_VALUE;
        long forMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long v = Unsafe.getLong(encodedAddr + (long) i * Long.BYTES);
            forBase = Math.min(forBase, v);
            forMax = Math.max(forMax, v);
        }
        int bw = bitsRequired(forBase, forMax);
        if (forBase == Long.MAX_VALUE) {
            forBase = 0;
        }

        // Write header
        long pos = destAddr;
        Unsafe.putInt(pos, count);
        pos += 4;
        Unsafe.putByte(pos++, (byte) e);
        Unsafe.putByte(pos++, (byte) f);
        Unsafe.putByte(pos++, (byte) bw);
        Unsafe.putInt(pos, excCount);
        pos += 4;
        Unsafe.putLong(pos, forBase);
        pos += 8;

        // Write packed data
        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        if (bw > 0) {
            PostingIndexNative.packValuesNativeFallback(encodedAddr, count, forBase, bw, pos);
        }
        pos += packedBytes;

        // Write exception positions and values
        for (int i = 0; i < count; i++) {
            if (Unsafe.getByte(exceptionAddr + i) != 0) {
                Unsafe.putInt(pos, i);
                pos += 4;
            }
        }
        for (int i = 0; i < count; i++) {
            if (Unsafe.getByte(exceptionAddr + i) != 0) {
                double val = readDouble(srcAddr, i, valueShift);
                Unsafe.putDouble(pos, val);
                pos += 8;
            }
        }

        return (int) (pos - destAddr);
    }

    /**
     * Compress a stride of float values using ALP + FoR bitpacking.
     *
     * @param encodedAddr   workspace for count longs (stores ALP-encoded ints sign-extended to longs)
     * @param exceptionAddr workspace for count bytes (exception bitmap)
     * @return number of bytes written
     */
    public static int compressFloats(long srcAddr, int count, long destAddr, long encodedAddr, long exceptionAddr) {
        int params = findBestAlpParamsFloat(srcAddr, count);
        int e = params >>> 16;
        int f = params & 0xFFFF;

        Unsafe.setMemory(exceptionAddr, count, (byte) 0);
        int excCount = 0;
        long fillValue = 0;
        boolean fillFound = false;

        for (int i = 0; i < count; i++) {
            float val = Unsafe.getFloat(srcAddr + (long) i * Float.BYTES);
            int enc = alpEncodeFloat(val, e, f);
            if (enc == Integer.MAX_VALUE) {
                Unsafe.putByte(exceptionAddr + i, (byte) 1);
                excCount++;
                continue;
            }
            float dec = alpDecodeFloat(enc, e, f);
            if (Float.floatToRawIntBits(dec) != Float.floatToRawIntBits(val)) {
                Unsafe.putByte(exceptionAddr + i, (byte) 1);
                excCount++;
            } else {
                Unsafe.putLong(encodedAddr + (long) i * Long.BYTES, enc);
                if (!fillFound) {
                    fillValue = enc;
                    fillFound = true;
                }
            }
        }

        for (int i = 0; i < count; i++) {
            if (Unsafe.getByte(exceptionAddr + i) != 0) {
                Unsafe.putLong(encodedAddr + (long) i * Long.BYTES, fillValue);
            }
        }

        long forBase = Long.MAX_VALUE;
        long forMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long v = Unsafe.getLong(encodedAddr + (long) i * Long.BYTES);
            forBase = Math.min(forBase, v);
            forMax = Math.max(forMax, v);
        }
        int bw = bitsRequired(forBase, forMax);
        if (forBase == Long.MAX_VALUE) {
            forBase = 0;
        }

        long pos = destAddr;
        Unsafe.putInt(pos, count);
        pos += 4;
        Unsafe.putByte(pos++, (byte) e);
        Unsafe.putByte(pos++, (byte) f);
        Unsafe.putByte(pos++, (byte) bw);
        Unsafe.putInt(pos, excCount);
        pos += 4;
        Unsafe.putInt(pos, (int) forBase);
        pos += 4;

        int packedBytes = BitpackUtils.packedDataSize(count, bw);
        if (bw > 0) {
            PostingIndexNative.packValuesNativeFallback(encodedAddr, count, forBase, bw, pos);
        }
        pos += packedBytes;

        // Exception positions and values
        for (int i = 0; i < count; i++) {
            if (Unsafe.getByte(exceptionAddr + i) != 0) {
                Unsafe.putInt(pos, i);
                pos += 4;
            }
        }
        for (int i = 0; i < count; i++) {
            if (Unsafe.getByte(exceptionAddr + i) != 0) {
                Unsafe.putFloat(pos, Unsafe.getFloat(srcAddr + (long) i * Float.BYTES));
                pos += 4;
            }
        }
        return (int) (pos - destAddr);
    }

    /**
     * Compress a stride of int values using FoR bitpacking.
     * Widens to long internally, then narrows on decompress.
     *
     * @return number of bytes written
     */
    public static int compressInts(long srcAddr, int count, long destAddr, long workspaceAddr) {
        int forBase = Integer.MAX_VALUE;
        int forMax = Integer.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            int val = Unsafe.getInt(srcAddr + (long) i * Integer.BYTES);
            forBase = Math.min(forBase, val);
            forMax = Math.max(forMax, val);
        }
        int bw = bitsRequired(forBase, forMax);
        if (forBase == Integer.MAX_VALUE) {
            forBase = 0;
        }

        long pos = destAddr;
        Unsafe.putInt(pos, count);
        pos += 4;
        Unsafe.putByte(pos++, (byte) bw);
        Unsafe.putInt(pos, forBase);
        pos += 4;

        if (bw > 0) {
            // Widen int→long into workspace for bitpacking
            for (int i = 0; i < count; i++) {
                Unsafe.putLong(workspaceAddr + (long) i * Long.BYTES,
                        Unsafe.getInt(srcAddr + (long) i * Integer.BYTES));
            }
            PostingIndexNative.packValuesNativeFallback(workspaceAddr, count, forBase, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(count, bw);

        return (int) (pos - destAddr);
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
            long val = Unsafe.getLong(srcAddr + (long) i * Long.BYTES);
            forBase = Math.min(forBase, val);
            forMax = Math.max(forMax, val);
        }
        int bw = bitsRequired(forBase, forMax);
        if (forBase == Long.MAX_VALUE) {
            forBase = 0;
        }

        long pos = destAddr;
        Unsafe.putInt(pos, count);
        pos += 4;
        Unsafe.putByte(pos++, (byte) bw);
        Unsafe.putLong(pos, forBase);
        pos += 8;

        if (bw > 0) {
            PostingIndexNative.packValuesNativeFallback(srcAddr, count, forBase, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(count, bw);

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
        long firstValue = Unsafe.getLong(srcAddr);
        long lastValue = Unsafe.getLong(srcAddr + (long) (count - 1) * Long.BYTES);
        assert lastValue >= firstValue : "linear-pred requires sorted ascending input";
        long stride = (lastValue - firstValue) / (count - 1);

        long resMin = Long.MAX_VALUE;
        long resMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getLong(srcAddr + (long) i * Long.BYTES);
            long predicted = firstValue + (long) i * stride;
            long residual = val - predicted;
            Unsafe.putLong(longWorkspaceAddr + (long) i * Long.BYTES, residual);
            resMin = Math.min(resMin, residual);
            resMax = Math.max(resMax, residual);
        }
        int bw = bitsRequired(resMin, resMax);
        if (bw > 63) {
            return compressLongs(srcAddr, count, destAddr);
        }
        if (resMin == Long.MAX_VALUE) {
            resMin = 0;
        }

        long pos = destAddr;
        Unsafe.putInt(pos, count);
        pos += 4;
        Unsafe.putByte(pos++, (byte) (bw | LINEAR_PRED_FLAG));
        Unsafe.putLong(pos, resMin);
        pos += 8;
        Unsafe.putLong(pos, firstValue);
        pos += 8;
        Unsafe.putLong(pos, stride);
        pos += 8;

        if (bw > 0) {
            PostingIndexNative.packValuesNativeFallback(longWorkspaceAddr, count, resMin, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(count, bw);

        return (int) (pos - destAddr);
    }

    public static int compressShorts(long srcAddr, int count, long destAddr, long workspaceAddr) {
        // Use long arithmetic to handle signed shorts and GeoHash null sentinels (-1)
        // without overflow. Sign-extend each short to long for correct signed comparison.
        long forBase = Long.MAX_VALUE;
        long forMax = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getShort(srcAddr + (long) i * Short.BYTES);
            if (val < forBase) forBase = val;
            if (val > forMax) forMax = val;
        }
        int bw = bitsRequired(forBase, forMax);
        if (forBase == Long.MAX_VALUE) {
            forBase = 0;
        }

        long pos = destAddr;
        Unsafe.putInt(pos, count);
        pos += 4;
        Unsafe.putByte(pos++, (byte) bw);
        Unsafe.putShort(pos, (short) forBase);
        pos += 2;

        if (bw > 0) {
            for (int i = 0; i < count; i++) {
                Unsafe.putLong(workspaceAddr + (long) i * Long.BYTES,
                        Unsafe.getShort(srcAddr + (long) i * Short.BYTES));
            }
            PostingIndexNative.packValuesNativeFallback(workspaceAddr, count, forBase, bw, pos);
        }
        pos += BitpackUtils.packedDataSize(count, bw);
        return (int) (pos - destAddr);
    }

    public static void decompressBytesToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getInt(pos);
        pos += 4;
        int bw = Unsafe.getByte(pos++) & 0xFF;
        long forBase = Unsafe.getByte(pos++); // sign-extend

        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, count, bw, forBase, workspaceAddr);
            for (int i = 0; i < count; i++) {
                Unsafe.putByte(outputAddr + i,
                        (byte) Unsafe.getLong(workspaceAddr + (long) i * Long.BYTES));
            }
        } else {
            Unsafe.setMemory(outputAddr, count, (byte) forBase);
        }
    }

    /**
     * Decompress ALP-encoded doubles directly to native memory.
     * The workspace must have room for count longs (read from the header at srcAddr).
     */
    public static void decompressDoublesToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
        long pos = srcAddr;
        int rawCount = Unsafe.getInt(pos);
        if ((rawCount & RAW_BLOCK_FLAG) != 0) {
            int count = rawCount & ~RAW_BLOCK_FLAG;
            Unsafe.copyMemory(pos + 4, outputAddr, (long) count * Double.BYTES);
            return;
        }
        pos += 4;
        int e = Unsafe.getByte(pos++) & 0xFF;
        int f = Unsafe.getByte(pos++) & 0xFF;
        int bw = Unsafe.getByte(pos++) & 0xFF;
        int excCount = Unsafe.getInt(pos);
        pos += 4;
        long forBase = Unsafe.getLong(pos);
        pos += 8;

        int packedBytes = BitpackUtils.packedDataSize(rawCount, bw);
        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, rawCount, bw, forBase, workspaceAddr);
        } else {
            for (int i = 0; i < rawCount; i++) {
                Unsafe.putLong(workspaceAddr + (long) i * Long.BYTES, forBase);
            }
        }
        pos += packedBytes;

        for (int i = 0; i < rawCount; i++) {
            Unsafe.putDouble(outputAddr + (long) i * Double.BYTES,
                    alpDecode(Unsafe.getLong(workspaceAddr + (long) i * Long.BYTES), e, f));
        }

        long excPosAddr = pos;
        long excValAddr = pos + (long) excCount * 4;
        for (int i = 0; i < excCount; i++) {
            int excIdx = Unsafe.getInt(excPosAddr + (long) i * 4);
            double excVal = Unsafe.getDouble(excValAddr + (long) i * 8);
            Unsafe.putDouble(outputAddr + (long) excIdx * Double.BYTES, excVal);
        }

    }

    public static void decompressFloatsToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
        long pos = srcAddr;
        int rawCount = Unsafe.getInt(pos);
        if ((rawCount & RAW_BLOCK_FLAG) != 0) {
            int count = rawCount & ~RAW_BLOCK_FLAG;
            Unsafe.copyMemory(pos + 4, outputAddr, (long) count * Float.BYTES);
            return;
        }
        pos += 4;
        int e = Unsafe.getByte(pos++) & 0xFF;
        int f = Unsafe.getByte(pos++) & 0xFF;
        int bw = Unsafe.getByte(pos++) & 0xFF;
        int excCount = Unsafe.getInt(pos);
        pos += 4;
        int forBase = Unsafe.getInt(pos);
        pos += 4;

        int packedBytes = BitpackUtils.packedDataSize(rawCount, bw);
        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, rawCount, bw, forBase, workspaceAddr);
        } else {
            for (int i = 0; i < rawCount; i++) {
                Unsafe.putLong(workspaceAddr + (long) i * Long.BYTES, forBase);
            }
        }
        pos += packedBytes;

        for (int i = 0; i < rawCount; i++) {
            int encoded = (int) Unsafe.getLong(workspaceAddr + (long) i * Long.BYTES);
            Unsafe.putFloat(outputAddr + (long) i * Float.BYTES, alpDecodeFloat(encoded, e, f));
        }

        long excPosAddr = pos;
        long excValAddr = pos + (long) excCount * 4;
        for (int i = 0; i < excCount; i++) {
            int excIdx = Unsafe.getInt(excPosAddr + (long) i * 4);
            float excVal = Unsafe.getFloat(excValAddr + (long) i * 4);
            Unsafe.putFloat(outputAddr + (long) excIdx * Float.BYTES, excVal);
        }
    }

    /**
     * Decompress FoR-encoded ints directly to native memory.
     * The workspace must have room for count longs (read from the header at srcAddr).
     */
    public static void decompressIntsToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getInt(pos);
        pos += 4;
        int bw = Unsafe.getByte(pos++) & 0xFF;
        int forBase = Unsafe.getInt(pos);
        pos += 4;

        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, count, bw, 0, workspaceAddr);
            for (int i = 0; i < count; i++) {
                Unsafe.putInt(outputAddr + (long) i * Integer.BYTES,
                        forBase + (int) Unsafe.getLong(workspaceAddr + (long) i * Long.BYTES));
            }
        } else {
            for (int i = 0; i < count; i++) {
                Unsafe.putInt(outputAddr + (long) i * Integer.BYTES, forBase);
            }
        }

    }

    /**
     * Decompress FoR-encoded longs directly to native memory.
     * The workspace must have room for count longs (read from the header at srcAddr).
     * Handles plain FoR and linear-prediction FoR.
     */
    public static void decompressLongsToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getInt(pos);
        pos += 4;
        int rawBw = Unsafe.getByte(pos++) & 0xFF;
        boolean isLinearPred = (rawBw & LINEAR_PRED_FLAG) == LINEAR_PRED_FLAG;
        int bw = isLinearPred ? rawBw & BW_MASK_6BIT : rawBw;
        long forBase = Unsafe.getLong(pos);
        pos += 8;

        if (isLinearPred) {
            long firstValue = Unsafe.getLong(pos);
            pos += 8;
            long stride = Unsafe.getLong(pos);
            pos += 8;
            if (bw > 0) {
                BitpackUtils.unpackAllValues(pos, count, bw, forBase, workspaceAddr);
            } else {
                for (int i = 0; i < count; i++) {
                    Unsafe.putLong(workspaceAddr + (long) i * Long.BYTES, forBase);
                }
            }
            for (int i = 0; i < count; i++) {
                Unsafe.putLong(outputAddr + (long) i * Long.BYTES,
                        firstValue + (long) i * stride
                                + Unsafe.getLong(workspaceAddr + (long) i * Long.BYTES));
            }
        } else {
            if (bw > 0) {
                BitpackUtils.unpackAllValues(pos, count, bw, forBase, outputAddr);
            } else {
                for (int i = 0; i < count; i++) {
                    Unsafe.putLong(outputAddr + (long) i * Long.BYTES, forBase);
                }
            }
        }

    }

    public static void decompressShortsToAddr(long srcAddr, long outputAddr, long workspaceAddr) {
        long pos = srcAddr;
        int count = Unsafe.getInt(pos);
        pos += 4;
        int bw = Unsafe.getByte(pos++) & 0xFF;
        long forBase = Unsafe.getShort(pos);
        pos += 2; // sign-extend

        if (bw > 0) {
            BitpackUtils.unpackAllValues(pos, count, bw, forBase, workspaceAddr);
            for (int i = 0; i < count; i++) {
                Unsafe.putShort(outputAddr + (long) i * Short.BYTES,
                        (short) Unsafe.getLong(workspaceAddr + (long) i * Long.BYTES));
            }
        } else {
            for (int i = 0; i < count; i++) {
                Unsafe.putShort(outputAddr + (long) i * Short.BYTES, (short) forBase);
            }
        }
    }

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
                int bw = bitsRequired(minEnc, maxEnc);
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
                    float val = Unsafe.getFloat(srcAddr + (long) i * Float.BYTES);
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
                int bw = bitsRequired(minEnc, maxEnc);
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
     * Compute the maximum compressed size for a stride of values.
     * Used to pre-allocate the output buffer.
     */
    public static int maxCompressedSize(int count, int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DOUBLE ->
                // ALP header + packed data (worst case 64 bits) + all exceptions
                    DOUBLE_HEADER_SIZE + BitpackUtils.packedDataSize(count, 64)
                            + count * (4 + 8); // worst case: all exceptions (4B pos + 8B value)
            case ColumnType.FLOAT ->
                // Float ALP header + packed data (worst case 32 bits) + all exceptions
                    FLOAT_ALP_HEADER_SIZE + BitpackUtils.packedDataSize(count, 32)
                            + count * (4 + 4); // worst case: all exceptions (4B pos + 4B value)
            case ColumnType.LONG, ColumnType.DATE, ColumnType.GEOLONG, ColumnType.DECIMAL64 ->
                    LONG_HEADER_SIZE + BitpackUtils.packedDataSize(count, 64);
            case ColumnType.TIMESTAMP ->
                // Linear-prediction header is larger than delta (29 vs 21 bytes)
                    LONG_LINEAR_PRED_HEADER_SIZE + BitpackUtils.packedDataSize(count, 64);
            case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT, ColumnType.SYMBOL, ColumnType.DECIMAL32 ->
                    INT_HEADER_SIZE + BitpackUtils.packedDataSize(count, 32);
            case ColumnType.CHAR, ColumnType.SHORT, ColumnType.GEOSHORT, ColumnType.DECIMAL16 ->
                    SHORT_HEADER_SIZE + BitpackUtils.packedDataSize(count, 16);
            case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE, ColumnType.DECIMAL8 ->
                    BYTE_HEADER_SIZE + BitpackUtils.packedDataSize(count, 8);
            case ColumnType.UUID, ColumnType.DECIMAL128, ColumnType.LONG256, ColumnType.DECIMAL256 ->
                    4 + count * ColumnType.sizeOf(columnType);
            default -> throw new AssertionError("maxCompressedSize: unsupported column type " + columnType);
        };
    }

    public static byte readByteAt(long srcAddr, int index) {
        long pos = srcAddr;
        int count = Unsafe.getInt(pos);
        if (index < 0 || index >= count) {
            return 0;
        }
        pos += 4;
        int bw = Unsafe.getByte(pos++) & 0xFF;
        long forBase = Unsafe.getByte(pos++); // sign-extend
        if (bw == 0) {
            return (byte) forBase;
        }
        return (byte) (forBase + BitpackUtils.unpackValue(pos, index, bw, 0));
    }

    /**
     * Read a single ALP-compressed double at the given index.
     * Parses the header, unpacks one FoR value, ALP-decodes it, and checks exceptions.
     */
    public static double readDoubleAt(long srcAddr, int index) {
        long pos = srcAddr;
        int rawCount = Unsafe.getInt(pos);
        if ((rawCount & RAW_BLOCK_FLAG) != 0) {
            int count = rawCount & ~RAW_BLOCK_FLAG;
            if (index < 0 || index >= count) return Double.NaN;
            return Unsafe.getDouble(pos + 4 + (long) index * Double.BYTES);
        }
        if (index < 0 || index >= rawCount) {
            return Double.NaN;
        }

        pos += 4;
        int e = Unsafe.getByte(pos++) & 0xFF;
        int f = Unsafe.getByte(pos++) & 0xFF;
        int bw = Unsafe.getByte(pos++) & 0xFF;
        int excCount = Unsafe.getInt(pos);
        pos += 4;
        long forBase = Unsafe.getLong(pos);
        pos += 8;

        // Check exceptions first (sparse — typically 0-5 entries)
        int packedBytes = BitpackUtils.packedDataSize(rawCount, bw);
        long excPosAddr = pos + packedBytes;
        long excValAddr = excPosAddr + (long) excCount * Integer.BYTES;
        for (int i = 0; i < excCount; i++) {
            if (Unsafe.getInt(excPosAddr + (long) i * Integer.BYTES) == index) {
                return Unsafe.getDouble(excValAddr + (long) i * Double.BYTES);
            }
        }

        long encoded = (bw > 0) ? BitpackUtils.unpackValue(pos, index, bw, forBase) : forBase;
        return alpDecode(encoded, e, f);
    }

    /**
     * Read a single ALP-compressed float at the given index.
     */
    public static float readFloatAt(long srcAddr, int index) {
        long pos = srcAddr;
        int rawCount = Unsafe.getInt(pos);
        if ((rawCount & RAW_BLOCK_FLAG) != 0) {
            int count = rawCount & ~RAW_BLOCK_FLAG;
            if (index < 0 || index >= count) return Float.NaN;
            return Unsafe.getFloat(pos + 4 + (long) index * Float.BYTES);
        }
        if (index < 0 || index >= rawCount) return Float.NaN;
        pos += 4;
        int e = Unsafe.getByte(pos++) & 0xFF;
        int f = Unsafe.getByte(pos++) & 0xFF;
        int bw = Unsafe.getByte(pos++) & 0xFF;
        int excCount = Unsafe.getInt(pos);
        pos += 4;
        int forBase = Unsafe.getInt(pos);
        pos += 4;

        int packedBytes = BitpackUtils.packedDataSize(rawCount, bw);
        long excPosAddr = pos + packedBytes;
        long excValAddr = excPosAddr + (long) excCount * Integer.BYTES;
        for (int i = 0; i < excCount; i++) {
            if (Unsafe.getInt(excPosAddr + (long) i * Integer.BYTES) == index) {
                return Unsafe.getFloat(excValAddr + (long) i * Float.BYTES);
            }
        }

        int encoded = (bw > 0) ? forBase + (int) BitpackUtils.unpackValue(pos, index, bw, 0) : forBase;
        return alpDecodeFloat(encoded, e, f);
    }

    /**
     * Read a single FoR-compressed int at the given index.
     */
    public static int readIntAt(long srcAddr, int index) {
        long pos = srcAddr;
        int count = Unsafe.getInt(pos);
        if (index < 0 || index >= count) {
            return Numbers.INT_NULL;
        }
        pos += 4;
        int bw = Unsafe.getByte(pos++) & 0xFF;
        int forBase = Unsafe.getInt(pos);
        pos += 4;
        if (bw == 0) {
            return forBase;
        }
        return forBase + (int) BitpackUtils.unpackValue(pos, index, bw, 0);
    }

    /**
     * Read a single FoR-compressed long at the given index.
     * Handles plain FoR and linear-prediction FoR; both O(1).
     */
    public static long readLongAt(long srcAddr, int index) {
        long pos = srcAddr;
        int count = Unsafe.getInt(pos);
        if (index < 0 || index >= count) return Numbers.LONG_NULL;
        pos += 4;
        int rawBw = Unsafe.getByte(pos++) & 0xFF;
        boolean isLinearPred = (rawBw & LINEAR_PRED_FLAG) == LINEAR_PRED_FLAG;
        int bw = isLinearPred ? rawBw & BW_MASK_6BIT : rawBw;
        long forBase = Unsafe.getLong(pos);
        pos += 8;

        if (isLinearPred) {
            long firstValue = Unsafe.getLong(pos);
            pos += 8;
            long stride = Unsafe.getLong(pos);
            pos += 8;
            long residual = bw > 0 ? BitpackUtils.unpackValue(pos, index, bw, forBase) : forBase;
            return firstValue + (long) index * stride + residual;
        }
        return bw > 0 ? BitpackUtils.unpackValue(pos, index, bw, forBase) : forBase;
    }

    public static short readShortAt(long srcAddr, int index) {
        long pos = srcAddr;
        int count = Unsafe.getInt(pos);
        if (index < 0 || index >= count) {
            return 0;
        }
        pos += 4;
        int bw = Unsafe.getByte(pos++) & 0xFF;
        long forBase = Unsafe.getShort(pos); // sign-extend
        pos += 2;
        if (bw == 0) {
            return (short) forBase;
        }
        return (short) (forBase + BitpackUtils.unpackValue(pos, index, bw, 0));
    }

    private static int bitsRequired(long min, long max) {
        if (min > max) {
            return 0;
        }
        long span = max - min;
        return span == 0 ? 0 : 64 - Long.numberOfLeadingZeros(span);
    }

    /**
     * Read a double from a source address, handling the case where the source
     * is a non-double column (e.g., float stored in 4 bytes).
     */
    private static double readDouble(long srcAddr, int index, int shift) {
        assert shift == 2 || shift == 3 : "readDouble: shift must be 2 (float) or 3 (double), got " + shift;
        if (shift == 3) { // Double.BYTES = 8 = 2^3
            return Unsafe.getDouble(srcAddr + (long) index * Double.BYTES);
        } else { // Float.BYTES = 4 = 2^2
            return Unsafe.getFloat(srcAddr + (long) index * Float.BYTES);
        }
    }
}

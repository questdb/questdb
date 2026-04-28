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

import io.questdb.std.Os;
import io.questdb.std.Unsafe;

/**
 * Native SIMD-accelerated bitpacking operations for BP bitmap index.
 * <p>
 * All operations work on native memory addresses — no Java array copies
 * across JNI. AVX2 specializations for 8/16/32-bit aligned widths
 * process 4 values per cycle.
 */
public final class PostingIndexNative {
    private static final boolean NATIVE_AVAILABLE;

    private PostingIndexNative() {
    }

    public static boolean isNativeAvailable() {
        return NATIVE_AVAILABLE;
    }

    /**
     * Packs values from native memory into bit-packed format.
     * Both source and destination are native memory addresses.
     *
     * @param valuesAddr native address of int64 values array
     * @param count      number of values
     * @param minValue   reference frame
     * @param bitWidth   bits per offset
     * @param destAddr   destination native address
     */
    public static void packValuesNative(long valuesAddr, int count, long minValue, int bitWidth, long destAddr) {
        validateBitWidth(bitWidth);
        if (NATIVE_AVAILABLE) {
            packValues0(valuesAddr, count, minValue, bitWidth, destAddr);
        } else {
            packValuesNativeFallback(valuesAddr, count, minValue, bitWidth, destAddr);
        }
    }

    public static void packValuesNativeFallback(long valuesAddr, int count, long minValue,
                                                int bitWidth, long destAddr) {
        long buffer = 0;
        int bufferBits = 0;
        int destOffset = 0;

        for (int i = 0; i < count; i++) {
            long offset = Unsafe.getUnsafe().getLong(valuesAddr + (long) i * Long.BYTES) - minValue;
            int oldBufferBits = bufferBits;
            buffer |= (offset << bufferBits);
            bufferBits += bitWidth;

            while (bufferBits >= 8) {
                Unsafe.getUnsafe().putByte(destAddr + destOffset, (byte) buffer);
                buffer >>>= 8;
                bufferBits -= 8;
                destOffset++;
            }

            // When the shift overflows 64 bits, the high bits of offset were
            // lost by (offset << oldBufferBits). Recover them into the residual buffer.
            if (oldBufferBits + bitWidth > 64) {
                int loBitsStored = 64 - oldBufferBits;
                buffer = offset >>> loBitsStored;
                bufferBits = bitWidth - loBitsStored;
            }
        }

        if (bufferBits > 0) {
            Unsafe.getUnsafe().putByte(destAddr + destOffset, (byte) buffer);
        }
    }

    /**
     * Unpacks bit-packed data to native int64 array.
     * Both source and destination are native memory addresses.
     *
     * @param srcAddr    packed data address
     * @param valueCount values to unpack
     * @param bitWidth   bits per value
     * @param minValue   reference frame to add back
     * @param destAddr   destination native address for int64 values
     */
    public static void unpackAllValuesNative(long srcAddr, int valueCount, int bitWidth,
                                             long minValue, long destAddr) {
        validateBitWidth(bitWidth);
        if (NATIVE_AVAILABLE) {
            unpackAllValues0(srcAddr, valueCount, bitWidth, minValue, destAddr);
        } else {
            unpackAllValuesNativeFallback(srcAddr, valueCount, bitWidth, minValue, destAddr);
        }
    }

    public static void unpackAllValuesNativeFallback(long srcAddr, int valueCount, int bitWidth,
                                                     long minValue, long destAddr) {
        long buffer = 0;
        int bufferBits = 0;
        int srcOffset = 0;
        long mask = (bitWidth == 64) ? -1L : (1L << bitWidth) - 1;
        int totalBytes = BitpackUtils.packedDataSize(valueCount, bitWidth);

        for (int i = 0; i < valueCount; i++) {
            while (bufferBits < bitWidth && srcOffset < totalBytes) {
                long b = Unsafe.getUnsafe().getByte(srcAddr + srcOffset) & 0xFFL;
                buffer |= (b << bufferBits);
                bufferBits += 8;
                srcOffset++;
            }
            Unsafe.getUnsafe().putLong(destAddr + (long) i * Long.BYTES, minValue + (buffer & mask));
            // Java's >>> uses only the low 6 bits of the shift count, so
            // buffer >>>= 64 is a no-op. Zero the buffer explicitly when
            // the full 64 bits were consumed; otherwise leftover high bits
            // would OR into the next value.
            if (bitWidth == 64) {
                buffer = 0;
            } else {
                buffer >>>= bitWidth;
            }
            bufferBits -= bitWidth;
        }
    }

    /**
     * Unpacks values from bit-packed data starting at an arbitrary index,
     * writing directly into a Java long[] array via GetPrimitiveArrayCritical
     * (zero-copy). Uses AVX2 for byte-aligned widths (8/16/32-bit).
     */
    public static void unpackValuesFrom(long srcAddr, int startIndex, int valueCount,
                                        int bitWidth, long minValue, long destAddr) {
        validateBitWidth(bitWidth);
        if (NATIVE_AVAILABLE) {
            unpackValuesFrom0(srcAddr, startIndex, valueCount, bitWidth, minValue, destAddr);
        } else {
            throw new UnsupportedOperationException("native library not available for unpackValuesFrom");
        }
    }

    private static void validateBitWidth(int bitWidth) {
        if (bitWidth < 1 || bitWidth > 64) {
            throw new IllegalArgumentException("bitWidth out of range [1, 64]: " + bitWidth);
        }
    }

    private static native void packValues0(long valuesAddr, int count, long minValue,
                                           int bitWidth, long destAddr);

    private static native void unpackAllValues0(long srcAddr, int valueCount, int bitWidth,
                                                long minValue, long destAddr);

    private static native void unpackValuesFrom0(long srcAddr, int startIndex, int valueCount,
                                                 int bitWidth, long minValue, long destAddr);

    static {
        boolean available;
        try {
            // Trigger Os static init which loads libquestdb
            @SuppressWarnings("unused")
            int osType = Os.type;
            // Probe: count=0 so no memory access
            packValues0(0, 0, 0, 1, 0);
            available = true;
        } catch (UnsatisfiedLinkError e) {
            available = false;
        }
        NATIVE_AVAILABLE = available;
    }
}

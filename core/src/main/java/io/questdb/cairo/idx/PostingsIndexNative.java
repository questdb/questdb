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

import io.questdb.std.Os;
import io.questdb.std.Unsafe;

/**
 * Native SIMD-accelerated bitpacking operations for BP bitmap index.
 * <p>
 * All operations work on native memory addresses — no Java array copies
 * across JNI. AVX2 specializations for 8/16/32-bit aligned widths
 * process 4 values per cycle.
 */
public final class PostingsIndexNative {
    private static final boolean NATIVE_AVAILABLE;

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

    private PostingsIndexNative() {
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
        if (NATIVE_AVAILABLE) {
            packValues0(valuesAddr, count, minValue, bitWidth, destAddr);
        } else {
            packValuesNativeFallback(valuesAddr, count, minValue, bitWidth, destAddr);
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
        if (NATIVE_AVAILABLE) {
            unpackAllValues0(srcAddr, valueCount, bitWidth, minValue, destAddr);
        } else {
            unpackAllValuesNativeFallback(srcAddr, valueCount, bitWidth, minValue, destAddr);
        }
    }

    /**
     * Unpacks bit-packed data to a Java long array. For the hot read path
     * (cursor decode), avoids intermediate native buffer by using Java fallback
     * for arbitrary widths and native SIMD only for aligned widths where the
     * gain outweighs copy cost.
     */
    public static void unpackAllValues(long srcAddr, int valueCount, int bitWidth,
                                        long minValue, long[] dest) {
        FORBitmapIndexUtils.unpackAllValues(srcAddr, valueCount, bitWidth, minValue, dest);
    }

    private static void packValuesNativeFallback(long valuesAddr, int count, long minValue,
                                                  int bitWidth, long destAddr) {
        long buffer = 0;
        int bufferBits = 0;
        int destOffset = 0;

        for (int i = 0; i < count; i++) {
            long offset = Unsafe.getUnsafe().getLong(valuesAddr + (long) i * Long.BYTES) - minValue;
            buffer |= (offset << bufferBits);
            bufferBits += bitWidth;

            while (bufferBits >= 8) {
                Unsafe.getUnsafe().putByte(destAddr + destOffset, (byte) buffer);
                buffer >>>= 8;
                bufferBits -= 8;
                destOffset++;
            }
        }

        if (bufferBits > 0) {
            Unsafe.getUnsafe().putByte(destAddr + destOffset, (byte) buffer);
        }
    }

    private static void unpackAllValuesNativeFallback(long srcAddr, int valueCount, int bitWidth,
                                                       long minValue, long destAddr) {
        long buffer = 0;
        int bufferBits = 0;
        int srcOffset = 0;
        long mask = (bitWidth == 64) ? -1L : (1L << bitWidth) - 1;

        for (int i = 0; i < valueCount; i++) {
            while (bufferBits < bitWidth) {
                long b = Unsafe.getUnsafe().getByte(srcAddr + srcOffset) & 0xFFL;
                buffer |= (b << bufferBits);
                bufferBits += 8;
                srcOffset++;
            }
            Unsafe.getUnsafe().putLong(destAddr + (long) i * Long.BYTES, minValue + (buffer & mask));
            buffer >>>= bitWidth;
            bufferBits -= bitWidth;
        }
    }

    private static native void packValues0(long valuesAddr, int count, long minValue,
                                            int bitWidth, long destAddr);

    private static native void unpackAllValues0(long srcAddr, int valueCount, int bitWidth,
                                                 long minValue, long destAddr);
}

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

package io.questdb.griffin.engine.orderby;

import io.questdb.std.Unsafe;

/**
 * Static utility methods for encoding sort key columns into byte-comparable
 * (memcmp-safe) representations. All multi-byte values are written big-endian
 * with sign bits flipped so that unsigned byte comparison yields the correct
 * signed ordering.
 * <p>
 * For DESC columns, the caller flips all bytes of the encoded column segment
 * with {@link #flipBytes(long, int)}.
 */
public final class SortKeyEncoding {

    private SortKeyEncoding() {
    }

    public static void encodeBoolean(long addr, boolean value) {
        Unsafe.getUnsafe().putByte(addr, value ? (byte) 1 : (byte) 0);
    }

    public static void encodeByte(long addr, byte value) {
        Unsafe.getUnsafe().putByte(addr, (byte) (value ^ 0x80));
    }

    public static void encodeChar(long addr, char value) {
        Unsafe.getUnsafe().putShort(addr, Short.reverseBytes((short) value));
    }

    public static void encodeDouble(long addr, double value) {
        long bits = Double.doubleToRawLongBits(value);
        // IEEE 754 → memcmp-safe: if positive, flip sign bit;
        // if negative, flip all bits
        bits = bits >= 0 ? bits ^ Long.MIN_VALUE : ~bits;
        Unsafe.getUnsafe().putLong(addr, Long.reverseBytes(bits));
    }

    public static void encodeFloat(long addr, float value) {
        int bits = Float.floatToRawIntBits(value);
        bits = bits >= 0 ? bits ^ Integer.MIN_VALUE : ~bits;
        Unsafe.getUnsafe().putInt(addr, Integer.reverseBytes(bits));
    }

    public static void encodeInt(long addr, int value) {
        Unsafe.getUnsafe().putInt(addr, Integer.reverseBytes(value ^ 0x80000000));
    }

    public static void encodeLong(long addr, long value) {
        Unsafe.getUnsafe().putLong(addr, Long.reverseBytes(value ^ Long.MIN_VALUE));
    }

    public static void encodeUnsignedLong(long addr, long value) {
        Unsafe.getUnsafe().putLong(addr, Long.reverseBytes(value));
    }

    public static void encodeShort(long addr, short value) {
        Unsafe.getUnsafe().putShort(addr, Short.reverseBytes((short) (value ^ 0x8000)));
    }

    /**
     * Encodes an unsigned rank value in big-endian format using the minimum
     * number of bytes needed for the symbol table size.
     *
     * @param addr      destination address
     * @param rank      unsigned rank (0-based)
     * @param byteWidth 1, 2, or 4
     */
    public static void encodeUnsignedRank(long addr, int rank, int byteWidth) {
        switch (byteWidth) {
            case 1 -> Unsafe.getUnsafe().putByte(addr, (byte) rank);
            case 2 -> Unsafe.getUnsafe().putShort(addr, Short.reverseBytes((short) rank));
            default -> Unsafe.getUnsafe().putInt(addr, Integer.reverseBytes(rank));
        }
    }

    /**
     * Flips all bytes in the given range (XOR 0xFF). Used for DESC encoding.
     */
    public static void flipBytes(long addr, int length) {
        int i = 0;
        // flip 8 bytes at a time
        for (; i + 8 <= length; i += 8) {
            long v = Unsafe.getUnsafe().getLong(addr + i);
            Unsafe.getUnsafe().putLong(addr + i, ~v);
        }
        // flip remaining bytes
        for (; i < length; i++) {
            byte v = Unsafe.getUnsafe().getByte(addr + i);
            Unsafe.getUnsafe().putByte(addr + i, (byte) ~v);
        }
    }

}

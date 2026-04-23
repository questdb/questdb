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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.Unsafe;

/**
 * Utility class for bit manipulation with LSB-first bit ordering.
 * <p>
 * Bits are packed into bytes starting from the least significant bit (bit 0)
 * to the most significant bit (bit 7) within each byte.
 */
public final class QwpBooleanDecoder {

    private QwpBooleanDecoder() {
    }

    /**
     * Gets a bit from a bit-packed array (direct memory, LSB first within each byte).
     *
     * @param address  bitmap start address
     * @param bitIndex bit index to check
     * @return true if the bit is set
     */
    public static boolean getBit(long address, int bitIndex) {
        int byteIndex = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        byte b = Unsafe.getUnsafe().getByte(address + byteIndex);
        return (b & (1 << bitOffset)) != 0;
    }

    /**
     * Gets a bit from a bit-packed array (byte array, LSB first within each byte).
     *
     * @param bitmap   bitmap byte array
     * @param offset   starting offset in array
     * @param bitIndex bit index to check
     * @return true if the bit is set
     */
    public static boolean getBit(byte[] bitmap, int offset, int bitIndex) {
        int byteIndex = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        byte b = bitmap[offset + byteIndex];
        return (b & (1 << bitOffset)) != 0;
    }

    /**
     * Sets a bit in a bit-packed array (direct memory, LSB first within each byte).
     *
     * @param address  bitmap start address
     * @param bitIndex bit index to set
     */
    public static void setBit(long address, int bitIndex) {
        int byteIndex = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        long addr = address + byteIndex;
        byte b = Unsafe.getUnsafe().getByte(addr);
        b |= (byte) (1 << bitOffset);
        Unsafe.getUnsafe().putByte(addr, b);
    }

    /**
     * Sets a bit in a bit-packed array (byte array, LSB first within each byte).
     *
     * @param bitmap   bitmap byte array
     * @param offset   starting offset in array
     * @param bitIndex bit index to set
     */
    public static void setBit(byte[] bitmap, int offset, int bitIndex) {
        int byteIndex = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        bitmap[offset + byteIndex] |= (byte) (1 << bitOffset);
    }
}

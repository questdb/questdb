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
 * Utility class for reading and writing null bitmaps in QWP v1 format.
 * <p>
 * Null bitmap format:
 * <ul>
 *   <li>Size: ceil(rowCount / 8) bytes</li>
 *   <li>bit[i] = 1 means row[i] is NULL</li>
 *   <li>Bit order: LSB first within each byte</li>
 * </ul>
 * <p>
 * Example: For 10 rows where rows 0, 2, 9 are null:
 * <pre>
 * Byte 0: 0b00000101 (bits 0,2 set)
 * Byte 1: 0b00000010 (bit 1 set, which is row 9)
 * </pre>
 */
public final class QwpNullBitmap {

    private QwpNullBitmap() {
        // utility class
    }

    /**
     * Counts the number of null values in the bitmap.
     *
     * @param address  bitmap start address
     * @param rowCount total number of rows
     * @return count of null values
     */
    public static int countNulls(long address, int rowCount) {
        int count = 0;
        int fullBytes = rowCount >>> 3;
        int fullLongs = fullBytes >>> 3;
        int tailBytes = fullBytes & 7;
        int remainingBits = rowCount & 7;

        // Count 8 bytes at a time
        long addr = address;
        for (int i = 0; i < fullLongs; i++) {
            count += Long.bitCount(Unsafe.getUnsafe().getLong(addr));
            addr += 8;
        }

        // Count remaining full bytes
        for (int i = 0; i < tailBytes; i++) {
            count += Integer.bitCount(Unsafe.getUnsafe().getByte(addr + i) & 0xFF);
        }

        // Count remaining bits in last partial byte
        if (remainingBits > 0) {
            byte b = Unsafe.getUnsafe().getByte(address + fullBytes);
            int mask = (1 << remainingBits) - 1;
            count += Integer.bitCount((b & mask) & 0xFF);
        }

        return count;
    }

    /**
     * Checks if a specific row is null in the bitmap (from direct memory).
     *
     * @param address  bitmap start address
     * @param rowIndex row index to check
     * @return true if the row is null
     */
    public static boolean isNull(long address, int rowIndex) {
        return QwpBooleanDecoder.getBit(address, rowIndex);
    }

    /**
     * Calculates the size in bytes needed for a null bitmap.
     *
     * @param rowCount number of rows
     * @return bitmap size in bytes
     */
    public static int sizeInBytes(int rowCount) {
        return (rowCount + 7) >>> 3;
    }
}

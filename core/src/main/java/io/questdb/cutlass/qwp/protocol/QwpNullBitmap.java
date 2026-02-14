/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
 * Utility class for reading and writing null bitmaps in ILP v4 format.
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
     * Calculates the size in bytes needed for a null bitmap.
     *
     * @param rowCount number of rows
     * @return bitmap size in bytes
     */
    public static int sizeInBytes(long rowCount) {
        return (int) ((rowCount + 7) / 8);
    }

    /**
     * Checks if a specific row is null in the bitmap (from direct memory).
     *
     * @param address  bitmap start address
     * @param rowIndex row index to check
     * @return true if the row is null
     */
    public static boolean isNull(long address, int rowIndex) {
        int byteIndex = rowIndex >>> 3; // rowIndex / 8
        int bitIndex = rowIndex & 7;    // rowIndex % 8
        byte b = Unsafe.getUnsafe().getByte(address + byteIndex);
        return (b & (1 << bitIndex)) != 0;
    }

    /**
     * Checks if a specific row is null in the bitmap (from byte array).
     *
     * @param bitmap   bitmap byte array
     * @param offset   starting offset in array
     * @param rowIndex row index to check
     * @return true if the row is null
     */
    public static boolean isNull(byte[] bitmap, int offset, int rowIndex) {
        int byteIndex = rowIndex >>> 3;
        int bitIndex = rowIndex & 7;
        byte b = bitmap[offset + byteIndex];
        return (b & (1 << bitIndex)) != 0;
    }

    /**
     * Sets a row as null in the bitmap (direct memory).
     *
     * @param address  bitmap start address
     * @param rowIndex row index to set as null
     */
    public static void setNull(long address, int rowIndex) {
        int byteIndex = rowIndex >>> 3;
        int bitIndex = rowIndex & 7;
        long addr = address + byteIndex;
        byte b = Unsafe.getUnsafe().getByte(addr);
        b |= (1 << bitIndex);
        Unsafe.getUnsafe().putByte(addr, b);
    }

    /**
     * Sets a row as null in the bitmap (byte array).
     *
     * @param bitmap   bitmap byte array
     * @param offset   starting offset in array
     * @param rowIndex row index to set as null
     */
    public static void setNull(byte[] bitmap, int offset, int rowIndex) {
        int byteIndex = rowIndex >>> 3;
        int bitIndex = rowIndex & 7;
        bitmap[offset + byteIndex] |= (1 << bitIndex);
    }

    /**
     * Clears a row's null flag in the bitmap (direct memory).
     *
     * @param address  bitmap start address
     * @param rowIndex row index to clear
     */
    public static void clearNull(long address, int rowIndex) {
        int byteIndex = rowIndex >>> 3;
        int bitIndex = rowIndex & 7;
        long addr = address + byteIndex;
        byte b = Unsafe.getUnsafe().getByte(addr);
        b &= ~(1 << bitIndex);
        Unsafe.getUnsafe().putByte(addr, b);
    }

    /**
     * Clears a row's null flag in the bitmap (byte array).
     *
     * @param bitmap   bitmap byte array
     * @param offset   starting offset in array
     * @param rowIndex row index to clear
     */
    public static void clearNull(byte[] bitmap, int offset, int rowIndex) {
        int byteIndex = rowIndex >>> 3;
        int bitIndex = rowIndex & 7;
        bitmap[offset + byteIndex] &= ~(1 << bitIndex);
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
        int remainingBits = rowCount & 7;

        // Count full bytes
        for (int i = 0; i < fullBytes; i++) {
            byte b = Unsafe.getUnsafe().getByte(address + i);
            count += Integer.bitCount(b & 0xFF);
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
     * Counts the number of null values in the bitmap (byte array).
     *
     * @param bitmap   bitmap byte array
     * @param offset   starting offset in array
     * @param rowCount total number of rows
     * @return count of null values
     */
    public static int countNulls(byte[] bitmap, int offset, int rowCount) {
        int count = 0;
        int fullBytes = rowCount >>> 3;
        int remainingBits = rowCount & 7;

        for (int i = 0; i < fullBytes; i++) {
            count += Integer.bitCount(bitmap[offset + i] & 0xFF);
        }

        if (remainingBits > 0) {
            byte b = bitmap[offset + fullBytes];
            int mask = (1 << remainingBits) - 1;
            count += Integer.bitCount((b & mask) & 0xFF);
        }

        return count;
    }

    /**
     * Checks if all rows are null.
     *
     * @param address  bitmap start address
     * @param rowCount total number of rows
     * @return true if all rows are null
     */
    public static boolean allNull(long address, int rowCount) {
        int fullBytes = rowCount >>> 3;
        int remainingBits = rowCount & 7;

        // Check full bytes (all bits should be 1)
        for (int i = 0; i < fullBytes; i++) {
            byte b = Unsafe.getUnsafe().getByte(address + i);
            if ((b & 0xFF) != 0xFF) {
                return false;
            }
        }

        // Check remaining bits
        if (remainingBits > 0) {
            byte b = Unsafe.getUnsafe().getByte(address + fullBytes);
            int mask = (1 << remainingBits) - 1;
            if ((b & mask) != mask) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if no rows are null.
     *
     * @param address  bitmap start address
     * @param rowCount total number of rows
     * @return true if no rows are null
     */
    public static boolean noneNull(long address, int rowCount) {
        int fullBytes = rowCount >>> 3;
        int remainingBits = rowCount & 7;

        // Check full bytes
        for (int i = 0; i < fullBytes; i++) {
            byte b = Unsafe.getUnsafe().getByte(address + i);
            if (b != 0) {
                return false;
            }
        }

        // Check remaining bits
        if (remainingBits > 0) {
            byte b = Unsafe.getUnsafe().getByte(address + fullBytes);
            int mask = (1 << remainingBits) - 1;
            if ((b & mask) != 0) {
                return false;
            }
        }

        return true;
    }

    /**
     * Fills the bitmap setting all rows as null (direct memory).
     *
     * @param address  bitmap start address
     * @param rowCount total number of rows
     */
    public static void fillAllNull(long address, int rowCount) {
        int fullBytes = rowCount >>> 3;
        int remainingBits = rowCount & 7;

        // Fill full bytes with all 1s
        for (int i = 0; i < fullBytes; i++) {
            Unsafe.getUnsafe().putByte(address + i, (byte) 0xFF);
        }

        // Set remaining bits in last byte
        if (remainingBits > 0) {
            byte mask = (byte) ((1 << remainingBits) - 1);
            Unsafe.getUnsafe().putByte(address + fullBytes, mask);
        }
    }

    /**
     * Clears the bitmap setting all rows as non-null (direct memory).
     *
     * @param address  bitmap start address
     * @param rowCount total number of rows
     */
    public static void fillNoneNull(long address, int rowCount) {
        int sizeBytes = sizeInBytes(rowCount);
        for (int i = 0; i < sizeBytes; i++) {
            Unsafe.getUnsafe().putByte(address + i, (byte) 0);
        }
    }

    /**
     * Clears the bitmap setting all rows as non-null (byte array).
     *
     * @param bitmap   bitmap byte array
     * @param offset   starting offset in array
     * @param rowCount total number of rows
     */
    public static void fillNoneNull(byte[] bitmap, int offset, int rowCount) {
        int sizeBytes = sizeInBytes(rowCount);
        for (int i = 0; i < sizeBytes; i++) {
            bitmap[offset + i] = 0;
        }
    }
}

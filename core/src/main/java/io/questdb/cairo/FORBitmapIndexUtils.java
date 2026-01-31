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

package io.questdb.cairo;

import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Constants and encoding/decoding utilities for Frame of Reference (FOR) bitmap index.
 * <p>
 * FOR compression stores row IDs in fixed-size blocks. Each block contains:
 * - A minimum value (reference frame)
 * - A bit-width for offsets
 * - Bit-packed offsets from the minimum
 * <p>
 * This achieves excellent compression for sequential/near-sequential data while
 * enabling fast SIMD-friendly decoding.
 * <p>
 * Block Layout (BLOCK_CAPACITY values per block):
 * <pre>
 * [minValue: 8B][bitWidth: 1B][valueCount: 2B][padding: 1B][packed offsets...]
 * </pre>
 * <p>
 * Key File Layout (.fk):
 * <pre>
 * [Header 64B: sig, seq, valMemSize, keyCount, seqCheck, maxVal]
 * [Key Entry 0: valueCount(8), firstBlockOffset(8), lastValue(8), blockCount(4), countCheck(4)]
 * [Key Entry 1: ...]
 * </pre>
 * <p>
 * Value File Layout (.fv):
 * <pre>
 * [Block 0 for Key 0: header + packed data]
 * [Block 1 for Key 0: ...]
 * [Block 0 for Key 1: ...]
 * </pre>
 */
public final class FORBitmapIndexUtils {

    // Block configuration
    public static final int BLOCK_CAPACITY = 128;  // Values per block (power of 2 for fast division)
    public static final int BLOCK_HEADER_SIZE = 12; // minValue(8) + bitWidth(1) + valueCount(2) + padding(1)
    public static final int BLOCK_OFFSET_BIT_WIDTH = 8;    // 1 byte
    public static final int BLOCK_OFFSET_DATA = 12;        // Packed data starts here
    // Block header offsets
    public static final int BLOCK_OFFSET_MIN_VALUE = 0;    // 8 bytes
    public static final int BLOCK_OFFSET_VALUE_COUNT = 9;  // 2 bytes
    public static final int KEY_ENTRY_OFFSET_BLOCK_COUNT = 24;     // 4 bytes - number of blocks
    public static final int KEY_ENTRY_OFFSET_COUNT_CHECK = 28;     // 4 bytes - consistency check
    public static final int KEY_ENTRY_OFFSET_FIRST_BLOCK = 8;      // 8 bytes - offset to first block in value file
    public static final int KEY_ENTRY_OFFSET_LAST_VALUE = 16;      // 8 bytes - last value added (for O(1) append check)
    // Key entry layout (32 bytes per key, 8-byte aligned)
    public static final int KEY_ENTRY_OFFSET_VALUE_COUNT = 0;      // 8 bytes - total values for this key
    public static final int KEY_ENTRY_SIZE = 32;

    // Key file header layout (64 bytes reserved, 8-byte aligned)
    public static final int KEY_FILE_RESERVED = 64;
    public static final int KEY_RESERVED_OFFSET_KEY_COUNT = 24;       // 8 bytes (4 byte count + 4 padding)
    public static final int KEY_RESERVED_OFFSET_MAX_VALUE = 40;       // 8 bytes
    public static final int KEY_RESERVED_OFFSET_SEQUENCE = 8;         // 8 bytes
    public static final int KEY_RESERVED_OFFSET_SEQUENCE_CHECK = 32;  // 8 bytes
    public static final int KEY_RESERVED_OFFSET_SIGNATURE = 0;        // 8 bytes (1 byte sig + 7 padding)
    public static final int KEY_RESERVED_OFFSET_VALUE_MEM_SIZE = 16;  // 8 bytes
    // Bytes 48-63 reserved for future use
    // Signature for FOR-encoded index (distinct from legacy 0xfa and delta 0xfc)
    public static final byte SIGNATURE = (byte) 0xfd;

    /**
     * Calculates the number of bits needed to represent a value.
     */
    public static int bitsNeeded(long value) {
        if (value == 0) {
            return 1; // Need at least 1 bit
        }
        return 64 - Long.numberOfLeadingZeros(value);
    }

    /**
     * Calculates the total block size in bytes.
     */
    public static int blockSize(int valueCount, int bitWidth) {
        return BLOCK_HEADER_SIZE + packedDataSize(valueCount, bitWidth);
    }

    /**
     * Returns the offset of a key entry in the key file.
     */
    public static long getKeyEntryOffset(int key) {
        return (long) key * KEY_ENTRY_SIZE + KEY_FILE_RESERVED;
    }

    /**
     * Generates the key file name for a FOR bitmap index.
     */
    public static LPSZ keyFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".fk");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    /**
     * Packs values into bit-packed format using Unsafe.
     * Values are stored as offsets from minValue.
     *
     * @param values   array of values to pack
     * @param count    number of values
     * @param minValue minimum value (reference frame)
     * @param bitWidth bits per offset
     * @param destAddr destination memory address
     */
    public static void packValues(long[] values, int count, long minValue, int bitWidth, long destAddr) {
        long buffer = 0;
        int bufferBits = 0;
        int destOffset = 0;

        for (int i = 0; i < count; i++) {
            long offset = values[i] - minValue;
            buffer |= (offset << bufferBits);
            bufferBits += bitWidth;

            // Write complete bytes
            while (bufferBits >= 8) {
                Unsafe.getUnsafe().putByte(destAddr + destOffset, (byte) buffer);
                buffer >>>= 8;
                bufferBits -= 8;
                destOffset++;
            }
        }

        // Write remaining bits
        if (bufferBits > 0) {
            Unsafe.getUnsafe().putByte(destAddr + destOffset, (byte) buffer);
        }
    }

    /**
     * Calculates the packed data size in bytes for a block.
     */
    public static int packedDataSize(int valueCount, int bitWidth) {
        return (valueCount * bitWidth + 7) / 8;
    }

    /**
     * Unpacks all values from a block into an array.
     *
     * @param srcAddr    source memory address of packed data
     * @param valueCount number of values to unpack
     * @param bitWidth   bits per offset
     * @param minValue   minimum value to add back
     * @param dest       destination array
     */
    public static void unpackAllValues(long srcAddr, int valueCount, int bitWidth, long minValue, long[] dest) {
        long buffer = 0;
        int bufferBits = 0;
        int srcOffset = 0;
        long mask = (1L << bitWidth) - 1;
        if (bitWidth == 64) {
            mask = -1L;
        }

        for (int i = 0; i < valueCount; i++) {
            // Ensure we have enough bits in buffer
            while (bufferBits < bitWidth) {
                long b = Unsafe.getUnsafe().getByte(srcAddr + srcOffset) & 0xFFL;
                buffer |= (b << bufferBits);
                bufferBits += 8;
                srcOffset++;
            }

            // Extract offset and add minValue
            long offset = buffer & mask;
            dest[i] = minValue + offset;

            // Remove used bits
            buffer >>>= bitWidth;
            bufferBits -= bitWidth;
        }
    }

    /**
     * Unpacks a single value from bit-packed data using Unsafe.
     *
     * @param srcAddr  source memory address of packed data
     * @param index    index of value to unpack (0-based)
     * @param bitWidth bits per offset
     * @param minValue minimum value to add back
     * @return unpacked value
     */
    public static long unpackValue(long srcAddr, int index, int bitWidth, long minValue) {
        long bitOffset = (long) index * bitWidth;
        int byteOffset = (int) (bitOffset / 8);
        int bitShift = (int) (bitOffset % 8);

        // Read enough bytes to cover the value
        long value = 0;
        int bitsRead = 0;
        int bytesNeeded = (bitShift + bitWidth + 7) / 8;

        for (int i = 0; i < bytesNeeded; i++) {
            long b = Unsafe.getUnsafe().getByte(srcAddr + byteOffset + i) & 0xFFL;
            value |= (b << bitsRead);
            bitsRead += 8;
        }

        // Shift and mask to get the offset
        value >>>= bitShift;
        long mask = (1L << bitWidth) - 1;
        if (bitWidth == 64) {
            mask = -1L; // All bits set
        }
        long offset = value & mask;

        return minValue + offset;
    }

    /**
     * Generates the value file name for a FOR bitmap index.
     */
    public static LPSZ valueFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".fv");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }
}

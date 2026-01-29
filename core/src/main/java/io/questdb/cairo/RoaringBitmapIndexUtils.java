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

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Roaring Bitmap Index utilities and constants.
 *
 * File format:
 *
 * KEY FILE (.rk):
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Header (64 bytes)                                           │
 * │   0: signature (1 byte) = 0xfb                              │
 * │   1: sequence (8 bytes)                                     │
 * │   9: value file size (8 bytes)                              │
 * │  17: unused (4 bytes)                                       │
 * │  21: key count (4 bytes)                                    │
 * │  25: unused (4 bytes)                                       │
 * │  29: sequence check (8 bytes)                               │
 * │  37: max value (8 bytes)                                    │
 * │  45: reserved (19 bytes)                                    │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Key Entry 0 (32 bytes)                                      │
 * │   0: total value count (8 bytes)                            │
 * │   8: chunk directory offset in value file (8 bytes)         │
 * │  16: chunk count (4 bytes)                                  │
 * │  20: unused (4 bytes)                                       │
 * │  24: count check (8 bytes)                                  │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Key Entry 1 ...                                             │
 * └─────────────────────────────────────────────────────────────┘
 *
 * VALUE FILE (.rv):
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Chunk Directory for Key 0                                   │
 * │   Per chunk (16 bytes):                                     │
 * │     0: chunk ID (2 bytes) - high 16 bits of row IDs         │
 * │     2: container type (1 byte) - 0=array, 1=bitmap, 2=run   │
 * │     3: flags (1 byte)                                       │
 * │     4: cardinality (4 bytes)                                │
 * │     8: container offset (8 bytes)                           │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Containers (variable size)                                  │
 * │   Array: cardinality * 2 bytes (sorted shorts)              │
 * │   Bitmap: 8192 bytes fixed (65536 bits)                     │
 * │   Run: runCount * 4 bytes + 2 bytes header                  │
 * └─────────────────────────────────────────────────────────────┘
 */
public final class RoaringBitmapIndexUtils {

    // Signature for roaring index (different from legacy 0xfa)
    public static final byte SIGNATURE = (byte) 0xfb;

    // Key file header layout (64 bytes)
    public static final int KEY_FILE_RESERVED = 64;
    public static final int KEY_RESERVED_OFFSET_SIGNATURE = 0;
    public static final int KEY_RESERVED_OFFSET_SEQUENCE = 1;
    public static final int KEY_RESERVED_OFFSET_VALUE_MEM_SIZE = 9;
    public static final int KEY_RESERVED_OFFSET_KEY_COUNT = 21;
    public static final int KEY_RESERVED_OFFSET_SEQUENCE_CHECK = 29;
    public static final int KEY_RESERVED_OFFSET_MAX_VALUE = 37;

    // Key entry layout (32 bytes per key)
    public static final int KEY_ENTRY_SIZE = 32;
    public static final int KEY_ENTRY_OFFSET_VALUE_COUNT = 0;
    public static final int KEY_ENTRY_OFFSET_CHUNK_DIR_OFFSET = 8;
    public static final int KEY_ENTRY_OFFSET_CHUNK_COUNT = 16;
    public static final int KEY_ENTRY_OFFSET_COUNT_CHECK = 24;

    // Chunk directory entry layout (16 bytes per chunk)
    public static final int CHUNK_DIR_ENTRY_SIZE = 16;
    public static final int CHUNK_DIR_OFFSET_CHUNK_ID = 0;
    public static final int CHUNK_DIR_OFFSET_CONTAINER_TYPE = 2;
    public static final int CHUNK_DIR_OFFSET_FLAGS = 3;
    public static final int CHUNK_DIR_OFFSET_CARDINALITY = 4;
    public static final int CHUNK_DIR_OFFSET_CONTAINER_OFFSET = 8;

    // Container types
    public static final byte CONTAINER_TYPE_ARRAY = 0;
    public static final byte CONTAINER_TYPE_BITMAP = 1;
    public static final byte CONTAINER_TYPE_RUN = 2;

    // Container sizes
    public static final int BITMAP_CONTAINER_SIZE = 8192;  // 65536 bits = 8KB
    public static final int CHUNK_SIZE = 65536;  // 2^16 values per chunk
    public static final int CHUNK_BITS = 16;
    public static final int CHUNK_MASK = 0xFFFF;

    // Threshold for array -> bitmap promotion
    // At 4096 elements: array = 4096 * 2 = 8KB, bitmap = 8KB
    public static final int ARRAY_TO_BITMAP_THRESHOLD = 4096;

    // Run container header size (stores run count)
    public static final int RUN_CONTAINER_HEADER_SIZE = 2;

    public static long getKeyEntryOffset(int key) {
        return (long) key * KEY_ENTRY_SIZE + KEY_FILE_RESERVED;
    }

    public static int getChunkId(long rowId) {
        return (int) (rowId >>> CHUNK_BITS);
    }

    public static short getOffsetInChunk(long rowId) {
        return (short) (rowId & CHUNK_MASK);
    }

    public static long toRowId(int chunkId, short offset) {
        return ((long) chunkId << CHUNK_BITS) | (offset & 0xFFFFL);
    }

    public static LPSZ keyFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".rk");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    public static LPSZ valueFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".rv");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    /**
     * Binary search in a sorted short array stored in memory.
     * Returns index of value if found, or -(insertionPoint + 1) if not found.
     */
    public static int binarySearch(long baseAddress, int size, short value) {
        int low = 0;
        int high = size - 1;
        int uValue = value & 0xFFFF;  // unsigned comparison

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = Short.toUnsignedInt(getShortAt(baseAddress, mid));

            if (midVal < uValue) {
                low = mid + 1;
            } else if (midVal > uValue) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    /**
     * Find the index of the first value >= target in a sorted short array.
     */
    public static int lowerBound(long baseAddress, int size, short target) {
        int low = 0;
        int high = size;
        int uTarget = target & 0xFFFF;

        while (low < high) {
            int mid = (low + high) >>> 1;
            int midVal = Short.toUnsignedInt(getShortAt(baseAddress, mid));

            if (midVal < uTarget) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /**
     * Find the index of the first value > target in a sorted short array.
     */
    public static int upperBound(long baseAddress, int size, short target) {
        int low = 0;
        int high = size;
        int uTarget = target & 0xFFFF;

        while (low < high) {
            int mid = (low + high) >>> 1;
            int midVal = Short.toUnsignedInt(getShortAt(baseAddress, mid));

            if (midVal <= uTarget) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private static short getShortAt(long baseAddress, int index) {
        return io.questdb.std.Unsafe.getUnsafe().getShort(baseAddress + (long) index * Short.BYTES);
    }
}

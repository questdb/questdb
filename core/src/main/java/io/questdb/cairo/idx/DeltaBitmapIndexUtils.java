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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Constants and encoding/decoding utilities for delta-encoded bitmap index
 * with linked blocks.
 * <p>
 * This format combines the linked-block structure of the legacy bitmap index
 * (allowing O(1) append without relocation) with delta encoding for compression.
 * <p>
 * Delta Encoding Scheme (same as before):
 * <ul>
 *   <li>Range 0-127: 1 byte (0x00-0x7F)</li>
 *   <li>Range 128-16383: 2 bytes (0x80|high6, low8)</li>
 *   <li>Range 16384-536870911: 4 bytes (0xC0|high5, 3 bytes)</li>
 *   <li>Larger: 9 bytes (0xE0, full long)</li>
 * </ul>
 * <p>
 * Key File Layout (.dk):
 * <pre>
 * [Header 64B: sig, seq, valMemSize, blockCapacity, keyCount, seqCheck, maxVal]
 * [Key Entry 0: valueCount(8), firstBlock(8), lastBlock(8), countCheck(4), pad(4)]
 * [Key Entry 1: ...]
 * </pre>
 * <p>
 * Value File Layout (.dv) - Linked Blocks:
 * <pre>
 * [Block: next(8), prev(8), count(4), dataLen(4), firstVal(8), lastVal(8), deltas...]
 * [Block: ...]
 * </pre>
 */
public final class DeltaBitmapIndexUtils {

    // === Key File Header (64 bytes) ===
    public static final int KEY_FILE_RESERVED = 64;
    public static final int KEY_RESERVED_OFFSET_SIGNATURE = 0;        // 1 byte + 7 padding
    public static final int KEY_RESERVED_OFFSET_SEQUENCE = 8;         // 8 bytes
    public static final int KEY_RESERVED_OFFSET_VALUE_MEM_SIZE = 16;  // 8 bytes
    public static final int KEY_RESERVED_OFFSET_BLOCK_CAPACITY = 24;  // 4 bytes
    public static final int KEY_RESERVED_OFFSET_KEY_COUNT = 28;       // 4 bytes
    public static final int KEY_RESERVED_OFFSET_SEQUENCE_CHECK = 32;  // 8 bytes
    public static final int KEY_RESERVED_OFFSET_MAX_VALUE = 40;       // 8 bytes
    // Bytes 48-63 reserved

    // === Key Entry (32 bytes per key) ===
    public static final int KEY_ENTRY_SIZE = 32;
    public static final int KEY_ENTRY_OFFSET_VALUE_COUNT = 0;         // 8 bytes - total values
    public static final int KEY_ENTRY_OFFSET_FIRST_BLOCK = 8;         // 8 bytes - first block offset
    public static final int KEY_ENTRY_OFFSET_LAST_BLOCK = 16;         // 8 bytes - last block offset
    public static final int KEY_ENTRY_OFFSET_COUNT_CHECK = 24;        // 4 bytes - for concurrency
    // 4 bytes padding at offset 28

    // === Block Header (40 bytes) ===
    public static final int BLOCK_HEADER_SIZE = 40;
    public static final int BLOCK_OFFSET_NEXT = 0;                    // 8 bytes - next block (-1 if none)
    public static final int BLOCK_OFFSET_PREV = 8;                    // 8 bytes - prev block (-1 if none)
    public static final int BLOCK_OFFSET_COUNT = 16;                  // 4 bytes - values in this block
    public static final int BLOCK_OFFSET_DATA_LEN = 20;               // 4 bytes - bytes of delta data
    public static final int BLOCK_OFFSET_FIRST_VALUE = 24;            // 8 bytes - base value
    public static final int BLOCK_OFFSET_LAST_VALUE = 32;             // 8 bytes - for delta computation

    // Default block capacity (power of 2 for alignment)
    public static final int DEFAULT_BLOCK_CAPACITY = 256;

    // Signature for delta-encoded linked block index
    public static final byte SIGNATURE = (byte) 0xfd;  // Different from legacy (0xfa) and old delta (0xfc)

    // Maximum delta value
    public static final long MAX_DELTA = 0x00FFFFFFFFFFFFFFL;  // 2^56 - 1

    // Delta encoding prefixes
    private static final int PREFIX_1BYTE_MAX = 0x7F;           // 127
    private static final int PREFIX_2BYTE_MASK = 0x80;          // 10xxxxxx
    private static final int PREFIX_2BYTE_MAX = 0x3FFF;         // 16383
    private static final int PREFIX_4BYTE_MASK = 0xC0;          // 110xxxxx
    private static final int PREFIX_4BYTE_MAX = 0x1FFFFFFF;     // 536870911
    private static final int PREFIX_9BYTE_MARKER = 0xE0;        // 11100000

    /**
     * Returns the data capacity of a block (total size minus header).
     */
    public static int blockDataCapacity(int blockCapacity) {
        return blockCapacity - BLOCK_HEADER_SIZE;
    }

    /**
     * Decodes a delta value from memory at the given offset.
     */
    public static void decodeDelta(MemoryR mem, long offset, long[] result) {
        int firstByte = mem.getByte(offset) & 0xFF;

        if ((firstByte & 0x80) == 0) {
            result[0] = firstByte;
            result[1] = 1;
        } else if ((firstByte & 0xC0) == PREFIX_2BYTE_MASK) {
            int highPart = (firstByte & 0x3F) << 8;
            int lowPart = mem.getByte(offset + 1) & 0xFF;
            result[0] = highPart | lowPart;
            result[1] = 2;
        } else if ((firstByte & 0xE0) == PREFIX_4BYTE_MASK) {
            long val = (firstByte & 0x1FL) << 24;
            val |= (mem.getByte(offset + 1) & 0xFFL) << 16;
            val |= (mem.getByte(offset + 2) & 0xFFL) << 8;
            val |= (mem.getByte(offset + 3) & 0xFFL);
            result[0] = val;
            result[1] = 4;
        } else {
            result[0] = mem.getLong(offset + 1);
            result[1] = 9;
        }
    }

    /**
     * Fast decode using direct memory address (Unsafe).
     * Returns encoded (delta | (bytesConsumed << 56)).
     */
    public static long decodeDeltaUnsafe(long address) {
        int firstByte = Unsafe.getUnsafe().getByte(address) & 0xFF;

        if ((firstByte & 0x80) == 0) {
            return firstByte | (1L << 56);
        } else if ((firstByte & 0xC0) == PREFIX_2BYTE_MASK) {
            int highPart = (firstByte & 0x3F) << 8;
            int lowPart = Unsafe.getUnsafe().getByte(address + 1) & 0xFF;
            return (highPart | lowPart) | (2L << 56);
        } else if ((firstByte & 0xE0) == PREFIX_4BYTE_MASK) {
            long val = (firstByte & 0x1FL) << 24;
            val |= (Unsafe.getUnsafe().getByte(address + 1) & 0xFFL) << 16;
            val |= (Unsafe.getUnsafe().getByte(address + 2) & 0xFFL) << 8;
            val |= (Unsafe.getUnsafe().getByte(address + 3) & 0xFFL);
            return val | (4L << 56);
        } else {
            return Unsafe.getUnsafe().getLong(address + 1) | (9L << 56);
        }
    }

    /**
     * Encodes a delta value and writes it to memory.
     */
    public static void encodeDelta(MemoryA mem, long delta) {
        if (delta < 0) {
            throw CairoException.critical(0)
                    .put("delta index value cannot be negative [delta=").put(delta).put(']');
        }

        if (delta <= PREFIX_1BYTE_MAX) {
            mem.putByte((byte) delta);
            return;
        }

        if (delta <= PREFIX_2BYTE_MAX) {
            mem.putByte((byte) (PREFIX_2BYTE_MASK | (delta >> 8)));
            mem.putByte((byte) (delta & 0xFF));
            return;
        }

        if (delta <= PREFIX_4BYTE_MAX) {
            mem.putByte((byte) (PREFIX_4BYTE_MASK | (delta >> 24)));
            mem.putByte((byte) ((delta >> 16) & 0xFF));
            mem.putByte((byte) ((delta >> 8) & 0xFF));
            mem.putByte((byte) (delta & 0xFF));
            return;
        }

        if (delta > MAX_DELTA) {
            throw CairoException.critical(0)
                    .put("delta index value exceeds maximum [delta=").put(delta)
                    .put(", max=").put(MAX_DELTA).put(']');
        }

        mem.putByte((byte) PREFIX_9BYTE_MARKER);
        mem.putLong(delta);
    }

    /**
     * Calculates the encoded size of a delta value without writing it.
     */
    public static int encodedSize(long delta) {
        if (delta < 0) {
            throw CairoException.critical(0)
                    .put("delta index value cannot be negative [delta=").put(delta).put(']');
        }
        if (delta > MAX_DELTA) {
            throw CairoException.critical(0)
                    .put("delta index value exceeds maximum [delta=").put(delta)
                    .put(", max=").put(MAX_DELTA).put(']');
        }

        if (delta <= PREFIX_1BYTE_MAX) {
            return 1;
        } else if (delta <= PREFIX_2BYTE_MAX) {
            return 2;
        } else if (delta <= PREFIX_4BYTE_MAX) {
            return 4;
        } else {
            return 9;
        }
    }

    /**
     * Extracts bytes consumed from packed decodeDeltaUnsafe result.
     */
    public static int getBytesConsumed(long packed) {
        return (int) (packed >>> 56);
    }

    /**
     * Extracts delta value from packed decodeDeltaUnsafe result.
     */
    public static long getDelta(long packed) {
        return packed & 0x00FFFFFFFFFFFFFFL;
    }

    /**
     * Returns the offset of a key entry in the key file.
     */
    public static long getKeyEntryOffset(int key) {
        return (long) key * KEY_ENTRY_SIZE + KEY_FILE_RESERVED;
    }

    /**
     * Generates the key file name for a delta-encoded bitmap index.
     */
    public static LPSZ keyFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".dk");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    /**
     * Generates the value file name for a delta-encoded bitmap index.
     */
    public static LPSZ valueFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".dv");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }
}

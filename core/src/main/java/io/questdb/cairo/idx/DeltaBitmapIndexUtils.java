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
 * Constants and encoding/decoding utilities for delta-encoded bitmap index.
 * <p>
 * Delta encoding achieves 2-4x compression for sequential row IDs common in time-series data.
 * <p>
 * Delta Encoding Scheme:
 * <ul>
 *   <li>Range 0-127: 1 byte (0x00-0x7F)</li>
 *   <li>Range 128-16383: 2 bytes (0x80|high6, low8)</li>
 *   <li>Range 16384-536870911: 4 bytes (0xC0|high5, 3 bytes)</li>
 *   <li>Larger/negative: 9 bytes (0xE0, full long)</li>
 * </ul>
 * <p>
 * Key File Layout (.dk):
 * <pre>
 * [Header 64B: sig, seq, valMemSize, keyCount, seqCheck, maxVal]
 * [Key Entry 0: valueCount(8), dataOffset(8), dataLen(4), countCheck(4)]
 * [Key Entry 1: ...]
 * </pre>
 * <p>
 * Value File Layout (.dv):
 * <pre>
 * [Key0: firstValue(8), delta1, delta2, ...]
 * [Key1: firstValue(8), delta1, delta2, ...]
 * </pre>
 */
public final class DeltaBitmapIndexUtils {

    public static final int KEY_ENTRY_OFFSET_COUNT_CHECK = 28;   // 4 bytes
    public static final int KEY_ENTRY_OFFSET_DATA_LEN = 24;      // 4 bytes
    public static final int KEY_ENTRY_OFFSET_DATA_OFFSET = 8;    // 8 bytes
    public static final int KEY_ENTRY_OFFSET_LAST_VALUE = 16;    // 8 bytes (for O(1) reopen)
    // Key entry layout (32 bytes per key, 8-byte aligned)
    public static final int KEY_ENTRY_OFFSET_VALUE_COUNT = 0;    // 8 bytes
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
    // Maximum delta value that can be encoded/decoded correctly.
    // The packed decoding format uses upper 8 bits for bytes consumed,
    // leaving 56 bits for the delta value.
    public static final long MAX_DELTA = 0x00FFFFFFFFFFFFFFL;  // 2^56 - 1
    // Signature for delta-encoded index (distinct from legacy 0xfa)
    public static final byte SIGNATURE = (byte) 0xfc;
    // Delta encoding prefixes
    private static final int PREFIX_1BYTE_MAX = 0x7F;           // 127
    private static final int PREFIX_2BYTE_MASK = 0x80;          // 10xxxxxx
    private static final int PREFIX_2BYTE_MAX = 0x3FFF;         // 16383
    private static final int PREFIX_4BYTE_MASK = 0xC0;          // 110xxxxx
    private static final int PREFIX_4BYTE_MAX = 0x1FFFFFFF;     // 536870911
    private static final int PREFIX_9BYTE_MARKER = 0xE0;        // 11100000

    /**
     * Decodes a delta value from memory at the given offset.
     *
     * @param mem    memory to read from
     * @param offset offset to read from
     * @param result array of size 2: [0]=decoded delta, [1]=bytes consumed
     */
    public static void decodeDelta(MemoryR mem, long offset, long[] result) {
        int firstByte = mem.getByte(offset) & 0xFF;

        if ((firstByte & 0x80) == 0) {
            // 1-byte encoding: 0xxxxxxx
            result[0] = firstByte;
            result[1] = 1;
        } else if ((firstByte & 0xC0) == PREFIX_2BYTE_MASK) {
            // 2-byte encoding: 10xxxxxx xxxxxxxx
            int highPart = (firstByte & 0x3F) << 8;
            int lowPart = mem.getByte(offset + 1) & 0xFF;
            result[0] = highPart | lowPart;
            result[1] = 2;
        } else if ((firstByte & 0xE0) == PREFIX_4BYTE_MASK) {
            // 4-byte encoding: 110xxxxx xxxxxxxx xxxxxxxx xxxxxxxx
            long val = (firstByte & 0x1FL) << 24;
            val |= (mem.getByte(offset + 1) & 0xFFL) << 16;
            val |= (mem.getByte(offset + 2) & 0xFFL) << 8;
            val |= (mem.getByte(offset + 3) & 0xFFL);
            result[0] = val;
            result[1] = 4;
        } else {
            // 9-byte encoding: 11100000 + full 8-byte long
            result[0] = mem.getLong(offset + 1);
            result[1] = 9;
        }
    }

    /**
     * Fast decode using direct memory address (Unsafe).
     * Returns encoded (delta | (bytesConsumed << 56)) to avoid array allocation.
     * Use getDelta() and getBytesConsumed() to extract values.
     *
     * @param address direct memory address to read from
     * @return packed result: lower 56 bits = delta, upper 8 bits = bytes consumed
     */
    public static long decodeDeltaUnsafe(long address) {
        int firstByte = Unsafe.getUnsafe().getByte(address) & 0xFF;

        if ((firstByte & 0x80) == 0) {
            // 1-byte encoding: 0xxxxxxx (most common case for sequential data)
            return firstByte | (1L << 56);
        } else if ((firstByte & 0xC0) == PREFIX_2BYTE_MASK) {
            // 2-byte encoding: 10xxxxxx xxxxxxxx
            int highPart = (firstByte & 0x3F) << 8;
            int lowPart = Unsafe.getUnsafe().getByte(address + 1) & 0xFF;
            return (highPart | lowPart) | (2L << 56);
        } else if ((firstByte & 0xE0) == PREFIX_4BYTE_MASK) {
            // 4-byte encoding: 110xxxxx xxxxxxxx xxxxxxxx xxxxxxxx
            long val = (firstByte & 0x1FL) << 24;
            val |= (Unsafe.getUnsafe().getByte(address + 1) & 0xFFL) << 16;
            val |= (Unsafe.getUnsafe().getByte(address + 2) & 0xFFL) << 8;
            val |= (Unsafe.getUnsafe().getByte(address + 3) & 0xFFL);
            return val | (4L << 56);
        } else {
            // 9-byte encoding: 11100000 + full 8-byte long
            return Unsafe.getUnsafe().getLong(address + 1) | (9L << 56);
        }
    }

    /**
     * Encodes a delta value and writes it to memory.
     *
     * @param mem   memory to write to
     * @param delta the delta value to encode (must be >= 0 and <= MAX_DELTA)
     * @return number of bytes written
     * @throws CairoException if delta is negative or exceeds MAX_DELTA
     */
    public static int encodeDelta(MemoryA mem, long delta) {
        if (delta < 0) {
            throw CairoException.critical(0)
                    .put("delta index value cannot be negative [delta=").put(delta).put(']');
        }

        if (delta <= PREFIX_1BYTE_MAX) {
            // 1-byte encoding: 0x00-0x7F
            mem.putByte((byte) delta);
            return 1;
        }

        if (delta <= PREFIX_2BYTE_MAX) {
            // 2-byte encoding: 10xxxxxx xxxxxxxx
            mem.putByte((byte) (PREFIX_2BYTE_MASK | (delta >> 8)));
            mem.putByte((byte) (delta & 0xFF));
            return 2;
        }
        if (delta <= PREFIX_4BYTE_MAX) {
            // 4-byte encoding: 110xxxxx xxxxxxxx xxxxxxxx xxxxxxxx
            mem.putByte((byte) (PREFIX_4BYTE_MASK | (delta >> 24)));
            mem.putByte((byte) ((delta >> 16) & 0xFF));
            mem.putByte((byte) ((delta >> 8) & 0xFF));
            mem.putByte((byte) (delta & 0xFF));
            return 4;
        }

        if (delta > MAX_DELTA) {
            throw CairoException.critical(0)
                    .put("delta index value exceeds maximum [delta=").put(delta)
                    .put(", max=").put(MAX_DELTA).put(']');
        }

        // 9-byte encoding: 11100000 + full 8-byte long
        mem.putByte((byte) PREFIX_9BYTE_MARKER);
        mem.putLong(delta);
        return 9;
    }

    /**
     * Calculates the encoded size of a delta value without writing it.
     *
     * @param delta the delta value (must be >= 0 and <= MAX_DELTA)
     * @return number of bytes required to encode this delta
     * @throws CairoException if delta is negative or exceeds MAX_DELTA
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

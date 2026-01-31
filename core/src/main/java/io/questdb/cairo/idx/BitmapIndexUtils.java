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
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public final class BitmapIndexUtils {
    public static final int KEY_ENTRY_OFFSET_COUNT_CHECK = 24;
    public static final int KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET = 8;
    public static final int KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET = 16;
    public static final int KEY_ENTRY_OFFSET_VALUE_COUNT = 0;
    public static final long KEY_ENTRY_SIZE = 32;
    /**
     * key file header offsets
     */
    public static final int KEY_FILE_RESERVED = 64;
    public static final int KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT = 17;
    public static final int KEY_RESERVED_OFFSET_KEY_COUNT = 21;
    public static final int KEY_RESERVED_OFFSET_SEQUENCE = 1;
    public static final int KEY_RESERVED_OFFSET_SEQUENCE_CHECK = 29;
    public static final int KEY_RESERVED_OFFSET_SIGNATURE = 0;
    public static final int KEY_RESERVED_OFFSET_VALUE_MEM_SIZE = 9;
    public static final byte SIGNATURE = (byte) 0xfa;
    public static final int VALUE_BLOCK_FILE_RESERVED = 16;

    public static long getKeyEntryOffset(int key) {
        return key * KEY_ENTRY_SIZE + KEY_FILE_RESERVED;
    }

    public static LPSZ keyFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".k");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    /**
     * Searches ordered list of long values. Return value is either index behind matching value in the list or
     * index of where values would be inserted in order to maintain ascending order of the list. When list
     * contains duplicate values the index would be behind group of duplicate values. In this example list:
     * <p>
     * 1,1,2,2,2,2,5,5,5
     * <p>
     * when we search for 2, the index would be here:
     * <p>
     * 1,1,2,2,2,2,5,5,5
     * ^
     * <p>
     * Same index will be returned when we search for value of 3.
     * <p>
     * This method is meant to search for the position of a rowid (value), 100% guaranteed to exist, otherwise
     * it means the index is corrupt.
     *
     * @param memory    virtual memory instance
     * @param offset    offset in virtual memory
     * @param cellCount length of the available memory measured in 64-bit cells
     * @param value     value we search of
     * @return index directly behind the searched value or group of values if list contains duplicate values
     * @throws CairoException when the index is corrupted
     */
    public static long searchValueBlock(MemoryR memory, long offset, long cellCount, long value) {
        // when block is "small", we just scan it linearly
        if (cellCount < 64) {
            // this will definitely exit because we had checked that at least the last value is greater than value
            for (long i = offset, limit = memory.size(); i < limit; i += 8) {
                if (memory.getLong(i) > value) {
                    return (i - offset) / 8;
                }
            }
        } else {
            // use binary search on larger block
            long low = 0;
            long high = cellCount - 1;
            long half;
            long pivot;
            do {
                half = (high - low) / 2;
                if (half == 0) {
                    break;
                }
                pivot = memory.getLong(offset + (low + half) * 8);
                if (pivot <= value) {
                    low += half;
                } else {
                    high = low + half;
                }
            } while (true);

            return low + 1;
        }
        throw CairoException.critical(0)
                .put("index is corrupt, rowid not found [offset=").put(offset)
                .put(", cellCount=").put(cellCount)
                .put(", value=").put(value)
                .put(']');
    }

    public static LPSZ valueFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".v");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    /**
     * Seeks first block that contains first value, which is greater or equal to minValue.
     * It starts from first block in the list and proceeds moving right until it finds
     * block with low and high values surrounding minValue.
     * <p>
     * The found block is then binary searched to count values that are all greater or
     * equal than minValue.
     * <p>
     * Unlike seekValueBlockRTL, this method does a boundary check over the valueMem size.
     *
     * @param totalCount            total count of values in all blocks;
     *                              *important note*: initialCount may include blocks that exceed the file boundary
     * @param firstValueBlockOffset offset of first block in linked list
     * @param valueMem              value block memory
     * @param minValue              lower limit for values
     * @param blockValueCountMod    number of values in single block - 1
     * @param seeker                interface that collects results of the search
     */
    static void seekValueBlockLTR(
            long totalCount,
            long firstValueBlockOffset,
            MemoryR valueMem,
            long minValue,
            long blockValueCountMod,
            ValueBlockSeeker seeker
    ) {
        long valueCount = totalCount;
        long valueBlockOffset = firstValueBlockOffset;
        if (firstValueBlockOffset >= valueMem.size()) {
            // The very first block is beyond the memory boundary. Report that we didn't find the value by
            // using the total count of values in the seeker call.
            seeker.seek(totalCount, firstValueBlockOffset);
            return;
        }
        if (valueCount > 0) {
            long cellCount;
            do {
                // check block range by peeking at first and last value
                if (valueCount > blockValueCountMod) {
                    cellCount = blockValueCountMod + 1;
                } else {
                    cellCount = valueCount;
                }
                final long hi = valueMem.getLong(valueBlockOffset + (cellCount - 1) * 8);

                // can we skip this block?
                if (hi < minValue) {
                    valueCount -= cellCount;
                    // do we have next block?
                    if (valueCount > 0) {
                        final long nextBlockOffset = (blockValueCountMod + 1) * 8 + 8;
                        valueBlockOffset = valueMem.getLong(valueBlockOffset + nextBlockOffset);
                        if (valueBlockOffset >= valueMem.size()) {
                            // We've reached the memory boundary. Report that we didn't find the value by
                            // using the total count of values in the seeker call.
                            seeker.seek(totalCount, valueBlockOffset);
                            return;
                        }
                        continue;
                    }
                }
                break;
            } while (true);

            if (valueCount > 0) {
                // do we need to search this block?
                final long lo = valueMem.getLong(valueBlockOffset);
                if (minValue > lo) {
                    // yes, we do
                    valueCount -= searchValueBlock(valueMem, valueBlockOffset, cellCount, minValue - 1);
                }
            }
        }
        seeker.seek(totalCount - valueCount, valueBlockOffset);
    }

    /**
     * Seeks index value block for rows <= maxValue. It starts from append block, which is the last block in
     * the list, and proceeds moving left until it finds block with low and high values surrounding maxValue.
     * This block is then binary searched to count values that are less or equal than maxValue.
     * <p>
     * List of value blocks is assumed to be ordered by value in ascending order.
     *
     * @param valueCount         total count of values in all blocks
     * @param blockOffset        offset of last value block in chain of blocks
     * @param valueMem           value block memory
     * @param maxValue           upper limit for block values
     * @param blockValueCountMod number of values in single block - 1
     * @param seeker             interface that collects results of the search
     */
    static void seekValueBlockRTL(
            long valueCount,
            long blockOffset,
            MemoryR valueMem,
            long maxValue,
            long blockValueCountMod,
            ValueBlockSeeker seeker
    ) {
        long valueBlockOffset = blockOffset;
        if (valueCount > 0) {
            long prevBlockOffset = (blockValueCountMod + 1) * 8;
            long cellCount;
            do {
                // check block range by peeking at first and last value
                long lo = valueMem.getLong(valueBlockOffset);
                cellCount = (valueCount - 1 & blockValueCountMod) + 1;

                // can we skip this block?
                if (lo > maxValue) {
                    valueCount -= cellCount;
                    // do we have previous block?
                    if (valueCount > 0) {
                        valueBlockOffset = valueMem.getLong(valueBlockOffset + prevBlockOffset);
                        continue;
                    }
                }
                break;
            } while (true);

            if (valueCount > 0) {
                // do we need to search this block?
                long hi = valueMem.getLong(valueBlockOffset + (cellCount - 1) * 8);
                if (maxValue < hi) {
                    // yes, we do
                    valueCount -= cellCount - searchValueBlock(valueMem, valueBlockOffset, cellCount, maxValue);
                }
            }
        }
        seeker.seek(valueCount, valueBlockOffset);
    }

    @FunctionalInterface
    interface ValueBlockSeeker {
        void seek(long count, long offset);
    }
}

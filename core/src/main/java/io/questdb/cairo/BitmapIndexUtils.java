/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.str.Path;

public final class BitmapIndexUtils {
    static final long KEY_ENTRY_SIZE = 32;
    static final int KEY_ENTRY_OFFSET_VALUE_COUNT = 0;
    static final int KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET = 16;
    static final int KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET = 8;
    static final int KEY_ENTRY_OFFSET_COUNT_CHECK = 24;

    /**
     * key file header offsets
     */
    static final int KEY_FILE_RESERVED = 64;
    static final int KEY_RESERVED_OFFSET_SIGNATURE = 0;
    static final int KEY_RESERVED_OFFSET_SEQUENCE = 1;
    static final int KEY_RESERVED_OFFSET_VALUE_MEM_SIZE = 9;
    static final int KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT = 17;
    static final int KEY_RESERVED_OFFSET_KEY_COUNT = 21;
    static final int KEY_RESERVED_OFFSET_SEQUENCE_CHECK = 29;

    static final byte SIGNATURE = (byte) 0xfa;
    static final int VALUE_BLOCK_FILE_RESERVED = 16;

    public static Path keyFileName(Path path, CharSequence name) {
        return path.concat(name).put(".k").$();
    }

    public static Path valueFileName(Path path, CharSequence name) {
        return path.concat(name).put(".v").$();
    }

    /**
     * Seeks index value block for rows <= maxValue. It starts from append block, which is the last block in
     * the list, and proceeds moving left until it finds block with low and high values surrounding maxValue.
     * This block is then binary searched to count values that are less or equal than maxValue.
     * <p>
     * List of value blocks is assumed to be ordered by value in ascending order.
     *
     * @param initialCount         total count of values in all blocks.
     * @param lastValueBlockOffset offset of last value block in chain of blocks.
     * @param valueMem             value block memory
     * @param maxValue             upper limit for block values.
     * @param blockValueCountMod   number of values in single block - 1
     * @param seeker               interface that collects results of the search
     */
    static void seekValueBlockRTL(
            long initialCount,
            long lastValueBlockOffset,
            VirtualMemory valueMem,
            long maxValue,
            long blockValueCountMod,
            ValueBlockSeeker seeker
    ) {
        long valueCount = initialCount;
        long valueBlockOffset = lastValueBlockOffset;
        if (valueCount > 0) {
            long prevBlockOffset = (blockValueCountMod + 1) * 8;
            long cellCount;
            do {
                // check block range by peeking at first and last value
                long lo = valueMem.getLong(valueBlockOffset);
                cellCount = (valueCount - 1 & blockValueCountMod) + 1;

                // can we skip this block ?
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

    /**
     * Seeks first block that contains first value, which is greater or equal to minValue.
     * It starts from first block in the list and proceeds moving right until it finds
     * block with low and high values surrounding minValue.
     * This block is then binary searched to count values that are all greater or equal
     * than minValue.
     *
     * @param initialCount          total count of values in all blocks.
     * @param firstValueBlockOffset offset of first block in linked list
     * @param valueMem              value block memory
     * @param minValue              lower limit for values
     * @param blockValueCountMod    number of values in single block - 1
     * @param seeker                interface that collects results of the search
     */
    static void seekValueBlockLTR(
            long initialCount,
            long firstValueBlockOffset,
            VirtualMemory valueMem,
            long minValue,
            long blockValueCountMod,
            ValueBlockSeeker seeker
    ) {
        long valueCount = initialCount;
        long valueBlockOffset = firstValueBlockOffset;
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

                // can we skip this block ?
                if (hi < minValue) {
                    valueCount -= cellCount;
                    // do we have previous block?
                    if (valueCount > 0) {
                        final long nextBlockOffset = (blockValueCountMod + 1) * 8 + 8;
                        valueBlockOffset = valueMem.getLong(valueBlockOffset + nextBlockOffset);
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
        seeker.seek(initialCount - valueCount, valueBlockOffset);
    }


    static long getKeyEntryOffset(int key) {
        return key * KEY_ENTRY_SIZE + KEY_FILE_RESERVED;
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
     *
     * @param memory    virtual memory instance
     * @param offset    offset in virtual memory
     * @param cellCount length of the available memory measured in 64-bit cells
     * @param value     value we search of
     * @return index directly behind the searched value or group of values if list contains duplicate values.
     */
    static long searchValueBlock(VirtualMemory memory, long offset, long cellCount, long value) {
        // when block is "small", we just scan it linearly
        if (cellCount < 64) {
            // this will definitely exit because we had checked that at least the last value is greater than value
            for (long i = offset; ; i += 8) {
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
    }

    @FunctionalInterface
    interface ValueBlockSeeker {
        void seek(long count, long offset);
    }
}

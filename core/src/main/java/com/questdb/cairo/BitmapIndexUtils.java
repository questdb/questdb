/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.std.str.Path;

public final class BitmapIndexUtils {
    static final int KEY_ENTRY_SIZE = 32;
    static final int KEY_ENTRY_OFFSET_VALUE_COUNT = 0;
    static final int KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET = 16;
    //    static final int KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET = 8;
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

    static void seekValueBlock(
            long initialCount,
            long initialOffset,
            VirtualMemory valueMem,
            long maxValue,
            long blockValueCountMod,
            ValueBlockSeeker seeker
    ) {
        long valueCount = initialCount;
        long valueBlockOffset = initialOffset;
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

            // do we need to search this block?
            long hi = valueMem.getLong(valueBlockOffset + (cellCount - 1) * 8);
            if (maxValue < hi) {
                // yes, we do
                valueCount -= cellCount - searchValueBlock(valueBlockOffset, maxValue, cellCount, valueMem);
            }
        }
        seeker.seek(valueCount, valueBlockOffset);
    }

    static void keyFileName(Path path, CharSequence root, CharSequence name) {
        path.of(root).concat(name).put(".k").$();
    }

    static void valueFileName(Path path, CharSequence root, CharSequence name) {
        path.of(root).concat(name).put(".v").$();
    }

    static long getKeyEntryOffset(int key) {
        return key * KEY_ENTRY_SIZE + KEY_FILE_RESERVED;
    }

    static long searchValueBlock(long valueBlockOffset, long maxValue, long cellCount, VirtualMemory valueMem) {
        // when block is "small", we just scan it linearly
        if (cellCount < 64) {
            // this will definitely exit because we had checked that at least the last value is greater that maxValue
            for (long i = valueBlockOffset; ; i += 8) {
                if (valueMem.getLong(i) > maxValue) {
                    return (i - valueBlockOffset) / 8;
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
                pivot = valueMem.getLong(valueBlockOffset + (low + half) * 8);
                if (pivot <= maxValue) {
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

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

#ifndef QUESTDB_BITMAP_INDEX_UTILS_H
#define QUESTDB_BITMAP_INDEX_UTILS_H

#include <utility>
#include "util.h"

struct out_arguments {
    int64_t key_lo;
    int64_t key_hi;
    int64_t rows_address;
    int64_t rows_capacity;
    int64_t rows_size;
};

struct key_header {
   int8_t  signature;
   int64_t sequence;
   int64_t value_mem_size;
   int32_t block_value_count;
   int64_t key_count;
   int64_t sequence_check;
   int8_t  padding[27];
} __attribute__((packed));

struct key_entry {
    int64_t value_count;
    int64_t first_value_block_offset;
    int64_t last_value_block_offset;
    int64_t count_check;
} __attribute__((packed));

struct value_block_link {
    int64_t prev;
    int64_t next;
} __attribute__((packed));


template<typename T>
int64_t searchValueBlock(const T* memory, int64_t offset, int64_t cellCount, T value) {
    // when block is "small", we just scan it linearly
    if (cellCount < 64) {
        // this will definitely exit because we had checked that at least the last value is greater than value
        for (long i = offset; i < offset + cellCount; ++i) {
            if (memory[i] > value) {
                return i - offset;
            }
        }
        return cellCount;
    } else {
        // use binary search on larger block
        return branch_free_search_upper(memory + offset, cellCount, value);
    }
}

template<typename T>
std::pair<int64_t, int64_t> seekValueBlockRTL(int64_t valueCount, int64_t blockOffset, const T *valueMem, T maxValue,
                                              int64_t blockValueCountMod) {
    int64_t valueBlockOffset = blockOffset / sizeof(T);
    if (valueCount > 0) {
        const int64_t prevBlockOffset = (blockValueCountMod + 1);
        int64_t cellCount;
        do {
            // check block range by peeking at first and last value
            const int64_t lo = valueMem[valueBlockOffset];
            cellCount = (valueCount - 1 & blockValueCountMod) + 1;

            // can we skip this block ?
            if (lo > maxValue) {
                valueCount -= cellCount;
                // do we have previous block?
                if (valueCount > 0) {
                    valueBlockOffset = valueMem[valueBlockOffset + prevBlockOffset] / sizeof(T);
                    continue;
                }
            }
            break;
        } while (true);

        if (valueCount > 0) {
            // do we need to search this block?
            const int64_t hi = valueMem[valueBlockOffset + (cellCount - 1)];
            if (maxValue < hi) {
                // yes, we do
                valueCount -= cellCount - searchValueBlock(valueMem, valueBlockOffset, cellCount, maxValue);
            }
        }
    }
    return {valueCount, valueBlockOffset*8};
}

template<typename T>
std::pair<int64_t, int64_t> seekValueBlockLTR(int64_t initialCount, int64_t firstValueBlockOffset, const T *valueMem,
                                              T minValue, int64_t blockValueCountMod) {
    int64_t valueCount = initialCount;
    int64_t valueBlockOffset = firstValueBlockOffset / sizeof(T);
    if (valueCount > 0) {
        int64_t cellCount;
        do {
            // check block range by peeking at first and last value
            if (valueCount > blockValueCountMod) {
                cellCount = blockValueCountMod + 1;
            } else {
                cellCount = valueCount;
            }
            const int64_t hi = valueMem[valueBlockOffset + (cellCount - 1)];

            // can we skip this block ?
            if (hi < minValue) {
                valueCount -= cellCount;
                // do we have previous block?
                if (valueCount > 0) {
                    const int64_t nextBlockOffset = (blockValueCountMod + 1) + 1;
                    valueBlockOffset = valueMem[valueBlockOffset + nextBlockOffset] / sizeof(T);
                    continue;
                }
            }
            break;
        } while (true);

        if (valueCount > 0) {
            // do we need to search this block?
            const int64_t lo = valueMem[valueBlockOffset];
            if (minValue > lo) {
                // yes, we do
                valueCount -= searchValueBlock(valueMem, valueBlockOffset, cellCount, minValue - 1);
            }
        }
    }
    return {initialCount - valueCount, valueBlockOffset*8};
}

#endif //QUESTDB_BITMAP_INDEX_UTILS_H

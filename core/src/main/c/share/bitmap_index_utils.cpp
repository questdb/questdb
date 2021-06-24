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

#include <algorithm>
#include "bitmap_index_utils.h"

void latest_scan_backward(uint64_t keys_memory_addr, size_t keys_memory_size, uint64_t values_memory_addr,
                          size_t value_memory_size, uint64_t args_memory_addr, int64_t unindexed_null_count,
                          int64_t max_value, int64_t min_value, int32_t partition_index, uint32_t vblock_capacity_mask) {

    auto keys_memory = reinterpret_cast<const uint8_t *>(keys_memory_addr);
    auto values_memory = reinterpret_cast<const uint8_t *>(values_memory_addr);
    auto out_args = reinterpret_cast<out_arguments *>(args_memory_addr);

    keys_reader keys(keys_memory, keys_memory_size);

    auto key_count = keys.key_count();

    auto key_begin = out_args->key_lo;
    auto key_end = out_args->key_hi;

    auto rows = reinterpret_cast<int64_t*>(out_args->rows_address);

    const auto vblock_capacity = vblock_capacity_mask + 1;

    int64_t local_key_begin = std::numeric_limits<int64_t>::max();
    int64_t local_key_end = std::numeric_limits<int64_t>::min();

    auto row_count = 0;
    for(int64_t k = key_begin; k < key_end; ++k) {
        if(k > key_count) {
            if (k < local_key_begin) local_key_begin = k;
            if (k > local_key_end) local_key_end = k;
            continue;
        }

        if (rows[k] > 0) continue;

        auto key = keys[k];
        int64_t value_count = key.value_count;
        bool update_range = true;

        if(value_count > 0) {
            block<int64_t> tail(values_memory, key.last_value_block_offset, vblock_capacity);
            block<int64_t> inconsistent_tail(values_memory, key.first_value_block_offset, vblock_capacity);

            bool is_offset_in_mapped_area = tail.offset() + tail.memory_size() <= value_memory_size;
            bool is_inconsistent = !key.is_block_consistent || !is_offset_in_mapped_area;
            if (is_inconsistent) {
                // can trust only first block offset
                int64_t block_traversed = 1;
                while (inconsistent_tail.next_offset()
                       && inconsistent_tail.next_offset() + inconsistent_tail.memory_size() <= value_memory_size) {
                    inconsistent_tail.move_next();
                    block_traversed += 1;
                }
                //assuming blocks are full
                value_count = vblock_capacity * block_traversed;
            }

            auto current_block = is_inconsistent ? inconsistent_tail : tail;
            value_count = scan_blocks_backward<int64_t>(current_block, value_count, max_value);
            if (value_count > 0) {
                int64_t local_row_id = current_block[value_count - 1];
                if (local_row_id >= min_value) {
                    rows[k] = to_row_id(partition_index, local_row_id) + 1;
                    row_count += 1;
                    update_range = false;
                }
            }
        }
        // unindexed nulls case
        if (k == 0 && unindexed_null_count > 0) {
            if (rows[k] <= 0 && unindexed_null_count - 1 >= min_value) {
                rows[k] = to_row_id(partition_index, unindexed_null_count);
                row_count += 1;
                update_range = false;
            }
        }

        if (update_range) {
            if (k < local_key_begin) local_key_begin = k;
            if (k > local_key_end) local_key_end = k;
        }
    }
    out_args->key_lo = local_key_begin;
    out_args->key_hi = local_key_end;
    out_args->rows_size = row_count;
}

int32_t findFirstLastInFrame0(
        int32_t outIndex
        , const int64_t rowIdLo
        , const int64_t rowIdHi
        , const int64_t* tsBase
        , const int64_t* sampleWindows
        , const int32_t sampleWindowCount
        , const int64_t* symbolIndex
        , const int64_t symbolIndexSize
        , const int64_t symbolIndexPosition
        , int64_t* timestampOut
        , int64_t* firstRowIdOut
        , int64_t* lastRowIdOut
        , const int32_t outSize
        ) {

    if (symbolIndexPosition < symbolIndexSize) {
        // Sample by window start, end
        int32_t winIndex = 0;
        bool firstRowUpdated = false;
        int64_t sampleByStart;

        const int64_t* symbolLo = symbolIndex + symbolIndexPosition;
        const int64_t* symbolHi = symbolIndex + symbolIndexSize;
        const int64_t* tsLo = tsBase + std::max(rowIdLo, *symbolLo);
        const int64_t* tsHi = tsBase + rowIdHi;

        while (symbolLo < symbolHi
               && winIndex < sampleWindowCount
               && tsLo < tsHi
               && outIndex < outSize - 1) {

            winIndex += branch_free_search_lower(sampleWindows + winIndex, sampleWindowCount - winIndex, *tsLo);
            sampleByStart = sampleWindows[winIndex];
            if (sampleByStart < *tsLo) {
                // first timestamp is beyond last period
                break;
            }
            symbolLo += branch_free_linked_search_lower(symbolLo, tsBase, symbolHi - symbolLo, sampleByStart);

            // Point next timestamp col position to next value in symbol column
            tsLo = tsBase + *symbolLo + ((symbolLo + 1) < symbolHi);
            if (tsBase[*symbolLo] < sampleByStart) {
                break;
            }
            lastRowIdOut[outIndex] = firstRowIdOut[outIndex] = *symbolLo;
            timestampOut[outIndex] = sampleByStart;
            outIndex++;
        }

        // Save additional values in out buffers Java expects to find
        // Next timestamp to start from
        timestampOut[outIndex] = sampleByStart;
        // Next symbolIndexPosition
        firstRowIdOut[outIndex] = symbolLo - symbolIndex;
        // Next rowIdLo
        lastRowIdOut[outIndex] = tsLo - tsBase + rowIdLo;
        return firstRowUpdated ? -outIndex : outIndex;
    }
    return outIndex;
}

extern "C" {

JNIEXPORT void JNICALL
Java_io_questdb_std_BitmapIndexUtilsNative_latestScanBackward0(JNIEnv *env, jclass cl
                                                , jlong keysMemory
                                                , jlong keysMemorySize
                                                , jlong valuesMemory
                                                , jlong valuesMemorySize
                                                , jlong argsMemory
                                                , jlong unIndexedNullCount
                                                , jlong maxValue
                                                , jlong minValue
                                                , jint partitionIndex
                                                , jint blockValueCountMod) {
    latest_scan_backward(keysMemory, keysMemorySize, valuesMemory, valuesMemorySize, argsMemory, unIndexedNullCount,
                         maxValue, minValue, partitionIndex, blockValueCountMod);
}

JNIEXPORT jint JNICALL
Java_io_questdb_std_BitmapIndexUtilsNative_findFirstLastInFrame0(JNIEnv *env, jclass cl
        , jint outIndex
        , jlong rowIdLo
        , jlong rowIdHi
        , jlong timestampColAddress
        , jlong symbolIndexAddress
        , jlong symbolIndexSize
        , jlong symbolIndexPosition
        , jlong windowBoundariesAddress
        , jint windowCount
        , jlong timestampOutAddress
        , jlong firstRowIdOutAddress
        , jlong lastRowIdOutAddress
        , jint outSize) {
    return findFirstLastInFrame0(
            outIndex
            , rowIdLo
            , rowIdHi
            , reinterpret_cast<int64_t *>(timestampColAddress)
            , reinterpret_cast<int64_t *>(windowBoundariesAddress)
            , windowCount
            , reinterpret_cast<int64_t *>(symbolIndexAddress)
            , symbolIndexSize
            , symbolIndexPosition
            , reinterpret_cast<int64_t *>(timestampOutAddress)
            , reinterpret_cast<int64_t *>(firstRowIdOutAddress)
            , reinterpret_cast<int64_t *>(lastRowIdOutAddress)
            , outSize
    );
}

} // extern "C"
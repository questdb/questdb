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

#include "bitmap_index_utils.h"
#include <atomic>

void latest_scan_backward(int64_t keys_memory_addr, int64_t keys_memory_size, int64_t values_memory_addr,
                          int64_t value_memory_size, int64_t args_memory_addr, int64_t unindexed_null_count,
                          int64_t max_value, int64_t min_value, int32_t partition_index, int32_t vblock_capacity_mask) {

    auto keys_memory = reinterpret_cast<const uint8_t *>(keys_memory_addr);
    auto values_memory = reinterpret_cast<const uint8_t *>(values_memory_addr);
    auto out_args = reinterpret_cast<out_arguments *>(args_memory_addr);

    auto header  = reinterpret_cast<const key_header *>(keys_memory);
    auto keys  = reinterpret_cast<const key_entry *>(keys_memory + sizeof(key_header));

    auto key_count = header->key_count;

    auto key_begin = out_args->key_lo;
    auto key_end = out_args->key_hi;

    auto rows = reinterpret_cast<int64_t*>(out_args->rows_address);

    const auto vblock_capacity = vblock_capacity_mask + 1;

    int64_t local_key_begin = std::numeric_limits<int64_t>::max();
    int64_t local_key_end = std::numeric_limits<int64_t>::min();
    int64_t vblock_size = vblock_capacity * 8 + 16;

    auto row_count = 0;
    for(int64_t k = key_begin; k < key_end; ++k) {
        if(k > key_count) {
            if (k < local_key_begin) local_key_begin = k;
            if (k > local_key_end) local_key_end = k;
            continue;
        }

        if (rows[k] > 0) continue;

        int64_t value_count = 0;
        int64_t last_vblock_offset = 0;
        int64_t first_vblock_offset = 0;
        auto retries_count = 10;
        bool is_kblock_consistent = false;
        while(retries_count--) {
            value_count = keys[k].value_count;
            std::atomic_thread_fence(std::memory_order_acquire);
            if (keys[k].count_check == value_count) {
                first_vblock_offset = keys[k].first_value_block_offset;
                last_vblock_offset = keys[k].last_value_block_offset;
                std::atomic_thread_fence(std::memory_order_acquire);
                if (keys[k].value_count == value_count) {
                    is_kblock_consistent = true;
                    break;
                }
            }
        }
        bool update_range = true;
        if(value_count > 0) {
            int64_t vblock_end_offset = last_vblock_offset + vblock_size;
            bool is_offset_in_mapped_area = vblock_end_offset <= value_memory_size;

            if (!is_kblock_consistent || !is_offset_in_mapped_area) {
                // can trust only first vblock offset
                last_vblock_offset = first_vblock_offset;
                auto link = reinterpret_cast<const value_block_link *>(values_memory + last_vblock_offset + vblock_size - sizeof(value_block_link));
                int64_t block_traversed = 1;
                while(link->next && link->next + vblock_size <= value_memory_size) {
                    last_vblock_offset = link->next;
                    link = reinterpret_cast<const value_block_link *>(values_memory + last_vblock_offset + vblock_size - sizeof(value_block_link));
                    block_traversed += 1;
                }
                //assuming blocks are full
                value_count = vblock_capacity*block_traversed;
            }

            auto values = reinterpret_cast<const int64_t *>(values_memory);
            auto res = seekValueBlockRTL(value_count, last_vblock_offset, values, max_value,
                                         vblock_capacity_mask);
            value_count = res.first;
            last_vblock_offset = res.second;
            if (value_count > 0) {
                uint64_t cell_index = --value_count & vblock_capacity_mask;
                int64_t local_row_id = values[last_vblock_offset / 8 + cell_index];
                if (local_row_id >= min_value) {
                    rows[k] = ((int64_t) partition_index << 44) + local_row_id + 1;
                    row_count += 1;
                    update_range = false;
                }
            }
        }

        // unindexed nulls case
        if (k == 0 && unindexed_null_count > 0) {
            if (rows[k] <= 0 && unindexed_null_count - 1 >= min_value) {
                rows[k] = ((int64_t) partition_index << 44) + unindexed_null_count;
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

} // extern "C"
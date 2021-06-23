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
        , const int64_t* timestampColumn
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
        int32_t tsNextPeriodIndex = 0;
        bool firstRowUpdated = false;
        int32_t prevPeriodIndex = 0;
        int64_t sampleByStart = sampleWindows[tsNextPeriodIndex++];
        int64_t sampleByEnd = tsNextPeriodIndex < sampleWindowCount ? sampleWindows[tsNextPeriodIndex] : INT64_MAX;

        const int64_t* symbolLo = symbolIndex + symbolIndexPosition;
        const int64_t* symbolHi = symbolIndex + symbolIndexSize;
        const int64_t* tsLo = timestampColumn + std::max(rowIdLo, *symbolLo);
        const int64_t* tsHi = timestampColumn + rowIdHi;

        while (symbolLo < symbolHi
               && tsNextPeriodIndex < sampleWindowCount
               && tsLo < tsHi
               && outIndex < outSize - 1) {

            while (*tsLo >= sampleByEnd) {
                sampleByStart = sampleWindows[tsNextPeriodIndex++];
                sampleByEnd = tsNextPeriodIndex < sampleWindowCount ? sampleWindows[tsNextPeriodIndex] : INT64_MAX;
            }

            // 2 options when tsLo >= sampleByEnd:
            //                    start                 end
            // | ------------------ | ------------------ | ------------------ | ------------------ |
            //         |                      |
            //       tsLo                    tsLo

            // Find rowId before the next sample by start boundary in Timestamp column
            tsLo += branch_free_search_lower(tsLo, tsHi - tsLo, sampleByStart);
            // 2 options after binary search
            //                    start                 end
            // | ------------------ | ------------------ | ------------------ | ------------------ |
            //         |                     |
            //   tsLo == tsHi - 1           tsLo

            const int64_t nextTsRowId = tsLo - timestampColumn;
            symbolLo += branch_free_search_lower(symbolLo, symbolHi - symbolLo, nextTsRowId);

            //                    start                 end
            // | ------------------ | ------------------ | ------------------ | ------------------ |
            //         |                     |
            //   tsLo == tsHi - 1           tsLo
            //         |   |                 |   |
            //       symLo                   symLo

            while (timestampColumn[*symbolLo] >= sampleByEnd) {
                sampleByStart = sampleWindows[tsNextPeriodIndex++];
                sampleByEnd = tsNextPeriodIndex < sampleWindowCount ? sampleWindows[tsNextPeriodIndex] : INT64_MAX;
            }

            if (prevPeriodIndex == tsNextPeriodIndex - 1 && outIndex > 0) {
                // Update lastRowId from last period
                lastRowIdOut[outIndex - 1] = *(symbolLo - (timestampColumn[*symbolLo] >= sampleByStart));
                firstRowUpdated |= (outIndex == 1);
            }

            if (*tsLo < sampleByStart) {
                // end of timestamp buffer reached but still didn't caught up with next sampleByStart
                break;
            }

            // Check index found is within data frame
            if (*symbolLo > rowIdHi) {
                tsLo = tsHi;
                break;
            }

            // timestampColumn[*symbolLo] >= sampleByStart
            // Found first row id
            lastRowIdOut[outIndex] = firstRowIdOut[outIndex] = *symbolLo;
            timestampOut[outIndex] = sampleByStart;
            prevPeriodIndex = tsNextPeriodIndex;
            sampleByStart = sampleWindows[tsNextPeriodIndex++];
            sampleByEnd = tsNextPeriodIndex < sampleWindowCount ? sampleWindows[tsNextPeriodIndex] : INT64_MAX;
            outIndex++;
            symbolLo++;

            if (symbolLo < symbolHi) {
                tsLo = timestampColumn + *(symbolLo);
            }
        }

        if (sampleByEnd == INT64_MAX && outIndex > 0) {
            symbolLo += branch_free_search_lower(symbolLo, symbolHi - symbolLo, rowIdHi);
            lastRowIdOut[outIndex - 1] = *symbolLo;
            firstRowUpdated |= (outIndex == 1);
        }

        // Save additional values in out buffers Java expects to find
        // Next timestamp to start from
        timestampOut[outIndex] = sampleByStart;
        // Next symbolIndexPosition
        firstRowIdOut[outIndex] = symbolLo - symbolIndex;
        // Next rowIdLo
        lastRowIdOut[outIndex] = tsLo - timestampColumn + rowIdLo;
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
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

void latest_scan_backward(
        uint8_t *keys_memory,
        size_t keys_memory_size,
        uint8_t *values_memory,
        size_t value_memory_size,
        out_arguments *out_args,
        int64_t unindexed_null_count,
        int64_t max_value,
        int64_t min_value,
        int32_t partition_index,
        uint32_t vblock_capacity_mask
) {
    keys_reader keys(keys_memory, keys_memory_size);

    auto key_count = keys.key_count();

    auto key_begin = out_args->key_lo;
    auto key_end = out_args->key_hi;
    auto rows = out_args->rows;

    const auto vblock_capacity = vblock_capacity_mask + 1;

    int64_t local_key_begin = std::numeric_limits<int64_t>::max();
    int64_t local_key_end = std::numeric_limits<int64_t>::min();

    auto row_count = 0;
    for (int64_t k = key_begin; k < key_end; ++k) {
        if (k > key_count) {
            if (k < local_key_begin) local_key_begin = k;
            if (k > local_key_end) local_key_end = k;
            continue;
        }

        if (rows[k] > 0) continue;

        auto key = keys[k];

        int64_t value_count = key.value_count;
        bool update_range = true;

        if (value_count > 0) {
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

inline int64_t linked_search_lower(const int64_t *indexBase,
                                   const int64_t *dataBase,
                                   int64_t indexLength,
                                   const int64_t estimatedCount,
                                   const int64_t value) {
    // Timeseries data is usually distributed equally in time
    // This code optimises using assumption that as if there are 200 index elements within 20 mins
    // and sample by is by 1 minute the next value will be around every 10 elements
    // To not degrade it to full scan, use miniumum increment of 32
    // This is same as
    // return branch_free_linked_search_lower(indexBase, dataBase, indexLength, value);
    const int64_t step = std::max<int64_t>(32LL, indexLength / estimatedCount);
    int64_t searchStart = 0L;
    while (searchStart + step < indexLength && dataBase[*(indexBase + searchStart + step - 1)] < value) {
        searchStart += step;
    }
    return searchStart + branch_free_linked_search_lower(
            indexBase + searchStart, dataBase,
            std::min(step, indexLength - searchStart),
            value
    );
}

template<typename lambda_search_index, typename lambda_ts_by_index>
inline int32_t findFirstLastInFrame0(
        int32_t outIndex,
        const int64_t rowIdLo,
        const int64_t rowIdHi,
        const int64_t *tsBase,
        const int64_t frameBaseOffset,
        const int64_t indexOffset,
        const int64_t indexCount,
        const int64_t *samplePeriods,
        const int32_t samplePeriodCount,
        const int64_t sampleIndexOffset,
        fl_record *outputRowIds,
        const int32_t outSize,
        const lambda_search_index fnSearchIndex,
        const lambda_ts_by_index fnTsRowIdByIndex
) {
    // This method searches Timestamp column (tsBase, rowLo, rowHi)
    // for the first and last values in the sample windows set in samplePeriods
    // using index (indexBase, indexCount, indexPosition)
    // Index is symbol index, ascending Row Ids of the Timestamp column
    // Example:
    // when data is
    // 0         1         2         3         4         5         6         7         8         9         10
    // 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890
    // ------------- | -------------------- | ---------------------- | ---------------------- | ------------------ |
    // a  b  c  a  b b  c a c b  a                                  ba     c a                   a           a
    //
    // periodStarts are noted as | at: 14, 37, 62, 87, 108
    // and indexes of 'a' are        : 0, 9, 19, 26, 62, 70, 90, 102
    // the search should find
    // outputRowIds(    first)       : 19, 62,  90
    // outputRowIds(     last)       : 26, 70, 102
    // outputRowIds(timestamp)       :  0,  2,   3
    // Output in timestampOut contains indexes of the periods found
    // Last value of output buffers reserved for positions to resume the search
    // outputRowIds[n + 0] :   7 ( index position                  )
    // outputRowIds[n + 1] :  108 ( timestamp column position       )
    // outputRowIds[n + 2] :    4 ( last processed period end index )
    int32_t periodIndex = 0;
    int64_t sampleStart;
    bool firstRowUpdated = false;

    int64_t indexLo = 0;
    const int64_t indexHi = indexCount;
    const int64_t *tsLo = tsBase + std::max(rowIdLo, fnTsRowIdByIndex(indexLo));
    const int64_t *tsHi = tsBase + rowIdHi;
    const int64_t maxTs = *(tsHi - 1);
    const int64_t maxOutLength = outSize - 1;

    while (indexLo < indexHi
           && periodIndex < samplePeriodCount
           && tsLo < tsHi
           && outIndex < maxOutLength) {

        sampleStart = samplePeriods[periodIndex];
        indexLo += fnSearchIndex(indexLo, samplePeriodCount - periodIndex, std::min<int64_t>(maxTs + 1, sampleStart));

        // Set last value as previous value to the found one
        if (outIndex > 0
            && outputRowIds[outIndex - 1].timestamp_index == periodIndex + sampleIndexOffset - 1 // prev out row is for period - 1
            && indexLo > 0) {
            int64_t prevLastRowId = fnTsRowIdByIndex(indexLo - 1);
            outputRowIds[outIndex - 1].last_row_id = prevLastRowId - frameBaseOffset;
            firstRowUpdated |= outIndex == 1; // need to know if firt row lat_row_id is updated
        }

        if (indexLo == indexHi || sampleStart > maxTs || periodIndex > samplePeriodCount - 2) {
            break;
        }

        int64_t indexTs = tsBase[fnTsRowIdByIndex(indexLo)];
        int64_t sampleEnd = samplePeriods[periodIndex + 1];
        if (PREDICT_FALSE(indexTs >= sampleEnd)) {
            // indexTs is beyond sampling period. Find the sampling period the indexTs is in.
            // branch_free_search_lower returns insert position of the value
            // We need the index of the first value less or equal to indexTs
            // This is the same as searching of (indexTs + 1) and getting previous index
            periodIndex += (int32_t) branch_free_search_lower(
                    samplePeriods + periodIndex,
                    samplePeriodCount - periodIndex,
                    indexTs + 1
            ) - 1;
            continue;
        }

        // Point next timestamp column position to found Index value
        tsLo = tsBase + fnTsRowIdByIndex(indexLo);
        if (tsLo >= tsHi || *tsLo < sampleStart) {
            // If index value is beyond data frame limits
            // or no symbol exists higher than the searched sampleStart
            // abort the search and return results
            break;
        }
        outputRowIds[outIndex].last_row_id = outputRowIds[outIndex].first_row_id = fnTsRowIdByIndex(indexLo) - frameBaseOffset;
        outputRowIds[outIndex].timestamp_index = sampleIndexOffset + periodIndex;
        outIndex++;
        periodIndex++;
    }

    // Save additional values in out buffers Java expects to find
    // Next indexPosition
    outputRowIds[outIndex].first_row_id = indexLo + indexOffset;
    // Next rowIdLo
    outputRowIds[outIndex].last_row_id = tsLo - tsBase;
    // Next timestamp to start from
    outputRowIds[outIndex].timestamp_index = sampleIndexOffset + std::min(periodIndex, samplePeriodCount - 1);
    return firstRowUpdated ? -outIndex : outIndex;
}

extern "C" {

JNIEXPORT void JNICALL
Java_io_questdb_std_BitmapIndexUtilsNative_latestScanBackward0(
        JNIEnv *env, jclass cl,
        jlong keysMemory,
        jlong keysMemorySize,
        jlong valuesMemory,
        jlong valuesMemorySize,
        jlong argsMemory,
        jlong unIndexedNullCount,
        jlong maxValue,
        jlong minValue,
        jint partitionIndex,
        jint blockValueCountMod
) {
    latest_scan_backward(
            reinterpret_cast<uint8_t *>(keysMemory),
            keysMemorySize,
            reinterpret_cast<uint8_t *>(valuesMemory),
            valuesMemorySize,
            reinterpret_cast<out_arguments *>(argsMemory),
            unIndexedNullCount,
            maxValue,
            minValue,
            partitionIndex,
            blockValueCountMod
    );
}

JNIEXPORT jint JNICALL
Java_io_questdb_std_BitmapIndexUtilsNative_findFirstLastInFrame0(
        JNIEnv *env,
        jclass cl,
        jint outIndex,
        jlong rowIdLo,
        jlong rowIdHi,
        jlong timestampColAddress,
        jlong frameBaseOffset,
        jlong symbolIndexAddress,
        jlong symbolIndexCount,
        jlong symbolIndexPosition,
        jlong samplePeriodsAddress,
        jint samplePeriodCount,
        jlong samplePeriodIndexOffset,
        jlong firstRowIdOutAddress,
        jint outSize
) {
    auto *indexBase = reinterpret_cast<int64_t *>(symbolIndexAddress);
    auto *tsBase = reinterpret_cast<int64_t *>(timestampColAddress);
    auto fnSearchIndex = [=](const int64_t indexLo, const int64_t periodCount, const int64_t value) {
        return linked_search_lower(
                indexBase + symbolIndexPosition + indexLo,
                tsBase,
                symbolIndexCount - symbolIndexPosition - indexLo,
                periodCount,
                value
        );
    };
    auto fnRowIdByIndex = [=](int64_t indexPosition) {
        return indexBase[symbolIndexPosition + indexPosition];
    };
    return findFirstLastInFrame0(
            outIndex,
            rowIdLo,
            rowIdHi,
            tsBase,
            frameBaseOffset,
            symbolIndexPosition,
            symbolIndexCount - symbolIndexPosition,
            reinterpret_cast<int64_t *>(samplePeriodsAddress),
            samplePeriodCount,
            samplePeriodIndexOffset,
            reinterpret_cast<fl_record *>(firstRowIdOutAddress),
            outSize,
            fnSearchIndex,
            fnRowIdByIndex
    );
}

JNIEXPORT jint JNICALL
Java_io_questdb_std_BitmapIndexUtilsNative_findFirstLastInFrameNoFilter0(
        JNIEnv *env,
        jclass cl,
        jint outIndex,
        jlong rowIdLo,
        jlong rowIdHi,
        jlong timestampColAddress,
        jlong frameBaseOffset,
        jlong samplePeriodsAddress,
        jint samplePeriodCount,
        jlong samplePeriodIndexOffset,
        jlong firstRowIdOutAddress,
        jint outSize
) {
    auto *tsBase = reinterpret_cast<int64_t *>(timestampColAddress);
    auto fnSearchIndex = [=](const int64_t tsLo, const int64_t periodCount, const int64_t value) {
        return branch_free_search_lower(tsBase + rowIdLo + tsLo, rowIdHi - rowIdLo - tsLo, value);
    };
    auto fnRowIdByIndex = [=](int64_t position) {
        return position + rowIdLo;
    };
    return findFirstLastInFrame0(
            outIndex,
            rowIdLo,
            rowIdHi,
            tsBase,
            frameBaseOffset,
            0,
            rowIdHi - rowIdLo,
            reinterpret_cast<int64_t *>(samplePeriodsAddress),
            samplePeriodCount,
            samplePeriodIndexOffset,
            reinterpret_cast<fl_record *>(firstRowIdOutAddress),
            outSize,
            fnSearchIndex,
            fnRowIdByIndex
    );
}
} // extern "C"
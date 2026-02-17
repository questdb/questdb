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

#include <jni.h>
#include "geohash_dispatch.h"
#include "bitmap_index_utils.h"
#include "simd.h"
#include <algorithm>

extern "C" {

DECLARE_DISPATCHER(simd_iota);

JNIEXPORT void JNICALL
Java_io_questdb_griffin_engine_functions_geohash_GeoHashNative_iota(
        JNIEnv */*env*/,
        jclass /*cl*/,
        jlong address,
        jlong size,
        jlong init
) {
    auto *array = reinterpret_cast<int64_t *>(address);
    const auto array_size = static_cast<int64_t>(size);
    const auto init_value = static_cast<int64_t>(init);
    simd_iota(array, array_size, init_value);
}

DECLARE_DISPATCHER(filter_with_prefix);

JNIEXPORT void JNICALL
Java_io_questdb_griffin_engine_functions_geohash_GeoHashNative_latestByAndFilterPrefix(
        JNIEnv */*env*/,
        jclass /*cl*/,
        jlong keysMemory,
        jlong keysMemorySize,
        jlong valuesMemory,
        jlong valuesMemorySize,
        jlong argsMemory,
        jlong unIndexedNullCount,
        jlong maxValue,
        jlong minValue,
        jint frameIndex, // inverted frame index
        jint blockValueCountMod,
        jlong geoHashColumnAddress,
        jint geoHashColumnSize,
        jlong prefixesAddress,
        jlong prefixesCount
) {
    auto out_args = reinterpret_cast<out_arguments *>(argsMemory);
    auto rows = reinterpret_cast<int64_t *>(out_args->rows_address);
    auto rows_count_prev = out_args->rows_size;

    latest_scan_backward(
            keysMemory,
            keysMemorySize,
            valuesMemory,
            valuesMemorySize,
            argsMemory,
            unIndexedNullCount,
            maxValue,
            minValue,
            frameIndex,
            blockValueCountMod
    );

    auto rows_count_after = out_args->rows_size;
    const auto geohash_column_addr = reinterpret_cast<void *>(geoHashColumnAddress);
    const auto geohash_column_size = static_cast<int32_t>(geoHashColumnSize);
    const auto *prefixes = reinterpret_cast<const int64_t *>(prefixesAddress);
    const auto prefixes_count = static_cast<int64_t>(prefixesCount);

    auto found_start = rows + out_args->key_lo;
    auto found_stop = rows + out_args->key_lo + rows_count_after;

    if (geohash_column_addr && prefixes && prefixes_count) {
        int64_t filtered_count = 0;
        filter_with_prefix(
                geohash_column_addr,
                rows + out_args->key_lo + rows_count_prev,
                geohash_column_size,
                rows_count_after - rows_count_prev,
                prefixes,
                prefixes_count,
                &filtered_count
        );

        auto filtered_start = rows + out_args->key_lo + rows_count_prev;
        auto len = filtered_count * sizeof(int64_t);
        auto dst = rows + out_args->key_lo + out_args->filtered_size;
        __MEMMOVE(reinterpret_cast<void *>(dst), reinterpret_cast<void *>(filtered_start), len);

        out_args->filtered_size += filtered_count;
    } else {
        out_args->filtered_size = std::distance(found_start, found_stop);
    }
}

JNIEXPORT jlong JNICALL
Java_io_questdb_griffin_engine_functions_geohash_GeoHashNative_slideFoundBlocks
        (
                JNIEnv */*env*/,
                jclass /*cl*/,
                jlong argsAddress,
                jlong argsCount
        ) {

    auto args = reinterpret_cast<out_arguments *>(argsAddress);
    auto args_count = static_cast<int64_t>(argsCount);

    if (args_count <= 0) {
        return 0;
    }

    const auto addr = reinterpret_cast<int64_t *>(args[0].rows_address);
    const auto begin0 = addr + args[0].key_lo;
    auto dst = begin0 + args[0].filtered_size;
    for (auto i = 1; i < args_count; ++i) {
        auto src = addr + args[i].key_lo;
        auto len = args[i].filtered_size * sizeof(int64_t);
        __MEMMOVE(reinterpret_cast<void *>(dst), reinterpret_cast<void *>(src), len);
        dst += args[i].filtered_size;
    }
    return std::distance(addr, dst);
}

} // extern "C"
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

#include <jni.h>
#include "geohash_dispatch.h"
#include "bitmap_index_utils.h"
#include "simd.h"
#include <algorithm>

extern "C" {

DECLARE_DISPATCHER(simd_iota);

JNIEXPORT void JNICALL
Java_io_questdb_griffin_engine_functions_geohash_GeoHashNative_iota(
        JNIEnv *e,
        jclass cl,
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
Java_io_questdb_griffin_engine_functions_geohash_GeoHashNative_latesByAndFilterPrefix
        (
                JNIEnv *env,
                jclass cl,
                jlong keysMemory,
                jlong keysMemorySize,
                jlong valuesMemory,
                jlong valuesMemorySize,
                jlong argsMemory,
                jlong unIndexedNullCount,
                jlong maxValue,
                jlong minValue,
                jint partitionIndex,
                jint blockValueCountMod,
                jlong hashesAddress,
                jint hashLength,
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
            partitionIndex,
            blockValueCountMod);

    auto rows_count_after = out_args->rows_size;
    const auto *hashes = reinterpret_cast<const int64_t *>(hashesAddress);
    const auto hash_length = static_cast<int32_t>(hashLength);
    const auto *prefixes = reinterpret_cast<const int64_t *>(prefixesAddress);
    const auto prefixes_count = static_cast<int64_t>(prefixesCount);

    auto found_start = rows + out_args->key_lo;
    auto found_stop = rows + out_args->key_lo + rows_count_after;

    if (hashes && prefixes && prefixes_count) {

        filter_with_prefix(
                hashes,
                rows + out_args->key_lo + rows_count_prev,
                rows_count_after - rows_count_prev,
                hash_length,
                prefixes,
                prefixes_count
        );

        auto filtered_start = rows + out_args->key_lo + rows_count_prev;
        auto filtered_stop = rows + out_args->key_lo + rows_count_after;
        auto p = std::partition(filtered_start, filtered_stop, [](int64_t n) { return n != 0; });
        auto filtered_count = std::distance(filtered_start, p);
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
                JNIEnv *env,
                jclass cl,
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
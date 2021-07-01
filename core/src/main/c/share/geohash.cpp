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
#include <algorithm>

extern "C" {
    DECLARE_DISPATCHER(filter_with_prefix);

    JNIEXPORT void JNICALL
    Java_io_questdb_griffin_engine_functions_geohash_GeoHashNative_filterWithPrefix(
            JNIEnv *e,
            jclass cl,
            jlong hashesAddress,
            jlong rowsAddress,
            jlong rowsCount,
            jint hashLength,
            jlong prefixesAddress,
            jlong prefixesCount
    ) {
        const auto *hashes = reinterpret_cast<const int64_t *>(hashesAddress);
        auto *rows = reinterpret_cast<int64_t *>(rowsAddress);
        const auto rows_count = static_cast<int64_t>(rowsCount);
        const auto hash_length = static_cast<int32_t>(hashLength);
        const auto *prefixes = reinterpret_cast<const int64_t *>(prefixesAddress);
        const auto prefixes_count = static_cast<int64_t>(prefixesCount);

        filter_with_prefix(hashes, rows, rows_count, hash_length, prefixes, prefixes_count);
    }

    JNIEXPORT void JNICALL
    Java_io_questdb_griffin_engine_functions_geohash_GeoHashNative_partitionBy(
            JNIEnv *env,
            jclass cl,
            jlong pLong,
            jlong len,
            jlong value
    ) {
        auto *values = reinterpret_cast<int64_t *>(pLong);
        auto count = static_cast<int64_t>(len);
        const auto v = static_cast<int64_t>(value);
        std::partition(values, values + count, [=](int64_t n) { return n == v; });
    }

}
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

#include "util.h"
#include "geohash_dispatch.h"

void simd_iota(int64_t *array, int64_t array_size, int64_t start) {
    int64_t next = start;
    for (int64_t i = 0; i < array_size; ++i) {
        array[i] = next++;
    }
}

void filter_with_prefix(
        const void *hashes,
        int64_t *rows,
        int32_t hashes_type_size,
        int64_t rows_count,
        const int64_t *prefixes,
        int64_t prefixes_count,
        int64_t *out_filtered_count
) {
    switch (hashes_type_size) {
        case 1:
            filter_with_prefix_generic_vanilla<int8_t>(
                    static_cast<const int8_t *>(hashes),
                    rows,
                    rows_count,
                    prefixes,
                    prefixes_count,
                    out_filtered_count
            );
            break;
        case 2:
            filter_with_prefix_generic_vanilla<int16_t>(
                    static_cast<const int16_t *>(hashes),
                    rows,
                    rows_count,
                    prefixes,
                    prefixes_count,
                    out_filtered_count
            );
            break;
        case 4:
            filter_with_prefix_generic_vanilla<int32_t>(
                    static_cast<const int32_t *>(hashes),
                    rows,
                    rows_count,
                    prefixes,
                    prefixes_count,
                    out_filtered_count
            );
            break;
        case 8:
            filter_with_prefix_generic_vanilla<int64_t>(
                    static_cast<const int64_t *>(hashes),
                    rows,
                    rows_count,
                    prefixes,
                    prefixes_count,
                    out_filtered_count
            );
            break;
        default:
            break;
    }
}

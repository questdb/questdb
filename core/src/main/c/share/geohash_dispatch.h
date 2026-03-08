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

#ifndef QUESTDB_GEOHASH_DISPATCH_H
#define QUESTDB_GEOHASH_DISPATCH_H

#include "util.h"
#include "dispatcher.h"
#include "bitmap_index_utils.h"
#include "rosti.h"

constexpr int64_t bitmask(uint8_t count, uint8_t shift) { return ((static_cast<int64_t>(1) << count) - 1) << shift; }

template<typename T, typename TVec, typename TVecB>
void filter_with_prefix_generic(
        const T *hashes,
        int64_t *rows,
        int64_t rows_count,
        const int64_t *prefixes,
        int64_t prefixes_count,
        int64_t *out_filtered_count
) {
    int64_t i = 0; // input index
    int64_t o = 0; // output index

    constexpr int step = TVec::size();
    const int64_t limit = rows_count - step + 1;

    for (; i < limit; i += step) {
        MM_PREFETCH_T0(rows + i + 64);

        TVec current_hashes_vec;
        for (int j = 0; j < TVec::size(); ++j) {
            current_hashes_vec.insert(j, hashes[to_local_row_id(rows[i + j] - 1)]);
        }

        TVecB hit_mask(false);
        for (size_t j = 0, size = prefixes_count / 2; j < size; ++j) {
            const T hash = static_cast<T>(prefixes[2 * j]); // narrow cast for int/short/byte cases
            const T mask = static_cast<T>(prefixes[2 * j + 1]);
            TVec target_hash(hash); // broadcast hash
            TVec target_mask(mask); // broadcast mask
            hit_mask |= (current_hashes_vec & target_mask) == target_hash;
        }

        uint64_t bits = to_bits(hit_mask);
        if (bits != 0) {
            while(bits) {
                auto idx = bit_scan_forward(bits);
                rows[o++] = rows[i + idx];
                bits &= ~(1ull << idx);
            }
        }
    }

    for (; i < rows_count; ++i) {
        const T current_hash = hashes[to_local_row_id(rows[i] - 1)];
        bool hit = false;
        for (size_t j = 0, sz = prefixes_count/2; j < sz; ++j) {
            const T hash = static_cast<T>(prefixes[2*j]);
            const T mask = static_cast<T>(prefixes[2*j+1]);
            hit |= (current_hash & mask) == hash;
        }
        if (hit) {
            rows[o++] = rows[i];
        }
    }
    *out_filtered_count = o;
}

template<typename T>
void filter_with_prefix_generic_vanilla(
        const T *hashes,
        int64_t *rows,
        int64_t rows_count,
        const int64_t *prefixes,
        int64_t prefixes_count,
        int64_t *out_filtered_count
) {
    int64_t i = 0; // input index
    int64_t o = 0; // output index
    for (; i < rows_count; ++i) {
        const T current_hash = hashes[to_local_row_id(rows[i] - 1)];
        bool hit = false;
        for (size_t j = 0, sz = prefixes_count/2; j < sz; ++j) {
            const T hash = static_cast<T>(prefixes[2*j]);
            const T mask = static_cast<T>(prefixes[2*j+1]);
            hit |= (current_hash & mask) == hash;
        }
        if (hit) {
            rows[o++] = rows[i];
        }
    }
    *out_filtered_count = o;
}

DECLARE_DISPATCHER_TYPE(filter_with_prefix,
                        const void *hashes,
                        int64_t *rows,
                        int32_t hashes_type_size,
                        int64_t rows_count,
                        const int64_t *prefixes,
                        int64_t prefixes_count,
                        int64_t *out_filtered_count
);

DECLARE_DISPATCHER_TYPE(simd_iota, int64_t *array, int64_t array_size, int64_t start);

#endif //QUESTDB_GEOHASH_DISPATCH_H

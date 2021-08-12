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

#ifndef QUESTDB_GEOHASH_DISPATCH_H
#define QUESTDB_GEOHASH_DISPATCH_H

#include "util.h"
#include "dispatcher.h"

constexpr int64_t unpack_length(int64_t packed_hash) { return packed_hash >> 60; }

constexpr int64_t unpack_hash(int64_t packed_hash) { return packed_hash & 0x0fffffffffffffffll; }

constexpr int64_t bitmask(uint8_t count, uint8_t shift) { return ((static_cast<int64_t>(1) << count) - 1) << shift; }

template<typename T, typename TVec, typename TVecB>
void filter_with_prefix_generic(
        const T *hashes,
        int64_t *rows,
        const int64_t rows_count,
        const int64_t *prefixes,
        const int64_t prefixes_count
) {
    int64_t i = 0;
    const int step = 8;
    const int64_t limit = rows_count - step + 1;
    for (; i < limit; i += step) {
        MM_PREFETCH_T0(rows + i + 64);
        Vec8q offset;
        offset.load(rows + i);

        TVec current_hashes_vec(
                hashes[to_local_row_id(offset[0] -1)],
                hashes[to_local_row_id(offset[1] -1)],
                hashes[to_local_row_id(offset[2] -1)],
                hashes[to_local_row_id(offset[3] -1)],
                hashes[to_local_row_id(offset[4] -1)],
                hashes[to_local_row_id(offset[5] -1)],
                hashes[to_local_row_id(offset[6] -1)],
                hashes[to_local_row_id(offset[7] -1)]);

        TVecB hit_mask(false);
        for (size_t j = 0, sz = prefixes_count/2; j < sz; ++j) {
            const T hash = static_cast<T>(prefixes[2*j]); // narrow cast for int/short/byte cases
            const T mask = static_cast<T>(prefixes[2*j + 1]);
            TVec target_hash(hash); // broadcast hash
            TVec target_mask(mask); // broadcast mask
            hit_mask |= (current_hashes_vec & target_mask) == target_hash;

        }
        TVec filtered = select(hit_mask, offset, 0);
        filtered.store(rows + i);
    }

    for (; i < rows_count; ++i) {
        const T current_hash = hashes[to_local_row_id(rows[i] - 1)];
        bool hit = false;
        for (size_t j = 0, sz = prefixes_count/2; j < sz; ++j) {
            const T hash = static_cast<T>(prefixes[2*j]);
            const T mask = static_cast<T>(prefixes[2*j+1]);
            hit |= (current_hash & mask) == hash;
        }
        const int64_t cv = rows[i];
        rows[i] = hit ? cv : 0;
    }
}

template<typename T>
void filter_with_prefix_generic_vanilla(
        const T *hashes,
        int64_t *rows,
        const int64_t rows_count,
        const int64_t *prefixes,
        const int64_t prefixes_count
) {
    int64_t i = 0;
    for (; i < rows_count; ++i) {
        const T current_hash = hashes[to_local_row_id(rows[i] - 1)];
        bool hit = false;
        for (size_t j = 0, sz = prefixes_count/2; j < sz; ++j) {
            const T hash = static_cast<T>(prefixes[2*j]);
            const T mask = static_cast<T>(prefixes[2*j+1]);
            hit |= (current_hash & mask) == hash;
        }
        const int64_t cv = rows[i];
        rows[i] = hit ? cv : 0;
    }
}


DECLARE_DISPATCHER_TYPE(filter_with_prefix,
                        const int64_t hashes,
                        int64_t *rows,
                        const int64_t rows_count,
                        const int32_t hash_length,
                        const int64_t *prefixes,
                        const int64_t prefixes_count
);

DECLARE_DISPATCHER_TYPE(simd_iota, int64_t *array, const int64_t array_size, const int64_t start);

#endif //QUESTDB_GEOHASH_DISPATCH_H

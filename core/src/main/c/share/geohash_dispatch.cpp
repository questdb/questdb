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

#include "util.h"
#include "bitmap_index_utils.h"
#include "geohash_dispatch.h"
#include <string>

void MULTI_VERSION_NAME (simd_iota)(int64_t *array, const int64_t array_size, const int64_t start) {
    const int64_t step = 8;
    int64_t i = 0;

    Vec8q init_vec(0, 1, 2, 3, 4, 5, 6, 7);
    init_vec += start;

    Vec8q step_vec(step);

    const int64_t limit = array_size - step + 1;
    for (; i < limit; i += step, init_vec += step_vec) {
        init_vec.store(array + i);
    }

    int64_t next = i < step ? i + start : init_vec[step - 1]  - step + 1;
    for (; i < array_size; ++i) {
        array[i] = next++;
    }
}


void MULTI_VERSION_NAME (filter_with_prefix)(
        const int64_t *hashes,
        int64_t *rows,
        const int64_t rows_count,
        const int32_t hash_length,
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

        Vec8q current_hashes_vec(
                hashes[to_local_row_id(offset[0] -1)],
                hashes[to_local_row_id(offset[1] -1)],
                hashes[to_local_row_id(offset[2] -1)],
                hashes[to_local_row_id(offset[3] -1)],
                hashes[to_local_row_id(offset[4] -1)],
                hashes[to_local_row_id(offset[5] -1)],
                hashes[to_local_row_id(offset[6] -1)],
                hashes[to_local_row_id(offset[7] -1)]);

        Vec8qb hit_mask(false);
        for (size_t j = 0, sz = prefixes_count/2; j < sz; ++j) {
            const int64_t hash = prefixes[2*j];
            const int64_t mask = prefixes[2*j + 1];
            Vec8q target_hash(hash); // broadcast hash
            Vec8q target_mask(mask); // broadcast mask
            hit_mask |= (current_hashes_vec & target_mask) == target_hash;

        }
        Vec8q bit_mask(hit_mask);
        offset &= bit_mask;
        offset.store(rows + i);
    }

    for (; i < rows_count; ++i) {
        const int64_t current_hash = hashes[to_local_row_id(rows[i] - 1)];
        bool hit = false;
        for (size_t j = 0, sz = prefixes_count/2; j < sz; ++j) {
            const int64_t hash = prefixes[2*j];
            const int64_t mask = prefixes[2*j+1];
            hit |= (current_hash & mask) == hash;
        }
        const int64_t cv = rows[i];
        rows[i] = hit ? cv : 0;
    }
}

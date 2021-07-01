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
#include "geohash_dispatch.h"
#include <vector>

constexpr int64_t unpack_length(int64_t packed_hash) { return packed_hash >> 60; }

constexpr int64_t unpack_hash(int64_t packed_hash) { return packed_hash & 0x0fffffffffffffffll; }

constexpr int64_t bitmask(uint8_t count, uint8_t shift) {
    return ((static_cast<int64_t>(1) << count) - 1) << shift;
}

void MULTI_VERSION_NAME (filter_with_prefix)(
        const int64_t *hashes,
        int64_t *rows,
        const int64_t rows_count,
        const int32_t hash_length,
        const int64_t *prefixes,
        const int64_t prefixes_count
        ) {

    struct normalized {
        int64_t hash;
        int64_t mask;
    };

    std::vector<normalized> normalized_hashes;
    normalized_hashes.reserve(prefixes_count);
    for (size_t i = 0; i < prefixes_count; ++i) {
        const int64_t hash = unpack_hash(prefixes[i]);
        const int64_t nbits = unpack_length(prefixes[i]) * 5;
        const int64_t shift = hash_length * 5 - nbits;
        const int64_t norm = hash << shift;
        const int64_t mask = bitmask(nbits, shift);
        normalized_hashes.push_back(normalized{norm, mask});
    }

    int64_t i = 0;
    const int step = 8;
    const int64_t limit = rows_count - step + 1;
    for (; i < limit; i += step) {
        MM_PREFETCH_T0(hashes + i + 64);
        MM_PREFETCH_T0(rows + i + 64);

        Vec8q current_hashes_vec;
        current_hashes_vec.load(hashes + i);

        Vec8qb hit_mask(false);
        for (size_t j = 0, sz = normalized_hashes.size(); j < sz; ++j) {
            const int64_t hash = normalized_hashes[j].hash;
            const int64_t mask = normalized_hashes[j].mask;
            Vec8q target_hash(hash); // broadcast hash
            Vec8q target_mask(mask); // broadcast mask
            hit_mask |= (current_hashes_vec & target_mask) == target_hash;

        }

        for (int k = 0; k < 8; ++k) {
            const int64_t cv = rows[i + k];
            rows[i + k] = hit_mask[k] ? cv : 0;
        }
    }

    for (; i < rows_count; ++i) {
        const int64_t current_hash = hashes[i];
        bool hit = false;
        for (size_t j = 0, sz = normalized_hashes.size(); j < sz; ++j) {
            const int64_t hash = normalized_hashes[j].hash;
            const int64_t mask = normalized_hashes[j].mask;
            hit |= (current_hash & mask) == hash;
        }
        const int64_t cv = rows[i];
        rows[i] = hit ? cv : 0;
    }

    std::partition(rows, rows + rows_count, [=](int64_t n) { return n == 0; });
}
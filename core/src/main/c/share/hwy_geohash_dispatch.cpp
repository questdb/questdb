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

#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "hwy_geohash_dispatch.cpp"
#include "hwy/foreach_target.h"
#include "hwy/highway.h"

#include "util.h"
#include "bitmap_index_utils.h"

HWY_BEFORE_NAMESPACE();
namespace questdb_geo {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

HWY_INLINE uint64_t LoadMaskWord(const uint8_t *mask_bits, size_t byte_count) {
    uint64_t bits = 0;
    for (size_t byte = 0; byte < byte_count; ++byte) {
        bits |= static_cast<uint64_t>(mask_bits[byte]) << (byte * 8);
    }
    return bits;
}

HWY_INLINE void CompactMatchingRows(
        const uint8_t *mask_bits,
        size_t lane_count,
        const int64_t *input_rows,
        int64_t *rows,
        int64_t &out_count
) {
    auto append_matches = [&](uint64_t bits, size_t lane_base) {
        while (bits) {
            const size_t idx = lane_base + static_cast<size_t>(__builtin_ctzll(bits));
            rows[out_count++] = input_rows[idx];
            bits &= bits - 1;
        }
    };

    const size_t mask_bytes = (lane_count + 7) / 8;
    if (mask_bytes <= sizeof(uint64_t)) {
        uint64_t bits = LoadMaskWord(mask_bits, mask_bytes);
        if (lane_count < 64) {
            bits &= (uint64_t{1} << lane_count) - 1;
        }
        append_matches(bits, 0);
        return;
    }

    const size_t full_words = lane_count / 64;
    for (size_t word = 0; word < full_words; ++word) {
        append_matches(LoadMaskWord(mask_bits + word * 8, sizeof(uint64_t)), word * 64);
    }

    const size_t tail_bits = lane_count & 63u;
    if (tail_bits != 0) {
        uint64_t bits = LoadMaskWord(mask_bits + full_words * 8, (tail_bits + 7) / 8);
        bits &= (uint64_t{1} << tail_bits) - 1;
        append_matches(bits, full_words * 64);
    }
}

// ---------------------------------------------------------------------------
// simd_iota: fill array with sequential values starting from `start`
// ---------------------------------------------------------------------------

void SIMDIota(int64_t *array, int64_t array_size, int64_t start) {
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    auto vec = hn::Iota(d, start);
    const auto step = hn::Set(d, static_cast<int64_t>(N));

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= array_size; i += static_cast<int64_t>(N)) {
        hn::StoreU(vec, d, array + i);
        vec = hn::Add(vec, step);
    }

    int64_t next = start + i;
    for (; i < array_size; ++i) {
        array[i] = next++;
    }
}

// ---------------------------------------------------------------------------
// filter_with_prefix: filter geohash values by matching against prefixes
// ---------------------------------------------------------------------------

template<typename T>
void FilterWithPrefixGeneric(
        const T *hashes,
        int64_t *rows,
        int64_t rows_count,
        const int64_t *prefixes,
        int64_t prefixes_count,
        int64_t *out_filtered_count
) {
    const hn::ScalableTag<T> d;
    const size_t N = hn::Lanes(d);
    int64_t i = 0;
    int64_t o = 0;

    // SIMD loop
    for (; i + static_cast<int64_t>(N) <= rows_count; i += static_cast<int64_t>(N)) {
        MM_PREFETCH_T0(rows + i + 64);

        // Gather hash values at row positions (indirect access through rows array)
        HWY_ALIGN T current_hashes[HWY_MAX_LANES_D(decltype(d))];
        for (size_t j = 0; j < N; ++j) {
            current_hashes[j] = hashes[to_local_row_id(rows[i + j] - 1)];
        }
        auto current_vec = hn::Load(d, current_hashes);

        auto hit_mask = hn::FirstN(d, 0); // all false
        for (int64_t j = 0, sz = prefixes_count / 2; j < sz; ++j) {
            const T hash = static_cast<T>(prefixes[2 * j]);
            const T mask = static_cast<T>(prefixes[2 * j + 1]);
            auto target_hash = hn::Set(d, hash);
            auto target_mask = hn::Set(d, mask);
            hit_mask = hn::Or(hit_mask, hn::Eq(hn::And(current_vec, target_mask), target_hash));
        }

        // Extract matching rows using StoreMaskBits
        uint8_t mask_bits[HWY_MAX_LANES_D(decltype(d)) / 8 + 1];
        hn::StoreMaskBits(d, hit_mask, mask_bits);

        CompactMatchingRows(mask_bits, N, rows + i, rows, o);
    }

    // Scalar tail
    for (; i < rows_count; ++i) {
        const T current_hash = hashes[to_local_row_id(rows[i] - 1)];
        bool hit = false;
        for (int64_t j = 0, sz = prefixes_count / 2; j < sz; ++j) {
            const T hash = static_cast<T>(prefixes[2 * j]);
            const T mask = static_cast<T>(prefixes[2 * j + 1]);
            hit |= (current_hash & mask) == hash;
        }
        if (hit) rows[o++] = rows[i];
    }
    *out_filtered_count = o;
}

void FilterWithPrefix(
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
            FilterWithPrefixGeneric<int8_t>(
                    static_cast<const int8_t *>(hashes),
                    rows, rows_count, prefixes, prefixes_count, out_filtered_count
            );
            break;
        case 2:
            FilterWithPrefixGeneric<int16_t>(
                    static_cast<const int16_t *>(hashes),
                    rows, rows_count, prefixes, prefixes_count, out_filtered_count
            );
            break;
        case 4:
            FilterWithPrefixGeneric<int32_t>(
                    static_cast<const int32_t *>(hashes),
                    rows, rows_count, prefixes, prefixes_count, out_filtered_count
            );
            break;
        case 8:
            FilterWithPrefixGeneric<int64_t>(
                    static_cast<const int64_t *>(hashes),
                    rows, rows_count, prefixes, prefixes_count, out_filtered_count
            );
            break;
        default:
            break;
    }
}

}  // namespace HWY_NAMESPACE
}  // namespace questdb_geo
HWY_AFTER_NAMESPACE();

// ---------------------------------------------------------------------------
// Dispatch tables and C-linkage wrappers (compiled once)
// ---------------------------------------------------------------------------

#if HWY_ONCE

namespace questdb_geo {

HWY_EXPORT(SIMDIota);
HWY_EXPORT(FilterWithPrefix);

}  // namespace questdb_geo

using namespace questdb_geo;

extern "C" {

void simd_iota(int64_t *array, int64_t array_size, int64_t start) {
    HWY_DYNAMIC_DISPATCH(SIMDIota)(array, array_size, start);
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
    HWY_DYNAMIC_DISPATCH(FilterWithPrefix)(
            hashes, rows, hashes_type_size, rows_count,
            prefixes, prefixes_count, out_filtered_count
    );
}

}  // extern "C"

#endif  // HWY_ONCE

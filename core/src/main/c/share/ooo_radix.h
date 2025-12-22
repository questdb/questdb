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

#ifndef QUESTDB_OOO_RADIX_H
#define QUESTDB_OOO_RADIX_H

#include <cstring>
#include <cassert>
#include "ooo_dispatch.h"
#include "util.h"
#include <algorithm>
#include "simd.h"
#include "ooo.h"

#define assertm(exp, msg) assert(((void)msg, exp))

template<typename TRevIdx>
void radix_shuffle_clean(
        uint64_t *counts, const index_tr<TRevIdx> *src, index_l *dest, const uint64_t size,
        int64_t min_value, uint8_t result_format, uint16_t sh, uint16_t txn_bits
) {
    MM_PREFETCH_T0(counts);

    if (result_format == shuffle_index_format) {
        // This index is used when there is not deduplication involved
        // The format is the normal suffle index +
        // 1. row count
        // 2. reverse index
        auto row_count_addr = reinterpret_cast<uint64_t *>(&dest[size]);
        auto dest_rev_idx = reinterpret_cast<TRevIdx *>(&row_count_addr[1]);

        for (uint64_t x = 0; x < size; x++) {
            const auto digit = (src[x].ts >> sh) & 0xffu;
            dest[counts[digit]].ts = (int64_t) (min_value + (src[x].ts >> txn_bits));
            dest[counts[digit]].i = src[x].i.i;
            dest_rev_idx[src[x].i.ri] = counts[digit];
            counts[digit]++;
            MM_PREFETCH_T2(src + x + 64);
        }
        row_count_addr[0] = size;
    } else {
        // This index format is used for deduplication
        // Every record contains an absolute index to copy the records from
        auto dest_raw = reinterpret_cast<index_tr<TRevIdx> * >(dest);
        for (uint64_t x = 0; x < size; x++) {
            const auto digit = (src[x].ts >> sh) & 0xffu;
            dest_raw[counts[digit]].ts = (int64_t) (min_value + (src[x].ts >> txn_bits));
            dest_raw[counts[digit]].i = src[x].i;
            counts[digit]++;
            MM_PREFETCH_T2(src + x + 64);
        }
    }
}

template<typename T>
inline void
radix_shuffle(uint64_t *counts, const T *src, index_l *dest, const uint64_t size, int64_t min_value, uint16_t sh) {
    MM_PREFETCH_T0(counts);

    for (uint64_t x = 0; x < size; x++) {
        const auto digit = (src[x] >> sh) & 0xffu;
        dest[counts[digit]].ts = (int64_t) (min_value + src[x].ts);
        dest[counts[digit]].i = src[x].i;
        counts[digit]++;
        MM_PREFETCH_T2(src + x + 64);
    }
}

template<typename T>
inline void radix_shuffle(uint64_t *counts, const T *src, T *dest, const uint64_t size, uint16_t sh) {
    MM_PREFETCH_T0(counts);
    for (uint64_t x = 0; x < size; x++) {
        const auto digit = (src[x] >> sh) & 0xffu;
        dest[counts[digit]] = src[x];
        counts[digit]++;
        MM_PREFETCH_T2(src + x + 64);
    }
}

template<uint16_t n, typename TRevIdx>
int64_t
radix_sort_segments_index_asc(
        const int64_t *lag_ts, const uint64_t lag_size,
        const index_l **segment_ts_maps,
        const txn_info *segment_txns,
        const uint64_t txn_count,
        index_l *out,
        index_l *cpy,
        const uint32_t segment_count,
        int64_t min_value,
        uint8_t result_format,
        uint16_t segment_bits,
        uint16_t ts_bits,
        uint16_t txn_bits
) {
    // Reverse index type must be an unsigned integer.
    static_assert(std::is_integral_v<TRevIdx> && std::is_unsigned_v<TRevIdx>, "TRevIdx must be a signed integer");
    assertm(n == (ts_bits + txn_bits + 7) >> 3, "n must be equal to (ts_bits + txn_bits + 7) >> 3");

    static_assert(n <= 8 && n > 0, "invalid byte range to sort");

    uint64_t counts[n][256] = {{0}};
    uint64_t o[n] = {0};
    uint64_t x;

    index_tr<TRevIdx> *buff1;
    index_tr<TRevIdx> *buff2;
    if constexpr (n % 2 == 0) {
        buff1 = (index_tr<TRevIdx> *) out;
        buff2 = (index_tr<TRevIdx> *) cpy;
    } else {
        // invert out and copy, so that after n iterations data ends up in out
        buff1 = (index_tr<TRevIdx> *) cpy;
        buff2 = (index_tr<TRevIdx> *) out;
    }

    // calculate counts
    for (x = 0; x < lag_size; x++) {
        buff1[x].ts = (lag_ts[x] - min_value) << txn_bits;
        buff1[x].i.i = x << segment_bits | segment_count;
        buff1[x].i.ri = (TRevIdx) x;
        constexpr_for<0, n, 1>(
                [&](auto i) {
                    constexpr uint64_t shift = 8u * (n - i - 1);
                    const auto t0 = (buff1[x].ts >> shift) & 0xffu;
                    counts[i][t0]++;
                }
        );
        MM_PREFETCH_T2(lag_ts + x + 64);
    }

    for (uint64_t txn_index = 0; txn_index < txn_count; txn_index++) {
        auto segment_index = segment_txns[txn_index].seg_info_index;
        auto seq_txn = segment_txns[txn_index].seq_txn;

        const uint64_t hi = segment_txns[txn_index].segment_row_offset + segment_txns[txn_index].row_count;
        for (uint64_t seg_row = segment_txns[txn_index].segment_row_offset; seg_row < hi; seg_row++, x++) {
            buff1[x].ts =
                    (((uint64_t) (segment_ts_maps[segment_index][seg_row].ts - min_value)) << txn_bits) | seq_txn;
            buff1[x].i.i = (seg_row << segment_bits) | segment_index;
            buff1[x].i.ri = (TRevIdx) x;

            constexpr_for<0, n, 1>(
                    [&](auto i) {
                        constexpr uint64_t shift = 8u * (n - i - 1);
                        const auto t0 = (buff1[x].ts >> shift) & 0xffu;
                        counts[i][t0]++;
                    }
            );
        }
    }
    auto size = x;

    // convert counts to offsets
    MM_PREFETCH_T0(&counts);
    for (int xx = 0; xx < 256; xx++) {
        // should be unrolled by compiler, n is a constexpr
        constexpr_for<0, n, 1>(
                [&](auto i) {
                    auto t0 = o[i] + counts[i][xx];
                    counts[i][xx] = o[i];
                    o[i] = t0;
                }
        );
    }

    if constexpr (n > 1) {
        radix_shuffle(counts[n - 1], buff1, buff2, size, 0u);
        if constexpr (n > 2) {
            radix_shuffle(counts[n - 2], buff2, buff1, size, 8u);
            if constexpr (n > 3) {
                radix_shuffle(counts[n - 3], buff1, buff2, size, 16u);
                if constexpr (n > 4) {
                    radix_shuffle(counts[n - 4], buff2, buff1, size, 24u);
                    if constexpr (n > 5) {
                        radix_shuffle(counts[n - 5], buff1, buff2, size, 32u);
                        if constexpr (n > 6) {
                            radix_shuffle(counts[n - 6], buff2, buff1, size, 40u);
                            if constexpr (n > 7) {
                                radix_shuffle(counts[n - 7], buff1, buff2, size, 48u);
                                radix_shuffle_clean<TRevIdx>(
                                        counts[n - 8], buff2, out, size,
                                        min_value, result_format, 56u, txn_bits);
                            } else {
                                radix_shuffle_clean<TRevIdx>(
                                        counts[n - 7], buff1, out, size,
                                        min_value, result_format, 48u, txn_bits);
                            }
                        } else {
                            radix_shuffle_clean<TRevIdx>(
                                    counts[n - 6], buff2, out, size, min_value, result_format, 40u, txn_bits);
                        }
                    } else {
                        radix_shuffle_clean<TRevIdx>(
                                counts[n - 5], buff1, out, size, min_value, result_format, 32u, txn_bits);
                    }
                } else {
                    radix_shuffle_clean<TRevIdx>(
                            counts[n - 4], buff2, out, size, min_value, result_format, 24u, txn_bits);
                }
            } else {
                radix_shuffle_clean<TRevIdx>(
                        counts[n - 3], buff1, out, size, min_value, result_format, 16u, txn_bits);
            }
        } else {
            radix_shuffle_clean<TRevIdx>(counts[n - 2], buff2, out, size, min_value, result_format, 8u, txn_bits);
        }
    } else {
        radix_shuffle_clean<TRevIdx>(counts[n - 1], buff1, out, size, min_value, result_format, 0u, txn_bits);
    }
    return (int64_t) size;
}


template<uint16_t N>
int64_t radix_sort_segments_index_asc_rev(
        const int64_t *lag_ts,
        const uint64_t lag_size,
        const index_l **segment_ts_maps,
        const txn_info *segment_txns,
        const uint64_t txn_count,
        index_l *out,
        index_l *cpy,
        const uint32_t segment_count,
        int64_t min_value,
        int32_t total_row_count_bytes,
        int64_t total_row_count,
        uint8_t result_format,
        uint16_t segment_bits,
        uint16_t ts_bits,
        uint16_t txn_bits
) {
    if (total_row_count_bytes != integral_type_bytes(range_bytes(total_row_count + 1))) {
        // Error, invalid total row count bytes
        return -100;
    }

    switch (total_row_count_bytes) {
        case 1u:
            return radix_sort_segments_index_asc<N, uint8_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                    min_value, result_format, segment_bits, ts_bits, txn_bits
            );
        case 2u:
            return radix_sort_segments_index_asc<N, uint16_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                    min_value, result_format, segment_bits, ts_bits, txn_bits
            );
        case 4u:
            return radix_sort_segments_index_asc<N, uint32_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                    min_value, result_format, segment_bits, ts_bits, txn_bits
            );
        case 8u:
            return radix_sort_segments_index_asc<N, uint64_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                    min_value, result_format, segment_bits, ts_bits, txn_bits
            );
        default:
            // Error, unsupported type
            return -1;
    }
}

inline int64_t radix_sort_segments_index_asc_precompiled(
        uint16_t ts_bits, uint16_t txn_bits, uint16_t segment_bits,
        const int64_t *lag_ts, const uint64_t lag_size,
        const index_l **segment_ts, const txn_info *segment_txns,
        const uint64_t txn_count, index_l *out, index_l *cpy,
        const uint32_t segment_count,
        int64_t min_value,
        int32_t total_row_count_bytes,
        int64_t total_row_count,
        uint8_t result_format
) {
    auto n = (ts_bits + txn_bits + 7) >> 3;
    switch (n) {
        case 1:
            return radix_sort_segments_index_asc_rev<1>(
                    lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format,
                    segment_bits, ts_bits, txn_bits
            );
        case 2:
            return radix_sort_segments_index_asc_rev<2>(
                    lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format,
                    segment_bits, ts_bits, txn_bits
            );
        case 3:
            return radix_sort_segments_index_asc_rev<3>(
                    lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format,
                    segment_bits, ts_bits, txn_bits
            );
        case 4:
            return radix_sort_segments_index_asc_rev<4>(
                    lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format,
                    segment_bits, ts_bits, txn_bits
            );
        case 5:
            return radix_sort_segments_index_asc_rev<5>(
                    lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format,
                    segment_bits, ts_bits, txn_bits
            );
        case 6:
            return radix_sort_segments_index_asc_rev<6>(
                    lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format,
                    segment_bits, ts_bits, txn_bits
            );
        case 7:
            return radix_sort_segments_index_asc_rev<7>(
                    lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format,
                    segment_bits, ts_bits, txn_bits
            );
        case 8:
            return radix_sort_segments_index_asc_rev<8>(
                    lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format,
                    segment_bits, ts_bits, txn_bits
            );
        default:
            // Error, total radix bytes should be 0-8
            return -1;
    }
}

template<typename T, typename TIdx>
int64_t merge_shuffle_fixed_columns_by_rev_index(
        const T **src_addresses,
        T *dst_address,
        const void *reverse_index_ptr,
        const seg_info *segments,
        int64_t segment_count,
        uint8_t merge_format
) {
    static_assert(std::is_integral_v<TIdx> && std::is_unsigned_v<TIdx>, "T must be an unsigned integer");
    const TIdx *reverse_index = reinterpret_cast<const TIdx *>(reverse_index_ptr);

    int64_t rev_row_index = 0;
    int64_t dups = 0;
    for (int64_t seg = 0; seg < segment_count; seg++) {
        auto src = src_addresses[seg];
        auto segment_lo = segments[seg].segment_lo;
        auto segment_hi = std::abs(segments[seg].segment_hi);

        for (int64_t r = segment_lo; r < segment_hi; r++) {
            auto dst_index = reverse_index[rev_row_index++];
            if (merge_format == dedup_shuffle_index_format) {
                if (dst_index == 0) {
                    // 0 means this row is not in the result set
                    dups++;
                    continue;
                }
                // rows shifted by 1
                dst_index--;
            }
            dst_address[dst_index] = src[r];
        }
    }
    return rev_row_index - dups;
}

template<typename T>
int64_t merge_shuffle_fixed_columns_by_rev_index_from_many_addresses(
        int32_t reverse_index_format_bytes, const void **src_addresses,
        void *dst_address, const void *reverse_index,
        const seg_info *segments, int64_t segment_count,
        uint8_t merge_index_format
) {
    auto src = reinterpret_cast<const T **>(src_addresses);
    auto dst = reinterpret_cast<T *>(dst_address);

    switch (reverse_index_format_bytes) {
        case 1:
            return merge_shuffle_fixed_columns_by_rev_index<T, uint8_t>(
                    src, dst,
                    reverse_index, segments,
                    segment_count,
                    merge_index_format
            );
        case 2:
            return merge_shuffle_fixed_columns_by_rev_index<T, uint16_t>(
                    src, dst,
                    reverse_index, segments,
                    segment_count,
                    merge_index_format
            );
        case 4:
            return merge_shuffle_fixed_columns_by_rev_index<T, uint32_t>(
                    src, dst,
                    reverse_index, segments,
                    segment_count,
                    merge_index_format
            );
        case 8:
            return merge_shuffle_fixed_columns_by_rev_index<T, uint64_t>(
                    src, dst,
                    reverse_index, segments,
                    segment_count,
                    merge_index_format
            );
        default:
            // Error, unsupported index_format type
            return -1;
    }

}


template<typename T>
inline int64_t merge_shuffle_string_column_from_many_addresses(
        const int32_t index_segment_encoding_bits,
        const char **src_primary,
        const int64_t **src_secondary,
        char *dst_primary,
        int64_t *dst_secondary,
        const index_l *merge_index_address,
        int64_t row_count,
        int64_t dst_var_offset,
        uint16_t mult,
        int64_t dst_var_size
) {
    int64_t dstVarOffset = dst_var_offset;
    uint64_t segmentMask = (1ULL << index_segment_encoding_bits) - 1;
    static_assert(std::is_integral_v<T> && std::is_signed_v<T>,
                  "string,binary len must be an signed integer");

    for (int64_t l = 0; l < row_count; l++) {
        MM_PREFETCH_T0(merge_index_address + l + 64);
        dst_secondary[l] = dstVarOffset;

        auto index = merge_index_address[l].i;
        auto row_index = index >> index_segment_encoding_bits;
        auto src_index = index & segmentMask;

        const int64_t offset = src_secondary[src_index][row_index];
        const char *srcVarPtr = src_primary[src_index] + offset;

        auto len = *reinterpret_cast<const T *>(srcVarPtr);
        auto char_count = len > 0 ? len * mult : 0;

        reinterpret_cast<T *>(dst_primary + dstVarOffset)[0] = len;
        assertm(dstVarOffset + char_count + sizeof(T) <= dst_var_size, "destination var buffer is too small");

        __MEMCPY(dst_primary + dstVarOffset + sizeof(T), srcVarPtr + sizeof(T), char_count);
        dstVarOffset += char_count + sizeof(T);
    }

    if (row_count > 0) {
        dst_secondary[row_count] = dstVarOffset;
    }
    return dstVarOffset;
}

inline int64_t merge_shuffle_array_column_from_many_addresses(
        const char **src_primary,
        const int64_t **src_secondary,
        char *dst_primary,
        int64_t *dst_secondary,
        const index_l *merge_index,
        int64_t row_count,
        int64_t dst_var_offset,
        uint16_t segment_bits,
        int64_t dst_data_size
) {
    // mask to extract segment ID from merge index
    const uint64_t segment_mask = (1ULL << segment_bits) - 1;
    // mask for the 48-bit offset portion of the first 64 bits
    constexpr uint64_t offset_mask = (1ULL << 48) - 1;
    // mask for the size bits in second word (lower 32 bits)
    const uint64_t size_mask = 0xFFFFFFFF;

    for (int64_t l = 0; l < row_count; l++) {
        auto index = merge_index[l].i;
        auto row_index = index >> segment_bits;
        auto src_index = index & segment_mask;

        // Arrays have 2 aux words per row:
        // 1st words = 48 offset bits + 16 reserved bits
        // 2nd words = 32 size bits + 32 reserved bits
        // NOTE: We do not preserve reserved bits!
        // If you're adding functionality that uses these bits, you have to update this code
        // to handle them appropriately!

        const int64_t src_offset_word = src_secondary[src_index][row_index * 2];
        const int64_t src_size_word = src_secondary[src_index][row_index * 2 + 1];

        auto src_offset = src_offset_word & offset_mask;
        auto src_size = src_size_word & size_mask;

        dst_secondary[l * 2] = dst_var_offset;
        dst_secondary[l * 2 + 1] = src_size;

        // only copy data if the array is not NULL (size > 0)
        if (src_size > 0) {
            assertm(dst_var_offset + src_size <= dst_data_size, "destination array buffer is too small");
            __MEMCPY(dst_primary + dst_var_offset, src_primary[src_index] + src_offset, src_size);
            dst_var_offset += src_size;
        }
    }
    return dst_var_offset;
}

int64_t merge_shuffle_varchar_column_from_many_addresses(
        const char **src_primary,
        const int64_t **src_secondary,
        char *dst_primary,
        int64_t *dst_secondary,
        const index_l *merge_index,
        int64_t row_count,
        int64_t dst_var_offset,
        uint16_t segment_bits,
        int64_t dst_data_size
) {
    const uint64_t segment_mask = (1ULL << segment_bits) - 1;

    for (int64_t l = 0; l < row_count; l++) {
        auto index = merge_index[l].i;
        auto row_index = index >> segment_bits;
        auto src_index = index & segment_mask;

        const int64_t first_word = src_secondary[src_index][row_index * 2];
        const int64_t second_word = src_secondary[src_index][row_index * 2 + 1];

        auto original_data = second_word & 0x000000000000ffffLL;
        auto rellocated_second_word = original_data | (dst_var_offset << 16);
        if ((first_word & 1) == 0 && (first_word & 4) == 0) {
            // not inlined and not null
            auto original_offset = second_word >> 16;
            auto len = (first_word >> 4) & 0xffffff;
            auto data = src_primary[src_index] + original_offset;
            assertm(dst_var_offset + len <= dst_data_size, "destination var buffer is too small");

            __MEMCPY(dst_primary + dst_var_offset, data, len);
            dst_var_offset += len;
        }
        dst_secondary[l * 2] = first_word;
        dst_secondary[l * 2 + 1] = rellocated_second_word;
    }
    return dst_var_offset;
}

template<typename TIdx>
jlong merge_shuffle_symbol_column_from_many_addresses(
        const int32_t **src,
        int32_t *dst,
        const txn_info *segment_txns,
        uint64_t txn_count,
        const int32_t *symbol_map,
        const void *reverse_index_ptr,
        int64_t reverse_index_row_count,
        uint8_t merge_format
) {
    static_assert(std::is_integral_v<TIdx> && std::is_unsigned_v<TIdx>, "T must be an unsigned integer");
    const TIdx *reverse_index = reinterpret_cast<const TIdx *>(reverse_index_ptr);

    int64_t out_index = 0;
    jlong rows_processed = 0;
    for (uint64_t txn_index = 0; txn_index < txn_count; txn_index++) {
        auto segment_addr = src[segment_txns[txn_index].seg_info_index];
        uint64_t hi = segment_txns[txn_index].segment_row_offset + segment_txns[txn_index].row_count;
        int32_t clean_symbol_count = symbol_map[2 * txn_index];
        int32_t map_offset = symbol_map[2 * txn_index + 1];

        for (uint64_t seg_row = segment_txns[txn_index].segment_row_offset; seg_row < hi; seg_row++, out_index++) {
            int dst_index = reverse_index[out_index];
            if (merge_format == dedup_shuffle_index_format) {
                if (dst_index == 0) {
                    // 0 means this row is not in the result set
                    continue;
                }
                // rows shifted by 1
                dst_index--;
            }
            assertm(dst_index >= 0 && dst_index < reverse_index_row_count, "reverse_index buggy");

            int32_t value = segment_addr[seg_row];
            if (value >= clean_symbol_count) {
                auto value2 = symbol_map[map_offset + value - clean_symbol_count];
                dst[dst_index] = value2;
            } else {
                dst[dst_index] = value;
            }
            rows_processed++;
        }
    }
    return rows_processed;
}

template<typename TIdx>
jlong merge_shuffle_symbol_column_by_reverse_index(
        const int32_t *src,
        int32_t *dst,
        const void *reverse_index_ptr,
        int64_t reverse_index_row_count,
        uint8_t merge_format
) {
    static_assert(std::is_integral_v<TIdx> && std::is_unsigned_v<TIdx>, "T must be an unsigned integer");
    const TIdx *reverse_index = reinterpret_cast<const TIdx *>(reverse_index_ptr);

    int64_t dups = 0;
    for (int64_t r_index = 0; r_index < reverse_index_row_count; r_index++) {
        auto dst_index = reverse_index[r_index];
        if (merge_format == dedup_shuffle_index_format) {
            if (dst_index == 0) {
                // 0 means this row is not in the result set
                dups++;
                continue;
            }
            // rows shifted by 1
            dst_index--;
        }

        dst[dst_index] = src[r_index];
    }
    return reverse_index_row_count - dups;
}

#endif //QUESTDB_OOO_RADIX_H

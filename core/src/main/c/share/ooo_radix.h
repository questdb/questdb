/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

template<uint16_t sh, uint16_t txn_bits, typename TRevIdx, uint8_t result_format>
inline void radix_shuffle_clean(
        uint64_t *counts, const index_tr<TRevIdx> *src, index_l *dest, const uint64_t size,
        int64_t min_value
) {
    MM_PREFETCH_T0(counts);

    if constexpr (result_format == SHUFFLE_INDEX_FORMAT) {
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

template<uint16_t sh, typename T>
inline void radix_shuffle(uint64_t *counts, const T *src, index_l *dest, const uint64_t size, int64_t min_value) {
    MM_PREFETCH_T0(counts);

    for (uint64_t x = 0; x < size; x++) {
        const auto digit = (src[x] >> sh) & 0xffu;
        dest[counts[digit]].ts = (int64_t) (min_value + src[x].ts);
        dest[counts[digit]].i = src[x].i;
        counts[digit]++;
        MM_PREFETCH_T2(src + x + 64);
    }
}

template<uint16_t sh, typename T>
inline void radix_shuffle(uint64_t *counts, const T *src, T *dest, const uint64_t size) {
    MM_PREFETCH_T0(counts);
    for (uint64_t x = 0; x < size; x++) {
        const auto digit = (src[x] >> sh) & 0xffu;
        dest[counts[digit]] = src[x];
        counts[digit]++;
        MM_PREFETCH_T2(src + x + 64);
    }
}

template<uint16_t ts_bits, uint16_t txn_bits, uint16_t segment_bits, typename TRevIdx, uint8_t result_format>
uint64_t
radix_copy_segments_index_asc(
        const int64_t *lag_ts, const uint64_t lag_size,
        const index_l **segment_ts_maps,
        const txn_info *segment_txns,
        const uint64_t txn_count,
        index_l *out,
        index_l *cpy,
        const uint32_t segment_count,
        int64_t min_value
) {
    // Reverse index type must be an unsigned integer.
    static_assert(std::is_integral_v<TRevIdx> && std::is_unsigned_v<TRevIdx>, "TRevIdx must be a signed integer");
    constexpr uint16_t n = (ts_bits + txn_bits + 7) >> 3;

    // Some combinations of template pre-generated code is not valid, do nothing in that case
    if constexpr (n <= 8 && n > 0) {

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
        for (int x = 0; x < 256; x++) {
            // should be unrolled by compiler, n is a compile time const
            constexpr_for<0, n, 1>(
                    [&](auto i) {
                        auto t0 = o[i] + counts[i][x];
                        counts[i][x] = o[i];
                        o[i] = t0;
                    }
            );
        }

        if constexpr (n > 1) {
            radix_shuffle<0u>(counts[n - 1], buff1, buff2, size);
            if constexpr (n > 2) {
                radix_shuffle<8u>(counts[n - 2], buff2, buff1, size);
                if constexpr (n > 3) {
                    radix_shuffle<16u>(counts[n - 3], buff1, buff2, size);
                    if constexpr (n > 4) {
                        radix_shuffle<24u>(counts[n - 4], buff2, buff1, size);
                        if constexpr (n > 5) {
                            radix_shuffle<32u>(counts[n - 5], buff1, buff2, size);
                            if constexpr (n > 6) {
                                radix_shuffle<40u>(counts[n - 6], buff2, buff1, size);
                                if constexpr (n > 7) {
                                    radix_shuffle<48u>(counts[n - 7], buff1, buff2, size);
                                    radix_shuffle_clean<56u, txn_bits, TRevIdx, result_format>(
                                            counts[n - 8], buff2, out, size,
                                            min_value);
                                } else {
                                    radix_shuffle_clean<48u, txn_bits, TRevIdx, result_format>(
                                            counts[n - 7], buff1, out, size,
                                            min_value);
                                }
                            } else {
                                radix_shuffle_clean<40u, txn_bits, TRevIdx, result_format>(
                                        counts[n - 6], buff2, out, size, min_value);
                            }
                        } else {
                            radix_shuffle_clean<32u, txn_bits, TRevIdx, result_format>(
                                    counts[n - 5], buff1, out, size, min_value);
                        }
                    } else {
                        radix_shuffle_clean<24u, txn_bits, TRevIdx, result_format>(
                                counts[n - 4], buff2, out, size, min_value);
                    }
                } else {
                    radix_shuffle_clean<16u, txn_bits, TRevIdx, result_format>(
                            counts[n - 3], buff1, out, size, min_value);
                }
            } else {
                radix_shuffle_clean<8u, txn_bits, TRevIdx, result_format>(counts[n - 2], buff2, out, size, min_value);
            }
        } else {
            radix_shuffle_clean<0u, txn_bits, TRevIdx, result_format>(counts[n - 1], buff1, out, size, min_value);
        }
        return size;
    }
    return 0;
}


template<uint16_t ts_bits, uint16_t txn_bits, uint16_t segment_bits>
uint64_t
radix_copy_segments_index_asc_rev(
        const int64_t *lag_ts, const uint64_t lag_size,
        const index_l **segment_ts_maps,
        const txn_info *segment_txns,
        const uint64_t txn_count,
        index_l *out,
        index_l *cpy,
        const uint32_t segment_count,
        int64_t min_value,
        int32_t total_row_count_bytes,
        int64_t total_row_count,
        uint8_t result_format
) {
    if (total_row_count_bytes != integral_type_bytes(range_bytes(total_row_count + 1))) {
        return -100;
    }

    switch (result_format) {
        case SHUFFLE_INDEX_FORMAT:
            switch (total_row_count_bytes) {
                case 1u:
                    return radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint8_t, SHUFFLE_INDEX_FORMAT>(
                            lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                            min_value
                    );
                case 2u:
                    return radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint16_t, SHUFFLE_INDEX_FORMAT>(
                            lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                            min_value
                    );
                case 4u:
                    return radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint32_t, SHUFFLE_INDEX_FORMAT>(
                            lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                            min_value
                    );
                case 8u:
                    return radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint64_t, SHUFFLE_INDEX_FORMAT>(
                            lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                            min_value
                    );
            }
            break;

        case DEDUP_INDEX_FORMAT:
            switch (total_row_count_bytes) {
                case 1u:
                    return radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint8_t, DEDUP_INDEX_FORMAT>(
                            lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                            min_value
                    );
                case 2u:
                    return radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint16_t, DEDUP_INDEX_FORMAT>(
                            lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                            min_value
                    );
                case 4u:
                    return radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint32_t, DEDUP_INDEX_FORMAT>(
                            lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                            min_value
                    );
                case 8u:
                    return radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint64_t, DEDUP_INDEX_FORMAT>(
                            lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                            min_value
                    );
            }
            break;
    }
    return 0;
}

template<uint16_t ts_bits, uint16_t txn_bits>
uint64_t radix_copy_segments_index_asc_dispatch_segment_bits(
        uint16_t segment_bits,
        const int64_t *lag_ts, const uint64_t lag_size,
        const index_l **segment_ts, const txn_info *segment_txns,
        const uint64_t txn_count, index_l *out, index_l *cpy,
        const uint32_t segment_count,
        int64_t min_value,
        int32_t total_row_count_bytes,
        int64_t total_row_count,
        uint8_t result_format
) {
    switch (segment_bits) {
        case 0:
            return radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 0u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 8:
            return radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 8u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 16:
            return radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 16u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 24:
            return radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 24u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 32:
            return radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 32u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 40:
            return radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 40u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 48:
            return radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 48u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 56:
            return radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 56u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes,
                    total_row_count,
                    result_format
            );
    }
    return 0;
}

template<uint16_t ts_bits>
inline uint64_t radix_copy_segments_index_asc_dispatch_txn_segment_bits(
        const uint16_t txn_bits, const uint16_t segment_bits,
        const int64_t *lag_ts, const uint64_t lag_size,
        const index_l **segment_ts, const txn_info *segment_txns,
        const uint64_t txn_count, index_l *out, index_l *cpy,
        const uint32_t segment_count,
        int64_t min_value,
        int total_row_count_bytes,
        int64_t total_row_count,
        uint8_t result_format
) {
    switch (txn_bits) {
        case 0:
            return radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 0u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts,
                    segment_txns, txn_count, out, cpy,
                    segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 8:
            return radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 8u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts,
                    segment_txns, txn_count, out, cpy,
                    segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 16:
            return radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 16u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 24:
            return radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 24u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 32:
            return radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 32u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 40:
            return radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 40u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 48:
            return radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 48u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 56:
            return radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 56u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
    }
    return 0;
}

inline uint64_t radix_copy_segments_index_asc_precompiled(uint16_t ts_bits, uint16_t txn_bits, uint16_t segment_bits,
                                                          const int64_t *lag_ts, const uint64_t lag_size,
                                                          const index_l **segment_ts, const txn_info *segment_txns,
                                                          const uint64_t txn_count, index_l *out, index_l *cpy,
                                                          const uint32_t segment_count,
                                                          int64_t min_value,
                                                          int32_t total_row_count_bytes,
                                                          int64_t total_row_count,
                                                          uint8_t result_format
) {
    switch (ts_bits) {
        case 0:
            return radix_copy_segments_index_asc_dispatch_txn_segment_bits<0u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format);
        case 8:
            return radix_copy_segments_index_asc_dispatch_txn_segment_bits<8u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format);
        case 16:
            return radix_copy_segments_index_asc_dispatch_txn_segment_bits<16u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format);
        case 24:
            return radix_copy_segments_index_asc_dispatch_txn_segment_bits<24u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format);
        case 32:
            return radix_copy_segments_index_asc_dispatch_txn_segment_bits<32u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format);
        case 40:
            return radix_copy_segments_index_asc_dispatch_txn_segment_bits<40u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format);
        case 48:
            return radix_copy_segments_index_asc_dispatch_txn_segment_bits<48u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format);
        case 56:
            return radix_copy_segments_index_asc_dispatch_txn_segment_bits<56u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes, total_row_count, result_format);
    }
    return 0;
}

template<typename T, uint16_t segment_bits>
void merge_shuffle_column_from_many_addresses_segment_bits(const T **src_addresses, T *dst_address,
                                                           const index_l *merge_index_address, uint64_t row_count) {
    constexpr uint64_t mask = (1ULL << segment_bits) - 1;

    for (uint64_t i = 0; i < row_count; i++) {
        auto index = merge_index_address[i].i;
        auto row_index = index >> segment_bits;
        auto src_index = index & mask;

        dst_address[i] = src_addresses[src_index][row_index];
    }
}

template<typename T>
int64_t merge_shuffle_column_from_many_addresses(
        int32_t index_segment_encoding_bytes, const void **src_addresses,
        void *dst_address, const index_l *merge_index_address,
        int64_t row_count
) {
    auto src = reinterpret_cast<const T **>(src_addresses);
    auto dst = reinterpret_cast<T *>(dst_address);

    switch (index_segment_encoding_bytes) {
        case 0:
            merge_shuffle_column_from_many_addresses_segment_bits<T, 0u>(src, dst, merge_index_address, row_count);
            break;
        case 1:
            merge_shuffle_column_from_many_addresses_segment_bits<T, 8u>(src, dst, merge_index_address, row_count);
            break;
        case 2:
            merge_shuffle_column_from_many_addresses_segment_bits<T, 16u>(src, dst, merge_index_address, row_count);
            break;
        case 3:
            merge_shuffle_column_from_many_addresses_segment_bits<T, 24u>(src, dst, merge_index_address, row_count);
            break;
        case 4:
            merge_shuffle_column_from_many_addresses_segment_bits<T, 32u>(src, dst, merge_index_address, row_count);
            break;
        case 5:
            merge_shuffle_column_from_many_addresses_segment_bits<T, 40u>(src, dst, merge_index_address, row_count);
            break;
        case 6:
            merge_shuffle_column_from_many_addresses_segment_bits<T, 48u>(src, dst, merge_index_address, row_count);
            break;
        case 7:
            merge_shuffle_column_from_many_addresses_segment_bits<T, 56u>(src, dst, merge_index_address, row_count);
            break;
        default:
            return -1;
    }
    return row_count;
}

template<typename T, uint16_t mult, uint16_t segment_bits>
void merge_shuffle_string_column_from_many_addresses_segment_bits(
        const char **src_primary,
        const int64_t **src_secondary,
        char *dst_primary,
        int64_t *dst_secondary,
        const index_l *merge_index,
        int64_t row_count,
        int64_t dst_var_offset
) {
    constexpr uint64_t segment_mask = (1ULL << segment_bits) - 1;

    for (int64_t l = 0; l < row_count; l++) {
        MM_PREFETCH_T0(merge_index + l + 64);
        dst_secondary[l] = dst_var_offset;

        auto index = merge_index[l].i;
        auto row_index = index >> segment_bits;
        auto src_index = index & segment_mask;

        const int64_t offset = src_secondary[src_index][row_index];
        const char *src_var_ptr = src_primary[src_index] + offset;

        auto len = *reinterpret_cast<const T *>(src_var_ptr);
        auto char_count = len > 0 ? len * mult : 0;

        reinterpret_cast<T *>(dst_primary + dst_var_offset)[0] = len;
        __MEMCPY(dst_primary + dst_var_offset + sizeof(T), src_var_ptr + sizeof(T), char_count);
        dst_var_offset += char_count + sizeof(T);
    }

    if (row_count > 0) {
        dst_secondary[row_count] = dst_var_offset;
    }
}

template<typename T, uint16_t mult>
inline int64_t merge_shuffle_string_column_from_many_addresses(const int32_t index_segment_encoding_bytes,
                                                               const char **src_primary,
                                                               const int64_t **src_secondary,
                                                               char *dst_primary,
                                                               int64_t *dst_secondary,
                                                               const index_l *merge_index_address,
                                                               int64_t row_count,
                                                               int64_t dst_var_offset) {
    switch (index_segment_encoding_bytes) {
        case 0:
            merge_shuffle_string_column_from_many_addresses_segment_bits<T, mult, 0u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
            break;
        case 1:
            merge_shuffle_string_column_from_many_addresses_segment_bits<T, mult, 8u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
            break;
        case 2:
            merge_shuffle_string_column_from_many_addresses_segment_bits<T, mult, 16u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
            break;
        case 3:
            merge_shuffle_string_column_from_many_addresses_segment_bits<T, mult, 24u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
            break;
        case 4:
            merge_shuffle_string_column_from_many_addresses_segment_bits<T, mult, 32u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
            break;
        case 5:
            merge_shuffle_string_column_from_many_addresses_segment_bits<T, mult, 40u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
            break;
        case 6:
            merge_shuffle_string_column_from_many_addresses_segment_bits<T, mult, 48u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
            break;
        case 7:
            merge_shuffle_string_column_from_many_addresses_segment_bits<T, mult, 56u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
            break;
        default:
            return -1;
    }
    return row_count;
}

template<uint16_t segment_bits>
void merge_shuffle_varchar_column_from_many_addresses(
        const char **src_primary,
        const int64_t **src_secondary,
        char *dst_primary,
        int64_t *dst_secondary,
        const index_l *merge_index,
        int64_t row_count,
        int64_t dst_var_offset) {
    constexpr uint64_t segment_mask = (1ULL << segment_bits) - 1;

    for (int64_t l = 0; l < row_count; l++) {
        auto index = merge_index[l].i;
        auto row_index = index >> segment_bits;
        auto src_index = index & segment_mask;

        const int64_t firstWord = src_secondary[src_index][row_index * 2];
        const int64_t secondWord = src_secondary[src_index][row_index * 2 + 1];

        auto originalData = secondWord & 0x000000000000ffffLL;
        auto rellocatedSecondWord = originalData | (dst_var_offset << 16);
        if ((firstWord & 1) == 0 && (firstWord & 4) == 0) {
            // not inlined and not null
            auto originalOffset = secondWord >> 16;
            auto len = (firstWord >> 4) & 0xffffff;
            auto data = src_primary[src_index] + originalOffset;
            __MEMCPY(dst_primary + dst_var_offset, data, len);
            dst_var_offset += len;
        }
        dst_secondary[l * 2] = firstWord;
        dst_secondary[l * 2 + 1] = rellocatedSecondWord;
    }
}

template<typename TIdx, uint8_t merge_format>
jlong merge_shuffle_symbol_column_from_many_addresses(
        const int32_t **src,
        int32_t *dst,
        const txn_info *segment_txns,
        uint64_t txn_count,
        const int32_t *symbol_map,
        const void *reverse_index_ptr,
        int64_t reverse_index_row_count
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
            if constexpr (merge_format == DEDUP_SHUFFLE_INDEX_FORMAT) {
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

template<typename TIdx, uint8_t merge_format>
jlong merge_shuffle_symbol_column_by_reverse_index(
        const int32_t *src,
        int32_t *dst,
        const void *reverse_index_ptr,
        int64_t revrese_index_row_count
) {
    static_assert(std::is_integral_v<TIdx> && std::is_unsigned_v<TIdx>, "T must be an unsigned integer");
    const TIdx *reverse_index = reinterpret_cast<const TIdx *>(reverse_index_ptr);

    for (int64_t r_index = 0; r_index < revrese_index_row_count; r_index++) {
        auto dst_index = reverse_index[r_index];
        if constexpr (merge_format == DEDUP_SHUFFLE_INDEX_FORMAT) {
            if (dst_index == 0) {
                // 0 means this row is not in the result set
                continue;
            }
            // rows shifted by 1
            dst_index--;
        }

        dst[dst_index] = src[r_index];
    }
    return revrese_index_row_count;
}


jlong remap_symbol_column_from_many_addresses(
        const int32_t **src,
        int32_t *dst,
        const txn_info *segment_txns,
        uint64_t txn_count,
        const int32_t *symbol_map
) {
    int64_t out_index = 0;
    jlong rows_processed = 0;
    for (uint64_t txn_index = 0; txn_index < txn_count; txn_index++) {
        auto segment_addr = src[segment_txns[txn_index].seg_info_index];
        uint64_t hi = segment_txns[txn_index].segment_row_offset + segment_txns[txn_index].row_count;
        int32_t clean_symbol_count = symbol_map[2 * txn_index];
        int32_t map_offset = symbol_map[2 * txn_index + 1];

        for (uint64_t seg_row = segment_txns[txn_index].segment_row_offset; seg_row < hi; seg_row++, out_index++) {
            int32_t value = segment_addr[seg_row];
            if (value >= clean_symbol_count) {
                auto value2 = symbol_map[map_offset + value - clean_symbol_count];
                dst[out_index] = value2;
            } else {
                dst[out_index] = value;
            }
            rows_processed++;
        }
    }
    return rows_processed;
}

#endif //QUESTDB_OOO_RADIX_H

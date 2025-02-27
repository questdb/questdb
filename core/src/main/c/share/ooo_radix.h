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

template<uint16_t Sh, uint16_t TxnBits, typename TRevIdx>
void radix_shuffle_clean(
        uint64_t *counts, const index_tr<TRevIdx> *src, index_l *dest, const uint64_t size,
        int64_t min_value, uint8_t result_format
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
            const auto digit = (src[x].ts >> Sh) & 0xffu;
            dest[counts[digit]].ts = (int64_t) (min_value + (src[x].ts >> TxnBits));
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
            const auto digit = (src[x].ts >> Sh) & 0xffu;
            dest_raw[counts[digit]].ts = (int64_t) (min_value + (src[x].ts >> TxnBits));
            dest_raw[counts[digit]].i = src[x].i;
            counts[digit]++;
            MM_PREFETCH_T2(src + x + 64);
        }
    }
}

template<uint16_t Sh, typename T>
inline void radix_shuffle(uint64_t *counts, const T *src, index_l *dest, const uint64_t size, int64_t min_value) {
    MM_PREFETCH_T0(counts);

    for (uint64_t x = 0; x < size; x++) {
        const auto digit = (src[x] >> Sh) & 0xffu;
        dest[counts[digit]].ts = (int64_t) (min_value + src[x].ts);
        dest[counts[digit]].i = src[x].i;
        counts[digit]++;
        MM_PREFETCH_T2(src + x + 64);
    }
}

template<uint16_t Sh, typename T>
inline void radix_shuffle(uint64_t *counts, const T *src, T *dest, const uint64_t size) {
    MM_PREFETCH_T0(counts);
    for (uint64_t x = 0; x < size; x++) {
        const auto digit = (src[x] >> Sh) & 0xffu;
        dest[counts[digit]] = src[x];
        counts[digit]++;
        MM_PREFETCH_T2(src + x + 64);
    }
}

template<uint16_t TsBits, uint16_t TxnBits, typename TRevIdx>
uint64_t
radix_copy_segments_index_asc(
        const int64_t *lag_ts, const uint64_t lag_size,
        const index_l **segment_ts_maps,
        const txn_info *segment_txns,
        const uint64_t txn_count,
        index_l *out,
        index_l *cpy,
        const uint32_t segment_count,
        int64_t min_value,
        uint8_t result_format,
        uint16_t segment_bits
) {
    // Reverse index type must be an unsigned integer.
    static_assert(std::is_integral_v<TRevIdx> && std::is_unsigned_v<TRevIdx>, "TRevIdx must be a signed integer");
    constexpr uint16_t n = (TsBits + TxnBits + 7) >> 3;

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
            buff1[x].ts = (lag_ts[x] - min_value) << TxnBits;
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
                        (((uint64_t) (segment_ts_maps[segment_index][seg_row].ts - min_value)) << TxnBits) | seq_txn;
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
                                    radix_shuffle_clean<56u, TxnBits, TRevIdx>(
                                            counts[n - 8], buff2, out, size,
                                            min_value, result_format);
                                } else {
                                    radix_shuffle_clean<48u, TxnBits, TRevIdx>(
                                            counts[n - 7], buff1, out, size,
                                            min_value, result_format);
                                }
                            } else {
                                radix_shuffle_clean<40u, TxnBits, TRevIdx>(
                                        counts[n - 6], buff2, out, size, min_value, result_format);
                            }
                        } else {
                            radix_shuffle_clean<32u, TxnBits, TRevIdx>(
                                    counts[n - 5], buff1, out, size, min_value, result_format);
                        }
                    } else {
                        radix_shuffle_clean<24u, TxnBits, TRevIdx>(
                                counts[n - 4], buff2, out, size, min_value, result_format);
                    }
                } else {
                    radix_shuffle_clean<16u, TxnBits, TRevIdx>(
                            counts[n - 3], buff1, out, size, min_value, result_format);
                }
            } else {
                radix_shuffle_clean<8u, TxnBits, TRevIdx>(counts[n - 2], buff2, out, size, min_value, result_format);
            }
        } else {
            radix_shuffle_clean<0u, TxnBits, TRevIdx>(counts[n - 1], buff1, out, size, min_value, result_format);
        }
        return size;
    }
    return 0;
}


template<uint16_t TsBits, uint16_t TxnBits>
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
        uint8_t result_format,
        uint16_t segment_bits
) {
    if (total_row_count_bytes != integral_type_bytes(range_bytes(total_row_count + 1))) {
        return -100;
    }

    switch (total_row_count_bytes) {
        case 1u:
            return radix_copy_segments_index_asc<TsBits, TxnBits, uint8_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                    min_value, result_format, segment_bits
            );
        case 2u:
            return radix_copy_segments_index_asc<TsBits, TxnBits, uint16_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                    min_value, result_format, segment_bits
            );
        case 4u:
            return radix_copy_segments_index_asc<TsBits, TxnBits, uint32_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                    min_value, result_format, segment_bits
            );
        case 8u:
            return radix_copy_segments_index_asc<TsBits, TxnBits, uint64_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count,
                    min_value, result_format, segment_bits
            );
        default:
            return -1;
    }
}

template<uint16_t TsBits, uint16_t TxnBits>
inline uint64_t radix_copy_segments_index_asc_dispatch_segment_bits(
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
    return radix_copy_segments_index_asc_rev<TsBits, TxnBits>(
            lag_ts, lag_size, segment_ts, segment_txns,
            txn_count,
            out, cpy, segment_count, min_value, total_row_count_bytes,
            total_row_count,
            result_format, segment_bits
    );
}

template<uint16_t TsBits>
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
            return radix_copy_segments_index_asc_dispatch_segment_bits<TsBits, 0u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts,
                    segment_txns, txn_count, out, cpy,
                    segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 8:
            return radix_copy_segments_index_asc_dispatch_segment_bits<TsBits, 8u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts,
                    segment_txns, txn_count, out, cpy,
                    segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 16:
            return radix_copy_segments_index_asc_dispatch_segment_bits<TsBits, 16u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 24:
            return radix_copy_segments_index_asc_dispatch_segment_bits<TsBits, 24u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 32:
            return radix_copy_segments_index_asc_dispatch_segment_bits<TsBits, 32u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 40:
            return radix_copy_segments_index_asc_dispatch_segment_bits<TsBits, 40u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 48:
            return radix_copy_segments_index_asc_dispatch_segment_bits<TsBits, 48u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        case 56:
            return radix_copy_segments_index_asc_dispatch_segment_bits<TsBits, 56u>(
                    segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns,
                    txn_count, out,
                    cpy, segment_count, min_value,
                    total_row_count_bytes,
                    total_row_count,
                    result_format
            );
        default:
            return -1;
    }
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
        default:
            return -1;
    }
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

template<typename T, typename TIdx, uint8_t MergeFormat>
int64_t merge_shuffle_fixed_columns_by_rev_index(
        const T **src_addresses,
        T *dst_address,
        const void *reverse_index_ptr,
        const seg_info *segments,
        int64_t segment_count
) {
    static_assert(std::is_integral_v<TIdx> && std::is_unsigned_v<TIdx>, "T must be an unsigned integer");
    const TIdx *reverse_index = reinterpret_cast<const TIdx *>(reverse_index_ptr);

    int64_t rev_row_index = 0;
    int64_t dups = 0;
    for (uint64_t seg = 0; seg < segment_count; seg++) {
        auto src = src_addresses[seg];
        auto segment_lo = segments[seg].segment_lo;
        auto segment_hi = std::abs(segments[seg].segment_hi);

        for (int64_t r = segment_lo; r < segment_hi; r++) {
            auto dst_index = reverse_index[rev_row_index++];
            if constexpr (MergeFormat == dedup_shuffle_index_format) {
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

    if (merge_index_format == shuffle_index_format) {
        switch (reverse_index_format_bytes) {
            case 1:
                return merge_shuffle_fixed_columns_by_rev_index<T, uint8_t, shuffle_index_format>(
                        src, dst,
                        reverse_index, segments,
                        segment_count
                );
            case 2:
                return merge_shuffle_fixed_columns_by_rev_index<T, uint16_t, shuffle_index_format>(
                        src, dst,
                        reverse_index, segments,
                        segment_count
                );
            case 4:
                return merge_shuffle_fixed_columns_by_rev_index<T, uint32_t, shuffle_index_format>(
                        src, dst,
                        reverse_index, segments,
                        segment_count
                );
            case 8:
                return merge_shuffle_fixed_columns_by_rev_index<T, uint64_t, shuffle_index_format>(
                        src, dst,
                        reverse_index, segments,
                        segment_count
                );
            default:
                return -1;
        }
    } else if (merge_index_format == dedup_shuffle_index_format) {
        switch (reverse_index_format_bytes) {
            case 1:
                return merge_shuffle_fixed_columns_by_rev_index<T, uint8_t, dedup_shuffle_index_format>(
                        src, dst,
                        reverse_index, segments,
                        segment_count
                );
            case 2:
                return merge_shuffle_fixed_columns_by_rev_index<T, uint16_t, dedup_shuffle_index_format>(
                        src, dst,
                        reverse_index, segments,
                        segment_count
                );
            case 4:
                return merge_shuffle_fixed_columns_by_rev_index<T, uint32_t, dedup_shuffle_index_format>(
                        src, dst,
                        reverse_index, segments,
                        segment_count
                );
            case 8:
                return merge_shuffle_fixed_columns_by_rev_index<T, uint64_t, dedup_shuffle_index_format>(
                        src, dst,
                        reverse_index, segments,
                        segment_count
                );
            default:
                return -1;
        }
    }
    return -2;
}


template<typename T, uint16_t Mult, uint16_t SegmentBits>
int64_t merge_shuffle_string_column_from_many_addresses_segment_bits(
        const char **src_primary,
        const int64_t **src_secondary,
        char *dst_primary,
        int64_t *dst_secondary,
        const index_l *merge_index,
        int64_t row_count,
        int64_t dst_var_offset
) {
    constexpr uint64_t segment_mask = (1ULL << SegmentBits) - 1;
    static_assert(std::is_integral_v<T> && std::is_signed_v<T>, "string,binary len must be an signed integer");

    for (int64_t l = 0; l < row_count; l++) {
        MM_PREFETCH_T0(merge_index + l + 64);
        dst_secondary[l] = dst_var_offset;

        auto index = merge_index[l].i;
        auto row_index = index >> SegmentBits;
        auto src_index = index & segment_mask;

        const int64_t offset = src_secondary[src_index][row_index];
        const char *src_var_ptr = src_primary[src_index] + offset;

        auto len = *reinterpret_cast<const T *>(src_var_ptr);
        auto char_count = len > 0 ? len * Mult : 0;

        reinterpret_cast<T *>(dst_primary + dst_var_offset)[0] = len;
        __MEMCPY(dst_primary + dst_var_offset + sizeof(T), src_var_ptr + sizeof(T), char_count);
        dst_var_offset += char_count + sizeof(T);
    }

    if (row_count > 0) {
        dst_secondary[row_count] = dst_var_offset;
    }
    return dst_var_offset;
}

template<typename T, uint16_t Mult>
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
            return merge_shuffle_string_column_from_many_addresses_segment_bits<T, Mult, 0u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
        case 1:
            return merge_shuffle_string_column_from_many_addresses_segment_bits<T, Mult, 8u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
        case 2:
            return merge_shuffle_string_column_from_many_addresses_segment_bits<T, Mult, 16u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
        case 3:
            return merge_shuffle_string_column_from_many_addresses_segment_bits<T, Mult, 24u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
        case 4:
            return merge_shuffle_string_column_from_many_addresses_segment_bits<T, Mult, 32u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
        case 5:
            return merge_shuffle_string_column_from_many_addresses_segment_bits<T, Mult, 40u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
        case 6:
            return merge_shuffle_string_column_from_many_addresses_segment_bits<T, Mult, 48u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
        case 7:
            return merge_shuffle_string_column_from_many_addresses_segment_bits<T, Mult, 56u>(
                    src_primary, src_secondary,
                    dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset);
        default:
            return -1;
    }
}

template<uint16_t SegmentBits>
int64_t merge_shuffle_varchar_column_from_many_addresses(
        const char **src_primary,
        const int64_t **src_secondary,
        char *dst_primary,
        int64_t *dst_secondary,
        const index_l *merge_index,
        int64_t row_count,
        int64_t dst_var_offset
) {
    constexpr uint64_t segment_mask = (1ULL << SegmentBits) - 1;

    for (int64_t l = 0; l < row_count; l++) {
        auto index = merge_index[l].i;
        auto row_index = index >> SegmentBits;
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
    jlong dups = 0;
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
                    dups++;
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
    return rows_processed - dups;
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

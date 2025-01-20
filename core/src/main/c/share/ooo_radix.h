//
// Created by Alex Pelagenko on 13/01/2025.
//

#ifndef QUESTDB_OOO_RADIX_H
#define QUESTDB_OOO_RADIX_H

#include <cstring>
#include <cassert>
#include "ooo_dispatch.h"
#include "util.h"
#include <algorithm>
#include "simd.h"

template<auto Start, auto End, auto Inc, class F>
constexpr void constexpr_for(F &&f) {
    if constexpr (Start < End) {
        f(std::integral_constant<decltype(Start), Start>());
        constexpr_for<Start + Inc, End, Inc>(f);
    }
}

inline uint16_t range_bits(uint64_t range) {
    if (range <= 1) return 0;
    return 64 - __builtin_clzll(range - 1);
}

inline uint16_t range_bytes(uint64_t range) {
    return (range_bits(range) + 7) >> 3;
}

typedef struct seg_info {
    int64_t wal_id;
    int64_t segment_id;
    int64_t segment_lo;
    int64_t segment_hi;
} seg_info;

typedef struct txn_info {
    int64_t segment_row_offset;
    int64_t seq_txn;
    int64_t row_count;
    int64_t seg_info_index;
} txn_info;

template<typename T>
struct index_tr {
    uint64_t ts;
    uint64_t i;
    T ri;

    uint64_t operator>>(uint64_t shr) const {
        return ts >> shr;
    }

    uint64_t operator&(uint64_t mask) const {
        return ts & mask;
    }
};


template<uint16_t sh, uint16_t txn_bits, typename TRevIdx>
inline void radix_shuffle_clean(uint64_t *counts, const index_tr<TRevIdx> *src, index_l *dest, const uint64_t size,
                                int64_t min_value) {
    MM_PREFETCH_T0(counts);
    auto dest_rev_idx = reinterpret_cast<TRevIdx *>(&dest[size]);

    for (uint64_t x = 0; x < size; x++) {
        const auto digit = (src[x].ts >> sh) & 0xffu;
        dest[counts[digit]].ts = (int64_t) (min_value + (src[x].ts >> txn_bits));
        dest[counts[digit]].i = src[x].i;
        dest_rev_idx[src[x].ri] = counts[digit];
        counts[digit]++;
        MM_PREFETCH_T2(src + x + 64);
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

template<uint16_t ts_bits, uint16_t txn_bits, uint16_t segment_bits, typename TRevIdx>
void
radix_copy_segments_index_asc(const int64_t *lag_ts, const uint64_t lag_size,
                              const index_l **segment_ts_maps,
                              const txn_info *segment_txns,
                              const uint64_t txn_count,
                              index_l *out,
                              index_l *cpy,
                              const uint32_t segment_count,
                              int64_t min_value
) {
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
            buff1[x].i = x << segment_bits | segment_count;
            buff1[x].ri = (TRevIdx) x;
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
                buff1[x].i = (seg_row << segment_bits) | segment_index;
                buff1[x].ri = (TRevIdx) x;

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
                                    radix_shuffle_clean<56u, txn_bits, TRevIdx>(
                                            counts[n - 8], buff2, out, size,
                                            min_value);
                                } else {
                                    radix_shuffle_clean<48u, txn_bits, TRevIdx>(
                                            counts[n - 7], buff1, out, size,
                                            min_value);
                                }
                            } else {
                                radix_shuffle_clean<40u, txn_bits, TRevIdx>(
                                        counts[n - 6], buff2, out, size, min_value);
                            }
                        } else {
                            radix_shuffle_clean<32u, txn_bits, TRevIdx>(
                                    counts[n - 5], buff1, out, size, min_value);
                        }
                    } else {
                        radix_shuffle_clean<24u, txn_bits, TRevIdx>(
                                counts[n - 4], buff2, out, size, min_value);
                    }
                } else {
                    radix_shuffle_clean<16u, txn_bits, TRevIdx>(
                            counts[n - 3], buff1, out, size, min_value);
                }
            }
        } else {
            radix_shuffle_clean<0u, txn_bits, TRevIdx>(
                    counts[n - 1], buff1, out, size, min_value);
        }
    }
}


template<uint16_t ts_bits, uint16_t txn_bits, uint16_t segment_bits>
void
radix_copy_segments_index_asc_rev(
        const int64_t *lag_ts, const uint64_t lag_size,
        const index_l **segment_ts_maps,
        const txn_info *segment_txns,
        const uint64_t txn_count,
        index_l *out,
        index_l *cpy,
        const uint32_t segment_count,
        int64_t min_value,
        int32_t total_row_count_bytes
) {
    switch (total_row_count_bytes) {
        case 1u:
            radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint8_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count, min_value);
            break;
        case 2u:
            radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint16_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count, min_value);
            break;
        case 3u:
        case 4u:
            radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint32_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count, min_value);
            break;
        case 5u:
        case 6u:
        case 7u:
        case 8u:
            radix_copy_segments_index_asc<ts_bits, txn_bits, segment_bits, uint64_t>(
                    lag_ts, lag_size, segment_ts_maps, segment_txns, txn_count, out, cpy, segment_count, min_value);
            break;
    }
}

template<uint16_t ts_bits, uint16_t txn_bits>
void radix_copy_segments_index_asc_dispatch_segment_bits(uint16_t segment_bits,
                                                         const int64_t *lag_ts, const uint64_t lag_size,
                                                         const index_l **segment_ts, const txn_info *segment_txns,
                                                         const uint64_t txn_count, index_l *out, index_l *cpy,
                                                         const uint32_t segment_count,
                                                         int64_t min_value,
                                                         int32_t total_row_count_bytes
) {
    switch (segment_bits) {
        case 0:
            radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 0u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes);
            break;
        case 8:
            radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 8u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes);
            break;
        case 16:
            radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 16u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes);
            break;
        case 24:
            radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 24u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes);
            break;
        case 32:
            radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 32u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes);
            break;
        case 40:
            radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 40u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes);
            break;
        case 48:
            radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 48u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes);
            break;
        case 56:
            radix_copy_segments_index_asc_rev<ts_bits, txn_bits, 56u>(
                    lag_ts, lag_size, segment_ts, segment_txns,
                    txn_count,
                    out, cpy, segment_count, min_value, total_row_count_bytes);
            break;
    }
}

template<uint16_t ts_bits>
inline void radix_copy_segments_index_asc_dispatch_txn_segment_bits(
        const uint16_t txn_bits, const uint16_t segment_bits,
        const int64_t *lag_ts, const uint64_t lag_size,
        const index_l **segment_ts, const txn_info *segment_txns,
        const uint64_t txn_count, index_l *out, index_l *cpy,
        const uint32_t segment_count,
        int64_t min_value,
        int total_row_count_bytes
) {
    switch (txn_bits) {
        case 0:
            radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 0u>(segment_bits, lag_ts, lag_size, segment_ts,
                                                                             segment_txns, txn_count, out, cpy,
                                                                             segment_count, min_value,
                                                                             total_row_count_bytes);
            break;
        case 8:
            radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 8u>(segment_bits, lag_ts, lag_size, segment_ts,
                                                                             segment_txns, txn_count, out, cpy,
                                                                             segment_count, min_value,
                                                                             total_row_count_bytes);
            break;
        case 16:
            radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 16u>(segment_bits, lag_ts, lag_size,
                                                                              segment_ts, segment_txns, txn_count, out,
                                                                              cpy, segment_count, min_value,
                                                                              total_row_count_bytes);
            break;
        case 24:
            radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 24u>(segment_bits, lag_ts, lag_size,
                                                                              segment_ts, segment_txns, txn_count, out,
                                                                              cpy, segment_count, min_value,
                                                                              total_row_count_bytes);
            break;
        case 32:
            radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 32u>(segment_bits, lag_ts, lag_size,
                                                                              segment_ts, segment_txns, txn_count, out,
                                                                              cpy, segment_count, min_value,
                                                                              total_row_count_bytes);
            break;
        case 40:
            radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 40u>(segment_bits, lag_ts, lag_size,
                                                                              segment_ts, segment_txns, txn_count, out,
                                                                              cpy, segment_count, min_value,
                                                                              total_row_count_bytes);
            break;
        case 48:
            radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 48u>(segment_bits, lag_ts, lag_size,
                                                                              segment_ts, segment_txns, txn_count, out,
                                                                              cpy, segment_count, min_value,
                                                                              total_row_count_bytes);
            break;
        case 56:
            radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 56u>(segment_bits, lag_ts, lag_size,
                                                                              segment_ts, segment_txns, txn_count, out,
                                                                              cpy, segment_count, min_value,
                                                                              total_row_count_bytes);
            break;
    }
}

inline void radix_copy_segments_index_asc_precompiled(uint16_t ts_bits, uint16_t txn_bits, uint16_t segment_bits,
                                                      const int64_t *lag_ts, const uint64_t lag_size,
                                                      const index_l **segment_ts, const txn_info *segment_txns,
                                                      const uint64_t txn_count, index_l *out, index_l *cpy,
                                                      const uint32_t segment_count,
                                                      int64_t min_value,
                                                      int32_t total_row_count_bytes
) {
    switch (ts_bits) {
        case 0:
            radix_copy_segments_index_asc_dispatch_txn_segment_bits<0u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes);
            break;
        case 8:
            radix_copy_segments_index_asc_dispatch_txn_segment_bits<8u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes);
            break;
        case 16:
            radix_copy_segments_index_asc_dispatch_txn_segment_bits<16u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes);
            break;
        case 24:
            radix_copy_segments_index_asc_dispatch_txn_segment_bits<24u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes);
            break;
        case 32:
            radix_copy_segments_index_asc_dispatch_txn_segment_bits<32u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes);
            break;
        case 40:
            radix_copy_segments_index_asc_dispatch_txn_segment_bits<40u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes);
            break;
        case 48:
            radix_copy_segments_index_asc_dispatch_txn_segment_bits<48u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes);
            break;
        case 56:
            radix_copy_segments_index_asc_dispatch_txn_segment_bits<56u>(
                    txn_bits, segment_bits, lag_ts, lag_size,
                    segment_ts, segment_txns, txn_count, out, cpy,
                    segment_count, min_value, total_row_count_bytes);
            break;
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
int merge_shuffle_column_from_many_addresses(
        int32_t index_segment_encoding_bytes, const void **src_addresses,
        void *dst_address, const index_l *merge_index_address,
        uint64_t row_count) {
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
    return 0;
}

template<typename T, uint16_t mult, uint16_t segment_bits>
void merge_shuffle_string_column_from_many_addresses_segment_bits(const char **src_primary,
                                                                  const int64_t **src_secondary,
                                                                  char *dst_primary,
                                                                  int64_t *dst_secondary,
                                                                  const index_l *merge_index,
                                                                  int64_t row_count,
                                                                  int64_t dst_var_offset) {
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
inline int32_t merge_shuffle_string_column_from_many_addresses(const int32_t index_segment_encoding_bytes,
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
    return 0;
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

template<typename TIdx>
void merge_shuffle_symbol_column_from_many_addresses(
        const int32_t **src,
        int32_t *dst,
        const index_l *merge_index,
        int64_t row_count,
        const txn_info *segment_txns,
        uint64_t txn_count,
        const int32_t *symbol_map,
        uint64_t symbol_map_size) {
    // Reverse index is saved at the end of the merge_index
    const TIdx *reverse_index = reinterpret_cast<const TIdx *>(merge_index + row_count);

    TIdx out_index = 0;
    for (uint64_t txn_index = 0; txn_index < txn_count; txn_index++) {
        auto segment_addr = src[segment_txns[txn_index].seg_info_index];
        uint64_t hi = segment_txns[txn_index].segment_row_offset + segment_txns[txn_index].row_count;
        int32_t clean_symbol_count = symbol_map[2 * txn_index];
        int32_t map_offset = symbol_map[2 * txn_index + 1];

        for (uint64_t seg_row = segment_txns[txn_index].segment_row_offset; seg_row < hi; seg_row++, out_index++) {
            int32_t value = segment_addr[seg_row];
            if (value > clean_symbol_count) {
                dst[reverse_index[out_index]] = symbol_map[map_offset + value - clean_symbol_count];
            } else {
                dst[reverse_index[out_index]] = value;
            }
        }
    }
}

#endif //QUESTDB_OOO_RADIX_H

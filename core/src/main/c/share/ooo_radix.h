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


template<uint16_t sh, uint16_t txn_bits, typename T>
inline void radix_shuffle_clean(uint64_t *counts, const T *src, index_l *dest, const uint64_t size, int64_t min_value) {
    MM_PREFETCH_T0(counts);
    for (uint64_t x = 0; x < size; x++) {
        const auto digit = (src[x] >> sh) & 0xffu;
        dest[counts[digit]].ts = (int64_t) (min_value + (src[x].ts >> txn_bits));
        dest[counts[digit]].i = src[x].i;
        counts[digit]++;
        MM_PREFETCH_T2(src + x + 64);
    }
}

template<uint16_t sh, typename T>
inline void radix_shuffle(uint64_t *counts, const T *src, index_l *dest, const uint64_t size, int64_t min_value) {
    radix_shuffle_clean<sh, 0u, T>(counts, src, dest, size, min_value);
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

template<uint16_t ts_bits, uint16_t txn_bits, uint16_t segment_bits>
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

        index_t *buff1;
        index_t *buff2;
        if constexpr (n % 2 == 0) {
            buff1 = (index_t *) out;
            buff2 = (index_t *) cpy;
        } else {
            // invert out and copy, so that after n iterations data ends up in out
            buff1 = (index_t *) cpy;
            buff2 = (index_t *) out;
        }

        // calculate counts
        for (x = 0; x < lag_size; x++) {
            buff1[x].ts = (lag_ts[x] - min_value) << txn_bits;
            buff1[x].i = x << segment_bits | segment_count;
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
            auto txn = &segment_txns[txn_index];
            auto segment_index = txn->seg_info_index;
            auto segment_addr = &segment_ts_maps[segment_index][txn->segment_row_offset];
            auto seq_txn = txn->seq_txn;

            const uint64_t hi = txn->segment_row_offset + txn->row_count;
            for (uint64_t txn_row = txn->segment_row_offset; txn_row < hi; txn_row++, x++) {
                buff1[x].ts = (((uint64_t )(segment_ts_maps[segment_index][txn_row].ts - min_value)) << txn_bits) | seq_txn;
                buff1[x].i = (txn_row << segment_bits) | segment_index;

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
                                    radix_shuffle_clean<56u, txn_bits>(counts[n - 8], buff2, out, size, min_value);
                                } else {
                                    radix_shuffle_clean<48u, txn_bits>(counts[n - 7], buff1, out, size, min_value);
                                }
                            } else {
                                radix_shuffle_clean<40u, txn_bits>(counts[n - 6], buff2, out, size, min_value);
                            }
                        } else {
                            radix_shuffle_clean<32u, txn_bits>(counts[n - 5], buff1, out, size, min_value);
                        }
                    } else {
                        radix_shuffle_clean<24u, txn_bits>(counts[n - 4], buff2, out, size, min_value);
                    }
                } else {
                    radix_shuffle_clean<16u, txn_bits>(counts[n - 3], buff1, out, size, min_value);
                }
            }
        } else {
            radix_shuffle_clean<0u, txn_bits>(counts[n - 1], buff1, out, size, min_value);
        }
    }
}


template<uint16_t ts_bits, uint16_t txn_bits>
void radix_copy_segments_index_asc_dispatch_segment_bits(uint16_t segment_bits,
                                                     const int64_t *lag_ts, const uint64_t lag_size,
                                                     const index_l **segment_ts, const txn_info *segment_txns,
                                                     const uint64_t txn_count, index_l *out, index_l *cpy,
                                                         const uint32_t segment_count,
                                                     int64_t min_value) {
    switch (segment_bits) {
        case 0:  radix_copy_segments_index_asc<ts_bits, txn_bits, 0u>(lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 8:  radix_copy_segments_index_asc<ts_bits, txn_bits, 8u>(lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 16: radix_copy_segments_index_asc<ts_bits, txn_bits, 16u>(lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 24: radix_copy_segments_index_asc<ts_bits, txn_bits, 24u>(lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 32: radix_copy_segments_index_asc<ts_bits, txn_bits, 32u>(lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 40: radix_copy_segments_index_asc<ts_bits, txn_bits, 40u>(lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 48: radix_copy_segments_index_asc<ts_bits, txn_bits, 48u>(lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 56: radix_copy_segments_index_asc<ts_bits, txn_bits, 56u>(lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
    }
}

template<uint16_t ts_bits>
inline void radix_copy_segments_index_asc_dispatch_txn_segment_bits(
                       const uint16_t txn_bits, const uint16_t segment_bits,
                       const int64_t *lag_ts, const uint64_t lag_size,
                       const index_l **segment_ts, const txn_info *segment_txns,
                       const uint64_t txn_count, index_l *out, index_l *cpy,
                       const uint32_t segment_count,
                       int64_t min_value
) {
    switch (txn_bits) {
        case 0:  radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 0u>(segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 8:  radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 8u>(segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 16: radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 16u>(segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 24: radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 24u>(segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 32: radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 32u>(segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 40: radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 40u>(segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 48: radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 48u>(segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 56: radix_copy_segments_index_asc_dispatch_segment_bits<ts_bits, 56u>(segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
    }
}

// Wrapper to dispatch all runtime values
inline void radix_copy_segments_index_asc_precompiled(uint16_t ts_bits, uint16_t txn_bits, uint16_t segment_bits,
                                           const int64_t *lag_ts, const uint64_t lag_size,
                                           const index_l **segment_ts, const txn_info *segment_txns,
                                           const uint64_t txn_count, index_l *out, index_l *cpy,
                                               const uint32_t segment_count,
                                           int64_t min_value) {
    switch (ts_bits) {
        case 0:  radix_copy_segments_index_asc_dispatch_txn_segment_bits<0u>(txn_bits, segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 8:  radix_copy_segments_index_asc_dispatch_txn_segment_bits<8u>(txn_bits, segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 16: radix_copy_segments_index_asc_dispatch_txn_segment_bits<16u>(txn_bits, segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 24: radix_copy_segments_index_asc_dispatch_txn_segment_bits<24u>(txn_bits, segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 32: radix_copy_segments_index_asc_dispatch_txn_segment_bits<32u>(txn_bits, segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 40: radix_copy_segments_index_asc_dispatch_txn_segment_bits<40u>(txn_bits, segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 48: radix_copy_segments_index_asc_dispatch_txn_segment_bits<48u>(txn_bits, segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
        case 56: radix_copy_segments_index_asc_dispatch_txn_segment_bits<56u>(txn_bits, segment_bits, lag_ts, lag_size, segment_ts, segment_txns, txn_count, out, cpy, segment_count, min_value); break;
    }
}
#endif //QUESTDB_OOO_RADIX_H

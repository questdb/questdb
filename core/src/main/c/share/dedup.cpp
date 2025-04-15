/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
#include "jni.h"
#include <cstring>
#include "util.h"
#include "simd.h"
#include "ooo_dispatch.h"
#include "bit_vector.h"
#include "column_type.h"
#include <algorithm>
#include <cassert>
#include "dedup.h"
#include "dedup_comparers.h"
#include "ooo.h"

#define assertm(exp, msg) assert(((void)msg, exp))

constexpr int64_t error_not_sorted = -1;
constexpr int64_t no_timestamp_duplicates = -2;
constexpr int64_t error_out_of_range = -3;

template<typename LambdaDiff>
inline int64_t branch_free_search(const index_t *array, int64_t count, int64_t value_index, LambdaDiff compare) {
    const index_t *base = array;
    int64_t n = count;
    while (n > 1) {
        int64_t half = n / 2;
        MM_PREFETCH_T0(base + half / 2);
        MM_PREFETCH_T0(base + half + half / 2);
        auto diff = compare(value_index, base[half].i);
        base = (diff > 0) ? base + half : base;
        n -= half;
    }
    if (compare(value_index, base[0].i) == 0) {
        return base - array;
    }
    if (base - array + 1 < count && compare(value_index, base[1].i) == 0) {
        return base - array + 1;
    }
    return -1;
}

template<typename LambdaDiff>
int64_t merge_dedup_long_index_int_keys(
        const uint64_t *src,
        int64_t src_lo,
        const int64_t src_hi_incl,
        const index_t *index,
        int64_t index_lo,
        const int64_t index_hi_incl,
        index_t *dest_index,
        const LambdaDiff compare
) {
    int64_t &src_pos = src_lo;
    int64_t &index_pos = index_lo;
    index_t *dest = dest_index;

    bit_vector used_indexes = {};
    while (src_pos <= src_hi_incl && index_pos <= index_hi_incl) {
        // Perform normal merge until the timestamp matches.
        if (src[src_pos] < index[index_pos].ts) {
            (*dest).ts = src[src_pos];
            // The first bit is used by merge procedures to know where to take the value from. 1 indicates column, 0 O3 data
            (*dest).i = src_pos | (1ull << 63);
            dest++;
            src_pos++;
        } else if (src[src_pos] > index[index_pos].ts) {
            *dest++ = index[index_pos];
            index_pos++;
        } else {
            // index_ts == src_ts
            const uint64_t conflict_ts = src[src_pos];
            const index_t *conflict_index_start = &index[index_pos];

            // Find end of the conflict in index
            const auto conflict_end_pos = index_pos +
                                          branch_free_search_lower<index_t>(&index[index_pos],
                                                                            index_hi_incl - index_pos + 1,
                                                                            {conflict_ts + 1, 0});

            // binary search for matching index record for every source record
            const int64_t binary_search_len = conflict_end_pos - index_pos;

            // track all found index records
            used_indexes.reset(binary_search_len);
            while (src_pos <= src_hi_incl && src[src_pos] == conflict_ts) {
                (*dest).ts = conflict_ts;
                const int64_t matched_index = branch_free_search(conflict_index_start, binary_search_len,
                                                                 src_pos, compare);
                if (matched_index > -1) {
                    used_indexes.set(matched_index);
                    (*dest).i = conflict_index_start[matched_index].i;
                } else {
                    (*dest).i = src_pos | (1ull << 63);
                }
                ++src_pos;
                ++dest;
            }

            // add all index records with no matches
            used_indexes.foreach_unset(
                    [&](const int64_t unused_index) {
                        *dest = conflict_index_start[unused_index];
                        ++dest;
                    }
            );
            index_pos = conflict_end_pos;
        }
    }

    if (index_pos <= index_hi_incl) {
        __MEMCPY(dest, &index[index_pos], (index_hi_incl - index_pos + 1) * sizeof(index_t));
        dest += index_hi_incl - index_pos + 1;
    } else {
        for (; src_pos <= src_hi_incl; src_pos++, dest++) {
            (*dest).ts = src[src_pos];
            // The first bit is used by merge procedures to know where to take the value from. 1 indicates column, 0 O3 data
            (*dest).i = src_pos | (1ull << 63);
        }
    }

    return dest - dest_index;
}

template<typename IndexT>
inline int64_t dedup_sorted_timestamp_index(const IndexT *index_in, int64_t count, IndexT *index_out) {
    // std::unique_copy takes first record but here we want last
    if (count > 0) {
        int64_t copyTo = 0;
        uint64_t lastTimestamp = index_in[0].ts;
        for (int64_t i = 1; i < count; i++) {
            if (index_in[i].ts > lastTimestamp) {
                index_out[copyTo] = index_in[i - 1];
                copyTo++;
                lastTimestamp = index_in[i].ts;
            } else if (index_in[i].ts < lastTimestamp) {
                return error_not_sorted;
            }
        }
        index_out[copyTo] = index_in[count - 1];
        return copyTo + 1;
    }
    return 0;
}

template<typename DiffLambda, typename IndexT>
inline int64_t dedup_sorted_timestamp_index_with_keys(
        const IndexT *index_src,
        const int64_t count,
        IndexT *index_dest,
        IndexT *index_tmp,
        const DiffLambda diff_l
) {
    if (count < 2) {
        return no_timestamp_duplicates;
    }

    // find duplicate ranges
    int64_t dup_start = -1;
    int64_t dup_end = 0;
    int64_t ts_index = 0;

    for (int64_t i = 1; i < count; i++) {
        if (index_src[i].ts > index_src[ts_index].ts) {
            if (i > ts_index + 1) {
                dup_start = dup_start > -1 ? dup_start : ts_index;
                dup_end = i;
            }
            ts_index = i;
        } else if (index_src[i].ts < index_src[ts_index].ts) {
            return error_not_sorted;
        }
    }
    if (ts_index < count - 1 && index_src[ts_index].ts == index_src[count - 1].ts) {
        // last element is a duplicate with the previous one
        dup_start = dup_start > -1 ? dup_start : ts_index;
        dup_end = count;
    } else if (dup_start == -1 || dup_end - dup_start <= 0) {
        // no timestamp duplicates
        return no_timestamp_duplicates;
    }

    // dedup range from dup_start to dup_end.
    // sort the data first by ts and keys using stable merge sort.
    const IndexT *merge_result = merge_sort(index_src, index_dest, index_tmp, dup_start, dup_end, diff_l);

    int64_t copy_to = dup_start;
    int64_t last = dup_start;

    for (int64_t i = dup_start + 1; i < dup_end; i++) {
        auto l = merge_result[last].i;
        auto r = merge_result[i].i;
        if (merge_result[i].ts > merge_result[last].ts || diff_l(l, r) != 0) {
            index_dest[copy_to++] = merge_result[i - 1];
            last = i;
        } else if (merge_result[i].ts != merge_result[last].ts) {
            return error_not_sorted;
        }
    }
    index_dest[copy_to] = merge_result[dup_end - 1];

    // copy prefix and the tail if necessary
    if (index_src != index_dest) {
        __MEMCPY(index_dest, index_src, dup_start * sizeof(IndexT));
    }

    const int64_t tail = count - dup_end;
    __MEMMOVE(&index_dest[copy_to + 1], &index_src[dup_end], tail * sizeof(IndexT));
    return copy_to + 1 + tail;
}

template<typename DiffLambda, typename IndexT>
inline void merge_sort_slice(const IndexT *src1, const IndexT *src2, IndexT *dest, const int64_t &src1_len,
                             const int64_t &src2_len, const DiffLambda diff_l) {

    int64_t i1 = 0, i2 = 0;

    while (i1 < src1_len && i2 < src2_len) {
        if (src1[i1].ts > src2[i2].ts) {
            *dest++ = src2[i2++];
        } else if (src1[i1].ts < src2[i2].ts) {
            *dest++ = src1[i1++];
        } else {
            // same timestamp
            if (diff_l(src1[i1].i, src2[i2].i) > 0) {
                *dest++ = src2[i2++];
            } else {
                *dest++ = src1[i1++];
            }
        }
    }

    if (i1 < src1_len) {
        __MEMCPY(dest, &src1[i1], (src1_len - i1) * sizeof(IndexT));
    } else {
        __MEMCPY(dest, &src2[i2], (src2_len - i2) * sizeof(IndexT));
    }
}

template<typename DiffLambda, typename IndexT>
inline IndexT *merge_sort(
        const IndexT *index_src,
        IndexT *index_dest1,
        IndexT *index_dest2,
        int64_t start,
        int64_t end,
        const DiffLambda diff_l
) {
    IndexT *const dest_arr[] = {index_dest2, index_dest1};
    const IndexT *source = index_src;
    IndexT *dest;
    const int64_t len = end - start;
    int64_t slice_len = 1;

    int cycle = 0;
    do {
        dest = dest_arr[cycle % 2]; // first write to index_dest2 then to index_dest1
        const int64_t twice_slice = 2 * slice_len;
        for (int64_t i = start; i < end; i += twice_slice) {
            merge_sort_slice(
                    &source[i],
                    &source[i + slice_len],
                    &dest[i],
                    std::min(slice_len, end - i),
                    std::max((int64_t) 0, std::min(slice_len, end - (i + slice_len))),
                    diff_l
            );
        }
        source = dest_arr[cycle++ % 2]; // rotate source and destination
        slice_len = twice_slice;
    } while (slice_len < len);

    return dest;
}

template<typename TIdx>
int64_t dedup_sorted_timestamp_index_many_addresses(
        index_tr<TIdx> *index_out,
        const index_tr<TIdx> *index_in,
        const int64_t index_count,
        index_tr<TIdx> *index_temp,
        int32_t dedup_key_count,
        const dedup_column *src_keys,
        uint16_t segment_bits
) {
    if (dedup_key_count == 0) {
        return dedup_sorted_timestamp_index(index_in, index_count, index_out);
    }
    const uint64_t segment_mask = (1ULL << segment_bits) - 1;
    const auto diff_l = [&](const index_tr_i<TIdx> l, const index_tr_i<TIdx> r) {
        for (int32_t c = 0; c < dedup_key_count; c++) {
            const dedup_column *col_key = &src_keys[c];
            int diff;
            switch (col_key->value_size_bytes) {
                case 1: {
                    diff = compare_dedup_column_fixed<int8_t, TIdx>(col_key, l, r, segment_bits, segment_mask);
                    break;
                }
                case 2: {
                    diff = compare_dedup_column_fixed<int16_t, TIdx>(col_key, l, r, segment_bits, segment_mask);
                    break;
                }
                case 4: {
                    diff = compare_dedup_column_fixed<int32_t, TIdx>(col_key, l, r, segment_bits, segment_mask);
                    break;
                }
                case 8: {
                    diff = compare_dedup_column_fixed<int64_t, TIdx>(col_key, l, r, segment_bits, segment_mask);
                    break;
                }
                case 16: {
                    diff = compare_dedup_column_fixed<__int128, TIdx>(col_key, l, r, segment_bits, segment_mask);
                    break;
                }
                case 32: {
                    diff = compare_dedup_column_fixed<int256, TIdx>(col_key, l, r, segment_bits, segment_mask);
                    break;
                }
                case -1: {
                    switch ((ColumnType) (col_key->column_type)) {
                        case ColumnType::VARCHAR: {
                            diff = compare_dedup_varchar_column<TIdx>(col_key, l, r, segment_bits, segment_mask);
                            break;
                        }
                        case ColumnType::STRING: {
                            diff = compare_str_bin_dedup_column<int32_t, 2, TIdx>(col_key, l, r, segment_bits,
                                                                                  segment_mask);
                            break;
                        }
                        case ColumnType::BINARY: {
                            diff = compare_str_bin_dedup_column<int64_t, 1, TIdx>(col_key, l, r, segment_bits,
                                                                                  segment_mask);
                            break;
                        }
                        case ColumnType::SYMBOL: {
                            // Very special case, it's the symbol that is re-mapped into a single buffer
                            // e.g. the values do not come from multiple segments but from a single buffer
                            diff = compare_dedup_symbol_column<TIdx>(col_key, l, r);
                            break;
                        }
                        default: {
                            assertm(false, "unsupported column type");
                            return -1;
                        }
                    }
                    break;
                }
                default:
                    assertm(false, "unsupported column type");
                    return -1;
            }
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    };

    return dedup_sorted_timestamp_index_with_keys(index_in, index_count, index_out, index_temp, diff_l);
}

template<typename TIdx>
int64_t dedup_sorted_timestamp_index_many_addresses_segment_bits_clean(
        int32_t segment_encoding_bytes,
        jlong indexOut,
        const jlong indexIn,
        const int64_t row_count,
        jlong indexTemp,
        int32_t dedup_key_count,
        const dedup_column *src_keys
) {
    static_assert(std::is_integral_v<TIdx> && std::is_unsigned_v<TIdx>, "TRevIdx must be an unsigned integer");

    int64_t dedup_rows = dedup_sorted_timestamp_index_many_addresses<TIdx>(
            reinterpret_cast<index_tr<TIdx> *>(indexOut),
            reinterpret_cast<const index_tr<TIdx> *>(indexIn),
            row_count,
            reinterpret_cast<index_tr<TIdx> *>(indexTemp),
            dedup_key_count,
            src_keys,
            segment_encoding_bytes * 8
    );

    // -2 means no dups
    if (dedup_rows == no_timestamp_duplicates) {
        dedup_rows = row_count;

        auto index_out = reinterpret_cast<index_tr<TIdx> *>(indexOut);
        auto index_temp = reinterpret_cast<index_l *>(indexTemp);

        // Return data in shuffle_index_format
        auto all_row_count = reinterpret_cast<int64_t *>(&index_temp[dedup_rows]);
        auto reverse_index = reinterpret_cast<TIdx *>(&all_row_count[1]);
        all_row_count[0] = row_count;

        for (int64_t i = 0; i < dedup_rows; i++) {
            index_temp[i].ts = index_out[i].ts;
            index_temp[i].i = index_out[i].i.i;
            reverse_index[index_out[i].i.ri] = i;
        }

        return no_timestamp_duplicates;
    }

    if (dedup_rows > 0) {
        auto index_out = reinterpret_cast<index_tr<TIdx> *>(indexOut);
        auto index_temp = reinterpret_cast<index_l *>(indexTemp);

        // Return data in dedup_index_format

        // Format of the index is
        // dedup_rows of records index_l
        // then row count of the all rows, before the dedup took place
        // and then reverse shuffle index of all rows, before the dedup took place
        // value 0 in reverse index means row is not used
        // value 1 means row should go to position 0
        // etc., reverse index destination is shifted by 1
        auto all_row_count = reinterpret_cast<int64_t *>(&index_temp[dedup_rows]);
        auto reverse_index = reinterpret_cast<TIdx *>(&all_row_count[1]);
        __MEMSET(reverse_index, 0, row_count * sizeof(TIdx));
        all_row_count[0] = row_count;

        for (int64_t i = 0; i < dedup_rows; i++) {
            index_temp[i].ts = index_out[i].ts;
            index_temp[i].i = index_out[i].i.i;
            reverse_index[index_out[i].i.ri] = i + 1;
        }
    }

    // 0 or error
    return dedup_rows;
}

int64_t dedup_sorted_timestamp_index_many_addresses_segment_bits_row_encoding(
        int32_t segment_encoding_bytes,
        jlong indexOut,
        const jlong indexIn,
        const jlong index_format,
        jlong indexTemp,
        int32_t dedup_key_count,
        const dedup_column *src_keys
) {

    auto rows_bytes = read_reverse_index_format_bytes(index_format);
    auto index_count = read_row_count(index_format);
    switch (rows_bytes) {
        case 1:
            return dedup_sorted_timestamp_index_many_addresses_segment_bits_clean<uint8_t>(
                    segment_encoding_bytes, indexOut,
                    indexIn, index_count, indexTemp,
                    dedup_key_count, src_keys
            );
        case 2:
            return dedup_sorted_timestamp_index_many_addresses_segment_bits_clean<uint16_t>(
                    segment_encoding_bytes, indexOut,
                    indexIn, index_count, indexTemp,
                    dedup_key_count, src_keys
            );
        case 4:
            return dedup_sorted_timestamp_index_many_addresses_segment_bits_clean<uint32_t>(
                    segment_encoding_bytes, indexOut,
                    indexIn, index_count, indexTemp,
                    dedup_key_count, src_keys
            );
        case 8:
            return dedup_sorted_timestamp_index_many_addresses_segment_bits_clean<uint64_t>(
                    segment_encoding_bytes, indexOut,
                    indexIn, index_count, indexTemp,
                    dedup_key_count, src_keys
            );
        default:
            return error_out_of_range;
    }
}

extern "C" {
JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_mergeDedupTimestampWithLongIndexAsc(
        JAVA_STATIC,
        jlong pSrc,
        jlong srcLo,
        jlong srcHiInclusive,
        jlong pIndex,
        jlong indexLo,
        jlong indexHiInclusive,
        jlong pDestIndex
) {
    const uint64_t *src = reinterpret_cast<uint64_t *> (pSrc);
    const index_t *index = reinterpret_cast<index_t *> (pIndex);

    auto src_pos = __JLONG_REINTERPRET_CAST__(int64_t, srcLo);
    auto index_pos = __JLONG_REINTERPRET_CAST__(int64_t, indexLo);
    auto *dest = reinterpret_cast<index_t *> (pDestIndex);

    const auto src_hi_incl = __JLONG_REINTERPRET_CAST__(int64_t, srcHiInclusive);
    const auto index_hi_inc = __JLONG_REINTERPRET_CAST__(int64_t, indexHiInclusive);

    while (src_pos <= src_hi_incl &&
           index_pos <= index_hi_inc) {
        if (src[src_pos] < index[index_pos].ts) {
            (*dest).ts = src[src_pos];
            // The first bit is used by merge procedures to know where to take the value from. 1 indicates column, 0 O3 data
            (*dest).i = src_pos | (1ull << 63);
            dest++;
            src_pos++;
        } else if (src[src_pos] > index[index_pos].ts) {
            *dest++ = index[index_pos];
            index_pos++;
        } else {
            // index_ts == src_ts
            const uint64_t conflict_ts = src[src_pos];
            while (index_pos <= index_hi_inc && index[index_pos].ts == conflict_ts) {
                index_pos++;
            }

            // replace all records with same timestamp with last version from index
            while (src_pos <= src_hi_incl && src[src_pos] == conflict_ts) {
                *dest++ = index[index_pos - 1];
                src_pos++;
            }
        }
    }

    while (index_pos <= index_hi_inc) {
        *dest++ = index[index_pos];
        index_pos++;
    }

    while (src_pos <= src_hi_incl) {
        (*dest).ts = src[src_pos];
        (*dest).i = src_pos | (1ull << 63);
        dest++;
        src_pos++;
    }
    return dest - reinterpret_cast<index_t *> (pDestIndex);
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_mergeDedupTimestampWithLongIndexIntKeys(
        JAVA_STATIC,
        jlong srcTimestampAddr,
        jlong mergeDataLo,
        jlong mergeDataHi,
        jlong sortedTimestampsAddr,
        jlong mergeOOOLo,
        jlong mergeOOOHi,
        jlong tempIndexAddr,
        jint dedupKeyCount,
        jlong dedupColBuffs
) {
    auto *src = reinterpret_cast<uint64_t *> (srcTimestampAddr);
    auto data_lo = __JLONG_REINTERPRET_CAST__(int64_t, mergeDataLo);
    auto data_hi = __JLONG_REINTERPRET_CAST__(int64_t, mergeDataHi);
    auto *index = reinterpret_cast<index_t *> (sortedTimestampsAddr);
    auto index_lo = __JLONG_REINTERPRET_CAST__(int64_t, mergeOOOLo);
    auto index_hi = __JLONG_REINTERPRET_CAST__(int64_t, mergeOOOHi);
    auto *index_tmp = reinterpret_cast<index_t *> (tempIndexAddr);

    const auto src_keys = reinterpret_cast<const dedup_column *>(dedupColBuffs);
    if (dedupKeyCount == 1) {
        const dedup_column *col_key = &src_keys[0];
        switch (col_key->value_size_bytes) {
            // inline comparison of 1 column type without any loops and switches
            case 1: {
                return merge_dedup_long_index_int_keys(
                        src, data_lo, data_hi,
                        index, index_lo, index_hi, index_tmp,
                        *reinterpret_cast<const MergeColumnComparer<int8_t> *>(src_keys)
                );
            }
            case 2: {
                return merge_dedup_long_index_int_keys(
                        src, data_lo, data_hi,
                        index, index_lo, index_hi, index_tmp,
                        *reinterpret_cast<const MergeColumnComparer<int16_t> *>(src_keys)
                );
            }
            case 4: {
                return merge_dedup_long_index_int_keys(
                        src, data_lo, data_hi,
                        index, index_lo, index_hi,
                        index_tmp,
                        *reinterpret_cast<const MergeColumnComparer<int32_t> *>(src_keys)
                );
            }
            case 8: {
                return merge_dedup_long_index_int_keys(
                        src, data_lo, data_hi,
                        index, index_lo, index_hi,
                        index_tmp,
                        *reinterpret_cast<const MergeColumnComparer<int64_t> *>(src_keys)
                );
            }
            case 16: {
                return merge_dedup_long_index_int_keys(
                        src, data_lo, data_hi,
                        index, index_lo, index_hi,
                        index_tmp,
                        *reinterpret_cast<const MergeColumnComparer<__int128> *>(src_keys)
                );
            }
            case 32: {
                return merge_dedup_long_index_int_keys(
                        src, data_lo, data_hi,
                        index, index_lo, index_hi,
                        index_tmp,
                        *reinterpret_cast<const MergeColumnComparer<int256> *>(src_keys)
                );
            }
            case -1: {
                switch ((ColumnType) (col_key->column_type)) {
                    case ColumnType::VARCHAR: {
                        return merge_dedup_long_index_int_keys(
                                src, data_lo, data_hi,
                                index, index_lo, index_hi,
                                index_tmp,
                                *reinterpret_cast<const MergeVarcharColumnComparer *>(src_keys)
                        );
                    }
                    case ColumnType::STRING: {
                        return merge_dedup_long_index_int_keys(
                                src, data_lo, data_hi,
                                index, index_lo, index_hi,
                                index_tmp,
                                *reinterpret_cast<const MergeStrBinColumnComparer<int32_t, 2> *>(src_keys)
                        );
                    }
                    case ColumnType::BINARY: {
                        return merge_dedup_long_index_int_keys(
                                src, data_lo, data_hi,
                                index, index_lo, index_hi,
                                index_tmp,
                                *reinterpret_cast<const MergeStrBinColumnComparer<int64_t, 1> *>(src_keys)
                        );
                    }
                    default: {
                        assertm(false, "unsupported column type");
                        return 0;
                    }
                }
            }
            default:
                assertm(false, "unsupported column value_size_bytes for comparison");
                return 0;
        }
    }

    // Multiple column dedup
    const auto compareIndexes = [&](const int64_t l, const int64_t r) -> int {
        for (int c = 0; c < dedupKeyCount; c++) {
            const dedup_column *col_key = &src_keys[c];
            int diff;
            switch (col_key->value_size_bytes) {
                case 1: {
                    const auto &comparer = *reinterpret_cast<const MergeColumnComparer<int8_t> *>(col_key);
                    diff = comparer(l, r);
                    break;
                }
                case 2: {
                    const auto &comparer = *reinterpret_cast<const MergeColumnComparer<int16_t> *>(col_key);
                    diff = comparer(l, r);
                    break;
                }
                case 4: {
                    const auto &comparer = *reinterpret_cast<const MergeColumnComparer<int32_t> *>(col_key);
                    diff = comparer(l, r);
                    break;
                }
                case 8: {
                    const auto &comparer = *reinterpret_cast<const MergeColumnComparer<int64_t> *>(col_key);
                    diff = comparer(l, r);
                    break;
                }
                case 16: {
                    const auto &comparer = *reinterpret_cast<const MergeColumnComparer<__int128> *>(col_key);
                    diff = comparer(l, r);
                    break;
                }
                case 32: {
                    const auto &comparer = *reinterpret_cast<const MergeColumnComparer<int256> *>(col_key);
                    diff = comparer(l, r);
                    break;
                }
                case -1: {
                    switch ((ColumnType) (col_key->column_type)) {
                        case ColumnType::VARCHAR: {
                            const auto &comparer = *reinterpret_cast<const MergeVarcharColumnComparer *>(col_key);
                            diff = comparer(l, r);
                            break;
                        }
                        case ColumnType::STRING: {
                            const auto &comparer = *reinterpret_cast<const MergeStrBinColumnComparer<int32_t, 2> *>(col_key);
                            diff = comparer(l, r);
                            break;
                        }
                        case ColumnType::BINARY: {
                            const auto &comparer = *reinterpret_cast<const MergeStrBinColumnComparer<int64_t, 1> *>(col_key);
                            diff = comparer(l, r);
                            break;
                        }
                        default: {
                            assertm(false, "unsupported column type");
                            return 0;
                        }
                    }
                    break;
                }
                default:
                    assertm(false, "unsupported column value_size_bytes");
                    return 0;
            }
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    };
    return merge_dedup_long_index_int_keys(src, data_lo, data_hi, index, index_lo, index_hi, index_tmp, compareIndexes);
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_dedupSortedTimestampIndex(
        JAVA_STATIC,
        jlong pIndexIn,
        jlong count,
        jlong pIndexOut,
        jlong pIndexTemp,
        const jint dedupKeyCount,
        jlong dedupColBuffs
) {
    const auto *index_in = reinterpret_cast<const index_t *> (pIndexIn);
    const auto index_count = __JLONG_REINTERPRET_CAST__(int64_t, count);
    auto *index_out = reinterpret_cast<index_t *> (pIndexOut);
    auto *index_temp = reinterpret_cast<index_t *> (pIndexTemp);

    if (dedupKeyCount == 0) {
        return dedup_sorted_timestamp_index(index_in, index_count, index_out);
    } else {
        const auto src_keys = reinterpret_cast<const dedup_column *>(dedupColBuffs);

        if (dedupKeyCount == 1) {
            const dedup_column *col_key = &src_keys[0];
            switch (col_key->value_size_bytes) {
                // inline comparison of 1 column type without any loops and switches
                case 1:
                    return dedup_sorted_timestamp_index_with_keys(
                            index_in, index_count, index_out, index_temp,
                            *reinterpret_cast<const SortColumnComparer<int8_t> *>(src_keys)
                    );
                case 2:
                    return dedup_sorted_timestamp_index_with_keys(
                            index_in, index_count, index_out, index_temp,
                            *reinterpret_cast<const SortColumnComparer<int16_t> *>(src_keys)
                    );
                case 4:
                    return dedup_sorted_timestamp_index_with_keys(
                            index_in, index_count, index_out, index_temp,
                            *reinterpret_cast<const SortColumnComparer<int32_t> *>(src_keys)
                    );
                case 8:
                    return dedup_sorted_timestamp_index_with_keys(
                            index_in, index_count, index_out, index_temp,
                            *reinterpret_cast<const SortColumnComparer<int64_t> *>(src_keys)
                    );
                case 16:
                    return dedup_sorted_timestamp_index_with_keys(
                            index_in, index_count, index_out, index_temp,
                            *reinterpret_cast<const SortColumnComparer<__int128> *>(src_keys)
                    );
                case 32:
                    return dedup_sorted_timestamp_index_with_keys(
                            index_in, index_count, index_out, index_temp,
                            *reinterpret_cast<const SortColumnComparer<int256> *>(src_keys)
                    );
                case -1:
                    switch ((ColumnType) (col_key->column_type)) {
                        case ColumnType::VARCHAR:
                            return dedup_sorted_timestamp_index_with_keys(
                                    index_in, index_count, index_out, index_temp,
                                    *reinterpret_cast<const SortVarcharColumnComparer *>(src_keys)
                            );
                        case ColumnType::STRING:
                            return dedup_sorted_timestamp_index_with_keys(
                                    index_in, index_count, index_out, index_temp,
                                    *reinterpret_cast<const SortStrBinColumnComparer<int32_t, 2> *>(src_keys)
                            );
                        case ColumnType::BINARY:
                            return dedup_sorted_timestamp_index_with_keys(
                                    index_in, index_count, index_out, index_temp,
                                    *reinterpret_cast<const SortStrBinColumnComparer<int64_t, 1> *>(src_keys)
                            );
                        default:
                            assertm(false, "unsupported column type");
                            return -1;

                    }
                default:
                    assertm(false, "unsupported column type");
                    return -1;
            }
        }

        const auto diff_l = [&](const int64_t l, const int64_t r) {
            for (int c = 0; c < dedupKeyCount; c++) {
                const dedup_column *col_key = &src_keys[c];
                int diff;
                switch (col_key->value_size_bytes) {
                    case 1: {
                        const auto &comparer = *reinterpret_cast<const SortColumnComparer<int8_t> *>(col_key);
                        diff = comparer(l, r);
                        break;
                    }
                    case 2: {
                        const auto &comparer = *reinterpret_cast<const SortColumnComparer<int16_t> *>(col_key);
                        diff = comparer(l, r);
                        break;
                    }
                    case 4: {
                        const auto &comparer = *reinterpret_cast<const SortColumnComparer<int32_t> *>(col_key);
                        diff = comparer(l, r);
                        break;
                    }
                    case 8: {
                        const auto &comparer = *reinterpret_cast<const SortColumnComparer<int64_t> *>(col_key);
                        diff = comparer(l, r);
                        break;
                    }
                    case 16: {
                        const auto &comparer = *reinterpret_cast<const SortColumnComparer<__int128> *>(col_key);
                        diff = comparer(l, r);
                        break;
                    }
                    case 32: {
                        const auto &comparer = *reinterpret_cast<const SortColumnComparer<int256> *>(col_key);
                        diff = comparer(l, r);
                        break;
                    }
                    case -1: {
                        switch ((ColumnType) (col_key->column_type)) {
                            case ColumnType::VARCHAR: {
                                const auto &comparer = *reinterpret_cast<const SortVarcharColumnComparer *>(col_key);
                                diff = comparer(l, r);
                                break;
                            }
                            case ColumnType::STRING: {
                                const auto &comparer = *reinterpret_cast<const SortStrBinColumnComparer<int32_t, 2> *>(col_key);
                                diff = comparer(l, r);
                                break;
                            }
                            case ColumnType::BINARY: {
                                const auto &comparer = *reinterpret_cast<const SortStrBinColumnComparer<int64_t, 1> *>(col_key);
                                diff = comparer(l, r);
                                break;
                            }
                            default: {
                                assertm(false, "unsupported column type");
                                return -1;
                            }
                        }
                        break;
                    }
                    default:
                        assertm(false, "unsupported column type");
                        return -1;
                }
                if (diff != 0) {
                    return diff;
                }
            }
            return 0;
        };

        return dedup_sorted_timestamp_index_with_keys(index_in, index_count, index_out, index_temp, diff_l);
    }
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_dedupSortedTimestampIndexManyAddresses(
        JAVA_STATIC,
        jlong indexFormat,
        jlong pIndexIn,
        jlong pIndexTemp,
        jint dedupKeyCount,
        jlong dedupColBuffs
) {
    auto segment_bytes = read_segment_bytes(indexFormat);
    auto dedup_key_count = (int32_t) dedupKeyCount;
    auto format = read_format(indexFormat);

    if (format != dedup_index_format) {
        return merge_index_format(-1, 0, 0, 0);
    }

    const auto src_keys = reinterpret_cast<const dedup_column *>(dedupColBuffs);
    auto dedup_row_count = (int64_t) dedup_sorted_timestamp_index_many_addresses_segment_bits_row_encoding(
            segment_bytes,
            pIndexIn,
            pIndexIn,
            indexFormat,
            pIndexTemp,
            dedup_key_count,
            src_keys
    );

    auto reverse_index_bytes = read_reverse_index_format_bytes(indexFormat);
    if (dedup_row_count == no_timestamp_duplicates) {
        return merge_index_format(dedup_row_count, reverse_index_bytes, segment_bytes, shuffle_index_format);
    }
    return merge_index_format(dedup_row_count, reverse_index_bytes, segment_bytes, dedup_shuffle_index_format);
}


JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_dedupMergeStrBinColumnSize(
        JNIEnv *env, jclass cl,
        jlong merge_index_addr,
        jlong merge_index_row_count,
        jlong src_data_fix_addr,
        jlong src_ooo_fix_addr
) {
    auto merge_index = reinterpret_cast<index_t *>(merge_index_addr);
    auto src_ooo_fix = reinterpret_cast<int64_t *>(src_ooo_fix_addr);
    auto src_data_fix = reinterpret_cast<int64_t *>(src_data_fix_addr);
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    int64_t dst_var_offset = 0;

    for (int64_t l = 0; l < merge_index_row_count; l++) {
        MM_PREFETCH_T0(merge_index + l + 64);
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const uint64_t rr = row & ~(1ull << 63);
        const int64_t size = src_fix[bit][rr + 1] - src_fix[bit][rr];
        dst_var_offset += size;
    }
    return dst_var_offset;
}


JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_dedupMergeVarcharColumnSize(JNIEnv *env, jclass cl,
                                                     jlong merge_index_addr,
                                                     jlong merge_index_row_count,
                                                     jlong src_data_fix_addr,
                                                     jlong src_ooo_fix_addr) {
    auto merge_index = reinterpret_cast<index_t *>(merge_index_addr);
    auto src_ooo_fix = reinterpret_cast<int64_t *>(src_ooo_fix_addr);
    auto src_data_fix = reinterpret_cast<int64_t *>(src_data_fix_addr);
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    int64_t dst_var_offset = 0;

    for (int64_t l = 0; l < merge_index_row_count; l++) {
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const uint64_t rr = row & ~(1ull << 63);
        const int64_t firstWord = src_fix[bit][rr * 2];

        if ((firstWord & 1) == 0 && (firstWord & 4) == 0) {
            // not inlined and not null
            auto size = (firstWord >> 4) & 0xffffff;
            dst_var_offset += size;
        }
    }
    return dst_var_offset;
}

} // extern C


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


template<typename IsNotNull, typename AreNotEqual>
inline bool is_column_merge_identical(
        int64_t column_top1, int64_t lo1, int64_t hi1,
        int64_t column_top2, int64_t lo2, int64_t hi2,
        const index_t *merge_index, int64_t merge_index_rows,
        AreNotEqual &&are_not_equal_lambda,
        IsNotNull &&is_not_null_lambda
) {
    if (column_top1 >= hi1) {
        // All old values are nulls, check that the new values are null
        for (int64_t i = std::max<int64_t>(lo2 - column_top2, 0); i < hi2 - column_top2; i++) {
            if (is_not_null_lambda(1, i)) {
                return false;
            }
        }
        return true;
    }

    for (int64_t out_index = 0; out_index < merge_index_rows; out_index++) {
        uint64_t index = merge_index[out_index].i;
        const uint32_t bit = (index >> 63);

        // Row is replaced, check if the data is the same
        if (bit == 0) {
            auto merge_side_index = (int64_t) (index & ~(1ull << 63));

            const auto data1_index = out_index + lo1 - column_top1;
            const auto data2_index = merge_side_index - column_top2;

            if (data1_index < 0) {
                // data1 points to column top, check new data2 is also null
                if (data2_index > -1 && is_not_null_lambda(1, data2_index)) {
                    return false;
                }
            } else {
                if (data2_index > -1) {
                    // data was not null, check new data is the same
                    if (are_not_equal_lambda(data1_index, data2_index)) {
                        return false;
                    }
                } else {
                    // data2 points to column top, check new data is also null
                    if (is_not_null_lambda(0, data1_index)) {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

template<typename IsNotNull, typename AreNotEqual>
inline bool is_column_replace_identical(
        int64_t column_top1, int64_t lo1, int64_t hi1,
        int64_t column_top2, int64_t lo2, int64_t hi2,
        AreNotEqual &&are_not_equal_lambda, IsNotNull &&is_not_null_lambda
) {
    column_top1 = std::min<int64_t>(column_top1, hi1);
    column_top2 = std::min<int64_t>(column_top2, hi2);
    auto row_count = hi1 - lo1;

    if (row_count != hi2 - lo2) {
        // Different row counts, cannot be identical
        return false;
    }

    int64_t min_col_top_rel = std::max<int64_t>(
            std::min<int64_t>(column_top1 - lo1, column_top2 - lo2),
            0
    );

    // If first column has column top sticking out of min_col_top_rel,
    // check that second column values are nulls in that range
    auto col1_col_top_sticks_out_count = column_top1 - lo1;
    if (col1_col_top_sticks_out_count > min_col_top_rel
        && is_not_null_lambda(
            1,
            lo2 - column_top2 + min_col_top_rel,
            lo2 - column_top2 + col1_col_top_sticks_out_count)) {
        return false;
    }

    // If second column has column top sticking out of min_col_top_rel,
    // check that second column values are nulls in that range
    auto col2_col_top_sticks_out_count = column_top2 - lo2;
    if (col2_col_top_sticks_out_count > min_col_top_rel
        && is_not_null_lambda(
            0,
            lo1 - column_top1 + min_col_top_rel,
            lo1 - column_top1 + col2_col_top_sticks_out_count)) {
        return false;
    }

    auto max_col_top_rel = std::max<int64_t>(
            std::max<int64_t>(column_top1 - lo1, column_top2 - lo2),
            0
    );
    auto compare_row_count = row_count - max_col_top_rel;
    if (compare_row_count > 0 && are_not_equal_lambda(lo1 + max_col_top_rel, lo2 + max_col_top_rel, compare_row_count)) {
        return false;
    }
    return true;
}

template<typename T>
bool is_fixed_column_merge_identical(
        int64_t column_top1, int64_t lo1, int64_t hi1, const T *data1,
        int64_t column_top2, int64_t lo2, int64_t hi2, const T *data2,
        const index_t *merge_index, int64_t merge_index_rows,
        T null_value
) {
    const T *data_ptrs[2] = {data1, data2};
    if (merge_index != nullptr) {
        return is_column_merge_identical(
                column_top1, lo1, hi1,
                column_top2, lo2, hi2,
                merge_index, merge_index_rows,
                // are_not_equal
                [&](int64_t index1, int64_t index2) -> bool {
                    return data1[index1] != data2[index2];
                },
                // is_not_null_lambda
                [&](int data_index, int64_t index) -> bool {
                    return data_ptrs[data_index][index] != null_value;
                }
        );
    } else {
        return is_column_replace_identical(
                column_top1, lo1, hi1,
                column_top2, lo2, hi2,
                // are_not_equal
                [&](int64_t lo1, int64_t lo2, int64_t size) -> bool {
                    return memcmp(data1 + lo1, data2 + lo2, size * sizeof(T)) != 0;
                },
                // is_not_null_lambda
                [&](int data_index, int64_t lo, int64_t hi) -> bool {
                    auto data = data_ptrs[data_index];
                    for (int64_t i = lo; i < hi; i++) {
                        if (data[i] != null_value) {
                            return true;
                        }
                    }
                    return false;
                }
        );
    }
}

bool is_varchar_column_merge_identical(
        int64_t column_top1, int64_t lo1_pos, int64_t hi1_pos, const VarcharAuxEntryInlined *aux1, const uint8_t *data1,
        int64_t column_top2, int64_t lo2_pos, int64_t hi2_pos, const VarcharAuxEntryInlined *aux2, const uint8_t *data2,
        const index_t *merge_index, int64_t merge_index_rows
) {
    const VarcharAuxEntryInlined *aux_ptrs[2] = {aux1, aux2};
    if (merge_index != nullptr) {
        return is_column_merge_identical(
                column_top1, lo1_pos, hi1_pos,
                column_top2, lo2_pos, hi2_pos,
                merge_index, merge_index_rows,
                // are_not_equal
                [&](int64_t index1, int64_t index2) -> bool {
                    return compare_varchar(
                            (uint8_t *) aux1, data1, std::numeric_limits<int64_t>::max(), index1,
                            (uint8_t *) aux2, data2, std::numeric_limits<int64_t>::max(), index2
                    ) != 0;
                },
                // is_not_null_lambda
                [&](int data_index, int64_t index) -> bool {
                    return !(aux_ptrs[data_index][index].header & HEADER_FLAG_NULL);
                }
        );
    } else {
        return is_column_replace_identical(
                column_top1, lo1_pos, hi1_pos,
                column_top2, lo2_pos, hi2_pos,
                // are_not_equal
                [&](int64_t lo1, int64_t lo2, int64_t size) -> bool {

                    // Aux entry is 10 bytes of meta + 6 bytes of pointer for both split and inlined
                    // Compare first 10 bytes of aux entries
                    auto aux1_cmp = reinterpret_cast<const VarcharAuxEntryBoth *>(aux1 + lo1);
                    auto aux2_cmp = reinterpret_cast<const VarcharAuxEntryBoth *>(aux2 + lo2);
                    for (int64_t i = 0; i < size; i++) {
                        auto aux1_entry = aux1_cmp[i];
                        auto aux2_entry = aux2_cmp[i];
                        if (aux1_entry.header1 != aux2_entry.header1 ||
                            aux1_entry.header2 != aux2_entry.header2) {
                            return true;
                        }
                    }
                    // Compare data
                    auto data1_offset_lo = aux1_cmp[0].get_data_offset();
                    auto data1_offset_hi = aux1_cmp[size - 1].get_data_offset();
                    auto data2_offset_lo = aux2_cmp[0].get_data_offset();
                    auto data2_offset_hi = aux2_cmp[size - 1].get_data_offset();
                    if (data1_offset_hi - data1_offset_lo != data2_offset_hi - data2_offset_lo) {
                        return true;
                    }
                    if (memcmp(data1 + data1_offset_lo, data2 + data2_offset_lo,
                               data1_offset_hi - data1_offset_lo) != 0) {
                        return true;
                    }
                    return false;
                },
                // is_not_null_lambda
                [&](int data_index, int64_t lo, int64_t hi) -> bool {
                    auto aux = aux_ptrs[data_index];
                    for (int64_t i = lo; i < hi; i++) {
                        if (!(aux[i].header & HEADER_FLAG_NULL)) {
                            return true;
                        }
                    }
                    return false;
                }
        );
    }
}

template<typename T>
bool is_str_bin_column_merge_identical(
        int64_t column_top1, int64_t lo1_pos, int64_t hi1_pos, const int64_t *aux1, const uint8_t *data1,
        int64_t column_top2, int64_t lo2_pos, int64_t hi2_pos, const int64_t *aux2, const uint8_t *data2,
        const index_t *merge_index, int64_t merge_index_rows, int32_t char_size_bytes
) {
    constexpr T null_len(-1);
    const int64_t *aux_ptrs[2] = {aux1, aux2};
    const uint8_t *data_ptrs[2] = {data1, data2};
    if (merge_index != nullptr) {
        return is_column_merge_identical(
                column_top1, lo1_pos, hi1_pos,
                column_top2, lo2_pos, hi2_pos,
                merge_index, merge_index_rows,
                // are_not_equal
                [&](int64_t index1, int64_t index2) -> bool {
                    // data was not null, check new data is the same
                    auto data1_offset = aux1[index1];
                    auto data2_offset = aux2[index2];
                    T len1 = *(T * )(data1 + data1_offset);
                    T len2 = *(T * )(data2 + data2_offset);

                    return len1 != len2 ||
                           (len1 > 0 && memcmp(
                                   data1 + data1_offset + sizeof(T),
                                   data2 + data2_offset + sizeof(T),
                                   len1 * char_size_bytes) != 0);

                },
                // is_not_null_lambda
                [&](int data_index, int64_t index) -> bool {
                    auto offset = aux_ptrs[data_index][index];
                    auto data = data_ptrs[data_index];
                    return *(T * )(data + offset) != null_len;
                }
        );
    } else {
        return is_column_replace_identical(
                column_top1, lo1_pos, hi1_pos,
                column_top2, lo2_pos, hi2_pos,
                [&](int64_t lo1, int64_t lo2, int64_t size) -> bool {

                    // Compare data
                    auto data1_offset_lo = aux1[lo1];
                    auto data1_offset_hi = aux1[lo1 + size - 1];
                    auto data2_offset_lo = aux2[lo2];
                    auto data2_offset_hi = aux2[lo2 + size - 1];
                    if (data1_offset_hi - data1_offset_lo != data2_offset_hi - data2_offset_lo
                        || memcmp(
                            data1 + data1_offset_lo,
                            data2 + data2_offset_lo,
                            data1_offset_hi - data1_offset_lo
                    ) != 0) {
                        return true;
                    }
                    return false;
                },
                [&](int data_index, int64_t lo, int64_t hi) -> bool {
                    auto data1_offset_lo = aux_ptrs[data_index][lo];
                    auto data1_offset_hi = aux_ptrs[data_index][hi];

                    // Check that data length contains only a length entry for each row
                    if (data1_offset_hi - data1_offset_lo != sizeof(T) * (hi - lo)) {
                        return true;
                    }

                    // Check that the data has all length as -1
                    auto data = data_ptrs[data_index];
                    for (int64_t o = data1_offset_lo; o < data1_offset_hi; o += sizeof(T)) {
                        if (*(T * )(data + o) != null_len) {
                            return true;
                        }
                    }
                    return false;
                }
        );
    }
}

bool is_array_column_merge_identical(
        int64_t column_top1, int64_t lo1_pos, int64_t hi1_pos, const ArrayAuxEntry *aux1, const uint8_t *data1,
        int64_t column_top2, int64_t lo2_pos, int64_t hi2_pos, const ArrayAuxEntry *aux2, const uint8_t *data2,
        const index_t *merge_index, int64_t merge_index_rows
) {
    const ArrayAuxEntry *aux_ptrs[2] = {aux1, aux2};
    if (merge_index != nullptr) {
        return is_column_merge_identical(
                column_top1, lo1_pos, hi1_pos,
                column_top2, lo2_pos, hi2_pos,
                merge_index, merge_index_rows,
                // are_not_equal
                [&](int64_t index1, int64_t index2) -> bool {
                    int32_t size1 = aux1[index1].data_size;
                    int32_t size2 = aux2[index2].data_size;
                    if (size1 != size2) {
                        return true;
                    }
                    auto offset1 = aux1[index1].offset_48 & ARRAY_OFFSET_MAX;
                    auto offset2 = aux2[index2].offset_48 & ARRAY_OFFSET_MAX;
                    return memcmp(data1 + offset1, data2 + offset2, size1) != 0;
                },
                // is_not_null_lambda
                [&](int data_index, int64_t index) -> bool {
                    return aux_ptrs[data_index][index].data_size > 0;
                }
        );
    } else {
        return is_column_replace_identical(
                column_top1, lo1_pos, hi1_pos,
                column_top2, lo2_pos, hi2_pos,
                // are_not_equal
                [&](int64_t lo1, int64_t lo2, int64_t size) -> bool {
                    auto data1_offset_lo = aux1[lo1].offset_48 & ARRAY_OFFSET_MAX;
                    auto data1_offset_hi = aux1[lo1 + size - 1].offset_48 & ARRAY_OFFSET_MAX;
                    auto data2_offset_lo = aux2[lo2].offset_48 & ARRAY_OFFSET_MAX;
                    auto data2_offset_hi = aux2[lo2 + size - 1].offset_48 & ARRAY_OFFSET_MAX;

                    if (data1_offset_hi - data1_offset_lo != data2_offset_hi - data2_offset_lo) {
                        // Compare array sizes
                        for (int64_t i = 0; i < size; i++) {
                            if (aux1[lo1 + i].data_size != aux2[lo2 + i].data_size) {
                                return true;
                            }
                        }

                        // Compare array data
                        if (memcmp(
                                data1 + data1_offset_lo,
                                data2 + data2_offset_lo,
                                data1_offset_hi - data1_offset_lo
                        ) != 0)
                            return true;
                    }
                    return false;
                },

                // is_not_null_lambda
                [&](int data_index, int64_t lo, int64_t hi) -> bool {
                    auto aux = aux_ptrs[data_index];
                    for (int64_t i = lo; i < hi; i++) {
                        if (aux[i].data_size > 0) {
                            return true;
                        }
                    }
                    return false;
                }
        );
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
    auto reverse_index_bytes = read_reverse_index_format_bytes(indexFormat);
    auto index_count = read_row_count(indexFormat);

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

    if (dedup_row_count == no_timestamp_duplicates) {
        return merge_index_format(index_count, reverse_index_bytes, segment_bytes, shuffle_index_format);
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

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_dedupMergeArrayColumnSize(JNIEnv *env, jclass cl,
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
        // add up non-zero array sizes
        dst_var_offset += std::max<int64_t>(0LL, src_fix[bit][rr * 2 + 1] & ARRAY_SIZE_MAX);
    }
    return dst_var_offset;
}

JNIEXPORT bool JNICALL
Java_io_questdb_cairo_frm_FrameAlgebra_isColumnReplaceIdentical(
        JNIEnv *env, jclass cl,
        jint columnType,
        jint valueSizeBytes,
        jlong columnTop1,
        jlong lo1,
        jlong hi1,
        jlong auxAddr1,
        jlong dataAddr1,
        jlong columnTop2,
        jlong lo2,
        jlong hi2,
        jlong auxAddr2,
        jlong dataAddr2,
        jlong mergeIndexAddr,
        jlong mergeIndexRows,
        jlong nullLong,
        jlong nullLong1,
        jlong nullLong2,
        jlong nullLong3
) {
    auto merge_index = reinterpret_cast<index_t *>(mergeIndexAddr);
    auto merge_index_rows = __JLONG_REINTERPRET_CAST__(int64_t, mergeIndexRows);
    auto column_top1 = __JLONG_REINTERPRET_CAST__(int64_t, columnTop1);
    auto column_top2 = __JLONG_REINTERPRET_CAST__(int64_t, columnTop2);
    auto data1 = reinterpret_cast<void *>(dataAddr1);
    auto data2 = reinterpret_cast<void *>(dataAddr2);
    auto aux1 = reinterpret_cast<void *>(auxAddr1);
    auto aux2 = reinterpret_cast<void *>(auxAddr2);
    auto lo1_pos = __JLONG_REINTERPRET_CAST__(int64_t, lo1);
    auto hi1_pos = __JLONG_REINTERPRET_CAST__(int64_t, hi1);
    auto lo2_pos = __JLONG_REINTERPRET_CAST__(int64_t, lo2);
    auto hi2_pos = __JLONG_REINTERPRET_CAST__(int64_t, hi2);

    auto value_size_bytes = (int32_t) valueSizeBytes;
    auto column_type = (int32_t) columnType;

    auto null_long = __JLONG_REINTERPRET_CAST__(int64_t, nullLong);
    auto null_long1 = __JLONG_REINTERPRET_CAST__(int64_t, nullLong1);
    auto null_long2 = __JLONG_REINTERPRET_CAST__(int64_t, nullLong2);
    auto null_long3 = __JLONG_REINTERPRET_CAST__(int64_t, nullLong3);
    int64_t null_val[4] = {null_long, null_long1, null_long2, null_long3};
    void *null_ptr = static_cast<void *>(null_val);

    switch (value_size_bytes) {
        case 1: {
            return is_fixed_column_merge_identical<int8_t>(
                    column_top1, lo1_pos, hi1_pos, (int8_t *) data1,
                    column_top2, lo2_pos, hi2_pos, (int8_t *) data2,
                    merge_index, merge_index_rows,
                    *reinterpret_cast<const int8_t *>(null_ptr)
            );
        }
        case 2: {
            return is_fixed_column_merge_identical<int16_t>(
                    column_top1, lo1_pos, hi1_pos, (int16_t *) data1,
                    column_top2, lo2_pos, hi2_pos, (int16_t *) data2,
                    merge_index, merge_index_rows,
                    *reinterpret_cast<const int16_t *>(null_ptr)
            );
        }
        case 4: {
            return is_fixed_column_merge_identical<int32_t>(
                    column_top1, lo1_pos, hi1_pos, (int32_t *) data1,
                    column_top2, lo2_pos, hi2_pos, (int32_t *) data2,
                    merge_index, merge_index_rows,
                    *reinterpret_cast<const int32_t *>(null_ptr)
            );
        }
        case 8: {
            return is_fixed_column_merge_identical<int64_t>(
                    column_top1, lo1_pos, hi1_pos, (int64_t *) data1,
                    column_top2, lo2_pos, hi2_pos, (int64_t *) data2,
                    merge_index, merge_index_rows,
                    *reinterpret_cast<const int64_t *>(null_ptr)
            );
        }
        case 16: {
            return is_fixed_column_merge_identical<__int128>(
                    column_top1, lo1_pos, hi1_pos, (__int128 *) data1,
                    column_top2, lo2_pos, hi2_pos, (__int128 *) data2,
                    merge_index, merge_index_rows,
                    *reinterpret_cast<const __int128 *>(null_ptr)
            );
        }
        case 32: {
            return is_fixed_column_merge_identical<int256>(
                    column_top1, lo1_pos, hi1_pos, (int256 *) data1,
                    column_top2, lo2_pos, hi2_pos, (int256 *) data2,
                    merge_index, merge_index_rows,
                    *reinterpret_cast<const int256 *>(null_ptr)
            );
        }
        case -1: {
            switch ((ColumnType) (column_type)) {
                case ColumnType::VARCHAR: {
                    return is_varchar_column_merge_identical(
                            column_top1, lo1_pos, hi1_pos, (const VarcharAuxEntryInlined *) aux1,
                            (const uint8_t *) data1,
                            column_top2, lo2_pos, hi2_pos, (const VarcharAuxEntryInlined *) aux2,
                            (const uint8_t *) data2,
                            merge_index, merge_index_rows
                    );
                }
                case ColumnType::STRING: {
                    return is_str_bin_column_merge_identical<int32_t>(
                            column_top1, lo1_pos, hi1_pos, (const int64_t *) aux1, (const uint8_t *) data1,
                            column_top2, lo2_pos, hi2_pos, (const int64_t *) aux2, (const uint8_t *) data2,
                            merge_index, merge_index_rows, 2
                    );
                }
                case ColumnType::BINARY: {
                    return is_str_bin_column_merge_identical<int64_t>(
                            column_top1, lo1_pos, hi1_pos, (const int64_t *) aux1, (const uint8_t *) data1,
                            column_top2, lo2_pos, hi2_pos, (const int64_t *) aux2, (const uint8_t *) data2,
                            merge_index, merge_index_rows, 1
                    );
                }
                case ColumnType::ARRAY: {
                    return is_array_column_merge_identical(
                            column_top1, lo1_pos, hi1_pos, (const ArrayAuxEntry *) aux1, (const uint8_t *) data1,
                            column_top2, lo2_pos, hi2_pos, (const ArrayAuxEntry *) aux2, (const uint8_t *) data2,
                            merge_index, merge_index_rows
                    );
                }
                default:
                    assertm(false, "unsupported column type");
                    return false;
            }
        }
        default:
            assertm(false, "unsupported column type");
            return false;
    }
}

JNIEXPORT bool JNICALL
Java_io_questdb_cairo_frm_FrameAlgebra_isDesignatedTimestampColumnReplaceIdentical0(
        JNIEnv *env, jclass cl,
        jlong partitionColumnAddr,
        jlong commitColumnAddr,
        jlong rowCount
) {
    auto data1 = reinterpret_cast<int64_t *>(partitionColumnAddr);
    auto data2 = reinterpret_cast<index_l *>(commitColumnAddr);
    auto row_count = __JLONG_REINTERPRET_CAST__(int64_t, rowCount);

    for(int64_t i = 0; i < row_count; i++) {
        if (data1[i] != data2[i].ts) {
            return false;
        }
    }
    return true;
}

} // extern C

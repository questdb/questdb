/*+*****************************************************************************
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

#ifndef QUESTDB_OOO_DISPATCH_H
#define QUESTDB_OOO_DISPATCH_H

#include "dispatcher.h"
#include "util.h"

struct index_l {
    int64_t ts;
    uint64_t i;
};

struct index_t {
    uint64_t ts;
    uint64_t i;

    bool operator<(const index_t& other) const {
        return ts < other.ts;
    }

    bool operator<(uint64_t other) const {
        return ts < other;
    }

    bool operator>(uint64_t other) const {
        return ts > other;
    }

    bool operator>(const index_t& other) const {
        return ts > other.ts;
    }

    bool operator==(index_t other) const {
        return ts == other.ts;
    }

    bool operator==(uint64_t other) const {
        return ts == other;
    }

    bool operator<=(index_t other) const {
        return ts <= other.ts;
    }

    uint64_t operator>>(uint64_t shr) const {
        return ts >> shr;
    }

    uint64_t operator&(uint64_t mask) const{
        return ts & mask;
    }
};

typedef struct __attribute__ ((packed)) long_128bit {
    uint64_t long0;
    uint64_t long1;
} long_128bit;

typedef struct __attribute__ ((packed)) long_256bit {
    uint64_t long0;
    uint64_t long1;
    uint64_t long2;
    uint64_t long3;
} long_256bit;

const int64_t OFFSET_MAX = (1LL << 48) - 1L;
const int64_t ARRAY_SIZE_MAX = 0xFFFFFFFFLL;

DECLARE_DISPATCHER_TYPE(copy_index_timestamp, index_t *index, int64_t index_lo, int64_t index_hi, int64_t *dest);

DECLARE_DISPATCHER_TYPE(shift_copy, int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest);

DECLARE_DISPATCHER_TYPE(shift_copy_varchar_aux, int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest);

DECLARE_DISPATCHER_TYPE(shift_copy_array_aux, int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest);

DECLARE_DISPATCHER_TYPE(copy_index, const index_t *index, const int64_t count, int64_t *dest);

DECLARE_DISPATCHER_TYPE(set_binary_column_null_refs, int64_t *data, int64_t offset, int64_t count);

DECLARE_DISPATCHER_TYPE(set_string_column_null_refs, int64_t *data, int64_t offset, int64_t count);

DECLARE_DISPATCHER_TYPE(set_varchar_null_refs, int64_t *aux, int64_t offset, int64_t count);

DECLARE_DISPATCHER_TYPE(set_array_null_refs, int64_t *aux, int64_t offset, int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_int64, int64_t *data, const int64_t value, const int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_int32, int32_t *data, const int32_t value, const int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_double, double *data, const double value, const int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_float, float *data, const float value, const int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_short, int16_t *data, const int16_t value, const int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_int128, long_128bit *data, const long_128bit value, const int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_int256, long_256bit *data, const long_256bit value, const int64_t count);

DECLARE_DISPATCHER_TYPE(make_timestamp_index, const int64_t *data, int64_t low, int64_t high, index_t *dest);

DECLARE_DISPATCHER_TYPE(shift_timestamp_index, const index_t *data, int64_t count, index_t *dest);

DECLARE_DISPATCHER_TYPE(flatten_index, index_t *index, int64_t count);

DECLARE_DISPATCHER_TYPE(merge_shuffle_int64, const int64_t *src1, const int64_t *src2, int64_t *dest,
                        const index_t *index, const int64_t count);

DECLARE_DISPATCHER_TYPE(re_shuffle_128bit, const __int128 *src, __int128 *dest, const index_t *index, const int64_t count);

DECLARE_DISPATCHER_TYPE(re_shuffle_256bit, const long_256bit *src, long_256bit *dest, const index_t *index, const int64_t count);

DECLARE_DISPATCHER_TYPE(re_shuffle_int64, const int64_t *src, int64_t *dest, const index_t *index, const int64_t count);

DECLARE_DISPATCHER_TYPE(re_shuffle_int32, const int32_t *src, int32_t *dest, const index_t *index, const int64_t count);

DECLARE_DISPATCHER_TYPE (merge_copy_var_column_int32, index_t *merge_index, int64_t merge_index_size,
                         int64_t *src_data_fix, char *src_data_var, int64_t *src_ooo_fix, char *src_ooo_var,
                         int64_t *dst_fix, char *dst_var, int64_t dst_var_offset);

DECLARE_DISPATCHER_TYPE (merge_copy_varchar_column, index_t *merge_index, int64_t merge_index_size,
                         int64_t *src_data_fix, char *src_data_var, int64_t *src_ooo_fix, char *src_ooo_var,
                         int64_t *dst_fix, char *dst_var, int64_t dst_var_offset);

DECLARE_DISPATCHER_TYPE (merge_copy_var_column_int64, index_t *merge_index, int64_t merge_index_size,
                         int64_t *src_data_fix, char *src_data_var, int64_t *src_ooo_fix, char *src_ooo_var,
                         int64_t *dst_fix, char *dst_var, int64_t dst_var_offset);

DECLARE_DISPATCHER_TYPE (merge_copy_array_column, index_t *merge_index, int64_t merge_index_size,
                         int64_t *src_data_fix, char *src_data_var, int64_t *src_ooo_fix, char *src_ooo_var,
                         int64_t *dst_fix, char *dst_var, int64_t dst_var_offset);

DECLARE_DISPATCHER_TYPE(platform_memcpy, void *dst, const void *src, const size_t len);

DECLARE_DISPATCHER_TYPE(platform_memcmp, const void *a, const void *b, const size_t len, int *res);

DECLARE_DISPATCHER_TYPE(platform_memset, void *dst, const int val, const size_t len);

DECLARE_DISPATCHER_TYPE(platform_memmove, void *dst, const void *src, const size_t len);

// 8 - 11
template<typename T>
inline void
merge_shuffle_vanilla(const T *src1, const T *src2, T *dest, const index_t *index, const int64_t count) {
    const T *sources[] = {src2, src1};
    for (int64_t i = 0; i < count; i++) {
        MM_PREFETCH_T0(index + i + 64);
        const auto r = reinterpret_cast<uint64_t>(index[i].i);
        const uint64_t pick = r >> 63u;
        const auto row = r & ~(1LLu << 63u);
        dest[i] = sources[pick][row];
    };
}

// 7-8
template<class T>
inline void re_shuffle_vanilla(const T *src, T *dest, const index_t *index, const int64_t count) {
    for (int64_t i = 0; i < count; i++) {
        MM_PREFETCH_T0(index + i + 64);
        dest[i] = src[index[i].i];
    };
}

// A column added after a partition was first written has no data for that partition's leading
// `src_data_top` rows (its "column top"): the column file starts at logical row `src_data_top`.
// The plain procs below cannot express that, so callers used to materialize a nulls+data image of
// the source column into scratch space past the source file's end and merge from that instead.
// These variants take the top directly and emit the type's NULL for any data-side row below it,
// which removes the materialization pass entirely -- and with it the scratch write, which is fatal
// when the source file IS the destination file (an append-merge into shared column files).
//
// `src1` (the data side) is UNBIASED here: it points at the column file's first stored row, which
// is logical row `src_data_top`, so a data-side row `r >= src_data_top` reads `src1[r - top]`.
// The o3 side (`src2`) never has a top. Deliberately scalar: these run only for the column-top
// case, and the branch per row defeats vectorization anyway.
template<typename T>
inline void
merge_shuffle_top_vanilla(
        const T *src1,
        const T *src2,
        T *dest,
        const index_t *index,
        const int64_t count,
        const int64_t src_data_top,
        const T *null_value
) {
    const T nv = *null_value;
    for (int64_t i = 0; i < count; i++) {
        const auto r = reinterpret_cast<uint64_t>(index[i].i);
        const uint64_t pick = r >> 63u;
        const auto row = static_cast<int64_t>(r & ~(1LLu << 63u));
        if (pick == 0) {
            dest[i] = src2[row];
        } else {
            const int64_t data_row = row - src_data_top;
            dest[i] = data_row < 0 ? nv : src1[data_row];
        }
    };
}

// STRING (T = int32_t) / BINARY (T = int64_t): the data vector stores [len][payload] and a NULL is
// a bare len of -1, so it still consumes sizeof(T) data bytes -- callers must size the destination
// data vector with `getDataVectorMinEntrySize()` per null, exactly as the scratch path reserved.
template<typename T>
inline void
merge_copy_var_column_top_vanilla(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset,
        T mult,
        int64_t src_data_top
) {
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    char *src_var[] = {src_ooo_var, src_data_var};

    for (int64_t l = 0; l < merge_index_size; l++) {
        dst_fix[l] = dst_var_offset;
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const auto rr = static_cast<int64_t>(row & ~(1ull << 63));
        const int64_t data_row = bit == 1 ? rr - src_data_top : rr;
        if (bit == 1 && data_row < 0) {
            reinterpret_cast<T *>(dst_var + dst_var_offset)[0] = -1;
            dst_var_offset += sizeof(T);
            continue;
        }
        const int64_t offset = src_fix[bit][data_row];
        char *src_var_ptr = src_var[bit] + offset;
        auto len = *reinterpret_cast<T *>(src_var_ptr);
        auto char_count = len > 0 ? len * mult : 0;
        reinterpret_cast<T *>(dst_var + dst_var_offset)[0] = len;
        memcpy(dst_var + dst_var_offset + sizeof(T), src_var_ptr + sizeof(T), char_count);
        dst_var_offset += char_count + sizeof(T);
    }
    if (merge_index_size > 0) {
        dst_fix[merge_index_size] = dst_var_offset;
    }
}

// VARCHAR: 16-byte aux entries; a NULL is the header flag 4 with the running data offset in the
// second word's high 48 bits (mirrors set_varchar_null_refs). Nulls consume no data bytes.
inline void
merge_copy_varchar_column_top_vanilla(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset,
        int64_t src_data_top
) {
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    char *src_var[] = {src_ooo_var, src_data_var};

    for (int64_t l = 0; l < merge_index_size; l++) {
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const auto rr = static_cast<int64_t>(row & ~(1ull << 63));
        const int64_t data_row = bit == 1 ? rr - src_data_top : rr;
        if (bit == 1 && data_row < 0) {
            dst_fix[l * 2] = 4;
            dst_fix[l * 2 + 1] = dst_var_offset << 16;
            continue;
        }
        const int64_t firstWord = src_fix[bit][data_row * 2];
        const int64_t secondWord = src_fix[bit][data_row * 2 + 1];

        auto originalData = secondWord & 0x000000000000ffffLL;
        auto relocatedSecondWord = originalData | (dst_var_offset << 16);
        if ((firstWord & 1) == 0 && (firstWord & 4) == 0) {
            // not inlined and not null
            auto originalOffset = secondWord >> 16;
            auto len = (firstWord >> 4) & 0xffffff;
            memcpy(dst_var + dst_var_offset, src_var[bit] + originalOffset, len);
            dst_var_offset += len;
        }
        dst_fix[l * 2] = firstWord;
        dst_fix[l * 2 + 1] = relocatedSecondWord;
    }
}

// ARRAY: 16-byte aux entries of {data offset, size}; a NULL is size 0 at the running data offset
// (mirrors set_array_null_refs). Nulls consume no data bytes.
inline void
merge_copy_array_column_top_vanilla(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset,
        int64_t src_data_top
) {
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    char *src_var[] = {src_ooo_var, src_data_var};

    for (int64_t l = 0; l < merge_index_size; l++) {
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const auto rr = static_cast<int64_t>(row & ~(1ull << 63));
        const int64_t data_row = bit == 1 ? rr - src_data_top : rr;
        if (bit == 1 && data_row < 0) {
            dst_fix[l * 2] = dst_var_offset & OFFSET_MAX;
            dst_fix[l * 2 + 1] = 0;
            continue;
        }
        const int64_t src_var_offset = src_fix[bit][data_row * 2] & OFFSET_MAX;
        auto size = static_cast<uint32_t>(src_fix[bit][data_row * 2 + 1] & ARRAY_SIZE_MAX);

        const auto relocated_var_offset = dst_var_offset & OFFSET_MAX;
        if (size > 0) {
            memcpy(dst_var + dst_var_offset, src_var[bit] + src_var_offset, size);
            dst_var_offset += size;
        }
        dst_fix[l * 2] = relocated_var_offset;
        dst_fix[l * 2 + 1] = size;
    }
}

#endif //QUESTDB_OOO_DISPATCH_H

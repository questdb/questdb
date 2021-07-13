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

#include <cstring>
#include "util.h"
#include "simd.h"
#include "ooo_dispatch.h"
#include <algorithm>

// 24, 25
template<int size>
inline void set_var_refs(int64_t *addr, const int64_t offset, const int64_t count) {
    for (int64_t i = 0; i < count; i++) {
        addr[i] = offset + i * size;
    }
}

// 19-23
template<typename T>
inline void set_memory_vanilla(T *addr, const T value, const int64_t count) {
    for (int64_t i = 0; i < count; i++) {
        addr[i] = value;
    }
}

template<class T>
inline void re_shuffle(const T *src, T *dest, const index_t *index, const int64_t count) {
    for (int64_t i = 0; i < count; i++) {
        dest[i] = src[index[i].i];
    };
}

// 0, 3
template<typename T>
inline void merge_copy_var_column(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset,
        T mult
) {
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    char *src_var[] = {src_ooo_var, src_data_var};

    for (int64_t l = 0; l < merge_index_size; l++) {
        MM_PREFETCH_T0(merge_index + l + 64);
        dst_fix[l] = dst_var_offset;
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const uint64_t rr = row & ~(1ull << 63);
        const int64_t offset = src_fix[bit][rr];
        char *src_var_ptr = src_var[bit] + offset;
        auto len = *reinterpret_cast<T *>(src_var_ptr);
        auto char_count = len > 0 ? len * mult : 0;
        reinterpret_cast<T *>(dst_var + dst_var_offset)[0] = len;
        __MEMCPY(dst_var + dst_var_offset + sizeof(T), src_var_ptr + sizeof(T), char_count);
        dst_var_offset += char_count + sizeof(T);
    }
}

void platform_memcpy(void *dst, const void *src, const size_t len) {
    __MEMCPY(dst, src, len);
}

void platform_memset(void *dst, const int val, const size_t len) {
    __MEMSET(dst, val, len);
}

void platform_memmove(void *dst, const void *src, const size_t len) {
    __MEMMOVE(dst, src, len);
}

// 0
void merge_copy_var_column_int32(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset
) {
    merge_copy_var_column<int32_t>(merge_index, merge_index_size, src_data_fix, src_data_var, src_ooo_fix, src_ooo_var,
                                   dst_fix, dst_var, dst_var_offset, 2);
}

// 3
void merge_copy_var_column_int64(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset
) {
    merge_copy_var_column<int64_t>(merge_index, merge_index_size, src_data_fix, src_data_var, src_ooo_fix, src_ooo_var,
                                   dst_fix, dst_var, dst_var_offset, 1);
}

// 5
void re_shuffle_int32(const int32_t *src, int32_t *dest, const index_t *index, const int64_t count) {
    re_shuffle_vanilla(src, dest, index, count);
}

// 6
void re_shuffle_int64(const int64_t *src, int64_t *dest, const index_t *index, const int64_t count) {
    re_shuffle_vanilla(src, dest, index, count);
}

void re_shuffle_256bit(const long_256bit *src, long_256bit *dest, const index_t *index, const int64_t count) {
    re_shuffle_vanilla(src, dest, index, count);
}

// 12
void merge_shuffle_int64(const int64_t *src1, const int64_t *src2, int64_t *dest, const index_t *index,
                                const int64_t count) {
    merge_shuffle_vanilla<int64_t>(src1, src2, dest, index, count);
}

//17
void flatten_index(index_t *index, int64_t count) {
    for (int64_t i = 0; i < count; i++) {
        index[i].i = i;
    }
}

// 18
void make_timestamp_index(const int64_t *data, int64_t low, int64_t high, index_t *dest) {
    for (int64_t l = low; l <= high; l++) {
        dest[l - low].ts = data[l];
        dest[l - low].i = l | (1ull << 63);
    }
}

// 31
void shift_timestamp_index(const index_t *data, int64_t count, index_t *dest) {
    for (int64_t l = 0; l < count; l++) {
        dest[l].ts = data[l].ts;
        dest[l].i = l;
    }
}

// 19
void set_memory_vanilla_int64(int64_t *data, const int64_t value, const int64_t count) {
    set_memory_vanilla<int64_t>(data, value, count);
}

// 20
void set_memory_vanilla_int32(int32_t *data, const int32_t value, const int64_t count) {
    set_memory_vanilla<int32_t>(data, value, count);
}

// 21
void set_memory_vanilla_double(double *data, const double value, const int64_t count) {
    set_memory_vanilla<double>(data, value, count);
}

// 22
void set_memory_vanilla_float(float *data, const float value, const int64_t count) {
    set_memory_vanilla<float>(data, value, count);
}

// 23
void set_memory_vanilla_short(int16_t *data, const int16_t value, const int64_t count) {
    set_memory_vanilla<int16_t>(data, value, count);
}

// 24
void set_var_refs_64_bit(int64_t *data, int64_t offset, int64_t count) {
    set_var_refs<sizeof(int64_t)>(data, offset, count);
}

// 25
void set_var_refs_32_bit(int64_t *data, int64_t offset, int64_t count) {
    set_var_refs<sizeof(int32_t)>(data, offset, count);
}

// 26
void copy_index(const index_t *index, const int64_t count, int64_t *dest) {
    for (int64_t i = 0; i < count; i++) {
        dest[i] = index[i].ts;
    }
}

// 27
void shift_copy(int64_t shift, int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    const int64_t count = src_hi - src_lo + 1;
    for (int64_t i = 0; i < count; i++) {
        dest[i] = src[i + src_lo] - shift;
    }
}

// 28
void copy_index_timestamp(index_t *index, int64_t index_lo, int64_t index_hi, int64_t *dest) {
    const int64_t count = index_hi - index_lo + 1;
    for (int64_t i = 0; i < count; i++) {
        dest[i] = index[index_lo + i].ts;
    }
}
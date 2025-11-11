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

// 19-23, 33-34
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
    }
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
    if (merge_index_size > 0) {
        dst_fix[merge_index_size] = dst_var_offset;
    }
}

void merge_copy_varchar_column(
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
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    char *src_var[] = {src_ooo_var, src_data_var};
    for (int64_t l = 0; l < merge_index_size; l++) {
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const uint64_t rr = row & ~(1ull << 63);
        const int64_t firstWord = src_fix[bit][rr * 2];
        const int64_t secondWord = src_fix[bit][rr * 2 + 1];

        auto originalData = secondWord & 0x000000000000ffffLL;
        auto rellocatedSecondWord = originalData | (dst_var_offset << 16);
        if ((firstWord & 1) == 0 && (firstWord & 4) == 0) {
            // not inlined and not null
            auto originalOffset = secondWord >> 16;
            auto len = (firstWord >> 4) & 0xffffff;
            auto data = src_var[bit] + originalOffset;
            __MEMCPY(dst_var + dst_var_offset, data, len);
            dst_var_offset += len;
        }
        dst_fix[l * 2] = firstWord;
        dst_fix[l * 2 + 1] = rellocatedSecondWord;
    }
}

void merge_copy_array_column(
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
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    char *src_var[] = {src_ooo_var, src_data_var};
    for (int64_t l = 0; l < merge_index_size; l++) {
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const uint64_t rr = row & ~(1ull << 63);
        const int64_t offset = src_fix[bit][rr * 2] & OFFSET_MAX;
        uint32_t size = static_cast<int>(src_fix[bit][rr * 2 + 1] & ARRAY_SIZE_MAX);

        const auto relocated_var_offset = dst_var_offset & OFFSET_MAX;
        if (size > 0) {
            // not inlined and not null
            auto data = src_var[bit] + offset;
            __MEMCPY(dst_var + dst_var_offset, data, size);
            dst_var_offset += size;
        }
        dst_fix[l * 2] = relocated_var_offset;
        dst_fix[l * 2 + 1] = size;
    }
}

void platform_memcpy(void *dst, const void *src, const size_t len) {
    __MEMCPY(dst, src, len);
}

void platform_memcmp(const void *a, const void *b, const size_t len, int *res) {
    *res = __MEMCMP(a, b, len);
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

// 30
void re_shuffle_256bit(const long_256bit *src, long_256bit *dest, const index_t *index, const int64_t count) {
    re_shuffle_vanilla(src, dest, index, count);
}

// 32
void re_shuffle_128bit(const __int128 *src, __int128 *dest, const index_t *index, const int64_t count) {
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
void set_binary_column_null_refs(int64_t *data, int64_t offset, int64_t count) {
    set_var_refs<sizeof(int64_t)>(data, offset, count);
}

// 25
void set_string_column_null_refs(int64_t *data, int64_t offset, int64_t count) {
    set_var_refs<sizeof(int32_t)>(data, offset, count);
}

// 26
void copy_index(const index_t *index, const int64_t count, int64_t *dest) {
    for (int64_t i = 0; i < count; i++) {
        dest[i] = index[i].ts;
    }
}

// 27
void shift_copy(int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    const int64_t count = src_hi - src_lo + 1;
    for (int64_t i = 0; i < count; i++) {
        dest[i] = src[i + src_lo] - shift;
    }
}

// 28
void shift_copy_varchar_aux(int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    const int64_t count = 2 * (src_hi - src_lo + 1);
    for (int64_t i = 0; i < count; i += 2) {
        dest[i] = src[i + 2 * src_lo];
        // The offset is stored in the second 8 bytes of varchar's 16 bytes.
        // 16 LSBs (little-endian) are reserved for other varchar fields.
        dest[i + 1] = src[i + 2 * src_lo + 1] - (shift << 16);
    }
}

// 29
void copy_index_timestamp(index_t *index, int64_t index_lo, int64_t index_hi, int64_t *dest) {
    const int64_t count = index_hi - index_lo + 1;
    for (int64_t i = 0; i < count; i++) {
        dest[i] = index[index_lo + i].ts;
    }
}

// 30
void set_varchar_null_refs(int64_t *aux, int64_t offset, int64_t count) {
    auto o = offset << 16;
    for (int64_t i = 0; i < 2 * count; i += 2) {
        aux[i] = 4;      // null flag
        aux[i + 1] = o;  // offset for subsequent null varchars stays the same
    }
}

// 31
void set_array_null_refs(int64_t *aux, int64_t offset, int64_t count) {
    for (int64_t i = 0; i < 2 * count; i += 2) {
        aux[i] = offset;
        aux[i + 1] = 0; // null flag
    }
}

// 32
void shift_copy_array_aux(int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    const int64_t count = 2 * (src_hi - src_lo + 1);
    for (int64_t i = 0; i < count; i += 2) {
        dest[i] = src[i + 2 * src_lo] - shift;
        dest[i + 1] = src[i + 2 * src_lo + 1];
    }
}

// 33
void set_memory_vanilla_int128(long_128bit *data, const long_128bit value, const int64_t count) {
    set_memory_vanilla<long_128bit>(data, value, count);
}

// 34
void set_memory_vanilla_int256(long_256bit *data, const long_256bit value, const int64_t count) {
    set_memory_vanilla<long_256bit>(data, value, count);
}

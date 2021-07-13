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


#include "util.h"
#include "simd.h"

#include "vcl/vectorclass.h"
#include <x86intrin.h>
#include <xmmintrin.h>

#include "ooo_dispatch.h"
#include <algorithm>

template<typename T, int alignment, typename l_iteration>
inline int64_t align_to_store_nt(T *address, const int64_t max_count, const l_iteration iteration) {

    const auto unaligned = ((uint64_t) address) % alignment;
    constexpr int64_t iteration_increment = sizeof(T);
    if (unaligned != 0) {

        if (unaligned % iteration_increment == 0) {

            const auto head_iteration_count = std::min<int64_t>(max_count,
                                                                (alignment - unaligned) / iteration_increment);
            for (int i = 0; i < head_iteration_count; i++) {
                iteration(i);
            }

            return head_iteration_count;
        } else {
            return -1;
        }
    }
    return 0;
}

template<typename T, typename TVec, typename lambda_iteration, typename lambda_vec_iteration>
inline void run_vec_bulk(T *dest,
                         const int64_t count,
                         const lambda_iteration l_iteration,
                         const lambda_vec_iteration l_vec_iteration) {

    constexpr int64_t alignment = TVec::store_nt_alignment();
    const auto unaligned = ((uint64_t) dest) % alignment;
    constexpr int64_t iteration_increment = sizeof(T);
    if (unaligned % iteration_increment == 0) {

        const auto head_iteration_count = unaligned > 0 ? (alignment - unaligned) / iteration_increment : 0;
        constexpr int64_t increment = TVec::size();
        const int64_t bulk_stop = count - increment + 1;

        int64_t i = 0;
        while (i < count) {
            if (i >= head_iteration_count && i < bulk_stop) {
                l_vec_iteration(i);
                i += increment;
            } else {
                l_iteration(i);
                i++;
            }
        }

    } else {
        for (int64_t i = 0; i < count; i++) {
            l_iteration(i);
        }
    }
}

// 24, 25
template<class T>
inline void set_var_refs(int64_t *addr, const int64_t offset, const int64_t count) {

    const auto size = sizeof(T);
    const auto vec_inc = 8 * size;
    auto l_set_address = [addr, offset](int64_t i) { addr[i] = offset + i * size; };
    int64_t i = align_to_store_nt<int64_t, Vec8q::store_nt_alignment()>(addr, count, l_set_address);

    if (i >= 0) {
        Vec8q add(vec_inc);
        Vec8q v_addr(offset + (i + 0) * size,
                     offset + (i + 1) * size,
                     offset + (i + 2) * size,
                     offset + (i + 3) * size,
                     offset + (i + 4) * size,
                     offset + (i + 5) * size,
                     offset + (i + 6) * size,
                     offset + (i + 7) * size);

        for (; i < count - 7; i += 8) {
            v_addr.store_a(addr + i);
            v_addr += add;
        }
    } else {
        // Pointer cannot be aligned
        i = 0;
    }

    // tail
    for (; i < count; i++) {
        addr[i] = offset + i * size;
    }
}

// 19-23
template<typename T, typename TVec>
inline void set_memory_vanilla(T *addr, const T value, const int64_t count) {

    const auto l_iteration = [addr, value](int64_t i) {
        addr[i] = value;
    };

    const TVec vec(value);
    const auto l_bulk = [&vec, addr](const int64_t i) {
        vec.store_nt(addr + i);
    };

    run_vec_bulk<T, TVec>(addr, count, l_iteration, l_bulk);
}

template<typename TVec, typename T>
inline TVec lookup_idx8(Vec8uq idx, const T *src) {
#if INSTRSET >= 8
    if constexpr (sizeof(T) == 4) {
        const auto max_index = horizontal_max(idx);
        if (max_index < INT32_MAX) {
            const Vec8i int32_idx1 = compress(idx);
            return _mm256_i32gather_epi32(src, int32_idx1, 4);
        }
    }
#endif

#if INSTRSET >= 10
    if constexpr (sizeof(T) == 8) {
        // VCL function is lookup
        // but it has additional params / checks which only slow things up
        return _mm512_i64gather_epi64(idx, src, 8);
    }
#endif

    return TVec(
            src[idx[0]],
            src[idx[1]],
            src[idx[2]],
            src[idx[3]],
            src[idx[4]],
            src[idx[5]],
            src[idx[6]],
            src[idx[7]]);
}

template<typename TVec, typename T>
inline TVec lookup_index(const int64_t i, const index_t *index, const T *src) {
    Vec8uq row_ind1 = gather8q<1, 3, 5, 7, 9, 11, 13, 15>(index + i + 0);
    static_assert(TVec::size() == 8);
    return lookup_idx8<TVec, T>(row_ind1, src);
}

template<class T, typename TVec>
inline void re_shuffle(const T *src, T *dest, const index_t *index, const int64_t count) {

    static_assert(TVec::size() == 8 || TVec::size() == 16);
    const auto l_iteration = [dest, src, index](int64_t i) {
        dest[i] = src[index[i].i];
    };

    const auto bulk_reshuffle = [src, dest, index](const int64_t i) {
        MM_PREFETCH_T0(index + i + 64);
        auto values = lookup_index<TVec, T>(i, index, src);
        values.store_a(dest + i);
    };

    run_vec_bulk<T, TVec>(
            dest,
            count,
            l_iteration,
            bulk_reshuffle
    );
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

void MULTI_VERSION_NAME (platform_memcpy)(void *dst, const void *src, const size_t len) {
    __MEMCPY(dst, src, len);
}

void MULTI_VERSION_NAME (platform_memset)(void *dst, const int val, const size_t len) {
    __MEMSET(dst, val, len);
}

void MULTI_VERSION_NAME (platform_memmove)(void *dst, const void *src, const size_t len) {
    __MEMMOVE(dst, src, len);
}

// 0
void MULTI_VERSION_NAME (merge_copy_var_column_int32)(
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
void MULTI_VERSION_NAME (merge_copy_var_column_int64)(
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
void
MULTI_VERSION_NAME (re_shuffle_int32)(const int32_t *src, int32_t *dest, const index_t *index, const int64_t count) {
#if INSTRSET >= 8
    re_shuffle<int32_t, Vec8i>(src, dest, index, count);
#else
    re_shuffle_vanilla(src, dest, index, count);
#endif
}

// 6
void
MULTI_VERSION_NAME (re_shuffle_int64)(const int64_t *src, int64_t *dest, const index_t *index, const int64_t count) {
#if INSTRSET >= 10
    re_shuffle<int64_t, Vec8q>(src, dest, index, count);
#else
    re_shuffle_vanilla(src, dest, index, count);
#endif
}

void
MULTI_VERSION_NAME (re_shuffle_256bit)(const long_256bit *src, long_256bit *dest, const index_t *index, const int64_t count) {
    // Let compile optimise copying 4 longs per every CPU.
    re_shuffle_vanilla(src, dest, index, count);
}

// 12
void
MULTI_VERSION_NAME (merge_shuffle_int64)(const int64_t *src1, const int64_t *src2, int64_t *dest, const index_t *index,
                                         const int64_t count) {
#if INSTRSET == 10
    const int64_t *sources[] = {src2, src1};

    const auto merge = [dest, index, &sources](int64_t i) {
        const auto r = reinterpret_cast<uint64_t>(index[i].i);
        const uint64_t pick = r >> 63u;
        const auto row = r & ~(1LLu << 63u);
        dest[i] = sources[pick][row];
    };

    const auto bulk_merge = [dest, index, src1, src2](const int64_t i) {
        MM_PREFETCH_T0(index + i + 64);
        const Vec8uq index_i1 = gather8q<1, 3, 5, 7, 9, 11, 13, 15>(index + i);
        const Vec8uq index1 = index_i1 & ~(1LLu << 63u);
        auto mask = (index_i1 >> 63) > 0;
        // when mask is 1 take src1
        Vec8q v1 = _mm512_mask_i64gather_epi64(index1, mask, index1, src1, 8);
        // and when 0 take src2
        Vec8q(_mm512_mask_i64gather_epi64(v1, ~mask, v1, src2, 8))
                .store_a(dest + i);
    };

    run_vec_bulk<int64_t, Vec8q>(
            dest,
            count,
            merge,
            bulk_merge
    );
#else
    merge_shuffle_vanilla<int64_t>(src1, src2, dest, index, count);
#endif
}

//17
void MULTI_VERSION_NAME (flatten_index)(index_t *index, int64_t count) {
    Vec8q v_i = Vec8q(0, 1, 2, 3, 4, 5, 6, 7);
    Vec8q v_inc = Vec8q(8);
    int64_t i = 0;
    for (; i < count - 7; i += 8) {
        scatter<1, 3, 5, 7, 9, 11, 13, 15>(v_i, index + i);
        v_i += v_inc;
    }

    // tail.
    for (; i < count; i++) {
        index[i].i = i;
    }
}

// 18
void MULTI_VERSION_NAME (make_timestamp_index)(const int64_t *data, int64_t low, int64_t high, index_t *dest) {

    // This code assumes that index_t is 16 bytes, 8 bytes ts and 8 bytes i
    static_assert(sizeof(index_t) == 16);

    int64_t l = low;
    Vec8q vec_i((low + 0) | (1ull << 63),
                (low + 1) | (1ull << 63),
                (low + 2) | (1ull << 63),
                (low + 3) | (1ull << 63),
                (low + 4) | (1ull << 63),
                (low + 5) | (1ull << 63),
                (low + 6) | (1ull << 63),
                (low + 7) | (1ull << 63));
    const Vec8q vec8(8);
    Vec8q vec_ts;

    for (; l <= high - 7; l += 8) {
        MM_PREFETCH_T0(data + l + 64);
        vec_ts.load(data + l);

        // save vec_ts into even 8b positions as index ts
        scatter<0, 2, 4, 6, 8, 10, 12, 14>(vec_ts, dest + l - low);

        // save vec_i into odd 8b positions as index i
        scatter<1, 3, 5, 7, 9, 11, 13, 15>(vec_i, dest + l - low);
        vec_i += vec8;
    }

    // tailok
    for (; l <= high; l++) {
        dest[l - low].ts = data[l];
        dest[l - low].i = l | (1ull << 63);
    }
}

//31
void MULTI_VERSION_NAME(shift_timestamp_index) (const index_t *src, int64_t count, index_t *dest) {
    // Same as vanilla, not expected to be big arrays
    for (int64_t l = 0; l < count; l++) {
        dest[l].ts = src[l].ts;
        dest[l].i = l;
    }
}

// 19
void MULTI_VERSION_NAME (set_memory_vanilla_int64)(int64_t *data, const int64_t value, const int64_t count) {
    set_memory_vanilla<int64_t, Vec8q>(data, value, count);
}

// 20
void MULTI_VERSION_NAME (set_memory_vanilla_int32)(int32_t *data, const int32_t value, const int64_t count) {
    set_memory_vanilla<int32_t, Vec16i>(data, value, count);
}

// 21
void MULTI_VERSION_NAME (set_memory_vanilla_double)(double *data, const double value, const int64_t count) {
    set_memory_vanilla<double, Vec8d>(data, value, count);
}

// 22
void MULTI_VERSION_NAME (set_memory_vanilla_float)(float *data, const float value, const int64_t count) {
    set_memory_vanilla<float, Vec16f>(data, value, count);
}

// 23
void MULTI_VERSION_NAME (set_memory_vanilla_short)(int16_t *data, const int16_t value, const int64_t count) {
    set_memory_vanilla<int16_t, Vec32s>(data, value, count);
}

// 24
void MULTI_VERSION_NAME (set_var_refs_64_bit)(int64_t *data, int64_t offset, int64_t count) {
    set_var_refs<int64_t>(data, offset, count);
}

// 25
void MULTI_VERSION_NAME (set_var_refs_32_bit)(int64_t *data, int64_t offset, int64_t count) {
    set_var_refs<int32_t>(data, offset, count);
}

// 26
void MULTI_VERSION_NAME (copy_index)(const index_t *index, const int64_t count, int64_t *dest) {
    auto l_iteration = [dest, index](int64_t i) {
        dest[i] = index[i].ts;
    };
    auto l_bulk = [dest, index](int64_t i) {
        MM_PREFETCH_T0(index + i + 64);
        gather8q<0, 2, 4, 6, 8, 10, 12, 14>(index + i).store_a(dest + i);
    };
    run_vec_bulk<int64_t, Vec8q>(dest, count, l_iteration, l_bulk);
}

// 27
void MULTI_VERSION_NAME (shift_copy)(int64_t shift, int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    const int64_t count = src_hi - src_lo + 1;

    Vec2q vec;
    Vec2q vec_shift = Vec2q(shift);

    auto l_iteration = [dest, src, src_lo, shift](int64_t i) {
        dest[i] = src[i + src_lo] - shift;
    };
    auto src_loo = src + src_lo;
    auto l_bulk = [&vec, &vec_shift, dest, src_loo](int64_t i) {
        MM_PREFETCH_T0(src_loo + i + 64);
        vec.load(src_loo + i);
        vec -= vec_shift;
        vec.store_a(dest + i);
    };
    run_vec_bulk<int64_t, Vec2q>(dest, count, l_iteration, l_bulk);
}

// 28
void MULTI_VERSION_NAME (copy_index_timestamp)(index_t *index, int64_t index_lo, int64_t index_hi, int64_t *dest) {
    const int64_t count = index_hi - index_lo + 1;
    auto l_iteration = [dest, index, index_lo](int64_t i) {
        dest[i] = index[index_lo + i].ts;
    };
    auto l_bulk = [dest, index, index_lo](int64_t i) {
        MM_PREFETCH_T0(index + index_lo + i + 64);
        gather8q<0, 2, 4, 6, 8, 10, 12, 14>(index + index_lo + i).store_a(dest + i);
    };
    run_vec_bulk<int64_t, Vec8q>(dest, count, l_iteration, l_bulk);
}
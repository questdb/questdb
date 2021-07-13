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

#ifndef QUESTDB_OOO_DISPATCH_H
#define QUESTDB_OOO_DISPATCH_H

#include "dispatcher.h"

typedef struct index_t {
    uint64_t ts;
    uint64_t i;

    bool operator<(int64_t other) const {
        return ts < other;
    }

    bool operator>(int64_t other) const {
        return ts > other;
    }

    bool operator==(index_t other) const {
        return ts == other.ts;
    }

    bool operator==(int64_t other) const {
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

} index_t;

typedef struct __attribute__ ((packed)) long_256bit {
    uint64_t long0;
    uint64_t long1;
    uint64_t long2;
    uint64_t long3;
} long_256bit;

DECLARE_DISPATCHER_TYPE(copy_index_timestamp, index_t *index, int64_t index_lo, int64_t index_hi, int64_t *dest);

DECLARE_DISPATCHER_TYPE(shift_copy, int64_t shift, int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest);

DECLARE_DISPATCHER_TYPE(copy_index, const index_t *index, const int64_t count, int64_t *dest);

DECLARE_DISPATCHER_TYPE(set_var_refs_64_bit, int64_t *data, int64_t offset, int64_t count);

DECLARE_DISPATCHER_TYPE(set_var_refs_32_bit, int64_t *data, int64_t offset, int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_int64, int64_t *data, const int64_t value, const int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_int32, int32_t *data, const int32_t value, const int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_double, double *data, const double value, const int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_float, float *data, const float value, const int64_t count);

DECLARE_DISPATCHER_TYPE(set_memory_vanilla_short, int16_t *data, const int16_t value, const int64_t count);

DECLARE_DISPATCHER_TYPE(make_timestamp_index, const int64_t *data, int64_t low, int64_t high, index_t *dest);

DECLARE_DISPATCHER_TYPE(shift_timestamp_index, const index_t *data, int64_t count, index_t *dest);

DECLARE_DISPATCHER_TYPE(flatten_index, index_t *index, int64_t count);

DECLARE_DISPATCHER_TYPE(merge_shuffle_int64, const int64_t *src1, const int64_t *src2, int64_t *dest,
                        const index_t *index, const int64_t count);

DECLARE_DISPATCHER_TYPE(re_shuffle_256bit, const long_256bit *src, long_256bit *dest, const index_t *index, const int64_t count);

DECLARE_DISPATCHER_TYPE(re_shuffle_int64, const int64_t *src, int64_t *dest, const index_t *index, const int64_t count);

DECLARE_DISPATCHER_TYPE(re_shuffle_int32, const int32_t *src, int32_t *dest, const index_t *index, const int64_t count);

DECLARE_DISPATCHER_TYPE (merge_copy_var_column_int32, index_t *merge_index, int64_t merge_index_size,
                         int64_t *src_data_fix, char *src_data_var, int64_t *src_ooo_fix, char *src_ooo_var,
                         int64_t *dst_fix, char *dst_var, int64_t dst_var_offset);

DECLARE_DISPATCHER_TYPE (merge_copy_var_column_int64, index_t *merge_index, int64_t merge_index_size,
                         int64_t *src_data_fix, char *src_data_var, int64_t *src_ooo_fix, char *src_ooo_var,
                         int64_t *dst_fix, char *dst_var, int64_t dst_var_offset);

DECLARE_DISPATCHER_TYPE(platform_memcpy, void *dst, const void *src, const size_t len);

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

#endif //QUESTDB_OOO_DISPATCH_H

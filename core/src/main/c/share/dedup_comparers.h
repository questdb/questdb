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
#ifndef QUESTDB_DEDUP_COMPARERS_H
#define QUESTDB_DEDUP_COMPARERS_H

#include <stddef.h>
#include "dedup.h"
#include "column_type.h"
#include <algorithm>

#define assertm(exp, msg) assert(((void)msg, exp))

template<typename T>
class MergeColumnComparer : dedup_column {
public:
    inline int operator()(int64_t col_index, int64_t index_index) const {
        const T l_val = col_index >= column_top
                        ? reinterpret_cast<T *>(column_data)[col_index]
                        : *reinterpret_cast<const T *>(&null_value);

        const T r_val = reinterpret_cast<T *>(o3_data)[index_index];

        // One of the values can be MIN of the type (null value)
        // and subtraction can result in type overflow
        return (l_val > r_val) - (l_val < r_val);
    }
};

template<typename T>
class SortColumnComparer : dedup_column {
public:
    inline int operator()(int64_t l, int64_t r) const {
        const T l_val = l > -1
                           ? reinterpret_cast<T *>(column_data)[l]
                           : reinterpret_cast<T *>(o3_data)[l & ~(1ull << 63)];

        const T r_val = r > -1
                           ? reinterpret_cast<T *>(column_data)[r]
                           : reinterpret_cast<T *>(o3_data)[r & ~(1ull << 63)];

        // One of the values can be MIN of the type (null value)
        // and subtraction can result in type overflow
        return (l_val > r_val) - (l_val < r_val);
    }
};


const int32_t HEADER_FLAGS_WIDTH = 4;
const int32_t MIN_INLINE_CHARS = 6;
const uint32_t HEADER_FLAG_NULL = 1 << 2;
const int32_t VARCHAR_MAX_BYTES_FULLY_INLINED = 9;
const uint32_t VARCHAR_HEADER_FLAG_INLINED = 1 << 0;


inline int32_t read_varchar_size(const uint8_t *l_val_aux) {
    const auto aux_data = reinterpret_cast<const VarcharAuxEntrySplit *>(l_val_aux);
    assertm(aux_data->header != 0, "ERROR: invalid varchar aux data");
    if (aux_data->header & HEADER_FLAG_NULL) {
        return -1;
    }

    if (aux_data->header & VARCHAR_HEADER_FLAG_INLINED) {
        const auto aux_inlined_data = reinterpret_cast<const VarcharAuxEntryInlined *>(l_val_aux);
        const uint8_t size = aux_inlined_data->header >> HEADER_FLAGS_WIDTH;
        assertm(size <= VARCHAR_MAX_BYTES_FULLY_INLINED, "ERROR: invalid len of inline varchar");
        return size;
    }

    const auto size = (int32_t) (aux_data->header >> HEADER_FLAGS_WIDTH);
    assertm(size > VARCHAR_MAX_BYTES_FULLY_INLINED, "ERROR: invalid varchar non-inlined size");
    return size;
}


inline const uint8_t *
get_tail_chars_ptr(const uint8_t *l_val_aux, int32_t size, const uint8_t *l_val_data, int64_t l_val_data_len) {
    const auto aux_data = reinterpret_cast<const VarcharAuxEntrySplit *>(l_val_aux);
    const uint64_t data_offset = aux_data->offset_lo | ((uint64_t) aux_data->offset_hi) << 16;
    assertm(data_offset < (uint64_t) l_val_data_len, "ERROR: reading beyond varchar address length!");

    const uint8_t *data = l_val_data + data_offset;
    assertm(memcmp(aux_data->chars, data, MIN_INLINE_CHARS) == 0,
            "ERROR: varchar inline prefix does not match the data");

    return data;
}


// This struct represents repated pointers in dedup_column
// Sometimes it's better to view dedup_column struct as if it has an array of 2 data_point(s)
#pragma pack (push, 1)
struct data_point {
    uint8_t *aux_data;
    uint8_t *var_data;
    int64_t var_data_len;
};
#pragma pack(pop)


inline int compare_varchar(const data_point *l_col, int64_t l_offset, const data_point *r_col, int64_t r_offset) {

    auto l_val_aux = l_col->aux_data + l_offset * sizeof(VarcharAuxEntrySplit);
    auto r_val_aux = r_col->aux_data + r_offset * sizeof(VarcharAuxEntrySplit);

    auto l_size = read_varchar_size(l_val_aux);
    auto r_size = read_varchar_size(r_val_aux);

    if (l_size != r_size) {
        return l_size - r_size;
    }

    switch (l_size) {
        case -1:
        case 0:
            return 0;

        case 1:
        case 2:
        case 3:
        case 4:
        case 5:
        case 6:
        case 7:
        case 8:
        case 9: {
            // Both are inlined
            auto l_inl = l_val_aux + offsetof(VarcharAuxEntryInlined, chars);
            auto r_inl = r_val_aux + offsetof(VarcharAuxEntryInlined, chars);
            return memcmp(l_inl, r_inl, l_size);
        }

        default: {
            // Both are not inlined
            auto l_tail_chars_ptr = get_tail_chars_ptr(l_val_aux, l_size, l_col->var_data, l_col->var_data_len);
            auto r_tail_chars_ptr = get_tail_chars_ptr(r_val_aux, r_size, r_col->var_data, r_col->var_data_len);
            return memcmp(l_tail_chars_ptr, r_tail_chars_ptr, l_size);
        }
    }
}


class SortVarcharColumnComparer : dedup_column {

public:
    inline int operator()(int64_t l, int64_t r) const {
        auto cols = reinterpret_cast<const data_point *>(&this->column_data);
        auto l_col = &cols[l < 0];
        auto r_col = &cols[r < 0];
        return compare_varchar(l_col, l & ~(1ull << 63), r_col, r & ~(1ull << 63));
    }
};


class MergeVarcharColumnComparer : dedup_column {
public:
    inline int operator()(int64_t l_offset, int64_t r_offset) const {
        const auto cols = reinterpret_cast<const data_point *>(&this->column_data);

        if (l_offset >= column_top) {
            return compare_varchar(&cols[0], l_offset, &cols[1], r_offset);
        } else {
            // left value is null, read right header null flag
            auto r_header_null =
                    reinterpret_cast<const VarcharAuxEntrySplit *>(this->o3_data)[r_offset].header & HEADER_FLAG_NULL;
            // if right is null, then 0 (equals), otherwise -1 (left null is first)
            return r_header_null ? 0 : -1;
        }
    }
};


template<typename T, int item_size>
inline int compare_str_bin(const uint8_t *l_val, const uint8_t *r_val) {
    T l_size = *reinterpret_cast<const T *>(l_val);
    T r_size = *reinterpret_cast<const T *>(r_val);
    if (l_size != r_size) {
        return l_size - r_size;
    }

    switch (l_size) {
        case -1:
        case 0:
            return 0;

        default: {
            return memcmp(l_val + sizeof(T), r_val + sizeof(T), l_size * item_size);
        }
    }
};

template<typename T, int item_size>
class SortStrBinColumnComparer : dedup_column {

public:
    inline int operator()(int64_t l, int64_t r) const {
        const auto cols = reinterpret_cast<const data_point *>(&this->column_data);
        const auto l_col = &cols[l < 0];
        const auto r_col = &cols[r < 0];

        const auto l_val_offset = reinterpret_cast<int64_t *>(l_col->aux_data)[l & ~(1ull << 63)];
        assertm(l_val_offset < l_col->var_data_len, "ERROR: column aux data point beyond var data buffer");

        const auto r_val_offset = reinterpret_cast<int64_t *>(r_col->aux_data)[r & ~(1ull << 63)];
        assertm(r_val_offset < r_col->var_data_len, "ERROR: column aux data point beyond var data buffer");

        return compare_str_bin<T, item_size>(
                l_col->var_data + l_val_offset,
                r_col->var_data + r_val_offset
        );
    }
};

template<typename T, int item_size>
class MergeStrBinColumnComparer : dedup_column {
public:
    inline int operator()(int64_t col_index, int64_t index_index) const {
        const T l_val_offset = col_index >= column_top
                               ? reinterpret_cast<int64_t *>(column_data)[col_index]
                               : -1;

        assertm(l_val_offset < column_var_data_len, "ERROR: column aux data point beyond var data buffer");
        const uint8_t *l_val_ptr = col_index >= column_top ?
                                   reinterpret_cast<const uint8_t *>(column_var_data) + l_val_offset
                                                           : null_value;

        const auto r_val_offset = reinterpret_cast<int64_t *>(o3_data)[index_index];
        assertm(r_val_offset < o3_var_data_len, "ERROR: column aux data point beyond var data buffer");
        const uint8_t *r_val_ptr = reinterpret_cast<const uint8_t *>(o3_var_data) + r_val_offset;

        return compare_str_bin<T, item_size>(l_val_ptr, r_val_ptr);
    }
};


#endif //QUESTDB_DEDUP_COMPARERS_H

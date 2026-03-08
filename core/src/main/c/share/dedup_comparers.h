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
#ifndef QUESTDB_DEDUP_COMPARERS_H
#define QUESTDB_DEDUP_COMPARERS_H

#include <cstddef>
#include "dedup.h"
#include "column_type.h"
#include <algorithm>
#include "ooo.h"

#define assertm(exp, msg) assert(((void)msg, exp))

template<typename T>
class MergeColumnComparer : dedup_column {
public:
    inline int operator()(int64_t col_index, int64_t index_index) const {
        const T l_val = col_index >= this->get_column_top()
                        ? this->get_column_data<T>()[col_index]
                        : this->get_null_value<T>();

        const T r_val = this->get_o3_data<T>()[index_index];

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
                        ? this->get_column_data<T>()[l]
                        : this->get_o3_data<T>()[l & ~(1ull << 63)];

        const T r_val = r > -1
                        ? this->get_column_data<T>()[r]
                        : this->get_o3_data<T>()[r & ~(1ull << 63)];

        // One of the values can be MIN of the type (null value)
        // and subtraction can result in type overflow
        return (l_val > r_val) - (l_val < r_val);
    }
};

template<typename T, typename TIdx>
inline int
compare_dedup_column_fixed(const dedup_column *dedup_col, index_tr_i<TIdx> l, index_tr_i<TIdx> r, uint16_t segment_bits,
                           uint64_t segment_mask) {
    auto l_row_index = l.i >> segment_bits;
    auto l_src_index = l.i & segment_mask;

    auto r_row_index = r.i >> segment_bits;
    auto r_src_index = r.i & segment_mask;

    const T l_val = dedup_col->get_column_data_2d<T>()[l_src_index][l_row_index];
    const T r_val = dedup_col->get_column_data_2d<T>()[r_src_index][r_row_index];

    // One of the values can be MIN of the type (null value)
    // and subtraction can result in type overflow
    return (l_val > r_val) - (l_val < r_val);
}

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

inline int compare_varchar(
        const uint8_t *l_aux, const uint8_t *l_var_data, int64_t l_var_data_len, int64_t l_offset,
        const uint8_t *r_aux, const uint8_t *r_var_data, int64_t r_var_data_len, int64_t r_offset
) {
    auto l_val_aux = l_aux + l_offset * sizeof(VarcharAuxEntrySplit);
    auto r_val_aux = r_aux + r_offset * sizeof(VarcharAuxEntrySplit);

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
            auto l_tail_chars_ptr = get_tail_chars_ptr(l_val_aux, l_size, l_var_data, l_var_data_len);
            auto r_tail_chars_ptr = get_tail_chars_ptr(r_val_aux, r_size, r_var_data, r_var_data_len);
            return memcmp(l_tail_chars_ptr, r_tail_chars_ptr, l_size);
        }
    }
}

inline int
compare_varchar(const column_pointer *l_col, int64_t l_offset, const column_pointer *r_col, int64_t r_offset) {

    return compare_varchar(
            l_col->aux_data, l_col->var_data, l_col->var_data_len, l_offset,
            r_col->aux_data, r_col->var_data, r_col->var_data_len, r_offset
    );
}


class SortVarcharColumnComparer : dedup_column {

public:
    inline int operator()(int64_t l, int64_t r) const {
        auto cols = this->get_column_data_pointers();
        auto l_col = &cols[l < 0];
        auto r_col = &cols[r < 0];
        return compare_varchar(l_col, l & ~(1ull << 63), r_col, r & ~(1ull << 63));
    }
};


class MergeVarcharColumnComparer : dedup_column {
public:
    inline int operator()(int64_t l_offset, int64_t r_offset) const {
        const auto cols = this->get_column_data_pointers();

        if (l_offset >= this->get_column_top()) {
            return compare_varchar(&cols[0], l_offset, &cols[1], r_offset);
        } else {
            // left value is null, read right header null flag
            auto r_header_null =
                    this->get_o3_data<VarcharAuxEntrySplit>()[r_offset].header & HEADER_FLAG_NULL;
            // if right is null, then 0 (equals), otherwise -1 (left null is first)
            return r_header_null ? 0 : -1;
        }
    }
};


template<typename TIdx>
inline int compare_dedup_varchar_column(const dedup_column *dedup_column, index_tr_i<TIdx> l, index_tr_i<TIdx> r,
                                        uint16_t segment_bits, uint64_t segment_mask) {
    auto column_aux = dedup_column->get_column_data_2d<uint8_t>();
    auto column_var_data = dedup_column->get_column_var_data_2d<uint8_t>();

    auto l_row_index = l.i >> segment_bits;
    auto l_src_index = l.i & segment_mask;

    auto r_row_index = r.i >> segment_bits;
    auto r_src_index = r.i & segment_mask;

    return compare_varchar(
            column_aux[l_src_index], column_var_data[l_src_index], std::numeric_limits<int64_t>::max(), l_row_index,
            column_aux[r_src_index], column_var_data[r_src_index], std::numeric_limits<int64_t>::max(), r_row_index
    );
}


template<typename T, int ItemSize>
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
            return memcmp(l_val + sizeof(T), r_val + sizeof(T), l_size * ItemSize);
        }
    }
};

template<typename T, int ItemSize>
class SortStrBinColumnComparer : dedup_column {

public:
    inline int operator()(int64_t l, int64_t r) const {
        const auto cols = this->get_column_data_pointers();
        const auto l_col = &cols[l < 0];
        const auto r_col = &cols[r < 0];

        const auto l_val_offset = reinterpret_cast<int64_t *>(l_col->aux_data)[l & ~(1ull << 63)];
        assertm(l_val_offset < l_col->var_data_len, "ERROR: column aux data point beyond var data buffer");

        const auto r_val_offset = reinterpret_cast<int64_t *>(r_col->aux_data)[r & ~(1ull << 63)];
        assertm(r_val_offset < r_col->var_data_len, "ERROR: column aux data point beyond var data buffer");

        return compare_str_bin<T, ItemSize>(
                l_col->var_data + l_val_offset,
                r_col->var_data + r_val_offset
        );
    }
};

template<typename T, int ItemSize, typename TIdx>
inline int compare_str_bin_dedup_column(
        const dedup_column *dedup_col,
        index_tr_i<TIdx> l,
        index_tr_i<TIdx> r,
        int segment_bits,
        uint64_t segment_mask
) {
    auto column_aux = dedup_col->get_column_data_2d<int64_t>();
    auto column_var_data = dedup_col->get_column_var_data_2d<uint8_t>();


    auto l_row_index = l.i >> segment_bits;
    auto l_src_index = l.i & segment_mask;

    auto r_row_index = r.i >> segment_bits;
    auto r_src_index = r.i & segment_mask;

    const auto l_val_offset = column_aux[l_src_index][l_row_index];
    assertm(l_val_offset >= 0, "ERROR: column aux data point beyond var data buffer");

    const auto r_val_offset = column_aux[r_src_index][r_row_index];
    assertm(r_val_offset >= 0, "ERROR: column aux data point beyond var data buffer");

    return compare_str_bin<T, ItemSize>(
            column_var_data[l_src_index] + l_val_offset,
            column_var_data[r_src_index] + r_val_offset
    );
}

template<typename TIdx>
inline int compare_dedup_symbol_column(const dedup_column *this1, index_tr_i<TIdx> l, index_tr_i<TIdx> r) {
    auto column_data = this1->get_column_data<int32_t>();

    auto l_val = column_data[l.ri];
    auto r_val = column_data[r.ri];

    return (l_val > r_val) - (l_val < r_val);
}

template<typename T, int ItemSize>
class MergeStrBinColumnComparer : dedup_column {
public:
    inline int operator()(int64_t col_index, int64_t index_index) const {
        const T l_val_offset = col_index >= this->get_column_top()
                               ? this->get_column_data<int64_t>()[col_index]
                               : -1;

        assertm(l_val_offset < this->get_column_var_data_len(), "ERROR: column aux data point beyond var data buffer");
        const uint8_t *l_val_ptr = col_index >= this->get_column_top() ?
                                   this->get_column_var_data<uint8_t>() + l_val_offset
                                                                       : &this->get_null_value<uint8_t>();

        const auto r_val_offset = this->get_o3_data<int64_t>()[index_index];
        assertm(r_val_offset < this->get_o3_var_data_len(), "ERROR: column aux data point beyond var data buffer");
        const uint8_t *r_val_ptr = this->get_o3_var_data<uint8_t>() + r_val_offset;

        return compare_str_bin<T, ItemSize>(l_val_ptr, r_val_ptr);
    }
};


#endif //QUESTDB_DEDUP_COMPARERS_H

//
// Created by Alex Pelagenko on 21/08/2024.
//
#ifndef QUESTDB_DEDUP_COMPARERS_H
#define QUESTDB_DEDUP_COMPARERS_H

#include "dedup.h"
#include "column_type.h"
#include <algorithm>

#define assertm(exp, msg) assert(((void)msg, exp))

template<typename T>
class MergeColumnComparer : dedup_column {
public:
    inline int operator()(int64_t col_index, int64_t index_index) const {
        const auto l_val = col_index >= dedup_column::column_top
                           ? reinterpret_cast<T *>(dedup_column::column_data)[col_index]
                           : *reinterpret_cast<const T *>(&null_value);

        const auto r_val = reinterpret_cast<T *>(dedup_column::o3_data)[index_index];

        // One of the values can be MIN of the type (null value)
        // and subtraction can result in type overflow
        return l_val > r_val ? 1 : (l_val < r_val ? -1 : 0);
    }
};

template<typename T>
class SortColumnComparer : dedup_column {
public:
    inline int operator()(int64_t l, int64_t r) const {
        const auto l_val = l > -1
                           ? reinterpret_cast<T *>(dedup_column::column_data)[l]
                           : reinterpret_cast<T *>(dedup_column::o3_data)[l & ~(1ull << 63)];

        const auto r_val = r > -1
                           ? reinterpret_cast<T *>(dedup_column::column_data)[r]
                           : reinterpret_cast<T *>(dedup_column::o3_data)[r & ~(1ull << 63)];

        // One of the values can be MIN of the type (null value)
        // and subtraction can result in type overflow
        return l_val > r_val ? 1 : (l_val < r_val ? -1 : 0);
    }
};

struct VarcharDataView {
    const uint8_t *const inline_chars;
    const uint8_t *const remaining_chars;
    const int32_t size;
};

const VarcharDataView NULL_VARCHAR_VIEW = {NULL, NULL, -1};

const int32_t HEADER_FLAGS_WIDTH = 4;
const int32_t MIN_INLINE_CHARS = 6;
const uint32_t HEADER_FLAG_NULL = 1 << 2;
const int32_t VARCHAR_MAX_BYTES_FULLY_INLINED = 9;
const uint32_t VARCHAR_HEADER_FLAG_INLINED = 1 << 0;

inline VarcharDataView
read_varchar(int64_t offset, const void *column_data, const void *column_var_data, const long column_var_data_len) {
    const auto aux_data = &reinterpret_cast<const VarcharAuxEntrySplit *>(column_data)[offset];
    assertm(aux_data->header != 0, "ERROR: invalid varchar aux data");
    if (aux_data->header & HEADER_FLAG_NULL) {
        return NULL_VARCHAR_VIEW;
    }

    if (aux_data->header & VARCHAR_HEADER_FLAG_INLINED) {
        const auto aux_inlined_data = &reinterpret_cast<const VarcharAuxEntryInlined *>(column_data)[offset];
        const uint8_t size = aux_inlined_data->header >> HEADER_FLAGS_WIDTH;
        assertm(size <= VARCHAR_MAX_BYTES_FULLY_INLINED, "ERROR: invalid len of inline varchar");
        return VarcharDataView{aux_inlined_data->chars, &aux_inlined_data->chars[MIN_INLINE_CHARS], (int32_t) size};
    }
    const auto size = (int32_t) (aux_data->header >> HEADER_FLAGS_WIDTH);

    assertm(size > VARCHAR_MAX_BYTES_FULLY_INLINED, "ERROR: invalid varchar non-inlined size");
    const uint64_t data_offset = aux_data->offset_lo | ((uint64_t) aux_data->offset_hi) << 16;

    assert(data_offset < (uint64_t) column_var_data_len || "ERROR: reading beyond varchar address length!");

    const uint8_t *data = &reinterpret_cast<const uint8_t *>(column_var_data)[data_offset];

    assertm(std::memcmp(aux_data->chars, data, MIN_INLINE_CHARS) == 0,
            "ERROR: varchar inline prefix does not match the data");
    return VarcharDataView{aux_data->chars, &data[MIN_INLINE_CHARS], size};
}

inline int compare_varchar(const VarcharDataView &l_val, const VarcharDataView &r_val) {
    const auto min_size = std::min(l_val.size, r_val.size);
    switch (min_size) {
        case -1:
        case 0:
            return l_val.size - r_val.size;

        case 1:
        case 2:
        case 3:
        case 4:
        case 5:
        case 6: {
            auto diff = std::memcmp(l_val.inline_chars, r_val.inline_chars, min_size);
            return diff != 0 ? diff : l_val.size - r_val.size;
        }

        default: {
            auto diff = std::memcmp(l_val.inline_chars, r_val.inline_chars, MIN_INLINE_CHARS);
            if (diff != 0) {
                return diff;
            }
            auto diff2 = std::memcmp(l_val.remaining_chars, r_val.remaining_chars, min_size - MIN_INLINE_CHARS);
            return diff2 != 0 ? diff2 : l_val.size - r_val.size;
        }
    }
}


class SortVarcharColumnComparer : dedup_column {
public:
    inline int operator()(int64_t l, int64_t r) const {
        const auto l_val = l > -1
                           ? read_varchar(l, this->column_data, this->column_var_data, this->column_var_data_len)
                           : read_varchar(l & ~(1ull << 63), this->o3_data, this->o3_var_data, this->o3_var_data_len);

        const auto r_val = r > -1
                           ? read_varchar(r, this->column_data, this->column_var_data, this->column_var_data_len)
                           : read_varchar(r & ~(1ull << 63), this->o3_data, this->o3_var_data, this->o3_var_data_len);

        return compare_varchar(l_val, r_val);
    }
};


class MergeVarcharColumnComparer : dedup_column {
public:
    inline int operator()(int64_t col_index, int64_t index_index) const {
        const VarcharDataView l_val = col_index >= dedup_column::column_top
                                      ? read_varchar(col_index, this->column_data, this->column_var_data,
                                                     this->column_var_data_len)
                                      : NULL_VARCHAR_VIEW;

        const VarcharDataView r_val = read_varchar(index_index, this->o3_data, this->o3_var_data,
                                                   this->o3_var_data_len);

        return compare_varchar(l_val, r_val);
    }
};

template<typename T, int item_size>
inline int compare_str_bin(const void *l_val, const void *r_val) {
    T l_size = reinterpret_cast<const T *>(l_val)[0];
    T r_size = reinterpret_cast<const T *>(r_val)[0];

    auto min_size = std::min(l_size, r_size);
    if (min_size > 0) {
        auto diff = std::memcmp(
                reinterpret_cast<const char *>(l_val) + sizeof(T),
                reinterpret_cast<const char *>(r_val) + sizeof(T),
                min_size * item_size
        );
        return diff != 0 ? diff : l_size - r_size;
    }
    return l_size - r_size;
};



template<typename T, int item_size>
class SortStrBinColumnComparer : dedup_column {

#pragma pack (push, 1)
    struct data_point {
        void *column_data;
        void *column_var_data;
        int64_t column_var_data_len;
    };
#pragma pack(pop)

public:
    inline int operator()(int64_t l, int64_t r) const {
        const auto cols = reinterpret_cast<const data_point *>(&this->column_data);
        const auto l_col = &cols[l < 0];
        const auto r_col = &cols[r < 0];

        const auto l_val_offset = reinterpret_cast<int64_t *>(l_col->column_data)[l < 0 ? l & ~(1ull << 63) : l];
        assertm(l_val_offset < l_col->column_var_data_len, "ERROR: column aux data point beyond var data buffer");

        const auto r_val_offset = reinterpret_cast<int64_t *>(r_col->column_data)[r < 0 ? r & ~(1ull << 63) : r];
        assertm(r_val_offset < r_col->column_var_data_len, "ERROR: column aux data point beyond var data buffer");

        return compare_str_bin<T, item_size>(
                reinterpret_cast<const char *>(l_col->column_var_data) + l_val_offset,
                reinterpret_cast<const char *>(r_col->column_var_data) + r_val_offset
        );
    }
};

template<typename T, int item_size>
class MergeStrBinColumnComparer : dedup_column {
public:
    inline int operator()(int64_t col_index, int64_t index_index) const {
        const auto l_val_offset = col_index >= dedup_column::column_top
                                  ? reinterpret_cast<int64_t *>(dedup_column::column_data)[col_index]
                                  : -1;

        assertm(l_val_offset < dedup_column::column_var_data_len,
                "ERROR: column aux data point beyond var data buffer");
        const char *l_val_ptr = col_index >= dedup_column::column_top ?
                                reinterpret_cast<const char *>(dedup_column::column_var_data) + l_val_offset
                                                                      : dedup_column::null_value;

        const auto r_val_offset = reinterpret_cast<int64_t *>(dedup_column::o3_data)[index_index];
        assertm(r_val_offset < dedup_column::o3_var_data_len, "ERROR: column aux data point beyond var data buffer");
        const char *r_val_ptr = reinterpret_cast<const char *>(dedup_column::o3_var_data) + r_val_offset;

        return compare_str_bin<T, item_size>(l_val_ptr, r_val_ptr);
    }
};


#endif //QUESTDB_DEDUP_COMPARERS_H

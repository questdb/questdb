//
// Created by Alex Pelagenko on 21/08/2024.
//
#ifndef QUESTDB_DEDUP_COMPARERS_H
#define QUESTDB_DEDUP_COMPARERS_H

#include "dedup.h"
#include "column_type.h"
#include <algorithm>

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

const int32_t INLINED_LENGTH_MASK = (1 << 4) - 1;
const int32_t HEADER_FLAGS_WIDTH = 4;
const int32_t MIN_INLINE_CHARS = 6;
const uint8_t HEADER_FLAG_NULL = 1 << 2;

inline VarcharDataView read_varchar(int64_t offset, const void * column_data, const void * column_var_data) {
    const auto aux_data = &reinterpret_cast<const VarcharAuxEntrySplit *>(column_data)[offset];
    if (aux_data->header & HEADER_FLAG_NULL) {
        return NULL_VARCHAR_VIEW;
    }

    if (aux_data->header & VARCHAR_HEADER_FLAG_INLINED) {
        const auto aux_inlined_data = reinterpret_cast<const VarcharAuxEntryInlined *>(aux_data);
        const uint8_t size = aux_inlined_data->header & INLINED_LENGTH_MASK;
        return VarcharDataView{aux_inlined_data->chars, &aux_inlined_data->chars[MIN_INLINE_CHARS], (int32_t) size};
    }
    const int32_t size = aux_data->header >> HEADER_FLAGS_WIDTH;
    const uint64_t data_offset = aux_data->offset_lo | ((uint64_t) aux_data->offset_lo) << 16;
    const uint8_t *data = &reinterpret_cast<const uint8_t *>(column_var_data)[data_offset];
    return VarcharDataView{aux_data->chars, &data[MIN_INLINE_CHARS], size};
}

inline int compare(const VarcharDataView &l_val, const VarcharDataView &r_val) {
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
                           ? read_varchar(l, this->column_data, this->column_var_data)
                           : read_varchar(l & ~(1ull << 63), this->column_data, this->column_var_data);

        const auto r_val = r > -1
                           ? read_varchar(r, this->column_data, this->column_var_data)
                           : read_varchar(r & ~(1ull << 63), this->column_data, this->column_var_data);

        return compare(l_val, r_val);
    }
};

class MergeVarcharColumnComparer : dedup_column {
public:
    inline int operator()(int64_t col_index, int64_t index_index) const {
        const VarcharDataView &l_val = col_index >= dedup_column::column_top
                                       ? read_varchar(col_index, this->column_data, this->column_var_data)
                                       : NULL_VARCHAR_VIEW;

        const VarcharDataView &r_val = read_varchar(index_index, this->o3_data, this->o3_var_data);

        return compare(l_val, r_val);
    }
};


#endif //QUESTDB_DEDUP_COMPARERS_H

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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


#ifndef QUESTDB_OOO_H
#define QUESTDB_OOO_H

constexpr uint8_t DEDUP_INDEX_FORMAT = 1;
constexpr uint8_t SHUFFLE_INDEX_FORMAT = 2;
constexpr uint8_t DEDUP_SHUFFLE_INDEX_FORMAT = 3;

template<auto Start, auto End, auto Inc, class F>
constexpr void constexpr_for(F &&f) {
    if constexpr (Start < End) {
        f(std::integral_constant<decltype(Start), Start>());
        constexpr_for<Start + Inc, End, Inc>(f);
    }
}

inline uint8_t range_bits(uint64_t range) {
    if (range <= 1) return 0;
    return 64 - __builtin_clzll(range - 1);
}


inline uint8_t range_bytes(uint64_t range) {
    return (range_bits(range) + 7) >> 3;
}

inline jlong merge_index_format(int64_t row_count, uint8_t reverse_index_item_bytes, uint8_t segment_bytes, uint8_t format) {
    uint8_t encoding_byte = reverse_index_item_bytes << 4 | segment_bytes;
    return static_cast<jlong>(
            row_count
            | (static_cast<jlong>(encoding_byte) << 48)
            | (static_cast<jlong>(format) << 56)
    );
}

inline uint8_t read_segment_bytes(jlong index_format) {
    return (index_format >> 48) & 0x0F;
}

inline uint8_t read_reverse_index_format_bytes(jlong index_format) {
    return (index_format >> 52) & 0x0F;
}

inline uint8_t read_format(jlong index_format) {
    return index_format >> 56;
}

inline int64_t read_row_count(jlong index_format) {
    return index_format & 0xFFFFFFFFFFFF;
}

inline void* read_reverse_index_ptr(jlong mergeIndexPtr, jlong index_format) {
    auto format = read_format(index_format);
    if (format == SHUFFLE_INDEX_FORMAT || format == DEDUP_SHUFFLE_INDEX_FORMAT) {
        auto row_count = read_row_count(index_format);
        return reinterpret_cast<void *>(mergeIndexPtr + row_count * sizeof(index_l) + sizeof (int64_t));
    }
    return nullptr;
}


typedef struct seg_info {
    int64_t wal_id;
    int64_t segment_id;
    int64_t segment_lo;
    int64_t segment_hi;
} seg_info;


typedef struct txn_info {
    int64_t segment_row_offset;
    int64_t seq_txn;
    int64_t row_count;
    int64_t seg_info_index;
} txn_info;


template<typename T>
struct index_tr_i {
    uint64_t i;
    T ri;
};


template<typename T>
struct index_tr {
    uint64_t ts;
    index_tr_i<T> i;

    uint64_t operator>>(uint64_t shr) const {
        return ts >> shr;
    }
};


#endif //QUESTDB_OOO_H

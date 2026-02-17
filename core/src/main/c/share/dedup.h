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

#ifndef QUESTDB_DEDUP_H
#define QUESTDB_DEDUP_H

// This struct represents repated pointers in dedup_column
// Sometimes it's better to view dedup_column struct as if it has an array of 2 column_pointer(s)
#pragma pack (push, 1)
struct column_pointer {
    uint8_t *aux_data;
    uint8_t *var_data;
    int64_t var_data_len;
};

// Should match data structure described in DedupColumnCommitAddresses.java
struct dedup_column {
public:
    int32_t column_type;
    int32_t value_size_bytes;
private:
    int64_t column_top;
    void *column_data;
    void *column_var_data;
    int64_t column_var_data_len;
    void *o3_data;
    void *o3_var_data;
    int64_t o3_var_data_len;
    [[maybe_unused]] int64_t java_reserved_1;
    [[maybe_unused]] int64_t java_reserved_2;
    [[maybe_unused]] int64_t java_reserved_3;
    [[maybe_unused]] int64_t java_reserved_4;
    [[maybe_unused]] int64_t java_reserved_5;
    uint8_t null_value[32];

public:
    template<typename T>
    inline const T *get_column_data() const{
        return reinterpret_cast<const T *>(column_data);
    }

    [[nodiscard]] inline const column_pointer *get_column_data_pointers() const {
        return reinterpret_cast<const column_pointer *>(&column_data);
    }

    template<typename T>
    inline const T *get_o3_data() const {
        return reinterpret_cast<const T *>(o3_data);
    }

    template<typename T>
    inline const T *get_o3_var_data() const {
        return reinterpret_cast<const T *>(o3_var_data);
    }

    template<typename T>
    inline const T **get_column_data_2d() const {
        return reinterpret_cast<const T **>(column_data);
    }

    template<typename T>
    inline const T *get_column_var_data() const {
        return reinterpret_cast<const T *>(column_var_data);
    }

    template<typename T>
    inline const T **get_column_var_data_2d() const {
        return reinterpret_cast<const T **>(column_var_data);
    }

    template<typename T>
    inline const T& get_null_value() const{
        return *reinterpret_cast<const T *>(&null_value);
    }

    [[nodiscard]]
    inline int64_t get_column_top() const {
        return column_top;
    }

    [[nodiscard]]
    inline int64_t get_column_var_data_len() const {
        return column_var_data_len;
    }

    [[nodiscard]]
    inline int64_t get_o3_var_data_len() const {
        return o3_var_data_len;
    }
};

#pragma pack(pop)

struct int256 {
    __int128 lo;
    __int128 hi;
};

inline bool operator>(const int256 &a, const int256 &b) {
    // First, compare the high 128-bit parts
    if (a.hi > b.hi) {
        return true;
    } else if (a.hi < b.hi) {
        return false;
    }

    // If the high parts are equal, compare the low 128-bit parts
    return a.lo > b.lo;
}

inline bool operator<(const int256 &a, const int256 &b) {
    // First, compare the high 128-bit parts
    if (a.hi < b.hi) {
        return true;
    } else if (a.hi > b.hi) {
        return false;
    }

    // If the high parts are equal, compare the low 128-bit parts
    return a.lo < b.lo;
}

inline bool operator!=(const int256 &a, const int256 &b) {
    return a.hi != b.hi || a.lo != b.lo;
}

#endif //QUESTDB_DEDUP_H

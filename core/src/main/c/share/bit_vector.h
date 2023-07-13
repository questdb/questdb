/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

#ifndef QUESTDB_BIT_VECTOR_H
#define QUESTDB_BIT_VECTOR_H

#include "../share/simd.h"

// Custom implementation of std::vector<bool>
// which is a problem to link on Windows, and in general std::vector<bool> may be dropped from STL
template<typename T>
class bit_vector_t {
public:
    bit_vector_t() {
        size_elements = 0ll;
        bit_array = nullptr;
    }

    ~bit_vector_t() {
        if (size_elements > 0 && bit_array != nullptr) {
            bit_array = static_cast<T *>(realloc(bit_array, 0));
            size_elements = 0;
        }
    }

    void reset(const size_t length_bits) {
        const auto elements = (length_bits + bits_per_element - 1) / bits_per_element;
        if (size_elements < elements) {
            bit_array = static_cast<T *>(realloc(bit_array, elements * sizeof(T)));
            size_elements = elements;
        }
        __MEMSET(bit_array, 0, elements * sizeof(T));
    }

    inline void set(const int64_t index) {
        const auto pos = index / bits_per_element;
        const auto bit = index & (bits_per_element - 1);
        bit_array[pos] |= (1 << bit);
    }

    inline bool operator[](const int64_t index) const {
        const auto pos = index / bits_per_element;
        const auto bit = index & (bits_per_element - 1);
        return bit_array[pos] & (1 << bit);
    }

    [[nodiscard]] inline int64_t next_unset(const int64_t from) const {
        auto pos = from / bits_per_element;
        auto bit = from & (bits_per_element - 1);

        for (; pos < size_elements; ++pos) {
            T val = bit_array[pos];
            val >>= bit;
            for (; bit < bits_per_element; ++bit) {
                if ((val & 1) == 0) {
                    return pos * bits_per_element + bit;
                }
                val >>= 1;
            }
            bit = 0;
        }
        return size_elements * bits_per_element;
    }

private:
    constexpr static size_t bits_per_element = sizeof(T) * 8;
    T *bit_array;
    size_t size_elements;
};

// Use 32bit storage for the bits by default.
typedef bit_vector_t<uint32_t> bit_vector;

#endif //QUESTDB_BIT_VECTOR_H

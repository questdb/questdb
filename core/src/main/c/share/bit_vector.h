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

template<auto Start, auto End, class F>
constexpr void constexpr_for(F &&f) {
    if constexpr (Start < End) {
        f(std::integral_constant<decltype(Start), Start>());
        constexpr_for<Start + 1, End>(f);
    }
}

// Custom implementation of std::vector<bool>
// which is a problem to link on Windows, and in general std::vector<bool> may be dropped from STL
template<typename T>
class bit_vector_t {
public:
    bit_vector_t() {
        size_elements = length_bits = 0ll;
        bit_array = nullptr;
    }

    ~bit_vector_t() {
        if (length_bits > 0 && bit_array != nullptr) {
            bit_array = static_cast<T *>(realloc(bit_array, 0));
            length_bits = 0;
        }
    }

    void reset(const size_t bits) {
        const auto elements = calc_elements(bits);
        if (size_elements < elements) {
            bit_array = static_cast<T *>(realloc(bit_array, elements * sizeof(T)));
            size_elements = elements;
        }
        length_bits = bits;
        __MEMSET(bit_array, 0, elements * sizeof(T));
    }

    inline void set(const int64_t index) {
        const auto pos = index / bits_per_element;
        const auto bit = index & (bits_per_element - 1);
        bit_array[pos] |= (1 << bit);
    }

    template<typename execute_on_unset>
    inline void foreach_unset(execute_on_unset on_unset) const {
        auto size = calc_elements(length_bits);
        for (size_t pos = 0; pos < size; ++pos) {
            T val = bit_array[pos];

            // Force compiler to unroll the loop for each bit.
            constexpr_for<0, bits_per_element>(
                    [&](auto bit) {
                        constexpr T mask = (1 << bit);
                        if ((val & mask) == 0) {
                            auto bit_index = pos * bits_per_element + bit;
                            if (bit_index < length_bits) {
                                on_unset(bit_index);
                            }
                        }
                    });
        }
    }

private:
    constexpr static size_t bits_per_element = sizeof(T) * 8;
    T *bit_array;
    size_t length_bits;
    size_t size_elements;

    [[nodiscard]] inline size_t calc_elements(const size_t bits) const {
        return (bits + bits_per_element - 1) / bits_per_element;
    }
};

// Use 32bit storage for the bits by default.
typedef bit_vector_t<uint32_t> bit_vector;

#endif //QUESTDB_BIT_VECTOR_H

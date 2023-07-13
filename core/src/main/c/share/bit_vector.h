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
        size = 0ll;
        impl = nullptr;
    }

    ~bit_vector_t() {
        if (size > 0 && impl != nullptr) {
            impl = static_cast<T *>(realloc(impl, 0));
            size = 0;
        }
    }

    void reset(const size_t length) {
        const auto byte_len = (length + sizeof(T) - 1) / sizeof(T) * sizeof(T);
        if (size < byte_len) {
            impl = static_cast<T *>(realloc(impl, byte_len));
            size = byte_len;
        }
        __MEMSET(impl, 0, byte_len);
    }

    inline void set(const int64_t index) {
        const auto pos = index / sizeof(T);
        const auto bit = index & (sizeof(T) - 1);
        impl[pos] |= (1 << bit);
    }

    inline bool operator[](const int64_t index) const {
        const auto pos = index / sizeof(T);
        const auto bit = index & (sizeof(T) - 1);
        return impl[pos] & (1 << bit);
    }

private:
    T *impl;
    size_t size;
};

// Use 32bit storage for the bits by default.
typedef bit_vector_t<uint32_t > bit_vector;

#endif //QUESTDB_BIT_VECTOR_H

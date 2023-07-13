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

#ifndef QUESTDB_VECTOR_BOOL_H
#define QUESTDB_VECTOR_BOOL_H

#if !defined(_WIN32)
#include <vector>

class vector_bool {
public:
    inline void reset(int64_t length) {
        impl.assign(length, false);
    }

    inline void set(const int64_t& index, const bool & value) {
        impl[index] = value;
    }

    inline bool operator[](const int64_t& index) const {
        return impl[index];
    }
private:
    std::vector<bool> impl;
};

#else

// custom implementation of std::vector<bool>
// which is a problem to link on Windows

#include "../share/simd.h"
class vector_bool {
public:
    vector_bool() {
        size = 0ll;
        impl = nullptr;
    }

    ~vector_bool() {
        if (size > 0 && impl != nullptr) {
            free(impl);
            size = 0;
        }
    }

    void reset(int64_t length) {
        if (size < length) {
            if (size > 0) {
                size = length;
                impl = (bool*)realloc(impl, size * sizeof(bool));
            } else {
                size = length;
                impl = (bool*)malloc(size * sizeof(bool));
            }
        }
        __MEMSET(impl, 0, length * sizeof(bool));
    }

    inline void set(const int64_t& index, const bool & value) {
        impl[index] = value;
    }

    inline bool operator[](const int64_t& index) const {
        return impl[index];
    }
private:
    bool* impl;
    size_t size;
};
#endif //ifndef WIN32

#endif //QUESTDB_VECTOR_BOOL_H

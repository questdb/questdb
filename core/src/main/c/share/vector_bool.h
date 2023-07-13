//
// Created by Alex Pelagenko on 13/07/2023.
//

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
        __MEMSET(impl, 0, size * sizeof(bool));
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

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

#ifndef UTIL_H
#define UTIL_H

#include <cmath>
#include <cstdint>
#include "jni.h"

#if (defined(__GNUC__) && !defined(__clang__))
#define ATTR_UNUSED
#else
#define ATTR_UNUSED __attribute__((unused))
#endif

#if (defined(__GNUC__) && !defined(__clang__))
#define ATTRIBUTE_NEVER_INLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#define ATTRIBUTE_NEVER_INLINE __declspec(noinline)
#else
#define ATTRIBUTE_NEVER_INLINE
#endif

#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define PREDICT_TRUE(x) (__builtin_expect(false || (x), true))

#define JAVA_STATIC ATTR_UNUSED JNIEnv *e, ATTR_UNUSED jclass cl

#if __GNUC__
// Fetch into all levels of the cache hierarchy.
#define MM_PREFETCH_T0(address)  __builtin_prefetch((address), 0, 3)
// Fetch into L2 and higher.
#define MM_PREFETCH_T1(address)  __builtin_prefetch((address), 0, 2)
// Fetch into L3 and higher or an implementation-specific choice (e.g., L2 if there is no L3).
#define MM_PREFETCH_T2(address)  __builtin_prefetch((address), 0, 1)
// Fetch data using the non-temporal access (NTA) hint.
#define MM_PREFETCH_NTA(address)  __builtin_prefetch((address), 0, 0)
#else
// Fetch into all levels of the cache hierarchy.
    #define MM_PREFETCH_T0(address) _mm_prefetch((address), _MM_HINT_T0)
    // Fetch into L2 and higher.
    #define MM_PREFETCH_T1(address) _mm_prefetch((address), _MM_HINT_T1)
    // Fetch into L3 and higher or an implementation-specific choice (e.g., L2 if there is no L3).
    #define MM_PREFETCH_T2(address) _mm_prefetch((address), _MM_HINT_T2)
    // Fetch data using the non-temporal access (NTA) hint.
    #define MM_PREFETCH_NTA(address) _mm_prefetch((address), _MM_HINT_NTA)
#endif

#ifdef __APPLE__
#define __JLONG_REINTERPRET_CAST__(type, var)  (type)var
#else
#define __JLONG_REINTERPRET_CAST__(type, var)  reinterpret_cast<type>(var)
#endif

constexpr jdouble D_MAX = std::numeric_limits<jdouble>::infinity();
constexpr jdouble D_MIN = -std::numeric_limits<jdouble>::infinity();
constexpr jint S_MIN = std::numeric_limits<jshort>::min();
constexpr jint S_MAX = std::numeric_limits<jshort>::max();
constexpr jint I_MIN = std::numeric_limits<jint>::min();
constexpr jint I_MAX = std::numeric_limits<jint>::max();
constexpr jlong L_MIN = std::numeric_limits<jlong>::min();
constexpr jlong L_MAX = std::numeric_limits<jlong>::max();
constexpr uint64_t UL_MAX = std::numeric_limits<uint64_t>::max();
constexpr jdouble D_NAN = std::numeric_limits<jdouble>::quiet_NaN();

inline uint32_t ceil_pow_2(uint32_t v) {
    v--;
    v |= v >> 1u;
    v |= v >> 2u;
    v |= v >> 4u;
    v |= v >> 8u;
    v |= v >> 16u;
    return v + 1;
}

template<class T, class V>
inline int64_t scroll_up(T data, int64_t low, int64_t high, V value) {
    do {
        if (high > low) {
            high--;
        } else {
            return high;
        }
    } while (data[high] == value);
    return high + 1;
}

template<class T, class V>
inline int64_t scroll_down(T data, int64_t low, int64_t high, V value) {
    do {
        if (low < high) {
            low++;
        } else {
            return low;
        }
    } while (data[low] == value);
    return low - 1;
}

template<class T, class V>
inline int64_t scan_up(T* data, V value, int64_t low, int64_t high) {
    for (int64_t i = low; i < high; i++) {
        T that = data[i];
        if (that == value) {
            return i;
        }
        if (that > value) {
            return -(i + 1);
        }
    }
    return -(high + 1);
}

template<class T, class V>
inline int64_t scan_down(T* data, V value, int64_t low, int64_t high) {
    for (int64_t i = high - 1; i >= low; i--) {
        T that = data[i];
        if (that == value) {
            return i;
        }
        if (that < value) {
            return -(i + 2);
        }
    }
    return -(low + 1);
}

// the "high" boundary is inclusive
template<class T, class V>
inline int64_t binary_search(T *data, V value, int64_t low, int64_t high, int32_t scan_dir) {
    int64_t diff;
    while ((diff = high - low) > 65) {
        const int64_t mid = low + diff / 2;
        const T midVal = data[mid];

        if (midVal < value) {
            low = mid;
        } else if (midVal > value) {
            high = mid - 1;
        } else {
            // In case of multiple equal values, find the first
            return scan_dir == -1 ?
                   scroll_up(data, low, mid, midVal) :
                   scroll_down(data, mid, high, midVal);
        }
    }
    return scan_dir == -1 ?
           scan_up(data, value, low, high + 1) :
           scan_down(data, value, low, high + 1);
}

template<typename T>
int64_t branch_free_search_lower(const T* array, const int64_t count, T x) {
    const T *base = array;
    int64_t n = count;
    while (n > 1) {
        int64_t half = n / 2;
        MM_PREFETCH_T0(base + half / 2);
        MM_PREFETCH_T0(base + half + half / 2);
        base = (base[half] < x) ? base + half : base;
        n -= half;
    }
    return (*base < x) + base - array;
}

template<typename T>
int64_t branch_free_linked_search_lower(const int64_t * index, const T* values, const int64_t count, T x) {
    const T *base = index;
    int64_t n = count;
    while (n > 1) {
        int64_t half = n / 2;
        MM_PREFETCH_T0(base + half / 2);
        MM_PREFETCH_T0(base + half + half / 2);
        base = (values[base[half]] < x) ? base + half : base;
        n -= half;
    }
    return (values[*base] < x) + base - index;
}

template<typename T>
int64_t branch_free_search_upper(const T* array, const int64_t count, T x) {
    const T *base = array;
    int64_t n = count;
    while (n > 1) {
        int64_t half = n / 2;
        MM_PREFETCH_T0(base + half / 2);
        MM_PREFETCH_T0(base + half + half / 2);
        base = (base[half] <= x) ? base + half : base;
        n -= half;
    }
    return (*base <= x) + base - array;
}

#endif //UTIL_H

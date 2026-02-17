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

#include "byte_sink.h"
#include <stdlib.h>
#include <jni.h>
#include <limits>
#include <algorithm>

static_assert(sizeof(size_t) == 8, "size_t must be 64-bits");
static_assert(sizeof(questdb_byte_sink_t) == 29, "questdb_byte_sink_t must be 29 bytes");
static_assert(offsetof(questdb_byte_sink_t, ptr) == 0, "ptr must be at offset 0");
static_assert(offsetof(questdb_byte_sink_t, lo) == 8, "lo must be at offset 8");
static_assert(offsetof(questdb_byte_sink_t, hi) == 16, "hi must be at offset 16");
static_assert(offsetof(questdb_byte_sink_t, overflow) == 24, "overflow must be at offset 24");
static_assert(offsetof(questdb_byte_sink_t, ascii) == 28, "unicode must be at offset 28");

// Due to restrictions in the signature of `ByteSequence`,
// we need to restrict allocations to 2GiB.
static const size_t max_alloc_size = std::numeric_limits<jint>::max();

static questdb_byte_sink_t* create(size_t capacity) {
    auto* sink = (questdb_byte_sink_t*) malloc(sizeof(questdb_byte_sink_t));
    if (sink == nullptr) {
        return nullptr;
    }

    // Ensure allocation.
    // The smallest allocation we can practically make on 64-bits
    // with malloc is 32 bytes. We use this as a minimum.
    capacity = capacity < 32 ? 32 : capacity;

    sink->lo = sink->ptr = (std::byte*) malloc(capacity);
    if (sink->lo == nullptr) {
        free(sink);
        return nullptr;
    }
    sink->hi = sink->lo + capacity;
    sink->overflow = false;
    sink->ascii = true;
    return sink;
}

static void destroy(questdb_byte_sink_t* sink) {
    free(sink->lo);
    free(sink);
}

/**
 * Calculate the next power of two.
 *
 * Here are some example inputs / outputs to understand behaviour:
 *     next_pow2(2): 2
 *     next_pow2(3): 4
 *     next_pow2(4): 4
 *     next_pow2(5): 8
 *     next_pow2(6): 8
 *     next_pow2(7): 8
 *     next_pow2(8): 8
 *     next_pow2(9): 16
 *
 * Note that values of 0 and 1 yield inconsistent results between compilers and
 * platforms, but this doesn't affect usage as we never input such values.
 */
static size_t next_pow2(size_t n)
{
    // See: https://jameshfisher.com/2018/03/30/round-up-power-2/
    // In this portable code we use two different slightly different intrinsics
    // for MSVC and others.
    //  * __builtin_clz(l): counts the number of leading zeros.
    //  * _BitScanReverse(64): counts the 0-based index of the highest bit.
    // As such they need to be handled slightly differently.
    const size_t prev = n - 1;

#if defined(__GNUC__) || defined(__clang__)
    const int n_leading_zeros = (size_t)__builtin_clzll(prev);
    const size_t width = 64;
    return ((size_t)1) << (width - n_leading_zeros);
#else
    unsigned long bit_index = 0;
    _BitScanReverse64(&bit_index, prev);
    return ((size_t)1) << (bit_index + 1);
#endif
}

std::byte* questdb_byte_sink_book(questdb_byte_sink_t* sink, size_t min_len) {
    const size_t curr_avail = sink->hi - sink->ptr;
    if (curr_avail >= min_len) {
        return sink->ptr;
    }
    const size_t curr_pos = sink->ptr - sink->lo;
    const size_t curr_capacity = sink->hi - sink->lo;
    const size_t add_req_capacity = min_len - curr_avail;
    const size_t new_capacity = std::min(next_pow2(curr_capacity + add_req_capacity), max_alloc_size);

    // Cap allocation to 2GiB.
    if ((new_capacity == max_alloc_size) && (new_capacity < (curr_capacity + add_req_capacity))) {
        sink->overflow = true;
        return nullptr;
    }

    auto* const new_lo = (std::byte*) realloc(sink->lo, new_capacity);
    if (new_lo == nullptr) {
        // NB: sink->lo is still valid here and will be freed later by `destroy`.
        return nullptr;
    }
    sink->lo = new_lo;
    sink->hi = new_lo + new_capacity;
    sink->ptr = new_lo + curr_pos;
    return sink->ptr;
}

extern "C" {
    JNIEXPORT jlong JNICALL Java_io_questdb_std_bytes_DirectByteSink_implCreate(
            JNIEnv *env,
            jclass cl,
            jlong capacity);

    JNIEXPORT void JNICALL Java_io_questdb_std_bytes_DirectByteSink_implDestroy(
            JNIEnv *env,
            jclass cl,
            jlong impl);

    JNIEXPORT jlong JNICALL Java_io_questdb_std_bytes_DirectByteSink_implBook(
            JNIEnv *env,
            jclass cl,
            jlong impl,
            jlong min_len);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_bytes_DirectByteSink_implCreate(
        JNIEnv *env,
        jclass cl,
        jlong capacity) {
    return (jlong) create(capacity);
}

JNIEXPORT void JNICALL Java_io_questdb_std_bytes_DirectByteSink_implDestroy(
        JNIEnv *env,
        jclass cl,
        jlong impl) {
    destroy((questdb_byte_sink_t*) impl);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_bytes_DirectByteSink_implBook(
        JNIEnv *env,
        jclass cl,
        jlong impl,
        jlong min_len) {
    return (jlong) questdb_byte_sink_book((questdb_byte_sink_t*) impl, (size_t) min_len);
}

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

#include <jni.h>
#include <cstdint>
#include <cstring>

// On x86-64, use target attributes for AVX2 functions so they compile
// with AVX2 enabled even when the TU default is SSE2/baseline.
#if defined(__x86_64__) || defined(_M_X64)
#define HAS_X86_64 1
#include <immintrin.h>
#include <cpuid.h>

static bool detect_avx2() {
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        return (ebx & (1 << 5)) != 0; // AVX2 bit
    }
    return false;
}

static const bool HAS_AVX2 = detect_avx2();
#else
#define HAS_X86_64 0
static const bool HAS_AVX2 = false;
#endif

// ============= Pack values: residuals into bit-packed format =============

static void pack_values_scalar(const int64_t *values, int32_t count, int64_t min_value,
                                int32_t bit_width, uint8_t *dest) {
    uint64_t buffer = 0;
    int buffer_bits = 0;
    int dest_offset = 0;

    for (int i = 0; i < count; i++) {
        uint64_t offset = static_cast<uint64_t>(values[i] - min_value);
        buffer |= (offset << buffer_bits);
        buffer_bits += bit_width;

        while (buffer_bits >= 8) {
            dest[dest_offset++] = static_cast<uint8_t>(buffer);
            buffer >>= 8;
            buffer_bits -= 8;
        }
    }

    if (buffer_bits > 0) {
        dest[dest_offset] = static_cast<uint8_t>(buffer);
    }
}

// ============= Unpack: scalar fallback =============

static void unpack_all_scalar(const uint8_t *src, int32_t value_count,
                               int32_t bit_width, int64_t min_value, int64_t *dest) {
    uint64_t buffer = 0;
    int buffer_bits = 0;
    int src_offset = 0;
    uint64_t mask = (bit_width == 64) ? ~0ULL : (1ULL << bit_width) - 1;

    for (int i = 0; i < value_count; i++) {
        while (buffer_bits < bit_width) {
            buffer |= (static_cast<uint64_t>(src[src_offset]) << buffer_bits);
            buffer_bits += 8;
            src_offset++;
        }
        dest[i] = min_value + static_cast<int64_t>(buffer & mask);
        buffer >>= bit_width;
        buffer_bits -= bit_width;
    }
}

// ============= Specialized AVX2 unpack for byte-aligned widths =============

#if HAS_X86_64

__attribute__((target("avx2")))
static void unpack_8bit_avx2(const uint8_t *src, int32_t count, int64_t min_value, int64_t *dest) {
    int i = 0;
    __m256i base = _mm256_set1_epi64x(min_value);
    // Process 4 values at a time
    for (; i + 3 < count; i += 4) {
        __m128i bytes = _mm_cvtsi32_si128(*reinterpret_cast<const int32_t*>(src + i));
        __m128i ints = _mm_cvtepu8_epi32(bytes);
        __m256i longs = _mm256_cvtepu32_epi64(ints);
        __m256i result = _mm256_add_epi64(longs, base);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < count; i++) {
        dest[i] = min_value + src[i];
    }
}

__attribute__((target("avx2")))
static void unpack_16bit_avx2(const uint8_t *src, int32_t count, int64_t min_value, int64_t *dest) {
    const auto *src16 = reinterpret_cast<const uint16_t*>(src);
    int i = 0;
    __m256i base = _mm256_set1_epi64x(min_value);
    for (; i + 3 < count; i += 4) {
        __m128i words = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(src16 + i));
        __m128i ints = _mm_cvtepu16_epi32(words);
        __m256i longs = _mm256_cvtepu32_epi64(ints);
        __m256i result = _mm256_add_epi64(longs, base);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < count; i++) {
        dest[i] = min_value + src16[i];
    }
}

__attribute__((target("avx2")))
static void unpack_32bit_avx2(const uint8_t *src, int32_t count, int64_t min_value, int64_t *dest) {
    const auto *src32 = reinterpret_cast<const uint32_t*>(src);
    int i = 0;
    __m256i base = _mm256_set1_epi64x(min_value);
    for (; i + 3 < count; i += 4) {
        __m128i ints = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src32 + i));
        __m256i longs = _mm256_cvtepu32_epi64(ints);
        __m256i result = _mm256_add_epi64(longs, base);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < count; i++) {
        dest[i] = min_value + src32[i];
    }
}

#endif // HAS_X86_64

// ============= Dispatch =============

static void unpack_all_values(const uint8_t *src, int32_t value_count,
                               int32_t bit_width, int64_t min_value, int64_t *dest) {
#if HAS_X86_64
    if (HAS_AVX2) {
        switch (bit_width) {
            case 8:
                unpack_8bit_avx2(src, value_count, min_value, dest);
                return;
            case 16:
                unpack_16bit_avx2(src, value_count, min_value, dest);
                return;
            case 32:
                unpack_32bit_avx2(src, value_count, min_value, dest);
                return;
            default:
                break;
        }
    }
#endif
    unpack_all_scalar(src, value_count, bit_width, min_value, dest);
}

// ============= JNI Exports =============

extern "C" {

JNIEXPORT void JNICALL
Java_io_questdb_cairo_idx_PostingsIndexNative_packValues0(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        jlong valuesAddr,
        jint count,
        jlong minValue,
        jint bitWidth,
        jlong destAddr
) {
    pack_values_scalar(
        reinterpret_cast<const int64_t *>(valuesAddr),
        count,
        minValue,
        bitWidth,
        reinterpret_cast<uint8_t *>(destAddr)
    );
}

JNIEXPORT void JNICALL
Java_io_questdb_cairo_idx_PostingsIndexNative_unpackAllValues0(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        jlong srcAddr,
        jint valueCount,
        jint bitWidth,
        jlong minValue,
        jlong destAddr
) {
    unpack_all_values(
        reinterpret_cast<const uint8_t *>(srcAddr),
        valueCount,
        bitWidth,
        minValue,
        reinterpret_cast<int64_t *>(destAddr)
    );
}

} // extern "C"

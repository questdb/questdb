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

#if defined(_MSC_VER)
#include <intrin.h>
static bool detect_avx2() {
    int cpuInfo[4];
    __cpuidex(cpuInfo, 7, 0);
    return (cpuInfo[1] & (1 << 5)) != 0; // AVX2 bit in EBX
}
#else
#include <cpuid.h>
static bool detect_avx2() {
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        return (ebx & (1 << 5)) != 0; // AVX2 bit
    }
    return false;
}
#endif

// MSVC doesn't support __attribute__((target(...))); use /arch:AVX2 at build level instead.
#if defined(__GNUC__) || defined(__clang__)
#define TARGET_AVX2 __attribute__((target("avx2")))
#else
#define TARGET_AVX2
#endif

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
        int old_buffer_bits = buffer_bits;
        buffer |= (offset << buffer_bits);
        buffer_bits += bit_width;

        while (buffer_bits >= 8) {
            dest[dest_offset++] = static_cast<uint8_t>(buffer);
            buffer >>= 8;
            buffer_bits -= 8;
        }

        // When old_buffer_bits + bit_width > 64, the shift lost high bits of
        // offset. After flushing, replace the incorrect residual with the
        // actual high bits.
        if (old_buffer_bits + bit_width > 64) {
            int lo_bits_stored = 64 - old_buffer_bits;
            buffer = offset >> lo_bits_stored;
            buffer_bits = bit_width - lo_bits_stored;
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
        uint64_t spill_bits = 0;
        int spill_count = 0;
        while (buffer_bits < bit_width) {
            uint64_t b = static_cast<uint64_t>(src[src_offset]);
            src_offset++;
            if (buffer_bits <= 56) {
                buffer |= (b << buffer_bits);
                buffer_bits += 8;
            } else {
                int fit_bits = 64 - buffer_bits;
                buffer |= (b << buffer_bits);
                spill_bits = b >> fit_bits;
                spill_count = 8 - fit_bits;
                buffer_bits = 64;
            }
        }
        dest[i] = min_value + static_cast<int64_t>(buffer & mask);
        if (bit_width < 64) {
            buffer >>= bit_width;
        } else {
            buffer = 0;
        }
        buffer_bits -= bit_width;
        if (spill_count > 0) {
            buffer |= (spill_bits << buffer_bits);
            buffer_bits += spill_count;
        }
    }
}

// ============= Specialized AVX2 unpack for byte-aligned widths =============

#if HAS_X86_64

TARGET_AVX2
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

TARGET_AVX2
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

TARGET_AVX2
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

// ============= Lemire-style AVX2 unpack using shuffle+variable-shift =============
// For each group of 8 values (8*W bits = W bytes of packed data):
// 1. Load W bytes into low part of __m256i
// 2. Shuffle bytes so each 32-bit lane holds the 4 bytes containing one value
// 3. Variable-shift right to align the value to bit 0 in each lane
// 4. Mask to W bits
// 5. Widen 8 × uint32 → 8 × int64, add minValue, store
//
// The shuffle mask and shift amounts are precomputed per bitwidth.
// This gives true SIMD extraction: 8 values per ~5 AVX2 instructions.

// (removed unused UnpackParams / make_unpack_params / unpack_lemire_avx2)

// ============= Hybrid AVX2 unpack for arbitrary bitwidths 1-31 ==================
// Scalar extraction (8-byte load + shift + mask per value) into a uint32 buffer,
// then SIMD widen (cvtepu32_epi64) + add (epi64) + store (256-bit).
// The widen+store is the bottleneck in the Java scalar path — this eliminates it.
// Processes 8 values per iteration: scalar extract 8 × uint32, then 2 × AVX2 stores.

// Read exactly the bytes that contain the bit-packed value at position
// (bo,bs) — never reads past byte (bo + ceil((bs + bit_width) / 8) - 1).
// Used by the byte-safe tail of the AVX2 unpackers to avoid the 8-byte
// memcpy overread that affects the last values when the packed buffer is
// not allocated with trailing slack.
static inline uint64_t read_bits_safe(const uint8_t *src, int bo, int bs, int bit_width) {
    uint64_t raw = 0;
    int bytes_to_read = (bs + bit_width + 7) >> 3;
    for (int b = 0; b < bytes_to_read; b++) {
        raw |= (uint64_t) src[bo + b] << (b * 8);
    }
    return raw >> bs;
}

TARGET_AVX2
static void unpack_general_avx2(const uint8_t *src, int32_t count,
                                 int32_t bit_width, int64_t min_value, int64_t *dest) {
    uint32_t mask32 = (bit_width == 32) ? ~0u : (1u << bit_width) - 1;
    __m256i vbase = _mm256_set1_epi64x(min_value);

    // The 8-byte memcpy below reads bytes [bo, bo+8). For a packed buffer
    // sized exactly ceil(count*bit_width/8), the last few values' loads
    // overread past the end. Restrict the AVX2 fast path to values where
    // the load is provably safe and let the byte-safe tail handle the rest.
    const int tail_size = (64 + bit_width - 1) / bit_width; // values needing safe reads
    const int safe_end = count > tail_size ? count - tail_size : 0;

    int i = 0;
    for (; i + 7 < safe_end; i += 8) {
        uint32_t vals[8];
        for (int k = 0; k < 8; k++) {
            int64_t bp = (int64_t)(i + k) * bit_width;
            int bo = (int)(bp >> 3);
            int bs = (int)(bp & 7);
            uint64_t raw;
            std::memcpy(&raw, src + bo, sizeof(raw));
            vals[k] = (uint32_t)((raw >> bs) & mask32);
        }
        __m128i lo4 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(vals));
        __m128i hi4 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(vals + 4));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i),
                            _mm256_add_epi64(_mm256_cvtepu32_epi64(lo4), vbase));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i + 4),
                            _mm256_add_epi64(_mm256_cvtepu32_epi64(hi4), vbase));
    }
    for (; i < count; i++) {
        int64_t bp = (int64_t)i * bit_width;
        int bo = (int)(bp >> 3);
        int bs = (int)(bp & 7);
        dest[i] = min_value + (int64_t)(read_bits_safe(src, bo, bs, bit_width) & mask32);
    }
}

TARGET_AVX2
static void unpack_from_general_avx2(const uint8_t *src, int32_t start_index, int32_t value_count,
                                      int32_t bit_width, int64_t min_value, int64_t *dest) {
    uint32_t mask32 = (bit_width == 32) ? ~0u : (1u << bit_width) - 1;
    __m256i vbase = _mm256_set1_epi64x(min_value);
    // Promote start_index to int64_t before arithmetic so that the
    // subsequent (start_index + i + k) * bit_width stays in 64-bit even
    // when start_index alone is close to INT32_MAX.
    const int64_t start64 = static_cast<int64_t>(start_index);

    // Caller's logical end is start_index + value_count. The buffer is
    // sized for the FULL value count (covers indices >= start_index +
    // value_count too), so the last unsafe values are those near the
    // global end of the buffer. We don't have the global count here, but
    // we conservatively cap the AVX2 fast path so that even if start_index
    // + value_count is the global end, the 8-byte loads stay safe.
    const int tail_size = (64 + bit_width - 1) / bit_width;
    const int safe_end = value_count > tail_size ? value_count - tail_size : 0;

    int i = 0;
    for (; i + 7 < safe_end; i += 8) {
        uint32_t vals[8];
        for (int k = 0; k < 8; k++) {
            int64_t bp = (start64 + i + k) * (int64_t) bit_width;
            int bo = (int)(bp >> 3);
            int bs = (int)(bp & 7);
            uint64_t raw;
            std::memcpy(&raw, src + bo, sizeof(raw));
            vals[k] = (uint32_t)((raw >> bs) & mask32);
        }
        __m128i lo4 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(vals));
        __m128i hi4 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(vals + 4));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i),
                            _mm256_add_epi64(_mm256_cvtepu32_epi64(lo4), vbase));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i + 4),
                            _mm256_add_epi64(_mm256_cvtepu32_epi64(hi4), vbase));
    }
    for (; i < value_count; i++) {
        int64_t bp = (start64 + i) * (int64_t) bit_width;
        int bo = (int)(bp >> 3);
        int bs = (int)(bp & 7);
        dest[i] = min_value + (int64_t)(read_bits_safe(src, bo, bs, bit_width) & mask32);
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
                if (bit_width > 0 && bit_width < 32) {
                    unpack_general_avx2(src, value_count, bit_width, min_value, dest);
                    return;
                }
                break;
        }
    }
#endif
    unpack_all_scalar(src, value_count, bit_width, min_value, dest);
}

// ============= Unpack from arbitrary start index =============

static void unpack_from_scalar(const uint8_t *src, int32_t start_index, int32_t value_count,
                                int32_t bit_width, int64_t min_value, int64_t *dest) {
    uint64_t mask = (bit_width == 64) ? ~0ULL : (1ULL << bit_width) - 1;

    // Seek to the byte containing the first value's bits
    int64_t bit_pos = (int64_t)start_index * bit_width;
    int src_offset = (int)(bit_pos / 8);
    int skip_bits = (int)(bit_pos % 8);

    uint64_t buffer = 0;
    int buffer_bits = 0;

    // Pre-fill buffer past skip bits
    while (buffer_bits < skip_bits + bit_width) {
        buffer |= (static_cast<uint64_t>(src[src_offset]) << buffer_bits);
        buffer_bits += 8;
        src_offset++;
    }
    buffer >>= skip_bits;
    buffer_bits -= skip_bits;

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

static void unpack_values_from(const uint8_t *src, int32_t start_index, int32_t value_count,
                                int32_t bit_width, int64_t min_value, int64_t *dest) {
#if HAS_X86_64
    if (HAS_AVX2) {
        // For byte-aligned widths, offset the source pointer and use AVX2.
        // Cast start_index to int64_t before scaling so that posting-index
        // ordinals above 2^30 (~1.07B values for 32-bit, ~536M for 16-bit)
        // do not silently overflow the int32_t multiply and produce a
        // corrupted source pointer.
        const int64_t start64 = static_cast<int64_t>(start_index);
        switch (bit_width) {
            case 8:
                unpack_8bit_avx2(src + start64, value_count, min_value, dest);
                return;
            case 16:
                unpack_16bit_avx2(src + start64 * 2, value_count, min_value, dest);
                return;
            case 32:
                unpack_32bit_avx2(src + start64 * 4, value_count, min_value, dest);
                return;
            default:
                if (bit_width > 0 && bit_width < 32) {
                    unpack_from_general_avx2(src, start_index, value_count, bit_width, min_value, dest);
                    return;
                }
                break;
        }
    }
#endif
    // For non-aligned widths, use scalar with bit-level seeking
    unpack_from_scalar(src, start_index, value_count, bit_width, min_value, dest);
}

// ============= JNI Exports =============

extern "C" {

JNIEXPORT void JNICALL
Java_io_questdb_cairo_idx_PostingIndexNative_packValues0(
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
Java_io_questdb_cairo_idx_PostingIndexNative_unpackAllValues0(
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

JNIEXPORT void JNICALL
Java_io_questdb_cairo_idx_PostingIndexNative_unpackValuesFrom0(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        jlong srcAddr,
        jint startIndex,
        jint valueCount,
        jint bitWidth,
        jlong minValue,
        jlong destAddr
) {
    unpack_values_from(
        reinterpret_cast<const uint8_t *>(srcAddr),
        startIndex,
        valueCount,
        bitWidth,
        minValue,
        reinterpret_cast<int64_t *>(destAddr)
    );
}

} // extern "C"

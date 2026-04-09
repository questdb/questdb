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
#include "util.h"

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

// Constants matching io.questdb.std.Hash
static constexpr uint64_t M2 = 0x517cc1b727220a95ULL;
static constexpr uint64_t FMIX_C1 = 0xff51afd7ed558ccdULL;
static constexpr uint64_t FMIX_C2 = 0xc4ceb9fe1a85ec53ULL;

static inline uint64_t fmix64(uint64_t h) {
    h = (h ^ (h >> 33)) * FMIX_C1;
    h = (h ^ (h >> 33)) * FMIX_C2;
    return h ^ (h >> 33);
}

// Hash.hashMem64 for 4-byte key (sign-extend, used by OrderedMap).
static inline uint64_t hashMem64_4(const uint8_t *p) {
    int32_t v;
    memcpy(&v, p, 4);
    return fmix64(static_cast<uint64_t>(static_cast<int64_t>(v)));
}

// Hash.hashInt64 for 4-byte key (zero-extend, used by Unordered4Map).
static inline uint64_t hashInt64_4(const uint8_t *p) {
    uint32_t v;
    memcpy(&v, p, 4);
    return fmix64(static_cast<uint64_t>(v));
}

// Hash.hashMem64 / Hash.hashLong64 for 8-byte key.
static inline uint64_t hashMem64_8(const uint8_t *p) {
    uint64_t v;
    memcpy(&v, p, 8);
    return fmix64(v);
}

// Hash.hashMem64 for 12-byte key (e.g., INT + LONG).
static inline uint64_t hashMem64_12(const uint8_t *p) {
    uint64_t v0;
    int32_t v1;
    memcpy(&v0, p, 8);
    memcpy(&v1, p + 8, 4);
    uint64_t h = v0;
    h = h * M2 + static_cast<uint64_t>(static_cast<int64_t>(v1));
    return fmix64(h);
}

// Hash.hashMem64 for 16-byte key (e.g., LONG + LONG).
static inline uint64_t hashMem64_16(const uint8_t *p) {
    uint64_t v0, v1;
    memcpy(&v0, p, 8);
    memcpy(&v1, p + 8, 8);
    uint64_t h = v0;
    h = h * M2 + v1;
    return fmix64(h);
}

// General case: matches Hash.hashMem64 exactly.
static inline uint64_t hashMem64(const uint8_t *p, int64_t len) {
    uint64_t h = 0;
    int64_t i = 0;
    for (; i + 7 < len; i += 8) {
        uint64_t v;
        memcpy(&v, p + i, 8);
        h = h * M2 + v;
    }
    if (i + 3 < len) {
        uint32_t v;
        memcpy(&v, p + i, 4);
        h = h * M2 + static_cast<uint64_t>(static_cast<int64_t>(static_cast<int32_t>(v)));
        i += 4;
    }
    for (; i < len; i++) {
        h = h * M2 + static_cast<uint64_t>(static_cast<int64_t>(static_cast<int8_t>(p[i])));
    }
    return fmix64(h);
}

// Hash-only loop for fixed-size keys.
template<uint64_t (*HashFn)(const uint8_t *)>
static inline void hashFixedSize(
        const uint8_t *keys,
        int32_t keySize,
        int32_t keyCount,
        int64_t *hashes
) {
    for (int32_t i = 0; i < keyCount; i++) {
        hashes[i] = static_cast<int64_t>(HashFn(keys + (int64_t) i * keySize));
    }
}

// 64-bit constant for the SIMD-friendly hash (matching Hash.hashLong64Simd / hashInt64Simd).
// Split into low/high 32-bit halves so 64-bit multiplies decompose into _mm256_mul_epu32.
static constexpr uint32_t SIMD_C_LO = 0xed558ccdU;
static constexpr uint32_t SIMD_C_HI = 0xff51afd7U;
static constexpr uint64_t SIMD_C = (static_cast<uint64_t>(SIMD_C_HI) << 32) | SIMD_C_LO;

// Scalar SIMD-friendly hash for 64-bit keys (for cleanup loop).
static inline uint64_t hashLong64Simd(uint64_t k) {
    return k * SIMD_C;
}

// Scalar SIMD-friendly hash for 32-bit keys (for cleanup loop).
static inline uint64_t hashInt64Simd(uint32_t k) {
    return static_cast<uint64_t>(k) * SIMD_C;
}

#if defined(__x86_64__) || defined(_M_X64)

// Detect AVX-512 (F + DQ for VPMULLQ) at library load. Cached as a static.
#if defined(__GNUC__) || defined(__clang__)
static const bool g_has_avx512 = (__builtin_cpu_supports("avx512f") != 0
                                   && __builtin_cpu_supports("avx512dq") != 0);
#else
// MSVC and other compilers: no AVX-512 path for now.
static const bool g_has_avx512 = false;
#endif

// ---- AVX2 helpers ----

// Vectorized 64x64→64 multiply by SIMD_C, lower 64 bits.
// Decomposes as: k * C = k_lo*C_lo + ((k_lo*C_hi + k_hi*C_lo) << 32)
// (the k_hi*C_hi cross-product is dropped since it shifts entirely above bit 63).
__attribute__((target("avx2")))
static inline __m256i hashLong64Simd_avx2(__m256i k) {
    const __m256i c_lo = _mm256_set1_epi64x(SIMD_C_LO);
    const __m256i c_hi = _mm256_set1_epi64x(SIMD_C_HI);
    __m256i k_hi = _mm256_srli_epi64(k, 32);
    __m256i lo_lo = _mm256_mul_epu32(k, c_lo);
    __m256i lo_hi = _mm256_mul_epu32(k, c_hi);
    __m256i hi_lo = _mm256_mul_epu32(k_hi, c_lo);
    __m256i cross = _mm256_add_epi64(lo_hi, hi_lo);
    return _mm256_add_epi64(lo_lo, _mm256_slli_epi64(cross, 32));
}

// Vectorized hashInt64Simd: k (zero-extended to 64-bit) multiplied by SIMD_C.
// k_hi = 0, so the k_hi*C_lo cross-product vanishes; the 64x64 multiply
// collapses to just two _mm256_mul_epu32 instructions.
__attribute__((target("avx2")))
static inline __m256i hashInt64Simd_avx2(__m256i k_zext) {
    const __m256i c_lo = _mm256_set1_epi64x(SIMD_C_LO);
    const __m256i c_hi = _mm256_set1_epi64x(SIMD_C_HI);
    __m256i lo_mul = _mm256_mul_epu32(k_zext, c_lo);
    __m256i hi_mul = _mm256_mul_epu32(k_zext, c_hi);
    return _mm256_add_epi64(lo_mul, _mm256_slli_epi64(hi_mul, 32));
}

// ---- AVX-512 helpers (8 lanes per __m512i, native 64-bit multiply via VPMULLQ) ----

__attribute__((target("avx512f,avx512dq")))
static inline __m512i hashLongSimd_avx512(__m512i k) {
    return _mm512_mullo_epi64(k, _mm512_set1_epi64(static_cast<int64_t>(SIMD_C)));
}

#endif

extern "C" {

// OrderedMap: fixed-size keys, hashMem64 semantics.
JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_OrderedMap_computeHashes(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        jlong keysAddr,
        jint keySize,
        jint keyCount,
        jlong hashesOut
) {
    const auto *keys = reinterpret_cast<const uint8_t *>(keysAddr);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);

    switch (keySize) {
        case 4:
            hashFixedSize<hashMem64_4>(keys, keySize, keyCount, hashes);
            break;
        case 8:
            hashFixedSize<hashMem64_8>(keys, keySize, keyCount, hashes);
            break;
        case 12:
            hashFixedSize<hashMem64_12>(keys, keySize, keyCount, hashes);
            break;
        case 16:
            hashFixedSize<hashMem64_16>(keys, keySize, keyCount, hashes);
            break;
        default:
            for (int32_t i = 0; i < keyCount; i++) {
                hashes[i] = static_cast<int64_t>(hashMem64(keys + (int64_t) i * keySize, keySize));
            }
            break;
    }
}

// ---- Unordered4Map: 4-byte (INT) keys ----

#if defined(__x86_64__) || defined(_M_X64)

__attribute__((target("avx2")))
static void unordered4_compute_hashes_avx2(const int32_t *keys, int32_t keyCount, int64_t *hashes) {
    int32_t i = 0;
    // 8 keys per iteration: load 8 int32, split into two halves of 4,
    // zero-extend each half to 4 int64, hash, store.
    for (; i + 7 < keyCount; i += 8) {
        __m256i k8 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(keys + i));
        __m256i k_lo = _mm256_cvtepu32_epi64(_mm256_castsi256_si128(k8));
        __m256i k_hi = _mm256_cvtepu32_epi64(_mm256_extracti128_si256(k8, 1));
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(hashes + i), hashInt64Simd_avx2(k_lo));
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(hashes + i + 4), hashInt64Simd_avx2(k_hi));
    }
    // 4 keys at a time for the remainder.
    for (; i + 3 < keyCount; i += 4) {
        __m256i k = _mm256_cvtepu32_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i *>(keys + i)));
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(hashes + i), hashInt64Simd_avx2(k));
    }
    for (; i < keyCount; i++) {
        hashes[i] = static_cast<int64_t>(hashInt64Simd(static_cast<uint32_t>(keys[i])));
    }
}

__attribute__((target("avx512f,avx512dq")))
static void unordered4_compute_hashes_avx512(const int32_t *keys, int32_t keyCount, int64_t *hashes) {
    int32_t i = 0;
    // 8 keys per iteration: load 8 int32, zero-extend to 8 int64, multiply, store.
    for (; i + 7 < keyCount; i += 8) {
        __m256i k8 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(keys + i));
        __m512i k = _mm512_cvtepu32_epi64(k8);
        _mm512_storeu_si512(reinterpret_cast<__m512i *>(hashes + i), hashLongSimd_avx512(k));
    }
    for (; i < keyCount; i++) {
        hashes[i] = static_cast<int64_t>(hashInt64Simd(static_cast<uint32_t>(keys[i])));
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_Unordered4Map_computeHashes(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        jlong keysAddr,
        jint keyCount,
        jlong hashesOut
) {
    const auto *keys = reinterpret_cast<const int32_t *>(keysAddr);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);
    if (g_has_avx512) {
        unordered4_compute_hashes_avx512(keys, keyCount, hashes);
    } else {
        unordered4_compute_hashes_avx2(keys, keyCount, hashes);
    }
}

#else

JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_Unordered4Map_computeHashes(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        jlong keysAddr,
        jint keyCount,
        jlong hashesOut
) {
    const auto *keys = reinterpret_cast<const int32_t *>(keysAddr);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);

    for (int32_t i = 0; i < keyCount; i++) {
        hashes[i] = static_cast<int64_t>(hashInt64Simd(static_cast<uint32_t>(keys[i])));
    }
}

#endif

// ---- Unordered8Map: 8-byte (LONG) keys ----

#if defined(__x86_64__) || defined(_M_X64)

__attribute__((target("avx2")))
static void unordered8_compute_hashes_avx2(const int64_t *keys, int32_t keyCount, int64_t *hashes) {
    int32_t i = 0;
    for (; i + 3 < keyCount; i += 4) {
        __m256i k = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(keys + i));
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(hashes + i), hashLong64Simd_avx2(k));
    }
    for (; i < keyCount; i++) {
        hashes[i] = static_cast<int64_t>(hashLong64Simd(static_cast<uint64_t>(keys[i])));
    }
}

__attribute__((target("avx512f,avx512dq")))
static void unordered8_compute_hashes_avx512(const int64_t *keys, int32_t keyCount, int64_t *hashes) {
    int32_t i = 0;
    // 8 keys per iteration: load 8 int64, native 64-bit multiply, store.
    for (; i + 7 < keyCount; i += 8) {
        __m512i k = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(keys + i));
        _mm512_storeu_si512(reinterpret_cast<__m512i *>(hashes + i), hashLongSimd_avx512(k));
    }
    for (; i < keyCount; i++) {
        hashes[i] = static_cast<int64_t>(hashLong64Simd(static_cast<uint64_t>(keys[i])));
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_Unordered8Map_computeHashes(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        jlong keysAddr,
        jint keyCount,
        jlong hashesOut
) {
    const auto *keys = reinterpret_cast<const int64_t *>(keysAddr);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);
    if (g_has_avx512) {
        unordered8_compute_hashes_avx512(keys, keyCount, hashes);
    } else {
        unordered8_compute_hashes_avx2(keys, keyCount, hashes);
    }
}

#else

JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_Unordered8Map_computeHashes(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        jlong keysAddr,
        jint keyCount,
        jlong hashesOut
) {
    const auto *keys = reinterpret_cast<const int64_t *>(keysAddr);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);

    for (int32_t i = 0; i < keyCount; i++) {
        hashes[i] = static_cast<int64_t>(hashLong64Simd(static_cast<uint64_t>(keys[i])));
    }
}

#endif

// OrderedMap: var-size keys with per-key offsets, hashMem64 semantics.
JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_OrderedMap_computeHashesVarSize(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        jlong keysAddr,
        jlong keyOffsetsAddr,
        jint keyCount,
        jlong hashesOut
) {
    const auto *keys = reinterpret_cast<const uint8_t *>(keysAddr);
    const auto *keyOffsets = reinterpret_cast<const int64_t *>(keyOffsetsAddr);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);

    for (int32_t i = 0; i < keyCount; i++) {
        const uint8_t *keyStart = keys + keyOffsets[i];
        int32_t keyLen;
        memcpy(&keyLen, keyStart, 4); // read 4-byte length prefix
        hashes[i] = static_cast<int64_t>(hashMem64(keyStart + 4, keyLen));
    }
}

// UnorderedVarcharMap: pointer+size arrays, hashMem64 semantics.
JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_UnorderedVarcharMap_computeHashes(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        jlong ptrsAddr,
        jlong sizesAddr,
        jint keyCount,
        jlong hashesOut
) {
    const auto *ptrs = reinterpret_cast<const int64_t *>(ptrsAddr);
    const auto *sizes = reinterpret_cast<const int32_t *>(sizesAddr);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);

    for (int32_t i = 0; i < keyCount; i++) {
        // Mask out the unstable flag (MSB) to get the raw pointer.
        const auto *ptr = reinterpret_cast<const uint8_t *>(ptrs[i] & 0x7FFFFFFFFFFFFFFFL);
        hashes[i] = static_cast<int64_t>(hashMem64(ptr, sizes[i]));
    }
}

} // extern "C"

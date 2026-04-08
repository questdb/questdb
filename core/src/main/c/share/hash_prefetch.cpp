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

// Prevent dead-code elimination of the accumulator without side effects.
#if __GNUC__
#define DO_NOT_ELIMINATE(x) asm volatile("" : "+r"(x))
#else
// MSVC: volatile write to a local is sufficient to prevent elimination.
static volatile int64_t _dne_sink;
#define DO_NOT_ELIMINATE(x) (_dne_sink = (x))
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

// Hash + demand-load with offset list stride of 8 (OrderedMap layout).
template<uint64_t (*HashFn)(const uint8_t *)>
static inline void hashAndPrefetchOffsetList(
        const uint8_t *keys,
        int32_t keySize,
        int32_t keyCount,
        const uint8_t *offsets,
        int32_t mask,
        int64_t *hashes
) {
    int64_t acc = 0;
    for (int32_t i = 0; i < keyCount; i++) {
        const uint64_t h = HashFn(keys + (int64_t) i * keySize);
        hashes[i] = static_cast<int64_t>(h);
        const auto index = static_cast<int32_t>(h) & mask;
        acc += *reinterpret_cast<const int64_t *>(offsets + ((int64_t) index << 3));
    }
    DO_NOT_ELIMINATE(acc);
}

// Hash + demand-load with configurable entry stride (Unordered map layout).
template<uint64_t (*HashFn)(const uint8_t *)>
static inline void hashAndPrefetchEntries(
        const uint8_t *keys,
        int32_t keySize,
        int32_t keyCount,
        const uint8_t *memStart,
        int32_t entrySize,
        int32_t mask,
        int64_t *hashes
) {
    int64_t acc = 0;
    for (int32_t i = 0; i < keyCount; i++) {
        const uint64_t h = HashFn(keys + (int64_t) i * keySize);
        hashes[i] = static_cast<int64_t>(h);
        const auto index = static_cast<int32_t>(h) & mask;
        acc += *reinterpret_cast<const int64_t *>(memStart + (int64_t) index * entrySize);
    }
    DO_NOT_ELIMINATE(acc);
}

extern "C" {

// OrderedMap: offset list with stride 8, hashMem64 semantics.
JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_OrderedMap_hashAndPrefetch(
        JNIEnv *env,
        jclass cl,
        jlong keysAddr,
        jint keySize,
        jint keyCount,
        jlong offsetsAddr,
        jint mask,
        jlong hashesOut
) {
    const auto *keys = reinterpret_cast<const uint8_t *>(keysAddr);
    const auto *offsets = reinterpret_cast<const uint8_t *>(offsetsAddr);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);

    switch (keySize) {
        case 4:
            hashAndPrefetchOffsetList<hashMem64_4>(keys, keySize, keyCount, offsets, mask, hashes);
            break;
        case 8:
            hashAndPrefetchOffsetList<hashMem64_8>(keys, keySize, keyCount, offsets, mask, hashes);
            break;
        case 12:
            hashAndPrefetchOffsetList<hashMem64_12>(keys, keySize, keyCount, offsets, mask, hashes);
            break;
        case 16:
            hashAndPrefetchOffsetList<hashMem64_16>(keys, keySize, keyCount, offsets, mask, hashes);
            break;
        default: {
            int64_t acc = 0;
            for (int32_t i = 0; i < keyCount; i++) {
                const uint64_t h = hashMem64(keys + (int64_t) i * keySize, keySize);
                hashes[i] = static_cast<int64_t>(h);
                const auto index = static_cast<int32_t>(h) & mask;
                acc += *reinterpret_cast<const int64_t *>(offsets + ((int64_t) index << 3));
            }
            DO_NOT_ELIMINATE(acc);
            break;
        }
    }
}

// Unordered4Map: entry stride, hashInt64 semantics (zero-extend 4B key).
JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_Unordered4Map_hashAndPrefetch(
        JNIEnv *env,
        jclass cl,
        jlong keysAddr,
        jint keyCount,
        jlong memStart,
        jint entrySize,
        jint mask,
        jlong hashesOut
) {
    const auto *keys = reinterpret_cast<const uint8_t *>(keysAddr);
    const auto *mem = reinterpret_cast<const uint8_t *>(memStart);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);

    hashAndPrefetchEntries<hashInt64_4>(keys, 4, keyCount, mem, entrySize, mask, hashes);
}

// Unordered8Map: entry stride, hashLong64 semantics (= hashMem64 for 8B).
JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_Unordered8Map_hashAndPrefetch(
        JNIEnv *env,
        jclass cl,
        jlong keysAddr,
        jint keyCount,
        jlong memStart,
        jint entrySize,
        jint mask,
        jlong hashesOut
) {
    const auto *keys = reinterpret_cast<const uint8_t *>(keysAddr);
    const auto *mem = reinterpret_cast<const uint8_t *>(memStart);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);

    hashAndPrefetchEntries<hashMem64_8>(keys, 8, keyCount, mem, entrySize, mask, hashes);
}

// OrderedMap: var-size keys with per-key offsets, hashMem64 semantics.
JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_OrderedMap_hashAndPrefetchVarSize(
        JNIEnv *env,
        jclass cl,
        jlong keysAddr,
        jlong keyOffsetsAddr,
        jint keyCount,
        jlong offsetsAddr,
        jint mask,
        jlong hashesOut
) {
    const auto *keys = reinterpret_cast<const uint8_t *>(keysAddr);
    const auto *keyOffsets = reinterpret_cast<const int64_t *>(keyOffsetsAddr);
    const auto *offsets = reinterpret_cast<const uint8_t *>(offsetsAddr);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);

    int64_t acc = 0;
    for (int32_t i = 0; i < keyCount; i++) {
        const uint8_t *keyStart = keys + keyOffsets[i];
        int32_t keyLen;
        memcpy(&keyLen, keyStart, 4); // read 4-byte length prefix
        const uint64_t h = hashMem64(keyStart + 4, keyLen); // hash data portion
        hashes[i] = static_cast<int64_t>(h);
        const auto index = static_cast<int32_t>(h) & mask;
        acc += *reinterpret_cast<const int64_t *>(offsets + ((int64_t) index << 3));
    }
    DO_NOT_ELIMINATE(acc);
}

// UnorderedVarcharMap: pointer+size arrays, entry-based demand-load, hashMem64 semantics.
JNIEXPORT void JNICALL
Java_io_questdb_cairo_map_UnorderedVarcharMap_hashAndPrefetch(
        JNIEnv *env,
        jclass cl,
        jlong ptrsAddr,
        jlong sizesAddr,
        jint keyCount,
        jlong memStart,
        jint entrySize,
        jint mask,
        jlong hashesOut
) {
    const auto *ptrs = reinterpret_cast<const int64_t *>(ptrsAddr);
    const auto *sizes = reinterpret_cast<const int32_t *>(sizesAddr);
    const auto *mem = reinterpret_cast<const uint8_t *>(memStart);
    auto *hashes = reinterpret_cast<int64_t *>(hashesOut);

    int64_t acc = 0;
    for (int32_t i = 0; i < keyCount; i++) {
        // Mask out the unstable flag (MSB) to get the raw pointer.
        const auto *ptr = reinterpret_cast<const uint8_t *>(ptrs[i] & 0x7FFFFFFFFFFFFFFFL);
        const int32_t sz = sizes[i];
        const uint64_t h = hashMem64(ptr, sz);
        hashes[i] = static_cast<int64_t>(h);
        const auto index = static_cast<int32_t>(h) & mask;
        acc += *reinterpret_cast<const int64_t *>(mem + (int64_t) index * entrySize);
    }
    DO_NOT_ELIMINATE(acc);
}

} // extern "C"

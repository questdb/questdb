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

// Constants matching io.questdb.std.Hash
static constexpr uint64_t M2 = 0x517cc1b727220a95ULL;
static constexpr uint64_t FMIX_C1 = 0xff51afd7ed558ccdULL;
static constexpr uint64_t FMIX_C2 = 0xc4ceb9fe1a85ec53ULL;

static inline uint64_t fmix64(uint64_t h) {
    h = (h ^ (h >> 33)) * FMIX_C1;
    h = (h ^ (h >> 33)) * FMIX_C2;
    return h ^ (h >> 33);
}

// Fast path: 4-byte key (e.g., single INT column).
static inline uint64_t hashMem64_4(const uint8_t *p) {
    int32_t v;
    memcpy(&v, p, 4);
    return fmix64(static_cast<uint64_t>(static_cast<int64_t>(v)));
}

// Fast path: 8-byte key (e.g., single LONG column).
static inline uint64_t hashMem64_8(const uint8_t *p) {
    uint64_t v;
    memcpy(&v, p, 8);
    return fmix64(v);
}

// Fast path: 12-byte key (e.g., INT + LONG).
static inline uint64_t hashMem64_12(const uint8_t *p) {
    uint64_t v0;
    int32_t v1;
    memcpy(&v0, p, 8);
    memcpy(&v1, p + 8, 4);
    uint64_t h = v0;
    h = h * M2 + static_cast<uint64_t>(static_cast<int64_t>(v1));
    return fmix64(h);
}

// Fast path: 16-byte key (e.g., LONG + LONG or INT + INT + LONG).
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

template<uint64_t (*HashFn)(const uint8_t *)>
static inline void hashAndPrefetchTyped(
        const uint8_t *keys,
        int32_t keySize,
        int32_t keyCount,
        const uint8_t *offsets,
        int32_t mask,
        int64_t *hashes
) {
    for (int32_t i = 0; i < keyCount; i++) {
        const uint64_t h = HashFn(keys + (int64_t) i * keySize);
        hashes[i] = static_cast<int64_t>(h);
        const auto index = static_cast<int32_t>(h) & mask;
        __builtin_prefetch(offsets + ((int64_t) index << 3), 0, 1);
    }
}

extern "C" {

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_hashAndPrefetch(
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
            hashAndPrefetchTyped<hashMem64_4>(keys, keySize, keyCount, offsets, mask, hashes);
            break;
        case 8:
            hashAndPrefetchTyped<hashMem64_8>(keys, keySize, keyCount, offsets, mask, hashes);
            break;
        case 12:
            hashAndPrefetchTyped<hashMem64_12>(keys, keySize, keyCount, offsets, mask, hashes);
            break;
        case 16:
            hashAndPrefetchTyped<hashMem64_16>(keys, keySize, keyCount, offsets, mask, hashes);
            break;
        default:
            for (int32_t i = 0; i < keyCount; i++) {
                const uint64_t h = hashMem64(keys + (int64_t) i * keySize, keySize);
                hashes[i] = static_cast<int64_t>(h);
                const auto index = static_cast<int32_t>(h) & mask;
                __builtin_prefetch(offsets + ((int64_t) index << 3), 0, 1);
            }
            break;
    }
}

} // extern "C"

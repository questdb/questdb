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

#include "utf8.h"
#include <cstdint>
#include <cstring>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

// On aarch64, MULTI_VERSION_NAME is not defined by dispatcher.h.
// Define it as identity so the function gets its plain name.
#ifndef MULTI_VERSION_NAME
#define MULTI_VERSION_NAME(x) x
#endif

// Returns the 48-bit data vector offset stored in aux entry bytes [10..15].
// Reads 8 bytes from offset 8 and shifts right by 16 to discard the 2 prefix bytes at [8..9].
static inline int64_t get_data_offset(const char *auxEntry) {
    uint64_t raw64;
    memcpy(&raw64, auxEntry + 8, sizeof(uint64_t));
    return static_cast<int64_t>(raw64 >> 16);
}

// Count UTF-8 continuation bytes in a byte range using SWAR (8 bytes at a time).
// A continuation byte has the pattern 10xxxxxx: bit 7 set, bit 6 clear.
static inline int32_t count_continuation_bytes_swar(const char *data, int32_t size) {
    int32_t continuations = 0;
    int32_t i = 0;

    // Process 8 bytes at a time
    for (; i <= size - 8; i += 8) {
        uint64_t c;
        memcpy(&c, data + i, sizeof(uint64_t));
        uint64_t x = c & 0x8080808080808080ULL;
        uint64_t y = ~c << 1;
        continuations += __builtin_popcountll(x & y);
    }

    // Tail bytes
    for (; i < size; i++) {
        uint8_t c = static_cast<uint8_t>(data[i]);
        continuations += ((c & 0x80) & (~c << 1)) >> 7;
    }

    return continuations;
}

#if defined(__x86_64__) || defined(_M_X64)

#if INSTRSET >= 10 // AVX-512

static inline int32_t count_continuation_bytes_simd(const char *data, int32_t size) {
    int32_t continuations = 0;
    int32_t i = 0;
    const __m512i mask_0xc0 = _mm512_set1_epi8(static_cast<char>(0xC0));
    const __m512i val_0x80 = _mm512_set1_epi8(static_cast<char>(0x80));

    // Process 64 bytes at a time with AVX-512
    for (; i <= size - 64; i += 64) {
        __m512i v = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(data + i));
        __m512i masked = _mm512_and_si512(v, mask_0xc0);
        __mmask64 is_cont = _mm512_cmpeq_epi8_mask(masked, val_0x80);
        continuations += _mm_popcnt_u64(is_cont);
    }

    // Tail via masked AVX-512 load (no SWAR fallback needed).
    // Masked loads do not fault on addresses corresponding to masked-off lanes.
    if (i < size) {
        __mmask64 tail_mask = (static_cast<uint64_t>(1) << (size - i)) - 1;
        __m512i v = _mm512_maskz_loadu_epi8(tail_mask, reinterpret_cast<const __m512i *>(data + i));
        __m512i masked = _mm512_and_si512(v, mask_0xc0);
        __mmask64 is_cont = _mm512_cmpeq_epi8_mask(masked, val_0x80);
        continuations += _mm_popcnt_u64(is_cont);
    }
    return continuations;
}

#elif INSTRSET >= 8 // AVX2

static inline int32_t count_continuation_bytes_simd(const char *data, int32_t size) {
    int32_t continuations = 0;
    int32_t i = 0;
    const __m256i mask_0xc0 = _mm256_set1_epi8(static_cast<char>(0xC0));
    const __m256i val_0x80 = _mm256_set1_epi8(static_cast<char>(0x80));

    // Process 32 bytes at a time with AVX2
    for (; i <= size - 32; i += 32) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + i));
        __m256i masked = _mm256_and_si256(v, mask_0xc0);
        __m256i is_cont = _mm256_cmpeq_epi8(masked, val_0x80);
        uint32_t mask_val = static_cast<uint32_t>(_mm256_movemask_epi8(is_cont));
        continuations += __builtin_popcount(mask_val);
    }

    // Tail via SWAR
    continuations += count_continuation_bytes_swar(data + i, size - i);
    return continuations;
}

#elif INSTRSET >= 5 // SSE4.1

static inline int32_t count_continuation_bytes_simd(const char *data, int32_t size) {
    int32_t continuations = 0;
    int32_t i = 0;
    const __m128i mask_0xc0 = _mm_set1_epi8(static_cast<char>(0xC0));
    const __m128i val_0x80 = _mm_set1_epi8(static_cast<char>(0x80));

    // Process 16 bytes at a time with SSE
    for (; i <= size - 16; i += 16) {
        __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + i));
        __m128i masked = _mm_and_si128(v, mask_0xc0);
        __m128i is_cont = _mm_cmpeq_epi8(masked, val_0x80);
        uint32_t mask_val = static_cast<uint32_t>(_mm_movemask_epi8(is_cont));
        continuations += __builtin_popcount(mask_val);
    }

    // Tail via SWAR
    continuations += count_continuation_bytes_swar(data + i, size - i);
    return continuations;
}

#else // SSE2 fallback

static inline int32_t count_continuation_bytes_simd(const char *data, int32_t size) {
    return count_continuation_bytes_swar(data, size);
}

#endif

#else // Non-x86 fallback

static inline int32_t count_continuation_bytes_simd(const char *data, int32_t size) {
    return count_continuation_bytes_swar(data, size);
}

#endif // __x86_64__

// Batch capacity for deferred processing of non-ASCII non-inlined strings.
// During aux scanning, data is prefetched into L2 (T1 hint) to avoid
// polluting L1 with data that won't be needed until the batch is full.
// During batch processing, T0 prefetch promotes data from L2 into L1
// a few entries ahead of actual use.
static constexpr int BATCH_CAPACITY = 256;

// During batch processing, prefetch this many entries ahead from L2 to L1.
static constexpr int L1_PREFETCH_AHEAD = 4;

struct DeferredRow {
    const char *data;
    int32_t size;
};

// Process a batch of deferred non-ASCII strings whose data is in L2.
// Promotes entries to L1 a few iterations ahead of actual use.
static inline int64_t process_deferred_batch(const DeferredRow *batch, int batchCount) {
    int64_t batchSum = 0;

    // Promote the first few entries to L1 before the loop starts
    for (int p = 0; p < batchCount && p < L1_PREFETCH_AHEAD; p++) {
#if defined(__x86_64__) || defined(_M_X64)
        _mm_prefetch(batch[p].data, _MM_HINT_T0);
        _mm_prefetch(batch[p].data + 64, _MM_HINT_T0);
#elif defined(__aarch64__)
        __builtin_prefetch(batch[p].data, 0, 3);
        __builtin_prefetch(batch[p].data + 64, 0, 3);
#endif
    }

    for (int i = 0; i < batchCount; i++) {
        // Promote a future entry from L2 to L1
        int future = i + L1_PREFETCH_AHEAD;
        if (future < batchCount) {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_prefetch(batch[future].data, _MM_HINT_T0);
            _mm_prefetch(batch[future].data + 64, _MM_HINT_T0);
#elif defined(__aarch64__)
            __builtin_prefetch(batch[future].data, 0, 3);
            __builtin_prefetch(batch[future].data + 64, 0, 3);
#endif
        }
        int32_t continuations = count_continuation_bytes_simd(batch[i].data, batch[i].size);
        batchSum += batch[i].size - continuations;
    }
    return batchSum;
}

void MULTI_VERSION_NAME(varchar_utf8_length_sum)(
        const char *auxAddr,
        const char *dataAddr,
        int64_t rowCount,
        double *outSum,
        int64_t *outCount) {

    int64_t sum = 0;
    int64_t count = 0;

    // Two-pass batch: collect non-ASCII data pointers with prefetches (pass 1),
    // then process them all with warm caches (pass 2).
    DeferredRow batch[BATCH_CAPACITY];
    int batchCount = 0;

    for (int64_t row = 0; row < rowCount; row++) {
        const char *auxEntry = auxAddr + VARCHAR_AUX_WIDTH_BYTES * row;
        int32_t raw;
        memcpy(&raw, auxEntry, sizeof(int32_t));

        if (raw & VARCHAR_HEADER_FLAG_NULL) {
            continue;
        }

        int32_t size;
        if (raw & VARCHAR_HEADER_FLAG_INLINED) {
            size = (raw >> VARCHAR_HEADER_FLAGS_WIDTH) & VARCHAR_INLINED_LENGTH_MASK;
            if (raw & VARCHAR_HEADER_FLAG_ASCII) {
                sum += size;
                count++;
                continue;
            }
            // Inlined non-ASCII: data is in aux entry itself (already in L1).
            // Use SWAR directly — max 9 bytes, not worth AVX-512 setup.
            const char *stringData = auxEntry + VARCHAR_FULLY_INLINED_STRING_OFFSET;
            int32_t continuations = count_continuation_bytes_swar(stringData, size);
            sum += size - continuations;
            count++;
        } else {
            size = (raw >> VARCHAR_HEADER_FLAGS_WIDTH) & VARCHAR_DATA_LENGTH_MASK;
            if (raw & VARCHAR_HEADER_FLAG_ASCII) {
                sum += size;
                count++;
                continue;
            }
            // Non-ASCII, non-inlined: prefetch into L2 and defer
            int64_t dataOffset = get_data_offset(auxEntry);
            const char *stringData = dataAddr + dataOffset;
#if defined(__x86_64__) || defined(_M_X64)
            _mm_prefetch(stringData, _MM_HINT_T1);
            _mm_prefetch(stringData + 64, _MM_HINT_T1);
#elif defined(__aarch64__)
            __builtin_prefetch(stringData, 0, 2);
            __builtin_prefetch(stringData + 64, 0, 2);
#endif
            batch[batchCount].data = stringData;
            batch[batchCount].size = size;
            batchCount++;

            if (batchCount == BATCH_CAPACITY) {
                // Process batch: data is in L2, promoted to L1 just-in-time
                sum += process_deferred_batch(batch, BATCH_CAPACITY);
                count += BATCH_CAPACITY;
                batchCount = 0;
            }
        }
    }

    // Process remaining deferred entries
    sum += process_deferred_batch(batch, batchCount);
    count += batchCount;

    *outSum += static_cast<double>(sum);
    *outCount += count;
}

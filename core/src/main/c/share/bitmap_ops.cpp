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

#include "bitmap_ops.h"
#include <jni.h>

void bitmap_copy_offset(
        const uint8_t *src, int64_t srcBitOff,
        uint8_t *dst, int64_t dstBitOff,
        int64_t bitCount
) {
    if (bitCount <= 0) return;

    // Both offsets are byte-aligned: memcpy
    if ((srcBitOff & 7) == 0 && (dstBitOff & 7) == 0) {
        memcpy(dst + (dstBitOff >> 3), src + (srcBitOff >> 3), (bitCount + 7) >> 3);
        return;
    }

    // Source is byte-aligned, destination is byte-aligned: same as above
    // General case: process with uint64_t word-level operations
    const uint8_t *srcBase = src + (srcBitOff >> 3);
    uint8_t *dstBase = dst + (dstBitOff >> 3);
    int srcShift = (int) (srcBitOff & 7);
    int dstShift = (int) (dstBitOff & 7);

    // If both have the same sub-byte alignment, we can do a shifted word copy
    if (srcShift == dstShift) {
        // Handle partial first byte
        if (dstShift != 0) {
            int bitsInFirstByte = 8 - dstShift;
            if (bitsInFirstByte > bitCount) bitsInFirstByte = (int) bitCount;
            uint8_t mask = (uint8_t) (((1 << bitsInFirstByte) - 1) << dstShift);
            *dstBase = (*dstBase & ~mask) | (*srcBase & mask);
            srcBase++;
            dstBase++;
            bitCount -= bitsInFirstByte;
        }
        // Copy full bytes
        int64_t fullBytes = bitCount >> 3;
        if (fullBytes > 0) {
            memcpy(dstBase, srcBase, fullBytes);
            srcBase += fullBytes;
            dstBase += fullBytes;
        }
        // Handle partial last byte
        int remaining = (int) (bitCount & 7);
        if (remaining > 0) {
            uint8_t mask = (uint8_t) ((1 << remaining) - 1);
            *dstBase = (*dstBase & ~mask) | (*srcBase & mask);
        }
        return;
    }

    // General unaligned case: shift source bits into destination
    for (int64_t i = 0; i < bitCount; i++) {
        int64_t srcBit = srcBitOff + i;
        int64_t dstBit = dstBitOff + i;
        bool val = (src[srcBit >> 3] >> (srcBit & 7)) & 1;
        if (val) {
            dst[dstBit >> 3] |= (1 << (dstBit & 7));
        } else {
            dst[dstBit >> 3] &= ~(1 << (dstBit & 7));
        }
    }
}

void bitmap_set_range(uint8_t *bitmap, int64_t bitOffset, int64_t bitCount) {
    if (bitCount <= 0) return;

    int64_t endBit = bitOffset + bitCount;
    int64_t startByte = bitOffset >> 3;
    int64_t endByte = (endBit + 7) >> 3;
    int startBitInByte = (int) (bitOffset & 7);
    int endBitInByte = (int) (endBit & 7);

    if (startByte == endByte - 1 || (endBitInByte == 0 && startByte == endByte)) {
        // All bits are in one or two bytes
        for (int64_t i = 0; i < bitCount; i++) {
            bitmap_set_bit(bitmap, bitOffset + i);
        }
        return;
    }

    // Handle partial first byte
    if (startBitInByte != 0) {
        bitmap[startByte] |= (uint8_t) (0xFF << startBitInByte);
        startByte++;
    }

    // Handle full bytes with memset
    int64_t fullEndByte = endBitInByte == 0 ? endByte : endByte - 1;
    if (fullEndByte > startByte) {
        memset(bitmap + startByte, 0xFF, fullEndByte - startByte);
    }

    // Handle partial last byte
    if (endBitInByte != 0) {
        bitmap[fullEndByte] |= (uint8_t) ((1 << endBitInByte) - 1);
    }
}

void bitmap_reshuffle(
        const uint8_t *src,
        uint8_t *dst,
        const uint8_t *sortIndex,
        int64_t sortEntryBytes,
        int64_t count
) {
    if (count <= 0) return;

    // Process with uint64_t accumulator for sequential output
    uint64_t accumulator = 0;
    int accBits = 0;
    uint8_t *dstPtr = dst;

    for (int64_t i = 0; i < count; i++) {
        // Read row index from sort entry (second 8 bytes = row index)
        int64_t origRow = *(const int64_t *) (sortIndex + i * sortEntryBytes + 8);

        // Read source bit
        bool isNull = bitmap_get_bit(src, origRow);

        // Accumulate into output
        if (isNull) {
            accumulator |= ((uint64_t) 1 << accBits);
        }
        accBits++;

        // Flush full bytes
        if (accBits == 64) {
            *(uint64_t *) dstPtr = accumulator;
            dstPtr += 8;
            accumulator = 0;
            accBits = 0;
        }
    }

    // Flush remaining bits
    while (accBits > 0) {
        *dstPtr++ = (uint8_t) (accumulator & 0xFF);
        accumulator >>= 8;
        accBits -= 8;
    }
}

void bitmap_merge_two_sources(
        const uint8_t *srcA,
        const uint8_t *srcB, int64_t srcBBitOff,
        uint8_t *dst,
        const uint8_t *mergeIndex,
        int64_t mergeEntryBytes,
        int64_t count
) {
    if (count <= 0) return;

    uint64_t accumulator = 0;
    int accBits = 0;
    uint8_t *dstPtr = dst;

    for (int64_t i = 0; i < count; i++) {
        int64_t indexEntry = *(const int64_t *) (mergeIndex + i * mergeEntryBytes + 8);
        int64_t pick = (int64_t) ((uint64_t) indexEntry >> 63); // 0 = srcA, 1 = srcB
        int64_t row = indexEntry & 0x7FFFFFFFFFFFFFFFLL;

        bool isNull = false;
        if (pick == 1 && srcB != nullptr) {
            isNull = bitmap_get_bit(srcB, srcBBitOff + row);
        } else if (pick == 0 && srcA != nullptr) {
            isNull = bitmap_get_bit(srcA, row);
        }

        if (isNull) {
            accumulator |= ((uint64_t) 1 << accBits);
        }
        accBits++;

        if (accBits == 64) {
            *(uint64_t *) dstPtr = accumulator;
            dstPtr += 8;
            accumulator = 0;
            accBits = 0;
        }
    }

    while (accBits > 0) {
        *dstPtr++ = (uint8_t) (accumulator & 0xFF);
        accumulator >>= 8;
        accBits -= 8;
    }
}

void bitmap_merge_shuffle(
        const uint8_t *srcData,
        const uint8_t *srcO3,
        uint8_t *dst,
        const uint8_t *mergeIndex,
        int64_t mergeEntryBytes,
        int64_t count,
        int64_t srcDataTop
) {
    if (count <= 0) return;

    uint64_t accumulator = 0;
    int accBits = 0;
    uint8_t *dstPtr = dst;

    for (int64_t i = 0; i < count; i++) {
        int64_t indexEntry = *(const int64_t *) (mergeIndex + i * mergeEntryBytes + 8);
        int64_t pick = (int64_t) ((uint64_t) indexEntry >> 63); // 0 = O3, 1 = partition data
        int64_t row = indexEntry & 0x7FFFFFFFFFFFFFFFLL;

        bool isNull = false;
        if (pick == 1) {
            // Partition data: null if within column-top range or marked in source bitmap
            if (row < srcDataTop) {
                isNull = true;
            } else if (srcData != nullptr) {
                isNull = bitmap_get_bit(srcData, row);
            }
        } else if (srcO3 != nullptr) {
            // O3 data
            isNull = bitmap_get_bit(srcO3, row);
        }

        if (isNull) {
            accumulator |= ((uint64_t) 1 << accBits);
        }
        accBits++;

        if (accBits == 64) {
            *(uint64_t *) dstPtr = accumulator;
            dstPtr += 8;
            accumulator = 0;
            accBits = 0;
        }
    }

    while (accBits > 0) {
        *dstPtr++ = (uint8_t) (accumulator & 0xFF);
        accumulator >>= 8;
        accBits -= 8;
    }
}

extern "C" {

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_bitmapCopyOffset(JNIEnv *env, jclass cl,
        jlong src, jlong srcBitOff, jlong dst, jlong dstBitOff, jlong bitCount) {
    bitmap_copy_offset((const uint8_t *) src, srcBitOff, (uint8_t *) dst, dstBitOff, bitCount);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_bitmapSetRange(JNIEnv *env, jclass cl,
        jlong bitmap, jlong bitOffset, jlong bitCount) {
    bitmap_set_range((uint8_t *) bitmap, bitOffset, bitCount);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_bitmapReshuffle(JNIEnv *env, jclass cl,
        jlong src, jlong dst, jlong sortIndex, jint sortEntryBytes, jlong count) {
    bitmap_reshuffle((const uint8_t *) src, (uint8_t *) dst, (const uint8_t *) sortIndex,
                     sortEntryBytes, count);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_bitmapMergeTwoSources(JNIEnv *env, jclass cl,
        jlong srcA, jlong srcB, jlong srcBBitOff, jlong dst,
        jlong mergeIndex, jint mergeEntryBytes, jlong count) {
    bitmap_merge_two_sources((const uint8_t *) srcA, (const uint8_t *) srcB, srcBBitOff,
                             (uint8_t *) dst, (const uint8_t *) mergeIndex, mergeEntryBytes, count);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_bitmapMergeShuffle(JNIEnv *env, jclass cl,
        jlong srcData, jlong srcO3, jlong dst, jlong mergeIndex,
        jint mergeEntryBytes, jlong count, jlong srcDataTop) {
    bitmap_merge_shuffle((const uint8_t *) srcData, (const uint8_t *) srcO3, (uint8_t *) dst,
                         (const uint8_t *) mergeIndex, mergeEntryBytes, count, srcDataTop);
}

} // extern "C"

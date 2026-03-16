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

#ifndef BITMAP_OPS_H
#define BITMAP_OPS_H

#include <cstdint>
#include <cstring>

// Read a single bit from a bitmap at the given bit position.
inline bool bitmap_get_bit(const uint8_t *bitmap, int64_t bitPos) {
    return (bitmap[bitPos >> 3] >> (bitPos & 7)) & 1;
}

// Set a single bit in a bitmap at the given bit position.
inline void bitmap_set_bit(uint8_t *bitmap, int64_t bitPos) {
    bitmap[bitPos >> 3] |= (1 << (bitPos & 7));
}

// Copy bitCount bits from src at srcBitOff to dst at dstBitOff.
// Both src and dst must be pre-allocated with sufficient size.
void bitmap_copy_offset(
        const uint8_t *src, int64_t srcBitOff,
        uint8_t *dst, int64_t dstBitOff,
        int64_t bitCount
);

// Set bitCount bits to 1 starting at bitOffset in bitmap.
void bitmap_set_range(uint8_t *bitmap, int64_t bitOffset, int64_t bitCount);

// Sort bitmap by timestamp sort index. For each output position i,
// reads the source row from sortIndex[i * sortEntryBytes + 8] (low 63 bits)
// and copies that bit from src to dst[i].
// Uses uint64_t accumulator for output: writes once per 64 bits.
void bitmap_reshuffle(
        const uint8_t *src,
        uint8_t *dst,
        const uint8_t *sortIndex,
        int64_t sortEntryBytes,
        int64_t count
);

// Merge two bitmap sources (WAL + lag) by merge index.
// For each position i in mergeIndex:
//   - pick = entry >> 63 (0 = srcA/WAL, 1 = srcB/lag)
//   - row = entry & 0x7FFFFFFFFFFFFFFF
//   - if pick == 1: read bit from srcB at position (srcBBitOff + row)
//   - if pick == 0: read bit from srcA at position (row)
//   - write to dst at position i
void bitmap_merge_two_sources(
        const uint8_t *srcA,
        const uint8_t *srcB, int64_t srcBBitOff,
        uint8_t *dst,
        const uint8_t *mergeIndex,
        int64_t mergeEntryBytes,
        int64_t count
);

// Merge bitmap by index with two sources (partition data + O3) and column-top.
// For each position i in mergeIndex:
//   - pick = entry >> 63 (0 = O3, 1 = partition data)
//   - row = entry & 0x7FFFFFFFFFFFFFFF
//   - if pick == 1: null if row < srcDataTop, else read from srcData bitmap
//   - if pick == 0: read from srcO3 bitmap
//   - write to dst at position i
void bitmap_merge_shuffle(
        const uint8_t *srcData,
        const uint8_t *srcO3,
        uint8_t *dst,
        const uint8_t *mergeIndex,
        int64_t mergeEntryBytes,
        int64_t count,
        int64_t srcDataTop
);

#endif //BITMAP_OPS_H

/*+*****************************************************************************
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

// JNI bridge to cwida/fsst (https://github.com/cwida/fsst).

#include "fsst.h"
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <jni.h>

extern "C" {

JNIEXPORT jint JNICALL
Java_io_questdb_cairo_idx_FSSTNative_decoderStructSize0(JNIEnv *, jclass) {
    return static_cast<jint>(sizeof(fsst_decoder_t));
}

JNIEXPORT jint JNICALL
Java_io_questdb_cairo_idx_FSSTNative_importTable0(
        JNIEnv *, jclass, jlong decoderAddr, jlong srcAddr) {
    if (decoderAddr == 0 || srcAddr == 0) return -1;
    auto *dec = reinterpret_cast<fsst_decoder_t *>(decoderAddr);
    unsigned int consumed = fsst_import(
            dec, reinterpret_cast<const unsigned char *>(srcAddr));
    return consumed == 0 ? -1 : static_cast<jint>(consumed);
}

// Train + compress + export in a single call; encoder lifecycle stays inside JNI.
// Returns -1 on failure, else: bits 48..63 = tableLen, bits 0..47 = totalCompressed.
// The 48-bit compressed span caps a single block at 256 TB — orders of magnitude
// beyond any realistic sidecar stride.
//
// batchScratchAddr points to a Java-allocated scratch buffer of at least count*32
// bytes, laid out as four count-sized sub-arrays: lensIn, ptrsIn, lensOut, ptrsOut.
// Keeping the scratch Java-owned gives it a MemoryTag, avoids per-thread leaks on
// worker exit, and sidesteps realloc partial-failure hazards.
JNIEXPORT jlong JNICALL
Java_io_questdb_cairo_idx_FSSTNative_trainAndCompressBlock0(
        JNIEnv *, jclass,
        jlong srcAddr, jlong srcOffsetsAddr, jint count,
        jlong cmpAddr, jlong cmpCap, jlong cmpOffsetsAddr,
        jlong tableAddr, jlong batchScratchAddr) {
    if (count <= 0 || cmpCap <= 0) return -1;
    if (srcAddr == 0 || srcOffsetsAddr == 0 || cmpAddr == 0 || cmpOffsetsAddr == 0
        || tableAddr == 0 || batchScratchAddr == 0) return -1;

    const int64_t segStride = static_cast<int64_t>(count) * 8;
    auto *lensIn = reinterpret_cast<size_t *>(batchScratchAddr);
    auto *ptrsIn = reinterpret_cast<const unsigned char **>(batchScratchAddr + segStride);
    auto *lensOut = reinterpret_cast<size_t *>(batchScratchAddr + segStride * 2);
    auto *ptrsOut = reinterpret_cast<unsigned char **>(batchScratchAddr + segStride * 3);

    auto *srcBase = reinterpret_cast<const unsigned char *>(srcAddr);
    auto *srcOffs = reinterpret_cast<const int64_t *>(srcOffsetsAddr);
    for (jint i = 0; i < count; i++) {
        int64_t lo = srcOffs[i];
        int64_t hi = srcOffs[i + 1];
        // Reject non-monotonic offsets. Without this, hi < lo wraps to a
        // ~2^64 length when cast to size_t and fsst_compress reads
        // arbitrary memory. lo < 0 is similarly invalid.
        if (lo < 0 || hi < lo) {
            return -1;
        }
        ptrsIn[i] = srcBase + lo;
        lensIn[i] = static_cast<size_t>(hi - lo);
    }

    fsst_encoder_t *enc = fsst_create(
            static_cast<size_t>(count), lensIn, ptrsIn, /*zeroTerminated=*/0);
    if (!enc) return -1;

    size_t produced = fsst_compress(
            enc,
            static_cast<size_t>(count),
            lensIn, ptrsIn,
            static_cast<size_t>(cmpCap),
            reinterpret_cast<unsigned char *>(cmpAddr),
            lensOut, ptrsOut);
    if (produced != static_cast<size_t>(count)) {
        fsst_destroy(enc);
        return -1;
    }

    auto *dstBase = reinterpret_cast<unsigned char *>(cmpAddr);
    auto *dstOffs = reinterpret_cast<int64_t *>(cmpOffsetsAddr);
    int64_t totalOut = 0;
    for (jint i = 0; i < count; i++) {
        int64_t off = static_cast<int64_t>(ptrsOut[i] - dstBase);
        dstOffs[i] = off;
        totalOut = off + static_cast<int64_t>(lensOut[i]);
    }
    dstOffs[count] = totalOut;

    unsigned int tableLen = fsst_export(enc, reinterpret_cast<unsigned char *>(tableAddr));
    fsst_destroy(enc);

    static constexpr int64_t TOTAL_OUT_MAX = (static_cast<int64_t>(1) << 48) - 1;
    if (tableLen == 0 || totalOut > TOTAL_OUT_MAX) {
        return -1;
    }
    return (static_cast<int64_t>(tableLen) << 48) | totalOut;
}

// Per-value decode in a tight loop (fsst_decompress is inlined from fsst.h).
// Lets the caller index any ordinal in O(1) without needing a self-describing
// length prefix on the decoded value (VARCHAR does not have one).
//
// dstCap and the return value are jlong: a single stride's decompressed output
// can exceed 2GB (the sidecar compressed block is bounded only by the 48-bit
// packing cap and may use long offsets; FSST worst-case expansion is 8x).
JNIEXPORT jlong JNICALL
Java_io_questdb_cairo_idx_FSSTNative_decompressBlock0(
        JNIEnv *, jclass,
        jlong decoderHandle,
        jlong srcAddr, jlong srcOffsetsAddr, jint srcOffsetsWidth, jint count,
        jlong dstAddr, jlong dstCap,
        jlong dstOffsetsAddr) {
    if (decoderHandle == 0 || count <= 0 || dstCap <= 0) return -1;
    if (srcAddr == 0 || srcOffsetsAddr == 0 || dstAddr == 0 || dstOffsetsAddr == 0) return -1;
    if (srcOffsetsWidth != 4 && srcOffsetsWidth != 8) return -1;

    auto *dec = reinterpret_cast<const fsst_decoder_t *>(decoderHandle);
    auto *srcBase = reinterpret_cast<const unsigned char *>(srcAddr);
    auto *dstBase = reinterpret_cast<unsigned char *>(dstAddr);
    auto *dstOffs = reinterpret_cast<int64_t *>(dstOffsetsAddr);
    auto *srcOffs8 = reinterpret_cast<const int64_t *>(srcOffsetsAddr);
    auto *srcOffs4 = reinterpret_cast<const uint32_t *>(srcOffsetsAddr);
    bool wide = (srcOffsetsWidth == 8);

    int64_t pos = 0;
    int64_t cap = static_cast<int64_t>(dstCap);
    int64_t prevHi = wide ? srcOffs8[0] : static_cast<int64_t>(srcOffs4[0]);
    if (prevHi < 0) return -1;
    for (jint i = 0; i < count; i++) {
        int64_t srcLo = prevHi;
        int64_t srcHi = wide ? srcOffs8[i + 1] : static_cast<int64_t>(srcOffs4[i + 1]);
        // Reject non-monotonic offsets. Without this, srcHi < srcLo wraps
        // to a ~2^64 length when cast to size_t and fsst_decompress reads
        // arbitrary memory.
        if (srcHi < srcLo) {
            return -1;
        }
        prevHi = srcHi;
        dstOffs[i] = pos;
        size_t srcLen = static_cast<size_t>(srcHi - srcLo);
        if (srcLen == 0) continue;
        size_t remaining = static_cast<size_t>(cap - pos);
        size_t out = fsst_decompress(
                dec, srcLen, srcBase + srcLo,
                remaining, dstBase + pos);
        // fsst_decompress returns the FULL decompressed length, even when
        // it truncated the output to fit. {@code out > remaining} therefore
        // correctly signals truncation; the caller doubles dstCap and
        // retries (see AbstractPostingIndexReader.decompressBlock loop).
        if (out > remaining) {
            return -1;
        }
        pos += static_cast<int64_t>(out);
    }
    dstOffs[count] = pos;
    return pos;
}

}  // extern "C"

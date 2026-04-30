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

package io.questdb.cairo.idx;

import io.questdb.std.Os;

public final class FSSTNative {

    /**
     * Per-value bytes the caller must provision in {@code batchScratchAddr} for
     * {@link #trainAndCompressBlock}: four 8-byte slots (lensIn, ptrsIn, lensOut, ptrsOut).
     */
    public static final int BATCH_SCRATCH_BYTES_PER_VALUE = 32;
    public static final int DECODER_STRUCT_SIZE;
    /**
     * High bit on the var-block count header marks the block as FSST-compressed.
     */
    public static final int FSST_BLOCK_FLAG = 0x80000000;
    /**
     * Mirrors cwida's {@code FSST_MAXHEADER}; minimum buffer for the table out param of {@link #trainAndCompressBlock}.
     */
    public static final int MAX_HEADER_SIZE = 2066;

    /**
     * @param srcOffsetsWidth 4 for uint32 offsets, 8 for int64 offsets
     * @return total decompressed bytes, or -1 on truncation / failure
     */
    public static long decompressBlock(
            long decoderAddr,
            long srcAddr, long srcOffsetsAddr, int srcOffsetsWidth, int count,
            long dstAddr, long dstCap,
            long dstOffsetsAddr
    ) {
        return decompressBlock0(decoderAddr, srcAddr, srcOffsetsAddr, srcOffsetsWidth, count, dstAddr, dstCap, dstOffsetsAddr);
    }

    public static int importTable(long decoderAddr, long srcAddr) {
        return importTable0(decoderAddr, srcAddr);
    }

    /**
     * Train + compress + export in a single call. Returns -1 on failure, else
     * a packed long: bits 0..47 = total compressed bytes, bits 48..63 = tableLen.
     * Use {@link #unpackCompressed} / {@link #unpackTableLen} to decode.
     *
     * @param batchScratchAddr native buffer of at least count*32 bytes, used by the
     *                         JNI layer to build cwida's per-string len/ptr arrays
     *                         (see {@link #BATCH_SCRATCH_BYTES_PER_VALUE})
     */
    public static long trainAndCompressBlock(
            long srcAddr, long srcOffsetsAddr, int count,
            long cmpAddr, long cmpCap, long cmpOffsetsAddr,
            long tableAddr, long batchScratchAddr
    ) {
        return trainAndCompressBlock0(srcAddr, srcOffsetsAddr, count, cmpAddr, cmpCap, cmpOffsetsAddr, tableAddr, batchScratchAddr);
    }

    public static long unpackCompressed(long packed) {
        return packed & ((1L << 48) - 1);
    }

    public static int unpackTableLen(long packed) {
        return (int) ((packed >>> 48) & 0xFFFF);
    }

    private static native int decoderStructSize0();

    private static native long decompressBlock0(
            long decoderAddr,
            long srcAddr, long srcOffsetsAddr, int srcOffsetsWidth, int count,
            long dstAddr, long dstCap,
            long dstOffsetsAddr
    );

    private static native int importTable0(long decoderAddr, long srcAddr);

    private static native long trainAndCompressBlock0(
            long srcAddr, long srcOffsetsAddr, int count,
            long cmpAddr, long cmpCap, long cmpOffsetsAddr,
            long tableAddr, long batchScratchAddr
    );

    static {
        Os.init();
        DECODER_STRUCT_SIZE = decoderStructSize0();
    }
}

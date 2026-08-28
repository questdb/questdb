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
     * Encode one batch of strings with the given encoder. Returns the
     * number of strings encoded (&lt;= {@code count}), or -1 on bad
     * arguments. When the return is less than {@code count}, the output
     * buffer ran out; the caller resumes at index {@code produced} for
     * the next batch.
     * <p>
     * The encoder's symbol table is reused across calls -- one train
     * (via {@link #createEncoder}) then many compressBatch calls with
     * bounded per-batch scratch (anonymous heap stays tiny regardless
     * of total input size).
     *
     * @param batchScratchAddr four count-sized slots [lensIn, ptrsIn, lensOut, ptrsOut].
     *                         JNI fills lensIn/ptrsIn from srcAddr+srcOffsets, calls
     *                         fsst_compress, then reads lensOut/ptrsOut to populate
     *                         cmpOffsetsAddr.
     */
    public static long compressBatch(
            long encoderHandle, int count,
            long srcAddr, long srcOffsetsAddr,
            long outCap, long outAddr,
            long cmpOffsetsAddr,
            long batchScratchAddr
    ) {
        return compressBatch0(encoderHandle, count, srcAddr, srcOffsetsAddr,
                outCap, outAddr, cmpOffsetsAddr, batchScratchAddr);
    }

    /**
     * Train an FSST encoder from a sample of strings. Returns 0 on
     * failure, else an opaque handle. cwida documents "at least 16 KB
     * of data" as the recommended sample size; smaller samples still
     * work, just produce a less efficient symbol table.
     * <p>
     * The caller passes whatever sample they like (a contiguous prefix,
     * stride-sampled rows, etc.). The returned encoder can then be
     * reused to compress the full input in batches via {@link #compressBatch},
     * and the symbol table extracted with {@link #exportEncoder}. Free
     * with {@link #destroyEncoder}.
     */
    public static long createEncoder(int sampleCount, long sampleLensAddr, long samplePtrsAddr) {
        return createEncoder0(sampleCount, sampleLensAddr, samplePtrsAddr);
    }

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

    public static void destroyEncoder(long encoderHandle) {
        destroyEncoder0(encoderHandle);
    }

    /**
     * Export the encoder's symbol table to the caller-provided buffer.
     * {@code tableAddr} must be at least {@link #MAX_HEADER_SIZE} bytes.
     * Returns the actual table length (0 on failure).
     */
    public static int exportEncoder(long encoderHandle, long tableAddr) {
        return exportEncoder0(encoderHandle, tableAddr);
    }

    public static int importTable(long decoderAddr, long srcAddr) {
        return importTable0(decoderAddr, srcAddr);
    }

    /**
     * Train + compress + export in a single call. Returns -1 on failure, else
     * a packed long: bits 0..47 = total compressed bytes, bits 48..63 = tableLen.
     * Use {@link #unpackCompressed} to decode.
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

    private static native long compressBatch0(
            long encoderHandle, int count,
            long srcAddr, long srcOffsetsAddr,
            long outCap, long outAddr,
            long cmpOffsetsAddr,
            long batchScratchAddr
    );

    private static native long createEncoder0(int sampleCount, long sampleLensAddr, long samplePtrsAddr);

    private static native int decoderStructSize0();

    private static native long decompressBlock0(
            long decoderAddr,
            long srcAddr, long srcOffsetsAddr, int srcOffsetsWidth, int count,
            long dstAddr, long dstCap,
            long dstOffsetsAddr
    );

    private static native void destroyEncoder0(long encoderHandle);

    private static native int exportEncoder0(long encoderHandle, long tableAddr);

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

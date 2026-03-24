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

package io.questdb.cairo.idx;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Constants and encoding/decoding utilities for the Posting index sealed format.
 *
 * <h2>Sealed stride encoding</h2>
 *
 * At seal time the writer groups keys into strides of {@link #DENSE_STRIDE} (256)
 * consecutive keys and independently encodes each stride using the smaller of two
 * modes:
 *
 * <h3>Delta mode ({@link #STRIDE_MODE_DELTA}, 0x00) — per-key delta + FoR</h3>
 *
 * Each key's sorted row-ID list is encoded independently:
 * <ol>
 *   <li>Delta-encode: compute successive differences.</li>
 *   <li>Split deltas into blocks of {@link #BLOCK_CAPACITY} (64).</li>
 *   <li>Per block: subtract min-delta (Frame of Reference), bitpack the residuals.</li>
 * </ol>
 * This exploits per-key regularity. Round-robin or periodic distributions produce
 * constant deltas (bitWidth=0), so delta mode compresses them to near zero.
 * The cost is per-key metadata: ~20 B of block headers per key regardless of how
 * few values the key holds.
 * <p>
 * <b>Wins when values/key is high</b> (≥ ~10) — the per-key overhead is amortised
 * and tight per-key delta distributions keep bitwidths low.
 *
 * <h3>Flat mode ({@link #STRIDE_MODE_FLAT}, 0x01) — stride-wide FoR</h3>
 *
 * All values across all 256 keys in the stride are packed into a single contiguous
 * bitpacked array with one shared base value and one bitwidth:
 * <pre>
 *   packed_value[i] = (row_id[i] - baseValue)   // bitWidth bits each
 * </pre>
 * A prefix-count array maps each key to its slice of the flat array, enabling O(1)
 * random access per key. No delta encoding step — just raw FoR.
 * <p>
 * <b>Wins when values/key is low</b> (≤ ~8) — avoids the per-key metadata overhead
 * that dominates delta mode at low cardinality. The bitwidth is determined by the
 * full row-ID range within the stride, which is typically wider than per-key delta
 * ranges, so each value takes more bits. But eliminating per-key headers more than
 * compensates.
 * <p>
 * When a flat-mode bitwidth happens to land on 8, 16, or 32, the reader decodes via
 * native AVX2 ({@link BitpackUtils#unpackValuesFrom}), which provides ~5–10× higher
 * throughput than the Java scalar path. This is opportunistic — the writer never
 * inflates the bitwidth to reach an aligned boundary.
 *
 * <h3>Mode selection</h3>
 *
 * The seal path trial-encodes both modes and picks whichever produces fewer bytes.
 * The threshold is purely size-based; there is no speed-vs-size trade-off knob.
 *
 * <h2>Per-key delta-encoded data layout</h2>
 * <pre>
 * blockCount           : 4B
 * valueCounts[]        : blockCount × 1B  (1–64 per block)
 * firstValues[]        : blockCount × 8B  (first absolute value per block)
 * minDeltas[]          : blockCount × 8B  (FoR reference per block)
 * bitWidths[]          : blockCount × 1B
 * packedBlock[0..n-1]  : variable size bitpacked residuals
 * </pre>
 *
 * <h2>Key file layout (.pk) — double-buffered 4 KB metadata pages</h2>
 * <pre>
 * Page A: [0, 4096)    Page B: [4096, 8192)
 *
 * Each page:
 *   [0-7]       sequence_start (8B)
 *   [8-15]      valueMemSize (8B)
 *   [16-19]     blockCapacity (4B)
 *   [20-23]     keyCount (4B)
 *   [24-31]     maxValue (8B)
 *   [32-35]     genCount (4B)
 *   [36-39]     formatVersion (4B)
 *   [40-63]     reserved (24B)
 *   [64-4087]   gen dir: up to 125 entries × 32B
 *   [4088-4095] sequence_end (8B) — must equal sequence_start for valid page
 * </pre>
 *
 * <h2>Dense generation (sealed) — stride-indexed</h2>
 * <pre>
 * [stride_index: (strideCount + 1) × 4B — byte offset per stride block]
 * [stride block 0]   (delta or flat mode, chosen per stride)
 * [stride block 1]
 * ...
 * </pre>
 *
 * <h3>Delta mode stride layout</h3>
 * <pre>
 *   [mode: 1B = 0x00]
 *   [reserved: 1B]
 *   [padding: 2B]
 *   [counts:  ks × 4B — value count per key]
 *   [offsets: (ks + 1) × 4B — prefix-sum data offsets, sentinel at end]
 *   [delta-encoded data for each key]
 * </pre>
 *
 * <h3>Flat mode stride layout</h3>
 * <pre>
 *   [mode: 1B = 0x01]
 *   [bitWidth: 1B]
 *   [padding: 2B]
 *   [baseValue: 8B]
 *   [prefixCounts: (ks + 1) × 4B — cumulative value count]
 *   [packed data: totalValues × bitWidth bits, contiguous]
 * </pre>
 *
 * <h2>Sparse generation (commit-time)</h2>
 * <pre>
 * [keyIds:  activeKeyCount × 4B — sorted ascending]
 * [counts:  activeKeyCount × 4B]
 * [offsets: activeKeyCount × 4B]
 * [delta-encoded data per key]
 * </pre>
 */
public final class PostingIndexUtils {

    public static final int BLOCK_CAPACITY = 64;
    public static final int COVER_INFO_MAGIC = 0x50434930; // "PCI0"
    public static final int DENSE_STRIDE = 256;
    public static final int PACKED_BATCH_SIZE = 256;

    // Stride block mode constants — see class javadoc for when each mode wins
    public static final byte STRIDE_MODE_DELTA = 0;
    public static final byte STRIDE_MODE_FLAT = 1;
    public static final int STRIDE_MODE_PREFIX_SIZE = 4; // mode(1B) + bitWidth/reserved(1B) + padding(2B)
    public static final int STRIDE_FLAT_BASE_OFFSET = STRIDE_MODE_PREFIX_SIZE; // baseValue(8B) follows mode prefix
    public static final int STRIDE_FLAT_PREFIX_COUNTS_OFFSET = STRIDE_MODE_PREFIX_SIZE + Long.BYTES; // = 12

    // Double-buffered 4KB metadata pages (v2 format)
    public static final int PAGE_SIZE = 4096;
    public static final long PAGE_A_OFFSET = 0;
    public static final long PAGE_B_OFFSET = 4096;
    public static final int KEY_FILE_RESERVED = 8192; // was 64

    // Per-page offsets
    public static final int PAGE_OFFSET_SEQUENCE_START = 0;
    public static final int PAGE_OFFSET_VALUE_MEM_SIZE = 8;
    public static final int PAGE_OFFSET_BLOCK_CAPACITY = 16;
    public static final int PAGE_OFFSET_KEY_COUNT = 20;
    public static final int PAGE_OFFSET_MAX_VALUE = 24;
    public static final int PAGE_OFFSET_GEN_COUNT = 32;
    public static final int PAGE_OFFSET_FORMAT_VERSION = 36;
    public static final int PAGE_OFFSET_GEN_DIR = 64;
    public static final int PAGE_OFFSET_SEQUENCE_END = 4088;

    public static final int MAX_GEN_COUNT = 125; // (4088-64)/32 = 125
    public static final int FORMAT_VERSION = 1;

    public static final byte SIGNATURE = (byte) 0xfb;

    // Generation directory entry (32 bytes per generation, 8-byte aligned)
    public static final int GEN_DIR_ENTRY_SIZE = 32;
    public static final int GEN_DIR_OFFSET_FILE_OFFSET = 0;
    public static final int GEN_DIR_OFFSET_SIZE = 8;
    public static final int GEN_DIR_OFFSET_KEY_COUNT = 16;
    public static final int GEN_DIR_OFFSET_MIN_KEY = 20;
    public static final int GEN_DIR_OFFSET_MAX_KEY = 24;

    private PostingIndexUtils() {
    }

    /**
     * Computes worst-case encoded size for a key with {@code count} values.
     * Used to pre-allocate encode buffers.
     */
    public static long computeMaxEncodedSize(int count) {
        int blockCount = (count + BLOCK_CAPACITY - 1) / BLOCK_CAPACITY;
        // 4B blockCount + blockCount * (1B valueCount + 8B firstValue + 8B minDelta + 1B bitWidth)
        // + (count - blockCount) * 8 bytes worst case packed data
        // Each block's first value is in firstValues[], so total deltas = count - blockCount.
        long totalDeltas = count - blockCount;
        return 4 + (long) blockCount * 18 + totalDeltas * 8;
    }

    /**
     * Decodes all values for a key from delta-encoded data.
     * Allocates temporary arrays per call — use the overload with DecodeContext
     * on hot paths to avoid allocations.
     *
     * @param srcAddr    address of the encoded data for this key
     * @param totalCount total number of values expected
     * @param dest       destination array (must have room for totalCount values)
     */
    public static void decodeKey(long srcAddr, int totalCount, long[] dest) {
        int blockCount = Unsafe.getUnsafe().getInt(srcAddr);
        long pos = srcAddr + 4;

        // Read valueCounts[]
        int[] valueCounts = new int[blockCount];
        for (int b = 0; b < blockCount; b++) {
            valueCounts[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
        }
        pos += blockCount;

        long firstValuesAddr = 0;
        long minDeltasAddr = 0;
        long blockDeltasAddr = 0;
        try {
            // Read firstValues[] (off-heap)
            firstValuesAddr = Unsafe.malloc((long) blockCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            for (int b = 0; b < blockCount; b++) {
                Unsafe.getUnsafe().putLong(firstValuesAddr + (long) b * Long.BYTES,
                        Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES));
            }
            pos += (long) blockCount * Long.BYTES;

            // Read minDeltas[] (off-heap)
            minDeltasAddr = Unsafe.malloc((long) blockCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            for (int b = 0; b < blockCount; b++) {
                Unsafe.getUnsafe().putLong(minDeltasAddr + (long) b * Long.BYTES,
                        Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES));
            }
            pos += (long) blockCount * Long.BYTES;

            // Read bitWidths[]
            int[] bitWidths = new int[blockCount];
            for (int b = 0; b < blockCount; b++) {
                bitWidths[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
            }
            pos += blockCount;

            // Decode each block — only count-1 deltas are packed (first value is in firstValues[])
            int destIdx = 0;
            blockDeltasAddr = Unsafe.malloc((long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            for (int b = 0; b < blockCount; b++) {
                int count = valueCounts[b];
                int bitWidth = bitWidths[b];
                int numDeltas = count - 1;

                if (numDeltas > 0) {
                    long minD = Unsafe.getUnsafe().getLong(minDeltasAddr + (long) b * Long.BYTES);
                    if (bitWidth == 0) {
                        for (int i = 0; i < numDeltas; i++) {
                            Unsafe.getUnsafe().putLong(blockDeltasAddr + (long) i * Long.BYTES, minD);
                        }
                    } else {
                        BitpackUtils.unpackAllValues(pos, numDeltas, bitWidth, minD, blockDeltasAddr);
                    }
                }
                pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);

                // Cumulative sum from firstValue to reconstruct absolute values
                long cumulative = Unsafe.getUnsafe().getLong(firstValuesAddr + (long) b * Long.BYTES);
                dest[destIdx++] = cumulative;
                for (int i = 0; i < numDeltas; i++) {
                    cumulative += Unsafe.getUnsafe().getLong(blockDeltasAddr + (long) i * Long.BYTES);
                    dest[destIdx++] = cumulative;
                }
            }
        } finally {
            if (firstValuesAddr != 0) {
                Unsafe.free(firstValuesAddr, (long) blockCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            if (minDeltasAddr != 0) {
                Unsafe.free(minDeltasAddr, (long) blockCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            if (blockDeltasAddr != 0) {
                Unsafe.free(blockDeltasAddr, (long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
        }
    }

    /**
     * Decodes all values for a key from delta-encoded data into a Java array,
     * using pre-allocated workspace arrays from the provided context.
     *
     * @param srcAddr    address of the encoded data for this key
     * @param totalCount total number of values expected
     * @param dest       destination array (must have room for totalCount values)
     * @param ctx        reusable decode context (call ensureCapacity first)
     */
    public static void decodeKey(long srcAddr, int totalCount, long[] dest, DecodeContext ctx) {
        int blockCount = Unsafe.getUnsafe().getInt(srcAddr);
        long pos = srcAddr + 4;

        ctx.ensureCapacity(blockCount);
        int[] valueCounts = ctx.valueCounts;
        long firstValuesAddr = ctx.firstValuesAddr;
        long minDeltasAddr = ctx.minDeltasAddr;
        int[] bitWidths = ctx.bitWidths;
        long blockDeltasAddr = ctx.blockDeltasAddr;

        for (int b = 0; b < blockCount; b++) {
            valueCounts[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
        }
        pos += blockCount;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putLong(firstValuesAddr + (long) b * Long.BYTES,
                    Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putLong(minDeltasAddr + (long) b * Long.BYTES,
                    Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        for (int b = 0; b < blockCount; b++) {
            bitWidths[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
        }
        pos += blockCount;

        int destIdx = 0;
        for (int b = 0; b < blockCount; b++) {
            int count = valueCounts[b];
            int bitWidth = bitWidths[b];
            int numDeltas = count - 1;

            if (numDeltas > 0) {
                long minD = Unsafe.getUnsafe().getLong(minDeltasAddr + (long) b * Long.BYTES);
                if (bitWidth == 0) {
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.getUnsafe().putLong(blockDeltasAddr + (long) i * Long.BYTES, minD);
                    }
                } else {
                    BitpackUtils.unpackAllValues(pos, numDeltas, bitWidth, minD, blockDeltasAddr);
                }
            }
            pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);

            long cumulative = Unsafe.getUnsafe().getLong(firstValuesAddr + (long) b * Long.BYTES);
            dest[destIdx++] = cumulative;
            for (int i = 0; i < numDeltas; i++) {
                cumulative += Unsafe.getUnsafe().getLong(blockDeltasAddr + (long) i * Long.BYTES);
                dest[destIdx++] = cumulative;
            }
        }
    }

    /**
     * Decodes all values for a key from delta-encoded data directly into native memory,
     * using pre-allocated workspace arrays from the provided context.
     *
     * @param srcAddr    address of the encoded data for this key
     * @param totalCount total number of values expected
     * @param destAddr   native memory destination address (must have room for totalCount longs)
     * @param ctx        reusable decode context (call ensureCapacity first)
     */
    public static void decodeKeyToNative(long srcAddr, int totalCount, long destAddr, DecodeContext ctx) {
        int blockCount = Unsafe.getUnsafe().getInt(srcAddr);
        long pos = srcAddr + 4;

        ctx.ensureCapacity(blockCount);
        int[] valueCounts = ctx.valueCounts;
        long firstValuesAddr = ctx.firstValuesAddr;
        long minDeltasAddr = ctx.minDeltasAddr;
        int[] bitWidths = ctx.bitWidths;
        long blockDeltasAddr = ctx.blockDeltasAddr;

        for (int b = 0; b < blockCount; b++) {
            valueCounts[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
        }
        pos += blockCount;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putLong(firstValuesAddr + (long) b * Long.BYTES,
                    Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putLong(minDeltasAddr + (long) b * Long.BYTES,
                    Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        for (int b = 0; b < blockCount; b++) {
            bitWidths[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
        }
        pos += blockCount;

        int destIdx = 0;
        for (int b = 0; b < blockCount; b++) {
            int count = valueCounts[b];
            int bitWidth = bitWidths[b];
            int numDeltas = count - 1;

            if (numDeltas > 0) {
                long minD = Unsafe.getUnsafe().getLong(minDeltasAddr + (long) b * Long.BYTES);
                if (bitWidth == 0) {
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.getUnsafe().putLong(blockDeltasAddr + (long) i * Long.BYTES, minD);
                    }
                } else {
                    BitpackUtils.unpackAllValues(pos, numDeltas, bitWidth, minD, blockDeltasAddr);
                }
            }
            pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);

            long cumulative = Unsafe.getUnsafe().getLong(firstValuesAddr + (long) b * Long.BYTES);
            Unsafe.getUnsafe().putLong(destAddr + (long) destIdx * Long.BYTES, cumulative);
            destIdx++;
            for (int i = 0; i < numDeltas; i++) {
                cumulative += Unsafe.getUnsafe().getLong(blockDeltasAddr + (long) i * Long.BYTES);
                Unsafe.getUnsafe().putLong(destAddr + (long) destIdx * Long.BYTES, cumulative);
                destIdx++;
            }
        }
    }

    /**
     * Encodes sorted values for a single key using delta + FoR64 bitpacking.
     * Allocates temporary arrays internally — use the overload with EncodeContext
     * on hot paths to avoid allocations.
     */
    public static int encodeKey(long[] values, int count, long destAddr) {
        EncodeContext ctx = new EncodeContext();
        try {
            ctx.ensureCapacity(count);
            return encodeKey(values, count, destAddr, ctx);
        } finally {
            ctx.close();
        }
    }

    /**
     * Encodes sorted values for a single key using delta + FoR64 bitpacking.
     * Uses pre-allocated workspace arrays from the provided context.
     *
     * @param values   array of sorted values
     * @param count    number of values
     * @param destAddr destination memory address
     * @param ctx      reusable encode context (call ensureCapacity first)
     * @return number of bytes written
     */
    public static int encodeKey(long[] values, int count, long destAddr, EncodeContext ctx) {
        if (count == 0) {
            Unsafe.getUnsafe().putInt(destAddr, 0);
            return 4;
        }

        int blockCount = (count + BLOCK_CAPACITY - 1) / BLOCK_CAPACITY;
        long deltasAddr = ctx.deltasAddr;
        int[] valueCounts = ctx.blockValueCounts;
        long blockFirstValuesAddr = ctx.blockFirstValuesAddr;
        long blockMinDeltasAddr = ctx.blockMinDeltasAddr;
        int[] bitWidths = ctx.blockBitWidths;
        long residualsAddr = ctx.residualsAddr;

        // Compute deltas
        Unsafe.getUnsafe().putLong(deltasAddr, values[0]);
        for (int i = 1; i < count; i++) {
            Unsafe.getUnsafe().putLong(deltasAddr + (long) i * Long.BYTES, values[i] - values[i - 1]);
        }

        // Per-block metadata
        for (int b = 0; b < blockCount; b++) {
            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;

            valueCounts[b] = blockSize;
            Unsafe.getUnsafe().putLong(blockFirstValuesAddr + (long) b * Long.BYTES, values[blockStart]);

            // Compute min/max delta for deltas[blockStart+1..blockEnd-1] only.
            // delta[blockStart] is redundant — firstValues[b] already stores the absolute value.
            int numDeltas = blockSize - 1;
            long minD, maxD;
            if (numDeltas == 0) {
                minD = 0;
                maxD = 0;
            } else {
                minD = Unsafe.getUnsafe().getLong(deltasAddr + (long) (blockStart + 1) * Long.BYTES);
                maxD = minD;
                for (int i = blockStart + 2; i < blockEnd; i++) {
                    long d = Unsafe.getUnsafe().getLong(deltasAddr + (long) i * Long.BYTES);
                    if (d < minD) minD = d;
                    if (d > maxD) maxD = d;
                }
            }

            Unsafe.getUnsafe().putLong(blockMinDeltasAddr + (long) b * Long.BYTES, minD);
            long range = maxD - minD;
            bitWidths[b] = range == 0 ? 0 : BitpackUtils.bitsNeeded(range);
        }

        // Write encoded data
        long pos = destAddr;

        // blockCount (4B)
        Unsafe.getUnsafe().putInt(pos, blockCount);
        pos += 4;

        // valueCounts[] (blockCount × 1B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putByte(pos + b, (byte) valueCounts[b]);
        }
        pos += blockCount;

        // firstValues[] (blockCount × 8B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putLong(pos + (long) b * Long.BYTES,
                    Unsafe.getUnsafe().getLong(blockFirstValuesAddr + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        // minDeltas[] (blockCount × 8B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putLong(pos + (long) b * Long.BYTES,
                    Unsafe.getUnsafe().getLong(blockMinDeltasAddr + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        // bitWidths[] (blockCount × 1B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putByte(pos + b, (byte) bitWidths[b]);
        }
        pos += blockCount;

        // Packed blocks — only pack the numDeltas=blockSize-1 inter-value deltas
        for (int b = 0; b < blockCount; b++) {
            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;
            int numDeltas = blockSize - 1;
            int bitWidth = bitWidths[b];

            if (bitWidth > 0 && numDeltas > 0) {
                long minD = Unsafe.getUnsafe().getLong(blockMinDeltasAddr + (long) b * Long.BYTES);
                if (ctx.nativeResidualsAddr != 0) {
                    long nrAddr = ctx.nativeResidualsAddr;
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.getUnsafe().putLong(nrAddr + (long) i * Long.BYTES,
                                Unsafe.getUnsafe().getLong(deltasAddr + (long) (blockStart + 1 + i) * Long.BYTES) - minD);
                    }
                    PostingIndexNative.packValuesNative(nrAddr, numDeltas, 0, bitWidth, pos);
                } else {
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.getUnsafe().putLong(residualsAddr + (long) i * Long.BYTES,
                                Unsafe.getUnsafe().getLong(deltasAddr + (long) (blockStart + 1 + i) * Long.BYTES) - minD);
                    }
                    BitpackUtils.packValues(residualsAddr, numDeltas, 0, bitWidth, pos);
                }
            }
            pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);
        }

        return (int) (pos - destAddr);
    }

    /**
     * Encodes sorted values for a single key directly from native memory,
     * eliminating the copy to a Java array. Used by the writer's flushAllPending()
     * to encode values directly from the pending values buffer.
     *
     * @param srcAddr  native memory address of sorted long values
     * @param count    number of values
     * @param destAddr destination memory address
     * @param ctx      reusable encode context (call ensureCapacity first)
     * @return number of bytes written
     */
    public static int encodeKeyNative(long srcAddr, int count, long destAddr, EncodeContext ctx) {
        if (count == 0) {
            Unsafe.getUnsafe().putInt(destAddr, 0);
            return 4;
        }
        assert count <= ctx.deltaCapacity : "encodeKeyNative: count=" + count + " exceeds ctx.deltaCapacity=" + ctx.deltaCapacity;

        // Fast path: single block (count <= BLOCK_CAPACITY) — avoid per-block loops
        if (count <= BLOCK_CAPACITY) {
            return encodeKeyNativeSingleBlock(srcAddr, count, destAddr, ctx);
        }

        int blockCount = (count + BLOCK_CAPACITY - 1) / BLOCK_CAPACITY;
        long deltasAddr = ctx.deltasAddr;
        int[] valueCounts = ctx.blockValueCounts;
        long blockFirstValuesAddr = ctx.blockFirstValuesAddr;
        long blockMinDeltasAddr = ctx.blockMinDeltasAddr;
        int[] bitWidths = ctx.blockBitWidths;
        long residualsAddr = ctx.residualsAddr;

        // Compute deltas — reading directly from native memory
        long prev = Unsafe.getUnsafe().getLong(srcAddr);
        Unsafe.getUnsafe().putLong(deltasAddr, prev);
        for (int i = 1; i < count; i++) {
            long val = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
            Unsafe.getUnsafe().putLong(deltasAddr + (long) i * Long.BYTES, val - prev);
            prev = val;
        }

        // Per-block metadata
        for (int b = 0; b < blockCount; b++) {
            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;

            valueCounts[b] = blockSize;
            Unsafe.getUnsafe().putLong(blockFirstValuesAddr + (long) b * Long.BYTES,
                    Unsafe.getUnsafe().getLong(srcAddr + (long) blockStart * Long.BYTES));

            int numDeltas = blockSize - 1;
            long minD, maxD;
            if (numDeltas == 0) {
                minD = 0;
                maxD = 0;
            } else {
                minD = Unsafe.getUnsafe().getLong(deltasAddr + (long) (blockStart + 1) * Long.BYTES);
                maxD = minD;
                for (int i = blockStart + 2; i < blockEnd; i++) {
                    long d = Unsafe.getUnsafe().getLong(deltasAddr + (long) i * Long.BYTES);
                    if (d < minD) minD = d;
                    if (d > maxD) maxD = d;
                }
            }

            Unsafe.getUnsafe().putLong(blockMinDeltasAddr + (long) b * Long.BYTES, minD);
            long range = maxD - minD;
            bitWidths[b] = range == 0 ? 0 : BitpackUtils.bitsNeeded(range);
        }

        // Write encoded data
        long pos = destAddr;

        Unsafe.getUnsafe().putInt(pos, blockCount);
        pos += 4;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putByte(pos + b, (byte) valueCounts[b]);
        }
        pos += blockCount;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putLong(pos + (long) b * Long.BYTES,
                    Unsafe.getUnsafe().getLong(blockFirstValuesAddr + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putLong(pos + (long) b * Long.BYTES,
                    Unsafe.getUnsafe().getLong(blockMinDeltasAddr + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putByte(pos + b, (byte) bitWidths[b]);
        }
        pos += blockCount;

        for (int b = 0; b < blockCount; b++) {
            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;
            int numDeltas = blockSize - 1;
            int bitWidth = bitWidths[b];

            if (bitWidth > 0 && numDeltas > 0) {
                long minD = Unsafe.getUnsafe().getLong(blockMinDeltasAddr + (long) b * Long.BYTES);
                if (ctx.nativeResidualsAddr != 0) {
                    long nrAddr = ctx.nativeResidualsAddr;
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.getUnsafe().putLong(nrAddr + (long) i * Long.BYTES,
                                Unsafe.getUnsafe().getLong(deltasAddr + (long) (blockStart + 1 + i) * Long.BYTES) - minD);
                    }
                    PostingIndexNative.packValuesNative(nrAddr, numDeltas, 0, bitWidth, pos);
                } else {
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.getUnsafe().putLong(residualsAddr + (long) i * Long.BYTES,
                                Unsafe.getUnsafe().getLong(deltasAddr + (long) (blockStart + 1 + i) * Long.BYTES) - minD);
                    }
                    BitpackUtils.packValues(residualsAddr, numDeltas, 0, bitWidth, pos);
                }
            }
            pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);
        }

        return (int) (pos - destAddr);
    }

    /**
     * Optimized single-block encode for keys with count <= BLOCK_CAPACITY.
     * Single pass: reads values, computes deltas inline, finds min/max,
     * writes metadata and packs residuals without per-block loop overhead.
     */
    private static int encodeKeyNativeSingleBlock(long srcAddr, int count, long destAddr, EncodeContext ctx) {
        int numDeltas = count - 1;
        long firstValue = Unsafe.getUnsafe().getLong(srcAddr);

        // Compute deltas and find min/max in a single pass
        long minD = Long.MAX_VALUE;
        long maxD = Long.MIN_VALUE;
        long deltasAddr = ctx.deltasAddr;

        long prev = firstValue;
        for (int i = 1; i < count; i++) {
            long val = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
            long d = val - prev;
            Unsafe.getUnsafe().putLong(deltasAddr + (long) i * Long.BYTES, d);
            if (d < minD) minD = d;
            if (d > maxD) maxD = d;
            prev = val;
        }

        if (numDeltas == 0) {
            minD = 0;
            maxD = 0;
        }

        long range = maxD - minD;
        int bitWidth = range == 0 ? 0 : BitpackUtils.bitsNeeded(range);

        // Write: blockCount(4B) + valueCount(1B) + firstValue(8B) + minDelta(8B) + bitWidth(1B) + packedData
        long pos = destAddr;
        Unsafe.getUnsafe().putInt(pos, 1);
        pos += 4;
        Unsafe.getUnsafe().putByte(pos, (byte) count);
        pos += 1;
        Unsafe.getUnsafe().putLong(pos, firstValue);
        pos += 8;
        Unsafe.getUnsafe().putLong(pos, minD);
        pos += 8;
        Unsafe.getUnsafe().putByte(pos, (byte) bitWidth);
        pos += 1;

        if (bitWidth > 0 && numDeltas > 0) {
            if (ctx.nativeResidualsAddr != 0) {
                long nrAddr = ctx.nativeResidualsAddr;
                for (int i = 0; i < numDeltas; i++) {
                    Unsafe.getUnsafe().putLong(nrAddr + (long) i * Long.BYTES,
                            Unsafe.getUnsafe().getLong(deltasAddr + (long) (i + 1) * Long.BYTES) - minD);
                }
                PostingIndexNative.packValuesNative(nrAddr, numDeltas, 0, bitWidth, pos);
            } else {
                long residualsAddr = ctx.residualsAddr;
                for (int i = 0; i < numDeltas; i++) {
                    Unsafe.getUnsafe().putLong(residualsAddr + (long) i * Long.BYTES,
                            Unsafe.getUnsafe().getLong(deltasAddr + (long) (i + 1) * Long.BYTES) - minD);
                }
                BitpackUtils.packValues(residualsAddr, numDeltas, 0, bitWidth, pos);
            }
        }
        pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);

        return (int) (pos - destAddr);
    }

    /**
     * Reusable workspace for encodeKey to avoid per-call allocations.
     */
    public static class EncodeContext {
        long deltasAddr;
        int[] blockValueCounts;
        long blockFirstValuesAddr;
        long blockMinDeltasAddr;
        int[] blockBitWidths;
        long residualsAddr;
        // Native residuals buffer for SIMD packing (BLOCK_CAPACITY * 8 bytes)
        long nativeResidualsAddr;
        int deltaCapacity;
        private int blockCapacity;
        private int residualsCapacity;

        public void ensureCapacity(int count) {
            if (count > deltaCapacity) {
                int newCapacity = Math.max(count, deltaCapacity * 2);
                long newAddr = Unsafe.malloc((long) newCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                if (deltasAddr != 0) {
                    Unsafe.free(deltasAddr, (long) deltaCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                deltasAddr = newAddr;
                deltaCapacity = newCapacity;
            }
            int bc = (count + BLOCK_CAPACITY - 1) / BLOCK_CAPACITY;
            if (bc > blockCapacity) {
                int newBc = Math.max(bc, blockCapacity * 2);
                long newFirstAddr = Unsafe.malloc((long) newBc * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                long newMinAddr;
                try {
                    newMinAddr = Unsafe.malloc((long) newBc * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                } catch (Throwable e) {
                    Unsafe.free(newFirstAddr, (long) newBc * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    throw e;
                }
                if (blockFirstValuesAddr != 0) {
                    Unsafe.free(blockFirstValuesAddr, (long) blockCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                if (blockMinDeltasAddr != 0) {
                    Unsafe.free(blockMinDeltasAddr, (long) blockCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                blockFirstValuesAddr = newFirstAddr;
                blockMinDeltasAddr = newMinAddr;
                blockCapacity = newBc;
                blockValueCounts = new int[newBc];
                blockBitWidths = new int[newBc];
            }
            if (residualsAddr == 0) {
                residualsAddr = Unsafe.malloc((long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                residualsCapacity = BLOCK_CAPACITY;
            }
            if (nativeResidualsAddr == 0 && PostingIndexNative.isNativeAvailable()) {
                nativeResidualsAddr = Unsafe.malloc((long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
        }

        public void close() {
            if (deltasAddr != 0) {
                Unsafe.free(deltasAddr, (long) deltaCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                deltasAddr = 0;
            }
            if (blockFirstValuesAddr != 0) {
                Unsafe.free(blockFirstValuesAddr, (long) blockCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockFirstValuesAddr = 0;
            }
            if (blockMinDeltasAddr != 0) {
                Unsafe.free(blockMinDeltasAddr, (long) blockCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockMinDeltasAddr = 0;
            }
            if (residualsAddr != 0) {
                Unsafe.free(residualsAddr, (long) residualsCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                residualsAddr = 0;
            }
            if (nativeResidualsAddr != 0) {
                Unsafe.free(nativeResidualsAddr, (long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                nativeResidualsAddr = 0;
            }
        }
    }

    /**
     * Reusable workspace for decodeKey to avoid per-call allocations.
     */
    public static class DecodeContext {
        int[] valueCounts;
        long firstValuesAddr;
        long minDeltasAddr;
        int[] bitWidths;
        long blockDeltasAddr;
        private int blockCapacity;

        public void ensureCapacity(int blockCount) {
            if (blockCount > blockCapacity) {
                int newCapacity = Math.max(blockCount, blockCapacity * 2);
                long newFirstAddr = Unsafe.malloc((long) newCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                long newMinAddr;
                try {
                    newMinAddr = Unsafe.malloc((long) newCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                } catch (Throwable e) {
                    Unsafe.free(newFirstAddr, (long) newCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    throw e;
                }
                if (firstValuesAddr != 0) {
                    Unsafe.free(firstValuesAddr, (long) blockCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                if (minDeltasAddr != 0) {
                    Unsafe.free(minDeltasAddr, (long) blockCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                firstValuesAddr = newFirstAddr;
                minDeltasAddr = newMinAddr;
                blockCapacity = newCapacity;
                valueCounts = new int[newCapacity];
                bitWidths = new int[newCapacity];
            }
            if (blockDeltasAddr == 0) {
                blockDeltasAddr = Unsafe.malloc((long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
        }

        public void close() {
            if (firstValuesAddr != 0) {
                Unsafe.free(firstValuesAddr, (long) blockCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                firstValuesAddr = 0;
            }
            if (minDeltasAddr != 0) {
                Unsafe.free(minDeltasAddr, (long) blockCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                minDeltasAddr = 0;
            }
            if (blockDeltasAddr != 0) {
                Unsafe.free(blockDeltasAddr, (long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockDeltasAddr = 0;
            }
        }
    }

    /**
     * Offset of generation directory entry within a metadata page.
     *
     * @param pageBase the base offset of the page (PAGE_A_OFFSET or PAGE_B_OFFSET)
     * @param genIndex the generation index
     */
    public static long getGenDirOffset(long pageBase, int genIndex) {
        return pageBase + PAGE_OFFSET_GEN_DIR + (long) genIndex * GEN_DIR_ENTRY_SIZE;
    }

    /**
     * Number of stride blocks for the given key count.
     */
    public static int strideCount(int keyCount) {
        return (keyCount + DENSE_STRIDE - 1) / DENSE_STRIDE;
    }

    /**
     * Size of the stride index: (strideCount + 1) × 4B.
     * The extra entry is a sentinel holding the total size of all stride blocks.
     */
    public static int strideIndexSize(int keyCount) {
        return (strideCount(keyCount) + 1) * Integer.BYTES;
    }

    /**
     * Number of keys in a given stride block.
     */
    public static int keysInStride(int keyCount, int stride) {
        int sc = strideCount(keyCount);
        if (stride < sc - 1) return DENSE_STRIDE;
        int rem = keyCount % DENSE_STRIDE;
        return rem == 0 ? DENSE_STRIDE : rem;
    }

    /**
     * Size of a delta-mode stride block header: mode prefix + counts + prefix-sum offsets.
     */
    public static int strideDeltaHeaderSize(int keysInStride) {
        return STRIDE_MODE_PREFIX_SIZE + keysInStride * Integer.BYTES + (keysInStride + 1) * Integer.BYTES;
    }

    /**
     * Size of a flat-mode stride block header: mode prefix + baseValue(8B) + prefixCounts.
     */
    public static int strideFlatHeaderSize(int keysInStride) {
        return STRIDE_FLAT_PREFIX_COUNTS_OFFSET + (keysInStride + 1) * Integer.BYTES;
    }

    /**
     * Size of the per-generation sparse header: keyIds + counts + offsets for active keys only.
     */
    public static int genHeaderSizeSparse(int activeKeyCount) {
        return activeKeyCount * Integer.BYTES * 3;
    }

    /**
     * Binary search for a key ID in a sorted keyIds array stored at native memory.
     *
     * @return index if found (>= 0), or -(insertionPoint + 1) if not found
     */
    public static int binarySearchKeyId(long keyIdsAddr, int activeKeyCount, int key) {
        int lo = 0, hi = activeKeyCount - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int midKey = Unsafe.getUnsafe().getInt(keyIdsAddr + (long) mid * Integer.BYTES);
            if (midKey < key) {
                lo = mid + 1;
            } else if (midKey > key) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -(lo + 1);
    }

    public static LPSZ coverDataFileName(Path path, CharSequence name, long columnNameTxn, int includeIdx) {
        path.concat(name).put(".pc").put(includeIdx);
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    public static LPSZ coverInfoFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".pci");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    public static LPSZ keyFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".pk");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    public static LPSZ valueFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".pv");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }
}

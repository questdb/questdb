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

import io.questdb.cairo.CairoException;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Constants and encoding/decoding utilities for the Posting index sealed format.
 *
 * <h2>Sealed stride encoding</h2>
 * <p>
 * At seal time the writer groups keys into strides of {@link #DENSE_STRIDE} (256)
 * consecutive keys and independently encodes each stride using the smaller of two
 * modes:
 *
 * <h3>Delta mode ({@link #STRIDE_MODE_DELTA}, 0x00) — per-key delta + FoR</h3>
 * <p>
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
 * <p>
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
 * <p>
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
 * packedOffsets[]      : blockCount × 4B  (byte offset from packed data start per block)
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
 *   [40-47]     valueFileTxn (8B) — txn suffix of the .pv file after seal
 *   [48-63]     reserved (16B)
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
    // Elias-Fano encoding: if the first 4 bytes of an encoded key blob equal this sentinel,
    // the blob uses Elias-Fano format. Otherwise, it is the legacy delta-FoR format.
    // Safe because delta-FoR's leading blockCount is always a small positive integer.
    public static final int EF_FORMAT_SENTINEL = Integer.MIN_VALUE; // 0x80000000
    public static final byte ENCODING_ADAPTIVE = 0;
    public static final byte ENCODING_DELTA = 1;
    public static final byte ENCODING_EF = 2;
    // EF header: sentinel(4B) + count(4B) + L(1B) + universe(8B) = 17B
    static final int EF_HEADER_SIZE = 17;
    public static final int FORMAT_VERSION = 1;
    // Generation directory entry (32 bytes per generation, 8-byte aligned)
    public static final int GEN_DIR_ENTRY_SIZE = 32;
    public static final int GEN_DIR_OFFSET_FILE_OFFSET = 0;
    public static final int GEN_DIR_OFFSET_KEY_COUNT = 16;
    public static final int GEN_DIR_OFFSET_MAX_KEY = 24;
    public static final int GEN_DIR_OFFSET_MIN_KEY = 20;
    public static final int GEN_DIR_OFFSET_SIDECAR_OFFSET = 28; // 4 bytes: offset into .pc* sidecar files
    public static final int GEN_DIR_OFFSET_SIZE = 8;
    public static final int KEY_FILE_RESERVED = 8192; // was 64
    public static final int MAX_BLOCK_COUNT = 1_000_000; // corruption guard: 64M values at BLOCK_CAPACITY=64
    public static final int MAX_COVER_COUNT = 4096; // corruption guard for readCoverCountFromInfoFile
    public static final int MAX_GEN_COUNT = 125; // (4088-64)/32 = 125
    public static final int PACKED_BATCH_SIZE = BLOCK_CAPACITY;
    public static final long PAGE_A_OFFSET = 0;
    public static final long PAGE_B_OFFSET = 4096;
    public static final int PAGE_OFFSET_BLOCK_CAPACITY = 16;
    public static final int PAGE_OFFSET_FORMAT_VERSION = 36;
    public static final int PAGE_OFFSET_GEN_COUNT = 32;
    public static final int PAGE_OFFSET_GEN_DIR = 64;
    public static final int PAGE_OFFSET_KEY_COUNT = 20;
    public static final int PAGE_OFFSET_MAX_VALUE = 24;
    public static final int PAGE_OFFSET_SEQUENCE_END = 4088;
    // Per-page offsets
    public static final int PAGE_OFFSET_SEQUENCE_START = 0;
    public static final int PAGE_OFFSET_VALUE_FILE_TXN = 40; // 8 bytes: txn suffix for the .pv file
    public static final int PAGE_OFFSET_VALUE_MEM_SIZE = 8;
    // Double-buffered 4KB metadata pages (v2 format)
    public static final int PAGE_SIZE = 4096;
    public static final byte SIGNATURE = (byte) 0xfb;
    // Stride block mode constants — see class javadoc for when each mode wins
    public static final byte STRIDE_MODE_DELTA = 0;
    public static final byte STRIDE_MODE_FLAT = 1;
    public static final int STRIDE_MODE_PREFIX_SIZE = 4; // mode(1B) + bitWidth/reserved(1B) + padding(2B)
    public static final int STRIDE_FLAT_BASE_OFFSET = STRIDE_MODE_PREFIX_SIZE; // baseValue(8B) follows mode prefix
    public static final int STRIDE_FLAT_PREFIX_COUNTS_OFFSET = STRIDE_MODE_PREFIX_SIZE + Long.BYTES; // = 12

    private PostingIndexUtils() {
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

    /**
     * Computes worst-case encoded size for a key with {@code count} values.
     * Used to pre-allocate encode buffers.
     */
    public static long computeMaxEncodedSize(int count) {
        // EF worst case: header + 8-byte-aligned low bits (L=63) + 8-byte-aligned high bits
        long efMax = EF_HEADER_SIZE + efLowBytesAligned(count, 63) + (long) ((count + 63) / 64) * 8;
        // Delta-FoR worst case (for reading old data):
        int blockCount = (count + BLOCK_CAPACITY - 1) / BLOCK_CAPACITY;
        long totalDeltas = count - blockCount;
        long packedOffsetsSize = blockCount > 1 ? (long) blockCount * Integer.BYTES : 0;
        long deltaMax = 4 + (long) blockCount * 18 + packedOffsetsSize + totalDeltas * 8;
        return Math.max(efMax, deltaMax);
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

    /**
     * Decodes all values for a key from delta-encoded data.
     * Allocates temporary arrays per call — use the overload with DecodeContext
     * on hot paths to avoid allocations.
     *
     * @param srcAddr address of the encoded data for this key
     * @param dest    destination array (must have room for totalCount values)
     */
    public static void decodeKey(long srcAddr, long[] dest) {
        int firstWord = Unsafe.getUnsafe().getInt(srcAddr);
        if (firstWord == EF_FORMAT_SENTINEL) {
            decodeKeyEF(srcAddr, dest);
            return;
        }
        int blockCount = firstWord;
        if (blockCount < 0 || blockCount > MAX_BLOCK_COUNT) {
            throw CairoException.critical(0).put("corrupt posting index: invalid blockCount [blockCount=").put(blockCount).put(']');
        }
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

            // Skip packedOffsets (only present for multi-block keys)
            if (blockCount > 1) {
                pos += (long) blockCount * Integer.BYTES;
            }

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
     * Decodes all values for a key from delta-encoded data directly into native memory,
     * using pre-allocated workspace arrays from the provided context.
     *
     * @param srcAddr  address of the encoded data for this key
     * @param destAddr native memory destination address (must have room for totalCount longs)
     * @param ctx      reusable decode context (call ensureCapacity first)
     */
    public static void decodeKeyToNative(long srcAddr, long destAddr, DecodeContext ctx) {
        int firstWord = Unsafe.getUnsafe().getInt(srcAddr);
        if (firstWord == EF_FORMAT_SENTINEL) {
            decodeKeyEFToNative(srcAddr, destAddr);
            return;
        }
        int blockCount = firstWord;
        if (blockCount < 0 || blockCount > MAX_BLOCK_COUNT) {
            throw CairoException.critical(0).put("corrupt posting index: invalid blockCount [blockCount=").put(blockCount).put(']');
        }
        long pos = srcAddr + 4;

        ctx.ensureCapacity(blockCount);
        long valueCountsAddr = ctx.valueCountsAddr;
        long firstValuesAddr = ctx.firstValuesAddr;
        long minDeltasAddr = ctx.minDeltasAddr;
        long bitWidthsAddr = ctx.bitWidthsAddr;
        long blockDeltasAddr = ctx.blockDeltasAddr;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putInt(valueCountsAddr + (long) b * Integer.BYTES, Unsafe.getUnsafe().getByte(pos + b) & 0xFF);
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
            Unsafe.getUnsafe().putInt(bitWidthsAddr + (long) b * Integer.BYTES, Unsafe.getUnsafe().getByte(pos + b) & 0xFF);
        }
        pos += blockCount;

        // Skip packedOffsets (only present for multi-block keys)
        if (blockCount > 1) {
            pos += (long) blockCount * Integer.BYTES;
        }

        int destIdx = 0;
        for (int b = 0; b < blockCount; b++) {
            int count = Unsafe.getUnsafe().getInt(valueCountsAddr + (long) b * Integer.BYTES);
            int bitWidth = Unsafe.getUnsafe().getInt(bitWidthsAddr + (long) b * Integer.BYTES);
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
     * Uses pre-allocated workspace arrays from the provided context.
     *
     * @param values   array of sorted values
     * @param count    number of values
     * @param destAddr destination memory address
     * @param ctx      reusable encode context (call ensureCapacity first)
     * @return number of bytes written
     */
    public static int encodeKey(long[] values, int count, long destAddr, EncodeContext ctx) {
        return encodeKey(values, count, destAddr, ctx, true);
    }

    public static int encodeKey(long[] values, int count, long destAddr, EncodeContext ctx, boolean useEliasFano) {
        if (count == 0) {
            Unsafe.getUnsafe().putInt(destAddr, 0);
            return 4;
        }
        if (useEliasFano) {
            // Copy values to deltasAddr for the EF trial encode, then use the
            // Java array for delta-FoR (encodeKeyDeltaFoR reads from the array,
            // not deltasAddr, avoiding aliasing issues).
            long srcAddr = ctx.deltasAddr;
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putLong(srcAddr + (long) i * Long.BYTES, values[i]);
            }
            int efSize = encodeKeyEF(srcAddr, count, ctx.efTrialAddr);
            int deltaSize = encodeKeyDeltaFoR(values, count, destAddr, ctx);
            if (efSize < deltaSize) {
                Unsafe.getUnsafe().copyMemory(ctx.efTrialAddr, destAddr, efSize);
                return efSize;
            }
            return deltaSize;
        }
        return encodeKeyDeltaFoR(values, count, destAddr, ctx);
    }

    /**
     * Legacy: encodes sorted values using delta + FoR64 bitpacking.
     * Retained for reading old-format data; new writes use Elias-Fano.
     */
    public static int encodeKeyDeltaFoR(long[] values, int count, long destAddr, EncodeContext ctx) {
        if (count == 0) {
            Unsafe.getUnsafe().putInt(destAddr, 0);
            return 4;
        }

        int blockCount = (count + BLOCK_CAPACITY - 1) / BLOCK_CAPACITY;
        long deltasAddr = ctx.deltasAddr;
        long valueCountsAddr = ctx.blockValueCountsAddr;
        long blockFirstValuesAddr = ctx.blockFirstValuesAddr;
        long blockMinDeltasAddr = ctx.blockMinDeltasAddr;
        long bitWidthsAddr = ctx.blockBitWidthsAddr;
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

            Unsafe.getUnsafe().putInt(valueCountsAddr + (long) b * Integer.BYTES, blockSize);
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
            Unsafe.getUnsafe().putInt(bitWidthsAddr + (long) b * Integer.BYTES, range == 0 ? 0 : BitpackUtils.bitsNeeded(range));
        }

        // Write encoded data
        long pos = destAddr;

        // blockCount (4B)
        Unsafe.getUnsafe().putInt(pos, blockCount);
        pos += 4;

        // valueCounts[] (blockCount × 1B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putByte(pos + b, (byte) Unsafe.getUnsafe().getInt(valueCountsAddr + (long) b * Integer.BYTES));
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
            Unsafe.getUnsafe().putByte(pos + b, (byte) Unsafe.getUnsafe().getInt(bitWidthsAddr + (long) b * Integer.BYTES));
        }
        pos += blockCount;

        // packedOffsets[] (only for multi-block keys)
        long packedOffsetsAddr = 0;
        if (blockCount > 1) {
            packedOffsetsAddr = pos;
            pos += (long) blockCount * Integer.BYTES;
        }

        // Packed blocks — only pack the numDeltas=blockSize-1 inter-value deltas
        long packedDataStart = pos;
        for (int b = 0; b < blockCount; b++) {
            if (packedOffsetsAddr != 0) {
                Unsafe.getUnsafe().putInt(packedOffsetsAddr + (long) b * Integer.BYTES, (int) (pos - packedDataStart));
            }

            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;
            int numDeltas = blockSize - 1;
            int bitWidth = Unsafe.getUnsafe().getInt(bitWidthsAddr + (long) b * Integer.BYTES);

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
        return encodeKeyNative(srcAddr, count, destAddr, ctx, ENCODING_ADAPTIVE);
    }

    public static int encodeKeyNative(long srcAddr, int count, long destAddr, EncodeContext ctx, byte encoding) {
        if (count == 0) {
            Unsafe.getUnsafe().putInt(destAddr, 0);
            return 4;
        }
        return switch (encoding) {
            case ENCODING_EF -> encodeKeyEF(srcAddr, count, destAddr);
            case ENCODING_DELTA -> encodeKeyNativeDeltaFoR(srcAddr, count, destAddr, ctx);
            default -> encodeKeyNativeAdaptive(srcAddr, count, destAddr, ctx);
        };
    }

    /**
     * Encodes a key using the smaller of EF and delta-FoR.
     * Trial-encodes with both codecs and picks the winner. The format sentinel
     * (EF_FORMAT_SENTINEL for EF, positive blockCount for delta-FoR) allows the
     * decoder to auto-detect the format, so mixed keys within a stride work
     * transparently.
     */
    public static int encodeKeyNativeAdaptive(long srcAddr, int count, long destAddr, EncodeContext ctx) {
        if (count == 0) {
            Unsafe.getUnsafe().putInt(destAddr, 0);
            return 4;
        }
        int efSize = encodeKeyEF(srcAddr, count, ctx.efTrialAddr);
        int deltaSize = encodeKeyNativeDeltaFoR(srcAddr, count, destAddr, ctx);
        if (efSize < deltaSize) {
            Unsafe.getUnsafe().copyMemory(ctx.efTrialAddr, destAddr, efSize);
            return efSize;
        }
        return deltaSize;
    }

    /**
     * Legacy: encodes sorted values using delta + FoR64 bitpacking from native memory.
     */
    public static int encodeKeyNativeDeltaFoR(long srcAddr, int count, long destAddr, EncodeContext ctx) {
        if (count == 0) {
            Unsafe.getUnsafe().putInt(destAddr, 0);
            return 4;
        }
        assert count <= ctx.deltaCapacity : "encodeKeyNativeDeltaFoR: count=" + count + " exceeds ctx.deltaCapacity=" + ctx.deltaCapacity;

        // Fast path: single block (count <= BLOCK_CAPACITY) — avoid per-block loops
        if (count <= BLOCK_CAPACITY) {
            return encodeKeyNativeSingleBlock(srcAddr, count, destAddr, ctx);
        }

        int blockCount = (count + BLOCK_CAPACITY - 1) / BLOCK_CAPACITY;
        long deltasAddr = ctx.deltasAddr;
        long valueCountsAddr = ctx.blockValueCountsAddr;
        long blockFirstValuesAddr = ctx.blockFirstValuesAddr;
        long blockMinDeltasAddr = ctx.blockMinDeltasAddr;
        long bitWidthsAddr = ctx.blockBitWidthsAddr;
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

            Unsafe.getUnsafe().putInt(valueCountsAddr + (long) b * Integer.BYTES, blockSize);
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
            Unsafe.getUnsafe().putInt(bitWidthsAddr + (long) b * Integer.BYTES, range == 0 ? 0 : BitpackUtils.bitsNeeded(range));
        }

        // Write encoded data
        long pos = destAddr;

        Unsafe.getUnsafe().putInt(pos, blockCount);
        pos += 4;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putByte(pos + b, (byte) Unsafe.getUnsafe().getInt(valueCountsAddr + (long) b * Integer.BYTES));
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
            Unsafe.getUnsafe().putByte(pos + b, (byte) Unsafe.getUnsafe().getInt(bitWidthsAddr + (long) b * Integer.BYTES));
        }
        pos += blockCount;

        // Packed data offsets: byte offset from packed data start to each block's packed data.
        // Enables O(1) random access to any block without forward scanning.
        long packedOffsetsAddr = pos;
        pos += (long) blockCount * Integer.BYTES;

        long packedDataStart = pos;
        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putInt(packedOffsetsAddr + (long) b * Integer.BYTES, (int) (pos - packedDataStart));

            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;
            int numDeltas = blockSize - 1;
            int bitWidth = Unsafe.getUnsafe().getInt(bitWidthsAddr + (long) b * Integer.BYTES);

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
     * Size of the per-generation sparse header: keyIds + counts + offsets for active keys only.
     */
    public static int genHeaderSizeSparse(int activeKeyCount) {
        return activeKeyCount * Integer.BYTES * 3;
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

    public static LPSZ keyFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".pk");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
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

    public static int readCoverCountFromInfoFile(FilesFacade ff, LPSZ pciFilePath) {
        long fd = ff.openRO(pciFilePath);
        if (fd < 0) {
            return 0;
        }
        try {
            int magic = ff.readNonNegativeInt(fd, 0);
            if (magic != COVER_INFO_MAGIC) {
                return 0;
            }
            int count = ff.readNonNegativeInt(fd, 4);
            // Guard against corrupted .pci: a garbled count could cause
            // billions of removeQuiet iterations in removeSidecarFiles.
            // No table can have more than a few thousand columns, so any
            // value beyond that is corruption.
            return count >= 0 && count <= MAX_COVER_COUNT ? count : 0;
        } finally {
            ff.close(fd);
        }
    }

    public static long readValueFileTxnFromKeyFile(FilesFacade ff, LPSZ keyFilePath) {
        long fd = ff.openRO(keyFilePath);
        if (fd < 0) {
            return -1;
        }
        try {
            // Read sequence from both pages, pick the one with higher valid sequence
            long seqA = ff.readNonNegativeLong(fd, PAGE_A_OFFSET + PAGE_OFFSET_SEQUENCE_START);
            long seqEndA = ff.readNonNegativeLong(fd, PAGE_A_OFFSET + PAGE_OFFSET_SEQUENCE_END);
            long seqB = ff.readNonNegativeLong(fd, PAGE_B_OFFSET + PAGE_OFFSET_SEQUENCE_START);
            long seqEndB = ff.readNonNegativeLong(fd, PAGE_B_OFFSET + PAGE_OFFSET_SEQUENCE_END);

            long pageOffset;
            if (seqA == seqEndA && seqB == seqEndB) {
                pageOffset = seqA >= seqB ? PAGE_A_OFFSET : PAGE_B_OFFSET;
            } else if (seqA == seqEndA) {
                pageOffset = PAGE_A_OFFSET;
            } else if (seqB == seqEndB) {
                pageOffset = PAGE_B_OFFSET;
            } else {
                return -1;
            }
            return ff.readNonNegativeLong(fd, pageOffset + PAGE_OFFSET_VALUE_FILE_TXN);
        } finally {
            ff.close(fd);
        }
    }

    public static void removeSidecarFiles(FilesFacade ff, Path path, int pathTrimTo, CharSequence columnName, long columnVersion) {
        LPSZ pciFile = coverInfoFileName(path.trimTo(pathTrimTo), columnName, columnVersion);
        if (ff.exists(pciFile)) {
            int coverCount = readCoverCountFromInfoFile(ff, pciFile);
            ff.removeQuiet(pciFile);
            for (int c = 0; c < coverCount; c++) {
                ff.removeQuiet(coverDataFileName(path.trimTo(pathTrimTo), columnName, columnVersion, c));
            }
        }
    }

    /**
     * Number of stride blocks for the given key count.
     */
    public static int strideCount(int keyCount) {
        return (keyCount + DENSE_STRIDE - 1) / DENSE_STRIDE;
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
     * Size of the stride index: (strideCount + 1) × 4B.
     * The extra entry is a sentinel holding the total size of all stride blocks.
     */
    public static int strideIndexSize(int keyCount) {
        return (strideCount(keyCount) + 1) * Integer.BYTES;
    }

    public static LPSZ valueFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".pv");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
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
        // No packedOffsets for single-block keys — the offset is implicitly 0.
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
     * Reusable workspace for decodeKey to avoid per-call allocations.
     */
    public static class DecodeContext implements QuietCloseable {
        long bitWidthsAddr;
        long blockDeltasAddr;
        long firstValuesAddr;
        long minDeltasAddr;
        long valueCountsAddr;
        private int blockCapacity;

        public void close() {
            if (firstValuesAddr != 0) {
                Unsafe.free(firstValuesAddr, (long) blockCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                firstValuesAddr = 0;
            }
            if (minDeltasAddr != 0) {
                Unsafe.free(minDeltasAddr, (long) blockCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                minDeltasAddr = 0;
            }
            if (valueCountsAddr != 0) {
                Unsafe.free(valueCountsAddr, (long) blockCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                valueCountsAddr = 0;
            }
            if (bitWidthsAddr != 0) {
                Unsafe.free(bitWidthsAddr, (long) blockCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                bitWidthsAddr = 0;
            }
            if (blockDeltasAddr != 0) {
                Unsafe.free(blockDeltasAddr, (long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockDeltasAddr = 0;
            }
            blockCapacity = 0;
        }

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
                long newValueCountsAddr = Unsafe.malloc((long) newCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                long newBitWidthsAddr;
                try {
                    newBitWidthsAddr = Unsafe.malloc((long) newCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                } catch (Throwable e) {
                    Unsafe.free(newValueCountsAddr, (long) newCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    throw e;
                }
                if (valueCountsAddr != 0) {
                    Unsafe.free(valueCountsAddr, (long) blockCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                if (bitWidthsAddr != 0) {
                    Unsafe.free(bitWidthsAddr, (long) blockCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                valueCountsAddr = newValueCountsAddr;
                bitWidthsAddr = newBitWidthsAddr;
                blockCapacity = newCapacity;
            }
            if (blockDeltasAddr == 0) {
                blockDeltasAddr = Unsafe.malloc((long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
        }
    }

    // ==================================================================================
    // Elias-Fano encode/decode
    // ==================================================================================

    /**
     * Decodes an Elias-Fano encoded key into a Java array.
     */
    public static void decodeKeyEF(long srcAddr, long[] dest) {
        long pos = srcAddr + 4; // skip sentinel
        int n = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int L = Unsafe.getUnsafe().getByte(pos) & 0xFF;
        pos += 1;
        long u = Unsafe.getUnsafe().getLong(pos);
        pos += 8;

        long lowMask = (L < 64) ? (1L << L) - 1 : -1L;
        long lowStart = pos;
        int lowBytes = efLowBytesAligned(n, L);
        long highStart = pos + lowBytes;
        long numHighWordsL = (n + (u >>> L) + 63) / 64;
        if (numHighWordsL > Integer.MAX_VALUE) {
            throw CairoException.critical(0).put("EF index too large");
        }
        int numHighWords = (int) numHighWordsL;

        int outputIdx = 0;
        for (int w = 0; w < numHighWords && outputIdx < n; w++) {
            long word = Unsafe.getUnsafe().getLong(highStart + (long) w * 8);
            if (word == 0) continue;
            long base = (long) w * 64 - outputIdx;
            while (word != 0 && outputIdx < n) {
                int trail = Long.numberOfTrailingZeros(word);
                long low = readBitsWord(lowStart, (long) outputIdx * L, L) & lowMask;
                dest[outputIdx++] = ((base + trail) << L) | low;
                base--;
                word &= word - 1;
            }
        }
    }

    /**
     * Decodes an Elias-Fano encoded key directly to native memory.
     * Two-pass: (1) SIMD bulk-unpack low bits, (2) scan high bits and merge.
     */
    public static void decodeKeyEFToNative(long srcAddr, long destAddr) {
        long pos = srcAddr + 4; // skip sentinel
        int n = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        int L = Unsafe.getUnsafe().getByte(pos) & 0xFF;
        pos += 1;
        long u = Unsafe.getUnsafe().getLong(pos);
        pos += 8;

        long lowStart = pos;
        int lowBytes = efLowBytesAligned(n, L);
        long highStart = pos + lowBytes;
        long numHighWordsL = (n + (u >>> L) + 63) / 64;
        if (numHighWordsL > Integer.MAX_VALUE) {
            throw CairoException.critical(0).put("EF index too large");
        }
        int numHighWords = (int) numHighWordsL;

        // Pass 1: SIMD bulk-unpack all low bits into destAddr
        if (L > 0) {
            BitpackUtils.unpackAllValues(lowStart, n, L, 0, destAddr);
        } else {
            Unsafe.getUnsafe().setMemory(destAddr, (long) n * Long.BYTES, (byte) 0);
        }

        // Pass 2: scan high bits, merge into destAddr via read-modify-write
        int outputIdx = 0;
        for (int w = 0; w < numHighWords && outputIdx < n; w++) {
            long word = Unsafe.getUnsafe().getLong(highStart + (long) w * 8);
            if (word == 0) continue;
            long base = (long) w * 64 - outputIdx;
            while (word != 0 && outputIdx < n) {
                int trail = Long.numberOfTrailingZeros(word);
                long addr = destAddr + (long) outputIdx * 8;
                Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) | ((base + trail) << L));
                outputIdx++;
                base--;
                word &= word - 1;
            }
        }
    }

    /**
     * Encodes sorted long values using Elias-Fano encoding directly from native memory.
     * Format: [sentinel:4B][count:4B][L:1B][universe:8B][lowBits][highBits]
     */
    public static int encodeKeyEF(long srcAddr, int count, long destAddr) {
        long lastValue = Unsafe.getUnsafe().getLong(srcAddr + (long) (count - 1) * Long.BYTES);
        long u = lastValue + 1;
        int L = Math.max(0, 63 - Long.numberOfLeadingZeros(u / count));
        long lowMask = (L < 64) ? (1L << L) - 1 : -1L;

        long pos = destAddr;
        Unsafe.getUnsafe().putInt(pos, EF_FORMAT_SENTINEL);
        pos += 4;
        Unsafe.getUnsafe().putInt(pos, count);
        pos += 4;
        Unsafe.getUnsafe().putByte(pos, (byte) L);
        pos += 1;
        Unsafe.getUnsafe().putLong(pos, u);
        pos += 8;

        long lowStart = pos;
        int lowBytes = efLowBytesAligned(count, L);
        for (long i = 0; i < lowBytes; i += 8) {
            Unsafe.getUnsafe().putLong(lowStart + i, 0);
        }
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
            writeBitsWord(lowStart, (long) i * L, val & lowMask, L);
        }
        pos += lowBytes;

        long highStart = pos;
        long totalHighBits = count + (u >>> L);
        long numHighWordsL = (totalHighBits + 63) / 64;
        if (numHighWordsL > Integer.MAX_VALUE) {
            throw CairoException.critical(0).put("EF index too large");
        }
        int numHighWords = (int) numHighWordsL;
        int highBytes = numHighWords * 8;
        for (long i = 0; i < highBytes; i += 8) {
            Unsafe.getUnsafe().putLong(highStart + i, 0);
        }
        long bitPos = 0;
        long prevHigh = 0;
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
            long high = val >>> L;
            bitPos += (high - prevHigh);
            setBitWord(highStart, bitPos);
            bitPos++;
            prevHigh = high;
        }
        pos += highBytes;

        return (int) (pos - destAddr);
    }

    // ==================================================================================
    // Elias-Fano support methods for readers
    // ==================================================================================



    /**
     * Computes 8-byte-aligned size for the EF low bits region.
     */
    static int efLowBytesAligned(int n, int L) {
        return (int) ((((long) n * L + 63) >>> 6) << 3);
    }


    // ==================================================================================
    // Bit manipulation helpers for Elias-Fano
    // ==================================================================================

    /**
     * Reads numBits starting at bitPos using 64-bit aligned loads.
     */
    static long readBitsWord(long baseAddr, long bitPos, int numBits) {
        if (numBits == 0) return 0;
        long wordAddr = baseAddr + ((bitPos >>> 6) << 3);
        int bitOffset = (int) (bitPos & 63);
        long word = Unsafe.getUnsafe().getLong(wordAddr);
        long value = word >>> bitOffset;
        if (bitOffset + numBits > 64) {
            value |= Unsafe.getUnsafe().getLong(wordAddr + 8) << (64 - bitOffset);
        }
        return (numBits < 64) ? value & ((1L << numBits) - 1) : value;
    }

    private static void setBitWord(long baseAddr, long bitPos) {
        long wordAddr = baseAddr + ((bitPos >>> 6) << 3);
        int bitOffset = (int) (bitPos & 63);
        long word = Unsafe.getUnsafe().getLong(wordAddr);
        Unsafe.getUnsafe().putLong(wordAddr, word | (1L << bitOffset));
    }

    private static void writeBitsWord(long baseAddr, long bitPos, long value, int numBits) {
        if (numBits == 0) return;
        long wordAddr = baseAddr + ((bitPos >>> 6) << 3);
        int bitOffset = (int) (bitPos & 63);
        long mask = (numBits < 64) ? (1L << numBits) - 1 : -1L;
        long word = Unsafe.getUnsafe().getLong(wordAddr);
        word = (word & ~(mask << bitOffset)) | ((value & mask) << bitOffset);
        Unsafe.getUnsafe().putLong(wordAddr, word);
        if (bitOffset + numBits > 64) {
            long word2 = Unsafe.getUnsafe().getLong(wordAddr + 8);
            int bitsInSecond = bitOffset + numBits - 64;
            long mask2 = (1L << bitsInSecond) - 1;
            word2 = (word2 & ~mask2) | ((value >>> (64 - bitOffset)) & mask2);
            Unsafe.getUnsafe().putLong(wordAddr + 8, word2);
        }
    }

    /**
     * Reusable workspace for encodeKey to avoid per-call allocations.
     */
    public static class EncodeContext implements QuietCloseable {
        long blockBitWidthsAddr;
        long blockFirstValuesAddr;
        long blockMinDeltasAddr;
        long blockValueCountsAddr;
        int deltaCapacity;
        long deltasAddr;
        long efTrialAddr;
        long efTrialCapacity;
        // Native residuals buffer for SIMD packing (BLOCK_CAPACITY * 8 bytes)
        long nativeResidualsAddr;
        long residualsAddr;
        private int blockCapacity;
        private int residualsCapacity;

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
            if (blockValueCountsAddr != 0) {
                Unsafe.free(blockValueCountsAddr, (long) blockCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockValueCountsAddr = 0;
            }
            if (blockBitWidthsAddr != 0) {
                Unsafe.free(blockBitWidthsAddr, (long) blockCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockBitWidthsAddr = 0;
            }
            blockCapacity = 0;
            deltaCapacity = 0;
            if (efTrialAddr != 0) {
                Unsafe.free(efTrialAddr, efTrialCapacity, MemoryTag.NATIVE_INDEX_READER);
                efTrialAddr = 0;
                efTrialCapacity = 0;
            }
            if (residualsAddr != 0) {
                Unsafe.free(residualsAddr, (long) residualsCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                residualsAddr = 0;
            }
            if (nativeResidualsAddr != 0) {
                Unsafe.free(nativeResidualsAddr, (long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                nativeResidualsAddr = 0;
            }
            residualsCapacity = 0;
        }

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
                long newValueCountsAddr = Unsafe.malloc((long) newBc * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                long newBitWidthsAddr;
                try {
                    newBitWidthsAddr = Unsafe.malloc((long) newBc * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                } catch (Throwable e) {
                    Unsafe.free(newValueCountsAddr, (long) newBc * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    throw e;
                }
                if (blockValueCountsAddr != 0) {
                    Unsafe.free(blockValueCountsAddr, (long) blockCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                if (blockBitWidthsAddr != 0) {
                    Unsafe.free(blockBitWidthsAddr, (long) blockCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                blockValueCountsAddr = newValueCountsAddr;
                blockBitWidthsAddr = newBitWidthsAddr;
                blockCapacity = newBc;
            }
            if (residualsAddr == 0) {
                residualsAddr = Unsafe.malloc((long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                residualsCapacity = BLOCK_CAPACITY;
            }
            if (nativeResidualsAddr == 0 && PostingIndexNative.isNativeAvailable()) {
                nativeResidualsAddr = Unsafe.malloc((long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            long needed = computeMaxEncodedSize(count);
            if (needed > efTrialCapacity) {
                if (efTrialAddr != 0) {
                    Unsafe.free(efTrialAddr, efTrialCapacity, MemoryTag.NATIVE_INDEX_READER);
                }
                efTrialCapacity = needed;
                efTrialAddr = Unsafe.malloc(efTrialCapacity, MemoryTag.NATIVE_INDEX_READER);
            }
        }
    }

}

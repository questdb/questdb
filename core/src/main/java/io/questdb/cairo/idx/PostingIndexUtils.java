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

import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

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
 * packedOffsets[]      : blockCount × 8B  (byte offset from packed data start per block)
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
 *   [64-4087]   gen dir: up to 143 entries × 28B
 *   [4088-4095] sequence_end (8B) — must equal sequence_start for valid page
 * </pre>
 *
 * <h2>Dense generation (sealed) — stride-indexed</h2>
 * <pre>
 * [stride_index: (strideCount + 1) × 8B — byte offset per stride block]
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
 *   [offsets: (ks + 1) × 8B — byte-offset per key into stride data, sentinel at end]
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
 * [offsets: activeKeyCount × 8B — byte-offset per key into delta data]
 * [delta-encoded data per key]
 * </pre>
 */
public final class PostingIndexUtils {

    public static final int BLOCK_CAPACITY = 64;
    // .pci layout: magic(4B) + count(4B) + writerIndex[count](4B each, -1 = tombstoned).
    public static final int COVER_INFO_MAGIC = 0x50434931; // "PCI1"
    public static final int DENSE_STRIDE = 256;
    // Elias-Fano encoding: if the first 4 bytes of an encoded key blob equal this sentinel,
    // the blob uses Elias-Fano format. Otherwise, it is the legacy delta-FoR format.
    // Safe because delta-FoR's leading blockCount is always a small positive integer.
    public static final int EF_FORMAT_SENTINEL = Integer.MIN_VALUE; // 0x80000000
    public static final byte ENCODING_ADAPTIVE = 0;
    public static final byte ENCODING_DELTA = 1;
    public static final byte ENCODING_EF = 2;
    public static final int FORMAT_VERSION = 1;
    public static final int GEN_DIR_ENTRY_SIZE = 28;
    public static final int GEN_DIR_OFFSET_FILE_OFFSET = 0;
    public static final int GEN_DIR_OFFSET_KEY_COUNT = 16;
    public static final int GEN_DIR_OFFSET_MAX_KEY = 24;
    public static final int GEN_DIR_OFFSET_MIN_KEY = 20;
    public static final int GEN_DIR_OFFSET_SIZE = 8;
    public static final int KEY_FILE_RESERVED = 8192; // was 64
    public static final int LONG_OFFSETS_FLAG = 0x4000_0000;
    public static final int MAX_BLOCK_COUNT = 1_000_000; // corruption guard: 64M values at BLOCK_CAPACITY=64
    public static final int MAX_GEN_COUNT = 143; // (4088-64)/28 = 143 (entry 142 ends at 4068 <= 4088)
    public static final int PACKED_BATCH_SIZE = BLOCK_CAPACITY;
    public static final long PAGE_A_OFFSET = 0;
    public static final long PAGE_B_OFFSET = 4096;
    public static final int PAGE_OFFSET_BLOCK_CAPACITY = 16;
    public static final int PAGE_OFFSET_FORMAT_VERSION = 36;
    public static final int PAGE_OFFSET_GEN_COUNT = 32;
    public static final int PAGE_OFFSET_GEN_DIR = 64;
    public static final int PAGE_OFFSET_KEY_COUNT = 20;
    public static final int PAGE_OFFSET_MAX_VALUE = 24;
    // Per-page offsets
    public static final int PAGE_OFFSET_SEAL_TXN = 40; // 8 bytes: sealTxn — the sealed-version suffix used for .pv and .pc<N> filenames
    public static final int PAGE_OFFSET_SEQUENCE_END = 4088;
    public static final int PAGE_OFFSET_SEQUENCE_START = 0;
    public static final int PAGE_OFFSET_VALUE_MEM_SIZE = 8;
    // Double-buffered 4KB metadata pages (v2 format)
    public static final int PAGE_SIZE = 4096;
    public static final int PC_HEADER_SIZE = MAX_GEN_COUNT * Long.BYTES; // 1144
    public static final long SEAL_TXN_TENTATIVE = -1L;
    public static final byte SIGNATURE = (byte) 0xfb;
    public static final double SPARSE_SBBF_DEFAULT_FPP = 0.01;
    public static final int SPARSE_SBBF_NUM_BLOCKS_FOOTER_SIZE = Integer.BYTES;
    public static final int STRIDE_IDX_BYTES = Long.BYTES;
    // Stride block mode constants — see class javadoc for when each mode wins
    public static final byte STRIDE_MODE_DELTA = 0;
    public static final byte STRIDE_MODE_FLAT = 1;
    public static final int STRIDE_MODE_PREFIX_SIZE = 4; // mode(1B) + bitWidth/reserved(1B) + padding(2B)
    public static final int STRIDE_FLAT_BASE_OFFSET = STRIDE_MODE_PREFIX_SIZE; // baseValue(8B) follows mode prefix
    public static final int STRIDE_FLAT_PREFIX_COUNTS_OFFSET = STRIDE_MODE_PREFIX_SIZE + Long.BYTES; // = 12
    // EF header: sentinel(4B) + count(4B) + L(1B) + universe(8B) = 17B
    static final int EF_HEADER_SIZE = 17;
    private static final long PARSE_FAIL = Long.MIN_VALUE;
    // Bounded retry budget for the seqlock loop in readSealTxnFromKeyFd. The
    // writer always leaves one of the two pages stable across a write, so a
    // consistent snapshot is normally found within one or two iterations.
    // Eight attempts gives ample slack under heavy seal contention before the
    // function gives up and returns -1.
    private static final int SEAL_TXN_READ_ATTEMPTS = 8;

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
            int midKey = Unsafe.getInt(keyIdsAddr + (long) mid * Integer.BYTES);
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
        long packedOffsetsSize = blockCount > 1 ? (long) blockCount * Long.BYTES : 0;
        long deltaMax = 4 + (long) blockCount * 18 + packedOffsetsSize + totalDeltas * 8;
        return Math.max(efMax, deltaMax);
    }

    public static LPSZ coverDataFileName(
            Path path,
            CharSequence name,
            int includeIdx,
            long postingColumnNameTxn,
            long coveredColumnNameTxn,
            long sealTxn
    ) {
        path.concat(name).put(".pc").put(includeIdx);
        path.put('.').put(postingColumnNameTxn == COLUMN_NAME_TXN_NONE ? 0 : postingColumnNameTxn);
        if (coveredColumnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(coveredColumnNameTxn);
        }
        path.put('.').put(sealTxn);
        return path.$();
    }

    public static LPSZ coverInfoFileName(Path path, CharSequence name, long postingColumnNameTxn) {
        path.concat(name).put(".pci");
        if (postingColumnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(postingColumnNameTxn);
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
        int firstWord = Unsafe.getInt(srcAddr);
        if (firstWord == EF_FORMAT_SENTINEL) {
            decodeKeyEF(srcAddr, dest);
            return;
        }
        if (firstWord < 0 || firstWord > MAX_BLOCK_COUNT) {
            throw CairoException.critical(0).put("corrupt posting index: invalid blockCount [blockCount=").put(firstWord).put(']');
        }
        long pos = srcAddr + 4;

        // Read valueCounts[]
        int[] valueCounts = new int[firstWord];
        for (int b = 0; b < firstWord; b++) {
            valueCounts[b] = Unsafe.getByte(pos + b) & 0xFF;
        }
        pos += firstWord;

        long firstValuesAddr = 0;
        long minDeltasAddr = 0;
        long blockDeltasAddr = 0;
        try {
            // Read firstValues[] (off-heap)
            firstValuesAddr = Unsafe.malloc((long) firstWord * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            for (int b = 0; b < firstWord; b++) {
                Unsafe.putLong(firstValuesAddr + (long) b * Long.BYTES,
                        Unsafe.getLong(pos + (long) b * Long.BYTES));
            }
            pos += (long) firstWord * Long.BYTES;

            // Read minDeltas[] (off-heap)
            minDeltasAddr = Unsafe.malloc((long) firstWord * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            for (int b = 0; b < firstWord; b++) {
                Unsafe.putLong(minDeltasAddr + (long) b * Long.BYTES,
                        Unsafe.getLong(pos + (long) b * Long.BYTES));
            }
            pos += (long) firstWord * Long.BYTES;

            // Read bitWidths[]
            int[] bitWidths = new int[firstWord];
            for (int b = 0; b < firstWord; b++) {
                bitWidths[b] = Unsafe.getByte(pos + b) & 0xFF;
            }
            pos += firstWord;

            // Skip packedOffsets (only present for multi-block keys)
            if (firstWord > 1) {
                pos += (long) firstWord * Long.BYTES;
            }

            // Decode each block — only count-1 deltas are packed (first value is in firstValues[])
            int destIdx = 0;
            blockDeltasAddr = Unsafe.malloc((long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            for (int b = 0; b < firstWord; b++) {
                int count = valueCounts[b];
                int bitWidth = bitWidths[b];
                int numDeltas = count - 1;

                if (numDeltas > 0) {
                    long minD = Unsafe.getLong(minDeltasAddr + (long) b * Long.BYTES);
                    if (bitWidth == 0) {
                        for (int i = 0; i < numDeltas; i++) {
                            Unsafe.putLong(blockDeltasAddr + (long) i * Long.BYTES, minD);
                        }
                    } else {
                        BitpackUtils.unpackAllValues(pos, numDeltas, bitWidth, minD, blockDeltasAddr);
                    }
                }
                pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);

                // Cumulative sum from firstValue to reconstruct absolute values
                long cumulative = Unsafe.getLong(firstValuesAddr + (long) b * Long.BYTES);
                dest[destIdx++] = cumulative;
                for (int i = 0; i < numDeltas; i++) {
                    cumulative += Unsafe.getLong(blockDeltasAddr + (long) i * Long.BYTES);
                    dest[destIdx++] = cumulative;
                }
            }
        } finally {
            if (firstValuesAddr != 0) {
                Unsafe.free(firstValuesAddr, (long) firstWord * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            if (minDeltasAddr != 0) {
                Unsafe.free(minDeltasAddr, (long) firstWord * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            if (blockDeltasAddr != 0) {
                Unsafe.free(blockDeltasAddr, (long) BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
        }
    }

    /**
     * Decodes an Elias-Fano encoded key into a Java array.
     */
    public static void decodeKeyEF(long srcAddr, long[] dest) {
        long pos = srcAddr + 4; // skip sentinel
        int n = Unsafe.getInt(pos);
        pos += 4;
        int L = Unsafe.getByte(pos) & 0xFF;
        pos += 1;
        long u = Unsafe.getLong(pos);
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
            long word = Unsafe.getLong(highStart + (long) w * 8);
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
        int n = Unsafe.getInt(pos);
        pos += 4;
        int L = Unsafe.getByte(pos) & 0xFF;
        pos += 1;
        long u = Unsafe.getLong(pos);
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
            Unsafe.setMemory(destAddr, (long) n * Long.BYTES, (byte) 0);
        }

        // Pass 2: scan high bits, merge into destAddr via read-modify-write
        int outputIdx = 0;
        for (int w = 0; w < numHighWords && outputIdx < n; w++) {
            long word = Unsafe.getLong(highStart + (long) w * 8);
            if (word == 0) continue;
            long base = (long) w * 64 - outputIdx;
            while (word != 0 && outputIdx < n) {
                int trail = Long.numberOfTrailingZeros(word);
                long addr = destAddr + (long) outputIdx * 8;
                Unsafe.putLong(addr, Unsafe.getLong(addr) | ((base + trail) << L));
                outputIdx++;
                base--;
                word &= word - 1;
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
        int firstWord = Unsafe.getInt(srcAddr);
        if (firstWord == EF_FORMAT_SENTINEL) {
            decodeKeyEFToNative(srcAddr, destAddr);
            return;
        }
        if (firstWord < 0 || firstWord > MAX_BLOCK_COUNT) {
            throw CairoException.critical(0).put("corrupt posting index: invalid blockCount [blockCount=").put(firstWord).put(']');
        }
        long pos = srcAddr + 4;

        ctx.ensureCapacity(firstWord);
        long valueCountsAddr = ctx.valueCountsAddr;
        long firstValuesAddr = ctx.firstValuesAddr;
        long minDeltasAddr = ctx.minDeltasAddr;
        long bitWidthsAddr = ctx.bitWidthsAddr;
        long blockDeltasAddr = ctx.blockDeltasAddr;

        for (int b = 0; b < firstWord; b++) {
            Unsafe.putInt(valueCountsAddr + (long) b * Integer.BYTES, Unsafe.getByte(pos + b) & 0xFF);
        }
        pos += firstWord;

        for (int b = 0; b < firstWord; b++) {
            Unsafe.putLong(firstValuesAddr + (long) b * Long.BYTES,
                    Unsafe.getLong(pos + (long) b * Long.BYTES));
        }
        pos += (long) firstWord * Long.BYTES;

        for (int b = 0; b < firstWord; b++) {
            Unsafe.putLong(minDeltasAddr + (long) b * Long.BYTES,
                    Unsafe.getLong(pos + (long) b * Long.BYTES));
        }
        pos += (long) firstWord * Long.BYTES;

        for (int b = 0; b < firstWord; b++) {
            Unsafe.putInt(bitWidthsAddr + (long) b * Integer.BYTES, Unsafe.getByte(pos + b) & 0xFF);
        }
        pos += firstWord;

        // Skip packedOffsets (only present for multi-block keys)
        if (firstWord > 1) {
            pos += (long) firstWord * Long.BYTES;
        }

        int destIdx = 0;
        for (int b = 0; b < firstWord; b++) {
            int count = Unsafe.getInt(valueCountsAddr + (long) b * Integer.BYTES);
            int bitWidth = Unsafe.getInt(bitWidthsAddr + (long) b * Integer.BYTES);
            int numDeltas = count - 1;

            if (numDeltas > 0) {
                long minD = Unsafe.getLong(minDeltasAddr + (long) b * Long.BYTES);
                if (bitWidth == 0) {
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.putLong(blockDeltasAddr + (long) i * Long.BYTES, minD);
                    }
                } else {
                    BitpackUtils.unpackAllValues(pos, numDeltas, bitWidth, minD, blockDeltasAddr);
                }
            }
            pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);

            long cumulative = Unsafe.getLong(firstValuesAddr + (long) b * Long.BYTES);
            Unsafe.putLong(destAddr + (long) destIdx * Long.BYTES, cumulative);
            destIdx++;
            for (int i = 0; i < numDeltas; i++) {
                cumulative += Unsafe.getLong(blockDeltasAddr + (long) i * Long.BYTES);
                Unsafe.putLong(destAddr + (long) destIdx * Long.BYTES, cumulative);
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
            Unsafe.putInt(destAddr, 0);
            return 4;
        }
        if (useEliasFano) {
            // Copy values to deltasAddr for the EF trial encode, then use the
            // Java array for delta-FoR (encodeKeyDeltaFoR reads from the array,
            // not deltasAddr, avoiding aliasing issues).
            long srcAddr = ctx.deltasAddr;
            for (int i = 0; i < count; i++) {
                Unsafe.putLong(srcAddr + (long) i * Long.BYTES, values[i]);
            }
            int efSize = encodeKeyEF(srcAddr, count, ctx.efTrialAddr);
            int deltaSize = encodeKeyDeltaFoR(values, count, destAddr, ctx);
            if (efSize < deltaSize) {
                Unsafe.copyMemory(ctx.efTrialAddr, destAddr, efSize);
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
            Unsafe.putInt(destAddr, 0);
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
        Unsafe.putLong(deltasAddr, values[0]);
        for (int i = 1; i < count; i++) {
            Unsafe.putLong(deltasAddr + (long) i * Long.BYTES, values[i] - values[i - 1]);
        }

        // Per-block metadata
        for (int b = 0; b < blockCount; b++) {
            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;

            Unsafe.putInt(valueCountsAddr + (long) b * Integer.BYTES, blockSize);
            Unsafe.putLong(blockFirstValuesAddr + (long) b * Long.BYTES, values[blockStart]);

            // Compute min/max delta for deltas[blockStart+1..blockEnd-1] only.
            // delta[blockStart] is redundant — firstValues[b] already stores the absolute value.
            int numDeltas = blockSize - 1;
            long minD, maxD;
            if (numDeltas == 0) {
                minD = 0;
                maxD = 0;
            } else {
                minD = Unsafe.getLong(deltasAddr + (long) (blockStart + 1) * Long.BYTES);
                maxD = minD;
                for (int i = blockStart + 2; i < blockEnd; i++) {
                    long d = Unsafe.getLong(deltasAddr + (long) i * Long.BYTES);
                    if (d < minD) minD = d;
                    if (d > maxD) maxD = d;
                }
            }

            Unsafe.putLong(blockMinDeltasAddr + (long) b * Long.BYTES, minD);
            long range = maxD - minD;
            Unsafe.putInt(bitWidthsAddr + (long) b * Integer.BYTES, range == 0 ? 0 : BitpackUtils.bitsNeeded(range));
        }

        // Write encoded data
        long pos = destAddr;

        // blockCount (4B)
        Unsafe.putInt(pos, blockCount);
        pos += 4;

        // valueCounts[] (blockCount × 1B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.putByte(pos + b, (byte) Unsafe.getInt(valueCountsAddr + (long) b * Integer.BYTES));
        }
        pos += blockCount;

        // firstValues[] (blockCount × 8B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.putLong(pos + (long) b * Long.BYTES,
                    Unsafe.getLong(blockFirstValuesAddr + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        // minDeltas[] (blockCount × 8B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.putLong(pos + (long) b * Long.BYTES,
                    Unsafe.getLong(blockMinDeltasAddr + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        // bitWidths[] (blockCount × 1B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.putByte(pos + b, (byte) Unsafe.getInt(bitWidthsAddr + (long) b * Integer.BYTES));
        }
        pos += blockCount;

        // packedOffsets[] (only for multi-block keys)
        long packedOffsetsAddr = 0;
        if (blockCount > 1) {
            packedOffsetsAddr = pos;
            pos += (long) blockCount * Long.BYTES;
        }

        // Packed blocks — only pack the numDeltas=blockSize-1 inter-value deltas
        long packedDataStart = pos;
        for (int b = 0; b < blockCount; b++) {
            if (packedOffsetsAddr != 0) {
                Unsafe.putLong(packedOffsetsAddr + (long) b * Long.BYTES, pos - packedDataStart);
            }

            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;
            int numDeltas = blockSize - 1;
            int bitWidth = Unsafe.getInt(bitWidthsAddr + (long) b * Integer.BYTES);

            if (bitWidth > 0 && numDeltas > 0) {
                long minD = Unsafe.getLong(blockMinDeltasAddr + (long) b * Long.BYTES);
                if (ctx.nativeResidualsAddr != 0) {
                    long nrAddr = ctx.nativeResidualsAddr;
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.putLong(nrAddr + (long) i * Long.BYTES,
                                Unsafe.getLong(deltasAddr + (long) (blockStart + 1 + i) * Long.BYTES) - minD);
                    }
                    PostingIndexNative.packValuesNative(nrAddr, numDeltas, 0, bitWidth, pos);
                } else {
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.putLong(residualsAddr + (long) i * Long.BYTES,
                                Unsafe.getLong(deltasAddr + (long) (blockStart + 1 + i) * Long.BYTES) - minD);
                    }
                    PostingIndexNative.packValuesNativeFallback(residualsAddr, numDeltas, 0, bitWidth, pos);
                }
            }
            pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);
        }

        return (int) (pos - destAddr);
    }

    /**
     * Encodes sorted long values using Elias-Fano encoding directly from native memory.
     * Format: [sentinel:4B][count:4B][L:1B][universe:8B][lowBits][highBits]
     */
    public static int encodeKeyEF(long srcAddr, int count, long destAddr) {
        long lastValue = Unsafe.getLong(srcAddr + (long) (count - 1) * Long.BYTES);
        long u = lastValue + 1;
        int L = Math.max(0, 63 - Long.numberOfLeadingZeros(u / count));
        long lowMask = (1L << L) - 1;

        long pos = destAddr;
        Unsafe.putInt(pos, EF_FORMAT_SENTINEL);
        pos += 4;
        Unsafe.putInt(pos, count);
        pos += 4;
        Unsafe.putByte(pos, (byte) L);
        pos += 1;
        Unsafe.putLong(pos, u);
        pos += 8;

        long lowStart = pos;
        int lowBytes = efLowBytesAligned(count, L);
        for (long i = 0; i < lowBytes; i += 8) {
            Unsafe.putLong(lowStart + i, 0);
        }
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getLong(srcAddr + (long) i * Long.BYTES);
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
            Unsafe.putLong(highStart + i, 0);
        }
        long bitPos = 0;
        long prevHigh = 0;
        for (int i = 0; i < count; i++) {
            long val = Unsafe.getLong(srcAddr + (long) i * Long.BYTES);
            long high = val >>> L;
            bitPos += (high - prevHigh);
            setBitWord(highStart, bitPos);
            bitPos++;
            prevHigh = high;
        }
        pos += highBytes;

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
            Unsafe.putInt(destAddr, 0);
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
            Unsafe.putInt(destAddr, 0);
            return 4;
        }
        int efSize = encodeKeyEF(srcAddr, count, ctx.efTrialAddr);
        int deltaSize = encodeKeyNativeDeltaFoR(srcAddr, count, destAddr, ctx);
        if (efSize < deltaSize) {
            Unsafe.copyMemory(ctx.efTrialAddr, destAddr, efSize);
            return efSize;
        }
        return deltaSize;
    }

    /**
     * Legacy: encodes sorted values using delta + FoR64 bitpacking from native memory.
     */
    public static int encodeKeyNativeDeltaFoR(long srcAddr, int count, long destAddr, EncodeContext ctx) {
        if (count == 0) {
            Unsafe.putInt(destAddr, 0);
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
        long prev = Unsafe.getLong(srcAddr);
        Unsafe.putLong(deltasAddr, prev);
        for (int i = 1; i < count; i++) {
            long val = Unsafe.getLong(srcAddr + (long) i * Long.BYTES);
            Unsafe.putLong(deltasAddr + (long) i * Long.BYTES, val - prev);
            prev = val;
        }

        // Per-block metadata
        for (int b = 0; b < blockCount; b++) {
            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;

            Unsafe.putInt(valueCountsAddr + (long) b * Integer.BYTES, blockSize);
            Unsafe.putLong(blockFirstValuesAddr + (long) b * Long.BYTES,
                    Unsafe.getLong(srcAddr + (long) blockStart * Long.BYTES));

            int numDeltas = blockSize - 1;
            long minD, maxD;
            if (numDeltas == 0) {
                minD = 0;
                maxD = 0;
            } else {
                minD = Unsafe.getLong(deltasAddr + (long) (blockStart + 1) * Long.BYTES);
                maxD = minD;
                for (int i = blockStart + 2; i < blockEnd; i++) {
                    long d = Unsafe.getLong(deltasAddr + (long) i * Long.BYTES);
                    if (d < minD) minD = d;
                    if (d > maxD) maxD = d;
                }
            }

            Unsafe.putLong(blockMinDeltasAddr + (long) b * Long.BYTES, minD);
            long range = maxD - minD;
            Unsafe.putInt(bitWidthsAddr + (long) b * Integer.BYTES, range == 0 ? 0 : BitpackUtils.bitsNeeded(range));
        }

        // Write encoded data
        long pos = destAddr;

        Unsafe.putInt(pos, blockCount);
        pos += 4;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.putByte(pos + b, (byte) Unsafe.getInt(valueCountsAddr + (long) b * Integer.BYTES));
        }
        pos += blockCount;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.putLong(pos + (long) b * Long.BYTES,
                    Unsafe.getLong(blockFirstValuesAddr + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.putLong(pos + (long) b * Long.BYTES,
                    Unsafe.getLong(blockMinDeltasAddr + (long) b * Long.BYTES));
        }
        pos += (long) blockCount * Long.BYTES;

        for (int b = 0; b < blockCount; b++) {
            Unsafe.putByte(pos + b, (byte) Unsafe.getInt(bitWidthsAddr + (long) b * Integer.BYTES));
        }
        pos += blockCount;

        // Packed data offsets: byte offset from packed data start to each block's packed data.
        // Enables O(1) random access to any block without forward scanning.
        long packedOffsetsAddr = pos;
        pos += (long) blockCount * Long.BYTES;

        long packedDataStart = pos;
        for (int b = 0; b < blockCount; b++) {
            Unsafe.putLong(packedOffsetsAddr + (long) b * Long.BYTES, pos - packedDataStart);

            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;
            int numDeltas = blockSize - 1;
            int bitWidth = Unsafe.getInt(bitWidthsAddr + (long) b * Integer.BYTES);

            if (bitWidth > 0 && numDeltas > 0) {
                long minD = Unsafe.getLong(blockMinDeltasAddr + (long) b * Long.BYTES);
                if (ctx.nativeResidualsAddr != 0) {
                    long nrAddr = ctx.nativeResidualsAddr;
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.putLong(nrAddr + (long) i * Long.BYTES,
                                Unsafe.getLong(deltasAddr + (long) (blockStart + 1 + i) * Long.BYTES) - minD);
                    }
                    PostingIndexNative.packValuesNative(nrAddr, numDeltas, 0, bitWidth, pos);
                } else {
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.putLong(residualsAddr + (long) i * Long.BYTES,
                                Unsafe.getLong(deltasAddr + (long) (blockStart + 1 + i) * Long.BYTES) - minD);
                    }
                    PostingIndexNative.packValuesNativeFallback(residualsAddr, numDeltas, 0, bitWidth, pos);
                }
            }
            pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);
        }

        return (int) (pos - destAddr);
    }

    /**
     * Size of the per-generation sparse header: keyIds + counts + offsets for active keys only.
     * keyIds and counts stay at 4B per entry; offsets are 8B so per-key data offsets into
     * the sparse gen's delta region can address >2 GB without int truncation.
     */
    public static int genHeaderSizeSparse(int activeKeyCount) {
        return activeKeyCount * (Integer.BYTES + Integer.BYTES + Long.BYTES);
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
     * Builds the full path to the key file (.pk).
     * <p>
     * Filename format: <code>&lt;name&gt;.pk.&lt;postingColumnNameTxn&gt;</code>.
     * The .pk file is per-column-instance and never seal-versioned; it stores the
     * current {@code sealTxn} as a metadata field so readers can locate the live
     * .pv and .pc&lt;N&gt; files.
     */
    public static LPSZ keyFileName(Path path, CharSequence name, long postingColumnNameTxn) {
        path.concat(name).put(".pk");
        if (postingColumnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(postingColumnNameTxn);
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

    /**
     * Same as {@link #readSealTxnFromKeyFile} but operates on an already-open file
     * descriptor. Useful when the caller has just opened the .pk file for write
     * and would otherwise pay an extra openRO/close pair.
     * <p>
     * Reads the four seqlock fields, picks the consistent page, reads SEAL_TXN,
     * then re-reads SEQUENCE_START / SEQUENCE_END on the chosen page to confirm
     * a writer did not begin a new generation between the seqlock check and the
     * SEAL_TXN read. The two-page seqlock guarantees one of A or B is always
     * stable across a single write, so a consistent snapshot is normally found
     * within one or two iterations. Mirrors the in-process post-validation in
     * {@code AbstractPostingIndexReader#readIndexMetadataFromBestPage}.
     * <p>
     * Returns -1 if the file is too short, both metadata pages fail seq-lock
     * validation, or every retry observes a torn read.
     */
    public static long readSealTxnFromKeyFd(FilesFacade ff, long keyFd) {
        long fileSize = ff.length(keyFd);
        if (fileSize < KEY_FILE_RESERVED) {
            return -1;
        }
        for (int attempt = 0; attempt < SEAL_TXN_READ_ATTEMPTS; attempt++) {
            long seqA = ff.readNonNegativeLong(keyFd, PAGE_A_OFFSET + PAGE_OFFSET_SEQUENCE_START);
            long seqEndA = ff.readNonNegativeLong(keyFd, PAGE_A_OFFSET + PAGE_OFFSET_SEQUENCE_END);
            long seqB = ff.readNonNegativeLong(keyFd, PAGE_B_OFFSET + PAGE_OFFSET_SEQUENCE_START);
            long seqEndB = ff.readNonNegativeLong(keyFd, PAGE_B_OFFSET + PAGE_OFFSET_SEQUENCE_END);

            long pageOffset;
            long expectedSeq;
            if (seqA == seqEndA && seqB == seqEndB) {
                if (seqA >= seqB) {
                    pageOffset = PAGE_A_OFFSET;
                    expectedSeq = seqA;
                } else {
                    pageOffset = PAGE_B_OFFSET;
                    expectedSeq = seqB;
                }
            } else if (seqA == seqEndA) {
                pageOffset = PAGE_A_OFFSET;
                expectedSeq = seqA;
            } else if (seqB == seqEndB) {
                pageOffset = PAGE_B_OFFSET;
                expectedSeq = seqB;
            } else {
                // Both pages mid-write — uncommon; retry.
                continue;
            }

            long sealTxn = ff.readNonNegativeLong(keyFd, pageOffset + PAGE_OFFSET_SEAL_TXN);

            // Post-check: re-read seqStart/seqEnd on the chosen page. If
            // either differs from the pre-read seq, the writer started a
            // new generation between the seqlock check and the SEAL_TXN
            // read, so sealTxn may be from that uncommitted generation.
            long postSeqStart = ff.readNonNegativeLong(keyFd, pageOffset + PAGE_OFFSET_SEQUENCE_START);
            long postSeqEnd = ff.readNonNegativeLong(keyFd, pageOffset + PAGE_OFFSET_SEQUENCE_END);
            if (postSeqStart == expectedSeq && postSeqEnd == expectedSeq) {
                return sealTxn;
            }
        }
        return -1;
    }

    /**
     * Reads the current {@code sealTxn} from a .pk file. Returns -1 if the file
     * cannot be opened or both metadata pages fail the seq-lock validation.
     * <p>
     * Callers use this when they need to construct the path of the live .pv or
     * .pc&lt;N&gt; files but only have the {@code postingColumnNameTxn}.
     */
    public static long readSealTxnFromKeyFile(FilesFacade ff, LPSZ keyFilePath) {
        long fd = ff.openRO(keyFilePath);
        if (fd < 0) {
            return -1;
        }
        try {
            return readSealTxnFromKeyFd(ff, fd);
        } finally {
            ff.close(fd);
        }
    }

    /**
     * Removes the .pci file plus every sealed .pv.* and .pc&lt;N&gt;.*.* file that
     * belongs to the given posting-column-name txn.
     *
     * @return {@code true} when at least one file failed to remove and is
     * still on disk afterwards. The caller must treat this as "purge could
     * not complete" and retry — otherwise the surviving sidecar files leak
     * permanently.
     */
    public static boolean removeAllSealedFiles(
            FilesFacade ff,
            Path path,
            int pathTrimTo,
            CharSequence columnName,
            long postingColumnNameTxn
    ) {
        final boolean[] anyFailed = {false};
        LPSZ pciFile = coverInfoFileName(path.trimTo(pathTrimTo), columnName, postingColumnNameTxn);
        if (ff.exists(pciFile) && !ff.removeQuiet(pciFile) && ff.exists(pciFile)) {
            anyFailed[0] = true;
        }
        path.trimTo(pathTrimTo);
        scanSealedFiles(ff, path, pathTrimTo, columnName, new SealedFileVisitor() {
            @Override
            public void onCoverDataFile(int includeIdx, long filePostingColumnNameTxn, long coveredColumnNameTxn, long sealTxn) {
                if (filePostingColumnNameTxn != postingColumnNameTxn) {
                    return;
                }
                LPSZ p = coverDataFileName(path.trimTo(pathTrimTo), columnName, includeIdx, filePostingColumnNameTxn, coveredColumnNameTxn, sealTxn);
                if (!ff.removeQuiet(p) && ff.exists(p)) {
                    anyFailed[0] = true;
                }
                path.trimTo(pathTrimTo);
            }

            @Override
            public void onValueFile(long fileColumnNameTxn, long sealTxn) {
                if (fileColumnNameTxn != postingColumnNameTxn) {
                    return;
                }
                LPSZ p = valueFileName(path.trimTo(pathTrimTo), columnName, fileColumnNameTxn, sealTxn);
                if (!ff.removeQuiet(p) && ff.exists(p)) {
                    anyFailed[0] = true;
                }
                path.trimTo(pathTrimTo);
            }
        });
        path.trimTo(pathTrimTo);
        return anyFailed[0];
    }

    public static void scanSealedFiles(
            FilesFacade ff,
            Path path,
            int pathTrimTo,
            CharSequence columnName,
            SealedFileVisitor visitor
    ) {
        path.trimTo(pathTrimTo);
        StringSink fileName = Misc.getThreadLocalSink();
        ff.iterateDir(path.$(), (pUtf8NameZ, type) -> {
            if (type != Files.DT_FILE && type != Files.DT_LNK && type != Files.DT_UNKNOWN) {
                return;
            }
            fileName.clear();
            Utf8s.utf8ToUtf16Z(pUtf8NameZ, fileName);
            parseSealedFileName(fileName, columnName, visitor);
        });
        path.trimTo(pathTrimTo);
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
        // 4B mode prefix + counts[ks] x 4B + offsets[ks+1] x 8B.
        // offsets are 8B so total stride_data (sum of 256 per-key blobs) can
        // exceed 2 GB without int truncation; counts stay at 4B since each is
        // a single-key value count.
        return STRIDE_MODE_PREFIX_SIZE + keysInStride * Integer.BYTES + (keysInStride + 1) * Long.BYTES;
    }

    /**
     * Size of a flat-mode stride block header: mode prefix + baseValue(8B) + prefixCounts.
     */
    public static int strideFlatHeaderSize(int keysInStride) {
        return STRIDE_FLAT_PREFIX_COUNTS_OFFSET + (keysInStride + 1) * Integer.BYTES;
    }

    /**
     * Size of the stride index: (strideCount + 1) x STRIDE_IDX_BYTES.
     * The extra entry is a sentinel holding the total size of all stride blocks.
     * Entries are 8B so offsets survive sealed segments &gt;= 2 GB.
     */
    public static int strideIndexSize(int keyCount) {
        return (strideCount(keyCount) + 1) * STRIDE_IDX_BYTES;
    }

    // ==================================================================================
    // Elias-Fano encode/decode
    // ==================================================================================

    /**
     * Builds the full path to a sealed value file (.pv).
     * <p>
     * Filename: <code>&lt;name&gt;.pv.&lt;postingColumnNameTxn&gt;.&lt;sealTxn&gt;</code>,
     * with {@code postingColumnNameTxn} omitted when it is
     * {@code COLUMN_NAME_TXN_NONE}. {@code sealTxn} is a monotonic counter:
     * 0 in the pre-seal initial state, N after N seals.
     */
    public static LPSZ valueFileName(Path path, CharSequence name, long postingColumnNameTxn, long sealTxn) {
        path.concat(name).put(".pv");
        // Match .pk / .pci / BITMAP convention: skip the column-name txn when
        // it is COLUMN_NAME_TXN_NONE. sealTxn is always present so two seals
        // of the same column produce distinct filenames.
        if (postingColumnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(postingColumnNameTxn);
        }
        path.put('.').put(sealTxn);
        return path.$();
    }

    /**
     * Optimized single-block encode for keys with count <= BLOCK_CAPACITY.
     * Single pass: reads values, computes deltas inline, finds min/max,
     * writes metadata and packs residuals without per-block loop overhead.
     */
    private static int encodeKeyNativeSingleBlock(long srcAddr, int count, long destAddr, EncodeContext ctx) {
        int numDeltas = count - 1;
        long firstValue = Unsafe.getLong(srcAddr);

        // Compute deltas and find min/max in a single pass
        long minD = Long.MAX_VALUE;
        long maxD = Long.MIN_VALUE;
        long deltasAddr = ctx.deltasAddr;

        long prev = firstValue;
        for (int i = 1; i < count; i++) {
            long val = Unsafe.getLong(srcAddr + (long) i * Long.BYTES);
            long d = val - prev;
            Unsafe.putLong(deltasAddr + (long) i * Long.BYTES, d);
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
        Unsafe.putInt(pos, 1);
        pos += 4;
        Unsafe.putByte(pos, (byte) count);
        pos += 1;
        Unsafe.putLong(pos, firstValue);
        pos += 8;
        Unsafe.putLong(pos, minD);
        pos += 8;
        Unsafe.putByte(pos, (byte) bitWidth);
        pos += 1;

        if (bitWidth > 0 && numDeltas > 0) {
            if (ctx.nativeResidualsAddr != 0) {
                long nrAddr = ctx.nativeResidualsAddr;
                for (int i = 0; i < numDeltas; i++) {
                    Unsafe.putLong(nrAddr + (long) i * Long.BYTES,
                            Unsafe.getLong(deltasAddr + (long) (i + 1) * Long.BYTES) - minD);
                }
                PostingIndexNative.packValuesNative(nrAddr, numDeltas, 0, bitWidth, pos);
            } else {
                long residualsAddr = ctx.residualsAddr;
                for (int i = 0; i < numDeltas; i++) {
                    Unsafe.putLong(residualsAddr + (long) i * Long.BYTES,
                            Unsafe.getLong(deltasAddr + (long) (i + 1) * Long.BYTES) - minD);
                }
                PostingIndexNative.packValuesNativeFallback(residualsAddr, numDeltas, 0, bitWidth, pos);
            }
        }
        pos += BitpackUtils.packedDataSize(numDeltas, bitWidth);

        return (int) (pos - destAddr);
    }

    private static int nextDot(CharSequence s, int from, int end) {
        for (int i = from; i < end; i++) {
            if (s.charAt(i) == '.') {
                return i;
            }
        }
        return -1;
    }

    /**
     * Parses a directory entry name and dispatches to the visitor if it matches
     * a sealed-file pattern for {@code columnName}.
     * <p>
     * Recognised patterns (segment indices use 0-based dot splits):
     * <ul>
     *   <li>{@code <columnName>.pv.<postingColumnNameTxn>.<sealTxn>} — value file</li>
     *   <li>{@code <columnName>.pc<includeIdx>.<coveredColumnNameTxn>.<sealTxn>} — cover data file</li>
     * </ul>
     * Files that do not match (different column, .pk, .pci, partition data, etc.)
     * are silently ignored.
     */
    private static void parseSealedFileName(CharSequence fileName, CharSequence columnName, SealedFileVisitor visitor) {
        int nameLen = columnName.length();
        int fileLen = fileName.length();
        if (fileLen < nameLen + 4) {
            return;
        }
        for (int i = 0; i < nameLen; i++) {
            if (fileName.charAt(i) != columnName.charAt(i)) {
                return;
            }
        }
        if (fileName.charAt(nameLen) != '.') {
            return;
        }
        int pos = nameLen + 1;
        if (pos + 1 >= fileLen || fileName.charAt(pos) != 'p') {
            return;
        }
        char kind = fileName.charAt(pos + 1);
        if (kind != 'v' && kind != 'c') {
            return;
        }
        pos += 2;
        int includeIdx = -1;
        if (kind == 'c') {
            int idxStart = pos;
            while (pos < fileLen && fileName.charAt(pos) != '.') {
                char ch = fileName.charAt(pos);
                if (ch < '0' || ch > '9') {
                    return;
                }
                pos++;
            }
            if (pos == idxStart || pos >= fileLen) {
                return;
            }
            try {
                includeIdx = Numbers.parseInt(fileName, idxStart, pos);
            } catch (NumericException e) {
                return;
            }
        }
        if (pos >= fileLen || fileName.charAt(pos) != '.') {
            return;
        }
        pos++;

        if (kind == 'v') {
            int dot = nextDot(fileName, pos, fileLen);
            long columnNameTxn;
            long sealTxn;
            if (dot < 0) {
                columnNameTxn = COLUMN_NAME_TXN_NONE;
                sealTxn = parseTxnSegment(fileName, pos, fileLen);
            } else {
                columnNameTxn = parseTxnSegment(fileName, pos, dot);
                sealTxn = parseTxnSegment(fileName, dot + 1, fileLen);
            }
            if (columnNameTxn == PARSE_FAIL || sealTxn == PARSE_FAIL) {
                return;
            }
            visitor.onValueFile(columnNameTxn, sealTxn);
            return;
        }

        int hostDot = nextDot(fileName, pos, fileLen);
        if (hostDot < 0) {
            return;
        }
        long rawHost = parseTxnSegment(fileName, pos, hostDot);
        if (rawHost == PARSE_FAIL) {
            return;
        }
        long postingColumnNameTxn = rawHost == 0 ? COLUMN_NAME_TXN_NONE : rawHost;
        long coveredColumnNameTxn;
        long sealTxn;
        int secondDot = nextDot(fileName, hostDot + 1, fileLen);
        if (secondDot < 0) {
            coveredColumnNameTxn = COLUMN_NAME_TXN_NONE;
            sealTxn = parseTxnSegment(fileName, hostDot + 1, fileLen);
        } else {
            coveredColumnNameTxn = parseTxnSegment(fileName, hostDot + 1, secondDot);
            sealTxn = parseTxnSegment(fileName, secondDot + 1, fileLen);
        }
        if (coveredColumnNameTxn == PARSE_FAIL || sealTxn == PARSE_FAIL) {
            return;
        }
        visitor.onCoverDataFile(includeIdx, postingColumnNameTxn, coveredColumnNameTxn, sealTxn);
    }

    private static long parseTxnSegment(CharSequence s, int from, int end) {
        if (end <= from) {
            return PARSE_FAIL;
        }
        try {
            return Numbers.parseLong(s, from, end);
        } catch (NumericException e) {
            return PARSE_FAIL;
        }
    }

    private static void setBitWord(long baseAddr, long bitPos) {
        long wordAddr = baseAddr + ((bitPos >>> 6) << 3);
        int bitOffset = (int) (bitPos & 63);
        long word = Unsafe.getLong(wordAddr);
        Unsafe.putLong(wordAddr, word | (1L << bitOffset));
    }

    // ==================================================================================
    // Elias-Fano support methods for readers
    // ==================================================================================

    private static void writeBitsWord(long baseAddr, long bitPos, long value, int numBits) {
        if (numBits == 0) return;
        long wordAddr = baseAddr + ((bitPos >>> 6) << 3);
        int bitOffset = (int) (bitPos & 63);
        long mask = (numBits < 64) ? (1L << numBits) - 1 : -1L;
        long word = Unsafe.getLong(wordAddr);
        word = (word & ~(mask << bitOffset)) | ((value & mask) << bitOffset);
        Unsafe.putLong(wordAddr, word);
        if (bitOffset + numBits > 64) {
            long word2 = Unsafe.getLong(wordAddr + 8);
            int bitsInSecond = bitOffset + numBits - 64;
            long mask2 = (1L << bitsInSecond) - 1;
            word2 = (word2 & ~mask2) | ((value >>> (64 - bitOffset)) & mask2);
            Unsafe.putLong(wordAddr + 8, word2);
        }
    }


    // ==================================================================================
    // Bit manipulation helpers for Elias-Fano
    // ==================================================================================

    /**
     * Computes 8-byte-aligned size for the EF low bits region.
     */
    static int efLowBytesAligned(int n, int L) {
        return (int) ((((long) n * L + 63) >>> 6) << 3);
    }

    /**
     * Reads numBits starting at bitPos using 64-bit aligned loads.
     */
    static long readBitsWord(long baseAddr, long bitPos, int numBits) {
        if (numBits == 0) return 0;
        long wordAddr = baseAddr + ((bitPos >>> 6) << 3);
        int bitOffset = (int) (bitPos & 63);
        long word = Unsafe.getLong(wordAddr);
        long value = word >>> bitOffset;
        if (bitOffset + numBits > 64) {
            value |= Unsafe.getLong(wordAddr + 8) << (64 - bitOffset);
        }
        return (numBits < 64) ? value & ((1L << numBits) - 1) : value;
    }

    /**
     * Receives one callback per sealed file discovered by {@link #scanSealedFiles}.
     * Implementations are typically used to enumerate orphan files for purge or
     * crash recovery.
     */
    public interface SealedFileVisitor {

        /**
         * Called for each {@code .pc<includeIdx>.<postingColumnNameTxn>.<coveredColumnNameTxn>.<sealTxn>} file.
         */
        void onCoverDataFile(int includeIdx, long postingColumnNameTxn, long coveredColumnNameTxn, long sealTxn);

        /**
         * Called for each {@code .pv.<postingColumnNameTxn>.<sealTxn>} file.
         */
        void onValueFile(long postingColumnNameTxn, long sealTxn);
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

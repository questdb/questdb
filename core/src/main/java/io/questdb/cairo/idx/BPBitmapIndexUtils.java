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

import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Constants and encoding/decoding utilities for Delta + FoR64 BitPacking (BP) bitmap index.
 * <p>
 * Combines delta encoding with Frame-of-Reference compression using 64-tuple blocks.
 * For sorted postings: delta → split into blocks of 64 → per-block FoR bitpacking.
 * <p>
 * Per-key encoded data layout:
 * <pre>
 * blockCount           : 2B
 * valueCounts[]        : blockCount × 1B  (1–64 per block)
 * firstValues[]        : blockCount × 8B  (first absolute value per block)
 * minDeltas[]          : blockCount × 8B  (FoR reference per block)
 * bitWidths[]          : blockCount × 1B
 * packedBlock[0..n-1]  : variable size bitpacked residuals
 * </pre>
 * <p>
 * Key File Layout (.bk):
 * <pre>
 * [Header 64B: sig(0xfb), seq, valMemSize, blockCapacity, keyCount, seqCheck, maxVal, genCount]
 * [Generation directory: genCount × 24B (offset(8), size(4), keyCount(4), minKey(4), maxKey(4))]
 * </pre>
 * <p>
 * Value File Layout (.bv):
 * <pre>
 * [Generation 0 data]
 * [Generation 1 data]
 * ...
 * </pre>
 * <p>
 * Generation Format (one per commit, covers all keys):
 * <p>
 * Dense format (genKeyCount >= 0 in directory, used by seal) — stride-indexed:
 * <pre>
 * [stride_index: (strideCount + 1) × 4B — byte offset of each stride block]
 * [stride block 0:
 *   [counts:  ks × 4B — value count per key]
 *   [offsets: (ks + 1) × 4B — prefix-sum data offsets, sentinel at end]
 *   [encoded data for keys 0..DENSE_STRIDE-1]
 * ]
 * [stride block 1: ...]
 * ...
 * [stride block N-1: last stride may have fewer keys]
 * </pre>
 * where strideCount = ceil(keyCount / DENSE_STRIDE), ks = keys in stride.
 * Sparse format (genKeyCount < 0 in directory, |genKeyCount| = active keys, used by commit):
 * <pre>
 * [keyIds:  activeKeyCount × 4B — sorted ascending, for binary search]
 * [counts:  activeKeyCount × 4B — indexed by position in keyIds]
 * [offsets: activeKeyCount × 4B — indexed by position in keyIds]
 * [Key data...]
 * </pre>
 */
public final class BPBitmapIndexUtils {

    public static final int BLOCK_CAPACITY = 64;
    public static final int DENSE_STRIDE = 256;

    // Key file header offsets (64 bytes)
    public static final int KEY_FILE_RESERVED = 64;
    public static final int KEY_RESERVED_OFFSET_SIGNATURE = 0;
    public static final int KEY_RESERVED_OFFSET_SEQUENCE = 8;
    public static final int KEY_RESERVED_OFFSET_VALUE_MEM_SIZE = 16;
    public static final int KEY_RESERVED_OFFSET_BLOCK_CAPACITY = 24;
    public static final int KEY_RESERVED_OFFSET_KEY_COUNT = 28;
    public static final int KEY_RESERVED_OFFSET_SEQUENCE_CHECK = 32;
    public static final int KEY_RESERVED_OFFSET_MAX_VALUE = 40;
    public static final int KEY_RESERVED_OFFSET_GEN_COUNT = 48;

    // Generation directory entry (24 bytes per generation)
    public static final int GEN_DIR_ENTRY_SIZE = 24;
    public static final int GEN_DIR_OFFSET_FILE_OFFSET = 0;
    public static final int GEN_DIR_OFFSET_SIZE = 8;
    public static final int GEN_DIR_OFFSET_KEY_COUNT = 12;
    public static final int GEN_DIR_OFFSET_MIN_KEY = 16;
    public static final int GEN_DIR_OFFSET_MAX_KEY = 20;

    public static final byte SIGNATURE = (byte) 0xfb;

    private BPBitmapIndexUtils() {
    }

    /**
     * Computes worst-case encoded size for a key with {@code count} values.
     * Used to pre-allocate encode buffers.
     */
    public static int computeMaxEncodedSize(int count) {
        int blockCount = (count + BLOCK_CAPACITY - 1) / BLOCK_CAPACITY;
        // 2B blockCount + blockCount * (1B valueCount + 8B firstValue + 8B minDelta + 1B bitWidth)
        // + blockCount * 63 * 8 bytes worst case packed data (63 deltas × 64 bits; first value per block is in firstValues[])
        return 2 + blockCount * 18 + blockCount * (BLOCK_CAPACITY - 1) * 8;
    }

    /**
     * Decodes all values for a key from BP-encoded data.
     *
     * @param srcAddr    address of the encoded data for this key
     * @param totalCount total number of values expected
     * @param dest       destination array (must have room for totalCount values)
     */
    public static void decodeKey(long srcAddr, int totalCount, long[] dest) {
        int blockCount = Unsafe.getUnsafe().getShort(srcAddr) & 0xFFFF;
        long pos = srcAddr + 2;

        // Read valueCounts[]
        int[] valueCounts = new int[blockCount];
        for (int b = 0; b < blockCount; b++) {
            valueCounts[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
        }
        pos += blockCount;

        // Read firstValues[]
        long[] firstValues = new long[blockCount];
        for (int b = 0; b < blockCount; b++) {
            firstValues[b] = Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES);
        }
        pos += (long) blockCount * Long.BYTES;

        // Read minDeltas[]
        long[] minDeltas = new long[blockCount];
        for (int b = 0; b < blockCount; b++) {
            minDeltas[b] = Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES);
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
        long[] blockDeltas = new long[BLOCK_CAPACITY];
        for (int b = 0; b < blockCount; b++) {
            int count = valueCounts[b];
            int bitWidth = bitWidths[b];
            int numDeltas = count - 1;

            if (numDeltas > 0) {
                if (bitWidth == 0) {
                    for (int i = 0; i < numDeltas; i++) {
                        blockDeltas[i] = minDeltas[b];
                    }
                } else {
                    FORBitmapIndexUtils.unpackAllValues(pos, numDeltas, bitWidth, minDeltas[b], blockDeltas);
                }
            }
            pos += FORBitmapIndexUtils.packedDataSize(numDeltas, bitWidth);

            // Cumulative sum from firstValue to reconstruct absolute values
            long cumulative = firstValues[b];
            dest[destIdx++] = cumulative;
            for (int i = 0; i < numDeltas; i++) {
                cumulative += blockDeltas[i];
                dest[destIdx++] = cumulative;
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
        ctx.ensureCapacity(count);
        return encodeKey(values, count, destAddr, ctx);
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
            Unsafe.getUnsafe().putShort(destAddr, (short) 0);
            return 2;
        }

        int blockCount = (count + BLOCK_CAPACITY - 1) / BLOCK_CAPACITY;
        long[] deltas = ctx.deltas;
        int[] valueCounts = ctx.blockValueCounts;
        long[] firstValues = ctx.blockFirstValues;
        long[] minDeltas = ctx.blockMinDeltas;
        int[] bitWidths = ctx.blockBitWidths;
        long[] residuals = ctx.residuals;

        // Compute deltas
        deltas[0] = values[0];
        for (int i = 1; i < count; i++) {
            deltas[i] = values[i] - values[i - 1];
        }

        // Per-block metadata
        for (int b = 0; b < blockCount; b++) {
            int blockStart = b * BLOCK_CAPACITY;
            int blockEnd = Math.min(blockStart + BLOCK_CAPACITY, count);
            int blockSize = blockEnd - blockStart;

            valueCounts[b] = blockSize;
            firstValues[b] = values[blockStart];

            // Compute min/max delta for deltas[blockStart+1..blockEnd-1] only.
            // delta[blockStart] is redundant — firstValues[b] already stores the absolute value.
            int numDeltas = blockSize - 1;
            long minD, maxD;
            if (numDeltas == 0) {
                minD = 0;
                maxD = 0;
            } else {
                minD = deltas[blockStart + 1];
                maxD = deltas[blockStart + 1];
                for (int i = blockStart + 2; i < blockEnd; i++) {
                    if (deltas[i] < minD) minD = deltas[i];
                    if (deltas[i] > maxD) maxD = deltas[i];
                }
            }

            minDeltas[b] = minD;
            long range = maxD - minD;
            bitWidths[b] = range == 0 ? 0 : FORBitmapIndexUtils.bitsNeeded(range);
        }

        // Write encoded data
        long pos = destAddr;

        // blockCount (2B)
        Unsafe.getUnsafe().putShort(pos, (short) blockCount);
        pos += 2;

        // valueCounts[] (blockCount × 1B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putByte(pos + b, (byte) valueCounts[b]);
        }
        pos += blockCount;

        // firstValues[] (blockCount × 8B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putLong(pos + (long) b * Long.BYTES, firstValues[b]);
        }
        pos += (long) blockCount * Long.BYTES;

        // minDeltas[] (blockCount × 8B)
        for (int b = 0; b < blockCount; b++) {
            Unsafe.getUnsafe().putLong(pos + (long) b * Long.BYTES, minDeltas[b]);
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
                for (int i = 0; i < numDeltas; i++) {
                    residuals[i] = deltas[blockStart + 1 + i] - minDeltas[b];
                }
                FORBitmapIndexUtils.packValues(residuals, numDeltas, 0, bitWidth, pos);
            }
            pos += FORBitmapIndexUtils.packedDataSize(numDeltas, bitWidth);
        }

        return (int) (pos - destAddr);
    }

    /**
     * Reusable workspace for encodeKey to avoid per-call allocations.
     */
    public static class EncodeContext {
        long[] deltas;
        int[] blockValueCounts;
        long[] blockFirstValues;
        long[] blockMinDeltas;
        int[] blockBitWidths;
        long[] residuals = new long[BLOCK_CAPACITY];
        private int deltaCapacity;
        private int blockCapacity;

        public void ensureCapacity(int count) {
            if (count > deltaCapacity) {
                deltaCapacity = Math.max(count, deltaCapacity * 2);
                deltas = new long[deltaCapacity];
            }
            int bc = (count + BLOCK_CAPACITY - 1) / BLOCK_CAPACITY;
            if (bc > blockCapacity) {
                blockCapacity = Math.max(bc, blockCapacity * 2);
                blockValueCounts = new int[blockCapacity];
                blockFirstValues = new long[blockCapacity];
                blockMinDeltas = new long[blockCapacity];
                blockBitWidths = new int[blockCapacity];
            }
        }
    }

    /**
     * Offset of generation directory entry in the key file (no symbol table).
     */
    public static long getGenDirOffset(int genIndex) {
        return KEY_FILE_RESERVED + (long) genIndex * GEN_DIR_ENTRY_SIZE;
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
     * Size of a stride block's local header: counts + prefix-sum offsets (with sentinel).
     */
    public static int strideLocalHeaderSize(int keysInStride) {
        return keysInStride * Integer.BYTES + (keysInStride + 1) * Integer.BYTES;
    }

    /**
     * Size of the per-generation dense header: counts + offsets for all keys.
     * Only used by legacy flat dense format — stride-indexed format uses stride blocks instead.
     */
    public static int genHeaderSize(int keyCount) {
        return keyCount * Integer.BYTES * 2;
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

    /**
     * Linear scan for a key ID starting from a hint position in a sorted keyIds array.
     * For sequential ascending key access, the hint advances forward yielding O(1) amortized per key.
     *
     * @return index if found (>= 0), or -(insertionPoint + 1) if not found
     */
    public static int scanKeyIdFromHint(long keyIdsAddr, int activeKeyCount, int key, int startPos) {
        for (int i = startPos; i < activeKeyCount; i++) {
            int k = Unsafe.getUnsafe().getInt(keyIdsAddr + (long) i * Integer.BYTES);
            if (k == key) return i;
            if (k > key) return -(i + 1);
        }
        return -(activeKeyCount + 1);
    }

    public static LPSZ keyFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".bk");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    public static LPSZ valueFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".bv");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }
}

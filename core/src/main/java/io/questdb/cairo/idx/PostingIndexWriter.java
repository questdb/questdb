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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import java.util.Arrays;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.idx.PostingIndexUtils.*;

/**
 * Delta + FoR64 BitPacking (BP) bitmap index writer.
 * <p>
 * Each commit appends one generation (covering all keys) to the value file.
 * No symbol table needed — encoding is purely arithmetic.
 */
public class PostingIndexWriter implements IndexWriter {
    private static final int INITIAL_KEY_CAPACITY = 64;
    private static final int MAX_GEN_COUNT = 256;
    private static final Log LOG = LogFactory.getLog(PostingIndexWriter.class);

    private final CairoConfiguration configuration;
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    private final MemoryMARW valueMem = Vm.getCMARWInstance();

    private final PostingIndexUtils.EncodeContext encodeCtx = new PostingIndexUtils.EncodeContext();
    private final PostingIndexUtils.DecodeContext decodeCtx = new PostingIndexUtils.DecodeContext();
    // Reusable DENSE_STRIDE-sized arrays for encodeDirtyStride / sealFull / rollback
    private final int[] strideKeyCounts = new int[PostingIndexUtils.DENSE_STRIDE];
    private final int[] strideKeyOffsets = new int[PostingIndexUtils.DENSE_STRIDE];
    private final int[] strideBpKeySizes = new int[PostingIndexUtils.DENSE_STRIDE];
    // Active key tracking to avoid scanning all keys per flush
    private int[] activeKeyIds = new int[INITIAL_KEY_CAPACITY];
    private int activeKeyCount;
    private int blockCapacity;
    private FilesFacade ff;
    // Reusable flush buffers to avoid malloc/free per commit
    private long flushHeaderBuf;
    private int flushHeaderBufCapacity;
    private long flushTmpBuf;
    private int flushTmpBufCapacity;
    private int genCount;
    private boolean hasPendingData;
    private int keyCapacity;
    private int keyCount;
    private long pendingCountsAddr;
    private long pendingValuesAddr;
    // Spill buffer: when a key's pending buffer overflows, its values are
    // copied here (raw longs) instead of triggering a global flush.
    // This avoids creating a gen per hot-key overflow.
    // Layout: per-key contiguous regions. Each key gets a growing slot.
    // Native arrays indexed by key:
    //   spillKeyAddrsAddr[key] (long) → native address of this key's spill values (or 0)
    //   spillKeyCountsAddr[key] (int) → number of spilled values
    //   spillKeyCapacitiesAddr[key] (int) → allocated capacity in longs
    private long spillKeyAddrsAddr;
    private long spillKeyCountsAddr;
    private long spillKeyCapacitiesAddr;
    private int spillArraysCapacity;
    private boolean hasSpillData;
    private long valueMemSize;

    public PostingIndexWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    @TestOnly
    public PostingIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        of(path, name, columnNameTxn, true);
    }

    public static void initKeyMemory(MemoryMA keyMem, int blockCapacity) {
        keyMem.jumpTo(0);
        keyMem.truncate();
        keyMem.putByte(SIGNATURE);
        keyMem.skip(7);
        keyMem.putLong(1); // SEQUENCE
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(0); // VALUE_MEM_SIZE
        keyMem.putInt(blockCapacity); // BLOCK_CAPACITY
        keyMem.putInt(0); // KEY_COUNT
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(1); // SEQUENCE_CHECK
        keyMem.putLong(-1); // MAX_VALUE
        keyMem.putInt(0); // GEN_COUNT
        keyMem.putInt(FORMAT_VERSION); // FORMAT_VERSION
        keyMem.skip(KEY_FILE_RESERVED - keyMem.getAppendOffset());
    }

    @Override
    public void add(int key, long value) {
        if (key < 0) {
            throw CairoException.critical(0).put("index key cannot be negative [key=").put(key).put(']');
        }

        if (key >= keyCapacity) {
            growKeyBuffers(key + 1);
        }

        int count = Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES);

        if (count >= blockCapacity) {
            // Buffer full for this key — spill its values to the overflow buffer
            // instead of flushing ALL keys. This prevents gen-count explosion
            // when a single hot key drives all overflows.
            spillKey(key, count);
            count = 0;
        }

        if (count > 0) {
            long lastVal = Unsafe.getUnsafe().getLong(
                    pendingValuesAddr + ((long) key * blockCapacity + count - 1) * Long.BYTES);
            if (value < lastVal) {
                throw CairoException.critical(0)
                        .put("index values must be added in ascending order [lastValue=")
                        .put(lastVal).put(", newValue=").put(value).put(']');
            }
        } else if (getSpillCount(key) == 0) {
            // First value for this key in this batch (and no prior spills) — track it
            // activeKeyIds is sized to keyCapacity and grown in growKeyBuffers()
            activeKeyIds[activeKeyCount++] = key;
        }

        Unsafe.getUnsafe().putLong(
                pendingValuesAddr + ((long) key * blockCapacity + count) * Long.BYTES, value);
        Unsafe.getUnsafe().putInt(pendingCountsAddr + (long) key * Integer.BYTES, count + 1);

        if (key >= keyCount) {
            keyCount = key + 1;
        }
        hasPendingData = true;
    }

    @Override
    public void close() {
        seal();
        compactValueFile();

        if (keyMem.isOpen()) {
            long keyFileSize = genCount > 0
                    ? PostingIndexUtils.getGenDirOffset(genCount)
                    : KEY_FILE_RESERVED;
            keyMem.setSize(keyFileSize);
            Misc.free(keyMem);
        }
        if (valueMem.isOpen()) {
            if (valueMemSize > 0) {
                valueMem.setSize(valueMemSize);
            }
            Misc.free(valueMem);
        }

        freeNativeBuffers();
        keyCount = 0;
        valueMemSize = 0;
        genCount = 0;
        hasPendingData = false;
        activeKeyCount = 0;
    }

    @Override
    public void commit() {
        flushAllPending();
        if (configuration.getCommitMode() != CommitMode.NOSYNC) {
            sync(configuration.getCommitMode() == CommitMode.ASYNC);
        }
    }

    /**
     * Seal the index: decode all generations, merge, re-encode into a single generation.
     * Uses incremental seal (dirty-stride) when gen 0 is dense and remaining gens are sparse.
     */
    public void seal() {
        flushAllPending();

        if (genCount == 0 || keyCount == 0) {
            return;
        }

        // Single sparse generation: seal to convert to stride-indexed dense format
        // (enables Packed mode compression which can be significantly smaller)
        if (genCount == 1) {
            long gen0DirOffset = PostingIndexUtils.getGenDirOffset(0);
            int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
            if (gen0KeyCount >= 0) {
                // Already dense — nothing to do
                return;
            }
            sealFull();
            return;
        }

        // Check if incremental seal is possible:
        // gen 0 must be dense, and all subsequent gens must be sparse
        long gen0DirOffset = PostingIndexUtils.getGenDirOffset(0);
        int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
        boolean isIncrementalCandidate = gen0KeyCount >= 0;

        if (isIncrementalCandidate) {
            for (int g = 1; g < genCount; g++) {
                long dirOffset = PostingIndexUtils.getGenDirOffset(g);
                int gkc = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                if (gkc >= 0) {
                    isIncrementalCandidate = false;
                    break;
                }
            }
        }

        if (isIncrementalCandidate && gen0KeyCount == keyCount) {
            sealIncremental();
        } else {
            sealFull();
        }
    }

    /**
     * Incremental seal: only re-encode dirty strides (those touched by sparse gens 1..N).
     * Clean strides are copied verbatim from the existing dense gen 0.
     */
    private void sealIncremental() {
        int sc = PostingIndexUtils.strideCount(keyCount);

        // Mark dirty strides by scanning sparse gens 1..N (native byte array)
        long dirtyStridesAddr = Unsafe.malloc(sc, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(dirtyStridesAddr, sc, (byte) 0);
        int dirtyCount = 0;
        for (int g = 1; g < genCount; g++) {
            long dirOffset = PostingIndexUtils.getGenDirOffset(g);
            long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
            int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
            int activeKeyCount = -genKeyCount;
            long genAddr = valueMem.addressOf(genFileOffset);

            for (int i = 0; i < activeKeyCount; i++) {
                int key = Unsafe.getUnsafe().getInt(genAddr + (long) i * Integer.BYTES);
                int stride = key / PostingIndexUtils.DENSE_STRIDE;
                if (stride < sc && Unsafe.getUnsafe().getByte(dirtyStridesAddr + stride) == 0) {
                    Unsafe.getUnsafe().putByte(dirtyStridesAddr + stride, (byte) 1);
                    dirtyCount++;
                }
            }
        }

        // If all strides are dirty, fall back to full seal (no savings)
        if (dirtyCount == sc) {
            Unsafe.free(dirtyStridesAddr, sc, MemoryTag.NATIVE_DEFAULT);
            sealFull();
            return;
        }

        // Read gen 0 metadata
        long gen0DirOffset = PostingIndexUtils.getGenDirOffset(0);
        long gen0FileOffset = keyMem.getLong(gen0DirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
        int gen0DataSize = keyMem.getInt(gen0DirOffset + GEN_DIR_OFFSET_SIZE);
        long gen0Addr = valueMem.addressOf(gen0FileOffset);
        int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
        int gen0SiSize = PostingIndexUtils.strideIndexSize(gen0KeyCount);

        // Allocate output buffers
        int siSize = PostingIndexUtils.strideIndexSize(keyCount);
        long strideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_DEFAULT);

        // Estimate max per-key values to size buffers
        int maxPerKey = estimateMaxPerKey(gen0Addr, gen0KeyCount, gen0SiSize);
        int perKeyBufSize = PostingIndexUtils.computeMaxEncodedSize(Math.max(maxPerKey, PostingIndexUtils.BLOCK_CAPACITY * genCount));
        int maxBPStrideDataSize = PostingIndexUtils.DENSE_STRIDE * perKeyBufSize;
        long bpTrialBuf = Unsafe.malloc(maxBPStrideDataSize, MemoryTag.NATIVE_DEFAULT);
        int maxHeaderSize = Math.max(
                PostingIndexUtils.strideBPHeaderSize(PostingIndexUtils.DENSE_STRIDE),
                PostingIndexUtils.stridePackedHeaderSize(PostingIndexUtils.DENSE_STRIDE)
        );
        long localHeaderBuf = Unsafe.malloc(maxHeaderSize, MemoryTag.NATIVE_DEFAULT);
        int[] bpKeySizes = strideBpKeySizes;

        // Buffer for merged key values — sized for an entire stride (all keys contiguous)
        // since encodeDirtyStride writes all keys' values contiguously with cumulative offsets.
        long maxPerStride = (long) PostingIndexUtils.DENSE_STRIDE * (maxPerKey + PostingIndexUtils.BLOCK_CAPACITY * genCount);
        long mergedValuesSize = Math.max(maxPerStride, 1024) * Long.BYTES;
        long mergedValuesAddr = Unsafe.malloc(mergedValuesSize, MemoryTag.NATIVE_DEFAULT);

        try {
            // Append-only seal: write sealed data AFTER existing data so that
            // concurrent readers with active cursors are never rugpulled.
            // Old generation data at [0..sealOffset) becomes dead space but
            // remains valid for any in-flight reader mmap pages.
            long sealOffset = valueMemSize;
            valueMem.jumpTo(sealOffset);
            // Reserve stride index
            for (int i = 0; i < siSize; i += Integer.BYTES) {
                valueMem.putInt(0);
            }

            for (int s = 0; s < sc; s++) {
                int strideOff = (int) (valueMem.getAppendOffset() - sealOffset - siSize);
                Unsafe.getUnsafe().putInt(strideIndexBuf + (long) s * Integer.BYTES, strideOff);

                if (Unsafe.getUnsafe().getByte(dirtyStridesAddr + s) == 0) {
                    // Clean stride: copy verbatim from gen 0
                    copyStrideFromGen0(gen0Addr, gen0KeyCount, gen0SiSize, s);
                } else {
                    // Dirty stride: decode from gen 0 + sparse gens, merge, re-encode
                    int ks = PostingIndexUtils.keysInStride(keyCount, s);
                    encodeDirtyStride(s, ks, gen0Addr, gen0KeyCount, gen0SiSize,
                            bpTrialBuf, localHeaderBuf, bpKeySizes, mergedValuesAddr);
                }
            }

            // Sentinel
            int totalStrideBlocksSize = (int) (valueMem.getAppendOffset() - sealOffset - siSize);
            Unsafe.getUnsafe().putInt(strideIndexBuf + (long) sc * Integer.BYTES, totalStrideBlocksSize);

            // Copy stride index
            long strideIndexAddr = valueMem.addressOf(sealOffset);
            Unsafe.getUnsafe().copyMemory(strideIndexBuf, strideIndexAddr, siSize);

            valueMemSize = valueMem.getAppendOffset();

            genCount = 1;
            updateHeaderAndGenDirAtomically(genCount, keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE),
                    sealOffset, (int) (valueMemSize - sealOffset), keyCount, 0, keyCount - 1);
        } finally {
            Unsafe.free(dirtyStridesAddr, sc, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(strideIndexBuf, siSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(bpTrialBuf, maxBPStrideDataSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(localHeaderBuf, maxHeaderSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(mergedValuesAddr, mergedValuesSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private int estimateMaxPerKey(long gen0Addr, int gen0KeyCount, int gen0SiSize) {
        int max = 0;
        int sc = PostingIndexUtils.strideCount(gen0KeyCount);
        for (int s = 0; s < sc; s++) {
            int strideOff = Unsafe.getUnsafe().getInt(gen0Addr + (long) s * Integer.BYTES);
            long strideAddr = gen0Addr + gen0SiSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(gen0KeyCount, s);
            byte mode = Unsafe.getUnsafe().getByte(strideAddr);
            if (mode == PostingIndexUtils.STRIDE_MODE_PACKED) {
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET;
                for (int j = 0; j < ks; j++) {
                    int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (j + 1) * Integer.BYTES)
                            - Unsafe.getUnsafe().getInt(prefixAddr + (long) j * Integer.BYTES);
                    if (count > max) max = count;
                }
            } else {
                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                for (int j = 0; j < ks; j++) {
                    int count = Unsafe.getUnsafe().getInt(countsAddr + (long) j * Integer.BYTES);
                    if (count > max) max = count;
                }
            }
        }
        return max;
    }

    private void copyStrideFromGen0(long gen0Addr, int gen0KeyCount, int gen0SiSize, int stride) {
        // If this stride existed in gen 0, copy it; otherwise write empty
        if (stride >= PostingIndexUtils.strideCount(gen0KeyCount)) {
            // Stride didn't exist in gen 0 — write empty BP stride
            int ks = PostingIndexUtils.keysInStride(keyCount, stride);
            int bpHeaderSize = PostingIndexUtils.strideBPHeaderSize(ks);
            long headerFilePos = valueMem.getAppendOffset();
            for (int i = 0; i < bpHeaderSize; i += Integer.BYTES) {
                valueMem.putInt(0);
            }
            // Zero header = BP mode, all counts 0, all offsets 0
            long headerAddr = valueMem.addressOf(headerFilePos);
            Unsafe.getUnsafe().setMemory(headerAddr, bpHeaderSize, (byte) 0);
            return;
        }

        int strideOff = Unsafe.getUnsafe().getInt(gen0Addr + (long) stride * Integer.BYTES);
        int nextStrideOff;
        if (stride + 1 < PostingIndexUtils.strideCount(gen0KeyCount)) {
            nextStrideOff = Unsafe.getUnsafe().getInt(gen0Addr + (long) (stride + 1) * Integer.BYTES);
        } else {
            // Last stride — get sentinel
            nextStrideOff = Unsafe.getUnsafe().getInt(gen0Addr + (long) PostingIndexUtils.strideCount(gen0KeyCount) * Integer.BYTES);
        }
        int strideSize = nextStrideOff - strideOff;
        if (strideSize <= 0) {
            return;
        }

        long srcAddr = gen0Addr + gen0SiSize + strideOff;
        // Extend valueMem and copy stride block
        long destFilePos = valueMem.getAppendOffset();
        // Write placeholder, then overwrite with copyMemory
        for (int i = 0; i < strideSize; i += Integer.BYTES) {
            valueMem.putInt(0);
        }
        // Handle remainder bytes
        int remainder = strideSize % Integer.BYTES;
        if (remainder > 0) {
            // Back up: we over-wrote some. Actually let's just use putByte for exact size
            valueMem.jumpTo(destFilePos);
            int written = 0;
            while (written + Long.BYTES <= strideSize) {
                valueMem.putLong(Unsafe.getUnsafe().getLong(srcAddr + written));
                written += (int) Long.BYTES;
            }
            while (written < strideSize) {
                valueMem.putByte(Unsafe.getUnsafe().getByte(srcAddr + written));
                written++;
            }
        } else {
            long destAddr = valueMem.addressOf(destFilePos);
            Unsafe.getUnsafe().copyMemory(srcAddr, destAddr, strideSize);
        }
    }

    private void encodeDirtyStride(int s, int ks, long gen0Addr, int gen0KeyCount, int gen0SiSize,
                                    long bpTrialBuf, long localHeaderBuf,
                                    int[] bpKeySizes, long mergedValuesAddr) {
        // For each key in this stride, decode from gen 0 + all sparse gens, merge.
        // Store all merged values contiguously in mergedValuesAddr with per-key offsets
        // so writePackedStride can read from the pre-merged buffer without re-merging.
        int[] keyCounts = strideKeyCounts;
        int[] keyOffsets = strideKeyOffsets;

        // Merge all keys' values contiguously into mergedValuesAddr
        int cumOffset = 0;
        for (int j = 0; j < ks; j++) {
            int key = s * PostingIndexUtils.DENSE_STRIDE + j;
            keyOffsets[j] = cumOffset;
            int mergedCount = mergeKeyValues(key, gen0Addr, gen0KeyCount, gen0SiSize,
                    mergedValuesAddr + (long) cumOffset * Long.BYTES);
            keyCounts[j] = mergedCount;
            cumOffset += mergedCount;
        }

        // Trial BP encode from the pre-merged buffer (encode directly from native memory)
        int bpDataTotal = 0;
        for (int j = 0; j < ks; j++) {
            int count = keyCounts[j];
            if (count > 0) {
                long keyAddr = mergedValuesAddr + (long) keyOffsets[j] * Long.BYTES;
                encodeCtx.ensureCapacity(count);
                bpKeySizes[j] = PostingIndexUtils.encodeKeyNative(keyAddr, count, bpTrialBuf + bpDataTotal, encodeCtx);
            } else {
                bpKeySizes[j] = 0;
            }
            bpDataTotal += bpKeySizes[j];
        }

        // Compute per-stride base value (min across all values in stride)
        int totalStrideValues = cumOffset;
        long strideMinValue = Long.MAX_VALUE;
        long strideMaxValue = Long.MIN_VALUE;
        for (int i = 0; i < totalStrideValues; i++) {
            long val = Unsafe.getUnsafe().getLong(mergedValuesAddr + (long) i * Long.BYTES);
            if (val < strideMinValue) strideMinValue = val;
            if (val > strideMaxValue) strideMaxValue = val;
        }
        if (totalStrideValues == 0) {
            strideMinValue = 0;
            strideMaxValue = 0;
        }
        long strideRange = strideMaxValue - strideMinValue;
        int localBitWidth = strideRange <= 0 ? 1 : FORBitmapIndexUtils.bitsNeeded(strideRange);

        // Compute sizes
        int bpHeaderSize = PostingIndexUtils.strideBPHeaderSize(ks);
        int bpSize = bpHeaderSize + bpDataTotal;

        int packedHeaderSize = PostingIndexUtils.stridePackedHeaderSize(ks);
        int packedDataSize = FORBitmapIndexUtils.packedDataSize(totalStrideValues, localBitWidth);
        int packedSize = packedHeaderSize + packedDataSize;

        boolean usePacked = packedSize < bpSize;

        if (usePacked) {
            writePackedStride(ks, keyCounts, keyOffsets, localBitWidth, strideMinValue, packedHeaderSize, packedDataSize,
                    localHeaderBuf, mergedValuesAddr);
        } else {
            writeBPStride(ks, keyCounts, bpHeaderSize, bpTrialBuf, bpKeySizes, bpDataTotal, localHeaderBuf);
        }
    }

    private int mergeKeyValues(int key, long gen0Addr, int gen0KeyCount, int gen0SiSize, long destAddr) {
        int totalCount = 0;

        // Decode from gen 0 (dense)
        if (key < gen0KeyCount) {
            int stride = key / PostingIndexUtils.DENSE_STRIDE;
            int localKey = key % PostingIndexUtils.DENSE_STRIDE;
            int strideOff = Unsafe.getUnsafe().getInt(gen0Addr + (long) stride * Integer.BYTES);
            long strideAddr = gen0Addr + gen0SiSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(gen0KeyCount, stride);
            byte mode = Unsafe.getUnsafe().getByte(strideAddr);

            if (mode == PostingIndexUtils.STRIDE_MODE_PACKED) {
                int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_PACKED_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET;
                int startIdx = Unsafe.getUnsafe().getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES) - startIdx;
                if (count > 0) {
                    int phSize = PostingIndexUtils.stridePackedHeaderSize(ks);
                    long packedDataAddr = strideAddr + phSize;
                    // Unpack directly to native destination using per-value random access
                    for (int i = 0; i < count; i++) {
                        long val = FORBitmapIndexUtils.unpackValue(packedDataAddr, startIdx + i, bitWidth, baseValue);
                        Unsafe.getUnsafe().putLong(destAddr + (long) totalCount * Long.BYTES, val);
                        totalCount++;
                    }
                }
            } else {
                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                int count = Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
                if (count > 0) {
                    long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
                    int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
                    int bpHdrSize = PostingIndexUtils.strideBPHeaderSize(ks);
                    long encodedAddr = strideAddr + bpHdrSize + dataOffset;
                    PostingIndexUtils.decodeKeyToNative(encodedAddr, count, destAddr + (long) totalCount * Long.BYTES, decodeCtx);
                    totalCount += count;
                }
            }
        }

        // Decode from sparse gens 1..N
        for (int g = 1; g < genCount; g++) {
            long dirOffset = PostingIndexUtils.getGenDirOffset(g);
            long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
            int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
            int activeKeyCount = -genKeyCount;
            long genAddr = valueMem.addressOf(genFileOffset);

            int idx = PostingIndexUtils.binarySearchKeyId(genAddr, activeKeyCount, key);
            if (idx < 0) continue;

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            int count = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
            if (count == 0) continue;

            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
            long encodedAddr = genAddr + headerSize + dataOffset;
            PostingIndexUtils.decodeKeyToNative(encodedAddr, count, destAddr + (long) totalCount * Long.BYTES, decodeCtx);
            totalCount += count;
        }

        return totalCount;
    }

    private void writePackedStride(int ks, int[] keyCounts, int[] keyOffsets,
                                    int localBitWidth, long strideMinValue, int packedHeaderSize, int packedDataSize,
                                    long localHeaderBuf, long mergedValuesAddr) {
        long headerFilePos = valueMem.getAppendOffset();
        for (int i = 0; i < packedHeaderSize; i += Integer.BYTES) {
            valueMem.putInt(0);
        }

        Unsafe.getUnsafe().setMemory(localHeaderBuf, packedHeaderSize, (byte) 0);
        Unsafe.getUnsafe().putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_PACKED);
        Unsafe.getUnsafe().putByte(localHeaderBuf + 1, (byte) localBitWidth);
        Unsafe.getUnsafe().putLong(localHeaderBuf + PostingIndexUtils.STRIDE_PACKED_BASE_OFFSET, strideMinValue);
        int cumCount = 0;
        for (int j = 0; j <= ks; j++) {
            Unsafe.getUnsafe().putInt(
                    localHeaderBuf + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET + (long) j * Integer.BYTES,
                    cumCount);
            if (j < ks) {
                cumCount += keyCounts[j];
            }
        }

        long packedBuf = Unsafe.malloc(packedDataSize > 0 ? packedDataSize : 1, MemoryTag.NATIVE_DEFAULT);
        try {
            if (packedDataSize > 0) {
                Unsafe.getUnsafe().setMemory(packedBuf, packedDataSize, (byte) 0);
            }
            int packIdx = 0;
            for (int j = 0; j < ks; j++) {
                int count = keyCounts[j];
                long keyAddr = mergedValuesAddr + (long) keyOffsets[j] * Long.BYTES;
                for (int i = 0; i < count; i++) {
                    long val = Unsafe.getUnsafe().getLong(keyAddr + (long) i * Long.BYTES);
                    packSingleValue(packedBuf, packIdx, localBitWidth, val - strideMinValue);
                    packIdx++;
                }
            }

            int written = 0;
            while (written + Long.BYTES <= packedDataSize) {
                valueMem.putLong(Unsafe.getUnsafe().getLong(packedBuf + written));
                written += (int) Long.BYTES;
            }
            while (written < packedDataSize) {
                valueMem.putByte(Unsafe.getUnsafe().getByte(packedBuf + written));
                written++;
            }
        } finally {
            Unsafe.free(packedBuf, packedDataSize > 0 ? packedDataSize : 1, MemoryTag.NATIVE_DEFAULT);
        }

        long headerAddr = valueMem.addressOf(headerFilePos);
        Unsafe.getUnsafe().copyMemory(localHeaderBuf, headerAddr, packedHeaderSize);
    }

    private void writeBPStride(int ks, int[] keyCounts, int bpHeaderSize,
                                long bpTrialBuf, int[] bpKeySizes, int bpDataTotal,
                                long localHeaderBuf) {
        long headerFilePos = valueMem.getAppendOffset();
        for (int i = 0; i < bpHeaderSize; i += Integer.BYTES) {
            valueMem.putInt(0);
        }

        Unsafe.getUnsafe().setMemory(localHeaderBuf, bpHeaderSize, (byte) 0);
        Unsafe.getUnsafe().putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_BP);
        long countsBase = localHeaderBuf + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
        long offsetsBase = countsBase + (long) ks * Integer.BYTES;

        int dataOffset = 0;
        int bpBufOffset = 0;
        for (int j = 0; j < ks; j++) {
            Unsafe.getUnsafe().putInt(countsBase + (long) j * Integer.BYTES, keyCounts[j]);
            Unsafe.getUnsafe().putInt(offsetsBase + (long) j * Integer.BYTES, dataOffset);

            if (bpKeySizes[j] > 0) {
                int bytesWritten = bpKeySizes[j];
                int written = 0;
                while (written + Long.BYTES <= bytesWritten) {
                    valueMem.putLong(Unsafe.getUnsafe().getLong(bpTrialBuf + bpBufOffset + written));
                    written += (int) Long.BYTES;
                }
                while (written < bytesWritten) {
                    valueMem.putByte(Unsafe.getUnsafe().getByte(bpTrialBuf + bpBufOffset + written));
                    written++;
                }
                dataOffset += bytesWritten;
            }
            bpBufOffset += bpKeySizes[j];
        }

        Unsafe.getUnsafe().putInt(offsetsBase + (long) ks * Integer.BYTES, dataOffset);

        long headerAddr = valueMem.addressOf(headerFilePos);
        Unsafe.getUnsafe().copyMemory(localHeaderBuf, headerAddr, bpHeaderSize);
    }

    private void sealFull() {
        // Phase 1: Count total values per key across all generations
        long totalCountsSize = (long) keyCount * Integer.BYTES;
        long totalCountsAddr = Unsafe.malloc(totalCountsSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(totalCountsAddr, totalCountsSize, (byte) 0);

            long totalValueCount = 0;
            for (int gen = 0; gen < genCount; gen++) {
                long dirOffset = PostingIndexUtils.getGenDirOffset(gen);
                long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                long genAddr = valueMem.addressOf(genFileOffset);

                if (genKeyCount < 0) {
                    // Sparse format
                    int activeKeyCount = -genKeyCount;
                    long keyIdsBase = genAddr;
                    long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
                    for (int i = 0; i < activeKeyCount; i++) {
                        int key = Unsafe.getUnsafe().getInt(keyIdsBase + (long) i * Integer.BYTES);
                        int count = Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
                        int existing = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                        Unsafe.getUnsafe().putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
                        totalValueCount += count;
                    }
                } else {
                    // Dense format — stride-indexed (supports BP and Packed modes)
                    int sc = PostingIndexUtils.strideCount(genKeyCount);
                    int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                    for (int s = 0; s < sc; s++) {
                        int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) s * Integer.BYTES);
                        long strideAddr = genAddr + siSize + strideOff;
                        int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
                        byte mode = Unsafe.getUnsafe().getByte(strideAddr);
                        if (mode == PostingIndexUtils.STRIDE_MODE_PACKED) {
                            // Packed mode: prefixCounts at STRIDE_PACKED_PREFIX_COUNTS_OFFSET
                            long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (j + 1) * Integer.BYTES)
                                        - Unsafe.getUnsafe().getInt(prefixAddr + (long) j * Integer.BYTES);
                                int existing = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                Unsafe.getUnsafe().putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
                                totalValueCount += count;
                            }
                        } else {
                            // BP mode: counts at offset STRIDE_MODE_PREFIX_SIZE
                            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getUnsafe().getInt(countsAddr + (long) j * Integer.BYTES);
                                int existing = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                Unsafe.getUnsafe().putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
                                totalValueCount += count;
                            }
                        }
                    }
                }
            }

            if (totalValueCount == 0) {
                return;
            }

            // Phase 2: Decode all values grouped by key into a flat buffer
            long allValuesAddr = Unsafe.malloc(totalValueCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long keyWriteOffsetsSize = (long) keyCount * Long.BYTES;
            long keyWriteOffsetsAddr = Unsafe.malloc(keyWriteOffsetsSize, MemoryTag.NATIVE_DEFAULT);
            try {
                // Compute per-key write offsets
                long writeOffset = 0;
                for (int key = 0; key < keyCount; key++) {
                    Unsafe.getUnsafe().putLong(keyWriteOffsetsAddr + (long) key * Long.BYTES, writeOffset);
                    writeOffset += Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                }

                // Decode from each generation
                for (int gen = 0; gen < genCount; gen++) {
                    long dirOffset = PostingIndexUtils.getGenDirOffset(gen);
                    long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                    int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                    long genAddr = valueMem.addressOf(genFileOffset);

                    if (genKeyCount < 0) {
                        // Sparse format
                        int activeKeyCount = -genKeyCount;
                        int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
                        long keyIdsBase = genAddr;
                        long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
                        long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;

                        for (int i = 0; i < activeKeyCount; i++) {
                            int key = Unsafe.getUnsafe().getInt(keyIdsBase + (long) i * Integer.BYTES);
                            int count = Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
                            if (count == 0) continue;

                            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) i * Integer.BYTES);
                            long encodedAddr = genAddr + headerSize + dataOffset;

                            long keyWriteOff = Unsafe.getUnsafe().getLong(
                                    keyWriteOffsetsAddr + (long) key * Long.BYTES);
                            long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;

                            PostingIndexUtils.decodeKeyToNative(encodedAddr, count, destAddr, decodeCtx);

                            Unsafe.getUnsafe().putLong(
                                    keyWriteOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                        }
                    } else {
                        // Dense format — stride-indexed (supports BP and Packed modes)
                        int sc = PostingIndexUtils.strideCount(genKeyCount);
                        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                        for (int s = 0; s < sc; s++) {
                            int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) s * Integer.BYTES);
                            long strideAddr = genAddr + siSize + strideOff;
                            int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
                            byte mode = Unsafe.getUnsafe().getByte(strideAddr);

                            if (mode == PostingIndexUtils.STRIDE_MODE_PACKED) {
                                // Packed mode
                                int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                                long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_PACKED_BASE_OFFSET);
                                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET;
                                int packedHeaderSize = PostingIndexUtils.stridePackedHeaderSize(ks);
                                long packedDataAddr = strideAddr + packedHeaderSize;

                                for (int j = 0; j < ks; j++) {
                                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                    int startIdx = Unsafe.getUnsafe().getInt(prefixAddr + (long) j * Integer.BYTES);
                                    int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (j + 1) * Integer.BYTES) - startIdx;
                                    if (count == 0) continue;

                                    long keyWriteOff = Unsafe.getUnsafe().getLong(
                                            keyWriteOffsetsAddr + (long) key * Long.BYTES);
                                    long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;

                                    for (int i = 0; i < count; i++) {
                                        long val = FORBitmapIndexUtils.unpackValue(packedDataAddr, startIdx + i, bitWidth, baseValue);
                                        Unsafe.getUnsafe().putLong(destAddr + (long) i * Long.BYTES, val);
                                    }

                                    Unsafe.getUnsafe().putLong(
                                            keyWriteOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                                }
                            } else {
                                // BP mode
                                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                                long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
                                int bpHeaderSize = PostingIndexUtils.strideBPHeaderSize(ks);

                                for (int j = 0; j < ks; j++) {
                                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                    int count = Unsafe.getUnsafe().getInt(countsAddr + (long) j * Integer.BYTES);
                                    if (count == 0) continue;

                                    int dataOff = Unsafe.getUnsafe().getInt(offsetsBase + (long) j * Integer.BYTES);
                                    long encodedAddr = strideAddr + bpHeaderSize + dataOff;

                                    long keyWriteOff = Unsafe.getUnsafe().getLong(
                                            keyWriteOffsetsAddr + (long) key * Long.BYTES);
                                    long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;

                                    PostingIndexUtils.decodeKeyToNative(encodedAddr, count, destAddr, decodeCtx);

                                    Unsafe.getUnsafe().putLong(
                                            keyWriteOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                                }
                            }
                        }
                    }
                }

                // Phase 3: Re-encode into single generation using stride-indexed format
                // with adaptive per-stride encoding (BP vs Packed)
                int maxPerKey = 0;
                for (int key = 0; key < keyCount; key++) {
                    int c = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    if (c > maxPerKey) maxPerKey = c;
                }
                int perKeyBufSize = PostingIndexUtils.computeMaxEncodedSize(maxPerKey);
                long tmpBuf = Unsafe.malloc(perKeyBufSize, MemoryTag.NATIVE_DEFAULT);

                try {
                    int sc = PostingIndexUtils.strideCount(keyCount);
                    int siSize = PostingIndexUtils.strideIndexSize(keyCount);

                    // Append-only seal: write sealed data AFTER existing data so that
                    // concurrent readers with active cursors are never rugpulled.
                    // Old generation data at [0..sealOffset) becomes dead space but
                    // remains valid for any in-flight reader mmap pages.
                    long sealOffset = valueMemSize;
                    valueMem.jumpTo(sealOffset);
                    for (int i = 0; i < siSize; i += Integer.BYTES) {
                        valueMem.putInt(0);
                    }

                    // Allocate stride index buffer
                    long strideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_DEFAULT);
                    // Max header is the BP header (larger than packed header)
                    int maxBPHeaderSize = PostingIndexUtils.strideBPHeaderSize(PostingIndexUtils.DENSE_STRIDE);
                    int maxPackedHeaderSize = PostingIndexUtils.stridePackedHeaderSize(PostingIndexUtils.DENSE_STRIDE);
                    int maxLocalHeaderSize = Math.max(maxBPHeaderSize, maxPackedHeaderSize);
                    long localHeaderBuf = Unsafe.malloc(maxLocalHeaderSize, MemoryTag.NATIVE_DEFAULT);

                    // Per-key BP sizes within a stride (to compute total BP data size)
                    int[] bpKeySizes = strideBpKeySizes;
                    // Trial buffer grows dynamically per stride
                    long bpTrialBuf = 0;
                    long bpTrialBufSize = 0;

                    try {
                        long readOffset = 0;

                        for (int s = 0; s < sc; s++) {
                            int ks = PostingIndexUtils.keysInStride(keyCount, s);

                            // Compute trial buffer size for this stride
                            long strideTrialSize = 0;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                strideTrialSize += PostingIndexUtils.computeMaxEncodedSize(count);
                            }
                            if (strideTrialSize > bpTrialBufSize) {
                                if (bpTrialBuf != 0) {
                                    Unsafe.free(bpTrialBuf, bpTrialBufSize, MemoryTag.NATIVE_DEFAULT);
                                }
                                bpTrialBufSize = strideTrialSize;
                                bpTrialBuf = Unsafe.malloc(bpTrialBufSize, MemoryTag.NATIVE_DEFAULT);
                            }

                            // --- Trial BP encode all keys in stride (directly from native memory) ---
                            int bpDataTotal = 0;
                            long trialReadOffset = readOffset;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                if (count > 0) {
                                    long keyAddr = allValuesAddr + trialReadOffset * Long.BYTES;
                                    encodeCtx.ensureCapacity(count);
                                    bpKeySizes[j] = PostingIndexUtils.encodeKeyNative(keyAddr, count, bpTrialBuf + bpDataTotal, encodeCtx);
                                    trialReadOffset += count;
                                } else {
                                    bpKeySizes[j] = 0;
                                }
                                bpDataTotal += bpKeySizes[j];
                            }

                            // --- Compute sizes for both modes ---
                            int bpHeaderSize = PostingIndexUtils.strideBPHeaderSize(ks);
                            int bpSize = bpHeaderSize + bpDataTotal;

                            // Count total values in stride and find per-stride min/max for packed size
                            int totalStrideValues = 0;
                            long strideMinValue = Long.MAX_VALUE;
                            long strideMaxValue = Long.MIN_VALUE;
                            {
                                long scanOffset = readOffset;
                                for (int j = 0; j < ks; j++) {
                                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                    int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                    totalStrideValues += count;
                                    for (int i = 0; i < count; i++) {
                                        long val = Unsafe.getUnsafe().getLong(allValuesAddr + (scanOffset + i) * Long.BYTES);
                                        if (val < strideMinValue) strideMinValue = val;
                                        if (val > strideMaxValue) strideMaxValue = val;
                                    }
                                    scanOffset += count;
                                }
                            }
                            if (totalStrideValues == 0) {
                                strideMinValue = 0;
                                strideMaxValue = 0;
                            }
                            long strideRange = strideMaxValue - strideMinValue;
                            int localBitWidth = strideRange <= 0 ? 1 : FORBitmapIndexUtils.bitsNeeded(strideRange);

                            int packedHeaderSize = PostingIndexUtils.stridePackedHeaderSize(ks);
                            int packedDataSize = FORBitmapIndexUtils.packedDataSize(totalStrideValues, localBitWidth);
                            int packedSize = packedHeaderSize + packedDataSize;

                            boolean usePacked = packedSize < bpSize;

                            // Record stride offset (relative to end of stride index)
                            int strideOff = (int) (valueMem.getAppendOffset() - sealOffset - siSize);
                            Unsafe.getUnsafe().putInt(strideIndexBuf + (long) s * Integer.BYTES, strideOff);

                            if (usePacked) {
                                // --- Write Packed mode ---
                                // Reserve header space
                                long localHeaderFilePos = valueMem.getAppendOffset();
                                for (int i = 0; i < packedHeaderSize; i += Integer.BYTES) {
                                    valueMem.putInt(0);
                                }

                                // Build header: mode + bitWidth + baseValue + prefixCounts
                                Unsafe.getUnsafe().setMemory(localHeaderBuf, packedHeaderSize, (byte) 0);
                                Unsafe.getUnsafe().putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_PACKED);
                                Unsafe.getUnsafe().putByte(localHeaderBuf + 1, (byte) localBitWidth);
                                Unsafe.getUnsafe().putLong(localHeaderBuf + PostingIndexUtils.STRIDE_PACKED_BASE_OFFSET, strideMinValue);
                                int cumCount = 0;
                                for (int j = 0; j <= ks; j++) {
                                    Unsafe.getUnsafe().putInt(
                                            localHeaderBuf + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET + (long) j * Integer.BYTES,
                                            cumCount);
                                    if (j < ks) {
                                        int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                        cumCount += Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                    }
                                }

                                // Write packed data: all values contiguously bit-packed (value - baseValue)
                                long packedBuf = Unsafe.malloc(packedDataSize > 0 ? packedDataSize : 1, MemoryTag.NATIVE_DEFAULT);
                                try {
                                    if (packedDataSize > 0) {
                                        Unsafe.getUnsafe().setMemory(packedBuf, packedDataSize, (byte) 0);
                                    }
                                    int packIdx = 0;
                                    for (int j = 0; j < ks; j++) {
                                        int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                        int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                        for (int i = 0; i < count; i++) {
                                            long val = Unsafe.getUnsafe().getLong(
                                                    allValuesAddr + (readOffset + i) * Long.BYTES);
                                            packSingleValue(packedBuf, packIdx, localBitWidth, val - strideMinValue);
                                            packIdx++;
                                        }
                                        readOffset += count;
                                    }

                                    // Write packed data to valueMem
                                    int written = 0;
                                    while (written + Long.BYTES <= packedDataSize) {
                                        valueMem.putLong(Unsafe.getUnsafe().getLong(packedBuf + written));
                                        written += (int) Long.BYTES;
                                    }
                                    while (written < packedDataSize) {
                                        valueMem.putByte(Unsafe.getUnsafe().getByte(packedBuf + written));
                                        written++;
                                    }
                                } finally {
                                    Unsafe.free(packedBuf, packedDataSize > 0 ? packedDataSize : 1, MemoryTag.NATIVE_DEFAULT);
                                }

                                // Copy header into reserved space
                                long localHeaderAddr = valueMem.addressOf(localHeaderFilePos);
                                Unsafe.getUnsafe().copyMemory(localHeaderBuf, localHeaderAddr, packedHeaderSize);
                            } else {
                                // --- Write BP mode ---
                                // Reserve header space
                                long localHeaderFilePos = valueMem.getAppendOffset();
                                for (int i = 0; i < bpHeaderSize; i += Integer.BYTES) {
                                    valueMem.putInt(0);
                                }

                                // Build header: mode + reserved + counts + offsets
                                Unsafe.getUnsafe().setMemory(localHeaderBuf, bpHeaderSize, (byte) 0);
                                Unsafe.getUnsafe().putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_BP);
                                Unsafe.getUnsafe().putByte(localHeaderBuf + 1, (byte) 0); // reserved
                                long countsBase = localHeaderBuf + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                                long offsetsBase = countsBase + (long) ks * Integer.BYTES;

                                // Copy trial-encoded BP data from bpTrialBuf to valueMem
                                int dataOffset = 0;
                                int bpBufOffset = 0;
                                for (int j = 0; j < ks; j++) {
                                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                    int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);

                                    Unsafe.getUnsafe().putInt(countsBase + (long) j * Integer.BYTES, count);
                                    Unsafe.getUnsafe().putInt(offsetsBase + (long) j * Integer.BYTES, dataOffset);

                                    if (bpKeySizes[j] > 0) {
                                        int bytesWritten = bpKeySizes[j];
                                        int written = 0;
                                        while (written + Long.BYTES <= bytesWritten) {
                                            valueMem.putLong(Unsafe.getUnsafe().getLong(bpTrialBuf + bpBufOffset + written));
                                            written += (int) Long.BYTES;
                                        }
                                        while (written < bytesWritten) {
                                            valueMem.putByte(Unsafe.getUnsafe().getByte(bpTrialBuf + bpBufOffset + written));
                                            written++;
                                        }
                                        dataOffset += bytesWritten;
                                    }
                                    bpBufOffset += bpKeySizes[j];
                                    readOffset += count;
                                }

                                // Sentinel offset
                                Unsafe.getUnsafe().putInt(offsetsBase + (long) ks * Integer.BYTES, dataOffset);

                                // Copy header into reserved space
                                long localHeaderAddr = valueMem.addressOf(localHeaderFilePos);
                                Unsafe.getUnsafe().copyMemory(localHeaderBuf, localHeaderAddr, bpHeaderSize);
                            }
                        }

                        // Sentinel: total size of all stride blocks
                        int totalStrideBlocksSize = (int) (valueMem.getAppendOffset() - sealOffset - siSize);
                        Unsafe.getUnsafe().putInt(strideIndexBuf + (long) sc * Integer.BYTES, totalStrideBlocksSize);

                        // Copy stride index into value file
                        long strideIndexAddr = valueMem.addressOf(sealOffset);
                        Unsafe.getUnsafe().copyMemory(strideIndexBuf, strideIndexAddr, siSize);

                        valueMemSize = valueMem.getAppendOffset();
                    } finally {
                        if (bpTrialBuf != 0) {
                            Unsafe.free(bpTrialBuf, bpTrialBufSize, MemoryTag.NATIVE_DEFAULT);
                        }
                        Unsafe.free(localHeaderBuf, maxLocalHeaderSize, MemoryTag.NATIVE_DEFAULT);
                        Unsafe.free(strideIndexBuf, siSize, MemoryTag.NATIVE_DEFAULT);
                    }

                    genCount = 1;

                    // Write gen dir[0] and header atomically (within the same sequence window).
                    // Without this, a concurrent reader could see the modified gen dir[0]
                    // (pointing to sealed data) while still using the old genCount (N gens),
                    // producing duplicate values.
                    updateHeaderAndGenDirAtomically(genCount, keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE),
                            sealOffset, (int) (valueMemSize - sealOffset), keyCount, 0, keyCount - 1);
                } finally {
                    Unsafe.free(tmpBuf, perKeyBufSize, MemoryTag.NATIVE_DEFAULT);
                }

            } finally {
                Unsafe.free(allValuesAddr, totalValueCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(keyWriteOffsetsAddr, keyWriteOffsetsSize, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Unsafe.free(totalCountsAddr, totalCountsSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @TestOnly
    public RowCursor getCursor(int key) {
        flushAllPending();

        if (key >= keyCount || key < 0 || genCount == 0) {
            return EmptyRowCursor.INSTANCE;
        }

        LongList values = new LongList();
        for (int gen = 0; gen < genCount; gen++) {
            long dirOffset = PostingIndexUtils.getGenDirOffset(gen);
            long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
            int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);

            int minKey = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY);
            int maxKey = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY);
            if (key < minKey || key > maxKey) continue;

            long genAddr = valueMem.addressOf(genFileOffset);

            int count;
            long encodedAddr;

            if (genKeyCount < 0) {
                // Sparse format
                int activeKeyCount = -genKeyCount;
                int idx = PostingIndexUtils.binarySearchKeyId(genAddr, activeKeyCount, key);
                if (idx < 0) continue;

                int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
                long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
                long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
                count = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
                int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
                encodedAddr = genAddr + headerSize + dataOffset;
            } else {
                // Dense format — stride-indexed (supports BP and Packed modes)
                if (key >= genKeyCount) continue;

                int stride = key / PostingIndexUtils.DENSE_STRIDE;
                int localKey = key % PostingIndexUtils.DENSE_STRIDE;
                int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) stride * Integer.BYTES);
                long strideAddr = genAddr + siSize + strideOff;
                int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
                byte mode = Unsafe.getUnsafe().getByte(strideAddr);

                if (mode == PostingIndexUtils.STRIDE_MODE_PACKED) {
                    // Packed mode
                    int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                    long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_PACKED_BASE_OFFSET);
                    long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET;
                    int startIdx = Unsafe.getUnsafe().getInt(prefixAddr + (long) localKey * Integer.BYTES);
                    count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES) - startIdx;
                    if (count == 0) continue;
                    int packedHeaderSize = PostingIndexUtils.stridePackedHeaderSize(ks);
                    long packedDataAddr = strideAddr + packedHeaderSize;
                    for (int i = 0; i < count; i++) {
                        values.add(FORBitmapIndexUtils.unpackValue(packedDataAddr, startIdx + i, bitWidth, baseValue));
                    }
                    continue;
                }

                // BP mode
                long countsBase = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                count = Unsafe.getUnsafe().getInt(countsBase + (long) localKey * Integer.BYTES);
                long offsetsBase = countsBase + (long) ks * Integer.BYTES;
                int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
                int bpHeaderSize = PostingIndexUtils.strideBPHeaderSize(ks);
                encodedAddr = strideAddr + bpHeaderSize + dataOffset;
            }

            if (count == 0) continue;
            long[] decoded = new long[count];
            PostingIndexUtils.decodeKey(encodedAddr, count, decoded);
            for (int i = 0; i < count; i++) {
                values.add(decoded[i]);
            }
        }

        return new TestFwdCursor(values);
    }

    @Override
    public int getKeyCount() {
        return keyCount;
    }

    @TestOnly
    public long getValueMemSize() {
        return valueMemSize;
    }

    @TestOnly
    public int getGenCount() {
        return genCount;
    }

    @Override
    public boolean isOpen() {
        return keyMem.isOpen();
    }

    @Override
    public byte getIndexType() {
        return IndexType.POSTING;
    }

    @Override
    public long getMaxValue() {
        return keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE);
    }

    @Override
    public void setMaxValue(long maxValue) {
        keyMem.putLong(KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
    }

    @Override
    public void of(Path path, CharSequence name, long columnNameTxn) {
        of(path, name, columnNameTxn, false);
    }

    @Override
    public void of(CairoConfiguration configuration, long keyFd, long valueFd, boolean init, int blockCapacity) {
        close();
        final FilesFacade ff = configuration.getFilesFacade();
        boolean kFdUnassigned = true;
        boolean vFdUnassigned = true;
        final long keyAppendPageSize = configuration.getDataIndexKeyAppendPageSize();
        final long valueAppendPageSize = configuration.getDataIndexValueAppendPageSize();

        try {
            if (init) {
                if (ff.truncate(keyFd, 0)) {
                    kFdUnassigned = false;
                    keyMem.of(ff, keyFd, false, null, keyAppendPageSize, keyAppendPageSize, MemoryTag.MMAP_INDEX_WRITER);
                    this.blockCapacity = BLOCK_CAPACITY;
                    initKeyMemory(keyMem, this.blockCapacity);
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(keyFd).put(']');
                }
            } else {
                final long keyFileSize = ff.length(keyFd);
                if (keyFileSize < KEY_FILE_RESERVED) {
                    throw CairoException.critical(0)
                            .put("Index file too short [fd=").put(keyFd)
                            .put(", expected>=").put(KEY_FILE_RESERVED)
                            .put(", actual=").put(keyFileSize).put(']');
                }

                kFdUnassigned = false;
                keyMem.of(ff, keyFd, null, keyFileSize, MemoryTag.MMAP_INDEX_WRITER);

                byte sig = keyMem.getByte(KEY_RESERVED_OFFSET_SIGNATURE);
                if (sig != SIGNATURE) {
                    throw CairoException.critical(0)
                            .put("Unknown format: invalid BP index signature [fd=").put(keyFd)
                            .put(", expected=").put(SIGNATURE)
                            .put(", actual=").put(sig).put(']');
                }
            }

            this.valueMemSize = keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            this.blockCapacity = keyMem.getInt(KEY_RESERVED_OFFSET_BLOCK_CAPACITY);
            this.keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
            this.genCount = keyMem.getInt(KEY_RESERVED_OFFSET_GEN_COUNT);

            if (init) {
                if (ff.truncate(valueFd, 0)) {
                    vFdUnassigned = false;
                    valueMem.of(ff, valueFd, false, null, valueAppendPageSize, valueAppendPageSize, MemoryTag.MMAP_INDEX_WRITER);
                    valueMem.jumpTo(0);
                    valueMemSize = 0;
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(valueFd).put(']');
                }
            } else {
                vFdUnassigned = false;
                valueMem.of(ff, valueFd, false, null, valueAppendPageSize, valueMemSize, MemoryTag.MMAP_INDEX_WRITER);
                if (valueMemSize > 0) {
                    long actualFileSize = ff.length(valueFd);
                    if (actualFileSize >= 0 && actualFileSize < valueMemSize) {
                        LOG.advisory().$("value file shorter than header claims [expected=").$(valueMemSize)
                                .$(", actual=").$(actualFileSize).$(", fd=").$(valueFd)
                                .$("] — possible incomplete seal").$();
                    }
                    valueMem.jumpTo(valueMemSize);
                    compactValueFile();
                }
            }

            allocateNativeBuffers();
        } catch (Throwable e) {
            close();
            if (kFdUnassigned) {
                ff.close(keyFd);
            }
            if (vFdUnassigned) {
                ff.close(valueFd);
            }
            throw e;
        }
    }

    @Override
    public void rollbackConditionally(long row) {
        final long currentMaxRow = getMaxValue();
        if (row >= 0 && (currentMaxRow < 1 || currentMaxRow >= row)) {
            if (row == 0) {
                truncate();
            } else {
                rollbackValues(row - 1);
            }
        }
    }

    @Override
    public void rollbackValues(long maxValue) {
        flushAllPending();

        if (genCount == 0 && keyCount == 0) {
            setMaxValue(maxValue);
            return;
        }

        LOG.info().$("rollback BP index [maxValue=").$(maxValue).$(", genCount=").$(genCount).$(", keyCount=").$(keyCount).$(']').$();
        rollbackToMaxValue(maxValue);
    }

    /**
     * Precise rollback: decode all generations, filter out values > maxValue,
     * re-encode surviving values into a single generation. Reuses sealFull machinery.
     */
    private void rollbackToMaxValue(long maxValue) {
        // Phase 1: Count total values per key across all generations (same as sealFull phase 1)
        long totalCountsSize = (long) keyCount * Integer.BYTES;
        long totalCountsAddr = Unsafe.malloc(totalCountsSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(totalCountsAddr, totalCountsSize, (byte) 0);

            long totalValueCount = 0;
            for (int gen = 0; gen < genCount; gen++) {
                long dirOffset = PostingIndexUtils.getGenDirOffset(gen);
                long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                long genAddr = valueMem.addressOf(genFileOffset);

                if (genKeyCount < 0) {
                    int activeKeyCount = -genKeyCount;
                    long keyIdsBase = genAddr;
                    long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
                    for (int i = 0; i < activeKeyCount; i++) {
                        int key = Unsafe.getUnsafe().getInt(keyIdsBase + (long) i * Integer.BYTES);
                        int count = Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
                        int existing = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                        Unsafe.getUnsafe().putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
                        totalValueCount += count;
                    }
                } else {
                    int sc = PostingIndexUtils.strideCount(genKeyCount);
                    int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                    for (int s = 0; s < sc; s++) {
                        int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) s * Integer.BYTES);
                        long strideAddr = genAddr + siSize + strideOff;
                        int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
                        byte mode = Unsafe.getUnsafe().getByte(strideAddr);
                        if (mode == PostingIndexUtils.STRIDE_MODE_PACKED) {
                            long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (j + 1) * Integer.BYTES)
                                        - Unsafe.getUnsafe().getInt(prefixAddr + (long) j * Integer.BYTES);
                                int existing = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                Unsafe.getUnsafe().putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
                                totalValueCount += count;
                            }
                        } else {
                            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getUnsafe().getInt(countsAddr + (long) j * Integer.BYTES);
                                int existing = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                Unsafe.getUnsafe().putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
                                totalValueCount += count;
                            }
                        }
                    }
                }
            }

            if (totalValueCount == 0) {
                truncate();
                setMaxValue(maxValue);
                return;
            }

            // Phase 2: Decode all values grouped by key (same as sealFull phase 2)
            long allValuesAddr = Unsafe.malloc(totalValueCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long keyWriteOffsetsSize = (long) keyCount * Long.BYTES;
            long keyWriteOffsetsAddr = Unsafe.malloc(keyWriteOffsetsSize, MemoryTag.NATIVE_DEFAULT);
            try {
                long writeOffset = 0;
                for (int key = 0; key < keyCount; key++) {
                    Unsafe.getUnsafe().putLong(keyWriteOffsetsAddr + (long) key * Long.BYTES, writeOffset);
                    writeOffset += Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                }

                for (int gen = 0; gen < genCount; gen++) {
                    long dirOffset = PostingIndexUtils.getGenDirOffset(gen);
                    long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                    int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                    long genAddr = valueMem.addressOf(genFileOffset);

                    if (genKeyCount < 0) {
                        int activeKeyCount = -genKeyCount;
                        int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
                        long keyIdsBase = genAddr;
                        long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
                        long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;

                        for (int i = 0; i < activeKeyCount; i++) {
                            int key = Unsafe.getUnsafe().getInt(keyIdsBase + (long) i * Integer.BYTES);
                            int count = Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
                            if (count == 0) continue;
                            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) i * Integer.BYTES);
                            long encodedAddr = genAddr + headerSize + dataOffset;
                            long keyWriteOff = Unsafe.getUnsafe().getLong(keyWriteOffsetsAddr + (long) key * Long.BYTES);
                            long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;
                            PostingIndexUtils.decodeKeyToNative(encodedAddr, count, destAddr, decodeCtx);
                            Unsafe.getUnsafe().putLong(keyWriteOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                        }
                    } else {
                        int sc = PostingIndexUtils.strideCount(genKeyCount);
                        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                        for (int s = 0; s < sc; s++) {
                            int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) s * Integer.BYTES);
                            long strideAddr = genAddr + siSize + strideOff;
                            int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
                            byte mode = Unsafe.getUnsafe().getByte(strideAddr);
                            if (mode == PostingIndexUtils.STRIDE_MODE_PACKED) {
                                int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                                long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_PACKED_BASE_OFFSET);
                                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET;
                                int packedHeaderSize = PostingIndexUtils.stridePackedHeaderSize(ks);
                                long packedDataAddr = strideAddr + packedHeaderSize;
                                for (int j = 0; j < ks; j++) {
                                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                    int startIdx = Unsafe.getUnsafe().getInt(prefixAddr + (long) j * Integer.BYTES);
                                    int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (j + 1) * Integer.BYTES) - startIdx;
                                    if (count == 0) continue;
                                    long keyWriteOff = Unsafe.getUnsafe().getLong(keyWriteOffsetsAddr + (long) key * Long.BYTES);
                                    long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;
                                    for (int i = 0; i < count; i++) {
                                        long val = FORBitmapIndexUtils.unpackValue(packedDataAddr, startIdx + i, bitWidth, baseValue);
                                        Unsafe.getUnsafe().putLong(destAddr + (long) i * Long.BYTES, val);
                                    }
                                    Unsafe.getUnsafe().putLong(keyWriteOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                                }
                            } else {
                                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                                long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
                                int bpHeaderSize = PostingIndexUtils.strideBPHeaderSize(ks);
                                for (int j = 0; j < ks; j++) {
                                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                    int count = Unsafe.getUnsafe().getInt(countsAddr + (long) j * Integer.BYTES);
                                    if (count == 0) continue;
                                    int dataOff = Unsafe.getUnsafe().getInt(offsetsBase + (long) j * Integer.BYTES);
                                    long encodedAddr = strideAddr + bpHeaderSize + dataOff;
                                    long keyWriteOff = Unsafe.getUnsafe().getLong(keyWriteOffsetsAddr + (long) key * Long.BYTES);
                                    long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;
                                    PostingIndexUtils.decodeKeyToNative(encodedAddr, count, destAddr, decodeCtx);
                                    Unsafe.getUnsafe().putLong(keyWriteOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                                }
                            }
                        }
                    }
                }

                // Phase 2.5: Filter — pre-compute per-key start offsets (from original counts),
                // then binary search for maxValue cutoff and update counts.
                // Use keyWriteOffsetsAddr (already allocated) to recompute start offsets
                // since we already used it above for write tracking.
                {
                    long cumOff = 0;
                    for (int key = 0; key < keyCount; key++) {
                        Unsafe.getUnsafe().putLong(keyWriteOffsetsAddr + (long) key * Long.BYTES, cumOff);
                        cumOff += Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    }
                }

                long survivingValueCount = 0;
                int newKeyCount = 0;
                for (int key = 0; key < keyCount; key++) {
                    int origCount = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    if (origCount == 0) continue;

                    long keyOff = Unsafe.getUnsafe().getLong(keyWriteOffsetsAddr + (long) key * Long.BYTES);
                    long keyAddr = allValuesAddr + keyOff * Long.BYTES;
                    int lo = 0, hi = origCount - 1;
                    int cutoff = -1;
                    while (lo <= hi) {
                        int mid = (lo + hi) >>> 1;
                        long midVal = Unsafe.getUnsafe().getLong(keyAddr + (long) mid * Long.BYTES);
                        if (midVal <= maxValue) {
                            cutoff = mid;
                            lo = mid + 1;
                        } else {
                            hi = mid - 1;
                        }
                    }

                    int newCount = cutoff + 1;
                    Unsafe.getUnsafe().putInt(totalCountsAddr + (long) key * Integer.BYTES, newCount);
                    survivingValueCount += newCount;
                    if (newCount > 0) {
                        newKeyCount = key + 1;
                    }
                }

                if (survivingValueCount == 0) {
                    truncate();
                    setMaxValue(maxValue);
                    return;
                }

                keyCount = newKeyCount;

                // Phase 3: Re-encode into single generation using pre-computed start offsets
                int maxPerKey = 0;
                for (int key = 0; key < keyCount; key++) {
                    int c = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    if (c > maxPerKey) maxPerKey = c;
                }
                {
                    int sc = PostingIndexUtils.strideCount(keyCount);
                    int siSize = PostingIndexUtils.strideIndexSize(keyCount);

                    long sealOffset = valueMemSize;
                    valueMem.jumpTo(sealOffset);
                    for (int i = 0; i < siSize; i += Integer.BYTES) {
                        valueMem.putInt(0);
                    }

                    long strideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_DEFAULT);
                    int maxBPHeaderSize = PostingIndexUtils.strideBPHeaderSize(PostingIndexUtils.DENSE_STRIDE);
                    int maxPackedHeaderSize = PostingIndexUtils.stridePackedHeaderSize(PostingIndexUtils.DENSE_STRIDE);
                    int maxLocalHeaderSize = Math.max(maxBPHeaderSize, maxPackedHeaderSize);
                    long localHeaderBuf = Unsafe.malloc(maxLocalHeaderSize, MemoryTag.NATIVE_DEFAULT);

                    int[] bpKeySizes = strideBpKeySizes;
                    long bpTrialBuf = 0;
                    long bpTrialBufSize = 0;

                    try {
                        for (int s = 0; s < sc; s++) {
                            int ks = PostingIndexUtils.keysInStride(keyCount, s);

                            long strideTrialSize = 0;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                strideTrialSize += PostingIndexUtils.computeMaxEncodedSize(count);
                            }
                            if (strideTrialSize > bpTrialBufSize) {
                                if (bpTrialBuf != 0) {
                                    Unsafe.free(bpTrialBuf, bpTrialBufSize, MemoryTag.NATIVE_DEFAULT);
                                }
                                bpTrialBufSize = strideTrialSize;
                                bpTrialBuf = Unsafe.malloc(bpTrialBufSize, MemoryTag.NATIVE_DEFAULT);
                            }

                            // Trial BP encode: read from keyWriteOffsetsAddr (re-used as start offsets)
                            int bpDataTotal = 0;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                if (count > 0) {
                                    long keyOff = Unsafe.getUnsafe().getLong(keyWriteOffsetsAddr + (long) key * Long.BYTES);
                                    long keyAddr = allValuesAddr + keyOff * Long.BYTES;
                                    encodeCtx.ensureCapacity(count);
                                    bpKeySizes[j] = PostingIndexUtils.encodeKeyNative(keyAddr, count, bpTrialBuf + bpDataTotal, encodeCtx);
                                } else {
                                    bpKeySizes[j] = 0;
                                }
                                bpDataTotal += bpKeySizes[j];
                            }

                            int bpHeaderSize = PostingIndexUtils.strideBPHeaderSize(ks);
                            int bpSize = bpHeaderSize + bpDataTotal;

                            // Count total values in stride and find per-stride min/max
                            int totalStrideValues = 0;
                            long strideMinValue = Long.MAX_VALUE;
                            long strideMaxValue = Long.MIN_VALUE;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                totalStrideValues += count;
                                long keyOff = Unsafe.getUnsafe().getLong(keyWriteOffsetsAddr + (long) key * Long.BYTES);
                                for (int i = 0; i < count; i++) {
                                    long val = Unsafe.getUnsafe().getLong(allValuesAddr + (keyOff + i) * Long.BYTES);
                                    if (val < strideMinValue) strideMinValue = val;
                                    if (val > strideMaxValue) strideMaxValue = val;
                                }
                            }
                            if (totalStrideValues == 0) {
                                strideMinValue = 0;
                                strideMaxValue = 0;
                            }
                            long strideRange = strideMaxValue - strideMinValue;
                            int localBitWidth = strideRange <= 0 ? 1 : FORBitmapIndexUtils.bitsNeeded(strideRange);

                            int packedHeaderSize = PostingIndexUtils.stridePackedHeaderSize(ks);
                            int packedDataSize = FORBitmapIndexUtils.packedDataSize(totalStrideValues, localBitWidth);
                            int packedSize = packedHeaderSize + packedDataSize;

                            boolean usePacked = packedSize < bpSize;

                            int strideOff = (int) (valueMem.getAppendOffset() - sealOffset - siSize);
                            Unsafe.getUnsafe().putInt(strideIndexBuf + (long) s * Integer.BYTES, strideOff);

                            if (usePacked) {
                                long localHeaderFilePos = valueMem.getAppendOffset();
                                for (int i = 0; i < packedHeaderSize; i += Integer.BYTES) {
                                    valueMem.putInt(0);
                                }

                                Unsafe.getUnsafe().setMemory(localHeaderBuf, packedHeaderSize, (byte) 0);
                                Unsafe.getUnsafe().putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_PACKED);
                                Unsafe.getUnsafe().putByte(localHeaderBuf + 1, (byte) localBitWidth);
                                Unsafe.getUnsafe().putLong(localHeaderBuf + PostingIndexUtils.STRIDE_PACKED_BASE_OFFSET, strideMinValue);
                                int cumCount = 0;
                                for (int j = 0; j <= ks; j++) {
                                    Unsafe.getUnsafe().putInt(
                                            localHeaderBuf + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET + (long) j * Integer.BYTES,
                                            cumCount);
                                    if (j < ks) {
                                        int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                        cumCount += Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                    }
                                }

                                long packedBuf = Unsafe.malloc(packedDataSize > 0 ? packedDataSize : 1, MemoryTag.NATIVE_DEFAULT);
                                try {
                                    if (packedDataSize > 0) {
                                        Unsafe.getUnsafe().setMemory(packedBuf, packedDataSize, (byte) 0);
                                    }
                                    int packIdx = 0;
                                    for (int j = 0; j < ks; j++) {
                                        int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                        int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                        long keyOff = Unsafe.getUnsafe().getLong(keyWriteOffsetsAddr + (long) key * Long.BYTES);
                                        for (int i = 0; i < count; i++) {
                                            long val = Unsafe.getUnsafe().getLong(
                                                    allValuesAddr + (keyOff + i) * Long.BYTES);
                                            packSingleValue(packedBuf, packIdx, localBitWidth, val - strideMinValue);
                                            packIdx++;
                                        }
                                    }
                                    int written = 0;
                                    while (written + Long.BYTES <= packedDataSize) {
                                        valueMem.putLong(Unsafe.getUnsafe().getLong(packedBuf + written));
                                        written += (int) Long.BYTES;
                                    }
                                    while (written < packedDataSize) {
                                        valueMem.putByte(Unsafe.getUnsafe().getByte(packedBuf + written));
                                        written++;
                                    }
                                } finally {
                                    Unsafe.free(packedBuf, packedDataSize > 0 ? packedDataSize : 1, MemoryTag.NATIVE_DEFAULT);
                                }

                                long localHeaderAddr = valueMem.addressOf(localHeaderFilePos);
                                Unsafe.getUnsafe().copyMemory(localHeaderBuf, localHeaderAddr, packedHeaderSize);
                            } else {
                                long localHeaderFilePos = valueMem.getAppendOffset();
                                for (int i = 0; i < bpHeaderSize; i += Integer.BYTES) {
                                    valueMem.putInt(0);
                                }

                                Unsafe.getUnsafe().setMemory(localHeaderBuf, bpHeaderSize, (byte) 0);
                                Unsafe.getUnsafe().putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_BP);
                                Unsafe.getUnsafe().putByte(localHeaderBuf + 1, (byte) 0);
                                long countsBase = localHeaderBuf + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                                long offsetsBase = countsBase + (long) ks * Integer.BYTES;

                                int dataOffset = 0;
                                int bpBufOffset = 0;
                                for (int j = 0; j < ks; j++) {
                                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                    int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);

                                    Unsafe.getUnsafe().putInt(countsBase + (long) j * Integer.BYTES, count);
                                    Unsafe.getUnsafe().putInt(offsetsBase + (long) j * Integer.BYTES, dataOffset);

                                    if (bpKeySizes[j] > 0) {
                                        int bytesWritten = bpKeySizes[j];
                                        int written = 0;
                                        while (written + Long.BYTES <= bytesWritten) {
                                            valueMem.putLong(Unsafe.getUnsafe().getLong(bpTrialBuf + bpBufOffset + written));
                                            written += (int) Long.BYTES;
                                        }
                                        while (written < bytesWritten) {
                                            valueMem.putByte(Unsafe.getUnsafe().getByte(bpTrialBuf + bpBufOffset + written));
                                            written++;
                                        }
                                        dataOffset += bytesWritten;
                                    }
                                    bpBufOffset += bpKeySizes[j];
                                }

                                Unsafe.getUnsafe().putInt(offsetsBase + (long) ks * Integer.BYTES, dataOffset);

                                long localHeaderAddr = valueMem.addressOf(localHeaderFilePos);
                                Unsafe.getUnsafe().copyMemory(localHeaderBuf, localHeaderAddr, bpHeaderSize);
                            }
                        }

                        int totalStrideBlocksSize = (int) (valueMem.getAppendOffset() - sealOffset - siSize);
                        Unsafe.getUnsafe().putInt(strideIndexBuf + (long) sc * Integer.BYTES, totalStrideBlocksSize);

                        long strideIndexAddr = valueMem.addressOf(sealOffset);
                        Unsafe.getUnsafe().copyMemory(strideIndexBuf, strideIndexAddr, siSize);

                        valueMemSize = valueMem.getAppendOffset();
                    } finally {
                        if (bpTrialBuf != 0) {
                            Unsafe.free(bpTrialBuf, bpTrialBufSize, MemoryTag.NATIVE_DEFAULT);
                        }
                        Unsafe.free(localHeaderBuf, maxLocalHeaderSize, MemoryTag.NATIVE_DEFAULT);
                        Unsafe.free(strideIndexBuf, siSize, MemoryTag.NATIVE_DEFAULT);
                    }

                    genCount = 1;

                    updateHeaderAndGenDirAtomically(genCount, maxValue,
                            sealOffset, (int) (valueMemSize - sealOffset), keyCount, 0, keyCount - 1);
                }

            } finally {
                Unsafe.free(allValuesAddr, totalValueCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(keyWriteOffsetsAddr, keyWriteOffsetsSize, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Unsafe.free(totalCountsAddr, totalCountsSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Override
    public void sync(boolean async) {
        // Flush pending data from native buffers to mmap'd files before syncing,
        // otherwise readers won't see buffered values.
        flushAllPending();
        if (keyMem.isOpen()) {
            keyMem.sync(async);
        }
        if (valueMem.isOpen()) {
            valueMem.sync(async);
        }
    }

    @Override
    public void closeNoTruncate() {
        close();
    }

    @Override
    public void clear() {
        close();
    }

    public void of(Path path, CharSequence name, long columnNameTxn, boolean init) {
        close();
        final int plen = path.size();
        boolean kFdUnassigned = true;

        try {
            LPSZ keyFile = PostingIndexUtils.keyFileName(path, name, columnNameTxn);

            if (init) {
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                this.blockCapacity = BLOCK_CAPACITY;
                initKeyMemory(keyMem, blockCapacity);
                kFdUnassigned = false;
            } else {
                if (!ff.exists(keyFile)) {
                    throw CairoException.critical(0).put("index does not exist [path=").put(path).put(']');
                }

                long keyFileSize = ff.length(keyFile);
                if (keyFileSize < KEY_FILE_RESERVED) {
                    throw CairoException.critical(0)
                            .put("Index file too short [expected>=").put(KEY_FILE_RESERVED)
                            .put(", actual=").put(keyFileSize).put(']');
                }

                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), -1L, MemoryTag.MMAP_INDEX_WRITER);
                kFdUnassigned = false;

                byte sig = keyMem.getByte(KEY_RESERVED_OFFSET_SIGNATURE);
                if (sig != SIGNATURE) {
                    throw CairoException.critical(0)
                            .put("Unknown format: invalid BP index signature [expected=").put(SIGNATURE)
                            .put(", actual=").put(sig).put(']');
                }
            }

            this.valueMemSize = keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            this.blockCapacity = keyMem.getInt(KEY_RESERVED_OFFSET_BLOCK_CAPACITY);
            this.keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
            this.genCount = keyMem.getInt(KEY_RESERVED_OFFSET_GEN_COUNT);

            valueMem.of(
                    ff,
                    PostingIndexUtils.valueFileName(path.trimTo(plen), name, columnNameTxn),
                    configuration.getDataIndexValueAppendPageSize(),
                    init ? 0 : valueMemSize,
                    MemoryTag.MMAP_INDEX_WRITER
            );

            if (!init && valueMemSize > 0) {
                long actualFileSize = ff.length(valueMem.getFd());
                if (actualFileSize >= 0 && actualFileSize < valueMemSize) {
                    LOG.advisory().$("value file shorter than header claims [expected=").$(valueMemSize)
                            .$(", actual=").$(actualFileSize).$("] — possible incomplete seal").$();
                }
                valueMem.jumpTo(valueMemSize);
                compactValueFile();
            }

            allocateNativeBuffers();
        } catch (Throwable e) {
            close();
            if (kFdUnassigned) {
                LOG.error().$("could not open BP index [path=").$(path).$(']').$();
            }
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void truncate() {
        freeNativeBuffers();
        initKeyMemory(keyMem, blockCapacity);
        valueMem.truncate();
        keyCount = 0;
        valueMemSize = 0;
        genCount = 0;
        hasPendingData = false;
        activeKeyCount = 0;
        allocateNativeBuffers();
    }

    /**
     * Reclaims dead space left by append-only seal. Copies the single live
     * generation to file offset 0 so future appends start from a compact base.
     * <p>
     * Safe for concurrent readers: the source region [gen0Offset, gen0Offset+gen0Size)
     * is NOT overwritten (destination [0, gen0Size) doesn't overlap when gen0Size <= gen0Offset,
     * which is the typical case since sealing compresses data). Old readers with cached
     * offsets continue to read valid data. We do NOT truncate here — close() handles that.
     */
    private void compactValueFile() {
        if (genCount != 1 || keyCount == 0) {
            return;
        }
        long dirOffset = PostingIndexUtils.getGenDirOffset(0);
        long gen0Offset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
        if (gen0Offset == 0) {
            return; // already compact
        }
        int gen0Size = keyMem.getInt(dirOffset + GEN_DIR_OFFSET_SIZE);
        if (gen0Size <= 0) {
            return;
        }
        long src = valueMem.addressOf(gen0Offset);
        long dst = valueMem.addressOf(0);
        if (gen0Size > gen0Offset) {
            // Regions overlap — use a temp buffer since Unsafe.copyMemory is not memmove.
            long tmpBuf = Unsafe.malloc(gen0Size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().copyMemory(src, tmpBuf, gen0Size);
                Unsafe.getUnsafe().copyMemory(tmpBuf, dst, gen0Size);
            } finally {
                Unsafe.free(tmpBuf, gen0Size, MemoryTag.NATIVE_DEFAULT);
            }
        } else {
            Unsafe.getUnsafe().copyMemory(src, dst, gen0Size);
        }

        keyMem.putLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET, 0);
        valueMemSize = gen0Size;
        valueMem.jumpTo(valueMemSize);

        updateHeaderAtomically(genCount, keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE));

        LOG.info().$("compacted BP index [deadSpace=").$(gen0Offset)
                .$(", liveSize=").$(gen0Size).$(']').$();
    }

    private void allocateNativeBuffers() {
        keyCapacity = Math.max(INITIAL_KEY_CAPACITY, keyCount);
        long valBufSize = (long) keyCapacity * blockCapacity * Long.BYTES;
        long countBufSize = (long) keyCapacity * Integer.BYTES;

        pendingValuesAddr = Unsafe.malloc(valBufSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingValuesAddr, valBufSize, (byte) 0);

        pendingCountsAddr = Unsafe.malloc(countBufSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingCountsAddr, countBufSize, (byte) 0);

        activeKeyIds = new int[keyCapacity];
    }

    private void flushAllPending() {
        if (!hasPendingData || pendingCountsAddr == 0 || activeKeyCount == 0) {
            return;
        }

        // Sort active keys for the sparse format (requires ascending keyIds).
        // Skip sort if keys were added in order (common for sequential writes).
        boolean isSorted = true;
        for (int i = 1; i < activeKeyCount; i++) {
            if (activeKeyIds[i] < activeKeyIds[i - 1]) {
                isSorted = false;
                break;
            }
        }
        if (!isSorted) {
            Arrays.sort(activeKeyIds, 0, activeKeyCount);
        }

        // Count total values from active keys (pending + spilled)
        long totalValues = 0;
        for (int i = 0; i < activeKeyCount; i++) {
            int key = activeKeyIds[i];
            totalValues += Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES);
            totalValues += getSpillCount(key);
        }

        if (totalValues == 0) {
            hasPendingData = false;
            activeKeyCount = 0;
            resetSpill();
            return;
        }

        long maxValue = keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE);

        // Use sparse format: keyIds + counts + offsets (3 arrays of activeKeyCount)
        int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);

        // Reuse header buffer, growing if needed
        if (headerSize > flushHeaderBufCapacity) {
            if (flushHeaderBuf != 0) {
                Unsafe.free(flushHeaderBuf, flushHeaderBufCapacity, MemoryTag.NATIVE_DEFAULT);
            }
            flushHeaderBufCapacity = Math.max(headerSize, flushHeaderBufCapacity * 2);
            flushHeaderBuf = Unsafe.malloc(flushHeaderBufCapacity, MemoryTag.NATIVE_DEFAULT);
        }

        Unsafe.getUnsafe().setMemory(flushHeaderBuf, headerSize, (byte) 0);
        long keyIdsBase = flushHeaderBuf;
        long countsBase = flushHeaderBuf + (long) activeKeyCount * Integer.BYTES;
        long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;

        // Reserve header + data space in valueMem. Use actual value counts for tighter bound.
        long genOffset = valueMemSize;
        long maxDataSize = 0;
        for (int i = 0; i < activeKeyCount; i++) {
            int key = activeKeyIds[i];
            int count = Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES)
                    + getSpillCount(key);
            maxDataSize += PostingIndexUtils.computeMaxEncodedSize(count);
        }
        long maxGenSize = headerSize + maxDataSize;
        valueMem.jumpTo(genOffset);
        // Extend to guarantee contiguous mapped region for direct encoding
        valueMem.jumpTo(genOffset + maxGenSize);
        valueMem.jumpTo(genOffset + headerSize);

        // Encode each active key's values directly into valueMem — no intermediate buffer
        int encodedOffset = 0;
        for (int idx = 0; idx < activeKeyCount; idx++) {
            int key = activeKeyIds[idx];
            int pendingCount = Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES);
            int spillCount = getSpillCount(key);
            int count = pendingCount + spillCount;

            Unsafe.getUnsafe().putInt(keyIdsBase + (long) idx * Integer.BYTES, key);
            Unsafe.getUnsafe().putInt(countsBase + (long) idx * Integer.BYTES, count);
            Unsafe.getUnsafe().putInt(offsetsBase + (long) idx * Integer.BYTES, encodedOffset);

            long destAddr = valueMem.addressOf(genOffset + headerSize + encodedOffset);
            int bytesWritten;

            if (spillCount == 0) {
                // No spill — encode directly from pending buffer
                long keyValuesAddr = pendingValuesAddr + (long) key * blockCapacity * Long.BYTES;
                encodeCtx.ensureCapacity(count);
                bytesWritten = PostingIndexUtils.encodeKeyNative(keyValuesAddr, count, destAddr, encodeCtx);

                if (pendingCount > 0) {
                    long lastVal = Unsafe.getUnsafe().getLong(keyValuesAddr + (long) (pendingCount - 1) * Long.BYTES);
                    if (lastVal > maxValue) {
                        maxValue = lastVal;
                    }
                }
            } else {
                // Has spill — merge spill + pending into the spill buffer, then encode from it.
                // The spill buffer already has the earlier values; append pending values to it.
                if (pendingCount > 0) {
                    ensureSpillArrays(key);
                    int needed = spillCount + pendingCount;
                    int curCap = Unsafe.getUnsafe().getInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES);
                    if (needed > curCap) {
                        int newCap = Math.max(needed, curCap * 2);
                        long oldSize = (long) curCap * Long.BYTES;
                        long newSize = (long) newCap * Long.BYTES;
                        long oldAddr = Unsafe.getUnsafe().getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                        long newAddr = Unsafe.realloc(oldAddr, oldSize, newSize, MemoryTag.NATIVE_DEFAULT);
                        Unsafe.getUnsafe().putLong(spillKeyAddrsAddr + (long) key * Long.BYTES, newAddr);
                        Unsafe.getUnsafe().putInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES, newCap);
                    }
                    // Append pending values after spill values
                    long pendingSrc = pendingValuesAddr + (long) key * blockCapacity * Long.BYTES;
                    long spillAddr = Unsafe.getUnsafe().getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                    Unsafe.getUnsafe().copyMemory(pendingSrc,
                            spillAddr + (long) spillCount * Long.BYTES,
                            (long) pendingCount * Long.BYTES);
                }
                // Encode from the spill buffer (which now holds all values in order)
                long spillAddr = Unsafe.getUnsafe().getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                encodeCtx.ensureCapacity(count);
                bytesWritten = PostingIndexUtils.encodeKeyNative(spillAddr, count, destAddr, encodeCtx);

                long lastVal = Unsafe.getUnsafe().getLong(
                        spillAddr + (long) (count - 1) * Long.BYTES);
                if (lastVal > maxValue) {
                    maxValue = lastVal;
                }
            }
            encodedOffset += bytesWritten;
        }

        int totalGenSize = headerSize + encodedOffset;
        // Set actual append position (encoded data may be smaller than reserved max)
        valueMem.jumpTo(genOffset + totalGenSize);
        valueMemSize = genOffset + totalGenSize;

        long headerAddr = valueMem.addressOf(genOffset);
        Unsafe.getUnsafe().copyMemory(flushHeaderBuf, headerAddr, headerSize);

        // Min/max key from the sorted active keyIds
        int minKey = activeKeyIds[0];
        int maxKey = activeKeyIds[activeKeyCount - 1];

        long dirOffset = PostingIndexUtils.getGenDirOffset(genCount);
        keyMem.putLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET, genOffset);
        keyMem.putInt(dirOffset + GEN_DIR_OFFSET_SIZE, totalGenSize);
        // Negative genKeyCount signals sparse format
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT, -activeKeyCount);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY, minKey);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, maxKey);
        genCount++;

        updateHeaderAtomically(genCount, maxValue);

        // Clear only the active keys' pending counts (not the entire array)
        for (int i = 0; i < activeKeyCount; i++) {
            int key = activeKeyIds[i];
            Unsafe.getUnsafe().putInt(pendingCountsAddr + (long) key * Integer.BYTES, 0);
        }
        resetSpill();
        hasPendingData = false;
        activeKeyCount = 0;

        if (genCount > MAX_GEN_COUNT) {
            seal();
        }
    }

    private void spillKey(int key, int count) {
        ensureSpillArrays(key);
        int prevCount = Unsafe.getUnsafe().getInt(spillKeyCountsAddr + (long) key * Integer.BYTES);
        int needed = prevCount + count;
        // Grow per-key spill buffer if needed
        int curCap = Unsafe.getUnsafe().getInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES);
        if (needed > curCap) {
            int newCap = Math.max(needed, curCap * 2);
            newCap = Math.max(newCap, 256); // minimum 256 values
            long oldSize = (long) curCap * Long.BYTES;
            long newSize = (long) newCap * Long.BYTES;
            long oldAddr = Unsafe.getUnsafe().getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
            long newAddr = Unsafe.realloc(oldAddr, oldSize, newSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().putLong(spillKeyAddrsAddr + (long) key * Long.BYTES, newAddr);
            Unsafe.getUnsafe().putInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES, newCap);
        }
        // Copy values from pending buffer to this key's spill
        long srcAddr = pendingValuesAddr + (long) key * blockCapacity * Long.BYTES;
        long spillAddr = Unsafe.getUnsafe().getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
        Unsafe.getUnsafe().copyMemory(srcAddr, spillAddr + (long) prevCount * Long.BYTES,
                (long) count * Long.BYTES);
        Unsafe.getUnsafe().putInt(spillKeyCountsAddr + (long) key * Integer.BYTES, needed);
        hasSpillData = true;
        // Reset pending count
        Unsafe.getUnsafe().putInt(pendingCountsAddr + (long) key * Integer.BYTES, 0);
    }

    private void ensureSpillArrays(int key) {
        int needed = key + 1;
        if (spillKeyAddrsAddr == 0) {
            int cap = Math.max(keyCapacity, needed);
            long addrsSize = (long) cap * Long.BYTES;
            long countsSize = (long) cap * Integer.BYTES;
            long capsSize = (long) cap * Integer.BYTES;
            spillKeyAddrsAddr = Unsafe.malloc(addrsSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().setMemory(spillKeyAddrsAddr, addrsSize, (byte) 0);
            spillKeyCountsAddr = Unsafe.malloc(countsSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().setMemory(spillKeyCountsAddr, countsSize, (byte) 0);
            spillKeyCapacitiesAddr = Unsafe.malloc(capsSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().setMemory(spillKeyCapacitiesAddr, capsSize, (byte) 0);
            spillArraysCapacity = cap;
        } else if (needed > spillArraysCapacity) {
            int newCap = Math.max(keyCapacity, needed);
            long oldAddrsSize = (long) spillArraysCapacity * Long.BYTES;
            long newAddrsSize = (long) newCap * Long.BYTES;
            spillKeyAddrsAddr = Unsafe.realloc(spillKeyAddrsAddr, oldAddrsSize, newAddrsSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().setMemory(spillKeyAddrsAddr + oldAddrsSize, newAddrsSize - oldAddrsSize, (byte) 0);

            long oldCountsSize = (long) spillArraysCapacity * Integer.BYTES;
            long newCountsSize = (long) newCap * Integer.BYTES;
            spillKeyCountsAddr = Unsafe.realloc(spillKeyCountsAddr, oldCountsSize, newCountsSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().setMemory(spillKeyCountsAddr + oldCountsSize, newCountsSize - oldCountsSize, (byte) 0);

            long oldCapsSize = (long) spillArraysCapacity * Integer.BYTES;
            long newCapsSize = (long) newCap * Integer.BYTES;
            spillKeyCapacitiesAddr = Unsafe.realloc(spillKeyCapacitiesAddr, oldCapsSize, newCapsSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().setMemory(spillKeyCapacitiesAddr + oldCapsSize, newCapsSize - oldCapsSize, (byte) 0);

            spillArraysCapacity = newCap;
        }
    }

    private int getSpillCount(int key) {
        if (spillKeyCountsAddr == 0 || key >= spillArraysCapacity) {
            return 0;
        }
        return Unsafe.getUnsafe().getInt(spillKeyCountsAddr + (long) key * Integer.BYTES);
    }

    private void resetSpill() {
        if (!hasSpillData || spillKeyCountsAddr == 0) {
            return;
        }
        for (int i = 0; i < activeKeyCount; i++) {
            int key = activeKeyIds[i];
            if (key < spillArraysCapacity) {
                Unsafe.getUnsafe().putInt(spillKeyCountsAddr + (long) key * Integer.BYTES, 0);
                // Keep the allocated buffer for reuse, just reset count
            }
        }
        hasSpillData = false;
    }

    private void freeNativeBuffers() {
        if (pendingValuesAddr != 0) {
            Unsafe.free(pendingValuesAddr, (long) keyCapacity * blockCapacity * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            pendingValuesAddr = 0;
        }
        if (pendingCountsAddr != 0) {
            Unsafe.free(pendingCountsAddr, (long) keyCapacity * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            pendingCountsAddr = 0;
        }
        if (spillKeyAddrsAddr != 0) {
            for (int i = 0; i < spillArraysCapacity; i++) {
                long addr = Unsafe.getUnsafe().getLong(spillKeyAddrsAddr + (long) i * Long.BYTES);
                if (addr != 0) {
                    int cap = Unsafe.getUnsafe().getInt(spillKeyCapacitiesAddr + (long) i * Integer.BYTES);
                    Unsafe.free(addr, (long) cap * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
            Unsafe.free(spillKeyAddrsAddr, (long) spillArraysCapacity * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            spillKeyAddrsAddr = 0;
            Unsafe.free(spillKeyCountsAddr, (long) spillArraysCapacity * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            spillKeyCountsAddr = 0;
            Unsafe.free(spillKeyCapacitiesAddr, (long) spillArraysCapacity * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            spillKeyCapacitiesAddr = 0;
            spillArraysCapacity = 0;
            hasSpillData = false;
        }
        if (flushHeaderBuf != 0) {
            Unsafe.free(flushHeaderBuf, flushHeaderBufCapacity, MemoryTag.NATIVE_DEFAULT);
            flushHeaderBuf = 0;
            flushHeaderBufCapacity = 0;
        }
        if (flushTmpBuf != 0) {
            Unsafe.free(flushTmpBuf, flushTmpBufCapacity, MemoryTag.NATIVE_DEFAULT);
            flushTmpBuf = 0;
            flushTmpBufCapacity = 0;
        }
        encodeCtx.close();
        keyCapacity = 0;
    }

    private void growKeyBuffers(int minCapacity) {
        int newCapacity = Math.max(keyCapacity * 2, minCapacity);

        long oldValSize = (long) keyCapacity * blockCapacity * Long.BYTES;
        long newValSize = (long) newCapacity * blockCapacity * Long.BYTES;
        pendingValuesAddr = Unsafe.realloc(pendingValuesAddr, oldValSize, newValSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingValuesAddr + oldValSize, newValSize - oldValSize, (byte) 0);

        long oldCountSize = (long) keyCapacity * Integer.BYTES;
        long newCountSize = (long) newCapacity * Integer.BYTES;
        pendingCountsAddr = Unsafe.realloc(pendingCountsAddr, oldCountSize, newCountSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingCountsAddr + oldCountSize, newCountSize - oldCountSize, (byte) 0);

        activeKeyIds = Arrays.copyOf(activeKeyIds, newCapacity);

        keyCapacity = newCapacity;
    }

    /**
     * Packs a single value into a contiguous bit stream at the given index.
     */
    private static void packSingleValue(long destAddr, int index, int bitWidth, long value) {
        long bitOffset = (long) index * bitWidth;
        int byteOffset = (int) (bitOffset / 8);
        int bitShift = (int) (bitOffset % 8);

        // Read-modify-write: OR the value bits into the existing bytes
        int bytesNeeded = (bitShift + bitWidth + 7) / 8;
        long shifted = value << bitShift;
        for (int i = 0; i < bytesNeeded; i++) {
            byte existing = Unsafe.getUnsafe().getByte(destAddr + byteOffset + i);
            Unsafe.getUnsafe().putByte(destAddr + byteOffset + i, (byte) (existing | (byte) shifted));
            shifted >>>= 8;
        }
    }

    /**
     * Atomically publishes a sealed generation by writing the gen dir[0] entry
     * AND the header within the same sequence window. This prevents a concurrent
     * reader from seeing the modified gen dir[0] (pointing to sealed data) while
     * still using the old genCount, which would cause it to read both the sealed
     * gen and the old sparse gens — producing duplicate values.
     */
    private void updateHeaderAndGenDirAtomically(int genCount, long maxValue,
                                                  long genFileOffset, int genSize,
                                                  int genKeyCount, int minKey, int maxKey) {
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();

        // Gen dir[0] — written inside the sequence window so readers never see
        // a partially-updated state (old genCount with new gen dir[0])
        long dirOffset = PostingIndexUtils.getGenDirOffset(0);
        keyMem.putLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET, genFileOffset);
        keyMem.putInt(dirOffset + GEN_DIR_OFFSET_SIZE, genSize);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT, genKeyCount);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY, minKey);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, maxKey);

        // Header fields
        keyMem.putLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        keyMem.putInt(KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        keyMem.putInt(KEY_RESERVED_OFFSET_GEN_COUNT, genCount);
        keyMem.putLong(KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private void updateHeaderAtomically(int genCount, long maxValue) {
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        keyMem.putInt(KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        keyMem.putInt(KEY_RESERVED_OFFSET_GEN_COUNT, genCount);
        keyMem.putLong(KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private static class TestFwdCursor implements RowCursor {
        private final LongList values;
        private int position;

        TestFwdCursor(LongList values) {
            this.values = values;
            this.position = 0;
        }

        @Override
        public boolean hasNext() {
            return position < values.size();
        }

        @Override
        public long next() {
            return values.getQuick(position++);
        }
    }
}

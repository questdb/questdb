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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
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
 * Delta + FoR64 BitPacking bitmap index writer.
 * <p>
 * Each commit appends one generation (covering all keys) to the value file.
 * No symbol table needed — encoding is purely arithmetic.
 */
public class PostingIndexWriter implements IndexWriter {
    private static final int INITIAL_KEY_CAPACITY = 64;
    private static final int MAX_GEN_COUNT = PostingIndexUtils.MAX_GEN_COUNT;
    private static final Log LOG = LogFactory.getLog(PostingIndexWriter.class);

    private final double alignedBitWidthThreshold;
    private final CairoConfiguration configuration;
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    private final MemoryMARW valueMem = Vm.getCMARWInstance();
    private final MemoryMARW sealValueMem = Vm.getCMARWInstance();

    private final PostingIndexUtils.DecodeContext decodeCtx = new PostingIndexUtils.DecodeContext();
    private final PostingIndexUtils.EncodeContext encodeCtx = new PostingIndexUtils.EncodeContext();
    private final FilesFacade ff;
    private final int[] strideBpKeySizes = new int[PostingIndexUtils.DENSE_STRIDE];
    private final int[] strideKeyCounts = new int[PostingIndexUtils.DENSE_STRIDE];
    private final long[] strideKeyOffsets = new long[PostingIndexUtils.DENSE_STRIDE];

    private static final int PENDING_SLOT_CAPACITY = 8;
    private int activeKeyCount;
    private int[] activeKeyIds = new int[INITIAL_KEY_CAPACITY];
    private long activePageOffset;
    private int blockCapacity;
    private long columnNameTxn;
    private boolean sealBumpedTxn;
    private MemoryMARW sealTarget; // points to sealValueMem during seal, valueMem during flush
    private int coverCount;
    private long[] coveredColumnAddrs;
    private MemoryMA[] coveredColumnMems;
    private int[] coveredColumnIndices;
    private int[] coveredColumnShifts;
    private long[] coveredColumnTops;
    private int[] coveredColumnTypes;
    private long flushHeaderBuf;
    private int flushHeaderBufCapacity;
    private int genCount;
    private boolean hasPendingData;
    private boolean hasSpillData;
    private int keyCapacity;
    private int keyCount;
    private String indexName;
    private long packedResidualsAddr;
    private int packedResidualsCapacity;
    private String partitionPath;
    private long pendingCountsAddr;
    private long pendingValuesAddr;
    private MemoryMARW[] sidecarMems;
    private MemoryMARW sidecarInfoMem;
    private int spillArraysCapacity;
    private long spillKeyAddrsAddr;
    private long spillKeyCapacitiesAddr;
    private long spillKeyCountsAddr;
    private long unpackBatchAddr;
    private int unpackBatchCapacity;
    private long valueMemSize;

    public PostingIndexWriter(CairoConfiguration configuration) {
        this.alignedBitWidthThreshold = configuration.getPostingIndexAlignedBitWidthThreshold();
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

        // Zero-fill both 4KB pages (8192 bytes total)
        for (int i = 0; i < KEY_FILE_RESERVED / Long.BYTES; i++) {
            keyMem.putLong(0L);
        }

        // Overwrite Page A fields via direct memory access (the region is already mapped)
        long baseAddr = keyMem.addressOf(PostingIndexUtils.PAGE_A_OFFSET);
        // sequence_start = 1
        Unsafe.getUnsafe().putLong(baseAddr + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, 1L);
        Unsafe.getUnsafe().storeFence();
        // valueMemSize = 0 (already zeroed)
        // blockCapacity
        Unsafe.getUnsafe().putInt(baseAddr + PostingIndexUtils.PAGE_OFFSET_BLOCK_CAPACITY, blockCapacity);
        // keyCount = 0 (already zeroed)
        // maxValue = -1
        Unsafe.getUnsafe().putLong(baseAddr + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE, -1L);
        // genCount = 0 (already zeroed)
        // formatVersion
        Unsafe.getUnsafe().putInt(baseAddr + PostingIndexUtils.PAGE_OFFSET_FORMAT_VERSION, FORMAT_VERSION);
        // valueFileTxn = -1 (COLUMN_NAME_TXN_NONE) = use the column's own txn
        Unsafe.getUnsafe().putLong(baseAddr + PostingIndexUtils.PAGE_OFFSET_VALUE_FILE_TXN, -1L);
        // sequence_end = 1
        Unsafe.getUnsafe().storeFence();
        Unsafe.getUnsafe().putLong(baseAddr + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END, 1L);

        // Page B stays zeroed (seq=0), so Page A is the valid page.
    }

    static int maybeAlignBitWidth(int bitWidth, double threshold) {
        if (bitWidth <= 0 || bitWidth > 32) return bitWidth;
        int aligned;
        if (bitWidth <= 8) aligned = 8;
        else if (bitWidth <= 16) aligned = 16;
        else aligned = 32;
        if (aligned == bitWidth) return bitWidth;
        double overhead = (double) (aligned - bitWidth) / bitWidth;
        return overhead <= threshold ? aligned : bitWidth;
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

        if (count >= PENDING_SLOT_CAPACITY) {
            // Buffer full for this key — spill its values to the overflow buffer
            // instead of flushing ALL keys. This prevents gen-count explosion
            // when a single hot key drives all overflows.
            spillKey(key, count);
            count = 0;
        }

        if (count > 0) {
            long lastVal = Unsafe.getUnsafe().getLong(
                    pendingValuesAddr + ((long) key * PENDING_SLOT_CAPACITY + count - 1) * Long.BYTES);
            if (value < lastVal) {
                throw CairoException.critical(0)
                        .put("index values must be added in ascending order [lastValue=")
                        .put(lastVal).put(", newValue=").put(value).put(']');
            }
        } else {
            int spillCount = getSpillCount(key);
            if (spillCount > 0) {
                long spillAddr = Unsafe.getUnsafe().getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                long lastSpilledVal = Unsafe.getUnsafe().getLong(spillAddr + (long) (spillCount - 1) * Long.BYTES);
                if (value < lastSpilledVal) {
                    throw CairoException.critical(0)
                            .put("index values must be added in ascending order [lastSpilledValue=")
                            .put(lastSpilledVal).put(", newValue=").put(value).put(']');
                }
            } else {
                activeKeyIds[activeKeyCount++] = key;
            }
        }

        Unsafe.getUnsafe().putLong(
                pendingValuesAddr + ((long) key * PENDING_SLOT_CAPACITY + count) * Long.BYTES, value);
        Unsafe.getUnsafe().putInt(pendingCountsAddr + (long) key * Integer.BYTES, count + 1);

        if (key >= keyCount) {
            keyCount = key + 1;
        }
        hasPendingData = true;
    }

    @Override
    public void close() {
        try {
            if (keyMem.isOpen() && partitionPath != null) {
                // Check if covered column files are still open before seal.
                if (coveredColumnMems != null && coveredColumnMems.length > 0
                        && !coveredColumnMems[0].isOpen()) {
                    coveredColumnMems = null;
                }
                seal();
            }
        } finally {
            try {
                if (keyMem.isOpen()) {
                    try {
                        keyMem.setSize(KEY_FILE_RESERVED);
                    } finally {
                        Misc.free(keyMem);
                    }
                }
            } finally {
                try {
                    Misc.free(sealValueMem);
                    Misc.free(valueMem);
                } finally {
                    closeSidecarMems();
                    freeNativeBuffers();
                    keyCount = 0;
                    valueMemSize = 0;
                    genCount = 0;
                    hasPendingData = false;
                    activeKeyCount = 0;
                    coverCount = 0;
                    sealBumpedTxn = false;
                    sealTarget = null;
                }
            }
        }
    }

    @Override
    public void commit() {
        flushAllPending();
        if (configuration.getCommitMode() != CommitMode.NOSYNC) {
            sync(configuration.getCommitMode() == CommitMode.ASYNC);
        }
    }

    public void configureCovering(
            long[] coveredColumnAddrs,
            long[] coveredColumnTops,
            int[] coveredColumnShifts,
            int[] coveredColumnIndices,
            int[] coveredColumnTypes,
            int coverCount
    ) {
        this.coveredColumnAddrs = coveredColumnAddrs;
        this.coveredColumnMems = null;
        this.coveredColumnTops = coveredColumnTops;
        this.coveredColumnShifts = coveredColumnShifts;
        this.coveredColumnIndices = coveredColumnIndices;
        this.coveredColumnTypes = coveredColumnTypes;
        this.coverCount = coverCount;
    }

    public void configureCovering(
            MemoryMA[] coveredColumnMems,
            long[] coveredColumnTops,
            int[] coveredColumnShifts,
            int[] coveredColumnIndices,
            int[] coveredColumnTypes,
            int coverCount
    ) {
        this.coveredColumnMems = coveredColumnMems;
        this.coveredColumnAddrs = null;
        this.coveredColumnTops = coveredColumnTops;
        this.coveredColumnShifts = coveredColumnShifts;
        this.coveredColumnIndices = coveredColumnIndices;
        this.coveredColumnTypes = coveredColumnTypes;
        this.coverCount = coverCount;
    }

    /**
     * Seal the index: decode all generations, merge, re-encode into a single generation.
     * Uses incremental seal (dirty-stride) when gen 0 is dense and remaining gens are sparse.
     */
    /**
     * Rebuilds covering sidecar files for an already-sealed posting index.
     * Called after O3 merge, which rebuilds the posting index but doesn't
     * write sidecar files (the O3 pool writer has no covering configuration).
     */
    public void rebuildSidecars() {
        if (coverCount <= 0 || genCount == 0 || keyCount == 0) {
            return;
        }
        // Open sidecar files and seal to write sidecar data.
        // This handles both sparse gens (after O3 commit) and dense gens
        // (after a prior seal without covering configuration).
        if (sidecarMems != null) {
            closeSidecarMems();
        }
        if (partitionPath != null) {
            try (Path p = new Path().of(partitionPath)) {
                openSidecarFiles(p, indexName, columnNameTxn);
            }
        }
        long gen0DirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, 0);
        int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
        if (gen0KeyCount >= 0) {
            // Already dense — just rewrite sidecars
            sealFull();
        } else {
            // Sparse — full seal converts to dense + writes sidecars
            seal();
        }
    }

    public void seal() {
        if (!keyMem.isOpen()) {
            return;
        }
        flushAllPending();

        if (genCount == 0 || keyCount == 0) {
            return;
        }

        // Single sparse generation: seal to convert to stride-indexed dense format
        // (enables flat mode compression which can be significantly smaller)
        if (genCount == 1) {
            long gen0DirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, 0);
            int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
            if (gen0KeyCount >= 0) {
                // Already dense — nothing to do (sidecar files from previous seal are valid)
                return;
            }
        }

        // Buffer old sidecar data from disk before openSidecarFiles truncates them.
        // Needed for incremental seal to copy clean stride sidecar blocks.
        long[] savedSidecarBufs = null;
        long[] savedSidecarSizes = null;
        if (coverCount > 0 && partitionPath != null) {
            try (Path p = new Path().of(partitionPath)) {
                int pp = p.size();
                savedSidecarBufs = new long[coverCount];
                savedSidecarSizes = new long[coverCount];
                for (int c = 0; c < coverCount; c++) {
                    LPSZ pcFile = PostingIndexUtils.coverDataFileName(p.trimTo(pp), indexName, columnNameTxn, c);
                    if (ff.exists(pcFile)) {
                        long fileLen = ff.length(pcFile);
                        if (fileLen > 0) {
                            long fd = ff.openRO(pcFile);
                            if (fd >= 0) {
                                try {
                                    long mapped = ff.mmap(fd, fileLen, 0, Files.MAP_RO, MemoryTag.MMAP_INDEX_WRITER);
                                    if (mapped > 0) {
                                        savedSidecarBufs[c] = Unsafe.malloc(fileLen, MemoryTag.NATIVE_INDEX_READER);
                                        savedSidecarSizes[c] = fileLen;
                                        Unsafe.getUnsafe().copyMemory(mapped, savedSidecarBufs[c], fileLen);
                                        ff.munmap(mapped, fileLen, MemoryTag.MMAP_INDEX_WRITER);
                                    }
                                } finally {
                                    ff.close(fd);
                                }
                            }
                        }
                    }
                    p.trimTo(pp);
                }
            }
            if (sidecarMems != null) {
                closeSidecarMems();
            }
        }

        try {
            // Open sidecar files (truncates to 0 and starts fresh)
            if (coverCount > 0 && sidecarMems == null && partitionPath != null) {
                try (Path p = new Path().of(partitionPath)) {
                    openSidecarFiles(p, indexName, columnNameTxn);
                }
            }

            if (genCount == 1) {
                sealFull();
                return;
            }

            // Check if incremental seal is possible:
            // gen 0 must be dense, and all subsequent gens must be sparse
            long gen0DirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, 0);
            int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
            boolean isIncrementalCandidate = gen0KeyCount >= 0;

            if (isIncrementalCandidate) {
                for (int g = 1; g < genCount; g++) {
                    long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, g);
                    int gkc = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                    if (gkc >= 0) {
                        isIncrementalCandidate = false;
                        break;
                    }
                }
            }

            if (isIncrementalCandidate && gen0KeyCount == keyCount) {
                sealIncremental(savedSidecarBufs, savedSidecarSizes);
                savedSidecarBufs = null; // ownership transferred
            } else {
                sealFull();
            }
        } finally {
            if (savedSidecarBufs != null) {
                for (int c = 0; c < savedSidecarBufs.length; c++) {
                    if (savedSidecarBufs[c] != 0) {
                        Unsafe.free(savedSidecarBufs[c], savedSidecarSizes[c], MemoryTag.NATIVE_INDEX_READER);
                    }
                }
            }
        }
    }

    /**
     * Incremental seal: only re-encode dirty strides (those touched by sparse gens 1..N).
     * Clean strides are copied verbatim from the existing dense gen 0.
     */
    private void sealIncremental(long[] savedSidecarBufs, long[] savedSidecarSizes) {
        int sc = PostingIndexUtils.strideCount(keyCount);
        long dirtyStridesAddr = Unsafe.malloc(sc, MemoryTag.NATIVE_INDEX_READER);
        int dirtyCount;
        try {
            Unsafe.getUnsafe().setMemory(dirtyStridesAddr, sc, (byte) 0);
            dirtyCount = 0;
            for (int g = 1; g < genCount; g++) {
                long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, g);
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
        } catch (Throwable t) {
            Unsafe.free(dirtyStridesAddr, sc, MemoryTag.NATIVE_INDEX_READER);
            throw t;
        }

        // If all strides are dirty, fall back to full seal (no savings).
        // Free savedSidecarBufs since sealFull rebuilds sidecars from scratch.
        if (dirtyCount == sc) {
            Unsafe.free(dirtyStridesAddr, sc, MemoryTag.NATIVE_INDEX_READER);
            if (savedSidecarBufs != null) {
                for (int c = 0; c < savedSidecarBufs.length; c++) {
                    if (savedSidecarBufs[c] != 0) {
                        Unsafe.free(savedSidecarBufs[c], savedSidecarSizes[c], MemoryTag.NATIVE_INDEX_READER);
                        savedSidecarBufs[c] = 0;
                    }
                }
            }
            sealFull();
            return;
        }

        // Read gen 0 metadata — do NOT cache gen0Addr here because valueMem
        // may be remapped (mremap) when the seal loop extends it to write
        // new stride data. Use gen0FileOffset and recompute the address each
        // time it's needed.
        long gen0DirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, 0);
        long gen0FileOffset = keyMem.getLong(gen0DirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
        long gen0DataSize = keyMem.getLong(gen0DirOffset + GEN_DIR_OFFSET_SIZE);
        int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
        int gen0SiSize = PostingIndexUtils.strideIndexSize(gen0KeyCount);

        // Allocate output buffers — initialize to 0 so the finally block
        // can conditionally free only those that were successfully allocated.
        int siSize = PostingIndexUtils.strideIndexSize(keyCount);
        int maxPerKey = estimateMaxPerKey(valueMem.addressOf(gen0FileOffset), gen0KeyCount, gen0SiSize);
        // Scan sparse gens for actual max per-key count — the spill mechanism
        // can produce counts >> BLOCK_CAPACITY, so BLOCK_CAPACITY * genCount
        // is not a safe upper bound.
        int sparseMaxPerKey = 0;
        for (int g = 1; g < genCount; g++) {
            long gDirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, g);
            long gFileOffset = keyMem.getLong(gDirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
            long gDataSize = keyMem.getLong(gDirOffset + GEN_DIR_OFFSET_SIZE);
            int gKeyCount = keyMem.getInt(gDirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
            if (gKeyCount >= 0 || gDataSize == 0) continue; // dense or empty
            int activeKeys = -gKeyCount;
            valueMem.extend(gFileOffset + gDataSize);
            long gAddr = valueMem.addressOf(gFileOffset);
            long countsBase = gAddr + (long) activeKeys * Integer.BYTES;
            for (int i = 0; i < activeKeys; i++) {
                int c = Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
                if (c > sparseMaxPerKey) sparseMaxPerKey = c;
            }
        }
        int maxMergedPerKey = maxPerKey + sparseMaxPerKey * (genCount - 1);
        long perKeyBufSize = PostingIndexUtils.computeMaxEncodedSize(Math.max(maxMergedPerKey, PostingIndexUtils.BLOCK_CAPACITY));
        long maxBPStrideDataSize = PostingIndexUtils.DENSE_STRIDE * perKeyBufSize;
        int maxHeaderSize = Math.max(
                PostingIndexUtils.strideDeltaHeaderSize(PostingIndexUtils.DENSE_STRIDE),
                PostingIndexUtils.strideFlatHeaderSize(PostingIndexUtils.DENSE_STRIDE)
        );
        long maxPerStride = (long) PostingIndexUtils.DENSE_STRIDE * maxMergedPerKey;
        long mergedValuesSize = Math.max(maxPerStride, 1024) * Long.BYTES;
        long copyBufSize = gen0DataSize;
        long copyBufAllocSize = copyBufSize > 0 ? copyBufSize : 1;
        int[] bpKeySizes = strideBpKeySizes;

        long strideIndexBuf = 0;
        long bpTrialBuf = 0;
        long localHeaderBuf = 0;
        long mergedValuesAddr = 0;
        long copyBuf = 0;
        long[] incrSidecarSiBufs = null;
        long incrSidecarBuf = 0;
        long incrSidecarBufSize = 0;
        long[] oldSidecarBufs = null;
        long[] oldSidecarSizes = null;

        try {
            strideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
            bpTrialBuf = Unsafe.malloc(maxBPStrideDataSize, MemoryTag.NATIVE_INDEX_READER);
            localHeaderBuf = Unsafe.malloc(maxHeaderSize, MemoryTag.NATIVE_INDEX_READER);
            mergedValuesAddr = Unsafe.malloc(mergedValuesSize, MemoryTag.NATIVE_INDEX_READER);
            copyBuf = Unsafe.malloc(copyBufAllocSize, MemoryTag.NATIVE_INDEX_READER);

            // Pre-allocate seal-path arrays to avoid per-stride allocations
            int preAllocPerKey = maxMergedPerKey;
            if (preAllocPerKey > unpackBatchCapacity) {
                if (unpackBatchAddr != 0) {
                    Unsafe.free(unpackBatchAddr, (long) unpackBatchCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                unpackBatchCapacity = preAllocPerKey;
                unpackBatchAddr = Unsafe.malloc((long) preAllocPerKey * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            int preAllocPerStride = (int) Math.min(maxPerStride, Integer.MAX_VALUE);
            if (preAllocPerStride > packedResidualsCapacity) {
                if (packedResidualsAddr != 0) {
                    Unsafe.free(packedResidualsAddr, (long) packedResidualsCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                packedResidualsCapacity = preAllocPerStride;
                packedResidualsAddr = Unsafe.malloc((long) preAllocPerStride * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }

            // Write sealed data to a NEW value file — the old .pv is untouched
            // so concurrent readers keep their valid mmap.
            long newTxn = Math.max(1, columnNameTxn + 1);
            openSealValueFile(newTxn);
            long sealOffset = 0;
            sealValueMem.jumpTo(0);
            // Reserve stride index
            for (int i = 0; i < siSize; i += Integer.BYTES) {
                sealValueMem.putInt(0);
            }

            // Sidecar: use saved old data (buffered in seal() before truncation)
            if (coverCount > 0 && sidecarMems != null) {
                incrSidecarSiBufs = new long[coverCount];
                oldSidecarBufs = savedSidecarBufs;
                oldSidecarSizes = savedSidecarSizes;
                for (int c = 0; c < coverCount; c++) {
                    sidecarMems[c].jumpTo(0);
                    sidecarMems[c].truncate();
                    incrSidecarSiBufs[c] = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
                    for (int i = 0; i < siSize; i += Integer.BYTES) {
                        sidecarMems[c].putInt(0);
                    }
                }
            }

            for (int s = 0; s < sc; s++) {
                int strideOff = (int) (sealValueMem.getAppendOffset() - sealOffset - siSize);
                Unsafe.getUnsafe().putInt(strideIndexBuf + (long) s * Integer.BYTES, strideOff);

                // Recompute gen0Addr each iteration because valueMem writes
                // (putBlockOfBytes, putInt, etc.) can trigger mremap which
                // moves the mapping, invalidating cached native addresses.
                long gen0Addr = valueMem.addressOf(gen0FileOffset);

                if (Unsafe.getUnsafe().getByte(dirtyStridesAddr + s) == 0) {
                    // Clean stride: copy verbatim from gen 0
                    copyStrideFromGen0(gen0Addr, gen0KeyCount, gen0SiSize, s, copyBuf, copyBufSize);
                    // Sidecar: copy old stride block verbatim
                    if (incrSidecarSiBufs != null && oldSidecarBufs != null) {
                        int oldSiSize = PostingIndexUtils.strideIndexSize(gen0KeyCount);
                        for (int c = 0; c < coverCount; c++) {
                            Unsafe.getUnsafe().putInt(
                                    incrSidecarSiBufs[c] + (long) s * Integer.BYTES,
                                    (int) (sidecarMems[c].getAppendOffset() - siSize));
                            if (oldSidecarBufs[c] != 0 && oldSidecarSizes[c] > oldSiSize) {
                                // Read old stride data range from old stride index
                                int oldStrideOff = Unsafe.getUnsafe().getInt(oldSidecarBufs[c] + (long) s * Integer.BYTES);
                                int nextStrideOff;
                                if (s + 1 < sc) {
                                    nextStrideOff = Unsafe.getUnsafe().getInt(oldSidecarBufs[c] + (long) (s + 1) * Integer.BYTES);
                                } else {
                                    nextStrideOff = (int) (oldSidecarSizes[c] - oldSiSize);
                                }
                                int strideDataSize = nextStrideOff - oldStrideOff;
                                if (strideDataSize > 0) {
                                    long oldStrideDataAddr = oldSidecarBufs[c] + oldSiSize + oldStrideOff;
                                    sidecarMems[c].putBlockOfBytes(oldStrideDataAddr, strideDataSize);
                                }
                            }
                        }
                    }
                } else {
                    // Dirty stride: decode from gen 0 + sparse gens, merge, re-encode
                    int ks = PostingIndexUtils.keysInStride(keyCount, s);
                    encodeDirtyStride(s, ks, gen0Addr, gen0KeyCount, gen0SiSize,
                            bpTrialBuf, localHeaderBuf, bpKeySizes, mergedValuesAddr);

                    // Write sidecar data for dirty stride
                    if (incrSidecarSiBufs != null) {
                        int totalStrideVals = 0;
                        for (int j = 0; j < ks; j++) {
                            totalStrideVals += strideKeyCounts[j];
                        }
                        if (totalStrideVals > 0) {
                            long neededBuf = (long) totalStrideVals * Long.BYTES;
                            if (neededBuf > incrSidecarBufSize) {
                                if (incrSidecarBuf != 0) {
                                    Unsafe.free(incrSidecarBuf, incrSidecarBufSize, MemoryTag.NATIVE_INDEX_READER);
                                }
                                incrSidecarBufSize = neededBuf;
                                incrSidecarBuf = Unsafe.malloc(incrSidecarBufSize, MemoryTag.NATIVE_INDEX_READER);
                            }
                            for (int c = 0; c < coverCount; c++) {
                                Unsafe.getUnsafe().putInt(
                                        incrSidecarSiBufs[c] + (long) s * Integer.BYTES,
                                        (int) (sidecarMems[c].getAppendOffset() - siSize));
                            }
                            writeSidecarStrideData(ks, strideKeyCounts, strideKeyOffsets,
                                    mergedValuesAddr, incrSidecarBuf, incrSidecarBufSize);
                        } else {
                            for (int c = 0; c < coverCount; c++) {
                                Unsafe.getUnsafe().putInt(
                                        incrSidecarSiBufs[c] + (long) s * Integer.BYTES,
                                        (int) (sidecarMems[c].getAppendOffset() - siSize));
                            }
                        }
                    }
                }
            }

            // Sentinel
            int totalStrideBlocksSize = (int) (sealValueMem.getAppendOffset() - sealOffset - siSize);
            Unsafe.getUnsafe().putInt(strideIndexBuf + (long) sc * Integer.BYTES, totalStrideBlocksSize);

            // Copy stride index
            long strideIndexAddr = sealValueMem.addressOf(sealOffset);
            Unsafe.getUnsafe().copyMemory(strideIndexBuf, strideIndexAddr, siSize);

            valueMemSize = sealValueMem.getAppendOffset();

            // Finalize sidecar stride indices for incremental seal
            if (incrSidecarSiBufs != null) {
                for (int c = 0; c < coverCount; c++) {
                    Unsafe.getUnsafe().putInt(
                            incrSidecarSiBufs[c] + (long) sc * Integer.BYTES,
                            (int) (sidecarMems[c].getAppendOffset() - siSize));
                    long sidecarIdxAddr = sidecarMems[c].addressOf(0);
                    Unsafe.getUnsafe().copyMemory(incrSidecarSiBufs[c], sidecarIdxAddr, siSize);
                }
            }

            genCount = 1;
            // Sync sealed file, switch writer to it, then publish metadata.
            // columnNameTxn must be set BEFORE writeMetadataPage so the
            // VALUE_FILE_TXN field in the metadata page reflects the new file.
            sealValueMem.sync(false);
            switchToSealedValueFile(newTxn);
            Unsafe.getUnsafe().storeFence();
            writeMetadataPage(genCount,
                    keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE),
                    0, sealOffset, valueMemSize - sealOffset, keyCount, 0, keyCount - 1);
        } finally {
            if (copyBuf != 0) {
                Unsafe.free(copyBuf, copyBufAllocSize, MemoryTag.NATIVE_INDEX_READER);
            }
            Unsafe.free(dirtyStridesAddr, sc, MemoryTag.NATIVE_INDEX_READER);
            if (strideIndexBuf != 0) {
                Unsafe.free(strideIndexBuf, siSize, MemoryTag.NATIVE_INDEX_READER);
            }
            if (bpTrialBuf != 0) {
                Unsafe.free(bpTrialBuf, maxBPStrideDataSize, MemoryTag.NATIVE_INDEX_READER);
            }
            if (localHeaderBuf != 0) {
                Unsafe.free(localHeaderBuf, maxHeaderSize, MemoryTag.NATIVE_INDEX_READER);
            }
            if (mergedValuesAddr != 0) {
                Unsafe.free(mergedValuesAddr, mergedValuesSize, MemoryTag.NATIVE_INDEX_READER);
            }
            if (incrSidecarBuf != 0) {
                Unsafe.free(incrSidecarBuf, incrSidecarBufSize, MemoryTag.NATIVE_INDEX_READER);
            }
            if (incrSidecarSiBufs != null) {
                for (int c = 0; c < coverCount; c++) {
                    if (incrSidecarSiBufs[c] != 0) {
                        Unsafe.free(incrSidecarSiBufs[c], siSize, MemoryTag.NATIVE_INDEX_READER);
                    }
                }
            }
            if (oldSidecarBufs != null) {
                for (int c = 0; c < coverCount; c++) {
                    if (oldSidecarBufs[c] != 0) {
                        Unsafe.free(oldSidecarBufs[c], oldSidecarSizes[c], MemoryTag.NATIVE_INDEX_READER);
                    }
                }
            }
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
            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
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

    private void copyStrideFromGen0(long gen0Addr, int gen0KeyCount, int gen0SiSize, int stride,
                                     long copyBuf, long copyBufSize) {
        // If this stride existed in gen 0, copy it; otherwise write empty
        if (stride >= PostingIndexUtils.strideCount(gen0KeyCount)) {
            // Stride didn't exist in gen 0 — write empty delta stride
            int ks = PostingIndexUtils.keysInStride(keyCount, stride);
            int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
            long headerFilePos = valueMem.getAppendOffset();
            for (int i = 0; i < deltaHeaderSize; i += Integer.BYTES) {
                valueMem.putInt(0);
            }
            // Zero header = delta mode, all counts 0, all offsets 0
            long headerAddr = valueMem.addressOf(headerFilePos);
            Unsafe.getUnsafe().setMemory(headerAddr, deltaHeaderSize, (byte) 0);
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
        // Copy via temp buffer because srcAddr may point into valueMem's mapping.
        if (strideSize <= copyBufSize) {
            Unsafe.getUnsafe().copyMemory(srcAddr, copyBuf, strideSize);
            sealTarget.putBlockOfBytes(copyBuf, strideSize);
        } else {
            long tmpBuf = Unsafe.malloc(strideSize, MemoryTag.NATIVE_INDEX_READER);
            try {
                Unsafe.getUnsafe().copyMemory(srcAddr, tmpBuf, strideSize);
                sealTarget.putBlockOfBytes(tmpBuf, strideSize);
            } finally {
                Unsafe.free(tmpBuf, strideSize, MemoryTag.NATIVE_INDEX_READER);
            }
        }
    }

    private void encodeDirtyStride(int s, int ks, long gen0Addr, int gen0KeyCount, int gen0SiSize,
                                    long bpTrialBuf, long localHeaderBuf,
                                    int[] bpKeySizes, long mergedValuesAddr) {
        // For each key in this stride, decode from gen 0 + all sparse gens, merge.
        // Store all merged values contiguously in mergedValuesAddr with per-key offsets
        // so writePackedStride can read from the pre-merged buffer without re-merging.
        int[] keyCounts = strideKeyCounts;
        long[] keyOffsets = strideKeyOffsets;

        // Merge all keys' values contiguously into mergedValuesAddr
        long cumOffset = 0;
        for (int j = 0; j < ks; j++) {
            int key = s * PostingIndexUtils.DENSE_STRIDE + j;
            keyOffsets[j] = cumOffset;
            int mergedCount = mergeKeyValues(key, gen0Addr, gen0KeyCount, gen0SiSize,
                    mergedValuesAddr + (long) cumOffset * Long.BYTES);
            keyCounts[j] = mergedCount;
            cumOffset += mergedCount;
        }

        // Trial delta encode from the pre-merged buffer (encode directly from native memory)
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
        int totalStrideValues = (int) cumOffset;
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
        int naturalBitWidth = strideRange <= 0 ? 1 : BitpackUtils.bitsNeeded(strideRange);
        int alignedBitWidth = maybeAlignBitWidth(naturalBitWidth, alignedBitWidthThreshold);

        // Compute sizes for all three options: delta, flat-natural, flat-aligned
        int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
        int deltaSize = deltaHeaderSize + bpDataTotal;

        int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);
        int naturalFlatDataSize = BitpackUtils.packedDataSize(totalStrideValues, naturalBitWidth);
        int naturalFlatSize = flatHeaderSize + naturalFlatDataSize;

        // Choose: prefer aligned flat (AVX2-friendly) if it still beats delta,
        // otherwise natural flat if it beats delta, otherwise delta.
        int localBitWidth;
        int flatDataSize;
        int flatSize;
        if (alignedBitWidth != naturalBitWidth) {
            int alignedFlatDataSize = BitpackUtils.packedDataSize(totalStrideValues, alignedBitWidth);
            int alignedFlatSize = flatHeaderSize + alignedFlatDataSize;
            if (alignedFlatSize < deltaSize) {
                // Aligned flat beats delta — use it for AVX2 decode
                localBitWidth = alignedBitWidth;
                flatDataSize = alignedFlatDataSize;
                flatSize = alignedFlatSize;
            } else if (naturalFlatSize < deltaSize) {
                // Aligned too big, but natural flat still beats delta
                localBitWidth = naturalBitWidth;
                flatDataSize = naturalFlatDataSize;
                flatSize = naturalFlatSize;
            } else {
                localBitWidth = naturalBitWidth;
                flatDataSize = naturalFlatDataSize;
                flatSize = naturalFlatSize;
            }
        } else {
            localBitWidth = naturalBitWidth;
            flatDataSize = naturalFlatDataSize;
            flatSize = naturalFlatSize;
        }

        boolean useFlat = flatSize < deltaSize;

        LOG.debug().$("stride mode [s=").$(s)
                .$(", deltaSize=").$(deltaSize)
                .$(", flatSize=").$(flatSize)
                .$(", natBW=").$(naturalBitWidth)
                .$(", alnBW=").$(localBitWidth)
                .$(", totalVals=").$(totalStrideValues)
                .$(", useFlat=").$(useFlat)
                .$(']').$();

        if (useFlat) {
            writePackedStride(ks, keyCounts, keyOffsets, localBitWidth, strideMinValue, flatHeaderSize, flatDataSize,
                    localHeaderBuf, mergedValuesAddr);
        } else {
            writeDeltaStride(ks, keyCounts, deltaHeaderSize, bpTrialBuf, bpKeySizes, bpDataTotal, localHeaderBuf);
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

            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                int startIdx = Unsafe.getUnsafe().getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES) - startIdx;
                if (count > 0) {
                    int flatHdrSize = PostingIndexUtils.strideFlatHeaderSize(ks);
                    long flatDataAddr = strideAddr + flatHdrSize;
                    if (count > unpackBatchCapacity) {
                        int newCap = Math.max(count, unpackBatchCapacity * 2);
                        if (unpackBatchAddr != 0) {
                            Unsafe.free(unpackBatchAddr, (long) unpackBatchCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                        }
                        unpackBatchCapacity = newCap;
                        unpackBatchAddr = Unsafe.malloc((long) newCap * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    }
                    BitpackUtils.unpackValuesFrom(flatDataAddr, startIdx, count, bitWidth, baseValue, unpackBatchAddr);
                    for (int i = 0; i < count; i++) {
                        Unsafe.getUnsafe().putLong(destAddr + (long) totalCount * Long.BYTES,
                                Unsafe.getUnsafe().getLong(unpackBatchAddr + (long) i * Long.BYTES));
                        totalCount++;
                    }
                }
            } else {
                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                int count = Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
                if (count > 0) {
                    long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
                    int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
                    int deltaHdrSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
                    long encodedAddr = strideAddr + deltaHdrSize + dataOffset;
                    PostingIndexUtils.decodeKeyToNative(encodedAddr, count, destAddr + (long) totalCount * Long.BYTES, decodeCtx);
                    totalCount += count;
                }
            }
        }

        // Decode from sparse gens 1..N
        for (int g = 1; g < genCount; g++) {
            long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, g);
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

    private void writePackedStride(int ks, int[] keyCounts, long[] keyOffsets,
                                    int localBitWidth, long strideMinValue, int flatHeaderSize, int flatDataSize,
                                    long localHeaderBuf, long mergedValuesAddr) {
        long headerFilePos = sealTarget.getAppendOffset();
        for (int i = 0; i < flatHeaderSize; i += Integer.BYTES) {
            sealTarget.putInt(0);
        }

        Unsafe.getUnsafe().setMemory(localHeaderBuf, flatHeaderSize, (byte) 0);
        Unsafe.getUnsafe().putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_FLAT);
        Unsafe.getUnsafe().putByte(localHeaderBuf + 1, (byte) localBitWidth);
        Unsafe.getUnsafe().putLong(localHeaderBuf + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET, strideMinValue);
        int cumCount = 0;
        for (int j = 0; j <= ks; j++) {
            Unsafe.getUnsafe().putInt(
                    localHeaderBuf + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET + (long) j * Integer.BYTES,
                    cumCount);
            if (j < ks) {
                cumCount += keyCounts[j];
            }
        }

        long packedBuf = Unsafe.malloc(flatDataSize > 0 ? flatDataSize : 1, MemoryTag.NATIVE_INDEX_READER);
        try {
            if (flatDataSize > 0) {
                Unsafe.getUnsafe().setMemory(packedBuf, flatDataSize, (byte) 0);
            }
            // Collect all residuals contiguously, then batch-pack
            int totalValues = 0;
            for (int j = 0; j < ks; j++) {
                totalValues += keyCounts[j];
            }
            if (totalValues > 0 && localBitWidth > 0) {
                if (totalValues > packedResidualsCapacity) {
                    int newCap = Math.max(totalValues, packedResidualsCapacity * 2);
                    if (packedResidualsAddr != 0) {
                        Unsafe.free(packedResidualsAddr, (long) packedResidualsCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    }
                    packedResidualsCapacity = newCap;
                    packedResidualsAddr = Unsafe.malloc((long) newCap * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                int idx = 0;
                for (int j = 0; j < ks; j++) {
                    int count = keyCounts[j];
                    long keyAddr = mergedValuesAddr + keyOffsets[j] * Long.BYTES;
                    for (int i = 0; i < count; i++) {
                        Unsafe.getUnsafe().putLong(packedResidualsAddr + (long) idx * Long.BYTES,
                                Unsafe.getUnsafe().getLong(keyAddr + (long) i * Long.BYTES) - strideMinValue);
                        idx++;
                    }
                }
                BitpackUtils.packValues(packedResidualsAddr, totalValues, 0, localBitWidth, packedBuf);
            }

            sealTarget.putBlockOfBytes(packedBuf, flatDataSize);
        } finally {
            Unsafe.free(packedBuf, flatDataSize > 0 ? flatDataSize : 1, MemoryTag.NATIVE_INDEX_READER);
        }

        long headerAddr = sealTarget.addressOf(headerFilePos);
        Unsafe.getUnsafe().copyMemory(localHeaderBuf, headerAddr, flatHeaderSize);
    }

    private void writeDeltaStride(int ks, int[] keyCounts, int deltaHeaderSize,
                                long bpTrialBuf, int[] bpKeySizes, int bpDataTotal,
                                long localHeaderBuf) {
        long headerFilePos = sealTarget.getAppendOffset();
        for (int i = 0; i < deltaHeaderSize; i += Integer.BYTES) {
            sealTarget.putInt(0);
        }

        Unsafe.getUnsafe().setMemory(localHeaderBuf, deltaHeaderSize, (byte) 0);
        Unsafe.getUnsafe().putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_DELTA);
        long countsBase = localHeaderBuf + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
        long offsetsBase = countsBase + (long) ks * Integer.BYTES;

        int dataOffset = 0;
        int bpBufOffset = 0;
        for (int j = 0; j < ks; j++) {
            Unsafe.getUnsafe().putInt(countsBase + (long) j * Integer.BYTES, keyCounts[j]);
            Unsafe.getUnsafe().putInt(offsetsBase + (long) j * Integer.BYTES, dataOffset);

            if (bpKeySizes[j] > 0) {
                int bytesWritten = bpKeySizes[j];
                sealTarget.putBlockOfBytes(bpTrialBuf + bpBufOffset, bytesWritten);
                dataOffset += bytesWritten;
            }
            bpBufOffset += bpKeySizes[j];
        }

        Unsafe.getUnsafe().putInt(offsetsBase + (long) ks * Integer.BYTES, dataOffset);

        long headerAddr = sealTarget.addressOf(headerFilePos);
        Unsafe.getUnsafe().copyMemory(localHeaderBuf, headerAddr, deltaHeaderSize);
    }

    private void switchToSealedValueFile(long newTxn) {
        // Close old .pv (leave on disk for concurrent readers)
        // Reopen valueMem on the new sealed .pv file
        long appendOffset = sealValueMem.getAppendOffset();
        Misc.free(sealValueMem); // release the seal mapping
        // Now reopen valueMem on the new file
        if (partitionPath != null) {
            Misc.free(valueMem); // close old .pv mapping (left on disk for readers)
            try (Path p = new Path().of(partitionPath)) {
                LPSZ fileName = PostingIndexUtils.valueFileName(p, indexName, newTxn);
                valueMem.of(ff, fileName,
                        configuration.getDataIndexValueAppendPageSize(), appendOffset,
                        MemoryTag.MMAP_INDEX_WRITER, configuration.getWriterFileOpenOpts(), -1);
                valueMem.jumpTo(appendOffset);
            }
        }
        columnNameTxn = newTxn;
        sealBumpedTxn = true;
    }

    private void openSealValueFile(long newTxn) {
        if (partitionPath == null) {
            return;
        }
        try (Path p = new Path().of(partitionPath)) {
            LPSZ fileName = PostingIndexUtils.valueFileName(p, indexName, newTxn);
            sealValueMem.of(ff, fileName,
                    configuration.getDataIndexValueAppendPageSize(),
                    MemoryTag.MMAP_INDEX_WRITER,
                    configuration.getWriterFileOpenOpts());
            sealValueMem.jumpTo(0);
        }
        sealTarget = sealValueMem;
    }

    private void openSidecarFiles(Path path, CharSequence name, long columnNameTxn) {
        if (coverCount <= 0) {
            return;
        }
        final int plen = path.size();
        try {
            // Write .pci info file
            sidecarInfoMem = Vm.getCMARWInstance();
            sidecarInfoMem.of(
                    ff,
                    PostingIndexUtils.coverInfoFileName(path, name, columnNameTxn),
                    configuration.getDataIndexValueAppendPageSize(),
                    0L,
                    MemoryTag.MMAP_INDEX_WRITER
            );
            path.trimTo(plen);
            sidecarInfoMem.putInt(PostingIndexUtils.COVER_INFO_MAGIC);
            sidecarInfoMem.putInt(coverCount);
            for (int c = 0; c < coverCount; c++) {
                sidecarInfoMem.putInt(coveredColumnIndices[c]);
                sidecarInfoMem.putInt(coveredColumnTypes[c]);
            }

            // Open .pc0, .pc1, ... files
            sidecarMems = new MemoryMARW[coverCount];
            for (int c = 0; c < coverCount; c++) {
                sidecarMems[c] = Vm.getCMARWInstance();
                sidecarMems[c].of(
                        ff,
                        PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, columnNameTxn, c),
                        configuration.getDataIndexValueAppendPageSize(),
                        0L,
                        MemoryTag.MMAP_INDEX_WRITER
                );
                path.trimTo(plen);
            }
        } catch (Throwable e) {
            closeSidecarMems();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    private static void writeNullSentinel(long addr, int valueSize, int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DOUBLE -> {
                Unsafe.getUnsafe().putDouble(addr, Double.NaN);
                return;
            }
            case ColumnType.FLOAT -> {
                Unsafe.getUnsafe().putFloat(addr, Float.NaN);
                return;
            }
            case ColumnType.GEOBYTE -> {
                Unsafe.getUnsafe().putByte(addr, GeoHashes.BYTE_NULL);
                return;
            }
            case ColumnType.GEOSHORT -> {
                Unsafe.getUnsafe().putShort(addr, GeoHashes.SHORT_NULL);
                return;
            }
            case ColumnType.GEOINT -> {
                Unsafe.getUnsafe().putInt(addr, GeoHashes.INT_NULL);
                return;
            }
            case ColumnType.GEOLONG -> {
                Unsafe.getUnsafe().putLong(addr, GeoHashes.NULL);
                return;
            }
            default -> {}
        }
        if (valueSize == Long.BYTES) {
            Unsafe.getUnsafe().putLong(addr, Long.MIN_VALUE);
        } else if (valueSize == Integer.BYTES) {
            Unsafe.getUnsafe().putInt(addr, Integer.MIN_VALUE);
        } else if (valueSize == Short.BYTES) {
            Unsafe.getUnsafe().putShort(addr, (short) 0);
        } else {
            Unsafe.getUnsafe().putByte(addr, (byte) 0);
        }
    }

    private void writeSidecarStrideData(
            int ks,
            int[] keyCounts,
            long[] keyOffsets,
            long mergedValuesAddr,
            long sidecarBuf,
            long sidecarBufSize
    ) {
        if (coverCount <= 0 || sidecarMems == null || (coveredColumnMems == null && coveredColumnAddrs == null)) {
            return;
        }
        // Pre-allocate workspaces once for all covered columns (maxKeyCount is the same)
        int maxKeyCount = 0;
        for (int j = 0; j < ks; j++) {
            maxKeyCount = Math.max(maxKeyCount, keyCounts[j]);
        }
        long longWorkspaceAddr = maxKeyCount > 0 ? Unsafe.malloc((long) maxKeyCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER) : 0;
        long exceptionWorkspaceAddr = maxKeyCount > 0 ? Unsafe.malloc(maxKeyCount, MemoryTag.NATIVE_INDEX_READER) : 0;

        try {
        for (int c = 0; c < coverCount; c++) {
            long colAddr = coveredColumnMems != null
                    ? coveredColumnMems[c].addressOf(0)
                    : coveredColumnAddrs[c];
            long colTop = coveredColumnTops[c];
            int shift = coveredColumnShifts[c];
            int colType = coveredColumnTypes[c];
            int valueSize = 1 << shift;

            // Per-key compressed layout: [key_offsets: ks × 4B][key_0_block][key_1_block]...
            int keyOffsetsSize = ks * Integer.BYTES;

            // Pre-allocate compress buffer for the largest key in this column type
            int compressBufSize = maxKeyCount > 0 ? AlpCompression.maxCompressedSize(maxKeyCount, colType) : 0;
            long compressBuf = compressBufSize > 0 ? Unsafe.malloc(compressBufSize, MemoryTag.NATIVE_INDEX_READER) : 0;

            try {
                // Write key offsets placeholder, then compress each key's values
                long keyOffsetsPos = sidecarMems[c].getAppendOffset();
                for (int j = 0; j < ks; j++) {
                    sidecarMems[c].putInt(0); // placeholder
                }

                for (int j = 0; j < ks; j++) {
                    int count = keyCounts[j];
                    long currentPos = sidecarMems[c].getAppendOffset();
                    long keyDataStart = keyOffsetsPos + keyOffsetsSize;
                    int keyOffset = (int) (currentPos - keyDataStart);
                    sidecarMems[c].putInt(keyOffsetsPos + (long) j * Integer.BYTES, keyOffset);

                    if (count == 0) {
                        continue;
                    }

                    // Assemble this key's raw values into sidecarBuf
                    long keyOff = keyOffsets[j];
                    long rawOffset = 0;
                    for (int i = 0; i < count; i++) {
                        long rowId = Unsafe.getUnsafe().getLong(
                                mergedValuesAddr + (keyOff + i) * Long.BYTES);
                        if (rowId < colTop) {
                            writeNullSentinel(sidecarBuf + rawOffset, valueSize, colType);
                        } else {
                            long srcOffset = (rowId - colTop) << shift;
                            if (valueSize == Long.BYTES) {
                                Unsafe.getUnsafe().putLong(sidecarBuf + rawOffset,
                                        Unsafe.getUnsafe().getLong(colAddr + srcOffset));
                            } else if (valueSize == Integer.BYTES) {
                                Unsafe.getUnsafe().putInt(sidecarBuf + rawOffset,
                                        Unsafe.getUnsafe().getInt(colAddr + srcOffset));
                            } else if (valueSize == Short.BYTES) {
                                Unsafe.getUnsafe().putShort(sidecarBuf + rawOffset,
                                        Unsafe.getUnsafe().getShort(colAddr + srcOffset));
                            } else {
                                Unsafe.getUnsafe().putByte(sidecarBuf + rawOffset,
                                        Unsafe.getUnsafe().getByte(colAddr + srcOffset));
                            }
                        }
                        rawOffset += valueSize;
                    }

                    // Compress and write
                    int compressedSize = compressSidecarBlock(sidecarBuf, count, shift, colType,
                            compressBuf, longWorkspaceAddr, exceptionWorkspaceAddr);
                    sidecarMems[c].putBlockOfBytes(compressBuf, compressedSize);
                }
            } finally {
                if (compressBuf != 0) {
                    Unsafe.free(compressBuf, compressBufSize, MemoryTag.NATIVE_INDEX_READER);
                }
            }
        }
        } finally {
            if (longWorkspaceAddr != 0) {
                Unsafe.free(longWorkspaceAddr, (long) maxKeyCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            if (exceptionWorkspaceAddr != 0) {
                Unsafe.free(exceptionWorkspaceAddr, maxKeyCount, MemoryTag.NATIVE_INDEX_READER);
            }
        }
    }

    private static int compressSidecarBlock(long rawBuf, int valueCount, int shift, int colType,
                                              long destBuf, long longWorkspaceAddr, long exceptionWorkspaceAddr) {
        return switch (ColumnType.tagOf(colType)) {
            case ColumnType.DOUBLE ->
                    AlpCompression.compressDoubles(rawBuf, valueCount, 3, destBuf, longWorkspaceAddr, exceptionWorkspaceAddr);
            case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG ->
                    AlpCompression.compressLongs(rawBuf, valueCount, destBuf);
            case ColumnType.FLOAT, ColumnType.GEOINT, ColumnType.INT, ColumnType.SYMBOL ->
                    AlpCompression.compressInts(rawBuf, valueCount, destBuf, longWorkspaceAddr);
            case ColumnType.SHORT, ColumnType.GEOSHORT, ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE -> {
                Unsafe.getUnsafe().putInt(destBuf, valueCount);
                Unsafe.getUnsafe().copyMemory(rawBuf, destBuf + 4, (long) valueCount << shift);
                yield 4 + (valueCount << shift);
            }
            default ->
                    throw new UnsupportedOperationException("unsupported sidecar column type: " + ColumnType.nameOf(colType));
        };
    }

    private void sealFull() {
        reencodeAllGenerations(
                keyMem.getLong(activePageOffset + PAGE_OFFSET_MAX_VALUE),
                Long.MAX_VALUE
        );
    }

    /**
     * Decode all generations, optionally filter values > maxValueCutoff, then
     * re-encode surviving values into a single dense stride-indexed generation.
     *
     * @param maxValue        the maxValue to write into the metadata header
     * @param maxValueCutoff  Long.MAX_VALUE means no filtering (seal path);
     *                        any other value trims per-key values to those <= cutoff (rollback path)
     */
    private void reencodeAllGenerations(long maxValue, long maxValueCutoff) {
        boolean isRollback = maxValueCutoff < Long.MAX_VALUE;

        // Phase 1: Count total values per key across all generations
        long totalCountsSize = (long) keyCount * Integer.BYTES;
        long totalCountsAddr = Unsafe.malloc(totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
        try {
            Unsafe.getUnsafe().setMemory(totalCountsAddr, totalCountsSize, (byte) 0);

            long totalValueCount = 0;
            for (int gen = 0; gen < genCount; gen++) {
                long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, gen);
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
                    // Dense format — stride-indexed (supports delta and flat modes)
                    int sc = PostingIndexUtils.strideCount(genKeyCount);
                    int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                    for (int s = 0; s < sc; s++) {
                        int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) s * Integer.BYTES);
                        long strideAddr = genAddr + siSize + strideOff;
                        int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
                        byte mode = Unsafe.getUnsafe().getByte(strideAddr);
                        if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                            // Flat mode: prefixCounts at STRIDE_FLAT_PREFIX_COUNTS_OFFSET
                            long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (j + 1) * Integer.BYTES)
                                        - Unsafe.getUnsafe().getInt(prefixAddr + (long) j * Integer.BYTES);
                                int existing = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                Unsafe.getUnsafe().putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
                                totalValueCount += count;
                            }
                        } else {
                            // Delta mode: counts at offset STRIDE_MODE_PREFIX_SIZE
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
                if (isRollback) {
                    truncate();
                    setMaxValue(maxValue);
                }
                return;
            }

            // Phase 2: Decode all values grouped by key into a flat buffer
            long allValuesAddr = Unsafe.malloc(totalValueCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            try {
                long keyOffsetsSize = (long) keyCount * Long.BYTES;
                long keyOffsetsAddr = Unsafe.malloc(keyOffsetsSize, MemoryTag.NATIVE_INDEX_READER);
                try {
                    // Compute per-key write offsets
                    long writeOffset = 0;
                    for (int key = 0; key < keyCount; key++) {
                        Unsafe.getUnsafe().putLong(keyOffsetsAddr + (long) key * Long.BYTES, writeOffset);
                        writeOffset += Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    }
    
                    // Pre-allocate reencodeUnpackBatch and packedResiduals to avoid
                    // per-key/per-stride allocations during the decode and re-encode loops.
                    // Scan totalCountsAddr to find max per-key count and max per-stride total.
                    int maxPerKeyCount = 0;
                    int maxStrideTotal = 0;
                    {
                        int sc0 = PostingIndexUtils.strideCount(keyCount);
                        for (int s0 = 0; s0 < sc0; s0++) {
                            int strideTotal = 0;
                            int ks0 = PostingIndexUtils.keysInStride(keyCount, s0);
                            for (int j0 = 0; j0 < ks0; j0++) {
                                int key0 = s0 * PostingIndexUtils.DENSE_STRIDE + j0;
                                int c = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key0 * Integer.BYTES);
                                if (c > maxPerKeyCount) maxPerKeyCount = c;
                                strideTotal += c;
                            }
                            if (strideTotal > maxStrideTotal) maxStrideTotal = strideTotal;
                        }
                    }
                    long reencodeUnpackBatchAddr = 0;
                    int reencodeUnpackBatchCapacity = 0;
                    if (maxPerKeyCount > 0) {
                        reencodeUnpackBatchCapacity = maxPerKeyCount;
                        reencodeUnpackBatchAddr = Unsafe.malloc((long) maxPerKeyCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    }
                    try {
                    if (maxStrideTotal > packedResidualsCapacity) {
                        if (packedResidualsAddr != 0) {
                            Unsafe.free(packedResidualsAddr, (long) packedResidualsCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                        }
                        packedResidualsCapacity = maxStrideTotal;
                        packedResidualsAddr = Unsafe.malloc((long) maxStrideTotal * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    }
                    for (int gen = 0; gen < genCount; gen++) {
                        long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, gen);
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
                                        keyOffsetsAddr + (long) key * Long.BYTES);
                                long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;
    
                                PostingIndexUtils.decodeKeyToNative(encodedAddr, count, destAddr, decodeCtx);
    
                                Unsafe.getUnsafe().putLong(
                                        keyOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                            }
                        } else {
                            // Dense format — stride-indexed (supports delta and flat modes)
                            int sc = PostingIndexUtils.strideCount(genKeyCount);
                            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                            for (int s = 0; s < sc; s++) {
                                int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) s * Integer.BYTES);
                                long strideAddr = genAddr + siSize + strideOff;
                                int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
                                byte mode = Unsafe.getUnsafe().getByte(strideAddr);
    
                                if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                                    // Flat mode
                                    int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                                    long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                                    long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                                    int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);
                                    long flatDataAddr = strideAddr + flatHeaderSize;

                                    for (int j = 0; j < ks; j++) {
                                        int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                        int startIdx = Unsafe.getUnsafe().getInt(prefixAddr + (long) j * Integer.BYTES);
                                        int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (j + 1) * Integer.BYTES) - startIdx;
                                        if (count == 0) continue;

                                        long keyWriteOff = Unsafe.getUnsafe().getLong(
                                                keyOffsetsAddr + (long) key * Long.BYTES);
                                        long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;

                                        if (count > reencodeUnpackBatchCapacity) {
                                            int newCap = Math.max(count, reencodeUnpackBatchCapacity * 2);
                                            if (reencodeUnpackBatchAddr != 0) {
                                                Unsafe.free(reencodeUnpackBatchAddr, (long) reencodeUnpackBatchCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                                            }
                                            reencodeUnpackBatchCapacity = newCap;
                                            reencodeUnpackBatchAddr = Unsafe.malloc((long) newCap * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                                        }
                                        BitpackUtils.unpackValuesFrom(flatDataAddr, startIdx, count, bitWidth, baseValue, reencodeUnpackBatchAddr);
                                        for (int i = 0; i < count; i++) {
                                            Unsafe.getUnsafe().putLong(destAddr + (long) i * Long.BYTES,
                                                    Unsafe.getUnsafe().getLong(reencodeUnpackBatchAddr + (long) i * Long.BYTES));
                                        }
    
                                        Unsafe.getUnsafe().putLong(
                                                keyOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                                    }
                                } else {
                                    // Delta mode
                                    long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                                    long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
                                    int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);

                                    for (int j = 0; j < ks; j++) {
                                        int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                        int count = Unsafe.getUnsafe().getInt(countsAddr + (long) j * Integer.BYTES);
                                        if (count == 0) continue;

                                        int dataOff = Unsafe.getUnsafe().getInt(offsetsBase + (long) j * Integer.BYTES);
                                        long encodedAddr = strideAddr + deltaHeaderSize + dataOff;
    
                                        long keyWriteOff = Unsafe.getUnsafe().getLong(
                                                keyOffsetsAddr + (long) key * Long.BYTES);
                                        long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;
    
                                        PostingIndexUtils.decodeKeyToNative(encodedAddr, count, destAddr, decodeCtx);
    
                                        Unsafe.getUnsafe().putLong(
                                                keyOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                                    }
                                }
                            }
                        }
                    }
    
                    // Filter step (rollback only): trim each key's values to those <= maxValueCutoff.
                    // Recompute per-key start offsets in keyOffsetsAddr, then binary search for cutoff.
                    if (isRollback) {
                        long cumOff = 0;
                        for (int key = 0; key < keyCount; key++) {
                            Unsafe.getUnsafe().putLong(keyOffsetsAddr + (long) key * Long.BYTES, cumOff);
                            cumOff += Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                        }
    
                        long survivingValueCount = 0;
                        int newKeyCount = 0;
                        for (int key = 0; key < keyCount; key++) {
                            int origCount = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                            if (origCount == 0) continue;
    
                            long keyOff = Unsafe.getUnsafe().getLong(keyOffsetsAddr + (long) key * Long.BYTES);
                            long keyAddr = allValuesAddr + keyOff * Long.BYTES;
                            int lo = 0, hi = origCount - 1;
                            int cutoff = -1;
                            while (lo <= hi) {
                                int mid = (lo + hi) >>> 1;
                                long midVal = Unsafe.getUnsafe().getLong(keyAddr + (long) mid * Long.BYTES);
                                if (midVal <= maxValueCutoff) {
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
                    } else {
                        // Seal path: recompute start offsets for Phase 3 (values are contiguous)
                        long cumOff = 0;
                        for (int key = 0; key < keyCount; key++) {
                            Unsafe.getUnsafe().putLong(keyOffsetsAddr + (long) key * Long.BYTES, cumOff);
                            cumOff += Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                        }
                    }
    
                    } finally {
                        if (reencodeUnpackBatchAddr != 0) {
                            Unsafe.free(reencodeUnpackBatchAddr, (long) reencodeUnpackBatchCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                        }
                    }

                    // Phase 3: Re-encode into single generation using stride-indexed format
                    // with adaptive per-stride encoding (delta vs flat).
                    // Write to a NEW value file (.pv.{txn+1}) at offset 0 — the old .pv
                    // is left untouched so concurrent readers keep their valid mmap.
                    {
                        int sc = PostingIndexUtils.strideCount(keyCount);
                        int siSize = PostingIndexUtils.strideIndexSize(keyCount);

                        // Open new value file for sealed data
                        long newTxn = Math.max(1, columnNameTxn + 1);
                        openSealValueFile(newTxn);
                        long sealOffset = 0;
                        sealValueMem.jumpTo(0);
                        for (int i = 0; i < siSize; i += Integer.BYTES) {
                            sealValueMem.putInt(0);
                        }
    
                        // Allocate stride index buffer
                        long strideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
                        int maxDeltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(PostingIndexUtils.DENSE_STRIDE);
                        int maxFlatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(PostingIndexUtils.DENSE_STRIDE);
                        int maxLocalHeaderSize = Math.max(maxDeltaHeaderSize, maxFlatHeaderSize);
                        long localHeaderBuf = Unsafe.malloc(maxLocalHeaderSize, MemoryTag.NATIVE_INDEX_READER);
    
                        // Per-key delta sizes within a stride (to compute total delta data size)
                        int[] bpKeySizes = strideBpKeySizes;
                        int[] keyCounts = strideKeyCounts;
                        long[] keyOffsets = strideKeyOffsets;
                        // Trial buffer grows dynamically per stride
                        long bpTrialBuf = 0;
                        long bpTrialBufSize = 0;

                        // Sidecar stride index buffers (one per covered column)
                        long sidecarBuf = 0;
                        long sidecarBufSize = 0;
                        long[] sidecarStrideIndexBufs = null;
                        if (coverCount > 0 && sidecarMems != null) {
                            sidecarStrideIndexBufs = new long[coverCount];
                            for (int c = 0; c < coverCount; c++) {
                                sidecarStrideIndexBufs[c] = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
                                // Reserve stride index space in sidecar file
                                for (int i = 0; i < siSize; i += Integer.BYTES) {
                                    sidecarMems[c].putInt(0);
                                }
                            }
                        }

                        try {
                            for (int s = 0; s < sc; s++) {
                                int ks = PostingIndexUtils.keysInStride(keyCount, s);
    
                                // Compute trial buffer size for this stride and collect per-key counts/offsets
                                long strideTrialSize = 0;
                                for (int j = 0; j < ks; j++) {
                                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                    int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                    keyCounts[j] = count;
                                    long keyOff = Unsafe.getUnsafe().getLong(keyOffsetsAddr + (long) key * Long.BYTES);
                                    keyOffsets[j] = keyOff;
                                    strideTrialSize += PostingIndexUtils.computeMaxEncodedSize(count);
                                }
                                if (strideTrialSize > bpTrialBufSize) {
                                    if (bpTrialBuf != 0) {
                                        Unsafe.free(bpTrialBuf, bpTrialBufSize, MemoryTag.NATIVE_INDEX_READER);
                                    }
                                    bpTrialBufSize = strideTrialSize;
                                    bpTrialBuf = Unsafe.malloc(bpTrialBufSize, MemoryTag.NATIVE_INDEX_READER);
                                }
    
                                // Trial delta encode all keys in stride
                                int bpDataTotal = 0;
                                for (int j = 0; j < ks; j++) {
                                    int count = keyCounts[j];
                                    if (count > 0) {
                                        long keyAddr = allValuesAddr + (long) keyOffsets[j] * Long.BYTES;
                                        encodeCtx.ensureCapacity(count);
                                        bpKeySizes[j] = PostingIndexUtils.encodeKeyNative(keyAddr, count, bpTrialBuf + bpDataTotal, encodeCtx);
                                    } else {
                                        bpKeySizes[j] = 0;
                                    }
                                    bpDataTotal += bpKeySizes[j];
                                }
    
                                // Compute sizes for both modes
                                int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
                                int deltaSize = deltaHeaderSize + bpDataTotal;

                                // Count total values in stride and find per-stride min/max for flat size
                                int totalStrideValues = 0;
                                long strideMinValue = Long.MAX_VALUE;
                                long strideMaxValue = Long.MIN_VALUE;
                                for (int j = 0; j < ks; j++) {
                                    int count = keyCounts[j];
                                    totalStrideValues += count;
                                    long keyAddr = allValuesAddr + (long) keyOffsets[j] * Long.BYTES;
                                    for (int i = 0; i < count; i++) {
                                        long val = Unsafe.getUnsafe().getLong(keyAddr + (long) i * Long.BYTES);
                                        if (val < strideMinValue) strideMinValue = val;
                                        if (val > strideMaxValue) strideMaxValue = val;
                                    }
                                }
                                if (totalStrideValues == 0) {
                                    strideMinValue = 0;
                                    strideMaxValue = 0;
                                }
                                long strideRange = strideMaxValue - strideMinValue;
                                int naturalBitWidth = strideRange <= 0 ? 1 : BitpackUtils.bitsNeeded(strideRange);
                                int alignedBitWidth = maybeAlignBitWidth(naturalBitWidth, alignedBitWidthThreshold);

                                int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);
                                int naturalFlatDataSize = BitpackUtils.packedDataSize(totalStrideValues, naturalBitWidth);
                                int naturalFlatSize = flatHeaderSize + naturalFlatDataSize;

                                int localBitWidth;
                                int flatDataSize;
                                int flatSize;
                                if (alignedBitWidth != naturalBitWidth) {
                                    int alignedFlatDataSize = BitpackUtils.packedDataSize(totalStrideValues, alignedBitWidth);
                                    int alignedFlatSize = flatHeaderSize + alignedFlatDataSize;
                                    if (alignedFlatSize < deltaSize) {
                                        localBitWidth = alignedBitWidth;
                                        flatDataSize = alignedFlatDataSize;
                                        flatSize = alignedFlatSize;
                                    } else if (naturalFlatSize < deltaSize) {
                                        localBitWidth = naturalBitWidth;
                                        flatDataSize = naturalFlatDataSize;
                                        flatSize = naturalFlatSize;
                                    } else {
                                        localBitWidth = naturalBitWidth;
                                        flatDataSize = naturalFlatDataSize;
                                        flatSize = naturalFlatSize;
                                    }
                                } else {
                                    localBitWidth = naturalBitWidth;
                                    flatDataSize = naturalFlatDataSize;
                                    flatSize = naturalFlatSize;
                                }

                                boolean useFlat = flatSize < deltaSize;
                                LOG.debug().$("reencode stride [s=").$(s)
                                        .$(", deltaSize=").$(deltaSize)
                                        .$(", flatSize=").$(flatSize)
                                        .$(", natBW=").$(naturalBitWidth)
                                        .$(", alnBW=").$(localBitWidth)
                                        .$(", totalVals=").$(totalStrideValues)
                                        .$(", useFlat=").$(useFlat)
                                        .$(']').$();

                                // Record stride offset (relative to end of stride index)
                                int strideOff = (int) (sealValueMem.getAppendOffset() - sealOffset - siSize);
                                Unsafe.getUnsafe().putInt(strideIndexBuf + (long) s * Integer.BYTES, strideOff);

                                if (useFlat) {
                                    writePackedStride(ks, keyCounts, keyOffsets, localBitWidth, strideMinValue,
                                            flatHeaderSize, flatDataSize, localHeaderBuf, allValuesAddr);
                                } else {
                                    writeDeltaStride(ks, keyCounts, deltaHeaderSize, bpTrialBuf, bpKeySizes,
                                            bpDataTotal, localHeaderBuf);
                                }

                                // Write sidecar covered values for this stride
                                if (coverCount > 0 && sidecarMems != null && totalStrideValues > 0) {
                                    long neededBuf = (long) totalStrideValues * Long.BYTES;
                                    if (neededBuf > sidecarBufSize) {
                                        if (sidecarBuf != 0) {
                                            Unsafe.free(sidecarBuf, sidecarBufSize, MemoryTag.NATIVE_INDEX_READER);
                                        }
                                        sidecarBufSize = neededBuf;
                                        sidecarBuf = Unsafe.malloc(sidecarBufSize, MemoryTag.NATIVE_INDEX_READER);
                                    }
                                    for (int c = 0; c < coverCount; c++) {
                                        Unsafe.getUnsafe().putInt(
                                                sidecarStrideIndexBufs[c] + (long) s * Integer.BYTES,
                                                (int) (sidecarMems[c].getAppendOffset() - siSize));
                                    }
                                    writeSidecarStrideData(ks, keyCounts, keyOffsets, allValuesAddr, sidecarBuf, sidecarBufSize);
                                }
                            }

                            // Sentinel: total size of all stride blocks
                            int totalStrideBlocksSize = (int) (sealValueMem.getAppendOffset() - sealOffset - siSize);
                            Unsafe.getUnsafe().putInt(strideIndexBuf + (long) sc * Integer.BYTES, totalStrideBlocksSize);

                            // Copy stride index into sealed value file
                            long strideIndexAddr = sealValueMem.addressOf(sealOffset);
                            Unsafe.getUnsafe().copyMemory(strideIndexBuf, strideIndexAddr, siSize);

                            valueMemSize = sealValueMem.getAppendOffset();

                            // Finalize sidecar stride indices
                            if (coverCount > 0 && sidecarStrideIndexBufs != null) {
                                for (int c = 0; c < coverCount; c++) {
                                    // Write sentinel
                                    Unsafe.getUnsafe().putInt(
                                            sidecarStrideIndexBufs[c] + (long) sc * Integer.BYTES,
                                            (int) (sidecarMems[c].getAppendOffset() - siSize));
                                    // Copy stride index to sidecar file start
                                    long sidecarIdxAddr = sidecarMems[c].addressOf(0);
                                    Unsafe.getUnsafe().copyMemory(sidecarStrideIndexBufs[c], sidecarIdxAddr, siSize);
                                }
                            }
                        } finally {
                            if (bpTrialBuf != 0) {
                                Unsafe.free(bpTrialBuf, bpTrialBufSize, MemoryTag.NATIVE_INDEX_READER);
                            }
                            if (sidecarBuf != 0) {
                                Unsafe.free(sidecarBuf, sidecarBufSize, MemoryTag.NATIVE_INDEX_READER);
                            }
                            if (sidecarStrideIndexBufs != null) {
                                for (int c = 0; c < coverCount; c++) {
                                    if (sidecarStrideIndexBufs[c] != 0) {
                                        Unsafe.free(sidecarStrideIndexBufs[c], siSize, MemoryTag.NATIVE_INDEX_READER);
                                    }
                                }
                            }
                            Unsafe.free(localHeaderBuf, maxLocalHeaderSize, MemoryTag.NATIVE_INDEX_READER);
                            Unsafe.free(strideIndexBuf, siSize, MemoryTag.NATIVE_INDEX_READER);
                        }
    
                        genCount = 1;
                        sealValueMem.sync(false);
                        switchToSealedValueFile(newTxn);
                        Unsafe.getUnsafe().storeFence();
                        writeMetadataPage(genCount, maxValue,
                                0, sealOffset, valueMemSize - sealOffset, keyCount, 0, keyCount - 1);
                    }
    
                } finally {
                    Unsafe.free(keyOffsetsAddr, keyOffsetsSize, MemoryTag.NATIVE_INDEX_READER);
                }
            } finally {
                Unsafe.free(allValuesAddr, totalValueCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
        } finally {
            Unsafe.free(totalCountsAddr, totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
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
            long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, gen);
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
                // Dense format — stride-indexed (supports delta and flat modes)
                if (key >= genKeyCount) continue;

                int stride = key / PostingIndexUtils.DENSE_STRIDE;
                int localKey = key % PostingIndexUtils.DENSE_STRIDE;
                int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) stride * Integer.BYTES);
                long strideAddr = genAddr + siSize + strideOff;
                int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
                byte mode = Unsafe.getUnsafe().getByte(strideAddr);

                if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                    // Flat mode
                    int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                    long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                    long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                    int startIdx = Unsafe.getUnsafe().getInt(prefixAddr + (long) localKey * Integer.BYTES);
                    count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES) - startIdx;
                    if (count == 0) continue;
                    int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);
                    long flatDataAddr = strideAddr + flatHeaderSize;
                    for (int i = 0; i < count; i++) {
                        values.add(BitpackUtils.unpackValue(flatDataAddr, startIdx + i, bitWidth, baseValue));
                    }
                    continue;
                }

                // Delta mode
                long countsBase = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                count = Unsafe.getUnsafe().getInt(countsBase + (long) localKey * Integer.BYTES);
                long offsetsBase = countsBase + (long) ks * Integer.BYTES;
                int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
                int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
                encodedAddr = strideAddr + deltaHeaderSize + dataOffset;
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

    public long getColumnNameTxn() {
        return columnNameTxn;
    }

    public boolean hasPendingSealTxn() {
        return sealBumpedTxn;
    }

    public void clearPendingSealTxn() {
        sealBumpedTxn = false;
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
        return keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE);
    }

    @Override
    public void setMaxValue(long maxValue) {
        // Write maxValue directly on the active page for writer-only reads.
        // Published to readers via writeMetadataPage on next commit/seal.
        keyMem.putLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE, maxValue);
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
            }

            if (init) {
                this.activePageOffset = PostingIndexUtils.PAGE_A_OFFSET;
            } else {
                determineActivePageOffset();

                int version = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_FORMAT_VERSION);
                if (version != FORMAT_VERSION) {
                    throw CairoException.critical(0)
                            .put("Unsupported Posting index version [fd=").put(keyMem.getFd())
                            .put(", expected=").put(FORMAT_VERSION)
                            .put(", actual=").put(version).put(']');
                }
            }

            this.valueMemSize = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE);
            this.blockCapacity = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_BLOCK_CAPACITY);
            this.keyCount = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT);
            this.genCount = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);

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
        if (!keyMem.isOpen()) {
            return;
        }
        flushAllPending();

        if (genCount == 0 && keyCount == 0) {
            setMaxValue(maxValue);
            return;
        }

        LOG.info().$("rollback posting index [maxValue=").$(maxValue).$(", genCount=").$(genCount).$(", keyCount=").$(keyCount).$(']').$();
        rollbackToMaxValue(maxValue);
    }

    private void rollbackToMaxValue(long maxValue) {
        reencodeAllGenerations(maxValue, maxValue);
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
        try {
            seal();
            compactValueFile();
        } finally {
            try {
                if (keyMem.isOpen()) {
                    keyMem.close(false);
                }
            } finally {
                try {
                    if (valueMem.isOpen()) {
                        valueMem.close(false);
                    }
                } finally {
                    closeSidecarMems();
                    freeNativeBuffers();
                    keyCount = 0;
                    valueMemSize = 0;
                    genCount = 0;
                    hasPendingData = false;
                    activeKeyCount = 0;
                    coverCount = 0;
                }
            }
        }
    }

    @Override
    public void clear() {
        close();
    }

    public void of(Path path, CharSequence name, long columnNameTxn, boolean init) {
        // Seal current partition before switching. This flushes buffered add()
        // calls to the mmap files and creates the sealed .pv file. Unlike the
        // bitmap index which writes directly to mmap, the posting index buffers
        // data in native memory — without this flush the data is lost.
        if (keyMem.isOpen() && partitionPath != null) {
            seal();
        }
        partitionPath = null; // prevent double-seal during close
        close();
        this.partitionPath = path.toString();
        this.indexName = name.toString();
        this.columnNameTxn = columnNameTxn;
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
            }

            if (init) {
                this.activePageOffset = PostingIndexUtils.PAGE_A_OFFSET;
            } else {
                determineActivePageOffset();

                int version = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_FORMAT_VERSION);
                if (version != FORMAT_VERSION) {
                    throw CairoException.critical(0)
                            .put("Unsupported Posting index version [expected=").put(FORMAT_VERSION)
                            .put(", actual=").put(version).put(']');
                }
            }

            this.valueMemSize = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE);
            this.blockCapacity = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_BLOCK_CAPACITY);
            this.keyCount = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT);
            this.genCount = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);

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
                LOG.error().$("could not open posting index [path=").$(path).$(']').$();
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
        activePageOffset = PostingIndexUtils.PAGE_A_OFFSET;
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
     * NOT safe for concurrent readers — must only be called when no readers
     * hold active cursors (i.e., from close() or of()). The destination region
     * [0, gen0Size) may contain old generation data that in-flight cursors
     * could still be reading.
     */
    private void closeSidecarMems() {
        if (sidecarMems != null) {
            for (int i = 0; i < sidecarMems.length; i++) {
                Misc.free(sidecarMems[i]);
                sidecarMems[i] = null;
            }
            sidecarMems = null;
        }
        if (sidecarInfoMem != null) {
            Misc.free(sidecarInfoMem);
            sidecarInfoMem = null;
        }
    }

    // Compaction copies gen 0 data from its file offset to offset 0, overwriting dead
    // space from earlier generations. This modifies MAP_SHARED pages that concurrent readers
    // may have mapped. Safety relies on the partition lifecycle: compaction runs only during
    // close() and of(), which are coordinated by the TableWriter/TableReader scoreboard
    // to ensure no reader has active cursors on this partition during writer lifecycle transitions.
    private void compactValueFile() {
        if (genCount != 1 || keyCount == 0) {
            return;
        }
        long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, 0);
        long gen0Offset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
        if (gen0Offset == 0) {
            return; // already compact
        }
        long gen0Size = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_SIZE);
        if (gen0Size <= 0) {
            return;
        }
        long src = valueMem.addressOf(gen0Offset);
        long dst = valueMem.addressOf(0);
        if (gen0Size > gen0Offset) {
            // Regions overlap — use a temp buffer since Unsafe.copyMemory is not memmove.
            long tmpBuf = Unsafe.malloc(gen0Size, MemoryTag.NATIVE_INDEX_READER);
            try {
                Unsafe.getUnsafe().copyMemory(src, tmpBuf, gen0Size);
                Unsafe.getUnsafe().copyMemory(tmpBuf, dst, gen0Size);
            } finally {
                Unsafe.free(tmpBuf, gen0Size, MemoryTag.NATIVE_INDEX_READER);
            }
        } else {
            Unsafe.getUnsafe().copyMemory(src, dst, gen0Size);
        }

        valueMemSize = gen0Size;
        valueMem.jumpTo(valueMemSize);

        // Publish compacted gen 0 with file offset 0 via double-buffer protocol
        int gen0KeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
        int gen0MinKey = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY);
        int gen0MaxKey = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY);
        Unsafe.getUnsafe().storeFence();
        writeMetadataPage(genCount,
                keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE),
                0, 0L, gen0Size, gen0KeyCount, gen0MinKey, gen0MaxKey);

        LOG.info().$("compacted posting index [deadSpace=").$(gen0Offset)
                .$(", liveSize=").$(gen0Size).$(']').$();
    }

    private void allocateNativeBuffers() {
        keyCapacity = Math.max(INITIAL_KEY_CAPACITY, keyCount);
        long valBufSize = (long) keyCapacity * PENDING_SLOT_CAPACITY * Long.BYTES;
        long countBufSize = (long) keyCapacity * Integer.BYTES;

        pendingValuesAddr = Unsafe.malloc(valBufSize, MemoryTag.NATIVE_INDEX_READER);
        Unsafe.getUnsafe().setMemory(pendingValuesAddr, valBufSize, (byte) 0);

        pendingCountsAddr = Unsafe.malloc(countBufSize, MemoryTag.NATIVE_INDEX_READER);
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

        long maxValue = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE);

        // Use sparse format: keyIds + counts + offsets (3 arrays of activeKeyCount)
        int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);

        // Reuse header buffer, growing if needed
        if (headerSize > flushHeaderBufCapacity) {
            if (flushHeaderBuf != 0) {
                Unsafe.free(flushHeaderBuf, flushHeaderBufCapacity, MemoryTag.NATIVE_INDEX_READER);
            }
            flushHeaderBufCapacity = Math.max(headerSize, flushHeaderBufCapacity * 2);
            flushHeaderBuf = Unsafe.malloc(flushHeaderBufCapacity, MemoryTag.NATIVE_INDEX_READER);
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
                long keyValuesAddr = pendingValuesAddr + (long) key * PENDING_SLOT_CAPACITY * Long.BYTES;
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
                        long newAddr = Unsafe.realloc(oldAddr, oldSize, newSize, MemoryTag.NATIVE_INDEX_READER);
                        Unsafe.getUnsafe().putLong(spillKeyAddrsAddr + (long) key * Long.BYTES, newAddr);
                        Unsafe.getUnsafe().putInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES, newCap);
                    }
                    // Append pending values after spill values
                    long pendingSrc = pendingValuesAddr + (long) key * PENDING_SLOT_CAPACITY * Long.BYTES;
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

        genCount++;
        // Ensure value-file writes are visible before publishing via metadata page
        Unsafe.getUnsafe().storeFence();
        writeMetadataPage(genCount, maxValue,
                genCount - 1, genOffset, totalGenSize, -activeKeyCount, minKey, maxKey);

        // Clear only the active keys' pending counts (not the entire array)
        for (int i = 0; i < activeKeyCount; i++) {
            int key = activeKeyIds[i];
            Unsafe.getUnsafe().putInt(pendingCountsAddr + (long) key * Integer.BYTES, 0);
        }
        resetSpill();
        hasPendingData = false;
        activeKeyCount = 0;

        // Seal at >= MAX_GEN_COUNT (not >). Entry MAX_GEN_COUNT-1 (index 124)
        // ends at page offset 4064. Entry 125 would overlap sequence_end at 4088.
        if (genCount >= MAX_GEN_COUNT) {
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
            long newAddr = Unsafe.realloc(oldAddr, oldSize, newSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.getUnsafe().putLong(spillKeyAddrsAddr + (long) key * Long.BYTES, newAddr);
            Unsafe.getUnsafe().putInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES, newCap);
        }
        // Copy values from pending buffer to this key's spill
        long srcAddr = pendingValuesAddr + (long) key * PENDING_SLOT_CAPACITY * Long.BYTES;
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
            spillKeyAddrsAddr = Unsafe.malloc(addrsSize, MemoryTag.NATIVE_INDEX_READER);
            try {
                spillKeyCountsAddr = Unsafe.malloc(countsSize, MemoryTag.NATIVE_INDEX_READER);
                try {
                    spillKeyCapacitiesAddr = Unsafe.malloc(capsSize, MemoryTag.NATIVE_INDEX_READER);
                } catch (Throwable e) {
                    Unsafe.free(spillKeyCountsAddr, countsSize, MemoryTag.NATIVE_INDEX_READER);
                    spillKeyCountsAddr = 0;
                    throw e;
                }
            } catch (Throwable e) {
                Unsafe.free(spillKeyAddrsAddr, addrsSize, MemoryTag.NATIVE_INDEX_READER);
                spillKeyAddrsAddr = 0;
                throw e;
            }
            Unsafe.getUnsafe().setMemory(spillKeyAddrsAddr, addrsSize, (byte) 0);
            Unsafe.getUnsafe().setMemory(spillKeyCountsAddr, countsSize, (byte) 0);
            Unsafe.getUnsafe().setMemory(spillKeyCapacitiesAddr, capsSize, (byte) 0);
            spillArraysCapacity = cap;
        } else if (needed > spillArraysCapacity) {
            int newCap = Math.max(keyCapacity, needed);
            long oldAddrsSize = (long) spillArraysCapacity * Long.BYTES;
            long newAddrsSize = (long) newCap * Long.BYTES;
            spillKeyAddrsAddr = Unsafe.realloc(spillKeyAddrsAddr, oldAddrsSize, newAddrsSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.getUnsafe().setMemory(spillKeyAddrsAddr + oldAddrsSize, newAddrsSize - oldAddrsSize, (byte) 0);

            long oldCountsSize = (long) spillArraysCapacity * Integer.BYTES;
            long newCountsSize = (long) newCap * Integer.BYTES;
            spillKeyCountsAddr = Unsafe.realloc(spillKeyCountsAddr, oldCountsSize, newCountsSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.getUnsafe().setMemory(spillKeyCountsAddr + oldCountsSize, newCountsSize - oldCountsSize, (byte) 0);

            long oldCapsSize = (long) spillArraysCapacity * Integer.BYTES;
            long newCapsSize = (long) newCap * Integer.BYTES;
            spillKeyCapacitiesAddr = Unsafe.realloc(spillKeyCapacitiesAddr, oldCapsSize, newCapsSize, MemoryTag.NATIVE_INDEX_READER);
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
            Unsafe.free(pendingValuesAddr, (long) keyCapacity * PENDING_SLOT_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            pendingValuesAddr = 0;
        }
        if (pendingCountsAddr != 0) {
            Unsafe.free(pendingCountsAddr, (long) keyCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
            pendingCountsAddr = 0;
        }
        if (spillKeyAddrsAddr != 0) {
            for (int i = 0; i < spillArraysCapacity; i++) {
                long addr = Unsafe.getUnsafe().getLong(spillKeyAddrsAddr + (long) i * Long.BYTES);
                if (addr != 0) {
                    int cap = Unsafe.getUnsafe().getInt(spillKeyCapacitiesAddr + (long) i * Integer.BYTES);
                    Unsafe.free(addr, (long) cap * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
            }
            Unsafe.free(spillKeyAddrsAddr, (long) spillArraysCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            spillKeyAddrsAddr = 0;
            Unsafe.free(spillKeyCountsAddr, (long) spillArraysCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
            spillKeyCountsAddr = 0;
            Unsafe.free(spillKeyCapacitiesAddr, (long) spillArraysCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
            spillKeyCapacitiesAddr = 0;
            spillArraysCapacity = 0;
            hasSpillData = false;
        }
        if (flushHeaderBuf != 0) {
            Unsafe.free(flushHeaderBuf, flushHeaderBufCapacity, MemoryTag.NATIVE_INDEX_READER);
            flushHeaderBuf = 0;
            flushHeaderBufCapacity = 0;
        }
        encodeCtx.close();
        decodeCtx.close();
        if (packedResidualsAddr != 0) {
            Unsafe.free(packedResidualsAddr, (long) packedResidualsCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            packedResidualsAddr = 0;
            packedResidualsCapacity = 0;
        }
        if (unpackBatchAddr != 0) {
            Unsafe.free(unpackBatchAddr, (long) unpackBatchCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            unpackBatchAddr = 0;
            unpackBatchCapacity = 0;
        }
        keyCapacity = 0;
    }

    private void growKeyBuffers(int minCapacity) {
        int newCapacity = Math.max(keyCapacity * 2, minCapacity);

        long oldValSize = (long) keyCapacity * PENDING_SLOT_CAPACITY * Long.BYTES;
        long newValSize = (long) newCapacity * PENDING_SLOT_CAPACITY * Long.BYTES;
        pendingValuesAddr = Unsafe.realloc(pendingValuesAddr, oldValSize, newValSize, MemoryTag.NATIVE_INDEX_READER);
        Unsafe.getUnsafe().setMemory(pendingValuesAddr + oldValSize, newValSize - oldValSize, (byte) 0);

        long oldCountSize = (long) keyCapacity * Integer.BYTES;
        long newCountSize = (long) newCapacity * Integer.BYTES;
        pendingCountsAddr = Unsafe.realloc(pendingCountsAddr, oldCountSize, newCountSize, MemoryTag.NATIVE_INDEX_READER);
        Unsafe.getUnsafe().setMemory(pendingCountsAddr + oldCountSize, newCountSize - oldCountSize, (byte) 0);

        activeKeyIds = Arrays.copyOf(activeKeyIds, newCapacity);

        keyCapacity = newCapacity;
    }

    /**
     * Determines which metadata page is currently active (has the highest valid sequence).
     * A page is valid when its sequence_start == sequence_end.
     */
    private void determineActivePageOffset() {
        long memSize = keyMem.size();
        long seqA = memSize >= PostingIndexUtils.PAGE_SIZE
                ? keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) : 0;
        long seqEndA = memSize >= PostingIndexUtils.PAGE_SIZE
                ? keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END) : 0;
        long seqB = memSize >= PostingIndexUtils.KEY_FILE_RESERVED
                ? keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) : 0;
        long seqEndB = memSize >= PostingIndexUtils.KEY_FILE_RESERVED
                ? keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END) : 0;

        long validA = (seqA == seqEndA) ? seqA : 0;
        long validB = (seqB == seqEndB) ? seqB : 0;
        activePageOffset = (validB > validA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
    }

    /**
     * Writes a new metadata page to the inactive page (double-buffer protocol).
     * Copies gen dir entries from the active page, with an optional override for one entry.
     *
     * @param genCount        number of generations
     * @param maxValue        max value to write
     * @param overrideGenIndex gen dir index to override (-1 to copy all from active page)
     * @param overrideFileOffset file offset for the overridden gen dir entry
     * @param overrideSize    size for the overridden gen dir entry
     * @param overrideKeyCount key count for the overridden gen dir entry
     * @param overrideMinKey  min key for the overridden gen dir entry
     * @param overrideMaxKey  max key for the overridden gen dir entry
     */
    private void writeMetadataPage(int genCount, long maxValue,
                                    int overrideGenIndex, long overrideFileOffset,
                                    long overrideSize, int overrideKeyCount,
                                    int overrideMinKey, int overrideMaxKey) {
        // Compute inactive page offset
        long inactivePageOffset = (activePageOffset == PostingIndexUtils.PAGE_A_OFFSET)
                ? PostingIndexUtils.PAGE_B_OFFSET
                : PostingIndexUtils.PAGE_A_OFFSET;

        // Compute new sequence = current active seq + 1
        long currentSeq = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        long newSeq = currentSeq + 1;

        // Write sequence_start first
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, newSeq);
        Unsafe.getUnsafe().storeFence();

        // Write header fields
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        keyMem.putInt(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_BLOCK_CAPACITY, blockCapacity);
        keyMem.putInt(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT, keyCount);
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE, maxValue);
        keyMem.putInt(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT, genCount);
        keyMem.putInt(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_FORMAT_VERSION, FORMAT_VERSION);
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_VALUE_FILE_TXN, columnNameTxn);

        // Bulk copy gen dir entries that already exist on the active page, then
        // overwrite or append the new entry. The active page has (genCount - 1) valid
        // entries when the caller incremented genCount before calling us.
        int activePageGenCount = (int) keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);
        int entriesToCopy = Math.min(genCount, activePageGenCount);
        if (entriesToCopy > 0) {
            long srcGenDir = keyMem.addressOf(activePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_DIR);
            long dstGenDir = keyMem.addressOf(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_DIR);
            int genDirBytes = entriesToCopy * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
            Unsafe.getUnsafe().copyMemory(srcGenDir, dstGenDir, genDirBytes);
        }
        if (overrideGenIndex >= 0) {
            long dstOffset = PostingIndexUtils.getGenDirOffset(inactivePageOffset, overrideGenIndex);
            keyMem.putLong(dstOffset + GEN_DIR_OFFSET_FILE_OFFSET, overrideFileOffset);
            keyMem.putLong(dstOffset + GEN_DIR_OFFSET_SIZE, overrideSize);
            keyMem.putInt(dstOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT, overrideKeyCount);
            keyMem.putInt(dstOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY, overrideMinKey);
            keyMem.putInt(dstOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, overrideMaxKey);
        }

        // Write sequence_end last
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END, newSeq);

        // Switch active page
        activePageOffset = inactivePageOffset;
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

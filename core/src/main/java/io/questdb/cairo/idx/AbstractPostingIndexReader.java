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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.BitSet;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

/**
 * Shared base for forward and backward posting index readers.
 * Contains index metadata management, memory mapping, generation lookup,
 * and reload logic common to both iteration directions.
 */
public abstract class AbstractPostingIndexReader implements BitmapIndexReader {

    private static final String INDEX_CORRUPT = "posting index is corrupt";
    private static final Log LOG = LogFactory.getLog(AbstractPostingIndexReader.class);
    protected final PostingGenLookup genLookup = new PostingGenLookup();
    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    protected long columnTop;
    protected int coverCount;
    protected int genCount;
    protected int keyCount;
    protected long reloadGeneration; // incremented when valueMem is remapped; cursors check for staleness
    protected int[] sidecarColumnIndices;
    protected int[] sidecarColumnTypes;
    protected MemoryMR[] sidecarMems;
    private long activePageOffset;
    private MillisecondClock clock;
    private long columnTxn;
    private FilesFacade ff;
    private int keyCountIncludingNulls;
    private long keyFileSequence = -1;
    private long partitionTxn;
    private String readerColumnName;
    private String readerPartitionPath;
    private long spinLockTimeoutMs;
    private long valueFileTxn; // txn suffix of the currently opened .pv file
    private long valueMemSize = -1;

    @Override
    public void close() {
        Misc.free(genLookup);
        Misc.free(keyMem);
        Misc.free(valueMem);
        closeSidecarMems();
    }

    @Override
    public int collectDistinctKeys(BitSet foundKeys) {
        if (genCount == 0 || keyCount == 0) {
            return 0;
        }
        int newlyFound = 0;
        for (int g = 0; g < genCount; g++) {
            int genKeyCount = genLookup.getGenKeyCount(g);
            long genFileOffset = genLookup.getGenFileOffset(g);
            long genDataSize = genLookup.getGenDataSize(g);
            if (genKeyCount >= 0) {
                newlyFound += collectDenseGenKeys(genFileOffset, genDataSize, genKeyCount, foundKeys);
            } else {
                newlyFound += collectSparseGenKeys(genFileOffset, genDataSize, -genKeyCount, foundKeys);
            }
        }
        return newlyFound;
    }

    @Override
    public long getColumnTop() {
        return columnTop;
    }

    @Override
    public long getColumnTxn() {
        return columnTxn;
    }

    /**
     * Returns the current tier used by the generation lookup.
     * Primarily for testing: 1 = per-key, 2 = SBBF, 0 = none/fallback.
     */
    public int getGenLookupTier() {
        return genLookup.getTier();
    }

    @Override
    public long getKeyBaseAddress() {
        return keyMem.addressOf(0);
    }

    @Override
    public int getKeyCount() {
        return keyCountIncludingNulls;
    }

    @Override
    public long getKeyMemorySize() {
        return keyMem.size();
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    @Override
    public long getValueBaseAddress() {
        return valueMem.addressOf(0);
    }

    /**
     * Returns 0 because PostingIndex does not use the legacy block-linked-list
     * value file layout. The only consumer (GeoHashNative.latestByAndFilterPrefix)
     * expects that layout and cannot operate on PostingIndex data regardless.
     */
    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    @Override
    public long getValueMemorySize() {
        return valueMem.size();
    }

    @Override
    public boolean isOpen() {
        return keyMem.getFd() != -1;
    }

    @Override
    public void of(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        this.columnTop = columnTop;
        this.columnTxn = columnNameTxn;
        this.partitionTxn = partitionTxn;
        this.spinLockTimeoutMs = configuration.getSpinLockTimeout();
        this.clock = configuration.getMillisecondClock();
        this.readerPartitionPath = path.toString();
        this.readerColumnName = columnName.toString();
        this.ff = configuration.getFilesFacade();
        final int plen = path.size();

        try {
            FilesFacade ff = this.ff;
            LPSZ name = PostingIndexUtils.keyFileName(path, columnName, columnNameTxn);
            keyMem.of(
                    ff,
                    name,
                    ff.getMapPageSize(),
                    PostingIndexUtils.KEY_FILE_RESERVED,
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );

            readIndexMetadataFromBestPage();

            int version = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_FORMAT_VERSION);
            if (version != 0 && version != PostingIndexUtils.FORMAT_VERSION) {
                throw CairoException.critical(0).put("Unsupported Posting index version: ").put(version);
            }

            // valueFileTxn determines which .pv file to open.
            // -1 (COLUMN_NAME_TXN_NONE) or 0 (pre-upgrade zero-fill) = same as columnNameTxn.
            // After seal, set to the bumped txn of the sealed .pv file.
            long metaValueTxn = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_VALUE_FILE_TXN);
            this.valueFileTxn = (metaValueTxn <= 0) ? columnNameTxn : metaValueTxn;

            this.valueMem.of(
                    configuration.getFilesFacade(),
                    PostingIndexUtils.valueFileName(path.trimTo(plen), columnName, valueFileTxn),
                    valueMemSize,
                    valueMemSize,
                    MemoryTag.MMAP_INDEX_READER
            );

            // Try to open sidecar files for covering index
            openSidecarFilesIfPresent(configuration, path.trimTo(plen), columnName, columnNameTxn);
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void reloadConditionally() {
        // Check both pages for a higher sequence than cached
        Unsafe.getUnsafe().loadFence();
        long seqA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        long seqB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        long maxSeq = Math.max(seqA, seqB);
        if (maxSeq != keyFileSequence) {
            readIndexMetadataFromBestPage();

            // Check if the value file txn changed (seal wrote a new .pv file)
            long metaValueTxn = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_VALUE_FILE_TXN);
            long newValueFileTxn = (metaValueTxn <= 0) ? columnTxn : metaValueTxn;
            if (newValueFileTxn != valueFileTxn && readerPartitionPath != null && ff != null) {
                // Value file changed — close old mapping and open new .pv.
                // Old .pv file stays on disk for other readers with active cursors.
                reloadGeneration++;
                valueFileTxn = newValueFileTxn;
                Misc.free(valueMem);
                try (Path p = new Path().of(readerPartitionPath)) {
                    valueMem.of(ff,
                            PostingIndexUtils.valueFileName(p, readerColumnName, valueFileTxn),
                            ff.getMapPageSize(), valueMemSize,
                            MemoryTag.MMAP_INDEX_READER, CairoConfiguration.O_NONE, -1);
                }
                // Also reopen sidecar files for covering index
                closeSidecarMems();
                try (Path p = new Path().of(readerPartitionPath)) {
                    openSidecarFilesIfPresent(p, readerColumnName, columnTxn);
                }
            } else if (valueMemSize > 0) {
                long oldAddr = valueMem.addressOf(0);
                ((MemoryCMR) this.valueMem).changeSize(valueMemSize);
                if (valueMem.addressOf(0) != oldAddr) {
                    reloadGeneration++;
                }
            }
            // snapshotMetadata (inside readIndexMetadataFromBestPage) already
            // reset builtForGenCount, so ensureGenLookup will rebuild the index.
        }
    }

    /**
     * Sets the memory budget for gen lookup tier selection.
     * Primarily for testing: a small budget forces Tier 2 (SBBF) or Tier 3 (none).
     */
    public void setGenLookupMemoryBudget(long budget) {
        genLookup.setMemoryBudget(budget);
    }

    public void updateKeyCount() {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        long prevSeqStartA = Long.MIN_VALUE;
        long prevSeqStartB = Long.MIN_VALUE;
        while (true) {
            Unsafe.getUnsafe().loadFence();
            long seqStartA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
            long seqStartB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);

            long bestPage = (seqStartB > seqStartA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
            long otherPage = (bestPage == PostingIndexUtils.PAGE_A_OFFSET) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;

            for (int attempt = 0; attempt < 2; attempt++) {
                long tryPage = (attempt == 0) ? bestPage : otherPage;

                long seqStart = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                if (seqStart <= keyFileSequence) {
                    if (attempt == 0) {
                        continue;
                    }
                    return; // no update available on either page
                }
                Unsafe.getUnsafe().loadFence();

                int keyCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT);
                int genCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);
                long valueMemSize = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE);

                Unsafe.getUnsafe().loadFence();
                long seqEnd = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

                if (seqStart == seqEnd && keyCount >= this.keyCount
                        && genCount >= 0 && genCount <= PostingIndexUtils.MAX_GEN_COUNT) {
                    // If VALUE_FILE_TXN changed (seal created a new .pv file),
                    // fall through to reloadConditionally() for full file reopen.
                    long metaValueTxn = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_VALUE_FILE_TXN);
                    long effectiveTxn = metaValueTxn > 0 ? metaValueTxn : this.valueFileTxn;
                    if (effectiveTxn != this.valueFileTxn) {
                        reloadConditionally();
                        return;
                    }

                    genLookup.snapshotMetadata(keyMem, genCount, tryPage);

                    Unsafe.getUnsafe().loadFence();
                    if (keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) != seqStart) {
                        break; // page overwritten during snapshot, re-pick best page
                    }

                    if (valueMemSize > 0) {
                        long oldAddr = valueMem.addressOf(0);
                        ((MemoryCMR) valueMem).changeSize(valueMemSize);
                        if (valueMem.addressOf(0) != oldAddr) {
                            reloadGeneration++;
                        }
                    }
                    this.activePageOffset = tryPage;
                    this.keyFileSequence = seqStart;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                    this.genCount = genCount;
                    return;
                }
            }

            // If sequence starts haven't changed since the previous iteration,
            // no writer is active and the corruption won't self-heal — stop spinning.
            if (seqStartA == prevSeqStartA && seqStartB == prevSeqStartB) {
                LOG.critical().$(INDEX_CORRUPT).$(" [both pages invalid, no active writer, updateKeyCount]").$();
                return;
            }
            prevSeqStartA = seqStartA;
            prevSeqStartB = seqStartB;

            if (clock.getTicks() > deadline) {
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms, updateKeyCount]").$();
                return;
            }
            Os.pause();
        }
    }

    private void closeSidecarMems() {
        if (sidecarMems != null) {
            for (int i = 0; i < sidecarMems.length; i++) {
                sidecarMems[i] = Misc.free(sidecarMems[i]);
            }
            sidecarMems = null;
        }
        coverCount = 0;
        sidecarColumnIndices = null;
        sidecarColumnTypes = null;
    }

    /**
     * Walks all strides in a dense generation sequentially and marks present keys.
     * Each stride is visited once, in order — optimal for sequential page access.
     */
    private int collectDenseGenKeys(long genFileOffset, long genDataSize, int genKeyCount, BitSet foundKeys) {
        valueMem.extend(genFileOffset + genDataSize);
        long genAddr = valueMem.addressOf(genFileOffset);
        int sc = PostingIndexUtils.strideCount(genKeyCount);
        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
        int newlyFound = 0;

        for (int s = 0; s < sc; s++) {
            int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) s * Integer.BYTES);
            long strideAddr = genAddr + siSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
            int keyBase = s * PostingIndexUtils.DENSE_STRIDE;
            byte mode = Unsafe.getUnsafe().getByte(strideAddr);

            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                for (int j = 0; j < ks; j++) {
                    int startCount = Unsafe.getUnsafe().getInt(prefixAddr + (long) j * Integer.BYTES);
                    int endCount = Unsafe.getUnsafe().getInt(prefixAddr + (long) (j + 1) * Integer.BYTES);
                    if (endCount > startCount && !foundKeys.getAndSet(keyBase + j)) {
                        newlyFound++;
                    }
                }
            } else {
                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                for (int j = 0; j < ks; j++) {
                    if (Unsafe.getUnsafe().getInt(countsAddr + (long) j * Integer.BYTES) > 0
                            && !foundKeys.getAndSet(keyBase + j)) {
                        newlyFound++;
                    }
                }
            }
        }
        return newlyFound;
    }

    /**
     * Scans sparse generation key IDs and marks present keys.
     */
    private int collectSparseGenKeys(long genFileOffset, long genDataSize, int activeKeyCount, BitSet foundKeys) {
        long needed = genFileOffset + genDataSize;
        if (needed > valueMem.size()) {
            valueMem.extend(needed);
        }
        long genAddr = valueMem.addressOf(genFileOffset);
        int newlyFound = 0;
        for (int i = 0; i < activeKeyCount; i++) {
            int key = Unsafe.getUnsafe().getInt(genAddr + (long) i * Integer.BYTES);
            if (!foundKeys.getAndSet(key)) {
                newlyFound++;
            }
        }
        return newlyFound;
    }

    private void openSidecarFilesIfPresent(
            FilesFacade ff,
            Path path,
            CharSequence columnName,
            long columnNameTxn
    ) {
        int plen = path.size();
        LPSZ pciFile = PostingIndexUtils.coverInfoFileName(path, columnName, columnNameTxn);
        if (!ff.exists(pciFile)) {
            path.trimTo(plen);
            return;
        }
        MemoryCMR infoMem = Vm.getCMRInstance();
        try {
            infoMem.of(ff, pciFile, ff.getMapPageSize(), 8, MemoryTag.MMAP_INDEX_READER, CairoConfiguration.O_NONE, -1);
            int magic = infoMem.getInt(0);
            if (magic != PostingIndexUtils.COVER_INFO_MAGIC) {
                return;
            }
            int count = infoMem.getInt(4);
            if (count <= 0) {
                return;
            }
            long neededSize = 8 + (long) count * 8;
            infoMem.extend(neededSize);
            sidecarColumnIndices = new int[count];
            sidecarColumnTypes = new int[count];
            for (int i = 0; i < count; i++) {
                sidecarColumnIndices[i] = infoMem.getInt(8 + (long) i * 8);
                sidecarColumnTypes[i] = infoMem.getInt(8 + (long) i * 8 + 4);
            }
            coverCount = count;

            sidecarMems = new MemoryMR[count];
            boolean allSidecarsPresent = true;
            for (int c = 0; c < count; c++) {
                LPSZ pcFile = PostingIndexUtils.coverDataFileName(path.trimTo(plen), columnName, columnNameTxn, c);
                if (ff.exists(pcFile)) {
                    sidecarMems[c] = Vm.getCMRInstance();
                    // Use -1 to map the full file via fd-based length check,
                    // avoiding stale length from ff.length(path).
                    sidecarMems[c].of(ff, pcFile, ff.getMapPageSize(), -1, MemoryTag.MMAP_INDEX_READER, CairoConfiguration.O_NONE, -1);
                } else {
                    allSidecarsPresent = false;
                }
                path.trimTo(plen);
            }
            if (!allSidecarsPresent) {
                // Incomplete sidecar data (e.g., O3 rebuild without covering).
                // Disable covering so the FallbackRecord reads column files.
                closeSidecarMems();
                coverCount = 0;
            }
        } catch (Throwable e) {
            LOG.error().$("failed to open sidecar files").$(e).$();
            closeSidecarMems();
        } finally {
            Misc.free(infoMem);
            path.trimTo(plen);
        }
    }

    private void openSidecarFilesIfPresent(Path path, CharSequence columnName, long columnNameTxn) {
        openSidecarFilesIfPresent(ff, path, columnName, columnNameTxn);
    }

    private void openSidecarFilesIfPresent(
            CairoConfiguration configuration,
            Path path,
            CharSequence columnName,
            long columnNameTxn
    ) {
        openSidecarFilesIfPresent(configuration.getFilesFacade(), path, columnName, columnNameTxn);
    }

    private void readIndexMetadataFromBestPage() {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        long prevSeqStartA = Long.MIN_VALUE;
        long prevSeqStartB = Long.MIN_VALUE;
        while (true) {
            Unsafe.getUnsafe().loadFence();
            // Use actual file length (not mapped size) to avoid accessing pages
            // beyond the physical file, which causes SIGBUS/SIGSEGV on truncated files.
            long memSize = keyMem.size();
            if (ff != null) {
                long fd = keyMem.getFd();
                if (fd > 0) {
                    long fileLen = ff.length(fd);
                    if (fileLen >= 0 && fileLen < memSize) {
                        memSize = fileLen;
                    }
                }
            }
            long seqStartA = memSize >= PostingIndexUtils.PAGE_SIZE ? keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) : 0;
            long seqStartB = memSize >= PostingIndexUtils.KEY_FILE_RESERVED ? keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) : 0;

            long bestPage = (seqStartB > seqStartA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
            long otherPage = (bestPage == PostingIndexUtils.PAGE_A_OFFSET) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;

            for (int attempt = 0; attempt < 2; attempt++) {
                long tryPage = (attempt == 0) ? bestPage : otherPage;
                if (tryPage == PostingIndexUtils.PAGE_B_OFFSET && memSize < PostingIndexUtils.KEY_FILE_RESERVED) {
                    continue;
                }

                long seqStart = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
                Unsafe.getUnsafe().loadFence();

                long valueMemSize = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE);
                int keyCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT);
                int genCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);

                Unsafe.getUnsafe().loadFence();
                long seqEnd = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

                if (seqStart == seqEnd && seqStart > 0
                        && genCount >= 0 && genCount <= PostingIndexUtils.MAX_GEN_COUNT) {
                    genLookup.snapshotMetadata(keyMem, genCount, tryPage);

                    Unsafe.getUnsafe().loadFence();
                    if (keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) != seqStart) {
                        break; // page overwritten during snapshot, re-pick best page
                    }

                    this.activePageOffset = tryPage;
                    this.keyFileSequence = seqStart;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.genCount = genCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                    return;
                }
            }

            // If sequence starts haven't changed since the previous iteration,
            // no writer is active and the corruption won't self-heal — stop spinning.
            if (seqStartA == prevSeqStartA && seqStartB == prevSeqStartB) {
                LOG.critical().$(INDEX_CORRUPT).$(" [both pages invalid, no active writer]").$();
                break;
            }
            prevSeqStartA = seqStartA;
            prevSeqStartB = seqStartB;

            if (clock.getTicks() > deadline) {
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms]").$();
                break;
            }
            Os.pause();
        }
        // Don't zero state on failure — preserve the last successfully read
        // snapshot so that concurrent readers keep serving stale-but-valid data
        // instead of returning empty results.
    }

    protected void ensureGenLookup() {
        if (genCount == 0 || keyCount == 0) {
            return;
        }
        // Gen dir was already snapshotted into genLookup during readIndexMetadataFromBestPage.
        // Only rebuild the lookup index (tier1/tier2) if genCount changed.
        genLookup.buildLookupIfNeeded(valueMem, keyCount, genCount);
    }

    /**
     * Shared covering cursor logic for forward and backward posting index readers.
     * Contains all sidecar reading, FSST decompression, ALP decoding, and covering
     * column access methods. Subclasses provide iteration direction (hasNext/next).
     */
    protected abstract class AbstractCoveringCursor implements CoveringRowCursor {
        protected final BorrowedArray arrayView = new BorrowedArray();
        protected final DirectBinarySequence binView = new DirectBinarySequence();
        protected final DirectString stringViewA = new DirectString();
        protected final DirectString stringViewB = new DirectString();
        protected final DirectUtf8String varcharViewA = new DirectUtf8String();
        protected final DirectUtf8String varcharViewB = new DirectUtf8String();
        protected int cachedKeyBlockStride = -1;
        protected int cachedSidecarIdx;
        protected int[] currentGenSidecarOffsets;
        protected long decodeWorkspaceAddr;
        protected int decodeWorkspaceCapacity;
        protected int denseVarKeyStartCount;
        protected long[] fsstCachedBlockBases;
        protected FSST.SymbolTable[] fsstCachedTables;
        protected long fsstDecompBufAddr;
        protected int fsstDecompBufCapacity;
        protected boolean isCurrentGenDense;
        // Per-column key block addresses for O(1) point access into compressed sidecar data.
        // Set by cacheSidecarKeyAddrs(), used by getCoveredXxx() methods.
        protected long[] keyBlockAddrs;
        protected int requestedKey;
        protected int sealedGenKeyCount;
        protected int sidecarOrdinal;
        protected int sidecarStrideKeyStart;
        // Per-column decoded data cache with lazy promotion. First access to
        // a block uses point read (readXxxAt); second access to the same block
        // triggers bulk decode. Scan queries auto-promote after one extra point
        // read; point queries (LATEST BY) never bulk-decode.
        private long[] colCacheAddrs;
        private long[] colCacheBlockAddrs;
        private int[] colCacheCapacities;
        private long[] colPointBlockAddrs;
        // Reusable workspace for computeSparseOffsets, avoids per-call heap allocation
        private int[] sparseOffsetsWorkspace;

        @Override
        public int decodeCoveredColumnsToAddr(long[] outputAddrs) {
            if (sidecarMems == null || coverCount == 0) {
                return -1;
            }
            ensureDecodeWorkspaceCapacity(65536); // pre-allocate workspace

            int totalWritten = 0;
            for (int g = 0; g < genCount; g++) {
                int genKeyCount = genLookup.getGenKeyCount(g);
                if (genKeyCount >= 0) {
                    totalWritten += decodeDenseGenToAddr(genKeyCount, outputAddrs, totalWritten);
                } else {
                    totalWritten += decodeSparseGenToAddr(g, outputAddrs, totalWritten);
                }
            }
            return totalWritten;
        }

        @Override
        public ArrayView getCoveredArray(int includeIdx, int columnType) {
            return getVarSidecarArray(includeIdx, columnType);
        }

        @Override
        public BinarySequence getCoveredBin(int includeIdx) {
            return getVarSidecarBin(includeIdx);
        }

        @Override
        public long getCoveredBinLen(int includeIdx) {
            return getVarSidecarBinLen(includeIdx);
        }

        @Override
        public byte getCoveredByte(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarByte(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return 0;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getUnsafe().getByte(colCacheAddrs[includeIdx] + idx);
            }
            return CoveringCompressor.readByteAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public int getCoveredColumnCount() {
            return coverCount;
        }

        @Override
        public int getCoveredColumnType(int includeIdx) {
            return sidecarColumnTypes != null && includeIdx < sidecarColumnTypes.length
                    ? sidecarColumnTypes[includeIdx] : -1;
        }

        @Override
        public double getCoveredDouble(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarDouble(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Double.NaN;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getUnsafe().getDouble(colCacheAddrs[includeIdx] + (long) idx * Double.BYTES);
            }
            return CoveringCompressor.readDoubleAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public float getCoveredFloat(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarFloat(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Float.NaN;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getUnsafe().getFloat(colCacheAddrs[includeIdx] + (long) idx * Float.BYTES);
            }
            return CoveringCompressor.readFloatAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public int getCoveredInt(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarInt(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Integer.MIN_VALUE;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getUnsafe().getInt(colCacheAddrs[includeIdx] + (long) idx * Integer.BYTES);
            }
            return CoveringCompressor.readIntAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public long getCoveredLong(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarLong(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Long.MIN_VALUE;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getUnsafe().getLong(colCacheAddrs[includeIdx] + (long) idx * Long.BYTES);
            }
            return CoveringCompressor.readLongAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public long getCoveredLong128Hi(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 16, 1);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Long.MIN_VALUE;
            }
            // UUID/DECIMAL128: raw 16 bytes per value, skip 4-byte count header
            return Unsafe.getUnsafe().getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 16 + 8);
        }

        @Override
        public long getCoveredLong128Lo(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 16, 0);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Long.MIN_VALUE;
            }
            return Unsafe.getUnsafe().getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 16);
        }

        @Override
        public long getCoveredLong256_0(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 32, 0);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Long.MIN_VALUE;
            }
            return Unsafe.getUnsafe().getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 32);
        }

        @Override
        public long getCoveredLong256_1(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 32, 1);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Long.MIN_VALUE;
            }
            return Unsafe.getUnsafe().getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 32 + 8);
        }

        @Override
        public long getCoveredLong256_2(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 32, 2);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Long.MIN_VALUE;
            }
            return Unsafe.getUnsafe().getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 32 + 16);
        }

        @Override
        public long getCoveredLong256_3(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarMultiLong(includeIdx, 32, 3);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return Long.MIN_VALUE;
            }
            return Unsafe.getUnsafe().getLong(keyBlockAddrs[includeIdx] + 4 + (long) idx * 32 + 24);
        }

        @Override
        public short getCoveredShort(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarShort(includeIdx);
            }
            int idx = cachedSidecarIdx;
            if (idx < 0 || keyBlockAddrs == null || keyBlockAddrs[includeIdx] == 0) {
                return 0;
            }
            if (ensureColumnDecoded(includeIdx)) {
                return Unsafe.getUnsafe().getShort(colCacheAddrs[includeIdx] + (long) idx * Short.BYTES);
            }
            return CoveringCompressor.readShortAt(keyBlockAddrs[includeIdx], idx);
        }

        @Override
        public CharSequence getCoveredStrA(int includeIdx) {
            return getVarSidecarStr(includeIdx, stringViewA);
        }

        @Override
        public CharSequence getCoveredStrB(int includeIdx) {
            return getVarSidecarStr(includeIdx, stringViewB);
        }

        @Override
        public int getCoveredValueCount() {
            if (sidecarMems == null || coverCount == 0) {
                return -1;
            }
            // Sum counts across all gens for this key.
            // Dense gens: read count from sidecar key block header.
            // Sparse gens: read count from raw sidecar block header.
            int total = 0;
            for (int g = 0; g < genCount; g++) {
                int gkc = genLookup.getGenKeyCount(g);
                if (gkc >= 0) {
                    int denseCount = getDenseGenCount(gkc);
                    if (denseCount < 0) return -1; // var-only INCLUDE, count unknown
                    total += denseCount;
                } else {
                    // Sparse gen — read count from raw sidecar block
                    total += getSparseGenCount(g);
                }
            }
            return total;
        }

        @Override
        public Utf8Sequence getCoveredVarcharA(int includeIdx) {
            return getVarSidecarUtf8(includeIdx, varcharViewA);
        }

        @Override
        public Utf8Sequence getCoveredVarcharB(int includeIdx) {
            return getVarSidecarUtf8(includeIdx, varcharViewB);
        }

        @Override
        public boolean hasCovering() {
            return coverCount > 0 && sidecarMems != null;
        }

        @Override
        public long seekToLast() {
            long lastRowId = -1;
            while (hasNext()) {
                lastRowId = next();
            }
            return lastRowId;
        }

        private long computeSealedSidecarSize(MemoryMR mem, int keyCount) {
            if (keyCount <= 0 || mem == null || mem.size() < 4) {
                return 0;
            }
            int sc = PostingIndexUtils.strideCount(keyCount);
            int siSize = PostingIndexUtils.strideIndexSize(keyCount);
            long sentinelPos = (long) sc * Integer.BYTES;
            if (sentinelPos + Integer.BYTES > mem.size()) {
                return 0;
            }
            int totalStrideDataSize = Unsafe.getUnsafe().getInt(mem.addressOf(sentinelPos));
            return siSize + totalStrideDataSize;
        }

        private void computeSparseOffsets(int gen, int[] offsets) {
            int firstSparseGen = 0;
            int denseKeyCount = 0;
            while (firstSparseGen < gen && genLookup.getGenKeyCount(firstSparseGen) >= 0) {
                denseKeyCount = genLookup.getGenKeyCount(firstSparseGen);
                firstSparseGen++;
            }
            for (int c = 0; c < coverCount; c++) {
                if (sidecarMems[c] == null) {
                    offsets[c] = 0;
                    continue;
                }
                long offset;
                if (firstSparseGen == 0) {
                    offset = 0;
                } else if (c == 0) {
                    offset = genLookup.getGenSidecarOffset(firstSparseGen);
                } else {
                    offset = computeSealedSidecarSize(sidecarMems[c], denseKeyCount);
                }
                int colType = sidecarColumnTypes[c];
                boolean isVar = ColumnType.isVarSize(colType);
                int elemSize = isVar ? 0 : ColumnType.sizeOf(colType);
                for (int g = firstSparseGen; g < gen; g++) {
                    if (genLookup.getGenKeyCount(g) >= 0) continue;
                    if (offset + 4 > sidecarMems[c].size()) break;
                    int count = Unsafe.getUnsafe().getInt(sidecarMems[c].addressOf(offset));
                    if (isVar) {
                        if (count == 0) {
                            offset += 4;
                        } else {
                            long sentinelPos = offset + 4 + (long) count * Integer.BYTES;
                            int dataSize = Unsafe.getUnsafe().getInt(sidecarMems[c].addressOf(sentinelPos));
                            offset += 4 + (long) (count + 1) * Integer.BYTES + dataSize;
                        }
                    } else {
                        offset += 4 + (long) count * elemSize;
                    }
                }
                offsets[c] = (int) offset;
            }
        }

        private int decodeDenseGenToAddr(int genKeyCount, long[] outputAddrs, int writeOffset) {
            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            int sc = PostingIndexUtils.strideCount(genKeyCount);
            if (stride >= sc) return 0;
            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
            int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
            if (localKey >= ks) return 0;

            int count = 0;
            for (int c = 0; c < coverCount; c++) {
                if (outputAddrs[c] == 0) continue;
                MemoryMR mem = sidecarMems[c];
                if (mem == null || mem.size() == 0) continue;
                int colType = sidecarColumnTypes[c];
                if (ColumnType.isVarSize(colType)) continue;

                long strideIdxOffset = (long) stride * Integer.BYTES;
                if (strideIdxOffset + Integer.BYTES > mem.size()) continue;
                int strideOff = mem.getInt(strideIdxOffset);
                long strideDataStart = siSize + strideOff;
                if (strideDataStart >= mem.size()) continue;

                long keyOffsetsEnd = strideDataStart + (long) ks * Integer.BYTES;
                if (keyOffsetsEnd > mem.size()) continue;
                long keyOffsetsAddr = mem.addressOf(strideDataStart);
                int keyBlockOff = Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) localKey * Integer.BYTES);
                long keyBlockFileOffset = keyOffsetsEnd + keyBlockOff;
                if (keyBlockFileOffset < 0 || keyBlockFileOffset + Integer.BYTES > mem.size()) continue;
                long keyBlockAddr = mem.addressOf(keyBlockFileOffset);
                int kc = Unsafe.getUnsafe().getInt(keyBlockAddr);
                if (kc <= 0) continue;
                // Validate that the compressed block fits within the sidecar mapping.
                // Conservative upper bound: 4-byte count header + kc values at full element size.
                int elemSize = ColumnType.sizeOf(colType);
                long maxBlockSize = Integer.BYTES + (long) kc * Math.max(elemSize, Long.BYTES);
                if (keyBlockFileOffset + maxBlockSize > mem.size()) continue;
                count = kc;

                long outAddr = outputAddrs[c] + (long) writeOffset * elemSize;
                ensureDecodeWorkspaceCapacity(kc);

                switch (ColumnType.tagOf(colType)) {
                    case ColumnType.DOUBLE ->
                            CoveringCompressor.decompressDoublesToAddr(keyBlockAddr, outAddr, decodeWorkspaceAddr);
                    case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG ->
                            CoveringCompressor.decompressLongsToAddr(keyBlockAddr, outAddr, decodeWorkspaceAddr);
                    case ColumnType.FLOAT ->
                            CoveringCompressor.decompressFloatsToAddr(keyBlockAddr, outAddr, decodeWorkspaceAddr);
                    case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT, ColumnType.SYMBOL ->
                            CoveringCompressor.decompressIntsToAddr(keyBlockAddr, outAddr, decodeWorkspaceAddr);
                    case ColumnType.CHAR, ColumnType.SHORT, ColumnType.GEOSHORT ->
                            CoveringCompressor.decompressShortsToAddr(keyBlockAddr, outAddr, decodeWorkspaceAddr);
                    case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE ->
                            CoveringCompressor.decompressBytesToAddr(keyBlockAddr, outAddr, decodeWorkspaceAddr);
                }
            }
            return count;
        }

        private int decodeSparseGenToAddr(int gen, long[] outputAddrs, int writeOffset) {
            int[] offsets = ensureSparseOffsetsWorkspace();
            computeSparseOffsets(gen, offsets);

            int count = 0;
            for (int c = 0; c < coverCount; c++) {
                if (outputAddrs[c] == 0) continue;
                MemoryMR mem = sidecarMems[c];
                if (mem == null) continue;
                int colType = sidecarColumnTypes[c];
                if (ColumnType.isVarSize(colType)) continue;

                int elemSize = ColumnType.sizeOf(colType);
                long blockAddr = mem.addressOf(offsets[c]);
                int gc = Unsafe.getUnsafe().getInt(blockAddr);
                if (gc == 0) continue;
                count = gc;

                // Raw sidecar: [count:4B][value0][value1]... — memcpy directly
                long srcAddr = blockAddr + 4;
                long dstAddr = outputAddrs[c] + (long) writeOffset * elemSize;
                Unsafe.getUnsafe().copyMemory(srcAddr, dstAddr, (long) gc * elemSize);
            }
            return count;
        }

        private CharSequence decompressFsstStr(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx, DirectString view) {
            int decompLen = decompressFsstValue(mem, blockBase, count, ordinal, includeIdx);
            if (decompLen < 0) return null;
            int len = Unsafe.getUnsafe().getInt(fsstDecompBufAddr);
            if (len < 0) return null;
            return view.of(fsstDecompBufAddr + Integer.BYTES, len);
        }

        private Utf8Sequence decompressFsstUtf8(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx, DirectUtf8String view) {
            int decompLen = decompressFsstValue(mem, blockBase, count, ordinal, includeIdx);
            if (decompLen < 0) return null;
            return view.of(fsstDecompBufAddr, fsstDecompBufAddr + decompLen);
        }

        private int decompressFsstValue(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx) {
            FSST.SymbolTable table = resolveFsstTable(mem, blockBase, includeIdx);
            long pos = blockBase + 4;
            int tableLen = Unsafe.getUnsafe().getShort(mem.addressOf(pos)) & 0xFFFF;

            long offsetsAddr = mem.addressOf(pos + 2 + tableLen);
            int lo = Unsafe.getUnsafe().getInt(offsetsAddr + (long) ordinal * Integer.BYTES);
            int hi = Unsafe.getUnsafe().getInt(offsetsAddr + (long) (ordinal + 1) * Integer.BYTES);
            if (lo == hi) return -1; // NULL

            long dataBase = pos + 2 + tableLen + (long) (count + 1) * Integer.BYTES;
            long compAddr = mem.addressOf(dataBase + lo);
            int compLen = hi - lo;

            int needed = compLen * 8;
            if (fsstDecompBufAddr == 0 || fsstDecompBufCapacity < needed) {
                if (fsstDecompBufAddr != 0) {
                    Unsafe.free(fsstDecompBufAddr, fsstDecompBufCapacity, MemoryTag.NATIVE_INDEX_READER);
                }
                fsstDecompBufCapacity = needed;
                fsstDecompBufAddr = Unsafe.malloc(needed, MemoryTag.NATIVE_INDEX_READER);
            }
            return FSST.decompressBytes(table, compAddr, compLen, fsstDecompBufAddr);
        }

        private int[] ensureSparseOffsetsWorkspace() {
            if (sparseOffsetsWorkspace == null || sparseOffsetsWorkspace.length < coverCount) {
                sparseOffsetsWorkspace = new int[coverCount];
            }
            return sparseOffsetsWorkspace;
        }

        private long findDenseVarBlockBase(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null || sealedGenKeyCount <= 0) {
                return -1;
            }
            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int siSize = PostingIndexUtils.strideIndexSize(sealedGenKeyCount);
            MemoryMR mem = sidecarMems[includeIdx];
            if ((long) stride * Integer.BYTES + Integer.BYTES > mem.size()) {
                return -1;
            }
            int strideOff = Unsafe.getUnsafe().getInt(mem.addressOf((long) stride * Integer.BYTES));
            return siSize + strideOff;
        }

        private int getDenseGenCount(int genKeyCount) {
            int memIdx = -1;
            for (int c = 0; c < coverCount; c++) {
                if (sidecarMems[c] != null && sidecarMems[c].size() > 0
                        && !ColumnType.isVarSize(sidecarColumnTypes[c])) {
                    memIdx = c;
                    break;
                }
            }
            if (memIdx < 0) return -1;

            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            int sc = PostingIndexUtils.strideCount(genKeyCount);
            if (stride >= sc) return 0;
            int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
            if (localKey >= ks) return 0;

            MemoryMR mem = sidecarMems[memIdx];
            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
            long strideIdxOffset = (long) stride * Integer.BYTES;
            if (strideIdxOffset + Integer.BYTES > mem.size()) return 0;
            int strideOff = mem.getInt(strideIdxOffset);
            long strideDataStart = siSize + strideOff;
            if (strideDataStart >= mem.size()) return 0;
            long keyOffsetsAddr = mem.addressOf(strideDataStart);
            int keyBlockOff = Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) localKey * Integer.BYTES);
            long keyBlockAddr = mem.addressOf(strideDataStart + (long) ks * Integer.BYTES + keyBlockOff);
            return Unsafe.getUnsafe().getInt(keyBlockAddr);
        }

        private byte getRawSidecarByte(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return 0;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) cachedSidecarIdx * Byte.BYTES);
            return Unsafe.getUnsafe().getByte(addr);
        }

        private double getRawSidecarDouble(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return Double.NaN;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) cachedSidecarIdx * Double.BYTES);
            return Unsafe.getUnsafe().getDouble(addr);
        }

        private float getRawSidecarFloat(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return Float.NaN;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) cachedSidecarIdx * Float.BYTES);
            return Unsafe.getUnsafe().getFloat(addr);
        }

        private int getRawSidecarInt(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return Integer.MIN_VALUE;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) cachedSidecarIdx * Integer.BYTES);
            return Unsafe.getUnsafe().getInt(addr);
        }

        private long getRawSidecarLong(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return Long.MIN_VALUE;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) cachedSidecarIdx * Long.BYTES);
            return Unsafe.getUnsafe().getLong(addr);
        }

        private long getRawSidecarMultiLong(int includeIdx, int valueSize, int longIndex) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return Long.MIN_VALUE;
            long addr = sidecarMems[includeIdx].addressOf(
                    currentGenSidecarOffsets[includeIdx] + 4
                            + (long) cachedSidecarIdx * valueSize
                            + (long) longIndex * Long.BYTES
            );
            return Unsafe.getUnsafe().getLong(addr);
        }

        private short getRawSidecarShort(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return 0;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) cachedSidecarIdx * Short.BYTES);
            return Unsafe.getUnsafe().getShort(addr);
        }

        private int getSparseGenCount(int gen) {
            if (sidecarMems[0] == null) {
                return 0;
            }
            int[] offsets = ensureSparseOffsetsWorkspace();
            computeSparseOffsets(gen, offsets);
            long addr = sidecarMems[0].addressOf(offsets[0]);
            return Unsafe.getUnsafe().getInt(addr);
        }

        private ArrayView getVarSidecarArray(int includeIdx, int columnType) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return null;
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets[includeIdx];
            if (blockBase < 0) return null;
            MemoryMR mem = sidecarMems[includeIdx];
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSST.FSST_BLOCK_FLAG) != 0;
            int count = rawCount & ~FSST.FSST_BLOCK_FLAG;
            if (ordinal >= count) return null;

            long dataAddr;
            int dataLen;
            if (fsst) {
                int decompLen = decompressFsstValue(mem, blockBase, count, ordinal, includeIdx);
                if (decompLen < 0) return null;
                dataAddr = fsstDecompBufAddr;
                dataLen = decompLen;
            } else {
                long offsetsAddr = mem.addressOf(blockBase + 4);
                int lo = Unsafe.getUnsafe().getInt(offsetsAddr + (long) ordinal * Integer.BYTES);
                int hi = Unsafe.getUnsafe().getInt(offsetsAddr + (long) (ordinal + 1) * Integer.BYTES);
                if (lo == hi) {
                    arrayView.ofNull();
                    return arrayView;
                }
                long dataBase = blockBase + 4 + (long) (count + 1) * Integer.BYTES;
                dataAddr = mem.addressOf(dataBase + lo);
                dataLen = hi - lo;
            }
            int dims = ColumnType.decodeArrayDimensionality(columnType);
            short elemType = ColumnType.decodeArrayElementType(columnType);
            int elemSize = ColumnType.sizeOf(elemType);
            int cardinality = 1;
            for (int d = 0; d < dims; d++) {
                cardinality *= Unsafe.getUnsafe().getInt(dataAddr + (long) d * Integer.BYTES);
            }
            int valueSize = cardinality * elemSize;
            long valuePtr = dataAddr + dataLen - valueSize;
            return arrayView.of(columnType, dataAddr, valuePtr, valueSize);
        }

        private BinarySequence getVarSidecarBin(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return null;
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets[includeIdx];
            if (blockBase < 0) return null;
            MemoryMR mem = sidecarMems[includeIdx];
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSST.FSST_BLOCK_FLAG) != 0;
            int count = rawCount & ~FSST.FSST_BLOCK_FLAG;
            if (ordinal >= count) return null;

            if (fsst) {
                int decompLen = decompressFsstValue(mem, blockBase, count, ordinal, includeIdx);
                if (decompLen < 0) return null;
                long len = Unsafe.getUnsafe().getLong(fsstDecompBufAddr);
                if (len < 0) return null;
                return binView.of(fsstDecompBufAddr + Long.BYTES, len);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            int lo = Unsafe.getUnsafe().getInt(offsetsAddr + (long) ordinal * Integer.BYTES);
            int hi = Unsafe.getUnsafe().getInt(offsetsAddr + (long) (ordinal + 1) * Integer.BYTES);
            if (lo == hi) return null;
            long dataBase = blockBase + 4 + (long) (count + 1) * Integer.BYTES;
            long dataAddr = mem.addressOf(dataBase + lo);
            long len = Unsafe.getUnsafe().getLong(dataAddr);
            if (len < 0) return null;
            return binView.of(dataAddr + Long.BYTES, len);
        }

        private long getVarSidecarBinLen(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return -1;
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets[includeIdx];
            if (blockBase < 0) return -1;
            MemoryMR mem = sidecarMems[includeIdx];
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSST.FSST_BLOCK_FLAG) != 0;
            int count = rawCount & ~FSST.FSST_BLOCK_FLAG;
            if (ordinal >= count) return -1;

            if (fsst) {
                int decompLen = decompressFsstValue(mem, blockBase, count, ordinal, includeIdx);
                if (decompLen < 0) return -1;
                return Unsafe.getUnsafe().getLong(fsstDecompBufAddr);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            int lo = Unsafe.getUnsafe().getInt(offsetsAddr + (long) ordinal * Integer.BYTES);
            int hi = Unsafe.getUnsafe().getInt(offsetsAddr + (long) (ordinal + 1) * Integer.BYTES);
            if (lo == hi) return -1;
            long dataBase = blockBase + 4 + (long) (count + 1) * Integer.BYTES;
            long dataAddr = mem.addressOf(dataBase + lo);
            return Unsafe.getUnsafe().getLong(dataAddr);
        }

        private CharSequence getVarSidecarStr(int includeIdx, DirectString view) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return null;
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets[includeIdx];
            if (blockBase < 0) return null;
            MemoryMR mem = sidecarMems[includeIdx];
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSST.FSST_BLOCK_FLAG) != 0;
            int count = rawCount & ~FSST.FSST_BLOCK_FLAG;
            if (ordinal >= count) return null;

            if (fsst) {
                return decompressFsstStr(mem, blockBase, count, ordinal, includeIdx, view);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            int lo = Unsafe.getUnsafe().getInt(offsetsAddr + (long) ordinal * Integer.BYTES);
            int hi = Unsafe.getUnsafe().getInt(offsetsAddr + (long) (ordinal + 1) * Integer.BYTES);
            if (lo == hi) return null;
            long dataBase = blockBase + 4 + (long) (count + 1) * Integer.BYTES;
            long dataAddr = mem.addressOf(dataBase + lo);
            int len = Unsafe.getUnsafe().getInt(dataAddr);
            if (len < 0) return null;
            return view.of(dataAddr + Integer.BYTES, len);
        }

        private Utf8Sequence getVarSidecarUtf8(int includeIdx, DirectUtf8String view) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return null;
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets[includeIdx];
            if (blockBase < 0) return null;
            MemoryMR mem = sidecarMems[includeIdx];
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSST.FSST_BLOCK_FLAG) != 0;
            int count = rawCount & ~FSST.FSST_BLOCK_FLAG;
            if (ordinal >= count) return null;

            if (fsst) {
                return decompressFsstUtf8(mem, blockBase, count, ordinal, includeIdx, view);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            int lo = Unsafe.getUnsafe().getInt(offsetsAddr + (long) ordinal * Integer.BYTES);
            int hi = Unsafe.getUnsafe().getInt(offsetsAddr + (long) (ordinal + 1) * Integer.BYTES);
            if (lo == hi) return null;
            long dataBase = blockBase + 4 + (long) (count + 1) * Integer.BYTES;
            long dataAddr = mem.addressOf(dataBase + lo);
            return view.of(dataAddr, dataAddr + (hi - lo));
        }

        private FSST.SymbolTable resolveFsstTable(MemoryMR mem, long blockBase, int includeIdx) {
            if (fsstCachedTables == null) {
                fsstCachedTables = new FSST.SymbolTable[coverCount];
                fsstCachedBlockBases = new long[coverCount];
                java.util.Arrays.fill(fsstCachedBlockBases, -1);
                for (int i = 0; i < coverCount; i++) {
                    fsstCachedTables[i] = new FSST.SymbolTable();
                }
            }
            if (includeIdx < fsstCachedTables.length
                    && fsstCachedBlockBases[includeIdx] == blockBase) {
                return fsstCachedTables[includeIdx];
            }
            long pos = blockBase + 4;
            long tableAddr = mem.addressOf(pos + 2);
            FSST.SymbolTable table = fsstCachedTables[includeIdx];
            FSST.deserializeInto(tableAddr, table);
            fsstCachedBlockBases[includeIdx] = blockBase;
            return table;
        }

        /**
         * Cache per-column key block addresses for O(1) point access.
         * Replaces decodeSidecarKey() — no bulk decompression, no heap allocation.
         * The getCoveredXxx() methods decode individual values on demand.
         */
        protected void cacheSidecarKeyAddrs(int stride, int localKey) {
            if (sidecarMems == null || coverCount == 0 || sealedGenKeyCount <= 0) {
                return;
            }
            if (stride == cachedKeyBlockStride) {
                return; // already cached for this stride
            }
            int sc = PostingIndexUtils.strideCount(sealedGenKeyCount);
            if (stride >= sc) {
                return;
            }
            int siSize = PostingIndexUtils.strideIndexSize(sealedGenKeyCount);
            int ks = PostingIndexUtils.keysInStride(sealedGenKeyCount, stride);
            if (localKey >= ks) {
                return;
            }

            if (keyBlockAddrs == null || keyBlockAddrs.length < coverCount) {
                keyBlockAddrs = new long[coverCount];
            }

            for (int c = 0; c < coverCount; c++) {
                MemoryMR mem = sidecarMems[c];
                if (mem == null) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                if (mem.size() == 0) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long strideIdxOffset = (long) stride * Integer.BYTES;
                if (strideIdxOffset + Integer.BYTES > mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                int strideOff = mem.getInt(strideIdxOffset);
                long strideDataStart = siSize + strideOff;
                if (strideDataStart >= mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long keyOffsetsEnd = strideDataStart + (long) ks * Integer.BYTES;
                if (keyOffsetsEnd > mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long keyOffsetsAddr = mem.addressOf(strideDataStart);
                int keyBlockOff = Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) localKey * Integer.BYTES);
                long keyBlockStart = keyOffsetsEnd + keyBlockOff;
                if (keyBlockStart + 4 > mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                keyBlockAddrs[c] = mem.addressOf(keyBlockStart);
            }
            cachedKeyBlockStride = stride;
        }

        protected void closeCoveringResources() {
            if (colCacheAddrs != null) {
                for (int i = 0; i < colCacheAddrs.length; i++) {
                    if (colCacheAddrs[i] != 0) {
                        Unsafe.free(colCacheAddrs[i], colCacheCapacities[i], MemoryTag.NATIVE_INDEX_READER);
                        colCacheAddrs[i] = 0;
                        colCacheCapacities[i] = 0;
                    }
                    colCacheBlockAddrs[i] = 0;
                }
            }
            if (decodeWorkspaceAddr != 0) {
                Unsafe.free(decodeWorkspaceAddr, (long) decodeWorkspaceCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                decodeWorkspaceAddr = 0;
                decodeWorkspaceCapacity = 0;
            }
            if (fsstDecompBufAddr != 0) {
                Unsafe.free(fsstDecompBufAddr, fsstDecompBufCapacity, MemoryTag.NATIVE_INDEX_READER);
                fsstDecompBufAddr = 0;
                fsstDecompBufCapacity = 0;
            }
            if (fsstCachedTables != null) {
                for (int i = 0; i < fsstCachedTables.length; i++) {
                    if (fsstCachedTables[i] != null) {
                        fsstCachedTables[i].close();
                        fsstCachedTables[i] = null;
                    }
                }
                fsstCachedTables = null;
            }
        }

        protected void computePerColumnSidecarOffsets(int gen) {
            if (sidecarMems == null || sidecarColumnTypes == null || coverCount == 0) {
                return;
            }
            if (currentGenSidecarOffsets == null || currentGenSidecarOffsets.length < coverCount) {
                currentGenSidecarOffsets = new int[coverCount];
            }

            // Find the first sparse gen (skip dense gens which have stride-indexed sidecars)
            int firstSparseGen = 0;
            while (firstSparseGen < gen && genLookup.getGenKeyCount(firstSparseGen) >= 0) {
                firstSparseGen++;
            }

            for (int c = 0; c < coverCount; c++) {
                if (sidecarMems[c] == null) {
                    currentGenSidecarOffsets[c] = 0;
                    continue;
                }

                long offset;
                if (firstSparseGen == 0) {
                    // No dense gens before this one — scan from file start
                    offset = 0;
                } else if (c == 0) {
                    // Column 0: use gen dir sidecar offset (exact for column 0)
                    offset = genLookup.getGenSidecarOffset(firstSparseGen);
                } else {
                    // Other columns: compute sealed data size from stride index sentinel.
                    // Each column's sealed sidecar has different sizes due to compression.
                    offset = computeSealedSidecarSize(sidecarMems[c], sealedGenKeyCount);
                }

                int colType = sidecarColumnTypes[c];
                boolean isVar = ColumnType.isVarSize(colType);
                int elemSize = isVar ? 0 : ColumnType.sizeOf(colType);

                // Scan through sparse gen blocks from firstSparseGen to gen-1
                for (int g = firstSparseGen; g < gen; g++) {
                    if (genLookup.getGenKeyCount(g) >= 0) {
                        continue; // skip dense gens (shouldn't happen after firstSparseGen)
                    }
                    if (offset + 4 > sidecarMems[c].size()) {
                        break;
                    }
                    int count = Unsafe.getUnsafe().getInt(sidecarMems[c].addressOf(offset));
                    if (isVar) {
                        if (count == 0) {
                            offset += 4;
                        } else {
                            long sentinelPos = offset + 4 + (long) count * Integer.BYTES;
                            int dataSize = Unsafe.getUnsafe().getInt(sidecarMems[c].addressOf(sentinelPos));
                            offset += 4 + (long) (count + 1) * Integer.BYTES + dataSize;
                        }
                    } else {
                        offset += 4 + (long) count * elemSize;
                    }
                }
                currentGenSidecarOffsets[c] = (int) offset;
            }
        }

        /**
         * Lazy per-column decode cache. Returns true when the column's block
         * has been bulk-decoded into colCacheAddrs[includeIdx]. On the first
         * access to a new block the method returns false (caller should use
         * point read). On the second access to the same block it bulk-decodes
         * and returns true. Scan cursors auto-promote after one point read;
         * point queries never pay the bulk-decode cost.
         */
        protected boolean ensureColumnDecoded(int includeIdx) {
            if (colCacheBlockAddrs == null) {
                colCacheAddrs = new long[coverCount];
                colCacheBlockAddrs = new long[coverCount];
                colCacheCapacities = new int[coverCount];
                colPointBlockAddrs = new long[coverCount];
            }
            long blockAddr = keyBlockAddrs[includeIdx];
            if (colCacheBlockAddrs[includeIdx] == blockAddr) {
                return true;
            }
            if (colPointBlockAddrs[includeIdx] != blockAddr) {
                colPointBlockAddrs[includeIdx] = blockAddr;
                return false;
            }
            int count = Unsafe.getUnsafe().getInt(blockAddr);
            if (count <= 0) {
                colCacheBlockAddrs[includeIdx] = blockAddr;
                return true;
            }
            int colType = sidecarColumnTypes[includeIdx];
            int elemSize = ColumnType.sizeOf(colType);
            int needed = count * elemSize;
            if (needed > colCacheCapacities[includeIdx]) {
                if (colCacheAddrs[includeIdx] != 0) {
                    Unsafe.free(colCacheAddrs[includeIdx], colCacheCapacities[includeIdx], MemoryTag.NATIVE_INDEX_READER);
                }
                colCacheCapacities[includeIdx] = needed;
                colCacheAddrs[includeIdx] = Unsafe.malloc(needed, MemoryTag.NATIVE_INDEX_READER);
            }
            ensureDecodeWorkspaceCapacity(count);
            switch (ColumnType.tagOf(colType)) {
                case ColumnType.DOUBLE ->
                        CoveringCompressor.decompressDoublesToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.FLOAT ->
                        CoveringCompressor.decompressFloatsToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG, ColumnType.DECIMAL64 ->
                        CoveringCompressor.decompressLongsToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT, ColumnType.SYMBOL, ColumnType.DECIMAL32 ->
                        CoveringCompressor.decompressIntsToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.SHORT, ColumnType.CHAR, ColumnType.GEOSHORT, ColumnType.DECIMAL16 ->
                        CoveringCompressor.decompressShortsToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
                case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE, ColumnType.DECIMAL8 ->
                        CoveringCompressor.decompressBytesToAddr(blockAddr, colCacheAddrs[includeIdx], decodeWorkspaceAddr);
            }
            colCacheBlockAddrs[includeIdx] = blockAddr;
            return true;
        }

        protected void ensureDecodeWorkspaceCapacity(int count) {
            if (count > decodeWorkspaceCapacity) {
                if (decodeWorkspaceAddr != 0) {
                    Unsafe.free(decodeWorkspaceAddr, (long) decodeWorkspaceCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    decodeWorkspaceAddr = 0;
                }
                decodeWorkspaceCapacity = count;
                decodeWorkspaceAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
        }

        protected void resetCoveringState() {
            this.sidecarOrdinal = 0;
            this.sidecarStrideKeyStart = 0;
            this.cachedKeyBlockStride = -1;
            this.cachedSidecarIdx = 0;
            this.isCurrentGenDense = false;
            if (colCacheBlockAddrs != null) {
                for (int i = 0; i < colCacheBlockAddrs.length; i++) {
                    colCacheBlockAddrs[i] = 0;
                    colPointBlockAddrs[i] = 0;
                }
            }
        }

        protected int sidecarValueIdx() {
            return sidecarStrideKeyStart + sidecarOrdinal - 1;
        }
    }
}

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
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.DirectBitSet;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

public abstract class AbstractPostingIndexReader implements IndexReader {
    private static final String INDEX_CORRUPT = "posting index is corrupt";
    private static final Log LOG = LogFactory.getLog(AbstractPostingIndexReader.class);
    protected final PostingGenLookup genLookup = new PostingGenLookup();
    protected final MemoryCMR infoMem = Vm.getCMRInstance();
    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final Path sidecarBasePath = new Path();
    protected final IntList sidecarColumnIndices = new IntList();
    protected final IntList sidecarColumnTypes = new IntList();
    protected final LongList sidecarCovTs = new LongList();
    protected final ObjList<MemoryMR> sidecarMems = new ObjList<>();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    protected long columnTop;
    protected int coverCount;
    protected int genCount;
    protected int keyCount;
    protected RecordMetadata metadata;
    private long activePageOffset;
    private MillisecondClock clock;
    private long columnTxn;
    private ColumnVersionReader columnVersionReader;
    private FilesFacade ff;
    private CharSequence indexColumnName;
    private int keyCountIncludingNulls;
    private long keyFileSequence = -1;
    private long partitionTimestamp;
    private long partitionTxn;
    private long spinLockTimeoutMs;
    private long valueFileTxn;
    private long valueMemSize = -1;

    @Override
    public void close() {
        Misc.free(genLookup);
        Misc.free(infoMem);
        Misc.free(keyMem);
        Misc.free(valueMem);
        closeSidecarMems();
        Misc.free(sidecarBasePath);
        keyCount = 0;
        keyCountIncludingNulls = 0;
        genCount = 0;
        valueMemSize = -1;
        keyFileSequence = -1;
    }

    @Override
    public int collectDistinctKeys(DirectBitSet foundKeys) {
        if (genCount == 0 || keyCount == 0) {
            return 0;
        }
        int newlyFound = 0;
        for (int g = 0; g < genCount; g++) {
            int genKeyCount = genLookup.getGenKeyCount(g);
            long genFileOffset = genLookup.getGenFileOffset(g);
            if (genKeyCount >= 0) {
                newlyFound += collectDenseGenKeys(genFileOffset, genKeyCount, foundKeys);
            } else {
                newlyFound += collectSparseGenKeys(genFileOffset, -genKeyCount, foundKeys);
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
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        this.columnTop = columnTop;
        this.columnTxn = columnNameTxn;
        this.partitionTxn = partitionTxn;
        this.metadata = metadata;
        this.columnVersionReader = columnVersionReader;
        this.partitionTimestamp = partitionTimestamp;
        this.spinLockTimeoutMs = configuration.getSpinLockTimeout();
        this.clock = configuration.getMillisecondClock();
        this.ff = configuration.getFilesFacade();
        this.indexColumnName = columnName;
        this.sidecarBasePath.of(path);
        genLookup.reopen();
        final int pLen = path.size();

        try {
            LPSZ keyFile = PostingIndexUtils.keyFileName(path, columnName, columnNameTxn);
            long keyFileSize = ff.length(keyFile);
            if (keyFileSize >= 0 && keyFileSize < PostingIndexUtils.KEY_FILE_RESERVED) {
                throw CairoException.critical(0)
                        .put("posting index key file too short [expected>=")
                        .put(PostingIndexUtils.KEY_FILE_RESERVED)
                        .put(", actual=").put(keyFileSize)
                        .put(", path=").put(keyFile).put(']');
            }
            keyMem.of(
                    ff,
                    keyFile,
                    ff.getMapPageSize(),
                    PostingIndexUtils.KEY_FILE_RESERVED,
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );

            readIndexMetadataFromBestPage(-1);

            int version = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_FORMAT_VERSION);
            if (version != PostingIndexUtils.FORMAT_VERSION) {
                throw CairoException.critical(0).put("Unsupported Posting index version: ").put(version);
            }

            valueMem.of(
                    ff,
                    PostingIndexUtils.valueFileName(path.trimTo(pLen), columnName, columnNameTxn, valueFileTxn),
                    valueMemSize,
                    valueMemSize,
                    MemoryTag.MMAP_INDEX_READER
            );

            openSidecarFilesIfPresent(path.trimTo(pLen), columnName, columnNameTxn);
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(pLen);
        }
    }

    @Override
    public void reloadConditionally() {
        Unsafe.getUnsafe().loadFence();
        long seqA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        long seqB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        if (Math.max(seqA, seqB) <= keyFileSequence) {
            return;
        }
        long prevSequence = keyFileSequence;
        readIndexMetadataFromBestPage(valueFileTxn);
        if (keyFileSequence != prevSequence && valueMemSize > 0) {
            ((MemoryCMR) this.valueMem).changeSize(valueMemSize);
        }
    }

    @TestOnly
    public void setGenLookupCacheBudget(long budget) {
        genLookup.setCacheMemoryBudget(budget);
    }

    private static int denseIndexFromWriter(RecordMetadata metadata, int writerIdx) {
        for (int d = 0, n = metadata.getColumnCount(); d < n; d++) {
            if (metadata.getColumnMetadata(d).getWriterIndex() == writerIdx) {
                return d;
            }
        }
        return -1;
    }

    private void closeSidecarMems() {
        Misc.freeObjListAndKeepObjects(sidecarMems);
        coverCount = 0;
        sidecarColumnIndices.clear();
        sidecarColumnTypes.clear();
        sidecarCovTs.clear();
    }

    private int collectDenseGenKeys(long genFileOffset, int genKeyCount, DirectBitSet foundKeys) {
        long genAddr = valueMem.addressOf(genFileOffset);
        int sc = PostingIndexUtils.strideCount(genKeyCount);
        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
        int newlyFound = 0;

        for (int s = 0; s < sc; s++) {
            long strideOff = Unsafe.getUnsafe().getLong(genAddr + (long) s * Long.BYTES);
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

    private int collectSparseGenKeys(long genFileOffset, int activeKeyCount, DirectBitSet foundKeys) {
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
            Path path,
            CharSequence columnName,
            long columnNameTxn
    ) {
        int plen = path.size();
        try {
            LPSZ pciFile = PostingIndexUtils.coverInfoFileName(path, columnName, columnNameTxn);
            if (!ff.exists(pciFile)) {
                return;
            }

            infoMem.of(ff, pciFile, ff.getMapPageSize(), -1, MemoryTag.MMAP_INDEX_READER, CairoConfiguration.O_NONE, -1);
            if (infoMem.size() < 8) {
                return;
            }
            int magic = infoMem.getInt(0);
            if (magic != PostingIndexUtils.COVER_INFO_MAGIC) {
                return;
            }
            int count = infoMem.getInt(4);
            if (count <= 0) {
                return;
            }
            sidecarColumnIndices.clear();
            sidecarColumnTypes.clear();
            sidecarCovTs.clear();
            closeSidecarMems();
            for (int i = 0; i < count; i++) {
                int writerIdx = infoMem.getInt(8 + (long) i * Integer.BYTES);
                sidecarColumnIndices.add(writerIdx);
                if (sidecarMems.getQuiet(i) == null) {
                    sidecarMems.extendAndSet(i, Vm.getCMRInstance());
                }
                if (writerIdx < 0) {
                    sidecarColumnTypes.add(-1);
                    sidecarCovTs.add(TableUtils.COLUMN_NAME_TXN_NONE);
                    continue;
                }
                int denseIdx = denseIndexFromWriter(metadata, writerIdx);
                if (denseIdx < 0) {
                    sidecarColumnTypes.add(-1);
                    sidecarCovTs.add(TableUtils.COLUMN_NAME_TXN_NONE);
                    continue;
                }
                sidecarColumnTypes.add(metadata.getColumnType(denseIdx));
                sidecarCovTs.add(columnVersionReader.getColumnNameTxn(partitionTimestamp, writerIdx));
            }
            coverCount = count;
        } catch (Throwable e) {
            LOG.error().$("failed to open sidecar files").$(e).$();
            closeSidecarMems();
        } finally {
            Misc.free(infoMem);
            path.trimTo(plen);
        }
    }

    private void readIndexMetadataFromBestPage(long pinnedSealTxn) {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        long prevSeqStartA = Long.MIN_VALUE;
        long prevSeqStartB = Long.MIN_VALUE;
        while (true) {
            Unsafe.getUnsafe().loadFence();
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

            boolean anyPinMismatch = false;
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
                long sealTxn = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN);

                Unsafe.getUnsafe().loadFence();
                long seqEnd = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

                if (seqStart == seqEnd && seqStart > 0
                        && genCount >= 0 && genCount <= PostingIndexUtils.MAX_GEN_COUNT) {
                    if (sealTxn < 0) {
                        continue;
                    }
                    if (pinnedSealTxn >= 0 && sealTxn != pinnedSealTxn) {
                        anyPinMismatch = true;
                        continue;
                    }
                    genLookup.snapshotMetadata(keyMem, genCount, tryPage);

                    Unsafe.getUnsafe().loadFence();
                    if (keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) != seqStart) {
                        break;
                    }

                    genLookup.invalidateCache();

                    this.activePageOffset = tryPage;
                    this.keyFileSequence = seqStart;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.genCount = genCount;
                    this.valueFileTxn = sealTxn;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                    return;
                }
            }

            // At least one page was seqlock-consistent but carried a newer
            // sealTxn than our pin. sealTxn is monotonic, so retrying won't
            // help — bail and let caller preserve the previous snapshot.
            if (anyPinMismatch) {
                return;
            }

            // Neither page was seqlock-consistent; if both seqStarts are
            // stuck (writer idle), the corruption won't self-heal.
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
    }

    protected static long readVarBlockOffset(long offsetsAddr, int ordinal, boolean longOffsets) {
        if (longOffsets) {
            return Unsafe.getUnsafe().getLong(offsetsAddr + (long) ordinal * Long.BYTES);
        }
        // Zero-extend to long; offsets in int blocks are non-negative.
        return Unsafe.getUnsafe().getInt(offsetsAddr + (long) ordinal * Integer.BYTES) & 0xFFFFFFFFL;
    }

    protected static long varBlockOffsetsSize(int count, boolean longOffsets) {
        return (long) (count + 1) * (longOffsets ? Long.BYTES : Integer.BYTES);
    }

    protected void ensureSidecarOpen(int c) {
        MemoryMR mem = sidecarMems.getQuick(c);
        if (mem.size() > 0) {
            return;
        }
        int pLen = sidecarBasePath.size();
        try {
            LPSZ pcFile = PostingIndexUtils.coverDataFileName(
                    sidecarBasePath.trimTo(pLen),
                    indexColumnName,
                    c,
                    columnTxn,
                    sidecarCovTs.getQuick(c),
                    valueFileTxn
            );
            if (!ff.exists(pcFile)) {
                return;
            }
            mem.of(ff, pcFile, ff.getMapPageSize(), -1, MemoryTag.MMAP_INDEX_READER, CairoConfiguration.O_NONE, -1);
        } finally {
            sidecarBasePath.trimTo(pLen);
        }
    }

    protected abstract class AbstractCoveringCursor implements CoveringRowCursor {
        protected final BorrowedArray arrayView = new BorrowedArray();
        protected final DirectBinarySequence binView = new DirectBinarySequence();
        protected final LongList currentGenSidecarOffsets = new LongList();
        protected final DirectString stringViewA = new DirectString();
        protected final DirectString stringViewB = new DirectString();
        protected final DirectUtf8String varcharViewA = new DirectUtf8String();
        protected final DirectUtf8String varcharViewB = new DirectUtf8String();
        protected int cachedKeyBlockStride = -1;
        protected int cachedSidecarIdx;
        protected int cursorGenCount;
        protected long decodeWorkspaceAddr;
        protected int decodeWorkspaceCapacity;
        protected int denseVarKeyStartCount;
        protected long[] fsstCachedBlockBases;
        protected long[] fsstDecoderAddrs;
        protected long[] fsstDstAddrs;
        protected long[] fsstDstCapacities;
        protected long[] fsstOffsetsAddrs;
        protected long[] fsstOffsetsCapacities;
        protected boolean isCurrentGenDense;
        protected long[] keyBlockAddrs;
        protected int requestedKey;
        protected int sealedGenKeyCount;
        protected int sidecarOrdinal;
        protected int sidecarStrideKeyStart;
        private long[] colCacheAddrs;
        private long[] colCacheBlockAddrs;
        private int[] colCacheCapacities;
        private long[] colPointBlockAddrs;

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
                return Numbers.INT_NULL;
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
                return Numbers.LONG_NULL;
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
                return Numbers.LONG_NULL;
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
                return Numbers.LONG_NULL;
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
                return Numbers.LONG_NULL;
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
                return Numbers.LONG_NULL;
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
                return Numbers.LONG_NULL;
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
                return Numbers.LONG_NULL;
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
        public Utf8Sequence getCoveredVarcharA(int includeIdx) {
            return getVarSidecarUtf8(includeIdx, varcharViewA);
        }

        @Override
        public Utf8Sequence getCoveredVarcharB(int includeIdx) {
            return getVarSidecarUtf8(includeIdx, varcharViewB);
        }

        @Override
        public boolean hasCovering() {
            return coverCount > 0;
        }

        @Override
        public boolean isCoveredAvailable(int includeIdx) {
            return includeIdx >= 0 && includeIdx < coverCount
                    && sidecarMems.getQuick(includeIdx).size() > 0;
        }

        @Override
        public long seekToLast() {
            throw new UnsupportedOperationException(
                    "seekToLast: use a backward index reader; forward iteration is O(n)");
        }

        @Override
        public long size() {
            if (requestedKey < 0) {
                return 0;
            }
            long total = 0;
            for (int g = 0; g < cursorGenCount; g++) {
                int gkc = genLookup.getGenKeyCount(g);
                if (gkc >= 0) {
                    total += getDenseGenKeyCount(g, gkc);
                } else if (!genLookup.notContainKey(valueMem, g, requestedKey)) {
                    total += getSparseGenKeyCount(g);
                }
            }
            return total;
        }

        private CharSequence decompressFsstStr(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx, DirectString view, boolean longOffsets) {
            if (isFsstBlockUnavailable(mem, blockBase, count, includeIdx, longOffsets)) {
                return null;
            }
            long offsBase = fsstOffsetsAddrs[includeIdx];
            long lo = Unsafe.getUnsafe().getLong(offsBase + (long) ordinal * Long.BYTES);
            long hi = Unsafe.getUnsafe().getLong(offsBase + (long) (ordinal + 1) * Long.BYTES);
            if (lo == hi) {
                return null;
            }
            long valAddr = fsstDstAddrs[includeIdx] + lo;
            int len = Unsafe.getUnsafe().getInt(valAddr);
            if (len < 0) {
                return null;
            }
            return view.of(valAddr + Integer.BYTES, len);
        }

        private Utf8Sequence decompressFsstUtf8(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx, DirectUtf8String view, boolean longOffsets) {
            if (isFsstBlockUnavailable(mem, blockBase, count, includeIdx, longOffsets)) {
                return null;
            }
            long offsBase = fsstOffsetsAddrs[includeIdx];
            long lo = Unsafe.getUnsafe().getLong(offsBase + (long) ordinal * Long.BYTES);
            long hi = Unsafe.getUnsafe().getLong(offsBase + (long) (ordinal + 1) * Long.BYTES);
            if (lo == hi) {
                return null;
            }
            long valAddr = fsstDstAddrs[includeIdx] + lo;
            return view.of(valAddr, valAddr + (hi - lo));
        }

        private void ensureFsstCacheCapacity() {
            if (fsstCachedBlockBases == null) {
                fsstCachedBlockBases = new long[coverCount];
                Arrays.fill(fsstCachedBlockBases, -1L);
                fsstDecoderAddrs = new long[coverCount];
                fsstDstAddrs = new long[coverCount];
                fsstDstCapacities = new long[coverCount];
                fsstOffsetsAddrs = new long[coverCount];
                fsstOffsetsCapacities = new long[coverCount];
            }
        }

        private long findDenseVarBlockBase(int includeIdx) {
            if (includeIdx >= sidecarMems.size() || sealedGenKeyCount <= 0) {
                return -1;
            }
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return -1;
            }
            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int siSize = PostingIndexUtils.strideIndexSize(sealedGenKeyCount);
            long strideIdxOffset = PostingIndexUtils.PC_HEADER_SIZE + (long) stride * Long.BYTES;
            if (strideIdxOffset + Long.BYTES > mem.size()) {
                return -1;
            }
            long strideOff = Unsafe.getUnsafe().getLong(mem.addressOf(strideIdxOffset));
            return siSize + strideOff;
        }

        private void freeFsstCache() {
            if (fsstCachedBlockBases == null) {
                return;
            }
            for (int i = 0, n = fsstCachedBlockBases.length; i < n; i++) {
                if (fsstDecoderAddrs[i] != 0) {
                    Unsafe.free(fsstDecoderAddrs[i], FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_INDEX_READER);
                    fsstDecoderAddrs[i] = 0;
                }
                if (fsstDstAddrs[i] != 0) {
                    Unsafe.free(fsstDstAddrs[i], fsstDstCapacities[i], MemoryTag.NATIVE_INDEX_READER);
                    fsstDstAddrs[i] = 0;
                    fsstDstCapacities[i] = 0;
                }
                if (fsstOffsetsAddrs[i] != 0) {
                    Unsafe.free(fsstOffsetsAddrs[i], fsstOffsetsCapacities[i], MemoryTag.NATIVE_INDEX_READER);
                    fsstOffsetsAddrs[i] = 0;
                    fsstOffsetsCapacities[i] = 0;
                }
                fsstCachedBlockBases[i] = -1;
            }
        }

        private int getDenseGenKeyCount(int gen, int genKeyCount) {
            if (requestedKey >= genKeyCount) {
                return 0;
            }
            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            long genAddr = valueMem.addressOf(genLookup.getGenFileOffset(gen));
            long strideOff = Unsafe.getUnsafe().getLong(genAddr + (long) stride * Long.BYTES);
            long strideAddr = genAddr + PostingIndexUtils.strideIndexSize(genKeyCount) + strideOff;
            byte mode = Unsafe.getUnsafe().getByte(strideAddr);
            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                int start = Unsafe.getUnsafe().getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int end = Unsafe.getUnsafe().getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES);
                return end - start;
            }
            if (mode != PostingIndexUtils.STRIDE_MODE_DELTA) {
                throw CairoException.critical(0).put(INDEX_CORRUPT).put(" [bad stride mode=").put(mode).put(']');
            }
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            return Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
        }

        private byte getRawSidecarByte(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return 0;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Byte.BYTES);
            return Unsafe.getUnsafe().getByte(addr);
        }

        private double getRawSidecarDouble(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return Double.NaN;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Double.BYTES);
            return Unsafe.getUnsafe().getDouble(addr);
        }

        private float getRawSidecarFloat(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return Float.NaN;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Float.BYTES);
            return Unsafe.getUnsafe().getFloat(addr);
        }

        private int getRawSidecarInt(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return Integer.MIN_VALUE;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Integer.BYTES);
            return Unsafe.getUnsafe().getInt(addr);
        }

        private long getRawSidecarLong(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return Long.MIN_VALUE;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Long.BYTES);
            return Unsafe.getUnsafe().getLong(addr);
        }

        private long getRawSidecarMultiLong(int includeIdx, int valueSize, int longIndex) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return Long.MIN_VALUE;
            }
            long addr = mem.addressOf(
                    currentGenSidecarOffsets.getQuick(includeIdx) + 4
                            + (long) cachedSidecarIdx * valueSize
                            + (long) longIndex * Long.BYTES
            );
            return Unsafe.getUnsafe().getLong(addr);
        }

        private short getRawSidecarShort(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return 0;
            }
            long addr = mem.addressOf(currentGenSidecarOffsets.getQuick(includeIdx) + 4 + (long) cachedSidecarIdx * Short.BYTES);
            return Unsafe.getUnsafe().getShort(addr);
        }

        private int getSparseGenKeyCount(int gen) {
            int minKey = genLookup.getGenMinKey(gen);
            int maxKey = genLookup.getGenMaxKey(gen);
            if (requestedKey < minKey || requestedKey > maxKey) {
                return 0;
            }
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(gen, valueMem));
            int k = requestedKey - minKey;
            int start = Unsafe.getUnsafe().getInt(prefixSumAddr + (long) k * Integer.BYTES);
            int end = Unsafe.getUnsafe().getInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES);
            if (start >= end) {
                return 0;
            }
            int activeKeyCount = -genLookup.getGenKeyCount(gen);
            long countsBase = valueMem.addressOf(genFileOffset) + (long) activeKeyCount * Integer.BYTES;
            return Unsafe.getUnsafe().getInt(countsBase + (long) start * Integer.BYTES);
        }

        private ArrayView getVarSidecarArray(int includeIdx, int columnType) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return null;
            }
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets.getQuick(includeIdx);
            if (blockBase < 0) {
                return null;
            }
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSSTNative.FSST_BLOCK_FLAG) != 0;
            boolean longOffsets = (rawCount & PostingIndexUtils.LONG_OFFSETS_FLAG) != 0;
            int count = rawCount & ~(FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG);
            if (ordinal >= count) {
                return null;
            }

            long dataAddr;
            int dataLen;
            if (fsst) {
                if (isFsstBlockUnavailable(mem, blockBase, count, includeIdx, longOffsets)) {
                    return null;
                }
                long offsBase = fsstOffsetsAddrs[includeIdx];
                long lo = Unsafe.getUnsafe().getLong(offsBase + (long) ordinal * Long.BYTES);
                long hi = Unsafe.getUnsafe().getLong(offsBase + (long) (ordinal + 1) * Long.BYTES);
                if (lo == hi) {
                    arrayView.ofNull();
                    return arrayView;
                }
                dataAddr = fsstDstAddrs[includeIdx] + lo;
                dataLen = (int) (hi - lo);
            } else {
                long offsetsAddr = mem.addressOf(blockBase + 4);
                long lo = readVarBlockOffset(offsetsAddr, ordinal, longOffsets);
                long hi = readVarBlockOffset(offsetsAddr, ordinal + 1, longOffsets);
                if (lo == hi) {
                    arrayView.ofNull();
                    return arrayView;
                }
                long dataBase = blockBase + 4 + varBlockOffsetsSize(count, longOffsets);
                dataAddr = mem.addressOf(dataBase + lo);
                // A single var value is always well under 2 GB
                dataLen = (int) (hi - lo);
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
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return null;
            }
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets.getQuick(includeIdx);
            if (blockBase < 0) {
                return null;
            }
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSSTNative.FSST_BLOCK_FLAG) != 0;
            boolean longOffsets = (rawCount & PostingIndexUtils.LONG_OFFSETS_FLAG) != 0;
            int count = rawCount & ~(FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG);
            if (ordinal >= count) {
                return null;
            }

            if (fsst) {
                if (isFsstBlockUnavailable(mem, blockBase, count, includeIdx, longOffsets)) {
                    return null;
                }
                long offsBase = fsstOffsetsAddrs[includeIdx];
                long lo = Unsafe.getUnsafe().getLong(offsBase + (long) ordinal * Long.BYTES);
                long hi = Unsafe.getUnsafe().getLong(offsBase + (long) (ordinal + 1) * Long.BYTES);
                if (lo == hi) {
                    return null;
                }
                long valAddr = fsstDstAddrs[includeIdx] + lo;
                long len = Unsafe.getUnsafe().getLong(valAddr);
                if (len < 0) {
                    return null;
                }
                return binView.of(valAddr + Long.BYTES, len);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            long lo = readVarBlockOffset(offsetsAddr, ordinal, longOffsets);
            long hi = readVarBlockOffset(offsetsAddr, ordinal + 1, longOffsets);
            if (lo == hi) {
                return null;
            }
            long dataBase = blockBase + 4 + varBlockOffsetsSize(count, longOffsets);
            long dataAddr = mem.addressOf(dataBase + lo);
            long len = Unsafe.getUnsafe().getLong(dataAddr);
            if (len < 0) {
                return null;
            }
            return binView.of(dataAddr + Long.BYTES, len);
        }

        private long getVarSidecarBinLen(int includeIdx) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return -1;
            }
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets.getQuick(includeIdx);
            if (blockBase < 0) {
                return -1;
            }
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSSTNative.FSST_BLOCK_FLAG) != 0;
            boolean longOffsets = (rawCount & PostingIndexUtils.LONG_OFFSETS_FLAG) != 0;
            int count = rawCount & ~(FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG);
            if (ordinal >= count) {
                return -1;
            }

            if (fsst) {
                if (isFsstBlockUnavailable(mem, blockBase, count, includeIdx, longOffsets)) {
                    return -1;
                }
                long offsBase = fsstOffsetsAddrs[includeIdx];
                long lo = Unsafe.getUnsafe().getLong(offsBase + (long) ordinal * Long.BYTES);
                long hi = Unsafe.getUnsafe().getLong(offsBase + (long) (ordinal + 1) * Long.BYTES);
                if (lo == hi) {
                    return -1;
                }
                return Unsafe.getUnsafe().getLong(fsstDstAddrs[includeIdx] + lo);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            long lo = readVarBlockOffset(offsetsAddr, ordinal, longOffsets);
            long hi = readVarBlockOffset(offsetsAddr, ordinal + 1, longOffsets);
            if (lo == hi) {
                return -1;
            }
            long dataBase = blockBase + 4 + varBlockOffsetsSize(count, longOffsets);
            long dataAddr = mem.addressOf(dataBase + lo);
            return Unsafe.getUnsafe().getLong(dataAddr);
        }

        private CharSequence getVarSidecarStr(int includeIdx, DirectString view) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return null;
            }
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets.getQuick(includeIdx);
            if (blockBase < 0) {
                return null;
            }
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSSTNative.FSST_BLOCK_FLAG) != 0;
            boolean longOffsets = (rawCount & PostingIndexUtils.LONG_OFFSETS_FLAG) != 0;
            int count = rawCount & ~(FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG);
            if (ordinal >= count) {
                return null;
            }

            if (fsst) {
                return decompressFsstStr(mem, blockBase, count, ordinal, includeIdx, view, longOffsets);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            long lo = readVarBlockOffset(offsetsAddr, ordinal, longOffsets);
            long hi = readVarBlockOffset(offsetsAddr, ordinal + 1, longOffsets);
            if (lo == hi) {
                return null;
            }
            long dataBase = blockBase + 4 + varBlockOffsetsSize(count, longOffsets);
            long dataAddr = mem.addressOf(dataBase + lo);
            int len = Unsafe.getUnsafe().getInt(dataAddr);
            if (len < 0) {
                return null;
            }
            return view.of(dataAddr + Integer.BYTES, len);
        }

        private Utf8Sequence getVarSidecarUtf8(int includeIdx, DirectUtf8String view) {
            MemoryMR mem = sidecarMems.getQuick(includeIdx);
            if (mem.size() == 0) {
                return null;
            }
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + cachedSidecarIdx
                    : cachedSidecarIdx;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets.getQuick(includeIdx);
            if (blockBase < 0) {
                return null;
            }
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSSTNative.FSST_BLOCK_FLAG) != 0;
            boolean longOffsets = (rawCount & PostingIndexUtils.LONG_OFFSETS_FLAG) != 0;
            int count = rawCount & ~(FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG);
            if (ordinal >= count) {
                return null;
            }

            if (fsst) {
                return decompressFsstUtf8(mem, blockBase, count, ordinal, includeIdx, view, longOffsets);
            }

            long offsetsAddr = mem.addressOf(blockBase + 4);
            long lo = readVarBlockOffset(offsetsAddr, ordinal, longOffsets);
            long hi = readVarBlockOffset(offsetsAddr, ordinal + 1, longOffsets);
            if (lo == hi) {
                return null;
            }
            long dataBase = blockBase + 4 + varBlockOffsetsSize(count, longOffsets);
            long dataAddr = mem.addressOf(dataBase + lo);
            return view.of(dataAddr, dataAddr + (hi - lo));
        }

        private boolean isFsstBlockUnavailable(MemoryMR mem, long blockBase, int count, int includeIdx, boolean longOffsets) {
            ensureFsstCacheCapacity();
            if (fsstCachedBlockBases[includeIdx] == blockBase) {
                return false;
            }

            long pos = blockBase + 4;
            int tableLen = Unsafe.getUnsafe().getShort(mem.addressOf(pos)) & 0xFFFF;
            long tableAddr = mem.addressOf(pos + 2);
            long offsetsAddr = mem.addressOf(pos + 2 + tableLen);
            long offsetsTableSize = varBlockOffsetsSize(count, longOffsets);
            long dataBase = pos + 2 + tableLen + offsetsTableSize;

            long decoderAddr = fsstDecoderAddrs[includeIdx];
            if (decoderAddr == 0) {
                decoderAddr = Unsafe.malloc(FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_INDEX_READER);
                fsstDecoderAddrs[includeIdx] = decoderAddr;
            }
            if (FSSTNative.importTable(decoderAddr, tableAddr) < 0) {
                fsstCachedBlockBases[includeIdx] = -1;
                return true;
            }

            // Buffers are overwritten end-to-end on each miss; free + malloc skips realloc's stale-copy.
            long offsetsBytes = (long) (count + 1) * Long.BYTES;
            if (fsstOffsetsCapacities[includeIdx] < offsetsBytes) {
                if (fsstOffsetsAddrs[includeIdx] != 0) {
                    Unsafe.free(fsstOffsetsAddrs[includeIdx], fsstOffsetsCapacities[includeIdx], MemoryTag.NATIVE_INDEX_READER);
                }
                fsstOffsetsAddrs[includeIdx] = Unsafe.malloc(offsetsBytes, MemoryTag.NATIVE_INDEX_READER);
                fsstOffsetsCapacities[includeIdx] = offsetsBytes;
            }

            long totalCompressed = readVarBlockOffset(offsetsAddr, count, longOffsets);
            long initialDstCap = Math.max(totalCompressed * 4L, 256L);
            if (fsstDstCapacities[includeIdx] < initialDstCap) {
                if (fsstDstAddrs[includeIdx] != 0) {
                    Unsafe.free(fsstDstAddrs[includeIdx], fsstDstCapacities[includeIdx], MemoryTag.NATIVE_INDEX_READER);
                }
                fsstDstAddrs[includeIdx] = Unsafe.malloc(initialDstCap, MemoryTag.NATIVE_INDEX_READER);
                fsstDstCapacities[includeIdx] = initialDstCap;
            }

            int srcOffsetsWidth = longOffsets ? Long.BYTES : Integer.BYTES;
            while (true) {
                long decoded = FSSTNative.decompressBlock(
                        decoderAddr,
                        mem.addressOf(dataBase), offsetsAddr, srcOffsetsWidth, count,
                        fsstDstAddrs[includeIdx], fsstDstCapacities[includeIdx],
                        fsstOffsetsAddrs[includeIdx]
                );
                if (decoded >= 0) {
                    break;
                }
                long newCap = fsstDstCapacities[includeIdx] * 2L;
                Unsafe.free(fsstDstAddrs[includeIdx], fsstDstCapacities[includeIdx], MemoryTag.NATIVE_INDEX_READER);
                fsstDstAddrs[includeIdx] = Unsafe.malloc(newCap, MemoryTag.NATIVE_INDEX_READER);
                fsstDstCapacities[includeIdx] = newCap;
            }

            fsstCachedBlockBases[includeIdx] = blockBase;
            return false;
        }

        protected void cacheSidecarKeyAddrs(int stride, int localKey) {
            if (coverCount == 0 || sealedGenKeyCount <= 0) {
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
                MemoryMR mem = sidecarMems.getQuick(c);
                if (mem.size() == 0) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long strideIdxOffset = PostingIndexUtils.PC_HEADER_SIZE + (long) stride * Long.BYTES;
                if (strideIdxOffset + Long.BYTES > mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long strideOff = mem.getLong(strideIdxOffset);
                long strideDataStart = siSize + strideOff;
                if (strideDataStart >= mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long keyOffsetsEnd = strideDataStart + (long) ks * Long.BYTES;
                if (keyOffsetsEnd > mem.size()) {
                    keyBlockAddrs[c] = 0;
                    continue;
                }
                long keyOffsetsAddr = mem.addressOf(strideDataStart);
                long keyBlockOff = Unsafe.getUnsafe().getLong(keyOffsetsAddr + (long) localKey * Long.BYTES);
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
            freeFsstCache();
        }

        protected void computePerColumnSidecarOffsets(int gen) {
            if (coverCount == 0) {
                return;
            }
            currentGenSidecarOffsets.setPos(coverCount);
            // For dense gens the header slot is 0 (never written, mmap zero-filled).
            // Dense reads go through stride_index (findDenseVarBlockBase), so the
            // 0 here is harmless — sparse reads are the only consumer of this cache.
            for (int c = 0; c < coverCount; c++) {
                MemoryMR mem = sidecarMems.getQuick(c);
                currentGenSidecarOffsets.setQuick(c, mem.size() == 0 ? 0L : mem.getLong((long) gen * Long.BYTES));
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
            if (blockAddr == 0) {
                return false;
            }
            if (colCacheBlockAddrs[includeIdx] == blockAddr) {
                return true;
            }
            if (colPointBlockAddrs[includeIdx] != blockAddr) {
                colPointBlockAddrs[includeIdx] = blockAddr;
                return false;
            }
            int rawCount = Unsafe.getUnsafe().getInt(blockAddr);
            int count = rawCount & ~CoveringCompressor.RAW_BLOCK_FLAG;
            if (count <= 0) {
                colCacheBlockAddrs[includeIdx] = blockAddr;
                return true;
            }
            int colType = sidecarColumnTypes.getQuick(includeIdx);
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
                decodeWorkspaceAddr = Unsafe.realloc(
                        decodeWorkspaceAddr,
                        (long) decodeWorkspaceCapacity * Long.BYTES,
                        (long) count * Long.BYTES,
                        MemoryTag.NATIVE_INDEX_READER
                );
                decodeWorkspaceCapacity = count;
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
    }
}

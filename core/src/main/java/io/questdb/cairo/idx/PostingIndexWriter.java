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

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.PostingSealPurgeTask;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.idx.PostingIndexUtils.*;

/**
 * Delta + FoR64 BitPacking bitmap index writer.
 * <p>
 * Each commit appends one generation (covering all keys) to the value file.
 * No symbol table needed — encoding is purely arithmetic.
 */
public class PostingIndexWriter implements IndexWriter {
    // Minimum raw data size to attempt FSST compression (below this, overhead > savings)
    private static final int FSST_MIN_RAW_SIZE = 4096;
    private static final int INITIAL_KEY_CAPACITY = 64;
    private static final Log LOG = LogFactory.getLog(PostingIndexWriter.class);
    private static final int MAX_GEN_COUNT = PostingIndexUtils.MAX_GEN_COUNT;
    private static final int PENDING_SLOT_CAPACITY = 8;
    private final double alignedBitWidthThreshold;
    private final CairoConfiguration configuration;
    // O3 addr-based covering: caller-provided native memory addresses
    private final LongList coveredColumnAddrs = new LongList();
    private final IntList coveredColumnIndices = new IntList();
    private final LongList coveredColumnNameTxns = new LongList();
    private final ObjList<CharSequence> coveredColumnNames = new ObjList<>();
    private final IntList coveredColumnShifts = new IntList();
    private final LongList coveredColumnTops = new LongList();
    private final IntList coveredColumnTypes = new IntList();
    private final Utf8StringSink coveredPartitionPath = new Utf8StringSink();
    private final PostingIndexUtils.DecodeContext decodeCtx = new PostingIndexUtils.DecodeContext();
    private final PostingIndexUtils.EncodeContext encodeCtx = new PostingIndexUtils.EncodeContext();
    private final FilesFacade ff;
    private final StringSink indexName = new StringSink();
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    private final Utf8StringSink lastOrphanScanPath = new Utf8StringSink();
    private final LongList orphanSealTxns = new LongList();
    private final Utf8StringSink partitionPath = new Utf8StringSink();
    private final ObjList<PendingSealPurge> pendingPurgePool = new ObjList<>();
    private final ObjList<PendingSealPurge> pendingPurges = new ObjList<>();
    private final byte rowIdEncoding;
    private final MemoryMARW sealValueMem = Vm.getCMARWInstance();
    private final ObjList<MemoryMARW> sidecarMems = new ObjList<>();
    // Staging for switchToSealedValueFile: new .pv is mapped here first, then
    // swapped into valueMem. On mmap failure valueMem keeps its old mapping so
    // rollback can still decode existing gens.
    private final MemoryMARW stagingValueMem = Vm.getCMARWInstance();
    private final int[] strideBpKeySizes = new int[PostingIndexUtils.DENSE_STRIDE];
    private final int[] strideKeyCounts = new int[PostingIndexUtils.DENSE_STRIDE];
    private final long[] strideKeyOffsets = new long[PostingIndexUtils.DENSE_STRIDE];
    private final MemoryMARW valueMem = Vm.getCMARWInstance();
    private int activeKeyCount;
    private int[] activeKeyIds = new int[INITIAL_KEY_CAPACITY];
    private long activePageOffset;
    private int blockCapacity;
    private int coverCount;
    private long[] coveredAuxReadAddrs;
    private long[] coveredAuxReadSizes;
    // RO mmaps owned by the writer (opened from coveredColumnNames + partitionPath).
    // size > 0 means writer-owned; size == 0 means addr-based (O3, not owned).
    private long[] coveredColReadAddrs;
    private long[] coveredColReadSizes;
    private long currentPublishTableTxn = -1;
    // When true, writeMetadataPage tags the inactive page with
    // SEAL_TXN_TENTATIVE so fd-based O3 per-column commits stay invisible
    // until the surrounding TableWriter transaction reaches seal.
    private boolean deferMetadataPublish;
    private long flushHeaderBuf;
    private int flushHeaderBufCapacity;
    private long fsstBatchScratchAddr;
    private long fsstBatchScratchCap;
    private long fsstCmpAddr;
    private long fsstCmpCap;
    private long fsstCmpOffsAddr;
    private long fsstCmpOffsCap;
    private long fsstSrcOffsAddr;
    private long fsstSrcOffsCap;
    private long fsstTableAddr;
    private int genCount;
    private boolean hasPendingData;
    private boolean hasSpillData;
    private int keyCapacity;
    private int keyCount;
    private int orphanScanPostLiveCount;
    private int orphanScanPreLiveCount;
    private long packedResidualsAddr;
    private int packedResidualsCapacity;
    private long partitionNameTxn = -1;
    private long partitionTimestamp = Long.MIN_VALUE;
    private long pendingCountsAddr;
    // Publish-txn the next in-process seal should record. -1 means no
    // commit-supplied value (e.g., close-time seal); seal then uses a
    // conservative scoreboard range.
    private long pendingPublishTableTxn = -1;
    private long pendingValuesAddr;
    private long postingColumnNameTxn; // host column-instance txn
    private long prevPublishTableTxn = -1;
    private long[] savedSidecarBufs;
    private long[] savedSidecarSizes;
    private MemoryMARW sealTarget; // points to sealValueMem during seal, valueMem during flush
    private long sealTxn;
    private MemoryMARW sidecarInfoMem;
    private int spillArraysCapacity;
    private long spillKeyAddrsAddr;
    private long spillKeyCapacitiesAddr;
    private long spillKeyCountsAddr;
    private long tentativeSlotOffset = -1L;
    private int timestampColumnIndex = -1;
    private long unpackBatchAddr;
    private int unpackBatchCapacity;
    private long valueMemSize;

    public PostingIndexWriter(CairoConfiguration configuration) {
        this(configuration, configuration.getPostingIndexRowIdEncoding());
    }

    public PostingIndexWriter(CairoConfiguration configuration, byte rowIdEncoding) {
        this.alignedBitWidthThreshold = configuration.getPostingIndexAlignedBitWidthThreshold();
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.rowIdEncoding = rowIdEncoding;
    }

    @TestOnly
    public PostingIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        of(path, name, columnNameTxn, true);
    }

    public static void initKeyMemory(MemoryMA keyMem, int blockCapacity) {
        initKeyMemory(keyMem, blockCapacity, 0L);
    }

    @Override
    public void add(int key, long value) {
        if (key < 0) {
            throw CairoException.critical(0).put("index key cannot be negative [key=").put(key).put(']');
        }

        if (pendingCountsAddr == 0) {
            // Pending buffers were freed by seal() to reduce peak memory.
            // Reallocate for continued writes (mid-stream seal case).
            allocateNativeBuffers();
        }

        if (key >= keyCapacity) {
            growKeyBuffers(key + 1);
        }

        int count = Unsafe.getInt(pendingCountsAddr + (long) key * Integer.BYTES);

        if (count >= PENDING_SLOT_CAPACITY) {
            // Buffer full for this key — spill its values to the overflow buffer
            // instead of flushing ALL keys. This prevents gen-count explosion
            // when a single hot key drives all overflows.
            spillKey(key, count);
            count = 0;
        }

        if (count > 0) {
            long lastVal = Unsafe.getLong(
                    pendingValuesAddr + ((long) key * PENDING_SLOT_CAPACITY + count - 1) * Long.BYTES);
            if (value < lastVal) {
                throw CairoException.critical(0)
                        .put("index values must be added in ascending order [lastValue=")
                        .put(lastVal).put(", newValue=").put(value).put(']');
            }
        } else {
            int spillCount = getSpillCount(key);
            if (spillCount > 0) {
                long spillAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                long lastSpilledVal = Unsafe.getLong(spillAddr + (long) (spillCount - 1) * Long.BYTES);
                if (value < lastSpilledVal) {
                    throw CairoException.critical(0)
                            .put("index values must be added in ascending order [lastSpilledValue=")
                            .put(lastSpilledVal).put(", newValue=").put(value).put(']');
                }
            } else {
                activeKeyIds[activeKeyCount++] = key;
            }
        }

        Unsafe.putLong(
                pendingValuesAddr + ((long) key * PENDING_SLOT_CAPACITY + count) * Long.BYTES, value);
        Unsafe.putInt(pendingCountsAddr + (long) key * Integer.BYTES, count + 1);

        if (key >= keyCount) {
            keyCount = key + 1;
        }
        hasPendingData = true;
    }

    @Override
    public void clear() {
        close();
    }

    public void clearCovering() {
        unmapCoveredColumnReads();
        this.coveredColumnAddrs.clear();
        this.coveredPartitionPath.clear();
        this.coveredColumnNames.clear();
        this.coveredColumnNameTxns.clear();
        this.coverCount = 0;
    }

    @Override
    public void close() {
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
                Misc.free(stagingValueMem);
                if (valueMem.isOpen()) {
                    valueMem.close(false, (byte) 0);
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
                sealTarget = null;
                releasePendingPurges();
                pendingPublishTableTxn = -1;
                currentPublishTableTxn = -1;
                prevPublishTableTxn = -1;
                deferMetadataPublish = false;
                tentativeSlotOffset = -1L;
                partitionPath.clear();
                indexName.clear();
            }
        }
    }

    @Override
    public void closeNoTruncate() {
        try {
            seal();
        } finally {
            try {
                if (keyMem.isOpen()) {
                    keyMem.close(false);
                }
            } finally {
                try {
                    Misc.free(sealValueMem);
                    Misc.free(stagingValueMem);
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
                    deferMetadataPublish = false;
                    tentativeSlotOffset = -1L;
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

    /**
     * Configure covering with column names. The writer opens its own RO mmaps
     * of the column files during seal using partitionPath + names + txns.
     */
    public void configureCovering(
            ObjList<CharSequence> coveredColumnNames,
            LongList coveredColumnNameTxns,
            LongList coveredColumnTops,
            IntList coveredColumnShifts,
            IntList coveredColumnIndices,
            IntList coveredColumnTypes,
            int timestampColumnIndex
    ) {
        unmapCoveredColumnReads();
        this.coveredPartitionPath.clear();
        this.coveredPartitionPath.put(partitionPath);
        this.coveredColumnNames.clear();
        this.coveredColumnNameTxns.clear();
        this.coveredColumnAddrs.clear();
        this.coveredColumnTops.clear();
        this.coveredColumnShifts.clear();
        this.coveredColumnIndices.clear();
        this.coveredColumnTypes.clear();
        this.coveredColumnNames.addAll(coveredColumnNames);
        this.coveredColumnNameTxns.addAll(coveredColumnNameTxns);
        this.coveredColumnTops.addAll(coveredColumnTops);
        this.coveredColumnShifts.addAll(coveredColumnShifts);
        this.coveredColumnIndices.addAll(coveredColumnIndices);
        this.coveredColumnTypes.addAll(coveredColumnTypes);
        this.coverCount = coveredColumnNames.size();
        this.timestampColumnIndex = timestampColumnIndex;
    }

    /**
     * Configure covering with pre-mapped addresses. Used by O3 where column
     * data lives in native memory buffers, not files on disk. The caller
     * owns the mmaps and is responsible for unmapping them.
     */
    public void configureCovering(
            LongList coveredColumnAddrs,
            LongList coveredColumnTops,
            IntList coveredColumnShifts,
            IntList coveredColumnIndices,
            IntList coveredColumnTypes,
            int coverCount
    ) {
        unmapCoveredColumnReads();
        this.coveredPartitionPath.clear();
        this.coveredColumnNames.clear();
        this.coveredColumnNameTxns.clear();
        this.coveredColumnAddrs.clear();
        this.coveredColumnTops.clear();
        this.coveredColumnShifts.clear();
        this.coveredColumnIndices.clear();
        this.coveredColumnTypes.clear();
        for (int i = 0; i < coverCount; i++) {
            this.coveredColumnAddrs.add(coveredColumnAddrs.getQuick(i));
            this.coveredColumnTops.add(coveredColumnTops.getQuick(i));
            this.coveredColumnShifts.add(coveredColumnShifts.getQuick(i));
            this.coveredColumnIndices.add(coveredColumnIndices.getQuick(i));
            this.coveredColumnTypes.add(coveredColumnTypes.getQuick(i));
            this.coveredColumnNameTxns.add(COLUMN_NAME_TXN_NONE);
        }
        this.coverCount = coverCount;
        this.timestampColumnIndex = -1;
    }

    @TestOnly
    public void configureCovering(
            long[] coveredColumnAddrs,
            long[] coveredColumnTops,
            int[] coveredColumnShifts,
            int[] coveredColumnIndices,
            int[] coveredColumnTypes,
            int coverCount
    ) {
        LongList addrs = new LongList(coverCount);
        LongList tops = new LongList(coverCount);
        IntList shifts = new IntList(coverCount);
        IntList indices = new IntList(coverCount);
        IntList types = new IntList(coverCount);
        for (int i = 0; i < coverCount; i++) {
            addrs.add(coveredColumnAddrs[i]);
            tops.add(coveredColumnTops[i]);
            shifts.add(coveredColumnShifts[i]);
            indices.add(coveredColumnIndices[i]);
            types.add(coveredColumnTypes[i]);
        }
        configureCovering(addrs, tops, shifts, indices, types, coverCount);
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
                count = Unsafe.getInt(countsBase + (long) idx * Integer.BYTES);
                long dataOffset = Unsafe.getLong(offsetsBase + (long) idx * Long.BYTES);
                encodedAddr = genAddr + headerSize + dataOffset;
            } else {
                // Dense format — stride-indexed (supports delta and flat modes)
                if (key >= genKeyCount) continue;

                int stride = key / PostingIndexUtils.DENSE_STRIDE;
                int localKey = key % PostingIndexUtils.DENSE_STRIDE;
                int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                long strideOff = Unsafe.getLong(genAddr + (long) stride * Long.BYTES);
                long strideAddr = genAddr + siSize + strideOff;
                int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
                byte mode = Unsafe.getByte(strideAddr);

                if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                    // Flat mode
                    int bitWidth = Unsafe.getByte(strideAddr + 1) & 0xFF;
                    long baseValue = Unsafe.getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                    long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                    int startIdx = Unsafe.getInt(prefixAddr + (long) localKey * Integer.BYTES);
                    count = Unsafe.getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES) - startIdx;
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
                count = Unsafe.getInt(countsBase + (long) localKey * Integer.BYTES);
                long offsetsBase = countsBase + (long) ks * Integer.BYTES;
                long dataOffset = Unsafe.getLong(offsetsBase + (long) localKey * Long.BYTES);
                int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
                encodedAddr = strideAddr + deltaHeaderSize + dataOffset;
            }

            if (count == 0) continue;
            long[] decoded = new long[count];
            PostingIndexUtils.decodeKey(encodedAddr, decoded);
            for (int i = 0; i < count; i++) {
                values.add(decoded[i]);
            }
        }

        return new TestFwdCursor(values);
    }

    @TestOnly
    public int getGenCount() {
        return genCount;
    }

    @Override
    public byte getIndexType() {
        return IndexType.POSTING;
    }

    @Override
    public int getKeyCount() {
        return keyCount;
    }

    @Override
    public long getMaxValue() {
        return keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE);
    }

    /**
     * Number of pending purge entries that have not yet been forwarded to
     * the global PostingSealPurge queue. Tests use this to verify that the
     * outbox is bounded under saturation.
     */
    @TestOnly
    public int getPendingPurgesSizeForTesting() {
        return pendingPurges.size();
    }

    @Override
    public boolean isOpen() {
        return keyMem.isOpen();
    }

    /**
     * Folds any tentative state left on the inactive metadata page by an
     * fd-based O3 writer into this writer's in-memory view. Must be called
     * after {@code of()} on the seal path and before {@code seal()} or
     * {@code rebuildSidecars()}. No-op if the partition wasn't touched by
     * the current O3. Leaves {@code sealTxn} at the committed value so seal
     * derives the new sealTxn correctly.
     */
    public void mergeTentativeIntoActiveIfAny() {
        if (!keyMem.isOpen()) {
            return;
        }
        long inactivePageOffset = (activePageOffset == PostingIndexUtils.PAGE_A_OFFSET)
                ? PostingIndexUtils.PAGE_B_OFFSET
                : PostingIndexUtils.PAGE_A_OFFSET;
        if (inactivePageOffset == PostingIndexUtils.PAGE_B_OFFSET
                && keyMem.size() < PostingIndexUtils.KEY_FILE_RESERVED) {
            return;
        }
        long inactiveSeqStart = keyMem.getLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        long inactiveSeqEnd = keyMem.getLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);
        long inactiveSealTxn = keyMem.getLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN);
        long activeSeq = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        if (inactiveSealTxn != PostingIndexUtils.SEAL_TXN_TENTATIVE
                || inactiveSeqStart != inactiveSeqEnd
                || inactiveSeqStart <= activeSeq) {
            return;
        }
        activePageOffset = inactivePageOffset;
        keyCount = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT);
        genCount = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);
        valueMemSize = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE);
        if (valueMem.isOpen() && valueMemSize > 0) {
            valueMem.extend(valueMemSize);
            valueMem.jumpTo(valueMemSize);
        }
    }

    @Override
    public void of(Path path, CharSequence name, long columnNameTxn) {
        of(path, name, columnNameTxn, false);
    }

    @Override
    public void of(Path path, CharSequence name, long columnNameTxn, long partitionTimestamp, long partitionNameTxn) {
        this.partitionTimestamp = partitionTimestamp;
        this.partitionNameTxn = partitionNameTxn;
        of(path, name, columnNameTxn, false);
    }

    /**
     * FD-based open — used by O3CopyJob. The caller never calls configureCovering(),
     * so coverCount stays 0 and sidecar operations are skipped. The postingColumnNameTxn
     * field is not set here (stays 0) which is safe because no sidecar code path runs
     * without coverCount &gt; 0. Writes after this open are tagged SEAL_TXN_TENTATIVE and
     * must be promoted by a later path-based seal via mergeTentativeIntoActiveIfAny.
     */
    @Override
    public void of(CairoConfiguration configuration, long keyFd, long valueFd, boolean init, int blockCapacity) {
        close();
        this.deferMetadataPublish = true;
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
                this.sealTxn = 0;
            } else {
                determineActivePageOffset();

                int version = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_FORMAT_VERSION);
                if (version != FORMAT_VERSION) {
                    throw CairoException.critical(0)
                            .put("Unsupported Posting index version [fd=").put(keyMem.getFd())
                            .put(", expected=").put(FORMAT_VERSION)
                            .put(", actual=").put(version).put(']');
                }
                this.sealTxn = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN);

                // Wipe any abandoned tentative state on the inactive page.
                // Why: an O3 commit that failed mid-flight leaves fd-based
                // tentative metadata on the inactive page (TableWriter.rollback
                // doesn't iterate O3Basket writers). The next attempt's
                // fd-based open would compute newSeq = activeSeq + 1, which
                // matches the abandoned tentative's seq because the active
                // (committed) page is unchanged across attempts. A partial
                // metadata-page write in the new attempt could then leave
                // start==end with mixed fields, looking consistent but
                // referencing .pv offsets the new attempt has overwritten.
                // Zero the inactive slot so any subsequent partial write
                // remains torn, and a new attempt that doesn't reach
                // writeMetadataPage at all leaves the inactive page
                // non-tentative (mergeTentativeIntoActiveIfAny skips it).
                invalidateStaleTentative();
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

    public void of(Path path, CharSequence name, long columnNameTxn, boolean init) {
        // close() releases resources but does NOT flush pending add() calls,
        // so the caller must have already committed or sealed. On the
        // TableWriter path this is guaranteed by commit() in switchPartition
        // and syncColumns.
        close();
        this.deferMetadataPublish = false;
        partitionPath.clear();
        partitionPath.put(path);
        indexName.clear();
        indexName.put(name);
        // postingColumnNameTxn is the stable column-instance txn; sealTxn
        // is a seal-count starting at 0 on fresh init, picked up from .pk on reopen.
        this.postingColumnNameTxn = columnNameTxn;
        this.sealTxn = 0;
        final int plen = path.size();
        boolean kFdUnassigned = true;

        try {
            LPSZ keyFile = PostingIndexUtils.keyFileName(path, name, columnNameTxn);

            if (init) {
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                this.blockCapacity = BLOCK_CAPACITY;
                initKeyMemory(keyMem, blockCapacity);
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

                // Map the whole file so both metadata pages (A and B) are visible.
                // Passing -1 would fall back to minMappedMemorySize (one PAGE_SIZE),
                // which leaves page B unmapped and silently drops any tentative
                // metadata an fd-based O3 writer parked on it.
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), keyFileSize, MemoryTag.MMAP_INDEX_WRITER);
            }
            kFdUnassigned = false;

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

            if (!init) {
                this.sealTxn = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN);
            }

            valueMem.of(
                    ff,
                    PostingIndexUtils.valueFileName(path.trimTo(plen), name, postingColumnNameTxn, this.sealTxn),
                    configuration.getDataIndexValueAppendPageSize(),
                    init ? 0 : valueMemSize,
                    MemoryTag.MMAP_INDEX_WRITER
            );

            if (!init && valueMemSize > 0) {
                valueMem.jumpTo(valueMemSize);
            }

            if (!init && !Utf8s.equals(path, lastOrphanScanPath)) {
                logOrphanSealedFiles(path.trimTo(plen), name);
                lastOrphanScanPath.clear();
                lastOrphanScanPath.put(path);
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

    /**
     * Publishes every accumulated {@link PendingSealPurge} onto the global
     * {@code PostingSealPurgeQueue} so the background {@code PostingSealPurgeJob}
     * can persist and eventually delete them under the
     * {@link TxnScoreboard}'s supervision.
     * <p>
     * The writer itself does no file deletion and no scoreboard check — that
     * is the job's responsibility. This decouples commit latency from purge
     * work and lets the persistent {@code sys.posting_seal_purge_log} survive
     * a writer-process crash.
     * <p>
     * Entries that cannot be published (queue full) stay in the outbox and
     * retry on the next commit.
     */
    public void publishPendingPurges(
            MessageBus messageBus,
            TableToken tableToken,
            int partitionBy,
            int timestampType,
            long currentTableTxn
    ) {
        if (pendingPurges.size() == 0 || partitionPath.size() == 0 || tableToken == null || messageBus == null) {
            return;
        }
        Sequence pubSeq = messageBus.getPostingSealPurgePubSeq();
        RingQueue<PostingSealPurgeTask> queue = messageBus.getPostingSealPurgeQueue();
        int writePos = 0;
        for (int readPos = 0, n = pendingPurges.size(); readPos < n; readPos++) {
            PendingSealPurge entry = pendingPurges.getQuick(readPos);
            if (entry.partitionTimestamp == Long.MIN_VALUE) {
                pendingPurges.setQuick(writePos++, entry);
                continue;
            }
            long cursor = pubSeq.next();
            if (cursor < 0) {
                // Queue full or contended — keep entry for retry on next commit.
                pendingPurges.setQuick(writePos++, entry);
                continue;
            }
            long toTxn = entry.toTableTxn == Long.MAX_VALUE ? currentTableTxn : entry.toTableTxn;
            try {
                PostingSealPurgeTask task = queue.get(cursor);
                task.of(
                        tableToken,
                        indexName,
                        entry.postingColumnNameTxn,
                        entry.sealTxn,
                        entry.partitionTimestamp,
                        entry.partitionNameTxn,
                        partitionBy,
                        timestampType,
                        entry.fromTableTxn,
                        toTxn
                );
            } finally {
                pubSeq.done(cursor);
            }
            entry.of(0L, 0L, 0L, 0L, Long.MIN_VALUE, -1L);
            pendingPurgePool.add(entry);
        }
        for (int i = pendingPurges.size() - 1; i >= writePos; i--) {
            pendingPurges.remove(i);
        }
    }

    /**
     * Rebuilds covering sidecar files for an already-sealed posting index.
     * Called after O3 merge, which rebuilds the posting index but doesn't
     * write sidecar files (the O3 pool writer has no covering configuration).
     */
    public void rebuildSidecars() {
        if (coverCount <= 0 || genCount == 0 || keyCount == 0) {
            return;
        }
        // Open new sidecar files at a fresh sealTxn so the previous-sealTxn
        // files on disk are never overwritten. The same sealTxn is reused for
        // both the .pv and every .pc<N> the seal produces.
        if (sidecarMems.size() > 0) {
            closeSidecarMems();
        }
        long gen0DirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, 0);
        int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
        if (gen0KeyCount >= 0) {
            // Already dense — just rewrite sidecars at a new sealTxn.
            final long oldSealTxn = sealTxn;
            final long newSealTxn = Math.max(1, sealTxn + 1);
            if (partitionPath.size() > 0) {
                openSidecarFiles(Path.getThreadLocal(partitionPath), indexName, postingColumnNameTxn, newSealTxn);
            }
            try {
                sealFull(newSealTxn);
            } finally {
                if (sealTxn != oldSealTxn) {
                    recordPostingSealPurge(oldSealTxn);
                }
            }
        } else {
            // Sparse — full seal() handles its own sealTxn allocation and
            // sidecar opening at newSealTxn.
            seal();
        }
    }

    @Override
    public void rollbackConditionally(long row) {
        if (row < 0) {
            return;
        }
        if (row == 0) {
            if (genCount > 0 || hasPendingData) {
                truncate();
            }
            return;
        }
        // row > 0: caller is about to append at row, so any existing rowid >= row
        // must be discarded. Mirrors BitmapIndexWriter.rollbackConditionally. An O3
        // split can shrink a partition (rows move into a new split sub-partition)
        // without resealing the parent's index, leaving rowids beyond the new size
        // in the parent's posting index. The next append (usually from squash) must
        // evict them before writing fresh entries at the same rowids.
        if (genCount == 0 && !hasPendingData) {
            return;
        }
        if (getMaxValue() < row) {
            return;
        }
        rollbackValues(row - 1);
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

    public void seal() {
        if (!keyMem.isOpen()) {
            return;
        }
        flushAllPending();
        // Free spill and pending allocations — their data has been encoded
        // into valueMem by flushAllPending(), so holding them during the
        // memory-heavy reencodeAllGenerations() phase is pure waste.
        // With many symbol keys (e.g. 11M), these buffers alone can total
        // several GB. The next add() call will reallocate if needed.
        freeSpillData();
        freePendingBuffers();

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

        final long oldSealTxn = sealTxn;
        final long newSealTxn = Math.max(1, sealTxn + 1);

        boolean haveSavedSidecars = false;
        if (coverCount > 0 && partitionPath.size() > 0) {
            if (savedSidecarBufs == null || savedSidecarBufs.length < coverCount) {
                savedSidecarBufs = new long[coverCount];
                savedSidecarSizes = new long[coverCount];
            } else {
                Arrays.fill(savedSidecarBufs, 0, savedSidecarBufs.length, 0L);
                Arrays.fill(savedSidecarSizes, 0, savedSidecarSizes.length, 0L);
            }
            haveSavedSidecars = true;
            Path p = Path.getThreadLocal(partitionPath);
            int pp = p.size();
            for (int c = 0; c < coverCount; c++) {
                if (coveredColumnIndices.getQuick(c) < 0) {
                    continue;
                }
                long covT = getCoveredColumnNameTxn(c);
                LPSZ pcFile = PostingIndexUtils.coverDataFileName(p.trimTo(pp), indexName, c, postingColumnNameTxn, covT, sealTxn);
                if (ff.exists(pcFile)) {
                    long fileLen = ff.length(pcFile);
                    if (fileLen > 0) {
                        long fd = ff.openRO(pcFile);
                        if (fd >= 0) {
                            try {
                                long mapped = ff.mmap(fd, fileLen, 0, Files.MAP_RO, MemoryTag.MMAP_INDEX_WRITER);
                                if (mapped > 0) {
                                    try {
                                        savedSidecarBufs[c] = Unsafe.malloc(fileLen, MemoryTag.NATIVE_INDEX_READER);
                                        savedSidecarSizes[c] = fileLen;
                                        Unsafe.copyMemory(mapped, savedSidecarBufs[c], fileLen);
                                    } finally {
                                        ff.munmap(mapped, fileLen, MemoryTag.MMAP_INDEX_WRITER);
                                    }
                                }
                            } finally {
                                ff.close(fd);
                            }
                        }
                    }
                }
                p.trimTo(pp);
            }
            if (sidecarMems.size() > 0) {
                closeSidecarMems();
            }
        }

        try {
            // Open new sidecar files at newSealTxn. The files do not exist on
            // disk yet, so the open creates them fresh — no truncation, no
            // racing with readers holding the previous-sealTxn files.
            if (coverCount > 0 && sidecarMems.size() == 0 && partitionPath.size() > 0) {
                openSidecarFiles(Path.getThreadLocal(partitionPath), indexName, postingColumnNameTxn, newSealTxn);
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

            // sealIncremental writes via sealValueMem (a fresh .pv file at
            // newSealTxn) and is path-based-only. fd-based seal (e.g. inline
            // at MAX_GEN_COUNT during O3) must take sealFull, whose chunked
            // path now handles fd-based via end-of-valueMem write + memmove
            // (see openSealValueFile / reencodeWithStrideDecoding).
            if (isIncrementalCandidate && gen0KeyCount == keyCount && partitionPath.size() > 0) {
                sealIncremental(newSealTxn, savedSidecarBufs, savedSidecarSizes);
                // ownership of inner buffers transferred to sealIncremental;
                // zero entries so the finally cleanup skips them.
                if (haveSavedSidecars) {
                    for (int c = 0; c < coverCount; c++) {
                        savedSidecarBufs[c] = 0;
                        savedSidecarSizes[c] = 0;
                    }
                }
            } else {
                sealFull(newSealTxn);
            }
        } finally {
            if (haveSavedSidecars) {
                for (int c = 0; c < coverCount; c++) {
                    if (savedSidecarBufs[c] != 0) {
                        Unsafe.free(savedSidecarBufs[c], savedSidecarSizes[c], MemoryTag.NATIVE_INDEX_READER);
                        savedSidecarBufs[c] = 0;
                        savedSidecarSizes[c] = 0;
                    }
                }
            }
            if (sealTxn != oldSealTxn) {
                recordPostingSealPurge(oldSealTxn);
            }
        }
    }

    @Override
    public void sealIfMultiGen(int threshold) {
        if (keyMem.isOpen() && partitionPath.size() > 0 && genCount > threshold) {
            seal();
        }
    }

    public void setCoveredColumnNameTxns(LongList txns) {
        coveredColumnNameTxns.clear();
        coveredColumnNameTxns.addAll(txns);
    }

    @Override
    public void setMaxValue(long maxValue) {
        // In tentative (fd-based O3) mode the active page is the committed
        // page; writing maxValue there would persist a value past any rows
        // not yet published. A subsequent failed/aborted attempt would leave
        // the committed metadata corrupted, causing the next open to read a
        // maxValue past the live row count and either skip a needed rollback
        // or roll back valid committed entries. Stage the value on the
        // tentative slot the same way writeMetadataPage does so committed
        // page A stays untouched until a successful seal promotes the slot.
        // Published to readers via writeMetadataPage on next commit/seal.
        long writeOffset;
        if (deferMetadataPublish) {
            if (tentativeSlotOffset == -1L) {
                tentativeSlotOffset = (activePageOffset == PostingIndexUtils.PAGE_A_OFFSET)
                        ? PostingIndexUtils.PAGE_B_OFFSET
                        : PostingIndexUtils.PAGE_A_OFFSET;
            }
            writeOffset = tentativeSlotOffset;
        } else {
            writeOffset = activePageOffset;
        }
        keyMem.putLong(writeOffset + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE, maxValue);
    }

    @Override
    public void setPendingPublishTableTxn(long publishTableTxn) {
        this.pendingPublishTableTxn = publishTableTxn;
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
    public void tombstoneCover(int writerIdx) {
        int targetC = -1;
        for (int c = 0, n = coveredColumnIndices.size(); c < n; c++) {
            if (coveredColumnIndices.getQuick(c) == writerIdx) {
                targetC = c;
                break;
            }
        }
        if (targetC < 0) {
            return;
        }
        coveredColumnIndices.setQuick(targetC, -1);
        if (targetC < sidecarMems.size()) {
            MemoryMARW mem = sidecarMems.getQuick(targetC);
            if (mem != null) {
                Misc.free(mem);
                sidecarMems.setQuick(targetC, null);
            }
        }
        if (sidecarInfoMem != null) {
            sidecarInfoMem.putInt(8L + (long) targetC * Integer.BYTES, -1);
        }
    }

    @Override
    public void truncate() {
        freeNativeBuffers();
        if (partitionPath.size() > 0) {
            // Create a new empty .pv file. The old .pv stays on disk so
            // concurrent readers with active mmaps don't SIGBUS.
            // If the new file open fails, valueMem is left closed — the writer
            // is degraded but safe to close() without further I/O errors.
            final long oldSealTxn = sealTxn;
            final long newTxn = Math.max(1, sealTxn + 1);
            // Drop sidecar mappings tied to oldSealTxn — the next seal will
            // re-open them at the new sealTxn.
            closeSidecarMems();
            valueMem.close(false, (byte) 0);
            Path p = Path.getThreadLocal(partitionPath);
            LPSZ fileName = PostingIndexUtils.valueFileName(p, indexName, postingColumnNameTxn, newTxn);
            valueMem.of(ff, fileName,
                    configuration.getDataIndexValueAppendPageSize(), 0L,
                    MemoryTag.MMAP_INDEX_WRITER, configuration.getWriterFileOpenOpts(), -1);
            sealTxn = newTxn;
            initKeyMemory(keyMem, blockCapacity, sealTxn);
            recordPostingSealPurge(oldSealTxn);
        } else {
            // fd-based writer (O3 path): no concurrent readers, safe to truncate in place
            valueMem.truncate();
            initKeyMemory(keyMem, blockCapacity);
        }
        activePageOffset = PostingIndexUtils.PAGE_A_OFFSET;
        keyCount = 0;
        valueMemSize = 0;
        genCount = 0;
        hasPendingData = false;
        activeKeyCount = 0;
        allocateNativeBuffers();
    }

    private static int compressSidecarBlock(long rawBuf, int valueCount, int shift, int colType,
                                            boolean isDesignatedTs,
                                            long destBuf, long longWorkspaceAddr, long exceptionWorkspaceAddr) {
        if (isDesignatedTs) {
            // Designated timestamp: non-null, monotonically increasing per key.
            // Linear-prediction FoR gives O(1) random access with same compression as delta.
            return CoveringCompressor.compressLongsLinearPred(rawBuf, valueCount, destBuf, longWorkspaceAddr);
        }
        return switch (ColumnType.tagOf(colType)) {
            case ColumnType.DOUBLE -> {
                int alpSize = CoveringCompressor.compressDoubles(rawBuf, valueCount, 3, destBuf, longWorkspaceAddr, exceptionWorkspaceAddr);
                int rawSize = 4 + valueCount * Double.BYTES;
                if (alpSize <= rawSize) {
                    yield alpSize;
                }
                Unsafe.putInt(destBuf, valueCount | CoveringCompressor.RAW_BLOCK_FLAG);
                Unsafe.copyMemory(rawBuf, destBuf + 4, (long) valueCount * Double.BYTES);
                yield rawSize;
            }
            case ColumnType.FLOAT -> {
                int alpSize = CoveringCompressor.compressFloats(rawBuf, valueCount, destBuf, longWorkspaceAddr, exceptionWorkspaceAddr);
                int rawSize = 4 + valueCount * Float.BYTES;
                if (alpSize <= rawSize) {
                    yield alpSize;
                }
                Unsafe.putInt(destBuf, valueCount | CoveringCompressor.RAW_BLOCK_FLAG);
                Unsafe.copyMemory(rawBuf, destBuf + 4, (long) valueCount * Float.BYTES);
                yield rawSize;
            }
            case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG, ColumnType.DECIMAL64 ->
                    CoveringCompressor.compressLongs(rawBuf, valueCount, destBuf);
            case ColumnType.GEOINT, ColumnType.INT, ColumnType.IPv4, ColumnType.SYMBOL,
                 ColumnType.DECIMAL32 ->
                    CoveringCompressor.compressInts(rawBuf, valueCount, destBuf, longWorkspaceAddr);
            case ColumnType.CHAR, ColumnType.SHORT, ColumnType.GEOSHORT, ColumnType.DECIMAL16 ->
                    CoveringCompressor.compressShorts(rawBuf, valueCount, destBuf, longWorkspaceAddr);
            case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE, ColumnType.DECIMAL8 ->
                    CoveringCompressor.compressBytes(rawBuf, valueCount, destBuf, longWorkspaceAddr);
            default -> {
                // Raw copy for remaining fixed-width types: UUID, LONG256, DECIMAL128/256
                Unsafe.putInt(destBuf, valueCount);
                Unsafe.copyMemory(rawBuf, destBuf + 4, (long) valueCount << shift);
                yield 4 + (valueCount << shift);
            }
        };
    }

    private static void initKeyMemory(MemoryMA keyMem, int blockCapacity, long valueFileTxn) {
        keyMem.jumpTo(0);
        keyMem.truncate();
        keyMem.putLong(1L);
        keyMem.putLong(0L);
        keyMem.putInt(blockCapacity);
        keyMem.putInt(0);
        keyMem.putLong(-1L);
        keyMem.putInt(0);
        keyMem.putInt(FORMAT_VERSION);
        keyMem.putLong(valueFileTxn);
        int padLongs = (PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END
                - PostingIndexUtils.PAGE_OFFSET_SEAL_TXN - Long.BYTES) / Long.BYTES;
        for (int i = 0; i < padLongs; i++) {
            keyMem.putLong(0L);
        }
        Unsafe.storeFence();
        keyMem.putLong(1L);

        for (int i = 0; i < PostingIndexUtils.PAGE_SIZE / Long.BYTES; i++) {
            keyMem.putLong(0L);
        }
    }

    private static void putFixedValue(MemoryMARW mem, long addr, int valueSize) {
        if (valueSize == Long.BYTES) {
            mem.putLong(Unsafe.getLong(addr));
        } else if (valueSize == Integer.BYTES) {
            mem.putInt(Unsafe.getInt(addr));
        } else if (valueSize == Short.BYTES) {
            mem.putShort(Unsafe.getShort(addr));
        } else if (valueSize == Byte.BYTES) {
            mem.putByte(Unsafe.getByte(addr));
        } else {
            // Multi-long types: UUID (16B), LONG256 (32B), DECIMAL128 (16B), DECIMAL256 (32B)
            mem.putBlockOfBytes(addr, valueSize);
        }
    }

    private static void writeNullSentinel(long addr, int valueSize, int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DOUBLE -> {
                Unsafe.putDouble(addr, Double.NaN);
                return;
            }
            case ColumnType.FLOAT -> {
                Unsafe.putFloat(addr, Float.NaN);
                return;
            }
            case ColumnType.GEOBYTE -> {
                Unsafe.putByte(addr, GeoHashes.BYTE_NULL);
                return;
            }
            case ColumnType.GEOSHORT -> {
                Unsafe.putShort(addr, GeoHashes.SHORT_NULL);
                return;
            }
            case ColumnType.GEOINT -> {
                Unsafe.putInt(addr, GeoHashes.INT_NULL);
                return;
            }
            case ColumnType.IPv4 -> {
                Unsafe.putInt(addr, Numbers.IPv4_NULL);
                return;
            }
            case ColumnType.GEOLONG -> {
                Unsafe.putLong(addr, GeoHashes.NULL);
                return;
            }
            default -> {
            }
        }
        // Generic null sentinel by size for types not handled by the switch above
        Unsafe.setMemory(addr, valueSize, (byte) 0);
        // Overlay Long.MIN_VALUE for each 8-byte slot (standard QuestDB null sentinel)
        for (int off = 0; off + Long.BYTES <= valueSize; off += Long.BYTES) {
            Unsafe.putLong(addr + off, Long.MIN_VALUE);
        }
    }

    private static void writeNullSentinel(MemoryMARW mem, int valueSize, int colType) {
        switch (ColumnType.tagOf(colType)) {
            case ColumnType.DOUBLE -> mem.putLong(Double.doubleToLongBits(Double.NaN));
            case ColumnType.FLOAT -> mem.putInt(Float.floatToIntBits(Float.NaN));
            case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.DECIMAL64 ->
                    mem.putLong(Numbers.LONG_NULL);
            case ColumnType.GEOLONG -> mem.putLong(GeoHashes.NULL);
            case ColumnType.INT, ColumnType.SYMBOL, ColumnType.DECIMAL32 -> mem.putInt(Numbers.INT_NULL);
            case ColumnType.IPv4 -> mem.putInt(Numbers.IPv4_NULL);
            case ColumnType.GEOINT -> mem.putInt(GeoHashes.INT_NULL);
            case ColumnType.CHAR, ColumnType.SHORT, ColumnType.DECIMAL16 -> mem.putShort((short) 0);
            case ColumnType.GEOSHORT -> mem.putShort(GeoHashes.SHORT_NULL);
            case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.DECIMAL8 -> mem.putByte((byte) 0);
            case ColumnType.GEOBYTE -> mem.putByte(GeoHashes.BYTE_NULL);
            case ColumnType.UUID, ColumnType.DECIMAL128 -> {
                mem.putLong(Numbers.LONG_NULL);
                mem.putLong(Numbers.LONG_NULL);
            }
            case ColumnType.LONG256, ColumnType.DECIMAL256 -> {
                for (int i = 0; i < 4; i++) mem.putLong(Numbers.LONG_NULL);
            }
            default -> {
                // Generic fallback: write zero bytes for the value size
                for (int i = 0; i < valueSize; i++) mem.putByte((byte) 0);
            }
        }
    }

    private static void writeVarOffset(MemoryMARW mem, long offsetsStart, int ordinal, long value, boolean longOffsets) {
        if (longOffsets) {
            mem.putLong(offsetsStart + (long) ordinal * Long.BYTES, value);
        } else {
            mem.putInt(offsetsStart + (long) ordinal * Integer.BYTES, (int) value);
        }
    }

    private void allocateNativeBuffers() {
        keyCapacity = Math.max(INITIAL_KEY_CAPACITY, keyCount);
        long valBufSize = (long) keyCapacity * PENDING_SLOT_CAPACITY * Long.BYTES;
        long countBufSize = (long) keyCapacity * Integer.BYTES;

        pendingValuesAddr = Unsafe.malloc(valBufSize, MemoryTag.NATIVE_INDEX_READER);
        Unsafe.setMemory(pendingValuesAddr, valBufSize, (byte) 0);

        pendingCountsAddr = Unsafe.malloc(countBufSize, MemoryTag.NATIVE_INDEX_READER);
        Unsafe.setMemory(pendingCountsAddr, countBufSize, (byte) 0);

        activeKeyIds = new int[keyCapacity];
    }

    private void closeSidecarMems() {
        Misc.freeObjListAndClear(sidecarMems);
        sidecarInfoMem = Misc.free(sidecarInfoMem);
    }

    private void copyStrideFromGen0(long gen0Addr, int gen0KeyCount, int gen0SiSize, int stride,
                                    long copyBuf, long copyBufSize) {
        // If this stride existed in gen 0, copy it; otherwise write empty
        if (stride >= PostingIndexUtils.strideCount(gen0KeyCount)) {
            // Stride didn't exist in gen 0 — write empty delta stride
            int ks = PostingIndexUtils.keysInStride(keyCount, stride);
            int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
            long headerFilePos = sealTarget.getAppendOffset();
            for (int i = 0; i < deltaHeaderSize; i += Integer.BYTES) {
                sealTarget.putInt(0);
            }
            // Zero header = delta mode, all counts 0, all offsets 0
            long headerAddr = sealTarget.addressOf(headerFilePos);
            Unsafe.setMemory(headerAddr, deltaHeaderSize, (byte) 0);
            return;
        }

        long strideOff = Unsafe.getLong(gen0Addr + (long) stride * Long.BYTES);
        long nextStrideOff;
        if (stride + 1 < PostingIndexUtils.strideCount(gen0KeyCount)) {
            nextStrideOff = Unsafe.getLong(gen0Addr + (long) (stride + 1) * Long.BYTES);
        } else {
            // Last stride — get sentinel
            nextStrideOff = Unsafe.getLong(gen0Addr + (long) PostingIndexUtils.strideCount(gen0KeyCount) * Long.BYTES);
        }
        long strideSize = nextStrideOff - strideOff;
        if (strideSize <= 0) {
            return;
        }

        long srcAddr = gen0Addr + gen0SiSize + strideOff;
        // Copy via temp buffer because srcAddr may point into valueMem's mapping.
        if (strideSize <= copyBufSize) {
            Unsafe.copyMemory(srcAddr, copyBuf, strideSize);
            sealTarget.putBlockOfBytes(copyBuf, strideSize);
        } else {
            long tmpBuf = Unsafe.malloc(strideSize, MemoryTag.NATIVE_INDEX_READER);
            try {
                Unsafe.copyMemory(srcAddr, tmpBuf, strideSize);
                sealTarget.putBlockOfBytes(tmpBuf, strideSize);
            } finally {
                Unsafe.free(tmpBuf, strideSize, MemoryTag.NATIVE_INDEX_READER);
            }
        }
    }

    /**
     * Decode a single stride's keys from a dense generation. The dense gen
     * is stride-indexed, so we read stride s directly. genKs may differ from
     * ks if the gen has fewer keys.
     */
    private void decodeDenseGenStride(
            long genBase, int genKeyCount, int s, int genKs, int ks,
            long strideValsAddr, long[] keyOffsets
    ) {
        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
        long strideOff = Unsafe.getLong(genBase + (long) s * Long.BYTES);
        long strideAddr = genBase + siSize + strideOff;
        byte mode = Unsafe.getByte(strideAddr);

        // Only decode keys up to min(genKs, ks) — the gen may have fewer keys
        int decodeKs = Math.min(genKs, ks);

        if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
            int bitWidth = Unsafe.getByte(strideAddr + 1) & 0xFF;
            long baseValue = Unsafe.getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
            long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
            int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(genKs);
            long flatDataAddr = strideAddr + flatHeaderSize;

            for (int j = 0; j < decodeKs; j++) {
                int startIdx = Unsafe.getInt(prefixAddr + (long) j * Integer.BYTES);
                int count = Unsafe.getInt(prefixAddr + (long) (j + 1) * Integer.BYTES) - startIdx;
                if (count == 0) continue;

                long destAddr = strideValsAddr + keyOffsets[j] * Long.BYTES;
                BitpackUtils.unpackValuesFrom(flatDataAddr, startIdx, count, bitWidth, baseValue, destAddr);
                keyOffsets[j] += count;
            }
        } else {
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            long genOffsetsBase = countsAddr + (long) genKs * Integer.BYTES;
            int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(genKs);

            for (int j = 0; j < decodeKs; j++) {
                int count = Unsafe.getInt(countsAddr + (long) j * Integer.BYTES);
                if (count == 0) continue;

                long dataOff = Unsafe.getLong(genOffsetsBase + (long) j * Long.BYTES);
                long encodedAddr = strideAddr + deltaHeaderSize + dataOff;

                long destAddr = strideValsAddr + keyOffsets[j] * Long.BYTES;
                PostingIndexUtils.decodeKeyToNative(encodedAddr, destAddr, decodeCtx);
                keyOffsets[j] += count;
            }
        }
    }

    /**
     * Decode a single stride's keys from a sparse generation using binary
     * search on the sorted keyIds array. Advances keyOffsets[j] for each
     * decoded key.
     */
    private void decodeSparseGenStride(
            long genBase, int activeKeyCount,
            int strideStart, int ks,
            long strideValsAddr, long[] keyOffsets
    ) {
        long countsBase = genBase + (long) activeKeyCount * Integer.BYTES;
        long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
        int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);

        // Binary search for first keyId >= strideStart
        int lo = 0, hi = activeKeyCount - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int midKey = Unsafe.getInt(genBase + (long) mid * Integer.BYTES);
            if (midKey < strideStart) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        int strideEnd = strideStart + ks;
        for (int i = lo; i < activeKeyCount; i++) {
            int keyId = Unsafe.getInt(genBase + (long) i * Integer.BYTES);
            if (keyId >= strideEnd) break;

            int count = Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
            if (count == 0) continue;

            long dataOffset = Unsafe.getLong(offsetsBase + (long) i * Long.BYTES);
            long encodedAddr = genBase + headerSize + dataOffset;

            int j = keyId - strideStart;
            long destAddr = strideValsAddr + keyOffsets[j] * Long.BYTES;
            PostingIndexUtils.decodeKeyToNative(encodedAddr, destAddr, decodeCtx);
            keyOffsets[j] += count;
        }
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
        long sealTxnA = memSize >= PostingIndexUtils.PAGE_SIZE
                ? keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN) : 0;
        long seqB = memSize >= PostingIndexUtils.KEY_FILE_RESERVED
                ? keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START) : 0;
        long seqEndB = memSize >= PostingIndexUtils.KEY_FILE_RESERVED
                ? keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END) : 0;
        long sealTxnB = memSize >= PostingIndexUtils.KEY_FILE_RESERVED
                ? keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN) : 0;

        long validA = (seqA == seqEndA && sealTxnA >= 0) ? seqA : 0;
        long validB = (seqB == seqEndB && sealTxnB >= 0) ? seqB : 0;
        activePageOffset = (validB > validA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
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
                    mergedValuesAddr + cumOffset * Long.BYTES);
            keyCounts[j] = mergedCount;
            cumOffset += mergedCount;
        }

        // Trial delta encode from the pre-merged buffer (encode directly from native memory)
        int bpDataTotal = 0;
        for (int j = 0; j < ks; j++) {
            int count = keyCounts[j];
            if (count > 0) {
                long keyAddr = mergedValuesAddr + keyOffsets[j] * Long.BYTES;
                encodeCtx.ensureCapacity(count);
                bpKeySizes[j] = PostingIndexUtils.encodeKeyNative(keyAddr, count, bpTrialBuf + bpDataTotal, encodeCtx, rowIdEncoding);
            } else {
                bpKeySizes[j] = 0;
            }
            bpDataTotal += bpKeySizes[j];
        }

        // Compute per-stride base value (min across all values in stride)
        if (cumOffset > Integer.MAX_VALUE) {
            // Too many values in a single stride for flat mode — force delta mode
            int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
            writeDeltaStride(ks, keyCounts, deltaHeaderSize, bpTrialBuf, bpKeySizes, localHeaderBuf);
            return;
        }
        int totalStrideValues = (int) cumOffset;
        long strideMinValue = Long.MAX_VALUE;
        long strideMaxValue = Long.MIN_VALUE;
        for (int i = 0; i < totalStrideValues; i++) {
            long val = Unsafe.getLong(mergedValuesAddr + (long) i * Long.BYTES);
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
            writeDeltaStride(ks, keyCounts, deltaHeaderSize, bpTrialBuf, bpKeySizes, localHeaderBuf);
        }
    }

    /**
     * Lazily creates read-only mmaps of the covered column files.
     * MemoryPMARImpl (the writer's column memory) maps one page at a time,
     * so addressOf() fails for random reads. These are separate RO mmaps
     * via the mmap cache, providing O(1) access to any offset.
     */
    private void ensureCoveredColumnReadMaps() {
        if (coveredColReadAddrs != null) {
            return;
        }
        coveredColReadAddrs = new long[coverCount];
        coveredColReadSizes = new long[coverCount];
        coveredAuxReadAddrs = new long[coverCount];
        coveredAuxReadSizes = new long[coverCount];

        if (coveredColumnNames.size() > 0 && coveredPartitionPath.size() > 0) {
            // Name-based: open files ourselves via mmap cache
            Path p = Path.getThreadLocal(coveredPartitionPath);
            for (int c = 0; c < coverCount; c++) {
                if (coveredColumnIndices.getQuick(c) < 0) {
                    continue;
                }
                mapColumnFile(p, coveredColumnNames.getQuick(c), coveredColumnNameTxns.getQuick(c),
                        coveredColReadAddrs, coveredColReadSizes, c, false);
                if (ColumnType.isVarSize(coveredColumnTypes.getQuick(c))) {
                    mapColumnFile(p, coveredColumnNames.getQuick(c), coveredColumnNameTxns.getQuick(c),
                            coveredAuxReadAddrs, coveredAuxReadSizes, c, true);
                }
            }
        } else if (coveredColumnAddrs.size() > 0) {
            // Addr-based (O3): reference caller's addresses, size=0 means not owned
            for (int c = 0; c < coverCount; c++) {
                coveredColReadAddrs[c] = coveredColumnAddrs.getQuick(c);
                coveredColReadSizes[c] = 0; // not owned — caller unmaps
            }
        }
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
            Unsafe.setMemory(spillKeyAddrsAddr, addrsSize, (byte) 0);
            Unsafe.setMemory(spillKeyCountsAddr, countsSize, (byte) 0);
            Unsafe.setMemory(spillKeyCapacitiesAddr, capsSize, (byte) 0);
            spillArraysCapacity = cap;
        } else if (needed > spillArraysCapacity) {
            int newCap = Math.max(keyCapacity, needed);

            // Realloc all three buffers. Only update spillArraysCapacity after
            // ALL succeed so freeSpillData() uses the correct size on OOM cleanup.
            // Realloc smallest buffers first (counts, caps at 4B/slot) before
            // the largest (addrs at 8B/slot) to minimize tag accounting error if
            // the last realloc is the one that fails.
            long oldCountsSize = (long) spillArraysCapacity * Integer.BYTES;
            long newCountsSize = (long) newCap * Integer.BYTES;
            spillKeyCountsAddr = Unsafe.realloc(spillKeyCountsAddr, oldCountsSize, newCountsSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.setMemory(spillKeyCountsAddr + oldCountsSize, newCountsSize - oldCountsSize, (byte) 0);

            long oldCapsSize = (long) spillArraysCapacity * Integer.BYTES;
            long newCapsSize = (long) newCap * Integer.BYTES;
            spillKeyCapacitiesAddr = Unsafe.realloc(spillKeyCapacitiesAddr, oldCapsSize, newCapsSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.setMemory(spillKeyCapacitiesAddr + oldCapsSize, newCapsSize - oldCapsSize, (byte) 0);

            long oldAddrsSize = (long) spillArraysCapacity * Long.BYTES;
            long newAddrsSize = (long) newCap * Long.BYTES;
            spillKeyAddrsAddr = Unsafe.realloc(spillKeyAddrsAddr, oldAddrsSize, newAddrsSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.setMemory(spillKeyAddrsAddr + oldAddrsSize, newAddrsSize - oldAddrsSize, (byte) 0);

            spillArraysCapacity = newCap;
        }
    }

    private int estimateMaxPerKey(long gen0Addr, int gen0KeyCount, int gen0SiSize) {
        int max = 0;
        int sc = PostingIndexUtils.strideCount(gen0KeyCount);
        for (int s = 0; s < sc; s++) {
            long strideOff = Unsafe.getLong(gen0Addr + (long) s * Long.BYTES);
            long strideAddr = gen0Addr + gen0SiSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(gen0KeyCount, s);
            byte mode = Unsafe.getByte(strideAddr);
            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                for (int j = 0; j < ks; j++) {
                    int count = Unsafe.getInt(prefixAddr + (long) (j + 1) * Integer.BYTES)
                            - Unsafe.getInt(prefixAddr + (long) j * Integer.BYTES);
                    if (count > max) max = count;
                }
            } else {
                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                for (int j = 0; j < ks; j++) {
                    int count = Unsafe.getInt(countsAddr + (long) j * Integer.BYTES);
                    if (count > max) max = count;
                }
            }
        }
        return max;
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
            totalValues += Unsafe.getInt(pendingCountsAddr + (long) key * Integer.BYTES);
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

        Unsafe.setMemory(flushHeaderBuf, headerSize, (byte) 0);
        long keyIdsBase = flushHeaderBuf;
        long countsBase = flushHeaderBuf + (long) activeKeyCount * Integer.BYTES;
        long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;

        // Reserve header + data space in valueMem. Use actual value counts for tighter bound.
        long genOffset = valueMemSize;
        long maxDataSize = 0;
        for (int i = 0; i < activeKeyCount; i++) {
            int key = activeKeyIds[i];
            int count = Unsafe.getInt(pendingCountsAddr + (long) key * Integer.BYTES)
                    + getSpillCount(key);
            maxDataSize += PostingIndexUtils.computeMaxEncodedSize(count);
        }
        long maxGenSize = headerSize + maxDataSize;
        valueMem.jumpTo(genOffset);
        // Extend to guarantee contiguous mapped region for direct encoding
        valueMem.jumpTo(genOffset + maxGenSize);
        valueMem.jumpTo(genOffset + headerSize);

        // Encode each active key's values directly into valueMem — no intermediate buffer
        long encodedOffset = 0;
        for (int idx = 0; idx < activeKeyCount; idx++) {
            int key = activeKeyIds[idx];
            int pendingCount = Unsafe.getInt(pendingCountsAddr + (long) key * Integer.BYTES);
            int spillCount = getSpillCount(key);
            int count = pendingCount + spillCount;

            Unsafe.putInt(keyIdsBase + (long) idx * Integer.BYTES, key);
            Unsafe.putInt(countsBase + (long) idx * Integer.BYTES, count);
            Unsafe.putLong(offsetsBase + (long) idx * Long.BYTES, encodedOffset);

            long destAddr = valueMem.addressOf(genOffset + headerSize + encodedOffset);
            int bytesWritten;

            if (spillCount == 0) {
                // No spill — encode directly from pending buffer
                long keyValuesAddr = pendingValuesAddr + (long) key * PENDING_SLOT_CAPACITY * Long.BYTES;
                encodeCtx.ensureCapacity(count);
                bytesWritten = PostingIndexUtils.encodeKeyNative(keyValuesAddr, count, destAddr, encodeCtx, rowIdEncoding);

                if (pendingCount > 0) {
                    long lastVal = Unsafe.getLong(keyValuesAddr + (long) (pendingCount - 1) * Long.BYTES);
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
                    int curCap = Unsafe.getInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES);
                    if (needed > curCap) {
                        int newCap = Math.max(needed, curCap * 2);
                        long oldSize = (long) curCap * Long.BYTES;
                        long newSize = (long) newCap * Long.BYTES;
                        long oldAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                        long newAddr = Unsafe.realloc(oldAddr, oldSize, newSize, MemoryTag.NATIVE_INDEX_READER);
                        Unsafe.putLong(spillKeyAddrsAddr + (long) key * Long.BYTES, newAddr);
                        Unsafe.putInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES, newCap);
                    }
                    // Append pending values after spill values
                    long pendingSrc = pendingValuesAddr + (long) key * PENDING_SLOT_CAPACITY * Long.BYTES;
                    long spillAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                    Unsafe.copyMemory(pendingSrc,
                            spillAddr + (long) spillCount * Long.BYTES,
                            (long) pendingCount * Long.BYTES);
                }
                // Encode from the spill buffer (which now holds all values in order)
                long spillAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                encodeCtx.ensureCapacity(count);
                bytesWritten = PostingIndexUtils.encodeKeyNative(spillAddr, count, destAddr, encodeCtx, rowIdEncoding);

                long lastVal = Unsafe.getLong(
                        spillAddr + (long) (count - 1) * Long.BYTES);
                if (lastVal > maxValue) {
                    maxValue = lastVal;
                }
            }
            encodedOffset += bytesWritten;
        }

        // Min/max key from the sorted active keyIds
        int minKey = activeKeyIds[0];
        int maxKey = activeKeyIds[activeKeyCount - 1];

        // Append per-gen prefix-sum index: keyOffsets[keyRange + 2] where
        // keyOffsets[k - minKey] = position of key k in the keyIds array.
        // Enables O(1) key lookup without binary search or CSR build.
        int keyRange = maxKey - minKey + 1;
        int prefixSumSize = (keyRange + 2) * Integer.BYTES;
        // Per-gen SBBF lets readers skip this gen on key misses without touching
        // any other part of the gen. Sized for the gen's active key set at the
        // configured FPP, then suffixed with a 4B numBlocks footer so the reader
        // can locate it from the tail without consulting the gen dir.
        int sbbfSize = SplitBlockBloomFilter.computeSize(activeKeyCount, PostingIndexUtils.SPARSE_SBBF_DEFAULT_FPP);
        int sbbfNumBlocks = sbbfSize >> SplitBlockBloomFilter.BLOCK_SIZE_SHIFT;
        long totalGenSize = headerSize + encodedOffset + prefixSumSize
                + sbbfSize + PostingIndexUtils.SPARSE_SBBF_NUM_BLOCKS_FOOTER_SIZE;
        valueMem.jumpTo(genOffset + totalGenSize);
        long prefixSumAddr = valueMem.addressOf(genOffset + headerSize + encodedOffset);
        Unsafe.setMemory(prefixSumAddr, prefixSumSize, (byte) 0);
        for (int i = 0; i < activeKeyCount; i++) {
            int k = activeKeyIds[i] - minKey;
            Unsafe.putInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES,
                    Unsafe.getInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES) + 1);
        }
        // Convert counts to prefix sums
        for (int i = 1; i <= keyRange + 1; i++) {
            Unsafe.putInt(prefixSumAddr + (long) i * Integer.BYTES,
                    Unsafe.getInt(prefixSumAddr + (long) i * Integer.BYTES)
                            + Unsafe.getInt(prefixSumAddr + (long) (i - 1) * Integer.BYTES));
        }

        long sbbfAddr = prefixSumAddr + prefixSumSize;
        Unsafe.setMemory(sbbfAddr, sbbfSize, (byte) 0);
        for (int i = 0; i < activeKeyCount; i++) {
            long hash = SplitBlockBloomFilter.hashKey(activeKeyIds[i]);
            SplitBlockBloomFilter.insert(sbbfAddr, sbbfSize, hash);
        }
        Unsafe.putInt(sbbfAddr + sbbfSize, sbbfNumBlocks);

        valueMemSize = genOffset + totalGenSize;

        long headerAddr = valueMem.addressOf(genOffset);
        Unsafe.copyMemory(flushHeaderBuf, headerAddr, headerSize);
        assert totalValues <= Integer.MAX_VALUE : "totalValues overflow: " + totalValues;
        writeSidecarGenData((int) totalValues, genCount);

        genCount++;
        // Ensure value-file writes are visible before publishing via metadata page
        Unsafe.storeFence();
        writeMetadataPage(genCount, maxValue,
                genCount - 1, genOffset, totalGenSize, -activeKeyCount, minKey, maxKey);

        // Clear only the active keys' pending counts (not the entire array)
        for (int i = 0; i < activeKeyCount; i++) {
            int key = activeKeyIds[i];
            Unsafe.putInt(pendingCountsAddr + (long) key * Integer.BYTES, 0);
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

    private void freeFsstScratch() {
        if (fsstSrcOffsAddr != 0) {
            Unsafe.free(fsstSrcOffsAddr, fsstSrcOffsCap, MemoryTag.NATIVE_INDEX_READER);
            fsstSrcOffsAddr = 0;
            fsstSrcOffsCap = 0;
        }
        if (fsstCmpAddr != 0) {
            Unsafe.free(fsstCmpAddr, fsstCmpCap, MemoryTag.NATIVE_INDEX_READER);
            fsstCmpAddr = 0;
            fsstCmpCap = 0;
        }
        if (fsstCmpOffsAddr != 0) {
            Unsafe.free(fsstCmpOffsAddr, fsstCmpOffsCap, MemoryTag.NATIVE_INDEX_READER);
            fsstCmpOffsAddr = 0;
            fsstCmpOffsCap = 0;
        }
        if (fsstBatchScratchAddr != 0) {
            Unsafe.free(fsstBatchScratchAddr, fsstBatchScratchCap, MemoryTag.NATIVE_INDEX_READER);
            fsstBatchScratchAddr = 0;
            fsstBatchScratchCap = 0;
        }
        if (fsstTableAddr != 0) {
            Unsafe.free(fsstTableAddr, FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_INDEX_READER);
            fsstTableAddr = 0;
        }
    }

    private void freeNativeBuffers() {
        freePendingBuffers();
        freeSpillData();
        freeFsstScratch();
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
        unmapCoveredColumnReads();
        // Don't null coveredColumnNames/coveredPartitionPath/coveredColumnNameTxns here —
        // those are configuration set by configureCovering(), not runtime buffers.
        // They must survive truncate() + rebuild cycles during commit.
        keyCapacity = 0;
    }

    private void freePendingBuffers() {
        if (pendingValuesAddr != 0) {
            Unsafe.free(pendingValuesAddr, (long) keyCapacity * PENDING_SLOT_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            pendingValuesAddr = 0;
        }
        if (pendingCountsAddr != 0) {
            Unsafe.free(pendingCountsAddr, (long) keyCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
            pendingCountsAddr = 0;
        }
    }

    private void freeSpillData() {
        if (spillKeyAddrsAddr != 0) {
            for (int i = 0; i < spillArraysCapacity; i++) {
                long addr = Unsafe.getLong(spillKeyAddrsAddr + (long) i * Long.BYTES);
                if (addr != 0) {
                    int cap = Unsafe.getInt(spillKeyCapacitiesAddr + (long) i * Integer.BYTES);
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
    }

    private long getCoveredAuxReadAddr(int covIdx, long offset, long needed) {
        ensureCoveredColumnReadMaps();
        long addr = coveredAuxReadAddrs[covIdx];
        long size = coveredAuxReadSizes[covIdx];
        if (addr == 0) {
            return 0;
        }
        if (size == 0) {
            return addr + offset;
        }
        if (offset + needed > size) {
            Files.munmap(addr, size, MemoryTag.MMAP_INDEX_WRITER);
            coveredAuxReadAddrs[covIdx] = 0;
            if (coveredColumnNames.size() > 0 && coveredPartitionPath.size() > 0) {
                mapColumnFile(Path.getThreadLocal(coveredPartitionPath),
                        coveredColumnNames.getQuick(covIdx), coveredColumnNameTxns.getQuick(covIdx),
                        coveredAuxReadAddrs, coveredAuxReadSizes, covIdx, true);
            }
            addr = coveredAuxReadAddrs[covIdx];
            size = coveredAuxReadSizes[covIdx];
            if (addr == 0 || offset + needed > size) {
                return 0;
            }
        }
        return addr + offset;
    }

    private long getCoveredColumnNameTxn(int c) {
        return c < coveredColumnNameTxns.size()
                ? coveredColumnNameTxns.getQuick(c)
                : COLUMN_NAME_TXN_NONE;
    }

    private long getCoveredDataReadAddr(int covIdx, long offset, long needed) {
        ensureCoveredColumnReadMaps();
        long addr = coveredColReadAddrs[covIdx];
        long size = coveredColReadSizes[covIdx];
        if (addr == 0) {
            return 0;
        }
        // size == 0 means addr-based (O3): caller-provided buffer, no bounds check or remap
        if (size == 0) {
            return addr + offset;
        }
        // size > 0 means name-based: writer-owned mmap, remap if file grew
        if (offset + needed > size) {
            Files.munmap(addr, size, MemoryTag.MMAP_INDEX_WRITER);
            coveredColReadAddrs[covIdx] = 0;
            if (coveredColumnNames.size() > 0 && coveredPartitionPath.size() > 0) {
                mapColumnFile(Path.getThreadLocal(coveredPartitionPath),
                        coveredColumnNames.getQuick(covIdx), coveredColumnNameTxns.getQuick(covIdx),
                        coveredColReadAddrs, coveredColReadSizes, covIdx, false);
            }
            addr = coveredColReadAddrs[covIdx];
            size = coveredColReadSizes[covIdx];
            if (addr == 0 || offset + needed > size) {
                return 0;
            }
        }
        return addr + offset;
    }

    private int getSpillCount(int key) {
        if (spillKeyCountsAddr == 0 || key >= spillArraysCapacity) {
            return 0;
        }
        return Unsafe.getInt(spillKeyCountsAddr + (long) key * Integer.BYTES);
    }

    private void growKeyBuffers(int minCapacity) {
        int newCapacity = Math.max(keyCapacity * 2, minCapacity);

        // Realloc counts first (smaller buffer, less likely to OOM).
        long oldCountSize = (long) keyCapacity * Integer.BYTES;
        long newCountSize = (long) newCapacity * Integer.BYTES;
        pendingCountsAddr = Unsafe.realloc(pendingCountsAddr, oldCountSize, newCountSize, MemoryTag.NATIVE_INDEX_READER);
        Unsafe.setMemory(pendingCountsAddr + oldCountSize, newCountSize - oldCountSize, (byte) 0);

        long oldValSize = (long) keyCapacity * PENDING_SLOT_CAPACITY * Long.BYTES;
        long newValSize = (long) newCapacity * PENDING_SLOT_CAPACITY * Long.BYTES;
        try {
            pendingValuesAddr = Unsafe.realloc(pendingValuesAddr, oldValSize, newValSize, MemoryTag.NATIVE_INDEX_READER);
        } catch (Throwable e) {
            // Counts buffer already resized — update keyCapacity to match so
            // freePendingBuffers() frees with the correct sizes.
            keyCapacity = newCapacity;
            activeKeyIds = Arrays.copyOf(activeKeyIds, newCapacity);
            throw e;
        }
        Unsafe.setMemory(pendingValuesAddr + oldValSize, newValSize - oldValSize, (byte) 0);

        keyCapacity = newCapacity;
        activeKeyIds = Arrays.copyOf(activeKeyIds, newCapacity);
    }

    /**
     * Zero out any abandoned tentative metadata on the inactive page. Called
     * at fd-based open so a previous failed O3 attempt's tentative state
     * cannot be folded in by a later mergeTentativeIntoActiveIfAny. No-op if
     * the inactive page is committed (sealTxn &gt;= 0) or already zeroed; the
     * existing seqlock protocol then naturally distinguishes new writes from
     * the cleared baseline.
     */
    private void invalidateStaleTentative() {
        if (keyMem.size() < PostingIndexUtils.KEY_FILE_RESERVED) {
            return;
        }
        long inactivePageOffset = (activePageOffset == PostingIndexUtils.PAGE_A_OFFSET)
                ? PostingIndexUtils.PAGE_B_OFFSET
                : PostingIndexUtils.PAGE_A_OFFSET;
        long inactiveSealTxn = keyMem.getLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN);
        if (inactiveSealTxn != PostingIndexUtils.SEAL_TXN_TENTATIVE) {
            return;
        }
        // Zero seqStart first (so a partial invalidation leaves the page
        // torn rather than half-cleared looking valid), then sealTxn, then
        // seqEnd. After this, the page reads as: sealTxn=0 (non-tentative),
        // start==end==0 — which mergeTentativeIntoActiveIfAny rejects on
        // the sealTxn check, and determineActivePageOffset gives validB=0
        // so the active page picker never selects it over a real committed
        // page.
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, 0L);
        Unsafe.storeFence();
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN, 0L);
        Unsafe.storeFence();
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END, 0L);
    }

    private boolean isClosed() {
        return coveredColumnNames.size() == 0 && coveredColumnAddrs.size() == 0;
    }

    private void logOrphanSealedFiles(Path path, CharSequence name) {
        final long liveSealTxn = this.sealTxn;
        final long thisInstanceTxn = this.postingColumnNameTxn;
        final int scanPathLen = path.size();
        orphanSealTxns.clear();
        orphanScanPreLiveCount = 0;
        orphanScanPostLiveCount = 0;
        PostingIndexUtils.scanSealedFiles(ff, path, scanPathLen, name, new PostingIndexUtils.SealedFileVisitor() {
            @Override
            public void onCoverDataFile(int includeIdx, long postingColumnNameTxn, long coveredColumnNameTxn, long sealTxn) {
                if (postingColumnNameTxn != thisInstanceTxn || sealTxn == liveSealTxn) {
                    return;
                }
                if (sealTxn > liveSealTxn) {
                    LPSZ pc = PostingIndexUtils.coverDataFileName(path.trimTo(scanPathLen), name,
                            includeIdx, postingColumnNameTxn, coveredColumnNameTxn, sealTxn);
                    ff.removeQuiet(pc);
                    path.trimTo(scanPathLen);
                    orphanScanPostLiveCount++;
                } else if (rememberOrphan(sealTxn, thisInstanceTxn)) {
                    orphanScanPreLiveCount++;
                }
            }

            @Override
            public void onValueFile(long postingColumnNameTxn, long sealTxn) {
                if (postingColumnNameTxn != thisInstanceTxn || sealTxn == liveSealTxn) {
                    return;
                }
                if (sealTxn > liveSealTxn) {
                    LPSZ pv = PostingIndexUtils.valueFileName(path.trimTo(scanPathLen),
                            name, postingColumnNameTxn, sealTxn);
                    ff.removeQuiet(pv);
                    path.trimTo(scanPathLen);
                    orphanScanPostLiveCount++;
                } else if (rememberOrphan(sealTxn, thisInstanceTxn)) {
                    orphanScanPreLiveCount++;
                }
            }
        });
        if (orphanScanPostLiveCount > 0) {
            LOG.advisory().$("post-live POSTING files detected on writer open, deleted inline ")
                    .$("[partition=").$(path)
                    .$(", column=").$(name)
                    .$(", postingColumnNameTxn=").$(thisInstanceTxn)
                    .$(", liveSealTxn=").$(liveSealTxn)
                    .$(", postLiveFilesDeleted=").$(orphanScanPostLiveCount)
                    .$(", preLivePurgeScheduled=").$(orphanScanPreLiveCount)
                    .I$();
        } else if (orphanScanPreLiveCount > 0) {
            LOG.info().$("pre-live POSTING orphans scheduled for async purge ")
                    .$("[partition=").$(path)
                    .$(", column=").$(name)
                    .$(", postingColumnNameTxn=").$(thisInstanceTxn)
                    .$(", liveSealTxn=").$(liveSealTxn)
                    .$(", preLivePurgeScheduled=").$(orphanScanPreLiveCount)
                    .I$();
        }
    }

    private void mapColumnFile(Path p, CharSequence colName, long colNameTxn,
                               long[] addrs, long[] sizes, int idx, boolean isAux) {
        p.of(coveredPartitionPath);
        LPSZ fileName = isAux
                ? TableUtils.iFile(p, colName, colNameTxn)
                : TableUtils.dFile(p, colName, colNameTxn);
        long fd = ff.openRO(fileName);
        if (fd >= 0) {
            try {
                long fileSize = ff.length(fd);
                if (fileSize > 0) {
                    addrs[idx] = Files.mmap(fd, fileSize, 0, Files.MAP_RO, MemoryTag.MMAP_INDEX_WRITER);
                    sizes[idx] = fileSize;
                }
            } finally {
                ff.close(fd); // mmap keeps data accessible after FD close
            }
        }
    }

    private void mapCoveredColumn(Path p, int c) {
        if (coveredColReadAddrs == null) {
            coveredColReadAddrs = new long[coverCount];
            coveredColReadSizes = new long[coverCount];
            coveredAuxReadAddrs = new long[coverCount];
            coveredAuxReadSizes = new long[coverCount];
        }
        mapColumnFile(p, coveredColumnNames.getQuick(c), coveredColumnNameTxns.getQuick(c),
                coveredColReadAddrs, coveredColReadSizes, c, false);
        if (ColumnType.isVarSize(coveredColumnTypes.getQuick(c))) {
            mapColumnFile(p, coveredColumnNames.getQuick(c), coveredColumnNameTxns.getQuick(c),
                    coveredAuxReadAddrs, coveredAuxReadSizes, c, true);
        }
    }

    private int mergeKeyValues(int key, long gen0Addr, int gen0KeyCount, int gen0SiSize, long destAddr) {
        int totalCount = 0;

        // Decode from gen 0 (dense)
        if (key < gen0KeyCount) {
            int stride = key / PostingIndexUtils.DENSE_STRIDE;
            int localKey = key % PostingIndexUtils.DENSE_STRIDE;
            long strideOff = Unsafe.getLong(gen0Addr + (long) stride * Long.BYTES);
            long strideAddr = gen0Addr + gen0SiSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(gen0KeyCount, stride);
            byte mode = Unsafe.getByte(strideAddr);

            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                int bitWidth = Unsafe.getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                int startIdx = Unsafe.getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int count = Unsafe.getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES) - startIdx;
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
                        Unsafe.putLong(destAddr + (long) totalCount * Long.BYTES,
                                Unsafe.getLong(unpackBatchAddr + (long) i * Long.BYTES));
                        totalCount++;
                    }
                }
            } else {
                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                int count = Unsafe.getInt(countsAddr + (long) localKey * Integer.BYTES);
                if (count > 0) {
                    long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
                    long dataOffset = Unsafe.getLong(offsetsBase + (long) localKey * Long.BYTES);
                    int deltaHdrSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
                    long encodedAddr = strideAddr + deltaHdrSize + dataOffset;
                    PostingIndexUtils.decodeKeyToNative(encodedAddr, destAddr, decodeCtx);
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
            int count = Unsafe.getInt(countsBase + (long) idx * Integer.BYTES);
            if (count == 0) continue;

            long dataOffset = Unsafe.getLong(offsetsBase + (long) idx * Long.BYTES);
            long encodedAddr = genAddr + headerSize + dataOffset;
            PostingIndexUtils.decodeKeyToNative(encodedAddr, destAddr + (long) totalCount * Long.BYTES, decodeCtx);
            totalCount += count;
        }

        return totalCount;
    }

    private void openSealValueFile(long newTxn) {
        if (partitionPath.size() == 0) {
            // fd-based: write encoded output past the current valueMem content
            // so source [0, valueMemSize) and destination [valueMemSize, ...)
            // are disjoint regions of the same .pv file. The chunked encoder
            // can safely read source while writing destination. Pre-extend the
            // mapping up front so addressOf results stay stable through the
            // whole encode loop; a runtime extend would invalidate them.
            // Seal compacts, so outputSize <= inputSize; 2 * inputSize is safe
            // headroom. The caller memmoves the output down to offset 0 and
            // shrinks the file after the encode loop. sealTxn is unchanged:
            // fd-based reuses the existing .pv rather than allocating one at
            // newTxn.
            long inputSize = valueMemSize;
            if (inputSize > 0) {
                valueMem.jumpTo(inputSize * 2);
            }
            valueMem.jumpTo(inputSize);
            sealTarget = valueMem;
            return;
        }
        Path p = Path.getThreadLocal(partitionPath);
        LPSZ fileName = PostingIndexUtils.valueFileName(p, indexName, postingColumnNameTxn, newTxn);
        sealValueMem.of(ff, fileName,
                configuration.getDataIndexValueAppendPageSize(),
                MemoryTag.MMAP_INDEX_WRITER,
                configuration.getWriterFileOpenOpts());
        sealValueMem.jumpTo(0);
        sealTarget = sealValueMem;
    }

    private void openSidecarFiles(Path path, CharSequence name, long postingColumnNameTxn, long sealTxn) {
        if (coverCount <= 0) {
            return;
        }
        final int plen = path.size();
        try {
            // Write .pci info file
            sidecarInfoMem = Vm.getCMARWInstance();
            sidecarInfoMem.of(
                    ff,
                    PostingIndexUtils.coverInfoFileName(path, name, postingColumnNameTxn),
                    configuration.getDataIndexValueAppendPageSize(),
                    0L,
                    MemoryTag.MMAP_INDEX_WRITER
            );
            path.trimTo(plen);
            // .pci layout: magic(4B) + count(4B) + indices[count] (4B each).
            // Per-cover-column type is intentionally NOT stored — readers resolve types
            // from the live RecordMetadata so an ALTER TYPE on a covered column never
            // produces a stale-snapshot mismatch.
            sidecarInfoMem.putInt(PostingIndexUtils.COVER_INFO_MAGIC);
            sidecarInfoMem.putInt(coverCount);
            for (int c = 0; c < coverCount; c++) {
                sidecarInfoMem.putInt(coveredColumnIndices.getQuick(c));
            }

            // Open .pc0, .pc1, ... files. Each cover c uses its own
            // coveredColumnNameTxn from _cv.d so ALTER TYPE on a covered
            // column produces a different filename and reader sees old
            // sidecar as missing -> falls back to main column.
            sidecarMems.clear();
            for (int c = 0; c < coverCount; c++) {
                if (coveredColumnIndices.getQuick(c) < 0) {
                    sidecarMems.add(null);    // tombstoned slot
                    continue;
                }
                long covT = getCoveredColumnNameTxn(c);
                MemoryMARW mem = Vm.getCMARWInstance();
                mem.of(
                        ff,
                        PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, c, postingColumnNameTxn, covT, sealTxn),
                        configuration.getDataIndexValueAppendPageSize(),
                        0L,
                        MemoryTag.MMAP_INDEX_WRITER
                );
                mem.jumpTo(PostingIndexUtils.PC_HEADER_SIZE);
                sidecarMems.add(mem);
                path.trimTo(plen);
            }
        } catch (Throwable e) {
            closeSidecarMems();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    /**
     * Opens sidecar files for append, preserving existing data. Used by
     * writeSidecarGenData to add per-gen raw blocks after seal's stride-indexed
     * data. Creates files if they don't exist, writes .pci header only when new.
     * <p>
     * When reopening existing files, the .pci header is preserved as-is. This is
     * safe because the covered column configuration (INCLUDE clause) is part of
     * the table schema and cannot change within a column version — schema changes
     * create new file versions via sealTxn bump.
     */
    private void openSidecarFilesForAppend(Path path, CharSequence name, long postingColumnNameTxn, long sealTxn) {
        if (coverCount <= 0) {
            return;
        }
        final int plen = path.size();
        try {
            LPSZ pciFile = PostingIndexUtils.coverInfoFileName(path, name, postingColumnNameTxn);
            boolean isNew = !ff.exists(pciFile);
            sidecarInfoMem = Vm.getCMARWInstance();
            long pciSize = isNew ? 0L : ff.length(pciFile);
            if (pciSize < 0) {
                pciSize = 0L; // I/O error reading length — treat as new file
            }
            sidecarInfoMem.of(ff, pciFile,
                    configuration.getDataIndexValueAppendPageSize(),
                    pciSize,
                    MemoryTag.MMAP_INDEX_WRITER);
            path.trimTo(plen);
            if (isNew) {
                // .pci layout: magic(4B) + count(4B) + indices[count] (4B each).
                // No per-cover-column type — see openSidecarFiles for rationale.
                sidecarInfoMem.putInt(PostingIndexUtils.COVER_INFO_MAGIC);
                sidecarInfoMem.putInt(coverCount);
                for (int c = 0; c < coverCount; c++) {
                    sidecarInfoMem.putInt(coveredColumnIndices.getQuick(c));
                }
            }

            // Each cover c's filename uses the covered column's own
            // coveredColumnNameTxn from _cv.d (per Phase B). Tombstoned
            // slots (coveredColumnIndices == -1) skipped — no .pcN file.
            sidecarMems.clear();
            for (int c = 0; c < coverCount; c++) {
                if (coveredColumnIndices.getQuick(c) < 0) {
                    sidecarMems.add(null);
                    continue;
                }
                long covT = getCoveredColumnNameTxn(c);
                LPSZ pcFile = PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, c, postingColumnNameTxn, covT, sealTxn);
                long fileLen = ff.exists(pcFile) ? ff.length(pcFile) : -1L;
                long existingSize = fileLen > 0 ? fileLen : 0L;
                MemoryMARW mem = Vm.getCMARWInstance();
                mem.of(ff, pcFile,
                        configuration.getDataIndexValueAppendPageSize(),
                        existingSize,
                        MemoryTag.MMAP_INDEX_WRITER);
                mem.jumpTo(Math.max(existingSize, PostingIndexUtils.PC_HEADER_SIZE));
                sidecarMems.add(mem);
                path.trimTo(plen);
            }
        } catch (Throwable e) {
            closeSidecarMems();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    /**
     * Records that the previous-sealTxn files for this column instance are
     * now superseded and eligible for purge. {@link #publishPendingPurges}
     * forwards each entry to the global queue; the background job checks the
     * table scoreboard before unlinking.
     * <p>
     * The scoreboard query range {@code [fromTableTxn, toTableTxn)} brackets
     * the txns at which a reader could still see the previous sealed
     * version: {@code fromTableTxn} is the publish-txn of the seal before
     * the one we just superseded; {@code toTableTxn} is the publish-txn of
     * the new sealed version.
     * <p>
     * When no commit-supplied publish-txn is available (close, rebuild) the
     * entry is conservatively scoped to {@code [0, Long.MAX_VALUE)}: better
     * to leak the file a bit longer than delete it under an active reader.
     */
    private void recordPostingSealPurge(long supersededSealTxn) {
        long publishTxn = pendingPublishTableTxn;
        long fromTxn;
        long toTxn;
        if (publishTxn >= 0) {
            long oldCurrent = currentPublishTableTxn;
            long oldPrev = prevPublishTableTxn;
            prevPublishTableTxn = oldCurrent;
            currentPublishTableTxn = publishTxn;
            fromTxn = oldCurrent >= 0 ? oldCurrent : (oldPrev >= 0 ? oldPrev : postingColumnNameTxn);
            toTxn = publishTxn;
        } else {
            fromTxn = 0L;
            toTxn = Long.MAX_VALUE;
        }
        pendingPublishTableTxn = -1;
        // Cap the in-memory outbox to prevent unbounded growth when the
        // global PostingSealPurge job is disabled, the queue is permanently
        // saturated, or publishPendingPurges() is never called. When at the
        // cap, drop the oldest entry: the writer-open orphan scan will pick
        // up the surviving sidecar files on the next reopen, so the leak is
        // bounded.
        int outboxMax = configuration.getPostingSealPurgeOutboxMax();
        if (outboxMax > 0 && pendingPurges.size() >= outboxMax) {
            PendingSealPurge oldest = pendingPurges.getQuick(0);
            LOG.critical()
                    .$("posting seal-purge outbox saturated, dropping oldest entry [")
                    .$("indexName=").$(indexName)
                    .$(", postingColumnNameTxn=").$(oldest.postingColumnNameTxn)
                    .$(", sealTxn=").$(oldest.sealTxn)
                    .$(", outboxMax=").$(outboxMax)
                    .$("]. The orphaned sidecar files will be cleaned up by the writer-open orphan scan on the next reopen.")
                    .$();
            oldest.of(0L, 0L, 0L, 0L, Long.MIN_VALUE, -1L);
            pendingPurgePool.add(oldest);
            pendingPurges.remove(0);
        }
        PendingSealPurge entry = pendingPurgePool.size() > 0
                ? pendingPurgePool.popLast()
                : new PendingSealPurge();
        entry.of(postingColumnNameTxn, supersededSealTxn, fromTxn, toTxn, partitionTimestamp, partitionNameTxn);
        pendingPurges.add(entry);
    }

    /**
     * Decode all generations, optionally filter values > maxValueCutoff, then
     * re-encode surviving values into a single dense stride-indexed generation.
     * <p>
     * Decoding is chunked by stride (256 keys): instead of decoding all values
     * into a single flat buffer (which would be totalValueCount × 8 bytes),
     * each stride's keys are decoded from the generations into a small reusable
     * buffer and immediately re-encoded. Peak memory is proportional to the
     * largest stride's value count, not the total across all keys.
     *
     * @param maxValue       the maxValue to write into the metadata header
     * @param maxValueCutoff Long.MAX_VALUE means no filtering (seal path);
     *                       any other value trims per-key values to those <= cutoff (rollback path)
     */
    private void reencodeAllGenerations(long newSealTxn, long maxValue, long maxValueCutoff) {

        // Phase 1: Count total values per key across all generations
        long totalCountsSize = (long) keyCount * Integer.BYTES;
        long totalCountsAddr = Unsafe.malloc(totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
        try {
            Unsafe.setMemory(totalCountsAddr, totalCountsSize, (byte) 0);

            long totalValueCount = 0;
            for (int gen = 0; gen < genCount; gen++) {
                long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, gen);
                long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                long keyIdsBase = valueMem.addressOf(genFileOffset);

                if (genKeyCount < 0) {
                    // Sparse format
                    int activeKeyCount = -genKeyCount;
                    long countsBase = keyIdsBase + (long) activeKeyCount * Integer.BYTES;
                    for (int i = 0; i < activeKeyCount; i++) {
                        int key = Unsafe.getInt(keyIdsBase + (long) i * Integer.BYTES);
                        int count = Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
                        int existing = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                        Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
                        totalValueCount += count;
                    }
                } else {
                    // Dense format — stride-indexed (supports delta and flat modes)
                    int sc = PostingIndexUtils.strideCount(genKeyCount);
                    int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                    for (int s = 0; s < sc; s++) {
                        long strideOff = Unsafe.getLong(keyIdsBase + (long) s * Long.BYTES);
                        long strideAddr = keyIdsBase + siSize + strideOff;
                        int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
                        byte mode = Unsafe.getByte(strideAddr);
                        if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                            long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getInt(prefixAddr + (long) (j + 1) * Integer.BYTES)
                                        - Unsafe.getInt(prefixAddr + (long) j * Integer.BYTES);
                                int existing = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
                                totalValueCount += count;
                            }
                        } else {
                            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                            for (int j = 0; j < ks; j++) {
                                int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                int count = Unsafe.getInt(countsAddr + (long) j * Integer.BYTES);
                                int existing = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                                Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
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

            // Scan totalCountsAddr to find max per-stride total. This determines
            // the stride decode buffer size — much smaller than totalValueCount.
            int maxStrideTotal = 0;
            {
                int sc0 = PostingIndexUtils.strideCount(keyCount);
                for (int s0 = 0; s0 < sc0; s0++) {
                    int strideTotal = 0;
                    int ks0 = PostingIndexUtils.keysInStride(keyCount, s0);
                    for (int j0 = 0; j0 < ks0; j0++) {
                        int key0 = s0 * PostingIndexUtils.DENSE_STRIDE + j0;
                        strideTotal += Unsafe.getInt(totalCountsAddr + (long) key0 * Integer.BYTES);
                    }
                    if (strideTotal > maxStrideTotal) maxStrideTotal = strideTotal;
                }
            }

            if (maxStrideTotal > packedResidualsCapacity) {
                if (packedResidualsAddr != 0) {
                    Unsafe.free(packedResidualsAddr, (long) packedResidualsCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                packedResidualsCapacity = maxStrideTotal;
                packedResidualsAddr = Unsafe.malloc((long) maxStrideTotal * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }

            if (maxValueCutoff < Long.MAX_VALUE) {
                // Rollback path: monolithic decode + filter + re-encode to new file.
                // Rollback is rare and operates on small data volumes, so the
                // monolithic buffer is acceptable here.
                reencodeMonolithic(maxValue, maxValueCutoff, totalCountsAddr, totalValueCount);
            } else {
                // Seal path: chunked stride-by-stride decode+encode. Replaces
                // the old monolithic allValuesAddr (totalValueCount × 8 bytes)
                // with a buffer sized for the largest single stride.
                long strideValsAddr = Unsafe.malloc((long) maxStrideTotal * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                try {
                    reencodeWithStrideDecoding(
                            newSealTxn, maxValue, totalCountsAddr, strideValsAddr
                    );
                } finally {
                    Unsafe.free(strideValsAddr, (long) maxStrideTotal * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
            }
        } finally {
            Unsafe.free(totalCountsAddr, totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    /**
     * Monolithic decode + filter + encode for the inPlace (rollback) path.
     * Decodes all values into a flat buffer, applies the maxValueCutoff filter,
     * then re-encodes. Uses more memory than the stride-chunked approach but
     * is needed for inPlace because the filter can change keyCount, which
     * affects stride layout. Rollback is rare and operates on small data.
     */
    private void reencodeMonolithic(long maxValue, long maxValueCutoff, long totalCountsAddr, long totalValueCount) {
        long allValuesAddr = Unsafe.malloc(totalValueCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        try {
            long keyOffsetsSize = (long) keyCount * Long.BYTES;
            long keyOffsetsAddr = Unsafe.malloc(keyOffsetsSize, MemoryTag.NATIVE_INDEX_READER);
            try {
                // Compute per-key write offsets
                long writeOffset = 0;
                for (int key = 0; key < keyCount; key++) {
                    Unsafe.putLong(keyOffsetsAddr + (long) key * Long.BYTES, writeOffset);
                    writeOffset += Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                }

                // Decode all values from all gens into allValuesAddr
                for (int gen = 0; gen < genCount; gen++) {
                    long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, gen);
                    long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                    int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                    long keyIdsBase = valueMem.addressOf(genFileOffset);

                    if (genKeyCount < 0) {
                        int activeKeyCount = -genKeyCount;
                        int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
                        long countsBase = keyIdsBase + (long) activeKeyCount * Integer.BYTES;
                        long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;

                        for (int i = 0; i < activeKeyCount; i++) {
                            int key = Unsafe.getInt(keyIdsBase + (long) i * Integer.BYTES);
                            int count = Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
                            if (count == 0) continue;

                            long dataOffset = Unsafe.getLong(offsetsBase + (long) i * Long.BYTES);
                            long encodedAddr = keyIdsBase + headerSize + dataOffset;

                            long keyWriteOff = Unsafe.getLong(keyOffsetsAddr + (long) key * Long.BYTES);
                            PostingIndexUtils.decodeKeyToNative(encodedAddr, allValuesAddr + keyWriteOff * Long.BYTES, decodeCtx);
                            Unsafe.putLong(keyOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                        }
                    } else {
                        int sc = PostingIndexUtils.strideCount(genKeyCount);
                        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
                        for (int s = 0; s < sc; s++) {
                            long strideOff = Unsafe.getLong(keyIdsBase + (long) s * Long.BYTES);
                            long strideAddr = keyIdsBase + siSize + strideOff;
                            int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
                            byte mode = Unsafe.getByte(strideAddr);

                            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                                int bitWidth = Unsafe.getByte(strideAddr + 1) & 0xFF;
                                long baseValue = Unsafe.getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                                int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);
                                long flatDataAddr = strideAddr + flatHeaderSize;

                                for (int j = 0; j < ks; j++) {
                                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                    int startIdx = Unsafe.getInt(prefixAddr + (long) j * Integer.BYTES);
                                    int count = Unsafe.getInt(prefixAddr + (long) (j + 1) * Integer.BYTES) - startIdx;
                                    if (count == 0) continue;

                                    long keyWriteOff = Unsafe.getLong(keyOffsetsAddr + (long) key * Long.BYTES);
                                    long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;
                                    BitpackUtils.unpackValuesFrom(flatDataAddr, startIdx, count, bitWidth, baseValue, destAddr);
                                    Unsafe.putLong(keyOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                                }
                            } else {
                                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                                long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
                                int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);

                                for (int j = 0; j < ks; j++) {
                                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                                    int count = Unsafe.getInt(countsAddr + (long) j * Integer.BYTES);
                                    if (count == 0) continue;

                                    long dataOff = Unsafe.getLong(offsetsBase + (long) j * Long.BYTES);
                                    long encodedAddr = strideAddr + deltaHeaderSize + dataOff;
                                    long keyWriteOff = Unsafe.getLong(keyOffsetsAddr + (long) key * Long.BYTES);
                                    PostingIndexUtils.decodeKeyToNative(encodedAddr, allValuesAddr + keyWriteOff * Long.BYTES, decodeCtx);
                                    Unsafe.putLong(keyOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                                }
                            }
                        }
                    }
                }

                // Recompute base offsets and apply rollback filter
                long cumOff = 0;
                for (int key = 0; key < keyCount; key++) {
                    Unsafe.putLong(keyOffsetsAddr + (long) key * Long.BYTES, cumOff);
                    cumOff += Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                }

                long survivingValueCount = 0;
                int newKeyCount = 0;
                for (int key = 0; key < keyCount; key++) {
                    int origCount = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    if (origCount == 0) continue;

                    long keyOff = Unsafe.getLong(keyOffsetsAddr + (long) key * Long.BYTES);
                    long keyAddr = allValuesAddr + keyOff * Long.BYTES;
                    int lo = 0, hi = origCount - 1;
                    int cutoff = -1;
                    while (lo <= hi) {
                        int mid = (lo + hi) >>> 1;
                        long midVal = Unsafe.getLong(keyAddr + (long) mid * Long.BYTES);
                        if (midVal <= maxValueCutoff) {
                            cutoff = mid;
                            lo = mid + 1;
                        } else {
                            hi = mid - 1;
                        }
                    }

                    int newCount = cutoff + 1;
                    Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES, newCount);
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

                // Re-encode into stride-indexed format in a new .pv file.
                // The old .pv stays on disk for concurrent readers.
                long newTxn = Math.max(1, sealTxn + 1);
                if (partitionPath.size() > 0) {
                    openSealValueFile(newTxn);
                } else {
                    // fd-based writer (O3): no concurrent readers, reuse valueMem
                    valueMem.jumpTo(0);
                    sealTarget = valueMem;
                }
                int sc = PostingIndexUtils.strideCount(keyCount);
                int siSize = PostingIndexUtils.strideIndexSize(keyCount);
                long sealOffset = 0;
                for (int i = 0; i < siSize; i += Long.BYTES) {
                    sealTarget.putLong(0L);
                }

                long strideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
                int maxLocalHeaderSize = Math.max(
                        PostingIndexUtils.strideDeltaHeaderSize(PostingIndexUtils.DENSE_STRIDE),
                        PostingIndexUtils.strideFlatHeaderSize(PostingIndexUtils.DENSE_STRIDE));
                long localHeaderBuf = 0;
                int[] bpKeySizes = strideBpKeySizes;
                int[] keyCounts = strideKeyCounts;
                long[] keyOffsets = strideKeyOffsets;
                long bpTrialBuf = 0;
                long bpTrialBufSize = 0;

                try {
                    localHeaderBuf = Unsafe.malloc(maxLocalHeaderSize, MemoryTag.NATIVE_INDEX_READER);
                    for (int s = 0; s < sc; s++) {
                        int ks = PostingIndexUtils.keysInStride(keyCount, s);

                        long strideTrialSize = 0;
                        for (int j = 0; j < ks; j++) {
                            int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                            int count = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                            keyCounts[j] = count;
                            long keyOff = Unsafe.getLong(keyOffsetsAddr + (long) key * Long.BYTES);
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

                        int bpDataTotal = 0;
                        for (int j = 0; j < ks; j++) {
                            int count = keyCounts[j];
                            if (count > 0) {
                                long keyAddr = allValuesAddr + keyOffsets[j] * Long.BYTES;
                                encodeCtx.ensureCapacity(count);
                                bpKeySizes[j] = PostingIndexUtils.encodeKeyNative(keyAddr, count, bpTrialBuf + bpDataTotal, encodeCtx, rowIdEncoding);
                            } else {
                                bpKeySizes[j] = 0;
                            }
                            bpDataTotal += bpKeySizes[j];
                        }

                        int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
                        int deltaSize = deltaHeaderSize + bpDataTotal;

                        long totalStrideValuesL = 0;
                        long strideMinValue = Long.MAX_VALUE;
                        long strideMaxValue = Long.MIN_VALUE;
                        for (int j = 0; j < ks; j++) {
                            int count = keyCounts[j];
                            totalStrideValuesL += count;
                            long keyAddr = allValuesAddr + keyOffsets[j] * Long.BYTES;
                            for (int i = 0; i < count; i++) {
                                long val = Unsafe.getLong(keyAddr + (long) i * Long.BYTES);
                                if (val < strideMinValue) strideMinValue = val;
                                if (val > strideMaxValue) strideMaxValue = val;
                            }
                        }
                        if (totalStrideValuesL == 0) {
                            strideMinValue = 0;
                            strideMaxValue = 0;
                        }

                        boolean useFlat;
                        int localBitWidth = 0;
                        int flatDataSize = 0;
                        int flatSize;
                        int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);

                        if (totalStrideValuesL > Integer.MAX_VALUE) {
                            useFlat = false;
                        } else {
                            int totalStrideValues = (int) totalStrideValuesL;
                            long strideRange = strideMaxValue - strideMinValue;
                            int naturalBitWidth = strideRange <= 0 ? 1 : BitpackUtils.bitsNeeded(strideRange);
                            int alignedBitWidth = maybeAlignBitWidth(naturalBitWidth, alignedBitWidthThreshold);
                            int naturalFlatDataSize = BitpackUtils.packedDataSize(totalStrideValues, naturalBitWidth);
                            int naturalFlatSize = flatHeaderSize + naturalFlatDataSize;

                            if (alignedBitWidth != naturalBitWidth) {
                                int alignedFlatDataSize = BitpackUtils.packedDataSize(totalStrideValues, alignedBitWidth);
                                int alignedFlatSize = flatHeaderSize + alignedFlatDataSize;
                                if (alignedFlatSize < deltaSize) {
                                    localBitWidth = alignedBitWidth;
                                    flatDataSize = alignedFlatDataSize;
                                    flatSize = alignedFlatSize;
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
                            useFlat = flatSize < deltaSize;
                        }

                        long strideOff = sealTarget.getAppendOffset() - sealOffset - siSize;
                        Unsafe.putLong(strideIndexBuf + (long) s * Long.BYTES, strideOff);

                        if (useFlat) {
                            writePackedStride(ks, keyCounts, keyOffsets, localBitWidth, strideMinValue,
                                    flatHeaderSize, flatDataSize, localHeaderBuf, allValuesAddr);
                        } else {
                            writeDeltaStride(ks, keyCounts, deltaHeaderSize, bpTrialBuf, bpKeySizes, localHeaderBuf);
                        }

                    }

                    long totalStrideBlocksSize = sealTarget.getAppendOffset() - sealOffset - siSize;
                    Unsafe.putLong(strideIndexBuf + (long) sc * Long.BYTES, totalStrideBlocksSize);
                    Unsafe.copyMemory(strideIndexBuf, sealTarget.addressOf(sealOffset), siSize);
                    valueMemSize = sealTarget.getAppendOffset();
                } finally {
                    if (bpTrialBuf != 0) {
                        Unsafe.free(bpTrialBuf, bpTrialBufSize, MemoryTag.NATIVE_INDEX_READER);
                    }
                    if (localHeaderBuf != 0) {
                        Unsafe.free(localHeaderBuf, maxLocalHeaderSize, MemoryTag.NATIVE_INDEX_READER);
                    }
                    Unsafe.free(strideIndexBuf, siSize, MemoryTag.NATIVE_INDEX_READER);
                }

                valueMemSize = sealTarget.getAppendOffset();
                sealTarget = null;

                if (partitionPath.size() > 0) {
                    sealValueMem.sync(false);
                    // Sync covering sidecars before publishing the new
                    // sealTxn in .pk so readers do not see a torn sidecar
                    // tail after a power loss.
                    for (int c = 0, n = sidecarMems.size(); c < n; c++) {
                        MemoryMARW mem = sidecarMems.getQuick(c);
                        if (mem != null && mem.isOpen()) {
                            mem.sync(false);
                        }
                    }
                    if (sidecarInfoMem != null && sidecarInfoMem.isOpen()) {
                        sidecarInfoMem.sync(false);
                    }
                    switchToSealedValueFile(newTxn);
                }

                Unsafe.storeFence();
                // genCount must publish after writeMetadataPage so a mid-seal failure
                // leaves the in-memory view consistent with on-disk .pk.
                writeMetadataPage(1, maxValue, sealOffset, valueMemSize - sealOffset, keyCount, keyCount - 1);
                genCount = 1;
            } finally {
                Unsafe.free(keyOffsetsAddr, keyOffsetsSize, MemoryTag.NATIVE_INDEX_READER);
            }
        } finally {
            Unsafe.free(allValuesAddr, totalValueCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    /**
     * Combined decode + encode loop (seal path only, never inPlace).
     * For each output stride, decodes just that stride's keys from all
     * generations into strideValsAddr, then encodes immediately.
     */
    private void reencodeWithStrideDecoding(
            long newSealTxn,
            long maxValue,
            long totalCountsAddr, long strideValsAddr
    ) {
        int sc = PostingIndexUtils.strideCount(keyCount);
        int siSize = PostingIndexUtils.strideIndexSize(keyCount);

        openSealValueFile(newSealTxn);
        // Encoded output starts at sealTarget's current append position. For
        // path-based seal that's offset 0 of a fresh .pv file at newSealTxn.
        // For fd-based seal that's the current valueMemSize: output is
        // appended past the source data and memmoved down to offset 0 in the
        // mode-specific tail below.
        long sealOffset = sealTarget.getAppendOffset();
        for (int i = 0; i < siSize; i += Long.BYTES) {
            sealTarget.putLong(0L);
        }

        long strideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
        int maxDeltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(PostingIndexUtils.DENSE_STRIDE);
        int maxFlatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(PostingIndexUtils.DENSE_STRIDE);
        int maxLocalHeaderSize = Math.max(maxDeltaHeaderSize, maxFlatHeaderSize);
        long localHeaderBuf = 0;

        int[] bpKeySizes = strideBpKeySizes;
        int[] keyCounts = strideKeyCounts;
        long[] keyOffsets = strideKeyOffsets;
        long bpTrialBuf = 0;
        long bpTrialBufSize = 0;

        try {
            localHeaderBuf = Unsafe.malloc(maxLocalHeaderSize, MemoryTag.NATIVE_INDEX_READER);
            for (int s = 0; s < sc; s++) {
                int ks = PostingIndexUtils.keysInStride(keyCount, s);
                int strideStart = s * PostingIndexUtils.DENSE_STRIDE;

                // Compute per-key counts and base offsets within stride buffer
                int strideValCount = 0;
                for (int j = 0; j < ks; j++) {
                    int key = strideStart + j;
                    int count = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    keyCounts[j] = count;
                    keyOffsets[j] = strideValCount;
                    strideValCount += count;
                }

                if (strideValCount == 0) {
                    // Empty stride — record offset but skip encode
                    long strideOff = sealTarget.getAppendOffset() - sealOffset - siSize;
                    Unsafe.putLong(strideIndexBuf + (long) s * Long.BYTES, strideOff);
                    continue;
                }

                // Decode this stride's keys from all generations into strideValsAddr.
                // keyOffsets serves as write cursor during decode (advanced per key).
                for (int gen = 0; gen < genCount; gen++) {
                    long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, gen);
                    long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                    int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                    long genBase = valueMem.addressOf(genFileOffset);

                    if (genKeyCount < 0) {
                        decodeSparseGenStride(genBase, -genKeyCount, strideStart, ks, strideValsAddr, keyOffsets);
                    } else {
                        int genSc = PostingIndexUtils.strideCount(genKeyCount);
                        if (s < genSc) {
                            int genKs = PostingIndexUtils.keysInStride(genKeyCount, s);
                            decodeDenseGenStride(genBase, genKeyCount, s, genKs, ks, strideValsAddr, keyOffsets);
                        }
                    }
                }

                // Restore base offsets for encoding (decode advanced them)
                long off = 0;
                for (int j = 0; j < ks; j++) {
                    keyOffsets[j] = off;
                    off += keyCounts[j];
                }

                // Trial buffer sizing
                long strideTrialSize = 0;
                for (int j = 0; j < ks; j++) {
                    strideTrialSize += PostingIndexUtils.computeMaxEncodedSize(keyCounts[j]);
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
                        long keyAddr = strideValsAddr + keyOffsets[j] * Long.BYTES;
                        encodeCtx.ensureCapacity(count);
                        bpKeySizes[j] = PostingIndexUtils.encodeKeyNative(keyAddr, count, bpTrialBuf + bpDataTotal, encodeCtx, rowIdEncoding);
                    } else {
                        bpKeySizes[j] = 0;
                    }
                    bpDataTotal += bpKeySizes[j];
                }

                int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
                int deltaSize = deltaHeaderSize + bpDataTotal;

                // Find per-stride min/max for flat mode evaluation
                long totalStrideValuesL = 0;
                long strideMinValue = Long.MAX_VALUE;
                long strideMaxValue = Long.MIN_VALUE;
                for (int j = 0; j < ks; j++) {
                    int count = keyCounts[j];
                    totalStrideValuesL += count;
                    long keyAddr = strideValsAddr + keyOffsets[j] * Long.BYTES;
                    for (int i = 0; i < count; i++) {
                        long val = Unsafe.getLong(keyAddr + (long) i * Long.BYTES);
                        if (val < strideMinValue) strideMinValue = val;
                        if (val > strideMaxValue) strideMaxValue = val;
                    }
                }
                if (totalStrideValuesL == 0) {
                    strideMinValue = 0;
                    strideMaxValue = 0;
                }

                boolean useFlat;
                int localBitWidth = 0;
                int flatDataSize = 0;
                int flatSize;
                int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);

                if (totalStrideValuesL > Integer.MAX_VALUE) {
                    useFlat = false;
                } else {
                    int totalStrideValues = (int) totalStrideValuesL;
                    long strideRange = strideMaxValue - strideMinValue;
                    int naturalBitWidth = strideRange <= 0 ? 1 : BitpackUtils.bitsNeeded(strideRange);
                    int alignedBitWidth = maybeAlignBitWidth(naturalBitWidth, alignedBitWidthThreshold);
                    int naturalFlatDataSize = BitpackUtils.packedDataSize(totalStrideValues, naturalBitWidth);
                    int naturalFlatSize = flatHeaderSize + naturalFlatDataSize;

                    if (alignedBitWidth != naturalBitWidth) {
                        int alignedFlatDataSize = BitpackUtils.packedDataSize(totalStrideValues, alignedBitWidth);
                        int alignedFlatSize = flatHeaderSize + alignedFlatDataSize;
                        if (alignedFlatSize < deltaSize) {
                            localBitWidth = alignedBitWidth;
                            flatDataSize = alignedFlatDataSize;
                            flatSize = alignedFlatSize;
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
                    useFlat = flatSize < deltaSize;
                }

                long strideOff = sealTarget.getAppendOffset() - sealOffset - siSize;
                Unsafe.putLong(strideIndexBuf + (long) s * Long.BYTES, strideOff);

                if (useFlat) {
                    writePackedStride(ks, keyCounts, keyOffsets, localBitWidth, strideMinValue,
                            flatHeaderSize, flatDataSize, localHeaderBuf, strideValsAddr);
                } else {
                    writeDeltaStride(ks, keyCounts, deltaHeaderSize, bpTrialBuf, bpKeySizes,
                            localHeaderBuf);
                }

            }

            long totalStrideBlocksSize = sealTarget.getAppendOffset() - sealOffset - siSize;
            Unsafe.putLong(strideIndexBuf + (long) sc * Long.BYTES, totalStrideBlocksSize);

            long strideIndexAddr = sealTarget.addressOf(sealOffset);
            Unsafe.copyMemory(strideIndexBuf, strideIndexAddr, siSize);

            // Defer valueMemSize update until after the mode-specific tail
            // below: fd-based memmoves output down to offset 0 first.
        } finally {
            if (bpTrialBuf != 0) {
                Unsafe.free(bpTrialBuf, bpTrialBufSize, MemoryTag.NATIVE_INDEX_READER);
            }
            if (localHeaderBuf != 0) {
                Unsafe.free(localHeaderBuf, maxLocalHeaderSize, MemoryTag.NATIVE_INDEX_READER);
            }
            Unsafe.free(strideIndexBuf, siSize, MemoryTag.NATIVE_INDEX_READER);
        }

        final long outputSize = sealTarget.getAppendOffset() - sealOffset;
        if (partitionPath.size() == 0) {
            // fd-based: relocate output from [sealOffset, sealOffset+outputSize)
            // down to [0, outputSize) and shrink the file. sealTxn stays at
            // its current value because fd-based reuses the existing .pv;
            // it does not allocate a fresh file at newSealTxn.
            if (sealOffset > 0 && outputSize > 0) {
                long srcAddr = valueMem.addressOf(sealOffset);
                long dstAddr = valueMem.addressOf(0);
                Unsafe.copyMemory(srcAddr, dstAddr, outputSize);
            }
            valueMem.jumpTo(outputSize);
            valueMem.setSize(outputSize);
            valueMemSize = outputSize;
        } else {
            valueMemSize = sealTarget.getAppendOffset();
            sealValueMem.sync(false);
            switchToSealedValueFile(newSealTxn);
        }
        sealTarget = null;

        if (coverCount > 0 && sidecarMems.size() > 0) {
            writeSidecarsPerColumn(totalCountsAddr, strideValsAddr);
            // Sync covering sidecars before publishing the new sealTxn in
            // .pk so readers do not see a torn sidecar tail after a power
            // loss.
            for (int c = 0, n = sidecarMems.size(); c < n; c++) {
                MemoryMARW mem = sidecarMems.getQuick(c);
                if (mem != null && mem.isOpen()) {
                    mem.sync(false);
                }
            }
            if (sidecarInfoMem != null && sidecarInfoMem.isOpen()) {
                sidecarInfoMem.sync(false);
            }
        }

        Unsafe.storeFence();
        // genFileOffset is 0 in both modes: path-based wrote at offset 0 of a
        // fresh .pv, fd-based just memmoved its output to offset 0.
        // genCount must publish after writeMetadataPage so a mid-seal failure
        // leaves the in-memory view consistent with on-disk .pk.
        writeMetadataPage(1, maxValue, 0, valueMemSize, keyCount, keyCount - 1);
        genCount = 1;
    }

    /**
     * Returns every entry in {@link #pendingPurges} to the pool. Used at
     * writer close — orphan files left on disk are rediscovered by
     * {@link #logOrphanSealedFiles} on the next writer open.
     */
    private void releasePendingPurges() {
        for (int i = pendingPurges.size() - 1; i >= 0; i--) {
            PendingSealPurge entry = pendingPurges.getQuick(i);
            entry.of(0L, 0L, 0L, 0L, Long.MIN_VALUE, -1L);
            pendingPurgePool.add(entry);
        }
        pendingPurges.clear();
    }

    /**
     * Schedules a {@link PendingSealPurge} for the pre-live orphan
     * {@code sealTxn} unless one is already enqueued. Returns {@code true} if
     * a fresh entry was added, {@code false} if the sealTxn was already known.
     * The dedup is O(N) over {@link #orphanSealTxns}, which is fine — N is the
     * number of distinct orphan generations on disk, typically &lt; 10.
     */
    private boolean rememberOrphan(long sealTxn, long postingColumnNameTxn) {
        for (int i = 0, n = orphanSealTxns.size(); i < n; i++) {
            if (orphanSealTxns.getQuick(i) == sealTxn) {
                return false;
            }
        }
        orphanSealTxns.add(sealTxn);
        PendingSealPurge entry = pendingPurgePool.size() > 0
                ? pendingPurgePool.popLast()
                : new PendingSealPurge();
        // Conservative scoreboard window: from column-instance creation to
        // forever. Operator runs this when the table has no readers in the
        // lifetime range. Best we can do without persisted publish-txn info.
        entry.of(postingColumnNameTxn, sealTxn, postingColumnNameTxn, Long.MAX_VALUE, partitionTimestamp, partitionNameTxn);
        pendingPurges.add(entry);
        return true;
    }

    private void resetSpill() {
        if (!hasSpillData || spillKeyCountsAddr == 0) {
            return;
        }
        for (int i = 0; i < activeKeyCount; i++) {
            int key = activeKeyIds[i];
            if (key < spillArraysCapacity) {
                Unsafe.putInt(spillKeyCountsAddr + (long) key * Integer.BYTES, 0);
                // Keep the allocated buffer for reuse, just reset count
            }
        }
        hasSpillData = false;
    }

    private void rollbackToMaxValue(long maxValue) {
        // Rollback writes to a NEW .pv file (like seal) so concurrent readers
        // with active mmaps on the old .pv don't SIGSEGV. Allocate the rollback
        // sealTxn the same way seal() does so the new .pv stays distinct from
        // any prior sealed generation on disk.
        final long oldSealTxn = sealTxn;
        final long newSealTxn = Math.max(1, sealTxn + 1);
        reencodeAllGenerations(newSealTxn, maxValue, maxValue);
        // Skip when reencode bypassed via truncate() — that path already
        // recorded its own purge entry.
        if (sealTxn != oldSealTxn) {
            recordPostingSealPurge(oldSealTxn);
        }
    }

    private void sealFull(long newSealTxn) {
        reencodeAllGenerations(
                newSealTxn,
                keyMem.getLong(activePageOffset + PAGE_OFFSET_MAX_VALUE),
                Long.MAX_VALUE
        );
    }

    /**
     * Incremental seal: only re-encode dirty strides (those touched by sparse gens 1..N).
     * Clean strides are copied verbatim from the existing dense gen 0.
     */
    private void sealIncremental(long newSealTxn, long[] savedSidecarBufs, long[] savedSidecarSizes) {
        int sc = PostingIndexUtils.strideCount(keyCount);
        long dirtyStridesAddr = Unsafe.malloc(sc, MemoryTag.NATIVE_INDEX_READER);
        int dirtyCount;
        try {
            Unsafe.setMemory(dirtyStridesAddr, sc, (byte) 0);
            dirtyCount = 0;
            for (int g = 1; g < genCount; g++) {
                long dirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, g);
                long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                int activeKeyCount = -genKeyCount;
                long genAddr = valueMem.addressOf(genFileOffset);

                for (int i = 0; i < activeKeyCount; i++) {
                    int key = Unsafe.getInt(genAddr + (long) i * Integer.BYTES);
                    int stride = key / PostingIndexUtils.DENSE_STRIDE;
                    if (stride < sc && Unsafe.getByte(dirtyStridesAddr + stride) == 0) {
                        Unsafe.putByte(dirtyStridesAddr + stride, (byte) 1);
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
            sealFull(newSealTxn);
            return;
        }

        // Read gen 0 metadata — do NOT cache gen0Addr here because valueMem
        // may be remapped (mremap) when the seal loop extends it to write
        // new stride data. Use gen0FileOffset and recompute the address each
        // time it's needed.
        long gen0DirOffset = PostingIndexUtils.getGenDirOffset(activePageOffset, 0);
        long gen0FileOffset = keyMem.getLong(gen0DirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
        long copyBufSize = keyMem.getLong(gen0DirOffset + GEN_DIR_OFFSET_SIZE);
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
                int c = Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
                if (c > sparseMaxPerKey) sparseMaxPerKey = c;
            }
        }
        int preAllocPerKey = maxPerKey + sparseMaxPerKey * (genCount - 1);
        long perKeyBufSize = PostingIndexUtils.computeMaxEncodedSize(Math.max(preAllocPerKey, PostingIndexUtils.BLOCK_CAPACITY));
        long maxBPStrideDataSize = PostingIndexUtils.DENSE_STRIDE * perKeyBufSize;
        int maxHeaderSize = Math.max(
                PostingIndexUtils.strideDeltaHeaderSize(PostingIndexUtils.DENSE_STRIDE),
                PostingIndexUtils.strideFlatHeaderSize(PostingIndexUtils.DENSE_STRIDE)
        );
        long maxPerStride = (long) PostingIndexUtils.DENSE_STRIDE * preAllocPerKey;
        long mergedValuesSize = Math.max(maxPerStride, 1024) * Long.BYTES;
        long copyBufAllocSize = copyBufSize > 0 ? copyBufSize : 1;

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
            // so concurrent readers keep their valid mmap. The sealTxn was
            // chosen up front by seal(); use it consistently for .pv and .pc<N>.
            openSealValueFile(newSealTxn);
            long sealOffset = 0;
            sealValueMem.jumpTo(0);
            // Reserve stride index
            for (int i = 0; i < siSize; i += Long.BYTES) {
                sealValueMem.putLong(0L);
            }

            // Sidecar: the sidecarMems opened by seal() at newSealTxn are fresh
            // empty files on disk — no truncation needed. The previous-sealTxn
            // sidecar bytes were already snapshotted into savedSidecarBufs and
            // are used to copy clean stride blocks verbatim below.
            if (coverCount > 0 && sidecarMems.size() > 0) {
                incrSidecarSiBufs = new long[coverCount];
                oldSidecarBufs = savedSidecarBufs;
                oldSidecarSizes = savedSidecarSizes;
                for (int c = 0; c < coverCount; c++) {
                    MemoryMARW mem = sidecarMems.getQuick(c);
                    if (mem == null) {
                        incrSidecarSiBufs[c] = 0;
                        continue;
                    }
                    incrSidecarSiBufs[c] = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
                    for (int i = 0; i < siSize; i += Long.BYTES) {
                        mem.putLong(0L);
                    }
                }
            }

            for (int s = 0; s < sc; s++) {
                long strideOff = sealValueMem.getAppendOffset() - sealOffset - siSize;
                Unsafe.putLong(strideIndexBuf + (long) s * Long.BYTES, strideOff);

                // Recompute gen0Addr each iteration because valueMem writes
                // (putBlockOfBytes, putInt, etc.) can trigger mremap which
                // moves the mapping, invalidating cached native addresses.
                long gen0Addr = valueMem.addressOf(gen0FileOffset);

                if (Unsafe.getByte(dirtyStridesAddr + s) == 0) {
                    // Clean stride: copy verbatim from gen 0
                    copyStrideFromGen0(gen0Addr, gen0KeyCount, gen0SiSize, s, copyBuf, copyBufSize);
                    // Sidecar: copy old stride block verbatim
                    if (incrSidecarSiBufs != null && oldSidecarBufs != null) {
                        int oldSiSize = PostingIndexUtils.strideIndexSize(gen0KeyCount);
                        long oldStrideIdxBase = PostingIndexUtils.PC_HEADER_SIZE;
                        for (int c = 0; c < coverCount; c++) {
                            if (incrSidecarSiBufs[c] == 0) continue;
                            MemoryMARW mem = sidecarMems.getQuick(c);
                            Unsafe.putLong(
                                    incrSidecarSiBufs[c] + (long) s * Long.BYTES,
                                    mem.getAppendOffset() - siSize);
                            if (oldSidecarBufs[c] != 0 && oldSidecarSizes[c] > oldStrideIdxBase + oldSiSize) {
                                long oldStrideOff = Unsafe.getLong(oldSidecarBufs[c] + oldStrideIdxBase + (long) s * Long.BYTES);
                                long nextStrideOff;
                                if (s + 1 < sc) {
                                    nextStrideOff = Unsafe.getLong(oldSidecarBufs[c] + oldStrideIdxBase + (long) (s + 1) * Long.BYTES);
                                } else {
                                    nextStrideOff = oldSidecarSizes[c] - oldSiSize;
                                }
                                long strideDataSize = nextStrideOff - oldStrideOff;
                                if (strideDataSize > 0) {
                                    long oldStrideDataAddr = oldSidecarBufs[c] + oldSiSize + oldStrideOff;
                                    mem.putBlockOfBytes(oldStrideDataAddr, strideDataSize);
                                }
                            }
                        }
                    }
                } else {
                    // Dirty stride: decode from gen 0 + sparse gens, merge, re-encode
                    int ks = PostingIndexUtils.keysInStride(keyCount, s);
                    encodeDirtyStride(s, ks, gen0Addr, gen0KeyCount, gen0SiSize,
                            bpTrialBuf, localHeaderBuf, strideBpKeySizes, mergedValuesAddr);

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
                                if (incrSidecarSiBufs[c] == 0) continue;
                                Unsafe.putLong(
                                        incrSidecarSiBufs[c] + (long) s * Long.BYTES,
                                        sidecarMems.getQuick(c).getAppendOffset() - siSize);
                            }
                            writeSidecarStrideData(ks, strideKeyCounts, strideKeyOffsets,
                                    mergedValuesAddr, incrSidecarBuf);
                        } else {
                            for (int c = 0; c < coverCount; c++) {
                                if (incrSidecarSiBufs[c] == 0) continue;
                                Unsafe.putLong(
                                        incrSidecarSiBufs[c] + (long) s * Long.BYTES,
                                        sidecarMems.getQuick(c).getAppendOffset() - siSize);
                            }
                        }
                    }
                }
            }

            // Sentinel
            long totalStrideBlocksSize = sealValueMem.getAppendOffset() - sealOffset - siSize;
            Unsafe.putLong(strideIndexBuf + (long) sc * Long.BYTES, totalStrideBlocksSize);

            // Copy stride index
            long strideIndexAddr = sealValueMem.addressOf(sealOffset);
            Unsafe.copyMemory(strideIndexBuf, strideIndexAddr, siSize);

            valueMemSize = sealValueMem.getAppendOffset();

            // Finalize sidecar stride indices for incremental seal. The
            // stride_index lives right after the per-gen offset header.
            if (incrSidecarSiBufs != null) {
                for (int c = 0; c < coverCount; c++) {
                    if (incrSidecarSiBufs[c] == 0) continue;
                    MemoryMARW mem = sidecarMems.getQuick(c);
                    Unsafe.putLong(
                            incrSidecarSiBufs[c] + (long) sc * Long.BYTES,
                            mem.getAppendOffset() - siSize);
                    long sidecarIdxAddr = mem.addressOf(PostingIndexUtils.PC_HEADER_SIZE);
                    Unsafe.copyMemory(incrSidecarSiBufs[c], sidecarIdxAddr, siSize);
                }
            }

            // Sync sealed file, switch writer to it, then publish metadata.
            // sealTxn must be set BEFORE writeMetadataPage so the
            // SEAL_TXN field in the metadata page reflects the new sealed files.
            sealValueMem.sync(false);
            // Sync covering sidecars before publishing the new sealTxn in
            // .pk. Without this, power loss can leave .pk pointing at
            // sealTxn=N+1 while .pc<N>/.pci tail pages are still dirty in
            // page cache; readers built around the new sealTxn would map a
            // torn sidecar and decode garbage.
            for (int c = 0, n = sidecarMems.size(); c < n; c++) {
                MemoryMARW mem = sidecarMems.getQuick(c);
                if (mem != null && mem.isOpen()) {
                    mem.sync(false);
                }
            }
            if (sidecarInfoMem != null && sidecarInfoMem.isOpen()) {
                sidecarInfoMem.sync(false);
            }
            switchToSealedValueFile(newSealTxn);
            Unsafe.storeFence();
            // genCount must publish after writeMetadataPage so a mid-seal failure
            // leaves the in-memory view consistent with on-disk .pk.
            writeMetadataPage(1,
                    keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE),
                    sealOffset, valueMemSize - sealOffset, keyCount, keyCount - 1);
            genCount = 1;
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
                        oldSidecarBufs[c] = 0;
                        oldSidecarSizes[c] = 0;
                    }
                }
            }
        }
    }

    private void spillKey(int key, int count) {
        ensureSpillArrays(key);
        int prevCount = Unsafe.getInt(spillKeyCountsAddr + (long) key * Integer.BYTES);
        int needed = prevCount + count;
        // Grow per-key spill buffer if needed
        int curCap = Unsafe.getInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES);
        if (needed > curCap) {
            int newCap = Math.max(needed, curCap * 2);
            newCap = Math.max(newCap, 32); // minimum 32 values
            long oldSize = (long) curCap * Long.BYTES;
            long newSize = (long) newCap * Long.BYTES;
            long oldAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
            long newAddr = Unsafe.realloc(oldAddr, oldSize, newSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.putLong(spillKeyAddrsAddr + (long) key * Long.BYTES, newAddr);
            Unsafe.putInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES, newCap);
        }
        // Copy values from pending buffer to this key's spill
        long srcAddr = pendingValuesAddr + (long) key * PENDING_SLOT_CAPACITY * Long.BYTES;
        long spillAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
        Unsafe.copyMemory(srcAddr, spillAddr + (long) prevCount * Long.BYTES,
                (long) count * Long.BYTES);
        Unsafe.putInt(spillKeyCountsAddr + (long) key * Integer.BYTES, needed);
        hasSpillData = true;
        // Reset pending count
        Unsafe.putInt(pendingCountsAddr + (long) key * Integer.BYTES, 0);
    }

    private void switchToSealedValueFile(long newTxn) {
        // fd-based seal does not reach this method: reencodeWithStrideDecoding
        // memmoves output in place rather than swapping in a new .pv file, and
        // reencodeMonolithic for fd-based rollback does its own valueMem reuse
        // inline. Any fd-based caller would silently advance sealTxn here while
        // the on-disk .pv stayed at the old txn — fail loud in dev builds.
        assert partitionPath.size() > 0 : "switchToSealedValueFile called in fd-based mode";
        long appendOffset = sealValueMem.getAppendOffset();
        Path p = Path.getThreadLocal(partitionPath);
        LPSZ fileName = PostingIndexUtils.valueFileName(p, indexName, postingColumnNameTxn, newTxn);
        long sealedFd = sealValueMem.detachFdClose();
        try {
            stagingValueMem.of(ff, sealedFd, false, fileName,
                    configuration.getDataIndexValueAppendPageSize(),
                    appendOffset, MemoryTag.MMAP_INDEX_WRITER);
            stagingValueMem.jumpTo(appendOffset);
        } catch (Throwable th) {
            // sealedFd ownership has already transferred to stagingValueMem (or
            // map0's internal close already released it). Misc.free handles both;
            // do NOT close sealedFd here or FdCache trips a double-close assertion.
            Misc.free(stagingValueMem);
            throw th;
        }
        ((MemoryCMARWImpl) valueMem).swapState((MemoryCMARWImpl) stagingValueMem);
        Misc.free(stagingValueMem);
        sealTxn = newTxn;
    }

    private void unmapCoveredColumn(int c) {
        if (coveredColReadAddrs != null && coveredColReadAddrs[c] != 0 && coveredColReadSizes[c] > 0) {
            Files.munmap(coveredColReadAddrs[c], coveredColReadSizes[c], MemoryTag.MMAP_INDEX_WRITER);
            coveredColReadAddrs[c] = 0;
            coveredColReadSizes[c] = 0;
        }
        if (coveredAuxReadAddrs != null && coveredAuxReadAddrs[c] != 0 && coveredAuxReadSizes[c] > 0) {
            Files.munmap(coveredAuxReadAddrs[c], coveredAuxReadSizes[c], MemoryTag.MMAP_INDEX_WRITER);
            coveredAuxReadAddrs[c] = 0;
            coveredAuxReadSizes[c] = 0;
        }
    }

    private void unmapCoveredColumnReads() {
        if (coveredColReadAddrs != null) {
            for (int c = 0; c < coveredColReadAddrs.length; c++) {
                // Only unmap if owned (size > 0). Size == 0 means O3 addr path.
                if (coveredColReadAddrs[c] != 0 && coveredColReadSizes[c] > 0) {
                    Files.munmap(coveredColReadAddrs[c], coveredColReadSizes[c], MemoryTag.MMAP_INDEX_WRITER);
                }
                coveredColReadAddrs[c] = 0;
                coveredColReadSizes[c] = 0;
                if (coveredAuxReadAddrs != null && coveredAuxReadAddrs[c] != 0 && coveredAuxReadSizes[c] > 0) {
                    Files.munmap(coveredAuxReadAddrs[c], coveredAuxReadSizes[c], MemoryTag.MMAP_INDEX_WRITER);
                }
                if (coveredAuxReadAddrs != null) {
                    coveredAuxReadAddrs[c] = 0;
                    coveredAuxReadSizes[c] = 0;
                }
            }
            coveredColReadAddrs = null;
            coveredColReadSizes = null;
            coveredAuxReadAddrs = null;
            coveredAuxReadSizes = null;
        }
    }

    private void writeBinaryLikeValue(MemoryMARW mem, int covIdx, long row) {
        if (isClosed()) {
            return;
        }
        int colType = coveredColumnTypes.getQuick(covIdx);
        int tag = ColumnType.tagOf(colType);
        if (tag == ColumnType.BINARY) {
            // BINARY aux: 8-byte offset per row. Data: [8-byte length][raw bytes]
            long auxAddr = getCoveredAuxReadAddr(covIdx, row << 3, Long.BYTES);
            if (auxAddr == 0) return;
            long dataOffset = Unsafe.getLong(auxAddr);

            long lenAddr = getCoveredDataReadAddr(covIdx, dataOffset, Long.BYTES);
            if (lenAddr == 0) return;
            long len = Unsafe.getLong(lenAddr);
            if (len < 0) return; // NULL

            long totalBytes = Long.BYTES + len;
            long dataAddr = getCoveredDataReadAddr(covIdx, dataOffset, totalBytes);
            if (dataAddr != 0) {
                mem.putBlockOfBytes(dataAddr, totalBytes);
            }
        } else {
            // Arrays: aux is ARRAY_AUX_WIDTH_BYTES per row [8-byte offset][8-byte size]
            long auxOffset = (long) ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES * row;
            long auxAddr = getCoveredAuxReadAddr(covIdx, auxOffset, ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES);
            if (auxAddr == 0) return;
            long dataOffset = Unsafe.getLong(auxAddr);
            long size = Unsafe.getLong(auxAddr + Long.BYTES);
            if (size <= 0) return; // NULL or empty

            long dataAddr = getCoveredDataReadAddr(covIdx, dataOffset, size);
            if (dataAddr != 0) {
                mem.putBlockOfBytes(dataAddr, size);
            }
        }
    }

    private void writeDeltaStride(int ks,
                                  int[] keyCounts,
                                  int deltaHeaderSize,
                                  long bpTrialBuf,
                                  int[] bpKeySizes,
                                  long localHeaderBuf
    ) {
        long headerFilePos = sealTarget.getAppendOffset();
        for (int i = 0; i < deltaHeaderSize; i += Integer.BYTES) {
            sealTarget.putInt(0);
        }

        Unsafe.setMemory(localHeaderBuf, deltaHeaderSize, (byte) 0);
        Unsafe.putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_DELTA);
        long countsBase = localHeaderBuf + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
        long offsetsBase = countsBase + (long) ks * Integer.BYTES;

        long dataOffset = 0;
        int bpBufOffset = 0;
        for (int j = 0; j < ks; j++) {
            Unsafe.putInt(countsBase + (long) j * Integer.BYTES, keyCounts[j]);
            Unsafe.putLong(offsetsBase + (long) j * Long.BYTES, dataOffset);

            if (bpKeySizes[j] > 0) {
                int bytesWritten = bpKeySizes[j];
                sealTarget.putBlockOfBytes(bpTrialBuf + bpBufOffset, bytesWritten);
                dataOffset += bytesWritten;
            }
            bpBufOffset += bpKeySizes[j];
        }

        Unsafe.putLong(offsetsBase + (long) ks * Long.BYTES, dataOffset);

        long headerAddr = sealTarget.addressOf(headerFilePos);
        Unsafe.copyMemory(localHeaderBuf, headerAddr, deltaHeaderSize);
    }

    /**
     * Writes a new metadata page to the inactive page (double-buffer protocol).
     * Copies gen dir entries from the active page, with an optional override for one entry.
     *
     * @param genCount           number of generations
     * @param maxValue           max value to write
     * @param overrideFileOffset file offset for the overridden gen dir entry
     * @param overrideSize       size for the overridden gen dir entry
     * @param overrideKeyCount   key count for the overridden gen dir entry
     * @param overrideMaxKey     max key for the overridden gen dir entry
     */
    private void writeMetadataPage(int genCount, long maxValue,
                                   long overrideFileOffset,
                                   long overrideSize, int overrideKeyCount,
                                   int overrideMaxKey) {
        writeMetadataPage(genCount, maxValue, 0, overrideFileOffset,
                overrideSize, overrideKeyCount, 0, overrideMaxKey);
    }

    private void writeMetadataPage(int genCount, long maxValue,
                                   int overrideGenIndex, long overrideFileOffset,
                                   long overrideSize, int overrideKeyCount,
                                   int overrideMinKey, int overrideMaxKey) {
        // Pick the slot to write. Tentative mode pins the slot for the whole
        // fd-based session so repeated flushes don't clobber the committed
        // page; non-tentative mode flips A/B as usual.
        long inactivePageOffset;
        if (deferMetadataPublish) {
            if (tentativeSlotOffset == -1L) {
                tentativeSlotOffset = (activePageOffset == PostingIndexUtils.PAGE_A_OFFSET)
                        ? PostingIndexUtils.PAGE_B_OFFSET
                        : PostingIndexUtils.PAGE_A_OFFSET;
            }
            inactivePageOffset = tentativeSlotOffset;
        } else {
            inactivePageOffset = (activePageOffset == PostingIndexUtils.PAGE_A_OFFSET)
                    ? PostingIndexUtils.PAGE_B_OFFSET
                    : PostingIndexUtils.PAGE_A_OFFSET;
        }

        long currentSeq = keyMem.getLong(activePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        long newSeq = currentSeq + 1;

        // Seqlock: write sequence_start first, then fields, then sequence_end.
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, newSeq);
        Unsafe.storeFence();

        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        keyMem.putInt(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_BLOCK_CAPACITY, blockCapacity);
        keyMem.putInt(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT, keyCount);
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_MAX_VALUE, maxValue);
        keyMem.putInt(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT, genCount);
        keyMem.putInt(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_FORMAT_VERSION, FORMAT_VERSION);
        keyMem.putLong(
                inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN,
                deferMetadataPublish ? PostingIndexUtils.SEAL_TXN_TENTATIVE : sealTxn
        );

        // Bulk copy gen dir entries that already exist on the active page, then
        // overwrite or append the new entry. The active page has (genCount - 1) valid
        // entries when the caller incremented genCount before calling us.
        int activePageGenCount = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);
        int entriesToCopy = Math.min(genCount, activePageGenCount);
        if (entriesToCopy > 0) {
            long srcGenDir = keyMem.addressOf(activePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_DIR);
            long dstGenDir = keyMem.addressOf(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_GEN_DIR);
            int genDirBytes = entriesToCopy * PostingIndexUtils.GEN_DIR_ENTRY_SIZE;
            Unsafe.copyMemory(srcGenDir, dstGenDir, genDirBytes);
        }
        if (overrideGenIndex >= 0) {
            long dstOffset = PostingIndexUtils.getGenDirOffset(inactivePageOffset, overrideGenIndex);
            keyMem.putLong(dstOffset + GEN_DIR_OFFSET_FILE_OFFSET, overrideFileOffset);
            keyMem.putLong(dstOffset + GEN_DIR_OFFSET_SIZE, overrideSize);
            keyMem.putInt(dstOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT, overrideKeyCount);
            keyMem.putInt(dstOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY, overrideMinKey);
            keyMem.putInt(dstOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, overrideMaxKey);
        }

        Unsafe.storeFence();
        keyMem.putLong(inactivePageOffset + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END, newSeq);

        activePageOffset = inactivePageOffset;
    }

    private void writePackedStride(int ks, int[] keyCounts, long[] keyOffsets,
                                   int localBitWidth, long strideMinValue, int flatHeaderSize, int flatDataSize,
                                   long localHeaderBuf, long mergedValuesAddr) {
        long headerFilePos = sealTarget.getAppendOffset();
        for (int i = 0; i < flatHeaderSize; i += Integer.BYTES) {
            sealTarget.putInt(0);
        }

        Unsafe.setMemory(localHeaderBuf, flatHeaderSize, (byte) 0);
        Unsafe.putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_FLAT);
        Unsafe.putByte(localHeaderBuf + 1, (byte) localBitWidth);
        Unsafe.putLong(localHeaderBuf + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET, strideMinValue);
        int cumCount = 0;
        for (int j = 0; j <= ks; j++) {
            Unsafe.putInt(
                    localHeaderBuf + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET + (long) j * Integer.BYTES,
                    cumCount);
            if (j < ks) {
                cumCount += keyCounts[j];
            }
        }

        long packedBuf = Unsafe.malloc(flatDataSize > 0 ? flatDataSize : 1, MemoryTag.NATIVE_INDEX_READER);
        try {
            if (flatDataSize > 0) {
                Unsafe.setMemory(packedBuf, flatDataSize, (byte) 0);
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
                        Unsafe.putLong(packedResidualsAddr + (long) idx * Long.BYTES,
                                Unsafe.getLong(keyAddr + (long) i * Long.BYTES) - strideMinValue);
                        idx++;
                    }
                }
                PostingIndexNative.packValuesNativeFallback(packedResidualsAddr, totalValues, 0, localBitWidth, packedBuf);
            }

            sealTarget.putBlockOfBytes(packedBuf, flatDataSize);
        } finally {
            Unsafe.free(packedBuf, flatDataSize > 0 ? flatDataSize : 1, MemoryTag.NATIVE_INDEX_READER);
        }

        long headerAddr = sealTarget.addressOf(headerFilePos);
        Unsafe.copyMemory(localHeaderBuf, headerAddr, flatHeaderSize);
    }

    /**
     * Writes raw (uncompressed) covered column values for the current gen's
     * posting entries to the sidecar files. Each sidecar file gets a block:
     * [valueCount: 4B][raw values: valueCount × elemSize].
     * <p>
     * Values are written in the same order as flushAllPending() encodes posting
     * data: sorted activeKeyIds, spill values first then pending values per key.
     * This ensures the sidecar ordinal (tracked by the reader's cursor) matches
     * the posting entry position within the gen.
     */
    private void writeSidecarFixedStrideForColumn(
            MemoryMARW mem, int c, long colTop, int colType, int shift,
            int ks, int[] keyCounts, long[] keyOffsets, long mergedValuesAddr,
            long sidecarBuf, long longWorkspaceAddr, long exceptionWorkspaceAddr
    ) {
        int valueSize = 1 << shift;

        // Per-key compressed layout: [key_offsets: ks x 8B][key_0_block][key_1_block]...
        int keyOffsetsSize = ks * Long.BYTES;

        // Pre-allocate compress buffer for the largest key in this column type
        int maxKeyCount = 0;
        for (int j = 0; j < ks; j++) {
            maxKeyCount = Math.max(maxKeyCount, keyCounts[j]);
        }
        int compressBufSize = maxKeyCount > 0 ? CoveringCompressor.maxCompressedSize(maxKeyCount, colType) : 0;
        long compressBuf = compressBufSize > 0 ? Unsafe.malloc(compressBufSize, MemoryTag.NATIVE_INDEX_READER) : 0;

        try {
            // Write key offsets placeholder, then compress each key's values
            long keyOffsetsPos = mem.getAppendOffset();
            for (int j = 0; j < ks; j++) {
                mem.putLong(0L); // placeholder
            }

            for (int j = 0; j < ks; j++) {
                int count = keyCounts[j];
                long currentPos = mem.getAppendOffset();
                long keyDataStart = keyOffsetsPos + keyOffsetsSize;
                long keyOffset = currentPos - keyDataStart;
                mem.putLong(keyOffsetsPos + (long) j * Long.BYTES, keyOffset);

                if (count == 0) {
                    continue;
                }

                // Assemble this key's raw values into sidecarBuf.
                long keyOff = keyOffsets[j];
                long rawOffset = 0;
                for (int i = 0; i < count; i++) {
                    long rowId = Unsafe.getLong(
                            mergedValuesAddr + (keyOff + i) * Long.BYTES);
                    if (rowId < colTop) {
                        writeNullSentinel(sidecarBuf + rawOffset, valueSize, colType);
                    } else {
                        long srcOffset = (rowId - colTop) << shift;
                        long addr = getCoveredDataReadAddr(c, srcOffset, valueSize);
                        if (addr != 0) {
                            Unsafe.copyMemory(addr, sidecarBuf + rawOffset, valueSize);
                        } else {
                            writeNullSentinel(sidecarBuf + rawOffset, valueSize, colType);
                        }
                    }
                    rawOffset += valueSize;
                }

                // Compress and write
                boolean isDesignatedTs = timestampColumnIndex >= 0
                        && coveredColumnIndices.getQuick(c) == timestampColumnIndex;
                int compressedSize = compressSidecarBlock(sidecarBuf, count, shift, colType,
                        isDesignatedTs, compressBuf, longWorkspaceAddr, exceptionWorkspaceAddr);
                mem.putBlockOfBytes(compressBuf, compressedSize);
            }
        } finally {
            if (compressBuf != 0) {
                Unsafe.free(compressBuf, compressBufSize, MemoryTag.NATIVE_INDEX_READER);
            }
        }
    }

    private void writeSidecarForColumn(
            int c, int sc, int siSize,
            long totalCountsAddr, long strideValsAddr,
            int globalMaxKeyCount
    ) {
        MemoryMARW mem = sidecarMems.getQuick(c);
        int colType = coveredColumnTypes.getQuick(c);
        int shift = coveredColumnShifts.getQuick(c);
        long colTop = coveredColumnTops.getQuick(c);
        int[] keyCounts = strideKeyCounts;
        long[] keyOffsets = strideKeyOffsets;

        long sidecarStrideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
        for (int i = 0; i < siSize; i += Long.BYTES) {
            mem.putLong(0L); // stride index placeholders
        }

        long sidecarBuf = 0;
        long sidecarBufSize = 0;
        long longWorkspaceSize = (long) globalMaxKeyCount * Long.BYTES;
        long longWorkspaceAddr = shift >= 0 && globalMaxKeyCount > 0
                ? Unsafe.malloc(longWorkspaceSize, MemoryTag.NATIVE_INDEX_READER) : 0;
        long exceptionWorkspaceAddr = shift >= 0 && globalMaxKeyCount > 0
                ? Unsafe.malloc(globalMaxKeyCount, MemoryTag.NATIVE_INDEX_READER) : 0;

        try {
            for (int s = 0; s < sc; s++) {
                int ks = PostingIndexUtils.keysInStride(keyCount, s);
                int strideStart = s * PostingIndexUtils.DENSE_STRIDE;

                // Compute per-key counts and base offsets within stride buffer
                int strideValCount = 0;
                for (int j = 0; j < ks; j++) {
                    int key = strideStart + j;
                    int count = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    keyCounts[j] = count;
                    keyOffsets[j] = strideValCount;
                    strideValCount += count;
                }

                Unsafe.putLong(
                        sidecarStrideIndexBuf + (long) s * Long.BYTES,
                        mem.getAppendOffset() - siSize);

                if (strideValCount == 0) {
                    continue;
                }

                // Decode row IDs from sealed output (single dense generation)
                decodeDenseGenStride(valueMem.addressOf(0), keyCount, s, ks, ks, strideValsAddr, keyOffsets);

                // Restore base offsets (decode advanced them)
                long off = 0;
                for (int j = 0; j < ks; j++) {
                    keyOffsets[j] = off;
                    off += keyCounts[j];
                }

                if (shift < 0) {
                    writeSidecarVarStrideData(mem, c, colTop, colType, ks, keyCounts, keyOffsets, strideValsAddr);
                } else {
                    int valueSize = 1 << shift;
                    long needed = (long) strideValCount * valueSize;
                    if (needed > sidecarBufSize) {
                        if (sidecarBuf != 0) {
                            Unsafe.free(sidecarBuf, sidecarBufSize, MemoryTag.NATIVE_INDEX_READER);
                        }
                        sidecarBufSize = needed;
                        sidecarBuf = Unsafe.malloc(sidecarBufSize, MemoryTag.NATIVE_INDEX_READER);
                    }
                    writeSidecarFixedStrideForColumn(mem, c, colTop, colType, shift,
                            ks, keyCounts, keyOffsets, strideValsAddr, sidecarBuf, longWorkspaceAddr, exceptionWorkspaceAddr);
                }
            }

            Unsafe.putLong(
                    sidecarStrideIndexBuf + (long) sc * Long.BYTES,
                    mem.getAppendOffset() - siSize);
            long sidecarIdxAddr = mem.addressOf(PostingIndexUtils.PC_HEADER_SIZE);
            Unsafe.copyMemory(sidecarStrideIndexBuf, sidecarIdxAddr, siSize);
        } finally {
            Unsafe.free(sidecarStrideIndexBuf, siSize, MemoryTag.NATIVE_INDEX_READER);
            if (sidecarBuf != 0) {
                Unsafe.free(sidecarBuf, sidecarBufSize, MemoryTag.NATIVE_INDEX_READER);
            }
            if (longWorkspaceAddr != 0) {
                Unsafe.free(longWorkspaceAddr, longWorkspaceSize, MemoryTag.NATIVE_INDEX_READER);
            }
            if (exceptionWorkspaceAddr != 0) {
                Unsafe.free(exceptionWorkspaceAddr, globalMaxKeyCount, MemoryTag.NATIVE_INDEX_READER);
            }
        }
    }

    private void writeSidecarGenData(int totalValues, int genIndex) {
        if (coverCount <= 0 || totalValues == 0
                || (coveredColumnNames.size() == 0 && coveredColumnAddrs.size() == 0)) {
            return;
        }
        // Lazily open sidecar files on first gen flush.
        // Use append mode to preserve existing data (e.g., stride-indexed
        // sidecar from a prior seal). openSidecarFiles() truncates, which
        // is correct for seal but wrong here.
        if (sidecarMems.size() == 0 && partitionPath.size() > 0) {
            openSidecarFilesForAppend(Path.getThreadLocal(partitionPath), indexName, postingColumnNameTxn, sealTxn);
        }
        if (sidecarMems.size() == 0) {
            return;
        }

        for (int c = 0; c < coverCount; c++) {
            if (coveredColumnIndices.getQuick(c) < 0) {
                continue;
            }
            MemoryMARW mem = sidecarMems.getQuick(c);
            int colType = coveredColumnTypes.getQuick(c);
            long colTop = coveredColumnTops.getQuick(c);
            long blockStart = mem.getAppendOffset();

            if (ColumnType.isVarSize(colType)) {
                writeSidecarVarGenBlock(mem, c, colTop, colType, totalValues);
            } else {
                int shift = coveredColumnShifts.getQuick(c);
                int valueSize = 1 << shift;

                mem.putInt(totalValues);

                for (int idx = 0; idx < activeKeyCount; idx++) {
                    int key = activeKeyIds[idx];
                    int pendingCount = Unsafe.getInt(pendingCountsAddr + (long) key * Integer.BYTES);
                    int spillCount = getSpillCount(key);

                    if (spillCount > 0) {
                        long spillAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                        for (int i = 0; i < spillCount; i++) {
                            long rowId = Unsafe.getLong(spillAddr + (long) i * Long.BYTES);
                            writeSidecarValueSafe(mem, c, colTop, rowId, shift, valueSize, colType);
                        }
                    }

                    long keyValuesAddr = pendingValuesAddr + (long) key * PENDING_SLOT_CAPACITY * Long.BYTES;
                    for (int i = 0; i < pendingCount; i++) {
                        long rowId = Unsafe.getLong(keyValuesAddr + (long) i * Long.BYTES);
                        writeSidecarValueSafe(mem, c, colTop, rowId, shift, valueSize, colType);
                    }
                }
            }
            mem.putLong((long) genIndex * Long.BYTES, blockStart);
        }
    }

    private void writeSidecarStrideData(
            int ks,
            int[] keyCounts,
            long[] keyOffsets,
            long mergedValuesAddr,
            long sidecarBuf
    ) {
        if (coverCount <= 0 || sidecarMems.size() == 0 || (coveredColumnNames.size() == 0 && coveredColumnAddrs.size() == 0)) {
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
                int colType = coveredColumnTypes.getQuick(c);
                int shift = coveredColumnShifts.getQuick(c);

                // Var-sized columns: write per-stride offset-based block (no ALP compression).
                // Fixed-sized columns: ALP-compressed per-key blocks with stride index.
                if (shift < 0) {
                    writeSidecarVarStrideData(sidecarMems.getQuick(c), c, coveredColumnTops.getQuick(c), colType, ks, keyCounts, keyOffsets, mergedValuesAddr);
                } else {
                    writeSidecarFixedStrideForColumn(sidecarMems.getQuick(c), c, coveredColumnTops.getQuick(c), colType, shift,
                            ks, keyCounts, keyOffsets, mergedValuesAddr, sidecarBuf, longWorkspaceAddr, exceptionWorkspaceAddr);
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

    private void writeSidecarValueSafe(
            MemoryMARW mem,
            int covIdx,
            long colTop,
            long rowId,
            int shift,
            int valueSize,
            int colType
    ) {
        if (rowId < colTop) {
            writeNullSentinel(mem, valueSize, colType);
        } else {
            long srcOffset = (rowId - colTop) << shift;
            if (coveredColumnNames.size() > 0 || coveredColumnAddrs.size() > 0) {
                long addr = getCoveredDataReadAddr(covIdx, srcOffset, valueSize);
                if (addr == 0) {
                    writeNullSentinel(mem, valueSize, colType);
                    return;
                }
                putFixedValue(mem, addr, valueSize);
            } else {
                putFixedValue(mem, srcOffset, valueSize);
            }
        }
    }

    /**
     * Writes a per-gen sidecar block for a var-sized column (VARCHAR or STRING).
     * Format: [count:4B][offsets: (count+1) x (4B|8B)][concatenated raw bytes]
     * <p>
     * Offset width defaults to 4B. If the block's total raw data exceeds 2 GB,
     * the writer rewinds and re-emits with 8B offsets, setting LONG_OFFSETS_FLAG
     * in the count header.
     * <p>
     * NULL values have offset[i] == offset[i+1] (zero-length) with a separate
     * null bitmap not needed — the reader checks for zero-length spans.
     */
    private void writeSidecarVarGenBlock(MemoryMARW mem, int covIdx, long colTop, int colType, int totalValues) {
        long blockStart = mem.getAppendOffset();
        if (!writeSidecarVarGenBlockAttempt(mem, covIdx, colTop, colType, totalValues, false)) {
            // int offset would have overflowed — rewind and re-emit with long offsets.
            mem.jumpTo(blockStart);
            boolean ok = writeSidecarVarGenBlockAttempt(mem, covIdx, colTop, colType, totalValues, true);
            assert ok : "long offsets must not overflow";
        }
    }

    private boolean writeSidecarVarGenBlockAttempt(
            MemoryMARW mem, int covIdx, long colTop, int colType, int totalValues, boolean longOffsets
    ) {
        mem.putInt(longOffsets ? totalValues | PostingIndexUtils.LONG_OFFSETS_FLAG : totalValues);
        long offsetsStart = mem.getAppendOffset();
        for (int i = 0; i <= totalValues; i++) {
            if (longOffsets) mem.putLong(0L);
            else mem.putInt(0);
        }
        long dataStart = mem.getAppendOffset();

        int valueOrdinal = 0;

        for (int idx = 0; idx < activeKeyCount; idx++) {
            int key = activeKeyIds[idx];
            int pendingCount = Unsafe.getInt(pendingCountsAddr + (long) key * Integer.BYTES);
            int spillCount = getSpillCount(key);

            if (spillCount > 0) {
                long spillAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                for (int i = 0; i < spillCount; i++) {
                    long rowId = Unsafe.getLong(spillAddr + (long) i * Long.BYTES);
                    long off = mem.getAppendOffset() - dataStart;
                    if (!longOffsets && off > Integer.MAX_VALUE) return false;
                    writeVarOffset(mem, offsetsStart, valueOrdinal, off, longOffsets);
                    writeVarValue(mem, covIdx, colTop, rowId, colType);
                    valueOrdinal++;
                }
            }

            long keyValuesAddr = pendingValuesAddr + (long) key * PENDING_SLOT_CAPACITY * Long.BYTES;
            for (int i = 0; i < pendingCount; i++) {
                long rowId = Unsafe.getLong(keyValuesAddr + (long) i * Long.BYTES);
                long off = mem.getAppendOffset() - dataStart;
                if (!longOffsets && off > Integer.MAX_VALUE) return false;
                writeVarOffset(mem, offsetsStart, valueOrdinal, off, longOffsets);
                writeVarValue(mem, covIdx, colTop, rowId, colType);
                valueOrdinal++;
            }
        }
        // Sentinel offset
        long endOff = mem.getAppendOffset() - dataStart;
        if (!longOffsets && endOff > Integer.MAX_VALUE) return false;
        writeVarOffset(mem, offsetsStart, valueOrdinal, endOff, longOffsets);
        return true;
    }

    /**
     * Writes var-sized sidecar data for one stride in the sealed path.
     * <p>
     * Uncompressed format: [totalCount:4B][offsets:(totalCount+1)×W][concatenated bytes]
     * <p>
     * FSST-compressed format (count high bit set):
     * [totalCount|FSST_BLOCK_FLAG|LONG_OFFSETS_FLAG?:4B][tableLen:2B][FSST table]
     * [offsets:(count+1)×W][compressed bytes]
     * <p>
     * Offset width W is 4 bytes by default; 8 bytes when the block sets LONG_OFFSETS_FLAG
     * (either uncompressed data span or compressed data span exceeds 2 GB).
     */
    private void writeSidecarVarStrideData(
            MemoryMARW mem, int covIdx, long colTop, int colType,
            int ks, int[] keyCounts, long[] keyOffsets, long mergedValuesAddr
    ) {
        int totalCount = 0;
        for (int j = 0; j < ks; j++) {
            totalCount += keyCounts[j];
        }

        long blockStart = mem.getAppendOffset();
        boolean longOffsets = false;
        if (!writeVarStrideDataAttempt(mem, covIdx, colTop, colType, ks, keyCounts, keyOffsets, mergedValuesAddr, totalCount, false)) {
            mem.jumpTo(blockStart);
            longOffsets = true;
            boolean ok = writeVarStrideDataAttempt(mem, covIdx, colTop, colType, ks, keyCounts, keyOffsets, mergedValuesAddr, totalCount, true);
            assert ok : "long offsets must not overflow";
        }
        int offsetWidth = longOffsets ? Long.BYTES : Integer.BYTES;
        long offsetsStart = blockStart + Integer.BYTES;
        long dataStart = offsetsStart + (long) (totalCount + 1) * offsetWidth;

        long rawDataLen = mem.getAppendOffset() - dataStart;
        if (rawDataLen < FSST_MIN_RAW_SIZE || totalCount == 0) {
            return;
        }

        long rawDataAddr = mem.addressOf(dataStart);
        long offsetsArrayBytes = (long) (totalCount + 1) * Long.BYTES;
        long cmpCap = rawDataLen * 2 + 16;
        long batchScratchBytes = (long) totalCount * FSSTNative.BATCH_SCRATCH_BYTES_PER_VALUE;
        if (fsstSrcOffsCap < offsetsArrayBytes) {
            fsstSrcOffsAddr = Unsafe.realloc(fsstSrcOffsAddr, fsstSrcOffsCap, offsetsArrayBytes, MemoryTag.NATIVE_INDEX_READER);
            fsstSrcOffsCap = offsetsArrayBytes;
        }
        if (fsstCmpCap < cmpCap) {
            fsstCmpAddr = Unsafe.realloc(fsstCmpAddr, fsstCmpCap, cmpCap, MemoryTag.NATIVE_INDEX_READER);
            fsstCmpCap = cmpCap;
        }
        if (fsstCmpOffsCap < offsetsArrayBytes) {
            fsstCmpOffsAddr = Unsafe.realloc(fsstCmpOffsAddr, fsstCmpOffsCap, offsetsArrayBytes, MemoryTag.NATIVE_INDEX_READER);
            fsstCmpOffsCap = offsetsArrayBytes;
        }
        if (fsstBatchScratchCap < batchScratchBytes) {
            fsstBatchScratchAddr = Unsafe.realloc(fsstBatchScratchAddr, fsstBatchScratchCap, batchScratchBytes, MemoryTag.NATIVE_INDEX_READER);
            fsstBatchScratchCap = batchScratchBytes;
        }
        if (fsstTableAddr == 0) {
            fsstTableAddr = Unsafe.malloc(FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_INDEX_READER);
        }

        for (int i = 0; i <= totalCount; i++) {
            long off = longOffsets
                    ? Unsafe.getLong(mem.addressOf(offsetsStart + (long) i * Long.BYTES))
                    : Unsafe.getInt(mem.addressOf(offsetsStart + (long) i * Integer.BYTES)) & 0xFFFFFFFFL;
            Unsafe.putLong(fsstSrcOffsAddr + (long) i * Long.BYTES, off);
        }

        long packed = FSSTNative.trainAndCompressBlock(
                rawDataAddr, fsstSrcOffsAddr, totalCount,
                fsstCmpAddr, fsstCmpCap, fsstCmpOffsAddr,
                fsstTableAddr, fsstBatchScratchAddr
        );
        if (packed < 0) {
            LOG.info().$("FSST compression skipped [covIdx=").$(covIdx)
                    .$(", totalCount=").$(totalCount)
                    .$(", rawDataLen=").$(rawDataLen)
                    .$(']').$();
            return;
        }
        long cmpPos = FSSTNative.unpackCompressed(packed);
        int tableLen = FSSTNative.unpackTableLen(packed);

        boolean fsstLongOffsets = cmpPos > Integer.MAX_VALUE;
        int fsstOffsetWidth = fsstLongOffsets ? Long.BYTES : Integer.BYTES;
        long compressedBlockSize = 4 + 2 + tableLen + (long) (totalCount + 1) * fsstOffsetWidth + cmpPos;
        long uncompressedBlockSize = mem.getAppendOffset() - blockStart;
        if (compressedBlockSize >= uncompressedBlockSize) {
            return;
        }

        mem.jumpTo(blockStart);
        int flags = FSSTNative.FSST_BLOCK_FLAG | (fsstLongOffsets ? PostingIndexUtils.LONG_OFFSETS_FLAG : 0);
        mem.putInt(totalCount | flags);
        mem.putShort((short) tableLen);
        for (int i = 0; i < tableLen; i++) {
            mem.putByte(Unsafe.getByte(fsstTableAddr + i));
        }
        if (fsstLongOffsets) {
            for (int i = 0; i <= totalCount; i++) {
                mem.putLong(Unsafe.getLong(fsstCmpOffsAddr + (long) i * Long.BYTES));
            }
        } else {
            for (int i = 0; i <= totalCount; i++) {
                mem.putInt((int) Unsafe.getLong(fsstCmpOffsAddr + (long) i * Long.BYTES));
            }
        }
        mem.putBlockOfBytes(fsstCmpAddr, cmpPos);
    }

    private void writeSidecarsPerColumn(long totalCountsAddr, long strideValsAddr) {
        // Unmap any prior full mapping, then map one column at a time to
        // reduce peak memory from coverCount × columnSize to 1 × columnSize.
        unmapCoveredColumnReads();

        int sc = PostingIndexUtils.strideCount(keyCount);
        int siSize = PostingIndexUtils.strideIndexSize(keyCount);

        // Compute global max key count once for workspace sizing
        int globalMaxKeyCount = 0;
        for (int key = 0; key < keyCount; key++) {
            globalMaxKeyCount = Math.max(globalMaxKeyCount,
                    Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES));
        }

        if (coveredColumnNames.size() > 0 && coveredPartitionPath.size() > 0) {
            Path p = Path.getThreadLocal(coveredPartitionPath);
            for (int c = 0; c < coverCount; c++) {
                if (coveredColumnIndices.getQuick(c) < 0) {
                    continue;
                }
                try {
                    mapCoveredColumn(p, c);
                    writeSidecarForColumn(c, sc, siSize, totalCountsAddr, strideValsAddr, globalMaxKeyCount);
                } finally {
                    unmapCoveredColumn(c);
                }
            }
        } else if (coveredColumnAddrs.size() > 0) {
            // O3 addr-based path: all addresses provided by caller, no per-column mapping needed
            ensureCoveredColumnReadMaps();
            for (int c = 0; c < coverCount; c++) {
                writeSidecarForColumn(c, sc, siSize, totalCountsAddr, strideValsAddr, globalMaxKeyCount);
            }
        }
    }

    private void writeStringValue(MemoryMARW mem, int covIdx, long row) {
        if (isClosed()) {
            return;
        }
        long auxAddr = getCoveredAuxReadAddr(covIdx, row << 3, Long.BYTES);
        if (auxAddr == 0) return;
        long dataOffset = Unsafe.getLong(auxAddr);

        long lenAddr = getCoveredDataReadAddr(covIdx, dataOffset, Integer.BYTES);
        if (lenAddr == 0) return;
        int len = Unsafe.getInt(lenAddr);
        if (len < 0) return;

        int totalBytes = Integer.BYTES + len * Character.BYTES;
        long dataAddr = getCoveredDataReadAddr(covIdx, dataOffset, totalBytes);
        if (dataAddr != 0) {
            mem.putBlockOfBytes(dataAddr, totalBytes);
        }
    }

    private boolean writeVarStrideDataAttempt(
            MemoryMARW mem, int covIdx, long colTop, int colType,
            int ks, int[] keyCounts, long[] keyOffsets, long mergedValuesAddr,
            int totalCount, boolean longOffsets
    ) {
        mem.putInt(longOffsets ? totalCount | PostingIndexUtils.LONG_OFFSETS_FLAG : totalCount);
        long offsetsStart = mem.getAppendOffset();
        for (int i = 0; i <= totalCount; i++) {
            if (longOffsets) mem.putLong(0L);
            else mem.putInt(0);
        }
        long dataStart = mem.getAppendOffset();

        int valueOrdinal = 0;
        for (int j = 0; j < ks; j++) {
            int count = keyCounts[j];
            long keyOff = keyOffsets[j];
            for (int i = 0; i < count; i++) {
                long rowId = Unsafe.getLong(mergedValuesAddr + (keyOff + i) * Long.BYTES);
                long off = mem.getAppendOffset() - dataStart;
                if (!longOffsets && off > Integer.MAX_VALUE) return false;
                writeVarOffset(mem, offsetsStart, valueOrdinal, off, longOffsets);
                writeVarValue(mem, covIdx, colTop, rowId, colType);
                valueOrdinal++;
            }
        }
        long endOff = mem.getAppendOffset() - dataStart;
        if (!longOffsets && endOff > Integer.MAX_VALUE) return false;
        writeVarOffset(mem, offsetsStart, valueOrdinal, endOff, longOffsets);
        return true;
    }

    private void writeVarValue(MemoryMARW mem, int covIdx, long colTop, long rowId, int colType) {
        if (rowId < colTop) {
            return; // NULL — zero-length (offset[i] == offset[i+1])
        }
        long row = rowId - colTop;
        switch (ColumnType.tagOf(colType)) {
            case ColumnType.VARCHAR -> writeVarcharValue(mem, covIdx, row);
            case ColumnType.STRING -> writeStringValue(mem, covIdx, row);
            default -> writeBinaryLikeValue(mem, covIdx, row);
        }
    }

    private void writeVarcharValue(MemoryMARW mem, int covIdx, long row) {
        if (coveredColumnNames.size() == 0 && coveredColumnAddrs.size() == 0) {
            return;
        }
        long auxOffset = VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES * row;
        long auxAddr = getCoveredAuxReadAddr(covIdx, auxOffset, VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES);
        if (auxAddr == 0) return;

        int header = Unsafe.getInt(auxAddr);
        if ((header & VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL) != 0) {
            // NULL — write nothing. offsets[i] == offsets[i+1] is the NULL
            // marker on read. Empty (non-null) writes a single sentinel
            // byte below so it is distinguishable from NULL.
            return;
        }
        // Sentinel: a single 0x00 byte preceding the value's bytes. Any
        // non-null VARCHAR (including empty) emits this byte. The reader
        // strips it and uses the remaining (hi - lo - 1) bytes as the
        // value. STRING/BINARY don't need this because they already prefix
        // their length, so empty is naturally distinguishable from NULL.
        mem.putByte((byte) 0);
        if ((header & 1) != 0) {
            int size = (header >>> 4) & 0xF;
            if (size > 0) {
                mem.putBlockOfBytes(auxAddr + 1, size);
            }
        } else {
            int size = (header >>> 4) & 0x0FFFFFFF;
            if (size > 0) {
                long dataOffset = Unsafe.getLong(auxAddr + 8) >>> 16;
                long dataAddr = getCoveredDataReadAddr(covIdx, dataOffset, size);
                if (dataAddr != 0) {
                    mem.putBlockOfBytes(dataAddr, size);
                }
            }
        }
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

    /**
     * Describes one superseded sealed version of this column's POSTING files
     * that the writer wants deleted as soon as the scoreboard says no reader
     * still holds it.
     * <p>
     * {@link #postingColumnNameTxn} is the column-instance txn (the
     * {@code .pk} suffix); {@link #sealTxn} identifies the specific sealed
     * version on disk. The scoreboard query range
     * {@code [fromTableTxn, toTableTxn)} brackets the txns at which a reader
     * could still see the previous sealed version.
     * <p>
     * Lifecycle: created by {@link #seal} after the new sealTxn is published,
     * recycled to the pool after the queue accepts the published task.
     */
    static final class PendingSealPurge {
        long fromTableTxn;
        long partitionNameTxn;
        long partitionTimestamp;
        long postingColumnNameTxn;
        long sealTxn;
        long toTableTxn;

        void of(
                long postingColumnNameTxn,
                long sealTxn,
                long fromTableTxn,
                long toTableTxn,
                long partitionTimestamp,
                long partitionNameTxn
        ) {
            this.postingColumnNameTxn = postingColumnNameTxn;
            this.sealTxn = sealTxn;
            this.fromTableTxn = fromTableTxn;
            this.toTableTxn = toTableTxn;
            this.partitionTimestamp = partitionTimestamp;
            this.partitionNameTxn = partitionNameTxn;
        }
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

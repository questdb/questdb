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
import io.questdb.std.Decimals;
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
    private int blockCapacity;
    // v2 chain helper: owns the in-memory mirror of the chain header,
    // exposes append/extend/recovery primitives. Single instance per writer
    // — reused across reopens via resetState().
    private final PostingIndexChainWriter chain = new PostingIndexChainWriter();
    // Reusable scratch list for sealTxns that recoveryDropAbandoned dropped
    // out of the chain on writer-open. Populated by of(...) when
    // currentTableTxn was supplied via setCurrentTableTxn; the orphan
    // sealTxns are then forwarded to the seal-purge outbox so the
    // background job removes the .pv.{N} / .pc{i}.{N} files. Cleared on
    // every of() call to avoid leaking entries across reopens.
    private final LongList recoveryOrphanScratch = new LongList();
    private int coverCount;
    private long[] coveredAuxReadAddrs;
    private long[] coveredAuxReadSizes;
    // RO mmaps owned by the writer (opened from coveredColumnNames + partitionPath).
    // size > 0 means writer-owned; size == 0 means addr-based (O3, not owned).
    private long[] coveredColReadAddrs;
    private long[] coveredColReadSizes;
    // Last-committed table _txn, supplied by TableWriter via
    // setCurrentTableTxn before of(). Used by the writer-open recovery walk
    // (recoveryDropAbandoned) to drop chain entries from a previous
    // distressed writer attempt where txnAtSeal > currentTableTxn (meaning
    // the publish landed on disk but txWriter.commit() never did). -1
    // sentinel means "unset" — recovery walk is skipped, matching the
    // historical behaviour for callers that haven't been wired yet
    // (tests, fd-based opens, snapshot restore).
    private long currentTableTxn = -1L;
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
    // In-memory mirror of the head entry's MAX_VALUE field. setMaxValue
    // updates it directly; flush/seal callsites read it back without going
    // to mmap. Persisted to the head entry on every chain publish (or via
    // chain.updateHeadMaxValue between flushes).
    private long maxValue;
    private long packedResidualsAddr;
    private int packedResidualsCapacity;
    private long partitionNameTxn = -1;
    private long partitionTimestamp = Long.MIN_VALUE;
    private long pendingCountsAddr;
    // Table _txn the next chain entry should record as its txnAtSeal,
    // supplied by TableWriter#syncColumns via setNextTxnAtSeal. -1 means
    // no caller-supplied value; the writer uses 0 as a placeholder so the
    // entry is visible to every pinned reader (existing semantics for
    // pre-commit / close-time flushes).
    private long pendingTxnAtSeal = -1;
    private long pendingValuesAddr;
    private long postingColumnNameTxn; // host column-instance txn
    private long[] savedSidecarBufs;
    private long[] savedSidecarSizes;
    private MemoryMARW sealTarget; // points to sealValueMem during seal, valueMem during flush
    // sealTxn is the suffix of the .pv file the writer is currently mapped
    // to. Updated on truncate, on switchToSealedValueFile, and at writer
    // open time from the chain head's sealTxn (or chain.peekNextSealTxn for
    // an empty chain).
    private long sealTxn;
    private MemoryMARW sidecarInfoMem;
    private int spillArraysCapacity;
    private long spillKeyAddrsAddr;
    private long spillKeyCapacitiesAddr;
    private long spillKeyCountsAddr;
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
        // Default: first chain entry will use sealTxn=0, matching the
        // historical .pv.0 filename for a freshly-initialised, never-sealed
        // index.
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
                        .put("index values must be added in ascending order [key=").put(key)
                        .put(", lastValue=").put(lastVal)
                        .put(", newValue=").put(value).put(']');
            }
        } else {
            int spillCount = getSpillCount(key);
            if (spillCount > 0) {
                long spillAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                long lastSpilledVal = Unsafe.getLong(spillAddr + (long) (spillCount - 1) * Long.BYTES);
                if (value < lastSpilledVal) {
                    throw CairoException.critical(0)
                            .put("index values must be added in ascending order [key=").put(key)
                            .put(", lastValue=").put(lastSpilledVal)
                            .put(", newValue=").put(value).put(']');
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
                    // v1 trimmed to KEY_FILE_RESERVED because the only live
                    // bytes were the two header pages. v2 keeps chain entries
                    // past the headers; trim to regionLimit instead so we
                    // don't lop off the head entry. For an empty chain
                    // regionLimit equals the entry region base which equals
                    // KEY_FILE_RESERVED.
                    long liveSize = chain.getRegionLimit();
                    if (liveSize < KEY_FILE_RESERVED) {
                        liveSize = KEY_FILE_RESERVED;
                    }
                    keyMem.setSize(liveSize);
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
                maxValue = 0;
                hasPendingData = false;
                activeKeyCount = 0;
                coverCount = 0;
                sealTarget = null;
                releasePendingPurges();
                pendingTxnAtSeal = -1;
                // currentTableTxn is intentionally not reset here.
                // path-based of(...) starts with close(), and the setter
                // contract is "call setCurrentTableTxn before of() so
                // recovery can consume it"; clearing the field here would
                // race with that pattern. runRecoveryWalkIfRequested
                // resets the field after it consumes the value.
                recoveryOrphanScratch.clear();
                chain.resetState();
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
                    maxValue = 0;
                    hasPendingData = false;
                    activeKeyCount = 0;
                    coverCount = 0;
                    chain.resetState();
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
     * <p>
     * timestampColumnIndex is the writer-space index of the designated
     * timestamp; pass -1 if no covered column is the designated timestamp.
     * It must be passed through so the O3 reseal path keeps using the
     * linear-prediction encoder for the timestamp covering instead of
     * silently falling back to the generic long encoder.
     */
    public void configureCovering(
            LongList coveredColumnAddrs,
            LongList coveredColumnTops,
            IntList coveredColumnShifts,
            IntList coveredColumnIndices,
            IntList coveredColumnTypes,
            int coverCount,
            int timestampColumnIndex
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
        this.timestampColumnIndex = timestampColumnIndex;
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
        configureCovering(coveredColumnAddrs, coveredColumnTops, coveredColumnShifts,
                coveredColumnIndices, coveredColumnTypes, coverCount, -1);
    }

    @TestOnly
    public void configureCovering(
            long[] coveredColumnAddrs,
            long[] coveredColumnTops,
            int[] coveredColumnShifts,
            int[] coveredColumnIndices,
            int[] coveredColumnTypes,
            int coverCount,
            int timestampColumnIndex
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
        configureCovering(addrs, tops, shifts, indices, types, coverCount, timestampColumnIndex);
    }

    @TestOnly
    public RowCursor getCursor(int key) {
        flushAllPending();

        if (key >= keyCount || key < 0 || genCount == 0) {
            return EmptyRowCursor.INSTANCE;
        }

        LongList values = new LongList();
        long headEntryOffset = chain.getHeadEntryOffset();
        for (int gen = 0; gen < genCount; gen++) {
            long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(headEntryOffset, gen);
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
                long nextStrideOff = Unsafe.getLong(genAddr + (long) (stride + 1) * Long.BYTES);
                // Empty stride: writer records strideOff[s] == strideOff[s+1].
                // Reading on would interpret the next stride's bytes here.
                if (nextStrideOff == strideOff) continue;
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
        return maxValue;
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

    /**
     * Read-only access to a queued purge entry's
     * {@code [fromTableTxn, toTableTxn)} window for testing. Returns
     * {@code -1} components if the index is out of range. Tests use this
     * to assert that Phase 4's chain-derived intervals replace the v1
     * {@code [0, Long.MAX_VALUE)} fallback.
     */
    @TestOnly
    public long getPendingPurgeFromTxnForTesting(int idx) {
        if (idx < 0 || idx >= pendingPurges.size()) {
            return -1L;
        }
        return pendingPurges.getQuick(idx).fromTableTxn;
    }

    @TestOnly
    public long getPendingPurgeToTxnForTesting(int idx) {
        if (idx < 0 || idx >= pendingPurges.size()) {
            return -1L;
        }
        return pendingPurges.getQuick(idx).toTableTxn;
    }

    @TestOnly
    public int getTimestampColumnIndex() {
        return timestampColumnIndex;
    }

    @Override
    public boolean isOpen() {
        return keyMem.isOpen();
    }

    /**
     * In v1 this folded an fd-based O3 writer's tentative metadata-page
     * state into the active view. v2 has no inactive metadata slot — every
     * append produces a new chain entry whose visibility is gated by the
     * reader's pinned _txn against the entry's {@code txnAtSeal}. The
     * caller in {@code TableWriter} is kept in place to preserve the seal
     * call shape; this implementation is a no-op until Phase 2c retires
     * the call site.
     */
    @Override
    public void mergeTentativeIntoActiveIfAny() {
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
     * without coverCount &gt; 0. v2 has no separate "tentative" state: any chain entry
     * the writer appends here is visible to readers as soon as the publish completes.
     * Phase 2c will plumb {@code currentTableTxn} through {@code of(...)} so the open
     * can drop chain entries with {@code txnAtSeal &gt; currentTableTxn} (abandoned
     * by a previous attempt that never committed).
     */
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
                chain.resetState();
                this.sealTxn = 0;
                this.maxValue = -1L;
                this.keyCount = 0;
                this.genCount = 0;
                this.valueMemSize = 0;
            } else {
                // Reads the v2 chain header and head entry; throws on
                // FORMAT_VERSION mismatch.
                chain.openExisting(keyMem);
                runRecoveryWalkIfRequested();
                if (chain.hasHead()) {
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(keyMem, head);
                    this.sealTxn = head.sealTxn;
                    this.valueMemSize = head.valueMemSize;
                    this.blockCapacity = head.blockCapacity;
                    this.keyCount = head.keyCount;
                    this.genCount = head.genCount;
                    this.maxValue = head.maxValue;
                } else {
                    this.sealTxn = chain.peekNextSealTxn();
                    this.maxValue = -1L;
                    this.keyCount = 0;
                    this.genCount = 0;
                    this.valueMemSize = 0;
                }
            }

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
                                .$("] - possible incomplete seal").$();
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
        partitionPath.clear();
        partitionPath.put(path);
        indexName.clear();
        indexName.put(name);
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

                // Map the whole file so both header pages plus the entry
                // region are visible. The chain helper's openExisting reads
                // the head entry which lives past the two header pages.
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), keyFileSize, MemoryTag.MMAP_INDEX_WRITER);
            }
            kFdUnassigned = false;

            if (init) {
                chain.resetState();
                this.maxValue = -1L;
                this.keyCount = 0;
                this.genCount = 0;
                this.valueMemSize = 0;
            } else {
                chain.openExisting(keyMem);
                runRecoveryWalkIfRequested();
                if (chain.hasHead()) {
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(keyMem, head);
                    this.sealTxn = head.sealTxn;
                    this.valueMemSize = head.valueMemSize;
                    this.blockCapacity = head.blockCapacity;
                    this.keyCount = head.keyCount;
                    this.genCount = head.genCount;
                    this.maxValue = head.maxValue;
                } else {
                    this.sealTxn = chain.peekNextSealTxn();
                    this.maxValue = -1L;
                    this.keyCount = 0;
                    this.genCount = 0;
                    this.valueMemSize = 0;
                }
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
            // Chain-derived intervals are meaningful: {@code entry.toTableTxn}
            // is the {@code txnAtSeal} of the new head that superseded this
            // file. The empty-chain branch in recordPostingSealPurge and the
            // orphan-recovery path in scheduleOrphanPurge can also publish
            // saturated sentinels of {@code Long.MAX_VALUE}. Such sentinels
            // must NOT reach the scoreboard: PostingSealPurgeOperator queries
            // {@link TxnScoreboard#isRangeAvailable} which has the side effect
            // of pushing the scoreboard's max to toTxn, so a single
            // {@code Long.MAX_VALUE} call would block every future reader
            // open until the next reset. Clamp to {@code currentTableTxn},
            // which is a safe upper bound: no reader pinned right now can
            // hold a txn beyond it, so dropping the file at this txn is
            // already protected by the regular reader-pin check.
            long toTableTxn = Math.min(entry.toTableTxn, currentTableTxn);
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
                        toTableTxn
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
        long gen0DirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), 0);
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

        long gen0DirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), 0);
        int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);

        // Single dense gen: already sealed, sidecars from prior seal still valid.
        if (genCount == 1 && gen0KeyCount >= 0) {
            return;
        }

        final long oldSealTxn = sealTxn;
        final long newSealTxn = Math.max(1, sealTxn + 1);

        // Incremental seal: gen 0 dense covering every current key, all later
        // gens sparse, and path-based (sealIncremental writes a fresh .pv via
        // sealValueMem; fd-based seal must take sealFull's chunked path).
        boolean isIncrementalCandidate = gen0KeyCount >= 0
                && gen0KeyCount == keyCount
                && partitionPath.size() > 0;
        if (isIncrementalCandidate) {
            for (int g = 1; g < genCount; g++) {
                long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), g);
                int gkc = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                if (gkc >= 0) {
                    isIncrementalCandidate = false;
                    break;
                }
            }
        }

        // Snapshot only when going incremental — sealFull rebuilds sidecars
        // from scratch, so mmap-copying old sidecar bytes into native bufs
        // would just be discarded. With many cover columns and large
        // partitions the snapshot cost is multi-GB.
        boolean haveSavedSidecars = false;
        if (isIncrementalCandidate && coverCount > 0) {
            if (savedSidecarBufs == null || savedSidecarBufs.length < coverCount) {
                savedSidecarBufs = new long[coverCount];
                savedSidecarSizes = new long[coverCount];
            } else {
                Arrays.fill(savedSidecarBufs, 0, savedSidecarBufs.length, 0L);
                Arrays.fill(savedSidecarSizes, 0, savedSidecarSizes.length, 0L);
            }
            haveSavedSidecars = true;
        }

        try {
            // Sidecar snapshot lives inside this try so a malloc OOM mid-loop
            // gets cleaned up by the finally instead of leaking partial bufs.
            if (haveSavedSidecars) {
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
            }

            if (coverCount > 0 && partitionPath.size() > 0) {
                if (sidecarMems.size() > 0) {
                    closeSidecarMems();
                }
                openSidecarFiles(Path.getThreadLocal(partitionPath), indexName, postingColumnNameTxn, newSealTxn);
            }

            if (isIncrementalCandidate) {
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
    public void setCurrentTableTxn(long currentTableTxn) {
        this.currentTableTxn = currentTableTxn;
    }

    @Override
    public void setMaxValue(long maxValue) {
        this.maxValue = maxValue;
        // The head entry's MAX_VALUE field is mutable while the entry is
        // the head of the chain (single-writer, aligned 8-byte store is
        // atomic). updateHeadMaxValue also republishes the chain header so
        // a concurrent reader's next read picks up the new value.
        // No-op when the chain is empty: the upcoming first flush will
        // create a head entry that captures this maxValue.
        chain.updateHeadMaxValue(keyMem, maxValue);
    }

    @Override
    public void setNextTxnAtSeal(long txnAtSeal) {
        this.pendingTxnAtSeal = txnAtSeal;
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
        // initKeyMemory just rewrote the .pk header pages; resync the chain
        // helper's in-memory mirror to the new starting state. genCounter
        // = sealTxn - 1 (path-based) or -1 (fd-based, where initKeyMemory
        // defaulted to 0).
        chain.resetState();
        if (partitionPath.size() > 0) {
            chain.openExisting(keyMem);
        }
        keyCount = 0;
        valueMemSize = 0;
        genCount = 0;
        maxValue = -1L;
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

    private static void initKeyMemory(MemoryMA keyMem, int blockCapacity, long startSealTxn) {
        // v2 layout: two 4 KB header pages followed by an empty entry region.
        // Page A is published with sequence=2 (even, non-zero) and an empty
        // chain. Page B is zeroed and stays inactive until the first
        // publish flips the seqlock to it.
        // GEN_COUNTER = startSealTxn - 1 so the very first appendNewEntry
        // accepts exactly startSealTxn — that lines the first chain entry
        // up with the .pv.{startSealTxn} file the writer is mapping.
        keyMem.jumpTo(0);
        keyMem.truncate();

        // ----- Page A header (active, empty chain) -----
        keyMem.putLong(2L);                                                  // SEQUENCE_START
        keyMem.putLong(PostingIndexUtils.V2_FORMAT_VERSION);                 // FORMAT_VERSION
        keyMem.putLong(PostingIndexUtils.V2_NO_HEAD);                        // HEAD_ENTRY_OFFSET
        keyMem.putLong(0L);                                                  // ENTRY_COUNT
        keyMem.putLong(PostingIndexUtils.V2_ENTRY_REGION_BASE);              // REGION_BASE
        keyMem.putLong(PostingIndexUtils.V2_ENTRY_REGION_BASE);              // REGION_LIMIT
        keyMem.putLong(startSealTxn - 1L);                                   // GEN_COUNTER
        // Reserved bytes between GEN_COUNTER (offset 48) and SEQUENCE_END
        // (offset 4088). Pad with zeros.
        long pageAReservedBytes = PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_END
                - (PostingIndexUtils.V2_HEADER_OFFSET_GEN_COUNTER + Long.BYTES);
        for (long i = 0; i < pageAReservedBytes; i += Long.BYTES) {
            keyMem.putLong(0L);
        }
        Unsafe.storeFence();
        keyMem.putLong(2L);                                                  // SEQUENCE_END

        // ----- Page B header (inactive, all zeros) -----
        for (int i = 0; i < PostingIndexUtils.PAGE_SIZE / Long.BYTES; i++) {
            keyMem.putLong(0L);
        }
        // Entry region is empty; the file is exactly KEY_FILE_RESERVED bytes.
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
            case ColumnType.INT, ColumnType.SYMBOL, ColumnType.DECIMAL32 -> {
                Unsafe.putInt(addr, Numbers.INT_NULL);
                return;
            }
            case ColumnType.DECIMAL16 -> {
                Unsafe.putShort(addr, Decimals.DECIMAL16_NULL);
                return;
            }
            case ColumnType.DECIMAL8 -> {
                Unsafe.putByte(addr, Decimals.DECIMAL8_NULL);
                return;
            }
            case ColumnType.DECIMAL128 -> {
                Unsafe.putLong(addr, Decimals.DECIMAL128_HI_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                return;
            }
            case ColumnType.DECIMAL256 -> {
                Unsafe.putLong(addr, Decimals.DECIMAL256_HH_NULL);
                Unsafe.putLong(addr + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                Unsafe.putLong(addr + 2 * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                Unsafe.putLong(addr + 3 * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
                return;
            }
            default -> {
            }
        }
        // Generic null sentinel by size for types not handled by the switch above.
        // Falls through for: BYTE, BOOLEAN, CHAR, SHORT (NULL == 0), and for
        // LONG/TIMESTAMP/DATE/DECIMAL64/UUID/LONG256 where every 8-byte slot
        // is Long.MIN_VALUE (covered by the overlay loop below).
        Unsafe.setMemory(addr, valueSize, (byte) 0);
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
            case ColumnType.CHAR, ColumnType.SHORT -> mem.putShort((short) 0);
            case ColumnType.DECIMAL16 -> mem.putShort(Decimals.DECIMAL16_NULL);
            case ColumnType.GEOSHORT -> mem.putShort(GeoHashes.SHORT_NULL);
            case ColumnType.BYTE, ColumnType.BOOLEAN -> mem.putByte((byte) 0);
            case ColumnType.DECIMAL8 -> mem.putByte(Decimals.DECIMAL8_NULL);
            case ColumnType.GEOBYTE -> mem.putByte(GeoHashes.BYTE_NULL);
            case ColumnType.UUID -> {
                mem.putLong(Numbers.LONG_NULL);
                mem.putLong(Numbers.LONG_NULL);
            }
            case ColumnType.DECIMAL128 -> {
                mem.putLong(Decimals.DECIMAL128_HI_NULL);
                mem.putLong(Decimals.DECIMAL128_LO_NULL);
            }
            case ColumnType.LONG256 -> {
                for (int i = 0; i < 4; i++) mem.putLong(Numbers.LONG_NULL);
            }
            case ColumnType.DECIMAL256 -> {
                mem.putLong(Decimals.DECIMAL256_HH_NULL);
                mem.putLong(Decimals.DECIMAL256_HL_NULL);
                mem.putLong(Decimals.DECIMAL256_LH_NULL);
                mem.putLong(Decimals.DECIMAL256_LL_NULL);
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
        long nextStrideOff = Unsafe.getLong(genBase + (long) (s + 1) * Long.BYTES);
        // Empty stride in this gen: writer records strideOff[s] == strideOff[s+1]
        // when stride s contributed no bytes. Reading on would interpret the next
        // stride's bytes as this stride, with the wrong genKs, and overflow the
        // prefix array into packed-data territory.
        if (nextStrideOff == strideOff) {
            return;
        }
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

            long oldCountsSize = (long) spillArraysCapacity * Integer.BYTES;
            long newCountsSize = (long) newCap * Integer.BYTES;
            long oldCapsSize = (long) spillArraysCapacity * Integer.BYTES;
            long newCapsSize = (long) newCap * Integer.BYTES;
            long oldAddrsSize = (long) spillArraysCapacity * Long.BYTES;
            long newAddrsSize = (long) newCap * Long.BYTES;

            // Roll back earlier reallocs on a later failure so each buffer's
            // size stays in sync with spillArraysCapacity; otherwise
            // freeSpillData drifts the memory-tag accounting.
            spillKeyCountsAddr = Unsafe.realloc(spillKeyCountsAddr, oldCountsSize, newCountsSize, MemoryTag.NATIVE_INDEX_READER);
            try {
                spillKeyCapacitiesAddr = Unsafe.realloc(spillKeyCapacitiesAddr, oldCapsSize, newCapsSize, MemoryTag.NATIVE_INDEX_READER);
            } catch (Throwable e) {
                spillKeyCountsAddr = Unsafe.realloc(spillKeyCountsAddr, newCountsSize, oldCountsSize, MemoryTag.NATIVE_INDEX_READER);
                throw e;
            }
            try {
                spillKeyAddrsAddr = Unsafe.realloc(spillKeyAddrsAddr, oldAddrsSize, newAddrsSize, MemoryTag.NATIVE_INDEX_READER);
            } catch (Throwable e) {
                spillKeyCountsAddr = Unsafe.realloc(spillKeyCountsAddr, newCountsSize, oldCountsSize, MemoryTag.NATIVE_INDEX_READER);
                spillKeyCapacitiesAddr = Unsafe.realloc(spillKeyCapacitiesAddr, newCapsSize, oldCapsSize, MemoryTag.NATIVE_INDEX_READER);
                throw e;
            }
            Unsafe.setMemory(spillKeyCountsAddr + oldCountsSize, newCountsSize - oldCountsSize, (byte) 0);
            Unsafe.setMemory(spillKeyCapacitiesAddr + oldCapsSize, newCapsSize - oldCapsSize, (byte) 0);
            Unsafe.setMemory(spillKeyAddrsAddr + oldAddrsSize, newAddrsSize - oldAddrsSize, (byte) 0);
            spillArraysCapacity = newCap;
        }
    }

    private int estimateMaxPerKey(long gen0Addr, int gen0KeyCount, int gen0SiSize) {
        int max = 0;
        int sc = PostingIndexUtils.strideCount(gen0KeyCount);
        for (int s = 0; s < sc; s++) {
            long strideOff = Unsafe.getLong(gen0Addr + (long) s * Long.BYTES);
            long nextStrideOff = Unsafe.getLong(gen0Addr + (long) (s + 1) * Long.BYTES);
            // Empty stride: writer records strideOff[s] == strideOff[s+1] when
            // stride s contributed no bytes. Reading on would interpret the next
            // stride's bytes here.
            if (nextStrideOff == strideOff) continue;
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

        // Sidecar block header is a 32-bit int; silent wrap here would feed
        // a negative count into reader-side varBlockOffsetsSize and dereference
        // outside the mapping. Lifting the limit needs a sidecar format bump.
        if (totalValues > Integer.MAX_VALUE) {
            throw CairoException.critical(0)
                    .put("posting index gen exceeds 2^31 values [totalValues=").put(totalValues)
                    .put(", genCount=").put(genCount)
                    .put("]; split commit into smaller batches");
        }

        // The in-memory mirror is the source of truth for maxValue: it's
        // bumped by setMaxValue and by previous flushes. The head entry's
        // MAX_VALUE field is just the persistent reflection of this same
        // value, written via chain.updateHeadMaxValue / extendHead.
        long maxValue = this.maxValue;

        // Use sparse format: keyIds + counts + offsets (3 arrays of activeKeyCount)
        int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);

        // Reuse header buffer, growing if needed. Use realloc so an OOM
        // throw leaves the previous (addr, capacity) intact — a free-then-
        // malloc pair would dangle the freed pointer if malloc failed.
        if (headerSize > flushHeaderBufCapacity) {
            int newCapacity = Math.max(headerSize, flushHeaderBufCapacity * 2);
            flushHeaderBuf = Unsafe.realloc(flushHeaderBuf, flushHeaderBufCapacity, newCapacity, MemoryTag.NATIVE_INDEX_READER);
            flushHeaderBufCapacity = newCapacity;
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
        writeSidecarGenData((int) totalValues, genCount);

        genCount++;
        this.maxValue = maxValue;
        // Persist the in-memory state to the chain. extendHead bumps the
        // head entry's GEN_COUNT/LEN/VALUE_MEM_SIZE/MAX_VALUE; appendNewEntry
        // creates a fresh entry when the chain is empty or sealTxn just
        // advanced past the head's sealTxn (path-based seal).
        publishToChain(
                /* newGenCount */ genCount,
                /* overrideGenIndex */ genCount - 1,
                /* overrideFileOffset */ genOffset,
                /* overrideSize */ totalGenSize,
                /* overrideKeyCount */ -activeKeyCount,
                /* overrideMinKey */ minKey,
                /* overrideMaxKey */ maxKey
        );

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
            ff.munmap(addr, size, MemoryTag.MMAP_INDEX_WRITER);
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

        long oldCountSize = (long) keyCapacity * Integer.BYTES;
        long newCountSize = (long) newCapacity * Integer.BYTES;
        long oldValSize = (long) keyCapacity * PENDING_SLOT_CAPACITY * Long.BYTES;
        long newValSize = (long) newCapacity * PENDING_SLOT_CAPACITY * Long.BYTES;

        // Roll back earlier reallocs on a later failure so each buffer's size
        // stays in sync with keyCapacity; otherwise freePendingBuffers frees
        // with the wrong size and drifts the memory-tag accounting.
        pendingCountsAddr = Unsafe.realloc(pendingCountsAddr, oldCountSize, newCountSize, MemoryTag.NATIVE_INDEX_READER);
        try {
            pendingValuesAddr = Unsafe.realloc(pendingValuesAddr, oldValSize, newValSize, MemoryTag.NATIVE_INDEX_READER);
        } catch (Throwable e) {
            pendingCountsAddr = Unsafe.realloc(pendingCountsAddr, newCountSize, oldCountSize, MemoryTag.NATIVE_INDEX_READER);
            throw e;
        }
        int[] newKeyIds;
        try {
            newKeyIds = Arrays.copyOf(activeKeyIds, newCapacity);
        } catch (Throwable e) {
            pendingCountsAddr = Unsafe.realloc(pendingCountsAddr, newCountSize, oldCountSize, MemoryTag.NATIVE_INDEX_READER);
            pendingValuesAddr = Unsafe.realloc(pendingValuesAddr, newValSize, oldValSize, MemoryTag.NATIVE_INDEX_READER);
            throw e;
        }
        Unsafe.setMemory(pendingCountsAddr + oldCountSize, newCountSize - oldCountSize, (byte) 0);
        Unsafe.setMemory(pendingValuesAddr + oldValSize, newValSize - oldValSize, (byte) 0);
        activeKeyIds = newKeyIds;
        keyCapacity = newCapacity;
    }

    private boolean isClosed() {
        return coveredColumnNames.size() == 0 && coveredColumnAddrs.size() == 0;
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
                    long mapped = ff.mmap(fd, fileSize, 0, Files.MAP_RO, MemoryTag.MMAP_INDEX_WRITER);
                    if (mapped == FilesFacade.MAP_FAILED) {
                        throw CairoException.critical(ff.errno())
                                .put("could not mmap covered column [file=").put(fileName)
                                .put(", size=").put(fileSize).put(']');
                    }
                    addrs[idx] = mapped;
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
            long nextStrideOff = Unsafe.getLong(gen0Addr + (long) (stride + 1) * Long.BYTES);
            // Empty stride in this gen: writer records strideOff[s] == strideOff[s+1].
            // Reading on would interpret the next stride's bytes here.
            if (nextStrideOff != strideOff) {
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
                            unpackBatchAddr = Unsafe.realloc(unpackBatchAddr,
                                    (long) unpackBatchCapacity * Long.BYTES,
                                    (long) newCap * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                            unpackBatchCapacity = newCap;
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
        }

        // Decode from sparse gens 1..N
        for (int g = 1; g < genCount; g++) {
            long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), g);
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
     * version. Both bounds come from the v2 chain: by the time this method
     * is called the new entry has already been appended, so
     * {@link PostingIndexChainWriter#getCurrentTxnAtSeal} returns the new
     * head's {@code txnAtSeal} (the upper bound), and
     * {@link PostingIndexChainWriter#getSecondEntryTxnAtSeal} returns the
     * predecessor entry's {@code txnAtSeal} (the lower bound). When the
     * chain has only one entry — the one we just appended is the very
     * first seal — there is no predecessor, so {@code fromTableTxn} falls
     * back to {@link #postingColumnNameTxn} (the column-instance creation
     * txn, which is a strict lower bound on any reader that could ever
     * have seen the file we're purging).
     */
    private void recordPostingSealPurge(long supersededSealTxn) {
        long toTxn = chain.getCurrentTxnAtSeal();
        long fromTxn;
        if (toTxn < 0) {
            // The chain is empty — recordPostingSealPurge was called from
            // a code path that didn't append an entry first (e.g. close
            // without seal). Use a conservative window so the file
            // lingers rather than disappearing under a live reader; the
            // writer-open recovery walk will pick it up on the next
            // reopen.
            fromTxn = 0L;
            toTxn = Long.MAX_VALUE;
        } else {
            long prevTxnAtSeal = chain.getSecondEntryTxnAtSeal(keyMem);
            fromTxn = prevTxnAtSeal >= 0 ? prevTxnAtSeal : postingColumnNameTxn;
        }
        pendingTxnAtSeal = -1;
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
                long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), gen);
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
                        long nextStrideOff = Unsafe.getLong(keyIdsBase + (long) (s + 1) * Long.BYTES);
                        // Empty stride: writer records strideOff[s] == strideOff[s+1].
                        // Reading on would interpret the next stride's bytes here.
                        if (nextStrideOff == strideOff) continue;
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
                packedResidualsAddr = Unsafe.realloc(packedResidualsAddr,
                        (long) packedResidualsCapacity * Long.BYTES,
                        (long) maxStrideTotal * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                packedResidualsCapacity = maxStrideTotal;
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
                    long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), gen);
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
                            long nextStrideOff = Unsafe.getLong(keyIdsBase + (long) (s + 1) * Long.BYTES);
                            // Empty stride: writer records strideOff[s] == strideOff[s+1].
                            // Reading on would interpret the next stride's bytes here.
                            if (nextStrideOff == strideOff) continue;
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
                // genCount must publish after the chain entry is appended so
                // a mid-seal failure leaves the in-memory view consistent
                // with on-disk .pk.
                this.maxValue = maxValue;
                this.genCount = 1;
                publishToChain(
                        /* newGenCount */ 1,
                        /* overrideGenIndex */ 0,
                        /* overrideFileOffset */ sealOffset,
                        /* overrideSize */ valueMemSize - sealOffset,
                        /* overrideKeyCount */ keyCount,
                        /* overrideMinKey */ 0,
                        /* overrideMaxKey */ keyCount - 1
                );
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
                    long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), gen);
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
        this.maxValue = maxValue;
        this.genCount = 1;
        publishToChain(
                /* newGenCount */ 1,
                /* overrideGenIndex */ 0,
                /* overrideFileOffset */ 0,
                /* overrideSize */ valueMemSize,
                /* overrideKeyCount */ keyCount,
                /* overrideMinKey */ 0,
                /* overrideMaxKey */ keyCount - 1
        );
    }

    /**
     * Returns every entry in {@link #pendingPurges} to the pool. Used at
     * writer close — leftover sidecar files on disk are dropped by the
     * chain's recovery walk on the next writer open (Phase 2c will plumb
     * {@code currentTableTxn} so the walk can run at open time).
     */
    private void releasePendingPurges() {
        for (int i = pendingPurges.size() - 1; i >= 0; i--) {
            PendingSealPurge entry = pendingPurges.getQuick(i);
            entry.of(0L, 0L, 0L, 0L, Long.MIN_VALUE, -1L);
            pendingPurgePool.add(entry);
        }
        pendingPurges.clear();
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

    /**
     * Run the v2 chain recovery walk if {@link #setCurrentTableTxn} was
     * called with a non-negative value before this {@code of(...)}. Drops
     * chain entries with {@code txnAtSeal > currentTableTxn} (abandoned by
     * a previous distressed writer) and queues their {@code sealTxn} files
     * for purge.
     * <p>
     * The setter's value is consumed: {@code currentTableTxn} is reset to
     * {@code -1} so a stale value can't accidentally drive a future open's
     * recovery walk if the next caller forgets to set it. The orphan
     * scratch list is cleared on entry so a previous open's residue is
     * never reused.
     */
    private void runRecoveryWalkIfRequested() {
        if (currentTableTxn < 0) {
            return;
        }
        recoveryOrphanScratch.clear();
        int dropped;
        try {
            dropped = chain.recoveryDropAbandoned(keyMem, currentTableTxn, recoveryOrphanScratch);
        } finally {
            // Single-shot consumption — next reopen must call the setter
            // again or recovery is skipped.
            currentTableTxn = -1L;
        }
        if (dropped <= 0) {
            return;
        }
        LOG.info().$("posting index recovery dropped abandoned chain entries [")
                .$("indexName=").$(indexName)
                .$(", postingColumnNameTxn=").$(postingColumnNameTxn)
                .$(", dropped=").$(dropped)
                .$(']').$();
        // Conservatively schedule each orphan .pv.{N} (and .pc{i}.{N}) for
        // purge with the widest possible reader window. Orphans were
        // published by a distressed attempt that never committed, so no
        // reader could ever have pinned at their txnAtSeal — but the
        // scoreboard check inside the purge job is the safety net we rely
        // on, not the [fromTxn, toTxn) interval. Using [0, MAX) is safe.
        for (int i = 0, n = recoveryOrphanScratch.size(); i < n; i++) {
            long orphanSealTxn = recoveryOrphanScratch.getQuick(i);
            scheduleOrphanPurge(orphanSealTxn);
        }
        recoveryOrphanScratch.clear();
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

    /**
     * Queue an orphan {@code .pv.{sealTxn}} (and any matching
     * {@code .pc{i}.{sealTxn}} sidecars) for purge after a recovery walk
     * dropped its chain entry. The widest possible reader-visibility
     * window is used because the orphan was never visible to a committed
     * reader; the seal-purge job's scoreboard check is the safety net.
     * <p>
     * The outbox saturation policy mirrors {@link #recordPostingSealPurge}:
     * if the queue is at capacity the oldest entry is dropped, since the
     * next writer-open recovery walk will re-discover any leftover orphan
     * files on disk.
     */
    private void scheduleOrphanPurge(long orphanSealTxn) {
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
        entry.of(postingColumnNameTxn, orphanSealTxn, 0L, Long.MAX_VALUE, partitionTimestamp, partitionNameTxn);
        pendingPurges.add(entry);
    }

    private void sealFull(long newSealTxn) {
        reencodeAllGenerations(
                newSealTxn,
                this.maxValue,
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
        int sparseMaxPerKey = 0;
        try {
            Unsafe.setMemory(dirtyStridesAddr, sc, (byte) 0);
            dirtyCount = 0;
            for (int g = 1; g < genCount; g++) {
                long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), g);
                long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                long gDataSize = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_SIZE);
                int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                if (genKeyCount >= 0 || gDataSize == 0) {
                    continue;
                }
                int activeKeyCount = -genKeyCount;
                valueMem.extend(genFileOffset + gDataSize);
                long genAddr = valueMem.addressOf(genFileOffset);
                long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;

                for (int i = 0; i < activeKeyCount; i++) {
                    int key = Unsafe.getInt(genAddr + (long) i * Integer.BYTES);
                    int stride = key / PostingIndexUtils.DENSE_STRIDE;
                    if (stride < sc && Unsafe.getByte(dirtyStridesAddr + stride) == 0) {
                        Unsafe.putByte(dirtyStridesAddr + stride, (byte) 1);
                        dirtyCount++;
                    }
                    int c = Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
                    if (c > sparseMaxPerKey) {
                        sparseMaxPerKey = c;
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
        long gen0DirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), 0);
        long gen0FileOffset = keyMem.getLong(gen0DirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
        long copyBufSize = keyMem.getLong(gen0DirOffset + GEN_DIR_OFFSET_SIZE);
        int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
        int gen0SiSize = PostingIndexUtils.strideIndexSize(gen0KeyCount);

        // Allocate output buffers — initialize to 0 so the finally block
        // can conditionally free only those that were successfully allocated.
        int siSize = PostingIndexUtils.strideIndexSize(keyCount);
        int maxPerKey = estimateMaxPerKey(valueMem.addressOf(gen0FileOffset), gen0KeyCount, gen0SiSize);
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
                unpackBatchAddr = Unsafe.realloc(unpackBatchAddr,
                        (long) unpackBatchCapacity * Long.BYTES,
                        (long) preAllocPerKey * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                unpackBatchCapacity = preAllocPerKey;
            }
            int preAllocPerStride = (int) Math.min(maxPerStride, Integer.MAX_VALUE);
            if (preAllocPerStride > packedResidualsCapacity) {
                packedResidualsAddr = Unsafe.realloc(packedResidualsAddr,
                        (long) packedResidualsCapacity * Long.BYTES,
                        (long) preAllocPerStride * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                packedResidualsCapacity = preAllocPerStride;
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
                                incrSidecarBuf = Unsafe.realloc(incrSidecarBuf, incrSidecarBufSize, neededBuf, MemoryTag.NATIVE_INDEX_READER);
                                incrSidecarBufSize = neededBuf;
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
            // sealTxn must be set BEFORE the chain publish so the new
            // entry's SEAL_TXN field references the .pv just allocated.
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
            // genCount must publish after the chain entry is appended so a
            // mid-seal failure leaves the in-memory view consistent with
            // on-disk .pk.
            this.genCount = 1;
            publishToChain(
                    /* newGenCount */ 1,
                    /* overrideGenIndex */ 0,
                    /* overrideFileOffset */ sealOffset,
                    /* overrideSize */ valueMemSize - sealOffset,
                    /* overrideKeyCount */ keyCount,
                    /* overrideMinKey */ 0,
                    /* overrideMaxKey */ keyCount - 1
            );
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
            ff.munmap(coveredColReadAddrs[c], coveredColReadSizes[c], MemoryTag.MMAP_INDEX_WRITER);
            coveredColReadAddrs[c] = 0;
            coveredColReadSizes[c] = 0;
        }
        if (coveredAuxReadAddrs != null && coveredAuxReadAddrs[c] != 0 && coveredAuxReadSizes[c] > 0) {
            ff.munmap(coveredAuxReadAddrs[c], coveredAuxReadSizes[c], MemoryTag.MMAP_INDEX_WRITER);
            coveredAuxReadAddrs[c] = 0;
            coveredAuxReadSizes[c] = 0;
        }
    }

    private void unmapCoveredColumnReads() {
        if (coveredColReadAddrs != null) {
            for (int c = 0; c < coveredColReadAddrs.length; c++) {
                // Only unmap if owned (size > 0). Size == 0 means O3 addr path.
                if (coveredColReadAddrs[c] != 0 && coveredColReadSizes[c] > 0) {
                    ff.munmap(coveredColReadAddrs[c], coveredColReadSizes[c], MemoryTag.MMAP_INDEX_WRITER);
                }
                coveredColReadAddrs[c] = 0;
                coveredColReadSizes[c] = 0;
                if (coveredAuxReadAddrs != null && coveredAuxReadAddrs[c] != 0 && coveredAuxReadSizes[c] > 0) {
                    ff.munmap(coveredAuxReadAddrs[c], coveredAuxReadSizes[c], MemoryTag.MMAP_INDEX_WRITER);
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
     * Persist the writer's in-memory state to the v2 chain. Writes the
     * supplied gen-dir entry to the right slot, then either extends the
     * head entry (when the .pv file matches the head's sealTxn) or
     * appends a brand-new chain entry (when {@code sealTxn} has just
     * advanced past the head's sealTxn — i.e. a path-based seal).
     * <p>
     * The new entry's bytes go to {@code chain.getRegionLimit()}; the
     * extended head entry's bytes mutate at {@code chain.getHeadEntryOffset()}.
     * In both cases the chain header is republished under its A/B seqlock so
     * concurrent readers observe a consistent snapshot.
     *
     * @param newGenCount        gen-dir entry count after the publish
     * @param overrideGenIndex   slot to write the new gen-dir entry to
     * @param overrideFileOffset {@code .pv} offset of the new gen
     * @param overrideSize       byte size of the new gen
     * @param overrideKeyCount   distinct keys in the new gen (negative for sparse)
     * @param overrideMinKey     min key in the new gen
     * @param overrideMaxKey     max key in the new gen
     */
    private void publishToChain(int newGenCount, int overrideGenIndex,
                                long overrideFileOffset, long overrideSize,
                                int overrideKeyCount, int overrideMinKey, int overrideMaxKey) {
        // The writer's sealTxn always points at the .pv file currently
        // mapped through valueMem. When chain.getGenCounter() == sealTxn
        // (head matches the same .pv file) we extend the head entry. When
        // sealTxn advances past genCounter (path-based seal switched .pv),
        // we append a fresh entry. The same applies to the empty-chain
        // case (genCounter < sealTxn) — first flush after init/truncate.
        boolean newEntry = !chain.hasHead() || this.sealTxn != chain.getGenCounter();
        long entryBase = newEntry ? chain.getRegionLimit() : chain.getHeadEntryOffset();

        long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(entryBase, overrideGenIndex);
        keyMem.putLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET, overrideFileOffset);
        keyMem.putLong(dirOffset + GEN_DIR_OFFSET_SIZE, overrideSize);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT, overrideKeyCount);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY, overrideMinKey);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, overrideMaxKey);

        if (newEntry) {
            // pendingTxnAtSeal supplied by TableWriter#syncColumns is the
            // table _txn the upcoming commit will assign. -1 (unset) means
            // the caller didn't wire the setter — fall back to txnAtSeal=0
            // so the entry is visible to every pinned reader. The seller
            // semantic is single-shot: the field is reset on consumption.
            long txnAtSeal = pendingTxnAtSeal >= 0 ? pendingTxnAtSeal : 0L;
            chain.appendNewEntry(
                    keyMem,
                    /* sealTxn */ this.sealTxn,
                    /* txnAtSeal */ txnAtSeal,
                    /* valueMemSize */ valueMemSize,
                    /* maxValue */ maxValue,
                    /* keyCount */ keyCount,
                    /* genCount */ newGenCount,
                    /* blockCapacity */ blockCapacity,
                    /* coveringFormat */ 0
            );
        } else {
            chain.extendHead(keyMem, newGenCount, keyCount, valueMemSize, maxValue);
        }
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
                    packedResidualsAddr = Unsafe.realloc(packedResidualsAddr,
                            (long) packedResidualsCapacity * Long.BYTES,
                            (long) newCap * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    packedResidualsCapacity = newCap;
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

        // Init to 0 so the finally block can free what was actually allocated.
        // Allocations happen inside the try so any throw mid-init is caught.
        long sidecarStrideIndexBuf = 0;
        long sidecarBuf = 0;
        long sidecarBufSize = 0;
        long longWorkspaceSize = (long) globalMaxKeyCount * Long.BYTES;
        long longWorkspaceAddr = 0;
        long exceptionWorkspaceAddr = 0;

        try {
            sidecarStrideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
            for (int i = 0; i < siSize; i += Long.BYTES) {
                mem.putLong(0L); // stride index placeholders
            }
            if (shift >= 0 && globalMaxKeyCount > 0) {
                longWorkspaceAddr = Unsafe.malloc(longWorkspaceSize, MemoryTag.NATIVE_INDEX_READER);
                exceptionWorkspaceAddr = Unsafe.malloc(globalMaxKeyCount, MemoryTag.NATIVE_INDEX_READER);
            }

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
                        sidecarBuf = Unsafe.realloc(sidecarBuf, sidecarBufSize, needed, MemoryTag.NATIVE_INDEX_READER);
                        sidecarBufSize = needed;
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
            if (sidecarStrideIndexBuf != 0) {
                Unsafe.free(sidecarStrideIndexBuf, siSize, MemoryTag.NATIVE_INDEX_READER);
            }
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
        // Init to 0 so the finally frees only what was allocated. If the
        // second malloc throws OOM, the first one would leak otherwise.
        long longWorkspaceAddr = 0;
        long exceptionWorkspaceAddr = 0;

        try {
            if (maxKeyCount > 0) {
                longWorkspaceAddr = Unsafe.malloc((long) maxKeyCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                exceptionWorkspaceAddr = Unsafe.malloc(maxKeyCount, MemoryTag.NATIVE_INDEX_READER);
            }

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

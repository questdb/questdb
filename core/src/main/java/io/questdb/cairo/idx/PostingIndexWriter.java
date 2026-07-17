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
import io.questdb.std.ObjectStackPool;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.tasks.PostingSealPurgeTask;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.idx.PostingIndexUtils.*;

/**
 * Delta + FoR64 BitPacking posting index writer.
 * <p>
 * Each commit appends one generation (covering all keys) to the value file.
 * No symbol table needed — encoding is purely arithmetic.
 */
public class PostingIndexWriter implements IndexWriter {
    // Safety ceiling for the batch output buffer when fsst_compress reports
    // produced=0 (a single value didn't fit, grow and retry). 64 MiB is
    // ~four orders of magnitude over the typical batch peak.
    private static final long FSST_BATCH_OUT_CAP_MAX = 64L * 1024L * 1024L;
    // Initial capacity for the streaming batch output buffer.
    private static final long FSST_BATCH_OUT_CAP_MIN = 1L << 20; // 1 MiB
    // Number of strings encoded per fsst_compress call when streaming. Keeps
    // anonymous-heap scratch (batch lens/ptrs in/out, batch output buffer)
    // bounded regardless of total input size.
    private static final int FSST_BATCH_SIZE = 1024;
    // Minimum raw data size to attempt FSST compression (below this, overhead > savings)
    private static final int FSST_MIN_RAW_SIZE = 4096;
    // Cap on the bytes drawn from the input for symbol-table training. cwida
    // recommends "at least 16 KiB"; 64 KiB is comfortably over that and
    // bounds the sample arrays in anonymous heap.
    private static final long FSST_SAMPLE_TARGET_BYTES = 64L * 1024L;
    // Cap on the number of values drawn for training; with stride sampling
    // this lands the sample around 1024 values regardless of stride size.
    private static final int FSST_SAMPLE_TARGET_COUNT = 1024;
    private static final int INITIAL_KEY_CAPACITY = 64;
    private static final Log LOG = LogFactory.getLog(PostingIndexWriter.class);
    private static final int MAX_GEN_COUNT = PostingIndexUtils.MAX_GEN_COUNT;
    private static final int PENDING_SLOT_CAPACITY = 8;
    private final double alignedBitWidthThreshold;
    // v2 chain helper: owns the in-memory mirror of the chain header,
    // exposes append/extend/recovery primitives. Single instance per writer
    // reused across reopens via resetState().
    private final PostingIndexChainWriter chain = new PostingIndexChainWriter();
    private final CairoConfiguration configuration;
    // Last-known valid byte extent of each cover column's .pcN, indexed by cover
    // slot. Updated from the live sidecar handle's append offset whenever it is
    // open (see captureCoverEndOffsets). Reused when the handle is closed so a
    // chain-head republish (extendHead) that runs after closeSidecarMems does not
    // clobber the entry's cover-end with 0 and make the on-disk .pc unreadable.
    private final LongList coverEndOffsetsCache = new LongList();
    // Reusable scratch list passed to PostingIndexChainWriter.appendNewEntry /
    // extendHead, built once per chain publish from each cover column's
    // current sidecar append offset. Reused across calls to avoid allocations.
    private final LongList coverEndOffsetsScratch = new LongList();
    // O3 addr-based covering: caller-provided native memory addresses, plus the
    // mapped byte length of each (the *AddrSizes lists bound the addr-based
    // covered data/aux reads under -ea).
    private final LongList coveredColumnAddrSizes = new LongList();
    private final LongList coveredColumnAddrs = new LongList();
    private final LongList coveredColumnAuxAddrSizes = new LongList();
    private final LongList coveredColumnAuxAddrs = new LongList();
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
    // Memory budget for the per-key spill arena. When the running
    // totalSpillBytes accountant crosses this watermark inside spillKey,
    // the writer drains pending+spill via flushAllPending and frees the
    // anonymous-heap buffers, bounding peak RSS during long indexing
    // runs (ALTER ADD INDEX TYPE POSTING, IndexBuilder, the
    // discardForRebuild + index-loop path on each O3 seal). Cached from
    // CairoConfiguration.getPostingIndexerSpillBytesMax(); 0 or negative
    // disables back-pressure entirely (legacy "accumulate until seal").
    private final long indexerSpillBytesMax;
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    private final StringSink o3CtxName = new StringSink();
    private final Utf8StringSink o3CtxPartitionPath = new Utf8StringSink();
    private final Utf8StringSink partitionPath = new Utf8StringSink();
    private final ObjList<PendingSealPurge> pendingPurgePool = new ObjList<>();
    private final ObjList<PendingSealPurge> pendingPurges = new ObjList<>();
    // Reusable scratch list for sealTxns that recoveryDropAbandoned dropped
    // out of the chain on writer-open. Populated by of(...) when
    // currentTableTxn was supplied via setCurrentTableTxn; the orphan
    // sealTxns are then forwarded to the seal-purge outbox so the
    // background job removes the .pv.{N} / .pc{i}.{N} files. Cleared on
    // every of() call to avoid leaking entries across reopens.
    private final LongList recoveryOrphanScratch = new LongList();
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
    // Reusable per-gen metadata scratch for the streaming rollback/seal
    // (filterCountsForRollback + reencodeWithPerKeyStreaming): base address,
    // key count and stride count per source generation. Grown on demand to
    // genCount; avoids a per-call long[]/int[] allocation on the seal/rollback
    // path and hoists strideCount out of the per-key decode loop.
    private long[] genMetaBases;
    private int[] genMetaKeyCounts;
    private int[] genMetaStrideCounts;
    private boolean hasPendingData;
    private boolean hasSpillData;
    private boolean isLastRollbackStreaming;
    private boolean isLastSealIncremental;
    private boolean isLastSealSnapshotDeferred;
    private boolean isLastSealStreaming;
    private boolean isPoisoned;
    private int keyCapacity;
    private int keyCount;
    // In-memory mirror of the head entry's MAX_VALUE field. setMaxValue
    // updates it directly; flush/seal callsites read it back without going
    // to mmap. Persisted to the head entry on every chain publish (or via
    // chain.updateHeadMaxValue between flushes).
    private long maxValue;
    private long o3CtxColumnNameTxn = TableUtils.COLUMN_NAME_TXN_NONE;
    private long o3CtxUpcomingTxn = -1L;
    private long packedResidualsAddr;
    private int packedResidualsCapacity;
    private long partitionNameTxn = -1;
    private long partitionTimestamp = Long.MIN_VALUE;
    private long pendingCountsAddr;
    // Old sealTxn stashed by discardForRebuild after it rotates valueMem to
    // a fresh sealTxn. Consumed by the next flushAllPending() right after
    // publishToChain so recordPostingSealPurge sees the post-append chain
    // shape ([REBUILD@new, OLD@old]) and computes a correct
    // [OLD.txnAtSeal, REBUILD.txnAtSeal) window for the OLD .pv's purge.
    // -1 means no rebuild rotation is pending. Cleared in close().
    private long pendingDiscardOldSealTxn = -1L;
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
    // Running tally of bytes held in the per-key spill arena. Bumped on
    // every spillKey realloc that grows a per-key buffer, and inside
    // flushAllPending's merge-spill grow site; reset to 0 inside
    // freeSpillData. Drives compactIfOverBudget's mid-stream flush
    // decision. Tracks NATIVE_INDEX_READER bytes attributable to the
    // spill arena only -- pending buffers, fsst scratch, etc. live under
    // the same memory tag but are separately bounded.
    private long totalSpillBytes;
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
        this.indexerSpillBytesMax = configuration.getPostingIndexerSpillBytesMax();
        this.rowIdEncoding = rowIdEncoding;
        this.encodeCtx.setAdaptiveDeltaAtOrAbove(configuration.getPostingIndexAdaptiveDeltaAtOrAbove());
    }

    @TestOnly
    public PostingIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        of(path, name, columnNameTxn, true);
        // Tests using this convenience constructor do not need to think
        // about txn context. Production callers use the no-arg constructor
        // + 5-arg of(...) (via SymbolColumnIndexer) and must call
        // setNextTxnAtSeal explicitly with the right per-context value.
        this.pendingTxnAtSeal = 0L;
    }

    @TestOnly
    public static long encodeCtxPeakBytesForTesting(int maxKeyCount) {
        return encodeCtxPeakBytes(maxKeyCount);
    }

    public static void initKeyMemory(MemoryMA keyMem) {
        // Default: first chain entry will use sealTxn=0, matching the
        // historical .pv.0 filename for a freshly-initialised, never-sealed
        // index.
        initKeyMemory(keyMem, 0L);
    }

    @Override
    public void add(int key, long value) {
        checkNotPoisoned();
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
        releaseCoveredColumnReadMappings();
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
                isPoisoned = false;
                isLastSealStreaming = false;
                isLastSealIncremental = false;
                isLastSealSnapshotDeferred = false;
                isLastRollbackStreaming = false;
                activeKeyCount = 0;
                coverCount = 0;
                // Drop the cover-extent cache so a pooled reopen for a different
                // column/partition starts clean. No live read depends on this
                // (publishToChain rewrites the cache before every captureCoverEndOffsets),
                // but clearing it keeps the cache's safety structural, not ordering-bound.
                coverEndOffsetsCache.clear();
                sealTarget = null;
                releasePendingPurges();
                pendingDiscardOldSealTxn = -1L;
                pendingTxnAtSeal = -1;
                o3CtxUpcomingTxn = -1L;
                // o3CtxName / o3CtxPartitionPath / o3CtxColumnNameTxn are
                // intentionally NOT cleared here. of(path, name, txn, isInit)
                // calls close() internally and then continues reading its
                // CharSequence `name` parameter — when callers pass o3CtxName
                // through (the openFromO3Context path), clearing it mid-call
                // would empty the name arg. openFromO3Context clears these
                // fields itself once of() has finished consuming them.
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
            // Emergency-cleanup path: a SymbolMapWriter rebuild or open
            // failed and we are closing without a transaction context.
            // The seal below is best-effort. Tag the chain entry it
            // publishes with txnAtSeal=0 so the publishToChain contract
            // (`pendingTxnAtSeal >= 0`) is satisfied. The writer-open
            // recovery walk on the next reopen cannot drop it (predicate
            // `0 > committedTxn` is unreachable), but that is acceptable
            // here -- the chain entry references whatever data was in
            // memory at the moment the rebuild failed, which is the
            // legitimate prior committed state, not an abandoned commit.
            // Skip the seal when the writer is poisoned: seal() would throw
            // via checkNotPoisoned() out of this best-effort cleanup. The
            // finally below still frees every resource either way.
            this.pendingTxnAtSeal = 0L;
            if (!isPoisoned) {
                seal();
            }
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
                    pendingTxnAtSeal = -1;
                    // Return the outbox entries to the pool, mirroring close(). A
                    // poisoned-rollback emergency close can leave the staged
                    // newSealTxn orphan here; releasePendingPurges does not publish
                    // (the .pv/.pc still leak on disk for a distressed writer, same
                    // as close()), it only recycles the pooled task objects.
                    releasePendingPurges();
                    chain.resetState();
                }
            }
        }
    }

    @Override
    public void commit() {
        checkNotPoisoned();
        flushAllPending();
        if (configuration.getCommitMode() != CommitMode.NOSYNC) {
            sync(configuration.getCommitMode() == CommitMode.ASYNC);
        }
    }

    // Sync order is .pv before .pk: a torn write must never leave the chain head
    // (keyMem) pointing at unsynced gen bytes in valueMem.
    //
    // The coverCount == 0 precondition holds on the covering rebuild path
    // (TableWriter.sealPostingIndexForPartition) only because
    // configureFollowerAndWriter forces close(), which resets coverCount to 0,
    // and configureCovering runs AFTER commitDense on that path. Do not move
    // configureCovering before commitDense: per-gen sidecars written here would
    // get overwritten by rebuildSidecars at a different sealTxn, leaving stale
    // bytes on disk in non-assert builds.
    public void commitDense() {
        checkNotPoisoned();
        assert coverCount == 0 : "commitDense does not write covered sidecars; use seal()/commit() for covering indexes";
        if (genCount > 0) {
            // flushAllPendingDense assumes it writes the first and only gen at
            // offset 0. That precondition breaks when the caller's preceding
            // add()-loop (a full index() rebuild over a large/squashed
            // partition) tripped the spill budget: compactIfOverBudget then ran
            // flushAllPending, persisting one or more sparse gens to this .pv.
            // A standalone flushAllPendingDense would extendHead(newGenCount=1),
            // overwriting gen-dir slot 0 to point at the freshly appended dense
            // gen -- orphaning those sparse gens (silently dropping the rows
            // they hold) and leaving the surviving gen-0 at a non-zero file
            // offset. The covering rebuildSidecars by-copy path reads gen-0 from
            // valueMem.addressOf(0) and SIGSEGVs on the layout mismatch; the
            // non-covering path returns short counts. Consolidate every gen into
            // a single dense gen-0 at offset 0 via the seal path instead: seal()
            // first runs flushAllPending to drain the final pending batch, then
            // re-encodes all gens into one dense gen-0 at offset 0 -- exactly the
            // shape the by-copy rebuild and the fast path both expect. (Whether
            // seal takes the full or incremental re-encode, gen-0 lands at offset
            // 0.) coverCount == 0 keeps seal() sidecar-free, matching
            // commitDense's contract; rebuildSidecars writes the sidecars after.
            seal();
        } else {
            flushAllPendingDense();
        }
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            boolean async = commitMode == CommitMode.ASYNC;
            if (valueMem.isOpen()) {
                valueMem.sync(async);
            }
            if (keyMem.isOpen()) {
                keyMem.sync(async);
            }
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
        this.coveredColumnAddrSizes.clear();
        this.coveredColumnAuxAddrs.clear();
        this.coveredColumnAuxAddrSizes.clear();
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

    public void configureCovering(
            LongList coveredColumnAddrs,
            LongList coveredColumnAuxAddrs,
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
        this.coveredColumnAddrSizes.clear();
        this.coveredColumnAuxAddrs.clear();
        this.coveredColumnAuxAddrSizes.clear();
        this.coveredColumnTops.clear();
        this.coveredColumnShifts.clear();
        this.coveredColumnIndices.clear();
        this.coveredColumnTypes.clear();
        for (int i = 0; i < coverCount; i++) {
            this.coveredColumnAddrs.add(coveredColumnAddrs.getQuick(i));
            this.coveredColumnAuxAddrs.add(coveredColumnAuxAddrs != null ? coveredColumnAuxAddrs.getQuick(i) : 0);
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
            long[] coveredColumnAuxAddrs,
            long[] coveredColumnTops,
            int[] coveredColumnShifts,
            int[] coveredColumnIndices,
            int[] coveredColumnTypes,
            int coverCount,
            int timestampColumnIndex
    ) {
        LongList addrs = new LongList(coverCount);
        LongList auxAddrs = coveredColumnAuxAddrs != null ? new LongList(coverCount) : null;
        LongList tops = new LongList(coverCount);
        IntList shifts = new IntList(coverCount);
        IntList indices = new IntList(coverCount);
        IntList types = new IntList(coverCount);
        for (int i = 0; i < coverCount; i++) {
            addrs.add(coveredColumnAddrs[i]);
            if (auxAddrs != null) auxAddrs.add(coveredColumnAuxAddrs[i]);
            tops.add(coveredColumnTops[i]);
            shifts.add(coveredColumnShifts[i]);
            indices.add(coveredColumnIndices[i]);
            types.add(coveredColumnTypes[i]);
        }
        configureCovering(addrs, auxAddrs, tops, shifts, indices, types, coverCount, timestampColumnIndex);
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
        configureCovering(addrs, null, tops, shifts, indices, types, coverCount, timestampColumnIndex);
    }

    @Override
    public void discardForRebuild() {
        checkNotPoisoned();
        if (!keyMem.isOpen()) {
            return;
        }
        // Drop pending and spill in-memory state, then rotate sealTxn to a
        // fresh value so the next commit() takes publishToChain's
        // appendNewEntry branch instead of mutating the existing head's
        // gen-dir slot 0 in place via extendHead. extendHead's protocol
        // assumes newGenCount > oldGenCount; without rotation a partition
        // whose head carries fast-lag-extended sparse gens (oldGenCount > 1)
        // would invoke extendHead with newGenCount=1 < oldGenCount, leaving
        // the previously-published entry's bytes mutated under its original
        // TXN_AT_SEAL -- visible to torn readers, snapshot-isolated readers
        // that walk to it as a prev entry, and recoveryDropAbandoned which
        // cannot detect the mutation (TXN_AT_SEAL is unchanged).
        //
        // The rotation mirrors truncate()'s block at the top of this file
        // but keeps the chain head and the OLD .pv on disk so concurrent
        // readers with active mmaps stay safe. The OLD .pv's purge is
        // deferred via pendingDiscardOldSealTxn: the next flushAllPending
        // records it right after publishToChain, when the chain has shape
        // [REBUILD@new, OLD@old] and recordPostingSealPurge can compute the
        // correct [OLD.txnAtSeal, REBUILD.txnAtSeal) window.
        //
        // The caller's republish runs in two steps:
        //   1. commit() -> flushAllPending appends the new gen to the
        //      freshly-opened .pv.{newSealTxn} at offset 0, then
        //      publishToChain takes the appendNewEntry branch (rotated
        //      sealTxn > head.sealTxn) and writes a fresh REBUILD chain
        //      entry. The OLD entry stays intact as the new prev.
        //   2. seal() / rebuildSidecars() rotates again to a further
        //      sealTxn, writes a dense single-gen .pv there, and appends a
        //      SEAL chain entry. REBUILD becomes prev to SEAL; both inherit
        //      the same txnAtSeal so the picker shadows REBUILD with SEAL,
        //      letting the seal-purge job remove .pv.{newSealTxn} on its
        //      next pass with an empty [X, X) window.
        freePendingBuffers();
        freeSpillData();
        keyCapacity = 0;
        keyCount = 0;
        genCount = 0;
        maxValue = -1L;
        hasPendingData = false;
        activeKeyCount = 0;
        if (chain.hasHead() && partitionPath.size() > 0) {
            long oldSealTxn = sealTxn;
            // peekNextSealTxn(), not sealTxn+1: after a recovery drop the
            // writer's sealTxn lags genCounter, and reusing a dropped
            // sealTxn would race the still-pending .pv purge.
            long newSealTxn = Math.max(1, chain.peekNextSealTxn());
            // Drop sidecar mappings tied to oldSealTxn -- the next
            // configureCovering / rebuildSidecars reopens them at the new
            // sealTxn.
            closeSidecarMems();
            valueMem.close(false, (byte) 0);
            Path p = Path.getThreadLocal(partitionPath);
            LPSZ fileName = PostingIndexUtils.valueFileName(p, indexName, postingColumnNameTxn, newSealTxn);
            valueMem.of(ff, fileName,
                    configuration.getDataIndexValueAppendPageSize(), 0L,
                    MemoryTag.MMAP_INDEX_WRITER, configuration.getWriterFileOpenOpts(), -1);
            sealTxn = newSealTxn;
            valueMemSize = 0;
            // The OLD .pv stays on disk until the deferred purge in
            // flushAllPending fires. If flushAllPending never runs (caller
            // closes without commit -- not a production path), the OLD .pv
            // is still chain-referenced by the OLD entry, while the new (empty)
            // .pv.{newSealTxn} was never published to the chain and so leaks
            // (bounded): no writer-open scan reclaims never-published sealTxn
            // files -- only the global purge job reclaims published-then-
            // superseded versions.
            pendingDiscardOldSealTxn = oldSealTxn;
        }
        allocateNativeBuffers();
    }

    @Override
    public void drainPendingFuturePurges(
            ObjList<PostingSealPurgeTask> sink,
            ObjectStackPool<PostingSealPurgeTask> pool,
            TableToken tableToken,
            int partitionBy,
            int timestampType,
            long currentTableTxn
    ) {
        if (pendingPurges.size() == 0 || partitionPath.size() == 0 || tableToken == null) {
            return;
        }
        int writePos = 0;
        for (int readPos = 0, n = pendingPurges.size(); readPos < n; readPos++) {
            PendingSealPurge entry = pendingPurges.getQuick(readPos);
            if (entry.partitionTimestamp != Long.MIN_VALUE
                    && entry.toTableTxn != Long.MAX_VALUE
                    && entry.toTableTxn > currentTableTxn) {
                PostingSealPurgeTask task = pool.next();
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
                        entry.toTableTxn
                );
                assert task.getToTableTxn() != Long.MAX_VALUE && task.getToTableTxn() > currentTableTxn;
                sink.add(task);
                entry.of(0L, 0L, 0L, 0L, Long.MIN_VALUE, -1L);
                pendingPurgePool.add(entry);
            } else {
                pendingPurges.setQuick(writePos++, entry);
            }
        }
        for (int i = pendingPurges.size() - 1; i >= writePos; i--) {
            pendingPurges.remove(i);
        }
    }

    @TestOnly
    public int getAdaptiveDeltaAtOrAbove() {
        return encodeCtx.adaptiveDeltaAtOrAbove;
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

    /**
     * Number of pending purge entries that have not yet been forwarded to
     * the global PostingSealPurge queue. Tests use this to verify that the
     * outbox is bounded under saturation.
     */
    @TestOnly
    public int getPendingPurgesSizeForTesting() {
        return pendingPurges.size();
    }

    @TestOnly
    public int getTimestampColumnIndex() {
        return timestampColumnIndex;
    }

    @TestOnly
    public long getValueMemSizeForTesting() {
        return valueMemSize;
    }

    /**
     * Whether the last rollback re-encoded the surviving rows via the per-key
     * streaming path ({@code true}) rather than truncating to an empty index
     * ({@code false}, every value rolled back). Lets a test assert the rollback
     * actually streamed the survivors instead of inferring it from a
     * regression-fragile headroom band. Reset on close/reopen.
     */
    @TestOnly
    public boolean isLastRollbackStreamingForTesting() {
        return isLastRollbackStreaming;
    }

    /**
     * Whether the last seal() committed to the incremental branch
     * ({@code true}) -- copying clean strides verbatim and merging only the
     * dirty ones -- rather than a full seal ({@code false}, set whenever
     * reencodeAllGenerations runs, including when sealIncremental's pre-flight
     * defers to it). Lets a test assert the incremental-vs-full-seal pre-flight
     * actually routed where it claims rather than inferring it from a
     * regression-fragile headroom band. Reset on close/reopen.
     */
    @TestOnly
    public boolean isLastSealIncrementalForTesting() {
        return isLastSealIncremental;
    }

    /**
     * Whether the last seal() abandoned its incremental candidacy because the
     * covered-column sidecar snapshot would not fit the live RSS headroom and
     * deferred to a full (streaming) seal instead. Lets a test assert the
     * snapshot pre-flight fired, distinguishing it from the later stride-merge
     * pre-flight (which also clears {@link #isLastSealIncremental}). Reset on
     * each seal() and on close/reopen.
     */
    @TestOnly
    public boolean isLastSealSnapshotDeferredForTesting() {
        return isLastSealSnapshotDeferred;
    }

    /**
     * Which path the last full seal() that reached the fast-vs-streaming
     * selection took: {@code true} for the per-key streaming compaction,
     * {@code false} for the fast stride-decoding path -- and {@code false} for an
     * incremental seal, which is neither (it copies clean strides and merges only
     * the dirty ones). Lets a test assert the RSS pre-flight actually routed to
     * streaming rather than inferring it from a regression-fragile headroom band.
     */
    @TestOnly
    public boolean isLastSealStreamingForTesting() {
        return isLastSealStreaming;
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
    public void of(CairoConfiguration configuration, long keyFd, long valueFd, boolean isInit, int blockCapacity) {
        throw new UnsupportedOperationException("fd-based open not supported for posting indexes");
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

    public void of(Path path, CharSequence name, long columnNameTxn, boolean isInit) {
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

            if (isInit) {
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                this.blockCapacity = BLOCK_CAPACITY;
                initKeyMemory(keyMem);
            } else {
                if (!ff.exists(keyFile)) {
                    throw CairoException.critical(0).put("index does not exist [path=").put(path).put(']');
                }

                // ff.length(keyFile) is safe as a MAP SIZE (it never exceeds the file's
                // backed extent) but must NOT be trusted as the live-data high-water: during
                // parallel WAL apply the reported length can lag the writer that extended this
                // .pk, which would leave the head entry past a length-sized mapping (the reopen
                // then reads past it). So seed the mapping from the length, then read the
                // header's regionLimit -- the live-region high-water, self-consistent with the
                // head entry it references -- and extend the mapping to it only when the
                // reported length genuinely lags. On the dominant path (regionLimit <=
                // keyFileSize) this leaves the whole entry region already visible to
                // openExisting without an extra mremap/fstat/madvise on every reopen.
                final long appendPageSize = configuration.getDataIndexKeyAppendPageSize();
                final long keyFileSize = ff.length(keyFile);
                if (keyFileSize < KEY_FILE_RESERVED) {
                    throw CairoException.critical(0)
                            .put("Index file too short [expected>=").put(KEY_FILE_RESERVED)
                            .put(", actual=").put(keyFileSize).put(']');
                }
                keyMem.of(ff, keyFile, appendPageSize, keyFileSize, MemoryTag.MMAP_INDEX_WRITER);
                final long regionLimit = chain.peekRegionLimit(keyMem);
                if (regionLimit > keyFileSize) {
                    keyMem.jumpTo(regionLimit);
                }
            }
            kFdUnassigned = false;

            if (isInit) {
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
                    isInit ? 0 : valueMemSize,
                    MemoryTag.MMAP_INDEX_WRITER
            );

            if (!isInit && valueMemSize > 0) {
                valueMem.jumpTo(valueMemSize);
            }

            allocateNativeBuffers();
        } catch (Throwable e) {
            // A failed (re)open leaves chain.regionLimit at the reset sentinel
            // (KEY_FILE_RESERVED): openExisting is what publishes the real
            // regionLimit into the chain, and it may not have run yet (a throw
            // from keyMem.of / peekRegionLimit / jumpTo, or from openExisting
            // itself). The normal truncating close() sizes its .pk trim from
            // that stale sentinel, so it would shrink the file back to
            // KEY_FILE_RESERVED and discard the live chain region [8192,
            // regionLimit) that is physically on disk. Release keyMem WITHOUT
            // truncation so the on-disk index is left intact for the next open;
            // the close() below then skips the already-closed keyMem. Do the
            // keyMem release in a try/finally so a failure to release it still
            // runs the full close() (freeing valueMem / native buffers and
            // resetting state) instead of leaking them.
            try {
                if (keyMem.isOpen()) {
                    keyMem.close(false);
                }
            } finally {
                close();
            }
            if (kFdUnassigned) {
                LOG.error().$("could not open posting index [path=").$(path).$(']').$();
            }
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void openFromO3Context(boolean isInit) {
        // Defense-in-depth: openFromO3Context clears the o3Ctx fields after of()
        // finishes, so a stale call without a matching setO3PathContext lands
        // here with zero-length name/path. Fail loud in production -- the
        // alternative is a silent open at the previous partition's path, which
        // would corrupt that partition's index.
        if (o3CtxPartitionPath.size() == 0 || o3CtxName.isEmpty()) {
            throw CairoException.critical(0)
                    .put("setO3PathContext must be called before openFromO3Context [indexName=").put(indexName)
                    .put(']');
        }
        Path p = Path.getThreadLocal(o3CtxPartitionPath);
        // of() internally calls close(), which resets o3CtxUpcomingTxn to -1.
        // The other o3Ctx fields survive close() because of() reads `name` from
        // o3CtxName mid-call; this method clears them after of() returns.
        long upcomingTxn = o3CtxUpcomingTxn;
        try {
            of(p, o3CtxName, o3CtxColumnNameTxn, isInit);
            if (upcomingTxn >= 0) {
                setNextTxnAtSeal(upcomingTxn);
            }
        } finally {
            o3CtxName.clear();
            o3CtxPartitionPath.clear();
            o3CtxColumnNameTxn = TableUtils.COLUMN_NAME_TXN_NONE;
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
            if (chain.hasHead() && chain.getHeadSealTxn() == entry.sealTxn) {
                // Defense-in-depth: never publish a purge for the chain HEAD's
                // sealTxn -- that .pv/.pc{c} is the live generation. A healthy
                // writer never schedules one: recordPostingSealPurge targets the
                // OLD sealTxn after a new head superseded it (predecessor entries
                // that still linger for pinned readers reference older sealTxns and
                // are purged normally under the job's scoreboard check), and
                // scheduleOrphanPurge targets a failed sealTxn the chain never made
                // head. Reaching here means a failed seal/rollback left the outbox
                // and the chain inconsistent; drop the entry rather than delete the
                // live head (the C1 endgame this backstops).
                LOG.critical().$("posting seal-purge skipped: sealTxn is the live chain head [")
                        .$("indexName=").$(indexName)
                        .$(", postingColumnNameTxn=").$(entry.postingColumnNameTxn)
                        .$(", sealTxn=").$(entry.sealTxn)
                        .$(']').$();
                entry.of(0L, 0L, 0L, 0L, Long.MIN_VALUE, -1L);
                pendingPurgePool.add(entry);
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
            // already protected by the regular reader-pin check. For finite
            // chain intervals, never clamp a future upper bound down to the
            // current committed txn: until that future txn is durable, the
            // superseded file is still the committed reader/recovery view.
            long toTableTxn;
            if (entry.toTableTxn == Long.MAX_VALUE) {
                toTableTxn = currentTableTxn;
            } else if (entry.toTableTxn > currentTableTxn) {
                pendingPurges.setQuick(writePos++, entry);
                continue;
            } else {
                toTableTxn = entry.toTableTxn;
            }
            long cursor = pubSeq.next();
            if (cursor < 0) {
                // Queue full or contended — keep entry for retry on next commit.
                pendingPurges.setQuick(writePos++, entry);
                continue;
            }
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
        checkNotPoisoned();
        if (coverCount <= 0 || genCount == 0 || keyCount == 0) {
            return;
        }
        closeSidecarMems();
        long gen0DirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), 0);
        int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);

        if (gen0KeyCount >= 0 && genCount == 1 && partitionPath.size() > 0) {
            // peekNextSealTxn(), not sealTxn+1: after a recovery drop the writer's
            // sealTxn lags genCounter, and reusing a dropped sealTxn would race
            // the still-pending .pv purge.
            final long oldSealTxn = sealTxn;
            final long newSealTxn = Math.max(1, chain.peekNextSealTxn());
            rebuildSidecarsByCopy(newSealTxn);
            // Record purge only after the new chain head is durably published;
            // an exception mid-rebuild must not queue oldSealTxn for unlink.
            if (sealTxn != oldSealTxn) {
                recordPostingSealPurge(oldSealTxn);
            }
        } else {
            seal();
        }
    }

    /**
     * Release the read-side state set up for the most recent seal: the
     * covered-column read mappings and the borrowed source-column addresses
     * passed in via {@link #configureCovering}. Keeps the covering schema
     * (coverCount, coveredColumnIndices, coveredColumnNames,
     * coveredColumnNameTxns, coveredColumnTops, coveredColumnShifts,
     * coveredColumnTypes, sidecarMems) intact so a subsequent commit() can
     * still publish a chain entry with a correctly-sized cover footer
     * sourced from {@link #sidecarMems}' append offsets at the last seal.
     * <p>
     * Called from {@code TableWriter}'s post-seal finally blocks where the
     * caller is about to munmap the covered-column files it mapped RO for
     * the seal. Without dropping the borrowed pointers held in
     * {@code coveredColumnAddrs} / {@code coveredColumnAuxAddrs} here, a
     * subsequent ensureCoveredColumnReadMaps would dereference garbage.
     * <p>
     * Use {@link #clearCovering()} only when truly tearing down covering
     * (writer discard, swap to a different cover schema). For the typical
     * "seal done, release temporary read mmaps, keep schema" lifecycle,
     * use this method - {@code clearCovering()} would zero {@code
     * coverCount} and the next {@code commit()}'s {@code
     * captureCoverEndOffsets} would short-circuit, dropping the cover
     * footer from the published chain entry.
     */
    public void releaseCoveredColumnReadMappings() {
        unmapCoveredColumnReads();
        this.coveredColumnAddrs.clear();
        this.coveredColumnAddrSizes.clear();
        this.coveredColumnAuxAddrs.clear();
        this.coveredColumnAuxAddrSizes.clear();
    }

    @Override
    public void rollbackConditionally(long row) {
        checkNotPoisoned();
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
        checkNotPoisoned();
        if (!keyMem.isOpen()) {
            return;
        }
        flushAllPending();

        if (genCount == 0 && keyCount == 0) {
            setMaxValue(maxValue);
            return;
        }

        // No indexed value lies above maxValue: the reencode would be a
        // no-op rewrite that bumps sealTxn and queues purge work. Mirror
        // rollbackConditionally's guard and keep the current seal generation
        // instead.
        if (getMaxValue() <= maxValue) {
            return;
        }

        LOG.info().$("rollback posting index [maxValue=").$(maxValue).$(", genCount=").$(genCount).$(", keyCount=").$(keyCount).$(']').$();
        rollbackToMaxValue(maxValue);
    }

    /**
     * Plants a pending purge whose sealTxn IS the current chain head, so a test
     * can drive publishPendingPurges' head-guard (which must drop the entry
     * rather than publish a purge for the live generation).
     */
    @TestOnly
    public void scheduleHeadPurgeForTesting() {
        scheduleOrphanPurge(chain.getHeadSealTxn());
    }

    public void seal() {
        checkNotPoisoned();
        isLastSealSnapshotDeferred = false;
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

        // peekNextSealTxn(), not sealTxn+1: after a recovery drop the
        // writer's sealTxn lags genCounter, and reusing a dropped
        // sealTxn would race the still-pending .pv purge.
        final long oldSealTxn = sealTxn;
        final long newSealTxn = Math.max(1, chain.peekNextSealTxn());
        // Captured for the pre-switch catch below: every encode path sets
        // valueMemSize to the staged compacted size before its failable sync/
        // switch, so a continuable pre-switch failure must restore it (else the
        // next gen would write at genOffset = valueMemSize into live sealed
        // bytes). Mirrors the rollback's not-switched branch.
        final long oldValueMemSize = valueMemSize;

        // Incremental seal: gen 0 dense covering every current key, all later
        // gens sparse. sealIncremental writes a fresh .pv via sealValueMem.
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

        boolean isSealed = false;
        try {
            // Sidecar snapshot lives inside this try so a malloc OOM mid-loop
            // gets cleaned up by the finally instead of leaking partial bufs.
            //
            // Each cover validates on the read-only mmap BEFORE paying the
            // copy, and the copy takes only the sealed region: gen header,
            // stride index and stride data, ending at sentinel + siSize. The
            // file tail past that point holds raw post-seal gen blocks that
            // no snapshot consumer reads: sealIncremental bounds clean-stride
            // copies by the stored sentinel and re-gathers dirty strides from
            // the column files, so copying the tail would be pure waste.
            //
            // Take the sealFull path, which rebuilds sidecars from column
            // data, whenever a live cover column's file is missing or
            // structurally not a sealed file. This rejects stale append-layout
            // sidecars from older writers or abandoned seal attempts before
            // the clean-stride copy can interpret them as sealed
            // stride-indexed data.
            if (haveSavedSidecars) {
                Path p = Path.getThreadLocal(partitionPath);
                int pp = p.size();
                int gen0SiSize = PostingIndexUtils.strideIndexSize(gen0KeyCount);
                long sentinelSlotOffset = PostingIndexUtils.PC_HEADER_SIZE + (long) PostingIndexUtils.strideCount(gen0KeyCount) * Long.BYTES;
                boolean snapshotExceedsHeadroom = false;
                for (int c = 0; c < coverCount; c++) {
                    if (coveredColumnIndices.getQuick(c) < 0) {
                        continue;
                    }
                    long covT = getCoveredColumnNameTxn(c);
                    LPSZ pcFile = PostingIndexUtils.coverDataFileName(p.trimTo(pp), indexName, c, postingColumnNameTxn, covT, sealTxn);
                    long fileLen = 0;
                    boolean isTrusted = false;
                    if (ff.exists(pcFile)) {
                        fileLen = ff.length(pcFile);
                        if (fileLen > 0) {
                            long fd = ff.openRO(pcFile);
                            if (fd >= 0) {
                                try {
                                    long mapped = ff.mmap(fd, fileLen, 0, Files.MAP_RO, MemoryTag.MMAP_INDEX_WRITER);
                                    if (mapped > 0) {
                                        try {
                                            // Validate against the real file length: gen header
                                            // offsets legitimately point into the post-seal tail
                                            // that the copy below leaves out.
                                            if (isTrustedSealedSidecarSnapshot(mapped, fileLen, gen0KeyCount)) {
                                                long sentinel = Unsafe.getLong(mapped + sentinelSlotOffset);
                                                long sealedLen = sentinel + gen0SiSize;
                                                // RSS pre-flight for the snapshot copy itself.
                                                // sealIncremental holds one native sidecar snapshot
                                                // per cover simultaneously and only pre-flights the
                                                // stride-merge buffers later (inside
                                                // reencodeAllGenerations) -- never these snapshots.
                                                // On a skewed symbol a single cover's sealed sidecar
                                                // is multi-GB, so an unguarded malloc here trips the
                                                // global RSS limit and suspends the table. Defer to
                                                // sealFull (which streams sidecars per-key from the
                                                // column files and needs no snapshot) when the copy
                                                // would not fit the live headroom. getRssMemUsed() is
                                                // re-read per cover so the check accounts for
                                                // snapshots already taken earlier in this loop.
                                                final long snapshotHeadroom = rssHeadroom();
                                                if (sealedLen > snapshotHeadroom) {
                                                    LOG.info().$("posting index incremental seal sidecar snapshot would exceed RSS headroom, sealing full [indexName=").$(indexName)
                                                            .$(", postingColumnNameTxn=").$(postingColumnNameTxn)
                                                            .$(", sealTxn=").$(sealTxn)
                                                            .$(", cover=").$(c)
                                                            .$(", snapshotBytes=").$(sealedLen)
                                                            .$(", headroom=").$(snapshotHeadroom)
                                                            .I$();
                                                    snapshotExceedsHeadroom = true;
                                                } else {
                                                    savedSidecarBufs[c] = Unsafe.malloc(sealedLen, MemoryTag.NATIVE_INDEX_READER);
                                                    savedSidecarSizes[c] = sealedLen;
                                                    Unsafe.copyMemory(mapped, savedSidecarBufs[c], sealedLen);
                                                    isTrusted = true;
                                                }
                                            }
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
                    if (snapshotExceedsHeadroom) {
                        // Any snapshots already taken for covers < c are freed by
                        // the finally; sealFull rebuilds every sidecar from the
                        // column files, so the incremental snapshot is pure waste here.
                        isIncrementalCandidate = false;
                        isLastSealSnapshotDeferred = true;
                        break;
                    }
                    if (!isTrusted) {
                        LOG.info().$("posting index sidecar snapshot not trusted, sealing full [indexName=").$(indexName)
                                .$(", postingColumnNameTxn=").$(postingColumnNameTxn)
                                .$(", sealTxn=").$(sealTxn)
                                .$(", cover=").$(c)
                                .$(", fileLen=").$(fileLen)
                                .I$();
                        isIncrementalCandidate = false;
                        break;
                    }
                }
            }

            if (coverCount > 0 && partitionPath.size() > 0) {
                closeSidecarMems();
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
            isSealed = true;
        } catch (Throwable th) {
            // Post-switch but pre-publish failure: the value file was switched
            // (sealTxn advanced to newSealTxn) but the chain head was not
            // published, so valueMem is at newSealTxn while the chain still
            // references oldSealTxn. Poison the writer -- a later op would publish
            // at newSealTxn and let the deferred purge delete a live file -- and
            // orphan-purge the staged files. A post-publish failure (chain head
            // already at newSealTxn) leaves the live files and the consistent
            // writer alone. Mirrors the rollback / rebuildSidecarsByCopy catches.
            if (sealTxn == newSealTxn && chain.getHeadSealTxn() != newSealTxn) {
                // Poisoning is a loud event -- log it with the same severity as
                // the rollback / rebuildSidecarsByCopy post-switch catches.
                // Poison FIRST, before the LOG record and scheduleOrphanPurge (both
                // allocate and can throw under heap pressure): a secondary failure must
                // still leave the half-switched writer poisoned, never re-drivable into
                // publishing a live chain entry at newSealTxn.
                isPoisoned = true;
                LOG.error().$("posting index seal post-switch failure, poisoning writer and scheduling orphan purge [")
                        .$("indexName=").$(indexName)
                        .$(", newSealTxn=").$(newSealTxn)
                        .$(']').$();
                scheduleOrphanPurge(newSealTxn);
            } else if (sealTxn != newSealTxn) {
                // Pre-switch failure: the switch never happened, so the chain and
                // valueMem still reference oldSealTxn. Free the staging map, unlink
                // the staged .pv/.pc directly (nothing references them yet), restore
                // valueMemSize (an encode path may have already advanced it to the
                // staged compacted size before throwing -- leaving it would corrupt
                // the next gen's append offset), and drop sealTarget -- mirrors the
                // rollback's not-switched branch. Without the unlink the staged files
                // leak permanently: the caller close()s a failed-seal writer and no
                // writer-open scan reclaims never-published sealTxn files.
                Misc.free(sealValueMem);
                closeSidecarMems();
                unlinkOrphanSealFiles(newSealTxn);
                valueMemSize = oldValueMemSize;
                sealTarget = null;
            }
            throw th;
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
            // Only record the superseded oldSealTxn for purge when the seal
            // actually published. On a failed seal the chain head is still
            // oldSealTxn (the live file), so recording its purge would lean
            // entirely on the liveness guard to avoid deleting it.
            if (isSealed && sealTxn != oldSealTxn) {
                recordPostingSealPurge(oldSealTxn);
            }
        }

        // Pair with sealFull/sealIncremental's path-based sync block: those
        // sync sealValueMem + sidecar + .pci only when partitionPath is set;
        // syncing .pk while .pv is still in mmap dirty pages would let a
        // power-loss reader see chain entries that reference unflushed data.
        if (partitionPath.size() > 0 && keyMem.isOpen()) {
            keyMem.sync(false);
        }
    }

    @Override
    public void sealIfMultiGen(int threshold) {
        if (keyMem.isOpen() && partitionPath.size() > 0 && genCount > threshold) {
            seal();
        }
    }

    public void setCoveredColumnAddrSizes(LongList dataSizes, LongList auxSizes) {
        coveredColumnAddrSizes.clear();
        coveredColumnAuxAddrSizes.clear();
        if (dataSizes != null) {
            coveredColumnAddrSizes.addAll(dataSizes);
        }
        if (auxSizes != null) {
            coveredColumnAuxAddrSizes.addAll(auxSizes);
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
        checkNotPoisoned();
        this.maxValue = maxValue;
        if (pendingDiscardOldSealTxn >= 0) {
            // discardForRebuild preserved the OLD chain head on disk for
            // concurrent readers and rotated valueMem to a fresh sealTxn,
            // but chain.headEntryOffset still points at the OLD entry.
            // Calling updateHeadMaxValue here would overwrite that
            // entry's MAX_VALUE field while leaving its other fields
            // (GEN_COUNT, KEY_COUNT, VALUE_MEM_SIZE, gen-dir slots,
            // cover footer) describing the pre-rebuild state -- the
            // OLD entry would become self-inconsistent and stay
            // visible to snapshot-isolated readers walking the prev
            // chain after the upcoming commit() turns it into a prev.
            // The next flushAllPending takes the appendNewEntry branch
            // (rotated sealTxn > head.sealTxn) and stamps maxValue
            // into the FRESH entry's header from the in-memory mirror
            // updated above, so skipping the on-disk republish here is
            // safe.
            return;
        }
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
    public void setO3PathContext(Path path, CharSequence name, long columnNameTxn, long upcomingTxn) {
        assert !keyMem.isOpen();
        o3CtxPartitionPath.clear();
        o3CtxPartitionPath.put(path);
        o3CtxName.clear();
        o3CtxName.put(name);
        o3CtxColumnNameTxn = columnNameTxn;
        o3CtxUpcomingTxn = upcomingTxn;
    }

    @Override
    public void setPartitionContext(long partitionTimestamp, long partitionNameTxn) {
        // The path-based of(...) overloads used by the parquet rebuild do not
        // carry the partition timestamp / name-txn, but a deferred seal-purge
        // task needs them to reconstruct the .pv directory after this writer is
        // freed. recordPostingSealPurge stamps each outbox entry with these, so
        // the caller sets them before commitDense seals.
        this.partitionTimestamp = partitionTimestamp;
        this.partitionNameTxn = partitionNameTxn;
    }

    @Override
    public void sync(boolean async) {
        checkNotPoisoned();
        // .pv before .pk: a torn write must never leave the chain head (keyMem)
        // pointing at unsynced gen bytes in valueMem.
        flushAllPending();
        if (valueMem.isOpen()) {
            valueMem.sync(async);
        }
        if (keyMem.isOpen()) {
            keyMem.sync(async);
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
        checkNotPoisoned();
        freeNativeBuffers();
        if (partitionPath.size() > 0) {
            // Create a new empty .pv file. The old .pv stays on disk so
            // concurrent readers with active mmaps don't SIGBUS.
            // If the new file open fails, valueMem is left closed — the writer
            // is degraded but safe to close() without further I/O errors.
            // peekNextSealTxn(), not sealTxn+1: after a recovery drop the
            // writer's sealTxn lags genCounter, and reusing a dropped
            // sealTxn would race the still-pending .pv purge.
            final long oldSealTxn = sealTxn;
            final long newTxn = Math.max(1, chain.peekNextSealTxn());
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
            initKeyMemory(keyMem, sealTxn);
            // Make the header rewrite durable before recording the purge of
            // the superseded .pv. PostingSealPurgeOperator unlinks with no
            // ordering against .pk writeback, so without this barrier a
            // power loss after the unlink journals but before the .pk page
            // writes back recovers the old chain head pointing at a deleted
            // .pv -- readers fail hard until REINDEX. Pairs with seal()'s
            // unconditional .pk sync.
            keyMem.sync(false);
            recordPostingSealPurge(oldSealTxn);
        }
        // initKeyMemory just rewrote the .pk header pages; resync the chain
        // helper's in-memory mirror to the new starting state (genCounter =
        // sealTxn - 1).
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
                // Raw copy for remaining fixed-width types: LONG128, UUID, LONG256, DECIMAL128/256
                Unsafe.putInt(destBuf, valueCount);
                Unsafe.copyMemory(rawBuf, destBuf + 4, (long) valueCount << shift);
                yield 4 + (valueCount << shift);
            }
        };
    }

    /**
     * Per-key encoding context overhead: {@code encodeCtx.ensureCapacity}
     * grows {@code efTrialAddr} (max ~9 bytes per value, smaller for
     * tiny counts where the EF header dominates) and
     * {@code efLowMaskedAddr} (8 bytes per value), plus block buffers
     * scaled to {@code count / BLOCK_CAPACITY}, plus fixed-size
     * residuals/native scratch sized to {@code BLOCK_CAPACITY * 8}
     * each. The peak is driven by the largest single key encoded, hence
     * sized to {@code maxKeyCount} for both the fast path (which
     * trial-encodes each key) and the streaming path (which encodes
     * each key directly into sealTarget but still grows the same
     * scratch).
     * <p>
     * Per-value coefficient: 9 (efTrial worst-case) + 8 (efLowMasked) +
     * 8 (deltas -- written by the DELTA/FoR encoder the adaptive path
     * trials on every key; previously omitted) + 1 (block buffers,
     * ~5/64 bytes per value rounded up). Plus a 2 KiB constant for
     * residuals and native scratch that exist regardless of count.
     * Note: deltas and efLowMasked grow geometrically (max(count,
     * cap*2)) and can transiently reach ~2x; charged here at 1x, as the
     * efLowMasked term always has been.
     */
    private static long encodeCtxPeakBytes(int maxKeyCount) {
        return (long) maxKeyCount * 26L + 2048L;
    }

    private static void initKeyMemory(MemoryMA keyMem, long startSealTxn) {
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

    private static long readSidecarOffsetWidened(MemoryMARW mem, long offsetsStart, long idx, boolean longOffsets) {
        return longOffsets
                ? Unsafe.getLong(mem.addressOf(offsetsStart + idx * Long.BYTES))
                : Unsafe.getInt(mem.addressOf(offsetsStart + idx * Integer.BYTES)) & 0xFFFFFFFFL;
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

    private static void writeVarOffset(MemoryMARW mem, long offsetsStart, int ordinal, long value, boolean longOffsets) {
        if (longOffsets) {
            mem.putLong(offsetsStart + (long) ordinal * Long.BYTES, value);
        } else {
            mem.putInt(offsetsStart + (long) ordinal * Integer.BYTES, (int) value);
        }
    }

    private int accumulateDenseGen0Counts(long totalCountsAddr) {
        long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), 0);
        long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
        int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
        long keyIdsBase = valueMem.addressOf(genFileOffset);
        int sc = PostingIndexUtils.strideCount(genKeyCount);
        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
        int maxStrideTotal = 0;
        for (int s = 0; s < sc; s++) {
            long strideOff = Unsafe.getLong(keyIdsBase + (long) s * Long.BYTES);
            long nextStrideOff = Unsafe.getLong(keyIdsBase + (long) (s + 1) * Long.BYTES);
            if (nextStrideOff == strideOff) continue;
            long strideAddr = keyIdsBase + siSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(genKeyCount, s);
            byte mode = Unsafe.getByte(strideAddr);
            int strideTotal = 0;
            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                for (int j = 0; j < ks; j++) {
                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                    int count = Unsafe.getInt(prefixAddr + (long) (j + 1) * Integer.BYTES)
                            - Unsafe.getInt(prefixAddr + (long) j * Integer.BYTES);
                    Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES, count);
                    strideTotal += count;
                }
            } else {
                long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                for (int j = 0; j < ks; j++) {
                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                    int count = Unsafe.getInt(countsAddr + (long) j * Integer.BYTES);
                    Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES, count);
                    strideTotal += count;
                }
            }
            if (strideTotal > maxStrideTotal) {
                maxStrideTotal = strideTotal;
            }
        }
        return maxStrideTotal;
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

    /**
     * Refresh {@link #coverEndOffsetsScratch} so it has exactly {@code coverCount}
     * entries and each slot reflects the current append offset of the matching
     * {@code sidecarMems} entry (or 0 when the slot is tombstoned / unopened).
     * The result is the authoritative valid-byte extent of each .pcN at the
     * moment the chain entry is republished.
     */
    private void captureCoverEndOffsets() {
        coverEndOffsetsScratch.clear();
        // The transient coverCount field is reset to 0 mid-reseal (clearCovering
        // before the next configureCovering), and the sidecar handles are closed
        // after each seal -- but the index is still covering on disk. Republishing
        // the chain head in that window with an empty footer (coverCount==0) makes
        // PostingIndexChainWriter.extendHead shrink the entry LEN to exclude the
        // cover footer, so a concurrent covered read of the republished entry sees
        // sidecarFileEndOffsets==0, fails to map the existing .pc and returns NULL
        // for the whole partition -- the native in-place reseal covered-read race.
        // Fall back to the last-known cover layout so the entry keeps its footer.
        int effectiveCoverCount = coverCount > 0 ? coverCount : coverEndOffsetsCache.size();
        if (effectiveCoverCount <= 0) {
            return;
        }
        // Keep the cache sized to the live cover count when it is known, so a later
        // transient coverCount==0 republish reuses exactly the right layout (and a
        // covering reconfigure that shrinks the cover set cannot over-report).
        if (coverCount > 0 && coverEndOffsetsCache.size() != coverCount) {
            int old = coverEndOffsetsCache.size();
            coverEndOffsetsCache.setPos(coverCount);
            for (int c = old; c < coverCount; c++) {
                coverEndOffsetsCache.setQuick(c, 0L);
            }
        }
        coverEndOffsetsScratch.setPos(effectiveCoverCount);
        for (int c = 0; c < effectiveCoverCount; c++) {
            long endOffset;
            MemoryMARW mem = c < sidecarMems.size() ? sidecarMems.getQuick(c) : null;
            if (mem != null && mem.isOpen()) {
                // Handle open: its append offset is the authoritative valid extent
                // of this .pcN. Remember it so a later republish can reuse it.
                endOffset = mem.getAppendOffset();
                coverEndOffsetsCache.setQuick(c, endOffset);
            } else {
                // Handle closed / transient coverCount==0: reuse the last-known
                // extent. The .pc on disk for this head's sealTxn is unchanged. A
                // genuinely never-written slot keeps its cached 0 and publishes 0.
                endOffset = coverEndOffsetsCache.getQuick(c);
            }
            coverEndOffsetsScratch.setQuick(c, endOffset);
        }
    }

    private void checkNotPoisoned() {
        if (isPoisoned) {
            throw CairoException.critical(0)
                    .put("posting index writer is poisoned by a seal, rollback or sidecar rebuild that failed after the value file switch; close and reopen the writer [index=")
                    .put(indexName).put(']');
        }
    }

    private void closeSidecarMems() {
        Misc.freeObjListAndClear(sidecarMems);
        sidecarInfoMem = Misc.free(sidecarInfoMem);
    }

    /**
     * Mid-stream drain when the per-key spill arena exceeds
     * {@link #indexerSpillBytesMax}. Encodes the in-memory pending+spill
     * state into a fresh sparse generation in {@code valueMem}, publishes
     * it to the chain, and frees the per-key spill buffers so the
     * arena is reclaimed. The {@link #add} call that drove us here can
     * complete its post-{@link #spillKey} write without dereferencing a
     * freed pointer because the pending arrays stay live -- with one
     * exception: {@link #flushAllPending} triggers an inline {@link #seal}
     * if its {@code genCount} hits {@link #MAX_GEN_COUNT}, and that seal
     * frees pending. The trailing {@code allocateNativeBuffers} below
     * re-establishes pending in that case; {@code keyCount} is preserved
     * by both flush and seal so the post-spillKey write to a key already
     * past {@link #PENDING_SLOT_CAPACITY} adds (its first add must have
     * bumped keyCount past it) stays within the new keyCapacity.
     * <p>
     * Why free spill on every trigger but pending only when the inline
     * seal does it: spill grows linearly with rows indexed for hot keys
     * (the unbounded blow-up the reported OOM exercised); pending is
     * bounded by symbol cardinality times {@code PENDING_SLOT_CAPACITY *
     * Long.BYTES}, a fixed cost we pay once per writer lifetime rather
     * than per indexing batch. Freeing pending on every trigger would
     * force a 64-bytes-per-key realloc on the very next {@link #add},
     * costing tens of milliseconds per flush cycle for the
     * high-cardinality cases this fix targets. {@link #seal} still
     * frees both before the seal-time reencode -- that path is
     * end-of-batch so the realloc cost is amortised across the entire
     * next batch.
     * <p>
     * Called from {@link #spillKey} after the per-key buffer grow.
     * Deliberately not called from the merge-spill grow site inside
     * {@link #flushAllPending}: that site only runs while we are already
     * draining, the encoded data lands in {@code valueMem} immediately
     * after, and {@link #resetSpill} clears the per-key counts on the
     * way out. Re-entering {@code flushAllPending} from inside itself
     * would recurse.
     * <p>
     * No-op when:
     * <ul>
     *   <li>{@code indexerSpillBytesMax <= 0} (operator disabled the
     *       back-pressure)</li>
     *   <li>{@code totalSpillBytes <= indexerSpillBytesMax} (still within
     *       budget)</li>
     *   <li>nothing is pending or spilled (defensive)</li>
     * </ul>
     */
    private void compactIfOverBudget() {
        if (indexerSpillBytesMax <= 0 || totalSpillBytes <= indexerSpillBytesMax) {
            return;
        }
        if (!hasPendingData && !hasSpillData) {
            return;
        }
        LOG.debug().$("posting index periodic flush [totalSpillBytes=").$(totalSpillBytes)
                .$(", threshold=").$(indexerSpillBytesMax)
                .$(", genCount=").$(genCount).I$();
        flushAllPending();
        freeSpillData();
        // flushAllPending may have triggered the inline seal at MAX_GEN_COUNT,
        // which calls freePendingBuffers and leaves pendingCountsAddr == 0.
        // The in-flight spillKey caller (add) is about to write into
        // pendingValuesAddr after we return; reallocate so it does not
        // dereference a freed pointer. allocateNativeBuffers preserves
        // keyCount, so the post-spillKey write to a key that already
        // existed when spillKey ran (count >= PENDING_SLOT_CAPACITY
        // means that key was seen at least 8 times, so its first add
        // bumped keyCount past it) stays within the new keyCapacity.
        if (pendingCountsAddr == 0) {
            allocateNativeBuffers();
        }
    }

    private long computeStrideTrialBufSize(int ks, int[] keyCounts) {
        long total = 0;
        for (int j = 0; j < ks; j++) {
            total += PostingIndexUtils.computeMaxEncodedSize(keyCounts[j]);
        }
        return total;
    }

    private void copyStrideFromGen0(long gen0Addr, int gen0KeyCount, int gen0SiSize, int stride) {
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
        // Copy gen 0's stride bytes straight into the sealed value file.
        // putBlockOfBytes remaps only its destination (sealTarget is always
        // sealValueMem during a seal -- a mapping distinct from valueMem), so
        // reading srcAddr out of valueMem stays valid with no bounce buffer.
        sealTarget.putBlockOfBytes(srcAddr, strideSize);
    }

    /**
     * Decode a single key's values from a dense generation. Used by the
     * per-key streaming compaction path. {@code stride} is the stride
     * index in this gen; {@code j} is the key offset within the stride.
     * Returns the number of values decoded into {@code dstAddr} (0 if the
     * stride is empty in this gen, or the key has no values).
     * <p>
     * Per-key analogue of {@link #decodeDenseGenStride}: same on-disk
     * layout, just isolated to one key. Mode handling is identical
     * (FLAT and DELTA), since the gen's stride header carries the mode
     * for the whole stride.
     */
    private int decodeDenseGenSingleKey(
            long genBase, int genKeyCount, int stride, int j, long dstAddr, int capacity
    ) {
        int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
        long strideOff = Unsafe.getLong(genBase + (long) stride * Long.BYTES);
        long nextStrideOff = Unsafe.getLong(genBase + (long) (stride + 1) * Long.BYTES);
        if (nextStrideOff == strideOff) {
            return 0;
        }
        long strideAddr = genBase + siSize + strideOff;
        byte mode = Unsafe.getByte(strideAddr);
        int genKs = PostingIndexUtils.keysInStride(genKeyCount, stride);
        if (j >= genKs) {
            return 0;
        }
        if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
            int bitWidth = Unsafe.getByte(strideAddr + 1) & 0xFF;
            long baseValue = Unsafe.getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
            long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
            int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(genKs);
            long flatDataAddr = strideAddr + flatHeaderSize;
            int startIdx = Unsafe.getInt(prefixAddr + (long) j * Integer.BYTES);
            int count = Unsafe.getInt(prefixAddr + (long) (j + 1) * Integer.BYTES) - startIdx;
            if (count == 0) {
                return 0;
            }
            if (count < 0 || count > capacity) {
                throw CairoException.critical(0)
                        .put("corrupt posting index: dense FLAT key count exceeds buffer [count=").put(count)
                        .put(", capacity=").put(capacity).put(']');
            }
            BitpackUtils.unpackValuesFrom(flatDataAddr, startIdx, count, bitWidth, baseValue, dstAddr);
            return count;
        } else {
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            long genOffsetsBase = countsAddr + (long) genKs * Integer.BYTES;
            int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(genKs);
            int count = Unsafe.getInt(countsAddr + (long) j * Integer.BYTES);
            if (count == 0) {
                return 0;
            }
            long dataOff = Unsafe.getLong(genOffsetsBase + (long) j * Long.BYTES);
            long encodedAddr = strideAddr + deltaHeaderSize + dataOff;
            PostingIndexUtils.decodeKeyToNative(encodedAddr, dstAddr, decodeCtx, capacity);
            return count;
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
                // A non-monotonic prefix (corruption) yields a negative count;
                // unguarded it drives keyOffsets[j] negative and a later gen's write
                // underflows strideValsAddr. The positive case cannot overflow -- the
                // prefix IS the buffer-sizing source. Mirrors the dense FLAT
                // single-key decoder's count<0 guard.
                if (count < 0) {
                    throw CairoException.critical(0)
                            .put("corrupt posting index: dense FLAT stride key count negative [count=").put(count).put(']');
                }

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
                // Bound the decode by the gen-dir count -- strideValsAddr is sized
                // from the same per-key counts, so a corrupt DELTA block whose
                // internal value count exceeds count would otherwise overflow it.
                PostingIndexUtils.decodeKeyToNative(encodedAddr, destAddr, decodeCtx, count);
                keyOffsets[j] += count;
            }
        }
    }

    /**
     * Decode a single key's values from a sparse generation using binary
     * search on the sorted keyIds array. Returns the number of values
     * decoded into {@code dstAddr} (0 if the key is absent from this gen).
     * <p>
     * Per-key analogue of {@link #decodeSparseGenStride}: same on-disk
     * layout, isolated to one key.
     */
    private int decodeSparseGenSingleKey(
            long genBase, int activeKeyCount, int key, long dstAddr, int capacity
    ) {
        long countsBase = genBase + (long) activeKeyCount * Integer.BYTES;
        long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
        int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);

        // Binary search for exact keyId match
        int lo = 0, hi = activeKeyCount - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int midKey = Unsafe.getInt(genBase + (long) mid * Integer.BYTES);
            if (midKey < key) {
                lo = mid + 1;
            } else if (midKey > key) {
                hi = mid - 1;
            } else {
                int count = Unsafe.getInt(countsBase + (long) mid * Integer.BYTES);
                if (count == 0) {
                    return 0;
                }
                long dataOffset = Unsafe.getLong(offsetsBase + (long) mid * Long.BYTES);
                long encodedAddr = genBase + headerSize + dataOffset;
                PostingIndexUtils.decodeKeyToNative(encodedAddr, dstAddr, decodeCtx, capacity);
                return count;
            }
        }
        return 0;
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
            // Bound the decode by the gen-dir count -- strideValsAddr is sized from
            // the same per-key counts, so a corrupt block whose internal value count
            // exceeds count would otherwise overflow it.
            PostingIndexUtils.decodeKeyToNative(encodedAddr, destAddr, decodeCtx, count);
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

    private void encodeStrideBlock(
            int ks, int[] keyCounts, long[] keyOffsets, long strideValsAddr,
            int[] bpKeySizes, long bpTrialBuf, long localHeaderBuf
    ) {
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
        int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);

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

        if (totalStrideValuesL > Integer.MAX_VALUE) {
            useFlat = false;
        } else {
            int totalStrideValues = (int) totalStrideValuesL;
            long strideRange = strideMaxValue - strideMinValue;
            int naturalBitWidth = strideRange <= 0 ? 1 : BitpackUtils.bitsNeeded(strideRange);
            int alignedBitWidth = maybeAlignBitWidth(naturalBitWidth, alignedBitWidthThreshold);
            int naturalFlatDataSize = BitpackUtils.packedDataSize(totalStrideValues, naturalBitWidth);

            if (alignedBitWidth != naturalBitWidth) {
                int alignedFlatDataSize = BitpackUtils.packedDataSize(totalStrideValues, alignedBitWidth);
                int alignedFlatSize = flatHeaderSize + alignedFlatDataSize;
                if (alignedFlatSize < deltaSize) {
                    localBitWidth = alignedBitWidth;
                    flatDataSize = alignedFlatDataSize;
                } else {
                    localBitWidth = naturalBitWidth;
                    flatDataSize = naturalFlatDataSize;
                }
            } else {
                localBitWidth = naturalBitWidth;
                flatDataSize = naturalFlatDataSize;
            }
            int flatSize = flatHeaderSize + flatDataSize;
            useFlat = flatSize < deltaSize;
        }

        if (useFlat) {
            writePackedStride(ks, keyCounts, keyOffsets, localBitWidth, strideMinValue,
                    flatHeaderSize, flatDataSize, localHeaderBuf, strideValsAddr);
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
            for (int c = 0; c < coverCount; c++) {
                coveredColReadAddrs[c] = coveredColumnAddrs.getQuick(c);
                coveredColReadSizes[c] = 0;
                if (coveredColumnAuxAddrs.size() > c) {
                    coveredAuxReadAddrs[c] = coveredColumnAuxAddrs.getQuick(c);
                    coveredAuxReadSizes[c] = 0;
                }
            }
        }
    }

    /**
     * Ensure the streaming FSST scratch buffers are sized to
     * {@link #FSST_BATCH_SIZE}. All four are anonymous-heap and small
     * (a few tens of KiB combined); they get reused across strides and
     * are freed by {@link #freeFsstScratch} on close. fsstCmpAddr starts
     * at {@link #FSST_BATCH_OUT_CAP_MIN} and grows on demand inside
     * {@link #tryFsstStreamingCompress} if a batch reports produced=0.
     */
    private void ensureFsstStreamingScratch() {
        final long offsArrayBytes = (long) (FSST_BATCH_SIZE + 1) * Long.BYTES;
        final long batchScratchBytes = (long) FSST_BATCH_SIZE * FSSTNative.BATCH_SCRATCH_BYTES_PER_VALUE;
        if (fsstSrcOffsCap < offsArrayBytes) {
            fsstSrcOffsAddr = Unsafe.realloc(fsstSrcOffsAddr, fsstSrcOffsCap, offsArrayBytes, MemoryTag.NATIVE_INDEX_READER);
            fsstSrcOffsCap = offsArrayBytes;
        }
        if (fsstCmpOffsCap < offsArrayBytes) {
            fsstCmpOffsAddr = Unsafe.realloc(fsstCmpOffsAddr, fsstCmpOffsCap, offsArrayBytes, MemoryTag.NATIVE_INDEX_READER);
            fsstCmpOffsCap = offsArrayBytes;
        }
        if (fsstBatchScratchCap < batchScratchBytes) {
            fsstBatchScratchAddr = Unsafe.realloc(fsstBatchScratchAddr, fsstBatchScratchCap, batchScratchBytes, MemoryTag.NATIVE_INDEX_READER);
            fsstBatchScratchCap = batchScratchBytes;
        }
        if (fsstCmpCap < FSST_BATCH_OUT_CAP_MIN) {
            fsstCmpAddr = Unsafe.realloc(fsstCmpAddr, fsstCmpCap, FSST_BATCH_OUT_CAP_MIN, MemoryTag.NATIVE_INDEX_READER);
            fsstCmpCap = FSST_BATCH_OUT_CAP_MIN;
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

    /**
     * Live RSS headroom in bytes for the given limit/usage snapshot:
     * {@code limit - used} floored at 0, or {@link Long#MAX_VALUE} when no limit
     * is configured ({@code limit <= 0}). Single source of truth for every seal/
     * rollback RSS pre-flight so the copies cannot drift apart -- a stale copy
     * would silently let a path that should defer/throw OOM and suspend the table.
     * The caller passes a consistent {@code used} snapshot when it also needs the
     * raw figure for a diagnostic; {@link #rssHeadroom()} reads a fresh pair.
     */
    private static long rssHeadroom(long rssLimit, long rssUsed) {
        return rssLimit > 0 ? Math.max(0L, rssLimit - rssUsed) : Long.MAX_VALUE;
    }

    /**
     * Live RSS headroom from the current limit and usage. See
     * {@link #rssHeadroom(long, long)}.
     */
    private static long rssHeadroom() {
        return rssHeadroom(Unsafe.getRssMemLimit(), Unsafe.getRssMemUsed());
    }

    /**
     * Conservative upper bound on the anonymous-heap footprint of the
     * fast-path seal compaction. Accounts for: strideVals decode buffer,
     * packedResiduals scratch (FLAT-mode encoder), bpTrialBuf (per-stride
     * trial DELTA encode, sized exactly via the maxStrideTrialSize
     * computed in Phase 1), encodeCtx grow-on-demand scratch,
     * worst-case per-stride sidecar buffer (one cover column at a
     * time), sidecar workspaces (sized to the worst single key), and
     * the already-allocated per-key counts table.
     * <p>
     * The valueMem and sealValueMem mappings do not count: their RSS
     * footprint is paged in/out by the OS, bounded by working set rather
     * than file size. Same for keyMem and the sidecar mmaps. Anonymous-
     * heap mallocs do count -- those are what Unsafe#checkAllocLimit
     * gates.
     */
    private long estimateFastPathPeakBytes(int maxStrideTotal, int maxKeyCount, long maxStrideTrialSize, long maxColValueSize) {
        long peak = 2L * maxStrideTotal * Long.BYTES;                  // strideVals + packedResiduals
        peak += maxStrideTrialSize;                                    // bpTrialBuf, exact per-stride trial total
        // FLAT mode strides allocate a packedBuf of size flatDataSize
        // inside writePackedStride; flatDataSize is bounded by
        // BitpackUtils.packedDataSize(totalStrideValues, 64) which
        // hits maxStrideTotal * 8 worst-case (64-bit-per-value
        // bit-packing). Lives concurrently with bpTrialBuf,
        // strideVals, and packedResiduals during the encode call.
        peak += (long) maxStrideTotal * Long.BYTES;                    // packedBuf in writePackedStride (FLAT mode)
        peak += encodeCtxPeakBytes(maxKeyCount);                       // efTrial + efLowMasked + block bufs + fixed scratch
        peak += sealAuxiliaryBufferBytes();                            // localHeaderBuf + strideIndexBuf
        if (coverCount > 0 && maxColValueSize > 0) {
            peak += (long) maxStrideTotal * maxColValueSize;           // worst-case sidecarBuf for the largest cover col
            peak += peakCoverColumnCompressBufBytes(maxKeyCount);      // ALP compressBuf for the worst cover col
            peak += (long) maxKeyCount * (Long.BYTES + Byte.BYTES);    // longWorkspace + exceptionWorkspace (fixed covers only)
        }
        peak += peakVarCoverFsstScratchBytes();                       // FSST scratch for var-size covers
        peak += (long) keyCount * Integer.BYTES;                       // totalCountsAddr (already allocated, kept in budget)
        return peak;
    }

    /**
     * Conservative upper bound on the anonymous-heap footprint of the
     * streaming compaction. Streaming encodes directly into sealTarget
     * (mmap, off-budget) so it does not allocate bpTrialBuf or
     * packedResiduals. The keyBuffer holds one key's decoded values,
     * encodeCtx still grows on demand to fit the worst key, the
     * per-stride sidecarBuf is sized to the worst single key, and the
     * workspaces are unchanged from the fast path.
     */
    private long estimateStreamingPathPeakBytes(int maxKeyCount, long maxColValueSize) {
        return estimateStreamingPathPeakBytes(maxKeyCount, maxColValueSize, true);
    }

    /**
     * @param isSidecarWritten whether this encode actually rebuilds cover
     *                         sidecars. The seal always does (when covers
     *                         exist); the rollback only does for path-based
     *                         covers -- an addr-based-cover or non-covered
     *                         rollback skips the sidecar write entirely
     *                         (sidecarMems is empty), so it allocates none of
     *                         the per-stride sidecarBuf, ALP compressBuf, FSST
     *                         batch floor or the per-key long/exception
     *                         workspaces. Charging them over-states the rollback
     *                         peak 2-4x and spuriously rejects a rollback that
     *                         actually fits.
     */
    private long estimateStreamingPathPeakBytes(int maxKeyCount, long maxColValueSize, boolean isSidecarWritten) {
        long peak = (long) maxKeyCount * Long.BYTES;                   // keyBuffer
        peak += encodeCtxPeakBytes(maxKeyCount);                       // efTrial + efLowMasked + block bufs + fixed scratch
        peak += sealAuxiliaryBufferBytes();                            // localHeaderBuf + strideIndexBuf
        if (isSidecarWritten) {
            if (coverCount > 0 && maxColValueSize > 0) {
                peak += (long) maxKeyCount * maxColValueSize;          // streaming sidecarBuf
                peak += peakCoverColumnCompressBufBytes(maxKeyCount);  // ALP compressBuf for the worst cover col
                peak += (long) maxKeyCount * (Long.BYTES + Byte.BYTES);// longWorkspace + exceptionWorkspace (fixed covers only)
            }
            peak += peakVarCoverFsstScratchBytes();                   // streaming FSST scratch for var-size covers
        }
        peak += (long) keyCount * Integer.BYTES;                       // totalCountsAddr
        return peak;
    }

    /**
     * Pass 1 of the streaming rollback (runs for all cover shapes).
     * Decodes each key from every generation into {@code keyBuffer},
     * binary-searches the rollback
     * cutoff (the per-key run is sorted ascending, so survivors are the leading
     * prefix &lt;= {@code maxValueCutoff}), and overwrites
     * {@code totalCountsAddr[key]} with the surviving count. Returns the new key
     * count (highest surviving key + 1); 0 means every value was rolled back and
     * the caller truncates. Peak heap is one key ({@code maxKeyCount} longs), not
     * the whole index. {@link #reencodeWithPerKeyStreaming} then re-decodes each
     * key and encodes the surviving prefix -- so a rollback decodes each surviving
     * key twice (this pass to find the cutoff, then the re-encode). That is the
     * deliberate cost of bounding peak memory at one key: holding the decoded
     * survivors to skip the second decode would reintroduce the whole-index buffer
     * this streaming rollback exists to avoid.
     */
    private int filterCountsForRollback(long maxValueCutoff, long totalCountsAddr, long keyBuffer, int maxKeyCount) {
        int newKeyCount = 0;
        final int oldKeyCount = keyCount;
        // Resolve per-gen metadata into reusable scratch (valueMem is read-only
        // in this pass, so the base addresses stay stable).
        resolveGenMetadataScratch();
        final long[] genBases = genMetaBases;
        final int[] genKeyCounts = genMetaKeyCounts;
        final int[] genStrideCounts = genMetaStrideCounts;
        for (int key = 0; key < oldKeyCount; key++) {
            int origCount = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
            if (origCount == 0) {
                continue;
            }
            int stride = key / PostingIndexUtils.DENSE_STRIDE;
            int j = key % PostingIndexUtils.DENSE_STRIDE;
            // Decode the key from EVERY generation. The per-key concatenation is
            // ascending (the indexer feeds each key's row ids monotonically), so the
            // surviving prefix is found by a single binary search. Per-gen
            // GEN_DIR_OFFSET_MAX_VALUE is a GLOBAL max across all keys, NOT a per-key
            // bound, so it cannot be used to skip generations per key: keys from
            // different commits interleave by row id, so a low-row-id key can still
            // have survivors in a generation whose global max exceeds the cutoff.
            int decodedTotal = 0;
            for (int gen = 0; gen < genCount; gen++) {
                int genKeyCount = genKeyCounts[gen];
                long appendAddr = keyBuffer + (long) decodedTotal * Long.BYTES;
                int capacity = maxKeyCount - decodedTotal;
                if (genKeyCount < 0) {
                    decodedTotal += decodeSparseGenSingleKey(genBases[gen], -genKeyCount, key, appendAddr, capacity);
                } else if (stride < genStrideCounts[gen]) {
                    decodedTotal += decodeDenseGenSingleKey(genBases[gen], genKeyCount, stride, j, appendAddr, capacity);
                }
            }
            // decodedTotal is the full per-key count totalCountsAddr held before this
            // pass; a mismatch means the encoded blocks disagree with the gen-dir
            // counts (corruption). maxKeyCount (the full max) bounds keyBuffer.
            assert decodedTotal <= maxKeyCount;
            if (decodedTotal != origCount) {
                throw CairoException.critical(0)
                        .put("posting index rollback decode mismatch [key=").put(key)
                        .put(", expected=").put(origCount)
                        .put(", decoded=").put(decodedTotal).put(']');
            }
            int lo = 0, hi = decodedTotal - 1, cut = -1;
            while (lo <= hi) {
                int mid = (lo + hi) >>> 1;
                long midVal = Unsafe.getLong(keyBuffer + (long) mid * Long.BYTES);
                if (midVal <= maxValueCutoff) {
                    cut = mid;
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
            int newCount = cut + 1;
            Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES, newCount);
            if (newCount > 0) {
                newKeyCount = key + 1;
            }
        }
        return newKeyCount;
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
                        // Long-arithmetic doubling, see spillKey for rationale.
                        long doubled = (long) curCap * 2L;
                        long want = Math.max(needed, doubled);
                        if (want > Integer.MAX_VALUE) {
                            throw CairoException.critical(0)
                                    .put("posting index spill capacity exceeds 2^31 entries [key=").put(key)
                                    .put(", needed=").put(needed)
                                    .put(", curCap=").put(curCap)
                                    .put("]; split commit into smaller batches");
                        }
                        int newCap = (int) want;
                        long oldSize = (long) curCap * Long.BYTES;
                        long newSize = (long) newCap * Long.BYTES;
                        long oldAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                        long newAddr = Unsafe.realloc(oldAddr, oldSize, newSize, MemoryTag.NATIVE_INDEX_READER);
                        Unsafe.putLong(spillKeyAddrsAddr + (long) key * Long.BYTES, newAddr);
                        Unsafe.putInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES, newCap);
                        totalSpillBytes += newSize - oldSize;
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

        // Record the OLD .pv's purge if discardForRebuild rotated sealTxn
        // before this flush. The chain shape is now [REBUILD@new, OLD@old]
        // so recordPostingSealPurge sees second.txnAtSeal = OLD.txnAtSeal
        // and head.txnAtSeal = REBUILD.txnAtSeal, producing the correct
        // [OLD.txnAtSeal, REBUILD.txnAtSeal) window.
        if (pendingDiscardOldSealTxn >= 0) {
            recordPostingSealPurge(pendingDiscardOldSealTxn);
            pendingDiscardOldSealTxn = -1L;
        }

        // Clear only the active keys' pending counts (not the entire array)
        for (int i = 0; i < activeKeyCount; i++) {
            int key = activeKeyIds[i];
            Unsafe.putInt(pendingCountsAddr + (long) key * Integer.BYTES, 0);
        }
        resetSpill();
        hasPendingData = false;
        activeKeyCount = 0;

        // Soft cap on per-entry gen count; trades seal frequency against
        // entry size.
        if (genCount >= MAX_GEN_COUNT) {
            seal();
        }
    }

    private void flushAllPendingDense() {
        if (!hasPendingData || pendingCountsAddr == 0 || activeKeyCount == 0) {
            return;
        }

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

        long totalCountsSize = (long) keyCount * Integer.BYTES;
        long totalCountsAddr = Unsafe.malloc(totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
        long strideValsAddr = 0;
        long strideValsBytes = 0;
        try {
            Unsafe.setMemory(totalCountsAddr, totalCountsSize, (byte) 0);
            long totalValues = 0;
            long maxValueAfter = this.maxValue;
            for (int i = 0; i < activeKeyCount; i++) {
                int key = activeKeyIds[i];
                int pendingCount = Unsafe.getInt(pendingCountsAddr + (long) key * Integer.BYTES);
                int spillCount = getSpillCount(key);
                int count = pendingCount + spillCount;
                Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES, count);
                totalValues += count;
                if (pendingCount > 0) {
                    long pendingSrc = pendingValuesAddr + (long) key * PENDING_SLOT_CAPACITY * Long.BYTES;
                    long lastVal = Unsafe.getLong(pendingSrc + (long) (pendingCount - 1) * Long.BYTES);
                    if (lastVal > maxValueAfter) maxValueAfter = lastVal;
                } else if (spillCount > 0) {
                    long spillAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                    long lastVal = Unsafe.getLong(spillAddr + (long) (spillCount - 1) * Long.BYTES);
                    if (lastVal > maxValueAfter) maxValueAfter = lastVal;
                }
            }

            if (totalValues == 0) {
                hasPendingData = false;
                activeKeyCount = 0;
                resetSpill();
                return;
            }
            if (totalValues > Integer.MAX_VALUE) {
                throw CairoException.critical(0)
                        .put("posting index gen exceeds 2^31 values [totalValues=").put(totalValues)
                        .put("]; split commit into smaller batches");
            }

            int sc = PostingIndexUtils.strideCount(keyCount);
            int siSize = PostingIndexUtils.strideIndexSize(keyCount);
            int maxStrideTotal = 0;
            for (int s = 0; s < sc; s++) {
                int strideTotal = 0;
                int ks = PostingIndexUtils.keysInStride(keyCount, s);
                for (int j = 0; j < ks; j++) {
                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                    strideTotal += Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                }
                if (strideTotal > maxStrideTotal) maxStrideTotal = strideTotal;
            }
            strideValsBytes = Math.max(1L, (long) maxStrideTotal * Long.BYTES);
            strideValsAddr = Unsafe.malloc(strideValsBytes, MemoryTag.NATIVE_INDEX_READER);

            long genOffset = valueMem.getAppendOffset();
            sealTarget = valueMem;

            int maxLocalHeaderSize = Math.max(
                    PostingIndexUtils.strideDeltaHeaderSize(PostingIndexUtils.DENSE_STRIDE),
                    PostingIndexUtils.strideFlatHeaderSize(PostingIndexUtils.DENSE_STRIDE)
            );
            long strideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
            long localHeaderBuf = 0;
            long bpTrialBuf = 0;
            long bpTrialBufSize = 0;
            int[] keyCounts = strideKeyCounts;
            long[] keyOffsets = strideKeyOffsets;

            try {
                localHeaderBuf = Unsafe.malloc(maxLocalHeaderSize, MemoryTag.NATIVE_INDEX_READER);

                for (int i = 0; i < siSize; i += Long.BYTES) {
                    valueMem.putLong(0L);
                }

                for (int s = 0; s < sc; s++) {
                    int ks = PostingIndexUtils.keysInStride(keyCount, s);
                    int strideStart = s * PostingIndexUtils.DENSE_STRIDE;

                    int strideValCount = 0;
                    for (int j = 0; j < ks; j++) {
                        int key = strideStart + j;
                        int count = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                        keyCounts[j] = count;
                        keyOffsets[j] = strideValCount;
                        strideValCount += count;
                    }

                    if (strideValCount == 0) {
                        long strideOff = valueMem.getAppendOffset() - genOffset - siSize;
                        Unsafe.putLong(strideIndexBuf + (long) s * Long.BYTES, strideOff);
                        continue;
                    }

                    // Spill must precede pending: add() flushes pending into spill when the
                    // pending slot fills, so spill holds the earlier rowids.
                    for (int j = 0; j < ks; j++) {
                        int count = keyCounts[j];
                        if (count == 0) continue;
                        int key = strideStart + j;
                        long writeAddr = strideValsAddr + keyOffsets[j] * Long.BYTES;
                        int spillCount = getSpillCount(key);
                        if (spillCount > 0) {
                            long spillAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
                            Unsafe.copyMemory(spillAddr, writeAddr, (long) spillCount * Long.BYTES);
                        }
                        int pendingCount = Unsafe.getInt(pendingCountsAddr + (long) key * Integer.BYTES);
                        if (pendingCount > 0) {
                            long pendingSrc = pendingValuesAddr + (long) key * PENDING_SLOT_CAPACITY * Long.BYTES;
                            Unsafe.copyMemory(pendingSrc, writeAddr + (long) spillCount * Long.BYTES, (long) pendingCount * Long.BYTES);
                        }
                    }

                    long trialBufNeeded = computeStrideTrialBufSize(ks, keyCounts);
                    if (trialBufNeeded > bpTrialBufSize) {
                        bpTrialBuf = Unsafe.realloc(bpTrialBuf, bpTrialBufSize, trialBufNeeded, MemoryTag.NATIVE_INDEX_READER);
                        bpTrialBufSize = trialBufNeeded;
                    }

                    long strideOff = valueMem.getAppendOffset() - genOffset - siSize;
                    Unsafe.putLong(strideIndexBuf + (long) s * Long.BYTES, strideOff);
                    encodeStrideBlock(ks, keyCounts, keyOffsets, strideValsAddr, strideBpKeySizes, bpTrialBuf, localHeaderBuf);
                }

                long totalStrideBlocksSize = valueMem.getAppendOffset() - genOffset - siSize;
                Unsafe.putLong(strideIndexBuf + (long) sc * Long.BYTES, totalStrideBlocksSize);

                long strideIndexAddr = valueMem.addressOf(genOffset);
                Unsafe.copyMemory(strideIndexBuf, strideIndexAddr, siSize);

                valueMemSize = valueMem.getAppendOffset();
            } finally {
                if (bpTrialBuf != 0) {
                    Unsafe.free(bpTrialBuf, bpTrialBufSize, MemoryTag.NATIVE_INDEX_READER);
                }
                if (localHeaderBuf != 0) {
                    Unsafe.free(localHeaderBuf, maxLocalHeaderSize, MemoryTag.NATIVE_INDEX_READER);
                }
                Unsafe.free(strideIndexBuf, siSize, MemoryTag.NATIVE_INDEX_READER);
                sealTarget = null;
            }

            writeSidecarGenData((int) totalValues, /* genIndex */ 0);

            this.maxValue = maxValueAfter;
            this.genCount = 1;
            publishToChain(
                    /* newGenCount */ 1,
                    /* overrideGenIndex */ 0,
                    /* overrideFileOffset */ genOffset,
                    /* overrideSize */ valueMemSize - genOffset,
                    /* overrideKeyCount */ keyCount,
                    /* overrideMinKey */ 0,
                    /* overrideMaxKey */ keyCount - 1
            );

            if (pendingDiscardOldSealTxn >= 0) {
                recordPostingSealPurge(pendingDiscardOldSealTxn);
                pendingDiscardOldSealTxn = -1L;
            }

            for (int i = 0; i < activeKeyCount; i++) {
                int key = activeKeyIds[i];
                Unsafe.putInt(pendingCountsAddr + (long) key * Integer.BYTES, 0);
            }
            resetSpill();
            hasPendingData = false;
            activeKeyCount = 0;
        } finally {
            if (strideValsAddr != 0) {
                Unsafe.free(strideValsAddr, strideValsBytes, MemoryTag.NATIVE_INDEX_READER);
            }
            Unsafe.free(totalCountsAddr, totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
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
        // The arena is gone, so the byte counter must follow. discardForRebuild
        // (PR 7077) calls freeSpillData directly, so resetting here keeps the
        // counter consistent across the rebuild lifecycle without an explicit
        // hook in that helper.
        totalSpillBytes = 0;
    }

    private long getCoveredAuxReadAddr(int covIdx, long offset, long needed) {
        ensureCoveredColumnReadMaps();
        long addr = coveredAuxReadAddrs[covIdx];
        long size = coveredAuxReadSizes[covIdx];
        if (addr == 0) {
            return 0;
        }
        // size == 0: addr-based caller-owned aux mapping (see getCoveredDataReadAddr).
        if (size == 0) {
            assert covIdx >= coveredColumnAuxAddrSizes.size()
                    || coveredColumnAuxAddrSizes.getQuick(covIdx) == 0
                    || offset + needed <= coveredColumnAuxAddrSizes.getQuick(covIdx)
                    : "addr-based covered aux read out of bounds [covIdx=" + covIdx
                    + ", offset=" + offset + ", needed=" + needed
                    + ", mapped=" + coveredColumnAuxAddrSizes.getQuick(covIdx) + ']';
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
        // size == 0 means addr-based (O3 seal / fast-lag): the mapping is owned by
        // the caller (TableWriter), so we never munmap/remap it -- coveredColReadSizes
        // stays 0 precisely so unmapCoveredColumnReads leaves caller memory alone.
        // When the caller supplied the mapped length via setCoveredColumnAddrSizes,
        // assert the read stays inside it (an empty list -- test/parquet -- skips it).
        if (size == 0) {
            assert covIdx >= coveredColumnAddrSizes.size()
                    || coveredColumnAddrSizes.getQuick(covIdx) == 0
                    || offset + needed <= coveredColumnAddrSizes.getQuick(covIdx)
                    : "addr-based covered data read out of bounds [covIdx=" + covIdx
                    + ", offset=" + offset + ", needed=" + needed
                    + ", mapped=" + coveredColumnAddrSizes.getQuick(covIdx) + ']';
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
        // Long-arithmetic doubling, see spillKey for rationale.
        long doubled = (long) keyCapacity * 2L;
        long want = Math.max(minCapacity, doubled);
        if (want > Integer.MAX_VALUE) {
            throw CairoException.critical(0)
                    .put("posting index key capacity exceeds 2^31 [minCapacity=").put(minCapacity)
                    .put(", keyCapacity=").put(keyCapacity)
                    .put("]; split commit into smaller batches");
        }
        int newCapacity = (int) want;

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

    /**
     * Structural check that a snapshotted {@code .pc{c}.{sealTxn}} sidecar
     * really is in the sealed layout
     * {@code [gen header][stride index][stride data][post-seal gen blocks]}
     * before {@link #sealIncremental} copies clean stride blocks out of it
     * verbatim.
     * <p>
     * The check rejects append-layout sidecars left by older writers or by
     * abandoned seal attempts. Those files have the same gen-header region,
     * but raw gen blocks start at {@code PC_HEADER_SIZE} with no stride index
     * at all. Interpreting that file as sealed turns appended cover bytes into
     * stride offsets and feeds garbage block sizes to {@code putBlockOfBytes}.
     * <p>
     * The two layouts are separable from the snapshot alone: a sealed file
     * places every post-seal gen block after the sealed region (header +
     * stride index + stride data), so any non-zero gen-header offset below
     * {@code PC_HEADER_SIZE + strideIndexSize} can only come from an
     * append-layout file, whose first gen block starts at exactly
     * {@code PC_HEADER_SIZE}. The stride-index invariants (first entry at
     * {@code PC_HEADER_SIZE}, monotonic growth, sealed data bounded by the
     * snapshot) additionally reject torn or otherwise corrupt files, e.g. a
     * stale sidecar left behind by an abandoned seal txn.
     *
     * @param bufAddr      read-only view of the whole sidecar file (mmap or
     *                     in-memory copy), 0 when the file was missing or
     *                     empty
     * @param bufSize      view size in bytes; must be the real file length so
     *                     gen header offsets pointing into the post-seal tail
     *                     validate correctly
     * @param gen0KeyCount key count of the dense gen 0 the sidecar was
     *                     sealed with
     * @return true when the snapshot can be trusted as sealed layout; false
     * routes the caller to {@link #sealFull}, which rebuilds sidecars from
     * column data
     */
    private boolean isTrustedSealedSidecarSnapshot(long bufAddr, long bufSize, int gen0KeyCount) {
        // A normal seal always writes the gen header and the stride index,
        // so a missing or undersized file for a live cover column means the
        // current sealTxn never had sealed sidecars.
        int siSize = PostingIndexUtils.strideIndexSize(gen0KeyCount);
        long sealedRegionMin = PostingIndexUtils.PC_HEADER_SIZE + (long) siSize;
        if (bufAddr == 0 || bufSize < sealedRegionMin) {
            return false;
        }
        // Gen header: slot g holds the absolute file offset of post-seal gen
        // g's raw block, 0 when absent. Sealed files only ever append gen
        // blocks after the sealed region.
        for (int g = 0; g < MAX_GEN_COUNT; g++) {
            long genOffset = Unsafe.getLong(bufAddr + (long) g * Long.BYTES);
            if (genOffset != 0 && (genOffset < sealedRegionMin || genOffset >= bufSize)) {
                return false;
            }
        }
        // Stride index: entries hold absolute offsets minus siSize, so the
        // first entry is always PC_HEADER_SIZE; entries grow monotonically
        // up to the sentinel, and the sealed data ends within the snapshot.
        int sc = PostingIndexUtils.strideCount(gen0KeyCount);
        long prev = Unsafe.getLong(bufAddr + PostingIndexUtils.PC_HEADER_SIZE);
        if (prev != PostingIndexUtils.PC_HEADER_SIZE) {
            return false;
        }
        for (int s = 1; s <= sc; s++) {
            long entry = Unsafe.getLong(bufAddr + PostingIndexUtils.PC_HEADER_SIZE + (long) s * Long.BYTES);
            if (entry < prev) {
                return false;
            }
            prev = entry;
        }
        // prev is the sentinel: sealed data end, stored as absolute - siSize.
        // Subtract instead of adding so a corrupt near-Long.MAX_VALUE sentinel
        // cannot overflow past the check; bufSize >= sealedRegionMin makes the
        // subtraction safe. seal() trusts this bound when it copies only
        // sentinel + siSize bytes of the snapshot.
        return prev <= bufSize - siSize;
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
                        // Bound the decode by the gen-dir count; see the sparse-gen
                        // decode below for the buffer-overflow rationale.
                        PostingIndexUtils.decodeKeyToNative(encodedAddr, destAddr, decodeCtx, count);
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
            // Bound the decode by the gen-dir count. destAddr is a slice of
            // mergedValuesAddr, which sealIncremental sizes from these same per-key
            // counts (totalCountsAddr); a corrupt block whose internal value count
            // exceeds count would otherwise overflow that exactly-sized buffer.
            // (The FLAT branches above read their counts from the prefix array --
            // the same source the sizing sweep folds in -- so they cannot exceed
            // the buffer and need no separate bound.)
            PostingIndexUtils.decodeKeyToNative(encodedAddr, destAddr + (long) totalCount * Long.BYTES, decodeCtx, count);
            totalCount += count;
        }

        return totalCount;
    }

    private void openSealValueFile(long newTxn) {
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
     * Worst-case ALP-compressed scratch size across the writer's
     * fixed-size cover columns at {@code maxKeyCount} values, or 0
     * when there are none. Each cover column allocates a compressBuf
     * sized to {@code CoveringCompressor.maxCompressedSize}; for
     * DOUBLE that's ~20 bytes per value (ALP header + packed +
     * exceptions worst case), for FLOAT ~12, for LONG/INT/etc. ~8/4
     * + header. Driven by the worst column type the writer indexes.
     * <p>
     * Returns 0 when there are no fixed-size cover columns; combined
     * with the {@code maxColValueSize} term in
     * {@link #estimateFastPathPeakBytes} this gives the full cover
     * footprint on the seal path.
     */
    private long peakCoverColumnCompressBufBytes(int maxKeyCount) {
        if (maxKeyCount <= 0) {
            return 0L;
        }
        long peak = 0L;
        for (int c = 0; c < coverCount; c++) {
            if (coveredColumnIndices.getQuick(c) < 0) {
                continue;
            }
            int shift = coveredColumnShifts.getQuick(c);
            if (shift < 0) {
                continue; // var-size cover column does not use this scratch
            }
            int colType = coveredColumnTypes.getQuick(c);
            long size = CoveringCompressor.maxCompressedSize(maxKeyCount, colType);
            if (size > peak) {
                peak = size;
            }
        }
        return peak;
    }

    /**
     * Largest fixed-size cover column's value size in bytes, or 0 when
     * either there are no cover columns or every cover column is var-size
     * (those use a different sidecar layout that does not allocate the
     * fixed-size per-stride sidecarBuf).
     */
    private long peakCoverColumnValueSize() {
        long peak = 0L;
        for (int c = 0; c < coverCount; c++) {
            if (coveredColumnIndices.getQuick(c) < 0) {
                continue;
            }
            int shift = coveredColumnShifts.getQuick(c);
            if (shift < 0) {
                continue; // var-size cover column, no fixed sidecarBuf
            }
            long size = 1L << shift;
            if (size > peak) {
                peak = size;
            }
        }
        return peak;
    }

    /**
     * Anonymous-heap scratch the streaming FSST compressor holds while encoding
     * a var-size cover stride: the 1 MiB compressed-output floor plus the batch
     * offset arrays, per-value scratch and symbol-table header. Zero when no
     * active cover is variable-size. Both seal estimates add this so the
     * pre-flight refuses (rather than OOMs) when even the FSST floor does not fit.
     */
    private long peakVarCoverFsstScratchBytes() {
        boolean hasVar = false;
        for (int c = 0; c < coverCount; c++) {
            if (coveredColumnIndices.getQuick(c) >= 0 && coveredColumnShifts.getQuick(c) < 0) {
                hasVar = true;
                break;
            }
        }
        if (!hasVar) {
            return 0L;
        }
        long offsArrays = 2L * (FSST_BATCH_SIZE + 1) * Long.BYTES;
        long batchScratch = (long) FSST_BATCH_SIZE * FSSTNative.BATCH_SCRATCH_BYTES_PER_VALUE;
        return FSST_BATCH_OUT_CAP_MIN + offsArrays + batchScratch + FSSTNative.MAX_HEADER_SIZE;
    }

    /**
     * Persist the writer's in-memory state to the v2 chain. Writes the
     * supplied gen-dir entry to the right slot, then either extends the
     * head entry (when the .pv file matches the head's sealTxn) or
     * appends a brand-new chain entry (when {@code sealTxn} has just
     * advanced past the head's sealTxn, i.e. a path-based seal).
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
        // Structural backstop for the poison invariant: every PUBLISH funnels
        // through here, so a poisoned writer can never append or extend a chain
        // entry at the staged sealTxn (which would let the deferred purge delete a
        // now-live .pv) -- including via the flush paths the entry-point guards
        // don't cover (add()'s spill flush, sync(), commitDense()). The two chain
        // mutators that bypass publishToChain -- truncate() and discardForRebuild()
        // -- guard at their own entry instead.
        checkNotPoisoned();
        // The writer's sealTxn always points at the .pv file currently
        // mapped through valueMem. When chain.getHeadSealTxn() == sealTxn
        // (head matches the same .pv file) we extend the head entry. When
        // sealTxn advances past the head's sealTxn (a seal switched .pv),
        // we append a fresh entry. The same applies to the empty-chain
        // case (headSealTxn = -1 < sealTxn): first flush after init/
        // truncate. The comparison is against the head's sealTxn rather
        // than chain.getGenCounter() because recoveryDropAbandoned can
        // leave the chain with headSealTxn < genCounter (the dropped
        // sealTxn .pv files are still on disk awaiting purge, so
        // genCounter cannot be safely rewound). Using genCounter here
        // would force a newEntry append at this.sealTxn = head.sealTxn,
        // tripping the appendNewEntry monotonicity assertion.
        boolean newEntry = !chain.hasHead() || this.sealTxn != chain.getHeadSealTxn();
        long entryBase = newEntry ? chain.getRegionLimit() : chain.getHeadEntryOffset();

        // For a same-sealTxn head extension the new gen-dir written below
        // overwrites the head entry's cover footer. Snapshot it into the cache
        // first so captureCoverEndOffsets can republish the existing extents even
        // when the writer's live coverCount is transiently 0 (between
        // clearCovering and configureCovering) or its sidecar handles are closed.
        // Without this the footer is dropped and a concurrent covered read of the
        // just-republished entry returns NULL for the whole partition.
        if (!newEntry) {
            chain.readHeadCoverEndOffsets(keyMem, coverEndOffsetsCache);
        } else {
            // New sealTxn: the cached extents belong to the superseded sealTxn's
            // .pc (a different file), so drop them. The new entry publishes an
            // empty footer until rebuildSidecars writes the new .pc and
            // repopulates the cache from the live sidecar handle.
            coverEndOffsetsCache.clear();
        }

        long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(entryBase, overrideGenIndex);
        long slotTxnAtSeal = pendingTxnAtSeal >= 0 ? pendingTxnAtSeal : 0L;
        keyMem.putLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET, overrideFileOffset);
        keyMem.putLong(dirOffset + GEN_DIR_OFFSET_SIZE, overrideSize);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT, overrideKeyCount);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY, overrideMinKey);
        keyMem.putInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY, overrideMaxKey);
        keyMem.putLong(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE, maxValue);
        // storeFence pairs with the loadFence in trimInFlightTailGens so a
        // recovery walk that finds slot.TXN_AT_SEAL > currentTableTxn sees
        // the matching slot payload too.
        Unsafe.storeFence();
        keyMem.putLong(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, slotTxnAtSeal);

        // Snapshot the current append offset of each open sidecar. Tombstoned
        // and not-yet-opened slots publish 0; readers treat them as "no file
        // / nothing to map", consistent with how isCoveredAvailable handles
        // missing sidecars.
        captureCoverEndOffsets();

        if (newEntry) {
            // pendingTxnAtSeal supplied by the upstream caller is the
            // table _txn this seal's chain entry should be tagged with.
            // The required value depends on the context:
            //   - commit-in-progress paths (TableWriter syncColumns,
            //     sealPostingIndexesForO3Partitions, ALTER ADD COLUMN/INDEX
            //     index build): txnAtSeal = txWriter.getTxn() + 1, so a
            //     writer-open recovery walk after a partial-publish
            //     failure can drop entries with txnAtSeal > committedTxn.
            //   - current-state paths (switchPartitionToLast rollback,
            //     IndexBuilder REINDEX, TableSnapshotRestore, the O3
            //     covering rebuildSidecars branch): txnAtSeal = current
            //     committed _txn, so recovery does NOT drop the
            //     legitimately published entry on the next reopen.
            //   - WAL fast-lag commit
            //     (publishPostingIndexesForLastPartitionFastLag): commit
            //     is in-progress, but txnAtSeal = txWriter.getTxn() (the
            //     pre-commit committed txn), NOT getTxn()+1. The chain
            //     walk therefore does NOT drop a partial entry on the
            //     next reopen. That is fine because the partition stays
            //     attached: openPartition runs
            //     rollbackConditionally(committed transientRowCount) on
            //     each posting writer, which evicts the orphan rowids
            //     directly. Contrast with switchPartition, where the
            //     retired partition is not reopened and the chain walk
            //     is the only recovery -- hence its getTxn()+1 tag.
            //     Note also that commit() takes the extendHead branch
            //     while a chain head exists, so this value only ends up
            //     on disk on the first commit after a fresh / truncated
            //     chain.
            // -1 (unset) means the caller didn't wire the setter; fall
            // back to txnAtSeal=0 so the entry is visible to every pinned
            // reader. The recovery walk cannot drop a 0-tagged entry
            // (predicate `0 > committedTxn` never fires), so a partial
            // publish on this path turns into an undroppable orphan. The
            // wiring in TableWriter eliminates this for the production
            // paths that matter; this fallback exists only for legacy
            // test fixtures and the no-arg of(...) used by O3CopyJob.
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
                    /* coveringFormat */ 0,
                    coverEndOffsetsScratch
            );
        } else {
            chain.extendHead(keyMem, newGenCount, keyCount, valueMemSize, maxValue, coverEndOffsetsScratch);
        }
    }

    private void rebuildSidecarsByCopy(long newSealTxn) {
        // Two-phase cleanup boundary. Before switchToSealedValueFile the new
        // .pv / .pc* files are on disk but not reachable by any reader (the
        // chain still references oldSealTxn), so a throw here can safely
        // unlink them directly. After the switch, valueMem and sidecarMems
        // are mapped to the new files; we cannot unmap-and-restore the old
        // valueMem in-place, so we queue an orphan purge instead. Real
        // unlink lands once the writer reaches a successful commit that
        // drains publishPendingPurges; if the caller closes without that
        // commit, the orphan files leak -- same window as the pre-PR
        // behaviour, just now narrower (only post-switch throws).
        openSealValueFile(newSealTxn);
        boolean isSwitched = false;
        boolean isPublished = false;
        try {
            long srcFd = valueMem.getFd();
            long dstFd = sealValueMem.getFd();
            long copied = ff.copyData(srcFd, dstFd, 0, valueMemSize);
            if (copied != valueMemSize) {
                throw CairoException.critical(ff.errno())
                        .put("incomplete .pv copy [expected=").put(valueMemSize)
                        .put(", actual=").put(copied)
                        .put(']');
            }
            sealValueMem.jumpTo(valueMemSize);

            openSidecarFiles(Path.getThreadLocal(partitionPath), indexName, postingColumnNameTxn, newSealTxn);
            switchToSealedValueFile(newSealTxn);
            isSwitched = true;

            long totalCountsSize = (long) keyCount * Integer.BYTES;
            long totalCountsAddr = Unsafe.malloc(totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
            long strideValsAddr = 0;
            long strideValsBytes = 0;
            try {
                Unsafe.setMemory(totalCountsAddr, totalCountsSize, (byte) 0);
                int maxStrideTotal = accumulateDenseGen0Counts(totalCountsAddr);
                strideValsBytes = Math.max(1L, (long) maxStrideTotal * Long.BYTES);
                strideValsAddr = Unsafe.malloc(strideValsBytes, MemoryTag.NATIVE_INDEX_READER);
                writeSidecarsPerColumn(totalCountsAddr, strideValsAddr);
            } finally {
                if (strideValsAddr != 0) {
                    Unsafe.free(strideValsAddr, strideValsBytes, MemoryTag.NATIVE_INDEX_READER);
                }
                Unsafe.free(totalCountsAddr, totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
            }

            // .pv before .pk: copyData's bytes must be durable before publishing
            // a chain head that references them. Mirrors sealIncremental.
            if (valueMem.isOpen()) {
                valueMem.sync(false);
            }
            for (int c = 0, n = sidecarMems.size(); c < n; c++) {
                MemoryMARW mem = sidecarMems.getQuick(c);
                if (mem != null && mem.isOpen()) {
                    mem.sync(false);
                }
            }
            if (sidecarInfoMem != null && sidecarInfoMem.isOpen()) {
                sidecarInfoMem.sync(false);
            }

            Unsafe.storeFence();
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
            // The chain entry at newSealTxn is now live and the writer is fully
            // consistent (chain head, valueMem and sealTxn all at newSealTxn). A
            // failure past this point (the .pk sync below) must leave the live
            // files alone -- mirrors the rollback path's isReencodePublished.
            isPublished = true;
            if (keyMem.isOpen()) {
                keyMem.sync(false);
            }
        } catch (Throwable th) {
            // A failure AFTER publishToChain leaves newSealTxn live and the writer
            // fully consistent; the only loss is the unsynced .pk, which the next
            // seal/commit re-syncs. Leave the live files alone -- just propagate.
            if (!isPublished) {
                if (isSwitched) {
                    // Pre-publish, post-switch: valueMem / sidecarMems are mapped to
                    // .{newSealTxn} but the chain still references oldSealTxn, so no
                    // reader can pin them yet. Defer cleanup to the seal-purge job
                    // with the widest safe window (the next publishPendingPurges
                    // drain). The chain head is on the old .pv but valueMem on the
                    // staged one, so poison the writer rather than risk a later op
                    // publishing a chain entry at newSealTxn and letting the deferred
                    // purge delete the live file. The orphan .pv.{newSealTxn} only
                    // becomes reachable later if a freed+reopened writer REUSES
                    // newSealTxn and publishes it live; PostingSealPurgeOperator
                    // re-reads the .pk head at delete time and abandons the purge
                    // when the file is live again, so the deferral is safe past reuse.
                    // Poison FIRST, before the LOG record and scheduleOrphanPurge (both
                    // allocate and can throw under heap pressure): a secondary failure
                    // must still leave the half-switched writer poisoned.
                    isPoisoned = true;
                    LOG.error().$("posting index rebuildSidecarsByCopy post-switch failure, poisoning writer and scheduling orphan purge [")
                            .$("indexName=").$(indexName)
                            .$(", newSealTxn=").$(newSealTxn)
                            .$(']').$();
                    scheduleOrphanPurge(newSealTxn);
                } else {
                    // Pre-switch: no chain mutation, no mapping survives into the
                    // writer's valueMem swap -- safe to unlink the staging files
                    // directly. sealValueMem and any opened sidecarMems are closed
                    // inline so the unlinks land before any retry races the name.
                    Misc.free(sealValueMem);
                    closeSidecarMems();
                    unlinkOrphanSealFiles(newSealTxn);
                }
            }
            throw th;
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
            // The chain is empty -- recordPostingSealPurge was called from a
            // code path that didn't append an entry first (e.g. close without
            // seal). Use a conservative window [0, MAX) so the queued purge can
            // never delete the file under a live reader; the global purge job
            // reclaims it under its scoreboard check once the outbox drains via
            // publishPendingPurges. If the writer is distressed and closes
            // without draining (releasePendingPurges), the file leaks (bounded)
            // like any other undrained entry -- no writer-open recovery walk
            // reclaims it, the walk being chain-driven and unable to rediscover a
            // never-published file. See scheduleOrphanPurge / releasePendingPurges.
            fromTxn = 0L;
            toTxn = Long.MAX_VALUE;
        } else {
            long prevTxnAtSeal = chain.getSecondEntryTxnAtSeal(keyMem);
            fromTxn = prevTxnAtSeal >= 0 ? prevTxnAtSeal : postingColumnNameTxn;
        }
        // pendingTxnAtSeal is intentionally NOT reset here: a single logical
        // TableWriter operation (e.g. sealPostingIndexForPartition) calls
        // setNextTxnAtSeal once and then runs through rollbackConditionally,
        // rebuildSidecars/seal in sequence -- every one of which may reach
        // publishToChain. Leaving the value live across sub-operations
        // preserves the right txnAtSeal. close() resets the field to -1
        // so a future of(...) starts clean; publishToChain's fallback
        // turns the never-set case (-1) into slot.TXN_AT_SEAL=0, leaving
        // the entry undroppable by recovery -- a deliberate trade-off for
        // test/legacy paths that never wire the setter.
        // Cap the in-memory outbox to prevent unbounded growth when the
        // global PostingSealPurge job is disabled, the queue is permanently
        // saturated, or publishPendingPurges() is never called. When at the
        // cap, drop the oldest entry. There is no writer-open scan that
        // reclaims its file -- dropping leaks the superseded .pv/.pc on disk
        // (bounded by outboxMax), so keep the global PostingSealPurge job
        // running to drain the outbox and keep this path cold.
        int outboxMax = configuration.getPostingSealPurgeOutboxMax();
        if (outboxMax > 0 && pendingPurges.size() >= outboxMax) {
            PendingSealPurge oldest = pendingPurges.getQuick(0);
            LOG.critical()
                    .$("posting seal-purge outbox saturated, dropping oldest entry [")
                    .$("indexName=").$(indexName)
                    .$(", postingColumnNameTxn=").$(oldest.postingColumnNameTxn)
                    .$(", sealTxn=").$(oldest.sealTxn)
                    .$(", outboxMax=").$(outboxMax)
                    .$("]. The dropped entry's files are left on disk; no writer-open scan reclaims them, so keep the global purge job running.")
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
                        if (key < 0 || key >= keyCount) {
                            continue; // corrupt sparse gen: key out of range; skip the OOB totalCountsAddr write
                        }
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

            // Scan totalCountsAddr to find:
            //  - maxStrideTotal: largest per-stride sum (sizes the fast-path
            //    stride decode buffer)
            //  - maxKeyCount: largest single-key count (sizes the streaming
            //    fallback's per-key buffer; also drives writeSidecarsPerColumn's
            //    longWorkspace / exceptionWorkspace, which are sized to the
            //    per-partition max key count)
            //  - maxStrideTrialSize: largest per-stride trial-encode total
            //    (sizes the fast-path bpTrialBuf). Computed exactly via
            //    computeMaxEncodedSize because the per-key fixed overhead
            //    (~22 bytes for delta-mode header, residuals, exception
            //    accounting) dominates for low-count keys -- a flat
            //    "maxStrideTotal * 9" approximation underestimates by 2-3x
            //    when most keys have counts < 3, leaving the budget
            //    misclassified.
            // strideTotal/maxStrideTotal are long arithmetic to avoid int
            // wrap when compaction merges multiple gens into one stride
            // whose per-key sum exceeds Integer.MAX_VALUE.
            long maxStrideTotalL = 0L;
            int maxKeyCount = 0;
            long maxStrideTrialSize = 0L;
            // maxStrideTrialSize sizes the fast seal path's bpTrialBuf and is read
            // only by the seal branch; skip the per-key computeMaxEncodedSize work
            // on the rollback path (maxValueCutoff < MAX), which streams and never
            // consults it.
            final boolean isSealSizing = maxValueCutoff == Long.MAX_VALUE;
            {
                int sc0 = PostingIndexUtils.strideCount(keyCount);
                for (int s0 = 0; s0 < sc0; s0++) {
                    long strideTotal = 0L;
                    long strideTrialSize = 0L;
                    int ks0 = PostingIndexUtils.keysInStride(keyCount, s0);
                    for (int j0 = 0; j0 < ks0; j0++) {
                        int key0 = s0 * PostingIndexUtils.DENSE_STRIDE + j0;
                        int c = Unsafe.getInt(totalCountsAddr + (long) key0 * Integer.BYTES);
                        strideTotal += c;
                        if (c > maxKeyCount) maxKeyCount = c;
                        if (isSealSizing && c > 0) {
                            strideTrialSize += PostingIndexUtils.computeMaxEncodedSize(c);
                        }
                    }
                    if (strideTotal > maxStrideTotalL) maxStrideTotalL = strideTotal;
                    if (strideTrialSize > maxStrideTrialSize) maxStrideTrialSize = strideTrialSize;
                }
            }
            if (maxStrideTotalL > Integer.MAX_VALUE) {
                // 2^31 longs in a single 256-key stride is ~17 GiB just for
                // the decode buffer. There is no path that succeeds; bail
                // with a clear diagnostic instead of overflowing into the
                // existing int-typed strideValsAddr sizing.
                throw CairoException.critical(0)
                        .put("posting index seal stride aggregate exceeds 2^31 values [maxStrideTotal=").put(maxStrideTotalL)
                        .put(", keyCount=").put(keyCount)
                        .put(", maxKeyCount=").put(maxKeyCount)
                        .put("]; split the partition into smaller commits");
            }
            int maxStrideTotal = (int) maxStrideTotalL;

            if (maxValueCutoff < Long.MAX_VALUE) {
                // The rollback always streams, bounded by the largest single key
                // (not the whole index): filterCountsForRollback finds each key's
                // surviving prefix, then reencodeWithPerKeyStreaming re-encodes
                // it. Path-based covered sidecars are rebuilt from the freshly
                // sealed gen 0; addr-based (O3) covers leave the sidecars unopened
                // -- the O3 flow re-seals after the rollback and rebuilds the .pc
                // there (matching the prior monolithic skip, without the
                // whole-index decode).
                //
                // Pre-flight: the rollback re-encode streams one key at a time, so
                // its peak is the same streaming estimate the seal path uses
                // (keyBuffer + encodeCtx + sidecar workspaces), sized to the
                // pre-filter hottest key. Only a path-based-cover rollback rebuilds
                // sidecars; an addr-based-cover or non-covered rollback skips the
                // sidecar write (sidecarMems stays empty below), so it allocates
                // none of the sidecarBuf / FSST floor / per-key workspaces -- pass
                // isSidecarWritten so the estimate does not over-charge them and
                // spuriously reject a rollback that fits. If even the right-sized
                // peak breaches the RSS limit, throw an operator-actionable
                // diagnostic BEFORE allocating keyBuffer or switching the value
                // file, rather than OOM mid-encode with a generic allocation message.
                final boolean isRollbackWritingSidecars = coverCount > 0
                        && coveredColumnNames.size() > 0
                        && coveredPartitionPath.size() > 0;
                final long rollbackRssLimit = Unsafe.getRssMemLimit();
                if (rollbackRssLimit > 0) {
                    final long rollbackPeak = estimateStreamingPathPeakBytes(
                            maxKeyCount,
                            isRollbackWritingSidecars ? peakCoverColumnValueSize() : 0L,
                            isRollbackWritingSidecars);
                    final long rollbackHeadroom = rssHeadroom(rollbackRssLimit, Unsafe.getRssMemUsed());
                    if (rollbackPeak > rollbackHeadroom) {
                        throw CairoException.critical(0)
                                .put("posting index rollback needs ").put(rollbackPeak)
                                .put(" bytes but only ").put(rollbackHeadroom)
                                .put(" bytes of RSS headroom remain [maxKeyCount=").put(maxKeyCount)
                                .put(", keyCount=").put(keyCount)
                                .put("]; split the partition into smaller commits");
                    }
                }
                long keyBuffer = Unsafe.malloc((long) maxKeyCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                final int oldKeyCount = keyCount;
                final long oldValueMemSize = valueMemSize;
                boolean isReencodeStarted = false;
                boolean isReencodePublished = false;
                try {
                    int newKeyCount = filterCountsForRollback(maxValueCutoff, totalCountsAddr, keyBuffer, maxKeyCount);
                    if (newKeyCount == 0) {
                        // Every value was above the cutoff; nothing survives.
                        isLastRollbackStreaming = false;
                        truncate();
                        setMaxValue(maxValue);
                    } else {
                        isLastRollbackStreaming = true;
                        keyCount = newKeyCount;
                        // Mark the reencode started BEFORE staging the covered
                        // sidecars -- openSidecarFiles can throw (ENOSPC/EMFILE) and
                        // the catch must then restore keyCount/valueMemSize and drop
                        // the staged files, not leave the writer with a shrunken
                        // keyCount against an unchanged chain.
                        isReencodeStarted = true;
                        if (coverCount > 0 && coveredColumnNames.size() > 0 && coveredPartitionPath.size() > 0) {
                            // Path-based covers: the rollback (unlike the seal
                            // flow) has not opened the covered sidecar files; open
                            // them at the new sealTxn so the streaming sidecar
                            // write inside reencodeWithPerKeyStreaming rebuilds the
                            // .pc for the surviving rows.
                            closeSidecarMems();
                            openSidecarFiles(Path.getThreadLocal(partitionPath), indexName, postingColumnNameTxn, newSealTxn);
                        } else if (coverCount > 0) {
                            // Addr-based (O3) covers: the sidecarMems may still be
                            // open at the OLD sealTxn from a prior fast-lag publish.
                            // The streaming reencode writes no sidecar bytes for
                            // addr-based covers, so close them now -- otherwise
                            // captureCoverEndOffsets snapshots stale old-txn append
                            // offsets into the new chain entry and readers probe a
                            // missing .pc. The O3 flow re-seals afterwards and
                            // rebuilds the .pc there before this rollback entry is
                            // ever exposed to a reader (the reseal supersedes it
                            // within the same writer-locked O3 op). Asserted because
                            // a path-based cover that ever reached here (covers live
                            // but no partition path) would SILENTLY skip rebuilding
                            // its real .pc; a genuine addr-based cover leaves
                            // coveredColumnNames empty (only coveredColumnAddrs set).
                            // Exercised by PostingIndexO3ConcurrencyFuzzTest's
                            // covering O3/squash/parquet-spill rollback fuzz.
                            assert coveredColumnNames.size() == 0
                                    : "addr-based cover rollback reached with path-based cover names but no partition path; its .pc would not be rebuilt";
                            closeSidecarMems();
                        }
                        reencodeWithPerKeyStreaming(newSealTxn, maxValue, totalCountsAddr, keyBuffer, maxKeyCount, true);
                        // reencodeWithPerKeyStreaming's last step is publishToChain;
                        // once it returns the new sealTxn is live (chain switched,
                        // .pv/.pc synced). A failure past this point must NOT unlink
                        // those files -- the rollback has committed.
                        isReencodePublished = true;
                        // Sync the chain publish now -- the new .pv/.pc are already
                        // synced inside reencodeWithPerKeyStreaming -- so once
                        // rollbackToMaxValue queues the old files for purge, a
                        // power loss cannot recover a committed chain head pointing
                        // at a deleted file. The seal path defers this to seal()'s
                        // unconditional .pk sync; the rollback path has none.
                        if (keyMem.isOpen()) {
                            keyMem.sync(false);
                        }
                    }
                } catch (Throwable th) {
                    // A throw BEFORE publishToChain (the last step of
                    // reencodeWithPerKeyStreaming) leaves the staged .pv/.pc at
                    // newSealTxn unpublished and the chain still on the old .pv.
                    // Recovery depends on whether the value file was already
                    // switched into valueMem -- switchToSealedValueFile advances
                    // sealTxn to newSealTxn as its last step, so sealTxn ==
                    // newSealTxn iff the swap completed. A throw AFTER publish (the
                    // post-publish .pk sync) leaves the now-live files alone and
                    // only propagates.
                    if (isReencodeStarted && !isReencodePublished) {
                        if (sealTxn == newSealTxn) {
                            // Switched: valueMem (and any open sidecarMems) are
                            // mapped to the staged .pv.{newSealTxn}, sealTxn advanced
                            // to newSealTxn, but the chain head still references the
                            // old .pv. The staged files cannot be unlinked here
                            // (Windows refuses to unlink an open map), so defer them
                            // to the purge job with the widest safe window, matching
                            // rebuildSidecarsByCopy -- no reader can pin them yet
                            // because the chain never referenced newSealTxn. The
                            // writer is now internally inconsistent (chain head at the
                            // old sealTxn, valueMem at newSealTxn): poison it so a
                            // later rollback/seal/commit throws instead of re-driving
                            // the poisoned state, which could append a chain entry at
                            // newSealTxn and let the deferred [0, MAX) purge delete
                            // the now-live .pv.{newSealTxn}. The remaining route --
                            // a freed+reopened writer REUSING newSealTxn and
                            // publishing it live -- is closed at delete time:
                            // PostingSealPurgeOperator re-reads the .pk head and
                            // abandons the purge when the file is live again.
                            // Poison FIRST, before the LOG record and scheduleOrphanPurge
                            // (both allocate and can throw under heap pressure): a secondary
                            // failure must still leave the half-switched writer poisoned.
                            isPoisoned = true;
                            LOG.error().$("posting index rollback post-switch failure, poisoning writer and scheduling orphan purge [")
                                    .$("indexName=").$(indexName)
                                    .$(", newSealTxn=").$(newSealTxn)
                                    .$(']').$();
                            scheduleOrphanPurge(newSealTxn);
                        } else {
                            // Not switched: nothing maps the staged files yet, so
                            // free the staging maps, unlink the staged files, and
                            // restore the writer state the reencode mutated before
                            // the switch -- valueMemSize and sealTarget (set inside
                            // reencodeWithPerKeyStreaming) and keyCount (below).
                            Misc.free(sealValueMem);
                            closeSidecarMems();
                            unlinkOrphanSealFiles(newSealTxn);
                            valueMemSize = oldValueMemSize;
                            sealTarget = null;
                        }
                        keyCount = oldKeyCount;
                    }
                    throw th;
                } finally {
                    Unsafe.free(keyBuffer, (long) maxKeyCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
            } else {
                // Seal path. Pick between the fast stride-chunked decode (peak
                // bounded by the largest single stride's aggregate row count)
                // and the per-key streaming fallback (peak bounded by the
                // largest single key's row count) based on RSS headroom. The
                // fast path is always preferred when it fits because it
                // amortises encoder setup over a stride and gets to use FLAT
                // mode where applicable; streaming runs in always-DELTA mode
                // and is several times slower, so it only kicks in when the
                // fast path would not run at all.
                //
                // Allocations gated by the pre-flight result so a streaming
                // selection does not incur the fast path's packedResidualsAddr
                // pre-realloc cost (the upstream realloc that lived here
                // previously could itself OOM under tight RSS, defeating the
                // whole purpose of the pre-flight).
                // A full seal ran (either directly or via sealIncremental's
                // pre-flight fallback): clear the incremental flag for the test
                // hook. The fast-vs-streaming sub-path then sets isLastSealStreaming.
                isLastSealIncremental = false;
                final long maxColValueSize = peakCoverColumnValueSize();
                final long fastPathPeakBytes = estimateFastPathPeakBytes(maxStrideTotal, maxKeyCount, maxStrideTrialSize, maxColValueSize);
                final long streamingPathPeakBytes = estimateStreamingPathPeakBytes(maxKeyCount, maxColValueSize);
                final long rssLimit = Unsafe.getRssMemLimit();
                final long rssUsed = Unsafe.getRssMemUsed();
                final long headroom = rssHeadroom(rssLimit, rssUsed);

                if (fastPathPeakBytes <= headroom) {
                    isLastSealStreaming = false;
                    if (maxStrideTotal > packedResidualsCapacity) {
                        packedResidualsAddr = Unsafe.realloc(packedResidualsAddr,
                                (long) packedResidualsCapacity * Long.BYTES,
                                (long) maxStrideTotal * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                        packedResidualsCapacity = maxStrideTotal;
                    }
                    long strideValsAddr = Unsafe.malloc((long) maxStrideTotal * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    try {
                        reencodeWithStrideDecoding(
                                newSealTxn, maxValue, totalCountsAddr, strideValsAddr
                        );
                    } finally {
                        Unsafe.free(strideValsAddr, (long) maxStrideTotal * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    }
                } else if (streamingPathPeakBytes <= headroom) {
                    isLastSealStreaming = true;
                    LOG.info().$("posting seal falling back to per-key streaming compaction ")
                            .$("[maxStrideTotal=").$(maxStrideTotal)
                            .$(", maxKeyCount=").$(maxKeyCount)
                            .$(", fastPathPeakBytes=").$(fastPathPeakBytes)
                            .$(", streamingPathPeakBytes=").$(streamingPathPeakBytes)
                            .$(", headroom=").$(headroom)
                            .I$();
                    long keyBuffer = Unsafe.malloc((long) maxKeyCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    try {
                        reencodeWithPerKeyStreaming(newSealTxn, maxValue, totalCountsAddr, keyBuffer, maxKeyCount, false);
                    } finally {
                        Unsafe.free(keyBuffer, (long) maxKeyCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    }
                } else {
                    throw CairoException.critical(0)
                            .put("posting index seal would exceed RSS limit even with streaming compaction ")
                            .put("[maxStrideTotal=").put(maxStrideTotal)
                            .put(", maxKeyCount=").put(maxKeyCount)
                            .put(", fastPathPeakBytes=").put(fastPathPeakBytes)
                            .put(", streamingPathPeakBytes=").put(streamingPathPeakBytes)
                            .put(", rssUsed=").put(rssUsed)
                            .put(", rssLimit=").put(rssLimit)
                            .put("]; the partition has a single symbol value with too many rows for current RSS_MEM_LIMIT, ")
                            .put("reduce partition size or raise RSS_MEM_LIMIT");
                }
            }
        } finally {
            Unsafe.free(totalCountsAddr, totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    /**
     * Streaming compaction fallback. Same on-disk output as
     * {@link #reencodeWithStrideDecoding} (DELTA-mode strides, no FLAT
     * mode), but bounds peak heap usage by the largest single key's
     * count rather than the largest stride's aggregate. Selected by the
     * pre-flight check when the stride-buffered fast path would exceed
     * RSS headroom.
     * <p>
     * Cost: ~3x slower than the fast path on partitions where both
     * fit, due to per-key (rather than per-stride) decode and the
     * always-DELTA encoding. Only kicks in when the fast path would
     * not run at all -- a strict win over the alternative.
     * <p>
     * For every output stride:
     * <ul>
     *   <li>read per-key counts from {@code totalCountsAddr};</li>
     *   <li>reserve the DELTA-mode stride header in {@code sealTarget};</li>
     *   <li>for each key in the stride, decode its values from every
     *       source generation into {@code keyBuffer}, encode directly
     *       into {@code sealTarget}, record the per-key encoded size;</li>
     *   <li>patch the stride header in place.</li>
     * </ul>
     * After the row-id index is sealed, sidecars are written via the
     * streaming variant {@link #writeSidecarsPerColumnStreaming}.
     *
     * @param keyBuffer  pre-allocated workspace sized to {@code maxKeyCount * 8} bytes,
     *                   reused across keys; lifetime owned by the caller
     * @param isRollback {@code true} when re-encoding the surviving prefix of a
     *                   rollback (the per-key decode may legitimately exceed the
     *                   surviving {@code count}, which is the filtered prefix, so
     *                   the decode-count check relaxes to {@code decodedTotal <
     *                   count}); {@code false} for a seal, where the decoded total
     *                   must equal {@code count} exactly (any mismatch is corruption)
     */
    private void reencodeWithPerKeyStreaming(
            long newSealTxn,
            long maxValue,
            long totalCountsAddr,
            long keyBuffer,
            int maxKeyCount,
            boolean isRollback
    ) {
        int sc = PostingIndexUtils.strideCount(keyCount);
        int siSize = PostingIndexUtils.strideIndexSize(keyCount);

        // Resolve per-gen metadata into reusable scratch, BEFORE any native
        // allocation or openSealValueFile here, so a fault in these reads cannot
        // leak strideIndexBuf/localHeaderBuf. valueMem is the read-only source
        // until switchToSealedValueFile below (the per-key loop writes only to
        // sealTarget, a distinct mapping), so the base addresses stay stable. The
        // inner loop would otherwise re-resolve genDirOffset + read two gen-dir
        // fields + recompute the gen base/strideCount per (key, gen), i.e.
        // O(keyCount * genCount) where O(genCount) suffices.
        resolveGenMetadataScratch();
        final long[] genBases = genMetaBases;
        final int[] genKeyCounts = genMetaKeyCounts;
        final int[] genStrideCounts = genMetaStrideCounts;

        openSealValueFile(newSealTxn);
        long sealOffset = sealTarget.getAppendOffset();
        // Reserve the stride index region; we patch it at the end of the loop.
        for (int i = 0; i < siSize; i += Long.BYTES) {
            sealTarget.putLong(0L);
        }

        long strideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
        int maxDeltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(PostingIndexUtils.DENSE_STRIDE);
        long localHeaderBuf = 0;
        int[] bpKeySizes = strideBpKeySizes;
        int[] keyCounts = strideKeyCounts;

        try {
            localHeaderBuf = Unsafe.malloc(maxDeltaHeaderSize, MemoryTag.NATIVE_INDEX_READER);
            for (int s = 0; s < sc; s++) {
                int ks = PostingIndexUtils.keysInStride(keyCount, s);
                int strideStart = s * PostingIndexUtils.DENSE_STRIDE;

                // hasAnyValues rather than a summed count: per-stride
                // aggregate could exceed Integer.MAX_VALUE on a heavily
                // compacted partition, and an int sum that wraps to 0
                // would silently skip a non-empty stride. The pre-flight
                // check above already throws on this case for the fast
                // path; defensive here so the streaming path stays
                // correct even if the pre-flight is bypassed in future
                // refactors.
                boolean hasAnyValues = false;
                for (int j = 0; j < ks; j++) {
                    int key = strideStart + j;
                    int count = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    keyCounts[j] = count;
                    if (count > 0) hasAnyValues = true;
                }

                long strideOff = sealTarget.getAppendOffset() - sealOffset - siSize;
                Unsafe.putLong(strideIndexBuf + (long) s * Long.BYTES, strideOff);

                if (!hasAnyValues) {
                    continue;
                }

                int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
                long headerFilePos = sealTarget.getAppendOffset();
                // Reserve header bytes; we patch them after the per-key
                // encode loop knows each key's encoded size.
                for (int i = 0; i < deltaHeaderSize; i += Integer.BYTES) {
                    sealTarget.putInt(0);
                }
                long dataStart = sealTarget.getAppendOffset();

                for (int j = 0; j < ks; j++) {
                    int count = keyCounts[j];
                    if (count == 0) {
                        bpKeySizes[j] = 0;
                        continue;
                    }
                    if (count > maxKeyCount) {
                        // Should be impossible -- maxKeyCount was computed
                        // from the same totalCountsAddr we just read. Defensive.
                        throw CairoException.critical(0)
                                .put("posting index streaming key count exceeds buffer [count=").put(count)
                                .put(", maxKeyCount=").put(maxKeyCount).put(']');
                    }
                    // Decode this key from every source generation. Order
                    // matters: each gen contributes a contiguous run that
                    // is itself sorted; concatenated, the runs span the
                    // partition's row-id range in the order rows were
                    // ingested. Since the indexer feeds row-ids
                    // monotonically across the whole indexing run, the
                    // concatenation is itself sorted. flushAllPending's
                    // gen production preserves the same invariant.
                    int decodedTotal = 0;
                    for (int gen = 0; gen < genCount; gen++) {
                        int genKeyCount = genKeyCounts[gen];
                        long appendAddr = keyBuffer + (long) decodedTotal * Long.BYTES;
                        int capacity = maxKeyCount - decodedTotal;
                        if (genKeyCount < 0) {
                            decodedTotal += decodeSparseGenSingleKey(genBases[gen], -genKeyCount, strideStart + j, appendAddr, capacity);
                        } else if (s < genStrideCounts[gen]) {
                            decodedTotal += decodeDenseGenSingleKey(genBases[gen], genKeyCount, s, j, appendAddr, capacity);
                        }
                    }
                    // Seal (isRollback=false): decodedTotal must equal count (the
                    // full key) exactly -- an over-decode means the encoded blocks
                    // hold more than the gen-dir counts, i.e. decoder/data
                    // corruption, so keep the strict check. Rollback: count is the
                    // surviving prefix (values <= the cutoff), so encodeKeyNative
                    // below writes the leading `count` values and drops the filtered
                    // tail; only a SHORT decode is corruption there.
                    if (isRollback ? decodedTotal < count : decodedTotal != count) {
                        throw CairoException.critical(0)
                                .put("posting index streaming decode mismatch [key=").put(strideStart + j)
                                .put(", expected=").put(count)
                                .put(", decoded=").put(decodedTotal).put(']');
                    }

                    // Encode directly into sealTarget. Reserve the
                    // worst-case bytes, encode, then jump back to the
                    // actual end so the next key starts there.
                    long maxEnc = PostingIndexUtils.computeMaxEncodedSize(count);
                    long encStart = sealTarget.getAppendOffset();
                    sealTarget.jumpTo(encStart + maxEnc);
                    long dstAddr = sealTarget.addressOf(encStart);
                    encodeCtx.ensureCapacity(count);
                    int bytesWritten = PostingIndexUtils.encodeKeyNative(keyBuffer, count, dstAddr, encodeCtx, rowIdEncoding);
                    sealTarget.jumpTo(encStart + bytesWritten);
                    bpKeySizes[j] = bytesWritten;
                }

                // Patch the DELTA stride header now that we know each key's encoded size.
                Unsafe.setMemory(localHeaderBuf, deltaHeaderSize, (byte) 0);
                Unsafe.putByte(localHeaderBuf, PostingIndexUtils.STRIDE_MODE_DELTA);
                long countsBase = localHeaderBuf + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                long offsetsBase = countsBase + (long) ks * Integer.BYTES;
                long dataOffset = 0;
                for (int j = 0; j < ks; j++) {
                    Unsafe.putInt(countsBase + (long) j * Integer.BYTES, keyCounts[j]);
                    Unsafe.putLong(offsetsBase + (long) j * Long.BYTES, dataOffset);
                    dataOffset += bpKeySizes[j];
                }
                Unsafe.putLong(offsetsBase + (long) ks * Long.BYTES, dataOffset);
                long headerAddr = sealTarget.addressOf(headerFilePos);
                Unsafe.copyMemory(localHeaderBuf, headerAddr, deltaHeaderSize);

                // Sanity: dataOffset must equal the bytes we appended after the header.
                assert dataStart + dataOffset == sealTarget.getAppendOffset();
            }

            long totalStrideBlocksSize = sealTarget.getAppendOffset() - sealOffset - siSize;
            Unsafe.putLong(strideIndexBuf + (long) sc * Long.BYTES, totalStrideBlocksSize);

            long strideIndexAddr = sealTarget.addressOf(sealOffset);
            Unsafe.copyMemory(strideIndexBuf, strideIndexAddr, siSize);
        } finally {
            if (localHeaderBuf != 0) {
                Unsafe.free(localHeaderBuf, maxDeltaHeaderSize, MemoryTag.NATIVE_INDEX_READER);
            }
            Unsafe.free(strideIndexBuf, siSize, MemoryTag.NATIVE_INDEX_READER);
        }

        valueMemSize = sealTarget.getAppendOffset();
        sealValueMem.sync(false);
        switchToSealedValueFile(newSealTxn);
        sealTarget = null;

        if (coverCount > 0 && sidecarMems.size() > 0) {
            writeSidecarsPerColumnStreaming(totalCountsAddr, keyBuffer, maxKeyCount);
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
        // Encoded output starts at offset 0 of the fresh .pv file
        // openSealValueFile staged at newSealTxn.
        long sealOffset = sealTarget.getAppendOffset();
        for (int i = 0; i < siSize; i += Long.BYTES) {
            sealTarget.putLong(0L);
        }

        long strideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
        int maxDeltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(PostingIndexUtils.DENSE_STRIDE);
        int maxFlatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(PostingIndexUtils.DENSE_STRIDE);
        int maxLocalHeaderSize = Math.max(maxDeltaHeaderSize, maxFlatHeaderSize);
        long localHeaderBuf = 0;

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

                long trialBufNeeded = computeStrideTrialBufSize(ks, keyCounts);
                if (trialBufNeeded > bpTrialBufSize) {
                    bpTrialBuf = Unsafe.realloc(bpTrialBuf, bpTrialBufSize, trialBufNeeded, MemoryTag.NATIVE_INDEX_READER);
                    bpTrialBufSize = trialBufNeeded;
                }

                long strideOff = sealTarget.getAppendOffset() - sealOffset - siSize;
                Unsafe.putLong(strideIndexBuf + (long) s * Long.BYTES, strideOff);
                encodeStrideBlock(ks, keyCounts, keyOffsets, strideValsAddr, strideBpKeySizes, bpTrialBuf, localHeaderBuf);
            }

            long totalStrideBlocksSize = sealTarget.getAppendOffset() - sealOffset - siSize;
            Unsafe.putLong(strideIndexBuf + (long) sc * Long.BYTES, totalStrideBlocksSize);

            long strideIndexAddr = sealTarget.addressOf(sealOffset);
            Unsafe.copyMemory(strideIndexBuf, strideIndexAddr, siSize);

            // valueMemSize is updated below, after the staged .pv is finalized
            // and switched in.
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
        sealValueMem.sync(false);
        switchToSealedValueFile(newSealTxn);
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
        // genFileOffset is 0: the seal wrote at offset 0 of a fresh .pv.
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
     * Returns every entry in {@link #pendingPurges} to the pool WITHOUT
     * publishing them. Used at writer close. Any entry still here references a
     * superseded-or-staged value file that was never handed to the global purge
     * job, so its .pv/.pc leak on disk: the writer-open recovery walk is
     * chain-driven and cannot rediscover a never-published sealTxn (see
     * {@link #scheduleOrphanPurge} and {@code getPostingSealPurgeOutboxMax}).
     * Every seal/commit path drains the outbox via publishPendingPurges before
     * close; this is the last-resort release for a distressed writer whose
     * outbox could not be drained.
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

    // Fills genMetaBases/genMetaKeyCounts/genMetaStrideCounts for every source
    // generation, growing the scratch arrays on demand. Read-only over valueMem
    // (the per-key encode writes only to sealTarget, a distinct mapping), so the
    // resolved base addresses stay valid until switchToSealedValueFile. The
    // rollback resolves once in filterCountsForRollback and again in
    // reencodeWithPerKeyStreaming -- O(genCount), genCount <= MAX_GEN_COUNT --
    // rather than carry the scratch across the keyCount shrink between them as
    // shared mutable state.
    private void resolveGenMetadataScratch() {
        if (genMetaBases == null || genMetaBases.length < genCount) {
            genMetaBases = new long[genCount];
            genMetaKeyCounts = new int[genCount];
            genMetaStrideCounts = new int[genCount];
        }
        for (int gen = 0; gen < genCount; gen++) {
            long dirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), gen);
            genMetaBases[gen] = valueMem.addressOf(keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET));
            int genKeyCount = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
            genMetaKeyCounts[gen] = genKeyCount;
            // strideCount is only consulted in the dense branch (genKeyCount >= 0);
            // a sparse gen (genKeyCount < 0) stores 0 and never reads it.
            genMetaStrideCounts[gen] = genKeyCount >= 0 ? PostingIndexUtils.strideCount(genKeyCount) : 0;
        }
    }

    private void rollbackToMaxValue(long maxValue) {
        // Rollback writes to a NEW .pv file (like seal) so concurrent readers
        // with active mmaps on the old .pv don't SIGSEGV. Allocate the rollback
        // sealTxn the same way seal() does so the new .pv stays distinct from
        // any prior sealed generation on disk.
        // peekNextSealTxn(), not sealTxn+1: after a recovery drop the
        // writer's sealTxn lags genCounter, and reusing a dropped
        // sealTxn would race the still-pending .pv purge.
        final long oldSealTxn = sealTxn;
        final long newSealTxn = Math.max(1, chain.peekNextSealTxn());
        reencodeAllGenerations(newSealTxn, maxValue, maxValue);
        // Record the superseded oldSealTxn for purge -- but ONLY for the normal
        // reencode path. When every value was rolled back, reencodeAllGenerations
        // routes through truncate(), which already recorded oldSealTxn and left
        // genCount == 0; recording again here would queue a redundant (no-op)
        // purge for the same file. The reencode path always leaves genCount == 1.
        if (genCount > 0 && sealTxn != oldSealTxn) {
            recordPostingSealPurge(oldSealTxn);
        }
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
        boolean isHeadTrimmed;
        try {
            dropped = chain.recoveryDropAbandoned(keyMem, currentTableTxn, recoveryOrphanScratch);
            isHeadTrimmed = chain.isHeadTrimmedOnLastRecovery();
        } finally {
            // Single-shot consumption — next reopen must call the setter
            // again or recovery is skipped.
            currentTableTxn = -1L;
        }
        if (dropped <= 0 && !isHeadTrimmed) {
            return;
        }
        LOG.info().$("posting index recovery [")
                .$("indexName=").$(indexName)
                .$(", postingColumnNameTxn=").$(postingColumnNameTxn)
                .$(", dropped=").$(dropped)
                .$(", isHeadTrimmed=").$(isHeadTrimmed)
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

    /**
     * Queue an orphan {@code .pv.{sealTxn}} (and any matching
     * {@code .pc{i}.{sealTxn}} sidecars) for purge after a recovery walk
     * dropped its chain entry. The widest possible reader-visibility
     * window is used because the orphan was never visible to a committed
     * reader; the seal-purge job's scoreboard check is the safety net.
     * <p>
     * The outbox saturation policy mirrors {@link #recordPostingSealPurge}:
     * if the queue is at capacity the oldest entry is dropped. Its files are
     * then left on disk -- the writer-open recovery walk is chain-driven and
     * cannot re-discover a never-published orphan -- so the (bounded) leak
     * relies on the global purge job draining the outbox before saturation.
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
                    .$("]. The dropped entry's files are left on disk; no writer-open scan reclaims them, so keep the global purge job running.")
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

    /**
     * Auxiliary allocations the seal paths make outside the per-stride
     * loop: localHeaderBuf (max stride header, bounded by
     * {@code DENSE_STRIDE * 12} bytes &asymp; 3 KiB regardless of
     * partition shape) and strideIndexBuf
     * ({@code (strideCount + 1) * 8} bytes -- this term scales with
     * keyCount, hitting 32 KiB for keyCount = 1M and unbounded above).
     * Both fast and streaming paths allocate the same pair.
     */
    private long sealAuxiliaryBufferBytes() {
        long strideIndexBuf = ((long) PostingIndexUtils.strideCount(keyCount) + 1L) * Long.BYTES;
        long localHeaderBuf = 4096L; // DENSE_STRIDE delta header rounded up
        return strideIndexBuf + localHeaderBuf;
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

                for (int i = 0; i < activeKeyCount; i++) {
                    int key = Unsafe.getInt(genAddr + (long) i * Integer.BYTES);
                    if (key < 0 || key >= keyCount) {
                        continue; // corrupt sparse gen: key out of range; skip the OOB dirtyStridesAddr write
                    }
                    int stride = key / PostingIndexUtils.DENSE_STRIDE;
                    if (Unsafe.getByte(dirtyStridesAddr + stride) == 0) {
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
        if (dirtyCount == sc) {
            sealIncrementalFallBackToFull(newSealTxn, dirtyStridesAddr, sc, savedSidecarBufs, savedSidecarSizes);
            return;
        }

        // Read gen 0 metadata — do NOT cache gen0Addr here because valueMem
        // may be remapped (mremap) when the seal loop extends it to write
        // new stride data. Use gen0FileOffset and recompute the address each
        // time it's needed.
        long gen0DirOffset = PostingIndexChainEntry.resolveGenDirOffset(chain.getHeadEntryOffset(), 0);
        long gen0FileOffset = keyMem.getLong(gen0DirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
        int gen0KeyCount = keyMem.getInt(gen0DirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
        // Incremental-candidate invariant (gated in seal()): gen 0 is dense and
        // covers every current key. The dirty-stride fold below uses a single
        // keysInStride(keyCount, s) to bound BOTH the gen-0 prefix read and the
        // sizing reduction; were keyCount > gen0KeyCount the gen-0 read would run
        // past gen 0's last stride into packed-data territory.
        assert gen0KeyCount == keyCount
                : "sealIncremental requires gen0KeyCount == keyCount [gen0KeyCount=" + gen0KeyCount + ", keyCount=" + keyCount + ']';
        int gen0SiSize = PostingIndexUtils.strideIndexSize(gen0KeyCount);

        // Allocate output buffers — initialize to 0 so the finally block
        // can conditionally free only those that were successfully allocated.
        int siSize = PostingIndexUtils.strideIndexSize(keyCount);
        int maxHeaderSize = Math.max(
                PostingIndexUtils.strideDeltaHeaderSize(PostingIndexUtils.DENSE_STRIDE),
                PostingIndexUtils.strideFlatHeaderSize(PostingIndexUtils.DENSE_STRIDE)
        );
        // Size the per-stride merge/trial buffers from the ACTUAL aggregate row
        // counts of the dirty strides, not a DENSE_STRIDE * maxPerKey worst
        // case. The old bound assumed every one of the 256 keys in a stride held
        // the single hottest key's count, over-allocating by up to ~256x on
        // skewed symbol columns and tripping the RSS limit with no fallback
        // (sealFull has the streaming pre-flight; this branch did not).
        // Aggregate gen 0 (dense, all keys) plus every sparse gen into a per-key
        // total, then take the max dirty-stride sum (sizes mergedValuesAddr /
        // packedResiduals), the max dirty-stride encoded trial total (sizes
        // bpTrialBuf) and the max single-key count (sizes unpackBatch /
        // encodeCtx). mergeKeyValues below re-derives the same gen 0 + sparse
        // counts, so these bounds match what it writes exactly.
        long maxDirtyStrideTotalL = 0;
        long maxDirtyStrideTrialL = 0;
        int maxDirtyKeyCount = 0;
        long totalCountsSize = (long) keyCount * Integer.BYTES;
        // This sizing block sits between the detection try/catch (which freed
        // dirtyStridesAddr on its own throws) and the main seal try/finally below
        // (which owns dirtyStridesAddr from there on). Wrap it so a throw here --
        // most likely the totalCountsAddr malloc under RSS pressure, but also any
        // valueMem mapping fault in the aggregation -- frees both dirtyStridesAddr
        // (catch) and totalCountsAddr (finally).
        long totalCountsAddr = 0;
        try {
            totalCountsAddr = Unsafe.malloc(totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
            // Zero only the dirty strides' key ranges, not the whole keyCount*4
            // table: every totalCountsAddr read and write below is guarded by
            // dirtyStridesAddr, so clean-stride entries are never touched. Saves
            // the full memset (44 MB at 11M keys) when only a few strides are dirty.
            for (int s = 0; s < sc; s++) {
                if (Unsafe.getByte(dirtyStridesAddr + s) == 0) {
                    continue;
                }
                int strideStart = s * PostingIndexUtils.DENSE_STRIDE;
                int ks = PostingIndexUtils.keysInStride(keyCount, s);
                Unsafe.setMemory(totalCountsAddr + (long) strideStart * Integer.BYTES,
                        (long) ks * Integer.BYTES, (byte) 0);
            }
            // Sparse gens 1..N: add each key's count. Sparse gens only touch
            // dirty strides, so clean strides keep a 0 here and are skipped.
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
                    if (key < 0 || key >= keyCount) {
                        continue; // corrupt sparse gen: key out of range; skip the OOB totalCountsAddr write
                    }
                    int count = Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
                    Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES,
                            Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES) + count);
                }
            }
            // gen 0 (dense, covers every key): fold each dirty stride's per-key
            // counts into totalCountsAddr and, in the SAME sweep, reduce the
            // now-final per-key totals into the sizing maxima. The sparse gens
            // were already summed above, so once gen 0 is added the stride's
            // totals are final -- no second sweep over the dirty strides is
            // needed. gen0KeyCount == keyCount here (incremental-candidate
            // invariant), so one keysInStride bounds both the gen-0 read and the
            // reduction. valueMem is only written from openSealValueFile onward,
            // so gen0Addr is stable across this read-only pass.
            long gen0Addr = valueMem.addressOf(gen0FileOffset);
            for (int s = 0; s < sc; s++) {
                if (Unsafe.getByte(dirtyStridesAddr + s) == 0) {
                    continue;
                }
                int ks = PostingIndexUtils.keysInStride(keyCount, s);
                long strideOff = Unsafe.getLong(gen0Addr + (long) s * Long.BYTES);
                long nextStrideOff = Unsafe.getLong(gen0Addr + (long) (s + 1) * Long.BYTES);
                if (nextStrideOff != strideOff) { // non-empty gen 0 stride: fold its per-key counts in
                    long strideAddr = gen0Addr + gen0SiSize + strideOff;
                    byte mode = Unsafe.getByte(strideAddr);
                    if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                        long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                        for (int j = 0; j < ks; j++) {
                            int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                            int count = Unsafe.getInt(prefixAddr + (long) (j + 1) * Integer.BYTES)
                                    - Unsafe.getInt(prefixAddr + (long) j * Integer.BYTES);
                            Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES,
                                    Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES) + count);
                        }
                    } else {
                        long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                        for (int j = 0; j < ks; j++) {
                            int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                            int count = Unsafe.getInt(countsAddr + (long) j * Integer.BYTES);
                            Unsafe.putInt(totalCountsAddr + (long) key * Integer.BYTES,
                                    Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES) + count);
                        }
                    }
                }
                // Reduce the now-final per-key totals for this dirty stride into
                // the sizing maxima -- actual aggregate (sizes mergedValues /
                // packedResiduals), exact encoded trial total (bpTrialBuf) and max
                // single-key count (unpackBatch / encodeCtx). Runs even when gen 0's
                // stride was empty: the sparse gens above still contributed.
                long strideTotal = 0;
                long strideTrial = 0;
                for (int j = 0; j < ks; j++) {
                    int key = s * PostingIndexUtils.DENSE_STRIDE + j;
                    int c = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    strideTotal += c;
                    if (c > 0) {
                        strideTrial += PostingIndexUtils.computeMaxEncodedSize(c);
                    }
                    if (c > maxDirtyKeyCount) {
                        maxDirtyKeyCount = c;
                    }
                }
                if (strideTotal > maxDirtyStrideTotalL) maxDirtyStrideTotalL = strideTotal;
                if (strideTrial > maxDirtyStrideTrialL) maxDirtyStrideTrialL = strideTrial;
            }
        } catch (Throwable th) {
            Unsafe.free(dirtyStridesAddr, sc, MemoryTag.NATIVE_INDEX_READER);
            throw th;
        } finally {
            if (totalCountsAddr != 0) {
                Unsafe.free(totalCountsAddr, totalCountsSize, MemoryTag.NATIVE_INDEX_READER);
            }
        }

        // A single dirty stride holding >= 2^31 merged values would overflow the
        // int stride sizing the encoder relies on. Defer to the full seal, which
        // throws the same operator-actionable "split the partition" diagnostic
        // for this case (it does not attempt an encode that cannot succeed).
        if (maxDirtyStrideTotalL > Integer.MAX_VALUE) {
            sealIncrementalFallBackToFull(newSealTxn, dirtyStridesAddr, sc, savedSidecarBufs, savedSidecarSizes);
            return;
        }
        int maxDirtyStrideTotal = (int) maxDirtyStrideTotalL;

        // bpTrialBuf and mergedValuesAddr are written directly (no auto-grow),
        // so they must cover the largest dirty stride exactly. unpackBatch and
        // packedResiduals auto-grow at their use sites, so their pre-allocation
        // below is only a per-stride realloc saver.
        long maxBPStrideDataSize = Math.max(maxDirtyStrideTrialL, maxHeaderSize);
        long mergedValuesSize = Math.max(maxDirtyStrideTotal, 1024L) * Long.BYTES;

        // Pre-flight: if even the correctly-sized incremental buffers would
        // breach the RSS limit (one stride genuinely too large for this box),
        // defer to the full seal, which streams per key or throws an
        // operator-actionable diagnostic. Mirrors reencodeAllGenerations.
        final long rssLimit = Unsafe.getRssMemLimit();
        if (rssLimit > 0) {
            final long maxColValueSize = peakCoverColumnValueSize();
            long incrementalPeak = maxBPStrideDataSize
                    + mergedValuesSize
                    + (long) maxDirtyStrideTotal * Long.BYTES        // packedResiduals (auto-grows to stride total)
                    + (long) maxDirtyStrideTotal * Long.BYTES        // packedBuf in writePackedStride (FLAT mode, up to 8 bytes/value)
                    + (long) maxDirtyKeyCount * Long.BYTES           // unpackBatch (auto-grows to max single key)
                    + encodeCtxPeakBytes(maxDirtyKeyCount)
                    + sealAuxiliaryBufferBytes();
            if (coverCount > 0) {
                // incrSidecarBuf is sized to the widest cover value and is
                // allocated (>= 8 bytes/value) even for var-only covers, where
                // the var sidecar writer ignores it -- budget what seal()
                // actually mallocs. peakCoverColumnCompressBufBytes is 0 for
                // var-only covers; peakVarCoverFsstScratchBytes is 0 for
                // fixed-only ones. Mirrors estimateFastPathPeakBytes /
                // estimateStreamingPathPeakBytes, which add the same FSST term --
                // without it a var-size cover with a >= FSST_MIN_RAW_SIZE dirty
                // stride could pass the pre-flight and then OOM in the FSST batch.
                final long sidecarValueSize = Math.max(Long.BYTES, maxColValueSize);
                incrementalPeak += (long) maxDirtyStrideTotal * sidecarValueSize;     // incrSidecarBuf (worst dirty stride)
                incrementalPeak += peakCoverColumnCompressBufBytes(maxDirtyKeyCount); // ALP compressBuf (fixed covers)
                incrementalPeak += (long) coverCount * siSize;                       // incrSidecarSiBufs
                incrementalPeak += peakVarCoverFsstScratchBytes();                   // streaming FSST batch (var covers)
                incrementalPeak += (long) maxDirtyKeyCount * (Long.BYTES + Byte.BYTES); // longWorkspace + exceptionWorkspace (writeSidecarStrideData, fixed covers)
            }
            final long headroom = rssHeadroom(rssLimit, Unsafe.getRssMemUsed());
            if (incrementalPeak > headroom) {
                LOG.info().$("posting incremental seal falling back to full seal under RSS pressure ")
                        .$("[maxDirtyStrideTotal=").$(maxDirtyStrideTotal)
                        .$(", incrementalPeak=").$(incrementalPeak)
                        .$(", headroom=").$(headroom)
                        .$(", dirtyCount=").$(dirtyCount)
                        .$(", strideCount=").$(sc)
                        .I$();
                sealIncrementalFallBackToFull(newSealTxn, dirtyStridesAddr, sc, savedSidecarBufs, savedSidecarSizes);
                return;
            }
        }

        // Committed to the incremental path now (every fallback above returns via
        // sealFull, which sets these in reencodeAllGenerations). The incremental
        // seal is neither the fast nor the streaming full-seal path, so clear
        // isLastSealStreaming and flag isLastSealIncremental for the test hooks
        // rather than leaving a prior full seal's values stale.
        isLastSealStreaming = false;
        isLastSealIncremental = true;

        long strideIndexBuf = 0;
        long bpTrialBuf = 0;
        long localHeaderBuf = 0;
        long mergedValuesAddr = 0;
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

            // Pre-allocate seal-path arrays to avoid per-stride reallocations.
            // Both auto-grow at their use sites if a stride needs more, so these
            // bounds are a perf optimization, not a correctness floor.
            if (maxDirtyKeyCount > unpackBatchCapacity) {
                unpackBatchAddr = Unsafe.realloc(unpackBatchAddr,
                        (long) unpackBatchCapacity * Long.BYTES,
                        (long) maxDirtyKeyCount * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                unpackBatchCapacity = maxDirtyKeyCount;
            }
            if (maxDirtyStrideTotal > packedResidualsCapacity) {
                packedResidualsAddr = Unsafe.realloc(packedResidualsAddr,
                        (long) packedResidualsCapacity * Long.BYTES,
                        (long) maxDirtyStrideTotal * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                packedResidualsCapacity = maxDirtyStrideTotal;
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

            // writeSidecarFixedStrideForColumn assembles one key's raw values
            // into the shared scratch at (1 << shift) bytes per value -- 16
            // for UUID/LONG128/DECIMAL128, 32 for LONG256/DECIMAL256 -- so
            // the scratch must be sized by the widest live fixed-size cover
            // column, not by Long.BYTES: with an 8-bytes-per-value allocation,
            // a single key holding more than half (16B types) or a quarter
            // (32B types) of a dirty stride's merged values writes past the
            // allocation. The full-seal twins already size by value width.
            final long sidecarValueSize = Math.max(Long.BYTES, peakCoverColumnValueSize());

            for (int s = 0; s < sc; s++) {
                long strideOff = sealValueMem.getAppendOffset() - sealOffset - siSize;
                Unsafe.putLong(strideIndexBuf + (long) s * Long.BYTES, strideOff);

                // Recompute gen0Addr each iteration because valueMem writes
                // (putBlockOfBytes, putInt, etc.) can trigger mremap which
                // moves the mapping, invalidating cached native addresses.
                long gen0Addr = valueMem.addressOf(gen0FileOffset);

                if (Unsafe.getByte(dirtyStridesAddr + s) == 0) {
                    // Clean stride: copy verbatim from gen 0
                    copyStrideFromGen0(gen0Addr, gen0KeyCount, gen0SiSize, s);
                    // Sidecar: copy old stride block verbatim. The incremental
                    // candidate check guarantees gen0KeyCount == keyCount, so
                    // the old sidecar's stride geometry matches the new one.
                    if (incrSidecarSiBufs != null && oldSidecarBufs != null) {
                        long oldStrideIdxBase = PostingIndexUtils.PC_HEADER_SIZE;
                        for (int c = 0; c < coverCount; c++) {
                            if (incrSidecarSiBufs[c] == 0) continue;
                            MemoryMARW mem = sidecarMems.getQuick(c);
                            Unsafe.putLong(
                                    incrSidecarSiBufs[c] + (long) s * Long.BYTES,
                                    mem.getAppendOffset() - siSize);
                            if (oldSidecarBufs[c] != 0 && oldSidecarSizes[c] > oldStrideIdxBase + gen0SiSize) {
                                long oldStrideOff = Unsafe.getLong(oldSidecarBufs[c] + oldStrideIdxBase + (long) s * Long.BYTES);
                                // The last stride's upper bound is the stored
                                // sentinel: post-seal gen flushes append raw
                                // gen blocks after the sealed region, so the
                                // snapshot length would sweep those blocks
                                // into the copy.
                                long nextStrideOff = Unsafe.getLong(oldSidecarBufs[c] + oldStrideIdxBase + (long) (s + 1) * Long.BYTES);
                                long sentinel = s + 1 < sc
                                        ? Unsafe.getLong(oldSidecarBufs[c] + oldStrideIdxBase + (long) sc * Long.BYTES)
                                        : nextStrideOff;
                                // seal() validated the snapshot layout; these
                                // bounds turn any residual mismatch into a
                                // clear error instead of feeding a garbage
                                // size to putBlockOfBytes.
                                if (oldStrideOff < oldStrideIdxBase
                                        || nextStrideOff < oldStrideOff
                                        || sentinel < nextStrideOff
                                        || sentinel + gen0SiSize > oldSidecarSizes[c]) {
                                    throw CairoException.critical(0)
                                            .put("posting index sidecar snapshot is structurally invalid [cover=").put(c)
                                            .put(", stride=").put(s)
                                            .put(", oldStrideOff=").put(oldStrideOff)
                                            .put(", nextStrideOff=").put(nextStrideOff)
                                            .put(", sentinel=").put(sentinel)
                                            .put(", snapshotSize=").put(oldSidecarSizes[c])
                                            .put(']');
                                }
                                long strideDataSize = nextStrideOff - oldStrideOff;
                                if (strideDataSize > 0) {
                                    long oldStrideDataAddr = oldSidecarBufs[c] + gen0SiSize + oldStrideOff;
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
                            long neededBuf = totalStrideVals * sidecarValueSize;
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

    // Release the incremental seal's detection scratch (and any covered-column
    // sidecar snapshot, which the full seal rebuilds from scratch) before
    // deferring to sealFull. Used when there are no clean strides to save, when
    // a dirty stride is too large for int stride sizing, or when the
    // correctly-sized incremental buffers would breach the RSS limit.
    private void sealIncrementalFallBackToFull(long newSealTxn, long dirtyStridesAddr, int sc, long[] savedSidecarBufs, long[] savedSidecarSizes) {
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
    }

    private void spillKey(int key, int count) {
        ensureSpillArrays(key);
        int prevCount = Unsafe.getInt(spillKeyCountsAddr + (long) key * Integer.BYTES);
        int needed = prevCount + count;
        // Grow per-key spill buffer if needed
        int curCap = Unsafe.getInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES);
        if (needed > curCap) {
            // Long-arithmetic doubling: curCap * 2 wraps to negative once
            // curCap >= 2^30, which would let Math.max pick `needed` and
            // silently break the doubling-amortization invariant. Clamp
            // to Integer.MAX_VALUE; if `needed` itself overflows int, the
            // sealCheck above (totalValues > Integer.MAX_VALUE) is the
            // long-term ceiling and would have already fired -- but
            // assert here for the unit-test case.
            long doubled = (long) curCap * 2L;
            long want = Math.max(needed, doubled);
            if (want > Integer.MAX_VALUE) {
                throw CairoException.critical(0)
                        .put("posting index spill capacity exceeds 2^31 entries [key=").put(key)
                        .put(", needed=").put(needed)
                        .put(", curCap=").put(curCap)
                        .put("]; split commit into smaller batches");
            }
            int newCap = (int) Math.max(want, 32L); // minimum 32 values
            long oldSize = (long) curCap * Long.BYTES;
            long newSize = (long) newCap * Long.BYTES;
            long oldAddr = Unsafe.getLong(spillKeyAddrsAddr + (long) key * Long.BYTES);
            long newAddr = Unsafe.realloc(oldAddr, oldSize, newSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.putLong(spillKeyAddrsAddr + (long) key * Long.BYTES, newAddr);
            Unsafe.putInt(spillKeyCapacitiesAddr + (long) key * Integer.BYTES, newCap);
            totalSpillBytes += newSize - oldSize;
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
        // Bound peak RSS: if the spill arena has grown past the configured
        // budget, drain pending+spill into a fresh sparse generation now and
        // free the anonymous-heap buffers. Cheap when within budget (one
        // long compare + branch). Critical for ALTER ADD INDEX, IndexBuilder,
        // and per-O3-seal rebuilds (PR 7077) where the loop above could
        // otherwise accumulate the whole partition in heap memory.
        compactIfOverBudget();
    }

    private void switchToSealedValueFile(long newTxn) {
        // Posting indexes are always path-based: the fd-based open throws
        // (UnsupportedOperationException) and O3 routes posting through
        // openFromO3Context, so partitionPath is always set by the time a seal
        // or rollback reaches this method. Assert the invariant -- a path-less
        // caller would advance sealTxn while the on-disk .pv stayed at the old
        // txn.
        assert partitionPath.size() > 0 : "switchToSealedValueFile called without a partition path";
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

    /**
     * Streaming FSST compression of an already-written uncompressed
     * sidecar stride block. Trains a symbol table from a sample, then
     * encodes in {@link #FSST_BATCH_SIZE}-sized batches into a staging
     * area past the uncompressed block. If the compressed total beats
     * the uncompressed block size, the sidecar is rewritten in
     * FSST-compressed format starting at {@code blockStart}; otherwise
     * the uncompressed block is left in place.
     * <p>
     * No anonymous-heap allocation scales with totalCount or rawDataLen --
     * the encoder lives inside JNI (~900 KiB), the sample and batch
     * scratch buffers are sized to constants. The sidecar mmap grows
     * to hold the staging area; that growth is paged by the OS and
     * truncated on close.
     */
    private void tryFsstStreamingCompress(
            MemoryMARW mem, int totalCount, boolean rawLongOffsets,
            long blockStart, long offsetsStart, long dataStart, long rawDataLen
    ) {
        final long uncompressedEnd = mem.getAppendOffset();
        final long uncompressedBlockSize = uncompressedEnd - blockStart;

        // Stride-sample the input. Pick every Nth value (stable) so the
        // sample reflects the stride's value-length distribution. Stop
        // when we cover SAMPLE_TARGET_BYTES or SAMPLE_TARGET_COUNT values.
        final int sampleStride = Math.max(1, totalCount / FSST_SAMPLE_TARGET_COUNT);
        final int sampleCap = Math.min(FSST_SAMPLE_TARGET_COUNT, (totalCount + sampleStride - 1) / sampleStride);
        final long sampleLensSize = (long) sampleCap * Long.BYTES;
        long sampleLensAddr = Unsafe.malloc(sampleLensSize, MemoryTag.NATIVE_INDEX_READER);
        long samplePtrsAddr = 0L;
        long encoder = 0L;
        try {
            samplePtrsAddr = Unsafe.malloc(sampleLensSize, MemoryTag.NATIVE_INDEX_READER);

            // Address validity contract: writeVarStrideDataAttempt extended
            // the sidecar to hold uncompressed bytes; the staging area below
            // also extends it. Capture the raw data address AFTER the extend
            // so addressOf is stable for the lifetime of this call.
            final long stagingOffsetsSize = (long) (totalCount + 1) * Long.BYTES;
            // Worst-case FSST expansion is ~2x; mmap is sparse, truncated
            // at close — no eager prealloc.
            final long stagingCmpCap = 2L * rawDataLen + 16L;
            final long stagingSize = stagingOffsetsSize + stagingCmpCap;
            try {
                mem.extend(uncompressedEnd + stagingSize);
            } catch (CairoException e) {
                // allocateDiskSpace (ENOSPC) leaves the mapping intact;
                // mremap failure closes it. Reset append position when
                // possible so the uncompressed block stays valid on disk.
                if (mem.isOpen()) {
                    mem.jumpTo(uncompressedEnd);
                }
                throw e;
            }
            final long rawDataAddr = mem.addressOf(dataStart);
            final long stagingOffsetsAddr = mem.addressOf(uncompressedEnd);
            final long stagingDataAddr = stagingOffsetsAddr + stagingOffsetsSize;

            // Build sample arrays from the just-written uncompressed block.
            int sampleCount = 0;
            long sampleBytes = 0L;
            for (int idx = 0; idx < totalCount && sampleCount < sampleCap && sampleBytes < FSST_SAMPLE_TARGET_BYTES; idx += sampleStride) {
                final long lo = readSidecarOffsetWidened(mem, offsetsStart, idx, rawLongOffsets);
                final long hi = readSidecarOffsetWidened(mem, offsetsStart, idx + 1, rawLongOffsets);
                final long len = hi - lo;
                if (len > 0) {
                    Unsafe.putLong(sampleLensAddr + (long) sampleCount * Long.BYTES, len);
                    Unsafe.putLong(samplePtrsAddr + (long) sampleCount * Long.BYTES, rawDataAddr + lo);
                    sampleCount++;
                    sampleBytes += len;
                }
            }
            if (sampleCount == 0) {
                return; // every value empty; nothing to FSST
            }

            encoder = FSSTNative.createEncoder(sampleCount, sampleLensAddr, samplePtrsAddr);
            if (encoder == 0L) {
                LOG.info().$("posting seal FSST createEncoder failed [sampleCount=").$(sampleCount)
                        .$(", sampleBytes=").$(sampleBytes).I$();
                return;
            }

            ensureFsstStreamingScratch();

            // Stream-compress in batches. Each batch fills fsstSrcOffsAddr
            // with widened (8B) src offsets for the batch's slice, calls
            // compressBatch0, then copies the produced compressed bytes to
            // the staging area and writes per-value offsets (relative to
            // the start of the compressed-bytes blob).
            long compressedTotal = 0L;
            int processed = 0;
            while (processed < totalCount) {
                int batchCount = Math.min(FSST_BATCH_SIZE, totalCount - processed);
                // 8B widened src offsets for the batch. Compress wants the
                // first entry as the start-of-first-value offset; the
                // last as past-the-end-of-last-value.
                for (int i = 0; i <= batchCount; i++) {
                    long off = readSidecarOffsetWidened(mem, offsetsStart, processed + i, rawLongOffsets);
                    Unsafe.putLong(fsstSrcOffsAddr + (long) i * Long.BYTES, off);
                }
                long produced;
                while (true) {
                    produced = FSSTNative.compressBatch(
                            encoder, batchCount,
                            rawDataAddr, fsstSrcOffsAddr,
                            fsstCmpCap, fsstCmpAddr,
                            fsstCmpOffsAddr,
                            fsstBatchScratchAddr);
                    if (produced < 0) {
                        LOG.info().$("posting seal FSST compressBatch failed [processed=").$(processed)
                                .$(", batchCount=").$(batchCount)
                                .I$();
                        return; // keep uncompressed
                    }
                    if (produced > 0) {
                        break;
                    }
                    // produced == 0 means the batch output buffer was
                    // too small for even the first value; grow and retry.
                    long newCap = Math.max(fsstCmpCap * 2L, (long) batchCount * 32L);
                    if (newCap > FSST_BATCH_OUT_CAP_MAX) {
                        LOG.info().$("posting seal FSST compressBatch out cap exceeded [")
                                .$("processed=").$(processed)
                                .$(", batchCount=").$(batchCount)
                                .$(", fsstCmpCap=").$(fsstCmpCap)
                                .I$();
                        return; // keep uncompressed
                    }
                    fsstCmpAddr = Unsafe.realloc(fsstCmpAddr, fsstCmpCap, newCap, MemoryTag.NATIVE_INDEX_READER);
                    fsstCmpCap = newCap;
                }
                // compressBatch0 wrote (produced + 1) offsets into
                // fsstCmpOffsAddr (byte positions within fsstCmpAddr).
                // Reposition the bytes into the staging compressed region
                // and write per-value offsets (relative to staging-data
                // base, i.e. accumulating compressedTotal).
                final long firstByteOff = Unsafe.getLong(fsstCmpOffsAddr);
                final long endByteOff = Unsafe.getLong(fsstCmpOffsAddr + produced * Long.BYTES);
                final long batchBytesLen = endByteOff - firstByteOff;
                if (batchBytesLen > 0) {
                    Vect.memcpy(stagingDataAddr + compressedTotal, fsstCmpAddr + firstByteOff, batchBytesLen);
                }
                for (long i = 0; i < produced; i++) {
                    long batchOff = Unsafe.getLong(fsstCmpOffsAddr + i * Long.BYTES);
                    Unsafe.putLong(stagingOffsetsAddr + (processed + i) * Long.BYTES,
                            compressedTotal + (batchOff - firstByteOff));
                }
                compressedTotal += batchBytesLen;
                processed += (int) produced;
            }
            // Sentinel "past-the-end" offset.
            Unsafe.putLong(stagingOffsetsAddr + (long) totalCount * Long.BYTES, compressedTotal);

            // Export the trained table.
            if (fsstTableAddr == 0) {
                fsstTableAddr = Unsafe.malloc(FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_INDEX_READER);
            }
            int tableLen = FSSTNative.exportEncoder(encoder, fsstTableAddr);
            if (tableLen <= 0) {
                return; // export failed; keep uncompressed
            }

            // Decide. Compressed block layout uses 8B offsets (matches
            // staging) so its on-disk size is fixed by tableLen and
            // compressedTotal.
            final long compressedBlockSize = 4L + 2L + tableLen
                    + (long) (totalCount + 1) * Long.BYTES + compressedTotal;
            if (compressedBlockSize >= uncompressedBlockSize) {
                // Compression didn't help. Truncate back to end of
                // uncompressed; the staging area becomes garbage that
                // the next stride or close-time truncate cleans up.
                mem.jumpTo(uncompressedEnd);
                return;
            }

            // Commit the compressed block in place of the uncompressed
            // one. The staging area sits past uncompressedEnd and the
            // final destination starts at blockStart, so the memcpy
            // ranges don't overlap (dest < src, dest + size < src).
            mem.jumpTo(blockStart);
            int flags = FSSTNative.FSST_BLOCK_FLAG | PostingIndexUtils.LONG_OFFSETS_FLAG;
            mem.putInt(totalCount | flags);
            mem.putShort((short) tableLen);
            mem.putBlockOfBytes(fsstTableAddr, tableLen);
            mem.putBlockOfBytes(stagingOffsetsAddr, stagingOffsetsSize);
            if (compressedTotal > 0) {
                mem.putBlockOfBytes(stagingDataAddr, compressedTotal);
            }
            assert mem.getAppendOffset() == blockStart + compressedBlockSize;
        } finally {
            if (encoder != 0L) {
                FSSTNative.destroyEncoder(encoder);
            }
            if (samplePtrsAddr != 0L) {
                Unsafe.free(samplePtrsAddr, sampleLensSize, MemoryTag.NATIVE_INDEX_READER);
            }
            Unsafe.free(sampleLensAddr, sampleLensSize, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    /**
     * Best-effort removal of the {@code .pv.{newSealTxn}} and any matching
     * {@code .pc{c}.{...}.{newSealTxn}} sidecar files when seal staging
     * aborts before {@link #switchToSealedValueFile}. The chain never
     * advertised these files, so no reader can have pinned them; unlink
     * errors are logged and ignored rather than masking the original throw.
     */
    private void unlinkOrphanSealFiles(long newSealTxn) {
        if (partitionPath.size() == 0) {
            return;
        }
        Path p = Path.getThreadLocal(partitionPath);
        int plen = p.size();
        try {
            LPSZ pv = PostingIndexUtils.valueFileName(p, indexName, postingColumnNameTxn, newSealTxn);
            if (ff.exists(pv) && !ff.removeQuiet(pv)) {
                LOG.error().$("could not unlink staged .pv [path=").$(pv).$(']').$();
            }
        } catch (Throwable ignore) {
            // path construction is unlikely to throw; swallow so we don't
            // mask the caller's original exception.
        }
        for (int c = 0; c < coverCount; c++) {
            int colIdx = coveredColumnIndices.getQuick(c);
            if (colIdx < 0) {
                continue;
            }
            try {
                long covT = getCoveredColumnNameTxn(c);
                LPSZ pc = PostingIndexUtils.coverDataFileName(p.trimTo(plen), indexName, c, postingColumnNameTxn, covT, newSealTxn);
                if (ff.exists(pc) && !ff.removeQuiet(pc)) {
                    LOG.error().$("could not unlink staged .pc [path=").$(pc).$(']').$();
                }
            } catch (Throwable ignore) {
                // see above
            }
        }
        p.trimTo(plen);
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
     * Streaming counterpart of {@link #writeSidecarFixedStrideForColumn}.
     * Processes one stride at a time but decodes from the freshly-sealed
     * dense gen 0 one key at a time into the shared {@code keyBuffer},
     * so the per-stride sidecarBuf is sized to {@code maxKeyCount *
     * valueSize} rather than the worst stride's aggregate.
     * <p>
     * Output layout matches {@link #writeSidecarFixedStrideForColumn}'s:
     * per-key compressed blocks with a leading {@code [key_offsets: ks
     * x 8B]} table per stride. A reader cannot tell whether the
     * sidecar was produced by the fast path or the streaming path.
     */
    private void writeSidecarFixedStreamingForColumn(
            MemoryMARW mem, int c, long colTop, int colType, int shift,
            int ks, int strideStart, int[] keyCounts, long keyBuffer, int maxKeyCount,
            long sidecarBuf, long longWorkspaceAddr, long exceptionWorkspaceAddr,
            long compressBuf
    ) {
        int valueSize = 1 << shift;
        int keyOffsetsSize = ks * Long.BYTES;

        long keyOffsetsPos = mem.getAppendOffset();
        for (int j = 0; j < ks; j++) {
            mem.putLong(0L); // placeholder
        }

        // valueMem (the freshly sealed gen 0) is the read-only decode source, so
        // its base address and the stride index are invariant across the per-key
        // decodes below -- resolve them once rather than per key.
        long gen0Base = valueMem.addressOf(0);
        int s = strideStart / PostingIndexUtils.DENSE_STRIDE;
        for (int j = 0; j < ks; j++) {
            int count = keyCounts[j];
            long currentPos = mem.getAppendOffset();
            long keyDataStart = keyOffsetsPos + keyOffsetsSize;
            long keyOffset = currentPos - keyDataStart;
            mem.putLong(keyOffsetsPos + (long) j * Long.BYTES, keyOffset);

            if (count == 0) {
                continue;
            }

            // Decode this key's row IDs from the freshly-sealed dense gen 0
            // directly into the shared keyBuffer.
            int decoded = decodeDenseGenSingleKey(gen0Base, keyCount, s, j, keyBuffer, maxKeyCount);
            if (decoded != count) {
                throw CairoException.critical(0)
                        .put("posting index streaming sidecar decode mismatch [key=").put(strideStart + j)
                        .put(", expected=").put(count)
                        .put(", decoded=").put(decoded).put(']');
            }

            // Materialise this key's covered values into sidecarBuf, then compress.
            long rawOffset = 0;
            for (int i = 0; i < count; i++) {
                long rowId = Unsafe.getLong(keyBuffer + (long) i * Long.BYTES);
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

            boolean isDesignatedTs = timestampColumnIndex >= 0
                    && coveredColumnIndices.getQuick(c) == timestampColumnIndex;
            int compressedSize = compressSidecarBlock(sidecarBuf, count, shift, colType,
                    isDesignatedTs, compressBuf, longWorkspaceAddr, exceptionWorkspaceAddr);
            mem.putBlockOfBytes(compressBuf, compressedSize);
        }
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

    private void writeSidecarForColumnStreaming(
            int c, int sc, int siSize,
            long totalCountsAddr, long keyBuffer, int maxKeyCount,
            int[] keyCounts
    ) {
        MemoryMARW mem = sidecarMems.getQuick(c);
        int colType = coveredColumnTypes.getQuick(c);
        int shift = coveredColumnShifts.getQuick(c);
        long colTop = coveredColumnTops.getQuick(c);

        // shift < 0 -> variable-size cover: writeSidecarVarStreamingForColumn
        // writes directly into the sidecar mem (offsets + values + streaming
        // FSST), so the fixed-size scratch below is allocated only for the
        // fixed-size path.
        final boolean isVarSize = shift < 0;
        int valueSize = isVarSize ? 0 : (1 << shift);

        long sidecarStrideIndexBuf = 0;
        long sidecarBuf = 0;
        long longWorkspaceSize = (long) maxKeyCount * Long.BYTES;
        long longWorkspaceAddr = 0;
        long exceptionWorkspaceAddr = 0;
        int compressBufSize = (!isVarSize && maxKeyCount > 0) ? CoveringCompressor.maxCompressedSize(maxKeyCount, colType) : 0;
        long compressBuf = 0;
        long sidecarBufSize = isVarSize ? 0 : (long) maxKeyCount * valueSize;

        try {
            sidecarStrideIndexBuf = Unsafe.malloc(siSize, MemoryTag.NATIVE_INDEX_READER);
            for (int i = 0; i < siSize; i += Long.BYTES) {
                mem.putLong(0L); // stride index placeholders
            }
            if (!isVarSize && maxKeyCount > 0) {
                longWorkspaceAddr = Unsafe.malloc(longWorkspaceSize, MemoryTag.NATIVE_INDEX_READER);
                exceptionWorkspaceAddr = Unsafe.malloc(maxKeyCount, MemoryTag.NATIVE_INDEX_READER);
                sidecarBuf = Unsafe.malloc(sidecarBufSize, MemoryTag.NATIVE_INDEX_READER);
                if (compressBufSize > 0) {
                    compressBuf = Unsafe.malloc(compressBufSize, MemoryTag.NATIVE_INDEX_READER);
                }
            }

            for (int s = 0; s < sc; s++) {
                int ks = PostingIndexUtils.keysInStride(keyCount, s);
                int strideStart = s * PostingIndexUtils.DENSE_STRIDE;

                // hasAnyValues rather than a summed count, see streaming
                // reencode path for rationale.
                boolean hasAnyValues = false;
                for (int j = 0; j < ks; j++) {
                    int key = strideStart + j;
                    int count = Unsafe.getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    keyCounts[j] = count;
                    if (count > 0) hasAnyValues = true;
                }

                Unsafe.putLong(
                        sidecarStrideIndexBuf + (long) s * Long.BYTES,
                        mem.getAppendOffset() - siSize);

                if (!hasAnyValues) {
                    continue;
                }

                if (isVarSize) {
                    writeSidecarVarStreamingForColumn(mem, c, colTop, colType,
                            ks, strideStart, keyCounts, keyBuffer, maxKeyCount);
                } else {
                    writeSidecarFixedStreamingForColumn(mem, c, colTop, colType, shift,
                            ks, strideStart, keyCounts, keyBuffer, maxKeyCount,
                            sidecarBuf, longWorkspaceAddr, exceptionWorkspaceAddr, compressBuf);
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
                Unsafe.free(exceptionWorkspaceAddr, maxKeyCount, MemoryTag.NATIVE_INDEX_READER);
            }
            if (compressBuf != 0) {
                Unsafe.free(compressBuf, compressBufSize, MemoryTag.NATIVE_INDEX_READER);
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
                // Tombstoned slot (dropped INCLUDE column): mapCoveringColumnsForSeal
                // encodes it as type=-1/shift=0, openSidecarFiles leaves its sidecar
                // mem null and creates no .pcN file, so there is nothing to write.
                // Without this skip the shift=0 sentinel routes into the fixed-stride
                // writer, which rejects type -1. Same skip as writeSidecarsPerColumn
                // on the full-seal path; captureCoverEndOffsets publishes end offset
                // 0 for the slot and covered reads fall back to NULL.
                if (coveredColumnIndices.getQuick(c) < 0) {
                    continue;
                }
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
            boolean isWritten = writeSidecarVarGenBlockAttempt(mem, covIdx, colTop, colType, totalValues, true);
            assert isWritten : "long offsets must not overflow";
        }
    }

    private boolean writeSidecarVarGenBlockAttempt(
            MemoryMARW mem, int covIdx, long colTop, int colType, int totalValues, boolean longOffsets
    ) {
        if (totalValues >= PostingIndexUtils.LONG_OFFSETS_FLAG) {
            throw CairoException.critical(0)
                    .put("posting index var cover gen block exceeds 2^30 values [totalValues=").put(totalValues)
                    .put("]; the 4-byte count field shares bits 30 and 31 with the long-offset and FSST flags. Split the partition into smaller commits");
        }
        mem.putInt(longOffsets ? totalValues | PostingIndexUtils.LONG_OFFSETS_FLAG : totalValues);
        long offsetsStart = mem.getAppendOffset();
        // Reserve the offsets region without zeroing -- writeVarOffset overwrites
        // every slot below.
        mem.skip((long) (totalValues + 1) * (longOffsets ? Long.BYTES : Integer.BYTES));
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
     * Per-key streaming var-size sidecar writer (the rollback / RSS-bounded
     * counterpart of {@link #writeSidecarVarStrideData}, which reads a whole
     * stride's merged row ids). For one stride it reserves the
     * {@code [totalCount][offsets][bytes]} block, then for each key decodes its
     * row ids out of the freshly sealed dense gen 0 into {@code keyBuffer} and
     * writes the covered var value at each, finally streaming-FSST-compressing
     * the data region. Anonymous-heap scratch stays bounded by the largest key
     * plus the FSST batch, not the whole stride's bytes.
     */
    private void writeSidecarVarStreamingForColumn(
            MemoryMARW mem, int c, long colTop, int colType,
            int ks, int strideStart, int[] keyCounts, long keyBuffer, int maxKeyCount
    ) {
        int totalCount = 0;
        for (int j = 0; j < ks; j++) {
            totalCount += keyCounts[j];
        }
        final long blockStart = mem.getAppendOffset();
        boolean longOffsets = false;
        if (!writeVarStreamingAttempt(mem, c, colTop, colType, ks, strideStart, keyCounts, keyBuffer, maxKeyCount, totalCount, false)) {
            mem.jumpTo(blockStart);
            longOffsets = true;
            boolean isWritten = writeVarStreamingAttempt(mem, c, colTop, colType, ks, strideStart, keyCounts, keyBuffer, maxKeyCount, totalCount, true);
            assert isWritten : "long offsets must not overflow";
        }
        final int offsetWidth = longOffsets ? Long.BYTES : Integer.BYTES;
        final long offsetsStart = blockStart + Integer.BYTES;
        final long dataStart = offsetsStart + (long) (totalCount + 1) * offsetWidth;
        final long rawDataLen = mem.getAppendOffset() - dataStart;
        if (rawDataLen < FSST_MIN_RAW_SIZE || totalCount == 0) {
            return;
        }
        tryFsstStreamingCompress(mem, totalCount, longOffsets, blockStart, offsetsStart, dataStart, rawDataLen);
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
     * Offset width W is 4 bytes by default for uncompressed blocks; the
     * FSST-compressed branch always emits 8-byte offsets and sets
     * LONG_OFFSETS_FLAG. Always-8B costs ~4 bytes per value on the
     * compressed side but lets the streaming compressor avoid a
     * rewind-and-narrow pass when total compressed bytes fit in 32 bits;
     * the saving on per-stride scratch is far bigger than the wasted
     * offset bytes.
     * <p>
     * Compression strategy: write the uncompressed block first (so the
     * sidecar is always valid even if FSST training fails), then attempt
     * streaming FSST. Streaming trains a symbol table from a stride sample
     * (~64 KiB of input bytes) and encodes the full input in fixed-size
     * batches into a staging area past the uncompressed block. Total
     * anonymous-heap scratch is bounded by {@link #FSST_BATCH_SIZE} -- a
     * few MiB regardless of stride bytes. If the compressed result is
     * smaller, the staging bytes are copied to {@code blockStart} and
     * the uncompressed block is overwritten; otherwise the staging is
     * discarded and the uncompressed block stays.
     */
    private void writeSidecarVarStrideData(
            MemoryMARW mem, int covIdx, long colTop, int colType,
            int ks, int[] keyCounts, long[] keyOffsets, long mergedValuesAddr
    ) {
        int totalCount = 0;
        for (int j = 0; j < ks; j++) {
            totalCount += keyCounts[j];
        }

        final long blockStart = mem.getAppendOffset();
        boolean longOffsets = false;
        if (!writeVarStrideDataAttempt(mem, covIdx, colTop, colType, ks, keyCounts, keyOffsets, mergedValuesAddr, totalCount, false)) {
            mem.jumpTo(blockStart);
            longOffsets = true;
            boolean isWritten = writeVarStrideDataAttempt(mem, covIdx, colTop, colType, ks, keyCounts, keyOffsets, mergedValuesAddr, totalCount, true);
            assert isWritten : "long offsets must not overflow";
        }
        final int offsetWidth = longOffsets ? Long.BYTES : Integer.BYTES;
        final long offsetsStart = blockStart + Integer.BYTES;
        final long dataStart = offsetsStart + (long) (totalCount + 1) * offsetWidth;

        final long rawDataLen = mem.getAppendOffset() - dataStart;
        if (rawDataLen < FSST_MIN_RAW_SIZE || totalCount == 0) {
            return;
        }
        tryFsstStreamingCompress(mem, totalCount, longOffsets, blockStart, offsetsStart, dataStart, rawDataLen);
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
                if (coveredColumnIndices.getQuick(c) < 0) {
                    continue;
                }
                writeSidecarForColumn(c, sc, siSize, totalCountsAddr, strideValsAddr, globalMaxKeyCount);
            }
        }
    }

    /**
     * Streaming sidecar writer. Same per-column orchestration as
     * {@link #writeSidecarsPerColumn}, but the per-column body decodes one
     * key at a time from the freshly-sealed dense gen 0. Both fixed- and
     * var-size cover columns are handled; the per-column body dispatches on
     * column type (var-size columns stream through
     * {@link #writeSidecarVarStreamingForColumn}).
     *
     * @param keyBuffer   shared per-key decode buffer (sized to {@code maxKeyCount * 8} bytes)
     * @param maxKeyCount worst single-key count across the partition
     */
    private void writeSidecarsPerColumnStreaming(long totalCountsAddr, long keyBuffer, int maxKeyCount) {
        unmapCoveredColumnReads();

        int sc = PostingIndexUtils.strideCount(keyCount);
        int siSize = PostingIndexUtils.strideIndexSize(keyCount);
        int[] keyCounts = strideKeyCounts;

        if (coveredColumnNames.size() > 0 && coveredPartitionPath.size() > 0) {
            Path p = Path.getThreadLocal(coveredPartitionPath);
            for (int c = 0; c < coverCount; c++) {
                if (coveredColumnIndices.getQuick(c) < 0) {
                    continue;
                }
                try {
                    mapCoveredColumn(p, c);
                    writeSidecarForColumnStreaming(c, sc, siSize, totalCountsAddr, keyBuffer, maxKeyCount, keyCounts);
                } finally {
                    unmapCoveredColumn(c);
                }
            }
        } else if (coveredColumnAddrs.size() > 0) {
            ensureCoveredColumnReadMaps();
            for (int c = 0; c < coverCount; c++) {
                if (coveredColumnIndices.getQuick(c) < 0) {
                    continue;
                }
                writeSidecarForColumnStreaming(c, sc, siSize, totalCountsAddr, keyBuffer, maxKeyCount, keyCounts);
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

    /**
     * Per-key variant of {@link #writeVarStrideDataAttempt}: instead of reading
     * a key's row ids out of a pre-merged stride buffer, decode them from the
     * freshly sealed dense gen 0 one key at a time. Returns false (so the caller
     * retries with 8-byte offsets) when a 4-byte offset would overflow.
     */
    private boolean writeVarStreamingAttempt(
            MemoryMARW mem, int c, long colTop, int colType,
            int ks, int strideStart, int[] keyCounts, long keyBuffer, int maxKeyCount,
            int totalCount, boolean longOffsets
    ) {
        if (totalCount >= PostingIndexUtils.LONG_OFFSETS_FLAG) {
            throw CairoException.critical(0)
                    .put("posting index var cover stride exceeds 2^30 values [totalCount=").put(totalCount)
                    .put("]; the 4-byte count field shares bits 30 and 31 with the long-offset and FSST flags. Split the partition into smaller commits");
        }
        mem.putInt(longOffsets ? totalCount | PostingIndexUtils.LONG_OFFSETS_FLAG : totalCount);
        long offsetsStart = mem.getAppendOffset();
        // Reserve the offsets region without zeroing -- writeVarOffset overwrites
        // every slot below.
        mem.skip((long) (totalCount + 1) * (longOffsets ? Long.BYTES : Integer.BYTES));
        long dataStart = mem.getAppendOffset();

        int s = strideStart / PostingIndexUtils.DENSE_STRIDE;
        // valueMem (the freshly sealed gen 0) is the read-only decode source, so
        // its base address is invariant across the per-key decodes below -- resolve
        // it once rather than per key.
        long gen0Base = valueMem.addressOf(0);
        int valueOrdinal = 0;
        for (int j = 0; j < ks; j++) {
            int count = keyCounts[j];
            if (count == 0) {
                continue;
            }
            // gen 0 post-reseal holds exactly the surviving rows for this key,
            // so decoded == count; read the covered var value at each row id.
            int decoded = decodeDenseGenSingleKey(gen0Base, keyCount, s, j, keyBuffer, maxKeyCount);
            if (decoded != count) {
                throw CairoException.critical(0)
                        .put("posting index streaming var sidecar decode mismatch [key=").put(strideStart + j)
                        .put(", expected=").put(count)
                        .put(", decoded=").put(decoded).put(']');
            }
            for (int i = 0; i < count; i++) {
                long rowId = Unsafe.getLong(keyBuffer + (long) i * Long.BYTES);
                long off = mem.getAppendOffset() - dataStart;
                if (!longOffsets && off > Integer.MAX_VALUE) {
                    return false;
                }
                writeVarOffset(mem, offsetsStart, valueOrdinal, off, longOffsets);
                writeVarValue(mem, c, colTop, rowId, colType);
                valueOrdinal++;
            }
        }
        long endOff = mem.getAppendOffset() - dataStart;
        if (!longOffsets && endOff > Integer.MAX_VALUE) {
            return false;
        }
        writeVarOffset(mem, offsetsStart, valueOrdinal, endOff, longOffsets);
        return true;
    }

    private boolean writeVarStrideDataAttempt(
            MemoryMARW mem, int covIdx, long colTop, int colType,
            int ks, int[] keyCounts, long[] keyOffsets, long mergedValuesAddr,
            int totalCount, boolean longOffsets
    ) {
        if (totalCount >= PostingIndexUtils.LONG_OFFSETS_FLAG) {
            throw CairoException.critical(0)
                    .put("posting index var cover stride exceeds 2^30 values [totalCount=").put(totalCount)
                    .put("]; the 4-byte count field shares bits 30 and 31 with the long-offset and FSST flags. Split the partition into smaller commits");
        }
        mem.putInt(longOffsets ? totalCount | PostingIndexUtils.LONG_OFFSETS_FLAG : totalCount);
        long offsetsStart = mem.getAppendOffset();
        // Reserve the offsets region without zeroing -- writeVarOffset overwrites
        // every slot below.
        mem.skip((long) (totalCount + 1) * (longOffsets ? Long.BYTES : Integer.BYTES));
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

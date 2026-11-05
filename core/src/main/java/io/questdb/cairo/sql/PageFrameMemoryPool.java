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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.idx.CoveringCompressor;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.IntLongHashMap;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

/**
 * Provides addresses for page frames in both native and Parquet formats.
 * Memory in native page frames is mmapped, so no additional actions are
 * necessary. Parquet frames must be explicitly deserialized into
 * the in-memory native format before being accessed directly or via a Record.
 * Thus, a {@link #navigateTo(int)} call is required before accessing memory
 * that belongs to a page frame.
 * <p>
 * Decoded Parquet frames live in a per-cursor LRU capped by total retained
 * bytes ({@code cairo.sql.parquet.cache.memory.size}). When a miss arrives
 * and {@code cachedBytes} is already at or above the budget,
 * {@link #acquireBuffer} reuses the LRU oldest unpinned
 * {@link ParquetBuffers} in place: it retains the tracker-aware data/aux
 * vectors so the upcoming decode overwrites them via the Rust
 * {@code ColumnChunkBuffers::reset()} path, growing each vector via realloc
 * only when the new chunk exceeds the buffer's historical peak. A
 * tracker-bound decode drops system-allocated VarcharSlice page buffers
 * before returning their explicit charge; untracked decodes may reuse those
 * buffers across columns. Because the cache retains its data/aux peak, each entry is
 * accounted at {@code retainedBytes} - the largest footprint it has held, where
 * the footprint sums every decode slot's peak data and aux bytes, since a
 * late-materialized re-decode can map other columns to the same slots - and
 * after every decode {@link #trimToBudget} closes LRU-oldest unpinned
 * entries until the total drops back under the budget. Entries currently
 * bound to a record or to the frame-memory flyweight are skipped during
 * victim selection and trimming, so when every cached entry is pinned the
 * pool creates a new buffer and the budget is temporarily exceeded.
 * <p>
 * A cached entry that cannot serve a request for its frame (a partial row window,
 * or a late-materialized column subset) is re-decoded, in place only while the
 * requesting holder is its sole pin; otherwise the entry stays with its other
 * holders and the request decodes into a fresh buffer (see {@link #prepareRedecode}).
 * <p>
 * The access-pattern hint declared by the enclosing factory scales the
 * effective ceiling: {@link ParquetDecodeHint#MONOTONIC} cursors get a quarter
 * of the configured budget, {@link ParquetDecodeHint#SCATTERED} cursors get all
 * of it. Hints come in via {@link #of(PageFrameAddressCache, ParquetDecodeHint)}
 * or {@link #setParquetDecodeHint(ParquetDecodeHint)} and default to MONOTONIC.
 * <p>
 * An owner that reads only some columns of every Parquet frame binds them as the pool's
 * projection, see {@link #of(PageFrameAddressCache, ParquetDecodeHint, IntHashSet)}, and the pool
 * then decodes only those columns, so the cache budget holds more frames.
 * <p>
 * A frame that a covering index serves for a single key reports NATIVE, but its columns are
 * decoded from the index into this pool's buffers, which live as a decoded Parquet frame's do:
 * while the frame memory or a record is bound to them, or while the same cache budget has room for
 * them, until {@link #releaseParquetBuffers()} or a query boundary frees them. A record over such a
 * frame is not stable, see {@link PageFrameMemory#isStable()}. So a pool holds the covered frames
 * its readers are on, not every frame it has decoded, and a pool of a shared queue's task keeps
 * none past the frame it carries.
 * <p>
 * This pool is thread-unsafe as it may hold navigated Parquet partition data,
 * so it shouldn't be shared between multiple threads.
 */
public class PageFrameMemoryPool implements RecordRandomAccess, QuietCloseable, Mutable {
    private static final byte FRAME_MEMORY_MASK = 1 << 2;
    // a covered decode that binds no holder, see acquireCoveringBuffers()
    private static final byte NO_HOLDER = 0;
    private static final byte RECORD_A_MASK = 1;
    private static final byte RECORD_B_MASK = 1 << 1;
    // values of a compressed covered run decoded at once, see CoveringBuffers.copyRun()
    private static final int RUN_DECODE_CHUNK = 4096;
    // CoveringBuffers shells kept at most, see evictCovered()
    private static final int COVERING_SHELL_POOL_CAP = 512;
    private static final int SHELL_POOL_CAP = 256;
    // O(1) frameIndex lookup. LRU order is tracked separately via the
    // intrusive lruHead/lruTail doubly linked list through ParquetBuffers.
    private final IntObjHashMap<ParquetBuffers> byFrameIndex;
    // Maps column ID (field_id / writer index) to parquet column index.
    // Rebuilt each time openParquet() encounters a new file.
    private final IntIntHashMap columnIdToParquetIdx;
    private final CairoConfiguration configuration;
    // The covered (posting-index sidecar) frames this pool has decoded, keyed by frame index, in LRU
    // order through coveredLruHead/coveredLruTail. A covered frame is a decoded frame, as a Parquet
    // frame is, and its buffers live as a Parquet frame's do: while the frame memory or a record is
    // bound to them, or unbound while the cache budget has room for them; releaseParquetBuffers()
    // frees them all, and so do the query boundaries (of, clear, close). A record bound to them is
    // not stable (see PageFrameMemory.isStable()), so a reader that keeps a value past the frame,
    // such as first() over a VARCHAR, copies it. Only a frame decoded ahead of a fold outlives
    // releaseParquetBuffers(), until the fold releases it, see decodeAhead().
    private final IntObjHashMap<CoveringBuffers> coveringByFrame = new IntObjHashMap<>();
    // the covered buffers the frame memory and the records are bound to, each pinning its buffers
    private CoveringBuffers coveredBoundForFrameMemory;
    private CoveringBuffers coveredBoundForRecordA;
    private CoveringBuffers coveredBoundForRecordB;
    private CoveringBuffers coveredLruHead;
    private CoveringBuffers coveredLruTail;
    // scratch of RUN_DECODE_CHUNK longs for the range decode of compressed covered runs, see
    // CoveringBuffers.copyRun(); 0 until first used, freed with the covered decode buffers
    private long runDecodeWorkspace;
    // Each selected include count gets one scratch array. The pool is thread-unsafe,
    // so detached covered-cursor decodes can reuse these arrays without per-frame GC.
    private final ObjList<int[]> coveringIncludeIndexScratch = new ObjList<>();
    private final IntList declaredFrameRowCounts = new IntList(16);
    // Released CoveringBuffers, their column buffers freed and their metadata arrays kept, reused by
    // the next frames' decodes, also of later queries; at most COVERING_SHELL_POOL_CAP, freed by
    // close(). A pool that keeps shells keeps their address lists too, see setRetainsCoveringShells();
    // any other parks them, closed, so that the pool keeps no native memory past a frame.
    private final ObjList<CoveringBuffers> freeCoveringShells = new ObjList<>();
    private final PageFrameMemoryImpl frameMemory;
    // Bounded LIFO of closed ParquetBuffers shells, reused by acquireBuffer on the
    // async-parquet per-frame release path so the wrapper object doesn't churn.
    private final ObjList<ParquetBuffers> freeParquetBufferShells = new ObjList<>();
    private final ParquetFileDecoder legacyDecoder;
    private final long maxCacheBytes;
    // Contains [parquet_column_index, column_type] pairs.
    // Each parquet column appears at most once even when multiple query
    // columns reference it (a SelectedRecord projection can list the same
    // base column twice). decode() iterates the query column mapping and
    // looks up the slot via parquetIdxToDecodeSlot.
    private final DirectIntList parquetColumns;
    // Maps parquet column index to its slot in parquetColumns / decoded
    // buffers. -1 when the parquet column is not part of the current
    // decode pass (excluded from the include/exclude filter, or absent
    // from the parquet file because it was added later).
    private final IntIntHashMap parquetIdxToDecodeSlot;
    private final IntList queryToSlot = new IntList(16);
    private final IntLongHashMap recordAtSlices = new IntLongHashMap();
    // test seam: covered frames decoded for their declared rows alone
    private static long coveredRowDecodeCount;
    // test seam: fills covered decode buffers with POISON_BYTE before they are freed, see
    // setPoisoningFreedCoveredBuffers()
    private static volatile boolean isPoisoningFreedCoveredBuffers;
    private static final int POISON_BYTE = 0xA5;
    // a covered frame's declared rows, see decodeCoveredFrameForRecord()
    private DirectLongList coveredRowScratch;
    // Per-column source type tag for fixed-to-var type-cast columns.
    // Indexed by query column index; -1 means no type cast.
    private final IntList sourceColumnTypes;
    private ParquetDecoder activeDecoder;
    private PageFrameAddressCache addressCache;
    // Bumped whenever the pool closes or repurposes buffers that bound records may
    // still alias (failed decode, eviction, in-place reuse for another frame, bulk
    // release). Records capture it on bind; a mismatch fails the navigateTo() fast
    // path and forces a safe rebind. A record bound through init(PageFrameMemory)
    // aliases the frame memory's buffer without pinning it, so once the frame memory
    // moves on, the LRU may hand that buffer to another frame; only this bump stops
    // the record from reading the other frame's rows under its old frame index.
    private long bindGeneration;
    // Tracks which cached buffer currently holds each usage bit. Used to clear
    // the previous pin in O(1) without scanning every cached entry.
    private ParquetBuffers boundForFrameMemory;
    private ParquetBuffers boundForRecordA;
    private ParquetBuffers boundForRecordB;
    private long cachedBytes;
    // Live native bytes of the covered decode buffers this pool holds, the sum of every
    // CoveringBuffers' allocatedBytes. With cachedBytes, it is what the cache budget bounds, see
    // trimCoveredToBudget().
    //
    // Covered allocations charge the per-query MemoryTracker (MemoryTag.NATIVE_INDEX_READER), so
    // the query memory limit sees and caps covered decode, and the free releases the charge, so
    // that the limit sees the covered frames the query holds, not those it has decoded. A buffer
    // freed after its query's tracker has been reconciled frees on global-only accounting, see
    // CoveringBuffers.chargedTracker().
    private long coveredCachedBytes;
    private ParquetDecodeHint decodeHint = ParquetDecodeHint.MONOTONIC;
    private long effectiveBudgetBytes;
    // True while parquetColumns/queryToSlot hold the mapping of the pool's
    // projection, every column without one, for the active decoder's file,
    // letting openParquet(int) skip the rebuild on every subsequent frame of
    // the same file. The projection stays fixed between of() calls, and of()
    // clears this flag, so the memoized mapping always matches it.
    private boolean hasFullProjectionMap;
    private boolean hasTypeCasts;
    // whether released covered decode buffers are kept as shells, see setRetainsCoveringShells()
    private boolean isRetainingCoveringShells;
    private ParquetBuffers lruHead;
    private ParquetBuffers lruTail;
    // Per-query tracker propagated to each ParquetBuffers' RowGroupBuffers when
    // it is reopened, so decoded parquet column data charges the owning
    // workload's limit. Null leaves decode buffers on global-only accounting
    // (e.g. context-less worker tasks and protocol-layer streaming pools).
    private MemoryTracker memoryTracker;
    // Lazily created list of zero entries published as column addresses/sizes for
    // an empty decode window; a zero address reads as a column top (NULL).
    private DirectLongList nullColumnAddresses;
    // Created lazily on the first parquet frame so the configuration's decoder factory is fully wired.
    private ParquetPartitionDecoder parquetMetaDecoder;
    // The query columns that a Parquet frame resolves and decodes, or null for every column; see
    // of(PageFrameAddressCache, ParquetDecodeHint, IntHashSet). Borrowed from the owner, never changed.
    @Nullable
    private IntHashSet projection;
    private DirectLongList recordAtRows;

    public PageFrameMemoryPool(CairoConfiguration configuration, long maxCacheBytes) {
        try {
            this.maxCacheBytes = Math.max(maxCacheBytes, 0L);
            this.effectiveBudgetBytes = decodeHint.applyTo(this.maxCacheBytes);
            // Passing the MONOTONIC cap floors to the map's 16-slot minimum; SCATTERED
            // cursors rehash up on demand rather than every pool (incl. all-native
            // scans) paying for 256 slots.
            byFrameIndex = new IntObjHashMap<>(ParquetDecodeHint.MONOTONIC.maxCachedBuffers);
            columnIdToParquetIdx = new IntIntHashMap(16);
            frameMemory = new PageFrameMemoryImpl();
            parquetColumns = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT, true);
            this.configuration = configuration;
            parquetIdxToDecodeSlot = new IntIntHashMap(16);
            legacyDecoder = new ParquetFileDecoder();
            sourceColumnTypes = new IntList();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public PageFrameMemoryPool(CairoConfiguration configuration) {
        this(configuration, configuration.getSqlParquetCacheMemorySize());
    }

    @Override
    public void clear() {
        releaseParquetBuffers();
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
        Misc.free(parquetColumns);
        nullColumnAddresses = Misc.free(nullColumnAddresses);
        recordAtRows = Misc.free(recordAtRows);
        recordAtSlices.clear();
        releaseCoveringBuffers();
        memoryTracker = null;
        projection = null;
    }

    @Override
    public void close() {
        releaseParquetBuffers();
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
        Misc.free(parquetColumns);
        nullColumnAddresses = Misc.free(nullColumnAddresses);
        recordAtRows = Misc.free(recordAtRows);
        coveredRowScratch = Misc.free(coveredRowScratch);
        recordAtSlices.clear();
        releaseCoveringBuffers();
        Misc.freeObjListAndClear(freeParquetBufferShells);
        Misc.freeObjListAndClear(freeCoveringShells);
        addressCache = null;
        memoryTracker = null;
        projection = null;
    }

    public long getBindGeneration() {
        return bindGeneration;
    }

    @TestOnly
    public static long getCoveredRowDecodeCount() {
        return coveredRowDecodeCount;
    }

    /**
     * Makes every pool fill a covered frame's decode buffers with a byte pattern before it frees
     * them, so that a reader that kept a pointer into a released covered frame reads the pattern
     * rather than the stale values, which a freed block still holds until it is reused, and a
     * result that a test compares with a reference query comes out wrong.
     */
    @TestOnly
    public static void setPoisoningFreedCoveredBuffers(boolean isPoisoning) {
        isPoisoningFreedCoveredBuffers = isPoisoning;
    }

    @TestOnly
    public long getCachedBytes() {
        return cachedBytes;
    }

    @TestOnly
    public int getCachedFrameCount() {
        return byFrameIndex.size();
    }

    @TestOnly
    public long getCoveredCachedBytes() {
        return coveredCachedBytes;
    }

    @TestOnly
    public int getCoveredFrameCount() {
        return coveringByFrame.size();
    }

    @TestOnly
    public int getCoveringShellCount() {
        return freeCoveringShells.size();
    }

    @TestOnly
    public ParquetDecodeHint getDecodeHint() {
        return decodeHint;
    }

    @TestOnly
    public long getEffectiveBudgetBytes() {
        return effectiveBudgetBytes;
    }

    @TestOnly
    @Nullable
    public IntHashSet getProjection() {
        return projection;
    }

    /**
     * Navigates to the given frame, potentially deserializing it to in-memory format
     * (for Parquet partitions). After this call, the input record can be used to access
     * any row within the frame.
     */
    public void navigateTo(int frameIndex, PageFrameMemoryRecord record) {
        final byte format = addressCache.getFrameFormat(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            // a deferred frame's columns are computed by the first thread that comes here
            addressCache.ensureFrameReady(frameIndex);
            // A covered frame reports NATIVE but is decoded into this pool's buffers, NOT read
            // from the query-stable address-cache arrays, so it must take the guarded bind of a
            // decoded frame -- never the frame-index-only fast-return, which would leave the
            // record pointing at buffers the pool has released or given to another frame.
            // Detect and handle it before that fast-return.
            if (navigateCoveredRecord(frameIndex, record)) {
                return;
            }
            // Native page addresses come from the address cache and stay valid for the whole
            // query, so a matching frame index proves the record is already positioned here.
            if (record.getFrameIndex() == frameIndex) {
                return;
            }
            final byte usageBit = record.getLetter() == PageFrameMemoryRecord.RECORD_A_LETTER ? RECORD_A_MASK : RECORD_B_MASK;
            unbind(usageBit);
            unbindCovered(usageBit);
            record.init(
                    frameIndex,
                    format,
                    true,
                    addressCache.getRowIdOffset(frameIndex),
                    addressCache.getPageAddresses(),
                    addressCache.getAuxPageAddresses(),
                    addressCache.getPageSizes(),
                    addressCache.getAuxPageSizes(),
                    addressCache.toColumnOffset(frameIndex),
                    addressCache.getColumnCount(),
                    false,
                    null,
                    null
            );
        } else if (format == PartitionFormat.PARQUET) {
            // Fast path: the record already points at THIS pool's live buffers for this frame,
            // so there is nothing to rebind. A matching frame index ALONE is not sufficient: a
            // foreign record bound to another pool's frame memory (e.g. a reduce task's, via
            // record.init(task.getFrameMemory())) can carry a matching frame index while that
            // pool already freed the buffers in releaseParquetBuffers() -- reading through it
            // would dereference freed memory. The boundPool identity check distinguishes "still
            // ours and live" from "bound elsewhere and possibly freed", so it restores the cheap
            // per-row repeat visit for sequential scans (PageFrameRecordCursorImpl.hasNext())
            // without reopening the parquet use-after-free on the random-access path. The
            // generation check rejects records bound before a failed decode closed buffers
            // they may still alias.
            if (record.getFrameIndex() == frameIndex && record.getBoundPool() == this && record.getBoundGeneration() == bindGeneration) {
                return;
            }
            // openParquet() rebuilds parquetColumns / parquetIdxToDecodeSlot AND the pool's
            // per-frame lazy-conversion metadata (sourceColumnTypes / hasTypeCasts). record.init()
            // below reads that metadata, so openParquet() must run on EVERY navigation: the pool's
            // sourceColumnTypes is shared and a navigation to another file overwrites it, so a
            // still-cached frame would otherwise hand the record a stale mapping and read a
            // converted column with the wrong source type. activateDecoder() inside openParquet()
            // clears hasFullProjectionMap on a file switch and forces the rebuild; on a same-file
            // repeat visit the rebuild is skipped but the still-valid mapping is reused. Only the
            // expensive decode() stays gated on the buffer cache miss / partial window.
            final byte usageBit = record.getLetter() == PageFrameMemoryRecord.RECORD_A_LETTER ? RECORD_A_MASK : RECORD_B_MASK;
            assert isCoveredUnbound(usageBit);
            ParquetBuffers parquetBuffers = tryHit(frameIndex, usageBit);
            final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
            final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
            if (parquetBuffers == null) {
                try {
                    openParquet(frameIndex);
                    parquetBuffers = acquireBuffer(frameIndex, usageBit);
                    final long slice = recordAtSlices.get(frameIndex);
                    if (shouldDecodeRowFiltered(frameIndex, slice)) {
                        decodeRowFilteredAndAccount(frameIndex, parquetBuffers, slice);
                    } else {
                        decodeAndAccount(frameIndex, parquetBuffers);
                    }
                } catch (Throwable th) {
                    // tryHit() unpinned the record's prior buffer, so any failure here leaves
                    // it evictable. Drop the stale binding so the fast path can't read freed
                    // memory; the next navigateTo() re-resolves a live entry or re-decodes.
                    record.clear();
                    throw th;
                }
            } else if (
                    parquetBuffers.decodedRowLo > rowGroupLo
                            || parquetBuffers.decodedRowHi < rowGroupHi
                            || (parquetBuffers.columnSubset != null && (!parquetBuffers.isPopulated || parquetBuffers.isCompacted))
            ) {
                // A record reads arbitrary rows of every column, so it re-decodes the full frame:
                // - a clamped (partial-window) buffer left by a LIMIT scan;
                // - a late-materialized decode whose remaining columns are not populated yet, or
                //   are populated in compacted (filtered-row) order.
                // A late-materialized decode populated in row order (fillWithNulls) is reused: Top-K's
                // recordB reads only the filtered rows of the current frame through it, and a
                // re-decode would move the addresses the frame memory's record aliases.
                try {
                    parquetBuffers = prepareRedecode(frameIndex, parquetBuffers, usageBit);
                    openParquet(frameIndex);
                    decodeAndAccount(frameIndex, parquetBuffers);
                } catch (Throwable th) {
                    record.clear();
                    throw th;
                }
            } else {
                // Full cache hit, no decode needed, but the column mapping / conversion
                // metadata must still be refreshed for record.init() below. tryHit()
                // already unpinned the record's prior buffer, so on failure clear the
                // binding (matching the decode branches above) to keep it evictable and
                // stop the fast path reading freed memory.
                try {
                    openParquet(frameIndex);
                } catch (Throwable th) {
                    record.clear();
                    throw th;
                }
            }
            record.init(
                    frameIndex,
                    format,
                    false,
                    addressCache.getRowIdOffset(frameIndex),
                    parquetBuffers.pageAddresses,
                    parquetBuffers.auxPageAddresses,
                    parquetBuffers.pageSizes,
                    parquetBuffers.auxPageSizes,
                    0, // parquet buffers use 0 offset since they're frame-specific
                    addressCache.getColumnCount(),
                    hasTypeCasts,
                    sourceColumnTypes,
                    parquetBuffers.columnTops
            );
            record.setBoundPool(this, bindGeneration);
        }
    }

    /**
     * Navigates to the given frame, potentially deserializing it to in-memory format
     * (for Parquet partitions). The returned PageFrameMemory object is a flyweight,
     * so it should be used immediately once returned. This method is useful for later
     * calls to native code.
     * <p>
     * If you need data access via {@link Record} API, use the
     * {@link #navigateTo(int, PageFrameMemoryRecord)} method.
     */
    public PageFrameMemory navigateTo(int frameIndex) {
        return navigateTo(frameIndex, 0, Integer.MAX_VALUE);
    }

    /**
     * Convenience overload of {@link #navigateTo(int, int, int)} with the window
     * starting at frame row 0, i.e. {@code [0, inFrameRowHi)}.
     */
    public PageFrameMemory navigateTo(int frameIndex, int inFrameRowHi) {
        return navigateTo(frameIndex, 0, inFrameRowHi);
    }

    /**
     * Navigates to the given frame with a window of frame rows the caller
     * intends to access. For Parquet partitions, only rows in
     * {@code [inFrameRowLo, inFrameRowHi)} of the frame are decoded; for native
     * partitions the window is ignored. The window is a hard contract on the
     * caller: rows outside it are undecoded memory and must not be accessed
     * via the returned memory.
     * <p>
     * Decoded buffers stay frame-origin-addressable: published column addresses are
     * shifted back by {@code inFrameRowLo} rows, so records and row cursors keep
     * using absolute frame-relative row indexes. The pool tracks the decoded window
     * of each cached buffer and transparently re-decodes a wider window when a
     * later call for the same frame requires one.
     * <p>
     * Invariant required by the record-bound fast path in
     * {@link #navigateTo(int, PageFrameMemoryRecord)}: a record bound to a partial
     * window (via {@link PageFrameMemoryRecord#init(PageFrameMemory)}) must never be
     * asked, through that fast path, for a row outside the window it was bound with.
     * The fast path re-points without checking coverage, so a wider later access
     * would read undecoded memory. The sole finite-window originator is
     * {@code LimitRecordCursor}, whose window only shrinks across a scan, so no bound
     * record ever needs widening; a new finite-window caller must uphold this.
     */
    public PageFrameMemory navigateTo(int frameIndex, int inFrameRowLo, int inFrameRowHi) {
        if (frameMemory.frameIndex == frameIndex && isFrameMemoryCovering(frameIndex, inFrameRowLo, inFrameRowHi)) {
            return frameMemory;
        }

        final byte format = addressCache.getFrameFormat(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            addressCache.ensureFrameReady(frameIndex);
            unbind(FRAME_MEMORY_MASK);
            frameMemory.pageAddresses = addressCache.getPageAddresses();
            frameMemory.auxPageAddresses = addressCache.getAuxPageAddresses();
            frameMemory.pageSizes = addressCache.getPageSizes();
            frameMemory.auxPageSizes = addressCache.getAuxPageSizes();
            frameMemory.columnOffset = addressCache.toColumnOffset(frameIndex);
            frameMemory.currentRowGroupBuffer = null;
            // Covered frame: decode its covered columns on this worker and rebind
            // the frame memory to the decoded buffers (overriding the eager flat
            // addresses just rebound above). A covered frame always reports
            // NATIVE, so it always lands here.
            try {
                patchCoveredFrameMemory(frameIndex);
            } catch (Throwable th) {
                // Mirror the PARQUET arm: drop frameMemory's stale binding so a
                // subsequent fast-return does not serve the half-bound state.
                unbindCovered(FRAME_MEMORY_MASK);
                frameMemory.clear();
                throw th;
            }
        } else if (format == PartitionFormat.PARQUET) {
            assert isCoveredUnbound(FRAME_MEMORY_MASK);
            final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
            final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
            final int decodeLo = (int) Math.min(rowGroupHi, rowGroupLo + (long) inFrameRowLo);
            final int decodeHi = (int) Math.min(rowGroupHi, rowGroupLo + (long) inFrameRowHi);
            final int frameRowLo = decodeLo - rowGroupLo;
            if (decodeLo == decodeHi) {
                // Nothing will be read from this frame (e.g. skipRows with a zero
                // post-skip cap); publish NULL addresses instead of decoding.
                unbind(FRAME_MEMORY_MASK);
                final DirectLongList zeroes = getNullColumnAddresses();
                frameMemory.currentRowGroupBuffer = null;
                frameMemory.pageAddresses = zeroes;
                frameMemory.auxPageAddresses = zeroes;
                frameMemory.pageSizes = zeroes;
                frameMemory.auxPageSizes = zeroes;
                frameMemory.columnOffset = 0;
                frameMemory.decodedColumnIndexes = null;
                frameMemory.frameIndex = frameIndex;
                frameMemory.frameFormat = format;
                return frameMemory;
            }
            ParquetBuffers parquetBuffers = tryHit(frameIndex, FRAME_MEMORY_MASK);
            if (parquetBuffers != null && parquetBuffers.isRowFiltered) {
                // A row-filtered buffer holds NULLs for undeclared rows and must not
                // serve full-frame access.
                unbind(FRAME_MEMORY_MASK);
                evictRowFiltered(parquetBuffers);
                parquetBuffers = null;
            }
            if (parquetBuffers == null) {
                try {
                    openParquet(frameIndex);
                    parquetBuffers = acquireBuffer(frameIndex, FRAME_MEMORY_MASK);
                    decodeWindowAndAccount(frameIndex, parquetBuffers, decodeLo, decodeHi, frameRowLo);
                } catch (Throwable th) {
                    // Same hazard as the record fast path; drop frameMemory's stale binding.
                    frameMemory.clear();
                    throw th;
                }
            } else if (parquetBuffers.columnSubset != null || parquetBuffers.decodedRowLo > decodeLo || parquetBuffers.decodedRowHi < decodeHi) {
                // Cached window doesn't cover the request, or a late-materialized decode left
                // the columns outside its subset undecoded (or populated for the filtered rows
                // only); re-decode every column, in place unless another holder pins the buffer.
                try {
                    parquetBuffers = prepareRedecode(frameIndex, parquetBuffers, FRAME_MEMORY_MASK);
                    openParquet(frameIndex);
                    decodeWindowAndAccount(frameIndex, parquetBuffers, decodeLo, decodeHi, frameRowLo);
                } catch (Throwable th) {
                    frameMemory.clear();
                    throw th;
                }
            } else {
                // Full cache hit, no decode needed, but the lazy-conversion metadata
                // (the pool's hasTypeCasts / sourceColumnTypes, surfaced through
                // PageFrameMemoryImpl.hasColumnTypeCasts() / getSourceColumnType()) still
                // reflects whichever frame openParquet() last ran for. A later
                // record.init(frameMemory) reads that metadata, so it must be rebuilt for
                // THIS frame or the record inherits another frame's mapping and reads a
                // converted column with the wrong source type. Mirrors the cache-hit refresh
                // in navigateTo(int, PageFrameMemoryRecord).
                try {
                    openParquet(frameIndex);
                } catch (Throwable th) {
                    frameMemory.clear();
                    throw th;
                }
            }
            frameMemory.currentRowGroupBuffer = parquetBuffers;
            frameMemory.pageAddresses = parquetBuffers.pageAddresses;
            frameMemory.auxPageAddresses = parquetBuffers.auxPageAddresses;
            frameMemory.pageSizes = parquetBuffers.pageSizes;
            frameMemory.auxPageSizes = parquetBuffers.auxPageSizes;
            frameMemory.columnTops = parquetBuffers.columnTops;
            frameMemory.columnOffset = 0; // parquet buffers use 0 offset
        }

        frameMemory.decodedColumnIndexes = null;
        frameMemory.frameIndex = frameIndex;
        frameMemory.frameFormat = format;
        return frameMemory;
    }

    public PageFrameMemory navigateTo(int frameIndex, IntHashSet columnIndexes) {
        // Only the async reduce pools call this overload, once per frame, but a matching frame
        // index alone does not prove the binding serves this request: the frame may have been
        // decoded for another column subset, for a partial row window by the windowed overload,
        // or populated by populateRemainingColumns() for an earlier caller's filtered rows.
        if (frameMemory.frameIndex == frameIndex && isFrameMemoryCovering(frameIndex, columnIndexes)) {
            if (frameMemory.frameFormat == PartitionFormat.PARQUET) {
                // the buffer may be a superset decode: judge column tops over what it decoded
                frameMemory.decodedColumnIndexes = frameMemory.currentRowGroupBuffer.columnSubset;
            }
            return frameMemory;
        }

        final byte format = addressCache.getFrameFormat(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            addressCache.ensureFrameReady(frameIndex);
            unbind(FRAME_MEMORY_MASK);
            frameMemory.pageAddresses = addressCache.getPageAddresses();
            frameMemory.auxPageAddresses = addressCache.getAuxPageAddresses();
            frameMemory.pageSizes = addressCache.getPageSizes();
            frameMemory.auxPageSizes = addressCache.getAuxPageSizes();
            frameMemory.columnOffset = addressCache.toColumnOffset(frameIndex);
            frameMemory.currentRowGroupBuffer = null;
            // Covered frame: decode only the requested columns on the first filter pass.
            try {
                patchCoveredFrameMemory(frameIndex, columnIndexes);
            } catch (Throwable th) {
                // Mirror the PARQUET arm: drop frameMemory's stale binding on failure.
                unbindCovered(FRAME_MEMORY_MASK);
                frameMemory.clear();
                throw th;
            }
        } else if (format == PartitionFormat.PARQUET) {
            assert isCoveredUnbound(FRAME_MEMORY_MASK);
            ParquetBuffers parquetBuffers = tryHit(frameIndex, FRAME_MEMORY_MASK);
            if (parquetBuffers != null && parquetBuffers.isRowFiltered) {
                // A row-filtered buffer holds NULLs for undeclared rows and must not
                // serve full-frame access.
                unbind(FRAME_MEMORY_MASK);
                evictRowFiltered(parquetBuffers);
                parquetBuffers = null;
            }
            if (parquetBuffers == null) {
                try {
                    openParquet(frameIndex, columnIndexes, true);
                    parquetBuffers = acquireBuffer(frameIndex, FRAME_MEMORY_MASK);
                    decodeAndAccount(frameIndex, parquetBuffers);
                } catch (Throwable th) {
                    // Same hazard as the record fast path; drop frameMemory's stale binding.
                    frameMemory.clear();
                    throw th;
                }
                setColumnSubset(frameIndex, parquetBuffers, columnIndexes);
            } else if (!servesColumnSubset(frameIndex, parquetBuffers, columnIndexes)) {
                // The cached buffer lacks a requested column, holds a partial row window left by a
                // windowed decode, or was populated for an earlier caller's filtered rows; decode the
                // requested subset over the whole frame, in place unless another holder pins it.
                try {
                    parquetBuffers = prepareRedecode(frameIndex, parquetBuffers, FRAME_MEMORY_MASK);
                    openParquet(frameIndex, columnIndexes, true);
                    decodeAndAccount(frameIndex, parquetBuffers);
                } catch (Throwable th) {
                    frameMemory.clear();
                    throw th;
                }
                setColumnSubset(frameIndex, parquetBuffers, columnIndexes);
            } else {
                // Full cache hit (a full decode, or a subset decode that holds every requested
                // column), no decode needed, but the lazy-conversion metadata (the pool's
                // hasTypeCasts / sourceColumnTypes) still reflects whichever frame openParquet()
                // last ran for. Rebuild it for THIS frame, over every column the buffer holds, so
                // a later record.init(frameMemory) does not inherit another frame's mapping.
                // Mirrors the cache-hit refresh in navigateTo(int, PageFrameMemoryRecord).
                try {
                    if (parquetBuffers.columnSubset == null) {
                        openParquet(frameIndex);
                    } else {
                        openParquet(frameIndex, parquetBuffers.columnSubset, true);
                    }
                } catch (Throwable th) {
                    frameMemory.clear();
                    throw th;
                }
            }
            frameMemory.currentRowGroupBuffer = parquetBuffers;
            frameMemory.pageAddresses = parquetBuffers.pageAddresses;
            frameMemory.auxPageAddresses = parquetBuffers.auxPageAddresses;
            frameMemory.pageSizes = parquetBuffers.pageSizes;
            frameMemory.auxPageSizes = parquetBuffers.auxPageSizes;
            frameMemory.columnTops = parquetBuffers.columnTops;
            frameMemory.columnOffset = 0; // parquet buffers use 0 offset
        }

        // Late materialization: only the subset's columns are decoded until
        // populateRemainingColumns(), so only they can be judged for column tops.
        // A cache hit on a full-frame decode has every column; one on a superset
        // decode has the superset.
        frameMemory.decodedColumnIndexes = format == PartitionFormat.PARQUET ? frameMemory.currentRowGroupBuffer.columnSubset : null;
        frameMemory.frameIndex = frameIndex;
        frameMemory.frameFormat = format;
        return frameMemory;
    }

    /**
     * Whether column {@code columnIndex} has the same value on every row of frame
     * {@code frameIndex}: the symbol key of a single-key covered frame.
     */
    public boolean isConstantColumn(int frameIndex, int columnIndex) {
        return addressCache.isColumnCoveredKey(frameIndex, columnIndex);
    }

    public void of(PageFrameAddressCache addressCache) {
        of(addressCache, ParquetDecodeHint.MONOTONIC);
    }

    public void of(PageFrameAddressCache addressCache, ParquetDecodeHint hint) {
        of(addressCache, hint, null);
    }

    /**
     * Binds the pool to the address cache of a query, as
     * {@link #of(PageFrameAddressCache, ParquetDecodeHint)} does, and limits what a Parquet frame
     * decodes to the query columns of the projection. Every navigation resolves and decodes only
     * those columns of a Parquet frame, with their lazy type conversions and column tops, and
     * publishes address 0 for the others: the record path, the windowed and full-frame paths, the
     * column subset of {@link #navigateTo(int, IntHashSet)} and
     * {@link PageFrameMemory#populateRemainingColumns}. A record reads a column outside the
     * projection as NULL, and {@link PageFrameMemory#hasColumnTops()} reports a column top for it.
     * Native and covered frames publish every column, as they do without a projection. A null
     * projection decodes every column, and an empty one none.
     * <p>
     * The projection stays fixed until the next {@code of()}, {@link #releaseQueryResources()},
     * {@link #clear()} or {@link #close()}, all of which drop it, so every buffer that the pool caches
     * for the query holds the same columns. The pool only reads the set: the owner keeps it unchanged
     * while any pool holds it, and may share it between pools.
     */
    public void of(PageFrameAddressCache addressCache, ParquetDecodeHint hint, @Nullable IntHashSet projection) {
        releaseParquetBuffers();
        // A new query (or the same query's next address cache) invalidates the covered
        // frames of the prior one, including those decoded ahead of a fold.
        releaseCoveringBuffers();
        this.addressCache = addressCache;
        this.decodeHint = hint;
        this.projection = projection;
        this.effectiveBudgetBytes = hint.applyTo(maxCacheBytes);
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
        hasFullProjectionMap = false;
        recordAtSlices.clear();
        Misc.clear(recordAtRows);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        navigateTo(Rows.toPartitionIndex(atRowId), frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(atRowId));
    }

    /**
     * Decodes every covered column of a single-key covered frame for a fold that reads the frame
     * later through this pool, see {@link io.questdb.cairo.sql.async.FrameDecodeAheadAtom}: the
     * pool passes to the fold's thread with the frame, which outlives {@link #releaseParquetBuffers()},
     * called by the fold's reducers after each frame, and the cache budget, until
     * {@link #releaseDecodedAhead(int)} or a query boundary releases it. Does nothing for another
     * frame. Binds neither the frame memory nor a record.
     */
    public void decodeAhead(int frameIndex) {
        if (!addressCache.isFrameSingleKeyCovered(frameIndex)) {
            return;
        }
        final CoveringBuffers buffers = acquireCoveringBuffers(frameIndex, NO_HOLDER);
        buffers.isDecodedAhead = true;
        decodeCovered(buffers, frameIndex, null);
        trimCoveredToBudget(false);
    }

    public void releaseFrameMemory() {
        unbind(FRAME_MEMORY_MASK);
        unbindCovered(FRAME_MEMORY_MASK);
        frameMemory.clear();
    }

    /**
     * Releases a frame that {@link #decodeAhead(int)} decoded, once the fold has read it: the frame
     * is freed, or, while the frame memory or a record is still bound to it, left to the cache
     * budget. Does nothing for a frame not decoded ahead.
     */
    public void releaseDecodedAhead(int frameIndex) {
        final CoveringBuffers buffers = coveringByFrame.get(frameIndex);
        if (buffers != null && buffers.isDecodedAhead) {
            buffers.isDecodedAhead = false;
            if (buffers.usageFlags == 0) {
                evictCovered(buffers);
            }
        }
    }

    /**
     * Frees every decoded frame: the Parquet buffers and the covered decode buffers, but the
     * covered frames decoded ahead of a fold (see {@link #decodeAhead(int)}), and clears
     * bookkeeping. The reduce paths call it once they are done with a frame, so that a pool,
     * also a shared queue task's, keeps no decoded frame between frames and past its query.
     * <p>
     * Bulk shutdown path: this does NOT honor the {@code usageFlags} pin
     * bits, so any {@link PageFrameMemoryRecord} or {@code frameMemory}
     * flyweight still bound to a cached entry will hold a dangling pointer
     * after this returns. Callers must ensure all records are abandoned
     * before invoking. Used from {@link #clear()} / {@link #close()} /
     * {@link #of(PageFrameAddressCache)} and from the async reduce paths that
     * release decoded frames between dispatch rounds.
     */
    public void releaseParquetBuffers() {
        ParquetBuffers b = lruHead;
        // byFrameIndex stays in lockstep with the LRU list, so an empty list means
        // an empty map: skip its O(capacity) clear on the common all-native scan.
        final boolean hadCachedBuffers = b != null;
        while (b != null) {
            ParquetBuffers next = b.next;
            b.close();
            if (freeParquetBufferShells.size() < SHELL_POOL_CAP) {
                freeParquetBufferShells.add(b);
            }
            b = next;
        }
        lruHead = null;
        lruTail = null;
        if (hadCachedBuffers) {
            byFrameIndex.clear();
        }
        boundForRecordA = null;
        boundForRecordB = null;
        boundForFrameMemory = null;
        bindGeneration++;
        cachedBytes = 0;
        if (frameMemory != null) {
            frameMemory.clear();
        }
        // A covered frame is a decoded frame too: a record over it is not stable, so no reader
        // keeps a pointer into it past the frame, see PageFrameMemory.isStable().
        releaseCoveredFrames(false);
    }

    /**
     * Releases every resource and borrowed binding owned by the current query while retaining
     * reusable container allocations for the next {@link #of(PageFrameAddressCache)} call.
     * <p>
     * Callers must first abandon any {@link PageFrameMemoryRecord} aliases into this pool. This
     * method closes the pool-local decoders while their source frame decoders are still valid, so
     * it must also run before the cursor that owns the address cache's Parquet metadata mappings is
     * closed. The address cache itself is borrowed and must be cleared separately by its owner.
     */
    public void releaseQueryResources() {
        releaseParquetBuffers();
        releaseCoveringBuffers();
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
        addressCache = null;
        memoryTracker = null;
        projection = null;
        hasFullProjectionMap = false;
        hasTypeCasts = false;
        parquetColumns.clear();
        parquetIdxToDecodeSlot.clear();
        columnIdToParquetIdx.clear();
        queryToSlot.clear();
        sourceColumnTypes.clear();
        declaredFrameRowCounts.clear();
        recordAtSlices.clear();
        Misc.clear(recordAtRows);
    }

    /**
     * Keeps the address lists of released covered decode buffers with their shells for later
     * decodes, up to a bound, see {@link #evictCovered}, and the run decode scratch between frames:
     * for a pool its owner keeps across queries and closes with itself, such as an atom's, not a
     * shared queue task's, which keeps no native memory past a frame.
     */
    public void setRetainsCoveringShells(boolean isRetainingCoveringShells) {
        this.isRetainingCoveringShells = isRetainingCoveringShells;
        if (!isRetainingCoveringShells) {
            for (int i = 0, n = freeCoveringShells.size(); i < n; i++) {
                freeCoveringShells.getQuick(i).park();
            }
        }
    }

    /**
     * Binds the per-query tracker propagated to each decode buffer on reopen.
     * Owners set it at per-query init (before the first {@link #navigateTo});
     * context-less owners leave it null for global-only accounting. A null
     * tracker is valid and matches pre-tracker behavior.
     */
    public void setMemoryTracker(MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        this.decodeHint = hint;
        this.effectiveBudgetBytes = hint.applyTo(maxCacheBytes);
        // A shrink (SCATTERED -> MONOTONIC) can leave cachedBytes above the new ceiling.
        trimToBudget();
    }

    public void setRecordAtRows(@Nullable RecordCursor.RowIdSource source) {
        evictRowFilteredBuffers();
        recordAtSlices.clear();
        if (recordAtRows != null) {
            recordAtRows.clear();
        }
        if (source == null || !addressCache.hasParquetFrames() && !addressCache.hasCoveredFrames()) {
            return;
        }
        if (recordAtRows == null) {
            recordAtRows = new DirectLongList(256, MemoryTag.NATIVE_DEFAULT);
        }
        source.copyRowFilterableRowIdsTo(recordAtRows, addressCache);
        final long kept = recordAtRows.size();
        if (kept == 0 || kept > Integer.MAX_VALUE) {
            recordAtRows = Misc.free(recordAtRows);
            return;
        }
        // First pass: per-frame declared-row counts plus a sortedness probe. When no
        // frame is sparse enough for row-filtered decode (e.g. an unfiltered sort
        // declares every row of every frame), bail out before paying for the sort.
        final int frameCount = addressCache.getFrameCount();
        declaredFrameRowCounts.setAll(frameCount, 0);
        boolean isSorted = true;
        long prevRowId = recordAtRows.get(0);
        for (long i = 0; i < kept; i++) {
            final long rowId = recordAtRows.get(i);
            isSorted &= Long.compareUnsigned(prevRowId, rowId) <= 0;
            prevRowId = rowId;
            declaredFrameRowCounts.increment(Rows.toPartitionIndex(rowId));
        }
        boolean hasEligibleFrame = false;
        for (int f = 0; f < frameCount; f++) {
            final int declared = declaredFrameRowCounts.getQuick(f);
            if (declared > 0 && isRowFilterEligible(f, declared)) {
                hasEligibleFrame = true;
                break;
            }
        }
        if (!hasEligibleFrame) {
            recordAtRows = Misc.free(recordAtRows);
            return;
        }
        if (!isSorted) {
            Vect.sortULongAscInPlace(recordAtRows.getAddress(), kept);
        }
        // Strip the frame bits in place (each frame's segment stays ascending within
        // the frame) and index the segments, so a decode can hand its segment straight
        // to the decoder without a local-row scratch copy.
        int runStart = 0;
        int runFrame = -1;
        for (int i = 0, n = (int) kept; i < n; i++) {
            final long rowId = recordAtRows.get(i);
            final int frameIndex = Rows.toPartitionIndex(rowId);
            if (frameIndex != runFrame) {
                if (runFrame >= 0) {
                    recordAtSlices.put(runFrame, Numbers.encodeLowHighInts(runStart, i));
                }
                runFrame = frameIndex;
                runStart = i;
            }
            recordAtRows.set(i, Rows.toLocalRowID(rowId));
        }
        recordAtSlices.put(runFrame, Numbers.encodeLowHighInts(runStart, (int) kept));
    }

    // Content, not identity: callers pass factory-lifetime sets, but an equal set built elsewhere
    // (or a narrower one) must still be served by a decode that holds every column it names.
    private static boolean containsAll(IntHashSet superset, IntHashSet subset) {
        if (superset == subset) {
            return true;
        }
        if (superset.size() < subset.size()) {
            return false;
        }
        for (int i = 0, n = subset.size(); i < n; i++) {
            if (!superset.contains(subset.get(i))) {
                return false;
            }
        }
        return true;
    }

    private void accountDecode(ParquetBuffers parquetBuffers) {
        if (parquetBuffers.decodedBytes > parquetBuffers.retainedBytes) {
            cachedBytes += parquetBuffers.decodedBytes - parquetBuffers.retainedBytes;
            parquetBuffers.retainedBytes = parquetBuffers.decodedBytes;
        }
        trimToBudget();
    }

    private ParquetBuffers acquireBuffer(int frameIndex, byte usageBit) {
        assert getBound(usageBit) == null : "acquireBuffer requires the prior pin to have been cleared by tryHit";
        if (cachedBytes >= effectiveBudgetBytes || byFrameIndex.size() >= maxCachedBuffers()) {
            for (ParquetBuffers victim = lruHead; victim != null; victim = victim.next) {
                if (victim.usageFlags != 0) {
                    continue;
                }
                // In-place reuse keeps the victim's native memory (and thus its
                // retainedBytes accounting); only the logical state resets.
                if (victim.frameIndex >= 0) {
                    // prepareRedecode() detaches a buffer by unmapping it (frameIndex -1)
                    byFrameIndex.remove(victim.frameIndex);
                }
                lruUnlink(victim);
                // The victim is unpinned, but a record bound through init(PageFrameMemory)
                // may still alias it under its old frame index.
                bindGeneration++;
                victim.frameIndex = frameIndex;
                victim.usageFlags = usageBit;
                victim.decodedBytes = 0;
                victim.decodedRowHi = -1;
                victim.decodedRowLo = -1;
                victim.slotCount = 0;
                victim.isRowFiltered = false;
                victim.columnSubset = null;
                victim.isPopulated = false;
                victim.isCompacted = false;
                lruAppend(victim);
                byFrameIndex.put(frameIndex, victim);
                setBound(usageBit, victim);
                return victim;
            }
        }
        final ParquetBuffers buffers;
        final int shellCount = freeParquetBufferShells.size();
        if (shellCount > 0) {
            buffers = freeParquetBufferShells.getQuick(shellCount - 1);
            freeParquetBufferShells.remove(shellCount - 1);
        } else {
            buffers = new ParquetBuffers();
        }
        // reopen() binds the pool's per-query tracker, then (re)allocates the
        // native buffers one by one. A fresh ParquetBuffers defers its
        // RowGroupBuffers allocation to here (keepClosed ctor) so the decoded
        // column data charges the per-query limit instead of the global counter;
        // a reused shell re-allocates everything it freed when it was parked.
        // Either way the buffers is not yet tracked in lruHead/byFrameIndex, so a
        // partial reopen would orphan it: free what reopen() managed to allocate
        // and discard it.
        try {
            buffers.reopen();
        } catch (Throwable th) {
            buffers.close();
            throw th;
        }
        buffers.frameIndex = frameIndex;
        buffers.usageFlags = usageBit;
        lruAppend(buffers);
        byFrameIndex.put(frameIndex, buffers);
        setBound(usageBit, buffers);
        return buffers;
    }

    private void activateDecoder(int frameIndex) {
        final ParquetDecoder frameDecoder = addressCache.getParquetDecoder(frameIndex);
        if (frameDecoder instanceof ParquetPartitionDecoder parquetMetaFrame) {
            if (parquetMetaDecoder == null) {
                // Created lazily so the configuration's decoder factory is fully wired before first use.
                parquetMetaDecoder = configuration.newParquetPartitionDecoder();
            }
            if (parquetMetaDecoder.getParquetMetaAddr() != parquetMetaFrame.getParquetMetaAddr() || parquetMetaDecoder.getParquetMetaSize() != parquetMetaFrame.getParquetMetaSize()) {
                parquetMetaDecoder.of(parquetMetaFrame);
                buildColumnIdMap(parquetMetaDecoder);
            }
            if (activeDecoder != parquetMetaDecoder) {
                hasFullProjectionMap = false;
                activeDecoder = parquetMetaDecoder;
            }
        } else {
            ParquetFileDecoder legacyFrame = (ParquetFileDecoder) frameDecoder;
            if (legacyDecoder.getFileAddr() != legacyFrame.getFileAddr() || legacyDecoder.getFileSize() != legacyFrame.getFileSize()) {
                legacyDecoder.of(legacyFrame);
                buildColumnIdMap(legacyDecoder);
            }
            if (activeDecoder != legacyDecoder) {
                hasFullProjectionMap = false;
                activeDecoder = legacyDecoder;
            }
        }
    }

    // Returns the decode slot the parquet column maps to, adding a new slot
    // (and its [parquetIdx, decodeType] entry in parquetColumns) on first sight.
    // A repeated parquet column reuses the slot recorded by the first caller, so
    // its decodeType wins; resolveParquetColumn() relies on this de-duplication.
    private int addDecodeSlotIfAbsent(int parquetIdx, int decodeType) {
        final int slotKey = parquetIdxToDecodeSlot.keyIndex(parquetIdx);
        final int existingSlot = parquetIdxToDecodeSlot.valueAt(slotKey);
        if (existingSlot >= 0) {
            return existingSlot;
        }
        final int slot = (int) (parquetColumns.size() / 2);
        parquetIdxToDecodeSlot.putAt(slotKey, parquetIdx, slot);
        parquetColumns.add(parquetIdx);
        parquetColumns.add(decodeType);
        return slot;
    }

    private void buildColumnIdMap(ParquetDecoder decoder) {
        final int parquetColumnCount = decoder.getColumnCount();
        columnIdToParquetIdx.clear();
        hasFullProjectionMap = false;
        for (int i = 0; i < parquetColumnCount; i++) {
            final int id = decoder.getColumnId(i);
            // External parquet files may not have field IDs (all -1).
            // Fall back to positional index so the lookup in openParquet() works.
            columnIdToParquetIdx.put(id < 0 ? i : id, i);
        }
    }

    private void decodeAndAccount(int frameIndex, ParquetBuffers parquetBuffers) {
        final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
        final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
        decodeWindowAndAccount(frameIndex, parquetBuffers, rowGroupLo, rowGroupHi, 0);
    }

    /**
     * Decodes the covered columns of a single-key covered frame into this pool's buffers for the
     * frame, binds the {@code usageBit} holder to them and returns them; returns {@code null} for a
     * non-covered frame or a multi-key (merged) covered frame, which cannot be reproduced from a
     * single detached cursor and stays on the eager flat addresses. Shared by the flyweight
     * ({@link #patchCoveredFrameMemory}) and record ({@link #navigateCoveredRecord}) arms so both
     * decode identically.
     */
    @Nullable
    private CoveringBuffers decodeCoveredFrame(int frameIndex, @Nullable IntHashSet columnIndexes, byte usageBit) {
        if (!addressCache.isFrameSingleKeyCovered(frameIndex)) {
            return null;
        }
        final CoveringBuffers buffers = acquireCoveringBuffers(frameIndex, usageBit);
        decodeCovered(buffers, frameIndex, columnIndexes);
        trimCoveredToBudget(false);
        return buffers;
    }

    // Decodes the frame's columns that the buffers lack into them. Idempotent: decode() is a no-op
    // for the columns the buffers already hold, so a repeat navigate to the same frame reuses
    // them. A failed decode frees the buffers: a holder bound to them is unbound, and the next
    // navigation decodes the frame again.
    private void decodeCovered(CoveringBuffers buffers, int frameIndex, @Nullable IntHashSet columnIndexes) {
        try {
            buffers.decode(
                    frameIndex,
                    addressCache.getCoveredIndexReader(frameIndex),
                    addressCache.getCoveredKey(frameIndex),
                    addressCache.getCoveredRowLo(frameIndex),
                    addressCache.getCoveredRowHi(frameIndex),
                    addressCache.getCoveredIncludeIndices(frameIndex),
                    coveredRowCount(frameIndex),
                    columnIndexes
            );
        } catch (Throwable th) {
            evictCovered(buffers);
            throw th;
        }
    }

    /**
     * A record's decode of a covered frame: of the rows declared through
     * {@link #setRecordAtRows} alone, when the frame has few enough of them (see
     * {@link #isRowFilterEligible}), as a record visits only those; else in full.
     */
    @Nullable
    private CoveringBuffers decodeCoveredFrameForRecord(int frameIndex, byte usageBit) {
        final long slice = recordAtSlices.get(frameIndex);
        if (slice == -1
                || !addressCache.isFrameSingleKeyCovered(frameIndex)
                || !isRowFilterEligible(frameIndex, Numbers.decodeHighInt(slice) - Numbers.decodeLowInt(slice))) {
            return decodeCoveredFrame(frameIndex, null, usageBit);
        }
        final CoveringBuffers buffers = acquireCoveringBuffers(frameIndex, usageBit);
        if (buffers.isRowFiltered && buffers.frameIndex == frameIndex || buffers.isFullyDecoded() && buffers.frameIndex == frameIndex) {
            // the holder's previous frame, which it has left, is the budget's
            trimCoveredToBudget(false);
            return buffers;
        }
        final int sliceLo = Numbers.decodeLowInt(slice);
        final int sliceHi = Numbers.decodeHighInt(slice);
        try {
            if (coveredRowScratch == null) {
                coveredRowScratch = new DirectLongList(sliceHi - sliceLo, MemoryTag.NATIVE_DEFAULT);
            }
            coveredRowScratch.clear();
            coveredRowScratch.ensureCapacity(sliceHi - sliceLo);
            // ascending, and a row declared twice decodes once
            long prev = -1;
            for (int i = sliceLo; i < sliceHi; i++) {
                final long row = recordAtRows.get(i);
                if (row != prev) {
                    coveredRowScratch.add(row);
                    prev = row;
                }
            }
            coveredRowDecodeCount++;
            buffers.decodeRows(
                    frameIndex,
                    addressCache.getCoveredIndexReader(frameIndex),
                    addressCache.getCoveredKey(frameIndex),
                    addressCache.getCoveredRowLo(frameIndex),
                    addressCache.getCoveredRowHi(frameIndex),
                    addressCache.getCoveredIncludeIndices(frameIndex),
                    coveredRowCount(frameIndex),
                    coveredRowScratch
            );
        } catch (Throwable th) {
            evictCovered(buffers);
            throw th;
        }
        trimCoveredToBudget(false);
        return buffers;
    }

    /**
     * Returns this pool's buffers for a covered frame, the cached ones or new ones, with the
     * {@code usageBit} holder bound to them, see {@link #bindCovered}; {@link #NO_HOLDER} binds none.
     * New buffers take the place of the unbound ones the cache budget has no room for, see
     * {@link #trimCoveredToBudget}, and the holder's own buffers are released first, so that a
     * zero-budget pool that moves from frame to frame holds one frame at a time.
     */
    private CoveringBuffers acquireCoveringBuffers(int frameIndex, byte usageBit) {
        final int keyIndex = coveringByFrame.keyIndex(frameIndex);
        CoveringBuffers buffers = coveringByFrame.valueAt(keyIndex);
        if (buffers != null) {
            coveredLruMoveToTail(buffers);
            bindCovered(usageBit, buffers);
            return buffers;
        }
        unbindCovered(usageBit);
        trimCoveredToBudget(true);
        final int shellCount = freeCoveringShells.size();
        if (shellCount > 0) {
            buffers = freeCoveringShells.getQuick(shellCount - 1);
            freeCoveringShells.setPos(shellCount - 1);
            // a parked shell gets its address lists back
            try {
                buffers.reopen();
            } catch (Throwable th) {
                // in no list now, so nothing else would free what the reopen allocated
                buffers.close();
                throw th;
            }
        } else {
            buffers = new CoveringBuffers();
        }
        buffers.cacheFrameIndex = frameIndex;
        coveringByFrame.put(frameIndex, buffers);
        coveredLruAppend(buffers);
        bindCovered(usageBit, buffers);
        return buffers;
    }

    // Binds the usageBit holder to the buffers, releasing the buffers it was bound to.
    private void bindCovered(byte usageBit, CoveringBuffers buffers) {
        if (usageBit == NO_HOLDER) {
            return;
        }
        final CoveringBuffers bound = getCoveredBound(usageBit);
        if (bound != buffers) {
            if (bound != null) {
                bound.usageFlags &= (byte) ~usageBit;
            }
            buffers.usageFlags |= usageBit;
            setCoveredBound(usageBit, buffers);
        }
    }

    private void coveredLruAppend(CoveringBuffers b) {
        b.prev = coveredLruTail;
        b.next = null;
        if (coveredLruTail != null) {
            coveredLruTail.next = b;
        } else {
            coveredLruHead = b;
        }
        coveredLruTail = b;
    }

    private void coveredLruMoveToTail(CoveringBuffers b) {
        if (b != coveredLruTail) {
            coveredLruUnlink(b);
            coveredLruAppend(b);
        }
    }

    private void coveredLruUnlink(CoveringBuffers b) {
        if (b.prev != null) {
            b.prev.next = b.next;
        } else {
            coveredLruHead = b.next;
        }
        if (b.next != null) {
            b.next.prev = b.prev;
        } else {
            coveredLruTail = b.prev;
        }
        b.prev = null;
        b.next = null;
    }

    /**
     * Frees a covered frame's buffers, unbinding the holders bound to them. A record bound to them
     * through {@link PageFrameMemoryRecord#init(PageFrameMemory)}, which does not bind, fails the
     * bind-generation check of its next navigation and rebinds. The buffers become a shell for the
     * next frames, with their address lists in a pool that keeps shells and parked, holding no
     * native memory, in any other, or past the shell cap are closed.
     */
    private void evictCovered(CoveringBuffers b) {
        bindGeneration++;
        if (b.usageFlags != 0) {
            if (coveredBoundForFrameMemory == b) {
                coveredBoundForFrameMemory = null;
                frameMemory.clear();
            }
            if (coveredBoundForRecordA == b) {
                coveredBoundForRecordA = null;
            }
            if (coveredBoundForRecordB == b) {
                coveredBoundForRecordB = null;
            }
            b.usageFlags = 0;
        }
        coveringByFrame.remove(b.cacheFrameIndex);
        coveredLruUnlink(b);
        b.isDecodedAhead = false;
        if (freeCoveringShells.size() < COVERING_SHELL_POOL_CAP) {
            if (isRetainingCoveringShells) {
                b.recycle();
            } else {
                b.park();
            }
            freeCoveringShells.add(b);
        } else {
            b.close();
        }
    }

    // A covering scan's frames are all NATIVE, so an address cache never holds a covered frame and a
    // Parquet frame: a holder that binds a Parquet frame is on no covered one.
    private boolean isCoveredUnbound(byte usageBit) {
        return getCoveredBound(usageBit) == null && (usageBit != FRAME_MEMORY_MASK || frameMemory.coveringBuffers == null);
    }

    private CoveringBuffers getCoveredBound(byte usageBit) {
        return switch (usageBit) {
            case RECORD_A_MASK -> coveredBoundForRecordA;
            case RECORD_B_MASK -> coveredBoundForRecordB;
            case FRAME_MEMORY_MASK -> coveredBoundForFrameMemory;
            default -> {
                assert false : "unknown usage bit";
                yield null;
            }
        };
    }

    // Frees the covered frames, but, unless isAll, those decoded ahead of a fold, whose holders are
    // unbound. The run decode scratch goes with them in a pool that keeps no shells, which keeps no
    // native memory past a frame.
    private void releaseCoveredFrames(boolean isAll) {
        CoveringBuffers b = coveredLruHead;
        while (b != null) {
            final CoveringBuffers next = b.next;
            if (!isAll && b.isDecodedAhead) {
                b.usageFlags = 0;
            } else {
                evictCovered(b);
            }
            b = next;
        }
        coveredBoundForFrameMemory = null;
        coveredBoundForRecordA = null;
        coveredBoundForRecordB = null;
        if (runDecodeWorkspace != 0 && (isAll || !isRetainingCoveringShells)) {
            runDecodeWorkspace = Unsafe.free(runDecodeWorkspace, (long) RUN_DECODE_CHUNK * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    private void setCoveredBound(byte usageBit, CoveringBuffers b) {
        switch (usageBit) {
            case RECORD_A_MASK -> coveredBoundForRecordA = b;
            case RECORD_B_MASK -> coveredBoundForRecordB = b;
            case FRAME_MEMORY_MASK -> coveredBoundForFrameMemory = b;
            default -> {
                assert false : "unknown usage bit";
            }
        }
    }

    /**
     * Frees the unbound covered frames, oldest first, that the cache budget has no room for: while
     * the decoded frames' bytes, covered and Parquet, exceed the pool's budget, or the covered frames
     * are more than {@link #maxCachedBuffers()}, counting one more when {@code isMakingRoom}, for a
     * frame about to be decoded. A zero-budget pool keeps the bound ones alone. A frame that the
     * frame memory or a record is bound to, or that was decoded ahead of a fold, stays.
     */
    private void trimCoveredToBudget(boolean isMakingRoom) {
        final long budgetBytes = effectiveBudgetBytes - cachedBytes;
        final int maxFrames = maxCachedBuffers() - (isMakingRoom ? 1 : 0);
        CoveringBuffers b = coveredLruHead;
        while (b != null && (coveredCachedBytes > budgetBytes || coveringByFrame.size() > maxFrames)) {
            final CoveringBuffers next = b.next;
            if (b.usageFlags == 0 && !b.isDecodedAhead) {
                evictCovered(b);
            }
            b = next;
        }
    }

    // Releases the covered buffers the usageBit holder is bound to; they stay cached, unbound.
    private void unbindCovered(byte usageBit) {
        if (usageBit == NO_HOLDER) {
            return;
        }
        final CoveringBuffers bound = getCoveredBound(usageBit);
        if (bound != null) {
            bound.usageFlags &= (byte) ~usageBit;
            setCoveredBound(usageBit, null);
        }
    }

    private int coveredRowCount(int frameIndex) {
        // The frame's declared size is the covered (matched) row count, NOT the
        // base range width: see CoveringPageFrameCursor#finalizeFrame, which sets
        // partitionHi = count. It is bounded by the table's page-frame row cap in
        // practice, but guard the int cast so a pathological >2^31-row covered frame
        // fails loud rather than truncating and silently under-sizing every buffer.
        final long frameSize = addressCache.getFrameSize(frameIndex);
        if (frameSize > Integer.MAX_VALUE) {
            throw CairoException.nonCritical().put("covered frame too large [rows=").put(frameSize).put(']');
        }
        return (int) frameSize;
    }

    private void decodeRowFilteredAndAccount(int frameIndex, ParquetBuffers parquetBuffers, long slice) {
        final int sliceLo = Numbers.decodeLowInt(slice);
        final int sliceHi = Numbers.decodeHighInt(slice);
        final int rowGroupIndex = addressCache.getParquetRowGroup(frameIndex);
        final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
        final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
        try {
            parquetBuffers.decodeRowFiltered(
                    activeDecoder,
                    parquetColumns,
                    rowGroupIndex,
                    rowGroupLo,
                    rowGroupHi,
                    recordAtRows.getAddress() + 8L * sliceLo,
                    sliceHi - sliceLo
            );
        } catch (Throwable th) {
            evictHalfInitialized(parquetBuffers);
            throw th;
        }
        accountDecode(parquetBuffers);
    }

    private void decodeWindowAndAccount(int frameIndex, ParquetBuffers parquetBuffers, int decodeLo, int decodeHi, int frameRowLo) {
        final int rowGroupIndex = addressCache.getParquetRowGroup(frameIndex);
        try {
            parquetBuffers.decode(activeDecoder, parquetColumns, rowGroupIndex, decodeLo, decodeHi, frameRowLo);
        } catch (Throwable th) {
            evictHalfInitialized(parquetBuffers);
            throw th;
        }
        accountDecode(parquetBuffers);
    }

    private void evictAndClose(ParquetBuffers buffers) {
        // An unpinned buffer may still be aliased by a record bound through
        // init(PageFrameMemory); fail its fast path before freeing the memory.
        bindGeneration++;
        cachedBytes -= buffers.retainedBytes;
        if (buffers.frameIndex >= 0) {
            byFrameIndex.remove(buffers.frameIndex);
        }
        lruUnlink(buffers);
        buffers.close();
        if (freeParquetBufferShells.size() < SHELL_POOL_CAP) {
            freeParquetBufferShells.add(buffers);
        }
    }

    private void evictHalfInitialized(ParquetBuffers buffers) {
        // The buffer may be aliased by records bound before the failed decode;
        // evictAndClose() bumps the generation, which fails their fast-path check
        // so they rebind instead of reading the freed memory.
        if (boundForRecordA == buffers) {
            boundForRecordA = null;
        }
        if (boundForRecordB == buffers) {
            boundForRecordB = null;
        }
        if (boundForFrameMemory == buffers) {
            boundForFrameMemory = null;
            frameMemory.clear();
        }
        evictAndClose(buffers);
    }

    private void evictRowFiltered(ParquetBuffers b) {
        if (b.usageFlags == 0) {
            evictAndClose(b);
        } else {
            // Evicting while a record still reads the buffer is a caller contract
            // violation; unmap it so it cannot serve further hits and let the LRU
            // close it once the pin clears.
            assert false : "row-filtered buffer is pinned";
            byFrameIndex.remove(b.frameIndex);
            b.frameIndex = -1;
        }
    }

    private void evictRowFilteredBuffers() {
        // a covered frame decoded for the declared rows alone decodes again, in full or for the
        // next declaration; records bound to it rebind
        for (CoveringBuffers c = coveredLruHead; c != null; c = c.next) {
            if (c.isRowFiltered) {
                c.isRowFiltered = false;
                bindGeneration++;
            }
        }
        ParquetBuffers b = lruHead;
        while (b != null) {
            final ParquetBuffers next = b.next;
            if (b.isRowFiltered) {
                evictRowFiltered(b);
            }
            b = next;
        }
    }

    private ParquetBuffers getBound(byte usageBit) {
        return switch (usageBit) {
            case RECORD_A_MASK -> boundForRecordA;
            case RECORD_B_MASK -> boundForRecordB;
            case FRAME_MEMORY_MASK -> boundForFrameMemory;
            default -> {
                assert false : "unknown usage bit";
                yield null;
            }
        };
    }

    private DirectLongList getNullColumnAddresses() {
        final int columnCount = addressCache.getColumnCount();
        if (nullColumnAddresses == null) {
            nullColumnAddresses = new DirectLongList(Math.max(columnCount, 16), MemoryTag.NATIVE_DEFAULT);
        }
        if (nullColumnAddresses.size() < columnCount) {
            nullColumnAddresses.setCapacity(columnCount);
            nullColumnAddresses.zero();
            nullColumnAddresses.setPos(columnCount);
        }
        return nullColumnAddresses;
    }

    // Zero-budget pools (per-worker parallel reduce slots, new PageFrameMemoryPool(0L)) never
    // cache, so they skip the per-decode byte sum: cachedBytes is unused for them.
    private boolean isAccountingEnabled() {
        return effectiveBudgetBytes != 0;
    }

    // Fast path for repeat visits: a native binding covers any window; a parquet
    // binding covers the request only when the buffer still belongs to the frame,
    // holds every column (not a late-materialized subset, even after
    // populateRemainingColumns(), which decodes the rest for the filtered rows only)
    // and its decoded window spans the requested rows. An empty-window binding
    // (currentRowGroupBuffer == null) never covers a non-empty request.
    private boolean isFrameMemoryCovering(int frameIndex, int inFrameRowLo, int inFrameRowHi) {
        if (frameMemory.frameFormat == PartitionFormat.NATIVE) {
            if (addressCache.isFrameSingleKeyCovered(frameIndex)) {
                // the frame memory on a covered frame is bound to its buffers
                final CoveringBuffers buffers = frameMemory.coveringBuffers;
                assert buffers != null && buffers.cacheFrameIndex == frameIndex;
                return buffers.isFullyDecoded();
            }
            return true;
        }
        if (frameMemory.decodedColumnIndexes != null) {
            return false;
        }
        final ParquetBuffers buffers = frameMemory.currentRowGroupBuffer;
        if (buffers == null || buffers.frameIndex != frameIndex || buffers.isRowFiltered || buffers.columnSubset != null) {
            return false;
        }
        final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
        final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
        final int decodeLo = (int) Math.min(rowGroupHi, rowGroupLo + (long) inFrameRowLo);
        final int decodeHi = (int) Math.min(rowGroupHi, rowGroupLo + (long) inFrameRowHi);
        return buffers.decodedRowLo <= decodeLo && buffers.decodedRowHi >= decodeHi;
    }

    // Fast path for a repeat late-materialized visit (navigateTo(int, IntHashSet)): see
    // servesColumnSubset(). An empty-window binding (currentRowGroupBuffer == null) never
    // covers it.
    private boolean isFrameMemoryCovering(int frameIndex, IntHashSet columnIndexes) {
        if (frameMemory.frameFormat == PartitionFormat.NATIVE) {
            if (addressCache.isFrameSingleKeyCovered(frameIndex)) {
                // a covered filter-first decode holds the filter's columns only; the frame memory
                // on a covered frame is bound to its buffers
                final CoveringBuffers buffers = frameMemory.coveringBuffers;
                assert buffers != null && buffers.cacheFrameIndex == frameIndex;
                return buffers.hasFullColumns(columnIndexes);
            }
            return true;
        }
        final ParquetBuffers buffers = frameMemory.currentRowGroupBuffer;
        return buffers != null && servesColumnSubset(frameIndex, buffers, columnIndexes);
    }

    private boolean isRowFilterEligible(int frameIndex, long declaredRowCount) {
        if (addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET) {
            return declaredRowCount * 2 < addressCache.getParquetRowGroupHi(frameIndex) - addressCache.getParquetRowGroupLo(frameIndex);
        }
        return addressCache.isFrameSingleKeyCovered(frameIndex) && declaredRowCount * 2 < addressCache.getFrameSize(frameIndex);
    }

    private void lruAppend(ParquetBuffers b) {
        b.prev = lruTail;
        b.next = null;
        if (lruTail != null) {
            lruTail.next = b;
        } else {
            lruHead = b;
        }
        lruTail = b;
    }

    private void lruMoveToTail(ParquetBuffers b) {
        if (b == lruTail) {
            return;
        }
        lruUnlink(b);
        lruAppend(b);
    }

    private void lruUnlink(ParquetBuffers b) {
        if (b.prev != null) {
            b.prev.next = b.next;
        } else {
            lruHead = b.next;
        }
        if (b.next != null) {
            b.next.prev = b.prev;
        } else {
            lruTail = b.prev;
        }
        b.prev = null;
        b.next = null;
    }

    // Row-filtered buffers retain only the declared rows, so a declaration may need
    // more entries than the hint's cap before the byte budget binds; it never needs
    // more than the declared frame count.
    private int maxCachedBuffers() {
        return Math.max(decodeHint.maxCachedBuffers, recordAtSlices.size());
    }

    /**
     * Worker-side covered decode arm for the RECORD fast-path (row filters,
     * {@link PageFrameFilteredMemoryRecord} late materialization /
     * {@code recordAt}, negative-limit, {@code AbstractPageFrameRecordCursor}).
     * Mirrors {@link #patchCoveredFrameMemory} but binds a {@link Record} instead
     * of the flyweight: when {@code frameIndex} is a single-key covered frame,
     * decode its covered columns into this pool's per-frame {@link CoveringBuffers},
     * bind the record's letter to them, which keeps them from the cache budget until
     * the record moves to another frame, and point {@code record} at them. The record
     * is not stable, see {@link PageFrameMemory#isStable()}.
     * <p>
     * The bind uses the SAME guard as the parquet record arm -- {@code boundPool ==
     * this} and {@code boundGeneration == bindGeneration} -- because a covered
     * record points at per-pool buffers that a release frees or a different
     * generation may own, exactly like parquet buffers and UNLIKE the query-stable
     * address-cache arrays a plain native record uses. Stamping the pool/generation
     * lets {@link #navigateTo(int, PageFrameMemoryRecord)} take the cheap
     * fast-return on a repeat visit while rejecting a stale binding, and ensures a
     * covered record never takes the frame-index-only NATIVE fast-return.
     * <p>
     * Returns {@code true} when the frame was handled as covered (the record is now
     * bound), {@code false} for a non-covered or multi-key frame (the caller falls
     * through to the normal native bind, keeping the eager flat addresses).
     */
    private boolean navigateCoveredRecord(int frameIndex, PageFrameMemoryRecord record) {
        if (!addressCache.isFrameSingleKeyCovered(frameIndex)) {
            return false;
        }
        final byte usageBit = record.getLetter() == PageFrameMemoryRecord.RECORD_A_LETTER ? RECORD_A_MASK : RECORD_B_MASK;
        // Fast-return: the record already points at THIS pool's live covered
        // buffers for this frame, which its letter is bound to. The pool/generation
        // guard distinguishes "still ours and live" from "bound elsewhere or
        // released", mirroring the parquet record arm; every release and eviction
        // bumps bindGeneration, so a stale covered binding rebinds instead of reading
        // freed buffers.
        if (record.getFrameIndex() == frameIndex && record.getBoundPool() == this && record.getBoundGeneration() == bindGeneration) {
            final CoveringBuffers buffers = getCoveredBound(usageBit);
            if (buffers != null && buffers.cacheFrameIndex == frameIndex && (buffers.isFullyDecoded() || buffers.isRowFiltered)) {
                return true;
            }
        }
        final CoveringBuffers buffers;
        try {
            buffers = decodeCoveredFrameForRecord(frameIndex, usageBit);
        } catch (Throwable th) {
            // as the Parquet arm does: the failed decode freed the buffers, so the record must
            // not read through its stale binding
            record.clear();
            throw th;
        }
        assert buffers != null;
        // the record leaves the Parquet buffer it was bound to, if any
        unbind(usageBit);
        record.init(
                frameIndex,
                PartitionFormat.NATIVE,
                false,
                addressCache.getRowIdOffset(frameIndex),
                buffers.getPageAddresses(),
                buffers.getAuxPageAddresses(),
                buffers.getPageSizes(),
                buffers.getAuxPageSizes(),
                0, // covered buffers are frame-specific, so they use a 0 column offset
                addressCache.getColumnCount(),
                false, // covered NATIVE frames have no parquet-style lazy type conversion
                null,
                null
        );
        record.setBoundPool(this, bindGeneration);
        return true;
    }

    private void openParquet(int frameIndex) {
        activateDecoder(frameIndex);
        if (hasFullProjectionMap) {
            return;
        }

        parquetColumns.reopen();
        parquetColumns.clear();
        parquetIdxToDecodeSlot.clear();

        final ColumnMapping columnMapping = addressCache.getColumnMapping();

        final int readParquetColumnCount = columnMapping.getColumnCount();
        queryToSlot.setPos(readParquetColumnCount);
        for (int q = 0; q < readParquetColumnCount; q++) {
            queryToSlot.setQuick(q, -1);
        }
        sourceColumnTypes.setAll(readParquetColumnCount, -1);
        hasTypeCasts = false;
        final IntHashSet projection = this.projection;
        for (int i = 0; i < readParquetColumnCount; i++) {
            // A column outside the projection keeps no decode slot, so it stays at address 0 (NULL)
            // with no type cast.
            if (projection == null || projection.contains(i)) {
                resolveParquetColumn(i, columnMapping, activeDecoder);
            }
        }
        assert parquetColumns.size() % 2 == 0 : "parquetColumns must hold [parquetIdx, columnType] pairs";
        hasFullProjectionMap = true;
    }

    private void openParquet(int frameIndex, IntHashSet columnIndexes, boolean isInclude) {
        activateDecoder(frameIndex);
        hasFullProjectionMap = false;

        parquetColumns.reopen();
        parquetColumns.clear();
        parquetIdxToDecodeSlot.clear();

        final ColumnMapping columnMapping = addressCache.getColumnMapping();

        final int readParquetColumnCount = columnMapping.getColumnCount();
        queryToSlot.setPos(readParquetColumnCount);
        for (int q = 0; q < readParquetColumnCount; q++) {
            queryToSlot.setQuick(q, -1);
        }
        if (isInclude) {
            // First-pass navigation: start from a clean slate.
            sourceColumnTypes.setAll(readParquetColumnCount, -1);
            hasTypeCasts = false;
        }
        // isInclude=false is populateRemainingColumns: retain sourceColumnTypes / hasTypeCasts
        // set by the prior isInclude=true call so that lazy conversion metadata for filter
        // columns survives. Without this, PageFrameMemoryRecord re-snapshots a stale -1
        // for filter columns and reads VARCHAR_SLICE bytes as the target fixed type.
        final IntHashSet projection = this.projection;
        for (int i = 0; i < readParquetColumnCount; i++) {
            if (columnIndexes.contains(i) != isInclude || (projection != null && !projection.contains(i))) {
                continue;
            }
            resolveParquetColumn(i, columnMapping, activeDecoder);
        }
        assert parquetColumns.size() % 2 == 0 : "parquetColumns must hold [parquetIdx, columnType] pairs";
    }

    /**
     * Worker-side covered decode arm. When {@code frameIndex} is a covered frame
     * with a single resolved key, decode its covered columns from the posting
     * sidecar into this pool's per-worker {@link CoveringBuffers} and rebind
     * {@code frameMemory} to those buffers (a covered frame's whole row is
     * sidecar-decoded, so every column is rebound). This OVERRIDES the flat
     * addresses just rebound by the NATIVE arm, so the result is correct
     * regardless of what those addresses hold -- single-key covered frames are
     * produced metadata-only (those addresses are PLACEHOLDER zeroes), and this
     * is their SOLE decoder. No-op for non-covered frames.
     * <p>
     * Multi-key covered frames ({@code key == VALUE_NOT_FOUND}) interleave several
     * keys per row in a merge order a single detached cursor cannot reproduce, so
     * they are left on the eager flat addresses (still produced eagerly at frame
     * production -- see {@code fillMergedFrame}).
     */
    private void patchCoveredFrameMemory(int frameIndex) {
        patchCoveredFrameMemory(frameIndex, null);
    }

    private void patchCoveredFrameMemory(int frameIndex, @Nullable IntHashSet columnIndexes) {
        final CoveringBuffers buffers = decodeCoveredFrame(frameIndex, columnIndexes, FRAME_MEMORY_MASK);
        frameMemory.coveringBuffers = buffers;
        if (buffers == null) {
            // Non-covered or multi-key frame: keep the eager flat addresses the
            // NATIVE arm bound, and leave the covered frame the frame memory was on.
            unbindCovered(FRAME_MEMORY_MASK);
            return;
        }
        frameMemory.currentRowGroupBuffer = null;
        frameMemory.pageAddresses = buffers.getPageAddresses();
        frameMemory.auxPageAddresses = buffers.getAuxPageAddresses();
        frameMemory.pageSizes = buffers.getPageSizes();
        frameMemory.auxPageSizes = buffers.getAuxPageSizes();
        frameMemory.columnOffset = 0;
    }

    /**
     * Returns the buffer a re-decode of {@code buffers} for the {@code usageBit} holder may
     * overwrite. Re-decoding in place rewrites the buffer's address lists and may move or
     * reallocate its slot vectors, which hollows out any other holder: a record that pinned the
     * buffer through the record path ({@code RECORD_A}/{@code RECORD_B}) or the frame memory, and
     * every raw pointer they handed out (JIT address arrays, varchar views).
     * <ul>
     *     <li>When {@code usageBit} is the only pin, the re-decode goes in place, and the bind
     *     generation is bumped so that records bound through
     *     {@link PageFrameMemoryRecord#init(PageFrameMemory)}, which alias the buffer without
     *     pinning it, fail their fast path and rebind.</li>
     *     <li>Otherwise the buffer is left to its other holders, unmapped so that it serves no
     *     further hit, and the re-decode goes to a fresh buffer. The LRU closes the old one once
     *     its last pin clears.</li>
     * </ul>
     */
    private ParquetBuffers prepareRedecode(int frameIndex, ParquetBuffers buffers, byte usageBit) {
        assert getBound(usageBit) == buffers && buffers.frameIndex == frameIndex;
        if ((buffers.usageFlags & ~usageBit) == 0) {
            bindGeneration++;
            return buffers;
        }
        buffers.usageFlags &= (byte) ~usageBit;
        setBound(usageBit, null);
        byFrameIndex.remove(frameIndex);
        buffers.frameIndex = -1;
        return acquireBuffer(frameIndex, usageBit);
    }

    /**
     * Frees every covered frame, also those decoded ahead of a fold, and the run decode scratch.
     * Called at query boundaries ({@link #of}, {@link #clear}, {@link #close},
     * {@link #releaseQueryResources}).
     */
    private void releaseCoveringBuffers() {
        releaseCoveredFrames(true);
        assert coveringByFrame.size() == 0 && coveredLruHead == null;
        // Every CoveringBuffers close() or recycle() subtracted its own allocation, so the running
        // total must net to zero.
        assert coveredCachedBytes == 0 : "covered byte accounting leaked: " + coveredCachedBytes;
        coveredCachedBytes = 0;
    }

    private void resolveParquetColumn(int i, ColumnMapping columnMapping, ParquetDecoder parquetMetadata) {
        final int columnWriterIndex = columnMapping.getWriterIndex(i);
        int parquetIdx = columnIdToParquetIdx.get(columnWriterIndex);

        if (parquetIdx < 0) {
            // Direct writer index lookup failed. The column may have been type-converted
            // (ALTER COLUMN TYPE), so the parquet file stores it under the original writer index.
            final int origWriterIndex = columnMapping.getOriginalWriterIndex(i);
            if (origWriterIndex >= 0 && origWriterIndex != columnWriterIndex) {
                parquetIdx = columnIdToParquetIdx.get(origWriterIndex);
            }
            if (parquetIdx >= 0) {
                int targetType = addressCache.getColumnTypes().getQuick(i);
                final int sourceType = parquetMetadata.getColumnType(parquetIdx);
                final int sourceTag = ColumnType.tagOf(sourceType);
                final int targetTag = ColumnType.tagOf(targetType);
                if (ColumnType.isSymbol(targetTag) && !ColumnType.isSymbol(sourceTag)) {
                    // Non-symbol -> symbol: the pre-pass in ConvertOperatorImpl should have
                    // converted this parquet partition to native. If we get here, it's a bug.
                    throw CairoException.critical(0)
                            .put("unexpected non-symbol->symbol in parquet, column=").put(i)
                            .put(", sourceType=").put(ColumnType.nameOf(sourceTag))
                            .put(", targetType=").put(ColumnType.nameOf(targetTag));
                }
                if (sourceTag == targetTag) {
                    // Same type, just a writer index mismatch after ALTER COLUMN TYPE.
                    // No conversion needed, decode normally.
                    if (ColumnType.tagOf(targetType) == ColumnType.VARCHAR) {
                        targetType = ColumnType.VARCHAR_SLICE;
                    }
                    queryToSlot.setQuick(i, addDecodeSlotIfAbsent(parquetIdx, targetType));
                    return;
                }

                if (ColumnType.isSymbol(sourceTag) && !ColumnType.isSymbol(targetTag)) {
                    // Symbol -> non-symbol: decode as VARCHAR_SLICE, Java converts lazily.
                    // For Symbol->VARCHAR, the fallthrough below handles it (VARCHAR_SLICE is native format).
                    if (targetTag != ColumnType.VARCHAR && targetTag != ColumnType.STRING) {
                        queryToSlot.setQuick(i, addDecodeSlotIfAbsent(parquetIdx, ColumnType.VARCHAR_SLICE));
                        // Negative VARCHAR tag signals var->fixed/var->string conversion.
                        // Same target-type metadata layout as the var->fixed branch
                        // below; the Symbol-as-VARCHAR_SLICE rows are converted by
                        // the same lazy converters in PageFrameMemoryRecord.
                        int encoded = ColumnType.VARCHAR;
                        if (ColumnType.isDecimal(targetType)) {
                            encoded |= (ColumnType.getDecimalPrecision(targetType) << 8)
                                    | (ColumnType.getDecimalScale(targetType) << 16);
                        } else if (ColumnType.isTimestampNano(targetType)) {
                            encoded |= (1 << 24);
                        }
                        sourceColumnTypes.setQuick(i, -encoded);
                        hasTypeCasts = true;
                        return;
                    }
                    // Symbol->VARCHAR falls through to the bottom of this method.
                }

                if (!ColumnType.isVarSize(sourceTag) && !ColumnType.isSymbol(sourceTag)
                        && (targetTag == ColumnType.VARCHAR || targetTag == ColumnType.STRING)) {
                    // Fixed -> var-size: decode as source fixed type.
                    // Java does lazy per-row conversion in PageFrameMemoryRecord.
                    queryToSlot.setQuick(i, addDecodeSlotIfAbsent(parquetIdx, sourceType));
                    sourceColumnTypes.setQuick(i, sourceType);
                    hasTypeCasts = true;
                    return;
                }

                if (ColumnType.isVarSize(sourceTag) && !ColumnType.isVarSize(targetTag)
                        && !ColumnType.isSymbol(targetTag)) {
                    // Var -> fixed-size: decode as source var type.
                    // Java does lazy per-row conversion in PageFrameMemoryRecord.
                    int decodeType = (sourceTag == ColumnType.VARCHAR)
                            ? ColumnType.VARCHAR_SLICE : sourceType;
                    queryToSlot.setQuick(i, addDecodeSlotIfAbsent(parquetIdx, decodeType));
                    // Negative value signals var->fixed direction.
                    // -1 remains the "no conversion" sentinel.
                    // Bit layout of the encoded value (target-specific metadata
                    // in the upper bits - only one target family fills 8-23 at a time):
                    //   bits 0-7:   source tag (STRING or VARCHAR)
                    //   bits 8-15:  target decimal precision (decimal targets)
                    //   bits 16-23: target decimal scale (decimal targets)
                    //   bit  24:    target timestamp precision (0 = micros, 1 = nanos)
                    int encoded = ColumnType.tagOf(sourceType);
                    if (ColumnType.isDecimal(targetType)) {
                        encoded |= (ColumnType.getDecimalPrecision(targetType) << 8)
                                | (ColumnType.getDecimalScale(targetType) << 16);
                    } else if (ColumnType.isTimestampNano(targetType)) {
                        encoded |= (1 << 24);
                    }
                    sourceColumnTypes.setQuick(i, -encoded);
                    hasTypeCasts = true;
                    return;
                }

                // Fixed -> fixed type conversion: tell Rust to decode as target type.
                if (targetTag == ColumnType.VARCHAR) {
                    targetType = ColumnType.VARCHAR_SLICE;
                }
                queryToSlot.setQuick(i, addDecodeSlotIfAbsent(parquetIdx, targetType));
                return;
            }
        }

        if (parquetIdx >= 0) {
            int columnType = addressCache.getColumnTypes().getQuick(i);
            if (ColumnType.tagOf(columnType) == ColumnType.VARCHAR) {
                columnType = ColumnType.VARCHAR_SLICE;
            }
            queryToSlot.setQuick(i, addDecodeSlotIfAbsent(parquetIdx, columnType));
        }
        // Column missing from parquet (ADD COLUMN): stays at address 0 (NULL).
        // Repeated parquet column: decode once; remapColumns() fans the
        // buffer out to every query column that shares the parquet column.
    }

    // A buffer serves a late-materialized navigate (navigateTo(int, IntHashSet)) when it belongs to
    // the frame, spans the whole row window, holds every requested column (a full decode, or a subset
    // decode whose subset contains the request) and has not been populated by
    // populateRemainingColumns(), which rewrites the other columns for an earlier caller's filtered
    // rows. Such a populated buffer re-decodes, so that each subset binding is populated at most once.
    private boolean servesColumnSubset(int frameIndex, ParquetBuffers buffers, IntHashSet columnIndexes) {
        if (buffers.frameIndex != frameIndex || buffers.isRowFiltered || buffers.isPopulated) {
            return false;
        }
        if (buffers.decodedRowLo > addressCache.getParquetRowGroupLo(frameIndex) || buffers.decodedRowHi < addressCache.getParquetRowGroupHi(frameIndex)) {
            // only a full decode by the windowed overload can hold a partial window
            assert buffers.columnSubset == null : "a late-materialized decode spans the whole frame";
            return false;
        }
        return buffers.columnSubset == null || containsAll(buffers.columnSubset, columnIndexes);
    }

    private void setBound(byte usageBit, ParquetBuffers b) {
        switch (usageBit) {
            case RECORD_A_MASK -> boundForRecordA = b;
            case RECORD_B_MASK -> boundForRecordB = b;
            case FRAME_MEMORY_MASK -> boundForFrameMemory = b;
            default -> {
                assert false : "unknown usage bit";
            }
        }
    }

    // A late-materialized decode covers the requested columns over the whole frame.
    private void setColumnSubset(int frameIndex, ParquetBuffers buffers, IntHashSet columnIndexes) {
        assert buffers.decodedRowLo == addressCache.getParquetRowGroupLo(frameIndex)
                && buffers.decodedRowHi == addressCache.getParquetRowGroupHi(frameIndex)
                && !buffers.isRowFiltered && !buffers.isPopulated;
        buffers.columnSubset = columnIndexes;
    }

    private boolean shouldDecodeRowFiltered(int frameIndex, long slice) {
        if (slice == -1) {
            return false;
        }
        return isRowFilterEligible(frameIndex, Numbers.decodeHighInt(slice) - Numbers.decodeLowInt(slice));
    }

    private void trimToBudget() {
        ParquetBuffers b = lruHead;
        while (b != null && cachedBytes > effectiveBudgetBytes) {
            final ParquetBuffers next = b.next;
            if (b.usageFlags == 0) {
                evictAndClose(b);
            }
            b = next;
        }
    }

    @Nullable
    private ParquetBuffers tryHit(int frameIndex, byte usageBit) {
        final ParquetBuffers previousBound = getBound(usageBit);
        if (previousBound != null && previousBound.frameIndex == frameIndex) {
            if (decodeHint == ParquetDecodeHint.SCATTERED) {
                lruMoveToTail(previousBound);
            }
            return previousBound;
        }
        final ParquetBuffers hit = byFrameIndex.get(frameIndex);
        if (hit == null) {
            unbind(usageBit);
            return null;
        }
        if (previousBound != null) {
            previousBound.usageFlags &= (byte) ~usageBit;
        }
        hit.usageFlags |= usageBit;
        setBound(usageBit, hit);
        if (decodeHint == ParquetDecodeHint.SCATTERED) {
            lruMoveToTail(hit);
        }
        return hit;
    }

    private void unbind(byte usageBit) {
        final ParquetBuffers bound = getBound(usageBit);
        if (bound != null) {
            bound.usageFlags &= (byte) ~usageBit;
            setBound(usageBit, null);
        }
    }

    /**
     * Decoded buffers for a single covered (posting-index sidecar) frame. It owns
     * native column buffers and the four published address/size lists that the
     * navigateTo covered arm rebinds the frame memory to. There is one instance
     * PER FRAME (keyed in {@link #coveringByFrame}), which lives as a decoded
     * Parquet frame's buffers do: while the frame memory or a record is bound to
     * it, or while the cache budget has room for it, until the pool releases its
     * decoded frames, see {@link #releaseParquetBuffers()}. A record over it is
     * not stable, so a reader that keeps a value past the frame copies it. The
     * eager production path of a multi-key frame keeps its own buffers, which its
     * page frame cursor frees at close.
     * <p>
     * The native bytes charge the pool's per-query tracker of the time the first
     * of them is allocated, and the free releases them from that tracker, see
     * {@link #chargedTracker()}.
     * <p>
     * {@link #decode} opens a DETACHED covering cursor that this worker owns
     * outright (see {@code PostingIndexFwdReader#getDetachedCursor}), drains it
     * fully into the native buffers, and closes it within the call -- so the
     * cursor never outlives a single decode and the buffers, once filled, no
     * longer depend on the reader's mmaps. Concurrent same-reader safety during
     * the drain comes from the reader being frozen (reload is a no-op) and warm
     * (no gen-load mutates shared state); the covering pipeline establishes both
     * before any worker decodes. {@link #decode} is idempotent: a repeat navigate
     * to the already-decoded frame reuses the live buffers untouched.
     */
    private class CoveringBuffers implements QuietCloseable, CoveredColumnDecoder.VarDataSink {
        private final DirectLongList auxPageAddresses;
        private final DirectLongList auxPageSizes;
        private final DirectLongList pageAddresses;
        private final DirectLongList pageSizes;
        // live native bytes of the column buffers, colCap + varDataCap + symCap
        private long allocatedBytes;
        // the frame index the pool caches the buffers under, see acquireCoveringBuffers()
        private int cacheFrameIndex = -1;
        // the tracker the live column buffers charge, and its covered generation at the time,
        // see chargedTracker()
        private long chargedGeneration;
        private MemoryTracker chargedTracker;
        // decoded ahead of a fold, which releases the frame, see decodeAhead()
        private boolean isDecodedAhead;
        // LRU links, see coveredLruHead
        private CoveringBuffers next;
        private CoveringBuffers prev;
        // the holders bound to the buffers: the frame memory, record A, record B
        private byte usageFlags;
        // Per query column primary native buffer: the fixed-width data vector, or
        // the aux (offsets/headers) vector for a var-size column. The symbol-key
        // column shares symAddr instead. 0 when unallocated. Lazily sized to the
        // query's column count on first decode.
        private long[] colAddr;
        private long[] colCap;
        // Per query column size (bytes) for fixed-width columns; type tag; full
        // type; and the sidecar include index (>= 0 covered, -1 symbol key).
        // Query-constant, built once with the column buffers.
        private int[] columnSizeBytes;
        private int[] columnTypeTags;
        private int[] columnTypes;
        private int[] coveredIncludeIdx;
        // Per query column: true iff served by the covered-decode arm (a covered INCLUDE
        // or the synthesized symbol key). False for a non-covered column introduced by a
        // null-pad / projection wrapper over the covered frame — published as a NULL column.
        private boolean[] coveredColumn;
        private boolean[] isColumnDecoded;
        private boolean[] isColumnFull;
        private boolean[] isColumnSelected;
        // true for a recycled shell, whose metadata arrays the next frame's decode fills again
        private boolean isMetadataStale;
        // holds the declared rows of every covered column alone, see decodeRows()
        private boolean isRowFiltered;
        // decodeInRuns(): the selected fixed-width columns, copied in runs, and the other selected columns
        private boolean[] isRowColumn;
        private boolean[] isRunColumn;
        // the run's blocks, see captureRunBlocks(): compressed (a dense generation's key block)
        // or the raw values of a sparse generation
        private long[] runBlockAddrs;
        private boolean runBlocksCompressed;
        private int runColumnCount;
        private int[] runColumns;
        private int frameIndex = -1;
        private int rowCount;
        // Synthesized symbol-key column (broadcast int), shared by every DIRECT
        // (indexed key) column of the covered frame. 0 when unallocated.
        private long symAddr;
        private int symCap;
        // Per query column var-size data vector (VARCHAR/STRING/BINARY/ARRAY). 0
        // for fixed-width columns and the symbol key.
        private long[] varDataAddr;
        private int[] varDataCap;
        private int[] varDataPos;

        CoveringBuffers() {
            DirectLongList auxPageAddresses = null;
            DirectLongList auxPageSizes = null;
            DirectLongList pageAddresses = null;
            DirectLongList pageSizes = null;
            try {
                auxPageAddresses = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                auxPageSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                pageAddresses = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                pageSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
            } catch (Throwable th) {
                Misc.free(auxPageAddresses);
                Misc.free(auxPageSizes);
                Misc.free(pageAddresses);
                Misc.free(pageSizes);
                throw th;
            }
            this.auxPageAddresses = auxPageAddresses;
            this.auxPageSizes = auxPageSizes;
            this.pageAddresses = pageAddresses;
            this.pageSizes = pageSizes;
        }

        @Override
        public void advance(int q, int written) {
            varDataPos[q] += written;
        }

        @Override
        public void close() {
            freeColumnBuffers();
            Misc.free(pageAddresses);
            Misc.free(pageSizes);
            Misc.free(auxPageAddresses);
            Misc.free(auxPageSizes);
            cacheFrameIndex = -1;
            frameIndex = -1;
        }

        /**
         * Frees the native memory, the column buffers and the address lists, and keeps the
         * metadata arrays, for a decode of another frame, possibly of another query, which
         * {@link #reopen()}s the lists, see evictCovered().
         */
        void park() {
            recycle();
            Misc.free(pageAddresses);
            Misc.free(pageSizes);
            Misc.free(auxPageAddresses);
            Misc.free(auxPageSizes);
        }

        /**
         * Frees the column buffers and keeps the address lists and metadata arrays, for a decode
         * of another frame, possibly of another query, see evictCovered().
         */
        void recycle() {
            freeColumnBuffers();
            cacheFrameIndex = -1;
            frameIndex = -1;
            rowCount = 0;
            isMetadataStale = true;
            isRowFiltered = false;
        }

        // Gives a parked shell its address lists back; does nothing for another one.
        void reopen() {
            pageAddresses.reopen();
            pageSizes.reopen();
            auxPageAddresses.reopen();
            auxPageSizes.reopen();
        }

        /**
         * Decodes either all covered columns or the requested filter columns. The
         * first pass keeps original frame ordinals so JIT and Java filters share
         * the same row-coordinate contract.
         */
        void decode(
                int frameIndex,
                IndexReader reader,
                int key,
                long rowLo,
                long rowHi,
                int[] includeIndices,
                int rowCount,
                @Nullable IntHashSet columnIndexes
        ) {
            if (isRowFiltered) {
                // the declared rows' decode gives way to a full one: the column buffers are
                // rewritten, so records bound to them rebind
                isRowFiltered = false;
                bindGeneration++;
            }
            prepareSelectedColumns(frameIndex, rowCount, columnIndexes, false);
            if (!hasSelectedColumns()) {
                publishAddresses(rowCount);
                return;
            }
            decodeSelectedColumns(reader, key, rowLo, rowHi, includeIndices, null);
            for (int q = 0, n = queryColCount(); q < n; q++) {
                if (isColumnSelected[q]) {
                    isColumnDecoded[q] = true;
                    isColumnFull[q] = true;
                }
            }
            publishAddresses(rowCount);
            this.frameIndex = frameIndex;
        }

        /**
         * Decodes every covered column at the given rows of the frame alone (ascending local
         * row indexes), at their positions in the frame; the other rows read garbage, and a
         * var-size column's offsets stay ordered. Serves records that visit those rows only,
         * see setRecordAtRows(). An INCLUDE value decodes by its block window on access, so the
         * rows' windows alone decode.
         */
        void decodeRows(
                int frameIndex,
                IndexReader reader,
                int key,
                long rowLo,
                long rowHi,
                int[] includeIndices,
                int rowCount,
                DirectLongList rows
        ) {
            prepareSelectedColumns(frameIndex, rowCount, null, false);
            if (hasSelectedColumns()) {
                decodeSelectedColumns(reader, key, rowLo, rowHi, includeIndices, rows);
                for (int q = 0, n = queryColCount(); q < n; q++) {
                    if (isColumnSelected[q]) {
                        isColumnDecoded[q] = true;
                        // the symbol key fills every row
                        isColumnFull[q] = coveredIncludeIdx[q] < 0;
                    }
                }
            }
            publishAddresses(rowCount);
            this.frameIndex = frameIndex;
            isRowFiltered = true;
        }

        private void decodeSelectedColumns(
                IndexReader reader,
                int key,
                long rowLo,
                long rowHi,
                int[] includeIndices,
                @Nullable DirectLongList filteredRows
        ) {
            final int[] selectedIncludeIndices = selectIncludeIndices(includeIndices);
            final RowCursor rowCursor = reader.getDetachedCursor(
                    TableUtils.toIndexKey(key),
                    rowLo,
                    rowHi - 1,
                    selectedIncludeIndices
            );
            try {
                int count = 0;
                long filteredPosition = 0;
                final long filteredCount = filteredRows != null ? filteredRows.size() : 0;
                if (rowCursor instanceof CoveringRowCursor crc && filteredRows == null && selectRunColumns()) {
                    count = decodeInRuns(crc);
                } else if (rowCursor instanceof CoveringRowCursor crc) {
                    while (crc.hasNext()) {
                        crc.next();
                        if (count >= rowCount) {
                            break;
                        }
                        final boolean isSelectedRow = filteredRows == null
                                || filteredPosition < filteredCount && filteredRows.get(filteredPosition) == count;
                        if (isSelectedRow) {
                            CoveredColumnDecoder.writeCoveredRow(
                                    colAddr,
                                    this,
                                    count,
                                    crc,
                                    queryColCount(),
                                    coveredIncludeIdx,
                                    columnTypeTags,
                                    columnTypes,
                                    isColumnSelected
                            );
                            filteredPosition++;
                        } else if (filteredRows != null) {
                            writeSkippedVarOffsets(count);
                        }
                        count++;
                        if (filteredRows != null && count < rowCount) {
                            if (filteredPosition == filteredCount) {
                                // past the last selected row: the rest read nothing
                                writeSkippedVarOffsets(count, rowCount);
                                count = rowCount;
                                break;
                            }
                            // the rows up to the next selected one are passed in runs where the
                            // cursor can tell them without decoding
                            int gap = (int) (filteredRows.get(filteredPosition) - count);
                            while (gap > 0) {
                                final int more = crc.peekCoveredRun(gap);
                                if (more <= 0) {
                                    break;
                                }
                                crc.skipCoveredRun(more);
                                writeSkippedVarOffsets(count, count + more);
                                count += more;
                                gap -= more;
                            }
                        }
                    }
                }
                if (count != rowCount) {
                    throw CairoException.critical(0)
                            .put("covered re-decode row count mismatch: decoded ").put(count)
                            .put(" of ").put(rowCount).put(" [frameIndex=").put(frameIndex).put(']');
                }
                for (int q = 0, n = queryColCount(); q < n; q++) {
                    if (isColumnSelected[q] && coveredIncludeIdx[q] < 0) {
                        CoveredColumnDecoder.fillSymbolKey(symAddr, key, rowCount);
                        break;
                    }
                }
            } finally {
                Misc.free(rowCursor);
            }
        }

        /**
         * Decodes every row of the frame, copying the fixed-width columns in runs: consecutive rows
         * that read consecutive values of one generation's sidecar values (a key's rows within a
         * generation) are copied with one memory copy per column, instead of one accessor call per
         * row and column. Var-size columns, and the fixed-width columns of rows that cannot be read
         * in bulk, take the per-row path.
         *
         * @return the rows decoded
         */
        private int decodeInRuns(CoveringRowCursor crc) {
            final int queryColCount = queryColCount();
            final int sentinelInclude = coveredIncludeIdx[runColumns[0]];
            boolean hasRowColumns = false;
            for (int q = 0; q < queryColCount; q++) {
                hasRowColumns |= isRowColumn[q] && coveredIncludeIdx[q] >= 0;
            }
            int count = 0;
            int runStartRow = 0;
            int runStartOrdinal = 0;
            int runLength = 0;
            long runBlockId = 0;
            while (crc.hasNext()) {
                crc.next();
                if (count >= rowCount) {
                    break;
                }
                final long blockId = crc.getCoveredBlockId(sentinelInclude);
                final int ordinal = crc.getCoveredBlockOrdinal();
                if (runLength > 0 && blockId == runBlockId && ordinal == runStartOrdinal + runLength) {
                    runLength++;
                } else {
                    // the run ends before the cursor decodes another block over the run's values
                    copyRun(runStartRow, runStartOrdinal, runLength);
                    runLength = 0;
                    if (blockId != 0 && captureRunBlocks(crc)) {
                        runStartRow = count;
                        runStartOrdinal = ordinal;
                        runLength = 1;
                        runBlockId = blockId;
                    } else {
                        CoveredColumnDecoder.writeCoveredRow(colAddr, this, count, crc, queryColCount, coveredIncludeIdx, columnTypeTags, columnTypes, isRunColumn);
                    }
                }
                if (hasRowColumns) {
                    CoveredColumnDecoder.writeCoveredRow(colAddr, this, count, crc, queryColCount, coveredIncludeIdx, columnTypeTags, columnTypes, isRowColumn);
                }
                count++;
                if (!hasRowColumns && runLength > 0 && count < rowCount) {
                    // the run's next rows, as many as the cursor can tell without decoding, are
                    // passed at once: they read the run's next values of the same block
                    final int more = crc.peekCoveredRun(rowCount - count);
                    if (more > 0 && crc.getCoveredRunOrdinal() == runStartOrdinal + runLength) {
                        crc.skipCoveredRun(more);
                        runLength += more;
                        count += more;
                    }
                }
            }
            copyRun(runStartRow, runStartOrdinal, runLength);
            return count;
        }

        // The blocks of the run columns for the current row, without decoding them: a compressed
        // block is decoded for the run's ordinals only when the run ends (copyRun()), so a frame
        // that reads a slice of a key's block decodes that slice, not the whole block. False when
        // one of them cannot be read in bulk, which sends the row to the per-row path.
        private boolean captureRunBlocks(CoveringRowCursor crc) {
            for (int i = 0, n = runColumnCount; i < n; i++) {
                final int q = runColumns[i];
                final long addr = crc.getCoveredBlockId(coveredIncludeIdx[q]);
                if (addr == 0) {
                    return false;
                }
                runBlockAddrs[i] = addr;
            }
            runBlocksCompressed = crc.isCoveredBlockCompressed();
            return true;
        }

        private void copyRun(int startRow, int startOrdinal, int length) {
            if (length == 0) {
                return;
            }
            for (int i = 0, n = runColumnCount; i < n; i++) {
                final int q = runColumns[i];
                final int size = columnSizeBytes[q];
                final long dest = colAddr[q] + (long) startRow * size;
                if (!runBlocksCompressed) {
                    Unsafe.copyMemory(runBlockAddrs[i] + (long) startOrdinal * size, dest, (long) length * size);
                    continue;
                }
                if (runDecodeWorkspace == 0) {
                    runDecodeWorkspace = Unsafe.malloc((long) RUN_DECODE_CHUNK * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                for (int done = 0; done < length; ) {
                    final int n1 = Math.min(RUN_DECODE_CHUNK, length - done);
                    CoveringCompressor.decompressRangeToAddr(columnTypes[q], runBlockAddrs[i], startOrdinal + done, n1, dest + (long) done * size, runDecodeWorkspace);
                    done += n1;
                }
            }
        }

        /**
         * Splits the selected covered columns into the fixed-width ones decoded in runs and the rest,
         * decoded per row. False when no selected column is fixed-width.
         */
        private boolean selectRunColumns() {
            final int queryColCount = queryColCount();
            if (isRunColumn == null || isRunColumn.length < queryColCount) {
                isRunColumn = new boolean[queryColCount];
                isRowColumn = new boolean[queryColCount];
                runColumns = new int[queryColCount];
                runBlockAddrs = new long[queryColCount];
            }
            runColumnCount = 0;
            for (int q = 0; q < queryColCount; q++) {
                final boolean isFixed = isColumnSelected[q] && coveredIncludeIdx[q] >= 0 && !ColumnType.isVarSize(columnTypes[q]);
                isRunColumn[q] = isFixed;
                isRowColumn[q] = isColumnSelected[q] && !isFixed;
                if (isFixed) {
                    runColumns[runColumnCount++] = q;
                }
            }
            return runColumnCount > 0;
        }

        private boolean hasFullColumns(IntHashSet columnIndexes) {
            if (isColumnFull == null) {
                return false;
            }
            for (int q = 0, n = queryColCount(); q < n; q++) {
                if (columnIndexes.contains(q) && coveredColumn[q] && !isColumnFull[q]) {
                    return false;
                }
            }
            return true;
        }

        private boolean hasSelectedColumns() {
            for (int q = 0, n = queryColCount(); q < n; q++) {
                if (isColumnSelected[q]) {
                    return true;
                }
            }
            return false;
        }

        private boolean isFullyDecoded() {
            if (isColumnFull == null) {
                return false;
            }
            for (int q = 0, n = queryColCount(); q < n; q++) {
                if (coveredColumn[q] && !isColumnFull[q]) {
                    return false;
                }
            }
            return true;
        }

        private void prepareSelectedColumns(
                int frameIndex,
                int rowCount,
                @Nullable IntHashSet columnIndexes,
                boolean isExcludedSet
        ) {
            ensureColumnMetadata(frameIndex, rowCount);
            for (int q = 0, n = queryColCount(); q < n; q++) {
                final boolean isRequested = columnIndexes == null
                        || isExcludedSet != columnIndexes.contains(q);
                isColumnSelected[q] = coveredColumn[q] && isRequested && !isColumnFull[q];
                if (isColumnSelected[q]) {
                    isColumnDecoded[q] = false;
                    varDataPos[q] = 0;
                    ensureColumnCapacity(q, rowCount);
                }
            }
        }

        private boolean populateRemainingColumns(IntHashSet skipColumnIndexes, DirectLongList filteredRows) {
            prepareSelectedColumns(frameIndex, rowCount, skipColumnIndexes, true);
            if (!hasSelectedColumns()) {
                return false;
            }
            decodeSelectedColumns(
                    addressCache.getCoveredIndexReader(frameIndex),
                    addressCache.getCoveredKey(frameIndex),
                    addressCache.getCoveredRowLo(frameIndex),
                    addressCache.getCoveredRowHi(frameIndex),
                    addressCache.getCoveredIncludeIndices(frameIndex),
                    filteredRows
            );
            for (int q = 0, n = queryColCount(); q < n; q++) {
                if (isColumnSelected[q]) {
                    isColumnDecoded[q] = true;
                    isColumnFull[q] = coveredIncludeIdx[q] < 0;
                }
            }
            publishAddresses(rowCount);
            return true;
        }

        @Override
        public long ensureCapacity(int q, int needed) {
            // Cumulative var-data position is int-addressed; compute the grow target in
            // long and guard against int overflow so a column whose accumulated var-data
            // approaches 2GB fails loud rather than wrapping the cap negative and reallocs
            // an under-sized buffer that the next write overruns.
            final long required = (long) varDataPos[q] + needed;
            if (required > varDataCap[q]) {
                final long newCapLong = Math.max(Math.max((long) varDataCap[q] * 2, required), 32);
                if (newCapLong > Integer.MAX_VALUE) {
                    throw CairoException.nonCritical()
                            .put("covered var-data column too large [bytes=").put(newCapLong).put(']');
                }
                final int newCap = (int) newCapLong;
                // the growth is charged to the query's memory limit, whose breach throws here
                varDataAddr[q] = reallocColumn(varDataAddr[q], varDataCap[q], newCap);
                varDataCap[q] = newCap;
            }
            return varDataAddr[q];
        }

        DirectLongList getAuxPageAddresses() {
            return auxPageAddresses;
        }

        DirectLongList getAuxPageSizes() {
            return auxPageSizes;
        }

        DirectLongList getPageAddresses() {
            return pageAddresses;
        }

        DirectLongList getPageSizes() {
            return pageSizes;
        }

        @Override
        public long position(int q) {
            return varDataPos[q];
        }

        private void ensureColumnCapacity(int q, int rowCount) {
            if (coveredIncludeIdx[q] < 0) {
                final long symBytes = (long) rowCount * Integer.BYTES;
                if (symBytes > Integer.MAX_VALUE) {
                    throw CairoException.critical(0).put("covered frame too large for int symCap [rows=").put(rowCount).put(']');
                }
                if (symCap < symBytes) {
                    symAddr = growNative(symAddr, symCap, symBytes);
                    symCap = (int) symBytes;
                }
                return;
            }

            final long auxBytes;
            final int initDataCap;
            switch (columnTypeTags[q]) {
                case ColumnType.VARCHAR -> {
                    auxBytes = (long) rowCount * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                    initDataCap = rowCount * 32;
                }
                case ColumnType.STRING, ColumnType.BINARY -> {
                    auxBytes = (long) (rowCount + 1) * Long.BYTES;
                    initDataCap = rowCount * 32;
                }
                case ColumnType.ARRAY -> {
                    auxBytes = (long) rowCount * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
                    initDataCap = rowCount * 32;
                }
                default -> {
                    auxBytes = (long) rowCount * columnSizeBytes[q];
                    initDataCap = 0;
                }
            }
            if (colCap[q] < auxBytes) {
                colAddr[q] = growNative(colAddr[q], colCap[q], auxBytes);
                colCap[q] = auxBytes;
            }
            if (initDataCap > 0 && varDataCap[q] < initDataCap) {
                varDataAddr[q] = growNative(varDataAddr[q], varDataCap[q], initDataCap);
                varDataCap[q] = initDataCap;
            }
        }

        private void ensureColumnMetadata(int frameIndex, int rowCount) {
            final int queryColCount = queryColCount();
            if (colAddr != null && isMetadataStale && colAddr.length != queryColCount) {
                // a shell of a query with other columns; its column buffers are freed
                colAddr = null;
            }
            if (colAddr == null || isMetadataStale) {
                if (colAddr == null) {
                    colAddr = new long[queryColCount];
                    colCap = new long[queryColCount];
                    varDataAddr = new long[queryColCount];
                    varDataCap = new int[queryColCount];
                    varDataPos = new int[queryColCount];
                    columnTypes = new int[queryColCount];
                    columnTypeTags = new int[queryColCount];
                    columnSizeBytes = new int[queryColCount];
                    coveredIncludeIdx = new int[queryColCount];
                    coveredColumn = new boolean[queryColCount];
                    isColumnDecoded = new boolean[queryColCount];
                    isColumnFull = new boolean[queryColCount];
                    isColumnSelected = new boolean[queryColCount];
                } else {
                    // a recycled shell: the column buffers are freed (colAddr, colCap, varDataAddr,
                    // varDataCap are 0), the decode state goes back to a fresh buffer's
                    Arrays.fill(varDataPos, 0);
                    Arrays.fill(isColumnDecoded, false);
                    Arrays.fill(isColumnFull, false);
                    Arrays.fill(isColumnSelected, false);
                }
                isMetadataStale = false;
                final IntList types = addressCache.getColumnTypes();
                for (int q = 0; q < queryColCount; q++) {
                    final int type = types.getQuick(q);
                    columnTypes[q] = type;
                    columnTypeTags[q] = ColumnType.tagOf(type);
                    columnSizeBytes[q] = ColumnType.sizeOf(type);
                    coveredIncludeIdx[q] = addressCache.getCoveredIncludeIndex(frameIndex, q);
                    coveredColumn[q] = addressCache.isColumnCovered(frameIndex, q);
                }
            }
            if ((long) rowCount * 32 > Integer.MAX_VALUE) {
                throw CairoException.critical(0).put("covered frame too large for int varDataCap init [rows=").put(rowCount).put(']');
            }
            this.frameIndex = frameIndex;
            this.rowCount = rowCount;
        }

        /**
         * The tracker the live column buffers charge: the pool's tracker of the time the first of
         * them was allocated, while its covered ledger has not been reconciled since, see
         * {@link MemoryTracker#getCoveredGeneration()}; null otherwise, and when the pool had no
         * tracker. A reconciled tracker has released the buffers' charge already, and may serve
         * another query by now, so their free is then global-only. A pool frees its covered
         * buffers while their query runs, so that the query's memory limit counts the frames it
         * holds, not every frame it has decoded; only a pool that its owner releases after the
         * query ends frees on global-only accounting.
         */
        private MemoryTracker chargedTracker() {
            if (allocatedBytes == 0) {
                // nothing is charged yet: the next allocation charges the pool's tracker of now
                chargedTracker = memoryTracker;
                chargedGeneration = memoryTracker != null ? memoryTracker.getCoveredGeneration() : 0;
            }
            final MemoryTracker tracker = chargedTracker;
            return tracker != null && tracker.getCoveredGeneration() == chargedGeneration ? tracker : null;
        }

        private void freeColumnBuffers() {
            final MemoryTracker tracker = chargedTracker();
            long freedBytes = 0;
            if (colAddr != null) {
                for (int q = 0, n = colAddr.length; q < n; q++) {
                    if (colAddr[q] != 0) {
                        freeColumn(colAddr[q], colCap[q], tracker);
                        freedBytes += colCap[q];
                        colAddr[q] = 0;
                        colCap[q] = 0;
                    }
                    if (varDataAddr[q] != 0) {
                        freeColumn(varDataAddr[q], varDataCap[q], tracker);
                        freedBytes += varDataCap[q];
                        varDataAddr[q] = 0;
                        varDataCap[q] = 0;
                    }
                }
            }
            if (symAddr != 0) {
                freeColumn(symAddr, symCap, tracker);
                freedBytes += symCap;
                symAddr = 0;
                symCap = 0;
            }
            assert freedBytes == allocatedBytes : "covered buffer accounting [freed=" + freedBytes + ", allocated=" + allocatedBytes + ']';
            coveredCachedBytes -= freedBytes;
            allocatedBytes = 0;
            if (tracker != null) {
                tracker.addCoveredBytes(-freedBytes);
            }
            chargedTracker = null;
        }

        private void freeColumn(long addr, long bytes, @Nullable MemoryTracker tracker) {
            if (isPoisoningFreedCoveredBuffers) {
                Vect.memset(addr, bytes, POISON_BYTE);
            }
            Unsafe.free(addr, bytes, MemoryTag.NATIVE_INDEX_READER, tracker);
        }

        // Grow a native buffer to newBytes when it is too small, preserving no
        // content (each decode rewrites every row). Reuses the existing allocation
        // when it already covers newBytes so a steady-state frame size never
        // reallocs. oldBytes == 0 means unallocated.
        private long growNative(long addr, long oldBytes, long newBytes) {
            if (newBytes <= 0) {
                return addr;
            }
            if (addr != 0 && oldBytes >= newBytes) {
                return addr;
            }
            return reallocColumn(addr, oldBytes, newBytes);
        }

        // Reallocates a column buffer, charging the growth to the query's memory limit, whose
        // breach throws and leaves the buffer as it was, see chargedTracker(). The charge goes to
        // the tracker's covered ledger as well, which keeps a pooled tracker clean should the
        // buffers outlive their query, see MemoryTracker.addCoveredBytes().
        private long reallocColumn(long addr, long oldBytes, long newBytes) {
            final MemoryTracker tracker = chargedTracker();
            final long result = Unsafe.realloc(addr, oldBytes, newBytes, MemoryTag.NATIVE_INDEX_READER, tracker);
            final long delta = newBytes - oldBytes;
            allocatedBytes += delta;
            coveredCachedBytes += delta;
            if (tracker != null) {
                tracker.addCoveredBytes(delta);
            }
            return result;
        }

        private int[] selectIncludeIndices(int[] includeIndices) {
            int selectedCount = 0;
            for (int includeIndex : includeIndices) {
                for (int q = 0, n = queryColCount(); q < n; q++) {
                    if (isColumnSelected[q] && coveredIncludeIdx[q] == includeIndex) {
                        selectedCount++;
                        break;
                    }
                }
            }
            if (selectedCount == includeIndices.length) {
                return includeIndices;
            }
            int[] selectedIncludeIndices = coveringIncludeIndexScratch.getQuiet(selectedCount);
            if (selectedIncludeIndices == null) {
                selectedIncludeIndices = new int[selectedCount];
                coveringIncludeIndexScratch.extendAndSet(selectedCount, selectedIncludeIndices);
            }
            int selectedIndex = 0;
            for (int includeIndex : includeIndices) {
                for (int q = 0, n = queryColCount(); q < n; q++) {
                    if (isColumnSelected[q] && coveredIncludeIdx[q] == includeIndex) {
                        selectedIncludeIndices[selectedIndex++] = includeIndex;
                        break;
                    }
                }
            }
            return selectedIncludeIndices;
        }

        // writeSkippedVarOffsets(int) for the rows [lo, hi)
        private void writeSkippedVarOffsets(int lo, int hi) {
            for (int q = 0, n = queryColCount(); q < n; q++) {
                if (isColumnSelected[q]
                        && (columnTypeTags[q] == ColumnType.STRING || columnTypeTags[q] == ColumnType.BINARY)) {
                    final long pos = varDataPos[q];
                    for (int row = lo; row < hi; row++) {
                        Unsafe.putLong(colAddr[q] + (long) row * Long.BYTES, pos);
                    }
                }
            }
        }

        private void writeSkippedVarOffsets(int row) {
            for (int q = 0, n = queryColCount(); q < n; q++) {
                if (isColumnSelected[q]
                        && (columnTypeTags[q] == ColumnType.STRING || columnTypeTags[q] == ColumnType.BINARY)) {
                    Unsafe.putLong(colAddr[q] + (long) row * Long.BYTES, varDataPos[q]);
                }
            }
        }

        // Fan the decoded native buffers out to the four published per-column
        // address/size lists, exactly mirroring CoveringPageFrameCursor#finalizeFrame
        // so a downstream native reader sees the same layout the eager production
        // path produces. Covered var-size columns publish aux+data (with the
        // STRING/BINARY trailing sentinel offset); covered fixed-width columns
        // publish data only; the symbol key column publishes the broadcast ints.
        private void publishAddresses(int count) {
            final int queryColCount = queryColCount();
            ensureListCapacity(pageAddresses, queryColCount);
            ensureListCapacity(pageSizes, queryColCount);
            ensureListCapacity(auxPageAddresses, queryColCount);
            ensureListCapacity(auxPageSizes, queryColCount);
            for (int q = 0; q < queryColCount; q++) {
                final int includeIdx = coveredIncludeIdx[q];
                if (!coveredColumn[q] || !isColumnDecoded[q]) {
                    // Non-covered or not-yet-decoded columns publish NULL addresses. The
                    // filter-first pass never exposes omitted columns to the filter.
                    pageAddresses.set(q, 0);
                    pageSizes.set(q, 0);
                    auxPageAddresses.set(q, 0);
                    auxPageSizes.set(q, 0);
                    continue;
                }
                if (includeIdx < 0) {
                    // Indexed symbol key column (covered: synthesized broadcast).
                    pageAddresses.set(q, symAddr);
                    pageSizes.set(q, (long) count * Integer.BYTES);
                    auxPageAddresses.set(q, 0);
                    auxPageSizes.set(q, 0);
                    continue;
                }
                switch (columnTypeTags[q]) {
                    case ColumnType.VARCHAR -> {
                        auxPageAddresses.set(q, colAddr[q]);
                        auxPageSizes.set(q, (long) count * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES);
                        pageAddresses.set(q, varDataAddr[q]);
                        pageSizes.set(q, varDataPos[q]);
                    }
                    case ColumnType.STRING, ColumnType.BINARY -> {
                        // Trailing sentinel offset at slot [count]. Guard colAddr[q] != 0:
                        // covered frames currently always have rowCount >= 1 so the buffer
                        // is always allocated, but the check makes this robust against a
                        // hypothetical zero-row covered frame (which would leave colAddr[q] == 0).
                        if (colAddr[q] != 0) {
                            Unsafe.putLong(colAddr[q] + (long) count * Long.BYTES, varDataPos[q]);
                        }
                        auxPageAddresses.set(q, colAddr[q]);
                        auxPageSizes.set(q, (long) (count + 1) * Long.BYTES);
                        pageAddresses.set(q, varDataAddr[q]);
                        pageSizes.set(q, varDataPos[q]);
                    }
                    case ColumnType.ARRAY -> {
                        auxPageAddresses.set(q, colAddr[q]);
                        auxPageSizes.set(q, (long) count * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES);
                        pageAddresses.set(q, varDataAddr[q]);
                        pageSizes.set(q, varDataPos[q]);
                    }
                    default -> {
                        pageAddresses.set(q, colAddr[q]);
                        pageSizes.set(q, (long) count * columnSizeBytes[q]);
                        auxPageAddresses.set(q, 0);
                        auxPageSizes.set(q, 0);
                    }
                }
            }
        }

        private void ensureListCapacity(DirectLongList list, int size) {
            list.setCapacity(size);
            list.zero();
            list.setPos(size);
        }

        private int queryColCount() {
            return addressCache.getColumnCount();
        }
    }

    private class PageFrameMemoryImpl implements PageFrameMemory, Mutable {
        private DirectLongList auxPageAddresses;
        private DirectLongList auxPageSizes;
        private int columnOffset;
        private DirectLongList columnTops;
        // the buffers of the single-key covered frame the memory is bound to, or null
        private CoveringBuffers coveringBuffers;
        private ParquetBuffers currentRowGroupBuffer;
        // The columns a late-materialized Parquet decode covered (navigateTo(int, IntHashSet)),
        // or null when every column has been decoded, including after populateRemainingColumns()
        // has decoded the rest for the filtered rows. See hasColumnTops().
        private IntHashSet decodedColumnIndexes;
        private byte frameFormat = -1;
        private int frameIndex = -1;
        private DirectLongList pageAddresses;
        private DirectLongList pageSizes;

        @Override
        public void clear() {
            frameIndex = -1;
            frameFormat = -1;
            columnOffset = 0;
            pageAddresses = null;
            auxPageAddresses = null;
            pageSizes = null;
            auxPageSizes = null;
            columnTops = null;
            coveringBuffers = null;
            currentRowGroupBuffer = null;
            decodedColumnIndexes = null;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return auxPageAddresses.get(columnOffset + columnIndex);
        }

        @Override
        public boolean isStable() {
            // a covered frame's buffers live until the pool releases the frame
            return frameFormat == PartitionFormat.NATIVE && coveringBuffers == null;
        }

        @Override
        public DirectLongList getAuxPageAddresses() {
            return auxPageAddresses;
        }

        @Override
        public DirectLongList getAuxPageSizes() {
            return auxPageSizes;
        }

        @Override
        public int getColumnCount() {
            return addressCache.getColumnCount();
        }

        @Override
        public int getColumnOffset() {
            return columnOffset;
        }

        @Override
        public DirectLongList getColumnTops() {
            return columnTops;
        }

        @Override
        public byte getFrameFormat() {
            return frameFormat;
        }

        @Override
        public int getFrameIndex() {
            return frameIndex;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return pageAddresses.get(columnOffset + columnIndex);
        }

        @Override
        public DirectLongList getPageAddresses() {
            return pageAddresses;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return pageSizes.get(columnOffset + columnIndex);
        }

        @Override
        public DirectLongList getPageSizes() {
            return pageSizes;
        }

        @Override
        public PageFrameMemoryPool getPool() {
            return PageFrameMemoryPool.this;
        }

        @Override
        public long getRowIdOffset() {
            return addressCache.getRowIdOffset(frameIndex);
        }

        @Override
        public int getSourceColumnType(int columnIndex) {
            if (frameFormat == PartitionFormat.PARQUET && hasTypeCasts) {
                return sourceColumnTypes.getQuick(columnIndex);
            }
            return -1;
        }

        @Override
        public boolean hasColumnTops() {
            if (addressCache.isFrameSingleKeyCovered(frameIndex)) {
                // Zero addresses can mean intentionally deferred columns on a covered
                // filter-first frame, not table column tops.
                return false;
            }
            final IntHashSet decoded = decodedColumnIndexes;
            if (decoded != null) {
                // A late-materialized Parquet frame: the columns outside the filter's set are
                // not decoded yet, and their zero addresses are not column tops. The callers ask
                // before running a compiled filter, which reads the filter's columns only, so
                // judging every column here would send every such frame to the Java filter.
                for (int j = 0, n = decoded.size(); j < n; j++) {
                    if (isColumnTop(decoded.get(j))) {
                        return true;
                    }
                }
                return false;
            }
            for (int i = 0, n = addressCache.getColumnCount(); i < n; i++) {
                if (isColumnTop(i)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean hasColumnTypeCasts() {
            return frameFormat == PartitionFormat.PARQUET && hasTypeCasts;
        }

        private boolean isColumnTop(int columnIndex) {
            // VARCHAR column that contains short strings will have zero data vector,
            // so for such columns we also need to check that the aux (index) vector is zero.
            return pageAddresses.get(columnOffset + columnIndex) == 0 && auxPageAddresses.get(columnOffset + columnIndex) == 0;
        }

        @Override
        public boolean populateRemainingColumns(IntHashSet filterColumnIndexes, DirectLongList filteredRows, boolean fillWithNulls) {
            if (addressCache.isFrameSingleKeyCovered(frameIndex)) {
                // Raw async filters use full frame coordinates. Compact-layout async
                // aggregate/join consumers remain outside the covering MVP.
                if (!fillWithNulls || filteredRows.size() == 0) {
                    return false;
                }
                // the frame memory on a covered frame is bound to its buffers
                assert coveringBuffers != null && coveringBuffers.cacheFrameIndex == frameIndex;
                return coveringBuffers.populateRemainingColumns(filterColumnIndexes, filteredRows);
            }
            assert frameFormat == PartitionFormat.PARQUET;
            final ParquetBuffers buffers = currentRowGroupBuffer;
            if (buffers == null || buffers.columnSubset == null) {
                // A full decode (which the subset overload serves as is) already holds every
                // column for every row, and an empty window holds none to read. Populating a full
                // decode for the filtered rows only would hollow it out, and the windowed overload
                // would then serve it as a full frame.
                assert decodedColumnIndexes == null;
                return false;
            }
            if (buffers.isPopulated) {
                // The subset overload re-decodes a populated buffer, so only a second call on one
                // binding gets here. Its remaining columns hold the first call's filtered rows, in
                // the first call's layout; neither "populated" nor "not populated" would be true.
                throw CairoException.critical(0)
                        .put("late-materialized frame populated twice [frameIndex=").put(frameIndex).put(']');
            }
            if (filterColumnIndexes.size() == addressCache.getColumnCount()) {
                decodedColumnIndexes = null;
                return false;
            }

            openParquet(frameIndex, filterColumnIndexes, false);
            final int rowGroupIndex = addressCache.getParquetRowGroup(frameIndex);
            final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
            final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
            if (filteredRows.size() != 0) {
                final long extra;
                try {
                    extra = currentRowGroupBuffer.decodeRemainingColumns(
                            activeDecoder,
                            filterColumnIndexes,
                            parquetColumns,
                            rowGroupIndex,
                            rowGroupLo,
                            rowGroupHi,
                            filteredRows,
                            fillWithNulls
                    );
                } catch (Throwable th) {
                    evictHalfInitialized(currentRowGroupBuffer);
                    clear();
                    throw th;
                }
                currentRowGroupBuffer.decodedBytes += extra;
                currentRowGroupBuffer.isPopulated = true;
                currentRowGroupBuffer.isCompacted = !fillWithNulls;
                accountDecode(currentRowGroupBuffer);
                // The remaining columns now have addresses, so hasColumnTops() judges every column
                // again. The buffer keeps its columnSubset: outside the filtered rows those columns
                // hold no data, so it still cannot serve a full-frame navigateTo().
                decodedColumnIndexes = null;
                return true;
            }
            return false;
        }
    }

    private class ParquetBuffers implements QuietCloseable {
        private final DirectLongList auxPageAddresses;
        private final DirectLongList auxPageSizes;
        // Per-query-column leading column-top count, parallel to pageAddresses. Lets a lazy
        // fixed->var conversion surface NULL for column-top rows (decoded as an in-band 0).
        private final DirectLongList columnTops;
        private final DirectLongList decodeResources;
        private final DirectLongList pageAddresses;
        private final DirectLongList pageSizes;
        private final RowGroupBuffers rowGroupBuffers;
        // Per decode slot, the peak data and aux bytes (two entries per slot) the slot has held since
        // the native buffers were last freed. Each slot's Rust vectors keep their peak capacity, and a
        // subset re-decode maps other columns to the slots, so the retained footprint is the sum of
        // the per-slot peaks, not the bytes of the latest decode.
        private final LongList slotPeakBytes = new LongList();
        // Non-null when a late-materialized navigateTo(int, IntHashSet) decoded only these query
        // columns, always over the whole frame. Such a buffer never serves a full-frame navigateTo(),
        // not even after populateRemainingColumns(), which decodes the other columns for the
        // filtered rows only. The set is the caller's, a factory-lifetime set that is never mutated,
        // and is compared by content (see containsAll()).
        private IntHashSet columnSubset;
        private long decodedBytes;
        // decoded window bounds (row group coordinates); a cached buffer serves a
        // request only when its window covers the requested [lo, hi)
        private int decodedRowHi = -1;
        private int decodedRowLo = -1;
        private int frameIndex = -1;
        // With isPopulated, populateRemainingColumns() stored the remaining columns in filtered-row
        // order (fillWithNulls false), so they cannot be read by frame row index.
        private boolean isCompacted;
        // populateRemainingColumns() has decoded the columns outside columnSubset for one caller's
        // filtered rows. The record path reuses the buffer only when they are in row order.
        private boolean isPopulated;
        private boolean isRowFiltered;
        private ParquetBuffers next;
        private ParquetBuffers prev;
        // Peak decodedBytes since the native buffers were last freed. In-place
        // reuse keeps the Rust Vec capacities at this watermark, so the budget
        // accounts it rather than the current chunk's logical size.
        private long retainedBytes;
        private int slotCount;
        private byte usageFlags;

        public ParquetBuffers() {
            // Each buffer below allocates native memory eagerly. If any allocation
            // throws (native OOM or RSS limit exceeded), free the ones already
            // allocated so the half-built object does not leak: acquireBuffer never
            // assigns the throwing ctor to a tracked reference, so close() would
            // never reach it.
            DirectLongList auxPageAddresses = null;
            DirectLongList auxPageSizes = null;
            DirectLongList columnTops = null;
            DirectLongList pageAddresses = null;
            DirectLongList decodeResources = null;
            DirectLongList pageSizes = null;
            RowGroupBuffers rowGroupBuffers;
            try {
                auxPageAddresses = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                auxPageSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                columnTops = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                pageAddresses = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                pageSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                // keepClosed: defer the native buffer allocation to the first
                // reopen(), which binds the pool's per-query tracker (see
                // acquireBuffer) before RowGroupBuffers.create() captures the
                // native allocator. The other DirectLongLists above are tiny and
                // tracker-agnostic, so they stay eager.
                rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER, true);
                decodeResources = new DirectLongList(2, MemoryTag.NATIVE_DEFAULT);
            } catch (Throwable th) {
                Misc.free(auxPageAddresses);
                Misc.free(auxPageSizes);
                Misc.free(columnTops);
                Misc.free(pageAddresses);
                Misc.free(decodeResources);
                Misc.free(pageSizes);
                throw th;
            }
            this.auxPageAddresses = auxPageAddresses;
            this.auxPageSizes = auxPageSizes;
            this.columnTops = columnTops;
            this.pageAddresses = pageAddresses;
            this.pageSizes = pageSizes;
            this.rowGroupBuffers = rowGroupBuffers;
            this.decodeResources = decodeResources;
        }

        @Override
        public void close() {
            releaseDecodeResources();
            Misc.free(decodeResources);
            Misc.free(pageAddresses);
            Misc.free(pageSizes);
            Misc.free(auxPageAddresses);
            Misc.free(auxPageSizes);
            Misc.free(columnTops);
            Misc.free(rowGroupBuffers);
            slotCount = 0;
            usageFlags = 0;
            frameIndex = -1;
            decodedBytes = 0;
            decodedRowHi = -1;
            decodedRowLo = -1;
            retainedBytes = 0;
            slotPeakBytes.clear();
            isRowFiltered = false;
            columnSubset = null;
            isPopulated = false;
            isCompacted = false;
            // releaseParquetBuffers() parks closed shells without unlinking first; drop the
            // LRU links so a pooled shell cannot retain its former neighbours.
            prev = null;
            next = null;
        }

        public void decode(ParquetDecoder decoder, DirectIntList parquetColumns, int rowGroup, int rowLo, int rowHi, int frameRowLo) {
            // This buffer is being repurposed for a new frame; drop the prior frame's pins.
            releaseDecodeResources();
            clearAddresses();
            if (parquetColumns.size() > 0) {
                decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroup, rowLo, rowHi);
                retainDecodeResource(decoder);
                slotCount = (int) (parquetColumns.size() / 2);
            } else {
                slotCount = 0;
            }
            decodedBytes = retainedFootprint(0);
            decodedRowLo = rowLo;
            decodedRowHi = rowHi;
            isRowFiltered = false;
            columnSubset = null;
            isPopulated = false;
            isCompacted = false;
            remapColumns(frameRowLo);
        }

        public long decodeRemainingColumns(
                ParquetDecoder decoder,
                IntHashSet filterColumnIndexes,
                DirectIntList parquetColumns,
                int rowGroup,
                int rowLo,
                int rowHi,
                DirectLongList filteredRows,
                boolean fillWithNulls
        ) {
            if (parquetColumns.size() == 0) {
                return 0;
            }
            final int columnOffset = slotCount;
            if (fillWithNulls) {
                decoder.decodeRowGroupWithRowFilterFillNulls(rowGroupBuffers, columnOffset, parquetColumns, rowGroup, rowLo, rowHi, filteredRows);
            } else {
                decoder.decodeRowGroupWithRowFilter(rowGroupBuffers, columnOffset, parquetColumns, rowGroup, rowLo, rowHi, filteredRows);
            }
            final int extraSlots = (int) (parquetColumns.size() / 2);
            if (extraSlots > 0) {
                slotCount += extraSlots;
            }
            final long extra = retainedFootprint(columnOffset) - decodedBytes;
            retainDecodeResource(decoder);
            remapRemainingColumns(columnOffset, filterColumnIndexes);
            return extra;
        }

        public void decodeRowFiltered(
                ParquetDecoder decoder,
                DirectIntList parquetColumns,
                int rowGroup,
                int rowLo,
                int rowHi,
                long localRowsAddr,
                long localRowCount
        ) {
            // This buffer is being repurposed for a new frame; drop the prior frame's pins.
            releaseDecodeResources();
            clearAddresses();
            if (parquetColumns.size() > 0) {
                decoder.decodeRowGroupWithRowFilterFillNulls(rowGroupBuffers, 0, parquetColumns, rowGroup, rowLo, rowHi, localRowsAddr, localRowCount);
                retainDecodeResource(decoder);
                slotCount = (int) (parquetColumns.size() / 2);
            } else {
                slotCount = 0;
            }
            decodedBytes = retainedFootprint(0);
            // The buffer's addresses span the full [rowLo, rowHi) range (undeclared
            // rows read as NULLs), so the record path's window-coverage check must
            // not trigger a full re-decode; the frame paths reject row-filtered
            // buffers via isRowFiltered before consulting the window.
            decodedRowLo = rowLo;
            decodedRowHi = rowHi;
            isRowFiltered = true;
            columnSubset = null;
            isPopulated = false;
            isCompacted = false;
            remapColumns(0);
        }

        public long getSlotAuxPtr(int slot) {
            return rowGroupBuffers.getChunkAuxPtr(slot);
        }

        public long getSlotAuxSize(int slot) {
            return rowGroupBuffers.getChunkAuxSize(slot);
        }

        public long getSlotDataPtr(int slot) {
            return rowGroupBuffers.getChunkDataPtr(slot);
        }

        public long getSlotDataSize(int slot) {
            return rowGroupBuffers.getChunkDataSize(slot);
        }

        public void reopen() {
            pageAddresses.reopen();
            pageSizes.reopen();
            auxPageAddresses.reopen();
            auxPageSizes.reopen();
            columnTops.reopen();
            // Bind the pool's per-query tracker before the lazy create() inside
            // reopen() captures the native allocator into the Rust struct.
            rowGroupBuffers.setMemoryTracker(memoryTracker);
            decodeResources.reopen();
            rowGroupBuffers.reopen();
        }

        // Raises the peaks of the slots [fromSlot, slotCount) the decode just wrote and returns the
        // buffer's retained footprint: every slot's peak data and aux bytes, plus the page buffers
        // the current chunks hold (those are parked or freed on the next decode, not kept per slot).
        private long retainedFootprint(int fromSlot) {
            if (!isAccountingEnabled()) {
                return 0;
            }
            while (slotPeakBytes.size() < 2 * slotCount) {
                slotPeakBytes.add(0);
            }
            for (int s = fromSlot; s < slotCount; s++) {
                final int i = 2 * s;
                slotPeakBytes.setQuick(i, Math.max(slotPeakBytes.getQuick(i), rowGroupBuffers.getChunkDataSize(s)));
                slotPeakBytes.setQuick(i + 1, Math.max(slotPeakBytes.getQuick(i + 1), rowGroupBuffers.getChunkAuxSize(s)));
            }
            long total = 0;
            for (int i = 0, n = slotPeakBytes.size(); i < n; i++) {
                total += slotPeakBytes.getQuick(i);
            }
            for (int s = 0; s < slotCount; s++) {
                total += rowGroupBuffers.getChunkPageBuffersSize(s);
            }
            return total;
        }

        private void clearAddresses() {
            pageAddresses.clear();
            pageSizes.clear();
            auxPageAddresses.clear();
            auxPageSizes.clear();
            columnTops.clear();
        }

        private void ensureCapacityAndZero(DirectLongList list, int size) {
            list.setCapacity(size);
            list.zero();
            list.setPos(size);
        }

        // Releases the chunk leases this buffer holds via the remote-aware decoder
        // (a no-op for the legacy/OSS decoders, which hold no per-decode resource).
        private void releaseDecodeResources() {
            for (long i = 0, n = decodeResources.size(); i < n; i++) {
                parquetMetaDecoder.releaseDecodeResource(decodeResources.get(i));
            }
            decodeResources.clear();
        }

        // Fan the decoded buffers out to query columns. parquetColumns is
        // deduplicated, so when several query columns reference the same
        // parquet column they share one decode slot and copy the same
        // address pair into their respective query slots.
        //
        // frameRowLo > 0 means the decode dropped the first frameRowLo frame rows,
        // yet readers keep addressing rows by absolute frame-relative index. The
        // remap shifts the published base pointer back by frameRowLo entries so that
        // index arithmetic lands on the right decoded row, and grows the published
        // size by the same amount so addr+size still marks the buffer end. Fixed-size
        // columns shift the data base pointer; var-size columns shift only the aux
        // base pointer -- the data base is left as is because the offset values
        // stored inside the aux entries (or absolute pointers, for varchar slices)
        // index the compacted data and need no adjustment.
        private void remapColumns(int frameRowLo) {
            final int columnCount = addressCache.getColumnCount();
            if (columnCount == 0) {
                // The query reads no columns (e.g. count(*)). clearAddresses() already
                // left the lists empty, which is the correct state; there is nothing to
                // remap. Sizing them to 0 would trip DirectLongList.setCapacity()'s
                // assert capacity > 0.
                return;
            }
            ensureCapacityAndZero(pageAddresses, columnCount);
            ensureCapacityAndZero(pageSizes, columnCount);
            ensureCapacityAndZero(auxPageAddresses, columnCount);
            ensureCapacityAndZero(auxPageSizes, columnCount);
            ensureCapacityAndZero(columnTops, columnCount);

            if (parquetColumns.size() == 0) {
                // No parquet column was decoded (every projected column was added
                // after this partition became parquet). openParquet() only adds a
                // column to parquetColumns when columnIdToParquetIdx maps it to a
                // present parquet column, so an empty parquetColumns means every
                // column below would resolve to parquetIdx < 0 and continue. The
                // zeroing above already left them all at address 0 (NULL), so skip
                // the dead remap loop.
                return;
            }

            final int readParquetColumnCount = queryToSlot.size();
            for (int q = 0; q < readParquetColumnCount; q++) {
                final int slot = queryToSlot.getQuick(q);
                if (slot < 0) {
                    // ADD COLUMN / outside the projection / not part of this decode pass: stays at
                    // address 0 (NULL).
                    continue;
                }
                // Use the decode type rather than the target type: type-converted
                // columns may decode as a fixed source even though the target is
                // var-size (or vice versa), and the aux pointers (and the row stride
                // for the frameRowLo shift) only exist on the side the rust decoder
                // actually produced.
                final int decodeType = parquetColumns.get(2L * slot + 1);
                long dataAddr = getSlotDataPtr(slot);
                long dataSize = getSlotDataSize(slot);
                columnTops.set(q, rowGroupBuffers.getChunkColumnTop(slot));
                if (ColumnType.isVarSize(decodeType)) {
                    long auxAddr = getSlotAuxPtr(slot);
                    long auxSize = getSlotAuxSize(slot);
                    if (frameRowLo > 0 && auxAddr != 0) {
                        final long auxShift = ColumnType.getDriver(decodeType).getAuxVectorOffset(frameRowLo);
                        auxAddr -= auxShift;
                        auxSize += auxShift;
                    }
                    auxPageAddresses.set(q, auxAddr);
                    auxPageSizes.set(q, auxSize);
                } else if (frameRowLo > 0 && dataAddr != 0) {
                    final long dataShift = (long) frameRowLo << ColumnType.pow2SizeOf(decodeType);
                    dataAddr -= dataShift;
                    dataSize += dataShift;
                }
                pageAddresses.set(q, dataAddr);
                pageSizes.set(q, dataSize);
            }
        }

        private void remapRemainingColumns(int columnOffset, IntHashSet filterColumnIndexes) {
            final int readParquetColumnCount = queryToSlot.size();
            for (int q = 0; q < readParquetColumnCount; q++) {
                // Filter columns hold full data read by absolute index; never overwrite
                // them with the compacted buffer when a remaining column shares their
                // parquet column. Guard only: the optimizer keeps filters below
                // duplicating projections, so the late-mat frame has no duplicate today.
                if (filterColumnIndexes.contains(q)) {
                    continue;
                }
                final int slot = queryToSlot.getQuick(q);
                if (slot < 0) {
                    continue; // Excluded from this decode pass; the previous decode set its address.
                }
                final int decodeType = parquetColumns.get(2L * slot + 1);
                pageAddresses.set(q, rowGroupBuffers.getChunkDataPtr(columnOffset + slot));
                pageSizes.set(q, rowGroupBuffers.getChunkDataSize(columnOffset + slot));
                columnTops.set(q, rowGroupBuffers.getChunkColumnTop(columnOffset + slot));
                if (ColumnType.isVarSize(decodeType)) {
                    auxPageAddresses.set(q, rowGroupBuffers.getChunkAuxPtr(columnOffset + slot));
                    auxPageSizes.set(q, rowGroupBuffers.getChunkAuxSize(columnOffset + slot));
                }
            }
        }

        // Takes ownership of the lease the just-completed decode acquired, if any.
        private void retainDecodeResource(ParquetDecoder decoder) {
            final long resource = decoder.takeDecodeResource();
            if (resource != 0) {
                decodeResources.add(resource);
            }
        }
    }
}

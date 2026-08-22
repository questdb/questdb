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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
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
 * {@link ParquetBuffers} in place: it resets only the logical state and
 * leaves all native memory alive so the upcoming decode overwrites it via
 * the Rust {@code ColumnChunkBuffers::reset()} path, growing each
 * {@code Vec} via realloc only when the new chunk exceeds the buffer's
 * historical peak. Because reuse keeps that peak allocated, each entry is
 * accounted at {@code retainedBytes} - the largest decode it has held - and
 * after every decode {@link #trimToBudget} closes LRU-oldest unpinned
 * entries until the total drops back under the budget. Entries currently
 * bound to a record or to the frame-memory flyweight are skipped during
 * victim selection and trimming, so when every cached entry is pinned the
 * pool creates a new buffer and the budget is temporarily exceeded.
 * <p>
 * The access-pattern hint declared by the enclosing factory scales the
 * effective ceiling: {@link ParquetDecodeHint#MONOTONIC} cursors get a quarter
 * of the configured budget, {@link ParquetDecodeHint#SCATTERED} cursors get all
 * of it. Hints come in via {@link #of(PageFrameAddressCache, ParquetDecodeHint)}
 * or {@link #setParquetDecodeHint(ParquetDecodeHint)} and default to MONOTONIC.
 * <p>
 * This pool is thread-unsafe as it may hold navigated Parquet partition data,
 * so it shouldn't be shared between multiple threads.
 */
public class PageFrameMemoryPool implements RecordRandomAccess, QuietCloseable, Mutable {
    private static final byte FRAME_MEMORY_MASK = 1 << 2;
    private static final byte RECORD_A_MASK = 1;
    private static final byte RECORD_B_MASK = 1 << 1;
    private static final int SHELL_POOL_CAP = 256;
    // O(1) frameIndex lookup. LRU order is tracked separately via the
    // intrusive lruHead/lruTail doubly linked list through ParquetBuffers.
    private final IntObjHashMap<ParquetBuffers> byFrameIndex;
    // Maps column ID (field_id / writer index) to parquet column index.
    // Rebuilt each time openParquet() encounters a new file.
    private final IntIntHashMap columnIdToParquetIdx;
    private final CairoConfiguration configuration;
    private final IntList declaredFrameRowCounts = new IntList(16);
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
    // Per-column source type tag for fixed-to-var type-cast columns.
    // Indexed by query column index; -1 means no type cast.
    private final IntList sourceColumnTypes;
    private ParquetDecoder activeDecoder;
    private PageFrameAddressCache addressCache;
    // Per-worker covered (posting-index sidecar) decode buffers, keyed by frame
    // index. A covered frame's decoded native buffers must live for the WHOLE
    // query, not just one navigateTo: covered frames report NATIVE, so a record
    // read of a covered VARCHAR/STRING is "stable" and zero-copy aggregates
    // (first/last) STORE the raw buffer pointer for the merge phase. Reusing one
    // slot across frames (or freeing it per frame in releaseParquetBuffers) would
    // dangle those pointers. So we cache one CoveringBuffers per frame and free
    // them all only at a query boundary (of / clear / close), mirroring the
    // eager production path, which allocates fresh per-frame buffers and frees
    // them at cursor close. The pool is per reduce slot, owned by one worker.
    private final IntObjHashMap<CoveringBuffers> coveringByFrame = new IntObjHashMap<>();
    // Bumped whenever the pool closes buffers that bound records may still alias
    // (failed decode, bulk release). Records capture it on bind; a mismatch fails
    // the navigateTo() fast path and forces a safe rebind.
    private long bindGeneration;
    // Tracks which cached buffer currently holds each usage bit. Used to clear
    // the previous pin in O(1) without scanning every cached entry.
    private ParquetBuffers boundForFrameMemory;
    private ParquetBuffers boundForRecordA;
    private ParquetBuffers boundForRecordB;
    private long cachedBytes;
    // Live native bytes held by retained CoveringBuffers (covered decode buffers).
    // Unlike parquet's cachedBytes this is NOT an eviction budget: covered buffers are
    // query-lifetime and cannot be LRU-evicted mid-query (zero-copy first()/last()
    // aggregates retain raw pointers into them for the merge phase, so evicting a still
    // referenced frame would be a use-after-free). For observability and tests only;
    // always == the sum of every retained CoveringBuffers' live allocation.
    //
    // Covered allocations charge the per-query MemoryTracker (MemoryTag.NATIVE_INDEX_READER,
    // via growNative/ensureCapacity), so the query memory limit sees and caps covered decode.
    // The matching free is global-only: a CoveringBuffers is freed lazily at the NEXT query's
    // clear()/of() (the reduce task that owns the pool is reset on reuse, not at the owning
    // query's teardown), which is after the owning query's tracker has been recycled, so a
    // tracker-charged free there would decrement an unrelated workload's recycled block. Instead
    // MemoryTracker.reconcileCovered() releases the outstanding charge from used at tracker
    // teardown, keeping the pooled block clean while the buffers stay accounted globally until
    // their lazy free.
    private long coveredCachedBytes;
    private ParquetDecodeHint decodeHint = ParquetDecodeHint.MONOTONIC;
    private long effectiveBudgetBytes;
    // True while parquetColumns/queryToSlot hold the full projection for the
    // active decoder's file, letting openParquet(int) skip the rebuild on
    // every subsequent frame of the same file.
    private boolean hasFullProjectionMap;
    private boolean hasTypeCasts;
    private ParquetBuffers lruHead;
    private ParquetBuffers lruTail;
    // Per-query tracker propagated to each ParquetBuffers' RowGroupBuffers when
    // it is reopened, so decoded parquet column data charges the owning
    // workload's limit. Null leaves decode buffers on global-only accounting
    // (e.g. context-less worker tasks and protocol-layer streaming pools).
    private MemoryTracker memoryTracker;
    // Created lazily on the first parquet frame so the configuration's decoder factory is fully wired.
    private ParquetPartitionDecoder parquetMetaDecoder;
    // Lazily created list of zero entries published as column addresses/sizes for
    // an empty decode window; a zero address reads as a column top (NULL).
    private DirectLongList nullColumnAddresses;
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
        recordAtSlices.clear();
        releaseCoveringBuffers();
        Misc.freeObjListAndClear(freeParquetBufferShells);
        addressCache = null;
        memoryTracker = null;
    }

    public long getBindGeneration() {
        return bindGeneration;
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
    public ParquetDecodeHint getDecodeHint() {
        return decodeHint;
    }

    @TestOnly
    public long getEffectiveBudgetBytes() {
        return effectiveBudgetBytes;
    }

    /**
     * Navigates to the given frame, potentially deserializing it to in-memory format
     * (for Parquet partitions). After this call, the input record can be used to access
     * any row within the frame.
     */
    public void navigateTo(int frameIndex, PageFrameMemoryRecord record) {
        final byte format = addressCache.getFrameFormat(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            // A covered frame reports NATIVE but is served by this pool's per-frame
            // CoveringBuffers, NOT the query-stable address-cache arrays, so it must
            // take the parquet-style guarded bind below -- never the frame-index-only
            // fast-return, which would leave the record pointing at recycled buffers
            // of a different generation. Detect and handle it before that fast-return.
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
            record.init(
                    frameIndex,
                    format,
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
            } else if (parquetBuffers.decodedRowLo > rowGroupLo || parquetBuffers.decodedRowHi < rowGroupHi) {
                // A record reads arbitrary rows, so a clamped (partial-window) buffer
                // left by a LIMIT scan must be re-decoded to the full frame in place.
                try {
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
                frameMemory.clear();
                throw th;
            }
        } else if (format == PartitionFormat.PARQUET) {
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
            } else if (parquetBuffers.decodedRowLo > decodeLo || parquetBuffers.decodedRowHi < decodeHi) {
                // Cached window doesn't cover the request; widen it in place.
                try {
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

        frameMemory.frameIndex = frameIndex;
        frameMemory.frameFormat = format;
        return frameMemory;
    }

    public PageFrameMemory navigateTo(int frameIndex, IntHashSet columnIndexes) {
        // No window-coverage check: only the async reduce pools call this overload,
        // and they never issue windowed decodes, so a matching frame index implies
        // a full-frame decode. A pool mixing this overload with the windowed one
        // must go through the coverage check instead.
        if (frameMemory.frameIndex == frameIndex) {
            return frameMemory;
        }

        final byte format = addressCache.getFrameFormat(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            unbind(FRAME_MEMORY_MASK);
            frameMemory.pageAddresses = addressCache.getPageAddresses();
            frameMemory.auxPageAddresses = addressCache.getAuxPageAddresses();
            frameMemory.pageSizes = addressCache.getPageSizes();
            frameMemory.auxPageSizes = addressCache.getAuxPageSizes();
            frameMemory.columnOffset = addressCache.toColumnOffset(frameIndex);
            frameMemory.currentRowGroupBuffer = null;
            // Covered frame: decode covered columns on this worker and rebind. See
            // the matching arm in navigateTo(int, int, int). The columnIndexes hint
            // is irrelevant to a covered frame -- its whole row is sidecar-decoded.
            try {
                patchCoveredFrameMemory(frameIndex);
            } catch (Throwable th) {
                // Mirror the PARQUET arm: drop frameMemory's stale binding on failure.
                frameMemory.clear();
                throw th;
            }
        } else if (format == PartitionFormat.PARQUET) {
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
            } else {
                // Full cache hit, no decode needed, but the lazy-conversion metadata
                // (the pool's hasTypeCasts / sourceColumnTypes) still reflects whichever
                // frame openParquet() last ran for. Rebuild it for THIS frame so a later
                // record.init(frameMemory) does not inherit another frame's mapping. Mirrors
                // the cache-hit refresh in navigateTo(int, PageFrameMemoryRecord).
                try {
                    openParquet(frameIndex, columnIndexes, true);
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

        frameMemory.frameIndex = frameIndex;
        frameMemory.frameFormat = format;
        return frameMemory;
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
        final CoveringBuffers buffers = decodeCoveredFrame(frameIndex);
        if (buffers == null) {
            // Non-covered or multi-key frame: keep the eager flat addresses the
            // NATIVE arm bound.
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
     * Worker-side covered decode arm for the RECORD fast-path (row filters,
     * {@link PageFrameFilteredMemoryRecord} late materialization /
     * {@code recordAt}, negative-limit, {@code AbstractPageFrameRecordCursor}).
     * Mirrors {@link #patchCoveredFrameMemory} but binds a {@link Record} instead
     * of the flyweight: when {@code frameIndex} is a single-key covered frame,
     * decode its covered columns into this pool's per-frame {@link CoveringBuffers}
     * and point {@code record} at them.
     * <p>
     * The bind uses the SAME guard as the parquet record arm -- {@code boundPool ==
     * this} and {@code boundGeneration == bindGeneration} -- because a covered
     * record points at per-pool buffers that a bulk release recycles or a different
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
        // Fast-return: the record already points at THIS pool's live covered
        // buffers for this frame. The pool/generation guard distinguishes "still
        // ours and live" from "bound elsewhere or recycled", mirroring the parquet
        // record arm; a bulk release (releaseParquetBuffers) bumps bindGeneration so
        // a stale covered binding rebinds instead of reading recycled buffers.
        if (record.getFrameIndex() == frameIndex && record.getBoundPool() == this && record.getBoundGeneration() == bindGeneration) {
            // Only covered frames stamp boundPool == this; a non-covered native
            // record leaves boundPool null, so reaching here proves this frame is
            // the covered one the record is bound to.
            return true;
        }
        final CoveringBuffers buffers = decodeCoveredFrame(frameIndex);
        if (buffers == null) {
            return false;
        }
        record.init(
                frameIndex,
                PartitionFormat.NATIVE,
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

    /**
     * Acquire (or reuse) this pool's per-frame {@link CoveringBuffers} for a
     * single-key covered frame and decode its covered columns into them, returning
     * the buffers; returns {@code null} for a non-covered frame or a multi-key
     * (merged) covered frame, which cannot be reproduced from a single detached
     * cursor and stays on the eager flat addresses. Shared by the flyweight
     * ({@link #patchCoveredFrameMemory}) and record ({@link #navigateCoveredRecord})
     * arms so both decode identically.
     */
    @Nullable
    private CoveringBuffers decodeCoveredFrame(int frameIndex) {
        if (!addressCache.isFrameCovered(frameIndex)) {
            return null;
        }
        final int key = addressCache.getCoveredKey(frameIndex);
        if (key == SymbolTable.VALUE_NOT_FOUND) {
            // Multi-key (merged) covered frame: cannot be reproduced from a single
            // detached cursor.
            return null;
        }
        final IndexReader reader = addressCache.getCoveredIndexReader(frameIndex);
        final long rowLo = addressCache.getCoveredRowLo(frameIndex);
        final long rowHi = addressCache.getCoveredRowHi(frameIndex);
        final int[] includeIndices = addressCache.getCoveredIncludeIndices(frameIndex);
        // The frame's declared size is the covered (matched) row count, NOT the
        // base range width: see CoveringPageFrameCursor#finalizeFrame, which sets
        // partitionHi = count. It is bounded by the table's page-frame row cap in
        // practice, but guard the int cast so a pathological >2^31-row covered frame
        // fails loud rather than truncating and silently under-sizing every buffer.
        final long frameSize = addressCache.getFrameSize(frameIndex);
        if (frameSize > Integer.MAX_VALUE) {
            throw CairoException.nonCritical().put("covered frame too large [rows=").put(frameSize).put(']');
        }
        final int rowCount = (int) frameSize;
        // One CoveringBuffers per frame, retained for the query (see coveringByFrame).
        final int keyIndex = coveringByFrame.keyIndex(frameIndex);
        CoveringBuffers buffers = coveringByFrame.valueAt(keyIndex);
        if (buffers == null) {
            buffers = new CoveringBuffers();
            coveringByFrame.putAt(keyIndex, frameIndex, buffers);
        }
        // Idempotent: decode() is a no-op once this frame has been decoded, so a
        // repeat navigate to the same frame reuses the live buffers (and keeps any
        // pointers stable aggregates stored into them).
        buffers.decode(frameIndex, reader, key, rowLo, rowHi, includeIndices, rowCount);
        return buffers;
    }

    public void of(PageFrameAddressCache addressCache) {
        of(addressCache, ParquetDecodeHint.MONOTONIC);
    }

    public void of(PageFrameAddressCache addressCache, ParquetDecodeHint hint) {
        releaseParquetBuffers();
        // A new query (or the same query's next address cache) invalidates any
        // covered buffers retained for stable-string aggregates of the prior one.
        releaseCoveringBuffers();
        this.addressCache = addressCache;
        this.decodeHint = hint;
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
     * Frees all decoded Parquet buffers and clears bookkeeping.
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
        // NOTE: covered (CoveringBuffers) buffers are deliberately NOT freed here.
        // The async reduce paths call this per frame, but a covered frame's native
        // buffers must outlive a single frame -- zero-copy first()/last() over a
        // covered NATIVE varchar/string stores a raw pointer into them for the
        // merge phase. They are freed only at a query boundary by
        // releaseCoveringBuffers() (of / clear / close).
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
     * Frees every per-frame covered decode buffer. Called only at query
     * boundaries ({@link #of}, {@link #clear}, {@link #close}); see
     * {@link #coveringByFrame} for why covered buffers are query-lifetime rather
     * than per-frame.
     */
    private void releaseCoveringBuffers() {
        if (coveringByFrame.size() == 0) {
            return;
        }
        final int[] keys = coveringByFrame.getKeys();
        final int noEntry = coveringByFrame.getNoEntryKey();
        for (int i = 0, n = keys.length; i < n; i++) {
            if (keys[i] != noEntry) {
                final CoveringBuffers b = coveringByFrame.get(keys[i]);
                if (b != null) {
                    b.close();
                }
            }
        }
        coveringByFrame.clear();
        // Every CoveringBuffers.close() above subtracted its own allocation, so the
        // running total must net to zero; reset defensively in case a buffer was added
        // to the map but never decoded (no allocation, no subtraction).
        assert coveredCachedBytes == 0 : "covered byte accounting leaked: " + coveredCachedBytes;
        coveredCachedBytes = 0;
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
        if (source == null || !addressCache.hasParquetFrames()) {
            return;
        }
        if (recordAtRows == null) {
            recordAtRows = new DirectLongList(256, MemoryTag.NATIVE_DEFAULT);
        }
        source.copyParquetRowIdsTo(recordAtRows, addressCache);
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
                byFrameIndex.remove(victim.frameIndex);
                lruUnlink(victim);
                victim.frameIndex = frameIndex;
                victim.usageFlags = usageBit;
                victim.decodedBytes = 0;
                victim.decodedRowHi = -1;
                victim.decodedRowLo = -1;
                victim.slotCount = 0;
                victim.isRowFiltered = false;
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
        // bumping the generation fails their fast-path check so they rebind
        // instead of reading the freed memory.
        bindGeneration++;
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
    // binding covers the request only when the buffer still belongs to the frame
    // and its decoded window spans the requested rows. An empty-window binding
    // (currentRowGroupBuffer == null) never covers a non-empty request.
    private boolean isFrameMemoryCovering(int frameIndex, int inFrameRowLo, int inFrameRowHi) {
        if (frameMemory.frameFormat == PartitionFormat.NATIVE) {
            return true;
        }
        final ParquetBuffers buffers = frameMemory.currentRowGroupBuffer;
        if (buffers == null || buffers.frameIndex != frameIndex || buffers.isRowFiltered) {
            return false;
        }
        final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
        final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
        final int decodeLo = (int) Math.min(rowGroupHi, rowGroupLo + (long) inFrameRowLo);
        final int decodeHi = (int) Math.min(rowGroupHi, rowGroupLo + (long) inFrameRowHi);
        return buffers.decodedRowLo <= decodeLo && buffers.decodedRowHi >= decodeHi;
    }

    private boolean isRowFilterEligible(int frameIndex, long declaredRowCount) {
        return declaredRowCount * 2 < addressCache.getParquetRowGroupHi(frameIndex) - addressCache.getParquetRowGroupLo(frameIndex);
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
        for (int i = 0; i < readParquetColumnCount; i++) {
            resolveParquetColumn(i, columnMapping, activeDecoder);
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
        for (int i = 0; i < readParquetColumnCount; i++) {
            if (columnIndexes.contains(i) != isInclude) {
                continue;
            }
            resolveParquetColumn(i, columnMapping, activeDecoder);
        }
        assert parquetColumns.size() % 2 == 0 : "parquetColumns must hold [parquetIdx, columnType] pairs";
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

    private class PageFrameMemoryImpl implements PageFrameMemory, Mutable {
        private DirectLongList auxPageAddresses;
        private DirectLongList auxPageSizes;
        private int columnOffset;
        private DirectLongList columnTops;
        private ParquetBuffers currentRowGroupBuffer;
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
            currentRowGroupBuffer = null;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return auxPageAddresses.get(columnOffset + columnIndex);
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
            for (int i = 0, n = addressCache.getColumnCount(); i < n; i++) {
                // VARCHAR column that contains short strings will have zero data vector,
                // so for such columns we also need to check that the aux (index) vector is zero.
                if (pageAddresses.get(columnOffset + i) == 0 && auxPageAddresses.get(columnOffset + i) == 0) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean hasColumnTypeCasts() {
            return frameFormat == PartitionFormat.PARQUET && hasTypeCasts;
        }

        @Override
        public boolean populateRemainingColumns(IntHashSet filterColumnIndexes, DirectLongList filteredRows, boolean fillWithNulls) {
            assert frameFormat == PartitionFormat.PARQUET;
            if (filterColumnIndexes.size() == addressCache.getColumnCount()) {
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
                accountDecode(currentRowGroupBuffer);
                return true;
            }
            return false;
        }
    }

    /**
     * Per-worker decoded buffers for a single covered (posting-index sidecar)
     * frame. It owns native column buffers and the four published address/size
     * lists that the navigateTo covered arm rebinds the frame memory to. There is
     * one instance PER FRAME (keyed in {@link #coveringByFrame}), retained for the
     * whole query, because a covered frame reports NATIVE and so a record read of
     * a covered VARCHAR/STRING is stable: zero-copy first()/last() aggregates
     * store the raw buffer pointer for the merge phase, so the buffer must not be
     * reused for another frame or freed mid-query. The eager production path has
     * the same lifetime (fresh per-frame buffers, freed at cursor close).
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
        private int frameIndex = -1;
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
        }

        /**
         * Decode the covered columns of {@code frameIndex} (resolved single key
         * {@code key} over base range {@code [rowLo, rowHi)}) into native buffers
         * and publish their addresses. Idempotent per frame: a repeat call for the
         * already-decoded frame is a no-op. Returns after the detached cursor has
         * been fully drained and closed.
         */
        void decode(int frameIndex, IndexReader reader, int key, long rowLo, long rowHi, int[] includeIndices, int rowCount) {
            if (this.frameIndex == frameIndex) {
                return;
            }
            ensureColumnBuffers(frameIndex, rowCount);
            // Open a detached cursor this worker owns; drain it fully, then close
            // it so it never outlives the decode. rowHi is exclusive at the frame
            // API; the index cursor's maxValue is inclusive, hence rowHi - 1.
            final RowCursor rowCursor = reader.getDetachedCursor(TableUtils.toIndexKey(key), rowLo, rowHi - 1, includeIndices);
            try {
                int count = 0;
                if (rowCursor instanceof CoveringRowCursor crc) {
                    while (crc.hasNext()) {
                        crc.next();
                        // The frame's declared size caps the cursor; the buffers are
                        // sized for exactly rowCount rows. Defensive guard only.
                        assert count < rowCount : "covered cursor yielded more rows than the frame's declared size";
                        if (count >= rowCount) {
                            break;
                        }
                        CoveredColumnDecoder.writeCoveredRow(
                                colAddr, this, count, crc, queryColCount(), coveredIncludeIdx, columnTypeTags, columnTypes);
                        count++;
                    }
                }
                // The detached re-scan must reproduce production's chunk row-for-row:
                // downstream aggregation reads exactly getFrameSize() (== rowCount) values
                // from these buffers regardless of how many the cursor yielded, so an
                // under-fill would read the uninitialized buffer tail. The loop above caps
                // over-fill, so count <= rowCount here; a short count is an invariant break
                // (frozen-reader or reseal corruption). Fail loud as a hard error — NOT just
                // an -ea assert — so a release build rejects the query instead of silently
                // aggregating uninitialized memory.
                if (count != rowCount) {
                    throw CairoException.critical(0)
                            .put("covered re-decode row count mismatch: decoded ").put(count)
                            .put(" of ").put(rowCount).put(" [frameIndex=").put(frameIndex).put(']');
                }
                // Single resolved key per covered frame: broadcast it across the
                // synthesized symbol column.
                CoveredColumnDecoder.fillSymbolKey(symAddr, key, count);
                publishAddresses(count);
                this.frameIndex = frameIndex;
            } finally {
                Misc.free(rowCursor);
            }
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
                // Charge the per-query tracker at alloc so covered decode is capped by
                // the query memory limit (a breach throws here); the matching release
                // is deferred to MemoryTracker.reconcileCovered() at tracker teardown,
                // NOT to the global-only free below. See growNative().
                varDataAddr[q] = Unsafe.realloc(varDataAddr[q], varDataCap[q], newCap, MemoryTag.NATIVE_INDEX_READER, memoryTracker);
                final long delta = (long) newCap - varDataCap[q];
                coveredCachedBytes += delta;
                if (memoryTracker != null) {
                    memoryTracker.addCoveredBytes(delta);
                }
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

        // Allocate / grow the per-column native buffers to hold rowCount rows, and
        // (re)resolve the per-column type / include metadata from the address cache.
        // colAddr[q] is the fixed-width data vector (rowCount * sizeOf) or the
        // var-size aux vector; var-size columns additionally get an initial data
        // vector grown on demand via ensureCapacity. STRING/BINARY aux carries a
        // trailing sentinel offset, so it is sized rowCount + 1.
        private void ensureColumnBuffers(int frameIndex, int rowCount) {
            final int queryColCount = queryColCount();
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
                final IntList types = addressCache.getColumnTypes();
                for (int q = 0; q < queryColCount; q++) {
                    final int type = types.getQuick(q);
                    columnTypes[q] = type;
                    columnTypeTags[q] = ColumnType.tagOf(type);
                    columnSizeBytes[q] = ColumnType.sizeOf(type);
                }
            }
            // The covered include mapping is query-constant across a query's covered
            // frames, but resolve it per decode so the slot stays correct even if a
            // pool is reused across address caches.
            for (int q = 0; q < queryColCount; q++) {
                coveredIncludeIdx[q] = addressCache.getCoveredIncludeIndex(frameIndex, q);
                coveredColumn[q] = addressCache.isColumnCovered(frameIndex, q);
            }
            // Guard against a theoretical int overflow in initDataCap = rowCount * 32
            // (var-size initial data cap). In practice a covered frame is bounded by
            // the table's page-frame row cap (order of thousands). Throw (not assert) so
            // a pathological rowCount fails loud even with -ea off, matching the >2GB
            // var-data guards below rather than silently truncating the int cap.
            if ((long) rowCount * 32 > Integer.MAX_VALUE) {
                throw CairoException.critical(0).put("covered frame too large for int varDataCap init [rows=").put(rowCount).put(']');
            }
            for (int q = 0; q < queryColCount; q++) {
                varDataPos[q] = 0;
                if (!coveredColumn[q] || coveredIncludeIdx[q] < 0) {
                    // Non-covered column (published as NULL) or the symbol key column
                    // (broadcast via symAddr) — neither needs a per-column decode buffer.
                    continue;
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
            final long symBytes = (long) rowCount * Integer.BYTES;
            if (symCap < symBytes) {
                // Only ever grow: symCap must stay the true allocation size so the
                // matching Unsafe.free() in freeColumnBuffers() accounts the right
                // number of bytes (a smaller frame must not shrink it).
                // Guard the int symCap cast below: symBytes = rowCount * 4 cannot
                // exceed int range for any page-frame-bounded rowCount, but never
                // say never — throw (not assert) so a pathological rowCount fails loud
                // even with -ea off rather than letting the cast truncate symCap and
                // under-account the matching Unsafe.free().
                if (symBytes > Integer.MAX_VALUE) {
                    throw CairoException.critical(0).put("covered frame too large for int symCap [rows=").put(rowCount).put(']');
                }
                symAddr = growNative(symAddr, symCap, symBytes);
                symCap = (int) symBytes;
            }
        }

        private void freeColumnBuffers() {
            if (colAddr != null) {
                for (int q = 0, n = colAddr.length; q < n; q++) {
                    if (colAddr[q] != 0) {
                        Unsafe.free(colAddr[q], colCap[q], MemoryTag.NATIVE_INDEX_READER);
                        coveredCachedBytes -= colCap[q];
                        colAddr[q] = 0;
                        colCap[q] = 0;
                    }
                    if (varDataAddr[q] != 0) {
                        Unsafe.free(varDataAddr[q], varDataCap[q], MemoryTag.NATIVE_INDEX_READER);
                        coveredCachedBytes -= varDataCap[q];
                        varDataAddr[q] = 0;
                        varDataCap[q] = 0;
                    }
                }
            }
            if (symAddr != 0) {
                Unsafe.free(symAddr, symCap, MemoryTag.NATIVE_INDEX_READER);
                coveredCachedBytes -= symCap;
                symAddr = 0;
                symCap = 0;
            }
        }

        // Grow a native buffer to newBytes when it is too small, preserving no
        // content (each decode rewrites every row). Reuses the existing allocation
        // when it already covers newBytes so a steady-state frame size never
        // reallocs. oldBytes == 0 means unallocated.
        //
        // Allocation charges the per-query MemoryTracker (enforcing the query memory
        // limit; a breach throws), but the matching free in freeColumnBuffers() is
        // global-only. That asymmetry is deliberate: covered buffers outlive the
        // owning query's tracker (freed lazily by the NEXT query's clear()/of() on
        // the shared reduce-task pool), so decrementing the tracker at free would
        // target a recycled block. MemoryTracker.reconcileCovered() instead releases
        // the charge from used at tracker teardown; addCoveredBytes() feeds it.
        private long growNative(long addr, long oldBytes, long newBytes) {
            if (newBytes <= 0) {
                return addr;
            }
            if (addr != 0 && oldBytes >= newBytes) {
                return addr;
            }
            final long result = Unsafe.realloc(addr, oldBytes, newBytes, MemoryTag.NATIVE_INDEX_READER, memoryTracker);
            final long delta = newBytes - oldBytes;
            coveredCachedBytes += delta;
            if (memoryTracker != null) {
                memoryTracker.addCoveredBytes(delta);
            }
            return result;
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
                if (!coveredColumn[q]) {
                    // Non-covered column in a covered frame (e.g. a null-pad synthetic added by
                    // a wrapper above the covering frame). The covered-decode arm owns only the
                    // covered includes and the symbol key; a non-covered column has no decoded
                    // data, so publish it as a NULL column (address 0) — matching how the address
                    // cache already stores such a column — rather than mis-binding it to the
                    // 4-byte symbol-key buffer (which would read the key int and, for a wider
                    // type, run past it). Today the only such columns are null-pad synthetics.
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
        private long decodedBytes;
        // decoded window bounds (row group coordinates); a cached buffer serves a
        // request only when its window covers the requested [lo, hi)
        private int decodedRowHi = -1;
        private int decodedRowLo = -1;
        private int frameIndex = -1;
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
            isRowFiltered = false;
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
                decodedBytes = isAccountingEnabled() ? rowGroupBuffers.sumChunkBytes(0, slotCount) : 0;
            } else {
                slotCount = 0;
                decodedBytes = 0;
            }
            decodedRowLo = rowLo;
            decodedRowHi = rowHi;
            isRowFiltered = false;
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
            final long extra = isAccountingEnabled() ? rowGroupBuffers.sumChunkBytes(columnOffset, extraSlots) : 0;
            if (extraSlots > 0) {
                slotCount += extraSlots;
            }
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
                decodedBytes = isAccountingEnabled() ? rowGroupBuffers.sumChunkBytes(0, slotCount) : 0;
            } else {
                slotCount = 0;
                decodedBytes = 0;
            }
            // The buffer's addresses span the full [rowLo, rowHi) range (undeclared
            // rows read as NULLs), so the record path's window-coverage check must
            // not trigger a full re-decode; the frame paths reject row-filtered
            // buffers via isRowFiltered before consulting the window.
            decodedRowLo = rowLo;
            decodedRowHi = rowHi;
            isRowFiltered = true;
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
                    continue; // ADD COLUMN / not part of this decode pass: stays at address 0 (NULL).
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

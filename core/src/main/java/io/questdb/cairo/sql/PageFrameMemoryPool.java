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
import io.questdb.cairo.ColumnType;
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
    private final ParquetPartitionDecoder parquetMetaDecoder;
    private final IntList queryToSlot = new IntList(16);
    private final IntLongHashMap recordAtSlices = new IntLongHashMap();
    // Per-column source type tag for fixed-to-var type-cast columns.
    // Indexed by query column index; -1 means no type cast.
    private final IntList sourceColumnTypes;
    private ParquetDecoder activeDecoder;
    private PageFrameAddressCache addressCache;
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
    // Lazily created list of zero entries published as column addresses/sizes for
    // an empty decode window; a zero address reads as a column top (NULL).
    private DirectLongList nullColumnAddresses;
    private DirectLongList recordAtRows;

    public PageFrameMemoryPool(long maxCacheBytes) {
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
            parquetIdxToDecodeSlot = new IntIntHashMap(16);
            parquetMetaDecoder = new ParquetPartitionDecoder();
            legacyDecoder = new ParquetFileDecoder();
            sourceColumnTypes = new IntList();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
        Misc.free(parquetColumns);
        nullColumnAddresses = Misc.free(nullColumnAddresses);
        recordAtRows = Misc.free(recordAtRows);
        recordAtSlices.clear();
        releaseParquetBuffers();
        memoryTracker = null;
    }

    @Override
    public void close() {
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
        Misc.free(parquetColumns);
        nullColumnAddresses = Misc.free(nullColumnAddresses);
        recordAtRows = Misc.free(recordAtRows);
        recordAtSlices.clear();
        releaseParquetBuffers();
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

    public void of(PageFrameAddressCache addressCache) {
        of(addressCache, ParquetDecodeHint.MONOTONIC);
    }

    public void of(PageFrameAddressCache addressCache, ParquetDecodeHint hint) {
        releaseParquetBuffers();
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
            if (parquetMetaDecoder.getFileAddr() != parquetMetaFrame.getFileAddr() || parquetMetaDecoder.getFileSize() != parquetMetaFrame.getFileSize()) {
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

    private class ParquetBuffers implements QuietCloseable {
        private final DirectLongList auxPageAddresses;
        private final DirectLongList auxPageSizes;
        // Per-query-column leading column-top count, parallel to pageAddresses. Lets a lazy
        // fixed->var conversion surface NULL for column-top rows (decoded as an in-band 0).
        private final DirectLongList columnTops;
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
            } catch (Throwable th) {
                Misc.free(auxPageAddresses);
                Misc.free(auxPageSizes);
                Misc.free(columnTops);
                Misc.free(pageAddresses);
                Misc.free(pageSizes);
                throw th;
            }
            this.auxPageAddresses = auxPageAddresses;
            this.auxPageSizes = auxPageSizes;
            this.columnTops = columnTops;
            this.pageAddresses = pageAddresses;
            this.pageSizes = pageSizes;
            this.rowGroupBuffers = rowGroupBuffers;
        }

        @Override
        public void close() {
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
            clearAddresses();
            if (parquetColumns.size() > 0) {
                decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroup, rowLo, rowHi);
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
            clearAddresses();
            if (parquetColumns.size() > 0) {
                decoder.decodeRowGroupWithRowFilterFillNulls(rowGroupBuffers, 0, parquetColumns, rowGroup, rowLo, rowHi, localRowsAddr, localRowCount);
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
    }
}

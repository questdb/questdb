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
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

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
    private ParquetDecodeHint accessPattern = ParquetDecodeHint.MONOTONIC;
    private ParquetDecoder activeDecoder;
    private PageFrameAddressCache addressCache;
    // Tracks which cached buffer currently holds each usage bit. Used to clear
    // the previous pin in O(1) without scanning every cached entry.
    private ParquetBuffers boundForFrameMemory;
    private ParquetBuffers boundForRecordA;
    private ParquetBuffers boundForRecordB;
    private long cachedBytes;
    private long effectiveBudgetBytes;
    // True while parquetColumns/queryToSlot hold the full projection for the
    // active decoder's file, letting openParquet(int) skip the rebuild on
    // every subsequent frame of the same file.
    private boolean hasFullProjectionMap;
    private ParquetBuffers lruHead;
    private ParquetBuffers lruTail;
    private DirectLongList recordAtRows;

    public PageFrameMemoryPool(long maxCacheBytes) {
        try {
            this.maxCacheBytes = Math.max(maxCacheBytes, 0L);
            this.effectiveBudgetBytes = accessPattern.applyTo(this.maxCacheBytes);
            byFrameIndex = new IntObjHashMap<>(16);
            columnIdToParquetIdx = new IntIntHashMap(16);
            frameMemory = new PageFrameMemoryImpl();
            parquetColumns = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT, true);
            parquetIdxToDecodeSlot = new IntIntHashMap(16);
            parquetMetaDecoder = new ParquetPartitionDecoder();
            legacyDecoder = new ParquetFileDecoder();
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
        recordAtRows = Misc.free(recordAtRows);
        recordAtSlices.clear();
        releaseParquetBuffers();
    }

    @Override
    public void close() {
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
        Misc.free(parquetColumns);
        recordAtRows = Misc.free(recordAtRows);
        recordAtSlices.clear();
        releaseParquetBuffers();
        Misc.freeObjListAndClear(freeParquetBufferShells);
        addressCache = null;
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
                    addressCache.toColumnOffset(frameIndex)
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
            // without reopening the parquet use-after-free on the random-access path.
            if (record.getFrameIndex() == frameIndex && record.getBoundPool() == this) {
                return;
            }
            final byte usageBit = record.getLetter() == PageFrameMemoryRecord.RECORD_A_LETTER ? RECORD_A_MASK : RECORD_B_MASK;
            ParquetBuffers parquetBuffers = tryHit(frameIndex, usageBit);
            if (parquetBuffers == null) {
                openParquet(frameIndex);
                parquetBuffers = acquireBuffer(frameIndex, usageBit);
                final long slice = recordAtSlices.get(frameIndex);
                if (shouldDecodeRowFiltered(frameIndex, slice)) {
                    decodeRowFilteredAndAccount(frameIndex, parquetBuffers, slice);
                } else {
                    decodeAndAccount(frameIndex, parquetBuffers);
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
                    0
            );
            record.setBoundPool(this);
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
            if (parquetBuffers == null) {
                openParquet(frameIndex);
                parquetBuffers = acquireBuffer(frameIndex, FRAME_MEMORY_MASK);
                decodeAndAccount(frameIndex, parquetBuffers);
            }
            assert !parquetBuffers.isRowFiltered : "row-filtered buffer hit by full-frame access";
            frameMemory.currentRowGroupBuffer = parquetBuffers;
            frameMemory.pageAddresses = parquetBuffers.pageAddresses;
            frameMemory.auxPageAddresses = parquetBuffers.auxPageAddresses;
            frameMemory.pageSizes = parquetBuffers.pageSizes;
            frameMemory.auxPageSizes = parquetBuffers.auxPageSizes;
            frameMemory.columnOffset = 0;
        }

        frameMemory.frameIndex = frameIndex;
        frameMemory.frameFormat = format;
        return frameMemory;
    }

    public PageFrameMemory navigateTo(int frameIndex, IntHashSet columnIndexes) {
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
            if (parquetBuffers == null) {
                openParquet(frameIndex, columnIndexes, true);
                parquetBuffers = acquireBuffer(frameIndex, FRAME_MEMORY_MASK);
                decodeAndAccount(frameIndex, parquetBuffers);
            }
            assert !parquetBuffers.isRowFiltered : "row-filtered buffer hit by full-frame access";
            frameMemory.currentRowGroupBuffer = parquetBuffers;
            frameMemory.pageAddresses = parquetBuffers.pageAddresses;
            frameMemory.auxPageAddresses = parquetBuffers.auxPageAddresses;
            frameMemory.pageSizes = parquetBuffers.pageSizes;
            frameMemory.auxPageSizes = parquetBuffers.auxPageSizes;
            frameMemory.columnOffset = 0;
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
        this.accessPattern = hint;
        this.effectiveBudgetBytes = hint.applyTo(maxCacheBytes);
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
        recordAtSlices.clear();
        if (recordAtRows != null) {
            recordAtRows.clear();
        }
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
     * before invoking. Used from {@link #clear()} / {@link #close()} and
     * from cursor lifecycle points (toTop, close).
     */
    public void releaseParquetBuffers() {
        ParquetBuffers b = lruHead;
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
        if (byFrameIndex != null) {
            byFrameIndex.clear();
        }
        boundForRecordA = null;
        boundForRecordB = null;
        boundForFrameMemory = null;
        cachedBytes = 0;
        if (frameMemory != null) {
            frameMemory.clear();
        }
    }

    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        this.accessPattern = hint;
        this.effectiveBudgetBytes = hint.applyTo(maxCacheBytes);
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
        boolean isSorted = true;
        long prevRowId = recordAtRows.get(0);
        for (long i = 0; i < kept; i++) {
            final long rowId = recordAtRows.get(i);
            isSorted &= Long.compareUnsigned(prevRowId, rowId) <= 0;
            prevRowId = rowId;
            final int frameIndex = Rows.toPartitionIndex(rowId);
            final int keyIndex = recordAtSlices.keyIndex(frameIndex);
            final long declared = keyIndex < 0 ? recordAtSlices.valueAt(keyIndex) : 0;
            recordAtSlices.putAt(keyIndex, frameIndex, declared + 1);
        }
        boolean hasEligibleFrame = false;
        for (long i = 0; i < kept; i++) {
            final int frameIndex = Rows.toPartitionIndex(recordAtRows.get(i));
            if (isRowFilterEligible(frameIndex, recordAtSlices.get(frameIndex))) {
                hasEligibleFrame = true;
                break;
            }
        }
        recordAtSlices.clear();
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
        if (cachedBytes >= effectiveBudgetBytes || byFrameIndex.size() >= accessPattern.maxCachedBuffers) {
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
                victim.isRowFiltered = false;
                victim.slotCount = 0;
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
            // reopen() re-allocates the native buffers one by one. The shell is
            // already popped off freeParquetBufferShells and not yet tracked in
            // lruHead/byFrameIndex, so a partial reopen would orphan it. Free what
            // reopen() managed to allocate and discard the shell.
            try {
                buffers.reopen();
            } catch (Throwable th) {
                buffers.close();
                throw th;
            }
        } else {
            buffers = new ParquetBuffers();
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
        final int rowGroupIndex = addressCache.getParquetRowGroup(frameIndex);
        final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
        final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
        try {
            parquetBuffers.decode(activeDecoder, parquetColumns, rowGroupIndex, rowGroupLo, rowGroupHi);
        } catch (Throwable th) {
            evictHalfInitialized(parquetBuffers);
            throw th;
        }
        accountDecode(parquetBuffers);
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
        if (boundForRecordA == buffers) {
            boundForRecordA = null;
        }
        if (boundForRecordB == buffers) {
            boundForRecordB = null;
        }
        if (boundForFrameMemory == buffers) {
            boundForFrameMemory = null;
        }
        evictAndClose(buffers);
    }

    private void evictRowFilteredBuffers() {
        ParquetBuffers b = lruHead;
        while (b != null) {
            final ParquetBuffers next = b.next;
            if (b.isRowFiltered) {
                if (b.usageFlags == 0) {
                    evictAndClose(b);
                } else {
                    // A new declaration while a record still reads buffers filtered
                    // under the old one is a caller contract violation; unmap the
                    // buffer so it cannot serve hits for the new declaration.
                    assert false : "row set redeclared while a row-filtered buffer is pinned";
                    byFrameIndex.remove(b.frameIndex);
                    b.frameIndex = -1;
                }
            }
            b = next;
        }
    }

    private ParquetBuffers getBound(byte usageBit) {
        return switch (usageBit) {
            case RECORD_A_MASK -> boundForRecordA;
            case RECORD_B_MASK -> boundForRecordB;
            case FRAME_MEMORY_MASK -> boundForFrameMemory;
            default -> null;
        };
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
        for (int i = 0; i < readParquetColumnCount; i++) {
            final int columnWriterIndex = columnMapping.getWriterIndex(i);
            final int parquetIdx = columnIdToParquetIdx.get(columnWriterIndex);
            if (parquetIdx < 0) {
                continue;
            }
            final int slotKey = parquetIdxToDecodeSlot.keyIndex(parquetIdx);
            final int existingSlot = parquetIdxToDecodeSlot.valueAt(slotKey);
            if (existingSlot >= 0) {
                queryToSlot.setQuick(i, existingSlot);
                continue;
            }
            int columnType = addressCache.getColumnTypes().getQuick(i);
            if (ColumnType.tagOf(columnType) == ColumnType.VARCHAR) {
                columnType = ColumnType.VARCHAR_SLICE;
            }
            final int slot = (int) (parquetColumns.size() / 2);
            parquetIdxToDecodeSlot.putAt(slotKey, parquetIdx, slot);
            parquetColumns.add(parquetIdx);
            parquetColumns.add(columnType);
            queryToSlot.setQuick(i, slot);
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
        for (int i = 0; i < readParquetColumnCount; i++) {
            if (columnIndexes.contains(i) != isInclude) {
                continue;
            }
            final int columnWriterIndex = columnMapping.getWriterIndex(i);
            final int parquetIdx = columnIdToParquetIdx.get(columnWriterIndex);
            if (parquetIdx < 0) {
                continue;
            }
            final int slotKey = parquetIdxToDecodeSlot.keyIndex(parquetIdx);
            final int existingSlot = parquetIdxToDecodeSlot.valueAt(slotKey);
            if (existingSlot >= 0) {
                queryToSlot.setQuick(i, existingSlot);
                continue;
            }
            int columnType = addressCache.getColumnTypes().getQuick(i);
            if (ColumnType.tagOf(columnType) == ColumnType.VARCHAR) {
                columnType = ColumnType.VARCHAR_SLICE;
            }
            final int slot = (int) (parquetColumns.size() / 2);
            parquetIdxToDecodeSlot.putAt(slotKey, parquetIdx, slot);
            parquetColumns.add(parquetIdx);
            parquetColumns.add(columnType);
            queryToSlot.setQuick(i, slot);
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
            if (accessPattern == ParquetDecodeHint.SCATTERED) {
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
        if (accessPattern == ParquetDecodeHint.SCATTERED) {
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

    private class PageFrameMemoryImpl implements PageFrameMemory, Mutable {
        private DirectLongList auxPageAddresses;
        private DirectLongList auxPageSizes;
        private int columnOffset;
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
        private final DirectLongList pageAddresses;
        private final DirectLongList pageSizes;
        private final RowGroupBuffers rowGroupBuffers;
        private long decodedBytes;
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
            DirectLongList pageAddresses = null;
            DirectLongList pageSizes = null;
            RowGroupBuffers rowGroupBuffers = null;
            try {
                auxPageAddresses = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                auxPageSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                pageAddresses = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                pageSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            } catch (Throwable th) {
                Misc.free(auxPageAddresses);
                Misc.free(auxPageSizes);
                Misc.free(pageAddresses);
                Misc.free(pageSizes);
                Misc.free(rowGroupBuffers);
                throw th;
            }
            this.auxPageAddresses = auxPageAddresses;
            this.auxPageSizes = auxPageSizes;
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
            Misc.free(rowGroupBuffers);
            slotCount = 0;
            usageFlags = 0;
            frameIndex = -1;
            decodedBytes = 0;
            retainedBytes = 0;
            isRowFiltered = false;
        }

        public void decode(ParquetDecoder decoder, DirectIntList parquetColumns, int rowGroup, int rowLo, int rowHi) {
            clearAddresses();
            isRowFiltered = false;
            if (parquetColumns.size() > 0) {
                decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroup, rowLo, rowHi);
                slotCount = (int) (parquetColumns.size() / 2);
                decodedBytes = rowGroupBuffers.sumChunkBytes(0, slotCount);
            } else {
                slotCount = 0;
                decodedBytes = 0;
            }
            remapColumns();
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
            final long extra = rowGroupBuffers.sumChunkBytes(columnOffset, extraSlots);
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
                decodedBytes = rowGroupBuffers.sumChunkBytes(0, slotCount);
            } else {
                slotCount = 0;
                decodedBytes = 0;
            }
            isRowFiltered = true;
            remapColumns();
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
            rowGroupBuffers.reopen();
        }

        private void clearAddresses() {
            pageAddresses.clear();
            pageSizes.clear();
            auxPageAddresses.clear();
            auxPageSizes.clear();
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
        private void remapColumns() {
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
                    continue;
                }
                final int columnType = addressCache.getColumnTypes().getQuick(q);
                pageAddresses.set(q, getSlotDataPtr(slot));
                pageSizes.set(q, getSlotDataSize(slot));
                if (ColumnType.isVarSize(columnType)) {
                    auxPageAddresses.set(q, getSlotAuxPtr(slot));
                    auxPageSizes.set(q, getSlotAuxSize(slot));
                }
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
                    continue;
                }
                final int columnType = addressCache.getColumnTypes().getQuick(q);
                pageAddresses.set(q, rowGroupBuffers.getChunkDataPtr(columnOffset + slot));
                pageSizes.set(q, rowGroupBuffers.getChunkDataSize(columnOffset + slot));
                if (ColumnType.isVarSize(columnType)) {
                    auxPageAddresses.set(q, rowGroupBuffers.getChunkAuxPtr(columnOffset + slot));
                    auxPageSizes.set(q, rowGroupBuffers.getChunkAuxSize(columnOffset + slot));
                }
            }
        }
    }
}

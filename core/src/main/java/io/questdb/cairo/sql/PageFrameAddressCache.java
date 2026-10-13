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
import io.questdb.cairo.idx.IndexReader;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.std.ByteList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;

import java.util.Arrays;

/**
 * Holds formats, addresses and sizes for page frames.
 * <p>
 * For native (mmapped) page frames we store addresses that correspond
 * to aux/data vectors for each column. For parquet page frames we store
 * addresses of mmapped files, as well as the list of row groups.
 * <p>
 * Once initialized, this cache is thread-safe.
 * <p>
 * Meant to be used along with {@link PageFrameMemoryPool}.
 */
public class PageFrameAddressCache implements QuietCloseable, Mutable {
    private static final int ADDRESS_LIST_INITIAL_CAPACITY = 64;
    // Flat arrays storing per-frame, per-column data. Indexed as: frameIndex * columnCount + columnIndex.
    // These are off-heap to reduce GC pressure for large and wide tables.
    private final DirectLongList auxPageAddresses;
    private final DirectLongList auxPageSizes;
    private final ColumnMapping columnMapping = new ColumnMapping();
    private final IntList columnTypes = new IntList();
    // Per-frame covered (posting-index sidecar) decode metadata. Populated
    // additively for frames that report at least one DataSource.COVERED column
    // (see PageFrame#getColumnSource); parallel to frameSizes/frameFormats so
    // every accessor indexes by frameIndex. Frames with no covered columns
    // store sentinels: key = SymbolTable.VALUE_NOT_FOUND (-2), rowLo/rowHi = -1L
    // (a row-index sentinel, NOT VALUE_NOT_FOUND — the two are deliberately different
    // to prevent conflating a missing key with a missing row range), null reader/desc.
    // Mirrors the parquet stash (parquetDecoders/parquetRowGroup*).
    // null entry => frame has no covered columns.
    private final ObjList<boolean[]> coveredColumns = new ObjList<>();
    // Per covered frame, the deduplicated sidecar include indices its covered
    // columns decode from (the required-cover-columns argument for opening a
    // covering cursor). null for non-covered frames. Same array every covered
    // frame of a query, but stored per frame to keep accessors index-uniform.
    private final ObjList<int[]> coveredIncludeIndices = new ObjList<>();
    // Per covered frame, the per-query-column sidecar include index (>= 0 for a
    // covered column, -1 for the symbol key / non-covered column). null for
    // non-covered frames. Mirrors PageFrame#getCoveredIncludeIndex.
    private final ObjList<int[]> coveredColumnIncludes = new ObjList<>();
    private final ObjList<IndexReader> coveredIndexReaders = new ObjList<>();
    private final IntList coveredKeys = new IntList();
    private final LongList coveredRowHis = new LongList();
    private final LongList coveredRowLos = new LongList();
    private final ByteList frameFormats = new ByteList();
    // Per frame: 1 once its column addresses are in the flat lists, 0 while a deferred frame waits
    // for its filler, see ensureFrameReady(). Written in order (release), read volatile (acquire).
    private final IntList frameReady = new IntList();
    // Per frame: the filler that computes a deferred frame's column addresses, null for any other
    // frame. Kept per frame, not per cache, so that frames of several cursors (a wrapper that chains
    // two) are each filled from their own reader.
    private final ObjList<PageFrameAddressFiller> frameFillers = new ObjList<>();
    private final LongList frameSizes = new LongList();
    private final DirectLongList pageAddresses;
    private final DirectLongList pageSizes;
    private final ObjList<ParquetDecoder> parquetDecoders = new ObjList<>();
    private final IntList parquetRowGroupHis = new IntList();
    private final IntList parquetRowGroupLos = new IntList();
    private final IntList parquetRowGroups = new IntList();
    // Makes it possible to determine real row id, not the one relative to the page.
    private final LongList rowIdOffsets = new LongList();
    private int columnCount;
    // whether any frame was added deferred, see ensureFrameReady()
    private boolean hasDeferredFrames;
    // True in case of external parquet files, false in case of table partition files.
    private boolean external;
    private boolean hasCoveredFrames;
    private boolean hasParquetFrames;

    public PageFrameAddressCache() {
        this.auxPageAddresses = new DirectLongList(ADDRESS_LIST_INITIAL_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
        this.auxPageSizes = new DirectLongList(ADDRESS_LIST_INITIAL_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
        this.pageAddresses = new DirectLongList(ADDRESS_LIST_INITIAL_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
        this.pageSizes = new DirectLongList(ADDRESS_LIST_INITIAL_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
    }

    public void add(int frameIndex, @Transient PageFrame frame) {
        if (frameSizes.size() >= frameIndex + 1) {
            // The page frame is already cached; covered stash was populated on the
            // first call for this frame, so no patching is needed here.
            return;
        }

        // Covered-frame stash (mirrors the parquet stash below) built in the SAME pass as the
        // native page addresses, so a plain (non-covered) frame pays no second per-column loop.
        // A covered frame always reports NATIVE, so only the native branch can populate this; for
        // a parquet (or any non-covered) frame covered stays null and we store sentinels below.
        boolean[] covered = null;
        int[] columnInclude = null;
        final byte format = frame.getFormat();
        final PageFrameAddressFiller filler = format == PartitionFormat.NATIVE ? frame.getDeferredAddressFiller() : null;
        if (filler != null) {
            // the frame's columns are computed later, by the thread that first needs them: zeros
            // hold their places, see ensureFrameReady()
            hasDeferredFrames = true;
            appendZeros(pageAddresses);
            appendZeros(pageSizes);
            appendZeros(auxPageAddresses);
            appendZeros(auxPageSizes);
        } else if (format == PartitionFormat.NATIVE) {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                pageAddresses.add(frame.getPageAddress(columnIndex));
                pageSizes.add(frame.getPageSize(columnIndex));
                if (ColumnType.isVarSize(columnTypes.getQuick(columnIndex))) {
                    auxPageAddresses.add(frame.getAuxPageAddress(columnIndex));
                    auxPageSizes.add(frame.getAuxPageSize(columnIndex));
                } else {
                    auxPageAddresses.add(0);
                    auxPageSizes.add(0);
                }
                if (frame.getColumnSource(columnIndex) == DataSource.COVERED) {
                    if (covered == null) {
                        covered = new boolean[columnCount];
                        columnInclude = new int[columnCount];
                        Arrays.fill(columnInclude, -1);
                    }
                    covered[columnIndex] = true;
                    columnInclude[columnIndex] = frame.getCoveredIncludeIndex(columnIndex);
                }
            }
        } else {
            // For parquet frames, we still need to reserve space in flat arrays
            // to maintain consistent indexing, but values will be unused.
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                pageAddresses.add(0);
                pageSizes.add(0);
                auxPageAddresses.add(0);
                auxPageSizes.add(0);
            }
            hasParquetFrames = true;
        }

        // Defensive consistency check: a covering frame produces its per-column
        // DataSource.COVERED flags and its per-frame covered accessors together, so the
        // two must agree. A future PageFrame wrapper that delegates some but not all of
        // the covered accessors (the realistic failure mode) would otherwise silently
        // record the frame as non-covered (or half-covered) here, no-op'ing the worker
        // covered-decode arm and producing wrong/NULL columns with no error. -ea is on in
        // all tests + CI, so this turns that silent corruption into a loud failure.
        assert coveredMetadataConsistent(frame, covered, format);

        frameSizes.add(frame.getPartitionHi() - frame.getPartitionLo());
        frameFormats.add(format);
        frameReady.add(filler != null ? 0 : 1);
        frameFillers.add(filler);
        ParquetDecoder decoder = frame.getParquetDecoder();
        parquetDecoders.add(decoder);
        assert (decoder != null && decoder.getFileSize() > 0) || format != PartitionFormat.PARQUET;
        parquetRowGroups.add(frame.getParquetRowGroup());
        parquetRowGroupLos.add(frame.getParquetRowGroupLo());
        parquetRowGroupHis.add(frame.getParquetRowGroupHi());
        rowIdOffsets.add(Rows.toRowID(frame.getPartitionIndex(), frame.getPartitionLo()));

        // Covered-frame stash populated in the native pass above. Single-key covered frames are
        // metadata-only at production (CoveringPageFrameCursor#finalizeFrame emits PLACEHOLDER
        // zero addresses), so those flat entries are 0 and the worker covered arm
        // (PageFrameMemoryPool#patchCoveredFrameMemory) re-decodes and rebinds; multi-key
        // (VALUE_NOT_FOUND) covered frames carry real eager addresses the worker arm leaves alone.
        coveredColumns.add(covered);
        coveredColumnIncludes.add(columnInclude);
        if (covered != null) {
            coveredKeys.add(frame.getCoveredKey());
            coveredRowLos.add(frame.getCoveredRowLo());
            coveredRowHis.add(frame.getCoveredRowHi());
            coveredIncludeIndices.add(frame.getCoveredIncludeIndices());
            // The covered frame carries a single per-partition posting reader;
            // column/direction are advisory (see CoveringPageFrame#getIndexReader).
            coveredIndexReaders.add(frame.getIndexReader(0, IndexReader.DIR_FORWARD));
            hasCoveredFrames = true;
        } else {
            coveredKeys.add(SymbolTable.VALUE_NOT_FOUND);
            coveredRowLos.add(-1);
            coveredRowHis.add(-1);
            coveredIncludeIndices.add(null);
            coveredIndexReaders.add(null);
        }
    }

    private void appendZeros(DirectLongList list) {
        // a frame holds a handful of columns: a plain loop beats a native memset call
        list.ensureCapacity(columnCount);
        final long address = list.getAppendAddress();
        for (int i = 0; i < columnCount; i++) {
            Unsafe.putLong(address + (long) i * Long.BYTES, 0);
        }
        list.skip(columnCount);
    }

    /**
     * Cross-checks that a frame's per-column {@link DataSource#COVERED} flags agree with its
     * per-frame covered accessors. A covering frame produces both together; a wrapper that
     * propagates one but not the other is a bug that would otherwise silently mis-handle the
     * frame. Only meaningful for NATIVE frames (covered frames always report NATIVE).
     *
     * @param covered the per-column covered flags collected in the native pass (null = none covered)
     * @return true if consistent (used via {@code assert})
     */
    private static boolean coveredMetadataConsistent(@Transient PageFrame frame, boolean[] covered, byte format) {
        if (format != PartitionFormat.NATIVE) {
            // Non-native (e.g. parquet) frames are never covered and skip the covered accessors.
            return covered == null;
        }
        final boolean anyColumnCovered = covered != null;
        final int[] perFrameIncludes = frame.getCoveredIncludeIndices();
        // The per-column COVERED flags and the per-frame include set are produced together, so
        // exactly one of two states is valid: both present (covered frame) or both absent (plain).
        if (anyColumnCovered != (perFrameIncludes != null)) {
            return false;
        }
        if (anyColumnCovered) {
            // Every COVERED column must carry a valid sidecar include index: >= 0 for an
            // INCLUDE-mapped column, or -1 for the synthesized symbol key. Anything below -1
            // means the per-column include remap was dropped/garbled.
            for (int c = 0; c < covered.length; c++) {
                if (covered[c] && frame.getCoveredIncludeIndex(c) < -1) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void clear() {
        frameSizes.clear();
        frameReady.clear();
        frameFillers.clear();
        hasDeferredFrames = false;
        frameFormats.clear();
        parquetDecoders.clear();
        parquetRowGroups.clear();
        parquetRowGroupLos.clear();
        parquetRowGroupHis.clear();
        coveredColumns.clear();
        coveredColumnIncludes.clear();
        coveredIncludeIndices.clear();
        coveredIndexReaders.clear();
        coveredKeys.clear();
        coveredRowLos.clear();
        coveredRowHis.clear();
        pageAddresses.clear();
        auxPageAddresses.clear();
        pageSizes.clear();
        auxPageSizes.clear();
        rowIdOffsets.clear();
        external = false;
        hasCoveredFrames = false;
        hasParquetFrames = false;
    }

    @Override
    public void close() {
        pageAddresses.close();
        pageSizes.close();
        auxPageAddresses.close();
        auxPageSizes.close();
        // the fillers reference the frame cursors' readers, which a closed cache must not keep
        frameFillers.clear();
    }

    /**
     * Makes sure the column addresses of the frame are in the flat lists, computing them on this
     * thread when the frame was added deferred (see {@link PageFrame#getDeferredAddressFiller()})
     * and no thread has computed them yet. Every navigation to a frame calls it first, see
     * {@link PageFrameMemoryPool}; any thread may call it, concurrently with others, once the
     * frames are added. Two threads that meet an unfilled frame both compute it, and write the
     * same values.
     */
    public void ensureFrameReady(int frameIndex) {
        if (hasDeferredFrames && frameReady.getVolatile(frameIndex) == 0) { // acquire
            final PageFrameAddressFiller filler = frameFillers.getQuick(frameIndex);
            final long rowIdOffset = rowIdOffsets.getQuick(frameIndex);
            final long partitionLo = Rows.toLocalRowID(rowIdOffset);
            filler.fillFrameAddresses(
                    Rows.toPartitionIndex(rowIdOffset),
                    partitionLo,
                    partitionLo + frameSizes.getQuick(frameIndex),
                    this,
                    frameIndex
            );
            frameReady.setOrdered(frameIndex, 1); // release
        }
    }

    /**
     * Whether the frame's column addresses are in the flat lists, see {@link #ensureFrameReady(int)}.
     */
    public boolean isFrameReady(int frameIndex) {
        return frameReady.getVolatile(frameIndex) != 0;
    }

    /**
     * For a {@link PageFrameAddressFiller}: one column's addresses and sizes of a deferred native
     * frame. The aux address and size are those of a var-size column, and ignored for any other.
     */
    public void setFrameColumnAddresses(int frameIndex, int columnIndex, long pageAddress, long pageSize, long auxPageAddress, long auxPageSize) {
        final long offset = (long) frameIndex * columnCount + columnIndex;
        pageAddresses.set(offset, pageAddress);
        pageSizes.set(offset, pageSize);
        if (ColumnType.isVarSize(columnTypes.getQuick(columnIndex))) {
            auxPageAddresses.set(offset, auxPageAddress);
            auxPageSizes.set(offset, auxPageSize);
        }
    }

    /**
     * Returns the flat auxPageAddresses list for direct access.
     * Use with {@link #toColumnOffset(int)} for efficient batch operations.
     */
    public DirectLongList getAuxPageAddresses() {
        return auxPageAddresses;
    }

    /**
     * Returns the flat auxPageSizes list for direct access.
     * Use with {@link #toColumnOffset(int)} for efficient batch operations.
     */
    public DirectLongList getAuxPageSizes() {
        return auxPageSizes;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public ColumnMapping getColumnMapping() {
        return columnMapping;
    }

    public IntList getColumnTypes() {
        return columnTypes;
    }

    /**
     * The per-partition posting (index) reader the covered frame's columns
     * decode from, or {@code null} when the frame has no covered columns.
     */
    public IndexReader getCoveredIndexReader(int frameIndex) {
        return coveredIndexReaders.getQuick(frameIndex);
    }

    /**
     * The posting-index sidecar include index that covered column {@code col} of
     * {@code frameIndex} decodes from (the argument for
     * {@code CoveringRowCursor.getCoveredXxx}), or {@code -1} for the symbol key,
     * a non-covered column, or any column of a non-covered frame. Matches
     * {@code PageFrame#getCoveredIncludeIndex(col)} for the frame that was added.
     */
    public int getCoveredIncludeIndex(int frameIndex, int col) {
        final int[] columnInclude = coveredColumnIncludes.getQuick(frameIndex);
        return columnInclude != null ? columnInclude[col] : -1;
    }

    /**
     * The deduplicated sidecar include indices a covered frame's columns decode
     * from — the required-cover-columns argument for opening a covering cursor
     * over {@link #getCoveredIndexReader(int)}. {@code null} for a frame with no
     * covered columns.
     */
    public int[] getCoveredIncludeIndices(int frameIndex) {
        return coveredIncludeIndices.getQuick(frameIndex);
    }

    /**
     * The resolved WHERE symbol key for a covered frame, or
     * {@link SymbolTable#VALUE_NOT_FOUND} for a frame with no covered columns.
     */
    public int getCoveredKey(int frameIndex) {
        return coveredKeys.getQuick(frameIndex);
    }

    /**
     * Exclusive high base-partition row index of a covered frame's source
     * range, or {@code -1} for a frame with no covered columns.
     */
    public long getCoveredRowHi(int frameIndex) {
        return coveredRowHis.getQuick(frameIndex);
    }

    /**
     * Inclusive low base-partition row index of a covered frame's source
     * range, or {@code -1} for a frame with no covered columns.
     */
    public long getCoveredRowLo(int frameIndex) {
        return coveredRowLos.getQuick(frameIndex);
    }

    public int getFrameCount() {
        return frameSizes.size();
    }

    public byte getFrameFormat(int frameIndex) {
        return frameFormats.getQuick(frameIndex);
    }

    public long getFrameSize(int frameIndex) {
        return frameSizes.getQuick(frameIndex);
    }

    /**
     * Returns the flat pageAddresses list for direct access.
     * Use with {@link #toColumnOffset(int)} for efficient batch operations.
     */
    public DirectLongList getPageAddresses() {
        return pageAddresses;
    }

    /**
     * Returns the flat pageSizes list for direct access.
     * Use with {@link #toColumnOffset(int)} for efficient batch operations.
     */
    public DirectLongList getPageSizes() {
        return pageSizes;
    }

    public ParquetDecoder getParquetDecoder(int frameIndex) {
        return parquetDecoders.getQuick(frameIndex);
    }

    public int getParquetRowGroup(int frameIndex) {
        return parquetRowGroups.getQuick(frameIndex);
    }

    public int getParquetRowGroupHi(int frameIndex) {
        return parquetRowGroupHis.getQuick(frameIndex);
    }

    public int getParquetRowGroupLo(int frameIndex) {
        return parquetRowGroupLos.getQuick(frameIndex);
    }

    public long getRowIdOffset(int frameIndex) {
        return rowIdOffsets.getQuick(frameIndex);
    }

    /**
     * Returns {@code true} if the frame at {@code frameIndex} carries any covered
     * (posting-index sidecar) columns; {@code false} for plain native and parquet
     * frames. This is a per-frame predicate — see {@link #hasCoveredFrames()} for
     * the cache-level flag.
     */
    public boolean isFrameCovered(int frameIndex) {
        return coveredColumns.getQuick(frameIndex) != null;
    }

    /**
     * Whether the frame can decode a declared subset of its rows alone, see
     * {@link PageFrameMemoryPool#setRecordAtRows}: a Parquet or a single-key covered frame.
     */
    public boolean isFrameRowFilterable(int frameIndex) {
        return getFrameFormat(frameIndex) == PartitionFormat.PARQUET || isFrameSingleKeyCovered(frameIndex);
    }

    public boolean isFrameSingleKeyCovered(int frameIndex) {
        return isFrameCovered(frameIndex) && coveredKeys.getQuick(frameIndex) != SymbolTable.VALUE_NOT_FOUND;
    }

    /**
     * Freeze every per-partition posting reader backing a covered frame, so that
     * {@link IndexReader#reloadConditionally()} becomes a no-op and the value /
     * sidecar mmaps stay stable while async workers iterate detached covering
     * cursors over them concurrently. Must be called once, AFTER the readers have
     * been positioned at the query txn and warmed (the eager production decode in
     * {@link #add} does both as a side effect of its full iteration) and BEFORE
     * any worker decode is dispatched. {@link #unfreezeCoveredReaders()} reverses
     * it at query teardown. {@code setFrozen} is idempotent, so the duplicate
     * reader entries multi-frame partitions produce are harmless.
     */
    public void freezeCoveredReaders() {
        if (!hasCoveredFrames) {
            return;
        }
        for (int i = 0, n = coveredIndexReaders.size(); i < n; i++) {
            final IndexReader reader = coveredIndexReaders.getQuick(i);
            if (reader != null) {
                reader.setFrozen(true);
            }
        }
    }

    /**
     * Unfreeze the covered posting readers frozen by {@link #freezeCoveredReaders()}
     * so later queries can reload them. Must run once all worker cursors over this
     * cache have finished (e.g. after the frame sequence has been awaited / the
     * vector aggregate done-latch has drained). A reader left frozen would make
     * its {@code reloadConditionally()} a permanent no-op and break the next
     * query against the same partition.
     */
    public void unfreezeCoveredReaders() {
        if (!hasCoveredFrames) {
            return;
        }
        for (int i = 0, n = coveredIndexReaders.size(); i < n; i++) {
            final IndexReader reader = coveredIndexReaders.getQuick(i);
            if (reader != null) {
                reader.setFrozen(false);
            }
        }
    }

    /**
     * Whether this cache holds at least one covered frame. Mirrors
     * {@link #hasParquetFrames()}.
     */
    public boolean hasCoveredFrames() {
        return hasCoveredFrames;
    }

    public boolean hasParquetFrames() {
        return hasParquetFrames;
    }

    /**
     * Whether column {@code col} of {@code frameIndex} is served from the
     * posting-index sidecar (matches {@code PageFrame#getColumnSource(col) ==
     * DataSource.COVERED} for the frame that was added). False for any column
     * of a non-covered frame.
     */
    public boolean isColumnCovered(int frameIndex, int col) {
        final boolean[] covered = coveredColumns.getQuick(frameIndex);
        return covered != null && covered[col];
    }

    /**
     * Whether column {@code col} of {@code frameIndex} is the symbol key of a single-key covered
     * frame: the column holds one value, the frame's key, on every row of the frame.
     */
    public boolean isColumnCoveredKey(int frameIndex, int col) {
        return isFrameSingleKeyCovered(frameIndex)
                && isColumnCovered(frameIndex, col)
                && getCoveredIncludeIndex(frameIndex, col) == -1;
    }

    public boolean isExternal() {
        return external;
    }

    public boolean isVarSizeColumn(int columnIndex) {
        return ColumnType.isVarSize(columnTypes.getQuick(columnIndex));
    }

    public void of(
            @Transient RecordMetadata metadata,
            @Transient ColumnMapping columnMapping,
            boolean external
    ) {
        // Reopen off-heap lists atomically: reopen() can trip the memory limit, so if a
        // later one fails, close the ones already reopened. A caller that propagates the
        // failure without ever reaching close() then leaks nothing.
        try {
            pageAddresses.reopen();
            pageSizes.reopen();
            auxPageAddresses.reopen();
            auxPageSizes.reopen();
        } catch (Throwable th) {
            close();
            throw th;
        }
        // Reset frame-derived state and external flag.
        clear();

        this.columnCount = metadata.getColumnCount();
        columnTypes.clear();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            columnTypes.add(metadata.getColumnType(columnIndex));
        }
        this.columnMapping.copyFrom(columnMapping);
        this.external = external;
    }

    /**
     * Makes room for {@code frameCount} frames in the flat column lists, so that adding them does
     * not grow the lists one doubling, and one copy, at a time; a cache closed between executions
     * starts from its lists' initial capacities again.
     */
    public void reserveFrames(long frameCount) {
        if (frameCount > 0) {
            final long n = frameCount * columnCount;
            pageAddresses.ensureCapacity(n);
            pageSizes.ensureCapacity(n);
            auxPageAddresses.ensureCapacity(n);
            auxPageSizes.ensureCapacity(n);
        }
    }

    /**
     * Converts a frame index to an offset into the flat column arrays.
     * Usage: {@code cache.getPageAddresses().getQuick(cache.toColumnOffset(frameIndex) + columnIndex)}
     */
    public int toColumnOffset(int frameIndex) {
        return frameIndex * columnCount;
    }

    /**
     * Updates column addresses and parquet decoder for an existing frame entry.
     * Called during lazy partition opening to patch zero-address skeleton entries
     * with real mmap addresses. Does not change frame structure (size, format,
     * rowIdOffset, parquet row group indices).
     */
    public void updateAddresses(int frameIndex, @Transient PageFrame frame) {
        final int offset = frameIndex * columnCount;
        if (frame.getFormat() == PartitionFormat.NATIVE) {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                pageAddresses.set(offset + columnIndex, frame.getPageAddress(columnIndex));
                pageSizes.set(offset + columnIndex, frame.getPageSize(columnIndex));
                if (ColumnType.isVarSize(columnTypes.getQuick(columnIndex))) {
                    auxPageAddresses.set(offset + columnIndex, frame.getAuxPageAddress(columnIndex));
                    auxPageSizes.set(offset + columnIndex, frame.getAuxPageSize(columnIndex));
                }
            }
        } else {
            parquetDecoders.setQuick(frameIndex, frame.getParquetDecoder());
        }
    }
}

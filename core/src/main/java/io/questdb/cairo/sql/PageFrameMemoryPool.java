/*******************************************************************************
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
import io.questdb.cairo.Reopenable;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

/**
 * Provides addresses for page frames in both native and Parquet formats.
 * Memory in native page frames is mmapped, so no additional actions are
 * necessary. Parquet frames must be explicitly deserialized into
 * the in-memory native format before being accessed directly or via a Record.
 * Thus, a {@link #navigateTo(int)} call is required before accessing memory
 * that belongs to a page frame.
 * <p>
 * This pool is thread-unsafe as it may hold navigated Parquet partition data,
 * so it shouldn't be shared between multiple threads.
 */
public class PageFrameMemoryPool implements RecordRandomAccess, QuietCloseable, Mutable {
    private static final byte FRAME_MEMORY_MASK = 1 << 2;
    private static final byte RECORD_A_MASK = 1;
    private static final byte RECORD_B_MASK = 1 << 1;
    // LRU cache (most recently used buffers are to the right)
    private final ObjList<ParquetBuffers> cachedParquetBuffers;
    private final PageFrameMemoryImpl frameMemory;
    private final ObjList<ParquetBuffers> freeParquetBuffers;
    // Contains parquet to query column index mapping.
    private final IntList fromParquetColumnIndexes;
    private final int parquetCacheSize;
    // Contains [parquet_column_index, column_type] pairs.
    private final DirectIntList parquetColumns;
    private final PartitionDecoder parquetDecoder;
    private PageFrameAddressCache addressCache;

    public PageFrameMemoryPool(int parquetCacheSize) {
        try {
            this.parquetCacheSize = parquetCacheSize;
            cachedParquetBuffers = new ObjList<>(parquetCacheSize);
            freeParquetBuffers = new ObjList<>(parquetCacheSize);
            for (int i = 0; i < parquetCacheSize; i++) {
                freeParquetBuffers.add(new ParquetBuffers());
            }
            frameMemory = new PageFrameMemoryImpl();
            fromParquetColumnIndexes = new IntList(16);
            parquetColumns = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT, true);
            parquetDecoder = new PartitionDecoder();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        Misc.free(parquetDecoder);
        Misc.free(parquetColumns);
        releaseParquetBuffers();
    }

    @Override
    public void close() {
        Misc.free(parquetDecoder);
        Misc.free(parquetColumns);
        releaseParquetBuffers();
        addressCache = null;
    }

    /**
     * Navigates to the given frame, potentially deserializing it to in-memory format
     * (for Parquet partitions). After this call, the input record can be used to access
     * any row within the frame.
     */
    public void navigateTo(int frameIndex, PageFrameMemoryRecord record) {
        if (record.getFrameIndex() == frameIndex) {
            return;
        }

        final byte format = addressCache.getFrameFormat(frameIndex);
        final int columnOffset = addressCache.toColumnOffset(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            record.init(
                    frameIndex,
                    format,
                    addressCache.getRowIdOffset(frameIndex),
                    addressCache.getPageAddresses(),
                    addressCache.getAuxPageAddresses(),
                    addressCache.getPageSizes(),
                    addressCache.getAuxPageSizes(),
                    columnOffset
            );
        } else if (format == PartitionFormat.PARQUET) {
            openParquet(frameIndex);
            final byte usageBit = record.getLetter() == PageFrameMemoryRecord.RECORD_A_LETTER ? RECORD_A_MASK : RECORD_B_MASK;
            final ParquetBuffers parquetBuffers = nextFreeBuffers(frameIndex, usageBit);
            if (!parquetBuffers.cacheHit) {
                final int rowGroupIndex = addressCache.getParquetRowGroup(frameIndex);
                final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
                final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
                parquetBuffers.decode(parquetDecoder, parquetColumns, rowGroupIndex, rowGroupLo, rowGroupHi);
            }

            record.init(
                    frameIndex,
                    format,
                    addressCache.getRowIdOffset(frameIndex),
                    parquetBuffers.pageAddresses,
                    parquetBuffers.auxPageAddresses,
                    parquetBuffers.pageSizes,
                    parquetBuffers.auxPageSizes,
                    0 // parquet buffers use 0 offset since they're frame-specific
            );
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
        final int columnOffset = addressCache.toColumnOffset(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            frameMemory.pageAddresses = addressCache.getPageAddresses();
            frameMemory.auxPageAddresses = addressCache.getAuxPageAddresses();
            frameMemory.pageSizes = addressCache.getPageSizes();
            frameMemory.auxPageSizes = addressCache.getAuxPageSizes();
            frameMemory.columnOffset = columnOffset;
            frameMemory.currentRowGroupBuffer = null;
        } else if (format == PartitionFormat.PARQUET) {
            openParquet(frameIndex);
            final ParquetBuffers parquetBuffers = nextFreeBuffers(frameIndex, FRAME_MEMORY_MASK);
            if (!parquetBuffers.cacheHit) {
                final int rowGroupIndex = addressCache.getParquetRowGroup(frameIndex);
                final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
                final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
                parquetBuffers.decode(parquetDecoder, parquetColumns, rowGroupIndex, rowGroupLo, rowGroupHi);
            }

            frameMemory.currentRowGroupBuffer = parquetBuffers;
            frameMemory.pageAddresses = parquetBuffers.pageAddresses;
            frameMemory.auxPageAddresses = parquetBuffers.auxPageAddresses;
            frameMemory.pageSizes = parquetBuffers.pageSizes;
            frameMemory.auxPageSizes = parquetBuffers.auxPageSizes;
            frameMemory.columnOffset = 0; // parquet buffers use 0 offset
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
        final int columnOffset = addressCache.toColumnOffset(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            frameMemory.pageAddresses = addressCache.getPageAddresses();
            frameMemory.auxPageAddresses = addressCache.getAuxPageAddresses();
            frameMemory.pageSizes = addressCache.getPageSizes();
            frameMemory.auxPageSizes = addressCache.getAuxPageSizes();
            frameMemory.columnOffset = columnOffset;
            frameMemory.currentRowGroupBuffer = null;
        } else if (format == PartitionFormat.PARQUET) {
            openParquet(frameIndex, columnIndexes, true);
            final ParquetBuffers parquetBuffers = nextFreeBuffers(frameIndex, FRAME_MEMORY_MASK);
            if (!parquetBuffers.cacheHit) {
                final int rowGroupIndex = addressCache.getParquetRowGroup(frameIndex);
                final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
                final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
                parquetBuffers.decode(parquetDecoder, parquetColumns, rowGroupIndex, rowGroupLo, rowGroupHi);
            }

            frameMemory.currentRowGroupBuffer = parquetBuffers;
            frameMemory.pageAddresses = parquetBuffers.pageAddresses;
            frameMemory.auxPageAddresses = parquetBuffers.auxPageAddresses;
            frameMemory.pageSizes = parquetBuffers.pageSizes;
            frameMemory.auxPageSizes = parquetBuffers.auxPageSizes;
            frameMemory.columnOffset = 0; // parquet buffers use 0 offset
        }

        frameMemory.frameIndex = frameIndex;
        frameMemory.frameFormat = format;
        return frameMemory;
    }

    public void of(PageFrameAddressCache addressCache) {
        this.addressCache = addressCache;
        frameMemory.clear();
        Misc.free(parquetDecoder);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        navigateTo(Rows.toPartitionIndex(atRowId), frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(atRowId));
    }

    public void releaseParquetBuffers() {
        freeParquetBuffers.addAll(cachedParquetBuffers);
        cachedParquetBuffers.clear();
        Misc.freeObjListAndKeepObjects(freeParquetBuffers);
        frameMemory.clear();
    }

    // We don't use additional data structures to speed up the lookups
    // such as <frame_index, buffers> hash table. That's because we don't
    // expect the cache size to be large.
    @NotNull
    private ParquetBuffers nextFreeBuffers(int frameIndex, byte usageBit) {
        // First, clear the usage bit.
        for (int i = 0, n = cachedParquetBuffers.size(); i < n; i++) {
            ParquetBuffers buffers = cachedParquetBuffers.getQuick(i);
            buffers.usageFlags &= (byte) ~usageBit;
        }
        // Next, check if the frame is already in the cache.
        final int cached = cachedParquetBuffers.size();
        for (int i = 0; i < cached; i++) {
            ParquetBuffers buffers = cachedParquetBuffers.getQuick(i);
            if (buffers.frameIndex == frameIndex) {
                buffers.usageFlags |= usageBit;
                buffers.cacheHit = true;
                // Preserve LRU order.
                cachedParquetBuffers.setQuick(i, cachedParquetBuffers.getQuick(cached - 1));
                cachedParquetBuffers.setQuick(cached - 1, buffers);
                return buffers;
            }
        }
        // Check free buffers.
        final int free = freeParquetBuffers.size();
        if (free > 0) {
            ParquetBuffers buffers = freeParquetBuffers.getQuick(free - 1);
            freeParquetBuffers.remove(free - 1);
            buffers.reopen();
            buffers.frameIndex = frameIndex;
            buffers.usageFlags = usageBit;
            buffers.cacheHit = false;
            cachedParquetBuffers.add(buffers);
            return buffers;
        }
        // Finally, try to find an unused buffer in the cache.
        for (int i = 0; i < cached; i++) {
            ParquetBuffers buffers = cachedParquetBuffers.getQuick(i);
            if (buffers.usageFlags == 0) {
                buffers.frameIndex = frameIndex;
                buffers.usageFlags = usageBit;
                buffers.cacheHit = false;
                // Preserve LRU order.
                cachedParquetBuffers.setQuick(i, cachedParquetBuffers.getQuick(cached - 1));
                cachedParquetBuffers.setQuick(cached - 1, buffers);
                return buffers;
            }
        }
        // Give up.
        throw CairoException.critical(0)
                .put("insufficient memory pool size [size=").put(parquetCacheSize)
                .put(", usageBit=").put(usageBit)
                .put(']');
    }

    private void openParquet(int frameIndex) {
        final PartitionDecoder frameDecoder = addressCache.getParquetPartitionDecoder(frameIndex);
        if (parquetDecoder.getFileAddr() != frameDecoder.getFileAddr() || parquetDecoder.getFileSize() != frameDecoder.getFileSize()) {
            parquetDecoder.of(frameDecoder);
        }
        final PartitionDecoder.Metadata parquetMetadata = parquetDecoder.metadata();
        int readParquetColumnCount = addressCache.getColumnIndexes().size();
        if (parquetMetadata.getColumnCount() < readParquetColumnCount) {
            throw CairoException.nonCritical().put("parquet column count is less than number of queried table columns [parquetColumnCount=")
                    .put(parquetMetadata.getColumnCount())
                    .put(", columnCount=")
                    .put(readParquetColumnCount);
        }

        parquetColumns.reopen();
        parquetColumns.clear();
        fromParquetColumnIndexes.clear();
        fromParquetColumnIndexes.setAll(parquetMetadata.getColumnCount(), -1);
        for (int i = 0; i < readParquetColumnCount; i++) {
            final int parquetColumnIndex = addressCache.getColumnIndexes().getQuick(i);
            final int columnType = addressCache.getColumnTypes().getQuick(i);
            parquetColumns.add(parquetColumnIndex);
            fromParquetColumnIndexes.setQuick(parquetColumnIndex, i);
            parquetColumns.add(columnType);
        }
    }

    private void openParquet(int frameIndex, IntHashSet columnIndexes, boolean include) {
        final PartitionDecoder frameDecoder = addressCache.getParquetPartitionDecoder(frameIndex);
        if (parquetDecoder.getFileAddr() != frameDecoder.getFileAddr() || parquetDecoder.getFileSize() != frameDecoder.getFileSize()) {
            parquetDecoder.of(frameDecoder);
        }
        final PartitionDecoder.Metadata parquetMetadata = parquetDecoder.metadata();
        int readParquetColumnCount = addressCache.getColumnIndexes().size();
        if (parquetMetadata.getColumnCount() < readParquetColumnCount) {
            throw CairoException.nonCritical().put("parquet column count is less than number of queried table columns [parquetColumnCount=")
                    .put(parquetMetadata.getColumnCount())
                    .put(", columnCount=")
                    .put(readParquetColumnCount);
        }

        parquetColumns.reopen();
        parquetColumns.clear();
        fromParquetColumnIndexes.clear();
        fromParquetColumnIndexes.setAll(parquetMetadata.getColumnCount(), -1);
        for (int i = 0; i < readParquetColumnCount; i++) {
            if (include && columnIndexes.contains(i) || (!include && !columnIndexes.contains(i))) {
                final int parquetColumnIndex = addressCache.getColumnIndexes().getQuick(i);
                final int columnType = addressCache.getColumnTypes().getQuick(i);
                parquetColumns.add(parquetColumnIndex);
                fromParquetColumnIndexes.setQuick(parquetColumnIndex, i);
                parquetColumns.add(columnType);
            }
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
                currentRowGroupBuffer.decodeRemainingColumns(parquetDecoder, filterColumnIndexes.size(), parquetColumns, rowGroupIndex, rowGroupLo, rowGroupHi, filteredRows, fillWithNulls);
                return true;
            }
            return false;
        }
    }

    private class ParquetBuffers implements QuietCloseable, Reopenable {
        private final DirectLongList auxPageAddresses;
        private final DirectLongList auxPageSizes;
        private final DirectLongList pageAddresses;
        private final DirectLongList pageSizes;
        private final RowGroupBuffers rowGroupBuffers;
        private boolean cacheHit;
        private int frameIndex = -1;
        // Contains bits FRAME_MEMORY_MASK, RECORD_A_MASK and RECORD_B_MASK.
        private byte usageFlags;

        public ParquetBuffers() {
            this.auxPageAddresses = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.auxPageSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.pageAddresses = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.pageSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER, true);
        }

        @Override
        public void close() {
            Misc.free(pageAddresses);
            Misc.free(pageSizes);
            Misc.free(auxPageAddresses);
            Misc.free(auxPageSizes);
            Misc.free(rowGroupBuffers);
            usageFlags = 0;
            frameIndex = -1;
        }

        public void decode(PartitionDecoder parquetDecoder, DirectIntList parquetColumns, int rowGroup, int rowLo, int rowHi) {
            clearAddresses();
            if (parquetColumns.size() > 0) {
                // Decode the requested columns from the row group.
                parquetDecoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroup, rowLo, rowHi);

                // Now, we need to remap parquet column indexes to the query ones.
                final int columnCount = addressCache.getColumnCount();
                // Ensure capacity and zero out the lists.
                ensureCapacityAndZero(pageAddresses, columnCount);
                ensureCapacityAndZero(pageSizes, columnCount);
                ensureCapacityAndZero(auxPageAddresses, columnCount);
                ensureCapacityAndZero(auxPageSizes, columnCount);

                for (int i = 0, n = (int) (parquetColumns.size() / 2); i < n; i++) {
                    final int parquetColumnIndex = parquetColumns.get(2L * i);
                    final int columnIndex = fromParquetColumnIndexes.getQuick(parquetColumnIndex);
                    final int columnType = parquetColumns.get(2L * i + 1);
                    pageAddresses.set(columnIndex, rowGroupBuffers.getChunkDataPtr(i));
                    pageSizes.set(columnIndex, rowGroupBuffers.getChunkDataSize(i));
                    if (ColumnType.isVarSize(columnType)) {
                        auxPageAddresses.set(columnIndex, rowGroupBuffers.getChunkAuxPtr(i));
                        auxPageSizes.set(columnIndex, rowGroupBuffers.getChunkAuxSize(i));
                    }
                }
            }
        }

        public void decodeRemainingColumns(
                PartitionDecoder parquetDecoder,
                int columnOffset,
                DirectIntList parquetColumns,
                int rowGroup,
                int rowLo,
                int rowHi,
                DirectLongList filteredRows,
                boolean fillWithNulls
        ) {
            if (parquetColumns.size() > 0) {
                if (fillWithNulls) {
                    parquetDecoder.decodeRowGroupWithRowFilterFillNulls(rowGroupBuffers, columnOffset, parquetColumns, rowGroup, rowLo, rowHi, filteredRows);
                } else {
                    parquetDecoder.decodeRowGroupWithRowFilter(rowGroupBuffers, columnOffset, parquetColumns, rowGroup, rowLo, rowHi, filteredRows);
                }
                for (int i = 0, n = (int) (parquetColumns.size() / 2); i < n; i++) {
                    final int parquetColumnIndex = parquetColumns.get(2L * i);
                    final int columnIndex = fromParquetColumnIndexes.getQuick(parquetColumnIndex);
                    final int columnType = parquetColumns.get(2L * i + 1);
                    pageAddresses.set(columnIndex, rowGroupBuffers.getChunkDataPtr(columnOffset + i));
                    pageSizes.set(columnIndex, rowGroupBuffers.getChunkDataSize(columnOffset + i));
                    if (ColumnType.isVarSize(columnType)) {
                        auxPageAddresses.set(columnIndex, rowGroupBuffers.getChunkAuxPtr(columnOffset + i));
                        auxPageSizes.set(columnIndex, rowGroupBuffers.getChunkAuxSize(columnOffset + i));
                    }
                }
            }
        }

        @Override
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
    }
}

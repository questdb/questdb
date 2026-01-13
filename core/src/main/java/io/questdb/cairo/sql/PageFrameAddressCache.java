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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
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
    private final IntList columnIndexes = new IntList();
    private final IntList columnTypes = new IntList();
    private final ByteList frameFormats = new ByteList();
    private final LongList frameSizes = new LongList();
    private final DirectLongList pageAddresses;
    private final DirectLongList pageSizes;
    private final ObjList<PartitionDecoder> parquetPartitionDecoders = new ObjList<>();
    private final IntList parquetRowGroupHis = new IntList();
    private final IntList parquetRowGroupLos = new IntList();
    private final IntList parquetRowGroups = new IntList();
    // Makes it possible to determine real row id, not the one relative to the page.
    private final LongList rowIdOffsets = new LongList();
    private int columnCount;
    // True in case of external parquet files, false in case of table partition files.
    private boolean external;

    public PageFrameAddressCache() {
        this.auxPageAddresses = new DirectLongList(ADDRESS_LIST_INITIAL_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
        this.auxPageSizes = new DirectLongList(ADDRESS_LIST_INITIAL_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
        this.pageAddresses = new DirectLongList(ADDRESS_LIST_INITIAL_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
        this.pageSizes = new DirectLongList(ADDRESS_LIST_INITIAL_CAPACITY, MemoryTag.NATIVE_DEFAULT, true);
    }

    public void add(int frameIndex, @Transient PageFrame frame) {
        if (frameSizes.size() >= frameIndex + 1) {
            return; // The page frame is already cached
        }

        if (frame.getFormat() == PartitionFormat.NATIVE) {
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
        }

        frameSizes.add(frame.getPartitionHi() - frame.getPartitionLo());
        frameFormats.add(frame.getFormat());
        PartitionDecoder decoder = frame.getParquetPartitionDecoder();
        parquetPartitionDecoders.add(decoder);
        assert (decoder != null && (decoder.getFileSize() > 0)) || frame.getFormat() != PartitionFormat.PARQUET;
        parquetRowGroups.add(frame.getParquetRowGroup());
        parquetRowGroupLos.add(frame.getParquetRowGroupLo());
        parquetRowGroupHis.add(frame.getParquetRowGroupHi());
        rowIdOffsets.add(Rows.toRowID(frame.getPartitionIndex(), frame.getPartitionLo()));
    }

    @Override
    public void clear() {
        frameSizes.clear();
        frameFormats.clear();
        parquetPartitionDecoders.clear();
        parquetRowGroups.clear();
        parquetRowGroupLos.clear();
        parquetRowGroupHis.clear();
        pageAddresses.clear();
        auxPageAddresses.clear();
        pageSizes.clear();
        auxPageSizes.clear();
        rowIdOffsets.clear();
        external = false;
    }

    @Override
    public void close() {
        pageAddresses.close();
        pageSizes.close();
        auxPageAddresses.close();
        auxPageSizes.close();
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

    // returns local (query) to table reader index mapping
    public IntList getColumnIndexes() {
        return columnIndexes;
    }

    public IntList getColumnTypes() {
        return columnTypes;
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

    public PartitionDecoder getParquetPartitionDecoder(int frameIndex) {
        final PartitionDecoder decoder = parquetPartitionDecoders.getQuick(frameIndex);
        assert decoder != null;
        return decoder;
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

    public boolean isExternal() {
        return external;
    }

    public boolean isVarSizeColumn(int columnIndex) {
        return ColumnType.isVarSize(columnTypes.getQuick(columnIndex));
    }

    public void of(@Transient RecordMetadata metadata, @Transient IntList columnIndexes, boolean external) {
        // Reopen off-heap lists in case they were closed.
        pageAddresses.reopen();
        pageSizes.reopen();
        auxPageAddresses.reopen();
        auxPageSizes.reopen();
        // Reset frame-derived state and external flag.
        clear();

        this.columnCount = metadata.getColumnCount();
        columnTypes.clear();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            columnTypes.add(metadata.getColumnType(columnIndex));
        }
        this.columnIndexes.clear();
        this.columnIndexes.addAll(columnIndexes);
        this.external = external;
    }

    /**
     * Converts a frame index to an offset into the flat column arrays.
     * Usage: {@code cache.getPageAddresses().getQuick(cache.toColumnOffset(frameIndex) + columnIndex)}
     */
    public int toColumnOffset(int frameIndex) {
        return frameIndex * columnCount;
    }
}

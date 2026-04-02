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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.std.DirectIntList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

/**
 * Parquet partition decoder that reads metadata from the {@code _pm} sidecar file
 * instead of parsing the parquet footer via thrift.
 * <p>
 * Java owns all metadata via {@link ParquetMetaFileReader}. Rust is a stateless
 * decode engine that receives explicit parameters per decode call. This is the
 * table partition path — the {@code read_parquet()} SQL function uses the
 * separate {@link PartitionDecoder} which parses the parquet footer.
 * <p>
 * For cold storage (future), only the {@code _pm} file is available locally.
 * Column chunks are lazily downloaded by byte range. The same decode API works.
 */
public class ParquetMetaPartitionDecoder implements ParquetDecoder, QuietCloseable {
    private final ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
    private long decodeContextPtr;
    private long parquetAddr;
    private long parquetSize;
    private long parquetMetaAddr;
    private long parquetMetaSize;
    private long allocator;
    private boolean owned;

    public static boolean decodeNoNeedToDecodeFlag(long encodedIndex) {
        return (encodedIndex & 1) == 1;
    }

    public static int decodeRowGroupIndex(long encodedIndex) {
        return (int) ((encodedIndex >> 1) - 1);
    }

    /**
     * Initializes the decoder with mmapped _pm and parquet file regions.
     *
     * @param parquetMetaAddr      base address of the mmapped _pm file
     * @param parquetMetaSize      size of the mmapped _pm file
     * @param parquetAddr base address of the mmapped parquet file
     * @param parquetSize size of the mmapped parquet file
     * @param memoryTag   memory tag for native allocations
     */
    public void of(long parquetMetaAddr, long parquetMetaSize, long parquetAddr, long parquetSize, int memoryTag) {
        destroy();
        this.parquetMetaAddr = parquetMetaAddr;
        this.parquetMetaSize = parquetMetaSize;
        this.parquetAddr = parquetAddr;
        this.parquetSize = parquetSize;
        this.allocator = Unsafe.getNativeAllocator(memoryTag);
        this.parquetMetaReader.of(parquetMetaAddr, parquetMetaSize);
        this.owned = true;
    }

    /**
     * Creates a non-owning shallow copy that shares mmapped regions with the source.
     * Each copy gets its own DecodeContext for thread-safe decoding.
     * The source must remain valid for the lifetime of this instance.
     */
    public void of(ParquetMetaPartitionDecoder other) {
        this.parquetMetaAddr = other.parquetMetaAddr;
        this.parquetMetaSize = other.parquetMetaSize;
        this.parquetAddr = other.parquetAddr;
        this.parquetSize = other.parquetSize;
        this.allocator = other.allocator;
        this.parquetMetaReader.of(parquetMetaAddr, parquetMetaSize);
        if (decodeContextPtr != 0) {
            PartitionDecoder.destroyDecodeContext(decodeContextPtr);
            decodeContextPtr = 0;
        }
        owned = false;
    }

    public ParquetMetaFileReader metadata() {
        return parquetMetaReader;
    }

    public long getParquetAddr() {
        return parquetAddr;
    }

    public long getParquetSize() {
        return parquetSize;
    }

    public long getParquetMetaAddr() {
        return parquetMetaAddr;
    }

    public long getParquetMetaSize() {
        return parquetMetaSize;
    }

    /**
     * Returns the parquet file address (for cache invalidation and export).
     * Equivalent to {@link PartitionDecoder#getFileAddr()}.
     */
    public long getFileAddr() {
        return parquetAddr;
    }

    /**
     * Returns the parquet file size.
     * Equivalent to {@link PartitionDecoder#getFileSize()}.
     */
    public long getFileSize() {
        return parquetSize;
    }

    @Override
    public int getColumnCount() {
        return parquetMetaReader.getColumnCount();
    }

    @Override
    public int getColumnId(int columnIndex) {
        return parquetMetaReader.getColumnId(columnIndex);
    }

    public long rowGroupMinTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        return parquetMetaReader.getRowGroupMinTimestamp(rowGroupIndex, timestampColumnIndex);
    }

    public long rowGroupMaxTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        return parquetMetaReader.getRowGroupMaxTimestamp(rowGroupIndex, timestampColumnIndex);
    }

    /**
     * Decodes a row group. The {@code columns} list uses the same
     * {@code [parquet_column_index, column_type]} pair format as
     * {@link PartitionDecoder#decodeRowGroup} for compatibility with
     * {@code PageFrameMemoryPool}. The column type from Java is used for
     * Symbol→Varchar and Varchar→VarcharSlice overrides; the base type
     * comes from the {@code _pm} file.
     *
     * @param rowGroupBuffers output buffers
     * @param columns         [parquet_column_index, column_type] pairs
     * @param rowGroupIndex   row group to decode
     * @param rowLo           first row (inclusive) within the row group
     * @param rowHi           last row (exclusive) within the row group
     * @return decoded row count
     */
    public int decodeRowGroup(
            RowGroupBuffers rowGroupBuffers,
            DirectIntList columns,
            int rowGroupIndex,
            int rowLo,
            int rowHi
    ) {
        if (decodeContextPtr == 0) {
            decodeContextPtr = PartitionDecoder.createDecodeContext(parquetAddr, parquetSize);
        }
        return decodeRowGroup(
                allocator,
                decodeContextPtr,
                parquetAddr,
                parquetSize,
                parquetMetaAddr,
                parquetMetaSize,
                rowGroupBuffers.ptr(),
                columns.getAddress(),
                (int) (columns.size() >>> 1),
                rowGroupIndex,
                rowLo,
                rowHi
        );
    }

    public void decodeRowGroupWithRowFilter(
            RowGroupBuffers rowGroupBuffers,
            int columnOffset,
            DirectIntList columns,
            int rowGroupIndex,
            int rowLo,
            int rowHi,
            io.questdb.std.DirectLongList filteredRows
    ) {
        if (decodeContextPtr == 0) {
            decodeContextPtr = PartitionDecoder.createDecodeContext(parquetAddr, parquetSize);
        }
        decodeRowGroupWithRowFilter(
                allocator, decodeContextPtr, parquetAddr, parquetSize,
                parquetMetaAddr, parquetMetaSize, rowGroupBuffers.ptr(), columnOffset,
                columns.getAddress(), (int) (columns.size() >>> 1),
                rowGroupIndex, rowLo, rowHi,
                filteredRows.getAddress(), filteredRows.size()
        );
    }

    public void decodeRowGroupWithRowFilterFillNulls(
            RowGroupBuffers rowGroupBuffers,
            int columnOffset,
            DirectIntList columns,
            int rowGroupIndex,
            int rowLo,
            int rowHi,
            io.questdb.std.DirectLongList filteredRows
    ) {
        if (decodeContextPtr == 0) {
            decodeContextPtr = PartitionDecoder.createDecodeContext(parquetAddr, parquetSize);
        }
        decodeRowGroupWithRowFilterFillNulls(
                allocator, decodeContextPtr, parquetAddr, parquetSize,
                parquetMetaAddr, parquetMetaSize, rowGroupBuffers.ptr(), columnOffset,
                columns.getAddress(), (int) (columns.size() >>> 1),
                rowGroupIndex, rowLo, rowHi,
                filteredRows.getAddress(), filteredRows.size()
        );
    }

    /**
     * Check if a row group can be skipped based on min/max statistics and bloom
     * filter conditions. Reads stats from _pm and bloom filters from the parquet file.
     */
    public boolean canSkipRowGroup(int rowGroupIndex, io.questdb.std.DirectLongList filters, long filterBufEnd) {
        assert parquetMetaAddr != 0;
        assert filters.size() % io.questdb.griffin.engine.table.ParquetRowGroupFilter.LONGS_PER_FILTER == 0;
        return canSkipRowGroup(
                parquetAddr, parquetSize, parquetMetaAddr, parquetMetaSize,
                rowGroupIndex,
                filters.getAddress(),
                (int) (filters.size() / io.questdb.griffin.engine.table.ParquetRowGroupFilter.LONGS_PER_FILTER),
                filterBufEnd
        );
    }

    /**
     * Checks if all filter values are absent from a bloom filter for a column chunk.
     *
     * @return true if all values are absent (row group can be skipped)
     */
    public boolean checkBloomFilter(
            int rowGroupIndex,
            int columnIndex,
            int physicalType,
            int fixedByteLen,
            long filterValuesPtr,
            int filterCount,
            long filterBufEnd,
            boolean hasNulls,
            boolean isDecimal,
            int qdbColumnType
    ) {
        long bloomOffset = parquetMetaReader.getChunkBloomFilterOffset(rowGroupIndex, columnIndex);
        if (bloomOffset == 0) {
            return false;
        }
        return checkBloomFilter(
                parquetAddr,
                parquetSize,
                bloomOffset,
                physicalType,
                fixedByteLen,
                filterValuesPtr,
                filterCount,
                filterBufEnd,
                hasNulls,
                isDecimal,
                qdbColumnType
        );
    }

    public long findRowGroupByTimestamp(
            long timestamp,
            long rowLo,
            long rowHi,
            int timestampColumnIndex
    ) {
        return findRowGroupByTimestamp(
                allocator,
                parquetAddr,
                parquetSize,
                parquetMetaAddr,
                parquetMetaSize,
                timestamp,
                rowLo,
                rowHi,
                timestampColumnIndex
        );
    }

    @Override
    public void close() {
        destroy();
    }

    private void destroy() {
        if (decodeContextPtr != 0) {
            PartitionDecoder.destroyDecodeContext(decodeContextPtr);
            decodeContextPtr = 0;
        }
        if (owned) {
            parquetMetaReader.clear();
            parquetMetaAddr = 0;
            parquetMetaSize = 0;
            parquetAddr = 0;
            parquetSize = 0;
        }
    }

    // ── Native methods ─────────────────────────────────────────────────

    private static native int decodeRowGroup(
            long allocator,
            long decodeContextPtr,
            long parquetFilePtr,
            long parquetFileSize,
            long parquetMetaPtr,
            long parquetMetaSize,
            long rowGroupBufsPtr,
            long columnsPtr,
            int columnCount,
            int rowGroupIndex,
            int rowLo,
            int rowHi
    ) throws CairoException;

    private static native boolean checkBloomFilter(
            long parquetFilePtr,
            long parquetFileSize,
            long bloomFilterOffset,
            int physicalType,
            int fixedByteLen,
            long filterValuesPtr,
            int filterCount,
            long filterBufEnd,
            boolean hasNulls,
            boolean isDecimal,
            int qdbColumnType
    );

    private static native void decodeRowGroupWithRowFilter(
            long allocator,
            long decodeContextPtr,
            long parquetFilePtr,
            long parquetFileSize,
            long parquetMetaPtr,
            long parquetMetaSize,
            long rowGroupBufsPtr,
            int columnOffset,
            long columnsPtr,
            int columnCount,
            int rowGroupIndex,
            int rowLo,
            int rowHi,
            long filteredRowsPtr,
            long filteredRowsSize
    ) throws CairoException;

    private static native void decodeRowGroupWithRowFilterFillNulls(
            long allocator,
            long decodeContextPtr,
            long parquetFilePtr,
            long parquetFileSize,
            long parquetMetaPtr,
            long parquetMetaSize,
            long rowGroupBufsPtr,
            int columnOffset,
            long columnsPtr,
            int columnCount,
            int rowGroupIndex,
            int rowLo,
            int rowHi,
            long filteredRowsPtr,
            long filteredRowsSize
    ) throws CairoException;

    private static native boolean canSkipRowGroup(
            long parquetFilePtr,
            long parquetFileSize,
            long parquetMetaPtr,
            long parquetMetaSize,
            int rowGroupIndex,
            long filtersPtr,
            int filterCount,
            long filterBufEnd
    ) throws CairoException;

    private static native long findRowGroupByTimestamp(
            long allocator,
            long parquetFilePtr,
            long parquetFileSize,
            long parquetMetaPtr,
            long parquetMetaSize,
            long timestamp,
            long rowLo,
            long rowHi,
            int timestampColumnIndex
    ) throws CairoException;

    static {
        Os.init();
    }
}

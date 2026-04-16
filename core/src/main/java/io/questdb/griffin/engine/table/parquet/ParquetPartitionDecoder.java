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
import io.questdb.cairo.TableToken;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
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
 * separate {@link ParquetFileDecoder} which parses the parquet footer.
 */
public class ParquetPartitionDecoder implements ParquetDecoder, QuietCloseable {
    private final ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
    private long allocator;
    private long decodeContextPtr;
    private long nameTxn;
    private long parquetAddr;
    private long parquetMetaAddr;
    private long parquetMetaSize;
    private long parquetSize;
    private int partitionBy;
    private TableToken table;
    private long timestamp;
    private int timestampType;

    public static boolean decodeNoNeedToDecodeFlag(long encodedIndex) {
        return (encodedIndex & 1) == 1;
    }

    public static int decodeRowGroupIndex(long encodedIndex) {
        return (int) ((encodedIndex >> 1) - 1);
    }

    @Override
    public void close() {
        destroy();
    }

    /**
     * Decodes a row group. The {@code columns} list uses the same {@code [parquet_column_index, column_type]}
     * pair format as {@link ParquetFileDecoder#decodeRowGroup(RowGroupBuffers, DirectIntList, int, int, int)}
     * for compatibility with {@code PageFrameMemoryPool}. The column type from Java is used for
     * Symbol->Varchar and Varchar->VarcharSlice overrides; the base type comes from the {@code _pm} file.
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
            decodeContextPtr = ParquetFileDecoder.createDecodeContext(parquetAddr, parquetSize);
        }
        return decodeRowGroup(
                allocator,
                decodeContextPtr,
                parquetAddr,
                parquetSize,
                parquetMetaReader.getOrCreateNativeReaderPtr(),
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
            DirectLongList filteredRows
    ) {
        if (decodeContextPtr == 0) {
            decodeContextPtr = ParquetFileDecoder.createDecodeContext(parquetAddr, parquetSize);
        }
        decodeRowGroupWithRowFilter(
                allocator, decodeContextPtr, parquetAddr, parquetSize,
                parquetMetaReader.getOrCreateNativeReaderPtr(), rowGroupBuffers.ptr(), columnOffset,
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
            DirectLongList filteredRows
    ) {
        if (decodeContextPtr == 0) {
            decodeContextPtr = ParquetFileDecoder.createDecodeContext(parquetAddr, parquetSize);
        }
        decodeRowGroupWithRowFilterFillNulls(
                allocator, decodeContextPtr, parquetAddr, parquetSize,
                parquetMetaReader.getOrCreateNativeReaderPtr(), rowGroupBuffers.ptr(), columnOffset,
                columns.getAddress(), (int) (columns.size() >>> 1),
                rowGroupIndex, rowLo, rowHi,
                filteredRows.getAddress(), filteredRows.size()
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
                parquetMetaReader.getOrCreateNativeReaderPtr(),
                timestamp,
                rowLo,
                rowHi,
                timestampColumnIndex
        );
    }

    @Override
    public int getColumnCount() {
        return parquetMetaReader.getColumnCount();
    }

    @Override
    public int getColumnId(int columnIndex) {
        return parquetMetaReader.getColumnId(columnIndex);
    }

    /**
     * Returns the parquet file address (for cache invalidation and export).
     * Equivalent to {@link ParquetFileDecoder#getFileAddr()}.
     */
    public long getFileAddr() {
        return parquetAddr;
    }

    /**
     * Returns the parquet file size.
     * Equivalent to {@link ParquetFileDecoder#getFileSize()}.
     */
    public long getFileSize() {
        return parquetSize;
    }

    public long getParquetAddr() {
        return parquetAddr;
    }

    public long getParquetMetaAddr() {
        return parquetMetaAddr;
    }

    public long getParquetMetaSize() {
        return parquetMetaSize;
    }

    public long getParquetSize() {
        return parquetSize;
    }

    public ParquetMetaFileReader metadata() {
        return parquetMetaReader;
    }

    /**
     * Initializes the decoder with mmapped _pm and parquet file regions.
     *
     * @param parquetMetaAddr base address of the mmapped _pm file
     * @param parquetMetaSize size of the mmapped _pm file
     * @param parquetAddr     base address of the mmapped parquet file
     * @param parquetSize     size of the mmapped parquet file
     * @param memoryTag       memory tag for native allocations
     */
    public void of(long parquetMetaAddr, long parquetMetaSize, long parquetAddr, long parquetSize, TableToken table, int partitionBy, int timestampType, long timestamp, long nameTxn, int memoryTag) {
        destroy();
        try {
            this.parquetMetaAddr = parquetMetaAddr;
            this.parquetMetaSize = parquetMetaSize;
            this.parquetAddr = parquetAddr;
            this.parquetSize = parquetSize;
            this.partitionBy = partitionBy;
            this.timestamp = timestamp;
            this.nameTxn = nameTxn;
            this.timestampType = timestampType;
            this.table = table;
            this.allocator = Unsafe.getNativeAllocator(memoryTag);
            this.parquetMetaReader.of(parquetMetaAddr, parquetMetaSize);
            if (!this.parquetMetaReader.resolveFooter(parquetSize)) {
                throw CairoException.critical(0).put("could not resolve _pm footer");
            }
        } catch (Throwable t) {
            destroy();
            throw t;
        }
    }

    /**
     * Initializes the decoder with {@code ParquetMetaFileReader} and parquet file regions.
     *
     * @param reader      parquet metadata file reader with the footer already resolved
     * @param parquetAddr base address of the mmapped parquet file
     * @param parquetSize size of the mmapped parquet file
     * @param memoryTag   memory tag for native allocations
     */
    public void of(ParquetMetaFileReader reader, long parquetAddr, long parquetSize, int memoryTag) {
        destroy();
        this.parquetMetaAddr = reader.getAddr();
        this.parquetMetaSize = reader.getFileSize();
        this.parquetAddr = parquetAddr;
        this.parquetSize = parquetSize;
        this.allocator = Unsafe.getNativeAllocator(memoryTag);
        this.parquetMetaReader.of(reader);
    }

    /**
     * Creates a shallow copy that shares mmapped regions with the source.
     * Each copy gets its own DecodeContext for thread-safe decoding and its
     * own {@link ParquetMetaFileReader} (with its own lazy native handle).
     * The source must remain valid for the lifetime of this instance — the
     * underlying mmap must outlive both the source and the copy.
     */
    public void of(ParquetPartitionDecoder other) {
        destroy();
        this.parquetMetaAddr = other.parquetMetaAddr;
        this.parquetMetaSize = other.parquetMetaSize;
        this.parquetAddr = other.parquetAddr;
        this.parquetSize = other.parquetSize;
        this.allocator = other.allocator;
        this.parquetMetaReader.of(other.parquetMetaReader);
    }

    public long rowGroupMaxTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        return parquetMetaReader.getRowGroupMaxTimestamp(rowGroupIndex, timestampColumnIndex);
    }

    public long rowGroupMinTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        return parquetMetaReader.getRowGroupMinTimestamp(rowGroupIndex, timestampColumnIndex);
    }

    private static native int decodeRowGroup(
            long allocator,
            long decodeContextPtr,
            long parquetFilePtr,
            long parquetFileSize,
            long parquetMetaReaderPtr,
            long rowGroupBufsPtr,
            long columnsPtr,
            int columnCount,
            int rowGroupIndex,
            int rowLo,
            int rowHi
    ) throws CairoException;

    private static native void decodeRowGroupWithRowFilter(
            long allocator,
            long decodeContextPtr,
            long parquetFilePtr,
            long parquetFileSize,
            long parquetMetaReaderPtr,
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
            long parquetMetaReaderPtr,
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

    private static native long findRowGroupByTimestamp(
            long allocator,
            long parquetFilePtr,
            long parquetFileSize,
            long parquetMetaReaderPtr,
            long timestamp,
            long rowLo,
            long rowHi,
            int timestampColumnIndex
    ) throws CairoException;

    private void destroy() {
        if (decodeContextPtr != 0) {
            ParquetFileDecoder.destroyDecodeContext(decodeContextPtr);
            decodeContextPtr = 0;
        }
        parquetMetaReader.clear();
        parquetMetaAddr = 0;
        parquetMetaSize = 0;
        parquetAddr = 0;
        parquetSize = 0;
    }

    static {
        Os.init();
    }
}

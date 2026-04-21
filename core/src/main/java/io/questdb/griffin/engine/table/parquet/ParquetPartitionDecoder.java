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
import io.questdb.cairo.TableUtils;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

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
    // Pre-computed [byte_offset, byte_length] pairs handed to the resolver. Lazy.
    private DirectLongList byteRanges;
    // Lazy [addr, size] pairs filled by ParquetColumnChunkResolver. Reused across decode calls.
    private DirectLongList chunks;
    // Built on-demand on the cold path; null until first cold decode, so hot-path-only
    // decoders don't account against NATIVE_PATH.
    private Path coldPartitionPath;
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
        ensureDecodeContext();
        final int columnsSize = (int) (columns.size() >>> 1);
        if (parquetAddr != 0) {
            return decodeRowGroup(
                    decodeContextPtr,
                    parquetAddr,
                    parquetSize,
                    parquetMetaReader.getOrCreateNativeReaderPtr(),
                    rowGroupBuffers.ptr(),
                    columns.getAddress(),
                    columnsSize,
                    rowGroupIndex,
                    rowLo,
                    rowHi
            );
        }
        final ParquetColumnChunkResolver resolver = requireColdResolver();
        final DirectLongList chunkList = resolveColdChunks(resolver, columns, columnsSize, rowGroupIndex);
        try {
            return decodeRowGroupFromBuffers(
                    decodeContextPtr,
                    parquetMetaReader.getOrCreateNativeReaderPtr(),
                    rowGroupBuffers.ptr(),
                    columns.getAddress(),
                    columnsSize,
                    rowGroupIndex,
                    chunkList.getAddress(),
                    rowLo,
                    rowHi
            );
        } finally {
            resolver.release(chunkList, columnsSize);
        }
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
        ensureDecodeContext();
        final int columnsSize = (int) (columns.size() >>> 1);
        if (parquetAddr != 0) {
            decodeRowGroupWithRowFilter(
                    decodeContextPtr, parquetAddr, parquetSize,
                    parquetMetaAddr, parquetMetaSize, rowGroupBuffers.ptr(), columnOffset,
                    columns.getAddress(), columnsSize,
                    rowGroupIndex, rowLo, rowHi,
                    filteredRows.getAddress(), filteredRows.size()
            );
            return;
        }
        final ParquetColumnChunkResolver resolver = requireColdResolver();
        final DirectLongList chunkList = resolveColdChunks(resolver, columns, columnsSize, rowGroupIndex);
        try {
            decodeRowGroupWithRowFilterFromBuffers(
                    decodeContextPtr, parquetMetaReader.getOrCreateNativeReaderPtr(),
                    rowGroupBuffers.ptr(), columnOffset,
                    columns.getAddress(), columnsSize,
                    rowGroupIndex, chunkList.getAddress(),
                    rowLo, rowHi,
                    filteredRows.getAddress(), filteredRows.size()
            );
        } finally {
            resolver.release(chunkList, columnsSize);
        }
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
        ensureDecodeContext();
        final int columnsSize = (int) (columns.size() >>> 1);
        if (parquetAddr != 0) {
            decodeRowGroupWithRowFilterFillNulls(
                    decodeContextPtr, parquetAddr, parquetSize,
                    parquetMetaReader.getOrCreateNativeReaderPtr(),
                    rowGroupBuffers.ptr(), columnOffset,
                    columns.getAddress(), columnsSize,
                    rowGroupIndex, rowLo, rowHi,
                    filteredRows.getAddress(), filteredRows.size()
            );
            return;
        }
        final ParquetColumnChunkResolver resolver = requireColdResolver();
        final DirectLongList chunkList = resolveColdChunks(resolver, columns, columnsSize, rowGroupIndex);
        try {
            decodeRowGroupWithRowFilterFillNullsFromBuffers(
                    decodeContextPtr, parquetMetaReader.getOrCreateNativeReaderPtr(),
                    rowGroupBuffers.ptr(), columnOffset,
                    columns.getAddress(), columnsSize,
                    rowGroupIndex, chunkList.getAddress(),
                    rowLo, rowHi,
                    filteredRows.getAddress(), filteredRows.size()
            );
        } finally {
            resolver.release(chunkList, columnsSize);
        }
    }

    public long findRowGroupByTimestamp(
            long timestamp,
            long rowLo,
            long rowHi,
            int timestampColumnIndex
    ) {
        if (parquetAddr != 0) {
            return findRowGroupByTimestamp(
                    parquetAddr,
                    parquetSize,
                    parquetMetaReader.getOrCreateNativeReaderPtr(),
                    timestamp,
                    rowLo,
                    rowHi,
                    timestampColumnIndex
            );
        }
        return findRowGroupByTimestampCold(
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
    /**
     * Hot-path-only initialization. Use when the parquet file is mmapped
     * locally (write/restore/index/O3 callers). The cold-path resolver
     * fields are left null/zero; calling cold-path entry points after this
     * overload will throw because the partition path cannot be built.
     */
    public void of(long parquetMetaAddr, long parquetMetaSize, long parquetAddr, long parquetSize, int memoryTag) {
        of(parquetMetaAddr, parquetMetaSize, parquetAddr, parquetSize, null, 0, 0, 0L, 0L, memoryTag);
    }

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
        this.table = other.table;
        this.partitionBy = other.partitionBy;
        this.timestampType = other.timestampType;
        this.timestamp = other.timestamp;
        this.nameTxn = other.nameTxn;
    }

    public long rowGroupMaxTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        return parquetMetaReader.getRowGroupMaxTimestamp(rowGroupIndex, timestampColumnIndex);
    }

    public long rowGroupMinTimestamp(int rowGroupIndex, int timestampColumnIndex) {
        return parquetMetaReader.getRowGroupMinTimestamp(rowGroupIndex, timestampColumnIndex);
    }

    private static native int decodeRowGroup(
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

    private static native int decodeRowGroupFromBuffers(
            long decodeContextPtr,
            long parquetMetaReaderPtr,
            long rowGroupBufsPtr,
            long columnsPtr,
            int columnCount,
            int rowGroupIndex,
            long chunksPtr,
            int rowLo,
            int rowHi
    ) throws CairoException;

    private static native void decodeRowGroupWithRowFilter(
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

    private static native void decodeRowGroupWithRowFilterFillNullsFromBuffers(
            long decodeContextPtr,
            long parquetMetaReaderPtr,
            long rowGroupBufsPtr,
            int columnOffset,
            long columnsPtr,
            int columnCount,
            int rowGroupIndex,
            long chunksPtr,
            int rowLo,
            int rowHi,
            long filteredRowsPtr,
            long filteredRowsSize
    ) throws CairoException;

    private static native void decodeRowGroupWithRowFilterFromBuffers(
            long decodeContextPtr,
            long parquetMetaReaderPtr,
            long rowGroupBufsPtr,
            int columnOffset,
            long columnsPtr,
            int columnCount,
            int rowGroupIndex,
            long chunksPtr,
            int rowLo,
            int rowHi,
            long filteredRowsPtr,
            long filteredRowsSize
    ) throws CairoException;

    private static native long findRowGroupByTimestamp(
            long parquetFilePtr,
            long parquetFileSize,
            long parquetMetaReaderPtr,
            long timestamp,
            long rowLo,
            long rowHi,
            int timestampColumnIndex
    ) throws CairoException;

    private static native long findRowGroupByTimestampCold(
            long parquetMetaReaderPtr,
            long timestamp,
            long rowLo,
            long rowHi,
            int timestampColumnIndex
    ) throws CairoException;

    private static DirectLongList resetLongList(DirectLongList list, int columnsSize) {
        final long required = 2L * columnsSize;
        list.clear();
        list.ensureCapacity(required);
        list.setPos(required);
        return list;
    }

    private void buildColdPartitionPath() {
        if (table == null) {
            throw CairoException.critical(0)
                    .put("cannot build cold-storage partition path: decoder was initialized via the hot-path-only of() overload");
        }
        if (coldPartitionPath == null) {
            coldPartitionPath = new Path();
        }
        coldPartitionPath.of(table.getDirName());
        TableUtils.setPathForParquetPartition(coldPartitionPath, timestampType, partitionBy, timestamp, nameTxn);
    }

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
        byteRanges = Misc.free(byteRanges);
        chunks = Misc.free(chunks);
        coldPartitionPath = Misc.free(coldPartitionPath);
    }

    private DirectLongList ensureByteRanges(int columnsSize) {
        if (byteRanges == null) {
            byteRanges = new DirectLongList(2L * columnsSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
        }
        return resetLongList(byteRanges, columnsSize);
    }

    private DirectLongList ensureChunks(int columnsSize) {
        if (chunks == null) {
            chunks = new DirectLongList(2L * columnsSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
        }
        return resetLongList(chunks, columnsSize);
    }

    private void ensureDecodeContext() {
        if (decodeContextPtr == 0) {
            decodeContextPtr = ParquetFileDecoder.createDecodeContext(parquetAddr, parquetSize);
        }
    }

    private ParquetColumnChunkResolver requireColdResolver() {
        final ParquetColumnChunkResolver resolver = ParquetColumnChunkResolver.Holder.INSTANCE;
        if (resolver == null) {
            throw CairoException.critical(0)
                    .put("parquet file not available locally and no column chunk resolver configured");
        }
        return resolver;
    }

    private DirectLongList resolveColdChunks(
            ParquetColumnChunkResolver resolver,
            DirectIntList columns,
            int columnsSize,
            int rowGroupIndex
    ) {
        final DirectLongList ranges = ensureByteRanges(columnsSize);
        final long columnsAddr = columns.getAddress();
        for (int i = 0; i < columnsSize; i++) {
            // Each (parquet_column_index, column_type) pair occupies 8 bytes (two ints).
            // The parquet column index is the first int.
            final int parquetColumnIndex = Unsafe.getUnsafe().getInt(columnsAddr + (i * 8L));
            ranges.set(2L * i, parquetMetaReader.getChunkByteRangeStart(rowGroupIndex, parquetColumnIndex));
            ranges.set(2L * i + 1L, parquetMetaReader.getChunkTotalCompressed(rowGroupIndex, parquetColumnIndex));
        }
        final DirectLongList chunkList = ensureChunks(columnsSize);
        buildColdPartitionPath();
        try {
            resolver.resolve(coldPartitionPath, ranges, columnsSize, chunkList);
        } catch (Throwable t) {
            // The resolver may have allocated buffers and partially populated chunkList before throwing.
            // Release defensively so those native buffers do not leak.
            try {
                resolver.release(chunkList, columnsSize);
            } catch (Throwable releaseFailure) {
                t.addSuppressed(releaseFailure);
            }
            throw t;
        }
        return chunkList;
    }

    static {
        Os.init();
    }
}

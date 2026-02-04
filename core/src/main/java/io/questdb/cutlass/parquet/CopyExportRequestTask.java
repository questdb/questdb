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

package io.questdb.cutlass.parquet;


import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.SymbolMapWriter.HEADER_SIZE;
import static io.questdb.griffin.engine.table.parquet.PartitionEncoder.*;

public class CopyExportRequestTask implements Mutable, QuietCloseable {
    private final StreamPartitionParquetExporter streamPartitionParquetExporter = new StreamPartitionParquetExporter();
    private int compressionCodec;
    private int compressionLevel;
    private @Nullable CreateTableOperation createOp;
    private int dataPageSize;
    private boolean descending;
    private CopyExportContext.ExportTaskEntry entry;
    private RecordCursorFactory factory;
    private CharSequence fileName;
    private RecordMetadata metadata;
    private long now;
    private int nowTimestampType;
    private @Nullable PageFrameCursor pageFrameCursor;
    private int parquetVersion;
    private boolean rawArrayEncoding;
    private @Nullable RecordCursorFactory rfc;
    private int rowGroupSize;
    private boolean statisticsEnabled;
    private String tableName;
    private @Nullable StreamWriteParquetCallBack writeCallback;

    @Override
    public void clear() {
        this.entry = null;
        this.tableName = null;
        this.fileName = null;
        this.compressionCodec = -1;
        this.compressionLevel = -1;
        this.dataPageSize = -1;
        this.parquetVersion = -1;
        this.rowGroupSize = -1;
        this.statisticsEnabled = true;
        this.now = 0;
        this.nowTimestampType = 0;
        this.createOp = Misc.free(createOp);
        this.rfc = Misc.free(rfc);
        pageFrameCursor = null;
        writeCallback = null;
        metadata = null;
        streamPartitionParquetExporter.clear();
        descending = false;
        if (factory != null) { // owned only if setUpStreamPartitionParquetExporter called with factory
            factory = Misc.free(factory);
            pageFrameCursor = Misc.free(pageFrameCursor);
        }
    }

    @Override
    public void close() {
        Misc.free(streamPartitionParquetExporter);
    }

    public SqlExecutionCircuitBreaker getCircuitBreaker() {
        return entry.getCircuitBreaker();
    }

    public int getCompressionCodec() {
        return compressionCodec;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public long getCopyID() {
        return entry.getId();
    }

    public @Nullable CreateTableOperation getCreateOp() {
        return createOp;
    }

    public int getDataPageSize() {
        return dataPageSize;
    }

    public CopyExportContext.ExportTaskEntry getEntry() {
        return entry;
    }

    public CharSequence getFileName() {
        return fileName;
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public long getNow() {
        return now;
    }

    public int getNowTimestampType() {
        return nowTimestampType;
    }

    public @Nullable PageFrameCursor getPageFrameCursor() {
        return pageFrameCursor;
    }

    public int getParquetVersion() {
        return parquetVersion;
    }

    public int getRowGroupSize() {
        return rowGroupSize;
    }

    public SecurityContext getSecurityContext() {
        return entry.getSecurityContext();
    }

    public StreamPartitionParquetExporter getStreamPartitionParquetExporter() {
        return streamPartitionParquetExporter;
    }

    public String getTableName() {
        return tableName;
    }

    public @Nullable StreamWriteParquetCallBack getWriteCallback() {
        return writeCallback;
    }

    public boolean isDescending() {
        return descending;
    }

    public boolean isRawArrayEncoding() {
        return rawArrayEncoding;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void of(
            CopyExportContext.ExportTaskEntry entry,
            CreateTableOperation createOp,
            String tableName,
            CharSequence fileName,
            int compressionCodec,
            int compressionLevel,
            int rowGroupSize,
            int dataPageSize,
            boolean statisticsEnabled,
            int parquetVersion,
            boolean rawArrayEncoding,
            int nowTimestampType,
            long now,
            boolean descending,
            PageFrameCursor pageFrameCursor, // for streaming export
            RecordMetadata metadata,
            StreamWriteParquetCallBack writeCallback
    ) {
        this.entry = entry;
        this.tableName = tableName;
        this.fileName = fileName;
        this.compressionCodec = compressionCodec;
        this.compressionLevel = compressionLevel;
        this.rowGroupSize = rowGroupSize;
        this.dataPageSize = dataPageSize;
        this.statisticsEnabled = statisticsEnabled;
        this.parquetVersion = parquetVersion;
        this.rawArrayEncoding = rawArrayEncoding;
        this.createOp = createOp;
        this.descending = descending;
        this.pageFrameCursor = pageFrameCursor;
        this.metadata = metadata;
        this.writeCallback = writeCallback;
        this.now = now;
        this.nowTimestampType = nowTimestampType;
    }

    public void setUpStreamPartitionParquetExporter() {
        if (pageFrameCursor != null) {
            // Enable streaming mode to use MADV_DONTNEED on mmap, avoiding page cache exhaustion
            pageFrameCursor.setStreamingMode(true);
            streamPartitionParquetExporter.setUp();
        }
    }

    public void setUpStreamPartitionParquetExporter(RecordCursorFactory factory, PageFrameCursor pageFrameCursor, RecordMetadata metadata, boolean descending) {
        assert this.pageFrameCursor == null;
        this.factory = factory;
        this.pageFrameCursor = pageFrameCursor;
        this.metadata = metadata;
        this.descending = descending;
        // Enable streaming mode to use MADV_DONTNEED on mmap, avoiding page cache exhaustion
        pageFrameCursor.setStreamingMode(true);
        streamPartitionParquetExporter.setUp();
    }

    public enum Phase {
        NONE(""),
        WAITING("wait_to_run"),
        POPULATING_TEMP_TABLE("populating_data_to_temp_table"),
        CONVERTING_PARTITIONS("converting_partitions"),
        MOVE_FILES("move_files"),
        DROPPING_TEMP_TABLE("dropping_temp_table"),
        STREAM_SENDING_DATA("stream_sending_data"),
        SUCCESS("success");

        private final String name;

        Phase(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public enum Status {
        NONE(""),
        STARTED("started"),
        FINISHED("finished"),
        FAILED("failed"),
        CANCELLED("cancelled");

        private final String name;

        Status(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    @FunctionalInterface
    public interface StreamWriteParquetCallBack {
        void onWrite(long dataPtr, long dataLen) throws Exception;
    }

    public class StreamPartitionParquetExporter implements Mutable, QuietCloseable {
        // Buffer header size: [8 bytes data_len][8 bytes rows_written_to_row_groups]
        private static final int BUFFER_HEADER_SIZE = 2 * Long.BYTES;
        private DirectLongList columnData = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER, true);
        private DirectLongList columnMetadata = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER, true);
        private DirectUtf8Sink columnNames = new DirectUtf8Sink(32, false, MemoryTag.NATIVE_PARQUET_EXPORTER);
        private long currentFrameRowCount = 0;
        private long currentPartitionIndex = -1;
        private boolean exportFinished = false;
        // Cumulative count of rows written to Parquet row groups by Rust.
        // Used to determine when partition memory can be safely released.
        private long rowsWrittenToRowGroups = 0;
        private long streamExportCurrentPtr;
        private long streamExportCurrentSize;
        private long streamWriter = -1;
        private long totalRows;

        @Override
        public void clear() {
            // free memory after one query finished, will re-malloc on next query
            columnNames.close();
            columnData.close();
            columnMetadata.close();
            closeWriter();
            streamExportCurrentPtr = 0;
            streamExportCurrentSize = 0;
            rowsWrittenToRowGroups = 0;
            totalRows = 0;
            freeOwnedPageFrameCursor();
            exportFinished = false;
        }

        @Override
        public void close() {
            closeWriter();
            freeOwnedPageFrameCursor();
            columnNames = Misc.free(columnNames);
            columnData = Misc.free(columnData);
            columnMetadata = Misc.free(columnMetadata);
        }

        public void finishExport() throws Exception {
            if (exportFinished) {
                clear();
                return;
            }
            long buffer = finishStreamingParquetWrite(streamWriter);
            exportFinished = true;
            streamExportCurrentPtr = buffer + BUFFER_HEADER_SIZE;
            streamExportCurrentSize = Unsafe.getUnsafe().getLong(buffer);
            rowsWrittenToRowGroups = Unsafe.getUnsafe().getLong(buffer + Long.BYTES);
            assert writeCallback != null;
            writeCallback.onWrite(streamExportCurrentPtr, streamExportCurrentSize);
            clear();
            streamWriter = -1;
        }

        public void freeOwnedPageFrameCursor() {
            if (factory != null) {
                factory = Misc.free(factory);
                pageFrameCursor = Misc.free(pageFrameCursor);
            }
        }

        public long getCurrentFrameRowCount() {
            return currentFrameRowCount;
        }

        public long getCurrentPartitionIndex() {
            return currentPartitionIndex;
        }

        public long getRowsWrittenToRowGroups() {
            return rowsWrittenToRowGroups;
        }

        public long getTotalRows() {
            return totalRows;
        }

        public boolean onResume() throws Exception {
            if (streamExportCurrentPtr != 0) {
                assert writeCallback != null;
                while (streamExportCurrentPtr != 0) {
                    writeCallback.onWrite(streamExportCurrentPtr, streamExportCurrentSize);
                    long buffer = writeStreamingParquetChunk(streamWriter, 0, 0);
                    if (buffer != 0) {
                        streamExportCurrentPtr = buffer + BUFFER_HEADER_SIZE;
                        streamExportCurrentSize = Unsafe.getUnsafe().getLong(buffer);
                        rowsWrittenToRowGroups = Unsafe.getUnsafe().getLong(buffer + Long.BYTES);
                    } else {
                        streamExportCurrentPtr = 0;
                        streamExportCurrentSize = 0;
                    }
                }

                if (!exportFinished) {
                    totalRows += currentFrameRowCount;
                    entry.setStreamingSendRowCount(totalRows);
                }
                return true;
            }
            return false;
        }

        public void setCurrentPartitionIndex(long currentPartitionIndex, long frameRowCount) {
            this.currentPartitionIndex = currentPartitionIndex;
            this.currentFrameRowCount = frameRowCount;
        }

        public void setUp() {
            columnNames.reopen();
            columnMetadata.reopen();
            columnData.reopen();

            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                CharSequence columnName = metadata.getColumnName(i);
                final int startSize = columnNames.size();
                columnNames.put(columnName);
                columnMetadata.add(columnNames.size() - startSize);
                final int columnType = metadata.getColumnType(i);

                if (ColumnType.isSymbol(columnType)) {
                    assert pageFrameCursor != null;
                    StaticSymbolTable symbolTable = pageFrameCursor.getSymbolTable(i);
                    assert symbolTable != null;
                    int symbolColumnType = columnType;
                    if (!symbolTable.containsNullValue()) {
                        symbolColumnType |= 1 << 31;
                    }
                    columnMetadata.add((long) metadata.getWriterIndex(i) << 32 | symbolColumnType);
                } else {
                    columnMetadata.add((long) metadata.getWriterIndex(i) << 32 | columnType);
                }
            }
            streamWriter = createStreamingParquetWriter(
                    Unsafe.getNativeAllocator(MemoryTag.NATIVE_PARQUET_EXPORTER),
                    metadata.getColumnCount(),
                    columnNames.ptr(),
                    columnNames.size(),
                    columnMetadata.getAddress(),
                    metadata.getTimestampIndex(),
                    descending,
                    ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                    statisticsEnabled,
                    rawArrayEncoding,
                    rowGroupSize,
                    dataPageSize,
                    parquetVersion
            );
        }

        public void writePageFrame(PageFrameCursor frameCursor, PageFrame frame) throws Exception {
            assert streamWriter != -1 && writeCallback != null;
            if (frame.getFormat() == PartitionFormat.NATIVE) {
                columnData.clear();
                final long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();

                for (int i = 0, n = frame.getColumnCount(); i < n; i++) {
                    long localColTop = frame.getPageAddress(i) > 0 ? 0 : frameRowCount;
                    final int columnType = metadata.getColumnType(i);
                    final long pageAddress = frame.getPageAddress(i);

                    // Assert alignment for SIMD operations in Rust parquet encoder
                    assert pageAddress == 0 || isAlignedForColumnType(pageAddress, columnType)
                            : "Unaligned address " + pageAddress + " for column type " + ColumnType.nameOf(columnType);

                    if (ColumnType.isSymbol(columnType)) {
                        SymbolMapReader symbolMapReader = (SymbolMapReader) frameCursor.getSymbolTable(i);
                        final MemoryR symbolValuesMem = symbolMapReader.getSymbolValuesColumn();
                        final MemoryR symbolOffsetsMem = symbolMapReader.getSymbolOffsetsColumn();

                        columnData.add(localColTop);
                        columnData.add(pageAddress);
                        columnData.add(frame.getPageSize(i));
                        columnData.add(symbolValuesMem.addressOf(0));
                        columnData.add(symbolValuesMem.size());
                        columnData.add(symbolOffsetsMem.addressOf(HEADER_SIZE));
                        columnData.add(symbolMapReader.getSymbolCount());
                    } else {
                        columnData.add(localColTop);
                        columnData.add(pageAddress);
                        columnData.add(frame.getPageSize(i));
                        columnData.add(frame.getAuxPageAddress(i));
                        columnData.add(frame.getAuxPageSize(i));
                        columnData.add(0L);
                        columnData.add(0L);
                    }
                }

                long buffer = writeStreamingParquetChunk(streamWriter, columnData.getAddress(), frameRowCount);
                while (buffer != 0) {
                    streamExportCurrentPtr = buffer + BUFFER_HEADER_SIZE;
                    streamExportCurrentSize = Unsafe.getUnsafe().getLong(buffer);
                    rowsWrittenToRowGroups = Unsafe.getUnsafe().getLong(buffer + Long.BYTES);
                    writeCallback.onWrite(streamExportCurrentPtr, streamExportCurrentSize);
                    buffer = writeStreamingParquetChunk(
                            streamWriter,
                            0,
                            0
                    );
                }
            } else {
                columnData.clear();

                for (int i = 0, n = frame.getColumnCount(); i < n; i++) {
                    final int columnType = metadata.getColumnType(i);
                    if (ColumnType.isSymbol(columnType)) {
                        SymbolMapReader symbolMapReader = (SymbolMapReader) frameCursor.getSymbolTable(i);
                        final MemoryR symbolValuesMem = symbolMapReader.getSymbolValuesColumn();
                        final MemoryR symbolOffsetsMem = symbolMapReader.getSymbolOffsetsColumn();
                        columnData.add(symbolValuesMem.addressOf(0));
                        columnData.add(symbolValuesMem.size());
                        columnData.add(symbolOffsetsMem.addressOf(HEADER_SIZE));
                        columnData.add(symbolMapReader.getSymbolCount());
                    }
                }

                long allocator = Unsafe.getNativeAllocator(MemoryTag.NATIVE_PARQUET_EXPORTER);
                long buffer = writeStreamingParquetChunkFromRowGroup(
                        streamWriter,
                        allocator,
                        columnData.getAddress(),
                        frame.getParquetPartitionDecoder().getFileAddr(),
                        frame.getParquetPartitionDecoder().getFileSize(),
                        frame.getParquetRowGroup(),
                        frame.getParquetRowGroupLo(),
                        frame.getParquetRowGroupHi()
                );
                while (buffer != 0) {
                    streamExportCurrentPtr = buffer + BUFFER_HEADER_SIZE;
                    streamExportCurrentSize = Unsafe.getUnsafe().getLong(buffer);
                    rowsWrittenToRowGroups = Unsafe.getUnsafe().getLong(buffer + Long.BYTES);
                    writeCallback.onWrite(streamExportCurrentPtr, streamExportCurrentSize);
                    buffer = writeStreamingParquetChunkFromRowGroup(
                            streamWriter,
                            allocator,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0
                    );
                }
            }
            totalRows += currentFrameRowCount;
            entry.setStreamingSendRowCount(totalRows);
            streamExportCurrentPtr = 0;
            streamExportCurrentSize = 0;
        }

        private void closeWriter() {
            if (streamWriter != -1) {
                closeStreamingParquetWriter(streamWriter);
                streamWriter = -1;
            }
        }

        /**
         * Checks if the given address is properly aligned for SIMD operations.
         * Only types that use SIMD in Rust parquet encoder require alignment:
         * - LONG, DOUBLE, TIMESTAMP: 8-byte alignment (Simd<i64, 8>, Simd<f64, 8>)
         * - INT, FLOAT, SYMBOL: 4-byte alignment (Simd<i32, 16>, Simd<f32, 16>)
         * Other types (SHORT, BYTE, etc.) use scalar paths and don't require SIMD alignment.
         */
        private static boolean isAlignedForColumnType(long address, int columnType) {
            int alignment = getRequiredAlignmentForSimd(columnType);
            return alignment <= 1 || (address & (alignment - 1)) == 0;
        }

        private static int getRequiredAlignmentForSimd(int columnType) {
            switch (ColumnType.tagOf(columnType)) {
                // Types using Simd<i64, 8> or Simd<f64, 8>
                case ColumnType.LONG:
                case ColumnType.DOUBLE:
                case ColumnType.TIMESTAMP:
                case ColumnType.DATE:
                    return 8;
                // Types using Simd<i32, 16> or Simd<f32, 16>
                case ColumnType.INT:
                case ColumnType.FLOAT:
                case ColumnType.SYMBOL:
                    return 4;
                // All other types use scalar paths - no SIMD alignment required
                default:
                    return 1;
            }
        }
    }
}

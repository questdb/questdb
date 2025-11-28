/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.SymbolMapWriter.HEADER_SIZE;
import static io.questdb.griffin.engine.table.parquet.PartitionEncoder.*;

public class CopyExportRequestTask implements Mutable {
    private final StreamPartitionParquetExporter streamPartitionParquetExporter = new StreamPartitionParquetExporter();
    private int compressionCodec;
    private int compressionLevel;
    private @Nullable CreateTableOperation createOp;
    private int dataPageSize;
    private boolean descending;
    private CopyExportContext.ExportTaskEntry entry;
    private RecordCursorFactory factory;
    private CharSequence fileName;
    private @Nullable RecordMetadata metadata;
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

    public @Nullable RecordMetadata getMetadata() {
        return metadata;
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
    }

    public void setUpStreamPartitionParquetExporter() {
        if (pageFrameCursor != null) {
            streamPartitionParquetExporter.setUp();
        }
    }

    public void setUpStreamPartitionParquetExporter(RecordCursorFactory factory, PageFrameCursor pageFrameCursor, RecordMetadata metadata) {
        assert this.pageFrameCursor == null;
        this.factory = factory;
        this.pageFrameCursor = pageFrameCursor;
        this.metadata = metadata;
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

    public class StreamPartitionParquetExporter implements Mutable {
        private final DirectLongList columnData = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER, false);
        private final DirectLongList columnMetadata = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER, false);
        private final DirectUtf8Sink columnNames = new DirectUtf8Sink(32, false);
        private long currentFrameRowCount = 0;
        private long currentPartitionIndex = -1;
        private long streamExportCurrentPtr;
        private long streamExportCurrentSize;
        private long streamWriter = -1;
        private long totalRows;

        @Override
        public void clear() {
            columnNames.close();
            columnData.close();
            columnMetadata.close();
            if (streamWriter != -1) {
                closeStreamingParquetWriter(streamWriter);
                streamWriter = -1;
            }
            streamExportCurrentPtr = 0;
            streamExportCurrentSize = 0;
            totalRows = 0;
            if (factory != null) {
                factory = Misc.free(factory);
                pageFrameCursor = Misc.free(pageFrameCursor);
            }
        }

        public void finishExport() throws Exception {
            if (streamWriter == -1) {
                if (factory != null) {
                    factory = Misc.free(factory);
                    pageFrameCursor = Misc.free(pageFrameCursor);
                }
                return;
            }
            long buffer = finishStreamingParquetWrite(streamWriter);
            long dataPtr = buffer + Long.BYTES;
            long dataSize = Unsafe.getUnsafe().getLong(buffer);
            assert writeCallback != null;
            closeStreamingParquetWriter(streamWriter);
            streamWriter = -1;
            writeCallback.onWrite(dataPtr, dataSize);
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

        public long getTotalRows() {
            return totalRows;
        }

        public boolean onResume() throws Exception {
            if (streamExportCurrentPtr != 0) {
                assert writeCallback != null;
                writeCallback.onWrite(streamExportCurrentPtr, streamExportCurrentSize);
                totalRows += currentFrameRowCount;
                streamExportCurrentPtr = 0;
                streamExportCurrentSize = 0;
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
                int startSize = columnNames.size();
                columnNames.put(columnName);
                columnMetadata.add(columnNames.size() - startSize);
                columnMetadata.add((long) metadata.getWriterIndex(i) << 32 | metadata.getColumnType(i));
            }
            streamWriter = createStreamingParquetWriter(
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
                    long localColTop = frame.hasColumnData(i) ? 0 : frameRowCount;
                    assert metadata != null;
                    final int columnType = metadata.getColumnType(i);
                    if (ColumnType.isSymbol(columnType)) {
                        SymbolMapReader symbolMapReader = (SymbolMapReader) frameCursor.getSymbolTable(i);
                        final MemoryR symbolValuesMem = symbolMapReader.getSymbolValuesColumn();
                        final MemoryR symbolOffsetsMem = symbolMapReader.getSymbolOffsetsColumn();

                        columnData.add(localColTop);
                        columnData.add(frame.getPageAddress(i));
                        columnData.add(frame.getPageSize(i));
                        columnData.add(symbolValuesMem.addressOf(0));
                        columnData.add(symbolValuesMem.size());
                        columnData.add(symbolOffsetsMem.addressOf(HEADER_SIZE));
                        columnData.add(symbolMapReader.getSymbolCount());
                    } else {
                        columnData.add(localColTop);
                        columnData.add(frame.getPageAddress(i));
                        columnData.add(frame.getPageSize(i));
                        columnData.add(frame.getAuxPageAddress(i));
                        columnData.add(frame.getAuxPageSize(i));
                        columnData.add(0L);
                        columnData.add(0L);
                    }
                }

                // Write chunk to Parquet
                long buffer = writeStreamingParquetChunk(
                        streamWriter,
                        columnData.getAddress(),
                        frameRowCount
                );
                streamExportCurrentPtr = buffer + Long.BYTES;
                streamExportCurrentSize = Unsafe.getUnsafe().getLong(buffer);
                writeCallback.onWrite(streamExportCurrentPtr, streamExportCurrentSize);
            } else {
                columnData.clear();

                for (int i = 0, n = frame.getColumnCount(); i < n; i++) {
                    assert metadata != null;
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
                        frame.getParquetAddr(),
                        frame.getParquetFileSize(),
                        frame.getParquetRowGroup(),
                        frame.getParquetRowGroupLo(),
                        frame.getParquetRowGroupHi()
                );
                streamExportCurrentPtr = buffer + Long.BYTES;
                streamExportCurrentSize = Unsafe.getUnsafe().getLong(buffer);
                writeCallback.onWrite(streamExportCurrentPtr, streamExportCurrentSize);
            }
            totalRows += currentFrameRowCount;
            streamExportCurrentPtr = 0;
            streamExportCurrentSize = 0;
        }
    }
}

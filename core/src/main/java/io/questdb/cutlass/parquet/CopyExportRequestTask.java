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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.cutlass.text.CopyExportResult;
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
    private CopyExportContext.ExportTaskEntry entry;
    private CharSequence fileName;
    private @Nullable RecordMetadata metadata;
    private @Nullable PageFrameCursor pageFrameCursor;
    private int parquetVersion;
    private boolean rawArrayEncoding;
    private @Nullable CopyExportResult result;
    private int rowGroupSize;
    private boolean statisticsEnabled;
    private String tableName;
    private @Nullable WriteParquetCallBack writeCallback;

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
        result = null;
        pageFrameCursor = null;
        writeCallback = null;
        metadata = null;
        streamPartitionParquetExporter.clear();
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

    public CreateTableOperation getCreateOp() {
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

    public PageFrameCursor getPageFrameCursor() {
        return pageFrameCursor;
    }

    public int getParquetVersion() {
        return parquetVersion;
    }

    public CopyExportResult getResult() {
        return result;
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

    public WriteParquetCallBack getWriteCallback() {
        return writeCallback;
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
            CopyExportResult result,
            String tableName,
            CharSequence fileName,
            int compressionCodec,
            int compressionLevel,
            int rowGroupSize,
            int dataPageSize,
            boolean statisticsEnabled,
            int parquetVersion,
            boolean rawArrayEncoding,
            PageFrameCursor pageFrameCursor, // for streaming export
            RecordMetadata metadata,
            WriteParquetCallBack writeCallback

    ) {
        this.entry = entry;
        this.result = result;
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
        this.pageFrameCursor = pageFrameCursor;
        this.metadata = metadata;
        this.writeCallback = writeCallback;
    }

    public void setUpStreamPartitionParquetExporter() {
        assert pageFrameCursor != null;
        streamPartitionParquetExporter.setUp();
    }

    public void setUpStreamPartitionParquetExporter(PageFrameCursor pageFrameCursor, RecordMetadata metadata) {
        assert this.pageFrameCursor == null && this.metadata == null;
        streamPartitionParquetExporter.setUp();
    }

    public enum Phase {
        NONE(""),
        WAITING("wait_to_run"),
        POPULATING_TEMP_TABLE("populating_data_to_temp_table"),
        CONVERTING_PARTITIONS("converting_partitions"),
        MOVE_FILES("move_files"),
        DROPPING_TEMP_TABLE("dropping_temp_table"),
        SENDING_DATA("sending_data"),
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
    public interface WriteParquetCallBack {
        void onWrite(long dataPtr, long dataLen, byte format) throws Exception;
    }

    public class StreamPartitionParquetExporter implements Mutable {
        private final DirectLongList columnData = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER, false);
        private final DirectLongList columnMetadata = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER, false);
        private final DirectUtf8Sink columnNames = new DirectUtf8Sink(32, false);
        private long lastPartitionIndex = -1;
        private long streamExportCurrentPtr;
        private long streamExportCurrentSize;
        private byte streamExportFormat;
        private long streamWriter = -1;

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
            lastPartitionIndex = -1;
        }

        public void finishExport() throws Exception {
            if (streamWriter == -1) {
                return;
            }
            long buffer = finishStreamingParquetWrite(streamWriter);
            long dataPtr = Unsafe.getUnsafe().getLong(buffer);
            long dataSize = Unsafe.getUnsafe().getLong(buffer + Long.BYTES);
            assert writeCallback != null;
            writeCallback.onWrite(dataPtr, dataSize, PartitionFormat.NATIVE);
            closeStreamingParquetWriter(streamWriter);
            streamWriter = -1;
        }

        public void onSuspend() throws Exception {
            if (streamExportCurrentPtr != 0) {
                assert writeCallback != null;
                writeCallback.onWrite(streamExportCurrentPtr, streamExportCurrentSize, streamExportFormat);
                streamExportCurrentPtr = 0;
                streamExportCurrentSize = 0;
            }
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
        }

        public void writePageFrame(PageFrameCursor frameCursor, PageFrame frame) throws Exception {
            int currentPartitionIndex = frame.getPartitionIndex();
            if (currentPartitionIndex != lastPartitionIndex) {
                if (lastPartitionIndex != -1 && streamExportFormat == PartitionFormat.NATIVE) {
                    finishExport();
                }
                if (frame.getFormat() == PartitionFormat.NATIVE) {
                    assert metadata != null;
                    streamWriter = createStreamingParquetWriter(
                            metadata.getColumnCount(),
                            columnNames.ptr(),
                            columnNames.size(),
                            columnMetadata.getAddress(),
                            metadata.getTimestampIndex(),
                            ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                            statisticsEnabled,
                            rawArrayEncoding,
                            rowGroupSize,
                            dataPageSize,
                            parquetVersion
                    );
                }
            }

            if (frame.getFormat() == PartitionFormat.NATIVE) {
                assert writeCallback != null;
                columnData.clear();
                final long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();

                for (int i = 0, n = frame.getColumnCount(); i < n; i++) {
                    long localColTop = frame.getColumnTop(i);
                    if (localColTop > frameRowCount) {
                        localColTop = frameRowCount;
                    }

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
                streamExportCurrentPtr = Unsafe.getUnsafe().getLong(buffer);
                streamExportCurrentSize = Unsafe.getUnsafe().getLong(buffer + Long.BYTES);
                streamExportFormat = PartitionFormat.NATIVE;
                writeCallback.onWrite(streamExportCurrentPtr, streamExportCurrentSize, PartitionFormat.NATIVE);
            } else {
                streamExportCurrentPtr = frame.getParquetAddr();
                streamExportCurrentSize = frame.getParquetFileSize();
                assert writeCallback != null;
                streamExportFormat = PartitionFormat.PARQUET;
                writeCallback.onWrite(frame.getParquetAddr(), frame.getParquetFileSize(), PartitionFormat.PARQUET);
            }
            streamExportCurrentPtr = 0;
            streamExportCurrentSize = 0;
        }
    }
}

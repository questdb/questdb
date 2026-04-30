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

package io.questdb.cutlass.parquet;


import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
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
    private @Nullable BindVariableService bindVariableService;
    private @Nullable CharSequence bloomFilterColumns;
    private int bloomFilterColumnsPosition = -1;
    private double bloomFilterFpp = Double.NaN;
    private int compressionCodec;
    private int compressionLevel;
    private @Nullable CreateTableOperation createOp;
    private int dataPageSize;
    private boolean descending;
    private CopyExportContext.ExportTaskEntry entry;
    private ParquetExportMode exportMode;
    private CharSequence fileName;
    private RecordMetadata metadata;
    private long now;
    private int nowTimestampType;
    private @Nullable PageFrameCursor pageFrameCursor;
    private int parquetVersion;
    private boolean rawArrayEncoding;
    private int rowGroupSize;
    private RecordCursorFactory selectFactory;
    private @Nullable String selectText;
    private boolean statisticsEnabled;
    private String tableName;
    private RecordCursorFactory tempTableFactory;
    private @Nullable StreamWriteParquetCallBack writeCallback;

    public static void validateBloomFilterColumns(@Nullable CharSequence columns, RecordMetadata meta, int position) throws SqlException {
        if (columns == null || columns.isEmpty()) {
            return;
        }
        int start = 0;
        int len = columns.length();
        for (int i = 0; i <= len; i++) {
            if (i == len || columns.charAt(i) == ',') {
                int nameStart = start;
                int nameEnd = i;
                while (nameStart < nameEnd && Character.isWhitespace(columns.charAt(nameStart))) {
                    nameStart++;
                }
                while (nameEnd > nameStart && Character.isWhitespace(columns.charAt(nameEnd - 1))) {
                    nameEnd--;
                }
                if (nameEnd <= nameStart) {
                    throw SqlException.$(position > 0 ? position + start : 0, "empty column name in bloom_filter_columns");
                }
                CharSequence columnName = columns.subSequence(nameStart, nameEnd);
                if (meta.getColumnIndexQuiet(columnName) < 0) {
                    throw SqlException.$(position > 0 ? position + start : 0,
                            "bloom_filter_columns contains non-existent column: ").put(columnName);
                }
                start = i + 1;
            }
        }
    }

    @Override
    public void clear() {
        this.bindVariableService = null;
        this.selectFactory = Misc.free(selectFactory);
        this.entry = null;
        this.exportMode = null;
        this.selectText = null;
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
        if (tempTableFactory != null) {
            // Temp-table path owns both the factory and the cursor.
            tempTableFactory = Misc.free(tempTableFactory);
            pageFrameCursor = Misc.free(pageFrameCursor);
        } else {
            // Ownership belongs to BaseParquetExporter subclass (streamingPfc)
            // or ExportQueryProcessorState (for DIRECT_PAGE_FRAME).
            pageFrameCursor = null;
        }
        writeCallback = null;
        metadata = null;
        streamPartitionParquetExporter.clear();
        descending = false;
        bloomFilterColumns = null;
        bloomFilterColumnsPosition = -1;
        bloomFilterFpp = Double.NaN;
    }

    @Override
    public void close() {
        selectFactory = Misc.free(selectFactory);
        createOp = Misc.free(createOp);
        if (tempTableFactory != null) {
            tempTableFactory = Misc.free(tempTableFactory);
            pageFrameCursor = Misc.free(pageFrameCursor);
        }
        Misc.free(streamPartitionParquetExporter);
    }

    public @Nullable BindVariableService getBindVariableService() {
        return bindVariableService;
    }

    public @Nullable CharSequence getBloomFilterColumns() {
        return bloomFilterColumns;
    }

    public int getBloomFilterColumnsPosition() {
        return bloomFilterColumnsPosition;
    }

    public double getBloomFilterFpp() {
        return bloomFilterFpp;
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

    public ParquetExportMode getExportMode() {
        return exportMode;
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

    public RecordCursorFactory getSelectFactory() {
        return selectFactory;
    }

    public @Nullable String getSelectText() {
        return selectText;
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
            StreamWriteParquetCallBack writeCallback,
            ParquetExportMode exportMode,
            @Nullable String selectText,
            @Nullable CharSequence bloomFilterColumns,
            int bloomFilterColumnsPosition,
            double bloomFilterFpp,
            @Nullable BindVariableService bindVariableService
    ) {
        this.bindVariableService = bindVariableService;
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
        this.exportMode = exportMode;
        this.selectText = selectText;
        this.bloomFilterColumns = bloomFilterColumns;
        this.bloomFilterColumnsPosition = bloomFilterColumnsPosition;
        this.bloomFilterFpp = bloomFilterFpp;
    }

    public void setCreateOp(@Nullable CreateTableOperation createOp) {
        this.createOp = createOp;
    }

    public void setSelectFactory(RecordCursorFactory selectFactory) {
        this.selectFactory = selectFactory;
    }

    public void setUpStreamPartitionParquetExporter() {
        if (pageFrameCursor != null) {
            // Enable streaming mode to use MADV_DONTNEED on mmap, avoiding page cache exhaustion
            pageFrameCursor.setStreamingMode(true);
            streamPartitionParquetExporter.setUp();
        }
    }

    public void setUpStreamPartitionParquetExporter(RecordCursorFactory tempTableFactory, PageFrameCursor pageFrameCursor, RecordMetadata metadata, boolean descending) {
        assert this.pageFrameCursor == null;
        this.tempTableFactory = tempTableFactory;
        this.pageFrameCursor = pageFrameCursor;
        this.metadata = metadata;
        this.descending = descending;
        // Enable streaming mode to use MADV_DONTNEED on mmap, avoiding page cache exhaustion
        pageFrameCursor.setStreamingMode(true);
        streamPartitionParquetExporter.setUp();
    }

    public void setWriteCallback(@Nullable StreamWriteParquetCallBack writeCallback) {
        this.writeCallback = writeCallback;
    }

    protected static void parseBloomFilterColumnIndexes(CharSequence columns, RecordMetadata meta, DirectIntList indexes, int bloomFilterColumnsPosition) {
        int start = 0;
        int len = columns.length();
        for (int i = 0; i <= len; i++) {
            if (i == len || columns.charAt(i) == ',') {
                int nameStart = start;
                int nameEnd = i;
                while (nameStart < nameEnd && Character.isWhitespace(columns.charAt(nameStart))) {
                    nameStart++;
                }
                while (nameEnd > nameStart && Character.isWhitespace(columns.charAt(nameEnd - 1))) {
                    nameEnd--;
                }
                if (nameStart >= nameEnd) {
                    throw CairoException.nonCritical().put("empty column name in bloom_filter_columns")
                            .position(bloomFilterColumnsPosition > 0 ? bloomFilterColumnsPosition + start : 0);
                }
                CharSequence columnName = columns.subSequence(nameStart, nameEnd);
                int columnIndex = meta.getColumnIndexQuiet(columnName);
                if (columnIndex >= 0) {
                    indexes.add(columnIndex);
                } else {
                    throw CairoException.nonCritical().put("bloom_filter_columns contains non-existent column: ").put(columnName)
                            .position(bloomFilterColumnsPosition > 0 ? bloomFilterColumnsPosition + start : 0);
                }
                start = i + 1;
            }
        }
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
        private DirectIntList bloomFilterColumnIndexes = new DirectIntList(16, MemoryTag.NATIVE_PARQUET_EXPORTER, true);
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
            Misc.free(columnNames);
            Misc.free(columnData);
            Misc.free(columnMetadata);
            Misc.free(bloomFilterColumnIndexes);
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
            bloomFilterColumnIndexes = Misc.free(bloomFilterColumnIndexes);
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
            if (tempTableFactory != null) {
                tempTableFactory = Misc.free(tempTableFactory);
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
            setUp(metadata, pageFrameCursor, null);
        }

        public void setUp(RecordMetadata adjustedMetadata) {
            setUp(adjustedMetadata, null, null);
        }

        /**
         * Unified writer setup for all export modes.
         *
         * @param meta          column metadata to encode
         * @param pfc           page frame cursor for SYMBOL table lookup (null when cursor-based)
         * @param baseColumnMap per-output-column base index for hybrid path, or null for
         *                      direct/temp-table path (where every column is a real table column)
         */
        public void setUp(RecordMetadata meta, PageFrameCursor pfc, IntList baseColumnMap) {
            metadata = meta;
            pageFrameCursor = pfc;
            columnNames.reopen();
            columnMetadata.reopen();
            columnData.reopen();

            for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
                CharSequence columnName = meta.getColumnName(i);
                final int startSize = columnNames.size();
                columnNames.put(columnName);
                columnMetadata.add(columnNames.size() - startSize);
                final int columnType = meta.getColumnType(i);
                // table metadata returns the physical writer column index.
                int writerIdx = meta.getWriterIndex(i);
                if (writerIdx < 0) {
                    // GenericRecordMetadata (hybrid/cursor paths) returns -1, use i instead
                    writerIdx = i;
                }

                // A SYMBOL column needs symbol-table metadata when it is a
                // real table column.  In the hybrid path (baseColumnMap != null)
                // only pass-through columns (baseColumnMap >= 0) qualify;
                // in the direct path (baseColumnMap == null) every column is real.
                boolean isPassThroughSymbol = ColumnType.isSymbol(columnType)
                        && (baseColumnMap == null || baseColumnMap.getQuick(i) >= 0);

                if (isPassThroughSymbol) {
                    assert pfc != null;
                    int symbolTableIdx = baseColumnMap != null ? baseColumnMap.getQuick(i) : i;
                    StaticSymbolTable symbolTable = pfc.getSymbolTable(symbolTableIdx);
                    assert symbolTable != null;
                    int symbolColumnType = columnType;
                    if (!symbolTable.containsNullValue()) {
                        symbolColumnType |= 1 << 31;
                    }
                    // Mask to 32 bits: symbolColumnType can have bit 31 set (no-null flag),
                    // making it negative. Without the mask, Java sign-extends it to 64 bits
                    // before the OR, clobbering writerIdx in the upper 32 bits.
                    columnMetadata.add((long) writerIdx << 32 | (symbolColumnType & 0xFFFFFFFFL));
                } else {
                    columnMetadata.add((long) writerIdx << 32 | (columnType & 0xFFFFFFFFL));
                }
            }

            long bloomFilterIndexesPtr = 0;
            int bloomFilterCount = 0;
            double fpp = Double.isNaN(bloomFilterFpp) ? DEFAULT_BLOOM_FILTER_FPP : bloomFilterFpp;

            if (bloomFilterColumns != null && !bloomFilterColumns.isEmpty()) {
                bloomFilterColumnIndexes.reopen();
                parseBloomFilterColumnIndexes(bloomFilterColumns, metadata, bloomFilterColumnIndexes, bloomFilterColumnsPosition);
                if (bloomFilterColumnIndexes.size() > 0) {
                    bloomFilterIndexesPtr = bloomFilterColumnIndexes.getAddress();
                    bloomFilterCount = (int) bloomFilterColumnIndexes.size();
                }
            }

            streamWriter = createStreamingParquetWriter(
                    Unsafe.getNativeAllocator(MemoryTag.NATIVE_PARQUET_EXPORTER),
                    meta.getColumnCount(),
                    columnNames.ptr(),
                    columnNames.size(),
                    columnMetadata.getAddress(),
                    meta.getTimestampIndex(),
                    descending,
                    ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                    statisticsEnabled,
                    rawArrayEncoding,
                    rowGroupSize,
                    dataPageSize,
                    parquetVersion,
                    bloomFilterIndexesPtr,
                    bloomFilterCount,
                    fpp,
                    0.0
            );
        }

        public void writeHybridFrame(DirectLongList prebuiltColumnData, long frameRowCount) throws Exception {
            assert streamWriter != -1 && writeCallback != null;
            currentFrameRowCount = frameRowCount;
            long buffer = writeStreamingParquetChunk(streamWriter, prebuiltColumnData.getAddress(), frameRowCount);
            while (buffer != 0) {
                streamExportCurrentPtr = buffer + BUFFER_HEADER_SIZE;
                streamExportCurrentSize = Unsafe.getUnsafe().getLong(buffer);
                rowsWrittenToRowGroups = Unsafe.getUnsafe().getLong(buffer + Long.BYTES);
                writeCallback.onWrite(streamExportCurrentPtr, streamExportCurrentSize);
                buffer = writeStreamingParquetChunk(streamWriter, 0, 0);
            }
            totalRows += frameRowCount;
            entry.setStreamingSendRowCount(totalRows);
            streamExportCurrentPtr = 0;
            streamExportCurrentSize = 0;
        }

        public void writePageFrame(PageFrameCursor frameCursor, PageFrame frame) throws Exception {
            assert streamWriter != -1 && writeCallback != null;
            if (frame.getFormat() == PartitionFormat.NATIVE) {
                columnData.clear();
                final long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();

                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    final int columnType = metadata.getColumnType(i);
                    final long pageAddress = frame.getPageAddress(i);
                    // Var-size columns may have an empty .d file when all values are inlined
                    // into the aux vector (see FwdTableReaderPageFrameCursor for the producer
                    // contract); use the aux address as the column-top detector to avoid
                    // materialising live rows as NULL.
                    final long localColTop;
                    if (ColumnType.isVarSize(columnType)) {
                        localColTop = frame.getAuxPageAddress(i) > 0 ? 0 : frameRowCount;
                    } else {
                        localColTop = pageAddress > 0 ? 0 : frameRowCount;
                    }

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

                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
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

        private static int getRequiredAlignmentForSimd(int columnType) {
            return switch (ColumnType.tagOf(columnType)) {
                // Types using Simd<i64, 8> or Simd<f64, 8>
                case ColumnType.LONG, ColumnType.DOUBLE, ColumnType.TIMESTAMP, ColumnType.DATE -> 8;
                // Types using Simd<i32, 16> or Simd<f32, 16>
                case ColumnType.INT, ColumnType.FLOAT, ColumnType.SYMBOL -> 4;
                // All other types use scalar paths - no SIMD alignment required
                default -> 1;
            };
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

        private void closeWriter() {
            if (streamWriter != -1) {
                closeStreamingParquetWriter(streamWriter);
                streamWriter = -1;
            }
        }
    }
}

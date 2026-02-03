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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

import static io.questdb.cairo.SymbolMapWriter.HEADER_SIZE;

public class PartitionEncoder {

    public static native void closeStreamingParquetWriter(
            long writerPtr
    ) throws CairoException;

    public static native long createStreamingParquetWriter(
            long allocator,
            int columnCount,
            long columnNamesPtr,
            int columnNamesSize,
            long columnMetadataPtr,
            int timestampIndex,
            boolean descending,
            long compressionCodec,
            boolean statisticsEnabled,
            boolean rawArrayEncoding,
            long rowGroupSize,
            long dataPageSize,
            int version,
            long bloomFilterColumnIndexesPtr,
            int bloomFilterColumnCount,
            double bloomFilterFpp
    ) throws CairoException;

    public static void encode(PartitionDescriptor descriptor, Path destPath) {
        encodeWithOptions(
                descriptor,
                destPath,
                ParquetCompression.COMPRESSION_UNCOMPRESSED,
                true,
                false,
                0, // DEFAULT_ROW_GROUP_SIZE (512 * 512) rows
                0, // DEFAULT_DATA_PAGE_SIZE (1024 * 1024) bytes
                ParquetVersion.PARQUET_VERSION_V1
        );
    }

    public static void encodeWithOptions(
            PartitionDescriptor descriptor,
            Path destPath,
            long compressionCodec,
            boolean statisticsEnabled,
            boolean rawArrayEncoding,
            long rowGroupSize,
            long dataPageSize,
            int version
    ) {
        encodeWithOptions(
                descriptor,
                destPath,
                compressionCodec,
                statisticsEnabled,
                rawArrayEncoding,
                rowGroupSize,
                dataPageSize,
                version,
                0,
                0,
                0.01
        );
    }

    public static void encodeWithOptions(
            PartitionDescriptor descriptor,
            Path destPath,
            long compressionCodec,
            boolean statisticsEnabled,
            boolean rawArrayEncoding,
            long rowGroupSize,
            long dataPageSize,
            int version,
            long bloomFilterColumnIndexesPtr,
            int bloomFilterColumnCount,
            double bloomFilterFpp
    ) {
        final Utf8Sequence tableName = descriptor.getTableName();
        final int columnCount = descriptor.getColumnCount();
        final long partitionSize = descriptor.getPartitionRowCount();
        final int timestampIndex = descriptor.getTimestampIndex();
        try {
            encodePartition(  // throws CairoException on error
                    tableName.ptr(),
                    tableName.size(),
                    columnCount,
                    descriptor.getColumnNamesPtr(),
                    descriptor.getColumnNamesLen(),
                    descriptor.getColumnDataPtr(),
                    descriptor.getColumnDataLen(),
                    timestampIndex,
                    partitionSize,
                    destPath.ptr(),
                    destPath.size(),
                    compressionCodec,
                    statisticsEnabled,
                    rawArrayEncoding,
                    rowGroupSize,
                    dataPageSize,
                    version,
                    bloomFilterColumnIndexesPtr,
                    bloomFilterColumnCount,
                    bloomFilterFpp
            );
        } finally {
            descriptor.clear();
        }
    }

    public static native long finishStreamingParquetWrite(long writerPtr) throws CairoException;

    public static void populateEmptyPartition(TableReader tableReader, PartitionDescriptor descriptor) throws CairoException {
        final int timestampIndex = tableReader.getMetadata().getTimestampIndex();
        descriptor.of(tableReader.getTableToken().getTableName(), 0, timestampIndex);
        final TableReaderMetadata metadata = tableReader.getMetadata();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int columnType = metadata.getColumnType(i);
            if (columnType > 0) {
                descriptor.addColumn(
                        metadata.getColumnName(i),
                        columnType,
                        metadata.getColumnMetadata(i).getWriterIndex(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0
                );
            }
        }
    }

    public static void populateFromTableReader(TableReader tableReader, PartitionDescriptor descriptor, int partitionIndex) throws CairoException {
        final long partitionSize = tableReader.openPartition(partitionIndex);
        assert partitionSize != 0;
        final int timestampIndex = tableReader.getMetadata().getTimestampIndex();
        descriptor.of(tableReader.getTableToken().getTableName(), partitionSize, timestampIndex);

        final TableReaderMetadata metadata = tableReader.getMetadata();
        final int columnCount = metadata.getColumnCount();
        final int columnBase = tableReader.getColumnBase(partitionIndex);
        for (int i = 0; i < columnCount; i++) {
            final String columnName = metadata.getColumnName(i);
            final int columnType = metadata.getColumnType(i);
            if (columnType > 0) {
                final int columnId = metadata.getColumnMetadata(i).getWriterIndex();
                final long colTop = Math.min(tableReader.getColumnTop(columnBase, i), partitionSize);

                final int primaryIndex = TableReader.getPrimaryColumnIndex(columnBase, i);
                final MemoryR primaryMem = tableReader.getColumn(primaryIndex);

                if (primaryMem == null) {
                    throw CairoException.critical(CairoException.ERRNO_FILE_DOES_NOT_EXIST)
                            .put("could not map primary mem for column [table=").put(metadata.getTableToken())
                            .put(", primaryIndex=").put(primaryIndex)
                            .put(']');
                }

                if (ColumnType.isSymbol(columnType)) {
                    SymbolMapReader symbolMapReader = tableReader.getSymbolMapReader(i);
                    final MemoryR symbolValuesMem = symbolMapReader.getSymbolValuesColumn();
                    final MemoryR symbolOffsetsMem = symbolMapReader.getSymbolOffsetsColumn();
                    int encodeColumnType = columnType;
                    if (!symbolMapReader.containsNullValue()) {
                        encodeColumnType |= Integer.MIN_VALUE;
                    }
                    descriptor.addColumn(
                            columnName,
                            encodeColumnType,
                            columnId,
                            colTop,
                            primaryMem.addressOf(0),
                            primaryMem.size(),
                            symbolValuesMem.addressOf(0),
                            symbolValuesMem.size(),
                            symbolOffsetsMem.addressOf(HEADER_SIZE),
                            symbolMapReader.getSymbolCount()
                    );
                } else if (ColumnType.isVarSize(columnType)) {
                    final MemoryR secondaryMem = tableReader.getColumn(primaryIndex + 1);
                    descriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            colTop,
                            primaryMem.addressOf(0),
                            primaryMem.size(),
                            secondaryMem.addressOf(0),
                            secondaryMem.size(),
                            0,
                            0
                    );
                } else {
                    descriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            colTop,
                            primaryMem.addressOf(0),
                            primaryMem.size(),
                            0,
                            0,
                            0,
                            0
                    );
                }
            }
        }
    }

    public static native long writeStreamingParquetChunk(
            long writerPtr,
            long columnDataPtr,
            long rowCount
    ) throws CairoException;

    public static native long writeStreamingParquetChunkFromRowGroup(
            long writerPtr,
            long allocatorPtr,
            long columnDataPtr,
            long sourceParquetAddr,
            long sourceParquetSize,
            int rowGroupIndex,
            int rowGroupLo,
            int rowGroupHi
    ) throws CairoException;

    private static native void encodePartition(
            long tableNamePtr,
            int tableNameSize,
            int columnCount,
            long columnNamesPtr,
            int columnNamesSize,
            long columnDataPtr,
            long columnDataSize,
            int timestampIndex,
            long rowCount,
            long destPathPtr,
            int destPathLength,
            long compressionCodec,
            boolean statisticsEnabled,
            boolean rawArrayEncoding,
            long rowGroupSize,
            long dataPageSize,
            int version,
            long bloomFilterColumnIndexesPtr,
            int bloomFilterColumnCount,
            double bloomFilterFpp
    ) throws CairoException;

    static {
        Os.init();
    }
}

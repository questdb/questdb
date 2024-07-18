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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.*;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

import static io.questdb.cairo.SymbolMapWriter.HEADER_SIZE;

public class PartitionEncoder {
    public static int COMPRESSION_BROTLI = 4;
    public static int COMPRESSION_GZIP = 2;
    public static int COMPRESSION_LZ4 = 5;
    public static int COMPRESSION_LZ4_RAW = 7;
    public static int COMPRESSION_LZO = 3;
    public static int COMPRESSION_SNAPPY = 1;
    public static int COMPRESSION_UNCOMPRESSED = 0;
    public static int COMPRESSION_ZSTD = 6;
    public static int PARQUET_VERSION_V1 = 1;
    public static int PARQUET_VERSION_V2 = 2;

    public static void encode(PartitionDescriptor descriptor, Path destPath) {
        encodeWithOptions(
                descriptor,
                destPath,
                COMPRESSION_UNCOMPRESSED,
                true,
                0, // DEFAULT_ROW_GROUP_SIZE
                0, // DEFAULT_DATA_PAGE_SIZE
                PARQUET_VERSION_V1
        );
    }

    public static void encodeWithOptions(
            PartitionDescriptor descriptor,
            Path destPath,
            long compressionCodec,
            boolean statisticsEnabled,
            long rowGroupSize,
            long dataPageSize,
            int version
    ) {
        final Utf8Sequence tableName = descriptor.getTableName();
        final int columnCount = descriptor.getColumnCount();
        final long partitionSize = descriptor.getPartitionRowCount();
        final int timestampIndex = descriptor.getTimestampIndex();
        try {
            encodePartition(
                    tableName.ptr(),
                    tableName.size(),
                    columnCount,
                    descriptor.getColumnNamesPtr(),
                    descriptor.getColumnNamesSize(),
                    descriptor.getColumnNameLengthsPtr(),
                    descriptor.getColumnTypesPtr(),
                    descriptor.getColumnIdsPtr(),
                    timestampIndex,
                    descriptor.getColumnTopsPtr(),
                    descriptor.getColumnAddressesPtr(),
                    descriptor.getColumnSizesPtr(),
                    descriptor.getColumnSecondaryAddressesPtr(),
                    descriptor.getColumnSecondarySizesPtr(),
                    descriptor.getSymbolOffsetsAddressesPtr(),
                    descriptor.getSymbolOffsetsSizesPtr(),
                    partitionSize,
                    destPath.ptr(),
                    destPath.size(),
                    compressionCodec,
                    statisticsEnabled,
                    rowGroupSize,
                    dataPageSize,
                    version
            );
        } catch (Throwable th) {
            throw CairoException.critical(0).put("Could not encode partition: [table=").put(tableName)
                    .put(", exception=").put(th.getClass().getSimpleName())
                    .put(", msg=").put(th.getMessage())
                    .put(']');
        } finally {
            descriptor.clear();
        }
    }

    public static void populateFromTableReader(TableReader tableReader, PartitionDescriptor descriptor, int partitionIndex) {
        final long partitionSize = tableReader.openPartition(partitionIndex);
        assert partitionSize != 0;
        final int timestampIndex = tableReader.getMetadata().getTimestampIndex();
        descriptor.of(tableReader.getTableToken().getTableName(), partitionSize, timestampIndex, false);

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

                if (ColumnType.isSymbol(columnType)) {
                    SymbolMapReader symbolMapReader = tableReader.getSymbolMapReader(i);
                    final MemoryR symbolValuesMem = symbolMapReader.getSymbolValuesColumn();
                    final MemoryR symbolOffsetsMem = symbolMapReader.getSymbolOffsetsColumn();
                    descriptor.addColumn(
                            columnName,
                            columnType,
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

    private static native void encodePartition(
            long tableNamePtr,
            int tableNameSize,
            int columnCount,
            long columnNamesPtr,
            int columnNamesLength,
            long columnNameLengthsPtr,
            long columnTypesPtr,
            long columnIdsPtr,
            int timestampIndex,
            long columnTopsPtr,
            long columnAddrsPtr,
            long columnSizesPtr,
            long columnSecondaryAddrsPtr,
            long columnSecondarySizesPtr,
            long symbolOffsetsAddrsPtr,
            long symbolOffsetsSizesPtr,
            long rowCount,
            long destPathPtr,
            int destPathLength,
            long compressionCodec,
            boolean statisticsEnabled,
            long rowGroupSize,
            long dataPageSize,
            int version
    );

    static {
        Os.init();
    }
}

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

package io.questdb.cairo;

import io.questdb.cairo.TableUtils.SymbolTableProvider;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionUpdater;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;

/**
 * Shared decode-to-current-schema pipeline used by O3 rewrites and background
 * parquet materialization. The caller owns partition-level file publication.
 */
final class ParquetRowGroupMaterializer {
    private ParquetRowGroupMaterializer() {
    }

    static void materialize(
            ParquetConversionContext context,
            ParquetPartitionDecoder decoder,
            PartitionUpdater partitionUpdater,
            int sourceRowGroupIndex,
            int targetRowGroupIndex,
            TableRecordMetadata metadata,
            IntList tableToParquetIndex,
            SymbolTableProvider symbolTableProvider
    ) {
        materialize(
                context,
                decoder,
                partitionUpdater,
                sourceRowGroupIndex,
                targetRowGroupIndex,
                metadata,
                tableToParquetIndex,
                symbolTableProvider,
                false,
                null,
                Long.MIN_VALUE
        );
    }

    static void materializeChangedColumns(
            ParquetConversionContext context,
            ParquetPartitionDecoder decoder,
            PartitionUpdater partitionUpdater,
            int sourceRowGroupIndex,
            TableRecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            IntList tableToParquetIndex,
            SymbolTableProvider symbolTableProvider
    ) {
        materialize(
                context,
                decoder,
                partitionUpdater,
                sourceRowGroupIndex,
                sourceRowGroupIndex,
                metadata,
                tableToParquetIndex,
                symbolTableProvider,
                true,
                columnVersionReader,
                partitionTimestamp
        );
    }

    private static void materialize(
            ParquetConversionContext context,
            ParquetPartitionDecoder decoder,
            PartitionUpdater partitionUpdater,
            int sourceRowGroupIndex,
            int targetRowGroupIndex,
            TableRecordMetadata metadata,
            IntList tableToParquetIndex,
            SymbolTableProvider symbolTableProvider,
            boolean changedColumnsOnly,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        final DirectIntList parquetColumns = context.getParquetColumns();
        parquetColumns.clear();
        final int columnCount = metadata.getColumnCount();
        final IntList activeToDecodeIndex = context.getActiveToDecodeIdx(columnCount);
        final IntList activeColumnIndexes = context.getActiveColIndices(columnCount);
        int activeColumnCount = 0;
        int decodeColumnCount = 0;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final int columnType = metadata.getColumnType(columnIndex);
            if (columnType < 0) {
                continue;
            }
            final int parquetIndex = tableToParquetIndex.getQuick(columnIndex);
            // The designated timestamp is always physically present from row zero and is not
            // represented by an ordinary column-version top; the reader's -1 sentinel therefore
            // means "no override", not an absent/full-top timestamp.
            final long columnTop = changedColumnsOnly && columnIndex != metadata.getTimestampIndex()
                    ? columnVersionReader.getColumnTop(partitionTimestamp, metadata.getWriterIndex(columnIndex))
                    : 0;
            final boolean materializeColumn = requiresMaterialization(
                    decoder,
                    parquetIndex,
                    columnType,
                    columnTop,
                    columnIndex == metadata.getTimestampIndex()
            );
            if (changedColumnsOnly && !materializeColumn) {
                continue;
            }
            if (parquetIndex >= 0) {
                parquetColumns.add(parquetIndex);
                parquetColumns.add(ParquetColumnTypeConverter.chooseDecodeType(decoder, parquetIndex, columnType));
                activeToDecodeIndex.setQuick(activeColumnCount, decodeColumnCount++);
            } else {
                activeToDecodeIndex.setQuick(activeColumnCount, -1);
            }
            activeColumnIndexes.setQuick(activeColumnCount++, columnIndex);
        }

        final long rowGroupSizeLong = decoder.metadata().getRowGroupSize(sourceRowGroupIndex);
        assert rowGroupSizeLong <= Integer.MAX_VALUE;
        final int rowGroupSize = (int) rowGroupSizeLong;
        final RowGroupBuffers rowGroupBuffers = context.getRowGroupBuffers();
        context.setLastDecodedColumnCount(decodeColumnCount);
        if (decodeColumnCount > 0) {
            decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, sourceRowGroupIndex, 0, rowGroupSize);
        }

        final PartitionDescriptor descriptor = context.getChunkDescriptor();
        descriptor.of(
                metadata.getTableToken().getTableName(),
                rowGroupSize,
                changedColumnsOnly ? -1 : metadata.getTimestampIndex()
        );
        final LongList ownedBuffers = context.getTmpBufs(activeColumnCount);
        final LongList targetPointers = context.getConvertedPtrs(activeColumnCount);

        try {
            for (int activeIndex = 0; activeIndex < activeColumnCount; activeIndex++) {
                final int columnIndex = activeColumnIndexes.getQuick(activeIndex);
                final int columnType = metadata.getColumnType(columnIndex);
                final TableColumnMetadata columnMetadata = metadata.getColumnMetadata(columnIndex);
                final int slot = activeIndex * 4;
                ParquetColumnTypeConverter.prepareSourceColumn(
                        decoder,
                        rowGroupBuffers,
                        tableToParquetIndex,
                        columnIndex,
                        columnType,
                        activeToDecodeIndex.getQuick(activeIndex),
                        rowGroupSize,
                        context,
                        ownedBuffers,
                        targetPointers,
                        slot
                );

                final String columnName = metadata.getColumnName(columnIndex);
                final int columnId = columnMetadata.getOriginalWriterIndex();
                final int parquetEncodingConfig = columnMetadata.getParquetEncodingConfig();
                final long columnDataAddress = targetPointers.getQuick(slot);
                final long columnDataSize = targetPointers.getQuick(slot + 1);
                final long columnAuxAddress = targetPointers.getQuick(slot + 2);
                final long columnAuxSize = targetPointers.getQuick(slot + 3);

                if (ColumnType.isVarSize(columnType)) {
                    descriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            0,
                            columnDataAddress,
                            columnDataSize,
                            columnAuxAddress,
                            columnAuxSize,
                            0,
                            0,
                            parquetEncodingConfig
                    );
                } else if (ColumnType.isSymbol(columnType)) {
                    final int symbolCount = symbolTableProvider.getSymbolCount(columnIndex);
                    final MemoryR offsetsMemory = symbolTableProvider.getSymbolOffsetsMemory(columnIndex);
                    final MemoryR valuesMemory = symbolTableProvider.getSymbolValuesMemory(columnIndex);
                    final long valuesSize = offsetsMemory.getLong(SymbolMapWriter.keyToOffset(symbolCount));
                    int encoderColumnType = columnType;
                    if (!symbolTableProvider.containsNullValue(columnIndex)) {
                        encoderColumnType |= ParquetColumnTypeConverter.PARQUET_SYMBOL_NOT_NULL_HINT;
                    }
                    descriptor.addColumn(
                            columnName,
                            encoderColumnType,
                            columnId,
                            0,
                            columnDataAddress,
                            columnDataSize,
                            valuesMemory.addressOf(0),
                            valuesSize,
                            offsetsMemory.addressOf(SymbolMapWriter.HEADER_SIZE),
                            symbolCount,
                            parquetEncodingConfig
                    );
                } else {
                    descriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            0,
                            columnDataAddress,
                            columnDataSize,
                            0,
                            0,
                            0,
                            0,
                            parquetEncodingConfig
                    );
                }
            }
            if (changedColumnsOnly) {
                partitionUpdater.rewriteRowGroupColumns(sourceRowGroupIndex, descriptor);
            } else {
                partitionUpdater.addRowGroup(targetRowGroupIndex, descriptor);
            }
        } finally {
            descriptor.clear();
            context.freeNativePairs(ownedBuffers);
        }
    }

    private static boolean requiresMaterialization(
            ParquetPartitionDecoder decoder,
            int parquetIndex,
            int targetType,
            long columnTop,
            boolean designatedTimestamp
    ) {
        if (parquetIndex < 0 || columnTop != 0) {
            return true;
        }
        final int sourceType = decoder.metadata().getColumnType(parquetIndex);
        if (sourceType != targetType
                && !(designatedTimestamp && ColumnType.tagOf(sourceType) == ColumnType.tagOf(targetType))) {
            return true;
        }
        // A raw Required page cannot sit under the modern Optional schema for
        // these no-sentinel types. Re-encode the column while the remaining,
        // schema-compatible chunks still take the hybrid raw-copy path.
        return decoder.metadata().getColumnMaxDefLevel(parquetIndex) == 0
                && (ColumnType.isSymbol(targetType) || ColumnType.isNoNullSentinelFixedType(targetType));
    }

    static void setTargetSchema(
            ParquetConversionContext context,
            PartitionUpdater partitionUpdater,
            TableRecordMetadata metadata,
            SymbolTableProvider symbolTableProvider
    ) {
        final PartitionDescriptor descriptor = context.getChunkDescriptor();
        descriptor.of(metadata.getTableToken().getTableName(), 0, metadata.getTimestampIndex());
        try {
            for (int columnIndex = 0, columnCount = metadata.getColumnCount(); columnIndex < columnCount; columnIndex++) {
                int columnType = metadata.getColumnType(columnIndex);
                if (columnType < 0) {
                    continue;
                }
                if (ColumnType.isSymbol(columnType) && !symbolTableProvider.containsNullValue(columnIndex)) {
                    columnType |= ParquetColumnTypeConverter.PARQUET_SYMBOL_NOT_NULL_HINT;
                }
                final TableColumnMetadata columnMetadata = metadata.getColumnMetadata(columnIndex);
                descriptor.addColumn(
                        metadata.getColumnName(columnIndex),
                        columnType,
                        columnMetadata.getOriginalWriterIndex(),
                        0,
                        columnMetadata.getParquetEncodingConfig()
                );
            }
            partitionUpdater.setTargetSchema(descriptor);
        } finally {
            descriptor.clear();
        }
    }
}

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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableAccessInfo;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;

/**
 * A table as one compilation plans it: the columns of the metadata version the binder bound, and the access facts of
 * the transaction the compiler read last. {@link PlanTables} copies both from a reader it releases at once, so the
 * binder, the optimiser and the code generator read this copy rather than a reader.
 */
public final class PlanTable implements Mutable, TableAccessInfo {
    private final IntList symbolCapacities = new IntList();
    private final IntList symbolColumnIndexes = new IntList();
    private final IntList symbolCounts = new IntList();
    private boolean hasConvertedColumns;
    private boolean hasParquetConvertedColumns;
    private boolean hasParquetPartitions;
    private GenericRecordMetadata metadata;
    private long metadataVersion;
    private int partitionBy;
    private long partitionTableVersion;
    private int position;
    private TableToken tableToken;

    @Override
    public void clear() {
        metadata = null;
        tableToken = null;
    }

    @Override
    public int getColumnIndex(CharSequence columnName) {
        return metadata.getColumnIndexQuiet(columnName);
    }

    @Override
    public int getColumnType(int columnIndex) {
        return metadata.getColumnType(columnIndex);
    }

    @Override
    public IntList getCoveringColumnIndices(int columnIndex) {
        return metadata.getColumnMetadata(columnIndex).getCoveringColumnIndices();
    }

    @Override
    public byte getIndexType(int columnIndex) {
        return metadata.getColumnIndexType(columnIndex);
    }

    /**
     * The table's columns, in the reader's order. The factories the compilation generates keep this metadata, so every
     * copy of the table makes a new one.
     */
    public RecordMetadata getMetadata() {
        return metadata;
    }

    public long getMetadataVersion() {
        return metadataVersion;
    }

    /**
     * The version of the partition table the access facts were read at; a parquet pushdown compares it at cursor open.
     */
    public long getPartitionTableVersion() {
        return partitionTableVersion;
    }

    @Override
    public int getPartitionedBy() {
        return partitionBy;
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return symbolCapacities.getQuick(columnIndex);
    }

    @Override
    public int getSymbolCount(int columnIndex) {
        return symbolCounts.getQuick(columnIndex);
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public int getWriterIndex(int columnIndex) {
        return metadata.getWriterIndex(columnIndex);
    }

    @Override
    public boolean hasParquetConvertedColumns() {
        return hasParquetConvertedColumns;
    }

    public boolean hasParquetPartitions() {
        return hasParquetPartitions;
    }

    @Override
    public boolean isSymbolTableStatic(int columnIndex) {
        return metadata.isSymbolTableStatic(columnIndex);
    }

    int getPosition() {
        return position;
    }

    /**
     * Copies the columns and the access facts of the table {@code reader} reads, at its metadata version.
     */
    void of(TableToken tableToken, int position, TableReader reader) {
        this.tableToken = tableToken;
        this.position = position;
        final TableReaderMetadata readerMetadata = reader.getMetadata();
        metadataVersion = readerMetadata.getMetadataVersion();
        partitionBy = readerMetadata.getPartitionBy();
        final int columnCount = readerMetadata.getColumnCount();
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        hasConvertedColumns = false;
        symbolColumnIndexes.clear();
        for (int i = 0; i < columnCount; i++) {
            final TableColumnMetadata column = TableColumnMetadata.copyOf(readerMetadata, i, readerMetadata.getColumnName(i));
            metadata.add(column);
            hasConvertedColumns |= column.getWriterIndex() != column.getOriginalWriterIndex();
            if (ColumnType.isSymbol(column.getColumnType())) {
                symbolColumnIndexes.add(i);
            }
        }
        metadata.setTimestampIndex(readerMetadata.getTimestampIndex());
        this.metadata = metadata;
        symbolCounts.setAll(columnCount, 0);
        symbolCapacities.setAll(columnCount, 0);
        readFacts(reader);
    }

    /**
     * Re-reads the access facts at the transaction of {@code reader}, which reads the metadata version of the copied
     * columns.
     */
    void readFacts(TableReader reader) {
        partitionTableVersion = reader.getTxFile().getPartitionTableVersion();
        hasParquetPartitions = reader.hasParquetPartitions();
        hasParquetConvertedColumns = hasParquetPartitions && hasConvertedColumns;
        for (int i = 0, n = symbolColumnIndexes.size(); i < n; i++) {
            final int columnIndex = symbolColumnIndexes.getQuick(i);
            final SymbolMapReader symbols = reader.getSymbolMapReader(columnIndex);
            symbolCounts.setQuick(columnIndex, symbols.getSymbolCount());
            symbolCapacities.setQuick(columnIndex, symbols.getSymbolCapacity());
        }
    }
}

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
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableAccessInfo;
import io.questdb.cairo.sql.TableDescriptor;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * A table as one compilation plans it: the columns of the metadata version the binder bound, and the access facts of
 * the transaction the compiler read last. {@link PlanTables} copies both from a reader it releases at once, so the
 * binder, the optimiser and the code generator read this copy rather than a reader. The copy keeps the facts of each
 * column in a holder, which the next compilation refills, and the factories the compilation generates get their own
 * copies of the columns they read: {@link #newColumnMetadata(int, String)} and {@link #getTableDescriptor()}.
 */
public final class PlanTable implements Mutable, TableAccessInfo {
    private final LowerCaseCharSequenceIntHashMap columnIndexes = new LowerCaseCharSequenceIntHashMap();
    private final ObjList<Column> columns = new ObjList<>();
    private final IntList symbolColumnIndexes = new IntList();
    private int columnCount;
    private TableDescriptor descriptor;
    private boolean hasConvertedColumns;
    private boolean hasParquetConvertedColumns;
    private boolean hasParquetPartitions;
    private long metadataVersion;
    private int partitionBy;
    private long partitionTableVersion;
    private int position;
    private long rowCount;
    private TableToken tableToken;
    private int timestampIndex;
    private int timestampType;

    @Override
    public void clear() {
        columnCount = 0;
        columnIndexes.clear();
        descriptor = null;
        tableToken = null;
    }

    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnIndex(CharSequence columnName) {
        return columnIndexes.get(columnName);
    }

    public String getColumnName(int columnIndex) {
        return columns.getQuick(columnIndex).name;
    }

    @Override
    public int getColumnType(int columnIndex) {
        return columns.getQuick(columnIndex).type;
    }

    @Override
    public int getCoveredPosition(int keyColumnIndex, int columnIndex) {
        final IntList covered = columns.getQuick(keyColumnIndex).coveredWriterIndexes;
        return covered.indexOf(getWriterIndex(columnIndex), 0, covered.size());
    }

    @Override
    public byte getIndexType(int columnIndex) {
        return columns.getQuick(columnIndex).indexType;
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
    public long getRowCount() {
        return rowCount;
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return columns.getQuick(columnIndex).symbolCapacity;
    }

    @Override
    public int getSymbolCount(int columnIndex) {
        return columns.getQuick(columnIndex).symbolCount;
    }

    /**
     * The descriptor the partition frame factories of this compilation keep for the table; the first call builds it.
     */
    public TableDescriptor getTableDescriptor() {
        if (descriptor == null) {
            final ObjList<String> columnNames = new ObjList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columnNames.add(columns.getQuick(i).name);
            }
            descriptor = new TableDescriptor(columnNames, timestampIndex, timestampType, hasConvertedColumns);
        }
        return descriptor;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public int getWriterIndex(int columnIndex) {
        return columns.getQuick(columnIndex).writerIndex;
    }

    @Override
    public boolean hasCoveringIndex(int columnIndex) {
        return columns.getQuick(columnIndex).coveredWriterIndexes.size() > 0;
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
        return columns.getQuick(columnIndex).isSymbolTableStatic;
    }

    /**
     * A new copy of the column at {@code columnIndex} named {@code name}, for the metadata of a factory that reads it.
     */
    public TableColumnMetadata newColumnMetadata(int columnIndex, String name) {
        return columns.getQuick(columnIndex).newColumnMetadata(name);
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
        descriptor = null;
        final TableReaderMetadata readerMetadata = reader.getMetadata();
        metadataVersion = readerMetadata.getMetadataVersion();
        partitionBy = readerMetadata.getPartitionBy();
        timestampIndex = readerMetadata.getTimestampIndex();
        timestampType = readerMetadata.getTimestampType();
        columnCount = readerMetadata.getColumnCount();
        columnIndexes.clear();
        symbolColumnIndexes.clear();
        hasConvertedColumns = false;
        for (int i = columns.size(); i < columnCount; i++) {
            columns.add(new Column());
        }
        for (int i = 0; i < columnCount; i++) {
            final TableColumnMetadata column = readerMetadata.getColumnMetadata(i);
            columns.getQuick(i).of(column);
            columnIndexes.put(column.getColumnName(), i);
            hasConvertedColumns |= column.isTypeConverted();
            if (ColumnType.isSymbol(column.getColumnType())) {
                symbolColumnIndexes.add(i);
            }
        }
        readFacts(reader);
    }

    /**
     * Re-reads the access facts at the transaction of {@code reader}, which reads the metadata version of the copied
     * columns.
     */
    void readFacts(TableReader reader) {
        partitionTableVersion = reader.getTxFile().getPartitionTableVersion();
        rowCount = reader.size();
        hasParquetPartitions = reader.hasParquetPartitions();
        hasParquetConvertedColumns = hasParquetPartitions && hasConvertedColumns;
        for (int i = 0, n = symbolColumnIndexes.size(); i < n; i++) {
            final int columnIndex = symbolColumnIndexes.getQuick(i);
            final SymbolMapReader symbols = reader.getSymbolMapReader(columnIndex);
            final Column column = columns.getQuick(columnIndex);
            column.symbolCount = symbols.getSymbolCount();
            column.symbolCapacity = symbols.getSymbolCapacity();
        }
    }

    /**
     * The facts of one column: the metadata the binder bound, and the symbol table facts of the transaction the
     * compiler read last, which {@link #readFacts} refreshes.
     */
    private static final class Column {
        private final IntList coveredWriterIndexes = new IntList(0);
        private int declaredSymbolCapacity;
        private byte indexType;
        private int indexValueBlockCapacity;
        private boolean isDedupKey;
        private boolean isSymbolCached;
        private boolean isSymbolTableStatic;
        private String name;
        private int originalWriterIndex;
        private int parquetEncodingConfig;
        private int replacingIndex;
        private int symbolCapacity;
        private int symbolCount;
        private int type;
        private int writerIndex;

        private TableColumnMetadata newColumnMetadata(String name) {
            final TableColumnMetadata column = new TableColumnMetadata(name, type, indexType, indexValueBlockCapacity,
                    isSymbolTableStatic, null, writerIndex, isDedupKey, replacingIndex, isSymbolCached, declaredSymbolCapacity,
                    originalWriterIndex);
            column.setParquetEncodingConfig(parquetEncodingConfig);
            return column;
        }

        private void of(TableColumnMetadata column) {
            name = column.getColumnName();
            type = column.getColumnType();
            indexType = column.getIndexType();
            indexValueBlockCapacity = column.getIndexValueBlockCapacity();
            isSymbolTableStatic = column.isSymbolTableStatic();
            writerIndex = column.getWriterIndex();
            isDedupKey = column.isDedupKeyFlag();
            replacingIndex = column.getReplacingIndex();
            isSymbolCached = column.isSymbolCacheFlag();
            declaredSymbolCapacity = column.getSymbolCapacity();
            originalWriterIndex = column.getOriginalWriterIndex();
            parquetEncodingConfig = column.getParquetEncodingConfig();
            coveredWriterIndexes.clear();
            final IntList covered = column.getCoveringColumnIndices();
            if (covered != null) {
                coveredWriterIndexes.addAll(covered);
            }
            symbolCount = 0;
            symbolCapacity = 0;
        }
    }
}

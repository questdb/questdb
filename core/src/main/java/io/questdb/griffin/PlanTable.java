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
 * binder, the optimiser and the code generator read this copy rather than a reader. The copy keeps the facts of every
 * column in flat lists, which the next compilation refills, and the factories the compilation generates get their own
 * copies of the columns they read: {@link #newColumnMetadata(int)} and {@link #getTableDescriptor()}.
 */
public final class PlanTable implements Mutable, TableAccessInfo {
    private static final int COVERED_HI = 9;
    private static final int COVERED_LO = 8;
    private static final int FACT_COUNT = 10;
    private static final int FLAGS = 7;
    private static final int FLAG_DEDUP_KEY = 1 << 9;
    private static final int FLAG_SYMBOL_CACHE = 1 << 10;
    private static final int FLAG_SYMBOL_TABLE_STATIC = 1 << 8;
    private static final int INDEX_TYPE_MASK = 0xff;
    private static final int INDEX_VALUE_BLOCK_CAPACITY = 4;
    private static final int ORIGINAL_WRITER_INDEX = 2;
    private static final int PARQUET_ENCODING_CONFIG = 6;
    private static final int REPLACING_INDEX = 3;
    private static final int SYMBOL_CAPACITY = 5;
    private static final int TYPE = 0;
    private static final int WRITER_INDEX = 1;
    private final IntList columnFacts = new IntList();
    private final LowerCaseCharSequenceIntHashMap columnIndexes = new LowerCaseCharSequenceIntHashMap();
    private final ObjList<String> columnNames = new ObjList<>();
    private final IntList coveredWriterIndexes = new IntList();
    private final IntList symbolCapacities = new IntList();
    private final IntList symbolColumnIndexes = new IntList();
    private final IntList symbolCounts = new IntList();
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

    @Override
    public void clear() {
        columnNames.clear();
        columnIndexes.clear();
        descriptor = null;
        tableToken = null;
    }

    public int getColumnCount() {
        return columnNames.size();
    }

    @Override
    public int getColumnIndex(CharSequence columnName) {
        return columnIndexes.get(columnName);
    }

    public String getColumnName(int columnIndex) {
        return columnNames.getQuick(columnIndex);
    }

    @Override
    public int getColumnType(int columnIndex) {
        return fact(columnIndex, TYPE);
    }

    @Override
    public int getCoveredPosition(int keyColumnIndex, int columnIndex) {
        final int lo = fact(keyColumnIndex, COVERED_LO);
        final int writerIndex = getWriterIndex(columnIndex);
        for (int i = lo, hi = fact(keyColumnIndex, COVERED_HI); i < hi; i++) {
            if (coveredWriterIndexes.getQuick(i) == writerIndex) {
                return i - lo;
            }
        }
        return -1;
    }

    @Override
    public byte getIndexType(int columnIndex) {
        return (byte) (fact(columnIndex, FLAGS) & INDEX_TYPE_MASK);
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
        return symbolCapacities.getQuick(columnIndex);
    }

    @Override
    public int getSymbolCount(int columnIndex) {
        return symbolCounts.getQuick(columnIndex);
    }

    /**
     * The descriptor the partition frame factories of this compilation keep for the table; the first call builds it.
     */
    public TableDescriptor getTableDescriptor() {
        if (descriptor == null) {
            descriptor = new TableDescriptor(new ObjList<>(columnNames), timestampIndex,
                    timestampIndex > -1 ? getColumnType(timestampIndex) : ColumnType.NULL, hasConvertedColumns);
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
        return fact(columnIndex, WRITER_INDEX);
    }

    @Override
    public boolean hasCoveringIndex(int columnIndex) {
        return fact(columnIndex, COVERED_HI) > fact(columnIndex, COVERED_LO);
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
        return (fact(columnIndex, FLAGS) & FLAG_SYMBOL_TABLE_STATIC) != 0;
    }

    /**
     * A new copy of the column at {@code columnIndex}, for the metadata of a factory that reads it.
     */
    public TableColumnMetadata newColumnMetadata(int columnIndex) {
        final int flags = fact(columnIndex, FLAGS);
        final TableColumnMetadata column = new TableColumnMetadata(
                columnNames.getQuick(columnIndex),
                fact(columnIndex, TYPE),
                (byte) (flags & INDEX_TYPE_MASK),
                fact(columnIndex, INDEX_VALUE_BLOCK_CAPACITY),
                (flags & FLAG_SYMBOL_TABLE_STATIC) != 0,
                null,
                fact(columnIndex, WRITER_INDEX),
                (flags & FLAG_DEDUP_KEY) != 0,
                fact(columnIndex, REPLACING_INDEX),
                (flags & FLAG_SYMBOL_CACHE) != 0,
                fact(columnIndex, SYMBOL_CAPACITY),
                fact(columnIndex, ORIGINAL_WRITER_INDEX)
        );
        column.setParquetEncodingConfig(fact(columnIndex, PARQUET_ENCODING_CONFIG));
        final int lo = fact(columnIndex, COVERED_LO);
        if (lo > -1) {
            final int hi = fact(columnIndex, COVERED_HI);
            final IntList covered = new IntList(hi - lo);
            for (int i = lo; i < hi; i++) {
                covered.add(coveredWriterIndexes.getQuick(i));
            }
            column.setCoveringColumnIndices(covered);
        }
        return column;
    }

    private int fact(int columnIndex, int fact) {
        return columnFacts.getQuick(columnIndex * FACT_COUNT + fact);
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
        final int columnCount = readerMetadata.getColumnCount();
        columnNames.clear();
        columnIndexes.clear();
        columnFacts.setPos(columnCount * FACT_COUNT);
        coveredWriterIndexes.clear();
        symbolColumnIndexes.clear();
        hasConvertedColumns = false;
        for (int i = 0; i < columnCount; i++) {
            final TableColumnMetadata column = readerMetadata.getColumnMetadata(i);
            final String name = column.getColumnName();
            final int type = column.getColumnType();
            final int writerIndex = column.getWriterIndex();
            final int originalWriterIndex = column.getOriginalWriterIndex();
            final int facts = i * FACT_COUNT;
            columnNames.add(name);
            columnIndexes.put(name, i);
            columnFacts.setQuick(facts + TYPE, type);
            columnFacts.setQuick(facts + WRITER_INDEX, writerIndex);
            columnFacts.setQuick(facts + ORIGINAL_WRITER_INDEX, originalWriterIndex);
            columnFacts.setQuick(facts + REPLACING_INDEX, column.getReplacingIndex());
            columnFacts.setQuick(facts + INDEX_VALUE_BLOCK_CAPACITY, column.getIndexValueBlockCapacity());
            columnFacts.setQuick(facts + SYMBOL_CAPACITY, column.getSymbolCapacity());
            columnFacts.setQuick(facts + PARQUET_ENCODING_CONFIG, column.getParquetEncodingConfig());
            columnFacts.setQuick(facts + FLAGS, (column.getIndexType() & INDEX_TYPE_MASK)
                    | (column.isSymbolTableStatic() ? FLAG_SYMBOL_TABLE_STATIC : 0)
                    | (column.isDedupKeyFlag() ? FLAG_DEDUP_KEY : 0)
                    | (column.isSymbolCacheFlag() ? FLAG_SYMBOL_CACHE : 0));
            final IntList covered = column.getCoveringColumnIndices();
            if (covered != null) {
                columnFacts.setQuick(facts + COVERED_LO, coveredWriterIndexes.size());
                coveredWriterIndexes.addAll(covered);
                columnFacts.setQuick(facts + COVERED_HI, coveredWriterIndexes.size());
            } else {
                columnFacts.setQuick(facts + COVERED_LO, -1);
                columnFacts.setQuick(facts + COVERED_HI, -1);
            }
            hasConvertedColumns |= writerIndex != originalWriterIndex;
            if (ColumnType.isSymbol(type)) {
                symbolColumnIndexes.add(i);
            }
        }
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
        rowCount = reader.size();
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

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.Chars;

import static io.questdb.cairo.TableUtils.META_OFFSET_PARTITION_BY;

public class TableWriterMetadata extends AbstractRecordMetadata implements TableRecordMetadata, TableStructure {
    private int maxUncommittedRows;
    private long metadataVersion;
    private long o3MaxLag;
    private int partitionBy;
    private int symbolMapCount;
    private int tableId;
    private TableToken tableToken;
    private int version;
    private boolean walEnabled;

    public TableWriterMetadata(TableToken tableToken, MemoryMR metaMem) {
        this.tableToken = tableToken;
        reload(metaMem);
    }

    @Override
    public void close() {
        // nothing to release
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return getColumnMetadata(columnIndex).getIndexValueBlockCapacity();
    }

    @Override
    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    @Override
    public long getMetadataVersion() {
        return metadataVersion;
    }

    @Override
    public long getO3MaxLag() {
        return o3MaxLag;
    }

    @Override
    public int getPartitionBy() {
        return partitionBy;
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        return getColumnMetadata(columnIndex).isSymbolTableStatic();
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return ((WriterTableColumnMetadata) getColumnMetadata(columnIndex)).symbolCapacity;
    }

    public int getSymbolMapCount() {
        return symbolMapCount;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public CharSequence getTableName() {
        return tableToken.getTableName();
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public boolean isIndexed(int columnIndex) {
        return getColumnMetadata(columnIndex).isIndexed();
    }

    @Override
    public boolean isSequential(int columnIndex) {
        return ((WriterTableColumnMetadata) getColumnMetadata(columnIndex)).sequential;
    }

    @Override
    public boolean isWalEnabled() {
        return walEnabled;
    }

    public final void reload(MemoryMR metaMem) {
        this.partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);
        this.columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        this.columnNameIndexMap.clear();
        this.version = metaMem.getInt(TableUtils.META_OFFSET_VERSION);
        this.tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
        this.maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
        this.o3MaxLag = metaMem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG);
        TableUtils.validateMeta(metaMem, columnNameIndexMap, ColumnType.VERSION);
        this.timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        this.columnMetadata.clear();
        this.metadataVersion = metaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION);
        this.walEnabled = metaMem.getBool(TableUtils.META_OFFSET_WAL_ENABLED);

        long offset = TableUtils.getColumnNameOffset(columnCount);
        this.symbolMapCount = 0;
        columnNameIndexMap.clear();
        // don't create strings in this loop, we already have them in columnNameIndexMap
        for (int i = 0; i < columnCount; i++) {
            CharSequence name = metaMem.getStr(offset);
            assert name != null;
            int type = TableUtils.getColumnType(metaMem, i);
            String nameStr = Chars.toString(name);
            columnMetadata.add(
                    new WriterTableColumnMetadata(
                            nameStr,
                            type,
                            TableUtils.isColumnIndexed(metaMem, i),
                            TableUtils.getIndexBlockCapacity(metaMem, i),
                            TableUtils.isSymbolCached(metaMem, i),
                            null,
                            i,
                            TableUtils.isSequential(metaMem, i),
                            TableUtils.getSymbolCapacity(metaMem, i),
                            TableUtils.isColumnDedupKey(metaMem, i)
                    )
            );
            if (type > -1) {
                columnNameIndexMap.put(nameStr, i);
                if (ColumnType.isSymbol(type)) {
                    symbolMapCount++;
                }
            }
            offset += Vm.getStorageLength(name);
        }
    }

    public void setMaxUncommittedRows(int rows) {
        this.maxUncommittedRows = rows;
    }

    public void setMetadataVersion(long value) {
        this.metadataVersion = value;
    }

    public void setO3MaxLag(long o3MaxLagUs) {
        this.o3MaxLag = o3MaxLagUs;
    }

    public void setTableVersion() {
        version = ColumnType.VERSION;
    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }

    void addColumn(CharSequence name, int type, boolean indexFlag, int indexValueBlockCapacity, int columnIndex, boolean sequential, int symbolCapacity, boolean isDedupKey) {
        String str = name.toString();
        columnNameIndexMap.put(str, columnMetadata.size());
        columnMetadata.add(
                new WriterTableColumnMetadata(
                        str,
                        type,
                        indexFlag,
                        indexValueBlockCapacity,
                        true,
                        null,
                        columnIndex,
                        sequential,
                        symbolCapacity,
                        isDedupKey
                )
        );
        columnCount++;
        if (ColumnType.isSymbol(type)) {
            symbolMapCount++;
        }
    }

    void clearTimestampIndex() {
        this.timestampIndex = -1;
    }

    void removeColumn(int columnIndex) {
        TableColumnMetadata deletedMeta = columnMetadata.getQuick(columnIndex);
        if (ColumnType.isSymbol(deletedMeta.getType())) {
            symbolMapCount--;
        }
        deletedMeta.markDeleted();
        columnNameIndexMap.remove(deletedMeta.getName());
    }

    void renameColumn(CharSequence name, CharSequence newName) {
        final int columnIndex = columnNameIndexMap.removeEntry(name);
        String newNameStr = Chars.toString(newName);
        columnNameIndexMap.put(newNameStr, columnIndex);

        TableColumnMetadata oldColumnMetadata = columnMetadata.get(columnIndex);
        oldColumnMetadata.setName(newNameStr);
    }

    private static class WriterTableColumnMetadata extends TableColumnMetadata {
        private final boolean sequential;
        private final int symbolCapacity;

        public WriterTableColumnMetadata(String nameStr, int type, boolean columnIndexed, int indexBlockCapacity, boolean symbolTableStatic, RecordMetadata parent, int i, boolean sequential, int symbolCapacity, boolean isDedupKey) {
            super(nameStr, type, columnIndexed, indexBlockCapacity, symbolTableStatic, parent, i, isDedupKey);
            this.sequential = sequential;
            this.symbolCapacity = symbolCapacity;
        }
    }
}

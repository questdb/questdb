/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;

public class TableWriterMetadata extends BaseRecordMetadata {
    private int id;
    private int metaFileSize;
    private int symbolMapCount;
    private int version;
    private int maxUncommittedRows;
    private long commitLag;
    private long structureVersion;

    public TableWriterMetadata(MemoryMR metaMem) {
        reload(metaMem);
    }

    public void reload(MemoryMR metaMem) {
        this.columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        this.columnNameIndexMap = new LowerCaseCharSequenceIntHashMap(columnCount);
        this.version = metaMem.getInt(TableUtils.META_OFFSET_VERSION);
        this.id = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
        this.maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
        this.commitLag = metaMem.getLong(TableUtils.META_OFFSET_COMMIT_LAG);
        TableUtils.validateMeta(metaMem, columnNameIndexMap, ColumnType.VERSION);
        this.timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        this.columnMetadata = new ObjList<>(this.columnCount);
        this.structureVersion = metaMem.getLong(TableUtils.META_OFFSET_STRUCTURE_VERSION);

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
                    new TableColumnMetadata(
                            nameStr,
                            TableUtils.getColumnHash(metaMem, i),
                            type,
                            TableUtils.isColumnIndexed(metaMem, i),
                            TableUtils.getIndexBlockCapacity(metaMem, i),
                            true,
                            null,
                            i
                    )
            );
            columnNameIndexMap.put(nameStr, i);
            if (ColumnType.isSymbol(type)) {
                symbolMapCount++;
            }
            offset += Vm.getStorageLength(name);
        }
        metaFileSize = (int) offset;
    }

    public long getCommitLag() {
        return commitLag;
    }

    public int getDenseColumnCount() {
        int count = 0;
        for (int i = 0; i < columnCount; i++) {
            if (columnMetadata.getQuick(i).getType() > 0) {
                count++;
            }
        }
        return count;
    }

    public void setCommitLag(long micros) {
        this.commitLag = micros;
    }

    public int getFileDataSize() {
        return metaFileSize;
    }

    public int getId() {
        return id;
    }

    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    public void setMaxUncommittedRows(int rows) {
        this.maxUncommittedRows = rows;
    }

    public long getStructureVersion() {
        return structureVersion;
    }

    public void setStructureVersion(long value) {
        this.structureVersion = value;
    }

    public int getSymbolMapCount() {
        return symbolMapCount;
    }

    public int getTableVersion() {
        return version;
    }

    public void setTableVersion() {
        version = ColumnType.VERSION;
    }

    public GenericRecordMetadata copyDense() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0; i < columnCount; i++) {
            TableColumnMetadata column = columnMetadata.getQuick(i);
            if (column.getType() >= 0) {
                metadata.add(column);
                if (i == timestampIndex) {
                    metadata.setTimestampIndex(metadata.getColumnCount() - 1);
                }
            }
        }
        return metadata;
    }

    void addColumn(CharSequence name, long hash, int type, boolean indexFlag, int indexValueBlockCapacity, int columnIndex) {
        String str = name.toString();
        columnNameIndexMap.put(str, columnMetadata.size());
        columnMetadata.add(
                new TableColumnMetadata(
                        str,
                        hash,
                        type,
                        indexFlag,
                        indexValueBlockCapacity,
                        true,
                        null,
                        columnIndex
                )
        );
        columnCount++;
        if (ColumnType.isSymbol(type)) {
            symbolMapCount++;
        }
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
        columnNameIndexMap.put(newName, columnIndex);

        TableColumnMetadata oldColumnMetadata = columnMetadata.get(columnIndex);
        oldColumnMetadata.setName(Chars.toString(newName));
    }

    void setTimestampIndex(int index) {
        this.timestampIndex = index;
    }
}

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.FilesFacade;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;

public class TableWriterMetadata extends BaseRecordMetadata {
    private int symbolMapCount;
    private int version;
    private final int id;
    private int maxUncommittedRows;
    private long commitLag;

    public TableWriterMetadata(FilesFacade ff, MemoryMR metaMem) {
        this.columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        this.columnNameIndexMap = new LowerCaseCharSequenceIntHashMap(columnCount);
        this.version = metaMem.getInt(TableUtils.META_OFFSET_VERSION);
        this.id = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
        this.maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
        this.commitLag = metaMem.getLong(TableUtils.META_OFFSET_COMMIT_LAG);
        TableUtils.validate(ff, metaMem, columnNameIndexMap, ColumnType.VERSION);
        this.timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        this.columnMetadata = new ObjList<>(this.columnCount);

        long offset = TableUtils.getColumnNameOffset(columnCount);
        this.symbolMapCount = 0;
        // don't create strings in this loop, we already have them in columnNameIndexMap
        for (int i = 0; i < columnCount; i++) {
            CharSequence name = metaMem.getStr(offset);
            assert name != null;
            int type = TableUtils.getColumnType(metaMem, i);
            columnMetadata.add(
                    new TableColumnMetadata(
                            Chars.toString(name),
                            type,
                            TableUtils.isColumnIndexed(metaMem, i),
                            TableUtils.getIndexBlockCapacity(metaMem, i),
                            true,
                            null
                    )
            );
            if (ColumnType.isSymbol(type)) {
                symbolMapCount++;
            }
            offset += Vm.getStorageLength(name);
        }
    }

    public int getSymbolMapCount() {
        return symbolMapCount;
    }

    void addColumn(CharSequence name, int type, boolean indexFlag, int indexValueBlockCapacity) {
        String str = name.toString();
        columnNameIndexMap.put(str, columnMetadata.size());
        columnMetadata.add(
                new TableColumnMetadata(
                        str,
                        type,
                        indexFlag,
                        indexValueBlockCapacity,
                        true,
                        null
                )
        );
        columnCount++;
        if (ColumnType.isSymbol(type)) {
            symbolMapCount++;
        }
    }

    void removeColumn(CharSequence name) {
        int index = columnNameIndexMap.keyIndex(name);
        int columnIndex = columnNameIndexMap.valueAt(index);
        if (ColumnType.isSymbol(columnMetadata.getQuick(columnIndex).getType())) {
            symbolMapCount--;
        }
        columnMetadata.remove(columnIndex);
        columnNameIndexMap.removeAt(index);
        columnCount--;

        // enumerate columns that would have moved up after column deletion
        for (int i = columnIndex; i < columnCount; i++) {
            columnNameIndexMap.put(columnMetadata.getQuick(i).getName(), i);
        }
    }

    void renameColumn(CharSequence name, CharSequence newName) {
        int index = columnNameIndexMap.keyIndex(name);
        int columnIndex = columnNameIndexMap.valueAt(index);
        columnNameIndexMap.removeAt(index);
        columnNameIndexMap.putAt(columnNameIndexMap.keyIndex(newName), newName, columnIndex);
        //
        TableColumnMetadata oldColumnMetadata = columnMetadata.get(columnIndex);
        oldColumnMetadata.setName(Chars.toString(newName));
    }

    void setTimestampIndex(int index) {
        this.timestampIndex = index;
    }

    public int getTableVersion() {
        return version;
    }

    public void setTableVersion() {
        version = ColumnType.VERSION;
    }

    public int getId() {
        return id;
    }

    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    public long getCommitLag() {
        return commitLag;
    }

    public void setMaxUncommittedRows(int rows) {
        this.maxUncommittedRows = rows;
    }

    public void setCommitLag(long micros) {
        this.commitLag = micros;
    }
}

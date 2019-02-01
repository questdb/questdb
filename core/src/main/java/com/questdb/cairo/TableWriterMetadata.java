/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.FilesFacade;
import com.questdb.std.ObjList;

public class TableWriterMetadata extends BaseRecordMetadata {
    private int symbolMapCount;

    public TableWriterMetadata(FilesFacade ff, ReadOnlyMemory metaMem) {
        this.columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        this.columnNameIndexMap = new CharSequenceIntHashMap(columnCount);
        TableUtils.validate(ff, metaMem, columnNameIndexMap);
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
                            name.toString(),
                            type,
                            TableUtils.isColumnIndexed(metaMem, i),
                            TableUtils.getIndexBlockCapacity(metaMem, i)
                    )
            );
            if (type == ColumnType.SYMBOL) {
                symbolMapCount++;
            }
            offset += ReadOnlyMemory.getStorageLength(name);
        }
    }

    public int getSymbolMapCount() {
        return symbolMapCount;
    }

    void addColumn(CharSequence name, int type, boolean indexFlag, int indexValueBlockCapacity) {
        String str = name.toString();
        columnNameIndexMap.put(str, columnMetadata.size());
        columnMetadata.add(new TableColumnMetadata(str, type, indexFlag, indexValueBlockCapacity));
        columnCount++;
        if (type == ColumnType.SYMBOL) {
            symbolMapCount++;
        }
    }

    void removeColumn(CharSequence name) {
        int index = columnNameIndexMap.keyIndex(name);
        int columnIndex = columnNameIndexMap.valueAt(index);
        if (columnMetadata.getQuick(columnIndex).getType() == ColumnType.SYMBOL) {
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
}

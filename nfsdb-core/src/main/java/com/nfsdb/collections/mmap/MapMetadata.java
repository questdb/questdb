/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.collections.mmap;

import com.nfsdb.collections.ObjIntHashMap;
import com.nfsdb.column.ColumnType;
import com.nfsdb.column.SymbolTable;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.lang.cst.impl.qry.RecordMetadata;

import java.util.List;

public final class MapMetadata implements RecordMetadata {

    private final ObjIntHashMap<CharSequence> nameCache;
    private final int columnCount;
    private final RecordColumnMetadata[] columns;

    public MapMetadata(List<RecordColumnMetadata> valueColumns, List<RecordColumnMetadata> keyColumns) {
        this.columnCount = valueColumns.size() + keyColumns.size();
        this.nameCache = new ObjIntHashMap<>(columnCount);
        this.columns = new RecordColumnMetadata[columnCount];
        int split = valueColumns.size();

        for (int i = 0; i < split; i++) {
            columns[i] = valueColumns.get(i);
            nameCache.put(columns[i].getName(), i);
        }

        for (int i = 0, sz = keyColumns.size(); i < sz; i++) {
            columns[i] = keyColumns.get(i);
            nameCache.put(columns[i].getName(), split + i);
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public ColumnType getColumnType(int x) {
        return getColumn(x).getType();
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        int index = nameCache.get(name);
        if (index == -1) {
            throw new JournalRuntimeException("No such column: " + name);
        }
        return index;
    }

    @Override
    public SymbolTable getSymbolTable(int index) {
        return getColumn(index).getSymbolTable();
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return columns[index];
    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return columns[getColumnIndex(name)];
    }
}

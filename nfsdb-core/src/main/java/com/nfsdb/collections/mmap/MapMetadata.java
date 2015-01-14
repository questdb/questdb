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
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.lang.cst.impl.qry.RecordMetadata;

import java.util.List;

public final class MapMetadata implements RecordMetadata {

    private final ObjIntHashMap<CharSequence> nameCache;
    private final int columnCount;
    private final ColumnType[] types;
    private final SymbolTable[] symbolTables;
    private final String[] columnNames;


    public MapMetadata(List<ColumnMetadata> valueColumns, List<ColumnMetadata> keyColumns) {
        this.columnCount = valueColumns.size() + keyColumns.size();
        this.types = new ColumnType[columnCount];
        this.nameCache = new ObjIntHashMap<>(columnCount);
        this.symbolTables = new SymbolTable[columnCount];
        this.columnNames = new String[columnCount];
        int split = valueColumns.size();

        for (int i = 0; i < split; i++) {
            ColumnMetadata m = valueColumns.get(i);
            types[i] = m.type;
            symbolTables[i] = m.symbolTable;
            nameCache.put(columnNames[i] = m.name, i);
        }

        for (int i = 0, sz = keyColumns.size(); i < sz; i++) {
            ColumnMetadata m = keyColumns.get(i);
            types[split + i] = m.type;
            symbolTables[split + i] = m.symbolTable;
            nameCache.put(columnNames[split + i] = m.name, split + i);
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public ColumnType getColumnType(int x) {
        return types[x];
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
        return symbolTables[index];
    }

    @Override
    public String getColumnName(int index) {
        return columnNames[index];
    }
}

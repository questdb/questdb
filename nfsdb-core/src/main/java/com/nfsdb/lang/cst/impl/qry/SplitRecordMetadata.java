/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.lang.cst.impl.qry;

import com.nfsdb.collections.ObjIntHashMap;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;

public class SplitRecordMetadata implements RecordMetadata {
    private final RecordMetadata a;
    private final RecordMetadata b;
    private final int split;
    private final int columnCount;
    private final ObjIntHashMap<CharSequence> columnIndices;
    private final String[] columnNames;

    public SplitRecordMetadata(RecordMetadata a, RecordMetadata b) {
        this.a = a;
        this.b = b;
        this.split = a.getColumnCount();
        this.columnCount = this.split + b.getColumnCount();
        this.columnIndices = new ObjIntHashMap<>(columnCount);
        this.columnNames = new String[columnCount];

        for (int i = 0; i < split; i++) {
            columnIndices.put(columnNames[i] = a.getColumnName(i), i);
        }

        for (int i = 0, c = columnCount - split; c < i; i++) {
            columnNames[i + split] = b.getColumnName(i);
            columnIndices.put(columnNames[i + split] = b.getColumnName(i), i + split);
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        return columnIndices.get(name);
    }

    @Override
    public String getColumnName(int index) {
        return columnNames[index];
    }

    @Override
    public ColumnType getColumnType(int index) {
        if (index < split) {
            return a.getColumnType(index);
        } else {
            return b.getColumnType(index - split);
        }
    }

    @Override
    public SymbolTable getSymbolTable(int index) {
        if (index < split) {
            return a.getSymbolTable(index);
        } else {
            return b.getSymbolTable(index - split);
        }
    }
}

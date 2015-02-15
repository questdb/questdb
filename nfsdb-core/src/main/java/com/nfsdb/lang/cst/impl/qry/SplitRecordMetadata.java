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

package com.nfsdb.lang.cst.impl.qry;

import com.nfsdb.collections.ObjIntHashMap;
import com.nfsdb.column.ColumnType;
import com.nfsdb.column.SymbolTable;
import com.nfsdb.factory.configuration.RecordColumnMetadata;

public class SplitRecordMetadata implements RecordMetadata {
    private final RecordMetadata a;
    private final RecordMetadata b;
    private final int split;
    private final int columnCount;
    private final ObjIntHashMap<CharSequence> columnIndices;
    private final RecordColumnMetadata[] columns;

    public SplitRecordMetadata(RecordMetadata a, RecordMetadata b) {
        this.a = a;
        this.b = b;
        this.split = a.getColumnCount();
        this.columnCount = this.split + b.getColumnCount();
        this.columnIndices = new ObjIntHashMap<>(columnCount);
        this.columns = new RecordColumnMetadata[columnCount];

        for (int i = 0; i < split; i++) {
            RecordColumnMetadata rc = a.getColumn(i);
            columns[i] = rc;
            columnIndices.put(columns[i].getName(), i);
        }

        for (int i = 0, c = columnCount - split; c < i; i++) {
            columns[i + split] = b.getColumn(i);
            columnIndices.put(columns[i + split].getName(), i + split);
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
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
        return columns[index].getSymbolTable();
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        return columnIndices.get(name);
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

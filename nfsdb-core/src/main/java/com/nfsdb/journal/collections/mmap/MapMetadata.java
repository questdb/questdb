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

package com.nfsdb.journal.collections.mmap;

import com.nfsdb.journal.collections.ObjIntHashMap;
import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.factory.configuration.ColumnMetadata;
import com.nfsdb.journal.lang.cst.impl.qry.RecordMetadata;

import java.util.List;

public class MapMetadata implements RecordMetadata {

    private final ObjIntHashMap<CharSequence> nameCache;
    private final int columnCount;
    private final ColumnType types[];

    public MapMetadata(List<ColumnMetadata> valueColumns, List<ColumnMetadata> keyColumns) {
        this.columnCount = valueColumns.size() + keyColumns.size();
        this.types = new ColumnType[columnCount];
        this.nameCache = new ObjIntHashMap<>(columnCount);
        int split = valueColumns.size();

        for (int i = 0; i < split; i++) {
            types[i] = valueColumns.get(i).type;
            nameCache.put(valueColumns.get(i).name, i);
        }

        for (int i = 0, sz = keyColumns.size(); i < sz; i++) {
            types[split + i] = keyColumns.get(i).type;
            nameCache.put(keyColumns.get(i).name, split + i);
        }
    }

    @Override
    public RecordMetadata nextMetadata() {
        return null;
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
        return nameCache.get(name);
    }
}

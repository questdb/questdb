/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql.collections;

import com.nfsdb.collections.CharSequenceIntHashMap;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.utils.Unsafe;

public final class MapMetadata extends AbstractRecordMetadata {

    private final CharSequenceIntHashMap nameCache;
    private final int columnCount;
    private final RecordColumnMetadata[] columns;

    public MapMetadata(ObjList<RecordColumnMetadata> valueColumns, ObjList<RecordColumnMetadata> keyColumns) {
        this.columnCount = valueColumns.size() + keyColumns.size();
        this.nameCache = new CharSequenceIntHashMap(columnCount);
        this.columns = new RecordColumnMetadata[columnCount];
        int split = valueColumns.size();

        for (int i = 0; i < split; i++) {
            columns[i] = valueColumns.get(i);
            nameCache.put(columns[i].getName(), i);
        }

        for (int i = 0, sz = keyColumns.size(); i < sz; i++) {
            columns[split + i] = keyColumns.get(i);
            nameCache.put(columns[split + i].getName(), split + i);
        }
    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return getColumnQuick(getColumnIndex(name));
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return columns[index];
    }

    @Override
    public int getColumnCount() {
        return columnCount;
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
    public RecordColumnMetadata getColumnQuick(int index) {
        return Unsafe.arrayGet(columns, index);
    }

    @Override
    public RecordColumnMetadata getTimestampMetadata() {
        return null;
    }

    @Override
    public boolean invalidColumn(CharSequence name) {
        return nameCache.get(name) == -1;
    }
}

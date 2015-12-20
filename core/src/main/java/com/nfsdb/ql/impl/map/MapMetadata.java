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

package com.nfsdb.ql.impl.map;

import com.nfsdb.collections.CharSequenceIntHashMap;
import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.collections.ObjList;
import com.nfsdb.collections.Transient;
import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Unsafe;

public final class MapMetadata extends AbstractRecordMetadata {

    private final CharSequenceIntHashMap nameCache;
    private final int columnCount;
    private final RecordColumnMetadata[] columns;

    public MapMetadata(
            @Transient RecordMetadata keySourceMetadata,
            @Transient ObjHashSet<String> keyNames,
            @Transient ObjList<RecordColumnMetadata> valueColumns) {

        int split = valueColumns.size();
        this.columnCount = split + keyNames.size();
        this.nameCache = new CharSequenceIntHashMap(columnCount);
        this.columns = new RecordColumnMetadata[columnCount];

        for (int i = 0; i < split; i++) {
            columns[i] = valueColumns.get(i);
            nameCache.put(columns[i].getName(), i);
        }

        for (int i = 0, sz = keyNames.size(); i < sz; i++) {
            int index = keySourceMetadata.getColumnIndex(keyNames.get(i));
            columns[split + i] = keySourceMetadata.getColumnQuick(index);
            nameCache.put(keyNames.get(i), split + i);
        }
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
    public int getColumnIndexQuiet(CharSequence name) {
        return nameCache.get(name);
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return Unsafe.arrayGet(columns, index);
    }

    @Override
    public int getTimestampIndex() {
        return -1;
    }
}

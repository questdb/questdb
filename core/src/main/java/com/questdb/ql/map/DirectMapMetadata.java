/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.map;

import com.questdb.std.*;
import com.questdb.store.AbstractRecordMetadata;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.RecordMetadata;

public final class DirectMapMetadata extends AbstractRecordMetadata {

    private final CharSequenceIntHashMap nameCache;
    private final int columnCount;
    private final RecordColumnMetadata[] columns;

    public DirectMapMetadata(
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

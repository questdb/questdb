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

package com.questdb.ql.virtual;

import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.ObjList;
import com.questdb.store.AbstractRecordMetadata;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.RecordMetadata;

public class VirtualRecordMetadata extends AbstractRecordMetadata {
    private final RecordMetadata delegate;
    private final ObjList<VirtualColumn> virtualColumns;
    private final int split;
    private final CharSequenceIntHashMap nameToIndexMap = new CharSequenceIntHashMap();

    public VirtualRecordMetadata(RecordMetadata delegate, ObjList<VirtualColumn> virtualColumns) {
        this.delegate = delegate;
        this.split = delegate.getColumnCount();
        this.virtualColumns = virtualColumns;

        for (int i = 0, k = virtualColumns.size(); i < k; i++) {
            nameToIndexMap.put(virtualColumns.getQuick(i).getName(), i + split);
        }
    }

    @Override
    public String getAlias() {
        return delegate.getAlias();
    }

    @Override
    public void setAlias(String alias) {
        delegate.setAlias(alias);
    }

    @Override
    public int getColumnCount() {
        return delegate.getColumnCount() + virtualColumns.size();
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        int index = nameToIndexMap.get(name);
        return index == -1 ? delegate.getColumnIndexQuiet(name) : index;
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return index < split ? delegate.getColumnQuick(index) : virtualColumns.getQuick(index - split);
    }

    @Override
    public int getTimestampIndex() {
        return delegate.getTimestampIndex();
    }
}

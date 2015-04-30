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

package com.nfsdb.ql.impl;

import com.nfsdb.collections.ObjIntHashMap;
import com.nfsdb.collections.ObjList;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.ops.VirtualColumn;

public class VirtualRecordMetadata implements RecordMetadata {
    private final RecordMetadata base;
    private final ObjList<VirtualColumn> virtualColumns;
    private final int split;
    private final ObjIntHashMap<CharSequence> nameToIndexMap = new ObjIntHashMap<>();

    public VirtualRecordMetadata(RecordMetadata base, ObjList<VirtualColumn> virtualColumns) {
        this.base = base;
        this.split = base.getColumnCount();
        this.virtualColumns = virtualColumns;

        for (int i = 0, k = virtualColumns.size(); i < k; i++) {
            nameToIndexMap.put(virtualColumns.get(i).getName(), i + split);
        }
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return index < split ? base.getColumn(index) : virtualColumns.get(index - split);
    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return getColumn(getColumnIndex(name));
    }

    @Override
    public int getColumnCount() {
        return base.getColumnCount() + virtualColumns.size();
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        int index = nameToIndexMap.get(name);
        return index == -1 ? base.getColumnIndex(name) : index;
    }

    @Override
    public boolean invalidColumn(CharSequence name) {
        return nameToIndexMap.get(name) == -1 && base.invalidColumn(name);
    }
}

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

package com.nfsdb.ql.impl;

import com.nfsdb.collections.CharSequenceIntHashMap;
import com.nfsdb.collections.ObjList;
import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.ops.VirtualColumn;

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
    public RecordColumnMetadata getColumn(CharSequence name) {
        return getColumnQuick(getColumnIndex(name));
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return index < split ? delegate.getColumn(index) : virtualColumns.get(index - split);
    }

    @Override
    public int getColumnCount() {
        return delegate.getColumnCount() + virtualColumns.size();
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        int index = nameToIndexMap.get(name);
        return index == -1 ? delegate.getColumnIndex(name) : index;
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return index < split ? delegate.getColumnQuick(index) : virtualColumns.getQuick(index - split);
    }

    @Override
    public RecordColumnMetadata getTimestampMetadata() {
        return delegate.getTimestampMetadata();
    }

    @Override
    public boolean invalidColumn(CharSequence name) {
        return nameToIndexMap.get(name) == -1 && delegate.invalidColumn(name);
    }


}

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

import com.nfsdb.collections.CharSequenceHashSet;
import com.nfsdb.collections.CharSequenceIntHashMap;
import com.nfsdb.collections.ObjList;
import com.nfsdb.factory.configuration.*;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Unsafe;

import java.util.Arrays;

public class SelectedColumnsMetadata extends AbstractRecordMetadata {
    private final RecordMetadata delegate;
    private final RecordColumnMetadata columnMetadata[];
    private final CharSequenceIntHashMap nameIndex;
    private final ColumnName columnName = new ColumnName();

    /**
     * Metadata that contains only selected columns from delegate metadata. There is also
     * a possibility to rename column via renameMap. Columns that are not renamed do
     * not have to be present in rename map.
     * <p/>
     * Both names and renameMap are read only and this metadata instances will not hold on
     * to their references, so these data structured do not have to be allocated new each time
     * constructing metadata.
     *
     * @param delegate  the delegate metadata
     * @param names     list of column names to select
     * @param aliases   set of column aliases
     */
    public SelectedColumnsMetadata(RecordMetadata delegate, ObjList<CharSequence> names, CharSequenceHashSet aliases) {
        this.delegate = delegate;
        int k = names.size();
        this.nameIndex = new CharSequenceIntHashMap(k);
        this.columnMetadata = new RecordColumnMetadata[k];
        for (int i = 0; i < k; i++) {
            CharSequence name = names.getQuick(i);
            CharSequence _newName = aliases.get(i);
            String result = (_newName != null ? _newName : name).toString();
            columnMetadata[i] = meta(delegate.getColumn(columnName.of(name)), result);
            nameIndex.put(result, i);
        }
    }

    public SelectedColumnsMetadata(RecordMetadata delegate, ObjList<CharSequence> names) {
        this.delegate = delegate;
        int k = names.size();
        this.nameIndex = new CharSequenceIntHashMap(k);
        this.columnMetadata = new RecordColumnMetadata[k];
        for (int i = 0; i < k; i++) {
            String name = names.getQuick(i).toString();
            columnMetadata[i] = meta(delegate.getColumn(name), name);
            nameIndex.put(name, i);
        }
    }

    @Override
    public String getAlias() {
        return delegate.getAlias();
    }

    @Override
    protected int getLocalColumnIndex(CharSequence name) {
        return nameIndex.get(name);
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return columnMetadata[index];
    }

    @Override
    public int getColumnCount() {
        return columnMetadata.length;
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return Unsafe.arrayGet(columnMetadata, index);
    }

    @Override
    public RecordColumnMetadata getTimestampMetadata() {
        return delegate.getTimestampMetadata();
    }

    @Override
    public String toString() {
        return "SelectedColumnsMetadata{" +
                "delegate=" + delegate +
                ", columnMetadata=" + Arrays.toString(columnMetadata) +
                '}';
    }

    private RecordColumnMetadata meta(RecordColumnMetadata from, String newName) {
        ColumnMetadata m = new ColumnMetadata();
        m.name = newName;
        m.distinctCountHint = from.getBucketCount();
        if ((m.type = from.getType()) == ColumnType.SYMBOL) {
            m.symbolTable = from.getSymbolTable();
        }
        m.indexed = from.isIndexed();
        return m;
    }
}

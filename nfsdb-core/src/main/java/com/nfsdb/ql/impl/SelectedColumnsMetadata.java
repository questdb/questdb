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
import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Unsafe;

import java.util.Arrays;

public class SelectedColumnsMetadata implements RecordMetadata {
    private final RecordMetadata delegate;
    private final RecordColumnMetadata columnMetadata[];
    private final CharSequenceIntHashMap nameIndex;

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
     * @param renameMap map of rename operations, original column name has to be key and new name is the value.
     */
    public SelectedColumnsMetadata(RecordMetadata delegate, ObjList<CharSequence> names, CharSequenceObjHashMap<String> renameMap) {
        this.delegate = delegate;
        int k = names.size();
        this.nameIndex = new CharSequenceIntHashMap(k);
        this.columnMetadata = new RecordColumnMetadata[k];
        for (int i = 0; i < k; i++) {
            CharSequence name = names.getQuick(i);
            String rename = renameMap.get(name);
            String newName = rename != null ? rename : name.toString();
            columnMetadata[i] = meta(delegate.getColumn(name), newName);
            nameIndex.put(newName, i);
        }
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return columnMetadata[index];
    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return getColumnQuick(getColumnIndex(name));
    }

    @Override
    public int getColumnCount() {
        return columnMetadata.length;
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        int index = nameIndex.get(name);
        if (index == -1) {
            throw new JournalRuntimeException("Invalid column name %s", name);
        }
        return index;
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
    public boolean invalidColumn(CharSequence name) {
        return nameIndex.get(name) == -1;
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

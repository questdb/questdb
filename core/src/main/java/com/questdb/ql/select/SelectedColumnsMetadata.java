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

package com.questdb.ql.select;

import com.questdb.ql.RecordColumnMetadataImpl;
import com.questdb.std.*;
import com.questdb.store.AbstractRecordMetadata;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.RecordMetadata;
import com.questdb.store.factory.configuration.ColumnName;

import java.util.Arrays;

class SelectedColumnsMetadata extends AbstractRecordMetadata {
    private final RecordMetadata delegate;
    private final RecordColumnMetadata columnMetadata[];
    private final CharSequenceIntHashMap nameIndex;
    private int timestampIndex = -1;

    /**
     * Metadata that contains only selected columns from delegate metadata. There is also
     * a possibility to rename column via renameMap. Columns that are not renamed do
     * not have to be present in rename map.
     * <p>
     * Both names and renameMap are read only and this metadata instances will not hold on
     * to their references, so these data structured do not have to be allocated new each time
     * constructing metadata.
     *
     * @param delegate the delegate metadata
     * @param names    list of column names to select
     * @param aliases  set of column aliases
     */
    SelectedColumnsMetadata(RecordMetadata delegate, @Transient ObjList<CharSequence> names, @Transient CharSequenceHashSet aliases) {
        this.delegate = delegate;
        int k = names.size();
        this.nameIndex = new CharSequenceIntHashMap(k);
        this.columnMetadata = new RecordColumnMetadata[k];
        for (int i = 0; i < k; i++) {
            CharSequence name = names.getQuick(i);
            CharSequence _newName = aliases == null ? null : aliases.get(i);
            String result = (_newName != null ? _newName : name).toString();
            int index = delegate.getColumnIndex(name);
            columnMetadata[i] = meta(delegate.getColumnQuick(index), result);
            nameIndex.put(result, i);
            if (index == delegate.getTimestampIndex()) {
                timestampIndex = i;
            }
        }
    }

    SelectedColumnsMetadata(RecordMetadata delegate, ObjList<CharSequence> names) {
        this(delegate, names, null);
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
        return columnMetadata.length;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        int index = nameIndex.get(name);
        if (index != -1) {
            return index;
        }

        String alias = getAlias();
        if (alias == null) {
            return -1;
        }

        ColumnName columnName = ColumnName.singleton(name);
        if (Chars.equalsNc(alias, columnName.alias())) {
            return nameIndex.get(columnName.name());
        }

        return -1;
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return Unsafe.arrayGet(columnMetadata, index);
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public String toString() {
        return "SelectedColumnsMetadata{" +
                "delegate=" + delegate +
                ", columnMetadata=" + Arrays.toString(columnMetadata) +
                '}';
    }

    private RecordColumnMetadata meta(RecordColumnMetadata from, String newName) {
        return new RecordColumnMetadataImpl(newName, from.getType(), from.getSymbolTable(), from.getBucketCount(), from.isIndexed());
    }
}

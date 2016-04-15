/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.select;

import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.CharSequenceHashSet;
import com.nfsdb.std.CharSequenceIntHashMap;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;

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
    SelectedColumnsMetadata(RecordMetadata delegate, ObjList<CharSequence> names, CharSequenceHashSet aliases) {
        this.delegate = delegate;
        int k = names.size();
        this.nameIndex = new CharSequenceIntHashMap(k);
        this.columnMetadata = new RecordColumnMetadata[k];
        for (int i = 0; i < k; i++) {
            CharSequence name = names.getQuick(i);
            CharSequence _newName = aliases == null ? null : aliases.get(i);
            String result = (_newName != null ? _newName : name).toString();
            int index = delegate.getColumnIndex(name);
            columnMetadata[i] = meta(delegate.getColumn(index), result);
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
    public RecordColumnMetadata getColumn(int index) {
        return columnMetadata[index];
    }

    @Override
    public int getColumnCount() {
        return columnMetadata.length;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        return nameIndex.get(name);
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

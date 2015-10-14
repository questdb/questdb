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
import com.nfsdb.collections.FlyweightCharSequence;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Unsafe;

public class SplitRecordMetadata extends AbstractRecordMetadata {
    private final int columnCount;
    private final RecordColumnMetadata[] columns;
    private final RecordColumnMetadata timestampMetadata;
    private final FlyweightCharSequence temp = new FlyweightCharSequence();
    private final RecordMetadata a;
    private final RecordMetadata b;
    private final CharSequenceIntHashMap aIndices = new CharSequenceIntHashMap();
    private final CharSequenceIntHashMap bIndices = new CharSequenceIntHashMap();
    private final int split;

    public SplitRecordMetadata(RecordMetadata a, RecordMetadata b) {
        this.a = a;
        this.b = b;

        int split = a.getColumnCount();
        this.timestampMetadata = a.getTimestampMetadata();
        this.columnCount = split + b.getColumnCount();
        this.columns = new RecordColumnMetadata[columnCount];

        for (int i = 0; i < split; i++) {
            RecordColumnMetadata m = a.getColumnQuick(i);
            columns[i] = m;
            aIndices.put(m.getName(), i);
        }

        for (int i = 0, c = columnCount - split; i < c; i++) {
            RecordColumnMetadata m = b.getColumnQuick(i);
            columns[i + split] = m;
            bIndices.put(m.getName(), i);
        }
        this.split = split;
    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return columns[getColumnIndex(name)];
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
        int index = getColumnIndex0(name);
        if (index == -1) {
            throw new JournalRuntimeException("Invalid column: %s", name);
        }
        return index;
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return Unsafe.arrayGet(columns, index);
    }

    @Override
    public RecordColumnMetadata getTimestampMetadata() {
        return timestampMetadata;
    }

    @Override
    public boolean invalidColumn(CharSequence name) {
        return getColumnIndex0(name) == -1;
    }

    private int getColumnIndex0(CharSequence name) {
        int dot = Chars.indexOf(name, '.');
        if (dot == -1) {
            int index = bIndices.get(name);
            if (index > -1) {
                return split + index;
            } else {
                return aIndices.get(name);
            }
        } else {
            temp.of(name, 0, dot);
            if (a.getAlias() != null && Chars.equals(a.getAlias(), temp)) {
                return aIndices.get(temp.of(name, dot + 1, name.length() - dot - 1));
            }

            if (b.getAlias() != null && Chars.equals(b.getAlias(), temp)) {
                int index = bIndices.get(temp.of(name, dot + 1, name.length() - dot - 1));
                return index == -1 ? index : index + split;
            }
            return -1;
        }
    }
}

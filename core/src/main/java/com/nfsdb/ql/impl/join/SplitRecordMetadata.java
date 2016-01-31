/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ql.impl.join;

import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Unsafe;

public class SplitRecordMetadata extends AbstractRecordMetadata {
    private final int columnCount;
    private final RecordColumnMetadata[] columns;
    private final RecordMetadata a;
    private final RecordMetadata b;
    private final int split;

    public SplitRecordMetadata(RecordMetadata a, RecordMetadata b) {
        this.a = a;
        this.b = b;

        int split = a.getColumnCount();
        this.columnCount = split + b.getColumnCount();
        this.columns = new RecordColumnMetadata[columnCount];

        for (int i = 0; i < split; i++) {
            RecordColumnMetadata m = a.getColumnQuick(i);
            columns[i] = m;
        }

        for (int i = 0, c = columnCount - split; i < c; i++) {
            RecordColumnMetadata m = b.getColumnQuick(i);
            columns[i + split] = m;
        }
        this.split = split;
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
        int index = a.getColumnIndexQuiet(name);
        if (index == -1) {
            index = b.getColumnIndexQuiet(name);
            if (index == -1) {
                return index;
            } else {
                return index + split;
            }
        } else {
            return index;
        }
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return Unsafe.arrayGet(columns, index);
    }

    @Override
    public int getTimestampIndex() {
        return a.getTimestampIndex();
    }
}

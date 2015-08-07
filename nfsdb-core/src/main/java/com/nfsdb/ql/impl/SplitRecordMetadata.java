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
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.ql.RecordMetadata;

public class SplitRecordMetadata implements RecordMetadata {
    private final int columnCount;
    private final CharSequenceIntHashMap columnIndices;
    private final RecordColumnMetadata[] columns;
    private final RecordColumnMetadata timestampMetadata;


    public SplitRecordMetadata(RecordMetadata a, RecordMetadata b) {
        int split = a.getColumnCount();
        this.timestampMetadata = a.getTimestampMetadata();
        this.columnCount = split + b.getColumnCount();
        this.columnIndices = new CharSequenceIntHashMap(columnCount);
        this.columns = new RecordColumnMetadata[columnCount];

        for (int i = 0; i < split; i++) {
            RecordColumnMetadata rc = a.getColumn(i);
            columns[i] = rc;
            columnIndices.put(columns[i].getName(), i);
        }

        for (int i = 0, c = columnCount - split; i < c; i++) {
            columns[i + split] = b.getColumn(i);
            columnIndices.put(columns[i + split].getName(), i + split);
        }
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return columns[index];
    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return columns[getColumnIndex(name)];
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        int index = columnIndices.get(name);
        if (index == -1) {
            throw new JournalRuntimeException("Invalid column: %s", name);
        }
        return index;
    }

    @Override
    public RecordColumnMetadata getTimestampMetadata() {
        return timestampMetadata;
    }

    @Override
    public boolean invalidColumn(CharSequence name) {
        return columnIndices.get(name) == -1;
    }
}

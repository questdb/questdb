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

import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.storage.IndexCursor;
import com.nfsdb.storage.KVIndex;
import com.nfsdb.storage.VariableColumn;
import com.nfsdb.utils.Hash;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class StringKvIndexRowSource extends AbstractRowSource {

    private final String columnName;
    private final ObjHashSet<String> values;
    private KVIndex index;
    private IndexCursor indexCursor;
    private long lo;
    private long hi;
    private long rowid;
    private int valueIndex = 0;
    private CharSequence currentValue;
    private VariableColumn column;
    private int bucketCount;
    private int columnIndex;

    public StringKvIndexRowSource(String columnName, ObjHashSet<String> values) {
        this.columnName = columnName;
        this.values = values;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.columnIndex = metadata.getColumnIndex(columnName);
        this.bucketCount = metadata.getColumn(columnIndex).distinctCountHint;
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            this.column = (VariableColumn) slice.partition.getAbstractColumn(columnIndex);
            this.index = slice.partition.getIndexForColumn(columnName);

            this.indexCursor = null;
            this.lo = slice.lo - 1;
            this.hi = slice.calcHi ? slice.partition.open().size() : slice.hi + 1;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
        return this;
    }

    @Override
    public void reset() {
        indexCursor = null;
        valueIndex = 0;
    }

    @Override
    public boolean hasNext() {

        if (indexCursor != null && indexCursor.hasNext()) {
            do {
                long r = indexCursor.next();
                if (r > lo && r < hi && column.cmpStr(r, currentValue)) {
                    this.rowid = r;
                    return true;
                }
            } while (indexCursor.hasNext());
        }

        return hasNext0();
    }

    @Override
    public long next() {
        return rowid;
    }

    private boolean hasNext0() {
        while (valueIndex < values.size()) {
            CharSequence str = values.get(valueIndex++);
            indexCursor = index.fwdCursor(Hash.boundedHash(str, bucketCount));

            while (indexCursor.hasNext()) {
                long r = indexCursor.next();

                if (r > lo && r < hi && column.cmpStr(r, str)) {
                    this.rowid = r;
                    this.currentValue = str;
                    return true;
                }
            }
        }

        return false;
    }
}

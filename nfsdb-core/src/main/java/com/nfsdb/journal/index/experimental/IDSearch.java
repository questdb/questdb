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

package com.nfsdb.journal.index.experimental;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.collections.LongArrayList;
import com.nfsdb.journal.column.VariableColumn;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.index.KVIndex;
import com.nfsdb.journal.index.experimental.filter.StringEqualsFilter;
import com.nfsdb.journal.utils.Checksum;

public class IDSearch<T> {

    private final FilteredCursor cursor = new FilteredCursor();
    private final StringEqualsFilter filter = new StringEqualsFilter();
    private int hashCapacity;
    private KVIndex index;
    private VariableColumn column;

    public void createState(Partition<T> partition, final String columnName) throws JournalException {
        int colIndex = partition.getJournal().getMetadata().getColumnIndex(columnName);
        this.hashCapacity = partition.getJournal().getColumnMetadata(colIndex).meta.distinctCountHint;
        this.index = partition.getIndexForColumn(colIndex);
        this.column = (VariableColumn) partition.getAbstractColumn(colIndex);
        this.filter.setColumn(column);
    }

    public LongArrayList execute(String searchTerm) {
        KVIndex.IndexCursor cursor = index.cachedCursor(searchTerm.hashCode() % hashCapacity);
        LongArrayList arrayList = new LongArrayList();
        while (cursor.hasNext()) {
            long v = cursor.next();
            if (column.equalsString(v, searchTerm)) {
                arrayList.add((int) v);
            }
        }
        return arrayList;
    }

    public Cursor exec(final String searchTerm) {
        filter.setSearchTerm(searchTerm);
        cursor.configure(index.cachedCursor(Checksum.hash(searchTerm, hashCapacity)), filter);
        return cursor;
    }
}

/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.cst.impl.rsrc;

import com.nfsdb.collections.IntHashSet;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.lang.cst.PartitionSlice;
import com.nfsdb.lang.cst.RowCursor;
import com.nfsdb.lang.cst.RowSource;
import com.nfsdb.storage.FixedColumn;

/**
 * Takes stream of rowids, converts them to int values of FixedColumn and
 * returns rowids for non-repeated int values. Rowids returned on first in - first out basis.
 * <p/>
 * One of use cases might be streaming of journal in reverse chronological order (latest rows first)
 * via this filter to receive last records for every value of given column.
 */
public class DistinctSymbolRowSource extends AbstractRowSource {

    private final RowSource delegate;
    private final String symbol;
    private final IntHashSet set = new IntHashSet();
    private FixedColumn column;
    private int columnIndex = -1;
    private RowCursor cursor;
    private long rowid;

    public DistinctSymbolRowSource(RowSource delegate, String symbol) {
        this.delegate = delegate;
        this.symbol = symbol;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        super.configure(metadata);
        delegate.configure(metadata);
    }

    @Override
    public RowCursor cursor(PartitionSlice slice) {
        if (columnIndex == -1) {
            columnIndex = slice.partition.getJournal().getMetadata().getColumnIndex(symbol);
        }
        column = (FixedColumn) slice.partition.getAbstractColumn(columnIndex);
        cursor = delegate.cursor(slice);
        return this;
    }

    @Override
    public boolean hasNext() {
        while (cursor.hasNext()) {
            long rowid = cursor.next();
            if (set.add(column.getInt(rowid))) {
                this.rowid = rowid;
                return true;
            }
        }
        return false;
    }

    @Override
    public long next() {
        return rowid;
    }

    @Override
    public void reset() {
        columnIndex = -1;
        delegate.reset();
        set.clear();
    }
}

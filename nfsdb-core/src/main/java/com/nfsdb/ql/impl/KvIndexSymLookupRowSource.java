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

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.SymFacade;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.storage.IndexCursor;
import com.nfsdb.storage.KVIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
public class KvIndexSymLookupRowSource extends AbstractRowSource {

    private final String symbol;
    private final VirtualColumn valueFunction;
    private final boolean newCursor;
    private int columnIndex;
    private int symbolKey = -2;
    private IndexCursor indexCursor;
    private long lo;
    private long hi;
    private boolean full;
    private long rowid;
    private boolean hasNext = false;

    public KvIndexSymLookupRowSource(String symbol, VirtualColumn valueFunction) {
        this(symbol, valueFunction, false);
    }

    public KvIndexSymLookupRowSource(String symbol, VirtualColumn valueFunction, boolean newCursor) {
        this.symbol = symbol;
        this.valueFunction = valueFunction;
        this.newCursor = newCursor;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.columnIndex = metadata.getColumnIndex(symbol);
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            KVIndex index = slice.partition.getIndexForColumn(columnIndex);
            this.indexCursor = newCursor ? index.newFwdCursor(symbolKey) : index.fwdCursor(symbolKey);
            this.full = slice.lo == 0 && slice.calcHi;
            this.lo = slice.lo - 1;
            this.hi = slice.calcHi ? slice.partition.open().size() : slice.hi + 1;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
        return this;
    }

    @Override
    public void reset() {
    }

    @Override
    public boolean hasNext() {

        if (hasNext) {
            return true;
        }

        if (indexCursor != null && indexCursor.hasNext()) {
            if (full) {
                this.rowid = indexCursor.next();
                return hasNext = true;
            }

            do {
                long rowid = indexCursor.next();
                if (rowid > lo && rowid < hi) {
                    this.rowid = rowid;
                    return hasNext = true;
                }
            } while (indexCursor.hasNext());
        }

        return false;
    }

    @Override
    public long next() {
        hasNext = false;
        return rowid;
    }

    @Override
    public void prepare(SymFacade facade) {
        symbolKey = facade.getSymbolTable(symbol).getQuick(valueFunction.getFlyweightStr(null));
    }
}

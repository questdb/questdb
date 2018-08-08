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

package com.questdb.ql.latest;

import com.questdb.ql.CancellationHandler;
import com.questdb.ql.PartitionSlice;
import com.questdb.ql.RowSource;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.CharSink;
import com.questdb.store.*;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

public class KvIndexSymLookupRowSource implements RowSource, RowCursor {

    private final String symbol;
    private final String value;
    private final boolean newCursor;
    private int columnIndex;
    private int symbolKey = -2;
    private IndexCursor indexCursor;
    private long lo;
    private long hi;
    private boolean full;
    private long rowid;
    private boolean hasNext = false;

    public KvIndexSymLookupRowSource(String symbol, String value) {
        this(symbol, value, false);
    }

    public KvIndexSymLookupRowSource(String symbol, String value, boolean newCursor) {
        this.symbol = symbol;
        this.value = value;
        this.newCursor = newCursor;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.columnIndex = metadata.getColumnIndex(symbol);
    }

    @Override
    public void prepare(ReaderFactory factory, StorageFacade facade, CancellationHandler cancellationHandler) {
        symbolKey = facade.getSymbolTable(columnIndex).getQuick(value);
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
    public void toTop() {
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
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("KvIndexSymLookupRowSource").put(',');
        sink.putQuoted("symbol").put(':').putQuoted(symbol).put(',');
        sink.putQuoted("value").put(':').putQuoted(value);
        sink.put('}');
    }
}

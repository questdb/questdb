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
 ******************************************************************************/

package com.questdb.ql.impl.latest;

import com.questdb.ex.JournalException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.ReaderFactory;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.misc.Hash;
import com.questdb.ql.*;
import com.questdb.std.str.CharSink;
import com.questdb.store.IndexCursor;
import com.questdb.store.KVIndex;
import com.questdb.store.VariableColumn;

public class KvIndexStrLookupRowSource implements RowSource, RowCursor {

    private final String columnName;
    private final String value;
    private final boolean newCursor;
    private int hash;
    private IndexCursor indexCursor;
    private long lo;
    private long hi;
    private long rowid;
    private VariableColumn column;
    private int columnIndex;
    private boolean hasNext = false;

    public KvIndexStrLookupRowSource(String columnName, String value) {
        this(columnName, value, false);
    }

    public KvIndexStrLookupRowSource(String columnName, String value, boolean newCursor) {
        this.columnName = columnName;
        this.value = value;
        this.newCursor = newCursor;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.columnIndex = metadata.getColumnIndex(columnName);
        this.hash = Hash.boundedHash(value, metadata.getColumnQuick(columnIndex).distinctCountHint);
    }

    @Override
    public void prepare(ReaderFactory factory, StorageFacade storageFacade, CancellationHandler cancellationHandler) {
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            this.column = slice.partition.varCol(columnIndex);
            KVIndex index = slice.partition.getIndexForColumn(columnIndex);
            this.indexCursor = newCursor ? index.newFwdCursor(hash) : index.fwdCursor(hash);
            this.lo = slice.lo - 1;
            this.hi = slice.calcHi ? slice.partition.open().size() : slice.hi + 1;
            this.hasNext = false;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
        return this;
    }

    @Override
    public void toTop() {
        indexCursor = null;
        hasNext = false;
    }

    @Override
    public boolean hasNext() {

        if (hasNext) {
            return true;
        }

        if (indexCursor != null) {
            while (indexCursor.hasNext()) {
                long r = indexCursor.next();
                if (r > lo && r < hi && column.cmpStr(r, value)) {
                    this.rowid = r;
                    return hasNext = true;
                }
            }
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
        sink.putQuoted("op").put(':').putQuoted("KvIndexStrLookupRowSource").put(',');
        sink.putQuoted("column").put(':').putQuoted(columnName).put(',');
        sink.putQuoted("value").put(':').putQuoted(value);
        sink.put('}');
    }
}

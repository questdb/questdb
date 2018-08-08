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

public class KvIndexLongLookupRowSource implements RowSource, RowCursor {

    private final String columnName;
    private final boolean newCursor;
    private final long value;
    private int columnIndex;
    private int key = -2;
    private IndexCursor indexCursor;
    private long lo;
    private long hi;
    private long rowid;
    private boolean hasNext = false;
    private FixedColumn column;

    public KvIndexLongLookupRowSource(String columnName, long value) {
        this(columnName, value, false);
    }

    public KvIndexLongLookupRowSource(String columnName, long value, boolean newCursor) {
        this.columnName = columnName;
        this.newCursor = newCursor;
        this.value = value;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.columnIndex = metadata.getColumnIndex(columnName);
        this.key = (int) (value & metadata.getColumnQuick(columnIndex).distinctCountHint);
    }

    @Override
    public void prepare(ReaderFactory factory, StorageFacade storageFacade, CancellationHandler cancellationHandler) {

    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            column = slice.partition.fixCol(columnIndex);
            KVIndex index = slice.partition.getIndexForColumn(columnIndex);
            this.indexCursor = newCursor ? index.newFwdCursor(key) : index.fwdCursor(key);
            this.lo = slice.lo - 1;
            this.hi = slice.calcHi ? slice.partition.open().size() : slice.hi + 1;
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
                if (r > lo && r < hi && column.getLong(r) == value) {
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
        sink.putQuoted("op").put(':').putQuoted("KvIndexIntLookupRowSource").put(',');
        sink.putQuoted("column").put(':').putQuoted(columnName);
        sink.putQuoted("value").put(':').put(value);
        sink.put('}');
    }
}

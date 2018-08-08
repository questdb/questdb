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
import com.questdb.ql.JournalRecord;
import com.questdb.ql.PartitionSlice;
import com.questdb.ql.RowSource;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.Hash;
import com.questdb.std.LongList;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.CharSink;
import com.questdb.store.*;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

public class KvIndexStrListHeadRowSource implements RowSource, RowCursor {

    private final String column;
    private final VirtualColumn filter;
    private final CharSequenceHashSet values;
    private final LongList rows = new LongList();
    private final JournalRecord rec = new JournalRecord();
    private int keyIndex;
    private int buckets;
    private int columnIndex;

    public KvIndexStrListHeadRowSource(String column, CharSequenceHashSet values, VirtualColumn filter) {
        this.column = column;
        this.values = values;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.columnIndex = metadata.getColumnIndex(column);
        this.buckets = metadata.getColumnQuick(columnIndex).distinctCountHint;
    }

    @Override
    public void prepare(ReaderFactory factory, StorageFacade storageFacade, CancellationHandler cancellationHandler) {
        if (filter != null) {
            filter.prepare(storageFacade);
        }
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            Partition partition = rec.partition = slice.partition.open();
            KVIndex index = partition.getIndexForColumn(columnIndex);
            VariableColumn col = partition.varCol(columnIndex);

            long lo = slice.lo - 1;
            long hi = slice.calcHi ? partition.size() : slice.hi + 1;
            rows.clear();

            for (int i = 0, n = values.size(); i < n; i++) {
                IndexCursor c = index.cursor(Hash.boundedHash(values.get(i), buckets));
                while (c.hasNext()) {
                    long r = rec.rowid = c.next();
                    if (r > lo && r < hi && col.cmpStr(r, values.get(i)) && (filter == null || filter.getBool(rec))) {
                        rows.add(r);
                        break;
                    }
                }
            }
            rows.sort();
            keyIndex = 0;
            return this;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void toTop() {
    }

    @Override
    public boolean hasNext() {
        return keyIndex < rows.size();
    }

    @Override
    public long next() {
        return rec.rowid = rows.getQuick(keyIndex++);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("KvIndexStrListHeadRowSource").put(',');
        sink.putQuoted("column").put(':').put(column);
        sink.put('}');
    }
}

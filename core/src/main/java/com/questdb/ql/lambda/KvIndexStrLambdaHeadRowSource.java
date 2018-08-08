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

package com.questdb.ql.lambda;

import com.questdb.ql.*;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.*;
import com.questdb.std.ex.JournalException;
import com.questdb.store.*;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

abstract class KvIndexStrLambdaHeadRowSource implements RowSource, RowCursor {
    protected final String column;
    protected final RecordSource recordSource;
    protected final int recordSourceColumn;
    private final VirtualColumn filter;
    private final LongList rows = new LongList();
    private final CharSequenceHashSet keys = new CharSequenceHashSet();
    private final IntList hashes = new IntList();
    private final JournalRecord rec = new JournalRecord();
    private int cursor;
    private int buckets;
    private int columnIndex;

    KvIndexStrLambdaHeadRowSource(String column, RecordSource recordSource, int recordSourceColumn, VirtualColumn filter) {
        this.column = column;
        this.recordSource = recordSource;
        this.recordSourceColumn = recordSourceColumn;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.columnIndex = metadata.getColumnIndex(column);
        this.buckets = metadata.getColumnQuick(columnIndex).distinctCountHint;
    }

    @Override
    public void prepare(ReaderFactory factory, StorageFacade fa, CancellationHandler cancellationHandler) {
        if (filter != null) {
            filter.prepare(fa);
        }

        keys.clear();
        hashes.clear();
        RecordCursor cursor = recordSource.prepareCursor(factory, cancellationHandler);
        try {
            for (Record r : cursor) {
                CharSequence s = getKey(r, recordSourceColumn);
                if (keys.add(s)) {
                    hashes.add(Hash.boundedHash(s, buckets));
                }
            }
        } finally {
            cursor.releaseCursor();
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

            for (int i = 0, n = hashes.size(); i < n; i++) {
                IndexCursor c = index.cursor(hashes.getQuick(i));
                while (c.hasNext()) {
                    long r = rec.rowid = c.next();
                    if (r > lo && r < hi && Chars.equals(col.getFlyweightStr(r), keys.get(i)) && (filter == null || filter.getBool(rec))) {
                        rows.add(r);
                        break;
                    }
                }
            }
            rows.sort();
            cursor = 0;
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
        return cursor < rows.size();
    }

    @Override
    public long next() {
        return rec.rowid = rows.getQuick(cursor++);
    }

    protected abstract CharSequence getKey(Record r, int col);
}

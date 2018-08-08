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
import com.questdb.std.IntHashSet;
import com.questdb.std.LongList;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.CharSink;
import com.questdb.store.*;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

public class KvIndexIntLambdaHeadRowSource implements RowSource, RowCursor {

    public static final LatestByLambdaRowSourceFactory FACTORY = new Factory();
    private final String column;
    private final VirtualColumn filter;
    private final RecordSource recordSource;
    private final int recordSourceColumn;
    private final LongList rows = new LongList();
    private final IntHashSet keys = new IntHashSet();
    private final JournalRecord rec = new JournalRecord();
    private int cursor;
    private int buckets;
    private int columnIndex;

    private KvIndexIntLambdaHeadRowSource(String column, RecordSource recordSource, int recordSourceColumn, VirtualColumn filter) {
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
        RecordCursor cursor = recordSource.prepareCursor(factory, cancellationHandler);
        try {
            for (Record r : cursor) {
                keys.add(r.getInt(recordSourceColumn));
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
            FixedColumn col = partition.fixCol(columnIndex);

            long lo = slice.lo - 1;
            long hi = slice.calcHi ? partition.size() : slice.hi + 1;
            rows.clear();

            for (int i = 0, n = keys.size(); i < n; i++) {
                IndexCursor c = index.cursor(keys.get(i) & buckets);
                while (c.hasNext()) {
                    long r = rec.rowid = c.next();
                    if (r > lo && r < hi && col.getInt(r) == keys.get(i) && (filter == null || filter.getBool(rec))) {
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

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("KvIndexIntLambdaHeadRowSource").put(',');
        sink.putQuoted("column").put(':').putQuoted(column).put(',');
        sink.putQuoted("src").put(':').put(recordSource);
        sink.put('}');
    }

    private static class Factory implements LatestByLambdaRowSourceFactory {
        @Override
        public RowSource newInstance(String column, RecordSource recordSource, int recordSourceColumn, VirtualColumn filter) {
            return new KvIndexIntLambdaHeadRowSource(column, recordSource, recordSourceColumn, filter);
        }
    }
}

/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql.impl.lambda;

import com.nfsdb.Partition;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.*;
import com.nfsdb.ql.impl.AbstractRowSource;
import com.nfsdb.ql.impl.JournalRecord;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.IntHashSet;
import com.nfsdb.std.LongList;
import com.nfsdb.store.FixedColumn;
import com.nfsdb.store.IndexCursor;
import com.nfsdb.store.KVIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class KvIndexIntLambdaHeadRowSource extends AbstractRowSource {

    public static final LatestByLambdaRowSourceFactory FACTORY = new Factory();
    private final String column;
    private final VirtualColumn filter;
    private final RecordSource<? extends Record> recordSource;
    private final int recordSourceColumn;
    private final LongList rows = new LongList();
    private final IntHashSet keys = new IntHashSet();
    private JournalRecord rec;
    private int cursor;
    private int buckets;
    private int columnIndex;

    private KvIndexIntLambdaHeadRowSource(String column, RecordSource<? extends Record> recordSource, int recordSourceColumn, VirtualColumn filter) {
        this.column = column;
        this.recordSource = recordSource;
        this.recordSourceColumn = recordSourceColumn;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.rec = new JournalRecord(metadata);
        this.columnIndex = metadata.getColumnIndex(column);
        this.buckets = metadata.getColumnQuick(columnIndex).distinctCountHint;
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
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
    public void reset() {
    }

    @Override
    public boolean hasNext() {
        return cursor < rows.size();
    }

    @Override
    public long next() {
        return rec.rowid = rows.getQuick(cursor++);
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CHECKED")
    @Override
    public void prepare(StorageFacade fa) {
        if (filter != null) {
            filter.prepare(fa);
        }

        keys.clear();
        try {
            for (Record r : recordSource.prepareCursor(fa.getFactory())) {
                keys.add(r.getInt(recordSourceColumn));
            }
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }

    }

    public static class Factory implements LatestByLambdaRowSourceFactory {
        @Override
        public RowSource newInstance(String column, RecordSource<? extends Record> recordSource, int recordSourceColumn, VirtualColumn filter) {
            return new KvIndexIntLambdaHeadRowSource(column, recordSource, recordSourceColumn, filter);
        }
    }
}

/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
import com.nfsdb.store.IndexCursor;
import com.nfsdb.store.KVIndex;
import com.nfsdb.store.SymbolTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class KvIndexSymLambdaHeadRowSource extends AbstractRowSource {
    private final String column;
    private final VirtualColumn filter;
    private final RecordSource<? extends Record> recordSource;
    private final int recordSourceColumn;
    private final IntHashSet keys = new IntHashSet();
    private final LongList rows = new LongList();
    private JournalRecord rec;
    private int cursor;

    KvIndexSymLambdaHeadRowSource(String column, RecordSource<? extends Record> recordSource, int recordSourceColumn, VirtualColumn filter) {
        this.column = column;
        this.recordSource = recordSource;
        this.recordSourceColumn = recordSourceColumn;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.rec = new JournalRecord(metadata);
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            Partition partition = rec.partition = slice.partition.open();
            KVIndex index = partition.getIndexForColumn(column);
            long lo = slice.lo - 1;
            long hi = slice.calcHi ? partition.size() : slice.hi + 1;
            rows.clear();

            for (int i = 0, n = keys.size(); i < n; i++) {
                IndexCursor c = index.cursor(keys.get(i));
                while (c.hasNext()) {
                    long r = rec.rowid = c.next();
                    if (r > lo && r < hi && (filter == null || filter.getBool(rec))) {
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

    @SuppressFBWarnings("ACEM_ABSTRACT_CLASS_EMPTY_METHODS")
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

        SymbolTable tab = fa.getSymbolTable(column);
        keys.clear();
        try {
            for (Record r : recordSource.prepareCursor(fa.getFactory())) {
                int k = tab.getQuick(getKey(r, recordSourceColumn));
                if (k > -1) {
                    keys.add(k);
                }
            }
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    protected abstract CharSequence getKey(Record r, int col);
}

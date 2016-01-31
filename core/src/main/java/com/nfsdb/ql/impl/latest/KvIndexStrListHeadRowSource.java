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

package com.nfsdb.ql.impl.latest;

import com.nfsdb.Partition;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.misc.Hash;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.AbstractRowSource;
import com.nfsdb.ql.impl.JournalRecord;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.CharSequenceHashSet;
import com.nfsdb.std.LongList;
import com.nfsdb.store.IndexCursor;
import com.nfsdb.store.KVIndex;
import com.nfsdb.store.VariableColumn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class KvIndexStrListHeadRowSource extends AbstractRowSource {

    private final String column;
    private final VirtualColumn filter;
    private final CharSequenceHashSet values;
    private final LongList rows = new LongList();
    private JournalRecord rec;
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
    public void reset() {
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
    public void prepare(StorageFacade storageFacade) {
        if (filter != null) {
            filter.prepare(storageFacade);
        }
    }
}

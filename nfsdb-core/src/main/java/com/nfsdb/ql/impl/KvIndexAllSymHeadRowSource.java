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

import com.nfsdb.Partition;
import com.nfsdb.collections.LongList;
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

public class KvIndexAllSymHeadRowSource extends AbstractRowSource {

    private final String column;
    private final VirtualColumn filter;
    private final LongList rows = new LongList();
    private JournalRecord rec;
    private int keyIndex;
    private int valueCount;
    private int columnIndex;

    public KvIndexAllSymHeadRowSource(String column, VirtualColumn filter) {
        this.column = column;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.rec = new JournalRecord(metadata);
        this.columnIndex = metadata.getColumnIndex(column);
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            Partition partition = rec.partition = slice.partition.open();
            KVIndex index = partition.getIndexForColumn(columnIndex);
            long lo = slice.lo - 1;
            long hi = slice.calcHi ? partition.size() : slice.hi + 1;
            rows.clear();

            for (int i = 0, n = valueCount; i < n; i++) {
                IndexCursor c = index.cursor(i);
                long r = -1;
                boolean found = false;
                while (c.hasNext()) {
                    r = rec.rowid = c.next();
                    if (r > lo && r < hi && (filter == null || filter.getBool(rec))) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    rows.add(r);
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
        return rec.rowid = rows.get(keyIndex++);
    }

    @Override
    public void prepare(SymFacade symFacade) {
        valueCount = symFacade.getSymbolTable(column).size();
    }
}

/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb.ql.impl;

import com.nfsdb.Partition;
import com.nfsdb.collections.IntList;
import com.nfsdb.collections.LongList;
import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.SymFacade;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.storage.IndexCursor;
import com.nfsdb.storage.KVIndex;
import com.nfsdb.storage.SymbolTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class KvIndexSymListHeadRowSource extends AbstractRowSource {

    private final String column;
    private final VirtualColumn filter;
    private final ObjHashSet<String> values;
    private final IntList keys = new IntList();
    private final LongList rows = new LongList();
    private JournalRecord rec;
    private int keyIndex;

    public KvIndexSymListHeadRowSource(String column, ObjHashSet<String> values, VirtualColumn filter) {
        this.column = column;
        this.values = values;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.rec = new JournalRecord(metadata);
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
    public void prepare(SymFacade fa) {
        SymbolTable tab = fa.getSymbolTable(column);
        keys.clear();

        for (int i = 0, n = values.size(); i < n; i++) {
            int k = tab.getQuick(values.get(i));
            if (k > -1) {
                keys.add(k);
            }
        }
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
                IndexCursor c = index.cursor(keys.getQuick(i));
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

}

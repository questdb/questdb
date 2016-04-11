/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.latest;

import com.nfsdb.Partition;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.AbstractRowSource;
import com.nfsdb.ql.impl.JournalRecord;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.IntHashSet;
import com.nfsdb.std.LongList;
import com.nfsdb.store.FixedColumn;
import com.nfsdb.store.IndexCursor;
import com.nfsdb.store.KVIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class KvIndexIntListHeadRowSource extends AbstractRowSource {

    private final String column;
    private final VirtualColumn filter;
    private final IntHashSet values;
    private final LongList rows = new LongList();
    private JournalRecord rec;
    private int keyIndex;
    private int buckets;
    private int columnIndex;

    public KvIndexIntListHeadRowSource(String column, IntHashSet values, VirtualColumn filter) {
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
            FixedColumn col = partition.fixCol(columnIndex);

            long lo = slice.lo - 1;
            long hi = slice.calcHi ? partition.size() : slice.hi + 1;
            rows.clear();

            for (int i = 0, n = values.size(); i < n; i++) {
                IndexCursor c = index.cursor(values.get(i) & buckets);
                long r = -1;
                boolean found = false;
                while (c.hasNext()) {
                    r = rec.rowid = c.next();
                    if (r > lo && r < hi && col.getInt(r) == values.get(i) && (filter == null || filter.getBool(rec))) {
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
        return rec.rowid = rows.getQuick(keyIndex++);
    }

    @Override
    public void prepare(StorageFacade storageFacade) {
        if (filter != null) {
            filter.prepare(storageFacade);
        }
    }
}

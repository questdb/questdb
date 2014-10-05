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

package com.nfsdb.journal.lang.cst.impl.dfrn;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JournalSource;
import com.nfsdb.journal.lang.cst.RowCursor;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;
import com.nfsdb.journal.utils.Rows;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;

public class MapHeadDataFrameSource implements DataFrameSource, DataFrame, RowCursor {

    private final JournalSource source;
    private final StringRef symbol;
    private final TIntObjectHashMap<TLongArrayList> frame = new TIntObjectHashMap<>();
    private TLongArrayList series;
    private int seriesPos;

    public MapHeadDataFrameSource(JournalSource source, StringRef symbol) {
        this.source = source;
        this.symbol = symbol;
    }

    @Override
    public DataFrame getFrame() {
        int columnIndex = source.getJournal().getMetadata().getColumnIndex(symbol.value);
        FixedColumn column = null;
        Partition p = null;
        int pIndex = 0;
        for (DataItem d : source) {
            if (p != d.partition) {
                column = (FixedColumn) d.partition.getAbstractColumn(columnIndex);
                p = d.partition;
                pIndex = d.partition.getPartitionIndex();
            }

            assert column != null;

            int key;
            TLongArrayList series = frame.get(key = column.getInt(d.rowid));
            if (series == null) {
                series = new TLongArrayList();
                frame.put(key, series);
            }

            series.add(Rows.toRowID(pIndex, d.rowid));
        }

        return this;
    }

    public void reset() {
        TIntObjectIterator<TLongArrayList> it = frame.iterator();
        while (it.hasNext()) {
            TLongArrayList l = it.value();
            l.resetQuick();
            it.advance();
        }
    }

    @Override
    public RowCursor cursor(int key) {
        series = frame.get(key);
        seriesPos = 0;
        return this;
    }

    @Override
    public boolean hasNext() {
        return series != null && seriesPos < series.size();
    }

    @Override
    public long next() {
        return series.getQuick(seriesPos++);
    }

    @Override
    public Journal getJournal() {
        return source.getJournal();
    }
}

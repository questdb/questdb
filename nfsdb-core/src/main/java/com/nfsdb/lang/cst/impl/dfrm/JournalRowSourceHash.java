/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.cst.impl.dfrm;

import com.nfsdb.Journal;
import com.nfsdb.collections.DirectLongList;
import com.nfsdb.collections.IntObjHashMap;
import com.nfsdb.lang.cst.RowCursor;
import com.nfsdb.lang.cst.impl.qry.JournalRecord;
import com.nfsdb.lang.cst.impl.qry.JournalRecordSource;
import com.nfsdb.lang.cst.impl.qry.RecordMetadata;
import com.nfsdb.lang.cst.impl.ref.StringRef;
import com.nfsdb.utils.Rows;

public class JournalRowSourceHash implements RowCursor {

    private final JournalRecordSource source;
    private final IntObjHashMap<DirectLongList> map = new IntObjHashMap<>();
    private DirectLongList series;
    private int seriesPos;

    public JournalRowSourceHash(JournalRecordSource source, StringRef symbol) {
        this.source = source;
        int columnIndex = source.getMetadata().getColumnIndex(symbol.value);
        for (JournalRecord d : source) {
            int key = d.get(columnIndex);
            DirectLongList series = map.get(key);
            if (series == null) {
                series = new DirectLongList();
                map.put(key, series);
            }
            series.add(Rows.toRowID(d.partition.getPartitionIndex(), d.rowid));
        }
    }

    public void reset() {
        for (DirectLongList list : map.values()) {
            list.reset();
        }
    }

    public RowCursor cursor(int key) {
        series = map.get(key);
        seriesPos = 0;
        return this;
    }

    @Override
    public boolean hasNext() {
        return series != null && seriesPos < series.size();
    }

    @Override
    public long next() {
        return series.get(seriesPos++);
    }

    public RecordMetadata getMetadata() {
        return source.getMetadata();
    }

    public Journal getJournal() {
        return source.getJournal();
    }
}

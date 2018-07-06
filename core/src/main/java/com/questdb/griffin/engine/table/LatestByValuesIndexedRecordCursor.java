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

package com.questdb.griffin.engine.table;

import com.questdb.cairo.BitmapIndexReader;
import com.questdb.cairo.sql.DataFrame;
import com.questdb.cairo.sql.DataFrameCursor;
import com.questdb.cairo.sql.Record;
import com.questdb.common.RowCursor;
import com.questdb.griffin.engine.LongTreeSet;
import com.questdb.std.IntHashSet;
import com.questdb.std.Rows;

class LatestByValuesIndexedRecordCursor extends AbstractDataFrameRecordCursor {

    private final int columnIndex;
    private final IntHashSet found = new IntHashSet();
    private final IntHashSet symbolKeys;
    private LongTreeSet treeSet;
    private LongTreeSet.TreeCursor treeCursor;

    public LatestByValuesIndexedRecordCursor(int columnIndex, LongTreeSet treeSet, IntHashSet symbolKeys) {
        this.columnIndex = columnIndex;
        this.treeSet = treeSet;
        this.symbolKeys = symbolKeys;
    }

    @Override
    public void close() {
        super.close();
        treeCursor = null;
    }

    @Override
    public boolean hasNext() {
        return treeCursor.hasNext();
    }

    @Override
    public Record next() {
        long row = treeCursor.next();
        record.jumpTo(Rows.toPartitionIndex(row), Rows.toLocalRowID(row));
        return record;
    }

    @Override
    public void toTop() {
        treeCursor.toTop();
    }

    private void buildTreeMap(int keyCount) {
        found.clear();
        treeSet.clear();
        while (this.dataFrameCursor.hasNext() && found.size() < keyCount) {
            final DataFrame frame = this.dataFrameCursor.next();
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            for (int i = 0, n = symbolKeys.size(); i < n; i++) {
                int symbolKey = symbolKeys.get(i);
                int index = found.keyIndex(symbolKey);
                if (index > -1) {
                    RowCursor cursor = indexReader.getCursor(false, symbolKey, rowLo, rowHi);
                    if (cursor.hasNext()) {
                        treeSet.put(Rows.toRowID(frame.getPartitionIndex(), cursor.next()));
                        found.addAt(index, symbolKey);
                    }
                }
            }
        }

        this.treeCursor = treeSet.getCursor();
    }

    void of(DataFrameCursor dataFrameCursor) {
        this.dataFrameCursor = dataFrameCursor;
        this.record.of(dataFrameCursor.getTableReader());
        buildTreeMap(symbolKeys.size());
    }
}

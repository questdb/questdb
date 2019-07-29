/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.RowCursor;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.std.IntHashSet;
import com.questdb.std.Rows;

class LatestByValuesIndexedFilteredRecordCursor extends AbstractTreeSetRecordCursor {

    private final int columnIndex;
    private final IntHashSet found = new IntHashSet();
    private final IntHashSet symbolKeys;
    private final Function filter;

    public LatestByValuesIndexedFilteredRecordCursor(
            int columnIndex,
            LongTreeSet treeSet,
            IntHashSet symbolKeys,
            Function filter) {
        super(treeSet);
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.filter = filter;
    }

    @Override
    public void toTop() {
        super.toTop();
        filter.toTop();
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) {
        final int keyCount = symbolKeys.size();
        found.clear();
        while (this.dataFrameCursor.hasNext() && found.size() < keyCount) {
            final DataFrame frame = this.dataFrameCursor.next();
            final int partitionIndex = frame.getPartitionIndex();
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;
            this.record.jumpTo(partitionIndex, 0);

            for (int i = 0, n = symbolKeys.size(); i < n; i++) {
                int symbolKey = symbolKeys.get(i);
                int index = found.keyIndex(symbolKey);
                if (index > -1) {
                    RowCursor cursor = indexReader.getCursor(false, symbolKey, rowLo, rowHi);
                    while (cursor.hasNext()) {
                        final long row = cursor.next();
                        record.setRecordIndex(row);
                        if (filter.getBool(record)) {
                            treeSet.put(Rows.toRowID(partitionIndex, row));
                            found.addAt(index, symbolKey);
                        }
                    }
                }
            }
        }
    }
}

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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Rows;

class LatestByValuesIndexedRecordCursor extends AbstractDataFrameRecordCursor {

    private final int columnIndex;
    private final IntHashSet found = new IntHashSet();
    private final IntHashSet symbolKeys;
    private final DirectLongList rows;
    private long index = 0;

    public LatestByValuesIndexedRecordCursor(int columnIndex, IntHashSet symbolKeys, DirectLongList rows) {
        this.rows = rows;
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
    }

    @Override
    public void toTop() {
        index = rows.size() - 1;
    }

    protected void buildTreeMap() {
        final int keyCount = symbolKeys.size();
        found.clear();
        rows.setPos(0);
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
                        final long row = Rows.toRowID(frame.getPartitionIndex(), cursor.next());
                        rows.add(row);
                        found.addAt(index, symbolKey);
                    }
                }
            }
        }
        index = rows.size() - 1;
    }

    void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) {
        this.dataFrameCursor = dataFrameCursor;
        this.record.of(dataFrameCursor.getTableReader());
        buildTreeMap();
    }

    @Override
    public boolean hasNext() {
        if (index > -1) {
            final long rowid = rows.get(index);
            record.jumpTo(Rows.toPartitionIndex(rowid), Rows.toLocalRowID(rowid));
            index--;
            return true;
        }
        return false;
    }

    @Override
    public long size() {
        return rows.size();
    }
}

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

import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.DataFrame;
import com.questdb.cairo.sql.Function;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.std.IntHashSet;
import com.questdb.std.IntIntHashMap;
import com.questdb.std.Numbers;
import com.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

class LatestByValuesFilteredRecordCursor extends AbstractTreeSetRecordCursor {

    private final int columnIndex;
    private final IntIntHashMap map;
    private final IntHashSet symbolKeys;
    private final Function filter;

    public LatestByValuesFilteredRecordCursor(
            int columnIndex,
            @NotNull LongTreeSet treeSet,
            @NotNull IntHashSet symbolKeys,
            @NotNull Function filter) {
        super(treeSet);
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.map = new IntIntHashMap(Numbers.ceilPow2(symbolKeys.size()));
        this.filter = filter;
    }

    @Override
    public void toTop() {
        super.toTop();
        filter.toTop();
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) {
        prepare();

        while (this.dataFrameCursor.hasNext()) {
            final DataFrame frame = this.dataFrameCursor.next();
            final int partitionIndex = frame.getPartitionIndex();
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            record.jumpTo(frame.getPartitionIndex(), rowHi);
            for (long row = rowHi; row >= rowLo; row--) {
                record.setRecordIndex(row);
                if (filter.getBool(record)) {
                    int key = TableUtils.toIndexKey(record.getInt(columnIndex));
                    int index = map.keyIndex(key);
                    if (index < 0 && map.valueAt(index) == 0) {
                        treeSet.put(Rows.toRowID(partitionIndex, row));
                        map.putAt(index, key, 1);
                    }
                }
            }
        }
    }

    private void prepare() {
        for (int i = 0, n = symbolKeys.size(); i < n; i++) {
            map.put(symbolKeys.get(i), 0);
        }
    }
}

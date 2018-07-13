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

import com.questdb.cairo.sql.DataFrame;
import com.questdb.griffin.engine.LongTreeSet;
import com.questdb.std.*;

class LatestByValuesRecordCursor extends AbstractTreeSetRecordCursor {

    private final int columnIndex;
    private final IntIntHashMap map;
    private final IntHashSet symbolKeys;

    public LatestByValuesRecordCursor(int columnIndex, LongTreeSet treeSet, IntHashSet symbolKeys) {
        super(treeSet);
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.map = new IntIntHashMap(Numbers.ceilPow2(symbolKeys.size()));
    }

    protected void buildTreeMap() {
        prepare();

        while (this.dataFrameCursor.hasNext()) {
            final DataFrame frame = this.dataFrameCursor.next();
            final int partitionIndex = frame.getPartitionIndex();
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            record.jumpTo(frame.getPartitionIndex(), rowHi);
            for (long row = rowHi; row >= rowLo; row--) {
                record.setRecordIndex(row);
                int key = record.getInt(columnIndex);
                int index = map.keyIndex(key);
                if (index < 0) {
                    if (map.valueAt(index) == 0) {
                        treeSet.put(Rows.toRowID(partitionIndex, row));
                        map.putAt(index, key, 1);
                    }
                }
            }
        }
    }

    private void prepare() {
        final int keys[] = symbolKeys.getKeys();
        final int noEntryValue = symbolKeys.getNoEntryValue();
        for (int i = 0, n = keys.length; i < n; i++) {
            int key = Unsafe.arrayGet(keys, i);
            if (key != noEntryValue) {
                map.put(key, 0);
            }
        }
    }
}

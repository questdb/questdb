/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
 *
 ******************************************************************************/

package io.questdb.griffin.engine.table;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;

class LatestByValuesRecordCursor extends AbstractRecordListCursor {

    private final int columnIndex;
    private final IntIntHashMap map;
    private final IntHashSet symbolKeys;

    public LatestByValuesRecordCursor(int columnIndex, DirectLongList rows, IntHashSet symbolKeys) {
        super(rows);
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.map = new IntIntHashMap(Numbers.ceilPow2(symbolKeys.size()));
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) {
        prepare();

        while (this.dataFrameCursor.hasNext()) {
            final DataFrame frame = this.dataFrameCursor.next();
            final int partitionIndex = frame.getPartitionIndex();
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            recordA.jumpTo(frame.getPartitionIndex(), rowHi);
            for (long row = rowHi; row >= rowLo; row--) {
                recordA.setRecordIndex(row);
                int key = TableUtils.toIndexKey(recordA.getInt(columnIndex));
                int index = map.keyIndex(key);
                if (index < 0 && map.valueAt(index) == 0) {
                    rows.add(Rows.toRowID(partitionIndex, row));
                    map.putAt(index, key, 1);
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

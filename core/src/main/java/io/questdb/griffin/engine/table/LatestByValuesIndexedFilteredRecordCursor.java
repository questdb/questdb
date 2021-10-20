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

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

class LatestByValuesIndexedFilteredRecordCursor extends AbstractRecordListCursor {

    private final int columnIndex;
    private final IntHashSet found = new IntHashSet();
    private final IntHashSet symbolKeys;
    private final Function filter;

    public LatestByValuesIndexedFilteredRecordCursor(
            int columnIndex,
            DirectLongList rows,
            IntHashSet symbolKeys,
            Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(rows, columnIndexes);
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
        DataFrame frame;
        while ((frame = this.dataFrameCursor.next()) != null && found.size() < keyCount) {
            final int partitionIndex = frame.getPartitionIndex();
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;
            this.recordA.jumpTo(partitionIndex, 0);

            for (int i = 0, n = symbolKeys.size(); i < n; i++) {
                int symbolKey = symbolKeys.get(i);
                int index = found.keyIndex(symbolKey);
                if (index > -1) {
                    RowCursor cursor = indexReader.getCursor(false, symbolKey, rowLo, rowHi);
                    if (cursor.hasNext()) {
                        final long row = cursor.next();
                        recordA.setRecordIndex(row);
                        if (filter.getBool(recordA)) {
                            rows.add(Rows.toRowID(partitionIndex, row));
                            found.addAt(index, symbolKey);
                        }
                    }
                }
            }
        }
        rows.sortAsUnsigned();
    }
}

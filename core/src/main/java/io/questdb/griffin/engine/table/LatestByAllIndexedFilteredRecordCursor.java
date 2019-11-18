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
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

class LatestByAllIndexedFilteredRecordCursor extends AbstractRecordListCursor {

    private final int columnIndex;
    private final IntHashSet found = new IntHashSet();
    private final Function filter;

    public LatestByAllIndexedFilteredRecordCursor(
            int columnIndex,
            @NotNull DirectLongList rows,
            @NotNull Function filter
    ) {
        super(rows);
        this.columnIndex = columnIndex;
        this.filter = filter;
    }

    @Override
    public void close() {
        filter.close();
        super.close();
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) {
        found.clear();
        filter.init(this, executionContext);

        final int keyCount = dataFrameCursor.getTableReader().getSymbolMapReader(columnIndex).size() + 1;
        int keyLo = 0;
        int keyHi = keyCount;

        int localLo = Integer.MAX_VALUE;
        int localHi = Integer.MIN_VALUE;

        while (this.dataFrameCursor.hasNext() && found.size() < keyCount) {
            final DataFrame frame = this.dataFrameCursor.next();
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;
            final int partitionIndex = frame.getPartitionIndex();
            record.jumpTo(partitionIndex, 0);

            for (int i = keyLo; i < keyHi; i++) {
                int index = found.keyIndex(i);
                if (index > -1) {
                    RowCursor cursor = indexReader.getCursor(false, i, rowLo, rowHi);
                    if (cursor.hasNext()) {
                        long row = cursor.next();
                        record.setRecordIndex(row);
                        if (filter.getBool(record)) {
                            rows.add(Rows.toRowID(partitionIndex, row));
                            found.addAt(index, i);
                        }
                    } else {
                        // adjust range
                        if (i < localLo) {
                            localLo = i;
                        }

                        if (i > localHi) {
                            localHi = i;
                        }
                    }
                }
            }

            keyLo = localLo;
            keyHi = localHi + 1;
            localLo = Integer.MAX_VALUE;
            localHi = Integer.MIN_VALUE;
        }
    }
}

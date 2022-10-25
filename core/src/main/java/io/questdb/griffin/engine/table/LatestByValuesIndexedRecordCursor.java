/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByValuesIndexedRecordCursor extends AbstractDataFrameRecordCursor {

    private final int columnIndex;
    private final IntHashSet found = new IntHashSet();
    private final IntHashSet symbolKeys;
    private final IntHashSet deferredSymbolKeys;
    private final DirectLongList rows;
    private long index = 0;

    public LatestByValuesIndexedRecordCursor(
            int columnIndex,
            @NotNull IntHashSet symbolKeys,
            @Nullable IntHashSet deferredSymbolKeys,
            DirectLongList rows,
            @NotNull IntList columnIndexes
    ) {
        super(columnIndexes);
        this.rows = rows;
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.deferredSymbolKeys = deferredSymbolKeys;
    }

    @Override
    public void toTop() {
        index = rows.size() - 1;
    }

    protected void buildTreeMap(SqlExecutionContext executionContext) {
        SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        int keyCount = symbolKeys.size();
        if (deferredSymbolKeys != null) {
            keyCount += deferredSymbolKeys.size();
        }
        found.clear();
        rows.setPos(0);
        DataFrame frame;
        // frame metadata is based on TableReader, which is "full" metadata
        // this cursor works with subset of columns, which warrants column index remap
        int frameColumnIndex = columnIndexes.getQuick(columnIndex);
        while ((frame = this.dataFrameCursor.next()) != null && found.size() < keyCount) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(frameColumnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            for (int i = 0, n = symbolKeys.size(); i < n; i++) {
                int symbolKey = symbolKeys.get(i);
                addFoundKey(symbolKey, indexReader, frame, rowLo, rowHi);
            }
            if (deferredSymbolKeys != null) {
                for (int i = 0, n = deferredSymbolKeys.size(); i < n; i++) {
                    int symbolKey = deferredSymbolKeys.get(i);
                    if (!symbolKeys.contains(symbolKey)) {
                        addFoundKey(symbolKey, indexReader, frame, rowLo, rowHi);
                    }
                }
            }
        }
        index = rows.size() - 1;
    }

    private void addFoundKey(int symbolKey, BitmapIndexReader indexReader, DataFrame frame, long rowLo, long rowHi) {
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

    void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) {
        this.dataFrameCursor = dataFrameCursor;
        this.recordA.of(dataFrameCursor.getTableReader());
        this.recordB.of(dataFrameCursor.getTableReader());
        buildTreeMap(executionContext);
    }

    @Override
    public boolean hasNext() {
        if (index > -1) {
            final long rowid = rows.get(index);
            recordA.jumpTo(Rows.toPartitionIndex(rowid), Rows.toLocalRowID(rowid));
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

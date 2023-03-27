/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByValuesIndexedFilteredRecordCursor extends AbstractDataFrameRecordCursor {

    private final int columnIndex;
    private final IntHashSet deferredSymbolKeys;
    private final Function filter;
    private final IntHashSet found = new IntHashSet();
    private final DirectLongList rows;
    private final IntHashSet symbolKeys;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long index;
    private boolean isTreeMapBuilt;
    private int keyCount;
    private long lim;

    public LatestByValuesIndexedFilteredRecordCursor(
            int columnIndex,
            DirectLongList rows,
            @NotNull IntHashSet symbolKeys,
            @Nullable IntHashSet deferredSymbolKeys,
            Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(columnIndexes);
        this.rows = rows;
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.deferredSymbolKeys = deferredSymbolKeys;
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        if (!isTreeMapBuilt) {
            buildTreeMap();
            isTreeMapBuilt = true;
        }
        if (index < lim) {
            long row = rows.get(index++);
            recordA.jumpTo(Rows.toPartitionIndex(row), Rows.toLocalRowID(row));
            return true;
        }
        return false;
    }

    @Override
    public void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.dataFrameCursor = dataFrameCursor;
        recordA.of(dataFrameCursor.getTableReader());
        recordB.of(dataFrameCursor.getTableReader());
        filter.init(this, executionContext);
        circuitBreaker = executionContext.getCircuitBreaker();
        rows.clear();
        found.clear();
        keyCount = -1;
        isTreeMapBuilt = false;
    }

    @Override
    public long size() {
        return isTreeMapBuilt ? lim : -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index backward scan").meta("on").putColumnName(columnIndex);
        sink.optAttr("filter", filter);
    }

    @Override
    public void toTop() {
        index = 0;
        filter.toTop();
    }

    private void addFoundKey(int symbolKey, BitmapIndexReader indexReader, int partitionIndex, long rowLo, long rowHi) {
        int index = found.keyIndex(symbolKey);
        if (index > -1) {
            RowCursor cursor = indexReader.getCursor(false, symbolKey, rowLo, rowHi);
            while (cursor.hasNext()) {
                final long row = cursor.next();
                recordA.setRecordIndex(row);
                if (filter.getBool(recordA)) {
                    rows.add(Rows.toRowID(partitionIndex, row));
                    found.addAt(index, symbolKey);
                    break;
                }
            }
        }
    }

    private void buildTreeMap() {
        if (keyCount < 0) {
            keyCount = symbolKeys.size();
            if (deferredSymbolKeys != null) {
                keyCount += deferredSymbolKeys.size();
            }
        }

        DataFrame frame;
        // frame metadata is based on TableReader, which is "full" metadata
        // this cursor works with subset of columns, which warrants column index remap
        int frameColumnIndex = columnIndexes.getQuick(columnIndex);
        while ((frame = dataFrameCursor.next()) != null && found.size() < keyCount) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int partitionIndex = frame.getPartitionIndex();
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(frameColumnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;
            this.recordA.jumpTo(partitionIndex, 0);

            for (int i = 0, n = symbolKeys.size(); i < n; i++) {
                int symbolKey = symbolKeys.get(i);
                addFoundKey(symbolKey, indexReader, partitionIndex, rowLo, rowHi);
            }
            if (deferredSymbolKeys != null) {
                for (int i = 0, n = deferredSymbolKeys.size(); i < n; i++) {
                    int symbolKey = deferredSymbolKeys.get(i);
                    if (!symbolKeys.contains(symbolKey)) {
                        addFoundKey(symbolKey, indexReader, partitionIndex, rowLo, rowHi);
                    }
                }
            }
        }

        rows.sortAsUnsigned();
        index = 0;
        lim = rows.size();
    }
}

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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;

class LatestByValueIndexedFilteredRecordCursor extends AbstractLatestByValueRecordCursor {
    private final Function filter;
    private SqlExecutionCircuitBreaker circuitBreaker;

    public LatestByValueIndexedFilteredRecordCursor(
            int columnIndex,
            int symbolKey,
            @NotNull Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(columnIndexes, columnIndex, symbolKey);
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        if (!isFindPending) {
            findRecord();
            hasNext = isRecordFound;
            isFindPending = true;
        }
        if (hasNext) {
            hasNext = false;
            return true;
        }
        return false;
    }

    @Override
    public void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.dataFrameCursor = dataFrameCursor;
        recordA.of(dataFrameCursor.getTableReader());
        recordB.of(dataFrameCursor.getTableReader());
        circuitBreaker = executionContext.getCircuitBreaker();
        filter.init(this, executionContext);
        isRecordFound = false;
        isFindPending = false;
    }

    @Override
    public void setSymbolKey(int symbolKey) {
        super.setSymbolKey(TableUtils.toIndexKey(symbolKey));
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index backward scan").meta("on").putColumnName(columnIndex);
        sink.optAttr("filter", filter);
    }

    @Override
    public void toTop() {
        hasNext = isRecordFound;
        filter.toTop();
    }

    private void findRecord() {
        DataFrame frame;
        // frame metadata is based on TableReader, which is "full" metadata
        // this cursor works with subset of columns, which warrants column index remap
        int frameColumnIndex = columnIndexes.getQuick(columnIndex);
        while ((frame = dataFrameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int partitionIndex = frame.getPartitionIndex();
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(frameColumnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;
            recordA.jumpTo(partitionIndex, 0);

            RowCursor cursor = indexReader.getCursor(false, symbolKey, rowLo, rowHi);
            while (cursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(cursor.next());
                if (filter.getBool(recordA)) {
                    isRecordFound = true;
                    return;
                }
            }
        }
    }
}

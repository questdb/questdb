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

import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;

class LatestByValueFilteredRecordCursor extends AbstractLatestByValueRecordCursor {

    private final Function filter;

    public LatestByValueFilteredRecordCursor(
            int columnIndex,
            int symbolKey,
            @NotNull Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(columnIndexes, columnIndex, symbolKey);
        this.filter = filter;
    }

    @Override
    public void close() {
        dataFrameCursor.close();
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
    public long size() {
        return -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Row backward scan");
        sink.attr("symbolFilter").putColumnName(columnIndex).val('=').val(symbolKey);
        sink.attr("filter").val(filter);
    }

    @Override
    public void toTop() {
        hasNext = isRecordFound;
        filter.toTop();
    }

    private void findRecord() {
        DataFrame frame;
        OUT:
        while ((frame = dataFrameCursor.next()) != null) {
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            recordA.jumpTo(frame.getPartitionIndex(), rowHi);
            for (long row = rowHi; row >= rowLo; row--) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(row);
                if (filter.getBool(recordA)) {
                    int key = recordA.getInt(columnIndex);
                    if (key == symbolKey) {
                        isRecordFound = true;
                        break OUT;
                    }
                }
            }
        }
    }
}

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

import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class DataFrameRecordCursorImpl extends AbstractDataFrameRecordCursor {

    private final boolean entityCursor;
    private final Function filter;
    private final RowCursorFactory rowCursorFactory;
    private boolean areCursorsPrepared;
    private RowCursor rowCursor;

    public DataFrameRecordCursorImpl(
            RowCursorFactory rowCursorFactory,
            boolean entityCursor,
            // this cursor owns "toTop()" lifecycle of filter
            @Nullable Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(columnIndexes);
        this.rowCursorFactory = rowCursorFactory;
        this.entityCursor = entityCursor;
        this.filter = filter;
    }

    public RowCursorFactory getRowCursorFactory() {
        return rowCursorFactory;
    }

    @Override
    public boolean hasNext() {
        if (!areCursorsPrepared) {
            rowCursorFactory.prepareCursor(dataFrameCursor.getTableReader());
            areCursorsPrepared = true;
        }

        try {
            if (rowCursor != null && rowCursor.hasNext()) {
                recordA.setRecordIndex(rowCursor.next());
                return true;
            }

            DataFrame dataFrame;
            while ((dataFrame = dataFrameCursor.next()) != null) {
                rowCursor = rowCursorFactory.getCursor(dataFrame);
                if (rowCursor.hasNext()) {
                    recordA.jumpTo(dataFrame.getPartitionIndex(), rowCursor.next());
                    return true;
                }
            }
        } catch (NoMoreFramesException ignore) {
            return false;
        }

        return false;
    }

    @Override
    public boolean isUsingIndex() {
        return rowCursorFactory.isUsingIndex();
    }

    @Override
    public void of(DataFrameCursor dataFrameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (this.dataFrameCursor != dataFrameCursor) {
            close();
            this.dataFrameCursor = dataFrameCursor;
        }
        recordA.of(dataFrameCursor.getTableReader());
        recordB.of(dataFrameCursor.getTableReader());
        rowCursorFactory.init(dataFrameCursor.getTableReader(), sqlExecutionContext);
        rowCursor = null;
        areCursorsPrepared = false;
    }

    @Override
    public long size() {
        return entityCursor ? dataFrameCursor.size() : -1;
    }

    @Override
    public boolean skipTo(long rowCount) {
        if (!dataFrameCursor.supportsRandomAccess() || filter != null || rowCursorFactory.isUsingIndex()) {
            return false;
        }

        DataFrame dataFrame = dataFrameCursor.skipTo(rowCount);
        if (dataFrame != null) {
            rowCursor = rowCursorFactory.getCursor(dataFrame);
            recordA.jumpTo(dataFrame.getPartitionIndex(), dataFrame.getRowLo()); // move to partition, rowlo doesn't matter
            return true;
        }
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Data frame scan");
    }

    @Override
    public void toTop() {
        if (filter != null) {
            filter.toTop();
        }
        dataFrameCursor.toTop();
        rowCursor = null;
    }
}

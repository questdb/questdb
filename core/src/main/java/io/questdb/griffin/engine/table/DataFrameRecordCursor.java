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

import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BooleanSupplier;

class DataFrameRecordCursor extends AbstractDataFrameRecordCursor {
    private final RowCursorFactory rowCursorFactory;
    private final boolean entityCursor;
    private RowCursor rowCursor;
    private BooleanSupplier next;
    private final BooleanSupplier nextRow = this::nextRow;
    private final BooleanSupplier nextFrame = this::nextFrame;
    private final Function filter;

    public DataFrameRecordCursor(
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

    @Override
    public boolean hasNext() {
        try {
            return next.getAsBoolean();
        } catch (NoMoreFramesException ignore) {
            return false;
        }
    }

    @Override
    public void toTop() {
        if (filter != null) {
            filter.toTop();
        }
        dataFrameCursor.toTop();
        next = nextFrame;
    }

    private boolean nextRow() {
        if (rowCursor.hasNext()) {
            recordA.setRecordIndex(rowCursor.next());
            return true;
        }
        return nextFrame();
    }

    @Override
    public void of(DataFrameCursor dataFrameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (this.dataFrameCursor != dataFrameCursor) {
            close();
            this.dataFrameCursor = dataFrameCursor;
        }
        this.recordA.of(dataFrameCursor.getTableReader());
        this.recordB.of(dataFrameCursor.getTableReader());
        this.rowCursorFactory.prepareCursor(dataFrameCursor.getTableReader(), sqlExecutionContext);
        this.next = nextFrame;
    }

    @Override
    public long size() {
        return entityCursor ? dataFrameCursor.size() : -1;
    }

    private boolean nextFrame() {
        DataFrame dataFrame;
        while ((dataFrame = dataFrameCursor.next()) != null) {
            rowCursor = rowCursorFactory.getCursor(dataFrame);
            if (rowCursor.hasNext()) {
                recordA.jumpTo(dataFrame.getPartitionIndex(), rowCursor.next());
                next = nextRow;
                return true;
            }
        }
        return false;
    }
}

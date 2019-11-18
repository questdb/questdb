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
import io.questdb.griffin.SqlExecutionContext;
import org.jetbrains.annotations.Nullable;

class DataFrameRecordCursor extends AbstractDataFrameRecordCursor {
    private final RowCursorFactory rowCursorFactory;
    private final Function filter;
    private final boolean entityCursor;
    private RowCursor rowCursor;

    public DataFrameRecordCursor(RowCursorFactory rowCursorFactory, @Nullable Function filter, boolean entityCursor) {
        this.rowCursorFactory = rowCursorFactory;
        this.filter = filter;
        this.entityCursor = entityCursor;
    }

    @Override
    public boolean hasNext() {
        try {
            if (rowCursor != null && rowCursor.hasNext()) {
                record.setRecordIndex(rowCursor.next());
                return true;
            }
            return nextFrame();
        } catch (NoMoreFramesException ignore) {
            return false;
        }
    }

    @Override
    public void toTop() {
        dataFrameCursor.toTop();
        rowCursor = null;
        if (filter != null) {
            filter.toTop();
        }
    }

    @Override
    public void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) {
        if (this.dataFrameCursor != dataFrameCursor) {
            close();
            this.dataFrameCursor = dataFrameCursor;
        }
        this.record.of(dataFrameCursor.getTableReader());
        this.rowCursorFactory.prepareCursor(dataFrameCursor.getTableReader());
        rowCursor = null;
        if (filter != null) {
            filter.init(dataFrameCursor, executionContext);
        }
    }

    @Override
    public long size() {
        return entityCursor ? dataFrameCursor.size() : -1;
    }

    private boolean nextFrame() {
        while (dataFrameCursor.hasNext()) {
            DataFrame dataFrame = dataFrameCursor.next();
            rowCursor = rowCursorFactory.getCursor(dataFrame);
            if (rowCursor.hasNext()) {
                record.jumpTo(dataFrame.getPartitionIndex(), rowCursor.next());
                return true;
            }
        }
        return false;
    }
}

/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.table;

import com.questdb.cairo.sql.*;
import com.questdb.griffin.SqlExecutionContext;
import org.jetbrains.annotations.Nullable;

public class LatestByValueFilteredRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {

    private final AbstractDataFrameRecordCursor cursor;
    private final Function filter;

    public LatestByValueFilteredRecordCursorFactory(
            RecordMetadata metadata,
            DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            int symbolKey,
            @Nullable Function filter) {
        super(metadata, dataFrameCursorFactory);
        if (filter == null) {
            this.cursor = new LatestByValueRecordCursor(columnIndex, symbolKey);
        } else {
            this.cursor = new LatestByValueFilteredRecordCursor(columnIndex, symbolKey, filter);
        }
        this.filter = filter;
    }

    @Override
    public void close() {
        super.close();
        if (filter != null) {
            filter.close();
        }
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        cursor.of(dataFrameCursor, executionContext);
        return cursor;
    }
}

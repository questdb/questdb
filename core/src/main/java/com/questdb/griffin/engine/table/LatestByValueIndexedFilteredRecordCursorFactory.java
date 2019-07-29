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

import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.SqlExecutionContext;
import org.jetbrains.annotations.NotNull;

public class LatestByValueIndexedFilteredRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final LatestByValueIndexedFilteredRecordCursor cursor;
    private final Function filter;

    public LatestByValueIndexedFilteredRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            int symbolKey,
            @NotNull Function filter) {
        super(metadata, dataFrameCursorFactory);
        this.cursor = new LatestByValueIndexedFilteredRecordCursor(columnIndex, TableUtils.toIndexKey(symbolKey), filter);
        this.filter = filter;
    }

    @Override
    public void close() {
        super.close();
        filter.close();
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }

    @Override
    protected RecordCursor getCursorInstance(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) {
        cursor.of(dataFrameCursor, executionContext);
        return cursor;
    }
}

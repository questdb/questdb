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
import com.questdb.griffin.engine.EmptyTableRecordCursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

abstract class AbstractDeferredValueRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {

    protected final Function filter;
    protected final int columnIndex;
    private final String symbol;
    private AbstractDataFrameRecordCursor cursor;

    public AbstractDeferredValueRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            String symbol,
            @Nullable Function filter
    ) {
        super(metadata, dataFrameCursorFactory);
        this.columnIndex = columnIndex;
        this.symbol = symbol;
        this.filter = filter;
    }

    @Override
    public void close() {
        super.close();
        if (filter != null) {
            filter.close();
        }
    }

    protected abstract AbstractDataFrameRecordCursor createDataFrameCursorFor(int symbolKey);

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        if (cursor == null && lookupDeferredSymbol(dataFrameCursor)) {
            return EmptyTableRecordCursor.INSTANCE;
        }
        cursor.of(dataFrameCursor, executionContext);
        return cursor;
    }

    private boolean lookupDeferredSymbol(DataFrameCursor dataFrameCursor) {
        int symbolKey = dataFrameCursor.getSymbolTable(columnIndex).getQuick(symbol);
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            dataFrameCursor.close();
            return true;
        }

        this.cursor = createDataFrameCursorFor(symbolKey);
        return false;
    }
}

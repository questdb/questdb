/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.AbstractRecordCursorFactory;
import com.questdb.cairo.sql.DataFrameCursor;
import com.questdb.cairo.sql.DataFrameCursorFactory;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.common.SymbolTable;

public class LatestByValueDeferredRecordCursorFactory extends AbstractRecordCursorFactory {

    private final DataFrameCursorFactory dataFrameCursorFactory;
    private final String symbol;
    private final int columnIndex;
    private LatestByValueRecordCursor cursor;

    public LatestByValueDeferredRecordCursorFactory(RecordMetadata metadata, DataFrameCursorFactory dataFrameCursorFactory, int columnIndex, String symbol) {
        super(metadata);
        this.dataFrameCursorFactory = dataFrameCursorFactory;
        this.columnIndex = columnIndex;
        this.symbol = symbol;
    }

    @Override
    public RecordCursor getCursor() {
        DataFrameCursor dataFrameCursor = dataFrameCursorFactory.getCursor();
        if (cursor != null) {
            cursor.of(dataFrameCursor);
            return cursor;
        }

        int symbolKey = dataFrameCursor.getSymbolTable(columnIndex).getQuick(symbol);
        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            cursor = new LatestByValueRecordCursor(columnIndex, symbolKey);
            cursor.of(dataFrameCursor);
            return cursor;
        }

        dataFrameCursor.close();
        return EmptyTableRecordCursor.INSTANCE;
    }
}

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

import com.questdb.cairo.EmptyRowCursor;
import com.questdb.cairo.TableReader;
import com.questdb.cairo.sql.*;

public class DeferredSymbolIndexFilteredRowCursorFactory implements RowCursorFactory {
    private final SymbolIndexFilteredRowCursor cursor;
    private final int columnIndex;
    private final String symbol;
    private int symbolKey = SymbolTable.VALUE_NOT_FOUND;

    public DeferredSymbolIndexFilteredRowCursorFactory(int columnIndex, String symbol, Function filter, boolean cachedIndexReaderCursor) {
        this.columnIndex = columnIndex;
        this.symbol = symbol;
        this.cursor = new SymbolIndexFilteredRowCursor(columnIndex, filter, cachedIndexReaderCursor);
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return EmptyRowCursor.INSTANCE;
        }
        return cursor.of(dataFrame);
    }

    @Override
    public void prepareCursor(TableReader tableReader) {
        symbolKey = tableReader.getSymbolMapReader(columnIndex).getQuick(symbol);
        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            this.cursor.of(symbolKey);
            this.cursor.setTableReader(tableReader);
        }
    }
}

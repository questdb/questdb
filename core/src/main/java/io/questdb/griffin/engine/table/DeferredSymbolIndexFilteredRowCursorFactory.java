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

import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.*;

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

    @Override
    public boolean isEntity() {
        return false;
    }
}

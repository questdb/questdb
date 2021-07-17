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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public class DeferredSymbolIndexRowCursorFactory implements FunctionBasedRowCursorFactory {
    private final int columnIndex;
    private final boolean cachedIndexReaderCursor;
    private final Function symbol;
    private int symbolKey;
    private final int indexDirection;

    public DeferredSymbolIndexRowCursorFactory(
            int columnIndex,
            Function symbol,
            boolean cachedIndexReaderCursor,
            int indexDirection
    ) {
        this.columnIndex = columnIndex;
        this.symbolKey = SymbolTable.VALUE_NOT_FOUND;
        this.symbol = symbol;
        this.cachedIndexReaderCursor = cachedIndexReaderCursor;
        this.indexDirection = indexDirection;
    }

    @Override
    public Function getFunction() {
        return symbol;
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return EmptyRowCursor.INSTANCE;
        }

        return dataFrame
                .getBitmapIndexReader(columnIndex, indexDirection)
                .getCursor(cachedIndexReaderCursor, symbolKey, dataFrame.getRowLo(), dataFrame.getRowHi() - 1);
    }

    @Override
    public void prepareCursor(TableReader tableReader, SqlExecutionContext sqlExecutionContext) throws SqlException {
        symbol.init(tableReader, sqlExecutionContext);
        int symbolKey = tableReader.getSymbolMapReader(columnIndex).keyOf(symbol.getSymbol(null));
        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            this.symbolKey = TableUtils.toIndexKey(symbolKey);
        }
    }

    @Override
    public boolean isEntity() {
        return false;
    }
}

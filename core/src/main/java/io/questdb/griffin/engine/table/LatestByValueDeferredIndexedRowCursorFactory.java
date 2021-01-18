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

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;

public class LatestByValueDeferredIndexedRowCursorFactory implements RowCursorFactory {
    private final int columnIndex;
    private final String symbol;
    private final boolean cachedIndexReaderCursor;
    private final LatestByValueIndexedRowCursor cursor = new LatestByValueIndexedRowCursor();
    private int symbolKey;

    // todo: make symbol function to allow use with bind variables
    public LatestByValueDeferredIndexedRowCursorFactory(int columnIndex, String symbol, boolean cachedIndexReaderCursor) {
        this.columnIndex = columnIndex;
        this.symbol = symbol;
        this.symbolKey = SymbolTable.VALUE_NOT_FOUND;
        this.cachedIndexReaderCursor = cachedIndexReaderCursor;
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            RowCursor cursor =
                    dataFrame
                            .getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD)
                            .getCursor(cachedIndexReaderCursor, symbolKey, dataFrame.getRowLo(), dataFrame.getRowHi() - 1);

            if (cursor.hasNext()) {
                this.cursor.of(cursor.next());
                return this.cursor;
            }
        }
        return EmptyRowCursor.INSTANCE;
    }

    @Override
    public void prepareCursor(TableReader tableReader, SqlExecutionContext sqlExecutionContext) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            symbolKey = tableReader.getSymbolMapReader(columnIndex).keyOf(symbol);
            if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                symbolKey++;
            }
        }
    }

    @Override
    public boolean isEntity() {
        return false;
    }
}

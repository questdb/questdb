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
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;

public class DeferredSymbolIndexFilteredRowCursorFactory implements FunctionBasedRowCursorFactory {
    private final SymbolIndexFilteredRowCursor cursor;
    private final int columnIndex;
    private final Function symbolFunction;
    private int symbolKey = SymbolTable.VALUE_NOT_FOUND;

    public DeferredSymbolIndexFilteredRowCursorFactory(
            int columnIndex,
            Function symbolFunction,
            Function filter,
            boolean cachedIndexReaderCursor,
            int indexDirection,
            IntList columnIndexes
    ) {
        this.columnIndex = columnIndex;
        this.symbolFunction = symbolFunction;
        this.cursor = new SymbolIndexFilteredRowCursor(columnIndex, filter, cachedIndexReaderCursor, indexDirection, columnIndexes);
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return EmptyRowCursor.INSTANCE;
        }
        return cursor.of(dataFrame);
    }

    @Override
    public void prepareCursor(TableReader tableReader, SqlExecutionContext sqlExecutionContext) throws SqlException {
        symbolFunction.init(tableReader, sqlExecutionContext);
        symbolKey = tableReader.getSymbolMapReader(columnIndex).keyOf(symbolFunction.getStr(null));
        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            this.cursor.of(symbolKey);
            this.cursor.prepare(tableReader);
        }
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public Function getFunction() {
        return symbolFunction;
    }
}

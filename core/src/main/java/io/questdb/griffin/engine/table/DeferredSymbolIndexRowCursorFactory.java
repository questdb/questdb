/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public class DeferredSymbolIndexRowCursorFactory implements FunctionBasedRowCursorFactory {
    private final boolean cachedIndexReaderCursor;
    private final int columnIndex;
    private final int indexDirection;
    private final Function symbol;
    private int symbolKey;

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
    public RowCursor getCursor(DataFrame dataFrame) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return EmptyRowCursor.INSTANCE;
        }

        return dataFrame
                .getBitmapIndexReader(columnIndex, indexDirection)
                .getCursor(cachedIndexReaderCursor, symbolKey, dataFrame.getRowLo(), dataFrame.getRowHi() - 1);
    }

    @Override
    public Function getFunction() {
        return symbol;
    }

    @Override
    public void init(TableReader tableReader, SqlExecutionContext sqlExecutionContext) throws SqlException {
        symbol.init(tableReader, sqlExecutionContext);
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public boolean isUsingIndex() {
        return true;
    }

    @Override
    public void prepareCursor(TableReader tableReader) {
        int symbolKey = tableReader.getSymbolMapReader(columnIndex).keyOf(symbol.getSymbol(null));
        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            this.symbolKey = TableUtils.toIndexKey(symbolKey);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index ").type(BitmapIndexReader.nameOf(indexDirection)).type(" scan").meta("on").putBaseColumnName(columnIndex);
        sink.meta("deferred").val("true");
        sink.attr("filter").putBaseColumnName(columnIndex).val('=').val(symbol);
    }
}

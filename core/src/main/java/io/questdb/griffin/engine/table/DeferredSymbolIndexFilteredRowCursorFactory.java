/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public class DeferredSymbolIndexFilteredRowCursorFactory implements FunctionBasedRowCursorFactory {
    private final int columnIndex;
    private final SymbolIndexFilteredRowCursor cursor;
    private final Function symbolFunction;
    private int symbolKey = SymbolTable.VALUE_NOT_FOUND;

    public DeferredSymbolIndexFilteredRowCursorFactory(
            int columnIndex,
            Function symbolFunction,
            Function filter,
            boolean cachedIndexReaderCursor,
            int indexDirection
    ) {
        this.columnIndex = columnIndex;
        this.symbolFunction = symbolFunction;
        cursor = new SymbolIndexFilteredRowCursor(columnIndex, filter, cachedIndexReaderCursor, indexDirection);
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return EmptyRowCursor.INSTANCE;
        }
        return cursor.of(pageFrame, pageFrameMemory);
    }

    @Override
    public Function getFunction() {
        return symbolFunction;
    }

    @Override
    public void init(PageFrameCursor pageFrameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        symbolFunction.init(pageFrameCursor, sqlExecutionContext);
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
    public void prepareCursor(PageFrameCursor pageFrameCursor) {
        symbolKey = pageFrameCursor.getSymbolTable(columnIndex).keyOf(symbolFunction.getStrA(null));
        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            cursor.of(symbolKey);
            cursor.prepare(pageFrameCursor);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index ").type(BitmapIndexReader.nameOf(cursor.getIndexDirection())).type(" scan").meta("on").putBaseColumnName(columnIndex);
        sink.meta("deferred").val(true);
        sink.attr("symbolFilter").putBaseColumnName(columnIndex).val('=').val(symbolFunction);
        sink.optAttr("filter", cursor.getFilter());
    }
}

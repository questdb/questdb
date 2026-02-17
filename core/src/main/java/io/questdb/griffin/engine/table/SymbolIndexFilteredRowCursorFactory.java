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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;

public class SymbolIndexFilteredRowCursorFactory implements SymbolFunctionRowCursorFactory {
    private final int columnIndex;
    private final SymbolIndexFilteredRowCursor cursor;
    private final Function symbolFunction;

    public SymbolIndexFilteredRowCursorFactory(
            int columnIndex,
            int symbolKey,
            Function filter,
            boolean cachedIndexReaderCursor,
            int indexDirection,
            Function symbolFunction
    ) {
        this.columnIndex = columnIndex;
        this.cursor = new SymbolIndexFilteredRowCursor(
                columnIndex,
                symbolKey,
                filter,
                cachedIndexReaderCursor,
                indexDirection
        );
        this.symbolFunction = symbolFunction;
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        return cursor.of(pageFrame, pageFrameMemory);
    }

    @Override
    public Function getFunction() {
        return symbolFunction;
    }

    public int getSymbolKey() {
        return cursor.getSymbolKey() == 0 ? SymbolTable.VALUE_IS_NULL : cursor.getSymbolKey() - 1;
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
    public void of(int symbolKey) {
        cursor.of(symbolKey);
    }

    @Override
    public void prepareCursor(PageFrameCursor pageFrameCursor) {
        cursor.prepare(pageFrameCursor);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index ").type(BitmapIndexReader.nameOf(cursor.getIndexDirection())).type(" scan").meta("on").putBaseColumnName(columnIndex);
        sink.attr("filter").putBaseColumnName(columnIndex).val('=').val(cursor.getSymbolKey()).val(" and ").val(cursor.getFilter());
    }
}


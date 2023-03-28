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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.std.IntList;

public class SymbolIndexFilteredRowCursorFactory implements SymbolFunctionRowCursorFactory {
    private final SymbolIndexFilteredRowCursor cursor;
    private final Function symbolFunction;

    public SymbolIndexFilteredRowCursorFactory(
            int columnIndex,
            int symbolKey,
            Function filter,
            boolean cachedIndexReaderCursor,
            int indexDirection,
            IntList columnIndexes,
            Function symbolFunction
    ) {
        this.cursor = new SymbolIndexFilteredRowCursor(
                columnIndex,
                symbolKey,
                filter,
                cachedIndexReaderCursor,
                indexDirection,
                columnIndexes
        );
        this.symbolFunction = symbolFunction;
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        return cursor.of(dataFrame);
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
    public void prepareCursor(TableReader tableReader) {
        this.cursor.prepare(tableReader);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index ").type(BitmapIndexReader.nameOf(cursor.getIndexDirection())).type(" scan").meta("on").putColumnName(cursor.getColumnIndex());
        sink.attr("filter").putColumnName(cursor.getColumnIndex()).val('=').val(cursor.getSymbolKey()).val(" and ").val(cursor.getFilter());
    }
}


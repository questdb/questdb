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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;

public class LatestByValueDeferredIndexedRowCursorFactory implements RowCursorFactory {
    private final boolean cachedIndexReaderCursor;
    private final int columnIndex;
    private final LatestByValueIndexedRowCursor cursor = new LatestByValueIndexedRowCursor();
    private final Function symbolFunc;
    private int symbolKey;

    public LatestByValueDeferredIndexedRowCursorFactory(int columnIndex, Function symbolFunc, boolean cachedIndexReaderCursor) {
        this.columnIndex = columnIndex;
        this.symbolFunc = symbolFunc;
        symbolKey = SymbolTable.VALUE_NOT_FOUND;
        this.cachedIndexReaderCursor = cachedIndexReaderCursor;
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            RowCursor cursor = dataFrame
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
    public boolean isEntity() {
        return false;
    }

    @Override
    public boolean isUsingIndex() {
        return true;
    }

    @Override
    public void prepareCursor(TableReader tableReader) {
        final CharSequence symbol = symbolFunc.getStr(null);
        symbolKey = tableReader.getSymbolMapReader(columnIndex).keyOf(symbol);
        if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
            symbolKey++;
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index ").type(BitmapIndexReader.NAME_BACKWARD).type(" scan").meta("on").putBaseColumnNameNoRemap(columnIndex).meta("deferred").val(true);
        sink.attr("filter").putBaseColumnNameNoRemap(columnIndex).val('=').val(symbolFunc);
    }
}

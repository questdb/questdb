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

import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FilterOnValuesRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final DataFrameRecordCursor cursor;
    private final int columnIndex;
    private final Function filter;
    private final ObjList<RowCursorFactory> cursorFactories;
    private CharSequenceHashSet deferredSymbols;

    public FilterOnValuesRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            @NotNull CharSequenceHashSet keyValues,
            int columnIndex,
            @NotNull @Transient TableReader reader,
            @Nullable Function filter
    ) {
        super(metadata, dataFrameCursorFactory);
        final int nKeyValues = keyValues.size();
        this.columnIndex = columnIndex;
        this.filter = filter;
        cursorFactories = new ObjList<>(nKeyValues);
        final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndex);
        for (int i = 0; i < nKeyValues; i++) {
            final CharSequence symbol = keyValues.get(i);
            final int symbolKey = symbolMapReader.keyOf(symbol);
            if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                addSymbolKey(symbolKey);
            } else {
                if (deferredSymbols == null) {
                    deferredSymbols = new CharSequenceHashSet();
                }
                deferredSymbols.add(Chars.toString(symbol));
            }
        }
        this.cursor = new DataFrameRecordCursor(new HeapRowCursorFactory(cursorFactories), filter, false);
    }

    @Override
    public void close() {
        if (filter != null) {
            filter.close();
        }
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }

    private void addSymbolKey(int symbolKey) {
        final RowCursorFactory rowCursorFactory;
        if (filter == null) {
            rowCursorFactory = new SymbolIndexRowCursorFactory(columnIndex, symbolKey, cursorFactories.size() == 0);
        } else {
            rowCursorFactory = new SymbolIndexFilteredRowCursorFactory(columnIndex, symbolKey, filter, cursorFactories.size() == 0);
        }
        cursorFactories.add(rowCursorFactory);
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        if (deferredSymbols != null && lookupDeferredSymbols(dataFrameCursor)) {
            return EmptyTableRecordCursor.INSTANCE;
        }
        this.cursor.of(dataFrameCursor, executionContext);
        return this.cursor;
    }

    private boolean lookupDeferredSymbols(DataFrameCursor dataFrameCursor) {
        StaticSymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndex);
        int n = deferredSymbols.size();
        for (int i = 0; i < n; ) {
            CharSequence symbolValue = deferredSymbols.get(i);
            int symbolKey = symbolTable.keyOf(symbolValue);
            if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                addSymbolKey(symbolKey);
                deferredSymbols.remove(symbolValue);
                n--;
            } else {
                i++;
            }
        }

        if (deferredSymbols.size() == 0) {
            deferredSymbols = null;
        }

        if (cursorFactories.size() == 0) {
            dataFrameCursor.close();
            return true;
        }
        return false;
    }
}

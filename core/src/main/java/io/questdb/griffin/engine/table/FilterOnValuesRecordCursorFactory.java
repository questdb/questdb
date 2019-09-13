/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
            final int symbolKey = symbolMapReader.getQuick(symbol);
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
        SymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndex);
        int n = deferredSymbols.size();
        for (int i = 0; i < n; ) {
            CharSequence symbolValue = deferredSymbols.get(i);
            int symbolKey = symbolTable.getQuick(symbolValue);
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

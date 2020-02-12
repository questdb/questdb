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
import io.questdb.griffin.OrderByMnemonic;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FilterOnValuesRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final DataFrameRecordCursor cursor;
    private final int columnIndex;
    private final Function filter;
    private final ObjList<RowCursorFactory> cursorFactories;
    private final boolean followedOrderByAdvice;

    public FilterOnValuesRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            @NotNull @Transient ObjList<CharSequence> keyValues,
            int columnIndex,
            @NotNull @Transient TableReader reader,
            @Nullable Function filter,
            int orderByMnemonic,
            boolean followedOrderByAdvice,
            int indexDirection
    ) {
        super(metadata, dataFrameCursorFactory);
        final int nKeyValues = keyValues.size();
        this.columnIndex = columnIndex;
        this.filter = filter;
        cursorFactories = new ObjList<>(nKeyValues);
        final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndex);
        for (int i = 0; i < nKeyValues; i++) {
            final CharSequence symbol = keyValues.get(i);
            addSymbolKey(symbolMapReader.keyOf(symbol), symbol, indexDirection);
        }
        if (orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT) {
            this.cursor = new DataFrameRecordCursor(new SequentialRowCursorFactory(cursorFactories), false);
        } else {
            this.cursor = new DataFrameRecordCursor(new HeapRowCursorFactory(cursorFactories), false);
        }
        this.followedOrderByAdvice = followedOrderByAdvice;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return followedOrderByAdvice;
    }

    @Override
    public void close() {
        Misc.free(filter);
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }

    private void addSymbolKey(int symbolKey, CharSequence symbolValue, int indexDirection) {
        final RowCursorFactory rowCursorFactory;
        if (filter == null) {
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                rowCursorFactory = new DeferredSymbolIndexRowCursorFactory(columnIndex, Chars.toString(symbolValue), cursorFactories.size() == 0, indexDirection);
            } else {
                rowCursorFactory = new SymbolIndexRowCursorFactory(columnIndex, symbolKey, cursorFactories.size() == 0, indexDirection);
            }
        } else {
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                rowCursorFactory = new DeferredSymbolIndexFilteredRowCursorFactory(columnIndex, Chars.toString(symbolValue), filter, cursorFactories.size() == 0, indexDirection);
            } else {
                rowCursorFactory = new SymbolIndexFilteredRowCursorFactory(columnIndex, symbolKey, filter, cursorFactories.size() == 0, indexDirection);
            }
        }
        cursorFactories.add(rowCursorFactory);
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        this.cursor.of(dataFrameCursor, executionContext);
        return this.cursor;
    }
}

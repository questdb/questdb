/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.griffin.engine.table;

import com.questdb.cairo.sql.DataFrameCursor;
import com.questdb.cairo.sql.DataFrameCursorFactory;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.common.SymbolTable;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.IntHashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class AbstractDeferredTreeSetRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    private final int columnIndex;
    // this instance is shared between factory and cursor
    // factory will be resolving symbols for cursor and if successful
    // symbol keys will be added to this hash set
    private final IntHashSet symbolKeys;
    private final CharSequenceHashSet deferredSymbols;

    public AbstractDeferredTreeSetRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @NotNull IntHashSet symbolKeys,
            @Nullable CharSequenceHashSet deferredSymbols) {
        super(metadata, dataFrameCursorFactory);
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.deferredSymbols = deferredSymbols;
    }

    @Override
    public RecordCursor getCursor() {
        DataFrameCursor frameCursor = dataFrameCursorFactory.getCursor();
        if (deferredSymbols != null && deferredSymbols.size() > 0) {
            SymbolTable symbolTable = frameCursor.getSymbolTable(columnIndex);
            for (int i = 0, n = deferredSymbols.size(); i < n; ) {
                CharSequence symbol = deferredSymbols.get(i);
                int symbolKey = symbolTable.getQuick(symbol);
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(symbolKey + 1);
                    deferredSymbols.removeAt(0);
                    n--;
                } else {
                    i++;
                }
            }
        }
        cursor.of(frameCursor);
        return cursor;
    }
}

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

package com.questdb.griffin.engine.table;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.SymbolMapReader;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.DataFrameCursor;
import com.questdb.cairo.sql.DataFrameCursorFactory;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.cairo.sql.SymbolTable;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.Chars;
import com.questdb.std.IntHashSet;
import com.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractDeferredTreeSetRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    // this instance is shared between factory and cursor
    // factory will be resolving symbols for cursor and if successful
    // symbol keys will be added to this hash set
    protected final IntHashSet symbolKeys;
    private final int columnIndex;
    private final CharSequenceHashSet deferredSymbols;

    public AbstractDeferredTreeSetRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @Transient CharSequenceHashSet keyValues,
            @Transient SymbolMapReader symbolMapReader
    ) {
        super(metadata, dataFrameCursorFactory, configuration);

        // we need two data structures, int hash set for symbol keys we can resolve here
        // and CharSequence hash set for symbols we cannot resolve
        // we could pass all symbols to factory to resolve, but this would lead to
        // creating Strings for symbols that we may be able to avoid doing so

        final int nKeyValues = keyValues.size();
        final IntHashSet symbolKeys = new IntHashSet(nKeyValues);
        CharSequenceHashSet deferredSymbols = null;

        for (int i = 0; i < nKeyValues; i++) {
            CharSequence symbol = keyValues.get(i);
            int symbolKey = symbolMapReader.getQuick(symbol);
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                if (deferredSymbols == null) {
                    deferredSymbols = new CharSequenceHashSet();
                }
                deferredSymbols.add(Chars.toString(symbol));
            } else {
                symbolKeys.add(TableUtils.toIndexKey(symbolKey));
            }
        }

        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.deferredSymbols = deferredSymbols;
    }

    @Override
    protected AbstractDataFrameRecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        if (deferredSymbols != null && deferredSymbols.size() > 0) {
            SymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndex);
            for (int i = 0, n = deferredSymbols.size(); i < n; ) {
                CharSequence symbol = deferredSymbols.get(i);
                int symbolKey = symbolTable.getQuick(symbol);
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(TableUtils.toIndexKey(symbolKey));
                    deferredSymbols.remove(symbol);
                    n--;
                } else {
                    i++;
                }
            }
        }
        return super.getCursorInstance(dataFrameCursor, executionContext);
    }
}

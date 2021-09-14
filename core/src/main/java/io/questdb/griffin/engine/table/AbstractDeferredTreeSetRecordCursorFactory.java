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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.Transient;
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
            int symbolKey = symbolMapReader.keyOf(symbol);
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
    ) throws SqlException {
        if (deferredSymbols != null && deferredSymbols.size() > 0) {
            StaticSymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndex);
            for (int i = 0, n = deferredSymbols.size(); i < n; ) {
                CharSequence symbol = deferredSymbols.get(i);
                int symbolKey = symbolTable.keyOf(symbol);
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

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.IntHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractDeferredTreeSetRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    // the following two instances are shared between factory and cursor
    // factory will be resolving symbols for cursor and if successful
    // symbol keys will be added to this hash set
    protected final IntHashSet symbolKeys;
    protected final IntHashSet deferredSymbolKeys;
    private final int columnIndex;
    private final ObjList<Function> deferredSymbolFuncs;

    public AbstractDeferredTreeSetRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @Transient ObjList<Function> keyValueFuncs,
            @Transient SymbolMapReader symbolMapReader
    ) {
        super(metadata, dataFrameCursorFactory, configuration);

        // we need two data structures, int hash set for symbol keys we can resolve here
        // and CharSequence hash set for symbols we cannot resolve
        // we could pass all symbols to factory to resolve, but this would lead to
        // creating Strings for symbols that we may be able to avoid doing so

        final int nKeyValues = keyValueFuncs.size();
        final IntHashSet symbolKeys = new IntHashSet(nKeyValues);
        IntHashSet deferredSymbolKeys = null;
        ObjList<Function> deferredFuncs = null;

        for (int i = 0; i < nKeyValues; i++) {
            Function symbolFunc = keyValueFuncs.get(i);
            int symbolKey = symbolFunc.isRuntimeConstant()
                    ? SymbolTable.VALUE_NOT_FOUND
                    : symbolMapReader.keyOf(symbolFunc.getStr(null));
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                if (deferredFuncs == null) {
                    deferredFuncs = new ObjList<>();
                    deferredSymbolKeys = new IntHashSet();
                }
                deferredFuncs.add(symbolFunc);
            } else {
                symbolKeys.add(TableUtils.toIndexKey(symbolKey));
            }
        }

        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.deferredSymbolKeys = deferredSymbolKeys;
        this.deferredSymbolFuncs = deferredFuncs;
    }

    @Override
    protected AbstractDataFrameRecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (deferredSymbolFuncs != null) {
            deferredSymbolKeys.clear();
            StaticSymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndex);
            for (int i = 0, n = deferredSymbolFuncs.size(); i < n; i++) {
                Function symbolFunc = deferredSymbolFuncs.get(i);
                final CharSequence symbol = symbolFunc.getStr(null);
                int symbolKey = symbolTable.keyOf(symbol);
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    deferredSymbolKeys.add(TableUtils.toIndexKey(symbolKey));
                }
            }
        }
        return super.getCursorInstance(dataFrameCursor, executionContext);
    }
}

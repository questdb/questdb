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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;

public class DeferredSingleSymbolFilterDataFrameRecordCursorFactory extends DataFrameRecordCursorFactory {
    private final int symbolColumnIndex;
    private final SingleSymbolFilter symbolFilter;
    private final Function symbolFunc;
    private int symbolKey;
    private boolean convertedToFrame;

    public DeferredSingleSymbolFilterDataFrameRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            int tableSymColIndex,
            Function symbolFunc,
            RowCursorFactory rowCursorFactory,
            RecordMetadata metadata,
            DataFrameCursorFactory dataFrameCursorFactory,
            boolean followsOrderByAdvice,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizes,
            boolean supportsRandomAccess
    ) {
        super(
                configuration,
                metadata,
                dataFrameCursorFactory,
                rowCursorFactory,
                followsOrderByAdvice,
                null,
                false,
                columnIndexes,
                columnSizes,
                supportsRandomAccess
        );
        this.symbolFunc = symbolFunc;
        this.symbolKey = SymbolTable.VALUE_NOT_FOUND;
        this.symbolColumnIndex = columnIndexes.indexOf(tableSymColIndex, 0, columnIndexes.size());

        this.symbolFilter = new SingleSymbolFilter() {
            @Override
            public int getColumnIndex() {
                return symbolColumnIndex;
            }

            @Override
            public int getSymbolFilterKey() {
                return symbolKey;
            }
        };
    }

    public SingleSymbolFilter convertToSampleByIndexDataFrameCursorFactory() {
        if (!this.convertedToFrame) {
            this.convertedToFrame = true;
        }
        return symbolFilter;
    }

    @Override
    public boolean supportPageFrameCursor() {
        return this.convertedToFrame;
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert !this.convertedToFrame;
        return super.getCursorInstance(dataFrameCursor, executionContext);
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        assert this.convertedToFrame;
        DataFrameCursor dataFrameCursor = dataFrameCursorFactory.getCursor(executionContext, order);
        initFwdPageFrameCursor(executionContext, dataFrameCursor);
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            final CharSequence symbol = symbolFunc.getStr(null);
            final StaticSymbolTable symbolMapReader = fwdPageFrameCursor.getSymbolTable(symbolColumnIndex);
            this.symbolKey = symbolMapReader.keyOf(symbol);
            if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                this.symbolKey = TableUtils.toIndexKey(symbolKey);
            }
        }
        return fwdPageFrameCursor;
    }
}

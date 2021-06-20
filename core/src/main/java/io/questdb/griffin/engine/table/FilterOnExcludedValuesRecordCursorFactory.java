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

import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.OrderByMnemonic;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FilterOnExcludedValuesRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final DataFrameRecordCursor cursor;
    private final int columnIndex;
    private final Function filter;
    private final ObjList<RowCursorFactory> cursorFactories;
    private final ObjList<Function> keyExcludedValueFunctions = new ObjList<>();
    private final ObjList<CharSequence> includedValues = new ObjList<>();
    private final ObjList<CharSequence> excludedValues = new ObjList<>();
    private final boolean followedOrderByAdvice;
    private final int indexDirection;
    private final int maxSymbolNotEqualsCount;
    private final IntList columnIndexes;

    public FilterOnExcludedValuesRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            @NotNull @Transient ObjList<Function> keyValues,
            int columnIndex,
            @Nullable Function filter,
            int orderByMnemonic,
            boolean followedOrderByAdvice,
            int indexDirection,
            @NotNull IntList columnIndexes,
            int maxSymbolNotEqualsCount
    ) {
        super(metadata, dataFrameCursorFactory);
        this.indexDirection = indexDirection;
        this.maxSymbolNotEqualsCount = maxSymbolNotEqualsCount;
        final int nKeyValues = keyValues.size();
        this.keyExcludedValueFunctions.addAll(keyValues);
        this.columnIndex = columnIndex;
        this.filter = filter;
        cursorFactories = new ObjList<>(nKeyValues);
        if (orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT) {
            this.cursor = new DataFrameRecordCursor(new SequentialRowCursorFactory(cursorFactories), false, filter, columnIndexes);
        } else {
            this.cursor = new DataFrameRecordCursor(new HeapRowCursorFactory(cursorFactories), false, filter, columnIndexes);
        }
        this.followedOrderByAdvice = followedOrderByAdvice;
        this.columnIndexes = columnIndexes;
    }

    @Override
    public void close() {
        Misc.free(filter);
        Misc.free(includedValues);
        Misc.free(keyExcludedValueFunctions);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return followedOrderByAdvice;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    public void recalculateIncludedValues(TableReader tableReader) {
        excludedValues.clear();
        for (int i = 0, n = keyExcludedValueFunctions.size(); i < n; i++) {
            excludedValues.add(Chars.toString(keyExcludedValueFunctions.getQuick(i).getStr(null)));
        }
        final SymbolMapReaderImpl symbolMapReader = (SymbolMapReaderImpl) tableReader.getSymbolMapReader(columnIndex);
        for (int i = 0; i < symbolMapReader.size(); i++) {
            final CharSequence symbol = symbolMapReader.valueOf(i);
            if (excludedValues.indexOf(symbol) < 0 && includedValues.indexOf(symbol) < 0) {
                final RowCursorFactory rowCursorFactory;
                int symbolKey = symbolMapReader.keyOf(symbol);
                if (filter == null) {
                    rowCursorFactory = new SymbolIndexRowCursorFactory(columnIndex, symbolKey, cursorFactories.size() == 0, indexDirection, null);
                } else {
                    rowCursorFactory = new SymbolIndexFilteredRowCursorFactory(columnIndex, symbolKey, filter, cursorFactories.size() == 0, indexDirection, columnIndexes, null);
                }
                includedValues.add(Chars.toString(symbol));
                cursorFactories.add(rowCursorFactory);
            }
        }
    }

    @Override
    protected RecordCursor getCursorInstance(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext)
            throws SqlException {
        try (TableReader reader = dataFrameCursor.getTableReader()) {
            if (reader.getSymbolMapReader(columnIndex).size() > maxSymbolNotEqualsCount) {
                throw ReaderOutOfDateException.INSTANCE;
            }
            Function.init(keyExcludedValueFunctions, reader, executionContext);
        }
        this.recalculateIncludedValues(dataFrameCursor.getTableReader());
        this.cursor.of(dataFrameCursor, executionContext);
        if (filter != null) {
            filter.init(this.cursor, executionContext);
        }
        return this.cursor;
    }
}

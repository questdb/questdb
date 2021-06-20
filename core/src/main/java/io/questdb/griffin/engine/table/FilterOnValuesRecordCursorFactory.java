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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

public class FilterOnValuesRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final DataFrameRecordCursor cursor;
    private final int columnIndex;
    private final Function filter;
    private final ObjList<FunctionBasedRowCursorFactory> cursorFactories;
    private final boolean followedOrderByAdvice;
    private final IntList columnIndexes;
    private final int orderDirection;

    public FilterOnValuesRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            @NotNull @Transient ObjList<Function> keyValues,
            int columnIndex,
            @NotNull @Transient TableReader reader,
            @Nullable Function filter,
            int orderByMnemonic,
            boolean followedOrderByAdvice,
            int orderDirection,
            int indexDirection,
            @NotNull IntList columnIndexes
    ) {
        super(metadata, dataFrameCursorFactory);
        final int nKeyValues = keyValues.size();
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.columnIndexes = columnIndexes;
        this.orderDirection = orderDirection;
        cursorFactories = new ObjList<>(nKeyValues);
        final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndex);
        for (int i = 0; i < nKeyValues; i++) {
            final Function symbol = keyValues.get(i);
            if (symbol.isConstant()) {
                addSymbolKey(symbolMapReader.keyOf(symbol.getStr(null)), symbol, indexDirection);
            } else {
                addSymbolKey(SymbolTable.VALUE_NOT_FOUND, symbol, indexDirection);
            }
        }
        if (orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT) {
            this.cursor = new DataFrameRecordCursor(new SequentialRowCursorFactory(cursorFactories), false, filter, columnIndexes);
        } else {
            this.cursor = new DataFrameRecordCursor(new HeapRowCursorFactory(cursorFactories), false, filter, columnIndexes);
        }
        this.followedOrderByAdvice = followedOrderByAdvice;
    }

    @Override
    public void close() {
        Misc.free(filter);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return followedOrderByAdvice;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    private void addSymbolKey(int symbolKey, Function symbolFunction, int indexDirection) {
        final FunctionBasedRowCursorFactory rowCursorFactory;
        if (filter == null) {
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                rowCursorFactory = new DeferredSymbolIndexRowCursorFactory(columnIndex, symbolFunction, cursorFactories.size() == 0, indexDirection);
            } else {
                rowCursorFactory = new SymbolIndexRowCursorFactory(columnIndex, symbolKey, cursorFactories.size() == 0, indexDirection, symbolFunction);
            }
        } else {
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                rowCursorFactory = new DeferredSymbolIndexFilteredRowCursorFactory(
                        columnIndex,
                        symbolFunction,
                        filter,
                        cursorFactories.size() == 0,
                        indexDirection,
                        columnIndexes
                );
            } else {
                rowCursorFactory = new SymbolIndexFilteredRowCursorFactory(
                        columnIndex,
                        symbolKey,
                        filter,
                        cursorFactories.size() == 0,
                        indexDirection,
                        columnIndexes,
                        symbolFunction
                );
            }
        }
        cursorFactories.add(rowCursorFactory);
    }

    @Override
    protected RecordCursor getCursorInstance(DataFrameCursor dataFrameCursor, SqlExecutionContext sqlExecutionContext)
            throws SqlException {
        for (int i = 0, n = cursorFactories.size(); i < n; i++) {
            cursorFactories.getQuick(i).getFunction().init(dataFrameCursor, sqlExecutionContext);
        }

        if (followedOrderByAdvice && orderDirection == QueryModel.ORDER_DIRECTION_ASCENDING) {
            cursorFactories.sort(COMPARATOR);
        } else {
            cursorFactories.sort(COMPARATOR_DESC);
        }

        this.cursor.of(dataFrameCursor, sqlExecutionContext);
        if (filter != null) {
            filter.init(this.cursor, sqlExecutionContext);
        }
        return this.cursor;
    }

    private static final Comparator<FunctionBasedRowCursorFactory> COMPARATOR = FilterOnValuesRecordCursorFactory::compareStrFunctions;
    private static final Comparator<FunctionBasedRowCursorFactory> COMPARATOR_DESC = FilterOnValuesRecordCursorFactory::compareStrFunctionsDesc;

    private static int compareStrFunctions(FunctionBasedRowCursorFactory a, FunctionBasedRowCursorFactory b) {
        return Chars.compare(a.getFunction().getStr(null), b.getFunction().getStrB(null));
    }

    private static int compareStrFunctionsDesc(FunctionBasedRowCursorFactory a, FunctionBasedRowCursorFactory b) {
        return Chars.compareDescending(a.getFunction().getStr(null), b.getFunction().getStrB(null));
    }

}

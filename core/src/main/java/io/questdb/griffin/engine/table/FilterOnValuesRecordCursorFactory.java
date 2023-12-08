/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

public class FilterOnValuesRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private static final Comparator<FunctionBasedRowCursorFactory> COMPARATOR = FilterOnValuesRecordCursorFactory::compareStrFunctions;
    private static final Comparator<FunctionBasedRowCursorFactory> COMPARATOR_DESC = FilterOnValuesRecordCursorFactory::compareStrFunctionsDesc;
    private final int columnIndex;
    private final IntList columnIndexes;
    private final DataFrameRecordCursorImpl cursor;
    private final ObjList<FunctionBasedRowCursorFactory> cursorFactories;
    private final int[] cursorFactoriesIdx;
    private final Function filter;
    private final boolean followedOrderByAdvice;
    private final boolean heapCursorUsed;
    private final int orderDirection;
    private final RowCursorFactory rowCursorFactory;

    public FilterOnValuesRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            @NotNull @Transient ObjList<Function> keyValues,
            int columnIndex,
            @NotNull @Transient TableReader reader,
            @Nullable Function filter,
            int orderByMnemonic,
            boolean orderByKeyColumn,
            boolean orderByTimestamp,
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
        cursorFactoriesIdx = new int[]{0};
        final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndex);
        for (int i = 0; i < nKeyValues; i++) {
            final Function symbol = keyValues.get(i);
            if (symbol.isConstant()) {
                addSymbolKey(symbolMapReader.keyOf(symbol.getStr(null)), symbol, indexDirection);
            } else {
                addSymbolKey(SymbolTable.VALUE_NOT_FOUND, symbol, indexDirection);
            }
        }
        if (orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT && !orderByTimestamp) {
            heapCursorUsed = false;
            rowCursorFactory = new SequentialRowCursorFactory(cursorFactories, cursorFactoriesIdx);
        } else {
            heapCursorUsed = true;
            rowCursorFactory = new HeapRowCursorFactory(cursorFactories, cursorFactoriesIdx);
        }
        cursor = new DataFrameRecordCursorImpl(rowCursorFactory, false, filter, columnIndexes);
        this.followedOrderByAdvice = orderByKeyColumn || orderByTimestamp;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return followedOrderByAdvice;
    }

    @Override
    public int getScanDirection() {
        if (dataFrameCursorFactory.getOrder() == DataFrameCursorFactory.ORDER_ASC && heapCursorUsed) {
            return SCAN_DIRECTION_FORWARD;
        }
        return SCAN_DIRECTION_OTHER;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("FilterOnValues");
        if (!heapCursorUsed) { // sorting symbols makes no sense for heap factory
            sink.meta("symbolOrder").val(followedOrderByAdvice && orderDirection == QueryModel.ORDER_DIRECTION_ASCENDING ? "asc" : "desc");
        }
        sink.child(rowCursorFactory);
        sink.child(dataFrameCursorFactory);
    }

    @Override
    public boolean usesIndex() {
        return true;
    }

    private static int compareStrFunctions(FunctionBasedRowCursorFactory a, FunctionBasedRowCursorFactory b) {
        return Chars.compare(a.getFunction().getStr(null), b.getFunction().getStrB(null));
    }

    private static int compareStrFunctionsDesc(FunctionBasedRowCursorFactory a, FunctionBasedRowCursorFactory b) {
        return Chars.compareDescending(a.getFunction().getStr(null), b.getFunction().getStrB(null));
    }

    private static boolean equals(CharSequence cs1, CharSequence cs2) {
        if (cs1 == null) {
            return cs2 == null;
        } else {
            return cs2 != null && Chars.equals(cs1, cs2);
        }
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

    private void findDuplicates() {
        // bind variable actual values might repeat, so to remove adjacent duplicates
        // go through the list and if duplicate is found push it to the end of the list
        int idx = 0;
        int max = cursorFactories.size();

        OUT:
        while (idx < max - 1) {
            CharSequence symbol = symbol(idx);
            idx++;

            while (equals(symbol, symbol(idx))) {
                FunctionBasedRowCursorFactory tmp = cursorFactories.get(idx);
                cursorFactories.remove(idx);
                cursorFactories.add(tmp);

                idx++;
                max--;
                if (idx >= max) {
                    break OUT;
                }
            }
        }
        cursorFactoriesIdx[0] = max;
    }

    private CharSequence symbol(int idx) {
        return cursorFactories.get(idx).getFunction().getSymbol(null);
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(filter);
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        for (int i = 0, n = cursorFactories.size(); i < n; i++) {
            cursorFactories.getQuick(i).getFunction().init(dataFrameCursor, sqlExecutionContext);
        }

        // sort values to facilitate duplicate removal (even for heap row cursor)  
        // sorting here can produce order of cursorFactories different from one shown by explain command       
        if (followedOrderByAdvice && orderDirection == QueryModel.ORDER_DIRECTION_ASCENDING) {
            cursorFactories.sort(COMPARATOR);
        } else {
            cursorFactories.sort(COMPARATOR_DESC);
        }

        findDuplicates();

        cursor.of(dataFrameCursor, sqlExecutionContext);
        if (filter != null) {
            filter.init(cursor, sqlExecutionContext);
        }
        return cursor;
    }
}

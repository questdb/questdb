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

import io.questdb.cairo.SymbolMapReaderImpl;
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

public class FilterOnExcludedValuesRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {

    private final int columnIndex;
    private final IntList columnIndexes;
    private final Comparator<SymbolFunctionRowCursorFactory> comparator;
    private final Comparator<SymbolFunctionRowCursorFactory> comparatorDesc;
    private final DataFrameRecordCursorImpl cursor;
    private final ObjList<SymbolFunctionRowCursorFactory> cursorFactories;
    // Points at the next factory to be reused.
    private final int[] cursorFactoriesIdx;//used to disable unneeded factories if there are duplicate excluded keys 
    private final boolean dynamicExcludedKeys;
    private final IntHashSet excludedKeys = new IntHashSet();
    private final Function filter;
    private final boolean followedOrderByAdvice;
    private final boolean heapCursorUsed;
    private final IntHashSet includedKeys = new IntHashSet();
    private final int indexDirection;
    private final ObjList<Function> keyExcludedValueFunctions = new ObjList<>();
    private final int maxSymbolNotEqualsCount;
    private final int orderDirection;
    private SymbolMapReaderImpl symbolMapReader;

    public FilterOnExcludedValuesRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            @NotNull @Transient ObjList<Function> keyValues,
            int columnIndex,
            @Nullable Function filter,
            int orderByMnemonic,
            boolean orderByKeyColumn,
            boolean orderByTimestamp,
            int orderDirection,
            int indexDirection,
            @NotNull IntList columnIndexes,
            int maxSymbolNotEqualsCount
    ) {
        super(metadata, dataFrameCursorFactory);
        this.orderDirection = orderDirection;
        this.indexDirection = indexDirection;
        this.maxSymbolNotEqualsCount = maxSymbolNotEqualsCount;
        final int nKeyValues = keyValues.size();
        boolean dynamicValues = false;
        for (int i = 0; i < nKeyValues; i++) {
            if (!keyValues.getQuick(i).isConstant()) {
                dynamicValues = true;
                break;
            }
        }
        dynamicExcludedKeys = dynamicValues;
        keyExcludedValueFunctions.addAll(keyValues);
        this.columnIndex = columnIndex;
        this.filter = filter;
        cursorFactoriesIdx = new int[]{0};
        cursorFactories = new ObjList<>(nKeyValues);
        if (orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT && !orderByTimestamp) {
            heapCursorUsed = false;
            cursor = new DataFrameRecordCursorImpl(new SequentialRowCursorFactory(cursorFactories, cursorFactoriesIdx), false, filter, columnIndexes);
        } else {
            heapCursorUsed = true;
            cursor = new DataFrameRecordCursorImpl(new HeapRowCursorFactory(cursorFactories, cursorFactoriesIdx), false, filter, columnIndexes);
        }
        this.followedOrderByAdvice = orderByKeyColumn || orderByTimestamp;
        this.columnIndexes = columnIndexes;

        comparator = this::compareStrFunctions;
        comparatorDesc = this::compareStrFunctionsDesc;
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

    public void recalculateIncludedValues(TableReader tableReader) {
        cursorFactoriesIdx[0] = cursorFactories.size();
        excludedKeys.clear();
        if (dynamicExcludedKeys) {
            // In case of bind variable excluded values that may change between
            // query executions the sets have to be subtracted from scratch.
            includedKeys.clear();
            cursorFactoriesIdx[0] = 0;
        }
        try {
            symbolMapReader = (SymbolMapReaderImpl) tableReader.getSymbolMapReader(columnIndex);

            // Generate excluded key set.
            for (int i = 0, n = keyExcludedValueFunctions.size(); i < n; i++) {
                final CharSequence value = keyExcludedValueFunctions.getQuick(i).getStr(null);
                excludedKeys.add(symbolMapReader.keyOf(value));
            }

            // Append new keys to the included set filtering out the excluded ones.
            // Note: both includedKeys and cursorFactories are guaranteed to be monotonically
            // growing in terms of the collection size.
            for (int k = 0, n = symbolMapReader.getSymbolCount(); k < n; k++) {
                if (!excludedKeys.contains(k) && includedKeys.add(k)) {
                    upsertRowCursorFactory(k);
                }
            }

            if (symbolMapReader.containsNullValue()
                    && !excludedKeys.contains(SymbolTable.VALUE_IS_NULL) && includedKeys.add(SymbolTable.VALUE_IS_NULL)) {
                // If the table contains null values, and they're not excluded, we need to include
                // them to the result set to match the behavior of the NOT IN() SQL function.
                upsertRowCursorFactory(SymbolTable.VALUE_IS_NULL);
            }

            // sorting values makes no sense for heap row cursor
            if (!heapCursorUsed) {
                // sorting here can produce order of cursorFactories different from one shown by explain command
                if (followedOrderByAdvice && orderDirection == QueryModel.ORDER_DIRECTION_ASCENDING) {
                    cursorFactories.sort(0, cursorFactoriesIdx[0], comparator);
                } else {
                    cursorFactories.sort(0, cursorFactoriesIdx[0], comparatorDesc);
                }
            }
        } finally {
            symbolMapReader = null;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("FilterOnExcludedValues");
        if (!heapCursorUsed) {//sorting symbols makes no sense for heap factory
            sink.meta("symbolOrder").val(followedOrderByAdvice && orderDirection == QueryModel.ORDER_DIRECTION_ASCENDING ? "asc" : "desc");
        }
        sink.attr("symbolFilter").putBaseColumnName(columnIndex).val(" not in ").val(keyExcludedValueFunctions);
        sink.optAttr("filter", filter);
        sink.child(cursor.getRowCursorFactory());
        sink.child(dataFrameCursorFactory);
    }

    @Override
    public boolean usesIndex() {
        return true;
    }

    private int compareStrFunctions(SymbolFunctionRowCursorFactory a, SymbolFunctionRowCursorFactory b) {
        return Chars.compare(symbolMapReader.valueOf(a.getSymbolKey()), symbolMapReader.valueOf(b.getSymbolKey()));
    }

    private int compareStrFunctionsDesc(SymbolFunctionRowCursorFactory a, SymbolFunctionRowCursorFactory b) {
        return Chars.compareDescending(symbolMapReader.valueOf(a.getSymbolKey()), symbolMapReader.valueOf(b.getSymbolKey()));
    }

    private void upsertRowCursorFactory(int symbolKey) {
        if (cursorFactoriesIdx[0] < cursorFactories.size()) {
            // Reuse the existing factory.
            cursorFactories.get(cursorFactoriesIdx[0]).of(symbolKey);
            cursorFactoriesIdx[0]++;
            return;
        }

        // Create a new factory.
        final SymbolFunctionRowCursorFactory rowCursorFactory;
        if (filter == null) {
            rowCursorFactory = new SymbolIndexRowCursorFactory(
                    columnIndex,
                    symbolKey,
                    false,
                    indexDirection,
                    null
            );
        } else {
            rowCursorFactory = new SymbolIndexFilteredRowCursorFactory(
                    columnIndex,
                    symbolKey,
                    filter,
                    false,
                    indexDirection,
                    columnIndexes,
                    null
            );
        }
        cursorFactories.add(rowCursorFactory);
        cursorFactoriesIdx[0]++;
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(filter);
        Misc.freeObjList(keyExcludedValueFunctions);
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        TableReader reader = dataFrameCursor.getTableReader();
        if (reader.getSymbolMapReader(columnIndex).getSymbolCount() > maxSymbolNotEqualsCount) {
            throw TableReferenceOutOfDateException.of(reader.getTableToken().getTableName());
        }

        Function.init(keyExcludedValueFunctions, reader, executionContext);
        recalculateIncludedValues(reader);
        cursor.of(dataFrameCursor, executionContext);
        if (filter != null) {
            filter.init(cursor, executionContext);
        }
        return cursor;
    }
}

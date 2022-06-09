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
    private final ObjList<SymbolFunctionRowCursorFactory> cursorFactories;
    // Points at the next factory to be reused.
    private int cursorFactoriesIdx;
    private final ObjList<Function> keyExcludedValueFunctions = new ObjList<>();
    private final IntHashSet includedKeys = new IntHashSet();
    private final IntHashSet excludedKeys = new IntHashSet();
    private final boolean dynamicExcludedKeys;
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
        boolean dynamicValues = false;
        for (int i = 0; i < nKeyValues; i++) {
            if (!keyValues.getQuick(i).isConstant()) {
                dynamicValues = true;
                break;
            }
        }
        this.dynamicExcludedKeys = dynamicValues;
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
    public void _close() {
        Misc.free(filter);
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
        cursorFactoriesIdx = 0;
        excludedKeys.clear();
        if (dynamicExcludedKeys) {
            // In case of bind variable excluded values that may change between
            // query executions the sets have to be subtracted from scratch.
            includedKeys.clear();
        }

        final SymbolMapReaderImpl symbolMapReader = (SymbolMapReaderImpl) tableReader.getSymbolMapReader(columnIndex);

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
            // If the table contains null values and they're not excluded, we need to include
            // them to the result set to match the behavior of the NOT IN() SQL function.
            upsertRowCursorFactory(SymbolTable.VALUE_IS_NULL);
        }
    }

    private void upsertRowCursorFactory(int symbolKey) {
        if (cursorFactoriesIdx < cursorFactories.size()) {
            // Reuse the existing factory.
            cursorFactories.get(cursorFactoriesIdx).of(symbolKey);
            cursorFactoriesIdx++;
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
        cursorFactoriesIdx++;
    }

    @Override
    protected RecordCursor getCursorInstance(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext)
            throws SqlException {
        TableReader reader = dataFrameCursor.getTableReader();
        if (reader.getSymbolMapReader(columnIndex).getSymbolCount() > maxSymbolNotEqualsCount) {
            throw ReaderOutOfDateException.of(reader.getTableName());
        }
        Function.init(keyExcludedValueFunctions, reader, executionContext);
        this.recalculateIncludedValues(reader);
        this.cursor.of(dataFrameCursor, executionContext);
        if (filter != null) {
            filter.init(this.cursor, executionContext);
        }
        return this.cursor;
    }
}

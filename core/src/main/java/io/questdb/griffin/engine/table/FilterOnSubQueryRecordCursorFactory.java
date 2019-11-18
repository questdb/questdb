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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.engine.StrTypeCaster;
import io.questdb.griffin.engine.SymbolTypeCaster;
import io.questdb.griffin.engine.TypeCaster;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FilterOnSubQueryRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final DataFrameRecordCursor cursor;
    private final int columnIndex;
    private final Function filter;
    private final ObjList<RowCursorFactory> cursorFactories;
    private final IntObjHashMap<RowCursorFactory> factoriesA = new IntObjHashMap<>(64, 0.5, -5);
    private final IntObjHashMap<RowCursorFactory> factoriesB = new IntObjHashMap<>(64, 0.5, -5);
    private final RecordCursorFactory recordCursorFactory;
    private final TypeCaster typeCaster;
    private IntObjHashMap<RowCursorFactory> factories;

    public FilterOnSubQueryRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            @NotNull RecordCursorFactory recordCursorFactory,
            int columnIndex,
            @Nullable Function filter,
            int firstColumnType
    ) {
        super(metadata, dataFrameCursorFactory);
        this.recordCursorFactory = recordCursorFactory;
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.factories = factoriesA;
        cursorFactories = new ObjList<>();
        this.cursor = new DataFrameRecordCursor(new HeapRowCursorFactory(cursorFactories), filter, false);
        if (firstColumnType == ColumnType.SYMBOL) {
            typeCaster = SymbolTypeCaster.INSTANCE;
        } else {
            typeCaster = StrTypeCaster.INSTANCE;
        }
    }

    @Override
    public void close() {
        if (filter != null) {
            filter.close();
        }
        recordCursorFactory.close();
        factoriesA.clear();
        factoriesB.clear();
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        SymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndex);
        IntObjHashMap<RowCursorFactory> targetFactories;
        if (factories == factoriesA) {
            targetFactories = factoriesB;
        } else {
            targetFactories = factoriesA;
        }

        cursorFactories.clear();
        targetFactories.clear();

        try (RecordCursor cursor = recordCursorFactory.getCursor(executionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final CharSequence symbol = typeCaster.getValue(record, 0);
                int symbolKey = symbolTable.getQuick(symbol);
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {

                    final RowCursorFactory rowCursorFactory;
                    final int index = factories.keyIndex(symbolKey);
                    if (index < 0) {
                        rowCursorFactory = factories.valueAt(index);
                    } else {
                        if (filter == null) {
                            rowCursorFactory = new SymbolIndexRowCursorFactory(columnIndex, symbolKey, cursorFactories.size() == 0);
                        } else {
                            rowCursorFactory = new SymbolIndexFilteredRowCursorFactory(columnIndex, symbolKey, filter, cursorFactories.size() == 0);
                        }
                    }

                    final int targetIndex = targetFactories.keyIndex(symbolKey);
                    if (targetIndex > -1) {
                        targetFactories.putAt(targetIndex, symbolKey, rowCursorFactory);
                        cursorFactories.add(rowCursorFactory);
                    }
                }
            }
        }

        factories.clear();
        factories = targetFactories;

        if (targetFactories.size() == 0) {
            dataFrameCursor.close();
            return EmptyTableRecordCursor.INSTANCE;
        }

        this.cursor.of(dataFrameCursor, executionContext);
        return this.cursor;
    }
}

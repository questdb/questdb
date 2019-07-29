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

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.engine.EmptyTableRecordCursor;
import com.questdb.griffin.engine.StrTypeCaster;
import com.questdb.griffin.engine.SymbolTypeCaster;
import com.questdb.griffin.engine.TypeCaster;
import com.questdb.std.IntObjHashMap;
import com.questdb.std.ObjList;
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
        this.cursor = new DataFrameRecordCursor(new HeapRowCursorFactory(cursorFactories), filter);
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

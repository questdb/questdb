/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.sql.*;
import com.questdb.common.ColumnType;
import com.questdb.common.SymbolTable;
import com.questdb.std.IntHashSet;
import com.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FilterOnSubQueryRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final DataFrameRecordCursor cursor;
    private final int columnIndex;
    private final Function filter;
    private final ObjList<RowCursorFactory> cursorFactories;
    private final IntHashSet symbolKeys = new IntHashSet(16, 0.5, SymbolTable.VALUE_NOT_FOUND);
    private final RecordCursorFactory recordCursorFactory;
    private final TypeCaster typeCaster;

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

        cursorFactories = new ObjList<>();
        this.cursor = new DataFrameRecordCursor(new HeapRowCursorFactory(cursorFactories));
        if (firstColumnType == ColumnType.SYMBOL) {
            typeCaster = SymbolTypeCaster.INSTANCE;
        } else {
            typeCaster = StrTypeCaster.INSTANCE;
        }
    }

    @Override
    public void close() {
        recordCursorFactory.close();
    }

    private void addSymbolKey(int symbolKey) {
        final RowCursorFactory rowCursorFactory;
        if (filter == null) {
            rowCursorFactory = new SymbolIndexRowCursorFactory(columnIndex, symbolKey, cursorFactories.size() == 0);
        } else {
            rowCursorFactory = new SymbolIndexFilteredRowCursorFactory(columnIndex, symbolKey, filter, cursorFactories.size() == 0);
        }
        cursorFactories.add(rowCursorFactory);
    }

    @Override
    protected RecordCursor getCursorInstance(DataFrameCursor dataFrameCursor) {
        SymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndex);
        try (RecordCursor cursor = recordCursorFactory.getCursor()) {
            while (cursor.hasNext()) {
                Record record = cursor.next();
                // todo: this is also incorrect, we cannot expect query to return same symbols incrementally
                final CharSequence symbol = typeCaster.getValue(record, 0);
                int symbolKey = symbolTable.getQuick(symbol);
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    if (symbolKeys.add(symbolKey)) {
                        addSymbolKey(symbolKey);
                    }
                }
            }
        }

        if (symbolKeys.size() == 0) {
            dataFrameCursor.close();
            return EmptyTableRecordCursor.INSTANCE;
        }

        this.cursor.of(dataFrameCursor);
        return this.cursor;
    }

    @FunctionalInterface
    private interface TypeCaster {
        CharSequence getValue(Record record, int columnIndex);
    }

    private static class SymbolTypeCaster implements TypeCaster {
        private static final SymbolTypeCaster INSTANCE = new SymbolTypeCaster();

        @Override
        public CharSequence getValue(Record record, int columnIndex) {
            return record.getSym(columnIndex);
        }
    }

    private static class StrTypeCaster implements TypeCaster {
        private static final StrTypeCaster INSTANCE = new StrTypeCaster();

        @Override
        public CharSequence getValue(Record record, int columnIndex) {
            return record.getStr(columnIndex);
        }
    }
}

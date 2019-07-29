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

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.engine.StrTypeCaster;
import com.questdb.griffin.engine.SymbolTypeCaster;
import com.questdb.griffin.engine.TypeCaster;
import com.questdb.std.IntHashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LatestBySubQueryRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    private final int columnIndex;
    // this instance is shared between factory and cursor
    // factory will be resolving symbols for cursor and if successful
    // symbol keys will be added to this hash set
    private final IntHashSet symbolKeys = new IntHashSet();
    private final RecordCursorFactory recordCursorFactory;
    private final TypeCaster typeCaster;
    private final Function filter;

    public LatestBySubQueryRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @NotNull RecordCursorFactory recordCursorFactory,
            @Nullable Function filter,
            boolean indexed,
            int firstColumnType) {
        super(metadata, dataFrameCursorFactory, configuration);
        if (indexed) {
            if (filter != null) {
                this.cursor = new LatestByValuesIndexedFilteredRecordCursor(columnIndex, treeSet, symbolKeys, filter);
            } else {
                this.cursor = new LatestByValuesIndexedRecordCursor(columnIndex, treeSet, symbolKeys);
            }
        } else {
            if (filter != null) {
                this.cursor = new LatestByValuesFilteredRecordCursor(columnIndex, treeSet, symbolKeys, filter);
            } else {
                this.cursor = new LatestByValuesRecordCursor(columnIndex, treeSet, symbolKeys);
            }
        }
        this.columnIndex = columnIndex;
        this.recordCursorFactory = recordCursorFactory;
        if (firstColumnType == ColumnType.STRING) {
            typeCaster = StrTypeCaster.INSTANCE;
        } else {
            typeCaster = SymbolTypeCaster.INSTANCE;
        }
        this.filter = filter;
    }

    @Override
    public void close() {
        super.close();
        recordCursorFactory.close();
        if (filter != null) {
            filter.close();
        }
    }

    @Override
    protected AbstractDataFrameRecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        SymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndex);
        symbolKeys.clear();
        try (RecordCursor cursor = recordCursorFactory.getCursor(executionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                int symbolKey = symbolTable.getQuick(typeCaster.getValue(record, 0));
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(TableUtils.toIndexKey(symbolKey));
                }
            }
        }

        if (filter != null) {
            AbstractDataFrameRecordCursor cursor = super.getCursorInstance(dataFrameCursor, executionContext);
            filter.init(cursor, executionContext);
            return cursor;
        }
        return super.getCursorInstance(dataFrameCursor, executionContext);
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }
}

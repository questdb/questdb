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

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.*;
import com.questdb.common.SymbolTable;
import com.questdb.std.IntHashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LatestBySubQueryRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    private final AbstractDataFrameRecordCursor cursor;
    private final int columnIndex;
    // this instance is shared between factory and cursor
    // factory will be resolving symbols for cursor and if successful
    // symbol keys will be added to this hash set
    private final IntHashSet symbolKeys = new IntHashSet();
    private final RecordCursorFactory recordCursorFactory;

    public LatestBySubQueryRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @NotNull RecordCursorFactory recordCursorFactory,
            @Nullable Function filter,
            boolean indexed) {
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
    }

    @Override
    public void close() {
        super.close();
        recordCursorFactory.close();
    }

    @Override
    public RecordCursor getCursor() {
        final DataFrameCursor frameCursor = dataFrameCursorFactory.getCursor();
        SymbolTable symbolTable = frameCursor.getSymbolTable(columnIndex);
        symbolKeys.clear();
        try (RecordCursor cursor = recordCursorFactory.getCursor()) {
            while (cursor.hasNext()) {
                Record record = cursor.next();
                int symbolKey = symbolTable.getQuick(record.getSym(0));
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(symbolKey + 1);
                }
            }
        }
        cursor.of(frameCursor);
        return cursor;
    }
}

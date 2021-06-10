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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SingleSymbolFilterDataFrameRecordCursorFactory extends DataFrameRecordCursorFactory {
    private final int symbolColumnIndex;
    private final int symbolKey;
    private final int indexDirection;
    private final SingleSymbolFilter symbolFilter;
    private final Function filter;
    private final IntList columnIndexes;
    private final IntList columnSizes;
    private boolean convertedToFrame;
    private DataFrameRecordCursor convertedCursor;
    private TableReaderPageFrameCursor pageFrameCursor;

    public SingleSymbolFilterDataFrameRecordCursorFactory(
            int tableSymColIndex,
            int symbolKey,
            boolean cachedIndexReaderCursor,
            int indexDirection,
            RecordMetadata metadata,
            DataFrameCursorFactory dataFrameCursorFactory,
            boolean followsOrderByAdvice,
            @Nullable Function filter,
            @NotNull IntList columnIndexes,
            @Nullable IntList columnSizes
    ) {
        super(
                metadata,
                dataFrameCursorFactory,
                new SymbolIndexRowCursorFactory(tableSymColIndex, symbolKey, cachedIndexReaderCursor, indexDirection, null),
                followsOrderByAdvice, filter,
                false,
                columnIndexes,
                columnSizes);

        this.symbolKey = symbolKey;
        this.indexDirection = indexDirection;
        this.filter = filter;
        this.columnIndexes = columnIndexes;
        this.symbolColumnIndex = columnIndexes.indexOf(tableSymColIndex, 0, columnIndexes.size());
        this.columnSizes = columnSizes;
        this.symbolFilter = new SingleSymbolFilter() {
            @Override
            public int getColumnIndex() {
                return symbolColumnIndex;
            }

            @Override
            public int getSymbolFilterKey() {
                return TableUtils.toIndexKey(symbolKey);
            }
        };
    }

    public SingleSymbolFilter convertToSampleByIndexDataFrameCursorFactory() {
        if (!this.convertedToFrame) {
            this.convertedToFrame = true;
            DataFrameRowCursorFactory rowCursorFactory = new DataFrameRowCursorFactory();
            this.convertedCursor = new DataFrameRecordCursor(rowCursorFactory, true, filter, columnIndexes);
        }
        return symbolFilter;
    }

    public int getSampleByIndexKey() {
        return SymbolTable.VALUE_NOT_FOUND;
    }

    @Override
    public boolean supportPageFrameCursor() {
        return this.convertedToFrame;
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        if (convertedToFrame) {
            convertedCursor.of(dataFrameCursor, executionContext);
            return convertedCursor;
        } else {
            return super.getCursorInstance(dataFrameCursor, executionContext);
        }
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext) {
        if (!convertedToFrame) {
            return super.getPageFrameCursor(executionContext);
        }

        DataFrameCursor dataFrameCursor = dataFrameCursorFactory.getCursor(executionContext);
        if (pageFrameCursor != null) {
            return pageFrameCursor.of(dataFrameCursor);
        } else {
            pageFrameCursor = new TableReaderPageFrameCursor(columnIndexes, columnSizes, getMetadata().getTimestampIndex());
        }
        return pageFrameCursor.of(dataFrameCursor);
    }
}

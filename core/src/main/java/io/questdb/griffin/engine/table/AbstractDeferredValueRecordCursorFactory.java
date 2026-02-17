/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRandomRecordCursor;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

abstract class AbstractDeferredValueRecordCursorFactory extends AbstractPageFrameRecordCursorFactory {
    protected final int columnIndex;
    protected final IntList columnIndexes;
    private final Function symbolFunc;
    protected Function filter;
    private AbstractLatestByValueRecordCursor cursor;

    public AbstractDeferredValueRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int columnIndex,
            Function symbolFunc,
            @Nullable Function filter,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(configuration, metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);
        this.columnIndex = columnIndex;
        this.symbolFunc = symbolFunc;
        this.filter = filter;
        this.columnIndexes = columnIndexes;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.optAttr("filter", filter);
        sink.attr("symbolFilter").putColumnName(columnIndex).val('=').val(symbolFunc);
        sink.child(partitionFrameCursorFactory);
    }

    private boolean lookupDeferredSymbol(PageFrameCursor pageFrameCursor) {
        final CharSequence symbol = symbolFunc.getStrA(null);
        final int newSymbolKey = pageFrameCursor.getSymbolTable(columnIndex).keyOf(symbol);
        if (newSymbolKey == SymbolTable.VALUE_NOT_FOUND) {
            pageFrameCursor.close();
            return true;
        }

        if (cursor != null) {
            cursor.setSymbolKey(newSymbolKey);
        } else {
            cursor = createCursorFor(newSymbolKey);
        }

        return false;
    }

    @Override
    protected void _close() {
        super._close();
        filter = Misc.free(filter);
        Misc.free(cursor);
    }

    protected abstract AbstractLatestByValueRecordCursor createCursorFor(int symbolKey);

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        symbolFunc.init(pageFrameCursor, executionContext);

        if (lookupDeferredSymbol(pageFrameCursor)) {
            if (recordCursorSupportsRandomAccess()) {
                return EmptyTableRandomRecordCursor.INSTANCE;
            }
            return EmptyTableRecordCursor.INSTANCE;
        }
        cursor.of(pageFrameCursor, executionContext);
        return cursor;
    }
}

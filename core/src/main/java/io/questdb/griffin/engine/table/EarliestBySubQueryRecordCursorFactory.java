/*+*****************************************************************************
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class EarliestBySubQueryRecordCursorFactory extends AbstractPageFrameRecordCursorFactory {
    private final int columnIndex;
    private final EarliestByValueListRecordCursor cursor;
    private final Function filter;
    private final Record.CharSequenceFunction func;
    private final RecordCursorFactory subQueryFactory;

    public EarliestBySubQueryRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int columnIndex,
            @NotNull RecordCursorFactory subQueryFactory,
            @Nullable Function filter,
            @NotNull Record.CharSequenceFunction func,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);
        this.subQueryFactory = subQueryFactory;
        this.filter = filter;
        this.columnIndex = columnIndex;
        this.func = func;
        this.cursor = new EarliestByValueListRecordCursor(
                configuration,
                metadata,
                columnIndex,
                filter,
                configuration.getDefaultSymbolCapacity(),
                true,
                false
        );
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("EarliestBySubQuery");
        sink.child("Subquery", subQueryFactory);
        sink.child(partitionFrameCursorFactory);
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(subQueryFactory);
        Misc.free(filter);
        Misc.free(cursor);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final IntHashSet symbolKeys = cursor.getIncludedSymbolKeys();
        symbolKeys.clear();
        final StaticSymbolTable symbolTable = pageFrameCursor.getSymbolTable(columnIndex);
        try (RecordCursor subCursor = subQueryFactory.getCursor(executionContext)) {
            final Record record = subCursor.getRecord();
            final StringSink sink = Misc.getThreadLocalSink();
            while (subCursor.hasNext()) {
                int symbolKey = symbolTable.keyOf(func.get(record, 0, sink));
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(symbolKey);
                }
            }
        }
        cursor.of(pageFrameCursor, executionContext);
        return cursor;
    }
}

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

import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.DataFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;

public class SortedSymbolIndexRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final DataFrameRecordCursorImpl cursor;

    public SortedSymbolIndexRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            boolean columnOrderAsc,
            int indexDirection,
            @NotNull IntList columnIndexes
    ) {
        super(metadata, dataFrameCursorFactory);
        cursor = new DataFrameRecordCursorImpl(
                new SortedSymbolIndexRowCursorFactory(
                        columnIndex,
                        columnOrderAsc,
                        indexDirection,
                        columnIndexes
                ),
                true,
                null,
                columnIndexes
        );
    }

    @Override
    public boolean followedOrderByAdvice() {
        // the fact this factory is created means we are following order by advice
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    public void toPlan(PlanSink sink) {
        sink.type("SortedSymbolIndex");
        sink.child(cursor.getRowCursorFactory());
        sink.child(dataFrameCursorFactory);
    }

    @Override
    public boolean usesIndex() {
        return true;
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        cursor.of(dataFrameCursor, executionContext);
        return cursor;
    }
}

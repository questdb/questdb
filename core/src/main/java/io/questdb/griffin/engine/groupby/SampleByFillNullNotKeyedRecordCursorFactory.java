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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class SampleByFillNullNotKeyedRecordCursorFactory implements RecordCursorFactory {

    protected final RecordCursorFactory base;
    private final SampleByFillValueNotKeyedRecordCursor cursor;
    private final ObjList<Function> recordFunctions;
    private final RecordMetadata metadata;

    public SampleByFillNullNotKeyedRecordCursorFactory(
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            IntList symbolTableSkewIndex,
            int valueCount,
            int timestampIndex
    ) throws SqlException {
        try {
            this.base = base;
            this.metadata = groupByMetadata;
            this.recordFunctions = recordFunctions;
            final SimpleMapValue simpleMapValue = new SimpleMapValue(valueCount);
            this.cursor = new SampleByFillValueNotKeyedRecordCursor(
                    groupByFunctions,
                    recordFunctions,
                    SampleByFillNullRecordCursorFactory.createPlaceholderFunctions(recordFunctions),
                    timestampIndex,
                    timestampSampler,
                    symbolTableSkewIndex,
                    simpleMapValue
            );
        } catch (SqlException | CairoException e) {
            Misc.freeObjList(recordFunctions);
            throw e;
        }
    }

    @Override
    public void close() {
        Misc.freeObjList(recordFunctions);
        Misc.free(base);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        if (baseCursor.hasNext()) {
            return initFunctionsAndCursor(executionContext, baseCursor);
        }
        Misc.free(baseCursor);
        return EmptyTableRecordCursor.INSTANCE;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return false;
    }

    @NotNull
    protected RecordCursor initFunctionsAndCursor(SqlExecutionContext executionContext, RecordCursor baseCursor) {
        cursor.of(baseCursor);
        // init all record function for this cursor, in case functions require metadata and/or symbol tables
        for (int i = 0, m = recordFunctions.size(); i < m; i++) {
            recordFunctions.getQuick(i).init(cursor, executionContext);
        }
        return cursor;
    }
}

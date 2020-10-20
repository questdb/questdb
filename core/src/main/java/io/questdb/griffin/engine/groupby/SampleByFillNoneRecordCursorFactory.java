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

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableNoSizeRecordCursor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class SampleByFillNoneRecordCursorFactory implements RecordCursorFactory {
    protected final RecordCursorFactory base;
    protected final Map map;
    private final SampleByFillNoneRecordCursor cursor;
    private final ObjList<Function> recordFunctions;
    private final RecordMetadata metadata;

    public SampleByFillNoneRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata groupByMetadata,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull BytecodeAssembler asm,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            int timestampIndex
    ) {
        this.recordFunctions = recordFunctions;
        // sink will be storing record columns to map key
        final RecordSink mapSink = RecordSinkFactory.getInstance(asm, base.getMetadata(), listColumnFilter, false);
        // this is the map itself, which we must not forget to free when factory closes
        this.map = MapFactory.createMap(configuration, keyTypes, valueTypes);
        try {
            this.base = base;
            this.metadata = groupByMetadata;
            this.cursor = new SampleByFillNoneRecordCursor(
                    this.map,
                    mapSink,
                    groupByFunctions,
                    this.recordFunctions,
                    timestampIndex,
                    timestampSampler
            );
        } catch (CairoException e) {
            Misc.free(map);
            Misc.freeObjList(recordFunctions);
            throw e;
        }
    }

    @Override
    public void close() {
        Misc.freeObjList(recordFunctions);
        Misc.free(map);
        Misc.free(base);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            if (baseCursor.hasNext()) {
                map.clear();
                return initFunctionsAndCursor(executionContext, baseCursor);
            }

            return EmptyTableNoSizeRecordCursor.INSTANCE;
        } catch (CairoException ex) {
            baseCursor.close();
            throw ex;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @NotNull
    private RecordCursor initFunctionsAndCursor(SqlExecutionContext executionContext, RecordCursor baseCursor) {
        try {
            cursor.of(baseCursor, executionContext);
            // init all record function for this cursor, in case functions require metadata and/or symbol tables
            Function.init(recordFunctions, baseCursor, executionContext);
            return cursor;
        } catch (CairoException ex) {
            baseCursor.close();
            throw ex;
        }
    }
}

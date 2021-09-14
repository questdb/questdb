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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableNoSizeRecordCursor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class SampleByFillNoneRecordCursorFactory extends AbstractSampleByRecordCursorFactory {
    protected final Map map;
    private final SampleByFillNoneRecordCursor cursor;

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
            int timestampIndex,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos
    ) {
        super(base, groupByMetadata, recordFunctions);
        // sink will be storing record columns to map key
        final RecordSink mapSink = RecordSinkFactory.getInstance(asm, base.getMetadata(), listColumnFilter, false);
        // this is the map itself, which we must not forget to free when factory closes
        this.map = MapFactory.createMap(configuration, keyTypes, valueTypes);
        this.cursor = new SampleByFillNoneRecordCursor(
                this.map,
                mapSink,
                groupByFunctions,
                this.recordFunctions,
                timestampIndex,
                timestampSampler,
                timezoneNameFunc,
                timezoneNameFuncPos,
                offsetFunc,
                offsetFuncPos
        );
    }

    @Override
    public void close() {
        super.close();
        Misc.free(map);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public AbstractNoRecordSampleByCursor getRawCursor() {
        return cursor;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            if (baseCursor.hasNext()) {
                map.clear();
                return initFunctionsAndCursor(executionContext, baseCursor);
            }
            Misc.free(baseCursor);
            return EmptyTableNoSizeRecordCursor.INSTANCE;
        } catch (Throwable ex) {
            Misc.free(baseCursor);
            throw ex;
        }
    }
}

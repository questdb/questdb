/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class SampleByFillNullNotKeyedRecordCursorFactory extends AbstractSampleByNotKeyedRecordCursorFactory {

    private final SampleByFillValueNotKeyedRecordCursor cursor;

    public SampleByFillNullNotKeyedRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            @Transient @NotNull IntList recordFunctionPositions,
            int valueCount,
            int timestampIndex,
            int timestampType,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function sampleFromFunc,
            int sampleFromFuncPos,
            Function sampleToFunc,
            int sampleToFuncPos
    ) throws SqlException {
        super(base, groupByMetadata, recordFunctions);
        try {
            final SimpleMapValue simpleMapValue = new SimpleMapValue(valueCount);
            final SimpleMapValuePeeker peeker = new SimpleMapValuePeeker(simpleMapValue, new SimpleMapValue(valueCount));
            final GroupByFunctionsUpdater groupByFunctionsUpdater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            cursor = new SampleByFillValueNotKeyedRecordCursor(
                    configuration,
                    groupByFunctions,
                    groupByFunctionsUpdater,
                    recordFunctions,
                    SampleByFillNullRecordCursorFactory.createPlaceholderFunctions(recordFunctions, recordFunctionPositions),
                    peeker,
                    timestampIndex,
                    timestampType,
                    timestampSampler,
                    simpleMapValue,
                    timezoneNameFunc,
                    timezoneNameFuncPos,
                    offsetFunc,
                    offsetFuncPos,
                    sampleFromFunc,
                    sampleFromFuncPos,
                    sampleToFunc,
                    sampleToFuncPos
            );
            peeker.setCursor(cursor);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sample By");
        sink.attr("fill").val("null");
        if (cursor.sampleFromFunc != TimestampConstant.TIMESTAMP_MICRO_NULL || cursor.sampleToFunc != TimestampConstant.TIMESTAMP_MICRO_NULL)
            sink.attr("range").val('(').val(cursor.sampleFromFunc).val(',').val(cursor.sampleToFunc).val(')');
        sink.optAttr("values", cursor.groupByFunctions, true);
        sink.child(base);
    }

    protected AbstractNoRecordSampleByCursor getRawCursor() {
        return cursor;
    }
}

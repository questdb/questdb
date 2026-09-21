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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class SampleByFillPrevNotKeyedRecordCursorFactory extends AbstractSampleByNotKeyedRecordCursorFactory {
    private SampleByFillPrevNotKeyedRecordCursor cursor;
    private SimpleMapValue value;

    public SampleByFillPrevNotKeyedRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            int timestampIndex,
            int timestampType,
            int groupByValueCount,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function sampleFromFunc,
            int sampleFromFuncPos,
            Function sampleToFunc,
            int sampleToFuncPos
    ) {
        super(base, groupByMetadata, recordFunctions, timezoneNameFunc, offsetFunc, sampleFromFunc, sampleToFunc);
        try {
            final GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            this.value = new SimpleMapValue(groupByValueCount);
            this.cursor = new SampleByFillPrevNotKeyedRecordCursor(
                    configuration,
                    groupByFunctions,
                    updater,
                    recordFunctions,
                    timestampIndex,
                    timestampType,
                    timestampSampler,
                    value,
                    timezoneNameFunc,
                    timezoneNameFuncPos,
                    offsetFunc,
                    offsetFuncPos,
                    sampleFromFunc,
                    sampleFromFuncPos,
                    sampleToFunc,
                    sampleToFuncPos
            );
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sample By");
        sink.attr("fill").val("prev");
        sink.optAttr("values", cursor.groupByFunctions, true);
        sink.child(base);
    }

    @Override
    protected void _close() {
        final AbstractNoRecordSampleByCursor cursor = detachRawCursor();
        final SimpleMapValue value = this.value;
        this.value = null;
        Throwable failure = closeSampleByOwnersBestEffort(null);
        failure = Misc.freeBestEffort(failure, value);
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    protected AbstractNoRecordSampleByCursor detachRawCursor() {
        final SampleByFillPrevNotKeyedRecordCursor cursor = this.cursor;
        this.cursor = null;
        return cursor;
    }

    @Override
    protected AbstractNoRecordSampleByCursor getRawCursor() {
        return cursor;
    }
}

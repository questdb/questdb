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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ListColumnFilter;
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

public class SampleByFillPrevRecordCursorFactory extends AbstractSampleByFillRecordCursorFactory {
    private SampleByFillPrevRecordCursor cursor;

    public SampleByFillPrevRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
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
    ) {
        super(
                asm,
                configuration,
                base,
                listColumnFilter,
                keyTypes,
                valueTypes,
                groupByMetadata,
                groupByFunctions,
                recordFunctions,
                timezoneNameFunc,
                offsetFunc,
                sampleFromFunc,
                sampleToFunc
        );
        try {
            final GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            cursor = new SampleByFillPrevRecordCursor(
                    configuration,
                    map,
                    mapSink,
                    groupByFunctions,
                    updater,
                    recordFunctions,
                    timestampIndex,
                    timestampType,
                    timestampSampler,
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
            // The superclass already adopted the record functions, base factory, map, and
            // temporal parameter functions; the unreturned partial object would strand them.
            // close() frees everything except the map, which _close() reaches only through the
            // cursor - not constructed yet - so free it directly.
            Misc.free(map, th);
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sample By");
        sink.attr("fill").val("prev");
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("values", groupByFunctions, true);
        sink.child(base);
    }

    @Override
    protected AbstractNoRecordSampleByCursor detachRawCursor() {
        final SampleByFillPrevRecordCursor cursor = this.cursor;
        this.cursor = null;
        return cursor;
    }

    @Override
    protected AbstractNoRecordSampleByCursor getRawCursor() {
        return cursor;
    }
}

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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.ObjList;

public class SampleByFillValueNotKeyedRecordCursor extends AbstractSampleByFillRecordCursor {
    private final SimpleMapValuePeeker peeker;
    private final SimpleMapValue simpleMapValue;
    private boolean endFill = false;
    private boolean firstRun = true;
    private boolean gapFill = false;
    private long upperBound = Long.MAX_VALUE;

    public SampleByFillValueNotKeyedRecordCursor(
            CairoConfiguration configuration,
            ObjList<GroupByFunction> groupByFunctions,
            GroupByFunctionsUpdater groupByFunctionsUpdater,
            ObjList<Function> recordFunctions,
            ObjList<Function> placeholderFunctions,
            SimpleMapValuePeeker peeker,
            int timestampIndex, // index of timestamp column in base cursor
            int timestampType,
            TimestampSampler timestampSampler,
            SimpleMapValue simpleMapValue,
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
                configuration,
                recordFunctions,
                timestampIndex,
                timestampType,
                timestampSampler,
                groupByFunctions,
                groupByFunctionsUpdater,
                placeholderFunctions,
                timezoneNameFunc,
                timezoneNameFuncPos,
                offsetFunc,
                offsetFuncPos,
                sampleFromFunc,
                sampleFromFuncPos,
                sampleToFunc,
                sampleToFuncPos
        );
        this.simpleMapValue = simpleMapValue;
        record.of(simpleMapValue);
        this.peeker = peeker;
    }

    @Override
    public boolean hasNext() {
        initTimestamps();

        if (baseRecord == null && !gapFill && !endFill) {
            firstRun = true;
            return false;
        }

        // the next sample epoch could be different from current sample epoch due to DST transition,
        // e.g. clock going backward
        // we need to ensure we do not fill time transition
        long expectedLocalEpoch;
        if (firstRun) {
            expectedLocalEpoch = nextSampleLocalEpoch;
            firstRun = false;
        } else {
            expectedLocalEpoch = timestampSampler.nextTimestamp(nextSampleLocalEpoch);
        }
        // is data timestamp ahead of next expected timestamp?
        if (expectedLocalEpoch < localEpoch) {
            setActiveB(expectedLocalEpoch);
            sampleLocalEpoch = expectedLocalEpoch;
            nextSampleLocalEpoch = expectedLocalEpoch;
            return true;
        }

        if (endFill) {
            sampleLocalEpoch = expectedLocalEpoch;
            nextSampleLocalEpoch = expectedLocalEpoch;
            endFill = false;
            gapFill = false;

            return localEpoch < upperBound;
        }
        if (setActiveA(expectedLocalEpoch)) {
            return peeker.reset();
        }

        final boolean hasNext = notKeyedLoop(simpleMapValue);

        if (baseRecord == null && sampleToFunc != timestampDriver.getTimestampConstantNull() && !endFill) {
            endFill = true;
            upperBound = timestampDriver.from(sampleToFunc.getTimestamp(null), sampleToFuncType);
            // we must not re-initialize baseRecord after base cursor has been exhausted
            nextSamplePeriod(upperBound);
        }
        return hasNext;
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        super.of(baseCursor, executionContext);
        endFill = false;
        upperBound = Long.MAX_VALUE;
        firstRun = true;
        peeker.clear();
    }

    @Override
    public void toTop() {
        super.toTop();
        endFill = false;
        upperBound = Long.MAX_VALUE;
        firstRun = true;
        peeker.clear();
    }

    private boolean setActiveA(long expectedLocalEpoch) {
        if (gapFill) {
            gapFill = false;
            record.setActiveA();
            sampleLocalEpoch = expectedLocalEpoch;
            nextSampleLocalEpoch = expectedLocalEpoch;
            return true;
        }
        return false;
    }

    private void setActiveB(long expectedLocalEpoch) {
        if (!gapFill) {
            record.setActiveB(sampleLocalEpoch, expectedLocalEpoch, localEpoch);
            if (endFill) {
                record.setInterpolationTarget(null);
            } else {
                record.setInterpolationTarget(peeker.peek());
            }
            gapFill = true;
        }
    }
}

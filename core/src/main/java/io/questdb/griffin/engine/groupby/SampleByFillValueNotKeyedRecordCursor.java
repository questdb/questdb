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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.InterpolationGroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class SampleByFillValueNotKeyedRecordCursor extends AbstractSampleByFillRecordCursor {
    private final SimpleMapValuePeeker peeker;
    // the group-by functions of the FILL(PREV) columns, which serve as their own placeholders
    private final ObjList<GroupByFunction> prevFunctions = new ObjList<>();
    private final SimpleMapValue value;
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
            SimpleMapValue value,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function sampleFromFunc,
            int sampleFromFuncPos,
            Function sampleToFunc,
            int sampleToFuncPos,
            char samplingIntervalUnit
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
                sampleToFuncPos,
                samplingIntervalUnit
        );
        this.peeker = peeker;
        this.value = value;
        record.of(value);
        for (int i = 0, n = placeholderFunctions.size(); i < n; i++) {
            if (placeholderFunctions.getQuick(i) instanceof GroupByFunction function && !(function instanceof InterpolationGroupByFunction)) {
                prevFunctions.add(function);
            }
        }
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
            // the bucket that TO ends can follow the last bucket with rows without a gap
            setActiveB(expectedLocalEpoch);
            sampleLocalEpoch = expectedLocalEpoch;
            nextSampleLocalEpoch = expectedLocalEpoch;
            endFill = false;
            gapFill = false;
            return expectedLocalEpoch < upperBound;
        }
        final boolean hasNext;
        if (setActiveA()) {
            // the peek after a gap may have read the last row, so the end fill can start after this bucket
            hasNext = peeker.reset();
        } else {
            hasNext = notKeyedLoop(value);
        }

        if (baseRecord == null && !endFill) {
            final long to = sampleToFunc.getTimestamp(null);
            if (to != Numbers.LONG_NULL) {
                endFill = true;
                upperBound = timestampSampler.localAnchorAsUtc(localBound(timestampDriver.from(to, sampleToFuncType)));
                // we must not re-initialize baseRecord after base cursor has been exhausted
                nextSamplePeriod(upperBound);
            }
        }
        return hasNext;
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        super.of(baseCursor, executionContext);
        endFill = false;
        gapFill = false;
        upperBound = Long.MAX_VALUE;
        firstRun = true;
        peeker.clear();
        setPrevValuesToNull();
        record.setActiveA();
    }

    @Override
    public void toTop() {
        super.toTop();
        endFill = false;
        gapFill = false;
        upperBound = Long.MAX_VALUE;
        firstRun = true;
        peeker.clear();
        setPrevValuesToNull();
        record.setActiveA();
    }

    // peeker.reset() restores the bucket start that the loop left
    private boolean setActiveA() {
        if (gapFill) {
            gapFill = false;
            record.setActiveA();
            return true;
        }
        return false;
    }

    // The end fill has no bucket to interpolate towards: a LINEAR column shows the last bucket's value.
    private void setActiveB(long expectedLocalEpoch) {
        if (!gapFill) {
            if (endFill) {
                record.setActiveB();
            } else {
                record.setActiveB(sampleLocalEpoch, expectedLocalEpoch, localEpoch);
                record.setInterpolationTarget(peeker.peek());
            }
            gapFill = true;
        }
    }

    // The buckets between FROM and the first row read the zeroed value; the PREV columns show NULL there. LINEAR
    // columns keep the zeroed slots, as interpolating from a NULL sentinel yields garbage.
    private void setPrevValuesToNull() {
        for (int i = 0, n = prevFunctions.size(); i < n; i++) {
            prevFunctions.getQuick(i).setNull(value);
        }
    }
}

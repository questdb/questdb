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
    // true when the stride is in hours, minutes, seconds or a smaller unit, also when it spans a day or more,
    // such as 24h: the end fill then steps from nextSampleLocalEpoch, also after kludge() moved it past a DST
    // transition, see hasNext()
    private final boolean isSubDayUnit;
    private final SimpleMapValuePeeker peeker;
    // the group-by functions of the FILL(PREV) columns, see setPrevValuesToNull()
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
            boolean isFromToUtc,
            boolean isSubDayUnit
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
                isFromToUtc
        );
        this.peeker = peeker;
        this.value = value;
        this.isSubDayUnit = isSubDayUnit;
        record.of(value);
        // A FILL(PREV) column uses its group-by function as the placeholder, unlike FILL(NULL), FILL(value)
        // and FILL(LINEAR), see SampleByFillValueRecordCursorFactory.createPlaceholderFunctions()
        for (int i = 0, n = placeholderFunctions.size(); i < n; i++) {
            final Function function = placeholderFunctions.getQuick(i);
            if (function instanceof GroupByFunction groupByFunction && !(function instanceof InterpolationGroupByFunction)) {
                prevFunctions.add(groupByFunction);
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

        // An execution that samples on a local grid although code generation converted a bound to UTC
        // (isUtcBoundOnLocalGrid) skips three end fill repairs below: setActiveB() on the bucket that TO ends,
        // the check that this bucket starts before TO, and the start of the end fill after the bucket that
        // follows a gap. With TO converted, such an execution ends the fill at a UTC TO, which GitHub issue #7743
        // tracks. These repairs assume one time coordinate for the grid and TO. Applied there, the repairs would
        // turn wrong rows into other wrong rows, and with TO converted alone, some statements that return the
        // rows of the GROUP BY path would stop doing so. The check for a TO that reads NULL, below, and the end
        // fill of a FILL(LINEAR) column, see setActiveB(), apply to every execution.
        if (endFill) {
            // The bucket that TO ends can follow the last bucket with rows without a gap. No gap then
            // switched the record to the fill values, and the bucket showed the aggregates of the bucket
            // before it. setActiveB() switches it, and with endFill still set, it peeks no bucket past the
            // last row and starts no interpolation.
            if (!isUtcBoundOnLocalGrid) {
                setActiveB(expectedLocalEpoch);
            }
            sampleLocalEpoch = expectedLocalEpoch;
            nextSampleLocalEpoch = expectedLocalEpoch;
            endFill = false;
            gapFill = false;

            // The bucket comes out only when it starts before TO. When TO falls inside the last bucket with
            // rows, nextSamplePeriod() rounds TO down to the start of that bucket, and the bucket after it
            // starts after TO.
            return (isUtcBoundOnLocalGrid ? localEpoch : expectedLocalEpoch) < upperBound;
        }
        final boolean hasNext;
        if (setActiveA()) {
            // The bucket after a gap, which peeker.peek() aggregated. The peek may have read the last row,
            // and the end fill below then starts after this bucket, as it does after notKeyedLoop().
            hasNext = peeker.reset();
            if (isUtcBoundOnLocalGrid) {
                return hasNext;
            }
        } else {
            hasNext = notKeyedLoop(value);
        }

        if (baseRecord == null && !endFill) {
            // A TO that reads NULL, such as TO NULL, a NULL DECLARE variable or a NULL bind variable, sets no
            // upper bound, so the end fill does not start. nextSamplePeriod() would round a NULL bound to a bucket
            // near the end of the timestamp range, and the gap fill would emit every bucket up to there. Any other
            // TO sets the upper bound, see getUpperBound() for its time coordinate.
            final long to = sampleToFunc.getTimestamp(null);
            if (to != Numbers.LONG_NULL) {
                endFill = true;
                upperBound = getUpperBound(timestampDriver.from(to, sampleToFuncType));
                // The end fill steps from nextSampleLocalEpoch, which holds the start of the last bucket with rows
                // on the grid, unless a row of that bucket came after a DST transition. kludge() then moved the label
                // of the bucket by the offset delta and set nextSampleLocalEpoch to the moved label. For a stride in
                // days, weeks, months or years, the grid follows the local calendar, and the moved label falls off
                // it. Stepping from it, the month and year samplers would round it back into the last bucket and fill
                // that bucket again, and the day and week samplers would label the end fill off local midnight by the
                // offset delta. The end fill of such a stride steps from the start of the last bucket on the grid
                // instead, and the buckets after it take the offset after the transition. A stride in hours, minutes,
                // seconds or a smaller unit (isSubDayUnit) steps from the moved label, also when it spans a day or
                // more, such as 24h, whose sampler equals that of 1d: the GROUP BY path samples such a stride on a
                // UTC grid, not on the local calendar, and this cursor samples it in the time zone only when code
                // generation did not convert FROM to UTC, converted a bound that this cursor reads from a runtime
                // constant, or converted FROM with another time zone than the execution reads, see
                // AbstractNoRecordSampleByCursor.of(). Code generation converts a bound only for such a stride, so an
                // execution with isUtcBoundOnLocalGrid, which skips the repairs above, steps from
                // nextSampleLocalEpoch as well.
                if (!isSubDayUnit) {
                    nextSampleLocalEpoch = getGridLocalEpoch();
                }
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

    // peeker.reset() sets the start of the bucket after the gap and the bucket that the gap check expects
    // next, see SimpleMapValuePeeker.reset()
    private boolean setActiveA() {
        if (gapFill) {
            gapFill = false;
            record.setActiveA();
            return true;
        }
        return false;
    }

    // Switches the record to the fill values. Between two buckets with rows, a FILL(LINEAR) column interpolates
    // towards the bucket after the gap, which peeker.peek() aggregates. The end fill, from the last bucket with
    // rows up to TO, has no bucket to interpolate towards, and an interpolation without one read a NULL record.
    // A FILL(LINEAR) column of the end fill therefore starts no interpolation, and it shows the value of the
    // last bucket with rows, which the record still reads.
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

    // hasNext() emits the gap buckets between FROM and the first row from the value before peeker.reset()
    // copies the first bucket into it, and the FILL(PREV) columns read that value. peeker.clear() zeroes
    // it, so those buckets showed zero, the first symbol key, or the first histogram of approx_percentile()
    // where the GROUP BY path shows NULL. Each FILL(PREV) function writes NULL into its own slots, as
    // SampleByFillPrevNotKeyedRecordCursor does for every function. FILL(LINEAR) columns keep the zeroed
    // slots: interpolating from a NULL long or int sentinel yields garbage.
    private void setPrevValuesToNull() {
        for (int i = 0, n = prevFunctions.size(); i < n; i++) {
            prevFunctions.getQuick(i).setNull(value);
        }
    }
}

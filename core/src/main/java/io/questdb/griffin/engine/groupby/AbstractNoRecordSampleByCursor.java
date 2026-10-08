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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractNoRecordSampleByCursor extends AbstractSampleByCursor {
    protected final ObjList<GroupByFunction> groupByFunctions;
    protected final GroupByFunctionsUpdater groupByFunctionsUpdater;
    protected final int timestampIndex;
    private final GroupByAllocator allocator;
    // true when code generation converted to UTC a bound that this cursor reads, see
    // SqlCodeGenerator.generateSampleBy()
    private final boolean isFromToUtc;
    private final ObjList<Function> recordFunctions;
    protected RecordCursor baseCursor;
    protected Record baseRecord;
    protected SqlExecutionCircuitBreaker circuitBreaker;
    // this epoch is generally the same as `sampleLocalEpoch` except for cases where
    // sampler passed thru Daytime Savings Transition date
    // diverging values tell `filling` implementations not to fill this gap
    protected long nextSampleLocalEpoch;
    protected long sampleLocalEpoch;
    protected long topTzOffset;
    private boolean areTimestampsInitialized;
    private boolean isNotKeyedLoopInitialized;
    // the amount that nextSamplePeriod() added to localEpoch to label the bucket, see getGridLocalEpoch()
    private long localEpochShift;
    private long rowId;
    private long topLocalEpoch;
    private long topNextDst;

    public AbstractNoRecordSampleByCursor(
            CairoConfiguration configuration,
            ObjList<Function> recordFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            int timestampType,
            TimestampSampler timestampSampler,
            ObjList<GroupByFunction> groupByFunctions,
            GroupByFunctionsUpdater groupByFunctionsUpdater,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function sampleFromFunc,
            int sampleFromFuncPos,
            Function sampleToFunc,
            int sampleToFuncPos,
            boolean isFromToUtc
    ) {
        super(
                timestampSampler,
                timestampType,
                timezoneNameFunc,
                timezoneNameFuncPos,
                offsetFunc,
                offsetFuncPos,
                sampleFromFunc,
                sampleFromFuncPos,
                sampleToFunc,
                sampleToFuncPos
        );
        this.timestampIndex = timestampIndex;
        this.recordFunctions = recordFunctions;
        this.groupByFunctions = groupByFunctions;
        this.groupByFunctionsUpdater = groupByFunctionsUpdater;
        this.isFromToUtc = isFromToUtc;
        // Lazy variant: the allocator's chunk index is not allocated until the
        // first cursor's of() binds a MemoryTracker and calls reopen(), keeping
        // per-query alloc/free accounting symmetric from the very first cursor.
        this.allocator = GroupByAllocatorFactory.createAllocator(configuration, false);
        GroupByUtils.setAllocator(groupByFunctions, allocator);
    }

    @Override
    public void close() {
        baseCursor = Misc.free(baseCursor);
        Misc.free(allocator);
        Misc.clearObjList(groupByFunctions);
        circuitBreaker = null;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) recordFunctions.getQuick(columnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) recordFunctions.getQuick(columnIndex)).newSymbolTable();
    }

    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        this.baseCursor = baseCursor;
        baseRecord = baseCursor.getRecord();
        prevDst = Long.MIN_VALUE;
        parseParams(baseCursor, executionContext);
        // toTop() restores tzOffset from topTzOffset. initTimestamps() saves it on the first read,
        // but a caller such as LIMIT rewinds the cursor before that, and only a time zone name has
        // rules to recompute the offset from. Save the numeric offset that parseParams() derived,
        // unless code generation converted to UTC a bound that this cursor reads (isFromToUtc):
        // FROM, which initTimestamps() reads, or TO, which only the FILL(NULL) and FILL(value)
        // cursor reads, in its end fill. Such a cursor rewinds to a zero offset instead. It adds the
        // time zone offset on top of the converted bound, and neither offset gives the right rows
        // for every such statement; GitHub issue #7743 tracks that root cause.
        topTzOffset = isFromToUtc ? 0 : tzOffset;
        topNextDst = nextDstUtc;
        circuitBreaker = executionContext.getCircuitBreaker();
        // Consult the breaker at open, so an empty base scan (whose row loops never run) stays cancellable.
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        rowId = 0;
        isNotKeyedLoopInitialized = false;
        areTimestampsInitialized = false;
        sampleFromFunc.init(baseCursor, executionContext);
        sampleToFunc.init(baseCursor, executionContext);
        allocator.setMemoryTracker(executionContext.getMemoryTracker());
        allocator.reopen();
    }

    @Override
    public long preComputedStateSize() {
        return baseCursor.preComputedStateSize();
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        GroupByUtils.toTop(recordFunctions);
        baseCursor.toTop();
        localEpoch = topLocalEpoch;
        sampleLocalEpoch = nextSampleLocalEpoch = topLocalEpoch;
        // timezone offset is liable to change when we pass over DST edges
        tzOffset = topTzOffset;
        prevDst = Long.MIN_VALUE;
        nextDstUtc = topNextDst;
        baseRecord = baseCursor.getRecord();
        rowId = 0;
        isNotKeyedLoopInitialized = false;
        areTimestampsInitialized = false;
    }

    private void kludge(long newTzOffset) {
        // time moved forward, we need to make sure we move our sample boundary
        sampleLocalEpoch += (newTzOffset - tzOffset);
        nextSampleLocalEpoch = sampleLocalEpoch;
        tzOffset = newTzOffset;
    }

    // Makes TimestampFunc return NULL for the bucket that goes out next. The function subtracts tzOffset from
    // sampleLocalEpoch, and long arithmetic wraps, so the difference is Long.MIN_VALUE whatever the offset. The
    // bucket after it assigns sampleLocalEpoch from localEpoch before it emits a row.
    private void setNullBucketLabel() {
        sampleLocalEpoch = Numbers.LONG_NULL + tzOffset;
    }

    protected long adjustDst(long timestamp, @Nullable MapValue mapValue, long nextSampleTimestamp) {
        final long utcTimestamp = timestamp - tzOffset;
        if (utcTimestamp < nextDstUtc) {
            return timestamp;
        }
        final long newTzOffset = rules.getOffset(utcTimestamp);
        prevDst = nextDstUtc;
        nextDstUtc = rules.getNextDST(utcTimestamp);
        // check if DST takes this timestamp back "before" the nextSampleTimestamp
        if (timestamp - (tzOffset - newTzOffset) < nextSampleTimestamp) {
            // time moved backwards, we need to check if we should be collapsing this
            // hour into previous period or not
            updateValueWhenClockMovesBack(mapValue);
            nextSampleLocalEpoch = timestampSampler.round(timestamp);
            localEpoch = nextSampleLocalEpoch;
            sampleLocalEpoch += (newTzOffset - tzOffset);
            tzOffset = newTzOffset;
            return Long.MIN_VALUE;
        }
        kludge(newTzOffset);

        // time moved forward, we need to make sure we move our sample boundary
        return utcTimestamp + newTzOffset;
    }

    protected void adjustDstInFlight(long utcEpoch) {
        if (utcEpoch < nextDstUtc) {
            return;
        }
        final long daylightSavings = rules.getOffset(utcEpoch);
        prevDst = nextDstUtc;
        nextDstUtc = rules.getNextDST(utcEpoch);
        kludge(daylightSavings);
    }

    // Aggregates the rows at the head of the base cursor that have a NULL designated timestamp into a bucket of their
    // own, starting with the current row. Returns true when it stopped at the first row that has a timestamp, which
    // is then the current row of the base cursor, and false when the base cursor ran out. Only the cursors without
    // FILL override it: a grid that a cursor fills has no place for these rows, so the cursors with FILL fail.
    protected boolean aggregateNullTimestampRows() {
        throw CairoException.nonCritical().put("SAMPLE BY designated timestamp cannot be NULL");
    }

    protected long getBaseRecordTimestamp() {
        return baseRecord.getTimestamp(timestampIndex) + tzOffset;
    }

    // Returns the start of the current bucket on the sampler grid, in the local time of the current
    // offset. It differs from localEpoch only after nextSamplePeriod() shifted localEpoch to label a
    // bucket that starts before the DST transition just crossed. The end of the bucket, and the bucket
    // that the gap check expects after it, derive from the grid start. Derived from the shifted start,
    // they moved by the offset delta: after a fall-back, the bucket ended before rows that belong to it,
    // and such a row rounded back to the same bucket without end (GitHub issue #7752); after a
    // spring-forward, the bucket took in rows of the next one.
    protected long getGridLocalEpoch() {
        return localEpoch - localEpochShift;
    }

    // Reads the first row of the base cursor and starts the sampler grid, the time zone offset and the first bucket
    // from its timestamp. Returns true when the call also aggregated the NULL bucket, which the caller emits before
    // it reads on, see aggregateNullTimestampRows(). Returns false in every other case, and always for the cursors
    // with FILL.
    protected boolean initTimestamps() {
        if (areTimestampsInitialized) {
            return false;
        }

        if (!baseCursor.hasNext()) {
            baseRecord = null;
            return false;
        }

        long timestamp = baseRecord.getTimestamp(timestampIndex);
        // TIMESTAMP(col) can designate a column that holds NULL, and an ascending base puts the rows with the NULL
        // first. No bucket of the grid can hold them. With ALIGN TO FIRST OBSERVATION, the grid would start at
        // Long.MIN_VALUE, and the fill cursors would walk it one stride at a time towards the next row. On a calendar
        // grid that starts at 0 with no time zone offset, the week samplers, and SimpleTimestampSampler unless the
        // stride is a power of two in the unit of the timestamp (microseconds or nanoseconds), would round
        // Long.MIN_VALUE down past the range of a long and wrap around into a bucket near Long.MAX_VALUE, which takes
        // in every row. So these rows never reach the sampler or the time zone rules. The cursors with FILL fail. The
        // cursors without FILL aggregate the rows into a bucket of their own and emit it first under a NULL label, as
        // the GROUP BY path does. The grid, the time zone offset and the DST state then start from the first row that
        // has a timestamp, exactly as they would without the NULL rows.
        final boolean hasNullBucket = timestamp == Numbers.LONG_NULL;
        if (hasNullBucket) {
            if (!aggregateNullTimestampRows()) {
                // Every row is in the NULL bucket, so there is no grid to start. A rewind or the next execution
                // clears the flag, as for any other source.
                baseRecord = null;
                setNullBucketLabel();
                areTimestampsInitialized = true;
                return true;
            }
            timestamp = baseRecord.getTimestamp(timestampIndex);
        }

        if (rules != null) {
            tzOffset = rules.getOffset(timestamp);
            nextDstUtc = rules.getNextDST(timestamp);
        }

        long from = Long.MIN_VALUE;
        if (tzOffset == 0 && fixedOffset == Long.MIN_VALUE) {
            // this is the default path, we align time intervals to the first observation
            timestampSampler.setStart(timestamp);
        } else {
            // FROM-TO may apply to align to calendar queries, fixing the lower bound.
            if (sampleFromFunc != timestampDriver.getTimestampConstantNull()) {
                from = sampleFromFunc.getTimestamp(null);
                timestampSampler.setStart(from != Long.MIN_VALUE ? timestampDriver.from(from, sampleFromFuncType) : 0);
            } else {
                timestampSampler.setOffset(fixedOffset != Long.MIN_VALUE ? fixedOffset : 0);
            }
        }

        topTzOffset = tzOffset;
        topNextDst = nextDstUtc;
        if (from != Long.MIN_VALUE) {
            // Set the top epoch to the bucket at FROM, in local time. For a stride of a day or longer,
            // code generation leaves FROM in local time, and the sampler grid starts at FROM. Adding the
            // offset would read that local FROM as a UTC instant: a zone behind UTC then moved it into the
            // bucket before FROM, and the month and year samplers, which floor, emitted that extra bucket
            // (GitHub issue #7763). Only a FROM that code generation converted to UTC takes the offset, see
            // #7743. Code generation converts FROM whenever it converts TO, so with FROM set, isFromToUtc
            // means that FROM is in UTC.
            final long fromTimestamp = timestampDriver.from(from, sampleFromFuncType);
            topLocalEpoch = timestampSampler.round(isFromToUtc ? fromTimestamp + tzOffset : fromTimestamp);
            // set current epoch to be the floor of the starting timestamp
            localEpoch = timestampSampler.round(timestamp + tzOffset);
        } else {
            topLocalEpoch = localEpoch = timestampSampler.round(timestamp + tzOffset);
        }
        localEpochShift = 0;
        sampleLocalEpoch = nextSampleLocalEpoch = topLocalEpoch;
        if (hasNullBucket) {
            // The NULL bucket ends as nextSamplePeriod() ends a bucket of the grid. The label goes in last, after
            // the offset of the first row with a timestamp replaced the offset that of() left.
            GroupByUtils.toTop(groupByFunctions);
            setNullBucketLabel();
        }
        areTimestampsInitialized = true;
        return hasNullBucket;
    }

    protected void nextSamplePeriod(long timestamp) {
        localEpoch = timestampSampler.round(timestamp);
        // After a DST transition, rounding down (common for multi-hour or day units) can place
        // the new bucket's boundary at a local time whose UTC instant lies before the transition
        // we just crossed. TimestampFunc emits `sampleLocalEpoch - tzOffset`, so if we leave
        // localEpoch as-is, we'd back-convert with the post-transition offset even though the
        // bucket start belongs to the pre-transition offset. Shift localEpoch by the delta
        // between the current offset and the one valid at the bucket boundary so the emitted
        // UTC timestamp lands on the correct side of the transition. The bucket end derives from the
        // unshifted start, see getGridLocalEpoch().
        localEpochShift = 0;
        if (rules != null && localEpoch - tzOffset < prevDst) {
            final long boundaryTzOffset = rules.getOffset(localEpoch - tzOffset);
            localEpochShift = tzOffset - boundaryTzOffset;
            // A spring-forward can skip the local start of the bucket. The shift then labels the bucket with
            // that start read in the offset before the change, which falls inside the bucket only when the
            // stride is longer than the change. Otherwise, the label lands at or after the end of the bucket,
            // and so at or after the label of the next bucket, and the bucket takes the first instant that it
            // covers, the change itself. The not-keyed FILL(NULL) and FILL(value) cursor also calls this method
            // with the TO bound once the rows run out (baseRecord is null), to find where its end fill stops. That
            // call labels no bucket and keeps the shifted bound.
            if (baseRecord != null && localEpoch + localEpochShift >= timestampSampler.nextTimestamp(localEpoch)) {
                localEpochShift = prevDst + tzOffset - localEpoch;
            }
            localEpoch += localEpochShift;
        }
        GroupByUtils.toTop(groupByFunctions);
    }

    protected boolean notKeyedLoop(MapValue mapValue) {
        if (!isNotKeyedLoopInitialized) {
            sampleLocalEpoch = localEpoch;
            nextSampleLocalEpoch = getGridLocalEpoch();
            // looks like we need to populate key map
            // at the start of this loop 'lastTimestamp' will be set to timestamp
            // of first record in base cursor
            groupByFunctionsUpdater.updateNew(mapValue, baseRecord, rowId++);
            isNotKeyedLoopInitialized = true;
        }

        long next = timestampSampler.nextTimestamp(getGridLocalEpoch());
        long timestamp;
        while (baseCursor.hasNext()) {
            timestamp = getBaseRecordTimestamp();
            if (timestamp < next) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                adjustDstInFlight(timestamp - tzOffset);
                groupByFunctionsUpdater.updateExisting(mapValue, baseRecord, rowId++);
            } else {
                // timestamp changed, make sure we keep the value of 'lastTimestamp'
                // unchanged. Timestamp column uses this variable.
                // When map is exhausted we would assign 'next' to 'lastTimestamp'
                // and build another map.
                timestamp = adjustDst(timestamp, mapValue, next);
                if (timestamp != Long.MIN_VALUE) {
                    nextSamplePeriod(timestamp);
                    isNotKeyedLoopInitialized = false;
                    return true;
                }
            }
        }
        // opportunity, after we stream map that's it
        baseRecord = null;
        isNotKeyedLoopInitialized = false;
        return true;
    }

    // The not-keyed form of aggregateNullTimestampRows(): aggregates the rows into mapValue, as notKeyedLoop()
    // aggregates the rows of a bucket of the grid.
    protected boolean notKeyedNullLoop(MapValue mapValue) {
        groupByFunctionsUpdater.updateNew(mapValue, baseRecord, rowId++);
        while (baseCursor.hasNext()) {
            if (baseRecord.getTimestamp(timestampIndex) != Numbers.LONG_NULL) {
                return true;
            }
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            groupByFunctionsUpdater.updateExisting(mapValue, baseRecord, rowId++);
        }
        return false;
    }

    protected void updateValueWhenClockMovesBack(MapValue value) {
        groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId++);
    }

    protected class TimestampFunc extends TimestampFunction implements Function {

        public TimestampFunc(int timestampType) {
            super(timestampType);
        }

        @Override
        public long getTimestamp(Record rec) {
            return sampleLocalEpoch - tzOffset;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("Timestamp");
        }
    }
}

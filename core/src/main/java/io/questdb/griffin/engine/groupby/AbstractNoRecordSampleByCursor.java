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
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractNoRecordSampleByCursor extends AbstractSampleByCursor {
    protected final ObjList<GroupByFunction> groupByFunctions;
    protected final GroupByFunctionsUpdater groupByFunctionsUpdater;
    protected final int timestampIndex;
    private final GroupByAllocator allocator;
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
            int sampleToFuncPos
    ) {
        super(timestampSampler, timestampType, timezoneNameFunc, timezoneNameFuncPos, offsetFunc, offsetFuncPos, sampleFromFunc, sampleFromFuncPos, sampleToFunc, sampleToFuncPos);
        this.timestampIndex = timestampIndex;
        this.recordFunctions = recordFunctions;
        this.groupByFunctions = groupByFunctions;
        this.groupByFunctionsUpdater = groupByFunctionsUpdater;
        this.allocator = GroupByAllocatorFactory.createAllocator(configuration);
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
        topNextDst = nextDstUtc;
        circuitBreaker = executionContext.getCircuitBreaker();
        rowId = 0;
        isNotKeyedLoopInitialized = false;
        areTimestampsInitialized = false;
        sampleFromFunc.init(baseCursor, executionContext);
        sampleToFunc.init(baseCursor, executionContext);
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

    protected long getBaseRecordTimestamp() {
        return baseRecord.getTimestamp(timestampIndex) + tzOffset;
    }

    protected void initTimestamps() {
        if (areTimestampsInitialized) {
            return;
        }

        if (!baseCursor.hasNext()) {
            baseRecord = null;
            return;
        }

        final long timestamp = baseRecord.getTimestamp(timestampIndex);

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
                timestampSampler.setStart(fixedOffset != Long.MIN_VALUE ? fixedOffset : 0);
            }
        }

        topTzOffset = tzOffset;
        topNextDst = nextDstUtc;
        if (from != Long.MIN_VALUE) {
            // set the top epoch to be the lower limit
            topLocalEpoch = timestampSampler.round(timestampDriver.from(from, sampleFromFuncType) + tzOffset);
            // set current epoch to be the floor of the starting timestamp
            localEpoch = timestampSampler.round(timestamp + tzOffset);
        } else {
            topLocalEpoch = localEpoch = timestampSampler.round(timestamp + tzOffset);
        }
        sampleLocalEpoch = nextSampleLocalEpoch = topLocalEpoch;
        areTimestampsInitialized = true;
    }

    protected void nextSamplePeriod(long timestamp) {
        localEpoch = timestampSampler.round(timestamp);
        // Sometimes rounding, especially around Days can throw localEpoch
        // to the "before" previous DST. When this happens we need to compensate for
        // tzOffset subtraction at the time of delivery of the timestamp to client
        if (localEpoch - tzOffset < prevDst) {
            localEpoch += tzOffset;
        }
        GroupByUtils.toTop(groupByFunctions);
    }

    protected boolean notKeyedLoop(MapValue mapValue) {
        if (!isNotKeyedLoopInitialized) {
            sampleLocalEpoch = localEpoch;
            nextSampleLocalEpoch = localEpoch;
            // looks like we need to populate key map
            // at the start of this loop 'lastTimestamp' will be set to timestamp
            // of first record in base cursor
            groupByFunctionsUpdater.updateNew(mapValue, baseRecord, rowId++);
            isNotKeyedLoopInitialized = true;
        }

        long next = timestampSampler.nextTimestamp(localEpoch);
        long timestamp;
        while (baseCursor.hasNext()) {
            timestamp = getBaseRecordTimestamp();
            if (timestamp < next) {
                circuitBreaker.statefulThrowExceptionIfTripped();
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

    protected void updateValueWhenClockMovesBack(MapValue value) {
        groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId);
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

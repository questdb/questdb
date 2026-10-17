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

public abstract class AbstractNoRecordSampleByCursor extends AbstractSampleByCursor {
    protected final ObjList<GroupByFunction> groupByFunctions;
    protected final GroupByFunctionsUpdater groupByFunctionsUpdater;
    protected final ObjList<Function> recordFunctions;
    protected final int timestampIndex;
    private final GroupByAllocator allocator;
    protected RecordCursor baseCursor;
    protected Record baseRecord;
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected long nextSampleLocalEpoch;
    protected long sampleLocalEpoch;
    private boolean areTimestampsInitialized;
    private boolean isNotKeyedLoopInitialized;
    private long rowId;
    private long topLocalEpoch;

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
            char samplingIntervalUnit
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
                sampleToFuncPos,
                samplingIntervalUnit
        );
        this.timestampIndex = timestampIndex;
        this.recordFunctions = recordFunctions;
        this.groupByFunctions = groupByFunctions;
        this.groupByFunctionsUpdater = groupByFunctionsUpdater;
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
        parseParams(baseCursor, executionContext);
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
        baseRecord = baseCursor.getRecord();
        rowId = 0;
        isNotKeyedLoopInitialized = false;
        areTimestampsInitialized = false;
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
        if (timestamp == Numbers.LONG_NULL) {
            throw CairoException.nonCritical().put("designated timestamp column cannot be NULL");
        }

        long from = Long.MIN_VALUE;
        if (tzOffset == 0 && fixedOffset == Long.MIN_VALUE) {
            // this is the default path, we align time intervals to the first observation
            timestampSampler.setStart(timestamp);
        } else {
            from = anchorCalendarGrid();
        }

        if (from != Long.MIN_VALUE) {
            // set the top epoch to be the lower limit
            topLocalEpoch = timestampSampler.round(timestampSampler.localAnchorAsUtc(localFrom(from)));
            // set current epoch to be the floor of the starting timestamp
            localEpoch = timestampSampler.round(timestamp + tzOffset);
        } else {
            topLocalEpoch = localEpoch = timestampSampler.round(timestamp + tzOffset);
        }
        sampleLocalEpoch = nextSampleLocalEpoch = topLocalEpoch;
        areTimestampsInitialized = true;
    }

    /**
     * Moves to the bucket of the timestamp, keeping the state the group-by functions hold for earlier buckets.
     */
    protected void moveToSamplePeriod(long timestamp) {
        localEpoch = timestampSampler.round(timestamp);
    }

    protected void nextSamplePeriod(long timestamp) {
        moveToSamplePeriod(timestamp);
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

        final long next = timestampSampler.nextTimestamp(localEpoch);
        while (baseCursor.hasNext()) {
            final long timestamp = getBaseRecordTimestamp();
            if (timestamp < next) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                groupByFunctionsUpdater.updateExisting(mapValue, baseRecord, rowId++);
            } else {
                nextSamplePeriod(timestamp);
                isNotKeyedLoopInitialized = false;
                return true;
            }
        }
        // opportunity, after we stream map that's it
        baseRecord = null;
        isNotKeyedLoopInitialized = false;
        return true;
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

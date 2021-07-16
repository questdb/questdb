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

import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractNoRecordSampleByCursor extends AbstractSampleByCursor {
    protected final int timestampIndex;
    protected final ObjList<GroupByFunction> groupByFunctions;
    private final ObjList<Function> recordFunctions;
    protected Record baseRecord;
    protected long sampleLocalEpoch;
    // this epoch is generally the same as `sampleLocalEpoch` except for cases where
    // sampler passed thru Daytime Savings Transition date
    // diverging values tell `filling` implementations not to fill this gap
    protected long nextSampleLocalEpoch;
    protected RecordCursor base;
    protected SqlExecutionInterruptor interruptor;
    protected long topTzOffset;
    private long topNextDst;
    private long topLocalEpoch;

    public AbstractNoRecordSampleByCursor(
            ObjList<Function> recordFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            TimestampSampler timestampSampler,
            ObjList<GroupByFunction> groupByFunctions,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos
    ) {
        super(timestampSampler, timezoneNameFunc, timezoneNameFuncPos, offsetFunc, offsetFuncPos);
        this.timestampIndex = timestampIndex;
        this.recordFunctions = recordFunctions;
        this.groupByFunctions = groupByFunctions;
    }

    @Override
    public void close() {
        base.close();
        interruptor = null;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) recordFunctions.getQuick(columnIndex);
    }

    @Override
    public void toTop() {
        GroupByUtils.toTop(recordFunctions);
        this.base.toTop();
        this.localEpoch = topLocalEpoch;
        this.sampleLocalEpoch = this.nextSampleLocalEpoch = topLocalEpoch;
        // timezone offset is liable to change when we pass over DST edges
        this.tzOffset = topTzOffset;
        this.prevDst = Long.MIN_VALUE;
        this.nextDstUTC = topNextDst;
    }

    @Override
    public long size() {
        return -1;
    }

    public void of(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
        this.prevDst = Long.MIN_VALUE;
        parseParams(base, executionContext);

        this.base = base;
        this.baseRecord = base.getRecord();
        final long timestamp = baseRecord.getTimestamp(timestampIndex);
        if (rules != null) {
            tzOffset = rules.getOffset(timestamp);
            nextDstUTC = rules.getNextDST(timestamp);
        }


        if (tzOffset == 0 && fixedOffset == Long.MIN_VALUE) {
            // this is the default path, we align time intervals to the first observation
            timestampSampler.setStart(timestamp);
        } else {
            timestampSampler.setStart(this.fixedOffset != Long.MIN_VALUE ? this.fixedOffset : 0L);
        }
        this.topTzOffset = tzOffset;
        this.topNextDst = nextDstUTC;
        this.topLocalEpoch = this.localEpoch = timestampSampler.round(timestamp + tzOffset);
        this.sampleLocalEpoch = this.nextSampleLocalEpoch = localEpoch;
        interruptor = executionContext.getSqlExecutionInterruptor();
    }

    protected long adjustDST(long timestamp, int n, @Nullable MapValue mapValue, long nextSampleTimestamp) {
        final long utcTimestamp = timestamp - tzOffset;
        if (utcTimestamp < nextDstUTC) {
            return timestamp;
        }
        final long newTzOffset = rules.getOffset(utcTimestamp);
        prevDst = nextDstUTC;
        nextDstUTC = rules.getNextDST(utcTimestamp);
        // check if DST takes this timestamp back "before" the nextSampleTimestamp
        if (timestamp - (tzOffset - newTzOffset) < nextSampleTimestamp) {
            // time moved backwards, we need to check if we should be collapsing this
            // hour into previous period or not
            updateValueWhenClockMovesBack(mapValue, n);
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

    protected void adjustDSTInFlight(long t) {
        if (t < nextDstUTC) {
            return;
        }
        final long daylightSavings = rules.getOffset(t);
        prevDst = nextDstUTC;
        nextDstUTC = rules.getNextDST(t);
        kludge(daylightSavings);
    }

    protected long getBaseRecordTimestamp() {
        return baseRecord.getTimestamp(timestampIndex) + tzOffset;
    }

    private void kludge(long newTzOffset) {
        // time moved forward, we need to make sure we move our sample boundary
        sampleLocalEpoch += (newTzOffset - tzOffset);
        nextSampleLocalEpoch = sampleLocalEpoch;
        tzOffset = newTzOffset;
    }

    protected void nextSamplePeriod(long timestamp) {
        this.localEpoch = timestampSampler.round(timestamp);
        // Sometimes rounding, especially around Days can throw localEpoch
        // to the "before" previous DST. When this happens we need to compensate for
        // tzOffset subtraction at the time of delivery of the timestamp to client
        if (localEpoch - tzOffset < prevDst) {
            localEpoch += tzOffset;
        }
        GroupByUtils.toTop(groupByFunctions);
    }

    protected boolean notKeyedLoop(MapValue mapValue) {
        long next = timestampSampler.nextTimestamp(this.localEpoch);
        this.sampleLocalEpoch = this.localEpoch;
        this.nextSampleLocalEpoch = this.localEpoch;
        // looks like we need to populate key map
        // at the start of this loop 'lastTimestamp' will be set to timestamp
        // of first record in base cursor
        int n = groupByFunctions.size();
        GroupByUtils.updateNew(groupByFunctions, n, mapValue, baseRecord);
        while (base.hasNext()) {
            long timestamp = getBaseRecordTimestamp();
            if (timestamp < next) {
                adjustDSTInFlight(timestamp - tzOffset);
                GroupByUtils.updateExisting(groupByFunctions, n, mapValue, baseRecord);
                interruptor.checkInterrupted();
            } else {
                // timestamp changed, make sure we keep the value of 'lastTimestamp'
                // unchanged. Timestamp columns uses this variable
                // When map is exhausted we would assign 'next' to 'lastTimestamp'
                // and build another map
                timestamp = adjustDST(timestamp, n, mapValue, next);
                if (timestamp != Long.MIN_VALUE) {
                    nextSamplePeriod(timestamp);
                    return true;
                }
            }
        }

        // opportunity, after we stream map that is.
        baseRecord = null;
        return true;
    }

    protected void updateValueWhenClockMovesBack(MapValue value, int n) {
        GroupByUtils.updateExisting(groupByFunctions, n, value, baseRecord);
    }

    protected class TimestampFunc extends TimestampFunction implements Function {

        @Override
        public long getTimestamp(Record rec) {
            return sampleLocalEpoch - tzOffset;
        }
    }
}

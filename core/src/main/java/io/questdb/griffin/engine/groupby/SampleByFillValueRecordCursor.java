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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

class SampleByFillValueRecordCursor extends AbstractSampleByFillRecordCursor implements Reopenable {
    private final RecordSink keyMapSink;
    private final Map map;
    private final RecordCursor mapCursor;
    private final Record mapRecord;
    private boolean hasNextPending;
    private boolean isMapBuildPending;
    private boolean isMapInitialized;
    private boolean isOpen;
    private long rowId;

    public SampleByFillValueRecordCursor(
            CairoConfiguration configuration,
            Map map,
            RecordSink keyMapSink,
            ObjList<GroupByFunction> groupByFunctions,
            GroupByFunctionsUpdater groupByFunctionsUpdater,
            ObjList<Function> recordFunctions,
            ObjList<Function> placeholderFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            int timestampType,
            TimestampSampler timestampSampler,
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
        this.map = map;
        this.keyMapSink = keyMapSink;
        record.of(map.getRecord());
        mapCursor = map.getCursor();
        mapRecord = map.getRecord();
        isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            map.close();
            super.close();
            isOpen = false;
        }
    }

    @Override
    public boolean hasNext() {
        initMap();
        initTimestamps();

        if (mapCursor.hasNext()) {
            // scroll down the map iterator
            // next() will return record that uses current map position
            return refreshRecord();
        }

        if (baseRecord == null) {
            return false;
        }

        buildMap();

        return refreshMapCursor();
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        super.of(baseCursor, executionContext);
        rowId = 0;
        hasNextPending = false;
        isMapBuildPending = true;
        isMapInitialized = false;
    }

    @Override
    public void reopen() {
        if (!isOpen) {
            isOpen = true;
            map.reopen();
        }
    }

    @Override
    public long preComputedStateSize() {
        return (!isMapBuildPending && isMapInitialized ? 1 : 0) + super.preComputedStateSize();
    }

    @Override
    public void toTop() {
        super.toTop();
        map.clear();
        rowId = 0;
        hasNextPending = false;
        isMapBuildPending = true;
        isMapInitialized = false;
    }

    private void buildMap() {
        if (isMapBuildPending) {
            // key map has been flushed
            // before we build another one we need to check
            // for timestamp gaps

            // what is the next timestamp we are expecting?
            final long expectedLocalEpoch = timestampSampler.nextTimestamp(nextSampleLocalEpoch);

            // is data timestamp ahead of next expected timestamp?
            if (expectedLocalEpoch < localEpoch) {
                sampleLocalEpoch = expectedLocalEpoch;
                nextSampleLocalEpoch = expectedLocalEpoch;
                isMapBuildPending = true;
                // stream contents
                return;
            }
            sampleLocalEpoch = localEpoch;
            nextSampleLocalEpoch = localEpoch;
            isMapBuildPending = false;
        }

        final long next = timestampSampler.nextTimestamp(localEpoch);
        while (true) {
            long timestamp = getBaseRecordTimestamp();
            if (timestamp < next) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                if (!hasNextPending) {
                    adjustDstInFlight(timestamp - tzOffset);
                    final MapKey key = map.withKey();
                    keyMapSink.copy(baseRecord, key);
                    final MapValue value = key.findValue();
                    assert value != null;

                    if (value.getLong(0) != localEpoch) {
                        value.putLong(0, localEpoch);
                        groupByFunctionsUpdater.updateNew(value, baseRecord, rowId++);
                    } else {
                        groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId++);
                    }
                }

                hasNextPending = true;
                boolean baseHasNext = baseCursor.hasNext();
                hasNextPending = false;
                // carry on with the loop if we still have data
                if (baseHasNext) {
                    continue;
                }

                // we ran out of data, make sure hasNext() returns false at the next
                // opportunity, after we stream map that is
                baseRecord = null;
            } else {
                // timestamp changed, make sure we keep the value of 'lastTimestamp'
                // unchanged. Timestamp column uses this variable.
                // When map is exhausted we would assign 'next' to 'lastTimestamp'
                // and build another map.
                timestamp = adjustDst(timestamp, null, next);
                if (timestamp != Long.MIN_VALUE) {
                    nextSamplePeriod(timestamp);
                }
            }
            isMapBuildPending = true;
            break;
        }
    }

    private void initMap() {
        if (isMapInitialized) {
            return;
        }

        // This factory fills gaps in data. To do that we
        // have to know all possible key values. Essentially, every time
        // we sample we return same set of key values with different
        // aggregation results and timestamp.

        final int n = groupByFunctions.size();
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();

            MapKey key = map.withKey();
            keyMapSink.copy(baseRecord, key);
            MapValue value = key.createValue();
            if (value.isNew()) {
                // timestamp is always stored in value field 0
                value.putLong(0, Numbers.LONG_NULL);
                // have functions reset their columns to "zero" state
                // this would set values for when keys are not found right away
                for (int i = 0; i < n; i++) {
                    groupByFunctions.getQuick(i).setNull(value);
                }
            }
        }

        // because we iterate the base cursor twice, we have to go back to top
        // for the second run
        baseCursor.toTop();
        isMapInitialized = true;
    }

    private boolean refreshMapCursor() {
        map.getCursor().hasNext();
        return refreshRecord();
    }

    private boolean refreshRecord() {
        if (mapRecord.getTimestamp(0) == sampleLocalEpoch) {
            record.setActiveA();
        } else {
            record.setActiveB();
        }
        return true;
    }

    @Override
    protected void updateValueWhenClockMovesBack(MapValue value) {
        final MapKey key = map.withKey();
        keyMapSink.copy(baseRecord, key);
        super.updateValueWhenClockMovesBack(key.createValue());
    }
}

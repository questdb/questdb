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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.ObjList;

class SampleByFillNoneRecordCursor extends AbstractVirtualRecordSampleByCursor {
    private final RecordSink keyMapSink;
    private final Map map;
    private final RecordCursor mapCursor;
    private boolean hasNextPending;
    private boolean isMapBuildPending;
    private boolean isOpen;
    private long rowId;

    public SampleByFillNoneRecordCursor(
            CairoConfiguration configuration,
            Map map,
            RecordSink keyMapSink,
            ObjList<GroupByFunction> groupByFunctions,
            GroupByFunctionsUpdater groupByFunctionsUpdater,
            ObjList<Function> recordFunctions,
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
        initTimestamps();

        if (mapCursor.hasNext()) {
            return true;
        }

        if (baseRecord == null) {
            return false;
        }

        buildMap();

        return mapCursor.hasNext();
    }

    @Override
    public void of(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
        super.of(base, executionContext);
        if (!isOpen) {
            isOpen = true;
            map.reopen();
        }
        rowId = 0;
        hasNextPending = false;
        isMapBuildPending = true;
    }

    @Override
    public void toTop() {
        super.toTop();
        rowId = 0;
        hasNextPending = false;
        isMapBuildPending = true;
    }

    private void buildMap() {
        if (isMapBuildPending) {
            map.clear();
            sampleLocalEpoch = localEpoch;
            isMapBuildPending = false;
        }

        final long next = timestampSampler.nextTimestamp(localEpoch);
        boolean baseHasNext = true;
        while (baseHasNext) {
            if (!hasNextPending) {
                long timestamp = getBaseRecordTimestamp();
                if (timestamp < next) {
                    circuitBreaker.statefulThrowExceptionIfTripped();

                    adjustDstInFlight(timestamp - tzOffset);
                    final MapKey key = map.withKey();
                    keyMapSink.copy(baseRecord, key);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        groupByFunctionsUpdater.updateNew(value, baseRecord, rowId++);
                    } else {
                        groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId++);
                    }
                } else {
                    // map value is conditional and only required when clock goes back
                    // we override base method for when this happens
                    // see: updateValueWhenClockMovesBack()
                    timestamp = adjustDst(timestamp, null, next);
                    if (timestamp != Long.MIN_VALUE) {
                        nextSamplePeriod(timestamp);
                        // reset map iterator
                        map.getCursor();
                        isMapBuildPending = true;
                        return;
                    }
                }
            }

            hasNextPending = true;
            baseHasNext = baseCursor.hasNext();
            hasNextPending = false;
        }

        // we ran out of data, make sure hasNext() returns false at the next
        // opportunity, after we stream map that is.
        baseRecord = null;
        // reset map iterator
        map.getCursor();
        isMapBuildPending = true;
    }

    @Override
    protected void updateValueWhenClockMovesBack(MapValue value) {
        final MapKey key = map.withKey();
        keyMapSink.copy(baseRecord, key);
        super.updateValueWhenClockMovesBack(key.createValue());
    }
}

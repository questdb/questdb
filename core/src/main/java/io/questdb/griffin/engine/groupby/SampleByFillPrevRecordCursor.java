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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

class SampleByFillPrevRecordCursor extends AbstractVirtualRecordSampleByCursor implements Reopenable {
    private final RecordSink keyMapSink;
    private final Map map;
    private final RecordCursor mapCursor;
    // Index of the bucket that buildMap() emits. Map value 0 of a key holds the index of the last
    // bucket with a row of that key. The local bucket start can't serve as that tag: a change of the
    // time zone offset moves it in the middle of a bucket, and the clock moving back repeats it.
    private long bucketIndex;
    private boolean isMapBuildPending;
    private boolean isMapInitialized;
    private boolean isOpen;
    private long rowId;

    public SampleByFillPrevRecordCursor(
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
                sampleToFuncPos,
                false
        );
        this.map = map;
        this.keyMapSink = keyMapSink;
        mapCursor = map.getCursor();
        record.of(map.getRecord());
        // Lazy map (openOnInit=false): start closed so the factory's reopen()
        // allocates the backing under the bound MemoryTracker on the first cursor.
        isOpen = false;
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
        initializeMap();
        initTimestamps();

        if (mapCursor.hasNext()) {
            // scroll down the map iterator
            // next() will return record that uses current map position
            return true;
        }

        if (baseRecord == null) {
            return false;
        }

        buildMap();

        return map.getCursor().hasNext();
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        super.of(baseCursor, executionContext);
        // hasNext() consults the map cursor right after initializeMap() rebuilds the keys,
        // so drop the rows that the previous execution left unread in it.
        map.clear();
        map.getCursor();
        rowId = 0;
        bucketIndex = 0;
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
    public void toTop() {
        super.toTop();
        map.clear();
        // drop the rows left unread in the map cursor, as of() does
        map.getCursor();
        rowId = 0;
        bucketIndex = 0;
        isMapBuildPending = true;
        isMapInitialized = false;
    }

    private void aggregateBaseRecord() {
        final MapKey key = map.withKey();
        keyMapSink.copy(baseRecord, key);
        final MapValue value = key.findValue();
        assert value != null;

        if (value.getLong(0) != bucketIndex) {
            value.putLong(0, bucketIndex);
            groupByFunctionsUpdater.updateNew(value, baseRecord, rowId++);
        } else {
            groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId++);
        }
    }

    private void buildMap() {
        // every call emits one bucket, either a gap or a bucket with rows
        bucketIndex++;
        if (isMapBuildPending) {
            // the next sample epoch could be different from current sample epoch due to DST transition,
            // e.g. clock going backward
            // we need to ensure we do not fill time transition
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
        do {
            long timestamp = getBaseRecordTimestamp();
            if (timestamp < next) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();

                adjustDstInFlight(timestamp - tzOffset);
                aggregateBaseRecord();
            } else {
                // Timestamp changed, make sure we keep the value of 'lastTimestamp'
                // unchanged. Timestamp column uses this variable.
                // When map is exhausted we would assign 'next' to 'lastTimestamp'
                // and build another map.
                timestamp = adjustDst(timestamp, null, next);
                if (timestamp != Long.MIN_VALUE) {
                    nextSamplePeriod(timestamp);
                    isMapBuildPending = true;
                    return;
                }
                // the clock moved back, and updateValueWhenClockMovesBack() added the row to this
                // bucket; the rows up to the end of the bucket in the new offset join it as well
            }
        } while (baseCursor.hasNext());

        // we ran out of data, make sure hasNext() returns false at the next
        // opportunity, after we stream map that is
        baseRecord = null;
        isMapBuildPending = true;
    }

    private void initializeMap() {
        if (isMapInitialized) {
            return;
        }

        // This factory fills gaps in data. To do that we
        // have to know all possible key values. Essentially, every time
        // we sample we return same set of key values with different
        // aggregation results and timestamp.

        int n = groupByFunctions.size();
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            MapKey key = map.withKey();
            keyMapSink.copy(baseRecord, key);
            MapValue value = key.createValue();
            if (value.isNew()) {
                // value field 0 holds the bucket index, see bucketIndex
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

    @Override
    protected void updateValueWhenClockMovesBack(MapValue value) {
        // The row joins the bucket in progress. The key may have no row in this bucket yet, and its
        // value then holds the aggregate of an earlier bucket, which the row must restart.
        aggregateBaseRecord();
    }
}

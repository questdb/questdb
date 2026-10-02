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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Keyed SAMPLE BY cursor. Filling, it holds every key of its input and keeps each key's
 * latest row: for every bucket it computes, its rows cover all keys in a fixed order. As a
 * {@link SampleByFillSource} after {@link #scanKeys()} it serves a PREV fill; after
 * {@link #ofValueFill} it fills its own rows, reading the rows of keys without data
 * through the fill's gap functions. A key without any row yet reads its key columns and
 * NULL values.
 */
class SampleByFillNoneRecordCursor extends AbstractVirtualRecordSampleByCursor implements SampleByFillSource {
    // Map value slot ahead of the aggregate values, which SAMPLE BY generation
    // reserves: the bucket sequence number of the key's latest row, kept once
    // scanKeys() makes the map hold every key across buckets.
    private static final int BUCKET_SLOT = 0;
    private final RecordSink keyMapSink;
    private final Map map;
    private final RecordCursor mapCursor;
    private final MapRecord mapRecord;
    private long bucketRowsLeft;
    private long bucketSeq;
    private long fillBucket;
    // Non-null after ofValueFill(): the cursor fills its own rows over this grid.
    private SampleByFillGrid fillGrid;
    private long fillMaxTimestamp;
    private SampleByFillRecord fillRecord;
    // Gap rows the value fill emits on its own; it polls the breaker on a stride of
    // them, while data rows poll it for every input row.
    private int gapRowCount;
    private boolean hasFillData;
    private boolean isEmittingGapKeys;
    private boolean isFillStarted;
    private boolean isMapBuildPending;
    private boolean isOpen;
    private boolean isRetainingKeys;
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
        mapRecord = map.getRecord();
        record.of(mapRecord);
        mapCursor = map.getCursor();
        // Lazy map (openOnInit=false): start closed so of() allocates the backing
        // under the bound MemoryTracker on the first cursor.
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
    public Record getRecord() {
        return fillGrid != null ? fillRecord : record;
    }

    @Override
    public boolean hasNext() {
        if (fillGrid != null) {
            return nextValueFillRow();
        }

        initTimestamps();

        if (isRetainingKeys) {
            while (!hasNextInBucket()) {
                if (baseRecord == null) {
                    return false;
                }
                buildPrevFillMap();
                bucketRowsLeft = map.size();
            }
            return true;
        }

        if (mapCursor.hasNext()) {
            return true;
        }

        if (baseRecord == null) {
            return false;
        }

        buildMap();

        return mapCursor.hasNext();
    }

    /**
     * Moves to the next row of the current bucket, without computing the next bucket.
     */
    public final boolean hasNextInBucket() {
        if (bucketRowsLeft == 0) {
            return false;
        }
        bucketRowsLeft--;
        return mapCursor.hasNext();
    }

    /**
     * Positions the record on the next key after {@link #rewindKeys()}, without
     * computing a bucket.
     */
    public final boolean nextKey() {
        return mapCursor.hasNext();
    }

    @Override
    public void of(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
        // isOpen before super.of() so an of() breach frees the base cursor via close().
        isOpen = true;
        super.of(base, executionContext);
        // Bind+reopen the map as super.of() does the allocator; reopen() is idempotent.
        map.setMemoryTracker(executionContext.getMemoryTracker());
        map.reopen();
        rowId = 0;
        isMapBuildPending = true;
        isRetainingKeys = false;
        bucketRowsLeft = 0;
        fillGrid = null;
    }

    /**
     * Makes the cursor fill its own rows over the given grid for this run: a gap row
     * reads the key's latest row through the gap functions of fillRecord (active B).
     */
    public void ofValueFill(SampleByFillGrid fillGrid, SampleByFillRecord fillRecord) {
        this.fillGrid = fillGrid;
        this.fillRecord = fillRecord;
        fillRecord.setActiveA();
        isFillStarted = false;
        hasFillData = false;
        isEmittingGapKeys = false;
    }

    @Override
    public long peekNextTimestamp() {
        initTimestamps();
        if (bucketRowsLeft > 0) {
            return sampleLocalEpoch - tzOffset;
        }
        // The next bucket is not computed yet, so the latest row of every key stays readable.
        return baseRecord != null ? localEpoch - tzOffset : Numbers.LONG_NULL;
    }

    /**
     * Restarts {@link #nextKey()} at the first key.
     */
    public void rewindKeys() {
        map.getCursor();
    }

    /**
     * Collects the keys of all input rows without aggregating them and rewinds the
     * cursor for a fill whose every value is its column's PREV.
     */
    public void scanKeys() {
        map.clear();
        scanPrevFillKeys();
        retainKeys();
    }

    @Override
    public void toTop() {
        super.toTop();
        rowId = 0;
        isMapBuildPending = true;
        bucketRowsLeft = 0;
        if (fillGrid != null) {
            fillRecord.setActiveA();
            isFillStarted = false;
            hasFillData = false;
            isEmittingGapKeys = false;
        }
    }

    private void activateKeyRecord() {
        if (mapRecord.getLong(BUCKET_SLOT) == bucketSeq) {
            fillRecord.setActiveA();
        } else {
            fillRecord.setActiveB();
        }
    }

    private void buildMap() {
        if (isMapBuildPending) {
            map.clear();
            sampleLocalEpoch = localEpoch;
            isMapBuildPending = false;
        }

        final long next = timestampSampler.nextTimestamp(localEpoch);
        do {
            long timestamp = getBaseRecordTimestamp();
            if (timestamp < next) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();

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
        } while (baseCursor.hasNext());

        // we ran out of data, make sure hasNext() returns false at the next
        // opportunity, after we stream map that is.
        baseRecord = null;
        // reset map iterator
        map.getCursor();
        isMapBuildPending = true;
    }

    // Accumulates the next bucket into the map that holds every key, leaving the
    // other keys' latest rows in place. buildValueFillMap() runs the same loop for
    // the value fill, so each loop's per-row call sites see the sinks and updaters
    // of one fill kind only.
    private void buildPrevFillMap() {
        if (isMapBuildPending) {
            bucketSeq++;
            sampleLocalEpoch = localEpoch;
            isMapBuildPending = false;
        }

        final long next = timestampSampler.nextTimestamp(localEpoch);
        do {
            long timestamp = getBaseRecordTimestamp();
            if (timestamp < next) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();

                adjustDstInFlight(timestamp - tzOffset);
                final MapKey key = map.withKey();
                keyMapSink.copy(baseRecord, key);
                MapValue value = key.findValue();
                if (value == null) {
                    value = createRetainedValue(key);
                }
                if (value.getLong(BUCKET_SLOT) != bucketSeq) {
                    value.putLong(BUCKET_SLOT, bucketSeq);
                    groupByFunctionsUpdater.updateNew(value, baseRecord, rowId++);
                } else {
                    groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId++);
                }
            } else {
                timestamp = adjustDst(timestamp, null, next);
                if (timestamp != Long.MIN_VALUE) {
                    nextSamplePeriod(timestamp);
                    map.getCursor();
                    isMapBuildPending = true;
                    return;
                }
            }
        } while (baseCursor.hasNext());

        baseRecord = null;
        map.getCursor();
        isMapBuildPending = true;
    }

    private void buildValueFillMap() {
        if (isMapBuildPending) {
            bucketSeq++;
            sampleLocalEpoch = localEpoch;
            isMapBuildPending = false;
        }

        final long next = timestampSampler.nextTimestamp(localEpoch);
        do {
            long timestamp = getBaseRecordTimestamp();
            if (timestamp < next) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();

                adjustDstInFlight(timestamp - tzOffset);
                final MapKey key = map.withKey();
                keyMapSink.copy(baseRecord, key);
                MapValue value = key.findValue();
                if (value == null) {
                    value = createRetainedValue(key);
                }
                if (value.getLong(BUCKET_SLOT) != bucketSeq) {
                    value.putLong(BUCKET_SLOT, bucketSeq);
                    groupByFunctionsUpdater.updateNew(value, baseRecord, rowId++);
                } else {
                    groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId++);
                }
            } else {
                timestamp = adjustDst(timestamp, null, next);
                if (timestamp != Long.MIN_VALUE) {
                    nextSamplePeriod(timestamp);
                    map.getCursor();
                    isMapBuildPending = true;
                    return;
                }
            }
        } while (baseCursor.hasNext());

        baseRecord = null;
        map.getCursor();
        isMapBuildPending = true;
    }

    // A volatile input can yield a key the scan did not see.
    private MapValue createRetainedValue(MapKey key) {
        final MapValue value = key.createValue();
        value.putLong(BUCKET_SLOT, Numbers.LONG_NULL);
        return value;
    }

    // A key without rows reads NULL values.
    private void initKeyValue(MapValue value) {
        value.putLong(BUCKET_SLOT, Numbers.LONG_NULL);
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            groupByFunctions.getQuick(i).setNull(value);
        }
    }

    // Fills this cursor's own rows: every key in each bucket of the fill grid, the
    // rows of keys without data in the bucket and the keys of buckets without data
    // read through the gap functions of fillRecord.
    private boolean nextValueFillBucket() {
        if (!isFillStarted) {
            startValueFill();
        }
        while (fillBucket < fillMaxTimestamp) {
            if (isEmittingGapKeys) {
                if (mapCursor.hasNext()) {
                    pollBreakerOnGapRow();
                    fillRecord.setActiveB();
                    return true;
                }
                isEmittingGapKeys = false;
                fillBucket = fillGrid.nextBucket(fillBucket);
                continue;
            }
            final long dataTs = peekNextTimestamp();
            if (dataTs == fillBucket) {
                buildValueFillMap();
                mapCursor.hasNext();
                hasFillData = true;
                activateKeyRecord();
                return true;
            }
            if (dataTs == Numbers.LONG_NULL && !fillGrid.hasExplicitTo()) {
                return false;
            }
            if (dataTs != Numbers.LONG_NULL && dataTs < fillBucket) {
                throw SampleByFillGrid.dataRowBeforeBucket(dataTs, fillBucket);
            }
            map.getCursor();
            setGapTimestamp(fillBucket);
            isEmittingGapKeys = true;
        }
        return false;
    }

    private boolean nextValueFillRow() {
        if (hasFillData) {
            if (mapCursor.hasNext()) {
                activateKeyRecord();
                return true;
            }
            hasFillData = false;
            fillBucket = fillGrid.nextBucket(fillBucket);
        }
        return nextValueFillBucket();
    }

    private void pollBreakerOnGapRow() {
        if ((++gapRowCount & 0x3FF) == 0) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        }
    }

    private void retainKeys() {
        isRetainingKeys = true;
        bucketSeq = 0;
        toTop();
    }

    private void scanPrevFillKeys() {
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            final MapKey key = map.withKey();
            keyMapSink.copy(baseRecord, key);
            final MapValue value = key.createValue();
            if (value.isNew()) {
                initKeyValue(value);
            }
        }
    }

    private void scanValueFillKeys() {
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            final MapKey key = map.withKey();
            keyMapSink.copy(baseRecord, key);
            final MapValue value = key.createValue();
            if (value.isNew()) {
                initKeyValue(value);
            }
        }
    }

    private void startValueFill() {
        map.clear();
        scanValueFillKeys();
        retainKeys();
        isFillStarted = true;
        fillGrid.resolveBounds();
        if (map.size() == 0) {
            // Empty input -- no keys to fill, emit zero rows.
            fillMaxTimestamp = Long.MIN_VALUE;
            fillBucket = Long.MAX_VALUE;
            return;
        }
        final long firstTs = peekNextTimestamp();
        fillBucket = firstTs != Numbers.LONG_NULL ? fillGrid.firstBucket(firstTs) : fillGrid.firstBucketWithoutRows();
        fillMaxTimestamp = fillGrid.getMaxTimestamp();
    }

    private void updateRetainedValue(MapKey key, long rowId) {
        MapValue value = key.findValue();
        if (value == null) {
            value = createRetainedValue(key);
        }
        if (value.getLong(BUCKET_SLOT) != bucketSeq) {
            value.putLong(BUCKET_SLOT, bucketSeq);
            groupByFunctionsUpdater.updateNew(value, baseRecord, rowId);
        } else {
            groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId);
        }
    }

    @Override
    protected void updateValueWhenClockMovesBack(MapValue value) {
        final MapKey key = map.withKey();
        keyMapSink.copy(baseRecord, key);
        if (isRetainingKeys) {
            updateRetainedValue(key, rowId);
        } else {
            super.updateValueWhenClockMovesBack(key.createValue());
        }
    }
}

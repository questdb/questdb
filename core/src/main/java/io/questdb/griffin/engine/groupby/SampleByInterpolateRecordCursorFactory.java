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

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.griffin.engine.EmptyTableRandomRecordCursor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

public class SampleByInterpolateRecordCursorFactory implements RecordCursorFactory {

    protected final RecordCursorFactory base;
    protected final Map recordKeyMap;
    private final Map dataMap;
    private final VirtualFunctionSkewedSymbolRecordCursor cursor;
    private final ObjList<Function> recordFunctions;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final ObjList<GroupByFunction> groupByScalarFunctions;
    private final ObjList<GroupByFunction> groupByTwoPointFunctions;
    private final ObjList<InterpolationUtil.StoreYFunction> storeYFunctions;
    private final ObjList<InterpolationUtil.InterpolatorFunction> interpolatorFunctions;

    private final RecordSink mapSink;
    // this sink is used to copy recordKeyMap keys to dataMap
    private final RecordSink mapSink2;
    private final RecordMetadata metadata;
    private final int timestampIndex;
    private final TimestampSampler sampler;
    private final int yDataSize;
    private long yData;
    private final int groupByFunctionCount;
    private final int groupByScalarFunctionCount;
    private final int groupByTwoPointFunctionCount;

    public SampleByInterpolateRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull QueryModel model,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull FunctionParser functionParser,
            @Transient @NotNull SqlExecutionContext executionContext,
            @Transient @NotNull BytecodeAssembler asm,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Transient @NotNull EntityColumnFilter entityColumnFilter,
            @Transient @NotNull IntList recordFunctionPositions,
            @Transient @NotNull IntList groupByFunctionPositions,
            int timestampIndex
    ) throws SqlException {
        final int columnCount = model.getBottomUpColumns().size();
        final RecordMetadata metadata = base.getMetadata();
        this.groupByFunctions = new ObjList<>(columnCount);
        this.groupByScalarFunctions = new ObjList<>(columnCount);
        this.groupByTwoPointFunctions = new ObjList<>(columnCount);
        valueTypes.add(ColumnType.BYTE); // gap flag

        GroupByUtils.prepareGroupByFunctions(
                model,
                metadata,
                functionParser,
                executionContext,
                groupByFunctions,
                groupByFunctionPositions,
                valueTypes
        );

        this.recordFunctions = new ObjList<>(columnCount);
        final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
        GroupByUtils.prepareGroupByRecordFunctions(
                model,
                metadata,
                listColumnFilter,
                groupByFunctions,
                groupByFunctionPositions,
                recordFunctions,
                recordFunctionPositions,
                groupByMetadata,
                keyTypes,
                valueTypes.getColumnCount(),
                false,
                timestampIndex
        );

        this.storeYFunctions = new ObjList<>(columnCount);
        this.interpolatorFunctions = new ObjList<>(columnCount);

        // create timestamp column
        TimestampColumn timestampColumn = TimestampColumn.newInstance(valueTypes.getColumnCount() + keyTypes.getColumnCount());
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            if (recordFunctions.getQuick(i) == null) {
                recordFunctions.setQuick(i, timestampColumn);
            }
        }

        this.groupByFunctionCount = groupByFunctions.size();
        for (int i = 0; i < groupByFunctionCount; i++) {
            GroupByFunction function = groupByFunctions.getQuick(i);
            if (function.isScalar()) {
                groupByScalarFunctions.add(function);
                switch (function.getType()) {
                    case ColumnType.BYTE:
                        storeYFunctions.add(InterpolationUtil.STORE_Y_BYTE);
                        interpolatorFunctions.add(InterpolationUtil.INTERPOLATE_BYTE);
                        break;
                    case ColumnType.SHORT:
                        storeYFunctions.add(InterpolationUtil.STORE_Y_SHORT);
                        interpolatorFunctions.add(InterpolationUtil.INTERPOLATE_SHORT);
                        break;
                    case ColumnType.INT:
                        storeYFunctions.add(InterpolationUtil.STORE_Y_INT);
                        interpolatorFunctions.add(InterpolationUtil.INTERPOLATE_INT);
                        break;
                    case ColumnType.LONG:
                        storeYFunctions.add(InterpolationUtil.STORE_Y_LONG);
                        interpolatorFunctions.add(InterpolationUtil.INTERPOLATE_LONG);
                        break;
                    case ColumnType.DOUBLE:
                        storeYFunctions.add(InterpolationUtil.STORE_Y_DOUBLE);
                        interpolatorFunctions.add(InterpolationUtil.INTERPOLATE_DOUBLE);
                        break;
                    case ColumnType.FLOAT:
                        storeYFunctions.add(InterpolationUtil.STORE_Y_FLOAT);
                        interpolatorFunctions.add(InterpolationUtil.INTERPOLATE_FLOAT);
                        break;
                    default:
                        Misc.freeObjList(groupByScalarFunctions);
                        throw SqlException.$(groupByFunctionPositions.getQuick(i), "Unsupported type: ").put(ColumnType.nameOf(function.getType()));
                }
            } else {
                groupByTwoPointFunctions.add(function);
            }
        }

        this.groupByScalarFunctionCount = groupByScalarFunctions.size();
        this.groupByTwoPointFunctionCount = groupByTwoPointFunctions.size();
        this.timestampIndex = timestampIndex;
        this.yDataSize = groupByFunctionCount * 16;
        this.yData = Unsafe.malloc(yDataSize);

        // sink will be storing record columns to map key
        this.mapSink = RecordSinkFactory.getInstance(asm, metadata, listColumnFilter, false);
        entityColumnFilter.of(keyTypes.getColumnCount());
        this.mapSink2 = RecordSinkFactory.getInstance(asm, keyTypes, entityColumnFilter, false);

        // this is the map itself, which we must not forget to free when factory closes
        this.recordKeyMap = MapFactory.createMap(configuration, keyTypes);

        // data map will contain rounded timestamp value as last key column
        keyTypes.add(ColumnType.TIMESTAMP);

        this.dataMap = MapFactory.createMap(configuration, keyTypes, valueTypes);
        this.base = base;
        this.metadata = groupByMetadata;
        this.sampler = timestampSampler;
        this.cursor = new VirtualFunctionSkewedSymbolRecordCursor(recordFunctions);
    }

    @Override
    public void close() {
        Misc.freeObjList(recordFunctions);
        Misc.free(recordKeyMap);
        Misc.free(dataMap);
        freeYData();
        Misc.free(base);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        recordKeyMap.clear();
        dataMap.clear();
        final RecordCursor baseCursor = base.getCursor(executionContext);
        final Record baseRecord = baseCursor.getRecord();
        final SqlExecutionInterruptor interruptor = executionContext.getSqlExecutionInterruptor();
        try {
            // Collect map of unique key values.
            // using this values we will fill gaps in main
            // data before jumping to another timestamp.
            // This will allow to maintain chronological order of
            // main data map.
            //
            // At the same time check if cursor has data
            while (baseCursor.hasNext()) {
                interruptor.checkInterrupted();
                final MapKey key = recordKeyMap.withKey();
                mapSink.copy(baseRecord, key);
                key.createValue();
            }

            // no data, nothing to do
            if (recordKeyMap.size() == 0) {
                baseCursor.close();
                return EmptyTableRandomRecordCursor.INSTANCE;
            }

            // topTop() is guaranteeing that we get
            // the same data as previous while() loop
            // there is no data
            baseCursor.toTop();

            // Evaluate group-by functions.
            // On every change of timestamp sample value we
            // check group for gaps and fill them with placeholder
            // entries. Values for these entries will be interpolated later

            // we have data in cursor, so we can grab first value
            final boolean good = baseCursor.hasNext();
            assert good;
            long timestamp = baseRecord.getTimestamp(timestampIndex);
            sampler.setStart(timestamp);

            long prevSample = sampler.round(timestamp);
            long loSample = prevSample; // the lowest timestamp value
            long hiSample;

            do {
                // this seems inefficient, but we only double-sample
                // very first record and nothing else
                long sample = sampler.round(baseRecord.getTimestamp(timestampIndex));
                if (sample != prevSample) {
                    // before we continue with next interval
                    // we need to fill gaps in current interval
                    // we will go over unique keys and attempt to
                    // find them in data map with current timestamp

                    fillGaps(prevSample, sample);
                    prevSample = sample;
                    GroupByUtils.toTop(groupByFunctions);
                }

                // same data group - evaluate group-by functions
                MapKey key = dataMap.withKey();
                mapSink.copy(baseRecord, key);
                key.putLong(sample);

                MapValue value = key.createValue();
                if (value.isNew()) {
                    value.putByte(0, (byte) 0); // not a gap
                    for (int i = 0; i < groupByFunctionCount; i++) {
                        groupByFunctions.getQuick(i).computeFirst(value, baseRecord);
                    }
                } else {
                    for (int i = 0; i < groupByFunctionCount; i++) {
                        groupByFunctions.getQuick(i).computeNext(value, baseRecord);
                    }
                }

                if (!baseCursor.hasNext()) {
                    hiSample = sampler.nextTimestamp(prevSample);
                    break;
                }
                interruptor.checkInterrupted();
            } while (true);

            // fill gaps if any at end of base cursor
            fillGaps(prevSample, hiSample);

            if (groupByTwoPointFunctionCount > 0) {
                final RecordCursor mapCursor = recordKeyMap.getCursor();
                final Record mapRecord = mapCursor.getRecord();
                while (mapCursor.hasNext()) {
                    MapValue value = findDataMapValue(mapRecord, loSample);
                    if (value.getByte(0) == 0) { //we have at least 1 data point
                        long x1 = loSample;
                        long x2 = x1;
                        while (true) {
                            // to timestamp after 'sample' to begin with
                            x2 = sampler.nextTimestamp(x2);
                            if (x2 < hiSample) {
                                value = findDataMapValue(mapRecord, x2);
                                if (value.getByte(0) == 0) {
                                    interpolateBoundaryRange(x1, x2, mapRecord);
                                    x1 = x2;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
            }

            // find gaps by checking each of the unique keys against every sample
            long sample;
            for (sample = prevSample = loSample; sample < hiSample; prevSample = sample, sample = sampler.nextTimestamp(sample)) {
                final RecordCursor mapCursor = recordKeyMap.getCursor();
                final Record mapRecord = mapCursor.getRecord();
                while (mapCursor.hasNext()) {
                    // locate first gap
                    MapValue value = findDataMapValue(mapRecord, sample);
                    if (value.getByte(0) == 1) {
                        // gap is at 'sample', so potential X-value is at 'prevSample'
                        // now we need to find Y-value
                        long current = sample;

                        while (true) {
                            // to timestamp after 'sample' to begin with
                            long x2 = sampler.nextTimestamp(current);
                            // is this timestamp within range?
                            if (x2 < hiSample) {
                                value = findDataMapValue(mapRecord, x2);
                                if (value.getByte(0) == 1) { // gap
                                    current = x2;
                                } else {
                                    // got something
                                    // Y-value is at 'x2', which is on first iteration
                                    // is 'sample+1', so

                                    // do we really have X-value?
                                    if (sample == loSample) {
                                        // prevSample does not exist
                                        // find first valid value from 'x2+1' onwards
                                        long x1 = x2;
                                        while (true) {
                                            x2 = sampler.nextTimestamp(x2);
                                            if (x2 < hiSample) {
                                                final MapValue x2value = findDataMapValue(mapRecord, x2);
                                                if (x2value.getByte(0) == 0) { // non-gap
                                                    // found value at 'x2' - this is our Y-value
                                                    // the X-value it at 'x1'
                                                    // compute slope and go back down all the way to start
                                                    // computing values in records

                                                    // this has to be a loop that would store y1 and y2 values for each
                                                    // group-by function
                                                    // use current 'value' for record
                                                    MapValue x1Value = findDataMapValue2(mapRecord, x1);
                                                    interpolate(loSample, x1, mapRecord, x1, x2, x1Value, x2value);
                                                    break;
                                                }
                                            } else {
                                                // we only have a single value at 'x1' - cannot interpolate
                                                // make all values before and after 'x1' NULL
                                                nullifyRange(loSample, x1, mapRecord);
                                                nullifyRange(sampler.nextTimestamp(x1), hiSample, mapRecord);
                                                break;
                                            }
                                        }
                                    } else {

                                        // calculate slope between 'preSample' and 'x2'
                                        // yep, that's right, and go all the way back down
                                        // to 'sample' calculating interpolated values
                                        MapValue x1Value = findDataMapValue2(mapRecord, prevSample);
                                        interpolate(sampler.nextTimestamp(prevSample), x2, mapRecord, prevSample, x2, x1Value, value);
                                    }
                                    break;
                                }
                            } else {
                                // try using first two values
                                // we had X-value at 'prevSample'
                                // it will become Y-value and X is at 'prevSample-1'
                                // and calculate interpolated value all the way to 'hiSample'

                                long x1 = sampler.previousTimestamp(prevSample);

                                if (x1 < loSample) {
                                    // not enough data points
                                    // fill all data points from 'sample' down with null
                                    nullifyRange(sample, hiSample, mapRecord);
                                } else {
                                    MapValue x1Value = findDataMapValue2(mapRecord, x1);
                                    MapValue x2value = findDataMapValue(mapRecord, prevSample);
                                    interpolate(sampler.nextTimestamp(prevSample), hiSample, mapRecord, x1, prevSample, x1Value, x2value);
                                }
                                break;
                            }
                        }
                    }
                }
            }

            return initFunctionsAndCursor(executionContext, dataMap.getCursor(), baseCursor);
        } catch (Throwable e) {
            baseCursor.close();
            throw e;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    private void computeYPoints(MapValue x1Value, MapValue x2value) {
        for (int i = 0; i < groupByScalarFunctionCount; i++) {
            InterpolationUtil.StoreYFunction storeYFunction = storeYFunctions.getQuick(i);
            GroupByFunction groupByFunction = groupByScalarFunctions.getQuick(i);
            storeYFunction.store(groupByFunction, x1Value, yData + i * 16L);
            storeYFunction.store(groupByFunction, x2value, yData + i * 16L + 8);
        }
    }

    private void fillGaps(long lo, long hi) {
        final RecordCursor keyCursor = recordKeyMap.getCursor();
        final Record record = keyCursor.getRecord();
        long timestamp = lo;
        while (timestamp < hi) {
            while (keyCursor.hasNext()) {
                MapKey key = dataMap.withKey();
                mapSink2.copy(record, key);
                key.putLong(timestamp);
                MapValue value = key.createValue();
                if (value.isNew()) {
                    value.putByte(0, (byte) 1); // this is gap
                }
            }
            timestamp = sampler.nextTimestamp(timestamp);
            keyCursor.toTop();
        }
    }

    private MapValue findDataMapValue(Record record, long timestamp) {
        final MapKey key = dataMap.withKey();
        mapSink2.copy(record, key);
        key.putLong(timestamp);
        return key.findValue();
    }

    private MapValue findDataMapValue2(Record record, long timestamp) {
        final MapKey key = dataMap.withKey();
        mapSink2.copy(record, key);
        key.putLong(timestamp);
        return key.findValue2();
    }

    private MapValue findDataMapValue3(Record record, long timestamp) {
        final MapKey key = dataMap.withKey();
        mapSink2.copy(record, key);
        key.putLong(timestamp);
        return key.findValue3();
    }

    private void freeYData() {
        if (yData != 0) {
            Unsafe.free(yData, yDataSize);
            yData = 0;
        }
    }

    @NotNull
    protected RecordCursor initFunctionsAndCursor(
            SqlExecutionContext executionContext,
            RecordCursor mapCursor,
            RecordCursor baseCursor
    ) throws SqlException {
        cursor.of(baseCursor, mapCursor);
        // init all record function for this cursor, in case functions require metadata and/or symbol tables
        Function.init(recordFunctions, baseCursor, executionContext);
        return cursor;
    }

    private void interpolate(long lo, long hi, Record mapRecord, long x1, long x2, MapValue x1Value, MapValue x2value) {
        computeYPoints(x1Value, x2value);
        for (long x = lo; x < hi; x = sampler.nextTimestamp(x)) {
            final MapValue result = findDataMapValue3(mapRecord, x);
            assert result != null && result.getByte(0) == 1;
            for (int i = 0; i < groupByTwoPointFunctionCount; i++) {
                GroupByFunction function = groupByTwoPointFunctions.getQuick(i);
                InterpolationUtil.interpolateGap(function, result, sampler.getBucketSize(), x1Value, x2value);
            }
            for (int i = 0; i < groupByScalarFunctionCount; i++) {
                GroupByFunction function = groupByScalarFunctions.getQuick(i);
                interpolatorFunctions.getQuick(i).interpolateAndStore(function, result, x, x1, x2, yData + i * 16L, yData + i * 16L + 8);
            }
            result.putByte(0, (byte) 0); // fill the value, change flag from 'gap' to 'fill'
        }
    }

    private void interpolateBoundaryRange(long x1, long x2, Record record) {
        //interpolating boundary
        for (int i = 0; i < groupByTwoPointFunctionCount; i++) {
            GroupByFunction function = groupByTwoPointFunctions.getQuick(i);
            MapValue startValue = findDataMapValue2(record, x1);
            MapValue endValue = findDataMapValue3(record, x2);
            InterpolationUtil.interpolateBoundary(function, sampler.nextTimestamp(x1), startValue, endValue, true);
            InterpolationUtil.interpolateBoundary(function, x2, startValue, endValue, false);
        }
    }

    private void nullifyRange(long lo, long hi, Record record) {
        for (long x = lo; x < hi; x = sampler.nextTimestamp(x)) {
            final MapKey key = dataMap.withKey();
            mapSink2.copy(record, key);
            key.putLong(x);
            MapValue value = key.findValue();
            assert value != null && value.getByte(0) == 1; // expect  'gap' flag
            value.putByte(0, (byte) 0); // fill the value, change flag from 'gap' to 'fill'
            for (int i = 0; i < groupByFunctionCount; i++) {
                groupByFunctions.getQuick(i).setNull(value);
            }
        }
    }
}

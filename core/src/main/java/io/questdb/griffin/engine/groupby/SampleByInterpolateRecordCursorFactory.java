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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.RecordValueSink;
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.join.JoinRecord;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

public class SampleByInterpolateRecordCursorFactory extends AbstractRecordCursorFactory {

    protected RecordCursorFactory base;
    private SampleByInterpolateRecordCursor cursor;
    private final int groupByFunctionCount;
    private ObjList<GroupByFunction> groupByFunctions;
    private final int groupByScalarFunctionCount;
    private ObjList<GroupByFunction> groupByScalarFunctions;
    private final int groupByTwoPointFunctionCount;
    private ObjList<GroupByFunction> groupByTwoPointFunctions;
    private ObjList<InterpolationUtil.InterpolatorFunction> interpolatorFunctions;
    private final boolean isPreAggregated;
    private final RecordSink mapSink;
    private Function offsetFunc;
    private Function sampleFromFunc;
    private Function sampleToFunc;
    private Function timezoneNameFunc;
    private ObjList<Function> recordFunctions;
    private final TimestampSampler sampler;
    private ObjList<InterpolationUtil.StoreYFunction> storeYFunctions;
    private final int timestampIndex;
    private final RecordValueSink valueSink;
    private final int yDataSize;
    private long yData;

    public SampleByInterpolateRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata metadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull IQueryModel model,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Transient @NotNull EntityColumnFilter entityColumnFilter,
            @Transient @NotNull IntList groupByFunctionPositions,
            int timestampIndex,
            int timestampType,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function sampleFromFunc,
            Function sampleToFunc
    ) throws SqlException {
        this(asm, configuration, base, metadata, groupByFunctions, recordFunctions, timestampSampler, model,
                listColumnFilter, keyTypes, valueTypes, entityColumnFilter, groupByFunctionPositions,
                timestampIndex, timestampType, timezoneNameFunc, timezoneNameFuncPos, offsetFunc, offsetFuncPos,
                sampleFromFunc, sampleToFunc, false);
    }

    public SampleByInterpolateRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata metadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull IQueryModel model,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Transient @NotNull EntityColumnFilter entityColumnFilter,
            @Transient @NotNull IntList groupByFunctionPositions,
            int timestampIndex,
            int timestampType,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function sampleFromFunc,
            Function sampleToFunc,
            boolean isPreAggregated
    ) throws SqlException {
        super(metadata);
        this.isPreAggregated = isPreAggregated;
        try {
            final int columnCount = model.getBottomUpColumns().size();
            this.base = base;
            this.groupByFunctions = groupByFunctions;
            this.recordFunctions = recordFunctions;
            this.sampler = timestampSampler;
            // adopt the temporal parameter functions first, so close() reaches them if the
            // remainder of the construction throws
            this.timezoneNameFunc = timezoneNameFunc;
            this.offsetFunc = offsetFunc;
            this.sampleFromFunc = sampleFromFunc;
            this.sampleToFunc = sampleToFunc;

            // create timestamp column
            TimestampColumn timestampColumn = TimestampColumn.newInstance(valueTypes.getColumnCount() + keyTypes.getColumnCount(), timestampType);
            for (int i = 0, n = recordFunctions.size(); i < n; i++) {
                if (recordFunctions.getQuick(i) == null) {
                    recordFunctions.setQuick(i, timestampColumn);
                }
            }

            this.groupByScalarFunctions = new ObjList<>(columnCount);
            this.groupByTwoPointFunctions = new ObjList<>(columnCount);
            this.storeYFunctions = new ObjList<>(columnCount);
            this.interpolatorFunctions = new ObjList<>(columnCount);
            this.groupByFunctionCount = groupByFunctions.size();
            for (int i = 0; i < groupByFunctionCount; i++) {
                GroupByFunction function = groupByFunctions.getQuick(i);
                if (function.isScalar()) {
                    groupByScalarFunctions.add(function);
                    switch (ColumnType.tagOf(function.getType())) {
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
                            throw SqlException.$(groupByFunctionPositions.getQuick(i), "Unsupported interpolation type: ").put(ColumnType.nameOf(function.getType()));
                    }
                } else {
                    groupByTwoPointFunctions.add(function);
                }
            }

            this.groupByScalarFunctionCount = groupByScalarFunctions.size();
            this.groupByTwoPointFunctionCount = groupByTwoPointFunctions.size();
            this.timestampIndex = isPreAggregated ? valueTypes.getColumnCount() + keyTypes.getColumnCount() : timestampIndex;
            this.yDataSize = groupByFunctionCount * 16;
            this.yData = Unsafe.malloc(yDataSize, MemoryTag.NATIVE_FUNC_RSS);

            if (isPreAggregated) {
                final ArrayColumnTypes preAggregatedTypes = new ArrayColumnTypes().addAll(valueTypes).addAll(keyTypes).add(timestampType);
                final ListColumnFilter keyFilter = new ListColumnFilter();
                for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
                    keyFilter.add(valueTypes.getColumnCount() + i + 1);
                }
                this.mapSink = RecordSinkFactory.getInstance(configuration, asm, preAggregatedTypes, keyFilter);
            } else {
                this.mapSink = RecordSinkFactory.getInstance(configuration, asm, base.getMetadata(), listColumnFilter);
            }
            entityColumnFilter.of(valueTypes.getColumnCount());
            this.valueSink = isPreAggregated ? RecordValueSinkFactory.getInstance(asm, valueTypes, entityColumnFilter) : null;

            this.cursor = new SampleByInterpolateRecordCursor(configuration, recordFunctions, groupByFunctions, keyTypes, valueTypes, timestampType, timezoneNameFunc, timezoneNameFuncPos, offsetFunc, offsetFuncPos, sampleFromFunc, sampleToFunc);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0; i < groupByTwoPointFunctionCount; i++) {
            final GroupByFunction function = groupByTwoPointFunctions.getQuick(i);
            if (!function.isInterpolationSupported()) {
                throw SqlException.position(0).put("interpolation is not supported for function: ").put(function.getClass().getName());
            }
        }

        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            // init all record functions for this cursor, in case functions require metadata and/or symbol tables
            if (isPreAggregated) {
                for (int i = 0, n = recordFunctions.size(); i < n; i++) {
                    final Function function = recordFunctions.getQuick(i);
                    if (!(function instanceof GroupByFunction)) {
                        function.init(baseCursor, executionContext);
                    }
                }
            } else {
                Function.init(recordFunctions, baseCursor, executionContext, null);
            }
        } catch (Throwable th) {
            baseCursor.close();
            throw th;
        }

        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @TestOnly
    public long getGridCellCount() {
        return cursor.gridSize / cursor.cellSize - 2;
    }

    @TestOnly
    public boolean isBuilt() {
        return cursor.isBuilt;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sample By");
        sink.attr("fill").val("linear");
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        if (!isPreAggregated) {
            sink.optAttr("values", groupByFunctions, true);
        }
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private void freeYData(long yData) {
        if (yData != 0) {
            Unsafe.free(yData, yDataSize, MemoryTag.NATIVE_FUNC_RSS);
        }
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final SampleByInterpolateRecordCursor cursor = this.cursor;
        this.cursor = null;
        this.groupByFunctions = null;
        this.groupByScalarFunctions = null;
        this.groupByTwoPointFunctions = null;
        this.interpolatorFunctions = null;
        final Function offsetFunc = this.offsetFunc;
        this.offsetFunc = null;
        final ObjList<Function> recordFunctions = this.recordFunctions;
        this.recordFunctions = null;
        final Function sampleFromFunc = this.sampleFromFunc;
        this.sampleFromFunc = null;
        final Function sampleToFunc = this.sampleToFunc;
        this.sampleToFunc = null;
        this.storeYFunctions = null;
        final Function timezoneNameFunc = this.timezoneNameFunc;
        this.timezoneNameFunc = null;
        final long yData = this.yData;
        this.yData = 0;

        Throwable failure = Misc.freeObjListBestEffort(null, recordFunctions);
        try {
            freeYData(yData);
        } catch (Throwable th) {
            if (failure == null) {
                failure = th;
            } else if (th != failure) {
                failure.addSuppressed(th);
            }
        }
        failure = Misc.freeBestEffort(failure, base);
        failure = Misc.freeBestEffort(failure, cursor);
        // The factory is the lifetime owner of the temporal parameter functions (timezone,
        // offset, FROM, TO); the cursor only borrows them across the executions of this cached
        // factory. The generator accepts runtime-constant expressions here, which may own child
        // functions, so they must be closed exactly once.
        failure = Misc.freeBestEffort(failure, timezoneNameFunc);
        if (offsetFunc != timezoneNameFunc) {
            failure = Misc.freeBestEffort(failure, offsetFunc);
        }
        if (sampleFromFunc != timezoneNameFunc && sampleFromFunc != offsetFunc) {
            failure = Misc.freeBestEffort(failure, sampleFromFunc);
        }
        if (sampleToFunc != timezoneNameFunc && sampleToFunc != offsetFunc && sampleToFunc != sampleFromFunc) {
            failure = Misc.freeBestEffort(failure, sampleToFunc);
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    private static class BucketRecord implements Record {
        private long timestamp;

        @Override
        public long getTimestamp(int col) {
            return timestamp;
        }
    }

    private static class GridRecord extends JoinRecord {
        private final BucketRecord bucket = new BucketRecord();
        private final MapRecord key;
        private final int scratchIndex;
        private final FlyweightPackedMapValue value;
        private long rowId;

        private GridRecord(int valueColumnCount, int keyColumnCount, FlyweightPackedMapValue value, MapRecord key, int scratchIndex) {
            super(valueColumnCount);
            this.value = value;
            this.key = key;
            this.scratchIndex = scratchIndex;
            // The key map record holds the key index ahead of the keys.
            final IntList keyColumns = new IntList(keyColumnCount);
            for (int i = 0; i < keyColumnCount; i++) {
                keyColumns.add(i + 1);
            }
            final SelectedRecord keys = new SelectedRecord(keyColumns);
            keys.of(key);
            final JoinRecord keyAndBucket = new JoinRecord(keyColumnCount);
            keyAndBucket.of(keys, bucket);
            of(value, keyAndBucket);
        }

        @Override
        public long getRowId() {
            return rowId;
        }
    }

    // Holds one row of cells per bucket, in bucket order. A row has a cell for each key seen by
    // the end of its bucket, so a key's cells start at its first data bucket and GridCursor
    // computes the earlier ones on read. Each cell has the value layout of the aggregation,
    // whose first byte flags a gap.
    private class SampleByInterpolateRecordCursor extends VirtualFunctionSkewedSymbolRecordCursor {
        private final GroupByAllocator allocator;
        private final DirectLongList bucketTimestamps;
        private final long cellSize;
        private final MemoryCARW grid;
        private final GridCursor gridCursor;
        private final DirectLongList keyBuckets;
        private final Map keyMap;
        private final MapRecordCursor keyMapCursor;
        private final DirectLongList keyRowIds;
        private final int maxResizes;
        private final DirectLongList rowOffsets;
        private final Function offsetFunc;
        private final int offsetFuncPos;
        private final Function sampleFromFunc;
        private final int sampleFromFuncType;
        private final Function sampleToFunc;
        private final TimestampDriver timestampDriver;
        private final Function timezoneNameFunc;
        private final int timezoneNameFuncPos;
        private final FlyweightPackedMapValue value;
        private final FlyweightPackedMapValue x1Value;
        private final FlyweightPackedMapValue x2Value;
        private long bucketCount;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long fixedOffset;
        private long gridAddress;
        private long gridSize;
        private boolean isBuilt;
        private boolean isOpen;
        private int keyCount;
        private long lastBucket;
        private Record managedRecord;
        private int resizes;
        private long rowId;
        private TimeZoneRules rules;
        private long tzOffset;

        public SampleByInterpolateRecordCursor(
                CairoConfiguration configuration,
                ObjList<Function> functions,
                ObjList<GroupByFunction> groupByFunctions,
                @Transient @NotNull ArrayColumnTypes keyTypes,
                @Transient @NotNull ArrayColumnTypes valueTypes,
                int timestampType,
                Function timezoneNameFunc,
                int timezoneNameFuncPos,
                Function offsetFunc,
                int offsetFuncPos,
                Function sampleFromFunc,
                Function sampleToFunc
        ) {
            super(functions);
            try {
                // Lazy variants: start closed so of() allocates the backing under the bound
                // MemoryTracker on the first cursor, keeping the per-query counter balanced
                // between malloc and the matching free at cursor close.
                isOpen = false;
                keyMap = MapFactory.createOrderedMap(configuration, keyTypes, new ArrayColumnTypes().add(ColumnType.INT), false);
                keyMapCursor = keyMap.newCursor();
                grid = Vm.getCARWInstance(configuration.getSqlSmallMapPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_FAST_MAP);
                maxResizes = configuration.getSqlMapMaxResizes();
                bucketTimestamps = new DirectLongList(configuration.getSqlSmallMapKeyCapacity(), MemoryTag.NATIVE_SAMPLE_BY_LONG_LIST, true);
                keyBuckets = new DirectLongList(configuration.getSqlSmallMapKeyCapacity(), MemoryTag.NATIVE_SAMPLE_BY_LONG_LIST, true);
                keyRowIds = new DirectLongList(configuration.getSqlSmallMapKeyCapacity(), MemoryTag.NATIVE_SAMPLE_BY_LONG_LIST, true);
                rowOffsets = new DirectLongList(configuration.getSqlSmallMapKeyCapacity(), MemoryTag.NATIVE_SAMPLE_BY_LONG_LIST, true);
                value = new FlyweightPackedMapValue(valueTypes);
                x1Value = new FlyweightPackedMapValue(valueTypes);
                x2Value = new FlyweightPackedMapValue(valueTypes);
                cellSize = value.getSizeInBytes();
                gridCursor = new GridCursor(
                        new GridRecord(valueTypes.getColumnCount(), keyTypes.getColumnCount(), new FlyweightPackedMapValue(valueTypes), keyMapCursor.getRecord(), 0),
                        new GridRecord(valueTypes.getColumnCount(), keyTypes.getColumnCount(), new FlyweightPackedMapValue(valueTypes), keyMapCursor.getRecordB(), 1)
                );
                allocator = GroupByAllocatorFactory.createAllocator(configuration, false);
                GroupByUtils.setAllocator(groupByFunctions, allocator);

                this.timezoneNameFunc = timezoneNameFunc;
                this.timezoneNameFuncPos = timezoneNameFuncPos;
                this.offsetFunc = offsetFunc;
                this.offsetFuncPos = offsetFuncPos;
                this.sampleFromFunc = sampleFromFunc;
                this.sampleToFunc = sampleToFunc;
                this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
                this.sampleFromFuncType = ColumnType.getTimestampType(sampleFromFunc.getType());
            } catch (Throwable th) {
                Misc.free(this, th);
                throw th;
            }
        }

        @Override
        public void close() {
            final RecordCursor sourceCursor = managedCursor;
            managedCursor = null;
            managedRecord = null;
            Throwable failure = null;
            try {
                super.close();
            } catch (Throwable th) {
                failure = Misc.foldCleanupFailure(failure, th);
            }
            failure = Misc.freeBestEffort(failure, sourceCursor);
            if (isOpen) {
                isOpen = false;
                failure = Misc.freeBestEffort(failure, keyMap);
                failure = Misc.freeBestEffort(failure, grid);
                failure = Misc.freeBestEffort(failure, bucketTimestamps);
                failure = Misc.freeBestEffort(failure, keyBuckets);
                failure = Misc.freeBestEffort(failure, keyRowIds);
                failure = Misc.freeBestEffort(failure, rowOffsets);
                failure = Misc.freeBestEffort(failure, allocator);
                failure = Misc.clearObjListBestEffort(failure, groupByFunctions);
            }
            CairoException.rethrowCleanupFailure(failure);
            // The temporal parameter functions (timezone, offset, FROM, TO) are borrowed from
            // the owning factory, which closes them exactly once at teardown; per-execution
            // cursor close must leave them usable for the next execution of the cached factory.
        }

        @Override
        public boolean hasNext() {
            if (!isBuilt) {
                build();
                isBuilt = true;
            }
            return super.hasNext();
        }

        public void of(RecordCursor managedCursor, SqlExecutionContext executionContext) throws SqlException {
            super.of(managedCursor, gridCursor);
            if (!isOpen) {
                isOpen = true;
                final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
                keyMap.setMemoryTracker(memoryTracker);
                keyMap.reopen();
                grid.setMemoryTracker(memoryTracker);
                bucketTimestamps.setMemoryTracker(memoryTracker);
                bucketTimestamps.reopen();
                keyBuckets.setMemoryTracker(memoryTracker);
                keyBuckets.reopen();
                keyRowIds.setMemoryTracker(memoryTracker);
                keyRowIds.reopen();
                rowOffsets.setMemoryTracker(memoryTracker);
                rowOffsets.reopen();
                allocator.setMemoryTracker(memoryTracker);
                allocator.reopen();
            }
            circuitBreaker = executionContext.getCircuitBreaker();
            managedRecord = isPreAggregated ? ((VirtualRecord) managedCursor.getRecord()).getBaseRecord() : managedCursor.getRecord();
            reset();
            parseParams(this, executionContext);
            sampleFromFunc.init(managedCursor, executionContext);
            sampleToFunc.init(managedCursor, executionContext);
        }

        @Override
        public long preComputedStateSize() {
            return isBuilt ? 1 : 0;
        }

        @Override
        public long size() {
            return isBuilt ? super.size() : -1;
        }

        @Override
        public void toTop() {
            super.toTop();
            if (!isBuilt) {
                reset();
                managedCursor.toTop();
            }
        }

        private void aggregate() {
            do {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                if (isPreAggregated) {
                    managedRecord = ((VirtualRecord) managedCursor.getRecord()).getBaseRecord();
                }
                final long sample = sampler.round(managedRecord.getTimestamp(timestampIndex));
                if (sample != lastBucket) {
                    if (sample < lastBucket) {
                        throw CairoException.nonCritical().put("SAMPLE BY FILL(LINEAR) requires ascending designated timestamp order");
                    }
                    do {
                        lastBucket = sampler.nextTimestamp(lastBucket);
                        appendBucket(lastBucket);
                    } while (lastBucket < sample);
                    GroupByUtils.toTop(groupByFunctions);
                }

                final MapKey key = keyMap.withKey();
                mapSink.copy(managedRecord, key);
                final MapValue keyValue = key.createValue();
                if (keyValue.isNew()) {
                    extendGrid(gridSize + cellSize);
                    Unsafe.putByte(gridAddress + gridSize, (byte) 1);
                    gridSize += cellSize;
                    keyBuckets.add(bucketCount - 1);
                    keyBuckets.add(-1);
                    keyBuckets.add(-1);
                    keyValue.putInt(0, keyCount++);
                }

                value.of(cell(bucketCount - 1, keyValue.getInt(0)));
                if (isPreAggregated) {
                    valueSink.copy(managedRecord, value);
                    value.putByte(0, (byte) 0);
                } else if (value.getByte(0) != 0) {
                    value.putByte(0, (byte) 0);
                    for (int i = 0; i < groupByFunctionCount; i++) {
                        groupByFunctions.getQuick(i).computeFirst(value, managedRecord, rowId++);
                    }
                } else {
                    for (int i = 0; i < groupByFunctionCount; i++) {
                        groupByFunctions.getQuick(i).computeNext(value, managedRecord, rowId++);
                    }
                }
            } while (managedCursor.hasNext());
        }

        private void appendBucket(long timestamp) {
            final long rowSize = keyCount * cellSize;
            extendGrid(gridSize + rowSize);
            for (long p = gridAddress + gridSize, hi = p + rowSize; p < hi; p += cellSize) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                Unsafe.putByte(p, (byte) 1);
            }
            rowOffsets.add(gridSize);
            gridSize += rowSize;
            bucketTimestamps.add(timestamp);
            bucketCount++;
        }

        private void build() {
            if (!managedCursor.hasNext()) {
                return;
            }
            initTimestamps();
            appendBucket(lastBucket);
            aggregate();

            final MapRecordCursor keys = keyMap.getCursor();
            final MapRecord keyRecord = keys.getRecord();
            while (keys.hasNext()) {
                keyRowIds.add(keyRecord.getRowId());
            }
            if (groupByTwoPointFunctionCount > 0) {
                interpolateBoundaries();
            }
            interpolateGaps();
        }

        private long cell(long bucket, int key) {
            return gridAddress + rowOffsets.get(bucket) + key * cellSize;
        }

        private void computeYPoints(MapValue x1Value, MapValue x2value) {
            for (int i = 0; i < groupByScalarFunctionCount; i++) {
                InterpolationUtil.StoreYFunction storeYFunction = storeYFunctions.getQuick(i);
                GroupByFunction groupByFunction = groupByScalarFunctions.getQuick(i);
                storeYFunction.store(groupByFunction, x1Value, yData + i * 16L);
                storeYFunction.store(groupByFunction, x2value, yData + i * 16L + 8);
            }
        }

        private void extendGrid(long size) {
            if (size > grid.size() && grid.size() > 0) {
                if (resizes == maxResizes) {
                    throw LimitOverflowException.instance().put("limit of ").put(maxResizes).put(" resizes exceeded in FILL(LINEAR)");
                }
                resizes++;
            }
            grid.extend(size);
            gridAddress = grid.getPageAddress(0);
        }

        private void initTimestamps() {
            if (isPreAggregated) {
                managedRecord = ((VirtualRecord) managedCursor.getRecord()).getBaseRecord();
            }

            final long timestamp = managedRecord.getTimestamp(timestampIndex);
            // TIMESTAMP(col) can designate a column that holds NULL, and an ascending base puts the NULL first. No
            // bucket can hold it. With ALIGN TO FIRST OBSERVATION, the grid would start at Long.MIN_VALUE, and
            // the build would create a gap row for every stride up to the next row. On a calendar grid, round()
            // would put the NULL in a bucket near Long.MAX_VALUE.
            if (timestamp == Numbers.LONG_NULL) {
                throw CairoException.nonCritical().put("SAMPLE BY designated timestamp cannot be NULL");
            }
            if (rules != null) {
                tzOffset = rules.getOffset(timestamp);
            }

            if (tzOffset == 0 && fixedOffset == Long.MIN_VALUE) {
                // this is the default path, we align time intervals to the first observation
                sampler.setStart(timestamp);
            } else {
                // FROM-TO may apply to align to calendar queries, fixing the lower bound.
                if (sampleFromFunc != timestampDriver.getTimestampConstantNull()) {
                    long from = sampleFromFunc.getTimestamp(null);
                    sampler.setStart(from != Long.MIN_VALUE ? timestampDriver.from(from, sampleFromFuncType) : 0);
                } else {
                    sampler.setOffset(fixedOffset != Long.MIN_VALUE ? fixedOffset : 0L);
                }
            }
            lastBucket = sampler.round(timestamp);
        }

        private void interpolate(int key, long lo, long hi, long x1, long x2) {
            prepareInterpolation(key, x1, x2);
            final long x1Timestamp = bucketTimestamps.get(x1);
            final long x2Timestamp = bucketTimestamps.get(x2);
            for (long bucket = lo; bucket < hi; bucket++) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                value.of(cell(bucket, key));
                interpolate(value, bucketTimestamps.get(bucket), x1Timestamp, x2Timestamp);
            }
        }

        private void interpolate(MapValue target, long timestamp, long x1Timestamp, long x2Timestamp) {
            for (int i = 0; i < groupByTwoPointFunctionCount; i++) {
                InterpolationUtil.interpolateGap(groupByTwoPointFunctions.getQuick(i), target, sampler.getBucketSize(), x1Value, x2Value);
            }
            for (int i = 0; i < groupByScalarFunctionCount; i++) {
                interpolatorFunctions.getQuick(i).interpolateAndStore(groupByScalarFunctions.getQuick(i), target, timestamp, x1Timestamp, x2Timestamp, yData + i * 16L, yData + i * 16L + 8);
            }
            target.putByte(0, (byte) 0);
        }

        // Two-point functions interpolate the boundaries between consecutive data buckets of the
        // keys that have data in the first bucket.
        private void interpolateBoundaries() {
            final int keys = rowKeyCount(0);
            for (int key = 0; key < keys; key++) {
                keyBuckets.set(3L * key + 1, 0);
            }
            for (long bucket = 1; bucket < bucketCount; bucket++) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                final long row = cell(bucket, 0);
                for (int key = 0; key < keys; key++) {
                    if (Unsafe.getByte(row + key * cellSize) == 0) {
                        final long last = keyBuckets.get(3L * key + 1);
                        x1Value.of(cell(last, key));
                        x2Value.of(row + key * cellSize);
                        for (int i = 0; i < groupByTwoPointFunctionCount; i++) {
                            final GroupByFunction function = groupByTwoPointFunctions.getQuick(i);
                            InterpolationUtil.interpolateBoundary(function, bucketTimestamps.get(last + 1), x1Value, x2Value, true);
                            InterpolationUtil.interpolateBoundary(function, bucketTimestamps.get(bucket), x1Value, x2Value, false);
                        }
                        keyBuckets.set(3L * key + 1, bucket);
                    }
                }
            }
            for (int key = 0; key < keys; key++) {
                keyBuckets.set(3L * key + 1, -1);
            }
        }

        // Fills the gaps in bucket order. A gap between two data buckets takes the line through them,
        // and the gaps after the last take the line through the last bucket and the one before it.
        // The gaps before the first data bucket have no cells; GridCursor computes them on read.
        private void interpolateGaps() {
            for (long bucket = 0; bucket < bucketCount; bucket++) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                final long row = cell(bucket, 0);
                for (int key = 0, n = rowKeyCount(bucket); key < n; key++) {
                    if (Unsafe.getByte(row + key * cellSize) == 0) {
                        final long last = keyBuckets.get(3L * key + 1);
                        if (last >= 0) {
                            if (last < bucket - 1) {
                                interpolate(key, last + 1, bucket, last, bucket);
                            }
                            if (keyBuckets.get(3L * key + 2) < 0) {
                                keyBuckets.set(3L * key + 2, bucket);
                            }
                        }
                        keyBuckets.set(3L * key + 1, bucket);
                    }
                }
            }
            for (int key = 0; key < keyCount; key++) {
                final long last = keyBuckets.get(3L * key + 1);
                if (keyBuckets.get(3L * key + 2) < 0) {
                    nullify(key, last + 1, bucketCount);
                } else if (last < bucketCount - 1) {
                    interpolate(key, last + 1, bucketCount, last - 1, last);
                }
            }
        }

        private void nullify(int key, long lo, long hi) {
            for (long bucket = lo; bucket < hi; bucket++) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                value.of(cell(bucket, key));
                nullify(value);
            }
        }

        private void nullify(MapValue target) {
            target.putByte(0, (byte) 0);
            for (int i = 0; i < groupByFunctionCount; i++) {
                groupByFunctions.getQuick(i).setNull(target);
            }
        }

        private void prepareInterpolation(int key, long x1, long x2) {
            x1Value.of(cell(x1, key));
            x2Value.of(cell(x2, key));
            computeYPoints(x1Value, x2Value);
        }

        private void reset() {
            keyMap.clear();
            bucketTimestamps.clear();
            keyBuckets.clear();
            keyRowIds.clear();
            rowOffsets.clear();
            bucketCount = 0;
            // The first two cells hold the values GridCursor computes for its two records.
            gridSize = 2 * cellSize;
            keyCount = 0;
            rowId = 0;
            resizes = 0;
            isBuilt = false;
        }

        private int rowKeyCount(long bucket) {
            final long hi = bucket + 1 < bucketCount ? rowOffsets.get(bucket + 1) : gridSize;
            return (int) ((hi - rowOffsets.get(bucket)) / cellSize);
        }

        protected void parseParams(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
            // factory guarantees that base cursor is not empty
            timezoneNameFunc.init(base, executionContext);
            offsetFunc.init(base, executionContext);
            rules = null;

            final CharSequence tz = timezoneNameFunc.getStrA(null);
            if (tz != null) {
                try {
                    long opt = Dates.parseOffset(tz);
                    if (opt == Long.MIN_VALUE) {
                        // this is timezone name
                        // fixed rules means the timezone does not have historical or daylight time changes
                        rules = DateLocaleFactory.EN_LOCALE.getZoneRules(
                                Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, tz.length())),
                                timestampDriver.getTZRuleResolution()
                        );
                    } else {
                        // here timezone is in numeric offset format
                        tzOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(opt));
                    }
                } catch (NumericException e) {
                    throw SqlException.$(timezoneNameFuncPos, "invalid timezone: ").put(tz);
                }
            } else {
                tzOffset = 0;
            }

            final CharSequence offset = offsetFunc.getStrA(null);
            if (offset != null) {
                final long val = Dates.parseOffset(offset);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(offsetFuncPos, "invalid offset: ").put(offset);
                }
                fixedOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(val));
            } else {
                fixedOffset = Long.MIN_VALUE;
            }
        }

        private class GridCursor implements RecordCursor {
            private final GridRecord recordA;
            private final GridRecord recordB;
            private long bucket;
            private int key;
            private long row;
            private int rowKeys;

            private GridCursor(GridRecord recordA, GridRecord recordB) {
                this.recordA = recordA;
                this.recordB = recordB;
            }

            @Override
            public void close() {
            }

            @Override
            public Record getRecord() {
                return recordA;
            }

            @Override
            public Record getRecordB() {
                return recordB;
            }

            @Override
            public boolean hasNext() {
                if (++key == keyCount) {
                    key = 0;
                    bucket++;
                }
                if (bucket < bucketCount) {
                    if (key == 0) {
                        row = cell(bucket, 0);
                        rowKeys = rowKeyCount(bucket);
                    }
                    position(recordA, bucket, key, row, rowKeys);
                    return true;
                }
                return false;
            }

            @Override
            public long preComputedStateSize() {
                return 0;
            }

            @Override
            public void recordAt(Record record, long atRowId) {
                final long bucket = atRowId / keyCount;
                position((GridRecord) record, bucket, (int) (atRowId % keyCount), cell(bucket, 0), rowKeyCount(bucket));
            }

            @Override
            public long size() {
                return bucketCount * keyCount;
            }

            @Override
            public void toTop() {
                bucket = 0;
                key = -1;
            }

            private void position(GridRecord record, long bucket, int key, long row, int rowKeys) {
                if (key < rowKeys) {
                    record.value.of(row + key * cellSize);
                } else {
                    positionBeforeFirstBucket(record, bucket, key);
                }
                keyMapCursor.recordAt(record.key, keyRowIds.get(key));
                record.bucket.timestamp = bucketTimestamps.get(bucket);
                record.rowId = bucket * keyCount + key;
            }

            private void positionBeforeFirstBucket(GridRecord record, long bucket, int key) {
                record.value.of(gridAddress + record.scratchIndex * cellSize);
                final long second = keyBuckets.get(3L * key + 2);
                if (second < 0) {
                    nullify(record.value);
                } else {
                    final long first = keyBuckets.get(3L * key);
                    prepareInterpolation(key, first, second);
                    interpolate(record.value, bucketTimestamps.get(bucket), bucketTimestamps.get(first), bucketTimestamps.get(second));
                }
            }
        }
    }
}

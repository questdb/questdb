/*******************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Single-threaded factory for keyed HORIZON JOIN (GROUP BY with keys).
 * <p>
 * Uses globally sorted horizon timestamp iteration via
 * {@link HorizonTimestampIterator} for efficient monotonic ASOF scans
 * through the slave. Requires the master cursor to support random access.
 */
public class HorizonJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final HorizonJoinRecordCursor cursor;
    private final RecordMetadata horizonJoinMetadata;
    private final RecordCursorFactory masterFactory;
    private final LongList offsets;
    private final ObjList<Function> recordFunctions;
    private final RecordCursorFactory slaveFactory;

    public HorizonJoinRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull RecordMetadata horizonJoinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            @NotNull LongList offsets,
            int masterTimestampColumnIndex,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterAsOfJoinMapSink,
            @Nullable RecordSink slaveAsOfJoinMapSink,
            int masterColumnCount,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
            @Transient @NotNull ListColumnFilter groupByColumnFilter,
            @NotNull ObjList<Function> keyFunctions,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            @Transient @NotNull BytecodeAssembler asm
    ) {
        super(metadata);
        try {
            this.horizonJoinMetadata = horizonJoinMetadata;
            this.masterFactory = masterFactory;
            this.slaveFactory = slaveFactory;
            this.offsets = offsets;
            this.recordFunctions = recordFunctions;

            // Compute timestamp scale factors for cross-resolution support
            final int masterTsType = masterFactory.getMetadata().getTimestampType();
            final int slaveTsType = slaveFactory.getMetadata().getTimestampType();
            long masterTsScale = 1;
            long slaveTsScale = 1;
            if (masterTsType != slaveTsType) {
                masterTsScale = ColumnType.getTimestampDriver(masterTsType).toNanosScale();
                slaveTsScale = ColumnType.getTimestampDriver(slaveTsType).toNanosScale();
            }

            this.cursor = new HorizonJoinRecordCursor(
                    configuration,
                    horizonJoinMetadata,
                    groupByFunctions,
                    recordFunctions,
                    keyFunctions,
                    asm,
                    masterTimestampColumnIndex,
                    offsets,
                    keyTypes,
                    valueTypes,
                    asOfJoinKeyTypes,
                    masterAsOfJoinMapSink,
                    slaveAsOfJoinMapSink,
                    masterColumnCount,
                    masterSymbolKeyColumnIndices,
                    slaveSymbolKeyColumnIndices,
                    groupByColumnFilter,
                    columnSources,
                    columnIndexes,
                    masterTsScale,
                    slaveTsScale
            );
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return masterFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        try {
            TimeFrameCursor slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            try {
                cursor.of(masterCursor, slaveCursor, executionContext);
                return cursor;
            } catch (Throwable th) {
                Misc.free(slaveCursor);
                throw th;
            }
        } catch (Throwable th) {
            Misc.free(masterCursor);
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Horizon Join");
        sink.meta("offsets").val(offsets.size());
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.setMetadata(horizonJoinMetadata);
        sink.optAttr("values", cursor.groupByFunctions);
        sink.setMetadata(null);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.free(cursor);
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.freeObjList(recordFunctions);
        Misc.freeIfCloseable(horizonJoinMetadata);
    }

    private static class HorizonJoinRecordCursor implements RecordCursor {
        private final Map asOfJoinMap;
        private final Map dataMap;
        private final GroupByAllocator groupByAllocator;
        private final ObjList<GroupByFunction> groupByFunctions;
        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        private final RecordSink groupByKeyCopier;
        private final HorizonTimestampIterator horizonIterator;
        private final HorizonJoinRecord horizonJoinRecord;
        private final HorizonJoinSymbolTableSource horizonJoinSymbolTableSource;
        private final ObjList<Function> keyFunctions;
        private final RecordSink masterAsOfJoinMapSink;
        private final int masterTimestampColumnIndex;
        private final long masterTsScale;
        private final LongList offsets;
        private final VirtualRecord recordA;
        private final VirtualRecord recordB;
        private final ObjList<Function> recordFunctions;
        private final RecordSink slaveAsOfJoinMapSink;
        private final MarkoutTimeFrameHelper slaveTimeFrameHelper;
        private final SymbolTranslatingRecord symbolTranslatingRecord;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isDataMapBuilt;
        private boolean isOpen;
        private MapRecordCursor mapCursor;
        private RecordCursor masterCursor;
        private TimeFrameCursor slaveCursor;

        HorizonJoinRecordCursor(
                CairoConfiguration configuration,
                RecordMetadata horizonJoinMetadata,
                ObjList<GroupByFunction> groupByFunctions,
                ObjList<Function> recordFunctions,
                ObjList<Function> keyFunctions,
                @Transient BytecodeAssembler asm,
                int masterTimestampColumnIndex,
                LongList offsets,
                @Transient ArrayColumnTypes keyTypes,
                @Transient ArrayColumnTypes valueTypes,
                @Nullable ColumnTypes asOfJoinKeyTypes,
                @Nullable RecordSink masterAsOfJoinMapSink,
                @Nullable RecordSink slaveAsOfJoinMapSink,
                int masterColumnCount,
                int @Nullable [] masterSymbolKeyColumnIndices,
                int @Nullable [] slaveSymbolKeyColumnIndices,
                @Transient ListColumnFilter groupByColumnFilter,
                int[] columnSources,
                int[] columnIndexes,
                long masterTsScale,
                long slaveTsScale
        ) {
            this.groupByFunctions = groupByFunctions;
            this.recordFunctions = recordFunctions;
            this.keyFunctions = keyFunctions;
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.offsets = offsets;
            this.masterTsScale = masterTsScale;
            this.masterAsOfJoinMapSink = masterAsOfJoinMapSink;
            this.slaveAsOfJoinMapSink = slaveAsOfJoinMapSink;

            this.recordA = new VirtualRecord(recordFunctions);
            this.recordB = new VirtualRecord(recordFunctions);
            this.horizonJoinRecord = new HorizonJoinRecord();
            this.horizonJoinRecord.init(columnSources, columnIndexes);
            this.horizonJoinSymbolTableSource = new HorizonJoinSymbolTableSource(columnSources, columnIndexes);
            this.horizonIterator = new HorizonTimestampIterator(offsets);

            Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, groupByFunctions.size());
            this.groupByFunctionsUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, groupByFunctions);
            this.groupByAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(groupByFunctions, groupByAllocator);

            // Create data map for GROUP BY keys
            this.dataMap = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
            // Create group by key copier with key functions
            Class<RecordSink> sinkClass = RecordSinkFactory.getInstanceClass(
                    configuration,
                    asm,
                    horizonJoinMetadata,
                    groupByColumnFilter,
                    keyFunctions,
                    null,
                    null,
                    null,
                    null
            );
            this.groupByKeyCopier = RecordSinkFactory.getInstance(
                    sinkClass,
                    horizonJoinMetadata,
                    groupByColumnFilter,
                    keyFunctions,
                    null,
                    null,
                    null,
                    null
            );

            if (asOfJoinKeyTypes != null) {
                SingleColumnType asOfValueTypes = new SingleColumnType(ColumnType.LONG);
                this.asOfJoinMap = MapFactory.createUnorderedMap(configuration, asOfJoinKeyTypes, asOfValueTypes);
            } else {
                this.asOfJoinMap = null;
            }

            if (masterSymbolKeyColumnIndices != null) {
                this.symbolTranslatingRecord = new SymbolTranslatingRecord(masterColumnCount, masterSymbolKeyColumnIndices, slaveSymbolKeyColumnIndices);
            } else {
                this.symbolTranslatingRecord = null;
            }

            this.slaveTimeFrameHelper = new MarkoutTimeFrameHelper(configuration.getSqlAsOfJoinLookAhead(), slaveTsScale);
            this.isOpen = true;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            buildMapConditionally();
            mapCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                masterCursor = Misc.free(masterCursor);
                slaveCursor = Misc.free(slaveCursor);
                mapCursor = Misc.free(mapCursor);
                Misc.free(dataMap);
                Misc.clearObjList(groupByFunctions);
                Misc.free(groupByAllocator);
                if (asOfJoinMap != null) {
                    asOfJoinMap.close();
                }
                Misc.clear(symbolTranslatingRecord);
                isOpen = false;
            }
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
        public SymbolTable getSymbolTable(int columnIndex) {
            return (SymbolTable) recordFunctions.getQuick(columnIndex);
        }

        @Override
        public boolean hasNext() {
            buildMapConditionally();
            return mapCursor.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return ((SymbolFunction) recordFunctions.getQuick(columnIndex)).newSymbolTable();
        }

        @Override
        public long preComputedStateSize() {
            return isDataMapBuilt ? 1 : 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            if (mapCursor != null) {
                mapCursor.recordAt(((VirtualRecord) record).getBaseRecord(), atRowId);
            }
        }

        @Override
        public long size() {
            if (!isDataMapBuilt) {
                return -1;
            }
            return mapCursor != null ? mapCursor.size() : -1;
        }

        @Override
        public void toTop() {
            if (mapCursor != null) {
                mapCursor.toTop();
                GroupByUtils.toTop(recordFunctions);
            }
        }

        private void buildMap() {
            final boolean keyedAsOfJoin = asOfJoinMap != null && masterAsOfJoinMapSink != null && slaveAsOfJoinMapSink != null;

            slaveTimeFrameHelper.toTop();
            if (keyedAsOfJoin) {
                asOfJoinMap.clear();
            }
            dataMap.clear();

            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            while (horizonIterator.next()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                final long horizonTs = horizonIterator.getHorizonTimestamp();
                final long masterRowId = horizonIterator.getMasterRowId();
                final int offsetIdx = horizonIterator.getOffsetIndex();
                final long offset = offsets.getQuick(offsetIdx);

                // Position master record at the correct row via random access
                masterCursor.recordAt(masterCursor.getRecordB(), masterRowId);
                Record masterRecord = masterCursor.getRecordB();

                // Scale horizon timestamp for ASOF lookup
                final long scaledHorizonTs = scaleTimestamp(horizonTs, masterTsScale);

                // Find ASOF row
                long asOfRowId = slaveTimeFrameHelper.findAsOfRow(scaledHorizonTs);

                long matchRowId = Long.MIN_VALUE;
                if (keyedAsOfJoin) {
                    Record masterKeyRecord = masterRecord;
                    if (symbolTranslatingRecord != null) {
                        symbolTranslatingRecord.of(masterRecord);
                        masterKeyRecord = symbolTranslatingRecord;
                    }

                    if (asOfRowId != Long.MIN_VALUE) {
                        if (slaveTimeFrameHelper.getForwardWatermark() == Long.MIN_VALUE) {
                            matchRowId = slaveTimeFrameHelper.backwardScanForKeyMatch(
                                    asOfRowId,
                                    masterKeyRecord,
                                    masterAsOfJoinMapSink,
                                    slaveAsOfJoinMapSink,
                                    asOfJoinMap
                            );
                            slaveTimeFrameHelper.initForwardWatermark(asOfRowId);
                        } else {
                            slaveTimeFrameHelper.forwardScanToPosition(
                                    asOfRowId,
                                    slaveAsOfJoinMapSink,
                                    asOfJoinMap
                            );

                            MapKey cacheKey = asOfJoinMap.withKey();
                            cacheKey.put(masterKeyRecord, masterAsOfJoinMapSink);
                            MapValue cacheValue = cacheKey.findValue();

                            if (cacheValue != null) {
                                matchRowId = cacheValue.getLong(0);
                            } else {
                                matchRowId = slaveTimeFrameHelper.backwardScanForKeyMatch(
                                        asOfRowId,
                                        masterKeyRecord,
                                        masterAsOfJoinMapSink,
                                        slaveAsOfJoinMapSink,
                                        asOfJoinMap
                                );
                            }
                        }
                    }
                } else {
                    matchRowId = asOfRowId;
                }

                // Aggregate the result
                Record matchedSlaveRecord = null;
                if (matchRowId != Long.MIN_VALUE) {
                    slaveTimeFrameHelper.recordAt(matchRowId);
                    matchedSlaveRecord = slaveRecord;
                }
                horizonJoinRecord.of(masterRecord, offset, horizonTs, matchedSlaveRecord);

                MapKey key = dataMap.withKey();
                key.put(horizonJoinRecord, groupByKeyCopier);
                MapValue value = key.createValue();
                if (value.isNew()) {
                    groupByFunctionsUpdater.updateNew(value, horizonJoinRecord, masterRowId);
                } else {
                    groupByFunctionsUpdater.updateExisting(value, horizonJoinRecord, masterRowId);
                }
            }

            mapCursor = dataMap.getCursor();
            recordA.of(mapCursor.getRecord());
            recordB.of(mapCursor.getRecordB());
            isDataMapBuilt = true;
        }

        private void buildMapConditionally() {
            if (!isDataMapBuilt) {
                buildMap();
            }
        }

        void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionContext executionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                groupByAllocator.reopen();
                dataMap.reopen();
                if (asOfJoinMap != null) {
                    asOfJoinMap.reopen();
                }
            }
            this.circuitBreaker = executionContext.getCircuitBreaker();
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            slaveTimeFrameHelper.of(slaveCursor);

            // Initialize horizon timestamp iterator with master cursor
            Record recordBMaster = masterCursor.getRecordB();
            horizonIterator.of(masterCursor, recordBMaster, masterTimestampColumnIndex);

            // Initialize symbol table sources
            horizonJoinSymbolTableSource.of(masterCursor, slaveCursor);

            // Initialize symbol translating record
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.initSources(masterCursor, slaveCursor);
            }

            // Initialize functions
            Function.init(recordFunctions, horizonJoinSymbolTableSource, executionContext, null);
            Function.init(keyFunctions, horizonJoinSymbolTableSource, executionContext, null);
            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                groupByFunctions.getQuick(i).init(horizonJoinSymbolTableSource, executionContext);
            }

            isDataMapBuilt = false;
        }
    }
}

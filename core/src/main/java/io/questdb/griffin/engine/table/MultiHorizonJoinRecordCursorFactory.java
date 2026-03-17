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
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
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
import io.questdb.cairo.sql.SymbolTableSource;
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
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Single-threaded factory for keyed HORIZON JOIN with multiple slave tables.
 */
public class MultiHorizonJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final MultiHorizonJoinRecordCursor cursor;
    private final JoinRecordMetadata horizonJoinMetadata;
    private final ObjList<Function> keyFunctions;
    private final RecordCursorFactory masterFactory;
    private final LongList offsets;
    private final ObjList<Function> recordFunctions;
    private final HorizonJoinSlaveState[] slaveStates;

    public MultiHorizonJoinRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull RecordMetadata metadata,
            @NotNull JoinRecordMetadata horizonJoinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull HorizonJoinSlaveState[] slaveStates,
            @NotNull LongList offsets,
            int masterTimestampColumnIndex,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @NotNull ObjList<Function> keyFunctions,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Transient @NotNull ListColumnFilter groupByColumnFilter,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes
    ) {
        super(metadata);
        try {
            this.horizonJoinMetadata = horizonJoinMetadata;
            this.keyFunctions = keyFunctions;
            this.masterFactory = masterFactory;
            this.slaveStates = slaveStates;
            this.offsets = offsets;
            this.recordFunctions = recordFunctions;

            this.cursor = new MultiHorizonJoinRecordCursor(
                    configuration,
                    horizonJoinMetadata,
                    groupByFunctions,
                    recordFunctions,
                    keyFunctions,
                    asm,
                    masterTimestampColumnIndex,
                    offsets,
                    slaveStates,
                    keyTypes,
                    valueTypes,
                    groupByColumnFilter,
                    columnSources,
                    columnIndexes
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
        TimeFrameCursor[] slaveCursors = new TimeFrameCursor[slaveStates.length];
        try {
            for (int i = 0; i < slaveStates.length; i++) {
                slaveCursors[i] = slaveStates[i].getFactory().getTimeFrameCursor(executionContext);
            }
            cursor.of(masterCursor, slaveCursors, executionContext);
            return cursor;
        } catch (Throwable th) {
            for (int i = 0; i < slaveCursors.length; i++) {
                Misc.free(slaveCursors[i]);
            }
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
        sink.type("Multi Horizon Join");
        sink.meta("offsets").val(offsets.size());
        sink.meta("slaves").val(slaveStates.length);
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.setMetadata(horizonJoinMetadata);
        sink.optAttr("values", cursor.groupByFunctions);
        sink.setMetadata(null);
        sink.child(masterFactory);
        for (HorizonJoinSlaveState ss : slaveStates) {
            sink.child(ss.getFactory());
        }
    }

    @Override
    protected void _close() {
        Misc.free(cursor);
        Misc.freeObjList(keyFunctions);
        Misc.free(masterFactory);
        for (HorizonJoinSlaveState ss : slaveStates) {
            Misc.free(ss);
            Misc.free(ss.getFactory());
        }
        Misc.free(horizonJoinMetadata);
        Misc.freeObjList(recordFunctions);
    }

    private static class MultiHorizonJoinRecordCursor implements RecordCursor {
        private final Map dataMap;
        private final GroupByAllocator groupByAllocator;
        private final ObjList<GroupByFunction> groupByFunctions;
        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        private final RecordSink groupByKeyCopier;
        private final HorizonTimestampIterator horizonIterator;
        private final MultiHorizonJoinRecord horizonJoinRecord;
        private final MultiHorizonJoinSymbolTableSource horizonJoinSymbolTableSource;
        private final ObjList<Function> keyFunctions;
        private final Record[] matchedSlaveRecords;
        private final int masterTimestampColumnIndex;
        private final LongList offsets;
        private final VirtualRecord recordA;
        private final VirtualRecord recordB;
        private final ObjList<Function> recordFunctions;
        private final int slaveCount;
        private final HorizonJoinSlaveState[] slaveStates;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isDataMapBuilt;
        private boolean isOpen;
        private MapRecordCursor mapCursor;
        private RecordCursor masterCursor;
        private TimeFrameCursor[] slaveCursors;

        MultiHorizonJoinRecordCursor(
                CairoConfiguration configuration,
                RecordMetadata horizonJoinMetadata,
                ObjList<GroupByFunction> groupByFunctions,
                ObjList<Function> recordFunctions,
                ObjList<Function> keyFunctions,
                @Transient BytecodeAssembler asm,
                int masterTimestampColumnIndex,
                LongList offsets,
                HorizonJoinSlaveState[] slaveStates,
                @Transient ArrayColumnTypes keyTypes,
                @Transient ArrayColumnTypes valueTypes,
                @Transient ListColumnFilter groupByColumnFilter,
                int[] columnSources,
                int[] columnIndexes
        ) {
            this.groupByFunctions = groupByFunctions;
            this.recordFunctions = recordFunctions;
            this.keyFunctions = keyFunctions;
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.offsets = offsets;
            this.slaveStates = slaveStates;
            this.slaveCount = slaveStates.length;
            this.matchedSlaveRecords = new Record[slaveCount];

            this.recordA = new VirtualRecord(recordFunctions);
            this.recordB = new VirtualRecord(recordFunctions);
            this.horizonJoinRecord = new MultiHorizonJoinRecord(slaveCount);
            this.horizonJoinRecord.init(columnSources, columnIndexes);
            this.horizonJoinSymbolTableSource = new MultiHorizonJoinSymbolTableSource(columnSources, columnIndexes, slaveCount);
            this.horizonIterator = new HorizonTimestampIterator(offsets);

            Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, groupByFunctions.size());
            this.groupByFunctionsUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, groupByFunctions);
            this.groupByAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(groupByFunctions, groupByAllocator);

            this.dataMap = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
            Class<RecordSink> sinkClass = RecordSinkFactory.getInstanceClass(
                    configuration, asm, horizonJoinMetadata, groupByColumnFilter,
                    keyFunctions, null, null, null, null
            );
            this.groupByKeyCopier = RecordSinkFactory.getInstance(
                    sinkClass, horizonJoinMetadata, groupByColumnFilter,
                    keyFunctions, null, null, null, null
            );

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
                if (slaveCursors != null) {
                    for (int i = 0; i < slaveCursors.length; i++) {
                        slaveCursors[i] = Misc.free(slaveCursors[i]);
                    }
                }
                mapCursor = Misc.free(mapCursor);
                Misc.free(dataMap);
                Misc.clearObjList(groupByFunctions);
                Misc.free(groupByAllocator);
                for (HorizonJoinSlaveState ss : slaveStates) {
                    if (ss.getAsOfJoinMap() != null) {
                        ss.getAsOfJoinMap().close();
                    }
                    Misc.clear(ss.getSymbolTranslatingRecord());
                }
                Misc.free(horizonIterator);
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
            for (int s = 0; s < slaveCount; s++) {
                slaveStates[s].getTimeFrameHelper().toTop();
                if (slaveStates[s].isKeyed() && slaveStates[s].getAsOfJoinMap() != null) {
                    slaveStates[s].getAsOfJoinMap().clear();
                }
            }
            dataMap.clear();

            while (horizonIterator.next()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                final long horizonTs = horizonIterator.getHorizonTimestamp();
                final long masterRowId = horizonIterator.getMasterRowId();
                final int offsetIdx = horizonIterator.getOffsetIndex();
                final long offset = offsets.getQuick(offsetIdx);

                masterCursor.recordAt(masterCursor.getRecordB(), masterRowId);
                Record masterRecord = masterCursor.getRecordB();

                // ASOF lookup on each slave
                for (int s = 0; s < slaveCount; s++) {
                    HorizonJoinSlaveState ss = slaveStates[s];
                    final long scaledHorizonTs = scaleTimestamp(horizonTs, ss.getMasterTsScale());
                    long asOfRowId = ss.getTimeFrameHelper().findAsOfRow(scaledHorizonTs);

                    long matchRowId = Long.MIN_VALUE;
                    if (ss.isKeyed()) {
                        Record masterKeyRecord = masterRecord;
                        if (ss.getSymbolTranslatingRecord() != null) {
                            ss.getSymbolTranslatingRecord().of(masterRecord);
                            masterKeyRecord = ss.getSymbolTranslatingRecord();
                        }

                        if (asOfRowId != Long.MIN_VALUE) {
                            if (ss.getTimeFrameHelper().getForwardWatermark() == Long.MIN_VALUE) {
                                matchRowId = ss.getTimeFrameHelper().backwardScanForKeyMatch(
                                        asOfRowId, masterKeyRecord,
                                        ss.getMasterAsOfJoinMapSink(), ss.getSlaveAsOfJoinMapSink(),
                                        ss.getAsOfJoinMap(), ss.getSymbolTranslatingRecord()
                                );
                                ss.getTimeFrameHelper().initForwardWatermark(asOfRowId);
                            } else {
                                ss.getTimeFrameHelper().forwardScanToPosition(
                                        asOfRowId, ss.getSlaveAsOfJoinMapSink(), ss.getAsOfJoinMap()
                                );

                                MapKey cacheKey = ss.getAsOfJoinMap().withKey();
                                cacheKey.put(masterKeyRecord, ss.getMasterAsOfJoinMapSink());
                                MapValue cacheValue = cacheKey.findValue();

                                if (cacheValue != null) {
                                    matchRowId = cacheValue.getLong(0);
                                } else {
                                    matchRowId = ss.getTimeFrameHelper().backwardScanForKeyMatch(
                                            asOfRowId, masterKeyRecord,
                                            ss.getMasterAsOfJoinMapSink(), ss.getSlaveAsOfJoinMapSink(),
                                            ss.getAsOfJoinMap(), ss.getSymbolTranslatingRecord()
                                    );
                                }
                            }
                        }
                    } else {
                        matchRowId = asOfRowId;
                    }

                    if (matchRowId != Long.MIN_VALUE) {
                        ss.getTimeFrameHelper().recordAt(matchRowId);
                        matchedSlaveRecords[s] = ss.getRecord();
                    } else {
                        matchedSlaveRecords[s] = null;
                    }
                }

                horizonJoinRecord.of(masterRecord, offset, horizonTs, matchedSlaveRecords);

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

        void of(RecordCursor masterCursor, TimeFrameCursor[] slaveCursors, SqlExecutionContext executionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                groupByAllocator.reopen();
                dataMap.reopen();
                for (HorizonJoinSlaveState ss : slaveStates) {
                    ss.reopen();
                }
            }
            this.circuitBreaker = executionContext.getCircuitBreaker();
            this.masterCursor = masterCursor;
            this.slaveCursors = slaveCursors;

            // Initialize each slave's time frame helper and symbol translating record
            SymbolTableSource[] slaveSymbolSources = new SymbolTableSource[slaveCount];
            for (int s = 0; s < slaveCount; s++) {
                slaveStates[s].getTimeFrameHelper().of(slaveCursors[s]);
                slaveStates[s].setTimeFrameCursor(slaveCursors[s]);
                slaveSymbolSources[s] = slaveCursors[s];
                if (slaveStates[s].getSymbolTranslatingRecord() != null) {
                    slaveStates[s].getSymbolTranslatingRecord().initSources(masterCursor, slaveCursors[s]);
                }
            }

            // Initialize horizon timestamp iterator with master cursor
            Record recordBMaster = masterCursor.getRecordB();
            horizonIterator.of(masterCursor, recordBMaster, masterTimestampColumnIndex);

            // Initialize symbol table sources
            horizonJoinSymbolTableSource.of(masterCursor, slaveSymbolSources);

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

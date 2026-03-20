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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
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
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Single-threaded factory for non-keyed HORIZON JOIN with multiple slave tables
 * (single output row).
 */
public class MultiHorizonJoinNotKeyedRecordCursorFactory extends AbstractRecordCursorFactory {
    private final MultiHorizonJoinNotKeyedRecordCursor cursor;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final JoinRecordMetadata horizonJoinMetadata;
    private final RecordCursorFactory masterFactory;
    private final long[] offsets;
    private final ObjList<HorizonJoinSlaveState> slaveStates;
    private final SimpleMapValue value;

    public MultiHorizonJoinNotKeyedRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull RecordMetadata metadata,
            @NotNull JoinRecordMetadata horizonJoinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull ObjList<HorizonJoinSlaveState> slaveStates,
            @Nullable Class<RecordSink> @NotNull [] masterAsOfJoinMapSinkClasses,
            @Nullable Class<RecordSink> @NotNull [] slaveAsOfJoinMapSinkClasses,
            long @NotNull [] offsets,
            int masterTimestampColumnIndex,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            int valueCount,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes
    ) {
        super(metadata);
        try {
            this.horizonJoinMetadata = horizonJoinMetadata;
            this.masterFactory = masterFactory;
            this.slaveStates = slaveStates;
            this.offsets = offsets;
            this.groupByFunctions = groupByFunctions;
            this.value = new SimpleMapValue(valueCount);

            ObjList<RecordSink> masterAsOfJoinMapSinks = new ObjList<>(slaveStates.size());
            ObjList<RecordSink> slaveAsOfJoinMapSinks = new ObjList<>(slaveStates.size());
            for (int i = 0; i < slaveStates.size(); i++) {
                masterAsOfJoinMapSinks.add(masterAsOfJoinMapSinkClasses[i] != null ? RecordSinkFactory.getInstance(masterAsOfJoinMapSinkClasses[i], null, null, null, null, null, null, null) : null);
                slaveAsOfJoinMapSinks.add(slaveAsOfJoinMapSinkClasses[i] != null ? RecordSinkFactory.getInstance(slaveAsOfJoinMapSinkClasses[i], null, null, null, null, null, null, null) : null);
            }

            this.cursor = new MultiHorizonJoinNotKeyedRecordCursor(
                    configuration,
                    asm,
                    masterTimestampColumnIndex,
                    offsets,
                    slaveStates,
                    groupByFunctions,
                    masterAsOfJoinMapSinks,
                    slaveAsOfJoinMapSinks,
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
        try {
            for (int i = 0; i < slaveStates.size(); i++) {
                cursor.slaveCursors.setQuick(i, slaveStates.getQuick(i).getFactory().getTimeFrameCursor(executionContext));
            }
            cursor.of(masterCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.freeObjList(cursor.slaveCursors);
            Misc.free(masterCursor);
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Multi Horizon Join");
        sink.meta("offsets").val(offsets.length);
        sink.meta("tables").val(slaveStates.size());
        sink.setMetadata(horizonJoinMetadata);
        sink.optAttr("values", groupByFunctions);
        sink.setMetadata(null);
        sink.child(masterFactory);
        for (int i = 0, n = slaveStates.size(); i < n; i++) {
            sink.child(slaveStates.getQuick(i).getFactory());
        }
    }

    @Override
    protected void _close() {
        Misc.free(value);
        Misc.free(cursor);
        Misc.free(masterFactory);
        Misc.freeObjList(slaveStates);
        Misc.free(horizonJoinMetadata);
        Misc.freeObjListAndClear(groupByFunctions);
    }

    private class MultiHorizonJoinNotKeyedRecordCursor implements NoRandomAccessRecordCursor {
        private final ObjList<Map> asOfJoinMaps;
        private final long bwdScanAbsoluteThreshold;
        private final long bwdScanMinGap;
        private final long[] bwdScanRowsAtPositionStarts;
        private final long bwdScanSwitchFactor;
        private final GroupByAllocator groupByAllocator;
        private final ObjList<GroupByFunction> groupByFunctions;
        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        private final HorizonTimestampIterator horizonIterator;
        private final MultiHorizonJoinRecord horizonJoinRecord;
        private final MultiHorizonJoinSymbolTableSource horizonJoinSymbolTableSource;
        private final boolean[] isForwardScanModes;
        private final ObjList<RecordSink> masterAsOfJoinMapSinks;
        private final int masterTimestampColumnIndex;
        private final ObjList<Record> matchedSlaveRecords;
        private final long[] offsets;
        private final long[] prevAsOfRowIds;
        private final VirtualRecord recordA;
        private final ObjList<RecordSink> slaveAsOfJoinMapSinks;
        private final int slaveCount;
        private final ObjList<TimeFrameCursor> slaveCursors;
        private final ObjList<HorizonJoinSlaveState> slaveStates;
        private final ObjList<SymbolTableSource> slaveSymbolSources;
        private final ObjList<SymbolTranslatingRecord> symbolTranslatingRecords;
        private final ObjList<HorizonJoinTimeFrameHelper> timeFrameHelpers;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isExhausted;
        private boolean isOpen;
        private boolean isValueBuilt;
        private RecordCursor masterCursor;

        MultiHorizonJoinNotKeyedRecordCursor(
                CairoConfiguration configuration,
                @Transient BytecodeAssembler asm,
                int masterTimestampColumnIndex,
                long[] offsets,
                ObjList<HorizonJoinSlaveState> slaveStates,
                ObjList<GroupByFunction> groupByFunctions,
                ObjList<RecordSink> masterAsOfJoinMapSinks,
                ObjList<RecordSink> slaveAsOfJoinMapSinks,
                int[] columnSources,
                int[] columnIndexes
        ) {
            this.groupByFunctions = groupByFunctions;
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.offsets = offsets;
            this.bwdScanAbsoluteThreshold = configuration.getSqlHorizonJoinBwdScanAbsoluteThreshold();
            this.bwdScanMinGap = configuration.getSqlHorizonJoinBwdScanMinGap();
            this.bwdScanSwitchFactor = configuration.getSqlHorizonJoinBwdScanSwitchFactor();
            this.slaveStates = slaveStates;
            this.slaveCount = slaveStates.size();
            this.prevAsOfRowIds = new long[slaveCount];
            this.isForwardScanModes = new boolean[slaveCount];
            this.bwdScanRowsAtPositionStarts = new long[slaveCount];
            this.slaveSymbolSources = new ObjList<>(slaveCount);
            this.slaveSymbolSources.setPos(slaveCount);
            this.matchedSlaveRecords = new ObjList<>(slaveCount);
            this.matchedSlaveRecords.setPos(slaveCount);
            this.masterAsOfJoinMapSinks = masterAsOfJoinMapSinks;
            this.slaveAsOfJoinMapSinks = slaveAsOfJoinMapSinks;

            this.recordA = new VirtualRecord(groupByFunctions);
            this.horizonJoinRecord = new MultiHorizonJoinRecord(slaveCount);
            this.horizonJoinRecord.init(columnSources, columnIndexes);
            this.horizonJoinSymbolTableSource = new MultiHorizonJoinSymbolTableSource(columnSources, columnIndexes, slaveCount);
            this.horizonIterator = new HorizonTimestampIterator(offsets);

            Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, groupByFunctions.size());
            this.groupByFunctionsUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, groupByFunctions);

            this.asOfJoinMaps = new ObjList<>(slaveCount);
            this.slaveCursors = new ObjList<>(slaveCount);
            this.slaveCursors.setPos(slaveCount);
            this.symbolTranslatingRecords = new ObjList<>(slaveCount);
            this.timeFrameHelpers = new ObjList<>(slaveCount);
            this.isOpen = true;
            try {
                this.groupByAllocator = GroupByAllocatorFactory.createAllocator(configuration);
                GroupByUtils.setAllocator(groupByFunctions, groupByAllocator);

                // Create per-slave maps, symbol translating records, and time frame helpers
                final long lookahead = configuration.getSqlAsOfJoinLookAhead();
                for (int s = 0; s < slaveCount; s++) {
                    HorizonJoinSlaveState ss = slaveStates.getQuick(s);
                    if (ss.getAsOfJoinKeyTypes() != null) {
                        asOfJoinMaps.add(MapFactory.createUnorderedMap(configuration, ss.getAsOfJoinKeyTypes(), new SingleColumnType(ColumnType.LONG)));
                    } else {
                        asOfJoinMaps.add(null);
                    }
                    if (ss.getMasterSymbolKeyColumnIndices() != null) {
                        symbolTranslatingRecords.add(new SymbolTranslatingRecord(ss.getMasterColumnCount(), ss.getMasterSymbolKeyColumnIndices(), ss.getSlaveSymbolKeyColumnIndices()));
                    } else {
                        symbolTranslatingRecords.add(null);
                    }
                    timeFrameHelpers.add(new HorizonJoinTimeFrameHelper(lookahead, ss.getSlaveTsScale()));
                }
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (!isExhausted) {
                counter.inc();
                isExhausted = true;
            }
        }

        @Override
        public void close() {
            if (isOpen) {
                masterCursor = Misc.free(masterCursor);
                Misc.freeObjListAndKeepObjects(slaveCursors);
                Misc.clearObjList(groupByFunctions);
                Misc.free(groupByAllocator);
                Misc.freeObjListAndKeepObjects(asOfJoinMaps);
                Misc.clearObjList(symbolTranslatingRecords);
                Misc.free(horizonIterator);
                isOpen = false;
            }
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return (SymbolTable) groupByFunctions.getQuick(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (isExhausted) {
                return false;
            }
            if (!isValueBuilt) {
                buildValue();
            }
            isExhausted = true;
            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return ((SymbolFunction) groupByFunctions.getQuick(columnIndex)).newSymbolTable();
        }

        @Override
        public long preComputedStateSize() {
            return RecordCursor.fromBool(isValueBuilt);
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            isExhausted = false;
        }

        private void buildValue() {
            for (int s = 0; s < slaveCount; s++) {
                timeFrameHelpers.getQuick(s).toTop();
                if (slaveStates.getQuick(s).isKeyed() && asOfJoinMaps.getQuick(s) != null) {
                    asOfJoinMaps.getQuick(s).clear();
                }
                prevAsOfRowIds[s] = Long.MIN_VALUE;
                isForwardScanModes[s] = false;
                bwdScanRowsAtPositionStarts[s] = 0;
            }

            while (horizonIterator.next()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                final long horizonTs = horizonIterator.getHorizonTimestamp();
                final long masterRowId = horizonIterator.getMasterRowId();
                final int offsetIdx = horizonIterator.getOffsetIndex();
                final long offset = offsets[offsetIdx];

                masterCursor.recordAt(masterCursor.getRecordB(), masterRowId);
                Record masterRecord = masterCursor.getRecordB();

                for (int s = 0; s < slaveCount; s++) {
                    HorizonJoinSlaveState ss = slaveStates.getQuick(s);
                    final HorizonJoinTimeFrameHelper helper = timeFrameHelpers.getQuick(s);
                    final long scaledHorizonTs = scaleTimestamp(horizonTs, ss.getMasterTsScale());
                    long asOfRowId = helper.findAsOfRow(scaledHorizonTs);

                    long matchRowId = Long.MIN_VALUE;
                    if (ss.isKeyed()) {
                        Record masterKeyRecord = masterRecord;
                        if (symbolTranslatingRecords.getQuick(s) != null) {
                            symbolTranslatingRecords.getQuick(s).of(masterRecord);
                            masterKeyRecord = symbolTranslatingRecords.getQuick(s);
                        }

                        if (asOfRowId != Long.MIN_VALUE) {
                            if (asOfRowId != prevAsOfRowIds[s]) {
                                if (!isForwardScanModes[s]) {
                                    long bwdScanCost = helper.getBackwardScanRows() - bwdScanRowsAtPositionStarts[s];
                                    if (prevAsOfRowIds[s] != Long.MIN_VALUE) {
                                        long gap = asOfRowId - prevAsOfRowIds[s];
                                        if (HorizonJoinTimeFrameHelper.shouldSwitchToForwardScan(
                                                bwdScanCost,
                                                gap,
                                                bwdScanMinGap,
                                                bwdScanSwitchFactor,
                                                bwdScanAbsoluteThreshold
                                        )) {
                                            isForwardScanModes[s] = true;
                                            helper.initForwardWatermark(prevAsOfRowIds[s]);
                                        }
                                    }
                                    if (!isForwardScanModes[s]) {
                                        asOfJoinMaps.getQuick(s).clear();
                                        helper.resetBackwardWatermark();
                                        bwdScanRowsAtPositionStarts[s] = helper.getBackwardScanRows();
                                    }
                                }
                                if (isForwardScanModes[s]) {
                                    helper.forwardScanToPosition(
                                            asOfRowId, slaveAsOfJoinMapSinks.getQuick(s), asOfJoinMaps.getQuick(s)
                                    );
                                }
                                prevAsOfRowIds[s] = asOfRowId;
                            }
                            matchRowId = helper.backwardScanForKeyMatch(
                                    asOfRowId,
                                    masterKeyRecord,
                                    masterAsOfJoinMapSinks.getQuick(s),
                                    slaveAsOfJoinMapSinks.getQuick(s),
                                    asOfJoinMaps.getQuick(s),
                                    symbolTranslatingRecords.getQuick(s)
                            );
                        }
                    } else {
                        matchRowId = asOfRowId;
                    }

                    if (matchRowId != Long.MIN_VALUE) {
                        helper.recordAt(matchRowId);
                        matchedSlaveRecords.setQuick(s, helper.getRecord());
                    } else {
                        matchedSlaveRecords.setQuick(s, null);
                    }
                }

                horizonJoinRecord.of(masterRecord, offset, horizonTs, matchedSlaveRecords);
                if (value.isNew()) {
                    groupByFunctionsUpdater.updateNew(value, horizonJoinRecord, masterRowId);
                    value.setNew(false);
                } else {
                    groupByFunctionsUpdater.updateExisting(value, horizonJoinRecord, masterRowId);
                }
            }

            isValueBuilt = true;
        }

        void of(RecordCursor masterCursor, SqlExecutionContext executionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                groupByAllocator.reopen();
                for (int s = 0; s < slaveCount; s++) {
                    if (asOfJoinMaps.getQuick(s) != null) {
                        asOfJoinMaps.getQuick(s).reopen();
                    }
                }
            }
            this.circuitBreaker = executionContext.getCircuitBreaker();
            this.masterCursor = masterCursor;

            for (int s = 0; s < slaveCount; s++) {
                timeFrameHelpers.getQuick(s).of(slaveCursors.getQuick(s));
                slaveSymbolSources.setQuick(s, slaveCursors.getQuick(s));
                if (symbolTranslatingRecords.getQuick(s) != null) {
                    symbolTranslatingRecords.getQuick(s).initSources(masterCursor, slaveCursors.getQuick(s));
                }
            }

            Record recordB = masterCursor.getRecordB();
            horizonIterator.of(masterCursor, recordB, masterTimestampColumnIndex);
            horizonJoinSymbolTableSource.of(masterCursor, slaveSymbolSources);

            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                groupByFunctions.getQuick(i).init(horizonJoinSymbolTableSource, executionContext);
            }

            recordA.of(value);
            groupByFunctionsUpdater.updateEmpty(value);
            value.setNew(true);

            isValueBuilt = false;
            isExhausted = false;
        }
    }
}

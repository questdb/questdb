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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
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
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Single-threaded factory for non-keyed HORIZON JOIN (single output row).
 * <p>
 * Uses globally sorted horizon timestamp iteration via
 * {@link HorizonTimestampIterator} for efficient monotonic ASOF scans
 * through the slave. Requires the master cursor to support random access.
 */
public class HorizonJoinNotKeyedRecordCursorFactory extends AbstractRecordCursorFactory {
    private final HorizonJoinNotKeyedRecordCursor cursor;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final RecordMetadata horizonJoinMetadata;
    private final RecordCursorFactory masterFactory;
    private final LongList offsets;
    private final RecordCursorFactory slaveFactory;
    private final SimpleMapValue value;

    public HorizonJoinNotKeyedRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull RecordMetadata horizonJoinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            @NotNull LongList offsets,
            int masterTimestampColumnIndex,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            int valueCount,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterAsOfJoinMapSink,
            @Nullable RecordSink slaveAsOfJoinMapSink,
            int masterColumnCount,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
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
            this.groupByFunctions = groupByFunctions;
            this.value = new SimpleMapValue(valueCount);

            // Compute timestamp scale factors for cross-resolution support
            final int masterTsType = masterFactory.getMetadata().getTimestampType();
            final int slaveTsType = slaveFactory.getMetadata().getTimestampType();
            long masterTsScale = 1;
            long slaveTsScale = 1;
            if (masterTsType != slaveTsType) {
                masterTsScale = ColumnType.getTimestampDriver(masterTsType).toNanosScale();
                slaveTsScale = ColumnType.getTimestampDriver(slaveTsType).toNanosScale();
            }

            this.cursor = new HorizonJoinNotKeyedRecordCursor(
                    configuration,
                    groupByFunctions,
                    asm,
                    masterTimestampColumnIndex,
                    offsets,
                    asOfJoinKeyTypes,
                    masterAsOfJoinMapSink,
                    slaveAsOfJoinMapSink,
                    masterColumnCount,
                    masterSymbolKeyColumnIndices,
                    slaveSymbolKeyColumnIndices,
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
        TimeFrameCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(masterCursor);
            Misc.free(slaveCursor);
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Horizon Join");
        sink.meta("offsets").val(offsets.size());
        sink.setMetadata(horizonJoinMetadata);
        sink.optAttr("values", groupByFunctions);
        sink.setMetadata(null);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.free(value);
        Misc.free(cursor);
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.freeObjListAndClear(groupByFunctions);
        Misc.freeIfCloseable(horizonJoinMetadata);
    }

    private class HorizonJoinNotKeyedRecordCursor implements NoRandomAccessRecordCursor {
        private final Map asOfJoinMap;
        private final GroupByAllocator groupByAllocator;
        private final ObjList<GroupByFunction> groupByFunctions;
        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        private final HorizonTimestampIterator horizonIterator;
        private final HorizonJoinRecord horizonJoinRecord;
        private final HorizonJoinSymbolTableSource horizonJoinSymbolTableSource;
        private final RecordSink masterAsOfJoinMapSink;
        private final int masterTimestampColumnIndex;
        private final long masterTsScale;
        private final LongList offsets;
        private final VirtualRecord recordA;
        private final RecordSink slaveAsOfJoinMapSink;
        private final MarkoutTimeFrameHelper slaveTimeFrameHelper;
        private final SymbolTranslatingRecord symbolTranslatingRecord;
        private boolean isExhausted;
        private boolean isOpen;
        private boolean isValueBuilt;
        private RecordCursor masterCursor;
        private TimeFrameCursor slaveCursor;

        HorizonJoinNotKeyedRecordCursor(
                CairoConfiguration configuration,
                ObjList<GroupByFunction> groupByFunctions,
                @Transient BytecodeAssembler asm,
                int masterTimestampColumnIndex,
                LongList offsets,
                @Nullable ColumnTypes asOfJoinKeyTypes,
                @Nullable RecordSink masterAsOfJoinMapSink,
                @Nullable RecordSink slaveAsOfJoinMapSink,
                int masterColumnCount,
                int @Nullable [] masterSymbolKeyColumnIndices,
                int @Nullable [] slaveSymbolKeyColumnIndices,
                int[] columnSources,
                int[] columnIndexes,
                long masterTsScale,
                long slaveTsScale
        ) {
            this.groupByFunctions = groupByFunctions;
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.offsets = offsets;
            this.masterTsScale = masterTsScale;
            this.masterAsOfJoinMapSink = masterAsOfJoinMapSink;
            this.slaveAsOfJoinMapSink = slaveAsOfJoinMapSink;

            this.recordA = new VirtualRecord(groupByFunctions);
            this.horizonJoinRecord = new HorizonJoinRecord();
            this.horizonJoinRecord.init(columnSources, columnIndexes);
            this.horizonJoinSymbolTableSource = new HorizonJoinSymbolTableSource(columnSources, columnIndexes);
            this.horizonIterator = new HorizonTimestampIterator(offsets);

            Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, groupByFunctions.size());
            this.groupByFunctionsUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, groupByFunctions);
            this.groupByAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(groupByFunctions, groupByAllocator);

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
            if (!isExhausted) {
                counter.inc();
                isExhausted = true;
            }
        }

        @Override
        public void close() {
            if (isOpen) {
                masterCursor = Misc.free(masterCursor);
                slaveCursor = Misc.free(slaveCursor);
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
            final boolean keyedAsOfJoin = asOfJoinMap != null && masterAsOfJoinMapSink != null && slaveAsOfJoinMapSink != null;

            slaveTimeFrameHelper.toTop();
            if (keyedAsOfJoin) {
                asOfJoinMap.clear();
            }

            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            while (horizonIterator.next()) {
                final long horizonTs = horizonIterator.getHorizonTimestamp();
                final long masterRowId = horizonIterator.getMasterRowId();
                final int offsetIdx = horizonIterator.getOffsetIndex();
                final long offset = offsets.getQuick(offsetIdx);

                // Position master record at the correct row via random access
                masterCursor.recordAt(masterCursor.getRecordB(), masterRowId);
                Record masterRecord = masterCursor.getRecordB();

                // Scale horizon timestamp for ASOF lookup
                final long scaledHorizonTs = scaleTimestamp(horizonTs, masterTsScale);

                // Find ASOF row for this horizon timestamp
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
                if (value.isNew()) {
                    groupByFunctionsUpdater.updateNew(value, horizonJoinRecord, masterRowId);
                    value.setNew(false);
                } else {
                    groupByFunctionsUpdater.updateExisting(value, horizonJoinRecord, masterRowId);
                }
            }

            isValueBuilt = true;
        }

        void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionContext executionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                groupByAllocator.reopen();
                if (asOfJoinMap != null) {
                    asOfJoinMap.reopen();
                }
            }
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            slaveTimeFrameHelper.of(slaveCursor);

            // Initialize horizon timestamp iterator with master cursor
            Record recordB = masterCursor.getRecordB();
            horizonIterator.of(masterCursor, recordB, masterTimestampColumnIndex);

            // Initialize symbol table sources
            horizonJoinSymbolTableSource.of(masterCursor, slaveCursor);

            // Initialize symbol translating record
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.initSources(masterCursor, slaveCursor);
            }

            // Initialize group by functions
            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                groupByFunctions.getQuick(i).init(horizonJoinSymbolTableSource, executionContext);
            }

            // Reset value
            recordA.of(value);
            groupByFunctionsUpdater.updateEmpty(value);
            value.setNew(true);

            isValueBuilt = false;
            isExhausted = false;
        }
    }
}

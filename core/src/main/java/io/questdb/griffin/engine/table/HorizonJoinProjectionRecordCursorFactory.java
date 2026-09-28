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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Streams one row per master row and offset when a HORIZON JOIN has no aggregation.
 * Unlike the aggregate factories, this cursor never groups or materializes its output.
 * The timestamp iterator and ASOF helpers keep the same monotonic slave scans.
 */
public class HorizonJoinProjectionRecordCursorFactory extends AbstractRecordCursorFactory {
    private final long[] offsets;
    private HorizonJoinProjectionRecordCursor cursor;
    private JoinRecordMetadata horizonJoinMetadata;
    private RecordCursorFactory masterFactory;
    private ObjList<HorizonJoinSlaveState> slaveStates;

    public HorizonJoinProjectionRecordCursorFactory(
            CairoConfiguration configuration,
            JoinRecordMetadata metadata,
            RecordCursorFactory masterFactory,
            ObjList<HorizonJoinSlaveState> slaveStates,
            ObjList<RecordSink> masterSinks,
            ObjList<RecordSink> slaveSinks,
            long[] offsets,
            int masterTimestampColumnIndex,
            int[] columnSources,
            int[] columnIndexes
    ) {
        super(metadata);
        this.horizonJoinMetadata = metadata;
        this.masterFactory = masterFactory;
        this.slaveStates = slaveStates;
        this.offsets = offsets;
        try {
            cursor = new HorizonJoinProjectionRecordCursor(
                    configuration, slaveStates, masterSinks, slaveSinks, offsets,
                    masterTimestampColumnIndex, columnSources, columnIndexes
            );
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return masterFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        try {
            // Adopt each cursor before opening the next resource, including before of() can throw.
            cursor.masterCursor = masterFactory.getCursor(executionContext);
            cursor.masterCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            for (int s = 0, n = slaveStates.size(); s < n; s++) {
                TimeFrameCursor slaveCursor = slaveStates.getQuick(s).getFactory().getTimeFrameCursor(executionContext);
                cursor.slaveCursors.setQuick(s, slaveCursor);
                slaveCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
            }
            cursor.of(executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(cursor, th);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return SCAN_DIRECTION_OTHER;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Horizon Join Projection").meta("offsets").val(offsets.length);
        sink.child(masterFactory);
        for (int s = 0, n = slaveStates.size(); s < n; s++) {
            sink.child(slaveStates.getQuick(s).getFactory());
        }
    }

    @Override
    public boolean usesExternalDataSource() {
        if (masterFactory != null && masterFactory.usesExternalDataSource()) {
            return true;
        }
        for (int s = 0, n = slaveStates != null ? slaveStates.size() : 0; s < n; s++) {
            if (slaveStates.getQuick(s).getFactory().usesExternalDataSource()) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void _close() {
        final HorizonJoinProjectionRecordCursor cursor = this.cursor;
        this.cursor = null;
        final RecordCursorFactory masterFactory = this.masterFactory;
        this.masterFactory = null;
        final ObjList<HorizonJoinSlaveState> slaveStates = this.slaveStates;
        this.slaveStates = null;
        final JoinRecordMetadata metadata = horizonJoinMetadata;
        horizonJoinMetadata = null;
        Throwable failure = Misc.freeBestEffort(null, cursor);
        failure = Misc.freeBestEffort(failure, masterFactory);
        failure = Misc.freeObjListBestEffort(failure, slaveStates);
        failure = Misc.freeBestEffort(failure, metadata);
        CairoException.rethrowCleanupFailure(failure);
    }

    private static class HorizonJoinProjectionRecordCursor implements RecordCursor {
        private final ObjList<Map> asOfJoinMaps;
        private final HorizonTimestampIterator horizonIterator;
        private final ObjList<RecordSink> masterSinks;
        private final int masterTimestampColumnIndex;
        private final ObjList<Record> matchedSlaveRecords;
        private final ObjList<Record> nullSlaveRecords;
        private final long[] offsets;
        private final MultiHorizonJoinRecord record;
        private final ObjList<TimeFrameCursor> slaveCursors;
        private final ObjList<RecordSink> slaveSinks;
        private final ObjList<HorizonJoinSlaveState> slaveStates;
        private final ObjList<SymbolTableSource> slaveSymbolSources;
        private final MultiHorizonJoinSymbolTableSource symbolTableSource;
        private final ObjList<SymbolTranslatingRecord> symbolTranslatingRecords;
        private final ObjList<HorizonJoinTimeFrameHelper> timeFrameHelpers;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private RecordCursor masterCursor;

        private HorizonJoinProjectionRecordCursor(
                CairoConfiguration configuration,
                ObjList<HorizonJoinSlaveState> slaveStates,
                ObjList<RecordSink> masterSinks,
                ObjList<RecordSink> slaveSinks,
                long[] offsets,
                int masterTimestampColumnIndex,
                int[] columnSources,
                int[] columnIndexes
        ) {
            final int slaveCount = slaveStates.size();
            this.slaveStates = slaveStates;
            this.masterSinks = masterSinks;
            this.slaveSinks = slaveSinks;
            this.offsets = offsets;
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            asOfJoinMaps = new ObjList<>(slaveCount);
            slaveCursors = new ObjList<>(slaveCount);
            slaveCursors.setPos(slaveCount);
            slaveSymbolSources = new ObjList<>(slaveCount);
            slaveSymbolSources.setPos(slaveCount);
            matchedSlaveRecords = new ObjList<>(slaveCount);
            matchedSlaveRecords.setPos(slaveCount);
            nullSlaveRecords = new ObjList<>(slaveCount);
            symbolTranslatingRecords = new ObjList<>(slaveCount);
            timeFrameHelpers = new ObjList<>(slaveCount);
            record = new MultiHorizonJoinRecord(slaveCount);
            record.init(columnSources, columnIndexes);
            symbolTableSource = new MultiHorizonJoinSymbolTableSource(columnSources, columnIndexes, slaveCount);
            horizonIterator = new HorizonTimestampIterator(offsets);
            try {
                for (int s = 0; s < slaveCount; s++) {
                    HorizonJoinSlaveState state = slaveStates.getQuick(s);
                    // Typed null records carry VALUE_IS_NULL for SYMBOL keys, not INT_NULL.
                    nullSlaveRecords.add(NullRecordFactory.getInstance(state.getFactory().getMetadata()));
                    asOfJoinMaps.add(state.isKeyed()
                            ? MapFactory.createUnorderedMap(configuration, state.getAsOfJoinKeyTypes(), new SingleColumnType(ColumnType.LONG), false, false)
                            : null);
                    symbolTranslatingRecords.add(state.getMasterSymbolKeyColumnIndices() != null
                            ? new SymbolTranslatingRecord(state.getMasterColumnCount(), state.getMasterSymbolKeyColumnIndices(), state.getSlaveSymbolKeyColumnIndices())
                            : null);
                    timeFrameHelpers.add(new HorizonJoinTimeFrameHelper(
                            configuration.getSqlAsOfJoinLookAhead(),
                            state.getSlaveTsScale(),
                            configuration.getSqlHorizonJoinBwdScanAbsoluteThreshold(),
                            configuration.getSqlHorizonJoinBwdScanMinGap(),
                            configuration.getSqlHorizonJoinBwdScanSwitchFactor()
                    ));
                }
            } catch (Throwable th) {
                Misc.free(this, th);
                throw th;
            }
        }

        @Override
        public void close() {
            final RecordCursor masterCursor = this.masterCursor;
            this.masterCursor = null;
            Throwable failure = null;
            for (int s = 0, n = symbolTranslatingRecords.size(); s < n; s++) {
                failure = Misc.freeBestEffort(failure, symbolTranslatingRecords.getQuick(s));
            }
            failure = Misc.freeBestEffort(failure, masterCursor);
            failure = Misc.freeObjListBestEffort(failure, slaveCursors);
            // Keep the map objects so the cached factory can reopen them on its next execution.
            for (int s = 0, n = asOfJoinMaps.size(); s < n; s++) {
                failure = Misc.freeBestEffort(failure, asOfJoinMaps.getQuick(s));
            }
            failure = Misc.freeBestEffort(failure, horizonIterator);
            CairoException.rethrowCleanupFailure(failure);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return symbolTableSource.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            if (!horizonIterator.next()) {
                return false;
            }
            final long horizonTs = horizonIterator.getHorizonTimestamp();
            final Record masterRecord = masterCursor.getRecordB();
            masterCursor.recordAt(masterRecord, horizonIterator.getMasterRowId());
            for (int s = 0, n = slaveStates.size(); s < n; s++) {
                final HorizonJoinSlaveState state = slaveStates.getQuick(s);
                final HorizonJoinTimeFrameHelper helper = timeFrameHelpers.getQuick(s);
                long matchRowId = helper.findAsOfRow(scaleTimestamp(horizonTs, state.getMasterTsScale()));
                if (state.isKeyed()) {
                    Record keyRecord = masterRecord;
                    final SymbolTranslatingRecord translatingRecord = symbolTranslatingRecords.getQuick(s);
                    if (translatingRecord != null) {
                        translatingRecord.of(masterRecord);
                        keyRecord = translatingRecord;
                    }
                    matchRowId = helper.findKeyedAsOfMatch(
                            matchRowId, keyRecord, masterSinks.getQuick(s), slaveSinks.getQuick(s),
                            asOfJoinMaps.getQuick(s), translatingRecord
                    );
                }
                if (matchRowId != Long.MIN_VALUE) {
                    helper.recordAt(matchRowId);
                    matchedSlaveRecords.setQuick(s, helper.getRecord());
                } else {
                    matchedSlaveRecords.setQuick(s, nullSlaveRecords.getQuick(s));
                }
            }
            record.of(masterRecord, offsets[horizonIterator.getOffsetIndex()], horizonTs, matchedSlaveRecords);
            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return symbolTableSource.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return masterCursor.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            final long masterSize = masterCursor.size();
            return masterSize >= 0 && masterSize <= Long.MAX_VALUE / offsets.length ? masterSize * offsets.length : -1;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            for (int s = 0, n = slaveStates.size(); s < n; s++) {
                timeFrameHelpers.getQuick(s).toTop();
                final Map map = asOfJoinMaps.getQuick(s);
                if (map != null) {
                    map.clear();
                }
            }
            horizonIterator.of(masterCursor, masterCursor.getRecordB(), masterTimestampColumnIndex);
        }

        private void of(SqlExecutionContext executionContext) {
            circuitBreaker = executionContext.getCircuitBreaker();
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            for (int s = 0, n = slaveStates.size(); s < n; s++) {
                final Map map = asOfJoinMaps.getQuick(s);
                if (map != null) {
                    map.setMemoryTracker(executionContext.getMemoryTracker());
                    map.reopen();
                    map.clear();
                }
                final TimeFrameCursor slaveCursor = slaveCursors.getQuick(s);
                timeFrameHelpers.getQuick(s).of(slaveCursor);
                slaveSymbolSources.setQuick(s, slaveCursor);
                final SymbolTranslatingRecord translatingRecord = symbolTranslatingRecords.getQuick(s);
                if (translatingRecord != null) {
                    translatingRecord.initSources(masterCursor, slaveCursor);
                }
            }
            symbolTableSource.of(masterCursor, slaveSymbolSources);
            horizonIterator.of(masterCursor, masterCursor.getRecordB(), masterTimestampColumnIndex);
        }
    }
}

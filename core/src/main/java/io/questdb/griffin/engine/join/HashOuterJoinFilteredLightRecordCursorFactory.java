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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordIdSink;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.SymbolTranslatingRecord;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Same as HashOuterJoinLightRecordCursorFactory but with added filtering (for non-equality
 * or complex join conditions that use functions).
 */
public class HashOuterJoinFilteredLightRecordCursorFactory extends AbstractJoinRecordCursorFactory {

    private final int columnSplit;
    private final JoinSymbolTableSource filterSymbolTableSource;
    private final int joinType;
    private final RecordSink masterKeySink;
    private final int @Nullable [] masterSymbolKeyColumnIndices;
    private final JoinInput.NullExtension nullExtension;
    private final RecordSink slaveKeySink;
    private final int @Nullable [] slaveSymbolKeyColumnIndices;
    private AbstractHashOuterJoinLightRecordCursor cursor;
    private Function filter;
    private Map joinKeyMap;
    private Map matchIdsMap;
    private @Nullable Function nullKeyCheck;
    private LongChain slaveChain;
    private @Nullable SymbolTranslatingRecord symbolTranslatingRecord;

    public HashOuterJoinFilteredLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just INT + optional BOOLEAN, we store chain references in map
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            int columnSplit,
            @NotNull Function filter,
            Plannable joinContext,
            int joinType,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
            JoinInput.NullExtension nullExtension,
            @Nullable Function nullKeyCheck
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        this.filter = filter;
        this.nullExtension = nullExtension;
        this.nullKeyCheck = nullKeyCheck;
        this.masterSymbolKeyColumnIndices = masterSymbolKeyColumnIndices;
        this.slaveSymbolKeyColumnIndices = slaveSymbolKeyColumnIndices;
        try {
            this.symbolTranslatingRecord = masterSymbolKeyColumnIndices != null
                    ? new SymbolTranslatingRecord(
                    configuration,
                    Math.max(masterFactory.getMetadata().getColumnCount(), slaveFactory.getMetadata().getColumnCount()),
                    masterSymbolKeyColumnIndices.length
            )
                    : null;
            this.masterKeySink = masterKeySink;
            this.slaveKeySink = slaveKeySink;
            this.joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes, false, false);
            this.slaveChain = new LongChain(configuration.getSqlHashJoinLightValuePageSize(), configuration.getSqlHashJoinLightValueMaxPages(), true);
            this.columnSplit = columnSplit;
            this.joinType = joinType;
            if (joinType != QueryModel.JOIN_LEFT_OUTER) {
                matchIdsMap = MapFactory.createUnorderedMap(configuration, RecordIdSink.RECORD_ID_COLUMN_TYPE, ArrayColumnTypes.EMPTY, false, false);
            }
            this.filterSymbolTableSource = new JoinSymbolTableSource(columnSplit);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return joinType == QueryModel.JOIN_LEFT_OUTER && masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor == null) {
            switch (joinType) {
                case QueryModel.JOIN_LEFT_OUTER:
                    cursor = new HashLeftOuterJoinLightRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                            joinKeyMap,
                            slaveChain
                    );
                    break;
                case QueryModel.JOIN_RIGHT_OUTER:
                    cursor = new HashRightOuterJoinLightRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(masterFactory.getMetadata()),
                            joinKeyMap,
                            matchIdsMap,
                            slaveChain
                    );
                    break;
                case QueryModel.JOIN_FULL_OUTER:
                    cursor = new HashFullOuterJoinLightRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(masterFactory.getMetadata()),
                            NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                            joinKeyMap,
                            matchIdsMap,
                            slaveChain
                    );
                    break;
                default:
                    assert false : "invalid join type " + joinType;
            }
            this.joinKeyMap = null;
            this.slaveChain = null;
            this.matchIdsMap = null;
        }

        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        RecordCursor masterCursor = null;
        try {
            masterCursor = masterFactory.getCursor(executionContext);
            if (joinType == QueryModel.JOIN_FULL_OUTER) {
                boolean swapped = false;
                if (nullExtension == JoinInput.NullExtension.BY_KEYS && masterFactory.recordCursorSupportsRandomAccess()) {
                    long masterSize = masterCursor.size();
                    long slaveSize = slaveCursor.size();

                    if (masterSize > 0 && slaveSize > 0 && masterSize < slaveSize) {
                        RecordCursor temp = masterCursor;
                        masterCursor = slaveCursor;
                        slaveCursor = temp;
                        swapped = true;
                    }
                }
                if (swapped) {
                    slaveCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
                    ((HashFullOuterJoinLightRecordCursor) cursor).of(masterCursor, slaveCursor, executionContext, true);
                    return cursor;
                }
            }
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            cursor.of(masterCursor, slaveCursor, executionContext);
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            // of() binds the per-query tracker and reopens the slave chain, join map and match-ids map
            // before it can throw; close() frees them under that tracker and resets isOpen for reuse.
            Misc.free(cursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        return joinType == QueryModel.JOIN_LEFT_OUTER ? masterFactory.getScanDirection() : SCAN_DIRECTION_OTHER;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Hash ").val(outerJoinTypeToString(joinType)).val(" Outer Join Light");
        sink.optAttr("condition", joinContext);
        if (symbolTranslatingRecord != null) {
            sink.attr("symbolKeyJoin").val(true);
        }
        sink.attr("filter").val(filter);
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    protected static CharSequence outerJoinTypeToString(int joinType) {
        return switch (joinType) {
            case QueryModel.JOIN_LEFT_OUTER -> "Left";
            case QueryModel.JOIN_RIGHT_OUTER -> "Right";
            case QueryModel.JOIN_FULL_OUTER -> "Full";
            default -> "Unknown";
        };
    }

    @Override
    protected void _close() {
        final AbstractHashOuterJoinLightRecordCursor cursor = this.cursor;
        this.cursor = null;
        final Function filter = this.filter;
        this.filter = null;
        final Function nullKeyCheck = this.nullKeyCheck;
        this.nullKeyCheck = null;
        final Map joinKeyMap = this.joinKeyMap;
        this.joinKeyMap = null;
        final Map matchIdsMap = this.matchIdsMap;
        this.matchIdsMap = null;
        final LongChain slaveChain = this.slaveChain;
        this.slaveChain = null;
        final SymbolTranslatingRecord symbolTranslatingRecord = this.symbolTranslatingRecord;
        this.symbolTranslatingRecord = null;
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeBestEffort(failure, filter);
        failure = Misc.freeBestEffort(failure, nullKeyCheck);
        failure = Misc.freeBestEffort(failure, joinKeyMap);
        failure = Misc.freeBestEffort(failure, slaveChain);
        if (matchIdsMap != joinKeyMap) {
            failure = Misc.freeBestEffort(failure, matchIdsMap);
        }
        failure = Misc.freeBestEffort(failure, symbolTranslatingRecord);
        CairoException.rethrowCleanupFailure(failure);
    }

    private abstract class AbstractHashOuterJoinFilteredLightRecordCursor extends AbstractHashOuterJoinLightRecordCursor {
        final HashOuterJoinNullKeys nullKeys;

        AbstractHashOuterJoinFilteredLightRecordCursor(int columnSplit, Map joinKeyMap, LongChain slaveChain) {
            super(columnSplit, joinKeyMap, slaveChain, symbolTranslatingRecord);
            nullKeys = new HashOuterJoinNullKeys(new NullKeyChains(), filter, joinKeyMap, masterKeySink, nullExtension, nullKeyCheck);
        }

        @Override
        public void close() {
            super.close();
            // _close() detaches the filter and the NULL key check before it frees this cursor
            if (filter != null) {
                filter.cursorClosed();
            }
            if (nullKeyCheck != null) {
                nullKeyCheck.cursorClosed();
            }
        }

        @Override
        public void toTop() {
            super.toTop();
            filter.toTop();
            if (nullKeyCheck != null) {
                nullKeyCheck.toTop();
            }
        }

        void initFunctions(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            filterSymbolTableSource.of(masterCursor, slaveCursor);
            filter.init(filterSymbolTableSource, sqlExecutionContext);
            if (nullKeyCheck != null) {
                nullKeyCheck.init(masterCursor, sqlExecutionContext);
            }
        }

        private final class NullKeyChains implements HashOuterJoinNullKeys.SlaveChains {
            private LongChain.Cursor chain;

            @Override
            public long chainHead(MapValue value) {
                return value.getInt(0);
            }

            @Override
            public long chainSize(MapValue value) {
                return slaveChain.getCursor(value.getInt(0)).remaining();
            }

            @Override
            public boolean nextChainRow() {
                if (chain.hasNext()) {
                    slaveCursor.recordAt(slaveRecord, chain.next());
                    return true;
                }
                return false;
            }

            @Override
            public void toChain(long chainHead) {
                chain = slaveChain.getCursor((int) chainHead);
            }
        }
    }

    private class HashFullOuterJoinLightRecordCursor extends AbstractHashOuterJoinFilteredLightRecordCursor {
        private final Map matchIdsMap;
        private final Record masterNullRecord;
        private final FullOuterJoinRecord record;
        private MapRecordCursor mapCursor;
        private RecordSink masterCursorSink;
        private RecordSink slaveCursorSink;
        private boolean swapped;
        private LongChain.Cursor unmatchedChainCursor;

        public HashFullOuterJoinLightRecordCursor(
                int columnSplit,
                Record masterNullRecord,
                Record slaveNullRecord,
                Map joinKeyMap,
                Map matchIdsMap,
                LongChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            this.masterNullRecord = masterNullRecord;
            record = new FullOuterJoinRecord(columnSplit, masterNullRecord, slaveNullRecord);
            this.matchIdsMap = matchIdsMap;
            isOpen = false;
        }

        @Override
        public void close() {
            if (isOpen) {
                matchIdsMap.close();
            }
            super.close();
            mapCursor = Misc.free(mapCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                RecordCursor cursor = swapped ? slaveCursor : masterCursor;
                return cursor.getSymbolTable(columnIndex);
            } else {
                RecordCursor cursor = swapped ? masterCursor : slaveCursor;
                return cursor.getSymbolTable(columnIndex - columnSplit);
            }
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                Record keyRecord = symbolTranslatingRecord != null ? symbolTranslatingRecord : slaveCursor.getRecord();
                populateRowIDHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveCursorSink, slaveChain, keyRecord);
                isMapBuilt = true;
                nullKeys.findNullKeyChain(masterNullRecord);
                hasMaster(true);
                mapCursor = joinKeyMap.getCursor();
            }

            if (slaveChainCursor != null) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                while (slaveChainCursor.hasNext()) {
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    if (filter.getBool(record)) {
                        MapKey keys = matchIdsMap.withKey();
                        keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                        keys.createValue();
                        return true;
                    }
                }
                slaveChainCursor = null;
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                hasSlave(true);
                nullKeys.matchNullKeyRows(record, slaveRecord, matchIdsMap);
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterCursorSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    while (slaveChainCursor.hasNext()) {
                        slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                        hasSlave(true);
                        if (filter.getBool(record)) {
                            MapKey keys = matchIdsMap.withKey();
                            keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                            keys.createValue();
                            return true;
                        }
                    }
                }
                slaveChainCursor = null;
                if (!nullKeys.isMasterRowDropped(masterRecord, record)) {
                    hasSlave(false);
                    return true;
                }
            }

            hasMaster(false);
            hasSlave(true);
            while (true) {
                if (unmatchedChainCursor != null) {
                    while (unmatchedChainCursor.hasNext()) {
                        slaveCursor.recordAt(slaveRecord, unmatchedChainCursor.next());
                        MapKey keys = matchIdsMap.withKey();
                        keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                        if (keys.findValue() == null) {
                            return true;
                        }
                    }
                }
                if (!mapCursor.hasNext()) {
                    return false;
                }
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                final int chainHead = mapCursor.getRecord().getValue().getInt(0);
                unmatchedChainCursor = nullKeys.isUnmatchedChainSkipped(chainHead) ? null : slaveChain.getCursor(chainHead);
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                RecordCursor cursor = swapped ? slaveCursor : masterCursor;
                return cursor.newSymbolTable(columnIndex);
            } else {
                RecordCursor cursor = swapped ? masterCursor : slaveCursor;
                return cursor.newSymbolTable(columnIndex - columnSplit);
            }
        }

        @Override
        public void toTop() {
            super.toTop();
            if (mapCursor != null) {
                mapCursor.toTop();
            }
            unmatchedChainCursor = null;
            hasMaster(true);
            if (!isMapBuilt) {
                matchIdsMap.clear();
            }
        }

        private void hasMaster(boolean value) {
            if (swapped) {
                record.hasSlave(value);
            } else {
                record.hasMaster(value);
            }
        }

        private void hasSlave(boolean value) {
            if (swapped) {
                record.hasMaster(value);
            } else {
                record.hasSlave(value);
            }
        }

        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext, boolean swapped) throws SqlException {
            if (!isOpen) {
                matchIdsMap.setMemoryTracker(sqlExecutionContext.getMemoryTracker());
                matchIdsMap.reopen();
            }
            ofWithoutAdopt(masterCursor, slaveCursor, sqlExecutionContext);
            this.swapped = swapped;
            if (swapped) {
                record.of(slaveRecord, masterRecord);
                this.masterCursorSink = slaveKeySink;
                this.slaveCursorSink = masterKeySink;
            } else {
                record.of(masterRecord, slaveRecord);
                this.masterCursorSink = masterKeySink;
                this.slaveCursorSink = slaveKeySink;
            }
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.of(slaveCursor.getRecord());
                if (swapped) {
                    symbolTranslatingRecord.initSources(slaveCursor, masterCursor,
                            masterSymbolKeyColumnIndices, slaveSymbolKeyColumnIndices);
                } else {
                    symbolTranslatingRecord.initSources(slaveCursor, masterCursor,
                            slaveSymbolKeyColumnIndices, masterSymbolKeyColumnIndices);
                }
            }
            // The filter source mirrors this cursor's getSymbolTable(): when swapped, master/slave
            // columns resolve against the swapped-back cursors. Adopt master/slave last so an
            // initFunctions() throw leaves them unset for the getCursor() catch.
            if (swapped) {
                initFunctions(slaveCursor, masterCursor, sqlExecutionContext);
            } else {
                initFunctions(masterCursor, slaveCursor, sqlExecutionContext);
            }
            this.mapCursor = Misc.free(mapCursor);
            this.unmatchedChainCursor = null;
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            of(masterCursor, slaveCursor, sqlExecutionContext, false);
        }
    }

    private class HashLeftOuterJoinLightRecordCursor extends AbstractHashOuterJoinFilteredLightRecordCursor {
        private final OuterJoinRecord record;

        public HashLeftOuterJoinLightRecordCursor(
                int columnSplit,
                Record nullRecord,
                Map joinKeyMap,
                LongChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            record = new OuterJoinRecord(columnSplit, nullRecord);
            isOpen = false;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                Record keyRecord = symbolTranslatingRecord != null ? symbolTranslatingRecord : slaveCursor.getRecord();
                populateRowIDHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain, keyRecord);
                isMapBuilt = true;
            }

            if (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                record.hasSlave(true);
                while (slaveChainCursor.hasNext()) {
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    if (filter.getBool(record)) {
                        return true;
                    }
                }
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterKeySink);
                MapValue value = key.findValue();
                record.hasSlave(true);
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    while (slaveChainCursor.hasNext()) {
                        slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                        if (filter.getBool(record)) {
                            return true;
                        }
                    }
                }
                slaveChainCursor = null;
                if (!nullKeys.isMasterRowDropped(masterRecord, record)) {
                    record.hasSlave(false);
                    return true;
                }
            }

            return false;
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            ofWithoutAdopt(masterCursor, slaveCursor, sqlExecutionContext);
            record.of(masterRecord, slaveRecord);
            initFunctions(masterCursor, slaveCursor, sqlExecutionContext);
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.of(slaveCursor.getRecord());
                symbolTranslatingRecord.initSources(slaveCursor, masterCursor,
                        slaveSymbolKeyColumnIndices, masterSymbolKeyColumnIndices);
            }
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
        }
    }

    private class HashRightOuterJoinLightRecordCursor extends AbstractHashOuterJoinFilteredLightRecordCursor {
        private final Map matchIdsMap;
        private final Record masterNullRecord;
        private final RightOuterJoinRecord record;
        private MapRecordCursor mapCursor;

        public HashRightOuterJoinLightRecordCursor(
                int columnSplit,
                Record nullRecord,
                Map joinKeyMap,
                Map matchIdsMap,
                LongChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            masterNullRecord = nullRecord;
            record = new RightOuterJoinRecord(columnSplit, nullRecord);
            this.matchIdsMap = matchIdsMap;
            isOpen = false;
        }

        @Override
        public void close() {
            if (isOpen) {
                matchIdsMap.close();
            }
            super.close();
            mapCursor = Misc.free(mapCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                Record keyRecord = symbolTranslatingRecord != null ? symbolTranslatingRecord : slaveCursor.getRecord();
                populateRowIDHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain, keyRecord);
                isMapBuilt = true;
                nullKeys.findNullKeyChain(masterNullRecord);
                record.hasMaster(true);
                mapCursor = joinKeyMap.getCursor();
            }

            if (slaveChainCursor != null) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                while (slaveChainCursor.hasNext()) {
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    if (record.hasMaster()) {
                        if (filter.getBool(record)) {
                            MapKey keys = matchIdsMap.withKey();
                            keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                            keys.createValue();
                            return true;
                        }
                    } else {
                        MapKey keys = matchIdsMap.withKey();
                        keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                        if (keys.findValue() == null) {
                            return true;
                        }
                    }
                }
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                nullKeys.matchNullKeyRows(record, slaveRecord, matchIdsMap);
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterKeySink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    while (slaveChainCursor.hasNext()) {
                        slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                        if (filter.getBool(record)) {
                            MapKey keys = matchIdsMap.withKey();
                            keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                            keys.createValue();
                            return true;
                        }
                    }
                }
            }

            record.hasMaster(false);
            while (mapCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                MapRecord mapRecord = mapCursor.getRecord();
                final int chainHead = mapRecord.getValue().getInt(0);
                if (nullKeys.isUnmatchedChainSkipped(chainHead)) {
                    continue;
                }
                slaveChainCursor = slaveChain.getCursor(chainHead);
                while (slaveChainCursor.hasNext()) {
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    MapKey keys = matchIdsMap.withKey();
                    keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                    if (keys.findValue() == null) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public void toTop() {
            super.toTop();
            if (mapCursor != null) {
                mapCursor.toTop();
            }
            record.hasMaster(true);
            if (!isMapBuilt) {
                matchIdsMap.clear();
            }
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            if (!isOpen) {
                matchIdsMap.setMemoryTracker(sqlExecutionContext.getMemoryTracker());
                matchIdsMap.reopen();
            }
            ofWithoutAdopt(masterCursor, slaveCursor, sqlExecutionContext);
            record.of(masterRecord, slaveRecord);
            initFunctions(masterCursor, slaveCursor, sqlExecutionContext);
            mapCursor = Misc.free(mapCursor);
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.of(slaveCursor.getRecord());
                symbolTranslatingRecord.initSources(slaveCursor, masterCursor,
                        slaveSymbolKeyColumnIndices, masterSymbolKeyColumnIndices);
            }
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
        }
    }
}

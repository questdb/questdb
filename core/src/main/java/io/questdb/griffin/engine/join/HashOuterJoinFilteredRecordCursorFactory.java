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
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordIdSink;
import io.questdb.cairo.RecordSink;
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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.SymbolTranslatingRecord;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.join.HashOuterJoinFilteredLightRecordCursorFactory.outerJoinTypeToString;

/**
 * Same as HashOuterJoinRecordCursorFactory but with added filtering (for non-equality
 * or complex join conditions that use functions).
 */
public class HashOuterJoinFilteredRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final int columnSplit;
    private final JoinSymbolTableSource filterSymbolTableSource;
    private final int joinType;
    private final @Nullable NullJoinKey masterNullKey;
    private final RecordSink masterSink;
    private final RecordSink slaveKeySink;
    private final @Nullable NullJoinKey slaveNullKey;
    private AbstractHashOuterJoinRecordCursor cursor;
    private Function filter;
    private Map joinKeyMap;
    private Map matchIdsMap;
    private RecordChain slaveChain;
    private SymbolTranslatingRecord symbolTranslatingRecord;

    public HashOuterJoinFilteredRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just 3 LONGs, we store chain references in map
            RecordSink masterSink,
            RecordSink slaveKeySink,
            RecordSink slaveChainSink,
            int columnSplit,
            @NotNull Function filter,
            Plannable joinContext,
            int joinType,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
            @Nullable NullJoinKey masterNullKey,
            @Nullable NullJoinKey slaveNullKey
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        this.filter = filter;
        this.masterNullKey = masterNullKey;
        this.slaveNullKey = slaveNullKey;
        try {
            this.masterSink = masterSink;
            this.slaveKeySink = slaveKeySink;
            this.joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes, false, false);
            this.slaveChain = new RecordChain(slaveFactory.getMetadata(),
                    slaveChainSink,
                    configuration.getSqlHashJoinLightValuePageSize(),
                    configuration.getSqlHashJoinValuePageSize(),
                    configuration.getSqlHashJoinValueMaxPages());
            this.columnSplit = columnSplit;
            this.joinType = joinType;
            if (joinType != QueryModel.JOIN_LEFT_OUTER) {
                matchIdsMap = MapFactory.createUnorderedMap(configuration, RecordIdSink.RECORD_ID_COLUMN_TYPE, ArrayColumnTypes.EMPTY, false, false);
            }
            this.filterSymbolTableSource = new JoinSymbolTableSource(columnSplit);
            this.symbolTranslatingRecord = masterSymbolKeyColumnIndices != null
                    ? new SymbolTranslatingRecord(configuration, slaveFactory.getMetadata().getColumnCount(), slaveSymbolKeyColumnIndices, masterSymbolKeyColumnIndices)
                    : null;
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
                    cursor = new HashLeftOuterJoinFilteredRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                            joinKeyMap,
                            slaveChain
                    );
                    break;
                case QueryModel.JOIN_RIGHT_OUTER:
                    cursor = new HashRightOuterJoinFilteredRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(masterFactory.getMetadata()),
                            joinKeyMap,
                            matchIdsMap,
                            slaveChain
                    );
                    break;
                case QueryModel.JOIN_FULL_OUTER:
                    cursor = new HashFullOuterJoinFilteredRecordCursor(
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
            cursor.of(masterCursor, slaveCursor, executionContext);
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            // of() reopens the join map (and, for FULL/RIGHT, the match-ids map) under the bound
            // per-query tracker before it can throw; close() frees them (and the slave chain) under
            // that tracker and resets isOpen for reuse.
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
        sink.type("Hash ").val(outerJoinTypeToString(joinType)).val(" Outer Join");
        sink.optAttr("condition", joinContext);
        sink.attr("filter").val(filter);
        if (symbolTranslatingRecord != null) {
            sink.attr("symbolKeyJoin").val(true);
        }
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    @Override
    protected void _close() {
        final AbstractHashOuterJoinRecordCursor cursor = this.cursor;
        this.cursor = null;
        final Function filter = this.filter;
        this.filter = null;
        final Map joinKeyMap = this.joinKeyMap;
        this.joinKeyMap = null;
        final Map matchIdsMap = this.matchIdsMap;
        this.matchIdsMap = null;
        final RecordChain slaveChain = this.slaveChain;
        this.slaveChain = null;
        final SymbolTranslatingRecord symbolTranslatingRecord = this.symbolTranslatingRecord;
        this.symbolTranslatingRecord = null;
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeBestEffort(failure, filter);
        failure = Misc.freeBestEffort(failure, joinKeyMap);
        failure = Misc.freeBestEffort(failure, slaveChain);
        if (matchIdsMap != joinKeyMap) {
            failure = Misc.freeBestEffort(failure, matchIdsMap);
        }
        failure = Misc.freeBestEffort(failure, symbolTranslatingRecord);
        CairoException.rethrowCleanupFailure(failure);
    }

    private abstract class AbstractHashOuterJoinFilteredRecordCursor extends AbstractHashOuterJoinRecordCursor {
        private boolean hasNullKeyChain;
        private long nullKeyChainHead;
        private long nullKeyRowsToMatch;

        AbstractHashOuterJoinFilteredRecordCursor(int columnSplit, Map joinKeyMap, RecordChain slaveChain) {
            super(columnSplit, joinKeyMap, slaveChain, symbolTranslatingRecord);
        }

        @Override
        public void close() {
            super.close();
            // _close() detaches the filter before it frees this cursor
            if (filter != null) {
                filter.cursorClosed();
            }
        }

        @Override
        public void toTop() {
            super.toTop();
            filter.toTop();
        }

        /**
         * Looks up the chain of the slave rows whose keys are all NULL, the key of {@code masterNullRecord}, which a
         * master row matches by the ON residual alone, see {@link #matchNullKeyRows}.
         */
        void findNullKeyChain(Record masterNullRecord) {
            nullKeyRowsToMatch = 0;
            hasNullKeyChain = false;
            if (slaveNullKey != null) {
                final MapKey key = joinKeyMap.withKey();
                key.put(masterNullRecord, masterSink);
                final MapValue value = key.findValue();
                if (value != null) {
                    hasNullKeyChain = true;
                    nullKeyChainHead = value.getLong(0);
                    nullKeyRowsToMatch = value.getLong(2);
                }
            }
        }

        void initFilter(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            filterSymbolTableSource.of(masterCursor, slaveCursor);
            filter.init(filterSymbolTableSource, sqlExecutionContext);
        }

        /**
         * Whether the join drops the current master row, which no slave row on its key chain passes the ON residual
         * with, instead of NULL-extending it: a key of the row is not NULL, or some slave row passes the ON residual
         * with it on {@code joined}, which pairs the master row with the slave record.
         */
        boolean isMasterRowDropped(Record joined) {
            if (masterNullKey == null) {
                return false;
            }
            if (!masterNullKey.isNull(masterRecord)) {
                return true;
            }
            final MapRecordCursor keys = joinKeyMap.getCursor();
            try {
                while (keys.hasNext()) {
                    slaveChain.of(keys.getRecord().getValue().getLong(0));
                    while (slaveChain.hasNext()) {
                        if (filter.getBool(joined)) {
                            return true;
                        }
                    }
                }
                return false;
            } finally {
                keys.toTop();
            }
        }

        /**
         * Whether the join skips the chain of slave rows at {@code chainHead} when it NULL-extends the slave rows no
         * master row matched: it NULL-extends only slave rows whose keys are all NULL, see {@link #findNullKeyChain}.
         */
        boolean isUnmatchedChainSkipped(long chainHead) {
            return slaveNullKey != null && (!hasNullKeyChain || chainHead != nullKeyChainHead);
        }

        /**
         * Records as matched each slave row of the NULL key chain, see {@link #findNullKeyChain}, that passes the ON
         * residual with the current master row on {@code joined}, which pairs the master row with the slave record.
         */
        void matchNullKeyRows(Record joined, Map matchIdsMap) {
            if (nullKeyRowsToMatch > 0) {
                slaveChain.of(nullKeyChainHead);
                while (slaveChain.hasNext()) {
                    if (filter.getBool(joined)) {
                        final MapKey keys = matchIdsMap.withKey();
                        keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                        if (keys.createValue().isNew()) {
                            nullKeyRowsToMatch--;
                        }
                    }
                }
            }
        }
    }

    private class HashFullOuterJoinFilteredRecordCursor extends AbstractHashOuterJoinFilteredRecordCursor {
        private final Map matchIdsMap;
        private final Record masterNullRecord;
        private final FullOuterJoinRecord record;
        private MapRecordCursor mapCursor;

        public HashFullOuterJoinFilteredRecordCursor(
                int columnSplit,
                Record masterNullRecord,
                Record slaveNullRecord,
                Map joinKeyMap,
                Map matchIdsMap,
                RecordChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            this.masterNullRecord = masterNullRecord;
            record = new FullOuterJoinRecord(columnSplit, masterNullRecord, slaveNullRecord);
            this.matchIdsMap = matchIdsMap;
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
                final Record keyRecord = symbolTranslatingRecord != null ? symbolTranslatingRecord : slaveCursor.getRecord();
                populateRecordHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain, keyRecord);
                isMapBuilt = true;
                findNullKeyChain(masterNullRecord);
                record.hasMaster(true);
                mapCursor = joinKeyMap.getCursor();
            }

            if (useSlaveCursor && slaveChain.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                do {
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
                } while (slaveChain.hasNext());
                useSlaveCursor = false;
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                record.hasSlave(true);
                matchNullKeyRows(record, matchIdsMap);
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChain.of(value.getLong(0));
                    useSlaveCursor = true;
                    while (slaveChain.hasNext()) {
                        if (filter.getBool(record)) {
                            MapKey keys = matchIdsMap.withKey();
                            keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                            keys.createValue();
                            return true;
                        }
                    }
                }

                useSlaveCursor = false;
                if (!isMasterRowDropped(record)) {
                    record.hasSlave(false);
                    return true;
                }
            }

            record.hasMaster(false);
            record.hasSlave(true);
            while (mapCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                MapValue value = mapCursor.getRecord().getValue();
                if (isUnmatchedChainSkipped(value.getLong(0))) {
                    continue;
                }
                slaveChain.of(value.getLong(0));
                while (slaveChain.hasNext()) {
                    MapKey keys = matchIdsMap.withKey();
                    keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                    if (keys.findValue() == null) {
                        useSlaveCursor = true;
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
            // Adopt the cursors last so an initFilter() throw leaves them unset for the getCursor() catch.
            initFilter(masterCursor, slaveCursor, sqlExecutionContext);
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.of(slaveCursor.getRecord());
                symbolTranslatingRecord.initSources(slaveCursor, masterCursor);
            }
            this.mapCursor = Misc.free(mapCursor);
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
        }
    }

    private class HashLeftOuterJoinFilteredRecordCursor extends AbstractHashOuterJoinFilteredRecordCursor {
        private final OuterJoinRecord record;

        public HashLeftOuterJoinFilteredRecordCursor(
                int columnSplit,
                Record nullRecord,
                Map joinKeyMap,
                RecordChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            record = new OuterJoinRecord(columnSplit, nullRecord);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                final Record keyRecord = symbolTranslatingRecord != null ? symbolTranslatingRecord : slaveCursor.getRecord();
                populateRecordHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain, keyRecord);
                isMapBuilt = true;
            }

            if (useSlaveCursor) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                while (slaveChain.hasNext()) {
                    if (filter.getBool(record)) {
                        return true;
                    }
                }
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                record.hasSlave(true);
                if (value != null) {
                    slaveChain.of(value.getLong(0));
                    useSlaveCursor = true;
                    while (slaveChain.hasNext()) {
                        if (filter.getBool(record)) {
                            return true;
                        }
                    }
                }
                useSlaveCursor = false;
                if (!isMasterRowDropped(record)) {
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
            initFilter(masterCursor, slaveCursor, sqlExecutionContext);
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.of(slaveCursor.getRecord());
                symbolTranslatingRecord.initSources(slaveCursor, masterCursor);
            }
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
        }
    }

    private class HashRightOuterJoinFilteredRecordCursor extends AbstractHashOuterJoinFilteredRecordCursor {
        private final Map matchIdsMap;
        private final Record masterNullRecord;
        private final RightOuterJoinRecord record;
        private MapRecordCursor mapCursor;

        public HashRightOuterJoinFilteredRecordCursor(
                int columnSplit,
                Record nullRecord,
                Map joinKeyMap,
                Map matchIdsMap,
                RecordChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            masterNullRecord = nullRecord;
            record = new RightOuterJoinRecord(columnSplit, nullRecord);
            this.matchIdsMap = matchIdsMap;
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
                final Record keyRecord = symbolTranslatingRecord != null ? symbolTranslatingRecord : slaveCursor.getRecord();
                populateRecordHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain, keyRecord);
                isMapBuilt = true;
                findNullKeyChain(masterNullRecord);
                record.hasMaster(true);
                mapCursor = joinKeyMap.getCursor();
            }

            if (useSlaveCursor) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                while (slaveChain.hasNext()) {
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
                matchNullKeyRows(record, matchIdsMap);
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChain.of(value.getLong(0));
                    useSlaveCursor = true;
                    while (slaveChain.hasNext()) {
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
                MapValue value = mapCursor.getRecord().getValue();
                if (isUnmatchedChainSkipped(value.getLong(0))) {
                    continue;
                }
                slaveChain.of(value.getLong(0));
                while (slaveChain.hasNext()) {
                    MapKey keys = matchIdsMap.withKey();
                    keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                    if (keys.findValue() == null) {
                        useSlaveCursor = true;
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
            initFilter(masterCursor, slaveCursor, sqlExecutionContext);
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.of(slaveCursor.getRecord());
                symbolTranslatingRecord.initSources(slaveCursor, masterCursor);
            }
            this.mapCursor = Misc.free(mapCursor);
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
        }
    }
}

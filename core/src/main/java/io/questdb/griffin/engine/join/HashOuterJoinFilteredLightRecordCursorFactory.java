/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

/**
 * Same as HashOuterJoinLightRecordCursorFactory but with added filtering (for non-equality
 * or complex join conditions that use functions).
 */
public class HashOuterJoinFilteredLightRecordCursorFactory extends AbstractJoinRecordCursorFactory {

    private final int columnSplit;
    private final Function filter;
    private final int joinType;
    private final RecordSink masterKeySink;
    private final RecordSink slaveKeySink;
    private AbstractHashOuterJoinLightRecordCursor cursor;
    private Map joinKeyMap;
    private LongChain slaveChain;

    public HashOuterJoinFilteredLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just 2 INTs, we store chain references in map
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            int columnSplit,
            @NotNull Function filter,
            JoinContext joinContext,
            int joinType
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        try {
            this.masterKeySink = masterKeySink;
            this.slaveKeySink = slaveKeySink;
            this.joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes);
            this.slaveChain = new LongChain(configuration.getSqlHashJoinLightValuePageSize(), configuration.getSqlHashJoinLightValueMaxPages());
            this.columnSplit = columnSplit;
            this.joinType = joinType;
            this.filter = filter;
        } catch (Throwable th) {
            close();
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
                    cursor = new HashLeftOuterJoinLightLightRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                            joinKeyMap,
                            slaveChain

                    );
                    break;
                case QueryModel.JOIN_RIGHT_OUTER:
                    cursor = new HashRightOuterJoinLightLightRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(masterFactory.getMetadata()),
                            joinKeyMap,
                            slaveChain
                    );
                    break;
                case QueryModel.JOIN_FULL_OUTER:
                    cursor = new HashFullOuterJoinLightLightRecordCursor(
                            columnSplit,
                            NullRecordFactory.getInstance(masterFactory.getMetadata()),
                            NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                            joinKeyMap,
                            slaveChain
                    );
                    break;

            }
            this.joinKeyMap = null;
            this.slaveChain = null;
        }

        // Forcefully disable column pre-touch for nested filter queries.
        executionContext.setColumnPreTouchEnabled(false);
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        RecordCursor masterCursor = null;
        try {
            masterCursor = masterFactory.getCursor(executionContext);
            if (joinType == QueryModel.JOIN_FULL_OUTER) {
                boolean swapped = false;
                if (masterFactory.recordCursorSupportsRandomAccess()) {
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
                    ((HashFullOuterJoinLightLightRecordCursor) cursor).of(masterCursor, slaveCursor, executionContext, true);
                    return cursor;
                }
            }
            cursor.of(masterCursor, slaveCursor, executionContext);
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
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
        sink.attr("filter").val(filter);
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    protected static CharSequence outerJoinTypeToString(int joinType) {
        switch (joinType) {
            case QueryModel.JOIN_LEFT_OUTER:
                return "Left";
            case QueryModel.JOIN_RIGHT_OUTER:
                return "Right";
            case QueryModel.JOIN_FULL_OUTER:
                return "Full";
            default:
                return "Unknown";
        }
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(cursor);
        Misc.free(filter);
        Misc.free(joinKeyMap);
        Misc.free(slaveChain);
    }

    private class HashFullOuterJoinLightLightRecordCursor extends AbstractHashOuterJoinLightRecordCursor {
        private final FullOuterJoinRecord record;
        private MapRecordCursor mapCursor;
        private RecordSink masterCursorSink;
        private RecordSink slaveCursorSink;
        private boolean swapped;

        public HashFullOuterJoinLightLightRecordCursor(
                int columnSplit,
                Record masterNullRecord,
                Record slaveNullRecord,
                Map joinKeyMap,
                LongChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            record = new FullOuterJoinRecord(columnSplit, masterNullRecord, slaveNullRecord);
            isOpen = true;
        }

        @Override
        public void close() {
            super.close();
            mapCursor = Misc.free(mapCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            RecordCursor cursor = swapped ? masterCursor : slaveCursor;
            if (columnIndex < columnSplit) {
                return cursor.getSymbolTable(columnIndex);
            } else {
                return cursor.getSymbolTable(columnIndex - columnSplit);
            }
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                populateRowIDHashMapWithMatchedFlag(circuitBreaker, slaveCursor, joinKeyMap, slaveCursorSink, slaveChain);
                isMapBuilt = true;
                hasMaster(true);
                mapCursor = joinKeyMap.getCursor();
            }

            if (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                while (slaveChainCursor.hasNext()) {
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    if (filter.getBool(record)) {
                        return true;
                    }
                }
                slaveChainCursor = null;
            }

            if (masterCursor.hasNext()) {
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterCursorSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    value.putBool(1, true); // mark as matched
                    while (slaveChainCursor.hasNext()) {
                        slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                        if (filter.getBool(record)) {
                            hasSlave(true);
                            return true;
                        }
                    }
                }
                slaveChainCursor = null;
                hasSlave(false);
                return true;
            }

            hasMaster(false);
            hasSlave(true);
            while (mapCursor.hasNext()) {
                MapValue value = mapCursor.getRecord().getValue();
                if (!value.getBool(1)) { // if not matched
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    return true;
                }
            }

            return false;
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
            filter.toTop();
            if (mapCursor != null) {
                mapCursor.toTop();
            }
            hasMaster(true);
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
            super.of(masterCursor, slaveCursor, sqlExecutionContext);
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
            filter.init(this, sqlExecutionContext);
            this.mapCursor = Misc.free(mapCursor);
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            of(masterCursor, slaveCursor, sqlExecutionContext, false);
        }
    }

    private class HashLeftOuterJoinLightLightRecordCursor extends AbstractHashOuterJoinLightRecordCursor {
        private final OuterJoinRecord record;

        public HashLeftOuterJoinLightLightRecordCursor(
                int columnSplit,
                Record nullRecord,
                Map joinKeyMap,
                LongChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            record = new OuterJoinRecord(columnSplit, nullRecord);
            isOpen = true;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                populateRowIDHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain);
                isMapBuilt = true;
            }

            if (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                record.hasSlave(true);
                while (slaveChainCursor.hasNext()) {
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    if (filter.getBool(record)) {
                        return true;
                    }
                }
            }

            if (masterCursor.hasNext()) {
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterKeySink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    record.hasSlave(true);
                    while (slaveChainCursor.hasNext()) {
                        slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                        if (filter.getBool(record)) {
                            return true;
                        }
                    }
                }
                slaveChainCursor = null;
                record.hasSlave(false);
                return true;
            }

            return false;
        }

        @Override
        public void toTop() {
            super.toTop();
            filter.toTop();
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            super.of(masterCursor, slaveCursor, sqlExecutionContext);
            record.of(masterRecord, slaveRecord);
            filter.init(this, sqlExecutionContext);
        }
    }

    private class HashRightOuterJoinLightLightRecordCursor extends AbstractHashOuterJoinLightRecordCursor {
        private final RightOuterJoinRecord record;
        private MapRecordCursor mapCursor;

        public HashRightOuterJoinLightLightRecordCursor(
                int columnSplit,
                Record nullRecord,
                Map joinKeyMap,
                LongChain slaveChain
        ) {
            super(columnSplit, joinKeyMap, slaveChain);
            record = new RightOuterJoinRecord(columnSplit, nullRecord);
            isOpen = true;
        }

        @Override
        public void close() {
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
                populateRowIDHashMapWithMatchedFlag(circuitBreaker, slaveCursor, joinKeyMap, slaveKeySink, slaveChain);
                isMapBuilt = true;
                record.hasMaster(true);
                mapCursor = joinKeyMap.getCursor();
            }

            if (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                while (slaveChainCursor.hasNext()) {
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    if (filter.getBool(record)) {
                        return true;
                    }
                }
            }

            while (masterCursor.hasNext()) {
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterKeySink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    value.putBool(1, true); // mark as matched
                    while (slaveChainCursor.hasNext()) {
                        slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                        if (filter.getBool(record)) {
                            return true;
                        }
                    }
                }
            }

            record.hasMaster(false);
            while (mapCursor.hasNext()) {
                MapValue value = mapCursor.getRecord().getValue();
                if (!value.getBool(1)) { // if not matched
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    return true;
                }
            }
            return false;
        }

        @Override
        public void toTop() {
            super.toTop();
            filter.toTop();
            if (mapCursor != null) {
                mapCursor.toTop();
            }
            record.hasMaster(true);
        }

        @Override
        protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            super.of(masterCursor, slaveCursor, sqlExecutionContext);
            record.of(masterRecord, slaveRecord);
            filter.init(this, sqlExecutionContext);
            this.mapCursor = Misc.free(mapCursor);
        }
    }
}

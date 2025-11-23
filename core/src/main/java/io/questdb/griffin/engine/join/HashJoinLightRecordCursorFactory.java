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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

public class HashJoinLightRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final HashJoinRecordCursor cursor;
    private final RecordSink masterSink;
    private final RecordSink slaveKeySink;
    private boolean masterDetermined = false;

    public HashJoinLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just 2 INTs, we store chain references in map
            RecordSink masterSink,
            RecordSink slaveKeySink,
            int columnSplit,
            JoinContext joinContext
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        try {
            this.masterSink = masterSink;
            this.slaveKeySink = slaveKeySink;
            this.cursor = new HashJoinRecordCursor(columnSplit, configuration, joinColumnTypes, valueTypes);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        boolean followOrderBy = masterFactory.followedOrderByAdvice();
        masterDetermined |= followOrderBy;
        return followOrderBy;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        RecordCursor masterCursor = null;
        try {
            masterCursor = masterFactory.getCursor(executionContext);
            boolean swapped = false;
            if (masterFactory.recordCursorSupportsRandomAccess() && !masterDetermined) {
                long masterSize = masterCursor.size();
                long slaveSize = slaveCursor.size();

                if (masterSize > 0 && slaveSize > 0 && masterSize < slaveSize) {
                    RecordCursor temp = masterCursor;
                    masterCursor = slaveCursor;
                    slaveCursor = temp;
                    swapped = true;
                }
            }

            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker(), swapped);
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        int scanDirection = masterFactory.getScanDirection();
        masterDetermined |= scanDirection != RecordCursorFactory.SCAN_DIRECTION_OTHER;
        return scanDirection;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return masterFactory.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Hash Join Light");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    private static void populateRowIDHashMap(
            SqlExecutionCircuitBreaker circuitBreaker,
            RecordCursor cursor,
            Map keyMap,
            RecordSink recordSink,
            LongChain rowIDChain
    ) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();

            MapKey key = keyMap.withKey();
            key.put(record, recordSink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                value.putInt(0, rowIDChain.put(record.getRowId(), -1));
                value.putInt(1, 1);
            } else {
                value.putInt(0, rowIDChain.put(record.getRowId(), value.getInt(0)));
                value.addInt(1, 1);
            }
        }
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(cursor);
    }

    private class HashJoinRecordCursor extends AbstractJoinCursor {
        private final Map joinKeyMap;
        private final JoinRecord record;
        private final LongChain slaveChain;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMapBuilt;
        private boolean isOpen;
        private RecordSink masterCursorSink;
        private Record masterRecord;
        private LongChain.Cursor slaveChainCursor;
        private RecordSink slaveCursorSink;
        private Record slaveRecord;
        private boolean swapped;

        public HashJoinRecordCursor(int columnSplit, CairoConfiguration configuration, ColumnTypes joinColumnTypes, ColumnTypes valueTypes) {
            super(columnSplit);
            try {
                isOpen = true;
                record = new JoinRecord(columnSplit);
                joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes);
                slaveChain = new LongChain(configuration.getSqlHashJoinLightValuePageSize(), configuration.getSqlHashJoinLightValueMaxPages());
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            buildMapOfSlaveRecords();
            while (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                slaveChainCursor.next();
                counter.inc();
            }

            final Record masterRecord = masterCursor.getRecord();
            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterCursorSink);
                MapValue value = key.findValue();
                if (value != null) {
                    counter.add(value.getInt(1));
                }
            }
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                Misc.free(joinKeyMap);
                Misc.free(slaveChain);
                super.close();
            }
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
            buildMapOfSlaveRecords();
            if (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                return true;
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterCursorSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    // we know cursor has values
                    // advance to get the first value
                    slaveChainCursor.hasNext();
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
        public long preComputedStateSize() {
            return isMapBuilt ? 1 : 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveChainCursor = null;
            if (!isMapBuilt) {
                slaveCursor.toTop();
                joinKeyMap.clear();
                slaveChain.clear();
            }
        }

        private void buildMapOfSlaveRecords() {
            if (!isMapBuilt) {
                populateRowIDHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveCursorSink, slaveChain);
                isMapBuilt = true;
            }
        }

        private void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker, boolean swapped) {
            if (!isOpen) {
                isOpen = true;
                joinKeyMap.reopen();
                slaveChain.reopen();
            }
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.circuitBreaker = circuitBreaker;
            masterRecord = masterCursor.getRecord();
            slaveRecord = slaveCursor.getRecordB();
            this.swapped = swapped;
            if (swapped) {
                record.of(slaveRecord, masterRecord);
                this.masterCursorSink = slaveKeySink;
                this.slaveCursorSink = masterSink;
            } else {
                record.of(masterRecord, slaveRecord);
                this.masterCursorSink = masterSink;
                this.slaveCursorSink = slaveKeySink;
            }
            slaveChainCursor = null;
            isMapBuilt = false;
        }
    }
}

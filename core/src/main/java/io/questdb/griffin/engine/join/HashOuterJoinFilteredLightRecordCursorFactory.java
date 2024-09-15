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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

/**
 * Same as HashOuterJoinLightRecordCursorFactory but with added filtering (for non-equality
 * or complex join conditions that use functions).
 */
public class HashOuterJoinFilteredLightRecordCursorFactory extends AbstractJoinRecordCursorFactory {

    private final HashOuterJoinLightRecordCursor cursor;
    private final Function filter;
    private final RecordSink masterKeySink;
    private final RecordSink slaveKeySink;

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
            JoinContext joinContext
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        try {
            this.masterKeySink = masterKeySink;
            this.slaveKeySink = slaveKeySink;
            cursor = new HashOuterJoinLightRecordCursor(
                    columnSplit,
                    NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                    joinColumnTypes,
                    valueTypes,
                    configuration
            );
            this.filter = filter;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        RecordCursor masterCursor = null;
        try {
            masterCursor = masterFactory.getCursor(executionContext);
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
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Hash Outer Join Light");
        sink.optAttr("condition", joinContext);
        sink.attr("filter").val(filter);
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(cursor);
        Misc.free(filter);
    }

    private class HashOuterJoinLightRecordCursor extends AbstractJoinCursor {
        private final Map joinKeyMap;
        private final OuterJoinRecord record;
        private final LongChain slaveChain;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMapBuilt;
        private boolean isOpen;
        private Record masterRecord;
        private LongChain.Cursor slaveChainCursor;
        private Record slaveRecord;

        public HashOuterJoinLightRecordCursor(
                int columnSplit,
                Record nullRecord,
                @Transient ColumnTypes joinColumnTypes,
                @Transient ColumnTypes valueTypes,
                CairoConfiguration configuration
        ) {
            super(columnSplit);
            joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes);
            slaveChain = new LongChain(configuration.getSqlHashJoinLightValuePageSize(), configuration.getSqlHashJoinLightValueMaxPages());
            record = new OuterJoinRecord(columnSplit, nullRecord);
            isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                joinKeyMap.close();
                slaveChain.close();
                super.close();
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!isMapBuilt) {
                buildMapOfSlaveRecords();
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
        public long size() {
            return -1L;
        }

        @Override
        public void toTop() {
            filter.toTop();
            masterCursor.toTop();
            slaveChainCursor = null;
            if (!isMapBuilt) {
                slaveCursor.toTop();
                joinKeyMap.clear();
                slaveChain.clear();
            }
        }

        private void buildMapOfSlaveRecords() {
            final Record record = slaveCursor.getRecord();
            while (slaveCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                MapKey key = joinKeyMap.withKey();
                key.put(record, slaveKeySink);
                MapValue value = key.createValue();
                if (value.isNew()) {
                    final int offset = slaveChain.put(record.getRowId(), -1);
                    value.putInt(0, offset);
                } else {
                    value.putInt(0, slaveChain.put(record.getRowId(), value.getInt(0)));
                }
            }
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                slaveChain.reopen();
                joinKeyMap.reopen();
            }
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.circuitBreaker = sqlExecutionContext.getCircuitBreaker();
            masterRecord = masterCursor.getRecord();
            slaveRecord = slaveCursor.getRecordB();
            record.of(masterRecord, slaveRecord);
            slaveChainCursor = null;
            isMapBuilt = false;
            filter.init(this, sqlExecutionContext);
        }
    }
}

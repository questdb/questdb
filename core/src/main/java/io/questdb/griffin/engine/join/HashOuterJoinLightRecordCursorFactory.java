/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

public class HashOuterJoinLightRecordCursorFactory extends AbstractRecordCursorFactory {
    private final Map joinKeyMap;
    private final LongChain slaveChain;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final RecordSink masterKeySink;
    private final RecordSink slaveKeySink;
    private final HashOuterJoinLightRecordCursor cursor;

    public HashOuterJoinLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just LONG, we store chain references in map
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            int columnSplit

    ) {
        super(metadata);
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        joinKeyMap = MapFactory.createMap(configuration, joinColumnTypes, valueTypes);
        slaveChain = new LongChain(configuration.getSqlHashJoinLightValuePageSize(), configuration.getSqlHashJoinLightValueMaxPages());
        this.masterKeySink = masterKeySink;
        this.slaveKeySink = slaveKeySink;
        this.cursor = new HashOuterJoinLightRecordCursor(
                columnSplit,
                joinKeyMap,
                slaveChain,
                NullRecordFactory.getInstance(slaveFactory.getMetadata())
        );
    }

    @Override
    public void close() {
        joinKeyMap.close();
        slaveChain.close();
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        try {
            buildMapOfSlaveRecords(slaveCursor, executionContext.getSqlExecutionInterruptor());
        } catch (Throwable e) {
            slaveCursor.close();
            throw e;
        }
        cursor.of(masterFactory.getCursor(executionContext), slaveCursor);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private void buildMapOfSlaveRecords(RecordCursor slaveCursor, SqlExecutionInterruptor interruptor) {
        slaveChain.clear();
        joinKeyMap.clear();
        final Record record = slaveCursor.getRecord();
        while (slaveCursor.hasNext()) {
            interruptor.checkInterrupted();
            MapKey key = joinKeyMap.withKey();
            key.put(record, slaveKeySink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                final long offset = slaveChain.put(record.getRowId(), -1);
                value.putLong(0, offset);
                value.putLong(1, offset);
            } else {
                value.putLong(1, slaveChain.put(record.getRowId(), value.getLong(1)));
            }
        }
    }

    private class HashOuterJoinLightRecordCursor implements NoRandomAccessRecordCursor {
        private final OuterJoinRecord record;
        private final LongChain slaveChain;
        private final Map joinKeyMap;
        private final int columnSplit;
        private RecordCursor masterCursor;
        private RecordCursor slaveCursor;
        private Record masterRecord;
        private LongChain.TreeCursor slaveChainCursor;
        private Record slaveRecord;

        public HashOuterJoinLightRecordCursor(
                int columnSplit,
                Map joinKeyMap,
                LongChain slaveChain,
                Record nullRecord
        ) {
            this.record = new OuterJoinRecord(columnSplit, nullRecord);
            this.joinKeyMap = joinKeyMap;
            this.slaveChain = slaveChain;
            this.columnSplit = columnSplit;
        }

        @Override
        public void close() {
            masterCursor = Misc.free(masterCursor);
            slaveCursor = Misc.free(slaveCursor);
        }

        @Override
        public long size() {
            return -1L;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterCursor.getSymbolTable(columnIndex);
            }
            return slaveCursor.getSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public boolean hasNext() {
            if (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                return true;
            }

            if (masterCursor.hasNext()) {
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterKeySink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getLong(0));
                    // we know cursor has values
                    // advance to get first value
                    slaveChainCursor.hasNext();
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    record.hasSlave(true);
                } else {
                    slaveChainCursor = null;
                    record.hasSlave(false);
                }
                return true;
            }

            return false;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveChainCursor = null;
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.masterRecord = masterCursor.getRecord();
            this.slaveRecord = slaveCursor.getRecordB();
            record.of(masterRecord, slaveRecord);
            slaveChainCursor = null;
        }
    }
}

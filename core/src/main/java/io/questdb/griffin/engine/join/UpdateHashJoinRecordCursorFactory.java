/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.griffin.update.UpdateStatementMasterCursor;
import io.questdb.griffin.update.UpdateStatementMasterCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

/***
 * Very similar to HashJoinLightRecordCursorFactory except used for UPDATE statements
 */
public class UpdateHashJoinRecordCursorFactory implements UpdateStatementMasterCursorFactory {
    private final Map joinKeyMap;
    private final RecordChain slaveChain;
    private final RecordMetadata metadata;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final RecordSink masterSink;
    private final RecordSink slaveKeySink;
    private final UpdateHashJoinRecordCursor cursor;

    public UpdateHashJoinRecordCursorFactory(CairoConfiguration configuration,
                                             RecordMetadata metadata,
                                             RecordCursorFactory masterFactory,
                                             RecordCursorFactory slaveFactory,
                                             @Transient ColumnTypes joinColumnTypes,
                                             @Transient ColumnTypes valueTypes,
                                             RecordSink masterSink,
                                             RecordSink slaveKeySink,
                                             RecordSink slaveChainSink,
                                             int columnSplit

    ) {
        this.metadata = metadata;
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        joinKeyMap = MapFactory.createMap(configuration, joinColumnTypes, valueTypes);
        slaveChain = new RecordChain(slaveFactory.getMetadata(), slaveChainSink, configuration.getSqlHashJoinValuePageSize(), configuration.getSqlHashJoinValueMaxPages());
        this.masterSink = masterSink;
        this.slaveKeySink = slaveKeySink;
        this.cursor = new UpdateHashJoinRecordCursor(columnSplit, joinKeyMap, slaveChain);
    }

    @Override
    public void close() {
        joinKeyMap.close();
        slaveChain.close();
        ((JoinRecordMetadata) metadata).close();
        masterFactory.close();
        slaveFactory.close();
    }

    @Override
    public UpdateStatementMasterCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        try {
            buildMapOfSlaveRecords(slaveCursor, executionContext.getSqlExecutionInterruptor());
        } catch (Throwable e) {
            slaveCursor.close();
            throw e;
        }
        cursor.of(slaveCursor);
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    private void buildMapOfSlaveRecords(RecordCursor slaveCursor, SqlExecutionInterruptor interrupter) {
        HashOuterJoinRecordCursorFactory.buildMap(slaveCursor, slaveCursor.getRecord(), joinKeyMap, slaveKeySink, slaveChain, interrupter);
    }

    private class UpdateHashJoinRecordCursor implements UpdateStatementMasterCursor {
        private final JoinRecord recordA;
        private final RecordChain slaveChain;
        private final Map joinKeyMap;
        private RecordCursor masterCursor;
        private RecordCursor slaveCursor;
        private boolean useSlaveCursor;

        public UpdateHashJoinRecordCursor(int columnSplit, Map joinKeyMap, RecordChain slaveChain) {
            this.recordA = new JoinRecord(columnSplit);
            this.joinKeyMap = joinKeyMap;
            this.slaveChain = slaveChain;
        }

        @Override
        public void setMaster(Record master) {
            MapKey key = joinKeyMap.withKey();
            key.put(master, masterSink);
            MapValue value = key.findValue();
            if (value != null) {
                slaveChain.of(value.getLong(0));
                useSlaveCursor = true;
                recordA.of(master, slaveChain.getRecord());
            } else {
                useSlaveCursor = false;
            }
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public boolean hasNext() {
            return useSlaveCursor && slaveChain.hasNext();
        }

        @Override
        public void close() {
            masterCursor = Misc.free(masterCursor);
            slaveCursor = Misc.free(slaveCursor);
        }

        public void of(RecordCursor slaveCursor) {
            this.slaveCursor = slaveCursor;
        }
    }
}

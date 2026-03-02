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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public abstract class AbstractHashOuterJoinLightRecordCursor extends AbstractJoinCursor {
    protected final Map joinKeyMap;
    protected final LongChain slaveChain;
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected boolean isMapBuilt;
    protected boolean isOpen;
    protected Record masterRecord;
    protected LongChain.Cursor slaveChainCursor;
    protected Record slaveRecord;

    public AbstractHashOuterJoinLightRecordCursor(
            int columnSplit,
            Map joinKeyMap,
            LongChain slaveChain
    ) {
        super(columnSplit);
        isOpen = true;
        this.joinKeyMap = joinKeyMap;
        this.slaveChain = slaveChain;
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
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isMapBuilt);
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

    protected static void populateRowIDHashMap(
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
            } else {
                value.putInt(0, rowIDChain.put(record.getRowId(), value.getInt(0)));
            }
        }
    }

    protected static void populateRowIDHashMapWithMatchedFlag(
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
                value.putBool(1, false);
            } else {
                value.putInt(0, rowIDChain.put(record.getRowId(), value.getInt(0)));
            }
        }
    }

    protected void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
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
        slaveChainCursor = null;
        isMapBuilt = false;
    }
}

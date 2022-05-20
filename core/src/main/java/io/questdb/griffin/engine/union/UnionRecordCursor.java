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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

class UnionRecordCursor implements NoRandomAccessRecordCursor {
    private final UnionDelegatingRecordImpl record = new UnionDelegatingRecordImpl();
    private final Map map;
    private final RecordSink recordSink;
    private RecordCursor masterCursor;
    private RecordMetadata masterMetadata;
    private RecordCursor slaveCursor;
    private final NextMethod nextSlave = this::nextSlave;
    private Record masterRecord;
    private Record slaveRecord;
    private NextMethod nextMethod;
    private RecordMetadata slaveMetadata;
    private final NextMethod nextMaster = this::nextMaster;
    private SqlExecutionCircuitBreaker circuitBreaker;

    public UnionRecordCursor(Map map, RecordSink recordSink) {
        this.map = map;
        this.recordSink = recordSink;
    }

    @Override
    public void close() {
        Misc.free(this.masterCursor);
        Misc.free(this.slaveCursor);
        circuitBreaker = null;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return masterCursor.newSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        while (true) {
            boolean next = nextMethod.next();
            if (next) {
                MapKey key = map.withKey();
                key.put(record, recordSink);
                if (key.create()) {
                    return true;
                }
                circuitBreaker.statefulThrowExceptionIfTripped();
            } else {
                return false;
            }
        }
    }

    @Override
    public void toTop() {
        map.clear();
        record.of(masterRecord, masterMetadata);
        nextMethod = nextMaster;
        masterCursor.toTop();
        slaveCursor.toTop();
    }

    @Override
    public long size() {
        return -1;
    }

    private boolean nextMaster() {
        if (masterCursor.hasNext()) {
            return true;
        }
        return switchToSlaveCursor();
    }

    private boolean nextSlave() {
        return slaveCursor.hasNext();
    }

    void of(RecordCursor masterCursor, RecordMetadata masterMetadata, RecordCursor slaveCursor, RecordMetadata slaveMetadata, SqlExecutionContext executionContext) {
        this.masterCursor = masterCursor;
        this.masterMetadata = masterMetadata;
        this.slaveCursor = slaveCursor;
        this.masterRecord = masterCursor.getRecord();
        this.slaveRecord = slaveCursor.getRecord();
        this.slaveMetadata = slaveMetadata;
        circuitBreaker = executionContext.getCircuitBreaker();
        toTop();
    }

    private boolean switchToSlaveCursor() {
        record.of(slaveRecord, slaveMetadata);
        nextMethod = nextSlave;
        return nextMethod.next();
    }

    interface NextMethod {
        boolean next();
    }
}

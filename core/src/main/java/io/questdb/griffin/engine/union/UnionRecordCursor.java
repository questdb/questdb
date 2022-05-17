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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Misc;

class UnionRecordCursor implements NoRandomAccessRecordCursor {
    private final DelegatingRecordImpl record;
    private final Map map;
    private final RecordSink recordSink;
    private RecordCursor masterCursor;
    private RecordCursor slaveCursor;
    private final NextMethod nextSlave = this::nextSlave;
    private Record masterRecord;
    private Record slaveRecord;
    private NextMethod nextMethod;
    private final NextMethod nextMaster = this::nextMaster;
    private SqlExecutionCircuitBreaker circuitBreaker;

    public UnionRecordCursor(RecordMetadata metadata, Map map, RecordSink recordSink) {
        this.map = map;
        this.recordSink = recordSink;
        if (metadata.hasType(ColumnType.SYMBOL)) {
            this.record = new DelegatingSymbolRecordImpl(metadata);
        } else {
            this.record = new DelegatingRecordImpl();
        }
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
        return record.getSymbolTable(columnIndex);
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
        toTop(false);
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return record.newSymbolTable(columnIndex);
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

    void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext executionContext) {
        this.masterCursor = masterCursor;
        this.slaveCursor = slaveCursor;
        this.masterRecord = masterCursor.getRecord();
        this.slaveRecord = slaveCursor.getRecord();
        this.record.of(masterCursor, slaveCursor);
        this.circuitBreaker = executionContext.getCircuitBreaker();
        toTop(true);
    }

    private boolean switchToSlaveCursor() {
        record.ofSlave(slaveRecord);
        nextMethod = nextSlave;
        return slaveCursor.hasNext();
    }

    private void toTop(boolean initial) {
        map.clear();
        if (!initial) {
            record.toTop();
        }
        record.ofMaster(masterRecord);
        nextMethod = nextMaster;
        masterCursor.toTop();
        slaveCursor.toTop();
    }

    interface NextMethod {
        boolean next();
    }
}

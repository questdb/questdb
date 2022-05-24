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
import io.questdb.cairo.sql.*;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

class UnionRecordCursor implements NoRandomAccessRecordCursor {
    private final UnionRecord record;
    private final Map map;
    private final RecordSink recordSink;
    private RecordCursor cursorA;
    private RecordCursor cursorB;
    private final NextMethod nextSlave = this::nextSlave;
    private NextMethod nextMethod;
    private final NextMethod nextMaster = this::nextMaster;
    private SqlExecutionCircuitBreaker circuitBreaker;

    public UnionRecordCursor(Map map, RecordSink recordSink, ObjList<Function> castFunctionsA, ObjList<Function> castFunctionsB) {
        this.record = new UnionRecord(castFunctionsA, castFunctionsB);
        this.map = map;
        this.recordSink = recordSink;
    }

    @Override
    public void close() {
        Misc.free(this.cursorA);
        Misc.free(this.cursorB);
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
        return cursorA.newSymbolTable(columnIndex);
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
        record.setAb(true);
        nextMethod = nextMaster;
        cursorA.toTop();
        cursorB.toTop();
    }

    @Override
    public long size() {
        return -1;
    }

    private boolean nextMaster() {
        if (cursorA.hasNext()) {
            return true;
        }
        return switchToSlaveCursor();
    }

    private boolean nextSlave() {
        return cursorB.hasNext();
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionCircuitBreaker circuitBreaker) {
        this.cursorA = cursorA;
        this.cursorB = cursorB;
        this.circuitBreaker = circuitBreaker;
        this.record.of(cursorA.getRecord(), cursorB.getRecord());
        toTop();
    }

    private boolean switchToSlaveCursor() {
        record.setAb(false);
        nextMethod = nextSlave;
        return nextMethod.next();
    }

    interface NextMethod {
        boolean next();
    }
}

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
import io.questdb.cairo.sql.Record;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

class IntersectRecordCursor implements RecordCursor {
    private final Map map;
    private final RecordSink recordSink;
    private final VirtualRecord virtualRecord;
    private RecordCursor cursorA;
    private RecordCursor cursorB;
    private Record recordA;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private Record activeBRecord;

    public IntersectRecordCursor(
            Map map,
            RecordSink recordSink,
            @Nullable ObjList<Function> castFunctionB
    ) {
        this.map = map;
        this.recordSink = recordSink;
        if (castFunctionB != null) {
            this.virtualRecord = new VirtualRecord(castFunctionB);
        } else {
            this.virtualRecord = null;
        }
    }

    @Override
    public void close() {
        this.cursorA = Misc.free(this.cursorA);
        this.cursorB = Misc.free(this.cursorB);
        this.map.clear();
        circuitBreaker = null;
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return cursorA.getSymbolTable(columnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return cursorA.newSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        while (cursorA.hasNext()) {
            MapKey key = map.withKey();
            key.put(recordA, recordSink);
            if (key.findValue() != null) {
                return true;
            }
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
        return false;
    }

    @Override
    public Record getRecordB() {
        return cursorA.getRecordB();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        cursorA.recordAt(record, atRowId);
    }

    @Override
    public void toTop() {
        cursorA.toTop();
    }

    @Override
    public long size() {
        return -1;
    }

    private void hashCursorB() {
        while (cursorB.hasNext()) {
            MapKey key = map.withKey();
            key.put(activeBRecord, recordSink);
            key.createValue();
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
        // this is an optimisation to release TableReader in case "this"
        // cursor lingers around. If there is exception or circuit breaker fault
        // we will rely on close() method to release reader.
        this.cursorB = Misc.free(this.cursorB);
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionCircuitBreaker circuitBreaker) {
        this.cursorA = cursorA;
        this.cursorB = cursorB;
        this.circuitBreaker = circuitBreaker;

        map.clear();
        if (virtualRecord != null) {
            virtualRecord.of(cursorB.getRecord());
            this.activeBRecord = this.virtualRecord;
        } else {
            this.activeBRecord = cursorB.getRecord();
        }
        hashCursorB();
        recordA = cursorA.getRecord();
        toTop();
    }
}

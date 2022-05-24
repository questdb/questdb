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

class IntersectRecordCursor implements RecordCursor {
    private final Map map;
    private final RecordSink recordSink;
    private final boolean convertSymbolsAsStrings;
    private RecordCursor cursorA;
    private RecordCursor cursorB;
    private Record record;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private UnionRecord unionRecord;

    public IntersectRecordCursor(
            Map map,
            RecordSink recordSink,
            boolean convertSymbolsAsStrings,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionB
    ) {
        this.map = map;
        this.recordSink = recordSink;
        this.convertSymbolsAsStrings = convertSymbolsAsStrings;
        if (convertSymbolsAsStrings) {
            this.unionRecord = new UnionRecord(castFunctionsA, castFunctionB);
            record = unionRecord;
        }
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
            key.put(record, recordSink);
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

    private void hashCursorB(RecordCursor cursorB) {
        Record recordB = cursorB.getRecord();
        if (convertSymbolsAsStrings) {
            unionRecord.of(null, recordB);
            unionRecord.setAb(false);
            recordB = unionRecord;
        }

        while (cursorB.hasNext()) {
            MapKey key = map.withKey();
            key.put(recordB, recordSink);
            key.createValue();
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionCircuitBreaker circuitBreaker) {
        this.cursorA = cursorA;
        this.cursorB = cursorB;
        this.circuitBreaker = circuitBreaker;

        map.clear();
        hashCursorB(cursorB);

        if (convertSymbolsAsStrings) {
            unionRecord.of(cursorA.getRecord(), null);
            unionRecord.setAb(true);
        } else {
            record = cursorA.getRecord();
        }

        toTop();
    }
}

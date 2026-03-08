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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;

class ExceptRecordCursor extends AbstractSetRecordCursor {
    private final Map mapA;
    private final Map mapB;
    private final RecordSink recordSink;
    private boolean isCursorBHashed;
    private boolean isOpen;
    private Record recordA;
    private Record recordB;

    public ExceptRecordCursor(Map mapA, Map mapB, RecordSink recordSink) {
        this.mapA = mapA;
        this.mapB = mapB;
        this.recordSink = recordSink;
        isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            mapA.close();
            mapB.close();
            super.close();
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return cursorA.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return cursorA.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isCursorBHashed) {
            hashCursorB();
            toTop();
            isCursorBHashed = true;
        }
        while (cursorA.hasNext()) {
            MapKey keyB = mapB.withKey();
            keyB.put(recordA, recordSink);
            if (keyB.notFound()) {
                MapKey keyA = mapA.withKey();
                keyA.put(recordA, recordSink);
                if (keyA.create()) {
                    return true;
                }
            }
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return cursorA.newSymbolTable(columnIndex);
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isCursorBHashed);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        cursorA.recordAt(record, atRowId);
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        cursorA.toTop();
        mapA.clear();
    }

    private void hashCursorB() {
        while (cursorB.hasNext()) {
            MapKey keyB = mapB.withKey();
            keyB.put(recordB, recordSink);
            keyB.createValue();
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
        // this is an optimisation to release TableReader in case "this"
        // cursor lingers around. If there is exception or circuit breaker fault
        // we will rely on close() method to release reader.
        this.cursorB = Misc.free(this.cursorB);
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionCircuitBreaker circuitBreaker) throws SqlException {
        if (!isOpen) {
            isOpen = true;
            mapA.reopen();
            mapB.reopen();
        }

        super.of(cursorA, cursorB, circuitBreaker);
        recordA = cursorA.getRecord();
        recordB = cursorB.getRecord();
        isCursorBHashed = false;
    }
}

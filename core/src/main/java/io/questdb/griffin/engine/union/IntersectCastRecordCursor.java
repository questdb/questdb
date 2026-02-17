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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

class IntersectCastRecordCursor extends AbstractSetRecordCursor {
    private final UnionCastRecord castRecord;
    private final Map mapA;
    private final Map mapB;
    private final RecordSink recordSink;
    private boolean isCursorBHashed;
    private boolean isOpen;
    // this is the B record of except cursor, required by sort algo
    private UnionCastRecord recordB;

    public IntersectCastRecordCursor(
            Map mapA,
            Map mapB,
            RecordSink recordSink,
            @NotNull ObjList<Function> castFunctionA,
            @NotNull ObjList<Function> castFunctionB
    ) {
        this.mapA = mapA;
        this.mapB = mapB;
        isOpen = true;
        this.recordSink = recordSink;
        castRecord = new UnionCastRecord(castFunctionA, castFunctionB);
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
        return castRecord;
    }

    @Override
    public Record getRecordB() {
        if (recordB == null) {
            recordB = new UnionCastRecord(castRecord.getCastFunctionsA(), castRecord.getCastFunctionsB());
            recordB.setAb(true);
            // we do not need cursorB here, it is likely to be closed anyway
            recordB.of(cursorA.getRecordB(), null);
        }
        return recordB;
    }

    @Override
    public boolean hasNext() {
        if (!isCursorBHashed) {
            hashCursorB();
            castRecord.setAb(true);
            toTop();
            isCursorBHashed = true;
        }
        while (cursorA.hasNext()) {
            MapKey keyB = mapB.withKey();
            keyB.put(castRecord, recordSink);
            if (!keyB.notFound()) {
                MapKey keyA = mapA.withKey();
                keyA.put(castRecord, recordSink);
                if (keyA.create()) {
                    return true;
                }
            }
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
        return false;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        cursorA.recordAt(((UnionCastRecord) record).getRecordA(), atRowId);
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public long preComputedStateSize() {
        return isCursorBHashed ? 1 : 0;
    }

    @Override
    public void toTop() {
        cursorA.toTop();
        mapA.clear();
    }

    private void hashCursorB() {
        while (cursorB.hasNext()) {
            MapKey key = mapB.withKey();
            key.put(castRecord, recordSink);
            key.createValue();
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
        // this is an optimisation to release TableReader in case "this"
        // cursor lingers around. If there is exception or circuit breaker fault
        // we will rely on close() method to release reader.
        this.cursorB = Misc.free(this.cursorB);
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionCircuitBreaker circuitBreaker) {
        if (!isOpen) {
            isOpen = true;
            mapA.reopen();
            mapB.reopen();
        }

        this.cursorA = cursorA;
        this.cursorB = cursorB;
        this.circuitBreaker = circuitBreaker;
        castRecord.of(cursorA.getRecord(), cursorB.getRecord());
        castRecord.setAb(false);
        isCursorBHashed = false;
    }
}

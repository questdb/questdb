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
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;

class UnionRecordCursor extends AbstractSetRecordCursor implements NoRandomAccessRecordCursor {
    private final Map map;
    private final NextMethod nextB = this::nextB;
    private final AbstractUnionRecord record;
    private final RecordSink recordSink;
    private boolean isOpen;
    private NextMethod nextMethod;
    private final NextMethod nextA = this::nextA;

    public UnionRecordCursor(Map map, RecordSink recordSink, ObjList<Function> castFunctionsA, ObjList<Function> castFunctionsB) {
        if (castFunctionsA != null && castFunctionsB != null) {
            this.record = new UnionCastRecord(castFunctionsA, castFunctionsB);
        } else {
            assert castFunctionsA == null && castFunctionsB == null;
            this.record = new UnionRecord();
        }
        this.map = map;
        this.isOpen = true;
        this.recordSink = recordSink;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            map.close();
            super.close();
        }
    }

    @Override
    public Record getRecord() {
        return record;
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
    public long preComputedStateSize() {
        return cursorA.preComputedStateSize() + cursorB.preComputedStateSize();
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        map.clear();
        record.setAb(true);
        nextMethod = nextA;
        cursorA.toTop();
        cursorB.toTop();
    }

    private boolean nextA() {
        if (cursorA.hasNext()) {
            return true;
        }
        return switchToCursorB();
    }

    private boolean nextB() {
        return cursorB.hasNext();
    }

    private boolean switchToCursorB() {
        record.setAb(false);
        nextMethod = nextB;
        return nextMethod.next();
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionCircuitBreaker circuitBreaker) throws SqlException {
        if (!isOpen) {
            this.isOpen = true;
            this.map.reopen();
        }
        super.of(cursorA, cursorB, circuitBreaker);
        this.record.of(cursorA.getRecord(), cursorB.getRecord());
        toTop();
    }

    interface NextMethod {
        boolean next();
    }
}

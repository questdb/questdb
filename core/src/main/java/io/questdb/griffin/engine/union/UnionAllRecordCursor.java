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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;

class UnionAllRecordCursor extends AbstractSetRecordCursor implements NoRandomAccessRecordCursor {
    private final NextMethod nextB = this::nextB;
    private final AbstractUnionRecord record;
    private NextMethod nextMethod;
    private final NextMethod nextA = this::nextA;

    public UnionAllRecordCursor(ObjList<Function> castFunctionsA, ObjList<Function> castFunctionsB) {
        if (castFunctionsA != null && castFunctionsB != null) {
            this.record = new UnionCastRecord(castFunctionsA, castFunctionsB);
        } else {
            assert castFunctionsA == null && castFunctionsB == null;
            this.record = new UnionRecord();
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        cursorA.calculateSize(circuitBreaker, counter);
        cursorB.calculateSize(circuitBreaker, counter);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        return nextMethod.next();
    }

    @Override
    public long preComputedStateSize() {
        return cursorA.preComputedStateSize() + cursorB.preComputedStateSize();
    }

    @Override
    public long size() {
        final long sizeA = cursorA.size();
        final long sizeB = cursorB.size();
        if (sizeA == -1 || sizeB == -1) {
            return -1;
        }
        return sizeA + sizeB;
    }

    @Override
    public void skipRows(Counter rowCount) {
        cursorA.skipRows(rowCount);
        if (rowCount.get() > 0) {
            cursorB.skipRows(rowCount);
            record.setAb(false);
            nextMethod = nextB;
        }
    }

    @Override
    public void toTop() {
        record.setAb(true);
        nextMethod = nextA;
        cursorA.toTop();
        cursorB.toTop();
    }

    private boolean nextA() {
        return cursorA.hasNext() || switchToSlaveCursor();
    }

    private boolean nextB() {
        return cursorB.hasNext();
    }

    private boolean switchToSlaveCursor() {
        record.setAb(false);
        nextMethod = nextB;
        return nextMethod.next();
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionCircuitBreaker circuitBreaker) throws SqlException {
        super.of(cursorA, cursorB, circuitBreaker);
        record.of(cursorA.getRecord(), cursorB.getRecord());
        toTop();
    }

    interface NextMethod {
        boolean next();
    }
}

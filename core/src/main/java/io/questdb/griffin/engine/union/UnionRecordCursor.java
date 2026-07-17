/*+*****************************************************************************
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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;

class UnionRecordCursor extends AbstractSetRecordCursor implements NoRandomAccessRecordCursor, UnionSymbolSourceCursor {
    private final Map map;
    private final NextMethod nextB = this::nextB;
    private final AbstractUnionRecord record;
    private final RecordSink recordSink;
    private boolean isOpen;
    private boolean isUsingCursorA;
    private NextMethod nextMethod;
    private final NextMethod nextA = this::nextA;
    private int symbolSourceIndexA = -1;
    private int symbolSourceIndexB = -1;
    private SymbolSourceTracker symbolSourceTracker;

    public UnionRecordCursor(Map map, RecordSink recordSink, ObjList<Function> castFunctionsA, ObjList<Function> castFunctionsB) {
        if (castFunctionsA != null && castFunctionsB != null) {
            this.record = new UnionCastRecord(castFunctionsA, castFunctionsB);
        } else {
            assert castFunctionsA == null && castFunctionsB == null;
            this.record = new UnionRecord();
        }
        this.map = map;
        this.isOpen = false;
        this.recordSink = recordSink;
    }

    @Override
    public int bindSymbolSourceTracker(SymbolSourceTracker tracker, int nextSourceIndex) {
        symbolSourceTracker = tracker;
        if (cursorA instanceof UnionSymbolSourceCursor sourceCursor) {
            nextSourceIndex = sourceCursor.bindSymbolSourceTracker(tracker, nextSourceIndex);
            symbolSourceIndexA = -1;
        } else {
            symbolSourceIndexA = nextSourceIndex++;
        }
        if (cursorB instanceof UnionSymbolSourceCursor sourceCursor) {
            nextSourceIndex = sourceCursor.bindSymbolSourceTracker(tracker, nextSourceIndex);
            symbolSourceIndexB = -1;
        } else {
            symbolSourceIndexB = nextSourceIndex++;
        }
        return nextSourceIndex;
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
        isUsingCursorA = true;
        record.setAb(true);
        nextMethod = nextA;
        cursorA.toTop();
        cursorB.toTop();
    }

    @Override
    public void updateSymbolSource() {
        updateSymbolSource(isUsingCursorA ? cursorA : cursorB, isUsingCursorA ? symbolSourceIndexA : symbolSourceIndexB);
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
        isUsingCursorA = false;
        record.setAb(false);
        nextMethod = nextB;
        updateSymbolSource();
        return nextMethod.next();
    }

    private void updateSymbolSource(RecordCursor cursor, int sourceIndex) {
        if (symbolSourceTracker != null) {
            if (cursor instanceof UnionSymbolSourceCursor sourceCursor) {
                sourceCursor.updateSymbolSource();
            } else {
                symbolSourceTracker.of(cursor, sourceIndex);
            }
        }
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionContext executionContext) throws SqlException {
        if (!isOpen) {
            this.isOpen = true;
            this.map.setMemoryTracker(executionContext.getMemoryTracker());
            this.map.reopen();
        }
        super.of(cursorA, cursorB, executionContext);
        this.record.of(cursorA.getRecord(), cursorB.getRecord());
        toTop();
    }

    interface NextMethod {
        boolean next();
    }
}

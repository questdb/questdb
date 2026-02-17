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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongLongAscList;
import io.questdb.std.DirectLongLongDescList;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

class LongTopKRecordCursor implements RecordCursor {
    private final int columnIndex;
    private final DirectLongLongSortedList.Cursor rowIdCursor;
    private final DirectLongLongSortedList sortedList;
    private RecordCursor baseCursor;
    private Record baseRecord;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean initialized;
    private boolean isOpen;

    public LongTopKRecordCursor(int columnIndex, int lo, boolean ascending) {
        this.columnIndex = columnIndex;
        isOpen = true;
        sortedList = ascending
                ? new DirectLongLongAscList(lo, MemoryTag.NATIVE_DEFAULT)
                : new DirectLongLongDescList(lo, MemoryTag.NATIVE_DEFAULT);
        rowIdCursor = sortedList.getCursor();
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(sortedList);
            baseCursor = Misc.free(baseCursor);
            baseRecord = null;
        }
    }

    @Override
    public Record getRecord() {
        return baseRecord;
    }

    @Override
    public Record getRecordB() {
        return baseCursor.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return baseCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        setupTopK();
        if (rowIdCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            baseCursor.recordAt(baseRecord, rowIdCursor.index());
            return true;
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
        // assign base cursor as the first step, so that we close it in close() call
        this.baseCursor = baseCursor;
        baseRecord = baseCursor.getRecord();

        if (!isOpen) {
            isOpen = true;
            sortedList.reopen();
        }

        circuitBreaker = executionContext.getCircuitBreaker();
        initialized = false;
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(initialized) + baseCursor.preComputedStateSize();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(record, atRowId);
    }

    @Override
    public long size() {
        return initialized ? sortedList.size() : -1;
    }

    @Override
    public void toTop() {
        rowIdCursor.toTop();
        if (!initialized) {
            sortedList.clear();
            baseCursor.toTop();
        }
    }

    private void setupTopK() {
        if (!initialized) {
            topK();
            initialized = true;
        }
    }

    private void topK() {
        baseCursor.longTopK(sortedList, columnIndex);
        rowIdCursor.toTop();
    }
}

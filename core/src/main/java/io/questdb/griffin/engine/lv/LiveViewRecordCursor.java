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

package io.questdb.griffin.engine.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.ObjList;

/**
 * Cursor that iterates over the rows of a live view's InMemoryTable.
 * Acquires a read lock on the view instance for the duration of the query.
 */
public class LiveViewRecordCursor implements RecordCursor {
    private final LiveViewRecord record;
    private final LiveViewRecord recordB;
    private final LiveViewInstance viewInstance;
    private long currentRow;
    private boolean isOpen;
    private long rowCount;

    public LiveViewRecordCursor(LiveViewInstance viewInstance) {
        this.viewInstance = viewInstance;
        this.record = new LiveViewRecord(viewInstance.getTable());
        this.recordB = new LiveViewRecord(viewInstance.getTable());
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        counter.add(rowCount - currentRow);
        currentRow = rowCount;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            viewInstance.unlockAfterRead();
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return newSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (currentRow < rowCount) {
            record.setRow(currentRow++);
            return true;
        }
        return false;
    }

    public void open() {
        if (isOpen) {
            // cursor reuse: reset state without re-acquiring lock
            currentRow = 0;
            return;
        }
        viewInstance.lockForRead();
        try {
            rowCount = viewInstance.getTable().getRowCount();
            currentRow = 0;
            isOpen = true;
        } catch (Throwable t) {
            viewInstance.unlockAfterRead();
            throw t;
        }
    }

    // TODO(live-view): zero-GC — allocates a fresh anonymous SymbolTable on every call. Parallel queries may call this
    //  per query per SYMBOL column. Promote to a reusable named inner class held in an ObjList<LiveViewSymbolTable>
    //  sized to columnCount (mirroring PageFrameRecordCursorFactory).
    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        int type = viewInstance.getTable().getColumnType(columnIndex);
        if (ColumnType.tagOf(type) != ColumnType.SYMBOL) {
            return null;
        }
        ObjList<String> st = viewInstance.getTable().getSymbolTable(columnIndex);
        return new SymbolTable() {
            @Override
            public CharSequence valueBOf(int key) {
                return valueOf(key);
            }

            @Override
            public CharSequence valueOf(int key) {
                if (key < 0 || st == null || key >= st.size()) {
                    return null;
                }
                return st.getQuick(key);
            }
        };
    }

    @Override
    public long preComputedStateSize() {
        return rowCount;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((LiveViewRecord) record).setRow(atRowId);
    }

    @Override
    public long size() {
        return rowCount;
    }

    @Override
    public void toTop() {
        currentRow = 0;
    }
}

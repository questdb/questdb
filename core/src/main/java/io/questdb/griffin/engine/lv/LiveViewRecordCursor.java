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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.InMemoryTable;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectString;

/**
 * Cursor over a snapshot of a live view's double-buffered InMemoryTable. On {@link
 * #open} the cursor pins the currently-published buffer via {@link
 * LiveViewInstance#acquireForRead}; writes by the refresh worker go to the other
 * buffer, so the pinned buffer's bytes are frozen for the cursor's lifetime. On
 * {@link #close} the pin is released and, if the view has been dropped, a close
 * attempt is made.
 */
public class LiveViewRecordCursor implements RecordCursor {
    private final LiveViewRecord record = new LiveViewRecord(null);
    private final LiveViewRecord recordB = new LiveViewRecord(null);
    // Cached instances read pinnedBuffer on each access, so they stay valid across open/close cycles.
    private final ObjList<LiveViewSymbolTable> symbolTableCache = new ObjList<>();
    private final LiveViewInstance viewInstance;
    private long currentRow;
    private boolean isOpen;
    // Pinned published buffer, set on open() and released on close(). Null when closed.
    private InMemoryTable pinnedBuffer;
    private long rowCount;

    public LiveViewRecordCursor(LiveViewInstance viewInstance) {
        this.viewInstance = viewInstance;
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
            InMemoryTable buffer = pinnedBuffer;
            pinnedBuffer = null;
            record.setTable(null);
            recordB.setTable(null);
            viewInstance.releaseAfterRead(buffer);
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
        LiveViewSymbolTable st = symbolTableCache.getQuiet(columnIndex);
        if (st == null) {
            if (ColumnType.tagOf(pinnedBuffer.getColumnType(columnIndex)) != ColumnType.SYMBOL) {
                return null;
            }
            st = new LiveViewSymbolTable(columnIndex);
            symbolTableCache.extendAndSet(columnIndex, st);
        }
        return st;
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
            // cursor reuse: reset position but keep the existing pin (same snapshot).
            currentRow = 0;
            return;
        }
        InMemoryTable buffer = viewInstance.acquireForRead();
        if (buffer == null) {
            throw CairoException.nonCritical()
                    .put("live view was dropped [name=").put(viewInstance.getDefinition().getViewName()).put(']');
        }
        try {
            if (viewInstance.isInvalid()) {
                throw CairoException.nonCritical()
                        .put("live view is invalid [name=").put(viewInstance.getDefinition().getViewName())
                        .put(", reason=").put(viewInstance.getInvalidationReason()).put(']');
            }
            pinnedBuffer = buffer;
            record.setTable(buffer);
            recordB.setTable(buffer);
            rowCount = buffer.getRowCount();
            currentRow = 0;
            isOpen = true;
        } catch (Throwable t) {
            viewInstance.releaseAfterRead(buffer);
            throw t;
        }
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        if (ColumnType.tagOf(pinnedBuffer.getColumnType(columnIndex)) != ColumnType.SYMBOL) {
            return null;
        }
        // Parallel workers call this to get an isolated DirectString A/B pair, so a fresh instance is required.
        return new LiveViewSymbolTable(columnIndex);
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

    private final class LiveViewSymbolTable implements SymbolTable {
        private final int columnIndex;
        private final DirectString viewA = new DirectString();
        private final DirectString viewB = new DirectString();

        LiveViewSymbolTable(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public CharSequence valueBOf(int key) {
            DirectSymbolMap st = pinnedBuffer.getSymbolTable(columnIndex);
            return key >= 0 && st != null ? st.valueOf(key, viewB) : null;
        }

        @Override
        public CharSequence valueOf(int key) {
            DirectSymbolMap st = pinnedBuffer.getSymbolTable(columnIndex);
            return key >= 0 && st != null ? st.valueOf(key, viewA) : null;
        }
    }
}

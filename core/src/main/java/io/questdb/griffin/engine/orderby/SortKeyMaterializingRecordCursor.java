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

import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

class SortKeyMaterializingRecordCursor implements DelegatingRecordCursor {
    private static final long INITIAL_BUFFER_CAPACITY = 1024;
    private final int[] bufferToColIndex;
    private final DirectLongList[] buffers;
    private final int[] colToBufferIndex;
    private final int[] colTypes;
    private final MaterializedRecord recordA = new MaterializedRecord();
    private final MaterializedRecord recordB = new MaterializedRecord();
    private final LongIntHashMap rowIdToOrdinal = new LongIntHashMap();
    private RecordCursor baseCursor;
    private boolean isOpen;
    private int nextOrdinal;

    SortKeyMaterializingRecordCursor(int columnCount, IntList materializedColIndices, IntList materializedColTypes) {
        final int bufferCount = materializedColIndices.size();
        this.buffers = new DirectLongList[bufferCount];
        this.colToBufferIndex = new int[columnCount];
        this.bufferToColIndex = new int[bufferCount];
        this.colTypes = new int[bufferCount];

        for (int i = 0; i < columnCount; i++) {
            colToBufferIndex[i] = -1;
        }

        for (int i = 0; i < bufferCount; i++) {
            final int colIndex = materializedColIndices.getQuick(i);
            //noinspection resource
            buffers[i] = new DirectLongList(INITIAL_BUFFER_CAPACITY, MemoryTag.NATIVE_TREE_CHAIN);
            colToBufferIndex[colIndex] = i;
            bufferToColIndex[i] = colIndex;
            colTypes[i] = materializedColTypes.getQuick(i);
        }
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            for (int i = 0, n = buffers.length; i < n; i++) {
                buffers[i].shrink(INITIAL_BUFFER_CAPACITY);
            }
            baseCursor = Misc.free(baseCursor);
        }
    }

    void freeBuffers() {
        close();
        for (int i = 0, n = buffers.length; i < n; i++) {
            buffers[i] = Misc.free(buffers[i]);
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return baseCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (baseCursor.hasNext()) {
            final Record baseRecord = recordA.getBaseRecord();
            final long rowId = baseRecord.getRowId();
            final int ordinal = nextOrdinal++;
            rowIdToOrdinal.put(rowId, ordinal);
            for (int i = 0, n = buffers.length; i < n; i++) {
                buffers[i].add(MaterializedRecord.extractValue(baseRecord, bufferToColIndex[i], colTypes[i]));
            }
            recordA.setOrdinal(ordinal);
            return true;
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
        this.baseCursor = baseCursor;
        isOpen = true;
        for (int i = 0, n = buffers.length; i < n; i++) {
            buffers[i].clear();
        }
        rowIdToOrdinal.clear();
        nextOrdinal = 0;
        recordA.of(baseCursor.getRecord(), colToBufferIndex, colTypes, buffers);
        recordB.of(baseCursor.getRecordB(), colToBufferIndex, colTypes, buffers);
    }

    @Override
    public long preComputedStateSize() {
        long size = 0;
        for (int i = 0, n = buffers.length; i < n; i++) {
            size += buffers[i].size();
        }
        return size;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(((MaterializedRecord) record).getBaseRecord(), atRowId);
        final int ordinal = rowIdToOrdinal.get(atRowId);
        ((MaterializedRecord) record).setOrdinal(ordinal);
    }

    @Override
    public long size() {
        return baseCursor.size();
    }

    @Override
    public void toTop() {
        baseCursor.toTop();
    }
}

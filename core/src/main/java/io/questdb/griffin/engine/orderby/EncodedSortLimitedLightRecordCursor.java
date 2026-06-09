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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;

class EncodedSortLimitedLightRecordCursor implements DelegatingRecordCursor {
    protected final EncodedTopKHeap heap;
    protected RecordCursor baseCursor;
    protected Record baseRecord;
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected long limit;
    private final SortKeyEncoder encoder;
    private long currentAddr;
    private long endAddr;
    private int entrySize;
    private boolean isBuilt;
    private boolean isFirstN;
    private boolean isOpen;
    private long rowsLeft;
    private long skipFirst;
    private long skipLast;

    EncodedSortLimitedLightRecordCursor(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            IntList sortColumnFilter
    ) {
        SortKeyEncoder encoderInit = null;
        EncodedTopKHeap heapInit = null;
        try {
            encoderInit = new SortKeyEncoder(metadata, sortColumnFilter);
            heapInit = new EncodedTopKHeap(configuration.getSqlSortEncodedParallelThreshold());
        } catch (Throwable th) {
            Misc.free(encoderInit);
            Misc.free(heapInit);
            throw th;
        }
        this.encoder = encoderInit;
        this.heap = heapInit;
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(heap);
            Misc.free(encoder);
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
        if (!isBuilt) {
            buildAndSort();
            isBuilt = true;
        }
        if (rowsLeft <= 0 || currentAddr >= endAddr) {
            return false;
        }
        circuitBreaker.statefulThrowExceptionIfTripped();
        final long rowId = Unsafe.getLong(currentAddr);
        currentAddr += entrySize;
        rowsLeft--;
        baseCursor.recordAt(baseRecord, rowId);
        return true;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        this.baseCursor = baseCursor;
        this.baseRecord = baseCursor.getRecord();
        if (!isOpen) {
            isOpen = true;
            heap.reopen();
        }
        final SortKeyType keyType = encoder.init(baseCursor);
        assert keyType != SortKeyType.UNSUPPORTED;
        entrySize = keyType.entrySize();
        circuitBreaker = executionContext.getCircuitBreaker();
        isBuilt = false;
        heap.of(keyType, limit, isFirstN);
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isBuilt) + baseCursor.preComputedStateSize();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(record, atRowId);
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        baseCursor.setParquetDecodeHint(hint);
    }

    public void setSelection(boolean isFirstN, long limit, long skipFirst, long skipLast) {
        this.isFirstN = isFirstN;
        this.limit = Math.max(limit, 0);
        this.skipFirst = Math.max(skipFirst, 0);
        this.skipLast = Math.max(skipLast, 0);
    }

    @Override
    public long size() {
        return isBuilt ? Math.max(heap.count() - skipFirst - skipLast, 0) : -1;
    }

    @Override
    public void toTop() {
        final long count = heap.count();
        final long startSkip = Math.min(skipFirst, count);
        final long emit = Math.max(count - skipFirst - skipLast, 0);
        currentAddr = heap.sortedStartAddr() + startSkip * entrySize;
        endAddr = currentAddr + emit * entrySize;
        rowsLeft = emit;
    }

    protected void buildAndSort() {
        try {
            if (limit > 0) {
                runBuild();
            }
        } finally {
            Misc.free(encoder);
        }
        heap.finalizeSort(circuitBreaker);
        toTop();
    }

    protected final void encodeAndPushCurrentRow() {
        encoder.encode(baseRecord, heap.scratchAddr(), baseRecord.getRowId());
        heap.tryPush();
    }

    protected void runBuild() {
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            encodeAndPushCurrentRow();
        }
    }
}

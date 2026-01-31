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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

class LongSortedLightRecordCursor implements DelegatingRecordCursor {
    private static final RecordAdapter getIPv4AsLongRef = LongSortedLightRecordCursor::getIPv4AsLong;
    private static final RecordAdapter getIntAsLongRef = LongSortedLightRecordCursor::getIntAsLong;
    private static final RecordAdapter getLongRef = LongSortedLightRecordCursor::getLong;
    private final int columnIndex;
    private final int columnType;
    private final long radixSortThreshold;
    private final Cursor rowIdCursor;
    private final DirectLongList valueRowIdMem; // holds <value, rowId> pairs
    private final DirectLongList valueRowIdMemCpy; // used in radix sort
    private boolean areValuesSorted;
    private RecordCursor baseCursor;
    private Record baseRecord;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean isOpen;
    private RecordAdapter recordAdapter;

    public LongSortedLightRecordCursor(CairoConfiguration configuration, int columnIndex, int columnType, boolean ascOrder) {
        try {
            this.columnIndex = columnIndex;
            this.columnType = columnType;
            isOpen = true;
            radixSortThreshold = configuration.getSqlOrderByRadixSortThreshold();
            valueRowIdMem = new DirectLongList(configuration.getSqlSortLightValuePageSize() / 16, MemoryTag.NATIVE_DEFAULT);
            valueRowIdMemCpy = new DirectLongList(configuration.getSqlSortLightValuePageSize() / 16, MemoryTag.NATIVE_DEFAULT);
            rowIdCursor = ascOrder ? new FwdCursor() : new BwdCursor();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(valueRowIdMem);
            Misc.free(valueRowIdMemCpy);
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
        if (!areValuesSorted) {
            sortValues();
            areValuesSorted = true;
        }
        if (rowIdCursor.hasNext()) {
            baseCursor.recordAt(baseRecord, rowIdCursor.next());
            return true;
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        // assign base cursor as the first step, so that we close it in close() call
        this.baseCursor = baseCursor;
        baseRecord = baseCursor.getRecord();

        if (!isOpen) {
            isOpen = true;
            valueRowIdMem.reopen();
        }

        final int columnTypeTag = ColumnType.tagOf(columnType);
        switch (columnTypeTag) {
            case ColumnType.LONG:
            case ColumnType.TIMESTAMP:
            case ColumnType.DATE:
                recordAdapter = getLongRef;
                break;
            case ColumnType.INT:
                recordAdapter = getIntAsLongRef;
                break;
            case ColumnType.IPv4:
                recordAdapter = getIPv4AsLongRef;
                break;
            default:
                throw SqlException.position(0).put("unsupported order by column type: ").put(ColumnType.nameOf(columnTypeTag));
        }
        circuitBreaker = executionContext.getCircuitBreaker();
        areValuesSorted = false;
    }

    @Override
    public long preComputedStateSize() {
        return areValuesSorted ? 1 : 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(record, atRowId);
    }

    @Override
    public long size() {
        return baseCursor.size();
    }

    @Override
    public void toTop() {
        rowIdCursor.toTop();
        if (!areValuesSorted) {
            valueRowIdMem.clear();
            baseCursor.toTop();
        }
    }

    private static long getIPv4AsLong(Record record, int columnIndex) {
        return record.getLongIPv4(columnIndex);
    }

    private static long getIntAsLong(Record record, int columnIndex) {
        final int value = record.getInt(columnIndex);
        if (value != Numbers.INT_NULL) {
            return value;
        }
        return Numbers.LONG_NULL;
    }

    private static long getLong(Record record, int columnIndex) {
        return record.getLong(columnIndex);
    }

    private void sortValues() {
        // first, copy all values to the buffer
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            // Note: the native radix sort implementation handles signed-to-unsigned
            // conversion internally via XOR with the sign bit in index_t operators
            valueRowIdMem.add(recordAdapter.getLong(baseRecord, columnIndex));
            valueRowIdMem.add(baseRecord.getRowId());
        }
        // now do the actual sort
        final long size = valueRowIdMem.size();
        if (size > 0) {
            if (size > radixSortThreshold) {
                // radix sort
                valueRowIdMemCpy.reopen();
                valueRowIdMemCpy.setCapacity(valueRowIdMem.size());
                Vect.radixSortLongIndexAscInPlace(valueRowIdMem.getAddress(), valueRowIdMem.size() >>> 1, valueRowIdMemCpy.getAddress());
                valueRowIdMemCpy.close();
            } else {
                // quick sort
                Vect.quickSortLongIndexAscInPlace(valueRowIdMem.getAddress(), valueRowIdMem.size() >>> 1);
            }
        }
        // we're ready to go
        rowIdCursor.toTop();
    }

    private interface Cursor {
        boolean hasNext();

        long next();

        void toTop();
    }

    @FunctionalInterface
    private interface RecordAdapter {
        long getLong(Record record, int columnIndex);
    }

    private class BwdCursor implements Cursor {
        private long pos;

        @Override
        public boolean hasNext() {
            return --pos > -1;
        }

        @Override
        public long next() {
            // we need row ids
            return valueRowIdMem.get((pos << 1) + 1);
        }

        @Override
        public void toTop() {
            pos = valueRowIdMem.size() >>> 1;
        }
    }

    private class FwdCursor implements Cursor {
        private long limit;
        private long pos;

        @Override
        public boolean hasNext() {
            return ++pos < limit;
        }

        @Override
        public long next() {
            // we need row ids
            return valueRowIdMem.get((pos << 1) + 1);
        }

        @Override
        public void toTop() {
            pos = -1;
            limit = valueRowIdMem.size() >>> 1;
        }
    }
}

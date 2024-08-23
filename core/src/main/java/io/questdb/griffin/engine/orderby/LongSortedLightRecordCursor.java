/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Vect;

class LongSortedLightRecordCursor implements DelegatingRecordCursor {
    private final int columnIndex;
    private final long radixSortThreshold;
    private final Cursor rowIdCursor;
    private final DirectLongList valueRowIdMem; // holds <value, rowId> pairs
    private final DirectLongList valueRowIdMemCpy; // used in radix sort
    private boolean areValuesSorted;
    private RecordCursor base;
    private Record baseRecord;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean isOpen;

    public LongSortedLightRecordCursor(CairoConfiguration configuration, int columnIndex, boolean ascOrder) {
        try {
            this.columnIndex = columnIndex;
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
            base = Misc.free(base);
            baseRecord = null;
        }
    }

    @Override
    public Record getRecord() {
        return baseRecord;
    }

    @Override
    public Record getRecordB() {
        return base.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!areValuesSorted) {
            sortValues();
            areValuesSorted = true;
        }
        if (rowIdCursor.hasNext()) {
            base.recordAt(baseRecord, rowIdCursor.next());
            return true;
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return base.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor base, SqlExecutionContext executionContext) {
        if (!isOpen) {
            isOpen = true;
            valueRowIdMem.reopen();
        }

        this.base = base;
        baseRecord = base.getRecord();
        circuitBreaker = executionContext.getCircuitBreaker();
        areValuesSorted = false;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        base.recordAt(record, atRowId);
    }

    @Override
    public long size() {
        return base.size();
    }

    @Override
    public void toTop() {
        rowIdCursor.toTop();
        if (!areValuesSorted) {
            valueRowIdMem.clear();
            base.toTop();
        }
    }

    private void sortValues() {
        // first, copy all values to the buffer
        while (base.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            // later sort assumes unsigned 64-bit integers,
            // so we flip the highest bit to get the correct order
            valueRowIdMem.add(baseRecord.getLong(columnIndex) ^ Long.MIN_VALUE);
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

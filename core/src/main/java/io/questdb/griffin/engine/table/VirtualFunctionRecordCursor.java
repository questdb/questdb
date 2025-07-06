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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualFunctionRecord;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class VirtualFunctionRecordCursor implements RecordCursor {
    protected final VirtualFunctionRecord recordA;
    private final ObjList<Function> functions;
    private final int memoizerCount;
    private final ObjList<Function> memoizers;
    private final VirtualFunctionRecord recordB;
    private final boolean supportsRandomAccess;
    protected RecordCursor baseCursor;

    public VirtualFunctionRecordCursor(
            ObjList<Function> functions,
            ObjList<Function> memoizers,
            boolean supportsRandomAccess,
            int virtualColumnReservedSlots
    ) {
        this.functions = functions;
        this.memoizers = memoizers;
        this.memoizerCount = memoizers.size();
        if (supportsRandomAccess) {
            this.recordA = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
            this.recordB = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
        } else {
            this.recordA = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
            this.recordB = null;
        }
        this.supportsRandomAccess = supportsRandomAccess;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        assert baseCursor != null;
        baseCursor.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        baseCursor = Misc.free(baseCursor);
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).cursorClosed();
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        if (supportsRandomAccess) {
            return recordB;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) functions.getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        final boolean result = baseCursor.hasNext();
        if (result && memoizerCount > 0) {
            memoizeFunctions(recordA);
        }
        return result;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) functions.getQuick(columnIndex)).newSymbolTable();
    }

    public void of(RecordCursor cursor) {
        baseCursor = cursor;
        recordA.of(baseCursor.getRecord());
        if (recordB != null) {
            recordB.of(baseCursor.getRecordB());
        }
        cursor.toTop();
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        if (supportsRandomAccess) {
            assert baseCursor != null;
            baseCursor.recordAt(((VirtualFunctionRecord) record).getBaseRecord(), atRowId);
            memoizeFunctions((VirtualFunctionRecord) record);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public long size() {
        assert baseCursor != null;
        return baseCursor.size();
    }

    @Override
    public void skipRows(Counter rowCount) throws DataUnavailableException {
        assert baseCursor != null;
        baseCursor.skipRows(rowCount);
    }

    @Override
    public void toTop() {
        assert baseCursor != null;
        baseCursor.toTop();
        GroupByUtils.toTop(functions);
    }

    private void memoizeFunctions(VirtualFunctionRecord record) {
        Record joinRecord = record.getInternalJoinRecord();
        for (int i = 0; i < memoizerCount; i++) {
            memoizers.getQuick(i).memoize(joinRecord);
        }
    }
}

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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;

class ExtraNullColumnRecordCursor implements RecordCursor {
    private final int columnSplit;
    private final ExtraNullColumnRecord recordA;
    private final ExtraNullColumnRecord recordB;
    private RecordCursor baseCursor;

    public ExtraNullColumnRecordCursor(
            int columnSplit,
            boolean supportsRandomAccess,
            @Nullable LongList emptyValues,
            @Nullable IntList emptyValueTypes
    ) {
        this.recordA = newRecord(columnSplit, emptyValues, emptyValueTypes);
        if (supportsRandomAccess) {
            this.recordB = newRecord(columnSplit, emptyValues, emptyValueTypes);
        } else {
            this.recordB = null;
        }
        this.columnSplit = columnSplit;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        baseCursor.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        baseCursor = Misc.free(baseCursor);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        if (recordB != null) {
            return recordB;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return columnIndex < columnSplit ? baseCursor.getSymbolTable(columnIndex) : EmptySymbolMapReader.INSTANCE;
    }

    @Override
    public boolean hasNext() {
        return baseCursor.hasNext();
    }

    @Override
    public void longTopK(DirectLongLongSortedList list, int columnIndex) {
        if (columnIndex < columnSplit) {
            baseCursor.longTopK(list, columnIndex);
        }
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return columnIndex < columnSplit ? baseCursor.newSymbolTable(columnIndex) : EmptySymbolMapReader.INSTANCE;
    }

    @Override
    public long preComputedStateSize() {
        return baseCursor.preComputedStateSize();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(((ExtraNullColumnRecord) record).getBaseRecord(), atRowId);
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        baseCursor.setParquetDecodeHint(hint);
    }

    @Override
    public long size() {
        return baseCursor.size();
    }

    @Override
    public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
        baseCursor.skipRows(rowCount, maxRowsAfterSkip);
    }

    @Override
    public void toTop() {
        baseCursor.toTop();
    }

    void of(RecordCursor cursor) {
        this.baseCursor = cursor;
        recordA.of(cursor.getRecord());
        if (recordB != null) {
            recordB.of(cursor.getRecordB());
        }
    }

    static ExtraNullColumnRecord newRecord(int columnSplit, @Nullable LongList emptyValues, @Nullable IntList emptyValueTypes) {
        return emptyValues != null
                ? new ExtraEmptyValueColumnRecord(columnSplit, emptyValues, emptyValueTypes)
                : new ExtraNullColumnRecord(columnSplit);
    }
}

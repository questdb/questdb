/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.IntList;

class SelectedRecordCursor implements RecordCursor {
    private final SelectedRecord recordA;
    private final SelectedRecord recordB;
    private final IntList columnCrossIndex;
    private RecordCursor baseCursor;

    public SelectedRecordCursor(IntList columnCrossIndex, boolean supportsRandomAccess) {
        this.recordA = new SelectedRecord(columnCrossIndex);
        if (supportsRandomAccess) {
            this.recordB = new SelectedRecord(columnCrossIndex);
        } else {
            this.recordB = null;
        }
        this.columnCrossIndex = columnCrossIndex;
    }

    @Override
    public void close() {
        baseCursor.close();
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return baseCursor.getSymbolTable(columnCrossIndex.getQuick(columnIndex));
    }

    @Override
    public long size() {
        return baseCursor.size();
    }

    @Override
    public boolean hasNext() {
        return baseCursor.hasNext();
    }

    @Override
    public Record getRecordB() {
        if (recordB != null) {
            return recordB;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(((SelectedRecord) record).getBaseRecord(), atRowId);
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
}

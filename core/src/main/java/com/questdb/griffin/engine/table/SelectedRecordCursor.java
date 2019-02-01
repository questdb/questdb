/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.table;

import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.SymbolTable;
import com.questdb.std.IntList;

class SelectedRecordCursor implements RecordCursor {
    private final SelectedRecord record;
    private final IntList columnCrossIndex;
    private RecordCursor baseCursor;

    public SelectedRecordCursor(IntList columnCrossIndex) {
        this.record = new SelectedRecord(columnCrossIndex);
        this.columnCrossIndex = columnCrossIndex;
    }

    @Override
    public void close() {
        baseCursor.close();
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return baseCursor.getSymbolTable(columnCrossIndex.getQuick(columnIndex));
    }

    @Override
    public boolean hasNext() {
        return baseCursor.hasNext();
    }

    @Override
    public Record newRecord() {
        SelectedRecord record = new SelectedRecord(this.record.getColumnCrossIndex());
        record.of(baseCursor.newRecord());
        return record;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(((SelectedRecord) record).getBaseRecord(), atRowId);
    }

    @Override
    public void recordAt(long rowId) {
        baseCursor.recordAt(rowId);
    }

    @Override
    public void toTop() {
        baseCursor.toTop();
    }

    void of(RecordCursor cursor) {
        this.baseCursor = cursor;
        record.of(cursor.getRecord());
    }
}

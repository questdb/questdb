/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.common.SymbolTable;

class FilteredRecordCursor implements RecordCursor {
    private final Function filter;
    private RecordCursor base;

    public FilteredRecordCursor(Function filter) {
        this.filter = filter;
    }

    @Override
    public void close() {
        base.close();
    }

    @Override
    public Record getRecord() {
        return base.getRecord();
    }

    @Override
    public Record newRecord() {
        return base.newRecord();
    }

    @Override
    public Record recordAt(long rowId) {
        return base.recordAt(rowId);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        base.recordAt(record, atRowId);
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(columnIndex);
    }

    @Override
    public void toTop() {
        base.toTop();
    }

    @Override
    public boolean hasNext() {
        while (base.hasNext()) {
            if (filter.getBool(next())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Record next() {
        return base.getRecord();
    }

    void of(RecordCursor base) {
        this.base = base;
    }
}

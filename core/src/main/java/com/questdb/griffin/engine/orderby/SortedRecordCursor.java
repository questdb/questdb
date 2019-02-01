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

package com.questdb.griffin.engine.orderby;

import com.questdb.cairo.sql.DelegatingRecordCursor;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.SymbolTable;

class SortedRecordCursor implements DelegatingRecordCursor {
    private final RecordTreeChain chain;
    private RecordTreeChain.TreeCursor chainCursor;

    public SortedRecordCursor(RecordTreeChain chain) {
        this.chain = chain;
    }

    @Override
    public void close() {
        chainCursor.close();
        chain.clear();
    }

    @Override
    public Record getRecord() {
        return chainCursor.getRecord();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return chainCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        return chainCursor.hasNext();
    }

    @Override
    public Record newRecord() {
        return chainCursor.newRecord();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        chainCursor.recordAt(record, atRowId);
    }

    @Override
    public void recordAt(long rowId) {
        chainCursor.recordAt(rowId);
    }

    @Override
    public void toTop() {
        chainCursor.toTop();
    }

    @Override
    public void of(RecordCursor base) {
        this.chainCursor = chain.getCursor(base);
        final Record record = base.getRecord();

        chain.clear();
        while (base.hasNext()) {
            // Tree chain is liable to re-position record to
            // other rows to do record comparison. We must use our
            // own record instance in case base cursor keeps
            // state in the record it returns.
            chain.put(record);
        }
        chainCursor.toTop();
    }
}

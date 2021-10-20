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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

class FilteredRecordCursor implements RecordCursor {
    private final Function filter;
    private RecordCursor base;
    private Record record;

    public FilteredRecordCursor(Function filter) {
        this.filter = filter;
    }

    @Override
    public void close() {
        base.close();
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (filter.isConstant()) {
            return filterIsConstant();
        }

        while (base.hasNext()) {
            if (filter.getBool(record)) {
                return true;
            }
        }

        return false;
    }

    private boolean filterIsConstant() {
        if (filter.getBool(record)) {
            return base.hasNext();
        }
        return false;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public Record getRecordB() {
        return base.getRecordB();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        base.recordAt(record, atRowId);
    }

    @Override
    public void toTop() {
        base.toTop();
        filter.toTop();
    }

    void of(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
        this.base = base;
        this.record = base.getRecord();
        filter.init(this, executionContext);
    }
}

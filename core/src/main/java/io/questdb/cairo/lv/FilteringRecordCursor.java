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

package io.questdb.cairo.lv;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

/**
 * Minimal pull-mode filter on top of a base {@link RecordCursor}. Applies the given
 * {@link Function} filter per row and skips rows for which it returns false.
 * <p>
 * The live view refresh job uses this wrapper to apply a WHERE clause to WAL segment
 * rows during incremental refresh, without reinvoking the compiled filter factory's
 * getCursor() (which would open a new base cursor). Filter state is re-initialised on
 * every {@link #of} so that bind variables and symbol-table caches reflect the current
 * base cursor.
 * <p>
 * Not to be confused with the table-package FilteredRecordCursor which is tied to a
 * FilteredRecordCursorFactory lifecycle.
 */
final class FilteringRecordCursor implements RecordCursor {
    private RecordCursor base;
    private Function filter;
    private Record record;

    @Override
    public void close() {
        base = null;
        record = null;
        filter = null;
    }

    @Override
    public Record getRecord() {
        return record;
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
        while (base.hasNext()) {
            if (filter.getBool(record)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return base.newSymbolTable(columnIndex);
    }

    public void of(RecordCursor base, Function filter, SqlExecutionContext executionContext) throws SqlException {
        this.base = base;
        this.record = base.getRecord();
        this.filter = filter;
        filter.init(base, executionContext);
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        base.recordAt(record, atRowId);
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        base.toTop();
        filter.toTop();
    }
}

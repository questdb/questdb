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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

/**
 * One consumer's access to a {@link SubqueryResult}: the sub-query's own cursor while the sub-query has a single
 * consumer, otherwise a cursor over the rows the result materialised for the current execution. A page-frame cursor
 * always comes from the sub-query's own factory. Worker clones, which inherit their owner's value, read through a
 * non-consumer view.
 */
public final class SubqueryRecordCursorFactory extends AbstractRecordCursorFactory {
    private final boolean isConsumer;
    private final SubqueryResult result;
    private final RowsCursor rowsCursor = new RowsCursor();

    public SubqueryRecordCursorFactory(SubqueryResult result, boolean isConsumer) {
        super(result.getBase().getMetadata());
        this.result = result;
        this.isConsumer = isConsumer;
        result.acquire(isConsumer);
    }

    @Override
    public String getBaseColumnName(int idx) {
        return result.getBase().getBaseColumnName(idx);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (!result.isShared()) {
            return result.getBase().getCursor(executionContext);
        }
        rowsCursor.of(result.getRows(executionContext), result.getBaseCursor());
        return rowsCursor;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        return result.getBase().getPageFrameCursor(executionContext, order);
    }

    @Override
    public boolean isNonDeterministic() {
        return result.getBase().isNonDeterministic();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return result.getBase().supportsPageFrameCursor();
    }

    @Override
    public void toPlan(PlanSink sink) {
        result.getBase().toPlan(sink);
    }

    @Override
    public boolean usesExternalDataSource() {
        return result.getBase().usesExternalDataSource();
    }

    @Override
    protected void _close() {
        result.release(isConsumer);
    }

    private static final class RowsCursor implements NoRandomAccessRecordCursor {
        private long index;
        private Record record;
        private RecordArray rows;
        private RecordCursor symbols;

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            counter.add(rows.size() - index);
            index = rows.size();
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return symbols.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (index < rows.size()) {
                rows.recordAtRowIndex(record, index++);
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return symbols.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return rows.size();
        }

        @Override
        public void toTop() {
            index = 0;
        }

        private void of(RecordArray rows, RecordCursor symbols) {
            if (record == null) {
                record = rows.newRecord();
            }
            this.rows = rows;
            this.symbols = symbols;
            index = 0;
        }
    }
}

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

package io.questdb.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

/**
 * Copies the rows of its base into a record array as a cursor first reads them and replays the copy after every
 * rewind, so the slave of a nested loop computes its rows once per execution instead of once per master row.
 */
public class MaterializedRecordCursorFactory extends AbstractRecordCursorFactory {
    private RecordCursorFactory base;
    private MaterializedRecordCursor cursor;

    /**
     * Consumes the base on entry, including on failure.
     */
    public MaterializedRecordCursorFactory(CairoConfiguration configuration, RecordCursorFactory base, RecordSink recordSink) {
        super(base.getMetadata());
        this.base = base;
        RecordArray rows = null;
        try {
            // The copy starts at the light join's value page, so a small slave costs little memory, and doubles from
            // there up to the bytes a full-fat join's value pages may hold.
            final long pageSize = Numbers.ceilPow2(configuration.getSqlHashJoinLightValuePageSize());
            final long maxBytes = (long) Numbers.ceilPow2(configuration.getSqlHashJoinValuePageSize()) * configuration.getSqlHashJoinValueMaxPages();
            rows = new RecordArray(
                    base.getMetadata(),
                    recordSink,
                    pageSize,
                    (int) Math.min(Integer.MAX_VALUE, Math.max(1, maxBytes / pageSize)),
                    PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES.getPropertyPath()
            );
            cursor = new MaterializedRecordCursor(rows);
        } catch (Throwable th) {
            Misc.free(rows, th);
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(baseCursor, th);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean isNonDeterministic() {
        return base.isNonDeterministic();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Materialize");
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final MaterializedRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Reads its rows from the copy, which it extends from the base whenever a pass reads past the rows it holds.
     */
    private static class MaterializedRecordCursor implements DelegatingRecordCursor {
        private final RecordArray rows;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isBaseExhausted;

        private MaterializedRecordCursor(RecordArray rows) {
            this.rows = rows;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            while (copyNext()) {
                this.circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            }
            rows.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
            Misc.free(rows);
        }

        @Override
        public Record getRecord() {
            return rows.getRecord();
        }

        @Override
        public Record getRecordB() {
            return rows.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext() || copyNext() && rows.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
            rows.clear();
            rows.setMemoryTracker(executionContext.getMemoryTracker());
            rows.setSymbolTableResolver(baseCursor);
            circuitBreaker = executionContext.getCircuitBreaker();
            baseRecord = baseCursor.getRecord();
            isBaseExhausted = false;
            this.baseCursor = baseCursor;
        }

        @Override
        public long preComputedStateSize() {
            return rows.size();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            rows.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public void toTop() {
            rows.toTop();
        }

        /**
         * Appends the next row of the base to the copy; false once the base has no more rows.
         */
        private boolean copyNext() {
            if (isBaseExhausted || !baseCursor.hasNext()) {
                isBaseExhausted = true;
                return false;
            }
            rows.put(baseRecord);
            return true;
        }
    }
}

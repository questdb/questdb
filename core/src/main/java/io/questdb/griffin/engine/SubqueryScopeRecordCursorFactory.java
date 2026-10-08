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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * The root of a statement whose sub-queries share their rows between several consumers. Opening a cursor starts an
 * execution: every shared {@link SubqueryResult} drops the rows of the previous execution, so each execution,
 * including every re-execution of a cached factory, evaluates each shared sub-query once. Closing the record cursor
 * ends the execution and releases the rows; a page-frame or time-frame cursor releases them at the next execution or
 * when the factory closes. The results belong to their consumers inside the base factory.
 */
public final class SubqueryScopeRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final ScopeRecordCursor cursor = new ScopeRecordCursor();
    private final ObjList<SubqueryResult> results;

    public SubqueryScopeRecordCursorFactory(RecordCursorFactory base, ObjList<SubqueryResult> results) {
        super(base.getMetadata());
        this.base = base;
        this.results = results;
    }

    @Override
    public void changePageFrameSizes(int minRows, int maxRows) {
        base.changePageFrameSizes(minRows, maxRows);
    }

    @Override
    public PageFrameSequence<?> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        releaseRows();
        return base.execute(executionContext, collectSubSeq, order);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return base.fragmentedSymbolTables();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return base.getBaseColumnName(idx);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        releaseRows();
        try {
            cursor.of(base.getCursor(executionContext));
        } catch (Throwable th) {
            releaseRows();
            throw th;
        }
        return cursor;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        releaseRows();
        return base.getPageFrameCursor(executionContext, order);
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        releaseRows();
        return base.getTimeFrameCursor(executionContext);
    }

    @Override
    public void halfClose() {
        base.halfClose();
    }

    @Override
    public boolean hasParquetConvertedColumns(SqlExecutionContext executionContext) {
        return base.hasParquetConvertedColumns(executionContext);
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean isNonDeterministic() {
        return base.isNonDeterministic();
    }

    @Override
    public boolean mayHaveParquetPartitions(SqlExecutionContext executionContext) {
        return base.mayHaveParquetPartitions(executionContext);
    }

    @Override
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        return base.newTimeFrameCursor();
    }

    @Override
    public boolean producesMaterializedPageFrames() {
        return base.producesMaterializedPageFrames();
    }

    @Override
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        return base.recordCursorSupportsLongTopK(columnIndex);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return base.supportsPageFrameCursor();
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        return base.supportsTimeFrameCursor();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableName) {
        return base.supportsUpdateRowId(tableName);
    }

    @Override
    public void toPlan(PlanSink sink) {
        base.toPlan(sink);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesExternalDataSource() {
        return base.usesExternalDataSource();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        try {
            releaseRows();
        } finally {
            Misc.free(base);
        }
    }

    private void releaseRows() {
        Throwable failure = null;
        for (int i = 0, n = results.size(); i < n; i++) {
            try {
                results.getQuick(i).releaseRows();
            } catch (Throwable th) {
                if (failure == null) {
                    failure = th;
                } else {
                    failure.addSuppressed(th);
                }
            }
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    private final class ScopeRecordCursor implements RecordCursor {
        private RecordCursor baseCursor;

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            baseCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (baseCursor != null) {
                try {
                    baseCursor = Misc.free(baseCursor);
                } finally {
                    releaseRows();
                }
            }
        }

        @Override
        public void expectLimitedIteration() {
            baseCursor.expectLimitedIteration();
        }

        @Override
        public Record getRecord() {
            return baseCursor.getRecord();
        }

        @Override
        public Record getRecordB() {
            return baseCursor.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return baseCursor.hasNext();
        }

        @Override
        public boolean isUsingIndex() {
            return baseCursor.isUsingIndex();
        }

        @Override
        public void longTopK(DirectLongLongSortedList list, int columnIndex) {
            baseCursor.longTopK(list, columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return baseCursor.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            baseCursor.recordAt(record, atRowId);
        }

        @Override
        public void resumeTimer() {
            baseCursor.resumeTimer();
        }

        @Override
        public void setParentUsedColumns(@Nullable IntHashSet columnIndexes) {
            baseCursor.setParentUsedColumns(columnIndexes);
        }

        @Override
        public void setParquetDecodeHint(ParquetDecodeHint hint) {
            baseCursor.setParquetDecodeHint(hint);
        }

        @Override
        public void setRecordAtRows(@Nullable RowIdSource source) {
            baseCursor.setRecordAtRows(source);
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
        public void suspendTimer() {
            baseCursor.suspendTimer();
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
        }

        private void of(RecordCursor baseCursor) {
            this.baseCursor = baseCursor;
        }
    }
}

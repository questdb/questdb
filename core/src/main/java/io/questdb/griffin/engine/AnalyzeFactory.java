/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


// wraps another factory and records runtime information about it

public class AnalyzeFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final AnalyzeRecordCursor cursor;

    private boolean isBaseClosed;

    public AnalyzeFactory(RecordCursorFactory base) {
        super(base.getMetadata());
        this.base = base;
        this.cursor = new AnalyzeRecordCursor();
    }

    @Override
    public SingleSymbolFilter convertToSampleByIndexPageFrameCursorFactory() {
        return base.convertToSampleByIndexPageFrameCursorFactory();
    }

    @Override
    public PageFrameSequence<?> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return base.execute(executionContext, collectSubSeq, order);
    }

    @Override
    public boolean followedLimitAdvice() {
        return base.followedLimitAdvice();
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    public void formatRowCount(PlanSink sink, long rowCount) {
        sink.val(String.format("%,d", rowCount));
    }

    public void formatTiming(PlanSink sink, long nanos) {
        if (nanos > 1e9) {
            sink.val(roundTiming(nanos / 1e9));
            sink.val("s");
            return;
        }

        if (nanos > 1e6) {
            sink.val(roundTiming(nanos / 1e6));
            sink.val("ms");
            return;
        }

        if (nanos > 1e3) {
            sink.val(roundTiming(nanos / 1e3));
            sink.val("μs");
        } else {
            sink.val(nanos);
            if (nanos != 0) {
                sink.val("ns");
            }
        }
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
        return base.getBaseFactory();
    }

    @Override
    public @Nullable ObjList<Function> getBindVarFunctions() {
        return base.getBindVarFunctions();
    }

    @Override
    public @Nullable MemoryCARW getBindVarMemory() {
        return base.getBindVarMemory();
    }

    @Override
    public @Nullable CompiledFilter getCompiledFilter() {
        return base.getCompiledFilter();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(base, executionContext);
        return cursor;
    }

    @Override
    public @Nullable Function getFilter() {
        return base.getFilter();
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
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
    public TimeFrameRecordCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        return base.getTimeFrameCursor(executionContext);
    }

    @Override
    public void halfClose() {
        base.halfClose();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean recordCursorSupportsLongTopK() {
        return base.recordCursorSupportsLongTopK();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void revertFromSampleByIndexPageFrameCursorFactory() {
        base.revertFromSampleByIndexPageFrameCursorFactory();
    }

    @Override
    public void setBaseFactory(RecordCursorFactory base) {
        base.setBaseFactory(base);
    }

    @Override
    public boolean supportsFilterStealing() {
        return base.supportsFilterStealing();
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
        sink.type("ANALYZE");
        sink.meta("TIME");
        formatTiming(sink, cursor.executionTimeNanos);
        sink.meta("ROWS");
        formatRowCount(sink, cursor.numberOfRecords);
        sink.child(base);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        base.toSink(sink);
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
        if (!isBaseClosed) {
            base.close();
            isBaseClosed = true;
        }
    }

    double roundTiming(double nanos) {
        return (double) Math.round(nanos * 100) / 100;
    }

    public static class AnalyzeRecordCursor implements RecordCursor {
        private RecordCursor baseCursor;
        private long executionTimeNanos;
        private long numberOfRecords;
        private long timeAfterNanos;
        private long timeBeforeNanos;

        public AnalyzeRecordCursor() {
        }

        @Override
        public void close() {
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
        public boolean hasNext() {
            preTime();
            boolean hasNext = baseCursor.hasNext();
            postTime();
            if (hasNext) {
                numberOfRecords++;
            }
            return hasNext;
        }

        public void of(RecordCursorFactory base, SqlExecutionContext executionContext) throws SqlException {
            // open the cursor to ensure bind variable types are initialized
            baseCursor = base.getCursor(executionContext);
            toTop();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            baseCursor.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public void toTop() {
            timeBeforeNanos = 0;
            timeAfterNanos = 0;
            numberOfRecords = 0;
            executionTimeNanos = 0;
        }

        void postTime() {
            timeAfterNanos = Os.currentTimeNanos();
            executionTimeNanos += timeAfterNanos - timeBeforeNanos;
        }

        void preTime() {
            timeBeforeNanos = Os.currentTimeNanos();
        }
    }
}

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.DataFrameCursorFactory.*;

public class AsyncFilteredRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final PageFrameReducer REDUCER = AsyncFilteredRecordCursorFactory::filter;

    private final RecordCursorFactory base;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncFilteredRecordCursor cursor;
    private final PageFrameSequence<AsyncFilterAtom> frameSequence;
    private final Function limitLoFunction;
    private final int limitLoPos;
    private final int maxNegativeLimit;
    private final AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor;
    private final int workerCount;
    private DirectLongList negativeLimitRows;

    public AsyncFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull Function filter,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable ObjList<Function> perWorkerFilters,
            @Nullable Function limitLoFunction,
            int limitLoPos,
            boolean preTouchColumns,
            int workerCount
    ) {
        super(base.getMetadata());
        assert !(base instanceof AsyncFilteredRecordCursorFactory);
        this.base = base;
        this.cursor = new AsyncFilteredRecordCursor(filter, base.getScanDirection());
        this.negativeLimitCursor = new AsyncFilteredNegativeLimitRecordCursor(base.getScanDirection());
        IntList preTouchColumnTypes = null;
        if (preTouchColumns) {
            preTouchColumnTypes = new IntList();
            for (int i = 0, n = base.getMetadata().getColumnCount(); i < n; i++) {
                int columnType = base.getMetadata().getColumnType(i);
                preTouchColumnTypes.add(columnType);
            }
        }
        AsyncFilterAtom atom = new AsyncFilterAtom(configuration, filter, perWorkerFilters, preTouchColumnTypes);
        this.frameSequence = new PageFrameSequence<>(configuration, messageBus, atom, REDUCER, reduceTaskFactory, PageFrameReduceTask.TYPE_FILTER);
        this.limitLoFunction = limitLoFunction;
        this.limitLoPos = limitLoPos;
        this.maxNegativeLimit = configuration.getSqlMaxNegativeLimit();
        this.workerCount = workerCount;
    }

    @Override
    public PageFrameSequence<AsyncFilterAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, order);
    }

    @Override
    public boolean followedLimitAdvice() {
        return limitLoFunction != null;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        long rowsRemaining;
        int baseOrder = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        final int order;
        if (limitLoFunction != null) {
            limitLoFunction.init(frameSequence.getSymbolTableSource(), executionContext);
            rowsRemaining = limitLoFunction.getLong(null);
            // on negative limit we will be looking for positive number of rows
            // while scanning table from the highest timestamp to the lowest
            if (rowsRemaining > -1) {
                order = baseOrder;
            } else {
                order = reverse(baseOrder);
                rowsRemaining = -rowsRemaining;
            }
        } else {
            rowsRemaining = Long.MAX_VALUE;
            order = baseOrder;
        }

        if (order != baseOrder && rowsRemaining != Long.MAX_VALUE) {
            if (rowsRemaining > maxNegativeLimit) {
                throw SqlException.position(limitLoPos).put("absolute LIMIT value is too large, maximum allowed value: ").put(maxNegativeLimit);
            }
            if (negativeLimitRows == null) {
                negativeLimitRows = new DirectLongList(maxNegativeLimit, MemoryTag.NATIVE_OFFLOAD);
            }
            negativeLimitCursor.of(execute(executionContext, collectSubSeq, order), rowsRemaining, negativeLimitRows);
            return negativeLimitCursor;
        }

        cursor.of(execute(executionContext, collectSubSeq, order), rowsRemaining);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Filter");
        sink.meta("workers").val(workerCount);
        // calc order and limit if possible
        long rowsRemaining;
        int baseOrder = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        int order;
        if (limitLoFunction != null) {
            try {
                limitLoFunction.init(frameSequence.getSymbolTableSource(), sink.getExecutionContext());
                rowsRemaining = limitLoFunction.getLong(null);
            } catch (Exception e) {
                rowsRemaining = Long.MAX_VALUE;
            }
            if (rowsRemaining > -1) {
                order = baseOrder;
            } else {
                order = reverse(baseOrder);
                rowsRemaining = -rowsRemaining;
            }
        } else {
            rowsRemaining = Long.MAX_VALUE;
            order = baseOrder;
        }
        if (rowsRemaining != Long.MAX_VALUE) {
            sink.attr("limit").val(rowsRemaining);
        }
        sink.attr("filter").val(frameSequence.getAtom());
        sink.child(base, order);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private static void filter(
            int workerId,
            @NotNull PageAddressCacheRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final DirectLongList rows = task.getFilteredRows();
        final long frameRowCount = task.getFrameRowCount();
        final AsyncFilterAtom atom = task.getFrameSequence(AsyncFilterAtom.class).getAtom();

        rows.clear();

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int filterId = atom.acquireFilter(workerId, owner, circuitBreaker);
        final Function filter = atom.getFilter(filterId);
        try {
            for (long r = 0; r < frameRowCount; r++) {
                record.setRowIndex(r);
                if (filter.getBool(record)) {
                    rows.add(r);
                }
            }
        } finally {
            atom.releaseFilter(filterId);
        }

        // Pre-touch fixed-size columns, if asked.
        atom.preTouchColumns(record, rows);
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(frameSequence);
        Misc.free(negativeLimitRows);
        cursor.freeRecords();
        negativeLimitCursor.freeRecords();
    }
}

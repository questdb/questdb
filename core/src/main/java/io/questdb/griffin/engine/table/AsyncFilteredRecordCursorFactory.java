/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cairo.sql.DataFrameCursorFactory.*;

public class AsyncFilteredRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final PageFrameReducer REDUCER = AsyncFilteredRecordCursorFactory::filter;

    private final RecordCursorFactory base;
    private final AsyncFilteredRecordCursor cursor;
    private final AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor;
    private final FilterAtom filterAtom;
    private final PageFrameSequence<FilterAtom> frameSequence;
    private final SCSequence collectSubSeq = new SCSequence();
    private final Function limitLoFunction;
    private final int limitLoPos;
    private final int maxNegativeLimit;
    private DirectLongList negativeLimitRows;

    public AsyncFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull Function filter,
            @NotNull @Transient WeakClosableObjectPool<PageFrameReduceTask> localTaskPool,
            @Nullable ObjList<Function> perWorkerFilters,
            @Nullable Function limitLoFunction,
            int limitLoPos
    ) {
        super(base.getMetadata());
        assert !(base instanceof AsyncFilteredRecordCursorFactory);
        this.base = base;
        this.cursor = new AsyncFilteredRecordCursor(filter, base.hasDescendingOrder());
        this.negativeLimitCursor = new AsyncFilteredNegativeLimitRecordCursor();
        this.filterAtom = new FilterAtom(filter, perWorkerFilters);
        this.frameSequence = new PageFrameSequence<>(configuration, messageBus, REDUCER, localTaskPool);
        this.limitLoFunction = limitLoFunction;
        this.limitLoPos = limitLoPos;
        this.maxNegativeLimit = configuration.getSqlMaxNegativeLimit();
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(filterAtom);
        Misc.free(frameSequence);
        Misc.free(negativeLimitRows);
        cursor.freeRecords();
        negativeLimitCursor.freeRecords();
    }

    @Override
    public boolean followedLimitAdvice() {
        return limitLoFunction != null;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        long rowsRemaining;
        int baseOrder = base.hasDescendingOrder() ? ORDER_DESC : ORDER_ASC;
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

        if (order == ORDER_DESC && rowsRemaining != Long.MAX_VALUE) {
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
    public PageFrameSequence<FilterAtom> execute(SqlExecutionContext executionContext, Sequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, filterAtom, order);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsUpdateRowId(CharSequence tableName) {
        return base.supportsUpdateRowId(tableName);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean hasDescendingOrder() {
        return base.hasDescendingOrder();
    }

    private static void filter(int workerId, PageAddressCacheRecord record, PageFrameReduceTask task) {
        final DirectLongList rows = task.getRows();
        final long frameRowCount = task.getFrameRowCount();
        final FilterAtom atom = task.getFrameSequence(FilterAtom.class).getAtom();
        final Function filter = atom.getFilter(workerId);

        rows.clear();
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);
            if (filter.getBool(record)) {
                rows.add(r);
            }
        }
    }

    private static class FilterAtom implements StatefulAtom, Closeable {

        private final Function filter;
        private final ObjList<Function> perWorkerFilters;

        public FilterAtom(@NotNull Function filter, @Nullable ObjList<Function> perWorkerFilters) {
            this.filter = filter;
            this.perWorkerFilters = perWorkerFilters;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            filter.init(symbolTableSource, executionContext);
            if (perWorkerFilters != null) {
                final boolean current = executionContext.getCloneSymbolTables();
                executionContext.setCloneSymbolTables(true);
                try {
                    for (int i = 0, n = perWorkerFilters.size(); i < n; i++) {
                        perWorkerFilters.getQuick(i).init(symbolTableSource, executionContext);
                    }
                } finally {
                    executionContext.setCloneSymbolTables(current);
                }
            }
        }

        @Override
        public void close() {
            Misc.free(filter);
            Misc.freeObjList(perWorkerFilters);
        }

        public Function getFilter(int workerId) {
            if (workerId == -1 || perWorkerFilters == null) {
                return filter;
            }
            return perWorkerFilters.getQuick(workerId);
        }
    }
}

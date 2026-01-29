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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.*;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.prepareBindVarMemory;

public class AsyncJitFilteredRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer REDUCER = AsyncJitFilteredRecordCursorFactory::filter;

    private final RecordCursorFactory base;
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final SCSequence collectSubSeq = new SCSequence();
    private final CompiledCountOnlyFilter compiledCountOnlyFilter;
    private final CompiledFilter compiledFilter;
    private final AsyncFilteredRecordCursor cursor;
    private final Function filter;
    private final ExpressionNode filterExpr;
    private final PageFrameSequence<AsyncJitFilterAtom> frameSequence;
    private final Function limitLoFunction;
    private final int limitLoPos;
    private final int maxNegativeLimit;
    private final AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor;
    private final int sharedQueryWorkerCount;
    private DirectLongList negativeLimitRows;

    public AsyncJitFilteredRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull ObjList<Function> bindVarFunctions,
            @NotNull CompiledFilter compiledFilter,
            @NotNull CompiledCountOnlyFilter compiledCountOnlyFilter,
            @NotNull Function filter,
            @NotNull IntHashSet filterUsedColumnIndexes,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable ObjList<Function> perWorkerFilters,
            @NotNull ExpressionNode filterExpr,
            @Nullable Function limitLoFunction,
            int limitLoPos,
            int workerCount,
            boolean enablePreTouch
    ) {
        super(base.getMetadata());
        assert !(base instanceof FilteredRecordCursorFactory);
        assert !(base instanceof AsyncJitFilteredRecordCursorFactory);
        this.base = base;
        this.compiledFilter = compiledFilter;
        this.compiledCountOnlyFilter = compiledCountOnlyFilter;
        this.filter = filter;
        this.filterExpr = filterExpr;
        this.cursor = new AsyncFilteredRecordCursor(configuration, filter, base.getScanDirection());
        this.negativeLimitCursor = new AsyncFilteredNegativeLimitRecordCursor(configuration, base.getScanDirection());
        this.bindVarMemory = Vm.getCARWInstance(
                configuration.getSqlJitBindVarsMemoryPageSize(),
                configuration.getSqlJitBindVarsMemoryMaxPages(),
                MemoryTag.NATIVE_JIT
        );
        this.bindVarFunctions = bindVarFunctions;
        final int columnCount = base.getMetadata().getColumnCount();
        final IntList columnTypes = new IntList(columnCount);
        for (int i = 0; i < columnCount; i++) {
            int columnType = base.getMetadata().getColumnType(i);
            columnTypes.add(columnType);
        }
        final AsyncJitFilterAtom atom = new AsyncJitFilterAtom(
                configuration,
                filter,
                filterUsedColumnIndexes,
                perWorkerFilters,
                compiledFilter,
                compiledCountOnlyFilter,
                bindVarMemory,
                bindVarFunctions,
                columnTypes,
                enablePreTouch
        );
        this.frameSequence = new PageFrameSequence<>(
                engine,
                configuration,
                messageBus,
                atom,
                REDUCER,
                reduceTaskFactory,
                workerCount,
                PageFrameReduceTask.TYPE_FILTER
        );
        this.limitLoFunction = limitLoFunction;
        this.limitLoPos = limitLoPos;
        this.maxNegativeLimit = configuration.getSqlMaxNegativeLimit();
        this.sharedQueryWorkerCount = workerCount;
    }

    @Override
    public void changePageFrameSizes(int minRows, int maxRows) {
        base.changePageFrameSizes(minRows, maxRows);
    }

    @Override
    public PageFrameSequence<AsyncJitFilterAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, order);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    @Override
    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    @Override
    public CompiledFilter getCompiledFilter() {
        return compiledFilter;
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
    public @NotNull Function getFilter() {
        return filter;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public ExpressionNode getStealFilterExpr() {
        return filterExpr;
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    @Override
    public void halfClose() {
        Misc.free(frameSequence);
        Misc.free(compiledCountOnlyFilter);
        cursor.freeRecords();
        negativeLimitCursor.freeRecords();
    }

    @Override
    public boolean implementsLimit() {
        return limitLoFunction != null;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean supportsFilterStealing() {
        return limitLoFunction == null;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async JIT Filter");
        sink.meta("workers").val(sharedQueryWorkerCount);
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
        return true;
    }

    private static void filter(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        final PageFrameSequence<AsyncJitFilterAtom> frameSequence = task.getFrameSequence(AsyncJitFilterAtom.class);
        final AsyncJitFilterAtom atom = frameSequence.getAtom();
        final DirectLongList rows = task.getFilteredRows();
        rows.clear();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int filterId = atom.maybeAcquireFilter(workerId, owner, circuitBreaker);

        try {
            final boolean isParquetFrame = task.isParquetFrame();
            final boolean useLateMaterialization = atom.shouldUseLateMaterialization(filterId, isParquetFrame, task.isCountOnly());

            final PageFrameMemory frameMemory;
            if (useLateMaterialization) {
                frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
            } else {
                frameMemory = task.populateFrameMemory();
            }
            record.init(frameMemory);

            if (frameMemory.hasColumnTops()) {
                // Use Java-based filter in case of a page frame with column tops.
                final Function filter = atom.getFilter(filterId);

                if (task.isCountOnly()) {
                    long count = 0;
                    for (long r = 0; r < frameRowCount; r++) {
                        record.setRowIndex(r);
                        if (filter.getBool(record)) {
                            count++;
                        }
                    }
                    task.setFilteredRowCount(count);
                } else { // normal filter task
                    for (long r = 0; r < frameRowCount; r++) {
                        record.setRowIndex(r);
                        if (filter.getBool(record)) {
                            rows.add(r);
                        }
                    }

                    if (isParquetFrame) {
                        atom.getSelectivityStats(filterId).update(rows.size(), frameRowCount);
                    }
                    if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                        record.init(frameMemory);
                    }
                    task.setFilteredRowCount(rows.size());
                }
                return;
            }

            // Use JIT-compiled filter.
            task.populateJitData();
            final DirectLongList dataAddresses = task.getDataAddresses();
            final DirectLongList auxAddresses = task.getAuxAddresses();

            if (task.isCountOnly()) {
                final long filteredRowCount = atom.compiledCountOnlyFilter.call(
                        dataAddresses.getAddress(),
                        dataAddresses.size(),
                        auxAddresses.getAddress(),
                        atom.bindVarMemory.getAddress(),
                        atom.bindVarFunctions.size(),
                        frameRowCount
                );
                task.setFilteredRowCount(filteredRowCount);
            } else { // normal filter task
                final long filteredRowCount = atom.compiledFilter.call(
                        dataAddresses.getAddress(),
                        dataAddresses.size(),
                        auxAddresses.getAddress(),
                        atom.bindVarMemory.getAddress(),
                        atom.bindVarFunctions.size(),
                        rows.getAddress(),
                        frameRowCount
                );

                rows.setPos(filteredRowCount);
                if (isParquetFrame) {
                    atom.getSelectivityStats(filterId).update(filteredRowCount, frameRowCount);
                }
                if (useLateMaterialization && task.populateRemainingColumns(atom.getFilterUsedColumnIndexes(), rows, true)) {
                    record.init(frameMemory);
                }

                task.setFilteredRowCount(rows.size());

                // Pre-touch native columns, if asked.
                if (frameMemory.getFrameFormat() == PartitionFormat.NATIVE) {
                    atom.preTouchColumns(record, rows, frameRowCount);
                }
            }
        } finally {
            atom.releaseFilter(filterId);
        }
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(negativeLimitRows);
        halfClose();
        Misc.free(compiledFilter);
        Misc.free(filter);
        Misc.free(bindVarMemory);
        Misc.freeObjList(bindVarFunctions);
    }

    public static class AsyncJitFilterAtom extends AsyncFilterAtom {
        final ObjList<Function> bindVarFunctions;
        final MemoryCARW bindVarMemory;
        final CompiledCountOnlyFilter compiledCountOnlyFilter;
        final CompiledFilter compiledFilter;

        public AsyncJitFilterAtom(
                CairoConfiguration configuration,
                Function filter,
                IntHashSet filterUsedColumnIndexes,
                ObjList<Function> perWorkerFilters,
                CompiledFilter compiledFilter,
                CompiledCountOnlyFilter compiledCountOnlyFilter,
                MemoryCARW bindVarMemory,
                ObjList<Function> bindVarFunctions,
                IntList columnTypes,
                boolean enablePreTouch
        ) {
            super(configuration, filter, filterUsedColumnIndexes, perWorkerFilters, columnTypes, enablePreTouch);
            this.compiledFilter = compiledFilter;
            this.compiledCountOnlyFilter = compiledCountOnlyFilter;
            this.bindVarMemory = bindVarMemory;
            this.bindVarFunctions = bindVarFunctions;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
            prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }
    }
}

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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitBounds;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
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
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.*;

public class AsyncJitFilteredRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer REDUCER = AsyncJitFilteredRecordCursorFactory::filter;

    private final SCSequence collectSubSeq = new SCSequence();
    private final int maxNegativeLimit;
    private final int sharedQueryWorkerCount;
    private RecordCursorFactory base;
    private AsyncFilteredRecordCursor cursor;
    private PageFrameSequence<AsyncJitFilterAtom> frameSequence;
    private LimitRecordCursorFactory.LimitRecordCursor lastRowsCursor;
    private LimitBounds limitBounds;
    private AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor;
    private DirectLongList negativeLimitRows;

    public AsyncJitFilteredRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull AsyncFilterResources filterResources,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable Function limitLoFunction,
            int limitLoPos,
            int workerCount,
            boolean enablePreTouch
    ) {
        super(base.getMetadata());
        assert !(base instanceof FilteredRecordCursorFactory);
        assert !(base instanceof AsyncJitFilteredRecordCursorFactory);
        this.base = base;
        final CompiledFilter compiledFilter = filterResources.takeCompiledFilter();
        final CompiledCountOnlyFilter compiledCountOnlyFilter = filterResources.takeCompiledCountOnlyFilter();
        final Function filter = filterResources.takeFilter();
        final ObjList<Function> bindVarFunctions = filterResources.takeBindVarFunctions();
        final IntHashSet filterUsedColumnIndexes = filterResources.getFilterUsedColumnIndexes();
        final ObjList<Function> perWorkerFilters = filterResources.takePerWorkerFilters();
        // A throw part-way through this constructor never returns the factory, so _close() never runs
        // and everything allocated up to that point is unreachable: the bind variable memory is
        // native, and a per-worker filter can hold native memory of its own. Build the rest into
        // locals and release them here, together with the inputs this constructor consumes.
        //
        // The atom owns the compiled filters, the bind variables, the filter and its per-worker clones
        // once its constructor returns. The atom belongs to the frame sequence from the moment the
        // PageFrameSequence constructor is entered: that constructor closes the atom on its own
        // failure path, and close() closes it afterwards. Nothing that can throw sits between the two
        // calls, so isFilterOwned covers the whole gap and every object below is closed exactly once
        // on every path.
        MemoryCARW bindVarMemory = null;
        AsyncFilteredRecordCursor cursor = null;
        AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor = null;
        PageFrameSequence<AsyncJitFilterAtom> frameSequence = null;
        final int maxNegativeLimit;
        boolean isFilterOwned = true;
        try {
            cursor = new AsyncFilteredRecordCursor(configuration, filter, base.getScanDirection());
            negativeLimitCursor = new AsyncFilteredNegativeLimitRecordCursor(configuration, base.getScanDirection());
            bindVarMemory = Vm.getCARWInstance(
                    configuration.getSqlJitBindVarsMemoryPageSize(),
                    configuration.getSqlJitBindVarsMemoryMaxPages(),
                    MemoryTag.NATIVE_JIT
            );
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
            isFilterOwned = false;
            frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    REDUCER,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_FILTER
            );
            maxNegativeLimit = configuration.getSqlMaxNegativeLimit();
        } catch (Throwable th) {
            Misc.free(frameSequence, th);
            if (isFilterOwned) {
                Misc.freeObjList(perWorkerFilters, th);
                Misc.free(bindVarMemory, th);
                Misc.free(compiledCountOnlyFilter, th);
                Misc.free(compiledFilter, th);
                Misc.free(filter, th);
                Misc.freeObjList(bindVarFunctions, th);
            }
            // The cursors are not open yet, and close() frees their records only once they are, so
            // release the records directly.
            freeRecordsBestEffort(th, cursor);
            freeRecordsBestEffort(th, negativeLimitCursor);
            Misc.free(limitLoFunction, th);
            Misc.free(filterResources, th);
            Misc.free(base, th);
            throw th;
        }
        this.cursor = cursor;
        this.negativeLimitCursor = negativeLimitCursor;
        this.frameSequence = frameSequence;
        this.limitBounds = limitLoFunction != null ? new LimitBounds(limitLoFunction, null, limitLoPos) : null;
        this.maxNegativeLimit = maxNegativeLimit;
        this.sharedQueryWorkerCount = workerCount;
        if (limitLoFunction instanceof MasterSuffixLimitFunction masterSuffixLimit) {
            masterSuffixLimit.of(negativeLimitCursor);
        }
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
    @TestOnly
    public AsyncJitFilterAtom getAtom() {
        return frameSequence.getAtom();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // Consult the breaker at open, so a scan over an empty table still observes cancellation.
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        long rowsRemaining = Long.MAX_VALUE;
        final int baseOrder = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        int order = baseOrder;
        if (limitBounds != null) {
            limitBounds.init(frameSequence.getSymbolTableSource(), executionContext);
            final long limit = limitBounds.getValues().getLimit();
            if (limit > -1) {
                rowsRemaining = limit;
                if (!limitBounds.getValues().isFirstN()) {
                    // on negative limit we will be looking for positive number of rows
                    // while scanning table from the highest timestamp to the lowest
                    order = reverse(baseOrder);
                }
            }
        }

        if (order != baseOrder && rowsRemaining != Long.MAX_VALUE) {
            if (rowsRemaining > maxNegativeLimit) {
                // The buffer cannot hold the last rows the limit selects, so read all rows and skip to the last ones.
                cursor.of(execute(executionContext, collectSubSeq, baseOrder), Long.MAX_VALUE);
                if (lastRowsCursor == null) {
                    lastRowsCursor = new LimitRecordCursorFactory.LimitRecordCursor(limitBounds, null);
                }
                try {
                    lastRowsCursor.ofResolvedBounds(cursor, executionContext);
                } catch (Throwable th) {
                    cursor.close();
                    throw th;
                }
                return lastRowsCursor;
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
        return frameSequence.getAtom().getFilter(-1);
    }

    // Stable iff the retained filter and the base are stable.
    @Override
    public boolean isNonDeterministic() {
        return getFilter().isNonDeterministic() || base.isNonDeterministic();
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
    public boolean implementsLimit() {
        return limitBounds != null;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
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
        long rowsRemaining = Long.MAX_VALUE;
        final int baseOrder = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        int order = baseOrder;
        if (limitBounds != null) {
            long limit;
            try {
                limitBounds.init(frameSequence.getSymbolTableSource(), sink.getExecutionContext());
                limit = limitBounds.getValues().getLimit();
            } catch (Exception e) {
                limit = -1;
            }
            if (limit > -1) {
                if (limitBounds.getValues().isFirstN()) {
                    rowsRemaining = limit;
                } else if (limit <= maxNegativeLimit) {
                    order = reverse(baseOrder);
                    rowsRemaining = limit;
                } else {
                    rowsRemaining = limitBounds.getValues().getLo();
                }
            }
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

            if (frameMemory.hasColumnTops() || frameMemory.hasColumnTypeCasts()) {
                // Use Java-based filter in case of a page frame with column tops
                // or type-cast columns (fixed→var conversion not supported in JIT).
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
                    if (useLateMaterialization && task.populateRemainingColumns(atom.getLateMaterializationSkipColumnIndexes(), rows, true)) {
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
                        atom.getBindVarMemory().getAddress(),
                        atom.getBindVarFunctions().size(),
                        frameRowCount
                );
                task.setFilteredRowCount(filteredRowCount);
            } else { // normal filter task
                final long filteredRowCount = atom.getCompiledFilter().call(
                        dataAddresses.getAddress(),
                        dataAddresses.size(),
                        auxAddresses.getAddress(),
                        atom.getBindVarMemory().getAddress(),
                        atom.getBindVarFunctions().size(),
                        rows.getAddress(),
                        frameRowCount
                );

                rows.setPos(filteredRowCount);
                if (isParquetFrame) {
                    atom.getSelectivityStats(filterId).update(filteredRowCount, frameRowCount);
                }
                if (useLateMaterialization && task.populateRemainingColumns(atom.getLateMaterializationSkipColumnIndexes(), rows, true)) {
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
        final RecordCursorFactory base = this.base;
        this.base = null;
        final AsyncFilteredRecordCursor cursor = this.cursor;
        this.cursor = null;
        final PageFrameSequence<AsyncJitFilterAtom> frameSequence = this.frameSequence;
        this.frameSequence = null;
        final AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor = this.negativeLimitCursor;
        this.negativeLimitCursor = null;
        final DirectLongList negativeLimitRows = this.negativeLimitRows;
        this.negativeLimitRows = null;
        // The generator hands the LIMIT advice function over on construction and keeps no
        // reference, so the bounds of this factory are its only owner.
        final LimitBounds limitBounds = this.limitBounds;
        this.limitBounds = null;

        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, negativeLimitRows);
        failure = Misc.freeBestEffort(failure, frameSequence);
        failure = freeRecordsBestEffort(failure, cursor);
        failure = freeRecordsBestEffort(failure, negativeLimitCursor);
        failure = Misc.freeBestEffort(failure, limitBounds);
        CairoException.rethrowCleanupFailure(failure);
    }

    private static Throwable freeRecordsBestEffort(
            Throwable failure,
            AsyncFilteredRecordCursorFactory.RecordFreer recordFreer
    ) {
        if (recordFreer != null) {
            try {
                recordFreer.freeRecords();
            } catch (Throwable th) {
                if (failure == null) {
                    return th;
                }
                if (failure != th) {
                    failure.addSuppressed(th);
                }
            }
        }
        return failure;
    }

    public static class AsyncJitFilterAtom extends AsyncFilterAtom {
        final CompiledCountOnlyFilter compiledCountOnlyFilter;

        /**
         * Takes the filter resources, the count-only filter included, which {@link #close()} frees; frees none of
         * them when it throws.
         */
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
            super(
                    configuration,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    filter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    columnTypes,
                    enablePreTouch
            );
            this.compiledCountOnlyFilter = compiledCountOnlyFilter;
        }

        @Override
        public Throwable close(@Nullable Throwable cleanupFailure) {
            return super.close(Misc.freeBestEffort(cleanupFailure, compiledCountOnlyFilter));
        }
    }
}

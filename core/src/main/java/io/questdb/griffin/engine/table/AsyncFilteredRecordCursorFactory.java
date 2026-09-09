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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.*;

public class AsyncFilteredRecordCursorFactory extends AbstractRecordCursorFactory {
    @TestOnly
    private static volatile Runnable constructorFailureHookForTesting;
    private static final PageFrameReducer REDUCER = AsyncFilteredRecordCursorFactory::filter;
    private RecordCursorFactory base;
    private final SCSequence collectSubSeq = new SCSequence();
    private AsyncFilteredRecordCursor cursor;
    private Function filter;
    private final ExpressionNode filterExpr;
    private PageFrameSequence<AsyncFilterAtom> frameSequence;
    private Function limitLoFunction;
    private final int limitLoPos;
    private final int maxNegativeLimit;
    private AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor;
    private final int workerCount;
    private DirectLongList negativeLimitRows;

    public AsyncFilteredRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
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
        final Runnable constructorFailureHook = constructorFailureHookForTesting;
        if (constructorFailureHook != null) {
            constructorFailureHook.run();
        }
        assert !(base instanceof AsyncFilteredRecordCursorFactory);
        this.base = base;
        this.filter = filter;
        this.filterExpr = filterExpr;
        // A throw part-way through this constructor never returns the factory, so _close() never runs
        // and everything allocated up to that point is unreachable: the cursors hold native records
        // and page frame memory, and a per-worker filter can hold native memory of its own. The
        // caller frees what it passed in (the filter and the base factory), so build the rest into
        // locals and release them here.
        //
        // The caller retains the per-worker filter list until this constructor returns. Once the atom
        // takes the filters, its failure paths close them and null the list slots, so the caller can
        // safely close any remaining entries. The atom belongs to the frame sequence from the moment
        // the PageFrameSequence constructor is entered: that constructor closes the atom on its own
        // failure path, and close() closes it afterwards. Nothing that can throw sits between the two
        // calls, so isPerWorkerFiltersOwned covers the whole gap and every object below is closed
        // exactly once on every path.
        AsyncFilteredRecordCursor cursor = null;
        AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor = null;
        PageFrameSequence<AsyncFilterAtom> frameSequence = null;
        final int maxNegativeLimit;
        boolean isPerWorkerFiltersOwned = true;
        try {
            cursor = new AsyncFilteredRecordCursor(configuration, filter, base.getScanDirection());
            negativeLimitCursor = new AsyncFilteredNegativeLimitRecordCursor(configuration, base.getScanDirection());
            final int columnCount = base.getMetadata().getColumnCount();
            final IntList columnTypes = new IntList(columnCount);
            for (int i = 0; i < columnCount; i++) {
                int columnType = base.getMetadata().getColumnType(i);
                columnTypes.add(columnType);
            }
            final AsyncFilterAtom atom = new AsyncFilterAtom(
                    configuration,
                    filter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    columnTypes,
                    enablePreTouch
            );
            isPerWorkerFiltersOwned = false;
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
            if (isPerWorkerFiltersOwned) {
                Misc.freeObjList(perWorkerFilters, th);
            }
            // The cursors are not open yet, and close() frees their records only once they are, so
            // release the records directly - the same call halfClose() makes on the open factory.
            halfCloseBestEffort(th, frameSequence, cursor, negativeLimitCursor);
            throw th;
        }
        this.cursor = cursor;
        this.negativeLimitCursor = negativeLimitCursor;
        this.frameSequence = frameSequence;
        this.limitLoPos = limitLoPos;
        this.maxNegativeLimit = maxNegativeLimit;
        // Assigned last: _close() frees this field, so it must not be set before a statement that
        // can still throw, or the caller's own free would become a double free.
        this.limitLoFunction = limitLoFunction;
        this.workerCount = workerCount;
    }

    @Override
    public void changePageFrameSizes(int minRows, int maxRows) {
        base.changePageFrameSizes(minRows, maxRows);
    }

    @Override
    public PageFrameSequence<AsyncFilterAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, order);
    }

    @Override
    @TestOnly
    public AsyncFilterAtom getAtom() {
        return frameSequence.getAtom();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // Consult the breaker at open, so a scan over an empty table still observes cancellation.
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottled();
        long rowsRemaining;
        int baseOrder = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        final int order;
        if (limitLoFunction != null) {
            limitLoFunction.init(frameSequence.getSymbolTableSource(), executionContext);
            rowsRemaining = limitLoFunction.getLong(null);
            // A NULL limit means "no limit", matching the unfiltered path (an unset LIMIT :lim
            // bind variable reaches here as NULL). Numbers.LONG_NULL is Long.MIN_VALUE, so it has
            // to be recognised before the sign flip below: negating it overflows back to a
            // negative value that then trips the max-negative-limit guard, turning a working
            // query into an error as soon as a WHERE clause is added.
            if (rowsRemaining == Numbers.LONG_NULL) {
                rowsRemaining = Long.MAX_VALUE;
                order = baseOrder;
            } else if (rowsRemaining > -1) {
                // on negative limit we will be looking for positive number of rows
                // while scanning table from the highest timestamp to the lowest
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
            // A negative limit is negated above; -Long.MIN_VALUE overflows back to a negative value,
            // so reject rowsRemaining < 0 too instead of letting it slip past the maxNegativeLimit
            // bound and produce an empty cursor.
            if (rowsRemaining < 0 || rowsRemaining > maxNegativeLimit) {
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

    // Stable iff the retained filter and the base are stable.
    @Override
    public boolean isNonDeterministic() {
        return filter.isNonDeterministic() || base.isNonDeterministic();
    }

    @Override
    public boolean isStableWithinExecution() {
        return filter.isStableWithinExecution() && base.isStableWithinExecution();
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
        halfClose(frameSequence, cursor, negativeLimitCursor);
    }

    @Override
    public boolean implementsLimit() {
        return limitLoFunction != null;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // The async-filtered cursor services recordAt()/getRecordB() through its own
        // page-frame memory pool (PageFrameMemoryRecord), independent of the base's
        // record-cursor random-access capability. It therefore always supports random
        // access. Delegating to base.recordCursorSupportsRandomAccess() was only ever
        // correct because every page-frame base reported true; a base whose record
        // cursor reports false (e.g. CoveringIndex, whose row cursor throws on recordAt)
        // made this factory wrongly report false while its cursor still serviced
        // recordAt(), violating the cursor random-access contract.
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
            // A NULL limit means "no limit", exactly as getCursor() treats it. Recognise it before the
            // sign flip: negating Numbers.LONG_NULL (Long.MIN_VALUE) overflows back to itself, which
            // would print a bogus "limit: null" line and reverse the scan direction the plan shows.
            if (rowsRemaining == Numbers.LONG_NULL) {
                rowsRemaining = Long.MAX_VALUE;
                order = baseOrder;
            } else if (rowsRemaining > -1) {
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

    /**
     * Test-only entry point for exercising half-close failure handling without exposing concrete cursors.
     */
    @TestOnly
    public static void halfCloseForTesting(
            Closeable frameSequence,
            RecordFreer cursor,
            RecordFreer negativeLimitCursor
    ) {
        halfClose(frameSequence, cursor, negativeLimitCursor);
    }

    @TestOnly
    public static void setConstructorFailureHookForTesting(@Nullable Runnable hook) {
        constructorFailureHookForTesting = hook;
    }

    private static void filter(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        final AsyncFilterAtom atom = task.getFrameSequence(AsyncFilterAtom.class).getAtom();

        final boolean isParquetFrame = task.isParquetFrame();
        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
        final int filterId = atom.maybeAcquireFilter(workerId, owner, circuitBreaker);
        // The slot is held from here on, so everything below belongs inside the try that releases
        // it: populateFrameMemory() navigates to the frame, which decodes parquet and can breach the
        // per-query memory limit. See PerWorkerLocks.acquireSlot().
        try {
            final boolean useLateMaterialization = atom.shouldUseLateMaterialization(filterId, isParquetFrame, task.isCountOnly());

            final PageFrameMemory frameMemory;
            if (useLateMaterialization) {
                frameMemory = task.populateFrameMemory(atom.getFilterUsedColumnIndexes());
            } else {
                frameMemory = task.populateFrameMemory();
            }
            record.init(frameMemory);

            final DirectLongList rows = task.getFilteredRows();
            rows.clear();

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

                // Pre-touch native columns, if asked.
                if (frameMemory.getFrameFormat() == PartitionFormat.NATIVE) {
                    atom.preTouchColumns(record, rows, frameRowCount);
                }
            }
        } finally {
            atom.releaseFilter(filterId);
        }
    }

    private static void halfClose(
            Closeable frameSequence,
            RecordFreer cursor,
            RecordFreer negativeLimitCursor
    ) {
        CairoException.rethrowCleanupFailure(halfCloseBestEffort(null, frameSequence, cursor, negativeLimitCursor));
    }

    private static Throwable halfCloseBestEffort(
            Throwable cleanupFailure,
            @Nullable Closeable frameSequence,
            @Nullable RecordFreer cursor,
            @Nullable RecordFreer negativeLimitCursor
    ) {
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, frameSequence);
        if (cursor != null) {
            try {
                cursor.freeRecords();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    cleanupFailure = th;
                } else if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        if (negativeLimitCursor != null) {
            try {
                negativeLimitCursor.freeRecords();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    cleanupFailure = th;
                } else if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        return cleanupFailure;
    }

    /**
     * Test-only abstraction for observable record cleanup in {@link #halfCloseForTesting}.
     */
    @FunctionalInterface
    @TestOnly
    public interface RecordFreer {
        void freeRecords();
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final AsyncFilteredRecordCursor cursor = this.cursor;
        this.cursor = null;
        final Function filter = this.filter;
        this.filter = null;
        final PageFrameSequence<AsyncFilterAtom> frameSequence = this.frameSequence;
        this.frameSequence = null;
        final AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor = this.negativeLimitCursor;
        this.negativeLimitCursor = null;
        final DirectLongList negativeLimitRows = this.negativeLimitRows;
        this.negativeLimitRows = null;
        // The generator hands the LIMIT advice function over on construction and keeps no
        // reference, so this factory is its only owner. Nothing freed it before, which leaked
        // any LIMIT bound holding native memory on every successful compile.
        final Function limitLoFunction = this.limitLoFunction;
        this.limitLoFunction = null;

        Throwable cleanupFailure = Misc.freeBestEffort(null, base);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, negativeLimitRows);
        cleanupFailure = halfCloseBestEffort(cleanupFailure, frameSequence, cursor, negativeLimitCursor);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, filter);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, limitLoFunction);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }
}

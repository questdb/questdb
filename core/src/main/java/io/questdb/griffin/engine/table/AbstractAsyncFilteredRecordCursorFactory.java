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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitBounds;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.mp.SCSequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.*;

/**
 * Filters the page frames of its base in parallel, through the atom and the reducer of the subclass, and applies a
 * LIMIT of one count: the first rows in scan order, or the last rows, which it reads in reverse scan order up to
 * {@link CairoConfiguration#getSqlMaxNegativeLimit()} of them and skips to after reading every row otherwise. Owns
 * the base, the frame sequence and its atom, the cursors and the LIMIT function.
 */
public abstract class AbstractAsyncFilteredRecordCursorFactory<T extends AsyncFilterAtom> extends AbstractRecordCursorFactory {
    private final SCSequence collectSubSeq = new SCSequence();
    private final int maxNegativeLimit;
    private final String planType;
    private final int scanOrder;
    private final int workerCount;
    private RecordCursorFactory base;
    private AsyncFilteredRecordCursor cursor;
    private PageFrameSequence<T> frameSequence;
    private LimitRecordCursorFactory.LimitRecordCursor lastRowsCursor;
    private LimitBounds limitBounds;
    private int limitOrder;
    private long limitRowCount;
    private AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor;
    private DirectLongList negativeLimitRows;

    /**
     * Takes the base, the filter resources, which pass to the atom {@code atomFactory} builds, and the LIMIT function,
     * and frees all of them when it throws. {@code planType}: the type the plan shows.
     */
    protected AbstractAsyncFilteredRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull AsyncFilterResources filterResources,
            @NotNull AtomFactory<T> atomFactory,
            @NotNull PageFrameReducer reducer,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable Function limitLoFunction,
            int limitLoPos,
            int workerCount,
            boolean enablePreTouch,
            @NotNull String planType
    ) {
        super(base.getMetadata());
        this.base = base;
        AsyncFilteredRecordCursor cursor = null;
        AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor = null;
        PageFrameSequence<T> frameSequence = null;
        final int maxNegativeLimit;
        try {
            cursor = new AsyncFilteredRecordCursor(configuration, filterResources.getFilter(), base.getScanDirection());
            negativeLimitCursor = new AsyncFilteredNegativeLimitRecordCursor(configuration, base.getScanDirection());
            final int columnCount = base.getMetadata().getColumnCount();
            final IntList columnTypes = new IntList(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columnTypes.add(base.getMetadata().getColumnType(i));
            }
            final T atom = atomFactory.newAtom(configuration, filterResources, columnTypes, enablePreTouch);
            frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    reducer,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_FILTER
            );
            maxNegativeLimit = configuration.getSqlMaxNegativeLimit();
        } catch (Throwable th) {
            Misc.free(filterResources, th);
            freeExecutionStateBestEffort(th, frameSequence, cursor, negativeLimitCursor);
            Misc.free(limitLoFunction, th);
            Misc.free(base, th);
            throw th;
        }
        this.cursor = cursor;
        this.negativeLimitCursor = negativeLimitCursor;
        this.frameSequence = frameSequence;
        this.maxNegativeLimit = maxNegativeLimit;
        this.limitBounds = limitLoFunction != null ? new LimitBounds(limitLoFunction, null, limitLoPos) : null;
        this.planType = planType;
        this.scanOrder = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        this.workerCount = workerCount;
        if (limitLoFunction instanceof MasterSuffixLimitFunction masterSuffixLimit) {
            masterSuffixLimit.getRows().of(negativeLimitCursor);
        }
    }

    /**
     * Test-only entry point for exercising execution-state cleanup failure handling without exposing concrete cursors.
     */
    @TestOnly
    public static void freeExecutionStateForTesting(
            Closeable frameSequence,
            RecordFreer cursor,
            RecordFreer negativeLimitCursor
    ) {
        CairoException.rethrowCleanupFailure(freeExecutionStateBestEffort(null, frameSequence, cursor, negativeLimitCursor));
    }

    @Override
    public void changePageFrameSizes(int minRows, int maxRows) {
        base.changePageFrameSizes(minRows, maxRows);
    }

    @Override
    public PageFrameSequence<T> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, order);
    }

    @Override
    @TestOnly
    public T getAtom() {
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
        resolveLimit(executionContext);
        if (limitRowCount < 0) {
            cursor.of(execute(executionContext, collectSubSeq, limitOrder), Long.MAX_VALUE);
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
        if (limitOrder != scanOrder) {
            if (negativeLimitRows == null) {
                negativeLimitRows = new DirectLongList(maxNegativeLimit, MemoryTag.NATIVE_OFFLOAD);
            }
            negativeLimitCursor.of(execute(executionContext, collectSubSeq, limitOrder), limitRowCount, negativeLimitRows);
            return negativeLimitCursor;
        }
        cursor.of(execute(executionContext, collectSubSeq, limitOrder), limitRowCount);
        return cursor;
    }

    @Override
    public @NotNull Function getFilter() {
        return frameSequence.getAtom().getFilter(-1);
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

    // Stable iff the retained filter and the base are stable.
    @Override
    public boolean isNonDeterministic() {
        return getFilter().isNonDeterministic() || base.isNonDeterministic();
    }

    /**
     * Supports random access whatever the base supports: the cursors serve {@code recordAt()} and
     * {@code getRecordB()} through page frame memory of their own.
     */
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
        sink.type(planType);
        sink.meta("workers").val(workerCount);
        try {
            resolveLimit(sink.getExecutionContext());
        } catch (Exception ignore) {
        }
        if (limitRowCount != Long.MAX_VALUE) {
            sink.attr("limit").val(limitRowCount);
        }
        sink.attr("filter").val(frameSequence.getAtom());
        sink.child(base, limitOrder);
    }

    private static Throwable freeExecutionStateBestEffort(
            Throwable cleanupFailure,
            @Nullable Closeable frameSequence,
            @Nullable RecordFreer cursor,
            @Nullable RecordFreer negativeLimitCursor
    ) {
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, frameSequence);
        cleanupFailure = freeRecordsBestEffort(cleanupFailure, cursor);
        return freeRecordsBestEffort(cleanupFailure, negativeLimitCursor);
    }

    private static Throwable freeRecordsBestEffort(Throwable cleanupFailure, @Nullable RecordFreer recordFreer) {
        if (recordFreer != null) {
            try {
                recordFreer.freeRecords();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    return th;
                }
                if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        return cleanupFailure;
    }

    /**
     * Resolves the LIMIT into the order the frame sequence dispatches the frames in, {@link #limitOrder}, and the
     * rows the filter returns, {@link #limitRowCount}: every row in scan order without a LIMIT, the first rows in
     * scan order, the last rows in reverse scan order where the negative LIMIT buffer holds them, and otherwise every
     * row in scan order with the negative count of the last rows, which the last rows cursor skips to. Leaves every
     * row in scan order where the LIMIT functions throw.
     */
    private void resolveLimit(SqlExecutionContext executionContext) throws SqlException {
        limitOrder = scanOrder;
        limitRowCount = Long.MAX_VALUE;
        if (limitBounds != null) {
            limitBounds.init(frameSequence.getSymbolTableSource(), executionContext);
            final LimitBounds.Values values = limitBounds.getValues();
            final long lastRowCount = values.getLastRowCount();
            if (lastRowCount > maxNegativeLimit) {
                limitRowCount = values.getLo();
            } else if (lastRowCount > -1) {
                limitOrder = reverse(scanOrder);
                limitRowCount = lastRowCount;
            } else if (values.isFirstN()) {
                limitRowCount = values.getLimit();
            }
        }
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final AsyncFilteredRecordCursor cursor = this.cursor;
        this.cursor = null;
        final PageFrameSequence<T> frameSequence = this.frameSequence;
        this.frameSequence = null;
        final AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor = this.negativeLimitCursor;
        this.negativeLimitCursor = null;
        final DirectLongList negativeLimitRows = this.negativeLimitRows;
        this.negativeLimitRows = null;
        // The generator hands the LIMIT advice function over on construction and keeps no
        // reference, so the bounds of this factory are its only owner.
        final LimitBounds limitBounds = this.limitBounds;
        this.limitBounds = null;

        Throwable cleanupFailure = Misc.freeBestEffort(null, base);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, negativeLimitRows);
        cleanupFailure = freeExecutionStateBestEffort(cleanupFailure, frameSequence, cursor, negativeLimitCursor);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, limitBounds);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    /**
     * Builds the atom of the factory over the filter of the resources.
     */
    @FunctionalInterface
    protected interface AtomFactory<T extends AsyncFilterAtom> {
        /**
         * Takes from {@code filterResources} the resources the atom owns, and frees the ones it took when it throws.
         */
        T newAtom(CairoConfiguration configuration, AsyncFilterResources filterResources, IntList columnTypes, boolean enablePreTouch);
    }

    /**
     * Test-only abstraction for observable record cleanup in {@link #freeExecutionStateForTesting}.
     */
    @FunctionalInterface
    @TestOnly
    public interface RecordFreer {
        void freeRecords();
    }
}

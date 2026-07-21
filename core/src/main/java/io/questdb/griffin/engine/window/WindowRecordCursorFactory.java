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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.lv.LiveViewCheckpointRangePlan;
import io.questdb.cairo.lv.LiveViewCheckpointRowsPlan;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.AbstractVirtualFunctionRecordCursor;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Factory implements select with window functions that support streaming, that is:
 * - they don't specify order by or order by is the same as underlying query
 * - all functions and their framing clause do support stream-ed processing (single pass)
 */
public class WindowRecordCursorFactory extends AbstractRecordCursorFactory {
    // Subset of windowFunctions whose frame is UNBOUNDED PRECEDING ... CURRENT ROW.
    // For a live view these are exactly the functions that belong to an anchored
    // named WINDOW (a bare unbounded window is rejected at CREATE, and at most one
    // anchored window is allowed), so the live-view ANCHOR runtime resets only
    // these and never touches a bounded ROWS/RANGE window's state. Null for
    // non-live-view compiles, which never consult it.
    private final ObjList<WindowFunction> anchorableWindowFunctions;
    // Union of the finite RANGE dependencies every window function in this factory carries,
    // or null when the factory mixes shapes, is not a live-view compile, or has no window
    // function with a finite RANGE frame. Bounds the localized O3 repair interval.
    private final LiveViewCheckpointRangePlan checkpointRangePlan;
    // The same for the finite ROWS dependencies, plus the key projector the repair counts
    // per-key rows through. At most one of the two plans is ever non-null: a factory whose
    // functions are all finite RANGE has no finite ROWS function and vice versa.
    private final LiveViewCheckpointRowsPlan checkpointRowsPlan;
    private final ObjList<WindowFunction> windowFunctions;
    private final int windowFunctionsCount;
    private RecordCursorFactory base;
    private boolean closed = false;
    private WindowRecordCursor cursor;
    private ObjList<Function> functions;

    public WindowRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions
    ) {
        this(base, metadata, functions, null, null, null);
    }

    public WindowRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions,
            ObjList<WindowFunction> anchorableWindowFunctions
    ) {
        this(base, metadata, functions, anchorableWindowFunctions, null, null);
    }

    public WindowRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions,
            ObjList<WindowFunction> anchorableWindowFunctions,
            LiveViewCheckpointRangePlan checkpointRangePlan,
            LiveViewCheckpointRowsPlan checkpointRowsPlan
    ) {
        super(metadata);
        this.base = base;
        this.functions = functions;
        this.anchorableWindowFunctions = anchorableWindowFunctions;
        this.checkpointRangePlan = checkpointRangePlan;
        this.checkpointRowsPlan = checkpointRowsPlan;

        windowFunctions = new ObjList<>();
        for (int i = 0, n = functions.size(); i < n; i++) {
            Function func = functions.getQuick(i);
            if (func instanceof WindowFunction) {
                windowFunctions.add((WindowFunction) func);
            }
        }
        windowFunctionsCount = windowFunctions.size();

        // random access is not supported because window function value depends on the window/frame
        // context and can't be computed from single row alone, e.g. even though we might be able
        // to skip to a rowId, we'd still need to compute values for all the rows in between
        this.cursor = new WindowRecordCursor(functions, false);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    /**
     * Returns the subset of window functions whose frame is UNBOUNDED PRECEDING ...
     * CURRENT ROW — the functions a live view's ANCHOR clause resets. Returns
     * {@code null} for non-live-view compiles. The live-view layer dispatches
     * {@code resetPartition}/{@code markPartitionAlive} only to these so a bounded
     * ROWS/RANGE window declared alongside an anchored window is never reset at the
     * anchored window's bucket crossings.
     */
    public ObjList<WindowFunction> getAnchorableWindowFunctions() {
        return anchorableWindowFunctions;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    /** Returns the immutable finite-RANGE repair contract, or null for a mixed/non-RANGE view. */
    public @Nullable LiveViewCheckpointRangePlan getCheckpointRangePlan() {
        return checkpointRangePlan;
    }

    /** Returns the immutable finite-ROWS repair contract, or null for a mixed/non-ROWS view. */
    public @Nullable LiveViewCheckpointRowsPlan getCheckpointRowsPlan() {
        return checkpointRowsPlan;
    }

    /**
     * Returns a cursor for the initial live view bootstrap. Calls {@link Function#init}
     * on ALL functions (including windows, which resets their state to zero — correct for
     * first run). The cursor enters incremental mode so that {@link RecordCursor#close()}
     * preserves window state instead of resetting it.
     *
     * @param baseCursor the already-opened base-table cursor
     */
    public RecordCursor getBootstrapCursor(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        cursor.ofBootstrap(baseCursor, executionContext);
        return cursor;
    }

    /**
     * Prepares the cursor for a live-view checkpoint restore. The restore path
     * fills each function's partition state directly (bypassing the base cursor),
     * so it must first allocate the lazy per-partition maps under the per-query
     * tracker and mark the cursor open. Marking it open makes the subsequent first
     * {@code getIncrementalCursor()} take the state-preserving path rather than
     * re-bootstrapping (which would reset/clobber the just-restored state).
     */
    public void openForLiveViewRestore(SqlExecutionContext executionContext) {
        cursor.openForLiveViewRestore(executionContext);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            // free partial allocations under the still-bound per-query tracker on a failed open
            cursor.close();
            throw th;
        }
    }

    public ObjList<Function> getFunctions() {
        return functions;
    }

    /**
     * Returns a cursor wrapping the given base cursor that drives window functions
     * incrementally — without resetting their accumulated state from prior refreshes.
     * Non-window functions are re-initialized to bind to the new cursor's symbol source.
     *
     * @param baseCursor a cursor over the new WAL segment rows
     */
    public RecordCursor getIncrementalCursor(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        cursor.ofIncremental(baseCursor, executionContext);
        return cursor;
    }

    public ObjList<WindowFunction> getWindowFunctions() {
        return windowFunctions;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // window functions normally depends on other rows in the window/frame, so we can't just jump to an arbitrary position
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Window");
        sink.optAttr("functions", windowFunctions, true);
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

    /**
     * Clears all window functions to their initial state without freeing native
     * resources. Called by the live view refresh job before a full recompute so
     * that the subsequent bootstrap starts from a clean slate.
     * <p>
     * Uses {@link WindowFunction#toTop()} (which calls e.g. {@code map.clear()})
     * rather than {@link WindowFunction#reset()} (which calls {@code map.close()}).
     * The latter frees native memory, making the function unusable without a
     * {@link Reopenable#reopen()}.
     */
    public void resetWindowFunctions() {
        for (int i = 0, n = windowFunctions.size(); i < n; i++) {
            windowFunctions.getQuick(i).toTop();
        }
    }

    @Override
    protected void _close() {
        if (closed) {
            return;
        }
        closed = true;
        final RecordCursorFactory base = this.base;
        this.base = null;
        final WindowRecordCursor cursor = this.cursor;
        this.cursor = null;
        final ObjList<Function> functions = this.functions;
        this.functions = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeObjListBestEffort(failure, functions);
        CairoException.rethrowCleanupFailure(failure);
    }

    class WindowRecordCursor extends AbstractVirtualFunctionRecordCursor {

        private SqlExecutionCircuitBreaker circuitBreaker;
        // When true, close() frees the base cursor but preserves window function state
        // and keeps the cursor logically open for subsequent incremental refreshes.
        private boolean isIncremental;
        private boolean isOpen;

        public WindowRecordCursor(ObjList<Function> functions, boolean supportsRandomAccess) {
            super(functions, supportsRandomAccess);
            // Start closed so the first of() binds the per-query tracker on each
            // window function and reopens its (lazy) per-partition map under it.
            this.isOpen = false;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            baseCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                if (isIncremental) {
                    // Free the base cursor but keep window state and the cursor logically open
                    // so that subsequent ofIncremental() calls can continue accumulating.
                    baseCursor = Misc.free(baseCursor);
                } else {
                    super.close();
                    for (int i = 0, n = windowFunctions.size(); i < n; i++) {
                        windowFunctions.getQuick(i).reset();
                    }
                    isOpen = false;
                }
            }
        }

        @Override
        public boolean hasNext() {
            circuitBreaker.statefulThrowExceptionIfTripped();
            boolean hasNext = super.hasNext();
            if (hasNext) {
                for (int i = 0; i < windowFunctionsCount; i++) {
                    windowFunctions.getQuick(i).computeNext(baseCursor.getRecord());
                }
            }
            return hasNext;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            // we can't skip to an arbitrary result set point because current window function value might depend
            // on values in other rows that could be located anywhere
            RecordCursor.skipRows(this, rowCount);
        }

        @Override
        public void toTop() {
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).toTop();
            }
            baseCursor.toTop();
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            isIncremental = false;
            super.of(baseCursor);
            circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                isOpen = true;
                // Bind the per-query tracker on each window function's per-partition
                // map before reopen() allocates the map backing under it. A breach here (or
                // in Function.init below) propagates to getCursor, which closes the cursor.
                final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
                for (int i = 0; i < windowFunctionsCount; i++) {
                    windowFunctions.getQuick(i).setMemoryTracker(memoryTracker);
                }
                reopen(functions);
            }
            Function.init(functions, baseCursor, executionContext, null);
        }

        /**
         * Bootstrap entry point for live view refresh. Calls {@link Function#init} on ALL
         * functions (resetting window state to zero) and enters incremental mode so that
         * {@link #close()} preserves window state.
         */
        private void ofBootstrap(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            isIncremental = true;
            super.of(baseCursor);
            circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                isOpen = true;
                try {
                    reopen(functions);
                } catch (Throwable t) {
                    close();
                    throw t;
                }
            }
            Function.init(functions, baseCursor, executionContext, null);
        }

        /**
         * Live-view checkpoint-restore entry point. Allocates the lazy per-partition
         * maps under the per-query tracker and marks the cursor open WITHOUT binding a
         * base cursor or calling {@link Function#init} - the restore code writes the
         * partition state directly afterwards. Marking it open ensures the subsequent
         * first {@link #ofIncremental} preserves the restored state instead of
         * re-bootstrapping it.
         */
        private void openForLiveViewRestore(SqlExecutionContext executionContext) {
            if (!isOpen) {
                isOpen = true;
                isIncremental = true;
                final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
                for (int i = 0; i < windowFunctionsCount; i++) {
                    windowFunctions.getQuick(i).setMemoryTracker(memoryTracker);
                }
                try {
                    reopen(functions);
                } catch (Throwable th) {
                    // Same transactional first-open guarantee as ofIncremental(): a partial
                    // reopen must not leave a half-open cursor whose next getIncrementalCursor()
                    // takes the else-branch, skips reopen, and drives computeNext over a
                    // never-reopened (closed) map. The restore failed, so there is no restored
                    // state to preserve - force the full non-incremental teardown so the caller's
                    // rebuild re-bootstraps from a clean slate.
                    isIncremental = false;
                    close();
                    throw th;
                }
            }
        }

        /**
         * Incremental entry point for live view refresh. Rebinds the base cursor
         * but skips {@link Function#init} for window functions so that their accumulated
         * state from prior refreshes is preserved. Non-window functions are re-initialized
         * to bind to the new cursor's symbol source; window functions get their
         * partition-by expressions re-bound through
         * {@link WindowFunction#initPartitionBy(SymbolTableSource, SqlExecutionContext)}
         * so SYMBOL partition columns can resolve through the current cursor's
         * symbol table.
         */
        private void ofIncremental(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            isIncremental = true;
            super.of(baseCursor);
            circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                // First refresh of this cursor: there is no accumulated window state yet,
                // so this call doubles as the bootstrap. Bind the per-query tracker and
                // reopen the (lazy) per-partition maps under it, then fully initialize
                // every function. The live-view refresh only ever calls
                // getIncrementalCursor(), so when the maps are lazy (#7184 starts the
                // cursor closed) this one-time setup has to happen here.
                isOpen = true;
                final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
                for (int i = 0; i < windowFunctionsCount; i++) {
                    windowFunctions.getQuick(i).setMemoryTracker(memoryTracker);
                }
                try {
                    reopen(functions);
                    Function.init(functions, baseCursor, executionContext, null);
                } catch (Throwable th) {
                    // First open doubles as the bootstrap: there is no accumulated window
                    // state to preserve. A partial reopen left some window maps allocated and
                    // later ones closed, and isOpen is already set - so the incremental close()
                    // would keep this half-open state and the next ofIncremental() would take
                    // the else-branch, skip reopen, and drive computeNext over a never-reopened
                    // (closed) native map. Force the full non-incremental teardown - free the
                    // base cursor and reset (free) every window function's map, clearing isOpen -
                    // so a retry re-bootstraps from a clean slate.
                    isIncremental = false;
                    close();
                    throw th;
                }
            } else {
                for (int i = 0, n = functions.size(); i < n; i++) {
                    Function f = functions.getQuick(i);
                    if (f instanceof WindowFunction wf) {
                        wf.initPartitionBy(baseCursor, executionContext);
                    } else {
                        f.init(baseCursor, executionContext);
                    }
                }
            }
        }

        private void reopen(ObjList<Function> list) {
            for (int i = 0, n = list.size(); i < n; i++) {
                Function function = list.getQuick(i);

                if (function instanceof Reopenable) {
                    ((Reopenable) function).reopen();
                }
            }
        }
    }
}

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
import io.questdb.cairo.lv.LiveViewCheckpointKeyProjector;
import io.questdb.cairo.lv.LiveViewCheckpointRangePlan;
import io.questdb.cairo.lv.LiveViewCheckpointRowsPlan;
import io.questdb.cairo.lv.LiveViewPartitionKeyClassifier;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
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
    // For a live view these are the functions of an anchored named WINDOW (at most
    // one anchored window is allowed) plus the checkpoint-stateless calls the
    // bare-unbounded reject admits over that frame, whose resetPartition is a no-op
    // because they keep no per-partition state. So the live-view ANCHOR runtime
    // resets only these and never touches a bounded ROWS/RANGE window's state. Null
    // for non-live-view compiles, which never consult it.
    private final ObjList<WindowFunction> anchorableWindowFunctions;
    // Union of the finite RANGE dependencies every window function in this factory carries,
    // or null when the factory mixes shapes, is not a live-view compile, or has no window
    // function with a finite RANGE frame. Bounds the localized O3 repair interval.
    private final LiveViewCheckpointKeyProjector checkpointKeyProjector;
    private final LiveViewCheckpointRangePlan checkpointRangePlan;
    // The same for the finite ROWS dependencies, plus the key projector the repair counts
    // per-key rows through. A factory mixing shapes carries both plans - each describes
    // the functions of its own kind - and the repair takes their union. This factory owns
    // the plan, because an expression-keyed projector holds compiled key functions.
    private final LiveViewCheckpointRowsPlan checkpointRowsPlan;
    // The fused window-state plan: which accumulator components this live view's durable
    // state is made of and which outputs project them. Null when the factory is not a
    // live-view compile or when nothing in it can join a fused group. Holds only
    // non-owning references into this factory's own functions, so it needs no cleanup.
    private final LiveViewWindowStatePlan checkpointWindowStatePlan;
    // How this compile decided to key every live-view SYMBOL partition term, and the
    // inventory of source columns those terms key by. Null for non-live-view compiles.
    // Holds no resources: it is the compiler's decision, kept so the refresh job can read
    // it instead of making its own.
    private final LiveViewPartitionKeyClassifier livePartitionKeyClassifier;
    // One entry per window Map group this factory's functions form: the components the
    // group's map value would be made of, and the outputs that read them. Null when the
    // factory is a live-view compile, or when no group forms that removes anything. Holds
    // only non-owning references into this factory's own functions, so it needs no cleanup.
    // A plan is compiled whether or not a runtime binds it - see windowMapStates.
    private final ObjList<WindowAccumulatorPlan> windowAccumulatorPlans;
    private final ObjList<WindowFunction> windowFunctions;
    private final int windowFunctionsCount;
    // The runtime owners of the groups above: one map and one lookup per group, with every
    // member's private map left closed. One per compiled plan, or null when
    // cairo.sql.window.map.fusion.enabled is off - the kill switch is what stands between the
    // two lists. Owned by this factory, which frees them; the functions they bind are owned as
    // they always were.
    private final ObjList<WindowMapState> windowMapStates;
    private final int windowMapStatesCount;
    private RecordCursorFactory base;
    private boolean closed = false;
    private WindowRecordCursor cursor;
    private ObjList<Function> functions;

    public WindowRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions
    ) {
        this(base, metadata, functions, null, null, null, null, null, null, null, null);
    }

    public WindowRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions,
            ObjList<WindowFunction> anchorableWindowFunctions
    ) {
        this(base, metadata, functions, anchorableWindowFunctions, null, null, null, null, null, null, null);
    }

    public WindowRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions,
            ObjList<WindowFunction> anchorableWindowFunctions,
            LiveViewPartitionKeyClassifier livePartitionKeyClassifier,
            LiveViewCheckpointKeyProjector checkpointKeyProjector,
            LiveViewCheckpointRangePlan checkpointRangePlan,
            LiveViewCheckpointRowsPlan checkpointRowsPlan,
            LiveViewWindowStatePlan checkpointWindowStatePlan,
            ObjList<WindowAccumulatorPlan> windowAccumulatorPlans,
            ObjList<WindowMapState> windowMapStates
    ) {
        super(metadata);
        this.base = base;
        this.functions = functions;
        this.anchorableWindowFunctions = anchorableWindowFunctions;
        this.livePartitionKeyClassifier = livePartitionKeyClassifier;
        this.checkpointKeyProjector = checkpointKeyProjector;
        this.checkpointRangePlan = checkpointRangePlan;
        this.checkpointRowsPlan = checkpointRowsPlan;
        this.checkpointWindowStatePlan = checkpointWindowStatePlan;
        this.windowAccumulatorPlans = windowAccumulatorPlans;
        this.windowMapStates = windowMapStates;
        this.windowMapStatesCount = windowMapStates == null ? 0 : windowMapStates.size();

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

    /**
     * Returns the partition identity every window function on this factory shares, or null
     * when they do not share one. Compiled once, and the only key schema the view has: the
     * anchor window, the checkpoint roots and every repair speak it.
     */
    public @Nullable LiveViewCheckpointKeyProjector getCheckpointKeyProjector() {
        return checkpointKeyProjector;
    }

    /**
     * Returns the classifier that decided how this compile keys every live-view SYMBOL
     * partition term, or null for an ordinary query. The refresh job hands it to
     * {@code LiveViewWindow.build} so the anchor map keys each term the way the window
     * functions' own maps do - the two are compared row for row by the frontier sweep, so
     * a term the anchor resolved differently would not be a slower view but a wrong one.
     * <p>
     * Its slots are column indexes in this factory's base metadata, which is the window
     * input metadata every site classifying for this view shares.
     */
    public @Nullable LiveViewPartitionKeyClassifier getLivePartitionKeyClassifier() {
        return livePartitionKeyClassifier;
    }

    /**
     * Returns the immutable finite-RANGE repair contract, or null for a mixed/non-RANGE view.
     */
    public @Nullable LiveViewCheckpointRangePlan getCheckpointRangePlan() {
        return checkpointRangePlan;
    }

    /**
     * Returns the immutable finite-ROWS repair contract, or null for a mixed/non-ROWS view.
     */
    public @Nullable LiveViewCheckpointRowsPlan getCheckpointRowsPlan() {
        return checkpointRowsPlan;
    }

    /**
     * Returns the fused window-state plan, or null when this factory carries no group
     * that can share one durable tree. Nothing persists it yet: the seal still writes
     * one legacy root per function, and the plan's first durable consumer is the
     * window-state root.
     */
    public @Nullable LiveViewWindowStatePlan getCheckpointWindowStatePlan() {
        return checkpointWindowStatePlan;
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

    /**
     * Returns the window Map groups this factory's functions form, or null when none does.
     * <p>
     * A compiled group is not necessarily a bound one: {@link #getWindowMapStates()} is the
     * subset this build gives a runtime, and a plan the kill switch or the
     * Map-implementation rule turned away stays compiled - which is what lets a test assert
     * that such a group was worked out and simply given to nobody, rather than assert an
     * absence. A live-view compile never produces one at all -
     * {@link #getCheckpointWindowStatePlan()} is that factory's group, and one accumulator
     * may have one owner.
     */
    public @Nullable ObjList<WindowAccumulatorPlan> getWindowAccumulatorPlans() {
        return windowAccumulatorPlans;
    }

    /**
     * Returns the bound window Map groups - the ones that own a map and make the row's one
     * lookup - or null when this factory binds none.
     * <p>
     * Their members' {@code computeNext} is a no-op and their private partition maps stay
     * closed, so the second dispatch in {@link WindowRecordCursor#hasNext()} reaches only
     * the residual functions in practice while still being written over all of them.
     */
    public @Nullable ObjList<WindowMapState> getWindowMapStates() {
        return windowMapStates;
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
        // Before the functions rather than after: a group owns only its own map and its key
        // projection over base columns, so freeing it touches nothing a function owns - but
        // ordering it first keeps that independence obvious rather than incidental.
        failure = Misc.freeObjListBestEffort(failure, windowMapStates);
        failure = Misc.freeObjListBestEffort(failure, functions);
        // The rows plan first: it frees the key projector only when it compiled one of its
        // own, and the shared one below is the object it would then not be holding.
        failure = Misc.freeBestEffort(failure, checkpointRowsPlan);
        failure = Misc.freeBestEffort(failure, checkpointKeyProjector);
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
                    // Symmetric with openWindowMapStates: each group hands its map backing
                    // back to the tracker that was bound when it was allocated. Reached on a
                    // failed partial open too - getCursor closes the cursor - where a group
                    // that never got as far as reopen() frees a map that is already closed.
                    for (int i = 0; i < windowMapStatesCount; i++) {
                        windowMapStates.getQuick(i).reset();
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
                final Record record = baseCursor.getRecord();
                // Groups first, and the whole of a group before any of it is read: a bound
                // function's computeNext below is a no-op, and its getters answer with
                // whatever the group's projection loop just materialized.
                for (int i = 0; i < windowMapStatesCount; i++) {
                    windowMapStates.getQuick(i).computeNext(record);
                }
                for (int i = 0; i < windowFunctionsCount; i++) {
                    windowFunctions.getQuick(i).computeNext(record);
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
            // Exactly once per group, not once per bound member: several functions read one
            // shared key domain, and a bound function's own toTop deliberately leaves it
            // alone (its private map is closed, and clearing a closed map would walk backing
            // it no longer holds).
            for (int i = 0; i < windowMapStatesCount; i++) {
                windowMapStates.getQuick(i).clear();
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
                openWindowMapStates(memoryTracker);
            }
            Function.init(functions, baseCursor, executionContext, null);
        }

        /**
         * Binds the per-query tracker on every bound group's map and allocates its backing,
         * in that order, so the malloc and the free that {@link #close()} performs land on the
         * same counter.
         * <p>
         * It runs after {@code reopen(functions)} and needs nothing from {@link Function#init}:
         * what it allocates is map backing, and nothing here evaluates a key.
         * <p>
         * A group whose key is an expression does read through compiled PARTITION BY functions,
         * and they are bound to the symbol source by the {@code Function.init} below - they are
         * a member function's own terms, borrowed, so initializing that function initializes
         * them. That ordering holds for {@code of}, which inits every row's worth of function
         * before the first {@code hasNext}; {@code ofIncremental} is a live-view entry point
         * and no live-view compile produces a group at all.
         */
        private void openWindowMapStates(MemoryTracker memoryTracker) {
            for (int i = 0; i < windowMapStatesCount; i++) {
                final WindowMapState state = windowMapStates.getQuick(i);
                state.setMemoryTracker(memoryTracker);
                state.reopen();
            }
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
                    // A live-view compile binds no group, so this is a no-op today. It is
                    // written all the same, because the day one is bound here the omission
                    // would show up as a cursor driving computeNext over a closed map.
                    openWindowMapStates(memoryTracker);
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
                    openWindowMapStates(memoryTracker);
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

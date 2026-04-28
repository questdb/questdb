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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.AbstractVirtualFunctionRecordCursor;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/**
 * Factory implements select with window functions that support streaming, that is:
 * - they don't specify order by or order by is the same as underlying query
 * - all functions and their framing clause do support stream-ed processing (single pass)
 */
public class WindowRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final WindowRecordCursor cursor;
    private final ObjList<Function> functions;
    private final ObjList<WindowFunction> windowFunctions;
    private final int windowFunctionsCount;
    private boolean closed = false;

    public WindowRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions
    ) {
        super(metadata);
        this.base = base;
        this.functions = functions;

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

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
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

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        cursor.of(baseCursor, executionContext);
        return cursor;
    }

    public ObjList<Function> getFunctions() {
        return functions;
    }

    /**
     * Conservative max ts lookback across all window functions, in micros. Returns
     * {@code -1} if any function reports an unbounded or ts-inexpressible lookback
     * (the live view's cold-path skip must then be disabled). Otherwise returns
     * the largest {@link WindowFunction#getMaxLookbackMicros()} across the factory's
     * functions — the horizon a late row must clear to be guaranteed cold.
     */
    public long getMaxLookbackMicros() {
        long max = 0;
        for (int i = 0; i < windowFunctionsCount; i++) {
            long lb = windowFunctions.getQuick(i).getMaxLookbackMicros();
            if (lb < 0) {
                return -1;
            }
            if (lb > max) {
                max = lb;
            }
        }
        return max;
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
     * Drives live view Phase 5 partition-state eviction across all window functions.
     * Each partitioned function sheds accumulator entries whose last-seen row
     * timestamp has fallen below {@code cutoffTs}. Non-partitioned window functions
     * treat this as a no-op.
     * <p>
     * Called by the refresh job after {@code applyRetention} has advanced the
     * retention cutoff, when the view's state horizon is not below the cutoff
     * (for an expanded-horizon any-unbounded view the accumulator legitimately
     * reflects pre-retention rows; eviction would discard work the disk-read
     * replay paid for).
     */
    public void evictStalePartitionState(long cutoffTs) {
        for (int i = 0, n = windowFunctions.size(); i < n; i++) {
            windowFunctions.getQuick(i).evictStalePartitionState(cutoffTs);
        }
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
        Misc.free(base);
        Misc.free(cursor);
        Misc.freeObjList(functions);
        closed = true;
    }

    class WindowRecordCursor extends AbstractVirtualFunctionRecordCursor {

        private SqlExecutionCircuitBreaker circuitBreaker;
        // When true, close() frees the base cursor but preserves window function state
        // and keeps the cursor logically open for subsequent incremental refreshes.
        private boolean isIncremental;
        private boolean isOpen;

        public WindowRecordCursor(ObjList<Function> functions, boolean supportsRandomAccess) {
            super(functions, supportsRandomAccess);
            this.isOpen = true;
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
        public void skipRows(Counter rowCount) {
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
         * Incremental entry point for live view refresh. Rebinds the base cursor
         * but skips {@link Function#init} for window functions so that their accumulated
         * state from prior refreshes is preserved. Non-window functions are re-initialized
         * to bind to the new cursor's symbol source.
         */
        private void ofIncremental(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            isIncremental = true;
            super.of(baseCursor);
            circuitBreaker = executionContext.getCircuitBreaker();
            assert isOpen : "incremental cursor requires a prior bootstrap";
            for (int i = 0, n = functions.size(); i < n; i++) {
                Function f = functions.getQuick(i);
                if (!(f instanceof WindowFunction)) {
                    f.init(baseCursor, executionContext);
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

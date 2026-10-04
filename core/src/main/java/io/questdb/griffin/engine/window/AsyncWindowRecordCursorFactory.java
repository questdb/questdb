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

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A streaming window, as {@link WindowRecordCursorFactory} computes it, over a key-major index
 * scan whose key every window function is partitioned by. The keys are independent of each other,
 * so the shared query workers compute them in parallel, a task of whole keys each, while the
 * query's own thread returns the rows in the scan's key order; see {@link AsyncWindowRecordCursor}.
 * The output is the serial window's, row for row.
 */
public class AsyncWindowRecordCursorFactory extends AbstractRecordCursorFactory {
    private final AsyncWindowAtom atom;
    private final int keyColumnIndex;
    private final AsyncWindowSplitPlan splitPlan;
    private final ObjList<WindowFunction> windowFunctions = new ObjList<>();
    private final int workerCount;
    private RecordCursorFactory base;
    private AsyncWindowRecordCursor cursor;
    private ObjList<Function> functions;
    // one per round that can be alive at a time
    private ObjList<UnorderedPageFrameSequence<AsyncWindowRecordCursor.RoundAtom>> sequences;
    private ObjList<WindowMapState> windowMapStates;

    /**
     * Takes ownership of {@code base}, {@code functions} and {@code windowMapStates} once it
     * returns, and of the worker copies also when it throws.
     *
     * @param functions            the output columns' functions, for the query's own thread
     * @param windowMapStates      the window Map groups over {@code functions}, or null
     * @param perWorkerFunctions   separately compiled copies of {@code functions}, one per worker slot;
     *                             there may be fewer slots than workers, which then share them
     * @param perWorkerMapStates   the window Map groups of each copy, entries may be null
     * @param keyColumnIndex       the base column the scan walks key by key, which every window
     *                             function is partitioned by
     * @param partitionedByKeyOnly whether every window function is partitioned by that column and
     *                             no other, so that tasks may compute key runs, see
     *                             {@link KeyRunWindowFunction}
     */
    public AsyncWindowRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull GenericRecordMetadata metadata,
            @NotNull ObjList<Function> functions,
            @Nullable ObjList<WindowMapState> windowMapStates,
            @NotNull ObjList<ObjList<Function>> perWorkerFunctions,
            @NotNull ObjList<ObjList<WindowMapState>> perWorkerMapStates,
            @NotNull RecordSink recordSink,
            @NotNull AsyncWindowSplitPlan splitPlan,
            int keyColumnIndex,
            boolean partitionedByKeyOnly,
            int workerCount
    ) {
        super(metadata);
        this.base = base;
        this.functions = functions;
        this.windowMapStates = windowMapStates;
        this.keyColumnIndex = keyColumnIndex;
        this.splitPlan = splitPlan;
        this.workerCount = workerCount;
        AsyncWindowAtom atom = null;
        final ObjList<UnorderedPageFrameSequence<AsyncWindowRecordCursor.RoundAtom>> sequences = new ObjList<>();
        try {
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (functions.getQuick(i) instanceof WindowFunction wf) {
                    windowFunctions.add(wf);
                }
            }
            // takes the worker copies out of the lists as it comes to own them
            atom = new AsyncWindowAtom(
                    configuration,
                    functions,
                    windowMapStates,
                    perWorkerFunctions,
                    perWorkerMapStates,
                    partitionedByKeyOnly ? keyColumnIndex : -1
            );
            for (int i = 0, n = Math.max(2, configuration.getSqlParallelWindowMaxRounds()); i < n; i++) {
                // each owns its round atom, never the shared atom
                sequences.add(new UnorderedPageFrameSequence<>(
                        engine,
                        configuration,
                        messageBus,
                        new AsyncWindowRecordCursor.RoundAtom(atom),
                        AsyncWindowRecordCursor.REDUCER,
                        workerCount
                ));
            }
            this.cursor = new AsyncWindowRecordCursor(configuration, atom, sequences, metadata, recordSink, splitPlan, workerCount);
        } catch (Throwable th) {
            // The caller keeps base, functions and windowMapStates; free what this built and the
            // worker copies no atom took.
            Misc.freeObjListAndClear(sequences);
            Misc.free(atom);
            for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                Misc.freeObjList(perWorkerMapStates.getQuiet(i));
                Misc.freeObjList(perWorkerFunctions.getQuick(i));
            }
            perWorkerFunctions.clear();
            perWorkerMapStates.clear();
            this.base = null;
            this.functions = null;
            this.windowMapStates = null;
            throw th;
        }
        this.atom = atom;
        this.sequences = sequences;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public @Nullable StatefulAtom getAtom() {
        return atom;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            // the cursor owns the base cursor from the start of of()
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    /**
     * The cursor this factory hands out, whose counters describe the last execution.
     */
    @TestOnly
    public AsyncWindowRecordCursor getAsyncCursor() {
        return cursor;
    }

    /**
     * The functions of the output columns, in column order.
     */
    public ObjList<Function> getFunctions() {
        return functions;
    }

    public AsyncWindowSplitPlan getSplitPlan() {
        return splitPlan;
    }

    public ObjList<WindowFunction> getWindowFunctions() {
        return windowFunctions;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // a window value depends on other rows of its partition
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Window");
        sink.meta("workers").val(workerCount);
        sink.optAttr("functions", windowFunctions, true);
        sink.attr("keyShards").putBaseColumnName(keyColumnIndex);
        if (splitPlan.getMode() != AsyncWindowSplitPlan.MODE_NONE) {
            sink.attr("keySplit").val(splitPlan);
        }
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
        final AsyncWindowRecordCursor cursor = this.cursor;
        this.cursor = null;
        final ObjList<UnorderedPageFrameSequence<AsyncWindowRecordCursor.RoundAtom>> sequences = this.sequences;
        this.sequences = null;
        final RecordCursorFactory base = this.base;
        this.base = null;
        final ObjList<WindowMapState> windowMapStates = this.windowMapStates;
        this.windowMapStates = null;
        final ObjList<Function> functions = this.functions;
        this.functions = null;
        Throwable failure = Misc.freeBestEffort(null, cursor);
        failure = Misc.freeObjListBestEffort(failure, sequences);
        // frees the worker copies
        failure = Misc.freeBestEffort(failure, atom);
        failure = Misc.freeBestEffort(failure, base);
        failure = Misc.freeObjListBestEffort(failure, windowMapStates);
        failure = Misc.freeObjListBestEffort(failure, functions);
        CairoException.rethrowCleanupFailure(failure);
    }
}

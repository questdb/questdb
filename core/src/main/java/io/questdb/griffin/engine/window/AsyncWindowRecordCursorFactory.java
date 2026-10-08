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
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.std.IntList;
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
    private final CairoConfiguration configuration;
    private final int keyColumnIndex;
    // the key-major scan, the base or the one under the base's projection
    private final RecordCursorFactory scan;
    // the query thread's copy of the steps after the window, which this factory owns
    private final ObjList<AsyncWindowStage> ownerStages;
    private final AsyncWindowSplitPlan splitPlan;
    private final ObjList<WindowFunction> windowFunctions;
    private final int workerCount;
    private AsyncWindowAtom atom;
    private RecordCursorFactory base;
    // how keys may still split as stages are appended, see AsyncWindowChainSplit
    private AsyncWindowChainSplit chainSplit;
    // the output column that is the group key after the scan's key, after a GROUP BY step, -1
    private int groupOrderIndex = -1;
    // whether the scan walks its keys in ascending order of their values
    private boolean isKeyOrderAscending;
    // the output column that carries the scan's key, -1 when none does
    private int keyOutputIndex = -1;
    // per output column: whether its values never decrease within a key of the scan, in walk order
    private boolean[] nonDecreasingColumns = new boolean[0];
    // per output column: whether its values are never negative, or NULL
    private boolean[] nonNegativeColumns = new boolean[0];
    // see getWholeBounds()
    private long[] wholeBounds = new long[0];
    // per output column: whether its values are never NULL
    private boolean[] nonNullColumns = new boolean[0];
    // whether the scan walks a single key
    private boolean singleKey;
    // while EXPLAIN prints a step: the metadata naming the columns its functions read
    private RecordMetadata planMetadata;
    // the output column that carries the column ascending within each key, -1 when none does
    private int timestampOutputIndex = -1;
    private AsyncWindowRecordCursor cursor;
    // the cursor of a plain scan whose keys are sharded by hash, see shardKeyColumnIndex
    private AsyncWindowShardCursor shardCursor;
    private final int shardKeyColumnIndex;
    // the scan's key column in the scan's own metadata, for EXPLAIN when the window drops it
    private int scanKeyColumnIndex = -1;
    private final boolean sliceMode;
    // the slice mode's WHERE for the query's thread, which this factory owns, or null
    private Function prefilter;
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
     * @param crossIndex           see {@link AsyncWindowAtom}
     * @param shardKeyColumnIndex  the symbol column of a plain scan whose keys the workers shard
     *                             by hash, see {@link AsyncWindowShardCursor}; -1 for a key-major
     *                             scan
     * @param sliceMode            whether the workers compute disjoint row ranges of a plain
     *                             scan of a single stream, see {@link AsyncWindowShardCursor}
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
            int workerCount,
            @Nullable IntList crossIndex,
            int shardKeyColumnIndex,
            boolean sliceMode
    ) {
        super(metadata);
        this.shardKeyColumnIndex = shardKeyColumnIndex;
        this.sliceMode = sliceMode;
        this.configuration = configuration;
        this.ownerStages = new ObjList<>();
        this.windowFunctions = new ObjList<>();
        this.base = base;
        this.functions = functions;
        this.windowMapStates = windowMapStates;
        this.keyColumnIndex = keyColumnIndex;
        // over a projection of the scan, the slots project the scan's own cursor themselves
        this.scan = crossIndex != null ? base.getBaseFactory() : base;
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
                    partitionedByKeyOnly && crossIndex == null ? keyColumnIndex : -1,
                    crossIndex
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
            if (shardKeyColumnIndex > -1 || sliceMode) {
                this.shardCursor = new AsyncWindowShardCursor(configuration, atom, sequences, metadata, recordSink, base.getMetadata(), sliceMode ? -1 : shardKeyColumnIndex, splitPlan);
            } else {
                this.cursor = new AsyncWindowRecordCursor(configuration, atom, sequences, metadata, recordSink, splitPlan, workerCount);
            }
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

    // Takes over everything the other factory owns, with the output of a step appended to it.
    private AsyncWindowRecordCursorFactory(
            AsyncWindowRecordCursorFactory from,
            GenericRecordMetadata metadata,
            RecordSink recordSink,
            AsyncWindowSplitPlan splitPlan
    ) {
        super(metadata);
        // the only allocation that can fail, before anything changes hands
        if (from.shardKeyColumnIndex > -1 || from.sliceMode) {
            this.shardCursor = new AsyncWindowShardCursor(from.configuration, from.atom, from.sequences, metadata, recordSink, from.base.getMetadata(), from.sliceMode ? -1 : from.shardKeyColumnIndex, splitPlan);
        } else {
            this.cursor = new AsyncWindowRecordCursor(from.configuration, from.atom, from.sequences, metadata, recordSink, splitPlan, from.workerCount);
        }
        this.shardKeyColumnIndex = from.shardKeyColumnIndex;
        this.sliceMode = from.sliceMode;
        this.prefilter = from.prefilter;
        from.prefilter = null;
        this.configuration = from.configuration;
        this.keyColumnIndex = from.keyColumnIndex;
        this.scan = from.scan;
        this.scanKeyColumnIndex = from.scanKeyColumnIndex;
        this.ownerStages = from.ownerStages;
        this.splitPlan = splitPlan;
        this.windowFunctions = from.windowFunctions;
        this.workerCount = from.workerCount;
        this.atom = from.atom;
        this.base = from.base;
        this.functions = from.functions;
        this.sequences = from.sequences;
        this.windowMapStates = from.windowMapStates;
        this.chainSplit = from.chainSplit;
        this.singleKey = from.singleKey;
        this.isKeyOrderAscending = from.isKeyOrderAscending;
        // the other factory's cursor was never opened, and its rounds hold no task yet
        Misc.free(from.cursor);
        from.cursor = null;
        Misc.free(from.shardCursor);
        from.shardCursor = null;
        from.atom = null;
        from.base = null;
        from.functions = null;
        from.sequences = null;
        from.windowMapStates = null;
    }

    @Override
    public boolean followedOrderByAdvice() {
        // a GROUP BY step returns groups, which follow no ORDER BY the scan followed, and shards
        // return their keys round by round
        return shardKeyColumnIndex < 0 && base.followedOrderByAdvice() && !atom.hasGroupByStage();
    }

    @Override
    public String getBaseColumnName(int idx) {
        final RecordMetadata metadata = planMetadata;
        return metadata != null ? metadata.getColumnName(idx) : super.getBaseColumnName(idx);
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
        if (shardCursor != null) {
            final PageFrameCursor frameCursor = scan.getPageFrameCursor(executionContext, PartitionFrameCursorFactory.ORDER_ASC);
            try {
                shardCursor.of(frameCursor, executionContext);
                return shardCursor;
            } catch (Throwable th) {
                // the cursor owns the frame cursor from the start of of()
                shardCursor.close();
                throw th;
            }
        }
        final RecordCursor baseCursor = scan.getCursor(executionContext);
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
        // shards return their keys round by round, not in the scan's order
        return shardKeyColumnIndex > -1 ? SCAN_DIRECTION_OTHER : base.getScanDirection();
    }

    /**
     * The cursor of a factory that shards a plain scan's keys by hash, see
     * {@link AsyncWindowShardCursor}, else null.
     */
    @TestOnly
    public AsyncWindowShardCursor getShardCursor() {
        return shardCursor;
    }

    /**
     * Whether the workers shard a plain scan's keys by hash, see {@link AsyncWindowShardCursor}.
     */
    public boolean isShardMode() {
        return shardKeyColumnIndex > -1;
    }

    /**
     * Whether the workers compute disjoint row ranges of a plain scan of a single stream, see
     * {@link AsyncWindowShardCursor}.
     */
    public boolean isSliceMode() {
        return sliceMode;
    }

    /**
     * Gives the slice mode's scan its WHERE: this factory takes {@code ownerFilter}, and the atom
     * the worker copies, also when it throws.
     */
    public void setPrefilters(@NotNull Function ownerFilter, @NotNull ObjList<Function> workerFilters) {
        this.prefilter = ownerFilter;
        atom.setPrefilters(ownerFilter, workerFilters);
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

    public AsyncWindowChainSplit getChainSplit() {
        return chainSplit;
    }

    /**
     * The output column that carries the scan's key, -1 when none does.
     */
    public int getKeyOutputIndex() {
        return keyOutputIndex;
    }

    /**
     * The output column whose values ascend within each key of the scan, -1 when none does.
     */
    public int getTimestampOutputIndex() {
        return timestampOutputIndex;
    }

    /**
     * Whether the scan walks a single key, which a window then partitions by or not alike.
     */
    public boolean isSingleKey() {
        return singleKey;
    }

    /**
     * Records where the scan's key and its ascending timestamp land in the output, for the stages
     * the planner may append.
     */
    public void setChainColumns(boolean singleKey, int keyOutputIndex, int timestampOutputIndex) {
        this.singleKey = singleKey;
        this.keyOutputIndex = keyOutputIndex;
        this.timestampOutputIndex = timestampOutputIndex;
    }

    /**
     * The scan's key column in the scan's metadata, which EXPLAIN names when the output drops it.
     */
    public void setScanKeyColumnIndex(int scanKeyColumnIndex) {
        this.scanKeyColumnIndex = scanKeyColumnIndex;
    }

    public void setChainSplit(AsyncWindowChainSplit chainSplit) {
        this.chainSplit = chainSplit;
    }

    /**
     * Records what the planner proved of each output column, so that a GROUP BY on one sees each
     * group's rows together, and the order of its groups is known:
     * <ul>
     *     <li>{@code nonDecreasingColumns}: along the walk, within each key of the scan, the
     *     column's values are a run of NULLs, possibly empty, then values that never decrease.
     *     The rows of a key that share a value, NULL included, are then contiguous, and so are
     *     the rows of a group of the key and the column.</li>
     *     <li>{@code nonNegativeColumns}: the values are never negative, or NULL, see
     *     {@code SqlCodeGenerator.isNonNegativeValue}: a running sum of them never decreases.</li>
     *     <li>{@code nonNullColumns}: the values are never NULL.</li>
     * </ul>
     */
    public void setColumnOrder(boolean[] nonDecreasingColumns, boolean[] nonNegativeColumns, boolean[] nonNullColumns) {
        this.nonDecreasingColumns = nonDecreasingColumns;
        this.nonNegativeColumns = nonNegativeColumns;
        this.nonNullColumns = nonNullColumns;
    }

    /**
     * Per output column, the magnitude bound of its values when they are always whole numbers, -1
     * when they may not be or have no known bound (and for a column past the array's end); see
     * {@code SqlCodeGenerator.wholeValueBound}. A window chained on reads them for its arguments.
     */
    public long[] getWholeBounds() {
        return wholeBounds;
    }

    public void setWholeBounds(long[] wholeBounds) {
        this.wholeBounds = wholeBounds;
    }

    public boolean[] getNonNullColumns() {
        return nonNullColumns;
    }

    public boolean[] getNonDecreasingColumns() {
        return nonDecreasingColumns;
    }

    public boolean[] getNonNegativeColumns() {
        return nonNegativeColumns;
    }

    /**
     * After a GROUP BY step: the output column of the group key that orders the groups of one
     * scan key, -1 when the groups are the scan's keys alone; see {@link #isOrderedByKeyThenGroup}.
     */
    public void setGroupOrder(int groupOrderIndex) {
        this.groupOrderIndex = groupOrderIndex;
    }

    public void setKeyOrderAscending(boolean isKeyOrderAscending) {
        this.isKeyOrderAscending = isKeyOrderAscending;
    }

    /**
     * Whether, after a GROUP BY step, the output is in ascending order of the scan's key (when
     * the scan has several keys) and then of the group key: the groups of a key come out in the
     * order of its rows, along which the group key never decreases.
     *
     * @param keyColumn   the output column ordered first, -1 for none
     * @param groupColumn the output column ordered next, -1 for none
     */
    public boolean isOrderedByKeyThenGroup(int keyColumn, int groupColumn) {
        if (atom == null || !atom.hasGroupByStage()) {
            return false;
        }
        if (groupColumn != groupOrderIndex) {
            return false;
        }
        if (singleKey) {
            return keyColumn == -1 || keyColumn == keyOutputIndex;
        }
        return isKeyOrderAscending && keyColumn > -1 && keyColumn == keyOutputIndex;
    }

    /**
     * The query thread's copy of the steps after the window, in order.
     */
    public ObjList<AsyncWindowStage> getStages() {
        return ownerStages;
    }

    /**
     * Returns a factory that computes this one's rows, then one more step over them, see
     * {@link AsyncWindowStage}, and owns everything this one owned; this factory is left empty
     * and is not to be used or closed. Takes the step's copies also when it throws, and this
     * factory is then unchanged.
     *
     * @param ownerStage   the query thread's copy of the step
     * @param workerStages one copy per worker slot, see {@link #getWorkerSlotCount()}
     * @param metadata     the step's output
     * @param recordSink   copies the step's output into a task's row buffer
     * @param splitPlan    how keys may split over tasks, now that the step is part of the rows
     * @param carryStage   see {@link AsyncWindowAtom#setCarryStage(int)}
     */
    public AsyncWindowRecordCursorFactory withStage(
            @NotNull AsyncWindowStage ownerStage,
            @NotNull ObjList<AsyncWindowStage> workerStages,
            @NotNull GenericRecordMetadata metadata,
            @NotNull RecordSink recordSink,
            @NotNull AsyncWindowSplitPlan splitPlan,
            int carryStage
    ) {
        final AsyncWindowRecordCursorFactory next;
        try {
            // The workers of a folded column output a stand-in for it (AsyncWindowFoldEcho), which
            // only the query thread's fold turns into the column: a step after it would read the
            // stand-in, see SqlCodeGenerator, which chains nothing over a fold.
            if (this.splitPlan.hasFold()) {
                throw CairoException.critical(0).put("internal error: a step over a folded window");
            }
            next = new AsyncWindowRecordCursorFactory(this, metadata, recordSink, splitPlan);
        } catch (Throwable th) {
            Misc.free(ownerStage);
            Misc.freeObjListAndClear(workerStages);
            throw th;
        }
        // the step's functions read this factory's output
        if (ownerStage.getPlanMetadata() == null) {
            ownerStage.setPlanMetadata(getMetadata());
        }
        next.ownerStages.add(ownerStage);
        next.atom.addStage(ownerStage, workerStages);
        next.atom.setCarryStage(carryStage);
        return next;
    }

    /**
     * Worker slots, each with a copy of the window and of the steps after it.
     */
    public int getWorkerSlotCount() {
        return atom.getWorkerSlotCount();
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
        if (sliceMode) {
            sink.attr("rowSlices").val(true);
            if (prefilter != null) {
                sink.optAttr("filter", prefilter, true);
            }
        } else if (shardKeyColumnIndex > -1) {
            sink.attr("hashShards").putBaseColumnName(shardKeyColumnIndex);
        } else if (keyColumnIndex > -1) {
            sink.attr("keyShards").putBaseColumnName(keyColumnIndex);
        } else {
            // a single key the window does not read
            sink.attr("keyShards").val(scan.getMetadata().getColumnName(scanKeyColumnIndex));
        }
        if (atom.isKeyRunEnabled()) {
            sink.attr("keyRuns").val(true);
        }
        if (splitPlan.getMode() != AsyncWindowSplitPlan.MODE_NONE) {
            sink.attr("keySplit").val(splitPlan);
        }
        for (int i = 0, n = ownerStages.size(); i < n; i++) {
            final AsyncWindowStage stage = ownerStages.getQuick(i);
            planMetadata = stage.getPlanMetadata();
            switch (stage.getKind()) {
                case AsyncWindowStage.KIND_VIRTUAL ->
                        sink.attr("then").val("project").optAttr("functions", stage.getFunctions(), true);
                case AsyncWindowStage.KIND_WINDOW ->
                        sink.attr("then").val("window").optAttr("functions", stage.getWindowFunctions(), true);
                case AsyncWindowStage.KIND_GROUP_BY -> {
                    sink.attr("then").val("group by");
                    sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(stage.getFunctions(), getMetadata()));
                    sink.optAttr("values", ((AsyncWindowGroupByStage) stage).getGroupByFunctions(), true);
                }
                default -> sink.attr("then").val("filter").optAttr("filter", stage.getFunctions().getQuick(0), true);
            }
        }
        planMetadata = null;
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
        final AsyncWindowShardCursor shardCursor = this.shardCursor;
        this.shardCursor = null;
        final Function prefilter = this.prefilter;
        this.prefilter = null;
        final ObjList<UnorderedPageFrameSequence<AsyncWindowRecordCursor.RoundAtom>> sequences = this.sequences;
        this.sequences = null;
        final RecordCursorFactory base = this.base;
        this.base = null;
        final ObjList<WindowMapState> windowMapStates = this.windowMapStates;
        this.windowMapStates = null;
        final ObjList<Function> functions = this.functions;
        this.functions = null;
        final AsyncWindowAtom atom = this.atom;
        this.atom = null;
        Throwable failure = Misc.freeBestEffort(null, cursor);
        failure = Misc.freeBestEffort(failure, shardCursor);
        failure = Misc.freeBestEffort(failure, prefilter);
        failure = Misc.freeObjListBestEffort(failure, sequences);
        // frees the worker copies
        failure = Misc.freeBestEffort(failure, atom);
        if (atom != null) {
            // the steps of an emptied factory belong to the factory that took them over
            failure = Misc.freeObjListBestEffort(failure, ownerStages);
        }
        failure = Misc.freeBestEffort(failure, base);
        failure = Misc.freeObjListBestEffort(failure, windowMapStates);
        failure = Misc.freeObjListBestEffort(failure, functions);
        CairoException.rethrowCleanupFailure(failure);
    }
}

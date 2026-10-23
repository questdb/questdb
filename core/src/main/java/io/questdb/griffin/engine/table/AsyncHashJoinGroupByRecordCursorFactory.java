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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * Keyed and scalar shared-build execution the planner selects for eligible join aggregations.
 * Takes ownership of both child factories, functions, both filter contexts and the build ON
 * filter with its worker copies on entry, including construction failure. Either context may carry a JIT-compiled
 * filter alongside the interpreted one. Both children are page frame scans: the planner steals
 * their filters into the contexts. Borrows metadata only during construction. Callers must
 * compile functions for the same worker count. The atom borrows the build factory, its filters
 * and its context, and the frame sequence closes the atom first.
 */
public final class AsyncHashJoinGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final UnorderedPageFrameReducer AGGREGATE = AsyncHashJoinGroupByRecordCursorFactory::aggregate;
    private static final UnorderedPageFrameReducer FILTER_AND_AGGREGATE = AsyncHashJoinGroupByRecordCursorFactory::filterAndAggregate;
    private final String condition;
    // True when any key column is a SYMBOL pair, which the probe translates into the build's keys.
    private final boolean hasSymbolKey;
    private final boolean isInputSwapped;
    private final boolean isOuter;
    private final RecordMetadata joinedMetadata;
    private final int logicalJoinType;
    private final int workerCount;
    private RecordCursorFactory buildFactory;
    private AsyncFilterContext buildFilterContext;
    private Function buildOnFilter;
    private AsyncHashJoinGroupByRecordCursor cursor;
    private AsyncFilterContext filterContext;
    private UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence;
    private HashJoinGroupByFunctions functions;
    private RecordCursorFactory probeFactory;
    // Each worker's copy of the build ON filter for a build on the workers; null when not needed.
    private ObjList<Function> workerBuildOnFilters;

    @TestOnly
    public AsyncHashJoinGroupByRecordCursorFactory(
            CairoEngine engine,
            RecordCursorFactory probeFactory,
            RecordCursorFactory buildFactory,
            AsyncFilterContext buildFilterContext,
            @Nullable Function buildOnFilter,
            HashJoinGroupByMetadata metadata,
            HashJoinGroupByFunctions functions,
            AsyncFilterContext filterContext,
            boolean isOuter,
            int workerCount
    ) {
        this(engine, probeFactory, buildFactory, buildFilterContext, buildOnFilter, null, metadata, functions, filterContext,
                isOuter, workerCount, isOuter ? IQueryModel.JOIN_LEFT_OUTER : IQueryModel.JOIN_INNER, false);
    }

    public AsyncHashJoinGroupByRecordCursorFactory(
            CairoEngine engine,
            RecordCursorFactory probeFactory,
            RecordCursorFactory buildFactory,
            AsyncFilterContext buildFilterContext,
            @Nullable Function buildOnFilter,
            @Nullable ObjList<Function> workerBuildOnFilters,
            HashJoinGroupByMetadata metadata,
            HashJoinGroupByFunctions functions,
            AsyncFilterContext filterContext,
            boolean isOuter,
            int workerCount,
            int logicalJoinType,
            boolean isInputSwapped
    ) {
        super(functions.getOutputMetadata());
        this.probeFactory = probeFactory;
        this.buildFactory = buildFactory;
        this.buildFilterContext = buildFilterContext;
        this.buildOnFilter = buildOnFilter;
        this.workerBuildOnFilters = workerBuildOnFilters;
        this.functions = functions;
        this.filterContext = filterContext;
        this.isOuter = isOuter;
        this.workerCount = workerCount;
        this.logicalJoinType = logicalJoinType;
        this.isInputSwapped = isInputSwapped;
        this.hasSymbolKey = metadata.getSymbolKeyProbeColumns().size() > 0;
        try {
            this.joinedMetadata = GenericRecordMetadata.copyOf(metadata.getJoinedMetadata());
            this.condition = metadata.getCondition();
            if (workerCount < 1 || functions.getWorkerCount() != workerCount
                    || !probeFactory.supportsPageFrameCursor() || !buildFactory.supportsPageFrameCursor()) {
                throw new IllegalArgumentException("unsupported fused hash join execution inputs");
            }
            AsyncHashJoinGroupByAtom atom = new AsyncHashJoinGroupByAtom(engine, buildFactory, buildFilterContext,
                    buildOnFilter, workerBuildOnFilters, metadata, functions, filterContext, isOuter, workerCount);
            // A probe filter belongs to the query, not to a frame, so the reducer is fixed here:
            // queries without one keep the dense loops that never test a filter per row.
            final UnorderedPageFrameReducer reducer = filterContext.getFilter(-1) != null
                    ? FILTER_AND_AGGREGATE : AGGREGATE;
            // The sequence takes atom ownership on entry, also on constructor failure.
            frameSequence = new UnorderedPageFrameSequence<>(engine, engine.getConfiguration(),
                    engine.getMessageBus(), atom, reducer, workerCount);
            atom.bindFrameSequence(frameSequence);
            cursor = new AsyncHashJoinGroupByRecordCursor(engine, frameSequence, functions);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    @TestOnly
    public AsyncHashJoinGroupByAtom getAtom() {
        return frameSequence.getAtom();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return probeFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.close();
        cursor.open(executionContext.getCircuitBreaker());
        try {
            executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottled();
            // Opens both scans and initializes the filters and the functions, and reads no row: the
            // cursor builds on first demand for a result; see AsyncHashJoinGroupByAtom.build().
            final int order = probeFactory.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
            frameSequence.of(probeFactory, executionContext, order);
            return cursor;
        } catch (Throwable th) {
            Misc.free(cursor, th);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return SCAN_DIRECTION_OTHER;
    }

    /**
     * True when either input scans a timestamp interval, so that the rows it contributes can
     * change between executions of this factory; see {@link HashJoinGroupByBuildChoiceRecordCursorFactory}.
     */
    public boolean hasIntervalScan() {
        return isIntervalScan(probeFactory) || isIntervalScan(buildFactory);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return functions.isKeyed();
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (usesCompiledFilter()) {
            sink.type("Async JIT Hash Join Group By");
        } else {
            sink.type("Async Hash Join Group By");
        }
        sink.meta("workers").val(workerCount);
        sink.attr("logicalJoinType").val(logicalJoinType == IQueryModel.JOIN_RIGHT_OUTER ? "right outer"
                : logicalJoinType == IQueryModel.JOIN_LEFT_OUTER ? "left outer" : "inner");
        sink.attr("physicalJoinType").val(isOuter ? "left outer" : "inner");
        sink.attr("inputSwapped").val(isInputSwapped);
        sink.attr("condition").val(condition);
        if (hasSymbolKey) {
            sink.attr("symbolKeyJoin").val(true);
        }
        sink.attr("buildStrategy").val("shared");
        // The copy is chosen per execution, from the build's and the probe's row counts, so the plan
        // names the rule rather than the outcome.
        if (frameSequence.getAtom().isPayloadCopyable()) {
            sink.attr("buildPayload").val("copied when the probe is large enough");
        }
        if (!functions.isKeyed()) {
            sink.attr("aggregation").val("scalar");
        }
        // The reducers add a key's build row count per probe row and visit no joined pair. Only a
        // build that repeats a key has to count its keys' rows first, which each execution decides.
        if (frameSequence.getAtom().isCountOnly()) {
            sink.attr("countStrategy").val("per key");
        }
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(functions.getOutputFunctions(), getMetadata()));
        // This plan can print inside another operator's attribute, e.g. as a scalar sub-query in an
        // aggregate, while that operator has set the sink's metadata or base-metadata mode for its
        // own columns. The plan resolves its columns against the metadata it sets itself, prints its
        // children as at the plan's root, and then gives the caller back its metadata and mode for
        // the columns that follow the sub-query.
        final RecordMetadata callerMetadata = sink.getMetadata();
        final boolean isCallerUsingBaseMetadata = sink.getUseBaseMetadata();
        sink.useBaseMetadata(false);
        try {
            sink.setMetadata(joinedMetadata);
            sink.optAttr("keyFunctions", functions.getKeyFunctions(-1));
            sink.optAttr("values", functions.getGroupByFunctions(-1));
            sink.optAttr("postJoinFilter", functions.getFilter(-1));
            sink.setMetadata(probeFactory.getMetadata());
            sink.optAttr("probeFilter", filterContext.getFilter(-1));
            sink.setMetadata(buildFactory.getMetadata());
            // A compiled build filter runs as JIT code, so its attribute says so; the operator's own
            // name speaks for the probe filter only.
            sink.optAttr(buildFilterContext.getCompiledFilter() != null ? "buildJitFilter" : "buildFilter",
                    buildFilterContext.getFilter(-1));
            sink.optAttr("buildOnFilter", buildOnFilter);
            sink.setMetadata(null);
            sink.child("Probe", probeFactory);
            sink.child("Build", buildFactory);
        } finally {
            sink.setMetadata(callerMetadata);
            sink.useBaseMetadata(isCallerUsingBaseMetadata);
        }
    }

    @Override
    public boolean usesCompiledFilter() {
        return filterContext.getCompiledFilter() != null;
    }

    /**
     * True when the build scan's WHERE filter runs as JIT code.
     */
    public boolean hasCompiledBuildFilter() {
        return buildFilterContext.getCompiledFilter() != null;
    }

    private static void aggregate(
            int workerId,
            PageFrameMemoryRecord unused,
            int frameIndex,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence,
            UnorderedPageFrameSequence<?> stealingSequence
    ) {
        final AsyncHashJoinGroupByAtom atom = (AsyncHashJoinGroupByAtom) sequence.getAtom();
        final int slotId = atom.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        try {
            final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
            final PageFrameMemoryPool pool = atom.getFilterContext().getMemoryPool(slotId);
            try {
                // Decoder initialization and map allocation must both release the acquired slot on failure.
                final PageFrameMemoryRecord probeRecord = slot.probeRecord;
                probeRecord.init(pool.navigateTo(frameIndex));
                final GroupByMapFragment fragment = atom.getFragment(slotId);
                if (atom.isSharded()) {
                    fragment.shard();
                }
                final Map map = fragment == null ? null
                        : fragment.isNotSharded() ? fragment.reopenMap() : fragment.getShards().getQuick(0);
                final long rowCount = sequence.getFrameRowCount(frameIndex);
                // One loop method per probe implementation, so that a hot call site sees one
                // receiver class. The branch below runs once per page frame, not per row.
                if (atom.isCountedByKey()) {
                    countByKey(atom, slot, probeRecord, rowCount);
                } else if (atom.isCountedByMatch()) {
                    countByMatch(atom, slot, probeRecord, rowCount);
                } else if (atom.isKeyStaged()) {
                    if (atom.isBuildPartitioned()) {
                        if (atom.isBuildUnique()) {
                            aggregateRecordPartitionedUnique(atom, slotId, probeRecord, fragment, map, rowCount);
                        } else if (!aggregateRecordPartitioned(atom, slotId, probeRecord, fragment, map, rowCount, breaker, sequence)) {
                            return;
                        }
                    } else if (atom.isBuildUnique()) {
                        aggregateRecordUnique(atom, slotId, probeRecord, fragment, map, rowCount);
                    } else if (!aggregateRecord(atom, slotId, probeRecord, fragment, map, rowCount, breaker, sequence)) {
                        return;
                    }
                } else if (atom.isSymbolKey()) {
                    if (atom.isBuildPartitioned()) {
                        if (atom.isBuildUnique()) {
                            aggregateSymbolPartitionedUnique(atom, slotId, probeRecord, fragment, map, rowCount);
                        } else if (!aggregateSymbolPartitioned(atom, slotId, probeRecord, fragment, map, rowCount, breaker, sequence)) {
                            return;
                        }
                    } else if (atom.isBuildUnique()) {
                        aggregateSymbolUnique(atom, slotId, probeRecord, fragment, map, rowCount);
                    } else if (!aggregateSymbol(atom, slotId, probeRecord, fragment, map, rowCount, breaker, sequence)) {
                        return;
                    }
                } else if (atom.isBuildPartitioned()) {
                    if (atom.isBuildUnique()) {
                        aggregateIntPartitionedUnique(atom, slotId, probeRecord, fragment, map, rowCount);
                    } else if (!aggregateIntPartitioned(atom, slotId, probeRecord, fragment, map, rowCount, breaker, sequence)) {
                        return;
                    }
                } else {
                    if (atom.isBuildUnique()) {
                        aggregateIntUnique(atom, slotId, probeRecord, fragment, map, rowCount);
                    } else if (!aggregateInt(atom, slotId, probeRecord, fragment, map, rowCount, breaker, sequence)) {
                        return;
                    }
                }
                if (fragment != null) {
                    atom.getShardingContext().maybeEnableSharding(fragment, 0);
                }
            } finally {
                pool.releaseParquetBuffers();
            }
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Returns false when cancellation cut the frame short, so the caller skips its frame epilogue.
     */
    private static boolean aggregateInt(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        // The shared reduce job checks the breaker before each frame, as for GROUP BY.
        // Join fanout can exceed a frame, so duplicate iteration repeats that check
        // once per page frame of matched pairs, counted across all rows of this frame.
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            probe.findUnchecked(probeRecord.getInt(probeKeyColumn));
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /**
     * The filtered twin of aggregateInt(), driven by the rows the filter phase kept.
     */
    private static boolean aggregateIntFiltered(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            probe.findUnchecked(probeRecord.getInt(probeKeyColumn));
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    private static void aggregateIntFilteredUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows
    ) {
        // The filtered twin of aggregateIntUnique(): same singleton probe, driven by the
        // row list the filter phase produced instead of by every row of the frame.
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            if (probe.findSingleUnchecked(probeRecord.getInt(probeKeyColumn))) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    /**
     * The twin of aggregateInt() for a build of more than one hash partition, whose probes are a class of
     * their own, so that each loop's call sites see one probe class. Returns false when cancellation
     * cut the frame short.
     */
    private static boolean aggregateIntPartitioned(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            probe.findUnchecked(probeRecord.getInt(probeKeyColumn));
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /**
     * The twin of aggregateIntFiltered() for a build of more than one hash partition, whose probes are a class of
     * their own, so that each loop's call sites see one probe class. Returns false when cancellation
     * cut the frame short.
     */
    private static boolean aggregateIntPartitionedFiltered(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            probe.findUnchecked(probeRecord.getInt(probeKeyColumn));
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /**
     * The twin of aggregateIntFilteredUnique() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static void aggregateIntPartitionedFilteredUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            if (probe.findSingleUnchecked(probeRecord.getInt(probeKeyColumn))) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    /**
     * The twin of aggregateIntUnique() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static void aggregateIntPartitionedUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            if (probe.findSingleUnchecked(probeRecord.getInt(probeKeyColumn))) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    private static void aggregateIntUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount
    ) {
        // Keep the singleton loop separate so its additional call sites do not
        // enlarge the general loop that handles arbitrarily long duplicate chains.
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            if (probe.findSingleUnchecked(probeRecord.getInt(probeKeyColumn))) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    /**
     * The staged-key twin of aggregateInt(): the slot's own key sink stages the probe row's
     * key into the build's map. Returns false when cancellation cut the frame short.
     */
    private static boolean aggregateRecord(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.RecordProbe probe = slot.recordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            probe.findUnchecked(keyRecord);
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /**
     * The filtered twin of aggregateRecord(), driven by the rows the filter phase kept.
     */
    private static boolean aggregateRecordFiltered(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.RecordProbe probe = slot.recordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            probe.findUnchecked(keyRecord);
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    private static void aggregateRecordFilteredUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.RecordProbe probe = slot.recordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final boolean isOuterJoin = atom.isOuter();
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            if (probe.findSingleUnchecked(keyRecord)) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    /**
     * The twin of aggregateRecord() for a build of more than one hash partition, whose probes are a class of
     * their own, so that each loop's call sites see one probe class. Returns false when cancellation
     * cut the frame short.
     */
    private static boolean aggregateRecordPartitioned(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.RecordProbe probe = slot.partitionedRecordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            probe.findUnchecked(keyRecord);
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /**
     * The twin of aggregateRecordFiltered() for a build of more than one hash partition, whose probes are a class
     * of their own, so that each loop's call sites see one probe class. Returns false when cancellation
     * cut the frame short.
     */
    private static boolean aggregateRecordPartitionedFiltered(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.RecordProbe probe = slot.partitionedRecordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            probe.findUnchecked(keyRecord);
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /**
     * The twin of aggregateRecordFilteredUnique() for a build of more than one hash partition; see aggregateRecordPartitioned().
     */
    private static void aggregateRecordPartitionedFilteredUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.RecordProbe probe = slot.partitionedRecordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final boolean isOuterJoin = atom.isOuter();
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            if (probe.findSingleUnchecked(keyRecord)) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    /**
     * The twin of aggregateRecordUnique() for a build of more than one hash partition; see aggregateRecordPartitioned().
     */
    private static void aggregateRecordPartitionedUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.RecordProbe probe = slot.partitionedRecordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final boolean isOuterJoin = atom.isOuter();
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            if (probe.findSingleUnchecked(keyRecord)) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    private static void aggregateRecordUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.RecordProbe probe = slot.recordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final boolean isOuterJoin = atom.isOuter();
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            if (probe.findSingleUnchecked(keyRecord)) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    /**
     * The SYMBOL twin of aggregateInt(): the same INT layout, with each probe symbol key
     * translated into the build's symbol key domain first. Returns false when cancellation cut
     * the frame short.
     */
    private static boolean aggregateSymbol(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            probe.findUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)));
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /**
     * The filtered twin of aggregateSymbol(), driven by the rows the filter phase kept.
     */
    private static boolean aggregateSymbolFiltered(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            probe.findUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)));
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    private static void aggregateSymbolFilteredUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            if (probe.findSingleUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)))) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    /**
     * The twin of aggregateSymbol() for a build of more than one hash partition, whose probes are a class of
     * their own, so that each loop's call sites see one probe class. Returns false when cancellation
     * cut the frame short.
     */
    private static boolean aggregateSymbolPartitioned(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            probe.findUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)));
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /**
     * The twin of aggregateSymbolFiltered() for a build of more than one hash partition, whose probes are a class of
     * their own, so that each loop's call sites see one probe class. Returns false when cancellation
     * cut the frame short.
     */
    private static boolean aggregateSymbolPartitionedFiltered(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            probe.findUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)));
            if (probe.hasNext()) {
                final long rowId = probeRecord.getRowId();
                record.setHasMatch(true);
                do {
                    if (--pairsUntilCheck == 0) {
                        if (isInterrupted(breaker, sequence)) {
                            return false;
                        }
                        pairsUntilCheck = pairsPerCheck;
                    }
                    probe.next();
                    rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /**
     * The twin of aggregateSymbolFilteredUnique() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static void aggregateSymbolPartitionedFilteredUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            DirectLongList rows
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            if (probe.findSingleUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)))) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    /**
     * The twin of aggregateSymbolUnique() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static void aggregateSymbolPartitionedUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            if (probe.findSingleUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)))) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    private static void aggregateSymbolUnique(
            AsyncHashJoinGroupByAtom atom,
            int slotId,
            PageFrameMemoryRecord probeRecord,
            GroupByMapFragment fragment,
            Map map,
            long rowCount
    ) {
        final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
        final HashJoinGroupByRecord record = slot.joinedRecord;
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final HashJoinGroupByRowUpdater rowUpdater = atom.getRowUpdater();
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean isOuterJoin = atom.isOuter();
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            if (probe.findSingleUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)))) {
                record.setHasMatch(true);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (isOuterJoin) {
                record.setHasMatch(false);
                rowUpdater.update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
    }

    /**
     * Adds the frame's joined pairs to the slot's count without visiting one: each probe row adds
     * the row count that the build keeps for the row's key, see
     * {@link AsyncHashJoinGroupByAtom#isCountedByKey()}. A frame bounds the rows a loop reads, so
     * the shared reduce job's check before each frame is the only cancellation check. One loop
     * method per probe implementation, as for the aggregating loops.
     */
    private static void countByKey(
            AsyncHashJoinGroupByAtom atom,
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            long rowCount
    ) {
        final long pairCount;
        if (atom.isKeyStaged()) {
            pairCount = atom.isBuildPartitioned()
                    ? countRecordPartitioned(slot, probeRecord, rowCount)
                    : countRecord(slot, probeRecord, rowCount);
        } else if (atom.isSymbolKey()) {
            pairCount = atom.isBuildPartitioned()
                    ? countSymbolPartitioned(slot, probeRecord, atom.getProbeKeyColumn(), rowCount)
                    : countSymbol(slot, probeRecord, atom.getProbeKeyColumn(), rowCount);
        } else {
            pairCount = atom.isBuildPartitioned()
                    ? countIntPartitioned(slot, probeRecord, atom.getProbeKeyColumn(), rowCount)
                    : countInt(slot, probeRecord, atom.getProbeKeyColumn(), rowCount);
        }
        atom.addPairCount(slot, pairCount);
    }

    /**
     * The filtered twin of countByKey(), driven by the rows the filter phase kept.
     */
    private static void countByKeyFiltered(
            AsyncHashJoinGroupByAtom atom,
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            DirectLongList rows
    ) {
        final long pairCount;
        if (atom.isKeyStaged()) {
            pairCount = atom.isBuildPartitioned()
                    ? countRecordPartitionedFiltered(slot, probeRecord, rows)
                    : countRecordFiltered(slot, probeRecord, rows);
        } else if (atom.isSymbolKey()) {
            pairCount = atom.isBuildPartitioned()
                    ? countSymbolPartitionedFiltered(slot, probeRecord, atom.getProbeKeyColumn(), rows)
                    : countSymbolFiltered(slot, probeRecord, atom.getProbeKeyColumn(), rows);
        } else {
            pairCount = atom.isBuildPartitioned()
                    ? countIntPartitionedFiltered(slot, probeRecord, atom.getProbeKeyColumn(), rows)
                    : countIntFiltered(slot, probeRecord, atom.getProbeKeyColumn(), rows);
        }
        atom.addPairCount(slot, pairCount);
    }

    /**
     * Adds the frame's joined pairs to the slot's count over a build of one row per key: each probe
     * row whose key the build holds adds one, see {@link AsyncHashJoinGroupByAtom#isCountedByMatch()}.
     * A frame bounds the rows a loop reads, as for countByKey(). One loop method per probe
     * implementation, as for the aggregating loops.
     */
    private static void countByMatch(
            AsyncHashJoinGroupByAtom atom,
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            long rowCount
    ) {
        final long pairCount;
        if (atom.isKeyStaged()) {
            pairCount = atom.isBuildPartitioned()
                    ? countRecordPartitionedUnique(slot, probeRecord, rowCount)
                    : countRecordUnique(slot, probeRecord, rowCount);
        } else if (atom.isSymbolKey()) {
            pairCount = atom.isBuildPartitioned()
                    ? countSymbolPartitionedUnique(slot, probeRecord, atom.getProbeKeyColumn(), rowCount)
                    : countSymbolUnique(slot, probeRecord, atom.getProbeKeyColumn(), rowCount);
        } else {
            pairCount = atom.isBuildPartitioned()
                    ? countIntPartitionedUnique(slot, probeRecord, atom.getProbeKeyColumn(), rowCount)
                    : countIntUnique(slot, probeRecord, atom.getProbeKeyColumn(), rowCount);
        }
        atom.addPairCount(slot, pairCount);
    }

    /**
     * The filtered twin of countByMatch(), driven by the rows the filter phase kept.
     */
    private static void countByMatchFiltered(
            AsyncHashJoinGroupByAtom atom,
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            DirectLongList rows
    ) {
        final long pairCount;
        if (atom.isKeyStaged()) {
            pairCount = atom.isBuildPartitioned()
                    ? countRecordPartitionedFilteredUnique(slot, probeRecord, rows)
                    : countRecordFilteredUnique(slot, probeRecord, rows);
        } else if (atom.isSymbolKey()) {
            pairCount = atom.isBuildPartitioned()
                    ? countSymbolPartitionedFilteredUnique(slot, probeRecord, atom.getProbeKeyColumn(), rows)
                    : countSymbolFilteredUnique(slot, probeRecord, atom.getProbeKeyColumn(), rows);
        } else {
            pairCount = atom.isBuildPartitioned()
                    ? countIntPartitionedFilteredUnique(slot, probeRecord, atom.getProbeKeyColumn(), rows)
                    : countIntFilteredUnique(slot, probeRecord, atom.getProbeKeyColumn(), rows);
        }
        atom.addPairCount(slot, pairCount);
    }

    /**
     * Returns the joined pairs of the frame's rows: the sum of their keys' build row counts.
     */
    private static long countInt(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            long rowCount
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findRowCountUnchecked(probeRecord.getInt(probeKeyColumn));
        }
        return pairCount;
    }

    /**
     * The filtered twin of countInt(), driven by the rows the filter phase kept.
     */
    private static long countIntFiltered(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findRowCountUnchecked(probeRecord.getInt(probeKeyColumn));
        }
        return pairCount;
    }

    /**
     * The filtered twin of countIntUnique(), driven by the rows the filter phase kept.
     */
    private static long countIntFilteredUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findSingleUnchecked(probeRecord.getInt(probeKeyColumn)) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The twin of countInt() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static long countIntPartitioned(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            long rowCount
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findRowCountUnchecked(probeRecord.getInt(probeKeyColumn));
        }
        return pairCount;
    }

    /**
     * The twin of countIntFiltered() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static long countIntPartitionedFiltered(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findRowCountUnchecked(probeRecord.getInt(probeKeyColumn));
        }
        return pairCount;
    }

    /**
     * The twin of countIntFilteredUnique() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static long countIntPartitionedFilteredUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findSingleUnchecked(probeRecord.getInt(probeKeyColumn)) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The twin of countIntUnique() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static long countIntPartitionedUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            long rowCount
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findSingleUnchecked(probeRecord.getInt(probeKeyColumn)) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The twin of countInt() for a build of one row per key: a probe row adds one pair when the
     * build holds its key.
     */
    private static long countIntUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            long rowCount
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findSingleUnchecked(probeRecord.getInt(probeKeyColumn)) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The staged-key twin of countInt(): the slot's own key sink stages the probe row's key into
     * the build's map.
     */
    private static long countRecord(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            long rowCount
    ) {
        final FrozenHashJoinBuild.RecordProbe probe = slot.recordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findRowCountUnchecked(keyRecord);
        }
        return pairCount;
    }

    /**
     * The filtered twin of countRecord(), driven by the rows the filter phase kept.
     */
    private static long countRecordFiltered(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.RecordProbe probe = slot.recordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findRowCountUnchecked(keyRecord);
        }
        return pairCount;
    }

    /**
     * The filtered twin of countRecordUnique(), driven by the rows the filter phase kept.
     */
    private static long countRecordFilteredUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.RecordProbe probe = slot.recordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findSingleUnchecked(keyRecord) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The twin of countRecord() for a build of more than one hash partition; see aggregateRecordPartitioned().
     */
    private static long countRecordPartitioned(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            long rowCount
    ) {
        final FrozenHashJoinBuild.RecordProbe probe = slot.partitionedRecordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findRowCountUnchecked(keyRecord);
        }
        return pairCount;
    }

    /**
     * The twin of countRecordFiltered() for a build of more than one hash partition; see aggregateRecordPartitioned().
     */
    private static long countRecordPartitionedFiltered(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.RecordProbe probe = slot.partitionedRecordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findRowCountUnchecked(keyRecord);
        }
        return pairCount;
    }

    /**
     * The twin of countRecordFilteredUnique() for a build of more than one hash partition; see aggregateRecordPartitioned().
     */
    private static long countRecordPartitionedFilteredUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.RecordProbe probe = slot.partitionedRecordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findSingleUnchecked(keyRecord) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The twin of countRecordUnique() for a build of more than one hash partition; see aggregateRecordPartitioned().
     */
    private static long countRecordPartitionedUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            long rowCount
    ) {
        final FrozenHashJoinBuild.RecordProbe probe = slot.partitionedRecordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findSingleUnchecked(keyRecord) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The twin of countRecord() for a build of one row per key: a probe row adds one pair when the
     * build holds its key.
     */
    private static long countRecordUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            long rowCount
    ) {
        final FrozenHashJoinBuild.RecordProbe probe = slot.recordProbe;
        // What the key sink reads: the probe record, or its SYMBOL-translating view of it.
        final Record keyRecord = slot.keyRecord;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findSingleUnchecked(keyRecord) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The SYMBOL twin of countInt(): the same INT layout, with each probe symbol key translated
     * into the build's symbol key domain first.
     */
    private static long countSymbol(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            long rowCount
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findRowCountUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)));
        }
        return pairCount;
    }

    /**
     * The filtered twin of countSymbol(), driven by the rows the filter phase kept.
     */
    private static long countSymbolFiltered(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findRowCountUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)));
        }
        return pairCount;
    }

    /**
     * The filtered twin of countSymbolUnique(), driven by the rows the filter phase kept.
     */
    private static long countSymbolFilteredUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findSingleUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn))) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The twin of countSymbol() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static long countSymbolPartitioned(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            long rowCount
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findRowCountUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)));
        }
        return pairCount;
    }

    /**
     * The twin of countSymbolFiltered() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static long countSymbolPartitionedFiltered(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findRowCountUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn)));
        }
        return pairCount;
    }

    /**
     * The twin of countSymbolFilteredUnique() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static long countSymbolPartitionedFilteredUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            DirectLongList rows
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        long pairCount = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            pairCount += probe.findSingleUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn))) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The twin of countSymbolUnique() for a build of more than one hash partition; see aggregateIntPartitioned().
     */
    private static long countSymbolPartitionedUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            long rowCount
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.partitionedIntProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findSingleUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn))) ? 1 : 0;
        }
        return pairCount;
    }

    /**
     * The twin of countSymbol() for a build of one row per key: a probe row adds one pair when the
     * build holds its key.
     */
    private static long countSymbolUnique(
            AsyncHashJoinGroupByAtom.Slot slot,
            PageFrameMemoryRecord probeRecord,
            int probeKeyColumn,
            long rowCount
    ) {
        final FrozenHashJoinBuild.IntProbe probe = slot.intProbe;
        final SymbolKeyTranslator.View translator = slot.symbolKeyView;
        long pairCount = 0;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            pairCount += probe.findSingleUnchecked(translator.translate(probeRecord.getInt(probeKeyColumn))) ? 1 : 0;
        }
        return pairCount;
    }

    private static void filterAndAggregate(
            int workerId,
            PageFrameMemoryRecord unused,
            int frameIndex,
            SqlExecutionCircuitBreaker breaker,
            UnorderedPageFrameSequence<?> sequence,
            UnorderedPageFrameSequence<?> stealingSequence
    ) {
        final AsyncHashJoinGroupByAtom atom = (AsyncHashJoinGroupByAtom) sequence.getAtom();
        final int slotId = atom.maybeAcquire(workerId, stealingSequence == sequence, breaker);
        try {
            final AsyncHashJoinGroupByAtom.Slot slot = atom.getSlot(slotId);
            final AsyncFilterContext filterCtx = atom.getFilterContext();
            final PageFrameMemoryPool pool = filterCtx.getMemoryPool(slotId);
            try {
                // Decoder initialization and map allocation must both release the acquired slot on failure.
                final PageFrameMemoryRecord probeRecord = slot.probeRecord;
                final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                probeRecord.init(frameMemory);
                final long rowCount = sequence.getFrameRowCount(frameIndex);
                // Phase one: narrow the frame to the rows the probe filter keeps. The compiled
                // filter reads raw column addresses, so column tops and Parquet type casts, which
                // the logical record resolves per row, fall back to the interpreted filter.
                final DirectLongList rows = filterCtx.getFilteredRows(slotId);
                rows.clear();
                final CompiledFilter compiledFilter = filterCtx.getCompiledFilter();
                if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.hasColumnTypeCasts()) {
                    AsyncFilterUtils.applyFilter(filterCtx.getFilter(slotId), rows, probeRecord, rowCount);
                } else {
                    AsyncFilterUtils.applyCompiledFilter(
                            compiledFilter,
                            filterCtx.getBindVarMemory(),
                            filterCtx.getBindVarFunctions(),
                            frameMemory,
                            sequence.getPageFrameAddressCache(),
                            filterCtx.getDataAddresses(slotId),
                            filterCtx.getAuxAddresses(slotId),
                            rows,
                            rowCount
                    );
                }
                // Phase two: join and aggregate the surviving rows.
                final GroupByMapFragment fragment = atom.getFragment(slotId);
                if (atom.isSharded()) {
                    fragment.shard();
                }
                final Map map = fragment == null ? null
                        : fragment.isNotSharded() ? fragment.reopenMap() : fragment.getShards().getQuick(0);
                // One loop method per probe implementation, so that a hot call site sees one
                // receiver class. The branch below runs once per page frame, not per row.
                if (atom.isCountedByKey()) {
                    countByKeyFiltered(atom, slot, probeRecord, rows);
                } else if (atom.isCountedByMatch()) {
                    countByMatchFiltered(atom, slot, probeRecord, rows);
                } else if (atom.isKeyStaged()) {
                    if (atom.isBuildPartitioned()) {
                        if (atom.isBuildUnique()) {
                            aggregateRecordPartitionedFilteredUnique(atom, slotId, probeRecord, fragment, map, rows);
                        } else if (!aggregateRecordPartitionedFiltered(atom, slotId, probeRecord, fragment, map, rows, breaker, sequence)) {
                            return;
                        }
                    } else if (atom.isBuildUnique()) {
                        aggregateRecordFilteredUnique(atom, slotId, probeRecord, fragment, map, rows);
                    } else if (!aggregateRecordFiltered(atom, slotId, probeRecord, fragment, map, rows, breaker, sequence)) {
                        return;
                    }
                } else if (atom.isSymbolKey()) {
                    if (atom.isBuildPartitioned()) {
                        if (atom.isBuildUnique()) {
                            aggregateSymbolPartitionedFilteredUnique(atom, slotId, probeRecord, fragment, map, rows);
                        } else if (!aggregateSymbolPartitionedFiltered(atom, slotId, probeRecord, fragment, map, rows, breaker, sequence)) {
                            return;
                        }
                    } else if (atom.isBuildUnique()) {
                        aggregateSymbolFilteredUnique(atom, slotId, probeRecord, fragment, map, rows);
                    } else if (!aggregateSymbolFiltered(atom, slotId, probeRecord, fragment, map, rows, breaker, sequence)) {
                        return;
                    }
                } else if (atom.isBuildPartitioned()) {
                    if (atom.isBuildUnique()) {
                        aggregateIntPartitionedFilteredUnique(atom, slotId, probeRecord, fragment, map, rows);
                    } else if (!aggregateIntPartitionedFiltered(atom, slotId, probeRecord, fragment, map, rows, breaker, sequence)) {
                        return;
                    }
                } else {
                    if (atom.isBuildUnique()) {
                        aggregateIntFilteredUnique(atom, slotId, probeRecord, fragment, map, rows);
                    } else if (!aggregateIntFiltered(atom, slotId, probeRecord, fragment, map, rows, breaker, sequence)) {
                        return;
                    }
                }
                if (fragment != null) {
                    atom.getShardingContext().maybeEnableSharding(fragment, 0);
                }
            } finally {
                pool.releaseParquetBuffers();
            }
        } finally {
            atom.release(slotId);
        }
    }

    // Repeats UnorderedPageFrameReduceJob.reduce()'s frame-boundary check inside a frame.
    private static boolean isInterrupted(SqlExecutionCircuitBreaker breaker, UnorderedPageFrameSequence<?> sequence) {
        if (!sequence.isActive()) {
            return true;
        }
        if (sequence.isUninterruptible()) {
            return false;
        }
        final int state = breaker.getState(sequence.getStartTime(), sequence.getCircuitBreaker().getFd());
        if (state != SqlExecutionCircuitBreaker.STATE_OK) {
            sequence.cancel(state);
            return true;
        }
        return false;
    }

    // Filters and projections wrap the table scan, so the walk stops at the first page frame factory.
    private static boolean isIntervalScan(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof PageFrameRecordCursorFactory frames) {
                return frames.isIntervalScan();
            }
        }
        return false;
    }

    @Override
    protected void _close() {
        Throwable failure = Misc.freeBestEffort(null, cursor);
        cursor = null;
        failure = Misc.freeBestEffort(failure, frameSequence);
        frameSequence = null;
        failure = Misc.freeBestEffort(failure, filterContext);
        filterContext = null;
        failure = Misc.freeBestEffort(failure, buildFilterContext);
        buildFilterContext = null;
        failure = Misc.freeBestEffort(failure, buildOnFilter);
        buildOnFilter = null;
        failure = Misc.freeObjListBestEffort(failure, workerBuildOnFilters);
        workerBuildOnFilters = null;
        failure = Misc.freeBestEffort(failure, functions);
        functions = null;
        failure = Misc.freeBestEffort(failure, probeFactory);
        probeFactory = null;
        failure = Misc.freeBestEffort(failure, buildFactory);
        buildFactory = null;
        CairoException.rethrowCleanupFailure(failure);
    }
}

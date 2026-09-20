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
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
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
import io.questdb.griffin.model.IQueryModel;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * Keyed and scalar shared-build execution the planner selects for eligible join aggregations.
 * Takes ownership of both child factories, functions and the probe filter context
 * on entry, including construction failure. The context may carry a JIT-compiled
 * probe filter alongside the interpreted one. Borrows metadata only during
 * construction. Callers must compile functions for the same worker count.
 * The atom borrows the build factory and the frame sequence closes the atom first.
 */
public final class AsyncHashJoinGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final UnorderedPageFrameReducer AGGREGATE = AsyncHashJoinGroupByRecordCursorFactory::aggregate;
    private static final UnorderedPageFrameReducer FILTER_AND_AGGREGATE = AsyncHashJoinGroupByRecordCursorFactory::filterAndAggregate;
    private final String condition;
    private final boolean inputSwapped;
    private final boolean isSymbolKey;
    private final RecordMetadata joinedMetadata;
    private final int logicalJoinType;
    private final boolean outer;
    private final int workerCount;
    private RecordCursorFactory buildFactory;
    private AsyncHashJoinGroupByRecordCursor cursor;
    private AsyncFilterContext filterContext;
    private UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence;
    private HashJoinGroupByFunctions functions;
    private RecordCursorFactory probeFactory;

    @TestOnly
    public AsyncHashJoinGroupByRecordCursorFactory(
            CairoEngine engine,
            RecordCursorFactory probeFactory,
            RecordCursorFactory buildFactory,
            HashJoinGroupByMetadata metadata,
            HashJoinGroupByFunctions functions,
            AsyncFilterContext filterContext,
            boolean outer,
            int workerCount
    ) {
        this(engine, probeFactory, buildFactory, metadata, functions, filterContext, outer, workerCount,
                outer ? IQueryModel.JOIN_LEFT_OUTER : IQueryModel.JOIN_INNER, false);
    }

    public AsyncHashJoinGroupByRecordCursorFactory(
            CairoEngine engine,
            RecordCursorFactory probeFactory,
            RecordCursorFactory buildFactory,
            HashJoinGroupByMetadata metadata,
            HashJoinGroupByFunctions functions,
            AsyncFilterContext filterContext,
            boolean outer,
            int workerCount,
            int logicalJoinType,
            boolean inputSwapped
    ) {
        super(functions.getOutputMetadata());
        this.probeFactory = probeFactory;
        this.buildFactory = buildFactory;
        this.functions = functions;
        this.filterContext = filterContext;
        this.outer = outer;
        this.workerCount = workerCount;
        this.logicalJoinType = logicalJoinType;
        this.inputSwapped = inputSwapped;
        this.isSymbolKey = metadata.isSymbolKey();
        try {
            this.joinedMetadata = GenericRecordMetadata.copyOf(metadata.getJoinedMetadata());
            this.condition = metadata.getCondition();
            if (workerCount < 1 || functions.getWorkerCount() != workerCount
                    || !probeFactory.supportsPageFrameCursor()) {
                throw new IllegalArgumentException("unsupported fused hash join execution inputs");
            }
            AsyncHashJoinGroupByAtom atom = new AsyncHashJoinGroupByAtom(engine, buildFactory, metadata,
                    functions, filterContext, outer, workerCount);
            // A probe filter belongs to the query, not to a frame, so the reducer is fixed here:
            // queries without one keep the dense loops that never test a filter per row.
            final UnorderedPageFrameReducer reducer = filterContext.getFilter(-1) != null
                    ? FILTER_AND_AGGREGATE : AGGREGATE;
            // The sequence takes atom ownership on entry, also on constructor failure.
            frameSequence = new UnorderedPageFrameSequence<>(engine, engine.getConfiguration(),
                    engine.getMessageBus(), atom, reducer, workerCount);
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
            // The atom builds once the probe frame cursor is open; see AsyncHashJoinGroupByAtom.init().
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
        sink.attr("physicalJoinType").val(outer ? "left outer" : "inner");
        sink.attr("inputSwapped").val(inputSwapped);
        sink.attr("condition").val(condition);
        if (isSymbolKey) {
            sink.attr("symbolKeyJoin").val(true);
        }
        sink.attr("buildStrategy").val("shared");
        if (!functions.isKeyed()) {
            sink.attr("aggregation").val("scalar");
        }
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(functions.getOutputFunctions(), getMetadata()));
        sink.setMetadata(joinedMetadata);
        try {
            sink.optAttr("keyFunctions", functions.getKeyFunctions(-1));
            sink.optAttr("values", functions.getGroupByFunctions(-1));
            sink.optAttr("postJoinFilter", functions.getFilter(-1));
        } finally {
            sink.setMetadata(null);
        }
        sink.setMetadata(probeFactory.getMetadata());
        try {
            sink.optAttr("probeFilter", filterContext.getFilter(-1));
        } finally {
            sink.setMetadata(null);
        }
        sink.child("Probe", probeFactory);
        sink.child("Build", buildFactory);
    }

    @Override
    public boolean usesCompiledFilter() {
        return filterContext.getCompiledFilter() != null;
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
                if (atom.isKeyStaged()) {
                    if (atom.isBuildUnique()) {
                        aggregateRecordUnique(atom, slotId, probeRecord, fragment, map, rowCount);
                    } else if (!aggregateRecord(atom, slotId, probeRecord, fragment, map, rowCount, breaker, sequence)) {
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

    /** Returns false when cancellation cut the frame short, so the caller skips its frame epilogue. */
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
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean outer = atom.isOuter();
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
                    update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (outer) {
                record.setHasMatch(false);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /** The filtered twin of aggregateInt(), driven by the rows the filter phase kept. */
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
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean outer = atom.isOuter();
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
                    update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (outer) {
                record.setHasMatch(false);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
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
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean outer = atom.isOuter();
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            if (probe.findSingleUnchecked(probeRecord.getInt(probeKeyColumn))) {
                record.setHasMatch(true);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (outer) {
                record.setHasMatch(false);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
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
        final int probeKeyColumn = atom.getProbeKeyColumn();
        final boolean outer = atom.isOuter();
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            if (probe.findSingleUnchecked(probeRecord.getInt(probeKeyColumn))) {
                record.setHasMatch(true);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (outer) {
                record.setHasMatch(false);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
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
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final boolean outer = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            probe.findUnchecked(probeRecord);
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
                    update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (outer) {
                record.setHasMatch(false);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
        return true;
    }

    /** The filtered twin of aggregateRecord(), driven by the rows the filter phase kept. */
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
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final boolean outer = atom.isOuter();
        final long pairsPerCheck = atom.getPairsPerCheck();
        long pairsUntilCheck = pairsPerCheck;
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            probe.findUnchecked(probeRecord);
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
                    update(slot, fragment, map, sink, updater, record, postJoinFilter, rowId);
                } while (probe.hasNext());
            } else if (outer) {
                record.setHasMatch(false);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
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
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final boolean outer = atom.isOuter();
        for (long p = 0, n = rows.size(); p < n; p++) {
            probeRecord.setRowIndex(rows.get(p));
            if (probe.findSingleUnchecked(probeRecord)) {
                record.setHasMatch(true);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (outer) {
                record.setHasMatch(false);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
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
        final HashJoinGroupByFunctions functions = atom.getFunctions();
        final RecordSink sink = functions.getMapSink(slotId);
        final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
        final Function postJoinFilter = functions.getFilter(slotId);
        final boolean outer = atom.isOuter();
        for (long r = 0; r < rowCount; r++) {
            probeRecord.setRowIndex(r);
            if (probe.findSingleUnchecked(probeRecord)) {
                record.setHasMatch(true);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            } else if (outer) {
                record.setHasMatch(false);
                update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
            }
        }
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
                if (atom.isKeyStaged()) {
                    if (atom.isBuildUnique()) {
                        aggregateRecordFilteredUnique(atom, slotId, probeRecord, fragment, map, rows);
                    } else if (!aggregateRecordFiltered(atom, slotId, probeRecord, fragment, map, rows, breaker, sequence)) {
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

    private static void update(AsyncHashJoinGroupByAtom.Slot slot, GroupByMapFragment fragment, Map map, RecordSink sink, GroupByFunctionsUpdater updater,
                               HashJoinGroupByRecord record, Function filter, long rowId) {
        if (filter == null || filter.getBool(record)) {
            final MapValue value;
            if (slot.value != null) {
                value = slot.value;
            } else {
                MapKey key = map.withKey();
                sink.copy(record, key);
                if (fragment.isNotSharded()) {
                    value = key.createValue();
                } else {
                    key.commit();
                    final long hashCode = key.hash();
                    final Map shard = fragment.getShardMap(hashCode);
                    if (shard != map) {
                        MapKey shardKey = shard.withKey();
                        shardKey.copyFrom(key);
                        value = shardKey.createValue(hashCode);
                    } else {
                        value = key.createValue(hashCode);
                    }
                }
            }
            if (value.isNew()) {
                updater.updateNew(value, record, rowId);
                if (slot.value != null) {
                    slot.value.setNew(false);
                }
            } else {
                updater.updateExisting(value, record, rowId);
            }
        }
    }

    @Override
    protected void _close() {
        Throwable failure = Misc.freeBestEffort(null, cursor);
        cursor = null;
        failure = Misc.freeBestEffort(failure, frameSequence);
        frameSequence = null;
        failure = Misc.freeBestEffort(failure, filterContext);
        filterContext = null;
        failure = Misc.freeBestEffort(failure, functions);
        functions = null;
        failure = Misc.freeBestEffort(failure, probeFactory);
        probeFactory = null;
        failure = Misc.freeBestEffort(failure, buildFactory);
        buildFactory = null;
        CairoException.rethrowCleanupFailure(failure);
    }
}

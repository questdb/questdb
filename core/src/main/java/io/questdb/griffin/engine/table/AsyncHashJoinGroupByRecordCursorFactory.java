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
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
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
import io.questdb.std.Misc;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * Keyed and scalar shared-build execution selected by the experimental planner gate.
 * Takes ownership of both child factories, functions and the interpreted probe
 * filter context on entry, including construction failure. Borrows metadata only
 * during construction. Callers must compile functions for the same worker count.
 */
public final class AsyncHashJoinGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private final String condition;
    private final boolean inputSwapped;
    private final RecordMetadata joinedMetadata;
    private final int logicalJoinType;
    private final HashJoinGroupByMetrics metrics = new HashJoinGroupByMetrics();
    private final boolean outer;
    private final int workerCount;
    private RecordCursorFactory buildFactory;
    private AsyncHashJoinGroupByRecordCursor cursor;
    private AsyncFilterContext filterContext;
    private UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence;
    private HashJoinGroupByFunctions functions;
    private RecordCursorFactory probeFactory;

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
        try {
            this.joinedMetadata = GenericRecordMetadata.copyOf(metadata.getJoinedMetadata());
            this.condition = metadata.getCondition();
            if (workerCount < 1 || functions.getWorkerCount() != workerCount
                    || !probeFactory.supportsPageFrameCursor()
                    || filterContext.getCompiledFilter() != null) {
                throw new IllegalArgumentException("unsupported fused hash join execution inputs");
            }
            AsyncHashJoinGroupByAtom atom = new AsyncHashJoinGroupByAtom(engine, metadata,
                    functions, filterContext, outer, workerCount);
            // The sequence takes atom ownership on entry, also on constructor failure.
            frameSequence = new UnorderedPageFrameSequence<>(engine, engine.getConfiguration(),
                    engine.getMessageBus(), atom, AsyncHashJoinGroupByRecordCursorFactory::aggregate, workerCount);
            cursor = new AsyncHashJoinGroupByRecordCursor(engine, frameSequence, functions, metrics);
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
        metrics.clear();
        cursor.open(executionContext.getCircuitBreaker());
        try {
            executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottled();
            long start = System.nanoTime();
            try (RecordCursor buildCursor = buildFactory.getCursor(executionContext)) {
                frameSequence.getAtom().build(buildCursor, executionContext, metrics);
            }
            metrics.buildNanos = System.nanoTime() - start;
            start = System.nanoTime();
            final int order = probeFactory.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
            frameSequence.of(probeFactory, executionContext, order);
            metrics.initNanos = System.nanoTime() - start;
            return cursor;
        } catch (Throwable th) {
            Misc.free(cursor, th);
            throw th;
        }
    }

    /** Last execution's counters, retained through cursor close and reset on acquisition. */
    public HashJoinGroupByMetrics getMetrics() {
        return metrics;
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
        sink.type("Async Hash Join Group By");
        sink.meta("workers").val(workerCount);
        sink.attr("logicalJoinType").val(logicalJoinType == IQueryModel.JOIN_RIGHT_OUTER ? "right outer"
                : logicalJoinType == IQueryModel.JOIN_LEFT_OUTER ? "left outer" : "inner");
        sink.attr("physicalJoinType").val(outer ? "left outer" : "inner");
        sink.attr("inputSwapped").val(inputSwapped);
        sink.attr("condition").val(condition);
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
                final HashJoinGroupByRecord record = slot.joinedRecord;
                final FrozenHashJoinBuild.Probe probe = slot.probe;
                final HashJoinGroupByFunctions functions = atom.getFunctions();
                final RecordSink sink = functions.getMapSink(slotId);
                final GroupByFunctionsUpdater updater = functions.getUpdater(slotId);
                final GroupByMapFragment fragment = atom.getFragment(slotId);
                if (atom.isSharded()) {
                    fragment.shard(breaker);
                }
                final Map map = fragment == null ? null
                        : fragment.isNotSharded() ? fragment.reopenMap() : fragment.getShards().getQuick(0);
                final Function probeFilter = atom.getFilterContext().getFilter(slotId);
                final Function postJoinFilter = functions.getFilter(slotId);
                for (long r = 0, n = sequence.getFrameRowCount(frameIndex); r < n && sequence.isActive(); r++) {
                    breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    slot.scannedRows++;
                    probeRecord.setRowIndex(r);
                    if (probeFilter != null && !probeFilter.getBool(probeRecord)) {
                        continue;
                    }
                    probe.find(probeRecord.getInt(atom.getProbeKeyColumn()));
                    if (probe.hasNext()) {
                        record.setHasMatch(true);
                        do {
                            breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                            if (!sequence.isActive()) {
                                return;
                            }
                            probe.next();
                            slot.matchedPairs++;
                            update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
                        } while (probe.hasNext());
                    } else if (atom.isOuter()) {
                        record.setHasMatch(false);
                        slot.nullExtendedRows++;
                        update(slot, fragment, map, sink, updater, record, postJoinFilter, probeRecord.getRowId());
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

    private static void update(AsyncHashJoinGroupByAtom.Slot slot, GroupByMapFragment fragment, Map map, RecordSink sink, GroupByFunctionsUpdater updater,
                               HashJoinGroupByRecord record, Function filter, long rowId) {
        if (filter == null || filter.getBool(record)) {
            slot.survivingRows++;
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

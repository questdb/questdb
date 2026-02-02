/*******************************************************************************
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

/**
 * Factory for parallel non-keyed markout horizon query execution using PageFrameSequence.
 * <p>
 * Produces a single output row by aggregating all master rows with their ASOF-joined slave rows
 * across all sequence offsets.
 */
public class AsyncHorizonJoinNotKeyedRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer FILTER_AND_REDUCE = AsyncHorizonJoinNotKeyedRecordCursorFactory::filterAndReduce;
    private static final PageFrameReducer REDUCE = AsyncHorizonJoinNotKeyedRecordCursorFactory::reduce;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncHorizonJoinNotKeyedRecordCursor cursor;
    private final PageFrameSequence<AsyncHorizonJoinNotKeyedAtom> frameSequence;
    private final ObjList<GroupByFunction> groupByFunctions;
    // Combined metadata (master + sequence + slave) used for GROUP BY function column references in toPlan
    private final RecordMetadata markoutMetadata;
    private final RecordCursorFactory masterFactory;
    // Pre-computed offset values (in microseconds)
    private final LongList offsets;
    private final RecordCursorFactory slaveFactory;
    private final int workerCount;

    public AsyncHorizonJoinNotKeyedRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull CairoEngine engine,
            @NotNull MessageBus messageBus,
            @NotNull RecordMetadata metadata,
            @NotNull RecordMetadata markoutMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            @NotNull LongList offsets,
            int masterTimestampColumnIndex,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            int valueCount,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterKeyCopier,
            @Nullable RecordSink slaveKeyCopier,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory
    ) {
        super(metadata);
        try {
            this.markoutMetadata = markoutMetadata;
            this.masterFactory = masterFactory;
            this.slaveFactory = slaveFactory;
            this.offsets = offsets;
            this.groupByFunctions = groupByFunctions;
            this.workerCount = workerCount;

            // Compute timestamp scale factors for cross-resolution support
            final int masterTsType = masterFactory.getMetadata().getTimestampType();
            final int slaveTsType = slaveFactory.getMetadata().getTimestampType();
            long masterTsScale = 1;
            long slaveTsScale = 1;
            if (masterTsType != slaveTsType) {
                masterTsScale = ColumnType.getTimestampDriver(masterTsType).toNanosScale();
                slaveTsScale = ColumnType.getTimestampDriver(slaveTsType).toNanosScale();
            }

            final AsyncHorizonJoinNotKeyedAtom atom = new AsyncHorizonJoinNotKeyedAtom(
                    asm,
                    configuration,
                    slaveFactory,
                    masterTimestampColumnIndex,
                    offsets,
                    valueCount,
                    asOfJoinKeyTypes,
                    masterKeyCopier,
                    slaveKeyCopier,
                    columnSources,
                    columnIndexes,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    filter,
                    perWorkerFilters,
                    masterTsScale,
                    slaveTsScale,
                    workerCount
            );

            this.frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    filter != null ? FILTER_AND_REDUCE : REDUCE,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_GROUP_BY_NOT_KEYED
            );

            this.cursor = new AsyncHorizonJoinNotKeyedRecordCursor(
                    groupByFunctions,
                    slaveFactory
            );
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public PageFrameSequence<AsyncHorizonJoinNotKeyedAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(masterFactory, executionContext, collectSubSeq, order);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return masterFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(execute(executionContext, collectSubSeq, ORDER_ASC), executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (usesCompiledFilter()) {
            sink.type("Async JIT Horizon Join");
        } else {
            sink.type("Async Horizon Join");
        }
        sink.meta("workers").val(workerCount);
        sink.meta("offsets").val(offsets.size());
        // GroupByFunctions reference columns from the combined markout metadata (master + sequence + slave)
        sink.setMetadata(markoutMetadata);
        sink.optAttr("values", frameSequence.getAtom().getOwnerGroupByFunctions());
        sink.setMetadata(null);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getCompiledFilter() != null;
    }

    private static void aggregateRecord(
            MarkoutRecord markoutRecord,
            long masterRowId,
            SimpleMapValue value,
            GroupByFunctionsUpdater functionUpdater
    ) {
        if (value.isNew()) {
            functionUpdater.updateNew(value, markoutRecord, masterRowId);
            value.setNew(false);
        } else {
            functionUpdater.updateExisting(value, markoutRecord, masterRowId);
        }
    }

    /**
     * Page frame reducer for filtered non-keyed markout GROUP BY.
     * <p>
     * Applies filter first, then uses sorted horizon timestamp iteration for efficient
     * sequential ASOF lookups on filtered rows. Aggregates results into a single value.
     * Supports both keyed and timestamp-only ASOF JOIN modes.
     */
    private static void filterAndReduce(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        if (frameRowCount == 0) {
            return;
        }

        final PageFrameSequence<AsyncHorizonJoinNotKeyedAtom> frameSequence = task.getFrameSequence(AsyncHorizonJoinNotKeyedAtom.class);
        final AsyncHorizonJoinNotKeyedAtom atom = frameSequence.getAtom();

        final long sequenceRowCount = atom.getSequenceRowCount();
        if (sequenceRowCount == 0) {
            return;
        }

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        try {
            final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
            final SimpleMapValue value = atom.getMapValue(slotId);
            final int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
            final MarkoutRecord markoutRecord = atom.getCombinedRecord(slotId);
            final CompiledFilter compiledFilter = atom.getCompiledFilter();
            final Function filter = atom.getFilter(slotId);

            // Apply filter to master rows
            final DirectLongList rows = task.getFilteredRows();
            rows.clear();
            if (compiledFilter == null || frameMemory.hasColumnTops()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(compiledFilter, atom.getBindVarMemory(), atom.getBindVarFunctions(), task);
            }

            final long filteredRowCount = rows.size();
            if (filteredRowCount == 0) {
                return;
            }

            // Get ASOF join resources
            final MarkoutTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
            final Map asOfJoinMap = atom.getAsOfJoinMap(slotId);  // Cache: joinKey -> rowId
            final RecordSink masterKeyCopier = atom.getMasterKeyCopier();
            final RecordSink slaveKeyCopier = atom.getSlaveKeyCopier();
            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            // Get horizon timestamp iterator and initialize for filtered rows
            final HorizonTimestampIterator horizonIterator = atom.getHorizonIterator(slotId);
            record.setRowIndex(0);
            long baseRowId = record.getRowId();
            horizonIterator.ofFiltered(record, rows, masterTimestampColumnIndex);

            // Process horizon timestamps in sorted order for sequential ASOF lookups
            processSortedHorizonTimestamps(
                    horizonIterator,
                    record,
                    baseRowId,
                    atom,
                    slaveTimeFrameHelper,
                    asOfJoinMap,
                    masterKeyCopier,
                    slaveKeyCopier,
                    slaveRecord,
                    markoutRecord,
                    value,
                    functionUpdater,
                    circuitBreaker
            );
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Process all horizon timestamps in sorted order using bidirectional scanning.
     * <p>
     * This method iterates through pre-sorted (horizonTs, masterRowIdx, offsetIdx) tuples.
     * It uses a "dense ASOF" approach optimized for the common case where most keys
     * appear in the "recent" slave rows:
     * <p>
     * 1. First tuple: Find ASOF position, backward scan until key match, set watermarks
     * 2. Subsequent tuples: Forward scan to new ASOF position (caching keys),
     *    then lookup in cache. On cache miss, continue backward scan.
     * <p>
     * Each slave row is scanned at most once per frame (either forward or backward).
     * Watermarks are tracked internally by the helper and reset via toTop().
     */
    private static void processSortedHorizonTimestamps(
            HorizonTimestampIterator horizonIterator,
            PageFrameMemoryRecord masterRecord,
            long baseRowId,
            AsyncHorizonJoinNotKeyedAtom atom,
            MarkoutTimeFrameHelper slaveTimeFrameHelper,
            Map asOfJoinMap,
            RecordSink masterKeyCopier,
            RecordSink slaveKeyCopier,
            Record slaveRecord,
            MarkoutRecord markoutRecord,
            SimpleMapValue value,
            GroupByFunctionsUpdater functionUpdater,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final boolean keyedAsOfJoin = asOfJoinMap != null && masterKeyCopier != null && slaveKeyCopier != null;

        // Reset helper state and clear the ASOF join map for this frame
        slaveTimeFrameHelper.toTop();
        if (keyedAsOfJoin) {
            asOfJoinMap.clear();
        }

        while (horizonIterator.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            horizonIterator.next();

            final long horizonTs = horizonIterator.getHorizonTimestamp();
            final long masterRowIdx = horizonIterator.getMasterRowIndex();
            final int offsetIdx = horizonIterator.getOffsetIndex();
            final long offset = atom.getOffset(offsetIdx);

            // Position master record at the correct row
            masterRecord.setRowIndex(masterRowIdx);
            final long masterRowId = baseRowId + masterRowIdx;

            // Find ASOF row for this horizon timestamp (sequential due to sorted iteration)
            long asOfRowId = slaveTimeFrameHelper.findAsOfRow(horizonTs);

            long matchRowId = Long.MIN_VALUE;
            if (keyedAsOfJoin) {
                // Keyed ASOF JOIN with bidirectional scanning
                if (asOfRowId != Long.MIN_VALUE) {
                    if (slaveTimeFrameHelper.getForwardWatermark() == Long.MIN_VALUE) {
                        // First tuple: backward scan from ASOF position to find matching key
                        matchRowId = slaveTimeFrameHelper.backwardScanForKeyMatch(
                                asOfRowId,
                                masterRecord,
                                masterKeyCopier,
                                slaveKeyCopier,
                                asOfJoinMap
                        );
                        // Initialize forward watermark to ASOF position for subsequent forward scans
                        slaveTimeFrameHelper.initForwardWatermark(asOfRowId);
                    } else {
                        // Subsequent tuples: forward scan first (updates internal watermark)
                        slaveTimeFrameHelper.forwardScanToPosition(
                                asOfRowId,
                                slaveKeyCopier,
                                asOfJoinMap
                        );

                        // Look up the key in the cache
                        MapKey cacheKey = asOfJoinMap.withKey();
                        cacheKey.put(masterRecord, masterKeyCopier);
                        MapValue cacheValue = cacheKey.findValue();

                        if (cacheValue != null) {
                            matchRowId = cacheValue.getLong(0);
                        } else {
                            // Cache miss: continue backward scan (uses internal watermark)
                            matchRowId = slaveTimeFrameHelper.backwardScanForKeyMatch(
                                    asOfRowId,
                                    masterRecord,
                                    masterKeyCopier,
                                    slaveKeyCopier,
                                    asOfJoinMap
                            );
                        }
                    }
                }
            } else {
                // Timestamp-only ASOF JOIN: ASOF row IS the match
                matchRowId = asOfRowId;
            }

            // Aggregate the result
            Record matchedSlaveRecord = null;
            if (matchRowId != Long.MIN_VALUE) {
                slaveTimeFrameHelper.recordAt(matchRowId);
                matchedSlaveRecord = slaveRecord;
            }
            markoutRecord.of(masterRecord, offset, matchedSlaveRecord);
            aggregateRecord(markoutRecord, masterRowId, value, functionUpdater);
        }
    }

    /**
     * Page frame reducer for non-keyed markout GROUP BY.
     * <p>
     * Uses sorted horizon timestamp iteration for efficient sequential ASOF lookups.
     * For each (horizonTs, masterRowIdx, offsetIdx) tuple in sorted order,
     * performs ASOF JOIN lookup and aggregates results into a single value.
     * Supports both keyed and timestamp-only ASOF JOIN modes.
     */
    private static void reduce(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = task.getFrameRowCount();
        if (frameRowCount == 0) {
            return;
        }

        final PageFrameSequence<AsyncHorizonJoinNotKeyedAtom> frameSequence = task.getFrameSequence(AsyncHorizonJoinNotKeyedAtom.class);
        final AsyncHorizonJoinNotKeyedAtom atom = frameSequence.getAtom();

        final long sequenceRowCount = atom.getSequenceRowCount();
        if (sequenceRowCount == 0) {
            return;
        }

        final PageFrameMemory frameMemory = task.populateFrameMemory();
        record.init(frameMemory);

        final boolean owner = stealingFrameSequence != null && stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        try {
            final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
            final SimpleMapValue value = atom.getMapValue(slotId);
            final int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
            final MarkoutRecord markoutRecord = atom.getCombinedRecord(slotId);

            // Get ASOF join resources
            final MarkoutTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
            final Map asOfJoinMap = atom.getAsOfJoinMap(slotId);  // Cache: joinKey -> rowId
            final RecordSink masterKeyCopier = atom.getMasterKeyCopier();
            final RecordSink slaveKeyCopier = atom.getSlaveKeyCopier();
            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            // Get horizon timestamp iterator and initialize for this frame
            final HorizonTimestampIterator horizonIterator = atom.getHorizonIterator(slotId);
            record.setRowIndex(0);
            long baseRowId = record.getRowId();
            horizonIterator.of(record, 0, frameRowCount, masterTimestampColumnIndex);

            // Process horizon timestamps in sorted order for sequential ASOF lookups
            processSortedHorizonTimestamps(
                    horizonIterator,
                    record,
                    baseRowId,
                    atom,
                    slaveTimeFrameHelper,
                    asOfJoinMap,
                    masterKeyCopier,
                    slaveKeyCopier,
                    slaveRecord,
                    markoutRecord,
                    value,
                    functionUpdater,
                    circuitBreaker
            );
        } finally {
            atom.release(slotId);
        }
    }

    @Override
    protected void _close() {
        Misc.free(frameSequence);
        Misc.free(cursor);
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.freeObjListAndClear(groupByFunctions);
        Misc.freeIfCloseable(markoutMetadata);
    }
}

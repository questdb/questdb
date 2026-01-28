/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleRecordSink;
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
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
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
import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

/**
 * Factory for parallel markout horizon query execution using PageFrameSequence.
 */
public class AsyncHorizonJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer FILTER_AND_REDUCE = AsyncHorizonJoinRecordCursorFactory::filterAndReduce;
    private static final PageFrameReducer REDUCE = AsyncHorizonJoinRecordCursorFactory::reduce;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncHorizonJoinRecordCursor cursor;
    private final PageFrameSequence<AsyncHorizonJoinAtom> frameSequence;
    // Combined metadata (master + sequence + slave) used for GROUP BY function column references in toPlan
    private final RecordMetadata markoutMetadata;
    private final RecordCursorFactory masterFactory;
    // Pre-computed offset values (in microseconds)
    private final LongList offsets;
    private final ObjList<Function> recordFunctions;
    private final RecordCursorFactory slaveFactory;
    private final int workerCount;

    public AsyncHorizonJoinRecordCursorFactory(
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
            @NotNull ObjList<Function> recordFunctions,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterKeyCopier,
            @Nullable RecordSink slaveKeyCopier,
            @NotNull RecordSink groupByKeyCopier,
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
            this.recordFunctions = recordFunctions;
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

            final AsyncHorizonJoinAtom atom = new AsyncHorizonJoinAtom(
                    asm,
                    configuration,
                    slaveFactory,
                    masterTimestampColumnIndex,
                    offsets,
                    keyTypes,
                    valueTypes,
                    asOfJoinKeyTypes,
                    masterKeyCopier,
                    slaveKeyCopier,
                    groupByKeyCopier,
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
                    // TODO(puzpuzpuz): support HORIZON JOIN without ON clause (timestamp-only ASOF lookup)
                    filter != null ? FILTER_AND_REDUCE : REDUCE,
                    reduceTaskFactory,
                    workerCount,
                    PageFrameReduceTask.TYPE_GROUP_BY
            );

            this.cursor = new AsyncHorizonJoinRecordCursor(
                    recordFunctions,
                    slaveFactory
            );
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public PageFrameSequence<AsyncHorizonJoinAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
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
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (usesCompiledFilter()) {
            sink.type("Async JIT Markout GroupBy");
        } else {
            sink.type("Async Markout GroupBy");
        }
        sink.meta("workers").val(workerCount);
        sink.meta("offsets").val(offsets.size());
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
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
            Map partialMap,
            RecordSink groupByKeyCopier,
            GroupByFunctionsUpdater functionUpdater
    ) {
        MapKey key = partialMap.withKey();
        key.put(markoutRecord, groupByKeyCopier);
        MapValue value = key.createValue();
        if (value.isNew()) {
            functionUpdater.updateNew(value, markoutRecord, masterRowId);
        } else {
            functionUpdater.updateExisting(value, markoutRecord, masterRowId);
        }
    }

    /**
     * Page frame reducer for filtered markout GROUP BY.
     * <p>
     * Applies filter first, then for each filtered master row iterates through all sequence offsets,
     * performs keyed ASOF JOIN lookup using MarkoutTimeFrameHelper, and aggregates results.
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

        final PageFrameSequence<AsyncHorizonJoinAtom> frameSequence = task.getFrameSequence(AsyncHorizonJoinAtom.class);
        final AsyncHorizonJoinAtom atom = frameSequence.getAtom();

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
            final Map partialMap = atom.getMap(slotId);
            final RecordSink groupByKeyCopier = atom.getGroupByKeyCopier();
            final int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
            final long masterTsScale = atom.getMasterTsScale();
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
            final SingleRecordSink masterSinkTarget = atom.getMasterSinkTarget(slotId);
            final SingleRecordSink slaveSinkTarget = atom.getSlaveSinkTarget(slotId);
            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            // Process filtered rows
            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            // Track previous master row's first offset ASOF position
            // (master rows move forward in time, so we can start from there)
            long prevFirstOffsetAsOfRowId = atom.getPrevFirstOffsetAsOfRowId(slotId);

            for (long i = 0; i < filteredRowCount; i++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                final long r = rows.get(i);
                record.setRowIndex(r);

                // Set bookmark to previous master row's first offset position
                if (prevFirstOffsetAsOfRowId != Long.MIN_VALUE) {
                    slaveTimeFrameHelper.setBookmark(prevFirstOffsetAsOfRowId);
                } else { // Reset to search from the beginning
                    slaveTimeFrameHelper.toTop();
                }

                prevFirstOffsetAsOfRowId = processMarkoutRow(
                        record,
                        baseRowId + r,
                        atom,
                        slaveTimeFrameHelper,
                        asOfJoinMap,
                        masterKeyCopier,
                        slaveKeyCopier,
                        masterSinkTarget,
                        slaveSinkTarget,
                        slaveRecord,
                        markoutRecord,
                        partialMap,
                        groupByKeyCopier,
                        functionUpdater,
                        masterTimestampColumnIndex,
                        sequenceRowCount,
                        masterTsScale
                );
            }

            // Cache previous master row's first offset ASOF position,
            // so that we don't find position for the first offset for
            // the next frame from scratch.
            atom.setPrevFirstOffsetAsOfRowId(slotId, prevFirstOffsetAsOfRowId);
        } finally {
            atom.release(slotId);
        }
    }

    /**
     * Process a single master row through all offsets with proper keyed ASOF JOIN semantics.
     *
     * @return the ASOF position (rowId) found for the first offset, for bookmark optimization
     */
    private static long processMarkoutRow(
            PageFrameMemoryRecord masterRecord,
            long masterRowId,
            AsyncHorizonJoinAtom atom,
            MarkoutTimeFrameHelper slaveTimeFrameHelper,
            Map asOfJoinMap,
            RecordSink masterKeyCopier,
            RecordSink slaveKeyCopier,
            SingleRecordSink masterSinkTarget,
            SingleRecordSink slaveSinkTarget,
            Record slaveRecord,
            MarkoutRecord markoutRecord,
            Map partialMap,
            RecordSink groupByKeyCopier,
            GroupByFunctionsUpdater functionUpdater,
            int masterTimestampColumnIndex,
            long sequenceRowCount,
            long masterTsScale
    ) {
        final long masterTimestamp = masterRecord.getTimestamp(masterTimestampColumnIndex);

        // Look up cached rowId for master's join key
        long cachedRowId = Long.MIN_VALUE;
        if (asOfJoinMap != null && masterKeyCopier != null) {
            MapKey cacheKey = asOfJoinMap.withKey();
            cacheKey.put(masterRecord, masterKeyCopier);
            MapValue cacheValue = cacheKey.findValue();
            if (cacheValue != null) {
                cachedRowId = cacheValue.getLong(0);
            }

            // Copy master key to sink target for key comparison in backwardScanForKeyMatch
            masterSinkTarget.clear();
            masterKeyCopier.copy(masterRecord, masterSinkTarget);
        }

        // Track state across offsets
        long prevMatchRowId = Long.MIN_VALUE;  // Last matched slave rowId (used for subsequent offsets)
        long prevAsOfRowId = Long.MIN_VALUE;   // Last ASOF position (used when no match found)

        // ========================================
        // FIRST OFFSET (bootstrap) - handles cache lookup and update
        // ========================================
        long offset = atom.getOffset(0);
        // Scale horizon timestamp to common unit (nanos) for cross-resolution support
        long horizonTs0 = scaleTimestamp(masterTimestamp + offset, masterTsScale);

        long match0RowId = Long.MIN_VALUE;
        long asOfRowId0 = Long.MIN_VALUE;  // Track first offset's ASOF position for bookmark optimization
        if (asOfJoinMap != null && masterKeyCopier != null) {
            // Navigate to ASOF position for first offset
            asOfRowId0 = slaveTimeFrameHelper.findAsOfRow(horizonTs0);
            prevAsOfRowId = asOfRowId0;

            if (asOfRowId0 != Long.MIN_VALUE) {
                // Backward scan for key match using memeq comparison, stop at cached position
                match0RowId = slaveTimeFrameHelper.backwardScanForKeyMatch(
                        masterSinkTarget,
                        slaveSinkTarget,
                        slaveKeyCopier,
                        cachedRowId
                );

                // Update cache with first offset's match
                if (match0RowId != Long.MIN_VALUE) {
                    MapKey newCacheKey = asOfJoinMap.withKey();
                    newCacheKey.put(masterRecord, masterKeyCopier);
                    newCacheKey.createValue().putLong(0, match0RowId);
                    prevMatchRowId = match0RowId;
                }
            }
        }

        // Aggregate first offset
        Record matchedSlaveRecord0 = null;
        if (match0RowId != Long.MIN_VALUE) {
            slaveTimeFrameHelper.recordAt(match0RowId);
            matchedSlaveRecord0 = slaveRecord;
        }
        markoutRecord.of(masterRecord, offset, matchedSlaveRecord0);
        aggregateRecord(markoutRecord, masterRowId, partialMap, groupByKeyCopier, functionUpdater);

        // ========================================
        // REMAINING OFFSETS
        // ========================================
        for (int seqIdx = 1; seqIdx < sequenceRowCount; seqIdx++) {
            long offset0 = atom.getOffset(seqIdx);
            // Scale horizon timestamp to common unit (nanos) for cross-resolution support
            long horizonTs = scaleTimestamp(masterTimestamp + offset0, masterTsScale);

            long matchRowId = Long.MIN_VALUE;
            if (asOfJoinMap != null && masterKeyCopier != null) {
                // Navigate forward to ASOF position for this offset
                long asOfRowId = slaveTimeFrameHelper.findAsOfRow(horizonTs);

                if (asOfRowId != Long.MIN_VALUE) {
                    // Determine stop position for backward scan
                    long stopRowId;
                    if (prevMatchRowId != Long.MIN_VALUE) {
                        // Stop at previous match (can't be before it)
                        stopRowId = prevMatchRowId;
                    } else {
                        // No previous match, stop at previous ASOF position (already scanned that range)
                        stopRowId = prevAsOfRowId;
                    }

                    // Backward scan for key match using memeq comparison
                    matchRowId = slaveTimeFrameHelper.backwardScanForKeyMatch(
                            masterSinkTarget,
                            slaveSinkTarget,
                            slaveKeyCopier,
                            stopRowId
                    );

                    if (matchRowId != Long.MIN_VALUE) {
                        // Found new match
                        prevMatchRowId = matchRowId;
                    }
                    // If no new match found, prevMatchRowId stays valid (its ts <= horizonTs0 < horizonTs)

                    prevAsOfRowId = asOfRowId;
                }
            }

            // Aggregate with prevMatchRowId (may be from earlier offset or null)
            Record matchedSlaveRecord = null;
            long effectiveMatchRowId = (matchRowId != Long.MIN_VALUE) ? matchRowId : prevMatchRowId;
            if (effectiveMatchRowId != Long.MIN_VALUE) {
                slaveTimeFrameHelper.recordAt(effectiveMatchRowId);
                matchedSlaveRecord = slaveRecord;
            }
            markoutRecord.of(masterRecord, offset0, matchedSlaveRecord);
            aggregateRecord(markoutRecord, masterRowId, partialMap, groupByKeyCopier, functionUpdater);
        }

        return asOfRowId0;
    }

    /**
     * Page frame reducer for markout GROUP BY.
     * <p>
     * For each master row in the page frame, iterates through all sequence offsets,
     * performs keyed ASOF JOIN lookup using MarkoutTimeFrameHelper, and aggregates results.
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

        final PageFrameSequence<AsyncHorizonJoinAtom> frameSequence = task.getFrameSequence(AsyncHorizonJoinAtom.class);
        final AsyncHorizonJoinAtom atom = frameSequence.getAtom();

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
            final Map partialMap = atom.getMap(slotId);
            final RecordSink groupByKeyCopier = atom.getGroupByKeyCopier();
            final int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
            final long masterTsScale = atom.getMasterTsScale();
            final MarkoutRecord markoutRecord = atom.getCombinedRecord(slotId);

            // Get ASOF join resources
            final MarkoutTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
            final Map asOfJoinMap = atom.getAsOfJoinMap(slotId);  // Cache: joinKey -> rowId
            final RecordSink masterKeyCopier = atom.getMasterKeyCopier();
            final RecordSink slaveKeyCopier = atom.getSlaveKeyCopier();
            final SingleRecordSink masterSinkTarget = atom.getMasterSinkTarget(slotId);
            final SingleRecordSink slaveSinkTarget = atom.getSlaveSinkTarget(slotId);
            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            // Process rows
            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            // Track previous master row's first offset ASOF position
            // (master rows move forward in time, so we can start from there)
            long prevFirstOffsetAsOfRowId = atom.getPrevFirstOffsetAsOfRowId(slotId);

            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);

                // Set bookmark to previous master row's first offset position
                if (prevFirstOffsetAsOfRowId != Long.MIN_VALUE) {
                    slaveTimeFrameHelper.setBookmark(prevFirstOffsetAsOfRowId);
                } else { // Reset to search from the beginning
                    slaveTimeFrameHelper.toTop();
                }

                prevFirstOffsetAsOfRowId = processMarkoutRow(
                        record,
                        baseRowId + r,
                        atom,
                        slaveTimeFrameHelper,
                        asOfJoinMap,
                        masterKeyCopier,
                        slaveKeyCopier,
                        masterSinkTarget,
                        slaveSinkTarget,
                        slaveRecord,
                        markoutRecord,
                        partialMap,
                        groupByKeyCopier,
                        functionUpdater,
                        masterTimestampColumnIndex,
                        sequenceRowCount,
                        masterTsScale
                );
            }

            // Cache previous master row's first offset ASOF position,
            // so that we don't find position for the first offset for
            // the next frame from scratch.
            atom.setPrevFirstOffsetAsOfRowId(slotId, prevFirstOffsetAsOfRowId);
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
        Misc.freeObjList(recordFunctions);
        Misc.freeIfCloseable(markoutMetadata);
    }
}

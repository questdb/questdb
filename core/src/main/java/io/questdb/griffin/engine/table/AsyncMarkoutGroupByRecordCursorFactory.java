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
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

/**
 * Factory for parallel markout query execution using PageFrameSequence.
 * <p>
 * This factory creates the infrastructure for parallelizing:
 * Master -> MarkoutHorizon (CROSS JOIN time_offset_sequence) -> ASOF JOIN slave -> GROUP BY time_offset
 */
public class AsyncMarkoutGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final PageFrameReducer FILTER_AND_REDUCE = AsyncMarkoutGroupByRecordCursorFactory::filterAndReduce;
    private static final PageFrameReducer REDUCE = AsyncMarkoutGroupByRecordCursorFactory::reduce;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncMarkoutGroupByRecordCursor cursor;
    private final PageFrameSequence<AsyncMarkoutGroupByAtom> frameSequence;
    private final RecordCursorFactory masterFactory;
    private final ObjList<Function> recordFunctions;
    private final RecordCursorFactory sequenceFactory;
    private final RecordCursorFactory slaveFactory;
    private final int workerCount;

    public AsyncMarkoutGroupByRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull CairoEngine engine,
            @NotNull MessageBus messageBus,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordCursorFactory slaveFactory,
            @NotNull RecordCursorFactory sequenceFactory,
            int masterTimestampColumnIndex,
            int sequenceColumnIndex,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterKeyCopier,
            @Nullable RecordSink slaveKeyCopier,
            int slaveTimestampIndex,
            @NotNull RecordSink groupByKeyCopier,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndices,
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
            this.masterFactory = masterFactory;
            this.slaveFactory = slaveFactory;
            this.sequenceFactory = sequenceFactory;
            this.recordFunctions = recordFunctions;
            this.workerCount = workerCount;

            AsyncMarkoutGroupByAtom atom = new AsyncMarkoutGroupByAtom(
                    asm,
                    configuration,
                    slaveFactory,
                    masterTimestampColumnIndex,
                    sequenceColumnIndex,
                    keyTypes,
                    valueTypes,
                    asOfJoinKeyTypes,
                    masterKeyCopier,
                    slaveKeyCopier,
                    slaveTimestampIndex,
                    groupByKeyCopier,
                    columnSources,
                    columnIndices,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    filter,
                    perWorkerFilters,
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
                    PageFrameReduceTask.TYPE_GROUP_BY
            );

            this.cursor = new AsyncMarkoutGroupByRecordCursor(
                    configuration,
                    recordFunctions,
                    sequenceFactory,
                    slaveFactory
            );
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public PageFrameSequence<AsyncMarkoutGroupByAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
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
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("values", frameSequence.getAtom().getOwnerGroupByFunctions(), true);
        sink.child(masterFactory);
        sink.child(sequenceFactory);
        sink.child(slaveFactory);
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getCompiledFilter() != null;
    }

    private static void aggregateRecord(
            CombinedRecord combinedRecord,
            long masterRowId,
            Map partialMap,
            RecordSink groupByKeyCopier,
            GroupByFunctionsUpdater functionUpdater
    ) {
        MapKey key = partialMap.withKey();
        key.put(combinedRecord, groupByKeyCopier);
        MapValue value = key.createValue();
        if (value.isNew()) {
            functionUpdater.updateNew(value, combinedRecord, masterRowId);
        } else {
            functionUpdater.updateExisting(value, combinedRecord, masterRowId);
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

        final PageFrameSequence<AsyncMarkoutGroupByAtom> frameSequence = task.getFrameSequence(AsyncMarkoutGroupByAtom.class);
        final AsyncMarkoutGroupByAtom atom = frameSequence.getAtom();

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
            final CombinedRecord combinedRecord = atom.getCombinedRecord(slotId);
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
            final Map asofJoinMap = atom.getAsOfJoinMap(slotId);  // Cache: joinKey -> rowId
            final RecordSink masterKeyCopier = atom.getMasterKeyCopier();
            final RecordSink slaveKeyCopier = atom.getSlaveKeyCopier();
            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            // Process filtered rows
            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            // Track previous master row's first offset ASOF position
            // (master rows move forward in time, so we can start from there)
            long prevFirstOffsetAsOfRowId = Long.MIN_VALUE;

            for (long i = 0; i < filteredRowCount; i++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                final long r = rows.get(i);
                record.setRowIndex(r);

                // Set bookmark to previous master row's first offset position
                slaveTimeFrameHelper.setBookmark(prevFirstOffsetAsOfRowId);

                prevFirstOffsetAsOfRowId = processMarkoutRow(
                        record,
                        baseRowId + r,
                        atom,
                        slaveTimeFrameHelper,
                        asofJoinMap,
                        masterKeyCopier,
                        slaveKeyCopier,
                        slaveRecord,
                        combinedRecord,
                        partialMap,
                        groupByKeyCopier,
                        functionUpdater,
                        masterTimestampColumnIndex,
                        sequenceRowCount
                );
            }
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
            AsyncMarkoutGroupByAtom atom,
            MarkoutTimeFrameHelper slaveTimeFrameHelper,
            Map asOfJoinMap,
            RecordSink masterKeyCopier,
            RecordSink slaveKeyCopier,
            Record slaveRecord,
            CombinedRecord combinedRecord,
            Map partialMap,
            RecordSink groupByKeyCopier,
            GroupByFunctionsUpdater functionUpdater,
            int masterTimestampColumnIndex,
            long sequenceRowCount
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
        }

        // Track state across offsets
        long prevMatchRowId = Long.MIN_VALUE;  // Last matched slave rowId (used for subsequent offsets)
        long prevAsOfRowId = Long.MIN_VALUE;   // Last ASOF position (used when no match found)

        // ========================================
        // FIRST OFFSET (bootstrap) - handles cache lookup and update
        // ========================================
        long secOffsValue0 = atom.getSequenceOffsetValue(0);
        // TODO(puzpuzpuz): support nanos
        long horizonTs0 = masterTimestamp + secOffsValue0 * 1_000_000L;

        long match0RowId = Long.MIN_VALUE;
        long asOfRowId0 = Long.MIN_VALUE;  // Track first offset's ASOF position for bookmark optimization
        if (asOfJoinMap != null && masterKeyCopier != null) {
            // Navigate to ASOF position for first offset
            asOfRowId0 = slaveTimeFrameHelper.findAsOfRow(horizonTs0);
            prevAsOfRowId = asOfRowId0;

            if (asOfRowId0 != Long.MIN_VALUE) {
                // Set master key in asofJoinMap for comparison during backward scan
                // We reuse the asofJoinMap temporarily - put master key, backward scan checks for it
                asOfJoinMap.clear();
                MapKey masterKey = asOfJoinMap.withKey();
                masterKey.put(masterRecord, masterKeyCopier);
                masterKey.createValue().putLong(0, 1L);  // Dummy value, just need key presence

                // Backward scan for key match, stop at cached position
                match0RowId = slaveTimeFrameHelper.backwardScanForKeyMatch(
                        asOfJoinMap,
                        slaveKeyCopier,
                        cachedRowId
                );

                // Update cache with first offset's match
                if (match0RowId != Long.MIN_VALUE) {
                    // TODO(puzpuzpuz): this is very inefficient
                    asOfJoinMap.clear();
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
        combinedRecord.of(masterRecord, secOffsValue0, matchedSlaveRecord0);
        aggregateRecord(combinedRecord, masterRowId, partialMap, groupByKeyCopier, functionUpdater);

        // ========================================
        // REMAINING OFFSETS
        // ========================================
        for (int seqIdx = 1; seqIdx < sequenceRowCount; seqIdx++) {
            long secOffsValue = atom.getSequenceOffsetValue(seqIdx);
            // TODO(puzpuzpuz): support nanos
            long horizonTs = masterTimestamp + secOffsValue * 1_000_000L;

            long matchRowId = Long.MIN_VALUE;
            if (asOfJoinMap != null && masterKeyCopier != null) {
                // Navigate forward to ASOF position for this offset
                long asofRowId = slaveTimeFrameHelper.findAsOfRow(horizonTs);

                if (asofRowId != Long.MIN_VALUE) {
                    // Determine stop position for backward scan
                    long stopRowId;
                    if (prevMatchRowId != Long.MIN_VALUE) {
                        // Stop at previous match (can't be before it)
                        stopRowId = prevMatchRowId;
                    } else {
                        // No previous match, stop at previous ASOF position (already scanned that range)
                        stopRowId = prevAsOfRowId;
                    }

                    // Set master key for comparison
                    asOfJoinMap.clear();
                    MapKey masterKey = asOfJoinMap.withKey();
                    masterKey.put(masterRecord, masterKeyCopier);
                    masterKey.createValue().putLong(0, 1L);

                    // Backward scan for key match
                    matchRowId = slaveTimeFrameHelper.backwardScanForKeyMatch(
                            asOfJoinMap,
                            slaveKeyCopier,
                            stopRowId
                    );

                    if (matchRowId != Long.MIN_VALUE) {
                        // Found new match
                        prevMatchRowId = matchRowId;
                    }
                    // If no new match found, prevMatchRowId stays valid (its ts <= horizonTs0 < horizonTs)

                    prevAsOfRowId = asofRowId;
                }
            }

            // Aggregate with prevMatchRowId (may be from earlier offset or null)
            Record matchedSlaveRecord = null;
            long effectiveMatchRowId = (matchRowId != Long.MIN_VALUE) ? matchRowId : prevMatchRowId;
            if (effectiveMatchRowId != Long.MIN_VALUE) {
                slaveTimeFrameHelper.recordAt(effectiveMatchRowId);
                matchedSlaveRecord = slaveRecord;
            }
            combinedRecord.of(masterRecord, secOffsValue, matchedSlaveRecord);
            aggregateRecord(combinedRecord, masterRowId, partialMap, groupByKeyCopier, functionUpdater);
        }

        // Restore the cache with the first offset's match position for use by subsequent master rows
        // This is necessary because the backward scan for subsequent offsets corrupts the cache
        // by putting dummy values
        if (asOfJoinMap != null && masterKeyCopier != null && match0RowId != Long.MIN_VALUE) {
            // TODO(puzpuzpuz): this is very inefficient
            asOfJoinMap.clear();
            MapKey cacheKey = asOfJoinMap.withKey();
            cacheKey.put(masterRecord, masterKeyCopier);
            cacheKey.createValue().putLong(0, match0RowId);
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

        final PageFrameSequence<AsyncMarkoutGroupByAtom> frameSequence = task.getFrameSequence(AsyncMarkoutGroupByAtom.class);
        final AsyncMarkoutGroupByAtom atom = frameSequence.getAtom();

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
            final CombinedRecord combinedRecord = atom.getCombinedRecord(slotId);

            // Get ASOF join resources
            final MarkoutTimeFrameHelper slaveTimeFrameHelper = atom.getSlaveTimeFrameHelper(slotId);
            final Map asofJoinMap = atom.getAsOfJoinMap(slotId);  // Cache: joinKey -> rowId
            final RecordSink masterKeyCopier = atom.getMasterKeyCopier();
            final RecordSink slaveKeyCopier = atom.getSlaveKeyCopier();
            final Record slaveRecord = slaveTimeFrameHelper.getRecord();

            // Process rows
            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            // Track previous master row's first offset ASOF position
            // (master rows move forward in time, so we can start from there)
            long prevFirstOffsetAsOfRowId = Long.MIN_VALUE;

            for (long r = 0; r < frameRowCount; r++) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                record.setRowIndex(r);

                // Set bookmark to previous master row's first offset position
                slaveTimeFrameHelper.setBookmark(prevFirstOffsetAsOfRowId);

                prevFirstOffsetAsOfRowId = processMarkoutRow(
                        record,
                        baseRowId + r,
                        atom,
                        slaveTimeFrameHelper,
                        asofJoinMap,
                        masterKeyCopier,
                        slaveKeyCopier,
                        slaveRecord,
                        combinedRecord,
                        partialMap,
                        groupByKeyCopier,
                        functionUpdater,
                        masterTimestampColumnIndex,
                        sequenceRowCount
                );
            }
        } finally {
            atom.release(slotId);
        }
    }

    @Override
    protected void _close() {
        Misc.free(frameSequence);
        Misc.free(cursor);
        Misc.free(masterFactory);
        Misc.free(sequenceFactory);
        Misc.free(slaveFactory);
        Misc.freeObjList(recordFunctions);
    }
}

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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyCompiledFilter;
import static io.questdb.griffin.engine.table.AsyncFilterUtils.applyFilter;

/**
 * Factory for parallel non-keyed multi-slave HORIZON JOIN query execution.
 * Produces a single output row.
 */
public class AsyncMultiHorizonJoinNotKeyedRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final UnorderedPageFrameReducer FILTER_AND_REDUCE = AsyncMultiHorizonJoinNotKeyedRecordCursorFactory::filterAndReduce;
    private static final UnorderedPageFrameReducer REDUCE = AsyncMultiHorizonJoinNotKeyedRecordCursorFactory::reduce;
    private AsyncMultiHorizonJoinNotKeyedRecordCursor cursor;
    private UnorderedPageFrameSequence<AsyncMultiHorizonJoinNotKeyedAtom> frameSequence;
    private ObjList<GroupByFunction> groupByFunctions;
    private JoinRecordMetadata horizonJoinMetadata;
    private RecordCursorFactory masterFactory;
    private final long[] offsets;
    private AsyncHorizonJoinResources resources;
    private ObjList<RecordCursorFactory> slaveFactories;
    private ObjList<HorizonJoinSlaveState> slaveStates;
    private final int workerCount;

    public AsyncMultiHorizonJoinNotKeyedRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoEngine engine,
            @NotNull MessageBus messageBus,
            @NotNull RecordMetadata metadata,
            @NotNull JoinRecordMetadata horizonJoinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull ObjList<HorizonJoinSlaveState> slaveStates,
            @Nullable ColumnTypes[] perSlaveAsOfJoinKeyTypes,
            @Nullable Class<RecordSink> @NotNull [] masterAsOfJoinMapSinkClasses,
            @Nullable Class<RecordSink> @NotNull [] slaveAsOfJoinMapSinkClasses,
            long @NotNull [] offsets,
            int masterTimestampColumnIndex,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            int valueCount,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            @NotNull AsyncHorizonJoinResources resources,
            int workerCount
    ) {
        super(metadata);
        try {
            this.horizonJoinMetadata = horizonJoinMetadata;
            this.masterFactory = masterFactory;
            this.offsets = offsets;
            this.groupByFunctions = groupByFunctions;
            this.resources = resources;
            this.slaveStates = slaveStates;
            this.slaveFactories = new ObjList<>(slaveStates.size());
            this.workerCount = workerCount;

            final boolean hasFilter = resources.getFilter() != null;
            final AsyncMultiHorizonJoinNotKeyedAtom atom = new AsyncMultiHorizonJoinNotKeyedAtom(
                    asm,
                    configuration,
                    slaveStates,
                    perSlaveAsOfJoinKeyTypes,
                    masterAsOfJoinMapSinkClasses,
                    slaveAsOfJoinMapSinkClasses,
                    masterTimestampColumnIndex,
                    offsets,
                    valueCount,
                    columnSources,
                    columnIndexes,
                    groupByFunctions,
                    resources,
                    workerCount
            );

            this.frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    hasFilter ? FILTER_AND_REDUCE : REDUCE,
                    workerCount
            );

            // Transfer one state factory at a time. The destination owns an entry only after add()
            // succeeds; detach then makes the state suffix the sole rollback owner.
            for (int i = 0; i < slaveStates.size(); i++) {
                final HorizonJoinSlaveState state = slaveStates.getQuick(i);
                slaveFactories.add(state.getFactory());
                state.detachFactory();
            }
            slaveStates.clear();

            this.cursor = new AsyncMultiHorizonJoinNotKeyedRecordCursor(groupByFunctions, slaveFactories);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    @TestOnly
    public AsyncMultiHorizonJoinNotKeyedAtom getAtom() {
        return frameSequence.getAtom();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return masterFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        frameSequence.of(masterFactory, executionContext, ORDER_ASC);
        try {
            cursor.of(frameSequence, executionContext);
            return cursor;
        } catch (Throwable th) {
            // On a mid-reopen breach, close() drains the partially reopened atom and resets isOpen
            // so the cached factory stays reusable.
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Multi Horizon Join");
        sink.meta("workers").val(workerCount);
        sink.meta("offsets").val(offsets.length);
        sink.meta("tables").val(slaveFactories.size());
        sink.setMetadata(horizonJoinMetadata);
        sink.optAttr("values", frameSequence.getAtom().getOwnerGroupByFunctions());
        sink.setMetadata(null);
        sink.child(masterFactory);
        for (int i = 0, n = slaveFactories.size(); i < n; i++) {
            sink.child(slaveFactories.getQuick(i));
        }
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getFilterContext().getCompiledFilter() != null;
    }

    private static void filterAndReduce(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        assert frameRowCount > 0;

        @SuppressWarnings("unchecked") final AsyncMultiHorizonJoinNotKeyedAtom atom =
                ((UnorderedPageFrameSequence<AsyncMultiHorizonJoinNotKeyedAtom>) frameSequence).getAtom();

        final long offsetCount = atom.getOffsetCount();
        if (offsetCount == 0) {
            return;
        }

        final boolean owner = stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        PageFrameMemoryPool frameMemoryPool = null;
        try {
            final AsyncFilterContext filterCtx = atom.getFilterContext();
            final PageFrameAddressCache addressCache = frameSequence.getPageFrameAddressCache();
            final boolean isParquetFrame = addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET;
            final boolean useLateMaterialization = filterCtx.shouldUseLateMaterialization(slotId, isParquetFrame);

            frameMemoryPool = filterCtx.getMemoryPool(slotId);
            final PageFrameMemory frameMemory;
            if (useLateMaterialization) {
                frameMemory = frameMemoryPool.navigateTo(frameIndex, filterCtx.getFilterUsedColumnIndexes());
            } else {
                frameMemory = frameMemoryPool.navigateTo(frameIndex);
            }
            record.init(frameMemory);

            final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
            final SimpleMapValue value = atom.getMapValue(slotId);
            final int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
            final MultiHorizonJoinRecord horizonJoinRecord = atom.getHorizonJoinRecord(slotId);
            final CompiledFilter compiledFilter = filterCtx.getCompiledFilter();
            final Function filter = filterCtx.getFilter(slotId);
            final int slaveCount = atom.getSlaveCount();

            // Apply filter to master rows
            final DirectLongList rows = filterCtx.getFilteredRows(slotId);
            rows.clear();
            if (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.hasColumnTypeCasts()) {
                applyFilter(filter, rows, record, frameRowCount);
            } else {
                applyCompiledFilter(
                        compiledFilter,
                        filterCtx.getBindVarMemory(),
                        filterCtx.getBindVarFunctions(),
                        frameMemory,
                        addressCache,
                        filterCtx.getDataAddresses(slotId),
                        filterCtx.getAuxAddresses(slotId),
                        rows,
                        frameRowCount
                );
            }

            final long filteredRowCount = rows.size();
            if (filteredRowCount == 0) {
                return;
            }

            if (isParquetFrame) {
                filterCtx.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }

            if (useLateMaterialization && frameMemory.populateRemainingColumns(filterCtx.getFilterUsedColumnIndexes(), rows, false)) {
                PageFrameFilteredMemoryRecord filteredMemoryRecord = filterCtx.getPageFrameFilteredMemoryRecord(slotId);
                filteredMemoryRecord.of(frameMemory, record, filterCtx.getFilterUsedColumnIndexes());
                record = filteredMemoryRecord;
            }

            // Get horizon timestamp iterator and initialize for filtered rows
            final AsyncHorizonTimestampIterator horizonIterator = atom.getHorizonIterator(slotId);
            long baseRowId = Rows.toRowID(frameIndex, 0);
            horizonIterator.ofFiltered(frameMemory.getPageAddress(masterTimestampColumnIndex), rows);

            processHorizonTimestamps(
                    atom,
                    horizonIterator,
                    slaveCount,
                    baseRowId,
                    record,
                    horizonJoinRecord,
                    value,
                    functionUpdater,
                    circuitBreaker,
                    slotId
            );
        } finally {
            // Release the slot even if buffer cleanup throws; a stranded slot never returns (PerWorkerLocks has no reset).
            try {
                if (frameMemoryPool != null) {
                    frameMemoryPool.releaseParquetBuffers();
                }
            } finally {
                atom.release(slotId);
            }
        }
    }

    /**
     * Process all horizon timestamps in sorted order, performing ASOF lookups
     * on each slave for each (horizonTs, masterRowIdx, offsetIdx) tuple.
     * <p>
     * For keyed ASOF JOINs, each slave adaptively chooses between two strategies:
     * <p>
     * 1. <b>Backward-only mode</b> (default): when the ASOF position changes, clear the
     * key cache and reset the backward watermark. Each position change costs ~K backward
     * scan rows for K distinct keys. Wins when K is small.
     * <p>
     * 2. <b>Forward scan mode</b>: forward-scan all slave rows between consecutive ASOF
     * positions, populating the key map. Cost = O(gap). Wins when K is large or rare keys
     * cause deep backward scans.
     * <p>
     * Each slave starts in backward-only mode per frame. The algorithm switches to forward
     * scan mode for the remainder of the frame when either: (a) backward scan cost at a
     * position exceeds gap * SWITCH_FACTOR (relative check, within a partition), or
     * (b) backward scan cost exceeds BWD_SCAN_ABSOLUTE_THRESHOLD (absolute check, handles
     * cross-partition boundaries where the relative check cannot trigger).
     * <p>
     * For non-keyed slaves, the ASOF position is used directly without key matching.
     */
    private static void processHorizonTimestamps(
            AsyncMultiHorizonJoinNotKeyedAtom atom,
            AsyncHorizonTimestampIterator horizonIterator,
            int slaveCount,
            long baseRowId,
            PageFrameMemoryRecord masterRecord,
            MultiHorizonJoinRecord horizonJoinRecord,
            SimpleMapValue value,
            GroupByFunctionsUpdater functionUpdater,
            SqlExecutionCircuitBreaker circuitBreaker,
            int slotId
    ) {
        final ObjList<Record> matchedSlaveRecords = atom.getMatchedSlaveRecords(slotId);

        for (int s = 0; s < slaveCount; s++) {
            atom.getSlaveTimeFrameHelper(slotId, s).toTop();
            Map asOfMap = atom.getAsOfJoinMap(slotId, s);
            if (asOfMap != null) {
                asOfMap.clear();
            }
        }

        while (horizonIterator.next()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final long horizonTs = horizonIterator.getHorizonTimestamp();
            final long masterRowIdx = horizonIterator.getMasterRowIndex();
            final int offsetIdx = horizonIterator.getOffsetIndex();
            final long offset = atom.getOffset(offsetIdx);

            masterRecord.setFilteredRowIndex(masterRowIdx, horizonIterator.getMasterRowCompactIndex());
            final long masterRowId = baseRowId + masterRowIdx;

            for (int s = 0; s < slaveCount; s++) {
                final HorizonJoinTimeFrameHelper helper = atom.getSlaveTimeFrameHelper(slotId, s);
                final long scaledHorizonTs = scaleTimestamp(horizonTs, atom.getMasterTimestampScale(s));
                long asOfRowId = helper.findAsOfRow(scaledHorizonTs);

                long matchRowId = Long.MIN_VALUE;
                final Map asOfJoinMap = atom.getAsOfJoinMap(slotId, s);
                final RecordSink masterSink = atom.getMasterAsOfJoinSink(slotId, s);
                final RecordSink slaveSink = atom.getSlaveAsOfJoinMapSink(slotId, s);

                if (asOfJoinMap != null && masterSink != null && slaveSink != null) {
                    final Record masterKeyRecord = atom.getMasterKeyRecord(slotId, s, masterRecord);
                    final SymbolTranslatingRecord symbolTranslatingRecord =
                            masterKeyRecord instanceof SymbolTranslatingRecord rec ? rec : null;
                    matchRowId = helper.findKeyedAsOfMatch(
                            asOfRowId, masterKeyRecord, masterSink, slaveSink,
                            asOfJoinMap, symbolTranslatingRecord
                    );
                } else {
                    matchRowId = asOfRowId;
                }

                if (matchRowId != Long.MIN_VALUE) {
                    helper.recordAt(matchRowId);
                    matchedSlaveRecords.setQuick(s, helper.getRecord());
                } else {
                    matchedSlaveRecords.setQuick(s, null);
                }
            }

            horizonJoinRecord.of(masterRecord, offset, horizonTs, matchedSlaveRecords);
            if (value.isNew()) {
                functionUpdater.updateNew(value, horizonJoinRecord, masterRowId);
                value.setNew(false);
            } else {
                functionUpdater.updateExisting(value, horizonJoinRecord, masterRowId);
            }
        }
    }

    private static void reduce(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        assert frameRowCount > 0;

        @SuppressWarnings("unchecked") final AsyncMultiHorizonJoinNotKeyedAtom atom =
                ((UnorderedPageFrameSequence<AsyncMultiHorizonJoinNotKeyedAtom>) frameSequence).getAtom();

        final long offsetCount = atom.getOffsetCount();
        if (offsetCount == 0) {
            return;
        }

        final boolean owner = stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        PageFrameMemoryPool frameMemoryPool = null;
        try {
            final AsyncFilterContext filterCtx = atom.getFilterContext();
            frameMemoryPool = filterCtx.getMemoryPool(slotId);
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
            record.init(frameMemory);

            final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
            final SimpleMapValue value = atom.getMapValue(slotId);
            final int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
            final MultiHorizonJoinRecord horizonJoinRecord = atom.getHorizonJoinRecord(slotId);
            final int slaveCount = atom.getSlaveCount();

            final AsyncHorizonTimestampIterator horizonIterator = atom.getHorizonIterator(slotId);
            long baseRowId = Rows.toRowID(frameIndex, 0);
            horizonIterator.of(frameMemory.getPageAddress(masterTimestampColumnIndex), 0, frameRowCount);

            processHorizonTimestamps(
                    atom,
                    horizonIterator,
                    slaveCount,
                    baseRowId,
                    record,
                    horizonJoinRecord,
                    value,
                    functionUpdater,
                    circuitBreaker,
                    slotId
            );
        } finally {
            // Release the slot even if buffer cleanup throws; a stranded slot never returns (PerWorkerLocks has no reset).
            try {
                if (frameMemoryPool != null) {
                    frameMemoryPool.releaseParquetBuffers();
                }
            } finally {
                atom.release(slotId);
            }
        }
    }

    @Override
    protected void _close() {
        final AsyncMultiHorizonJoinNotKeyedRecordCursor cursor = this.cursor;
        this.cursor = null;
        final UnorderedPageFrameSequence<AsyncMultiHorizonJoinNotKeyedAtom> frameSequence = this.frameSequence;
        this.frameSequence = null;
        final ObjList<GroupByFunction> groupByFunctions = this.groupByFunctions;
        this.groupByFunctions = null;
        final JoinRecordMetadata horizonJoinMetadata = this.horizonJoinMetadata;
        this.horizonJoinMetadata = null;
        final RecordCursorFactory masterFactory = this.masterFactory;
        this.masterFactory = null;
        final AsyncHorizonJoinResources resources = this.resources;
        this.resources = null;
        final ObjList<RecordCursorFactory> slaveFactories = this.slaveFactories;
        this.slaveFactories = null;
        final ObjList<HorizonJoinSlaveState> slaveStates = this.slaveStates;
        this.slaveStates = null;

        Throwable cleanupFailure = null;
        // Free cursor before frameSequence: a cursor left half-open by a failed
        // getCursor() still references frameSequence and reset()s it on close().
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, cursor);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, frameSequence);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, masterFactory);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, slaveFactories);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, slaveStates);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, horizonJoinMetadata);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, resources);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, groupByFunctions);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }
}

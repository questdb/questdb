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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
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
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
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
 * Factory for parallel keyed multi-slave HORIZON JOIN query execution.
 */
public class AsyncMultiHorizonJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final UnorderedPageFrameReducer FILTER_AND_REDUCE = AsyncMultiHorizonJoinRecordCursorFactory::filterAndReduce;
    private static final UnorderedPageFrameReducer REDUCE = AsyncMultiHorizonJoinRecordCursorFactory::reduce;
    private final AsyncMultiHorizonJoinRecordCursor cursor;
    private final UnorderedPageFrameSequence<AsyncMultiHorizonJoinAtom> frameSequence;
    private final JoinRecordMetadata horizonJoinMetadata;
    private final RecordCursorFactory masterFactory;
    private final LongList offsets;
    private final ObjList<Function> recordFunctions;
    private final RecordCursorFactory[] slaveFactories;
    private final int workerCount;

    public AsyncMultiHorizonJoinRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoEngine engine,
            @NotNull MessageBus messageBus,
            @NotNull RecordMetadata metadata,
            @NotNull JoinRecordMetadata horizonJoinMetadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull HorizonJoinSlaveState[] slaveStates,
            @Nullable ColumnTypes[] perSlaveAsOfJoinKeyTypes,
            @Nullable Class<RecordSink> @NotNull [] masterAsOfJoinMapSinkClasses,
            @Nullable Class<RecordSink> @NotNull [] slaveAsOfJoinMapSinkClasses,
            @NotNull LongList offsets,
            int masterTimestampColumnIndex,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @NotNull ObjList<Function> keyFunctions,
            @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Transient @NotNull ListColumnFilter groupByColumnFilter,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        super(metadata);
        try {
            this.horizonJoinMetadata = horizonJoinMetadata;
            this.masterFactory = masterFactory;
            this.offsets = offsets;
            this.recordFunctions = recordFunctions;
            this.workerCount = workerCount;

            this.slaveFactories = new RecordCursorFactory[slaveStates.length];
            for (int i = 0; i < slaveStates.length; i++) {
                slaveFactories[i] = slaveStates[i].getFactory();
            }

            final AsyncMultiHorizonJoinAtom atom = new AsyncMultiHorizonJoinAtom(
                    asm,
                    configuration,
                    horizonJoinMetadata,
                    slaveStates,
                    perSlaveAsOfJoinKeyTypes,
                    masterAsOfJoinMapSinkClasses,
                    slaveAsOfJoinMapSinkClasses,
                    masterTimestampColumnIndex,
                    offsets,
                    keyTypes,
                    valueTypes,
                    groupByColumnFilter,
                    keyFunctions,
                    perWorkerKeyFunctions,
                    columnSources,
                    columnIndexes,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    filter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    workerCount
            );

            this.frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    filter != null ? FILTER_AND_REDUCE : REDUCE,
                    workerCount
            );

            this.cursor = new AsyncMultiHorizonJoinRecordCursor(
                    engine,
                    messageBus,
                    recordFunctions,
                    slaveFactories
            );
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return masterFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        frameSequence.of(masterFactory, executionContext, ORDER_ASC);
        cursor.of(frameSequence, executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Multi Horizon Join");
        sink.meta("workers").val(workerCount);
        sink.meta("offsets").val(offsets.size());
        sink.meta("slaves").val(slaveFactories.length);
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.setMetadata(horizonJoinMetadata);
        sink.optAttr("values", frameSequence.getAtom().getOwnerGroupByFunctions());
        sink.setMetadata(null);
        sink.child(masterFactory);
        for (RecordCursorFactory sf : slaveFactories) {
            sink.child(sf);
        }
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getFilterContext().getCompiledFilter() != null;
    }

    private static void aggregateRecord(
            MultiHorizonJoinRecord horizonJoinRecord,
            long masterRowId,
            GroupByMapFragment fragment,
            RecordSink groupByKeyCopier,
            GroupByFunctionsUpdater functionUpdater
    ) {
        final Map map = fragment.reopenMap();
        MapKey key = map.withKey();
        key.put(horizonJoinRecord, groupByKeyCopier);
        MapValue value = key.createValue();
        if (value.isNew()) {
            functionUpdater.updateNew(value, horizonJoinRecord, masterRowId);
        } else {
            functionUpdater.updateExisting(value, horizonJoinRecord, masterRowId);
        }
    }

    private static void aggregateRecordSharded(
            MultiHorizonJoinRecord horizonJoinRecord,
            long masterRowId,
            GroupByMapFragment fragment,
            RecordSink groupByKeyCopier,
            GroupByFunctionsUpdater functionUpdater
    ) {
        final Map lookupShard = fragment.getShards().getQuick(0);
        final MapKey lookupKey = lookupShard.withKey();
        lookupKey.put(horizonJoinRecord, groupByKeyCopier);
        lookupKey.commit();
        final long hashCode = lookupKey.hash();

        final Map shard = fragment.getShardMap(hashCode);
        final MapKey shardKey;
        if (shard != lookupShard) {
            shardKey = shard.withKey();
            shardKey.copyFrom(lookupKey);
        } else {
            shardKey = lookupKey;
        }

        MapValue shardValue = shardKey.createValue(hashCode);
        if (shardValue.isNew()) {
            functionUpdater.updateNew(shardValue, horizonJoinRecord, masterRowId);
        } else {
            functionUpdater.updateExisting(shardValue, horizonJoinRecord, masterRowId);
        }
    }

    /**
     * Page frame reducer for filtered multi-slave keyed HORIZON JOIN GROUP BY.
     * <p>
     * Applies filter first, then uses sorted horizon timestamp iteration for efficient
     * sequential ASOF lookups on filtered rows.
     */
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

        @SuppressWarnings("unchecked") final AsyncMultiHorizonJoinAtom atom =
                ((UnorderedPageFrameSequence<AsyncMultiHorizonJoinAtom>) frameSequence).getAtom();

        final long offsetCount = atom.getOffsetCount();
        if (offsetCount == 0) {
            return;
        }

        final boolean owner = stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final AsyncFilterContext filterCtx = atom.getFilterContext();
        final PageFrameAddressCache addressCache = frameSequence.getPageFrameAddressCache();
        final boolean isParquetFrame = addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET;
        final boolean useLateMaterialization = filterCtx.shouldUseLateMaterialization(slotId, isParquetFrame);

        final PageFrameMemoryPool frameMemoryPool = filterCtx.getMemoryPool(slotId);
        final PageFrameMemory frameMemory;
        if (useLateMaterialization) {
            frameMemory = frameMemoryPool.navigateTo(frameIndex, filterCtx.getFilterUsedColumnIndexes());
        } else {
            frameMemory = frameMemoryPool.navigateTo(frameIndex);
        }
        record.init(frameMemory);

        try {
            final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
            final GroupByMapFragment groupByMapFragment = atom.getFragment(slotId);
            final RecordSink groupByMapSink = atom.getMapSink(slotId);
            final int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
            final MultiHorizonJoinRecord horizonJoinRecord = atom.getHorizonJoinRecord(slotId);
            final CompiledFilter compiledFilter = filterCtx.getCompiledFilter();
            final Function filter = filterCtx.getFilter(slotId);
            final int slaveCount = atom.getSlaveCount();

            // Apply filter to master rows
            final DirectLongList rows = filterCtx.getFilteredRows(slotId);
            rows.clear();
            if (compiledFilter == null || frameMemory.hasColumnTops()) {
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
            record.setRowIndex(0);
            long baseRowId = record.getRowId();
            horizonIterator.ofFiltered(frameMemory.getPageAddress(masterTimestampColumnIndex), rows);

            processHorizonTimestamps(
                    atom,
                    horizonIterator,
                    slaveCount,
                    baseRowId,
                    record,
                    horizonJoinRecord,
                    groupByMapFragment,
                    groupByMapSink,
                    functionUpdater,
                    slotId
            );
        } finally {
            frameMemoryPool.releaseParquetBuffers();
            atom.release(slotId);
        }
    }

    /**
     * Process all horizon timestamps in sorted order, performing ASOF lookups
     * on each slave for each tuple.
     */
    private static void processHorizonTimestamps(
            AsyncMultiHorizonJoinAtom atom,
            AsyncHorizonTimestampIterator horizonIterator,
            int slaveCount,
            long baseRowId,
            PageFrameMemoryRecord masterRecord,
            MultiHorizonJoinRecord horizonJoinRecord,
            GroupByMapFragment groupByMapFragment,
            RecordSink groupByMapSink,
            GroupByFunctionsUpdater functionUpdater,
            int slotId
    ) {
        atom.resetLocalStats(groupByMapFragment.slotId);

        if (atom.isSharded()) {
            groupByMapFragment.shard();
        }

        final boolean sharded = !groupByMapFragment.isNotSharded();
        final Record[] matchedSlaveRecords = new Record[slaveCount];

        // Reset all slave helpers for this frame
        for (int s = 0; s < slaveCount; s++) {
            atom.getSlaveTimeFrameHelper(slotId, s).toTop();
            Map asOfMap = atom.getAsOfJoinMap(slotId, s);
            if (asOfMap != null) {
                asOfMap.clear();
            }
        }

        while (horizonIterator.next()) {
            final long horizonTs = horizonIterator.getHorizonTimestamp();
            final long masterRowIdx = horizonIterator.getMasterRowIndex();
            final int offsetIdx = horizonIterator.getOffsetIndex();
            final long offset = atom.getOffset(offsetIdx);

            masterRecord.setRowIndex(masterRowIdx, horizonIterator.getMasterRowCompactIndex());
            final long masterRowId = baseRowId + masterRowIdx;

            // ASOF lookup on each slave
            for (int s = 0; s < slaveCount; s++) {
                final HorizonJoinTimeFrameHelper helper = atom.getSlaveTimeFrameHelper(slotId, s);
                final long scaledHorizonTs = scaleTimestamp(horizonTs, atom.getMasterTimestampScale(s));
                long asOfRowId = helper.findAsOfRow(scaledHorizonTs);

                long matchRowId = Long.MIN_VALUE;
                final Map asOfJoinMap = atom.getAsOfJoinMap(slotId, s);
                final RecordSink masterSink = atom.getMasterAsOfJoinSink(slotId, s);
                final RecordSink slaveSink = atom.getSlaveAsOfJoinMapSink(slotId, s);

                if (asOfJoinMap != null && masterSink != null && slaveSink != null) {
                    // Keyed ASOF JOIN
                    final Record masterKeyRecord = atom.getMasterKeyRecord(slotId, s, masterRecord);
                    final SymbolTranslatingRecord symbolTranslatingRecord =
                            masterKeyRecord instanceof SymbolTranslatingRecord rec ? rec : null;

                    if (asOfRowId != Long.MIN_VALUE) {
                        if (helper.getForwardWatermark() == Long.MIN_VALUE) {
                            matchRowId = helper.backwardScanForKeyMatch(
                                    asOfRowId,
                                    masterKeyRecord,
                                    masterSink,
                                    slaveSink,
                                    asOfJoinMap,
                                    symbolTranslatingRecord
                            );
                            helper.initForwardWatermark(asOfRowId);
                        } else {
                            helper.forwardScanToPosition(asOfRowId, slaveSink, asOfJoinMap);

                            MapKey cacheKey = asOfJoinMap.withKey();
                            cacheKey.put(masterKeyRecord, masterSink);
                            MapValue cacheValue = cacheKey.findValue();

                            if (cacheValue != null) {
                                matchRowId = cacheValue.getLong(0);
                            } else {
                                matchRowId = helper.backwardScanForKeyMatch(
                                        asOfRowId,
                                        masterKeyRecord,
                                        masterSink,
                                        slaveSink,
                                        asOfJoinMap,
                                        symbolTranslatingRecord
                                );
                            }
                        }
                    }
                } else {
                    // Timestamp-only ASOF JOIN
                    matchRowId = asOfRowId;
                }

                if (matchRowId != Long.MIN_VALUE) {
                    helper.recordAt(matchRowId);
                    matchedSlaveRecords[s] = helper.getRecord();
                } else {
                    matchedSlaveRecords[s] = null;
                }
            }

            horizonJoinRecord.of(masterRecord, offset, horizonTs, matchedSlaveRecords);
            if (sharded) {
                aggregateRecordSharded(horizonJoinRecord, masterRowId, groupByMapFragment, groupByMapSink, functionUpdater);
            } else {
                aggregateRecord(horizonJoinRecord, masterRowId, groupByMapFragment, groupByMapSink, functionUpdater);
            }
        }

        atom.maybeEnableSharding(groupByMapFragment);
    }

    /**
     * Page frame reducer for multi-slave keyed HORIZON JOIN GROUP BY.
     * Uses sorted horizon timestamp iteration for efficient sequential ASOF lookups
     * on each slave.
     */
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

        @SuppressWarnings("unchecked") final AsyncMultiHorizonJoinAtom atom =
                ((UnorderedPageFrameSequence<AsyncMultiHorizonJoinAtom>) frameSequence).getAtom();

        final long offsetCount = atom.getOffsetCount();
        if (offsetCount == 0) {
            return;
        }

        final boolean owner = stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

        final AsyncFilterContext filterCtx = atom.getFilterContext();
        final PageFrameMemoryPool frameMemoryPool = filterCtx.getMemoryPool(slotId);
        final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
        record.init(frameMemory);

        try {
            final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
            final GroupByMapFragment groupByMapFragment = atom.getFragment(slotId);
            final RecordSink groupByMapSink = atom.getMapSink(slotId);
            final int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
            final MultiHorizonJoinRecord horizonJoinRecord = atom.getHorizonJoinRecord(slotId);
            final int slaveCount = atom.getSlaveCount();

            // Get horizon timestamp iterator and initialize for this frame
            final AsyncHorizonTimestampIterator horizonIterator = atom.getHorizonIterator(slotId);
            record.setRowIndex(0);
            long baseRowId = record.getRowId();
            horizonIterator.of(frameMemory.getPageAddress(masterTimestampColumnIndex), 0, frameRowCount);

            processHorizonTimestamps(
                    atom,
                    horizonIterator,
                    slaveCount,
                    baseRowId,
                    record,
                    horizonJoinRecord,
                    groupByMapFragment,
                    groupByMapSink,
                    functionUpdater,
                    slotId
            );
        } finally {
            frameMemoryPool.releaseParquetBuffers();
            atom.release(slotId);
        }
    }

    @Override
    protected void _close() {
        Misc.free(frameSequence);
        Misc.free(cursor);
        Misc.free(masterFactory);
        for (RecordCursorFactory sf : slaveFactories) {
            Misc.free(sf);
        }
        Misc.free(horizonJoinMetadata);
        Misc.freeObjList(recordFunctions);
    }
}

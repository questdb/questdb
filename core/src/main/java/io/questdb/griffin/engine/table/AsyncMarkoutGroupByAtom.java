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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.griffin.engine.table.AsyncFilterUtils.prepareBindVarMemory;

/**
 * Atom that manages per-worker resources for parallel markout query execution.
 * <p>
 * This class holds:
 * 1. Per-worker time frame helpers for ASOF JOIN lookups via ConcurrentTimeFrameCursor
 * 2. Per-worker aggregation maps and group by functions
 * 3. Per-worker ASOF join maps for symbol -> rowId mappings
 */
public class AsyncMarkoutGroupByAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledFilter;
    private final RecordSink groupByKeyCopier;
    private final RecordSink masterKeyCopier;
    private final int masterTimestampColumnIndex;
    private final GroupByAllocator ownerAllocator;
    private final Map ownerAsOfJoinMap;
    private final CombinedRecord ownerCombinedRecord;
    private final Function ownerFilter;
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final Map ownerMap;
    private final ConcurrentTimeFrameCursor ownerSlaveTimeFrameCursor;
    private final MarkoutTimeFrameHelper ownerSlaveTimeFrameHelper;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<Map> perWorkerAsOfJoinMaps;
    private final ObjList<CombinedRecord> perWorkerCombinedRecords;
    private final ObjList<Function> perWorkerFilters;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<Map> perWorkerMaps;
    private final LongList perWorkerPrevFirstOffsetAsOfRowIds;
    private final ObjList<ConcurrentTimeFrameCursor> perWorkerSlaveTimeFrameCursors;
    private final ObjList<MarkoutTimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    private final int sequenceColumnIndex;
    private final LongList sequenceOffsetValues = new LongList();
    private final RecordSink slaveKeyCopier;
    private final int slaveTimestampIndex;
    private long ownerPrevFirstOffsetAsOfRowId = Long.MIN_VALUE;
    private long sequenceRowCount;

    public AsyncMarkoutGroupByAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            int masterTimestampColumnIndex,
            int sequenceColumnIndex,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterKeyCopier,
            @Nullable RecordSink slaveKeyCopier,
            int slaveTimestampIndex,
            @NotNull RecordSink groupByKeyCopier,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndices,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerFilter,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        assert slaveFactory.supportsTimeFrameCursor();
        assert perWorkerGroupByFunctions == null || perWorkerGroupByFunctions.size() == workerCount;
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;

        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.sequenceColumnIndex = sequenceColumnIndex;

            // Filter resources
            this.compiledFilter = compiledFilter;
            this.bindVarMemory = bindVarMemory;
            this.bindVarFunctions = bindVarFunctions;
            this.ownerFilter = ownerFilter;
            this.perWorkerFilters = perWorkerFilters;

            // ASOF join lookup resources
            this.masterKeyCopier = masterKeyCopier;
            this.slaveKeyCopier = slaveKeyCopier;
            this.slaveTimestampIndex = slaveTimestampIndex;

            // GROUP BY key copier and column mappings for CombinedRecord
            this.groupByKeyCopier = groupByKeyCopier;

            // Per-worker locks
            this.perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

            // Create time frame cursors from slave factory - one per worker + owner
            final long lookahead = configuration.getSqlAsOfJoinLookAhead();
            this.ownerSlaveTimeFrameCursor = slaveFactory.newTimeFrameCursor();
            this.ownerSlaveTimeFrameHelper = new MarkoutTimeFrameHelper(lookahead);
            this.perWorkerSlaveTimeFrameCursors = new ObjList<>(slotCount);
            this.perWorkerSlaveTimeFrameHelpers = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveTimeFrameCursors.add(slaveFactory.newTimeFrameCursor());
                perWorkerSlaveTimeFrameHelpers.add(new MarkoutTimeFrameHelper(lookahead));
            }

            // Per-worker ASOF maps
            this.perWorkerAsOfJoinMaps = new ObjList<>(slotCount);
            if (asOfJoinKeyTypes != null) {
                ArrayColumnTypes asOfValueTypes = new ArrayColumnTypes();
                asOfValueTypes.add(ColumnType.LONG);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerAsOfJoinMaps.add(MapFactory.createUnorderedMap(configuration, asOfJoinKeyTypes, asOfValueTypes));
                }
                this.ownerAsOfJoinMap = MapFactory.createUnorderedMap(configuration, asOfJoinKeyTypes, asOfValueTypes);
            } else {
                for (int i = 0; i < slotCount; i++) {
                    perWorkerAsOfJoinMaps.add(null);
                }
                this.ownerAsOfJoinMap = null;
            }

            // Per-worker aggregation maps
            this.perWorkerMaps = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMaps.add(MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes));
            }
            this.ownerMap = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);

            // Group by functions and updaters
            this.ownerGroupByFunctions = ownerGroupByFunctions;
            this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;

            final Class<GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
            this.ownerFunctionUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, ownerGroupByFunctions);
            this.perWorkerFunctionUpdaters = new ObjList<>(slotCount);
            if (perWorkerGroupByFunctions != null) {
                for (int i = 0; i < slotCount; i++) {
                    perWorkerFunctionUpdaters.add(GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerGroupByFunctions.getQuick(i)));
                }
            } else {
                for (int i = 0; i < slotCount; i++) {
                    perWorkerFunctionUpdaters.add(ownerFunctionUpdater);
                }
            }

            // Allocators
            this.ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            if (perWorkerGroupByFunctions != null) {
                this.perWorkerAllocators = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    perWorkerAllocators.add(allocator);
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), allocator);
                }
            } else {
                perWorkerAllocators = null;
            }

            // Per-worker combined records
            this.ownerCombinedRecord = new CombinedRecord();
            this.ownerCombinedRecord.init(columnSources, columnIndices);
            this.perWorkerCombinedRecords = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                CombinedRecord record = new CombinedRecord();
                record.init(columnSources, columnIndices);
                perWorkerCombinedRecords.add(record);
            }

            // Per-worker previous master row's first offset ASOF position;
            // Used as the low boundary for the ASOF JOIN row search in the next reduced frame
            this.perWorkerPrevFirstOffsetAsOfRowIds = new LongList(slotCount);
            perWorkerPrevFirstOffsetAsOfRowIds.setAll(slotCount, Long.MIN_VALUE);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        // Clear sequence data
        sequenceOffsetValues.clear();
        sequenceRowCount = 0;

        // Clear aggregation resources
        Misc.free(ownerMap);
        Misc.freeObjListAndKeepObjects(perWorkerMaps);
        Misc.clearObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.clear(ownerAllocator);
        Misc.clearObjList(perWorkerAllocators);

        // Clear ASOF join maps
        if (ownerAsOfJoinMap != null) {
            Misc.free(ownerAsOfJoinMap);
        }
        Misc.freeObjListAndKeepObjects(perWorkerAsOfJoinMaps);

        // Clear time frame cursors
        Misc.free(ownerSlaveTimeFrameCursor);
        Misc.freeObjListAndKeepObjects(perWorkerSlaveTimeFrameCursors);

        // Clear previous master row's first offset ASOF positions
        ownerPrevFirstOffsetAsOfRowId = Long.MIN_VALUE;
        perWorkerPrevFirstOffsetAsOfRowIds.setAll(perWorkerPrevFirstOffsetAsOfRowIds.size(), Long.MIN_VALUE);
    }

    @Override
    public void close() {
        Misc.free(ownerMap);
        Misc.freeObjList(perWorkerMaps);
        Misc.free(ownerAllocator);
        Misc.freeObjList(perWorkerAllocators);
        Misc.freeObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.freeObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.free(ownerAsOfJoinMap);
        Misc.freeObjList(perWorkerAsOfJoinMaps);
        Misc.free(ownerSlaveTimeFrameCursor);
        Misc.freeObjList(perWorkerSlaveTimeFrameCursors);
        // Filter resources
        Misc.free(compiledFilter);
        Misc.free(bindVarMemory);
        Misc.freeObjList(bindVarFunctions);
        Misc.free(ownerFilter);
        Misc.freeObjList(perWorkerFilters);
    }

    public Map getAsOfJoinMap(int slotId) {
        Map map;
        if (slotId == -1) {
            map = ownerAsOfJoinMap;
        } else {
            map = perWorkerAsOfJoinMaps.getQuick(slotId);
        }
        if (map != null && !map.isOpen()) {
            map.reopen();
        }
        return map;
    }

    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    public CombinedRecord getCombinedRecord(int slotId) {
        if (slotId == -1) {
            return ownerCombinedRecord;
        }
        return perWorkerCombinedRecords.getQuick(slotId);
    }

    public CompiledFilter getCompiledFilter() {
        return compiledFilter;
    }

    public Function getFilter(int slotId) {
        if (slotId == -1 || perWorkerFilters == null) {
            return ownerFilter;
        }
        return perWorkerFilters.getQuick(slotId);
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    public RecordSink getGroupByKeyCopier() {
        return groupByKeyCopier;
    }

    public Map getMap(int slotId) {
        Map map;
        if (slotId == -1) {
            map = ownerMap;
        } else {
            map = perWorkerMaps.getQuick(slotId);
        }
        if (!map.isOpen()) {
            map.reopen();
        }
        return map;
    }

    public RecordSink getMasterKeyCopier() {
        return masterKeyCopier;
    }

    public int getMasterTimestampColumnIndex() {
        return masterTimestampColumnIndex;
    }

    public ObjList<GroupByFunction> getOwnerGroupByFunctions() {
        return ownerGroupByFunctions;
    }

    public long getPrevFirstOffsetAsOfRowId(int slotId) {
        if (slotId == -1) {
            return ownerPrevFirstOffsetAsOfRowId;
        }
        return perWorkerPrevFirstOffsetAsOfRowIds.getQuick(slotId);
    }

    /**
     * Get the pre-computed sequence offset value at the given index.
     */
    public long getSequenceOffsetValue(int index) {
        return sequenceOffsetValues.getQuick(index);
    }

    public long getSequenceRowCount() {
        return sequenceRowCount;
    }

    public RecordSink getSlaveKeyCopier() {
        return slaveKeyCopier;
    }

    /**
     * Get the time frame helper for the given slot.
     */
    public MarkoutTimeFrameHelper getSlaveTimeFrameHelper(int slotId) {
        if (slotId == -1) {
            return ownerSlaveTimeFrameHelper;
        }
        return perWorkerSlaveTimeFrameHelpers.getQuick(slotId);
    }

    public int getSlaveTimestampIndex() {
        return slaveTimestampIndex;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        // Initialize filter functions
        if (ownerFilter != null) {
            ownerFilter.init(symbolTableSource, executionContext);
        }

        if (perWorkerFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerFilters, symbolTableSource, executionContext, ownerFilter);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

        // Initialize bind variables for compiled filter
        if (bindVarFunctions != null) {
            Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
            prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }

        // Initialize group by functions
        for (int i = 0, n = ownerGroupByFunctions.size(); i < n; i++) {
            ownerGroupByFunctions.getQuick(i).init(symbolTableSource, executionContext);
        }
        if (perWorkerGroupByFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                    ObjList<GroupByFunction> functions = perWorkerGroupByFunctions.getQuick(i);
                    for (int j = 0, m = functions.size(); j < m; j++) {
                        functions.getQuick(j).init(symbolTableSource, executionContext);
                    }
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
    }

    /**
     * Initialize all time frame cursors with shared frame data.
     * Must be called after the slave page frame cursor has been fully iterated.
     */
    public void initTimeFrameCursors(
            TablePageFrameCursor slavePageFrameCursor,
            PageFrameAddressCache slaveFrameAddressCache,
            IntList slaveFramePartitionIndexes,
            LongList slaveFrameRowCounts,
            LongList slavePartitionTimestamps,
            int frameCount
    ) throws SqlException {
        // Initialize owner cursor
        ownerSlaveTimeFrameCursor.of(
                slavePageFrameCursor,
                slaveFrameAddressCache,
                slaveFramePartitionIndexes,
                slaveFrameRowCounts,
                slavePartitionTimestamps,
                frameCount
        );
        ownerSlaveTimeFrameHelper.of(ownerSlaveTimeFrameCursor);

        // Initialize per-worker cursors with the same shared data
        for (int i = 0, n = perWorkerSlaveTimeFrameCursors.size(); i < n; i++) {
            ConcurrentTimeFrameCursor workerCursor = perWorkerSlaveTimeFrameCursors.getQuick(i);
            workerCursor.of(
                    slavePageFrameCursor,
                    slaveFrameAddressCache,
                    slaveFramePartitionIndexes,
                    slaveFrameRowCounts,
                    slavePartitionTimestamps,
                    frameCount
            );
            perWorkerSlaveTimeFrameHelpers.getQuick(i).of(workerCursor);
        }

        // Reopen ASOF maps
        if (ownerAsOfJoinMap != null) {
            ownerAsOfJoinMap.reopen();
        }
        for (int i = 0, n = perWorkerAsOfJoinMaps.size(); i < n; i++) {
            Map map = perWorkerAsOfJoinMaps.getQuick(i);
            if (map != null) {
                map.reopen();
            }
        }

        // Clear previous master row's first offset ASOF positions
        ownerPrevFirstOffsetAsOfRowId = Long.MIN_VALUE;
        perWorkerPrevFirstOffsetAsOfRowIds.setAll(perWorkerPrevFirstOffsetAsOfRowIds.size(), Long.MIN_VALUE);
    }

    /**
     * Materialize the sequence cursor (offset records) and cache offset values.
     * Must be called once before dispatching tasks.
     */
    public void materializeSequenceCursor(RecordCursor sequenceCursor, SqlExecutionCircuitBreaker circuitBreaker) {
        sequenceOffsetValues.clear();

        Record sequenceRecord = sequenceCursor.getRecord();
        while (sequenceCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            long offsetValue = sequenceRecord.getLong(sequenceColumnIndex);
            sequenceOffsetValues.add(offsetValue);
        }
        sequenceRowCount = sequenceOffsetValues.size();
    }

    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    /**
     * Merge all per-worker maps into the owner map.
     */
    public Map mergeOwnerMap() {
        Map destMap = ownerMap;
        if (!destMap.isOpen()) {
            destMap.reopen();
        }

        for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
            Map srcMap = perWorkerMaps.getQuick(i);
            if (srcMap.isOpen() && srcMap.size() > 0) {
                destMap.merge(srcMap, ownerFunctionUpdater);
                srcMap.close();
            }
        }

        return destMap;
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        // Maps and cursors will be opened lazily
        // The maps will be open lazily by worker threads, but we need to reopen the allocators.
        ownerAllocator.reopen();
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                perWorkerAllocators.getQuick(i).reopen();
            }
        }
    }

    public void setPrevFirstOffsetAsOfRowId(int slotId, long rowId) {
        if (slotId == -1) {
            this.ownerPrevFirstOffsetAsOfRowId = rowId;
        } else {
            perWorkerPrevFirstOffsetAsOfRowIds.setQuick(slotId, rowId);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("AsyncMarkoutGroupByAtom");
    }

    public void toTop() {
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }
}

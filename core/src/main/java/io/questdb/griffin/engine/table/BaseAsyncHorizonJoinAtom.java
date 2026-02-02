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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
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
 * Base class for HORIZON JOIN atoms that manages common per-worker resources.
 * <p>
 * This class holds:
 * 1. Per-worker time frame helpers for ASOF JOIN lookups via ConcurrentTimeFrameCursor
 * 2. Per-worker group by functions and updaters
 * 3. Per-worker ASOF join maps for symbol -> rowId mappings (when keyed join)
 * 4. Filter resources (compiled and Java filters)
 */
public abstract class BaseAsyncHorizonJoinAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    protected final ObjList<Function> bindVarFunctions;
    protected final MemoryCARW bindVarMemory;
    protected final CompiledFilter compiledFilter;
    protected final MarkoutSymbolTableSource markoutSymbolTableSource;
    protected final RecordSink masterKeyCopier;
    protected final int masterTimestampColumnIndex;
    protected final long masterTsScale;
    protected final LongList offsets;
    protected final GroupByAllocator ownerAllocator;
    protected final Map ownerAsOfJoinMap;
    protected final MarkoutRecord ownerCombinedRecord;
    protected final Function ownerFilter;
    protected final GroupByFunctionsUpdater ownerFunctionUpdater;
    protected final ObjList<GroupByFunction> ownerGroupByFunctions;
    // Per-worker horizon timestamp iterators for sorted processing
    protected final HorizonTimestampIterator ownerHorizonIterator;
    protected final ConcurrentTimeFrameCursor ownerSlaveTimeFrameCursor;
    protected final MarkoutTimeFrameHelper ownerSlaveTimeFrameHelper;
    protected final ObjList<GroupByAllocator> perWorkerAllocators;
    protected final ObjList<Map> perWorkerAsOfJoinMaps;
    protected final ObjList<MarkoutRecord> perWorkerCombinedRecords;
    protected final ObjList<Function> perWorkerFilters;
    protected final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    protected final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    protected final ObjList<HorizonTimestampIterator> perWorkerHorizonIterators;
    protected final PerWorkerLocks perWorkerLocks;
    protected final ObjList<ConcurrentTimeFrameCursor> perWorkerSlaveTimeFrameCursors;
    protected final ObjList<MarkoutTimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    protected final long sequenceRowCount;
    protected final RecordSink slaveKeyCopier;
    protected final int slotCount;

    protected BaseAsyncHorizonJoinAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            int masterTimestampColumnIndex,
            @NotNull LongList offsets,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterKeyCopier,
            @Nullable RecordSink slaveKeyCopier,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerFilter,
            @Nullable ObjList<Function> perWorkerFilters,
            long masterTsScale,
            long slaveTsScale,
            int workerCount
    ) {
        assert slaveFactory.supportsTimeFrameCursor();
        assert perWorkerGroupByFunctions == null || perWorkerGroupByFunctions.size() == workerCount;
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;

        this.slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        this.masterTimestampColumnIndex = masterTimestampColumnIndex;
        this.offsets = offsets;
        this.sequenceRowCount = offsets.size();
        this.markoutSymbolTableSource = new MarkoutSymbolTableSource(columnSources, columnIndexes);

        // Filter resources
        this.compiledFilter = compiledFilter;
        this.bindVarMemory = bindVarMemory;
        this.bindVarFunctions = bindVarFunctions;
        this.ownerFilter = ownerFilter;
        this.perWorkerFilters = perWorkerFilters;

        // ASOF join lookup resources
        this.masterKeyCopier = masterKeyCopier;
        this.slaveKeyCopier = slaveKeyCopier;

        // Timestamp scale factor for cross-resolution support (1 if same type, otherwise scale to nanos)
        this.masterTsScale = masterTsScale;

        // Per-worker locks
        this.perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

        // Create time frame cursors from slave factory - one per worker + owner
        final long lookahead = configuration.getSqlAsOfJoinLookAhead();
        this.ownerSlaveTimeFrameCursor = slaveFactory.newTimeFrameCursor();
        this.ownerSlaveTimeFrameHelper = new MarkoutTimeFrameHelper(lookahead, slaveTsScale);
        this.perWorkerSlaveTimeFrameCursors = new ObjList<>(slotCount);
        this.perWorkerSlaveTimeFrameHelpers = new ObjList<>(slotCount);
        for (int i = 0; i < slotCount; i++) {
            perWorkerSlaveTimeFrameCursors.add(slaveFactory.newTimeFrameCursor());
            perWorkerSlaveTimeFrameHelpers.add(new MarkoutTimeFrameHelper(lookahead, slaveTsScale));
        }

        // Per-worker ASOF maps and SingleRecordSink targets for key comparison
        if (asOfJoinKeyTypes != null) {
            this.perWorkerAsOfJoinMaps = new ObjList<>(slotCount);
            final SingleColumnType asOfValueTypes = new SingleColumnType(ColumnType.LONG);
            for (int i = 0; i < slotCount; i++) {
                perWorkerAsOfJoinMaps.add(MapFactory.createUnorderedMap(configuration, asOfJoinKeyTypes, asOfValueTypes));
            }
            this.ownerAsOfJoinMap = MapFactory.createUnorderedMap(configuration, asOfJoinKeyTypes, asOfValueTypes);
        } else {
            this.perWorkerAsOfJoinMaps = null;
            this.ownerAsOfJoinMap = null;
        }

        // Group by functions and updaters
        this.ownerGroupByFunctions = ownerGroupByFunctions;
        this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;

        final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
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
        this.ownerCombinedRecord = new MarkoutRecord();
        ownerCombinedRecord.init(columnSources, columnIndexes);
        this.perWorkerCombinedRecords = new ObjList<>(slotCount);
        for (int i = 0; i < slotCount; i++) {
            MarkoutRecord record = new MarkoutRecord();
            record.init(columnSources, columnIndexes);
            perWorkerCombinedRecords.add(record);
        }

        // Per-worker horizon timestamp iterators for sorted processing
        this.ownerHorizonIterator = new HorizonTimestampIterator(offsets, masterTsScale);
        this.perWorkerHorizonIterators = new ObjList<>(slotCount);
        for (int i = 0; i < slotCount; i++) {
            perWorkerHorizonIterators.add(new HorizonTimestampIterator(offsets, masterTsScale));
        }
    }

    @Override
    public void clear() {
        // Clear group by functions
        Misc.clearObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.clear(ownerAllocator);
        Misc.clearObjList(perWorkerAllocators);

        // Clear ASOF join maps
        Misc.free(ownerAsOfJoinMap);
        Misc.freeObjListAndKeepObjects(perWorkerAsOfJoinMaps);

        // Clear time frame cursors
        Misc.free(ownerSlaveTimeFrameCursor);
        Misc.freeObjListAndKeepObjects(perWorkerSlaveTimeFrameCursors);

        // Let subclass clear its resources
        clearAggregationState();
    }

    @Override
    public void close() {
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
        // Horizon timestamp iterators
        Misc.free(ownerHorizonIterator);
        Misc.freeObjList(perWorkerHorizonIterators);
        // Filter resources
        Misc.free(compiledFilter);
        Misc.free(bindVarMemory);
        Misc.freeObjList(bindVarFunctions);
        Misc.free(ownerFilter);
        Misc.freeObjList(perWorkerFilters);

        // Let subclass close its resources
        closeAggregationState();
    }

    public Map getAsOfJoinMap(int slotId) {
        if (slotId == -1) {
            return ownerAsOfJoinMap;
        }
        return perWorkerAsOfJoinMaps != null ? perWorkerAsOfJoinMaps.getQuick(slotId) : null;
    }

    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    public MarkoutRecord getCombinedRecord(int slotId) {
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

    /**
     * Get the horizon timestamp iterator for the given slot.
     * Used for sorted processing of horizon timestamps within a page frame.
     */
    public HorizonTimestampIterator getHorizonIterator(int slotId) {
        if (slotId == -1) {
            return ownerHorizonIterator;
        }
        return perWorkerHorizonIterators.getQuick(slotId);
    }

    public RecordSink getMasterKeyCopier() {
        return masterKeyCopier;
    }

    public int getMasterTimestampColumnIndex() {
        return masterTimestampColumnIndex;
    }

    /**
     * Get the offset value at the given index. Offsets are in master's scale.
     */
    public long getOffset(int index) {
        return offsets.getQuick(index);
    }

    public ObjList<GroupByFunction> getOwnerGroupByFunctions() {
        return ownerGroupByFunctions;
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

        // Note: group by functions are initialized in initTimeFrameCursors() where we have
        // access to both master and slave symbol table sources
    }

    /**
     * Initialize all time frame cursors with shared frame data.
     * Must be called after the slave page frame cursor has been fully iterated.
     */
    public void initTimeFrameCursors(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSymbolTableSource,
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

        // Initialize group by functions with combined symbol table source
        markoutSymbolTableSource.of(masterSymbolTableSource, slavePageFrameCursor);
        for (int i = 0, n = ownerGroupByFunctions.size(); i < n; i++) {
            ownerGroupByFunctions.getQuick(i).init(markoutSymbolTableSource, executionContext);
        }
        if (perWorkerGroupByFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                    ObjList<GroupByFunction> functions = perWorkerGroupByFunctions.getQuick(i);
                    for (int j = 0, m = functions.size(); j < m; j++) {
                        functions.getQuick(j).init(markoutSymbolTableSource, executionContext);
                    }
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

        // Reopen ASOF maps
        if (ownerAsOfJoinMap != null) {
            ownerAsOfJoinMap.reopen();
        }
        if (perWorkerAsOfJoinMaps != null) {
            for (int i = 0, n = perWorkerAsOfJoinMaps.size(); i < n; i++) {
                Map map = perWorkerAsOfJoinMaps.getQuick(i);
                if (map != null) {
                    map.reopen();
                }
            }
        }

    }

    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        // The maps/values will be opened lazily by worker threads, but we need to reopen the allocators.
        ownerAllocator.reopen();
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                perWorkerAllocators.getQuick(i).reopen();
            }
        }
    }

    public void toTop() {
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }

    /**
     * Clear aggregation-specific state. Called by {@link #clear()}.
     */
    protected abstract void clearAggregationState();

    /**
     * Close aggregation-specific resources. Called by {@link #close()}.
     */
    protected abstract void closeAggregationState();

    // package-private to make linter happy
    MarkoutSymbolTableSource getMarkoutSymbolTableSource() {
        return markoutSymbolTableSource;
    }
}

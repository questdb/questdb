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
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.Record;
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
import io.questdb.std.DirectIntList;
import io.questdb.std.IntHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Base class for multi-slave HORIZON JOIN atoms that manages per-worker x per-slave resources.
 * <p>
 * This class holds:
 * 1. Per-worker x per-slave time frame helpers for ASOF JOIN lookups via ConcurrentTimeFrameCursor
 * 2. Per-worker group by functions and updaters (shared across slaves)
 * 3. Per-worker x per-slave ASOF join maps for symbol -> rowId mappings (when keyed join)
 * 4. Per-worker x per-slave RecordSinks (RecordSink instances have mutable state and must not be shared)
 * 5. Filter resources (compiled and Java filters, shared across slaves)
 */
public abstract class BaseAsyncMultiHorizonJoinAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    protected final AsyncFilterContext filterCtx;
    protected final int masterTimestampColumnIndex;
    protected final long offsetCount;
    protected final LongList offsets;
    protected final GroupByAllocator ownerAllocator;
    protected final Map[] ownerAsOfJoinMaps;
    protected final MultiHorizonJoinRecord ownerCombinedRecord;
    protected final GroupByFunctionsUpdater ownerFunctionUpdater;
    protected final ObjList<GroupByFunction> ownerGroupByFunctions;
    protected final AsyncHorizonTimestampIterator ownerHorizonIterator;
    protected final ObjList<RecordSink> ownerMasterAsOfJoinSinks;
    protected final ObjList<RecordSink> ownerSlaveAsOfJoinSinks;
    protected final ConcurrentTimeFrameCursor[] ownerSlaveTimeFrameCursors;
    protected final HorizonJoinTimeFrameHelper[] ownerSlaveTimeFrameHelpers;
    protected final SymbolTranslatingRecord[] ownerSymbolTranslatingRecords;
    protected final long[] perSlaveMasterTsScales;
    protected final ObjList<GroupByAllocator> perWorkerAllocators;
    protected final ObjList<Map> perWorkerAsOfJoinMaps;
    protected final ObjList<MultiHorizonJoinRecord> perWorkerCombinedRecords;
    protected final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    protected final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    protected final ObjList<AsyncHorizonTimestampIterator> perWorkerHorizonIterators;
    protected final PerWorkerLocks perWorkerLocks;
    protected final ObjList<ObjList<RecordSink>> perWorkerMasterAsOfJoinSinks;
    protected final ObjList<ObjList<RecordSink>> perWorkerSlaveAsOfJoinSinks;
    protected final ObjList<ConcurrentTimeFrameCursor> perWorkerSlaveTimeFrameCursors;
    protected final ObjList<HorizonJoinTimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    protected final ObjList<SymbolTranslatingRecord> perWorkerSymbolTranslatingRecords;
    protected final int slaveCount;
    protected final int workerCount;
    private final MultiHorizonJoinSymbolTableSource horizonJoinSymbolTableSource;

    protected BaseAsyncMultiHorizonJoinAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull HorizonJoinSlaveState[] slaveStates,
            @Nullable ColumnTypes[] perSlaveAsOfJoinKeyTypes,
            @Nullable Class<RecordSink> @NotNull [] masterAsOfJoinMapSinkClasses,
            @Nullable Class<RecordSink> @NotNull [] slaveAsOfJoinMapSinkClasses,
            int masterTimestampColumnIndex,
            @NotNull LongList offsets,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerFilter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        assert slaveStates.length > 0;
        assert perWorkerGroupByFunctions == null || perWorkerGroupByFunctions.size() == workerCount;
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;

        this.slaveCount = slaveStates.length;
        this.workerCount = workerCount;
        this.masterTimestampColumnIndex = masterTimestampColumnIndex;
        this.offsets = offsets;
        this.offsetCount = offsets.size();
        this.horizonJoinSymbolTableSource = new MultiHorizonJoinSymbolTableSource(columnSources, columnIndexes, slaveCount);

        // Filter and memory pool resources (ownership transferred from caller)
        this.filterCtx = new AsyncFilterContext(
                configuration,
                compiledFilter,
                bindVarMemory,
                bindVarFunctions,
                ownerFilter,
                filterUsedColumnIndexes,
                perWorkerFilters,
                workerCount,
                workerCount,
                1, // owner memory pool capacity
                1  // per-worker memory pool capacity
        );

        // Group by functions (ownership transferred from caller)
        this.ownerGroupByFunctions = ownerGroupByFunctions;
        this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;

        try {
            // Per-worker locks
            this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);

            // Per-slave master timestamp scales and per-worker sinks
            this.perSlaveMasterTsScales = new long[slaveCount];
            for (int s = 0; s < slaveCount; s++) {
                perSlaveMasterTsScales[s] = slaveStates[s].getMasterTsScale();
            }
            this.ownerMasterAsOfJoinSinks = new ObjList<>(slaveCount);
            this.ownerSlaveAsOfJoinSinks = new ObjList<>(slaveCount);
            this.perWorkerMasterAsOfJoinSinks = new ObjList<>(workerCount);
            this.perWorkerSlaveAsOfJoinSinks = new ObjList<>(workerCount);
            for (int w = 0; w < workerCount; w++) {
                perWorkerMasterAsOfJoinSinks.add(new ObjList<>(slaveCount));
                perWorkerSlaveAsOfJoinSinks.add(new ObjList<>(slaveCount));
            }
            for (int s = 0; s < slaveCount; s++) {
                if (masterAsOfJoinMapSinkClasses[s] != null) {
                    ownerMasterAsOfJoinSinks.add(RecordSinkFactory.getInstance(masterAsOfJoinMapSinkClasses[s], null, null, null, null, null, null, null));
                    ownerSlaveAsOfJoinSinks.add(RecordSinkFactory.getInstance(slaveAsOfJoinMapSinkClasses[s], null, null, null, null, null, null, null));
                    for (int w = 0; w < workerCount; w++) {
                        perWorkerMasterAsOfJoinSinks.getQuick(w).add(RecordSinkFactory.getInstance(masterAsOfJoinMapSinkClasses[s], null, null, null, null, null, null, null));
                        perWorkerSlaveAsOfJoinSinks.getQuick(w).add(RecordSinkFactory.getInstance(slaveAsOfJoinMapSinkClasses[s], null, null, null, null, null, null, null));
                    }
                } else {
                    ownerMasterAsOfJoinSinks.add(null);
                    ownerSlaveAsOfJoinSinks.add(null);
                    for (int w = 0; w < workerCount; w++) {
                        perWorkerMasterAsOfJoinSinks.getQuick(w).add(null);
                        perWorkerSlaveAsOfJoinSinks.getQuick(w).add(null);
                    }
                }
            }

            // Create time frame cursors from slave factories - one per worker + owner per slave
            final long lookahead = configuration.getSqlAsOfJoinLookAhead();
            this.ownerSlaveTimeFrameCursors = new ConcurrentTimeFrameCursor[slaveCount];
            this.ownerSlaveTimeFrameHelpers = new HorizonJoinTimeFrameHelper[slaveCount];
            this.perWorkerSlaveTimeFrameCursors = new ObjList<>(workerCount * slaveCount);
            this.perWorkerSlaveTimeFrameHelpers = new ObjList<>(workerCount * slaveCount);
            for (int s = 0; s < slaveCount; s++) {
                HorizonJoinSlaveState state = slaveStates[s];
                ownerSlaveTimeFrameCursors[s] = state.getFactory().newTimeFrameCursor();
                ownerSlaveTimeFrameHelpers[s] = new HorizonJoinTimeFrameHelper(lookahead, state.getSlaveTsScale());
                for (int w = 0; w < workerCount; w++) {
                    perWorkerSlaveTimeFrameCursors.add(state.getFactory().newTimeFrameCursor());
                    perWorkerSlaveTimeFrameHelpers.add(new HorizonJoinTimeFrameHelper(lookahead, state.getSlaveTsScale()));
                }
            }

            // Per-worker x per-slave ASOF maps
            final SingleColumnType asOfValueTypes = new SingleColumnType(ColumnType.LONG);
            this.ownerAsOfJoinMaps = new Map[slaveCount];
            this.perWorkerAsOfJoinMaps = new ObjList<>(workerCount * slaveCount);
            for (int s = 0; s < slaveCount; s++) {
                if (perSlaveAsOfJoinKeyTypes != null && perSlaveAsOfJoinKeyTypes[s] != null) {
                    ownerAsOfJoinMaps[s] = MapFactory.createUnorderedMap(configuration, perSlaveAsOfJoinKeyTypes[s], asOfValueTypes);
                    for (int w = 0; w < workerCount; w++) {
                        perWorkerAsOfJoinMaps.add(MapFactory.createUnorderedMap(configuration, perSlaveAsOfJoinKeyTypes[s], asOfValueTypes));
                    }
                } else {
                    ownerAsOfJoinMaps[s] = null;
                    for (int w = 0; w < workerCount; w++) {
                        perWorkerAsOfJoinMaps.add(null);
                    }
                }
            }

            // Per-worker x per-slave symbol translating records
            this.ownerSymbolTranslatingRecords = new SymbolTranslatingRecord[slaveCount];
            this.perWorkerSymbolTranslatingRecords = new ObjList<>(workerCount * slaveCount);
            for (int s = 0; s < slaveCount; s++) {
                HorizonJoinSlaveState state = slaveStates[s];
                if (state.getMasterSymbolKeyColumnIndices() != null) {
                    ownerSymbolTranslatingRecords[s] = new SymbolTranslatingRecord(
                            state.getMasterColumnCount(),
                            state.getMasterSymbolKeyColumnIndices(),
                            state.getSlaveSymbolKeyColumnIndices()
                    );
                    for (int w = 0; w < workerCount; w++) {
                        perWorkerSymbolTranslatingRecords.add(new SymbolTranslatingRecord(
                                state.getMasterColumnCount(),
                                state.getMasterSymbolKeyColumnIndices(),
                                state.getSlaveSymbolKeyColumnIndices()
                        ));
                    }
                } else {
                    ownerSymbolTranslatingRecords[s] = null;
                    for (int w = 0; w < workerCount; w++) {
                        perWorkerSymbolTranslatingRecords.add(null);
                    }
                }
            }

            // Group by updaters (shared across slaves)
            final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
            this.ownerFunctionUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, ownerGroupByFunctions);
            this.perWorkerFunctionUpdaters = new ObjList<>(workerCount);
            if (perWorkerGroupByFunctions != null) {
                for (int i = 0; i < workerCount; i++) {
                    perWorkerFunctionUpdaters.add(GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerGroupByFunctions.getQuick(i)));
                }
            } else {
                for (int i = 0; i < workerCount; i++) {
                    perWorkerFunctionUpdaters.add(ownerFunctionUpdater);
                }
            }

            // Allocators (shared across slaves)
            this.ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            if (perWorkerGroupByFunctions != null) {
                this.perWorkerAllocators = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    perWorkerAllocators.add(allocator);
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), allocator);
                }
            } else {
                perWorkerAllocators = null;
            }

            // Per-worker combined records (shared across slaves)
            this.ownerCombinedRecord = new MultiHorizonJoinRecord(slaveCount);
            ownerCombinedRecord.init(columnSources, columnIndexes);
            this.perWorkerCombinedRecords = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                MultiHorizonJoinRecord record = new MultiHorizonJoinRecord(slaveCount);
                record.init(columnSources, columnIndexes);
                perWorkerCombinedRecords.add(record);
            }

            // Per-worker horizon timestamp iterators (shared across slaves)
            this.ownerHorizonIterator = new AsyncHorizonTimestampIterator(offsets);
            this.perWorkerHorizonIterators = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerHorizonIterators.add(new AsyncHorizonTimestampIterator(offsets));
            }
        } catch (Throwable th) {
            close();
            throw th;
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

        // Clear ASOF join maps (per-slave)
        for (int s = 0; s < slaveCount; s++) {
            Misc.free(ownerAsOfJoinMaps[s]);
        }
        Misc.freeObjListAndKeepObjects(perWorkerAsOfJoinMaps);

        // Clear filter context (memory pools, etc.)
        filterCtx.clear();

        // Clear symbol translating records (per-slave)
        for (int s = 0; s < slaveCount; s++) {
            Misc.clear(ownerSymbolTranslatingRecords[s]);
        }
        Misc.clearObjList(perWorkerSymbolTranslatingRecords);

        // Clear time frame cursors (per-slave)
        for (int s = 0; s < slaveCount; s++) {
            Misc.free(ownerSlaveTimeFrameCursors[s]);
        }
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
        for (int s = 0; s < slaveCount; s++) {
            Misc.free(ownerAsOfJoinMaps[s]);
        }
        Misc.freeObjList(perWorkerAsOfJoinMaps);
        for (int s = 0; s < slaveCount; s++) {
            Misc.free(ownerSlaveTimeFrameCursors[s]);
        }
        Misc.freeObjList(perWorkerSlaveTimeFrameCursors);
        // Horizon timestamp iterators
        Misc.free(ownerHorizonIterator);
        Misc.freeObjList(perWorkerHorizonIterators);
        // Filter and memory pool resources
        Misc.free(filterCtx);
        // Symbol translating records (per-slave)
        for (int s = 0; s < slaveCount; s++) {
            Misc.free(ownerSymbolTranslatingRecords[s]);
        }
        Misc.freeObjList(perWorkerSymbolTranslatingRecords);

        // Let subclass close its resources
        closeAggregationState();
    }

    public Map getAsOfJoinMap(int slotId, int slaveIndex) {
        if (slotId == -1) {
            return ownerAsOfJoinMaps[slaveIndex];
        }
        return perWorkerAsOfJoinMaps.getQuick(slotId * slaveCount + slaveIndex);
    }

    public AsyncFilterContext getFilterContext() {
        return filterCtx;
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
    public AsyncHorizonTimestampIterator getHorizonIterator(int slotId) {
        if (slotId == -1) {
            return ownerHorizonIterator;
        }
        return perWorkerHorizonIterators.getQuick(slotId);
    }

    public MultiHorizonJoinRecord getHorizonJoinRecord(int slotId) {
        if (slotId == -1) {
            return ownerCombinedRecord;
        }
        return perWorkerCombinedRecords.getQuick(slotId);
    }

    public RecordSink getMasterAsOfJoinSink(int slotId, int slaveIndex) {
        if (slotId == -1) {
            return ownerMasterAsOfJoinSinks.getQuick(slaveIndex);
        }
        return perWorkerMasterAsOfJoinSinks.getQuick(slotId).getQuick(slaveIndex);
    }

    public Record getMasterKeyRecord(int slotId, int slaveIndex, Record masterRecord) {
        final SymbolTranslatingRecord translatingRecord;
        if (slotId == -1) {
            translatingRecord = ownerSymbolTranslatingRecords[slaveIndex];
        } else {
            translatingRecord = perWorkerSymbolTranslatingRecords.getQuick(slotId * slaveCount + slaveIndex);
        }
        if (translatingRecord != null) {
            translatingRecord.of(masterRecord);
            return translatingRecord;
        }
        return masterRecord;
    }

    public int getMasterTimestampColumnIndex() {
        return masterTimestampColumnIndex;
    }

    public long getMasterTimestampScale(int slaveIndex) {
        return perSlaveMasterTsScales[slaveIndex];
    }

    /**
     * Get the offset value at the given index. Offsets are in master's scale.
     */
    public long getOffset(int index) {
        return offsets.getQuick(index);
    }

    public long getOffsetCount() {
        return offsetCount;
    }

    public ObjList<GroupByFunction> getOwnerGroupByFunctions() {
        return ownerGroupByFunctions;
    }

    public RecordSink getSlaveAsOfJoinMapSink(int slotId, int slaveIndex) {
        if (slotId == -1) {
            return ownerSlaveAsOfJoinSinks.getQuick(slaveIndex);
        }
        return perWorkerSlaveAsOfJoinSinks.getQuick(slotId).getQuick(slaveIndex);
    }

    public int getSlaveCount() {
        return slaveCount;
    }

    /**
     * Get the time frame helper for the given slot and slave.
     */
    public HorizonJoinTimeFrameHelper getSlaveTimeFrameHelper(int slotId, int slaveIndex) {
        if (slotId == -1) {
            return ownerSlaveTimeFrameHelpers[slaveIndex];
        }
        return perWorkerSlaveTimeFrameHelpers.getQuick(slotId * slaveCount + slaveIndex);
    }

    public MultiHorizonJoinSymbolTableSource getSymbolTableSource() {
        return horizonJoinSymbolTableSource;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        filterCtx.initFilters(symbolTableSource, executionContext);
        // Note: group by functions are initialized in initGroupByFunctions() where we have
        // access to both master and slave symbol table sources
    }

    /**
     * Initialize group by functions with combined symbol table source from all slaves.
     * Must be called after all slaves have been initialized via initSlaveTimeFrameCursors.
     */
    public void initGroupByFunctions(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSource,
            ObjList<SymbolTableSource> slaveSources
    ) throws SqlException {
        horizonJoinSymbolTableSource.of(masterSource, slaveSources);
        for (int i = 0, n = ownerGroupByFunctions.size(); i < n; i++) {
            ownerGroupByFunctions.getQuick(i).init(horizonJoinSymbolTableSource, executionContext);
        }
        if (perWorkerGroupByFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                    ObjList<GroupByFunction> functions = perWorkerGroupByFunctions.getQuick(i);
                    for (int j = 0, m = functions.size(); j < m; j++) {
                        functions.getQuick(j).init(horizonJoinSymbolTableSource, executionContext);
                    }
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
    }

    /**
     * Initialize time frame cursors for a single slave with shared frame data.
     * Must be called after the slave page frame cursor has been fully iterated.
     */
    public void initSlaveTimeFrameCursors(
            int slaveIndex,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor slavePageFrameCursor,
            PageFrameAddressCache slaveFrameAddressCache,
            DirectIntList slaveFramePartitionIndexes,
            LongList slaveFrameRowCounts,
            LongList slavePartitionTimestamps,
            LongList slavePartitionCeilings,
            int frameCount
    ) throws SqlException {
        // Initialize owner cursor for this slave
        int tsIndex = ownerSlaveTimeFrameCursors[slaveIndex].getTimestampIndex();
        ownerSlaveTimeFrameCursors[slaveIndex].of(
                slavePageFrameCursor,
                slaveFrameAddressCache,
                slaveFramePartitionIndexes,
                slaveFrameRowCounts,
                slavePartitionTimestamps,
                slavePartitionCeilings,
                frameCount,
                tsIndex
        );
        ownerSlaveTimeFrameHelpers[slaveIndex].of(ownerSlaveTimeFrameCursors[slaveIndex]);

        // Initialize per-worker cursors for this slave
        for (int w = 0; w < workerCount; w++) {
            int idx = w * slaveCount + slaveIndex;
            ConcurrentTimeFrameCursor c = perWorkerSlaveTimeFrameCursors.getQuick(idx);
            c.of(
                    slavePageFrameCursor,
                    slaveFrameAddressCache,
                    slaveFramePartitionIndexes,
                    slaveFrameRowCounts,
                    slavePartitionTimestamps,
                    slavePartitionCeilings,
                    frameCount,
                    tsIndex
            );
            perWorkerSlaveTimeFrameHelpers.getQuick(idx).of(c);
        }

        // Initialize symbol translating records for this slave
        if (ownerSymbolTranslatingRecords[slaveIndex] != null) {
            ownerSymbolTranslatingRecords[slaveIndex].initSources(masterSymbolTableSource, slavePageFrameCursor);
            for (int w = 0; w < workerCount; w++) {
                int idx = w * slaveCount + slaveIndex;
                SymbolTranslatingRecord r = perWorkerSymbolTranslatingRecords.getQuick(idx);
                if (r != null) {
                    r.initSources(masterSymbolTableSource, slavePageFrameCursor);
                }
            }
        }

        // Reopen ASOF maps for this slave
        if (ownerAsOfJoinMaps[slaveIndex] != null) {
            ownerAsOfJoinMaps[slaveIndex].reopen();
        }
        for (int w = 0; w < workerCount; w++) {
            int idx = w * slaveCount + slaveIndex;
            Map m = perWorkerAsOfJoinMaps.getQuick(idx);
            if (m != null) {
                m.reopen();
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
}

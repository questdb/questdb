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
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameFilteredNoRandomAccessMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryPool;
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
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.griffin.engine.table.AsyncFilterUtils.prepareBindVarMemory;

public class AsyncGroupByNotKeyedAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledFilter;
    private final IntHashSet filterUsedColumnIndexes;
    private final ObjList<PageFrameFilteredNoRandomAccessMemoryRecord> frameFilteredMemoryRecords = new ObjList<>();
    private final GroupByAllocator ownerAllocator;
    private final DirectLongList ownerAuxAddresses;
    private final DirectLongList ownerDataAddresses;
    private final Function ownerFilter;
    private final DirectLongList ownerFilteredRows;
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final SimpleMapValue ownerMapValue;
    private final PageFrameMemoryPool ownerMemoryPool;
    private final PageFrameFilteredNoRandomAccessMemoryRecord ownerPageFrameFilteredNoRandomAccessMemoryRecord = new PageFrameFilteredNoRandomAccessMemoryRecord();
    private final SelectivityStats ownerSelectivityStats = new SelectivityStats();
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<DirectLongList> perWorkerAuxAddresses;
    private final ObjList<DirectLongList> perWorkerDataAddresses;
    private final ObjList<Function> perWorkerFilters;
    private final ObjList<DirectLongList> perWorkerFilteredRows;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<SimpleMapValue> perWorkerMapValues;
    private final ObjList<PageFrameMemoryPool> perWorkerMemoryPools;
    private final ObjList<SelectivityStats> perWorkerSelectivityStats;

    public AsyncGroupByNotKeyedAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            int valueCount,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerFilter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;
        assert perWorkerGroupByFunctions == null || perWorkerGroupByFunctions.size() == workerCount;

        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.compiledFilter = compiledFilter;
            this.bindVarMemory = bindVarMemory;
            this.bindVarFunctions = bindVarFunctions;
            this.ownerFilter = ownerFilter;
            this.filterUsedColumnIndexes = filterUsedColumnIndexes;
            this.perWorkerFilters = perWorkerFilters;
            this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;

            final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
            ownerFunctionUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, ownerGroupByFunctions);
            if (perWorkerGroupByFunctions != null) {
                perWorkerFunctionUpdaters = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerFunctionUpdaters.extendAndSet(i, GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerGroupByFunctions.getQuick(i)));
                }
            } else {
                perWorkerFunctionUpdaters = null;
            }
            perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

            ownerMapValue = new SimpleMapValue(valueCount);
            perWorkerMapValues = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMapValues.extendAndSet(i, new SimpleMapValue(valueCount));
                frameFilteredMemoryRecords.extendAndSet(i, new PageFrameFilteredNoRandomAccessMemoryRecord());
            }

            ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            // Make sure to set worker-local allocator for the group by functions.
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            if (perWorkerGroupByFunctions != null) {
                perWorkerAllocators = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    final GroupByAllocator workerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
                    perWorkerAllocators.extendAndSet(i, workerAllocator);
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), workerAllocator);
                }
            } else {
                perWorkerAllocators = null;
            }

            perWorkerSelectivityStats = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSelectivityStats.extendAndSet(i, new SelectivityStats());
            }

            ownerMemoryPool = new PageFrameMemoryPool(1);
            ownerFilteredRows = new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), MemoryTag.NATIVE_OFFLOAD);
            if (compiledFilter != null) {
                ownerDataAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD);
                ownerAuxAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD);
            } else {
                ownerDataAddresses = null;
                ownerAuxAddresses = null;
            }
            perWorkerMemoryPools = new ObjList<>(slotCount);
            perWorkerFilteredRows = new ObjList<>(slotCount);
            perWorkerDataAddresses = new ObjList<>(slotCount);
            perWorkerAuxAddresses = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMemoryPools.extendAndSet(i, new PageFrameMemoryPool(1));
                perWorkerFilteredRows.extendAndSet(i, new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), MemoryTag.NATIVE_OFFLOAD));
                if (compiledFilter != null) {
                    perWorkerDataAddresses.extendAndSet(i, new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD));
                    perWorkerAuxAddresses.extendAndSet(i, new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD));
                }
            }

            clear();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        ownerFunctionUpdater.updateEmpty(ownerMapValue);
        ownerMapValue.setNew(true);
        for (int i = 0, n = perWorkerMapValues.size(); i < n; i++) {
            SimpleMapValue value = perWorkerMapValues.getQuick(i);
            ownerFunctionUpdater.updateEmpty(value);
            value.setNew(true);
        }
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.clear(ownerAllocator);
        Misc.clearObjList(perWorkerAllocators);
        Misc.free(ownerMemoryPool);
        Misc.freeObjListAndKeepObjects(perWorkerMemoryPools);
        ownerSelectivityStats.clear();
        Misc.clearObjList(perWorkerSelectivityStats);
    }

    @Override
    public void close() {
        Misc.free(compiledFilter);
        Misc.free(bindVarMemory);
        Misc.freeObjList(bindVarFunctions);
        Misc.free(ownerFilter);
        Misc.freeObjList(perWorkerFilters);
        Misc.free(ownerAllocator);
        Misc.freeObjList(perWorkerAllocators);
        Misc.free(ownerMapValue);
        Misc.freeObjList(perWorkerMapValues);
        Misc.free(ownerMemoryPool);
        Misc.freeObjList(perWorkerMemoryPools);
        Misc.free(ownerFilteredRows);
        Misc.freeObjList(perWorkerFilteredRows);
        Misc.free(ownerDataAddresses);
        Misc.freeObjList(perWorkerDataAddresses);
        Misc.free(ownerAuxAddresses);
        Misc.freeObjList(perWorkerAuxAddresses);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.freeObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }

    public DirectLongList getAuxAddresses(int slotId) {
        if (slotId == -1) {
            return ownerAuxAddresses;
        }
        return perWorkerAuxAddresses.getQuick(slotId);
    }

    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    public DirectLongList getDataAddresses(int slotId) {
        if (slotId == -1) {
            return ownerDataAddresses;
        }
        return perWorkerDataAddresses.getQuick(slotId);
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

    public DirectLongList getFilteredRows(int slotId) {
        if (slotId == -1) {
            return ownerFilteredRows;
        }
        return perWorkerFilteredRows.getQuick(slotId);
    }

    public @Nullable IntHashSet getFilterUsedColumnIndexes() {
        return filterUsedColumnIndexes;
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1 || perWorkerFunctionUpdaters == null) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    public PageFrameMemoryPool getMemoryPool(int slotId) {
        if (slotId == -1) {
            return ownerMemoryPool;
        }
        return perWorkerMemoryPools.getQuick(slotId);
    }

    public SimpleMapValue getMapValue(int slotId) {
        if (slotId == -1) {
            return ownerMapValue;
        }
        return perWorkerMapValues.getQuick(slotId);
    }

    // Thread-unsafe, should be used by query owner thread only.
    public SimpleMapValue getOwnerMapValue() {
        return ownerMapValue;
    }

    public PageFrameFilteredNoRandomAccessMemoryRecord getPageFrameFilteredMemoryRecord(int slotId) {
        if (slotId == -1) {
            return ownerPageFrameFilteredNoRandomAccessMemoryRecord;
        }
        return frameFilteredMemoryRecords.getQuick(slotId);
    }

    // Thread-unsafe, should be used by query owner thread only.
    public ObjList<SimpleMapValue> getPerWorkerMapValues() {
        return perWorkerMapValues;
    }

    public SelectivityStats getSelectivityStats(int slotId) {
        if (slotId == -1) {
            return ownerSelectivityStats;
        }
        return perWorkerSelectivityStats.getQuick(slotId);
    }

    public void initMemoryPools(PageFrameAddressCache pageFrameAddressCache) {
        ownerMemoryPool.of(pageFrameAddressCache);
        for (int i = 0, n = perWorkerMemoryPools.size(); i < n; i++) {
            perWorkerMemoryPools.getQuick(i).of(pageFrameAddressCache);
        }
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
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

        if (perWorkerGroupByFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                    Function.init(perWorkerGroupByFunctions.getQuick(i), symbolTableSource, executionContext, null);
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

        if (bindVarFunctions != null) {
            Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
            prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }
    }

    /**
     * Attempts to acquire a slot for the given worker thread.
     * On success, a {@link #release(int)} call must follow.
     *
     * @throws io.questdb.cairo.CairoException when circuit breaker has tripped
     */
    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            // Owner thread is free to use its own private filter, function updaters, etc. anytime.
            return -1;
        }
        // All other threads, e.g. worker or work stealing threads, must always acquire a lock
        // to use shared resources.
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        ownerAllocator.reopen();
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                perWorkerAllocators.getQuick(i).reopen();
            }
        }
    }

    public boolean shouldUseLateMaterialization(int slotId, boolean isParquetFrame) {
        if (!isParquetFrame) {
            return false;
        }
        if (filterUsedColumnIndexes == null || filterUsedColumnIndexes.size() == 0) {
            return false;
        }
        return getSelectivityStats(slotId).shouldUseLateMaterialization();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(ownerFilter);
    }

    public void toTop() {
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }
}

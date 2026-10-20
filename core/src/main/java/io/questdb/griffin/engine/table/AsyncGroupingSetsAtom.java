/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| |_) |
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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.functions.groupby.GroupingFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.NullingRecord;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Per-query worker state for asynchronous GROUPING SETS aggregation.
 * Each worker owns an independent map for every grouping set, so reducers can
 * update aggregate state without synchronization. The owner merges those maps
 * after page-frame dispatch completes.
 */
public class AsyncGroupingSetsAtom implements StatefulAtom, Reopenable, Closeable {
    private final ObjList<GroupByAllocator> allocators;
    private final CairoConfiguration configuration;
    private final ObjList<Map> maps;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<ObjList<Function>> perWorkerKeyFunctions;
    private final ObjList<RecordSink> recordSinks;
    private final ObjList<GroupByFunctionsUpdater> updaters;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final ObjList<Function> ownerKeyFunctions;
    private final ObjList<NullingRecord> nullingRecords;
    private final ObjList<IntList> nulledColumnsPerSet;
    private final PerWorkerLocks locks;
    private final int setCount;
    private final int workerCount;
    private final AsyncFilterContext filterContext;
    private final ColumnTypes keyTypes;
    private final ColumnTypes valueTypes;
    private MemoryTracker memoryTracker;

    public AsyncGroupingSetsAtom(
            @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull ColumnTypes baseColumnTypes,
            @NotNull ArrayColumnTypes keyTypes,
            @NotNull ArrayColumnTypes valueTypes,
            @NotNull ListColumnFilter listColumnFilter,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @NotNull ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> ownerKeyFunctions,
            @NotNull ObjList<ObjList<Function>> perWorkerKeyFunctions,
            @NotNull ObjList<IntList> groupingSets,
            @NotNull IntList keyColumnIndices,
            int workerCount
    ) {
        final int setCount = groupingSets.size();
        this.configuration = configuration;
        this.ownerGroupByFunctions = ownerGroupByFunctions;
        this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;
        this.ownerKeyFunctions = ownerKeyFunctions;
        this.perWorkerKeyFunctions = perWorkerKeyFunctions;
        this.setCount = groupingSets.size();
        this.workerCount = workerCount;
        this.keyTypes = new ArrayColumnTypes().addAll(keyTypes);
        this.valueTypes = new ArrayColumnTypes().addAll(valueTypes);
        this.filterContext = new AsyncFilterContext(configuration, null, null, null, null, null, null, workerCount, 0, 0L, 0L);
        this.locks = new PerWorkerLocks(configuration, workerCount);
        this.maps = new ObjList<>((workerCount + 1) * setCount);
        this.nullingRecords = new ObjList<>((workerCount + 1) * setCount);
        this.nulledColumnsPerSet = new ObjList<>(setCount);
        for (int setIndex = 0; setIndex < setCount; setIndex++) {
            IntList activeKeys = groupingSets.getQuick(setIndex);
            IntList nulledColumns = new IntList();
            for (int keyIndex = 0; keyIndex < keyColumnIndices.size(); keyIndex++) {
                if (!containsValue(activeKeys, keyIndex)) {
                    nulledColumns.add(keyColumnIndices.getQuick(keyIndex));
                }
            }
            nulledColumnsPerSet.add(nulledColumns);
        }
        this.recordSinks = new ObjList<>(workerCount + 1);
        this.updaters = new ObjList<>(workerCount + 1);
        this.allocators = new ObjList<>(workerCount + 1);

        try {
            final Class<RecordSink> sinkClass = RecordSinkFactory.getInstanceClass(
                    configuration,
                    asm,
                    baseColumnTypes,
                    listColumnFilter,
                    ownerKeyFunctions,
                    null,
                    null,
                    null,
                    null
            );
            final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(
                    asm, ownerGroupByFunctions.size()
            );
            createSlot(-1, sinkClass, updaterClass, baseColumnTypes, listColumnFilter);
            for (int worker = 0; worker < workerCount; worker++) {
                createSlot(worker, sinkClass, updaterClass, baseColumnTypes, listColumnFilter);
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void close() {
        Throwable failure = null;
        failure = Misc.freeObjListBestEffort(failure, maps);
        for (int i = 0, n = allocators.size(); i < n; i++) {
            GroupByAllocator allocator = allocators.getQuick(i);
            if (allocator != null) {
                allocator.setMemoryTracker(null);
            }
        }
        failure = Misc.freeObjListBestEffort(failure, allocators);
        failure = Misc.freeBestEffort(failure, filterContext);
        failure = Misc.freeObjListBestEffort(failure, ownerKeyFunctions);
        nullingRecords.clear();
        nulledColumnsPerSet.clear();
        failure = closeWorkerFunctions(failure, perWorkerKeyFunctions);
        failure = closeWorkerFunctions(failure, perWorkerGroupByFunctions);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        memoryTracker = executionContext.getMemoryTracker();
        Function.init(ownerKeyFunctions, symbolTableSource, executionContext, null);
        Function.init(ownerGroupByFunctions, symbolTableSource, executionContext, null);
        initWorkerFunctions(perWorkerKeyFunctions, ownerKeyFunctions, symbolTableSource, executionContext);
        initWorkerFunctions(perWorkerGroupByFunctions, ownerGroupByFunctions, symbolTableSource, executionContext);
        filterContext.initFilters(symbolTableSource, executionContext);
    }

    @Override
    public void reopen() {
        for (int i = 0, n = allocators.size(); i < n; i++) {
            GroupByAllocator allocator = allocators.getQuick(i);
            allocator.setMemoryTracker(memoryTracker);
            allocator.reopen();
        }
    }

    @Override
    public void clear() {
        for (int i = 0, n = maps.size(); i < n; i++) {
            Map map = maps.getQuick(i);
            if (map != null) {
                map.close();
            }
        }
        Misc.clearObjList(ownerGroupByFunctions);
        for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
            PerWorkerFunctionList.clear(perWorkerGroupByFunctions.getQuick(i));
        }
        for (int i = 0, n = allocators.size(); i < n; i++) {
            Misc.clear(allocators.getQuick(i));
        }
        filterContext.clear();
        memoryTracker = null;
    }

    public AsyncFilterContext getFilterContext() {
        return filterContext;
    }

    public GroupByFunctionsUpdater getUpdater(int slotId) {
        return updaters.getQuick(slotIndex(slotId));
    }

    public ObjList<Function> getOwnerKeyFunctions() {
        return ownerKeyFunctions;
    }

    public ObjList<GroupByFunction> getGroupByFunctions(int slotId) {
        if (slotId < 0) {
            return ownerGroupByFunctions;
        }
        return perWorkerGroupByFunctions.getQuick(slotId);
    }

    public RecordSink getRecordSink(int slotId) {
        return recordSinks.getQuick(slotIndex(slotId));
    }

    public NullingRecord getNullingRecord(int slotId, int setIndex) {
        return nullingRecords.getQuick(slotIndex(slotId) * setCount + setIndex);
    }

    public IntList getNulledColumns(int setIndex) {
        return nulledColumnsPerSet.getQuick(setIndex);
    }

    public Map getMap(int slotId, int setIndex) {
        int mapIndex = slotIndex(slotId) * setCount + setIndex;
        Map map = maps.getQuick(mapIndex);
        if (map == null) {
            map = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes, true, false);
            maps.setQuick(mapIndex, map);
            map.setMemoryTracker(memoryTracker);
            map.reopen();
        } else if (!map.isOpen()) {
            map.setMemoryTracker(memoryTracker);
            map.reopen();
        }
        return map;
    }

    public Map getMapIfExists(int slotId, int setIndex) {
        return maps.getQuick(slotIndex(slotId) * setCount + setIndex);
    }

    public Map getOwnerMap(int setIndex) {
        return getMap(-1, setIndex);
    }

    public int acquireSlot(int workerId, boolean owner, io.questdb.cairo.sql.SqlExecutionCircuitBreaker circuitBreaker) {
        return workerId == -1 && owner ? -1 : locks.acquireSlot(workerId, circuitBreaker);
    }

    public void releaseSlot(int slotId) {
        locks.releaseSlot(slotId);
    }

    public int getSetCount() {
        return setCount;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setCurrentGroupingSet(int slotId, int setIndex) {
        ObjList<GroupByFunction> functions = getGroupByFunctions(slotId);
        for (int i = 0, n = functions.size(); i < n; i++) {
            GroupByFunction function = functions.getQuick(i);
            if (function instanceof GroupingFunction groupingFunction) {
                groupingFunction.setCurrentSet(setIndex);
            }
        }
    }

    public void toTop() {
        GroupByUtils.toTop(ownerGroupByFunctions);
        for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
            GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
        }
    }

    private static Throwable closeWorkerFunctions(
            Throwable failure,
            ObjList<? extends ObjList<? extends Function>> functions
    ) {
        if (functions == null) {
            return failure;
        }
        for (int i = 0, n = functions.size(); i < n; i++) {
            try {
                PerWorkerFunctionList.close(functions.getQuick(i));
            } catch (Throwable th) {
                if (failure == null) {
                    failure = th;
                } else if (failure != th) {
                    failure.addSuppressed(th);
                }
            }
        }
        return failure;
    }

    private static void initWorkerFunctions(
            ObjList<? extends ObjList<? extends Function>> functions,
            ObjList<? extends Function> ownerFunctions,
            SymbolTableSource symbolTableSource,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (functions == null) {
            return;
        }
        boolean cloneSymbolTables = executionContext.getCloneSymbolTables();
        executionContext.setCloneSymbolTables(true);
        try {
            for (int i = 0, n = functions.size(); i < n; i++) {
                PerWorkerFunctionList.init(functions.getQuick(i), ownerFunctions, symbolTableSource, executionContext);
            }
        } finally {
            executionContext.setCloneSymbolTables(cloneSymbolTables);
        }
    }

    private void createSlot(
            int slotId,
            Class<RecordSink> sinkClass,
            Class<? extends GroupByFunctionsUpdater> updaterClass,
            ColumnTypes baseColumnTypes,
            ListColumnFilter listColumnFilter
    ) {
        final int index = slotIndex(slotId);
        final ObjList<Function> keyFunctions = slotId < 0 || perWorkerKeyFunctions == null
                ? ownerKeyFunctions
                : perWorkerKeyFunctions.getQuick(slotId);
        final ObjList<GroupByFunction> groupByFunctions = slotId < 0 ? ownerGroupByFunctions : perWorkerGroupByFunctions.getQuick(slotId);
        recordSinks.extendAndSet(index, RecordSinkFactory.getInstance(
                sinkClass, baseColumnTypes, listColumnFilter, keyFunctions, null, null, null, null
        ));
        updaters.extendAndSet(index, GroupByFunctionsUpdaterFactory.getInstance(updaterClass, groupByFunctions));
        final GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration, false);
        allocators.extendAndSet(index, allocator);
        GroupByUtils.setAllocator(groupByFunctions, allocator);
        for (int set = 0; set < setCount; set++) {
            int mapIndex = index * setCount + set;
            maps.extendAndSet(mapIndex, null);
            nullingRecords.extendAndSet(mapIndex, new NullingRecord());
        }
    }

    private int slotIndex(int slotId) {
        return slotId < 0 ? workerCount : slotId;
    }

    private static boolean containsValue(IntList list, int value) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (list.getQuick(i) == value) {
                return true;
            }
        }
        return false;
    }
}

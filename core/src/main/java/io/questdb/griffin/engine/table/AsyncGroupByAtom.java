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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
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
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;


public class AsyncGroupByAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    private final AsyncFilterContext filterCtx;
    private final GroupByAllocator ownerAllocator;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final ObjList<Function> ownerKeyFunctions;
    private final RecordSink ownerMapSink;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<ObjList<Function>> perWorkerKeyFunctions;
    private final PerWorkerLocks perWorkerLocks;
    // Initialized lazily.
    private final ObjList<DirectLongLongSortedList> perWorkerLongTopKLists;
    private final ObjList<RecordSink> perWorkerMapSinks;
    private final GroupByShardingContext shardingCtx;
    // Initialized lazily.
    private DirectLongLongSortedList ownerLongTopKList;

    public AsyncGroupByAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes columnTypes,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> ownerKeyFunctions,
            @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerFilter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;
        assert perWorkerKeyFunctions == null || perWorkerKeyFunctions.size() == workerCount;
        assert perWorkerGroupByFunctions == null || perWorkerGroupByFunctions.size() == workerCount;

        try {
            this.ownerKeyFunctions = ownerKeyFunctions;
            this.perWorkerKeyFunctions = perWorkerKeyFunctions;
            this.ownerGroupByFunctions = ownerGroupByFunctions;
            this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;

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
                    1,
                    1
            );

            final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
            final GroupByFunctionsUpdater ownerFunctionUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, ownerGroupByFunctions);
            ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters = null;
            if (perWorkerGroupByFunctions != null) {
                perWorkerFunctionUpdaters = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    perWorkerFunctionUpdaters.extendAndSet(i, GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerGroupByFunctions.getQuick(i)));
                }
            }

            perWorkerLocks = new PerWorkerLocks(configuration, workerCount);

            final ColumnTypes storedKeyTypes = new ArrayColumnTypes().addAll(keyTypes);
            final ColumnTypes storedValueTypes = new ArrayColumnTypes().addAll(valueTypes);
            shardingCtx = new GroupByShardingContext(
                    configuration,
                    storedKeyTypes,
                    storedValueTypes,
                    ownerFunctionUpdater,
                    perWorkerFunctionUpdaters,
                    perWorkerLocks,
                    workerCount
            );

            final Class<RecordSink> sinkClass = RecordSinkFactory.getInstanceClass(
                    configuration,
                    asm,
                    columnTypes,
                    listColumnFilter,
                    ownerKeyFunctions,
                    null,
                    null,
                    null,
                    null
            );
            ownerMapSink = RecordSinkFactory.getInstance(
                    sinkClass,
                    columnTypes,
                    listColumnFilter,
                    ownerKeyFunctions,
                    null,
                    null,
                    null,
                    null
            );

            perWorkerMapSinks = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                final ObjList<Function> workerKeyFunctions = perWorkerKeyFunctions != null
                        ? perWorkerKeyFunctions.getQuick(i)
                        : ownerKeyFunctions;
                perWorkerMapSinks.extendAndSet(
                        i,
                        RecordSinkFactory.getInstance(
                                sinkClass,
                                columnTypes,
                                listColumnFilter,
                                workerKeyFunctions,
                                null,
                                null,
                                null,
                                null
                        )
                );
            }

            ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            // Make sure to set worker-local allocator for the group by functions.
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            if (perWorkerGroupByFunctions != null) {
                perWorkerAllocators = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    final GroupByAllocator workerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
                    perWorkerAllocators.extendAndSet(i, workerAllocator);
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), workerAllocator);
                }
            } else {
                perWorkerAllocators = null;
            }

            perWorkerLongTopKLists = new ObjList<>(workerCount);
            perWorkerLongTopKLists.setAll(workerCount, null);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        shardingCtx.clear();
        Misc.clearObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.clear(ownerAllocator);
        Misc.clearObjList(perWorkerAllocators);
        Misc.clear(ownerLongTopKList);
        Misc.clearObjList(perWorkerLongTopKLists);
        filterCtx.clear();
    }

    @Override
    public void close() {
        Misc.free(shardingCtx);
        Misc.freeObjList(ownerKeyFunctions);
        Misc.free(ownerAllocator);
        Misc.freeObjList(perWorkerAllocators);
        Misc.free(ownerLongTopKList);
        Misc.freeObjListAndKeepObjects(perWorkerLongTopKLists);
        if (perWorkerKeyFunctions != null) {
            for (int i = 0, n = perWorkerKeyFunctions.size(); i < n; i++) {
                Misc.freeObjList(perWorkerKeyFunctions.getQuick(i));
            }
        }
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.freeObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.free(filterCtx);
    }

    public ObjList<Map> getDestShards() {
        return shardingCtx.getDestShards();
    }

    public AsyncFilterContext getFilterContext() {
        return filterCtx;
    }

    public GroupByMapFragment getFragment(int slotId) {
        return shardingCtx.getFragment(slotId);
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        return shardingCtx.getFunctionUpdater(slotId);
    }

    public DirectLongLongSortedList getLongTopKList(int slotId, int order, int limit) {
        if (slotId == -1) {
            if (ownerLongTopKList == null || ownerLongTopKList.getOrder() != order) {
                Misc.free(ownerLongTopKList);
                ownerLongTopKList = DirectLongLongSortedList.getInstance(order, limit, MemoryTag.NATIVE_DEFAULT);
            }
            ownerLongTopKList.reopen(limit);
            return ownerLongTopKList;
        }

        DirectLongLongSortedList workerList = perWorkerLongTopKLists.getQuick(slotId);
        if (workerList == null || workerList.getOrder() != order) {
            Misc.free(workerList);
            workerList = DirectLongLongSortedList.getInstance(order, limit, MemoryTag.NATIVE_DEFAULT);
            perWorkerLongTopKLists.setQuick(slotId, workerList);
        }
        workerList.reopen(limit);
        return workerList;
    }

    public RecordSink getMapSink(int slotId) {
        if (slotId == -1) {
            return ownerMapSink;
        }
        return perWorkerMapSinks.getQuick(slotId);
    }

    // thread-unsafe
    public ObjList<GroupByFunction> getOwnerGroupByFunctions() {
        return ownerGroupByFunctions;
    }

    // thread-unsafe
    public DirectLongLongSortedList getOwnerLongTopKList() {
        return ownerLongTopKList;
    }

    // thread-unsafe
    public ObjList<DirectLongLongSortedList> getPerWorkerLongTopKLists() {
        return perWorkerLongTopKLists;
    }

    public GroupByShardingContext getShardingContext() {
        return shardingCtx;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        filterCtx.initFilters(symbolTableSource, executionContext);

        if (ownerKeyFunctions != null) {
            Function.init(ownerKeyFunctions, symbolTableSource, executionContext, null);
        }

        initPerWorkerFunctions(perWorkerKeyFunctions, symbolTableSource, executionContext);
        initPerWorkerFunctions(perWorkerGroupByFunctions, symbolTableSource, executionContext);
    }

    public boolean isSharded() {
        return shardingCtx.isSharded();
    }

    /**
     * Attempts to acquire a slot for the given worker thread.
     * On success, a {@link #release(int)} call must follow.
     *
     * @throws io.questdb.cairo.CairoException when circuit breaker has tripped
     */
    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void maybeEnableSharding(GroupByMapFragment fragment) {
        shardingCtx.maybeEnableSharding(fragment, getTotalFunctionCardinality(fragment.slotId));
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        shardingCtx.reopen();
        // The maps will be open lazily by worker threads, but we need to reopen the allocators.
        ownerAllocator.reopen();
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                perWorkerAllocators.getQuick(i).reopen();
            }
        }
    }

    public void resetLocalStats(int slotId) {
        final ObjList<GroupByFunction> groupByFunctions = getGroupByFunctions(slotId);
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            groupByFunctions.getQuick(i).resetStats();
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        filterCtx.toPlan(sink);
    }

    public void toTop() {
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }

    private ObjList<GroupByFunction> getGroupByFunctions(int slotId) {
        if (slotId == -1 || perWorkerGroupByFunctions == null) {
            return ownerGroupByFunctions;
        }
        return perWorkerGroupByFunctions.getQuick(slotId);
    }

    private long getTotalFunctionCardinality(int slotId) {
        final ObjList<GroupByFunction> groupByFunctions = getGroupByFunctions(slotId);
        long totalCardinality = 0;
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            totalCardinality += groupByFunctions.getQuick(i).getCardinalityStat();
        }
        return totalCardinality;
    }

    private void initPerWorkerFunctions(
            ObjList<? extends ObjList<? extends Function>> functions,
            SymbolTableSource symbolTableSource,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (functions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = functions.size(); i < n; i++) {
                    Function.init(functions.getQuick(i), symbolTableSource, executionContext, null);
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
    }
}

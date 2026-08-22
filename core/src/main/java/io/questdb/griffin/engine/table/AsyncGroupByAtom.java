/*+*****************************************************************************
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
import io.questdb.cairo.CairoException;
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
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;


public class AsyncGroupByAtom implements StatefulAtom, PerWorkerLockOwner, Closeable, Reopenable, Plannable {
    private final int batchSize;
    private final AsyncFilterContext filterCtx;
    private final GroupByAllocator ownerAllocator;
    private final DirectLongList ownerBatchList;
    private final FlyweightPackedMapValue ownerBatchMapValue;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final ObjList<Function> ownerKeyFunctions;
    private final RecordSink ownerMapSink;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<DirectLongList> perWorkerBatchLists;
    private final ObjList<FlyweightPackedMapValue> perWorkerBatchMapValues;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<ObjList<Function>> perWorkerKeyFunctions;
    private final PerWorkerLocks perWorkerLocks;
    // Initialized lazily.
    private final ObjList<DirectLongLongSortedList> perWorkerLongTopKLists;
    private final ObjList<RecordSink> perWorkerMapSinks;
    private final GroupByShardingContext shardingCtx;
    // Per-query native memory tracker captured from SqlExecutionContext on init.
    // Null when no per-query limit applies. Workers and operator code feed it to
    // tracker-aware Unsafe overloads to charge allocations to the active workload.
    private MemoryTracker memoryTracker;
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
                    0L,
                    0L
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

            // Lazy variant (openOnInit=false): the chunk index is global-counter bookkeeping;
            // only the data chunks it hands out are charged to the per-query tracker.
            ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration, false);
            // Make sure to set worker-local allocator for the group by functions.
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            if (perWorkerGroupByFunctions != null) {
                perWorkerAllocators = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    final GroupByAllocator workerAllocator = GroupByAllocatorFactory.createAllocator(configuration, false);
                    perWorkerAllocators.extendAndSet(i, workerAllocator);
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), workerAllocator);
                }
            } else {
                perWorkerAllocators = null;
            }

            perWorkerLongTopKLists = new ObjList<>(workerCount);
            perWorkerLongTopKLists.setAll(workerCount, null);

            // Per-slot batch scratch buffers and flyweights for batched dispatch.
            // The owner uses slotId -1; worker slots 0..N-1 index directly into the per-worker lists.
            batchSize = configuration.getGroupByBatchSize();
            ownerBatchList = new DirectLongList(batchSize, MemoryTag.NATIVE_DEFAULT, true);
            ownerBatchMapValue = new FlyweightPackedMapValue(valueTypes);
            perWorkerBatchLists = new ObjList<>(workerCount);
            perWorkerBatchMapValues = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerBatchLists.extendAndSet(i, new DirectLongList(batchSize, MemoryTag.NATIVE_DEFAULT, true));
                perWorkerBatchMapValues.extendAndSet(i, new FlyweightPackedMapValue(valueTypes));
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        shardingCtx.clear();
        Misc.clearObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                PerWorkerFunctionList.clear(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.clear(ownerAllocator);
        Misc.clearObjList(perWorkerAllocators);
        Misc.clear(ownerLongTopKList);
        Misc.clearObjList(perWorkerLongTopKLists);
        Misc.free(ownerBatchList);
        Misc.freeObjListAndKeepObjects(perWorkerBatchLists);
        filterCtx.clear();
        memoryTracker = null;
    }

    @Override
    public void close() {
        Throwable cleanupFailure = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, shardingCtx);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, ownerKeyFunctions);
        // clear() already freed the data chunks under the bound tracker (the index is on the
        // global counter), so close() has nothing tracked to free. Nulling is defensive: any
        // stray free hits the global counter and cannot underflow an already-recycled block.
        if (ownerAllocator != null) {
            ownerAllocator.setMemoryTracker(null);
        }
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                final GroupByAllocator allocator = perWorkerAllocators.getQuick(i);
                if (allocator != null) {
                    allocator.setMemoryTracker(null);
                }
            }
        }
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerAllocator);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerAllocators);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerLongTopKList);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerLongTopKLists);
        cleanupFailure = closePerWorkerFunctions(cleanupFailure, perWorkerKeyFunctions);
        cleanupFailure = closePerWorkerFunctions(cleanupFailure, perWorkerGroupByFunctions);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerBatchList);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerBatchLists);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, filterCtx);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    private static Throwable closePerWorkerFunctions(Throwable cleanupFailure, ObjList<? extends ObjList<? extends Function>> perWorkerFunctions) {
        if (perWorkerFunctions != null) {
            for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                final ObjList<? extends Function> functions = perWorkerFunctions.getQuick(i);
                perWorkerFunctions.setQuick(i, null);
                try {
                    PerWorkerFunctionList.close(functions);
                } catch (Throwable th) {
                    if (cleanupFailure == null) {
                        cleanupFailure = th;
                    } else if (cleanupFailure != th) {
                        cleanupFailure.addSuppressed(th);
                    }
                }
            }
        }
        return cleanupFailure;
    }

    public DirectLongList getBatchList(int slotId) {
        if (slotId == -1) {
            return ownerBatchList;
        }
        return perWorkerBatchLists.getQuick(slotId);
    }

    public FlyweightPackedMapValue getBatchMapValue(int slotId) {
        if (slotId == -1) {
            return ownerBatchMapValue;
        }
        return perWorkerBatchMapValues.getQuick(slotId);
    }

    public int getBatchSize() {
        return batchSize;
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

    public ObjList<GroupByFunction> getGroupByFunctions(int slotId) {
        if (slotId == -1 || perWorkerGroupByFunctions == null) {
            return ownerGroupByFunctions;
        }
        return perWorkerGroupByFunctions.getQuick(slotId);
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

    public MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    // thread-unsafe
    public ObjList<GroupByFunction> getOwnerGroupByFunctions() {
        return ownerGroupByFunctions;
    }

    // thread-unsafe
    public ObjList<Function> getOwnerKeyFunctions() {
        return ownerKeyFunctions;
    }

    // thread-unsafe
    public DirectLongLongSortedList getOwnerLongTopKList() {
        return ownerLongTopKList;
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
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
        memoryTracker = executionContext.getMemoryTracker();
        filterCtx.initFilters(symbolTableSource, executionContext);

        if (ownerKeyFunctions != null) {
            Function.init(ownerKeyFunctions, symbolTableSource, executionContext, null);
        }

        // The owner group by functions initialize here, once per query execution; the cursor does
        // not re-initialize them. Donate the initialized owner state to the aligned per-worker
        // clones before they initialize. Stateful functions inside aggregate arguments, such as
        // cursor comparisons caching a scalar sub-query result, must run their expensive and
        // potentially nondeterministic initialization exactly once per query, not once per worker,
        // and every worker must observe the same state as the owner.
        Function.init(ownerGroupByFunctions, symbolTableSource, executionContext, null);

        initPerWorkerFunctions(perWorkerKeyFunctions, ownerKeyFunctions, symbolTableSource, executionContext);
        initPerWorkerFunctions(perWorkerGroupByFunctions, ownerGroupByFunctions, symbolTableSource, executionContext);
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
        // init() runs before reopen() (frameSequence.of() -> atom.init(), then cursor.of()
        // -> atom.reopen()), so memoryTracker is available here to bind on the fragments
        // and allocators before any backing is allocated. The maps are opened lazily by
        // worker threads via reopenMap()/reopenShards(); the allocators are reopened here.
        shardingCtx.setMemoryTracker(memoryTracker);
        shardingCtx.reopen();
        ownerAllocator.setMemoryTracker(memoryTracker);
        ownerAllocator.reopen();
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                final GroupByAllocator allocator = perWorkerAllocators.getQuick(i);
                allocator.setMemoryTracker(memoryTracker);
                allocator.reopen();
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
            ObjList<? extends Function> ownerFunctions,
            SymbolTableSource symbolTableSource,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (functions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = functions.size(); i < n; i++) {
                    PerWorkerFunctionList.init(
                            functions.getQuick(i),
                            ownerFunctions,
                            symbolTableSource,
                            executionContext
                    );
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
    }
}

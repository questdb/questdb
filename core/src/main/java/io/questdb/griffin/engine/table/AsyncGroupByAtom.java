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
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
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
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;


public class AsyncGroupByAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    // We use the first bits of hash code to determine the shard.
    private static final int NUM_SHARDS = 256;
    private static final int NUM_SHARDS_SHR = Long.numberOfLeadingZeros(NUM_SHARDS) + 1;
    private final CairoConfiguration configuration;
    // Used to merge shards from ownerFragment and perWorkerFragments.
    private final ObjList<Map> destShards;
    private final AsyncFilterContext filterCtx;
    private final ColumnTypes keyTypes;
    private final MapStats lastOwnerStats;
    private final ObjList<MapStats> lastShardStats;
    private final GroupByAllocator ownerAllocator;
    private final MapFragment ownerFragment;
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final ObjList<Function> ownerKeyFunctions;
    private final RecordSink ownerMapSink;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<Function> perWorkerFilters;
    private final ObjList<MapFragment> perWorkerFragments;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<ObjList<Function>> perWorkerKeyFunctions;
    private final PerWorkerLocks perWorkerLocks;
    // Initialized lazily.
    private final ObjList<DirectLongLongSortedList> perWorkerLongTopKLists;
    private final ObjList<RecordSink> perWorkerMapSinks;
    private final ColumnTypes valueTypes;
    // Initialized lazily.
    private DirectLongLongSortedList ownerLongTopKList;
    private volatile boolean sharded;
    // A hint whether to shard during the next query execution.
    private boolean shardedHint;

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

        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.configuration = configuration;
            this.keyTypes = new ArrayColumnTypes().addAll(keyTypes);
            this.valueTypes = new ArrayColumnTypes().addAll(valueTypes);
            this.perWorkerFilters = perWorkerFilters;
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
                    slotCount,
                    slotCount,
                    1,
                    1
            );

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

            lastShardStats = new ObjList<>(NUM_SHARDS);
            for (int i = 0; i < NUM_SHARDS; i++) {
                lastShardStats.extendAndSet(i, new MapStats());
            }
            lastOwnerStats = new MapStats();
            ownerFragment = new MapFragment(-1);
            perWorkerFragments = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerFragments.extendAndSet(i, new MapFragment(i));
            }
            // Destination shards are lazily initialized by the worker threads.
            destShards = new ObjList<>(NUM_SHARDS);
            destShards.setPos(NUM_SHARDS);

            final Class<RecordSink> sinkClass = RecordSinkFactory.getInstanceClass(
                    asm,
                    columnTypes,
                    listColumnFilter,
                    ownerKeyFunctions,
                    null,
                    null,
                    null,
                    null,
                    configuration
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
            if (perWorkerKeyFunctions != null) {
                perWorkerMapSinks = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerMapSinks.extendAndSet(i, RecordSinkFactory.getInstance(
                            sinkClass,
                            columnTypes,
                            listColumnFilter,
                            perWorkerKeyFunctions.getQuick(i),
                            null,
                            null,
                            null,
                            null
                    ));
                }
            } else {
                perWorkerMapSinks = null;
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

            perWorkerLongTopKLists = new ObjList<>(slotCount);
            perWorkerLongTopKLists.setAll(slotCount, null);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        sharded = false;
        Misc.free(ownerFragment);
        Misc.freeObjListAndKeepObjects(perWorkerFragments);
        Misc.freeObjListAndKeepObjects(destShards);
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
        Misc.free(ownerFragment);
        Misc.freeObjList(perWorkerFragments);
        Misc.freeObjList(destShards);
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

    public void finalizeShardStats() {
        // Find max heap size and apply it to all shards.
        if (configuration.isGroupByPresizeEnabled()) {
            // First, calculate max heap size.
            long maxHeapSize = 0;
            for (int i = 0; i < NUM_SHARDS; i++) {
                final MapStats stats = lastShardStats.getQuick(i);
                maxHeapSize = Math.max(stats.maxHeapSize, maxHeapSize);
            }
            // Next, apply it to all shard stats.
            for (int i = 0; i < NUM_SHARDS; i++) {
                lastShardStats.getQuick(i).maxHeapSize = maxHeapSize;
            }
        }

        updateShardedHint();
    }

    public ObjList<Map> getDestShards() {
        return destShards;
    }

    public AsyncFilterContext getFilterContext() {
        return filterCtx;
    }

    public MapFragment getFragment(int slotId) {
        if (slotId == -1) {
            return ownerFragment;
        }
        return perWorkerFragments.getQuick(slotId);
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1 || perWorkerFunctionUpdaters == null) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
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
        if (slotId == -1 || perWorkerMapSinks == null) {
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

    public int getShardCount() {
        return NUM_SHARDS;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        filterCtx.initFilters(symbolTableSource, executionContext);

        if (ownerKeyFunctions != null) {
            Function.init(ownerKeyFunctions, symbolTableSource, executionContext, null);
        }

        if (perWorkerKeyFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerKeyFunctions.size(); i < n; i++) {
                    Function.init(perWorkerKeyFunctions.getQuick(i), symbolTableSource, executionContext, null);
                }
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
    }

    public boolean isSharded() {
        return sharded;
    }

    /**
     * Attempts to acquire a slot for the given worker thread.
     * On success, a {@link #release(int)} call must follow.
     *
     * @throws io.questdb.cairo.CairoException when circuit breaker has tripped
     */
    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            // Owner thread is free to use the original functions anytime.
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    /**
     * Attempts to acquire a slot for the given worker thread.
     * On success, a {@link #release(int)} call must follow.
     *
     * @throws io.questdb.cairo.CairoException when circuit breaker has tripped
     */
    public int maybeAcquire(int workerId, boolean owner, ExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            // Owner thread is free to use its own private filter, function updaters, allocator,
            // etc. anytime.
            return -1;
        }
        // All other threads, e.g. worker or work stealing threads, must always acquire a lock
        // to use shared resources.
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void maybeEnableSharding(MapFragment fragment) {
        // First, update function cardinality stats for the fragment.
        fragment.totalFunctionCardinality += getTotalFunctionCardinality(fragment.slotId);
        // Functions are cheaper to merge when compared with merging the maps, hence the 10x multiplier.
        final int shardingThreshold = configuration.getGroupByShardingThreshold();
        if (!sharded && (fragment.getMap().size() > shardingThreshold || fragment.totalFunctionCardinality > 10L * shardingThreshold)) {
            sharded = true;
        }
    }

    public Map mergeOwnerMap() {
        final Map destMap = ownerFragment.reopenMap();
        final int perWorkerMapCount = perWorkerFragments.size();
        // Make sure to set the allocator for the owner's group by functions.
        // This is done by the getFunctionUpdater() method.
        final GroupByFunctionsUpdater functionUpdater = getFunctionUpdater(-1);

        // Calculate medians before the merge.
        final MapStats stats = lastOwnerStats;
        final LongList medianList = stats.medianList;
        medianList.clear();
        for (int i = 0; i < perWorkerMapCount; i++) {
            final Map srcMap = perWorkerFragments.getQuick(i).getMap();
            if (srcMap.size() > 0) {
                medianList.add(srcMap.size());
            }
        }
        medianList.sort();
        // This is not very precise, but does the job.
        long medianSize = medianList.size() > 0
                ? medianList.getQuick(Math.min(medianList.size() - 1, (medianList.size() / 2) + 1))
                : 0;
        medianList.clear();
        long maxHeapSize = -1;
        if (destMap.getUsedHeapSize() != -1) {
            for (int i = 0; i < perWorkerMapCount; i++) {
                final Map srcMap = perWorkerFragments.getQuick(i).getMap();
                maxHeapSize = Math.max(maxHeapSize, srcMap.getHeapSize());
            }
        }

        // Now do the actual merge.
        for (int i = 0; i < perWorkerMapCount; i++) {
            final Map srcMap = perWorkerFragments.getQuick(i).getMap();
            destMap.merge(srcMap, functionUpdater);
            srcMap.close();
        }

        // Don't forget to update the stats.
        if (configuration.isGroupByPresizeEnabled()) {
            stats.update(medianSize, maxHeapSize, destMap.size(), destMap.getHeapSize());
        }

        updateShardedHint();

        return destMap;
    }

    public void mergeShard(int slotId, int shardIndex) {
        assert sharded;

        final GroupByFunctionsUpdater functionUpdater = getFunctionUpdater(slotId);
        final Map destMap = reopenDestShard(shardIndex);
        final int perWorkerMapCount = perWorkerFragments.size();

        // Calculate medians before the merge.
        final MapStats stats = lastShardStats.getQuick(shardIndex);
        final LongList medianList = stats.medianList;
        medianList.clear();
        for (int i = 0; i < perWorkerMapCount; i++) {
            final MapFragment srcFragment = perWorkerFragments.getQuick(i);
            final Map srcMap = srcFragment.getShards().getQuick(shardIndex);
            if (srcMap.size() > 0) {
                medianList.add(srcMap.size());
            }
        }
        // Include shard from the owner fragment.
        final Map srcOwnerMap = ownerFragment.getShards().getQuick(shardIndex);
        if (srcOwnerMap.size() > 0) {
            medianList.add(srcOwnerMap.size());
        }
        medianList.sort();
        // This is not very precise, but does the job.
        long medianSize = medianList.size() > 0
                ? medianList.getQuick(Math.min(medianList.size() - 1, (medianList.size() / 2) + 1))
                : 0;
        long maxHeapSize = -1;
        if (destMap.getUsedHeapSize() != -1) {
            for (int i = 0; i < perWorkerMapCount; i++) {
                final MapFragment srcFragment = perWorkerFragments.getQuick(i);
                final Map srcMap = srcFragment.getShards().getQuick(shardIndex);
                maxHeapSize = Math.max(maxHeapSize, srcMap.getHeapSize());
            }
        }

        // Now do the actual merge.
        for (int i = 0; i < perWorkerMapCount; i++) {
            final MapFragment srcFragment = perWorkerFragments.getQuick(i);
            final Map srcMap = srcFragment.getShards().getQuick(shardIndex);
            destMap.merge(srcMap, functionUpdater);
            srcMap.close();
        }
        // Merge shard from the owner fragment.
        destMap.merge(srcOwnerMap, functionUpdater);
        srcOwnerMap.close();

        // Don't forget to update the stats.
        if (configuration.isGroupByPresizeEnabled()) {
            stats.update(medianSize, maxHeapSize, destMap.size(), destMap.getHeapSize());
        }
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        if (shardedHint) {
            // Looks like we had to shard during previous execution, so let's do it ahead of time.
            sharded = true;
        }
        // The maps will be open lazily by worker threads, but we need to reopen the allocators.
        ownerAllocator.reopen();
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                perWorkerAllocators.getQuick(i).reopen();
            }
        }
    }

    public void shardAll() {
        ownerFragment.shard();
        for (int i = 0, n = perWorkerFragments.size(); i < n; i++) {
            perWorkerFragments.getQuick(i).shard();
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

    private Map reopenDestShard(int shardIndex) {
        Map destMap = destShards.getQuick(shardIndex);
        if (destMap == null) {
            destMap = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
            destShards.set(shardIndex, destMap);
        } else if (!destMap.isOpen()) {
            MapStats stats = lastShardStats.getQuick(shardIndex);
            int keyCapacity = targetKeyCapacity(stats, true);
            long heapSize = targetHeapSize(stats, true);
            destMap.reopen(keyCapacity, heapSize);
        }
        return destMap;
    }

    /**
     * Calculates pre-sized map's heap size based on the given stats.
     *
     * @param stats statistics collected during the last query run
     * @param dest  merge destination map flag;
     *              once merge is done, merge destination contains entries from all fragment maps;
     *              in the non-sharded case, it's set to true for the map in {@link #ownerFragment}
     *              and false for maps in {@link #perWorkerFragments};
     *              in the sharded case, it's set to true for the maps in {@link #destShards}
     *              and false for shard maps in {@link #ownerFragment} and {@link #perWorkerFragments}
     * @return heap size to pre-size the map
     */
    private long targetHeapSize(MapStats stats, boolean dest) {
        final long statHeapSize = dest ? stats.mergedHeapSize : stats.maxHeapSize;
        // Per-worker limit is smaller than the owner one.
        final long statLimit = dest
                ? configuration.getGroupByPresizeMaxHeapSize()
                : configuration.getGroupByPresizeMaxHeapSize() / perWorkerFragments.size();
        long heapSize = configuration.getSqlSmallMapPageSize();
        if (statHeapSize <= statLimit) {
            heapSize = Math.max(statHeapSize, heapSize);
        }
        return heapSize;
    }

    /**
     * Calculates pre-sized map's capacity based on the given stats.
     *
     * @param stats statistics collected during the last query run
     * @param dest  merge destination map flag;
     *              once merge is done, merge destination contains entries from all fragment maps;
     *              in the non-sharded case, it's set to true for the map in {@link #ownerFragment}
     *              and false for maps in {@link #perWorkerFragments};
     *              in the sharded case, it's set to true for the maps in {@link #destShards}
     *              and false for shard maps in {@link #ownerFragment} and {@link #perWorkerFragments}
     * @return capacity to pre-size the map
     */
    private int targetKeyCapacity(MapStats stats, boolean dest) {
        final long statKeyCapacity = dest ? stats.mergedSize : stats.medianSize;
        // Per-worker limit is smaller than the owner one.
        final long statLimit = dest
                ? configuration.getGroupByPresizeMaxCapacity()
                : configuration.getGroupByPresizeMaxCapacity() / perWorkerFragments.size();
        int keyCapacity = configuration.getSqlSmallMapKeyCapacity();
        if (statKeyCapacity <= statLimit) {
            keyCapacity = Math.max((int) statKeyCapacity, keyCapacity);
        }
        return keyCapacity;
    }

    private void updateShardedHint() {
        long mapSize = 0;
        if (sharded) {
            for (int i = 0; i < NUM_SHARDS; i++) {
                // All destShards should be non-null at this point,
                // so the null check is merely for future-proof code.
                final Map destShard = destShards.getQuick(i);
                mapSize += destShard != null ? destShard.size() : 0;
            }
        } else {
            mapSize = ownerFragment.map.size();
        }

        long functionCardinality = 0;
        functionCardinality += ownerFragment.totalFunctionCardinality;
        for (int i = 0, n = perWorkerFragments.size(); i < n; i++) {
            functionCardinality += perWorkerFragments.getQuick(i).totalFunctionCardinality;
        }

        final int shardingThreshold = configuration.getGroupByShardingThreshold();
        // Functions are cheaper to merge when compared with merging the maps, hence the 10x multiplier.
        shardedHint = (mapSize > shardingThreshold || functionCardinality > 10L * shardingThreshold);
    }

    private static class MapStats {
        // We don't use median for heap size since heap is mmapped lazily initialized memory.
        long maxHeapSize;
        LongList medianList = new LongList();
        long medianSize;
        long mergedHeapSize;
        long mergedSize;

        void update(long medianSize, long maxHeapSize, long mergedSize, long mergedHeapSize) {
            this.medianSize = medianSize;
            this.maxHeapSize = maxHeapSize;
            this.mergedSize = mergedSize;
            this.mergedHeapSize = mergedHeapSize;
        }
    }

    public class MapFragment implements QuietCloseable {
        private final Map map; // non-sharded partial result
        private final ObjList<Map> shards; // this.map split into shards
        private final int slotId; // -1 stands for owner fragment
        private boolean sharded;
        private long totalFunctionCardinality;

        private MapFragment(int slotId) {
            this.map = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
            this.shards = new ObjList<>(NUM_SHARDS);
            this.slotId = slotId;
        }

        @Override
        public void close() {
            sharded = false;
            totalFunctionCardinality = 0;
            map.close();
            for (int i = 0, n = shards.size(); i < n; i++) {
                Map m = shards.getQuick(i);
                Misc.free(m);
            }
        }

        public Map getMap() {
            return map;
        }

        public Map getShardMap(long hashCode) {
            return shards.getQuick((int) (hashCode >>> NUM_SHARDS_SHR));
        }

        public ObjList<Map> getShards() {
            return shards;
        }

        public boolean isNotSharded() {
            return !sharded;
        }

        public Map reopenMap() {
            if (!map.isOpen()) {
                final boolean owner = slotId == -1;
                int keyCapacity = targetKeyCapacity(lastOwnerStats, owner);
                long heapSize = targetHeapSize(lastOwnerStats, owner);
                map.reopen(keyCapacity, heapSize);
            }
            return map;
        }

        public void resetLocalStats() {
            final ObjList<GroupByFunction> groupByFunctions = getGroupByFunctions(slotId);
            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                groupByFunctions.getQuick(i).resetStats();
            }
        }

        public void shard() {
            if (sharded) {
                return;
            }

            reopenShards();

            if (map.size() > 0) {
                RecordCursor cursor = map.getCursor();
                MapRecord record = map.getRecord();
                while (cursor.hasNext()) {
                    final long hashCode = record.keyHashCode();
                    final Map shard = getShardMap(hashCode);
                    MapKey shardKey = shard.withKey();
                    record.copyToKey(shardKey);
                    MapValue shardValue = shardKey.createValue(hashCode);
                    record.copyValue(shardValue);
                }
            }

            map.close();
            sharded = true;
        }

        private void reopenShards() {
            int size = shards.size();
            if (size == 0) {
                for (int i = 0; i < NUM_SHARDS; i++) {
                    shards.add(MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes));
                }
            } else {
                for (int i = 0; i < NUM_SHARDS; i++) {
                    MapStats stats = lastShardStats.getQuick(i);
                    int keyCapacity = targetKeyCapacity(stats, false);
                    long heapSize = targetHeapSize(stats, false);
                    shards.getQuick(i).reopen(keyCapacity, heapSize);
                }
            }
        }
    }
}

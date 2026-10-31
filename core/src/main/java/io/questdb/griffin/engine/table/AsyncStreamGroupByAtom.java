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
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.AsyncQueryProgressState;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByShardMerger;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.StreamGroupByMap;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS;

public class AsyncStreamGroupByAtom implements StatefulAtom, PerWorkerLockOwner, GroupByShardMerger {
    private final int[] batchColumnIndexes;
    private final StreamGroupByMap collectorMap;
    private final CairoConfiguration configuration;
    // The merged shards of the span maps of a bucket, from its close to its emission.
    private final ObjList<Map> destShards = new ObjList<>();
    private final AsyncFilterContext filterContext;
    // A frame's segments, by frame index modulo the reduce queue capacity, from its reduce to its collection.
    private final ObjList<RecordChain> frameRecords;
    private final ObjList<DirectLongList> frameValues;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final boolean isDescending;
    private final Function keyFunction;
    private final int keyFunctionIndex;
    private final ObjList<Function> keyFunctions;
    private final PerWorkerLocks locks;
    private final ObjList<StreamGroupByMap> mergeSources = new ObjList<>();
    private final StreamGroupByMap ownerMap;
    private final ObjList<ObjList<GroupByFunction>> perWorkerFunctions;
    private final ObjList<ObjList<Function>> perWorkerKeys;
    private final ObjList<StreamGroupByMap> perWorkerMaps;
    private final ObjList<GroupByFunctionsUpdater> perWorkerUpdaters;
    private final ObjList<FlyweightPackedMapValue> perWorkerValues;
    private final AsyncQueryProgressState progressState = new AsyncQueryProgressState();
    private final ObjList<SpanMaps> spanMaps;
    private final ArrayColumnTypes stateTypes;
    private final GroupByFunctionsUpdater updater;
    private final FlyweightPackedMapValue value;
    private MemoryTracker memoryTracker;
    private int shardMergeCount;

    public AsyncStreamGroupByAtom(
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            RecordMetadata baseMetadata,
            ListColumnFilter columnFilter,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            int streamKeyIndex,
            Function keyFunction,
            ObjList<Function> keyFunctions,
            ObjList<ObjList<Function>> perWorkerKeys,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<ObjList<GroupByFunction>> perWorkerFunctions,
            CompiledFilter compiledFilter,
            MemoryCARW bindVarMemory,
            ObjList<Function> bindVarFunctions,
            Function filter,
            IntHashSet filterUsedColumnIndexes,
            ObjList<Function> perWorkerFilters,
            int workerCount,
            boolean isDescending,
            int dispatchLimit
    ) {
        this.configuration = configuration;
        this.frameRecords = new ObjList<>(dispatchLimit);
        this.frameRecords.setPos(dispatchLimit);
        this.frameValues = new ObjList<>(dispatchLimit);
        this.frameValues.setPos(dispatchLimit);
        this.keyFunction = keyFunction;
        this.isDescending = isDescending;
        this.keyFunctionIndex = streamKeyIndex - columnFilter.getColumnCount();
        this.keyFunctions = keyFunctions;
        this.perWorkerKeys = perWorkerKeys;
        this.groupByFunctions = groupByFunctions;
        this.perWorkerFunctions = perWorkerFunctions;
        try {
            filterContext = filter == null ? null : new AsyncFilterContext(configuration, compiledFilter,
                    bindVarMemory, bindVarFunctions, filter, filterUsedColumnIndexes, perWorkerFilters,
                    workerCount, workerCount, 0, 0);
            stateTypes = new ArrayColumnTypes().addAll(valueTypes);
            stateTypes.add(keyFunction.getType());
            batchColumnIndexes = new int[groupByFunctions.size()];
            for (int i = 0; i < batchColumnIndexes.length; i++) {
                batchColumnIndexes[i] = AsyncGroupByNotKeyedRecordCursorFactory.computeBatchColumnIndex(groupByFunctions.getQuick(i), baseMetadata);
            }
            locks = new PerWorkerLocks(configuration, workerCount);
            final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, groupByFunctions.size());
            updater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, groupByFunctions);
            value = new FlyweightPackedMapValue(stateTypes);
            perWorkerValues = new ObjList<>(workerCount);
            perWorkerUpdaters = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerValues.add(new FlyweightPackedMapValue(stateTypes));
                perWorkerUpdaters.add(perWorkerFunctions == null ? updater
                        : GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerFunctions.getQuick(i)));
            }
            if (keyTypes.getColumnCount() > 1) {
                ownerMap = new StreamGroupByMap(configuration, asm, baseMetadata, columnFilter, keyTypes, valueTypes,
                        streamKeyIndex, keyFunctions, groupByFunctions, updater);
                collectorMap = new StreamGroupByMap(configuration, asm, baseMetadata, columnFilter, keyTypes, valueTypes,
                        streamKeyIndex, keyFunctions, groupByFunctions, updater);
                perWorkerMaps = new ObjList<>(workerCount);
                spanMaps = new ObjList<>(workerCount + 1);
                spanMaps.add(new SpanMaps(ownerMap));
                for (int i = 0; i < workerCount; i++) {
                    final StreamGroupByMap map = new StreamGroupByMap(configuration, asm, baseMetadata, columnFilter, keyTypes,
                            valueTypes, streamKeyIndex, perWorkerKeys == null ? keyFunctions : perWorkerKeys.getQuick(i),
                            getFunctions(i), getUpdater(i));
                    perWorkerMaps.add(map);
                    spanMaps.add(new SpanMaps(map));
                }
            } else {
                ownerMap = collectorMap = null;
                perWorkerMaps = null;
                spanMaps = null;
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        CairoException.rethrowCleanupFailure(closeMaps(null));
        if (filterContext != null) {
            filterContext.clear();
        }
        cursorClosed(groupByFunctions);
        cursorClosed(keyFunctions);
        Misc.clearObjList(groupByFunctions);
        if (perWorkerFunctions != null) {
            for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                cursorClosed(perWorkerFunctions.getQuick(i));
                PerWorkerFunctionList.clear(perWorkerFunctions.getQuick(i));
            }
        }
        if (perWorkerKeys != null) {
            for (int i = 0, n = perWorkerKeys.size(); i < n; i++) {
                cursorClosed(perWorkerKeys.getQuick(i));
            }
        }
    }

    @Override
    public void close() {
        Throwable th = Misc.freeObjListBestEffort(null, keyFunctions);
        th = Misc.freeBestEffort(th, filterContext);
        th = closeMaps(th);
        th = Misc.freeObjListBestEffort(th, frameRecords);
        th = Misc.freeObjListBestEffort(th, frameValues);
        th = AsyncGroupByAtom.closePerWorkerFunctions(th, perWorkerKeys);
        th = AsyncGroupByAtom.closePerWorkerFunctions(th, perWorkerFunctions);
        CairoException.rethrowCleanupFailure(th);
    }

    public StreamGroupByMap acquireSpanMap(int slot, long key) {
        return spanMaps.getQuick(slot + 1).acquire(configuration, memoryTracker, key);
    }

    public void closeShardMerge() {
        mergeSources.clear();
    }

    public int[] getBatchColumnIndexes() {
        return batchColumnIndexes;
    }

    public StreamGroupByMap getCollectorMap() {
        return collectorMap;
    }

    public AsyncFilterContext getFilterContext() {
        return filterContext;
    }

    public int getDispatchLimit() {
        return frameRecords.size();
    }

    public ObjList<GroupByFunction> getFunctions(int slot) {
        return slot < 0 || perWorkerFunctions == null ? groupByFunctions : perWorkerFunctions.getQuick(slot);
    }

    public Function getKey(int slot) {
        return slot < 0 || perWorkerKeys == null || keyFunctionIndex < 0
                ? keyFunction : perWorkerKeys.getQuick(slot).getQuick(keyFunctionIndex);
    }

    public ObjList<Function> getKeyFunctions() {
        return keyFunctions;
    }

    public StreamGroupByMap getMap(int slot) {
        return slot < 0 ? ownerMap : perWorkerMaps.getQuick(slot);
    }

    @Override
    public PerWorkerLocks getPerWorkerLocks() {
        return locks;
    }

    @TestOnly
    public int getSpanMapKeyCapacity() {
        int capacity = 0;
        for (int i = 0, n = spanMaps.size(); i < n; i++) {
            capacity = Math.max(capacity, spanMaps.getQuick(i).getKeyCapacity());
        }
        return capacity;
    }

    public RecordChain getResultRecords(PageFrameReduceTask task) {
        final int slot = task.getFrameIndex() % frameRecords.size();
        RecordChain records = frameRecords.getQuick(slot);
        if (records == null) {
            records = new RecordChain(ownerMap.getStateTypes(), null, configuration.getSqlSmallMapPageSize(), Integer.MAX_VALUE);
            frameRecords.setQuick(slot, records);
        }
        records.setMemoryTracker(task.getMemoryTracker());
        return records;
    }

    public DirectLongList getResultValues(PageFrameReduceTask task) {
        final int slot = task.getFrameIndex() % frameValues.size();
        DirectLongList values = frameValues.getQuick(slot);
        if (values == null) {
            values = new DirectLongList(64, MemoryTag.NATIVE_FAST_MAP, true);
            frameValues.setQuick(slot, values);
        }
        values.setMemoryTracker(task.getMemoryTracker());
        return values;
    }

    @TestOnly
    public int getShardMergeCount() {
        return shardMergeCount;
    }

    @Override
    public AsyncQueryProgressState getProgressState() {
        return progressState;
    }

    public ArrayColumnTypes getStateTypes() {
        return stateTypes;
    }

    public GroupByFunctionsUpdater getUpdater(int slot) {
        return slot < 0 ? updater : perWorkerUpdaters.getQuick(slot);
    }

    public FlyweightPackedMapValue getValue(int slot) {
        return slot < 0 ? value : perWorkerValues.getQuick(slot);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (filterContext != null) {
            filterContext.initFilters(symbolTableSource, executionContext);
        }
        Function.init(groupByFunctions, symbolTableSource, executionContext, null);
        Function.init(keyFunctions, symbolTableSource, executionContext, null);
        final boolean cloneSymbolTables = executionContext.getCloneSymbolTables();
        executionContext.setCloneSymbolTables(true);
        try {
            if (perWorkerFunctions != null) {
                for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                    PerWorkerFunctionList.init(perWorkerFunctions.getQuick(i), groupByFunctions, symbolTableSource, executionContext);
                }
            }
            if (perWorkerKeys != null) {
                for (int i = 0, n = perWorkerKeys.size(); i < n; i++) {
                    PerWorkerFunctionList.init(perWorkerKeys.getQuick(i), keyFunctions, symbolTableSource, executionContext);
                }
            }
        } finally {
            executionContext.setCloneSymbolTables(cloneSymbolTables);
        }
        if (ownerMap != null) {
            try {
                memoryTracker = executionContext.getMemoryTracker();
                ownerMap.reopen(memoryTracker);
                collectorMap.reopen(memoryTracker);
                for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
                    perWorkerMaps.getQuick(i).reopen(memoryTracker);
                }
            } catch (Throwable th) {
                closeMaps(th);
                throw th;
            }
        }
    }

    public boolean isDescending() {
        return isDescending;
    }

    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        return workerId == -1 && owner ? -1 : locks.acquireSlot(workerId, circuitBreaker);
    }

    @Override
    public int maybeAcquire(int carrierId, boolean owner, ExecutionCircuitBreaker circuitBreaker) {
        return carrierId == -1 && owner ? -1 : locks.acquireSlot(carrierId, circuitBreaker);
    }

    @Override
    public void mergeShard(int slotId, int shardIndex) {
        final Map dest = destShards.getQuick(shardIndex);
        dest.clear();
        long size = 0;
        for (int i = 0, n = mergeSources.size(); i < n; i++) {
            size += mergeSources.getQuick(i).getShards().getQuick(shardIndex).size();
        }
        if (size > dest.getKeyCapacity()) {
            dest.setKeyCapacity((int) Math.min(size, Integer.MAX_VALUE));
        }
        final GroupByFunctionsUpdater updater = getUpdater(slotId);
        for (int i = 0, n = mergeSources.size(); i < n; i++) {
            final Map source = mergeSources.getQuick(i).getShards().getQuick(shardIndex);
            dest.merge(source, updater);
            source.close();
        }
    }

    // The span maps must be sharded. The merged shards keep their capacity until the cursor closes.
    public ObjList<Map> openShardMerge(ObjList<StreamGroupByMap> sources) {
        shardMergeCount++;
        mergeSources.clear();
        mergeSources.addAll(sources);
        if (destShards.size() == 0) {
            for (int i = 0; i < NUM_SHARDS; i++) {
                destShards.add(ownerMap.newMergeShard());
            }
        }
        for (int i = 0; i < NUM_SHARDS; i++) {
            final Map shard = destShards.getQuick(i);
            if (!shard.isOpen()) {
                shard.setMemoryTracker(memoryTracker);
                shard.reopen();
            }
        }
        return destShards;
    }

    @Override
    public void release(int slot) {
        locks.releaseSlot(slot);
    }

    public void releaseAllResults() {
        for (int i = 0, n = frameRecords.size(); i < n; i++) {
            releaseResults(i);
        }
    }

    public void releaseResults(int frameIndex) {
        final int slot = frameIndex % frameRecords.size();
        final RecordChain records = frameRecords.getQuick(slot);
        if (records != null) {
            records.close();
            records.setMemoryTracker(null);
        }
        final DirectLongList values = frameValues.getQuick(slot);
        if (values != null) {
            values.close();
            values.setMemoryTracker(null);
        }
    }

    public void releaseSpanMap(int slot, StreamGroupByMap map) {
        spanMaps.getQuick(slot + 1).release(map);
    }

    public void takeSpanMaps(long key, ObjList<StreamGroupByMap> maps, IntList slots) {
        for (int i = 0, n = spanMaps.size(); i < n; i++) {
            final StreamGroupByMap map = spanMaps.getQuick(i).take(key);
            if (map != null) {
                maps.add(map);
                slots.add(i - 1);
            }
        }
    }

    public void toTop() {
        releaseAllResults();
        if (filterContext != null) {
            filterContext.toTop();
        }
        if (spanMaps != null) {
            for (int i = 0, n = spanMaps.size(); i < n; i++) {
                spanMaps.getQuick(i).releaseAll();
            }
        }
        GroupByUtils.toTop(groupByFunctions);
        GroupByUtils.toTop(keyFunctions);
        if (perWorkerFunctions != null) {
            for (int i = 0, n = perWorkerFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerFunctions.getQuick(i));
            }
        }
        if (perWorkerKeys != null) {
            for (int i = 0, n = perWorkerKeys.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerKeys.getQuick(i));
            }
        }
    }

    private Throwable closeMaps(Throwable th) {
        mergeSources.clear();
        for (int i = 0, n = destShards.size(); i < n; i++) {
            th = Misc.freeBestEffort(th, destShards.getQuick(i));
        }
        th = Misc.freeBestEffort(th, ownerMap);
        th = Misc.freeBestEffort(th, collectorMap);
        if (perWorkerMaps != null) {
            for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
                th = Misc.freeBestEffort(th, perWorkerMaps.getQuick(i));
            }
        }
        if (spanMaps != null) {
            for (int i = 0, n = spanMaps.size(); i < n; i++) {
                th = spanMaps.getQuick(i).close(th);
            }
        }
        return th;
    }

    private static void cursorClosed(ObjList<? extends Function> functions) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (PerWorkerFunctionList.isOwned(functions, i)) {
                functions.getQuick(i).cursorClosed();
            }
        }
    }

    // The reducing thread and the owner share the lists; a slot only adds to segments the owner has not taken.
    private static class SpanMaps {
        private final ObjList<StreamGroupByMap> freeMaps = new ObjList<>();
        private final ObjList<StreamGroupByMap> maps = new ObjList<>();
        private final LongList openKeys = new LongList();
        private final ObjList<StreamGroupByMap> openMaps = new ObjList<>();
        private final StreamGroupByMap prototype;

        private SpanMaps(StreamGroupByMap prototype) {
            this.prototype = prototype;
        }

        private StreamGroupByMap acquire(CairoConfiguration configuration, MemoryTracker memoryTracker, long key) {
            final StreamGroupByMap map;
            synchronized (this) {
                final int index = openKeys.indexOf(key);
                if (index > -1) {
                    return openMaps.getQuick(index);
                }
                if (freeMaps.size() > 0) {
                    map = freeMaps.getLast();
                    freeMaps.setPos(freeMaps.size() - 1);
                } else {
                    map = prototype.newInstance(configuration);
                    maps.add(map);
                }
                openKeys.add(key);
                openMaps.add(map);
            }
            if (!map.isOpen()) {
                map.reopen(memoryTracker);
            }
            map.clear(key);
            return map;
        }

        private synchronized Throwable close(Throwable th) {
            for (int i = 0, n = maps.size(); i < n; i++) {
                th = Misc.freeBestEffort(th, maps.getQuick(i));
            }
            openKeys.clear();
            openMaps.clear();
            freeMaps.clear();
            freeMaps.addAll(maps);
            return th;
        }

        private synchronized int getKeyCapacity() {
            int capacity = 0;
            for (int i = 0, n = maps.size(); i < n; i++) {
                capacity = Math.max(capacity, maps.getQuick(i).getKeyCapacity());
            }
            return capacity;
        }

        private synchronized void release(StreamGroupByMap map) {
            freeMaps.add(map);
        }

        private synchronized void releaseAll() {
            freeMaps.addAll(openMaps);
            openKeys.clear();
            openMaps.clear();
        }

        private synchronized StreamGroupByMap take(long key) {
            final int index = openKeys.indexOf(key);
            if (index < 0) {
                return null;
            }
            final StreamGroupByMap map = openMaps.getQuick(index);
            openKeys.removeIndex(index);
            openMaps.remove(index);
            return map;
        }
    }
}

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory.prepareBindVarMemory;

public class AsyncGroupByAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    // We use the first 8 bits of a hash code to determine the shard, hence 128 as the max number of shards.
    private static final int MAX_SHARDS = 128;
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledFilter;
    private final CairoConfiguration configuration;
    private final Function filter;
    private final GroupByFunctionsUpdater functionUpdater;
    private final ObjList<Function> keyFunctions;
    private final ColumnTypes keyTypes;
    private final RecordSink ownerMapSink;
    private final MapStats ownerMapStats;
    private final Particle ownerParticle;
    private final ObjList<Function> perWorkerFilters;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<ObjList<Function>> perWorkerKeyFunctions;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<RecordSink> perWorkerMapSinks;
    private final ObjList<Particle> perWorkerParticles;
    private final int shardCount;
    private final int shardCountShr;
    private final ObjList<MapStats> shardMapStats;
    private final ColumnTypes valueTypes;
    private volatile boolean sharded;

    public AsyncGroupByAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes columnTypes,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> keyFunctions,
            @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;
        assert perWorkerKeyFunctions == null || perWorkerKeyFunctions.size() == workerCount;

        // We don't want to pay for merging redundant maps, so we limit their number.
        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());
        try {
            this.configuration = configuration;
            this.keyTypes = new ArrayColumnTypes().addAll(keyTypes);
            this.valueTypes = new ArrayColumnTypes().addAll(valueTypes);
            this.compiledFilter = compiledFilter;
            this.bindVarMemory = bindVarMemory;
            this.bindVarFunctions = bindVarFunctions;
            this.filter = filter;
            this.perWorkerFilters = perWorkerFilters;
            this.keyFunctions = keyFunctions;
            this.perWorkerKeyFunctions = perWorkerKeyFunctions;
            this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;

            functionUpdater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            if (perWorkerGroupByFunctions != null) {
                perWorkerFunctionUpdaters = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerFunctionUpdaters.extendAndSet(i, GroupByFunctionsUpdaterFactory.getInstance(asm, perWorkerGroupByFunctions.getQuick(i)));
                }
            } else {
                perWorkerFunctionUpdaters = null;
            }

            perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

            shardCount = Math.min(Numbers.ceilPow2(2 * workerCount), MAX_SHARDS);
            shardCountShr = Integer.numberOfLeadingZeros(shardCount) + 1;
            shardMapStats = new ObjList<>(shardCount);
            for (int i = 0; i < shardCount; i++) {
                shardMapStats.extendAndSet(i, new MapStats());
            }
            ownerMapStats = new MapStats();
            ownerParticle = new Particle();
            perWorkerParticles = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerParticles.extendAndSet(i, new Particle());
            }

            ownerMapSink = RecordSinkFactory.getInstance(asm, columnTypes, listColumnFilter, keyFunctions, false);
            if (perWorkerKeyFunctions != null) {
                perWorkerMapSinks = new ObjList<>(slotCount);
                for (int i = 0; i < slotCount; i++) {
                    RecordSink sink = RecordSinkFactory.getInstance(asm, columnTypes, listColumnFilter, perWorkerKeyFunctions.getQuick(i), false);
                    perWorkerMapSinks.extendAndSet(i, sink);
                }
            } else {
                perWorkerMapSinks = null;
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public int acquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            // Owner thread is free to use the original functions anytime.
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public int acquire(int workerId, ExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1) {
            // Owner thread is free to use the original functions anytime.
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    @Override
    public void clear() {
        sharded = false;
        ownerParticle.close();
        for (int i = 0, n = perWorkerParticles.size(); i < n; i++) {
            Particle p = perWorkerParticles.getQuick(i);
            Misc.free(p);
        }
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }

    @Override
    public void close() {
        Misc.free(ownerParticle);
        Misc.freeObjList(perWorkerParticles);
        Misc.free(compiledFilter);
        Misc.free(bindVarMemory);
        Misc.freeObjList(bindVarFunctions);
        Misc.free(filter);
        Misc.freeObjList(keyFunctions);
        Misc.freeObjList(perWorkerFilters);
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
    }

    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    public CompiledFilter getCompiledFilter() {
        return compiledFilter;
    }

    public Function getFilter(int slotId) {
        if (slotId == -1 || perWorkerFilters == null) {
            return filter;
        }
        return perWorkerFilters.getQuick(slotId);
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1 || perWorkerFunctionUpdaters == null) {
            return functionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    public RecordSink getMapSink(int slotId) {
        if (slotId == -1 || perWorkerMapSinks == null) {
            return ownerMapSink;
        }
        return perWorkerMapSinks.getQuick(slotId);
    }

    // Thread-unsafe, should be used by query owner thread only.
    public Particle getOwnerParticle() {
        return ownerParticle;
    }

    public Particle getParticle(int slotId) {
        if (slotId == -1) {
            return ownerParticle;
        }
        return perWorkerParticles.getQuick(slotId);
    }

    // Thread-unsafe, should be used by query owner thread only.
    public ObjList<Particle> getPerWorkerParticles() {
        return perWorkerParticles;
    }

    public int getShardCount() {
        return shardCount;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (filter != null) {
            filter.init(symbolTableSource, executionContext);
        }

        if (perWorkerFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerFilters, symbolTableSource, executionContext);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

        if (keyFunctions != null) {
            Function.init(keyFunctions, symbolTableSource, executionContext);
        }

        if (perWorkerKeyFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerKeyFunctions.size(); i < n; i++) {
                    Function.init(perWorkerKeyFunctions.getQuick(i), symbolTableSource, executionContext);
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
                    Function.init(perWorkerGroupByFunctions.getQuick(i), symbolTableSource, executionContext);
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

        if (bindVarFunctions != null) {
            Function.init(bindVarFunctions, symbolTableSource, executionContext);
            prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }
    }

    @Override
    public void initCursor() {
        if (filter != null) {
            filter.initCursor();
        }
        if (perWorkerFilters != null) {
            // Initialize all per-worker filters on the query owner thread to avoid
            // DataUnavailableException thrown on worker threads when filtering.
            Function.initCursor(perWorkerFilters);
        }
    }

    public boolean isSharded() {
        return sharded;
    }

    public Map mergeOwnerMap() {
        final Map destMap = ownerParticle.getMap();
        final int perWorkerMapCount = perWorkerParticles.size();

        // Pre-size the destination map, if we have the stats.
        final MapStats stats = ownerMapStats;
        if (stats.mergedSize > 0 && stats.mergedSize <= configuration.getGroupByPresizeMaxSize()) {
            destMap.setKeyCapacity((int) stats.mergedSize);
        }
        if (stats.mergedHeapSize > 0 && stats.mergedHeapSize <= configuration.getGroupByPresizeMaxHeapSize()) {
            destMap.setHeapSize(stats.mergedHeapSize);
        }

        // Calculate the medians before the merge.
        final LongList medianList = stats.medianList;
        medianList.clear();
        for (int i = 0; i < perWorkerMapCount; i++) {
            final Map srcMap = perWorkerParticles.getQuick(i).getMap();
            medianList.add(srcMap.size());
        }
        medianList.sort();
        // This is not very precise, but does the job.
        long medianSize = medianList.getQuick(medianList.size() / 2);
        medianList.clear();
        long medianHeapSize = -1;
        if (destMap.getUsedHeapSize() != -1) {
            medianList.clear();
            for (int i = 0; i < perWorkerMapCount; i++) {
                final Map srcMap = perWorkerParticles.getQuick(i).getMap();
                medianList.add(srcMap.getHeapSize());
            }
            medianList.sort();
            medianHeapSize = medianList.getQuick(medianList.size() / 2);
        }

        // Now do the actual merge.
        for (int i = 0; i < perWorkerMapCount; i++) {
            final Map srcMap = perWorkerParticles.getQuick(i).getMap();
            destMap.merge(srcMap, functionUpdater);
            srcMap.close();
        }

        // Don't forget to update the stats.
        stats.update(medianSize, medianHeapSize, destMap.size(), destMap.getHeapSize());

        return destMap;
    }

    public void mergeShard(int slotId, int shardIndex) {
        assert sharded;

        final GroupByFunctionsUpdater functionUpdater = getFunctionUpdater(slotId);
        final Map destMap = ownerParticle.getShardMaps().getQuick(shardIndex);
        final int perWorkerMapCount = perWorkerParticles.size();

        // Pre-size the destination map, if we have the stats.
        final MapStats stats = shardMapStats.getQuick(shardIndex);
        if (stats.mergedSize > 0 && stats.mergedSize <= configuration.getGroupByPresizeMaxSize()) {
            destMap.setKeyCapacity((int) stats.mergedSize);
        }
        if (stats.mergedHeapSize > 0 && stats.mergedHeapSize <= configuration.getGroupByPresizeMaxHeapSize()) {
            destMap.setHeapSize(stats.mergedHeapSize);
        }

        // Calculate the medians before the merge.
        final LongList medianList = stats.medianList;
        medianList.clear();
        for (int i = 0; i < perWorkerMapCount; i++) {
            final Particle srcParticle = perWorkerParticles.getQuick(i);
            final Map srcMap = srcParticle.getShardMaps().getQuick(shardIndex);
            medianList.add(srcMap.size());
        }
        medianList.sort();
        // This is not very precise, but does the job.
        long medianSize = medianList.getQuick(medianList.size() / 2);
        long medianHeapSize = -1;
        if (destMap.getUsedHeapSize() != -1) {
            medianList.clear();
            for (int i = 0; i < perWorkerMapCount; i++) {
                final Particle srcParticle = perWorkerParticles.getQuick(i);
                final Map srcMap = srcParticle.getShardMaps().getQuick(shardIndex);
                medianList.add(srcMap.getHeapSize());
            }
            medianList.sort();
            medianHeapSize = medianList.getQuick(medianList.size() / 2);
        }

        // Now do the actual merge.
        for (int i = 0; i < perWorkerMapCount; i++) {
            final Particle srcParticle = perWorkerParticles.getQuick(i);
            final Map srcMap = srcParticle.getShardMaps().getQuick(shardIndex);
            destMap.merge(srcMap, functionUpdater);
            srcMap.close();
        }

        // Don't forget to update the stats.
        stats.update(medianSize, medianHeapSize, destMap.size(), destMap.getHeapSize());
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        ownerParticle.reopen();
        for (int i = 0, n = perWorkerParticles.size(); i < n; i++) {
            perWorkerParticles.getQuick(i).reopen();
        }

        if (ownerMapStats.mergedSize > configuration.getGroupByShardingThreshold()) {
            // Looks like we had to shard during previous execution, so let's do it ahead of time.
            shardAll();
        }
    }

    public void setAllocator(GroupByAllocator allocator) {
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), allocator);
            }
        }
    }

    public void shardAll() {
        ownerParticle.shard();
        for (int i = 0, n = perWorkerParticles.size(); i < n; i++) {
            Particle p = perWorkerParticles.getQuick(i);
            p.shard();
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(filter);
    }

    public void toTop() {
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }

    public void tryShard(Particle particle) {
        if (particle.isSharded()) {
            return;
        }
        if (particle.getMap().size() > configuration.getGroupByShardingThreshold() || sharded) {
            particle.shard();
            sharded = true;
        }
    }

    private static class MapStats {
        long medianHeapSize;
        LongList medianList = new LongList();
        long medianSize;
        long mergedHeapSize;
        long mergedSize;

        void update(long medianSize, long medianHeapSize, long mergedSize, long mergedHeapSize) {
            this.medianSize = medianSize;
            this.medianHeapSize = medianHeapSize;
            this.mergedSize = mergedSize;
            this.mergedHeapSize = mergedHeapSize;
        }
    }

    public class Particle implements QuietCloseable {
        private final Map map; // non-sharded partial result
        private final ObjList<Map> shards; // this.map split into shards
        private boolean sharded;

        private Particle() {
            this.map = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
            this.shards = new ObjList<>(shardCount);
        }

        @Override
        public void close() {
            sharded = false;
            map.close();
            for (int i = 0, n = shards.size(); i < n; i++) {
                Map m = shards.getQuick(i);
                Misc.free(m);
            }
        }

        public Map getMap() {
            return map;
        }

        public Map getShardMap(int hashCode) {
            return shards.getQuick(hashCode >>> shardCountShr);
        }

        public ObjList<Map> getShardMaps() {
            return shards;
        }

        public boolean isSharded() {
            return sharded;
        }

        public void reopen() {
            int keyCapacity = targetKeyCapacity(ownerMapStats);
            int pageSize = targetPageSize(ownerMapStats);
            map.reopen(keyCapacity, pageSize);
        }

        private void reopenShards() {
            int size = shards.size();
            if (size == 0) {
                for (int i = 0; i < shardCount; i++) {
                    MapStats stats = shardMapStats.getQuick(i);
                    int keyCapacity = targetKeyCapacity(stats);
                    int pageSize = targetPageSize(stats);
                    shards.add(MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes, keyCapacity, pageSize));
                }
            } else {
                assert size == shardCount;
                for (int i = 0; i < shardCount; i++) {
                    MapStats stats = shardMapStats.getQuick(i);
                    int keyCapacity = targetKeyCapacity(stats);
                    int pageSize = targetPageSize(stats);
                    shards.getQuick(i).reopen(keyCapacity, pageSize);
                }
            }
        }

        private void shard() {
            if (sharded) {
                return;
            }

            reopenShards();

            if (map.size() > 0) {
                RecordCursor cursor = map.getCursor();
                MapRecord record = map.getRecord();
                while (cursor.hasNext()) {
                    final int hashCode = record.keyHashCode();
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

        private int targetKeyCapacity(MapStats stats) {
            int keyCapacity = configuration.getSqlSmallMapKeyCapacity();
            if (stats.medianSize <= configuration.getGroupByPresizeMaxSize()) {
                keyCapacity = Math.max((int) stats.medianSize, keyCapacity);
            }
            return keyCapacity;
        }

        private int targetPageSize(MapStats stats) {
            int pageSize = configuration.getSqlSmallMapPageSize();
            if (stats.medianHeapSize <= configuration.getGroupByPresizeMaxHeapSize()) {
                pageSize = Math.max((int) stats.medianHeapSize, pageSize);
            }
            return pageSize;
        }
    }
}

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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class AsyncGroupByAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    // We use the first 8 bits of a hash code to determine the shard, hence 128 as the max number of shards.
    private static final int MAX_SHARDS = 128;
    private final CairoConfiguration configuration;
    private final Function filter;
    private final GroupByFunctionsUpdater functionUpdater;
    private final ObjList<Function> keyFunctions;
    private final ColumnTypes keyTypes;
    private final RecordSink ownerMapSink;
    private final Particle ownerParticle;
    private final ObjList<Function> perWorkerFilters;
    private final ObjList<ObjList<Function>> perWorkerKeyFunctions;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<RecordSink> perWorkerMapSinks;
    private final ObjList<Particle> perWorkerParticles;
    private final int shardCount;
    private final int shardCountShr;
    private final int shardingThreshold;
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
            @NotNull ObjList<Function> keyFunctions,
            @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions,
            @Nullable Function filter,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;
        assert perWorkerKeyFunctions == null || perWorkerKeyFunctions.size() == workerCount;
        try {
            this.configuration = configuration;
            this.shardingThreshold = configuration.getGroupByShardingThreshold();
            this.keyTypes = new ArrayColumnTypes().addAll(keyTypes);
            this.valueTypes = new ArrayColumnTypes().addAll(valueTypes);
            this.filter = filter;
            this.perWorkerFilters = perWorkerFilters;
            this.keyFunctions = keyFunctions;
            this.perWorkerKeyFunctions = perWorkerKeyFunctions;
            functionUpdater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            perWorkerLocks = new PerWorkerLocks(configuration, workerCount);

            shardCount = Math.min(configuration.getGroupByShardCount(), MAX_SHARDS);
            shardCountShr = 32 - Numbers.msb(shardCount);
            ownerParticle = new Particle();
            perWorkerParticles = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerParticles.extendAndSet(i, new Particle());
            }

            ownerMapSink = RecordSinkFactory.getInstance(asm, columnTypes, listColumnFilter, keyFunctions, false);
            if (perWorkerKeyFunctions != null) {
                perWorkerMapSinks = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
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
        if (perWorkerLocks == null) {
            return -1;
        }
        if (workerId == -1 && owner) {
            // Owner thread is free to use the original filter anytime.
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
    }

    @Override
    public void close() {
        Misc.free(ownerParticle);
        Misc.freeObjList(perWorkerParticles);
        Misc.free(filter);
        Misc.freeObjList(keyFunctions);
        Misc.freeObjList(perWorkerFilters);
        if (perWorkerKeyFunctions != null) {
            for (int i = 0, n = perWorkerKeyFunctions.size(); i < n; i++) {
                Misc.freeObjList(perWorkerKeyFunctions.getQuick(i));
            }
        }
    }

    public Function getFilter(int slotId) {
        if (slotId == -1 || perWorkerFilters == null) {
            return filter;
        }
        return perWorkerFilters.getQuick(slotId);
    }

    public GroupByFunctionsUpdater getFunctionUpdater() {
        return functionUpdater;
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

    public void mergeShard(int shardIndex) {
        assert sharded;

        final Map destMap = ownerParticle.getShardMaps().getQuick(shardIndex);
        final int perWorkerMapCount = perWorkerParticles.size();

        long sizeEstimate = destMap.size();
        for (int i = 0; i < perWorkerMapCount; i++) {
            final Particle srcParticle = perWorkerParticles.getQuick(i);
            final Map srcMap = srcParticle.getShardMaps().getQuick(shardIndex);
            sizeEstimate += srcMap.size();
        }

        if (sizeEstimate > 0) {
            // Pre-size the destination map, so that we don't have to resize it later.
            destMap.setKeyCapacity((int) sizeEstimate);
        }

        for (int i = 0; i < perWorkerMapCount; i++) {
            final Particle srcParticle = perWorkerParticles.getQuick(i);
            final Map srcMap = srcParticle.getShardMaps().getQuick(shardIndex);
            destMap.merge(srcMap, functionUpdater);
        }

        for (int i = 0, n = perWorkerParticles.size(); i < n; i++) {
            final Particle srcParticle = perWorkerParticles.getQuick(i);
            final Map srcMap = srcParticle.getShardMaps().getQuick(shardIndex);
            srcMap.close();
        }
    }

    public void release(int slotId) {
        if (perWorkerLocks != null) {
            perWorkerLocks.releaseSlot(slotId);
        }
    }

    @Override
    public void reopen() {
        ownerParticle.reopen();
        for (int i = 0, n = perWorkerParticles.size(); i < n; i++) {
            Particle p = perWorkerParticles.getQuick(i);
            p.reopen();
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

    public void tryShard(Particle particle) {
        if (particle.isSharded()) {
            return;
        }
        if (particle.getMap().size() > shardingThreshold || sharded) {
            particle.shard();
            sharded = true;
        }
    }

    private int shardIndex(int hashCode) {
        return hashCode >>> shardCountShr;
    }

    public class Particle implements Reopenable, QuietCloseable {
        private final Map map; // non-sharded partial result
        private final ObjList<Map> shards; // this.map split into shards
        private boolean sharded;

        private Particle() {
            this.map = MapFactory.createMap(configuration, keyTypes, valueTypes);
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
            final int shardIndex = shardIndex(hashCode);
            return shards.getQuick(shardIndex);
        }

        public ObjList<Map> getShardMaps() {
            return shards;
        }

        public boolean isSharded() {
            return sharded;
        }

        @Override
        public void reopen() {
            map.reopen();
        }

        private void reopenShards() {
            int size = shards.size();
            if (size == 0) {
                for (int i = 0; i < shardCount; i++) {
                    shards.add(MapFactory.createMap(configuration, keyTypes, valueTypes));
                }
            } else {
                assert size == shardCount;
                for (int i = 0, n = shards.size(); i < n; i++) {
                    Map m = shards.getQuick(i);
                    if (m != null) {
                        m.reopen();
                    }
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
    }
}

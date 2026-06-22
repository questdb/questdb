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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Manages a group-by map that can optionally be split into shards
 * for parallel merge during high-cardinality GROUP BY.
 * <p>
 * Used by both {@link AsyncGroupByAtom} and {@link AsyncHorizonJoinAtom}.
 */
public class GroupByMapFragment implements QuietCloseable {
    // We use the first bits of hash code to determine the shard.
    public static final int NUM_SHARDS = 256;
    public static final int NUM_SHARDS_SHR = Long.numberOfLeadingZeros(NUM_SHARDS) + 1;
    final Map map; // non-sharded partial result
    final int slotId; // -1 stands for owner fragment
    private final CairoConfiguration configuration;
    private final GroupByFunctionsUpdater groupByFunctionsUpdater;
    private final ColumnTypes keyTypes;
    // Per-query native memory tracker bound by the owning atom before any map is
    // (re)opened. Null when no per-query limit applies (e.g. the shared horizon-join
    // path that never binds a tracker), in which case allocations stay global-only.
    @Nullable
    private MemoryTracker memoryTracker;
    private final GroupByMapStats ownerStats;
    private final ObjList<GroupByMapStats> shardStats;
    private final ObjList<Map> shards; // this.map split into shards
    private final ColumnTypes valueTypes;
    private final int workerCount;
    boolean sharded;
    long totalFunctionCardinality;

    GroupByMapFragment(
            CairoConfiguration configuration,
            ColumnTypes keyTypes,
            ColumnTypes valueTypes,
            GroupByMapStats ownerStats,
            ObjList<GroupByMapStats> shardStats,
            GroupByFunctionsUpdater groupByFunctionsUpdater,
            int workerCount,
            int slotId
    ) {
        this.configuration = configuration;
        this.keyTypes = keyTypes;
        this.valueTypes = valueTypes;
        this.ownerStats = ownerStats;
        this.shardStats = shardStats;
        this.groupByFunctionsUpdater = groupByFunctionsUpdater;
        this.workerCount = workerCount;
        this.shards = new ObjList<>(NUM_SHARDS);
        this.slotId = slotId;
        // Lazy variant: the map's native backing is allocated by the first reopenMap()
        // call, after the owning atom binds a per-query MemoryTracker, so malloc and free
        // are charged symmetrically on the per-query counter. reopenMap() runs
        // setBatchEmptyValue() once the map is open.
        this.map = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes, true, false);
    }

    @Override
    public void close() {
        // Free the map and shards under the still-bound per-query tracker (setMemoryTracker()
        // is never nulled here): each free debits the same tracker that charged the matching
        // (re)open, so the per-query counter balances. The owning atom closes this fragment
        // while that tracker is live; it nulls only its pooled allocators, whose backing
        // outlives the query, before their final free.
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
            int keyCapacity = targetKeyCapacity(configuration, workerCount, ownerStats, owner);
            long heapSize = targetHeapSize(configuration, workerCount, ownerStats, owner);
            map.setMemoryTracker(memoryTracker);
            map.reopen(keyCapacity, heapSize);
            try {
                // Set up the empty value pattern used by the batched dispatch path.
                map.setBatchEmptyValue(groupByFunctionsUpdater);
            } catch (Throwable th) {
                map.close();
                throw th;
            }
        }
        return map;
    }

    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        this.memoryTracker = tracker;
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
            // Lazy variant: shards start closed so their backing allocates under the
            // bound tracker on the reopen() below, matching the free at fragment close().
            // Register each shard before reopen() so a reopen that trips the per-query
            // memory limit leaves the partial map tracked for cleanup at fragment close().
            for (int i = 0; i < NUM_SHARDS; i++) {
                final Map shard = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes, true, false);
                shards.add(shard);
                shard.setMemoryTracker(memoryTracker);
                shard.reopen();
            }
        } else {
            for (int i = 0; i < NUM_SHARDS; i++) {
                GroupByMapStats stats = shardStats.getQuick(i);
                int keyCapacity = targetKeyCapacity(configuration, workerCount, stats, false);
                long heapSize = targetHeapSize(configuration, workerCount, stats, false);
                final Map shard = shards.getQuick(i);
                shard.setMemoryTracker(memoryTracker);
                shard.reopen(keyCapacity, heapSize);
            }
        }
    }

    /**
     * Calculates pre-sized map's heap size based on the given stats.
     *
     * @param configuration Cairo configuration
     * @param workerCount   number of per-worker fragments
     * @param stats         statistics collected during the last query run
     * @param dest          merge destination map flag;
     *                      once merge is done, merge destination contains entries from all fragment maps
     * @return heap size to pre-size the map
     */
    static long targetHeapSize(CairoConfiguration configuration, int workerCount, GroupByMapStats stats, boolean dest) {
        final long statHeapSize = dest ? stats.mergedHeapSize : stats.maxHeapSize;
        // Per-worker limit is smaller than the owner one.
        final long statLimit = dest
                ? configuration.getGroupByPresizeMaxHeapSize()
                : configuration.getGroupByPresizeMaxHeapSize() / workerCount;
        long heapSize = configuration.getSqlSmallMapPageSize();
        if (statHeapSize <= statLimit) {
            heapSize = Math.max(statHeapSize, heapSize);
        }
        return heapSize;
    }

    /**
     * Calculates pre-sized map's capacity based on the given stats.
     *
     * @param configuration Cairo configuration
     * @param workerCount   number of per-worker fragments
     * @param stats         statistics collected during the last query run
     * @param dest          merge destination map flag;
     *                      once merge is done, merge destination contains entries from all fragment maps
     * @return capacity to pre-size the map
     */
    static int targetKeyCapacity(CairoConfiguration configuration, int workerCount, GroupByMapStats stats, boolean dest) {
        final long statKeyCapacity = dest ? stats.mergedSize : stats.medianSize;
        // Per-worker limit is smaller than the owner one.
        final long statLimit = dest
                ? configuration.getGroupByPresizeMaxCapacity()
                : configuration.getGroupByPresizeMaxCapacity() / workerCount;
        int keyCapacity = configuration.getSqlSmallMapKeyCapacity();
        if (statKeyCapacity <= statLimit) {
            keyCapacity = Math.max((int) statKeyCapacity, keyCapacity);
        }
        return keyCapacity;
    }
}

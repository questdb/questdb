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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

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
    private final CairoConfiguration configuration;
    private final ColumnTypes keyTypes;
    final Map map; // non-sharded partial result
    private final GroupByMapStats ownerStats;
    private final ObjList<GroupByMapStats> shardStats;
    private final ObjList<Map> shards; // this.map split into shards
    final int slotId; // -1 stands for owner fragment
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
            int workerCount,
            int slotId
    ) {
        this.configuration = configuration;
        this.keyTypes = keyTypes;
        this.valueTypes = valueTypes;
        this.ownerStats = ownerStats;
        this.shardStats = shardStats;
        this.workerCount = workerCount;
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
            int keyCapacity = targetKeyCapacity(configuration, workerCount, ownerStats, owner);
            long heapSize = targetHeapSize(configuration, workerCount, ownerStats, owner);
            map.reopen(keyCapacity, heapSize);
        }
        return map;
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

    private void reopenShards() {
        int size = shards.size();
        if (size == 0) {
            for (int i = 0; i < NUM_SHARDS; i++) {
                shards.add(MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes));
            }
        } else {
            for (int i = 0; i < NUM_SHARDS; i++) {
                GroupByMapStats stats = shardStats.getQuick(i);
                int keyCapacity = targetKeyCapacity(configuration, workerCount, stats, false);
                long heapSize = targetHeapSize(configuration, workerCount, stats, false);
                shards.getQuick(i).reopen(keyCapacity, heapSize);
            }
        }
    }
}

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

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.tasks.GroupByMergeShardTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS;

/**
 * Holds radix-partitioned (sharded) group-by state and provides shared
 * merge/pre-sizing logic used by both {@link AsyncGroupByAtom} and
 * {@link AsyncHorizonJoinAtom}.
 * <p>
 * When the number of distinct keys exceeds the sharding threshold, each
 * per-worker map is split into {@link GroupByMapFragment#NUM_SHARDS} shards
 * using the top bits of the key hash ({@code hashCode >>> NUM_SHARDS_SHR}).
 * Shards with the same index across all workers contain non-intersecting
 * key sets, so they can be merged in parallel via
 * {@link io.questdb.griffin.engine.groupby.GroupByMergeShardJob}.
 */
public class GroupByShardingContext implements QuietCloseable, Mutable {
    private static final Log LOG = LogFactory.getLog(GroupByShardingContext.class);
    private final CairoConfiguration configuration;
    private final ObjList<Map> destShards;
    private final ColumnTypes keyTypes;
    private final GroupByMapStats lastOwnerStats;
    private final ObjList<GroupByMapStats> lastShardStats;
    private final GroupByMapFragment ownerFragment;
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByMapFragment> perWorkerFragments;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final PerWorkerLocks perWorkerLocks;
    private final ColumnTypes valueTypes;
    volatile boolean sharded;
    boolean shardedHint;

    GroupByShardingContext(
            CairoConfiguration configuration,
            ColumnTypes keyTypes,
            ColumnTypes valueTypes,
            GroupByFunctionsUpdater ownerFunctionUpdater,
            @Nullable ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters,
            PerWorkerLocks perWorkerLocks,
            int workerCount
    ) {
        this.configuration = configuration;
        this.keyTypes = keyTypes;
        this.valueTypes = valueTypes;
        this.perWorkerLocks = perWorkerLocks;
        this.ownerFunctionUpdater = ownerFunctionUpdater;
        this.perWorkerFunctionUpdaters = perWorkerFunctionUpdaters;

        lastShardStats = new ObjList<>(NUM_SHARDS);
        for (int i = 0; i < NUM_SHARDS; i++) {
            lastShardStats.extendAndSet(i, new GroupByMapStats());
        }
        lastOwnerStats = new GroupByMapStats();
        ownerFragment = new GroupByMapFragment(configuration, keyTypes, valueTypes, lastOwnerStats, lastShardStats, workerCount, -1);
        perWorkerFragments = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            perWorkerFragments.extendAndSet(i, new GroupByMapFragment(configuration, keyTypes, valueTypes, lastOwnerStats, lastShardStats, workerCount, i));
        }
        // Destination shards are lazily initialized by the worker threads.
        destShards = new ObjList<>(NUM_SHARDS);
        destShards.setPos(NUM_SHARDS);
    }

    @Override
    public void clear() {
        sharded = false;
        Misc.free(ownerFragment);
        Misc.freeObjListAndKeepObjects(perWorkerFragments);
        Misc.freeObjListAndKeepObjects(destShards);
    }

    @Override
    public void close() {
        Misc.free(ownerFragment);
        Misc.freeObjList(perWorkerFragments);
        Misc.freeObjList(destShards);
    }

    public int maybeAcquire(int workerId, boolean owner, ExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void mergeShard(int slotId, int shardIndex) {
        mergeShard(shardIndex, getFunctionUpdater(slotId));
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    private Map mergeOwnerMap(GroupByFunctionsUpdater functionUpdater) {
        final Map destMap = ownerFragment.reopenMap();
        final int perWorkerMapCount = perWorkerFragments.size();

        // Calculate medians before the merge.
        final GroupByMapStats stats = lastOwnerStats;
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

    private void mergeShard(int shardIndex, GroupByFunctionsUpdater functionUpdater) {
        assert sharded;

        final Map destMap = reopenDestShard(shardIndex);
        final int perWorkerMapCount = perWorkerFragments.size();

        // Calculate medians before the merge.
        final GroupByMapStats stats = lastShardStats.getQuick(shardIndex);
        final LongList medianList = stats.medianList;
        medianList.clear();
        for (int i = 0; i < perWorkerMapCount; i++) {
            final GroupByMapFragment srcFragment = perWorkerFragments.getQuick(i);
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
                final GroupByMapFragment srcFragment = perWorkerFragments.getQuick(i);
                final Map srcMap = srcFragment.getShards().getQuick(shardIndex);
                maxHeapSize = Math.max(maxHeapSize, srcMap.getHeapSize());
            }
        }

        // Now do the actual merge.
        for (int i = 0; i < perWorkerMapCount; i++) {
            final GroupByMapFragment srcFragment = perWorkerFragments.getQuick(i);
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

    private Map reopenDestShard(int shardIndex) {
        Map destMap = destShards.getQuick(shardIndex);
        if (destMap == null) {
            destMap = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
            destShards.set(shardIndex, destMap);
        } else if (!destMap.isOpen()) {
            GroupByMapStats stats = lastShardStats.getQuick(shardIndex);
            int keyCapacity = GroupByMapFragment.targetKeyCapacity(configuration, perWorkerFragments.size(), stats, true);
            long heapSize = GroupByMapFragment.targetHeapSize(configuration, perWorkerFragments.size(), stats, true);
            destMap.reopen(keyCapacity, heapSize);
        }
        return destMap;
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

    void finalizeShardStats() {
        // Find max heap size and apply it to all shards.
        if (configuration.isGroupByPresizeEnabled()) {
            // First, calculate max heap size.
            long maxHeapSize = 0;
            for (int i = 0; i < NUM_SHARDS; i++) {
                final GroupByMapStats stats = lastShardStats.getQuick(i);
                maxHeapSize = Math.max(stats.maxHeapSize, maxHeapSize);
            }
            // Next, apply it to all shard stats.
            for (int i = 0; i < NUM_SHARDS; i++) {
                lastShardStats.getQuick(i).maxHeapSize = maxHeapSize;
            }
        }

        updateShardedHint();
    }

    ObjList<Map> getDestShards() {
        return destShards;
    }

    GroupByMapFragment getFragment(int slotId) {
        if (slotId == -1) {
            return ownerFragment;
        }
        return perWorkerFragments.getQuick(slotId);
    }

    GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1 || perWorkerFunctionUpdaters == null) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    boolean isSharded() {
        return sharded;
    }

    void maybeEnableSharding(GroupByMapFragment fragment, long functionCardinalityIncrement) {
        fragment.totalFunctionCardinality += functionCardinalityIncrement;
        // Functions are cheaper to merge when compared with merging the maps, hence the 10x multiplier.
        final int shardingThreshold = configuration.getGroupByShardingThreshold();
        if (!sharded && (fragment.getMap().size() > shardingThreshold || fragment.totalFunctionCardinality > 10L * shardingThreshold)) {
            sharded = true;
        }
    }

    Map mergeOwnerMap() {
        return mergeOwnerMap(getFunctionUpdater(-1));
    }

    /**
     * Merges per-worker shard maps into destination shards in parallel.
     * The owner thread publishes merge tasks and work-steals when the queue is full.
     *
     * @return destination shard maps (non-intersecting key sets)
     */
    ObjList<Map> mergeShards(
            MessageBus messageBus,
            WorkStealingStrategy workStealingStrategy,
            SqlExecutionCircuitBreaker circuitBreaker,
            AtomicBooleanCircuitBreaker postAggregationCircuitBreaker,
            SOUnboundedCountDownLatch postAggregationDoneLatch,
            AtomicInteger postAggregationStartedCounter
    ) {
        postAggregationCircuitBreaker.reset();
        postAggregationStartedCounter.set(0);
        postAggregationDoneLatch.reset();

        // First, make sure to shard all non-sharded maps, if any.
        shardAll();

        // Next, merge each set of partial shard maps into the final shard map. This is done in parallel.
        final RingQueue<GroupByMergeShardTask> queue = messageBus.getGroupByMergeShardQueue();
        final MPSequence pubSeq = messageBus.getGroupByMergeShardPubSeq();
        final MCSequence subSeq = messageBus.getGroupByMergeShardSubSeq();
        final WorkStealingStrategy strategy = workStealingStrategy.of(postAggregationStartedCounter);

        int queuedCount = 0;
        int ownCount = 0;
        int reclaimed = 0;
        int total = 0;
        int mergedCount = 0; // used for work stealing decisions

        try {
            for (int shardIndex = 0; shardIndex < NUM_SHARDS; shardIndex++) {
                while (true) {
                    long cursor = pubSeq.next();
                    if (cursor < 0) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

                        if (strategy.shouldSteal(mergedCount)) {
                            mergeShard(-1, shardIndex);
                            ownCount++;
                            total++;
                            mergedCount = postAggregationDoneLatch.getCount();
                            break;
                        }
                        mergedCount = postAggregationDoneLatch.getCount();
                    } else {
                        queue.get(cursor).of(
                                postAggregationCircuitBreaker,
                                postAggregationStartedCounter,
                                postAggregationDoneLatch,
                                this,
                                shardIndex
                        );
                        pubSeq.done(cursor);
                        queuedCount++;
                        total++;
                        break;
                    }
                }
            }
        } catch (Throwable th) {
            postAggregationCircuitBreaker.cancel();
            throw th;
        } finally {
            while (!postAggregationDoneLatch.done(queuedCount)) {
                if (circuitBreaker.checkIfTripped()) {
                    postAggregationCircuitBreaker.cancel();
                }

                if (strategy.shouldSteal(mergedCount)) {
                    long cursor = subSeq.next();
                    if (cursor > -1) {
                        GroupByMergeShardTask task = queue.get(cursor);
                        GroupByMergeShardJob.run(-1, task, subSeq, cursor, this);
                        reclaimed++;
                    } else {
                        Os.pause();
                    }
                } else {
                    Os.pause();
                }
                mergedCount = postAggregationDoneLatch.getCount();
            }
        }

        if (!postAggregationCircuitBreaker.checkIfTripped()) {
            finalizeShardStats();
        }

        LOG.debug().$("merge shards done [total=").$(total)
                .$(", ownCount=").$(ownCount)
                .$(", reclaimed=").$(reclaimed)
                .$(", queuedCount=").$(queuedCount)
                .I$();

        return destShards;
    }

    void reopen() {
        if (shardedHint) {
            // Looks like we had to shard during previous execution, so let's do it ahead of time.
            sharded = true;
        }
    }

    void shardAll() {
        ownerFragment.shard();
        for (int i = 0, n = perWorkerFragments.size(); i < n; i++) {
            perWorkerFragments.getQuick(i).shard();
        }
    }
}

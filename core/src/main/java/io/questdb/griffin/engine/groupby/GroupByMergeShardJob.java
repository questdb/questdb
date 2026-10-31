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

package io.questdb.griffin.engine.groupby;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.AsyncQueryProgressState;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.cairo.sql.async.QueryParallelOwnerLoop;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.tasks.GroupByMergeShardTask;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS;

/**
 * Handles parallel merge map shard tasks.
 *
 * @see GroupByShardMerger
 */
public class GroupByMergeShardJob extends AbstractQueueConsumerJob<GroupByMergeShardTask> {
    private static final Log LOG = LogFactory.getLog(GroupByMergeShardJob.class);
    private final MessageBus messageBus;

    public GroupByMergeShardJob(MessageBus messageBus) {
        super(messageBus.getGroupByMergeShardQueue(), messageBus.getGroupByMergeShardSubSeq());
        this.messageBus = messageBus;
    }

    /**
     * Publishes one task per shard, merges a shard on the owner thread when the queue is full or the owner may
     * steal, and returns once every shard is merged or the merge is cancelled.
     */
    public static void mergeShards(
            GroupByShardMerger merger,
            QueryParallelOwnerLoop ownerLoop,
            MessageBus messageBus,
            WorkStealingStrategy workStealingStrategy,
            SqlExecutionCircuitBreaker circuitBreaker,
            PostAggregationCircuitBreaker postAggregationCircuitBreaker,
            SOUnboundedCountDownLatch postAggregationDoneLatch,
            AtomicInteger postAggregationStartedCounter
    ) {
        postAggregationCircuitBreaker.reset();
        postAggregationStartedCounter.set(0);
        postAggregationDoneLatch.reset();

        final RingQueue<GroupByMergeShardTask> queue = messageBus.getGroupByMergeShardQueue();
        final MPSequence pubSeq = messageBus.getGroupByMergeShardPubSeq();
        final MCSequence subSeq = messageBus.getGroupByMergeShardSubSeq();
        final WorkStealingStrategy strategy = workStealingStrategy.of(postAggregationStartedCounter);
        final QueryParallelFiberDispatcher dispatcher = messageBus.getQueryParallelFiberDispatcher();
        ownerLoop.of(dispatcher, circuitBreaker, merger.getProgressState());
        ownerLoop.tryAcquirePublication();

        if (!ownerLoop.hasPublication()) {
            for (int shardIndex = 0; shardIndex < NUM_SHARDS; shardIndex++) {
                ownerLoop.checkBeforeHelping();
                merger.mergeShard(-1, shardIndex);
            }
            return;
        }

        int queuedCount = 0;
        int ownCount = 0;
        int reclaimed = 0;
        int total = 0;
        int mergedCount = 0; // positive completed-task count; the latch counts down from zero

        try {
            for (int shardIndex = 0; shardIndex < NUM_SHARDS; shardIndex++) {
                while (true) {
                    ownerLoop.observeProgress();
                    long cursor = pubSeq.next();
                    if (cursor < 0) {
                        if (strategy.shouldSteal(mergedCount)) {
                            ownerLoop.checkBeforeHelping();
                            merger.mergeShard(-1, shardIndex);
                            ownCount++;
                            total++;
                            mergedCount = -postAggregationDoneLatch.getCount();
                            break;
                        }
                        if (!ownerLoop.awaitProgress()) {
                            Os.pause();
                        }
                        mergedCount = -postAggregationDoneLatch.getCount();
                    } else {
                        queue.get(cursor).of(
                                postAggregationCircuitBreaker,
                                postAggregationStartedCounter,
                                postAggregationDoneLatch,
                                merger,
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
            try {
                ownerLoop.releasePublication();
            } finally {
                while (true) {
                    ownerLoop.observeProgress();
                    if (postAggregationDoneLatch.done(queuedCount)) {
                        break;
                    }
                    final boolean isOwnerTripped = circuitBreaker.checkIfTrippedOrYield();
                    if (isOwnerTripped) {
                        postAggregationCircuitBreaker.cancel();
                    }

                    if (!ownerLoop.isOwnerParkable() && strategy.shouldSteal(mergedCount)) {
                        long cursor = subSeq.next();
                        if (cursor > -1) {
                            GroupByMergeShardTask task = queue.get(cursor);
                            // run() releases the slot
                            if (dispatcher != null) {
                                run(-1, task, subSeq, cursor, merger, dispatcher);
                            } else {
                                run(-1, task, subSeq, cursor, merger);
                            }
                            reclaimed++;
                        } else {
                            Os.pause();
                        }
                    } else if (!ownerLoop.awaitProgressWhileDraining(isOwnerTripped)) {
                        Os.pause();
                    }
                    mergedCount = -postAggregationDoneLatch.getCount();
                }
            }
        }

        LOG.debug().$("merge shards done [total=").$(total)
                .$(", ownCount=").$(ownCount)
                .$(", reclaimed=").$(reclaimed)
                .$(", queuedCount=").$(queuedCount)
                .I$();
    }

    public static void run(
            int carrierId,
            GroupByMergeShardTask task,
            Sequence subSeq,
            long cursor,
            GroupByShardMerger stealingMerger
    ) {
        final PostAggregationCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final GroupByShardMerger merger = task.getMerger();
        final int shardIndex = task.getShardIndex();

        task.clear();
        subSeq.done(cursor);

        final boolean owner = stealingMerger != null && stealingMerger == merger;
        runDetached(carrierId, circuitBreaker, startedCounter, doneLatch, merger, shardIndex, owner);
    }

    public static void run(
            int carrierId,
            GroupByMergeShardTask task,
            Sequence subSeq,
            long cursor,
            GroupByShardMerger stealingMerger,
            @NotNull QueryParallelFiberDispatcher dispatcher
    ) {
        final PostAggregationCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final GroupByShardMerger merger = task.getMerger();
        final int shardIndex = task.getShardIndex();
        final AsyncQueryProgressState ownerProgress = merger.getProgressState();
        final boolean isOwner = stealingMerger != null && stealingMerger == merger;

        task.clear();
        Throwable failure = null;
        try {
            subSeq.done(cursor);
        } catch (Throwable th) {
            failure = th;
        }
        try {
            dispatcher.signalQueueProgress();
        } catch (Throwable th) {
            failure = Misc.foldCleanupFailure(failure, th);
        }
        try {
            runDetached(carrierId, circuitBreaker, startedCounter, doneLatch, merger, shardIndex, isOwner);
        } catch (Throwable th) {
            failure = Misc.foldCleanupFailure(failure, th);
        }
        try {
            dispatcher.signalOwnerProgress(ownerProgress);
        } catch (Throwable th) {
            failure = Misc.foldCleanupFailure(failure, th);
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    public static void runDetached(
            int carrierId,
            PostAggregationCircuitBreaker circuitBreaker,
            AtomicInteger startedCounter,
            CountDownLatchSPI doneLatch,
            GroupByShardMerger merger,
            int shardIndex,
            boolean owner
    ) {
        startedCounter.incrementAndGet();
        try {
            final int slotId = merger.maybeAcquire(carrierId, owner, circuitBreaker);
            try {
                if (!circuitBreaker.checkIfTrippedOrYield()) {
                    merger.mergeShard(slotId, shardIndex);
                }
            } finally {
                merger.release(slotId);
            }
        } catch (Throwable th) {
            Throwable failure = null;
            try {
                LOG.error().$("merge shard failed [error=").$(th).I$();
            } catch (Throwable cleanupFailure) {
                failure = Misc.foldCleanupFailure(failure, cleanupFailure);
            }
            try {
                circuitBreaker.cancel(th);
            } catch (Throwable cleanupFailure) {
                failure = Misc.foldCleanupFailure(failure, cleanupFailure);
            }
            try {
                doneLatch.detachResourceMemoryAndCountDown();
            } catch (Throwable cleanupFailure) {
                failure = Misc.foldCleanupFailure(failure, cleanupFailure);
            }
            CairoException.rethrowCleanupFailure(failure);
            return;
        }
        doneLatch.detachResourceMemoryAndCountDown();
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        final QueryParallelFiberDispatcher dispatcher = messageBus.getQueryParallelFiberDispatcher();
        return dispatcher != null
                ? !dispatcher.consumeMergeShard(workerContext.carrierId())
                : super.run(workerContext);
    }

    @Override
    protected boolean doRun(long cursor, WorkerContext workerContext) {
        final GroupByMergeShardTask task = queue.get(cursor);
        run(workerContext.carrierId(), task, subSeq, cursor, null);
        return true;
    }
}

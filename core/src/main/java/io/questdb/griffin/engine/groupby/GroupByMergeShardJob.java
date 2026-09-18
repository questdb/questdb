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
import io.questdb.cairo.sql.async.AsyncQueryProgressState;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.griffin.engine.table.GroupByShardingContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Sequence;
import io.questdb.std.Misc;
import io.questdb.tasks.GroupByMergeShardTask;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles parallel merge map shard tasks.
 *
 * @see GroupByShardingContext
 */
public class GroupByMergeShardJob extends AbstractQueueConsumerJob<GroupByMergeShardTask> {
    private static final Log LOG = LogFactory.getLog(GroupByMergeShardJob.class);
    private final MessageBus messageBus;

    public GroupByMergeShardJob(MessageBus messageBus) {
        super(messageBus.getGroupByMergeShardQueue(), messageBus.getGroupByMergeShardSubSeq());
        this.messageBus = messageBus;
    }

    public static void run(
            int carrierId,
            GroupByMergeShardTask task,
            Sequence subSeq,
            long cursor,
            GroupByShardingContext stealingCtx
    ) {
        final PostAggregationCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final GroupByShardingContext ctx = task.getShardingContext();
        final int shardIndex = task.getShardIndex();

        task.clear();
        subSeq.done(cursor);

        final boolean owner = stealingCtx != null && stealingCtx == ctx;
        runDetached(carrierId, circuitBreaker, startedCounter, doneLatch, ctx, shardIndex, owner);
    }

    public static void run(
            int carrierId,
            GroupByMergeShardTask task,
            Sequence subSeq,
            long cursor,
            GroupByShardingContext stealingCtx,
            @NotNull QueryParallelFiberDispatcher dispatcher
    ) {
        final PostAggregationCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final GroupByShardingContext ctx = task.getShardingContext();
        final int shardIndex = task.getShardIndex();
        final AsyncQueryProgressState ownerProgress = ctx.getProgressState();
        final boolean isOwner = stealingCtx != null && stealingCtx == ctx;

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
            runDetached(carrierId, circuitBreaker, startedCounter, doneLatch, ctx, shardIndex, isOwner);
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
            GroupByShardingContext ctx,
            int shardIndex,
            boolean owner
    ) {
        startedCounter.incrementAndGet();
        try {
            final int slotId = ctx.maybeAcquire(carrierId, owner, circuitBreaker);
            try {
                if (!circuitBreaker.checkIfTripped()) {
                    ctx.mergeShard(slotId, shardIndex);
                }
            } finally {
                ctx.release(slotId);
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
                doneLatch.countDown();
            } catch (Throwable cleanupFailure) {
                failure = Misc.foldCleanupFailure(failure, cleanupFailure);
            }
            CairoException.rethrowCleanupFailure(failure);
            return;
        }
        doneLatch.countDown();
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

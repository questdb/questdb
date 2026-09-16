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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.async.AsyncQueryProgressState;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Sequence;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.Misc;
import io.questdb.tasks.GroupByLongTopKTask;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles parallel ORDER BY long_column LIMIT N tasks on shards from a sharded map.
 *
 * @see AsyncGroupByRecordCursorFactory
 */
public class GroupByLongTopKJob extends AbstractQueueConsumerJob<GroupByLongTopKTask> {
    private static final Log LOG = LogFactory.getLog(GroupByLongTopKJob.class);
    private final MessageBus messageBus;

    public GroupByLongTopKJob(MessageBus messageBus) {
        super(messageBus.getGroupByLongTopKQueue(), messageBus.getGroupByLongTopKSubSeq());
        this.messageBus = messageBus;
    }

    public static void run(
            int workerId,
            GroupByLongTopKTask task,
            Sequence subSeq,
            long cursor,
            AsyncGroupByAtom stealingAtom
    ) {
        final PostAggregationCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final AsyncGroupByAtom atom = task.getAtom();
        final Function longFunc = task.getFunction();
        final int shardIndex = task.getShardIndex();
        final int order = task.getOrder();
        final int limit = task.getLimit();

        task.clear();
        subSeq.done(cursor);

        final boolean owner = stealingAtom != null && stealingAtom == atom;
        runDetached(
                workerId,
                circuitBreaker,
                startedCounter,
                doneLatch,
                atom,
                longFunc,
                shardIndex,
                order,
                limit,
                owner
        );
    }

    public static void run(
            int workerId,
            GroupByLongTopKTask task,
            Sequence subSeq,
            long cursor,
            AsyncGroupByAtom stealingAtom,
            @NotNull QueryParallelFiberDispatcher dispatcher
    ) {
        final PostAggregationCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final AsyncGroupByAtom atom = task.getAtom();
        final Function longFunc = task.getFunction();
        final int shardIndex = task.getShardIndex();
        final int order = task.getOrder();
        final int limit = task.getLimit();
        final AsyncQueryProgressState ownerProgress = atom.getShardingContext().getProgressState();
        final boolean isOwner = stealingAtom != null && stealingAtom == atom;

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
            runDetached(
                    workerId,
                    circuitBreaker,
                    startedCounter,
                    doneLatch,
                    atom,
                    longFunc,
                    shardIndex,
                    order,
                    limit,
                    isOwner
            );
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
            int workerId,
            PostAggregationCircuitBreaker circuitBreaker,
            AtomicInteger startedCounter,
            CountDownLatchSPI doneLatch,
            AsyncGroupByAtom atom,
            Function longFunc,
            int shardIndex,
            int order,
            int limit,
            boolean owner
    ) {
        startedCounter.incrementAndGet();
        try {
            final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
            try {
                if (!circuitBreaker.checkIfTripped()) {
                    final Map shard = atom.getDestShards().getQuick(shardIndex);
                    final DirectLongLongSortedList list = atom.getLongTopKList(slotId, order, limit);
                    shard.getCursor().longTopK(list, longFunc);
                }
            } finally {
                atom.release(slotId);
            }
        } catch (Throwable th) {
            Throwable failure = null;
            try {
                LOG.error().$("long top K on shard failed [error=").$(th).I$();
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
                ? !dispatcher.consumeLongTopK(workerContext.carrierId())
                : super.run(workerContext);
    }

    @Override
    protected boolean doRun(long cursor, WorkerContext workerContext) {
        final GroupByLongTopKTask task = queue.get(cursor);
        run(workerContext.carrierId(), task, subSeq, cursor, null);
        return true;
    }
}

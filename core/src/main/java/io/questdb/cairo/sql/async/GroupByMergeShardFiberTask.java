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

package io.questdb.cairo.sql.async;

import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.groupby.PostAggregationCircuitBreaker;
import io.questdb.griffin.engine.table.GroupByShardingContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.tasks.GroupByMergeShardTask;

import java.util.concurrent.atomic.AtomicInteger;

final class GroupByMergeShardFiberTask extends AbstractQueryParallelFiberTask {
    private static final Log LOG = LogFactory.getLog(GroupByMergeShardFiberTask.class);
    private RingQueue<GroupByMergeShardTask> batchQueue;
    private PostAggregationCircuitBreaker circuitBreaker;
    private long cursor = -1;
    private CountDownLatchSPI doneLatch;
    private GroupByShardingContext shardingContext;
    private int shardIndex = -1;
    private boolean started;
    private AtomicInteger startedCounter;
    private Sequence subSeq;
    private int workerId = -1;

    GroupByMergeShardFiberTask(
            QueryParallelFiberDispatcher dispatcher,
            FiberTaskPool<?> pool,
            TimerShards timerShards
    ) {
        super(dispatcher, pool, timerShards);
    }

    void of(int workerId, GroupByMergeShardTask task, RingQueue<GroupByMergeShardTask> queue, MCSequence subSeq, long cursor) {
        this.workerId = workerId;
        this.circuitBreaker = task.getCircuitBreaker();
        this.startedCounter = task.getStartedCounter();
        this.doneLatch = task.getDoneLatch();
        this.shardingContext = task.getShardingContext();
        this.shardIndex = task.getShardIndex();
        this.subSeq = subSeq;
        this.cursor = cursor;
        this.batchQueue = queue;
        bindBatch(workerId, subSeq);
        bindCancellation(circuitBreaker);
        bindProgress(shardingContext.getProgressState());
        task.clear();
        releaseCursor();
    }

    @Override
    boolean isBound() {
        return doneLatch != null || cursor > -1;
    }

    @Override
    protected void cancelOwner() {
        if (circuitBreaker != null) {
            circuitBreaker.cancel();
        }
    }

    @Override
    protected void clearBatchBinding() {
        batchQueue = null;
    }

    @Override
    protected void clearBinding() {
        circuitBreaker = null;
        cursor = -1;
        doneLatch = null;
        shardingContext = null;
        shardIndex = -1;
        started = false;
        startedCounter = null;
        subSeq = null;
        workerId = -1;
    }

    @Override
    protected void completeOwnership() {
        if (!isBound()) {
            return;
        }
        try {
            releaseCursor();
        } finally {
            try {
                if (doneLatch != null) {
                    if (!started) {
                        startedCounter.incrementAndGet();
                    }
                    doneLatch.countDown();
                }
            } finally {
                clearBinding();
            }
        }
    }

    @Override
    protected void onTaskError(Throwable th) {
        LOG.error().$("merge shard fiber failed [error=").$(th).I$();
        cancelOwner();
    }

    @Override
    protected void rebind(int workerId, MCSequence subSeq, long cursor) {
        of(workerId, batchQueue.get(cursor), batchQueue, subSeq, cursor);
    }

    @Override
    protected boolean runTask() {
        started = true;
        try {
            GroupByMergeShardJob.runDetached(
                    workerId,
                    circuitBreaker,
                    startedCounter,
                    doneLatch,
                    shardingContext,
                    shardIndex,
                    false
            );
        } finally {
            clearBinding();
        }
        return true;
    }

    private void releaseCursor() {
        if (cursor > -1) {
            final long claimedCursor = cursor;
            final Sequence claimedSubSeq = subSeq;
            cursor = -1;
            subSeq = null;
            claimedSubSeq.done(claimedCursor);
            signalQueueProgress();
        }
    }
}

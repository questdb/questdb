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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.tasks.LatestByTask;

final class LatestByFiberTask extends AbstractQueryParallelFiberTask {
    private static final Log LOG = LogFactory.getLog(LatestByFiberTask.class);
    private RingQueue<LatestByTask> batchQueue;
    private long cursor = -1;
    private Sequence subSeq;
    private LatestByTask task;

    LatestByFiberTask(
            QueryParallelFiberDispatcher dispatcher,
            FiberTaskPool<?> pool,
            TimerShards timerShards
    ) {
        super(dispatcher, pool, timerShards);
    }

    void of(LatestByTask task, RingQueue<LatestByTask> queue, MCSequence subSeq, long cursor) {
        this.task = task;
        this.subSeq = subSeq;
        this.cursor = cursor;
        this.batchQueue = queue;
        bindBatch(-1, subSeq);
        bindCancellation(task.getCircuitBreaker());
        bindProgress(task.getProgressState());
    }

    @Override
    boolean isBound() {
        return task != null || cursor > -1;
    }

    @Override
    protected void cancelOwner() {
        if (task != null) {
            task.getCircuitBreaker().cancel();
        }
    }

    @Override
    protected void clearBatchBinding() {
        batchQueue = null;
    }

    @Override
    protected void clearBinding() {
        cursor = -1;
        subSeq = null;
        task = null;
    }

    @Override
    protected void completeOwnership() {
        if (!isBound()) {
            return;
        }
        try {
            if (task != null) {
                task.abort();
            }
        } finally {
            try {
                releaseCursor();
            } finally {
                clearBinding();
            }
        }
    }

    @Override
    protected void onTaskError(Throwable th) {
        LOG.error().$("latest by fiber failed [error=").$(th).I$();
        cancelOwner();
    }

    @Override
    protected void rebind(int workerId, MCSequence subSeq, long cursor) {
        of(batchQueue.get(cursor), batchQueue, subSeq, cursor);
    }

    @Override
    protected boolean runTask() {
        try {
            task.run();
        } catch (Throwable th) {
            task.getCircuitBreaker().cancel();
            throw th;
        } finally {
            task = null;
            try {
                releaseCursor();
            } finally {
                clearBinding();
            }
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

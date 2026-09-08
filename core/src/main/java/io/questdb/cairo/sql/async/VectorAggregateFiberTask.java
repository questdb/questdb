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

import io.questdb.griffin.engine.groupby.vect.VectorAggregateEntry;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.tasks.VectorAggregateTask;

final class VectorAggregateFiberTask extends AbstractQueryParallelFiberTask {
    private static final Log LOG = LogFactory.getLog(VectorAggregateFiberTask.class);
    private RingQueue<VectorAggregateTask> batchQueue;
    private long cursor = -1;
    private VectorAggregateEntry entry;
    private boolean started;
    private Sequence subSeq;
    private int workerId = -1;

    VectorAggregateFiberTask(
            QueryParallelFiberDispatcher dispatcher,
            FiberTaskPool<?> pool,
            TimerShards timerShards
    ) {
        super(dispatcher, pool, timerShards);
    }

    void of(int workerId, VectorAggregateTask task, RingQueue<VectorAggregateTask> queue, MCSequence subSeq, long cursor) {
        this.workerId = workerId;
        this.entry = task.entry;
        this.subSeq = subSeq;
        this.cursor = cursor;
        this.batchQueue = queue;
        bindBatch(workerId, subSeq);
        bindCancellation(entry.getCircuitBreaker());
        bindProgress(entry.getProgressState());
        task.entry = null;
        releaseCursor();
    }

    @Override
    boolean isBound() {
        return entry != null || cursor > -1;
    }

    @Override
    protected long boundEntryWeight() {
        final VectorAggregateEntry boundEntry = entry;
        return boundEntry != null ? boundEntry.getFrameRowCount() : 0;
    }

    @Override
    protected void cancelOwner() {
        if (entry != null) {
            entry.getCircuitBreaker().cancel();
        }
    }

    @Override
    protected void clearBatchBinding() {
        batchQueue = null;
    }

    @Override
    protected void clearBinding() {
        cursor = -1;
        entry = null;
        started = false;
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
                if (entry != null) {
                    entry.abort(started);
                }
            } finally {
                clearBinding();
            }
        }
    }

    @Override
    protected void onTaskError(Throwable th) {
        LOG.error().$("vectorized reduce fiber failed [ex=").$(th).I$();
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
            entry.runDetached(workerId);
        } finally {
            entry = null;
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

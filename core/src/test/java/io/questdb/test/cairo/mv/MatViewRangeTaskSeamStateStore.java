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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.std.Numbers;

/**
 * The real mat view state store, which can run the refresh job right after it queued a base table range task. The WAL
 * apply job queues the range task of a batch once the batch has committed, while it still holds the base table's
 * writer, and it moves the base's SeqTxnTracker only after it released the writer. A refresh job run there stands for
 * a refresh worker that takes the task in that window.
 * <p>
 * Once armed, the store calls the observer right after it queued the next range task, then runs refresh job passes,
 * on the caller's thread, until the job has taken that task: the passes run the tasks queued before it, the task, and
 * the tasks after it that the last pass reaches. The passes stop there, since a base table's incremental refresh that
 * reads the batch keeps queueing itself again until the WAL apply job notifies the batch's commit, which follows. The
 * apply job catches and logs what a range task enqueue throws, so the store keeps the failure for
 * {@link #disarmEarlyRangeTaskRun()} to throw. Disarmed, the store is the real one.
 */
public class MatViewRangeTaskSeamStateStore extends MatViewStateStoreImpl {
    private final CairoEngine engine;
    // the operation txn of the range task the refresh job is to take, LONG_NULL when there is none
    private volatile long earlyRangeTaskTxn = Numbers.LONG_NULL;
    private volatile Throwable earlyRangeTaskFailure;
    private volatile RangeTaskObserver rangeTaskObserver;

    public MatViewRangeTaskSeamStateStore(CairoEngine engine) {
        super(engine);
        this.engine = engine;
    }

    // Runs the refresh job right after the store queues the next range task, once, after the observer.
    public void armEarlyRangeTaskRun(RangeTaskObserver observer) {
        earlyRangeTaskFailure = null;
        rangeTaskObserver = observer;
    }

    // Disarms the store and throws what the observer or the refresh job threw.
    public void disarmEarlyRangeTaskRun() {
        rangeTaskObserver = null;
        final Throwable failure = earlyRangeTaskFailure;
        earlyRangeTaskFailure = null;
        if (failure != null) {
            throw new AssertionError("early range task run failed", failure);
        }
    }

    @Override
    public void enqueueAddRefreshRange(
            TableToken matViewToken,
            TableToken baseTableToken,
            long rangeFrom,
            long rangeTo,
            long baseTableTxn
    ) {
        super.enqueueAddRefreshRange(matViewToken, baseTableToken, rangeFrom, rangeTo, baseTableTxn);
        final RangeTaskObserver observer = rangeTaskObserver;
        if (observer != null) {
            rangeTaskObserver = null;
            try {
                observer.onRangeTaskQueued(baseTableToken, baseTableTxn);
                earlyRangeTaskTxn = baseTableTxn;
                try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine, 1)) {
                    // every pass takes a task, and the queue hands them out in order
                    while (earlyRangeTaskTxn != Numbers.LONG_NULL) {
                        refreshJob.run();
                    }
                }
            } catch (Throwable th) {
                earlyRangeTaskFailure = th;
            } finally {
                earlyRangeTaskTxn = Numbers.LONG_NULL;
            }
        }
    }

    @Override
    public boolean tryDequeueRefreshTask(MatViewRefreshTask task) {
        final boolean isDequeued = super.tryDequeueRefreshTask(task);
        if (isDequeued && task.operation == MatViewRefreshTask.ADD_REFRESH_RANGE && task.baseTableTxn == earlyRangeTaskTxn) {
            earlyRangeTaskTxn = Numbers.LONG_NULL;
        }
        return isDequeued;
    }

    @FunctionalInterface
    public interface RangeTaskObserver {
        void onRangeTaskQueued(TableToken baseTableToken, long baseTableTxn);
    }
}

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

package io.questdb.mp;

import org.jetbrains.annotations.NotNull;

/**
 * Job that drains an unbounded MPMC queue of {@link SqlContinuation} objects and
 * remounts each one on the calling worker thread. Implements {@link ContinuationSink}
 * so a continuation captures the queue at construction via {@code resumeSink::put}
 * and pushes itself in via {@link SqlContinuation#scheduleResume()} when fired or
 * cancelled.
 *
 * <p>Owned by a {@link io.questdb.mp.WorkerPool} and assigned to all workers of
 * that pool. A given continuation should be put exactly once per suspension --
 * the queue does not deduplicate; the producer must gate via TxnWaiter state CAS.
 */
public final class ContinuationResumeJob implements Job, ContinuationSink {
    private final Queue<ResumeTask> queue = ConcurrentQueue.createConcurrentQueue(ResumeTask::new);
    private final ThreadLocal<ResumeTask> scratch = ThreadLocal.withInitial(ResumeTask::new);

    @Override
    public void put(SqlContinuation cont) {
        ResumeTask t = scratch.get();
        t.cont = cont;
        queue.enqueue(t);
        t.cont = null;
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        ResumeTask t = scratch.get();
        if (!queue.tryDequeue(t)) {
            return false;
        }
        SqlContinuation cont = t.cont;
        t.cont = null;
        // A waiter can be fired by a concurrent thread (e.g. updateWriterTxns from
        // the WAL apply pool) in the narrow window between registerWaiter enqueueing
        // the waiter and the body actually reaching SqlContinuation.suspend(). In
        // that window the cont is still mounted on its registering carrier, and
        // Continuation.run() rejects the remount with IllegalStateException. Spin
        // until the carrier unmounts -- typically nanoseconds. We use cont.isDone()
        // as the structural test (not message-text matching against an internal
        // JDK string): a not-done cont that refused to run can only be in the
        // mounted-elsewhere state, since it was sitting in our queue with no
        // contender besides the original carrier. If isDone() is true, this is a
        // real bug (cont was put after completing) -- propagate.
        int spins = 0;
        while (true) {
            try {
                cont.run();
                return true;
            } catch (IllegalStateException e) {
                if (cont.isDone()) {
                    throw e;
                }
                if (++spins < 64) {
                    Thread.onSpinWait();
                } else {
                    Thread.yield();
                }
            }
        }
    }

    public static final class ResumeTask implements ValueHolder<ResumeTask> {
        public SqlContinuation cont;

        @Override
        public void clear() {
            cont = null;
        }

        @Override
        public void copyTo(ResumeTask dest) {
            dest.cont = cont;
        }
    }
}

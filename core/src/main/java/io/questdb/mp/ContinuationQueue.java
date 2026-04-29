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

/**
 * Holds an MPMC queue of {@link WorkerContinuation} objects waiting to be remounted on
 * a worker. Implements {@link ContinuationSink} so a continuation captures the
 * queue at construction via {@code resumeSink::put}, and pushes itself in via
 * {@link WorkerContinuation#scheduleResume()} when fired or cancelled.
 *
 * <p>This class is not a {@link Job}. The drain is driven by each worker's outer
 * driver between continuation mounts (see {@code Worker.run}). It cannot be a
 * regular job because invoking {@link WorkerContinuation#run} on a parked cont
 * requires the calling thread to NOT already be carrying a cont in the same
 * scope -- a constraint that holds in the worker's outer driver but not inside a
 * mounted worker-loop body.
 *
 * <p>Owned by a {@link WorkerPool}; pushes go onto this queue from any thread
 * (firing thread, timeout sweep, etc.); workers of the owning pool drain it.
 */
public final class ContinuationQueue implements ContinuationSink {
    private final Queue<ResumeTask> queue = ConcurrentQueue.createConcurrentQueue(ResumeTask::new);
    private final ThreadLocal<ResumeTask> producerScratch = ThreadLocal.withInitial(ResumeTask::new);

    @Override
    public void put(WorkerContinuation cont) {
        ResumeTask t = producerScratch.get();
        t.cont = cont;
        queue.enqueue(t);
        t.cont = null;
    }

    /**
     * Pop one parked continuation off the queue without running it. Returns
     * {@code null} if the queue is empty. The caller is responsible for
     * eventually running the returned cont via {@link #run(WorkerContinuation)}.
     *
     * <p>The {@code scratch} is a caller-managed buffer used by the queue's
     * value-holder copy protocol; passing it in (rather than having the queue
     * fetch a thread-local) lets the JVM escape-analyze and scalar-replace it
     * when the caller allocates it on its own stack frame.
     *
     * <p>Safe to call from inside a mounted continuation: this is just a queue
     * dequeue, no remount happens until {@link #run} is called.
     */
    public WorkerContinuation tryDequeue(ResumeTask scratch) {
        if (!queue.tryDequeue(scratch)) {
            return null;
        }
        WorkerContinuation cont = scratch.cont;
        scratch.cont = null;
        return cont;
    }

    /**
     * Remount {@code cont} on the calling thread. Must be called from a thread
     * that is NOT already carrying a cont in {@link WorkerContinuation#SCOPE}.
     *
     * <p>A waiter can be fired by a concurrent thread (e.g. updateWriterTxns from
     * the WAL apply pool) in the narrow window between registerWaiter enqueueing
     * the waiter and the body actually reaching WorkerContinuation.suspend(). In
     * that window the cont is still mounted on its registering carrier and
     * Continuation.run() rejects the remount with IllegalStateException. We spin
     * until the carrier unmounts -- typically nanoseconds. {@code cont.isDone()}
     * is the structural test (not message-text matching against an internal JDK
     * string): a not-done cont that refused to run can only be in the
     * mounted-elsewhere state. If isDone() is true, this is a real bug (cont
     * was put after completing) and the exception propagates.
     */
    public void run(WorkerContinuation cont) {
        int spins = 0;
        while (true) {
            try {
                cont.run();
                return;
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
        public WorkerContinuation cont;

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

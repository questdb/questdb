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
 * remounts each one on the calling worker thread. Any thread may call
 * {@link #enqueue(SqlContinuation)} to hand off a parked continuation to the worker
 * pool. The first worker to dequeue an item calls {@code cont.run()}, which copies
 * the parked SQL frames onto that worker's native stack and resumes execution past
 * the {@link SqlContinuation#suspend()} call.
 *
 * <p>The queue is MPMC, but a given continuation should be enqueued only once per
 * suspension (enforce this externally with a CAS on the waiter state). The queue
 * itself does not guard against double-enqueue of the same continuation reference.
 */
public final class ContinuationResumeJob implements Job {
    private final Queue<ResumeTask> queue = ConcurrentQueue.createConcurrentQueue(ResumeTask::new);
    private final ThreadLocal<ResumeTask> scratch = ThreadLocal.withInitial(ResumeTask::new);

    public void enqueue(SqlContinuation cont) {
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
        // A continuation can be enqueued by the producer (e.g. fireWaiters from within
        // registerWaiter) while its caller is still mounted and hasn't yet reached
        // SqlContinuation.suspend(). Continuation.run() rejects remounting with an
        // IllegalStateException ("Mounted!!!!") in that narrow window. Spin-wait for
        // the caller to yield; the window is typically nanoseconds. Yield the OS
        // scheduler after a short busy-spin to avoid pinning a core if the caller was
        // preempted right after enqueue.
        int spins = 0;
        while (true) {
            try {
                cont.run();
                return true;
            } catch (IllegalStateException e) {
                String msg = e.getMessage();
                if (msg == null || !msg.startsWith("Mounted")) {
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

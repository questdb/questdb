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

import io.questdb.Metrics;
import io.questdb.log.Log;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Worker extends Thread {
    public static final Clock CLOCK_MICROS = MicrosecondClockImpl.INSTANCE;
    public static final int NO_THREAD_AFFINITY = -1;
    private final int affinity;
    private final String criticalErrorLine;
    private final SOCountDownLatch haltLatch;
    private final boolean haltOnError;
    private final AtomicLong jobStartMicros = new AtomicLong();
    private final ObjHashSet<? extends Job> jobs;
    private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>(Lifecycle.BORN);
    private final Log log;
    private final Metrics metrics;
    private final long napThreshold;
    private final OnHaltAction onHaltAction;
    private final String poolName;
    // The pool's continuation queue. Used both as the sink for the worker's own
    // WorkerContinuation (so a yield from inside the loop body parks back into this
    // pool) and as the source of parked conts to remount in the outer driver.
    private final ContinuationQueue continuationQueue;
    private final Job.RunStatus runStatus = () -> lifecycle.get() == Lifecycle.HALTED;
    private final long sleepMs;
    private final long sleepThreshold;
    private final int workerId;
    private final long yieldThreshold;
    // Handoff slot: loopBody dequeues a parked cont from the queue and stores it
    // here, then suspends. The outer driver finds the handoff and remounts the
    // cont. Same-thread access only (body runs on this worker thread; outer
    // driver runs on this worker thread). No synchronization needed.
    private WorkerContinuation pendingResume;

    public Worker(
            String poolName,
            int workerId,
            int affinity,
            ObjHashSet<? extends Job> jobs,
            SOCountDownLatch haltLatch,
            @Nullable OnHaltAction onHaltAction,
            boolean haltOnError,
            long yieldThreshold,
            long napThreshold,
            long sleepThreshold,
            long sleepMs,
            Metrics metrics,
            ContinuationQueue continuationQueue,
            @Nullable Log log
    ) {
        assert yieldThreshold > 0L;
        this.setName(poolName + '_' + workerId);
        this.poolName = poolName;
        this.workerId = workerId;
        this.affinity = affinity;
        this.jobs = jobs;
        this.haltLatch = haltLatch;
        this.onHaltAction = onHaltAction;
        this.haltOnError = haltOnError;
        this.criticalErrorLine = "0000-00-00T00:00:00.000000Z C Unhandled exception in worker " + getName();
        this.yieldThreshold = yieldThreshold;
        this.napThreshold = napThreshold;
        this.sleepThreshold = sleepThreshold;
        this.sleepMs = sleepMs;
        this.metrics = metrics;
        this.continuationQueue = continuationQueue;
        this.log = log;
    }

    public String getPoolName() {
        return poolName;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void halt() {
        lifecycle.set(Lifecycle.HALTED);
    }

    @Override
    public void run() {
        Throwable ex = null;
        try {
            if (lifecycle.compareAndSet(Lifecycle.BORN, Lifecycle.RUNNING)) {

                String workerName = getName();

                // set affinity
                if (affinity > NO_THREAD_AFFINITY) {
                    if (Os.setCurrentThreadAffinity(affinity) == 0) {
                        if (log != null) {
                            log.info().$("affinity set [cpu=").$(affinity).$(", name=").$(workerName).I$();
                        }
                    } else {
                        if (log != null) {
                            log.error().$("could not set affinity [cpu=").$(affinity).$(", name=").$(workerName).I$();
                        }
                    }
                } else {
                    if (log != null) {
                        log.debug().$("os scheduled worker started [name=").$(workerName).I$();
                    }
                }

                // setup eager jobs
                for (int i = 0, n = jobs.size(); i < n; i++) {
                    Unsafe.loadFence();
                    try {
                        Job job = jobs.get(i);
                        if (job instanceof EagerThreadSetup) {
                            ((EagerThreadSetup) job).setup();
                        }
                    } finally {
                        Unsafe.storeFence();
                    }
                }

                // Outer driver: this thread is NOT carrying any cont here.
                //
                // Each iteration:
                //   1. Build and run a fresh worker-loop continuation.
                //   2. If the body handed off a parked cont via {@link #pendingResume},
                //      remount that cont here -- the body has dequeued it from the
                //      pool's queue and yielded specifically so we can run it.
                //
                // The fresh-per-iteration policy avoids racing peer workers that
                // might also try to remount a cont captured by a suspending function:
                // we never re-run our own cont reference, so even if the body's cont
                // got stashed in a TxnWaiter and pushed onto the queue, a peer worker
                // owns the remount, not us.
                while (lifecycle.get() == Lifecycle.RUNNING) {
                    WorkerContinuation workerContinuation = new WorkerContinuation(this::loopBody, continuationQueue);
                    workerContinuation.run();
                    if (workerContinuation.isDone()) {
                        // Body returned because lifecycle observed HALTED. Exit.
                        break;
                    }
                    // Body yielded. If it dequeued a parked cont before yielding,
                    // remount that cont here. Otherwise the cont was captured by
                    // a deep suspending function and the fresh next-iteration cont
                    // is enough.
                    if (pendingResume != null) {
                        WorkerContinuation toResume = pendingResume;
                        pendingResume = null;
                        continuationQueue.run(toResume);
                    }
                }
            }
        } catch (Throwable e) {
            ex = e;
            stdErrCritical(e);
        } finally {
            if (onHaltAction != null) {
                try {
                    onHaltAction.run(ex);
                    if (log != null) {
                        log.debug().$("cleaned worker [name=").$(poolName).$(", worker=").$(workerId).I$();
                    }
                } catch (Throwable t) {
                    stdErrCritical(t);
                }
            }
            haltLatch.countDown();
            if (log != null) {
                log.debug().$("os scheduled worker stopped [name=").$(getName()).I$();
            }
        }
    }

    /**
     * Body of the worker's continuation. Runs the job loop inside the cont so any
     * suspending function called by a job can yield the cont and free this carrier
     * thread to do other work. When the loop body's lifecycle observes HALTED, this
     * method returns and the cont becomes done.
     *
     * <p>Each iteration the body attempts to dequeue one parked cont from the
     * pool's queue. If it wins (tryDequeue returns non-null), it stashes the
     * dequeued cont in {@link #pendingResume} and yields, letting the outer
     * driver remount that cont. Workers that lose the dequeue race (null
     * return) keep running their own jobs without yielding -- only the winner
     * pays the yield cost per queue item.
     */
    private void loopBody() {
        ContinuationQueue.ResumeTask scratchResumeTask = new ContinuationQueue.ResumeTask();
        long ticker = 0L;
        while (lifecycle.get() == Lifecycle.RUNNING) {
            boolean runAsap = false;
            // measure latency of all jobs tick
            jobStartMicros.lazySet(CLOCK_MICROS.getTicks());
            for (int i = 0, n = jobs.size(); i < n; i++) {
                Unsafe.loadFence();
                try {
                    runAsap |= jobs.get(i).run(workerId, runStatus);
                } catch (Throwable e) {
                    if (metrics.isEnabled()) {
                        try {
                            metrics.healthMetrics().incrementUnhandledErrors();
                        } catch (Throwable t) {
                            stdErrCritical(t);
                        }
                    }
                    if (log != null) {
                        log.critical().$("unhandled error [job=").$(jobs.get(i).toString()).$(", ex=").$(e).I$();
                    } else {
                        stdErrCritical(e); // log regardless
                    }
                    if (haltOnError) {
                        throw e;
                    }
                } finally {
                    Unsafe.storeFence();
                }
            }

            // Attempt to claim a parked cont from the pool's queue. Only the
            // worker that wins the dequeue actually yields -- peer workers that
            // lose the race (tryDequeue returns null) keep running their own
            // jobs. ConcurrentQueue tryDequeue on an empty queue is a couple of
            // volatile reads, cheap enough to do every iteration without a hint.
            WorkerContinuation toResume = continuationQueue.tryDequeue(scratchResumeTask);
            if (toResume != null) {
                pendingResume = toResume;
                if (!WorkerContinuation.suspend()) {
                    // Yield refused (carrier pinned). We can't escape this cont
                    // to mount the dequeued one. Re-queue it so a peer worker
                    // can pick it up, and continue the loop.
                    pendingResume = null;
                    continuationQueue.put(toResume);
                }
                // If suspend yielded, body never returns here -- the outer
                // driver picks up pendingResume and runs it.
            }

            if (runAsap) {
                ticker = 0;
                continue;
            }
            if (++ticker < 0) {
                ticker = sleepThreshold + 1; // overflow
            }
            if (ticker > sleepThreshold) {
                Os.sleep(sleepMs);
            } else if (ticker > napThreshold) {
                Os.sleep(1);
            } else if (ticker > yieldThreshold) {
                Os.pause();
            }
        }
    }

    private void stdErrCritical(Throwable e) {
        System.err.println(criticalErrorLine);
        e.printStackTrace(System.err);
    }

    long getJobStartMicros() {
        return jobStartMicros.get();
    }

    private enum Lifecycle {
        BORN, RUNNING, HALTED
    }

    @FunctionalInterface
    public interface OnHaltAction {
        void run(Throwable ex);
    }
}

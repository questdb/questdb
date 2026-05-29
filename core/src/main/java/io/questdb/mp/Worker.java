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
import io.questdb.mp.continuation.ContinuationQueue;
import io.questdb.mp.continuation.WorkerContinuation;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Worker extends Thread {
    public static final Clock CLOCK_MICROS = MicrosecondClockImpl.INSTANCE;
    public static final int NO_THREAD_AFFINITY = -1;
    // Carrier-pinned suspends are throttled to one log line per worker per this
    // interval. Pinning typically clears within a job iteration, but if it
    // persists, periodic re-logging keeps the condition visible without flooding.
    private static final long YIELD_REFUSED_LOG_INTERVAL_MICROS = 2_000_000L;
    private final int affinity;
    // The pool's continuation queue. Used both as the sink for the worker's own
    // WorkerContinuation (so a yield from inside the loop body parks back into this
    // pool) and as the source of parked conts to remount in the outer driver.
    private final ContinuationQueue continuationQueue;
    private final String criticalErrorLine;
    private final SOCountDownLatch haltLatch;
    private final boolean haltOnError;
    private final AtomicLong jobStartMicros = new AtomicLong();
    private final ObjHashSet<? extends Job> jobs;
    private final AtomicReference<WorkerLifecycle> lifecycle = new AtomicReference<>(WorkerLifecycle.BORN);
    private final Log log;
    private final Metrics metrics;
    private final long napThreshold;
    private final OnHaltAction onHaltAction;
    private final ObjList<Job> ownedJobClones = new ObjList<>();
    private final String poolName;
    private final Job.RunStatus runStatus = () -> lifecycle.get() == WorkerLifecycle.HALTED;
    private final long sleepMs;
    private final long sleepThreshold;
    // Per-worker pool of recycled job-generation snapshots. Touched only by
    // this worker's outer driver, so no synchronization is needed. A snapshot
    // is pushed here by recycleSnapshot() after a cont becomes done and
    // popped by mintNextGen() on the next suspend. Pool size converges to the
    // workload's concurrent-suspend high-water mark; empty on cold start.
    private final ObjList<ObjList<Job>> snapshotPool = new ObjList<>();
    private final int workerId;
    private final long yieldThreshold;

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
            @Nullable ContinuationQueue continuationQueue,
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
        lifecycle.set(WorkerLifecycle.HALTED);
    }

    @Override
    public void run() {
        Throwable ex = null;
        try {
            if (lifecycle.compareAndSet(WorkerLifecycle.BORN, WorkerLifecycle.RUNNING)) {

                // Stamp this OS thread's identity into TLS that survives cont
                // freeze/thaw. CarrierLocal reads this on every access; without
                // it, a hoisted Thread.currentThread() would alias the holder
                // of whatever carrier first ran the cont's preheader. The id is
                // globally unique across pools - workerId is pool-local and
                // would collide between e.g. shared:0 and io:0.
                CarrierIdentity.bind();

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

                ObjList<Job> currentJobsGen = buildInitialGeneration(jobs);
                if (continuationQueue == null) {
                    // Legacy pool: no continuation wrapping. Workers iterate
                    // their job set directly, never park, never clone. workerId
                    // identity is stable for the worker's lifetime.
                    loopBody(currentJobsGen);
                    return;
                }

                // Outer driver: this thread is NOT carrying any cont here.
                //
                // currentJobsGen is the worker's current job generation: gen-0 is the
                // registered Job instances; subsequent generations are minted by
                // mintNextGen() when a body suspends mid-iteration. The captured
                // cont keeps its own snapshot via the lambda's effectively-final
                // capture and via WorkerContinuation.attachJobs(); the framework
                // calls recycleInstance() on each Job in the snapshot when the
                // cont completes.
                //
                // Each iteration:
                //   1. Build and run a fresh worker-loop continuation over the
                //      current generation.
                //   2. After every run() return, read the cont's handoff slot. The
                //      body writes the slot just before suspending if it dequeued
                //      a parked cont; we then remount that cont here. A remounted
                //      cont may itself dequeue and hand off another, so we walk
                //      the chain until a body suspends without a handoff (captured
                //      by a deep suspending function) or returns (worker halted).
                //
                // The fresh-per-iteration policy avoids racing peer workers that
                // might also try to remount a cont captured by a suspending function:
                // we never re-run our own cont reference, so even if the body's cont
                // got stashed in a TxnWaiter and pushed onto the queue, a peer worker
                // owns the remount, not us.
                //
                // The handoff slot lives on the WorkerContinuation rather than on
                // this Worker because a cont parked deep inside a suspending
                // function can be remounted on a peer carrier; the body's writes
                // to per-mount state must travel with the cont, not be aliased to
                // a specific Worker instance.
                while (lifecycle.get() == WorkerLifecycle.RUNNING) {
                    final ObjList<Job> jobsForThisCont = currentJobsGen;
                    WorkerContinuation cont = new WorkerContinuation(
                            () -> loopBody(jobsForThisCont),
                            continuationQueue
                    );
                    cont.attachJobs(jobsForThisCont);
                    cont.run();
                    if (cont.isDone()) {
                        // loopBody returned, which only happens when this worker
                        // observed lifecycle HALTED. Exit the pool.
                        recycleJobList(cont);
                        return;
                    }
                    // Walk the handoff chain. Each parked body, just before yielding,
                    // may have stashed a dequeued foreign cont in its handoff slot.
                    // Remount it here in the outer driver, since cont.run() requires
                    // the calling thread NOT to already be carrying a cont in the
                    // same scope. A foreign cont may itself dequeue and hand off
                    // another, so the chain length is unbounded; it terminates when
                    // a body parks without a handoff (captured deep inside a
                    // suspending function) or completes. A foreign-done end of chain
                    // is NOT an exit signal: tying worker exit to any done cont
                    // would deplete the pool whenever a stale or shutdown-race cont
                    // surfaces, and leave parked queries with no remounter. The
                    // authoritative exit signal is the outer while's lifecycle check.
                    while (true) {
                        WorkerContinuation handoff = cont.takeHandoff();
                        if (handoff == null) {
                            // Body parked deep inside a job (no dequeue this turn).
                            // Our cont retains the current generation in its snapshot;
                            // mint a fresh generation so the next outer iteration's
                            // cont runs on instances that share no state with the
                            // parked one. Stateless slots return the same singleton
                            // from cloneInstance(); stateful slots return fresh or
                            // pooled instances.
                            currentJobsGen = mintNextGen(currentJobsGen);
                            break;
                        }
                        cont = handoff;
                        mountForeignCont(cont);
                        if (cont.isDone()) {
                            recycleJobList(cont);
                            break;
                        }
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
            // Release the CarrierLocal row pinned to this thread's id so it
            // does not survive across engine restarts in long-running JVMs.
            // No-op if bind() was never reached (lifecycle CAS failed).
            CarrierIdentity.unbind();
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
     * dequeued cont in the currently mounted cont's handoff slot and yields,
     * letting whichever outer driver mounted us remount the dequeued cont.
     * Workers that lose the dequeue race (null return) keep running their own
     * jobs without yielding -- only the winner pays the yield cost per queue
     * item.
     *
     * <p>The handoff slot is read off {@link WorkerContinuation#current()}, not
     * a {@link Worker} field: a cont parked deep inside a suspending function
     * may be remounted on a peer carrier, and any per-mount writes need to
     * travel with the cont rather than be aliased to a specific Worker.
     */
    private void loopBody(ObjList<Job> myJobs) {
        // On legacy pools continuationQueue is null and no cont is mounted; the
        // dequeue/suspend block below is skipped. self stays null in that case.
        WorkerContinuation self = continuationQueue != null ? WorkerContinuation.current() : null;
        ContinuationQueue.ResumeTask scratchResumeTask = continuationQueue != null
                ? new ContinuationQueue.ResumeTask()
                : null;
        long ticker = 0L;
        // Throttle: log a refused yield at most once per
        // YIELD_REFUSED_LOG_INTERVAL_MICROS. A successful suspend resets the
        // window so a future pin episode logs immediately.
        long nextYieldRefusedLogMicros = 0L;
        while (lifecycle.get() == WorkerLifecycle.RUNNING) {
            boolean runAsap = false;
            // measure latency of all jobs tick
            jobStartMicros.lazySet(CLOCK_MICROS.getTicks());
            final int carrierId = CarrierIdentity.current();
            for (int i = 0, n = myJobs.size(); i < n; i++) {
                Unsafe.loadFence();
                try {
                    runAsap |= myJobs.get(i).run(carrierId, runStatus);
                } catch (Throwable e) {
                    if (metrics.isEnabled()) {
                        try {
                            metrics.healthMetrics().incrementUnhandledErrors();
                        } catch (Throwable t) {
                            stdErrCritical(t);
                        }
                    }
                    if (log != null) {
                        log.critical().$("unhandled error [job=").$(myJobs.get(i).toString()).$(", ex=").$(e).I$();
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
            // Skipped on legacy pools (continuationQueue == null).
            if (continuationQueue != null) {
                WorkerContinuation toResume = continuationQueue.tryDequeue(scratchResumeTask);
                if (toResume != null) {
                    self.setHandoff(toResume);
                    if (!WorkerContinuation.suspend()) {
                        // Yield refused (carrier pinned). We can't escape this cont
                        // to mount the dequeued one. Re-queue it so a peer worker
                        // can pick it up, and continue the loop.
                        long now = CLOCK_MICROS.getTicks();
                        if (now >= nextYieldRefusedLogMicros) {
                            nextYieldRefusedLogMicros = now + YIELD_REFUSED_LOG_INTERVAL_MICROS;
                            if (log != null) {
                                log.info().$("async yield to run continuation is refused (carrier pinned), re-queuing [worker=").$(getName()).I$();
                            } else {
                                StringSink sink = Misc.getThreadLocalSink();
                                MicrosFormatUtils.appendDateTimeUSec(sink, now);
                                sink.put(" I async yield to run continuation is refused (carrier pinned), re-queuing [worker=").put(getName()).put(']');
                                System.err.println(sink);
                            }
                        }
                        self.setHandoff(null);
                        continuationQueue.put(toResume);
                    } else {
                        // Resumed after a successful yield: reset the throttle so a
                        // future pin episode logs immediately rather than waiting
                        // for the residual window to expire.
                        nextYieldRefusedLogMicros = 0L;
                    }
                    // If suspend yielded, body never returns here directly --
                    // whichever outer driver mounted this cont takes the handoff
                    // and runs it; execution lands on the else branch above only
                    // when this cont is later remounted.
                }
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

    /**
     * Builds the initial job generation from the registered Job instances.
     * Gen-0 references the very same Job instances assigned to the pool; no
     * cloning happens on the hot path. Subsequent generations are minted via
     * {@link #mintNextGen(ObjList)} only when a suspend creates demand.
     */
    private static ObjList<Job> buildInitialGeneration(ObjHashSet<? extends Job> registered) {
        ObjList<Job> gen = new ObjList<>(registered.size());
        for (int i = 0, n = registered.size(); i < n; i++) {
            gen.add(registered.get(i));
        }
        return gen;
    }

    /**
     * Mints the next job generation. Prefers a snapshot recycled to this
     * worker's snapshot pool; on a cold pool, allocates a fresh generation by
     * calling {@link Job#cloneInstance()} on each entry of the current
     * generation. Stateless slots return the same singleton reference from
     * cloneInstance() (cheap); stateful slots return fresh instances that
     * share no mutable state with the captured cont's snapshot.
     */
    private ObjList<Job> mintNextGen(ObjList<Job> from) {
        if (snapshotPool.size() > 0) {
            ObjList<Job> recycled = snapshotPool.getLast();
            snapshotPool.remove(snapshotPool.size() - 1);
            return recycled;
        }
        ObjList<Job> next = new ObjList<>(from.size());
        for (int i = 0, n = from.size(); i < n; i++) {
            Job src = from.get(i);
            Job clone = src.cloneInstance();
            next.add(clone);
            // Track fresh clones (not the shared singleton returned by the
            // default cloneInstance()) so the pool can close their native
            // resources at halt -- the cont holding this clone may be abandoned
            // and never recycled.
            if (clone != src) {
                ownedJobClones.add(clone);
            }
        }
        return next;
    }

    /**
     * Remount a foreign cont on this thread. Three race shapes:
     * <ol>
     *   <li><b>Benign mount race</b> -- the cont was scheduleResume'd in the narrow
     *       window before its registering carrier reached suspend(); cont.run()
     *       throws ISE until that carrier unmounts (typically nanoseconds). Spin.</li>
     *   <li><b>Phantom resume</b> -- the body's suspend() returned false (carrier
     *       pinned) after a scheduleResume had already enqueued the cont; the cont
     *       stays mounted on its polling carrier. parkRefused is set by
     *       abortContinuation; we consume it and drop the dequeue.</li>
     *   <li><b>Already-done shutdown race</b> -- a TimerShards drain scheduleResumed
     *       a cont that has already completed via another path. cont.isDone() is
     *       the structural test.</li>
     * </ol>
     * Lifecycle bail-out: if this worker is halting, abandon the spin so the pool
     * can shut down. The cont stays mounted on its carrier; when that carrier
     * finishes its body the cont becomes done and is naturally disposed of.
     */
    private void mountForeignCont(WorkerContinuation cont) {
        if (cont.consumeParkRefused() || cont.isDone()) {
            return;
        }
        while (true) {
            try {
                cont.run();
                return;
            } catch (IllegalStateException e) {
                if (cont.isDone() || cont.consumeParkRefused()) {
                    return;
                }
                if (lifecycle.get() != WorkerLifecycle.RUNNING) {
                    return;
                }
                Os.pause();
            }
        }
    }

    /**
     * Returns the cont's attached job-generation snapshot to this worker's
     * snapshot pool for reuse on the next suspend. Calls
     * {@link Job#recycleInstance()} on each entry first to clear per-iteration
     * scratch. Called by the outer driver after a cont becomes done via the
     * primary {@code cont.run()} site or via {@link #mountForeignCont}.
     */
    private void recycleJobList(WorkerContinuation cont) {
        ObjList<Job> snapshot = cont.takeJobs();
        if (snapshot == null) {
            return;
        }
        for (int i = 0, n = snapshot.size(); i < n; i++) {
            try {
                snapshot.get(i).recycleInstance();
            } catch (Throwable ignore) {
                // contract: recycleInstance() must not throw
            }
        }
        snapshotPool.add(snapshot);
    }

    private void stdErrCritical(Throwable e) {
        System.err.println(criticalErrorLine);
        e.printStackTrace(System.err);
    }

    long getJobStartMicros() {
        return jobStartMicros.get();
    }

    ObjList<Job> getOwnedJobClones() {
        return ownedJobClones;
    }

    @FunctionalInterface
    public interface OnHaltAction {
        void run(Throwable ex);
    }
}

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
import io.questdb.std.CarrierLocal;
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
    // The current carrier's POOL-LOCAL worker id, published per carrier so that
    // cont-aware code can read it after a continuation migrates to a peer carrier.
    // Distinct from CarrierIdentity (the globally-unique carrier id used for
    // CarrierLocal row addressing): the pool-local id is what PerWorkerLocks expects,
    // since a globally-unique id collides mod slot count and breaks per-slot ordering.
    public static final CarrierLocal<Integer> WORKER_ID = CarrierLocal.withInitial(() -> -1);
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
    private final long sleepMs;
    private final long sleepThreshold;
    // Per-worker pool of recycled job-generation snapshots. Touched only by
    // this worker's outer driver, so no synchronization is needed. A snapshot
    // is pushed here by recycleJobList() once its cont is spent -- either the
    // cont became done (halt) or it handoff-suspended and was abandoned during
    // RUNNING -- and popped by mintNextGen() on the next suspend. Pool size
    // converges to the workload's concurrent-suspend high-water mark; empty on
    // cold start.
    private final ObjList<ObjList<Job>> snapshotPool = new ObjList<>();
    // The per-tick context handed to every job. carrierId() is pulled lazily by the
    // job: on a continuation-aware pool it reads WORKER_ID (carrier-aware, so it
    // tracks a remount onto a peer carrier); on a legacy pool, which never migrates,
    // it returns the stable workerId field and pays no carrier-local read. Folding
    // both signals into one instance keeps the hot loop free of a per-iteration
    // WORKER_ID.get() while still letting cont-aware jobs read the live id.
    private final Job.WorkerContext workerContext = new Job.WorkerContext() {
        @Override
        public int carrierId() {
            return continuationQueue != null ? WORKER_ID.get() : workerId;
        }

        @Override
        public boolean isTerminating() {
            return lifecycle.get() == WorkerLifecycle.HALTED;
        }
    };
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
                // Publish this carrier's pool-local worker id (bound for the carrier's
                // lifetime). Read it via WORKER_ID.get() wherever the pool-local id is
                // needed, instead of conflating it with the global CarrierIdentity.
                WORKER_ID.set(workerId);

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
                        // observed lifecycle HALTED. Break to drain any parked conts
                        // before exiting the pool.
                        recycleJobList(cont);
                        break;
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
                            // Our cont retains the current generation in its snapshot
                            // and will be resumed by a peer, so we must NOT recycle it
                            // here. The fresh generation for the next outer cont is
                            // minted after this loop.
                            break;
                        }
                        // cont suspended only to surrender the dequeued handoff cont;
                        // its handoff slot is non-null only after a successful handoff
                        // suspend (loopBody resets it to null on a refused one). Such a
                        // cont is never re-enqueued and never resumed -- it is spent --
                        // and it parked between job iterations, so its stateful clones
                        // (e.g. the HTTP selector + handler set) are idle. Recycle its
                        // generation now, during RUNNING, returning those clones to the
                        // pool instead of leaking them in ownedJobClones until halt.
                        recycleJobList(cont);
                        cont = handoff;
                        if (!mountForeignCont(cont)) {
                            // Dropped as a phantom or re-enqueued for the shutdown drain;
                            // the cont is no longer ours to inspect. Stop walking the chain.
                            break;
                        }
                        if (cont.isDone()) {
                            recycleJobList(cont);
                            break;
                        }
                    }
                    // The next outer cont always runs on a freshly minted generation.
                    // Either the deep-parked cont still owns currentJobsGen (cold path
                    // clones it), or the chain above recycled currentJobsGen into the
                    // snapshot pool (mintNextGen pops it back out, removing it from the
                    // pool so it is owned by nobody else). Either way the generation the
                    // next cont runs shares no live state with any parked or spent cont.
                    currentJobsGen = mintNextGen(currentJobsGen);
                }
                // Drain any continuations scheduled for resume after the RUNNING loop exited.
                drainShutdownContinuations();
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
     * Drains continuations scheduled for resume during shutdown, run after the worker's
     * RUNNING loop exits. Mirrors the outer driver's handoff walk: each parked cont is
     * remounted and run to completion so its body observes {@code isShuttingDown()} and
     * unwinds, releasing any checked-out resource (e.g. a connection context). A cont
     * scheduled in the narrow window before its registering carrier reached suspend() is
     * still transiently mounted there, so its remount throws and {@link #mountForeignCont}
     * re-enqueues it; this loop re-dequeues and retries until that carrier unmounts
     * (nanoseconds later), rather than stranding the cont off-queue with its resource
     * unreleased.
     */
    private void drainShutdownContinuations() {
        if (continuationQueue == null) {
            return;
        }
        final ContinuationQueue.ResumeTask scratch = new ContinuationQueue.ResumeTask();
        WorkerContinuation cont;
        while ((cont = continuationQueue.tryDequeue(scratch)) != null) {
            if (!mountForeignCont(cont)) {
                // Phantom resume (dropped), or re-enqueued because its registering carrier
                // still has it mounted. A re-enqueue lands back on this queue, so pause and
                // let the loop re-dequeue until the carrier unmounts -- bounded, since a cont
                // on the resume queue is one suspend() away from parking.
                Os.pause();
                continue;
            }
            while (!cont.isDone()) {
                WorkerContinuation handoff = cont.takeHandoff();
                if (handoff == null) {
                    // Re-parked deep without a handoff; nothing more to run here. In-tree
                    // suspending functions throw on isShuttingDown() rather than re-park, so
                    // this is not reached by them.
                    break;
                }
                recycleJobList(cont);
                cont = handoff;
                if (!mountForeignCont(cont)) {
                    cont = null;
                    break;
                }
            }
            if (cont != null && cont.isDone()) {
                recycleJobList(cont);
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
            for (int i = 0, n = myJobs.size(); i < n; i++) {
                Unsafe.loadFence();
                try {
                    runAsap |= myJobs.get(i).run(workerContext);
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
                    // suspend() only ever returns to this frame with the refused
                    // (false) outcome below. On a successful yield the carrier unmounts
                    // here and this cont is NOT resumed at this point: the outer driver
                    // reads the handoff (takeHandoff), recycles this now-spent cont's
                    // job generation, and mounts toResume in its place. A deep park
                    // inside a job (Job.run) resumes at the job call, not here. So there
                    // is no resumed-after-success branch. See DESIGN_NOTES.md, "Worker
                    // loop control flow: handoff suspend vs deep park".
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
                    }
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
     * Lifecycle handling: if this worker is halting, it does not keep spinning to
     * completion here -- a slow registering carrier could stall the pool's shutdown.
     * Instead it re-enqueues the cont so the shutdown drain
     * ({@link #drainShutdownContinuations}) retries it once that carrier unmounts,
     * which happens within nanoseconds since a cont on the resume queue is one
     * {@code suspend()} from parking. Returning the cont to the queue rather than
     * abandoning it keeps a cont caught in the benign-mount-race window from being
     * stranded off-queue with its checked-out resources unreleased.
     *
     * @return {@code true} if the cont ran on this carrier and is now parked or done,
     * so the caller owns it and may inspect its handoff slot or recycle it;
     * {@code false} if the cont was not run here and the caller must not touch it --
     * either a phantom resume still owned by its polling carrier, or a cont
     * re-enqueued for the shutdown drain to retry.
     */
    private boolean mountForeignCont(WorkerContinuation cont) {
        if (cont.consumeParkRefused()) {
            return false;
        }
        if (cont.isDone()) {
            return true;
        }
        while (true) {
            try {
                cont.run();
                return true;
            } catch (IllegalStateException e) {
                if (cont.isDone()) {
                    return true;
                }
                if (cont.consumeParkRefused()) {
                    return false;
                }
                if (lifecycle.get() != WorkerLifecycle.RUNNING) {
                    // Halting: hand the cont back to the queue so the drain retries it
                    // once its registering carrier unmounts, instead of stranding it.
                    continuationQueue.put(cont);
                    return false;
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

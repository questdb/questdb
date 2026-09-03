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
import io.questdb.cairo.O3PartitionJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.WorkerMetrics;
import io.questdb.mp.continuation.ContinuationQueue;
import io.questdb.mp.continuation.ContinuationSink;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class WorkerPool implements Closeable {
    // Generous backstop used by the unbounded halt() so a wedged worker cannot block shutdown forever.
    // Callers that want a tighter, shared budget across several pools pass an explicit timeout to halt(long).
    public static final long DEFAULT_HALT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final Log LOG = LogFactory.getLog(WorkerPool.class);
    @TestOnly
    private volatile Runnable afterClosedSignalForTesting;
    // Every Job instance the pool mints through assign() (blueprints and their
    // gen-0 clones). halt() closeInstance()s each one. closeInstance() is a
    // no-op default on caller-owned singletons and idempotent on recycled
    // clones, so the pool needs no blueprint-vs-clone bookkeeping to free them.
    private final ObjList<Job> assignedJobs = new ObjList<>();
    @TestOnly
    private volatile Runnable beforeStartedSignalForTesting;
    @TestOnly
    private volatile Runnable beforeWorkerAddedForTesting;
    private final AtomicBoolean closed = new AtomicBoolean();
    // Non-legacy pools own a ContinuationQueue. Workers drain it from their
    // outer driver between continuation mounts, NOT as a regular job. It cannot
    // be a regular job because resume requires the calling thread to not already
    // be carrying a cont in the same scope, which holds in the outer driver but
    // not inside a mounted worker-loop body. Null on legacy pools.
    private final ContinuationQueue continuationQueue;
    private final boolean daemons;
    private final ObjList<Job> freeOnExit = new ObjList<>();
    private final boolean haltOnError;
    private final SOCountDownLatch halted;
    private final ReentrantLock haltLock = new ReentrantLock();
    // Legacy pools run their loop body directly (no WorkerContinuation
    // wrapping) and accept per-worker Job assignment via
    // {@link #assign(int, Job)}. Used today for the ILP TCP IO and writer
    // Jobs which key per-worker state by workerId at construction time.
    private final boolean legacy;
    private final Metrics metrics;
    private final long napThreshold;
    private final String poolName;
    private final int priority;
    private boolean isHaltComplete;
    private boolean isStartAttempted;
    private final AtomicBoolean running = new AtomicBoolean();
    private final long sleepMs;
    private final long sleepThreshold;
    private final SOCountDownLatch started = new SOCountDownLatch(1);
    private final ObjList<ObjList<Closeable>> threadLocalCleaners;
    private final int[] workerAffinity;
    private final int workerCount;
    private final ObjList<ObjHashSet<Job>> workerJobs;
    private final ObjList<Worker> workers = new ObjList<>();
    // Guards every mutation of and iteration over the workers list so halt()'s first pass can never
    // read it torn while start() is still adding. ObjList.add reallocates a non-volatile buffer and
    // bumps a non-volatile pos; halt()'s first-pass size()/getQuick() are guarded only by an assert
    // (and -ea ships), so a concurrent halt() during boot could read a half-published pos/buffer or a
    // null slot -> NPE/AssertionError -> the error escapes halt()/close() and freeOnExit.close() is
    // skipped, leaking native handles. Building the list under this monitor makes a concurrent halt()
    // observe an empty-or-complete-and-consistent list, never torn.
    private final Object workersLock = new Object();
    private final long yieldThreshold;

    public WorkerPool(WorkerPoolConfiguration configuration) {
        this.workerCount = configuration.getWorkerCount();
        int[] workerAffinity = configuration.getWorkerAffinity();
        if (workerAffinity != null && workerAffinity.length > 0) {
            this.workerAffinity = workerAffinity;
        } else {
            this.workerAffinity = Misc.getWorkerAffinity(workerCount);
        }
        this.halted = new SOCountDownLatch(workerCount);
        this.haltOnError = configuration.haltOnError();
        this.daemons = configuration.isDaemonPool();
        this.legacy = configuration.isLegacy();
        this.poolName = configuration.getPoolName();
        this.yieldThreshold = configuration.getYieldThreshold();
        this.napThreshold = configuration.getNapThreshold();
        this.sleepThreshold = configuration.getSleepThreshold();
        this.sleepMs = configuration.getSleepTimeout();
        this.metrics = configuration.getMetrics();
        this.priority = configuration.workerPoolPriority();

        assert this.workerAffinity.length == workerCount;

        this.workerJobs = new ObjList<>(workerCount);
        this.threadLocalCleaners = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            workerJobs.add(new ObjHashSet<>());
            threadLocalCleaners.add(new ObjList<>());
        }

        // Legacy pools skip the continuation queue entirely; workers do not
        // wrap their loop body and no peer-cont remount path exists.
        this.continuationQueue = legacy ? null : new ContinuationQueue();
        // NOT assigned via assign(): drained by worker outer driver instead.
    }

    /**
     * Assigns job instance to all workers. Job member variables
     * could be accessed by multiple threads at the same time. Jobs cannot
     * be added after pool is started.
     *
     * @param job instance of job
     */
    public void assign(Job job) {
        assert !running.get() && !closed.get();

        // The blueprint is closeInstance()d at halt; with zero workers it is
        // never cloned, so this is also what frees its construction resources.
        assignedJobs.add(job);
        for (int i = 0; i < workerCount; i++) {
            Job clone = i == 0 ? job : job.cloneInstance();
            workerJobs.getQuick(i).add(clone);
            // A stateful Job mints a fresh clone per worker; a stateless one
            // returns the same singleton. Track only the fresh clones -- the
            // singleton is already tracked above and closeInstance() is a no-op
            // on it anyway.
            if (clone != job) {
                assignedJobs.add(clone);
            }
        }
    }

    /**
     * Assigns a specific Job instance to a specific worker. Preferred on
     * legacy pools (where workerId is stable identity). Permitted on
     * non-legacy pools when the caller already constructs per-worker Job
     * instances (e.g., HttpServer's per-worker selectors): per-worker state
     * survives cont rotation because the captured frame holds a stable
     * reference, and any state-sharing concerns are the caller's to manage.
     */
    public void assign(int worker, Job job) {
        assert worker > -1 && worker < workerCount && !running.get() && !closed.get();
        workerJobs.getQuick(worker).add(job);
    }

    public void assignThreadLocalCleaner(int worker, Closeable cleaner) {
        assert worker > -1 && worker < workerCount && !running.get() && !closed.get();
        threadLocalCleaners.getQuick(worker).add(cleaner);
    }

    @Override
    public void close() {
        halt();
    }

    public void freeOnExit(Job job) {
        assert !running.get() && !closed.get();
        freeOnExit.add(job);
    }

    /**
     * Returns the {@link ContinuationSink} for this pool. Continuations constructed
     * with this sink will resume on workers of this pool. Non-null on non-legacy
     * pools; throws on legacy pools, which do not run continuations.
     */
    public ContinuationSink getContinuationSink() {
        if (legacy) {
            throw new IllegalStateException("legacy worker pool does not host continuations");
        }
        return continuationQueue;
    }

    public String getPoolName() {
        return poolName;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void halt() {
        halt0(false, 0, false);
    }

    /**
     * Halts the pool, bounding how long it blocks waiting for worker threads.
     * <p>
     * The unbounded variant of this wait could block the caller forever: if a worker is wedged
     * (GC-starvation, a stuck native job) it never reaches halted.countDown(), so a plain
     * halted.await() in the close path made server shutdown unkillable under SIGTERM. This variant
     * waits at most timeoutNanos for started/halted and then returns without freeing resources
     * that a live worker may still access. A later halt attempt resumes the same shutdown.
     * <p>
     * This legacy void overload does not report whether the halt completed. Call
     * {@link #haltWithin(long)} when the caller must condition later teardown on completion.
     * <p>
     * Footgun: this overload takes a RELATIVE timeout (a nanosecond duration measured from now),
     * but {@link io.questdb.WorkerPoolManager#halt(long)} has the identical {@code (long)} signature
     * and takes an ABSOLUTE deadline (a {@link System#nanoTime()} value). The two cannot be used
     * interchangeably: passing this method an absolute nanoTime would wait for a duration roughly
     * equal to the system's uptime, and passing WorkerPoolManager a small relative value would make
     * its deadline already in the past. Read the parameter name before calling either one.
     *
     * @param timeoutNanos upper bound on the combined wait for started and halted, a RELATIVE
     *                     duration in nanoseconds measured from the call (NOT an absolute
     *                     {@link System#nanoTime()} deadline, unlike {@link io.questdb.WorkerPoolManager#halt(long)})
     * @deprecated use {@link #haltWithin(long)} and inspect its completion result
     */
    @Deprecated
    public void halt(long timeoutNanos) {
        haltWithin(timeoutNanos);
    }

    /**
     * Attempts to halt using an absolute {@link System#nanoTime()} deadline. A timeout retains all
     * worker-owned resources so a later attempt can retry safely.
     */
    public boolean haltBy(long deadlineNanos) {
        return halt0(true, deadlineNanos, false);
    }

    /**
     * Attempts to halt using a relative nanosecond timeout and reports completion.
     */
    public boolean haltWithin(long timeoutNanos) {
        return halt0(true, System.nanoTime() + Math.max(0, timeoutNanos), false);
    }

    @TestOnly
    public void haltAndAssertCleanForTest(long timeoutNanos) {
        halt0(true, System.nanoTime() + Math.max(0, timeoutNanos), true);
    }

    private boolean halt0(boolean isBounded, long deadlineNanos, boolean isStrict) {
        final long timeoutNanos = isBounded ? Math.max(0, deadlineNanos - System.nanoTime()) : 0;
        boolean isInterrupted = false;
        if (isBounded) {
            boolean isLocked = haltLock.tryLock();
            while (!isLocked) {
                final long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    if (isInterrupted) {
                        Thread.currentThread().interrupt();
                    }
                    return false;
                }
                try {
                    isLocked = haltLock.tryLock(remainingNanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    isInterrupted = true;
                }
            }
        } else {
            haltLock.lock();
        }
        try {
            if (isHaltComplete) {
                return true;
            }
            if (closed.compareAndSet(false, true)) {
                final Runnable afterClosed = afterClosedSignalForTesting;
                if (afterClosed != null) {
                    afterClosed.run();
                }
            }
            if (running.compareAndSet(true, false)) {
                isStartAttempted = true;
            }
            if (isStartAttempted) {
                synchronized (workersLock) {
                    for (int i = 0, n = workers.size(); i < n; i++) {
                        workers.getQuick(i).halt();
                    }
                }
                if (!awaitLatch(started, isBounded, deadlineNanos)) {
                    if (isStrict) {
                        throw workerPoolHaltTimeout(timeoutNanos, false);
                    }
                    return false;
                }
                synchronized (workersLock) {
                    for (int i = 0, n = workers.size(); i < n; i++) {
                        workers.getQuick(i).halt();
                    }
                }
                if (!awaitLatch(halted, isBounded, deadlineNanos)) {
                    if (isStrict) {
                        throw workerPoolHaltTimeout(timeoutNanos, true);
                    }
                    return false;
                }
            }

            // No worker can reach the owned object graph after both latches complete.
            closeInstances(assignedJobs);
            synchronized (workersLock) {
                for (int i = 0, n = workers.size(); i < n; i++) {
                    closeInstances(workers.getQuick(i).getOwnedJobClones());
                }
                workers.clear();
            }
            Misc.freeObjListIfCloseable(freeOnExit);
            isHaltComplete = true;
            return true;
        } finally {
            haltLock.unlock();
            if (isInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean awaitLatch(SOCountDownLatch latch, boolean isBounded, long deadlineNanos) {
        if (!isBounded) {
            latch.await();
            return true;
        }
        return latch.await(Math.max(0, deadlineNanos - System.nanoTime()));
    }

    @TestOnly
    public void pause() {
        if (running.compareAndSet(true, false)) {
            started.await();
            for (int i = 0; i < workerCount; i++) {
                workers.getQuick(i).halt();
            }
            halted.await();
        }
        synchronized (workersLock) {
            workers.clear();
        }
    }

    /**
     * Installs a hook fired immediately after {@link #halt(long)} flips {@code closed}.
     * Tests use it to prove a concurrent {@link #start(Log)} observes the close before
     * the parked add-loop resumes. Pass {@code null} to clear.
     */
    @TestOnly
    public void setAfterClosedSignalForTesting(Runnable hook) {
        this.afterClosedSignalForTesting = hook;
    }

    /**
     * Installs a hook fired inside {@link #start(Log)} after the worker threads are spawned and
     * running but BEFORE {@code started.countDown()}. A test uses it to reproduce a start() that
     * stalls in that window (realistic on an OOM mid-launch): the hook blocks or throws, leaving
     * {@code started} un-counted while the workers loop, so a concurrent {@link #halt(long)} takes
     * the start-latch-timeout branch. Pass {@code null} to clear.
     */
    @TestOnly
    public void setBeforeStartedSignalForTesting(Runnable hook) {
        this.beforeStartedSignalForTesting = hook;
    }

    /**
     * Installs a hook fired inside {@link #start(Log)} on every iteration of the spawn loop, WHILE
     * the workersLock is held for that worker's add. Unlike {@link #setBeforeStartedSignalForTesting(Runnable)},
     * which fires AFTER the whole add-loop has completed (outside the monitor), this hook fires in the
     * middle of the add-loop with the monitor held: a test can block here to hold the add critical
     * section open and prove that a concurrent {@link #halt(long)} first pass is held off (serialized)
     * rather than reading the half-built list torn. Pass {@code null} to clear.
     */
    @TestOnly
    public void setBeforeWorkerAddedForTesting(Runnable hook) {
        this.beforeWorkerAddedForTesting = hook;
    }

    public void start() {
        start(null);
    }

    public void start(@Nullable Log log) {
        if (!closed.get() && running.compareAndSet(false, true)) {
            int startedWorkerCount = 0;
            try {
                // very common cleaner
                // it is set up from start() to make sure it is called last
                // some other thread local cleaners are liable to access thread local Path instances
                setupPathCleaner();

                for (int i = 0; i < workerCount; i++) {
                    final int index = i;
                    Worker worker = new Worker(
                            poolName,
                            i,
                            workerAffinity[i],
                            workerJobs.getQuick(i),
                            halted,
                            _ -> Misc.freeObjListAndClear(threadLocalCleaners.getQuick(index)),
                            haltOnError,
                            yieldThreshold,
                            napThreshold,
                            sleepThreshold,
                            sleepMs,
                            metrics,
                            continuationQueue,
                            log
                    );
                    worker.setPriority(priority);
                    worker.setDaemon(daemons);
                    // Add + spawn under workersLock so a concurrent halt() first pass never reads the list
                    // torn (ObjList.add mutates a non-volatile pos/buffer). The worker is spawned inside the
                    // monitor too, so halt() either has not yet seen this worker (it is not spawned) or sees
                    // it fully published -- never a spawned-but-invisible worker that would loop on freed
                    // resources.
                    synchronized (workersLock) {
                        // Fire the test seam INSIDE the monitor so a test can hold the add critical section
                        // open and prove a concurrent halt() first pass is held off (serialized), never
                        // reading a half-built list. The seam is a strict no-op when unset.
                        final Runnable beforeWorkerAdded = beforeWorkerAddedForTesting;
                        if (beforeWorkerAdded != null) {
                            beforeWorkerAdded.run();
                        }
                        // Re-check closed inside the critical section, before spawning. A concurrent
                        // halt(long) sets closed and frees freeOnExit under this same monitor; if the seam
                        // (or a real OOM-stalled launch) held the add open while halt() ran, freeOnExit is
                        // already gone by the time this loop resumes. Spawning a worker now would loop it on
                        // freed resources -- a use-after-free plus an orphan thread. Break instead: the
                        // workers added so far will be halt-signalled once the add critical section releases,
                        // and started.countDown() below still runs so a waiting halt() proceeds.
                        if (closed.get()) {
                            break;
                        }
                        workers.add(worker);
                        worker.start();
                        startedWorkerCount++;
                    }
                }
                if (log != null) {
                    log.debug().$("worker pool started [pool=").$(poolName).I$();
                }
                final Runnable beforeStarted = beforeStartedSignalForTesting;
                if (beforeStarted != null) {
                    beforeStarted.run();
                }
            } finally {
                countDownUnstartedWorkers(startedWorkerCount);
                started.countDown();
            }
        }
    }

    public void updateWorkerMetrics(long now) {
        WorkerMetrics workerMetrics = metrics.workerMetrics();
        long min = workerMetrics.getMinElapsedMicros();
        long max = workerMetrics.getMaxElapsedMicros();
        // Iterate the workers list under the monitor: the /metrics scrape calls this concurrently
        // with start()'s add-loop and halt()'s clear(). Without the guard a torn read returns a null
        // slot (NPE on getQuick(i).getJobStartMicros()) or a half-published pos/buffer.
        synchronized (workersLock) {
            for (int i = 0, n = workers.size(); i < n; i++) {
                long elapsed = now - workers.getQuick(i).getJobStartMicros();
                if (elapsed > 0) {
                    min = Math.min(min, elapsed);
                    max = Math.max(max, elapsed);
                }
            }
        }
        workerMetrics.update(min, max);
    }

    private static void closeInstances(ObjList<Job> jobs) {
        for (int i = 0, n = jobs.size(); i < n; i++) {
            try {
                jobs.getQuick(i).closeInstance();
            } catch (Throwable ignore) {
                // contract: Job.closeInstance() must not throw
            }
        }
    }

    private void countDownUnstartedWorkers(int firstUnstartedWorker) {
        for (int i = firstUnstartedWorker; i < workerCount; i++) {
            halted.countDown();
        }
    }

    private AssertionError workerPoolHaltTimeout(long timeoutNanos, boolean startCompleted) {
        return new AssertionError(
                "WorkerPool timed out waiting for workers to halt before leak-sensitive test cleanup [pool="
                        + poolName
                        + ", timeoutMs=" + (timeoutNanos / 1_000_000)
                        + ", startCompleted=" + startCompleted
                        + ", remainingHalted=" + halted.getCount()
                        + ']'
        );
    }

    private void setupPathCleaner() {
        for (int i = 0; i < workerCount; i++) {
            ObjList<Closeable> workerCleaners = threadLocalCleaners.getQuick(i);
            workerCleaners.add(Path.THREAD_LOCAL_CLEANER);
            workerCleaners.add(O3PartitionJob.THREAD_LOCAL_CLEANER);
        }
    }
}

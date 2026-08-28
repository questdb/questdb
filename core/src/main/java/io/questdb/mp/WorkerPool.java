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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.WorkerMetrics;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class WorkerPool implements Closeable {
    // Default budget for explicitly bounded shutdown paths such as the JVM shutdown hook.
    public static final long DEFAULT_HALT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final Log LOG = LogFactory.getLog(WorkerPool.class);
    // Every Job instance the pool mints through assign() (the blueprint and its
    // per-worker clones). halt() closeInstance()s each one. closeInstance() is
    // a no-op default on caller-owned singletons, so the pool needs no
    // blueprint-vs-clone bookkeeping to free them.
    private final ObjList<Job> assignedJobs = new ObjList<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final boolean daemons;
    private final DynamicFiberWorkerPoolConfiguration dynamicFiberConfiguration;
    private final FiberRuntime fiberRuntime;
    private final ObjList<Object> freeOnExit = new ObjList<>();
    private final ReentrantLock haltLock = new ReentrantLock();
    private final boolean haltOnError;
    private final SOCountDownLatch halted;
    private final Metrics metrics;
    private final WorkerPoolMode mode;
    private final long napThreshold;
    private final String poolName;
    private final int priority;
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
    @TestOnly
    private volatile Runnable afterClosedSignalForTesting;
    @TestOnly
    private volatile Runnable beforeStartedSignalForTesting;
    @TestOnly
    private volatile Runnable beforeWorkerAddedForTesting;
    private boolean isHaltComplete;
    private volatile boolean isStartAttempted;

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
        this.mode = configuration.getWorkerPoolMode();
        if (mode == null) {
            throw new IllegalArgumentException("worker pool mode is required [pool=" + configuration.getPoolName() + ']');
        }
        final boolean isFiberHost = mode == WorkerPoolMode.FIBER_HOST;
        this.dynamicFiberConfiguration = isFiberHost
                && configuration instanceof DynamicFiberWorkerPoolConfiguration dynamicConfiguration
                ? dynamicConfiguration
                : null;
        this.poolName = configuration.getPoolName();
        this.yieldThreshold = configuration.getYieldThreshold();
        this.napThreshold = configuration.getNapThreshold();
        this.sleepThreshold = configuration.getSleepThreshold();
        this.sleepMs = configuration.getSleepTimeout();
        this.metrics = configuration.getMetrics();
        this.priority = configuration.workerPoolPriority();
        final int fiberMaxLiveCount;
        final int fiberMountBudget;
        final int fiberRetainedCount;
        if (dynamicFiberConfiguration != null) {
            final DynamicFiberWorkerPoolConfiguration.FiberConfiguration fiberConfiguration =
                    dynamicFiberConfiguration.getFiberConfiguration();
            fiberMaxLiveCount = fiberConfiguration.maxLiveCount();
            fiberMountBudget = fiberConfiguration.mountBudget();
            fiberRetainedCount = fiberConfiguration.retainedCount();
        } else {
            fiberMaxLiveCount = isFiberHost ? configuration.getFiberMaxLiveCount() : 0;
            fiberMountBudget = isFiberHost ? configuration.getFiberMountBudget() : 1;
            fiberRetainedCount = isFiberHost ? configuration.getFiberRetainedCount() : 0;
        }
        if (fiberMountBudget < 1) {
            throw new IllegalArgumentException("fiber mount budget must be positive [pool=" + poolName + ']');
        }

        assert this.workerAffinity.length == workerCount;

        this.workerJobs = new ObjList<>(workerCount);
        this.threadLocalCleaners = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            workerJobs.add(new ObjHashSet<>());
            threadLocalCleaners.add(new ObjList<>());
        }

        this.fiberRuntime = isFiberHost
                ? new FiberRuntime(
                fiberRetainedCount,
                fiberMaxLiveCount,
                fiberMountBudget
        )
                : null;
        if (fiberRuntime != null) {
            try {
                if (dynamicFiberConfiguration != null) {
                    dynamicFiberConfiguration.setFiberConfigurationListener(fiberRuntime::updateConfiguration);
                }
                metrics.fiberMetrics().register(poolName, fiberRuntime);
            } catch (Throwable th) {
                rollbackFiberRuntimeConstruction(th);
                throw th;
            }
        }
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
        trackOwnedJob(job);
        for (int i = 0; i < workerCount; i++) {
            Job clone = i == 0 ? job : job.cloneInstance();
            // A stateful Job mints a fresh clone per worker; a stateless one
            // returns the same singleton. Track only the fresh clones -- the
            // singleton is already tracked above and closeInstance() is a no-op
            // on it anyway.
            if (clone != job) {
                trackOwnedJob(clone);
            }
            workerJobs.getQuick(i).add(clone);
        }
    }

    /**
     * Assigns a specific Job instance to a specific worker. Preferred on
     * pools whose caller constructs per-worker Job instances.
     */
    public void assign(int worker, Job job) {
        assert worker > -1 && worker < workerCount && !running.get() && !closed.get();
        workerJobs.getQuick(worker).add(job);
    }

    public void assignThreadLocalCleaner(int worker, Closeable cleaner) {
        assert worker > -1 && worker < workerCount && !running.get() && !closed.get();
        threadLocalCleaners.getQuick(worker).add(cleaner);
    }

    /**
     * Closes the pool by waiting without a deadline for all workers and hosted fibers to stop,
     * then releases the pool-owned object graph. This terminal operation never releases resources
     * while a live worker or fiber may still access them. Use {@link #haltWithin(long)} when the
     * caller needs a retryable bounded wait.
     */
    @Override
    public void close() {
        halt();
    }

    public void freeOnExit(Job job) {
        assert !running.get() && !closed.get();
        try {
            freeOnExit.add(job);
        } catch (Throwable th) {
            if (job instanceof Closeable closeable) {
                Misc.free(closeable, th);
            }
            throw th;
        }
    }

    public void freeResourceOnExit(Closeable resource) {
        assert !running.get() && !closed.get();
        try {
            freeOnExit.add(resource);
        } catch (Throwable th) {
            Misc.free(resource, th);
            throw th;
        }
    }

    public int getFiberMaxLiveCount() {
        return fiberRuntime != null ? fiberRuntime.getMaxLiveFiberCount() : 0;
    }

    public int getFiberMountBudget() {
        return fiberRuntime != null ? fiberRuntime.getMountBudget() : 1;
    }

    public int getFiberRetainedCount() {
        return fiberRuntime != null ? fiberRuntime.getMaxRetainedFiberCount() : 0;
    }

    public FiberRuntime getFiberRuntime() {
        if (fiberRuntime == null) {
            throw new IllegalStateException("worker pool does not host fibers [pool=" + poolName + ']');
        }
        return fiberRuntime;
    }

    public String getPoolName() {
        return poolName;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public WorkerPoolMode getWorkerPoolMode() {
        return mode;
    }

    /**
     * Halts the pool, waiting without a deadline for all workers and hosted fibers to stop before
     * releasing the pool-owned object graph. Use {@link #haltWithin(long)} for a relative wait
     * budget.
     */
    public void halt() {
        isHaltComplete(false, 0, false);
    }

    /**
     * @deprecated use {@link #haltWithin(long)} and inspect its completion result
     */
    @Deprecated
    public void halt(long timeoutNanos) {
        haltWithin(timeoutNanos);
    }

    /**
     * Attempts to halt workers and hosted fibers using one absolute {@link System#nanoTime()}
     * deadline. A timeout retains the live pool-owned object graph for a later retry.
     */
    public boolean haltBy(long deadlineNanos) {
        return isHaltComplete(true, deadlineNanos, false);
    }

    @TestOnly
    public void haltAndAssertCleanForTest(long timeoutNanos) {
        isHaltComplete(true, System.nanoTime() + Math.max(0, timeoutNanos), true);
    }

    /**
     * Halts the pool with a relative nanosecond budget for shutdown waits. When a wait exhausts
     * the budget, shutdown has begun but the pool retains resources that a live worker or fiber
     * may still access. The caller may retry with another budget. The budget covers lock, runtime,
     * start, and worker-halt waits; it does not bound logging or cleanup after everything stops.
     *
     * @param timeoutNanos relative shutdown-wait budget in nanoseconds
     * @return true when the pool released all owned resources, false when it retained its live
     * object graph after the deadline
     */
    public boolean haltWithin(long timeoutNanos) {
        return isHaltComplete(true, System.nanoTime() + Math.max(0, timeoutNanos), false);
    }

    public boolean isFiberHost() {
        return mode == WorkerPoolMode.FIBER_HOST;
    }

    @TestOnly
    public boolean isHaltTerminalSuccessfulForTesting(long timeoutNanos) {
        return isHaltComplete(true, System.nanoTime() + Math.max(0, timeoutNanos), false);
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
     * Installs a hook fired immediately after {@link #haltWithin(long)} flips {@code closed}.
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
     * {@code started} un-counted while the workers loop, so a concurrent {@link #haltWithin(long)} takes
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
     * section open and prove that a concurrent {@link #haltWithin(long)} first pass is held off (serialized)
     * rather than reading the half-built list torn. Pass {@code null} to clear.
     */
    @TestOnly
    public void setBeforeWorkerAddedForTesting(Runnable hook) {
        this.beforeWorkerAddedForTesting = hook;
    }

    public void start() {
        start(LOG);
    }

    public void start(@Nullable Log log) {
        if (!closed.get() && running.compareAndSet(false, true)) {
            isStartAttempted = true;
            int spawnedWorkerCount = 0;
            try {
                if (log != null) {
                    log.info().$("worker pool configured [pool=").$(poolName)
                            .$(", workers=").$(workerCount)
                            .$(", mode=").$(mode.name()).I$();
                }
                if (log != null && fiberRuntime != null) {
                    log.info().$("fiber-host worker pool configured [pool=").$(poolName)
                            .$(", workers=").$(workerCount)
                            .$(", maxLive=").$(fiberRuntime.getMaxLiveFiberCount())
                            .$(", maxRetained=").$(fiberRuntime.getMaxRetainedFiberCount())
                            .$(", mountBudget=").$(fiberRuntime.getMountBudget())
                            .I$();
                }
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
                            fiberRuntime,
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
                        // haltWithin(long) sets closed and frees freeOnExit under this same monitor; if the seam
                        // (or a real OOM-stalled launch) held the add open while halt() ran, freeOnExit is
                        // already gone by the time this loop resumes. Spawning a worker now would loop it on
                        // freed resources -- a use-after-free plus an orphan thread. Break instead: the
                        // workers added so far will be halt-signalled once the add critical section releases,
                        // and started.countDown() below still runs so a waiting halt() proceeds.
                        if (closed.get()) {
                            break;
                        }
                        workers.add(worker);
                        try {
                            worker.start();
                            spawnedWorkerCount++;
                        } catch (Throwable th) {
                            workers.popLast();
                            throw th;
                        }
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
                countDownUnstartedWorkers(spawnedWorkerCount);
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

    private static Throwable addCleanupFailure(@Nullable Throwable primary, Throwable failure) {
        if (primary == null) {
            return failure;
        }
        if (primary != failure) {
            primary.addSuppressed(failure);
        }
        return primary;
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

    private static long remaining(long deadline) {
        // Never hand SOCountDownLatch.await() a non-positive budget; parkNanos(<=0) returns
        // immediately, which is the intended behaviour once the overall deadline has passed.
        return Math.max(1, deadline - System.nanoTime());
    }

    // Polls at the same cadence SOCountDownLatch.await() parks at, so a lost unpark still
    // recovers within one park interval rather than one stall-log interval.
    private void awaitHalt(SOCountDownLatch latch, String stage) {
        final long startNanos = System.nanoTime();
        long nextStallLogNanos = startNanos + DEFAULT_HALT_TIMEOUT_NANOS;
        while (!latch.await(Os.PARK_NANOS_MAX)) {
            if (System.nanoTime() - nextStallLogNanos >= 0) {
                logHaltStall(stage, startNanos);
                nextStallLogNanos += DEFAULT_HALT_TIMEOUT_NANOS;
            }
        }
    }

    private void countDownUnstartedWorkers(int firstUnstartedWorker) {
        for (int i = firstUnstartedWorker; i < workerCount; i++) {
            halted.countDown();
        }
    }

    private AssertionError fiberRuntimeHaltTimeout(long timeoutNanos, FiberRuntime runtime) {
        return new AssertionError(
                "WorkerPool timed out waiting for fiber runtime to drain before leak-sensitive test cleanup [pool="
                        + poolName
                        + ", timeoutMs=" + (timeoutNanos / 1_000_000)
                        + ", state=" + runtime.state()
                        + ", outstanding=" + runtime.getOutstandingTaskCount()
                        + ", queued=" + runtime.getQueuedCount()
                        + ", mounted=" + runtime.getMountedCount()
                        + ", parked=" + runtime.getParkedFiberCount()
                        + ", live=" + runtime.getLiveFiberCount()
                        + ", retained=" + runtime.getRetainedFiberCount()
                        + ", finalizing=" + runtime.getFinalizerCount()
                        + ", budgetExhaustions=" + runtime.getBudgetExhaustionCount()
                        + ']'
        );
    }

    private boolean isHaltComplete(boolean isBounded, long deadlineNanos, boolean isStrict) {
        final long timeoutNanos = isBounded ? Math.max(0, deadlineNanos - System.nanoTime()) : 0;
        boolean isInterrupted = false;
        if (isBounded) {
            boolean isLockAcquired = haltLock.tryLock();
            while (!isLockAcquired) {
                final long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    if (isInterrupted) {
                        Thread.currentThread().interrupt();
                    }
                    if (isStrict) {
                        throw workerPoolHaltLockTimeout(timeoutNanos);
                    }
                    return false;
                }
                try {
                    isLockAcquired = haltLock.tryLock(remainingNanos, TimeUnit.NANOSECONDS);
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
            final boolean isRunning = running.compareAndSet(true, false);
            if (isRunning) {
                isStartAttempted = true;
            }
            final FiberRuntime runtime = fiberRuntime;
            boolean isRuntimeDrained = true;
            AssertionError runtimeHaltFailure = null;
            if (runtime != null) {
                runtime.beginQuiesce();
                if (isBounded && !runtime.awaitClosed(deadlineNanos)) {
                    isRuntimeDrained = false;
                    if (isStrict) {
                        runtimeHaltFailure = fiberRuntimeHaltTimeout(timeoutNanos, runtime);
                    } else {
                        try {
                            LOG.error().$("timed out waiting for fiber runtime to drain; retaining live pool resources [pool=").$(poolName)
                                    .$(", timeout=").$(timeoutNanos / 1_000_000).$("ms")
                                    .$(", state=").$(runtime.state().name())
                                    .$(", outstanding=").$(runtime.getOutstandingTaskCount())
                                    .$(", queued=").$(runtime.getQueuedCount())
                                    .$(", mounted=").$(runtime.getMountedCount())
                                    .$(", parked=").$(runtime.getParkedFiberCount())
                                    .$(", live=").$(runtime.getLiveFiberCount())
                                    .$(", retained=").$(runtime.getRetainedFiberCount())
                                    .$(", finalizing=").$(runtime.getFinalizerCount())
                                    .$(", budgetExhaustions=").$(runtime.getBudgetExhaustionCount()).I$();
                        } catch (Throwable ignore) {
                        }
                    }
                } else if (!isBounded) {
                    // the halting thread drains so closure does not depend on a live worker
                    final long drainStartNanos = System.nanoTime();
                    long nextStallLogNanos = drainStartNanos + DEFAULT_HALT_TIMEOUT_NANOS;
                    boolean hasDrainedOnHaltingThread = false;
                    while (!runtime.awaitClosed(System.nanoTime() + 1_000_000L)) {
                        hasDrainedOnHaltingThread |= runtime.drain(runtime.getMountBudget()) > 0;
                        if (System.nanoTime() - nextStallLogNanos >= 0) {
                            logHaltStall("drain the fiber runtime", drainStartNanos);
                            nextStallLogNanos += DEFAULT_HALT_TIMEOUT_NANOS;
                        }
                    }
                    if (hasDrainedOnHaltingThread) {
                        // fiber bodies ran on this thread, which has no worker cleaner registered
                        Misc.free(Path.THREAD_LOCAL_CLEANER);
                        Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                    }
                }
            }
            boolean isWorkerHaltComplete = true;
            AssertionError workerHaltFailure = null;
            if (isStartAttempted) {
                // Signal halt to every spawned worker UNCONDITIONALLY, before clearing or freeing.
                // start() may have stalled between running=true and started.countDown() (e.g. an OOM
                // mid-launch), so the start latch may never count down -- but the worker threads are
                // already spawned and looping. Skipping the signal there (the old start-latch-timeout
                // branch) left those workers looping on RUNNING against the freeOnExit resources this
                // method then frees: a use-after-free plus an orphan thread leak. The per-worker halt
                // flag is idempotent, so signalling unconditionally is safe on every branch. Iterate
                // the live workers list (not workerCount) so a partially-spawned pool is covered.
                //
                // Read the list under workersLock so a concurrent start() still mid-add cannot present
                // it torn (a half-published pos/buffer or a null slot). The monitor makes this pass see
                // an empty-or-complete-and-consistent snapshot; the signal still runs UNCONDITIONALLY
                // and BEFORE started.await() below, preserving the start-stall halt ordering.
                signalHalt(runtime != null);
                final boolean isStartComplete;
                if (isBounded) {
                    isStartComplete = started.await(remaining(deadlineNanos));
                } else {
                    awaitHalt(started, "start");
                    isStartComplete = true;
                }
                if (isStartComplete) {
                    // start() completed: every worker is now in the list. Re-signal to catch any
                    // worker spawned after the first pass but before started counted down (the flag
                    // is idempotent), then wait for them to exit.
                    signalHalt(runtime != null);
                    final boolean isWorkerHalted;
                    if (isBounded) {
                        isWorkerHalted = halted.await(remaining(deadlineNanos));
                    } else {
                        awaitHalt(halted, "halt");
                        isWorkerHalted = true;
                    }
                    if (!isWorkerHalted) {
                        isWorkerHaltComplete = false;
                        if (isStrict) {
                            workerHaltFailure = workerPoolHaltTimeout(timeoutNanos, true);
                        } else {
                            try {
                                LOG.error().$("timed out waiting for worker pool to halt; retaining live pool resources [pool=")
                                        .$(poolName)
                                        .$(", timeout=").$(timeoutNanos / 1_000_000).$("ms").I$();
                            } catch (Throwable ignore) {
                            }
                        }
                    }
                } else {
                    isWorkerHaltComplete = false;
                    if (isStrict) {
                        workerHaltFailure = workerPoolHaltTimeout(timeoutNanos, false);
                    } else {
                        try {
                            LOG.error().$("timed out waiting for worker pool to start; retaining live pool resources [pool=")
                                    .$(poolName)
                                    .$(", timeout=").$(timeoutNanos / 1_000_000).$("ms").I$();
                        } catch (Throwable ignore) {
                        }
                    }
                }
            }
            if (!isRuntimeDrained || !isWorkerHaltComplete) {
                if (runtimeHaltFailure != null) {
                    if (workerHaltFailure != null) {
                        runtimeHaltFailure.addSuppressed(workerHaltFailure);
                    }
                    throw runtimeHaltFailure;
                }
                if (workerHaltFailure != null) {
                    throw workerHaltFailure;
                }
                return false;
            }
            Throwable cleanupFailure = null;
            if (runtime != null) {
                if (dynamicFiberConfiguration != null) {
                    try {
                        dynamicFiberConfiguration.setFiberConfigurationListener(null);
                    } catch (Throwable th) {
                        cleanupFailure = addCleanupFailure(cleanupFailure, th);
                    }
                }
                try {
                    runtime.closeAfterDrained();
                } catch (Throwable th) {
                    cleanupFailure = addCleanupFailure(cleanupFailure, th);
                }
                try {
                    metrics.fiberMetrics().unregister(runtime);
                } catch (Throwable th) {
                    cleanupFailure = addCleanupFailure(cleanupFailure, th);
                }
            }
            closeInstances(assignedJobs);
            synchronized (workersLock) {
                workers.clear(); // Worker is not closable
            }
            // Closeables the caller explicitly handed to the pool via freeOnExit() are closed here;
            // the pool never close()d the jobs it minted itself -- those release through
            // closeInstance() above.
            cleanupFailure = Misc.freeObjListIfCloseableBestEffort(cleanupFailure, freeOnExit);
            isHaltComplete = true;
            CairoException.rethrowCleanupFailure(cleanupFailure);
            return true;
        } finally {
            haltLock.unlock();
            if (isInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void logHaltStall(String stage, long startNanos) {
        try {
            LOG.error().$("still waiting for worker pool to ").$(stage)
                    .$(" [pool=").$(poolName)
                    .$(", waited=").$((System.nanoTime() - startNanos) / 1_000_000).$("ms").I$();
        } catch (Throwable ignore) {
        }
    }

    private void rollbackFiberRuntimeConstruction(Throwable failure) {
        if (dynamicFiberConfiguration != null) {
            try {
                dynamicFiberConfiguration.setFiberConfigurationListener(null);
            } catch (Throwable th) {
                addCleanupFailure(failure, th);
            }
        }
        try {
            metrics.fiberMetrics().unregister(fiberRuntime);
        } catch (Throwable th) {
            addCleanupFailure(failure, th);
        }
        try {
            fiberRuntime.beginQuiesce();
        } catch (Throwable th) {
            addCleanupFailure(failure, th);
        }
        try {
            fiberRuntime.closeAfterDrained();
        } catch (Throwable th) {
            addCleanupFailure(failure, th);
        }
    }

    private void setupPathCleaner() {
        for (int i = 0; i < workerCount; i++) {
            ObjList<Closeable> workerCleaners = threadLocalCleaners.getQuick(i);
            workerCleaners.add(Path.THREAD_LOCAL_CLEANER);
            workerCleaners.add(O3PartitionJob.THREAD_LOCAL_CLEANER);
        }
    }

    private void signalHalt(boolean isFiberRuntimeDraining) {
        synchronized (workersLock) {
            for (int i = 0, n = workers.size(); i < n; i++) {
                final Worker worker = workers.getQuick(i);
                if (isFiberRuntimeDraining) {
                    worker.haltAfterFiberDrain();
                } else {
                    worker.halt();
                }
            }
        }
    }

    private void trackOwnedJob(Job job) {
        try {
            assignedJobs.add(job);
        } catch (Throwable th) {
            try {
                job.closeInstance();
            } catch (Throwable cleanupFailure) {
                if (cleanupFailure != th) {
                    th.addSuppressed(cleanupFailure);
                }
            }
            throw th;
        }
    }

    private AssertionError workerPoolHaltLockTimeout(long timeoutNanos) {
        return new AssertionError(
                "WorkerPool timed out waiting to enter halt before leak-sensitive test cleanup [pool="
                        + poolName
                        + ", timeoutMs=" + (timeoutNanos / 1_000_000)
                        + ']'
        );
    }

    private AssertionError workerPoolHaltTimeout(long timeoutNanos, boolean isStartComplete) {
        return new AssertionError(
                "WorkerPool timed out waiting for workers to halt before leak-sensitive test cleanup [pool="
                        + poolName
                        + ", timeoutMs=" + (timeoutNanos / 1_000_000)
                        + ", startCompleted=" + isStartComplete
                        + ", remainingHalted=" + halted.getCount()
                        + ']'
        );
    }
}

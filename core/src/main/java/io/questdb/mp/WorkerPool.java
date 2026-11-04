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
import io.questdb.log.LogRecord;
import io.questdb.metrics.WorkerMetrics;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberWakeSink;
import io.questdb.mp.continuation.SuspensionScope;
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
    // One runtime per group, so a group's fibers, fiber pool and run queues stay on its own
    // workers; empty when the pool doesn't host fibers.
    private final ObjList<FiberRuntime> fiberRuntimes = new ObjList<>();
    private final ObjList<Object> freeOnExit = new ObjList<>();
    private final int groupCount;
    // per group, the CPUs its workers may run on; null leaves placement to workerAffinity
    private final @Nullable int[][] groupCpus;
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
    // one per group, parallel to fiberRuntimes; empty unless the pool hosts fibers on workers
    private final ObjList<WorkerWakeController> workerWakeControllers = new ObjList<>();
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
    private volatile boolean isHaltComplete;
    private volatile boolean isStartAttempted;

    public WorkerPool(WorkerPoolConfiguration configuration) {
        this.workerCount = configuration.getWorkerCount();
        int[] workerAffinity = configuration.getWorkerAffinity();
        if (workerAffinity != null && workerAffinity.length > 0) {
            this.workerAffinity = workerAffinity;
        } else {
            this.workerAffinity = Misc.getWorkerAffinity(workerCount);
        }
        this.groupCount = configuration.getGroupCount();
        this.groupCpus = configuration.getGroupCpus();
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

        if (this.workerAffinity.length != workerCount) {
            throw new IllegalArgumentException("worker affinity length does not match worker count [pool=" + poolName
                    + ", affinity=" + this.workerAffinity.length + ", workers=" + workerCount + ']');
        }
        if (groupCount < 1 || groupCount > Math.max(1, workerCount)) {
            throw new IllegalArgumentException("group count must be between 1 and the worker count [pool=" + poolName
                    + ", groups=" + groupCount + ", workers=" + workerCount + ']');
        }
        if (groupCpus != null) {
            if (groupCpus.length != groupCount) {
                throw new IllegalArgumentException("group CPU sets do not match group count [pool=" + poolName
                        + ", cpuSets=" + groupCpus.length + ", groups=" + groupCount + ']');
            }
            for (int i = 0; i < workerCount; i++) {
                if (this.workerAffinity[i] > Worker.NO_THREAD_AFFINITY) {
                    throw new IllegalArgumentException("worker affinity cannot be combined with group CPU sets [pool=" + poolName + ']');
                }
            }
        }

        this.workerJobs = new ObjList<>(workerCount);
        this.threadLocalCleaners = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            workerJobs.add(new ObjHashSet<>());
            threadLocalCleaners.add(new ObjList<>());
        }

        if (isFiberHost) {
            try {
                for (int g = 0; g < groupCount; g++) {
                    final int groupWorkerCount = getGroupStart(g + 1) - getGroupStart(g);
                    final WorkerWakeController wakeController = groupWorkerCount > 0
                            ? new WorkerWakeController(groupWorkerCount)
                            : null;
                    if (wakeController != null) {
                        workerWakeControllers.add(wakeController);
                    }
                    fiberRuntimes.add(new FiberRuntime(
                            getGroupShare(fiberRetainedCount, g),
                            getGroupShare(fiberMaxLiveCount, g),
                            fiberMountBudget,
                            groupWorkerCount,
                            configuration.getFiberDispatchController(),
                            wakeController != null ? wakeController : FiberWakeSink.NO_OP
                    ));
                }
                if (dynamicFiberConfiguration != null) {
                    dynamicFiberConfiguration.setFiberConfigurationListener(this::updateFiberConfiguration);
                }
                for (int g = 0; g < groupCount; g++) {
                    metrics.fiberMetrics().register(poolName, groupCount > 1 ? g : -1, fiberRuntimes.getQuick(g));
                }
            } catch (Throwable th) {
                rollbackFiberRuntimeConstruction(th);
                throw th;
            }
        }
    }

    /**
     * The group of worker {@code worker} when {@code workerCount} workers split into
     * {@code groupCount} contiguous blocks.
     */
    public static int getGroup(int worker, int groupCount, int workerCount) {
        return (int) ((long) worker * groupCount / workerCount);
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
        freeOnExit.add(job);
    }

    public void freeResourceOnExit(Closeable resource) {
        assert !running.get() && !closed.get();
        freeOnExit.add(resource);
    }

    /**
     * The live-fiber limit of the whole pool: the sum of its groups' limits.
     */
    public int getFiberMaxLiveCount() {
        int count = 0;
        for (int g = 0, n = fiberRuntimes.size(); g < n; g++) {
            count += fiberRuntimes.getQuick(g).getMaxLiveFiberCount();
        }
        return count;
    }

    public int getFiberMountBudget() {
        return fiberRuntimes.size() > 0 ? fiberRuntimes.getQuick(0).getMountBudget() : 1;
    }

    /**
     * The retained-fiber limit of the whole pool: the sum of its groups' limits.
     */
    public int getFiberRetainedCount() {
        int count = 0;
        for (int g = 0, n = fiberRuntimes.size(); g < n; g++) {
            count += fiberRuntimes.getQuick(g).getMaxRetainedFiberCount();
        }
        return count;
    }

    /**
     * The fiber runtime of a pool with one group. A pool split into groups has one runtime per
     * group, so this throws; use {@link #getFiberRuntime(int)} there.
     */
    public FiberRuntime getFiberRuntime() {
        if (fiberRuntimes.size() == 0) {
            throw new IllegalStateException("worker pool does not host fibers [pool=" + poolName + ']');
        }
        if (fiberRuntimes.size() > 1) {
            throw new IllegalStateException("worker pool has one fiber runtime per worker group, use getFiberRuntime(group) [pool="
                    + poolName + ", groups=" + groupCount + ']');
        }
        return fiberRuntimes.getQuick(0);
    }

    /**
     * The fiber runtime that hosts the fibers of group {@code group}, and that only that group's
     * workers run.
     */
    public FiberRuntime getFiberRuntime(int group) {
        if (fiberRuntimes.size() == 0) {
            throw new IllegalStateException("worker pool does not host fibers [pool=" + poolName + ']');
        }
        if (group < 0 || group >= groupCount) {
            throw new IllegalArgumentException("worker group is out of range [pool=" + poolName
                    + ", group=" + group + ", groups=" + groupCount + ']');
        }
        return fiberRuntimes.getQuick(group);
    }

    /**
     * The group of worker {@code worker}: the pool's workers split into {@link #getGroupCount()}
     * contiguous blocks.
     */
    public int getGroup(int worker) {
        return getGroup(worker, groupCount, workerCount);
    }

    public int getGroupCount() {
        return groupCount;
    }

    /**
     * The CPU set of group {@code group}, sorted, or null when the pool's groups have no CPU sets.
     */
    public @Nullable int[] getGroupCpus(int group) {
        return groupCpus != null ? groupCpus[group] : null;
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
        haltAndRelease(false, 0, false);
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
        return haltAndRelease(true, deadlineNanos, false);
    }

    @TestOnly
    public void haltAndAssertCleanForTest(long timeoutNanos) {
        haltAndRelease(true, System.nanoTime() + Math.max(0, timeoutNanos), true);
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
        return haltAndRelease(true, System.nanoTime() + Math.max(0, timeoutNanos), false);
    }

    public boolean isFiberHost() {
        return mode == WorkerPoolMode.FIBER_HOST;
    }

    @TestOnly
    public int getReadyWorkerCountForTesting() {
        int count = 0;
        for (int g = 0, n = workerWakeControllers.size(); g < n; g++) {
            count += workerWakeControllers.getQuick(g).getReadyCount();
        }
        return count;
    }

    @TestOnly
    public boolean isWorkerReadyForTesting(int workerId) {
        return workerWakeControllers.size() > 0 && getWakeController(workerId).isReady(getOwnerIndex(workerId));
    }

    @TestOnly
    public boolean registerReadyWorkerForTesting(int workerId) {
        return workerWakeControllers.size() > 0 && getWakeController(workerId).registerReady(getOwnerIndex(workerId));
    }

    @TestOnly
    public void registerWakeTargetForTesting(int workerId, Thread target) {
        if (workerWakeControllers.size() == 0) {
            throw new IllegalStateException("worker pool has no wake controller");
        }
        getWakeController(workerId).registerTarget(getOwnerIndex(workerId), target);
    }

    /**
     * Sets the wake cursor of the group of worker {@code wakeCursor} to that worker.
     */
    @TestOnly
    public void setWakeCursorForTesting(int wakeCursor) {
        if (workerWakeControllers.size() == 0) {
            throw new IllegalStateException("worker pool has no wake controller");
        }
        getWakeController(wakeCursor).setWakeCursorForTesting(getOwnerIndex(wakeCursor));
    }

    @TestOnly
    public void unregisterReadyWorkerForTesting(int workerId) {
        if (workerWakeControllers.size() > 0) {
            getWakeController(workerId).unregisterReady(getOwnerIndex(workerId));
        }
    }

    @TestOnly
    public void wakeAllForTesting() {
        for (int g = 0, n = workerWakeControllers.size(); g < n; g++) {
            workerWakeControllers.getQuick(g).wakeAll();
        }
    }

    @TestOnly
    public boolean wakeOneForTesting(int preferredWorkerId) {
        return workerWakeControllers.size() > 0
                && getWakeController(preferredWorkerId).wakeOne(getOwnerIndex(preferredWorkerId));
    }

    @TestOnly
    public void pause() {
        if (fiberRuntimes.size() > 0) {
            throw new IllegalStateException("fiber-host worker pool cannot pause [pool=" + poolName + ']');
        }
        if (running.compareAndSet(true, false)) {
            started.await();
            synchronized (workersLock) {
                for (int i = 0, n = workers.size(); i < n; i++) {
                    workers.getQuick(i).halt();
                }
            }
            halted.await();
            // re-arm so the next start() and halt() pair every spawned worker with one countdown
            halted.setCount(workerCount);
            started.setCount(1);
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
                    final LogRecord record = log.info().$("worker pool configured [pool=").$(poolName)
                            .$(", workers=").$(workerCount)
                            .$(", mode=").$(mode.name());
                    if (groupCount > 1) {
                        record.$(", groups=").$(groupCount);
                    }
                    if (fiberRuntimes.size() > 0) {
                        record.$(", maxLive=").$(getFiberMaxLiveCount())
                                .$(", maxRetained=").$(getFiberRetainedCount())
                                .$(", mountBudget=").$(getFiberMountBudget());
                    }
                    record.I$();
                }
                // very common cleaner
                // it is set up from start() to make sure it is called last
                // some other thread local cleaners are liable to access thread local Path instances
                setupPathCleaner();

                for (int i = 0; i < workerCount; i++) {
                    final int index = i;
                    final int group = getGroup(i);
                    final FiberRuntime fiberRuntime = fiberRuntimes.size() > 0 ? fiberRuntimes.getQuick(group) : null;
                    final WorkerWakeController wakeController = fiberRuntime != null ? workerWakeControllers.getQuick(group) : null;
                    final int ownerIndex = getOwnerIndex(i);
                    Worker worker = new Worker(
                            poolName,
                            i,
                            workerAffinity[i],
                            groupCpus != null ? groupCpus[group] : null,
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
                            fiberRuntime != null ? fiberRuntime.getOwnerContext(ownerIndex) : null,
                            wakeController,
                            log
                    );
                    if (wakeController != null) {
                        wakeController.registerTarget(ownerIndex, worker);
                    }
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

    public void updateWorkerMetrics() {
        final long nowNanos = System.nanoTime();
        WorkerMetrics workerMetrics = metrics.workerMetrics();
        long min = workerMetrics.getMinElapsedMicros();
        long max = workerMetrics.getMaxElapsedMicros();
        // Iterate the workers list under the monitor: the /metrics scrape calls this concurrently
        // with start()'s add-loop and halt()'s clear(). Without the guard a torn read returns a null
        // slot (NPE on getQuick(i).getJobStartNanos()) or a half-published pos/buffer.
        synchronized (workersLock) {
            for (int i = 0, n = workers.size(); i < n; i++) {
                long elapsed = (nowNanos - workers.getQuick(i).getJobStartNanos()) / 1000;
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

    private static void suppressCleanupFailure(Throwable primary, Throwable failure) {
        if (primary != failure) {
            primary.addSuppressed(failure);
        }
    }

    // Polls at the same cadence SOCountDownLatch.await() parks at, so a lost unpark still
    // recovers within one park interval rather than one stall-log interval.
    private void awaitHalt(SOCountDownLatch latch, String stage) {
        final long startNanos = System.nanoTime();
        long nextStallLogNanos = startNanos + DEFAULT_HALT_TIMEOUT_NANOS;
        while (!latch.await(Os.PARK_NANOS_MAX)) {
            nextStallLogNanos = logStallIfDue(stage, startNanos, nextStallLogNanos);
        }
    }

    private boolean awaitRuntimeQuiesce(FiberRuntime runtime, boolean isBounded, long deadlineNanos) {
        runtime.beginQuiesce();
        if (isBounded) {
            return runtime.awaitClosed(deadlineNanos);
        }
        // the halting thread drains so closure does not depend on a live worker
        final long drainStartNanos = System.nanoTime();
        long nextStallLogNanos = drainStartNanos + DEFAULT_HALT_TIMEOUT_NANOS;
        boolean hasDrainedOnHaltingThread = false;
        while (!runtime.awaitClosed(System.nanoTime() + 1_000_000L)) {
            hasDrainedOnHaltingThread |= runtime.drain(runtime.getMountBudget()) > 0;
            nextStallLogNanos = logStallIfDue("drain the fiber runtime", drainStartNanos, nextStallLogNanos);
        }
        if (hasDrainedOnHaltingThread) {
            // fiber bodies ran on this thread, which has no worker cleaner registered
            Misc.free(Path.THREAD_LOCAL_CLEANER);
            Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
        }
        return true;
    }

    // Begins quiescing every group's runtime before waiting on any, so all groups stop admitting
    // work together. Returns the first group whose runtime didn't close by the deadline, or -1.
    private int awaitRuntimesQuiesce(boolean isBounded, long deadlineNanos) {
        for (int g = 0, n = fiberRuntimes.size(); g < n; g++) {
            fiberRuntimes.getQuick(g).beginQuiesce();
        }
        for (int g = 0, n = fiberRuntimes.size(); g < n; g++) {
            if (!awaitRuntimeQuiesce(fiberRuntimes.getQuick(g), isBounded, deadlineNanos)) {
                return g;
            }
        }
        return -1;
    }

    private WorkerHaltResult awaitWorkerHalt(boolean isFiberRuntimeDraining, boolean isBounded, long deadlineNanos) {
        // Signal every spawned worker before waiting on the start latch: start() may have stalled
        // between running=true and started.countDown(), leaving spawned workers looping against
        // resources this halt is about to free. The per-worker halt flag is idempotent.
        signalHalt(isFiberRuntimeDraining);
        if (isBounded) {
            if (!started.await(remaining(deadlineNanos))) {
                return WorkerHaltResult.START_TIMEOUT;
            }
        } else {
            awaitHalt(started, "start");
        }
        // start() completed: re-signal to catch a worker spawned after the first pass
        signalHalt(isFiberRuntimeDraining);
        if (isBounded) {
            return halted.await(remaining(deadlineNanos)) ? WorkerHaltResult.HALTED : WorkerHaltResult.HALT_TIMEOUT;
        }
        awaitHalt(halted, "halt");
        return WorkerHaltResult.HALTED;
    }

    private void countDownUnstartedWorkers(int firstUnstartedWorker) {
        for (int i = firstUnstartedWorker; i < workerCount; i++) {
            halted.countDown();
        }
    }

    private String describeRuntime(int group) {
        final FiberRuntime runtime = fiberRuntimes.getQuick(group);
        return "state=" + runtime.state()
                + ", outstanding=" + runtime.getOutstandingTaskCount()
                + ", queued=" + runtime.getQueuedCount()
                + ", mounted=" + runtime.getMountedCount()
                + ", parked=" + runtime.getParkedFiberCount()
                + ", live=" + runtime.getLiveFiberCount()
                + ", retained=" + runtime.getRetainedFiberCount()
                + ", finalizing=" + runtime.getFinalizerCount()
                + ", budgetExhaustions=" + runtime.getBudgetExhaustionCount()
                + (groupCount > 1 ? ", group=" + group : "");
    }

    private AssertionError fiberRuntimeHaltTimeout(long timeoutNanos, int group) {
        return new AssertionError(
                "WorkerPool timed out waiting for fiber runtime to drain before leak-sensitive test cleanup [pool="
                        + poolName
                        + ", timeoutMs=" + (timeoutNanos / 1_000_000)
                        + ", " + describeRuntime(group)
                        + ']'
        );
    }

    // A group's share of a pool-wide fiber limit, in proportion to its share of the workers,
    // rounded up and at least 1. A real split has at least CpuTopology.MIN_WORKERS_PER_GROUP
    // workers per group, so dividing the default limits gives what the default formulas give for
    // the group's own worker count. A limit below 1 passes through for the runtime to reject.
    private int getGroupShare(int limit, int group) {
        if (groupCount == 1 || limit < 1) {
            return limit;
        }
        final long groupWorkerCount = getGroupStart(group + 1) - getGroupStart(group);
        return (int) Math.max(1, (limit * groupWorkerCount + workerCount - 1) / workerCount);
    }

    // the first worker of group group; group == groupCount gives workerCount
    private int getGroupStart(int group) {
        return (int) (((long) group * workerCount + groupCount - 1) / groupCount);
    }

    // Worker workerId's index among its group's workers, which is its owner index in the group's
    // fiber runtime and its id in the group's wake controller. An id outside the pool passes
    // through unchanged, for the wake controller to treat as out of range.
    private int getOwnerIndex(int workerId) {
        return workerId >= 0 && workerId < workerCount ? workerId - getGroupStart(getGroup(workerId)) : workerId;
    }

    // the wake controller of worker workerId's group; an id outside the pool goes to group 0's
    private WorkerWakeController getWakeController(int workerId) {
        return workerWakeControllers.getQuick(workerId >= 0 && workerId < workerCount ? getGroup(workerId) : 0);
    }

    private boolean haltAndRelease(boolean isBounded, long deadlineNanos, boolean isStrict) {
        // Preserve the terminal operation's idempotent no-op without making an unsafe Worker
        // wait for haltLock. A Worker blocked on that lock may itself be needed by the active
        // halter to drain the runtime and exit.
        if (isHaltComplete) {
            return true;
        }
        preflightTerminalFiberHalt();
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
            if (running.compareAndSet(true, false)) {
                isStartAttempted = true;
            }
            final boolean isFiberHost = fiberRuntimes.size() > 0;
            AssertionError runtimeHaltFailure = null;
            boolean isRuntimeDrained = true;
            if (isFiberHost) {
                final int undrainedGroup = awaitRuntimesQuiesce(isBounded, deadlineNanos);
                isRuntimeDrained = undrainedGroup < 0;
                if (!isRuntimeDrained) {
                    if (isStrict) {
                        runtimeHaltFailure = fiberRuntimeHaltTimeout(timeoutNanos, undrainedGroup);
                    } else {
                        final String runtimeDescription = describeRuntime(undrainedGroup);
                        LOG.error().$("timed out waiting for fiber runtime to drain; retaining live pool resources [pool=").$(poolName)
                                .$(", timeout=").$(timeoutNanos / 1_000_000).$("ms")
                                .$(", ").$(runtimeDescription).I$();
                    }
                }
            }
            AssertionError workerHaltFailure = null;
            boolean isWorkerHaltComplete = true;
            if (isStartAttempted) {
                final WorkerHaltResult workerHaltResult = awaitWorkerHalt(isFiberHost, isBounded, deadlineNanos);
                if (workerHaltResult != WorkerHaltResult.HALTED) {
                    isWorkerHaltComplete = false;
                    final boolean isStartComplete = workerHaltResult == WorkerHaltResult.HALT_TIMEOUT;
                    if (isStrict) {
                        workerHaltFailure = workerPoolHaltTimeout(timeoutNanos, isStartComplete);
                    } else {
                        LOG.error().$("timed out waiting for worker pool to ").$(isStartComplete ? "halt" : "start")
                                .$("; retaining live pool resources [pool=").$(poolName)
                                .$(", timeout=").$(timeoutNanos / 1_000_000).$("ms").I$();
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
            final Throwable cleanupFailure = releaseResources();
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

    private long logStallIfDue(String stage, long startNanos, long nextStallLogNanos) {
        if (System.nanoTime() - nextStallLogNanos < 0) {
            return nextStallLogNanos;
        }
        LOG.error().$("still waiting for worker pool to ").$(stage)
                .$(" [pool=").$(poolName)
                .$(", waited=").$((System.nanoTime() - startNanos) / 1_000_000).$("ms").I$();
        return nextStallLogNanos + DEFAULT_HALT_TIMEOUT_NANOS;
    }

    private @Nullable Throwable releaseResources() {
        Throwable cleanupFailure = null;
        for (int g = 0, n = workerWakeControllers.size(); g < n; g++) {
            workerWakeControllers.getQuick(g).deactivate();
        }
        if (fiberRuntimes.size() > 0 && dynamicFiberConfiguration != null) {
            try {
                dynamicFiberConfiguration.setFiberConfigurationListener(null);
            } catch (Throwable th) {
                cleanupFailure = addCleanupFailure(cleanupFailure, th);
            }
        }
        for (int g = 0, n = fiberRuntimes.size(); g < n; g++) {
            final FiberRuntime runtime = fiberRuntimes.getQuick(g);
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
        // the pool close()s only what freeOnExit() handed over; its own jobs released via closeInstance() above
        return Misc.freeObjListIfCloseableBestEffort(cleanupFailure, freeOnExit);
    }

    // Undoes construction for the runtimes created before the failure, which can be fewer than
    // the groups when a later group's runtime fails to construct.
    private void rollbackFiberRuntimeConstruction(Throwable failure) {
        if (dynamicFiberConfiguration != null) {
            try {
                dynamicFiberConfiguration.setFiberConfigurationListener(null);
            } catch (Throwable th) {
                suppressCleanupFailure(failure, th);
            }
        }
        for (int g = 0, n = fiberRuntimes.size(); g < n; g++) {
            final FiberRuntime runtime = fiberRuntimes.getQuick(g);
            try {
                metrics.fiberMetrics().unregister(runtime);
            } catch (Throwable th) {
                suppressCleanupFailure(failure, th);
            }
            try {
                runtime.beginQuiesce();
            } catch (Throwable th) {
                suppressCleanupFailure(failure, th);
            }
            try {
                runtime.closeAfterDrained();
            } catch (Throwable th) {
                suppressCleanupFailure(failure, th);
            }
        }
        for (int g = 0, n = workerWakeControllers.size(); g < n; g++) {
            workerWakeControllers.getQuick(g).deactivate();
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
        for (int g = 0, n = workerWakeControllers.size(); g < n; g++) {
            workerWakeControllers.getQuick(g).wakeAll();
        }
    }

    private void preflightTerminalFiberHalt() {
        if (fiberRuntimes.size() == 0) {
            return;
        }
        if (Worker.current() != null) {
            throw new IllegalStateException("terminal Fiber-host halt requires a non-Worker carrier [pool="
                    + poolName + ']');
        }
        if (Fiber.isMounted()) {
            throw new IllegalStateException("terminal Fiber-host halt cannot run from a mounted Fiber [pool="
                    + poolName + ']');
        }
        if (SuspensionScope.hasAnyRoleSwitchLock()) {
            throw new IllegalStateException("terminal Fiber-host halt requires a clean carrier role [pool="
                    + poolName + ']');
        }
    }

    private void trackOwnedJob(Job job) {
        assignedJobs.add(job);
    }

    // receives the pool-wide limits and gives each group's runtime its share
    private void updateFiberConfiguration(int maxLiveCount, int retainedCount, int mountBudget) {
        for (int g = 0, n = fiberRuntimes.size(); g < n; g++) {
            fiberRuntimes.getQuick(g).updateConfiguration(
                    getGroupShare(maxLiveCount, g),
                    getGroupShare(retainedCount, g),
                    mountBudget
            );
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

    private enum WorkerHaltResult {
        HALTED, START_TIMEOUT, HALT_TIMEOUT
    }
}

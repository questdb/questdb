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
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerPool implements Closeable {
    // Generous backstop used by the unbounded halt() so a wedged worker cannot block shutdown forever.
    // Callers that want a tighter, shared budget across several pools pass an explicit timeout to halt(long).
    public static final long DEFAULT_HALT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final Log LOG = LogFactory.getLog(WorkerPool.class);
    @TestOnly
    private volatile Runnable beforeStartedSignalForTesting;
    @TestOnly
    private volatile Runnable beforeWorkerAddedForTesting;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final boolean daemons;
    private final ObjList<Closeable> freeOnExit = new ObjList<>();
    private final boolean haltOnError;
    private final SOCountDownLatch halted;
    private final Metrics metrics;
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

        for (int i = 0; i < workerCount; i++) {
            workerJobs.getQuick(i).add(job);
        }
    }

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

    public void freeOnExit(Closeable closeable) {
        assert !running.get() && !closed.get();

        freeOnExit.add(closeable);
    }

    public String getPoolName() {
        return poolName;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void halt() {
        halt(DEFAULT_HALT_TIMEOUT_NANOS);
    }

    /**
     * Halts the pool, bounding how long it blocks waiting for worker threads.
     * <p>
     * The unbounded variant of this wait could block the caller forever: if a worker is wedged
     * (GC-starvation, a stuck native job) it never reaches halted.countDown(), so a plain
     * halted.await() in the close path made server shutdown unkillable under SIGTERM. This variant
     * waits at most timeoutNanos for started/halted and then logs a warning and proceeds, so the
     * caller can finish closing.
     * <p>
     * Tradeoff: proceeding while a worker is still running means that worker may touch state that
     * later cleanup frees. Keep the timeout generous -- it is only a backstop against a truly
     * wedged worker, not a normal-path tuning knob. Healthy pools count down well within it.
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
     */
    public void halt(long timeoutNanos) {
        if (closed.compareAndSet(false, true)) {
            if (running.compareAndSet(true, false)) {
                final long deadline = System.nanoTime() + timeoutNanos;
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
                synchronized (workersLock) {
                    for (int i = 0, n = workers.size(); i < n; i++) {
                        workers.getQuick(i).halt();
                    }
                }
                if (started.await(remaining(deadline))) {
                    // start() completed: every worker is now in the list. Re-signal to catch any
                    // worker spawned after the first pass but before started counted down (the flag
                    // is idempotent), then wait for them to exit.
                    for (int i = 0, n = workers.size(); i < n; i++) {
                        workers.getQuick(i).halt();
                    }
                    if (!halted.await(remaining(deadline))) {
                        LOG.error().$("timed out waiting for worker pool to halt; proceeding with close [pool=").$(poolName)
                                .$(", timeout=").$(timeoutNanos / 1_000_000).$("ms").I$();
                    }
                } else {
                    LOG.error().$("timed out waiting for worker pool to start; proceeding with close [pool=").$(poolName)
                            .$(", timeout=").$(timeoutNanos / 1_000_000).$("ms").I$();
                }
            }
            workers.clear(); // Worker is not closable
            Misc.freeObjListAndClear(freeOnExit);
        }
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
        workers.clear();
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
                        ex -> Misc.freeObjListAndClear(threadLocalCleaners.getQuick(index)),
                        haltOnError,
                        yieldThreshold,
                        napThreshold,
                        sleepThreshold,
                        sleepMs,
                        metrics,
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
                    workers.add(worker);
                    worker.start();
                }
            }
            if (log != null) {
                log.debug().$("worker pool started [pool=").$(poolName).I$();
            }
            final Runnable beforeStarted = beforeStartedSignalForTesting;
            if (beforeStarted != null) {
                beforeStarted.run();
            }
            started.countDown();
        }
    }

    public void updateWorkerMetrics(long now) {
        WorkerMetrics workerMetrics = metrics.workerMetrics();
        long min = workerMetrics.getMinElapsedMicros();
        long max = workerMetrics.getMaxElapsedMicros();
        for (int i = 0, n = workers.size(); i < n; i++) {
            long elapsed = now - workers.getQuick(i).getJobStartMicros();
            if (elapsed > 0) {
                min = Math.min(min, elapsed);
                max = Math.max(max, elapsed);
            }
        }
        workerMetrics.update(min, max);
    }

    private static long remaining(long deadline) {
        // Never hand SOCountDownLatch.await() a non-positive budget; parkNanos(<=0) returns
        // immediately, which is the intended behaviour once the overall deadline has passed.
        return Math.max(1, deadline - System.nanoTime());
    }

    private void setupPathCleaner() {
        for (int i = 0; i < workerCount; i++) {
            ObjList<Closeable> workerCleaners = threadLocalCleaners.getQuick(i);
            workerCleaners.add(Path.THREAD_LOCAL_CLEANER);
            workerCleaners.add(O3PartitionJob.THREAD_LOCAL_CLEANER);
        }
    }
}

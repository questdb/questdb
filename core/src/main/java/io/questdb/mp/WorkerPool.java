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
import io.questdb.metrics.WorkerMetrics;
import io.questdb.mp.continuation.ContinuationQueue;
import io.questdb.mp.continuation.ContinuationSink;
import io.questdb.mp.continuation.WorkerContinuation;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerPool implements Closeable {
    private final AtomicBoolean closed = new AtomicBoolean();
    // Non-legacy pools own a ContinuationQueue. Workers drain it from their
    // outer driver between continuation mounts, NOT as a regular job. It cannot
    // be a regular job because resume requires the calling thread to not already
    // be carrying a cont in the same scope, which holds in the outer driver but
    // not inside a mounted worker-loop body. Null on legacy pools.
    private final ContinuationQueue continuationQueue;
    private final boolean daemons;
    private final ObjList<Closeable> freeOnExit = new ObjList<>();
    private final boolean haltOnError;
    private final SOCountDownLatch halted;
    // Legacy pools run their loop body directly (no WorkerContinuation
    // wrapping) and accept per-worker Job assignment via
    // {@link #assign(int, Job)}. Used today for the ILP TCP IO and writer
    // Jobs which key per-worker state by workerId at construction time.
    private final boolean legacy;
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

        boolean isStatefulBlueprint = false;
        for (int i = 0; i < workerCount; i++) {
            Job clone = job.cloneInstance();
            workerJobs.getQuick(i).add(clone);
            // Clones minted by cloneInstance() (clone != receiver) have no
            // other lifecycle owner -- the framework registers them for close
            // at halt(). Singletons returned by the default cloneInstance()
            // (clone == job) stay caller-owned: e.g., PGServer.close() and
            // HttpServer.close() free their dispatcher explicitly, so the
            // pool must NOT close it again.
            if (clone != job) {
                isStatefulBlueprint = true;
                if (clone instanceof Closeable) {
                    freeOnExit.add((Closeable) clone);
                }
            }
        }
        // When the blueprint produces fresh clones (typical stateful Job
        // pattern), the blueprint itself is never added to workerJobs and
        // would leak its construction resources. Register it for close too.
        if (isStatefulBlueprint && job instanceof Closeable) {
            freeOnExit.add((Closeable) job);
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

    public void freeOnExit(Closeable closeable) {
        assert !running.get() && !closed.get();

        freeOnExit.add(closeable);
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
        if (closed.compareAndSet(false, true)) {
            if (running.compareAndSet(true, false)) {
                started.await();
                for (int i = 0; i < workerCount; i++) {
                    workers.getQuick(i).halt();
                }
                halted.await();
            }
            workers.clear(); // Worker is not closable
            // Closeable entries that the caller owns (the original Job passed
            // to assign() and singletons returned by Job.cloneInstance()) are
            // released by the caller via freeOnExit. Stateful clones minted by
            // assign(Job) live in workerJobs without a separate freeOnExit
            // registration; their resources are released when the workerJobs
            // entries become unreachable on pool GC. (TODO: track owned clones
            // explicitly to release native resources at halt time.)
            Misc.freeObjListAndClear(freeOnExit);
        }
    }

    /**
     * Constructs a fresh {@link WorkerContinuation} bound to this pool's resume sink.
     * Used by workers to build their own loop-body continuation, and by
     * suspending-evaluation gateways to wrap a one-shot body. Throws on legacy
     * pools, which do not run continuations.
     */
    public WorkerContinuation newContinuation(Runnable body) {
        if (legacy) {
            throw new IllegalStateException("legacy worker pool does not host continuations");
        }
        return new WorkerContinuation(body, continuationQueue);
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
                workers.add(worker);
                worker.start();
            }
            if (log != null) {
                log.debug().$("worker pool started [pool=").$(poolName).I$();
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

    private void setupPathCleaner() {
        for (int i = 0; i < workerCount; i++) {
            ObjList<Closeable> workerCleaners = threadLocalCleaners.getQuick(i);
            workerCleaners.add(Path.THREAD_LOCAL_CLEANER);
            workerCleaners.add(O3PartitionJob.THREAD_LOCAL_CLEANER);
        }
    }
}

/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.*;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerPool implements Closeable {
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final int workerCount;
    private final int[] workerAffinity;
    private final SOCountDownLatch started = new SOCountDownLatch(1);
    private final ObjList<ObjHashSet<Job>> workerJobs;
    private final SOCountDownLatch halted;
    private final ObjList<Worker> workers = new ObjList<>();
    private final ObjList<ObjList<Closeable>> cleaners;
    private final boolean haltOnError;
    private final boolean daemons;
    private final String poolName;
    private final long yieldThreshold;
    private final long sleepThreshold;
    private final long sleepMs;
    private final ObjList<Closeable> freeOnHalt = new ObjList<>();
    private final Metrics metrics;

    public WorkerPool(WorkerPoolConfiguration configuration) {
        this(configuration, Metrics.disabled());
    }

    public WorkerPool(WorkerPoolConfiguration configuration, Metrics metrics) {
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
        this.sleepThreshold = configuration.getSleepThreshold();
        this.sleepMs = configuration.getSleepTimeout();
        this.metrics = metrics;

        assert this.workerAffinity.length == workerCount;

        this.workerJobs = new ObjList<>(workerCount);
        this.cleaners = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            workerJobs.add(new ObjHashSet<>());
            cleaners.add(new ObjList<>());
        }
    }

    public String getPoolName() {
        return poolName;
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

    public void assign(int worker, Closeable cleaner) {
        assert worker > -1 && worker < workerCount && !running.get() && !closed.get();
        cleaners.getQuick(worker).add(cleaner);
    }

    public void assignCleaner(Closeable cleaner) {
        assert !running.get() && !closed.get();

        for (int i = 0; i < workerCount; i++) {
            cleaners.getQuick(i).add(cleaner);
        }
    }

    public void freeOnHalt(Closeable closeable) {
        assert !running.get() && !closed.get();

        freeOnHalt.add(closeable);
    }

    public int getWorkerCount() {
        return workerCount;
    }


    public void start() {
        start(null);
    }

    public void start(@Nullable Log log) {
        if (!closed.get() && running.compareAndSet(false, true)) {
            for (int i = 0; i < workerCount; i++) {
                final int index = i;
                Worker worker = new Worker(
                        workerJobs.getQuick(i),
                        halted,
                        workerAffinity[i],
                        log,
                        (ex) -> {
                            Misc.freeObjListAndClear(cleaners.getQuick(index));
                            if (log != null) {
                                log.info().$("cleaned worker [name=").$(poolName)
                                        .$(", worker=").$(index)
                                        .$(", total=").$(workerCount)
                                        .I$();
                            }
                        },
                        haltOnError,
                        i,
                        poolName,
                        yieldThreshold,
                        sleepThreshold,
                        sleepMs,
                        metrics
                );
                worker.setDaemon(daemons);
                workers.add(worker);
                worker.start();
            }
            if (log != null) {
                log.info().$("worker pool started [pool=").$(poolName).I$();
            }
            started.countDown();
        }
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
            Misc.freeObjListAndClear(freeOnHalt);

            // try cleaners, if worker was started and stopped, cleaner will be empty
            for (int i = 0, n = cleaners.size(); i < n; i++) {
                Misc.freeObjListAndClear(cleaners.getQuick(i));
            }
        }
    }

    @Override
    public void close() {
        halt();
    }
}

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

import io.questdb.MessageBus;
import io.questdb.Metrics;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.async.PageFrameReduceJob;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerPool implements QuietCloseable {

    private final AtomicBoolean running = new AtomicBoolean(false);
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

    WorkerPool(WorkerPoolConfiguration configuration, Metrics metrics) {
        this.workerCount = configuration.getWorkerCount();
        this.workerAffinity = configuration.getWorkerAffinity();
        this.halted = new SOCountDownLatch(workerCount);
        this.haltOnError = configuration.haltOnError();
        this.daemons = configuration.isDaemonPool();
        this.poolName = configuration.getPoolName();
        this.yieldThreshold = configuration.getYieldThreshold();
        this.sleepThreshold = configuration.getSleepThreshold();
        this.sleepMs = configuration.getSleepTimeout();
        this.metrics = metrics;

        assert workerAffinity.length == workerCount;

        this.workerJobs = new ObjList<>(workerCount);
        this.cleaners = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            workerJobs.add(new ObjHashSet<>());
            cleaners.add(new ObjList<>());
        }
    }

    public WorkerPool configure(
            CairoEngine cairoEngine,
            @Nullable FunctionFactoryCache functionFactoryCache,
            boolean withCircuitBreaker,
            boolean needMaintenanceJob
    ) throws SqlException {
        final MessageBus messageBus = cairoEngine.getMessageBus();
        final O3PartitionPurgeJob purgeDiscoveryJob = new O3PartitionPurgeJob(messageBus, workerCount);
        final ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(cairoEngine, functionFactoryCache);

        assign(purgeDiscoveryJob);
        assign(columnPurgeJob);
        assign(new O3PartitionJob(messageBus));
        assign(new O3OpenColumnJob(messageBus));
        assign(new O3CopyJob(messageBus));
        assign(new O3CallbackJob(messageBus));
        freeOnHalt(purgeDiscoveryJob);
        freeOnHalt(columnPurgeJob);
        if (needMaintenanceJob) {
            assign(cairoEngine.getEngineMaintenanceJob());
        }
        assignCleaner(Path.CLEANER);

        final MicrosecondClock microsecondClock = messageBus.getConfiguration().getMicrosecondClock();
        final NanosecondClock nanosecondClock = messageBus.getConfiguration().getNanosecondClock();

        for (int i = 0; i < workerCount; i++) {
            // create job per worker to allow each worker to have
            // own shard walk sequence
            final PageFrameReduceJob pageFrameReduceJob = new PageFrameReduceJob(
                    messageBus,
                    new Rnd(microsecondClock.getTicks(), nanosecondClock.getTicks()),
                    withCircuitBreaker ? cairoEngine.getConfiguration().getCircuitBreakerConfiguration() :  null
            );
            assign(i, (Job) pageFrameReduceJob);
            freeOnHalt(pageFrameReduceJob);
        }
        return this;
    }

    /**
     * Assigns job instance to all workers. Job member variables
     * could be accessed by multiple threads at the same time. Jobs cannot
     * be added after pool is started.
     *
     * @param job instance of job
     */
    public void assign(Job job) {
        assert !running.get();
        for (int i = 0; i < workerCount; i++) {
            workerJobs.getQuick(i).add(job);
        }
    }

    public void assign(int worker, Job job) {
        assert worker > -1 && worker < workerCount;
        workerJobs.getQuick(worker).add(job);
    }

    public void assign(int worker, Closeable cleaner) {
        assert worker > -1 && worker < workerCount;
        cleaners.getQuick(worker).add(cleaner);
    }

    public void assignCleaner(Closeable cleaner) {
        assert !running.get();
        for (int i = 0; i < workerCount; i++) {
            cleaners.getQuick(i).add(cleaner);
        }
    }

    public void freeOnHalt(Closeable closeable) {
        assert !running.get();
        freeOnHalt.add(closeable);
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void start() {
        start(null);
    }

    public void start(@Nullable Log log) {
        if (running.compareAndSet(false, true)) {
            for (int i = 0; i < workerCount; i++) {
                final int index = i;
                Worker worker = new Worker(
                        workerJobs.getQuick(i),
                        halted,
                        workerAffinity[i],
                        log,
                        (ex) -> {
                            final ObjList<Closeable> cl = cleaners.getQuick(index);
                            for (int j = 0, n = cl.size(); j < n; j++) {
                                Misc.free(cl.getQuick(j));
                            }
                            if (log != null) {
                                log.info().$("cleaned [worker=").$(index).$(']').$();
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
                log.info().$("started").$();
            }
            started.countDown();
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            started.await();
            for (int i = 0; i < workerCount; i++) {
                Worker worker = workers.getQuick(i);
                if (worker != null) {
                    worker.halt();
                }
            }
            halted.await();
            Misc.freeObjList(workers);
            Misc.freeObjList(freeOnHalt);
        }
    }
}

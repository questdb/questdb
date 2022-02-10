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

import io.questdb.log.Log;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerPool {
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

    public WorkerPool(WorkerPoolConfiguration configuration) {
        this.workerCount = configuration.getWorkerCount();
        this.workerAffinity = configuration.getWorkerAffinity();
        this.halted = new SOCountDownLatch(workerCount);
        this.haltOnError = configuration.haltOnError();
        this.daemons = configuration.isDaemonPool();
        this.poolName = configuration.getPoolName();
        this.yieldThreshold = configuration.getYieldThreshold();
        this.sleepThreshold = configuration.getSleepThreshold();
        this.sleepMs = configuration.getSleepMs();

        assert workerAffinity.length == workerCount;

        this.workerJobs = new ObjList<>(workerCount);
        this.cleaners = new ObjList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            workerJobs.add(new ObjHashSet<>());
            cleaners.add(new ObjList<>());
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

    public void halt() {
        if (running.compareAndSet(true, false)) {
            started.await();
            for (int i = 0; i < workerCount; i++) {
                workers.getQuick(i).halt();
            }
            halted.await();

            Misc.freeObjList(workers);
            Misc.freeObjList(freeOnHalt);
        }
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
                        sleepMs
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
}

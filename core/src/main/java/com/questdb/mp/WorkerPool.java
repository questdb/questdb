/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.mp;

import com.questdb.log.Log;
import com.questdb.std.Misc;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerPool {
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final int workerCount;
    private final int[] workerAffinity;
    private final SOCountDownLatch started = new SOCountDownLatch(1);
    private final ObjList<ObjHashSet<Job>> workerJobs;
    private final SOCountDownLatch haltLatch;
    private final ObjList<Worker> workers = new ObjList<>();
    private final ObjList<ObjList<Closeable>> cleaners;
    private final boolean haltOnError;

    public WorkerPool(WorkerPoolConfiguration configuration) {
        this.workerCount = configuration.getWorkerCount();
        this.workerAffinity = configuration.getWorkerAffinity();
        this.haltLatch = new SOCountDownLatch(workerCount);
        this.haltOnError = configuration.haltOnError();

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

    public int getWorkerCount() {
        return workerCount;
    }

    public void halt() {
        if (running.compareAndSet(true, false)) {
            started.await();
            for (int i = 0; i < workerCount; i++) {
                workers.getQuick(i).halt();
            }
            haltLatch.await();

            for (int i = 0; i < workerCount; i++) {
                Misc.free(workers.getQuick(i));
            }
        }
    }

    public void start(@Nullable Log log) {
        if (running.compareAndSet(false, true)) {
            for (int i = 0; i < workerCount; i++) {
                final int index = i;
                Worker worker = new Worker(
                        workerJobs.getQuick(i),
                        haltLatch,
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
                        haltOnError
                );
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

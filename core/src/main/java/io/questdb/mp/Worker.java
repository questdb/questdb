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
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;

public class Worker extends Thread {
    private final static long RUNNING_OFFSET = Unsafe.getFieldOffset(Worker.class, "running");
    private final static AtomicInteger COUNTER = new AtomicInteger();
    private final ObjHashSet<? extends Job> jobs;
    private final SOCountDownLatch haltLatch;
    private final int affinity;
    private final Log log;
    private final WorkerCleaner cleaner;
    private final boolean haltOnError;
    private final int workerId;
    private final long sleepMs;
    private volatile int running = 0;
    private final long yieldThreshold;
    private final long sleepThreshold;
    private final Metrics metrics;

    public Worker(
            final ObjHashSet<? extends Job> jobs,
            final SOCountDownLatch haltLatch,
            final int affinity,
            final Log log,
            final WorkerCleaner cleaner,
            final boolean haltOnError,
            final int workerId,
            String poolName,
            long yieldThreshold,
            long sleepThreshold,
            long sleepMs,
            Metrics metrics
    ) {
        this.log = log;
        this.jobs = jobs;
        this.haltLatch = haltLatch;
        this.setName("questdb-" + poolName + "-" + COUNTER.incrementAndGet());
        this.affinity = affinity;
        this.cleaner = cleaner;
        this.haltOnError = haltOnError;
        this.workerId = workerId;
        this.yieldThreshold = yieldThreshold;
        this.sleepThreshold = sleepThreshold;
        this.sleepMs = sleepMs;
        this.metrics = metrics;
    }

    public int getWorkerId() {
        return workerId;
    }

    public void halt() {
        running = 2;
    }

    @Override
    public void run() {
        Throwable ex = null;
        try {
            if (Unsafe.getUnsafe().compareAndSwapInt(this, RUNNING_OFFSET, 0, 1)) {
                if (affinity > -1) {
                    if (Os.setCurrentThreadAffinity(this.affinity) == 0) {
                        if (log != null) {
                            log.info().$("affinity set [cpu=").$(affinity).$(", name=").$(getName()).$(']').$();
                        }
                    } else {
                        if (log != null) {
                            log.error().$("could not set affinity [cpu=").$(affinity).$(", name=").$(getName()).$(']').$();
                        }
                    }
                } else {
                    if (log != null) {
                        log.info().$("os scheduled [name=").$(getName()).$(']').$();
                    }
                }
                setupJobs();
                int n = jobs.size();
                long uselessCounter = 0;
                while (running == 1) {

                    boolean useful = false;
                    for (int i = 0; i < n; i++) {
                        Unsafe.getUnsafe().loadFence();
                        try {
                            try {
                                useful |= jobs.get(i).run(workerId);
                            } catch (Throwable e) {
                                onError(i, e);
                            }
                        } finally {
                            Unsafe.getUnsafe().storeFence();
                        }
                    }

                    if (useful) {
                        uselessCounter = 0;
                        continue;
                    }

                    uselessCounter++;

                    if (uselessCounter < 0) {
                        // deal with overflow
                        uselessCounter = sleepThreshold + 1;
                    }

                    if (uselessCounter > yieldThreshold) {
                        Thread.yield();
                    }

                    if (uselessCounter > sleepThreshold) {
                        Os.sleep(sleepMs);
                    }
                }
            }
        } catch (Throwable e) {
            ex = e;
        } finally {
            // cleaner will typically attempt to release
            // thread-local instances
            if (cleaner != null) {
                cleaner.run(ex);
            }
            haltLatch.countDown();
        }
    }

    private void onError(int i, Throwable e) throws Throwable {
        metrics.healthCheck().incrementUnhandledErrors();
        // Log error even when halt on error is set
        if (log != null) {
            log.error().$("unhandled error [job=").$(jobs.get(i).toString()).$(", ex=").$(e).$(']').$();
        } else {
            e.printStackTrace();
        }
        if (haltOnError) {
            throw e;
        }
    }

    private void setupJobs() {
        if (running == 1) {
            for (int i = 0; i < jobs.size(); i++) {
                Unsafe.getUnsafe().loadFence();
                try {
                    Job job = jobs.get(i);
                    if (job instanceof EagerThreadSetup) {
                        ((EagerThreadSetup) job).setup();
                    }
                } finally {
                    Unsafe.getUnsafe().storeFence();
                }
            }
        }
    }

}

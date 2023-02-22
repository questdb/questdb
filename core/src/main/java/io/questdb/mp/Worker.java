/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.metrics.HealthMetrics;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;

public class Worker extends Thread {
    private final static AtomicInteger COUNTER = new AtomicInteger();
    private final int affinity;
    private final WorkerCleaner cleaner;
    private final String criticalErrorLine;
    private final SOCountDownLatch haltLatch;
    private final boolean haltOnError;
    private final ObjHashSet<? extends Job> jobs;
    private final Log log;
    private final HealthMetrics metrics;
    private final AtomicInteger running = new AtomicInteger();
    private final Job.RunStatus runStatus = () -> running.get() == 2;
    private final long sleepMs;
    private final long sleepThreshold;
    private final int workerId;
    private final long yieldThreshold;

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
            HealthMetrics metrics
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
        this.criticalErrorLine = "0000-00-00T00:00:00.000000Z C Unhandled exception in worker " + getName();
    }

    public int getWorkerId() {
        return workerId;
    }

    public void halt() {
        running.set(2);
    }

    @Override
    public void run() {
        Throwable ex = null;
        try {
            if (running.compareAndSet(0, 1)) {
                if (affinity > -1) {
                    if (Os.setCurrentThreadAffinity(this.affinity) == 0) {
                        if (log != null) {
                            log.info().$("affinity set [cpu=").$(affinity).$(", name=").$(getName()).I$();
                        }
                    } else {
                        if (log != null) {
                            log.error().$("could not set affinity [cpu=").$(affinity).$(", name=").$(getName()).I$();
                        }
                    }
                } else {
                    if (log != null) {
                        log.info().$("os scheduled worker started [name=").$(getName()).I$();
                    }
                }
                setupJobs();
                int n = jobs.size();
                long uselessCounter = 0;
                while (running.get() == 1) {
                    boolean useful = false;
                    for (int i = 0; i < n; i++) {
                        Unsafe.getUnsafe().loadFence();
                        try {
                            try {
                                useful |= jobs.get(i).run(workerId, runStatus);
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

                    if (uselessCounter > sleepThreshold) {
                        Os.sleep(sleepMs);
                    } else if (uselessCounter > yieldThreshold) {
                        Os.pause();
                    }
                }
            }
        } catch (Throwable e) {
            ex = e;
            stdErrCritical(e);
        } finally {
            // cleaner will typically attempt to release
            // thread-local instances
            if (cleaner != null) {
                cleaner.run(ex);
            }
            haltLatch.countDown();
            if (log != null) {
                log.info().$("os scheduled worker stopped [name=").$(getName()).I$();
            }
        }
    }

    private void onError(int i, Throwable e) throws Throwable {
        try {
            metrics.incrementUnhandledErrors();
        } catch (Throwable t) {
            stdErrCritical(e);
        }

        // Log error even then halt if halt error setting is on.
        if (log != null) {
            log.critical().$("unhandled error [job=").$(jobs.get(i).toString()).$(", ex=").$(e).I$();
        } else {
            stdErrCritical(e);
        }
        if (haltOnError) {
            throw e;
        }
    }

    private void setupJobs() {
        if (running.get() == 1) {
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

    private void stdErrCritical(Throwable e) {
        System.err.println(criticalErrorLine);
        e.printStackTrace();
    }
}

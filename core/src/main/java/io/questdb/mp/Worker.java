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

package io.questdb.mp;

import io.questdb.log.Log;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class Worker extends Thread {
    private final static long RUNNING_OFFSET = Unsafe.getFieldOffset(Worker.class, "running");
    private static final long YIELD_THRESHOLD = 100000L;
    private static final long SLEEP_THRESHOLD = 10000000L;
    private final static AtomicInteger COUNTER = new AtomicInteger();
    private final ObjHashSet<? extends Job> jobs;
    private final SOCountDownLatch haltLatch;
    private final int affinity;
    private final Log log;
    private final WorkerCleaner cleaner;
    private final boolean haltOnError;
    @SuppressWarnings("FieldCanBeLocal")
    private volatile int running = 0;
    private volatile int fence;

    public Worker(
            ObjHashSet<? extends Job> jobs,
            SOCountDownLatch haltLatch
    ) {
        this(jobs, haltLatch, -1, null, null, true);
    }

    public Worker(
            final ObjHashSet<? extends Job> jobs,
            final SOCountDownLatch haltLatch,
            final int affinity,
            final Log log,
            final WorkerCleaner cleaner,
            final boolean haltOnError
    ) {
        this.log = log;
        this.jobs = jobs;
        this.haltLatch = haltLatch;
        this.setName("questdb-worker-" + COUNTER.incrementAndGet());
        this.affinity = affinity;
        this.cleaner = cleaner;
        this.haltOnError = haltOnError;
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
                        loadFence();
                        try {
                            try {
                                useful |= jobs.get(i).run();
                            } catch (Throwable e) {
                                if (haltOnError) {
                                    throw e;
                                }
                                if (log != null) {
                                    log.error().$("unhandled error [job=").$(jobs.get(i).toString()).$(", ex=").$(e).$(']').$();
                                } else {
                                    e.printStackTrace();
                                }
                            }
                        } finally {
                            storeFence();
                        }
                    }

                    if (useful) {
                        uselessCounter = 0;
                        continue;
                    }

                    uselessCounter++;

                    if (uselessCounter < 0) {
                        // deal with overflow
                        uselessCounter = SLEEP_THRESHOLD + 1;
                    }

                    if (uselessCounter > YIELD_THRESHOLD) {
                        Thread.yield();
                    }

                    if (uselessCounter > SLEEP_THRESHOLD) {
                        LockSupport.parkNanos(1000000);
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

    @SuppressWarnings("UnusedReturnValue")
    private int loadFence() {
        return fence;
    }

    private void setupJobs() {
        if (running == 1) {
            for (int i = 0; i < jobs.size(); i++) {
                loadFence();
                try {
                    Job job = jobs.get(i);
                    if (job instanceof EagerThreadSetup) {
                        ((EagerThreadSetup) job).setup();
                    }
                } finally {
                    storeFence();
                }
            }
        }
    }

    private void storeFence() {
        fence = 1;
    }
}

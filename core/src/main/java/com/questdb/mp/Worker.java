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
import com.questdb.log.LogFactory;
import com.questdb.std.ObjHashSet;
import com.questdb.std.Os;
import com.questdb.std.Unsafe;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class Worker extends Thread {
    private static final Log LOG = LogFactory.getLog(Worker.class);

    private final static long RUNNING_OFFSET = Unsafe.getFieldOffset(Worker.class, "running");
    private static final long YIELD_THRESHOLD = 100000L;
    private static final long SLEEP_THRESHOLD = 10000000L;
    private final static AtomicInteger COUNTER = new AtomicInteger();
    private final ObjHashSet<? extends Job> jobs;
    private final CountDownLatch haltLatch;
    private final int affinity;
    @SuppressWarnings("FieldCanBeLocal")
    private volatile int running = 0;
    private volatile int fence;

    public Worker(ObjHashSet<? extends Job> jobs, CountDownLatch haltLatch) {
        this(jobs, haltLatch, -1);
    }

    public Worker(ObjHashSet<? extends Job> jobs, CountDownLatch haltLatch, int affinity) {
        this.jobs = jobs;
        this.haltLatch = haltLatch;
        this.setName("questdb-worker-" + COUNTER.incrementAndGet());
        this.affinity = affinity;
    }

    public void halt() {
        running = 2;
    }

    @Override
    public void run() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, RUNNING_OFFSET, 0, 1)) {
            setupJobs();
            if (affinity > -1) {
                if (Os.setCurrentThreadAffinity(this.affinity) == 0) {
                    LOG.info().$("affinity set [cpu=").$(affinity).$(", name=").$(getName()).$(']').$();
                } else {
                    LOG.error().$("could not set affinity [cpu=").$(affinity).$(", name=").$(getName()).$(']').$();
                }
            } else {
                LOG.info().$("os scheduled [name=").$(getName()).$(']').$();
            }
            int n = jobs.size();
            long uselessCounter = 0;
            while (running == 1) {

                boolean useful = false;
                for (int i = 0; i < n; i++) {
                    loadFence();
                    try {
                        useful |= jobs.get(i).run();
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
        haltLatch.countDown();
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
                    jobs.get(i).setupThread();
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

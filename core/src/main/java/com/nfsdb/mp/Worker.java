/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.mp;

import com.nfsdb.ex.FatalError;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.ObjHashSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

public class Worker extends Thread {
    private final static long RUNNING_OFFSET;
    private static final long YIELD_THRESHOLD = 100000L;
    private static final long SLEEP_THRESHOLD = 10000000L;
    private final ObjHashSet<? extends Job> jobs;
    private final CountDownLatch haltLatch;
    private final WorkerContext context = new WorkerContext();
    @SuppressWarnings("FieldCanBeLocal")
    private volatile int running = 0;

    public Worker(ObjHashSet<? extends Job> jobs, CountDownLatch haltLatch) {
        this.jobs = jobs;
        this.haltLatch = haltLatch;
    }

    public void halt() {
        running = 2;
    }

    @SuppressFBWarnings("MDM_THREAD_YIELD")
    @Override
    public void run() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, RUNNING_OFFSET, 0, 1)) {
            // acquire CPU lock
            int n = jobs.size();
            long uselessCounter = 0;
            while (running == 1) {

                boolean useful = false;
                for (int i = 0; i < n; i++) {
                    context.loadFence();
                    try {
                        useful |= jobs.get(i).run(context);
                    } finally {
                        context.storeFence();
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

    static {
        try {
            RUNNING_OFFSET = Unsafe.getUnsafe().objectFieldOffset(Worker.class.getDeclaredField("running"));
        } catch (NoSuchFieldException e) {
            throw new FatalError(e);
        }
    }
}

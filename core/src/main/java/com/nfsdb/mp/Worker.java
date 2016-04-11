/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

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
    @SuppressWarnings("FieldCanBeLocal")
    private volatile int running = 0;

    private volatile int fence;

    public Worker(ObjHashSet<? extends Job> jobs, CountDownLatch haltLatch) {
        this.jobs = jobs;
        this.haltLatch = haltLatch;
    }

    public void halt() {
        running = 2;
    }

    @SuppressFBWarnings({"MDM_THREAD_YIELD", "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"})
    @Override
    public void run() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, RUNNING_OFFSET, 0, 1)) {
            // acquire CPU lock
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

    private void storeFence() {
        fence = 1;
    }

    static {
        try {
            RUNNING_OFFSET = Unsafe.getUnsafe().objectFieldOffset(Worker.class.getDeclaredField("running"));
        } catch (NoSuchFieldException e) {
            throw new FatalError(e);
        }
    }
}

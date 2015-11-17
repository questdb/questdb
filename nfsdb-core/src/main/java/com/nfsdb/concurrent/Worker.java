/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.concurrent;

import com.nfsdb.collections.ObjList;
import com.nfsdb.utils.Unsafe;

import java.util.concurrent.CountDownLatch;

public class Worker extends Thread {
    private final static long RUNNING_OFFSET;

    private final ObjList<Runnable> jobs;
    private final CountDownLatch haltLatch;
    @SuppressWarnings("FieldCanBeLocal")
    private volatile int running = 0;

    public Worker(ObjList<Runnable> jobs, CountDownLatch haltLatch) {
        this.jobs = jobs;
        this.haltLatch = haltLatch;
    }

    public void halt() {
        Unsafe.getUnsafe().compareAndSwapInt(this, RUNNING_OFFSET, 1, 2);
    }

    @Override
    public void run() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, RUNNING_OFFSET, 0, 1)) {
            // acquire CPU lock
            int n = jobs.size();
            while (running == 1) {
                for (int i = 0; i < n; i++) {
                    jobs.getQuick(i).run();
                }
            }
        }
        haltLatch.countDown();
    }

    static {
        try {
            RUNNING_OFFSET = Unsafe.getUnsafe().objectFieldOffset(Worker.class.getDeclaredField("running"));
        } catch (NoSuchFieldException e) {
            throw new Error(e);
        }
    }
}

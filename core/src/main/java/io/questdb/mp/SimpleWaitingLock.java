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

import io.questdb.std.Os;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class SimpleWaitingLock {
    private final AtomicBoolean lock = new AtomicBoolean(false);
    private volatile Thread waiter = null;

    public boolean isLocked() {
        return lock.get();
    }

    public void lock() {
        this.waiter = Thread.currentThread();
        while (true) {
            while (lock.get()) {
                // Don't use LockSupport.park() here.
                // Once in a while there can be a delay between check of lock.get()
                // and parking and unlock() will be called before LockSupport.park().
                // Limit the parking time by using Os.park() instead of LockSupport.park()
                Os.park();
            }
            if (!lock.getAndSet(true)) {
                return;
            }
        }
    }

    public boolean tryLock(long timeout, TimeUnit unit) {
        if (tryLock()) {
            return true;
        }

        if (timeout <= 0L) {
            return false;
        }

        long nanos = unit.toNanos(timeout);
        this.waiter = Thread.currentThread();
        while (true) {
            long start = System.nanoTime();
            LockSupport.parkNanos(nanos);
            long elapsed = System.nanoTime() - start;

            if (elapsed < nanos) {
                if (tryLock()) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    public boolean tryLock() {
        return lock.compareAndSet(false, true);
    }

    public void unlock() {
        if (lock.compareAndSet(true, false)) {
            Thread waiter = this.waiter;
            this.waiter = null;
            if (waiter != null) {
                LockSupport.unpark(waiter);
            }
        } else {
            throw new IllegalStateException();
        }
    }
}

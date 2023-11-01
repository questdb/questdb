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
import io.questdb.std.Unsafe;

import java.util.concurrent.locks.LockSupport;

/**
 * Single owner count down latch. This latch is mutable and it does not actively
 */
public class SOCountDownLatch implements CountDownLatchSPI {
    private static final long VALUE_OFFSET;
    private volatile int count;
    private volatile Thread waiter;

    public SOCountDownLatch(int count) {
        this.count = count;
    }

    public SOCountDownLatch() {
        // no-op
    }

    public void await() {
        this.waiter = Thread.currentThread();
        while (getCount() > 0) {
            // Don't use LockSupport.park() here.
            // Once in a while there can be a delay between check of this.count > -count
            // and parking and unparkWaiter() will be called before park().
            // Limit the parking time by using Os.park() instead of LockSupport.park()
            Os.park();
        }
    }

    public boolean await(long nanos) {
        this.waiter = Thread.currentThread();
        if (getCount() == 0) {
            return true;
        }

        while (true) {
            long start = System.nanoTime();
            LockSupport.parkNanos(nanos);
            long elapsed = System.nanoTime() - start;

            if (elapsed < nanos) {
                if (getCount() == 0) {
                    return true;
                } else {
                    nanos -= elapsed;
                }
            } else {
                return getCount() == 0;
            }
        }
    }

    @Override
    public void countDown() {
        do {
            int current = getCount();

            if (current < 1) {
                break;
            }

            int next = current - 1;
            if (Unsafe.cas(this, VALUE_OFFSET, current, next)) {
                if (next == 0) {
                    unparkWaiter();
                }
                break;
            }
        } while (true);
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    private void unparkWaiter() {
        Thread waiter = this.waiter;
        if (waiter != null) {
            LockSupport.unpark(waiter);
        }
    }

    static {
        VALUE_OFFSET = Unsafe.getFieldOffset(SOCountDownLatch.class, "count");
    }
}

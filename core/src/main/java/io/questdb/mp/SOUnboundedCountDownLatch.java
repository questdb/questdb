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

import io.questdb.std.Unsafe;

import java.util.concurrent.locks.LockSupport;

/**
 * Single owner count down latch. This latch is mutable and it does not actively
 */
public class SOUnboundedCountDownLatch implements CountDownLatchSPI {
    private static final long COUNT_OFFSET;
    private volatile int awaitedCount;
    private volatile int count;
    private volatile Thread waiter;

    public SOUnboundedCountDownLatch() {
    }

    public void await(int count) {
        this.awaitedCount = count;
        this.waiter = Thread.currentThread();
        while (this.count > -count) {
            LockSupport.park();
        }
    }

    @Override
    public void countDown() {
        final int prevCount = Unsafe.getUnsafe().getAndAddInt(this, COUNT_OFFSET, -1);
        final int awaitedCount = this.awaitedCount;
        if ((prevCount - 1) <= -awaitedCount) {
            unparkWaiter();
        }
    }

    public boolean done(int count) {
        return this.count <= -count;
    }

    public int getCount() {
        return count;
    }

    public void reset() {
        count = 0;
        awaitedCount = 0;
        waiter = null;
    }

    private void unparkWaiter() {
        Thread waiter = this.waiter;
        if (waiter != null) {
            LockSupport.unpark(waiter);
        }
    }

    static {
        COUNT_OFFSET = Unsafe.getFieldOffset(SOUnboundedCountDownLatch.class, "count");
    }
}

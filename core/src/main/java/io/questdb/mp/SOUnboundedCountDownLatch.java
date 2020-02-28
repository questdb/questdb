/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
    private static final long VALUE_OFFSET;

    static {
        VALUE_OFFSET = Unsafe.getFieldOffset(SOUnboundedCountDownLatch.class, "count");
    }

    private volatile int count = 0;
    private volatile Thread waiter = null;
    private volatile int target = 0;

    public SOUnboundedCountDownLatch() {
    }

    public void await(int count) {
        this.target = -count;
        this.waiter = Thread.currentThread();
        while (this.count > -count) {
            LockSupport.park();
        }
    }

    @Override
    public void countDown() {
        do {
            int current = this.count;

            int next = current - 1;
            if (Unsafe.cas(this, VALUE_OFFSET, current, next)) {
                if (next <= target) {
                    unparkWaiter();
                }
                break;
            }
        } while (true);
    }

    public void reset() {
        count = 0;
    }

    private void unparkWaiter() {
        Thread waiter = this.waiter;
        if (waiter != null) {
            LockSupport.unpark(waiter);
        }
    }
}

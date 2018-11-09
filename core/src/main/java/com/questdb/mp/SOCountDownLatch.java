/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.std.Unsafe;

import java.util.concurrent.locks.LockSupport;

/**
 * Single owner count down latch. This latch is mutable and it does not actively
 */
public class SOCountDownLatch {
    private static final long VALUE_OFFSET;
    private volatile int count = 0;
    private volatile Thread waiter = null;

    public void await() {
        this.waiter = Thread.currentThread();
        while (getCount() > 0) {
            LockSupport.park();
        }
    }

    public boolean await(long nanos) {
        this.waiter = Thread.currentThread();
        if (getCount() == 0) {
            return true;
        }

        while (true) {
            long deadline = System.nanoTime() + nanos;
            LockSupport.parkNanos(nanos);

            if (System.nanoTime() < deadline) {
                // this could be spurious wakeup, ignore if count is non-zero
                if (getCount() == 0) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

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

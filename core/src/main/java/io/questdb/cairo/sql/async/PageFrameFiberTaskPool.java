/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cairo.sql.async;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicInteger;

final class PageFrameFiberTaskPool implements QuietCloseable {
    private volatile int capacity;
    private int createdCount;
    private final PageFrameReduceDispatcher dispatcher;
    private final CairoEngine engine;
    private int freeCount;
    private PageFrameFiberTask freeTasks;
    private volatile boolean isClosed;
    // Leases gate capacity without the pool monitor, so a worker that loses the queue claim never
    // serializes on it. An outstanding lease also keeps the pool from closing, which is what lets
    // acquireLeased() mint or pop unconditionally.
    private final AtomicInteger leasedCount = new AtomicInteger();
    private int maxRetainedCount;

    PageFrameFiberTaskPool(
            CairoEngine engine,
            int capacity,
            int maxRetainedCount,
            PageFrameReduceDispatcher dispatcher
    ) {
        if (capacity < 1) {
            throw new IllegalArgumentException("page frame fiber task capacity must be positive");
        }
        if (maxRetainedCount < 1 || maxRetainedCount > capacity) {
            throw new IllegalArgumentException(
                    "page frame fiber task retention must be positive and not exceed capacity"
            );
        }
        this.capacity = capacity;
        this.dispatcher = dispatcher;
        this.engine = engine;
        this.maxRetainedCount = maxRetainedCount;
    }

    @Override
    public void close() {
        final Throwable initialFailure;
        PageFrameFiberTask task;
        synchronized (this) {
            if (isClosed) {
                return;
            }
            isClosed = true;
            initialFailure = leasedCount.get() == 0
                    ? null
                    : new IllegalStateException(
                    "page frame fiber task pool closed with leased tasks [leased="
                    + leasedCount.get()
                    + ", created=" + createdCount
                    + ", free=" + freeCount
                    + ']'
            );
            task = freeTasks;
            freeTasks = null;
            createdCount -= freeCount;
            freeCount = 0;
        }
        Throwable failure = initialFailure;
        while (task != null) {
            final PageFrameFiberTask next = task.nextFree;
            task.isPooled = false;
            task.nextFree = null;
            failure = Misc.freeBestEffort(failure, task);
            task = next;
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    @TestOnly
    synchronized int getCapacity() {
        return capacity;
    }

    synchronized int getCreatedCount() {
        return createdCount;
    }

    @TestOnly
    synchronized int getMaxRetainedCount() {
        return maxRetainedCount;
    }

    boolean hasLeasedTasks() {
        return leasedCount.get() != 0;
    }

    void release(PageFrameFiberTask task) {
        boolean isFree = false;
        try {
            synchronized (this) {
                if (task.isPooled || createdCount <= freeCount) {
                    throw new IllegalStateException("page frame fiber task pool overflow");
                }
                if (isClosed
                        || freeCount >= maxRetainedCount
                        || task.getScheduleState() != FiberTask.STATE_IDLE) {
                    createdCount--;
                    isFree = true;
                } else {
                    task.isPooled = true;
                    task.nextFree = freeTasks;
                    freeTasks = task;
                    freeCount++;
                }
            }
        } finally {
            leasedCount.decrementAndGet();
        }
        if (isFree) {
            Misc.free(task);
        }
    }

    void releaseLease() {
        leasedCount.decrementAndGet();
    }

    boolean tryLease() {
        if (isClosed) {
            return false;
        }
        while (true) {
            final int current = leasedCount.get();
            if (current >= capacity) {
                return false;
            }
            if (leasedCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    @TestOnly
    synchronized void setFreeTaskScheduleStateForTesting(int expectedState, int targetState) {
        if (freeTasks == null) {
            throw new IllegalStateException("page frame fiber task pool has no free task");
        }
        freeTasks.setScheduleStateForTesting(expectedState, targetState);
    }

    synchronized PageFrameFiberTask acquireLeased() {
        if (freeTasks != null) {
            final PageFrameFiberTask task = freeTasks;
            freeTasks = task.nextFree;
            task.isPooled = false;
            task.nextFree = null;
            freeCount--;
            return task;
        }
        PageFrameFiberTask task = null;
        try {
            task = new PageFrameFiberTask(engine, this, dispatcher);
            createdCount++;
            return task;
        } catch (Throwable th) {
            Misc.free(task, th);
            throw th;
        }
    }

    void updateLimits(int capacity, int maxRetainedCount) {
        PageFrameFiberTask retiredTasks = null;
        synchronized (this) {
            if (isClosed) {
                return;
            }
            this.capacity = capacity;
            this.maxRetainedCount = maxRetainedCount;
            while (freeCount > maxRetainedCount) {
                final PageFrameFiberTask task = freeTasks;
                freeTasks = task.nextFree;
                task.isPooled = false;
                task.nextFree = retiredTasks;
                retiredTasks = task;
                createdCount--;
                freeCount--;
            }
        }
        while (retiredTasks != null) {
            final PageFrameFiberTask next = retiredTasks.nextFree;
            retiredTasks.nextFree = null;
            Misc.free(retiredTasks);
            retiredTasks = next;
        }
    }
}

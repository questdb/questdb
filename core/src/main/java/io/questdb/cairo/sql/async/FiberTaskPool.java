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

import io.questdb.cairo.CairoException;
import io.questdb.mp.ConcurrentPool;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class FiberTaskPool<T extends FiberTask & QuietCloseable> implements QuietCloseable {
    private static final long LEASE_COUNT_MASK = Long.MAX_VALUE;
    private static final long LEASE_OPEN = Long.MIN_VALUE;
    private final AtomicInteger createdCount = new AtomicInteger();
    private final Factory<T> factory;
    private final ConcurrentPool<T> freeTasks = new ConcurrentPool<>();
    // The high bit seals admission; the low bits count leases. One CAS therefore cannot cross close.
    private final AtomicLong leaseState = new AtomicLong(LEASE_OPEN);
    private volatile Runnable beforeNewTaskForTesting;
    private volatile int capacity;
    private volatile int maxRetainedCount;

    FiberTaskPool(int capacity, int maxRetainedCount, Factory<T> factory) {
        if (capacity < 1) {
            throw new IllegalArgumentException("fiber task capacity must be positive");
        }
        if (maxRetainedCount < 1 || maxRetainedCount > capacity) {
            throw new IllegalArgumentException(
                    "fiber task retention must be positive and not exceed capacity"
            );
        }
        this.capacity = capacity;
        this.factory = factory;
        this.maxRetainedCount = maxRetainedCount;
    }

    @Override
    public void close() {
        final long leasedCount;
        while (true) {
            final long current = leaseState.get();
            if ((current & LEASE_OPEN) == 0) {
                return;
            }
            if (leaseState.compareAndSet(current, current & LEASE_COUNT_MASK)) {
                leasedCount = current & LEASE_COUNT_MASK;
                break;
            }
        }
        Throwable failure = leasedCount == 0
                ? null
                : new IllegalStateException(
                "fiber task pool closed with leased tasks [leased="
                + leasedCount
                + ", created=" + createdCount.get()
                + ", free=" + freeTasks.count()
                + ']'
        );
        failure = freeRetainedTasks(0, failure);
        CairoException.rethrowCleanupFailure(failure);
    }

    private Throwable freeRetainedTasks(int maxCount, Throwable failure) {
        while (freeTasks.count() > maxCount) {
            final T task = freeTasks.pop();
            if (task == null) {
                break;
            }
            createdCount.decrementAndGet();
            failure = Misc.freeBestEffort(failure, task);
        }
        return failure;
    }

    private boolean isOpen() {
        return (leaseState.get() & LEASE_OPEN) != 0;
    }

    T acquireLeased() {
        final T pooledTask = freeTasks.pop();
        if (pooledTask != null) {
            return pooledTask;
        }
        T task = null;
        try {
            final Runnable hook = beforeNewTaskForTesting;
            if (hook != null) {
                beforeNewTaskForTesting = null;
                hook.run();
            }
            task = factory.newTask(this);
            createdCount.incrementAndGet();
            return task;
        } catch (Throwable th) {
            Misc.free(task, th);
            throw th;
        }
    }

    @TestOnly
    int getCapacity() {
        return capacity;
    }

    int getCreatedCount() {
        return createdCount.get();
    }

    @TestOnly
    int getMaxRetainedCount() {
        return maxRetainedCount;
    }

    boolean hasNoLeasedTasks() {
        return (leaseState.get() & LEASE_COUNT_MASK) == 0;
    }

    void release(T task) {
        Throwable failure = null;
        boolean isRetained = false;
        try {
            if (task.getScheduleState() == FiberTask.STATE_IDLE && isOpen()) {
                isRetained = freeTasks.tryPush(task, maxRetainedCount);
            }
        } catch (Throwable th) {
            failure = th;
        }
        if (isRetained) {
            failure = freeRetainedTasks(isOpen() ? maxRetainedCount : 0, failure);
        } else {
            createdCount.decrementAndGet();
            failure = Misc.freeBestEffort(failure, task);
        }
        try {
            releaseLease();
        } catch (Throwable th) {
            if (failure == null) {
                failure = th;
            } else if (failure != th) {
                failure.addSuppressed(th);
            }
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    void releaseLease() {
        while (true) {
            final long current = leaseState.get();
            if ((current & LEASE_COUNT_MASK) == 0) {
                throw new IllegalStateException("fiber task lease underflow");
            }
            if (leaseState.compareAndSet(current, current - 1)) {
                return;
            }
        }
    }

    // a pooled task holds its pool as FiberTaskPool<?>; the factory is the only task producer
    @SuppressWarnings("unchecked")
    void releaseSelf(FiberTask task) {
        release((T) task);
    }

    @TestOnly
    void setBeforeNewTaskForTesting(Runnable hook) {
        beforeNewTaskForTesting = hook;
    }

    @TestOnly
    void setFreeTaskScheduleStateForTesting(int expectedState, int targetState) {
        final T task = freeTasks.pop();
        if (task == null) {
            throw new IllegalStateException("fiber task pool has no free task");
        }
        try {
            task.setScheduleStateForTesting(expectedState, targetState);
        } finally {
            freeTasks.push(task);
        }
    }

    boolean tryLease() {
        while (true) {
            final long current = leaseState.get();
            if ((current & LEASE_OPEN) == 0 || (current & LEASE_COUNT_MASK) >= capacity) {
                return false;
            }
            if (leaseState.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    void updateLimits(int capacity, int maxRetainedCount) {
        if (!isOpen()) {
            return;
        }
        this.capacity = capacity;
        this.maxRetainedCount = maxRetainedCount;
        final Throwable failure = freeRetainedTasks(isOpen() ? maxRetainedCount : 0, null);
        CairoException.rethrowCleanupFailure(failure);
    }

    interface Factory<T extends FiberTask & QuietCloseable> {
        T newTask(FiberTaskPool<T> pool);
    }
}

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
import io.questdb.mp.continuation.FiberTask;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

final class QueryParallelFiberTaskPool<T extends AbstractQueryParallelFiberTask> implements QuietCloseable {
    private Runnable beforeNewTaskForTesting;
    private int capacity;
    private int createdCount;
    private final Factory<T> factory;
    private int freeCount;
    private T freeTasks;
    private boolean closed;
    private int maxRetainedCount;

    QueryParallelFiberTaskPool(
            int capacity,
            int maxRetainedCount,
            Factory<T> factory
    ) {
        if (capacity < 1) {
            throw new IllegalArgumentException("query parallel fiber task capacity must be positive");
        }
        if (maxRetainedCount < 1 || maxRetainedCount > capacity) {
            throw new IllegalArgumentException(
                    "query parallel fiber task retention must be positive and not exceed capacity"
            );
        }
        this.capacity = capacity;
        this.factory = factory;
        this.maxRetainedCount = maxRetainedCount;
    }

    @Override
    public void close() {
        final Throwable initialFailure;
        T task;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            initialFailure = freeCount == createdCount
                    ? null
                    : new IllegalStateException(
                    "query parallel fiber task pool closed with leased tasks [created="
                    + createdCount
                    + ", free="
                    + freeCount
                    + ']'
            );
            task = freeTasks;
            freeTasks = null;
            createdCount -= freeCount;
            freeCount = 0;
        }
        Throwable failure = initialFailure;
        while (task != null) {
            @SuppressWarnings("unchecked") final T next = (T) task.nextFree;
            task.pooled = false;
            task.nextFree = null;
            failure = Misc.freeBestEffort(failure, task);
            task = next;
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    synchronized boolean hasNoLeasedTasks() {
        return createdCount == freeCount;
    }

    synchronized int getCreatedCount() {
        return createdCount;
    }

    synchronized T tryAcquire() {
        if (closed) {
            return null;
        }

        if (freeTasks != null) {
            final T task = freeTasks;
            @SuppressWarnings("unchecked") final T next = (T) task.nextFree;
            freeTasks = next;
            task.pooled = false;
            task.nextFree = null;
            freeCount--;
            return task;
        }

        if (createdCount >= capacity) {
            return null;
        }
        T task = null;
        try {
            final Runnable hook = beforeNewTaskForTesting;
            beforeNewTaskForTesting = null;
            if (hook != null) {
                hook.run();
            }
            task = factory.newTask(this);
            createdCount++;
            return task;
        } catch (Throwable th) {
            Misc.free(task, th);
            throw th;
        }
    }

    synchronized void setBeforeNewTaskForTesting(Runnable hook) {
        beforeNewTaskForTesting = hook;
    }

    void updateLimits(int capacity, int maxRetainedCount) {
        T retiredTasks = null;
        synchronized (this) {
            if (closed) {
                return;
            }
            this.capacity = capacity;
            this.maxRetainedCount = maxRetainedCount;
            while (freeCount > maxRetainedCount) {
                final T task = freeTasks;
                @SuppressWarnings("unchecked") final T next = (T) task.nextFree;
                freeTasks = next;
                task.pooled = false;
                task.nextFree = retiredTasks;
                retiredTasks = task;
                freeCount--;
                createdCount--;
            }
        }
        while (retiredTasks != null) {
            final T task = retiredTasks;
            @SuppressWarnings("unchecked") final T next = (T) task.nextFree;
            retiredTasks = next;
            task.nextFree = null;
            Misc.free(task);
        }
    }

    void release(AbstractQueryParallelFiberTask task) {
        boolean free = false;
        synchronized (this) {
            if (task.pooled || createdCount <= freeCount) {
                throw new IllegalStateException("query parallel fiber task pool overflow");
            }
            if (closed
                    || freeCount >= maxRetainedCount
                    || task.getScheduleState() != FiberTask.STATE_IDLE) {
                createdCount--;
                free = true;
            } else {
                @SuppressWarnings("unchecked") final T typedTask = (T) task;
                typedTask.pooled = true;
                typedTask.nextFree = freeTasks;
                freeTasks = typedTask;
                freeCount++;
            }
        }
        if (free) {
            Misc.free(task);
        }
    }

    interface Factory<T extends AbstractQueryParallelFiberTask> {
        T newTask(QueryParallelFiberTaskPool<T> pool);
    }
}

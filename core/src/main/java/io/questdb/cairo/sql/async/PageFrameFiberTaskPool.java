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

final class PageFrameFiberTaskPool implements QuietCloseable {
    private final int capacity;
    private int createdCount;
    private final PageFrameReduceDispatcher dispatcher;
    private final CairoEngine engine;
    private int freeCount;
    private PageFrameFiberTask freeTasks;
    private boolean isClosed;

    PageFrameFiberTaskPool(
            CairoEngine engine,
            int capacity,
            PageFrameReduceDispatcher dispatcher
    ) {
        if (capacity < 1) {
            throw new IllegalArgumentException("page frame fiber task capacity must be positive");
        }
        this.capacity = capacity;
        this.dispatcher = dispatcher;
        this.engine = engine;
    }

    @Override
    public void close() {
        final Throwable failure;
        synchronized (this) {
            if (isClosed) {
                return;
            }
            isClosed = true;
            Throwable cleanupFailure = freeCount == createdCount
                    ? null
                    : new IllegalStateException(
                    "page frame fiber task pool closed with leased tasks [created="
                    + createdCount
                    + ", free=" + freeCount
                    + ']'
            );
            PageFrameFiberTask task = freeTasks;
            freeTasks = null;
            createdCount -= freeCount;
            freeCount = 0;
            while (task != null) {
                final PageFrameFiberTask next = task.nextFree;
                task.isPooled = false;
                task.nextFree = null;
                cleanupFailure = Misc.freeBestEffort(cleanupFailure, task);
                task = next;
            }
            failure = cleanupFailure;
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    synchronized int getCreatedCount() {
        return createdCount;
    }

    synchronized boolean hasLeasedTasks() {
        return createdCount != freeCount;
    }

    synchronized void release(PageFrameFiberTask task) {
        if (task.isPooled || createdCount <= freeCount) {
            throw new IllegalStateException("page frame fiber task pool overflow");
        }
        if (isClosed || task.getScheduleState() != FiberTask.STATE_IDLE) {
            createdCount--;
            Misc.free(task);
            return;
        }
        task.isPooled = true;
        task.nextFree = freeTasks;
        freeTasks = task;
        freeCount++;
    }

    @TestOnly
    synchronized void setFreeTaskScheduleStateForTesting(int expectedState, int targetState) {
        if (freeTasks == null) {
            throw new IllegalStateException("page frame fiber task pool has no free task");
        }
        freeTasks.setScheduleStateForTesting(expectedState, targetState);
    }

    synchronized PageFrameFiberTask tryAcquire() {
        if (isClosed) {
            return null;
        }
        if (freeTasks != null) {
            final PageFrameFiberTask task = freeTasks;
            freeTasks = task.nextFree;
            task.isPooled = false;
            task.nextFree = null;
            freeCount--;
            return task;
        }
        if (createdCount >= capacity) {
            return null;
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
}

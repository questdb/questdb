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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

final class PageFrameFiberTaskPool implements QuietCloseable {
    private final ObjList<PageFrameFiberTask> allTasks = new ObjList<>();
    private final int capacity;
    private int createdCount;
    private final CairoEngine engine;
    private final ObjList<PageFrameFiberTask> freeTasks = new ObjList<>();
    private boolean isClosed;

    PageFrameFiberTaskPool(CairoEngine engine, int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("page frame fiber task capacity must be positive");
        }
        this.capacity = capacity;
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
            Throwable cleanupFailure = freeTasks.size() == createdCount
                    ? null
                    : new IllegalStateException(
                            "page frame fiber task pool closed with leased tasks [created="
                                    + createdCount
                                    + ", free=" + freeTasks.size()
                                    + ']'
                    );
            cleanupFailure = Misc.freeObjListIfCloseableBestEffort(cleanupFailure, allTasks);
            allTasks.clear();
            freeTasks.clear();
            failure = cleanupFailure;
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    synchronized int getCreatedCount() {
        return createdCount;
    }

    synchronized void release(PageFrameFiberTask task) {
        if (isClosed) {
            throw new IllegalStateException("page frame fiber task pool is closed");
        }
        if (freeTasks.size() >= createdCount) {
            throw new IllegalStateException("page frame fiber task pool overflow");
        }
        freeTasks.add(task);
    }

    synchronized PageFrameFiberTask tryAcquire() {
        if (isClosed) {
            return null;
        }
        if (freeTasks.size() > 0) {
            return freeTasks.popLast();
        }
        if (createdCount >= capacity) {
            return null;
        }
        PageFrameFiberTask task = null;
        try {
            task = new PageFrameFiberTask(engine, this);
            allTasks.add(task);
            createdCount++;
            return task;
        } catch (Throwable th) {
            Misc.free(task, th);
            throw th;
        }
    }
}

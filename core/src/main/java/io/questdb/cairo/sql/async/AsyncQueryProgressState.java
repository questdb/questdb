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

import io.questdb.mp.continuation.FiberEventWaitQueue;
import io.questdb.mp.continuation.FiberWaitCoordinator;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-query progress signal for {@link QueryParallelFiberDispatcher} owners. The owner is the
 * queue's only waiter, so a completed task wakes exactly the query it belongs to instead of
 * whichever owner sits at the head of the dispatcher's shared queue.
 */
public final class AsyncQueryProgressState {
    private final AtomicLong version = new AtomicLong();
    private final FiberEventWaitQueue waitQueue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);

    public long getVersion() {
        return version.get();
    }

    FiberEventWaitQueue getWaitQueue() {
        return waitQueue;
    }

    void signalProgress() {
        version.incrementAndGet();
        waitQueue.fire();
    }
}

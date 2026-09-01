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

package io.questdb.mp.continuation;

import io.questdb.mp.ConcurrentQueue;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.TestOnly;

/**
 * Growable MPMC queue of {@link Fiber} references. The logical Fiber limits bound the number of
 * entries; segment slots are allocated eagerly, so the startup capacity is clamped and linked
 * queue segments serve any excess.
 */
final class FiberRing {
    private static final int MAX_CAPACITY = 1 << 30;
    private static final int MAX_EAGER_CAPACITY = 1 << 20;
    private static final int MIN_CAPACITY = 32;
    private final ConcurrentQueue<Fiber> queue;
    @TestOnly
    private volatile int forcedDepth = -1;

    FiberRing(int initialCapacity) {
        if (initialCapacity < 1 || initialCapacity > MAX_CAPACITY) {
            throw new IllegalArgumentException("initialCapacity is out of range [value=" + initialCapacity + ']');
        }

        queue = ConcurrentQueue.createConcurrentObjectQueue(
                Numbers.ceilPow2(Math.min(MAX_EAGER_CAPACITY, Math.max(MIN_CAPACITY, initialCapacity)))
        );
    }

    int capacity() {
        return queue.capacity();
    }

    int depth() {
        final int depth = forcedDepth;
        return depth > -1 ? depth : queue.getApproximateCount();
    }

    boolean hasAvailable() {
        return queue.hasAvailable();
    }

    void put(Fiber fiber) {
        if (fiber == null) {
            throw new IllegalArgumentException("fiber must not be null");
        }
        final int forcedDepth = this.forcedDepth;
        if (forcedDepth > -1 && forcedDepth >= capacity()) {
            throw new IllegalStateException("fiber ring is full");
        }
        queue.enqueue(fiber);
    }

    @TestOnly
    void setDepthForTesting(int depth) {
        if (depth < 0 || depth > capacity()) {
            throw new IllegalArgumentException("depth is out of range [value=" + depth + ']');
        }
        forcedDepth = depth == 0 ? -1 : depth;
    }

    Fiber tryDequeue() {
        return queue.tryDequeueValue(null);
    }
}

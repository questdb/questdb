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

import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import org.jetbrains.annotations.TestOnly;

/**
 * Bounded lock-free MPMC ring of {@link Fiber} references. This is the scheduler's hot queue:
 * every launch and wake enqueues here and every carrier tick dequeues, so it must not funnel all
 * workers through one monitor. Capacity covers the pool's live-fiber limit and a fiber is enqueued
 * at most once (guarded by {@code Fiber.notificationState}), so a full ring indicates an accounting
 * bug rather than backpressure.
 */
final class FiberRing {
    private static final int MAX_CAPACITY = 1 << 30;
    private static final int MIN_CAPACITY = 32;
    private final RingQueue<Holder> buffer;
    private final int capacity;
    @TestOnly
    private volatile boolean isFullForTesting;
    private final MPSequence pubSeq;
    private final MCSequence subSeq;

    FiberRing(int initialCapacity) {
        if (initialCapacity < 1 || initialCapacity > MAX_CAPACITY) {
            throw new IllegalArgumentException("initialCapacity is out of range [value=" + initialCapacity + ']');
        }
        this.capacity = Numbers.ceilPow2(Math.max(MIN_CAPACITY, initialCapacity));
        this.buffer = new RingQueue<>(Holder::new, capacity);
        this.pubSeq = new MPSequence(capacity);
        this.subSeq = new MCSequence(capacity);
        pubSeq.then(subSeq).then(pubSeq);
    }

    int capacity() {
        return capacity;
    }

    int depth() {
        // racy by design: transiently overcounts by in-flight publications
        return (int) (pubSeq.current() - subSeq.current());
    }

    void put(Fiber fiber) {
        if (fiber == null) {
            throw new IllegalArgumentException("fiber must not be null");
        }
        if (isFullForTesting) {
            throw new IllegalStateException("fiber ring is full");
        }
        while (true) {
            final long cursor = pubSeq.next();
            if (cursor > -1) {
                buffer.get(cursor).fiber = fiber;
                pubSeq.done(cursor);
                return;
            }
            // -1 also covers a consumer that claimed but has not released a slot yet; only a ring
            // genuinely holding capacity entries is the accounting-bug throw
            if (cursor == -1 && depth() >= capacity) {
                throw new IllegalStateException("fiber ring is full");
            }
            Os.pause();
        }
    }

    @TestOnly
    void setDepthForTesting(int depth) {
        if (depth < 0 || depth > capacity) {
            throw new IllegalArgumentException("depth is out of range [value=" + depth + ']');
        }
        isFullForTesting = depth == capacity;
    }

    Fiber tryDequeue() {
        while (true) {
            final long cursor = subSeq.next();
            if (cursor > -1) {
                final Holder holder = buffer.get(cursor);
                final Fiber fiber = holder.fiber;
                holder.fiber = null;
                subSeq.done(cursor);
                return fiber;
            }
            if (cursor == -1) {
                return null;
            }
            Os.pause();
        }
    }

    private static final class Holder {
        private Fiber fiber;
    }
}

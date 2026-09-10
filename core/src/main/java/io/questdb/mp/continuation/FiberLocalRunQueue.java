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

import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Fixed-capacity single-producer, multiple-consumer queue used by one Fiber-runtime owner.
 * Consumers share the same head-claim path, including the owner, which keeps stealing and
 * wrap-around correctness in one protocol.
 */
final class FiberLocalRunQueue {
    static final int MAX_CAPACITY = 256;
    static final int MIN_CAPACITY = 2;
    private static final long HEAD_OFFSET = Unsafe.getFieldOffset(FiberLocalRunQueue.class, "head");
    private static final VarHandle TAIL;
    private final int capacity;
    private final int mask;
    private final long[] sequences;
    private final ObjList<Fiber> values;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long head;
    // Single-writer scheduler field. Opaque access gives diagnostic readers a coherent, possibly
    // stale snapshot without making this a second publication barrier beside the cell sequence.
    private long tail;

    FiberLocalRunQueue(int capacity) {
        if (capacity < MIN_CAPACITY
                || capacity > MAX_CAPACITY
                || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("local Fiber queue capacity must be a power of two [capacity="
                    + capacity + ']');
        }
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.sequences = new long[capacity];
        this.values = new ObjList<>(capacity);
        for (int i = 0; i < capacity; i++) {
            sequences[i] = i;
            values.add(null);
        }
    }

    static int calculateCapacity(int initialMaxLiveCount, int workerCount) {
        if (initialMaxLiveCount < 1) {
            throw new IllegalArgumentException("initialMaxLiveCount must be positive");
        }
        if (workerCount < 1) {
            throw new IllegalArgumentException("workerCount must be positive");
        }
        final long fairShare = ((long) initialMaxLiveCount + workerCount - 1L) / workerCount;
        final long target = Math.min(initialMaxLiveCount, Math.min(Integer.MAX_VALUE, fairShare * 2L));
        final int bounded = (int) Math.max(MIN_CAPACITY, Math.min(MAX_CAPACITY, target));
        int powerOfTwo = MIN_CAPACITY;
        while (powerOfTwo < bounded) {
            powerOfTwo <<= 1;
        }
        return powerOfTwo;
    }

    int capacity() {
        return capacity;
    }

    int depth() {
        final long tailSnapshot = (long) TAIL.getOpaque(this);
        final long headSnapshot = head;
        final long distance = tailSnapshot - headSnapshot;
        if (distance <= 0) {
            return 0;
        }
        return distance >= capacity ? capacity : (int) distance;
    }

    boolean hasAvailable() {
        while (true) {
            final long currentHead = head;
            final int index = (int) currentHead & mask;
            final long sequence = Unsafe.arrayGetVolatile(sequences, index);
            if (sequence == currentHead + 1) {
                return true;
            }
            if (head == currentHead) {
                return false;
            }
        }
    }

    /**
     * Called only by the queue's fixed owner. A false result leaves the Fiber unpublished and
     * allows the caller to fall back to the global queue.
     */
    boolean offer(Fiber fiber) {
        if (fiber == null) {
            throw new IllegalArgumentException("fiber must not be null");
        }
        final long currentTail = (long) TAIL.getOpaque(this);
        final int index = (int) currentTail & mask;
        if (Unsafe.arrayGetVolatile(sequences, index) != currentTail) {
            return false;
        }
        values.setQuick(index, fiber);
        // This is private to the producer. The sequence release below is the publication point.
        TAIL.setOpaque(this, currentTail + 1);
        Unsafe.arrayPutOrdered(sequences, index, currentTail + 1);
        return true;
    }

    Fiber tryDequeue() {
        while (true) {
            final long currentHead = head;
            final int index = (int) currentHead & mask;
            final long sequence = Unsafe.arrayGetVolatile(sequences, index);
            if (sequence == currentHead + 1) {
                if (!Unsafe.cas(this, HEAD_OFFSET, currentHead, currentHead + 1)) {
                    continue;
                }
                final Fiber fiber = values.getQuick(index);
                if (fiber == null) {
                    throw new IllegalStateException("committed local Fiber queue cell is empty");
                }
                values.setQuick(index, null);
                Unsafe.arrayPutOrdered(sequences, index, currentHead + capacity);
                return fiber;
            }
            if (head == currentHead) {
                return null;
            }
        }
    }

    @TestOnly
    boolean claimHeadForTesting(long expectedHead) {
        final int index = (int) expectedHead & mask;
        return head == expectedHead
                && Unsafe.arrayGetVolatile(sequences, index) == expectedHead + 1
                && Unsafe.cas(this, HEAD_OFFSET, expectedHead, expectedHead + 1);
    }

    @TestOnly
    void initializeEmptyPositionForTesting(long position) {
        if (hasAvailable() || head != (long) TAIL.getOpaque(this)) {
            throw new IllegalStateException("local Fiber queue must be empty before changing its position");
        }
        head = position;
        TAIL.setOpaque(this, position);
        final int positionIndex = (int) position & mask;
        for (int i = 0; i < capacity; i++) {
            values.setQuick(i, null);
            sequences[i] = position + ((i - positionIndex) & mask);
        }
    }

    @TestOnly
    Fiber releaseClaimForTesting(long claimedHead) {
        final int index = (int) claimedHead & mask;
        if (Unsafe.arrayGetVolatile(sequences, index) != claimedHead + 1) {
            throw new IllegalStateException("local Fiber queue claim is not committed");
        }
        final Fiber fiber = values.getQuick(index);
        if (fiber == null) {
            throw new IllegalStateException("committed local Fiber queue cell is empty");
        }
        values.setQuick(index, null);
        Unsafe.arrayPutOrdered(sequences, index, claimedHead + capacity);
        return fiber;
    }

    static {
        try {
            TAIL = MethodHandles.lookup().findVarHandle(FiberLocalRunQueue.class, "tail", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}

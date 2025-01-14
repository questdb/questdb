/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.mp;

import io.questdb.std.ObjectFactory;

// This is stripped down version taken from the .NET Core source code at
// https://github.com/dotnet/runtime/blob/main/src/libraries/System.Private.CoreLib/src/System/Collections/Concurrent/ConcurrentQueue.cs
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

/***
 * A thread-safe first in-first out (FIFO) unlimited concurrent queue. This type of queue is
 * more convenient to use for notifications in some situations instead of using a fixed-size
 * RingQueue with Sequencers. These are the cases when a limited number of items can be
 * produced, like notifications to rebuild a materialized view, or apply WAL commits to a
 * table. Given that there is an external deduplication mechanism, those queues are limited to
 * the number of tables in the database that can be quite large, but still considered to be in
 * thousands rather than millions.
 */
public class ConcurrentQueue<T extends ValueHolder<T>> {
    // This implementation provides an unbounded, multi-producer multi-consumer queue that
    // supports the standard Enqueue/TryDequeue operations. It is composed of a linked list of
    // bounded ring buffers, each of which has a head and a tail index, isolated from each other
    // to avoid false sharing. As long as the number of elements in the queue remains less than
    // the size of the current buffer (Segment), no additional allocations are required for
    // enqueued items. When the number of items exceeds the size of the current segment, the
    // current segment is "frozen" to prevent further enqueues, and a new segment is linked from
    // it and set as the new tail segment for subsequent enqueues. As old segments are consumed
    // by dequeues, the head reference is updated to point to the segment that dequeuers should
    // try next. When an old segment is empty, it is discarded and becomes garbage.

    // Initial length of the segments used in the queue.
    private static final int INITIAL_SEGMENT_LENGTH = 32;

    // Maximum length of the segments used in the queue. This is a somewhat arbitrary limit:
    // larger means that as long as we don't exceed the size, we avoid allocating more segments,
    // but if we do exceed it, the segment becomes garbage.
    private static final int MAX_SEGMENT_LENGTH = 1024 * 1024;

    // Lock used to protect cross-segment operations, including any updates to "tail" or "head"
    // and any operations that need to get a consistent view of them.
    private final Object crossSegmentLock = new Object();
    private final ObjectFactory<T> factory;
    // The current head segment.
    private volatile ConcurrentQueueSegment<T> head;
    // The current tail segment.
    private volatile ConcurrentQueueSegment<T> tail;

    /**
     * Initializes a new instance of the ConcurrentQueue class with default initial capacity.
     *
     * @param factory The factory to use to create new items.
     */
    public ConcurrentQueue(ObjectFactory<T> factory) {
        this(factory, INITIAL_SEGMENT_LENGTH);
    }


    /**
     * Initializes a new instance of the ConcurrentQueue class with the specified initial capacity.
     *
     * @param factory The factory to use to create new items.
     * @param size    The initial capacity of the queue, must be a power of 2.
     */
    public ConcurrentQueue(ObjectFactory<T> factory, int size) {
        assert (size & (size - 1)) == 0; // must be a power of 2
        this.factory = factory;
        tail = head = new ConcurrentQueueSegment<>(factory, INITIAL_SEGMENT_LENGTH);
    }

    /**
     * Gets the number of items in the last segment of the queue. Useful for debugging.
     *
     * @return The largest segment size of the queue.
     */
    public int capacity() {
        return tail.getCapacity();
    }

    /**
     * Gets the number of items in the queue.
     *
     * @param item The object to add to the end of the "ConcurrentQueue".
     */
    public void enqueue(T item) {
        // Try to enqueue to the current tail.
        if (!tail.tryEnqueue(item)) {
            // If we're unable to, we need to take a slow path that will
            // try to add a new tail segment.
            enqueueSlow(item);
        }
    }

    public ObjectFactory<T> itemFactory() {
        return factory;
    }

    /**
     * Attempts to remove and return the object at the beginning of the ConcurrentQueue.
     *
     * @param result When this method returns, if the operation was successful, param "result"
     *               contains the object removed. If no object was available to be removed, the value is unspecified.
     * @return true if an element was removed and returned from the beginning of the ConcurrentQueue successfully;
     * otherwise, false.
     */
    public boolean tryDequeue(T result) {
        // Get the current head
        ConcurrentQueueSegment<T> head = this.head;

        // Try to take. If we're successful, we're done.
        if (head.tryDequeue(result)) {
            return true;
        }

        // Check to see whether this segment is the last. If it is, we can consider
        // this to be a moment-in-time empty condition (even though between the tryDequeue
        // check and this check, another item could have arrived).
        if (head.nextSegment == null) {
            return false;
        }

        // slow path that needs to fix up segments
        return tryDequeueSlow(result);
    }

    // Adds to the end of the queue, adding a new segment if necessary.
    private void enqueueSlow(T item) {
        while (true) {
            ConcurrentQueueSegment<T> tail = this.tail;

            // Try to append to the existing tail.
            if (tail.tryEnqueue(item)) {
                return;
            }

            // If we were unsuccessful, take the lock so that we can compare and manipulate the tail. Assuming another
            // enqueuer hasn't already added a new segment, do so, then loop around to try enqueueing again.
            synchronized (crossSegmentLock) {
                if (tail == this.tail) {
                    // Make sure no one else can enqueue to this segment.
                    tail.ensureFrozenForEnqueues();

                    // We determine the new segment's length based on the old length.
                    // In general, we double the size of the segment, to make it less likely
                    // that we'll need to grow again.
                    int nextSize = Math.min(tail.getCapacity() * 2, MAX_SEGMENT_LENGTH);
                    ConcurrentQueueSegment<T> newTail = new ConcurrentQueueSegment<>(factory, nextSize);

                    // Hook up the new tail.
                    tail.nextSegment = newTail;
                    this.tail = newTail;
                }
            }
        }
    }

    // Tries to dequeue an item, removing empty segments as needed.
    private boolean tryDequeueSlow(T item) {
        while (true) {
            // Get the current head
            ConcurrentQueueSegment<T> head = this.head;

            // Try to take. If we're successful, we're done.
            if (head.tryDequeue(item)) {
                return true;
            }

            // Check to see whether this segment is the last. If it is, we can consider
            // this to be a moment-in-time empty condition (even though between the tryDequeue
            // check and this check, another item could have arrived).
            if (head.nextSegment == null) {
                return false;
            }

            // At this point we know that head.Next != null, which means this segment has been
            // frozen for additional enqueues. But between the time that we ran tryDequeue and
            // checked for a next segment, another item could have been added. Try to dequeue
            // one more time to confirm that the segment is indeed empty.
            assert head.frozenForEnqueues;
            if (head.tryDequeue(item)) {
                return true;
            }

            // This segment is frozen (nothing more can be added) and empty (nothing is in it).
            // Update head to point to the next segment in the list, assuming no one's beat us to it.
            synchronized (crossSegmentLock) {
                if (head == this.head) {
                    this.head = head.nextSegment;
                }
            }
        }
    }
}

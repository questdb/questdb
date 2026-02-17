/*******************************************************************************
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

package io.questdb.mp;

import io.questdb.std.ObjectFactory;
import io.questdb.std.Os;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

// This is a stripped down version of the .NET Core source code at
// https://github.com/dotnet/runtime/blob/main/src/libraries/System.Private.CoreLib/src/System/Collections/Concurrent/ConcurrentQueueSegment.cs
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

/**
 * Provides a multi-producer, multi-consumer thread-safe bounded segment. When the queue is full,
 * enqueues fail and return false. When the queue is empty, dequeues fail and return null.
 * These segments are linked together to form the unbounded "ConcurrentQueue".
 */
final class ConcurrentQueueSegment<T> {
    // Segment design is inspired by the algorithm outlined at:
    // http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue

    private static final AtomicLongFieldUpdater<PaddedHeadAndTail> HEAD =
            AtomicLongFieldUpdater.newUpdater(PaddedHeadAndTail.class, "head");
    private static final AtomicLongFieldUpdater<PaddedHeadAndTail> TAIL =
            AtomicLongFieldUpdater.newUpdater(PaddedHeadAndTail.class, "tail");
    private final ObjectFactory<T> factory;

    private final int freezeOffset;
    // The head and tail positions, with padding to help avoid false sharing contention.
    // Dequeuing happens from the head, enqueuing happens at the tail.
    private final PaddedHeadAndTail headAndTail = new PaddedHeadAndTail();
    private final ConcurrentSegmentManipulator<T> queueManipulator;
    // The array of items in this queue. Each slot contains the item in that slot and its "sequence number".
    private final Slot<T>[] slots;
    // Mask for quickly accessing a position within the queue's array.
    private final long slotsMask;
    // flag used to ensure we don't increase the tail more than once if frozen more than once
    volatile boolean frozenForEnqueues;
    // The segment following this one in the queue, or null if this segment is the last in the queue.
    ConcurrentQueueSegment<T> nextSegment;

    /**
     * Initializes a new instance of the "ConcurrentQueueSegment" class.
     *
     * @param factory       The factory to use to create new items.
     * @param boundedLength The maximum number of elements the segment can contain. Must be a power of 2.
     */
    @SuppressWarnings("unchecked")
    ConcurrentQueueSegment(ObjectFactory<T> factory, ConcurrentSegmentManipulator<T> queueManipulator, int boundedLength) {
        this.queueManipulator = queueManipulator;
        // Initialize the slots and the mask. The mask is used as a way of quickly doing "% slots.Length",
        // instead letting us do "& slotsMask".
        slots = new Slot[boundedLength];
        slotsMask = boundedLength - 1;
        freezeOffset = slots.length * 2;

        // Initialize the sequence number for each slot. The sequence number provides a ticket that
        // allows dequeuers to know whether they can dequeue and enqueuers to know whether they can
        // enqueue. An enqueuer at position N can enqueue when the sequence number is N, and a dequeuer
        // for position N can dequeue when the sequence number is N + 1. When an enqueuer is done writing
        // at position N, it sets the sequence number to N + 1 so that a dequeuer will be able to dequeue,
        // and when a dequeuer is done dequeueing at position N, it sets the sequence number to N + slots.Length,
        // so that when an enqueuer loops around the slots, it'll find that the sequence number at
        // position N is N. This also means that when an enqueuer finds that at position N the sequence
        // number is < N, there is still a value in that slot, i.e. the segment is full, and when a
        // dequeuer finds that the value in a slot is < N + 1, there is nothing currently available to
        // dequeue. (It is possible for multiple enqueuers to enqueue concurrently, writing into
        // subsequent slots, and to have the first enqueuer take longer, so that the slots for 1, 2, 3, etc.
        // may have values, but the 0th slot may still be being filled... in that case, TryDequeue will
        // return false.)

        this.factory = factory;
        for (int i = 0; i < slots.length; i++) {
            slots[i] = new Slot<>();
            slots[i].item = createSlotItem();
            slots[i].sequenceNumber = i;
        }
    }

    /**
     * Gets the capacity of the segment.
     *
     * @return The capacity of the segment.
     */
    public int getCapacity() {
        return slots.length;
    }

    /**
     * Attempts to dequeue an element from the queue.
     *
     * @param target the item holder to dequeue into
     * @return the element that was dequeued, or null if no element was dequeued.
     */
    public T tryDequeue(T target) {
        Slot<T>[] slots = this.slots;

        // Loop in case of contention...
        while (true) {
            // Get the head at which to try to dequeue.
            long currentHead = headAndTail.head;
            int slotsIndex = (int) (currentHead & slotsMask);

            // Read the sequence number for the head position.
            long sequenceNumber = slots[slotsIndex].sequenceNumber;

            // We can dequeue from this slot if it's been filled by an enqueuer, which
            // would have left the sequence number at pos+1.
            long diff = sequenceNumber - (currentHead + 1);
            if (diff == 0) {
                // We may be racing with other dequeuers. Try to reserve the slot by incrementing
                // the head. Once we've done that, no one else will be able to read from this slot,
                // and no enqueuer will be able to read from this slot until we've written the new
                // sequence number.
                if (HEAD.compareAndSet(headAndTail, currentHead, currentHead + 1)) {
                    // Successfully reserved the slot. Note that after the above compareAndSet, other threads
                    // trying to dequeue from this slot will end up spinning until we do the subsequent write.
                    T val = queueManipulator.dequeue(slots, slotsIndex, target);
                    slots[slotsIndex].sequenceNumber = currentHead + slots.length;
                    return val;
                }
                // The head was already advanced by another thread. A newer head has already been observed and the next
                // iteration would make forward progress, so there's no need to spin-wait before trying again.
            } else if (diff < 0) {
                // The sequence number was less than what we needed, which means this slot doesn't
                // yet contain a value we can dequeue, i.e. the segment is empty. Technically it's
                // possible that multiple enqueuers could have written concurrently, with those
                // getting later slots actually finishing first, so there could be elements after
                // this one that are available, but we need to dequeue in order. So before declaring
                // failure and that the segment is empty, we check the tail to see if we're actually
                // empty or if we're just waiting for items in flight or after this one to become available.

                boolean frozen = frozenForEnqueues;
                long currentTail = headAndTail.tail;
                if (currentTail - currentHead <= 0 || (frozen && (currentTail - freezeOffset - currentHead <= 0))) {
                    return null;
                }
                // It's possible it could have become frozen after we checked frozenForEnqueues
                // and before reading the tail. That's ok: in that rare race condition, we just
                // loop around again. This is not necessarily an always-forward-progressing
                // situation since this thread is waiting for another to write to the slot and
                // this thread may have to check the same slot multiple times. Spin-wait to avoid
                // a potential busy-wait, and then try again.
                Os.pause();
            }
            // The item was already dequeued by another thread. The head has already been updated beyond what was
            // observed above, and the sequence number observed above as a volatile load is more recent than the update
            // to the head. So, the next iteration of the loop is guaranteed to see a new head. Since this is an
            // always-forward-progressing situation, there's no need to spin-wait before trying again.
        }
    }

    /**
     * Attempts to enqueue the item.
     *
     * @param item The item to enqueue.
     * @return true if the item was enqueued; otherwise, false.
     */
    public boolean tryEnqueue(T item) {
        Slot<T>[] slots = this.slots;

        // Loop in case of contention...
        while (true) {
            // Get the tail at which to try to insert.
            long currentTail = headAndTail.tail;
            int slotsIndex = (int) (currentTail & slotsMask);

            // Read the sequence number for the tail position.
            long sequenceNumber = slots[slotsIndex].sequenceNumber;

            // The slot is empty and ready for us to enqueue into it if its sequence
            // number matches the slot.
            long diff = sequenceNumber - currentTail;
            if (diff == 0) {
                // We may be racing with other enqueuers. Try to reserve the slot by incrementing
                // the tail. Once we've done that, no one else will be able to write to this slot,
                // and no dequeuer will be able to read from this slot until we've written the new
                // sequence number. WARNING: The next few lines are not reliable on a runtime that
                // supports thread aborts. If a thread abort were to sneak in after the compareAndSet
                // but before the assignment to sequenceNumber, other threads will spin trying to
                // access this slot. If this implementation is ever used on such a platform, this if
                // block should be wrapped in a try-finally.
                if (TAIL.compareAndSet(headAndTail, currentTail, currentTail + 1)) {
                    // Successfully reserved the slot. Note that after the above CompareExchange, other threads
                    // trying to return will end up spinning until we do the subsequent Write.
                    // Copy the item to the slot item and return it.
                    queueManipulator.enqueue(item, slots, slotsIndex);
                    slots[slotsIndex].sequenceNumber = currentTail + 1;
                    return true;
                }
                // The tail was already advanced by another thread. A newer tail has already been observed and the next
                // iteration would make forward progress, so there's no need to spin-wait before trying again.
            } else if (diff < 0) {
                // The sequence number was less than what we needed, which means this slot still
                // contains a value, i.e. the segment is full. Technically it's possible that multiple
                // dequeuers could have read concurrently, with those getting later slots actually
                // finishing first, so there could be spaces after this one that are available, but
                // we need to enqueue in order.
                return false;
            }
            // Either the slot contains an item, or it is empty but because the slot was filled and dequeued. In either
            // case, the tail has already been updated beyond what was observed above, and the sequence number observed
            // above as a volatile load is more recent than the update to the tail. So, the next iteration of the loop
            // is guaranteed to see a new tail. Since this is an always-forward-progressing situation, there's no need
            // to spin-wait before trying again.
        }
    }

    private T createSlotItem() {
        return factory.newInstance();
    }

    /**
     * Ensures that the segment will not accept any subsequent enqueues that aren't already underway.
     * When we mark a segment as being frozen for additional enqueues, we set the `frozenForEnqueues`
     * flag, but that's mostly as a small helper to avoid marking it twice. The real marking comes
     * by modifying the `tail` for the segment, increasing it by `freezeOffset`. This effectively knocks
     * it off the sequence expected by future enqueuers, such that any additional enqueuer will be unable
     * to enqueue due to it not lining up with the expected sequence numbers. This value is chosen
     * specially so that tail will grow to a value that maps to the same slot but that won't be confused
     * with any other enqueue/dequeue sequence number.
     * <p>
     * Must only be called while queue's segment lock is held!
     */
    void ensureFrozenForEnqueues() {
        if (!frozenForEnqueues) {
            frozenForEnqueues = true;
            TAIL.addAndGet(headAndTail, freezeOffset);
        }
    }

    // Padded head and tail indices, to avoid false sharing between producers and consumers.
    private static class PaddedHeadAndTail {
        public volatile long head;
        public long head1, head2, head3, head4, head5, head6, head7; // 7 long fields to pad to 64 bytes
        public volatile long tail;
        public long tail1, tail2, tail3, tail4, tail5, tail6, tail7; // 7 long fields to pad to 64 bytes
    }

    // Represents a slot in the queue.
    public static class Slot<T> {
        // The item.
        public T item;
        public volatile long sequenceNumber;
    }
}

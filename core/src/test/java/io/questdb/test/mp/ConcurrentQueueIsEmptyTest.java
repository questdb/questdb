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

package io.questdb.test.mp;

import io.questdb.mp.ConcurrentPool;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.ValueHolder;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentQueueIsEmptyTest {

    @Test
    public void testClearedQueueIsEmpty() {
        final ConcurrentQueue<Integer> queue = newSmallQueue();
        // Enough items to freeze the first segment and link a second one.
        for (int i = 0; i < 10; i++) {
            queue.enqueue(i);
        }
        Assert.assertFalse(queue.isEmpty());
        queue.clear();
        Assert.assertTrue(queue.isEmpty());
        Assert.assertNull(queue.tryDequeueValue(null));
        Assert.assertTrue(queue.isEmpty());
    }

    @Test
    public void testDrainedHeadSegmentWithItemsInTail() {
        final ConcurrentQueue<Integer> queue = newSmallQueue();
        // The first four fill the initial segment and freeze it; the rest land in the next one.
        for (int i = 0; i < 9; i++) {
            queue.enqueue(i);
        }
        Assert.assertEquals(8, queue.capacity());

        // Drain the head segment only. It is frozen and empty, but the chain is not.
        for (int i = 0; i < 4; i++) {
            Assert.assertFalse(queue.isEmpty());
            Assert.assertEquals(Integer.valueOf(i), queue.tryDequeueValue(null));
        }
        Assert.assertFalse("a drained head segment must not hide items in the tail segment", queue.isEmpty());

        for (int i = 4; i < 9; i++) {
            Assert.assertFalse(queue.isEmpty());
            Assert.assertEquals(Integer.valueOf(i), queue.tryDequeueValue(null));
        }
        Assert.assertTrue(queue.isEmpty());
        Assert.assertNull(queue.tryDequeueValue(null));
        Assert.assertTrue(queue.isEmpty());

        // The queue keeps working after it was drained across a segment boundary.
        queue.enqueue(42);
        Assert.assertFalse(queue.isEmpty());
        Assert.assertEquals(Integer.valueOf(42), queue.tryDequeueValue(null));
        Assert.assertTrue(queue.isEmpty());
    }

    @Test
    public void testNeverReportsEmptyWhileCommittedItemsRemain() throws InterruptedException {
        // Mirrors ConcurrentQueueFuzzTest.testTryDequeueNeverMissesCommittedItem: the queue starts
        // with one item per thread and every thread holds at most one dequeued item at a time, so it
        // always contains at least one committed item. The tiny segment keeps enqueues overflowing
        // into new segments, so the emptiness check races against in-flight segment freezes and
        // links, and it must never read empty.
        final int threadCount = 32;
        final int iterations = 100_000;
        final ConcurrentQueue<Object> queue = newSmallQueue();
        for (int i = 0; i < threadCount; i++) {
            queue.enqueue(new Object());
        }

        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
        final AtomicInteger emptyReadings = new AtomicInteger();
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final ObjList<Thread> threads = new ObjList<>();
        for (int i = 0; i < threadCount; i++) {
            final Thread th = new Thread(() -> {
                try {
                    barrier.await();
                    for (int j = 0; j < iterations; j++) {
                        if (queue.isEmpty()) {
                            emptyReadings.incrementAndGet();
                        }
                        Object item = queue.tryDequeueValue(null);
                        if (item == null) {
                            item = new Object();
                        }
                        queue.enqueue(item);
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            th.start();
            threads.add(th);
        }
        for (int i = 0; i < threadCount; i++) {
            threads.getQuick(i).join();
        }

        Assert.assertTrue(errors.toString(), errors.isEmpty());
        Assert.assertEquals("isEmpty() reported an empty queue while it held committed items", 0, emptyReadings.get());
    }

    @Test
    public void testNewQueueIsEmpty() {
        final ConcurrentQueue<IntHolder> queue = ConcurrentQueue.createConcurrentQueue(IntHolder::new);
        Assert.assertTrue(queue.isEmpty());
        Assert.assertFalse(queue.tryDequeue(new IntHolder()));
        Assert.assertTrue(queue.isEmpty());

        // The default, state-copying manipulator agrees with the check too.
        final IntHolder holder = new IntHolder();
        holder.value = 7;
        queue.enqueue(holder);
        Assert.assertFalse(queue.isEmpty());
        holder.value = 0;
        Assert.assertTrue(queue.tryDequeue(holder));
        Assert.assertEquals(7, holder.value);
        Assert.assertTrue(queue.isEmpty());
    }

    @Test
    public void testTracksSingleSegmentAcrossLaps() {
        final ConcurrentQueue<Integer> queue = newSmallQueue();
        // Several laps around a single segment: every slot sees its sequence number advance past
        // the first lap, and the check must keep agreeing with the dequeue at every step.
        for (int i = 0; i < 20; i++) {
            Assert.assertTrue(queue.isEmpty());
            queue.enqueue(i);
            Assert.assertFalse(queue.isEmpty());
            Assert.assertEquals(Integer.valueOf(i), queue.tryDequeueValue(null));
        }
        Assert.assertTrue(queue.isEmpty());
        Assert.assertEquals(4, queue.capacity());

        // Two items, then a partial drain.
        queue.enqueue(1);
        queue.enqueue(2);
        Assert.assertEquals(Integer.valueOf(1), queue.tryDequeueValue(null));
        Assert.assertFalse(queue.isEmpty());
        Assert.assertEquals(Integer.valueOf(2), queue.tryDequeueValue(null));
        Assert.assertTrue(queue.isEmpty());
    }

    /**
     * A four-slot queue that retains the enqueued objects themselves, so segments fill, freeze
     * and link after a handful of items.
     */
    @SuppressWarnings("unchecked")
    private static <T> ConcurrentQueue<T> newSmallQueue() {
        return new ConcurrentQueue<>(() -> null, ConcurrentPool.POOL_MANIPULATOR, 4);
    }

    private static class IntHolder implements ValueHolder<IntHolder> {
        int value;

        @Override
        public void clear() {
            value = 0;
        }

        @Override
        public void copyTo(IntHolder target) {
            target.value = value;
        }
    }
}

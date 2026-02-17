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


import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentPool<T> {
    @SuppressWarnings("rawtypes")
    public static final ConcurrentSegmentManipulator POOL_MANIPULATOR = new ConcurrentSegmentManipulator() {

        @Override
        public Object dequeue(ConcurrentQueueSegment.Slot[] slots, int slotsIndex, Object unused) {
            var val = slots[slotsIndex].item;
            slots[slotsIndex].item = null;
            return val;
        }

        @Override
        public void enqueue(Object item, ConcurrentQueueSegment.Slot[] slots, int slotsIndex) {
            slots[slotsIndex].item = item;
        }
    };
    private final AtomicInteger count = new AtomicInteger(0);
    private final ConcurrentQueue<T> queue;

    public ConcurrentPool() {
        //noinspection unchecked
        this.queue = new ConcurrentQueue<T>(() -> null, POOL_MANIPULATOR);
    }

    public int capacity() {
        return queue.capacity();
    }

    public int count() {
        return count.get();
    }

    public T pop() {
        T val = queue.tryDequeueValue(null);
        if (val != null) {
            count.decrementAndGet();
        }
        return val;
    }

    public void push(T item) {
        queue.enqueue(item);
        count.incrementAndGet();
    }
}

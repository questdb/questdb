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

package io.questdb.mp;

import io.questdb.std.ObjectFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A ConcurrentQueue variant that tracks the current item count with an AtomicInteger,
 * exposed via {@link #sizeDirty()}. The counter adds a contended atomic increment on
 * every enqueue and a decrement on every successful dequeue, serializing the otherwise
 * lock-free hot path on a single cache line. Use only when a non-blocking size hint is
 * required (e.g., bounding a drain loop). For all other cases, prefer the plain
 * {@link ConcurrentQueue}.
 */
public class CountedConcurrentQueue<T> extends ConcurrentQueue<T> {
    private final AtomicInteger length = new AtomicInteger();

    public CountedConcurrentQueue(ObjectFactory<T> factory, ConcurrentSegmentManipulator<T> queueManipulator) {
        super(factory, queueManipulator);
    }

    public static <T extends ValueHolder<T>> CountedConcurrentQueue<T> create(ObjectFactory<T> factory) {
        return new CountedConcurrentQueue<>(factory, new ValueHolderManipulator<>());
    }

    @Override
    public void enqueue(T item) {
        super.enqueue(item);
        length.incrementAndGet();
    }

    public int sizeDirty() {
        return length.get();
    }

    @Override
    public T tryDequeueValue(T maybeTarget) {
        T val = super.tryDequeueValue(maybeTarget);
        if (val != null) {
            length.decrementAndGet();
        }
        return val;
    }
}

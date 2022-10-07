/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class ThreadSafeObjectPool<T> {
    private final ObjectFactory<T> fact;
    private final AtomicIntegerArray locks;
    private final ObjList<Slot> slots;
    private final int capacity;

    public ThreadSafeObjectPool(ObjectFactory<T> fact, int capacity) {
        this.fact = fact;
        this.capacity = capacity;
        locks = new AtomicIntegerArray(capacity);
        slots = new ObjList<>(capacity);
        for (int i = 0; i < capacity; i++) {
            slots.add(new Slot(i));
        }
    }

    public void releaseInactive() {
        for (int i = 0, n = slots.size(); i < n; i++) {
            if (locks.compareAndSet(i, 0, 1)) {
                slots.getQuick(i).clear();
                locks.set(i, 0);
            }
        }
    }

    public ClosableInstance<T> get() {
        int start = (int)Thread.currentThread().getId() % capacity;
        while (true) {
            for (int i = 0; i < capacity; i++) {
                int id = (i + start) % capacity;
                if (locks.compareAndSet(id, 0, 1)) {
                    return slots.getQuick(id);
                }
            }
            Os.pause();
        }
    }

    private void release(int index) {
        locks.set(index, 0);
    }

    private class Slot implements ClosableInstance<T> {
        private final int index;
        private T obj;

        private Slot(int index) {
            this.index = index;
        }

        @Override
        public void close() {
            release(index);
        }

        @Override
        public T instance() {
            if (obj != null) {
                return obj;
            }
            obj = fact.newInstance();
            return obj;
        }

        void clear() {
            if (obj instanceof Closeable) {
                Misc.free((Closeable) obj);
                obj = null;
            }
        }
    }
}

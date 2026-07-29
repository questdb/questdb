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

import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

final class FiberRing {
    private static final int MAX_CAPACITY = 1 << 30;
    private static final int MIN_CAPACITY = 32;
    private ObjList<Fiber> buffer;
    private volatile int depth;
    private int head;
    private int mask;

    FiberRing(int initialCapacity) {
        if (initialCapacity < 1 || initialCapacity > MAX_CAPACITY) {
            throw new IllegalArgumentException("initialCapacity is out of range [value=" + initialCapacity + ']');
        }
        final int capacity = Numbers.ceilPow2(Math.max(MIN_CAPACITY, initialCapacity));
        buffer = new ObjList<>(capacity);
        buffer.setAll(capacity, null);
        mask = capacity - 1;
    }

    synchronized int capacity() {
        return buffer.size();
    }

    int depth() {
        return depth;
    }

    synchronized void put(Fiber fiber) {
        if (fiber == null) {
            throw new IllegalArgumentException("fiber must not be null");
        }
        if (depth == buffer.size()) {
            throw new IllegalStateException("fiber ring is full");
        }
        final int tail = (head + depth) & mask;
        if (buffer.getQuick(tail) != null) {
            throw new IllegalStateException("fiber ring slot is occupied");
        }
        buffer.setQuick(tail, fiber);
        depth++;
    }

    @TestOnly
    synchronized void setDepthForTesting(int depth) {
        if (depth < 0 || depth > buffer.size()) {
            throw new IllegalArgumentException("depth is out of range [value=" + depth + ']');
        }
        this.depth = depth;
    }

    Fiber tryDequeue() {
        if (depth == 0) {
            return null;
        }
        return dequeue();
    }

    private synchronized Fiber dequeue() {
        if (depth == 0) {
            return null;
        }
        final Fiber fiber = buffer.getQuick(head);
        if (fiber == null) {
            throw new IllegalStateException("fiber ring slot is empty");
        }
        buffer.setQuick(head, null);
        head = (head + 1) & mask;
        depth--;
        return fiber;
    }
}

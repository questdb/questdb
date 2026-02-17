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

import io.questdb.std.*;

import java.io.Closeable;

public class RingQueue<T> implements Closeable {
    private final T[] buf;
    private final int mask;
    private final int memoryTag;
    private long memory;
    private long memorySize;

    @SuppressWarnings("unchecked")
    public RingQueue(ObjectFactory<T> factory, int cycle) {
        // zero queue is allowed for testing
        assert cycle == 0 || Numbers.isPow2(cycle);
        try {
            this.mask = cycle - 1;
            this.buf = (T[]) new Object[cycle];

            for (int i = 0; i < cycle; i++) {
                buf[i] = factory.newInstance();
            }

            // heap based queue
            this.memory = 0;
            this.memorySize = 0;
            this.memoryTag = 0;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @SuppressWarnings("unchecked")
    public RingQueue(DirectObjectFactory<T> factory, long slotSize, int cycle, int memoryTag) {
        try {
            this.mask = cycle - 1;
            this.buf = (T[]) new Object[cycle];

            this.memorySize = slotSize * cycle;
            this.memoryTag = memoryTag;
            this.memory = Unsafe.calloc(memorySize, memoryTag);
            long p = memory;
            for (int i = 0; i < cycle; i++) {
                // intention is that whatever comes out of the factory it should work with the
                // memory allocated by the queue for this slot and should not reallocate ever
                buf[i] = factory.newInstance(p, slotSize);
                p += slotSize;
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        for (int i = 0, n = buf.length; i < n; i++) {
            buf[i] = Misc.freeIfCloseable(buf[i]);
        }
        if (memory != 0) {
            memory = Unsafe.free(memory, memorySize, memoryTag);
            this.memorySize = 0;
        }
    }

    public T get(long cursor) {
        return buf[(int) (cursor & mask)];
    }

    public int getCycle() {
        return buf.length;
    }
}

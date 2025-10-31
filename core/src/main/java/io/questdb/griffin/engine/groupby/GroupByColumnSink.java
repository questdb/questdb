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

package io.questdb.griffin.engine.groupby;

import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class GroupByColumnSink {
    private static final long HEADER_SIZE = 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private final int initialCapacity;
    private GroupByAllocator allocator;
    private long ptr;

    public GroupByColumnSink(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    public void checkCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity. Integer overflow may be?");
        }

        final int oldCapacity = capacity();

        long oldPtr = ptr;
        if (capacity > oldCapacity) {
            final int newCapacity = Math.max(oldCapacity << 1, capacity);
            ptr = allocator.realloc(oldPtr, oldCapacity + HEADER_SIZE, newCapacity + HEADER_SIZE);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
        }
    }

    public GroupByColumnSink of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(HEADER_SIZE + initialCapacity);
            Vect.memset(this.ptr + HEADER_SIZE, initialCapacity, 0);
            Unsafe.getUnsafe().putInt(this.ptr, initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_OFFSET, 0);
        } else {
            this.ptr = ptr;
        }
        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void putDouble(double value) {
        int size = size();
        int newSize = size + 8;
        checkCapacity(newSize);
        Unsafe.getUnsafe().putDouble(ptr + HEADER_SIZE + size, value);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, newSize);
    }

    public void resetPtr() {
        ptr = 0;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }
}
